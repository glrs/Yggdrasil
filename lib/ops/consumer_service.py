from __future__ import annotations

import asyncio
import functools
import logging
import os
from concurrent.futures import Executor, ThreadPoolExecutor

from lib.core_utils.logging_utils import custom_logger
from lib.core_utils.runtime_paths import resolve_event_spool
from lib.core_utils.worker_futures import outlast_cancellation
from lib.ops.consumer import FileSpoolConsumer
from lib.ops.sinks.couch import OpsWriter
from lib.storage.protocols import OpsSnapshotSink

# TODO: consider moving to lib/ops/services/ when more services land.


class OpsConsumerService:
    """Periodic event-spool consumer writing plan_status snapshots.

    Each cycle reads the spool and writes every plan's snapshot. It runs in a
    worker thread of the service's own, never on the event loop, however long
    the spool: the loop only waits for it. With one worker thread, and each
    cycle awaited before the next is scheduled, cycles never overlap. A cycle
    that fails is logged, and the next one runs as usual.

    Work handed to the thread is never abandoned: stopping the service waits
    for the cycle in progress to finish, and so does its task when shutdown
    cancels it, which then starts no further cycle.

    Each start runs a task with a worker of its own, which only that task's
    end releases. A stop acts on the task running when it was called and
    touches nothing once it has waited, so stops and restarts that overlap
    can never leave a restarted service without its worker.

    The snapshot sink is normally injected by YggdrasilCore from its
    InternalStorageBundle. Bare construction falls back to the legacy
    CouchDB ``OpsWriter`` (honoring ``OPS_DB``, deprecated).
    """

    def __init__(
        self,
        interval_sec: float = 2.0,
        db_name: str | None = None,
        *,
        writer: OpsSnapshotSink | None = None,
        logger: logging.Logger | None = None,
    ):
        """Configure the service; nothing runs until :meth:`start`.

        Args:
            interval_sec: Seconds between the end of one consumption cycle
                and the start of the next.
            db_name: Legacy CouchDB ops database name override; only used
                when no ``writer`` is injected.
            writer: Injected snapshot sink (backend-neutral).
            logger: Logger; a class logger is created when omitted.
        """
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")
        self.interval = interval_sec
        self.spool = resolve_event_spool()
        if writer is None:
            writer = OpsWriter(
                db_name=db_name or os.environ.get("OPS_DB") or "yggdrasil_ops"
            )
        self.writer = writer
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    async def _loop(self, worker: Executor) -> None:
        """Run consumption cycles until the service is stopped.

        Args:
            worker: The thread every cycle runs in.
        """
        consumer = FileSpoolConsumer(self.spool, self.writer)
        while not self._stop.is_set():
            await self._consume_once(consumer, worker)
            if self._stop.is_set():
                return
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval)
            except TimeoutError:
                pass

    async def _consume_once(
        self, consumer: FileSpoolConsumer, worker: Executor
    ) -> None:
        """Run one consumption cycle in the worker thread and wait for it.

        If this task is cancelled meanwhile, the cycle still finishes and no
        further cycle starts.

        Args:
            consumer: The spool consumer, kept across cycles.
            worker: The thread the cycle runs in.
        """
        cycle = asyncio.get_running_loop().run_in_executor(worker, consumer.consume)
        try:
            await outlast_cancellation(cycle, self._stop.set)
        except Exception:
            self._logger.exception(
                "Writing plan snapshots from the event spool failed; "
                "retrying in the next cycle"
            )

    def start(self) -> None:
        """Start consuming, unless the service is already running.

        A service still finishing the cycle a stop interrupted counts as
        running, so a new one never overlaps it. The new task gets a worker
        thread of its own, released when the task is done, however it ends:
        even a task cancelled before it first ran releases it.
        """
        if self._task is not None and not self._task.done():
            return
        self._stop.clear()
        worker = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="ygg-ops-consumer"
        )
        task = asyncio.create_task(self._loop(worker), name="ops-consumer")
        task.add_done_callback(functools.partial(self._task_ended, worker))
        self._task = task

    async def stop(self) -> None:
        """Stop consuming, once the cycle in progress, if any, has finished.

        Stops the task running when it is called, and does nothing more once
        that task is done, so it cannot affect a service restarted while it
        waited. The task is waited for, never cancelled, so a cycle's snapshot
        writes are not cut off part-way.
        """
        self._stop.set()
        task = self._task
        if task is not None and not task.done():
            # asyncio.wait, unlike awaiting the task, never cancels it.
            await asyncio.wait({task})

    def _task_ended(self, worker: ThreadPoolExecutor, task: asyncio.Task[None]) -> None:
        """Release a finished task's worker, and report how the task ended.

        The task's own worker and no other: a task never ends while a cycle
        runs in its worker, so the worker is idle by now.

        Args:
            worker: The worker the task ran its cycles in.
            task: The finished task.
        """
        worker.shutdown(wait=False)
        if not task.cancelled() and task.exception() is not None:
            self._logger.error(
                "The ops consumer stopped unexpectedly", exc_info=task.exception()
            )
