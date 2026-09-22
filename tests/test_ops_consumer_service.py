"""Tests for lib/ops/consumer_service.py.

The service runs every consumption cycle in a worker thread of its own, one
cycle at a time, and never abandons a cycle it started. These tests drive it
on a real event loop against a scripted consumer, coordinating with Gate
handshakes rather than sleeps: nothing here asserts how long anything takes.
"""

import asyncio
import os
import threading
import unittest
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import Mock, patch

from lib.ops.consumer_service import OpsConsumerService
from tests.execution_support import WAIT, Gate, run_bounded

SERVICE_LOGGER = "lib.ops.consumer_service.OpsConsumerService"


class ScriptedConsumer:
    """Stands in for FileSpoolConsumer: records its cycles, and can hold or fail one.

    Attributes:
        gates: Gates at which given cycles (numbered from 1) wait.
        errors: Exceptions given cycles raise.
        calls: Cycles started so far.
        finished: Numbers of the cycles that completed without an error.
        threads: The thread each cycle ran in.
        running: Cycles running right now.
        max_running: The most cycles ever running at once.
    """

    def __init__(self) -> None:
        self.gates: dict[int, Gate] = {}
        self.errors: dict[int, Exception] = {}
        self.calls = 0
        self.finished: list[int] = []
        self.threads: list[int] = []
        self.running = 0
        self.max_running = 0
        self._changed = threading.Condition()

    def consume(self) -> None:
        """Run one scripted cycle."""
        with self._changed:
            self.calls += 1
            cycle = self.calls
            self.threads.append(threading.get_ident())
            self.running += 1
            self.max_running = max(self.max_running, self.running)
        try:
            gate = self.gates.get(cycle)
            if gate is not None:
                gate.pass_through()
            if cycle in self.errors:
                raise self.errors[cycle]
            with self._changed:
                self.finished.append(cycle)
        finally:
            with self._changed:
                self.running -= 1
                self._changed.notify_all()

    def hold(self, cycle: int) -> Gate:
        """Make a cycle wait at a gate, and return the gate."""
        gate = Gate()
        self.gates[cycle] = gate
        return gate

    async def wait_for(self, condition: Callable[[], bool]) -> None:
        """Wait, off the event loop, until a condition on the cycles holds.

        Raises:
            AssertionError: If it does not hold within WAIT.
        """

        def wait() -> bool:
            with self._changed:
                return self._changed.wait_for(condition, WAIT)

        if not await asyncio.to_thread(wait):
            raise AssertionError("the consumer never reached the expected state")


class ServiceTestCase(unittest.TestCase):
    """A service over a scripted consumer and a throwaway spool."""

    def setUp(self) -> None:
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        environment = patch.dict(os.environ, {"YGG_EVENT_SPOOL": temp_dir.name})
        environment.start()
        self.addCleanup(environment.stop)
        self.consumer = ScriptedConsumer()
        consumer_class = patch(
            "lib.ops.consumer_service.FileSpoolConsumer", return_value=self.consumer
        )
        self.consumer_class = consumer_class.start()
        self.addCleanup(consumer_class.stop)
        self.writer = Mock()

    def service(self, interval_sec: float = 0.0) -> OpsConsumerService:
        """A service writing to a mock sink."""
        return OpsConsumerService(interval_sec=interval_sec, writer=self.writer)

    def record_workers(self) -> list[ThreadPoolExecutor]:
        """Keep every worker the service creates from now on, in order."""
        workers: list[ThreadPoolExecutor] = []

        def create(*args: Any, **kwargs: Any) -> ThreadPoolExecutor:
            worker = ThreadPoolExecutor(*args, **kwargs)
            workers.append(worker)
            return worker

        creation = patch(
            "lib.ops.consumer_service.ThreadPoolExecutor", side_effect=create
        )
        creation.start()
        self.addCleanup(creation.stop)
        return workers

    def assert_released(self, worker: ThreadPoolExecutor) -> None:
        """Assert a worker was shut down: it accepts no further work."""
        with self.assertRaises(RuntimeError):
            worker.submit(lambda: None)


class TestConstruction(unittest.TestCase):
    """Configuration is resolved when the service is built."""

    def test_init_default_values(self):
        with patch.dict(os.environ, {}, clear=True):
            with patch("lib.ops.consumer_service.OpsWriter"):
                service = OpsConsumerService()

                self.assertEqual(service.interval, 2.0)
                self.assertEqual(service.spool, Path("/tmp/ygg_events"))

    def test_init_custom_interval(self):
        with patch("lib.ops.consumer_service.OpsWriter"):
            service = OpsConsumerService(interval_sec=5.0)
            self.assertEqual(service.interval, 5.0)

    def test_init_custom_db_name(self):
        with patch("lib.ops.consumer_service.OpsWriter") as mock_writer:
            OpsConsumerService(db_name="custom_db")

            mock_writer.assert_called_once_with(db_name="custom_db")

    def test_init_env_spool_path(self):
        with patch.dict(os.environ, {"YGG_EVENT_SPOOL": "/custom/spool"}):
            with patch("lib.ops.consumer_service.OpsWriter"):
                service = OpsConsumerService()

                self.assertEqual(service.spool, Path("/custom/spool"))

    def test_init_env_db_name(self):
        with patch.dict(os.environ, {"OPS_DB": "env_db"}):
            with patch("lib.ops.consumer_service.OpsWriter") as mock_writer:
                OpsConsumerService()

                mock_writer.assert_called_once_with(db_name="env_db")

    def test_injected_writer_replaces_the_legacy_one(self):
        writer = Mock()
        with patch("lib.ops.consumer_service.OpsWriter") as legacy:
            service = OpsConsumerService(writer=writer)

        legacy.assert_not_called()
        self.assertIs(service.writer, writer)

    def test_nothing_runs_before_start(self):
        with patch("lib.ops.consumer_service.OpsWriter"):
            service = OpsConsumerService()

        self.assertIsNone(service._task)
        self.assertFalse(service._stop.is_set())


class TestLifecycle(ServiceTestCase):
    """Starting and stopping."""

    def test_consumes_the_spool_until_stopped(self):
        service = self.service()

        async def scenario():
            service.start()
            self.assertEqual(service._task.get_name(), "ops-consumer")
            await self.consumer.wait_for(lambda: len(self.consumer.finished) >= 2)
            await service.stop()
            return service._task

        workers = self.record_workers()

        task = run_bounded(scenario())

        self.assertTrue(task.done())
        self.consumer_class.assert_called_once_with(
            Path(os.environ["YGG_EVENT_SPOOL"]), self.writer
        )
        (worker,) = workers
        self.assert_released(worker)

    def test_start_while_running_starts_nothing_more(self):
        service = self.service()
        gate = self.consumer.hold(1)

        async def scenario():
            service.start()
            first = service._task
            await gate.reached()
            service.start()
            self.assertIs(service._task, first)
            gate.release()
            await service.stop()

        run_bounded(scenario())

        self.consumer_class.assert_called_once()

    def test_restart_after_stop_consumes_again(self):
        service = self.service()

        async def scenario():
            service.start()
            first = service._task
            await self.consumer.wait_for(lambda: len(self.consumer.finished) >= 1)
            await service.stop()
            calls = self.consumer.calls
            service.start()
            await self.consumer.wait_for(lambda: self.consumer.calls > calls)
            await service.stop()
            return first, service._task

        first, second = run_bounded(scenario())

        self.assertIsNot(first, second)
        self.assertTrue(second.done())

    def test_each_start_gets_a_worker_released_when_its_task_ends(self):
        service = self.service()
        workers = self.record_workers()

        async def scenario():
            for _ in range(2):
                calls = self.consumer.calls
                service.start()
                await self.consumer.wait_for(lambda: self.consumer.calls > calls)
                await service.stop()

        run_bounded(scenario())

        self.assertEqual(len(workers), 2)
        self.assertIsNot(workers[0], workers[1])
        for worker in workers:
            self.assert_released(worker)

    def test_worker_of_a_task_cancelled_before_it_ran_is_released(self):
        service = self.service()
        workers = self.record_workers()

        async def scenario():
            service.start()
            task = service._task
            task.cancel()
            await asyncio.wait({task})
            return task

        task = run_bounded(scenario())

        self.assertTrue(task.cancelled())
        self.assertEqual(self.consumer.calls, 0)
        (worker,) = workers
        self.assert_released(worker)

    def test_an_earlier_stop_cannot_stop_a_restarted_service(self):
        # Two callers stop the service at once; the first restarts it as soon
        # as its own stop returns, while the second is still resuming.
        service = self.service()
        workers = self.record_workers()
        gate = self.consumer.hold(1)
        finished: list[str] = []

        async def stop_then_restart() -> None:
            await service.stop()
            service.start()
            finished.append("restarted")

        async def stop() -> None:
            await service.stop()
            finished.append("second stop returned")

        async def scenario():
            service.start()
            await gate.reached()
            restarting = asyncio.create_task(stop_then_restart())
            await asyncio.sleep(0)
            stopping = asyncio.create_task(stop())
            await asyncio.sleep(0)
            gate.release()
            await asyncio.gather(restarting, stopping)
            restarted = service._task
            # The restarted service keeps consuming after both stops returned.
            await self.consumer.wait_for(lambda: len(self.consumer.finished) >= 4)
            alive = not restarted.done()
            await service.stop()
            return alive

        alive = run_bounded(scenario())

        self.assertEqual(finished, ["restarted", "second stop returned"])
        self.assertTrue(alive, "the restarted service stopped")
        self.assertEqual(len(workers), 2)
        for worker in workers:
            self.assert_released(worker)

    def test_task_ending_unexpectedly_is_reported_once(self):
        service = self.service()
        self.consumer_class.side_effect = RuntimeError("spool consumer broken")

        async def scenario():
            service.start()
            await asyncio.gather(service.stop(), service.stop())

        with self.assertLogs(SERVICE_LOGGER, level="ERROR") as logs:
            run_bounded(scenario())

        self.assertEqual(len(logs.records), 1)
        self.assertIn("spool consumer broken", logs.output[0])

    def test_stop_without_start_does_nothing(self):
        service = self.service()

        run_bounded(service.stop())

        self.assertIsNone(service._task)


class TestCyclesStayOffTheEventLoop(ServiceTestCase):
    """A cycle never blocks the event loop, and cycles never overlap."""

    def test_cycles_run_in_the_services_own_thread(self):
        service = self.service()

        async def scenario():
            service.start()
            await self.consumer.wait_for(lambda: len(self.consumer.finished) >= 3)
            await service.stop()
            return threading.get_ident()

        loop_thread = run_bounded(scenario())

        self.assertEqual(len(set(self.consumer.threads)), 1)
        self.assertNotIn(loop_thread, self.consumer.threads)

    def test_event_loop_keeps_running_while_a_cycle_is_held(self):
        service = self.service()
        gate = self.consumer.hold(1)

        async def scenario():
            service.start()
            await gate.reached()
            ticks = 0
            for _ in range(100):
                await asyncio.sleep(0)
                ticks += 1
            # Still inside the held cycle: the loop ran while it was blocked.
            held = self.consumer.running
            gate.release()
            await service.stop()
            return ticks, held

        ticks, held = run_bounded(scenario())

        self.assertEqual(ticks, 100)
        self.assertEqual(held, 1)

    def test_cycles_never_overlap(self):
        service = self.service()

        async def scenario():
            service.start()
            await self.consumer.wait_for(lambda: len(self.consumer.finished) >= 5)
            await service.stop()

        run_bounded(scenario())

        self.assertEqual(self.consumer.max_running, 1)

    def test_failed_cycle_is_logged_and_the_next_one_runs(self):
        service = self.service()
        self.consumer.errors[1] = RuntimeError("spool unreadable")

        async def scenario():
            service.start()
            await self.consumer.wait_for(lambda: 2 in self.consumer.finished)
            await service.stop()

        with self.assertLogs(SERVICE_LOGGER, level="ERROR") as logs:
            run_bounded(scenario())

        self.assertIn("spool unreadable", "\n".join(logs.output))
        self.assertFalse(service._task.cancelled())


class TestNoCycleIsAbandoned(ServiceTestCase):
    """Stopping, or cancellation at shutdown, waits for the cycle in progress."""

    def test_stop_waits_for_the_cycle_in_progress(self):
        service = self.service()
        gate = self.consumer.hold(1)

        async def scenario():
            service.start()
            await gate.reached()
            stopping = asyncio.create_task(service.stop())
            for _ in range(20):
                await asyncio.sleep(0)
            waiting = not stopping.done()
            gate.release()
            await stopping
            return waiting

        waited = run_bounded(scenario())

        self.assertTrue(waited, "stop() returned while the cycle was running")
        self.assertEqual(self.consumer.finished, [1])
        self.assertEqual(self.consumer.calls, 1, "a cycle started after stop()")

    def test_shutdown_cancelling_the_service_waits_for_its_cycle(self):
        # asyncio.run cancels every task still pending when its main coroutine
        # returns; the service's task is cancelled mid-cycle.
        service = self.service()
        gate = self.consumer.hold(1)
        stop_requested = threading.Event()

        class ObservedStop(asyncio.Event):
            def set(self) -> None:
                super().set()
                stop_requested.set()

        service._stop = ObservedStop()

        def release_once_cancellation_is_handled() -> None:
            if gate.entered.wait(WAIT):
                stop_requested.wait(WAIT)
            gate.release()

        releaser = threading.Thread(target=release_once_cancellation_is_handled)
        releaser.start()
        self.addCleanup(releaser.join, WAIT)

        async def main():
            service.start()
            await gate.reached()

        run_bounded(main())
        releaser.join(WAIT)

        self.assertTrue(stop_requested.is_set())
        self.assertEqual(self.consumer.finished, [1])
        self.assertEqual(self.consumer.calls, 1, "a cycle started after shutdown")
        self.assertEqual(self.consumer.running, 0)


if __name__ == "__main__":
    unittest.main()
