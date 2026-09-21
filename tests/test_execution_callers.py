"""Daemon and run-once execution through their real callers.

A real ``YggdrasilCore`` on SQLite internal storage, executing through a real
Engine with scripted ``@step`` functions. The plan watcher, plan store and
coordinator are the ones the core wires itself; only the engine's step
callable, the finalization backoff and the plan store's observation hooks are
test stand-ins. Each test states a guarantee of the callers:

- the daemon admits a plan from its current document, never from the event
  that prompted it, and duplicate events run one attempt, rechecking the plan
  afterwards;
- daemon shutdown, by ``asyncio.run`` or by ``stop()``, stops new steps, waits
  for the running one, and leaves an interrupted plan eligible; ``stop()``
  asks for that before it waits for the watchers;
- run-once exits nonzero for a continuation that finished with failures, for
  a result it could not record, and for an attempt cancelled without Ctrl+C,
  after finishing the healthy work;
- run-once Ctrl+C stops between steps, and its timeout neither interrupts a
  running plan nor abandons plans already eligible.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

from lib.core_utils.event_types import EventType
from lib.core_utils.plan_eligibility import is_plan_eligible
from lib.core_utils.plan_execution import ExecutionStatus, PlanExecutionCoordinator
from lib.core_utils.singleton_decorator import SingletonMeta
from lib.core_utils.yggdrasil_core import YggdrasilCore
from lib.storage.errors import PlanStoreError
from lib.storage.protocols import InternalStorageBundle
from lib.storage.sqlite import (
    SQLiteCheckpointStore,
    SQLiteInternalStore,
    SQLiteOpsSnapshotSink,
    SQLitePlanChangeSource,
    SQLitePlanStore,
)
from lib.watchers.abstract_watcher import YggdrasilEvent
from tests.execution_support import (
    PLAN_ID,
    REALM,
    SCOPE,
    WAIT,
    CapturingEngine,
    ObservedStore,
    RecordingEmitter,
    ScriptedSteps,
    Watchdog,
    chain_plan,
    lanes_plan,
    make_plan,
    run_bounded,
    spec,
)
from yggdrasil.flow.model import CONTINUE_INDEPENDENT_POLICY, Plan
from yggdrasil.flow.planner.api import PlanDraft

CONTINUE = CONTINUE_INDEPENDENT_POLICY
CORE_LOGGER = "lib.core_utils.yggdrasil_core.YggdrasilCore"


def plan_event(doc: dict[str, Any]) -> YggdrasilEvent:
    """The event PlanWatcher emits for a plan document it saw."""
    return YggdrasilEvent(
        EventType.PLAN_EXECUTION,
        {"plan_doc_id": doc["_id"], "plan_doc": doc},
        "PlanWatcher",
    )


class DraftingHandler:
    """Stands in for a realm handler: drafts the same plans for any document."""

    realm_id = REALM

    def __init__(self, *plans: Plan) -> None:
        self.plans = plans

    def derive_scope(self, doc: dict[str, Any]) -> dict[str, Any]:
        return dict(SCOPE)

    def build_planning_context(self, **kwargs: Any) -> None:
        return None

    def run_now(self, payload: dict[str, Any]) -> list[PlanDraft]:
        return [PlanDraft(plan=plan, auto_run=True) for plan in self.plans]

    def class_qualified_name(self) -> str:
        return f"{__name__}.DraftingHandler"


class CallerTestCase(unittest.TestCase):
    """A YggdrasilCore on SQLite storage whose engine runs scripted steps."""

    def setUp(self) -> None:
        SingletonMeta._instances.clear()
        self.addCleanup(SingletonMeta._instances.clear)
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        directory = Path(temp_dir.name)

        environment = patch.dict(
            os.environ, {"YGG_EVENT_SPOOL": str(directory / "spool")}
        )
        environment.start()
        self.addCleanup(environment.stop)

        self.journal: list[str] = []
        self.steps = ScriptedSteps()
        self.start_patch(
            patch("yggdrasil.core.engine.resolve_callable", return_value=self.steps.fn)
        )
        self.emitter = RecordingEmitter()
        self.engine = CapturingEngine(
            work_root=directory / "work", emitter=self.emitter, journal=self.journal
        )
        self.start_patch(
            patch("lib.core_utils.yggdrasil_core.Engine", return_value=self.engine)
        )
        ops_service = self.start_patch(
            patch("lib.core_utils.yggdrasil_core.OpsConsumerService")
        )
        ops_service.return_value.stop = AsyncMock()

        internal = SQLiteInternalStore(directory / "ygg.sqlite3")
        self.internal = internal
        self.plans = SQLitePlanStore(internal)
        self.store = ObservedStore(self.plans, self.journal)
        bundle = InternalStorageBundle(
            backend="sqlite",
            plans=self.store,  # type: ignore[arg-type]
            plan_changes=SQLitePlanChangeSource(internal),
            checkpoints=SQLiteCheckpointStore(internal),
            ops_snapshots=SQLiteOpsSnapshotSink(internal),
        )
        self.core = YggdrasilCore(
            {"work_root": str(directory / "work")}, storage=bundle
        )
        self.assertIs(self.core.plan_executions._engine, self.engine)

        # The core's own coordinator, with backoff delays recorded instead of
        # waited out.
        self.delays: list[float] = []
        self.core.plan_executions = PlanExecutionCoordinator(
            engine=self.engine,
            plan_store=self.store,  # type: ignore[arg-type]
            logger=self.core._logger,
            sleep=self.record_delay,
        )

    def start_patch(self, patcher: Any) -> Any:
        """Start a patch for the rest of the test."""
        started = patcher.start()
        self.addCleanup(patcher.stop)
        return started

    async def record_delay(self, delay: float) -> None:
        self.delays.append(delay)
        await asyncio.sleep(0)

    def save(
        self,
        plan: Plan | None = None,
        *,
        auto_run: bool = True,
        authority: str = "daemon",
        owner: str | None = None,
    ) -> dict[str, Any]:
        """Save plan (approved by default) and return the stored document."""
        plan = plan or chain_plan()
        self.plans.save_plan(
            plan,
            REALM,
            dict(SCOPE),
            auto_run=auto_run,
            execution_authority=authority,
            execution_owner=owner,
        )
        return self.stored(plan.plan_id)

    def stored(self, plan_id: str = PLAN_ID) -> dict[str, Any]:
        doc = self.plans.fetch_plan(plan_id)
        assert doc is not None
        return doc

    def assert_unconsumed(self, plan_id: str = PLAN_ID) -> None:
        doc = self.stored(plan_id)
        self.assertEqual(doc["executed_run_token"], -1)
        self.assertNotIn("last_finalized_execution", doc)
        self.assertTrue(is_plan_eligible(doc))

    def assert_unconsumed_draft(self, plan_id: str) -> None:
        doc = self.stored(plan_id)
        self.assertEqual(doc["executed_run_token"], -1)
        self.assertNotIn("last_finalized_execution", doc)

    def spy_on_submissions(self) -> list[asyncio.Future]:
        """Collect the result future of every request the core submits."""
        futures: list[asyncio.Future] = []
        submit = self.core.plan_executions.submit

        def recording_submit(plan_doc_id, claim):
            future = submit(plan_doc_id, claim)
            futures.append(future)
            return future

        self.core.plan_executions.submit = recording_submit  # type: ignore[method-assign]
        return futures

    def release_once_cancellation_is_requested(self, gate) -> threading.Thread:
        """Release gate from another thread once the attempt is asked to stop."""

        def release() -> None:
            if gate.entered.wait(WAIT):
                self.engine.contexts[0].cancel_event.wait(WAIT)
            gate.release()

        thread = threading.Thread(target=release)
        thread.start()
        self.addCleanup(thread.join, WAIT)
        return thread


class TestDaemonExecution(CallerTestCase):
    """PlanWatcher events through the daemon's handler."""

    def test_duplicate_events_run_one_attempt_and_the_plan_is_rechecked(self):
        doc = self.save()
        submitted = self.spy_on_submissions()

        async def scenario():
            # Both in the same event-loop tick, before either can start.
            self.core._handle_plan_execution_event(plan_event(doc))
            self.core._handle_plan_execution_event(plan_event(doc))
            return [await future for future in submitted]

        first, second = run_bounded(scenario())

        self.assertEqual(first.status, ExecutionStatus.FINALIZED)
        self.assertEqual(second.status, ExecutionStatus.NOT_ELIGIBLE)
        self.assertEqual(self.journal, ["read", "attempt:0", "finalize:0", "read"])
        self.assertEqual(self.stored()["executed_run_token"], 0)

    def test_event_document_is_never_used_for_admission(self):
        stale = self.save(make_plan(spec("a", sample="old")))
        current = self.save(make_plan(spec("a", sample="new")), auto_run=False)
        submitted = self.spy_on_submissions()

        async def deliver():
            self.core._handle_plan_execution_event(plan_event(stale))
            return await submitted[-1]

        # The event says approved; the plan is a draft now.
        unapproved = run_bounded(deliver())
        self.assertEqual(unapproved.status, ExecutionStatus.NOT_ELIGIBLE)
        self.assertEqual(self.engine.contexts, [])

        # Approved again: the current version runs, under its own generation.
        doc = self.stored()
        doc["status"] = "approved"
        self.internal.put_document(
            "plans", PLAN_ID, doc, bump_plan_seq=True, expected_rev=doc["_rev"]
        )
        result = run_bounded(deliver())

        self.assertEqual(result.status, ExecutionStatus.FINALIZED)
        self.assertEqual(self.steps.params["a"], {"sample": "new"})
        self.assertEqual(result.report.plan_generation, current["plan_generation"])

    def test_failed_continuation_is_recorded_and_not_rerun(self):
        self.steps.fail("lane1_demux", RuntimeError("bad sample sheet"))
        doc = self.save(lanes_plan(CONTINUE))
        submitted = self.spy_on_submissions()

        async def deliver():
            self.core._handle_plan_execution_event(plan_event(doc))
            return await submitted[-1]

        result = run_bounded(deliver())

        self.assertEqual(result.status, ExecutionStatus.FINALIZED)
        self.assertEqual(self.steps.calls.count("lane2_upload"), 1)
        self.assertNotIn("lane1_upload", self.steps.calls)
        stored = self.stored()
        self.assertEqual(stored["executed_run_token"], 0)
        self.assertEqual(stored["last_finalized_execution"]["outcome"], "failed")

        # A later event for the same, finished request runs nothing.
        again = run_bounded(deliver())
        self.assertEqual(again.status, ExecutionStatus.NOT_ELIGIBLE)
        self.assertEqual(len(self.engine.contexts), 1)

    def test_fail_fast_failure_leaves_the_plan_eligible(self):
        self.steps.fail("a", RuntimeError("Engine failed"))
        doc = self.save()
        submitted = self.spy_on_submissions()

        async def deliver():
            self.core._handle_plan_execution_event(plan_event(doc))
            return await submitted[-1]

        with self.assertLogs(CORE_LOGGER, level=logging.ERROR):
            result = run_bounded(deliver())

        self.assertEqual(result.status, ExecutionStatus.UNFINISHED)
        self.assertEqual(self.store.calls["finalize"], 0)
        self.assert_unconsumed()

    def test_shutdown_waits_for_the_running_step_and_leaves_the_plan_eligible(self):
        gate = self.steps.block("a")
        doc = self.save()
        self.release_once_cancellation_is_requested(gate)

        async def main():
            self.core._handle_plan_execution_event(plan_event(doc))
            await gate.reached()
            # Returning now makes asyncio.run cancel the pending execution
            # task on the way out, as the daemon's Ctrl+C shutdown does.

        run_bounded(main())

        self.assertEqual(self.steps.calls, ["a"])
        self.assertEqual(self.steps.running, 0)
        self.assertTrue(self.engine.contexts[0].report.is_finished)
        self.assertFalse(self.core.plan_executions.is_in_flight(PLAN_ID))
        self.assert_unconsumed()

    def test_stop_drains_in_flight_executions_before_the_ops_consumer(self):
        gate = self.steps.block("a")
        doc = self.save()
        self.release_once_cancellation_is_requested(gate)
        in_flight_when_ops_stopped: list[bool] = []
        self.core.ops_consumer.stop.side_effect = lambda: (
            in_flight_when_ops_stopped.append(
                self.core.plan_executions.is_in_flight(PLAN_ID)
            )
        )

        async def main():
            self.core._running = True
            self.core._handle_plan_execution_event(plan_event(doc))
            await gate.reached()
            await self.core.stop()

        run_bounded(main())

        self.assertEqual(in_flight_when_ops_stopped, [False])
        self.assertEqual(self.steps.calls, ["a"])
        self.assert_unconsumed()

    def test_stop_asks_attempts_to_stop_before_waiting_for_watchers(self):
        # A watcher can take its time to stop; no step may start meanwhile.
        gate = self.steps.block("a")
        doc = self.save()
        self.release_once_cancellation_is_requested(gate)
        requested_when_watcher_stopped: list[bool] = []

        class SlowToStopWatcher:
            async def stop(watcher) -> None:
                requested_when_watcher_stopped.append(
                    self.engine.contexts[0].cancellation_requested
                )

        self.core.register_watcher(SlowToStopWatcher())

        async def main():
            self.core._running = True
            self.core._handle_plan_execution_event(plan_event(doc))
            await gate.reached()
            await self.core.stop()

        run_bounded(main())

        self.assertEqual(requested_when_watcher_stopped, [True])
        self.assertEqual(self.steps.calls, ["a"])
        self.assert_unconsumed()


class TestRunOnceExecution(CallerTestCase):
    """``run_once_with_watcher`` and its watcher loop."""

    OWNER = "run_once:test-session"

    def run_once(self, *plans: Plan) -> int:
        """Run ``yggdrasil run-doc --run-once`` for a document drafting plans."""
        self.core.pdm = Mock()
        self.core.pdm.fetch_document_by_id.return_value = {"project_id": "P1"}
        handler: Any = DraftingHandler(*plans)
        self.core.subscriptions[EventType.PROJECT_CHANGE] = [handler]
        return self.core.run_once_with_watcher("P1", timeout_seconds=30)

    def watch(self, *plan_ids: str, timeout_seconds: float = 30) -> asyncio.Task[int]:
        """Start the run-once watcher loop over plan_ids (PLAN_ID) as a task."""
        return asyncio.create_task(
            self.core._run_once_watcher_loop(
                pending_plan_ids=list(plan_ids or [PLAN_ID]),
                execution_owner=self.OWNER,
                timeout_seconds=timeout_seconds,  # type: ignore[arg-type]
            )
        )

    def flag_when_logged(self, text: str) -> threading.Event:
        """An event set once the core logs a message containing text."""
        logged = threading.Event()

        class Flag(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                if text in record.getMessage():
                    logged.set()

        flag = Flag()
        logger = logging.getLogger(CORE_LOGGER)
        logger.addHandler(flag)
        self.addCleanup(logger.removeHandler, flag)
        return logged

    def test_failed_continuation_finishes_healthy_work_and_exits_nonzero(self):
        self.steps.fail("lane1_demux", RuntimeError("bad sample sheet"))

        exit_code = self.run_once(lanes_plan(CONTINUE))

        self.assertEqual(exit_code, 1)
        self.assertEqual(
            sorted(self.steps.calls), ["lane1_demux", "lane2_demux", "lane2_upload"]
        )
        stored = self.stored()
        self.assertEqual(stored["executed_run_token"], 0)
        self.assertEqual(stored["last_finalized_execution"]["outcome"], "failed")

    def test_successful_plans_exit_zero(self):
        exit_code = self.run_once(
            chain_plan(), make_plan(spec("x"), plan_id="pln_second")
        )

        self.assertEqual(exit_code, 0)
        for plan_id in (PLAN_ID, "pln_second"):
            self.assertEqual(
                self.stored(plan_id)["last_finalized_execution"]["outcome"],
                "succeeded",
            )

    def test_result_that_cannot_be_recorded_exits_nonzero(self):
        self.store.fail_finalize(PlanStoreError("permission denied"))

        exit_code = self.run_once(chain_plan())

        self.assertEqual(exit_code, 1)
        self.assertEqual(self.steps.calls, ["a", "b"])
        self.assert_unconsumed()

    def test_interrupt_stops_between_steps_and_leaves_the_plan_eligible(self):
        gate = self.steps.block("a")
        self.save(authority="run_once", owner=self.OWNER)

        async def scenario():
            loop = self.watch()
            await gate.reached()
            signal.raise_signal(signal.SIGINT)
            self.assertTrue(self.engine.contexts[0].cancellation_requested)
            gate.release()
            return await loop

        original = signal.getsignal(signal.SIGINT)
        try:
            exit_code = run_bounded(scenario())
        finally:
            signal.signal(signal.SIGINT, original)

        self.assertEqual(exit_code, 130)
        self.assertEqual(self.steps.calls, ["a"])
        self.assertEqual(self.store.calls["finalize"], 0)
        self.assert_unconsumed()

    def test_timeout_never_interrupts_a_running_plan(self):
        gate = self.steps.block("a")
        self.save(authority="run_once", owner=self.OWNER)
        timed_out = self.flag_when_logged("Timeout after")

        async def scenario():
            loop = self.watch(timeout_seconds=0.01)
            await gate.reached()
            await asyncio.to_thread(timed_out.wait, WAIT)
            self.assertFalse(self.engine.contexts[0].cancellation_requested)
            gate.release()
            return await loop

        exit_code = run_bounded(scenario())

        self.assertTrue(timed_out.is_set())
        self.assertEqual(exit_code, 0)
        self.assertEqual(self.steps.calls, ["a", "b"])
        self.assertEqual(self.stored()["executed_run_token"], 0)

    def test_timeout_still_executes_plans_already_eligible(self):
        # The timeout bounds the wait for approval. A plan approved and queued
        # behind a long-running one was never waiting for approval.
        gate = self.steps.block("a")
        self.save(authority="run_once", owner=self.OWNER)
        self.save(
            make_plan(spec("x"), plan_id="pln_second"),
            authority="run_once",
            owner=self.OWNER,
        )
        timed_out = self.flag_when_logged("Timeout after")

        async def scenario():
            loop = self.watch(PLAN_ID, "pln_second", timeout_seconds=0.01)
            await gate.reached()
            await asyncio.to_thread(timed_out.wait, WAIT)
            gate.release()
            return await loop

        exit_code = run_bounded(scenario())

        self.assertTrue(timed_out.is_set())
        self.assertEqual(exit_code, 0)
        self.assertEqual(self.steps.calls, ["a", "b", "x"])
        for plan_id in (PLAN_ID, "pln_second"):
            self.assertEqual(self.stored(plan_id)["executed_run_token"], 0)

    def test_plan_still_unapproved_at_the_timeout_exits_nonzero(self):
        self.save(authority="run_once", owner=self.OWNER)
        self.save(
            make_plan(spec("x"), plan_id="pln_draft"),
            auto_run=False,
            authority="run_once",
            owner=self.OWNER,
        )

        async def scenario():
            return await self.watch(PLAN_ID, "pln_draft", timeout_seconds=0.01)

        exit_code = run_bounded(scenario())

        self.assertEqual(exit_code, 1)
        self.assertEqual(self.stored()["executed_run_token"], 0)
        self.assertNotIn("x", self.steps.calls)
        self.assertEqual(self.stored("pln_draft")["status"], "draft")
        self.assert_unconsumed_draft("pln_draft")

    def test_step_raising_cancelled_error_ends_the_session_with_an_error(self):
        # Without Ctrl+C, a cancelled attempt is this session's failure: the
        # session must not wait for its timeout as if the plan were pending.
        self.steps.fail("a", asyncio.CancelledError("subwork was cancelled"))

        with Watchdog(WAIT) as watchdog:
            exit_code = self.run_once(chain_plan())

        self.assertFalse(watchdog.expired)
        self.assertEqual(exit_code, 1)
        self.assertEqual(self.steps.calls, ["a"])
        self.assert_unconsumed()


if __name__ == "__main__":
    unittest.main()
