"""Tests for what an execution attempt publishes.

Every event an attempt publishes carries the attempt's correlation, including
the events the engine emits itself rather than through a StepContext. Each
attempt leaves a start record before anything else happens, publishes one
``step.blocked`` per step a failure blocks at the moment it is blocked, and
ends with one report. The execution IDs that order attempts come from the
engine's allocator, which reads back only the spool the engine writes to.

Steps run through real ``@step`` functions. Spool-backed tests read the files a
FileSpoolEmitter wrote; the rest record events in memory. Cross-thread tests
coordinate with Gate handshakes, never sleeps.
"""

import json
import os
import shutil
import threading
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import Mock, patch

from lib.ops.consumer import build_plan_snapshot
from tests.execution_support import (
    REALM,
    SCOPE,
    WAIT,
    Clock,
    Gate,
    RecordingEmitter,
    ScriptedSteps,
    make_plan,
    spec,
)
from yggdrasil.core.engine import Engine
from yggdrasil.core.execution_ids import ExecutionIdAllocator, execution_timestamp
from yggdrasil.flow.attempt import AttemptContext
from yggdrasil.flow.errors import (
    AttemptCancelledError,
    EventPublicationError,
    OrchestrationError,
    PermanentStepError,
    PreflightValidationError,
    TransientStepError,
)
from yggdrasil.flow.events.attempt_records import (
    ATTEMPT_REPORT_EVENT,
    ATTEMPT_STARTED_EVENT,
    STEP_BLOCKED_EVENT,
    SpoolAttemptHistory,
    record_filename,
)
from yggdrasil.flow.events.emitter import EventEmitter, FileSpoolEmitter
from yggdrasil.flow.model import CONTINUE_INDEPENDENT_POLICY, FAIL_FAST_POLICY, Plan
from yggdrasil.flow.outcomes import AttemptReport, StepOutcome, TerminationReason
from yggdrasil.flow.step import StepContext

CONTINUE = CONTINUE_INDEPENDENT_POLICY
FAIL_FAST = FAIL_FAST_POLICY
PLAN_ID = "pln_events"
CORRELATION_FIELDS = ("execution_id", "plan_generation", "run_token")
T0 = datetime(2026, 9, 21, 12, 5, tzinfo=UTC)
FAR_FUTURE_ID = "exec_20990101T000000000000Z_" + "f" * 32


def exception_chain(exc: BaseException) -> list[BaseException]:
    """Every exception reachable from exc through __cause__ and __context__."""
    seen: list[BaseException] = []
    pending = [exc]
    while pending:
        current = pending.pop()
        if any(current is s for s in seen):
            continue
        seen.append(current)
        pending.extend(
            e for e in (current.__cause__, current.__context__) if e is not None
        )
    return seen


def plan_of(*specs, policy: str = CONTINUE, plan_id: str = PLAN_ID) -> Plan:
    """A plan of scripted steps under this module's plan ID."""
    return make_plan(*specs, policy=policy, plan_id=plan_id)


class EngineEventsTestCase(unittest.TestCase):
    """An engine recording events in memory, and a spool for spool-backed ones."""

    def setUp(self) -> None:
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.root = Path(temp_dir.name)
        self.work_root = self.root / "work"
        self.spool = self.root / "spool"
        self.steps = ScriptedSteps()
        resolver = patch(
            "yggdrasil.core.engine.resolve_callable", return_value=self.steps.fn
        )
        resolver.start()
        self.addCleanup(resolver.stop)
        self.emitter = RecordingEmitter()
        self.engine = Engine(work_root=self.work_root, emitter=self.emitter)

    def spool_engine(self, **kwargs: Any) -> Engine:
        """An engine publishing to this test's spool."""
        return Engine(
            work_root=self.work_root, emitter=FileSpoolEmitter(self.spool), **kwargs
        )

    def run_attempt(
        self, plan: Plan, context: AttemptContext, engine: Engine | None = None
    ) -> BaseException | None:
        """Run one attempt through _run_attempt; return what it raised."""
        try:
            (engine or self.engine)._run_attempt(plan, context=context)
        except BaseException as exc:
            return exc
        return None

    def types(self) -> list[str]:
        """Event types recorded so far, in emission order."""
        return [str(event.get("type")) for event in self.emitter.events]

    def of_type(self, event_type: str) -> list[dict]:
        """Recorded events of one type."""
        return [e for e in self.emitter.events if e.get("type") == event_type]

    def plan_dir(self, plan: Plan) -> Path:
        """The plan's spool directory."""
        return self.spool / plan.realm / plan.plan_id

    def spooled(self, path: Path) -> dict:
        """One spooled event."""
        return json.loads(path.read_text(encoding="utf-8"))

    def snapshot(self, plan: Plan) -> dict:
        """The plan's snapshot, built from the spool."""
        return build_plan_snapshot(self.plan_dir(plan), plan.realm, plan.plan_id)


class TestEventCorrelation(EngineEventsTestCase):
    """Every event of one attempt carries that attempt's identity, and only it."""

    def test_attempts_are_told_apart_by_every_event_type(self):
        # "stable" executes in the first attempt and is reused in the second;
        # "flaky" fails transiently in both. Between them the attempts publish
        # every kind of event, including both the engine emits directly.
        self.steps.fail("flaky", TransientStepError("cluster busy"))
        plan = plan_of(spec("stable"), spec("flaky"), spec("after", "flaky"))

        first = self.engine.run(plan)
        first_events = list(self.emitter.events)
        self.emitter.events.clear()
        second = self.engine.run(plan)
        second_events = list(self.emitter.events)

        assert first is not None and second is not None
        self.assertNotEqual(first.execution_id, second.execution_id)
        for report, events in ((first, first_events), (second, second_events)):
            with self.subTest(execution_id=report.execution_id):
                self.assertEqual(
                    {event["execution_id"] for event in events},
                    {report.execution_id},
                )
        types = {str(event["type"]) for event in second_events}
        self.assertLessEqual(
            {
                ATTEMPT_STARTED_EVENT,
                "step.skipped",
                "step.started",
                "step.failed",
                "step.retry_unimplemented",
                STEP_BLOCKED_EVENT,
                ATTEMPT_REPORT_EVENT,
            },
            types,
        )

    def test_captured_generation_and_token_are_on_every_event(self):
        self.steps.fail("b", PermanentStepError("broken"))
        plan = plan_of(spec("a"), spec("b"), spec("c", "b"))
        context = AttemptContext.for_plan(
            plan, execution_id="exec_captured", plan_generation="gen-1", run_token=3
        )

        self.assertIsNone(self.run_attempt(plan, context))

        self.assertGreater(len(self.emitter.events), 5)
        for event in self.emitter.events:
            with self.subTest(type=event["type"]):
                self.assertEqual(
                    {field: event[field] for field in CORRELATION_FIELDS},
                    {
                        "execution_id": "exec_captured",
                        "plan_generation": "gen-1",
                        "run_token": 3,
                    },
                )

    def test_direct_run_events_say_that_nothing_was_captured(self):
        report = self.engine.run(plan_of(spec("a")))

        assert report is not None
        for event in self.emitter.events:
            with self.subTest(type=event["type"]):
                self.assertEqual(event["execution_id"], report.execution_id)
                self.assertIsNone(event["plan_generation"])
                self.assertIsNone(event["run_token"])

    def test_a_payload_field_cannot_move_an_event_into_another_attempt(self):
        self.steps.behaviors["a"] = lambda ctx: ctx.emit(
            "step.echo", execution_id="forged", run_token=99
        )

        report = self.engine.run(plan_of(spec("a")))

        assert report is not None
        (echo,) = self.of_type("step.echo")
        self.assertEqual(echo["execution_id"], report.execution_id)
        self.assertIsNone(echo["run_token"])

    def test_step_context_and_its_data_access_carry_the_attempt(self):
        seen: list[tuple[Any, Any]] = []

        def inspect(ctx: StepContext) -> None:
            assert ctx.data is not None
            trace = ctx.data._trace_context
            assert trace is not None
            seen.append((ctx.correlation, trace.correlation))

        self.steps.behaviors["a"] = inspect
        plan = plan_of(spec("a"))
        context = AttemptContext.for_plan(
            plan, execution_id="exec_ctx", plan_generation="gen-2", run_token=1
        )

        self.assertIsNone(self.run_attempt(plan, context))

        self.assertEqual(seen, [(context.correlation, context.correlation)])

    def test_cache_skip_says_it_was_a_cache_hit(self):
        plan = plan_of(spec("a"))
        self.engine.run(plan)

        self.engine.run(plan)

        (skipped,) = self.of_type("step.skipped")
        self.assertEqual(skipped["reason"], "cache_hit")


class TestAttemptStartRecord(EngineEventsTestCase):
    """Every attempt is recorded before it does anything else."""

    def test_start_record_comes_first_and_describes_the_attempt(self):
        plan = plan_of(spec("a"), spec("b", "a"), policy=FAIL_FAST)
        context = AttemptContext.for_plan(
            plan,
            execution_id="exec_start",
            plan_generation="gen-3",
            run_token=2,
            execution_authority="run_once",
            execution_owner="owner-1",
        )

        self.assertIsNone(self.run_attempt(plan, context))

        self.assertEqual(self.types()[0], ATTEMPT_STARTED_EVENT)
        (started,) = self.of_type(ATTEMPT_STARTED_EVENT)
        self.assertEqual(
            {
                key: started[key]
                for key in (
                    "realm",
                    "scope",
                    "plan_id",
                    "execution_id",
                    "plan_generation",
                    "run_token",
                    "execution_authority",
                    "execution_owner",
                    "failure_policy",
                    "started_at",
                    "steps",
                )
            },
            {
                "realm": REALM,
                "scope": SCOPE,
                "plan_id": PLAN_ID,
                "execution_id": "exec_start",
                "plan_generation": "gen-3",
                "run_token": 2,
                "execution_authority": "run_once",
                "execution_owner": "owner-1",
                "failure_policy": FAIL_FAST,
                "started_at": context.report.started_at,
                "steps": [
                    {"step_id": "a", "step_name": "a", "deps": []},
                    {"step_id": "b", "step_name": "b", "deps": ["a"]},
                ],
            },
        )
        self.assertEqual(
            started["_spool_path"],
            {
                "realm": REALM,
                "plan_id": PLAN_ID,
                "filename": record_filename("exec_start", ATTEMPT_STARTED_EVENT),
            },
        )

    def test_rejected_attempt_leaves_a_readable_start_record(self):
        engine = self.spool_engine()
        plan = plan_of(spec("a", "b"), spec("b", "a"))

        with self.assertRaises(PreflightValidationError):
            engine.run(plan)

        records = sorted(self.plan_dir(plan).glob("*.json"))
        by_type = {self.spooled(p)["type"]: self.spooled(p) for p in records}
        self.assertEqual(set(by_type), {ATTEMPT_STARTED_EVENT, ATTEMPT_REPORT_EVENT})
        started = by_type[ATTEMPT_STARTED_EVENT]
        self.assertEqual([entry["step_id"] for entry in started["steps"]], ["a", "b"])
        self.assertEqual(
            by_type[ATTEMPT_REPORT_EVENT]["report"]["termination_reason"],
            TerminationReason.PREFLIGHT_REJECTED.value,
        )
        self.assertEqual(
            by_type[ATTEMPT_REPORT_EVENT]["execution_id"], started["execution_id"]
        )
        # No step ran: nothing below the plan level, and no work directory.
        self.assertEqual([p for p in self.plan_dir(plan).iterdir() if p.is_dir()], [])
        self.assertFalse((self.work_root / PLAN_ID).exists())
        self.assertEqual(self.steps.calls, [])

    def test_attempt_without_steps_is_recorded_too(self):
        report = self.engine.run(plan_of())

        assert report is not None
        self.assertEqual(self.types(), [ATTEMPT_STARTED_EVENT, ATTEMPT_REPORT_EVENT])
        self.assertEqual(self.of_type(ATTEMPT_STARTED_EVENT)[0]["steps"], [])
        self.assertIs(report.termination_reason, TerminationReason.COMPLETED)

    def test_failed_start_publication_runs_and_writes_nothing(self):
        self.emitter.fail_on = {ATTEMPT_STARTED_EVENT}
        plan = plan_of(spec("a"))
        context = AttemptContext.for_plan(plan, execution_id="exec_unrecorded")

        exc = self.run_attempt(plan, context)

        self.assertIsInstance(exc, EventPublicationError)
        self.assertEqual(self.steps.calls, [])
        self.assertFalse((self.work_root / PLAN_ID).exists())
        report = context.report
        self.assertIs(report.termination_reason, TerminationReason.ORCHESTRATION_ERROR)
        self.assertEqual(report.unreached_step_ids, ["a"])
        # The report is not pushed through the emitter that just failed.
        self.assertEqual(self.types(), [ATTEMPT_STARTED_EVENT])
        assert report.publication_failure is not None
        self.assertEqual(
            report.publication_failure.details["publication_skipped"], True
        )

    def test_execution_id_that_cannot_name_records_is_refused(self):
        plan = plan_of(spec("a"))
        for execution_id in ("", "../elsewhere", "a/b"):
            with self.subTest(execution_id=execution_id):
                context = AttemptContext.for_plan(plan, execution_id=execution_id)

                exc = self.run_attempt(plan, context)

                self.assertIsInstance(exc, OrchestrationError)
                self.assertIn("cannot name spool records", str(exc))
                self.assertFalse(context.report.is_finished)
        self.assertEqual(self.emitter.events, [])
        self.assertEqual(self.steps.calls, [])


class TestBlockedStepEvents(EngineEventsTestCase):
    """A blocked step is published once, as soon as it is blocked."""

    def test_blocked_step_is_published_before_the_next_step_starts(self):
        self.steps.fail("a", PermanentStepError("broken"))

        self.engine.run(plan_of(spec("a"), spec("b", "a"), spec("c")))

        lifecycle = [
            (e["type"], e.get("step_id"))
            for e in self.emitter.events
            if e["type"] not in (ATTEMPT_STARTED_EVENT, ATTEMPT_REPORT_EVENT)
        ]
        self.assertEqual(
            lifecycle,
            [
                ("step.started", "a"),
                ("step.failed", "a"),
                (STEP_BLOCKED_EVENT, "b"),
                ("step.started", "c"),
                ("step.succeeded", "c"),
            ],
        )

    def test_blocked_chain_names_the_failure_at_its_root(self):
        self.steps.fail("a", PermanentStepError("broken"))

        self.engine.run(plan_of(spec("a"), spec("b", "a"), spec("c", "b")))

        blocked = {e["step_id"]: e for e in self.of_type(STEP_BLOCKED_EVENT)}
        self.assertEqual(set(blocked), {"b", "c"})
        self.assertEqual(blocked["b"]["direct_blockers"], ["a"])
        self.assertEqual(blocked["b"]["failed_ancestors"], ["a"])
        self.assertEqual(blocked["c"]["direct_blockers"], ["b"])
        self.assertEqual(blocked["c"]["failed_ancestors"], ["a"])
        self.assertEqual(blocked["c"]["step_name"], "c")
        self.assertNotIn("run_id", blocked["c"]["_spool_path"])

    def test_join_is_shown_blocked_at_once_and_its_diagnostics_settle_later(self):
        # A and B are independent prerequisites of join J. A fails first; B is
        # held running, then fails too.
        engine = self.spool_engine()
        self.steps.fail("A", PermanentStepError("lane A broke"))
        gate = Gate()

        def hold_then_fail(ctx: StepContext) -> None:
            gate.pass_through()
            raise PermanentStepError("lane B broke")

        self.steps.behaviors["B"] = hold_then_fail
        plan = plan_of(spec("A"), spec("B"), spec("J", "A", "B"))
        outcome: dict[str, Any] = {}

        def run() -> None:
            try:
                outcome["report"] = engine.run(plan)
            except BaseException as exc:  # surfaced by the assertions below
                outcome["error"] = exc

        worker = threading.Thread(target=run)
        worker.start()
        self.addCleanup(worker.join, WAIT)
        self.addCleanup(gate.release)
        self.assertTrue(gate.entered.wait(WAIT))

        # B is still running, and J already appears blocked by A.
        running = self.snapshot(plan)
        self.assertEqual(running["attempt"]["state"], "running")
        self.assertEqual(running["steps"]["B"]["state"], "step.started")
        self.assertEqual(running["steps"]["J"]["state"], STEP_BLOCKED_EVENT)
        self.assertEqual(running["steps"]["J"]["outcome"], "blocked")
        self.assertEqual(running["steps"]["J"]["direct_blockers"], ["A"])
        self.assertEqual(running["steps"]["J"]["failed_ancestors"], ["A"])

        gate.release()
        worker.join(WAIT)
        self.assertFalse(worker.is_alive())
        self.assertNotIn("error", outcome)
        report: AttemptReport = outcome["report"]

        # One step.blocked for J, filed without a run directory.
        join_dir = self.plan_dir(plan) / "J"
        blocked_name = record_filename(report.execution_id, STEP_BLOCKED_EVENT)
        self.assertEqual([p.name for p in join_dir.iterdir()], [blocked_name])
        blocked = self.spooled(join_dir / blocked_name)
        self.assertEqual(blocked["direct_blockers"], ["A"])
        self.assertFalse((self.work_root / PLAN_ID / "J").exists())

        # The report and the snapshot name both failures.
        self.assertEqual(report.direct_blockers["J"], ["A", "B"])
        self.assertEqual(report.failed_ancestors["J"], ["A", "B"])
        finished = self.snapshot(plan)
        self.assertEqual(finished["attempt"]["state"], "finished")
        self.assertEqual(finished["steps"]["J"]["direct_blockers"], ["A", "B"])
        self.assertEqual(finished["steps"]["J"]["failed_ancestors"], ["A", "B"])

        # Delivering J's earlier event again, late, narrows neither list.
        shutil.copy(join_dir / blocked_name, join_dir / "zz_redelivered.json")
        (join_dir / blocked_name).write_text(json.dumps(blocked), encoding="utf-8")
        replayed = self.snapshot(plan)
        self.assertEqual(replayed["steps"]["J"]["direct_blockers"], ["A", "B"])
        self.assertEqual(replayed["steps"]["J"]["failed_ancestors"], ["A", "B"])

        # Exactly one report, and it is what run() returned.
        reports = [
            self.spooled(p)
            for p in self.plan_dir(plan).glob("*.json")
            if self.spooled(p)["type"] == ATTEMPT_REPORT_EVENT
        ]
        self.assertEqual(len(reports), 1)
        self.assertEqual(reports[0]["report"], report.to_dict())

    def test_cancellation_after_blocking_keeps_the_blockers(self):
        plan = plan_of(spec("a"), spec("b", "a"), spec("c"), spec("d"))
        context = AttemptContext.for_plan(plan, execution_id="exec_cancelled")
        self.steps.fail("a", PermanentStepError("broken"))
        self.steps.behaviors["c"] = lambda ctx: context.request_cancellation()

        exc = self.run_attempt(plan, context)

        self.assertIsInstance(exc, AttemptCancelledError)
        report = context.report
        self.assertIs(report.termination_reason, TerminationReason.CANCELLED)
        self.assertIs(report.step_outcomes["b"], StepOutcome.BLOCKED)
        self.assertEqual(report.direct_blockers["b"], ["a"])
        self.assertEqual(report.failed_ancestors["b"], ["a"])
        # d had no failed prerequisite: it has no outcome, not a blocked one.
        self.assertEqual(report.unreached_step_ids, ["d"])
        self.assertEqual(
            [e["step_id"] for e in self.of_type(STEP_BLOCKED_EVENT)], ["b"]
        )

    def test_failure_to_publish_a_block_aborts_the_attempt(self):
        self.steps.fail("a", PermanentStepError("lane broke"))
        self.emitter.fail_on = {STEP_BLOCKED_EVENT}
        plan = plan_of(spec("a"), spec("b", "a"), spec("c"))
        context = AttemptContext.for_plan(plan, execution_id="exec_unobserved")

        exc = self.run_attempt(plan, context)

        self.assertIsInstance(exc, EventPublicationError)
        self.assertEqual(self.steps.calls, ["a"], "work continued unobservably")
        report = context.report
        self.assertIs(report.termination_reason, TerminationReason.ORCHESTRATION_ERROR)
        self.assertIs(report.step_outcomes["a"], StepOutcome.FAILED)
        self.assertEqual(report.unreached_step_ids, ["c"])
        # The step failure that led to the block stays reachable.
        assert exc is not None
        self.assertTrue(
            any(isinstance(e, PermanentStepError) for e in exception_chain(exc)),
            exception_chain(exc),
        )
        self.assertNotIn(ATTEMPT_REPORT_EVENT, self.types())

    def test_fail_fast_blocks_nothing(self):
        self.steps.fail("a", PermanentStepError("broken"))

        with self.assertRaises(PermanentStepError):
            self.engine.run(plan_of(spec("a"), spec("b", "a"), policy=FAIL_FAST))

        self.assertEqual(self.of_type(STEP_BLOCKED_EVENT), [])


class TestAttemptReportRecord(EngineEventsTestCase):
    """One report per attempt, filed under the attempt's own name."""

    def test_report_is_filed_under_the_attempt_with_its_correlation(self):
        engine = self.spool_engine()
        self.steps.fail("a", PermanentStepError("broken"))

        report = engine.run(plan_of(spec("a"), spec("b")))

        assert report is not None
        path = self.plan_dir(plan_of()) / record_filename(
            report.execution_id, ATTEMPT_REPORT_EVENT
        )
        published = self.spooled(path)
        self.assertEqual(published["report"], report.to_dict())
        self.assertEqual(
            {field: published[field] for field in CORRELATION_FIELDS},
            report.correlation.event_fields(),
        )


class TestExecutionIdSource(EngineEventsTestCase):
    """The engine orders attempts against the spool it writes, and no other."""

    def far_future_record_in(self, spool: Path) -> None:
        """Record an attempt at the plan far in the future, in a spool."""
        FileSpoolEmitter(spool).emit(
            {
                "type": ATTEMPT_STARTED_EVENT,
                "execution_id": FAR_FUTURE_ID,
                "_spool_path": {
                    "realm": REALM,
                    "plan_id": PLAN_ID,
                    "filename": record_filename(FAR_FUTURE_ID, ATTEMPT_STARTED_EVENT),
                },
            }
        )

    def test_default_allocator_reads_the_emitters_spool(self):
        self.far_future_record_in(self.spool)
        engine = self.spool_engine()

        report = engine.run(plan_of(spec("a")))

        assert report is not None
        history = engine.execution_ids.history
        self.assertIsInstance(history, SpoolAttemptHistory)
        assert isinstance(history, SpoolAttemptHistory)
        self.assertEqual(history.root, self.spool)
        self.assertGreater(report.execution_id, FAR_FUTURE_ID)

    def test_restarted_engine_with_its_clock_behind_still_orders_after(self):
        def allocator(clock: Clock) -> ExecutionIdAllocator:
            return ExecutionIdAllocator(SpoolAttemptHistory(self.spool), clock=clock)

        plan = plan_of(spec("a"))
        before = self.spool_engine(execution_ids=allocator(Clock(T0)))
        first = before.run(plan)

        after = self.spool_engine(
            execution_ids=allocator(Clock(T0 - timedelta(hours=1)))
        )
        second = after.run(plan)

        assert first is not None and second is not None
        self.assertGreater(second.execution_id, first.execution_id)
        self.assertEqual(
            self.snapshot(plan)["attempt"]["execution_id"], second.execution_id
        )

    def test_mocked_emitter_reaches_no_spool(self):
        # An unrelated spool the environment points at, holding a record that
        # would push this plan's IDs into the future if it were read.
        default_spool = self.root / "default_spool"
        self.far_future_record_in(default_spool)
        emitter = Mock(spec=EventEmitter)

        with (
            patch.dict(os.environ, {"YGG_EVENT_SPOOL": str(default_spool)}),
            patch(
                "yggdrasil.flow.events.emitter.resolve_event_spool",
                side_effect=AssertionError("resolved a default spool"),
            ),
            patch.object(
                SpoolAttemptHistory,
                "recorded_execution_ids",
                side_effect=AssertionError("read a spool"),
            ),
        ):
            engine = Engine(work_root=self.work_root, emitter=emitter)
            report = engine.run(plan_of(spec("a")))

        assert report is not None
        self.assertIsNone(engine.execution_ids.history)
        self.assertLess(report.execution_id, FAR_FUTURE_ID)
        self.assertEqual(
            {call.args[0]["execution_id"] for call in emitter.emit.call_args_list},
            {report.execution_id},
        )

    def test_unreadable_history_starts_no_attempt(self):
        class Unreadable:
            def recorded_execution_ids(self, realm: str, plan_id: str) -> list[str]:
                raise PermissionError("spool unreadable")

        engine = Engine(
            work_root=self.work_root,
            emitter=self.emitter,
            execution_ids=ExecutionIdAllocator(Unreadable()),
        )

        with self.assertRaises(OrchestrationError) as cm:
            engine.run(plan_of(spec("a")))

        self.assertIn("Allocating an execution ID", str(cm.exception))
        self.assertIsInstance(cm.exception.__cause__, PermissionError)
        self.assertEqual(self.emitter.events, [])
        self.assertEqual(self.steps.calls, [])

    def test_attempts_sharing_an_engine_share_its_order(self):
        engine = Engine(
            work_root=self.work_root,
            emitter=self.emitter,
            execution_ids=ExecutionIdAllocator(clock=Clock(T0)),
        )

        first = engine.run(plan_of(spec("a"), plan_id="plan_one"))
        second = engine.run(plan_of(spec("a"), plan_id="plan_two"))

        assert first is not None and second is not None
        self.assertEqual(execution_timestamp(first.execution_id), T0)
        self.assertEqual(
            execution_timestamp(second.execution_id), T0 + timedelta(microseconds=1)
        )


if __name__ == "__main__":
    unittest.main()
