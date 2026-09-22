"""Snapshots of real attempts, built from the spool the engine wrote.

Every test runs attempts through a real Engine and FileSpoolEmitter, then
builds the plan's snapshot the way the ops consumer does. Each one pins a way
the old per-step selection went wrong, or a way an ordering key could: a
snapshot must show one attempt, the most recently admitted, with every step
it planned, and must say how that attempt ended.
"""

import json
import shutil
import threading
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import Mock, patch

import lib.ops.consumer as consumer_module
from lib.ops.consumer import FileSpoolConsumer, build_plan_snapshot
from lib.ops.snapshot import (
    ATTEMPT_FINISHED,
    ATTEMPT_RUNNING,
    PROJECTION_ATTEMPT,
    PROJECTION_LEGACY,
    STATE_INTERRUPTED,
    STATE_PENDING,
    STATE_UNREACHED,
)
from tests.execution_support import (
    REALM,
    SCOPE,
    WAIT,
    Clock,
    ScriptedSteps,
    make_plan,
    spec,
)
from yggdrasil.core.engine import Engine
from yggdrasil.core.execution_ids import ExecutionIdAllocator, format_execution_id
from yggdrasil.flow.attempt import AttemptContext
from yggdrasil.flow.errors import PermanentStepError
from yggdrasil.flow.events.attempt_records import (
    ATTEMPT_REPORT_EVENT,
    ATTEMPT_STARTED_EVENT,
    STEP_BLOCKED_EVENT,
    SpoolAttemptHistory,
    record_filename,
)
from yggdrasil.flow.events.emitter import FileSpoolEmitter
from yggdrasil.flow.model import CONTINUE_INDEPENDENT_POLICY, FAIL_FAST_POLICY, Plan
from yggdrasil.flow.outcomes import AttemptReport, TerminationReason

CONTINUE = CONTINUE_INDEPENDENT_POLICY
FAIL_FAST = FAIL_FAST_POLICY
PLAN_ID = "pln_snapshots"
T0 = datetime(2026, 9, 21, 12, 5, tzinfo=UTC)


def plan_of(*specs, policy: str = CONTINUE) -> Plan:
    """A plan of scripted steps under this module's plan ID."""
    return make_plan(*specs, policy=policy, plan_id=PLAN_ID)


class SnapshotTestCase(unittest.TestCase):
    """An engine publishing to a temporary spool, and the snapshot it yields."""

    def setUp(self) -> None:
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        root = Path(temp_dir.name)
        self.spool = root / "spool"
        self.plan_dir = self.spool / REALM / PLAN_ID
        self.steps = ScriptedSteps()
        resolver = patch(
            "yggdrasil.core.engine.resolve_callable", return_value=self.steps.fn
        )
        resolver.start()
        self.addCleanup(resolver.stop)
        self.engine = Engine(
            work_root=root / "work", emitter=FileSpoolEmitter(self.spool)
        )

    def attempt(
        self,
        plan: Plan,
        *,
        plan_generation: str | None = None,
        run_token: int | None = None,
    ) -> AttemptContext:
        """Run one attempt with a captured identity; return its context."""
        context = AttemptContext.for_plan(
            plan,
            execution_id=self.engine.execution_ids.allocate(plan.realm, plan.plan_id),
            plan_generation=plan_generation,
            run_token=run_token,
        )
        try:
            self.engine._run_attempt(plan, context=context)
        except Exception:
            pass  # how it ended is in the report
        return context

    def snapshot(self) -> dict[str, Any]:
        """The plan's snapshot, as the consumer builds it."""
        return build_plan_snapshot(self.plan_dir, REALM, PLAN_ID)

    def states(self, snapshot: dict[str, Any]) -> dict[str, tuple[Any, Any]]:
        """Each step's (state, outcome) in a snapshot."""
        return {
            step_id: (entry["state"], entry["outcome"])
            for step_id, entry in snapshot["steps"].items()
        }

    def assert_shows(self, snapshot: dict[str, Any], report: AttemptReport) -> None:
        """Assert the snapshot shows the finished attempt behind report."""
        self.assertEqual(snapshot["projection"], PROJECTION_ATTEMPT)
        attempt = snapshot["attempt"]
        self.assertEqual(attempt["execution_id"], report.execution_id)
        self.assertEqual(attempt["state"], ATTEMPT_FINISHED)
        assert report.termination_reason is not None and report.outcome is not None
        self.assertEqual(attempt["termination_reason"], report.termination_reason.value)
        self.assertEqual(attempt["outcome"], report.outcome.value)
        self.assertEqual(attempt["counts"], report.counts)


class TestOneAttemptPerSnapshot(SnapshotTestCase):
    """A snapshot never mixes attempts, and never shows a stale one."""

    def assert_later_failure_imports_nothing(
        self, policy: str, downstream: tuple[Any, Any]
    ) -> None:
        """Run a successful attempt, then a failing one; check the snapshot."""
        # Everything succeeds first; then a, with new params, fails.
        self.attempt(plan_of(spec("a"), spec("b", "a"), spec("c", "b"), policy=policy))
        self.steps.fail("a", PermanentStepError("lane broke"))
        second = self.attempt(
            plan_of(spec("a", version=2), spec("b", "a"), spec("c", "b"), policy=policy)
        )

        snapshot = self.snapshot()

        self.assert_shows(snapshot, second.report)
        self.assertEqual(
            self.states(snapshot),
            {"a": ("step.failed", "failed"), "b": downstream, "c": downstream},
        )
        # b still has the first attempt's run in the spool; it is not shown.
        self.assertTrue(any((self.plan_dir / "b").iterdir()))
        self.assertIsNone(snapshot["steps"]["b"]["run_id"])

    def test_later_failed_continuation_does_not_import_earlier_successes(self):
        self.assert_later_failure_imports_nothing(CONTINUE, ("step.blocked", "blocked"))

    def test_later_fail_fast_failure_does_not_import_earlier_successes(self):
        self.assert_later_failure_imports_nothing(FAIL_FAST, (STATE_UNREACHED, None))

    def test_newer_running_attempt_is_shown_over_an_older_success(self):
        self.attempt(plan_of(spec("a"), spec("b", "a")))
        gate = self.steps.block("a")
        newer = plan_of(spec("a", version=2), spec("b", "a"))
        worker = threading.Thread(target=self.attempt, args=(newer,))
        worker.start()
        self.addCleanup(worker.join, WAIT)
        self.addCleanup(gate.release)
        self.assertTrue(gate.entered.wait(WAIT))

        running = self.snapshot()

        self.assertEqual(running["attempt"]["state"], ATTEMPT_RUNNING)
        self.assertEqual(
            self.states(running),
            {"a": ("step.started", None), "b": (STATE_PENDING, None)},
        )
        gate.release()
        worker.join(WAIT)
        # b's reuse is the newer attempt's own outcome; the older one ran b.
        self.assertEqual(
            self.states(self.snapshot()),
            {"a": ("step.succeeded", "succeeded"), "b": ("step.skipped", "reused")},
        )

    def test_replaying_an_old_attempt_changes_nothing(self):
        first = self.attempt(plan_of(spec("a"), spec("b")))
        self.steps.fail("b", PermanentStepError("broken"))
        second = self.attempt(plan_of(spec("a"), spec("b", version=2)))
        expected = self.snapshot()

        # Re-deliver every file the first attempt wrote, and a copy of its report.
        first_id = first.report.execution_id
        for path in self.spool.rglob("*.json"):
            if (
                json.loads(path.read_text(encoding="utf-8")).get("execution_id")
                == first_id
            ):
                path.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
        shutil.copy(
            self.plan_dir / record_filename(first_id, ATTEMPT_REPORT_EVENT),
            self.plan_dir / "zz_replayed_report.json",
        )

        replayed = self.snapshot()
        self.assert_shows(replayed, second.report)
        self.assertEqual(replayed["steps"], expected["steps"])

    def test_late_and_repeated_events_do_not_erase_a_failure(self):
        self.steps.fail("a", PermanentStepError("broken"))
        context = self.attempt(plan_of(spec("a")))
        (run_dir,) = (self.plan_dir / "a").iterdir()
        failed_file = next(run_dir.glob("*_step_failed.json"))
        failed = json.loads(failed_file.read_text(encoding="utf-8"))

        # Progress arriving after the failure, and the failure delivered twice.
        late = {
            **failed,
            "type": "step.progress",
            "seq": 99,
            "eid": "late",
            "progress": 80,
        }
        (run_dir / "0099_step_progress.json").write_text(
            json.dumps(late), encoding="utf-8"
        )
        shutil.copy(failed_file, run_dir / "9999_step_failed_copy.json")

        entry = self.snapshot()["steps"]["a"]
        self.assertEqual((entry["state"], entry["outcome"]), ("step.failed", "failed"))
        self.assertEqual(entry["error"], context.report.failures["a"].to_dict())

    def test_plan_level_records_of_every_attempt_coexist(self):
        contexts = [self.attempt(plan_of(spec("a"))) for _ in range(3)]

        self.assertEqual(
            sorted(p.name for p in self.plan_dir.glob("*.json")),
            sorted(
                record_filename(c.report.execution_id, event_type)
                for c in contexts
                for event_type in (ATTEMPT_STARTED_EVENT, ATTEMPT_REPORT_EVENT)
            ),
        )
        self.assert_shows(self.snapshot(), contexts[-1].report)


class TestAttemptEndings(SnapshotTestCase):
    """However an attempt ends, its snapshot says so and stops looking live."""

    def test_fail_fast_failure(self):
        self.steps.fail("a", PermanentStepError("broken"))
        context = self.attempt(
            plan_of(spec("a"), spec("b", "a"), spec("c"), policy=FAIL_FAST)
        )

        snapshot = self.snapshot()

        self.assert_shows(snapshot, context.report)
        self.assertEqual(
            snapshot["attempt"]["termination_reason"],
            TerminationReason.FAILED_FAST.value,
        )
        # Neither the dependent nor the unrelated step is relabelled blocked.
        self.assertEqual(
            self.states(snapshot),
            {
                "a": ("step.failed", "failed"),
                "b": (STATE_UNREACHED, None),
                "c": (STATE_UNREACHED, None),
            },
        )
        self.assertEqual(snapshot["steps"]["a"]["error"]["error"], "broken")

    def test_cooperative_cancellation(self):
        plan = plan_of(spec("a"), spec("b", "a"), spec("c"), spec("d"))
        context = AttemptContext.for_plan(
            plan, execution_id=self.engine.execution_ids.allocate(REALM, PLAN_ID)
        )
        self.steps.fail("a", PermanentStepError("broken"))
        self.steps.behaviors["c"] = lambda ctx: context.request_cancellation()
        with self.assertRaises(Exception):
            self.engine._run_attempt(plan, context=context)

        snapshot = self.snapshot()

        self.assert_shows(snapshot, context.report)
        self.assertEqual(
            snapshot["attempt"]["termination_reason"], TerminationReason.CANCELLED.value
        )
        self.assertEqual(
            self.states(snapshot),
            {
                "a": ("step.failed", "failed"),
                "b": ("step.blocked", "blocked"),
                "c": ("step.succeeded", "succeeded"),
                "d": (STATE_UNREACHED, None),
            },
        )
        self.assertEqual(snapshot["steps"]["b"]["direct_blockers"], ["a"])
        self.assertEqual(snapshot["steps"]["d"]["direct_blockers"], [])

    def test_preflight_rejection(self):
        context = self.attempt(plan_of(spec("a", "b"), spec("b", "a")))

        snapshot = self.snapshot()

        self.assert_shows(snapshot, context.report)
        self.assertEqual(
            snapshot["attempt"]["termination_reason"],
            TerminationReason.PREFLIGHT_REJECTED.value,
        )
        self.assertIn("cycle", snapshot["attempt"]["diagnostic"]["message"])
        self.assertEqual(snapshot["scope"], SCOPE)
        self.assertEqual(
            self.states(snapshot),
            {"a": (STATE_UNREACHED, None), "b": (STATE_UNREACHED, None)},
        )

    def test_step_failing_before_its_wrapper_ran_is_shown_failed(self):
        # Hashing a declared input is realm-controlled work done before the
        # @step wrapper runs, so the failure publishes no step event at all.
        data = self.spool.parent / "input.txt"
        data.write_text("reads", encoding="utf-8")
        unhashable = spec("a")
        unhashable.inputs = {"data": str(data)}
        with patch(
            "yggdrasil.core.engine.sha256_file",
            side_effect=PermissionError("input unreadable"),
        ):
            context = self.attempt(plan_of(unhashable, spec("b")))

        snapshot = self.snapshot()

        self.assert_shows(snapshot, context.report)
        self.assertFalse((self.plan_dir / "a").exists(), "a published an event")
        self.assertEqual(
            self.states(snapshot),
            {"a": ("step.failed", "failed"), "b": ("step.succeeded", "succeeded")},
        )
        self.assertEqual(
            snapshot["steps"]["a"]["error"], context.report.failures["a"].to_dict()
        )
        self.assertIsNone(snapshot["steps"]["a"]["run_id"])

    def test_success_event_the_engine_never_confirmed_is_not_shown_as_success(self):
        # a's wrapper publishes step.succeeded; writing its cache marker then
        # fails, which aborts the attempt with no outcome recorded for a.
        with patch(
            "yggdrasil.core.engine._replace_marker",
            side_effect=OSError("disk full"),
        ):
            context = self.attempt(plan_of(spec("a"), spec("b")))

        snapshot = self.snapshot()

        self.assert_shows(snapshot, context.report)
        self.assertEqual(
            snapshot["attempt"]["termination_reason"],
            TerminationReason.ORCHESTRATION_ERROR.value,
        )
        (run_dir,) = (self.plan_dir / "a").iterdir()
        self.assertTrue(any(run_dir.glob("*_step_succeeded.json")))
        self.assertEqual(
            self.states(snapshot),
            {"a": (STATE_INTERRUPTED, None), "b": (STATE_UNREACHED, None)},
        )
        self.assertEqual(snapshot["steps"]["a"]["run_id"], run_dir.name)

    def test_consumer_writes_the_snapshot_of_an_attempt_that_ran_no_step(self):
        self.attempt(plan_of(spec("a", "b"), spec("b", "a")))
        writer = Mock()

        FileSpoolConsumer(spool_root=self.spool, writer=writer).consume()

        writer.write.assert_called_once()
        plan_dir, snapshot = writer.write.call_args.args
        self.assertEqual(plan_dir, self.plan_dir)
        self.assertEqual(
            snapshot["attempt"]["termination_reason"],
            TerminationReason.PREFLIGHT_REJECTED.value,
        )


class TestAttemptOrder(SnapshotTestCase):
    """Attempts are ordered by execution ID, never by generation or token."""

    def test_regeneration_whose_generation_sorts_lower_is_still_shown(self):
        self.attempt(plan_of(spec("a")), plan_generation="f" * 32, run_token=4)
        newer = self.attempt(
            plan_of(spec("a", regenerated=True)), plan_generation="0" * 32, run_token=0
        )

        snapshot = self.snapshot()

        self.assert_shows(snapshot, newer.report)
        self.assertEqual(snapshot["attempt"]["plan_generation"], "0" * 32)
        self.assertEqual(snapshot["attempt"]["run_token"], 0)

    def test_attempts_of_one_request_are_ordered_and_kept_apart(self):
        # An interrupted attempt and its retry share generation and token.
        plan = plan_of(spec("a"), spec("b"))
        interrupted = AttemptContext.for_plan(
            plan,
            execution_id=self.engine.execution_ids.allocate(REALM, PLAN_ID),
            plan_generation="gen",
            run_token=0,
        )
        self.steps.behaviors["a"] = lambda ctx: interrupted.request_cancellation()
        with self.assertRaises(Exception):
            self.engine._run_attempt(plan, context=interrupted)
        self.steps.behaviors.clear()
        self.steps.fail("b", PermanentStepError("broken"))
        retry = self.attempt(plan, plan_generation="gen", run_token=0)

        snapshot = self.snapshot()

        self.assert_shows(snapshot, retry.report)
        self.assertEqual(
            self.states(snapshot),
            {"a": ("step.skipped", "reused"), "b": ("step.failed", "failed")},
        )
        # a's run is the retry's cache hit, not the interrupted attempt's run.
        (retry_run,) = (
            run
            for run in (self.plan_dir / "a").iterdir()
            if any("step_skipped" in p.name for p in run.iterdir())
        )
        self.assertEqual(snapshot["steps"]["a"]["run_id"], retry_run.name)


class TestReadingStaysProportional(SnapshotTestCase):
    """A snapshot reads what its attempt can have left behind, not all history.

    Reads are counted where the consumer opens every step-level file, so each
    test states exactly which files a snapshot may open.
    """

    def count_reads(self) -> list[Path]:
        """Record every step-level file the consumer opens from now on."""
        reads: list[Path] = []
        original = consumer_module._safe_load

        def counting(path: Path) -> dict[str, Any]:
            reads.append(path)
            return original(path)

        patcher = patch.object(consumer_module, "_safe_load", counting)
        patcher.start()
        self.addCleanup(patcher.stop)
        return reads

    def history(self, *step_ids: str, attempts: int = 3) -> None:
        """Run attempts that each execute every step afresh."""
        for version in range(attempts):
            self.attempt(plan_of(*(spec(s, version=version) for s in step_ids)))

    def test_rejected_attempt_reads_no_step_file(self):
        self.history("a", "b")
        self.attempt(plan_of(spec("a", "b"), spec("b", "a")))
        reads = self.count_reads()

        snapshot = self.snapshot()

        self.assertEqual(
            snapshot["attempt"]["termination_reason"],
            TerminationReason.PREFLIGHT_REJECTED.value,
        )
        self.assertEqual(reads, [])

    def test_blocked_steps_are_never_searched(self):
        self.history("a", "b", "c")
        self.steps.fail("a", PermanentStepError("broken"))
        context = self.attempt(
            plan_of(spec("a", version=9), spec("b", "a"), spec("c", "b"))
        )
        reads = self.count_reads()

        snapshot = self.snapshot()

        self.assertEqual(self.states(snapshot)["c"], ("step.blocked", "blocked"))
        # Each opens its blocked record only, none of its three earlier runs.
        blocked_name = record_filename(context.report.execution_id, STEP_BLOCKED_EVENT)
        for step_id in ("b", "c"):
            with self.subTest(step=step_id):
                step_dir = self.plan_dir / step_id
                self.assertEqual(
                    [p for p in reads if p.is_relative_to(step_dir)],
                    [step_dir / blocked_name],
                )

    def test_steps_outside_the_attempt_are_never_read(self):
        self.history("a", "retired")
        self.attempt(plan_of(spec("a", version=9)))
        reads = self.count_reads()

        snapshot = self.snapshot()

        self.assertEqual(set(snapshot["steps"]), {"a"})
        self.assertFalse(
            [p for p in reads if p.is_relative_to(self.plan_dir / "retired")]
        )

    def test_each_run_directory_is_opened_once_across_cycles(self):
        # a fails before its wrapper runs, so it has no run in the attempt,
        # and its search goes through every earlier run of a.
        self.history("a")
        data = self.spool.parent / "input.txt"
        data.write_text("reads", encoding="utf-8")
        unhashable = spec("a", version=9)
        unhashable.inputs = {"data": str(data)}
        with patch(
            "yggdrasil.core.engine.sha256_file", side_effect=PermissionError("denied")
        ):
            self.attempt(plan_of(unhashable))
        consumer = FileSpoolConsumer(spool_root=self.spool, writer=Mock())
        reads = self.count_reads()

        consumer.consume()
        first_cycle = [p for p in reads if p.is_relative_to(self.plan_dir / "a")]
        del reads[:]
        consumer.consume()
        second_cycle = [p for p in reads if p.is_relative_to(self.plan_dir / "a")]

        self.assertEqual(len(first_cycle), 3, "one first event per earlier run")
        self.assertEqual(second_cycle, [])


class TestHistoryAgreement(SnapshotTestCase):
    """The allocator orders new attempts against exactly what the consumer shows."""

    def write(self, name: str, content: object) -> None:
        """Write a file into the plan's spool directory directly."""
        self.plan_dir.mkdir(parents=True, exist_ok=True)
        text = content if isinstance(content, str) else json.dumps(content)
        (self.plan_dir / name).write_text(text, encoding="utf-8")

    def test_both_read_the_same_records(self):
        started = format_execution_id(T0, "a" * 32)
        report_only = format_execution_id(T0 + timedelta(minutes=1), "b" * 32)
        provisional = format_execution_id(T0 + timedelta(minutes=2), "c" * 32)
        self.write(
            record_filename(started, ATTEMPT_STARTED_EVENT),
            {"type": ATTEMPT_STARTED_EVENT, "execution_id": started},
        )
        self.write(
            record_filename(report_only, ATTEMPT_REPORT_EVENT),
            {"type": ATTEMPT_REPORT_EVENT, "execution_id": report_only},
        )
        self.write(
            "0e8f0c1d-uuid-named.json",
            {"type": ATTEMPT_REPORT_EVENT, "execution_id": provisional},
        )
        # Neither is an attempt record, for either reader.
        later = format_execution_id(T0 + timedelta(days=1), "d" * 32)
        self.write("draft.json", {"type": "plan.draft", "execution_id": later})
        self.write("bad.json", {"type": ATTEMPT_REPORT_EVENT, "execution_id": "a/b"})
        self.write("broken.json", "{not json")

        with self.assertLogs(level="WARNING"):
            history = SpoolAttemptHistory(self.spool).recorded_execution_ids(
                REALM, PLAN_ID
            )
        shown = self.snapshot()["attempt"]["execution_id"]

        self.assertEqual(set(history), {started, report_only, provisional})
        self.assertEqual(shown, provisional)

    def test_new_attempt_after_an_upgrade_is_shown_despite_a_clock_behind(self):
        # History from before attempt-start records: one report, named after
        # its event ID, and the uncorrelated step events of the time.
        provisional = format_execution_id(T0, "a" * 32)
        self.write(
            "3f2b9c1e-5a6d-4f1e-9b2a-0c7d8e9f1a2b.json",
            {
                "type": ATTEMPT_REPORT_EVENT,
                "execution_id": provisional,
                "scope": SCOPE,
                "report": {
                    "execution_id": provisional,
                    "step_ids": ["a"],
                    "step_outcomes": {"a": "succeeded"},
                    "termination_reason": "completed",
                },
            },
        )
        self.assertEqual(self.snapshot()["attempt"]["execution_id"], provisional)
        # The first engine of the upgraded version, started with its clock
        # behind that report.
        self.engine = Engine(
            work_root=self.spool.parent / "work",
            emitter=FileSpoolEmitter(self.spool),
            execution_ids=ExecutionIdAllocator(
                SpoolAttemptHistory(self.spool),
                clock=Clock(T0 - timedelta(hours=1)),
            ),
        )
        self.steps.fail("a", PermanentStepError("broken"))

        context = self.attempt(plan_of(spec("a")))

        self.assertGreater(context.report.execution_id, provisional)
        snapshot = self.snapshot()
        self.assert_shows(snapshot, context.report)
        self.assertEqual(self.states(snapshot), {"a": ("step.failed", "failed")})

    def test_attempt_with_a_caller_built_id_never_hides_an_allocated_one(self):
        # "exec_sched_plan" sorts above any allocated ID by name alone.
        plan = plan_of(spec("a"))
        custom = AttemptContext.for_plan(plan, execution_id="exec_sched_plan")
        self.engine._run_attempt(plan, context=custom)

        allocated = self.attempt(plan)

        self.assertEqual(
            self.snapshot()["attempt"]["execution_id"], allocated.report.execution_id
        )

    def test_id_resembling_an_allocated_one_never_hides_a_newer_attempt(self):
        november = datetime(2026, 11, 1, tzinfo=UTC)
        for index, malformed in enumerate(
            (
                # Read on its own, the unpadded date is 1 November, yet the ID
                # sorts above every ID allocated on that day.
                "exec_2026111T000000000000Z",
                "exec_20991231T235959999999Z",  # no suffix
                "exec_2026111T0000000000000Z_" + "f" * 32,  # timestamp widths off
            )
        ):
            with self.subTest(malformed=malformed):
                plan = make_plan(
                    spec("a"), policy=CONTINUE, plan_id=f"pln_malformed_{index}"
                )
                self.engine._run_attempt(
                    plan, context=AttemptContext.for_plan(plan, execution_id=malformed)
                )
                self.engine = Engine(
                    work_root=self.spool.parent / "work",
                    emitter=FileSpoolEmitter(self.spool),
                    execution_ids=ExecutionIdAllocator(
                        SpoolAttemptHistory(self.spool), clock=Clock(november)
                    ),
                )

                allocated = self.attempt(plan)

                plan_dir = self.spool / REALM / plan.plan_id
                shown = build_plan_snapshot(plan_dir, REALM, plan.plan_id)
                self.assertEqual(
                    shown["attempt"]["execution_id"], allocated.report.execution_id
                )


class TestLegacyHistories(SnapshotTestCase):
    """Uncorrelated events are a labelled projection, never part of an attempt."""

    def write_legacy_run(self, step_id: str, run_id: str, *types: str) -> None:
        """Write a pre-correlation run of one step."""
        run_dir = self.plan_dir / step_id / run_id
        run_dir.mkdir(parents=True)
        for seq, type_ in enumerate(types, start=1):
            (run_dir / f"{seq:04d}_{type_.replace('.', '_')}.json").write_text(
                json.dumps(
                    {
                        "type": type_,
                        "seq": seq,
                        "scope": SCOPE,
                        "step_id": step_id,
                        "step_name": step_id,
                    }
                ),
                encoding="utf-8",
            )

    def test_history_without_attempt_records_is_a_legacy_projection(self):
        self.write_legacy_run("a", "run_20250101T000000000000Z_aaaaaa", "step.started")
        self.write_legacy_run(
            "a", "run_20250102T000000000000Z_bbbbbb", "step.started", "step.succeeded"
        )

        snapshot = self.snapshot()

        self.assertEqual(snapshot["projection"], PROJECTION_LEGACY)
        self.assertIsNone(snapshot["attempt"])
        self.assertEqual(snapshot["scope"], SCOPE)
        self.assertEqual(
            snapshot["steps"]["a"]["run_id"], "run_20250102T000000000000Z_bbbbbb"
        )
        self.assertEqual(snapshot["steps"]["a"]["state"], "step.succeeded")

    def test_correlated_attempt_is_shown_without_any_legacy_step(self):
        # Legacy runs that even sort after the attempt's own.
        self.write_legacy_run(
            "a", "run_29990101T000000000000Z_legacy", "step.started", "step.succeeded"
        )
        self.write_legacy_run("x", "run_29990101T000000000000Z_legacy", "step.failed")
        self.steps.fail("a", PermanentStepError("broken"))
        context = self.attempt(plan_of(spec("a"), spec("b")))

        snapshot = self.snapshot()

        self.assert_shows(snapshot, context.report)
        self.assertEqual(set(snapshot["steps"]), {"a", "b"})
        self.assertEqual(
            self.states(snapshot),
            {"a": ("step.failed", "failed"), "b": ("step.succeeded", "succeeded")},
        )
        self.assertNotEqual(
            snapshot["steps"]["a"]["run_id"], "run_29990101T000000000000Z_legacy"
        )


if __name__ == "__main__":
    unittest.main()
