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
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import Mock, patch

from lib.ops.consumer import FileSpoolConsumer, build_plan_snapshot
from lib.ops.snapshot import (
    ATTEMPT_FINISHED,
    ATTEMPT_RUNNING,
    PROJECTION_ATTEMPT,
    PROJECTION_LEGACY,
    STATE_PENDING,
    STATE_UNREACHED,
)
from tests.execution_support import (
    REALM,
    SCOPE,
    WAIT,
    ScriptedSteps,
    make_plan,
    spec,
)
from yggdrasil.core.engine import Engine
from yggdrasil.flow.attempt import AttemptContext
from yggdrasil.flow.errors import PermanentStepError
from yggdrasil.flow.events.attempt_records import (
    ATTEMPT_REPORT_EVENT,
    ATTEMPT_STARTED_EVENT,
    record_filename,
)
from yggdrasil.flow.events.emitter import FileSpoolEmitter
from yggdrasil.flow.model import CONTINUE_INDEPENDENT_POLICY, FAIL_FAST_POLICY, Plan
from yggdrasil.flow.outcomes import AttemptReport, TerminationReason

CONTINUE = CONTINUE_INDEPENDENT_POLICY
FAIL_FAST = FAIL_FAST_POLICY
PLAN_ID = "pln_snapshots"


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
