"""Tests for reducing spooled events to a snapshot, without a filesystem.

These pin the selection and precedence rules on hand-built events: which
attempt is shown, which event a step's state is read from, and which source
wins when an attempt's events and its report disagree.
"""

import unittest
from typing import Any

from lib.ops.snapshot import (
    ATTEMPT_FINISHED,
    ATTEMPT_RUNNING,
    STATE_INTERRUPTED,
    STATE_PENDING,
    STATE_UNKNOWN,
    STATE_UNREACHED,
    AttemptEvents,
    SpooledEvent,
    StepRun,
    fold_run,
    order_events,
    project_attempt,
    project_legacy,
    select_execution,
)
from yggdrasil.flow.events.attempt_records import (
    ATTEMPT_REPORT_EVENT,
    ATTEMPT_STARTED_EVENT,
    STEP_BLOCKED_EVENT,
)

OLDER = "exec_20260921T120500000000Z_" + "f" * 32
NEWER = "exec_20260921T120600000000Z_" + "0" * 32


def event(type_: str, seq: int | None = None, **fields: Any) -> dict[str, Any]:
    """An event of one type, with an optional sequence number."""
    built: dict[str, Any] = {"type": type_, **fields}
    if seq is not None:
        built["seq"] = seq
    return built


def run(run_id: str, *events: dict[str, Any]) -> StepRun:
    """A run whose events are named in the order given."""
    return StepRun(
        run_id, [SpooledEvent(f"{i:04d}_event.json", e) for i, e in enumerate(events)]
    )


def started(execution_id: str, *step_ids: str, **fields: Any) -> dict[str, Any]:
    """An attempt-start record for the given steps."""
    return {
        "type": ATTEMPT_STARTED_EVENT,
        "execution_id": execution_id,
        "steps": [
            {"step_id": s, "step_name": f"name_{s}", "deps": []} for s in step_ids
        ],
        **fields,
    }


def report_event(execution_id: str, **report: Any) -> dict[str, Any]:
    """An attempt-report event carrying the given report fields."""
    return {
        "type": ATTEMPT_REPORT_EVENT,
        "execution_id": execution_id,
        "report": {"execution_id": execution_id, **report},
    }


class TestSelection(unittest.TestCase):
    """The newest admitted attempt is selected, finished or not."""

    def test_highest_execution_id_wins_whatever_else_the_records_say(self):
        records = [
            # The older attempt finished and succeeded, under a generation
            # that sorts above the newer attempt's, with a higher run token.
            started(OLDER, plan_generation="ffff", run_token=7),
            report_event(OLDER, outcome="succeeded"),
            started(NEWER, plan_generation="0000", run_token=0),
        ]

        self.assertEqual(select_execution(records), NEWER)

    def test_only_correlated_attempt_records_count(self):
        records = [
            {"type": "step.succeeded", "execution_id": NEWER},
            {"type": ATTEMPT_STARTED_EVENT},
            {"type": ATTEMPT_STARTED_EVENT, "execution_id": ""},
            {"type": "plan.draft", "execution_id": NEWER},
        ]

        self.assertIsNone(select_execution(records))
        self.assertEqual(select_execution([*records, started(OLDER)]), OLDER)


class TestOneRun(unittest.TestCase):
    """A step's state comes from its effective event."""

    def test_sequence_numbers_order_events_before_file_names(self):
        events = [
            SpooledEvent("a.json", event("step.failed", seq=3)),
            SpooledEvent("b.json", event("step.started", seq=1)),
            SpooledEvent("0000_trace.json", event("data_access.write.succeeded")),
            SpooledEvent("c.json", event("step.progress", seq=2)),
        ]

        self.assertEqual(
            [e["type"] for e in order_events(events)],
            [
                "step.started",
                "step.progress",
                "step.failed",
                "data_access.write.succeeded",
            ],
        )

    def test_copies_of_one_event_count_once(self):
        events = [
            SpooledEvent("0001.json", event("step.started", seq=1, eid="e1")),
            SpooledEvent("zz_copy.json", event("step.started", seq=1, eid="e1")),
        ]

        self.assertEqual(len(order_events(events)), 1)

    def test_late_non_terminal_events_do_not_replace_a_failure(self):
        failed = run(
            "run_1",
            event("step.started", seq=1),
            event("step.failed", seq=2, error="boom", kind="permanent"),
            event("step.progress", seq=3, progress=40),
            event("step.artifact", seq=4, artifact={"key": "x"}),
            event("step.retry_unimplemented"),
        )

        status = fold_run(failed, None)

        self.assertEqual(status.state, "step.failed")
        self.assertEqual(status.outcome, "failed")
        self.assertEqual(status.progress, 0)
        self.assertEqual(
            status.error,
            {"error": "boom", "kind": "permanent", "code": None, "advice": None},
        )

    def test_a_failure_replaces_an_earlier_success_but_not_the_reverse(self):
        success_then_failure = run(
            "run_1",
            event("step.succeeded", seq=2),
            event("step.failed", seq=3),
        )
        failure_then_success = run(
            "run_1",
            event("step.failed", seq=2),
            event("step.succeeded", seq=3),
        )

        self.assertEqual(fold_run(success_then_failure, None).outcome, "failed")
        self.assertEqual(fold_run(failure_then_success, None).outcome, "failed")

    def test_write_traces_never_set_a_steps_state(self):
        traced = run(
            "run_1",
            event("step.started", seq=1),
            event("data_access.write.succeeded"),
        )

        self.assertEqual(fold_run(traced, None).state, "step.started")

    def test_only_the_projected_attempts_events_count(self):
        mixed = run(
            "run_1",
            event("step.started", seq=1, execution_id=NEWER),
            # Copied in from another attempt, or uncorrelated.
            event("step.succeeded", seq=2, execution_id=OLDER),
            event("step.succeeded", seq=3),
        )

        self.assertEqual(fold_run(mixed, NEWER).state, "step.started")
        self.assertEqual(fold_run(mixed, None).state, "step.succeeded")

    def test_success_and_reuse_are_complete(self):
        for type_, outcome in (
            ("step.succeeded", "succeeded"),
            ("step.skipped", "reused"),
        ):
            with self.subTest(type=type_):
                status = fold_run(run("run_1", event(type_, seq=1)), None)
                self.assertEqual(status.outcome, outcome)
                self.assertEqual(status.progress, 100)


class TestAttemptProjection(unittest.TestCase):
    """A running attempt shows its events; a finished one defers to its report."""

    def test_running_attempt_shows_every_planned_step(self):
        attempt = AttemptEvents(
            execution_id=NEWER,
            started=started(NEWER, "a", "b", "c", run_token=2),
            runs={"a": run("run_a", event("step.started", seq=1, execution_id=NEWER))},
            blocked={
                "c": event(
                    STEP_BLOCKED_EVENT,
                    execution_id=NEWER,
                    step_name="name_c",
                    direct_blockers=["x"],
                    failed_ancestors=["x"],
                )
            },
        )

        summary, steps = project_attempt(attempt)

        self.assertEqual(summary["state"], ATTEMPT_RUNNING)
        self.assertEqual(summary["run_token"], 2)
        self.assertIsNone(summary["outcome"])
        self.assertEqual(list(steps), ["a", "b", "c"])
        self.assertEqual(steps["a"]["state"], "step.started")
        self.assertEqual(steps["b"]["state"], STATE_PENDING)
        self.assertEqual(steps["b"]["step_name"], "name_b")
        self.assertIsNone(steps["b"]["outcome"])
        self.assertEqual(steps["c"]["state"], STEP_BLOCKED_EVENT)
        self.assertEqual(steps["c"]["outcome"], "blocked")
        self.assertEqual(steps["c"]["direct_blockers"], ["x"])

    def test_report_settles_blockers_an_event_published_earlier(self):
        attempt = AttemptEvents(
            execution_id=NEWER,
            started=started(NEWER, "a", "b", "j"),
            blocked={
                "j": event(
                    STEP_BLOCKED_EVENT,
                    execution_id=NEWER,
                    direct_blockers=["a"],
                    failed_ancestors=["a"],
                )
            },
            report=report_event(
                NEWER,
                termination_reason="completed",
                outcome="failed",
                step_outcomes={"a": "failed", "b": "failed", "j": "blocked"},
                direct_blockers={"j": ["a", "b"]},
                failed_ancestors={"j": ["a", "b"]},
            ),
        )

        summary, steps = project_attempt(attempt)

        self.assertEqual(summary["state"], ATTEMPT_FINISHED)
        self.assertEqual(steps["j"]["direct_blockers"], ["a", "b"])
        self.assertEqual(steps["j"]["failed_ancestors"], ["a", "b"])

    def test_unconfirmed_terminal_event_does_not_make_an_outcome(self):
        # The attempt aborted after a's success event but before a's success
        # was finalized; b was never reached.
        attempt = AttemptEvents(
            execution_id=NEWER,
            started=started(NEWER, "a", "b"),
            runs={
                "a": run(
                    "run_a",
                    event("step.started", seq=1, execution_id=NEWER, fingerprint="f"),
                    event(
                        "step.succeeded",
                        seq=2,
                        execution_id=NEWER,
                        artifacts=[{"key": "x"}],
                    ),
                )
            },
            report=report_event(
                NEWER,
                termination_reason="orchestration_error",
                outcome="failed",
                step_outcomes={},
                diagnostic={
                    "message": "marker write failed",
                    "details": {"running_step_id": "a"},
                },
            ),
        )

        summary, steps = project_attempt(attempt)

        self.assertEqual(summary["termination_reason"], "orchestration_error")
        self.assertEqual(
            (steps["a"]["state"], steps["a"]["outcome"]), (STATE_INTERRUPTED, None)
        )
        # What the run had reached is kept; the unconfirmed success is not.
        self.assertEqual(steps["a"]["run_id"], "run_a")
        self.assertEqual(steps["a"]["fingerprint"], "f")
        self.assertEqual(steps["a"]["artifacts"], [])
        self.assertEqual(steps["a"]["progress"], 0)
        self.assertEqual(steps["b"]["state"], STATE_UNREACHED)

    def test_step_running_when_the_attempt_ended_is_interrupted_without_events(self):
        # Preparing a's execution context failed: it was running, but its
        # @step wrapper never published anything.
        attempt = AttemptEvents(
            execution_id=NEWER,
            started=started(NEWER, "a", "b"),
            report=report_event(
                NEWER,
                termination_reason="orchestration_error",
                step_outcomes={},
                diagnostic={"details": {"running_step_id": "a"}},
            ),
        )

        _, steps = project_attempt(attempt)

        self.assertEqual(steps["a"]["state"], STATE_INTERRUPTED)
        self.assertEqual(steps["b"]["state"], STATE_UNREACHED)

    def test_report_decides_the_state_of_every_step_with_an_outcome(self):
        # Only the report survives: no start record, no step events.
        attempt = AttemptEvents(
            execution_id=NEWER,
            report=report_event(
                NEWER,
                step_ids=["ok", "cached", "bad", "stuck", "never"],
                step_outcomes={
                    "ok": "succeeded",
                    "cached": "reused",
                    "bad": "failed",
                    "stuck": "blocked",
                },
                failures={"bad": {"step_id": "bad", "error": "boom"}},
                direct_blockers={"stuck": ["bad"]},
                failed_ancestors={"stuck": ["bad"]},
            ),
        )

        _, steps = project_attempt(attempt)

        self.assertEqual(
            {
                sid: (e["state"], e["outcome"], e["progress"])
                for sid, e in steps.items()
            },
            {
                "ok": ("step.succeeded", "succeeded", 100),
                "cached": ("step.skipped", "reused", 100),
                "bad": ("step.failed", "failed", 0),
                "stuck": (STEP_BLOCKED_EVENT, "blocked", 0),
                "never": (STATE_UNREACHED, None, 0),
            },
        )
        self.assertEqual(steps["bad"]["error"], {"step_id": "bad", "error": "boom"})
        self.assertEqual(steps["stuck"]["direct_blockers"], ["bad"])
        self.assertIsNone(steps["ok"]["run_id"])

    def test_report_failure_overrides_a_run_left_without_its_terminal_event(self):
        attempt = AttemptEvents(
            execution_id=NEWER,
            started=started(NEWER, "a", "b"),
            runs={
                "a": run(
                    "run_a",
                    event(
                        "step.started",
                        seq=1,
                        execution_id=NEWER,
                        fingerprint="fa",
                        step_name="A",
                        ts="t1",
                    ),
                    event("step.progress", seq=2, execution_id=NEWER, progress=60),
                ),
                "b": run(
                    "run_b",
                    event("step.started", seq=1, execution_id=NEWER, fingerprint="fb"),
                ),
            },
            report=report_event(
                NEWER,
                step_outcomes={"a": "failed", "b": "succeeded"},
                failures={"a": {"step_id": "a", "error": "lost"}},
            ),
        )

        _, steps = project_attempt(attempt)

        self.assertEqual(
            {k: steps["a"][k] for k in ("state", "outcome", "run_id", "fingerprint")},
            {
                "state": "step.failed",
                "outcome": "failed",
                "run_id": "run_a",
                "fingerprint": "fa",
            },
        )
        self.assertEqual(steps["a"]["progress"], 0)
        self.assertEqual(steps["a"]["error"], {"step_id": "a", "error": "lost"})
        self.assertEqual(
            (steps["b"]["state"], steps["b"]["progress"], steps["b"]["run_id"]),
            ("step.succeeded", 100, "run_b"),
        )

    def test_report_without_a_start_record_still_describes_the_attempt(self):
        attempt = AttemptEvents(
            execution_id=NEWER,
            report=report_event(
                NEWER,
                step_ids=["a", "b"],
                plan_generation="gen",
                step_outcomes={"a": "failed"},
                failures={"a": {"step_id": "a", "error": "boom"}},
            ),
        )

        summary, steps = project_attempt(attempt)

        self.assertEqual(summary["plan_generation"], "gen")
        self.assertIsNone(summary["execution_authority"])
        self.assertEqual(list(steps), ["a", "b"])
        self.assertEqual(steps["a"]["error"], {"step_id": "a", "error": "boom"})
        self.assertEqual(steps["b"]["state"], STATE_UNREACHED)


class TestLegacyProjection(unittest.TestCase):
    """Uncorrelated runs are shown per step, as before."""

    def test_each_steps_run_is_reduced_on_its_own(self):
        steps = project_legacy(
            {
                "a": run("run_1", event("step.succeeded", step_name="A")),
                "b": run("run_2", event("data_access.write.succeeded")),
            }
        )

        self.assertEqual(steps["a"]["state"], "step.succeeded")
        self.assertEqual(steps["a"]["run_id"], "run_1")
        self.assertEqual(steps["a"]["step_name"], "A")
        self.assertEqual(steps["b"]["state"], STATE_UNKNOWN)


if __name__ == "__main__":
    unittest.main()
