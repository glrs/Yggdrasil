"""How a stored snapshot reads: the attempt, each step's status and observations.

These pin what a program showing a plan's execution gets from the reader,
case by case. Each input is a regression-corpus snapshot, or one changed in
the field the case is about, or the snapshot of a real attempt for a
scenario the corpus lacks. A changed snapshot is deliberate input, not a
claim about what Yggdrasil writes.
"""

from __future__ import annotations

import dataclasses
import unittest
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from tests.contract.read_support import HistoryTestCase, ReaderTestCase, snapshot
from tests.contract.support import ABSENT, changed
from tests.execution_support import spec
from yggdrasil.contract.read import (
    ObservedStep,
    Recorded,
    SnapshotRead,
    StepRef,
    read_snapshot,
    step_status,
)
from yggdrasil.flow.errors import PermanentStepError, TransientStepError
from yggdrasil.flow.model import CONTINUE_INDEPENDENT_POLICY
from yggdrasil.flow.step import StepContext

AT = datetime(2026, 10, 22, 10, 0, 0, tzinfo=UTC)


def read(document: Any) -> SnapshotRead:
    """Read a snapshot for the plan and realm it names."""
    return read_snapshot(document, plan_id=document["plan_id"], realm=document["realm"])


def steps_of(result: SnapshotRead) -> dict[str, ObservedStep]:
    """Return a read's observed steps by step ID."""
    assert result.steps is not None
    return {step.step_id: step for step in result.steps}


def paths_and_codes(result: SnapshotRead) -> list[tuple[str, str]]:
    """Return each problem of a read as (path, code), in reported order."""
    return [(problem.path, problem.code) for problem in result.problems]


def fails(error: Exception) -> Callable[[StepContext], None]:
    """Return a step body that raises error."""

    def body(ctx: StepContext) -> None:
        raise error

    return body


class TestStepStatus(ReaderTestCase):
    """The status rule, on its own."""

    def test_step_status_without_contradiction(self):
        rows = (
            ("step.succeeded", "succeeded", "succeeded"),
            ("step.skipped", "reused", "reused"),
            ("step.failed", "failed", "failed"),
            ("step.blocked", "blocked", "blocked"),
            ("pending", None, "pending"),
            ("interrupted", None, "interrupted"),
            ("unreached", None, "unreached"),
            # Any lifecycle event of a step without an outcome yet: recorded
            # past activity, a realm's own events included.
            ("step.started", None, "started"),
            ("step.progress", None, "started"),
            ("step.artifact", None, "started"),
            ("step.checkpoint_written", None, "started"),
            # A legacy step run with no lifecycle event.
            ("unknown", None, "unknown"),
            # Nothing recognised.
            ("queued", None, "unknown"),
            ("data_access.write_succeeded", None, "unknown"),
            (None, None, "unknown"),
            (7, None, "unknown"),
            ("step.succeeded", "skipped", "unknown"),
            # A state that is not text is never looked up as if it were a word.
            ([], None, "unknown"),
            ({}, None, "unknown"),
            (["step.started"], None, "unknown"),
            ({"type": "step.started"}, None, "unknown"),
            (True, None, "unknown"),
            (1.5, None, "unknown"),
            # Nor is an outcome that is not text.
            ("step.succeeded", [], "unknown"),
            ("step.succeeded", {"outcome": "succeeded"}, "unknown"),
            ([], {}, "unknown"),
        )
        for index, (state, outcome, status) in enumerate(rows):
            with self.subTest(row=index):
                result = step_status(state, outcome)

                self.assertEqual((result.status, result.contradiction), (status, None))

    def test_a_contradictory_record_is_unknown_never_started_or_success(self):
        rows = (
            # A terminal event without the outcome it records.
            ("step.succeeded", None),
            ("step.failed", None),
            ("step.skipped", None),
            ("step.blocked", None),
            # A recognised outcome beside a state that does not record it.
            ("step.failed", "succeeded"),
            ("step.started", "succeeded"),
            ("pending", "failed"),
            ("interrupted", "succeeded"),
            (None, "blocked"),
            ([], "succeeded"),
            ({"type": "step.failed"}, "failed"),
        )
        for index, (state, outcome) in enumerate(rows):
            with self.subTest(row=index):
                result = step_status(state, outcome)

                self.assertEqual(result.status, "unknown")
                self.assertIsNotNone(result.contradiction)

    def test_a_contradictory_step_record_names_both_recorded_words(self):
        progress = step_status("step.progress", "succeeded").contradiction
        failed = step_status("step.failed", None).contradiction

        self.assertEqual(
            progress, "the outcome 'succeeded' contradicts the state 'step.progress'"
        )
        self.assertEqual(
            failed,
            "the state 'step.failed' is a terminal event, but the outcome is null",
        )


class TestRunningAttempt(ReaderTestCase):
    """An attempt observed while it ran, after one branch failed."""

    def setUp(self) -> None:
        self.running = snapshot("running_with_failure")

    def test_a_contradiction_is_reported_on_the_step(self):
        document = changed(self.running, "steps.lane1_demux.outcome", None)

        result = read(document)

        self.assertEqual(steps_of(result)["lane1_demux"].status, "unknown")
        self.assertEqual(result.projection, "attempt")
        self.assertEqual(
            paths_and_codes(result),
            [("steps.lane1_demux", "step_record_contradiction")],
        )

    def test_running_attempt_identity_and_state(self):
        result = read(self.running)

        attempt = result.attempt
        assert attempt is not None and result.written_at is not None
        self.assertEqual(result.projection, "attempt")
        self.assertIs(attempt.settled, False)
        self.assertEqual(result.written_at.value, AT)
        self.assertEqual(attempt.execution_id, "20261022T100000000000Z_0001")
        self.assertEqual(
            attempt.plan_generation.value, "2362b79a8685f22a55cc75f81395bf55"
        )
        self.assertEqual(attempt.run_token.value, 0)
        self.assertEqual(attempt.state.value, "running")
        self.assertEqual(attempt.failure_policy.value, "continue_independent")
        self.assertEqual(attempt.execution_authority.value, "daemon")
        self.assertEqual(attempt.started_at.value, AT)
        # Nothing is recorded about how it ended, so nothing is said.
        for field in ("ended_at", "termination_reason", "outcome", "is_drained"):
            self.assertEqual(getattr(attempt, field), Recorded(None, None, True), field)
        self.assertIsNone(attempt.counts)
        self.assertIsNone(attempt.diagnostic)

    def test_running_attempt_steps(self):
        result = read(self.running)

        assert result.steps is not None
        self.assertEqual(
            [(step.step_id, step.status) for step in result.steps],
            [
                ("lane1_demux", "failed"),
                ("lane1_upload", "blocked"),
                ("lane2_demux", "started"),
                ("lane2_upload", "pending"),
            ],
        )
        self.assertEqual(result.problems, ())

    def test_the_other_branch_progressed_after_the_failure(self):
        """The healthy branch started after the other failed; its next step has no time.

        The corpus's clock holds every event at one instant, so the order of
        the events is not shown by their times here.
        """
        steps = steps_of(read(self.running))

        self.assertEqual(steps["lane2_demux"].recorded_state, "step.started")
        self.assertEqual(steps["lane2_demux"].recorded_at, AT)
        self.assertEqual(steps["lane1_demux"].recorded_at, AT)
        self.assertIsNone(steps["lane2_upload"].recorded_at)

    def test_a_running_attempts_error_comes_from_the_event(self):
        error = steps_of(read(self.running))["lane1_demux"].error

        assert error is not None
        self.assertEqual(error.message, "lane 1 is listed twice in the sample sheet")
        self.assertEqual(
            (error.kind.value, error.code), ("permanent", "SAMPLESHEET_DUPLICATE_LANE")
        )
        self.assertEqual(
            error.advice, "Correct the sample sheet, then regenerate the plan."
        )
        self.assertIsNone(error.error_type)

    def test_blockers_of_an_unsettled_attempt(self):
        """The failed steps upstream are "not established yet" until the report closes."""
        steps = steps_of(read(self.running))

        blocked = steps["lane1_upload"]
        self.assertEqual(blocked.direct_blockers, (StepRef("lane1_demux", True),))
        self.assertIsNone(blocked.failed_upstream)
        self.assertIsNone(steps["lane1_demux"].direct_blockers)
        self.assertIsNone(steps["lane2_demux"].direct_blockers)

    def test_an_end_recorded_beside_a_running_state_is_returned_as_recorded(self):
        """Yggdrasil's projection does not write this; a store can hold it."""
        document = changed(self.running, "attempt.outcome", "failed")
        document = changed(document, "attempt.ended_at", "2026-10-22T10:05:30Z")

        result = read(document)

        attempt = result.attempt
        assert attempt is not None
        self.assertEqual(attempt.state.value, "running")
        self.assertEqual(attempt.outcome.value, "failed")
        self.assertEqual(
            attempt.ended_at.value, datetime(2026, 10, 22, 10, 5, 30, tzinfo=UTC)
        )
        # The state alone says whether the results are final: they are not.
        self.assertIs(attempt.settled, False)
        self.assertIsNone(steps_of(result)["lane1_upload"].failed_upstream)


class TestProgress(ReaderTestCase):
    """Progress, read from the one event that reports it."""

    def test_progress_is_reported_only_by_a_progress_event(self):
        running = snapshot("running")

        steps = steps_of(read(running))

        self.assertEqual(steps["demux"].progress_percent, 50)
        # The snapshot stores 0 and 100 for these, as defaults, not as reports.
        self.assertEqual(running["steps"]["qc"]["progress"], 0)
        self.assertIsNone(steps["qc"].progress_percent)
        finished = steps_of(read(snapshot("finished_success")))
        self.assertIsNone(finished["demux"].progress_percent)

    def test_unreadable_progress_is_null_and_reported(self):
        values = (
            ("below", -1),
            ("above", 101),
            ("text", "40"),
            ("true", True),
            ("null", None),
            ("a list", [40]),
            ("NaN", float("nan")),
            ("infinity", float("inf")),
            ("minus infinity", float("-inf")),
            ("the largest float", 1e308),
            ("too large for a float", 10**400),
            ("too small for a float", -(10**400)),
        )
        for name, value in values:
            with self.subTest(name):
                result = read(
                    changed(snapshot("running"), "steps.demux.progress", value)
                )

                self.assertIsNone(steps_of(result)["demux"].progress_percent)
                self.assertEqual(
                    [p.path for p in result.problems], ["steps.demux.progress"]
                )

    def test_progress_within_range_is_kept_as_reported(self):
        for value in (0, 100, 0.5, 99.9):
            with self.subTest(repr(value)):
                result = read(
                    changed(snapshot("running"), "steps.demux.progress", value)
                )

                self.assertEqual(steps_of(result)["demux"].progress_percent, value)
                self.assertEqual(result.problems, ())

    def test_a_step_state_that_is_not_text_costs_only_that_step(self):
        """Beyond the status, the state is reported as the wrong type."""
        for name, state in (
            ("empty list", []),
            ("empty object", {}),
            ("list", ["step.started"]),
            ("object", {"type": "step.started"}),
        ):
            with self.subTest(name):
                result = read(changed(snapshot("running"), "steps.demux.state", state))

                self.assertEqual(result.projection, "attempt")
                step = steps_of(result)["demux"]
                self.assertEqual(step.status, "unknown")
                self.assertEqual(step.recorded_state, state)
                self.assertIsNone(step.progress_percent)
                self.assertEqual(steps_of(result)["qc"].status, "pending")
                self.assertEqual(
                    paths_and_codes(result), [("steps.demux.state", "wrong_type")]
                )

    def test_an_unreadable_step_time_is_null_and_reported(self):
        """Read tolerantly, a time that names no instant is unknown and reported."""
        for value in (
            "0001-01-01T00:00:00+05:00",
            "9999-12-31T23:59:59-05:00",
            "not a time",
        ):
            with self.subTest(value):
                result = read(changed(snapshot("running"), "steps.demux.ts", value))

                self.assertIsNone(steps_of(result)["demux"].recorded_at)
                self.assertEqual(
                    paths_and_codes(result), [("steps.demux.ts", "bad_timestamp")]
                )


class TestSettledAttempt(ReaderTestCase):
    """An attempt whose report closed it, with a failure and a blocked step."""

    def setUp(self) -> None:
        self.settled = snapshot("finished_with_failures")

    def test_settled_attempt(self):
        result = read(self.settled)

        attempt = result.attempt
        assert attempt is not None
        self.assertIs(attempt.settled, True)
        self.assertEqual(attempt.state.value, "finished")
        self.assertEqual(attempt.outcome.value, "failed")
        self.assertEqual(attempt.termination_reason.value, "completed")
        self.assertIs(attempt.is_drained.value, True)
        self.assertEqual(attempt.ended_at.value, AT)
        assert attempt.counts is not None
        self.assertEqual(
            dataclasses.asdict(attempt.counts),
            {"succeeded": 2, "reused": 0, "failed": 1, "blocked": 1, "unreached": 0},
        )

    def test_settled_blockers_and_error(self):
        steps = steps_of(read(self.settled))

        blocked = steps["lane1_upload"]
        self.assertEqual(blocked.direct_blockers, (StepRef("lane1_demux", True),))
        self.assertEqual(blocked.failed_upstream, (StepRef("lane1_demux", True),))
        # Once the report exists the error also names the exception type.
        error = steps["lane1_demux"].error
        assert error is not None
        self.assertEqual(error.error_type, "PermanentStepError")
        self.assertEqual(error.message, "lane 1 is listed twice in the sample sheet")

    def test_a_blocker_missing_from_the_inventory_stays_visible(self):
        document = changed(
            self.settled, "steps.lane1_upload.direct_blockers", ["lane9_upload"]
        )

        blocked = steps_of(read(document))["lane1_upload"]

        self.assertEqual(blocked.direct_blockers, (StepRef("lane9_upload", False),))

    def test_unknown_attempt_state_is_not_settled_and_not_unsettled(self):
        result = read(changed(self.settled, "attempt.state", "archived"))

        attempt = result.attempt
        assert attempt is not None
        self.assertEqual(attempt.state, Recorded(None, "archived", True))
        self.assertIsNone(attempt.settled)
        # Without knowing the attempt is settled, the final list is not claimed.
        self.assertIsNone(steps_of(result)["lane1_upload"].failed_upstream)

    def test_unrecognised_attempt_vocabulary_degrades_one_field(self):
        for field, value in (
            ("outcome", "partially_succeeded"),
            ("termination_reason", "timed_out"),
            ("failure_policy", "retry_forever"),
        ):
            with self.subTest(field):
                result = read(changed(self.settled, f"attempt.{field}", value))

                attempt = result.attempt
                assert attempt is not None and result.steps is not None
                self.assertEqual(getattr(attempt, field), Recorded(None, value, True))
                self.assertEqual(len(result.steps), 4)
                self.assertEqual(result.problems, ())

    def test_an_unknown_attempt_state_does_not_cost_the_recorded_outcome(self):
        """The state and the outcome are two recorded words, read independently."""
        for outcome in ("partial", "failed", "succeeded"):
            with self.subTest(outcome):
                document = changed(self.settled, "attempt.state", "paused")
                document = changed(document, "attempt.outcome", outcome)

                result = read(document)

                attempt = result.attempt
                assert attempt is not None
                self.assertEqual(attempt.state, Recorded(None, "paused", True))
                self.assertEqual(attempt.outcome.recorded, outcome)
                self.assertEqual(
                    attempt.outcome.value, None if outcome == "partial" else outcome
                )
                self.assertEqual(attempt.ended_at.value, AT)
                self.assertEqual(attempt.termination_reason.value, "completed")
                # An outcome does not make the attempt settled; only its state can.
                self.assertIsNone(attempt.settled)
                self.assertEqual(result.problems, ())

    def test_unrecognised_error_kind_is_unknown(self):
        document = changed(self.settled, "steps.lane1_demux.error.kind", "flaky")

        error = steps_of(read(document))["lane1_demux"].error

        assert error is not None
        self.assertEqual(error.kind, Recorded(None, "flaky", True))


class TestWhoRanIt(ReaderTestCase):
    """An attempt that does not say who ran it."""

    def test_an_attempt_that_does_not_say_who_ran_it(self):
        """No authority is no proof that the attempt's start record was missing.

        The projection takes the authority from the start record, so a start
        record that names none gives the same field as a missing one. A null
        authority is valid; an absent field is missing, as the attempt
        requires it. Nothing else is lost with it.
        """
        for name in ("running_with_failure", "finished_with_failures"):
            for case, value, problems in (
                ("null", None, []),
                ("absent", ABSENT, [("attempt.execution_authority", "missing_field")]),
            ):
                with self.subTest(snapshot=name, authority=case):
                    untouched = read(snapshot(name)).attempt
                    document = changed(
                        snapshot(name), "attempt.execution_authority", value
                    )

                    result = read(document)

                    attempt = result.attempt
                    assert attempt is not None and untouched is not None
                    assert result.steps is not None
                    self.assertIsNone(attempt.execution_authority.value)
                    self.assertEqual(paths_and_codes(result), problems)
                    self.assertEqual(
                        dataclasses.replace(
                            attempt,
                            execution_authority=untouched.execution_authority,
                            recorded=untouched.recorded,
                        ),
                        untouched,
                    )
                    self.assertEqual(len(result.steps), 4)


class TestOtherProjections(HistoryTestCase):
    """Other endings, and other projections, some through a real attempt."""

    def test_fail_fast_ending_tells_reused_failed_and_unreached_apart(self):
        """A rerun under fail_fast: an earlier success reused, a failure, a step never reached."""
        document = self.planned(
            spec("prepare"), spec("run", "prepare"), spec("collect", "run")
        )
        finalized = self.finalized(document, self.attempt(document))
        self.run_again("run", "collect")
        self.attempt(
            self.rerun_requested(finalized),
            {
                "run": fails(
                    TransientStepError(
                        "the reference is missing", code="REFERENCE_MISSING"
                    )
                )
            },
        )

        result = read(self.stored())

        assert result.steps is not None and result.attempt is not None
        self.assertEqual(
            [(step.step_id, step.status, step.recorded_state) for step in result.steps],
            [
                ("prepare", "reused", "step.skipped"),
                ("run", "failed", "step.failed"),
                ("collect", "unreached", "unreached"),
            ],
        )
        self.assertEqual(result.attempt.termination_reason.value, "failed_fast")
        self.assertIs(result.attempt.is_drained.value, False)
        error = result.steps[1].error
        assert error is not None
        self.assertEqual(
            (error.kind.value, error.error_type), ("transient", "TransientStepError")
        )
        self.assertEqual(result.problems, ())

    def test_a_cancelled_attempt_tells_interrupted_from_unreached(self):
        result = read(snapshot("cancelled"))

        attempt = result.attempt
        assert attempt is not None
        self.assertEqual(attempt.ended_at.value, AT)
        steps = steps_of(result)
        interrupted = steps["qc"]
        self.assertEqual(
            (
                interrupted.status,
                interrupted.recorded_state,
                interrupted.recorded_outcome,
            ),
            ("interrupted", "interrupted", None),
        )
        # The entry keeps the time of the step's last event; no event records
        # the interruption itself.
        self.assertEqual(interrupted.recorded_at, AT)
        self.assertIsNone(interrupted.progress_percent)
        self.assertIsNone(interrupted.error)
        self.assertIsNone(interrupted.direct_blockers)
        untouched = steps["upload"]
        self.assertEqual(
            (untouched.status, untouched.recorded_state, untouched.recorded_at),
            ("unreached", "unreached", None),
        )
        self.assertIsNone(untouched.direct_blockers)

    def test_preflight_rejection_is_a_diagnostic_not_a_failed_step(self):
        cycle = self.planned(
            spec("demux", "qc"), spec("qc", "demux"), policy=CONTINUE_INDEPENDENT_POLICY
        )
        self.attempt(cycle)

        result = read(self.stored())

        attempt = result.attempt
        assert attempt is not None and attempt.diagnostic is not None
        assert result.steps is not None
        self.assertEqual(attempt.termination_reason.value, "preflight_rejected")
        self.assertEqual(attempt.outcome.value, "failed")
        self.assertEqual(attempt.diagnostic.error_type, "PreflightValidationError")
        self.assertIn("Dependency cycle", attempt.diagnostic.message or "")
        self.assertEqual(
            [step.status for step in result.steps], ["unreached", "unreached"]
        )
        self.assertTrue(all(step.error is None for step in result.steps))
        self.assertEqual(result.problems, ())

    def test_legacy_projection_is_not_an_attempt(self):
        result = read(snapshot("legacy"))

        self.assertEqual(result.projection, "legacy")
        self.assertIsNone(result.attempt)
        assert result.steps is not None
        self.assertEqual(
            [(step.step_id, step.status, step.name) for step in result.steps],
            [("demux", "succeeded", "demux"), ("qc", "unknown", None)],
        )

    def test_report_without_its_start_record_keeps_surviving_step_events(self):
        result = read(snapshot("report_without_start"))

        attempt = result.attempt
        assert attempt is not None
        # The projection takes the authority from the start record, which this
        # attempt lacks.
        self.assertEqual(attempt.execution_authority, Recorded(None, None, True))
        self.assertEqual(attempt.state.value, "finished")
        self.assertEqual(attempt.started_at.value, AT)
        steps = steps_of(result)
        self.assertEqual(
            (steps["demux"].name, steps["demux"].recorded_at), ("demux", AT)
        )
        self.assertEqual(steps["qc"].status, "succeeded")
        self.assertEqual(result.problems, ())

    def test_report_with_no_surviving_step_events(self):
        """Only the report survived: statuses and the failure come from it alone."""
        document = self.planned(
            spec("prepare"), spec("run", "prepare"), spec("collect", "run")
        )
        context = self.attempt(
            document,
            {
                "run": fails(
                    PermanentStepError(
                        "no reference", code="REFERENCE_MISSING", advice="Install it."
                    )
                )
            },
        )
        self.without_start_record(context)
        self.without_step_events(context)

        result = read(self.stored())

        assert result.attempt is not None and result.steps is not None
        self.assertIsNone(result.attempt.execution_authority.value)
        self.assertEqual(
            [
                (step.step_id, step.status, step.name, step.recorded_at)
                for step in result.steps
            ],
            [
                ("prepare", "succeeded", None, None),
                ("run", "failed", None, None),
                ("collect", "unreached", None, None),
            ],
        )
        # The report still explains the failure.
        error = result.steps[1].error
        assert error is not None
        self.assertEqual(error.code, "REFERENCE_MISSING")
        self.assertEqual(result.problems, ())

    def test_an_attempt_recorded_as_running_claims_nothing_about_now(self):
        """A crashed attempt stays recorded as running: what was observed, not liveness."""
        document = changed(
            snapshot("start_without_report"),
            "steps.qc.state",
            "step.checkpoint_written",
        )
        document = changed(document, "steps.qc.outcome", None)

        result = read(document)

        attempt = result.attempt
        assert attempt is not None and result.written_at is not None
        self.assertEqual(attempt.state.value, "running")
        self.assertIs(attempt.settled, False)
        step = steps_of(result)["qc"]
        # A realm-defined event: recorded past activity, its type kept.
        self.assertEqual(
            (step.status, step.recorded_state), ("started", "step.checkpoint_written")
        )
        # When the snapshot was written, not when anything last ran.
        self.assertEqual(result.written_at.value, AT)

    def test_an_attempt_without_generation_or_token(self):
        document = changed(snapshot("running"), "attempt.plan_generation", None)
        document = changed(document, "attempt.run_token", None)

        result = read(document)

        attempt = result.attempt
        assert attempt is not None
        self.assertIsNone(attempt.plan_generation.value)
        self.assertIsNone(attempt.run_token.value)
        self.assertEqual(result.problems, ())


class TestMalformedSnapshots(ReaderTestCase):
    """A snapshot whose identity or structure cannot be trusted."""

    def test_a_malformed_snapshot_is_invalid_as_a_whole(self):
        """Each malformation: not read, nothing derived, never a partial inventory."""

        def empty_step_id(status: dict[str, Any]) -> Any:
            status["steps"][""] = status["steps"]["lane2_demux"]
            return status

        malformations: tuple[tuple[str, Callable[[dict[str, Any]], Any]], ...] = (
            ("not an object", lambda status: "snapshot"),
            (
                "steps a list",
                lambda status: changed(status, "steps", list(status["steps"].values())),
            ),
            (
                "a step entry is text",
                lambda status: changed(status, "steps.lane2_demux", "step.started"),
            ),
            ("a step ID is empty", empty_step_id),
            (
                "blockers are text",
                lambda status: changed(
                    status, "steps.lane1_upload.direct_blockers", "lane1_demux"
                ),
            ),
            (
                "a blocker is a number",
                lambda status: changed(
                    status, "steps.lane1_upload.failed_ancestors", [4]
                ),
            ),
            (
                "the attempt is text",
                lambda status: changed(
                    status, "attempt", "20261022T100000000000Z_0001"
                ),
            ),
            ("the attempt is null", lambda status: changed(status, "attempt", None)),
            (
                "the attempt has no ID",
                lambda status: changed(status, "attempt.execution_id", ""),
            ),
            (
                "a legacy projection claims an attempt",
                lambda status: changed(status, "projection", "legacy"),
            ),
            (
                "the projection is unrecognised",
                lambda status: changed(status, "projection", "materialised"),
            ),
            (
                "the projection is missing",
                lambda status: changed(status, "projection", ABSENT),
            ),
            ("another plan", lambda status: changed(status, "plan_id", "pln_other")),
            ("another realm", lambda status: changed(status, "realm", "tenx")),
            # Both lists are required on every entry, whatever its status.
            (
                "a blocked step without direct blockers",
                lambda status: changed(
                    status, "steps.lane1_upload.direct_blockers", ABSENT
                ),
            ),
            (
                "a blocked step without failed ancestors",
                lambda status: changed(
                    status, "steps.lane1_upload.failed_ancestors", ABSENT
                ),
            ),
            (
                "a step without direct blockers",
                lambda status: changed(
                    status, "steps.lane2_demux.direct_blockers", ABSENT
                ),
            ),
            (
                "a step without failed ancestors",
                lambda status: changed(
                    status, "steps.lane2_demux.failed_ancestors", ABSENT
                ),
            ),
        )
        for name, malform in malformations:
            with self.subTest(name):
                result = read_snapshot(
                    malform(snapshot("finished_with_failures")),
                    plan_id="pln_lanes",
                    realm="test_realm",
                )

                self.assertFalse(result.usable)
                self.assertIsNone(result.steps)
                self.assertIsNone(result.attempt)
                self.assertIsNone(result.projection)
                self.assertTrue(result.problems)
                self.assertTrue(
                    {p.code for p in result.problems}
                    <= {"wrong_type", "invalid_structure", "identity_mismatch"}
                )


class TestRecordedValuesInMessages(ReaderTestCase):
    """How a problem names the stored value it could not read."""

    def test_an_unreadable_value_is_named_as_it_was_stored(self):
        token = read(changed(snapshot("finished_success"), "attempt.run_token", 0.5))
        progress = read(changed(snapshot("running"), "steps.demux.progress", "40"))

        self.assertEqual(paths_and_codes(token), [("attempt.run_token", "wrong_type")])
        self.assertIn(" 0.5 ", token.problems[0].message)
        self.assertEqual(
            paths_and_codes(progress), [("steps.demux.progress", "wrong_type")]
        )
        self.assertIn("'40'", progress.problems[0].message)


if __name__ == "__main__":
    unittest.main()
