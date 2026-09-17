"""Tests for the execution outcome vocabulary and the per-attempt report.

These pin down the contract the engine and its callers build on: the four
terminal step outcomes, the separation of "blocked" from "never reached",
attempt-level diagnostics that need no invented failed step, and the rules that
decide when an attempt counts as drained and successful.
"""

import json
import unittest

from yggdrasil.flow.errors import OrchestrationError
from yggdrasil.flow.outcomes import (
    AttemptDiagnostic,
    AttemptReport,
    ExecutionOutcome,
    StepFailure,
    StepOutcome,
    TerminationReason,
)


def _report(*step_ids: str, **kwargs) -> AttemptReport:
    """Build a report over the given planned step inventory."""
    kwargs.setdefault("execution_id", "exec_1")
    kwargs.setdefault("plan_id", "plan_1")
    return AttemptReport(step_ids=list(step_ids), **kwargs)


class TestStepOutcome(unittest.TestCase):
    """The four terminal step outcomes and their dependency semantics.

    The "table" these tests refer to is the step outcome table in
    ``docs/design/prds/independent_branch_execution_prd.md``: which outcomes
    exist, and which of them satisfy a success dependency.
    """

    def test_value_set_matches_the_prd_table_exactly(self):
        """Exactly four terminal outcomes exist - no pending/running."""
        self.assertEqual(
            {o.value for o in StepOutcome},
            {"succeeded", "reused", "failed", "blocked"},
        )

    def test_serializes_as_plain_string(self):
        """Outcomes must land in JSON as plain strings, not enum reprs."""
        payload = json.dumps({"outcome": StepOutcome.REUSED})
        self.assertEqual(json.loads(payload), {"outcome": "reused"})

    def test_satisfies_dependency_matches_the_prd_table(self):
        """Only succeeded/reused release a successor; failed/blocked do not."""
        self.assertTrue(StepOutcome.SUCCEEDED.satisfies_dependency())
        self.assertTrue(StepOutcome.REUSED.satisfies_dependency())
        self.assertFalse(StepOutcome.FAILED.satisfies_dependency())
        self.assertFalse(StepOutcome.BLOCKED.satisfies_dependency())


class TestExecutionOutcomeAndTerminationReason(unittest.TestCase):
    """The attempt-level vocabularies."""

    def test_execution_outcome_is_binary(self):
        """No partially-successful overall outcome exists."""
        self.assertEqual({o.value for o in ExecutionOutcome}, {"succeeded", "failed"})

    def test_termination_reasons_cover_every_way_an_attempt_can_end(self):
        self.assertEqual(
            {r.value for r in TerminationReason},
            {
                "completed",
                "failed_fast",
                "cancelled",
                "preflight_rejected",
                "orchestration_error",
            },
        )

    def test_termination_reasons_serialize_as_plain_strings(self):
        payload = json.dumps({"reason": TerminationReason.CANCELLED})
        self.assertEqual(json.loads(payload), {"reason": "cancelled"})


class TestAttemptDiagnostic(unittest.TestCase):
    """Attempt-level diagnostics, independent of any step."""

    def test_from_exception_captures_message_and_type(self):
        diagnostic = AttemptDiagnostic.from_exception(
            ValueError("bad policy"), details={"policy": "nope"}
        )

        self.assertEqual(diagnostic.message, "bad policy")
        self.assertEqual(diagnostic.error_type, "ValueError")
        self.assertEqual(diagnostic.details, {"policy": "nope"})

    def test_to_dict_is_json_serializable(self):
        diagnostic = AttemptDiagnostic(message="cycle", details={"cycle": ["a", "b"]})
        self.assertEqual(
            json.loads(json.dumps(diagnostic.to_dict()))["message"], "cycle"
        )


class TestAttemptReportRecording(unittest.TestCase):
    """Recording outcomes, failures, blockers and diagnostics."""

    def test_record_failure_sets_outcome_and_diagnostics_together(self):
        """One call, so an attempt cannot record one without the other."""
        report = _report("a")

        report.record_failure(StepFailure(step_id="a", error="boom", kind="transient"))

        self.assertEqual(report.step_outcomes["a"], StepOutcome.FAILED)
        self.assertEqual(report.failures["a"].error, "boom")
        self.assertEqual(report.failures["a"].kind, "transient")

    def test_record_blocked_sets_outcome_and_blockers_together(self):
        report = _report("a", "b")

        report.record_blocked("b", ["a"])

        self.assertEqual(report.step_outcomes["b"], StepOutcome.BLOCKED)
        self.assertEqual(report.direct_blockers["b"], ["a"])

    def test_record_blocked_copies_the_blocker_list(self):
        """A caller mutating its own list must not rewrite recorded history."""
        report = _report("a", "b")
        blockers = ["a"]

        report.record_blocked("b", blockers)
        blockers.append("mutated")

        self.assertEqual(report.direct_blockers["b"], ["a"])

    def test_attempt_level_diagnostic_needs_no_invented_failed_step(self):
        """An unknown policy or cycle is not attributable to any step."""
        report = _report("a", "b")

        report.record_diagnostic(
            AttemptDiagnostic(message="Dependency cycle: a -> b -> a")
        )
        report.finish(TerminationReason.PREFLIGHT_REJECTED)

        self.assertIn("cycle", report.diagnostic.message)
        self.assertEqual(report.failures, {})
        self.assertEqual(report.step_outcomes, {})
        self.assertEqual(report.unreached_step_ids, ["a", "b"])
        self.assertEqual(report.outcome, ExecutionOutcome.FAILED)


class TestAttemptReportUnreachedWork(unittest.TestCase):
    """Never-reached work stays distinct from dependency-blocked work."""

    def test_unreached_is_absence_of_an_outcome_in_plan_order(self):
        report = _report("a", "b", "c", "d")
        report.record_outcome("a", StepOutcome.SUCCEEDED)
        report.record_blocked("b", ["a"])

        self.assertEqual(report.unreached_step_ids, ["c", "d"])

    def test_partial_report_keeps_unreached_work_distinct_from_blocked(self):
        """A cancelled attempt must not relabel unreached work as blocked."""
        report = _report("root", "dependent", "independent")
        report.record_failure(StepFailure(step_id="root", error="boom"))
        report.record_blocked("dependent", ["root"])
        report.finish(TerminationReason.CANCELLED)

        restored = report.to_dict()

        self.assertEqual(restored["step_outcomes"]["dependent"], "blocked")
        self.assertEqual(restored["unreached_step_ids"], ["independent"])
        self.assertNotIn("independent", restored["step_outcomes"])

    def test_drained_report_contains_only_terminal_outcomes(self):
        report = _report("a", "b")
        report.record_outcome("a", StepOutcome.SUCCEEDED)
        report.record_outcome("b", StepOutcome.REUSED)
        report.finish(TerminationReason.COMPLETED)

        self.assertTrue(report.is_drained)
        for outcome in report.step_outcomes.values():
            self.assertIsInstance(outcome, StepOutcome)

    def test_counts_sum_to_the_planned_inventory(self):
        report = _report("a", "b", "c")
        report.record_outcome("a", StepOutcome.SUCCEEDED)
        report.record_failure(StepFailure(step_id="b", error="boom"))

        counts = report.counts

        self.assertEqual(counts["succeeded"], 1)
        self.assertEqual(counts["failed"], 1)
        self.assertEqual(counts["unreached"], 1)
        self.assertEqual(sum(counts.values()), len(report.step_ids))


class TestAttemptReportCompletion(unittest.TestCase):
    """finish(), is_drained, and the derived overall outcome."""

    def test_unfinished_report_has_no_established_outcome(self):
        """An in-flight attempt must not read as a terminal failure."""
        report = _report("a")
        report.record_failure(StepFailure(step_id="a", error="boom"))

        self.assertFalse(report.is_finished)
        self.assertIsNone(report.outcome)
        self.assertFalse(report.is_drained)

    def test_completed_empty_plan_succeeds(self):
        """An empty plan drains trivially and is a success."""
        report = _report()

        report.finish(TerminationReason.COMPLETED)

        self.assertTrue(report.is_drained)
        self.assertEqual(report.outcome, ExecutionOutcome.SUCCEEDED)

    def test_rejected_empty_plan_fails_and_is_not_drained(self):
        """Trivial inventory coverage is not the same as having run."""
        report = _report()

        report.finish(TerminationReason.PREFLIGHT_REJECTED)

        self.assertFalse(report.is_drained)
        self.assertEqual(report.outcome, ExecutionOutcome.FAILED)

    def test_finish_completed_rejects_missing_outcomes(self):
        """Claiming normal completion with unreached work cannot be published."""
        report = _report("a", "b")
        report.record_outcome("a", StepOutcome.SUCCEEDED)

        with self.assertRaises(OrchestrationError) as cm:
            report.finish(TerminationReason.COMPLETED)

        self.assertIn("b", str(cm.exception))
        self.assertIn("no recorded outcome", str(cm.exception))
        # The failed finish must not half-close the report.
        self.assertFalse(report.is_finished)
        self.assertIsNone(report.outcome)

    def test_fail_fast_failure_on_the_final_step_is_not_drained(self):
        """Full inventory coverage is not enough; it did not complete normally."""
        report = _report("a", "b")
        report.record_outcome("a", StepOutcome.SUCCEEDED)
        report.record_failure(StepFailure(step_id="b", error="boom"))

        report.finish(TerminationReason.FAILED_FAST)

        self.assertEqual(report.unreached_step_ids, [])
        self.assertFalse(report.is_drained)
        self.assertEqual(report.outcome, ExecutionOutcome.FAILED)

    def test_drained_attempt_with_a_failure_is_not_successful(self):
        """Finishing healthy branches does not make a plan successful."""
        report = _report("ok", "bad", "dependent")
        report.record_outcome("ok", StepOutcome.SUCCEEDED)
        report.record_failure(StepFailure(step_id="bad", error="boom"))
        report.record_blocked("dependent", ["bad"])

        report.finish(TerminationReason.COMPLETED)

        self.assertTrue(report.is_drained)
        self.assertEqual(report.outcome, ExecutionOutcome.FAILED)

    def test_reused_steps_count_as_success(self):
        report = _report("a", "b")
        report.record_outcome("a", StepOutcome.REUSED)
        report.record_outcome("b", StepOutcome.SUCCEEDED)

        report.finish(TerminationReason.COMPLETED)

        self.assertEqual(report.outcome, ExecutionOutcome.SUCCEEDED)

    def test_finish_records_the_supplied_end_timestamp(self):
        report = _report()

        report.finish(TerminationReason.COMPLETED, ended_at="2026-09-15T00:00:00+00:00")

        self.assertEqual(report.ended_at, "2026-09-15T00:00:00+00:00")


class TestAttemptReportIdentity(unittest.TestCase):
    """Captured identity describes the request the report was opened for."""

    def test_identity_stays_fixed_while_outcomes_and_diagnostics_change(self):
        report = _report(
            "a",
            "b",
            execution_id="exec_42",
            plan_id="plan_42",
            plan_generation="gen_42",
            run_token=7,
            failure_policy="continue_independent",
        )
        identity_before = (
            report.execution_id,
            report.plan_id,
            report.plan_generation,
            report.run_token,
            report.failure_policy,
            report.started_at,
        )

        report.record_outcome("a", StepOutcome.SUCCEEDED)
        report.record_failure(StepFailure(step_id="b", error="boom"))
        report.record_diagnostic(AttemptDiagnostic(message="spool unavailable"))
        report.finish(TerminationReason.ORCHESTRATION_ERROR)

        self.assertEqual(
            identity_before,
            (
                report.execution_id,
                report.plan_id,
                report.plan_generation,
                report.run_token,
                report.failure_policy,
                report.started_at,
            ),
        )


class TestAttemptReportSerialization(unittest.TestCase):
    """to_dict() is the shape reporting consumes."""

    def test_to_dict_is_json_serializable_with_plain_strings(self):
        report = _report(
            "a",
            "b",
            plan_generation="gen_1",
            run_token=3,
            failure_policy="continue_independent",
        )
        report.record_failure(
            StepFailure(step_id="a", error="boom", kind="transient", code="E1")
        )
        report.record_blocked("b", ["a"])
        report.record_failed_ancestors("b", ["a"])
        report.finish(TerminationReason.COMPLETED)

        payload = json.loads(json.dumps(report.to_dict()))

        self.assertEqual(payload["execution_id"], "exec_1")
        self.assertEqual(payload["plan_generation"], "gen_1")
        self.assertEqual(payload["run_token"], 3)
        self.assertEqual(payload["failure_policy"], "continue_independent")
        self.assertEqual(payload["termination_reason"], "completed")
        self.assertEqual(payload["outcome"], "failed")
        self.assertTrue(payload["is_drained"])
        self.assertEqual(payload["step_outcomes"], {"a": "failed", "b": "blocked"})
        self.assertEqual(payload["direct_blockers"], {"b": ["a"]})
        self.assertEqual(payload["failed_ancestors"], {"b": ["a"]})
        self.assertEqual(payload["failures"]["a"]["code"], "E1")
        self.assertIsNone(payload["diagnostic"])

    def test_to_dict_of_an_unfinished_report_reports_no_outcome(self):
        report = _report("a")

        payload = report.to_dict()

        self.assertIsNone(payload["outcome"])
        self.assertIsNone(payload["termination_reason"])
        self.assertIsNone(payload["ended_at"])


class TestAttemptReportConsistencyGuards(unittest.TestCase):
    """A report must not be able to contradict itself.

    The derived overall outcome is only trustworthy if the recorded outcomes it
    derives from are. These guards close the writes that would let a report say
    one thing in its outcome and another in its diagnostics.
    """

    def test_conflicting_terminal_outcomes_are_rejected(self):
        """A failed step cannot be re-recorded as succeeded."""
        report = _report("s")
        report.record_failure(StepFailure(step_id="s", error="failed work"))

        with self.assertRaises(OrchestrationError) as cm:
            report.record_outcome("s", StepOutcome.SUCCEEDED)

        message = str(cm.exception)
        self.assertIn("Conflicting outcome", message)
        self.assertIn("failed", message)
        self.assertIn("succeeded", message)
        # The original record stands.
        self.assertEqual(report.step_outcomes["s"], StepOutcome.FAILED)

    def test_blocking_a_step_that_already_succeeded_is_rejected(self):
        report = _report("s")
        report.record_outcome("s", StepOutcome.SUCCEEDED)

        with self.assertRaises(OrchestrationError):
            report.record_blocked("s", ["other"])

    def test_failing_a_step_that_already_succeeded_is_rejected(self):
        report = _report("s")
        report.record_outcome("s", StepOutcome.SUCCEEDED)

        with self.assertRaises(OrchestrationError):
            report.record_failure(StepFailure(step_id="s", error="boom"))

    def test_re_recording_the_same_outcome_is_allowed(self):
        """Only contradiction is rejected, not a harmless repeat."""
        report = _report("s")
        report.record_outcome("s", StepOutcome.SUCCEEDED)

        report.record_outcome("s", StepOutcome.SUCCEEDED)

        self.assertEqual(report.step_outcomes["s"], StepOutcome.SUCCEEDED)

    def test_unknown_step_ids_are_rejected_by_every_mutator(self):
        """Otherwise the breakdown would exceed the planned inventory."""
        report = _report("known")

        with self.assertRaises(OrchestrationError) as cm:
            report.record_outcome("ghost", StepOutcome.SUCCEEDED)
        self.assertIn("not in plan", str(cm.exception))

        with self.assertRaises(OrchestrationError):
            report.record_failure(StepFailure(step_id="ghost", error="boom"))

        with self.assertRaises(OrchestrationError):
            report.record_blocked("ghost", ["known"])

        with self.assertRaises(OrchestrationError):
            report.record_failed_ancestors("ghost", ["known"])

        self.assertEqual(report.step_outcomes, {})

    def test_counts_cannot_exceed_the_planned_inventory(self):
        report = _report("a")
        report.record_outcome("a", StepOutcome.SUCCEEDED)

        with self.assertRaises(OrchestrationError):
            report.record_outcome("ghost", StepOutcome.SUCCEEDED)

        self.assertEqual(sum(report.counts.values()), len(report.step_ids))

    def test_failed_ancestors_require_a_blocked_step(self):
        """They explain blocking; attaching them elsewhere is a contradiction."""
        report = _report("a", "b")
        report.record_outcome("a", StepOutcome.SUCCEEDED)

        with self.assertRaises(OrchestrationError) as cm:
            report.record_failed_ancestors("a", ["x"])
        self.assertIn("not blocked", str(cm.exception))

        with self.assertRaises(OrchestrationError):
            report.record_failed_ancestors("b", ["x"])  # unreached

    def test_failed_ancestors_are_recorded_for_a_blocked_step(self):
        report = _report("a", "b")
        report.record_failure(StepFailure(step_id="a", error="boom"))
        report.record_blocked("b", ["a"])

        report.record_failed_ancestors("b", ["a"])

        self.assertEqual(report.failed_ancestors["b"], ["a"])

    def test_failed_ancestors_list_is_copied(self):
        report = _report("a", "b")
        report.record_failure(StepFailure(step_id="a", error="boom"))
        report.record_blocked("b", ["a"])
        ancestors = ["a"]

        report.record_failed_ancestors("b", ancestors)
        ancestors.append("mutated")

        self.assertEqual(report.failed_ancestors["b"], ["a"])

    def test_normal_completion_rejects_an_attempt_level_failure(self):
        """An attempt-level failure means it did not complete normally."""
        report = _report("a")
        report.record_outcome("a", StepOutcome.SUCCEEDED)
        report.record_diagnostic(AttemptDiagnostic(message="event spool unavailable"))

        with self.assertRaises(OrchestrationError) as cm:
            report.finish(TerminationReason.COMPLETED)

        self.assertIn("attempt-level failure", str(cm.exception))
        self.assertFalse(report.is_finished)
        self.assertIsNone(report.outcome)

    def test_an_attempt_level_failure_finishes_under_its_own_reason(self):
        report = _report("a")
        report.record_outcome("a", StepOutcome.SUCCEEDED)
        report.record_diagnostic(AttemptDiagnostic(message="event spool unavailable"))

        report.finish(TerminationReason.ORCHESTRATION_ERROR)

        self.assertEqual(report.outcome, ExecutionOutcome.FAILED)
        self.assertFalse(report.is_drained)

    def test_normal_completion_rejects_a_failure_contradicting_its_outcome(self):
        """Closes the same contradiction reached by writing the fields directly."""
        report = _report("s")
        report.record_outcome("s", StepOutcome.SUCCEEDED)
        report.failures["s"] = StepFailure(step_id="s", error="failed work")

        with self.assertRaises(OrchestrationError) as cm:
            report.finish(TerminationReason.COMPLETED)

        self.assertIn("recorded failure", str(cm.exception))
        self.assertFalse(report.is_finished)


class TestAttemptReportPublicationFailure(unittest.TestCase):
    """A closed report that could not be published must stop claiming its ending."""

    def publication_failure(self) -> AttemptDiagnostic:
        return AttemptDiagnostic(
            message="Publishing the report: spool unavailable",
            error_type="EventPublicationError",
        )

    def test_a_drained_report_becomes_an_orchestration_error(self):
        report = _report("a", "b")
        report.record_outcome("a", StepOutcome.SUCCEEDED)
        report.record_failure(StepFailure(step_id="b", error="boom"))
        report.finish(TerminationReason.COMPLETED)
        self.assertTrue(report.is_drained)

        report.record_publication_failure(self.publication_failure())

        self.assertIs(report.termination_reason, TerminationReason.ORCHESTRATION_ERROR)
        self.assertFalse(report.is_drained)
        self.assertIs(report.outcome, ExecutionOutcome.FAILED)
        failure = report.publication_failure
        assert failure is not None
        self.assertEqual(
            failure.details, {"superseded_termination_reason": "completed"}
        )
        # Nothing the attempt established is rewritten.
        self.assertEqual(
            report.step_outcomes,
            {"a": StepOutcome.SUCCEEDED, "b": StepOutcome.FAILED},
        )
        self.assertEqual(report.failures["b"].error, "boom")
        self.assertIsNone(report.diagnostic)

    def test_every_non_cancelled_ending_is_superseded_and_kept_as_context(self):
        for reason in (
            TerminationReason.FAILED_FAST,
            TerminationReason.PREFLIGHT_REJECTED,
        ):
            with self.subTest(reason=reason):
                report = _report("a")
                original = AttemptDiagnostic(message="duplicate step_id")
                report.record_diagnostic(original)
                report.finish(reason)

                report.record_publication_failure(self.publication_failure())

                self.assertIs(
                    report.termination_reason, TerminationReason.ORCHESTRATION_ERROR
                )
                failure = report.publication_failure
                assert failure is not None
                self.assertEqual(
                    failure.details["superseded_termination_reason"], reason.value
                )
                self.assertIs(report.diagnostic, original)

    def test_a_cancelled_ending_is_kept(self):
        report = _report("a")
        report.record_diagnostic(
            AttemptDiagnostic(message="", error_type="KeyboardInterrupt")
        )
        report.finish(TerminationReason.CANCELLED)

        report.record_publication_failure(self.publication_failure())

        self.assertIs(report.termination_reason, TerminationReason.CANCELLED)
        failure = report.publication_failure
        assert failure is not None
        self.assertNotIn("superseded_termination_reason", failure.details)

    def test_an_orchestration_error_has_nothing_to_supersede(self):
        report = _report("a")
        report.finish(TerminationReason.ORCHESTRATION_ERROR)

        report.record_publication_failure(self.publication_failure())

        self.assertIs(report.termination_reason, TerminationReason.ORCHESTRATION_ERROR)
        failure = report.publication_failure
        assert failure is not None
        self.assertEqual(failure.details, {})

    def test_the_callers_diagnostic_object_is_not_mutated(self):
        report = _report()
        report.finish(TerminationReason.COMPLETED)
        given = self.publication_failure()

        report.record_publication_failure(given)

        self.assertEqual(given.details, {})

    def test_an_open_report_cannot_record_a_publication_failure(self):
        report = _report("a")

        with self.assertRaises(OrchestrationError):
            report.record_publication_failure(self.publication_failure())

        self.assertIsNone(report.publication_failure)

    def test_only_one_publication_failure_can_be_recorded(self):
        report = _report()
        report.finish(TerminationReason.COMPLETED)
        report.record_publication_failure(self.publication_failure())

        with self.assertRaises(OrchestrationError):
            report.record_publication_failure(self.publication_failure())

    def test_publication_failure_is_serialized(self):
        report = _report()
        report.finish(TerminationReason.COMPLETED)
        self.assertIsNone(report.to_dict()["publication_failure"])

        report.record_publication_failure(self.publication_failure())
        payload = json.loads(json.dumps(report.to_dict()))

        self.assertEqual(payload["termination_reason"], "orchestration_error")
        self.assertEqual(
            payload["publication_failure"]["error_type"], "EventPublicationError"
        )
        self.assertEqual(
            payload["publication_failure"]["details"],
            {"superseded_termination_reason": "completed"},
        )


if __name__ == "__main__":
    unittest.main()
