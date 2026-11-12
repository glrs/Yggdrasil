"""Correlating a plan with its snapshot: three questions, each with a closed reason.

Each pair is of one plan's history in the regression corpus: a plan document
and a snapshot the manifest's ``request_lifecycle`` sequence puts together, or
``continue_independent_failures`` with ``finished_with_failures``, which
record one attempt. A case changes a few fields of such a pair, and says
which; the change is about interpretation, not about what Yggdrasil writes.
"""

from __future__ import annotations

import copy
import dataclasses
import unittest
from typing import Any

from tests.contract.read_support import (
    ReaderTestCase,
    plan_document,
    sequence_pairs,
    snapshot,
)
from tests.contract.support import ABSENT, changed
from yggdrasil.contract.read import (
    FINALIZED_OBSERVATION_REASONS,
    FINALIZED_OBSERVATION_VERDICTS,
    LOOKUP_IMPOSSIBLE,
    MISSING,
    PLAN_OBSERVATION_REASONS,
    PLAN_OBSERVATION_VERDICTS,
    REQUEST_OBSERVATION_REASONS,
    REQUEST_OBSERVATION_VERDICTS,
    UNAVAILABLE,
    Correlation,
    Ending,
    NoDocument,
    PlanRead,
    Recorded,
    SnapshotRead,
    Verdict,
    correlate,
    read_plan,
    read_snapshot,
)

LIFECYCLE = "pln_lifecycle"
REALM = "test_realm"

# The questions a correlation answers, by their field.
QUESTIONS = (
    "plan_and_observation",
    "finalized_and_observation",
    "request_and_observation",
)


def read_pair(plan: Any, observed: Any) -> tuple[PlanRead, SnapshotRead]:
    """Read a plan document under its own ID, and a snapshot for that plan."""
    plan_read = read_plan(plan, plan_id=LIFECYCLE)
    return plan_read, read_snapshot(observed, plan_id=LIFECYCLE, realm=REALM)


def correlated(plan: Any, observed: Any) -> Correlation:
    """Read a plan document and a snapshot of pln_lifecycle, and correlate them."""
    return correlate(*read_pair(plan, observed))


def answers(correlation: Correlation) -> tuple[tuple[str, str], ...]:
    """Return the three verdicts of a correlation, each as (verdict, reason)."""
    return tuple(
        (getattr(correlation, name).verdict, getattr(correlation, name).reason)
        for name in QUESTIONS
    )


def with_tokens(document: dict[str, Any], run: Any, executed: Any) -> dict[str, Any]:
    """Return a plan document with its two execution tokens set by hand."""
    return changed(changed(document, "run_token", run), "executed_run_token", executed)


def with_record_token(document: dict[str, Any], token: Any) -> dict[str, Any]:
    """Return a plan document whose finalized record and its report name a token."""
    document = changed(document, "last_finalized_execution.run_token", token)
    return changed(document, "last_finalized_execution.report.run_token", token)


class TestWhatIsCompared(ReaderTestCase):
    """Whether there is an attempt to compare: one order for all three questions."""

    def test_a_snapshot_the_caller_could_not_read(self):
        plan = read_plan(plan_document("approved"), plan_id=LIFECYCLE)
        for given, reason in (
            (LOOKUP_IMPOSSIBLE, "lookup_impossible"),
            (MISSING, "no_observation"),
            (UNAVAILABLE, "observation_unavailable"),
            (read_snapshot(MISSING, plan_id=LIFECYCLE, realm=REALM), "no_observation"),
            (
                read_snapshot(UNAVAILABLE, plan_id=LIFECYCLE, realm=REALM),
                "observation_unavailable",
            ),
        ):
            with self.subTest(reason=reason, given=type(given).__name__):
                correlation = correlate(plan, given)

                self.assertEqual(
                    answers(correlation),
                    (
                        ("unknown", reason),
                        ("unknown", "nothing_finalized"),
                        ("unknown", reason),
                    ),
                )

    def test_a_snapshot_that_cannot_be_compared(self):
        finished = snapshot("finished_success")
        cases = (
            ("unversioned", snapshot("unversioned"), "observation_inspect_only"),
            (
                "family 2",
                changed(finished, "schema_version", 2),
                "observation_inspect_only",
            ),
            (
                "text marker",
                changed(finished, "schema_version", "1"),
                "observation_inspect_only",
            ),
            ("not an object", "a snapshot", "observation_invalid"),
            (
                "another plan",
                changed(finished, "plan_id", "pln_other"),
                "observation_invalid",
            ),
            ("not structured", changed(finished, "steps", []), "observation_invalid"),
            ("legacy", self.legacy_of_lifecycle(), "legacy_projection"),
        )
        for name, observed, reason in cases:
            with self.subTest(name):
                correlation = correlated(plan_document("approved_finalized"), observed)

                self.assertEqual(
                    answers(correlation),
                    (("unknown", reason), ("unknown", reason), ("unknown", reason)),
                )

    def test_a_plan_that_is_not_usable(self):
        cases = (
            ("missing", MISSING),
            ("unavailable", UNAVAILABLE),
            ("not an object", ["a plan"]),
            ("inspect-only", plan_document("pre_contract")),
            ("another ID", changed(plan_document("approved"), "_id", "pln_other")),
        )
        for name, document in cases:
            with self.subTest(name):
                plan = read_plan(document, plan_id=LIFECYCLE)
                for observed in (
                    read_snapshot(snapshot("running"), plan_id=LIFECYCLE, realm=REALM),
                    MISSING,
                    LOOKUP_IMPOSSIBLE,
                ):
                    correlation = correlate(plan, observed)

                    self.assertEqual(
                        answers(correlation), (("unknown", "plan_not_usable"),) * 3
                    )
                    self.assertIsNone(correlation.conflicting_endings)

    def test_a_readable_plan_stays_available_whatever_its_snapshot_is(self):
        """Snapshot problems never cost the plan its answers."""
        document = plan_document("approved")
        alone = read_plan(document, plan_id=LIFECYCLE)
        for observed in (
            MISSING,
            UNAVAILABLE,
            LOOKUP_IMPOSSIBLE,
            "a snapshot",
            snapshot("unversioned"),
            changed(snapshot("running"), "steps", []),
        ):
            with self.subTest(type(observed).__name__):
                if not isinstance(observed, NoDocument):
                    observed = read_snapshot(observed, plan_id=LIFECYCLE, realm=REALM)

                correlation = correlate(alone, observed)

                self.assertIs(correlation.plan, alone)
                self.assertEqual(
                    correlation.plan, read_plan(document, plan_id=LIFECYCLE)
                )
                assert alone.eligibility is not None and alone.request is not None
                self.assertTrue(alone.eligibility.eligible)
                self.assertIs(alone.request.finished, False)

    def legacy_of_lifecycle(self) -> dict[str, Any]:
        """Return the corpus's legacy snapshot as if it were pln_lifecycle's."""
        return changed(snapshot("legacy"), "plan_id", LIFECYCLE)


class TestPlanAndObservation(ReaderTestCase):
    """Did the observed attempt run the plan's current generation?"""

    def test_one_generation(self):
        correlation = correlated(plan_document("approved"), snapshot("running"))

        self.assertEqual(
            correlation.plan_and_observation,
            Verdict("same_generation", "generations_equal"),
        )

    def test_two_generations(self):
        """Which is older is unknown: generations have no order."""
        correlation = correlated(
            plan_document("regenerated"), snapshot("finished_success")
        )

        self.assertEqual(
            correlation.plan_and_observation,
            Verdict("different_generation", "generations_differ"),
        )

    def test_a_missing_generation(self):
        """Absent, null, empty or of another type, a generation is missing."""
        for value in (ABSENT, None, "", 5):
            with self.subTest(side="plan", value=repr(value)):
                plan = changed(plan_document("approved"), "plan_generation", value)
                correlation = correlated(plan, snapshot("running"))
                self.assertEqual(
                    correlation.plan_and_observation,
                    Verdict("unknown", "plan_generation_missing"),
                )
            with self.subTest(side="attempt", value=repr(value)):
                observed = changed(
                    snapshot("running"), "attempt.plan_generation", value
                )
                correlation = correlated(plan_document("approved"), observed)
                self.assertEqual(
                    correlation.plan_and_observation,
                    Verdict("unknown", "attempt_generation_missing"),
                )

    def test_the_plan_side_is_judged_first(self):
        plan = changed(plan_document("approved"), "plan_generation", ABSENT)
        observed = changed(snapshot("running"), "attempt.plan_generation", None)

        correlation = correlated(plan, observed)

        self.assertEqual(
            correlation.plan_and_observation.reason, "plan_generation_missing"
        )

    def test_generations_are_compared_typed(self):
        """The text ``"5"`` and the number 5 are not one generation."""
        plan = changed(plan_document("approved"), "plan_generation", "5")
        observed = changed(snapshot("running"), "attempt.plan_generation", 5)

        correlation = correlated(plan, observed)

        self.assertEqual(
            correlation.plan_and_observation,
            Verdict("unknown", "attempt_generation_missing"),
        )


class TestFinalizedAndObservation(ReaderTestCase):
    """Are the plan's finalized record and the observed attempt one execution?"""

    def test_one_execution(self):
        correlation = correlated(
            plan_document("approved_finalized"), snapshot("finished_success")
        )

        self.assertEqual(
            correlation.finalized_and_observation,
            Verdict("same_execution", "execution_ids_equal"),
        )
        self.assertEqual(correlation.problems, ())

    def test_another_execution_of_the_same_request(self):
        """One request can have several attempts: a different ID is no conflict."""
        observed = changed(
            snapshot("finished_success"),
            "attempt.execution_id",
            "20261022T110000000000Z_0002",
        )

        correlation = correlated(plan_document("approved_finalized"), observed)

        self.assertEqual(
            correlation.finalized_and_observation,
            Verdict("different_execution", "execution_ids_differ"),
        )
        self.assertEqual(correlation.problems, ())

    def test_nothing_finalized(self):
        correlation = correlated(plan_document("approved"), snapshot("running"))

        self.assertEqual(
            correlation.finalized_and_observation,
            Verdict("unknown", "nothing_finalized"),
        )

    def test_an_unusable_record_is_not_compared(self):
        """A record that disagrees with its own report, or lacks a field, is unusable."""
        for name, path, value in (
            ("its outcome disagrees", "last_finalized_execution.outcome", "failed"),
            (
                "its execution ID is absent",
                "last_finalized_execution.execution_id",
                ABSENT,
            ),
            (
                "its generation is absent",
                "last_finalized_execution.plan_generation",
                ABSENT,
            ),
        ):
            with self.subTest(name):
                plan = changed(plan_document("approved_finalized"), path, value)

                correlation = correlated(plan, snapshot("finished_success"))

                self.assertEqual(
                    correlation.finalized_and_observation,
                    Verdict("unknown", "finalized_unusable"),
                )

    def test_the_record_is_judged_before_the_observation(self):
        plan = read_plan(plan_document("approved"), plan_id=LIFECYCLE)
        self.assertEqual(
            correlate(plan, MISSING).finalized_and_observation.reason,
            "nothing_finalized",
        )

        unusable = changed(
            plan_document("approved_finalized"),
            "last_finalized_execution.report",
            ABSENT,
        )
        plan = read_plan(unusable, plan_id=LIFECYCLE)
        self.assertEqual(
            correlate(plan, MISSING).finalized_and_observation.reason,
            "finalized_unusable",
        )

    def test_a_record_without_an_execution_id_is_not_compared(self):
        """No read gives a usable record without an ID, since a usable record agrees
        with its report on every identity field; a read made by other means is
        still judged."""
        plan, observed = read_pair(
            plan_document("approved_finalized"), snapshot("finished_success")
        )
        assert plan.finalized is not None
        record = dataclasses.replace(
            plan.finalized, execution_id=Recorded(None, None, False)
        )

        correlation = correlate(dataclasses.replace(plan, finalized=record), observed)

        self.assertEqual(
            correlation.finalized_and_observation,
            Verdict("unknown", "execution_id_missing"),
        )

    def test_one_id_with_conflicting_identity_is_not_one_execution(self):
        cases = (
            (
                "generation",
                "attempt.plan_generation",
                "ffffffffffffffffffffffffffffffff",
                ["last_finalized_execution.plan_generation"],
            ),
            (
                "run token",
                "attempt.run_token",
                7,
                ["last_finalized_execution.run_token"],
            ),
        )
        for name, path, value, conflicts in cases:
            with self.subTest(name):
                observed = changed(snapshot("finished_success"), path, value)

                correlation = correlated(plan_document("approved_finalized"), observed)

                self.assertEqual(
                    correlation.finalized_and_observation,
                    Verdict("unknown", "identity_conflict"),
                )
                self.assertEqual(
                    [(p.path, p.code) for p in correlation.problems],
                    [(at, "identity_conflict") for at in conflicts],
                )
                self.assertIsNone(correlation.conflicting_endings)

    def test_each_conflicting_field_is_reported(self):
        observed = changed(
            snapshot("finished_success"), "attempt.plan_generation", "f" * 32
        )
        observed = changed(observed, "attempt.run_token", 7)

        correlation = correlated(plan_document("approved_finalized"), observed)

        self.assertEqual(
            [p.path for p in correlation.problems],
            [
                "last_finalized_execution.plan_generation",
                "last_finalized_execution.run_token",
            ],
        )

    def test_missing_metadata_is_not_a_conflict(self):
        """A value either record lacks is missing evidence, never a disagreement."""
        for name, value in (("null", None), ("true for 1", True), ("text", "0")):
            with self.subTest(name):
                observed = changed(
                    snapshot("finished_success"), "attempt.run_token", value
                )
                observed = changed(observed, "attempt.plan_generation", None)

                correlation = correlated(plan_document("approved_finalized"), observed)

                self.assertEqual(
                    correlation.finalized_and_observation,
                    Verdict("same_execution", "execution_ids_equal"),
                )
                self.assertEqual(correlation.problems, ())


class TestRequestAndObservation(ReaderTestCase):
    """Does the observed attempt belong to the plan's current request?

    Recorded identity only: the generation and the run token. The verdict
    says nothing about whether the request is valid, finished or eligible.
    """

    def test_the_current_request(self):
        correlation = correlated(plan_document("approved"), snapshot("running"))

        self.assertEqual(
            correlation.request_and_observation,
            Verdict("current", "generation_and_run_token_equal"),
        )

    def test_another_request_or_generation(self):
        regenerated = correlated(
            plan_document("regenerated"), snapshot("finished_success")
        )
        rerun = correlated(
            plan_document("rerun_requested"), snapshot("finished_success")
        )

        self.assertEqual(
            regenerated.request_and_observation, Verdict("other", "generations_differ")
        )
        self.assertEqual(
            rerun.request_and_observation, Verdict("other", "run_tokens_differ")
        )

    def test_a_difference_is_enough_whatever_the_other_value(self):
        cases = (
            (
                "generation, no attempt token",
                "attempt.plan_generation",
                "f" * 32,
                "attempt.run_token",
                None,
                "generations_differ",
            ),
            (
                "generation, attempt token absent",
                "attempt.plan_generation",
                "f" * 32,
                "attempt.run_token",
                ABSENT,
                "generations_differ",
            ),
            (
                "token, no attempt generation",
                "attempt.run_token",
                9,
                "attempt.plan_generation",
                None,
                "run_tokens_differ",
            ),
            (
                "both differ",
                "attempt.plan_generation",
                "f" * 32,
                "attempt.run_token",
                9,
                "generations_differ",
            ),
        )
        for name, path, value, other_path, other, reason in cases:
            with self.subTest(name):
                observed = changed(
                    changed(snapshot("running"), path, value), other_path, other
                )

                correlation = correlated(plan_document("approved"), observed)

                self.assertEqual(
                    correlation.request_and_observation, Verdict("other", reason)
                )

    def test_a_token_difference_wins_over_a_missing_plan_generation(self):
        plan = changed(plan_document("rerun_requested"), "plan_generation", ABSENT)

        correlation = correlated(plan, snapshot("finished_success"))

        self.assertEqual(
            correlation.request_and_observation, Verdict("other", "run_tokens_differ")
        )

    def test_missing_evidence_is_unknown(self):
        """Missing covers absent, null, the wrong type, and for a token, out of range."""
        approved, running = plan_document("approved"), snapshot("running")
        cases = (
            (
                "plan generation absent",
                changed(approved, "plan_generation", ABSENT),
                running,
                "plan_generation_missing",
            ),
            (
                "attempt generation null",
                approved,
                changed(running, "attempt.plan_generation", None),
                "attempt_generation_missing",
            ),
            (
                "plan token absent",
                changed(approved, "run_token", ABSENT),
                running,
                "plan_run_token_missing",
            ),
            (
                "plan token text",
                changed(approved, "run_token", "0"),
                running,
                "plan_run_token_missing",
            ),
            (
                "plan token true",
                changed(approved, "run_token", True),
                running,
                "plan_run_token_missing",
            ),
            (
                "plan token out of range",
                changed(approved, "run_token", 2**53),
                running,
                "plan_run_token_missing",
            ),
            (
                "attempt token null",
                approved,
                changed(running, "attempt.run_token", None),
                "attempt_run_token_missing",
            ),
            (
                "attempt token false",
                approved,
                changed(running, "attempt.run_token", False),
                "attempt_run_token_missing",
            ),
        )
        for name, plan, observed, reason in cases:
            with self.subTest(name):
                correlation = correlated(plan, observed)

                self.assertEqual(
                    correlation.request_and_observation, Verdict("unknown", reason)
                )

    def test_a_token_is_compared_typed(self):
        """``true`` is not the token 1, and the text ``"1"`` is not the number 1."""
        for name, value in (("true", True), ("text", "1"), ("a float", 1.0)):
            with self.subTest(name):
                observed = changed(
                    snapshot("finished_success"), "attempt.run_token", value
                )

                correlation = correlated(plan_document("rerun_requested"), observed)

                assert correlation.plan.tokens is not None
                self.assertEqual(correlation.plan.tokens.run, 1)
                self.assertEqual(
                    correlation.request_and_observation,
                    Verdict("unknown", "attempt_run_token_missing"),
                )

    def test_the_order_of_missing_evidence(self):
        """The plan's generation, the attempt's, the plan's token, the attempt's."""
        approved, running = plan_document("approved"), snapshot("running")
        no_plan_token = changed(approved, "run_token", ABSENT)
        no_attempt_token = changed(running, "attempt.run_token", None)
        no_attempt_generation = changed(
            no_attempt_token, "attempt.plan_generation", None
        )
        cases = (
            (
                changed(no_plan_token, "plan_generation", ABSENT),
                no_attempt_generation,
                "plan_generation_missing",
            ),
            (no_plan_token, no_attempt_generation, "attempt_generation_missing"),
            (no_plan_token, no_attempt_token, "plan_run_token_missing"),
            (approved, no_attempt_token, "attempt_run_token_missing"),
        )
        for plan, observed, reason in cases:
            with self.subTest(reason):
                correlation = correlated(plan, observed)

                self.assertEqual(
                    correlation.request_and_observation, Verdict("unknown", reason)
                )

    def test_identity_is_independent_of_the_request_being_valid_or_finished(self):
        """An attempt of the plan's generation and run token is the current request's."""
        approved = plan_document("approved")
        cases = (
            (
                "executed token unreadable",
                with_tokens(approved, 4, "x"),
                None,
                "invalid_tokens",
            ),
            ("executed above run", with_tokens(approved, 3, 4), None, "invalid_tokens"),
            ("finished", with_tokens(approved, 4, 4), True, "already_executed"),
        )
        for name, plan_body, finished, code in cases:
            with self.subTest(name):
                token = plan_body["run_token"]
                observed = changed(
                    snapshot("finished_success"), "attempt.run_token", token
                )

                correlation = correlated(plan_body, observed)

                plan = correlation.plan
                assert plan.request is not None and plan.eligibility is not None
                self.assertEqual(
                    correlation.request_and_observation,
                    Verdict("current", "generation_and_run_token_equal"),
                )
                self.assertIs(plan.request.finished, finished)
                self.assertEqual(plan.eligibility.code, code)

    def test_a_newer_request_beside_the_older_attempt(self):
        plan_body = with_tokens(plan_document("approved"), 5, 4)
        observed = changed(snapshot("finished_success"), "attempt.run_token", 4)

        correlation = correlated(plan_body, observed)

        plan = correlation.plan
        assert plan.request is not None and plan.eligibility is not None
        self.assertEqual(
            correlation.request_and_observation, Verdict("other", "run_tokens_differ")
        )
        self.assertIs(plan.request.finished, False)
        self.assertTrue(plan.eligibility.eligible)

    def test_it_agrees_with_the_finalized_record_on_the_same_evidence(self):
        """A record and an attempt of one request answer recorded identity alike."""
        cases = (
            ("current", lambda doc: doc, True, "current"),
            (
                "plan regenerated",
                lambda doc: changed(doc, "plan_generation", "f" * 32),
                False,
                "other",
            ),
            (
                "rerun requested",
                lambda doc: changed(doc, "run_token", 1),
                False,
                "other",
            ),
            (
                "plan generation absent",
                lambda doc: changed(doc, "plan_generation", ABSENT),
                None,
                "unknown",
            ),
            (
                "plan token text",
                lambda doc: changed(doc, "run_token", "0"),
                None,
                "unknown",
            ),
        )
        for name, change, current, verdict in cases:
            with self.subTest(name):
                correlation = correlated(
                    change(plan_document("approved_finalized")),
                    snapshot("finished_success"),
                )

                plan = correlation.plan
                assert plan.request is not None
                self.assertIs(plan.request.finalized_is_current, current)
                self.assertEqual(correlation.request_and_observation.verdict, verdict)


class TestRequestRows(ReaderTestCase):
    """What the plan and its observed attempt say together about a request."""

    def test_a_request_whose_attempt_was_observed_running(self):
        """Running was observed; whether anything runs now is not known."""
        plan_body = with_tokens(plan_document("approved"), 4, 3)
        observed = changed(snapshot("running"), "attempt.run_token", 4)

        correlation = correlated(plan_body, observed)

        assert correlation.plan.request is not None
        assert isinstance(correlation.snapshot, SnapshotRead)
        attempt = correlation.snapshot.attempt
        assert attempt is not None
        self.assertIs(correlation.plan.request.finished, False)
        self.assertEqual(correlation.request_and_observation.verdict, "current")
        self.assertEqual(attempt.state.value, "running")
        self.assertIs(attempt.settled, False)

    def test_a_successful_attempt_whose_completion_is_not_recorded(self):
        """Nothing is guessed about why the plan does not record it."""
        plan_body = with_tokens(plan_document("approved"), 4, 3)
        observed = changed(snapshot("finished_success"), "attempt.run_token", 4)

        correlation = correlated(plan_body, observed)

        request = correlation.plan.request
        assert request is not None
        assert isinstance(correlation.snapshot, SnapshotRead)
        assert correlation.snapshot.attempt is not None
        self.assertEqual(correlation.snapshot.attempt.outcome.value, "succeeded")
        self.assertEqual((request.finished, request.outcome), (False, None))
        self.assertEqual(correlation.request_and_observation.verdict, "current")

    def test_an_attempt_that_failed_fast_leaves_the_request_unfinished(self):
        """A finished attempt is not a finished request."""
        plan_body = with_tokens(plan_document("approved"), 4, 3)
        observed = snapshot("finished_success")
        for path, value in (
            ("attempt.run_token", 4),
            ("attempt.outcome", "failed"),
            ("attempt.termination_reason", "failed_fast"),
        ):
            observed = changed(observed, path, value)

        correlation = correlated(plan_body, observed)

        plan = correlation.plan
        assert plan.request is not None and plan.eligibility is not None
        self.assertIs(plan.request.finished, False)
        self.assertTrue(plan.eligibility.eligible)
        self.assertEqual(correlation.request_and_observation.verdict, "current")

    def test_a_newer_request_beside_the_last_finished_one(self):
        """Request 5 is not finished; request 4's result is the last finished one."""
        plan_body = with_record_token(
            with_tokens(plan_document("approved_finalized"), 5, 4), 4
        )
        observed = changed(snapshot("finished_success"), "attempt.run_token", 4)

        correlation = correlated(plan_body, observed)

        plan = correlation.plan
        assert plan.request is not None and plan.finalized is not None
        self.assertEqual((plan.request.finished, plan.request.outcome), (False, None))
        self.assertIs(plan.request.finalized_is_current, False)
        self.assertEqual(plan.finalized.outcome.value, "succeeded")
        self.assertEqual(
            correlation.finalized_and_observation.verdict, "same_execution"
        )
        self.assertEqual(
            correlation.request_and_observation, Verdict("other", "run_tokens_differ")
        )

    def test_a_bad_step_time_leaves_every_answer(self):
        plan_body = plan_document("approved_finalized")
        observed = changed(snapshot("finished_success"), "steps.demux.ts", "yesterday")

        correlation = correlated(plan_body, observed)

        assert correlation.plan.request is not None
        self.assertEqual(correlation.plan.request.outcome, "succeeded")
        self.assertEqual(
            answers(correlation),
            (
                ("same_generation", "generations_equal"),
                ("same_execution", "execution_ids_equal"),
                ("current", "generation_and_run_token_equal"),
            ),
        )


def ended(**attempt: Any) -> Correlation:
    """Correlate the finalized success with its snapshot, the attempt's fields changed."""
    observed = snapshot("finished_success")
    for field, value in attempt.items():
        observed = changed(observed, f"attempt.{field}", value)
    return correlated(plan_document("approved_finalized"), observed)


class TestConflictingEndings(ReaderTestCase):
    """Two terminal claims about one execution that disagree are both shown."""

    def test_a_conflicting_outcome(self):
        correlation = ended(outcome="failed")

        conflict = correlation.conflicting_endings
        assert conflict is not None
        self.assertEqual(conflict.finalized, Ending("succeeded", "completed"))
        self.assertEqual(conflict.observed, Ending("failed", "completed"))
        self.assertEqual(conflict.fields, ("outcome",))
        self.assertEqual(
            [(p.path, p.code) for p in correlation.problems],
            [("last_finalized_execution.outcome", "conflicting_endings")],
        )

    def test_a_conflicting_termination_reason(self):
        correlation = ended(termination_reason="cancelled")

        conflict = correlation.conflicting_endings
        assert conflict is not None
        self.assertEqual(conflict.fields, ("termination_reason",))

    def test_both_conflict(self):
        correlation = ended(outcome="failed", termination_reason="failed_fast")

        conflict = correlation.conflicting_endings
        assert conflict is not None
        self.assertEqual(conflict.fields, ("outcome", "termination_reason"))
        self.assertEqual(len(correlation.problems), 2)

    def test_no_winner_is_picked(self):
        """The plan still states what its record states."""
        correlation = ended(outcome="failed")

        assert correlation.plan.request is not None
        self.assertEqual(correlation.plan.request.outcome, "succeeded")
        assert isinstance(correlation.snapshot, SnapshotRead)
        assert correlation.snapshot.attempt is not None
        self.assertEqual(correlation.snapshot.attempt.outcome.value, "failed")

    def test_lag_is_no_conflict(self):
        """A snapshot that has not caught up with the finalized result is lag."""
        running = correlated(plan_document("approved_finalized"), snapshot("running"))
        no_outcome = ended(outcome=None, termination_reason=None)

        for correlation in (running, no_outcome):
            self.assertEqual(
                correlation.finalized_and_observation.verdict, "same_execution"
            )
            self.assertIsNone(correlation.conflicting_endings)
            self.assertEqual(correlation.problems, ())

    def test_unrecognised_values_are_incomplete_evidence(self):
        for attempt in (
            {"outcome": "partially_succeeded"},
            {"termination_reason": 5},
            {"state": "paused", "outcome": "failed"},
        ):
            with self.subTest(sorted(attempt)):
                correlation = ended(**attempt)

                self.assertIsNone(correlation.conflicting_endings)
                self.assertNotIn(
                    "conflicting_endings", [p.code for p in correlation.problems]
                )

    def test_an_identity_conflict_is_never_a_conflicting_ending(self):
        correlation = ended(outcome="failed", run_token=7)

        self.assertEqual(
            correlation.finalized_and_observation.reason, "identity_conflict"
        )
        self.assertIsNone(correlation.conflicting_endings)

    def test_another_execution_is_never_a_conflicting_ending(self):
        correlation = ended(
            outcome="failed", execution_id="20261022T110000000000Z_0002"
        )

        self.assertIsNone(correlation.conflicting_endings)


class TestCorrelateArguments(ReaderTestCase):
    """Reads of one plan, and nothing else."""

    def test_the_reads_must_be_reads(self):
        plan, observed = read_pair(plan_document("approved"), snapshot("running"))
        with self.assertRaises(TypeError):
            correlate(plan_document("approved"), observed)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            correlate(plan, snapshot("running"))  # type: ignore[arg-type]

    def test_the_reads_must_be_of_one_plan(self):
        plan = read_plan(plan_document("approved"), plan_id=LIFECYCLE)
        for plan_id, realm in (("pln_other", REALM), (LIFECYCLE, "other_realm")):
            with self.subTest(plan_id=plan_id, realm=realm):
                observed = read_snapshot(
                    snapshot("running"), plan_id=plan_id, realm=realm
                )
                with self.assertRaises(ValueError):
                    correlate(plan, observed)

    def test_a_plan_without_a_usable_realm_takes_any_snapshot_read(self):
        plan = read_plan(
            changed(plan_document("approved"), "realm", 5), plan_id=LIFECYCLE
        )
        observed = read_snapshot(
            snapshot("running"), plan_id=LIFECYCLE, realm="other_realm"
        )

        correlation = correlate(plan, observed)

        self.assertEqual(correlation.plan_and_observation.reason, "observation_invalid")


class TestCorrelationProperties(ReaderTestCase):
    """Silent, closed sets, and never changing what it was given."""

    def test_every_answer_is_from_its_closed_set(self):
        for plan_name, snapshot_name in sequence_pairs():
            with self.subTest(plan=plan_name, snapshot=snapshot_name):
                correlation = correlated(
                    plan_document(plan_name), snapshot(snapshot_name)
                )

                for name, verdicts, reasons in (
                    (
                        "plan_and_observation",
                        PLAN_OBSERVATION_VERDICTS,
                        PLAN_OBSERVATION_REASONS,
                    ),
                    (
                        "finalized_and_observation",
                        FINALIZED_OBSERVATION_VERDICTS,
                        FINALIZED_OBSERVATION_REASONS,
                    ),
                    (
                        "request_and_observation",
                        REQUEST_OBSERVATION_VERDICTS,
                        REQUEST_OBSERVATION_REASONS,
                    ),
                ):
                    answer = getattr(correlation, name)
                    self.assertIn(answer.verdict, verdicts)
                    self.assertIn(answer.reason, reasons)

    def test_the_reason_sets_are_the_documented_ones(self):
        observation = {
            "plan_not_usable",
            "lookup_impossible",
            "no_observation",
            "observation_unavailable",
            "observation_inspect_only",
            "observation_invalid",
            "legacy_projection",
        }
        self.assertEqual(
            PLAN_OBSERVATION_REASONS,
            observation
            | {
                "plan_generation_missing",
                "attempt_generation_missing",
                "generations_equal",
                "generations_differ",
            },
        )
        self.assertEqual(
            FINALIZED_OBSERVATION_REASONS,
            observation
            | {
                "nothing_finalized",
                "finalized_unusable",
                "execution_id_missing",
                "execution_ids_equal",
                "execution_ids_differ",
                "identity_conflict",
            },
        )
        self.assertEqual(
            REQUEST_OBSERVATION_REASONS,
            observation
            | {
                "generations_differ",
                "run_tokens_differ",
                "plan_generation_missing",
                "attempt_generation_missing",
                "plan_run_token_missing",
                "attempt_run_token_missing",
                "generation_and_run_token_equal",
            },
        )

    def test_correlating_is_silent_and_changes_neither_read(self):
        with self.assert_silent():
            for plan_name, snapshot_name in sequence_pairs():
                with self.subTest(plan=plan_name, snapshot=snapshot_name):
                    plan, observed = read_pair(
                        plan_document(plan_name), snapshot(snapshot_name)
                    )
                    before = (copy.deepcopy(plan), copy.deepcopy(observed))

                    correlation = correlate(plan, observed)

                    self.assertIs(correlation.plan, plan)
                    self.assertIs(correlation.snapshot, observed)
                    self.assertEqual((plan, observed), before)
                    self.assertEqual(correlation, correlate(plan, observed))

    def test_results_are_immutable(self):
        correlation = ended(outcome="failed")

        conflict = correlation.conflicting_endings
        assert conflict is not None
        for value in (
            correlation,
            correlation.plan_and_observation,
            conflict,
            conflict.finalized,
        ):
            with self.subTest(type(value).__name__):
                field = dataclasses.fields(value)[0].name
                with self.assertRaises(dataclasses.FrozenInstanceError):
                    setattr(value, field, None)
        self.assertIsInstance(correlation.problems, tuple)


if __name__ == "__main__":
    unittest.main()
