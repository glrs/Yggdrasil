"""How a stored plan document reads: plan, approval, requests and steps.

These pin what a program showing a plan gets from the reader, case by case.
Each input is a regression-corpus document, or one built by
``build_plan_document``, with one field changed; the change says what the
case is about, not what Yggdrasil normally writes. Where a case documents a
rule that a consumer might be tempted to soften (a default, an alias, a
match on the token alone), its docstring states the contract's rule.
"""

from __future__ import annotations

import unittest
from datetime import UTC, datetime
from typing import Any

from tests.contract.read_support import ReaderTestCase, plan_document
from tests.contract.support import ABSENT, assert_conforms, changed
from tests.plan_store_support import make_plan
from yggdrasil.contract import PLAN_DOCUMENT
from yggdrasil.contract.diagnostics import describe_recorded
from yggdrasil.contract.read import (
    IDENTITY_MISMATCH,
    INVALID_DOCUMENT,
    INVALID_TOKENS,
    STATUS_MISSING,
    PlanRead,
    StepCounts,
    read_plan,
)
from yggdrasil.storage.plan_documents import build_plan_document

LIFECYCLE = "pln_lifecycle"


def read(document: dict[str, Any]) -> PlanRead:
    """Read a document under the ID it is stored under."""
    return read_plan(document, plan_id=document["_id"])


def paths_and_codes(plan: PlanRead) -> list[tuple[str, str]]:
    """Return each problem of a read as (path, code), in reported order."""
    return [(problem.path, problem.code) for problem in plan.problems]


class TestPlan(ReaderTestCase):
    """The plan's identity, metadata and failure policy."""

    def test_plan_identity_and_metadata_are_read(self):
        document = build_plan_document(
            make_plan("pln_notes"),
            "test_realm",
            {"kind": "project", "id": "P12345"},
            notes="Lane QC for P12345, 4 samples",
            now="2026-10-22T09:00:00Z",
            plan_generation="5f0c9d2e7a3b4c61a2d48e0f9b1c7d55",
        )
        assert_conforms(self, PLAN_DOCUMENT, document)

        plan = read(document)

        assert plan.recorded is not None
        self.assertEqual(plan.plan_id, "pln_notes")
        self.assertEqual(plan.recorded.realm.value, "test_realm")
        self.assertEqual(
            (plan.recorded.scope_kind.value, plan.recorded.scope_id.value),
            ("project", "P12345"),
        )
        self.assertEqual(plan.recorded.notes.value, "Lane QC for P12345, 4 samples")
        self.assertEqual(plan.generation, "5f0c9d2e7a3b4c61a2d48e0f9b1c7d55")
        self.assertIsNotNone(plan.steps)
        self.assertEqual(plan.problems, ())

    def test_both_utc_spellings_become_the_same_instant(self):
        """An earlier release wrote ``+00:00``; the canonical spelling ends in ``Z``."""
        document = changed(
            plan_document("draft"), "created_at", "2026-10-22T09:00:00+00:00"
        )

        plan = read(document)

        assert plan.recorded is not None
        expected = datetime(2026, 10, 22, 9, 0, 0, tzinfo=UTC)
        self.assertEqual(plan.recorded.created_at.value, expected)
        self.assertEqual(plan.recorded.updated_at.value, expected)
        self.assertEqual(plan.problems, ())

    def test_unreadable_timestamp_is_none_and_reported(self):
        cases = (
            ("words", "yesterday"),
            ("no timezone", "2026-10-02T08:00:00"),
            ("a number", 1759392000),
            ("a list", ["2026-10-02T08:00:00Z"]),
            # At the edge of the calendar, applying the offset leaves it.
            ("before year 1 in UTC", "0001-01-01T00:00:00+05:00"),
            ("after year 9999 in UTC", "9999-12-31T23:59:59-05:00"),
        )
        for name, value in cases:
            with self.subTest(name):
                plan = read(changed(plan_document("draft"), "updated_at", value))

                assert plan.recorded is not None
                self.assertIsNone(plan.recorded.updated_at.value)
                self.assertIs(plan.recorded.updated_at.recorded, value)
                self.assertEqual(
                    paths_and_codes(plan), [("updated_at", "bad_timestamp")]
                )

    def test_a_document_without_a_generation_has_none(self):
        plan = read(changed(plan_document("draft"), "plan_generation", ABSENT))

        self.assertIsNone(plan.generation)
        self.assertEqual(paths_and_codes(plan), [("plan_generation", "missing_field")])

    def test_only_a_non_empty_string_counts_as_a_generation(self):
        for name, value in (("empty", ""), ("int", 7), ("null", None), ("list", ["a"])):
            with self.subTest(name):
                plan = read(changed(plan_document("draft"), "plan_generation", value))

                self.assertIsNone(plan.generation)
                self.assertEqual(
                    paths_and_codes(plan), [("plan_generation", "wrong_type")]
                )

    def test_failure_policy_is_read_from_the_plan(self):
        plan = read(plan_document("continue_independent_failures"))

        assert plan.failure_policy is not None
        self.assertTrue(plan.failure_policy.valid)
        self.assertEqual(plan.failure_policy.policy, "continue_independent")

    def test_an_absent_failure_policy_is_invalid_not_fail_fast(self):
        """No policy is filled in: an absent ``plan.failure_policy`` is invalid.

        It is reported once, as the failure-policy check names it, although
        the execution-definition check finds it missing too.
        """
        plan = read(changed(plan_document("draft"), "plan.failure_policy", ABSENT))

        assert plan.failure_policy is not None
        self.assertFalse(plan.failure_policy.valid)
        self.assertIsNone(plan.failure_policy.policy)
        self.assertEqual(
            paths_and_codes(plan), [("plan.failure_policy", "invalid_failure_policy")]
        )
        self.assertIsNotNone(plan.steps)

    def test_a_null_failure_policy_is_unknown_not_fail_fast(self):
        plan = read(changed(plan_document("draft"), "plan.failure_policy", None))

        assert plan.failure_policy is not None
        self.assertIsNone(plan.failure_policy.policy)
        self.assertEqual(
            [problem.message for problem in plan.problems],
            ["plan.failure_policy is null"],
        )
        self.assertIsNotNone(plan.steps)

    def test_a_plan_that_is_not_an_object_has_no_readable_policy(self):
        for name, value in (
            ("text", "see attachment"),
            ("null", None),
            ("list", ["steps"]),
            ("int", 7),
        ):
            with self.subTest(name):
                plan = read(changed(plan_document("draft"), "plan", value))

                assert plan.failure_policy is not None
                self.assertIsNone(plan.failure_policy.policy)
                self.assertIsNone(plan.steps)
                self.assertEqual([p.path for p in plan.problems], ["plan"])

    def test_a_failure_policy_that_is_not_text_is_unknown(self):
        for name, value in (
            ("list", ["fail_fast"]),
            ("object", {"policy": "fail_fast"}),
            ("int", 1),
            ("bool", True),
        ):
            with self.subTest(name):
                plan = read(
                    changed(plan_document("draft"), "plan.failure_policy", value)
                )

                assert plan.failure_policy is not None
                self.assertIsNone(plan.failure_policy.policy)
                (problem,) = plan.problems
                self.assertIn(describe_recorded(value), problem.message)

    def test_unrecognised_failure_policy_is_unknown_with_its_text(self):
        plan = read(
            changed(plan_document("draft"), "plan.failure_policy", "retry_forever")
        )

        assert plan.failure_policy is not None
        self.assertIsNone(plan.failure_policy.policy)
        (problem,) = plan.problems
        self.assertIn("'retry_forever'", problem.message)
        # One unknown word costs one answer.
        self.assertIsNotNone(plan.steps)

    def test_run_once_authority_keeps_its_owner(self):
        plan = read(plan_document("run_once_finalized"))

        assert plan.recorded is not None
        self.assertEqual(plan.recorded.execution_authority.value, "run_once")
        self.assertEqual(
            plan.recorded.execution_owner.value,
            "run_once:20cbf763-754d-f793-2135-ec3d80dde357",
        )

    def test_absent_authority_is_unknown_not_daemon(self):
        plan = read(changed(plan_document("draft"), "execution_authority", ABSENT))

        assert plan.recorded is not None
        authority = plan.recorded.execution_authority
        self.assertEqual((authority.value, authority.present), (None, False))
        self.assertEqual(
            paths_and_codes(plan), [("execution_authority", "missing_field")]
        )


class TestApproval(ReaderTestCase):
    """The approval status, its approver and its time."""

    def test_approval_status(self):
        for recorded, value in (
            ("draft", "draft"),
            ("approved", "approved"),
            ("rejected", None),
            ("APPROVED", None),
        ):
            with self.subTest(recorded):
                plan = read(changed(plan_document("draft"), "status", recorded))

                assert plan.approval is not None
                self.assertEqual(
                    (plan.approval.value, plan.approval.recorded), (value, recorded)
                )
                self.assertEqual(plan.problems, ())

    def test_pending_is_not_draft(self):
        """``pending`` is not a word of the approval vocabulary, and no alias.

        It is unknown, kept as recorded, and the plan is not approved.
        """
        plan = read(changed(plan_document("draft"), "status", "pending"))

        assert plan.approval is not None and plan.eligibility is not None
        self.assertEqual(
            (plan.approval.value, plan.approval.recorded), (None, "pending")
        )
        self.assertEqual(plan.eligibility.code, "not_approved")
        self.assertEqual(plan.problems, ())

    def test_absent_approval_status_is_unknown(self):
        plan = read(changed(plan_document("draft"), "status", ABSENT))

        assert plan.approval is not None and plan.eligibility is not None
        self.assertEqual((plan.approval.value, plan.approval.present), (None, False))
        self.assertEqual(plan.eligibility.code, STATUS_MISSING)

    def test_approver_is_present_only_when_an_actor_recorded_it(self):
        draft = read(plan_document("draft"))
        approved = read(plan_document("approved"))

        assert draft.recorded is not None and approved.recorded is not None
        self.assertFalse(draft.recorded.approved_by.present)
        self.assertFalse(draft.recorded.approved_at.present)
        self.assertEqual(approved.recorded.approved_by.value, "alice")
        self.assertEqual(
            approved.recorded.approved_at.value,
            datetime(2026, 10, 22, 9, 30, 0, tzinfo=UTC),
        )


class TestRequests(ReaderTestCase):
    """The current request, its completion and its outcome."""

    def test_a_new_plan_has_an_unfinished_request_zero(self):
        plan = read(plan_document("draft"))

        assert plan.tokens is not None and plan.request is not None
        self.assertEqual((plan.tokens.run, plan.tokens.executed), (0, -1))
        self.assertIs(plan.request.finished, False)
        self.assertIsNone(plan.request.outcome)
        self.assertIsNone(plan.finalized)

    def test_missing_tokens_are_invalid_not_request_zero(self):
        """Absent tokens are invalid control state: nothing is filled in.

        The request state is unknown and the plan is not eligible.
        """
        document = changed(plan_document("draft"), "run_token", ABSENT)
        plan = read(changed(document, "executed_run_token", ABSENT))

        assert plan.tokens is not None and plan.request is not None
        assert plan.eligibility is not None
        self.assertFalse(plan.tokens.valid)
        self.assertEqual((plan.tokens.run, plan.tokens.executed), (None, None))
        self.assertIsNone(plan.request.finished)
        self.assertEqual(plan.eligibility.code, INVALID_TOKENS)
        self.assertEqual(
            paths_and_codes(plan),
            [("run_token", "invalid_tokens"), ("executed_run_token", "invalid_tokens")],
        )

    def test_an_unreadable_token_is_not_coerced(self):
        values = (
            ("text", "1"),
            ("fraction", 1.5),
            ("bool", True),
            ("null", None),
            ("list", [1]),
            # Whole numbers a browser cannot represent exactly.
            ("above the maximum", 2**53),
            ("below the minimum", -(2**53)),
            ("huge", 10**400),
        )
        for token in ("run_token", "executed_run_token"):
            for name, value in values:
                with self.subTest(token=token, value=name):
                    plan = read(changed(plan_document("draft"), token, value))

                    assert plan.tokens is not None and plan.request is not None
                    reading = (
                        plan.tokens.run
                        if token == "run_token"
                        else plan.tokens.executed
                    )
                    self.assertIsNone(reading)
                    self.assertIsNone(plan.request.finished)
                    self.assertIsNone(plan.request.outcome)
                    self.assertEqual(paths_and_codes(plan), [(token, "invalid_tokens")])

    def test_a_negative_token_is_invalid_except_an_executed_minus_one(self):
        """Only ``executed_run_token`` may be negative, and only -1."""
        cases = (
            ("run_token", -1),
            ("run_token", -5),
            ("executed_run_token", -2),
            ("executed_run_token", -(2**53 - 1)),
        )
        for token, value in cases:
            with self.subTest(token=token, value=value):
                plan = read(changed(plan_document("draft"), token, value))

                assert plan.tokens is not None and plan.request is not None
                assert plan.eligibility is not None
                self.assertFalse(plan.tokens.valid)
                self.assertIsNone(plan.request.finished)
                self.assertEqual(plan.eligibility.code, INVALID_TOKENS)
                self.assertEqual(paths_and_codes(plan), [(token, "invalid_tokens")])

    def test_the_largest_exact_whole_number_is_still_a_token(self):
        plan = read(changed(plan_document("draft"), "run_token", 2**53 - 1))

        assert plan.tokens is not None
        self.assertEqual(plan.tokens.run, 2**53 - 1)
        self.assertEqual(plan.problems, ())

    def test_a_finished_request_reports_its_recorded_outcome(self):
        plan = read(plan_document("continue_independent_failures"))

        assert plan.tokens is not None and plan.request is not None
        assert plan.finalized is not None
        self.assertEqual((plan.tokens.run, plan.tokens.executed), (0, 0))
        self.assertIs(plan.request.finished, True)
        self.assertEqual(plan.request.outcome, "failed")
        self.assertEqual(plan.request.termination_reason, "completed")
        self.assertIs(plan.request.finalized_is_current, True)
        finalized = plan.finalized
        self.assertTrue(finalized.usable)
        self.assertEqual(finalized.execution_id.value, "20261022T100000000000Z_0001")
        self.assertEqual(finalized.run_token.value, 0)
        self.assertEqual(finalized.outcome.value, "failed")
        self.assertEqual(finalized.termination_reason.value, "completed")
        self.assertEqual(finalized.failure_policy.value, "continue_independent")
        self.assertEqual(
            finalized.finalized_at.value, datetime(2026, 10, 22, 10, 0, 0, tzinfo=UTC)
        )
        self.assertEqual(
            finalized.counts,
            StepCounts(succeeded=2, reused=0, failed=1, blocked=1, unreached=0),
        )

        succeeded = read(plan_document("approved_finalized"))
        assert succeeded.request is not None
        self.assertEqual(succeeded.request.outcome, "succeeded")

    def test_an_unfinished_rerun_does_not_inherit_the_older_outcome(self):
        plan = read(plan_document("rerun_requested"))

        assert plan.tokens is not None and plan.request is not None
        assert plan.finalized is not None and plan.recorded is not None
        self.assertEqual((plan.tokens.run, plan.tokens.executed), (1, 0))
        self.assertIs(plan.request.finished, False)
        self.assertIsNone(plan.request.outcome)
        self.assertIs(plan.request.finalized_is_current, False)
        self.assertEqual(plan.finalized.run_token.value, 0)
        self.assertEqual(plan.finalized.outcome.value, "succeeded")
        self.assertEqual(plan.recorded.run_requested_by.value, "bob")
        self.assertEqual(
            plan.recorded.run_requested_at.value,
            datetime(2026, 10, 22, 11, 0, 0, tzinfo=UTC),
        )

    def test_equal_tokens_without_a_matching_record_give_no_outcome(self):
        """Finished is not succeeded: the record must be this request's own."""
        plan = read(changed(plan_document("rerun_requested"), "executed_run_token", 1))

        assert plan.request is not None
        self.assertIs(plan.request.finished, True)
        self.assertIsNone(plan.request.outcome)
        self.assertIs(plan.request.finalized_is_current, False)

    def test_a_record_of_another_generation_or_plan_gives_no_outcome(self):
        """A record belongs to the request only if plan ID, generation and token match.

        A record carrying the current token but another ``plan_generation``
        or ``plan_id`` is the last finalized result of something else, not
        the current request's outcome. The record agrees with its report, so
        it stays usable and visible.
        """
        for field, value in (
            ("plan_generation", "0123456789abcdef0123456789abcdef"),
            ("plan_id", "pln_other"),
        ):
            with self.subTest(field):
                document = plan_document("approved_finalized")
                document = changed(document, f"last_finalized_execution.{field}", value)
                document = changed(
                    document, f"last_finalized_execution.report.{field}", value
                )

                plan = read(document)

                assert plan.request is not None and plan.finalized is not None
                self.assertIs(plan.request.finished, True)
                self.assertIsNone(plan.request.outcome)
                self.assertIsNone(plan.request.termination_reason)
                self.assertIs(plan.request.finalized_is_current, False)
                self.assertTrue(plan.finalized.usable)
                self.assertEqual(plan.problems, ())

    def test_a_request_finished_without_any_record_has_no_outcome(self):
        plan = read(
            changed(
                plan_document("approved_finalized"), "last_finalized_execution", ABSENT
            )
        )

        assert plan.request is not None
        self.assertIs(plan.request.finished, True)
        self.assertIsNone(plan.request.outcome)
        self.assertIsNone(plan.request.finalized_is_current)
        self.assertIsNone(plan.finalized)

    def test_unreadable_recorded_counts_are_none_and_reported(self):
        path = "last_finalized_execution.report.counts"
        for name, value, code in (
            ("negative", -1, "out_of_range"),
            ("above the maximum", 2**53, "out_of_range"),
            ("huge", 10**400, "out_of_range"),
            ("text", "4", "wrong_type"),
            ("bool", True, "wrong_type"),
            ("null", None, "wrong_type"),
            ("absent", ABSENT, "missing_field"),
        ):
            with self.subTest(name):
                document = changed(
                    plan_document("continue_independent_failures"),
                    f"{path}.succeeded",
                    value,
                )

                plan = read(document)

                assert plan.finalized is not None and plan.request is not None
                self.assertIsNone(plan.finalized.counts)
                self.assertEqual(paths_and_codes(plan), [(path, code)])
                # The rest of the record is still read.
                self.assertEqual(plan.finalized.outcome.value, "failed")
                self.assertEqual(plan.request.outcome, "failed")

    def test_a_malformed_finalized_record_is_reported_not_guessed(self):
        plan = read(
            changed(
                plan_document("rerun_requested"),
                "last_finalized_execution",
                "succeeded",
            )
        )

        assert plan.finalized is not None and plan.request is not None
        self.assertFalse(plan.finalized.usable)
        self.assertIsNone(plan.finalized.outcome.value)
        self.assertEqual(plan.finalized.recorded, "succeeded")
        self.assertIsNone(plan.request.finalized_is_current)
        self.assertEqual(
            paths_and_codes(plan), [("last_finalized_execution", "wrong_type")]
        )

    def test_unrecognised_finalized_outcome_is_unknown(self):
        document = plan_document("approved_finalized")
        for path in ("outcome", "report.outcome"):
            document = changed(
                document, f"last_finalized_execution.{path}", "partially_succeeded"
            )

        plan = read(document)

        assert plan.finalized is not None and plan.request is not None
        self.assertTrue(plan.finalized.usable)
        self.assertEqual(
            (plan.finalized.outcome.value, plan.finalized.outcome.recorded),
            (None, "partially_succeeded"),
        )
        self.assertIs(plan.request.finalized_is_current, True)
        self.assertIsNone(plan.request.outcome)


class TestPlannedSteps(ReaderTestCase):
    """The planned-step inventory: complete, in stored order, or not at all."""

    def test_steps_keep_stored_order_and_dependencies(self):
        plan = read(plan_document("continue_independent_failures"))

        assert plan.steps is not None
        self.assertEqual(
            [step.step_id for step in plan.steps],
            ["lane1_demux", "lane1_upload", "lane2_demux", "lane2_upload"],
        )
        self.assertEqual(plan.steps[1].name, "lane1_upload")
        self.assertEqual(
            [(ref.step_id, ref.found) for ref in plan.steps[3].deps],
            [("lane2_demux", True)],
        )
        self.assertEqual(plan.steps[0].deps, ())

    def test_a_dependency_on_an_unknown_step_stays_visible(self):
        plan = read(
            changed(
                plan_document("draft"), "plan.steps.1.deps", ["demux", "no_such_step"]
            )
        )

        assert plan.steps is not None
        self.assertEqual(
            [(ref.step_id, ref.found) for ref in plan.steps[1].deps],
            [("demux", True), ("no_such_step", False)],
        )
        self.assertEqual(plan.problems, ())

    def test_absent_steps_or_dependencies_leave_the_inventory_unavailable(self):
        """An absent ``plan.steps`` or step ``deps`` is not an empty one.

        The inventory is not returned, and the problem is at the path. An
        explicit empty list is known empty.
        """
        plan = read(changed(plan_document("draft"), "plan.steps.0.deps", ABSENT))
        self.assertIsNone(plan.steps)
        self.assertEqual([p.path for p in plan.problems], ["plan.steps[0].deps"])

        plan = read(changed(plan_document("draft"), "plan.steps", ABSENT))
        self.assertIsNone(plan.steps)
        self.assertEqual([p.path for p in plan.problems], ["plan.steps"])

        plan = read(changed(plan_document("draft"), "plan.steps", []))
        self.assertEqual(plan.steps, ())
        self.assertEqual(plan.problems, ())

        plan = read(changed(plan_document("draft"), "plan.steps.1.deps", []))
        assert plan.steps is not None
        self.assertEqual(plan.steps[1].deps, ())
        self.assertEqual(plan.problems, ())

    def test_a_malformed_step_inventory_is_unavailable_as_a_whole(self):
        """A partly readable inventory is never returned as if it were complete."""
        malformations = (
            ("plan is text", "plan", "see attachment"),
            ("plan is missing", "plan", ABSENT),
            ("steps are an object", "plan.steps", {"a": {}}),
            ("a step is text", "plan.steps.1", "run_cellranger"),
            ("a step has no ID", "plan.steps.1.step_id", ABSENT),
            ("a step ID is empty", "plan.steps.1.step_id", ""),
            ("a step ID is a number", "plan.steps.1.step_id", 2),
            ("a step ID repeats", "plan.steps.1.step_id", "demux"),
            ("dependencies are text", "plan.steps.1.deps", "demux"),
            ("a dependency is a number", "plan.steps.1.deps", ["demux", 3]),
        )
        for name, path, value in malformations:
            with self.subTest(name):
                plan = read(changed(plan_document("draft"), path, value))

                self.assertIsNone(plan.steps)
                self.assertTrue(plan.problems)
                for problem in plan.problems:
                    self.assertTrue(problem.path.startswith("plan"), problem.path)
                # The rest of the document is still read.
                assert plan.recorded is not None and plan.tokens is not None
                self.assertTrue(plan.usable)
                self.assertEqual(plan.recorded.realm.value, "test_realm")
                self.assertEqual(plan.recorded.status.value, "draft")
                self.assertEqual(plan.tokens.run, 0)


class TestUninterpretableDocuments(ReaderTestCase):
    """Documents from which nothing is derived; the reader returns, never raises."""

    def test_a_document_that_is_not_an_object_is_invalid(self):
        for name, body in (
            ("null", None),
            ("text", "text"),
            ("list", ["a"]),
            ("int", 3),
        ):
            with self.subTest(name):
                plan = read_plan(body, plan_id=LIFECYCLE)

                assert plan.eligibility is not None
                self.assertFalse(plan.usable)
                self.assertEqual(paths_and_codes(plan), [("", "wrong_type")])
                self.assertEqual(plan.eligibility.code, INVALID_DOCUMENT)
                self.assertIs(plan.document, body)

    def test_a_document_stored_under_another_id_is_invalid(self):
        for name, own_id in (
            ("another ID", "pln_other"),
            ("empty", ""),
            ("null", None),
            ("int", 5),
            ("absent", ABSENT),
        ):
            with self.subTest(name):
                plan = read_plan(
                    changed(plan_document("draft"), "_id", own_id), plan_id=LIFECYCLE
                )

                assert plan.eligibility is not None
                self.assertFalse(plan.usable)
                self.assertEqual(paths_and_codes(plan), [("_id", "identity_mismatch")])
                self.assertEqual(plan.eligibility.code, IDENTITY_MISMATCH)


class TestRecordedValuesInMessages(ReaderTestCase):
    """How a problem names the stored value it could not read."""

    def test_a_message_tells_text_from_other_values(self):
        values = (
            ("digit text", "0"),
            ("zero", 0),
            ("fraction", 0.5),
            ("empty text", ""),
            ("bool", True),
            ("list", ["step.progress"]),
            ("object", {"kind": "flowcell"}),
            ("quoted text", 'say "hi"'),
            ("non-ASCII text", "flödescell"),
        )
        messages = {}
        for name, value in values:
            with self.subTest(name):
                plan = read_plan(
                    changed(plan_document("draft"), "_id", value), plan_id=LIFECYCLE
                )

                (problem,) = plan.problems
                self.assertIn(describe_recorded(value), problem.message)
                messages[name] = problem.message
        self.assertNotEqual(messages["digit text"], messages["zero"])
        self.assertIn("'0'", messages["digit text"])

    def test_a_value_that_cannot_be_printed_is_still_described(self):
        for name, value, shown in (
            ("int", 10**5000, "<unrepresentable int>"),
            ("list", [10**5000], "<unrepresentable list>"),
        ):
            with self.subTest(name):
                plan = read_plan(
                    changed(plan_document("draft"), "_id", value), plan_id=LIFECYCLE
                )

                (problem,) = plan.problems
                self.assertIn(shown, problem.message)

    def test_the_recorded_value_of_a_field_is_not_quoted(self):
        """A recorded value is kept as stored; only messages quote it."""
        document = changed(plan_document("draft"), "status", "pending")

        plan = read(document)

        assert plan.approval is not None and plan.eligibility is not None
        self.assertIs(plan.approval.recorded, document["status"])
        self.assertIn("'pending'", plan.eligibility.reason)

    def test_an_unreadable_value_is_named_as_it_was_stored(self):
        document = plan_document("draft")
        for path, value in (
            ("run_token", "0"),
            ("updated_at", "2026-10-01 09:12"),
            ("approved_at", 1759306231),
            ("notes", 7),
        ):
            document = changed(document, path, value)

        plan = read(document)

        self.assertEqual(
            sorted(paths_and_codes(plan)),
            [
                ("approved_at", "bad_timestamp"),
                ("notes", "wrong_type"),
                ("run_token", "invalid_tokens"),
                ("updated_at", "bad_timestamp"),
            ],
        )
        messages = {problem.path: problem.message for problem in plan.problems}
        self.assertIn("'0'", messages["run_token"])
        self.assertIn("'2026-10-01 09:12'", messages["updated_at"])
        self.assertIn("1759306231", messages["approved_at"])
        self.assertIn(" 7 ", messages["notes"])

    def test_a_readable_value_is_no_problem(self):
        document = changed(plan_document("draft"), "notes", "0")

        plan = read(document)

        assert plan.recorded is not None and plan.tokens is not None
        self.assertEqual(plan.tokens.run, 0)
        self.assertEqual(plan.recorded.notes.value, "0")
        self.assertEqual(plan.problems, ())


if __name__ == "__main__":
    unittest.main()
