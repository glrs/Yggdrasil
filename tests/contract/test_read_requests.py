"""A plan's requests: which endings finish one, completion, outcome and ownership.

Plan documents start from the regression corpus's finalized documents and
change only the tokens or record fields each case is about.
"""

from __future__ import annotations

import itertools
import unittest
from typing import Any

from tests.contract.read_support import (
    HOSTILE,
    ReaderTestCase,
    plan_document,
    plan_document_names,
)
from tests.contract.support import ABSENT, changed
from yggdrasil.contract.fields import FINALIZATION_IDENTITY_FIELDS
from yggdrasil.contract.read import (
    ALREADY_EXECUTED,
    ELIGIBLE,
    INVALID_TOKENS,
    PlanRead,
    finishes_request,
    read_plan,
    request_state,
)
from yggdrasil.flow.model import CONTINUE_INDEPENDENT_POLICY
from yggdrasil.flow.outcomes import ExecutionOutcome, TerminationReason
from yggdrasil.storage import plan_updates

LIFECYCLE = "pln_lifecycle"
RECORD = "last_finalized_execution"
OTHER_GENERATION = "0123456789abcdef0123456789abcdef"

# Every enum value the finalizer can pass, and policies known and unknown.
POLICIES = ("fail_fast", "continue_independent", "retry_forever", "")


def finishes_request_with_enums(
    outcome: ExecutionOutcome,
    termination_reason: TerminationReason,
    failure_policy: str,
) -> bool:
    """Return the enum truth table the finalizer has always applied.

    Copied verbatim, so the comparison stays meaningful whatever the
    finalizer's own function later delegates to.
    """
    if outcome is ExecutionOutcome.SUCCEEDED:
        return termination_reason is TerminationReason.COMPLETED
    return failure_policy == CONTINUE_INDEPENDENT_POLICY and termination_reason in (
        TerminationReason.COMPLETED,
        TerminationReason.PREFLIGHT_REJECTED,
    )


def read(document: Any) -> PlanRead:
    """Read a document under the lifecycle plan's ID."""
    return read_plan(document, plan_id=LIFECYCLE)


def finalized(
    run: Any, executed: Any, *, record_token: Any = 4, **record: Any
) -> dict[str, Any]:
    """Return the finalized lifecycle plan with its tokens and record changed.

    Args:
        run: The plan's ``run_token``.
        executed: The plan's ``executed_run_token``.
        record_token: The record's ``run_token``, set in the record and in
            its report alike.
        **record: Other identity fields to set in the record and its report.

    Returns:
        dict[str, Any]: The document, its record consistent with its report.
    """
    document = plan_document("approved_finalized")
    document = changed(document, "run_token", run)
    document = changed(document, "executed_run_token", executed)
    for field, value in {"run_token": record_token, **record}.items():
        document = changed(document, f"{RECORD}.{field}", value)
        document = changed(document, f"{RECORD}.report.{field}", value)
    return document


class TestFinishesRequest(unittest.TestCase):
    """Which endings finish a request: today's truth table, on plain strings."""

    def test_a_successful_completion_finishes_under_any_policy(self):
        for name, policy in (
            ("fail_fast", "fail_fast"),
            ("continue_independent", "continue_independent"),
            ("unknown", "retry_forever"),
            ("empty", ""),
            ("null", None),
            ("empty list", []),
            ("empty object", {}),
            ("list of a policy", ["continue_independent"]),
        ):
            with self.subTest(name):
                self.assertIs(finishes_request("succeeded", "completed", policy), True)

    def test_success_ending_any_other_way_finishes_nothing(self):
        for reason in (
            "failed_fast",
            "cancelled",
            "preflight_rejected",
            "orchestration_error",
            "COMPLETED",
            "",
            None,
            5,
            ["completed"],
        ):
            for policy in ("fail_fast", "continue_independent"):
                with self.subTest(reason=str(reason), policy=policy):
                    self.assertIs(finishes_request("succeeded", reason, policy), False)

    def test_a_drained_or_rejected_failure_finishes_only_continue_independent(self):
        for reason in ("completed", "preflight_rejected"):
            with self.subTest(reason):
                self.assertIs(
                    finishes_request("failed", reason, "continue_independent"), True
                )
                for policy in (
                    "fail_fast",
                    "retry_forever",
                    "Continue_Independent",
                    "",
                    None,
                    ["continue_independent"],
                    {},
                ):
                    self.assertIs(finishes_request("failed", reason, policy), False)

    def test_a_failure_ending_any_other_way_finishes_nothing(self):
        for reason in ("failed_fast", "cancelled", "orchestration_error", "x", None, 1):
            for policy in ("fail_fast", "continue_independent"):
                with self.subTest(reason=str(reason), policy=policy):
                    self.assertIs(finishes_request("failed", reason, policy), False)

    def test_any_other_outcome_finishes_nothing(self):
        for outcome in (
            "partially_succeeded",
            "SUCCEEDED",
            "",
            None,
            1,
            True,
            ["succeeded"],
            {"succeeded": 1},
        ):
            for reason, policy in itertools.product(
                ("completed", "preflight_rejected"), POLICIES
            ):
                with self.subTest(outcome=str(outcome), reason=reason, policy=policy):
                    self.assertIs(finishes_request(outcome, reason, policy), False)

    def test_no_argument_makes_it_raise(self):
        words = (
            ("succeeded", "succeeded"),
            ("failed", "failed"),
            ("completed", "completed"),
        )
        for (name, value), position in itertools.product((*HOSTILE, *words), range(3)):
            arguments = ["failed", "completed", "continue_independent"]
            arguments[position] = value
            with self.subTest(value=name, position=position):
                self.assertIsInstance(finishes_request(*arguments), bool)

    def test_it_agrees_with_the_enum_truth_table_on_every_enum_input(self):
        for outcome, reason, policy in itertools.product(
            ExecutionOutcome, TerminationReason, POLICIES
        ):
            with self.subTest(
                outcome=outcome.value, reason=reason.value, policy=policy
            ):
                expected = finishes_request_with_enums(outcome, reason, policy)
                self.assertIs(
                    finishes_request(outcome.value, reason.value, policy), expected
                )
                self.assertIs(
                    plan_updates.finishes_request(outcome, reason, policy), expected
                )


class TestRequestStateRows(ReaderTestCase):
    """What the plan alone says about its current request, row by row."""

    def assert_request(
        self,
        plan: PlanRead,
        finished: bool | None,
        outcome: str | None,
        current: bool | None,
    ) -> None:
        """Assert a read's request state."""
        assert plan.request is not None
        self.assertIs(plan.request.finished, finished)
        self.assertEqual(plan.request.outcome, outcome)
        self.assertIs(plan.request.finalized_is_current, current)
        if outcome is None:
            self.assertIsNone(plan.request.termination_reason)

    def test_an_unfinished_request_without_a_record(self):
        document = changed(
            changed(plan_document("approved"), "run_token", 4), "executed_run_token", 3
        )

        plan = read(document)

        self.assert_request(plan, False, None, None)
        self.assertIsNone(plan.finalized)

    def test_a_finished_request_that_failed(self):
        document = plan_document("continue_independent_failures")
        document = changed(document, "run_token", 4)
        document = changed(document, "executed_run_token", 4)
        for path in (f"{RECORD}.run_token", f"{RECORD}.report.run_token"):
            document = changed(document, path, 4)

        plan = read_plan(document, plan_id="pln_lanes")

        self.assert_request(plan, True, "failed", True)
        assert plan.request is not None
        self.assertEqual(plan.request.termination_reason, "completed")

    def test_a_finished_request_without_a_usable_record(self):
        plan = read(changed(finalized(4, 4), RECORD, ABSENT))
        self.assert_request(plan, True, None, None)
        self.assertIsNone(plan.finalized)

        plan = read(changed(finalized(4, 4), f"{RECORD}.outcome", "failed"))
        self.assert_request(plan, True, None, None)
        assert plan.finalized is not None
        self.assertFalse(plan.finalized.usable)

    def test_a_finished_request_whose_record_is_of_another_generation_or_plan(self):
        for name, fields in (
            ("generation", {"plan_generation": OTHER_GENERATION}),
            ("plan", {"plan_id": "pln_other"}),
        ):
            with self.subTest(name):
                plan = read(finalized(4, 4, **fields))

                self.assert_request(plan, True, None, False)
                assert plan.finalized is not None
                self.assertTrue(plan.finalized.usable)

    def test_a_rerun_after_a_finished_request(self):
        plan = read(finalized(5, 4))

        self.assert_request(plan, False, None, False)
        assert plan.finalized is not None
        self.assertTrue(plan.finalized.usable)
        self.assertEqual(plan.finalized.run_token.value, 4)
        self.assertEqual(plan.finalized.outcome.value, "succeeded")

    def test_invalid_tokens_leave_the_request_unknown(self):
        for name, run, executed in (
            ("executed above run", 3, 4),
            ("run absent", ABSENT, 4),
            ("executed text", 4, "4"),
            ("run a bool", True, 0),
            ("run a float", 4.0, 4),
        ):
            with self.subTest(name):
                plan = read(finalized(run, executed))

                assert plan.request is not None and plan.eligibility is not None
                self.assertIsNone(plan.request.finished)
                self.assertIsNone(plan.request.outcome)
                self.assertEqual(plan.eligibility.code, INVALID_TOKENS)


class TestCompletionGuardAndRecordedIdentity(ReaderTestCase):
    """An outcome needs recorded completion and the current request's own record.

    Whether the record belongs to the current request is judged from recorded
    identity alone, whatever the tokens say about completion.
    """

    def assert_case(
        self,
        document: dict[str, Any],
        *,
        finished: bool | None,
        current: bool | None,
        code: str,
        record: str,
    ) -> None:
        """Assert a case's request state, eligibility and record.

        Args:
            document: The plan document.
            finished: The expected ``finished``.
            current: The expected ``finalized_is_current``.
            code: The expected eligibility code.
            record: ``usable``, ``unusable`` or ``absent``.
        """
        plan = read(document)

        assert plan.request is not None and plan.eligibility is not None
        self.assertIs(plan.request.finished, finished)
        self.assertIsNone(plan.request.outcome)
        self.assertIsNone(plan.request.termination_reason)
        self.assertIs(plan.request.finalized_is_current, current)
        self.assertEqual(plan.eligibility.code, code)
        if record == "absent":
            self.assertIsNone(plan.finalized)
        else:
            assert plan.finalized is not None
            self.assertIs(plan.finalized.usable, record == "usable")

    def test_a_record_never_establishes_completion(self):
        self.assert_case(
            finalized(4, 3),
            finished=False,
            current=True,
            code=ELIGIBLE,
            record="usable",
        )

    def test_identity_holds_while_the_token_pair_is_invalid(self):
        self.assert_case(
            finalized(4, "x"),
            finished=None,
            current=True,
            code=INVALID_TOKENS,
            record="usable",
        )

    def test_a_positive_difference_is_a_mismatch(self):
        cases = (
            ("the record's plan ID", finalized(4, 4, plan_id="pln_other")),
            (
                "the record's generation",
                finalized(4, 4, plan_generation=OTHER_GENERATION),
            ),
            ("the record's run token", finalized(4, 4, record_token=3)),
            (
                "the run token, the plan's generation missing",
                changed(finalized(4, 4, record_token=3), "plan_generation", ABSENT),
            ),
            (
                "the generation, the plan's run token not counting",
                changed(
                    finalized(4, 4, plan_generation=OTHER_GENERATION), "run_token", "4"
                ),
            ),
        )
        for name, document in cases:
            with self.subTest(name):
                plan = read(document)

                assert plan.request is not None
                self.assertIs(plan.request.finalized_is_current, False)
                self.assertIsNone(plan.request.outcome)

    def test_insufficient_evidence_is_no_verdict(self):
        cases = (
            (
                "the plan's generation missing",
                changed(finalized(4, 4), "plan_generation", ABSENT),
                True,
                ALREADY_EXECUTED,
                "usable",
            ),
            (
                "the plan's generation of the wrong type",
                changed(finalized(4, 4), "plan_generation", 5),
                True,
                ALREADY_EXECUTED,
                "usable",
            ),
            (
                "the plan's run token as text",
                finalized("4", 4),
                None,
                INVALID_TOKENS,
                "usable",
            ),
            (
                "the plan's run token a bool",
                finalized(True, 0, record_token=1),
                None,
                INVALID_TOKENS,
                "usable",
            ),
            (
                "no record",
                changed(finalized(4, 4), RECORD, ABSENT),
                True,
                ALREADY_EXECUTED,
                "absent",
            ),
            (
                "a record disagreeing with its report",
                changed(finalized(4, 4), f"{RECORD}.report.execution_id", "other"),
                True,
                ALREADY_EXECUTED,
                "unusable",
            ),
        )
        for name, document, finished, code, record in cases:
            with self.subTest(name):
                self.assert_case(
                    document, finished=finished, current=None, code=code, record=record
                )

    def test_an_outcome_needs_completion_and_the_current_record(self):
        plan = read(finalized(4, 4))

        assert plan.request is not None
        self.assertIs(plan.request.finished, True)
        self.assertIs(plan.request.finalized_is_current, True)
        self.assertEqual(plan.request.outcome, "succeeded")
        self.assertEqual(plan.request.termination_reason, "completed")

    def test_a_record_token_out_of_range_does_not_count(self):
        for name, value, code in (
            ("negative", -1, "out_of_range"),
            ("above the maximum", 2**53, "out_of_range"),
        ):
            with self.subTest(name):
                plan = read(finalized(4, 4, record_token=value))

                assert plan.finalized is not None and plan.request is not None
                self.assertTrue(plan.finalized.usable)
                self.assertIsNone(plan.finalized.run_token.value)
                self.assertIsNone(plan.request.finalized_is_current)
                self.assertIn(
                    (f"{RECORD}.run_token", code),
                    [(problem.path, problem.code) for problem in plan.problems],
                )


class TestRecordAgainstItsReport(ReaderTestCase):
    """A finalized record is usable only if it agrees with its report on its identity."""

    @staticmethod
    def variations(field: str) -> tuple[tuple[str, Any], ...]:
        """Return the ways one side's value of field can fail to agree."""
        if field == "run_token":
            return (
                ("absent", ABSENT),
                ("null", None),
                ("text", "0"),
                ("equal float", 0.0),
                ("bool", False),
                ("different", 7),
            )
        return (
            ("absent", ABSENT),
            ("null", None),
            ("int", 5),
            ("empty", ""),
            ("different", "other"),
        )

    def assert_disagreement(self, document: dict[str, Any], fields: list[str]) -> None:
        """Assert the record is unusable, disagreeing on exactly these fields."""
        plan = read(document)

        assert plan.finalized is not None and plan.request is not None
        self.assertFalse(plan.finalized.usable)
        self.assertEqual(plan.finalized.disagreeing, tuple(sorted(fields)))
        self.assertIsNone(plan.request.outcome)
        self.assertIsNone(plan.request.finalized_is_current)
        disagreements = [
            problem
            for problem in plan.problems
            if problem.code == "record_report_disagree"
        ]
        self.assertEqual(len(disagreements), 1)
        self.assertEqual(disagreements[0].path, RECORD)
        for field in fields:
            self.assertIn(field, disagreements[0].message)

    def test_each_identity_field_on_either_side(self):
        for field in sorted(FINALIZATION_IDENTITY_FIELDS):
            for side in ("", "report."):
                for name, value in self.variations(field):
                    with self.subTest(field=field, side=side or "record", value=name):
                        document = changed(
                            plan_document("approved_finalized"),
                            f"{RECORD}.{side}{field}",
                            value,
                        )
                        self.assert_disagreement(document, [field])

    def test_types_are_checked_before_values(self):
        for name, field, ours, theirs in (
            ("true against 1", "run_token", True, 1),
            ("1 against true", "run_token", 1, True),
            ("true on both sides", "run_token", True, True),
            ("text against an int", "execution_id", "5", 5),
            ("an int on both sides", "execution_id", 5, 5),
        ):
            with self.subTest(name):
                document = changed(
                    plan_document("approved_finalized"), f"{RECORD}.{field}", ours
                )
                document = changed(document, f"{RECORD}.report.{field}", theirs)
                self.assert_disagreement(document, [field])

    def test_every_disagreeing_field_is_named_in_order(self):
        document = plan_document("approved_finalized")
        for path, value in (
            ("termination_reason", "cancelled"),
            ("execution_id", "other"),
            ("report.outcome", "failed"),
        ):
            document = changed(document, f"{RECORD}.{path}", value)

        self.assert_disagreement(
            document, ["execution_id", "outcome", "termination_reason"]
        )

    def test_a_report_that_is_not_an_object_makes_the_record_unusable(self):
        for name, value, code in (
            ("absent", ABSENT, "missing_field"),
            ("null", None, "wrong_type"),
            ("text", "see the spool", "wrong_type"),
            ("list", [], "wrong_type"),
        ):
            with self.subTest(name):
                plan = read(
                    changed(
                        plan_document("approved_finalized"), f"{RECORD}.report", value
                    )
                )

                assert plan.finalized is not None and plan.request is not None
                self.assertFalse(plan.finalized.usable)
                self.assertEqual(plan.finalized.disagreeing, ())
                self.assertIsNone(plan.finalized.counts)
                self.assertIsNone(plan.request.outcome)
                self.assertEqual(
                    [(problem.path, problem.code) for problem in plan.problems],
                    [(f"{RECORD}.report", code)],
                )

    def test_every_finalized_corpus_document_agrees_with_its_report(self):
        checked = 0
        for name in plan_document_names():
            document = plan_document(name)
            plan = read_plan(document, plan_id=document["_id"])
            if not plan.usable or plan.finalized is None:
                continue
            with self.subTest(name):
                self.assertTrue(plan.finalized.usable)
                self.assertEqual(plan.finalized.disagreeing, ())
                # A usable record of the corpus always has a decided identity.
                assert plan.request is not None
                self.assertIsNotNone(plan.request.finalized_is_current)
                checked += 1
        self.assertGreaterEqual(checked, 8)


class TestTypedEquality(ReaderTestCase):
    """``true`` never equals ``1``, and ``"5"`` never equals ``5``, in any comparison."""

    def test_the_run_token_in_recorded_identity(self):
        for name, plan_token, record_token in (
            ("plan true, record 1", True, 1),
            ("plan 1, record true", 1, True),
            ("plan '1', record 1", "1", 1),
            ("plan 1, record '1'", 1, "1"),
        ):
            with self.subTest(name):
                plan = read(finalized(plan_token, 0, record_token=record_token))

                assert plan.request is not None
                self.assertIsNot(plan.request.finalized_is_current, True)
                self.assertIsNone(plan.request.outcome)

    def test_the_generation_in_recorded_identity(self):
        for name, plan_generation, record_generation in (
            ("plan 5, record '5'", 5, "5"),
            ("plan '5', record 5", "5", 5),
            ("plan true, record 'True'", True, "True"),
        ):
            with self.subTest(name):
                document = finalized(4, 4, plan_generation=record_generation)
                document = changed(document, "plan_generation", plan_generation)

                plan = read(document)

                assert plan.request is not None
                self.assertIsNot(plan.request.finalized_is_current, True)
                self.assertIsNone(plan.request.outcome)

    def test_matching_values_of_the_contract_type_do_match(self):
        document = finalized(4, 4, plan_generation="5")
        document = changed(document, "plan_generation", "5")

        plan = read(document)

        assert plan.request is not None
        self.assertIs(plan.request.finalized_is_current, True)
        self.assertEqual(plan.request.outcome, "succeeded")


class TestRequestStateFunction(ReaderTestCase):
    """``request_state`` is the request state of ``read_plan``, under an expected ID."""

    def test_it_is_the_request_of_read_plan(self):
        for name in plan_document_names():
            with self.subTest(name):
                document = plan_document(name)
                self.assertEqual(
                    request_state(document, plan_id=document["_id"]),
                    read_plan(document, plan_id=document["_id"]).request,
                )

    def test_a_document_read_under_another_id_has_no_request_state(self):
        self.assertIsNone(request_state(finalized(4, 4), plan_id="pln_other"))

    def test_the_expected_id_is_required(self):
        with self.assertRaises(TypeError):
            request_state(finalized(4, 4))


if __name__ == "__main__":
    unittest.main()
