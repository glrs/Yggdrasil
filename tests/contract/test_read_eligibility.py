"""Eligibility: one document-local decision, in one order, with closed codes.

Plan documents are corpus documents with the fields each case is about
changed. Where a check exists today, its reason text is unchanged; the
decision is compared with Yggdrasil's existing eligibility functions on every
supported-family case.
"""

from __future__ import annotations

import unittest
from typing import Any

from tests.contract.read_support import (
    HOSTILE,
    ReaderTestCase,
    plan_document,
    plan_document_names,
)
from tests.contract.support import ABSENT, changed
from yggdrasil.contract.read import (
    ALREADY_EXECUTED,
    ELIGIBILITY_CODES,
    ELIGIBLE,
    IDENTITY_MISMATCH,
    INVALID_DOCUMENT,
    INVALID_TOKENS,
    NOT_APPROVED,
    STATUS_MISSING,
    UNSUPPORTED_DOCUMENT,
    Eligibility,
    eligibility,
    eligibility_reason,
    is_eligible,
    read_plan,
)
from yggdrasil.storage.plan_eligibility import get_eligibility_reason, is_plan_eligible


def approved(**fields: Any) -> dict[str, Any]:
    """Return the approved corpus plan with top-level fields set; ABSENT removes one."""
    document = plan_document("approved")
    for field, value in fields.items():
        document = changed(document, field, value)
    return document


# Supported-family documents, one per answer of the checks after the version.
SUPPORTED_CASES: tuple[tuple[str, dict[str, Any], str], ...] = (
    ("approved, never run", approved(), ELIGIBLE),
    ("a rerun requested", approved(run_token=4, executed_run_token=3), ELIGIBLE),
    ("already executed", approved(executed_run_token=0), ALREADY_EXECUTED),
    ("draft", approved(status="draft"), NOT_APPROVED),
    ("pending", approved(status="pending"), NOT_APPROVED),
    ("other text", approved(status="APPROVED"), NOT_APPROVED),
    ("empty status", approved(status=""), NOT_APPROVED),
    ("null status", approved(status=None), NOT_APPROVED),
    ("status a bool", approved(status=True), NOT_APPROVED),
    ("status a list", approved(status=["approved"]), NOT_APPROVED),
    ("status absent", approved(status=ABSENT), STATUS_MISSING),
    ("run token absent", approved(run_token=ABSENT), INVALID_TOKENS),
    ("run token text", approved(run_token="5"), INVALID_TOKENS),
    ("run token a bool", approved(run_token=True), INVALID_TOKENS),
    ("executed above run", approved(run_token=3, executed_run_token=4), INVALID_TOKENS),
    ("executed token null", approved(executed_run_token=None), INVALID_TOKENS),
)


class TestOrder(ReaderTestCase):
    """A document failing several checks gets the first one's code."""

    def test_the_first_failing_check_names_the_refusal(self):
        cases = (
            ("not an object", ["approved"], INVALID_DOCUMENT),
            (
                "unsupported, with bad tokens and no status",
                approved(schema_version=2, run_token="5", status=ABSENT),
                UNSUPPORTED_DOCUMENT,
            ),
            (
                "bad tokens and no status",
                approved(run_token="5", status=ABSENT),
                INVALID_TOKENS,
            ),
            (
                "executed above run, as a draft",
                approved(run_token=3, executed_run_token=4, status="draft"),
                INVALID_TOKENS,
            ),
            (
                "no status, already executed",
                approved(status=ABSENT, executed_run_token=0),
                STATUS_MISSING,
            ),
            (
                "a draft, already executed",
                approved(status="draft", executed_run_token=0),
                NOT_APPROVED,
            ),
        )
        for name, document, code in cases:
            with self.subTest(name):
                self.assertEqual(eligibility(document).code, code)

    def test_every_code_is_closed(self):
        self.assertEqual(
            ELIGIBILITY_CODES,
            {
                INVALID_DOCUMENT,
                UNSUPPORTED_DOCUMENT,
                INVALID_TOKENS,
                STATUS_MISSING,
                NOT_APPROVED,
                ALREADY_EXECUTED,
                ELIGIBLE,
                IDENTITY_MISMATCH,
            },
        )
        for _, document, code in SUPPORTED_CASES:
            self.assertIn(code, ELIGIBILITY_CODES)


class TestEachCode(ReaderTestCase):
    """What each code says, and the problems behind each refusal."""

    def test_each_supported_case(self):
        for name, document, code in SUPPORTED_CASES:
            with self.subTest(name):
                decision = eligibility(document)

                self.assertEqual(decision.code, code)
                self.assertIs(decision.eligible, code == ELIGIBLE)

    def test_the_reason_texts(self):
        cases = (
            (
                approved(),
                "Eligible: status='approved', run_token=0 > executed_run_token=-1",
            ),
            (
                approved(executed_run_token=0),
                "Not eligible: run_token=0 was already executed (executed_run_token=0)",
            ),
            (
                approved(status="draft"),
                "Not eligible: status 'draft' (must be 'approved')",
            ),
            (approved(status=None), "Not eligible: status None (must be 'approved')"),
            (
                approved(status=ABSENT),
                "Not eligible: status is missing (must be 'approved')",
            ),
            (
                approved(run_token="5"),
                "Not eligible: invalid tokens (run_token '5' is a str, not a JSON "
                "integer)",
            ),
            (
                approved(schema_version="1"),
                "Not eligible: its schema_version is '1', so it is invalid "
                "(supported plan-document families: [1])",
            ),
            (
                [],
                "Not eligible: the plan document [] is a list, not an object",
            ),
        )
        for document, reason in cases:
            with self.subTest(reason):
                self.assertEqual(eligibility(document).reason, reason)

    def test_the_problems_behind_each_refusal(self):
        cases = (
            (approved(), []),
            (approved(executed_run_token=0), []),
            (approved(status="pending"), []),
            (approved(status=5), [("status", "wrong_type")]),
            (approved(status=ABSENT), [("status", "missing_field")]),
            (
                approved(run_token=ABSENT, executed_run_token="x"),
                [
                    ("run_token", "invalid_tokens"),
                    ("executed_run_token", "invalid_tokens"),
                ],
            ),
            (approved(schema_version=ABSENT), [("schema_version", "unversioned")]),
            (approved(schema_version=2), [("schema_version", "unsupported_version")]),
            (approved(schema_version=True), [("schema_version", "invalid_version")]),
            ("a plan", [("", "wrong_type")]),
        )
        for document, expected in cases:
            with self.subTest(expected=expected):
                decision = eligibility(document)
                self.assertEqual(
                    [(problem.path, problem.code) for problem in decision.problems],
                    expected,
                )

    def test_the_token_problems_are_the_token_check_details(self):
        document = approved(run_token=3, executed_run_token=4)

        decision = eligibility(document)

        plan = read_plan(document, plan_id="pln_lifecycle")
        assert plan.tokens is not None
        self.assertEqual(decision.problems, plan.tokens.details)

    def test_identity_is_judged_only_by_read_plan(self):
        document = approved(_id="pln_other")

        self.assertEqual(eligibility(document).code, ELIGIBLE)
        plan = read_plan(document, plan_id="pln_lifecycle")
        assert plan.eligibility is not None
        self.assertEqual(plan.eligibility.code, IDENTITY_MISMATCH)
        self.assertFalse(plan.eligibility.eligible)

    def test_authority_is_not_judged(self):
        for authority, owner in (("run_once", "run_once:x"), ("cron", None)):
            with self.subTest(authority):
                document = approved(
                    execution_authority=authority, execution_owner=owner
                )
                self.assertEqual(eligibility(document).code, ELIGIBLE)

    def test_nothing_else_in_the_document_is_judged(self):
        document = approved(created_at="yesterday", plan_generation=ABSENT, realm=5)
        document = changed(document, "plan.failure_policy", "retry_forever")

        self.assertEqual(eligibility(document).code, ELIGIBLE)


class TestDerivedAnswers(ReaderTestCase):
    """``is_eligible``, ``eligibility_reason`` and ``read_plan`` follow one decision."""

    def all_documents(self) -> list[tuple[str, Any]]:
        """Return every case, corpus document and hostile body, by name."""
        documents: list[tuple[str, Any]] = [
            (name, document) for name, document, _ in SUPPORTED_CASES
        ]
        documents += [(name, plan_document(name)) for name in plan_document_names()]
        documents += [(f"body: {name}", value) for name, value in HOSTILE]
        return documents

    def test_is_eligible_and_the_reason_agree_with_the_decision(self):
        with self.assert_silent():
            for name, document in self.all_documents():
                with self.subTest(name):
                    decision = eligibility(document)
                    self.assertIsInstance(decision, Eligibility)
                    self.assertIs(is_eligible(document), decision.eligible)
                    self.assertEqual(eligibility_reason(document), decision.reason)

    def test_read_plan_carries_the_same_decision(self):
        for name in plan_document_names():
            with self.subTest(name):
                document = plan_document(name)
                plan = read_plan(document, plan_id=document["_id"])
                self.assertEqual(plan.eligibility, eligibility(document))

    def test_the_decision_on_a_supported_document_is_today_s(self):
        documents = [(name, document) for name, document, _ in SUPPORTED_CASES]
        documents += [
            (name, plan_document(name))
            for name in plan_document_names()
            if name not in ("pre_contract", "schema_version_2", "schema_version_string")
        ]
        for name, document in documents:
            with self.subTest(name):
                self.assertIs(is_eligible(document), is_plan_eligible(document))
                self.assertEqual(
                    eligibility_reason(document), get_eligibility_reason(document)
                )

    def test_an_unsupported_document_is_not_eligible(self):
        """Every other check passes: the family alone refuses it."""
        for name, document in (
            ("unversioned", approved(schema_version=ABSENT)),
            ("family 2", approved(schema_version=2)),
            ("pre-contract corpus document", plan_document("pre_contract")),
        ):
            with self.subTest(name):
                self.assertFalse(is_eligible(document))
                self.assertEqual(eligibility(document).code, UNSUPPORTED_DOCUMENT)
                supported = changed(document, "schema_version", 1)
                self.assertEqual(eligibility(supported).code, ELIGIBLE)


if __name__ == "__main__":
    unittest.main()
