"""Unit tests for plan generations and the shared finalization rules.

Pure logic only: document construction, the rule for which attempts finish
their request, finalization request validation, the order of checks against a
plan document, and the bounds of the shared update drivers (exercised with
in-memory fetch/replace functions). Backend behavior is covered by
``tests/test_plan_store_conditional_updates.py``.
"""

from __future__ import annotations

import copy
import itertools
import logging
import unittest
from typing import Any

from lib.storage.errors import PlanStoreError, RevisionConflictError
from lib.storage.plan_documents import (
    build_plan_document,
    new_plan_generation,
    plan_generation_of,
    plan_summary_from_document,
)
from lib.storage.plan_updates import (
    ExecutionFinalization,
    FinalizationResult,
    FinalizationStatus,
    SupersessionReason,
    check_finalization,
    finalize_execution,
    finishes_request,
    initialize_plan_generation,
)
from tests.plan_store_support import (
    CANCELLED,
    DRAINED_FAILURE,
    FAILED_FAST,
    ORCHESTRATION_ERROR,
    PREFLIGHT_REJECTED,
    SCOPE,
    SUCCEEDED,
    continuation_plan,
    finalization_for,
    finished_attempt,
    make_plan,
)
from yggdrasil.flow.model import CONTINUE_INDEPENDENT_POLICY, FAIL_FAST_POLICY
from yggdrasil.flow.outcomes import (
    AttemptDiagnostic,
    ExecutionOutcome,
    TerminationReason,
)

LOGGER = logging.getLogger("tests.plan_updates")


def _document(**overrides: Any) -> dict[str, Any]:
    """Build a stored-looking plan document with a generation and a revision."""
    doc = build_plan_document(
        make_plan(), "test_realm", dict(SCOPE), auto_run=True, plan_generation="g1"
    )
    doc["_rev"] = "1"
    doc.update(overrides)
    return doc


class TestPlanGeneration(unittest.TestCase):
    """Generation IDs on plan documents."""

    def test_new_generations_are_unique_opaque_strings(self):
        generations = {new_plan_generation() for _ in range(200)}
        self.assertEqual(len(generations), 200)
        self.assertTrue(all(isinstance(g, str) and g for g in generations))

    def test_only_a_non_empty_string_counts_as_a_generation(self):
        self.assertEqual(plan_generation_of({"plan_generation": "g1"}), "g1")
        for value in (None, "", 0, 7, ["g1"]):
            with self.subTest(value=value):
                self.assertIsNone(plan_generation_of({"plan_generation": value}))
        self.assertIsNone(plan_generation_of({}))

    def test_every_built_document_gets_a_fresh_generation(self):
        first = build_plan_document(make_plan(), "test_realm", dict(SCOPE))
        second = build_plan_document(make_plan(), "test_realm", dict(SCOPE))
        self.assertNotEqual(first["plan_generation"], second["plan_generation"])

    def test_explicit_generation_is_used(self):
        doc = build_plan_document(
            make_plan(), "test_realm", dict(SCOPE), plan_generation="fixed"
        )
        self.assertEqual(doc["plan_generation"], "fixed")

    def test_regeneration_drops_the_finalized_result_with_the_tokens(self):
        existing = _document(
            run_token=4,
            executed_run_token=4,
            created_at="2026-01-01T00:00:00+00:00",
            last_finalized_execution={"execution_id": "e", "outcome": "failed"},
        )

        regenerated = build_plan_document(
            make_plan(), "test_realm", dict(SCOPE), existing=existing
        )

        self.assertNotEqual(regenerated["plan_generation"], "g1")
        self.assertEqual(
            (regenerated["run_token"], regenerated["executed_run_token"]), (0, -1)
        )
        self.assertNotIn("last_finalized_execution", regenerated)
        self.assertEqual(regenerated["created_at"], "2026-01-01T00:00:00+00:00")

    def test_summary_reports_generation_and_last_outcome_separately(self):
        doc = _document(last_finalized_execution={"outcome": "failed"})
        summary = plan_summary_from_document(doc)
        self.assertEqual(summary["status"], "approved")
        self.assertEqual(summary["plan_generation"], "g1")
        self.assertEqual(summary["last_finalized_outcome"], "failed")

        legacy = plan_summary_from_document({"_id": "p"})
        self.assertIsNone(legacy["plan_generation"])
        self.assertIsNone(legacy["last_finalized_outcome"])


class TestFinishesRequest(unittest.TestCase):
    """Which attempt endings consume their execution request."""

    def test_rule_over_every_outcome_reason_and_policy(self):
        finishing = {
            (ExecutionOutcome.SUCCEEDED, TerminationReason.COMPLETED, FAIL_FAST_POLICY),
            (
                ExecutionOutcome.SUCCEEDED,
                TerminationReason.COMPLETED,
                CONTINUE_INDEPENDENT_POLICY,
            ),
            (
                ExecutionOutcome.FAILED,
                TerminationReason.COMPLETED,
                CONTINUE_INDEPENDENT_POLICY,
            ),
            (
                ExecutionOutcome.FAILED,
                TerminationReason.PREFLIGHT_REJECTED,
                CONTINUE_INDEPENDENT_POLICY,
            ),
        }
        for combination in itertools.product(
            ExecutionOutcome,
            TerminationReason,
            (FAIL_FAST_POLICY, CONTINUE_INDEPENDENT_POLICY),
        ):
            with self.subTest(combination=combination):
                self.assertEqual(
                    finishes_request(*combination), combination in finishing
                )


class TestExecutionFinalization(unittest.TestCase):
    """Building and validating a finalization request."""

    def test_success_captures_the_attempts_identity(self):
        context = finished_attempt(
            make_plan(),
            plan_generation="g1",
            run_token=3,
            execution_id="exec-9",
            execution_authority="run_once",
            execution_owner="run_once:abc",
        )

        request = ExecutionFinalization.from_attempt(context)

        self.assertEqual(
            request.identity(),
            {
                "execution_id": "exec-9",
                "plan_id": "pln_test_P1_v1",
                "plan_generation": "g1",
                "run_token": 3,
                "failure_policy": FAIL_FAST_POLICY,
                "outcome": "succeeded",
                "termination_reason": "completed",
            },
        )
        self.assertEqual(
            (request.execution_authority, request.execution_owner),
            ("run_once", "run_once:abc"),
        )
        self.assertEqual(request.report, context.report.to_dict())

    def test_finished_requests_can_be_finalized(self):
        cases = {
            "fail_fast success": (make_plan(), SUCCEEDED),
            "continuation success": (continuation_plan(), SUCCEEDED),
            "drained continuation failure": (continuation_plan(), DRAINED_FAILURE),
            "continuation preflight rejection": (
                continuation_plan(),
                PREFLIGHT_REJECTED,
            ),
        }
        for label, (plan, ending) in cases.items():
            with self.subTest(label):
                context = finished_attempt(
                    plan, plan_generation="g1", run_token=0, ending=ending
                )
                request = ExecutionFinalization.from_attempt(context)
                self.assertEqual(request.outcome, context.report.outcome)

    def test_unfinished_requests_are_refused(self):
        cases = {
            "fail_fast failure": (make_plan(), FAILED_FAST),
            "fail_fast preflight rejection": (make_plan(), PREFLIGHT_REJECTED),
            "cancelled continuation": (continuation_plan(), CANCELLED),
            "continuation orchestration error": (
                continuation_plan(),
                ORCHESTRATION_ERROR,
            ),
        }
        for label, (plan, ending) in cases.items():
            with self.subTest(label):
                context = finished_attempt(
                    plan, plan_generation="g1", run_token=0, ending=ending
                )
                with self.assertRaisesRegex(ValueError, "did not finish its request"):
                    ExecutionFinalization.from_attempt(context)

    def test_unpublished_report_is_not_a_finished_request(self):
        context = finished_attempt(
            continuation_plan(),
            plan_generation="g1",
            run_token=0,
            ending=DRAINED_FAILURE,
        )
        context.report.record_publication_failure(
            AttemptDiagnostic(message="spool unwritable")
        )
        with self.assertRaisesRegex(ValueError, "did not finish its request"):
            ExecutionFinalization.from_attempt(context)

    def test_running_attempt_is_refused(self):
        context = finished_attempt(make_plan(), plan_generation="g1", run_token=0)
        context.report.termination_reason = None
        with self.assertRaisesRegex(ValueError, "has not finished"):
            ExecutionFinalization.from_attempt(context)

    def test_attempt_without_captured_generation_or_token_is_refused(self):
        for generation, token in (("g1", None), (None, 0)):
            with self.subTest(generation=generation, token=token):
                context = finished_attempt(
                    make_plan(), plan_generation=generation, run_token=token
                )
                with self.assertRaisesRegex(ValueError, "captured no plan_generation"):
                    ExecutionFinalization.from_attempt(context)

    def _fields(self) -> dict[str, Any]:
        request = ExecutionFinalization.from_attempt(
            finished_attempt(make_plan(), plan_generation="g1", run_token=2)
        )
        return {
            "plan_id": request.plan_id,
            "plan_generation": request.plan_generation,
            "run_token": request.run_token,
            "execution_id": request.execution_id,
            "execution_authority": request.execution_authority,
            "execution_owner": request.execution_owner,
            "failure_policy": request.failure_policy,
            "outcome": request.outcome,
            "termination_reason": request.termination_reason,
            "report": copy.deepcopy(request.report),
        }

    def test_request_that_contradicts_its_report_is_refused(self):
        for field, value in (
            ("execution_id", "someone-else"),
            ("run_token", 5),
            ("plan_generation", "g2"),
        ):
            with self.subTest(field=field):
                fields = self._fields()
                fields[field] = value
                with self.assertRaisesRegex(ValueError, f"disagrees .*'{field}'"):
                    ExecutionFinalization(**fields)

    def test_invalid_identity_is_refused(self):
        for field, value, message in (
            ("plan_generation", "", "no captured plan_generation"),
            ("run_token", -1, "invalid run_token"),
            ("execution_authority", "cron", "Invalid execution_authority"),
        ):
            with self.subTest(field=field):
                fields = self._fields()
                fields[field] = value
                with self.assertRaisesRegex(ValueError, message):
                    ExecutionFinalization(**fields)

    def test_execution_record_is_identity_time_and_report(self):
        request = ExecutionFinalization(**self._fields())
        record = request.execution_record(finalized_at="2026-09-17T12:00:00+00:00")
        self.assertEqual(
            record,
            {
                **request.identity(),
                "finalized_at": "2026-09-17T12:00:00+00:00",
                "report": request.report,
            },
        )


class TestFinalizationResult(unittest.TestCase):
    """Result invariants."""

    def test_reason_is_required_exactly_for_supersession(self):
        with self.assertRaises(ValueError):
            FinalizationResult(status=FinalizationStatus.SUPERSEDED, message="x")
        for status in (
            FinalizationStatus.COMMITTED,
            FinalizationStatus.ALREADY_COMMITTED,
            FinalizationStatus.CONFLICT,
        ):
            with self.subTest(status=status), self.assertRaises(ValueError):
                FinalizationResult(
                    status=status,
                    message="x",
                    reason=SupersessionReason.PLAN_MISSING,
                )

    def test_only_committed_statuses_count_as_recorded(self):
        recorded = {
            status: FinalizationResult(
                status=status,
                message="x",
                reason=(
                    SupersessionReason.PLAN_MISSING
                    if status is FinalizationStatus.SUPERSEDED
                    else None
                ),
            ).recorded
            for status in FinalizationStatus
        }
        self.assertEqual(
            recorded,
            {
                FinalizationStatus.COMMITTED: True,
                FinalizationStatus.ALREADY_COMMITTED: True,
                FinalizationStatus.CONFLICT: False,
                FinalizationStatus.SUPERSEDED: False,
            },
        )


class TestCheckFinalization(unittest.TestCase):
    """The order in which a request is judged against a document."""

    def setUp(self):
        self.doc = _document()
        self.request = finalization_for(self.doc)

    def committed(self, request: ExecutionFinalization, **overrides: Any):
        """The document as it looks once request is recorded."""
        doc = _document(
            executed_run_token=request.run_token,
            last_finalized_execution=request.execution_record(finalized_at="t"),
        )
        doc.update(overrides)
        return doc

    def test_valid_unrecorded_request_should_be_written(self):
        self.assertIsNone(check_finalization(self.doc, self.request))

    def test_recorded_completion_stays_committed_after_an_authority_change(self):
        doc = self.committed(self.request, execution_authority="run_once")
        result = check_finalization(doc, self.request)
        self.assertEqual(result.status, FinalizationStatus.ALREADY_COMMITTED)

    def test_same_execution_with_a_different_recorded_outcome_is_not_mine(self):
        doc = self.committed(self.request)
        doc["last_finalized_execution"]["outcome"] = "failed"
        result = check_finalization(doc, self.request)
        self.assertEqual(result.reason, SupersessionReason.REQUEST_ALREADY_FINALIZED)

    def test_matching_record_under_a_moved_token_is_not_mine(self):
        doc = self.committed(self.request, executed_run_token=1, run_token=1)
        result = check_finalization(doc, self.request)
        self.assertEqual(result.reason, SupersessionReason.REQUEST_ALREADY_FINALIZED)

    def test_generation_is_judged_before_authority(self):
        doc = _document(plan_generation="g2", execution_authority="run_once")
        result = check_finalization(doc, self.request)
        self.assertEqual(result.reason, SupersessionReason.GENERATION_CHANGED)

    def test_unreadable_executed_token_is_refused(self):
        with self.assertRaisesRegex(ValueError, "unreadable executed_run_token"):
            check_finalization(_document(executed_run_token="soon"), self.request)


class _InMemoryPlans:
    """fetch/replace primitives over one in-memory document, with call counts."""

    def __init__(self, doc: dict[str, Any] | None) -> None:
        self.doc = copy.deepcopy(doc)
        self.fetches = 0
        self.replaces = 0
        self.before_replace: list[Any] = []

    def fetch(self, doc_id: str) -> dict[str, Any] | None:
        self.fetches += 1
        return copy.deepcopy(self.doc)

    def replace(self, doc_id: str, body: dict[str, Any], expected_rev: str) -> str:
        self.replaces += 1
        for action in self.before_replace:
            action(self)
        if self.doc is None or self.doc["_rev"] != expected_rev:
            raise RevisionConflictError(
                "moved", doc_id=doc_id, expected_rev=expected_rev
            )
        new_rev = str(int(expected_rev) + 1)
        self.doc = {**copy.deepcopy(body), "_rev": new_rev}
        return new_rev


class TestFinalizeExecutionDriver(unittest.TestCase):
    """One attempt per call; the caller owns retries."""

    def setUp(self):
        self.doc = _document()
        self.request = finalization_for(self.doc)

    def test_conflict_is_reported_without_retrying(self):
        plans = _InMemoryPlans(self.doc)

        def concurrent_rerun_request(p: _InMemoryPlans) -> None:
            assert p.doc is not None
            p.doc = {**p.doc, "run_token": 1, "_rev": "2"}

        plans.before_replace.append(concurrent_rerun_request)

        result = finalize_execution(
            self.request, fetch=plans.fetch, replace=plans.replace, logger=LOGGER
        )

        self.assertEqual(result.status, FinalizationStatus.CONFLICT)
        self.assertEqual((plans.fetches, plans.replaces), (2, 1))

    def test_supersession_is_decided_without_writing(self):
        plans = _InMemoryPlans(_document(plan_generation="g2"))
        result = finalize_execution(
            self.request, fetch=plans.fetch, replace=plans.replace, logger=LOGGER
        )
        self.assertEqual(result.reason, SupersessionReason.GENERATION_CHANGED)
        self.assertEqual(plans.replaces, 0)

    def test_commit_uses_one_timestamp_for_every_completion_field(self):
        plans = _InMemoryPlans(self.doc)
        result = finalize_execution(
            self.request,
            fetch=plans.fetch,
            replace=plans.replace,
            logger=LOGGER,
            now="2026-09-17T12:00:00+00:00",
        )
        self.assertEqual(result.status, FinalizationStatus.COMMITTED)
        self.assertEqual(
            (
                plans.doc["updated_at"],
                plans.doc["last_executed_at"],
                plans.doc["last_finalized_execution"]["finalized_at"],
            ),
            ("2026-09-17T12:00:00+00:00",) * 3,
        )
        self.assertEqual(plans.doc["run_token"], self.doc["run_token"])

    def test_storage_failure_propagates(self):
        plans = _InMemoryPlans(self.doc)

        def fail(p: _InMemoryPlans) -> None:
            raise PlanStoreError("backend down")

        plans.before_replace.append(fail)
        with self.assertRaises(PlanStoreError):
            finalize_execution(
                self.request, fetch=plans.fetch, replace=plans.replace, logger=LOGGER
            )


class TestInitializePlanGenerationDriver(unittest.TestCase):
    """Bounded rereads when assigning a legacy document's first generation."""

    def legacy(self, **overrides: Any) -> dict[str, Any]:
        """A document written before generations existed."""
        doc = _document()
        del doc["plan_generation"]
        doc.update(overrides)
        return doc

    def test_attempts_must_be_positive(self):
        plans = _InMemoryPlans(self.legacy())
        with self.assertRaises(ValueError):
            initialize_plan_generation(
                "p", fetch=plans.fetch, replace=plans.replace, logger=LOGGER, attempts=0
            )

    def test_gives_up_after_the_attempt_bound(self):
        plans = _InMemoryPlans(self.legacy())

        def unrelated_write(p: _InMemoryPlans) -> None:
            assert p.doc is not None
            p.doc = {**p.doc, "_rev": str(int(p.doc["_rev"]) + 1)}

        plans.before_replace.append(unrelated_write)

        with self.assertRaises(RevisionConflictError):
            initialize_plan_generation(
                "p", fetch=plans.fetch, replace=plans.replace, logger=LOGGER, attempts=4
            )
        self.assertEqual((plans.fetches, plans.replaces), (4, 4))
        self.assertNotIn("plan_generation", plans.doc)

    def test_unusable_generation_is_replaced_with_a_warning(self):
        plans = _InMemoryPlans(self.legacy(plan_generation=""))
        with self.assertLogs(LOGGER, level="WARNING") as logs:
            doc = initialize_plan_generation(
                "p", fetch=plans.fetch, replace=plans.replace, logger=LOGGER
            )
        self.assertTrue(doc["plan_generation"])
        self.assertIn("unusable plan_generation", "\n".join(logs.output))


if __name__ == "__main__":
    unittest.main()
