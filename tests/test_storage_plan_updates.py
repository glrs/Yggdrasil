"""Unit tests for plan generations and the shared finalization rules.

Pure logic only: document construction, the rule for which attempts finish
their request, finalization request validation, the order of checks against a
plan document, the shared finalization driver (exercised with in-memory
fetch/replace functions), and what a read must return to count as a stored
document. Backend behavior is covered by
``tests/test_plan_store_conditional_updates.py``.
"""

from __future__ import annotations

import copy
import dataclasses
import itertools
import logging
import unittest
from typing import Any

from tests.contract.support import ConformingBuilders
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
from yggdrasil.contract import PLAN_DOCUMENT, read
from yggdrasil.contract.controls import MAX_TOKEN, token_pair
from yggdrasil.contract.diagnostics import describe_recorded
from yggdrasil.contract.read import generation_of
from yggdrasil.contract.timestamps import is_canonical
from yggdrasil.contract.versions import SUPPORTED, version_of
from yggdrasil.flow.model import CONTINUE_INDEPENDENT_POLICY, FAIL_FAST_POLICY
from yggdrasil.flow.outcomes import (
    AttemptDiagnostic,
    ExecutionOutcome,
    TerminationReason,
)
from yggdrasil.storage.errors import PlanStoreError, RevisionConflictError
from yggdrasil.storage.plan_documents import (
    build_plan_document,
    new_plan_generation,
    plan_summary_from_document,
)
from yggdrasil.storage.plan_updates import (
    ExecutionFinalization,
    FinalizationResult,
    FinalizationStatus,
    SupersessionReason,
    check_finalization,
    finalize_execution,
    finishes_request,
    require_storage_envelope,
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


# Stands for a field removed from a document.
ABSENT = object()


def _changed(doc: dict[str, Any], **fields: Any) -> dict[str, Any]:
    """A copy of doc with fields replaced; ABSENT removes one."""
    changed = copy.deepcopy(doc)
    for key, value in fields.items():
        if value is ABSENT:
            del changed[key]
        else:
            changed[key] = value
    return changed


class TestPlanGeneration(ConformingBuilders):
    """Generation IDs on plan documents."""

    def test_new_generations_are_unique_opaque_strings(self):
        generations = {new_plan_generation() for _ in range(200)}
        self.assertEqual(len(generations), 200)
        self.assertTrue(all(isinstance(g, str) and g for g in generations))

    def test_only_a_non_empty_string_counts_as_a_generation(self):
        self.assertEqual(generation_of({"plan_generation": "g1"}), "g1")
        for value in (None, "", 0, 7, ["g1"]):
            with self.subTest(value=value):
                self.assertIsNone(generation_of({"plan_generation": value}))
        self.assertIsNone(generation_of({}))

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
        finalized = _document(
            run_token=4,
            executed_run_token=4,
            last_finalized_execution={"execution_id": "e", "outcome": "failed"},
        )
        unversioned = {k: v for k, v in finalized.items() if k != "schema_version"}
        cases = {
            "family 1 keeps its canonical created_at": dict(
                finalized, created_at="2026-01-01T00:00:00Z"
            ),
            "unversioned has its legacy created_at re-spelled": dict(
                unversioned, created_at="2026-01-01T00:00:00+00:00"
            ),
        }
        for case, existing in cases.items():
            with self.subTest(case=case):
                regenerated = build_plan_document(
                    make_plan(), "test_realm", dict(SCOPE), existing=existing
                )

                self.assertNotEqual(regenerated["plan_generation"], "g1")
                self.assertEqual(
                    (regenerated["run_token"], regenerated["executed_run_token"]),
                    (0, -1),
                )
                self.assertNotIn("last_finalized_execution", regenerated)
                self.assertEqual(regenerated["created_at"], "2026-01-01T00:00:00Z")
                self.assertEqual(regenerated["schema_version"], 1)

    def test_summary_reports_generation_and_last_outcome_separately(self):
        doc = _document(last_finalized_execution={"outcome": "failed"})
        summary = plan_summary_from_document(doc)
        self.assertEqual(summary["status"], "approved")
        self.assertEqual(summary["plan_generation"], "g1")
        self.assertEqual(summary["last_finalized_outcome"], "failed")

        legacy = plan_summary_from_document({"_id": "p"})
        self.assertIsNone(legacy["plan_generation"])
        self.assertIsNone(legacy["last_finalized_outcome"])


class TestBuiltDocumentContract(ConformingBuilders):
    """Every built document names its family and spells timestamps canonically."""

    def test_new_and_regenerated_documents_are_family_one(self):
        unversioned = {k: v for k, v in _document().items() if k != "schema_version"}
        cases = {
            "new": build_plan_document(make_plan(), "test_realm", dict(SCOPE)),
            "over an unversioned document": build_plan_document(
                make_plan(), "test_realm", dict(SCOPE), existing=unversioned
            ),
            "over a family-1 document": build_plan_document(
                make_plan(), "test_realm", dict(SCOPE), existing=_document()
            ),
        }
        for case, doc in cases.items():
            with self.subTest(case=case):
                self.assertIs(type(doc["schema_version"]), int)
                self.assertEqual(doc["schema_version"], 1)
                self.assertEqual(version_of(PLAN_DOCUMENT, doc).state, SUPPORTED)

    def test_built_timestamps_are_canonical(self):
        doc = build_plan_document(make_plan(), "test_realm", dict(SCOPE))
        self.assertTrue(is_canonical(doc["created_at"]), doc["created_at"])
        self.assertEqual(doc["updated_at"], doc["created_at"])


class TestFinishesRequest(ConformingBuilders):
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

    def test_the_adapter_answers_as_the_shared_rule_on_every_enum_input(self):
        """The enum signature only converts: the shared rule gives the answer."""
        policies = (FAIL_FAST_POLICY, CONTINUE_INDEPENDENT_POLICY, "retry_forever", "")
        for outcome, reason, policy in itertools.product(
            ExecutionOutcome, TerminationReason, policies
        ):
            with self.subTest(
                outcome=outcome.value, reason=reason.value, policy=policy
            ):
                self.assertIs(
                    finishes_request(outcome, reason, policy),
                    read.finishes_request(outcome.value, reason.value, policy),
                )


class TestExecutionFinalization(ConformingBuilders):
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

        request = ExecutionFinalization.from_attempt(context, schema_version=1)

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
                request = ExecutionFinalization.from_attempt(context, schema_version=1)
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
                    ExecutionFinalization.from_attempt(context, schema_version=1)

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
            ExecutionFinalization.from_attempt(context, schema_version=1)

    def test_running_attempt_is_refused(self):
        context = finished_attempt(make_plan(), plan_generation="g1", run_token=0)
        context.report.termination_reason = None
        with self.assertRaisesRegex(ValueError, "has not finished"):
            ExecutionFinalization.from_attempt(context, schema_version=1)

    def test_attempt_without_captured_generation_or_token_is_refused(self):
        for generation, token in (("g1", None), (None, 0)):
            with self.subTest(generation=generation, token=token):
                context = finished_attempt(
                    make_plan(), plan_generation=generation, run_token=token
                )
                with self.assertRaisesRegex(ValueError, "captured no plan_generation"):
                    ExecutionFinalization.from_attempt(context, schema_version=1)

    def _fields(self) -> dict[str, Any]:
        request = ExecutionFinalization.from_attempt(
            finished_attempt(make_plan(), plan_generation="g1", run_token=2),
            schema_version=1,
        )
        return {
            "plan_id": request.plan_id,
            "schema_version": request.schema_version,
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

    def test_the_captured_family_is_a_positive_integer(self):
        for value in (0, -1, True, False, 1.0, "1", None):
            with self.subTest(value=value):
                fields = self._fields()
                fields["schema_version"] = value
                with self.assertRaisesRegex(
                    ValueError, "invalid captured schema_version"
                ):
                    ExecutionFinalization(**fields)

    def test_the_captured_family_is_required(self):
        fields = self._fields()
        del fields["schema_version"]
        with self.assertRaises(TypeError):
            ExecutionFinalization(**fields)

        context = finished_attempt(make_plan(), plan_generation="g1", run_token=0)
        with self.assertRaises(TypeError):
            ExecutionFinalization.from_attempt(context)  # type: ignore[call-arg]

    def test_the_family_is_neither_identity_nor_record(self):
        # The persisted record keeps the shape it had before families existed.
        fields = self._fields()
        first = ExecutionFinalization(**{**fields, "schema_version": 1})
        other = ExecutionFinalization(**{**fields, "schema_version": 2})

        self.assertNotIn("schema_version", first.identity())
        self.assertEqual(first.identity(), other.identity())
        self.assertEqual(
            first.execution_record(finalized_at="t"),
            other.execution_record(finalized_at="t"),
        )


class TestFinalizationResult(ConformingBuilders):
    """Result invariants."""

    def test_reason_is_required_exactly_for_supersession(self):
        with self.assertRaises(ValueError):
            FinalizationResult(status=FinalizationStatus.SUPERSEDED, message="x")
        for status in (
            FinalizationStatus.COMMITTED,
            FinalizationStatus.ALREADY_COMMITTED,
            FinalizationStatus.CONFLICT,
            FinalizationStatus.INVALID_CONTROL_STATE,
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
                FinalizationStatus.INVALID_CONTROL_STATE: False,
            },
        )

    def test_invalid_control_state_is_neither_recorded_nor_superseded(self):
        result = FinalizationResult(
            status=FinalizationStatus.INVALID_CONTROL_STATE, message="x"
        )
        self.assertFalse(result.recorded)
        self.assertIsNone(result.reason)


class TestCheckFinalization(ConformingBuilders):
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

    def test_unreadable_executed_token_is_invalid_control_state(self):
        result = check_finalization(_document(executed_run_token="soon"), self.request)
        self.assertEqual(result.status, FinalizationStatus.INVALID_CONTROL_STATE)
        self.assertIn(
            "executed_run_token 'soon' is a str, not a JSON integer", result.message
        )

    def test_every_invalid_stored_pair_is_invalid_control_state(self):
        cases = {
            "run_token missing": {"run_token": ABSENT},
            "executed_run_token missing": {"executed_run_token": ABSENT},
            "run_token null": {"run_token": None},
            "run_token a string": {"run_token": "0"},
            "run_token a float": {"run_token": 0.0},
            "run_token a boolean": {"run_token": False},
            "executed_run_token a boolean": {"executed_run_token": False},
            "executed_run_token below -1": {"executed_run_token": -2},
            "run_token above the maximum": {"run_token": MAX_TOKEN + 1},
            "run_token too long to show": {"run_token": 10**5000},
            "executed above run": {"run_token": 0, "executed_run_token": 1},
        }
        for case, fields in cases.items():
            with self.subTest(case=case):
                result = check_finalization(_changed(self.doc, **fields), self.request)
                self.assertEqual(
                    result.status, FinalizationStatus.INVALID_CONTROL_STATE
                )
                self.assertIsNone(result.reason)
                self.assertFalse(result.recorded)
                for problem in token_pair(_changed(self.doc, **fields)).problems:
                    self.assertIn(problem, result.message)

    def test_hostile_stored_values_never_make_the_judgment_raise(self):
        huge = 10**5000
        cases = {
            "authority": {"execution_authority": huge},
            "owner": {"execution_owner": [huge]},
            "generation": {"plan_generation": huge},
            "recorder": {
                "executed_run_token": 0,
                "last_finalized_execution": {"execution_id": huge},
            },
        }
        for case, fields in cases.items():
            with self.subTest(case=case):
                result = check_finalization(_changed(self.doc, **fields), self.request)
                self.assertEqual(result.status, FinalizationStatus.SUPERSEDED)
                self.assertLess(len(result.message), 600)

    def test_a_hostile_captured_owner_never_makes_the_judgment_raise(self):
        # Admission captures the owner as stored: the daemon executes a plan
        # whoever owns it.
        huge = 10**5000
        long_string = "x" * 100_000
        wide_list = list(range(100_000))
        nested = {"names": [{"name": "x" * 10_000}] * 10}
        # Each owner, with how a diagnostic shows it: by its type when Python
        # cannot render it as text, and bounded otherwise.
        cases = {
            "a huge integer": (huge, "<unrepresentable int>"),
            "a list holding one": ([huge], "<unrepresentable list>"),
            "a long string": (long_string, describe_recorded(long_string)),
            "a wide list": (wide_list, describe_recorded(wide_list)),
            "a nested value": (nested, describe_recorded(nested)),
        }
        reassigned = _changed(
            self.doc, execution_authority="run_once", execution_owner="run_once:other"
        )
        for case, (owner, shown) in cases.items():
            with self.subTest(case=case):
                captured = _changed(self.doc, execution_owner=owner)
                request = finalization_for(captured)

                result = check_finalization(reassigned, request)

                self.assertEqual(result.status, FinalizationStatus.SUPERSEDED)
                self.assertEqual(result.reason, SupersessionReason.AUTHORITY_CHANGED)
                self.assertIn(f"captured as 'daemon'/{shown}", result.message)
                self.assertLess(len(result.message), 600)
                # Owners are compared, not their renderings: against a plan
                # that still carries the captured owner, finalization is allowed.
                self.assertIsNone(check_finalization(captured, request))

    # ----- the pair the write would produce -----

    def admitted_at(self, run_token: int) -> ExecutionFinalization:
        """A request admitted from this plan at run_token."""
        return finalization_for(_document(run_token=run_token))

    def test_a_write_that_would_leave_executed_above_run_is_refused(self):
        request = self.admitted_at(5)
        lowered = _document(run_token=4, executed_run_token=-1)

        result = check_finalization(lowered, request)

        self.assertEqual(result.status, FinalizationStatus.INVALID_CONTROL_STATE)
        self.assertIsNone(result.reason)
        self.assertIn("run_token 4 / executed_run_token -1", result.message)
        self.assertIn("recording run_token 5 as executed", result.message)
        self.assertIn("run_token 4 / executed_run_token 5", result.message)
        self.assertIn("executed_run_token 5 is above run_token 4", result.message)

    def test_a_lowered_run_token_already_executed_is_refused_too(self):
        request = self.admitted_at(5)
        result = check_finalization(
            _document(run_token=4, executed_run_token=4), request
        )
        self.assertEqual(result.status, FinalizationStatus.INVALID_CONTROL_STATE)
        self.assertIn("run_token 4 / executed_run_token 5", result.message)

    def test_the_admitted_pair_is_written(self):
        request = self.admitted_at(5)
        for run_token in (5, 6):
            with self.subTest(run_token=run_token):
                doc = _document(run_token=run_token, executed_run_token=-1)
                self.assertIsNone(check_finalization(doc, request))

    # ----- precedence -----

    def test_a_regeneration_is_judged_before_broken_tokens(self):
        doc = _document(plan_generation="g2", run_token="soon")
        result = check_finalization(doc, self.request)
        self.assertEqual(result.reason, SupersessionReason.GENERATION_CHANGED)

    def test_broken_tokens_are_judged_before_a_changed_authority(self):
        broken = _document(executed_run_token="soon", execution_authority="run_once")
        result = check_finalization(broken, self.request)
        self.assertEqual(result.status, FinalizationStatus.INVALID_CONTROL_STATE)

        repaired = _document(executed_run_token=-1, execution_authority="run_once")
        result = check_finalization(repaired, self.request)
        self.assertEqual(result.reason, SupersessionReason.AUTHORITY_CHANGED)

    def test_broken_tokens_are_judged_before_a_recorded_completion(self):
        doc = self.committed(self.request, run_token="0")
        result = check_finalization(doc, self.request)
        self.assertEqual(result.status, FinalizationStatus.INVALID_CONTROL_STATE)

    # ----- the family -----

    def assert_unsupported(self, doc: dict[str, Any], shown: str) -> None:
        """Assert the request is superseded over doc's family, which shows as shown."""
        result = check_finalization(doc, self.request)

        assert result is not None, "the request would be written"
        self.assertEqual(result.status, FinalizationStatus.SUPERSEDED)
        self.assertEqual(result.reason, SupersessionReason.UNSUPPORTED_DOCUMENT)
        self.assertFalse(result.recorded)
        self.assertIn(f"the plan's schema_version is now {shown}", result.message)
        self.assertIn("captured under family 1", result.message)

    def test_only_the_captured_supported_family_is_written(self):
        cases: dict[str, tuple[Any, str]] = {
            "absent": (ABSENT, "absent (unversioned)"),
            "a string": ("1", "'1' (invalid)"),
            "a float": (1.0, "1.0 (invalid)"),
            "a boolean": (True, "True (invalid)"),
            "null": (None, "None (invalid)"),
            "zero": (0, "0 (invalid)"),
            "negative": (-1, "-1 (invalid)"),
            "a newer family": (2, "2 (unsupported)"),
        }
        for case, (marker, shown) in cases.items():
            with self.subTest(case=case):
                self.assert_unsupported(
                    _changed(self.doc, schema_version=marker), shown
                )
        self.assertIsNone(check_finalization(self.doc, self.request))

    def test_a_supported_family_other_than_the_captured_one_supersedes(self):
        # Only family 1 is supported in this release, so a request captured
        # under another family cannot arise from admission; the judgment
        # covers it anyway.
        captured_under_2 = dataclasses.replace(self.request, schema_version=2)

        result = check_finalization(self.doc, captured_under_2)

        self.assertEqual(result.reason, SupersessionReason.UNSUPPORTED_DOCUMENT)
        self.assertIn("is now 1 (supported)", result.message)
        self.assertIn("captured under family 2", result.message)

    def test_a_recorded_completion_in_another_family_is_not_reported_recorded(self):
        for marker, shown in ((2, "2 (unsupported)"), (ABSENT, "absent (unversioned)")):
            with self.subTest(marker=shown):
                doc = _changed(self.committed(self.request), schema_version=marker)
                self.assert_unsupported(doc, shown)

    def test_the_family_is_judged_before_everything_else(self):
        doc = _changed(
            self.committed(self.request),
            schema_version=2,
            plan_generation="g2",
            run_token="soon",
            execution_authority="run_once",
        )
        self.assert_unsupported(doc, "2 (unsupported)")

    def test_a_hostile_marker_never_makes_the_judgment_raise(self):
        cases = {
            "a huge integer": (10**5000, "<unrepresentable int>"),
            "a list holding one": ([10**5000], "<unrepresentable list>"),
            "a long string": ("x" * 100_000, describe_recorded("x" * 100_000)),
        }
        for case, (marker, shown) in cases.items():
            with self.subTest(case=case):
                doc = _changed(self.doc, schema_version=marker)
                self.assert_unsupported(doc, shown)
                result = check_finalization(doc, self.request)
                self.assertLess(len(result.message), 600)


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


class TestFinalizeExecutionDriver(ConformingBuilders):
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

    def test_default_finalization_time_is_canonical(self):
        plans = _InMemoryPlans(self.doc)
        result = finalize_execution(
            self.request, fetch=plans.fetch, replace=plans.replace, logger=LOGGER
        )
        self.assertEqual(result.status, FinalizationStatus.COMMITTED)
        written = {
            "updated_at": plans.doc["updated_at"],
            "last_executed_at": plans.doc["last_executed_at"],
            "finalized_at": plans.doc["last_finalized_execution"]["finalized_at"],
        }
        for field, value in written.items():
            with self.subTest(field=field):
                self.assertTrue(is_canonical(value), value)

    def test_invalid_stored_tokens_are_refused_without_writing(self):
        stored = _document(executed_run_token="soon")
        plans = _InMemoryPlans(stored)

        with self.assertLogs(LOGGER, level="ERROR") as logs:
            result = finalize_execution(
                self.request, fetch=plans.fetch, replace=plans.replace, logger=LOGGER
            )

        self.assertEqual(result.status, FinalizationStatus.INVALID_CONTROL_STATE)
        self.assertEqual((plans.fetches, plans.replaces), (1, 0))
        self.assertEqual(plans.doc, stored)
        self.assertIn(result.message, "\n".join(logs.output))

    def test_a_write_that_would_leave_executed_above_run_is_not_made(self):
        request = finalization_for(_document(run_token=5))
        lowered = _document(run_token=4)
        plans = _InMemoryPlans(lowered)

        result = finalize_execution(
            request, fetch=plans.fetch, replace=plans.replace, logger=LOGGER
        )

        self.assertEqual(result.status, FinalizationStatus.INVALID_CONTROL_STATE)
        self.assertEqual((plans.fetches, plans.replaces), (1, 0))
        self.assertEqual(plans.doc, lowered)

    def test_tokens_lowered_between_read_and_write_are_refused_on_the_reread(self):
        admitted = _document(run_token=5)
        request = finalization_for(admitted)
        plans = _InMemoryPlans(admitted)

        def lower_the_run_token(p: _InMemoryPlans) -> None:
            assert p.doc is not None
            p.doc = {**p.doc, "run_token": 4, "executed_run_token": -1, "_rev": "2"}

        plans.before_replace.append(lower_the_run_token)

        result = finalize_execution(
            request, fetch=plans.fetch, replace=plans.replace, logger=LOGGER
        )

        self.assertEqual(result.status, FinalizationStatus.INVALID_CONTROL_STATE)
        self.assertIn("run_token 4 / executed_run_token 5", result.message)
        # The one replace was the write the conflict rejected; nothing landed.
        self.assertEqual((plans.fetches, plans.replaces), (2, 1))
        self.assertEqual(plans.doc, {**admitted, "run_token": 4, "_rev": "2"})

    def test_storage_failure_propagates(self):
        plans = _InMemoryPlans(self.doc)

        def fail(p: _InMemoryPlans) -> None:
            raise PlanStoreError("backend down")

        plans.before_replace.append(fail)
        with self.assertRaises(PlanStoreError):
            finalize_execution(
                self.request, fetch=plans.fetch, replace=plans.replace, logger=LOGGER
            )

    def test_an_unsupported_document_is_refused_without_writing(self):
        for marker in (2, ABSENT, "1"):
            with self.subTest(marker="absent" if marker is ABSENT else marker):
                stored = _changed(self.doc, schema_version=marker)
                plans = _InMemoryPlans(stored)

                result = finalize_execution(
                    self.request,
                    fetch=plans.fetch,
                    replace=plans.replace,
                    logger=LOGGER,
                )

                self.assertEqual(result.reason, SupersessionReason.UNSUPPORTED_DOCUMENT)
                self.assertEqual((plans.fetches, plans.replaces), (1, 0))
                self.assertEqual(plans.doc, stored)

    def test_a_marker_changed_before_the_conflict_reread_stops_the_write(self):
        plans = _InMemoryPlans(self.doc)
        newer = {**self.doc, "schema_version": 2, "_rev": "2"}

        def change_the_family(p: _InMemoryPlans) -> None:
            p.doc = copy.deepcopy(newer)

        plans.before_replace.append(change_the_family)

        result = finalize_execution(
            self.request, fetch=plans.fetch, replace=plans.replace, logger=LOGGER
        )

        self.assertEqual(result.status, FinalizationStatus.SUPERSEDED)
        self.assertEqual(result.reason, SupersessionReason.UNSUPPORTED_DOCUMENT)
        self.assertIn("is now 2 (unsupported)", result.message)
        # The one replace was the write the conflict rejected; nothing landed.
        self.assertEqual((plans.fetches, plans.replaces), (2, 1))
        self.assertEqual(plans.doc, newer)


class TestStorageEnvelope(ConformingBuilders):
    """What a read must return to count as a plan's stored document."""

    def test_an_intact_envelope_is_the_document_unchanged(self):
        doc = _document()
        before = copy.deepcopy(doc)

        self.assertIs(require_storage_envelope(doc["_id"], doc), doc)
        self.assertEqual(doc, before)

    def test_anything_else_is_a_storage_failure(self):
        doc = _document()
        plan_id = doc["_id"]
        cases: dict[str, tuple[Any, str]] = {
            "null": (None, "a value of type NoneType instead"),
            "a list": ([doc], "a value of type list instead"),
            "a string": ("<html>proxy error</html>", "a value of type str instead"),
            "a number": (42, "a value of type int instead"),
            "an object-shaped error": ({"error": "proxy failure"}, "without an _id"),
            "no _id": (_changed(doc, _id=ABSENT), "without an _id"),
            "another document's _id": (
                _changed(doc, _id="pln_other"),
                "whose _id is 'pln_other'",
            ),
            "a null _id": (_changed(doc, _id=None), "whose _id is None"),
            "a numeric _id": (_changed(doc, _id=7), "whose _id is 7"),
            "a list _id": (_changed(doc, _id=[plan_id]), "whose _id is ["),
            "no _rev": (_changed(doc, _rev=ABSENT), "without a _rev"),
            "an empty _rev": (_changed(doc, _rev=""), "whose _rev is ''"),
            "a numeric _rev": (_changed(doc, _rev=1), "whose _rev is 1"),
            "a null _rev": (_changed(doc, _rev=None), "whose _rev is None"),
            "a list _rev": (_changed(doc, _rev=["1"]), "whose _rev is ['1']"),
        }
        for case, (body, problem) in cases.items():
            with self.subTest(case=case):
                with self.assertRaises(PlanStoreError) as ctx:
                    require_storage_envelope(plan_id, body)

                self.assertFalse(ctx.exception.retryable)
                message = str(ctx.exception)
                self.assertIn(f"plan '{plan_id}'", message)
                self.assertIn(problem, message)
                self.assertIn("not the plan's stored document", message)

    def test_a_hostile_envelope_is_described_bounded(self):
        doc = _document()
        cases = {
            "a huge _id": (_changed(doc, _id=10**5000), "<unrepresentable int>"),
            "a long _id": (_changed(doc, _id="x" * 100_000), "'xxx"),
            "a huge _rev": (_changed(doc, _rev=[10**5000]), "<unrepresentable list>"),
        }
        for case, (body, shown) in cases.items():
            with self.subTest(case=case):
                with self.assertRaises(PlanStoreError) as ctx:
                    require_storage_envelope(doc["_id"], body)
                self.assertIn(shown, str(ctx.exception))
                self.assertLess(len(str(ctx.exception)), 600)


if __name__ == "__main__":
    unittest.main()
