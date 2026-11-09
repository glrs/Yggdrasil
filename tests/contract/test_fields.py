"""The contract's spellings agree with what producers write and schemas publish.

``yggdrasil.contract.fields`` names every field and word of the contract once.
The producers (``flow``, ``storage``, ``ops``) and the published schemas still
spell their own, so each spelling here is compared with the place that
decides it. A producer that renames a field, or a schema that gains a value,
fails here until ``fields`` follows.
"""

from __future__ import annotations

import ast
import json
import unittest
from enum import Enum
from pathlib import Path
from typing import Any

from tests.contract.support import SpoolTestCase
from yggdrasil.contract import controls, fields, schema, versions
from yggdrasil.contract.schema import schema_for
from yggdrasil.core import engine
from yggdrasil.flow.errors import PermanentStepError
from yggdrasil.flow.events import attempt_records
from yggdrasil.flow.model import VALID_FAILURE_POLICIES, StepResult
from yggdrasil.flow.outcomes import (
    ExecutionOutcome,
    StepFailure,
    StepOutcome,
    TerminationReason,
)
from yggdrasil.flow.step import StepContext
from yggdrasil.ops import snapshot
from yggdrasil.storage import plan_documents
from yggdrasil.storage.plan_updates import ExecutionFinalization


def values(enum: type[Enum]) -> frozenset[Any]:
    """Return the values of an enum."""
    return frozenset(member.value for member in enum)


def enum_of(document: dict[str, Any], name: str) -> frozenset[Any]:
    """Return the values a schema's ``$defs`` entry allows."""
    return frozenset(document["$defs"][name]["enum"])


def properties(definition: dict[str, Any]) -> frozenset[str]:
    """Return the property names of a schema definition."""
    return frozenset(definition["properties"])


class TestProducerVocabularies(unittest.TestCase):
    """Each word is the one the producing module uses."""

    def test_outcomes_and_reasons(self):
        self.assertEqual(fields.STEP_OUTCOMES, values(StepOutcome))
        self.assertEqual(fields.EXECUTION_OUTCOMES, values(ExecutionOutcome))
        self.assertEqual(fields.TERMINATION_REASONS, values(TerminationReason))

    def test_each_word_constant_matches_its_member(self):
        pairs = (
            (fields.SUCCEEDED, StepOutcome.SUCCEEDED),
            (fields.REUSED, StepOutcome.REUSED),
            (fields.FAILED, StepOutcome.FAILED),
            (fields.BLOCKED, StepOutcome.BLOCKED),
            (fields.SUCCEEDED, ExecutionOutcome.SUCCEEDED),
            (fields.FAILED, ExecutionOutcome.FAILED),
            (fields.COMPLETED, TerminationReason.COMPLETED),
            (fields.FAILED_FAST, TerminationReason.FAILED_FAST),
            (fields.CANCELLED, TerminationReason.CANCELLED),
            (fields.PREFLIGHT_REJECTED, TerminationReason.PREFLIGHT_REJECTED),
            (fields.ORCHESTRATION_ERROR, TerminationReason.ORCHESTRATION_ERROR),
        )
        for word, member in pairs:
            with self.subTest(member=member):
                self.assertEqual(word, member.value)

    def test_failure_policies_and_authorities(self):
        self.assertEqual(fields.FAILURE_POLICIES, VALID_FAILURE_POLICIES)
        self.assertEqual(
            fields.EXECUTION_AUTHORITIES, plan_documents.VALID_EXECUTION_AUTHORITIES
        )

    def test_error_kinds_are_the_two_a_failure_has(self):
        self.assertEqual(
            StepFailure.__dataclass_fields__["kind"].default, fields.PERMANENT
        )
        engine_source = Path(engine.__file__).read_text("utf-8")
        self.assertIn(f'kind="{fields.TRANSIENT}"', engine_source)
        self.assertEqual(
            fields.ERROR_KINDS, frozenset({fields.PERMANENT, fields.TRANSIENT})
        )

    def test_snapshot_words(self):
        self.assertEqual(fields.ATTEMPT, snapshot.PROJECTION_ATTEMPT)
        self.assertEqual(fields.LEGACY, snapshot.PROJECTION_LEGACY)
        self.assertEqual(
            fields.PROJECTIONS,
            {snapshot.PROJECTION_ATTEMPT, snapshot.PROJECTION_LEGACY},
        )
        self.assertEqual(fields.RUNNING, snapshot.ATTEMPT_RUNNING)
        self.assertEqual(fields.FINISHED, snapshot.ATTEMPT_FINISHED)
        self.assertEqual(
            fields.ATTEMPT_STATES,
            {snapshot.ATTEMPT_RUNNING, snapshot.ATTEMPT_FINISHED},
        )
        self.assertEqual(
            fields.STEP_STATES,
            {
                snapshot.STATE_PENDING,
                snapshot.STATE_INTERRUPTED,
                snapshot.STATE_UNREACHED,
                snapshot.STATE_UNKNOWN,
            },
        )

    def test_outcome_events_are_the_ones_the_snapshot_reads(self):
        recorded = {
            event: outcome.value for outcome, event in snapshot._OUTCOME_STATES.items()
        }
        self.assertEqual(dict(fields.OUTCOME_EVENTS), recorded)
        self.assertEqual(fields.EVENT_STEP_BLOCKED, attempt_records.STEP_BLOCKED_EVENT)

    def test_step_statuses_are_outcomes_states_and_started(self):
        self.assertEqual(
            fields.STEP_STATUSES,
            fields.STEP_OUTCOMES | fields.STEP_STATES | {fields.STARTED},
        )

    def test_plan_document_field_names(self):
        self.assertEqual(
            fields.PLAN_GENERATION_FIELD, plan_documents.PLAN_GENERATION_FIELD
        )
        self.assertEqual(
            fields.LAST_FINALIZED_EXECUTION_FIELD,
            plan_documents.LAST_FINALIZED_EXECUTION_FIELD,
        )
        self.assertEqual(fields.CREATED_AT_FIELD, plan_documents.CREATED_AT_FIELD)

    def test_the_finalization_identity_is_what_a_record_and_its_report_share(self):
        finalization = ExecutionFinalization(
            plan_id="p",
            schema_version=1,
            plan_generation="g",
            run_token=1,
            execution_id="e",
            execution_authority=fields.RUN_ONCE,
            execution_owner=None,
            failure_policy=fields.FAIL_FAST,
            outcome=ExecutionOutcome.SUCCEEDED,
            termination_reason=TerminationReason.COMPLETED,
            report={
                "execution_id": "e",
                "plan_id": "p",
                "plan_generation": "g",
                "run_token": 1,
                "failure_policy": fields.FAIL_FAST,
                "outcome": fields.SUCCEEDED,
                "termination_reason": fields.COMPLETED,
            },
        )
        self.assertEqual(
            fields.FINALIZATION_IDENTITY_FIELDS, frozenset(finalization.identity())
        )


class TestPublishedSchemas(unittest.TestCase):
    """The schemas allow the words and the fields ``fields`` names."""

    def setUp(self) -> None:
        self.document = schema_for("plan_document", 1)
        self.status = schema_for("plan_status", 1)
        self.report = schema_for("attempt_report", 1)

    def test_plan_document_fields(self):
        self.assertEqual(
            fields.PLAN_DOCUMENT_FIELDS, frozenset(self.document["properties"])
        )

    def test_plan_document_vocabularies(self):
        self.assertEqual(fields.APPROVAL_STATUSES, enum_of(self.document, "status"))
        self.assertEqual(
            fields.EXECUTION_AUTHORITIES,
            enum_of(self.document, "execution_authority"),
        )
        self.assertEqual(
            fields.FAILURE_POLICIES, enum_of(self.document, "failure_policy")
        )

    def test_plan_and_step_definition_fields(self):
        defs = self.document["$defs"]
        self.assertEqual(
            frozenset(controls.PLAN_DEFINITION_FIELDS), properties(defs["plan"])
        )
        self.assertEqual(
            frozenset(controls.STEP_DEFINITION_FIELDS), properties(defs["step"])
        )

    def test_finalized_record_fields(self):
        record = self.document["$defs"]["last_finalized_execution"]
        self.assertEqual(fields.FINALIZED_RECORD_FIELDS, properties(record))

    def test_snapshot_fields(self):
        self.assertEqual(fields.SNAPSHOT_FIELDS, properties(self.status))
        self.assertEqual(
            fields.ATTEMPT_FIELDS, properties(self.status["$defs"]["attempt"])
        )
        self.assertEqual(
            fields.STEP_ENTRY_FIELDS, properties(self.status["$defs"]["step"])
        )

    def test_snapshot_vocabularies(self):
        self.assertEqual(fields.PROJECTIONS, enum_of(self.status, "projection"))
        self.assertEqual(fields.ATTEMPT_STATES, enum_of(self.status, "attempt_state"))
        self.assertEqual(fields.STEP_STATES, enum_of(self.status, "step_state"))
        self.assertEqual(
            fields.EXECUTION_AUTHORITIES,
            enum_of(self.status, "execution_authority"),
        )

    def test_report_vocabularies(self):
        self.assertEqual(fields.EXECUTION_OUTCOMES, enum_of(self.report, "outcome"))
        self.assertEqual(fields.STEP_OUTCOMES, enum_of(self.report, "step_outcome"))
        self.assertEqual(
            fields.TERMINATION_REASONS, enum_of(self.report, "termination_reason")
        )


class TestEmittedEvents(SpoolTestCase):
    """The step events a real attempt publishes are the ones ``fields`` lists."""

    def emitted(self) -> set[str]:
        """Return the step event types found anywhere in the spool."""
        found: set[str] = set()
        for record in self.spool.rglob("*.json"):
            event = json.loads(record.read_text("utf-8"))
            if isinstance(event, dict) and isinstance(event.get("type"), str):
                found.add(event["type"])
        return {event for event in found if event.startswith("step.")}

    def test_attempts_publish_exactly_the_listed_events(self):
        def work(ctx: StepContext) -> StepResult:
            ctx.progress(50, "half")
            return StepResult()

        plan = self.plan("a")
        self.finished(plan, work)
        self.finished(plan, work)  # reused from the first attempt

        def fail(ctx: StepContext) -> None:
            raise PermanentStepError("boom")

        chain = self.plan("f", "g")
        chain.steps[1].deps = ["f"]
        self.finished(chain, fail)

        self.assertEqual(self.emitted(), set(fields.STEP_EVENTS))


class TestOneSpellingEverywhere(unittest.TestCase):
    """The modules that used to spell a name now share the object."""

    def test_controls_versions_and_schema_share_fields_objects(self):
        self.assertIs(controls.FAILURE_POLICIES, fields.FAILURE_POLICIES)
        self.assertIs(controls.RUN_TOKEN_FIELD, fields.RUN_TOKEN_FIELD)
        self.assertIs(
            controls.EXECUTED_RUN_TOKEN_FIELD, fields.EXECUTED_RUN_TOKEN_FIELD
        )
        self.assertIs(versions.SCHEMA_VERSION_FIELD, fields.SCHEMA_VERSION_FIELD)
        self.assertIs(schema.RUN_TOKEN_FIELD, fields.RUN_TOKEN_FIELD)
        self.assertIs(schema.EXECUTED_RUN_TOKEN_FIELD, fields.EXECUTED_RUN_TOKEN_FIELD)

    def test_fields_imports_nothing_from_the_package(self):
        tree = ast.parse(Path(fields.__file__).read_text("utf-8"))
        imported = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Import | ast.ImportFrom)
        ]
        for node in imported:
            module = getattr(node, "module", None) or ""
            names = [alias.name for alias in node.names]
            with self.subTest(line=node.lineno):
                self.assertEqual(getattr(node, "level", 0), 0)
                self.assertFalse(
                    any(n.split(".")[0] == "yggdrasil" for n in (module, *names)),
                    (module, names),
                )


if __name__ == "__main__":
    unittest.main()
