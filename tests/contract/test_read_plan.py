"""Reading a plan document: presence, version first, identity, then each field.

Every input is a regression-corpus document with at most a few fields
changed, so each case shows which field the rule judges.
"""

from __future__ import annotations

import copy
import dataclasses
import unittest
from datetime import UTC, datetime
from typing import Any

from tests.contract.read_support import (
    HOSTILE,
    ReaderTestCase,
    plan_document,
    plan_document_names,
)
from tests.contract.support import ABSENT, changed
from yggdrasil.contract import PLAN_DOCUMENT
from yggdrasil.contract.diagnostics import MAX_LENGTH
from yggdrasil.contract.read import (
    INVALID_DOCUMENT,
    LOOKUP_IMPOSSIBLE,
    MISSING,
    PRESENT,
    UNAVAILABLE,
    UNSUPPORTED_DOCUMENT,
    PlanRead,
    eligibility,
    eligibility_reason,
    generation_of,
    is_eligible,
    read_plan,
    request_state,
    snapshot_id_for,
)
from yggdrasil.contract.versions import version_sentence

LIFECYCLE = "pln_lifecycle"

# The longest problem message a read may hold: a path, at most two recorded
# values shown bounded, and fixed words.
MAX_MESSAGE = 2 * MAX_LENGTH + 200

# What a usable read derives, and an unusable one leaves None.
DERIVED = (
    "recorded",
    "generation",
    "tokens",
    "failure_policy",
    "approval",
    "request",
    "finalized",
    "steps",
    "snapshot_id",
)


def read(document: Any, plan_id: str = LIFECYCLE) -> PlanRead:
    """Read a document under plan_id."""
    return read_plan(document, plan_id=plan_id)


def paths_and_codes(plan: PlanRead) -> list[tuple[str, str]]:
    """Return each problem of a read as (path, code), in reported order."""
    return [(problem.path, problem.code) for problem in plan.problems]


class TestPresence(ReaderTestCase):
    """A caller passes a body, MISSING or UNAVAILABLE; they are different answers."""

    def test_a_document_the_store_could_not_provide(self):
        for sentinel, presence in ((MISSING, "missing"), (UNAVAILABLE, "unavailable")):
            with self.subTest(presence):
                plan = read(sentinel)

                self.assertEqual(plan.presence, presence)
                self.assertFalse(plan.usable)
                self.assertIsNone(plan.version)
                self.assertIsNone(plan.document)
                self.assertIsNone(plan.eligibility)
                self.assertEqual(plan.problems, ())
                for name in DERIVED:
                    self.assertIsNone(getattr(plan, name), name)
                self.assertIsNone(request_state(sentinel, plan_id=LIFECYCLE))

    def test_a_body_is_present(self):
        self.assertEqual(read(plan_document("draft")).presence, PRESENT)
        self.assertEqual(read(None).presence, PRESENT)

    def test_a_plan_is_never_a_lookup_that_was_impossible(self):
        with self.assertRaises(ValueError):
            read(LOOKUP_IMPOSSIBLE)
        with self.assertRaises(ValueError):
            request_state(LOOKUP_IMPOSSIBLE, plan_id=LIFECYCLE)

    def test_the_expected_id_is_a_non_empty_string(self):
        document = plan_document("draft")
        for function in (read_plan, request_state):
            with self.subTest(function.__name__):
                with self.assertRaises(TypeError):
                    function(document, plan_id=None)
                with self.assertRaises(TypeError):
                    function(document, plan_id=5)
                with self.assertRaises(ValueError):
                    function(document, plan_id="")

    def test_a_sentinel_is_not_a_body_to_judge(self):
        for function in (eligibility, is_eligible, eligibility_reason):
            for sentinel in (MISSING, UNAVAILABLE, LOOKUP_IMPOSSIBLE):
                with self.subTest(function.__name__, sentinel=sentinel.value):
                    with self.assertRaises(TypeError):
                        function(sentinel)


class TestVersionFirst(ReaderTestCase):
    """An unversioned, invalid or unsupported document is inspect-only."""

    CASES = (
        ("unversioned", ABSENT, "unversioned", "unversioned"),
        ("text", "1", "invalid", "invalid_version"),
        ("float", 1.0, "invalid", "invalid_version"),
        ("bool", True, "invalid", "invalid_version"),
        ("null", None, "invalid", "invalid_version"),
        ("zero", 0, "invalid", "invalid_version"),
        ("family 2", 2, "unsupported", "unsupported_version"),
    )

    def test_each_version_state_is_inspect_only(self):
        for name, value, state, code in self.CASES:
            with self.subTest(name):
                document = changed(plan_document("draft"), "schema_version", value)

                plan = read(document)

                assert plan.version is not None and plan.eligibility is not None
                self.assertEqual(plan.version.state, state)
                self.assertFalse(plan.usable)
                self.assertIs(plan.document, document)
                self.assertEqual(paths_and_codes(plan), [("schema_version", code)])
                for derived in DERIVED:
                    self.assertIsNone(getattr(plan, derived), derived)
                self.assertEqual(plan.eligibility.code, UNSUPPORTED_DOCUMENT)
                self.assertEqual(plan.eligibility.problems, plan.problems)
                sentence = version_sentence(PLAN_DOCUMENT, plan.version)
                self.assertIn(sentence, plan.problems[0].message)
                self.assertEqual(plan.eligibility.reason, f"Not eligible: {sentence}")

    def test_the_corpus_documents_outside_the_family(self):
        for name, code in (
            ("pre_contract", "unversioned"),
            ("schema_version_string", "invalid_version"),
            ("schema_version_2", "unsupported_version"),
        ):
            with self.subTest(name):
                plan = read(plan_document(name))

                self.assertFalse(plan.usable)
                self.assertEqual(paths_and_codes(plan), [("schema_version", code)])

    def test_a_missing_schema_version_at_every_whole_document_read(self):
        document = changed(plan_document("approved"), "schema_version", ABSENT)
        expected = (
            "Not eligible: its schema_version is absent, so it is unversioned "
            "(supported plan-document families: [1])"
        )

        plan = read(document)
        assert plan.eligibility is not None
        self.assertFalse(plan.usable)
        self.assertEqual(paths_and_codes(plan), [("schema_version", "unversioned")])
        self.assertEqual(plan.eligibility.code, UNSUPPORTED_DOCUMENT)
        self.assertIsNone(request_state(document, plan_id=LIFECYCLE))
        self.assertEqual(eligibility(document).code, UNSUPPORTED_DOCUMENT)
        self.assertFalse(is_eligible(document))
        self.assertEqual(eligibility_reason(document), expected)

    def test_nothing_else_is_interpreted_in_an_inspect_only_document(self):
        document = plan_document("schema_version_2")
        for path, value in (
            ("_id", "pln_other"),
            ("run_token", "5"),
            ("status", None),
            ("created_at", "yesterday"),
            ("plan", ABSENT),
        ):
            document = changed(document, path, value)

        plan = read(document)

        self.assertEqual(
            paths_and_codes(plan), [("schema_version", "unsupported_version")]
        )

    def test_a_body_that_is_not_an_object_is_invalid(self):
        for name, body in HOSTILE:
            if isinstance(body, dict):
                continue
            with self.subTest(name):
                plan = read(body)

                assert plan.version is not None and plan.eligibility is not None
                self.assertFalse(plan.usable)
                self.assertEqual(plan.version.state, "invalid")
                self.assertEqual(paths_and_codes(plan), [("", "wrong_type")])
                self.assertTrue(
                    plan.problems[0].message.startswith("the plan document")
                )
                self.assertEqual(plan.eligibility.code, INVALID_DOCUMENT)
                self.assertIs(plan.document, body)
                for derived in DERIVED:
                    self.assertIsNone(getattr(plan, derived), derived)


class TestIdentity(ReaderTestCase):
    """A body never establishes its own identity: it is read under an expected ID."""

    def test_identity_is_judged_before_any_other_field(self):
        document = plan_document("draft")
        for path, value in (("run_token", "5"), ("status", ABSENT), ("plan", None)):
            document = changed(document, path, value)

        plan = read(document, plan_id="pln_other")

        self.assertFalse(plan.usable)
        self.assertEqual(paths_and_codes(plan), [("_id", "identity_mismatch")])
        for derived in DERIVED:
            self.assertIsNone(getattr(plan, derived), derived)

    def test_the_mismatch_names_both_ids(self):
        plan = read(plan_document("draft"), plan_id="pln_other")

        assert plan.eligibility is not None
        (problem,) = plan.problems
        self.assertIn("'pln_lifecycle'", problem.message)
        self.assertIn("'pln_other'", problem.message)
        self.assertEqual(plan.eligibility.reason, f"Not eligible: {problem.message}")
        self.assertEqual(plan.eligibility.problems, (problem,))

    def test_only_read_plan_knows_the_expected_id(self):
        document = changed(plan_document("approved"), "_id", "pln_other")

        self.assertTrue(eligibility(document).eligible)
        self.assertFalse(read(document).usable)
        self.assertTrue(read(document, plan_id="pln_other").usable)


class TestRecordedFields(ReaderTestCase):
    """Each descriptive field, recorded beside what was recognised."""

    def field(self, document: dict[str, Any], name: str) -> Any:
        """Return one recorded field of a read, which must be usable."""
        plan = read(document)
        assert plan.recorded is not None
        return getattr(plan.recorded, name)

    def test_a_built_document_has_every_required_field(self):
        plan = read(plan_document("draft"))

        assert plan.recorded is not None
        self.assertEqual(plan.problems, ())
        self.assertEqual(plan.recorded.realm.value, "test_realm")
        self.assertEqual(plan.recorded.scope_kind.value, "project")
        self.assertEqual(plan.recorded.scope_id.value, "P1")
        self.assertEqual(plan.recorded.execution_authority.value, "daemon")
        execution_owner = plan.recorded.execution_owner
        self.assertEqual(
            (execution_owner.value, execution_owner.recorded, execution_owner.present),
            (None, None, True),
        )
        self.assertEqual(
            plan.recorded.created_at.value, datetime(2026, 10, 22, 9, 0, tzinfo=UTC)
        )
        for optional in (
            "notes",
            "approved_at",
            "approved_by",
            "run_requested_at",
            "run_requested_by",
            "last_executed_at",
        ):
            self.assertFalse(getattr(plan.recorded, optional).present, optional)

    def test_required_fields_that_are_absent(self):
        for path in (
            "realm",
            "scope",
            "scope.kind",
            "scope.id",
            "status",
            "execution_authority",
            "execution_owner",
            "created_at",
            "updated_at",
        ):
            with self.subTest(path):
                plan = read(changed(plan_document("draft"), path, ABSENT))

                self.assertEqual(paths_and_codes(plan), [(path, "missing_field")])

    def test_a_required_identifier_of_the_wrong_type(self):
        for path in ("realm", "scope.kind", "scope.id"):
            for name, value in (
                ("empty", ""),
                ("int", 5),
                ("null", None),
                ("list", ["a"]),
            ):
                with self.subTest(path=path, value=name):
                    document = changed(plan_document("draft"), path, value)

                    plan = read(document)

                    self.assertEqual(paths_and_codes(plan), [(path, "wrong_type")])

    def test_a_scope_that_is_not_an_object(self):
        for name, value in (
            ("text", "P1"),
            ("null", None),
            ("list", ["project", "P1"]),
        ):
            with self.subTest(name):
                plan = read(changed(plan_document("draft"), "scope", value))

                assert plan.recorded is not None
                self.assertEqual(paths_and_codes(plan), [("scope", "wrong_type")])
                self.assertFalse(plan.recorded.scope_kind.present)
                self.assertFalse(plan.recorded.scope_id.present)
                self.assertIsNone(plan.snapshot_id)

    def test_a_word_outside_its_vocabulary_is_unknown_without_a_problem(self):
        for path, value in (
            ("status", "pending"),
            ("status", "Approved"),
            ("status", ""),
            ("execution_authority", "cron"),
            ("execution_authority", ""),
        ):
            with self.subTest(path=path, value=value):
                plan = read(changed(plan_document("draft"), path, value))

                recorded = getattr(plan.recorded, path)
                self.assertEqual((recorded.value, recorded.recorded), (None, value))
                self.assertEqual(plan.problems, ())

    def test_a_word_that_is_not_text_is_the_wrong_type(self):
        for path in ("status", "execution_authority"):
            for name, value in (
                ("null", None),
                ("int", 1),
                ("bool", True),
                ("list", []),
            ):
                with self.subTest(path=path, value=name):
                    plan = read(changed(plan_document("draft"), path, value))

                    recorded = getattr(plan.recorded, path)
                    self.assertIsNone(recorded.value)
                    self.assertIs(recorded.recorded, value)
                    self.assertEqual(paths_and_codes(plan), [(path, "wrong_type")])

    def test_the_owner_is_a_non_empty_string_or_null(self):
        owner = self.field(
            changed(plan_document("draft"), "execution_owner", "run_once:abc"),
            "execution_owner",
        )
        self.assertEqual(owner.value, "run_once:abc")
        for name, value in (("empty", ""), ("int", 5), ("object", {})):
            with self.subTest(name):
                plan = read(changed(plan_document("draft"), "execution_owner", value))

                self.assertEqual(
                    paths_and_codes(plan), [("execution_owner", "wrong_type")]
                )

    def test_optional_text(self):
        for path in ("notes", "approved_by", "run_requested_by"):
            with self.subTest(path):
                recorded = self.field(changed(plan_document("draft"), path, "x"), path)
                self.assertEqual((recorded.value, recorded.present), ("x", True))
                # Empty text is valid and says nothing.
                recorded = self.field(changed(plan_document("draft"), path, ""), path)
                self.assertEqual((recorded.value, recorded.recorded), (None, ""))
                self.assertEqual(
                    read(changed(plan_document("draft"), path, "")).problems, ()
                )
                for name, value in (("null", None), ("int", 7), ("list", ["x"])):
                    plan = read(changed(plan_document("draft"), path, value))
                    self.assertEqual(
                        paths_and_codes(plan), [(path, "wrong_type")], name
                    )

    def test_timestamps_that_are_required_or_optional(self):
        required = ("created_at", "updated_at")
        optional = ("approved_at", "run_requested_at", "last_executed_at")
        # The rerun request carries all five.
        base = plan_document("rerun_requested")
        for path in required + optional:
            with self.subTest(path):
                plan = read(changed(base, path, ABSENT))
                expected = [(path, "missing_field")] if path in required else []
                self.assertEqual(paths_and_codes(plan), expected)

                plan = read(changed(base, path, "2026-10-22T11:00:00+02:00"))
                self.assertEqual(
                    getattr(plan.recorded, path).value,
                    datetime(2026, 10, 22, 9, 0, tzinfo=UTC),
                )
                self.assertEqual(plan.problems, ())

                for name, value in (
                    ("null", None),
                    ("no timezone", "2026-10-22T11:00:00"),
                    ("seven fractional digits", "2026-10-22T11:00:00.1234567Z"),
                    ("int", 0),
                ):
                    plan = read(changed(base, path, value))
                    recorded = getattr(plan.recorded, path)
                    self.assertIsNone(recorded.value, name)
                    self.assertIs(recorded.recorded, value, name)
                    self.assertEqual(
                        paths_and_codes(plan), [(path, "bad_timestamp")], name
                    )

    def test_the_finalization_time_is_required(self):
        path = "last_finalized_execution.finalized_at"
        plan = read(changed(plan_document("approved_finalized"), path, ABSENT))
        self.assertEqual(paths_and_codes(plan), [(path, "missing_field")])
        plan = read(changed(plan_document("approved_finalized"), path, None))
        self.assertEqual(paths_and_codes(plan), [(path, "bad_timestamp")])
        assert plan.finalized is not None
        self.assertTrue(plan.finalized.usable)

    def test_a_timestamp_is_read_tolerantly_and_exactly(self):
        plan = read(plan_document("noncanonical_fraction"), plan_id="pln_fractional")
        assert plan.recorded is not None
        self.assertEqual(plan.problems, ())
        self.assertEqual(
            plan.recorded.created_at.value,
            datetime(2026, 10, 22, 9, 0, 0, 250000, tzinfo=UTC),
        )

        plan = read(plan_document("fractional_regenerated"), plan_id="pln_fractional")
        assert plan.recorded is not None
        self.assertEqual(
            plan.recorded.updated_at.value,
            datetime(2026, 10, 23, 8, 0, 0, 123456, tzinfo=UTC),
        )

        plan = read(plan_document("excess_precision"), plan_id="pln_fractional")
        assert plan.recorded is not None
        self.assertIsNone(plan.recorded.created_at.value)
        self.assertEqual(paths_and_codes(plan), [("created_at", "bad_timestamp")])

    def test_a_field_no_answer_needs_is_not_checked(self):
        document = plan_document("draft")
        for path, value in (
            ("preview", None),
            ("approvals_required", "lab"),
            ("source_doc_id", 5),
            ("x_ops_ticket", {"a": 10**5000}),
        ):
            document = changed(document, path, value)

        self.assertEqual(read(document).problems, ())

    def test_a_problem_leaves_the_answers_that_do_not_depend_on_it(self):
        clean = read(plan_document("approved_finalized"))
        for path, value in (
            ("created_at", "yesterday"),
            ("updated_at", None),
            ("realm", 5),
            ("notes", 7),
            ("execution_owner", 5),
            ("last_finalized_execution.finalized_at", "never"),
            ("last_finalized_execution.report.counts", None),
            ("plan.steps.0.deps", ABSENT),
        ):
            with self.subTest(path):
                plan = read(changed(plan_document("approved_finalized"), path, value))

                self.assertEqual(len(plan.problems), 1)
                self.assertEqual(plan.eligibility, clean.eligibility)
                self.assertEqual(plan.request, clean.request)
                self.assertEqual(plan.generation, clean.generation)
                self.assertEqual(plan.tokens, clean.tokens)
                self.assertEqual(plan.failure_policy, clean.failure_policy)
                assert plan.finalized is not None
                self.assertTrue(plan.finalized.usable)

    def test_a_bad_generation_leaves_the_rest(self):
        clean = read(plan_document("draft"))

        plan = read(changed(plan_document("draft"), "plan_generation", 5))

        self.assertEqual(plan.eligibility, clean.eligibility)
        self.assertEqual(plan.steps, clean.steps)
        self.assertEqual(plan.recorded, clean.recorded)
        self.assertEqual(plan.snapshot_id, clean.snapshot_id)


class TestControlChecks(ReaderTestCase):
    """The control checks' structured problems, copied, each finding once."""

    def test_the_token_details_are_copied(self):
        plan = read(plan_document("executed_above_run"))

        assert plan.tokens is not None
        self.assertEqual(plan.problems, plan.tokens.details)

    def test_the_execution_definition_details_are_copied(self):
        plan = read(changed(plan_document("draft"), "plan.steps.1.fn_ref", ABSENT))

        self.assertEqual(
            paths_and_codes(plan),
            [("plan.steps[1].fn_ref", "invalid_execution_definition")],
        )

    def test_a_finding_of_two_checks_is_reported_once(self):
        cases = (
            (
                "plan.failure_policy",
                ABSENT,
                [("plan.failure_policy", "invalid_failure_policy")],
            ),
            (
                "plan.failure_policy",
                None,
                [("plan.failure_policy", "invalid_failure_policy")],
            ),
            ("plan", ABSENT, [("plan", "invalid_failure_policy")]),
            ("plan", "text", [("plan", "invalid_failure_policy")]),
            ("plan.steps", ABSENT, [("plan.steps", "invalid_execution_definition")]),
            ("plan.steps", None, [("plan.steps", "invalid_execution_definition")]),
            ("plan.steps", "text", [("plan.steps", "invalid_execution_definition")]),
            ("plan.steps.0", 5, [("plan.steps[0]", "invalid_execution_definition")]),
            (
                "plan.steps.0.step_id",
                None,
                [("plan.steps[0].step_id", "invalid_execution_definition")],
            ),
            (
                "plan.steps.0.deps",
                None,
                [("plan.steps[0].deps", "invalid_execution_definition")],
            ),
            (
                "plan.steps.0.name",
                ABSENT,
                [("plan.steps[0].name", "invalid_execution_definition")],
            ),
        )
        for path, value, expected in cases:
            with self.subTest(path=path, value="absent" if value is ABSENT else value):
                plan = read(changed(plan_document("draft"), path, value))

                self.assertEqual(paths_and_codes(plan), expected)


class TestPlannedSteps(ReaderTestCase):
    """The inventory is complete or not returned; each fault is reported at its path."""

    def assert_unavailable(
        self, path: str, value: Any, expected: list[tuple[str, str]]
    ) -> None:
        """Assert one change leaves the inventory unavailable with these problems."""
        plan = read(changed(plan_document("draft"), path, value))

        self.assertTrue(plan.usable)
        self.assertIsNone(plan.steps)
        self.assertEqual(paths_and_codes(plan), expected)

    def test_a_step_id_that_is_not_a_non_empty_string(self):
        for name, value in (("int", 3), ("empty", ""), ("list", ["demux"])):
            with self.subTest(name):
                self.assert_unavailable(
                    "plan.steps.0.step_id",
                    value,
                    [("plan.steps[0].step_id", "wrong_type")],
                )

    def test_the_corpus_plan_with_a_step_id_that_is_not_text(self):
        plan = read(plan_document("non_string_step_id"), plan_id="pln_step_id")

        self.assertIsNone(plan.steps)
        self.assertEqual(
            paths_and_codes(plan), [("plan.steps[0].step_id", "wrong_type")]
        )

    def test_a_repeated_step_id(self):
        self.assert_unavailable(
            "plan.steps.1.step_id",
            "demux",
            [("plan.steps[1].step_id", "invalid_execution_definition")],
        )

    def test_dependencies_that_are_not_a_list_of_strings(self):
        for name, value, path in (
            ("text", "demux", "plan.steps[1].deps"),
            ("object", {"demux": True}, "plan.steps[1].deps"),
            ("int entry", ["demux", 3], "plan.steps[1].deps[1]"),
            ("null entry", [None], "plan.steps[1].deps[0]"),
        ):
            with self.subTest(name):
                self.assert_unavailable(
                    "plan.steps.1.deps", value, [(path, "wrong_type")]
                )

    def test_every_fault_is_reported(self):
        document = plan_document("draft")
        for path, value in (
            ("plan.steps.0.step_id", 3),
            ("plan.steps.1.deps", "demux"),
        ):
            document = changed(document, path, value)

        plan = read(document)

        self.assertIsNone(plan.steps)
        self.assertEqual(
            paths_and_codes(plan),
            [
                ("plan.steps[0].step_id", "wrong_type"),
                ("plan.steps[1].deps", "wrong_type"),
            ],
        )

    def test_a_name_is_text_or_none_and_never_hides_the_inventory(self):
        plan = read(changed(plan_document("draft"), "plan.steps.0.name", ""))
        assert plan.steps is not None
        self.assertIsNone(plan.steps[0].name)
        self.assertEqual(plan.problems, ())

        plan = read(changed(plan_document("draft"), "plan.steps.0.name", 5))
        assert plan.steps is not None
        self.assertIsNone(plan.steps[0].name)
        self.assertEqual(paths_and_codes(plan), [("plan.steps[0].name", "wrong_type")])

    def test_a_dependency_is_found_only_in_the_same_inventory(self):
        document = changed(
            plan_document("draft"), "plan.steps.1.deps", ["qc", "demux", "QC"]
        )

        plan = read(document)

        assert plan.steps is not None
        self.assertEqual(
            [(ref.step_id, ref.found) for ref in plan.steps[1].deps],
            [("qc", True), ("demux", True), ("QC", False)],
        )


class TestReaderProperties(ReaderTestCase):
    """Silent, never raising, immutable, bounded, and leaving the body untouched."""

    # Where a hostile value is planted, one at a time.
    PATHS = (
        "_id",
        "realm",
        "scope",
        "scope.kind",
        "scope.id",
        "status",
        "plan",
        "plan.steps",
        "plan.steps.0",
        "plan.steps.0.step_id",
        "plan.steps.0.name",
        "plan.steps.1.deps",
        "plan.failure_policy",
        "plan_generation",
        "run_token",
        "executed_run_token",
        "execution_authority",
        "execution_owner",
        "notes",
        "created_at",
        "approved_at",
        "last_finalized_execution",
        "last_finalized_execution.execution_id",
        "last_finalized_execution.run_token",
        "last_finalized_execution.outcome",
        "last_finalized_execution.finalized_at",
        "last_finalized_execution.report",
        "last_finalized_execution.report.run_token",
        "last_finalized_execution.report.counts",
        "last_finalized_execution.report.counts.failed",
    )

    def read_everything(self, document: Any, plan_id: str = LIFECYCLE) -> PlanRead:
        """Call every plan-side function on document, and return the read."""
        plan = read_plan(document, plan_id=plan_id)
        request_state(document, plan_id=plan_id)
        eligibility(document)
        is_eligible(document)
        eligibility_reason(document)
        generation_of(document)
        snapshot_id_for(document, plan_id=plan_id)
        return plan

    def assert_bounded(self, plan: PlanRead) -> None:
        """Assert every problem message of a read is bounded."""
        for problem in plan.problems:
            self.assertLessEqual(len(problem.message), MAX_MESSAGE, problem.path)
        if plan.eligibility is not None:
            self.assertLessEqual(len(plan.eligibility.reason), 3 * MAX_MESSAGE)

    def test_every_corpus_document_is_read_silently(self):
        with self.assert_silent():
            for name in plan_document_names():
                document = plan_document(name)
                plan_id = document["_id"]
                plan = self.read_everything(document, plan_id)
                self.assert_bounded(plan)

    def test_no_hostile_value_makes_a_read_raise_or_log(self):
        base = plan_document("approved_finalized")
        variations = 0
        with self.assert_silent():
            for path in self.PATHS:
                for name, value in HOSTILE:
                    with self.subTest(path=path, value=name):
                        plan = self.read_everything(changed(base, path, value))
                        self.assert_bounded(plan)
                        variations += 1
            for name, value in HOSTILE:
                with self.subTest(body=name):
                    self.read_everything(value)
                    variations += 1
        self.assertEqual(variations, (len(self.PATHS) + 1) * len(HOSTILE))

    def test_a_recorded_value_is_kept_unconverted(self):
        for name, value in HOSTILE:
            with self.subTest(name):
                plan = read(changed(plan_document("draft"), "notes", value))

                assert plan.recorded is not None
                self.assertIs(plan.recorded.notes.recorded, value)

    def test_the_body_is_passed_through_untouched(self):
        for name in plan_document_names():
            with self.subTest(name):
                document = plan_document(name)
                before = copy.deepcopy(document)

                plan = read(document, plan_id=document["_id"])

                self.assertIs(plan.document, document)
                self.assertEqual(document, before)

    def test_reading_twice_gives_the_same_answer(self):
        for name in plan_document_names():
            with self.subTest(name):
                document = plan_document(name)
                self.assertEqual(
                    read(document, plan_id=document["_id"]),
                    read(plan_document(name), plan_id=document["_id"]),
                )

    def test_results_are_immutable_and_hold_tuples(self):
        plan = read(plan_document("continue_independent_failures"), plan_id="pln_lanes")

        assert plan.steps is not None and plan.finalized is not None
        results = (plan, plan.steps[0], plan.finalized, plan.recorded, plan.request)
        for result in results:
            with self.subTest(type(result).__name__):
                field = dataclasses.fields(result)[0].name
                with self.assertRaises(dataclasses.FrozenInstanceError):
                    setattr(result, field, None)
        self.assertIsInstance(plan.problems, tuple)
        self.assertIsInstance(plan.steps, tuple)
        self.assertIsInstance(plan.steps[1].deps, tuple)
        self.assertIsInstance(plan.finalized.disagreeing, tuple)


if __name__ == "__main__":
    unittest.main()
