"""The published schemas, and what ``validate`` reports.

Documents start as builder outputs and are changed in one place, so each case
shows exactly which field a verdict is about.
"""

from __future__ import annotations

import re
import unittest
from collections.abc import Iterator
from typing import Any

from jsonschema import Draft202012Validator
from referencing import Registry
from referencing.exceptions import Unresolvable
from referencing.jsonschema import DRAFT202012

from tests.contract.support import (
    ABSENT,
    SpoolTestCase,
    changed,
    problems,
    projection_snapshot,
    stored_snapshot,
)
from tests.plan_store_support import (
    SCOPE,
    finalization_for,
    finished_attempt,
    make_plan,
)
from yggdrasil.contract import (
    ATTEMPT_REPORT,
    CURRENT_FAMILIES,
    PLAN_DOCUMENT,
    PLAN_STATUS,
)
from yggdrasil.contract.controls import FAILURE_POLICIES, MAX_TOKEN
from yggdrasil.contract.schema import (
    BAD_TIMESTAMP,
    INVALID_TOKENS,
    INVALID_VERSION,
    MISSING_FIELD,
    NOT_IN_VOCABULARY,
    OUT_OF_RANGE,
    PROBLEM_CODES,
    UNEXPECTED_FIELD,
    UNSUPPORTED_VERSION,
    UNVERSIONED,
    WRONG_TYPE,
    ContractError,
    _registry,
    schema_for,
    validate,
)
from yggdrasil.contract.timestamps import CANONICAL_MAX_LENGTH, CANONICAL_PATTERN
from yggdrasil.flow.model import VALID_FAILURE_POLICIES, StepSpec
from yggdrasil.flow.outcomes import (
    AttemptReport,
    ExecutionOutcome,
    StepOutcome,
    TerminationReason,
)
from yggdrasil.ops.snapshot import (
    ATTEMPT_FINISHED,
    ATTEMPT_RUNNING,
    PROJECTION_ATTEMPT,
    PROJECTION_LEGACY,
    STATE_INTERRUPTED,
    STATE_PENDING,
    STATE_UNKNOWN,
    STATE_UNREACHED,
)
from yggdrasil.storage.plan_documents import (
    VALID_EXECUTION_AUTHORITIES,
    build_plan_document,
)
from yggdrasil.storage.plan_updates import build_finalized_document

KINDS = (PLAN_DOCUMENT, PLAN_STATUS, ATTEMPT_REPORT)
CANONICAL = "2026-10-22T10:00:00Z"
DRAFT_2020_12 = "https://json-schema.org/draft/2020-12/schema"

# Timestamps that are not in the canonical spelling, or not real.
NONCANONICAL_TIMESTAMPS = (
    "2026-10-22T10:00:00+00:00",  # the spelling earlier releases wrote
    "2026-02-30T00:00:00Z",  # no such date
    "2026-10-22T10:00:00Z\n",  # a trailing newline
    "2026-10-22T10:00:00.5Z\n",  # a trailing newline after a fraction
    "2026-10-22T10:00:00.50Z",  # a fraction ending in 0
    "2026-10-22T10:00:00.1234567Z",  # a fraction of seven digits
    "2026-10-22t10:00:00z",  # lowercase
    "2026-10-22 10:00:00Z",  # a space
)

# Canonical timestamps with a fraction of a second.
FRACTIONAL_TIMESTAMPS = (
    "2026-10-22T10:00:00.5Z",
    "2026-10-22T10:00:00.000001Z",
    "2026-10-22T10:00:00.123456Z",
)

# Timestamps followed by a newline, short enough that only the pattern can
# stop them: none is longer than the timestamp's maximum length.
FINAL_NEWLINES = (
    "2026-10-22T10:00:00Z\n",
    "2026-10-22T10:00:00.5Z\n",
    "2026-10-22T10:00:00.12345Z\n",
)


def plan_document(**fields: Any) -> dict[str, Any]:
    """A built plan document with top-level fields replaced; ABSENT removes one."""
    document = build_plan_document(make_plan(), "test_realm", dict(SCOPE))
    for name, value in fields.items():
        document = changed(document, name, value)
    return document


def finalized_document() -> dict[str, Any]:
    """A built plan document after its first request was finalized, with actor stamps."""
    document = plan_document(
        approved_at=CANONICAL,
        approved_by="alice",
        run_requested_at=CANONICAL,
        run_requested_by="bob",
    )
    request = finalization_for(document)
    return build_finalized_document(document, request, now=CANONICAL)


def report() -> dict[str, Any]:
    """The closed report of an attempt in which s1 failed and blocked s2."""
    context = finished_attempt(
        make_plan(), plan_generation="gen", run_token=0, ending="drained_failure"
    )
    return context.report.to_dict()


def references(node: Any) -> Iterator[str]:
    """Yield every ``$ref`` in a schema."""
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "$ref":
                yield value
            else:
                yield from references(value)
    elif isinstance(node, list):
        for item in node:
            yield from references(item)


def keyword_locations(
    node: Any, keyword: str, at: str = ""
) -> Iterator[tuple[str, Any]]:
    """Yield (JSON pointer, value) of every use of one keyword in a schema."""
    if isinstance(node, dict):
        for key, value in node.items():
            if key == keyword:
                yield at, value
            yield from keyword_locations(value, keyword, f"{at}/{key}")
    elif isinstance(node, list):
        for index, item in enumerate(node):
            yield from keyword_locations(item, keyword, f"{at}/{index}")


def plain_validator(kind: str) -> Any:
    """A stock 2020-12 validator over the published resources, with nothing added."""
    registry = Registry().with_resources(
        (schema["$id"], DRAFT202012.create_resource(schema))
        for schema in (schema_for(other, 1) for other in KINDS)
    )
    return Draft202012Validator(schema_for(kind, 1), registry=registry)


class TestResources(unittest.TestCase):
    """Three handwritten resources, resolvable offline and only among themselves."""

    def test_each_resource_names_its_dialect_kind_and_family(self):
        for kind in KINDS:
            with self.subTest(kind=kind):
                schema = schema_for(kind, 1)
                self.assertEqual(schema["$schema"], DRAFT_2020_12)
                self.assertEqual(schema["$id"], f"urn:yggdrasil:contract:{kind}:1")

    def test_each_resource_is_a_valid_schema(self):
        for kind in KINDS:
            with self.subTest(kind=kind):
                Draft202012Validator.check_schema(schema_for(kind, 1))

    def test_every_reference_resolves_within_the_bundle(self):
        registry = _registry()
        for kind in KINDS:
            schema = schema_for(kind, 1)
            resolver = registry.resolver(base_uri=schema["$id"])
            for ref in references(schema):
                with self.subTest(kind=kind, ref=ref):
                    self.assertIsNotNone(resolver.lookup(ref).contents)

    def test_nothing_outside_the_bundle_resolves(self):
        resolver = _registry().resolver()
        for ref in (
            "urn:yggdrasil:contract:plan_document:2",
            "urn:yggdrasil:contract:daemon_lease:1",
            "https://example.org/schema.json",
        ):
            with self.subTest(ref=ref):
                with self.assertRaises(Unresolvable):
                    resolver.lookup(ref)

    def test_schema_for_returns_an_independent_copy(self):
        first = schema_for(PLAN_DOCUMENT, 1)
        first["properties"]["ticket"] = {}
        first["$defs"]["status"]["enum"].append("pending")

        second = schema_for(PLAN_DOCUMENT, 1)

        self.assertNotIn("ticket", second["properties"])
        self.assertEqual(second["$defs"]["status"]["enum"], ["draft", "approved"])
        self.assertEqual(
            problems(PLAN_DOCUMENT, plan_document(status="pending")),
            [("status", NOT_IN_VOCABULARY)],
        )

    def test_an_unknown_kind_or_family_is_a_lookup_error(self):
        for kind, family in (
            (PLAN_DOCUMENT, 2),
            (PLAN_DOCUMENT, 0),
            (PLAN_DOCUMENT, True),
            (PLAN_DOCUMENT, 1.0),
            (PLAN_DOCUMENT, "1"),
            ("daemon_lease", 1),
            ("", 1),
        ):
            with self.subTest(kind=kind, family=family):
                with self.assertRaises(LookupError):
                    schema_for(kind, family)  # type: ignore[arg-type]
        for kind in ("daemon_lease", "plan", ""):
            with self.subTest(kind=kind):
                with self.assertRaises(LookupError):
                    validate(kind, plan_document())

    def test_the_plan_documents_top_level_fields(self):
        schema = schema_for(PLAN_DOCUMENT, 1)
        required = {
            "schema_version",
            "_id",
            "realm",
            "scope",
            "status",
            "plan",
            "preview",
            "plan_generation",
            "run_token",
            "executed_run_token",
            "execution_authority",
            "execution_owner",
            "created_at",
            "updated_at",
        }
        optional = {
            "_rev",
            "source_doc_id",
            "source_doc_rev",
            "notes",
            "last_executed_at",
            "last_finalized_execution",
            "approved_at",
            "approved_by",
            "run_requested_at",
            "run_requested_by",
            "approvals_required",
        }
        self.assertEqual(set(schema["required"]), required)
        self.assertEqual(set(schema["properties"]), required | optional)
        self.assertEqual(schema["patternProperties"], {"^x_": {}})
        self.assertIs(schema["additionalProperties"], False)


class TestVersions(unittest.TestCase):
    """The marker is read first; without a supported family there is no schema."""

    def documents(self) -> dict[str, dict[str, Any]]:
        """A conforming document of each kind that carries a marker."""
        return {
            PLAN_DOCUMENT: plan_document(),
            PLAN_STATUS: projection_snapshot(None, {}),
        }

    def test_the_builders_documents_are_family_one(self):
        for kind, document in self.documents().items():
            with self.subTest(kind=kind):
                self.assertEqual(document["schema_version"], CURRENT_FAMILIES[kind])
                self.assertEqual(problems(kind, document), [])

    def test_each_version_state_is_one_problem_at_the_marker(self):
        cases = [
            (ABSENT, UNVERSIONED),
            ("1", INVALID_VERSION),
            (1.0, INVALID_VERSION),
            (True, INVALID_VERSION),
            (None, INVALID_VERSION),
            (0, INVALID_VERSION),
            (-1, INVALID_VERSION),
            (2, UNSUPPORTED_VERSION),
        ]
        for kind, document in self.documents().items():
            for value, code in cases:
                with self.subTest(kind=kind, value=value):
                    # Another problem in the document is not reported: no
                    # schema applies to it.
                    broken = changed(
                        changed(document, "realm", 7), "schema_version", value
                    )
                    self.assertEqual(problems(kind, broken), [("schema_version", code)])

    def test_a_document_that_is_not_an_object_has_an_invalid_marker(self):
        for kind in (PLAN_DOCUMENT, PLAN_STATUS):
            for document in ([1], "doc", None):
                with self.subTest(kind=kind, document=document):
                    self.assertEqual(
                        problems(kind, document), [("schema_version", INVALID_VERSION)]
                    )
        self.assertEqual(problems(ATTEMPT_REPORT, [1]), [("", WRONG_TYPE)])

    def test_the_report_has_no_marker_of_its_own(self):
        self.assertEqual(problems(ATTEMPT_REPORT, report()), [])
        self.assertEqual(
            problems(ATTEMPT_REPORT, {**report(), "schema_version": 1}),
            [("schema_version", UNEXPECTED_FIELD)],
        )


class TestTokens(unittest.TestCase):
    """JSON integers within bounds, and executed never above run."""

    def test_a_token_must_be_a_json_integer(self):
        for field in ("run_token", "executed_run_token"):
            for value in (5.0, 0.0, True, False, "5", None, [5]):
                with self.subTest(field=field, value=value):
                    self.assertEqual(
                        problems(PLAN_DOCUMENT, plan_document(**{field: value})),
                        [(field, WRONG_TYPE)],
                    )

    def test_a_token_must_be_in_range(self):
        for field, value in (
            ("run_token", -1),
            ("run_token", MAX_TOKEN + 1),
            ("run_token", 10**400),
            ("executed_run_token", -2),
            ("executed_run_token", MAX_TOKEN + 1),
        ):
            with self.subTest(field=field, value=value):
                self.assertEqual(
                    problems(PLAN_DOCUMENT, plan_document(**{field: value})),
                    [(field, OUT_OF_RANGE)],
                )

    def test_the_largest_tokens_are_valid(self):
        document = plan_document(run_token=MAX_TOKEN, executed_run_token=MAX_TOKEN)
        self.assertEqual(problems(PLAN_DOCUMENT, document), [])

    def test_executed_above_run_is_invalid_tokens(self):
        document = plan_document(run_token=4, executed_run_token=5)
        self.assertEqual(
            problems(PLAN_DOCUMENT, document),
            [("executed_run_token", INVALID_TOKENS)],
        )

    def test_a_token_invalid_on_its_own_is_reported_once(self):
        document = plan_document(run_token="5", executed_run_token=7)
        self.assertEqual(problems(PLAN_DOCUMENT, document), [("run_token", WRONG_TYPE)])

    def test_integers_are_json_integers_everywhere(self):
        for path, value in (
            ("counts.failed", 1.0),
            ("counts.unreached", True),
            ("run_token", 0.0),
        ):
            with self.subTest(path=path, value=value):
                self.assertEqual(
                    problems(ATTEMPT_REPORT, changed(report(), path, value)),
                    [(path, WRONG_TYPE)],
                )


class TestProblemReports(unittest.TestCase):
    """Every problem, once per path and code, sorted by path, with a description."""

    def test_problem_codes(self):
        self.assertEqual(
            PROBLEM_CODES,
            {
                "unversioned",
                "invalid_version",
                "unsupported_version",
                "missing_field",
                "unexpected_field",
                "wrong_type",
                "not_in_vocabulary",
                "out_of_range",
                "bad_timestamp",
                "invalid_tokens",
                "schema",
            },
        )

    def test_every_problem_is_reported_sorted_by_path(self):
        plan = make_plan()
        plan.steps = [
            StepSpec(step_id=f"s{index}", name="echo", fn_ref="m:f", params={})
            for index in range(11)
        ]
        document = build_plan_document(plan, "test_realm", dict(SCOPE))
        document["ticket"] = "OPS-1"
        document["status"] = "pending"
        document["run_token"] = "5"
        document["plan"]["steps"][10]["step_id"] = 3
        del document["plan"]["steps"][2]["deps"]

        self.assertEqual(
            problems(PLAN_DOCUMENT, document),
            [
                ("plan.steps[2].deps", MISSING_FIELD),
                ("plan.steps[10].step_id", WRONG_TYPE),
                ("run_token", WRONG_TYPE),
                ("status", NOT_IN_VOCABULARY),
                ("ticket", UNEXPECTED_FIELD),
            ],
        )

    def test_the_error_describes_every_problem(self):
        document = plan_document(ticket="OPS-1", run_token=5.0)

        with self.assertRaises(ContractError) as caught:
            validate(PLAN_DOCUMENT, document)

        error = caught.exception
        self.assertEqual(error.kind, PLAN_DOCUMENT)
        self.assertIsInstance(error.problems, tuple)
        self.assertEqual(
            [problem.message for problem in error.problems],
            [
                "run_token 5.0 is a number, not an integer",
                "ticket is not a contract field, and its name matches none of ['^x_']",
            ],
        )
        for problem in error.problems:
            self.assertIn(f"[{problem.code}] {problem.message}", str(error))
        self.assertIn("2 problems", str(error))

    def test_a_value_failing_several_rules_is_one_problem(self):
        # A seven-digit fraction breaks the pattern and the length.
        document = plan_document(created_at="2026-10-22T10:00:00.1234567Z")
        self.assertEqual(
            problems(PLAN_DOCUMENT, document), [("created_at", BAD_TIMESTAMP)]
        )

    def test_a_bad_timestamp_says_what_the_canonical_spelling_is(self):
        for value, detail in (
            (
                "2026-10-22T09:30:00+00:00",
                "is not a timestamp spelled canonically: in UTC, ending in Z, to "
                "the whole second or with a fraction of 1 to 6 digits not ending "
                "in 0, as in 2026-10-22T10:00:00Z or 2026-10-22T10:00:00.25Z",
            ),
            ("2026-02-30T00:00:00.5Z", "is not a date and time that exists"),
        ):
            with self.subTest(value=value):
                with self.assertRaises(ContractError) as caught:
                    validate(PLAN_DOCUMENT, plan_document(updated_at=value))
                (problem,) = caught.exception.problems
                self.assertEqual(problem.message, f"updated_at '{value}' {detail}")

    def test_keys_that_are_not_plain_names_are_quoted(self):
        document = plan_document(**{"my-field": 1})
        self.assertEqual(
            problems(PLAN_DOCUMENT, document), [('["my-field"]', UNEXPECTED_FIELD)]
        )

    def test_recorded_values_are_shortened(self):
        for value in (
            "x" * 10_000,
            [list(range(1000))] * 100,
            {str(i): i for i in range(1000)},
        ):
            with self.subTest(value=type(value).__name__):
                with self.assertRaises(ContractError) as caught:
                    validate(PLAN_DOCUMENT, plan_document(run_token=value))
                (problem,) = caught.exception.problems
                self.assertLess(len(problem.message), 200)


class TestTimestamps(SpoolTestCase):
    """Every contract timestamp is canonical and real; a step's ts is not one."""

    def snapshot_of_a_finished_attempt(self) -> dict[str, Any]:
        """A stored snapshot of an attempt the engine ran to its end."""
        self.finished(self.plan(), lambda ctx: None)
        snapshot = stored_snapshot(self.snapshot())
        assert snapshot is not None
        return snapshot

    def contract_timestamps(self) -> list[tuple[str, dict[str, Any], str]]:
        """Each contract timestamp: its kind, a document carrying it, its path."""
        finalized = finalized_document()
        record = "last_finalized_execution"
        snapshot = self.snapshot_of_a_finished_attempt()
        return [
            (PLAN_DOCUMENT, finalized, path)
            for path in (
                "created_at",
                "updated_at",
                "last_executed_at",
                "approved_at",
                "run_requested_at",
                f"{record}.finalized_at",
                f"{record}.report.started_at",
                f"{record}.report.ended_at",
            )
        ] + [
            (ATTEMPT_REPORT, report(), "started_at"),
            (ATTEMPT_REPORT, report(), "ended_at"),
            (PLAN_STATUS, snapshot, "updated_at"),
            (PLAN_STATUS, snapshot, "attempt.started_at"),
            (PLAN_STATUS, snapshot, "attempt.ended_at"),
        ]

    def test_the_documents_conform(self):
        for kind, document, path in self.contract_timestamps():
            with self.subTest(kind=kind, path=path):
                self.assertEqual(problems(kind, document), [])

    def test_a_canonical_fraction_conforms(self):
        for kind, document, path in self.contract_timestamps():
            for value in FRACTIONAL_TIMESTAMPS:
                with self.subTest(kind=kind, path=path, value=value):
                    self.assertEqual(problems(kind, changed(document, path, value)), [])

    def test_a_noncanonical_timestamp_is_one_bad_timestamp(self):
        for kind, document, path in self.contract_timestamps():
            for value in NONCANONICAL_TIMESTAMPS:
                with self.subTest(kind=kind, path=path, value=value):
                    self.assertEqual(
                        problems(kind, changed(document, path, value)),
                        [(path, BAD_TIMESTAMP)],
                    )

    def test_a_timestamp_that_is_not_text_is_the_wrong_type(self):
        for kind, document, path in self.contract_timestamps():
            with self.subTest(kind=kind, path=path):
                self.assertEqual(
                    problems(kind, changed(document, path, 1761127200)),
                    [(path, WRONG_TYPE)],
                )

    def test_a_steps_ts_is_kept_as_published(self):
        snapshot = self.snapshot_of_a_finished_attempt()
        for value in ("yesterday", "2026-10-22T10:00:00+00:00", 1761127200, None):
            with self.subTest(value=value):
                self.assertEqual(
                    problems(PLAN_STATUS, changed(snapshot, "steps.a.ts", value)), []
                )


class TestPublishedSchemas(unittest.TestCase):
    """The canonical spelling is in the artifacts, for any validator to apply."""

    def carriers(self) -> list[tuple[str, dict[str, Any], str]]:
        """One conforming document of each kind, and a timestamp's path in it."""
        return [
            (PLAN_DOCUMENT, plan_document(), "created_at"),
            (ATTEMPT_REPORT, report(), "started_at"),
            (PLAN_STATUS, projection_snapshot(None, {}), "updated_at"),
        ]

    def test_a_plain_validator_rejects_the_legacy_spelling_and_a_newline(self):
        document = plan_document()
        validator = plain_validator(PLAN_DOCUMENT)
        self.assertTrue(validator.is_valid(document))
        for value in ("2026-10-22T10:00:00+00:00", "2026-10-22T10:00:00Z\n"):
            with self.subTest(value=value):
                self.assertFalse(validator.is_valid({**document, "created_at": value}))

    def test_a_plain_validator_applies_the_canonical_spelling(self):
        for kind, document, path in self.carriers():
            validator = plain_validator(kind)
            self.assertTrue(validator.is_valid(document))
            for value in FRACTIONAL_TIMESTAMPS:
                with self.subTest(kind=kind, value=value):
                    self.assertTrue(validator.is_valid(changed(document, path, value)))
            for value in (
                "2026-10-22T10:00:00.50Z",
                "2026-10-22T10:00:00.000000Z",
                "2026-10-22T10:00:00.1234567Z",
                "2026-10-22T10:00:00.5+00:00",
            ):
                with self.subTest(kind=kind, value=value):
                    self.assertFalse(validator.is_valid(changed(document, path, value)))

    def test_the_pattern_itself_stops_a_final_newline(self):
        # With $ at its end, the pattern would let each value through: in
        # Python, $ also matches just before a final newline.
        dollar_ended = CANONICAL_PATTERN.removesuffix("(?![\\s\\S])") + "$"
        for kind, document, path in self.carriers():
            validator = plain_validator(kind)
            for value in FINAL_NEWLINES:
                with self.subTest(kind=kind, value=value):
                    self.assertLessEqual(len(value), CANONICAL_MAX_LENGTH)
                    self.assertIsNotNone(re.search(dollar_ended, value))
                    self.assertIsNone(re.search(CANONICAL_PATTERN, value))
                    errors = validator.iter_errors(changed(document, path, value))
                    self.assertEqual([error.validator for error in errors], ["pattern"])
                    self.assertEqual(
                        problems(kind, changed(document, path, value)),
                        [(path, BAD_TIMESTAMP)],
                    )

    def test_whether_a_date_exists_is_the_format_check(self):
        document = plan_document(created_at="2026-02-30T00:00:00Z")
        # A stock validator does not assert formats; validate does.
        self.assertTrue(plain_validator(PLAN_DOCUMENT).is_valid(document))
        self.assertEqual(
            problems(PLAN_DOCUMENT, document), [("created_at", BAD_TIMESTAMP)]
        )

    def test_each_resource_defines_the_timestamp_once(self):
        for kind in KINDS:
            with self.subTest(kind=kind):
                schema = schema_for(kind, 1)
                self.assertEqual(
                    [at for at, _ in keyword_locations(schema, "format")],
                    ["/$defs/timestamp"],
                )
                self.assertEqual(
                    schema["$defs"]["timestamp"],
                    {
                        "description": schema["$defs"]["timestamp"]["description"],
                        "type": "string",
                        "format": "date-time",
                        "pattern": CANONICAL_PATTERN,
                        "maxLength": CANONICAL_MAX_LENGTH,
                    },
                )
                self.assertEqual(
                    len("2026-10-22T10:00:00.123456Z"), CANONICAL_MAX_LENGTH
                )

    def test_the_contract_timestamps_use_the_definition(self):
        expected = {
            PLAN_DOCUMENT: {
                "/properties/created_at",
                "/properties/updated_at",
                "/properties/last_executed_at",
                "/properties/approved_at",
                "/properties/run_requested_at",
                "/$defs/last_finalized_execution/properties/finalized_at",
            },
            ATTEMPT_REPORT: {"/properties/started_at", "/properties/ended_at"},
            PLAN_STATUS: {
                "/properties/updated_at",
                "/$defs/attempt/properties/started_at/anyOf/1",
                "/$defs/attempt/properties/ended_at/anyOf/1",
            },
        }
        for kind in KINDS:
            with self.subTest(kind=kind):
                users = {
                    at
                    for at, ref in keyword_locations(schema_for(kind, 1), "$ref")
                    if ref == "#/$defs/timestamp"
                }
                self.assertEqual(users, expected[kind])

    def test_patterns_use_ascii_digits(self):
        for kind in KINDS:
            for at, pattern in keyword_locations(schema_for(kind, 1), "pattern"):
                with self.subTest(kind=kind, at=at):
                    self.assertNotIn("\\d", pattern)


class TestVocabularies(unittest.TestCase):
    """Every vocabulary in the schemas is the one the code writes."""

    def vocabulary(self, kind: str, name: str) -> set[Any]:
        """The values of one ``$defs`` enum."""
        return set(schema_for(kind, 1)["$defs"][name]["enum"])

    def test_plan_document_vocabularies(self):
        statuses = {
            build_plan_document(make_plan(), "r", dict(SCOPE), auto_run=auto)["status"]
            for auto in (True, False)
        }
        self.assertEqual(self.vocabulary(PLAN_DOCUMENT, "status"), statuses)
        self.assertEqual(
            self.vocabulary(PLAN_DOCUMENT, "execution_authority"),
            set(VALID_EXECUTION_AUTHORITIES),
        )
        self.assertEqual(
            self.vocabulary(PLAN_DOCUMENT, "failure_policy"),
            set(VALID_FAILURE_POLICIES),
        )
        self.assertEqual(set(FAILURE_POLICIES), set(VALID_FAILURE_POLICIES))

    def test_report_vocabularies(self):
        for name, values in (
            ("outcome", ExecutionOutcome),
            ("termination_reason", TerminationReason),
            ("step_outcome", StepOutcome),
        ):
            with self.subTest(name=name):
                self.assertEqual(
                    self.vocabulary(ATTEMPT_REPORT, name), {v.value for v in values}
                )

    def test_snapshot_vocabularies(self):
        for name, values in (
            ("projection", {PROJECTION_ATTEMPT, PROJECTION_LEGACY}),
            ("attempt_state", {ATTEMPT_RUNNING, ATTEMPT_FINISHED}),
            ("execution_authority", set(VALID_EXECUTION_AUTHORITIES)),
            (
                "step_state",
                {STATE_PENDING, STATE_INTERRUPTED, STATE_UNREACHED, STATE_UNKNOWN},
            ),
        ):
            with self.subTest(name=name):
                self.assertEqual(self.vocabulary(PLAN_STATUS, name), values)

    def test_counts_are_the_step_outcomes_and_unreached(self):
        counts = schema_for(ATTEMPT_REPORT, 1)["$defs"]["counts"]
        expected = {outcome.value for outcome in StepOutcome} | {"unreached"}
        self.assertEqual(set(counts["properties"]), expected)
        self.assertEqual(set(counts["required"]), expected)
        self.assertEqual(set(AttemptReport("e", "p").counts), expected)

    def test_every_vocabulary_is_one_of_these(self):
        expected = {
            PLAN_DOCUMENT: {
                "/$defs/status",
                "/$defs/execution_authority",
                "/$defs/failure_policy",
            },
            PLAN_STATUS: {
                "/$defs/projection",
                "/$defs/attempt_state",
                "/$defs/execution_authority",
                "/$defs/step_state",
            },
            ATTEMPT_REPORT: {
                "/$defs/outcome",
                "/$defs/termination_reason",
                "/$defs/step_outcome",
            },
        }
        for kind in KINDS:
            with self.subTest(kind=kind):
                locations = {
                    at for at, _ in keyword_locations(schema_for(kind, 1), "enum")
                }
                self.assertEqual(locations, expected[kind])

    def test_conditions_name_vocabulary_values(self):
        consts = list(keyword_locations(schema_for(PLAN_STATUS, 1), "const"))
        self.assertEqual(
            {value for at, value in consts if at.endswith("/properties/projection")},
            {PROJECTION_LEGACY, PROJECTION_ATTEMPT},
        )
        self.assertEqual(
            {value for at, value in consts if at.endswith("/properties/state")},
            {ATTEMPT_RUNNING},
        )

    def test_markers_name_this_releases_families(self):
        for kind in (PLAN_DOCUMENT, PLAN_STATUS):
            with self.subTest(kind=kind):
                marker = schema_for(kind, 1)["properties"]["schema_version"]
                self.assertEqual(marker["const"], CURRENT_FAMILIES[kind])
        self.assertEqual(
            schema_for(PLAN_STATUS, 1)["properties"]["type"], {"const": PLAN_STATUS}
        )


if __name__ == "__main__":
    unittest.main()
