"""Reading a snapshot: presence, version first, identity, structure, then each field.

Every input is a regression-corpus snapshot with at most a few fields
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
    snapshot,
    snapshot_names,
)
from tests.contract.support import ABSENT, changed
from yggdrasil.contract.diagnostics import MAX_LENGTH
from yggdrasil.contract.fields import STEP_STATUSES
from yggdrasil.contract.read import (
    LOOKUP_IMPOSSIBLE,
    MISSING,
    PRESENT,
    UNAVAILABLE,
    ObservedStep,
    Recorded,
    SnapshotRead,
    StepRef,
    StepStatus,
    read_snapshot,
    step_status,
)
from yggdrasil.contract.versions import version_sentence

# What a usable read derives, and an unusable one leaves None.
DERIVED = ("projection", "written_at", "attempt", "steps")

# The longest problem message: a path and at most two recorded values, each
# shown bounded, and fixed words.
MAX_MESSAGE = 3 * MAX_LENGTH + 200


def read(document: Any, **identity: str) -> SnapshotRead:
    """Read a snapshot for the plan and realm it names, unless others are given."""
    plan_id = identity["plan_id"] if "plan_id" in identity else document["plan_id"]
    realm = identity["realm"] if "realm" in identity else document["realm"]
    return read_snapshot(document, plan_id=plan_id, realm=realm)


def paths_and_codes(read_: SnapshotRead) -> list[tuple[str, str]]:
    """Return each problem of a read as (path, code), in reported order."""
    return [(problem.path, problem.code) for problem in read_.problems]


def steps_of(read_: SnapshotRead) -> dict[str, ObservedStep]:
    """Return a read's observed steps by step ID."""
    assert read_.steps is not None
    return {step.step_id: step for step in read_.steps}


class TestPresence(ReaderTestCase):
    """A body, or what the caller passes for a snapshot it could not provide."""

    def test_a_snapshot_the_store_could_not_provide(self):
        for sentinel in (MISSING, UNAVAILABLE):
            with self.subTest(sentinel.value):
                result = read_snapshot(sentinel, plan_id="pln_lifecycle", realm="r")

                self.assertEqual(result.presence, sentinel.value)
                self.assertFalse(result.usable)
                self.assertIsNone(result.version)
                self.assertIsNone(result.document)
                self.assertEqual(result.problems, ())
                self.assertEqual((result.plan_id, result.realm), ("pln_lifecycle", "r"))
                for name in DERIVED:
                    self.assertIsNone(getattr(result, name), name)

    def test_a_body_is_present(self):
        self.assertEqual(read(snapshot("running")).presence, PRESENT)

    def test_a_snapshot_that_could_not_be_looked_up_has_no_body_to_read(self):
        with self.assertRaises(ValueError):
            read_snapshot(LOOKUP_IMPOSSIBLE, plan_id="pln_lifecycle", realm="r")

    def test_the_plan_and_realm_are_non_empty_strings(self):
        """They come from the caller's code, so a wrong one is a programming error."""
        body = snapshot("running")
        for argument in ("plan_id", "realm"):
            for value, error in ((None, TypeError), (5, TypeError), ("", ValueError)):
                with self.subTest(argument=argument, value=repr(value)):
                    identity: dict[str, Any] = {
                        "plan_id": "pln_lifecycle",
                        "realm": "test_realm",
                        argument: value,
                    }
                    with self.assertRaises(error):
                        read_snapshot(body, **identity)


class TestVersionFirst(ReaderTestCase):
    """Only a supported family is read; anything else is inspect-only."""

    CASES = (
        ("absent", ABSENT, "unversioned", "unversioned"),
        ("text", "1", "invalid", "invalid_version"),
        ("float", 1.0, "invalid", "invalid_version"),
        ("true", True, "invalid", "invalid_version"),
        ("null", None, "invalid", "invalid_version"),
        ("zero", 0, "invalid", "invalid_version"),
        ("family 2", 2, "unsupported", "unsupported_version"),
    )

    def test_each_version_state_is_inspect_only(self):
        for name, value, state, code in self.CASES:
            with self.subTest(name):
                document = changed(
                    snapshot("finished_success"), "schema_version", value
                )

                result = read(document)

                assert result.version is not None
                self.assertEqual(result.version.state, state)
                self.assertFalse(result.usable)
                self.assertIs(result.document, document)
                self.assertEqual(paths_and_codes(result), [("schema_version", code)])
                self.assertIn(
                    version_sentence("plan_status", result.version),
                    result.problems[0].message,
                )
                self.assertIn("plan-status families: [1]", result.problems[0].message)
                for field in DERIVED:
                    self.assertIsNone(getattr(result, field), field)

    def test_the_corpus_snapshot_outside_the_family(self):
        result = read(snapshot("unversioned"))

        self.assertFalse(result.usable)
        self.assertEqual(paths_and_codes(result), [("schema_version", "unversioned")])

    def test_nothing_else_is_interpreted_in_an_inspect_only_snapshot(self):
        document = snapshot("unversioned")
        for path, value in (("plan_id", "other"), ("steps", []), ("projection", 5)):
            document = changed(document, path, value)

        result = read(document, plan_id="pln_lifecycle", realm="test_realm")

        self.assertEqual(paths_and_codes(result), [("schema_version", "unversioned")])

    def test_a_body_that_is_not_an_object_is_invalid(self):
        for name, body in HOSTILE:
            if isinstance(body, dict):
                continue
            with self.subTest(name):
                result = read_snapshot(body, plan_id="pln_lifecycle", realm="r")

                self.assertFalse(result.usable)
                self.assertIs(result.document, body)
                self.assertEqual(paths_and_codes(result), [("", "wrong_type")])
                self.assertIn("the snapshot", result.problems[0].message)
                for field in DERIVED:
                    self.assertIsNone(getattr(result, field), field)


class TestIdentity(ReaderTestCase):
    """A snapshot of another plan or realm is not this plan's, whatever it holds."""

    def test_another_plan_or_realm_is_invalid_as_a_whole(self):
        for field in ("plan_id", "realm"):
            for name, value in (
                ("another", "other"),
                ("empty", ""),
                ("null", None),
                ("a number", 5),
                ("absent", ABSENT),
            ):
                with self.subTest(field=field, value=name):
                    document = changed(snapshot("running"), field, value)

                    result = read(document, plan_id="pln_lifecycle", realm="test_realm")

                    self.assertFalse(result.usable)
                    self.assertEqual(
                        paths_and_codes(result), [(field, "identity_mismatch")]
                    )
                    for derived in DERIVED:
                        self.assertIsNone(getattr(result, derived), derived)

    def test_identity_is_typed(self):
        """The text ``"5"`` names no plan stored as the number 5, nor the reverse."""
        document = changed(snapshot("running"), "plan_id", 5)

        result = read(document, plan_id="5", realm="test_realm")

        self.assertEqual(paths_and_codes(result), [("plan_id", "identity_mismatch")])

    def test_the_mismatch_names_both_values(self):
        result = read(snapshot("running"), plan_id="pln_other", realm="other_realm")

        self.assertEqual(
            paths_and_codes(result),
            [("plan_id", "identity_mismatch"), ("realm", "identity_mismatch")],
        )
        self.assertIn("'pln_lifecycle'", result.problems[0].message)
        self.assertIn("'pln_other'", result.problems[0].message)
        self.assertIn("'other_realm'", result.problems[1].message)

    def test_identity_is_judged_before_structure(self):
        document = changed(snapshot("running"), "steps", [])

        result = read(document, plan_id="pln_other", realm="test_realm")

        self.assertEqual(paths_and_codes(result), [("plan_id", "identity_mismatch")])


class TestStructure(ReaderTestCase):
    """A snapshot not structured as one is invalid as a whole."""

    def assert_invalid(self, document: Any, expected: list[tuple[str, str]]) -> None:
        """Assert a snapshot is invalid as a whole, with exactly these problems."""
        result = read(document)
        self.assertFalse(result.usable)
        self.assertEqual(paths_and_codes(result), expected)
        for name in DERIVED:
            self.assertIsNone(getattr(result, name), name)

    def test_an_attempt_projection_needs_an_attempt_with_an_id(self):
        cases = (
            ("attempt absent", "attempt", ABSENT, "attempt"),
            ("attempt null", "attempt", None, "attempt"),
            ("attempt text", "attempt", "20261022T100000000000Z_0001", "attempt"),
            ("attempt a list", "attempt", [], "attempt"),
            ("ID absent", "attempt.execution_id", ABSENT, "attempt.execution_id"),
            ("ID empty", "attempt.execution_id", "", "attempt.execution_id"),
            ("ID null", "attempt.execution_id", None, "attempt.execution_id"),
            ("ID a number", "attempt.execution_id", 5, "attempt.execution_id"),
        )
        for name, path, value, at in cases:
            with self.subTest(name):
                self.assert_invalid(
                    changed(snapshot("finished_success"), path, value),
                    [(at, "invalid_structure")],
                )

    def test_a_legacy_projection_carries_no_attempt(self):
        for name, document in (
            (
                "attempt kept",
                changed(snapshot("finished_success"), "projection", "legacy"),
            ),
            ("attempt text", changed(snapshot("legacy"), "attempt", "x")),
        ):
            with self.subTest(name):
                self.assert_invalid(document, [("attempt", "invalid_structure")])

    def test_a_legacy_projection_without_the_attempt_key_is_read(self):
        result = read(changed(snapshot("legacy"), "attempt", ABSENT))

        self.assertTrue(result.usable)
        self.assertEqual(result.problems, ())

    def test_the_projection_is_one_of_two_words(self):
        for name, value in (
            ("unrecognised", "materialised"),
            ("absent", ABSENT),
            ("null", None),
            ("a number", 1),
            ("a list", ["attempt"]),
        ):
            with self.subTest(name):
                self.assert_invalid(
                    changed(snapshot("finished_success"), "projection", value),
                    [("projection", "invalid_structure")],
                )

    def test_the_steps_are_an_object_of_step_entries(self):
        running = snapshot("running")
        cases = (
            ("steps absent", changed(running, "steps", ABSENT), "steps"),
            (
                "steps a list",
                changed(running, "steps", list(running["steps"].values())),
                "steps",
            ),
            ("steps null", changed(running, "steps", None), "steps"),
            ("an entry is text", changed(running, "steps.qc", "pending"), "steps.qc"),
            ("an entry is null", changed(running, "steps.qc", None), "steps.qc"),
        )
        for name, document, at in cases:
            with self.subTest(name):
                self.assert_invalid(document, [(at, "invalid_structure")])

    def test_a_step_id_is_not_empty(self):
        document = snapshot("running")
        document["steps"][""] = document["steps"]["qc"]

        self.assert_invalid(document, [('steps[""]', "invalid_structure")])

    def test_the_blocker_lists_are_on_every_entry(self):
        """Both lists are required on every step entry, whatever its status.

        The family-1 schema requires both, and every writer writes both, so
        an absent list is the same structural fault as one of the wrong type.
        """
        for step_id, status in (
            ("lane1_upload", "blocked"),
            ("lane2_demux", "succeeded"),
        ):
            for field in ("direct_blockers", "failed_ancestors"):
                with self.subTest(step=status, field=field):
                    document = changed(
                        snapshot("finished_with_failures"),
                        f"steps.{step_id}.{field}",
                        ABSENT,
                    )

                    self.assert_invalid(
                        document, [(f"steps.{step_id}.{field}", "invalid_structure")]
                    )

    def test_a_blocker_list_holds_step_ids(self):
        path = "steps.lane1_upload.direct_blockers"
        for name, value, at in (
            ("text", "lane1_demux", path),
            ("null", None, path),
            ("an object", {}, path),
            ("an empty ID", ["lane1_demux", ""], f"{path}[1]"),
            ("a number", [4], f"{path}[0]"),
            ("null in it", [None], f"{path}[0]"),
        ):
            with self.subTest(name):
                self.assert_invalid(
                    changed(snapshot("finished_with_failures"), path, value),
                    [(at, "invalid_structure")],
                )

    def test_an_empty_blocker_list_is_valid(self):
        document = changed(
            snapshot("finished_with_failures"), "steps.lane1_upload.direct_blockers", []
        )

        result = read(document)

        self.assertEqual(result.problems, ())
        self.assertEqual(steps_of(result)["lane1_upload"].direct_blockers, ())

    def test_every_fault_is_reported(self):
        document = snapshot("finished_with_failures")
        document["projection"] = "materialised"
        document["steps"][""] = {}
        document["steps"]["lane2_demux"]["failed_ancestors"] = "lane1_demux"

        self.assert_invalid(
            document,
            [
                ("projection", "invalid_structure"),
                ("steps.lane2_demux.failed_ancestors", "invalid_structure"),
                ('steps[""]', "invalid_structure"),
                ('steps[""].direct_blockers', "invalid_structure"),
                ('steps[""].failed_ancestors', "invalid_structure"),
            ],
        )


class TestStepStatus(ReaderTestCase):
    """A step's status from its recorded state and outcome, row by row."""

    def test_each_row(self):
        rows = (
            # A recognised outcome beside the event that records it.
            ("step.succeeded", "succeeded", "succeeded", False),
            ("step.skipped", "reused", "reused", False),
            ("step.failed", "failed", "failed", False),
            ("step.blocked", "blocked", "blocked", False),
            # A recognised outcome beside anything else.
            ("step.started", "succeeded", "unknown", True),
            ("pending", "failed", "unknown", True),
            (None, "blocked", "unknown", True),
            ([], "succeeded", "unknown", True),
            # An outcome present but unrecognised, of any type.
            ("step.succeeded", "skipped", "unknown", False),
            ("step.succeeded", [], "unknown", False),
            ("step.succeeded", 1, "unknown", False),
            # No outcome, in a state without an event.
            ("pending", None, "pending", False),
            ("interrupted", None, "interrupted", False),
            ("unreached", None, "unreached", False),
            ("unknown", None, "unknown", False),
            # No outcome beside a terminal event.
            ("step.succeeded", None, "unknown", True),
            ("step.skipped", None, "unknown", True),
            ("step.failed", None, "unknown", True),
            ("step.blocked", None, "unknown", True),
            # No outcome beside another step event, a realm's own included.
            ("step.started", None, "started", False),
            ("step.progress", None, "started", False),
            ("step.checkpoint_written", None, "started", False),
            # No outcome, in anything else.
            ("queued", None, "unknown", False),
            ("", None, "unknown", False),
            (None, None, "unknown", False),
            (7, None, "unknown", False),
            ({"type": "step.started"}, None, "unknown", False),
        )
        for index, (state, outcome, status, contradicts) in enumerate(rows):
            with self.subTest(row=index):
                result = step_status(state, outcome)

                self.assertEqual(result.status, status)
                self.assertEqual(result.contradiction is not None, contradicts)
                self.assertIn(result.status, STEP_STATUSES)

    def test_a_contradiction_names_both_recorded_values(self):
        result = step_status("step.progress", "succeeded")
        assert result.contradiction is not None
        self.assertIn("'succeeded'", result.contradiction)
        self.assertIn("'step.progress'", result.contradiction)

        result = step_status("step.failed", None)
        assert result.contradiction is not None
        self.assertIn("'step.failed'", result.contradiction)
        self.assertIn("null", result.contradiction)

    def test_no_value_of_either_makes_it_raise(self):
        for state_name, state in HOSTILE:
            for outcome_name, outcome in HOSTILE:
                with self.subTest(state=state_name, outcome=outcome_name):
                    self.assertIsInstance(step_status(state, outcome), StepStatus)

    def test_a_contradiction_is_reported_on_the_step(self):
        document = changed(snapshot("running"), "steps.demux.outcome", "succeeded")

        result = read(document)

        step = steps_of(result)["demux"]
        self.assertEqual(step.status, "unknown")
        self.assertIsNotNone(step.contradiction)
        self.assertEqual(
            paths_and_codes(result), [("steps.demux", "step_record_contradiction")]
        )
        self.assertTrue(result.usable)

    def test_a_state_or_outcome_of_the_wrong_type_costs_that_step(self):
        for field, value, expected in (
            ("state", [], ("steps.qc.state", "wrong_type")),
            ("state", None, ("steps.qc.state", "wrong_type")),
            ("state", ABSENT, ("steps.qc.state", "missing_field")),
            ("outcome", 5, ("steps.qc.outcome", "wrong_type")),
            ("outcome", ABSENT, ("steps.qc.outcome", "missing_field")),
        ):
            with self.subTest(field=field, value=repr(value)):
                result = read(changed(snapshot("running"), f"steps.qc.{field}", value))

                steps = steps_of(result)
                self.assertEqual(paths_and_codes(result), [expected])
                self.assertEqual(steps["demux"].status, "started")
                self.assertIn(steps["qc"].status, ("unknown", "pending"))

    def test_an_absent_outcome_is_no_outcome(self):
        result = read(changed(snapshot("running"), "steps.qc.outcome", ABSENT))

        self.assertEqual(steps_of(result)["qc"].status, "pending")


class TestSettled(ReaderTestCase):
    """An attempt's results are final once its state is ``finished``."""

    def test_settled_follows_the_state_alone(self):
        for name, document, settled in (
            ("finished", snapshot("finished_success"), True),
            ("running", snapshot("running"), False),
            (
                "unknown word",
                changed(snapshot("finished_success"), "attempt.state", "paused"),
                None,
            ),
            (
                "not text",
                changed(snapshot("finished_success"), "attempt.state", 1),
                None,
            ),
        ):
            with self.subTest(name):
                result = read(document)

                assert result.attempt is not None
                self.assertIs(result.attempt.settled, settled)

    def test_an_unknown_state_is_kept_as_recorded(self):
        result = read(changed(snapshot("finished_success"), "attempt.state", "paused"))

        assert result.attempt is not None
        self.assertEqual(result.attempt.state, Recorded(None, "paused", True))
        self.assertEqual(result.problems, ())

    def test_a_state_that_is_not_text_is_the_wrong_type(self):
        for value in (None, 1, ["finished"]):
            with self.subTest(repr(value)):
                result = read(
                    changed(snapshot("finished_success"), "attempt.state", value)
                )

                self.assertEqual(
                    paths_and_codes(result), [("attempt.state", "wrong_type")]
                )


class TestAttempt(ReaderTestCase):
    """The attempt's fields: typed, ``null`` where the schema allows it."""

    NULLABLE = (
        "plan_generation",
        "run_token",
        "execution_authority",
        "execution_owner",
        "started_at",
        "ended_at",
        "termination_reason",
        "outcome",
        "is_drained",
        "counts",
        "diagnostic",
    )

    def test_a_finished_attempt_is_read(self):
        result = read(snapshot("finished_with_failures"))

        attempt = result.attempt
        assert attempt is not None
        at = datetime(2026, 10, 22, 10, 0, 0, tzinfo=UTC)
        self.assertEqual(attempt.execution_id, "20261022T100000000000Z_0001")
        self.assertEqual(
            attempt.plan_generation.value, "6f41ef9b55dd15f40e1b25f536e6661b"
        )
        self.assertEqual(attempt.run_token.value, 0)
        self.assertEqual(attempt.failure_policy.value, "continue_independent")
        self.assertEqual(attempt.execution_authority.value, "daemon")
        self.assertEqual(attempt.execution_owner, Recorded(None, None, True))
        self.assertEqual(attempt.state.value, "finished")
        self.assertEqual((attempt.started_at.value, attempt.ended_at.value), (at, at))
        self.assertEqual(attempt.termination_reason.value, "completed")
        self.assertEqual(attempt.outcome.value, "failed")
        self.assertIs(attempt.is_drained.value, True)
        assert attempt.counts is not None
        self.assertEqual((attempt.counts.failed, attempt.counts.blocked), (1, 1))
        self.assertIsNone(attempt.diagnostic)
        self.assertIs(attempt.recorded, result.document["attempt"])  # type: ignore[index]

    def test_null_is_no_value_and_no_problem(self):
        for field in self.NULLABLE:
            with self.subTest(field):
                result = read(
                    changed(snapshot("finished_success"), f"attempt.{field}", None)
                )

                attempt = result.attempt
                assert attempt is not None
                self.assertEqual(result.problems, ())
                held = getattr(attempt, field)
                if isinstance(held, Recorded):
                    self.assertEqual(held, Recorded(None, None, True))
                else:
                    self.assertIsNone(held)

    def test_a_value_of_the_wrong_type_is_one_problem(self):
        cases = (
            ("plan_generation", "", "wrong_type"),
            ("plan_generation", 5, "wrong_type"),
            ("run_token", "0", "wrong_type"),
            ("run_token", True, "wrong_type"),
            ("run_token", 0.5, "wrong_type"),
            ("run_token", -1, "out_of_range"),
            ("run_token", 2**53, "out_of_range"),
            ("execution_authority", 5, "wrong_type"),
            ("started_at", "yesterday", "bad_timestamp"),
            ("ended_at", 1759392000, "bad_timestamp"),
            ("termination_reason", ["completed"], "wrong_type"),
            ("outcome", {}, "wrong_type"),
            ("is_drained", 1, "wrong_type"),
            ("is_drained", "true", "wrong_type"),
            ("counts", "2", "wrong_type"),
            ("counts.failed", -1, "out_of_range"),
            ("diagnostic", "x", "wrong_type"),
        )
        for field, value, code in cases:
            with self.subTest(field=field, value=repr(value)):
                result = read(
                    changed(snapshot("finished_success"), f"attempt.{field}", value)
                )

                at = (
                    "attempt.counts"
                    if field.startswith("counts")
                    else f"attempt.{field}"
                )
                self.assertEqual(paths_and_codes(result), [(at, code)])
                assert result.attempt is not None
                self.assertEqual(result.attempt.state.value, "finished")
                self.assertTrue(result.usable)

    def test_an_absent_field_is_missing(self):
        fields = (
            *self.NULLABLE,
            "failure_policy",
            "state",
        )
        for field in fields:
            with self.subTest(field):
                result = read(
                    changed(snapshot("finished_success"), f"attempt.{field}", ABSENT)
                )

                self.assertEqual(
                    paths_and_codes(result), [(f"attempt.{field}", "missing_field")]
                )

    def test_typed_equality(self):
        """``true`` is not the token 1, and the text ``"5"`` is not the number 5."""
        result = read(changed(snapshot("finished_success"), "attempt.run_token", True))
        assert result.attempt is not None
        self.assertIsNone(result.attempt.run_token.value)

        result = read(changed(snapshot("finished_success"), "attempt.run_token", 5))
        assert result.attempt is not None
        self.assertEqual(result.attempt.run_token.value, 5)

        result = read(
            changed(snapshot("finished_success"), "attempt.plan_generation", 5)
        )
        assert result.attempt is not None
        self.assertIsNone(result.attempt.plan_generation.value)

    def test_the_owner_is_text_or_null(self):
        """Read as on the plan document: empty text and null name no owner."""
        cases = (
            (
                "an owner",
                "run_once:abc",
                Recorded("run_once:abc", "run_once:abc", True),
                [],
            ),
            ("empty text", "", Recorded(None, "", True), []),
            ("null", None, Recorded(None, None, True), []),
            (
                "absent",
                ABSENT,
                Recorded(None, None, False),
                [("attempt.execution_owner", "missing_field")],
            ),
            (
                "a number",
                5,
                Recorded(None, 5, True),
                [("attempt.execution_owner", "wrong_type")],
            ),
        )
        for name, value, expected, problems in cases:
            with self.subTest(name):
                result = read(
                    changed(
                        snapshot("finished_success"), "attempt.execution_owner", value
                    )
                )

                assert result.attempt is not None
                self.assertEqual(result.attempt.execution_owner, expected)
                self.assertEqual(paths_and_codes(result), problems)

    def test_the_policy_is_kept_as_the_plan_gave_it(self):
        """The attempt records the plan's policy as given: any value, read or unknown."""
        for name, value in (
            ("unrecognised", "retry_forever"),
            ("null", None),
            ("a number", 3),
        ):
            with self.subTest(name):
                result = read(
                    changed(
                        snapshot("finished_success"), "attempt.failure_policy", value
                    )
                )

                assert result.attempt is not None
                self.assertEqual(
                    result.attempt.failure_policy, Recorded(None, value, True)
                )
                self.assertEqual(result.problems, ())

    def test_unrecognised_words_are_unknown_with_their_text(self):
        for field in ("state", "execution_authority", "termination_reason", "outcome"):
            with self.subTest(field):
                result = read(
                    changed(snapshot("finished_success"), f"attempt.{field}", "novel")
                )

                assert result.attempt is not None
                self.assertEqual(
                    getattr(result.attempt, field), Recorded(None, "novel", True)
                )
                self.assertEqual(result.problems, ())

    def test_an_end_recorded_beside_a_running_state_is_returned_as_recorded(self):
        document = changed(snapshot("running"), "attempt.outcome", "failed")
        document = changed(document, "attempt.ended_at", "2026-10-22T10:05:00Z")

        result = read(document)

        attempt = result.attempt
        assert attempt is not None
        self.assertEqual(attempt.state.value, "running")
        self.assertIs(attempt.settled, False)
        self.assertEqual(attempt.outcome.value, "failed")
        self.assertEqual(
            attempt.ended_at.value, datetime(2026, 10, 22, 10, 5, tzinfo=UTC)
        )
        self.assertEqual(result.problems, ())


class TestDiagnostic(ReaderTestCase):
    """Why an attempt ended without a step to blame; ``null`` for none."""

    def test_null_is_no_diagnostic(self):
        for name in snapshot_names():
            document = snapshot(name)
            attempt = document.get("attempt")
            if not isinstance(attempt, dict) or attempt["diagnostic"] is not None:
                continue
            with self.subTest(name):
                result = read(document)

                if result.attempt is not None:
                    self.assertIsNone(result.attempt.diagnostic)
                self.assertNotIn(
                    "attempt.diagnostic", [p.path for p in result.problems]
                )

    def test_a_value_that_is_not_an_object_is_the_wrong_type(self):
        for name, value in (("text", "x"), ("a number", 5), ("a list", [])):
            with self.subTest(name):
                result = read(
                    changed(snapshot("cancelled"), "attempt.diagnostic", value)
                )

                attempt = result.attempt
                assert attempt is not None
                self.assertIsNone(attempt.diagnostic)
                self.assertEqual(
                    paths_and_codes(result), [("attempt.diagnostic", "wrong_type")]
                )
                self.assertEqual(attempt.termination_reason.value, "cancelled")
                self.assertTrue(result.usable)

    def test_its_fields_are_read(self):
        result = read(snapshot("cancelled"))

        assert result.attempt is not None and result.attempt.diagnostic is not None
        diagnostic = result.attempt.diagnostic
        # A bare interrupt carries its type and no message.
        self.assertEqual(
            (diagnostic.message, diagnostic.error_type), (None, "KeyboardInterrupt")
        )
        self.assertEqual(diagnostic.details, {"running_step_id": "qc"})

    def test_each_field_costs_only_itself(self):
        cases = (
            ("message", 5, "attempt.diagnostic.message", "wrong_type"),
            ("message", None, "attempt.diagnostic.message", "wrong_type"),
            ("message", ABSENT, "attempt.diagnostic.message", "missing_field"),
            ("error_type", 5, "attempt.diagnostic.error_type", "wrong_type"),
            ("error_type", ABSENT, "attempt.diagnostic.error_type", "missing_field"),
            ("details", "x", "attempt.diagnostic.details", "wrong_type"),
            ("details", ABSENT, "attempt.diagnostic.details", "missing_field"),
        )
        for field, value, at, code in cases:
            with self.subTest(field=field, value=repr(value)):
                result = read(
                    changed(snapshot("cancelled"), f"attempt.diagnostic.{field}", value)
                )

                assert result.attempt is not None
                diagnostic = result.attempt.diagnostic
                assert diagnostic is not None
                self.assertEqual(paths_and_codes(result), [(at, code)])
                if field != "error_type":
                    self.assertEqual(diagnostic.error_type, "KeyboardInterrupt")

    def test_a_null_error_type_is_valid(self):
        result = read(
            changed(snapshot("cancelled"), "attempt.diagnostic.error_type", None)
        )

        assert result.attempt is not None and result.attempt.diagnostic is not None
        self.assertIsNone(result.attempt.diagnostic.error_type)
        self.assertEqual(result.problems, ())


class TestStepTime(ReaderTestCase):
    """``ts``: the time of the step's latest event, read tolerantly."""

    def test_a_readable_time(self):
        for name, value, expected in (
            (
                "canonical",
                "2026-10-22T10:00:00Z",
                datetime(2026, 10, 22, 10, tzinfo=UTC),
            ),
            (
                "a fraction",
                "2026-10-22T10:00:00.25Z",
                datetime(2026, 10, 22, 10, 0, 0, 250000, tzinfo=UTC),
            ),
            # A realm may supply the time; any timezone-aware spelling is read.
            (
                "a realm's offset",
                "2026-10-22T12:00:00+02:00",
                datetime(2026, 10, 22, 10, tzinfo=UTC),
            ),
            (
                "a space",
                "2026-10-22 10:00:00+00:00",
                datetime(2026, 10, 22, 10, tzinfo=UTC),
            ),
        ):
            with self.subTest(name):
                result = read(changed(snapshot("running"), "steps.demux.ts", value))

                self.assertEqual(steps_of(result)["demux"].recorded_at, expected)
                self.assertEqual(result.problems, ())

    def test_null_is_no_time_and_no_problem(self):
        result = read(snapshot("running"))

        self.assertIsNone(steps_of(result)["qc"].recorded_at)
        self.assertEqual(result.problems, ())

    def test_an_absent_time_is_missing(self):
        result = read(changed(snapshot("running"), "steps.demux.ts", ABSENT))

        self.assertIsNone(steps_of(result)["demux"].recorded_at)
        self.assertEqual(paths_and_codes(result), [("steps.demux.ts", "missing_field")])

    def test_an_unreadable_time_costs_only_the_time(self):
        for name, value in (
            ("words", "yesterday"),
            ("no timezone", "2026-10-22T10:00:00"),
            ("a number", 1761127200),
            ("a list", ["2026-10-22T10:00:00Z"]),
            ("before year 1 in UTC", "0001-01-01T00:00:00+05:00"),
        ):
            with self.subTest(name):
                document = changed(snapshot("running"), "steps.demux.ts", value)

                result = read(document)

                step = steps_of(result)["demux"]
                self.assertIsNone(step.recorded_at)
                self.assertIs(step.recorded["ts"], document["steps"]["demux"]["ts"])
                self.assertEqual(
                    paths_and_codes(result), [("steps.demux.ts", "bad_timestamp")]
                )
                self.assertEqual(step.status, "started")
                self.assertEqual(step.progress_percent, 50)
                self.assertTrue(result.usable)


class TestProgress(ReaderTestCase):
    """Progress is a percentage only beside the event that reports it."""

    def test_progress_is_read_only_in_state_step_progress(self):
        result = read(snapshot("running"))

        steps = steps_of(result)
        self.assertEqual(steps["demux"].progress_percent, 50)
        self.assertIsNone(steps["qc"].progress_percent)
        finished = steps_of(read(snapshot("finished_success")))
        self.assertIsNone(finished["demux"].progress_percent)

    def test_progress_in_another_state_is_not_read_at_all(self):
        result = read(changed(snapshot("running"), "steps.qc.progress", "garbage"))

        self.assertIsNone(steps_of(result)["qc"].progress_percent)
        self.assertEqual(result.problems, ())

    def test_progress_within_range_is_taken_as_recorded(self):
        for value in (0, 100, 0.5, 99.9, 37):
            with self.subTest(repr(value)):
                result = read(
                    changed(snapshot("running"), "steps.demux.progress", value)
                )

                progress = steps_of(result)["demux"].progress_percent
                self.assertEqual(progress, value)
                self.assertIs(type(progress), type(value))
                self.assertEqual(result.problems, ())

    def test_unreadable_progress_is_unknown_and_reported(self):
        for name, value, code in (
            ("below", -1, "out_of_range"),
            ("above", 101, "out_of_range"),
            ("NaN", float("nan"), "out_of_range"),
            ("infinity", float("inf"), "out_of_range"),
            ("too large for a float", 10**400, "out_of_range"),
            ("text", "40", "wrong_type"),
            ("true", True, "wrong_type"),
            ("null", None, "wrong_type"),
            ("a list", [40], "wrong_type"),
            ("absent", ABSENT, "missing_field"),
        ):
            with self.subTest(name):
                result = read(
                    changed(snapshot("running"), "steps.demux.progress", value)
                )

                step = steps_of(result)["demux"]
                self.assertIsNone(step.progress_percent)
                self.assertEqual(
                    paths_and_codes(result), [("steps.demux.progress", code)]
                )
                self.assertEqual(step.status, "started")


class TestStepError(ReaderTestCase):
    """One reading of a step's error, from either of its stored shapes."""

    def test_the_event_shape_while_the_attempt_runs(self):
        error = steps_of(read(snapshot("running_with_failure")))["lane1_demux"].error

        assert error is not None
        self.assertEqual(error.message, "lane 1 is listed twice in the sample sheet")
        self.assertEqual(error.kind, Recorded("permanent", "permanent", True))
        self.assertEqual(error.code, "SAMPLESHEET_DUPLICATE_LANE")
        self.assertEqual(
            error.advice, "Correct the sample sheet, then regenerate the plan."
        )
        self.assertIsNone(error.error_type)

    def test_the_report_shape_once_the_attempt_has_ended(self):
        error = steps_of(read(snapshot("finished_with_failures")))["lane1_demux"].error

        assert error is not None
        self.assertEqual(error.message, "lane 1 is listed twice in the sample sheet")
        self.assertEqual(error.error_type, "PermanentStepError")

    def test_null_is_no_failure(self):
        steps = steps_of(read(snapshot("finished_with_failures")))

        self.assertIsNone(steps["lane2_demux"].error)

    def test_an_error_that_is_not_an_object_is_the_wrong_type(self):
        for name, value in (("text", "boom"), ("a list", []), ("a number", 1)):
            with self.subTest(name):
                result = read(
                    changed(
                        snapshot("finished_with_failures"),
                        "steps.lane1_demux.error",
                        value,
                    )
                )

                step = steps_of(result)["lane1_demux"]
                self.assertIsNone(step.error)
                self.assertEqual(
                    paths_and_codes(result), [("steps.lane1_demux.error", "wrong_type")]
                )
                self.assertEqual(step.status, "failed")

    def test_its_values_are_kept_as_the_step_gave_them(self):
        """The event's values are the step's own, so they are text or unknown."""
        path = "steps.lane1_demux.error"
        for field, value in (
            ("error", 7),
            ("code", 404),
            ("advice", ["retry"]),
            ("kind", 1),
        ):
            with self.subTest(field):
                result = read(
                    changed(snapshot("running_with_failure"), f"{path}.{field}", value)
                )

                self.assertEqual(result.problems, ())
                error = steps_of(result)["lane1_demux"].error
                assert error is not None
                read_back = {
                    "error": error.message,
                    "code": error.code,
                    "advice": error.advice,
                    "kind": error.kind.value,
                }
                self.assertIsNone(read_back[field])

    def test_an_unrecognised_kind_is_unknown_with_its_text(self):
        result = read(
            changed(
                snapshot("finished_with_failures"),
                "steps.lane1_demux.error.kind",
                "flaky",
            )
        )

        error = steps_of(result)["lane1_demux"].error
        assert error is not None
        self.assertEqual(error.kind, Recorded(None, "flaky", True))
        self.assertEqual(result.problems, ())

    def test_the_error_type_is_text_or_null(self):
        path = "steps.lane1_demux.error.error_type"
        result = read(changed(snapshot("finished_with_failures"), path, None))
        self.assertEqual(result.problems, ())

        result = read(changed(snapshot("finished_with_failures"), path, 5))
        self.assertEqual(paths_and_codes(result), [(path, "wrong_type")])

    def test_an_absent_field_is_missing(self):
        for field in ("error", "kind", "code", "advice"):
            with self.subTest(field):
                path = f"steps.lane1_demux.error.{field}"
                result = read(changed(snapshot("running_with_failure"), path, ABSENT))

                self.assertEqual(paths_and_codes(result), [(path, "missing_field")])
        result = read(
            changed(
                snapshot("finished_with_failures"), "steps.lane1_demux.error", ABSENT
            )
        )
        self.assertEqual(
            paths_and_codes(result), [("steps.lane1_demux.error", "missing_field")]
        )


class TestBlockers(ReaderTestCase):
    """Blockers mean something only for a blocked step."""

    def test_a_blocked_step_of_a_settled_attempt(self):
        step = steps_of(read(snapshot("finished_with_failures")))["lane1_upload"]

        self.assertEqual(step.direct_blockers, (StepRef("lane1_demux", True),))
        self.assertEqual(step.failed_upstream, (StepRef("lane1_demux", True),))

    def test_failed_upstream_is_not_established_until_settled(self):
        step = steps_of(read(snapshot("running_with_failure")))["lane1_upload"]

        self.assertEqual(step.direct_blockers, (StepRef("lane1_demux", True),))
        self.assertIsNone(step.failed_upstream)

    def test_an_attempt_in_an_unknown_state_establishes_none(self):
        document = changed(
            snapshot("finished_with_failures"), "attempt.state", "archived"
        )

        step = steps_of(read(document))["lane1_upload"]

        self.assertIsNone(step.failed_upstream)

    def test_a_blocker_outside_the_inventory_stays_visible(self):
        document = changed(
            snapshot("finished_with_failures"),
            "steps.lane1_upload.direct_blockers",
            ["lane9_demux"],
        )

        step = steps_of(read(document))["lane1_upload"]

        self.assertEqual(step.direct_blockers, (StepRef("lane9_demux", False),))

    def test_a_step_that_is_not_blocked_has_no_blockers(self):
        document = changed(
            snapshot("finished_with_failures"),
            "steps.lane2_demux.direct_blockers",
            ["x"],
        )

        step = steps_of(read(document))["lane2_demux"]

        self.assertIsNone(step.direct_blockers)
        self.assertIsNone(step.failed_upstream)


class TestStepFields(ReaderTestCase):
    """The step's name, and the fields passed through uninterpreted."""

    def test_the_name_is_text_or_none(self):
        for name, value, expected, problems in (
            ("text", "demux", "demux", []),
            ("empty", "", None, []),
            ("null", None, None, [("steps.demux.step_name", "wrong_type")]),
            ("a number", 5, None, [("steps.demux.step_name", "wrong_type")]),
            ("absent", ABSENT, None, [("steps.demux.step_name", "missing_field")]),
        ):
            with self.subTest(name):
                result = read(
                    changed(snapshot("running"), "steps.demux.step_name", value)
                )

                self.assertEqual(steps_of(result)["demux"].name, expected)
                self.assertEqual(paths_and_codes(result), problems)

    def test_the_raw_entry_is_passed_through(self):
        document = snapshot("finished_success")

        step = steps_of(read(document))["demux"]

        entry = document["steps"]["demux"]
        self.assertIs(step.recorded, entry)
        for field in ("run_id", "fingerprint", "job", "artifacts", "metrics"):
            self.assertIs(step.recorded[field], entry[field])
        self.assertEqual(
            (step.recorded_state, step.recorded_outcome),
            ("step.succeeded", "succeeded"),
        )

    def test_fields_the_reader_does_not_interpret_are_not_checked(self):
        document = snapshot("finished_success")
        for field in ("run_id", "fingerprint", "job", "artifacts", "metrics"):
            document = changed(document, f"steps.demux.{field}", float("nan"))
        document = changed(document, "type", "something else")
        document = changed(document, "scope", "P1")

        self.assertEqual(read(document).problems, ())

    def test_steps_keep_their_stored_order(self):
        result = read(snapshot("running_with_failure"))

        assert result.steps is not None
        self.assertEqual(
            [step.step_id for step in result.steps],
            ["lane1_demux", "lane1_upload", "lane2_demux", "lane2_upload"],
        )


class TestWrittenAt(ReaderTestCase):
    """``updated_at``: when the snapshot was written, not when anything ran."""

    def test_it_is_read(self):
        result = read(snapshot("running"))

        assert result.written_at is not None
        self.assertEqual(
            result.written_at.value, datetime(2026, 10, 22, 10, tzinfo=UTC)
        )

    def test_it_is_required_and_never_null(self):
        for name, value, code in (
            ("absent", ABSENT, "missing_field"),
            ("null", None, "bad_timestamp"),
            ("words", "now", "bad_timestamp"),
        ):
            with self.subTest(name):
                result = read(changed(snapshot("running"), "updated_at", value))

                self.assertEqual(paths_and_codes(result), [("updated_at", code)])
                self.assertTrue(result.usable)


class TestLegacy(ReaderTestCase):
    """A legacy projection: labelled step observations, and no attempt."""

    def test_its_steps_are_read_and_it_has_no_attempt(self):
        result = read(snapshot("legacy"))

        self.assertTrue(result.usable)
        self.assertEqual(result.projection, "legacy")
        self.assertIsNone(result.attempt)
        assert result.steps is not None
        self.assertEqual(
            [(step.step_id, step.status, step.name) for step in result.steps],
            [("demux", "succeeded", "demux"), ("qc", "unknown", None)],
        )

    def test_a_blocked_legacy_step_has_no_settled_upstream(self):
        document = snapshot("legacy")
        document["steps"]["qc"].update(
            state="step.blocked",
            outcome="blocked",
            direct_blockers=["demux"],
            failed_ancestors=["demux"],
        )

        step = steps_of(read(document))["qc"]

        self.assertEqual(step.direct_blockers, (StepRef("demux", True),))
        self.assertIsNone(step.failed_upstream)


class TestReaderProperties(ReaderTestCase):
    """Silent, immutable, bounded, and leaving the body untouched."""

    def test_every_corpus_snapshot_is_read_silently(self):
        with self.assert_silent():
            for name in snapshot_names():
                result = read(snapshot(name))
                for problem in result.problems:
                    self.assertLessEqual(len(problem.message), MAX_MESSAGE)

    def test_the_body_is_passed_through_untouched(self):
        for name in snapshot_names():
            with self.subTest(name):
                document = snapshot(name)
                before = copy.deepcopy(document)

                result = read(document)

                self.assertIs(result.document, document)
                self.assertEqual(document, before)

    def test_reading_twice_gives_the_same_answer(self):
        for name in snapshot_names():
            with self.subTest(name):
                self.assertEqual(read(snapshot(name)), read(snapshot(name)))

    def test_results_are_immutable_and_hold_tuples(self):
        result = read(snapshot("finished_with_failures"))

        assert result.steps is not None and result.attempt is not None
        step = result.steps[1]
        error = result.steps[0].error
        assert error is not None
        for value in (
            result,
            result.attempt,
            step,
            error,
            step_status("pending", None),
        ):
            with self.subTest(type(value).__name__):
                field = dataclasses.fields(value)[0].name
                with self.assertRaises(dataclasses.FrozenInstanceError):
                    setattr(value, field, None)
        self.assertIsInstance(result.problems, tuple)
        self.assertIsInstance(result.steps, tuple)
        self.assertIsInstance(step.direct_blockers, tuple)
        self.assertIsInstance(step.failed_upstream, tuple)

    def test_a_long_step_id_is_shown_bounded(self):
        """A message names a path bounded, although the path itself is exact."""
        document = snapshot("running")
        step_id = "x" * 10_000
        document["steps"][step_id] = changed(document["steps"]["qc"], "ts", "now")

        result = read(document)

        (problem,) = result.problems
        self.assertEqual(problem.path, f"steps.{step_id}.ts")
        self.assertLessEqual(len(problem.message), MAX_MESSAGE)


if __name__ == "__main__":
    unittest.main()
