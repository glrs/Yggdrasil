"""Value helpers: a plan's generation, and the ID of its snapshot.

None of them checks the version, and none raises on stored content. The
snapshot ID is the one the sinks store a plan's snapshot under.
"""

from __future__ import annotations

import json
import unittest

from tests.contract.read_support import (
    CURRENT,
    HOSTILE,
    ReaderTestCase,
    plan_document,
    plan_document_names,
)
from tests.contract.support import ABSENT, changed
from yggdrasil.contract.read import (
    generation_of,
    partition_name,
    read_plan,
    snapshot_id,
    snapshot_id_for,
)
from yggdrasil.storage.partitions import partition_key
from yggdrasil.storage.plan_documents import plan_generation_of

ARGUMENTS = ("test_realm", "project", "P1", "pln_lifecycle")


def stored_snapshot_id(name: str) -> str:
    """Return the ``_id`` a corpus snapshot was stored under."""
    path = CURRENT / "plan_status" / f"{name}.json"
    return json.loads(path.read_text("utf-8"))["_id"]


class TestGenerationOf(ReaderTestCase):
    """Only a non-empty string is a generation."""

    def test_a_non_empty_string(self):
        self.assertEqual(
            generation_of(plan_document("draft")), "19ba8ec8e02efe85c36b6a8b3f8c39fe"
        )
        self.assertEqual(generation_of({"plan_generation": "g"}), "g")

    def test_anything_else_is_none(self):
        values = [("absent", ABSENT), *HOSTILE]
        for name, value in values:
            if isinstance(value, str) and value:
                continue
            with self.subTest(name):
                document = changed(plan_document("draft"), "plan_generation", value)
                self.assertIsNone(generation_of(document))

    def test_a_body_that_is_not_an_object_has_none(self):
        with self.assert_silent():
            for name, value in HOSTILE:
                if isinstance(value, dict):
                    continue
                with self.subTest(name):
                    self.assertIsNone(generation_of(value))

    def test_no_version_is_checked(self):
        document = plan_document("pre_contract")
        self.assertEqual(generation_of(document), document["plan_generation"])

    def test_the_store_s_old_name_is_this_function(self):
        """The plan store's name for this function is kept for one release."""
        self.assertIs(plan_generation_of, generation_of)


class TestPartitionName(unittest.TestCase):
    """The partition of a scope, formatted without validation."""

    def test_the_kinds_with_their_own_prefix(self):
        self.assertEqual(partition_name("project", "P12345"), "proj-P12345")
        self.assertEqual(partition_name("flowcell", "A22FN2"), "fc-A22FN2")
        self.assertEqual(partition_name("bundle", "B12345"), "bundle-B12345")
        self.assertEqual(partition_name("Project", "P1"), "Project-P1")

    def test_it_equals_the_writer_s_partition_on_everything_the_writer_accepts(self):
        cases = (
            ("project", "P1"),
            ("flowcell", "A22FN2"),
            ("bundle", "B1"),
            ("project", 5),
            (5, "P1"),
            (True, True),
            (1.5, "x"),
            (["project"], "P1"),
            ({"kind": "project"}, "P1"),
            ("project", ["P1"]),
            ("flowcell", {"id": 1}),
        )
        for kind, scope_id in cases:
            with self.subTest(kind=str(kind), scope_id=str(scope_id)):
                self.assertEqual(
                    partition_name(kind, scope_id),
                    partition_key({"kind": kind, "id": scope_id}),
                )

    def test_it_validates_nothing(self):
        self.assertEqual(partition_name("", ""), "-")
        self.assertEqual(partition_name(None, None), "None-None")
        with self.assertRaises(ValueError):
            partition_key({"kind": "", "id": ""})


class TestSnapshotId(ReaderTestCase):
    """The snapshot's ID from four non-empty strings, or None; never an exception."""

    def test_the_id_of_a_plan_s_snapshot(self):
        self.assertEqual(
            snapshot_id(*ARGUMENTS), "proj-P1:plan_status:test_realm:pln_lifecycle"
        )
        self.assertEqual(
            snapshot_id("tenx", "flowcell", "A22FN2", "tenx:A22FN2"),
            "fc-A22FN2:plan_status:tenx:tenx:A22FN2",
        )

    def test_any_argument_that_is_not_a_non_empty_string_gives_none(self):
        with self.assert_silent():
            for position in range(len(ARGUMENTS)):
                for name, value in HOSTILE:
                    if isinstance(value, str) and value:
                        continue
                    with self.subTest(position=position, value=name):
                        arguments = list(ARGUMENTS)
                        arguments[position] = value
                        self.assertIsNone(snapshot_id(*arguments))

    def test_it_is_the_id_the_sink_stored_the_snapshot_under(self):
        for plan, snapshot in (
            ("approved_finalized", "finished_success"),
            ("continue_independent_failures", "finished_with_failures"),
        ):
            with self.subTest(plan):
                document = plan_document(plan)
                self.assertEqual(
                    snapshot_id_for(document, plan_id=document["_id"]),
                    stored_snapshot_id(snapshot),
                )

    def test_the_plan_id_is_the_one_read_under(self):
        document = plan_document("draft")

        self.assertEqual(
            snapshot_id_for(document, plan_id="pln_other"),
            "proj-P1:plan_status:test_realm:pln_other",
        )

    def test_a_document_that_cannot_name_its_snapshot(self):
        cases = [
            ("realm absent", changed(plan_document("draft"), "realm", ABSENT)),
            ("realm empty", changed(plan_document("draft"), "realm", "")),
            ("scope absent", changed(plan_document("draft"), "scope", ABSENT)),
            ("scope text", changed(plan_document("draft"), "scope", "P1")),
            ("kind absent", changed(plan_document("draft"), "scope.kind", ABSENT)),
            ("kind an int", changed(plan_document("draft"), "scope.kind", 5)),
            ("id null", changed(plan_document("draft"), "scope.id", None)),
        ]
        cases += [(f"body: {name}", value) for name, value in HOSTILE]
        with self.assert_silent():
            for name, document in cases:
                with self.subTest(name):
                    self.assertIsNone(
                        snapshot_id_for(document, plan_id="pln_lifecycle")
                    )
        for name, value in HOSTILE:
            if isinstance(value, str) and value:
                continue
            with self.subTest(plan_id=name):
                self.assertIsNone(
                    snapshot_id_for(plan_document("draft"), plan_id=value)
                )

    def test_no_version_is_checked(self):
        document = plan_document("pre_contract")
        self.assertEqual(
            snapshot_id_for(document, plan_id="pln_lifecycle"),
            "proj-P1:plan_status:test_realm:pln_lifecycle",
        )

    def test_a_read_names_the_snapshot_only_of_a_usable_plan(self):
        for name in plan_document_names():
            with self.subTest(name):
                document = plan_document(name)
                plan = read_plan(document, plan_id=document["_id"])
                expected = (
                    snapshot_id_for(document, plan_id=document["_id"])
                    if plan.usable
                    else None
                )
                self.assertEqual(plan.snapshot_id, expected)
        plan = read_plan(plan_document("pre_contract"), plan_id="pln_lifecycle")
        self.assertIsNone(plan.snapshot_id)


if __name__ == "__main__":
    unittest.main()
