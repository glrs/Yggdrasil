"""Planning replaces only the plan documents it may.

Planning replaces the whole document in a plan's slot. It may replace an
empty slot, an unversioned (pre-contract) document or a document of a
supported family. It refuses an invalid ``schema_version``, an unsupported
family and a ``created_at`` the replacement cannot keep, and a refusal writes
nothing: the stored document, its revision and the plan-change sequence stay
as they were. Covered here:

- the rule on documents built by ``build_plan_document``;
- ``save_plan`` on both backends: SQLite on a real temporary file, and
  ``PlanDBManager`` on an in-memory client that enforces CouchDB's revision
  rules;
- the planning callers of ``YggdrasilCore`` (daemon planning,
  ``--plan-only`` and run-once) on both backends: each logs a refusal once
  and goes on with its other drafts.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import unittest
from collections.abc import Callable
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

from tests.contract.support import ConformingBuilders
from tests.execution_support import CouchBackend, SQLiteBackend
from tests.plan_store_support import SCOPE, make_plan
from yggdrasil.daemon.core import YggdrasilCore
from yggdrasil.daemon.singleton import SingletonMeta
from yggdrasil.flow.model import Plan
from yggdrasil.flow.planner.api import PlanDraft
from yggdrasil.storage import (
    PlanStoreError,
    RevisionConflictError,
    UnsupportedDocumentError,
)
from yggdrasil.storage.plan_documents import build_plan_document
from yggdrasil.storage.protocols import InternalStorageBundle
from yggdrasil.watchers.events import EventType

PLAN_ID = "pln_test_P1_v1"
REALM = "test_realm"

# A canonical creation time no build in these tests can produce, and the same
# instant as earlier releases spelled it.
CREATED = "2026-01-01T00:00:00Z"
LEGACY_CREATED = "2026-01-01T00:00:00+00:00"

# Marks a field to remove from a builder-made document.
ABSENT = object()

# An integer Python refuses to convert to text: it has more digits than
# sys.get_int_max_str_digits() allows by default.
HUGE = 10**5000

# Markers planning refuses, with the reason it gives.
REFUSED_MARKERS = (
    (2, "unsupported"),
    ("1", "invalid"),
    (1.0, "invalid"),
    (True, "invalid"),
    (None, "invalid"),
    (0, "invalid"),
    (-1, "invalid"),
)

# created_at values no replacement can keep, whatever the slot's family.
UNUSABLE_CREATED_AT = (
    (ABSENT, "missing"),
    (None, "malformed"),
    (20260101, "malformed"),
    ("yesterday", "malformed"),
    ("2026-02-30T00:00:00Z", "malformed"),
    ("2026-01-01T00:00:00Z\n", "malformed"),
    ("2026-01-01T02:00:00+02:00", "unsupported"),
    ("2026-01-01T00:00:00-00:00", "unsupported"),
    ("2026-01-01T00:00:00.000+00:00", "unsupported"),
    ("2026-01-01T00:00:00.5+00:00", "unsupported"),
    ("2026-01-01T00:00:00.250Z", "unsupported"),
    ("2026-01-01T00:00:00.1234567Z", "unsupported"),
    ("2026-01-01t00:00:00z", "unsupported"),
)

# Canonical creation times with a fraction of a second. A supported document
# keeps one verbatim. An unversioned document cannot carry one: earlier
# releases wrote whole seconds only.
FRACTIONAL_CREATED = (
    "2026-01-01T00:00:00.5Z",
    "2026-01-01T00:00:00.25Z",
    "2026-01-01T00:00:00.000001Z",
    "2026-01-01T00:00:00.123456Z",
)


def slot_document(plan_id: str = PLAN_ID, **changes: Any) -> dict[str, Any]:
    """Return a builder-made family-1 document created at CREATED, changed.

    Args:
        plan_id: The plan ID.
        **changes: Fields to set; a field set to ABSENT is removed.

    Returns:
        dict[str, Any]: The document, without a revision.
    """
    doc = build_plan_document(make_plan(plan_id), REALM, dict(SCOPE), now=CREATED)
    for field, value in changes.items():
        if value is ABSENT:
            doc.pop(field, None)
        else:
            doc[field] = value
    return doc


def recorded_value(value: Any) -> Any:
    """Return what a refusal records for a stored value: None when absent."""
    return None if value is ABSENT else value


def label(value: Any) -> str:
    """Return a subTest label for a stored value, ABSENT included."""
    return "absent" if value is ABSENT else repr(value)


class TestReplaceRule(ConformingBuilders):
    """What ``build_plan_document`` may build over the document in a slot."""

    def rebuild(self, existing: dict[str, Any]) -> dict[str, Any]:
        """Build a new generation of the plan over existing."""
        return build_plan_document(
            make_plan(PLAN_ID), REALM, dict(SCOPE), existing=existing
        )

    def assert_refused(
        self, existing: dict[str, Any], *, field: str, reason: str, recorded: Any
    ) -> UnsupportedDocumentError:
        """Assert that building over existing is refused, and how."""
        with self.assertRaises(UnsupportedDocumentError) as ctx:
            self.rebuild(existing)
        error = ctx.exception
        self.assertEqual(
            (error.doc_id, error.field, error.reason), (PLAN_ID, field, reason)
        )
        self.assertIs(type(error.recorded), type(recorded))
        self.assertEqual(error.recorded, recorded)
        self.assertEqual(error.supported, frozenset({1}))
        return error

    def test_an_invalid_or_unsupported_marker_is_refused(self):
        for marker, reason in REFUSED_MARKERS:
            with self.subTest(marker=marker):
                self.assert_refused(
                    slot_document(schema_version=marker),
                    field="schema_version",
                    reason=reason,
                    recorded=marker,
                )

    def test_the_marker_is_judged_before_created_at(self):
        for created_at, _ in UNUSABLE_CREATED_AT:
            with self.subTest(created_at=label(created_at)):
                self.assert_refused(
                    slot_document(schema_version=2, created_at=created_at),
                    field="schema_version",
                    reason="unsupported",
                    recorded=2,
                )

    def test_an_unversioned_document_keeps_its_instant_spelled_canonically(self):
        for created_at in (LEGACY_CREATED, CREATED):
            with self.subTest(created_at=created_at):
                existing = slot_document(schema_version=ABSENT, created_at=created_at)

                regenerated = self.rebuild(existing)

                self.assertEqual(regenerated["created_at"], CREATED)
                self.assertEqual(regenerated["schema_version"], 1)

    def test_an_unversioned_document_with_an_unusable_created_at_is_refused(self):
        for created_at, reason in UNUSABLE_CREATED_AT:
            with self.subTest(created_at=label(created_at)):
                self.assert_refused(
                    slot_document(schema_version=ABSENT, created_at=created_at),
                    field="created_at",
                    reason=reason,
                    recorded=recorded_value(created_at),
                )

    def test_a_supported_document_keeps_its_canonical_created_at_verbatim(self):
        regenerated = self.rebuild(slot_document())

        self.assertEqual(regenerated["created_at"], CREATED)
        self.assertNotEqual(regenerated["updated_at"], CREATED)

    def test_a_supported_document_keeps_a_canonical_fraction_verbatim(self):
        for created_at in FRACTIONAL_CREATED:
            with self.subTest(created_at=created_at):
                regenerated = self.rebuild(slot_document(created_at=created_at))

                self.assertEqual(regenerated["created_at"], created_at)
                self.assertNotEqual(regenerated["updated_at"], created_at)

    def test_an_unversioned_document_with_a_fraction_is_refused(self):
        for created_at in FRACTIONAL_CREATED:
            with self.subTest(created_at=created_at):
                self.assert_refused(
                    slot_document(schema_version=ABSENT, created_at=created_at),
                    field="created_at",
                    reason="unsupported",
                    recorded=created_at,
                )

    def test_a_supported_document_needs_a_canonical_created_at(self):
        for created_at, reason in (
            (LEGACY_CREATED, "unsupported"),
            *UNUSABLE_CREATED_AT,
        ):
            with self.subTest(created_at=label(created_at)):
                self.assert_refused(
                    slot_document(created_at=created_at),
                    field="created_at",
                    reason=reason,
                    recorded=recorded_value(created_at),
                )

    def test_the_refusal_names_the_plan_the_value_and_the_supported_families(self):
        error = self.assert_refused(
            slot_document(schema_version="1"),
            field="schema_version",
            reason="invalid",
            recorded="1",
        )
        self.assertEqual(
            str(error),
            "Planning refused to replace plan 'pln_test_P1_v1': its "
            "schema_version '1' is invalid (supported plan-document families: [1])",
        )

        missing = self.assert_refused(
            slot_document(created_at=ABSENT),
            field="created_at",
            reason="missing",
            recorded=None,
        )
        self.assertIn("its created_at is missing", str(missing))

    def test_a_value_that_cannot_be_shown_is_still_refused(self):
        cases = {
            "huge marker": ("schema_version", HUGE, "unsupported"),
            "nested huge marker": ("schema_version", {"family": [HUGE]}, "invalid"),
            "huge created_at": ("created_at", HUGE, "malformed"),
            "nested huge created_at": ("created_at", {"at": [HUGE]}, "malformed"),
        }
        for case, (field, value, reason) in cases.items():
            with self.subTest(case=case):
                error = self.assert_refused(
                    slot_document(**{field: value}),
                    field=field,
                    reason=reason,
                    recorded=value,
                )

                self.assertIs(error.recorded, value)
                shown = f"<unrepresentable {type(value).__name__}>"
                self.assertIn(f"its {field} {shown} is {reason}", str(error))
                self.assertIn(shown, repr(error))

    def test_the_message_shows_a_bounded_representation(self):
        cases = {
            "long string": ("created_at", "x" * 100_000, "malformed"),
            "long list": ("schema_version", list(range(100_000)), "invalid"),
        }
        for case, (field, value, reason) in cases.items():
            with self.subTest(case=case):
                error = self.assert_refused(
                    slot_document(**{field: value}),
                    field=field,
                    reason=reason,
                    recorded=value,
                )

                self.assertIs(error.recorded, value)
                self.assertLess(len(str(error)), 400)

    def test_the_refusal_is_neither_a_backend_failure_nor_a_lost_race(self):
        error = self.assert_refused(
            slot_document(schema_version=2),
            field="schema_version",
            reason="unsupported",
            recorded=2,
        )
        self.assertNotIsInstance(error, PlanStoreError)
        self.assertNotIsInstance(error, RevisionConflictError)


class SQLiteSlots(SQLiteBackend):
    """The SQLite plan store, with reads of its stored state."""

    def stored(self, doc_id: str) -> dict[str, Any] | None:
        """Return the document as stored, ``_rev`` included."""
        return self.internal.get_document("plans", doc_id)

    def plan_seq(self) -> int | None:
        """Return the head of the plan-change sequence."""
        return self.internal.current_plan_seq()

    def after_next_read(self, action: Callable[[], None]) -> None:
        """Run action once, right after the store's next document read."""
        real = self.internal.get_document

        def read_then_act(namespace: str, doc_id: str) -> dict[str, Any] | None:
            self.internal.get_document = real  # type: ignore[method-assign]
            doc = real(namespace, doc_id)
            action()
            return doc

        self.internal.get_document = read_then_act  # type: ignore[method-assign]


class CouchSlots(CouchBackend):
    """The CouchDB plan store on its fake client, with reads of its state."""

    def stored(self, doc_id: str) -> dict[str, Any] | None:
        """Return the document as stored, ``_rev`` included."""
        return self.server.raw(doc_id)

    def plan_seq(self) -> int | None:
        """Return None: CouchDB keeps no plan-change sequence of its own."""
        return None

    def after_next_read(self, action: Callable[[], None]) -> None:
        """Run action once, right after the store's next document read."""
        self.server.after_read.append(action)


class SlotTestCase(ConformingBuilders):
    """A plan store whose slots tests fill directly.

    Subclasses choose the backend with ``backend_type``.
    """

    backend_type: type[SQLiteSlots] | type[CouchSlots] = SQLiteSlots

    def setUp(self) -> None:
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.backend = self.backend_type(self, Path(temp_dir.name))

    def put(self, doc: dict[str, Any]) -> dict[str, Any]:
        """Store doc as given, bypassing the plan store; return it as stored."""
        self.backend.raw_put(doc)
        stored = self.backend.stored(doc["_id"])
        assert stored is not None
        return stored

    def save(self, plan_id: str = PLAN_ID) -> dict[str, Any]:
        """Plan plan_id through the store and return the stored document."""
        self.backend.plans.save_plan(
            make_plan(plan_id), REALM, dict(SCOPE), auto_run=True
        )
        stored = self.backend.stored(plan_id)
        assert stored is not None
        return stored

    def assert_refused_without_writing(
        self, plan_id: str, *, field: str, reason: str, recorded: Any
    ) -> None:
        """Assert that planning plan_id is refused, and that nothing was written.

        The stored document is compared whole, ``_rev`` included, and the
        plan-change sequence must not move.
        """
        before = self.backend.stored(plan_id)
        sequence = self.backend.plan_seq()

        with self.assertRaises(UnsupportedDocumentError) as ctx:
            self.save(plan_id)

        error = ctx.exception
        self.assertEqual(
            (error.doc_id, error.field, error.reason), (plan_id, field, reason)
        )
        self.assertIs(type(error.recorded), type(recorded))
        self.assertEqual(error.recorded, recorded)
        self.assertEqual(self.backend.stored(plan_id), before)
        self.assertEqual(self.backend.plan_seq(), sequence)


class SaveScenarios:
    """``save_plan`` over each kind of slot, against one backend.

    Mixed into one test case per plan-store backend.
    """

    def test_an_invalid_or_unsupported_marker_is_left_as_it_is(self):
        for index, (marker, reason) in enumerate(REFUSED_MARKERS):
            plan_id = f"pln_marker_{index}"
            with self.subTest(marker=marker):
                self.put(slot_document(plan_id, schema_version=marker))
                self.assert_refused_without_writing(
                    plan_id, field="schema_version", reason=reason, recorded=marker
                )

    def test_an_unversioned_document_becomes_a_new_family_one_generation(self):
        for index, created_at in enumerate((LEGACY_CREATED, CREATED)):
            plan_id = f"pln_unversioned_{index}"
            with self.subTest(created_at=created_at):
                legacy = self.put(
                    slot_document(
                        plan_id,
                        schema_version=ABSENT,
                        created_at=created_at,
                        plan_generation="g-legacy",
                        run_token=3,
                        executed_run_token=3,
                    )
                )

                stored = self.save(plan_id)

                self.assertEqual(stored["schema_version"], 1)
                self.assertNotIn(stored["plan_generation"], ("g-legacy", None, ""))
                self.assertEqual(
                    (stored["run_token"], stored["executed_run_token"]), (0, -1)
                )
                self.assertEqual(stored["created_at"], CREATED)
                self.assertNotEqual(stored["_rev"], legacy["_rev"])

    def test_an_unversioned_document_with_an_unusable_created_at_is_left(self):
        for index, (created_at, reason) in enumerate(UNUSABLE_CREATED_AT):
            plan_id = f"pln_unversioned_{index}"
            with self.subTest(created_at=label(created_at)):
                self.put(
                    slot_document(plan_id, schema_version=ABSENT, created_at=created_at)
                )
                self.assert_refused_without_writing(
                    plan_id,
                    field="created_at",
                    reason=reason,
                    recorded=recorded_value(created_at),
                )

    def test_a_supported_document_keeps_its_canonical_created_at(self):
        self.put(slot_document())

        stored = self.save()

        self.assertEqual(stored["created_at"], CREATED)
        self.assertNotEqual(stored["updated_at"], CREATED)

    def test_a_supported_document_keeps_a_canonical_fraction_verbatim(self):
        for index, created_at in enumerate(FRACTIONAL_CREATED):
            plan_id = f"pln_fraction_{index}"
            with self.subTest(created_at=created_at):
                before = self.put(slot_document(plan_id, created_at=created_at))

                stored = self.save(plan_id)

                self.assertEqual(stored["created_at"], created_at)
                self.assertNotEqual(stored["updated_at"], created_at)
                self.assertNotEqual(
                    stored["plan_generation"], before["plan_generation"]
                )
                self.assertNotEqual(stored["_rev"], before["_rev"])

    def test_an_unversioned_document_with_a_fraction_is_left(self):
        for index, created_at in enumerate(FRACTIONAL_CREATED):
            plan_id = f"pln_unversioned_fraction_{index}"
            with self.subTest(created_at=created_at):
                self.put(
                    slot_document(plan_id, schema_version=ABSENT, created_at=created_at)
                )
                self.assert_refused_without_writing(
                    plan_id,
                    field="created_at",
                    reason="unsupported",
                    recorded=created_at,
                )

    def test_a_supported_document_with_a_noncanonical_created_at_is_left(self):
        cases = (
            (LEGACY_CREATED, "unsupported"),
            (ABSENT, "missing"),
            ("yesterday", "malformed"),
            ("2026-01-01T00:00:00.250Z", "unsupported"),
            ("2026-01-01T00:00:00.1234567Z", "unsupported"),
        )
        for index, (created_at, reason) in enumerate(cases):
            plan_id = f"pln_supported_{index}"
            with self.subTest(created_at=label(created_at)):
                self.put(slot_document(plan_id, created_at=created_at))
                self.assert_refused_without_writing(
                    plan_id,
                    field="created_at",
                    reason=reason,
                    recorded=recorded_value(created_at),
                )

    def test_regeneration_keeps_only_the_creation_time(self):
        self.put(
            slot_document(
                plan_generation="g-reviewed",
                status="approved",
                approved_at="2026-01-02T00:00:00Z",
                approved_by="alice",
                run_token=2,
                run_requested_at="2026-01-03T00:00:00Z",
                run_requested_by="bob",
                executed_run_token=1,
                last_finalized_execution={"execution_id": "e1", "outcome": "failed"},
                last_executed_at="2026-01-03T01:00:00Z",
                ticket="OPS-1",
                x_ops_ticket="OPS-1",
            )
        )

        stored = self.save()

        fresh = build_plan_document(make_plan(PLAN_ID), REALM, dict(SCOPE))
        self.assertEqual(set(stored), set(fresh) | {"_rev"})
        self.assertEqual(stored["created_at"], CREATED)
        self.assertEqual(stored["schema_version"], 1)
        self.assertNotEqual(stored["plan_generation"], "g-reviewed")
        self.assertEqual((stored["run_token"], stored["executed_run_token"]), (0, -1))

    def test_a_slot_that_changes_family_after_the_read_is_not_replaced(self):
        read = self.put(slot_document())
        newer = dict(read, schema_version=2)
        self.backend.after_next_read(lambda: self.backend.conditional_put(newer))

        with self.assertRaises(RevisionConflictError):
            self.save()

        stored = self.backend.stored(PLAN_ID)
        self.assertEqual(stored["schema_version"], 2)
        self.assertEqual(stored["plan_generation"], read["plan_generation"])


class TestSQLiteSave(SaveScenarios, SlotTestCase):
    """``save_plan`` against SQLitePlanStore on a real temporary database."""


class TestCouchSave(SaveScenarios, SlotTestCase):
    """``save_plan`` against PlanDBManager on a revision-enforcing fake client."""

    backend_type = CouchSlots


REFUSED_ID = "pln_test_P1_refused"
ACCEPTED_ID = "pln_test_P1_accepted"

# The words with which the stores and the callers report a plan they wrote.
SAVE_REPORT = re.compile(r"\b(saved|persisted|created)\b", re.IGNORECASE)


class DraftingHandler:
    """Stands in for a realm handler: drafts the same plans for any document."""

    realm_id = REALM

    def __init__(self, *plans: Plan) -> None:
        self.plans = plans

    def derive_scope(self, doc: dict[str, Any]) -> dict[str, Any]:
        return dict(SCOPE)

    def build_planning_context(self, **kwargs: Any) -> None:
        return None

    def drafts(self) -> list[PlanDraft]:
        return [PlanDraft(plan=plan, auto_run=True) for plan in self.plans]

    def run_now(self, payload: dict[str, Any]) -> list[PlanDraft]:
        return self.drafts()

    async def generate_plan_drafts(self, payload: dict[str, Any]) -> list[PlanDraft]:
        return self.drafts()

    def class_qualified_name(self) -> str:
        return f"{__name__}.DraftingHandler"


class CallerTestCase(ConformingBuilders):
    """A real ``YggdrasilCore`` planning two drafts, one into a refused slot.

    The first draft's slot holds a family-2 document, which this release must
    not replace; the second draft's slot is empty. Subclasses choose the
    backend with ``backend_type``.
    """

    backend_type: type[SQLiteSlots] | type[CouchSlots] = SQLiteSlots

    def setUp(self) -> None:
        SingletonMeta._instances.clear()
        self.addCleanup(SingletonMeta._instances.clear)
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        directory = Path(temp_dir.name)
        for patcher in (
            patch.dict(os.environ, {"YGG_EVENT_SPOOL": str(directory / "spool")}),
            patch("yggdrasil.daemon.core.Engine"),
            patch("yggdrasil.daemon.core.OpsConsumerService"),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

        self.backend = self.backend_type(self, directory)
        bundle = InternalStorageBundle(
            backend=self.backend.name,
            plans=self.backend.plans,
            plan_changes=Mock(),
            checkpoints=Mock(),
            ops_snapshots=Mock(),
        )
        self.core = YggdrasilCore(
            {"work_root": str(directory / "work")}, storage=bundle
        )
        self.core.pdm = Mock()
        self.core.pdm.fetch_document_by_id.return_value = {"project_id": "P1"}

        self.backend.raw_put(slot_document(REFUSED_ID, schema_version=2))
        self.newer = self.backend.stored(REFUSED_ID)
        handler: Any = DraftingHandler(make_plan(REFUSED_ID), make_plan(ACCEPTED_ID))
        self.handler = handler
        self.core.subscriptions[EventType.PROJECT_CHANGE] = [handler]

    def assert_only_the_refused_draft_was_skipped(
        self, records: list[logging.LogRecord]
    ) -> dict[str, Any]:
        """Assert the refused slot is unchanged and logged once; the other is saved.

        Args:
            records: Everything the yggdrasil loggers logged meanwhile.

        Returns:
            dict[str, Any]: The accepted plan's stored document.
        """
        self.assertEqual(self.backend.stored(REFUSED_ID), self.newer)
        accepted = self.backend.stored(ACCEPTED_ID)
        assert accepted is not None
        self.assertEqual(accepted["schema_version"], 1)

        errors = [r for r in records if r.levelno >= logging.ERROR]
        self.assertEqual(len(errors), 1, [r.getMessage() for r in errors])
        message = errors[0].getMessage()
        self.assertIn(f"'{REFUSED_ID}'", message)
        self.assertIn("its schema_version 2 is unsupported", message)
        self.assertIsNone(errors[0].exc_info)

        # Only the accepted draft is reported as saved, persisted or created.
        reports = [
            r.getMessage()
            for r in records
            if r.levelno == logging.INFO and SAVE_REPORT.search(r.getMessage())
        ]
        self.assertTrue(reports)
        for report in reports:
            with self.subTest(report=report):
                self.assertIn(ACCEPTED_ID, report)
        return accepted


class CallerScenarios:
    """Each planning caller skips a refused draft and persists the others.

    Mixed into one test case per plan-store backend.
    """

    def test_daemon_planning_persists_the_other_drafts(self):
        with self.assertLogs("yggdrasil", level=logging.INFO) as logs:
            asyncio.run(self.core._generate_and_persist_plan(self.handler, {}))

        self.assert_only_the_refused_draft_was_skipped(logs.records)

    def test_plan_only_returns_only_the_persisted_drafts(self):
        with self.assertLogs("yggdrasil", level=logging.INFO) as logs:
            persisted = self.core.create_plan_from_doc("P1", force_overwrite=True)

        self.assertEqual(persisted, [ACCEPTED_ID])
        accepted = self.assert_only_the_refused_draft_was_skipped(logs.records)
        self.assertEqual(accepted["status"], "draft")

    def test_run_once_watches_only_the_persisted_drafts(self):
        watch = AsyncMock(return_value=0)
        with (
            patch.object(self.core, "_run_once_watcher_loop", watch),
            patch("yggdrasil.ops.consumer.FileSpoolConsumer"),
            self.assertLogs("yggdrasil", level=logging.INFO) as logs,
        ):
            exit_code = self.core.run_once_with_watcher("P1", force_overwrite=True)

        self.assertEqual(exit_code, 0)
        session = watch.await_args.kwargs
        self.assertEqual(session["pending_plan_ids"], [ACCEPTED_ID])
        accepted = self.assert_only_the_refused_draft_was_skipped(logs.records)
        self.assertEqual(accepted["execution_authority"], "run_once")
        self.assertEqual(accepted["execution_owner"], session["execution_owner"])

    def test_run_once_without_any_persisted_plan_exits_nonzero(self):
        self.handler.plans = (make_plan(REFUSED_ID),)
        watch = AsyncMock(return_value=0)
        with (
            patch.object(self.core, "_run_once_watcher_loop", watch),
            self.assertLogs("yggdrasil", level=logging.ERROR) as logs,
        ):
            exit_code = self.core.run_once_with_watcher("P1", force_overwrite=True)

        self.assertEqual(exit_code, 1)
        watch.assert_not_called()
        self.assertEqual(self.backend.stored(REFUSED_ID), self.newer)
        messages = [r.getMessage() for r in logs.records]
        self.assertEqual(len(messages), 2, messages)
        self.assertIn("its schema_version 2 is unsupported", messages[0])
        self.assertIn("Failed to create any plans", messages[1])


class TestSQLiteCallers(CallerScenarios, CallerTestCase):
    """The planning callers over SQLitePlanStore."""


class TestCouchCallers(CallerScenarios, CallerTestCase):
    """The planning callers over PlanDBManager on a revision-enforcing fake client."""

    backend_type = CouchSlots


if __name__ == "__main__":
    unittest.main()
