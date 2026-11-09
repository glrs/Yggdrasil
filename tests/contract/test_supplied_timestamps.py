"""Writers keep the timestamps they are given, a fraction of a second included.

The clocks write whole seconds. A caller may supply a timestamp instead, and
each writer that takes one writes it as given: planning, the finalizer, the
attempt report and the snapshot envelope. So a supplied canonical fraction
survives every write, on both plan stores, and conforms to the published
schemas. Planning over a stored document keeps a canonical fractional
``created_at`` too; that is tested with the rule it follows, in
``tests/test_planning_replace_rule.py``.
"""

from __future__ import annotations

import dataclasses
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import patch

from tests.contract.support import (
    PROJECTION_PLAN_ID,
    PROJECTION_REALM,
    PROJECTION_SCOPE,
    ConformingBuilders,
    assert_conforms,
    stored_snapshot,
)
from tests.execution_support import CouchBackend, SQLiteBackend
from tests.plan_store_support import SCOPE, finalization_for, make_plan
from yggdrasil.contract import ATTEMPT_REPORT, PLAN_DOCUMENT, PLAN_STATUS
from yggdrasil.contract.timestamps import is_canonical
from yggdrasil.flow.outcomes import AttemptReport, StepOutcome, TerminationReason
from yggdrasil.ops.snapshot import PROJECTION_LEGACY, snapshot_document
from yggdrasil.storage.plan_documents import build_plan_document
from yggdrasil.storage.plan_updates import (
    FinalizationStatus,
    build_finalized_document,
    finalize_execution,
)

LOGGER = logging.getLogger(__name__)

# Canonical timestamps a caller supplies, each with a fraction of its own length.
PLANNED = "2026-10-22T09:00:00.25Z"
STARTED = "2026-10-22T10:00:00.000001Z"
ENDED = "2026-10-22T10:00:05.123456Z"
FINALIZED = "2026-10-22T10:00:06.5Z"
SNAPSHOT_WRITTEN = "2026-10-22T10:00:07.75Z"

PLAN_ID = "pln_test_P1_v1"


def planned(**kwargs: Any) -> dict[str, Any]:
    """Return the builder's document of the default plan, planned at PLANNED."""
    return build_plan_document(
        make_plan(PLAN_ID), "test_realm", dict(SCOPE), now=PLANNED, **kwargs
    )


def completion_times(document: dict[str, Any]) -> tuple[Any, Any, Any]:
    """Return the three times a finalization writes, as the document holds them."""
    return (
        document["updated_at"],
        document["last_executed_at"],
        document["last_finalized_execution"]["finalized_at"],
    )


def supplied_report() -> AttemptReport:
    """Return the closed report of a succeeded attempt, with supplied times."""
    report = AttemptReport(
        execution_id="exec-1",
        plan_id=PLAN_ID,
        step_ids=["s1", "s2"],
        plan_generation="g1",
        run_token=0,
        started_at=STARTED,
    )
    report.record_outcome("s1", StepOutcome.SUCCEEDED)
    report.record_outcome("s2", StepOutcome.SUCCEEDED)
    report.finish(TerminationReason.COMPLETED, ended_at=ENDED)
    return report


class _InMemoryPlan:
    """fetch and replace over one stored plan document, as a store offers them."""

    def __init__(self, document: dict[str, Any]) -> None:
        """Hold document as stored, at revision 1."""
        self.document = {**document, "_rev": "1"}

    def fetch(self, doc_id: str) -> dict[str, Any] | None:
        """Return the stored document."""
        return dict(self.document)

    def replace(self, doc_id: str, body: dict[str, Any], expected_rev: str) -> str:
        """Store body as the next revision; the test never races it."""
        self.document = {**body, "_rev": str(int(expected_rev) + 1)}
        return self.document["_rev"]


class TestSuppliedTimestamps(ConformingBuilders):
    """Each writer writes a supplied timestamp verbatim, and conforms."""

    def test_planning_writes_a_supplied_time_verbatim(self):
        document = planned()

        self.assertEqual(
            (document["created_at"], document["updated_at"]), (PLANNED,) * 2
        )
        assert_conforms(self, PLAN_DOCUMENT, document)

    def test_the_finalizer_writes_a_supplied_time_verbatim(self):
        document = planned(auto_run=True)

        finalized = build_finalized_document(
            document, finalization_for(document), now=FINALIZED
        )

        self.assertEqual(completion_times(finalized), (FINALIZED,) * 3)
        self.assertEqual(finalized["created_at"], PLANNED)
        assert_conforms(self, PLAN_DOCUMENT, finalized)

    def test_finalizing_writes_a_supplied_time_verbatim(self):
        store = _InMemoryPlan(planned(auto_run=True))
        request = finalization_for(store.document)

        result = finalize_execution(
            request,
            fetch=store.fetch,
            replace=store.replace,
            logger=LOGGER,
            now=FINALIZED,
        )

        self.assertEqual(result.status, FinalizationStatus.COMMITTED)
        self.assertEqual(completion_times(store.document), (FINALIZED,) * 3)
        assert_conforms(self, PLAN_DOCUMENT, store.document)

    def test_the_report_writes_supplied_start_and_end_verbatim(self):
        written = supplied_report().to_dict()

        self.assertEqual((written["started_at"], written["ended_at"]), (STARTED, ENDED))
        assert_conforms(self, ATTEMPT_REPORT, written)

    def test_a_recorded_report_keeps_its_supplied_times(self):
        document = planned(auto_run=True, plan_generation="g1")
        request = dataclasses.replace(
            finalization_for(document), report=supplied_report().to_dict()
        )

        finalized = build_finalized_document(document, request, now=FINALIZED)

        recorded = finalized["last_finalized_execution"]["report"]
        self.assertEqual(
            (recorded["started_at"], recorded["ended_at"]), (STARTED, ENDED)
        )
        assert_conforms(self, PLAN_DOCUMENT, finalized)

    def test_the_snapshot_writes_a_supplied_time_verbatim(self):
        snapshot = snapshot_document(
            realm=PROJECTION_REALM,
            plan_id=PROJECTION_PLAN_ID,
            scope=dict(PROJECTION_SCOPE),
            projection=PROJECTION_LEGACY,
            attempt=None,
            steps={},
            updated_at=SNAPSHOT_WRITTEN,
        )

        self.assertEqual(snapshot["updated_at"], SNAPSHOT_WRITTEN)
        assert_conforms(self, PLAN_STATUS, stored_snapshot(snapshot))

    def test_without_a_supplied_time_the_writers_write_whole_seconds(self):
        document = build_plan_document(make_plan(PLAN_ID), "test_realm", dict(SCOPE))
        report = AttemptReport(execution_id="exec-1", plan_id=PLAN_ID)
        report.finish(TerminationReason.COMPLETED)
        store = _InMemoryPlan(planned(auto_run=True))
        finalize_execution(
            finalization_for(store.document),
            fetch=store.fetch,
            replace=store.replace,
            logger=LOGGER,
        )

        for name, value in (
            ("created_at", document["created_at"]),
            ("updated_at", document["updated_at"]),
            ("started_at", report.started_at),
            ("ended_at", report.ended_at),
            *zip(
                ("updated_at", "last_executed_at", "finalized_at"),
                completion_times(store.document),
                strict=True,
            ),
        ):
            with self.subTest(name):
                self.assertTrue(is_canonical(value))
                self.assertNotIn(".", value)


class StoredFractionScenarios:
    """A stored document's supplied fractions survive a store's finalization.

    The stores read the finalization time from the clock, so the clock is set
    to a fractional instant: what the finalizer is given is what the store
    writes. Mixed into one ConformingBuilders case per backend, which names
    the backend in ``backend_type``.
    """

    backend_type: Any
    backend: Any

    def setUp(self):
        super().setUp()
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.backend = self.backend_type(self, Path(temp_dir.name))

    def test_finalizing_keeps_every_fraction(self):
        self.backend.raw_put(planned(auto_run=True))
        stored = self.backend.plans.fetch_plan(PLAN_ID)
        request = finalization_for(stored)

        with patch("yggdrasil.storage.plan_updates.utcnow_iso", return_value=FINALIZED):
            result = self.backend.plans.finalize_execution(request)

        self.assertTrue(result.recorded, result.message)
        finalized = self.backend.plans.fetch_plan(PLAN_ID)
        self.assertEqual(finalized["created_at"], PLANNED)
        self.assertEqual(completion_times(finalized), (FINALIZED,) * 3)
        assert_conforms(self, PLAN_DOCUMENT, finalized)


class TestSQLiteStoredFractions(StoredFractionScenarios, ConformingBuilders):
    """The scenarios on the SQLite plan store, on a temporary database file."""

    backend_type = SQLiteBackend


class TestCouchStoredFractions(StoredFractionScenarios, ConformingBuilders):
    """The scenarios on the CouchDB plan store, on an in-memory client."""

    backend_type = CouchBackend
