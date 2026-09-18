"""Plan-store contract for generations, conditional writes and finalization.

Every scenario runs unchanged against both backends: SQLite on a real
temporary file, and ``PlanDBManager`` on an in-memory client that enforces
CouchDB's revision rules. Concurrent writers are interleaved deterministically
by running them right after the store under test reads the document, which is
exactly the window a lost update needs.
"""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any

from requests.exceptions import ConnectionError as RequestsConnectionError

from lib.core_utils.plan_eligibility import is_plan_eligible
from lib.storage.errors import PlanStoreError, RevisionConflictError
from lib.storage.plan_documents import build_plan_document
from lib.storage.plan_updates import FinalizationStatus, SupersessionReason
from lib.storage.sqlite import SQLiteInternalStore, SQLitePlanStore
from tests.plan_store_support import (
    DRAINED_FAILURE,
    PREFLIGHT_REJECTED,
    SCOPE,
    FakeApiException,
    FakeCouchServer,
    continuation_plan,
    finalization_for,
    make_plan,
    patch_api_exception,
    plan_db_manager_on,
)

PLAN_ID = "pln_test_P1_v1"


class PlanStoreContract:
    """Scenarios every PlanStore backend must satisfy identically.

    Mixed into one ``unittest.TestCase`` per backend. Subclasses provide the
    store under test as ``self.plans`` and implement the backend hooks below.
    """

    plans: Any

    # ----- backend hooks -----

    def after_next_read(self, action: Callable[[], None]) -> None:
        """Run action once, right after the store's next document read."""
        raise NotImplementedError

    def fail_after_next_write(self) -> None:
        """Apply the next write, then make the backend call fail anyway."""
        raise NotImplementedError

    def fail_next_read(self) -> None:
        """Make the next document read fail in the backend."""
        raise NotImplementedError

    def raw_put(self, doc: dict[str, Any]) -> None:
        """Store doc as-is, bypassing the plan store (e.g. a legacy document)."""
        raise NotImplementedError

    def write_count(self, doc: dict[str, Any]) -> int:
        """Number of writes the document's revision reflects."""
        raise NotImplementedError

    # ----- helpers -----

    def save(self, plan=None, **kwargs: Any) -> dict[str, Any]:
        """Save plan (approved by default) and return the stored document."""
        kwargs.setdefault("auto_run", True)
        plan = plan or make_plan(PLAN_ID)
        self.plans.save_plan(plan, "test_realm", dict(SCOPE), **kwargs)
        return self.plans.fetch_plan(plan.plan_id)

    def external_update(self, **fields: Any) -> None:
        """Change fields the way an approval actor does: conditionally."""
        doc = self.plans.fetch_plan(PLAN_ID)
        doc.update(fields)
        self.raw_conditional_put(doc)

    def raw_conditional_put(self, doc: dict[str, Any]) -> None:
        """Replace doc at the ``_rev`` it carries, bypassing the plan store."""
        raise NotImplementedError

    # ----- plan generation -----

    def test_every_save_assigns_a_fresh_generation(self):
        first = self.save()
        second = self.save()
        self.assertTrue(first["plan_generation"])
        self.assertTrue(second["plan_generation"])
        self.assertNotEqual(first["plan_generation"], second["plan_generation"])

    def test_approval_style_update_keeps_the_generation(self):
        doc = self.save(auto_run=False)
        self.external_update(status="approved", run_token=1)
        self.assertEqual(
            self.plans.fetch_plan(PLAN_ID)["plan_generation"], doc["plan_generation"]
        )

    def test_recreated_plan_does_not_reuse_the_deleted_generation(self):
        doc = self.save()
        self.assertTrue(self.plans.delete_plan(PLAN_ID))
        recreated = self.save()
        self.assertNotEqual(recreated["plan_generation"], doc["plan_generation"])

    # ----- competing save_plan -----

    def test_competing_creation_loser_raises_and_keeps_winner(self):
        self.after_next_read(lambda: self.save(make_plan(PLAN_ID, message="winner")))
        with self.assertRaises(RevisionConflictError):
            self.save(make_plan(PLAN_ID, message="loser"))

        doc = self.plans.fetch_plan(PLAN_ID)
        self.assertEqual(doc["plan"]["steps"][0]["params"]["message"], "winner")
        self.assertEqual(self.write_count(doc), 1)

    def test_competing_regeneration_loser_raises_without_replaying(self):
        self.save(make_plan(PLAN_ID, message="original"))
        self.after_next_read(lambda: self.save(make_plan(PLAN_ID, message="winner")))
        with self.assertRaises(RevisionConflictError):
            self.save(make_plan(PLAN_ID, message="loser"))

        doc = self.plans.fetch_plan(PLAN_ID)
        self.assertEqual(doc["plan"]["steps"][0]["params"]["message"], "winner")
        # original + winner only: the loser neither overwrote nor retried.
        self.assertEqual(self.write_count(doc), 2)

    def test_stale_regeneration_cannot_overwrite_a_finalized_result(self):
        doc = self.save()
        request = finalization_for(doc)
        finalized = []
        self.after_next_read(
            lambda: finalized.append(self.plans.finalize_execution(request))
        )

        with self.assertRaises(RevisionConflictError):
            self.save(make_plan(PLAN_ID, message="stale regeneration"))

        self.assertEqual(finalized[0].status, FinalizationStatus.COMMITTED)
        stored = self.plans.fetch_plan(PLAN_ID)
        self.assertEqual(stored["executed_run_token"], 0)
        self.assertEqual(
            stored["last_finalized_execution"]["execution_id"], request.execution_id
        )

    # ----- update_executed_token -----

    def test_token_update_reapplies_over_a_concurrent_rerun_request(self):
        self.save()
        self.after_next_read(lambda: self.external_update(run_token=1))
        self.assertTrue(self.plans.update_executed_token(PLAN_ID, 0, max_retries=2))

        doc = self.plans.fetch_plan(PLAN_ID)
        self.assertEqual(doc["executed_run_token"], 0)
        self.assertEqual(doc["run_token"], 1)  # the rerun request survived
        self.assertTrue(is_plan_eligible(doc))

    # ----- finalization: recording -----

    def test_success_records_outcome_and_token_in_one_write(self):
        doc = self.save()
        request = finalization_for(doc)

        result = self.plans.finalize_execution(request)

        self.assertEqual(result.status, FinalizationStatus.COMMITTED)
        self.assertTrue(result.recorded)
        stored = self.plans.fetch_plan(PLAN_ID)
        self.assertEqual(self.write_count(stored), self.write_count(doc) + 1)
        self.assertEqual(stored["executed_run_token"], 0)
        record = stored["last_finalized_execution"]
        self.assertEqual(
            {key: record[key] for key in request.identity()}, request.identity()
        )
        self.assertEqual(record["report"], request.report)
        self.assertEqual(stored["last_executed_at"], record["finalized_at"])
        self.assertFalse(is_plan_eligible(stored))
        self.assertEqual(
            self.plans.get_plan_summary(PLAN_ID)["last_finalized_outcome"], "succeeded"
        )

    def test_drained_continuation_failure_consumes_request_until_rerun(self):
        doc = self.save(continuation_plan(PLAN_ID))
        result = self.plans.finalize_execution(
            finalization_for(doc, ending=DRAINED_FAILURE)
        )

        self.assertEqual(result.status, FinalizationStatus.COMMITTED)
        stored = self.plans.fetch_plan(PLAN_ID)
        self.assertEqual(stored["executed_run_token"], 0)
        self.assertEqual(stored["last_finalized_execution"]["outcome"], "failed")
        self.assertFalse(is_plan_eligible(stored))
        self.assertEqual(stored["status"], "approved")

        # Only an explicit new request makes it eligible again, same generation.
        self.external_update(run_token=1)
        rerun = self.plans.fetch_plan(PLAN_ID)
        self.assertTrue(is_plan_eligible(rerun))
        self.assertEqual(rerun["plan_generation"], doc["plan_generation"])

    def test_preflight_rejection_consumes_request_with_its_diagnostic(self):
        doc = self.save(continuation_plan(PLAN_ID))
        result = self.plans.finalize_execution(
            finalization_for(doc, ending=PREFLIGHT_REJECTED)
        )

        self.assertEqual(result.status, FinalizationStatus.COMMITTED)
        stored = self.plans.fetch_plan(PLAN_ID)
        self.assertFalse(is_plan_eligible(stored))
        record = stored["last_finalized_execution"]
        self.assertEqual(record["termination_reason"], "preflight_rejected")
        self.assertEqual(
            record["report"]["diagnostic"]["message"],
            "dependency cycle: s1 -> s2 -> s1",
        )

    def test_newer_run_token_is_preserved_and_stays_pending(self):
        doc = self.save()
        request = finalization_for(doc)  # captured at run_token 0
        self.external_update(run_token=1)

        result = self.plans.finalize_execution(request)

        self.assertEqual(result.status, FinalizationStatus.COMMITTED)
        stored = self.plans.fetch_plan(PLAN_ID)
        self.assertEqual(stored["run_token"], 1)
        self.assertEqual(stored["executed_run_token"], 0)
        self.assertTrue(is_plan_eligible(stored))

    # ----- finalization: idempotency -----

    def test_replaying_a_committed_completion_writes_nothing(self):
        request = finalization_for(self.save())
        self.plans.finalize_execution(request)
        committed = self.plans.fetch_plan(PLAN_ID)

        result = self.plans.finalize_execution(request)

        self.assertEqual(result.status, FinalizationStatus.ALREADY_COMMITTED)
        self.assertTrue(result.recorded)
        self.assertEqual(self.plans.fetch_plan(PLAN_ID), committed)

    def test_uncertain_commit_is_resolved_by_calling_again(self):
        request = finalization_for(self.save())
        self.fail_after_next_write()

        with self.assertRaises(PlanStoreError):
            self.plans.finalize_execution(request)
        landed = self.plans.fetch_plan(PLAN_ID)
        self.assertEqual(landed["executed_run_token"], 0)

        result = self.plans.finalize_execution(request)
        self.assertEqual(result.status, FinalizationStatus.ALREADY_COMMITTED)
        self.assertEqual(self.plans.fetch_plan(PLAN_ID), landed)

    def test_other_attempt_with_equal_generation_token_and_outcome_is_not_mine(self):
        doc = self.save(continuation_plan(PLAN_ID))
        interrupted = finalization_for(doc, ending=DRAINED_FAILURE, execution_id="a")
        retry = finalization_for(doc, ending=DRAINED_FAILURE, execution_id="b")
        self.assertEqual(
            self.plans.finalize_execution(retry).status, FinalizationStatus.COMMITTED
        )
        committed = self.plans.fetch_plan(PLAN_ID)

        result = self.plans.finalize_execution(interrupted)

        self.assertEqual(result.status, FinalizationStatus.SUPERSEDED)
        self.assertEqual(result.reason, SupersessionReason.REQUEST_ALREADY_FINALIZED)
        self.assertFalse(result.recorded)
        self.assertEqual(self.plans.fetch_plan(PLAN_ID), committed)

    # ----- finalization: supersession -----

    def test_stale_worker_holding_a_fresh_revision_is_rejected_on_generation(self):
        request = finalization_for(self.save())
        regenerated = self.save()  # new generation; tokens reset to the same 0

        result = self.plans.finalize_execution(request)

        self.assertEqual(result.status, FinalizationStatus.SUPERSEDED)
        self.assertEqual(result.reason, SupersessionReason.GENERATION_CHANGED)
        self.assertEqual(self.plans.fetch_plan(PLAN_ID), regenerated)

    def test_old_worker_cannot_finalize_a_deleted_and_recreated_plan(self):
        request = finalization_for(self.save())
        self.plans.delete_plan(PLAN_ID)
        missing = self.plans.finalize_execution(request)
        self.assertEqual(missing.reason, SupersessionReason.PLAN_MISSING)

        recreated = self.save()
        result = self.plans.finalize_execution(request)
        self.assertEqual(result.reason, SupersessionReason.GENERATION_CHANGED)
        self.assertEqual(self.plans.fetch_plan(PLAN_ID), recreated)

    def test_authority_or_owner_change_supersedes(self):
        request = finalization_for(
            self.save(execution_authority="run_once", execution_owner="run_once:a")
        )
        self.external_update(execution_owner="run_once:b")
        self.assertEqual(
            self.plans.finalize_execution(request).reason,
            SupersessionReason.AUTHORITY_CHANGED,
        )

        self.external_update(execution_authority="daemon", execution_owner=None)
        changed = self.plans.fetch_plan(PLAN_ID)
        result = self.plans.finalize_execution(request)
        self.assertEqual(result.reason, SupersessionReason.AUTHORITY_CHANGED)
        self.assertEqual(self.plans.fetch_plan(PLAN_ID), changed)

    def test_executed_token_is_never_lowered(self):
        doc = self.save()
        older = finalization_for(doc, execution_id="older")  # run_token 0
        self.external_update(run_token=1)
        newer = finalization_for(self.plans.fetch_plan(PLAN_ID), execution_id="newer")
        self.assertEqual(
            self.plans.finalize_execution(newer).status, FinalizationStatus.COMMITTED
        )
        committed = self.plans.fetch_plan(PLAN_ID)

        result = self.plans.finalize_execution(older)

        self.assertEqual(result.reason, SupersessionReason.REQUEST_ALREADY_FINALIZED)
        self.assertEqual(self.plans.fetch_plan(PLAN_ID), committed)
        self.assertEqual(committed["executed_run_token"], 1)

    def test_legacy_document_without_generation_cannot_be_finalized(self):
        doc = self.save()
        request = finalization_for(doc)
        stripped = {k: v for k, v in doc.items() if k != "plan_generation"}
        self.raw_conditional_put(stripped)

        result = self.plans.finalize_execution(request)

        self.assertEqual(result.reason, SupersessionReason.GENERATION_CHANGED)

    # ----- finalization: races between read and write -----

    def test_harmless_concurrent_write_returns_retryable_conflict(self):
        request = finalization_for(self.save())
        self.after_next_read(lambda: self.external_update(run_token=1))

        result = self.plans.finalize_execution(request)

        self.assertEqual(result.status, FinalizationStatus.CONFLICT)
        self.assertFalse(result.recorded)
        raced = self.plans.fetch_plan(PLAN_ID)
        self.assertEqual(raced["executed_run_token"], -1)
        self.assertNotIn("last_finalized_execution", raced)

        retried = self.plans.finalize_execution(request)
        self.assertEqual(retried.status, FinalizationStatus.COMMITTED)
        stored = self.plans.fetch_plan(PLAN_ID)
        self.assertEqual(stored["executed_run_token"], 0)
        self.assertEqual(stored["run_token"], 1)

    def test_regeneration_between_read_and_write_supersedes(self):
        request = finalization_for(self.save())
        self.after_next_read(lambda: self.save(make_plan(PLAN_ID, message="new")))

        result = self.plans.finalize_execution(request)

        self.assertEqual(result.reason, SupersessionReason.GENERATION_CHANGED)
        stored = self.plans.fetch_plan(PLAN_ID)
        self.assertNotIn("last_finalized_execution", stored)
        self.assertEqual(stored["plan"]["steps"][0]["params"]["message"], "new")

    def test_own_completion_landing_between_read_and_write_is_already_committed(self):
        request = finalization_for(self.save())
        self.after_next_read(lambda: self.plans.finalize_execution(request))

        result = self.plans.finalize_execution(request)

        self.assertEqual(result.status, FinalizationStatus.ALREADY_COMMITTED)

    def test_deletion_between_read_and_write_supersedes(self):
        request = finalization_for(self.save())
        self.after_next_read(lambda: self.plans.delete_plan(PLAN_ID))

        result = self.plans.finalize_execution(request)

        self.assertEqual(result.reason, SupersessionReason.PLAN_MISSING)
        self.assertIsNone(self.plans.fetch_plan(PLAN_ID))

    def test_backend_read_failure_is_a_plan_store_error(self):
        request = finalization_for(self.save())
        self.fail_next_read()
        with self.assertRaises(PlanStoreError) as ctx:
            self.plans.finalize_execution(request)
        self.assertIsNotNone(ctx.exception.__cause__)

    # ----- legacy generation initialization -----

    def _legacy_document(self) -> dict[str, Any]:
        """Store and return a plan document written before generations existed."""
        doc = build_plan_document(make_plan(PLAN_ID), "test_realm", dict(SCOPE))
        del doc["plan_generation"]
        doc["executed_run_token"] = 0
        doc["run_token"] = 1
        self.raw_put(doc)
        return self.plans.fetch_plan(PLAN_ID)

    def test_legacy_document_gets_a_generation_and_nothing_else_changes(self):
        legacy = self._legacy_document()

        initialized = self.plans.ensure_plan_generation(PLAN_ID)

        self.assertTrue(initialized["plan_generation"])
        stored = self.plans.fetch_plan(PLAN_ID)
        self.assertEqual(stored, initialized)
        self.assertEqual(
            {k: v for k, v in stored.items() if k not in ("_rev", "plan_generation")},
            {k: v for k, v in legacy.items() if k != "_rev"},
        )
        self.assertEqual(self.write_count(stored), self.write_count(legacy) + 1)

    def test_document_with_a_generation_is_returned_without_a_write(self):
        doc = self.save()
        self.assertEqual(self.plans.ensure_plan_generation(PLAN_ID), doc)
        self.assertEqual(self.plans.fetch_plan(PLAN_ID), doc)

    def test_missing_plan_has_no_generation_to_ensure(self):
        self.assertIsNone(self.plans.ensure_plan_generation("pln_missing"))

    def test_legacy_init_race_adopts_the_winners_generation(self):
        legacy = self._legacy_document()
        winner: dict[str, Any] = {}
        self.after_next_read(
            lambda: winner.update(self.plans.ensure_plan_generation(PLAN_ID))
        )

        loser = self.plans.ensure_plan_generation(PLAN_ID)

        self.assertEqual(loser["plan_generation"], winner["plan_generation"])
        stored = self.plans.fetch_plan(PLAN_ID)
        self.assertEqual(stored["plan_generation"], winner["plan_generation"])
        self.assertEqual(self.write_count(stored), self.write_count(legacy) + 1)

    def test_legacy_init_reapplies_over_an_unrelated_concurrent_change(self):
        self._legacy_document()
        self.after_next_read(lambda: self.external_update(run_token=2))

        initialized = self.plans.ensure_plan_generation(PLAN_ID)

        self.assertTrue(initialized["plan_generation"])
        self.assertEqual(initialized["run_token"], 2)
        self.assertEqual(self.plans.fetch_plan(PLAN_ID), initialized)


class TestSQLiteConditionalUpdates(PlanStoreContract, unittest.TestCase):
    """The contract against SQLitePlanStore on a real temporary database."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.store = SQLiteInternalStore(Path(self._tmp.name) / "ygg.sqlite3")
        self.plans = SQLitePlanStore(self.store)

    def tearDown(self):
        self._tmp.cleanup()

    def after_next_read(self, action):
        real = self.store.get_document

        def read_then_act(namespace, doc_id):
            doc = real(namespace, doc_id)
            self.store.get_document = real  # one-shot
            action()
            return doc

        self.store.get_document = read_then_act

    def fail_after_next_write(self):
        real = self.store.put_document

        def write_then_fail(*args, **kwargs):
            self.store.put_document = real  # one-shot
            real(*args, **kwargs)
            raise sqlite3.OperationalError("disk I/O error")

        self.store.put_document = write_then_fail

    def fail_next_read(self):
        real = self.store.get_document

        def fail(namespace, doc_id):
            self.store.get_document = real  # one-shot
            raise sqlite3.OperationalError("database is locked")

        self.store.get_document = fail

    def raw_put(self, doc):
        self.store.put_document("plans", doc["_id"], doc, bump_plan_seq=True)

    def raw_conditional_put(self, doc):
        self.store.put_document(
            "plans", doc["_id"], doc, bump_plan_seq=True, expected_rev=doc["_rev"]
        )

    def write_count(self, doc):
        return int(doc["_rev"])


class TestCouchConditionalUpdates(PlanStoreContract, unittest.TestCase):
    """The contract against PlanDBManager on a revision-enforcing fake client."""

    def setUp(self):
        patch_api_exception(self)
        self.server = FakeCouchServer()
        self.plans = plan_db_manager_on(self.server)

    def after_next_read(self, action):
        self.server.after_read.append(action)

    def fail_after_next_write(self):
        self.server.fail_after_next_write = RequestsConnectionError("reset by peer")

    def fail_next_read(self):
        self.server.fail_next_read = FakeApiException(503, "unavailable")

    def raw_put(self, doc):
        body = dict(doc)
        body.pop("_rev", None)
        self.server.put_document(db="yggdrasil_plans", doc_id=doc["_id"], document=body)

    def raw_conditional_put(self, doc):
        self.server.put_document(db="yggdrasil_plans", doc_id=doc["_id"], document=doc)

    def write_count(self, doc):
        return int(doc["_rev"].split("-")[0])


if __name__ == "__main__":
    unittest.main()
