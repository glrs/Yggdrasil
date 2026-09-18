"""SQLite conditional writes and plan-store races on real database files.

The low-level tests pin the two write conditions of
``SQLiteInternalStore.put_document`` and prove the condition is checked inside
the write transaction. The race tests run real threads and hold each one at a
barrier right after it reads, so every racer acts on the same state before any
of them writes.
"""

from __future__ import annotations

import sqlite3
import tempfile
import threading
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import patch

from lib.core_utils.plan_eligibility import is_plan_eligible
from lib.storage.errors import PlanStoreError, RevisionConflictError
from lib.storage.plan_documents import build_plan_document
from lib.storage.plan_updates import FinalizationStatus
from lib.storage.sqlite import SQLiteInternalStore, SQLitePlanStore
from tests.plan_store_support import SCOPE, finalization_for, make_plan

PLAN_ID = "pln_test_P1_v1"
THREAD_TIMEOUT = 20.0


class _GatedConnection:
    """Forwards to a real connection; signals just before a write transaction."""

    def __init__(self, conn: sqlite3.Connection, begin_reached: threading.Event):
        self._conn = conn
        self._begin_reached = begin_reached

    def execute(self, sql: str, *params: Any) -> sqlite3.Cursor:
        if sql == "BEGIN IMMEDIATE":
            self._begin_reached.set()
        return self._conn.execute(sql, *params)

    def close(self) -> None:
        self._conn.close()


class _GatedStore(SQLiteInternalStore):
    """Store whose connections signal when a write transaction is about to begin."""

    begin_reached: threading.Event | None = None

    def _connect(self):  # type: ignore[override]
        conn = super()._connect()
        if self.begin_reached is None:
            return conn
        return _GatedConnection(conn, self.begin_reached)


class _ReadBarrierStore(SQLiteInternalStore):
    """Store that holds each thread's first read at a barrier, once armed."""

    barrier: threading.Barrier | None = None

    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self._held = threading.local()

    def get_document(self, namespace, doc_id):
        doc = super().get_document(namespace, doc_id)
        if self.barrier is not None and not getattr(self._held, "done", False):
            self._held.done = True
            self.barrier.wait()
        return doc


def _row(path: Path, doc_id: str) -> tuple[Any, ...] | None:
    """Read a plan row's stored state directly."""
    conn = sqlite3.connect(path)
    try:
        return conn.execute(
            "SELECT revision, change_seq, deleted, body_json, updated_at "
            "FROM documents WHERE namespace = 'plans' AND document_id = ?",
            (doc_id,),
        ).fetchone()
    finally:
        conn.close()


def _race(*writers: Callable[[], Any]) -> list[tuple[str, Any]]:
    """Start writers together on threads; return ("ok", value) or ("conflict", exc)."""
    start = threading.Barrier(len(writers), timeout=THREAD_TIMEOUT)
    results: list[tuple[str, Any]] = [("unfinished", None)] * len(writers)

    def run(index: int, writer: Callable[[], Any]) -> None:
        start.wait()
        try:
            results[index] = ("ok", writer())
        except RevisionConflictError as exc:
            results[index] = ("conflict", exc)
        except BaseException as exc:  # surfaced by the caller's assertions
            results[index] = ("error", exc)

    threads = [
        threading.Thread(target=run, args=(index, writer))
        for index, writer in enumerate(writers)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(THREAD_TIMEOUT)
        if thread.is_alive():
            raise AssertionError("a racing writer did not finish")
    return results


class _SQLiteTestBase(unittest.TestCase):
    """Temporary database file per test."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.path = Path(self._tmp.name) / "ygg.sqlite3"

    def tearDown(self):
        self._tmp.cleanup()


class TestPutDocumentConditions(_SQLiteTestBase):
    """Create-if-absent and expected-revision replace."""

    def setUp(self):
        super().setUp()
        self.store = SQLiteInternalStore(self.path)

    def test_create_if_absent_creates_a_new_document(self):
        rev = self.store.put_document("plans", "p1", {"v": 1}, expected_rev=None)
        self.assertEqual(rev, "1")
        self.assertEqual(self.store.get_document("plans", "p1")["_rev"], "1")

    def test_create_if_absent_rejects_a_live_document(self):
        self.store.put_document("plans", "p1", {"v": 1})
        with self.assertRaises(RevisionConflictError) as ctx:
            self.store.put_document("plans", "p1", {"v": 2}, expected_rev=None)
        self.assertEqual(ctx.exception.doc_id, "p1")
        self.assertIsNone(ctx.exception.expected_rev)
        self.assertEqual(self.store.get_document("plans", "p1")["v"], 1)

    def test_create_over_a_tombstone_never_reuses_a_revision(self):
        self.store.put_document("plans", "p1", {"v": 1})
        self.store.delete_document("plans", "p1")

        rev = self.store.put_document("plans", "p1", {"v": 2}, expected_rev=None)

        # A reader still holding revision "1" must not match the new document.
        self.assertEqual(rev, "3")
        with self.assertRaises(RevisionConflictError):
            self.store.put_document("plans", "p1", {"v": 3}, expected_rev="1")

    def test_replace_at_the_current_revision_applies(self):
        self.store.put_document("plans", "p1", {"v": 1})
        rev = self.store.put_document("plans", "p1", {"v": 2}, expected_rev="1")
        self.assertEqual(rev, "2")
        self.assertEqual(self.store.get_document("plans", "p1")["v"], 2)

    def test_replace_at_a_stale_revision_is_rejected(self):
        self.store.put_document("plans", "p1", {"v": 1})
        self.store.put_document("plans", "p1", {"v": 2})
        with self.assertRaises(RevisionConflictError) as ctx:
            self.store.put_document("plans", "p1", {"v": 3}, expected_rev="1")
        self.assertEqual(ctx.exception.expected_rev, "1")
        self.assertIn("revision 2", str(ctx.exception))

    def test_replace_of_a_missing_or_deleted_document_is_rejected(self):
        with self.assertRaises(RevisionConflictError):
            self.store.put_document("plans", "p1", {"v": 1}, expected_rev="1")
        self.assertIsNone(_row(self.path, "p1"))

        self.store.put_document("plans", "p2", {"v": 1})
        self.store.delete_document("plans", "p2")
        with self.assertRaises(RevisionConflictError):
            self.store.put_document("plans", "p2", {"v": 2}, expected_rev="1")
        self.assertIsNone(self.store.get_document("plans", "p2"))

    def test_rev_inside_the_body_is_not_a_condition(self):
        self.store.put_document("plans", "p1", {"v": 1})
        self.store.put_document("plans", "p1", {"v": 2})
        rev = self.store.put_document("plans", "p1", {"_rev": "1", "v": 3})
        self.assertEqual(rev, "3")

    def test_rejected_writes_change_nothing(self):
        self.store.put_document("plans", "live", {"v": 1}, bump_plan_seq=True)
        self.store.put_document("plans", "live", {"v": 2}, bump_plan_seq=True)
        self.store.put_document("plans", "gone", {"v": 1}, bump_plan_seq=True)
        self.store.delete_document("plans", "gone", bump_plan_seq=True)

        rejected = {
            "create over a live document": ("live", None),
            "replace at a stale revision": ("live", "1"),
            "replace of a deleted document": ("gone", "1"),
        }
        for label, (doc_id, expected_rev) in rejected.items():
            with self.subTest(label):
                row_before = _row(self.path, doc_id)
                seq_before = self.store.current_plan_seq()

                with self.assertRaises(RevisionConflictError):
                    self.store.put_document(
                        "plans",
                        doc_id,
                        {"v": "rejected"},
                        bump_plan_seq=True,
                        expected_rev=expected_rev,
                    )

                self.assertEqual(_row(self.path, doc_id), row_before)
                self.assertEqual(self.store.current_plan_seq(), seq_before)

    def test_unconditional_upsert_is_unchanged_for_other_namespaces(self):
        self.store.put_document("checkpoints", "c1", {"value": "1"})
        rev = self.store.put_document("checkpoints", "c1", {"value": "2"})
        self.assertEqual(rev, "2")
        self.assertEqual(self.store.get_document("checkpoints", "c1")["value"], "2")


class TestConditionIsCheckedInsideTheWriteTransaction(_SQLiteTestBase):
    """A write committed while the writer waits for the lock must be detected."""

    def test_write_committed_before_the_writer_gets_the_lock_is_a_conflict(self):
        store = _GatedStore(self.path)
        store.put_document("plans", "p1", {"v": "original"})

        holder = sqlite3.connect(self.path, isolation_level=None)
        try:
            holder.execute("BEGIN IMMEDIATE")
            store.begin_reached = threading.Event()
            outcome: dict[str, Any] = {}

            def stale_writer() -> None:
                try:
                    outcome["rev"] = store.put_document(
                        "plans", "p1", {"v": "stale"}, expected_rev="1"
                    )
                except RevisionConflictError as exc:
                    outcome["conflict"] = exc

            writer = threading.Thread(target=stale_writer)
            writer.start()
            # The writer has done everything it does before its transaction;
            # a condition checked outside the transaction has already passed.
            self.assertTrue(store.begin_reached.wait(THREAD_TIMEOUT))
            holder.execute(
                "UPDATE documents SET revision = 2, "
                'body_json = \'{"_id": "p1", "v": "concurrent"}\' '
                "WHERE namespace = 'plans' AND document_id = 'p1'"
            )
            holder.execute("COMMIT")
            writer.join(THREAD_TIMEOUT)
            self.assertFalse(writer.is_alive())
        finally:
            holder.close()

        self.assertIn("conflict", outcome)
        doc = SQLiteInternalStore(self.path).get_document("plans", "p1")
        self.assertEqual((doc["v"], doc["_rev"]), ("concurrent", "2"))


class TestConcurrentConditionalWrites(_SQLiteTestBase):
    """Threads racing the same conditional write: exactly one wins."""

    def setUp(self):
        super().setUp()
        self.store = SQLiteInternalStore(self.path)

    def test_concurrent_creates_have_exactly_one_winner(self):
        results = _race(
            *(
                lambda i=i: self.store.put_document(
                    "plans", "p1", {"writer": i}, bump_plan_seq=True, expected_rev=None
                )
                for i in range(8)
            )
        )

        winners = [i for i, (kind, _) in enumerate(results) if kind == "ok"]
        self.assertEqual(len(winners), 1, results)
        self.assertEqual(sorted(kind for kind, _ in results), ["conflict"] * 7 + ["ok"])
        doc = self.store.get_document("plans", "p1")
        self.assertEqual((doc["writer"], doc["_rev"]), (winners[0], "1"))
        self.assertEqual(self.store.current_plan_seq(), 1)

    def test_concurrent_replaces_of_one_revision_have_exactly_one_winner(self):
        self.store.put_document("plans", "p1", {"writer": None}, bump_plan_seq=True)
        results = _race(
            *(
                lambda i=i: self.store.put_document(
                    "plans", "p1", {"writer": i}, bump_plan_seq=True, expected_rev="1"
                )
                for i in range(8)
            )
        )

        winners = [i for i, (kind, _) in enumerate(results) if kind == "ok"]
        self.assertEqual(len(winners), 1, results)
        self.assertEqual(sorted(kind for kind, _ in results), ["conflict"] * 7 + ["ok"])
        doc = self.store.get_document("plans", "p1")
        self.assertEqual((doc["writer"], doc["_rev"]), (winners[0], "2"))
        self.assertEqual(self.store.current_plan_seq(), 2)


class TestPlanStoreThreadRaces(_SQLiteTestBase):
    """Plan-store operations racing on threads after reading the same state."""

    def setUp(self):
        super().setUp()
        self.store = _ReadBarrierStore(self.path)
        self.plans = SQLitePlanStore(self.store)

    def race_after_reads(self, *writers: Callable[[], Any]) -> list[tuple[str, Any]]:
        """Race writers, holding each one's first read until all have read."""
        self.store.barrier = threading.Barrier(len(writers), timeout=THREAD_TIMEOUT)
        try:
            return _race(*writers)
        finally:
            self.store.barrier = None

    def save(self, message: str) -> str:
        return self.plans.save_plan(
            make_plan(PLAN_ID, message=message),
            "test_realm",
            dict(SCOPE),
            auto_run=True,
        )

    def test_competing_first_saves_have_one_winner(self):
        results = self.race_after_reads(lambda: self.save("a"), lambda: self.save("b"))

        self.assertEqual(sorted(kind for kind, _ in results), ["conflict", "ok"])
        winner = "ab"[[kind for kind, _ in results].index("ok")]
        doc = self.plans.fetch_plan(PLAN_ID)
        self.assertEqual(doc["plan"]["steps"][0]["params"]["message"], winner)
        self.assertEqual(doc["_rev"], "1")

    def test_competing_regenerations_have_one_winner_and_no_replay(self):
        self.save("original")
        seq_before = self.store.current_plan_seq()
        results = self.race_after_reads(lambda: self.save("a"), lambda: self.save("b"))

        self.assertEqual(sorted(kind for kind, _ in results), ["conflict", "ok"])
        winner = "ab"[[kind for kind, _ in results].index("ok")]
        doc = self.plans.fetch_plan(PLAN_ID)
        self.assertEqual(doc["plan"]["steps"][0]["params"]["message"], winner)
        self.assertEqual(doc["_rev"], "2")
        self.assertEqual(self.store.current_plan_seq(), seq_before + 1)

    def test_concurrent_legacy_generation_init_writes_once(self):
        legacy = build_plan_document(make_plan(PLAN_ID), "test_realm", dict(SCOPE))
        del legacy["plan_generation"]
        self.store.put_document("plans", PLAN_ID, legacy, bump_plan_seq=True)

        results = self.race_after_reads(
            lambda: self.plans.ensure_plan_generation(PLAN_ID),
            lambda: self.plans.ensure_plan_generation(PLAN_ID),
        )

        self.assertEqual([kind for kind, _ in results], ["ok", "ok"])
        generations = {doc["plan_generation"] for _, doc in results}
        self.assertEqual(len(generations), 1)
        stored = self.plans.fetch_plan(PLAN_ID)
        self.assertEqual({stored["plan_generation"]}, generations)
        self.assertEqual(stored["_rev"], "2")

    def test_finalization_racing_a_rerun_request_keeps_both(self):
        self.save("original")
        request = finalization_for(self.plans.fetch_plan(PLAN_ID))

        def finalize() -> FinalizationStatus:
            for _ in range(3):
                result = self.plans.finalize_execution(request)
                if result.status is not FinalizationStatus.CONFLICT:
                    return result.status
            raise AssertionError("finalization kept conflicting")

        def request_rerun() -> None:
            for _ in range(3):
                doc = self.store.get_document("plans", PLAN_ID)
                doc["run_token"] = 1
                try:
                    self.store.put_document(
                        "plans",
                        PLAN_ID,
                        doc,
                        bump_plan_seq=True,
                        expected_rev=doc["_rev"],
                    )
                    return
                except RevisionConflictError:
                    continue
            raise AssertionError("rerun request kept conflicting")

        results = self.race_after_reads(finalize, request_rerun)

        self.assertEqual(results[0], ("ok", FinalizationStatus.COMMITTED))
        self.assertEqual(results[1][0], "ok")
        doc = self.plans.fetch_plan(PLAN_ID)
        self.assertEqual((doc["executed_run_token"], doc["run_token"]), (0, 1))
        self.assertEqual(
            doc["last_finalized_execution"]["execution_id"], request.execution_id
        )
        self.assertTrue(is_plan_eligible(doc))  # the newer request is pending

    def test_token_update_racing_a_rerun_request_keeps_both(self):
        self.save("original")

        def request_rerun() -> None:
            doc = self.store.get_document("plans", PLAN_ID)
            doc["run_token"] = 1
            try:
                self.store.put_document(
                    "plans", PLAN_ID, doc, bump_plan_seq=True, expected_rev=doc["_rev"]
                )
            except RevisionConflictError:
                doc = self.store.get_document("plans", PLAN_ID)
                doc["run_token"] = 1
                self.store.put_document(
                    "plans", PLAN_ID, doc, bump_plan_seq=True, expected_rev=doc["_rev"]
                )

        results = self.race_after_reads(
            lambda: self.plans.update_executed_token(PLAN_ID, 0), request_rerun
        )

        self.assertEqual(results[0], ("ok", True))
        self.assertEqual(results[1][0], "ok")
        doc = self.plans.fetch_plan(PLAN_ID)
        self.assertEqual((doc["executed_run_token"], doc["run_token"]), (0, 1))


class TestBoundedRereads(_SQLiteTestBase):
    """Operations that reread after a lost race stop at their bound."""

    def setUp(self):
        super().setUp()
        self.store = SQLiteInternalStore(self.path)
        self.plans = SQLitePlanStore(self.store)

    def interfere_after_every_read(self, change: Callable[[dict[str, Any]], None]):
        """After each plan read, commit change on top of what was read."""
        real = self.store.get_document
        writes = []

        def read_then_interfere(namespace, doc_id):
            doc = real(namespace, doc_id)
            if doc is not None:
                concurrent = dict(doc)
                change(concurrent)
                self.store.put_document(
                    namespace, doc_id, concurrent, expected_rev=doc["_rev"]
                )
                writes.append(doc_id)
            return doc

        self.store.get_document = read_then_interfere
        return writes

    def test_token_update_gives_up_after_max_retries(self):
        self.plans.save_plan(make_plan(PLAN_ID), "test_realm", dict(SCOPE))
        writes = self.interfere_after_every_read(
            lambda doc: doc.update(notes=f"edit {doc['_rev']}")
        )

        self.assertFalse(self.plans.update_executed_token(PLAN_ID, 0, max_retries=2))

        self.assertEqual(len(writes), 2)
        del self.store.get_document
        self.assertEqual(self.plans.fetch_plan(PLAN_ID)["executed_run_token"], -1)

    def test_legacy_generation_init_gives_up_after_three_attempts(self):
        legacy = build_plan_document(make_plan(PLAN_ID), "test_realm", dict(SCOPE))
        del legacy["plan_generation"]
        self.store.put_document("plans", PLAN_ID, legacy)
        writes = self.interfere_after_every_read(
            lambda doc: doc.update(notes=f"edit {doc['_rev']}")
        )

        with self.assertRaises(RevisionConflictError):
            self.plans.ensure_plan_generation(PLAN_ID)

        self.assertEqual(len(writes), 3)
        del self.store.get_document
        self.assertNotIn("plan_generation", self.plans.fetch_plan(PLAN_ID))


class TestBackendFailureClassification(_SQLiteTestBase):
    """Which SQLite failures a caller may retry."""

    def setUp(self):
        super().setUp()
        self.store = SQLiteInternalStore(self.path)
        self.plans = SQLitePlanStore(self.store)
        self.plans.save_plan(
            make_plan(PLAN_ID), "test_realm", dict(SCOPE), auto_run=True
        )
        self.request = finalization_for(self.plans.fetch_plan(PLAN_ID))

    def test_only_contention_is_offered_for_retry(self):
        cases = {
            "locked by another writer": (
                sqlite3.OperationalError("database is locked"),
                True,
            ),
            "busy": (sqlite3.OperationalError("database is busy"), True),
            "corrupt file": (sqlite3.DatabaseError("file is not a database"), False),
            "schema gone": (
                sqlite3.OperationalError("no such table: documents"),
                False,
            ),
            "failing disk": (sqlite3.OperationalError("disk I/O error"), False),
        }
        for label, (error, retryable) in cases.items():
            with self.subTest(label):
                with patch.object(
                    SQLiteInternalStore, "put_document", side_effect=error
                ):
                    with self.assertRaises(PlanStoreError) as ctx:
                        self.plans.finalize_execution(self.request)

                self.assertIs(ctx.exception.__cause__, error)
                self.assertEqual(ctx.exception.retryable, retryable)

        # Nothing was written, so the request can still be finalized.
        self.assertEqual(
            self.plans.finalize_execution(self.request).status,
            FinalizationStatus.COMMITTED,
        )


if __name__ == "__main__":
    unittest.main()
