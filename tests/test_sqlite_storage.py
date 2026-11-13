"""Unit tests for the SQLite internal-storage backend.

Uses real temporary SQLite files (the backend is local and fast); no
CouchDB involvement anywhere.
"""

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from yggdrasil.flow.model import Plan, StepSpec
from yggdrasil.storage.config import SQLiteInternalStorageConfig
from yggdrasil.storage.sqlite import (
    SQLiteCheckpointStore,
    SQLiteInternalStore,
    SQLiteOpsSnapshotSink,
    SQLitePlanStore,
    SQLiteStorageError,
    build_sqlite_bundle,
)
from yggdrasil.watchers.backends.base import Checkpoint


def _make_plan(plan_id: str = "pln_test_P1_v1") -> Plan:
    """Build a minimal but real Plan model."""
    return Plan(
        plan_id=plan_id,
        realm="test_realm",
        scope={"kind": "project", "id": "P1"},
        steps=[
            StepSpec(
                step_id="s1",
                name="echo",
                fn_ref="tests.integration.mock_steps:echo_step",
                params={"message": "hi"},
            )
        ],
    )


class SQLiteStoreTestBase(unittest.TestCase):
    """Shared temp-dir setup."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self._tmp.name) / "internal_state" / "dev" / "ygg.sqlite3"

    def tearDown(self):
        self._tmp.cleanup()


class TestSQLiteLifecycle(SQLiteStoreTestBase):
    """File/dir creation, permissions, and rejection paths."""

    def test_creates_file_and_dirs_with_restrictive_permissions(self):
        store = SQLiteInternalStore(self.db_path)
        self.assertTrue(store.path.is_file())
        self.assertEqual(os.stat(self.db_path).st_mode & 0o777, 0o600)
        self.assertEqual(os.stat(self.db_path.parent).st_mode & 0o777, 0o700)
        self.assertEqual(os.stat(self.db_path.parent.parent).st_mode & 0o777, 0o700)

    def test_reopens_existing_database(self):
        SQLiteInternalStore(self.db_path).put_document("plans", "d1", {"a": 1})
        store2 = SQLiteInternalStore(self.db_path)
        self.assertEqual(store2.get_document("plans", "d1")["a"], 1)

    def test_deleted_database_recreated_fresh(self):
        store = SQLiteInternalStore(self.db_path)
        store.put_document("plans", "d1", {"a": 1}, bump_plan_seq=True)
        self.db_path.unlink()
        store2 = SQLiteInternalStore(self.db_path)
        self.assertIsNone(store2.get_document("plans", "d1"))
        self.assertEqual(store2.current_plan_seq(), 0)

    def test_rejects_symlinked_database(self):
        real = Path(self._tmp.name) / "real.sqlite3"
        real.touch()
        self.db_path.parent.mkdir(parents=True)
        self.db_path.symlink_to(real)
        with self.assertRaises(SQLiteStorageError):
            SQLiteInternalStore(self.db_path)

    def test_rejects_malformed_file(self):
        self.db_path.parent.mkdir(parents=True)
        self.db_path.write_text("this is not a sqlite database, not even close")
        with self.assertRaises(SQLiteStorageError):
            SQLiteInternalStore(self.db_path)

    def test_rejects_unrelated_sqlite_file(self):
        self.db_path.parent.mkdir(parents=True)
        conn = sqlite3.connect(self.db_path)
        conn.execute("CREATE TABLE unrelated (x INTEGER)")
        conn.commit()
        conn.close()
        with self.assertRaises(SQLiteStorageError) as ctx:
            SQLiteInternalStore(self.db_path)
        self.assertIn("application_id", str(ctx.exception))

    def test_rejects_newer_schema_version(self):
        SQLiteInternalStore(self.db_path)
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA user_version = 99")
        conn.close()
        with self.assertRaises(SQLiteStorageError) as ctx:
            SQLiteInternalStore(self.db_path)
        self.assertIn("Upgrade", str(ctx.exception))


class TestSQLiteDocuments(SQLiteStoreTestBase):
    """Low-level document semantics."""

    def setUp(self):
        super().setUp()
        self.store = SQLiteInternalStore(self.db_path)

    def test_get_missing_returns_none(self):
        self.assertIsNone(self.store.get_document("plans", "nope"))

    def test_put_get_roundtrip_injects_rev(self):
        self.store.put_document("plans", "d1", {"x": 1})
        doc = self.store.get_document("plans", "d1")
        self.assertEqual(doc["x"], 1)
        self.assertEqual(doc["_id"], "d1")
        self.assertEqual(doc["_rev"], "1")

    def test_rev_increments_and_is_not_stored_in_body(self):
        self.store.put_document("plans", "d1", {"x": 1})
        doc = self.store.get_document("plans", "d1")
        doc["x"] = 2
        self.store.put_document("plans", "d1", doc)  # carries _rev "1"
        doc2 = self.store.get_document("plans", "d1")
        self.assertEqual(doc2["_rev"], "2")
        self.assertEqual(doc2["x"], 2)

    def test_delete_tombstones_document(self):
        self.store.put_document("plans", "d1", {"x": 1})
        self.assertTrue(self.store.delete_document("plans", "d1"))
        self.assertIsNone(self.store.get_document("plans", "d1"))
        self.assertFalse(self.store.delete_document("plans", "d1"))

    def test_namespaces_are_isolated(self):
        self.store.put_document("plans", "d1", {"ns": "plans"})
        self.store.put_document("checkpoints", "d1", {"ns": "checkpoints"})
        self.assertEqual(self.store.get_document("plans", "d1")["ns"], "plans")
        self.assertEqual(
            self.store.get_document("checkpoints", "d1")["ns"], "checkpoints"
        )

    def test_plan_seq_bumps_only_when_requested(self):
        self.store.put_document("checkpoints", "c1", {"v": 1})
        self.assertEqual(self.store.current_plan_seq(), 0)
        self.store.put_document("plans", "p1", {"v": 1}, bump_plan_seq=True)
        self.assertEqual(self.store.current_plan_seq(), 1)

    def test_touch_advances_metadata_without_rewriting_body(self):
        self.store.put_document(
            "plans",
            "p1",
            {"status": "approved", "executed_run_token": -1},
            bump_plan_seq=True,
        )
        conn = sqlite3.connect(self.db_path)
        before = conn.execute(
            "SELECT revision, change_seq, body_json, updated_at "
            "FROM documents WHERE namespace = 'plans' AND document_id = 'p1'"
        ).fetchone()
        conn.close()

        self.assertTrue(self.store.touch_document("plans", "p1"))

        conn = sqlite3.connect(self.db_path)
        after = conn.execute(
            "SELECT revision, change_seq, body_json, updated_at "
            "FROM documents WHERE namespace = 'plans' AND document_id = 'p1'"
        ).fetchone()
        conn.close()
        self.assertEqual(after[0], before[0] + 1)
        self.assertEqual(after[1], before[1] + 1)
        self.assertEqual(after[2], before[2])
        self.assertGreaterEqual(after[3], before[3])
        self.assertEqual(self.store.current_plan_seq(), after[1])

    def test_touch_missing_or_tombstoned_document_is_noop(self):
        self.assertFalse(self.store.touch_document("plans", "missing"))
        self.assertEqual(self.store.current_plan_seq(), 0)

        self.store.put_document("plans", "p1", {"x": 1}, bump_plan_seq=True)
        self.store.delete_document("plans", "p1", bump_plan_seq=True)
        seq_after_delete = self.store.current_plan_seq()

        self.assertFalse(self.store.touch_document("plans", "p1"))
        self.assertEqual(self.store.current_plan_seq(), seq_after_delete)


class TestSQLitePlanStoreContract(SQLiteStoreTestBase):
    """PlanStore behavioral contract against a real temp SQLite file."""

    def setUp(self):
        super().setUp()
        self.store = SQLiteInternalStore(self.db_path)
        self.plans = SQLitePlanStore(self.store)

    def mark_executed(self, doc_id: str) -> None:
        """Record the plan's request as executed, conditionally, as a writer does."""
        doc = self.plans.fetch_plan(doc_id)
        assert doc is not None, f"plan {doc_id} is not stored"
        doc["executed_run_token"] = doc["run_token"]
        self.store.put_document(
            "plans", doc_id, doc, bump_plan_seq=True, expected_rev=doc["_rev"]
        )

    def test_save_plan_draft_shape(self):
        doc_id = self.plans.save_plan(
            _make_plan(), "test_realm", {"kind": "project", "id": "P1"}
        )
        doc = self.plans.fetch_plan(doc_id)
        self.assertEqual(doc["status"], "draft")
        self.assertEqual(doc["run_token"], 0)
        self.assertEqual(doc["executed_run_token"], -1)
        self.assertEqual(doc["execution_authority"], "daemon")
        self.assertIn("created_at", doc)

    def test_save_plan_auto_run_is_approved(self):
        doc_id = self.plans.save_plan(
            _make_plan(), "test_realm", {"kind": "project", "id": "P1"}, auto_run=True
        )
        self.assertEqual(self.plans.fetch_plan(doc_id)["status"], "approved")

    def test_regeneration_resets_tokens_and_keeps_created_at(self):
        doc_id = self.plans.save_plan(
            _make_plan(), "test_realm", {"kind": "project", "id": "P1"}, auto_run=True
        )
        self.mark_executed(doc_id)
        created_at = self.plans.fetch_plan(doc_id)["created_at"]

        self.plans.save_plan(
            _make_plan(), "test_realm", {"kind": "project", "id": "P1"}
        )
        doc = self.plans.fetch_plan(doc_id)
        self.assertEqual(doc["executed_run_token"], -1)
        self.assertEqual(doc["run_token"], 0)
        self.assertEqual(doc["created_at"], created_at)

    def test_invalid_execution_authority_rejected(self):
        with self.assertRaises(ValueError):
            self.plans.save_plan(
                _make_plan(),
                "test_realm",
                {"kind": "project", "id": "P1"},
                execution_authority="bogus",
            )

    def test_fetch_plan_as_model_roundtrip(self):
        plan = _make_plan()
        doc_id = self.plans.save_plan(
            plan, "test_realm", {"kind": "project", "id": "P1"}
        )
        model = self.plans.fetch_plan_as_model(doc_id)
        self.assertIsNotNone(model)
        self.assertEqual(model.plan_id, plan.plan_id)
        self.assertEqual(model.steps[0].step_id, "s1")

    def test_query_approved_pending_filters_eligibility(self):
        self.plans.save_plan(
            _make_plan("pln_a"), "test_realm", {"kind": "project", "id": "P1"}
        )  # draft — not eligible
        self.plans.save_plan(
            _make_plan("pln_b"),
            "test_realm",
            {"kind": "project", "id": "P1"},
            auto_run=True,
        )  # approved + pending — eligible
        executed = self.plans.save_plan(
            _make_plan("pln_c"),
            "test_realm",
            {"kind": "project", "id": "P1"},
            auto_run=True,
        )
        self.mark_executed(executed)  # executed — not eligible

        eligible = self.plans.query_approved_pending()
        self.assertEqual([d["_id"] for d in eligible], ["pln_b"])

    def test_delete_and_exists(self):
        doc_id = self.plans.save_plan(
            _make_plan(), "test_realm", {"kind": "project", "id": "P1"}
        )
        self.assertTrue(self.plans.plan_exists(doc_id))
        self.assertTrue(self.plans.delete_plan(doc_id))
        self.assertFalse(self.plans.plan_exists(doc_id))
        self.assertFalse(self.plans.delete_plan(doc_id))

    def test_get_plan_summary(self):
        doc_id = self.plans.save_plan(
            _make_plan(), "test_realm", {"kind": "project", "id": "P1"}
        )
        summary = self.plans.get_plan_summary(doc_id)
        self.assertEqual(summary["status"], "draft")
        self.assertEqual(summary["realm"], "test_realm")
        self.assertEqual(summary["run_token"], 0)
        self.assertIsNone(self.plans.get_plan_summary("nope"))


class TestSQLiteCheckpointStore(SQLiteStoreTestBase):
    """CheckpointStore contract."""

    def setUp(self):
        super().setUp()
        self.checkpoints = SQLiteCheckpointStore(SQLiteInternalStore(self.db_path))

    def test_load_missing_returns_none(self):
        self.assertIsNone(self.checkpoints.load("couchdb:projects_db"))

    def test_save_and_load_roundtrip(self):
        self.checkpoints.save(
            Checkpoint(
                backend_key="couchdb:projects_db",
                value="42-abc",
                updated_at="2026-07-17T00:00:00+00:00",
            )
        )
        cp = self.checkpoints.load("couchdb:projects_db")
        self.assertEqual(cp.backend_key, "couchdb:projects_db")
        self.assertEqual(cp.value, "42-abc")

    def test_save_overwrites(self):
        for value in ("1", "2"):
            self.checkpoints.save(
                Checkpoint(backend_key="watcher:PlanWatcher", value=value)
            )
        self.assertEqual(self.checkpoints.load("watcher:PlanWatcher").value, "2")


class TestSQLiteOpsSnapshotSink(SQLiteStoreTestBase):
    """OpsSnapshotSink contract."""

    def setUp(self):
        super().setUp()
        self.store = SQLiteInternalStore(self.db_path)
        self.sink = SQLiteOpsSnapshotSink(self.store)

    def test_write_upserts_latest_snapshot(self):
        snapshot = {
            "type": "plan_status",
            "realm": "test_realm",
            "plan_id": "pln_x",
            "scope": {"kind": "project", "id": "P1"},
            "steps": {"s1": {"state": "step.started"}},
        }
        self.sink.write(Path("/unused"), snapshot)
        snapshot2 = dict(snapshot)
        snapshot2["steps"] = {"s1": {"state": "step.succeeded"}}
        self.sink.write(Path("/unused"), snapshot2)

        doc_id = "project-P1:plan_status:test_realm:pln_x"
        doc = self.store.get_document("operations_snapshots", doc_id)
        self.assertEqual(doc["steps"]["s1"]["state"], "step.succeeded")
        self.assertEqual(doc["_rev"], "2")


class TestBuildSQLiteBundle(SQLiteStoreTestBase):
    """Bundle composition."""

    def test_bundle_shares_one_store(self):
        bundle = build_sqlite_bundle(SQLiteInternalStorageConfig(path=self.db_path))
        self.assertEqual(bundle.backend, "sqlite")
        doc_id = bundle.plans.save_plan(
            _make_plan(), "test_realm", {"kind": "project", "id": "P1"}, auto_run=True
        )
        # The checkpoint store and ops sink hit the same file
        bundle.checkpoints.save(Checkpoint(backend_key="k", value="v"))
        self.assertTrue(bundle.plans.plan_exists(doc_id))
        self.assertEqual(bundle.checkpoints.load("k").value, "v")


if __name__ == "__main__":
    unittest.main()
