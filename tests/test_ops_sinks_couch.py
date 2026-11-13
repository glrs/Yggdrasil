"""
Comprehensive tests for yggdrasil/storage/couchdb/ops_sink.py

Tests the OpsWriter class for CouchDB operations.
"""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from yggdrasil.storage.couchdb.ops_sink import OpsWriter


class MockApiException(Exception):
    """Mock ApiException for testing."""

    def __init__(self, code):
        super().__init__(f"API Error {code}")
        self.code = code


# Make ApiException available for tests
ApiException = MockApiException


@patch("yggdrasil.storage.couchdb.ops_sink.ApiException", MockApiException)
class TestOpsWriterInitialization(unittest.TestCase):
    """Tests for OpsWriter initialization."""

    @patch("yggdrasil.storage.couchdb.ops_sink._get_couchdb_endpoint_config")
    @patch("yggdrasil.storage.couchdb.ops_sink.CouchDBHandler.__init__")
    def test_init_default_db_name(self, mock_parent_init, mock_get_config):
        """Test initialization with default database name."""
        from yggdrasil.storage.couchdb.ops_sink import OpsWriter

        mock_get_config.return_value = {
            "url": "http://localhost:5984",
            "auth": {"user_env": "TEST_USER", "pass_env": "TEST_PASS"},
        }
        mock_parent_init.return_value = None

        writer = OpsWriter()

        mock_parent_init.assert_called_once_with(
            "yggdrasil_ops_dev",
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )

    @patch("yggdrasil.storage.couchdb.ops_sink._get_couchdb_endpoint_config")
    @patch("yggdrasil.storage.couchdb.ops_sink.CouchDBHandler.__init__")
    def test_init_custom_db_name(self, mock_parent_init, mock_get_config):
        """Test initialization with custom database name."""
        from yggdrasil.storage.couchdb.ops_sink import OpsWriter

        mock_get_config.return_value = {
            "url": "http://localhost:5984",
            "auth": {"user_env": "TEST_USER", "pass_env": "TEST_PASS"},
        }
        mock_parent_init.return_value = None

        writer = OpsWriter(db_name="custom_ops_db")

        mock_parent_init.assert_called_once_with(
            "custom_ops_db",
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )


@patch("yggdrasil.storage.couchdb.ops_sink.ApiException", MockApiException)
class TestOpsWriterDocIdStatus(unittest.TestCase):
    """Tests for _doc_id_status method."""

    def setUp(self):
        with patch(
            "yggdrasil.storage.couchdb.ops_sink.CouchDBHandler.__init__",
            return_value=None,
        ):
            self.writer = OpsWriter()

    def test_doc_id_status_project(self):
        """Test document ID generation for project scope."""
        snapshot = {
            "scope": {"kind": "project", "id": "P12345"},
            "realm": "tenx",
            "plan_id": "plan_001",
        }

        doc_id = self.writer._doc_id_status(snapshot)

        self.assertEqual(doc_id, "project-P12345:plan_status:tenx:plan_001")

    def test_doc_id_status_flowcell(self):
        """Test document ID generation for flowcell scope."""
        snapshot = {
            "scope": {"kind": "flowcell", "id": "FC123"},
            "realm": "smartseq3",
            "plan_id": "plan_002",
        }

        doc_id = self.writer._doc_id_status(snapshot)

        self.assertEqual(doc_id, "flowcell-FC123:plan_status:smartseq3:plan_002")

    def test_doc_id_status_custom_kind(self):
        """Test document ID generation for custom kind."""
        snapshot = {
            "scope": {"kind": "experiment", "id": "EXP999"},
            "realm": "custom",
            "plan_id": "plan_003",
        }

        doc_id = self.writer._doc_id_status(snapshot)

        self.assertEqual(doc_id, "experiment-EXP999:plan_status:custom:plan_003")


@patch("yggdrasil.storage.couchdb.ops_sink.ApiException", MockApiException)
class TestOpsWriterWrite(unittest.TestCase):
    """Tests for write method (plan_status)."""

    def setUp(self):
        with patch(
            "yggdrasil.storage.couchdb.ops_sink.CouchDBHandler.__init__",
            return_value=None,
        ):
            self.writer = OpsWriter()
            self.writer.server = Mock()
            self.writer.db_name = "test_db"

    def test_write_creates_document(self):
        """Test write creates document with correct structure."""
        with TemporaryDirectory() as tmpdir:
            plan_dir = Path(tmpdir)

            snapshot = {
                "type": "plan_status",
                "realm": "tenx",
                "plan_id": "plan_001",
                "scope": {"kind": "project", "id": "P123"},
                "steps": {"step1": {"state": "succeeded"}},
                "updated_at": "2025-01-01T00:00:00Z",
            }

            # Mock get_document to return 404 (no existing doc)
            self.writer.server.get_document.side_effect = ApiException(404)  # type: ignore

            self.writer.write(plan_dir, snapshot)

            # Should call put_document
            self.writer.server.put_document.assert_called_once()  # type: ignore
            call_kwargs = self.writer.server.put_document.call_args[1]  # type: ignore
            self.assertEqual(call_kwargs["db"], "test_db")
            self.assertEqual(
                call_kwargs["doc_id"], "project-P123:plan_status:tenx:plan_001"
            )

    def test_write_updates_existing_document(self):
        """Test write updates existing document with _rev."""
        with TemporaryDirectory() as tmpdir:
            plan_dir = Path(tmpdir)

            snapshot = {
                "type": "plan_status",
                "realm": "tenx",
                "plan_id": "plan_001",
                "scope": {"kind": "project", "id": "P123"},
                "steps": {},
            }

            # Mock existing document
            existing_doc = {"_id": "doc_id", "_rev": "1-abc"}
            self.writer.server.get_document.return_value = Mock(get_result=lambda: existing_doc)  # type: ignore

            self.writer.write(plan_dir, snapshot)

            # Should get document first
            self.writer.server.get_document.assert_called()  # type: ignore

            # Should call put_document with _rev
            self.writer.server.put_document.assert_called_once()  # type: ignore

    def test_write_adds_id_to_payload(self):
        """Test write adds _id to payload."""
        with TemporaryDirectory() as tmpdir:
            plan_dir = Path(tmpdir)

            snapshot = {
                "type": "plan_status",
                "realm": "tenx",
                "plan_id": "plan_001",
                "scope": {"kind": "project", "id": "P123"},
            }

            self.writer.server.get_document.side_effect = ApiException(404)  # type: ignore

            self.writer.write(plan_dir, snapshot)

            # Check the payload has _id
            call_kwargs = self.writer.server.put_document.call_args[1]  # type: ignore
            doc = call_kwargs["document"]
            # The document should have been created from a dict with _id
            self.writer.server.put_document.assert_called_once()  # type: ignore


@patch("yggdrasil.storage.couchdb.ops_sink.ApiException", MockApiException)
class TestOpsWriterUpsert(unittest.TestCase):
    """Tests for _upsert helper method."""

    def setUp(self):
        with patch(
            "yggdrasil.storage.couchdb.ops_sink.CouchDBHandler.__init__",
            return_value=None,
        ):
            self.writer = OpsWriter()
            self.writer.server = Mock()
            self.writer.db_name = "test_db"

    def test_upsert_new_document(self):
        """Test upserting a new document (no existing _rev)."""
        doc_id = "test_doc_id"
        payload = {"type": "test", "data": "value"}

        # No existing document
        self.writer.server.get_document.side_effect = ApiException(404)  # type: ignore

        self.writer._upsert(doc_id, payload)

        # Should call put_document without _rev
        self.writer.server.put_document.assert_called_once()  # type: ignore
        call_kwargs = self.writer.server.put_document.call_args[1]  # type: ignore
        self.assertEqual(call_kwargs["doc_id"], doc_id)
        self.assertEqual(call_kwargs["db"], "test_db")

    def test_upsert_existing_document(self):
        """Test upserting an existing document (with _rev)."""
        doc_id = "test_doc_id"
        payload = {"type": "test", "data": "value"}

        # Existing document with _rev
        existing_doc = {"_id": doc_id, "_rev": "1-abc123"}
        self.writer.server.get_document.return_value = Mock(get_result=lambda: existing_doc)  # type: ignore

        self.writer._upsert(doc_id, payload)

        # Should get the document first
        self.writer.server.get_document.assert_called_once_with(  # type: ignore
            db=self.writer.db_name, doc_id=doc_id
        )

        # Should call put_document with _rev in payload
        self.writer.server.put_document.assert_called_once()  # type: ignore

    def test_upsert_handles_409_conflict(self):
        """Test upsert handles 409 conflict and retries."""
        doc_id = "test_doc_id"
        payload = {"type": "test", "data": "value"}

        # First get: no document
        # First put: conflict
        # Second get: returns document with new rev
        self.writer.server.get_document.side_effect = [  # type: ignore
            ApiException(404),
            Mock(get_result=lambda: {"_id": doc_id, "_rev": "2-def456"}),
        ]

        self.writer.server.put_document.side_effect = [  # type: ignore
            ApiException(409),
            None,  # Second put succeeds
        ]

        self.writer._upsert(doc_id, payload)

        # Should have called get_document twice
        self.assertEqual(self.writer.server.get_document.call_count, 2)  # type: ignore

        # Should have called put_document twice
        self.assertEqual(self.writer.server.put_document.call_count, 2)  # type: ignore

    def test_upsert_propagates_non_404_errors(self):
        """Test upsert propagates errors other than 404."""
        doc_id = "test_doc_id"
        payload = {"type": "test"}

        # Simulate server error
        self.writer.server.get_document.side_effect = ApiException(500)  # type: ignore

        with self.assertRaises(ApiException) as context:
            self.writer._upsert(doc_id, payload)

        self.assertEqual(context.exception.code, 500)

    def test_upsert_propagates_non_409_put_errors(self):
        """Test upsert propagates put errors other than 409."""
        doc_id = "test_doc_id"
        payload = {"type": "test"}

        self.writer.server.get_document.side_effect = ApiException(404)  # type: ignore
        self.writer.server.put_document.side_effect = ApiException(403)  # type: ignore

        with self.assertRaises(ApiException) as context:
            self.writer._upsert(doc_id, payload)

        self.assertEqual(context.exception.code, 403)

    def test_upsert_conflict_no_rev_in_retry(self):
        """Test upsert raises if conflict retry returns no _rev."""
        doc_id = "test_doc_id"
        payload = {"type": "test"}

        self.writer.server.get_document.side_effect = [  # type: ignore
            ApiException(404),
            Mock(get_result=lambda: {"_id": doc_id}),  # No _rev
        ]

        self.writer.server.put_document.side_effect = ApiException(409)  # type: ignore

        with self.assertRaises(Exception):
            self.writer._upsert(doc_id, payload)

    def test_upsert_get_returns_non_dict(self):
        """Test upsert handles non-dict response from get_document."""
        doc_id = "test_doc_id"
        payload = {"type": "test"}

        # get_document returns something other than dict
        self.writer.server.get_document.return_value = Mock(get_result=lambda: "not a dict")  # type: ignore

        self.writer._upsert(doc_id, payload)

        # Should still call put_document (without _rev)
        self.writer.server.put_document.assert_called_once()  # type: ignore


@patch("yggdrasil.storage.couchdb.ops_sink.ApiException", MockApiException)
class TestOpsWriterIntegration(unittest.TestCase):
    """Integration tests for OpsWriter."""

    def setUp(self):
        with patch(
            "yggdrasil.storage.couchdb.ops_sink.CouchDBHandler.__init__",
            return_value=None,
        ):
            self.writer = OpsWriter(db_name="integration_test_db")
            self.writer.server = Mock()
            self.writer.db_name = "integration_test_db"

    def test_multiple_writes_update_same_document(self):
        """Test multiple writes to same plan update the same document."""
        snapshot = {
            "type": "plan_status",
            "realm": "tenx",
            "plan_id": "plan_001",
            "scope": {"kind": "project", "id": "P123"},
            "steps": {"step1": {"state": "running"}},
        }

        # First write - no existing doc
        self.writer.server.get_document.side_effect = [  # type: ignore
            ApiException(404),
            Mock(get_result=lambda: {"_id": "doc_id", "_rev": "1-abc"}),
        ]

        with TemporaryDirectory() as tmpdir:
            plan_dir = Path(tmpdir)

            # First write
            self.writer.write(plan_dir, snapshot)
            first_doc_id = self.writer.server.put_document.call_args_list[0][1]["doc_id"]  # type: ignore

            # Update snapshot
            snapshot["steps"]["step1"]["state"] = "succeeded"

            # Second write
            self.writer.write(plan_dir, snapshot)
            second_doc_id = self.writer.server.put_document.call_args_list[1][1]["doc_id"]  # type: ignore

            # Should use same doc ID
            self.assertEqual(first_doc_id, second_doc_id)


if __name__ == "__main__":
    unittest.main()
