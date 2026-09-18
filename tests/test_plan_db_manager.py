"""
Unit tests for PlanDBManager.

Tests CRUD operations on plan documents without requiring a live database.
All CouchDB operations are mocked.

NOTE: Pylance reports type errors on mock assertion methods (e.g., assert_called_once,
return_value, call_args) because it doesn't recognize that self.manager.server methods
are replaced with MagicMock instances. These are false positives and can be ignored.
The tests run correctly despite these warnings.
"""

import os
import unittest
from unittest.mock import MagicMock, Mock, patch

from requests.exceptions import ConnectionError as RequestsConnectionError

from lib.couchdb.plan_db_manager import PlanDBManager
from lib.storage.errors import PlanStoreError, RevisionConflictError
from lib.storage.plan_documents import build_plan_document
from lib.storage.plan_updates import (
    ExecutionFinalization,
    FinalizationStatus,
    SupersessionReason,
)
from yggdrasil.flow.attempt import AttemptContext
from yggdrasil.flow.model import Plan, StepSpec
from yggdrasil.flow.outcomes import StepOutcome, TerminationReason


class MockApiException(Exception):
    """Mock ApiException for testing."""

    def __init__(self, code, message="Test error"):
        super().__init__(message)
        self.code = code
        self.status_code = code
        self.message = message


class TestPlanDBManager(unittest.TestCase):
    """Unit tests for PlanDBManager."""

    def setUp(self):
        """Set up test fixtures with mocked DB connection."""
        # Mock config endpoint
        self.mock_config = {
            "url": "https://mock-couchdb.example.com:5984",
            "auth": {
                "user_env": "MOCK_COUCH_USER",
                "pass_env": "MOCK_COUCH_PASS",
            },
        }
        self.mock_couchdb_params = Mock(
            url=self.mock_config["url"],
            user_env=self.mock_config["auth"]["user_env"],
            pass_env=self.mock_config["auth"]["pass_env"],
        )
        self.config_patcher = patch(
            "lib.couchdb.plan_db_manager.resolve_couchdb_params",
            return_value=self.mock_couchdb_params,
        )
        self.config_patcher.start()

        # Mock CouchDBClientFactory.create_client to return mock client
        self.mock_server = MagicMock()
        self.client_factory_patcher = patch(
            "lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client",
            return_value=self.mock_server,
        )
        self.client_factory_patcher.start()

        # Provide env vars required by CouchDBHandler.__init__
        self.env_patcher = patch.dict(
            os.environ,
            {"MOCK_COUCH_USER": "mock_user", "MOCK_COUCH_PASS": "mock_pass"},
        )
        self.env_patcher.start()

        # Create manager instance (will use mocked client factory)
        self.manager = PlanDBManager()

        # Create sample plan for testing
        self.sample_plan = Plan(
            plan_id="test_plan_1",
            realm="tenx",
            scope={"kind": "project", "id": "P12345"},
            steps=[
                StepSpec(
                    step_id="step_1",
                    name="test_step",
                    fn_ref="tests.test_plan_db_manager.test_step_fn",
                    params={"key": "value"},
                )
            ],
        )

    def tearDown(self):
        """Clean up patches."""
        self.config_patcher.stop()
        self.client_factory_patcher.stop()
        self.env_patcher.stop()

    # ==========================================
    # save_plan Tests
    # ==========================================

    def test_save_plan_new_document(self):
        """Test saving a new plan document."""
        # Mock: no existing document
        self.manager.fetch_document_by_id = Mock(return_value=None)
        mock_result = Mock()
        mock_result.get_result.return_value = {"ok": True, "rev": "1-abc"}
        self.manager.server.put_document.return_value = mock_result

        scope = {"kind": "project", "id": "P12345"}
        doc_id = self.manager.save_plan(
            self.sample_plan,
            realm="tenx",
            scope=scope,
            auto_run=False,
        )

        # Document ID comes from plan.plan_id (realm owns identity)
        self.assertEqual(doc_id, self.sample_plan.plan_id)
        self.manager.server.put_document.assert_called_once()

        # Verify document structure
        call_kwargs = self.manager.server.put_document.call_args[1]
        doc = call_kwargs["document"]
        self.assertEqual(doc["_id"], self.sample_plan.plan_id)
        self.assertEqual(doc["realm"], "tenx")
        self.assertEqual(doc["status"], "draft")  # auto_run=False
        self.assertEqual(doc["run_token"], 0)
        self.assertEqual(doc["executed_run_token"], -1)
        self.assertIn("plan", doc)
        self.assertNotIn("_rev", doc)  # No existing doc

    def test_save_plan_auto_run_approved(self):
        """Test saving a plan with auto_run=True sets status='approved'."""
        self.manager.fetch_document_by_id = Mock(return_value=None)
        mock_result = Mock()
        mock_result.get_result.return_value = {"ok": True}
        self.manager.server.put_document.return_value = mock_result

        self.manager.save_plan(
            self.sample_plan,
            realm="tenx",
            scope={"kind": "project", "id": "P12345"},
            auto_run=True,
        )

        call_kwargs = self.manager.server.put_document.call_args[1]
        doc = call_kwargs["document"]
        self.assertEqual(doc["status"], "approved")

    def test_save_plan_update_existing(self):
        """Test updating existing plan preserves _rev and created_at."""
        existing_doc = {
            "_id": "pln_tenx_P12345_v1",
            "_rev": "2-xyz",
            "created_at": "2025-01-01T00:00:00+00:00",
        }
        self.manager.fetch_document_by_id = Mock(return_value=existing_doc)
        mock_result = Mock()
        mock_result.get_result.return_value = {"ok": True}
        self.manager.server.put_document.return_value = mock_result

        self.manager.save_plan(
            self.sample_plan,
            realm="tenx",
            scope={"kind": "project", "id": "P12345"},
        )

        call_kwargs = self.manager.server.put_document.call_args[1]
        doc = call_kwargs["document"]
        self.assertEqual(doc["_rev"], "2-xyz")
        self.assertEqual(doc["created_at"], "2025-01-01T00:00:00+00:00")

    def test_save_plan_with_optional_fields(self):
        """Test saving plan with optional metadata fields."""
        self.manager.fetch_document_by_id = Mock(return_value=None)
        mock_result = Mock()
        mock_result.get_result.return_value = {"ok": True}
        self.manager.server.put_document.return_value = mock_result

        self.manager.save_plan(
            self.sample_plan,
            realm="tenx",
            scope={"kind": "project", "id": "P12345"},
            preview={"summary": "Test preview"},
            source_doc_id="projects:P12345",
            source_doc_rev="3-abc123",
            notes="Test notes",
        )

        call_kwargs = self.manager.server.put_document.call_args[1]
        doc = call_kwargs["document"]
        self.assertEqual(doc["preview"], {"summary": "Test preview"})
        self.assertEqual(doc["source_doc_id"], "projects:P12345")
        self.assertEqual(doc["source_doc_rev"], "3-abc123")
        self.assertEqual(doc["notes"], "Test notes")

    @patch("lib.couchdb.plan_db_manager.ApiException", MockApiException)
    def test_save_plan_api_error(self):
        """Test save_plan raises on API error."""
        self.manager.fetch_document_by_id = Mock(return_value=None)
        mock_result = Mock()
        mock_result.get_result.side_effect = MockApiException(500, "Server error")
        self.manager.server.put_document.return_value = mock_result

        with self.assertRaises(MockApiException):
            self.manager.save_plan(
                self.sample_plan,
                realm="tenx",
                scope={"kind": "project", "id": "P12345"},
            )

    # ==========================================
    # fetch_plan Tests
    # ==========================================

    def test_fetch_plan_exists(self):
        """Test fetching existing plan document."""
        expected_doc = {
            "_id": "pln_tenx_P12345_v1",
            "realm": "tenx",
            "status": "approved",
        }
        self.manager.fetch_document_by_id = Mock(return_value=expected_doc)

        result = self.manager.fetch_plan("pln_tenx_P12345_v1")

        self.assertEqual(result, expected_doc)
        self.manager.fetch_document_by_id.assert_called_once_with("pln_tenx_P12345_v1")

    def test_fetch_plan_not_found(self):
        """Test fetching non-existent plan returns None."""
        self.manager.fetch_document_by_id = Mock(return_value=None)

        result = self.manager.fetch_plan("pln_nonexistent_v1")

        self.assertIsNone(result)

    # ==========================================
    # fetch_plan_as_model Tests
    # ==========================================

    def test_fetch_plan_as_model_success(self):
        """Test deserializing plan document to Plan model."""
        plan_dict = self.sample_plan.to_dict()
        doc = {
            "_id": "pln_tenx_P12345_v1",
            "plan": plan_dict,
        }
        self.manager.fetch_document_by_id = Mock(return_value=doc)

        result = self.manager.fetch_plan_as_model("pln_tenx_P12345_v1")

        self.assertIsNotNone(result)
        self.assertEqual(result.plan_id, "test_plan_1")
        self.assertEqual(result.realm, "tenx")

    def test_fetch_plan_as_model_not_found(self):
        """Test fetch_plan_as_model returns None when doc not found."""
        self.manager.fetch_document_by_id = Mock(return_value=None)

        result = self.manager.fetch_plan_as_model("pln_nonexistent_v1")

        self.assertIsNone(result)

    def test_fetch_plan_as_model_no_plan_field(self):
        """Test fetch_plan_as_model handles missing 'plan' field."""
        doc = {"_id": "pln_tenx_P12345_v1"}  # no 'plan' field
        self.manager.fetch_document_by_id = Mock(return_value=doc)

        result = self.manager.fetch_plan_as_model("pln_tenx_P12345_v1")

        self.assertIsNone(result)

    def test_fetch_plan_as_model_invalid_plan_data(self):
        """Test fetch_plan_as_model handles invalid plan data."""
        doc = {
            "_id": "pln_tenx_P12345_v1",
            "plan": {"invalid": "data"},  # missing required fields
        }
        self.manager.fetch_document_by_id = Mock(return_value=doc)

        result = self.manager.fetch_plan_as_model("pln_tenx_P12345_v1")

        self.assertIsNone(result)

    # ==========================================
    # update_executed_token Tests
    # ==========================================

    def test_update_executed_token_success(self):
        """Test successful token update."""
        existing_doc = {
            "_id": "pln_tenx_P12345_v1",
            "_rev": "1-abc",
            "run_token": 0,
            "executed_run_token": -1,
        }
        self.manager.fetch_document_by_id = Mock(return_value=existing_doc)
        mock_result = Mock()
        mock_result.get_result.return_value = {"ok": True}
        self.manager.server.put_document.return_value = mock_result

        result = self.manager.update_executed_token("pln_tenx_P12345_v1", run_token=0)

        self.assertTrue(result)
        call_kwargs = self.manager.server.put_document.call_args[1]
        doc = call_kwargs["document"]
        self.assertEqual(doc["executed_run_token"], 0)
        self.assertIn("last_executed_at", doc)

    def test_update_executed_token_not_found(self):
        """Test token update when plan not found."""
        self.manager.fetch_document_by_id = Mock(return_value=None)

        result = self.manager.update_executed_token("pln_nonexistent_v1", run_token=0)

        self.assertFalse(result)
        self.manager.server.put_document.assert_not_called()

    @patch("lib.couchdb.plan_db_manager.ApiException", MockApiException)
    def test_update_executed_token_conflict_retry(self):
        """Test token update retries on 409 conflict."""
        existing_doc = {
            "_id": "pln_tenx_P12345_v1",
            "_rev": "1-abc",
            "run_token": 0,
            "executed_run_token": -1,
        }
        self.manager.fetch_document_by_id = Mock(return_value=existing_doc)

        # First call: conflict, second call: success
        mock_result_fail = Mock()
        mock_result_fail.get_result.side_effect = MockApiException(409, "Conflict")
        mock_result_success = Mock()
        mock_result_success.get_result.return_value = {"ok": True}
        self.manager.server.put_document.side_effect = [
            mock_result_fail,
            mock_result_success,
        ]

        result = self.manager.update_executed_token(
            "pln_tenx_P12345_v1", run_token=0, max_retries=2
        )

        self.assertTrue(result)
        self.assertEqual(self.manager.server.put_document.call_count, 2)

    @patch("lib.couchdb.plan_db_manager.ApiException", MockApiException)
    def test_update_executed_token_max_retries_exceeded(self):
        """Test token update fails after max retries."""
        existing_doc = {
            "_id": "pln_tenx_P12345_v1",
            "_rev": "1-abc",
            "run_token": 0,
        }
        self.manager.fetch_document_by_id = Mock(return_value=existing_doc)
        mock_result = Mock()
        mock_result.get_result.side_effect = MockApiException(409, "Conflict")
        self.manager.server.put_document.return_value = mock_result

        result = self.manager.update_executed_token(
            "pln_tenx_P12345_v1", run_token=0, max_retries=2
        )

        self.assertFalse(result)
        self.assertEqual(self.manager.server.put_document.call_count, 2)

    @patch("lib.couchdb.plan_db_manager.ApiException", MockApiException)
    def test_update_executed_token_other_error(self):
        """Test token update fails on non-409 error."""
        existing_doc = {
            "_id": "pln_tenx_P12345_v1",
            "_rev": "1-abc",
            "run_token": 0,
        }
        self.manager.fetch_document_by_id = Mock(return_value=existing_doc)
        mock_result = Mock()
        mock_result.get_result.side_effect = MockApiException(500, "Server error")
        self.manager.server.put_document.return_value = mock_result

        result = self.manager.update_executed_token("pln_tenx_P12345_v1", run_token=0)

        self.assertFalse(result)
        # Should not retry on 500
        self.assertEqual(self.manager.server.put_document.call_count, 1)

    # ==========================================
    # query_approved_pending Tests
    # ==========================================

    def test_query_approved_pending_finds_eligible(self):
        """Test query_approved_pending returns eligible plans."""
        # Mock database response with mix of eligible and ineligible
        all_docs_result = {
            "rows": [
                {
                    "doc": {
                        "_id": "pln_tenx_P1_v1",
                        "status": "approved",
                        "run_token": 0,
                        "executed_run_token": -1,
                    }
                },
                {
                    "doc": {
                        "_id": "pln_tenx_P2_v1",
                        "status": "draft",
                        "run_token": 0,
                        "executed_run_token": -1,
                    }
                },
                {
                    "doc": {
                        "_id": "pln_tenx_P3_v1",
                        "status": "approved",
                        "run_token": 0,
                        "executed_run_token": 0,  # already executed
                    }
                },
            ]
        }
        mock_result = Mock()
        mock_result.get_result.return_value = all_docs_result
        self.manager.server.post_all_docs.return_value = mock_result

        eligible = self.manager.query_approved_pending()

        self.assertEqual(len(eligible), 1)
        self.assertEqual(eligible[0]["_id"], "pln_tenx_P1_v1")

    def test_query_approved_pending_skips_design_docs(self):
        """Test query_approved_pending skips design documents."""
        all_docs_result = {
            "rows": [
                {"doc": {"_id": "_design/views", "views": {}}},
                {
                    "doc": {
                        "_id": "pln_tenx_P1_v1",
                        "status": "approved",
                        "run_token": 0,
                        "executed_run_token": -1,
                    }
                },
            ]
        }
        mock_result = Mock()
        mock_result.get_result.return_value = all_docs_result
        self.manager.server.post_all_docs.return_value = mock_result

        eligible = self.manager.query_approved_pending()

        self.assertEqual(len(eligible), 1)
        self.assertNotIn("_design", eligible[0]["_id"])

    def test_query_approved_pending_empty_db(self):
        """Test query_approved_pending on empty database."""
        all_docs_result = {"rows": []}
        mock_result = Mock()
        mock_result.get_result.return_value = all_docs_result
        self.manager.server.post_all_docs.return_value = mock_result

        eligible = self.manager.query_approved_pending()

        self.assertEqual(eligible, [])

    @patch("lib.couchdb.plan_db_manager.ApiException", MockApiException)
    def test_query_approved_pending_api_error(self):
        """Test query_approved_pending returns empty on API error."""
        mock_result = Mock()
        mock_result.get_result.side_effect = MockApiException(500, "Server error")
        self.manager.server.post_all_docs.return_value = mock_result

        eligible = self.manager.query_approved_pending()

        self.assertEqual(eligible, [])

    # ==========================================
    # delete_plan Tests
    # ==========================================

    def test_delete_plan_success(self):
        """Test successful plan deletion."""
        existing_doc = {"_id": "pln_tenx_P12345_v1", "_rev": "1-abc"}
        self.manager.fetch_document_by_id = Mock(return_value=existing_doc)
        mock_result = Mock()
        mock_result.get_result.return_value = {"ok": True}
        self.manager.server.delete_document.return_value = mock_result

        result = self.manager.delete_plan("pln_tenx_P12345_v1")

        self.assertTrue(result)
        self.manager.server.delete_document.assert_called_once_with(
            db="yggdrasil_plans",
            doc_id="pln_tenx_P12345_v1",
            rev="1-abc",
        )

    def test_delete_plan_not_found(self):
        """Test delete_plan when plan not found."""
        self.manager.fetch_document_by_id = Mock(return_value=None)

        result = self.manager.delete_plan("pln_nonexistent_v1")

        self.assertFalse(result)
        self.manager.server.delete_document.assert_not_called()

    def test_delete_plan_no_rev(self):
        """Test delete_plan when doc has no _rev."""
        existing_doc = {"_id": "pln_tenx_P12345_v1"}  # no _rev
        self.manager.fetch_document_by_id = Mock(return_value=existing_doc)

        result = self.manager.delete_plan("pln_tenx_P12345_v1")

        self.assertFalse(result)
        self.manager.server.delete_document.assert_not_called()

    @patch("lib.couchdb.plan_db_manager.ApiException", MockApiException)
    def test_delete_plan_api_error(self):
        """Test delete_plan handles API errors gracefully."""
        existing_doc = {"_id": "pln_tenx_P12345_v1", "_rev": "1-abc"}
        self.manager.fetch_document_by_id = Mock(return_value=existing_doc)
        mock_result = Mock()
        mock_result.get_result.side_effect = MockApiException(500, "Server error")
        self.manager.server.delete_document.return_value = mock_result

        result = self.manager.delete_plan("pln_tenx_P12345_v1")

        self.assertFalse(result)

    # ==========================================
    # Execution Origin & Owner Tests (Phase 1)
    # ==========================================

    def test_save_plan_default_execution_origin_is_daemon(self):
        """Test that default execution_authority is 'daemon'."""
        self.manager.fetch_document_by_id = Mock(return_value=None)
        mock_result = Mock()
        mock_result.get_result.return_value = {"ok": True}
        self.manager.server.put_document.return_value = mock_result

        self.manager.save_plan(
            self.sample_plan,
            realm="tenx",
            scope={"kind": "project", "id": "P12345"},
        )

        call_kwargs = self.manager.server.put_document.call_args[1]
        doc = call_kwargs["document"]
        self.assertEqual(doc["execution_authority"], "daemon")
        self.assertIsNone(doc["execution_owner"])

    def test_save_plan_with_run_once_origin(self):
        """Test saving plan with execution_authority='run_once'."""
        self.manager.fetch_document_by_id = Mock(return_value=None)
        mock_result = Mock()
        mock_result.get_result.return_value = {"ok": True}
        self.manager.server.put_document.return_value = mock_result

        self.manager.save_plan(
            self.sample_plan,
            realm="tenx",
            scope={"kind": "project", "id": "P12345"},
            execution_authority="run_once",
        )

        call_kwargs = self.manager.server.put_document.call_args[1]
        doc = call_kwargs["document"]
        self.assertEqual(doc["execution_authority"], "run_once")

    def test_save_plan_with_execution_owner(self):
        """Test saving plan with execution_owner token."""
        self.manager.fetch_document_by_id = Mock(return_value=None)
        mock_result = Mock()
        mock_result.get_result.return_value = {"ok": True}
        self.manager.server.put_document.return_value = mock_result

        owner_token = "run_once:550e8400-e29b-41d4-a716-446655440000"
        self.manager.save_plan(
            self.sample_plan,
            realm="tenx",
            scope={"kind": "project", "id": "P12345"},
            execution_authority="run_once",
            execution_owner=owner_token,
        )

        call_kwargs = self.manager.server.put_document.call_args[1]
        doc = call_kwargs["document"]
        self.assertEqual(doc["execution_authority"], "run_once")
        self.assertEqual(doc["execution_owner"], owner_token)

    def test_save_plan_invalid_execution_origin_raises(self):
        """Test that invalid execution_authority raises ValueError."""
        with self.assertRaises(ValueError) as ctx:
            self.manager.save_plan(
                self.sample_plan,
                realm="tenx",
                scope={"kind": "project", "id": "P12345"},
                execution_authority="invalid_origin",
            )

        self.assertIn("Invalid execution_authority", str(ctx.exception))
        self.assertIn("invalid_origin", str(ctx.exception))
        # Should not have attempted to save
        self.manager.server.put_document.assert_not_called()

    # ==========================================
    # plan_exists Tests
    # ==========================================

    def test_plan_exists_returns_true_for_existing(self):
        """Test plan_exists returns True for existing plan."""
        existing_doc = {"_id": "pln_tenx_P12345_v1", "_rev": "1-abc"}
        self.manager.fetch_document_by_id = Mock(return_value=existing_doc)

        result = self.manager.plan_exists("pln_tenx_P12345_v1")

        self.assertTrue(result)
        self.manager.fetch_document_by_id.assert_called_once_with("pln_tenx_P12345_v1")

    def test_plan_exists_returns_false_for_missing(self):
        """Test plan_exists returns False for non-existent plan."""
        self.manager.fetch_document_by_id = Mock(return_value=None)

        result = self.manager.plan_exists("pln_nonexistent_v1")

        self.assertFalse(result)

    # ==========================================
    # get_plan_summary Tests
    # ==========================================

    def test_get_plan_summary_returns_expected_fields(self):
        """Test get_plan_summary returns correct subset of fields."""
        existing_doc = {
            "_id": "pln_tenx_P12345_v1",
            "_rev": "2-xyz",
            "status": "approved",
            "execution_authority": "run_once",
            "execution_owner": "run_once:test-uuid",
            "updated_at": "2025-01-15T10:30:00Z",
            "realm": "tenx",
            "run_token": 1,
            "executed_run_token": 0,
            "plan": {"large": "data", "not": "included"},
        }
        self.manager.fetch_document_by_id = Mock(return_value=existing_doc)

        result = self.manager.get_plan_summary("pln_tenx_P12345_v1")

        self.assertIsNotNone(result)
        self.assertEqual(result["status"], "approved")
        self.assertEqual(result["execution_authority"], "run_once")
        self.assertEqual(result["execution_owner"], "run_once:test-uuid")
        self.assertEqual(result["updated_at"], "2025-01-15T10:30:00Z")
        self.assertEqual(result["realm"], "tenx")
        self.assertEqual(result["run_token"], 1)
        self.assertEqual(result["executed_run_token"], 0)
        # Should NOT include full plan data
        self.assertNotIn("plan", result)
        self.assertNotIn("_rev", result)

    def test_get_plan_summary_returns_none_for_missing(self):
        """Test get_plan_summary returns None for non-existent plan."""
        self.manager.fetch_document_by_id = Mock(return_value=None)

        result = self.manager.get_plan_summary("pln_nonexistent_v1")

        self.assertIsNone(result)

    def test_get_plan_summary_handles_missing_fields(self):
        """Test get_plan_summary provides defaults for missing fields."""
        existing_doc = {
            "_id": "pln_tenx_P12345_v1",
            # Missing: execution_authority, execution_owner, etc.
        }
        self.manager.fetch_document_by_id = Mock(return_value=existing_doc)

        result = self.manager.get_plan_summary("pln_tenx_P12345_v1")

        self.assertIsNotNone(result)
        self.assertEqual(result["status"], "unknown")
        self.assertEqual(result["execution_authority"], "daemon")  # default
        self.assertIsNone(result["execution_owner"])  # default
        self.assertEqual(result["run_token"], 0)
        self.assertEqual(result["executed_run_token"], -1)

    # ==========================================
    # Plan generation, conflicts and finalization
    # ==========================================

    def _stored_doc(self, **overrides):
        """A stored plan document, as fetch_document_by_id returns it."""
        doc = build_plan_document(
            self.sample_plan,
            "tenx",
            {"kind": "project", "id": "P12345"},
            auto_run=True,
            plan_generation="gen-1",
        )
        doc["_rev"] = "1-abc"
        doc.update(overrides)
        return doc

    def _finalization(self, doc, execution_id="exec-1"):
        """Finalization of a successful attempt admitted from doc."""
        context = AttemptContext.for_plan(
            self.sample_plan,
            execution_id=execution_id,
            plan_generation=doc["plan_generation"],
            run_token=doc["run_token"],
        )
        context.report.record_outcome("step_1", StepOutcome.SUCCEEDED)
        context.report.finish(TerminationReason.COMPLETED)
        return ExecutionFinalization.from_attempt(context)

    def _get_returns(self, *documents):
        """Queue the plan store's document reads.

        Each entry is a response body or an exception to raise. The last entry
        answers every further read, so a test lists only the reads it cares
        about.
        """
        queued = list(documents)

        def _read(*, db, doc_id):
            item = queued.pop(0) if len(queued) > 1 else queued[0]
            result = Mock()
            if isinstance(item, BaseException):
                result.get_result.side_effect = item
            else:
                result.get_result.return_value = item
            return result

        self.manager.server.get_document = Mock(side_effect=_read)

    def _put_returns(self, result=None, error=None):
        mock_result = Mock()
        if error is not None:
            mock_result.get_result.side_effect = error
        else:
            mock_result.get_result.return_value = result
        self.manager.server.put_document.return_value = mock_result

    def test_save_plan_assigns_a_fresh_generation_each_time(self):
        """Every save is a new plan version with its own generation."""
        self.manager.fetch_document_by_id = Mock(return_value=None)
        self._put_returns({"ok": True, "rev": "1-abc"})

        generations = []
        for _ in range(2):
            self.manager.save_plan(
                self.sample_plan, realm="tenx", scope={"kind": "project", "id": "P1"}
            )
            doc = self.manager.server.put_document.call_args[1]["document"]
            generations.append(doc["plan_generation"])

        self.assertTrue(all(generations))
        self.assertNotEqual(generations[0], generations[1])

    @patch("lib.couchdb.plan_db_manager.ApiException", MockApiException)
    def test_save_plan_conflict_raises_revision_conflict_without_retry(self):
        """A 409 on regeneration surfaces as the backend-neutral conflict."""
        self.manager.fetch_document_by_id = Mock(return_value=self._stored_doc())
        self._put_returns(error=MockApiException(409, "Conflict"))

        with self.assertRaises(RevisionConflictError) as ctx:
            self.manager.save_plan(
                self.sample_plan, realm="tenx", scope={"kind": "project", "id": "P1"}
            )

        self.assertEqual(ctx.exception.expected_rev, "1-abc")
        self.assertIsInstance(ctx.exception.__cause__, MockApiException)
        self.manager.server.put_document.assert_called_once()

    @patch("lib.couchdb.plan_db_manager.ApiException", MockApiException)
    def test_save_plan_conflict_on_create_raises_revision_conflict(self):
        """A 409 on a first save means another writer created the plan."""
        self.manager.fetch_document_by_id = Mock(return_value=None)
        self._put_returns(error=MockApiException(409, "Conflict"))

        with self.assertRaises(RevisionConflictError) as ctx:
            self.manager.save_plan(
                self.sample_plan, realm="tenx", scope={"kind": "project", "id": "P1"}
            )
        self.assertIsNone(ctx.exception.expected_rev)

    def test_finalize_rejects_a_generation_mismatch_without_any_409(self):
        """The _rev would allow the write; the generation check must not."""
        request = self._finalization(self._stored_doc())
        self._get_returns(self._stored_doc(_rev="7-current", plan_generation="gen-2"))

        result = self.manager.finalize_execution(request)

        self.assertEqual(result.status, FinalizationStatus.SUPERSEDED)
        self.assertEqual(result.reason, SupersessionReason.GENERATION_CHANGED)
        self.manager.server.put_document.assert_not_called()

    def test_finalize_writes_the_completion_at_the_fetched_revision(self):
        """Outcome and token go out together in one conditional write."""
        doc = self._stored_doc(run_token=1)
        request = self._finalization(doc)
        self._get_returns(doc)
        self._put_returns({"ok": True, "rev": "2-def"})

        result = self.manager.finalize_execution(request)

        self.assertEqual(result.status, FinalizationStatus.COMMITTED)
        self.manager.server.put_document.assert_called_once()
        written = self.manager.server.put_document.call_args[1]["document"]
        self.assertEqual(written["_rev"], "1-abc")
        self.assertEqual(written["executed_run_token"], 1)
        self.assertEqual(written["last_finalized_execution"]["execution_id"], "exec-1")
        self.assertEqual(written["plan_generation"], "gen-1")

    @patch("lib.couchdb.plan_db_manager.ApiException", MockApiException)
    def test_finalize_409_is_reported_as_conflict_not_retried(self):
        """One call makes one write attempt; the caller owns retries."""
        doc = self._stored_doc()
        request = self._finalization(doc)
        self._get_returns(doc, self._stored_doc(_rev="2-xyz", run_token=1))
        self._put_returns(error=MockApiException(409, "Conflict"))

        result = self.manager.finalize_execution(request)

        self.assertEqual(result.status, FinalizationStatus.CONFLICT)
        self.manager.server.put_document.assert_called_once()
        self.assertEqual(self.manager.server.get_document.call_count, 2)

    @patch("lib.couchdb.plan_db_manager.ApiException", MockApiException)
    def test_finalize_409_revealing_a_regeneration_is_superseded(self):
        """After a conflict the reread decides; a new generation is final."""
        doc = self._stored_doc()
        request = self._finalization(doc)
        self._get_returns(doc, self._stored_doc(_rev="2-xyz", plan_generation="gen-2"))
        self._put_returns(error=MockApiException(409, "Conflict"))

        result = self.manager.finalize_execution(request)

        self.assertEqual(result.reason, SupersessionReason.GENERATION_CHANGED)

    @patch("lib.couchdb.plan_db_manager.ApiException", MockApiException)
    def test_finalize_backend_failures_are_plan_store_errors(self):
        """Server and network failures surface as PlanStoreError.

        Whether another attempt could help is part of that contract: a busy or
        restarting server and a dropped connection are worth retrying, while a
        rejected credential or a malformed request would fail again the same
        way.
        """
        cases = {
            "overloaded": (MockApiException(503, "Service unavailable"), True),
            "server error": (MockApiException(500, "Server error"), True),
            "dropped connection": (RequestsConnectionError("reset"), True),
            "forbidden": (MockApiException(403, "Forbidden"), False),
            "bad request": (MockApiException(400, "Bad request"), False),
        }
        for label, (error, retryable) in cases.items():
            with self.subTest(label):
                doc = self._stored_doc()
                self._get_returns(doc)
                self.manager.server.put_document.reset_mock()
                self._put_returns(error=error)

                with self.assertRaises(PlanStoreError) as ctx:
                    self.manager.finalize_execution(self._finalization(doc))

                self.assertIs(ctx.exception.__cause__, error)
                self.assertEqual(ctx.exception.retryable, retryable)
                self.manager.server.put_document.assert_called_once()

    @patch("lib.couchdb.plan_db_manager.ApiException", MockApiException)
    def test_finalize_reports_a_genuinely_deleted_plan_as_missing(self):
        """Only CouchDB's own 404 means the plan is gone."""
        request = self._finalization(self._stored_doc())
        self._get_returns(MockApiException(404, "missing"))

        result = self.manager.finalize_execution(request)

        self.assertEqual(result.reason, SupersessionReason.PLAN_MISSING)
        self.manager.server.put_document.assert_not_called()

    @patch("lib.couchdb.plan_db_manager.ApiException", MockApiException)
    def test_finalize_refuses_to_read_a_malformed_response_as_a_missing_plan(self):
        """A non-document answer says nothing about whether the plan exists.

        Reporting supersession here would tell the caller its completed
        execution can never be recorded, and stop it from trying again.
        """
        request = self._finalization(self._stored_doc())
        for body in ("", "not json", [], [{"_id": "x"}], None, 42):
            with self.subTest(body=repr(body)):
                self._get_returns(body)
                self.manager.server.put_document.reset_mock()

                with self.assertRaises(PlanStoreError) as ctx:
                    self.manager.finalize_execution(request)

                self.assertFalse(ctx.exception.retryable)
                self.manager.server.put_document.assert_not_called()

    @patch("lib.couchdb.plan_db_manager.ApiException", MockApiException)
    def test_finalize_refuses_a_malformed_response_after_a_conflict(self):
        """The reread that interprets a conflict must be trustworthy too."""
        doc = self._stored_doc()
        request = self._finalization(doc)
        self._get_returns(doc, "<html>proxy error</html>")
        self._put_returns(error=MockApiException(409, "Conflict"))

        with self.assertRaises(PlanStoreError):
            self.manager.finalize_execution(request)

    @patch("lib.couchdb.plan_db_manager.ApiException", MockApiException)
    def test_ensure_plan_generation_refuses_a_malformed_response(self):
        """A generation is never assigned on top of an unreadable answer."""
        self._get_returns(["not", "a", "document"])

        with self.assertRaises(PlanStoreError):
            self.manager.ensure_plan_generation("pln_tenx_P12345_v1")

        self.manager.server.put_document.assert_not_called()

    def test_finalize_response_without_a_revision_is_a_plan_store_error(self):
        """A write whose new revision is unknown is not reported as committed."""
        doc = self._stored_doc()
        self._get_returns(doc)
        self._put_returns({"ok": True})

        with self.assertRaises(PlanStoreError):
            self.manager.finalize_execution(self._finalization(doc))

    def test_ensure_plan_generation_writes_only_the_generation(self):
        """A legacy document gains a generation at the fetched revision."""
        legacy = self._stored_doc()
        del legacy["plan_generation"]
        self._get_returns(legacy)
        self._put_returns({"ok": True, "rev": "2-def"})

        doc = self.manager.ensure_plan_generation(legacy["_id"])

        written = self.manager.server.put_document.call_args[1]["document"]
        self.assertEqual(written["_rev"], "1-abc")
        self.assertEqual(
            {k: v for k, v in written.items() if k != "plan_generation"}, legacy
        )
        self.assertEqual(doc["plan_generation"], written["plan_generation"])
        self.assertEqual(doc["_rev"], "2-def")

    @patch("lib.couchdb.plan_db_manager.ApiException", MockApiException)
    def test_ensure_plan_generation_adopts_the_winner_after_409(self):
        """Losing the race to another initializer adopts its generation."""
        legacy = self._stored_doc()
        del legacy["plan_generation"]
        winner = self._stored_doc(_rev="2-xyz", plan_generation="gen-winner")
        self._get_returns(legacy, winner)
        self._put_returns(error=MockApiException(409, "Conflict"))

        doc = self.manager.ensure_plan_generation(legacy["_id"])

        self.assertEqual(doc, winner)
        self.manager.server.put_document.assert_called_once()

    @patch("lib.couchdb.plan_db_manager.ApiException", MockApiException)
    def test_ensure_plan_generation_read_failure_is_a_plan_store_error(self):
        """A failed read is a backend failure, not a missing plan."""
        self._get_returns(MockApiException(500, "Server error"))
        with self.assertRaises(PlanStoreError):
            self.manager.ensure_plan_generation("pln_tenx_P12345_v1")


if __name__ == "__main__":
    unittest.main()
