"""
Tests for CouchDB connection utilities.

Tests for:
- CouchDBClientFactory: Stateless factory for creating CloudantV1 clients
- CouchDBHandler: Base class for database operations
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, Mock, patch

import requests
import requests.exceptions


# Create mocks for IBM Cloud SDK classes
class MockApiException(Exception):
    def __init__(self, message="", code=None):
        super().__init__(message)
        self.code = code
        self.status_code = code
        self.message = message


# Mock the IBM Cloud SDK modules to avoid import errors in test environment
mock_api_exception_module = MagicMock()
mock_api_exception_module.ApiException = MockApiException
sys.modules["ibm_cloud_sdk_core"] = MagicMock()
sys.modules["ibm_cloud_sdk_core.api_exception"] = mock_api_exception_module
sys.modules["ibmcloudant"] = MagicMock()
sys.modules["ibmcloudant.cloudant_v1"] = MagicMock()

# Import after mocks
import lib.couchdb.couchdb_connection

lib.couchdb.couchdb_connection.ApiException = MockApiException

from lib.core_utils.errors import ExternalSystemUnavailableError
from lib.couchdb.couchdb_connection import (
    CouchDBClientFactory,
    CouchDBHandler,
    is_transient_doc_fetch_error,
    is_transient_poll_error,
)


class TestCouchDBClientFactory(unittest.TestCase):
    """Tests for CouchDBClientFactory."""

    def setUp(self):
        # Isolate the dedup set between tests
        self._saved_connections = CouchDBClientFactory._logged_connections.copy()
        CouchDBClientFactory._logged_connections.clear()

    def tearDown(self):
        CouchDBClientFactory._logged_connections.clear()
        CouchDBClientFactory._logged_connections.update(self._saved_connections)

    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    def test_create_client_success(self, mock_auth, mock_cloudant):
        """Test successful client creation."""
        mock_client = MagicMock()
        mock_client.get_server_information.return_value.get_result.return_value = {
            "version": "3.1.1"
        }
        mock_cloudant.return_value = mock_client

        client = CouchDBClientFactory.create_client(
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )

        # Verify client was created and configured
        mock_auth.assert_called_once_with("admin", "secret")
        mock_cloudant.assert_called_once()
        mock_client.set_service_url.assert_called_once_with("http://localhost:5984")
        mock_client.get_server_information.assert_called_once()
        self.assertEqual(client, mock_client)

    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    def test_create_client_skip_verification(self, mock_auth, mock_cloudant):
        """Test client creation without connection verification."""
        mock_client = MagicMock()
        mock_cloudant.return_value = mock_client

        client = CouchDBClientFactory.create_client(
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
            verify_connection=False,
        )

        # Verify no ping was attempted
        mock_client.get_server_information.assert_not_called()
        self.assertEqual(client, mock_client)

    def test_create_client_missing_scheme_raises(self):
        """Test that URL without scheme raises ValueError."""
        with self.assertRaises(ValueError) as ctx:
            CouchDBClientFactory.create_client(
                url="localhost:5984",
                user_env="TEST_USER",
                pass_env="TEST_PASS",
            )
        self.assertIn("must include scheme", str(ctx.exception))

    @patch.dict(os.environ, {"TEST_PASS": "secret"}, clear=False)
    def test_create_client_missing_user_env_raises(self):
        """Test that missing user env var raises RuntimeError."""
        # Ensure TEST_USER is not set
        os.environ.pop("TEST_USER", None)

        with self.assertRaises(RuntimeError) as ctx:
            CouchDBClientFactory.create_client(
                url="http://localhost:5984",
                user_env="TEST_USER",
                pass_env="TEST_PASS",
            )
        self.assertIn("TEST_USER", str(ctx.exception))

    @patch.dict(os.environ, {"TEST_USER": "admin"}, clear=False)
    def test_create_client_missing_pass_env_raises(self):
        """Test that missing password env var raises RuntimeError."""
        # Ensure TEST_PASS is not set
        os.environ.pop("TEST_PASS", None)

        with self.assertRaises(RuntimeError) as ctx:
            CouchDBClientFactory.create_client(
                url="http://localhost:5984",
                user_env="TEST_USER",
                pass_env="TEST_PASS",
            )
        self.assertIn("TEST_PASS", str(ctx.exception))

    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    def test_create_client_connection_failure_raises(self, mock_auth, mock_cloudant):
        """Test that connection failure raises ExternalSystemUnavailableError."""
        mock_cloudant.side_effect = Exception("Connection refused")

        with self.assertRaises(ExternalSystemUnavailableError) as ctx:
            CouchDBClientFactory.create_client(
                url="http://localhost:5984",
                user_env="TEST_USER",
                pass_env="TEST_PASS",
            )
        self.assertIn("Cannot reach CouchDB", str(ctx.exception))
        self.assertEqual(ctx.exception.system, "CouchDB")
        self.assertEqual(ctx.exception.endpoint, "http://localhost:5984")
        self.assertIn("VPN", ctx.exception.hint or "")

    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    def test_create_client_non_dict_server_info_uses_unknown_version(
        self, mock_auth, mock_cloudant
    ):
        """Test that non-dict server info results in version='unknown'."""
        mock_client = MagicMock()
        # Return a truthy non-dict value so `info or {}` keeps it and isinstance fails
        mock_client.get_server_information.return_value.get_result.return_value = (
            "not-a-dict"
        )
        mock_cloudant.return_value = mock_client

        # Should not raise; just log version="unknown"
        client = CouchDBClientFactory.create_client(
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )
        self.assertEqual(client, mock_client)

    @patch("lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1")
    @patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator")
    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    def test_create_client_dedup_logs_debug_on_reconnect(
        self, mock_auth, mock_cloudant
    ):
        """Second connection to same (url, user) logs DEBUG instead of INFO."""
        mock_client = MagicMock()
        mock_client.get_server_information.return_value.get_result.return_value = {
            "version": "3.1.1"
        }
        mock_cloudant.return_value = mock_client

        with self.assertLogs("lib.couchdb.couchdb_connection", level="DEBUG") as cm:
            CouchDBClientFactory.create_client(
                url="http://unique-dedup.example:5984",
                user_env="TEST_USER",
                pass_env="TEST_PASS",
            )
            CouchDBClientFactory.create_client(
                url="http://unique-dedup.example:5984",
                user_env="TEST_USER",
                pass_env="TEST_PASS",
            )

        messages = [r.getMessage() for r in cm.records]
        info_msgs = [m for m in messages if "Connected to CouchDB" in m]
        debug_msgs = [m for m in messages if "Reconnected to CouchDB" in m]
        self.assertEqual(len(info_msgs), 1, "First connect should log INFO once")
        self.assertEqual(len(debug_msgs), 1, "Second connect should log DEBUG once")


class TestCouchDBHandler(unittest.TestCase):
    """Tests for CouchDBHandler."""

    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    @patch("lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client")
    def test_init_creates_client_and_verifies_db(self, mock_create_client):
        """Test handler initialization creates client and verifies db exists."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client

        handler = CouchDBHandler(
            db_name="test_db",
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )

        # Verify factory was called correctly
        mock_create_client.assert_called_once_with(
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
            enable_retries=True,
        )

        # Verify database was checked
        mock_client.get_database_information.assert_called_once_with(db="test_db")

        self.assertEqual(handler.db_name, "test_db")
        self.assertEqual(handler.server, mock_client)

    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    @patch("lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client")
    def test_init_raises_on_missing_db(self, mock_create_client):
        """Test handler raises ExternalSystemUnavailableError if database doesn't exist."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_client.get_database_information.side_effect = MockApiException(
            "not found", code=404
        )

        with self.assertRaises(ExternalSystemUnavailableError) as ctx:
            CouchDBHandler(
                db_name="nonexistent_db",
                url="http://localhost:5984",
                user_env="TEST_USER",
                pass_env="TEST_PASS",
            )
        self.assertIn("does not exist", str(ctx.exception))
        self.assertIn("nonexistent_db", ctx.exception.hint or "")

    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    @patch("lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client")
    def test_fetch_document_by_id_success(self, mock_create_client):
        """Test fetching a document by ID."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_client.get_document.return_value.get_result.return_value = {
            "_id": "doc123",
            "name": "Test Doc",
        }

        handler = CouchDBHandler(
            db_name="test_db",
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )

        doc = handler.fetch_document_by_id("doc123")

        mock_client.get_document.assert_called_with(db="test_db", doc_id="doc123")
        self.assertEqual(doc["_id"], "doc123")

    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    @patch("lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client")
    def test_fetch_document_by_id_not_found(self, mock_create_client):
        """Test fetching a non-existent document returns None."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_client.get_document.side_effect = MockApiException("not found", code=404)

        handler = CouchDBHandler(
            db_name="test_db",
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )

        doc = handler.fetch_document_by_id("missing_doc")
        self.assertIsNone(doc)

    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    @patch("lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client")
    def test_fetch_document_by_id_non_dict_response_returns_none(
        self, mock_create_client
    ):
        """Test that a non-dict response from get_document returns None."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        # Simulate SDK returning a non-dict (e.g. a string or list)
        mock_client.get_document.return_value.get_result.return_value = "not-a-dict"

        handler = CouchDBHandler(
            db_name="test_db",
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )

        doc = handler.fetch_document_by_id("some_doc")
        self.assertIsNone(doc)

    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    @patch("lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client")
    def test_fetch_document_by_id_non_404_api_exception_re_raises(
        self, mock_create_client
    ):
        """Test that a non-404 ApiException from get_document is re-raised."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_client.get_document.side_effect = MockApiException(
            "server error", code=500
        )

        handler = CouchDBHandler(
            db_name="test_db",
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )

        with self.assertRaises(MockApiException):
            handler.fetch_document_by_id("some_doc")

    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    @patch("lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client")
    def test_fetch_document_by_id_generic_exception_re_raises(self, mock_create_client):
        """Test that a generic exception from get_document is re-raised."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_client.get_document.side_effect = RuntimeError("unexpected")

        handler = CouchDBHandler(
            db_name="test_db",
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )

        with self.assertRaises(RuntimeError):
            handler.fetch_document_by_id("some_doc")

    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    @patch("lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client")
    def test_init_non_404_api_exception_re_raises(self, mock_create_client):
        """Test that a non-404 ApiException during db verification is re-raised."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_client.get_database_information.side_effect = MockApiException(
            "forbidden", code=403
        )

        with self.assertRaises(MockApiException):
            CouchDBHandler(
                db_name="test_db",
                url="http://localhost:5984",
                user_env="TEST_USER",
                pass_env="TEST_PASS",
            )

    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    @patch("lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client")
    def test_find_documents_success(self, mock_create_client):
        """Test a successful Mango query returns the docs list."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_client.post_find.return_value.get_result.return_value = {
            "docs": [{"_id": "a"}, {"_id": "b"}]
        }

        handler = CouchDBHandler(
            db_name="test_db",
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )

        docs = handler.find_documents({"status": "ready"})
        self.assertEqual(len(docs), 2)
        self.assertEqual(docs[0]["_id"], "a")
        mock_client.post_find.assert_called_once_with(
            db="test_db", selector={"status": "ready"}, fields=[], limit=200
        )

    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    @patch("lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client")
    def test_find_documents_non_dict_result_returns_empty(self, mock_create_client):
        """Test that a non-dict post_find result returns an empty list."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_client.post_find.return_value.get_result.return_value = "unexpected"

        handler = CouchDBHandler(
            db_name="test_db",
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )

        docs = handler.find_documents({"status": "ready"})
        self.assertEqual(docs, [])

    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    @patch("lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client")
    def test_find_documents_api_exception_re_raises(self, mock_create_client):
        """Test that an ApiException from post_find is re-raised."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_client.post_find.side_effect = MockApiException("bad request", code=400)

        handler = CouchDBHandler(
            db_name="test_db",
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )

        with self.assertRaises(MockApiException):
            handler.find_documents({"status": "ready"})

    @patch.dict(os.environ, {"TEST_USER": "admin", "TEST_PASS": "secret"})
    @patch("lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client")
    def test_find_documents_generic_exception_re_raises(self, mock_create_client):
        """Test that a generic exception from post_find is re-raised."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_client.post_find.side_effect = RuntimeError("network failure")

        handler = CouchDBHandler(
            db_name="test_db",
            url="http://localhost:5984",
            user_env="TEST_USER",
            pass_env="TEST_PASS",
        )

        with self.assertRaises(RuntimeError):
            handler.find_documents({"status": "ready"})


class TestCouchDBHandlerFetchChangesRaw(unittest.TestCase):
    """Tests for CouchDBHandler.fetch_changes_raw using mocked requests.get."""

    def setUp(self):
        """Create a handler with mocked factory and env vars."""
        with (
            patch(
                "lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client"
            ) as mock_factory,
            patch.dict(os.environ, {"FC_USER": "admin", "FC_PASS": "secret"}),
        ):
            mock_factory.return_value = MagicMock()
            self.handler = CouchDBHandler(
                db_name="test_db",
                url="http://localhost:5984",
                user_env="FC_USER",
                pass_env="FC_PASS",
            )
        # handler._url = "http://localhost:5984", handler._auth = ("admin", "secret")

    def _make_response(self, results=None, last_seq="1-abc", pending=0):
        """Helper: return a mock requests.Response with the given JSON payload."""
        mock_resp = Mock()
        mock_resp.json.return_value = {
            "results": results or [],
            "last_seq": last_seq,
            "pending": pending,
        }
        mock_resp.raise_for_status = Mock()
        return mock_resp

    @patch("lib.couchdb.couchdb_connection.requests.get")
    def test_fetch_changes_raw_basic(self, mock_get):
        """Test basic success: correct URL, params, and returned ChangesBatch."""
        mock_get.return_value = self._make_response(
            results=[
                {"id": "doc1", "seq": "1-abc", "changes": [{"rev": "1-r1"}]},
            ],
            last_seq="1-abc",
            pending=0,
        )

        batch = self.handler.fetch_changes_raw(since="0")

        mock_get.assert_called_once()
        call_kwargs = mock_get.call_args
        self.assertIn("http://localhost:5984/test_db/_changes", call_kwargs[0][0])
        self.assertEqual(call_kwargs[1]["params"]["feed"], "normal")
        self.assertEqual(call_kwargs[1]["params"]["since"], "0")
        self.assertEqual(call_kwargs[1]["params"]["include_docs"], "false")
        self.assertEqual(call_kwargs[1]["auth"], ("admin", "secret"))

        self.assertEqual(len(batch.rows), 1)
        self.assertEqual(batch.rows[0].id, "doc1")
        self.assertEqual(batch.rows[0].rev, "1-r1")
        self.assertEqual(batch.last_seq, "1-abc")
        self.assertEqual(batch.pending, 0)

    @patch("lib.couchdb.couchdb_connection.requests.get")
    def test_fetch_changes_raw_since_none_defaults_to_zero(self, mock_get):
        """Test that since=None sends '0' to CouchDB."""
        mock_get.return_value = self._make_response()

        self.handler.fetch_changes_raw(since=None)

        params = mock_get.call_args[1]["params"]
        self.assertEqual(params["since"], "0")

    @patch("lib.couchdb.couchdb_connection.requests.get")
    def test_fetch_changes_raw_limit_param(self, mock_get):
        """Test that limit is included in params when specified."""
        mock_get.return_value = self._make_response()

        self.handler.fetch_changes_raw(since="0", limit=50)

        params = mock_get.call_args[1]["params"]
        self.assertEqual(params["limit"], 50)

    @patch("lib.couchdb.couchdb_connection.requests.get")
    def test_fetch_changes_raw_no_limit_omits_param(self, mock_get):
        """Test that limit is omitted from params when not specified."""
        mock_get.return_value = self._make_response()

        self.handler.fetch_changes_raw(since="0")

        params = mock_get.call_args[1]["params"]
        self.assertNotIn("limit", params)

    @patch("lib.couchdb.couchdb_connection.requests.get")
    def test_fetch_changes_raw_longpoll_mode(self, mock_get):
        """Test that longpoll feed adds timeout param and uses correct socket timeout."""
        mock_get.return_value = self._make_response()

        self.handler.fetch_changes_raw(since="5", feed="longpoll", timeout_ms=30_000)

        call_kwargs = mock_get.call_args[1]
        params = call_kwargs["params"]
        self.assertEqual(params["feed"], "longpoll")
        self.assertEqual(params["timeout"], 30_000)
        # Socket timeout = 30_000 / 1000 + 5 = 35.0
        self.assertAlmostEqual(call_kwargs["timeout"], 35.0)

    @patch("lib.couchdb.couchdb_connection.requests.get")
    def test_fetch_changes_raw_normal_mode_no_timeout_param(self, mock_get):
        """Test that normal feed does not add the CouchDB timeout param."""
        mock_get.return_value = self._make_response()

        self.handler.fetch_changes_raw(since="0", feed="normal")

        params = mock_get.call_args[1]["params"]
        self.assertNotIn("timeout", params)

    @patch("lib.couchdb.couchdb_connection.requests.get")
    def test_fetch_changes_raw_deleted_row(self, mock_get):
        """Test that deleted=True in a result row is captured correctly."""
        mock_get.return_value = self._make_response(
            results=[
                {
                    "id": "doc-deleted",
                    "seq": "5-xyz",
                    "deleted": True,
                    "changes": [{"rev": "3-r"}],
                }
            ],
            last_seq="5-xyz",
        )

        batch = self.handler.fetch_changes_raw(since="4")

        self.assertEqual(len(batch.rows), 1)
        self.assertTrue(batch.rows[0].deleted)
        self.assertEqual(batch.rows[0].id, "doc-deleted")

    @patch("lib.couchdb.couchdb_connection.requests.get")
    def test_fetch_changes_raw_pending_extracted(self, mock_get):
        """Test that the pending count is extracted from the response."""
        mock_get.return_value = self._make_response(pending=42, last_seq="10-z")

        batch = self.handler.fetch_changes_raw(since="0")

        self.assertEqual(batch.pending, 42)
        self.assertEqual(batch.last_seq, "10-z")

    @patch("lib.couchdb.couchdb_connection.requests.get")
    def test_fetch_changes_raw_row_without_changes_has_none_rev(self, mock_get):
        """Test that a row with no changes list produces rev=None."""
        mock_get.return_value = self._make_response(
            results=[{"id": "doc1", "seq": "1-abc"}],  # no "changes" key
            last_seq="1-abc",
        )

        batch = self.handler.fetch_changes_raw(since="0")

        self.assertIsNone(batch.rows[0].rev)

    @patch("lib.couchdb.couchdb_connection.requests.get")
    def test_fetch_changes_raw_http_error_propagates(self, mock_get):
        """Test that an HTTP error from raise_for_status propagates."""
        mock_resp = Mock()
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=Mock(status_code=503)
        )
        mock_get.return_value = mock_resp

        with self.assertRaises(requests.exceptions.HTTPError):
            self.handler.fetch_changes_raw(since="0")


class TestTransientErrorClassifiers(unittest.TestCase):
    """Tests for is_transient_poll_error and is_transient_doc_fetch_error."""

    # --- is_transient_poll_error ---

    def test_poll_error_requests_timeout_is_transient(self):
        self.assertTrue(is_transient_poll_error(requests.exceptions.Timeout()))

    def test_poll_error_requests_connection_error_is_transient(self):
        self.assertTrue(is_transient_poll_error(requests.exceptions.ConnectionError()))

    def test_poll_error_5xx_http_error_is_transient(self):
        resp = Mock()
        resp.status_code = 503
        exc = requests.exceptions.HTTPError(response=resp)
        self.assertTrue(is_transient_poll_error(exc))

    def test_poll_error_4xx_http_error_is_not_transient(self):
        resp = Mock()
        resp.status_code = 404
        exc = requests.exceptions.HTTPError(response=resp)
        self.assertFalse(is_transient_poll_error(exc))

    def test_poll_error_http_error_no_response_is_not_transient(self):
        exc = requests.exceptions.HTTPError(response=None)
        self.assertFalse(is_transient_poll_error(exc))

    def test_poll_error_generic_exception_is_not_transient(self):
        self.assertFalse(is_transient_poll_error(ValueError("unexpected")))

    # --- is_transient_doc_fetch_error ---

    def test_doc_fetch_error_500_is_transient(self):
        exc = MockApiException("server error", code=500)
        self.assertTrue(is_transient_doc_fetch_error(exc))

    def test_doc_fetch_error_503_is_transient(self):
        exc = MockApiException("unavailable", code=503)
        self.assertTrue(is_transient_doc_fetch_error(exc))

    def test_doc_fetch_error_429_is_transient(self):
        exc = MockApiException("rate limited", code=429)
        self.assertTrue(is_transient_doc_fetch_error(exc))

    def test_doc_fetch_error_404_is_not_transient(self):
        exc = MockApiException("not found", code=404)
        self.assertFalse(is_transient_doc_fetch_error(exc))

    def test_doc_fetch_error_400_is_not_transient(self):
        exc = MockApiException("bad request", code=400)
        self.assertFalse(is_transient_doc_fetch_error(exc))

    def test_doc_fetch_error_requests_timeout_is_transient(self):
        self.assertTrue(is_transient_doc_fetch_error(requests.exceptions.Timeout()))

    def test_doc_fetch_error_requests_connection_error_is_transient(self):
        self.assertTrue(
            is_transient_doc_fetch_error(requests.exceptions.ConnectionError())
        )

    def test_doc_fetch_error_generic_exception_is_not_transient(self):
        self.assertFalse(is_transient_doc_fetch_error(RuntimeError("unexpected")))


class TestCouchDBHandlerPutDocument(unittest.TestCase):
    """Tests for CouchDBHandler.put_document."""

    def setUp(self):
        with (
            patch(
                "lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client"
            ) as mock_factory,
            patch.dict(os.environ, {"PD_USER": "admin", "PD_PASS": "secret"}),
        ):
            self.mock_client = MagicMock()
            mock_factory.return_value = self.mock_client
            self.handler = CouchDBHandler(
                db_name="test_db",
                url="http://localhost:5984",
                user_env="PD_USER",
                pass_env="PD_PASS",
            )
        self.mock_client.reset_mock()

    def _set_put_result(self, result):
        self.mock_client.put_document.return_value.get_result.return_value = result

    def test_create_returns_sdk_response(self):
        """put_document without rev returns the SDK response dict unchanged."""
        expected = {"id": "doc1", "rev": "1-abc", "ok": True}
        self._set_put_result(expected)

        result = self.handler.put_document("doc1", {"status": "ready"})

        self.mock_client.put_document.assert_called_once()
        self.assertEqual(result, expected)

    def test_create_calls_sdk_with_correct_db_and_doc_id(self):
        """put_document forwards db and doc_id as kwargs to the SDK."""
        self._set_put_result({"id": "doc1", "rev": "1-x", "ok": True})

        self.handler.put_document("doc1", {"status": "ready"})

        call_kwargs = self.mock_client.put_document.call_args[1]
        self.assertEqual(call_kwargs["db"], "test_db")
        self.assertEqual(call_kwargs["doc_id"], "doc1")

    def test_create_injects_id_without_rev(self):
        """Without rev arg, Document.from_dict receives _id but not _rev."""
        self._set_put_result({"id": "doc1", "rev": "1-x", "ok": True})

        with patch("lib.couchdb.couchdb_connection.cloudant_v1") as mock_cv1:
            self.handler.put_document("doc1", {"status": "ready"})
            body = mock_cv1.Document.from_dict.call_args[0][0]

        self.assertEqual(body["_id"], "doc1")
        self.assertNotIn("_rev", body)
        self.assertEqual(body["status"], "ready")

    def test_update_injects_id_and_rev(self):
        """With rev arg, Document.from_dict receives both _id and _rev."""
        self._set_put_result({"id": "doc1", "rev": "2-y", "ok": True})

        with patch("lib.couchdb.couchdb_connection.cloudant_v1") as mock_cv1:
            self.handler.put_document("doc1", {"status": "done"}, rev="1-abc")
            body = mock_cv1.Document.from_dict.call_args[0][0]

        self.assertEqual(body["_id"], "doc1")
        self.assertEqual(body["_rev"], "1-abc")

    def test_raises_if_doc_contains_id(self):
        """ValueError is raised if caller's doc contains _id."""
        with self.assertRaises(ValueError) as ctx:
            self.handler.put_document("doc1", {"_id": "doc1", "status": "x"})
        self.assertIn("_id", str(ctx.exception))
        self.mock_client.put_document.assert_not_called()

    def test_raises_if_doc_contains_rev(self):
        """ValueError is raised if caller's doc contains _rev."""
        with self.assertRaises(ValueError) as ctx:
            self.handler.put_document("doc1", {"_rev": "1-abc", "status": "x"})
        self.assertIn("_rev", str(ctx.exception))
        self.mock_client.put_document.assert_not_called()

    def test_raises_if_doc_contains_both_reserved_keys(self):
        """ValueError is raised if caller's doc contains both _id and _rev."""
        with self.assertRaises(ValueError):
            self.handler.put_document(
                "doc1", {"_id": "doc1", "_rev": "1-x", "status": "x"}
            )
        self.mock_client.put_document.assert_not_called()

    def test_does_not_mutate_caller_doc(self):
        """put_document must not modify the caller's dict."""
        self._set_put_result({"id": "doc1", "rev": "1-abc", "ok": True})
        original = {"status": "ready", "count": 5}
        snapshot = dict(original)

        self.handler.put_document("doc1", original)

        self.assertEqual(original, snapshot)
        self.assertNotIn("_id", original)
        self.assertNotIn("_rev", original)

    def test_409_api_exception_propagates(self):
        """Conflict (409) from the SDK propagates unchanged."""
        self.mock_client.put_document.side_effect = MockApiException(
            "conflict", code=409
        )

        with self.assertRaises(MockApiException) as ctx:
            self.handler.put_document("doc1", {"status": "x"})
        self.assertEqual(ctx.exception.status_code, 409)

    def test_404_api_exception_propagates(self):
        """Not-found (404) from the SDK propagates unchanged (update on absent doc)."""
        self.mock_client.put_document.side_effect = MockApiException(
            "not found", code=404
        )

        with self.assertRaises(MockApiException) as ctx:
            self.handler.put_document("doc1", {"status": "x"}, rev="1-abc")
        self.assertEqual(ctx.exception.status_code, 404)

    def test_generic_exception_propagates(self):
        """Non-ApiException from the SDK propagates unchanged."""
        self.mock_client.put_document.side_effect = RuntimeError("network failure")

        with self.assertRaises(RuntimeError):
            self.handler.put_document("doc1", {"status": "x"})


class TestCouchDBHandlerPostDocument(unittest.TestCase):
    """Tests for CouchDBHandler.post_document."""

    def setUp(self):
        with (
            patch(
                "lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client"
            ) as mock_factory,
            patch.dict(os.environ, {"PD_USER": "admin", "PD_PASS": "secret"}),
        ):
            self.mock_client = MagicMock()
            mock_factory.return_value = self.mock_client
            self.handler = CouchDBHandler(
                db_name="test_db",
                url="http://localhost:5984",
                user_env="PD_USER",
                pass_env="PD_PASS",
            )
        self.mock_client.reset_mock()

    def _set_post_result(self, result):
        self.mock_client.post_document.return_value.get_result.return_value = result

    def test_returns_sdk_response(self):
        """post_document returns the SDK response dict unchanged."""
        expected = {"id": "gen-uuid-abc", "rev": "1-new", "ok": True}
        self._set_post_result(expected)

        result = self.handler.post_document({"status": "ready"})

        self.assertEqual(result, expected)

    def test_calls_sdk_with_correct_db(self):
        """post_document forwards correct db to the SDK."""
        self._set_post_result({"id": "x", "rev": "1-r", "ok": True})

        self.handler.post_document({"status": "ready"})

        call_kwargs = self.mock_client.post_document.call_args[1]
        self.assertEqual(call_kwargs["db"], "test_db")

    def test_does_not_inject_id_into_body(self):
        """post_document must NOT inject _id — CouchDB generates it."""
        self._set_post_result({"id": "gen-x", "rev": "1-r", "ok": True})

        with patch("lib.couchdb.couchdb_connection.cloudant_v1") as mock_cv1:
            self.handler.post_document({"type": "run", "status": "new"})
            body = mock_cv1.Document.from_dict.call_args[0][0]

        self.assertNotIn("_id", body)
        self.assertNotIn("_rev", body)
        self.assertEqual(body["type"], "run")

    def test_does_not_mutate_caller_doc(self):
        """post_document must not modify the caller's dict."""
        self._set_post_result({"id": "gen-x", "rev": "1-r", "ok": True})
        original = {"type": "run", "status": "new"}
        snapshot = dict(original)

        self.handler.post_document(original)

        self.assertEqual(original, snapshot)

    def test_raises_if_doc_contains_id(self):
        """ValueError is raised if caller's doc contains _id."""
        with self.assertRaises(ValueError) as ctx:
            self.handler.post_document({"_id": "existing", "status": "x"})
        self.assertIn("_id", str(ctx.exception))
        self.mock_client.post_document.assert_not_called()

    def test_raises_if_doc_contains_rev(self):
        """ValueError is raised if caller's doc contains _rev."""
        with self.assertRaises(ValueError) as ctx:
            self.handler.post_document({"_rev": "1-abc", "status": "x"})
        self.assertIn("_rev", str(ctx.exception))
        self.mock_client.post_document.assert_not_called()

    def test_raises_if_doc_contains_both_reserved_keys(self):
        """ValueError is raised if caller's doc contains both _id and _rev."""
        with self.assertRaises(ValueError):
            self.handler.post_document({"_id": "x", "_rev": "1-abc", "status": "x"})
        self.mock_client.post_document.assert_not_called()

    def test_api_exception_propagates(self):
        """ApiException from the SDK propagates unchanged."""
        self.mock_client.post_document.side_effect = MockApiException(
            "server error", code=500
        )

        with self.assertRaises(MockApiException) as ctx:
            self.handler.post_document({"status": "x"})
        self.assertEqual(ctx.exception.status_code, 500)

    def test_generic_exception_propagates(self):
        """Non-ApiException from the SDK propagates unchanged."""
        self.mock_client.post_document.side_effect = RuntimeError("network failure")

        with self.assertRaises(RuntimeError):
            self.handler.post_document({"status": "x"})


class TestCouchDBHandlerQueryView(unittest.TestCase):
    """Tests for CouchDBHandler.query_view."""

    def setUp(self):
        with (
            patch(
                "lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client"
            ) as mock_factory,
            patch.dict(os.environ, {"QV_USER": "admin", "QV_PASS": "secret"}),
        ):
            self.mock_client = MagicMock()
            mock_factory.return_value = self.mock_client
            self.handler = CouchDBHandler(
                db_name="test_db",
                url="http://localhost:5984",
                user_env="QV_USER",
                pass_env="QV_PASS",
            )
        self.mock_client.reset_mock()

    def _set_view_result(self, result):
        self.mock_client.post_view.return_value.get_result.return_value = result

    def test_returns_raw_result_dict(self):
        """query_view returns the full result dict from the SDK."""
        rows = [{"id": "doc1", "key": "k", "value": None}]
        self._set_view_result({"rows": rows, "total_rows": 1, "offset": 0})

        result = self.handler.query_view("design_doc", "my_view", key="k")

        self.assertEqual(result["rows"], rows)

    def test_calls_sdk_with_mandatory_kwargs(self):
        """query_view always sends db, ddoc, view, include_docs, reduce, stable."""
        self._set_view_result({"rows": []})

        self.handler.query_view("design_doc", "my_view")

        call_kwargs = self.mock_client.post_view.call_args[1]
        self.assertEqual(call_kwargs["db"], "test_db")
        self.assertEqual(call_kwargs["ddoc"], "design_doc")
        self.assertEqual(call_kwargs["view"], "my_view")
        self.assertIn("include_docs", call_kwargs)
        self.assertIn("reduce", call_kwargs)
        self.assertIn("stable", call_kwargs)

    def test_default_flags_are_false(self):
        """Default include_docs, reduce, and stable are all False."""
        self._set_view_result({"rows": []})

        self.handler.query_view("ddoc", "view")

        call_kwargs = self.mock_client.post_view.call_args[1]
        self.assertFalse(call_kwargs["include_docs"])
        self.assertFalse(call_kwargs["reduce"])
        self.assertFalse(call_kwargs["stable"])

    def test_key_included_when_provided(self):
        """key is sent to the SDK when not None."""
        self._set_view_result({"rows": []})

        self.handler.query_view("ddoc", "view", key="flowcell-1")

        call_kwargs = self.mock_client.post_view.call_args[1]
        self.assertEqual(call_kwargs["key"], "flowcell-1")

    def test_key_omitted_when_none(self):
        """key is not sent to the SDK when None (the default)."""
        self._set_view_result({"rows": []})

        self.handler.query_view("ddoc", "view", key=None)

        call_kwargs = self.mock_client.post_view.call_args[1]
        self.assertNotIn("key", call_kwargs)

    def test_limit_included_when_provided(self):
        """limit is sent to the SDK when not None."""
        self._set_view_result({"rows": []})

        self.handler.query_view("ddoc", "view", limit=10)

        call_kwargs = self.mock_client.post_view.call_args[1]
        self.assertEqual(call_kwargs["limit"], 10)

    def test_limit_omitted_when_none(self):
        """limit is not sent to the SDK when None (the default)."""
        self._set_view_result({"rows": []})

        self.handler.query_view("ddoc", "view", limit=None)

        call_kwargs = self.mock_client.post_view.call_args[1]
        self.assertNotIn("limit", call_kwargs)

    def test_key_and_limit_both_sent_when_provided(self):
        """key and limit are both included when both are specified."""
        self._set_view_result({"rows": []})

        self.handler.query_view("ddoc", "view", key="k1", limit=2)

        call_kwargs = self.mock_client.post_view.call_args[1]
        self.assertEqual(call_kwargs["key"], "k1")
        self.assertEqual(call_kwargs["limit"], 2)

    def test_include_docs_true_forwarded(self):
        """include_docs=True is forwarded to the SDK."""
        self._set_view_result({"rows": []})

        self.handler.query_view("ddoc", "view", include_docs=True)

        call_kwargs = self.mock_client.post_view.call_args[1]
        self.assertTrue(call_kwargs["include_docs"])

    def test_reduce_true_forwarded(self):
        """reduce=True is forwarded to the SDK."""
        self._set_view_result({"rows": []})

        self.handler.query_view("ddoc", "view", reduce=True)

        call_kwargs = self.mock_client.post_view.call_args[1]
        self.assertTrue(call_kwargs["reduce"])

    def test_stable_true_forwarded(self):
        """stable=True is forwarded to the SDK."""
        self._set_view_result({"rows": []})

        self.handler.query_view("ddoc", "view", stable=True)

        call_kwargs = self.mock_client.post_view.call_args[1]
        self.assertTrue(call_kwargs["stable"])

    def test_non_dict_result_returns_empty_rows(self):
        """Non-dict SDK result returns {"rows": []}."""
        self.mock_client.post_view.return_value.get_result.return_value = "not-a-dict"

        result = self.handler.query_view("ddoc", "view")

        self.assertEqual(result, {"rows": []})

    def test_404_api_exception_propagates(self):
        """ApiException from the SDK propagates unchanged."""
        self.mock_client.post_view.side_effect = MockApiException("not found", code=404)

        with self.assertRaises(MockApiException) as ctx:
            self.handler.query_view("ddoc", "view")
        self.assertEqual(ctx.exception.status_code, 404)

    def test_500_api_exception_propagates(self):
        """5xx ApiException propagates unchanged."""
        self.mock_client.post_view.side_effect = MockApiException(
            "server error", code=500
        )

        with self.assertRaises(MockApiException) as ctx:
            self.handler.query_view("ddoc", "view")
        self.assertEqual(ctx.exception.status_code, 500)

    def test_generic_exception_propagates(self):
        """Non-ApiException from the SDK propagates unchanged."""
        self.mock_client.post_view.side_effect = RuntimeError("connection reset")

        with self.assertRaises(RuntimeError):
            self.handler.query_view("ddoc", "view")


if __name__ == "__main__":
    unittest.main()
