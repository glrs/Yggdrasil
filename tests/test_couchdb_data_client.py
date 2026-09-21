"""Unit tests for CouchDB data clients (planning + execution).

All tests use a mock CouchDBHandler — no real CouchDB required.
"""

import unittest
from unittest.mock import MagicMock

from ibm_cloud_sdk_core.api_exception import ApiException

from yggdrasil.flow.data_access.couchdb_data import (
    CouchDBExecutionClient,
    CouchDBPlanningClient,
    _CouchDBSyncOps,
    _CouchDBWriteOps,
)
from yggdrasil.flow.data_access.errors import (
    DataAccessDeniedError,
    DataAccessNotFoundError,
    DataAccessWriteError,
)
from yggdrasil.flow.data_access.models import (
    DataAccessTraceContext,
    DataAccessWriteResult,
)
from yggdrasil.flow.events.correlation import ExecutionCorrelation

# ---------------------------------------------------------------------------
# Setup helpers
# ---------------------------------------------------------------------------


def make_handler_mock(
    *,
    get_result=None,
    find_result=None,
    put_result=None,
    post_doc_result=None,
    view_result=None,
):
    handler = MagicMock()
    handler.fetch_document_by_id.return_value = get_result
    handler.find_documents.return_value = find_result or []
    handler.put_document.return_value = put_result or {
        "id": "x",
        "rev": "1-abc",
        "ok": True,
    }
    handler.post_document.return_value = post_doc_result or {
        "id": "gen-uuid-123",
        "rev": "1-new",
        "ok": True,
    }
    handler.query_view.return_value = view_result or {"rows": []}
    return handler


def make_planning_client(handler, options=None):
    ops = _CouchDBSyncOps(handler, options or {})
    return CouchDBPlanningClient(ops)


def make_execution_client(
    handler,
    *,
    permissions=frozenset({"read", "write"}),
    options=None,
    trace_context=None,
):
    ops = _CouchDBSyncOps(handler, options or {})
    write_ops = _CouchDBWriteOps(handler)
    return CouchDBExecutionClient(
        ops=ops,
        write_ops=write_ops,
        permissions=permissions,
        realm_id="test_realm",
        connection_name="test_db",
        resource="test_db",
        trace_context=trace_context,
    )


def make_api_exception(status_code: int) -> ApiException:
    exc = ApiException(status_code, message=f"HTTP {status_code}")
    return exc


# ---------------------------------------------------------------------------
# CouchDBPlanningClient
# ---------------------------------------------------------------------------


class TestCouchDBPlanningClient(unittest.IsolatedAsyncioTestCase):
    """Async tests for CouchDBPlanningClient."""

    async def test_get_returns_doc(self):
        handler = make_handler_mock(get_result={"_id": "x"})
        client = make_planning_client(handler)
        result = await client.get("x")
        self.assertEqual(result, {"_id": "x"})

    async def test_get_returns_none_if_not_found(self):
        handler = make_handler_mock(get_result=None)
        client = make_planning_client(handler)
        result = await client.get("x")
        self.assertIsNone(result)

    async def test_find_clamps_limit_to_max_limit(self):
        handler = make_handler_mock(find_result=[])
        client = make_planning_client(handler, options={"max_limit": 10})
        await client.find({"type": "run"}, limit=100)
        handler.find_documents.assert_called_once_with({"type": "run"}, limit=10)

    async def test_find_uses_max_limit_when_no_limit_given(self):
        handler = make_handler_mock(find_result=[])
        client = make_planning_client(handler, options={"max_limit": 5})
        await client.find({"type": "run"})
        handler.find_documents.assert_called_once_with({"type": "run"}, limit=5)

    async def test_require_raises_not_found(self):
        handler = make_handler_mock(get_result=None)
        client = make_planning_client(handler)
        with self.assertRaises(DataAccessNotFoundError):
            await client.require("x")

    async def test_require_returns_doc_when_found(self):
        handler = make_handler_mock(get_result={"_id": "x", "val": 1})
        client = make_planning_client(handler)
        doc = await client.require("x")
        self.assertEqual(doc["val"], 1)

    def test_planning_client_has_no_save_method(self):
        handler = make_handler_mock()
        client = make_planning_client(handler)
        self.assertFalse(hasattr(client, "save"))


# ---------------------------------------------------------------------------
# CouchDBExecutionClient — reads
# ---------------------------------------------------------------------------


class TestCouchDBExecutionClientReads(unittest.TestCase):
    """Tests for CouchDBExecutionClient sync read methods."""

    def test_get_is_synchronous(self):
        """get() returns a plain dict, not a coroutine."""
        import inspect

        handler = make_handler_mock(get_result={"_id": "x"})
        client = make_execution_client(handler, permissions=frozenset({"read"}))
        result = client.get("x")
        self.assertFalse(inspect.iscoroutine(result))
        self.assertEqual(result, {"_id": "x"})

    def test_get_denied_without_read_permission(self):
        handler = make_handler_mock()
        client = make_execution_client(handler, permissions=frozenset({"write"}))
        with self.assertRaises(DataAccessDeniedError):
            client.get("x")

    def test_find_denied_without_read_permission(self):
        handler = make_handler_mock()
        client = make_execution_client(handler, permissions=frozenset({"write"}))
        with self.assertRaises(DataAccessDeniedError):
            client.find({})

    def test_get_succeeds_with_read_permission(self):
        handler = make_handler_mock(get_result={"_id": "x"})
        client = make_execution_client(handler, permissions=frozenset({"read"}))
        result = client.get("x")
        self.assertEqual(result, {"_id": "x"})

    def test_find_clamps_limit(self):
        handler = make_handler_mock(find_result=[])
        client = make_execution_client(
            handler, permissions=frozenset({"read"}), options={"max_limit": 5}
        )
        client.find({"status": "new"}, limit=100)
        handler.find_documents.assert_called_once_with({"status": "new"}, limit=5)

    def test_require_raises_not_found_when_absent(self):
        handler = make_handler_mock(get_result=None)
        client = make_execution_client(handler)
        with self.assertRaises(DataAccessNotFoundError):
            client.require("missing")


# ---------------------------------------------------------------------------
# CouchDBExecutionClient.save(doc_id=...) — mode tests
# ---------------------------------------------------------------------------


class TestCouchDBExecutionClientSaveByDocIdRetryPaths(unittest.TestCase):
    def test_create_returns_created_status(self):
        handler = make_handler_mock(put_result={"rev": "1-abc", "ok": True})
        client = make_execution_client(handler)
        result = client.save({}, doc_id="x", mode="create")
        handler.fetch_document_by_id.assert_not_called()
        self.assertEqual(result.status, "created")
        self.assertIsNone(result.old_rev)
        self.assertEqual(result.new_rev, "1-abc")

    def test_create_fails_on_409(self):
        handler = make_handler_mock()
        handler.put_document.side_effect = make_api_exception(409)
        client = make_execution_client(handler)
        with self.assertRaises(DataAccessWriteError) as ctx:
            client.save({}, doc_id="x", mode="create")
        self.assertIn("already exists", str(ctx.exception))

    def test_update_returns_updated_status(self):
        handler = make_handler_mock(
            get_result={"_id": "x", "_rev": "1-old"},
            put_result={"rev": "2-new", "ok": True},
        )
        client = make_execution_client(handler)
        result = client.save({"val": 2}, doc_id="x", mode="update")
        self.assertEqual(result.status, "updated")
        self.assertEqual(result.old_rev, "1-old")
        self.assertEqual(result.new_rev, "2-new")

    def test_update_fails_when_doc_absent(self):
        handler = make_handler_mock(get_result=None)
        client = make_execution_client(handler)
        with self.assertRaises(DataAccessWriteError) as ctx:
            client.save({}, doc_id="x", mode="update")
        self.assertIn("does not exist", str(ctx.exception))

    def test_update_fetch_failure_raises_write_error(self):
        """ApiException during pre-fetch is wrapped as DataAccessWriteError."""
        handler = make_handler_mock()
        handler.fetch_document_by_id.side_effect = make_api_exception(500)
        client = make_execution_client(handler)
        with self.assertRaises(DataAccessWriteError) as ctx:
            client.save({}, doc_id="x", mode="update")
        self.assertIn("pre-check", str(ctx.exception))

    def test_upsert_creates_when_absent(self):
        handler = make_handler_mock(
            get_result=None,
            put_result={"rev": "1-abc", "ok": True},
        )
        client = make_execution_client(handler)
        result = client.save({}, doc_id="x", mode="upsert")
        self.assertEqual(result.status, "created")

    def test_upsert_updates_when_present(self):
        handler = make_handler_mock(
            get_result={"_id": "x", "_rev": "1-old"},
            put_result={"rev": "2-new", "ok": True},
        )
        client = make_execution_client(handler)
        result = client.save({"val": 2}, doc_id="x", mode="upsert")
        self.assertEqual(result.status, "updated")
        self.assertEqual(result.old_rev, "1-old")

    def test_upsert_retries_once_on_409(self):
        handler = make_handler_mock()
        # First fetch returns present doc
        handler.fetch_document_by_id.side_effect = [
            {"_id": "x", "_rev": "1-old"},  # first fetch
            {"_id": "x", "_rev": "2-new"},  # retry fetch
        ]
        # First put raises 409, second succeeds
        handler.put_document.side_effect = [
            make_api_exception(409),
            {"rev": "3-xyz", "ok": True},
        ]
        client = make_execution_client(handler)
        result = client.save({"val": 1}, doc_id="x", mode="upsert")
        self.assertEqual(result.status, "updated")
        self.assertEqual(result.new_rev, "3-xyz")

    def test_upsert_raises_after_two_409s(self):
        handler = make_handler_mock()
        handler.fetch_document_by_id.return_value = {"_id": "x", "_rev": "1-old"}
        handler.put_document.side_effect = make_api_exception(409)
        client = make_execution_client(handler)
        with self.assertRaises(DataAccessWriteError) as ctx:
            client.save({}, doc_id="x", mode="upsert")
        self.assertIn("consecutive", str(ctx.exception))

    def test_upsert_fetch_failure_raises_write_error(self):
        from requests.exceptions import RequestException

        handler = make_handler_mock()
        handler.fetch_document_by_id.side_effect = RequestException("timeout")
        client = make_execution_client(handler)
        with self.assertRaises(DataAccessWriteError) as ctx:
            client.save({}, doc_id="x", mode="upsert")
        self.assertIn("pre-check", str(ctx.exception))

    def test_upsert_retry_fetch_failure_raises_write_error(self):
        """Fetch failure during retry raises DataAccessWriteError."""
        from requests.exceptions import RequestException

        handler = make_handler_mock()
        # First fetch succeeds, first put raises 409, retry fetch fails
        handler.fetch_document_by_id.side_effect = [
            {"_id": "x", "_rev": "1-old"},
            RequestException("timeout on retry"),
        ]
        handler.put_document.side_effect = make_api_exception(409)
        client = make_execution_client(handler)
        with self.assertRaises(DataAccessWriteError):
            client.save({}, doc_id="x", mode="upsert")

    # --- Fix 3: create-path 409 retry ---

    def test_upsert_create_path_409_retries_and_updates(self):
        """Document absent at fetch, create returns 409, retry updates successfully."""
        handler = make_handler_mock()
        handler.fetch_document_by_id.side_effect = [
            None,  # initial fetch: absent
            {"_id": "x", "_rev": "1-abc"},  # retry fetch after create-path 409
        ]
        handler.put_document.side_effect = [
            make_api_exception(409),  # first create attempt: 409
            {"rev": "2-xyz", "ok": True},  # retry update: success
        ]
        client = make_execution_client(handler)
        result = client.save({}, doc_id="x", mode="upsert")
        self.assertEqual(result.status, "updated")
        self.assertEqual(result.new_rev, "2-xyz")

    def test_upsert_create_path_two_consecutive_409s_raise_with_conflict_message(self):
        """Create-path 409, refetch present, retry update 409 → 'two consecutive' message."""
        handler = make_handler_mock()
        handler.fetch_document_by_id.side_effect = [
            None,  # initial fetch: absent
            {"_id": "x", "_rev": "1-abc"},  # retry fetch after create-path 409
        ]
        handler.put_document.side_effect = [
            make_api_exception(409),  # first create: 409
            make_api_exception(409),  # retry update: 409 again
        ]
        client = make_execution_client(handler)
        with self.assertRaises(DataAccessWriteError) as ctx:
            client.save({}, doc_id="x", mode="upsert")
        self.assertIn("consecutive", str(ctx.exception))

    def test_upsert_create_path_409_refetch_absent_retries_create(self):
        """Create-path 409, refetch returns None, second create succeeds."""
        handler = make_handler_mock()
        handler.fetch_document_by_id.side_effect = [
            None,  # initial fetch: absent
            None,  # retry fetch: still absent
        ]
        handler.put_document.side_effect = [
            make_api_exception(409),  # first create: 409
            {"rev": "1-abc", "ok": True},  # second create: success
        ]
        client = make_execution_client(handler)
        result = client.save({}, doc_id="x", mode="upsert")
        self.assertEqual(result.status, "created")

    def test_upsert_create_path_non_409_error_raises(self):
        """Non-409 ApiException on create path propagates as DataAccessWriteError."""
        handler = make_handler_mock()
        handler.fetch_document_by_id.return_value = None
        handler.put_document.side_effect = make_api_exception(500)
        client = make_execution_client(handler)
        with self.assertRaises(DataAccessWriteError) as ctx:
            client.save({}, doc_id="x", mode="upsert")
        self.assertIn("500", str(ctx.exception))

    # --- Fix 4: retry transport errors wrapped ---

    def test_upsert_update_path_retry_transport_error_wrapped(self):
        """RequestException on update-path retry is wrapped as DataAccessWriteError."""
        from requests.exceptions import RequestException

        handler = make_handler_mock()
        handler.fetch_document_by_id.side_effect = [
            {"_id": "x", "_rev": "1-old"},  # initial fetch: present
            {"_id": "x", "_rev": "2-new"},  # retry fetch after 409
        ]
        handler.put_document.side_effect = [
            make_api_exception(409),  # first write: 409
            RequestException("connection reset"),  # retry write: transport error
        ]
        client = make_execution_client(handler)
        with self.assertRaises(DataAccessWriteError) as ctx:
            client.save({}, doc_id="x", mode="upsert")
        self.assertIn("connection reset", str(ctx.exception))

    # --- Fix 5: _rev guard ---

    def test_update_raises_when_existing_doc_has_no_rev(self):
        """Existing document without '_rev' raises DataAccessWriteError."""
        handler = make_handler_mock(get_result={"_id": "x", "value": 1})  # no _rev
        client = make_execution_client(handler)
        with self.assertRaises(DataAccessWriteError) as ctx:
            client.save({}, doc_id="x", mode="update")
        self.assertIn("_rev", str(ctx.exception))

    def test_upsert_raises_when_existing_doc_has_no_rev(self):
        """Existing document without '_rev' raises DataAccessWriteError in upsert."""
        handler = make_handler_mock(get_result={"_id": "x", "value": 1})  # no _rev
        client = make_execution_client(handler)
        with self.assertRaises(DataAccessWriteError) as ctx:
            client.save({}, doc_id="x", mode="upsert")
        self.assertIn("_rev", str(ctx.exception))

    def test_result_has_identity_doc_id(self):
        handler = make_handler_mock(put_result={"rev": "1-abc"})
        result = make_execution_client(handler).save({}, doc_id="x", mode="create")
        self.assertEqual(result.identity, "doc_id")

    def test_result_operation_matches_mode(self):
        handler = make_handler_mock(put_result={"rev": "1-abc"})
        result = make_execution_client(handler).save({}, doc_id="x", mode="create")
        self.assertEqual(result.operation, "create")


# ---------------------------------------------------------------------------
# CouchDBExecutionClient.save() — result shape
# ---------------------------------------------------------------------------


class TestCouchDBExecutionClientWriteResult(unittest.TestCase):
    def _make_result(self):
        handler = make_handler_mock(put_result={"rev": "1-abc", "ok": True})
        client = make_execution_client(handler)
        return client.save({"val": 1}, doc_id="x", mode="upsert")

    def test_write_result_fields(self):
        result = self._make_result()
        self.assertEqual(result.backend, "couchdb")
        self.assertEqual(result.connection_name, "test_db")
        self.assertEqual(result.resource, "test_db")
        self.assertEqual(result.operation, "upsert")
        self.assertEqual(result.doc_id, "x")
        self.assertIn(result.status, ("created", "updated"))
        self.assertEqual(result.identity, "doc_id")

    def test_write_result_status_is_str_not_bool(self):
        result = self._make_result()
        self.assertIsInstance(result.status, str)
        self.assertIn(result.status, ("created", "updated"))


# ---------------------------------------------------------------------------
# CouchDBExecutionClient.save() — event emission
# ---------------------------------------------------------------------------


class TestCouchDBExecutionClientEvents(unittest.TestCase):
    def _make_trace(self, emitter):
        return DataAccessTraceContext(
            realm="r",
            phase="execution",
            plan_id="p1",
            run_id="r1",
            step_id="s1",
            step_name="my_step",
            emitter=emitter,
        )

    def test_succeeded_event_emitted_on_success(self):
        mock_emitter = MagicMock()
        handler = make_handler_mock(put_result={"rev": "1-abc"})
        client = make_execution_client(
            handler, trace_context=self._make_trace(mock_emitter)
        )
        client.save({}, doc_id="x", mode="create")
        mock_emitter.emit.assert_called_once()
        event = mock_emitter.emit.call_args[0][0]
        self.assertEqual(event["type"], "data_access.write.succeeded")
        self.assertEqual(event["status"], "created")
        self.assertEqual(event["realm"], "r")
        self.assertEqual(event["plan_id"], "p1")
        self.assertEqual(event["doc_id"], "x")

    def test_denied_event_emitted_before_denied_error(self):
        mock_emitter = MagicMock()
        trace = self._make_trace(mock_emitter)
        handler = make_handler_mock()
        client = make_execution_client(
            handler, permissions=frozenset({"read"}), trace_context=trace
        )
        with self.assertRaises(DataAccessDeniedError):
            client.save({}, doc_id="x")
        mock_emitter.emit.assert_called_once()
        event = mock_emitter.emit.call_args[0][0]
        self.assertEqual(event["type"], "data_access.write.denied")

    def test_failed_event_emitted_on_backend_error(self):
        mock_emitter = MagicMock()
        handler = make_handler_mock()
        handler.put_document.side_effect = make_api_exception(409)
        client = make_execution_client(
            handler, trace_context=self._make_trace(mock_emitter)
        )
        with self.assertRaises(DataAccessWriteError):
            client.save({}, doc_id="x", mode="create")
        mock_emitter.emit.assert_called_once()
        event = mock_emitter.emit.call_args[0][0]
        self.assertEqual(event["type"], "data_access.write.failed")

    def test_emission_failure_does_not_suppress_write_result(self):
        mock_emitter = MagicMock()
        mock_emitter.emit.side_effect = RuntimeError("spool full")
        handler = make_handler_mock(put_result={"rev": "1-abc"})
        client = make_execution_client(
            handler, trace_context=self._make_trace(mock_emitter)
        )
        result = client.save({}, doc_id="x", mode="create")
        self.assertEqual(result.status, "created")

    def test_no_emission_when_trace_context_is_none(self):
        handler = make_handler_mock(put_result={"rev": "1-abc"})
        client = make_execution_client(handler, trace_context=None)
        result = client.save({}, doc_id="x", mode="create")
        self.assertIsNotNone(result)

    def test_spool_path_hints_in_event(self):
        mock_emitter = MagicMock()
        trace = self._make_trace(mock_emitter)
        handler = make_handler_mock(put_result={"rev": "1-abc"})
        client = make_execution_client(handler, trace_context=trace)
        client.save({}, doc_id="x", mode="create")
        event = mock_emitter.emit.call_args[0][0]
        sp = event["_spool_path"]
        self.assertEqual(sp["realm"], trace.realm)
        self.assertEqual(sp["plan_id"], trace.plan_id)
        self.assertEqual(sp["step_id"], trace.step_id)

    def test_events_carry_the_attempt_the_step_belongs_to(self):
        mock_emitter = MagicMock()
        trace = self._make_trace(mock_emitter)
        trace.correlation = ExecutionCorrelation(
            execution_id="exec_1", plan_generation="gen", run_token=2
        )
        handler = make_handler_mock(put_result={"rev": "1-abc"})
        client = make_execution_client(handler, trace_context=trace)
        client.save({}, doc_id="x", mode="create")
        event = mock_emitter.emit.call_args[0][0]
        self.assertEqual(
            {
                key: event[key]
                for key in ("execution_id", "plan_generation", "run_token")
            },
            {"execution_id": "exec_1", "plan_generation": "gen", "run_token": 2},
        )

    def test_events_outside_an_attempt_are_uncorrelated(self):
        mock_emitter = MagicMock()
        handler = make_handler_mock(put_result={"rev": "1-abc"})
        client = make_execution_client(
            handler, trace_context=self._make_trace(mock_emitter)
        )
        client.save({}, doc_id="x", mode="create")
        self.assertNotIn("execution_id", mock_emitter.emit.call_args[0][0])

    def test_write_events_have_unique_filenames(self):
        """Two save() calls in the same step produce unique _spool_path filenames."""
        mock_emitter = MagicMock()
        trace = self._make_trace(mock_emitter)
        handler = make_handler_mock(put_result={"rev": "1-abc"})
        client = make_execution_client(handler, trace_context=trace)
        client.save({}, doc_id="doc_a", mode="create")
        client.save({}, doc_id="doc_b", mode="create")
        filenames = [
            c[0][0]["_spool_path"]["filename"] for c in mock_emitter.emit.call_args_list
        ]
        self.assertEqual(len(filenames), 2)
        self.assertNotEqual(filenames[0], filenames[1])
        for fn in filenames:
            self.assertTrue(fn.startswith("data_access_write_"))
            self.assertTrue(fn.endswith(".json"))


# ---------------------------------------------------------------------------
# _emit() spool filename fix — no doc_id / doc_id=None must not raise
# ---------------------------------------------------------------------------


class TestEmitSpoolFilename(unittest.TestCase):
    """Spool filename is UUID-only; absent or None doc_id must not crash."""

    def _make_trace(self, emitter):
        return DataAccessTraceContext(
            realm="r",
            phase="execution",
            plan_id="p1",
            run_id="r1",
            step_id="s1",
            step_name="step",
            emitter=emitter,
        )

    def test_emit_no_doc_id_does_not_raise(self):
        mock_emitter = MagicMock()
        handler = make_handler_mock(put_result={"rev": "1-abc"})
        client = make_execution_client(
            handler, trace_context=self._make_trace(mock_emitter)
        )
        client._emit(
            "data_access.write.succeeded", operation="save", identity="selector"
        )
        mock_emitter.emit.assert_called_once()
        fn = mock_emitter.emit.call_args[0][0]["_spool_path"]["filename"]
        self.assertRegex(fn, r"^data_access_write_[0-9a-f]{32}\.json$")

    def test_emit_doc_id_none_does_not_raise(self):
        mock_emitter = MagicMock()
        handler = make_handler_mock(put_result={"rev": "1-abc"})
        client = make_execution_client(
            handler, trace_context=self._make_trace(mock_emitter)
        )
        client._emit(
            "data_access.write.denied", doc_id=None, operation="save", identity="view"
        )
        mock_emitter.emit.assert_called_once()
        fn = mock_emitter.emit.call_args[0][0]["_spool_path"]["filename"]
        self.assertRegex(fn, r"^data_access_write_[0-9a-f]{32}\.json$")


# ---------------------------------------------------------------------------
# save() — validation
# ---------------------------------------------------------------------------


class TestCouchDBExecutionClientSaveValidation(unittest.TestCase):
    """ValueError is raised before any I/O for bad arguments."""

    def _client(self):
        return make_execution_client(make_handler_mock())

    def test_no_identity_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self._client().save({})
        self.assertIn("exactly one", str(ctx.exception))

    def test_two_identities_doc_id_and_selector(self):
        with self.assertRaises(ValueError):
            self._client().save({}, doc_id="x", selector={"type": "run"})

    def test_all_three_identities(self):
        with self.assertRaises(ValueError):
            self._client().save(
                {},
                doc_id="x",
                selector={"type": "run"},
                view={"design": "d", "view": "v", "key": "k"},
            )

    def test_doc_with_id_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self._client().save({"_id": "x"}, doc_id="y")
        self.assertIn("_id", str(ctx.exception))
        self.assertIn("clean_doc", str(ctx.exception))

    def test_doc_with_rev_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self._client().save({"_rev": "1-a"}, doc_id="y")
        self.assertIn("_rev", str(ctx.exception))

    def test_view_missing_design_key(self):
        with self.assertRaises(ValueError) as ctx:
            self._client().save({}, view={"view": "v", "key": "k"})
        self.assertIn("design", str(ctx.exception))

    def test_view_missing_view_key(self):
        with self.assertRaises(ValueError) as ctx:
            self._client().save({}, view={"design": "d", "key": "k"})
        self.assertIn("view", str(ctx.exception))

    def test_view_missing_key_field(self):
        with self.assertRaises(ValueError) as ctx:
            self._client().save({}, view={"design": "d", "view": "v"})
        self.assertIn("key", str(ctx.exception))

    def test_view_key_none_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self._client().save({}, view={"design": "d", "view": "v", "key": None})
        self.assertIn("None", str(ctx.exception))


# ---------------------------------------------------------------------------
# save() — permissions
# ---------------------------------------------------------------------------


class TestCouchDBExecutionClientSavePermissions(unittest.TestCase):
    def _make_trace(self, emitter):
        return DataAccessTraceContext(
            realm="r",
            phase="execution",
            plan_id="p1",
            run_id="r1",
            step_id="s1",
            step_name="step",
            emitter=emitter,
        )

    def test_denied_without_write_permission(self):
        from yggdrasil.flow.data_access.errors import DataAccessDeniedError

        client = make_execution_client(
            make_handler_mock(), permissions=frozenset({"read"})
        )
        with self.assertRaises(DataAccessDeniedError):
            client.save({}, doc_id="x")

    def test_succeeds_with_write_only_no_read(self):
        handler = make_handler_mock(get_result=None)
        client = make_execution_client(handler, permissions=frozenset({"write"}))
        result = client.save({}, doc_id="x")
        self.assertIsInstance(result, DataAccessWriteResult)

    def test_denied_event_emitted(self):
        mock_emitter = MagicMock()
        from yggdrasil.flow.data_access.errors import DataAccessDeniedError

        client = make_execution_client(
            make_handler_mock(),
            permissions=frozenset({"read"}),
            trace_context=self._make_trace(mock_emitter),
        )
        with self.assertRaises(DataAccessDeniedError):
            client.save({}, doc_id="x")
        mock_emitter.emit.assert_called_once()
        event = mock_emitter.emit.call_args[0][0]
        self.assertEqual(event["type"], "data_access.write.denied")
        self.assertEqual(event["identity"], "doc_id")
        self.assertEqual(event["operation"], "upsert")


# ---------------------------------------------------------------------------
# save(doc_id=...) — doc_id mode
# ---------------------------------------------------------------------------


class TestCouchDBExecutionClientSaveByDocId(unittest.TestCase):
    def test_creates_when_absent(self):
        handler = make_handler_mock(get_result=None, put_result={"rev": "1-new"})
        client = make_execution_client(handler)
        result = client.save({"val": 1}, doc_id="x")
        self.assertEqual(result.status, "created")
        self.assertEqual(result.doc_id, "x")
        self.assertEqual(result.operation, "upsert")
        self.assertEqual(result.identity, "doc_id")

    def test_updates_when_present(self):
        handler = make_handler_mock(
            get_result={"_id": "x", "_rev": "1-old"},
            put_result={"rev": "2-new"},
        )
        client = make_execution_client(handler)
        result = client.save({"val": 2}, doc_id="x")
        self.assertEqual(result.status, "updated")
        self.assertEqual(result.old_rev, "1-old")
        self.assertEqual(result.new_rev, "2-new")


# ---------------------------------------------------------------------------
# save(selector=...) — selector mode
# ---------------------------------------------------------------------------


class TestCouchDBExecutionClientSaveBySelector(unittest.TestCase):
    def test_creates_via_post_when_no_match(self):
        handler = make_handler_mock(find_result=[])
        client = make_execution_client(handler)
        result = client.save({"val": 1}, selector={"type": "run"})
        self.assertEqual(result.status, "created")
        self.assertEqual(result.doc_id, "gen-uuid-123")
        handler.put_document.assert_not_called()
        handler.post_document.assert_called_once()

    def test_updates_single_match(self):
        handler = make_handler_mock(
            find_result=[{"_id": "abc", "_rev": "1-xyz", "val": 0}],
            put_result={"rev": "2-new"},
        )
        client = make_execution_client(handler)
        result = client.save({"val": 1}, selector={"type": "run"})
        self.assertEqual(result.status, "updated")
        self.assertEqual(result.doc_id, "abc")
        handler.post_document.assert_not_called()
        handler.put_document.assert_called_once_with("abc", {"val": 1}, rev="1-xyz")

    def test_raises_on_multiple_matches(self):
        handler = make_handler_mock(
            find_result=[
                {"_id": "a", "_rev": "1-a"},
                {"_id": "b", "_rev": "1-b"},
            ]
        )
        client = make_execution_client(handler)
        with self.assertRaises(DataAccessWriteError) as ctx:
            client.save({"val": 1}, selector={"type": "run"})
        self.assertIn("2", str(ctx.exception))

    def test_uses_limit_2_for_find(self):
        handler = make_handler_mock(find_result=[])
        client = make_execution_client(handler)
        client.save({"val": 1}, selector={"type": "run"})
        handler.find_documents.assert_called_once_with({"type": "run"}, limit=2)

    def test_update_retries_on_409(self):
        handler = make_handler_mock(
            find_result=[{"_id": "abc", "_rev": "1-xyz"}],
        )
        handler.put_document.side_effect = [
            make_api_exception(409),
            {"rev": "3-new", "ok": True},
        ]
        handler.fetch_document_by_id.return_value = {"_id": "abc", "_rev": "2-mid"}
        client = make_execution_client(handler)
        result = client.save({"val": 1}, selector={"type": "run"})
        self.assertEqual(result.status, "updated")
        self.assertEqual(result.new_rev, "3-new")

    def test_query_failure_raises_write_error(self):
        handler = make_handler_mock()
        handler.find_documents.side_effect = make_api_exception(500)
        client = make_execution_client(handler)
        with self.assertRaises(DataAccessWriteError):
            client.save({"val": 1}, selector={"type": "run"})


# ---------------------------------------------------------------------------
# save(view=...) — view mode
# ---------------------------------------------------------------------------


class TestCouchDBExecutionClientSaveByView(unittest.TestCase):
    _view_spec = {"design": "flowcells", "view": "by_id", "key": "fc-1"}

    def test_creates_via_post_when_no_rows(self):
        handler = make_handler_mock(view_result={"rows": []})
        client = make_execution_client(handler)
        result = client.save({"val": 1}, view=self._view_spec)
        self.assertEqual(result.status, "created")
        self.assertEqual(result.doc_id, "gen-uuid-123")
        handler.put_document.assert_not_called()

    def test_updates_with_include_docs(self):
        row = {
            "id": "abc",
            "key": "fc-1",
            "value": None,
            "doc": {"_id": "abc", "_rev": "1-xyz", "val": 0},
        }
        handler = make_handler_mock(
            view_result={"rows": [row]},
            put_result={"rev": "2-new"},
        )
        client = make_execution_client(handler)
        spec = {**self._view_spec, "include_docs": True}
        result = client.save({"val": 1}, view=spec)
        self.assertEqual(result.status, "updated")
        self.assertEqual(result.doc_id, "abc")
        handler.fetch_document_by_id.assert_not_called()

    def test_updates_without_include_docs_fetches_separately(self):
        row = {"id": "abc", "key": "fc-1", "value": None}
        handler = make_handler_mock(
            view_result={"rows": [row]},
            get_result={"_id": "abc", "_rev": "1-xyz"},
            put_result={"rev": "2-new"},
        )
        client = make_execution_client(handler)
        spec = {**self._view_spec, "include_docs": False}
        result = client.save({"val": 1}, view=spec)
        self.assertEqual(result.status, "updated")
        handler.fetch_document_by_id.assert_called_once_with("abc")

    def test_raises_on_multiple_rows(self):
        rows = [
            {"id": "a", "key": "fc-1", "value": None},
            {"id": "b", "key": "fc-1", "value": None},
        ]
        handler = make_handler_mock(view_result={"rows": rows})
        client = make_execution_client(handler)
        with self.assertRaises(DataAccessWriteError) as ctx:
            client.save({"val": 1}, view=self._view_spec)
        self.assertIn("2", str(ctx.exception))

    def test_uses_limit_2_for_query(self):
        handler = make_handler_mock(view_result={"rows": []})
        client = make_execution_client(handler)
        client.save({"val": 1}, view=self._view_spec)
        call_kwargs = handler.query_view.call_args[1]
        self.assertEqual(call_kwargs["limit"], 2)

    def test_maps_design_key_to_ddoc(self):
        handler = make_handler_mock(view_result={"rows": []})
        client = make_execution_client(handler)
        client.save({"val": 1}, view=self._view_spec)
        args = handler.query_view.call_args[0]
        self.assertEqual(args[0], "flowcells")
        self.assertEqual(args[1], "by_id")

    def test_query_failure_raises_write_error(self):
        handler = make_handler_mock()
        handler.query_view.side_effect = make_api_exception(500)
        client = make_execution_client(handler)
        with self.assertRaises(DataAccessWriteError):
            client.save({"val": 1}, view=self._view_spec)


# ---------------------------------------------------------------------------
# clean_doc()
# ---------------------------------------------------------------------------


class TestCouchDBExecutionClientCleanDoc(unittest.TestCase):
    def _client(self):
        return make_execution_client(make_handler_mock())

    def test_strips_id(self):
        self.assertEqual(self._client().clean_doc({"_id": "x", "a": 1}), {"a": 1})

    def test_strips_rev(self):
        self.assertEqual(self._client().clean_doc({"_rev": "1-abc", "a": 1}), {"a": 1})

    def test_strips_both(self):
        self.assertEqual(
            self._client().clean_doc({"_id": "x", "_rev": "1-abc", "a": 1}),
            {"a": 1},
        )

    def test_preserves_other_fields(self):
        doc = {"type": "run", "status": "new"}
        self.assertEqual(self._client().clean_doc(doc), doc)

    def test_empty_doc(self):
        self.assertEqual(self._client().clean_doc({}), {})


# ---------------------------------------------------------------------------
# save() — result shape
# ---------------------------------------------------------------------------


class TestCouchDBExecutionClientSaveResultShape(unittest.TestCase):
    def test_operation_reflects_mode_default_upsert(self):
        handler = make_handler_mock(get_result=None)
        result = make_execution_client(handler).save({"a": 1}, doc_id="x")
        self.assertEqual(result.operation, "upsert")

    def test_doc_id_is_generated_for_selector_create(self):
        handler = make_handler_mock(find_result=[])
        result = make_execution_client(handler).save({"a": 1}, selector={"type": "run"})
        self.assertEqual(result.doc_id, "gen-uuid-123")
        self.assertEqual(result.identity, "selector")

    def test_doc_id_is_provided_for_doc_id_mode(self):
        handler = make_handler_mock(get_result=None)
        result = make_execution_client(handler).save({"a": 1}, doc_id="my-doc")
        self.assertEqual(result.doc_id, "my-doc")
        self.assertEqual(result.identity, "doc_id")


# ---------------------------------------------------------------------------
# save() — event emission
# ---------------------------------------------------------------------------


class TestCouchDBExecutionClientSaveEvents(unittest.TestCase):
    def _make_trace(self, emitter):
        return DataAccessTraceContext(
            realm="r",
            phase="execution",
            plan_id="p1",
            run_id="r1",
            step_id="s1",
            step_name="step",
            emitter=emitter,
        )

    def test_succeeded_has_operation_upsert_by_default(self):
        mock_emitter = MagicMock()
        handler = make_handler_mock(get_result=None)
        client = make_execution_client(
            handler, trace_context=self._make_trace(mock_emitter)
        )
        client.save({"a": 1}, doc_id="x")
        event = mock_emitter.emit.call_args[0][0]
        self.assertEqual(event["operation"], "upsert")

    def test_succeeded_has_identity_doc_id(self):
        mock_emitter = MagicMock()
        handler = make_handler_mock(get_result=None)
        client = make_execution_client(
            handler, trace_context=self._make_trace(mock_emitter)
        )
        client.save({"a": 1}, doc_id="x")
        event = mock_emitter.emit.call_args[0][0]
        self.assertEqual(event["identity"], "doc_id")

    def test_succeeded_has_identity_selector(self):
        mock_emitter = MagicMock()
        handler = make_handler_mock(find_result=[])
        client = make_execution_client(
            handler, trace_context=self._make_trace(mock_emitter)
        )
        client.save({"a": 1}, selector={"type": "run"})
        event = mock_emitter.emit.call_args[0][0]
        self.assertEqual(event["identity"], "selector")

    def test_succeeded_has_identity_view(self):
        mock_emitter = MagicMock()
        handler = make_handler_mock(view_result={"rows": []})
        client = make_execution_client(
            handler, trace_context=self._make_trace(mock_emitter)
        )
        client.save({"a": 1}, view={"design": "d", "view": "v", "key": "k"})
        event = mock_emitter.emit.call_args[0][0]
        self.assertEqual(event["identity"], "view")

    def test_succeeded_selector_includes_selector_and_resolved_doc_id(self):
        mock_emitter = MagicMock()
        handler = make_handler_mock(find_result=[])
        client = make_execution_client(
            handler, trace_context=self._make_trace(mock_emitter)
        )
        sel = {"type": "run"}
        client.save({"a": 1}, selector=sel)
        event = mock_emitter.emit.call_args[0][0]
        self.assertEqual(event["selector"], sel)
        self.assertEqual(event["doc_id"], "gen-uuid-123")

    def test_succeeded_view_includes_view_spec_and_resolved_doc_id(self):
        mock_emitter = MagicMock()
        handler = make_handler_mock(view_result={"rows": []})
        client = make_execution_client(
            handler, trace_context=self._make_trace(mock_emitter)
        )
        view_spec = {"design": "d", "view": "v", "key": "k"}
        client.save({"a": 1}, view=view_spec)
        event = mock_emitter.emit.call_args[0][0]
        self.assertEqual(event["view"], view_spec)
        self.assertEqual(event["doc_id"], "gen-uuid-123")

    def test_failed_selector_event_includes_selector(self):
        mock_emitter = MagicMock()
        handler = make_handler_mock()
        handler.find_documents.side_effect = make_api_exception(500)
        client = make_execution_client(
            handler, trace_context=self._make_trace(mock_emitter)
        )
        sel = {"type": "run"}
        with self.assertRaises(DataAccessWriteError):
            client.save({"a": 1}, selector=sel)
        mock_emitter.emit.assert_called_once()
        event = mock_emitter.emit.call_args[0][0]
        self.assertEqual(event["type"], "data_access.write.failed")
        self.assertEqual(event["selector"], sel)

    def test_failed_view_event_includes_view(self):
        mock_emitter = MagicMock()
        handler = make_handler_mock()
        handler.query_view.side_effect = make_api_exception(500)
        client = make_execution_client(
            handler, trace_context=self._make_trace(mock_emitter)
        )
        view_spec = {"design": "d", "view": "v", "key": "k"}
        with self.assertRaises(DataAccessWriteError):
            client.save({"a": 1}, view=view_spec)
        mock_emitter.emit.assert_called_once()
        event = mock_emitter.emit.call_args[0][0]
        self.assertEqual(event["type"], "data_access.write.failed")
        self.assertEqual(event["view"], view_spec)


# ---------------------------------------------------------------------------
# save(doc_id=...) — mode matrix
# ---------------------------------------------------------------------------


class TestCouchDBExecutionClientSaveByDocIdModes(unittest.TestCase):
    def test_create_mode_succeeds_when_absent(self):
        handler = make_handler_mock(put_result={"rev": "1-new"})
        result = make_execution_client(handler).save(
            {"v": 1}, doc_id="x", mode="create"
        )
        self.assertEqual(result.status, "created")
        self.assertEqual(result.operation, "create")
        self.assertEqual(result.identity, "doc_id")
        # create goes straight to PUT — no pre-fetch of the existing doc
        handler.fetch_document_by_id.assert_not_called()

    def test_create_mode_fails_when_doc_exists(self):
        handler = make_handler_mock()
        handler.put_document.side_effect = make_api_exception(409)
        with self.assertRaises(DataAccessWriteError):
            make_execution_client(handler).save({}, doc_id="x", mode="create")

    def test_update_mode_succeeds_when_present(self):
        handler = make_handler_mock(
            get_result={"_id": "x", "_rev": "1-old"},
            put_result={"rev": "2-new"},
        )
        result = make_execution_client(handler).save(
            {"v": 2}, doc_id="x", mode="update"
        )
        self.assertEqual(result.status, "updated")
        self.assertEqual(result.operation, "update")

    def test_update_mode_fails_when_doc_absent(self):
        handler = make_handler_mock(get_result=None)
        with self.assertRaises(DataAccessWriteError):
            make_execution_client(handler).save({}, doc_id="x", mode="update")


# ---------------------------------------------------------------------------
# save(selector=...) — mode matrix
# ---------------------------------------------------------------------------


class TestCouchDBExecutionClientSaveBySelectorModes(unittest.TestCase):
    _sel = {"type": "run"}

    def test_create_mode_creates_on_no_match(self):
        handler = make_handler_mock(find_result=[])
        result = make_execution_client(handler).save(
            {"v": 1}, selector=self._sel, mode="create"
        )
        self.assertEqual(result.status, "created")
        self.assertEqual(result.operation, "create")

    def test_create_mode_fails_on_single_match(self):
        handler = make_handler_mock(find_result=[{"_id": "a", "_rev": "1-a"}])
        with self.assertRaises(DataAccessWriteError):
            make_execution_client(handler).save({}, selector=self._sel, mode="create")

    def test_update_mode_updates_on_single_match(self):
        handler = make_handler_mock(
            find_result=[{"_id": "a", "_rev": "1-a"}],
            put_result={"rev": "2-new"},
        )
        result = make_execution_client(handler).save(
            {"v": 2}, selector=self._sel, mode="update"
        )
        self.assertEqual(result.status, "updated")
        self.assertEqual(result.operation, "update")

    def test_update_mode_fails_on_no_match(self):
        handler = make_handler_mock(find_result=[])
        with self.assertRaises(DataAccessWriteError):
            make_execution_client(handler).save({}, selector=self._sel, mode="update")

    def test_identity_is_selector_in_result(self):
        handler = make_handler_mock(find_result=[])
        result = make_execution_client(handler).save({"v": 1}, selector=self._sel)
        self.assertEqual(result.identity, "selector")


# ---------------------------------------------------------------------------
# save(view=...) — mode matrix
# ---------------------------------------------------------------------------


class TestCouchDBExecutionClientSaveByViewModes(unittest.TestCase):
    _spec = {"design": "fc", "view": "by_id", "key": "k1"}

    def _row(self, doc_id="abc", rev="1-xyz"):
        return {
            "id": doc_id,
            "key": "k1",
            "value": None,
            "doc": {"_id": doc_id, "_rev": rev},
        }

    def test_create_mode_creates_on_no_rows(self):
        handler = make_handler_mock(view_result={"rows": []})
        result = make_execution_client(handler).save(
            {"v": 1}, view=self._spec, mode="create"
        )
        self.assertEqual(result.status, "created")
        self.assertEqual(result.operation, "create")

    def test_create_mode_fails_on_single_row(self):
        handler = make_handler_mock(view_result={"rows": [self._row()]})
        with self.assertRaises(DataAccessWriteError):
            make_execution_client(handler).save({}, view=self._spec, mode="create")

    def test_update_mode_updates_on_single_row(self):
        handler = make_handler_mock(
            view_result={"rows": [self._row()]},
            put_result={"rev": "2-new"},
        )
        result = make_execution_client(handler).save(
            {"v": 2}, view=self._spec, mode="update"
        )
        self.assertEqual(result.status, "updated")
        self.assertEqual(result.operation, "update")

    def test_update_mode_fails_on_no_rows(self):
        handler = make_handler_mock(view_result={"rows": []})
        with self.assertRaises(DataAccessWriteError):
            make_execution_client(handler).save({}, view=self._spec, mode="update")

    def test_identity_is_view_in_result(self):
        handler = make_handler_mock(view_result={"rows": []})
        result = make_execution_client(handler).save({"v": 1}, view=self._spec)
        self.assertEqual(result.identity, "view")


if __name__ == "__main__":
    unittest.main()
