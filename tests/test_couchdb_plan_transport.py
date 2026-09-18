"""Transport-level tests for the plan store's CouchDB client.

The plan store owns its retry policy: the shared finalizer makes one write
attempt per call, and a caller decides whether to attempt again after
rereading and rechecking the request. The Cloudant SDK can also retry inside a
single call, which would repeat that write invisibly and multiply the caller's
attempt bound, so the plan store's client is built with SDK retries off.

That configuration lives below every mock-based test in the suite: a fake
client object never exercises it. These tests therefore run a real CloudantV1
client against a stub CouchDB over HTTP on localhost, and count the requests
that actually arrive.
"""

from __future__ import annotations

import email.utils
import gzip
import json
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from unittest.mock import Mock, patch

from lib.couchdb.couchdb_connection import CouchDBClientFactory
from lib.couchdb.plan_db_manager import PlanDBManager
from lib.storage.errors import PlanStoreError
from lib.storage.plan_documents import build_plan_document
from lib.storage.plan_updates import FinalizationStatus, SupersessionReason
from tests.plan_store_support import SCOPE, finalization_for, make_plan

DB_NAME = "yggdrasil_plans"
PLAN_ID = "pln_test_P1_v1"
USER_ENV = "YGG_TRANSPORT_TEST_USER"
PASS_ENV = "YGG_TRANSPORT_TEST_PASS"


class _StubCouchDB:
    """A CouchDB stand-in served over real HTTP, recording every request.

    Implements only what the plan store calls: the session handshake, the
    server and database pings, and document reads and writes with CouchDB's
    revision rules.

    Attributes:
        documents: Stored documents by ID.
        requests: (method, path) of every request that arrived, in order.
        put_failures: Number of upcoming PUTs to answer 503 instead of
            applying. ``Retry-After: 0`` keeps a retrying client fast.
        get_body: When set, the body to answer document reads with, for
            responses that are not a document at all.
    """

    def __init__(self) -> None:
        self.documents: dict[str, dict[str, Any]] = {}
        self.requests: list[tuple[str, str]] = []
        self.put_failures = 0
        self.get_body: Any = None
        self._writes: dict[str, int] = {}
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    @property
    def url(self) -> str:
        """Base URL of the stub server."""
        host, port = self._server.server_address[:2]
        if isinstance(host, bytes):
            host = host.decode()
        return f"http://{host}:{port}"

    def stop(self) -> None:
        """Shut the server down and join its thread."""
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=10)

    def count(self, method: str) -> int:
        """Number of recorded requests with this HTTP method."""
        return sum(1 for recorded, _ in self.requests if recorded == method)

    def store(self, document: dict[str, Any]) -> dict[str, Any]:
        """Put a document in place directly, as an existing plan."""
        body = dict(document)
        self._writes[body["_id"]] = self._writes.get(body["_id"], 0) + 1
        body["_rev"] = f"{self._writes[body['_id']]}-stub"
        self.documents[body["_id"]] = body
        return dict(body)

    def _handler(self) -> type[BaseHTTPRequestHandler]:
        stub = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args: Any) -> None:
                """Keep the test output quiet."""

            def _send(self, code: int, body: Any, headers: Any = ()) -> None:
                raw = (
                    body.encode()
                    if isinstance(body, str)
                    else json.dumps(body).encode()
                )
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                for name, value in headers:
                    self.send_header(name, value)
                self.end_headers()
                self.wfile.write(raw)

            def _read_body(self) -> dict[str, Any]:
                """Read the request body, decompressing it like CouchDB does.

                The SDK gzips request bodies above a size threshold, which a
                finalized plan document (it carries the attempt's report)
                comfortably exceeds.
                """
                length = int(self.headers.get("Content-Length", 0))
                raw = self.rfile.read(length) or b"{}"
                if self.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.decompress(raw)
                return json.loads(raw)

            def do_POST(self) -> None:
                stub.requests.append(("POST", self.path))
                self._read_body()
                if self.path.startswith("/_session"):
                    expires = email.utils.formatdate(time.time() + 3600, usegmt=True)
                    return self._send(
                        200,
                        {"ok": True, "name": "tester", "roles": []},
                        [
                            (
                                "Set-Cookie",
                                f"AuthSession=stub; Expires={expires}; Path=/",
                            )
                        ],
                    )
                self._send(404, {"error": "not_found"})

            def do_GET(self) -> None:
                stub.requests.append(("GET", self.path))
                if self.path == "/":
                    return self._send(200, {"couchdb": "Welcome", "version": "3.3.3"})
                if self.path == f"/{DB_NAME}":
                    return self._send(200, {"db_name": DB_NAME, "doc_count": 0})
                doc_id = self.path.rsplit("/", 1)[-1].split("?", 1)[0]
                if stub.get_body is not None:
                    return self._send(200, stub.get_body)
                document = stub.documents.get(doc_id)
                if document is None:
                    return self._send(404, {"error": "not_found"})
                return self._send(200, document)

            def do_PUT(self) -> None:
                stub.requests.append(("PUT", self.path))
                body = self._read_body()
                if stub.put_failures > 0:
                    stub.put_failures -= 1
                    return self._send(
                        503, {"error": "unavailable"}, [("Retry-After", "0")]
                    )
                doc_id = self.path.rsplit("/", 1)[-1].split("?", 1)[0]
                current = stub.documents.get(doc_id)
                expected = current["_rev"] if current else None
                if body.get("_rev") != expected:
                    return self._send(409, {"error": "conflict"})
                stored = stub.store(body)
                return self._send(
                    201, {"ok": True, "id": doc_id, "rev": stored["_rev"]}
                )

        return Handler


class _StubCouchTestCase(unittest.TestCase):
    """Runs a stub CouchDB and points a real plan store at it."""

    def setUp(self):
        self.stub = _StubCouchDB()
        self.addCleanup(self.stub.stop)
        env = patch.dict("os.environ", {USER_ENV: "tester", PASS_ENV: "secret"})
        env.start()
        self.addCleanup(env.stop)
        params = patch(
            "lib.couchdb.plan_db_manager.resolve_couchdb_params",
            return_value=Mock(url=self.stub.url, user_env=USER_ENV, pass_env=PASS_ENV),
        )
        params.start()
        self.addCleanup(params.stop)
        self.plans = PlanDBManager(db_name=DB_NAME)

    def seed_plan(self, **overrides: Any) -> dict[str, Any]:
        """Store an approved plan document and return it."""
        document = build_plan_document(
            make_plan(PLAN_ID), "test_realm", dict(SCOPE), auto_run=True
        )
        document.update(overrides)
        return self.stub.store(document)


class TestPlanStoreRetryOwnership(_StubCouchTestCase):
    """One finalization call makes at most one write attempt."""

    def test_a_transient_write_failure_is_not_retried_by_the_transport(self):
        request = finalization_for(self.seed_plan())
        self.stub.put_failures = 5

        with self.assertRaises(PlanStoreError) as ctx:
            self.plans.finalize_execution(request)

        self.assertEqual(self.stub.count("PUT"), 1)
        self.assertTrue(ctx.exception.retryable)
        self.assertNotIn("last_finalized_execution", self.stub.documents[PLAN_ID])

    def test_the_sdk_would_otherwise_retry_the_same_write(self):
        """The stub really does drive SDK retries; the plan store opts out.

        Without this control, the test above would also pass against a client
        that simply never reached the server.
        """
        client = CouchDBClientFactory.create_client(
            self.stub.url, USER_ENV, PASS_ENV, enable_retries=True
        )
        self.seed_plan()
        self.stub.put_failures = 5
        self.stub.requests.clear()
        backoffs: list[float] = []

        # The retry logic runs for real; only its waiting is skipped.
        with patch("urllib3.util.retry.time.sleep", backoffs.append):
            with self.assertRaises(Exception):
                client.put_document(
                    db=DB_NAME, doc_id=PLAN_ID, document={"_id": PLAN_ID}
                ).get_result()

        self.assertEqual(self.stub.count("PUT"), 4)  # one attempt plus 3 retries
        self.assertTrue(backoffs)

    def test_the_next_caller_attempt_rereads_and_revalidates(self):
        """A retry is the caller's, and it re-decides against current state."""
        document = self.seed_plan()
        request = finalization_for(document)
        self.stub.put_failures = 1
        with self.assertRaises(PlanStoreError):
            self.plans.finalize_execution(request)

        # The plan is regenerated before the caller's next attempt.
        regenerated = dict(document)
        regenerated["plan_generation"] = "generation-after-replan"
        self.stub.store(regenerated)
        reads_before = self.stub.count("GET")
        writes_before = self.stub.count("PUT")

        result = self.plans.finalize_execution(request)

        self.assertEqual(result.status, FinalizationStatus.SUPERSEDED)
        self.assertEqual(result.reason, SupersessionReason.GENERATION_CHANGED)
        self.assertGreater(self.stub.count("GET"), reads_before)
        self.assertEqual(self.stub.count("PUT"), writes_before)

    def test_a_recovered_server_lets_the_next_attempt_commit(self):
        request = finalization_for(self.seed_plan())
        self.stub.put_failures = 1
        with self.assertRaises(PlanStoreError):
            self.plans.finalize_execution(request)

        result = self.plans.finalize_execution(request)

        self.assertEqual(result.status, FinalizationStatus.COMMITTED)
        self.assertEqual(self.stub.count("PUT"), 2)  # one per caller attempt
        stored = self.stub.documents[PLAN_ID]
        self.assertEqual(stored["executed_run_token"], request.run_token)
        self.assertEqual(
            stored["last_finalized_execution"]["execution_id"], request.execution_id
        )


class TestPlanStoreReadsOverRealTransport(_StubCouchTestCase):
    """What a real response body means for a conditional update."""

    def test_a_deleted_plan_reads_as_missing(self):
        request = finalization_for(self.seed_plan())
        self.stub.documents.clear()

        result = self.plans.finalize_execution(request)

        self.assertEqual(result.reason, SupersessionReason.PLAN_MISSING)

    def test_a_non_document_response_is_a_failure_not_a_missing_plan(self):
        request = finalization_for(self.seed_plan())
        self.stub.get_body = ["not", "a", "document"]

        with self.assertRaises(PlanStoreError):
            self.plans.finalize_execution(request)

        self.assertEqual(self.stub.count("PUT"), 0)


class TestClientRetryConfiguration(unittest.TestCase):
    """How the retry setting reaches the SDK client."""

    def setUp(self):
        env = patch.dict("os.environ", {USER_ENV: "tester", PASS_ENV: "secret"})
        env.start()
        self.addCleanup(env.stop)
        self.client = Mock()
        factory = patch(
            "lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1",
            return_value=self.client,
        )
        factory.start()
        self.addCleanup(factory.stop)
        self.client.get_server_information.return_value.get_result.return_value = {
            "version": "3.3.3"
        }

    def test_clients_retry_transient_failures_by_default(self):
        CouchDBClientFactory.create_client("http://couch.invalid", USER_ENV, PASS_ENV)
        self.client.enable_retries.assert_called_once_with(
            max_retries=3, retry_interval=5.0
        )

    def test_retries_can_be_left_to_the_caller(self):
        CouchDBClientFactory.create_client(
            "http://couch.invalid", USER_ENV, PASS_ENV, enable_retries=False
        )
        self.client.enable_retries.assert_not_called()

    def test_the_plan_store_builds_its_client_without_sdk_retries(self):
        with patch(
            "lib.couchdb.plan_db_manager.resolve_couchdb_params",
            return_value=Mock(
                url="http://couch.invalid", user_env=USER_ENV, pass_env=PASS_ENV
            ),
        ):
            PlanDBManager(db_name=DB_NAME)
        self.client.enable_retries.assert_not_called()


if __name__ == "__main__":
    unittest.main()
