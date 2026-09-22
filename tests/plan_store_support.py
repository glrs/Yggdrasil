"""Shared builders for plan-store tests.

Provides real plans, finished execution attempts for every way an attempt can
end, and an in-memory stand-in for the Cloudant client with builders that bind
the CouchDB plan store and ops snapshot sink to it, so the same storage
scenarios can run against both backends.
"""

from __future__ import annotations

import copy
import os
import unittest
from collections.abc import Callable
from typing import Any
from unittest.mock import Mock, patch

from lib.couchdb.plan_db_manager import PlanDBManager
from lib.ops.sinks.couch import OpsWriter
from lib.storage.plan_updates import ExecutionFinalization
from yggdrasil.flow.attempt import AttemptContext
from yggdrasil.flow.model import (
    CONTINUE_INDEPENDENT_POLICY,
    FAIL_FAST_POLICY,
    Plan,
    StepSpec,
)
from yggdrasil.flow.outcomes import (
    AttemptDiagnostic,
    StepFailure,
    StepOutcome,
    TerminationReason,
)

SCOPE = {"kind": "project", "id": "P1"}

# Ways an attempt can end, as accepted by finished_attempt().
SUCCEEDED = "succeeded"
DRAINED_FAILURE = "drained_failure"
PREFLIGHT_REJECTED = "preflight_rejected"
FAILED_FAST = "failed_fast"
CANCELLED = "cancelled"
ORCHESTRATION_ERROR = "orchestration_error"


def make_plan(
    plan_id: str = "pln_test_P1_v1",
    *,
    failure_policy: str = FAIL_FAST_POLICY,
    message: str = "hi",
) -> Plan:
    """Build a two-step plan in which s2 depends on s1.

    Args:
        plan_id: The plan ID.
        failure_policy: The plan's failure policy.
        message: Parameter value, to tell two versions of a plan apart.

    Returns:
        Plan: The plan.
    """
    return Plan(
        plan_id=plan_id,
        realm="test_realm",
        scope=dict(SCOPE),
        steps=[
            StepSpec(
                step_id="s1",
                name="echo",
                fn_ref="tests.integration.mock_steps:echo_step",
                params={"message": message},
            ),
            StepSpec(
                step_id="s2",
                name="echo",
                fn_ref="tests.integration.mock_steps:echo_step",
                params={"message": message},
                deps=["s1"],
            ),
        ],
        failure_policy=failure_policy,
    )


def finished_attempt(
    plan: Plan,
    *,
    plan_generation: str | None,
    run_token: int | None,
    ending: str = SUCCEEDED,
    execution_id: str = "exec-1",
    execution_authority: str = "daemon",
    execution_owner: str | None = None,
) -> AttemptContext:
    """Build the context of an attempt at plan that has ended.

    Args:
        plan: The plan the attempt ran (its policy is the attempt's policy).
        plan_generation: Captured generation.
        run_token: Captured run token.
        ending: One of the ending constants in this module.
        execution_id: The attempt's execution ID.
        execution_authority: Captured authority.
        execution_owner: Captured owner.

    Returns:
        AttemptContext: A context whose report is closed with that ending.

    Raises:
        ValueError: If ending is unknown.
    """
    context = AttemptContext.for_plan(
        plan,
        execution_id=execution_id,
        plan_generation=plan_generation,
        run_token=run_token,
        execution_authority=execution_authority,
        execution_owner=execution_owner,
    )
    report = context.report
    if ending == SUCCEEDED:
        report.record_outcome("s1", StepOutcome.SUCCEEDED)
        report.record_outcome("s2", StepOutcome.SUCCEEDED)
        report.finish(TerminationReason.COMPLETED)
    elif ending == DRAINED_FAILURE:
        report.record_failure(StepFailure(step_id="s1", error="boom"))
        report.record_blocked("s2", ["s1"])
        report.record_failed_ancestors("s2", ["s1"])
        report.finish(TerminationReason.COMPLETED)
    elif ending == PREFLIGHT_REJECTED:
        report.record_diagnostic(
            AttemptDiagnostic(message="dependency cycle: s1 -> s2 -> s1")
        )
        report.finish(TerminationReason.PREFLIGHT_REJECTED)
    elif ending == FAILED_FAST:
        report.record_failure(StepFailure(step_id="s1", error="boom"))
        report.finish(TerminationReason.FAILED_FAST)
    elif ending == CANCELLED:
        report.record_outcome("s1", StepOutcome.SUCCEEDED)
        report.finish(TerminationReason.CANCELLED)
    elif ending == ORCHESTRATION_ERROR:
        report.record_diagnostic(AttemptDiagnostic(message="spool unwritable"))
        report.finish(TerminationReason.ORCHESTRATION_ERROR)
    else:
        raise ValueError(f"unknown ending {ending!r}")
    return context


def finalization_for(
    doc: dict[str, Any],
    *,
    ending: str = SUCCEEDED,
    execution_id: str = "exec-1",
) -> ExecutionFinalization:
    """Build the finalization of an attempt admitted from doc.

    The generation, token, authority, owner and plan are captured from doc,
    the way a caller captures an execution request from one plan snapshot.

    Args:
        doc: The plan document the request was admitted from.
        ending: How the attempt ended.
        execution_id: The attempt's execution ID.

    Returns:
        ExecutionFinalization: The request to record.
    """
    plan = Plan.from_dict(doc["plan"])
    context = finished_attempt(
        plan,
        plan_generation=doc["plan_generation"],
        run_token=doc["run_token"],
        ending=ending,
        execution_id=execution_id,
        execution_authority=doc["execution_authority"],
        execution_owner=doc["execution_owner"],
    )
    return ExecutionFinalization.from_attempt(context)


def continuation_plan(plan_id: str = "pln_test_P1_v1", **kwargs: Any) -> Plan:
    """Build make_plan() with the continue_independent policy."""
    return make_plan(plan_id, failure_policy=CONTINUE_INDEPENDENT_POLICY, **kwargs)


class FakeApiException(Exception):
    """Stands in for the SDK's ApiException in the CouchDB layer under test.

    Some test modules replace ``ibm_cloud_sdk_core`` in ``sys.modules`` when
    they are imported, so which class ``lib.couchdb`` bound as
    ``ApiException`` depends on import order. :func:`patch_api_exception`
    binds this class instead, and :class:`FakeCouchServer` raises it.

    Attributes:
        status_code: HTTP status of the failed request.
        code: Same as status_code, for callers that still read the
            deprecated attribute.
        message: Error message.
    """

    def __init__(self, status_code: int, message: str = "") -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = status_code
        self.message = message


# Modules whose ``except ApiException`` clauses the fake server must satisfy.
_API_EXCEPTION_BINDINGS = (
    "lib.couchdb.couchdb_connection.ApiException",
    "lib.couchdb.plan_db_manager.ApiException",
    "lib.ops.sinks.couch.ApiException",
)


def patch_api_exception(test: unittest.TestCase) -> None:
    """Bind FakeApiException in the CouchDB layer for the rest of test.

    Args:
        test: The running test case; the patches are undone at its cleanup.
    """
    for target in _API_EXCEPTION_BINDINGS:
        patcher = patch(target, FakeApiException)
        patcher.start()
        test.addCleanup(patcher.stop)


class _Result:
    """Mimics the SDK's DetailedResponse: only get_result() is used."""

    def __init__(self, result: dict[str, Any]) -> None:
        self._result = result

    def get_result(self) -> dict[str, Any]:
        return self._result


class FakeCouchServer:
    """In-memory stand-in for the Cloudant client's document calls.

    Enforces CouchDB's revision rules: a write to an existing document must
    carry its current ``_rev``, a create must carry none, and anything else
    raises ``FakeApiException(409)``. Use it together with
    :func:`patch_api_exception`. Revisions are ``"<n>-fake"`` and never
    reused for a document ID, including after deletion.

    Attributes:
        after_read: One-shot actions run after the next document read and
            before its result is returned, to interleave a concurrent writer.
        fail_after_next_write: If set, the next successful write is applied
            and then this exception is raised, like a lost response.
        fail_next_read: If set, the next read raises this exception.
    """

    def __init__(self) -> None:
        self._docs: dict[str, dict[str, Any]] = {}
        self._writes: dict[str, int] = {}
        self.after_read: list[Callable[[], None]] = []
        self.fail_after_next_write: Exception | None = None
        self.fail_next_read: Exception | None = None

    def get_database_information(self, *, db: str) -> _Result:
        return _Result({"db_name": db})

    def get_document(self, *, db: str, doc_id: str) -> _Result:
        if self.fail_next_read is not None:
            exc, self.fail_next_read = self.fail_next_read, None
            raise exc
        doc = copy.deepcopy(self._docs.get(doc_id))
        actions, self.after_read = self.after_read, []
        for action in actions:
            action()
        if doc is None:
            raise FakeApiException(404, "missing")
        return _Result(doc)

    def put_document(self, *, db: str, doc_id: str, document: Any) -> _Result:
        # The SDK accepts its own Document model as well as a plain mapping.
        mapping = document.to_dict() if hasattr(document, "to_dict") else document
        body = copy.deepcopy(dict(mapping))
        rev = body.pop("_rev", None)
        current = self._docs.get(doc_id)
        if rev != (current["_rev"] if current else None):
            raise FakeApiException(409, "Document update conflict.")
        self._writes[doc_id] = self._writes.get(doc_id, 0) + 1
        new_rev = f"{self._writes[doc_id]}-fake"
        body["_id"] = doc_id
        body["_rev"] = new_rev
        self._docs[doc_id] = body
        if self.fail_after_next_write is not None:
            exc, self.fail_after_next_write = self.fail_after_next_write, None
            raise exc
        return _Result({"ok": True, "id": doc_id, "rev": new_rev})

    def delete_document(self, *, db: str, doc_id: str, rev: str) -> _Result:
        current = self._docs.get(doc_id)
        if current is None or current["_rev"] != rev:
            raise FakeApiException(409, "Document update conflict.")
        del self._docs[doc_id]
        self._writes[doc_id] += 1
        return _Result(
            {"ok": True, "id": doc_id, "rev": f"{self._writes[doc_id]}-fake"}
        )

    def raw(self, doc_id: str) -> dict[str, Any] | None:
        """Return the stored document without triggering read hooks."""
        return copy.deepcopy(self._docs.get(doc_id))


def plan_db_manager_on(server: FakeCouchServer) -> PlanDBManager:
    """Construct a PlanDBManager whose Cloudant client is server.

    Args:
        server: The fake client to bind.

    Returns:
        PlanDBManager: A manager that talks only to server.
    """
    params = Mock(url="http://couch.invalid:5984", user_env="FAKE_U", pass_env="FAKE_P")
    with (
        patch(
            "lib.couchdb.plan_db_manager.resolve_couchdb_params", return_value=params
        ),
        patch(
            "lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client",
            return_value=server,
        ),
        patch.dict(os.environ, {"FAKE_U": "user", "FAKE_P": "pass"}),
    ):
        return PlanDBManager()


def ops_writer_on(server: FakeCouchServer) -> OpsWriter:
    """Construct the CouchDB ops snapshot sink with server as its client.

    Use a server of its own: FakeCouchServer does not keep databases apart.

    Args:
        server: The fake client to bind.

    Returns:
        OpsWriter: A snapshot sink that talks only to server.
    """
    with (
        patch(
            "lib.couchdb.couchdb_connection.CouchDBClientFactory.create_client",
            return_value=server,
        ),
        patch.dict(os.environ, {"FAKE_U": "user", "FAKE_P": "pass"}),
    ):
        return OpsWriter(
            db_name="yggdrasil_ops",
            url="http://couch.invalid:5984",
            user_env="FAKE_U",
            pass_env="FAKE_P",
        )
