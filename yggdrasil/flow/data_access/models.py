"""Data models for DataAccess trace context and write results."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from yggdrasil.flow.events.correlation import ExecutionCorrelation
    from yggdrasil.flow.events.emitter import EventEmitter


@dataclass
class DataAccessTraceContext:
    """Trace metadata injected by core into execution-phase DataAccess.

    Attributes:
        realm: Realm identifier (e.g. "demux").
        phase: Always "execution" when trace context is present.
        plan_id: Plan ID for the current run.
        run_id: Run ID for the current engine execution.
        step_id: Step ID being executed.
        step_name: Human-readable step name.
        scope: Step scope dict.
        emitter: Event emitter for write trace events. None if not configured.
        correlation: The execution attempt the step belongs to, stamped onto
            write trace events. None outside an execution attempt.
    """

    realm: str
    phase: Literal["planning", "execution"]
    plan_id: str | None = None
    run_id: str | None = None
    step_id: str | None = None
    step_name: str | None = None
    scope: dict[str, Any] = field(default_factory=dict)
    emitter: EventEmitter | None = None
    correlation: ExecutionCorrelation | None = None


@dataclass(frozen=True)
class DataAccessWriteResult:
    """Result of a successful DataAccess write operation.

    Attributes:
        backend: Backend type (e.g. "couchdb").
        connection_name: Connection name from config.
        resource: Backend resource identifier (db name for CouchDB).
        operation: Requested write mode: "create", "update", or "upsert".
        identity: Identity resolution method used: "doc_id", "selector", or "view".
        doc_id: Document ID that was written (existing, provided, or CouchDB-generated).
        status: "created" if the document was new; "updated" if it existed.
        old_rev: Previous CouchDB revision, or None if document was created.
        new_rev: New CouchDB revision after the write, or None if unavailable.
    """

    backend: str
    connection_name: str
    resource: str
    operation: Literal["create", "update", "upsert"]
    identity: Literal["doc_id", "selector", "view"]
    doc_id: str
    status: Literal["created", "updated"]
    old_rev: str | None
    new_rev: str | None
