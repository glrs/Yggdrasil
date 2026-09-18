"""Protocols and the composition-root bundle for internal storage.

These protocols capture exactly the capabilities Core already uses today.
The CouchDB implementations (``PlanDBManager``, ``OpsWriter``,
``CouchDBCheckpointStore``) satisfy them structurally; the SQLite backend
implements them against a single local database file.

The bundle intentionally has no coordination-document ("state") store: the
only live internal consumer of the ``yggdrasil`` coordination database in the
modern daemon path is checkpoint persistence, which is covered by
``CheckpointStore``. The legacy ``YggdrasilDocument`` operations are used only
by unwired legacy code (see ``docs/TECH_DEBT_LEDGER.md``) and are excluded
until a live consumer exists.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from lib.storage.plan_updates import ExecutionFinalization, FinalizationResult
from lib.watchers.backends.base import CheckpointStore, RawWatchEvent
from yggdrasil.flow.model import Plan


@runtime_checkable
class PlanStore(Protocol):
    """Storage for plan documents (intent + approval state).

    Mirrors the ``PlanDBManager`` contract exactly. Plan eligibility,
    regeneration, run tokens, plan generations, execution authority,
    ownership, document shapes, and conflict behavior are identical across
    backends.

    Every write that replaces a plan document's content is conditioned on
    the state it was derived from, so a concurrent writer is never silently
    overwritten, and a lost race always surfaces as the backend-neutral
    :class:`~lib.storage.errors.RevisionConflictError`.

    Backend *failures* are normalized only by the two conditional-update
    methods, :meth:`ensure_plan_generation` and :meth:`finalize_execution`:
    they raise :class:`~lib.storage.errors.PlanStoreError`, which also says
    whether another attempt could help. The older methods still let their
    backend's own exceptions through — a CouchDB ``ApiException``, a
    ``sqlite3.Error`` — so a caller that must behave identically on both
    backends either uses these two methods or catches broadly.
    """

    def save_plan(
        self,
        plan: Plan,
        realm: str,
        scope: dict[str, Any],
        *,
        auto_run: bool = False,
        execution_authority: str = "daemon",
        execution_owner: str | None = None,
        preview: dict[str, Any] | None = None,
        source_doc_id: str | None = None,
        source_doc_rev: str | None = None,
        notes: str | None = None,
    ) -> str:
        """Persist a new or regenerated plan document; returns the document ID.

        Every call assigns a fresh ``plan_generation`` and resets execution
        tokens. The write applies only to the document state that was read,
        so a lost race raises instead of replaying this call's planning intent
        over a newer document.

        Raises:
            ValueError: If execution_authority is invalid or plan.plan_id is
                missing.
            RevisionConflictError: If another writer created or changed the
                plan document first. Nothing was written.
        """
        ...

    def fetch_plan(self, doc_id: str) -> dict[str, Any] | None:
        """Fetch a plan document by ID, or None if not found."""
        ...

    def fetch_plan_as_model(self, doc_id: str) -> Plan | None:
        """Fetch a plan document and deserialize its Plan model."""
        ...

    def update_executed_token(
        self,
        doc_id: str,
        run_token: int,
        *,
        max_retries: int = 3,
    ) -> bool:
        """Record a successful execution of ``run_token`` for the plan.

        Retries lost races by rereading and reapplying the token, up to
        ``max_retries`` attempts in total. Unlike :meth:`finalize_execution`,
        it does not check that the plan is still the generation that was
        executed, and records no outcome.
        """
        ...

    def ensure_plan_generation(self, doc_id: str) -> dict[str, Any] | None:
        """Return the current plan document, giving a legacy one a generation.

        A document that predates ``plan_generation`` gets a fresh generation
        through a conditional write that changes nothing else. The returned
        document is one consistent snapshot to capture an execution request
        from.

        Returns:
            The current document with its ``plan_generation``, or None if the
            plan does not exist.

        Raises:
            RevisionConflictError: If the document kept changing without
                gaining a generation.
            PlanStoreError: If the storage backend fails.
        """
        ...

    def finalize_execution(self, request: ExecutionFinalization) -> FinalizationResult:
        """Record a finished execution request's outcome and executed token.

        Both are written together in one conditional write, and only if the
        plan is still the captured generation, under the captured authority
        and owner, and has not recorded this request or a newer one. Each
        call makes one attempt and resolves to one status: COMMITTED,
        ALREADY_COMMITTED (this execution's completion was already recorded),
        CONFLICT (still valid, retry), or SUPERSEDED (never retry).

        Raises:
            PlanStoreError: If the storage backend fails. The write may have
                landed; calling again reports that as ALREADY_COMMITTED. Its
                ``retryable`` flag says whether another attempt could help.
        """
        ...

    def query_approved_pending(self) -> list[dict[str, Any]]:
        """Return all plans eligible for a full recovery scan."""
        ...

    def delete_plan(self, doc_id: str) -> bool:
        """Delete a plan document (testing/cleanup)."""
        ...

    def plan_exists(self, doc_id: str) -> bool:
        """Return True if the plan document exists."""
        ...

    def get_plan_summary(self, doc_id: str) -> dict[str, Any] | None:
        """Return a minimal display summary of the plan, or None."""
        ...


class PlanChangeSource(Protocol):
    """Stream of plan-document changes, backend-agnostic.

    Yields :class:`RawWatchEvent` objects where ``id`` is the plan ID,
    ``doc`` is the current plan document (None for deletions), ``seq`` is an
    opaque backend cursor, and ``deleted`` marks tombstones. Consumers never
    inspect CouchDB change-response fields or SQLite rows directly.

    Multiple mutations between polls may coalesce to the latest state: this
    source provides current plan eligibility, not an audit feed.
    """

    def stream_changes_continuously(
        self,
        *,
        since: str | int,
        poll_interval_sec: float,
    ) -> AsyncIterator[RawWatchEvent]:
        """Stream plan changes after ``since`` indefinitely.

        Args:
            since: Opaque cursor from a previous event's ``seq``, or the
                string ``"now"`` to start at the current head.
            poll_interval_sec: Seconds to sleep between polls when idle.
        """
        ...


class OpsSnapshotSink(Protocol):
    """Sink for latest-state plan_status snapshots built from the event spool.

    Matches the writer interface consumed by ``FileSpoolConsumer``.
    """

    def write(self, plan_dir: Path, snapshot: dict[str, Any]) -> None:
        """Upsert the latest snapshot for one plan."""
        ...


@dataclass(frozen=True)
class InternalStorageBundle:
    """All internal-storage capabilities, resolved once at composition root.

    Attributes:
        backend: Backend identifier ("couchdb" or "sqlite"), for logging and
            tests only — consumers must not branch on it.
        plans: Plan document store.
        plan_changes: Plan change stream for PlanWatcher.
        checkpoints: Watcher checkpoint persistence.
        ops_snapshots: Operations snapshot sink for the spool consumer.
    """

    backend: str
    plans: PlanStore
    plan_changes: PlanChangeSource
    checkpoints: CheckpointStore
    ops_snapshots: OpsSnapshotSink
