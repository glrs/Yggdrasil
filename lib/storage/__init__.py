"""Backend-neutral internal storage for Yggdrasil Core.

This package defines a narrow boundary between Core and the databases that
hold Yggdrasil's *internal* state: plan documents, watcher checkpoints, and
operations snapshots. Core consumes the :class:`InternalStorageBundle`
protocols only and never constructs backend clients directly.

Backends:
    - CouchDB (production; wraps the existing ``lib/couchdb`` managers)
    - SQLite (explicit configuration, effective dev mode or tests only)

External data sources (the ``projects`` database, realm watch sources,
realm ``DataAccess`` providers) are *not* part of internal storage and keep
their existing resolution paths.
"""

from lib.storage.config import (
    CouchInternalStorageConfig,
    SQLiteInternalStorageConfig,
    default_sqlite_path,
    resolve_internal_storage_config,
)
from lib.storage.errors import PlanStoreError, RevisionConflictError
from lib.storage.factory import build_internal_storage
from lib.storage.plan_updates import (
    ExecutionFinalization,
    FinalizationResult,
    FinalizationStatus,
    SupersessionReason,
)
from lib.storage.protocols import (
    InternalStorageBundle,
    OpsSnapshotSink,
    PlanChangeSource,
    PlanStore,
)

__all__ = [
    "CouchInternalStorageConfig",
    "ExecutionFinalization",
    "FinalizationResult",
    "FinalizationStatus",
    "InternalStorageBundle",
    "OpsSnapshotSink",
    "PlanChangeSource",
    "PlanStore",
    "PlanStoreError",
    "RevisionConflictError",
    "SQLiteInternalStorageConfig",
    "SupersessionReason",
    "build_internal_storage",
    "default_sqlite_path",
    "resolve_internal_storage_config",
]
