# PRD: Backend-Neutral Internal Storage with Explicit Dev SQLite

**Status:** Implemented
**Branch:** `feature/internal-storage`

## Summary

Add a narrow internal-storage boundary so Yggdrasil Core uses the same
capabilities regardless of database backend.

- Preserve CouchDB as the existing and production backend.
- Add SQLite only through explicit configuration in effective dev mode or
  tests.
- Preserve the plan, checkpoint, event-spool, and operations-snapshot
  contracts across backends. The explicit run-once recovery and prod/dev
  machine-local isolation changes are documented below.
- Store SQLite records as JSON documents, not a second relational domain
  model.
- Do not redesign execution claims, events, or external data access in this
  branch.

### Amendments over the original draft (review outcomes)

1. **Daemon lock split included** (originally deferred): without it, the dev
   daemon could never run beside production on one machine, which was the
   motivating problem. See "Concurrent prod/dev daemons" below.
2. **SQLite threading model specified**: one connection per operation
   (see "SQLite bundle").
3. **Approval remains external**: Yggdrasil does not add a plan-approval UI,
   CLI command, or supported SQLite administration tool in this branch.
   Operators that require manual approval provide their own integration with
   the selected plan store (see "Plan approval in SQLite mode").
4. **`YggdrasilStateStore` dropped from the bundle**: the coordination
   database's only live internal consumer in the modern daemon path is
   checkpoint persistence, which `CheckpointStore` covers.
   `YggdrasilDocument` operations are used only by dead code
   (`ScenarioDocWatcher`, `BestPracticeAnalysisHandler` — flagged in
   `docs/TECH_DEBT_LEDGER.md` #15 for the deprecation cleanup). A state
   store protocol is added only when a live consumer exists.

## Configuration

### Explicit CouchDB

`internal_storage` assigns one named `external_systems` connection to each
logical database role:

```json
{
  "internal_storage": {
    "backend": "couchdb",
    "couchdb": {
      "connections": {
        "coordination": "yggdrasil_db",
        "plans": "yggdrasil_plans_db",
        "operations": "yggdrasil_ops_db"
      }
    }
  }
}
```

Resolution rules (implemented in `lib/storage/config.py`, reusing
`lib/core_utils/external_systems_resolver.py`):

- Every role resolves through the existing connection resolver; the
  database name comes from `connection.resource.db`, URL and auth env-var
  names from the referenced endpoint.
- Credentials, URLs, and database names are never duplicated under
  `internal_storage`.
- The three connections may share an endpoint but must resolve to CouchDB
  endpoints.
- Realm `data_access` policies do not control Core's internal-storage
  access.

### Explicit SQLite

```json
{
  "internal_storage": {
    "backend": "sqlite",
    "sqlite": { "path": null }
  }
}
```

- Accepted only when effective dev mode is active (tests may pass an
  explicit `dev_mode` override or inject a bundle).
- `null` selects `$YGG_HOME/internal_state/dev/yggdrasil.sqlite3`; an
  explicit path must be absolute.
- Invalid explicit configuration is fatal
  (`InternalStorageConfigurationError`); never fall back to another
  backend.

### Compatibility

- Absence of `internal_storage` preserves the implicit CouchDB
  construction exactly, with a deprecation warning.
- `OPS_DB` is honored only by the implicit legacy path (deprecation
  warning); explicit configuration ignores it.
- An installation's workspace configuration should explicitly select
  CouchDB in `main.json` (adding the plans/ops connections); its complete
  `dev_main.json` should select SQLite while retaining any external
  connections realms require. These workspace config files are
  operator-managed under the gitignored `yggdrasil_workspace/`; the Python
  package does not install them, so a clean deployment without an
  `internal_storage` block falls through to the deprecated implicit CouchDB
  path (with a warning) until the operator adds the block.

## Storage interfaces (`lib/storage/protocols.py`)

```python
@dataclass(frozen=True)
class InternalStorageBundle:
    backend: str          # "couchdb" | "sqlite" — logging/tests only
    plans: PlanStore
    plan_changes: PlanChangeSource
    checkpoints: CheckpointStore   # reused ABC from watchers.backends.base
    ops_snapshots: OpsSnapshotSink
```

- `PlanStore` mirrors the `PlanDBManager` contract exactly (8 methods);
  `PlanDBManager` satisfies it structurally.
- `PlanChangeSource.stream_changes_continuously(*, since, poll_interval_sec)`
  yields `RawWatchEvent` (id = plan ID, doc = current document or `None`
  for deletion, seq = opaque cursor, deleted = tombstone flag). Core and
  PlanWatcher never inspect Couch change-response fields or SQLite rows.
- `OpsSnapshotSink.write(plan_dir, snapshot)` preserves the
  `FileSpoolConsumer` writer interface.
- Plan document shapes are built by one shared module
  (`lib/storage/plan_documents.py`) used by both backends — no drift.

## Backend implementations

### CouchDB bundle (`lib/storage/couch.py`)

Wraps the existing implementations (`PlanDBManager`, `ChangesFetcher`,
`CouchDBCheckpointStore`, `OpsWriter`); the only adapter code is
`CouchPlanChangeSource`, translating change dicts to `RawWatchEvent`.
Database/document IDs, plan JSON and `_rev`, `_changes` semantics,
eligibility/token behavior, checkpoint documents, snapshot upserts, and
the filesystem event spool are unchanged.

### SQLite bundle (`lib/storage/sqlite.py`)

One SQLite file with `store_metadata` and `documents(namespace,
document_id, revision, change_seq, deleted, body_json, updated_at)`;
namespaces `plans`, `checkpoints`, `operations_snapshots`.

- Canonical JSON as `TEXT`; revisions and change cursors are opaque.
- Every plan mutation increments a global plan-change counter and writes
  the new `change_seq` in the same transaction; polls return rows with
  `change_seq > cursor` ordered by sequence. Mutations between polls
  coalesce to the latest state (current eligibility, not an audit feed).
  `since="now"` resolves to the current counter; deletions are tombstones.
- Recovery eligibility is evaluated in Python with the existing
  `is_plan_eligible`.
- **Threading model:** consumers are asyncio tasks whose sync calls run on
  `asyncio.to_thread` worker threads; `sqlite3` connections are
  thread-bound, so the store opens a **fresh connection per operation**
  (WAL + bounded busy timeout) and never caches connections.
- Short transactions, WAL, busy timeout; reliable host-local storage
  required (network filesystems unsupported for WAL).

### SQLite lifecycle

- Missing parents of the database path are created `0700`; the database is
  created `0600`.
- Symlinked, wrong-owner, malformed, or unrelated existing files are
  rejected (`application_id` marks Yggdrasil files); creation of a new
  disposable dev database is logged; a deleted database is recreated
  fresh at next startup.
- `application_id` + `user_version` gate schema compatibility: newer
  schemas fail with upgrade guidance; unsupported older schemas fail with
  reset guidance; future migrations run transactionally and never
  auto-delete.
- No marker files, storage-administration CLI, backups, or retention.

## Core integration

- `YggdrasilCore(..., storage=None)` resolves one `InternalStorageBundle`
  at construction (`build_internal_storage`).
- PlanWatcher receives `plan_store`, `change_source`, and
  `checkpoint_store` (bare construction keeps the legacy CouchDB defaults
  for compatibility); it consumes `RawWatchEvent`.
- WatcherManager receives the bundle's checkpoint store.
- `OpsConsumerService(..., writer=...)` and run-once's final spool drain
  write through `ops_snapshots`.
- Run-once now performs an **eligible-plan recovery pass before starting
  its scoped watcher**. It fetches only the plan IDs created by that
  invocation, so plans saved before watcher startup are observed without
  scanning the whole CouchDB plans database.
- `ProjectDBManager` is external (projects DB), not part of the bundle: it
  is a lazy property constructed only by the run-doc paths; normal daemon
  initialization never constructs it. The dead `self.ydm` construction was
  removed.

## Plan approval in SQLite mode

Yggdrasil does not ship a plan-approval interface. An operator-provided
integration approves a draft by setting `status="approved"` in the selected
plan store and requests a later re-run by incrementing `run_token`. For
SQLite, the integration must advance the plan change sequence transactionally;
editing only the stored JSON does not notify PlanWatcher.

With no PlanWatcher checkpoint, a plan approved before daemon startup may be
missed (Tech Debt #17). Once PlanWatcher is running, an operator integration
can re-emit an observable change for an approved-and-pending plan. It must not
do this while the plan may already be executing because duplicate scheduling
is possible.

## Concurrent prod/dev daemons (pulled in from the deferred list)

The daemon lock is scoped by effective mode: `daemon.lock` (prod) vs
`daemon-dev.lock` (dev), same runtime directory and security checks. Prod
and dev daemons coexist on one machine; duplicates of the same mode are
rejected, and lock errors name the mode. To keep coexistence from
polluting production state, machine-local *defaults* are mode-scoped too
(`lib/core_utils/runtime_paths.py`): dev defaults become
`/tmp/ygg_work_dev` and `/tmp/ygg_events_dev` (explicit config/env values
are never rewritten), and log filenames gain a mode marker + PID
(`yggdrasil[_dev]_<timestamp>_<pid>.log`). Without this, a shared spool
would cross-feed the two ops sinks — dev events would be written into the
production ops database — and shared `success.fingerprint` caches could
cross-skip steps.

The lock still does not coordinate across users, containers, or hosts,
and is not database-aware; shared-database detection remains out of scope
(documentation is the safeguard).

## External database direction

Unchanged from the original draft: external `WatcherBackend`
implementations own acquisition mechanics and emit `RawWatchEvent`; realm
queries stay behind `DataAccess` providers; PostgreSQL and other external
providers need a separate RFC; internal-storage connections do not grant
realm access.

## Known limitations

- Multiple SQLite-mode instances sharing one stage CouchDB each keep
  private internal state (no checkpoint conflicts — the point of this
  change) but will each independently observe and execute the same
  watched scenario documents.
- A dev SQLite daemon still requires its configured external CouchDB
  endpoints to be reachable; SQLite mode isolates internal state, it is
  not an offline mode.
- Run-once scoped watchers share the daemon's checkpoint key
  (pre-existing; `docs/TECH_DEBT_LEDGER.md` #16).
- Daemon startup recovery is not wired (Tech Debt #17). The remaining
  full eligible-plan scan has no active production caller.
- Plan-store writes to SQLite are conditional on the revision they were
  derived from, so a stale write is rejected instead of overwriting (Tech
  Debt #18, resolved). `SQLiteInternalStore.put_document` still upserts
  unconditionally when no `expected_rev` is given, so an external integration
  that rewrites plan documents must pass the revision it read, or it can
  overwrite a concurrent daemon update to the same plan.

## Deferred

- SQLite in normal or production operation
- Couch/SQLite import, synchronization, or automatic switching
- Execution claims, fencing, retries, or status redesign
- Event ledger, outbox, retention, or broker integration
- Plan-management API or supported storage-administration CLI
- General external-database provider architecture
- Removal of deprecated project realms, `run-doc`, and the dead classes
  flagged in the tech-debt ledger
