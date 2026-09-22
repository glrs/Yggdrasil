# Yggdrasil Glossary

---

## Core data model

### Scope
A small dict identifying what a run pertains to: `{"kind": ..., "id": ...}`. Every plan and step is bound to a scope.
```json
{"kind": "project", "id": "proj-123"}
```

### Plan
A frozen graph of `StepSpec`s to execute, identified by `plan_id`, with a `failure_policy`. The steps' dependencies decide the execution order. Created by a handler's `generate_plan_drafts()` and persisted in `yggdrasil_plans`.

### StepSpec
One step instance inside a `Plan`. Declares the function reference (`fn_ref`), static parameters (`params`), its prerequisites (`deps`), optional input paths used for fingerprinting, and optional required outputs (`outputs`) that gate reuse.

### PlanDraft
One element of the list returned by a handler's `generate_plan_drafts()`. Wraps a `Plan` with an `auto_run` flag (`True` = execute immediately; `False` = hold for approval), a list of required approvers (future), and human-readable notes.

### PlanningContext
Passed to `generate_plan_drafts()`. Carries: `realm`, `scope`, `scope_dir`, `emitter`, `source_doc` (the triggering document), `reason`, optional `realm_config`, and a `DataAccess` instance.

### StepContext (`ctx`)
Passed to every `@step` function. Provides: `realm`, `scope`, `plan_id`, `step_id`, `step_name`, `workdir`, `scope_dir`, `emitter`, `fingerprint`, `run_id`, and `data` (DataAccess).

### Artifact
A named output of a step: `key` (semantic label), `path` (location on disk), `digest` (`sha256:<hex>` for files, `dirhash:<hex>` for directories).

### Fingerprint
A deterministic SHA-256 digest of a step's params, declared inputs and declared outputs, used for caching. If it matches the `success.fingerprint` from a previous run, and every declared output still exists, the step is reused instead of run.

### StepResult
Returned by a `@step` function. Contains `artifacts`, `metrics` (scalar key-value map), and optional `extra` data.

---

## Realm system

### Realm
A Python package that extends Yggdrasil with handlers and WatchSpecs. Registered via the `ygg.realm` entry-point group.

### RealmDescriptor
Declares a realm: a unique `realm_id`, a list of handler classes, and WatchSpecs (static list or callable). Returned by the realm's `get_realm_descriptor()` function.

### BaseHandler
Abstract base class for all realm handlers. Subclasses declare `event_type` and `handler_id` class attributes and implement `derive_scope(doc)` and `generate_plan_drafts(payload)`.

### WatchSpec
A frozen, declarative watcher intent: watch a named `connection` (from config) for backend events, apply an optional `filter_expr` (JSON Logic predicate), and — on match — emit a `YggdrasilEvent` of a given `EventType` to subscribed handlers.

### RawWatchEvent
The raw event object produced by a backend (e.g. `CouchDBBackend`). Contains the changed document (`doc`), a `deleted` flag, and backend metadata. Passed to `build_scope()` and `build_payload()` in a `WatchSpec`.

---

## Event routing

### EventType
Enum (`lib.core_utils.event_types`) controlling which handlers receive a given event. Active values:
- `COUCHDB_DOC_CHANGED` — a document was created or updated in a watched CouchDB database
- `COUCHDB_DOC_DELETED` — a document was deleted
- `PLAN_EXECUTION` — internal; used by `PlanWatcher` to trigger Engine runs

### YggdrasilEvent
An enriched event routed from `WatcherManager` to handlers. Carries `event_type`, `scope`, `payload`, and metadata.

### WatcherManager
Manages backend watcher instances. Resolves `WatchSpec.connection` to a concrete endpoint via the `external_systems` config block, evaluates `filter_expr` on each raw event, constructs a `YggdrasilEvent`, and routes it to `YggdrasilCore`.

### YggdrasilCore
Central orchestrator (singleton). Discovers realms via `ygg.realm` entry points, manages handler subscriptions, dispatches `YggdrasilEvent` objects, persists `PlanDraft` outputs to `yggdrasil_plans`, and runs the main async event loop.

### PlanWatcher
Watches `yggdrasil_plans`. When a plan transitions to `status="approved"` with an unexecuted run token, hands it to the execution coordinator, which runs it through the Engine and records the result.

---

## Execution

### Engine (`yggdrasil.core.engine`)
Plan executor. Validates the plan, then runs its steps one at a time in dependency order. For each step it creates a workdir, computes a fingerprint, reuses a still-valid earlier success, or else calls the `@step`-decorated function. It publishes step lifecycle events and the attempt's report. See [Flow API](../flow_api/overview.md#engine-yggdrasilcoreengine).

### Failure policy
A plan's `failure_policy`. `fail_fast` (the default) stops the attempt at the first step failure. `continue_independent` blocks the steps that depend on a failure, runs everything else, and ends the attempt failed.

### Step outcome
How a step ended in one attempt: `succeeded`, `reused` (an earlier success was reused), `failed`, or `blocked` (never invoked, because a prerequisite failed or was blocked). A step without an outcome was never reached.

### Execution attempt
One run of a plan, identified by an `execution_id` (`exec_<UTC timestamp>_<hex>`). Later attempts at a plan get higher IDs. Every event an attempt publishes carries its ID, and it ends with an attempt report.

### Run token
`run_token` on a plan document is its latest execution request. `executed_run_token` is the latest request that was finished, which for a `continue_independent` plan may have failed. Raising `run_token` requests a rerun. See [Plan Execution](plan_execution.md).

### Plan generation
`plan_generation`, an opaque ID naming one planned version of a plan. Approval and run-token changes keep it; regenerating the plan replaces it, so a result is never recorded onto a different version of the plan than the one that ran.

### `@step` decorator
Wraps a plain Python function to standardise the step lifecycle: creates `ctx.workdir`, emits `step.started`, calls the function, emits `step.succeeded` or `step.failed`.

---

## Step events

Structured JSON records emitted to the configured event spool during plan execution. Distinct from trigger events — these record *what happened during execution*.

**Types:**
- `plan.attempt_started` — an attempt was admitted, with its planned steps
- `step.started` — step function entered
- `step.progress` — optional mid-step update
- `step.artifact` — one artifact registered
- `step.succeeded` — step completed
- `step.failed` — step raised an exception, or did not produce a required output
- `step.retry_unimplemented` — follows `step.failed` for a transient error
- `step.skipped` — an earlier success was reused (`reason: "cache_hit"`)
- `step.blocked` — a prerequisite failed or was blocked, so the step will not run
- `plan.attempt_report` — the attempt ended; carries its full report

Each record contains `type`, `ts`, `eid`, `realm`, `scope`, `plan_id`, and the attempt's `execution_id`, `plan_generation` and `run_token`. Events of a step's run also carry `seq`, `step_id`, `step_name` and `fingerprint`.

**Spool layout:**
```
<spool_root>/
  <realm>/<plan_id>/
    <execution_id>_plan_attempt_started.json
    <execution_id>_plan_attempt_report.json
    <step_id>/<execution_id>_step_blocked.json
    <step_id>/<run_id>/
      0001_step_started.json
      ...
      000N_step_succeeded.json
```

### EventEmitter
Protocol: a single `emit(event: dict)` method. Three concrete implementations (in `yggdrasil.flow.events`):
- `FileSpoolEmitter` — writes one JSON file per event to the spool directory (default)
- `TeeEmitter` — fans out to multiple emitters
- `CouchEmitter` — writes events as CouchDB documents

Realm code interacts with the emitter only via `ctx.emitter` (typed as `EventEmitter`). Concrete emitter classes should never be imported by realm code.

### Operational snapshot (`plan_status`)
A per-plan summary the ops consumer builds from the event spool and writes to the operations store. It shows one attempt, the most recently admitted, with every planned step's state and outcome. Plans whose spool predates attempt records get a labelled legacy projection instead. See [Plan Execution](plan_execution.md#operational-snapshots-plan_status).

---

## Data access

### DataAccess
Phase-aware, realm-scoped gateway to external system connections. Accessed via `ctx.data` in both step functions (execution phase — sync reads and writes) and the planning context (planning phase — async reads only). Call `ctx.data.connection(name)` to obtain the appropriate client for the current phase. Authorization is per-realm and per-phase: a realm must be listed under `data_access.realms` for a connection, with the appropriate phase permissions (`"read"`, `"write"`). `"write"` does not imply `"read"` — grant both explicitly for realms that need both. Only connections with a `data_access` block are accessible.
