# Flow API Overview

`yggdrasil.flow` provides the `@step` decorator, the handler planner contract, and event emitters used by all realm handlers.

---

## Core dataclasses (`yggdrasil.flow.model`)

### `Plan`

The concrete, frozen workflow to execute. Created by a handler's planner and persisted in `yggdrasil_plans`.

| Field | Type | Description |
|-------|------|-------------|
| `plan_id` | `str` | Unique identifier (e.g. `"my_realm:object-123"`) |
| `realm` | `str` | Realm that owns this plan |
| `scope` | `dict` | Scope dict (`{"kind": ..., "id": ...}`) |
| `steps` | `list[StepSpec]` | The plan's steps. Dependencies decide the execution order; list order only breaks ties between steps that are ready at the same time |
| `failure_policy` | `str` | What a step failure does to the rest of the plan: `"fail_fast"` (default) or `"continue_independent"`. See [Failure policies](#failure-policies). Any other value is rejected |

### `StepSpec`

Defines one step inside a plan.

| Field | Type | Description |
|-------|------|-------------|
| `step_id` | `str` | Unique within the plan |
| `name` | `str` | Human-readable label |
| `fn_ref` | `str` | Dotted import path to the `@step` function |
| `params` | `dict` | Static parameters passed to the step |
| `deps` | `list[str]` | Prerequisites: `step_id`s that must all have succeeded, or been reused, in the same attempt before this step runs. They may appear anywhere in the plan's step list |
| `inputs` | `dict` | Artifact paths tracked for fingerprinting (optional) |
| `outputs` | `dict[str, str]` | Required output paths, keyed by artifact key: absolute, or relative to the step's work directory. An earlier success is reused only while all of them exist. See [Declared outputs and reuse](#declared-outputs-and-reuse) (optional) |
| `scope` | `dict \| None` | Override scope for this step (optional) |

### `StepContext`

Passed to every `@step` function at execution time.

| Field | Type | Description |
|-------|------|-------------|
| `realm` | `str` | Realm ID |
| `scope` | `dict` | Scope dict for this step |
| `plan_id` | `str` | Owning plan ID |
| `step_id` | `str` | This step's ID |
| `step_name` | `str` | Human-readable step name |
| `workdir` | `Path` | The step's work directory, `<work_root>/<plan_id>/<step_id>`. Every attempt at the plan uses the same one |
| `scope_dir` | `Path` | Shared scope directory for artifacts across all plan steps |
| `emitter` | `BaseEmitter` | Event emitter |
| `run_mode` | `str` | `"auto"` or `"manual"` |
| `fingerprint` | `str` | SHA-256 fingerprint for this run |
| `run_id` | `str` | ID of this invocation of the step |
| `data` | `DataAccess` | Phase-aware read/write gateway to configured data sources. Call `connection(name)` to obtain a sync client (execution phase: reads + writes; planning phase: async reads only). |

### `PlanningContext`

Passed to `generate_plan_drafts()`. Contains everything a handler needs to build a plan.

| Field | Type | Description |
|-------|------|-------------|
| `realm` | `str` | Realm ID |
| `scope` | `dict` | Scope dict for the triggering event |
| `scope_dir` | `Path` | Workspace directory for this scope |
| `emitter` | `BaseEmitter` | Event emitter to use for the plan |
| `source_doc` | `dict` | The raw document that triggered the event |
| `reason` | `str` | Human-readable trigger reason |
| `realm_config` | `dict \| None` | Optional realm-specific config slice |
| `data` | `DataAccess` | Planning-phase DataAccess gateway. Call `connection(name)` to obtain an async read-only client (`await client.get(...)`, `await client.find(...)`). |

### `PlanDraft`

Output of a handler's plan generation.

| Field | Type | Description |
|-------|------|-------------|
| `plan` | `Plan` | The generated plan |
| `auto_run` | `bool` | If `True`, plan is approved immediately; if `False`, status is `"draft"` |
| `approvals_required` | `list[str]` | Labels of required approvers (informational) |
| `notes` | `str` | Human-readable description of the plan |
| `preview` | `dict \| None` | Metadata for display before execution |

### `Artifact`

A named output of a step.

| Field | Type | Description |
|-------|------|-------------|
| `key` | `str` | Semantic label (e.g. `"output_dir"`, `"report_file"`) |
| `path` | `str \| Path` | Location on disk |
| `digest` | `str` | `sha256:<hex>` for files, `dirhash:<hex>` for directories |

---

## `@step` decorator (`yggdrasil.flow.step`)

Wraps a plain function to standardize step lifecycle:

1. Creates `ctx.workdir`
2. Emits `step.started`
3. Calls the function with `ctx` as first argument
4. Checks that every output the step declares exists. A missing one fails the step (`PermanentStepError` with code `missing_required_outputs`), so no success is ever reported for it
5. Emits `step.succeeded` (with artifacts + metrics) or `step.failed`

```python
from yggdrasil.flow.step import step
from yggdrasil.flow.model import StepContext, StepResult

@step
def my_step(ctx: StepContext, message: str) -> StepResult:
    print(f"[{ctx.scope['id']}] {message}")
    return StepResult(metrics={"message_len": len(message)})
```

**Typed inputs and outputs** via `Annotated` annotations:

```python
from typing import Annotated
from pathlib import Path
from yggdrasil.flow.artifacts import In, Out

@step
def run_processor(
    ctx: StepContext,
    input_dir: Annotated[Path, In("input_dir")],
    output_file: Annotated[Path, Out("output_file")],
    config: str,
) -> StepResult:
    ...
```

`In(key)` declarations tell the Engine which paths to include in the fingerprint computation. `Out(key)` declarations name the artifacts the step produces; `PlanBuilder` records them, as absolute paths, in the step's `outputs`.

---

## Handler planner contract

A planner is a `BaseHandler` subclass — not just any object implementing `generate_plan_drafts()`. The full contract `YggdrasilCore` enforces at registration time:

| Requirement | Kind | Description |
|-------------|------|-------------|
| `event_type` | class variable | `EventType` the handler subscribes to |
| `handler_id` | class variable | Unique string identifier for this handler |
| `derive_scope(doc)` | method | Extracts `{"kind": ..., "id": ...}` from the triggering document |
| `async generate_plan_drafts(payload)` | method | Returns a `list[PlanDraft]` |

`realm_id` is set on the handler instance by `YggdrasilCore` during realm registration — handlers must not set it themselves.

`BaseHandler` provides a default `build_planning_context()` method that constructs a `PlanningContext` from the scope dict and event payload. Override it only if you need non-standard context setup.

---

## Engine (`yggdrasil.core.engine`)

`Engine.run(plan)` runs one **attempt** at a plan. Steps run one at a time, in dependency order:

1. **Preflight**, before anything is written to the work root. The plan is rejected if a step ID is empty or duplicated, a dependency names an unknown step or the step itself, the dependencies form a cycle, `failure_policy` is unknown, `outputs` are malformed, or a step's `fn_ref` cannot be resolved, is not `@step`-decorated, or cannot bind its `params`. A rejected plan raises `PreflightValidationError` (a `ValueError`), and no step runs.
2. Creates `<work_root>/<plan_id>/` and writes `plan.json`.
3. Runs every step whose prerequisites are satisfied. A step is **ready** once every step in its `deps` has succeeded or been reused in this attempt. When several steps are ready, the one listed first in `plan.steps` runs first. For each step it:
    - creates its work directory, `<plan_dir>/<step_id>/`
    - computes its fingerprint
    - reuses an earlier success when one is still valid, emitting `step.skipped` (see [Declared outputs and reuse](#declared-outputs-and-reuse))
    - otherwise removes the step's old success marker, then calls the function with its `StepContext` and coerced params. On success it writes a new `success.fingerprint`
4. Hands a step failure to the plan's [failure policy](#failure-policies).

!!! note "`deps` did not always order execution"
    Before dependency scheduling existed, the engine ran steps in the order of `plan.steps` and used `deps` only to check that the named steps existed. A plan that listed a step before its prerequisite ran it first anyway. Now `deps` alone decides the order. A dependency may be listed after the step that needs it, and a plan whose list was already in dependency order runs in the same order as before.

The engine's workspace and event spool are configured at daemon startup by `YggdrasilCore`. See [Configuration](../getting_started/configuration.md).

### Failure policies

A plan's `failure_policy` decides what an ordinary step failure does to the rest of the attempt. An ordinary step failure is any exception the step raises, including `PermanentStepError` and `TransientStepError` (retries are not implemented), and a required output the step did not produce.

| `failure_policy` | After a step fails | `Engine.run` returns |
|---|---|---|
| `fail_fast` (default) | The attempt stops. Steps not run yet are never reached. | `None` if every step succeeded. The failure propagates; a `TransientStepError` surfaces as a `PermanentStepError`. |
| `continue_independent` | The failure is recorded, and every step that depends on it, directly or through other steps, is **blocked**. Everything else keeps running until no step is ready. | The finished `AttemptReport`. Its `outcome` is `"failed"` if any step failed or was blocked. A returned report is **not** a success. |

A plan without `failure_policy` runs under `fail_fast`, exactly as it did before failure policies existed.

Whichever the policy, some failures stop the whole attempt rather than one step: event publication and cache-marker bookkeeping failures (`OrchestrationError`), and cancellation. `Engine.run` then raises under either policy. The attempt's published report (`plan.attempt_report`, below) still records what was established before it stopped.

Each step in an attempt ends with one outcome:

| Outcome | Meaning | Satisfies a dependent's `deps`? |
|---|---|---|
| `succeeded` | Executed and completed | Yes |
| `reused` | An earlier success was still valid and was reused | Yes |
| `failed` | Executed, and failed | No |
| `blocked` | Never invoked: a prerequisite failed or was blocked | No |

A step without an outcome was never reached: a `fail_fast` failure, a cancellation or an infrastructure failure ended the attempt first. Blocked and unreached work are always told apart. A blocked step's report entry names its **direct blockers** (the failed or blocked prerequisites) and its **failed ancestors** (every failed step upstream of it).

Only dependencies decide what a failure blocks. The engine gives no step special treatment for its name or position. To make one step mandatory for another, such as a metadata update before a set of branches, list it in `deps`. A step left out of `deps` does not stop the branch when it fails; the attempt as a whole still ends failed. See [Realm authoring](../realm_authoring/guide.md#dependencies-and-failure-policy).

Operational callers (the daemon and `run-doc --run-once`) interpret the report under both policies, and record the result on the plan document. See [Plan Execution](../reference/plan_execution.md).

---

## Fingerprint computation

Default fingerprint = `sha256(JSON(params) + digests_of_inputs + declared outputs)`.

Input digests are sourced from (in priority order):
1. `StepSpec.inputs` dict (planner-provided)
2. `fn._input_keys` (declared via `In(...)` annotations on the function)
3. No inputs (params-only fingerprint)

For each input path:
- **File** → `sha256:<hex>`
- **Directory** → `dirhash:<hex>` (hash of sorted paths + sizes + mtimes)
- **Missing** → recorded as `"missing"`

A step's `outputs` are hashed as declared, and only when it has some, so changing a step's declared outputs invalidates its earlier success. Steps without `outputs` fingerprint as they always have.

### Declared outputs and reuse

A step's work directory, `<work_root>/<plan_id>/<step_id>`, belongs to the plan, not to one attempt. A rerun executes the step in the same directory, over whatever the previous attempt left behind. A step that succeeds leaves a `success.fingerprint` marker there.

An attempt **reuses** a step instead of running it when all of these hold:

1. Its prerequisites have succeeded or been reused in this attempt. A blocked step is never reused, whatever its marker says.
2. Its marker matches its current fingerprint.
3. Every path in its `outputs` exists.

Reuse emits `step.skipped` with `reason: "cache_hit"`, and satisfies the step's dependents like a success. A missing declared output makes the step run again. The engine logs why it did not reuse the step.

Declared outputs work as follows:

- **Paths.** An absolute path is used as is. Any other path is relative to the producing step's own work directory, never the plan directory or the process's working directory. `PlanBuilder` always declares absolute paths.
- **Presence only.** A path counts as present when it exists. The existence of a directory says nothing about whether its contents are complete, so declare a file, or a completion sentinel, inside it when completeness matters. Failing to check a path at all, such as a permission error, aborts the attempt rather than counting as missing.
- **No outputs, no check.** A step that declares no outputs is reused on its marker alone, as before. Any artifact-producing step that later steps rely on should declare its required outputs. A step that produces no artifact, such as a validation, needs none.

When a step is not reused, its marker is removed before it is called, so a rerun that fails part-way never leaves an earlier success reusable over outputs it partly replaced. A step's success is then finalized in one order: its declared outputs are checked, `step.succeeded` is published, the new marker is written atomically (a same-filesystem rename), and only then do its dependents become ready. A failure to write the marker aborts the attempt.

The marker is not a transaction across the event spool, the artifacts and the plan store, and nothing is flushed to disk. Realm steps must still tolerate being run again over their own earlier output.

---

## Event system

### Event spool layout

```
$YGG_EVENT_SPOOL/
  <realm>/
    <plan_id>/
      <execution_id>_plan_attempt_started.json
      <execution_id>_plan_attempt_report.json
      <step_id>/
        <execution_id>_step_blocked.json     # only if the attempt blocked the step
        <run_id>/
          0001_step_started.json
          0002_step_progress.json
          0003_step_artifact.json
          0004_step_succeeded.json
```

Plan-level records sit directly in the plan's directory. Every attempt writes its own, since the file names carry its execution ID. Each reuse or execution of a step gets its own `<run_id>` directory. A blocked step never ran, so its record sits in the step's directory, with no run directory.

### Event types

| Type | When emitted |
|------|-------------|
| `plan.attempt_started` | An attempt is admitted, before preflight and before any step runs. Carries the attempt's identity, failure policy and every planned step, so an attempt that runs no step is still visible |
| `step.started` | Step function entered |
| `step.progress` | Optional mid-step progress update |
| `step.artifact` | One artifact registered |
| `step.succeeded` | Step finished successfully |
| `step.failed` | Step raised an exception, or did not produce a required output |
| `step.retry_unimplemented` | Follows `step.failed` for a `TransientStepError`: retries are not implemented |
| `step.skipped` | An earlier success was reused (`reason: "cache_hit"`) |
| `step.blocked` | A prerequisite failed or was blocked, so the step will not run in this attempt. Published once, as soon as it is blocked, with the blockers known at that moment |
| `plan.attempt_report` | Once, whenever the attempt ends, including by failure, cancellation or preflight rejection. Carries the full report: termination reason, outcome, every step's outcome, failures, and the complete blocker lists |

Each event JSON record contains `type`, `ts`, `eid`, `realm`, `scope` and `plan_id`. The events of a step's run also carry `seq`, `step_id`, `step_name` and `fingerprint`. A `step.blocked` record carries `step_id`, `step_name`, `direct_blockers` and `failed_ancestors`.

Every event an attempt publishes also carries the attempt's identity: `execution_id`, and the `plan_generation` and `run_token` the attempt captured from the plan document (both `null` for a direct `Engine.run` call). A step's `run_id` identifies one invocation of that step. The `execution_id` says which attempt at the whole plan it belonged to.

Execution IDs have the form `exec_<UTC timestamp>_<32 hex digits>` and are allocated when an attempt is admitted. With the file spool, a later attempt at a plan gets a higher ID than every attempt recorded there, even when the clock stands still or has been set back, so sorting IDs sorts attempts. Attempts started at the same time by independent processes are not ordered against each other.

### Emitters

| Class | Description | Use case |
|-------|-------------|----------|
| `FileSpoolEmitter` | Writes one JSON file per event to the configured spool directory | Default; zero infrastructure, crash-tolerant |
| `TeeEmitter` | Fans out to multiple emitters in parallel | Combine FileSpool with another sink |
| `CouchEmitter` | Writes events as CouchDB documents | When you need queryable event history |

The concrete emitter is configured by the operator at daemon startup (via `YGG_EVENT_SPOOL` or, in future, `main.json`). Realm step functions interact with the emitter only via `ctx.emitter`, which is typed as `EventEmitter`. Realm code should never import or instantiate concrete emitter classes directly.

---

## DataAccess (`ctx.data`)

`DataAccess` is the realm's phase-aware gateway to configured external connections. It is injected into both `StepContext` (execution phase) and `PlanningContext` (planning phase) as `ctx.data`.

### Two clients, two phases

Calling `ctx.data.connection(name)` returns a different client type depending on the phase:

| Phase | Client returned | Read methods | Write methods |
|-------|-----------------|--------------|---------------|
| `"planning"` | `CouchDBPlanningClient` | `async` — must be `await`ed | none |
| `"execution"` | `CouchDBExecutionClient` | sync — return results directly | `save()` |

**Planning phase** (inside `generate_plan_drafts`):

```python
async def generate_plan_drafts(self, payload):
    ctx = payload["planning_ctx"]
    client = ctx.data.connection("my_db")          # CouchDBPlanningClient
    doc = await client.get("some_id")              # async — await required
    docs = await client.find({"status": "ready"})
```

**Execution phase** (inside a `@step` function):

```python
@step
def my_step(ctx: StepContext, item_id: str) -> StepResult:
    client = ctx.data.connection("my_db")          # CouchDBExecutionClient
    doc = client.get(item_id)                      # sync — no await
    docs = client.find({"status": "ready"})
```

### Available read methods

Both clients expose the same read methods (async in planning, sync in execution):

| Method | Returns | Raises |
|--------|---------|--------|
| `get(doc_id)` | `dict \| None` | — |
| `find(selector)` | `list[dict]` | — |
| `find_one(selector)` | `dict \| None` | — |
| `fetch_by_field(field, value)` | `list[dict]` | — |
| `require(doc_id)` | `dict` | `DataAccessNotFoundError` if absent |
| `require_one(selector)` | `dict` | `DataAccessNotFoundError` if no match |

### Writing — `save()` (execution phase only)

`CouchDBExecutionClient.save(doc, *, doc_id, selector, view, mode)` writes a document using one of three identity modes. The `doc` dict must be **clean** — no `_id` or `_rev` keys; the client manages revision tracking internally. Exactly one of `doc_id`, `selector`, or `view` must be provided.

```python
# Write by explicit document ID
result = client.save({"status": "done"}, doc_id="run_123", mode="upsert")
# result.status  → "created" or "updated"
# result.new_rev → new CouchDB revision string
# result.old_rev → previous revision (None if created)
# result.identity → "doc_id"

# Write without _id — CouchDB generates one (selector identity)
result = client.save({"status": "done"}, selector={"type": "run", "run_id": "r-1"}, mode="upsert")
# result.identity → "selector"
# result.doc_id   → CouchDB-generated or matched ID

# Write by view row (view identity)
result = client.save({"status": "done"}, view={"design": "runs", "view": "by_id", "key": "r-1"}, mode="update")
# result.identity → "view"
```

| `mode` | Behaviour |
|--------|-----------|
| `"create"` | Fail (raise) if the document already exists |
| `"update"` | Fail (raise) if the document does not exist |
| `"upsert"` | Create if absent, update if present; retries once on conflict |

`DataAccessWriteResult` fields:

| Field | Type | Description |
|-------|------|-------------|
| `status` | `"created" \| "updated"` | Outcome |
| `doc_id` | `str` | Resolved or provided document ID |
| `operation` | `"create" \| "update" \| "upsert"` | Requested write mode |
| `identity` | `"doc_id" \| "selector" \| "view"` | Identity resolution method used |
| `old_rev` | `str \| None` | Previous revision (`None` if created) |
| `new_rev` | `str \| None` | New revision after write |

### API entry points

`ctx.data.connection(name)` is the preferred form — it works for any supported backend.
`ctx.data.couchdb(name)` is a CouchDB-specific alias that validates the backend type before delegating to `connection()`. Use it when you want to make the CouchDB dependency explicit.

### Permission model

Access is controlled per-realm, per-phase in the connection's `data_access.realms` config block:

- A realm must appear in `realms` for a given connection to access it at all.
- Each phase (`planning`, `execution`) has its own `permissions` list.
- `"read"` grants access to `get`, `find`, and related methods.
- `"write"` grants access to `save()`. It does **not** imply `"read"` — a realm with only `"write"` can call `save()` but not `get()`.
- Planning phase requires at least `"read"` — a planning-phase connection with write-only permission is denied.

### Configuration shape

```json
"my_db": {
    "endpoint": "couchdb",
    "resource": { "db": "actual_db_name" },
    "data_access": {
        "realms": {
            "my_realm": {
                "planning":   { "permissions": ["read"] },
                "execution":  { "permissions": ["read", "write"] }
            }
        },
        "options": { "max_limit": 50 }
    }
}
```

Global backend defaults (e.g. `defaults.couchdb.max_limit`) are merged with per-connection `options`; the connection-level value wins on conflict. See [Configuration](../getting_started/configuration.md) for the full schema.

---

## Artifact protocol

Artifacts implement `ArtifactRefProtocol`:

- `key()` → semantic label string
- `resolve_path(scope_dir: Path)` → absolute path

The built-in `SimpleArtifactRef` covers the common case:

```python
from yggdrasil.flow.artifacts import SimpleArtifactRef

ref = SimpleArtifactRef(key="pipeline_output", relative_path="results/output")
abs_path = ref.resolve_path(scope_dir)
```

Register an artifact from within a step:

```python
ctx.record_artifact(ref, path=outs_dir, digest=compute_dirhash(outs_dir))
```

---

## See also

- [Architecture Overview](../architecture/overview.md) — how the Engine fits into the broader event flow
- [Realm Authoring Cookbook](../realm_authoring/cookbook.md) — step writing patterns, recipe factories, progress emission
- [Glossary](../reference/glossary.md) — full terminology reference
