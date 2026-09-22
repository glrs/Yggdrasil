# Realm Authoring Cookbook

Common patterns for realm authors. For the full reference, see [guide.md](guide.md).

---

## Pattern 1: Dev-mode gating (realm invisible in production)

Return `None` from `get_realm_descriptor()` when the realm should not be active:

```python
# my_realm/__init__.py
from lib.core_utils.ygg_session import YggSession
from yggdrasil.core.realm.descriptor import RealmDescriptor


def get_realm_descriptor() -> RealmDescriptor | None:
    if not YggSession.is_dev():
        return None  # Not discovered at all in production
    from my_realm.handler import MyDevHandler
    return RealmDescriptor(
        realm_id="my_realm",
        handler_classes=[MyDevHandler],
        watchspecs=_get_watchspecs,
    )
```

**When to use:** Dev-only or debug realms that must be completely invisible in production.

**Alternative:** Return the descriptor but make `watchspecs` return `[]` when disabled — the handler stays registered for CLI use, but no automatic events fire.

---

## Pattern 2: Multiple handlers in one realm

A single realm can export multiple handlers for different event types:

```python
def get_realm_descriptor() -> RealmDescriptor:
    return RealmDescriptor(
        realm_id="my_realm",
        handler_classes=[MyProjectHandler, MyDeliveryHandler],
        watchspecs=_get_watchspecs,
    )
```

```python
class MyProjectHandler(BaseHandler):
    event_type: ClassVar[EventType] = EventType.COUCHDB_DOC_CHANGED
    handler_id: ClassVar[str] = "project_handler"
    ...

class MyDeliveryHandler(BaseHandler):
    event_type: ClassVar[EventType] = EventType.COUCHDB_DOC_CHANGED
    handler_id: ClassVar[str] = "delivery_handler"
    ...
```

Route each WatchSpec to a specific handler via `target_handlers`:

```python
WatchSpec(
    backend="couchdb",
    connection="projects_db",
    event_type=EventType.COUCHDB_DOC_CHANGED,
    filter_expr={"==": [{"var": "doc.type"}, "project"]},
    build_scope=_project_scope,
    build_payload=_project_payload,
    target_handlers=["project_handler"],  # Only this handler receives it
),
WatchSpec(
    backend="couchdb",
    connection="projects_db",
    event_type=EventType.COUCHDB_DOC_CHANGED,
    filter_expr={"==": [{"var": "doc.type"}, "delivery"]},
    build_scope=_delivery_scope,
    build_payload=_delivery_payload,
    target_handlers=["delivery_handler"],
),
```

---

## Pattern 3: Schema-driven routing in a single handler

When one handler needs to dispatch to different plan shapes based on document content:

```python
async def generate_plan_drafts(self, payload: dict[str, Any]) -> list[PlanDraft]:
    doc = payload["doc"]
    ctx: PlanningContext = payload["planning_ctx"]

    analysis_type = doc.get("analysis_type", "default")

    if analysis_type == "mode_a":
        steps = build_mode_a_steps(doc, ctx)
    elif analysis_type == "mode_b":
        steps = build_mode_b_steps(doc, ctx)
    else:
        raise ValueError(f"Unknown analysis_type: {analysis_type!r}")

    plan = Plan(
        plan_id=f"my_realm:{ctx.scope['id']}",
        realm=self.realm_id or "my_realm",
        scope=ctx.scope,
        steps=steps,
    )
    return [PlanDraft(plan=plan, auto_run=True, approvals_required=[], notes="")]
```

---

## Pattern 4: Approval workflow

Set `auto_run=False` to require manual approval before execution:

```python
return PlanDraft(
    plan=plan,
    auto_run=False,             # Plan saved as status="draft"
    approvals_required=["team_lead"],
    notes="Requires review before execution",
    preview={
        "item_count": len(items),
        "estimated_gb": total_gb,
    },
)
```

The plan is stored in `yggdrasil_plans` with `status="draft"`. It executes once an operator sets `status="approved"`. (Currently there is no UI available to perform this task.) Approval is all `status` ever records: the outcome of a run is kept separately, and a later run is requested by raising `run_token`. See [Plan Execution](../reference/plan_execution.md).

---

## Pattern 5: Writing steps with StepContext

Steps receive a `StepContext` providing workdir, emitter, scope, and realm. Decorate with `@step`:

```python
from yggdrasil.flow.step import step, StepContext
from yggdrasil.flow.model import StepResult
from yggdrasil.flow.artifacts import SimpleArtifactRef


@step
def run_pipeline(ctx: StepContext, config_file: str, threads: int = 4) -> StepResult:
    """Run an external pipeline tool."""
    cmd = [
        "my_tool", "run",
        "--id", ctx.scope["id"],
        "--config", config_file,
        "--threads", str(threads),
    ]

    # ctx.workdir is the step's own directory, reused by every attempt
    result = subprocess.run(cmd, cwd=ctx.workdir, capture_output=True)

    if result.returncode != 0:
        raise RuntimeError(f"my_tool failed: {result.stderr.decode()}")

    # Register output directory as artifact.
    # record_artifact() requires an ArtifactRefProtocol object, not a plain string.
    # Use SimpleArtifactRef(key_name, folder) for the common case.
    outs_dir = ctx.workdir / ctx.scope["id"] / "output"
    ctx.record_artifact(SimpleArtifactRef("pipeline_output", "output"), path=outs_dir)

    return StepResult(metrics={"returncode": result.returncode})
```

**`StepContext` fields:**

| Field | Type | Description |
|-------|------|-------------|
| `realm` | `str` | Realm ID |
| `scope` | `dict` | Scope dict (`{"kind": ..., "id": ...}`) |
| `plan_id` | `str` | Current plan ID |
| `step_id` | `str` | Current step ID |
| `step_name` | `str` | Human-readable step name |
| `workdir` | `Path` | The step's work directory, `<work_root>/<plan_id>/<step_id>`, shared by every attempt at the plan |
| `scope_dir` | `Path` | Shared scope directory across all steps in this plan |
| `emitter` | `BaseEmitter` | Event emitter for progress/artifact events |
| `run_mode` | `str` | `"auto"` or `"manual"` |
| `fingerprint` | `str` | SHA-256 fingerprint for this run |
| `run_id` | `str` | Unique run ID |
| `data` | `DataAccess` | Phase-aware gateway to configured data sources. Call `ctx.data.connection(conn)` to get a sync client for reads (`get`, `find`, `require`, …) and writes (`save`). |

---

## Pattern 6: Emitting progress from a long step

Use `ctx.emitter` to emit progress events so operators know a long step is alive:
(It is generally recommended to keep steps as short as possible - i.e. perform a well defined single task)

```python
@step
def step_sleep(ctx: StepContext, duration_sec: float) -> StepResult:
    import time
    from yggdrasil.flow.events.emitter import ProgressEvent

    steps = 4
    for i in range(1, steps + 1):
        time.sleep(duration_sec / steps)
        pct = int(i / steps * 100)
        ctx.emitter.emit(ProgressEvent(
            realm=ctx.realm,
            scope=ctx.scope,
            plan_id=ctx.plan_id,
            step_id=ctx.step_id,
            run_id=ctx.run_id,
            message=f"Sleep {pct}% complete",
            percent=pct,
        ))

    return StepResult(metrics={"slept_sec": duration_sec})
```

---

## Pattern 7: Recipe factory — common step patterns

Use a `recipes.py` module to keep handler logic thin. A recipe is a plain function returning a list of `StepSpec`:

```python
# my_realm/recipes.py
from yggdrasil.flow.model import StepSpec

_PREFIX = "my_realm.steps"


def standard_pipeline(item_id: str, config: str) -> list[StepSpec]:
    return [
        StepSpec(
            step_id="process",
            name="Process item",
            fn_ref=f"{_PREFIX}.run_processor",
            params={"item_id": item_id, "config": config},
        ),
        StepSpec(
            step_id="report",
            name="Generate report",
            fn_ref=f"{_PREFIX}.run_reporter",
            params={"item_id": item_id},
            deps=["process"],
        ),
    ]
```

In the handler:

```python
from my_realm.recipes import standard_pipeline

steps = standard_pipeline(item_id=doc["_id"], config=doc["config"])
```

**Metadata harvest pattern** — when domain metadata from the triggering doc should be baked into plan params as a structured dict (so the plan record is self-documenting):

```python
# my_realm/recipes.py

def analysis_pipeline(scenario: dict) -> list[StepSpec]:
    """Recipe that carries harvested doc metadata as a structured dict."""
    return [
        StepSpec(
            step_id="run_analysis",
            fn_ref=f"{_PREFIX}.run_analysis",
            params={"scenario": scenario},   # structured dict, not a string
        ),
        StepSpec(
            step_id="report",
            fn_ref=f"{_PREFIX}.run_reporter",
            params={"sample_id": scenario["sample_id"]},
            deps=["run_analysis"],
        ),
    ]
```

```python
# my_realm/handler.py

async def generate_plan_drafts(self, payload):
    doc = payload["doc"]
    ctx = payload["planning_ctx"]

    # Harvest domain fields — map doc structure into a clean dict
    scenario = {
        "input_path": doc["input_path"],
        "mode": doc.get("mode", "default"),
        "priority": doc.get("priority", 0),
        "sample_id": doc["sample_id"],
        "flags": doc.get("flags", []),
    }

    steps = analysis_pipeline(scenario=scenario)
    preview = {"scenario": scenario, "step_count": len(steps)}
    ...
```

The step receives the structured dict through `params` and can access individual fields:

```python
@step
def run_analysis(ctx: StepContext, scenario: dict) -> StepResult:
    input_path = scenario["input_path"]
    mode = scenario["mode"]
    # ...
```

**When to use:** Any time a real domain document (a sequencing run, a delivery record, an order) drives plan generation. Map the fields explicitly rather than passing the whole doc or building a formatted string.

---

## Pattern 8: Plan-time data fetch

To embed data fetched from CouchDB directly into step params as a **structured dict** (so the plan record shows exactly what was fetched and is queryable):

```python
async def generate_plan_drafts(self, payload: dict[str, Any]) -> list[PlanDraft]:
    ctx: PlanningContext = payload["planning_ctx"]

    # Fetch at planning time — use the async API (handler runs in async context)
    # connection() is the preferred backend-neutral form; couchdb() is a CouchDB-specific
    # alias that validates the backend type and delegates to connection().
    client = ctx.data.connection("config_db")
    doc = await client.get("config:pipeline_defaults")

    # Build a structured dict — not a formatted string
    if doc is None:
        ref = {"doc_id": "config:pipeline_defaults", "missing": True}
    else:
        ref = {
            "doc_id": doc["_id"],
            "config_path": doc.get("default_config", "/fallback/defaults.yaml"),
            "version": doc.get("version"),
            "missing": False,
        }

    steps = [
        StepSpec(
            step_id="process",
            fn_ref="my_realm.steps.run_processor",
            params={"ref_doc": ref},    # structured dict baked into plan params
        ),
    ]
    preview = {"ref_doc": ref}          # keep preview structured too
    ...
```

The step receives the dict through its `params` and can access fields directly:

```python
@step   # required — emits step.started / step.succeeded / step.failed
def run_processor(ctx: StepContext, ref_doc: dict) -> StepResult:
    if ref_doc.get("missing"):
        raise RuntimeError("Reference config not found in database")
    config_path = ref_doc["config_path"]
    # ...
```

**When to use:** When the step itself doesn't need live data access, but the plan record should document exactly what configuration was resolved at plan-generation time.  Using a structured dict (rather than a formatted string) keeps the plan record queryable and makes it clear what fields were inspected.

**Alternative — fetch at *execution time* inside the step:** Steps are synchronous (`def`, not `async def`). Call `ctx.data.connection(conn)` to get a synchronous client — all read methods return results directly:

```python
@step
def run_processor(ctx: StepContext, item_id: str) -> StepResult:
    # Execution steps are synchronous — connection() returns a sync client directly
    client = ctx.data.connection("config_db")
    doc = client.get("config:pipeline_defaults")         # returns dict or None
    # or: client.find(selector)                          # list of matching docs
    # or: client.require(doc_id)                         # raises DataAccessNotFoundError if absent
    # or: client.find_one(selector)                      # first match or None
    # or: client.fetch_by_field(field, value)            # equality shorthand
    # or: client.require_one(selector)                   # raises DataAccessNotFoundError if no match
    config_path = doc["default_config"] if doc else "/fallback/defaults.yaml"
    # ...
```

The fetch is visible via step events and metrics (not baked into plan params), which is appropriate when live data is needed at run time.

---

## Pattern 8b: Writing to CouchDB in a step

Steps with write permission can call `save()` on the execution client. Pass a clean body dict — do not include `_id` or `_rev`; the client manages them. Provide exactly one of `doc_id`, `selector`, or `view` to identify the target document:

```python
@step
def update_run_status(ctx: StepContext, run_id: str) -> StepResult:
    client = ctx.data.connection("flowcell_db")

    # Body must not contain _id or _rev — those are managed by the client
    body = {"status": "complete", "processed_by": "yggdrasil"}

    # Write by explicit document ID
    result = client.save(body, doc_id=run_id, mode="upsert")
    # result.status is "created" or "updated"
    # result.old_rev / result.new_rev carry revision info
    # result.identity → "doc_id"

    return StepResult(metrics={"write_status": result.status, "new_rev": result.new_rev})
```

To let CouchDB auto-generate the document ID, use selector or view identity instead:

```python
# CouchDB generates the _id on create; selector matches the doc on update
result = client.save(body, selector={"type": "run_status", "run_id": run_id}, mode="upsert")
# result.identity → "selector"
# result.doc_id   → CouchDB-generated ID (on create) or matched ID (on update)
```

**`mode` values:**

| `mode` | Behaviour |
|--------|-----------|
| `"create"` | Fail if the document already exists (409) |
| `"update"` | Fail if the document does not exist |
| `"upsert"` | Create if absent, update if present; retries once on conflict |

> **Note on write vs read:** A realm configured with only `"write"` permission can call `save()` but not `get()` or `find()`. Grant `"read"` explicitly for realms that need both.

---

## Pattern 9: Declaring step inputs for fingerprinting

To make the Engine re-run a step when an input file changes (not just params), declare inputs:

```python
# In the plan
StepSpec(
    step_id="transform",
    fn_ref="my_realm.steps.run_transform",
    params={"item_id": "item-001"},
    inputs={"input_file": "/path/to/prepared.dat"},  # tracked for fingerprint
    deps=["prepare"],
)
```

Or declare via type annotation on the step function:

```python
from typing import Annotated
from yggdrasil.flow.artifacts import In, Out

@step
def run_transform(
    ctx: StepContext,
    input_file: Annotated[Path, In("input_file")],
    item_id: str,
) -> StepResult:
    ...
```

The Engine computes `sha256(params + sha256(input_file))` as the fingerprint. If the file changes, the cached fingerprint mismatches and the step re-runs.

---

## Pattern 10: Independent branches that finish when one fails

When a plan holds one branch per lane, sample or delivery, a failure in one branch need not stop the others. Build each branch from a recipe with stable, namespaced step IDs, point each branch's first step at the shared prerequisites, and set `failure_policy="continue_independent"`:

```python
# my_realm/recipes.py
from yggdrasil.flow.model import CONTINUE_INDEPENDENT_POLICY, Plan, StepSpec

_PREFIX = "my_realm.steps"


def lane_branch(lane: int, prerequisites: list[str]) -> list[StepSpec]:
    """One lane's chain: process, then upload."""
    ns = f"lane_{lane}"
    return [
        StepSpec(
            step_id=f"{ns}__process",
            name=f"Process lane {lane}",
            fn_ref=f"{_PREFIX}.process_lane",
            params={"lane": lane},
            deps=list(prerequisites),
            outputs={"result": "output/DONE"},  # sentinel in this step's workdir
        ),
        StepSpec(
            step_id=f"{ns}__upload",
            name=f"Upload lane {lane}",
            fn_ref=f"{_PREFIX}.upload_lane",
            params={"lane": lane},
            deps=[f"{ns}__process"],
        ),
    ]


def flowcell_plan(plan_id: str, realm: str, scope: dict, lanes: list[int]) -> Plan:
    steps = [
        StepSpec(
            step_id="validate",
            name="Validate inputs",
            fn_ref=f"{_PREFIX}.validate",
            params={},
        ),
    ]
    for lane in lanes:
        steps.extend(lane_branch(lane, prerequisites=["validate"]))
    return Plan(
        plan_id=plan_id,
        realm=realm,
        scope=scope,
        steps=steps,
        failure_policy=CONTINUE_INDEPENDENT_POLICY,
    )
```

If `lane_2__process` fails, `lane_2__upload` is blocked, every other lane still runs, and the attempt ends failed. Its request counts as finished: rerunning it takes a raised `run_token`, and the rerun reuses the lanes that already succeeded.

**When to use:** Branches that do not need each other's results. Make a shared step a prerequisite only of the branches that really need it. See [Dependencies and failure policy](guide.md#dependencies-and-failure-policy) for the choices, and [Plan Execution](../reference/plan_execution.md) for what operators see.

---

## See also

- [Realm Authoring Guide](guide.md) — full reference for `RealmDescriptor`, `WatchSpec`, validation rules
- [Flow API Overview](../flow_api/overview.md) — `@step`, `Engine`, emitters, `PlanDraft` fields
- [Architecture Overview](../architecture/overview.md) — how realms plug into the core
- [Test Realm](../reference/test_realm.md) — running test scenarios to validate your pipeline
