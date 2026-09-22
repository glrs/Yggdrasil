# Test Realm Scenarios

This document describes test scenario documents that exercise different aspects of the plan approval and execution pipeline.

**_Disclaimer: Mainly used for internal dev/test purposes. Might contain mistakes or deprecated information_**

## Overview

Test scenarios are inserted into the yggdrasil database as documents with `type="ygg_test_scenario"`. The `ScenarioDocWatcher` detects them and generates plans via predefined recipes **or custom step definitions**.

**Two modes supported:**

1. **Recipe-based**: Use predefined recipes (easy, recommended for common patterns)
2. **Custom steps**: Define steps directly in the document (flexible, for advanced testing)

Each scenario below shows:
- **Recipe** (if using recipe mode): Which predefined recipe to use
- **Purpose**: What it tests
- **Auto-run**: Whether the plan auto-executes or waits for approval
- **Overrides**: Optional parameter customizations
- **Expected behavior**: What should happen

Available recipes:

Standard recipes (selected via `"recipe"` field, in RECIPES registry):
- **happy_path**: All steps succeed (echo_start → brief_sleep(0.5s) → echo_end)
- **random_fail**: Probabilistic failure (50% chance by default) - tests retry logic
- **fail_fast**: First step always fails
- **fail_mid_plan**: Succeeds initially (echo → sleep), then fails mid-execution
- **long_running**: Extended sleep (30s default) for testing responsiveness
- **artifact_write**: Creates files and registers artifacts
- **branch_failure**: Shared validation, a metadata update and two lane branches; the metadata update and lane 2 fail, lane 1 completes (`continue_independent` by default)
- **branch_failure_metadata_required**: The same plan with the metadata update a prerequisite of both lanes, so both are blocked (`continue_independent` by default)
- **data_fetch_exec**: Fetches a CouchDB doc at *execution time* inside the step
- **data_access_denied**: Verifies DataAccess correctly rejects unauthorized connections
- **data_fetch_all_methods**: Exercises every read method on the execution-phase DataAccess client
- **data_verify_limit_clamping**: Confirms `find()` results are clamped to the effective `max_limit` from `data_access.options`
- **data_write_exec**: Writes a document to CouchDB at execution time via `client.save()`; proves write permission works end-to-end
- **data_write_only_permission**: Proves write permission does not imply read; `save()` must succeed, `get()` must raise `DataAccessDeniedError`
- **data_write_no_id**: Writes a document without supplying a `_id`; CouchDB auto-generates the ID via selector identity; the resolved `doc_id` appears in step metrics

Planning-time recipes (handler processes the doc before building steps; not in RECIPES registry):
- **data_fetch_plan**: Async-fetches a CouchDB doc *during planning*; bakes the result as a structured `ref_doc` dict into step params.
- **metadata_harvest**: Extracts domain fields (`input_path`, `mode`, `priority`, `sample_id`, `flags`) from the scenario doc; bakes them as a structured `scenario` dict into step params.

Every plan runs under the scenario's `failure_policy` field when it has one (`"fail_fast"` or `"continue_independent"`; any other value rejects the scenario). Without it, the `branch_failure` recipes run under `continue_independent`, and every other recipe and custom steps under `fail_fast`. So any recipe can be compared under both policies:

```json
{
  "_id": "test_scenario:mid_plan_continue",
  "type": "ygg_test_scenario",
  "recipe": "fail_mid_plan",
  "failure_policy": "continue_independent"
}
```

Available steps (for custom mode):

> All test realm steps are decorated with `@step`, so lifecycle events (`step.started`,
> `step.succeeded`, `step.failed`) are emitted automatically by the decorator. Exceptions
> still bubble up to the Engine: a `fail_fast` plan stops at the first failure, while a
> `continue_independent` plan blocks the failed step's dependents and runs the rest.

- **step_echo**: Echo message (params: `message`)
- **step_sleep**: Configurable sleep with progress events (params: `duration_sec`)
- **step_fail**: Always fails (params: `error_message`)
- **step_random_fail**: Probabilistic failure (params: `failure_probability`, `success_message`, `failure_message`)
- **step_write_file**: Write file to workdir (params: `filename`, `content`)
- **step_fetch_from_db**: Fetch a CouchDB document at execution time (params: `connection`, `doc_id`)
- **step_expect_denied**: Assert that DataAccess correctly rejects a restricted connection (params: `connection`)
- **step_exercise_all_fetch_methods**: Exercise every read method on the execution-phase DataAccess client in one step (params: `connection`, `doc_id`, `selector_type`)
- **step_verify_limit_clamping**: Assert that `find()` results are clamped to the effective `max_limit` from `data_access.options` (params: `connection`, `selector_type`, `request_limit`, `expected_max`)
- **step_emit_metadata**: Emit structured metadata baked into the plan at planning time (params: `scenario` dict and/or `ref_doc` dict)
- **step_write_to_db**: Write a document via `client.save(doc, doc_id=..., mode=...)` with write permission; returns `write_status`, `doc_id`, `old_rev`, `new_rev` in metrics (params: `connection`, `doc_id`, `mode`)
- **step_write_to_db_no_id**: Write a document using Mango selector identity — no `_id` supplied, CouchDB auto-generates it; returns `write_status`, `doc_id`, `old_rev`, `new_rev`, `identity`, `operation` in metrics (params: `connection`, `selector`, `mode`)
- **step_expect_read_denied**: Assert that `client.get()` raises `DataAccessDeniedError` on a write-only connection; step succeeds only if denial is raised (params: `connection`, `doc_id`)

---

## Scenario 1: Simple Success (Auto-run, Happy Path)

**Purpose**: Verify basic happy path with auto-execution.

**Insert as**:
```json
{
  "_id": "test_scenario:simple_success",
  "type": "ygg_test_scenario",
  "recipe": "happy_path",
  "name": "Simple Success",
  "description": "Basic happy path: echo → sleep → echo, auto-runs immediately",
  "auto_run": true
}
```

**Expected**:
- Plan created with `auto_run=true` → `status="approved"`
- PlanWatcher picks it up immediately
- All steps succeed (echo_start → brief_sleep(0.5s) → echo_end)
- Execution completes in ~0.5 seconds
- `executed_run_token` updated after completion

---

## Scenario 2: Pending Approval (Draft)

**Purpose**: Test plan approval workflow.

**Insert as**:
```json
{
  "_id": "test_scenario:pending_approval",
  "type": "ygg_test_scenario",
  "recipe": "happy_path",
  "name": "Pending Approval",
  "description": "Plan requires manual approval before execution",
  "auto_run": false
}
```

**Expected**:
- Plan created with `auto_run=false` → `status="draft"`
- PlanWatcher ignores it (not approved yet)
- Plan stays in DB indefinitely
- **Manual step**: Approve plan manually (set `status="approved"`, increment `run_token`)
- Then PlanWatcher detects change and executes
- Demonstrates approval workflow integration

---

## Scenario 3: Long-Running (Thread Pool Test)

**Purpose**: Verify that long steps don't block the event loop.

**Insert as**:
```json
{
  "_id": "test_scenario:long_running",
  "type": "ygg_test_scenario",
  "recipe": "long_running",
  "name": "Long Running (40s)",
  "description": "Tests that watchers remain responsive during 40-second execution",
  "auto_run": true,
  "overrides": {
    "long_sleep": {
      "duration_sec": 40.0
    }
  }
}
```

**Expected**:
- Plan executes in thread pool (non-blocking)
- During execution, other watchers remain responsive
- Progress events emitted at 25%, 50%, 75%, 100% by the sleep step
- Execution takes ~40 seconds (wall clock)
- Event loop not blocked; other plans can be queued
- Can monitor watchers in separate terminal to verify responsiveness

**Variant (shorter test, 5 seconds)**:
```json
{
  "_id": "test_scenario:long_running_5s",
  "type": "ygg_test_scenario",
  "recipe": "long_running",
  "name": "Long Running (5s)",
  "description": "5-second sleep for faster testing of thread pool",
  "auto_run": true,
  "overrides": {
    "long_sleep": {
      "duration_sec": 5.0
    }
  }
}
```

---

## Scenario 4: Fail Fast

**Purpose**: Test error handling and immediate failure.

**Insert as**:
```json
{
  "_id": "test_scenario:fail_fast",
  "type": "ygg_test_scenario",
  "recipe": "fail_fast",
  "name": "Fail Fast",
  "description": "First step fails immediately; subsequent steps never execute",
  "auto_run": true
}
```

**Expected**:
- Step 1 (fail_immediately) fails with RuntimeError (emits `step.failed`)
- Step 2 (never_reached) never executes: it is unreached, not blocked
- Plan execution halts with error
- `executed_run_token` NOT updated (plan remains eligible for retry): a `fail_fast` failure does not finish its request

---

## Scenario 5: Fail Mid-Plan

**Purpose**: Test partial execution and failure recovery.

**Insert as**:
```json
{
  "_id": "test_scenario:fail_mid_plan",
  "type": "ygg_test_scenario",
  "recipe": "fail_mid_plan",
  "name": "Fail Mid-Plan",
  "description": "Starts successfully, fails in the middle, last steps never execute",
  "auto_run": true
}
```

**Expected**:
- Step 1 (echo_start) succeeds (emits `step.succeeded`)
- Step 2 (brief_sleep, 0.3s) succeeds (emits `step.succeeded`)
- Step 3 (mid_failure) fails with RuntimeError (emits `step.failed`)
- Step 4 (never_reached) never executes
- Plan marked as failed
- `executed_run_token` NOT updated (eligible for retry)
- With `"failure_policy": "continue_independent"`, step 4 is blocked instead, and the request is finished: `executed_run_token` is updated and `last_finalized_execution.outcome` is `"failed"`

---

## Scenario 6: Artifact Write

**Purpose**: Test artifact registration and retrieval.

**Insert as**:
```json
{
  "_id": "test_scenario:artifact_write",
  "type": "ygg_test_scenario",
  "recipe": "artifact_write",
  "name": "Artifact Write",
  "description": "Creates output files and registers artifacts",
  "auto_run": true
}
```

**Expected**:
- Step 1 (echo_start) succeeds
- Step 2 (write_artifact) creates a file and registers it as artifact
- Step 3 (echo_end) succeeds
- All steps complete successfully
- Artifacts tracked in plan document
- Files retrievable from plan's scope directory

---

## Scenario 7: Happy Path with Custom Sleep Duration

**Purpose**: Test parameter override mechanism.

**Insert as**:
```json
{
  "_id": "test_scenario:custom_sleep",
  "type": "ygg_test_scenario",
  "recipe": "happy_path",
  "name": "Custom Sleep Duration",
  "description": "Happy path with longer sleep via overrides",
  "auto_run": true,
  "overrides": {
    "brief_sleep": {
      "duration_sec": 3.0
    }
  }
}
```

**Expected**:
- All steps succeed
- Sleep step takes 3.0 seconds (instead of default 0.5s)
- Progress events emitted at 25%, 50%, 75%, 100% (at ~0.75s, ~1.5s, ~2.25s, ~3.0s)
- Total execution time ~3.0 seconds
- Demonstrates parameter override system

---

## Scenario 8: Quick Echo (Baseline Performance)

**Purpose**: Baseline performance test; verify minimal overhead.

**Insert as**:
```json
{
  "_id": "test_scenario:quick_echo",
  "type": "ygg_test_scenario",
  "recipe": "happy_path",
  "name": "Quick Echo",
  "description": "Minimal execution; should complete in <100ms",
  "auto_run": true,
  "overrides": {
    "brief_sleep": {
      "duration_sec": 0.01
    }
  }
}
```

**Expected**:
- All steps execute quickly
- Execution latency <100ms (minimal sleep, mostly overhead)
- Events emitted: echo_start, sleep (brief), echo_end
- Plan marked complete almost immediately
- Useful for timing overhead

---

## Scenario 9: Random Failure (Retry Logic Test)

**Purpose**: Test probabilistic failure and retry logic.

**Insert as**:
```json
{
  "_id": "test_scenario:random_fail_50",
  "type": "ygg_test_scenario",
  "recipe": "random_fail",
  "name": "Random Failure (50%)",
  "description": "50% chance of failure; ideal for testing retry mechanisms",
  "auto_run": true
}
```

**Expected**:
- Step 1 (echo_start) always succeeds
- Step 2 (random_step) has 50% chance of failure
  - If succeeds: Step 3 (echo_end) executes; plan completes
  - If fails: Plan execution halts; `executed_run_token` NOT updated (eligible for retry)
- On retry (manual re-approval or automatic), rolls dice again

**Variant (90% failure for testing retry resilience)**:
```json
{
  "_id": "test_scenario:random_fail_90",
  "type": "ygg_test_scenario",
  "recipe": "random_fail",
  "name": "Random Failure (90%)",
  "description": "90% failure rate; tests retry exhaustion",
  "auto_run": true,
  "overrides": {
    "random_step": {
      "failure_probability": 0.9
    }
  }
}
```

**Variant (10% failure for testing eventual success)**:
```json
{
  "_id": "test_scenario:random_fail_10",
  "type": "ygg_test_scenario",
  "recipe": "random_fail",
  "name": "Random Failure (10%)",
  "description": "10% failure rate; usually succeeds",
  "auto_run": true,
  "overrides": {
    "random_step": {
      "failure_probability": 0.1
    }
  }
}
```

---

## Scenario 10: Custom Steps (Single Echo)

**Purpose**: Test custom step definition without recipe.

**Insert as**:
```json
{
  "_id": "test_scenario:custom_single_echo",
  "type": "ygg_test_scenario",
  "name": "Custom Single Echo",
  "description": "Single custom step: echo",
  "auto_run": true,
  "steps": [
    {
      "step_id": "my_echo",
      "name": "My Custom Echo",
      "fn_name": "step_echo",
      "params": {
        "message": "Hello from custom step definition!"
      }
    }
  ]
}
```

**Expected**:
- Plan created with 1 step (my_echo)
- Step executes successfully
- Message emitted: "Hello from custom step definition!"
- Demonstrates custom step syntax

---

## Scenario 11: Custom Steps (Multi-Step Pipeline)

**Purpose**: Test custom multi-step plan with dependencies.

**Insert as**:
```json
{
  "_id": "test_scenario:custom_pipeline",
  "type": "ygg_test_scenario",
  "name": "Custom Pipeline",
  "description": "Multi-step custom plan: echo → random → sleep → echo",
  "auto_run": true,
  "steps": [
    {
      "step_id": "start",
      "name": "Start Pipeline",
      "fn_name": "step_echo",
      "params": {
        "message": "Pipeline starting"
      }
    },
    {
      "step_id": "chaos",
      "name": "Chaos Step",
      "fn_name": "step_random_fail",
      "params": {
        "failure_probability": 0.3,
        "success_message": "Chaos survived",
        "failure_message": "Chaos triggered failure"
      },
      "deps": ["start"]
    },
    {
      "step_id": "wait",
      "name": "Wait Step",
      "fn_name": "step_sleep",
      "params": {
        "duration_sec": 2.0
      },
      "deps": ["chaos"]
    },
    {
      "step_id": "finish",
      "name": "Finish Pipeline",
      "fn_name": "step_echo",
      "params": {
        "message": "Pipeline complete!"
      },
      "deps": ["wait"]
    }
  ]
}
```

**Expected**:
- 4 steps execute in order (start → chaos → wait → finish)
- 30% chance of failure at chaos step
- If chaos succeeds, waits 2s then finishes
- If chaos fails, wait and finish never execute
- Demonstrates custom step dependencies

---

## Scenario 12: Custom Steps (Isolated Random Test)

**Purpose**: Test single random_fail step in isolation (for debugging).

**Insert as**:
```json
{
  "_id": "test_scenario:isolated_random",
  "type": "ygg_test_scenario",
  "name": "Isolated Random Step",
  "description": "Single random_fail step at 50% for quick retry testing",
  "auto_run": true,
  "steps": [
    {
      "step_id": "random_only",
      "name": "Random Failure",
      "fn_name": "step_random_fail",
      "params": {
        "failure_probability": 0.5
      }
    }
  ]
}
```

**Expected**:
- 50% chance of immediate success
- 50% chance of immediate failure (eligible for retry)
- Fastest scenario for testing retry logic
- No dependencies, just pure random outcome

---

## Scenario 13: Parallel Random Failures

**Purpose**: Test parallel step execution where all must succeed.

**Insert as**:
```json
{
  "_id": "test_scenario:parallel_chaos",
  "type": "ygg_test_scenario",
  "name": "Parallel Chaos",
  "description": "Three parallel random steps; all must succeed",
  "auto_run": true,
  "steps": [
    {"step_id": "init", "name": "Init", "fn_name": "step_echo", "params": {"message": "Starting parallel chaos"}},
    {"step_id": "chaos1", "name": "Chaos 1", "fn_name": "step_random_fail", "params": {"failure_probability": 0.3}, "deps": ["init"]},
    {"step_id": "chaos2", "name": "Chaos 2", "fn_name": "step_random_fail", "params": {"failure_probability": 0.3}, "deps": ["init"]},
    {"step_id": "chaos3", "name": "Chaos 3", "fn_name": "step_random_fail", "params": {"failure_probability": 0.3}, "deps": ["init"]},
    {"step_id": "finish", "name": "Finish", "fn_name": "step_echo", "params": {"message": "All survived!"}, "deps": ["chaos1", "chaos2", "chaos3"]}
  ]
}
```

**Expected**:
- init step runs first
- chaos1, chaos2, chaos3 run in parallel (all depend on init)
- Each has 30% failure probability
- **Probability all succeed:** 0.7³ = 34.3% (high chance of retry needed)
- finish only runs if all chaos steps succeed
- Tests dependency fan-out and fan-in patterns

---

## Scenario 14: Artifact Write with Random Failure

**Purpose**: Test artifact persistence when subsequent step fails.

**Insert as**:
```json
{
  "_id": "test_scenario:artifact_with_chaos",
  "type": "ygg_test_scenario",
  "name": "Artifact with Chaos",
  "description": "Write artifact, then 50% chance of failure",
  "auto_run": true,
  "steps": [
    {"step_id": "write", "name": "Write Artifact", "fn_name": "step_write_file", "params": {"filename": "output.txt", "content": "Test data created before chaos"}},
    {"step_id": "chaos", "name": "Chaos", "fn_name": "step_random_fail", "params": {"failure_probability": 0.5}, "deps": ["write"]},
    {"step_id": "verify", "name": "Verify", "fn_name": "step_echo", "params": {"message": "Artifact survived chaos"}, "deps": ["chaos"]}
  ]
}
```

**Expected**:
- Step 1 (write) creates file and registers artifact
- Step 2 (chaos) has 50% chance of failure
- If chaos fails: artifact was still created (partial execution)
- If chaos succeeds: verify step confirms completion
- Tests artifact persistence across retries

---

## Scenario 15: Metadata Harvest (Planning-Time)

**Purpose**: Demonstrate the real-realm pattern — handler harvests domain fields from the triggering document and bakes them as a structured dict into `StepSpec.params` at plan-generation time.

**Key distinction from overrides**: The handler *reads and maps* fields from the doc into a clean `scenario` dict; the plan record contains structured data, not a formatted string.

**Insert as**:
```json
{
  "_id": "test_scenario:metadata_harvest",
  "type": "ygg_test_scenario",
  "recipe": "metadata_harvest",
  "name": "Metadata Harvest Demo",
  "description": "Harvest domain fields at plan-generation time and bake into step params",
  "auto_run": true,
  "input_path": "/data/sequencing/run_20260312/sample_001.fastq.gz",
  "mode": "full_analysis",
  "priority": 2,
  "sample_id": "SAMPLE-001",
  "flags": ["paired_end", "quality_filter"]
}
```

**Expected**:
- Handler extracts `input_path`, `mode`, `priority`, `sample_id`, `flags` from the doc
- These are baked as `params={"scenario": {...}}` into the first step of the plan
- Inspecting the plan record in `yggdrasil_plans` shows the structured `scenario` dict
- `emit_metadata` step emits `step.metadata_harvested` event with the harvested fields
- `echo_confirm` step logs a friendly summary (sample_id, mode, priority)

**Verify plan params after insertion**:
```bash
curl http://localhost:5984/yggdrasil_plans/test_realm:test_scenario:metadata_harvest | \
  jq '.steps[] | select(.step_id == "emit_metadata") | .params.scenario'
# Expected output:
# {
#   "input_path": "/data/sequencing/run_20260312/sample_001.fastq.gz",
#   "mode": "full_analysis",
#   "priority": 2,
#   "sample_id": "SAMPLE-001",
#   "flags": ["paired_end", "quality_filter"]
# }
```

---

## Scenario 16: Plan-Time Data Fetch (Structured Dict)

**Purpose**: Demonstrate plan-time CouchDB fetch where the result is baked into step params as a **structured dict** (`ref_doc`), not a formatted string. The plan record is self-documenting: it contains the exact doc snapshot that drove the run.

**Requires**: A document `data_access_test:reference_doc` in the `yggdrasil` database, and `yggdrasil_db` configured with `data_access.realms.test_realm.planning.permissions: ["read"]`.

**Insert as**:
```json
{
  "_id": "test_scenario:data_fetch_plan_structured",
  "type": "ygg_test_scenario",
  "recipe": "data_fetch_plan",
  "name": "Plan-Time Fetch (Structured)",
  "description": "Fetch reference doc during planning; result baked as structured dict in plan params",
  "auto_run": true
}
```

**Expected**:
- Handler calls `await ctx.data.connection("yggdrasil_db").get("data_access_test:reference_doc")` during `generate_plan_drafts`
- Result is a structured dict: `{"doc_id": "...", "message": "...", "value": 13, "missing": false}`
- Dict is baked into `echo_fetched.params["ref_doc"]` — visible in the persisted plan record
- `echo_fetched` step emits `step.ref_doc_echoed` event with the dict fields
- `echo_confirm` step logs a friendly message stating fetch succeeded
- If the doc is missing: `ref_doc = {"doc_id": "...", "missing": true}` — plan still created
- If DataAccessError: `ref_doc = {"doc_id": "...", "error": "...", "error_type": "..."}` — plan still created

**Verify plan params after insertion**:
```bash
curl http://localhost:5984/yggdrasil_plans/test_realm:test_scenario:data_fetch_plan_structured | \
  jq '.steps[] | select(.step_id == "echo_fetched") | .params.ref_doc'
# Expected output (success case):
# {
#   "doc_id": "data_access_test:reference_doc",
#   "message": "Reference document for DataAccess testing",
#   "value": 13,
#   "missing": false
# }
```

---

## Scenario 17: Execution-Time Write

**Purpose**: Prove that a step with execution write permission can call `client.save()` via
`ctx.data.connection()` and have the result visible in step metrics and the
`data_access.write.succeeded` event.

**Requires**: Connection `test_realm_write_db` configured with
`data_access.realms.test_realm.execution.permissions: ["write"]`.

**Insert as**:
```json
{
  "_id": "test_scenario:data_write_exec",
  "type": "ygg_test_scenario",
  "recipe": "data_write_exec",
  "name": "Execution-Time Write",
  "auto_run": true
}
```

**Expected**:
- `write_doc` step calls `ctx.data.connection("test_realm_write_db").save(body, doc_id="data_access_test:write_result", mode="upsert")`
- Body is a clean dict without `_id` or `_rev` — DataAccess manages those internally
- `step.write_result` event emitted with `write_status`, `doc_id`, `old_rev`, `new_rev`
- `data_access.write.succeeded` event emitted by the DataAccess layer (visible in event spool)
- Step metrics include all four fields; `old_rev` is `null` on first create, populated on subsequent runs
- `echo_confirm` step succeeds after write

---

## Scenario 18: Write-Only Permission (Read Denied)

**Purpose**: Prove that a connection with only `"write"` in its execution permissions correctly
denies read operations. `save()` must succeed; `get()` must raise `DataAccessDeniedError`.

**Requires**: Connection `test_realm_write_db` (write-only for test_realm — same connection as Scenario 17).

**Insert as**:
```json
{
  "_id": "test_scenario:data_write_only_permission",
  "type": "ygg_test_scenario",
  "recipe": "data_write_only_permission",
  "name": "Write-Only Permission Test",
  "auto_run": true
}
```

**Expected**:
- `write_only_put` step calls `save()` → must succeed (write permission is present)
- `write_only_read_denied` step calls `get()` on the same connection → must raise `DataAccessDeniedError`
- `step_expect_read_denied` treats the `DataAccessDeniedError` as the expected outcome; emits `step.read_denied_as_expected`
- Step metrics include `read_correctly_denied: true` and the denial reason
- Plan completes successfully — both steps pass

---

## Scenario 19: Write Without _id (Auto-Generated ID)

**Purpose**: Prove that a step can write a document to CouchDB without supplying a `_id`. The
selector identity path of `save()` is used; CouchDB generates the document ID on create. The
resolved `doc_id`, `identity`, and `operation` are visible in step metrics.

**Requires**: Connection `test_realm_write_db` configured with
`data_access.realms.test_realm.execution.permissions: ["write"]`.

**Insert as**:
```json
{
  "_id": "test_scenario:data_write_no_id",
  "type": "ygg_test_scenario",
  "recipe": "data_write_no_id",
  "name": "Write Without _id",
  "auto_run": true
}
```

**Expected**:
- `write_doc_no_id` step calls `client.save(body, selector={...}, mode="upsert")` — no `_id` in body or call
- On first run: CouchDB creates a new document and returns a generated ID
- `step.write_result` event emitted with `write_status`, `doc_id`, `old_rev`, `new_rev`, `identity="selector"`, `operation="upsert"`
- `data_access.write.succeeded` event emitted by the DataAccess layer
- Step metrics include `identity="selector"` confirming the selector path was used
- `echo_confirm` step succeeds after write

---

## Scenario 20: Independent Branches, One Fails

**Purpose**: Show a failure contained to its branch. After shared validation, a metadata update and two lane branches (prepare → process → upload) run independently. This mirrors the demux realm's plan of one branch per lane.

**Insert as**:
```json
{
  "_id": "test_scenario:branch_failure",
  "type": "ygg_test_scenario",
  "recipe": "branch_failure",
  "auto_run": true
}
```

**Expected** (under `continue_independent`, the recipe's default policy):
- `validate_shared` succeeds; `update_metadata` fails, and nothing depends on it
- Lane 1 completes: `lane_1__prepare` writes `lane_1_config.txt` (a declared output), then `lane_1__process` and `lane_1__upload` succeed
- Lane 2 fails partway: `lane_2__prepare` succeeds, `lane_2__process` fails, and `lane_2__upload` is blocked (`step.blocked`, with `lane_2__process` as its direct blocker and failed ancestor)
- The attempt ends `failed` with 5 succeeded, 2 failed and 1 blocked step
- The request is finished: `executed_run_token` equals `run_token`, `status` stays `"approved"`, and `last_finalized_execution.outcome` is `"failed"`. The plan does not run again until `run_token` is raised
- After raising `run_token`, the rerun reuses every step that succeeded (`step.skipped`), unless its declared output was deleted, and runs the two failing steps again

With `"failure_policy": "fail_fast"`, the same plan stops at `update_metadata`. The lane steps are unreached, and the plan stays eligible.

---

## Scenario 21: Independent Branches, Metadata Required

**Purpose**: The same plan, with the author's other choice: the metadata update is a prerequisite of each lane branch. Only the dependencies differ.

**Insert as**:
```json
{
  "_id": "test_scenario:branch_failure_metadata_required",
  "type": "ygg_test_scenario",
  "recipe": "branch_failure_metadata_required",
  "auto_run": true
}
```

**Expected**:
- `validate_shared` succeeds; `update_metadata` fails
- Every lane step is blocked, with `update_metadata` as the failed ancestor. Lane 2's failing step is never invoked
- The attempt ends `failed` with 1 succeeded, 1 failed and 6 blocked steps, and the request is finished

---

## How to Insert Scenarios

### Via `curl` (local CouchDB):

```bash
curl -X POST http://localhost:5984/yggdrasil \
  -H "Content-Type: application/json" \
  -d '{
    "_id": "test_scenario:simple_success",
    "type": "ygg_test_scenario",
    "recipe": "happy_path",
    "name": "Simple Success",
    "auto_run": true
  }'
```

### Via Python REPL:

```python
from lib.couchdb.yggdrasil_db_manager import YggdrasilDBManager

ydm = YggdrasilDBManager()

scenario = {
    "_id": "test_scenario:simple_success",
    "type": "ygg_test_scenario",
    "recipe": "happy_path",
    "name": "Simple Success",
    "auto_run": true
}

ydm.server.post_document(db="yggdrasil", document=scenario).get_result()
print("Scenario inserted successfully")
```

### Via Python script:

```python
#!/usr/bin/env python3
import json
from lib.couchdb.yggdrasil_db_manager import YggdrasilDBManager

scenarios = [
    {
        "_id": "test_scenario:simple_success",
        "type": "ygg_test_scenario",
        "recipe": "happy_path",
        "name": "Simple Success",
        "auto_run": true
    },
    {
        "_id": "test_scenario:fail_fast",
        "type": "ygg_test_scenario",
        "recipe": "fail_fast",
        "name": "Fail Fast",
        "auto_run": true
    },
]

ydm = YggdrasilDBManager()
for scenario in scenarios:
    ydm.server.post_document(db="yggdrasil", document=scenario).get_result()
    print(f"Inserted: {scenario['_id']}")
```

---

## Observing Execution

### 1. Watch logs in real-time:

```bash
tail -f yggdrasil.log | grep -E "(TEST_SCENARIO|test_realm|step\.)"
```

### 2. Check event spool (emitted events):

```bash
# List recent events
find $YGG_EVENT_SPOOL -name "*.json" -type f -mmin -5 | sort

# Pretty-print a specific event
cat $YGG_EVENT_SPOOL/.../step_succeeded.json | jq .
```

### 3. Query plan status:

```bash
# Get plan document
curl http://localhost:5984/yggdrasil_plans/test_scenario:simple_success | jq .

# Extract key fields
curl http://localhost:5984/yggdrasil_plans/test_scenario:simple_success | \
  jq '{status, run_token, executed_run_token, realm}'
```

### 4. Monitor watchers responsiveness (during long-running):

In one terminal, start daemon:
```bash
yggdrasil daemon --dev
```

In another terminal, insert long-running scenario while it's executing:
```bash
# Monitor how quickly a new scenario is detected
while true; do curl http://localhost:5984/yggdrasil | jq '.total_rows' 2>/dev/null; sleep 1; done
```

---

## Testing Approval Workflow

For **Scenario 2 (Pending Approval)**:

1. **Insert scenario**:
```bash
curl -X POST http://localhost:5984/yggdrasil \
  -H "Content-Type: application/json" \
  -d '{"_id":"test_scenario:pending_approval","type":"ygg_test_scenario","recipe":"happy_path","auto_run":false}'
```

2. **Verify plan is drafted**:
```bash
curl http://localhost:5984/yggdrasil_plans/test_scenario:pending_approval | \
  jq '{status, run_token, executed_run_token}'
# Should show: status="draft", run_token=0, executed_run_token=-1
```

3. **Simulate approval** (increment run_token):
```bash
# Fetch current doc
PLAN=$(curl http://localhost:5984/yggdrasil_plans/test_scenario:pending_approval)

# Update with status="approved" and run_token increment
curl -X PUT http://localhost:5984/yggdrasil_plans/test_scenario:pending_approval \
  -H "Content-Type: application/json" \
  -d "$(echo $PLAN | jq '.status="approved" | .run_token=1')"
```

4. **Watch PlanWatcher execute**:
```bash
tail -f yggdrasil.log | grep "Eligible plan detected"
```

---

## Testing Retry Logic (Future Implementation)

Once retry logic is implemented, use **fail_fast** or **fail_mid_plan** scenarios:

1. Insert a failing scenario
2. Observe plan fails (executed_run_token NOT updated)
3. Verify plan remains eligible: `is_plan_eligible(plan_doc) == True`
4. Trigger retry (manual re-approval or automatic)
5. Observe re-execution

---

## Summary: Quick Reference

| Scenario | Recipe/Custom | Auto-run | Duration | Expected Result |
|----------|----------------|----------|----------|-----------------|
| Simple Success | happy_path | ✓ | ~0.5s | All steps succeed |
| Pending Approval | happy_path | ✗ | manual | Draft, waits for approval |
| Long Running (30s) | long_running | ✓ | ~30s | Non-blocking, responsive watchers |
| Long Running (5s) | long_running | ✓ | ~5s | Faster variant |
| Fail Fast | fail_fast | ✓ | <1s | Step 1 fails immediately |
| Fail Mid-Plan | fail_mid_plan | ✓ | ~0.3s | Steps 1-2 succeed, 3 fails |
| Artifact Write | artifact_write | ✓ | <1s | Files created, artifacts tracked |
| Independent Branches | branch_failure | ✓ | <1s | Lane 1 completes; lane 2 fails partway; attempt failed, request finished |
| Metadata Required | branch_failure_metadata_required | ✓ | <1s | Metadata failure blocks both lanes |
| Custom Sleep | happy_path | ✓ | ~3s | Tests parameter override |
| Quick Echo | happy_path | ✓ | <100ms | Baseline overhead |
| Random Fail (50%) | random_fail | ✓ | ~0.5s | 50% chance of failure |
| Random Fail (90%) | random_fail | ✓ | ~0.5s | 90% chance of failure |
| Random Fail (10%) | random_fail | ✓ | ~0.5s | 10% chance of failure |
| Custom Single Echo | Custom steps | ✓ | <50ms | Single echo step |
| Custom Pipeline | Custom steps | ✓ | ~2s | Multi-step with 30% fail chance |
| Isolated Random | Custom steps | ✓ | <50ms | Pure random 50/50 outcome |
| Parallel Chaos | Custom steps | ✓ | <1s | 3 parallel random (34% all succeed) |
| Artifact with Chaos | Custom steps | ✓ | <1s | Artifact + 50% failure |
| Metadata Harvest | metadata_harvest | ✓ | <50ms | Domain fields baked as structured dict in plan params |
| Plan-Time Fetch (Structured) | data_fetch_plan | ✓ | <1s | CouchDB ref doc baked as structured dict in plan params |
| Execution-Time Write | data_write_exec | ✓ | <1s | Write succeeds; write_status + revs in step metrics |
| Write-Only Permission | data_write_only_permission | ✓ | <1s | save() passes, get() correctly denied |
| Write Without _id | data_write_no_id | ✓ | <1s | CouchDB generates ID; identity="selector" in metrics |

---

## Troubleshooting

**Plan not created after inserting scenario:**
- Check ScenarioDocWatcher is running: `tail -f yggdrasil.log | grep ScenarioDocWatcher`
- Verify `_id` field is set (must be unique)
- Verify `type="ygg_test_scenario"` 
- Verify EITHER `recipe` field OR `steps` array exists
- Check if plan was created with different ID: `curl http://localhost:5984/yggdrasil_plans/_all_docs | jq '.rows[] | select(.id | contains("test_scenario"))'`

**Plan created but not executing:**
- Check plan status: `curl http://localhost:5984/yggdrasil_plans/<plan_id> | jq '.status'`
- If `status="draft"`, manually approve (increment `run_token`)
- Check PlanWatcher is running: `tail -f yggdrasil.log | grep PlanWatcher`

**Step failures with missing fn_ref:**
- For standard recipes, ensure the recipe exists in `lib/realms/test_realm/recipes.py`.
- For planning-time handler scenarios such as `data_fetch_plan` or `metadata_harvest`, ensure the handler special-case exists in `lib/realms/test_realm/handler.py` — these are intentionally not in the RECIPES registry.
- Verify step function exists in `lib/realms/test_realm/steps.py`
- Check error message for typos in override field names or fn_name

**Custom steps not working:**
- Verify `steps` is an array of dicts
- Each step must have `step_id` and `fn_name`
- Valid `fn_name` values: `step_echo`, `step_sleep`, `step_fail`, `step_random_fail`, `step_write_file`, `step_fetch_from_db`, `step_expect_denied`, `step_write_to_db`, `step_expect_read_denied`, `step_exercise_all_fetch_methods`, `step_verify_limit_clamping`, `step_emit_metadata`
- Check deps refer to existing step_id values

For broader troubleshooting (CouchDB connectivity, config errors, realm discovery, DataAccess), see [troubleshooting.md](troubleshooting.md).
