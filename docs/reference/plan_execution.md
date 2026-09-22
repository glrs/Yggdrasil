# Plan Execution

What happens between an approved plan and its recorded result, and how to read and steer that as an operator. For how the engine runs the steps themselves, see the [Flow API](../flow_api/overview.md#engine-yggdrasilcoreengine).

---

## The plan document

Each persisted plan is one document in the plan store (`yggdrasil_plans` on CouchDB, or the dev SQLite file). The fields that govern execution:

| Field | Meaning |
|---|---|
| `status` | Approval only: `"draft"` or `"approved"`. Never changed by executing the plan |
| `run_token` | The latest execution request. Raise it to request another run |
| `executed_run_token` | The latest request that was *finished*. Starts at `-1` |
| `plan_generation` | Opaque ID of this planned version. Kept by approval and token changes, replaced when the plan is regenerated |
| `execution_authority` / `execution_owner` | Who may execute the plan: `"daemon"`, or one `"run_once"` session |
| `plan.failure_policy` | `"fail_fast"` or `"continue_independent"` (see [Failure policies](../flow_api/overview.md#failure-policies)) |
| `last_finalized_execution` | The result of the latest finished request: `execution_id`, `plan_generation`, `run_token`, `failure_policy`, `outcome`, `termination_reason`, `finalized_at`, and the full attempt `report` |

A plan is **eligible** to run when `status == "approved"` and `run_token > executed_run_token`. The caller must also hold its execution authority: the daemon runs `"daemon"` plans, and a `run-doc --run-once` session runs only the plans it created.

When a plan becomes eligible, the executing process reads the plan document afresh. The plan, its policy, generation, run token and authority all come from that one read, never from the change event that prompted it. Within one process, a plan has at most one attempt in flight. A request that arrives while it runs is checked again once that attempt is done.

---

## Approval status is not an execution outcome

`status` says only whether a plan may run. It stays `"approved"` after a run, however the run went. The outcome of a finished request is recorded in `last_finalized_execution.outcome`, which is `"succeeded"` or `"failed"`.

The plan summary's `last_finalized_outcome` reflects `last_finalized_execution`. The ops snapshot reflects something else: the newest observed attempt, which may be newer than the last finalized request, or not finalized at all. For example, if request 0 succeeds and request 1 then fails under `fail_fast`, the snapshot shows request 1's failure, while `last_finalized_execution` still records request 0's success. To relate the two, compare their `execution_id`, `plan_generation` and `run_token`.

`executed_run_token == run_token` means the latest request is **finished**, not that it succeeded. A `continue_independent` plan finishes its request even when steps failed, so it can have equal tokens and a failed outcome. To tell whether the latest request succeeded, check all three:

```text
executed_run_token == run_token
last_finalized_execution.run_token == run_token
last_finalized_execution.outcome == "succeeded"
```

---

## Which endings finish a request

Every attempt closes a report when it ends, but only some endings finish the request, recording its result and its token together:

| How the attempt ended | `fail_fast` | `continue_independent` |
|---|---|---|
| Every step succeeded or was reused | Finished, `succeeded` | Finished, `succeeded` |
| Every step that could run ran, with some failed or blocked | Not possible: the attempt stops at the first failure | Finished, `failed` |
| A step failed and the attempt stopped (`failed_fast`) | **Not finished**: stays eligible | Not possible |
| Rejected by preflight: invalid graph, malformed output declarations, or a step reference that is malformed or names a missing module or function | **Not finished**: stays eligible | Finished, `failed`: rerunning an unchanged plan cannot help |
| Cancelled (daemon shutdown, Ctrl+C) before every step had run | Not finished: stays eligible | Not finished: stays eligible |
| Aborted by an infrastructure failure, such as the event spool being unwritable, or a step module that exists but fails to import | Not finished: stays eligible | Not finished: stays eligible |

A plan whose `failure_policy` is unknown is rejected by preflight too, but belongs to neither column: it is not a valid `continue_independent` request, so its request stays eligible, like a rejected `fail_fast` one.

A request that is not finished stays eligible. Nothing retries it on a timer: the daemon runs it again the next time it observes a change to the plan document. A `fail_fast` plan therefore keeps its established behavior: after a failure, it runs again on its next change.

A `continue_independent` plan whose request finished with failures does **not** run again by itself, and approving it again changes nothing. It needs a new request.

Once an attempt has run every step it could, its result is recorded even if the caller then shuts down. Stopping the daemon at that point does not throw the finished result away.

---

## Requesting a rerun

To run a plan again, raise `run_token` by one and leave `status` as `"approved"`. This is the only way to rerun a finished `continue_independent` request, and it works for any plan. Write the change conditionally on the document's revision (`_rev`), as an approval actor does: if the plan changed since it was read, reread it and decide again. On the dev SQLite backend, the write must also advance the plan-change sequence in the same transaction, or PlanWatcher never observes it (see **Manual plan approval** in [Configuration](../getting_started/configuration.md)).

The new attempt runs in the same work directories as the previous one:

- A step whose earlier success is still valid is **reused** (`step.skipped`): its fingerprint is unchanged and its declared outputs exist.
- A step that failed runs again. So does a step that was blocked, once its prerequisites succeed. If they fail again, it is blocked again.
- To force a successful step to run again, remove its `success.fingerprint` from `<work_root>/<plan_id>/<step_id>/`, or one of its declared outputs.

Regenerating the plan, when a realm handler saves a new version of it, starts a new `plan_generation` with its tokens reset. That new version is a new request, and it runs as such. A result still in flight from the old version is never recorded onto the new one.

---

## When a result cannot be recorded

A finished request is recorded by a conditional write that commits the outcome and the executed token together. It refuses to overwrite a newer generation, a newer result or a changed authority. A write that loses a race or hits a transient storage error is tried again, up to three tries in all, 0.5 s and then 1 s apart. Those retries only record the result. A failed step is never retried.

If the result still cannot be recorded, after three failed tries or one non-retryable storage error, it is kept in memory as a **pending finalization**. What that looks like:

- The log reports `Plan '<plan_id>' execution '<execution_id>' ... finished, but its result could not be recorded; keeping it pending, and not executing the plan again until it is recorded or superseded`.
- The ops snapshot shows the attempt as `finished`, but the plan document still has the previous `executed_run_token`, and no `last_finalized_execution` for that `run_token`.
- `run-doc --run-once` exits with code 1.
- Other plans keep running.

While the result is pending, this process does not execute the plan again, and further change events for it retry nothing. To recover once the plan store is healthy again, raise `run_token`. The process then tries once more to record the pending result, with the same bounded tries, and runs the new request only once the old result is recorded or definitively refused. If recording fails again, the new request waits too: raise `run_token` again to try again. Each newer `run_token` buys one round of tries. A pending result is also dropped, without anything being written, when the plan is regenerated, its authority changes, or its result turns out to have been recorded after all.

A pending result lives only in memory. If the process restarts, it is lost, and the old request is still eligible, so it may run again. Realm steps must therefore tolerate running again over their own earlier work.

If recording is refused for good, because the plan was regenerated, deleted or reassigned while it ran, the result is dropped (`superseded`). Run-once exits with code 1.

---

## Operational snapshots (`plan_status`)

The ops consumer reads the event spool periodically in daemon mode, and once when `run-doc --run-once` exits. It writes one `plan_status` snapshot per plan to the operations store (`yggdrasil_ops` on CouchDB, or the dev SQLite file).

A snapshot shows **one attempt**: the one whose execution ID orders highest, finished or not. That is normally the most recently admitted attempt, and replaying an old attempt's events cannot change the choice. How far that ordering can be relied on, across restarts, clocks and concurrent processes, is described in [Execution IDs and attempt order](../flow_api/overview.md#execution-ids-and-attempt-order). Every step is shown as that attempt left it. A step never takes an earlier attempt's state.

An attempt shows as `running` until its report is published. An attempt that ended without publishing one, because event publication failed or the process was killed, keeps showing as `running` until a newer attempt is admitted. It did not finish its request, so the plan document does not record it. See [Attempt reports](../flow_api/overview.md#attempt-reports).

| Field | Content |
|---|---|
| `type`, `realm`, `plan_id`, `scope`, `updated_at` | Identity of the plan and time of the snapshot |
| `projection` | `"attempt"`, or `"legacy"` for a plan whose spool has no attempt records (events published before attempts were recorded). A legacy projection shows each step's latest run, which need not belong to one attempt |
| `attempt` | The attempt shown: `execution_id`, the `plan_generation` and `run_token` it captured, `failure_policy`, `execution_authority`, `execution_owner`, `started_at`, and `state` (`"running"` or `"finished"`). Once finished: `ended_at`, `termination_reason`, `outcome`, `is_drained`, `counts` per outcome plus `unreached` (every step without an outcome, interrupted ones included), and any attempt-level `diagnostic`, such as a preflight rejection. `null` for a legacy projection |
| `steps` | One entry per planned step, keyed by step ID: `step_name`, `state`, `outcome`, `run_id`, `fingerprint`, `progress`, `artifacts`, `metrics`, `job`, `ts`, `error` for a failed step, and `direct_blockers` and `failed_ancestors` for a blocked one |

A step's `state` is the event type that records its outcome: `step.succeeded`, `step.skipped` (reused), `step.failed` or `step.blocked`. While it runs, it is its latest lifecycle event. A step with no outcome is `pending` while the attempt runs. Once the attempt has ended, it is `interrupted` if it had started and `unreached` if it never did.

The snapshot's `plan_generation` is the one the attempt captured. It is not necessarily the stored plan's current generation, which only the plan document holds.

---

## See also

- [Flow API](../flow_api/overview.md) — scheduling, failure policies, declared outputs and events
- [CLI](../getting_started/cli.md) — `run-doc --run-once` exit codes
- [Troubleshooting](troubleshooting.md) — plans that do not run, or do not run again
