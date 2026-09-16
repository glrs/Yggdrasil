# Yggdrasil: Independent Branch Execution for Concrete Plans

**Status:** draft PRD for review; no implementation authorized by this document  
**Date:** 2026-09-14  
**Source baseline:** Yggdrasil `dev`, `250e9e17e8b29cfc8db40268aba8db6e2287308e`  
**Companion:** [Demux single-flowcell plan PRD](demux_realm_single_flowcell_plan_prd.md)

**Implementation-agent review:** targeted clarifications and decisions are recorded in [the review response](independent_branch_execution_agent_review.md); [the saved original](yggdrasil_independent_branch_execution_prd.before_agent_review.md) and [unified diff](yggdrasil_independent_branch_execution_prd.agent_review.diff) track this revision.

## 1. Problem and intended outcome

The demux realm already knows its lane/settings combinations during planning. It can express them as branches of one ordinary, concrete Yggdrasil plan. The current engine, however, stops the entire plan on the first step exception. Consolidating today's independent lane plans would therefore prevent healthy lanes from continuing after another lane fails.

This introduces Yggdrasil's first dependency-driven execution scheduler: today `deps` does not order or gate execution. Treat this as a critical engine change with explicit invariants and independently reviewed tests, not a small exception-handling adjustment to an existing scheduler.

This feature lets a realm opt into executing all independent work that remains possible in a fixed graph. A failed step blocks steps that require its success. Other runnable steps continue. The final execution outcome retains every failure and blocked step; finishing healthy branches does not make the plan successful.

Example: lane 2 demultiplexing fails; lane 2 collection and upload are blocked; lane 3 completes; the execution finishes with a failed outcome and an accurate breakdown.

## 2. Decision status and ownership

### Agreed requirements

- Unaffected lanes must continue in this iteration.
- The engine must use explicit dependencies to determine which work is affected.
- Whether a step is a prerequisite is the realm author's design choice. There is no core exception for metadata updates, demultiplexing, or particular step names.
- The immediate demux graph is fully known before execution. Runtime expansion and execution-time resolvers are unnecessary for this delivery.
- Retain artifact-based data handoffs and caching. Required artifact resources must exist before accepting reuse.
- Do not represent failed or blocked work as successful.

### Proposed implementation decisions

This PRD recommends a persisted, opt-in `failure_policy="continue_independent"`; omission retains `"fail_fast"`. Sequential execution remains sufficient. It also proposes a structured execution result, terminal-request bookkeeping, and explicit required-output declarations. These mechanisms are recommendations for implementing the agreed behavior, not additional decisions already made by the user.

The implementation-agent review resolves policy placement, the shared scheduler, error boundaries, SQLite CAS scope, generation initialization, and bounded retries below. These are technical design decisions made under the user's request to resolve the agent's questions; they do not change the agreed workflow behavior.

Yggdrasil owns graph validation, scheduling, outcomes, reuse checks, reporting, execution eligibility, and storage parity. The realm owns grouping inputs, constructing branches and dependencies, selecting the policy, and declaring artifacts. See the companion PRD for that work.

## 3. Verified baseline

The local project folder is a discussion/document workspace. Findings below come from the pinned GitHub source, documentation, and inspected tests; tests were not executed for this PRD.

- `Plan` contains concrete `StepSpec`s with parameters, dependency IDs, scope, and input paths. It has no failure-policy field. [Model][model]
- `Engine.run` walks the supplied list. Its topology check detects unknown dependency IDs but does not reject cycles or duplicate identities or enforce dependency order. An existing test deliberately accepts forward references. [Engine][engine], [tests][tests]
- Cache matching currently checks `success.fingerprint` without producer-output availability validation. The current step work directory is `<work_root>/<plan_id>/<step_id>`, reused across attempts. [Engine][engine]
- The step wrapper emits success/failure events; transient exceptions currently have no automatic retry implementation. [Step wrapper][step]
- Daemon and run-once callers currently interpret a normal engine return as success and update `executed_run_token`. A continuation implementation cannot simply catch errors and return normally. [Core callers][core]
- Eligibility is `status == "approved"` and `run_token > executed_run_token`, subject to execution authority/ownership. An unchanged token leaves a failed request eligible; this does not mean a periodic retry service is already implemented. [Eligibility][eligibility]
- Regenerating a plan resets execution tokens. Existing finalization fetches the current document and writes the completed token without checking that it is still the generation that executed. [Document construction][documents], [CouchDB store][couch]
- The spool consumer selects a latest run independently for each step and takes fields from its last event. It has no authoritative aggregate execution result or dependency-blocking explanation. [Consumer][consumer]
- `PlanBuilder` computes output paths, but does not retain that output map in `StepSpec`. [Builder][builder]

## 4. Scope

### Included

1. Explicit continuation policy for concrete plans, with legacy fail-fast compatibility.
2. Graph validation and deterministic dependency-aware scheduling.
3. Per-step failure containment and transitive blocking.
4. Accurate terminal execution result, events, snapshots, and CLI behavior.
5. Coherent completion and rerun bookkeeping across CouchDB and SQLite, explicitly including Tech Debt #18's conditional-write gap for plan documents.
6. Minimal declared-output availability checks for reusable artifact-producing steps.
7. Tests and documentation covering both policies and all engine entry points.

### Excluded

- Runtime map expansion, LogicalPlan/MaterializedPlan schemas, and resolver APIs.
- A generic author-facing map DSL; demux can expand known inputs in its recipe.
- Parallel execution, distributed scheduling/leases, and exactly-once side-effect guarantees.
- Automatic step-execution retries, failure-conditioned execution, cleanup/finally tasks, and `collect`/`outcome` APIs.
- General persistence/restoration of Python return values, a cache database, deep directory hashing, retention management, and migration of old caches.
- Changes to demux business rules or implementation of real Nextflow execution.

## 5. Plan and dependency contract

### Failure policy

Add and round-trip a plan-level policy:

`failure_policy` belongs directly on `Plan`, alongside its existing fields, and is serialized within the persisted document's `plan` object. It does not belong on `StepSpec`, `PlanDraft`, or a duplicated top-level control field. `PlanDraft` retains its approval responsibilities.

| Value | Meaning |
|---|---|
| `fail_fast` | Default for omitted legacy fields. Stop on the first execution failure, preserving the existing fail-fast exception behavior. |
| `continue_independent` | Record ordinary step failures, block their dependents, and continue unrelated runnable work. |

Reject an unknown explicitly supplied policy. Do not silently downgrade a requested continuation plan to fail-fast. The companion realm must require a compatible Yggdrasil version before emitting this field.

All `StepSpec.deps` in this iteration are success prerequisites: every named predecessor must have succeeded or been validly reused in this execution attempt. Multiple prerequisites mean all are required. A list position, shared scope, or naming prefix does not create a dependency.

An author makes a metadata step mandatory by including it in the relevant dependency paths. Without that relationship, its failure does not block those branches. The engine never infers this rule from domain names.

### Preflight

Before invoking any step:

- Validate unique, nonempty step IDs and known dependencies.
- Reject self-dependencies and cycles with actionable diagnostics.
- Validate the selected policy and required execution metadata.
- Resolve/check callable registration and basic binding errors where possible without invoking step bodies.

Forward references remain supported. The engine schedules a valid graph rather than requiring the serialized list to be topologically sorted. Tightened graph correctness applies to both policies; correctly ordered legacy plans retain their existing order.

## 6. Scheduling and failure handling

Use one sequential dependency scheduler for both policies. Build and validate the dependency graph once; a stable topological traversal using original plan position as the ready-node priority is sufficient. Visit that ordering through one per-step execute/reuse path. The policy changes the response to an ordinary failure, not graph readiness or cache rules. Do not retain a second list-order legacy engine or introduce a polling scheduler service.

Maintain execution-local outcomes for each concrete step. At each scheduling decision:

1. Mark a pending step blocked when any required predecessor has failed or is blocked. Retain direct blockers and originating failed-step IDs.
2. A step is runnable only when all its required predecessors have succeeded or been validly reused.
3. Choose among runnable steps using their original plan-list order as a stable tie-breaker.
4. Only after dependency readiness is established, evaluate cache reuse or execute the step.
5. Under continuation policy, record an ordinary step failure and repeat until no work remains runnable or unresolved.

An old matching fingerprint on a descendant must never override a failed prerequisite in the current attempt. A blocked step is neither called nor accepted as a cache hit.

Required invariants: graph validation precedes any step invocation, output overwrite, or cache-marker mutation; each step is invoked or reused at most once per attempt; every predecessor is successfully satisfied before its successor's cache is read; attempt state is local to that execution, not mutable state on a shared `Engine` instance. Pending work with no possible progress in a validated graph is an engine invariant failure, not clean completion. At final aggregation, blocker diagnostics must include all failed ancestors, including ones that failed after a join was first marked blocked.

Ordinary permanent, transient-without-retry, and unexpected `Exception`s from author work are failures of that step when reliable orchestration remains available. Missing required output after an otherwise successful body is also a step failure. Preserve their diagnostics and do not automatically retry. Where the wrapper has already emitted `step.failed`, avoid a duplicate competing terminal failure.

Exception location alone cannot classify failures: the decorated `fn(ctx, ...)` also performs event publication, and author calls such as `ctx.progress()` and `ctx.record_artifact()` invoke infrastructure. Wrap/report core infrastructure failures at their boundary using a distinct orchestration-error type or equivalent explicit classification, and propagate that before generic step-failure handling. Emitter failure at started/progress/artifact/success/failure publication, scheduler invariant failure, cache-marker bookkeeping failure, and terminal-store failure abort the attempt. A realm's own file or external DataAccess error is not automatically systemic merely because it is an `OSError` or database exception. If publishing a step failure also fails, preserve both causes; do not hide the original error or continue unobservably.

Cancellation, process interruption, or an orchestration/storage/reporting failure that prevents reliable execution tracking may stop the entire attempt. Do not treat these as harmless lane errors and continue unobservably. A crash or interrupted attempt is not a completed successful execution.

Do not contain cancellation/control-flow signals as ordinary step errors. Cancelling an async caller awaiting a threaded step does not prove that the worker stopped; retain execution exclusion until the worker actually exits, and stop scheduling further steps. Do not promise forceful interruption of arbitrary author code.

The graph remains fixed throughout the attempt. No runtime discovery or partial expansion is needed.

## 7. Execution result and reporting

### Result contract

For continuation executions, return a structured result after independent work has drained. Suggested fields are execution ID, plan ID/generation, captured run token when applicable, policy, outcome, timestamps, per-step outcomes, and failure/blocker details.

| Step outcome | Meaning | Satisfies a success dependency? |
|---|---|---|
| `succeeded` | Executed and completed successfully. | Yes |
| `reused` | Runnable step accepted through validated historical reuse. | Yes |
| `failed` | Execution was attempted and failed. | No |
| `blocked` | Not invoked because a required predecessor failed or was blocked. | No |

Pending/running states may exist while executing. Interruption and fail-fast termination must distinguish work not reached from dependency-blocked work. Do not label every unstarted step blocked without an actual failed prerequisite.

A normally completed continuation result is `succeeded` only when every included step succeeded or was reused; otherwise it is `failed`. Counts of successful branches can accompany failure, but do not introduce a misleading successful overall outcome. Invalid preflight graphs also produce an observable unsuccessful execution request without invoking steps.

Preserve legacy fail-fast behavior for external callers. Internal daemon, run-once, direct execution, and recovery paths must all interpret the new continuation result explicitly. Run-once finishes healthy work and then exits nonzero for a failed execution. A returned result is not inherently a successful result.

Use the least-breaking public return contract: fail-fast success continues to return `None`; continuation returns the structured result. Internal callers validate the return expected for the selected policy; `None` under continuation is a contract error. Fail-fast preserves current failure propagation, including the existing conversion of `TransientStepError` to a retry-unimplemented `PermanentStepError`; it does not necessarily re-raise the original exception unchanged. Both policies use the same internal step outcomes and scheduler.

Provide an explicit internal execution-context/result path for callers to finalize successful fail-fast attempts while preserving that public return convention. Do not recover outcomes through shared mutable state such as `Engine.last_result`.

### Events and snapshots

- Correlate events with one execution ID plus the captured plan generation/token. Existing per-step run IDs alone are insufficient to select a coherent plan attempt.
- Emit explicit blocking diagnostics and a plan-level terminal result. Existing cache-skip events may remain, with an unambiguous cache-hit reason; they are different from blocked events.
- Supply step names/IDs, errors, direct blockers, original failed ancestors, outcome counts, and enough plan information to show unstarted work.
- Build the latest operational snapshot from one selected execution, not a mixture of independently selected historical step runs.
- Do not let a later progress/artifact event, delayed event delivery, or replay erase a known terminal failure. Correlate/deduplicate by event identity and use defined terminal ordering.
- Preserve old event readability. Upgrade the consumer's handling of plan-level events and scope discovery rather than assuming its current directory traversal already supports the new layout.

Approval `status` stays `draft`/`approved`. Execution outcome is separate metadata, including in `yggdrasil_ops` and plan summaries. No new approval status is required.

## 8. Completion, reruns, and concurrency boundaries

### Proposed terminal-request semantics

For `continue_independent` plans, a fully drained attempt consumes its captured execution request even if its outcome is failed. A definitive preflight rejection of such a request is also terminal: persist a failed result with validation diagnostics and consume the captured request without invoking steps. This differs from an interrupted/systemically incomplete attempt. Persist the terminal outcome together with the corresponding executed token. A subsequent intentional rerun increments `run_token`; ordinary reporting changes must not restart that finished failed request.

This deliberately extends the meaning of `executed_run_token` for continuation plans: it identifies a completed request, not proof that the request succeeded. Preserve legacy fail-fast failure eligibility initially. Update documentation and callers so neither token equality nor an approved status is used as a success indicator.

Extend the internal `PlanStore` interface with a conditional, idempotent terminal-finalization operation. Implement equivalent behavior in production CouchDB and development/testing SQLite. Do not write orchestration state through realm DataAccess.

Use this generation-safe finalizer for successful executions under both policies. Only continuation changes completed-failure token consumption; legacy fail-fast failure eligibility remains as specified. Leaving successful fail-fast callers on a generation-blind update would retain the stale-worker race.

### SQLite conditional writes — Tech Debt #18 is in scope

Add an expected-revision condition to `SQLiteInternalStore.put_document`, checked in the same transaction as the write. Distinguish conditional creation of an absent document from unconditional replacement. A rejected conflict must change neither document body/revision nor plan-change sequence. `BEGIN IMMEDIATE` around the write alone does not protect an earlier Python-level read. [SQLite store][sqlite]

Thread conditional writes through every supported plan-body mutation: `save_plan`, retained token-update interfaces, generation initialization, finalization, and supported approval/rerun integrations. A stale alternate writer must not overwrite a correctly finalized result. `touch_document()` remains the already-atomic metadata-only operation; checkpoints and operational snapshots need no redesign here. Regeneration conflicts may surface as they do on CouchDB rather than automatically replaying a stale replacement. Any retry must refetch and reapply the intended field-level operation. Update the debt record only after these paths and race tests pass.

### Generation and race handling

Use a fresh opaque `plan_generation` ID, such as a UUID, on each new plan or regeneration; metadata updates and run-token increments preserve it. It is separate from document revision and per-attempt execution ID. For a legacy document missing the field, initialize it once using CAS under the in-flight guard, preserving the plan, approval, tokens, and authority. Refetch after any conflict and capture the authoritative document before invoking steps. A missing field is a migration condition, not a permanent generation `0`; no bulk migration is required. Deleted/recreated plans must not reuse the old generation. Unsupported writers that strip generation metadata must not silently preserve execution identity.

Finalization must:

- Apply only to the captured plan generation and permitted execution authority/owner.
- Record the captured request's result without overwriting a newer request's result.
- Preserve a newer `run_token`; never lower an already recorded executed token.
- Refuse to finalize a regenerated plan on behalf of an old worker.
- Commit outcome and token together, using the backend's conditional update/transaction mechanism.
- Report persistence failure as an orchestration error, not a clean success or successful CLI exit.

Use a process-local per-plan in-flight guard across tokens/generations to prevent repeated eligible events from launching overlapping work in the same directories. Under that guard, fetch one current plan document and derive eligibility, the executable plan, policy, generation, token, and authority/owner from that same snapshot. Do not combine a stale watcher event's token or approval with a separately fetched newer plan. Reconcile a newer pending request after the active one drains; do not simply drop its event. Keep authority/ownership checks intact. Distributed leases and cross-host exactly-once execution remain outside scope.

Finalization uses at most three total attempts per bounded cycle, with short nonblocking backoff (default 0.5 then 1 second). Use finite backend call timeouts and keep blocking storage I/O off the event loop. Do not multiply nested store/caller retry counts. Retry revision conflicts and transient storage errors only after refetching and rechecking generation, token, execution ID, and authority, then reapplying the intended completion fields. An already committed identical execution is idempotent success; definite supersession is not a retryable conflict and must not overwrite the newer result.

After retry exhaustion, retain the result and an in-memory finalization-pending guard, surface an orchestration error/nonzero CLI result, and allow unrelated plans to continue. Duplicate events must not rerun the engine or start unlimited retry cycles. An explicit retry/recovery request may retry finalization first; no immortal retry task or new general retry service is required. On definite supersession, reconcile the newer request under the guard instead of attaching the old result to it. Crash-safe recovery of the retained result is not promised; interrupted work may be repeated later and realm idempotency remains necessary.

While finalization is pending, permit read-only reconciliation of a newer generation or authority change. A higher `run_token` within the same generation does not itself supersede the old result: finalize the old captured request while preserving the newer pending request before starting that work.

Plan regeneration currently resets tokens and remains an explicit new generation. This PRD does not claim to deduplicate semantically identical planning events across restarts.

## 9. Minimal artifact-aware reuse

Keep existing per-plan/per-step cache locations and fingerprinting. Do not disable historical reuse globally or require restoring arbitrary return values.

Proposed small model extension: `StepSpec.outputs: dict[str, str]`, defaulting to an empty mapping, describes required output artifact paths. Preserve the output map already computed by `PlanBuilder`, normalized to absolute paths after resolving its base. Declarations must reflect effective injected or explicitly overridden I/O parameters, not a different default annotation path. For manually constructed specs, allow absolute paths or explicitly producer-workdir-relative paths; only the latter are resolved against that step's work directory. Do not prefix a builder-resolved path with the step directory a second time. Existing input-path semantics remain unchanged.

For steps declaring outputs:

1. Check current dependency readiness first.
2. Require both a matching fingerprint and availability of every declared required output before reuse.
3. Include the declared output contract in the fingerprint for those specs so changing it invalidates reuse.
4. Treat a deleted output as a cache miss and execute its producer. Diagnose permission/storage errors separately from ordinary absence.
5. Before a non-reused execution can overwrite outputs, invalidate its previous success marker. A failed rerun must not leave old reusable success pointing at partially replaced data.
6. Verify required outputs before publishing step success and a new success marker; use atomic marker replacement. Integrate this check with the wrapper/engine finalization boundary so an output-validation failure is not first reported as success.

Document one owner and ordering for execution success publication and marker creation. Do not admit a successor until required-output checks and the chosen successful finalization sequence complete. Failure to publish a step's success aborts the attempt and must not create a new reuse marker for that step. One permitted sequence is required-output validation, step-success publication, atomic marker replacement, then admission of successors. Marker failure still aborts the attempt even if the step-success event was published. A later plan-finalization failure does not invalidate markers from steps already finalized successfully. Atomic marker replacement is a same-filesystem temporary-file replacement; it is not an atomic transaction spanning the event spool, artifacts, and database, nor a promise of rollback after filesystem failure. Do not build a general transaction utility for this feature.

A directory's existence does not establish completeness. A realm may declare required files or a completion sentinel within it; this feature does not add deep content validation. Legacy specs without output declarations retain their existing reuse limitations. New reusable artifact producers in the companion realm must declare their required outputs. Artifact-free validation steps need no artificial output file.

Typed result serialization, caches shared across plans, implementation/environment fingerprint redesign, and retention remain future work.

## 10. Implementation areas and documentation

| Area | Responsibility |
|---|---|
| `flow/model.py`, planner builder | Policy/output declarations, serialization compatibility, preserve known output paths. |
| `core/engine.py`, `flow/step.py` | Preflight, scheduler, outcome handling, readiness-before-cache, required-output finalization. |
| `core_utils/yggdrasil_core.py`, CLI/session integration | Interpret execution results, correct exits/logging, in-flight guard, conditional finalization. |
| `storage/protocols.py`, `plan_documents.py`, CouchDB/SQLite stores | SQLite CAS for supported plan mutations, safe generation initialization, result/token atomicity, equivalent backend behavior. |
| `flow/events/emitter.py`, ops consumer/sinks | Execution correlation, blocked/terminal events, coherent snapshots and summaries. |
| Eligibility/watcher integration | Finished failed requests do not relaunch; newer requests are preserved and reconciled. |

Update Flow API, realm-authoring, CLI, and approval/execution documentation. Correct the current implication that merely supplying `deps` already enforces dependency order. Clarify work-directory reuse and the distinction between approval status, terminal execution, and success.

The existing dynamic-planning recap remains a future design record. The earlier caching pre-PRD's blanket-disable proposal was superseded in the discussion and is not a requirement of this PRD. Neither older document was edited when this document was created.

## 11. Acceptance criteria

Use isolated temporary files and mocked external services. Cover both policies and both internal-storage backends.

1. Branch 2 fails; its descendants are blocked; branch 3 completes; final outcome is failed.
2. A shared prerequisite fails; all dependent branches are blocked; an unrelated root still runs.
3. A join with one failed prerequisite is blocked even when its other prerequisites succeeded.
4. An old matching cache marker on a blocked descendant does not allow execution or reuse.
5. Valid reuse satisfies dependencies; deleting a declared output causes its producer to run; a failed rerun leaves no reusable success marker.
6. Forward references execute correctly; ready-node tie-breaking is deterministic.
7. Duplicate IDs, unknown dependencies, self-dependencies, and cycles are rejected before any step invocation; a safely recorded preflight rejection terminally fails the captured continuation request rather than leaving it repeatedly eligible.
8. Transient and unexpected ordinary step exceptions preserve diagnostics while healthy branches continue; cancellation/systemic failure stops the attempt distinctly.
9. Omitted policy round-trips as fail-fast; unknown policy rejects; legacy stop/raise behavior remains.
10. Daemon and run-once consume the result correctly. Failed continuation finishes healthy work, reports failure, and run-once exits nonzero.
11. Finished failed continuation requests become ineligible until an explicit new request/generation; outcome and token cannot disagree after a successful commit.
12. Duplicate events do not run the same plan concurrently in one process. A newer pending token is not lost.
13. A stale eligible event after regeneration or an approval/ownership change cannot execute an inconsistent or unauthorized plan snapshot. Finalization preserves newer tokens and rejects stale generations/authority; backend failure never reports clean completion.
14. Snapshot replay includes all current-attempt failures/blockers and does not import an old successful step state into a newer failed attempt.
15. Yggdrasil's own `test_realm` or test-only realm steps exercise the companion graph's branch-failure and author-selected prerequisite scenarios through real callers, storage, and events. Do not add a runtime dependency on `demux_realm` to core. Actual demux acceptance runs in that repository against the compatible core version as a cross-repository release check.
16. Parameterized chain, fan-out, diamond/join, independent-root, out-of-order, empty, and all-reused graphs verify deterministic order and at-most-once invocation under both policies. Include multiple independent failures converging on a join and concurrent distinct plans using one Engine instance.
17. Inject failure at each event-publication boundary and marker publication: no subsequent step runs, infrastructure failure is not drained as an ordinary failed branch, and the original cause remains visible. Missing required output produces one failed terminal step event with no success event/marker. Cancellation retains exclusion until actual worker completion.
18. SQLite stale writes and conditional-create races cannot change body, revision, or sequence on conflict. Test rerun-token/finalization races, legacy initialization/regeneration races, and idempotent replay after an uncertain commit. Retry exhaustion plus duplicate events never re-executes completed work locally; another plan still progresses.

Implementation is complete only after this contract works through the real engine callers and event/storage paths, not merely in a scheduler unit test.

## 12. Review points

The implementation plan must define result/error/publication contracts and characterization tests before changing failure-catching behavior. Review graph scheduling and storage finalization as separate risk areas. Implement SQLite CAS before claiming backend parity. Introduce event correlation before relying on it in snapshots, and integrate caller interpretation with completion/eligibility handling before enabling continuation in real entry points. Staged changes are acceptable; deploying a continuation engine into an unchanged success-assuming caller is not.

Each engine change must state its invariant, why existing behavior is insufficient, compatibility impact, and its failure-path tests. Run focused tests per phase and the full existing suite before release; retain independent review of scheduler traces and storage race tests. A green baseline alone does not prove the new engine guarantees. The result schema's internal names may be refined while preserving the decisions above. Keep the local generation/in-flight solution bounded; distributed scheduling and general resume remain deferred.

[model]: https://github.com/NationalGenomicsInfrastructure/Yggdrasil/blob/250e9e17e8b29cfc8db40268aba8db6e2287308e/yggdrasil/flow/model.py
[engine]: https://github.com/NationalGenomicsInfrastructure/Yggdrasil/blob/250e9e17e8b29cfc8db40268aba8db6e2287308e/yggdrasil/core/engine.py
[tests]: https://github.com/NationalGenomicsInfrastructure/Yggdrasil/blob/250e9e17e8b29cfc8db40268aba8db6e2287308e/tests/test_core_engine.py
[step]: https://github.com/NationalGenomicsInfrastructure/Yggdrasil/blob/250e9e17e8b29cfc8db40268aba8db6e2287308e/yggdrasil/flow/step.py
[core]: https://github.com/NationalGenomicsInfrastructure/Yggdrasil/blob/250e9e17e8b29cfc8db40268aba8db6e2287308e/lib/core_utils/yggdrasil_core.py
[eligibility]: https://github.com/NationalGenomicsInfrastructure/Yggdrasil/blob/250e9e17e8b29cfc8db40268aba8db6e2287308e/lib/core_utils/plan_eligibility.py
[documents]: https://github.com/NationalGenomicsInfrastructure/Yggdrasil/blob/250e9e17e8b29cfc8db40268aba8db6e2287308e/lib/storage/plan_documents.py
[couch]: https://github.com/NationalGenomicsInfrastructure/Yggdrasil/blob/250e9e17e8b29cfc8db40268aba8db6e2287308e/lib/couchdb/plan_db_manager.py
[consumer]: https://github.com/NationalGenomicsInfrastructure/Yggdrasil/blob/250e9e17e8b29cfc8db40268aba8db6e2287308e/lib/ops/consumer.py
[builder]: https://github.com/NationalGenomicsInfrastructure/Yggdrasil/blob/250e9e17e8b29cfc8db40268aba8db6e2287308e/yggdrasil/flow/planner/builder.py
[sqlite]: https://github.com/NationalGenomicsInfrastructure/Yggdrasil/blob/250e9e17e8b29cfc8db40268aba8db6e2287308e/lib/storage/sqlite.py
