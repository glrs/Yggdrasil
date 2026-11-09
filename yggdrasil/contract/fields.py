"""The contract's field names and vocabulary words, spelled once.

Plan documents, snapshots and attempt reports are written and read by programs
released separately, so a misspelled field or word is a silent incompatibility.
This module is the one place the contract's spellings are written down, as
plain strings and frozensets. Constants for fields end in ``_FIELD``; a word
of a vocabulary is a constant named after the word (``APPROVED``,
``FAIL_FAST``, ``EVENT_STEP_FAILED``).

**Spellings only.** The module spells names; it does not decide what they
mean. A module that needs a set built from them, such as the terminal events
of one projection, builds its own from these constants. No set here replaces
another module's, and importing a spelling changes no behavior. A test keeps
every vocabulary equal to its source of truth: the ``flow`` enums, the
projection's constants, the events the engine emits, and the enumerations of
the published schemas.

**A leaf.** The module imports nothing from the package, so every contract
module, whatever its place in the package, can take its spellings from here
without an import cycle.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

# Fields of a plan document: the top level of what the plan store persists.
ID_FIELD = "_id"
REV_FIELD = "_rev"
SCHEMA_VERSION_FIELD = "schema_version"
REALM_FIELD = "realm"
SCOPE_FIELD = "scope"
STATUS_FIELD = "status"
PLAN_FIELD = "plan"
PREVIEW_FIELD = "preview"
PLAN_GENERATION_FIELD = "plan_generation"
RUN_TOKEN_FIELD = "run_token"
EXECUTED_RUN_TOKEN_FIELD = "executed_run_token"
EXECUTION_AUTHORITY_FIELD = "execution_authority"
EXECUTION_OWNER_FIELD = "execution_owner"
APPROVALS_REQUIRED_FIELD = "approvals_required"
CREATED_AT_FIELD = "created_at"
UPDATED_AT_FIELD = "updated_at"
APPROVED_AT_FIELD = "approved_at"
APPROVED_BY_FIELD = "approved_by"
RUN_REQUESTED_AT_FIELD = "run_requested_at"
RUN_REQUESTED_BY_FIELD = "run_requested_by"
LAST_EXECUTED_AT_FIELD = "last_executed_at"
LAST_FINALIZED_EXECUTION_FIELD = "last_finalized_execution"
NOTES_FIELD = "notes"
SOURCE_DOC_ID_FIELD = "source_doc_id"
SOURCE_DOC_REV_FIELD = "source_doc_rev"

PLAN_DOCUMENT_FIELDS = frozenset(
    {
        ID_FIELD,
        REV_FIELD,
        SCHEMA_VERSION_FIELD,
        REALM_FIELD,
        SCOPE_FIELD,
        STATUS_FIELD,
        PLAN_FIELD,
        PREVIEW_FIELD,
        PLAN_GENERATION_FIELD,
        RUN_TOKEN_FIELD,
        EXECUTED_RUN_TOKEN_FIELD,
        EXECUTION_AUTHORITY_FIELD,
        EXECUTION_OWNER_FIELD,
        APPROVALS_REQUIRED_FIELD,
        CREATED_AT_FIELD,
        UPDATED_AT_FIELD,
        APPROVED_AT_FIELD,
        APPROVED_BY_FIELD,
        RUN_REQUESTED_AT_FIELD,
        RUN_REQUESTED_BY_FIELD,
        LAST_EXECUTED_AT_FIELD,
        LAST_FINALIZED_EXECUTION_FIELD,
        NOTES_FIELD,
        SOURCE_DOC_ID_FIELD,
        SOURCE_DOC_REV_FIELD,
    }
)

# Fields of the plan inside a plan document, and of each of its steps.
PLAN_ID_FIELD = "plan_id"
STEPS_FIELD = "steps"
FAILURE_POLICY_FIELD = "failure_policy"
STEP_ID_FIELD = "step_id"
NAME_FIELD = "name"
FN_REF_FIELD = "fn_ref"
PARAMS_FIELD = "params"
DEPS_FIELD = "deps"
INPUTS_FIELD = "inputs"
OUTPUTS_FIELD = "outputs"

# Fields of a finalized execution record, which a plan document keeps under
# ``last_finalized_execution``, and of the attempt report it embeds.
EXECUTION_ID_FIELD = "execution_id"
OUTCOME_FIELD = "outcome"
TERMINATION_REASON_FIELD = "termination_reason"
FINALIZED_AT_FIELD = "finalized_at"
REPORT_FIELD = "report"

# What a finalized record and its report must agree on: the fields of
# ``ExecutionFinalization.identity()``.
FINALIZATION_IDENTITY_FIELDS = frozenset(
    {
        EXECUTION_ID_FIELD,
        PLAN_ID_FIELD,
        PLAN_GENERATION_FIELD,
        RUN_TOKEN_FIELD,
        FAILURE_POLICY_FIELD,
        OUTCOME_FIELD,
        TERMINATION_REASON_FIELD,
    }
)

FINALIZED_RECORD_FIELDS = FINALIZATION_IDENTITY_FIELDS | {
    FINALIZED_AT_FIELD,
    REPORT_FIELD,
}

# Fields of a plan_status snapshot.
TYPE_FIELD = "type"
PROJECTION_FIELD = "projection"
ATTEMPT_FIELD = "attempt"

SNAPSHOT_FIELDS = frozenset(
    {
        ID_FIELD,
        REV_FIELD,
        SCHEMA_VERSION_FIELD,
        TYPE_FIELD,
        REALM_FIELD,
        SCOPE_FIELD,
        PLAN_ID_FIELD,
        PROJECTION_FIELD,
        ATTEMPT_FIELD,
        STEPS_FIELD,
        UPDATED_AT_FIELD,
    }
)

# Fields of a snapshot's attempt.
STARTED_AT_FIELD = "started_at"
ENDED_AT_FIELD = "ended_at"
STATE_FIELD = "state"
IS_DRAINED_FIELD = "is_drained"
COUNTS_FIELD = "counts"
DIAGNOSTIC_FIELD = "diagnostic"

ATTEMPT_FIELDS = frozenset(
    {
        EXECUTION_ID_FIELD,
        PLAN_GENERATION_FIELD,
        RUN_TOKEN_FIELD,
        FAILURE_POLICY_FIELD,
        EXECUTION_AUTHORITY_FIELD,
        EXECUTION_OWNER_FIELD,
        STARTED_AT_FIELD,
        STATE_FIELD,
        ENDED_AT_FIELD,
        TERMINATION_REASON_FIELD,
        OUTCOME_FIELD,
        IS_DRAINED_FIELD,
        COUNTS_FIELD,
        DIAGNOSTIC_FIELD,
    }
)

# Fields of a snapshot's step entry.
STEP_NAME_FIELD = "step_name"
RUN_ID_FIELD = "run_id"
FINGERPRINT_FIELD = "fingerprint"
PROGRESS_FIELD = "progress"
ARTIFACTS_FIELD = "artifacts"
METRICS_FIELD = "metrics"
JOB_FIELD = "job"
TS_FIELD = "ts"
ERROR_FIELD = "error"
DIRECT_BLOCKERS_FIELD = "direct_blockers"
FAILED_ANCESTORS_FIELD = "failed_ancestors"

STEP_ENTRY_FIELDS = frozenset(
    {
        STEP_NAME_FIELD,
        STATE_FIELD,
        OUTCOME_FIELD,
        RUN_ID_FIELD,
        FINGERPRINT_FIELD,
        PROGRESS_FIELD,
        ARTIFACTS_FIELD,
        METRICS_FIELD,
        JOB_FIELD,
        TS_FIELD,
        ERROR_FIELD,
        DIRECT_BLOCKERS_FIELD,
        FAILED_ANCESTORS_FIELD,
    }
)

# A plan document's approval status.
DRAFT = "draft"
APPROVED = "approved"
APPROVAL_STATUSES = frozenset({DRAFT, APPROVED})

# Who may execute a plan.
DAEMON = "daemon"
RUN_ONCE = "run_once"
EXECUTION_AUTHORITIES = frozenset({DAEMON, RUN_ONCE})

# What a plan does when a step fails.
FAIL_FAST = "fail_fast"
CONTINUE_INDEPENDENT = "continue_independent"
FAILURE_POLICIES = frozenset({FAIL_FAST, CONTINUE_INDEPENDENT})

# What a step or a whole execution ended as. A step can also be reused from
# an earlier run or blocked by a failed dependency; an execution only
# succeeds or fails.
SUCCEEDED = "succeeded"
REUSED = "reused"
FAILED = "failed"
BLOCKED = "blocked"
EXECUTION_OUTCOMES = frozenset({SUCCEEDED, FAILED})
STEP_OUTCOMES = frozenset({SUCCEEDED, REUSED, FAILED, BLOCKED})

# Why an execution ended.
COMPLETED = "completed"
FAILED_FAST = "failed_fast"
CANCELLED = "cancelled"
PREFLIGHT_REJECTED = "preflight_rejected"
ORCHESTRATION_ERROR = "orchestration_error"
TERMINATION_REASONS = frozenset(
    {COMPLETED, FAILED_FAST, CANCELLED, PREFLIGHT_REJECTED, ORCHESTRATION_ERROR}
)

# The kind of a step's error.
PERMANENT = "permanent"
TRANSIENT = "transient"
ERROR_KINDS = frozenset({PERMANENT, TRANSIENT})

# What a snapshot projects: one attempt, or a history from before attempts.
ATTEMPT = "attempt"
LEGACY = "legacy"
PROJECTIONS = frozenset({ATTEMPT, LEGACY})

# How far the projected attempt has got.
RUNNING = "running"
FINISHED = "finished"
ATTEMPT_STATES = frozenset({RUNNING, FINISHED})

# The state of a step entry that no step event describes: still to come,
# cut short, never reached, or a legacy run with no lifecycle event at all.
PENDING = "pending"
INTERRUPTED = "interrupted"
UNREACHED = "unreached"
UNKNOWN = "unknown"
STEP_STATES = frozenset({PENDING, INTERRUPTED, UNREACHED, UNKNOWN})

# The lifecycle events of a step, which an entry's ``state`` otherwise names.
EVENT_STEP_STARTED = "step.started"
EVENT_STEP_PROGRESS = "step.progress"
EVENT_STEP_SUCCEEDED = "step.succeeded"
EVENT_STEP_SKIPPED = "step.skipped"
EVENT_STEP_FAILED = "step.failed"
EVENT_STEP_BLOCKED = "step.blocked"
STEP_EVENTS = frozenset(
    {
        EVENT_STEP_STARTED,
        EVENT_STEP_PROGRESS,
        EVENT_STEP_SUCCEEDED,
        EVENT_STEP_SKIPPED,
        EVENT_STEP_FAILED,
        EVENT_STEP_BLOCKED,
    }
)

# The terminal events, and the outcome each records. Read-only, so no caller
# can change what an event means.
OUTCOME_EVENTS: Mapping[str, str] = MappingProxyType(
    {
        EVENT_STEP_SUCCEEDED: SUCCEEDED,
        EVENT_STEP_SKIPPED: REUSED,
        EVENT_STEP_FAILED: FAILED,
        EVENT_STEP_BLOCKED: BLOCKED,
    }
)

# What a step's status can be: its outcome, the state of an entry with none,
# or ``started`` for a step whose latest event is not terminal.
STARTED = "started"
STEP_STATUSES = frozenset(
    {
        PENDING,
        STARTED,
        SUCCEEDED,
        REUSED,
        FAILED,
        BLOCKED,
        INTERRUPTED,
        UNREACHED,
        UNKNOWN,
    }
)
