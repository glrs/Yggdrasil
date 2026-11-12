"""What the reader is given in place of a document, and what it returns.

Every result is a frozen dataclass holding tuples, never lists, and carries
its problems as :class:`~yggdrasil.contract.problems.ContractProblem` records.
A derived answer sits beside the recorded value it came from, so a consumer
can always show what was stored.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Generic, TypeVar

from yggdrasil.contract import problems as _problems
from yggdrasil.contract.controls import FailurePolicyCheck, TokenPair
from yggdrasil.contract.problems import ContractProblem
from yggdrasil.contract.versions import VersionInfo

T = TypeVar("T")


class NoDocument(Enum):
    """What a caller passes for a document it could not provide.

    Attributes:
        MISSING: The store confirmed that the document does not exist.
        UNAVAILABLE: The read failed, so whether the document exists is
            unknown. Never read as missing.
        LOOKUP_IMPOSSIBLE: The document's ID cannot be derived, so the store
            was never asked. Neither missing nor unavailable; given only for
            a snapshot.
    """

    MISSING = "missing"
    UNAVAILABLE = "unavailable"
    LOOKUP_IMPOSSIBLE = "lookup_impossible"


MISSING = NoDocument.MISSING
UNAVAILABLE = NoDocument.UNAVAILABLE
LOOKUP_IMPOSSIBLE = NoDocument.LOOKUP_IMPOSSIBLE

# Whether the reader was given a body. A result's presence is this word, or
# the value of the NoDocument the caller passed.
PRESENT = "present"
PRESENCES = frozenset({PRESENT, MISSING.value, UNAVAILABLE.value})

# Eligibility codes, a closed set: the first check a plan document fails, in
# this order, or ``eligible``. Two share their words with the problem codes
# that cause them.
INVALID_DOCUMENT = "invalid_document"
UNSUPPORTED_DOCUMENT = "unsupported_document"
INVALID_TOKENS = _problems.INVALID_TOKENS
STATUS_MISSING = "status_missing"
NOT_APPROVED = "not_approved"
ALREADY_EXECUTED = "already_executed"
ELIGIBLE = "eligible"
# Given only by read_plan, which knows the ID a document was read under.
IDENTITY_MISMATCH = _problems.IDENTITY_MISMATCH

ELIGIBILITY_CODES = frozenset(
    {
        INVALID_DOCUMENT,
        UNSUPPORTED_DOCUMENT,
        INVALID_TOKENS,
        STATUS_MISSING,
        NOT_APPROVED,
        ALREADY_EXECUTED,
        ELIGIBLE,
        IDENTITY_MISMATCH,
    }
)

# Verdicts of a correlation, one closed set per question it answers. Each
# question can also be undecided.
UNKNOWN = "unknown"
SAME_GENERATION = "same_generation"
DIFFERENT_GENERATION = "different_generation"
SAME_EXECUTION = "same_execution"
DIFFERENT_EXECUTION = "different_execution"
CURRENT = "current"
OTHER = "other"

PLAN_OBSERVATION_VERDICTS = frozenset({SAME_GENERATION, DIFFERENT_GENERATION, UNKNOWN})
FINALIZED_OBSERVATION_VERDICTS = frozenset(
    {SAME_EXECUTION, DIFFERENT_EXECUTION, UNKNOWN}
)
REQUEST_OBSERVATION_VERDICTS = frozenset({CURRENT, OTHER, UNKNOWN})

# Why there is no observed attempt to compare, in the order they are judged:
# the plan, then whether and how the snapshot was read. Every question shares
# them. The reason for a snapshot that could not be looked up is the word of
# the NoDocument the caller passed for it.
PLAN_NOT_USABLE = "plan_not_usable"
NO_LOOKUP = LOOKUP_IMPOSSIBLE.value
NO_OBSERVATION = "no_observation"
OBSERVATION_UNAVAILABLE = "observation_unavailable"
OBSERVATION_INSPECT_ONLY = "observation_inspect_only"
OBSERVATION_INVALID = "observation_invalid"
LEGACY_PROJECTION = "legacy_projection"
OBSERVATION_REASONS = (
    PLAN_NOT_USABLE,
    NO_LOOKUP,
    NO_OBSERVATION,
    OBSERVATION_UNAVAILABLE,
    OBSERVATION_INSPECT_ONLY,
    OBSERVATION_INVALID,
    LEGACY_PROJECTION,
)

# What decides whether the plan and the observation are of one generation.
PLAN_GENERATION_MISSING = "plan_generation_missing"
ATTEMPT_GENERATION_MISSING = "attempt_generation_missing"
GENERATIONS_EQUAL = "generations_equal"
GENERATIONS_DIFFER = "generations_differ"

# What decides whether the finalized record and the observation are of one
# execution.
NOTHING_FINALIZED = "nothing_finalized"
FINALIZED_UNUSABLE = "finalized_unusable"
EXECUTION_ID_MISSING = "execution_id_missing"
EXECUTION_IDS_DIFFER = "execution_ids_differ"
IDENTITY_CONFLICT = _problems.IDENTITY_CONFLICT
EXECUTION_IDS_EQUAL = "execution_ids_equal"

# What decides whether the observed attempt belongs to the current request.
RUN_TOKENS_DIFFER = "run_tokens_differ"
PLAN_RUN_TOKEN_MISSING = "plan_run_token_missing"
ATTEMPT_RUN_TOKEN_MISSING = "attempt_run_token_missing"
GENERATION_AND_RUN_TOKEN_EQUAL = "generation_and_run_token_equal"

PLAN_OBSERVATION_REASONS = frozenset(
    {
        *OBSERVATION_REASONS,
        PLAN_GENERATION_MISSING,
        ATTEMPT_GENERATION_MISSING,
        GENERATIONS_EQUAL,
        GENERATIONS_DIFFER,
    }
)
FINALIZED_OBSERVATION_REASONS = frozenset(
    {
        *OBSERVATION_REASONS,
        NOTHING_FINALIZED,
        FINALIZED_UNUSABLE,
        EXECUTION_ID_MISSING,
        EXECUTION_IDS_DIFFER,
        IDENTITY_CONFLICT,
        EXECUTION_IDS_EQUAL,
    }
)
REQUEST_OBSERVATION_REASONS = frozenset(
    {
        *OBSERVATION_REASONS,
        GENERATIONS_DIFFER,
        RUN_TOKENS_DIFFER,
        PLAN_GENERATION_MISSING,
        ATTEMPT_GENERATION_MISSING,
        PLAN_RUN_TOKEN_MISSING,
        ATTEMPT_RUN_TOKEN_MISSING,
        GENERATION_AND_RUN_TOKEN_EQUAL,
    }
)


@dataclass(frozen=True)
class Recorded(Generic[T]):
    """A recorded value, beside what the reader recognised in it.

    Attributes:
        value: The recognised value: a value of the field's contract type,
            such as a non-empty string, a word of the field's vocabulary or a
            timestamp's instant in UTC; None when nothing was recognised.
        recorded: The stored value exactly as read, the same object, never
            converted; None when the field is absent.
        present: Whether the field is recorded at all, which tells an absent
            field from one holding ``null``.
    """

    value: T | None
    recorded: object
    present: bool


@dataclass(frozen=True)
class Eligibility:
    """Whether a plan document is eligible for execution, and why.

    Attributes:
        eligible: True only for the code ``eligible``.
        code: One of ELIGIBILITY_CODES; the machine-readable answer.
        reason: The decision in words, for a log line; not a protocol.
        problems: The problems that decided a refusal. Empty when eligible,
            and for a status that is text but not ``approved`` or a request
            already executed, which are answers rather than problems.
    """

    eligible: bool
    code: str
    reason: str
    problems: tuple[ContractProblem, ...]


@dataclass(frozen=True)
class RequestState:
    """What a plan document records about its current execution request.

    The request is the plan's ``plan_generation`` and ``run_token``. Each
    answer is None when the document does not establish it.

    Attributes:
        finished: Whether the request's completion is recorded:
            ``executed_run_token == run_token``. None when the token pair is
            not valid, so the request state is unknown.
        outcome: The request's outcome, a word of EXECUTION_OUTCOMES. Given
            only when ``finished`` is True and the finalized record is usable
            and belongs to this request (``finalized_is_current`` True); None
            otherwise, and for a word the reader does not recognise.
        termination_reason: Why the request's attempt ended, a word of
            TERMINATION_REASONS, under the same condition as ``outcome``.
        finalized_is_current: Whether the finalized record belongs to this
            request, from recorded identity alone: True when its plan ID,
            generation and run token each equal the plan's, False when one
            of them differs, None when there is no usable record or too
            little evidence. Independent of ``finished`` and of the plan's
            eligibility.
    """

    finished: bool | None
    outcome: str | None
    termination_reason: str | None
    finalized_is_current: bool | None


@dataclass(frozen=True)
class StepCounts:
    """How many of an attempt's steps reached each outcome.

    Attributes:
        succeeded: Steps that succeeded.
        reused: Steps whose earlier result was reused.
        failed: Steps that failed.
        blocked: Steps blocked by a failed dependency.
        unreached: Steps the attempt never reached.
    """

    succeeded: int
    reused: int
    failed: int
    blocked: int
    unreached: int


@dataclass(frozen=True)
class FinalizedRecord:
    """A plan document's ``last_finalized_execution``: the last finished request.

    It may belong to an earlier request or generation than the plan's
    current one, and it never establishes by itself that a request finished.

    Attributes:
        execution_id: The attempt that finished the request.
        plan_id: The plan the record names.
        plan_generation: The generation the request was made under.
        run_token: The request's run token; recognised only as an integer
            from 0 to 2**53 - 1 that is not a boolean.
        failure_policy: The policy the attempt ran under, recognised as a
            word of FAILURE_POLICIES.
        outcome: How the request ended, recognised as a word of
            EXECUTION_OUTCOMES.
        termination_reason: Why the attempt ended, recognised as a word of
            TERMINATION_REASONS.
        finalized_at: When the request was recorded as finished.
        counts: The report's step counts; None unless all five are readable.
        usable: Whether the record agrees with its own report on every
            identity field. Only a usable record can state a request's
            outcome.
        disagreeing: The identity fields on which the record and its report
            disagree, sorted. Empty for a usable record, and for a record
            that is not an object, which has no fields to compare.
        recorded: The record exactly as stored.
    """

    execution_id: Recorded[str]
    plan_id: Recorded[str]
    plan_generation: Recorded[str]
    run_token: Recorded[int]
    failure_policy: Recorded[str]
    outcome: Recorded[str]
    termination_reason: Recorded[str]
    finalized_at: Recorded[datetime]
    counts: StepCounts | None
    usable: bool
    disagreeing: tuple[str, ...]
    recorded: object


@dataclass(frozen=True)
class StepRef:
    """A reference from one step to another by step ID.

    Attributes:
        step_id: The step ID, as recorded.
        found: Whether it names a step of the same inventory.
    """

    step_id: str
    found: bool


@dataclass(frozen=True)
class PlannedStep:
    """One step of a plan's planned inventory.

    Attributes:
        step_id: The step's ID.
        name: The step's name; None when it is not a non-empty string.
        deps: The steps it depends on, in stored order; empty for a step
            recorded with no dependencies.
    """

    step_id: str
    name: str | None
    deps: tuple[StepRef, ...]


@dataclass(frozen=True)
class PlanFields:
    """The plan document's descriptive fields, each recorded and recognised.

    Attributes:
        realm: The realm, a non-empty string.
        scope_kind: ``scope.kind``, a non-empty string; absent when ``scope``
            is not an object.
        scope_id: ``scope.id``, a non-empty string; absent when ``scope`` is
            not an object.
        status: The approval status, recognised as a word of
            APPROVAL_STATUSES.
        execution_authority: Who may execute the plan, recognised as a word
            of EXECUTION_AUTHORITIES.
        execution_owner: Which run-once process owns the plan, recognised
            when it is non-empty text. Required; ``null``, as recorded for
            the daemon, and empty text name no owner, without a problem.
        notes: Notes about the plan.
        created_at: When the plan was first planned.
        updated_at: When the document was last written.
        approved_at: When an actor approved the plan.
        approved_by: Who approved it.
        run_requested_at: When an actor last requested a run.
        run_requested_by: Who requested it.
        last_executed_at: When a request was last recorded as finished.
    """

    realm: Recorded[str]
    scope_kind: Recorded[str]
    scope_id: Recorded[str]
    status: Recorded[str]
    execution_authority: Recorded[str]
    execution_owner: Recorded[str]
    notes: Recorded[str]
    created_at: Recorded[datetime]
    updated_at: Recorded[datetime]
    approved_at: Recorded[datetime]
    approved_by: Recorded[str]
    run_requested_at: Recorded[datetime]
    run_requested_by: Recorded[str]
    last_executed_at: Recorded[datetime]


@dataclass(frozen=True)
class PlanRead:
    """What one plan document establishes on its own.

    When ``usable`` is False (no body, not an object, not of a supported
    family, or stored under another ID), only ``plan_id``, ``presence``,
    ``version``, ``document``, ``problems`` and ``eligibility`` are set and
    every other answer is None: nothing is derived from such a document.

    Attributes:
        plan_id: The ID the document was requested or stored under.
        presence: ``present`` for a body; ``missing`` or ``unavailable`` for
            the NoDocument the caller passed.
        version: What the body's ``schema_version`` says; None without a
            body.
        usable: Whether the answers below were derived.
        document: The body exactly as given, for display; None without one.
        problems: Every problem found. A finding that two checks share is
            reported once.
        eligibility: The execution decision. For a body that is not usable it
            is the refusal that explains why: ``invalid_document``,
            ``unsupported_document`` or ``identity_mismatch``. None without a
            body.
        recorded: The descriptive fields.
        generation: The plan's ``plan_generation``, a non-empty string.
        tokens: The execution tokens, under the strict token rule.
        failure_policy: The plan's failure policy, checked with no default.
        approval: The approval status; the same record as
            ``recorded.status``.
        request: The current request's state.
        finalized: The last finalized execution; None when the document has
            none.
        steps: The planned-step inventory, in stored order; None when it
            cannot be returned complete.
        snapshot_id: The ID of the plan's ``plan_status`` snapshot; None when
            it cannot be derived.
    """

    plan_id: str
    presence: str
    version: VersionInfo | None
    usable: bool
    document: object
    problems: tuple[ContractProblem, ...]
    eligibility: Eligibility | None
    recorded: PlanFields | None
    generation: str | None
    tokens: TokenPair | None
    failure_policy: FailurePolicyCheck | None
    approval: Recorded[str] | None
    request: RequestState | None
    finalized: FinalizedRecord | None
    steps: tuple[PlannedStep, ...] | None
    snapshot_id: str | None


@dataclass(frozen=True)
class StepStatus:
    """What a step entry's ``state`` and ``outcome`` say about the step.

    Attributes:
        status: One of STEP_STATUSES: the recorded outcome, the state of an
            entry with none, ``started`` for a step whose latest event is not
            terminal, or ``unknown``.
        contradiction: Why the record contradicts itself, naming both
            recorded values: an outcome beside a state that does not record
            it, or a terminal state without an outcome. None when it does
            not. A contradictory record's status is always ``unknown``.
    """

    status: str
    contradiction: str | None


@dataclass(frozen=True)
class StepError:
    """How a step failed, read the same way from either of its stored shapes.

    While the attempt runs, a step entry's ``error`` is the step's
    ``step.failed`` event's; once the attempt has ended, it is the report's
    failure record, which also names the error's type. The message and the
    error type are text or ``null``, and any other value is unknown and
    reported; the kind, the code and the advice are kept as the error carried
    them, so a value of any type is unknown but no problem.

    Attributes:
        message: The error's message, from its ``error`` field; None when it
            is not non-empty text.
        kind: ``permanent`` or ``transient``, recognised beside the recorded
            value.
        code: The error's code, as the step gave it.
        advice: What to do about it, as the step gave it.
        error_type: The type of the exception that ended the step; recorded
            only once the attempt has ended.
    """

    message: str | None
    kind: Recorded[str]
    code: str | None
    advice: str | None
    error_type: str | None


@dataclass(frozen=True)
class ObservedStep:
    """One step of a snapshot's observed inventory.

    Attributes:
        step_id: The step's ID: its key in the snapshot's ``steps``.
        name: The step's ``step_name``; None when it is not non-empty text.
        status: The step's status, one of STEP_STATUSES (see
            :func:`~yggdrasil.contract.read.step_status`); ``unknown`` for an
            entry without its ``state`` or ``outcome`` key. ``started`` is
            recorded past activity, not proof of present activity.
        contradiction: Why the entry contradicts itself; None when it does
            not.
        recorded_state: The entry's ``state``, as stored; None when absent
            or ``null``.
        recorded_outcome: The entry's ``outcome``, as stored; None when absent
            or ``null``.
        recorded_at: The instant of the step's latest event, in UTC: the time
            the events carry, which a realm may supply. Not necessarily when
            the step reached its outcome. None when the entry records no
            usable time.
        progress_percent: The reported progress, a number from 0 to 100,
            unconverted; given only for a step whose state is
            ``step.progress``, the one event that reports progress.
        error: How the step failed; None when it records no failure, or
            records one that is not an object.
        direct_blockers: For a blocked step, the failed or blocked steps it
            waited on, each marked with whether it is in this snapshot's
            inventory; None for any other step.
        failed_upstream: For a blocked step of a settled attempt, the failed
            steps upstream of it, marked the same way; None for any other
            step, and until the attempt is settled, because only the report
            records them in full.
        recorded: The entry exactly as stored, for the fields the reader does
            not interpret, such as ``run_id``, ``fingerprint`` or ``metrics``.
    """

    step_id: str
    name: str | None
    status: str
    contradiction: str | None
    recorded_state: object
    recorded_outcome: object
    recorded_at: datetime | None
    progress_percent: float | None
    error: StepError | None
    direct_blockers: tuple[StepRef, ...] | None
    failed_upstream: tuple[StepRef, ...] | None
    recorded: Mapping[str, Any]


@dataclass(frozen=True)
class AttemptDiagnostic:
    """Why an attempt ended without a step to blame, such as a preflight rejection.

    Attributes:
        message: The diagnostic's message; None when it is not non-empty
            text, as for a bare interrupt.
        error_type: The type of the error that ended the attempt.
        details: Structured context, such as the step that was running; None
            when it is not an object.
    """

    message: str | None
    error_type: str | None
    details: Mapping[str, Any] | None


@dataclass(frozen=True)
class Attempt:
    """The attempt a snapshot projects: the latest one observed for the plan.

    It may belong to another request or another generation than the plan's
    current one, and a recorded ``running`` is what was observed, not proof
    that anything runs now. Each field is None in ``value`` where the attempt's
    records lack it: the fields copied from the report are ``null`` while the
    attempt runs, and its authority is ``null`` when its start record is
    missing.

    Attributes:
        execution_id: The attempt's ID.
        plan_generation: The generation the attempt ran, a non-empty string.
        run_token: The request's run token, an integer from 0 to 2**53 - 1
            that is not a boolean.
        failure_policy: The policy the attempt ran under, recognised as a word
            of FAILURE_POLICIES; recorded as the plan gave it.
        execution_authority: Who executed it, recognised as a word of
            EXECUTION_AUTHORITIES.
        execution_owner: Which run-once process owned it, recognised when it
            is non-empty text, as on the plan document.
        state: ``running`` or ``finished``, recognised as a word of
            ATTEMPT_STATES.
        started_at: When the attempt started.
        ended_at: When it ended.
        termination_reason: Why it ended, recognised as a word of
            TERMINATION_REASONS.
        outcome: How it ended, recognised as a word of EXECUTION_OUTCOMES.
        is_drained: Whether every runnable step ran before it ended.
        counts: The report's step counts; None while the attempt runs, or
            unless all five are readable.
        diagnostic: Why it ended without a step to blame; None for none.
        settled: Whether the attempt's results are final: True when its
            state is ``finished``, False when ``running``, None otherwise. An
            outcome alone does not settle an attempt.
        recorded: The attempt exactly as stored.
    """

    execution_id: str
    plan_generation: Recorded[str]
    run_token: Recorded[int]
    failure_policy: Recorded[str]
    execution_authority: Recorded[str]
    execution_owner: Recorded[str]
    state: Recorded[str]
    started_at: Recorded[datetime]
    ended_at: Recorded[datetime]
    termination_reason: Recorded[str]
    outcome: Recorded[str]
    is_drained: Recorded[bool]
    counts: StepCounts | None
    diagnostic: AttemptDiagnostic | None
    settled: bool | None
    recorded: Mapping[str, Any]


@dataclass(frozen=True)
class SnapshotRead:
    """What one ``plan_status`` snapshot establishes on its own.

    When ``usable`` is False (no body, not an object, not of a supported
    family, of another plan or realm, or not structured as a snapshot), only
    ``plan_id``, ``realm``, ``presence``, ``version``, ``document`` and
    ``problems`` are set, and every other answer is None: a partly readable
    step inventory is never returned.

    Attributes:
        plan_id: The plan the snapshot was read for.
        realm: The realm it was read for.
        presence: ``present`` for a body; ``missing`` or ``unavailable`` for
            the NoDocument the caller passed.
        version: What the body's ``schema_version`` says; None without a
            body.
        usable: Whether the answers below were derived.
        document: The body exactly as given, for display; None without one.
        problems: Every problem found.
        projection: ``attempt`` for one attempt's status; ``legacy`` for each
            step's latest run from before attempts were recorded, which need
            not belong to one attempt.
        written_at: The snapshot's ``updated_at``: when it was last written,
            not when anything last ran.
        attempt: The projected attempt; None for a legacy projection.
        steps: The observed steps, in stored order.
    """

    plan_id: str
    realm: str
    presence: str
    version: VersionInfo | None
    usable: bool
    document: object
    problems: tuple[ContractProblem, ...]
    projection: str | None
    written_at: Recorded[datetime] | None
    attempt: Attempt | None
    steps: tuple[ObservedStep, ...] | None


@dataclass(frozen=True)
class Verdict:
    """One answer of a correlation, and the reason for it.

    Attributes:
        verdict: The answer, from its question's closed set.
        reason: Why, from its question's closed set of reasons.
    """

    verdict: str
    reason: str


@dataclass(frozen=True)
class Ending:
    """How one record says an execution ended.

    Attributes:
        outcome: The recorded outcome, if recognised.
        termination_reason: The recorded termination reason, if recognised.
    """

    outcome: str | None
    termination_reason: str | None


@dataclass(frozen=True)
class ConflictingEndings:
    """Two usable terminal claims about one execution that disagree.

    Neither is picked as the true one.

    Attributes:
        finalized: How the plan's finalized record says it ended.
        observed: How the snapshot's finished attempt says it ended; its
            outcome is always recognised.
        fields: The fields, ``outcome`` or ``termination_reason``, whose
            recognised values differ, sorted.
    """

    finalized: Ending
    observed: Ending
    fields: tuple[str, ...]


@dataclass(frozen=True)
class Correlation:
    """What a plan document and its snapshot establish together.

    The two are read at different moments, so disagreement from lag is
    normal. Neither read is changed: a qualification of a plan-local answer
    is stated here, beside it.

    Attributes:
        plan: The plan's read.
        snapshot: The snapshot's read, or the NoDocument the caller passed.
        plan_and_observation: Whether the observed attempt ran the plan's
            current generation: ``same_generation``,
            ``different_generation`` or ``unknown``.
        finalized_and_observation: Whether the finalized record and the
            observed attempt are one execution: ``same_execution``,
            ``different_execution`` or ``unknown``.
        request_and_observation: Whether the observed attempt belongs to the
            plan's current request, from recorded identity only (generation
            and run token): ``current``, ``other`` or ``unknown``. Never a
            statement about the request's validity, completion or
            eligibility.
        conflicting_endings: The two endings when the finalized record and
            the same execution's finished attempt, with a recognised outcome,
            disagree on how it ended; None otherwise.
        problems: What the pair contradicts: an identity conflict, or
            conflicting endings.
    """

    plan: PlanRead
    snapshot: SnapshotRead | NoDocument
    plan_and_observation: Verdict
    finalized_and_observation: Verdict
    request_and_observation: Verdict
    conflicting_endings: ConflictingEndings | None
    problems: tuple[ContractProblem, ...]
