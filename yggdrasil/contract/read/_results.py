"""What the reader is given in place of a document, and what it returns.

Every result is a frozen dataclass holding tuples, never lists, and carries
its problems as :class:`~yggdrasil.contract.problems.ContractProblem` records.
A derived answer sits beside the recorded value it came from, so a consumer
can always show what was stored.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Generic, TypeVar

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
        execution_owner: Which run-once process owns the plan, a non-empty
            string; recorded as ``null`` for the daemon.
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
