"""Conditional plan-document updates shared by every plan store.

Two updates rewrite an existing plan document on behalf of execution, and both
must stay correct while other writers change the same document:

- **Generation initialization** gives a document written before
  ``plan_generation`` existed its first generation ID.
- **Execution finalization** records a finished execution request's outcome
  together with its executed token.

A backend supplies only two primitives: fetch the current document, and
replace it on condition that its revision has not moved, raising
:class:`~lib.storage.errors.RevisionConflictError` otherwise. Everything that
depends on what a plan document means (generations, tokens, authority) is
decided here, once, so the CouchDB and SQLite stores cannot interpret the same
race differently.

A revision check answers only "has this document changed since I read it". It
cannot answer "is this still the request that was executed": a worker that
rereads the document holds a current revision whatever happened to the plan
in between. The finalizer therefore checks generation, token and authority
against every document it reads, before its first write as well as after a
conflict.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any

from lib.storage.errors import RevisionConflictError
from lib.storage.plan_documents import (
    LAST_FINALIZED_EXECUTION_FIELD,
    PLAN_GENERATION_FIELD,
    new_plan_generation,
    plan_generation_of,
    utc_now_iso,
    validate_execution_authority,
)
from yggdrasil.flow.attempt import AttemptContext
from yggdrasil.flow.model import CONTINUE_INDEPENDENT_POLICY
from yggdrasil.flow.outcomes import ExecutionOutcome, TerminationReason

# Backend primitive: fetch(doc_id) returns the live plan document with its
# ``_rev``, or None when there is none.
PlanFetcher = Callable[[str], dict[str, Any] | None]

# Backend primitive: replace(doc_id, body, expected_rev) writes body only if the
# stored revision still equals expected_rev, returning the new revision, and
# raises RevisionConflictError otherwise.
PlanReplacer = Callable[[str, dict[str, Any], str], str]

# Bound on fetch-and-write attempts when assigning a legacy document's first
# generation. Each attempt that loses a race rereads the document first.
GENERATION_INIT_ATTEMPTS = 3


def initialize_plan_generation(
    doc_id: str,
    *,
    fetch: PlanFetcher,
    replace: PlanReplacer,
    logger: logging.Logger,
    attempts: int = GENERATION_INIT_ATTEMPTS,
) -> dict[str, Any] | None:
    """Return the current plan document, first giving it a generation if needed.

    A document that already has a generation is returned as read, without a
    write. A legacy document gets a fresh generation added to exactly the state
    that was read: every other field, ``updated_at`` included, is written back
    unchanged, conditioned on the revision of that read.

    Losing that write to another writer is safe to retry, because the intent
    ("give this plan a generation if it has none") does not depend on the
    state it was computed from. The document is reread: if the other writer
    assigned a generation, it is adopted without writing; otherwise the
    generation is added to the newer state. The returned document is a
    complete snapshot, so a caller can capture the generation and every other
    field from one read.

    Args:
        doc_id: The plan document ID.
        fetch: Backend fetch primitive.
        replace: Backend conditional-replace primitive.
        logger: Logger for the assignment and any lost races.
        attempts: Maximum number of fetch-and-write attempts.

    Returns:
        dict | None: The current document, with its ``plan_generation`` and
        current ``_rev``; None if the plan does not exist.

    Raises:
        RevisionConflictError: If every attempt lost a race to a writer that
            did not assign a generation.
        PlanStoreError: If the backend fails.
        ValueError: If attempts is less than 1.
    """
    if attempts < 1:
        raise ValueError(f"attempts must be at least 1, got {attempts}")

    attempt = 0
    while True:
        attempt += 1
        doc = fetch(doc_id)
        if doc is None or plan_generation_of(doc) is not None:
            return doc

        if PLAN_GENERATION_FIELD in doc:
            logger.warning(
                "Plan '%s' has an unusable plan_generation %r; assigning a new one",
                doc_id,
                doc[PLAN_GENERATION_FIELD],
            )
        initialized = dict(doc)
        initialized[PLAN_GENERATION_FIELD] = new_plan_generation()
        try:
            initialized["_rev"] = replace(doc_id, initialized, str(doc["_rev"]))
        except RevisionConflictError:
            if attempt >= attempts:
                logger.error(
                    "Plan '%s' kept changing while its first plan_generation was "
                    "being assigned; giving up after %d attempts",
                    doc_id,
                    attempts,
                )
                raise
            logger.info(
                "Plan '%s' changed while its first plan_generation was being "
                "assigned; rereading it (attempt %d/%d)",
                doc_id,
                attempt,
                attempts,
            )
            continue

        logger.info(
            "Assigned plan_generation '%s' to legacy plan '%s'",
            initialized[PLAN_GENERATION_FIELD],
            doc_id,
        )
        return initialized


class FinalizationStatus(str, Enum):
    """How one finalization call resolved.

    Attributes:
        COMMITTED: This call wrote the completion.
        ALREADY_COMMITTED: This execution's identical completion was already
            recorded, typically by an earlier call whose success was never
            confirmed. Nothing was written; the request is finalized.
        CONFLICT: The document changed between read and write, and rereading
            it shows the request is still valid and not yet recorded. Nothing
            was written; calling again applies the completion to the newer
            state.
        SUPERSEDED: The request can never be finalized, for the reason given.
            Nothing was written; retrying cannot succeed.
    """

    COMMITTED = "committed"
    ALREADY_COMMITTED = "already_committed"
    CONFLICT = "conflict"
    SUPERSEDED = "superseded"


class SupersessionReason(str, Enum):
    """Why an execution request can no longer be finalized.

    Attributes:
        PLAN_MISSING: The plan document no longer exists.
        GENERATION_CHANGED: The plan was regenerated, or deleted and
            recreated, after the request was captured, or another writer
            removed its generation.
        AUTHORITY_CHANGED: The plan's execution authority or owner is no
            longer the one the request was captured under.
        REQUEST_ALREADY_FINALIZED: The executed token already covers the
            captured token, recorded by a different execution or with a
            different result. Writing would overwrite that result or lower the
            token.
    """

    PLAN_MISSING = "plan_missing"
    GENERATION_CHANGED = "generation_changed"
    AUTHORITY_CHANGED = "authority_changed"
    REQUEST_ALREADY_FINALIZED = "request_already_finalized"


@dataclass(frozen=True)
class FinalizationResult:
    """The resolution of one finalization call.

    Attributes:
        status: How the call resolved.
        message: Human-readable explanation, for logs.
        reason: Why the request was superseded; set exactly when status is
            SUPERSEDED.
    """

    status: FinalizationStatus
    message: str
    reason: SupersessionReason | None = None

    def __post_init__(self) -> None:
        """Keep reason and status consistent.

        Raises:
            ValueError: If a SUPERSEDED result has no reason, or any other
                result has one.
        """
        if (self.status is FinalizationStatus.SUPERSEDED) != (self.reason is not None):
            raise ValueError(
                f"A {self.status.value} finalization result "
                f"{'requires' if self.reason is None else 'cannot carry'} a "
                "supersession reason"
            )

    @property
    def recorded(self) -> bool:
        """Whether the execution's completion is now recorded in the plan.

        Returns:
            bool: True for COMMITTED and ALREADY_COMMITTED.
        """
        return self.status in (
            FinalizationStatus.COMMITTED,
            FinalizationStatus.ALREADY_COMMITTED,
        )


def finishes_request(
    outcome: ExecutionOutcome,
    termination_reason: TerminationReason,
    failure_policy: str,
) -> bool:
    """Whether an attempt that ended this way finished its execution request.

    A finished request is consumed: its token is recorded as executed, so the
    plan is not eligible again until a newer request raises ``run_token``.
    Three endings finish a request:

    - A successful completion, under either failure policy.
    - A ``continue_independent`` attempt that drained with failures. Its
      failures and blocked steps are the request's result.
    - A ``continue_independent`` plan rejected by preflight. Retrying an
      unchanged plan would be rejected the same way.

    Every other ending leaves the request eligible for another attempt: a
    fail-fast failure keeps its established retry behavior, and a cancelled
    or orchestration-failed attempt never finished.

    Args:
        outcome: The attempt's overall outcome.
        termination_reason: How the attempt ended.
        failure_policy: The failure policy the attempt ran under.

    Returns:
        bool: True if the request is finished and should be finalized.
    """
    if outcome is ExecutionOutcome.SUCCEEDED:
        return termination_reason is TerminationReason.COMPLETED
    return failure_policy == CONTINUE_INDEPENDENT_POLICY and termination_reason in (
        TerminationReason.COMPLETED,
        TerminationReason.PREFLIGHT_REJECTED,
    )


@dataclass(frozen=True)
class ExecutionFinalization:
    """A finished execution request to record in its plan document.

    Usually built from the attempt's context with :meth:`from_attempt`. Carries
    the identity captured when the request was admitted (generation, token,
    authority, owner) because finalization is judged against what was
    executed, not against what the plan document says now.

    Only a request whose execution finished can be finalized (see
    :func:`finishes_request`); constructing one for any other attempt raises.

    Attributes:
        plan_id: The plan document ID.
        plan_generation: Generation captured when the request was admitted.
        run_token: Run token captured when the request was admitted.
        execution_id: The attempt that executed the request. An interrupted
            attempt and its retry share generation, token and possibly
            outcome; the execution ID is what tells their results apart.
        execution_authority: Authority captured with the request.
        execution_owner: Owner captured with the request, if any.
        failure_policy: The failure policy the attempt ran under.
        outcome: The attempt's overall outcome.
        termination_reason: How the attempt ended.
        report: JSON form of the attempt's report, persisted with the outcome
            as its diagnostics. Its identity and outcome fields must match the
            fields above.
    """

    plan_id: str
    plan_generation: str
    run_token: int
    execution_id: str
    execution_authority: str
    execution_owner: str | None
    failure_policy: str
    outcome: ExecutionOutcome
    termination_reason: TerminationReason
    report: dict[str, Any]

    def __post_init__(self) -> None:
        """Reject a request that cannot be finalized or contradicts its report.

        Raises:
            ValueError: If the generation is empty, the token is negative, the
                authority is invalid, the attempt did not finish its request,
                or the report disagrees with the request's identity or outcome.
        """
        if not self.plan_generation:
            raise ValueError(
                f"Execution '{self.execution_id}' of plan '{self.plan_id}' has no "
                "captured plan_generation; finalization is always judged against one"
            )
        if self.run_token < 0:
            raise ValueError(
                f"Execution '{self.execution_id}' of plan '{self.plan_id}' has an "
                f"invalid run_token {self.run_token}"
            )
        validate_execution_authority(self.execution_authority)
        if not finishes_request(
            self.outcome, self.termination_reason, self.failure_policy
        ):
            raise ValueError(
                f"Execution '{self.execution_id}' of plan '{self.plan_id}' did not "
                f"finish its request (outcome={self.outcome.value}, "
                f"termination_reason={self.termination_reason.value}, "
                f"failure_policy={self.failure_policy!r}); only a successful "
                "completion, or a drained or preflight-rejected "
                f"{CONTINUE_INDEPENDENT_POLICY} attempt, is finalized"
            )
        mismatched = sorted(
            key
            for key, value in self.identity().items()
            if self.report.get(key) != value
        )
        if mismatched:
            raise ValueError(
                f"Execution '{self.execution_id}' of plan '{self.plan_id}': its "
                f"report disagrees with the finalization on {mismatched}"
            )

    @classmethod
    def from_attempt(cls, context: AttemptContext) -> ExecutionFinalization:
        """Build the finalization of a finished attempt.

        Args:
            context: The attempt's context, after the attempt ended.

        Returns:
            ExecutionFinalization: The request to record.

        Raises:
            ValueError: If the attempt has not finished, captured no generation
                or run token, or did not finish its request.
        """
        report = context.report
        outcome = report.outcome
        reason = report.termination_reason
        if outcome is None or reason is None:
            raise ValueError(
                f"Attempt '{report.execution_id}' of plan '{report.plan_id}' has "
                "not finished; there is nothing to finalize"
            )
        if report.plan_generation is None or report.run_token is None:
            raise ValueError(
                f"Attempt '{report.execution_id}' of plan '{report.plan_id}' "
                "captured no plan_generation or run_token; finalization needs both"
            )
        return cls(
            plan_id=report.plan_id,
            plan_generation=report.plan_generation,
            run_token=report.run_token,
            execution_id=report.execution_id,
            execution_authority=context.execution_authority,
            execution_owner=context.execution_owner,
            failure_policy=report.failure_policy,
            outcome=outcome,
            termination_reason=reason,
            report=report.to_dict(),
        )

    def identity(self) -> dict[str, Any]:
        """Return the fields that identify this completion.

        Two completions are the same only if all of these match. They are
        also the fields the persisted record and the report must agree on.

        Returns:
            dict: Execution ID, plan ID, generation, token, failure policy,
            outcome and termination reason, as plain JSON values.
        """
        return {
            "execution_id": self.execution_id,
            "plan_id": self.plan_id,
            "plan_generation": self.plan_generation,
            "run_token": self.run_token,
            "failure_policy": self.failure_policy,
            "outcome": self.outcome.value,
            "termination_reason": self.termination_reason.value,
        }

    def execution_record(self, *, finalized_at: str) -> dict[str, Any]:
        """Return the record persisted as the plan's last finalized execution.

        Args:
            finalized_at: ISO-8601 UTC time of finalization.

        Returns:
            dict: The completion's identity, the finalization time, and the
            attempt's report.
        """
        return {**self.identity(), "finalized_at": finalized_at, "report": self.report}


def _executed_token(doc: dict[str, Any]) -> int:
    """Read a plan document's executed token, defaulting to never executed.

    Args:
        doc: A persisted plan document.

    Returns:
        int: The executed run token, or -1 if the document has none.

    Raises:
        ValueError: If the stored value is not an integer.
    """
    value = doc.get("executed_run_token", -1)
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Plan '{doc.get('_id', '<unknown>')}' has an unreadable "
            f"executed_run_token {value!r}; refusing to finalize over it"
        ) from exc


def _describe(request: ExecutionFinalization) -> str:
    """Name a finalization request for log and result messages."""
    return (
        f"execution '{request.execution_id}' of plan '{request.plan_id}' "
        f"(generation '{request.plan_generation}', run_token {request.run_token})"
    )


def _superseded(
    request: ExecutionFinalization, reason: SupersessionReason, detail: str
) -> FinalizationResult:
    """Build a SUPERSEDED result for request."""
    return FinalizationResult(
        status=FinalizationStatus.SUPERSEDED,
        reason=reason,
        message=f"Cannot finalize {_describe(request)}: {detail}",
    )


def check_finalization(
    doc: dict[str, Any], request: ExecutionFinalization
) -> FinalizationResult | None:
    """Judge a finalization request against one read of its plan document.

    The checks run in this order:

    1. The document records this exact completion (every field of
       :meth:`ExecutionFinalization.identity`) under the captured generation,
       and its executed token equals the captured token: ALREADY_COMMITTED.
       This comes first because a recorded result stays true even if the
       plan's authority changed after it was written.
    2. The generation differs from the captured one: SUPERSEDED.
    3. The authority or owner differs from the captured one: SUPERSEDED.
    4. The executed token already reaches the captured token: SUPERSEDED.
       Another execution, or another result, finalized this request or a
       newer one.

    Args:
        doc: The plan document as just read.
        request: The finalization being attempted.

    Returns:
        FinalizationResult | None: The resolution when the completion must not
        be written; None when the request is valid and not yet recorded.

    Raises:
        ValueError: If the document's executed_run_token is not an integer.
    """
    generation = plan_generation_of(doc)
    executed = _executed_token(doc)
    record = doc.get(LAST_FINALIZED_EXECUTION_FIELD)

    if (
        generation == request.plan_generation
        and executed == request.run_token
        and isinstance(record, dict)
        and all(record.get(key) == value for key, value in request.identity().items())
    ):
        return FinalizationResult(
            status=FinalizationStatus.ALREADY_COMMITTED,
            message=f"{_describe(request)} is already finalized",
        )
    if generation != request.plan_generation:
        return _superseded(
            request,
            SupersessionReason.GENERATION_CHANGED,
            f"the plan's generation is now {generation!r}",
        )
    authority = doc.get("execution_authority")
    owner = doc.get("execution_owner")
    if (authority, owner) != (request.execution_authority, request.execution_owner):
        return _superseded(
            request,
            SupersessionReason.AUTHORITY_CHANGED,
            f"execution authority/owner is now {authority!r}/{owner!r}, captured "
            f"as {request.execution_authority!r}/{request.execution_owner!r}",
        )
    if executed >= request.run_token:
        return _superseded(
            request,
            SupersessionReason.REQUEST_ALREADY_FINALIZED,
            f"executed_run_token is already {executed}, recorded by "
            f"{record.get('execution_id') if isinstance(record, dict) else None!r}",
        )
    return None


def build_finalized_document(
    doc: dict[str, Any], request: ExecutionFinalization, *, now: str
) -> dict[str, Any]:
    """Return a copy of doc with request's completion applied.

    Only the completion fields change: ``executed_run_token``, the last
    finalized execution record, ``last_executed_at`` and ``updated_at``. Every
    other field is carried over exactly as read, including ``run_token``, which
    may already name a newer request that must stay pending.

    Args:
        doc: The plan document the completion was checked against.
        request: The finalization to apply.
        now: ISO-8601 UTC time of finalization.

    Returns:
        dict: The document to write, still carrying the ``_rev`` it was read at.
    """
    finalized = dict(doc)
    finalized["executed_run_token"] = request.run_token
    finalized[LAST_FINALIZED_EXECUTION_FIELD] = request.execution_record(
        finalized_at=now
    )
    finalized["last_executed_at"] = now
    finalized["updated_at"] = now
    return finalized


def _logged(logger: logging.Logger, result: FinalizationResult) -> FinalizationResult:
    """Log a finalization result at a level matching its status, and return it."""
    level = logging.INFO if result.recorded else logging.WARNING
    logger.log(level, "%s", result.message)
    return result


def finalize_execution(
    request: ExecutionFinalization,
    *,
    fetch: PlanFetcher,
    replace: PlanReplacer,
    logger: logging.Logger,
    now: str | None = None,
) -> FinalizationResult:
    """Record a finished execution request in its plan document.

    Makes exactly one attempt: read the document, judge the request against it
    with :func:`check_finalization`, and if it is still valid write the
    completion conditioned on the revision that was read. The executed token
    and the outcome record land in that one write, so they cannot disagree.

    If the write loses a race, the document is read and judged again, so the
    result reports what the newer state means: a supersession, a completion
    that already landed, or a CONFLICT for a request that is still valid.
    Nothing is retried here. A caller that retries CONFLICT or PlanStoreError
    owns the retry bound and backoff, so retries never multiply across layers.

    Args:
        request: The finished request to record.
        fetch: Backend fetch primitive.
        replace: Backend conditional-replace primitive.
        logger: Logger for the resolution.
        now: ISO-8601 UTC finalization time; defaults to the current time.

    Returns:
        FinalizationResult: COMMITTED, ALREADY_COMMITTED, CONFLICT or SUPERSEDED.

    Raises:
        PlanStoreError: If the backend fails. A failed write may still have
            landed; calling again reports that as ALREADY_COMMITTED.
        ValueError: If the document's executed_run_token is not an integer.
    """
    doc = fetch(request.plan_id)
    if doc is None:
        return _logged(
            logger,
            _superseded(
                request, SupersessionReason.PLAN_MISSING, "the plan no longer exists"
            ),
        )
    verdict = check_finalization(doc, request)
    if verdict is not None:
        return _logged(logger, verdict)

    finalized = build_finalized_document(doc, request, now=now or utc_now_iso())
    try:
        replace(request.plan_id, finalized, str(doc["_rev"]))
    except RevisionConflictError:
        current = fetch(request.plan_id)
        if current is None:
            return _logged(
                logger,
                _superseded(
                    request,
                    SupersessionReason.PLAN_MISSING,
                    "the plan was deleted while it was being finalized",
                ),
            )
        verdict = check_finalization(current, request)
        if verdict is not None:
            return _logged(logger, verdict)
        return _logged(
            logger,
            FinalizationResult(
                status=FinalizationStatus.CONFLICT,
                message=(
                    f"Plan changed while finalizing {_describe(request)}; the "
                    "request is still valid and nothing was written"
                ),
            ),
        )

    return _logged(
        logger,
        FinalizationResult(
            status=FinalizationStatus.COMMITTED,
            message=(
                f"Finalized {_describe(request)} with outcome "
                f"'{request.outcome.value}'"
            ),
        ),
    )
