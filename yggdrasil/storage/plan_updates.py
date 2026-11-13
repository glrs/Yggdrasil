"""Conditional plan-document updates shared by every plan store.

One update rewrites an existing plan document on behalf of execution:
**execution finalization** records a finished execution request's outcome
together with its executed token. It must stay correct while other writers
change the same document.

A backend supplies only two primitives: fetch the current document, and
replace it on condition that its revision has not moved, raising
:class:`~yggdrasil.storage.errors.RevisionConflictError` otherwise. Every
judgment finalization makes from a plan document (family, generation, tokens,
authority) is made here, once, so the CouchDB and SQLite stores cannot
interpret the same race differently. So is what counts as a stored document
at all (:func:`require_storage_envelope`), which both stores' fetch applies.
Each value is read by the contract's own rule (``yggdrasil.contract``): the
family by ``versions.version_of``, the generation by ``read.generation_of``,
the tokens by ``controls.token_pair``. Field names and words are spelled in
``yggdrasil.contract.fields``.

A revision check answers only "has this document changed since I read it". It
cannot answer "is this still the request that was executed": a worker that
rereads the document holds a current revision whatever happened to the plan
in between. The finalizer therefore checks family, generation, token and
authority against every document it reads, before its first write as well as
after a conflict.

The family comes first. Finalization is an update: it changes only the fields
it owns and keeps the rest as read, so it may write only into a document of
the family the request was captured under, which this release must support.
It never writes to a document of any other family, or to one without a valid
``schema_version``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any

from yggdrasil.contract import PLAN_DOCUMENT, read, versions
from yggdrasil.contract.controls import token_pair
from yggdrasil.contract.diagnostics import describe_recorded
from yggdrasil.contract.fields import (
    CONTINUE_INDEPENDENT,
    EXECUTED_RUN_TOKEN_FIELD,
    EXECUTION_AUTHORITY_FIELD,
    EXECUTION_ID_FIELD,
    EXECUTION_OWNER_FIELD,
    FAILURE_POLICY_FIELD,
    FINALIZED_AT_FIELD,
    ID_FIELD,
    LAST_EXECUTED_AT_FIELD,
    LAST_FINALIZED_EXECUTION_FIELD,
    OUTCOME_FIELD,
    PLAN_GENERATION_FIELD,
    PLAN_ID_FIELD,
    REPORT_FIELD,
    REV_FIELD,
    RUN_TOKEN_FIELD,
    TERMINATION_REASON_FIELD,
    UPDATED_AT_FIELD,
)
from yggdrasil.contract.versions import SUPPORTED, version_of
from yggdrasil.flow.attempt import AttemptContext
from yggdrasil.flow.outcomes import ExecutionOutcome, TerminationReason
from yggdrasil.flow.utils.ygg_time import utcnow_iso
from yggdrasil.storage.errors import PlanStoreError, RevisionConflictError
from yggdrasil.storage.plan_documents import validate_execution_authority

# Backend primitive: fetch(doc_id) returns the plan document as stored, with
# its ``_rev``, or None only when the backend confirms there is none. A body
# whose storage envelope is not intact is never returned: the fetch raises
# PlanStoreError instead (see require_storage_envelope).
PlanFetcher = Callable[[str], dict[str, Any] | None]

# Backend primitive: replace(doc_id, body, expected_rev) writes body only if the
# stored revision still equals expected_rev, returning the new revision, and
# raises RevisionConflictError otherwise.
PlanReplacer = Callable[[str, dict[str, Any], str], str]


def require_storage_envelope(doc_id: str, body: object) -> dict[str, Any]:
    """Return what a backend read for a plan, if it is the plan's stored document.

    A body counts as the stored document of plan doc_id only when it is a JSON
    object whose ``_id`` is doc_id and whose ``_rev`` is a non-empty string.
    Anything else, such as an object-shaped error from a proxy, another
    document's body or a body without a revision, says nothing about the
    plan: it is neither a missing plan nor an unversioned document, so no
    conclusion may be drawn from it. Only the envelope is checked; what the
    document says is for its readers to judge.

    Args:
        doc_id: The plan document ID that was read.
        body: What the backend returned for it, as an existing document.

    Returns:
        dict[str, Any]: body itself, unchanged.

    Raises:
        PlanStoreError: Not retryable, if body's envelope is not intact.
    """
    if not isinstance(body, dict):
        problem = f"a value of type {type(body).__name__} instead of a JSON object"
    elif ID_FIELD not in body:
        problem = "an object without an _id"
    elif body[ID_FIELD] != doc_id:
        problem = f"an object whose _id is {describe_recorded(body[ID_FIELD])}"
    elif REV_FIELD not in body:
        problem = "an object without a _rev"
    elif not isinstance(body[REV_FIELD], str) or not body[REV_FIELD]:
        problem = (
            f"an object whose _rev is {describe_recorded(body[REV_FIELD])}, not a "
            "non-empty string"
        )
    else:
        return body
    raise PlanStoreError(
        f"The read of plan '{doc_id}' returned {problem}, which is not the "
        "plan's stored document; the plan's stored state is unknown"
    )


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
        INVALID_CONTROL_STATE: The plan's execution tokens are not a valid
            pair as read, or recording this completion would make them
            invalid: an executed token above the run token it leaves in
            place. Nothing was written; retrying cannot succeed until the
            document changes. The request is neither recorded nor superseded.
    """

    COMMITTED = "committed"
    ALREADY_COMMITTED = "already_committed"
    CONFLICT = "conflict"
    SUPERSEDED = "superseded"
    INVALID_CONTROL_STATE = "invalid_control_state"


class SupersessionReason(str, Enum):
    """Why an execution request can no longer be finalized.

    Attributes:
        PLAN_MISSING: The plan document no longer exists.
        UNSUPPORTED_DOCUMENT: The plan document's ``schema_version`` is
            absent or invalid, names a family this release does not support,
            or is not the family the request was captured under. The
            executor captures requests only from supported documents, and a
            document keeps its family within a generation, so for such a
            request this means another writer replaced or changed the
            document after it was captured.
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
    UNSUPPORTED_DOCUMENT = "unsupported_document"
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

    The adapter for callers that hold the ``flow`` enums, such as
    :class:`ExecutionFinalization` and the execution coordinator. It passes
    the words the enums stand for to
    :func:`yggdrasil.contract.read.finishes_request`, which owns the rule:
    a successful completion finishes a request under any failure policy, and
    a ``continue_independent`` attempt finishes one when it drained with
    failures or was rejected by preflight. Every other ending leaves the
    request eligible for another attempt.

    Args:
        outcome: The attempt's overall outcome.
        termination_reason: How the attempt ended.
        failure_policy: The failure policy the attempt ran under.

    Returns:
        bool: True if the request is finished and should be finalized.
    """
    return read.finishes_request(
        outcome.value, termination_reason.value, failure_policy
    )


@dataclass(frozen=True)
class ExecutionFinalization:
    """A finished execution request to record in its plan document.

    Usually built from the attempt's context with :meth:`from_attempt`. Carries
    the identity captured when the request was admitted (family, generation,
    token, authority, owner) because finalization is judged against what was
    executed, not against what the plan document says now.

    Only a request whose execution finished can be finalized (see
    :func:`finishes_request`); constructing one for any other attempt raises.

    Attributes:
        plan_id: The plan document ID.
        schema_version: The plan document's family, captured when the request
            was admitted. Finalization writes only into a document that is
            still of this family. It is not part of :meth:`identity`: the
            persisted record does not carry it.
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
    schema_version: int
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
            ValueError: If the family is not a positive integer, the
                generation is empty, the token is negative, the authority is
                invalid, the attempt did not finish its request, or the report
                disagrees with the request's identity or outcome.
        """
        # bool is a subclass of int, and 1.0 == 1: only a true int names a family.
        if type(self.schema_version) is not int or self.schema_version < 1:
            raise ValueError(
                f"Execution '{self.execution_id}' of plan '{self.plan_id}' has an "
                f"invalid captured schema_version {self.schema_version!r}; "
                "finalization is always judged against a family"
            )
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
                f"{CONTINUE_INDEPENDENT} attempt, is finalized"
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
    def from_attempt(
        cls, context: AttemptContext, *, schema_version: int
    ) -> ExecutionFinalization:
        """Build the finalization of a finished attempt.

        Args:
            context: The attempt's context, after the attempt ended.
            schema_version: The plan document's family, captured when the
                request was admitted. The attempt context does not carry it.

        Returns:
            ExecutionFinalization: The request to record.

        Raises:
            ValueError: If the attempt has not finished, captured no generation
                or run token, or did not finish its request, or if
                schema_version is not a positive integer.
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
            schema_version=schema_version,
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
            EXECUTION_ID_FIELD: self.execution_id,
            PLAN_ID_FIELD: self.plan_id,
            PLAN_GENERATION_FIELD: self.plan_generation,
            RUN_TOKEN_FIELD: self.run_token,
            FAILURE_POLICY_FIELD: self.failure_policy,
            OUTCOME_FIELD: self.outcome.value,
            TERMINATION_REASON_FIELD: self.termination_reason.value,
        }

    def execution_record(self, *, finalized_at: str) -> dict[str, Any]:
        """Return the record persisted as the plan's last finalized execution.

        Args:
            finalized_at: ISO-8601 UTC time of finalization.

        Returns:
            dict: The completion's identity, the finalization time, and the
            attempt's report.
        """
        return {
            **self.identity(),
            FINALIZED_AT_FIELD: finalized_at,
            REPORT_FIELD: self.report,
        }


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


def _invalid_control_state(
    request: ExecutionFinalization, detail: str
) -> FinalizationResult:
    """Build an INVALID_CONTROL_STATE result for request."""
    return FinalizationResult(
        status=FinalizationStatus.INVALID_CONTROL_STATE,
        message=(
            f"Cannot finalize {_describe(request)}: {detail}; nothing was "
            "written, and the result cannot be recorded until the plan's "
            "tokens change"
        ),
    )


def check_finalization(
    doc: dict[str, Any], request: ExecutionFinalization
) -> FinalizationResult | None:
    """Judge a finalization request against one read of its plan document.

    The checks run in this order:

    1. The document is not of a supported family equal to the captured one
       (:func:`~yggdrasil.contract.versions.version_of`): SUPERSEDED. This
       comes before everything else, because nothing in a document of
       another family, or of none, means what this release reads it as: not
       even a record of this exact completion.
    2. The generation differs from the captured one: SUPERSEDED. This comes
       next so that a regenerated plan always settles the request, whatever
       its tokens say.
    3. The stored execution tokens are not a valid pair under the strict
       token rule (:func:`~yggdrasil.contract.controls.token_pair`):
       INVALID_CONTROL_STATE. Nothing else is concluded from tokens that
       cannot be read.
    4. The document records this exact completion (every field of
       :meth:`ExecutionFinalization.identity`), and its executed token equals
       the captured token: ALREADY_COMMITTED. This comes before the authority
       because a recorded result stays true even if the plan's authority
       changed after it was written.
    5. The authority or owner differs from the captured one: SUPERSEDED.
    6. The executed token already reaches the captured token: SUPERSEDED.
       Another execution, or another result, finalized this request or a
       newer one.
    7. The pair the write would produce, the stored run token with the
       captured token as executed, is not a valid pair:
       INVALID_CONTROL_STATE. Writing would leave an executed token above
       the run token, as when the run token was lowered after admission.

    Args:
        doc: The plan document as just read.
        request: The finalization being attempted.

    Returns:
        FinalizationResult | None: The resolution when the completion must not
        be written; None when the request is valid and not yet recorded.
        Never raises on stored content.
    """
    version = version_of(PLAN_DOCUMENT, doc)
    if version.state != SUPPORTED or version.family != request.schema_version:
        return _superseded(
            request,
            SupersessionReason.UNSUPPORTED_DOCUMENT,
            "the plan's schema_version is now "
            f"{versions.describe_schema_version(version)} ({version.state}), "
            f"and the request was captured under family {request.schema_version}",
        )

    generation = read.generation_of(doc)
    if generation != request.plan_generation:
        return _superseded(
            request,
            SupersessionReason.GENERATION_CHANGED,
            f"the plan's generation is now {describe_recorded(generation)}",
        )

    tokens = token_pair(doc)
    if not tokens.valid:
        return _invalid_control_state(
            request,
            f"the plan's tokens are not a valid pair ({'; '.join(tokens.problems)})",
        )
    executed = tokens.executed
    assert executed is not None  # a valid pair has both tokens

    record = doc.get(LAST_FINALIZED_EXECUTION_FIELD)
    if (
        executed == request.run_token
        and isinstance(record, dict)
        and all(record.get(key) == value for key, value in request.identity().items())
    ):
        return FinalizationResult(
            status=FinalizationStatus.ALREADY_COMMITTED,
            message=f"{_describe(request)} is already finalized",
        )
    authority = doc.get(EXECUTION_AUTHORITY_FIELD)
    owner = doc.get(EXECUTION_OWNER_FIELD)
    if (authority, owner) != (request.execution_authority, request.execution_owner):
        return _superseded(
            request,
            SupersessionReason.AUTHORITY_CHANGED,
            f"execution authority/owner is now {describe_recorded(authority)}/"
            f"{describe_recorded(owner)}, captured as "
            f"{request.execution_authority!r}/"
            f"{describe_recorded(request.execution_owner)}",
        )
    if executed >= request.run_token:
        recorder = record.get(EXECUTION_ID_FIELD) if isinstance(record, dict) else None
        return _superseded(
            request,
            SupersessionReason.REQUEST_ALREADY_FINALIZED,
            f"executed_run_token is already {executed}, recorded by "
            f"{describe_recorded(recorder)}",
        )

    produced = token_pair({**doc, EXECUTED_RUN_TOKEN_FIELD: request.run_token})
    if not produced.valid:
        return _invalid_control_state(
            request,
            f"the plan's tokens are run_token {tokens.run} / executed_run_token "
            f"{executed}, and recording run_token {request.run_token} as executed "
            f"would leave run_token {tokens.run} / executed_run_token "
            f"{request.run_token}, which is not a valid pair "
            f"({'; '.join(produced.problems)})",
        )
    return None


def build_finalized_document(
    doc: dict[str, Any], request: ExecutionFinalization, *, now: str
) -> dict[str, Any]:
    """Return a copy of doc with request's completion applied.

    Only the completion fields change: ``executed_run_token``, the last
    finalized execution record, ``last_executed_at`` and ``updated_at``. Every
    other field is carried over exactly as read, including ``schema_version``,
    and ``run_token``, which may already name a newer request that must stay
    pending.

    Args:
        doc: The plan document the completion was checked against.
        request: The finalization to apply.
        now: ISO-8601 UTC time of finalization.

    Returns:
        dict: The document to write, still carrying the ``_rev`` it was read at.
    """
    finalized = dict(doc)
    finalized[EXECUTED_RUN_TOKEN_FIELD] = request.run_token
    finalized[LAST_FINALIZED_EXECUTION_FIELD] = request.execution_record(
        finalized_at=now
    )
    finalized[LAST_EXECUTED_AT_FIELD] = now
    finalized[UPDATED_AT_FIELD] = now
    return finalized


def _logged(logger: logging.Logger, result: FinalizationResult) -> FinalizationResult:
    """Log a finalization result at a level matching its status, and return it.

    A recorded result is INFO. Invalid control state is ERROR: a finished
    request cannot be recorded until someone changes the plan's tokens. Any
    other result is WARNING.
    """
    if result.recorded:
        level = logging.INFO
    elif result.status is FinalizationStatus.INVALID_CONTROL_STATE:
        level = logging.ERROR
    else:
        level = logging.WARNING
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
    result reports what the newer state means: a supersession, including a
    document that is no longer of the captured family, a completion that
    already landed, tokens that are no longer a valid pair or would be made
    invalid by the write, or a CONFLICT for a request that is still valid.
    Nothing is retried here. A caller that retries CONFLICT or PlanStoreError
    owns the retry bound and backoff, so retries never multiply across layers.

    Args:
        request: The finished request to record.
        fetch: Backend fetch primitive.
        replace: Backend conditional-replace primitive.
        logger: Logger for the resolution.
        now: ISO-8601 UTC finalization time; defaults to the current time.

    Returns:
        FinalizationResult: COMMITTED, ALREADY_COMMITTED, CONFLICT, SUPERSEDED
        or INVALID_CONTROL_STATE.

    Raises:
        PlanStoreError: If the backend fails, or a read returns something
            other than the plan's stored document. A failed write may still
            have landed; calling again reports that as ALREADY_COMMITTED.
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

    finalized = build_finalized_document(doc, request, now=now or utcnow_iso())
    try:
        replace(request.plan_id, finalized, str(doc[REV_FIELD]))
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
