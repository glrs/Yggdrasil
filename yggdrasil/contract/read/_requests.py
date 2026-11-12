"""A plan's execution requests: which endings finish one, and what is recorded.

Four facts are kept apart, never merged into one "plan status":

- whether the current request's completion is recorded
  (``executed_run_token == run_token`` for a valid token pair);
- the current request's outcome, known only when its completion is recorded
  and the finalized record is usable and belongs to that same request;
- the last finalized result, ``last_finalized_execution``, which may belong
  to an earlier request or generation;
- whether that record belongs to the current request, from recorded
  identity alone.

A finalized record never establishes completion by itself: with tokens
``4 / 3`` and a record of request 4, request 4 is not finished and its
outcome is unknown, while the record stays available as the last result.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from yggdrasil.contract.controls import TokenPair
from yggdrasil.contract.fields import (
    COMPLETED,
    CONTINUE_INDEPENDENT,
    COUNTS_FIELD,
    EXECUTION_ID_FIELD,
    EXECUTION_OUTCOMES,
    FAILED,
    FAILURE_POLICIES,
    FAILURE_POLICY_FIELD,
    FINALIZATION_IDENTITY_FIELDS,
    FINALIZED_AT_FIELD,
    LAST_FINALIZED_EXECUTION_FIELD,
    OUTCOME_FIELD,
    PLAN_GENERATION_FIELD,
    PLAN_ID_FIELD,
    PREFLIGHT_REJECTED,
    REPORT_FIELD,
    RUN_TOKEN_FIELD,
    SUCCEEDED,
    TERMINATION_REASON_FIELD,
    TERMINATION_REASONS,
)
from yggdrasil.contract.problems import (
    RECORD_REPORT_DISAGREE,
    ContractProblem,
    path_text,
)
from yggdrasil.contract.read import _values
from yggdrasil.contract.read._results import FinalizedRecord, RequestState

_RECORD_PATH: _values.Path = (LAST_FINALIZED_EXECUTION_FIELD,)


def finishes_request(
    outcome: object, termination_reason: object, failure_policy: object
) -> bool:
    """Return whether an attempt that ended this way finished its request.

    A finished request is consumed: its token is recorded as executed, so the
    plan is not eligible again until a newer request raises ``run_token``.
    Three endings finish one:

    - ``succeeded`` and ``completed``, whatever the failure policy says: the
      policy governs only how a failure ends;
    - ``failed`` and ``completed`` (drained) under ``continue_independent``;
    - ``failed`` and ``preflight_rejected`` under ``continue_independent``.

    Every other ending, and any value that is not one of these words exactly,
    finishes nothing. A value helper: it checks no document version. Never
    raises, whatever the arguments hold; no argument is hashed.

    Args:
        outcome: The attempt's recorded outcome.
        termination_reason: How the attempt ended.
        failure_policy: The failure policy the attempt ran under.

    Returns:
        bool: True if the request is finished.
    """
    if _values.is_word(outcome, SUCCEEDED):
        return _values.is_word(termination_reason, COMPLETED)
    return (
        _values.is_word(outcome, FAILED)
        and _values.is_word(failure_policy, CONTINUE_INDEPENDENT)
        and (
            _values.is_word(termination_reason, COMPLETED)
            or _values.is_word(termination_reason, PREFLIGHT_REJECTED)
        )
    )


def finalized_record(
    document: Mapping[str, Any], problems: list[ContractProblem]
) -> FinalizedRecord | None:
    """Read a plan document's ``last_finalized_execution``.

    The record is usable only when it and its embedded ``report`` both carry
    every identity field (``execution_id``, ``plan_id``, ``plan_generation``,
    ``run_token``, ``failure_policy``, ``outcome``, ``termination_reason``),
    each of the contract's type (a non-empty string, or for ``run_token`` an
    integer that is not a boolean), and equal. Types are checked before
    values, so ``true`` never equals ``1``. A disagreement is reported once,
    naming the fields; a report that is not an object makes the record
    unusable too. An unrecognised word is kept as recorded and leaves the
    record usable.

    Args:
        document: A plan document of a supported family.
        problems: Where problems are appended.

    Returns:
        FinalizedRecord | None: The record; None when the document has none.
    """
    if LAST_FINALIZED_EXECUTION_FIELD not in document:
        return None
    record = document[LAST_FINALIZED_EXECUTION_FIELD]
    if not isinstance(record, Mapping):
        problems.append(_values.wrong_type(_RECORD_PATH, record, _values.OBJECT))
        return _unreadable(record)
    path = _RECORD_PATH
    execution_id = _values.identifier(record, EXECUTION_ID_FIELD, path, problems)
    plan_id = _values.identifier(record, PLAN_ID_FIELD, path, problems)
    generation = _values.identifier(record, PLAN_GENERATION_FIELD, path, problems)
    run_token = _values.run_token(record, RUN_TOKEN_FIELD, path, problems)
    policy = _values.word(
        record, FAILURE_POLICY_FIELD, path, problems, FAILURE_POLICIES
    )
    outcome = _values.word(record, OUTCOME_FIELD, path, problems, EXECUTION_OUTCOMES)
    reason = _values.word(
        record, TERMINATION_REASON_FIELD, path, problems, TERMINATION_REASONS
    )
    finalized_at = _values.timestamp(
        record, FINALIZED_AT_FIELD, path, problems, required=True
    )
    report = _report_of(record, problems)
    counts = None
    disagreeing: tuple[str, ...] = ()
    if report is not None:
        report_path = (*path, REPORT_FIELD)
        counts = _values.counts(report, COUNTS_FIELD, report_path, problems)
        disagreeing = disagreements(record, report)
        if disagreeing:
            problems.append(
                _values.problem(
                    RECORD_REPORT_DISAGREE,
                    path,
                    f"{path_text(path)} and its {REPORT_FIELD} disagree on "
                    f"{list(disagreeing)}",
                )
            )
    return FinalizedRecord(
        execution_id=execution_id,
        plan_id=plan_id,
        plan_generation=generation,
        run_token=run_token,
        failure_policy=policy,
        outcome=outcome,
        termination_reason=reason,
        finalized_at=finalized_at,
        counts=counts,
        usable=report is not None and not disagreeing,
        disagreeing=disagreeing,
        recorded=record,
    )


def disagreements(
    record: Mapping[str, Any], report: Mapping[str, Any]
) -> tuple[str, ...]:
    """Return the identity fields on which a finalized record and its report disagree.

    Args:
        record: The finalized record.
        report: Its embedded report.

    Returns:
        tuple[str, ...]: The fields, sorted, that are absent from either, of
        another type than the contract's in either, or unequal.
    """
    return tuple(
        sorted(
            field
            for field in FINALIZATION_IDENTITY_FIELDS
            if not _agree(record, report, field)
        )
    )


def finalized_is_current(
    record: FinalizedRecord | None,
    *,
    plan_id: str,
    generation: str | None,
    run_token: int | None,
) -> bool | None:
    """Say whether a finalized record belongs to the plan's current request.

    Recorded identity only, compared typed: the record's ``plan_id`` against
    the ID the plan was read under, its ``plan_generation`` against the
    plan's, its ``run_token`` against the plan's. A value counts only when it
    is valid on its own. A difference between two values that count decides,
    whatever the other values; missing evidence alone is never a mismatch.
    Independent of whether the request finished, and of whether the plan's
    token pair is valid.

    Args:
        record: The plan's finalized record, if it has one.
        plan_id: The ID the plan was read under.
        generation: The plan's generation, if it has a usable one.
        run_token: The plan's ``run_token``, if it is valid on its own.

    Returns:
        bool | None: True when all three are equal; False when one differs;
        None when the record is absent or unusable, or when no difference is
        established and a value is missing.
    """
    if record is None or not record.usable:
        return None
    return identity_verdict(
        (
            (plan_id, record.plan_id.value),
            (generation, record.plan_generation.value),
            (run_token, record.run_token.value),
        )
    )


def identity_verdict(pairs: tuple[tuple[object, object], ...]) -> bool | None:
    """Judge recorded identity from pairs of values that count, or None.

    Each pair holds two values already recognised as valid on their own, or
    None where a value does not count.

    Args:
        pairs: The values to compare, the plan's side first.

    Returns:
        bool | None: False if a pair whose values both count differs; True if
        every pair counts and is equal; None otherwise.
    """
    verdicts = [None if a is None or b is None else a == b for a, b in pairs]
    if any(verdict is False for verdict in verdicts):
        return False
    if all(verdict is True for verdict in verdicts):
        return True
    return None


def current_request(
    tokens: TokenPair,
    generation: str | None,
    record: FinalizedRecord | None,
    *,
    plan_id: str,
) -> RequestState:
    """Say what a plan document records about its current request.

    Args:
        tokens: The plan's token pair.
        generation: The plan's generation, if it has a usable one.
        record: The plan's finalized record, if it has one.
        plan_id: The ID the plan was read under.

    Returns:
        RequestState: Completion from the tokens alone; the outcome only when
        completion is recorded and the usable record belongs to this request.
    """
    finished = tokens.executed == tokens.run if tokens.valid else None
    current = finalized_is_current(
        record, plan_id=plan_id, generation=generation, run_token=tokens.run
    )
    if finished is True and current is True and record is not None:
        return RequestState(
            finished=True,
            outcome=record.outcome.value,
            termination_reason=record.termination_reason.value,
            finalized_is_current=True,
        )
    return RequestState(
        finished=finished,
        outcome=None,
        termination_reason=None,
        finalized_is_current=current,
    )


def _report_of(
    record: Mapping[str, Any], problems: list[ContractProblem]
) -> Mapping[str, Any] | None:
    """Return a finalized record's report if it is an object, else report why not."""
    path = (*_RECORD_PATH, REPORT_FIELD)
    if REPORT_FIELD not in record:
        problems.append(_values.missing(path))
        return None
    report = record[REPORT_FIELD]
    if not isinstance(report, Mapping):
        problems.append(_values.wrong_type(path, report, _values.OBJECT))
        return None
    return report


def _agree(record: Mapping[str, Any], report: Mapping[str, Any], field: str) -> bool:
    """Return whether record and report hold the same value of field's type."""
    if field not in record or field not in report:
        return False
    ours, theirs = record[field], report[field]
    if field == RUN_TOKEN_FIELD:
        # bool is a subclass of int: true and 1 are of different types here.
        return type(ours) is int and type(theirs) is int and ours == theirs
    return (
        _values.is_identifier(ours) and _values.is_identifier(theirs) and ours == theirs
    )


def _unreadable(record: object) -> FinalizedRecord:
    """Return the unusable record of a value that is not an object."""
    return FinalizedRecord(
        execution_id=_values.absent(),
        plan_id=_values.absent(),
        plan_generation=_values.absent(),
        run_token=_values.absent(),
        failure_policy=_values.absent(),
        outcome=_values.absent(),
        termination_reason=_values.absent(),
        finalized_at=_values.absent(),
        counts=None,
        usable=False,
        disagreeing=(),
        recorded=record,
    )
