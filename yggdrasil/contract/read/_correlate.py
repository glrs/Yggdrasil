"""Relating a plan document to its snapshot, without merging the two.

The two documents are written and read at different moments, so they may
describe different requests, generations or executions, and disagreement
from lag is normal. :func:`correlate` compares only what each recorded, by
typed equality of values each read already recognised, and answers three
questions, each with a verdict and a reason from a closed set:

- **plan and observation**: did the observed attempt run the plan's current
  generation? Generations are random identifiers without an order, so a
  different one is neither older nor newer;
- **finalized and observation**: are the plan's finalized record and the
  observed attempt one execution?
- **request and observation**: does the observed attempt belong to the
  plan's current request? Recorded identity only: the generation and the
  run token, under the same rule as the finalized record's
  ``finalized_is_current``.

Each question first asks whether there is an attempt to compare at all, in
this order: the plan is not usable (``plan_not_usable``); the snapshot could
not be looked up (``lookup_impossible``), was confirmed absent
(``no_observation``) or could not be read (``observation_unavailable``); it
is inspect-only (``observation_inspect_only``), invalid
(``observation_invalid``) or a legacy projection, which has no attempt
(``legacy_projection``). The finalized-record question asks first whether
there is a usable record to compare (``plan_not_usable``,
``nothing_finalized``, ``finalized_unusable``).
"""

from __future__ import annotations

from collections.abc import Mapping

from yggdrasil.contract import versions
from yggdrasil.contract.diagnostics import describe_recorded
from yggdrasil.contract.fields import (
    LAST_FINALIZED_EXECUTION_FIELD,
    LEGACY,
    OUTCOME_FIELD,
    PLAN_GENERATION_FIELD,
    RUN_TOKEN_FIELD,
    TERMINATION_REASON_FIELD,
)
from yggdrasil.contract.problems import CONFLICTING_ENDINGS, ContractProblem
from yggdrasil.contract.read import _requests, _values
from yggdrasil.contract.read._results import (
    ATTEMPT_GENERATION_MISSING,
    ATTEMPT_RUN_TOKEN_MISSING,
    CURRENT,
    DIFFERENT_EXECUTION,
    DIFFERENT_GENERATION,
    EXECUTION_ID_MISSING,
    EXECUTION_IDS_DIFFER,
    EXECUTION_IDS_EQUAL,
    FINALIZED_UNUSABLE,
    GENERATION_AND_RUN_TOKEN_EQUAL,
    GENERATIONS_DIFFER,
    GENERATIONS_EQUAL,
    IDENTITY_CONFLICT,
    LEGACY_PROJECTION,
    LOOKUP_IMPOSSIBLE,
    MISSING,
    NO_LOOKUP,
    NO_OBSERVATION,
    NOTHING_FINALIZED,
    OBSERVATION_INSPECT_ONLY,
    OBSERVATION_INVALID,
    OBSERVATION_UNAVAILABLE,
    OTHER,
    PLAN_GENERATION_MISSING,
    PLAN_NOT_USABLE,
    PLAN_RUN_TOKEN_MISSING,
    PRESENT,
    RUN_TOKENS_DIFFER,
    SAME_EXECUTION,
    SAME_GENERATION,
    UNAVAILABLE,
    UNKNOWN,
    Attempt,
    ConflictingEndings,
    Correlation,
    Ending,
    FinalizedRecord,
    NoDocument,
    PlanRead,
    SnapshotRead,
    Verdict,
)

_RECORD_PATH: _values.Path = (LAST_FINALIZED_EXECUTION_FIELD,)

# Why there is no attempt to compare, for each NoDocument a snapshot can be.
_UNREAD = {
    LOOKUP_IMPOSSIBLE: NO_LOOKUP,
    MISSING: NO_OBSERVATION,
    UNAVAILABLE: OBSERVATION_UNAVAILABLE,
}

# The request verdict of each answer of the recorded-identity rule.
_REQUEST_VERDICTS = {True: CURRENT, False: OTHER, None: UNKNOWN}


def correlate(plan: PlanRead, snapshot: SnapshotRead | NoDocument) -> Correlation:
    """Say what a plan document and its snapshot establish together.

    Compares the identities each read recognised, typed, and never changes
    either read: a conclusion about the plan that the snapshot qualifies
    stays as the plan states it, and the qualification is stated here. Two
    finished records of one execution that disagree on how it ended are both
    shown, and neither is picked. A snapshot that still shows the execution
    running, or finished without a recognised outcome, beside its finalized
    result is lag or incomplete evidence, not disagreement; so is any other
    missing or unrecognised value.

    The reasons of each verdict are judged in this order, the first that
    applies giving the answer:

    - plan and observation: whether there is an attempt to compare (see the
      module), then ``plan_generation_missing``,
      ``attempt_generation_missing``, and ``generations_equal`` or
      ``generations_differ``;
    - finalized and observation: ``plan_not_usable``, ``nothing_finalized``,
      ``finalized_unusable``, whether there is an attempt to compare, then
      ``execution_id_missing``, ``execution_ids_differ``, and
      ``identity_conflict`` (equal IDs whose generations or run tokens are
      both recognised and differ) or ``execution_ids_equal``;
    - request and observation: whether there is an attempt to compare, then
      a difference, ``generations_differ`` or ``run_tokens_differ``, before
      missing evidence, ``plan_generation_missing``,
      ``attempt_generation_missing``, ``plan_run_token_missing`` or
      ``attempt_run_token_missing``, and otherwise
      ``generation_and_run_token_equal``. A difference of either value is
      enough for ``other``, whatever the other value; only both equal make
      it ``current``.

    Pure and silent, and no stored content makes it raise.

    Args:
        plan: The plan's read, from :func:`~yggdrasil.contract.read.read_plan`.
        snapshot: The snapshot's read, from
            :func:`~yggdrasil.contract.read.read_snapshot`; or the NoDocument
            the caller has instead: MISSING or UNAVAILABLE for a snapshot the
            store did not give, LOOKUP_IMPOSSIBLE when the plan's snapshot ID
            cannot be derived.

    Returns:
        Correlation: The three verdicts, any conflicting endings, and the
        problems the pair shows.

    Raises:
        TypeError: If plan is not a PlanRead, or snapshot neither a
            SnapshotRead nor a NoDocument.
        ValueError: If the snapshot was read for another plan, or for
            another realm than the plan records.
    """
    if not isinstance(plan, PlanRead):
        raise TypeError(f"plan must be a PlanRead, not {type(plan).__name__}")
    if not isinstance(snapshot, SnapshotRead | NoDocument):
        raise TypeError(
            "snapshot must be a SnapshotRead or a NoDocument, not "
            f"{type(snapshot).__name__}"
        )
    _require_same_plan(plan, snapshot)
    problems: list[ContractProblem] = []
    observed = observed_attempt(plan, snapshot)
    finalized = finalized_and_observation(plan, observed, problems)
    return Correlation(
        plan=plan,
        snapshot=snapshot,
        plan_and_observation=plan_and_observation(plan, observed),
        finalized_and_observation=finalized,
        request_and_observation=request_and_observation(plan, observed),
        conflicting_endings=conflicting_endings(plan, observed, finalized, problems),
        problems=tuple(problems),
    )


def observed_attempt(
    plan: PlanRead, snapshot: SnapshotRead | NoDocument
) -> Attempt | str:
    """Return the observed attempt to compare with the plan, or why there is none.

    Args:
        plan: The plan's read.
        snapshot: The snapshot's read, or the NoDocument passed instead.

    Returns:
        Attempt | str: The attempt, when the plan is usable and the snapshot
        projects one; otherwise the first reason that applies, in the
        module's order.
    """
    if not plan.usable:
        return PLAN_NOT_USABLE
    if isinstance(snapshot, NoDocument):
        return _UNREAD[snapshot]
    if snapshot.presence != PRESENT:
        # The read of a NoDocument the caller passed instead of a body.
        return _UNREAD[NoDocument(snapshot.presence)]
    version = snapshot.version
    if (
        isinstance(snapshot.document, Mapping)
        and version is not None
        and version.state != versions.SUPPORTED
    ):
        return OBSERVATION_INSPECT_ONLY
    if not snapshot.usable:
        return OBSERVATION_INVALID
    if snapshot.attempt is None or _values.is_word(snapshot.projection, LEGACY):
        return LEGACY_PROJECTION
    return snapshot.attempt


def plan_and_observation(plan: PlanRead, observed: Attempt | str) -> Verdict:
    """Say whether the observed attempt ran the plan's current generation.

    Args:
        plan: The plan's read.
        observed: The observed attempt, or why there is none to compare.

    Returns:
        Verdict: ``same_generation`` or ``different_generation`` when both
        generations are recognised; ``unknown`` with the reason otherwise.
    """
    if isinstance(observed, str):
        return Verdict(UNKNOWN, observed)
    attempt = observed
    if plan.generation is None:
        return Verdict(UNKNOWN, PLAN_GENERATION_MISSING)
    if attempt.plan_generation.value is None:
        return Verdict(UNKNOWN, ATTEMPT_GENERATION_MISSING)
    if plan.generation == attempt.plan_generation.value:
        return Verdict(SAME_GENERATION, GENERATIONS_EQUAL)
    return Verdict(DIFFERENT_GENERATION, GENERATIONS_DIFFER)


def finalized_and_observation(
    plan: PlanRead, observed: Attempt | str, problems: list[ContractProblem]
) -> Verdict:
    """Say whether the plan's finalized record and the observed attempt are one execution.

    One request can have several attempts, so different execution IDs are a
    different execution, not a conflict. Equal IDs whose generations or run
    tokens are both recognised and differ claim no shared identity: that is
    an ``identity_conflict``, reported once per field. A value either record
    lacks is missing evidence, never a conflict.

    Args:
        plan: The plan's read.
        observed: The observed attempt, or why there is none to compare.
        problems: Where an identity conflict is reported.

    Returns:
        Verdict: ``same_execution``, ``different_execution`` or ``unknown``,
        with the reason.
    """
    if not plan.usable:
        return Verdict(UNKNOWN, PLAN_NOT_USABLE)
    record = plan.finalized
    if record is None:
        return Verdict(UNKNOWN, NOTHING_FINALIZED)
    if not record.usable:
        return Verdict(UNKNOWN, FINALIZED_UNUSABLE)
    if isinstance(observed, str):
        return Verdict(UNKNOWN, observed)
    attempt = observed
    if record.execution_id.value is None:
        return Verdict(UNKNOWN, EXECUTION_ID_MISSING)
    if record.execution_id.value != attempt.execution_id:
        return Verdict(DIFFERENT_EXECUTION, EXECUTION_IDS_DIFFER)
    conflicts = _identity_conflicts(record, attempt)
    if conflicts:
        problems.extend(conflicts)
        return Verdict(UNKNOWN, IDENTITY_CONFLICT)
    return Verdict(SAME_EXECUTION, EXECUTION_IDS_EQUAL)


def request_and_observation(plan: PlanRead, observed: Attempt | str) -> Verdict:
    """Say whether the observed attempt belongs to the plan's current request.

    Recorded identity only, under the rule the finalized record's
    ``finalized_is_current`` follows: the plan's generation and its run
    token, valid on its own, against the attempt's. The verdict says nothing
    about the request's validity, completion or eligibility: an attempt of
    the plan's generation and run token is ``current`` even when the plan's
    executed token is invalid.

    Args:
        plan: The plan's read.
        observed: The observed attempt, or why there is none to compare.

    Returns:
        Verdict: ``current`` when both values are recognised on both sides
        and equal, ``other`` when either recognised pair differs,
        ``unknown`` otherwise, with the reason.
    """
    if isinstance(observed, str):
        return Verdict(UNKNOWN, observed)
    plan_token = plan.tokens.run if plan.tokens is not None else None
    generations = (plan.generation, observed.plan_generation.value)
    tokens = (plan_token, observed.run_token.value)
    verdict = _REQUEST_VERDICTS[_requests.identity_verdict((generations, tokens))]
    return Verdict(verdict, _request_reason(generations, tokens))


def conflicting_endings(
    plan: PlanRead,
    observed: Attempt | str,
    finalized: Verdict,
    problems: list[ContractProblem],
) -> ConflictingEndings | None:
    """Return the two endings of one execution when its two terminal claims disagree.

    Only two explicit, usable terminal claims can conflict: the finalized
    record, and the snapshot's attempt in state ``finished`` with a
    recognised outcome, of the same execution without an identity conflict,
    whose outcomes or termination reasons are both recognised and differ.
    A finished attempt whose outcome is absent, ``null`` or unrecognised has
    not said how it ended, so it is incomplete evidence: none of its values
    is compared, its termination reason included. Each differing field is
    reported once.

    Args:
        plan: The plan's read.
        observed: The observed attempt, or why there is none to compare.
        finalized: The finalized-and-observation verdict.
        problems: Where a conflict is reported.

    Returns:
        ConflictingEndings | None: Both endings; None when they do not
        conflict.
    """
    record = plan.finalized
    if (
        finalized.verdict != SAME_EXECUTION
        or record is None
        or isinstance(observed, str)
        or observed.settled is not True
        or observed.outcome.value is None
    ):
        return None
    attempt = observed
    claims = (
        (OUTCOME_FIELD, record.outcome.value, attempt.outcome.value),
        (
            TERMINATION_REASON_FIELD,
            record.termination_reason.value,
            attempt.termination_reason.value,
        ),
    )
    fields: list[str] = []
    for field, claimed, ended in claims:
        if claimed is not None and ended is not None and claimed != ended:
            fields.append(field)
            path = (*_RECORD_PATH, field)
            problems.append(
                _values.problem(
                    CONFLICTING_ENDINGS,
                    path,
                    f"{_values.subject(path)} {_values.shown(claimed)} and the "
                    f"snapshot's finished attempt's {_values.shown(ended)} "
                    "disagree on how the same execution ended",
                )
            )
    if not fields:
        return None
    return ConflictingEndings(
        finalized=Ending(record.outcome.value, record.termination_reason.value),
        observed=Ending(attempt.outcome.value, attempt.termination_reason.value),
        fields=tuple(sorted(fields)),
    )


def _identity_conflicts(
    record: FinalizedRecord, attempt: Attempt
) -> list[ContractProblem]:
    """Return a problem per identity field on which one execution's records differ."""
    conflicts: list[ContractProblem] = []
    for field, recorded, observed in (
        (
            PLAN_GENERATION_FIELD,
            record.plan_generation.value,
            attempt.plan_generation.value,
        ),
        (RUN_TOKEN_FIELD, record.run_token.value, attempt.run_token.value),
    ):
        if recorded is not None and observed is not None and recorded != observed:
            path = (*_RECORD_PATH, field)
            conflicts.append(
                _values.problem(
                    IDENTITY_CONFLICT,
                    path,
                    f"{_values.subject(path)} {_values.shown(recorded)} and the "
                    f"snapshot's attempt's {_values.shown(observed)} differ, for "
                    "one execution ID",
                )
            )
    return conflicts


def _request_reason(
    generations: tuple[str | None, str | None],
    tokens: tuple[int | None, int | None],
) -> str:
    """Return the first reason that applies to a request's recorded identity."""
    plan_generation, attempt_generation = generations
    plan_token, attempt_token = tokens
    if (
        plan_generation is not None
        and attempt_generation is not None
        and plan_generation != attempt_generation
    ):
        return GENERATIONS_DIFFER
    if (
        plan_token is not None
        and attempt_token is not None
        and plan_token != attempt_token
    ):
        return RUN_TOKENS_DIFFER
    if plan_generation is None:
        return PLAN_GENERATION_MISSING
    if attempt_generation is None:
        return ATTEMPT_GENERATION_MISSING
    if plan_token is None:
        return PLAN_RUN_TOKEN_MISSING
    if attempt_token is None:
        return ATTEMPT_RUN_TOKEN_MISSING
    return GENERATION_AND_RUN_TOKEN_EQUAL


def _require_same_plan(plan: PlanRead, snapshot: SnapshotRead | NoDocument) -> None:
    """Refuse a snapshot read for another plan or realm than the plan's.

    Raises:
        ValueError: If the reads are not of one plan.
    """
    if not isinstance(snapshot, SnapshotRead):
        return
    if snapshot.plan_id != plan.plan_id:
        raise ValueError(
            f"the snapshot was read for plan {describe_recorded(snapshot.plan_id)}, "
            f"not {describe_recorded(plan.plan_id)}"
        )
    realm = plan.recorded.realm.value if plan.recorded is not None else None
    if realm is not None and snapshot.realm != realm:
        raise ValueError(
            f"the snapshot was read for realm {describe_recorded(snapshot.realm)}, "
            f"but the plan records realm {describe_recorded(realm)}"
        )
