"""Execution outcome vocabulary and the per-attempt report.

This module owns the words Yggdrasil uses to describe what happened during one
execution attempt. It is deliberately pure data: no engine, no emitter, no
filesystem. The scheduler (:mod:`yggdrasil.core.engine`) records into an
:class:`AttemptReport`; callers and reporting read out of it.

Two distinctions carry most of the weight here:

- **Blocked is not unreached.** A ``blocked`` step had a real predecessor that
  failed or was itself blocked. A step that was simply never evaluated — because
  a fail-fast run aborted, or an attempt was cancelled — has *no* recorded
  outcome at all, and surfaces through :attr:`AttemptReport.unreached_step_ids`.
  They live in different containers precisely so they can never be confused.
- **Attempt-level failure is not step failure.** An unknown failure policy, a
  dependency cycle, or a broken event spool is not attributable to any step.
  Those are recorded in :attr:`AttemptReport.diagnostic`, so nothing ever has to
  invent a failed step to describe them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from yggdrasil.flow.errors import OrchestrationError
from yggdrasil.flow.utils.ygg_time import utcnow_iso


class StepOutcome(str, Enum):
    """Terminal outcome of one step within one execution attempt.

    These four values are the complete vocabulary for a *drained* attempt. They
    are ``str``-valued so they serialize into events and documents as plain
    strings without any custom encoder.

    Non-terminal scheduler bookkeeping (``pending``/``running``) is deliberately
    absent: it belongs to the scheduler's internal state, not to a report. Work
    that was never reached is represented by the *absence* of an outcome, via
    :attr:`AttemptReport.unreached_step_ids`.
    """

    SUCCEEDED = "succeeded"
    REUSED = "reused"
    FAILED = "failed"
    BLOCKED = "blocked"

    def satisfies_dependency(self) -> bool:
        """Whether a successor depending on this step may become runnable.

        Encodes the "Satisfies a success dependency?" column of the PRD's step
        outcome table: every ``deps`` entry is a success prerequisite, so only
        a step that actually succeeded or was validly reused releases its
        dependents.

        Returns:
            bool: True for SUCCEEDED and REUSED, False for FAILED and BLOCKED.
        """
        return self in (StepOutcome.SUCCEEDED, StepOutcome.REUSED)


class ExecutionOutcome(str, Enum):
    """Overall outcome of one execution attempt.

    Deliberately binary. Counts of healthy branches may accompany a failure, but
    there is no partially-successful outcome: finishing healthy branches does not
    make a plan successful.
    """

    SUCCEEDED = "succeeded"
    FAILED = "failed"


class TerminationReason(str, Enum):
    """How an execution attempt ended.

    This is what distinguishes "finished, with failures" from "stopped before
    finishing" — a distinction later phases depend on when deciding whether an
    execution request is complete.

    Attributes:
        COMPLETED: Drained normally. Every planned step reached a terminal
            outcome. The attempt may still have *failed* overall.
        FAILED_FAST: A fail-fast attempt stopped at its first step failure.
            Work after that point was never reached.
        CANCELLED: Stopped by an external cancellation signal. Not an ordinary
            failure, and not a completed request.
        PREFLIGHT_REJECTED: The plan was rejected before any step ran.
        ORCHESTRATION_ERROR: Yggdrasil's own infrastructure failed, so the
            attempt could not be tracked or reported reliably.
    """

    COMPLETED = "completed"
    FAILED_FAST = "failed_fast"
    CANCELLED = "cancelled"
    PREFLIGHT_REJECTED = "preflight_rejected"
    ORCHESTRATION_ERROR = "orchestration_error"


@dataclass
class StepFailure:
    """Diagnostics for one step that was attempted and failed.

    Mirrors the payload the ``@step`` wrapper already publishes on
    ``step.failed``, so reporting and the event spool describe a failure the
    same way.

    Attributes:
        step_id: The step this failure belongs to.
        error: Human-readable error message.
        kind: "permanent" or "transient", matching the emitted event's field.
        code: Optional machine-readable code from StepError.
        advice: Optional operator guidance from StepError.
        error_type: Exception class name, when the failure came from one. The
            event payload carries only ``str(exc)``, which for many exceptions
            (``KeyError('x')`` renders as ``'x'``) loses what went wrong.
    """

    step_id: str
    error: str
    kind: str = "permanent"
    code: str | None = None
    advice: str | None = None
    error_type: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation.

        Returns:
            dict: The failure's fields as plain JSON types.
        """
        return {
            "step_id": self.step_id,
            "error": self.error,
            "kind": self.kind,
            "code": self.code,
            "advice": self.advice,
            "error_type": self.error_type,
        }


@dataclass
class AttemptDiagnostic:
    """An attempt-level failure not attributable to any single step.

    Exists so that an unknown failure policy, a dependency cycle, a plan-file
    write failure, or a broken event spool can be reported without inventing a
    failed step to hang them on. Its *classification* is not duplicated here —
    that is :attr:`AttemptReport.termination_reason`, so the two cannot disagree.

    Attributes:
        message: Human-readable description of what went wrong.
        error_type: Exception class name, when the diagnostic came from one.
        details: Optional structured context (e.g. the offending step IDs).
    """

    message: str
    error_type: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_exception(
        cls, exc: BaseException, *, details: dict[str, Any] | None = None
    ) -> AttemptDiagnostic:
        """Build a diagnostic from an exception.

        Args:
            exc: The exception that ended the attempt.
            details: Optional structured context to attach.

        Returns:
            AttemptDiagnostic: Diagnostic carrying the message and type of exc.
        """
        return cls(
            message=str(exc),
            error_type=type(exc).__name__,
            details=dict(details or {}),
        )

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation.

        Returns:
            dict: The diagnostic's fields as plain JSON types.
        """
        return {
            "message": self.message,
            "error_type": self.error_type,
            "details": self.details,
        }


@dataclass
class AttemptReport:
    """Mutable record of what one execution attempt did.

    Owned by exactly one attempt, via :class:`~yggdrasil.flow.attempt.AttemptContext`,
    and never stored on the engine. The attempt records into it as it goes, so
    the outcomes determined before an exception remain readable afterwards — a
    return value cannot carry information out of an attempt that ends by raising,
    and fail-fast failure, cancellation, preflight rejection and orchestration
    failure all end that way.

    The captured identity fields are set once at construction and are not
    modified as outcomes accumulate: a report always describes the request it
    was opened for.

    Attributes:
        execution_id: Identifier for this attempt.
        plan_id: The plan being executed.
        step_ids: The planned step inventory, in original plan order. This is
            what makes "never reached" computable rather than guessed.
        plan_generation: Captured plan generation, when the caller has one.
        run_token: Captured execution request token, when the caller has one.
        failure_policy: The policy this attempt ran under.
        started_at: ISO-8601 UTC start timestamp.
        ended_at: ISO-8601 UTC end timestamp; None until finished.
        termination_reason: How the attempt ended; None while it is running.
        step_outcomes: Terminal outcome per step. Steps absent from this mapping
            were never reached.
        failures: Diagnostics for steps that were attempted and failed.
        direct_blockers: Per blocked step, the immediate failed/blocked
            predecessors that blocked it.
        failed_ancestors: Per blocked step, every originating failed step
            upstream of it.
        diagnostic: Attempt-level failure, if any.
        publication_failure: Why this closed report could not be published, if
            it could not; None once it was published or while it is open. See
            :meth:`record_publication_failure`.
    """

    execution_id: str
    plan_id: str
    step_ids: list[str] = field(default_factory=list)
    plan_generation: str | None = None
    run_token: int | None = None
    failure_policy: str = "fail_fast"
    started_at: str = field(default_factory=utcnow_iso)
    ended_at: str | None = None
    termination_reason: TerminationReason | None = None
    step_outcomes: dict[str, StepOutcome] = field(default_factory=dict)
    failures: dict[str, StepFailure] = field(default_factory=dict)
    direct_blockers: dict[str, list[str]] = field(default_factory=dict)
    failed_ancestors: dict[str, list[str]] = field(default_factory=dict)
    diagnostic: AttemptDiagnostic | None = None
    publication_failure: AttemptDiagnostic | None = None

    # ----- recording -----

    def _check_recordable(self, step_id: str, outcome: StepOutcome) -> None:
        """Reject a write that would make the report contradict itself.

        Two things a report must never be able to say: that a step it never
        planned reached an outcome, or that one step reached two different
        terminal outcomes. Both would make the recorded breakdown disagree with
        the plan or with itself, and a report that disagrees with itself cannot
        be used to decide whether an execution request is finished.

        Each step is invoked or reused at most once per attempt, so a second,
        different outcome is a scheduler invariant violation rather than a plan
        defect. Re-recording the same outcome is harmless and allowed.

        Args:
            step_id: The step being recorded.
            outcome: The terminal outcome being recorded for it.

        Raises:
            OrchestrationError: If step_id is outside the planned inventory, or
                already carries a different terminal outcome.
        """
        if step_id not in self.step_ids:
            raise OrchestrationError(
                f"Cannot record an outcome for step '{step_id}' in attempt "
                f"'{self.execution_id}': it is not in plan '{self.plan_id}'s "
                f"step inventory {self.step_ids}."
            )
        recorded = self.step_outcomes.get(step_id)
        if recorded is not None and recorded is not outcome:
            raise OrchestrationError(
                f"Conflicting outcome for step '{step_id}' in attempt "
                f"'{self.execution_id}': already recorded as "
                f"'{recorded.value}', cannot re-record as '{outcome.value}'."
            )

    def record_outcome(self, step_id: str, outcome: StepOutcome) -> None:
        """Record a step's terminal outcome.

        Args:
            step_id: The step that reached a terminal state.
            outcome: Its terminal outcome.

        Raises:
            OrchestrationError: If the step is outside the planned inventory or
                already carries a different terminal outcome.
        """
        self._check_recordable(step_id, outcome)
        self.step_outcomes[step_id] = outcome

    def record_failure(self, failure: StepFailure) -> None:
        """Record a failed step's outcome and diagnostics together.

        Kept as one operation so an attempt cannot record a failure without its
        outcome, or an outcome without its diagnostics.

        Args:
            failure: The failure detail; its step_id identifies the step.

        Raises:
            OrchestrationError: If the step is outside the planned inventory or
                already carries a different terminal outcome.
        """
        self._check_recordable(failure.step_id, StepOutcome.FAILED)
        self.step_outcomes[failure.step_id] = StepOutcome.FAILED
        self.failures[failure.step_id] = failure

    def record_blocked(self, step_id: str, direct_blockers: list[str]) -> None:
        """Record a blocked step's outcome and its immediate blockers together.

        Args:
            step_id: The step that could not run.
            direct_blockers: The failed or blocked predecessors responsible.

        Raises:
            OrchestrationError: If the step is outside the planned inventory or
                already carries a different terminal outcome.
        """
        self._check_recordable(step_id, StepOutcome.BLOCKED)
        self.step_outcomes[step_id] = StepOutcome.BLOCKED
        self.direct_blockers[step_id] = list(direct_blockers)

    def record_failed_ancestors(self, step_id: str, ancestors: list[str]) -> None:
        """Record every originating failure upstream of a blocked step.

        Written by the final blocker-diagnostics pass, once every failure in the
        attempt is known: a join blocked by one failed prerequisite must report
        every failed ancestor, including ones that failed after it was first
        marked blocked.

        Args:
            step_id: The blocked step these ancestors explain.
            ancestors: Originating failed step IDs upstream of it.

        Raises:
            OrchestrationError: If the step is outside the planned inventory, or
                is not recorded as blocked - failed ancestors explain blocking,
                so attaching them to anything else is a contradiction.
        """
        if step_id not in self.step_ids:
            raise OrchestrationError(
                f"Cannot record failed ancestors for step '{step_id}' in attempt "
                f"'{self.execution_id}': it is not in plan '{self.plan_id}'s "
                f"step inventory {self.step_ids}."
            )
        recorded = self.step_outcomes.get(step_id)
        if recorded is not StepOutcome.BLOCKED:
            raise OrchestrationError(
                f"Cannot record failed ancestors for step '{step_id}' in attempt "
                f"'{self.execution_id}': it is recorded as "
                f"'{recorded.value if recorded else 'unreached'}', not blocked."
            )
        self.failed_ancestors[step_id] = list(ancestors)

    def record_diagnostic(self, diagnostic: AttemptDiagnostic) -> None:
        """Record an attempt-level failure.

        Args:
            diagnostic: The attempt-level failure detail.
        """
        self.diagnostic = diagnostic

    def finish(self, reason: TerminationReason, *, ended_at: str | None = None) -> None:
        """Close the report with how the attempt ended.

        Called exactly once, on the attempt's single common exit path.

        ``COMPLETED`` asserts that the attempt drained, so it requires every
        planned step to carry a terminal outcome. A report that claims normal
        completion while steps have no recorded outcome cannot be published
        truthfully, which is an orchestration failure rather than a plan defect.

        Args:
            reason: How the attempt ended.
            ended_at: ISO-8601 UTC end timestamp; defaults to now.

        Raises:
            OrchestrationError: If reason is COMPLETED while planned steps have
                no recorded outcome, an attempt-level failure was recorded, or a
                recorded step failure disagrees with that step's outcome.
        """
        if reason is TerminationReason.COMPLETED:
            missing = self.unreached_step_ids
            if missing:
                raise OrchestrationError(
                    f"Cannot finish attempt '{self.execution_id}' for plan "
                    f"'{self.plan_id}' as COMPLETED: {len(missing)} planned "
                    f"step(s) have no recorded outcome: {missing}"
                )
            if self.diagnostic is not None:
                # An attempt-level failure means the attempt did not complete
                # normally. Finish with the reason that actually describes it,
                # rather than letting a failure sit inside a normal completion.
                raise OrchestrationError(
                    f"Cannot finish attempt '{self.execution_id}' for plan "
                    f"'{self.plan_id}' as COMPLETED: an attempt-level failure "
                    f"was recorded ({self.diagnostic.message!r})."
                )
            contradicted = sorted(
                step_id
                for step_id in self.failures
                if self.step_outcomes.get(step_id) is not StepOutcome.FAILED
            )
            if contradicted:
                raise OrchestrationError(
                    f"Cannot finish attempt '{self.execution_id}' for plan "
                    f"'{self.plan_id}' as COMPLETED: step(s) {contradicted} "
                    f"carry a recorded failure but are not recorded as failed."
                )
        self.termination_reason = reason
        self.ended_at = ended_at or utcnow_iso()

    def record_publication_failure(self, diagnostic: AttemptDiagnostic) -> None:
        """Record that this closed report could not be published.

        A report is published only after it is closed, because the published
        report has to say how the attempt ended. If publication then fails, the
        attempt was not tracked reliably after all, and the report must stop
        claiming the ending it was closed with. Otherwise the caller would hold
        an orchestration failure next to a report that says the attempt drained.

        - The termination reason becomes ORCHESTRATION_ERROR. The reason it
          replaces is kept as ``superseded_termination_reason`` in the recorded
          diagnostic's details.
        - A CANCELLED ending is kept as it is: the attempt was interrupted, and
          that remains what callers must act on. The publication failure is
          recorded alongside it.
        - Step outcomes, failures, blocker diagnostics and the attempt-level
          ``diagnostic`` are left untouched, so the original ending stays
          readable as context.

        Args:
            diagnostic: What prevented publication.

        Raises:
            OrchestrationError: If the report is not closed yet, or a publication
                failure was already recorded for it.
        """
        reason = self.termination_reason
        if reason is None:
            raise OrchestrationError(
                f"Cannot record a publication failure for attempt "
                f"'{self.execution_id}': its report is not closed yet."
            )
        if self.publication_failure is not None:
            raise OrchestrationError(
                f"Cannot record a second publication failure for attempt "
                f"'{self.execution_id}'."
            )
        details = dict(diagnostic.details)
        if reason not in (
            TerminationReason.CANCELLED,
            TerminationReason.ORCHESTRATION_ERROR,
        ):
            details["superseded_termination_reason"] = reason.value
            self.termination_reason = TerminationReason.ORCHESTRATION_ERROR
        self.publication_failure = AttemptDiagnostic(
            message=diagnostic.message,
            error_type=diagnostic.error_type,
            details=details,
        )

    # ----- derived views -----

    @property
    def unreached_step_ids(self) -> list[str]:
        """Planned steps that were never evaluated, in original plan order.

        These are distinct from blocked steps: a blocked step had a real failed
        or blocked predecessor, while these were simply never reached — because
        a fail-fast run aborted, the attempt was cancelled, or it is still
        running.

        Returns:
            list[str]: Step IDs with no recorded outcome.
        """
        return [sid for sid in self.step_ids if sid not in self.step_outcomes]

    @property
    def is_finished(self) -> bool:
        """Whether the attempt has ended, however it ended.

        Returns:
            bool: True once finish() has been called.
        """
        return self.termination_reason is not None

    @property
    def is_drained(self) -> bool:
        """Whether the attempt completed normally with full inventory coverage.

        Both halves are required. A fail-fast failure on the *final* step covers
        every planned step but did not complete normally; a rejected empty plan
        completes its inventory trivially but did not run. Neither drained.

        Returns:
            bool: True only for a normally completed attempt in which every
            planned step reached a terminal outcome.
        """
        return (
            self.termination_reason is TerminationReason.COMPLETED
            and not self.unreached_step_ids
        )

    @property
    def outcome(self) -> ExecutionOutcome | None:
        """Overall outcome, or None while the attempt is unfinished.

        Derived rather than stored, so a report can never claim success while
        carrying a failure. None until the attempt is finished: an unfinished
        report must not present itself as an established terminal failure.

        Returns:
            ExecutionOutcome | None: SUCCEEDED only for a drained attempt in
            which every step succeeded or was validly reused; FAILED for any
            other finished attempt; None if unfinished.
        """
        if not self.is_finished:
            return None
        if self.is_drained and all(
            outcome.satisfies_dependency() for outcome in self.step_outcomes.values()
        ):
            return ExecutionOutcome.SUCCEEDED
        return ExecutionOutcome.FAILED

    @property
    def counts(self) -> dict[str, int]:
        """Number of steps per terminal outcome, plus unreached work.

        Returns:
            dict[str, int]: One entry per StepOutcome value and an "unreached"
            entry, so the breakdown always sums to the planned inventory.
        """
        tally = {outcome.value: 0 for outcome in StepOutcome}
        for outcome in self.step_outcomes.values():
            tally[outcome.value] += 1
        tally["unreached"] = len(self.unreached_step_ids)
        return tally

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation of the report.

        Returns:
            dict: All recorded and derived fields as plain JSON types.
        """
        overall = self.outcome
        reason = self.termination_reason
        return {
            "execution_id": self.execution_id,
            "plan_id": self.plan_id,
            "plan_generation": self.plan_generation,
            "run_token": self.run_token,
            "failure_policy": self.failure_policy,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "termination_reason": reason.value if reason else None,
            "outcome": overall.value if overall else None,
            "is_drained": self.is_drained,
            "step_ids": list(self.step_ids),
            "step_outcomes": {
                sid: outcome.value for sid, outcome in self.step_outcomes.items()
            },
            "unreached_step_ids": self.unreached_step_ids,
            "failures": {
                sid: failure.to_dict() for sid, failure in self.failures.items()
            },
            "direct_blockers": {
                sid: list(blockers) for sid, blockers in self.direct_blockers.items()
            },
            "failed_ancestors": {
                sid: list(ancestors) for sid, ancestors in self.failed_ancestors.items()
            },
            "diagnostic": self.diagnostic.to_dict() if self.diagnostic else None,
            "publication_failure": (
                self.publication_failure.to_dict() if self.publication_failure else None
            ),
            "counts": self.counts,
        }
