"""Dependency-driven readiness for one execution attempt.

The engine's scheduler decides *which* step of a plan may run next; it never
runs anything. Keeping it free of I/O is deliberate: graph readiness and
failure containment are the invariants most worth reviewing in isolation, and a
pure state machine can be exercised over every graph shape without an engine,
an emitter, or a filesystem.

Every ``StepSpec.deps`` entry is a *success* prerequisite. A step becomes
runnable once each of its dependencies has succeeded or been validly reused in
this attempt. Among runnable steps the one earliest in the original plan list
goes first, so execution order is deterministic and a correctly ordered legacy
plan keeps exactly its existing order.

Terminal outcomes are not stored here. They are recorded straight into the
attempt's :class:`~yggdrasil.flow.outcomes.AttemptReport`, so there is one
record of what happened rather than two that could disagree. This class keeps
only the bookkeeping the report has no use for: which dependencies are still
outstanding, which steps are runnable, and which one is running.

Engine-internal: constructed fresh for every attempt and never shared.
"""

from __future__ import annotations

import heapq
from collections import deque
from collections.abc import Sequence

from yggdrasil.flow.errors import OrchestrationError
from yggdrasil.flow.model import StepSpec
from yggdrasil.flow.outcomes import AttemptReport, StepFailure, StepOutcome

_UNSATISFIED = (StepOutcome.FAILED, StepOutcome.BLOCKED)


class DependencyScheduler:
    """Readiness state for the steps of one execution attempt.

    Assumes a graph that preflight has already validated: unique step IDs,
    known dependencies, no self-dependencies and no cycles. The scheduler does
    not re-validate, but it does not trust validation blindly either — a graph
    that cannot make progress is reported by :meth:`ensure_drained` as an
    engine invariant failure instead of passing for a finished attempt.

    Execution is sequential: at most one step runs at a time, and each step is
    started at most once.
    """

    def __init__(self, steps: Sequence[StepSpec], report: AttemptReport) -> None:
        """Build readiness state for an attempt at ``steps``.

        Args:
            steps: The plan's steps, in original plan order.
            report: The attempt's report, which receives every terminal outcome.
        """
        self._report = report
        self._steps: list[StepSpec] = list(steps)
        self._position: dict[str, int] = {
            spec.step_id: index for index, spec in enumerate(self._steps)
        }
        # Distinct outstanding prerequisites per step. A set, so a dependency
        # listed twice is still satisfied by a single success.
        self._remaining: dict[str, set[str]] = {
            spec.step_id: set(spec.deps) for spec in self._steps
        }
        # Built by iterating in plan order, so each successor list is too.
        self._successors: dict[str, list[str]] = {
            spec.step_id: [] for spec in self._steps
        }
        for spec in self._steps:
            for dep in dict.fromkeys(spec.deps):
                self._successors[dep].append(spec.step_id)
        # Min-heap of plan positions: the earliest runnable step pops first.
        self._runnable: list[int] = [
            index
            for index, spec in enumerate(self._steps)
            if not self._remaining[spec.step_id]
        ]
        heapq.heapify(self._runnable)
        self._running: str | None = None

    @property
    def running_step_id(self) -> str | None:
        """The step started but not yet resolved, if any.

        Returns:
            str | None: The running step's ID, or None between steps.
        """
        return self._running

    def has_runnable(self) -> bool:
        """Whether some step's prerequisites are all satisfied and it has not started.

        Returns:
            bool: True if :meth:`start_next` has a step to hand out.
        """
        return bool(self._runnable)

    def start_next(self) -> StepSpec:
        """Hand out the runnable step earliest in the plan and mark it running.

        Returns:
            StepSpec: The step to reuse or execute next.

        Raises:
            OrchestrationError: If another step is still running, or no step is
                runnable. Either means the caller broke the scheduling protocol.
        """
        if self._running is not None:
            raise OrchestrationError(
                f"Scheduler invariant violated in plan '{self._report.plan_id}': "
                f"cannot start another step while '{self._running}' is running."
            )
        if not self._runnable:
            raise OrchestrationError(
                f"Scheduler invariant violated in plan '{self._report.plan_id}': "
                f"no step is runnable."
            )
        spec = self._steps[heapq.heappop(self._runnable)]
        self._running = spec.step_id
        return spec

    def record_success(self, step_id: str, outcome: StepOutcome) -> None:
        """Resolve the running step as succeeded or reused, admitting its successors.

        A successor becomes runnable once this was its last outstanding
        prerequisite.

        Args:
            step_id: The running step.
            outcome: SUCCEEDED or REUSED.

        Raises:
            OrchestrationError: If ``step_id`` is not the running step, the
                outcome does not satisfy a dependency, or the report rejects it.
        """
        if not outcome.satisfies_dependency():
            raise OrchestrationError(
                f"Scheduler invariant violated in plan '{self._report.plan_id}': "
                f"'{outcome.value}' is not a successful outcome for step '{step_id}'."
            )
        self._require_running(step_id)
        self._report.record_outcome(step_id, outcome)
        self._running = None

        for successor in self._successors[step_id]:
            remaining = self._remaining[successor]
            remaining.discard(step_id)
            if not remaining:
                heapq.heappush(self._runnable, self._position[successor])

    def record_failure(self, failure: StepFailure) -> None:
        """Resolve the running step as failed.

        Admits no successors. Whether its dependents are blocked or simply left
        unreached is the failure policy's decision, not the scheduler's; see
        :meth:`block_dependents`.

        Args:
            failure: The failure detail; its step_id must be the running step.

        Raises:
            OrchestrationError: If the failure is not for the running step, or
                the report rejects it.
        """
        self._require_running(failure.step_id)
        self._report.record_failure(failure)
        self._running = None

    def block_dependents(self, step_id: str) -> list[str]:
        """Block every step that transitively requires a failed step's success.

        Blocking is propagated through the whole descendant set at once. Every
        descendant of a failed step has that failure somewhere on a dependency
        path, so none of them can ever run in this attempt; marking them all
        now reaches the same end state as discovering them one level at a time,
        and keeps a report taken mid-attempt (after a cancellation, say)
        independent of exactly when it was taken.

        A descendant already blocked by an earlier failure is left as it is.
        Its complete blocker diagnostics are settled by
        :meth:`record_blocker_diagnostics`.

        Args:
            step_id: A step recorded as failed or blocked.

        Returns:
            list[str]: The newly blocked step IDs, in plan order.

        Raises:
            OrchestrationError: If ``step_id`` is not recorded as failed or
                blocked, or the report rejects a block.
        """
        outcomes = self._report.step_outcomes
        if outcomes.get(step_id) not in _UNSATISFIED:
            raise OrchestrationError(
                f"Scheduler invariant violated in plan '{self._report.plan_id}': "
                f"cannot block the dependents of step '{step_id}', which has not "
                f"failed or been blocked."
            )

        newly_blocked: list[str] = []
        frontier = deque([step_id])
        while frontier:
            for successor in self._successors[frontier.popleft()]:
                if successor in outcomes:
                    continue  # already blocked through another failed path
                self._report.record_blocked(successor, self._direct_blockers(successor))
                newly_blocked.append(successor)
                frontier.append(successor)
        return sorted(newly_blocked, key=self._position.__getitem__)

    def ensure_drained(self) -> None:
        """Verify that every step reached a terminal outcome.

        Called once nothing is runnable. In a validated graph every unresolved
        step is waiting on something that will itself resolve, so running out
        of runnable work with steps still unresolved cannot happen — if it
        does, it is an engine bug and must not pass for a completed attempt.

        Raises:
            OrchestrationError: If a step is still running or unresolved.
        """
        if self._running is not None:
            raise OrchestrationError(
                f"Scheduler invariant violated in plan '{self._report.plan_id}': "
                f"step '{self._running}' is still running."
            )
        outcomes = self._report.step_outcomes
        unresolved = [
            spec.step_id for spec in self._steps if spec.step_id not in outcomes
        ]
        if unresolved:
            raise OrchestrationError(
                f"Scheduler invariant violated in plan '{self._report.plan_id}': "
                f"{len(unresolved)} step(s) remain unresolved with no possible "
                f"progress: {unresolved}. A validated graph cannot stall, so this "
                f"is an engine defect rather than a plan defect."
            )

    def record_blocker_diagnostics(self) -> None:
        """Settle the complete blocker diagnostics of every blocked step.

        A final pass over the whole graph, run once no more failures can occur.
        Doing this at the end is what makes it correct for joins: a join blocked
        when its first prerequisite failed must still name a second prerequisite
        that failed later. Maintaining that incrementally during scheduling is
        easy to get subtly wrong, so it is not attempted.

        For each blocked step this records, in plan order:

        - direct blockers: every dependency that failed or was blocked;
        - failed ancestors: every failed step that reaches it through a chain
          of blocked steps.

        Safe to call more than once; each call recomputes the same values.

        Raises:
            OrchestrationError: If the report rejects a diagnostic.
        """
        outcomes = self._report.step_outcomes
        ancestors: dict[str, set[str]] = {}
        for spec in self._steps:
            if outcomes.get(spec.step_id) is not StepOutcome.FAILED:
                continue
            reached: set[str] = set()
            frontier = deque([spec.step_id])
            while frontier:
                for successor in self._successors[frontier.popleft()]:
                    if successor in reached:
                        continue
                    if outcomes.get(successor) is not StepOutcome.BLOCKED:
                        continue
                    reached.add(successor)
                    ancestors.setdefault(successor, set()).add(spec.step_id)
                    frontier.append(successor)

        for spec in self._steps:
            if outcomes.get(spec.step_id) is not StepOutcome.BLOCKED:
                continue
            self._report.record_blocked(
                spec.step_id, self._direct_blockers(spec.step_id)
            )
            self._report.record_failed_ancestors(
                spec.step_id,
                sorted(ancestors.get(spec.step_id, ()), key=self._position.__getitem__),
            )

    # ----- helpers -----

    def _require_running(self, step_id: str) -> None:
        """Reject resolving a step that is not the one currently running.

        Args:
            step_id: The step the caller is resolving.

        Raises:
            OrchestrationError: If ``step_id`` is not the running step.
        """
        if self._running != step_id:
            raise OrchestrationError(
                f"Scheduler invariant violated in plan '{self._report.plan_id}': "
                f"cannot resolve step '{step_id}' while the running step is "
                f"{self._running!r}."
            )

    def _direct_blockers(self, step_id: str) -> list[str]:
        """Return a step's dependencies that are currently failed or blocked.

        Args:
            step_id: The step whose dependencies are inspected.

        Returns:
            list[str]: The unsatisfied dependency IDs, in plan order.
        """
        outcomes = self._report.step_outcomes
        spec = self._steps[self._position[step_id]]
        return sorted(
            (dep for dep in set(spec.deps) if outcomes.get(dep) in _UNSATISFIED),
            key=self._position.__getitem__,
        )
