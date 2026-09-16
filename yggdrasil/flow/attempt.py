"""Per-attempt execution context.

One :class:`AttemptContext` belongs to exactly one execution attempt. The caller
constructs it, passes it into the engine, and reads it afterwards — including
after the attempt ends by raising.

That last point is the reason this type exists. A return value cannot carry
information out of an attempt that raises, and fail-fast failure, cancellation,
preflight rejection and orchestration failure all end that way. A
caller-owned context is still readable; a return value that never arrives is not.

Because the caller owns it, nothing per-attempt has to live on the ``Engine``
instance. Engine instances are long-lived and shared across plans, so mutable
attempt state on the engine would leak between attempts.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from yggdrasil.flow.model import Plan
from yggdrasil.flow.outcomes import AttemptReport


@dataclass
class AttemptContext:
    """Captured identity, cancellation signal, and mutable report for one attempt.

    Attributes:
        report: The attempt's mutable record of outcomes and diagnostics. Also
            the carrier of the captured identity, so there is no second copy to
            drift out of sync.
        cancel_event: Cooperative cancellation signal. A ``threading.Event``
            rather than an ``asyncio.Event`` because the engine runs in a worker
            thread (via ``asyncio.to_thread``) while the caller that signals
            cancellation runs on the event loop.
        execution_authority: "daemon" or "run_once" — who is permitted to
            execute and finalize this request.
        execution_owner: Unique owner token for run_once isolation, if any.
    """

    report: AttemptReport
    cancel_event: threading.Event = field(default_factory=threading.Event)
    execution_authority: str = "daemon"
    execution_owner: str | None = None

    @classmethod
    def for_plan(
        cls,
        plan: Plan,
        *,
        execution_id: str,
        plan_generation: str | None = None,
        run_token: int | None = None,
        execution_authority: str = "daemon",
        execution_owner: str | None = None,
        cancel_event: threading.Event | None = None,
    ) -> AttemptContext:
        """Open a context for one attempt at ``plan``.

        Seeds the report's planned step inventory from the plan, so no caller can
        forget it — without that inventory, "never reached" is not computable.

        ``execution_id`` is required rather than generated here: allocating it is
        an operational concern (ordering attempts across restarts), and inventing
        a local identifier would create an identity that operational callers do
        not recognize.

        Args:
            plan: The plan to be attempted.
            execution_id: Identifier allocated for this attempt by its caller.
            plan_generation: Captured plan generation, when the caller has one.
            run_token: Captured execution request token, when the caller has one.
            execution_authority: "daemon" (default) or "run_once".
            execution_owner: Unique owner token for run_once isolation.
            cancel_event: Existing cancellation signal to share; a fresh unset
                event is created when omitted.

        Returns:
            AttemptContext: A context whose report is open and empty.
        """
        report = AttemptReport(
            execution_id=execution_id,
            plan_id=plan.plan_id,
            step_ids=[spec.step_id for spec in plan.steps],
            plan_generation=plan_generation,
            run_token=run_token,
            failure_policy=plan.failure_policy,
        )
        return cls(
            report=report,
            cancel_event=cancel_event or threading.Event(),
            execution_authority=execution_authority,
            execution_owner=execution_owner,
        )

    # ----- captured identity (read-through to the report) -----

    @property
    def execution_id(self) -> str:
        """Identifier of this attempt."""
        return self.report.execution_id

    @property
    def plan_id(self) -> str:
        """The plan this attempt is executing."""
        return self.report.plan_id

    @property
    def plan_generation(self) -> str | None:
        """Captured plan generation, if the caller supplied one."""
        return self.report.plan_generation

    @property
    def run_token(self) -> int | None:
        """Captured execution request token, if the caller supplied one."""
        return self.report.run_token

    # ----- cancellation -----

    @property
    def cancellation_requested(self) -> bool:
        """Whether cancellation has been signaled.

        Read between steps, never mid-step: cancellation stops the scheduler from
        starting further work, and does not interrupt author code already running.

        Returns:
            bool: True once request_cancellation() has been called.
        """
        return self.cancel_event.is_set()

    def request_cancellation(self) -> None:
        """Signal that the attempt should stop starting new work.

        Safe to call from a different thread than the one running the attempt,
        and safe to call more than once.
        """
        self.cancel_event.set()
