"""Coordinate plan execution for the daemon and run-once callers.

Both callers hand a possibly-ready plan to one :class:`PlanExecutionCoordinator`,
which owns everything between "this plan may be eligible" and "its result is
recorded":

1. **Admission from one snapshot.** The current plan document is read once,
   under the plan's in-flight guard; a legacy document gets its first
   ``plan_generation`` in that same read. Eligibility, execution authority and
   owner, the executable plan, its failure policy, generation and run token all
   come from that one read, never from the watcher event that prompted it, so a
   stale event cannot combine an old approval or token with a newer plan.
2. **Execution.** The attempt runs in a worker thread through
   ``Engine._run_attempt``, with an AttemptContext the coordinator owns, so the
   attempt's report is readable however the attempt ends.
3. **Interpretation.** The report's termination reason decides whether the
   request finished (see ``finishes_request`` in
   ``lib/storage/plan_updates.py``); the type of any exception does not, since
   a step body can raise any exception type itself. A request is consumed when
   an attempt succeeds, when a ``continue_independent`` attempt drains with
   failures, and when preflight rejects a ``continue_independent`` plan. Any
   other ending leaves the token untouched, so the plan stays eligible.
4. **Finalization.** A finished request is recorded through the plan store's
   generation-safe finalizer, with a bounded number of attempts.

Exclusion:
    At most one attempt per plan document is in flight in this process. The
    guard is held from admission until the result is recorded or retained as
    pending, not merely until the worker returns, so a duplicate request that
    arrives while a result is being finalized cannot start a second attempt.
    Such a request is not dropped either: once the active attempt is done, the
    plan is checked again from a fresh snapshot. That check runs a newer
    request, finds an already-served one ineligible, and does not run the
    request just attempted a second time, even when that attempt left it
    eligible: the duplicate arrived while that very request was running. Other
    processes and hosts are not covered; ``DaemonLock`` keeps to one daemon per
    mode on a host.

Cancellation:
    Cancelling the task that awaits a worker thread does not stop the worker,
    and says nothing about whether it has finished. Each plan's coordination
    therefore runs as a task of its own, held here, which callers await through
    ``asyncio.shield`` and which shields every blocking call it makes.
    Cancellation, whether of a caller or of the coordination task itself (as
    ``asyncio.run`` does to every task left at shutdown), never unwinds it. It
    asks the engine to stop starting new steps, keeps waiting for the worker,
    and then interprets and records whatever the attempt reached. An attempt
    interrupted before it drained leaves its request unconsumed; one that
    drained is recorded like any other. A task cancelled before it ever ran
    releases its plan through its completion callback instead, since its body
    never runs. A CancelledError that a worker raises itself, from a step's own
    asynchronous work say, cancels nothing: it ends that attempt as cancelled.

Finalization retries:
    Each attempt is one call to ``PlanStore.finalize_execution``, which rereads
    the plan and rechecks generation, token, execution and authority before it
    writes, so retrying never resends a stale document. A revision conflict and
    a retryable ``PlanStoreError`` are retried, up to three attempts in all,
    0.5 s and then 1 s apart. The waiting happens on the event loop, never in a
    worker thread. A recorded result, a supersession and a non-retryable failure
    end the retries at once. Each backend bounds its own calls: a CouchDB
    request times out after 60 s, and SQLite waits at most 5 s for a lock.

    When no attempt records the result, it is kept in memory as a pending
    finalization, and that is established before the plan's guard is released.
    While it is pending, the plan is never executed again. A change of
    generation or authority, or the result turning out to be recorded after
    all, is recognized by reading the plan alone, and ends the pending state
    without writing anything. A newer run token in the same generation retries
    the pending finalization first, with one more bounded cycle, and runs the
    newer request only once the old result is recorded or definitively
    rejected. Each newer run token buys one such cycle: repeated requests, for
    the pending run token or for one that already had its cycle, change
    nothing and retry nothing. A pending result does not survive a restart;
    the request is then still eligible, so it may run again, and realm
    idempotency remains the protection against repeated side effects.
"""

from __future__ import annotations

import asyncio
import functools
import logging
import threading
from collections.abc import Awaitable, Callable, Sequence
from concurrent.futures import Executor, ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Any, TypeVar

from lib.core_utils.logging_utils import custom_logger
from lib.core_utils.plan_eligibility import get_eligibility_reason, is_plan_eligible
from lib.storage.errors import PlanStoreError
from lib.storage.plan_documents import (
    plan_generation_of,
    plan_model_from_document,
    validate_execution_authority,
)
from lib.storage.plan_updates import (
    ExecutionFinalization,
    FinalizationResult,
    FinalizationStatus,
    check_finalization,
    finishes_request,
)
from lib.storage.protocols import PlanStore
from yggdrasil.core.engine import Engine, _new_execution_id
from yggdrasil.flow.attempt import AttemptContext
from yggdrasil.flow.model import Plan
from yggdrasil.flow.outcomes import AttemptReport, ExecutionOutcome, TerminationReason

T = TypeVar("T")

# Total finalization attempts per bounded cycle, and the delays between them.
FINALIZATION_ATTEMPTS = 3
FINALIZATION_BACKOFF_SECONDS = (0.5, 1.0)

# Worker threads reserved for plan-store calls. Kept apart from the default
# executor, where attempts run for as long as their steps take, so that
# recording one plan's result never queues behind other plans' executions.
PLAN_STORE_WORKERS = 4


@dataclass(frozen=True)
class ExecutionClaim:
    """Which plans a caller is allowed to execute.

    Attributes:
        authority: The ``execution_authority`` a plan must carry: "daemon" or
            "run_once".
        owner: The ``execution_owner`` a plan must carry, or None to accept
            whichever owner it has. The daemon accepts any owner; a run-once
            session only its own.
    """

    authority: str
    owner: str | None = None

    def __post_init__(self) -> None:
        """Reject an authority no plan can carry.

        Raises:
            ValueError: If authority is not a valid execution authority.
        """
        validate_execution_authority(self.authority)

    def refusal(self, doc: dict[str, Any]) -> str | None:
        """Explain why this claim does not cover a plan document, if it does not.

        Args:
            doc: The plan document.

        Returns:
            str | None: The reason the plan is not the caller's to execute, or
            None if it is.
        """
        authority = doc.get("execution_authority")
        if authority != self.authority:
            return f"its execution authority is {authority!r}, not {self.authority!r}"
        owner = doc.get("execution_owner")
        if self.owner is not None and owner != self.owner:
            return f"its execution owner is {owner!r}, not {self.owner!r}"
        return None


# The daemon executes every plan under daemon authority, whoever owns it.
DAEMON_CLAIM = ExecutionClaim(authority="daemon")


class ExecutionStatus(str, Enum):
    """How one execution request was resolved.

    Attributes:
        NOT_FOUND: The plan document does not exist.
        NOT_AUTHORIZED: The plan is not the caller's to execute: its execution
            authority or owner differs from the caller's claim.
        NOT_ELIGIBLE: The plan is not approved, or its current request was
            already served.
        DUPLICATE: The request is the one the plan's previous attempt ran; it
            arrived while that attempt was in flight. It is not run a second
            time: that attempt's result stands for it. An unfinished request
            stays eligible, so a later event still retries it.
        INVALID_DOCUMENT: The plan document is eligible but does not describe
            an executable request. Nothing ran; the plan stays eligible.
        ADMISSION_FAILED: The plan store could not provide the snapshot to
            admit the request from. Nothing ran; the plan stays eligible.
        FINALIZED: The attempt finished its request and the result is
            recorded, consuming the token. The report's outcome says whether it
            succeeded.
        UNFINISHED: The attempt ended without finishing its request, other
            than by cancellation: a ``fail_fast`` step failure or preflight
            rejection, or an orchestration failure. Nothing was recorded; the
            plan stays eligible.
        CANCELLED: The attempt was interrupted before it drained: by
            cooperative cancellation, or by a step raising a control-flow
            exception such as its own ``asyncio.CancelledError``. Or it never
            started, because cancellation came first. Nothing was recorded;
            the plan stays eligible.
        FINALIZATION_PENDING: The attempt finished its request but its result
            could not be recorded. It is retained in memory, and the plan is
            not executed again while it is.
        SUPERSEDED: The attempt finished its request but recording it was
            refused for good: the plan was regenerated or deleted, its
            authority changed, or another execution recorded the request first.
    """

    NOT_FOUND = "not_found"
    NOT_AUTHORIZED = "not_authorized"
    NOT_ELIGIBLE = "not_eligible"
    DUPLICATE = "duplicate"
    INVALID_DOCUMENT = "invalid_document"
    ADMISSION_FAILED = "admission_failed"
    FINALIZED = "finalized"
    UNFINISHED = "unfinished"
    CANCELLED = "cancelled"
    FINALIZATION_PENDING = "finalization_pending"
    SUPERSEDED = "superseded"


@dataclass(frozen=True)
class ExecutionResult:
    """The resolution of one execution request.

    Attributes:
        plan_doc_id: The plan document the request was for.
        status: How the request was resolved.
        message: Human-readable explanation, as logged.
        report: The report of the attempt this result is about, if an attempt
            ran (or, for a pending finalization, the retained attempt's).
        finalization: How the last finalization call resolved, if one did.
    """

    plan_doc_id: str
    status: ExecutionStatus
    message: str
    report: AttemptReport | None = None
    finalization: FinalizationResult | None = None

    @property
    def succeeded(self) -> bool:
        """Whether an attempt succeeded and its success is recorded.

        Returns:
            bool: True only for a FINALIZED result whose report succeeded.
        """
        return (
            self.status is ExecutionStatus.FINALIZED
            and self.report is not None
            and self.report.outcome is ExecutionOutcome.SUCCEEDED
        )


@dataclass(frozen=True)
class _Admission:
    """An execution request captured from one plan-document snapshot.

    Attributes:
        plan: The executable plan.
        plan_generation: The plan's generation in that snapshot.
        run_token: The request's run token in that snapshot.
        execution_authority: The plan's execution authority in that snapshot.
        execution_owner: The plan's execution owner in that snapshot.
    """

    plan: Plan
    plan_generation: str
    run_token: int
    execution_authority: str
    execution_owner: str | None


@dataclass(frozen=True)
class _PendingFinalization:
    """A finished request whose result could not be recorded yet.

    Attributes:
        request: The finalization to record. Its identity never changes while
            it is pending.
        report: The report of the attempt that finished the request.
        retried_for: The newest run token whose request has already been
            spent on retrying this finalization, if any. Each newer request
            buys at most one bounded retry cycle, so repeated events for a run
            token that already had its cycle retry nothing.
    """

    request: ExecutionFinalization
    report: AttemptReport
    retried_for: int | None = None


@dataclass
class _PlanSlot:
    """In-flight state of one plan document.

    The slot sits in the coordinator's registry exactly while the plan is
    excluded: from the first request until its coordination task has recorded
    or retained its last result.

    Attributes:
        claim: The claim to admit the next cycle's request under; the most
            recent request's.
        cancel_event: Cooperative cancellation for every attempt of this slot.
            A ``threading.Event``, because the engine reads it from the worker
            thread and signal handlers may set it.
        task: The coordination task.
        started: Whether the coordination task has begun running. Until it
            has, only the task's completion callback can release the slot.
        next_result: Result of the next cycle, if a request arrived while the
            current cycle was running; every such request shares it.
        attempted: Plan generation and run token of the last request this
            slot ran an attempt for, so that a request arriving during that
            attempt cannot run the same request again.
    """

    claim: ExecutionClaim
    cancel_event: threading.Event = field(default_factory=threading.Event)
    task: asyncio.Task[None] | None = None
    started: bool = False
    next_result: asyncio.Future[ExecutionResult] | None = None
    attempted: tuple[str, int] | None = None


class _CycleStopped(Exception):
    """Ends a coordination cycle early, with the result it resolved to."""

    def __init__(self, result: ExecutionResult) -> None:
        """Carry result out of the cycle.

        Args:
            result: How the cycle's request was resolved.
        """
        super().__init__(result.message)
        self.result = result


def _resolve(future: asyncio.Future[ExecutionResult], result: ExecutionResult) -> None:
    """Set a result future unless it is already done."""
    if not future.done():
        future.set_result(result)


def _cancellation_requested_of_current_task() -> bool:
    """Whether the running task has been asked to cancel.

    Tells a cancellation of this task apart from a CancelledError that some
    other code raised, such as a worker's: only ``Task.cancel()`` counts here.
    The count is never withdrawn, so once cancelled, a task stays cancelled
    for this check, which is what its coordination needs.

    Returns:
        bool: True if ``cancel()`` has been called on the current task.
    """
    task = asyncio.current_task()
    return task is not None and task.cancelling() > 0


def _describe_attempt(report: AttemptReport) -> str:
    """Summarize an attempt's report for log messages.

    Args:
        report: The attempt's report.

    Returns:
        str: The execution ID, outcome, termination reason and step counts.
    """
    outcome = report.outcome.value if report.outcome else None
    reason = report.termination_reason.value if report.termination_reason else None
    return (
        f"execution '{report.execution_id}' (outcome={outcome}, "
        f"termination={reason}, steps={report.counts})"
    )


# Log level for each resolution; FINALIZED depends on the attempt's outcome.
_LOG_LEVELS: dict[ExecutionStatus, int] = {
    ExecutionStatus.NOT_FOUND: logging.ERROR,
    ExecutionStatus.NOT_AUTHORIZED: logging.INFO,
    ExecutionStatus.NOT_ELIGIBLE: logging.INFO,
    ExecutionStatus.DUPLICATE: logging.INFO,
    ExecutionStatus.INVALID_DOCUMENT: logging.ERROR,
    ExecutionStatus.ADMISSION_FAILED: logging.ERROR,
    ExecutionStatus.UNFINISHED: logging.ERROR,
    ExecutionStatus.CANCELLED: logging.WARNING,
    ExecutionStatus.FINALIZATION_PENDING: logging.ERROR,
    ExecutionStatus.SUPERSEDED: logging.WARNING,
}


class PlanExecutionCoordinator:
    """Admits, executes and finalizes plans, one attempt per plan at a time.

    One instance serves a whole process and both of its execution callers. It
    holds no attempt state beyond its registry: each attempt's outcomes live in
    the AttemptContext the coordinator creates for it.
    """

    def __init__(
        self,
        *,
        engine: Engine,
        plan_store: PlanStore,
        logger: logging.Logger | None = None,
        finalization_attempts: int = FINALIZATION_ATTEMPTS,
        finalization_backoff: Sequence[float] = FINALIZATION_BACKOFF_SECONDS,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        new_execution_id: Callable[[], str] = _new_execution_id,
        store_executor: Executor | None = None,
    ) -> None:
        """Initialize the coordinator.

        Args:
            engine: The engine that runs attempts.
            plan_store: The store plans are read from and finalized in.
            logger: Logger; a module logger is created when omitted.
            finalization_attempts: Total finalization attempts per bounded
                cycle.
            finalization_backoff: Seconds to wait before each finalization
                attempt after the first.
            sleep: Awaitable sleep used for that backoff; replaceable so tests
                can observe the delays without waiting them out.
            new_execution_id: Allocates each attempt's execution ID. Defaults
                to the allocator ``Engine.run`` uses, so coordinated and direct
                attempts are identified the same way.
            store_executor: Executor for blocking plan-store calls; a small
                dedicated thread pool when omitted.

        Raises:
            ValueError: If finalization_attempts is less than 1, or
                finalization_backoff has fewer delays than there are retries.
        """
        if finalization_attempts < 1:
            raise ValueError(
                f"finalization_attempts must be at least 1, got {finalization_attempts}"
            )
        if len(finalization_backoff) < finalization_attempts - 1:
            raise ValueError(
                f"finalization_backoff needs {finalization_attempts - 1} delay(s), "
                f"got {len(finalization_backoff)}"
            )
        self._engine = engine
        self._store = plan_store
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")
        self._attempts = finalization_attempts
        self._backoff = tuple(finalization_backoff)
        self._sleep = sleep
        self._new_execution_id = new_execution_id
        self._store_executor = store_executor or ThreadPoolExecutor(
            max_workers=PLAN_STORE_WORKERS, thread_name_prefix="ygg-plan-store"
        )
        self._slots: dict[str, _PlanSlot] = {}
        self._pending: dict[str, _PendingFinalization] = {}

    # ----- callers -----

    def submit(
        self, plan_doc_id: str, claim: ExecutionClaim
    ) -> asyncio.Future[ExecutionResult]:
        """Request execution of a plan, without waiting for it.

        If the plan is not in flight, its coordination starts now, as a task of
        its own. If it is, the request is kept for a recheck of the plan once
        the current cycle is done; every request that arrives meanwhile shares
        that one recheck.

        Must be called from the event loop the coordinator runs on.

        Args:
            plan_doc_id: The plan document to execute.
            claim: Which plans the caller may execute.

        Returns:
            asyncio.Future[ExecutionResult]: Resolves once the cycle serving
            this request is done. Await it through ``asyncio.shield``, as
            :meth:`execute` does, so that a cancelled waiter cannot cancel a
            result other waiters share.
        """
        loop = asyncio.get_running_loop()
        slot = self._slots.get(plan_doc_id)
        if slot is not None:
            slot.claim = claim
            if slot.next_result is None:
                slot.next_result = loop.create_future()
                self._logger.info(
                    "Plan '%s' is already being executed; checking it again once "
                    "the current attempt is done",
                    plan_doc_id,
                )
            return slot.next_result

        slot = _PlanSlot(claim=claim)
        result: asyncio.Future[ExecutionResult] = loop.create_future()
        self._slots[plan_doc_id] = slot
        slot.task = loop.create_task(
            self._coordinate(plan_doc_id, slot, result),
            name=f"plan-execution:{plan_doc_id}",
        )
        slot.task.add_done_callback(
            functools.partial(self._release_if_never_started, plan_doc_id, slot, result)
        )
        return result

    async def execute(self, plan_doc_id: str, claim: ExecutionClaim) -> ExecutionResult:
        """Request execution of a plan and wait for the result.

        Cancelling the caller does not cancel the execution. It asks the plan's
        in-flight attempt to stop starting new steps, and the coordination
        carries on without the caller: it waits for the running step, then
        records whatever the attempt reached.

        Args:
            plan_doc_id: The plan document to execute.
            claim: Which plans the caller may execute.

        Returns:
            ExecutionResult: How the request was resolved.

        Raises:
            asyncio.CancelledError: If the caller is cancelled while waiting.
        """
        result = self.submit(plan_doc_id, claim)
        try:
            return await asyncio.shield(result)
        except asyncio.CancelledError:
            self.request_cancellation(plan_doc_id)
            raise

    def request_cancellation(self, plan_doc_id: str | None = None) -> None:
        """Ask in-flight attempts to stop starting new steps.

        Cooperative: a running step is never interrupted, and the plan stays
        excluded until its worker has actually finished. A request that
        arrived while the plan was in flight is not checked again. Safe to call
        from a signal handler or another thread.

        Args:
            plan_doc_id: The plan whose attempt to cancel, or None for every
                plan in flight.
        """
        if plan_doc_id is None:
            slots = list(self._slots.values())
        else:
            slot = self._slots.get(plan_doc_id)
            slots = [slot] if slot is not None else []
        for slot in slots:
            slot.cancel_event.set()

    async def drain(self) -> None:
        """Cancel every in-flight attempt cooperatively and wait for it to end.

        Returns once each plan in flight has finished its running step and
        recorded or retained its result. Coordination tasks that belong to a
        different event loop are only asked to cancel, not waited for.
        """
        self.request_cancellation()
        loop = asyncio.get_running_loop()
        tasks = [
            slot.task
            for slot in list(self._slots.values())
            if slot.task is not None and slot.task.get_loop() is loop
        ]
        if tasks:
            self._logger.info(
                "Waiting for %d in-flight plan execution(s) to stop", len(tasks)
            )
            # asyncio.wait, unlike gather, never cancels what it waits for.
            await asyncio.wait(tasks)

    def is_in_flight(self, plan_doc_id: str) -> bool:
        """Whether the plan is currently excluded from a new attempt.

        Args:
            plan_doc_id: The plan document.

        Returns:
            bool: True from the plan's first request until its coordination
            has recorded or retained its last result.
        """
        return plan_doc_id in self._slots

    def pending_finalization(self, plan_doc_id: str) -> ExecutionFinalization | None:
        """Return the plan's retained, unrecorded finalization, if it has one.

        Args:
            plan_doc_id: The plan document.

        Returns:
            ExecutionFinalization | None: The finalization kept after its
            bounded attempts failed; None if there is none.
        """
        pending = self._pending.get(plan_doc_id)
        return pending.request if pending is not None else None

    # ----- coordination -----

    async def _coordinate(
        self,
        plan_doc_id: str,
        slot: _PlanSlot,
        result: asyncio.Future[ExecutionResult],
    ) -> None:
        """Serve a plan's requests, one cycle at a time, then release the plan.

        Runs as the plan's own task. After each cycle, a request that arrived
        during it gets one more cycle, from a fresh snapshot, unless
        cancellation was requested. The plan is released only after the last
        cycle has recorded or retained its result.

        Args:
            plan_doc_id: The plan document.
            slot: The plan's registry slot.
            result: Future for the first cycle's result.
        """
        # From here on this body owns the slot, and its cleanup releases it.
        slot.started = True
        try:
            while True:
                outcome = await self._run_cycle(plan_doc_id, slot)
                self._log_result(outcome)
                _resolve(result, outcome)
                # The next result stays on the slot until it is handed over, so
                # the cleanup below can always reach it.
                if slot.next_result is None:
                    return
                if slot.cancel_event.is_set():
                    skipped = ExecutionResult(
                        plan_doc_id,
                        ExecutionStatus.CANCELLED,
                        f"Plan '{plan_doc_id}' was not checked again: "
                        "cancellation was requested while it was in flight",
                    )
                    self._log_result(skipped)
                    _resolve(slot.next_result, skipped)
                    return
                result, slot.next_result = slot.next_result, None
        except Exception as exc:
            # A defect in coordination itself. Fail the waiters loudly rather
            # than leave them waiting for a result that will never come.
            self._logger.exception(
                "Coordinating the execution of plan '%s' failed unexpectedly",
                plan_doc_id,
            )
            for waiting in (result, slot.next_result):
                if waiting is not None and not waiting.done():
                    waiting.set_exception(exc)
        finally:
            for waiting in (result, slot.next_result):
                if waiting is not None and not waiting.done():
                    waiting.cancel()
            if self._slots.get(plan_doc_id) is slot:
                del self._slots[plan_doc_id]

    def _release_if_never_started(
        self,
        plan_doc_id: str,
        slot: _PlanSlot,
        result: asyncio.Future[ExecutionResult],
        task: asyncio.Task[None],
    ) -> None:
        """Release a plan whose coordination task ended before it began running.

        A task cancelled before its first step never runs its coroutine at all,
        so the cleanup :meth:`_coordinate` would do never happens either. This
        completion callback does it instead: the waiting requests resolve as
        cancelled, since nothing ran, and the slot is removed. A coordination
        that did start is left entirely to its own cleanup, which releases the
        plan only once its last result is recorded or retained.

        Args:
            plan_doc_id: The plan document.
            slot: The slot the task was created for.
            result: Future for the first cycle's result.
            task: The finished coordination task.
        """
        if slot.started:
            return
        skipped = ExecutionResult(
            plan_doc_id,
            ExecutionStatus.CANCELLED,
            f"Plan '{plan_doc_id}' was not executed: its coordination was "
            "cancelled before it started",
        )
        self._log_result(skipped)
        for waiting in (result, slot.next_result):
            if waiting is not None:
                _resolve(waiting, skipped)
        slot.next_result = None
        if self._slots.get(plan_doc_id) is slot:
            del self._slots[plan_doc_id]

    async def _run_cycle(self, plan_doc_id: str, slot: _PlanSlot) -> ExecutionResult:
        """Admit, execute and finalize one request for a plan.

        Args:
            plan_doc_id: The plan document.
            slot: The plan's registry slot, whose claim the request is admitted
                under.

        Returns:
            ExecutionResult: How the request was resolved.
        """
        cancel_event = slot.cancel_event
        try:
            doc = await self._read_snapshot(plan_doc_id, cancel_event)
            if plan_doc_id in self._pending:
                await self._settle_pending(plan_doc_id, doc, cancel_event)
                # Settling may have written the plan; admit from a fresh read.
                doc = await self._read_snapshot(plan_doc_id, cancel_event)
            admission = self._admit(plan_doc_id, doc, slot.claim)
            request = (admission.plan_generation, admission.run_token)
            if request == slot.attempted:
                raise _CycleStopped(
                    ExecutionResult(
                        plan_doc_id,
                        ExecutionStatus.DUPLICATE,
                        f"Plan '{plan_doc_id}': run_token {admission.run_token} "
                        "was being attempted when this request arrived, so it "
                        "is not run again",
                    )
                )
            if cancel_event.is_set():
                raise _CycleStopped(
                    ExecutionResult(
                        plan_doc_id,
                        ExecutionStatus.CANCELLED,
                        f"Plan '{plan_doc_id}' was not executed: cancellation was "
                        "requested before its attempt started",
                    )
                )
            slot.attempted = request
            return await self._execute(plan_doc_id, admission, cancel_event)
        except _CycleStopped as stopped:
            return stopped.result

    async def _read_snapshot(
        self, plan_doc_id: str, cancel_event: threading.Event
    ) -> dict[str, Any] | None:
        """Read the plan document every admission decision is taken from.

        A legacy document gets its first generation in the same read, under the
        plan's guard, so the snapshot always carries the generation it will be
        finalized against.

        Args:
            plan_doc_id: The plan document.
            cancel_event: The slot's cooperative cancellation signal.

        Returns:
            dict | None: The current document, or None if there is none.

        Raises:
            _CycleStopped: ADMISSION_FAILED, if the store cannot provide it.
        """
        try:
            return await self._call_store(
                f"reading plan '{plan_doc_id}'",
                self._store.ensure_plan_generation,
                plan_doc_id,
                cancel_event=cancel_event,
            )
        except Exception as exc:
            # Nothing has run, so whatever went wrong, the request stays
            # eligible and a later event can try again.
            raise _CycleStopped(
                ExecutionResult(
                    plan_doc_id,
                    ExecutionStatus.ADMISSION_FAILED,
                    f"Could not read plan '{plan_doc_id}' to admit it for "
                    f"execution ({type(exc).__name__}: {exc}); it stays eligible",
                )
            ) from exc

    def _admit(
        self, plan_doc_id: str, doc: dict[str, Any] | None, claim: ExecutionClaim
    ) -> _Admission:
        """Capture an execution request from one plan-document snapshot.

        Args:
            plan_doc_id: The plan document.
            doc: The snapshot, or None if the plan does not exist.
            claim: Which plans the caller may execute.

        Returns:
            _Admission: The request to execute.

        Raises:
            _CycleStopped: If the snapshot does not admit an execution:
                NOT_FOUND, NOT_AUTHORIZED, NOT_ELIGIBLE or INVALID_DOCUMENT.
        """

        def stop(status: ExecutionStatus, message: str) -> _CycleStopped:
            return _CycleStopped(ExecutionResult(plan_doc_id, status, message))

        if doc is None:
            raise stop(
                ExecutionStatus.NOT_FOUND,
                f"Plan document '{plan_doc_id}' not found; cannot execute",
            )
        refusal = claim.refusal(doc)
        if refusal is not None:
            raise stop(
                ExecutionStatus.NOT_AUTHORIZED,
                f"Plan '{plan_doc_id}' is not executed here: {refusal}",
            )
        if not is_plan_eligible(doc):
            raise stop(
                ExecutionStatus.NOT_ELIGIBLE,
                f"Plan '{plan_doc_id}' is not eligible; skipping execution "
                f"({get_eligibility_reason(doc)})",
            )

        plan = plan_model_from_document(doc, self._logger)
        if plan is None:
            raise stop(
                ExecutionStatus.INVALID_DOCUMENT,
                f"Failed to deserialize plan '{plan_doc_id}'; cannot execute",
            )
        if plan.plan_id != plan_doc_id:
            # The result would be finalized under the embedded ID, which is
            # a different document.
            raise stop(
                ExecutionStatus.INVALID_DOCUMENT,
                f"Plan document '{plan_doc_id}' embeds plan '{plan.plan_id}'; "
                "cannot execute a plan stored under another ID",
            )
        generation = plan_generation_of(doc)
        if generation is None:
            raise stop(
                ExecutionStatus.INVALID_DOCUMENT,
                f"Plan '{plan_doc_id}' has no plan_generation to capture; "
                "cannot execute",
            )
        return _Admission(
            plan=plan,
            plan_generation=generation,
            # is_plan_eligible has already checked that the token is an integer.
            run_token=int(doc.get("run_token", 0)),
            execution_authority=str(doc["execution_authority"]),
            execution_owner=doc.get("execution_owner"),
        )

    async def _execute(
        self, plan_doc_id: str, admission: _Admission, cancel_event: threading.Event
    ) -> ExecutionResult:
        """Run one attempt for an admitted request, then resolve the request.

        Args:
            plan_doc_id: The plan document.
            admission: The captured request.
            cancel_event: The slot's cooperative cancellation signal, handed to
                the attempt.

        Returns:
            ExecutionResult: How the request was resolved.
        """
        plan = admission.plan
        context = AttemptContext.for_plan(
            plan,
            execution_id=self._new_execution_id(),
            plan_generation=admission.plan_generation,
            run_token=admission.run_token,
            execution_authority=admission.execution_authority,
            execution_owner=admission.execution_owner,
            cancel_event=cancel_event,
        )
        self._logger.info(
            "Executing plan '%s' (realm=%s, policy=%s, run_token=%d, "
            "generation=%s, execution=%s)",
            plan_doc_id,
            plan.realm,
            plan.failure_policy,
            admission.run_token,
            admission.plan_generation,
            context.execution_id,
        )

        loop = asyncio.get_running_loop()
        worker = loop.run_in_executor(
            None, functools.partial(self._engine._run_attempt, plan, context=context)
        )
        returned: AttemptReport | None = None
        raised: BaseException | None = None
        try:
            finished = await self._outlast_cancellation(
                worker, cancel_event, f"plan '{plan_doc_id}' was executing"
            )
        except BaseException as exc:
            # The attempt's own ending. Cancellation of this coordination never
            # arrives here, and the report says how the attempt ended. Nothing
            # is re-raised: whatever a step raised belongs to that step's
            # thread, not to the event loop every other plan runs on.
            raised = exc
        else:
            returned = finished

        report = context.report
        if not report.is_finished:
            return ExecutionResult(
                plan_doc_id,
                ExecutionStatus.UNFINISHED,
                f"Plan '{plan_doc_id}': the engine ended without closing the "
                f"report of {context.execution_id!r}"
                + (f" ({type(raised).__name__}: {raised})" if raised else "")
                + f"; not recording the attempt, run_token {context.run_token} "
                "stays eligible",
                report=report,
            )
        if raised is None and returned is not report:
            return ExecutionResult(
                plan_doc_id,
                ExecutionStatus.UNFINISHED,
                f"Plan '{plan_doc_id}': the engine returned "
                f"{type(returned).__name__} instead of the report of "
                f"{context.execution_id!r}; not recording the attempt",
                report=report,
            )
        return await self._resolve_attempt(plan_doc_id, context, raised, cancel_event)

    async def _resolve_attempt(
        self,
        plan_doc_id: str,
        context: AttemptContext,
        raised: BaseException | None,
        cancel_event: threading.Event,
    ) -> ExecutionResult:
        """Record a finished request, or explain why the request is not finished.

        Args:
            plan_doc_id: The plan document.
            context: The ended attempt's context; its report is closed.
            raised: The exception the attempt ended with, if any.
            cancel_event: The slot's cooperative cancellation signal.

        Returns:
            ExecutionResult: How the request was resolved.
        """
        report = context.report
        outcome, reason = report.outcome, report.termination_reason
        if (
            outcome is None
            or reason is None
            or not finishes_request(outcome, reason, report.failure_policy)
        ):
            status = (
                ExecutionStatus.CANCELLED
                if reason is TerminationReason.CANCELLED
                else ExecutionStatus.UNFINISHED
            )
            cause = f": {type(raised).__name__}: {raised}" if raised else ""
            return ExecutionResult(
                plan_doc_id,
                status,
                f"Plan '{plan_doc_id}' {_describe_attempt(report)} did not finish "
                f"its request{cause}; run_token {report.run_token} stays eligible",
                report=report,
            )

        request = ExecutionFinalization.from_attempt(context)
        final = await self._finalize(request, cancel_event)
        if final is None:
            # Established before _coordinate releases the plan, so no request
            # can slip in between and run the plan again.
            self._pending[plan_doc_id] = _PendingFinalization(request, report)
            return ExecutionResult(
                plan_doc_id,
                ExecutionStatus.FINALIZATION_PENDING,
                f"Plan '{plan_doc_id}' {_describe_attempt(report)} finished, but "
                "its result could not be recorded; keeping it pending, and not "
                "executing the plan again until it is recorded or superseded",
                report=report,
            )
        if final.status is FinalizationStatus.SUPERSEDED:
            return ExecutionResult(
                plan_doc_id,
                ExecutionStatus.SUPERSEDED,
                f"Plan '{plan_doc_id}' {_describe_attempt(report)} finished, but "
                f"its result was not recorded: {final.message}",
                report=report,
                finalization=final,
            )
        if outcome is ExecutionOutcome.SUCCEEDED:
            summary = f"✓ Plan '{plan_doc_id}' execution succeeded"
            rerun = ""
        else:
            summary = f"✗ Plan '{plan_doc_id}' execution finished with failures"
            rerun = "; the plan runs again only on a new run request"
        return ExecutionResult(
            plan_doc_id,
            ExecutionStatus.FINALIZED,
            f"{summary}: {_describe_attempt(report)}; recorded "
            f"executed_run_token={request.run_token}{rerun}",
            report=report,
            finalization=final,
        )

    async def _settle_pending(
        self,
        plan_doc_id: str,
        doc: dict[str, Any] | None,
        cancel_event: threading.Event,
    ) -> None:
        """Resolve a plan's pending finalization before admitting anything new.

        Reading the plan is enough to settle it when the plan was deleted or
        regenerated, its authority changed, or the result turns out to be
        recorded after all. A newer run token in the same generation retries
        the pending finalization first: that newer request is the explicit
        action that pending results wait for, and it buys exactly one bounded
        retry cycle. Any other request, for the pending run token or for one
        that already had its cycle, leaves the pending result untouched and
        retries nothing, however often it is repeated.

        Args:
            plan_doc_id: The plan document.
            doc: The current snapshot, or None if the plan does not exist.
            cancel_event: The slot's cooperative cancellation signal.

        Raises:
            _CycleStopped: FINALIZATION_PENDING, if the pending result is still
                unrecorded and still valid.
        """
        pending = self._pending[plan_doc_id]
        request = pending.request

        def still_pending(detail: str) -> _CycleStopped:
            return _CycleStopped(
                ExecutionResult(
                    plan_doc_id,
                    ExecutionStatus.FINALIZATION_PENDING,
                    f"Plan '{plan_doc_id}' has a pending finalization of execution "
                    f"'{request.execution_id}' (run_token {request.run_token}); "
                    f"{detail}",
                    report=pending.report,
                )
            )

        if doc is None:
            del self._pending[plan_doc_id]
            self._logger.warning(
                "Dropping the pending finalization of execution '%s': plan '%s' "
                "no longer exists",
                request.execution_id,
                plan_doc_id,
            )
            return
        try:
            verdict = check_finalization(doc, request)
            requested_token = int(doc.get("run_token", 0))
        except (TypeError, ValueError) as exc:
            raise still_pending(f"the plan cannot be judged ({exc})") from exc
        if verdict is not None:
            del self._pending[plan_doc_id]
            self._logger.warning(
                "Pending finalization of plan '%s' settled without writing: %s",
                plan_doc_id,
                verdict.message,
            )
            return
        if requested_token <= request.run_token:
            raise still_pending(
                "not executing the plan again; a newer run request retries the "
                "finalization first"
            )
        if pending.retried_for is not None and requested_token <= pending.retried_for:
            raise still_pending(
                f"run_token {requested_token} already had its retry, which "
                "failed; only a newer run request retries it again"
            )

        self._logger.info(
            "Plan '%s' has a newer run request (run_token %d); recording the "
            "pending result of run_token %d first",
            plan_doc_id,
            requested_token,
            request.run_token,
        )
        # Spent before the cycle starts, so this request buys one cycle however
        # the cycle ends.
        self._pending[plan_doc_id] = replace(pending, retried_for=requested_token)
        final = await self._finalize(request, cancel_event)
        if final is None:
            raise still_pending(
                f"recording it failed again, so run_token {requested_token} "
                "waits; only a newer run request retries it again"
            )
        del self._pending[plan_doc_id]

    async def _finalize(
        self, request: ExecutionFinalization, cancel_event: threading.Event
    ) -> FinalizationResult | None:
        """Record a finished request, retrying within the bound.

        Args:
            request: The finalization to record.
            cancel_event: The slot's cooperative cancellation signal.

        Returns:
            FinalizationResult | None: The recorded or SUPERSEDED result; None
            if no attempt resolved it.
        """
        what = (
            f"finalizing execution '{request.execution_id}' of plan "
            f"'{request.plan_id}' (run_token {request.run_token})"
        )
        for attempt in range(1, self._attempts + 1):
            if attempt > 1:
                await self._wait_before_retry(self._backoff[attempt - 2], cancel_event)
            try:
                result = await self._call_store(
                    what,
                    self._store.finalize_execution,
                    request,
                    cancel_event=cancel_event,
                )
            except PlanStoreError as exc:
                if not exc.retryable:
                    self._logger.error(
                        "%s failed and is not worth retrying (attempt %d/%d): %s",
                        what,
                        attempt,
                        self._attempts,
                        exc,
                    )
                    return None
                self._logger.warning(
                    "%s failed (attempt %d/%d): %s", what, attempt, self._attempts, exc
                )
                continue
            except Exception:
                self._logger.exception(
                    "%s failed unexpectedly (attempt %d/%d); not retrying",
                    what,
                    attempt,
                    self._attempts,
                )
                return None
            if result.status is FinalizationStatus.CONFLICT:
                self._logger.warning(
                    "%s lost a race (attempt %d/%d): %s",
                    what,
                    attempt,
                    self._attempts,
                    result.message,
                )
                continue
            return result
        self._logger.error("%s: giving up after %d attempts", what, self._attempts)
        return None

    # ----- waiting without being unwound -----

    async def _call_store(
        self,
        what: str,
        fn: Callable[..., T],
        *args: Any,
        cancel_event: threading.Event,
    ) -> T:
        """Run a blocking plan-store call on the store executor.

        Args:
            what: Description of the call, for logs.
            fn: The plan-store method.
            *args: Its arguments.
            cancel_event: The slot's cooperative cancellation signal.

        Returns:
            T: What the call returned.

        Raises:
            Exception: Whatever the call raised.
        """
        loop = asyncio.get_running_loop()
        call = loop.run_in_executor(self._store_executor, functools.partial(fn, *args))
        return await self._outlast_cancellation(call, cancel_event, what)

    async def _outlast_cancellation(
        self, future: asyncio.Future[T], cancel_event: threading.Event, what: str
    ) -> T:
        """Wait for a worker thread's outcome, even if this task is cancelled.

        The future comes from ``run_in_executor``, not from a task, so shutdown
        routines that cancel every task never cancel it; and awaiting it through
        ``asyncio.shield`` means cancelling this task cannot cancel it either.

        A CancelledError at the await means one of two things, and whether the
        future has finished tells them apart. While it has not, this task was
        cancelled and the worker is still running: that becomes a cooperative
        cancellation request, and the wait goes on. Once it has, the worker's
        own outcome is returned or raised unchanged, including a CancelledError
        that the worker raised itself, which ends its attempt but cancels
        nothing here. A finished future is never awaited again: that would
        re-raise its exception without yielding, and a loop doing so would
        stall the event loop.

        Args:
            future: The worker thread's future.
            cancel_event: The slot's cooperative cancellation signal.
            what: What the worker is doing, for logs.

        Returns:
            T: The worker's result.

        Raises:
            BaseException: Whatever the worker raised, CancelledError included.
        """
        while not future.done():
            try:
                await asyncio.shield(future)
            except asyncio.CancelledError:
                # Noted even when the worker finished at the same moment: its
                # outcome still stands, but this coordination starts nothing
                # further.
                if _cancellation_requested_of_current_task():
                    self._note_cancellation(cancel_event, what)
        return future.result()

    async def _wait_before_retry(
        self, delay: float, cancel_event: threading.Event
    ) -> None:
        """Wait out a finalization backoff, unless cancellation cuts it short.

        Cancellation ends the wait early but not the bounded finalization: the
        next attempt follows at once, since a result that is ready should not
        be abandoned because the process is stopping.

        Args:
            delay: Seconds to wait.
            cancel_event: The slot's cooperative cancellation signal.
        """
        try:
            await self._sleep(delay)
        except asyncio.CancelledError:
            self._note_cancellation(
                cancel_event, "waiting to retry a finalization, which is retried now"
            )

    def _note_cancellation(self, cancel_event: threading.Event, what: str) -> None:
        """Turn a cancellation of this task into a cooperative cancellation request.

        Args:
            cancel_event: The slot's cooperative cancellation signal.
            what: What was being awaited, for the log.
        """
        if cancel_event.is_set():
            return
        cancel_event.set()
        self._logger.warning(
            "Cancelled while %s; no further steps or requests will be started, "
            "and whatever the attempt reached is still awaited and recorded",
            what,
        )

    def _log_result(self, result: ExecutionResult) -> None:
        """Log a request's resolution at a level matching it."""
        if result.status is ExecutionStatus.FINALIZED:
            level = logging.INFO if result.succeeded else logging.ERROR
        else:
            level = _LOG_LEVELS[result.status]
        self._logger.log(level, "%s", result.message)
