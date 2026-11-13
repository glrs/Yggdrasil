"""Coordinate plan execution for the daemon and run-once callers.

Both callers hand a possibly-ready plan to one :class:`PlanExecutionCoordinator`,
which owns everything between "this plan may be eligible" and "its result is
recorded":

1. **Admission from one snapshot.** The current plan document is read once,
   under the plan's in-flight guard, with a plain fetch that writes nothing and
   returns a document only when its storage envelope is intact. Its family is
   checked before anything else: a document whose ``schema_version`` is absent,
   invalid or names a family this release does not support is not executed
   (``UNSUPPORTED_DOCUMENT``). Eligibility, execution authority and owner, the
   executable plan, its failure policy, family, generation and run token all
   come from that one read, never from the watcher event that prompted it, so a
   stale event cannot combine an old approval or token with a newer plan.
2. **Execution.** The attempt runs in a worker thread through
   ``Engine._run_attempt``, with an AttemptContext the coordinator owns, so the
   attempt's report is readable however the attempt ends. Its execution ID is
   allocated and reserved just before, through the engine's own helper, the
   one direct ``Engine.run`` calls use as well, so attempts at a plan are
   ordered however they were started and never share an attempt directory.
   Allocation reads the plan's attempt history and reservation creates the
   attempt's directory, so both run in a worker thread too. A request that is
   not going to run, such as a duplicate, gets no execution ID and leaves no
   trace in reporting.
3. **Interpretation.** The report's termination reason decides whether the
   request finished, by the rule of ``finishes_request`` in
   ``yggdrasil.contract.read``, applied through its adapter for the ``flow``
   enums in ``yggdrasil/storage/plan_updates.py``; the type of any exception
   does not, since a step body can raise any exception type itself. A
   request is consumed when an attempt succeeds, when a
   ``continue_independent`` attempt drains with failures, and when preflight
   rejects a ``continue_independent`` plan. Any other ending leaves the token
   untouched, so the plan stays eligible.
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
    the plan and rechecks family, generation, token, execution and authority
    before it writes, so retrying never resends a stale document. A revision
    conflict and a retryable ``PlanStoreError`` are retried, up to three
    attempts in all, 0.5 s and then 1 s apart. The waiting happens on the event
    loop, never in a worker thread. A recorded result, a supersession and a
    non-retryable failure end the retries at once, and so does a refusal over
    the plan's tokens: a pair that is not valid as read, or that recording the
    result would make invalid, can only change when the document does. Each
    backend bounds its own calls: a CouchDB request times out after 60 s, and
    SQLite waits at most 5 s for a lock.

    When no attempt records the result, or recording it is refused over the
    plan's tokens, it is kept in memory as a pending finalization, and that is
    established before the plan's guard is released. While it is pending, the
    plan is never executed again. Reading the plan alone recognizes a document
    that is no longer of a supported family, which drops the pending result
    with a warning: it is never recorded and never retried, and its report
    remains in the event spool. A read whose answer is not the plan's stored
    document says nothing about the plan and keeps the result pending. Reading
    the plan also recognizes a regeneration, which ends the pending state
    whatever the tokens say. While the tokens are not a valid pair, or
    recording the result would make them invalid, the result stays pending.
    Once they are valid, a change of authority, or the result turning out to
    be recorded after all, ends the pending state too. None of this writes
    anything. A newer run token in the same generation retries the pending
    finalization first, with one more bounded cycle, and runs the newer
    request only once the old result is recorded or definitively rejected.
    Each newer run token buys one such cycle: repeated requests, for the
    pending run token or for one that already had its cycle, change nothing
    and retry nothing. A pending result does not survive a restart; the
    request is then still eligible, so it may run again, and realm
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

from yggdrasil.contract import PLAN_DOCUMENT, SUPPORTED_FAMILIES
from yggdrasil.contract.controls import (
    execution_definition_problems,
    failure_policy_of,
    token_pair,
)
from yggdrasil.contract.diagnostics import describe_recorded
from yggdrasil.contract.fields import EXECUTION_AUTHORITY_FIELD, EXECUTION_OWNER_FIELD
from yggdrasil.contract.read import generation_of
from yggdrasil.contract.versions import (
    SUPPORTED,
    describe_schema_version,
    version_of,
    version_sentence,
)
from yggdrasil.core.engine import Engine
from yggdrasil.daemon.worker_futures import outlast_cancellation
from yggdrasil.flow.attempt import AttemptContext
from yggdrasil.flow.model import Plan
from yggdrasil.flow.outcomes import AttemptReport, ExecutionOutcome, TerminationReason
from yggdrasil.logging_utils import custom_logger
from yggdrasil.storage.errors import PlanStoreError
from yggdrasil.storage.plan_documents import (
    plan_model_from_document,
    validate_execution_authority,
)
from yggdrasil.storage.plan_eligibility import get_eligibility_reason, is_plan_eligible
from yggdrasil.storage.plan_updates import (
    ExecutionFinalization,
    FinalizationResult,
    FinalizationStatus,
    check_finalization,
    finishes_request,
)
from yggdrasil.storage.protocols import PlanStore

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
            None if it is. Never raises on stored content.
        """
        authority = doc.get(EXECUTION_AUTHORITY_FIELD)
        if authority != self.authority:
            return (
                f"its execution authority is {describe_recorded(authority)}, "
                f"not {self.authority!r}"
            )
        owner = doc.get(EXECUTION_OWNER_FIELD)
        if self.owner is not None and owner != self.owner:
            return (
                f"its execution owner is {describe_recorded(owner)}, "
                f"not {self.owner!r}"
            )
        return None


# The daemon executes every plan under daemon authority, whoever owns it.
DAEMON_CLAIM = ExecutionClaim(authority="daemon")


class ExecutionStatus(str, Enum):
    """How one execution request was resolved.

    Attributes:
        NOT_FOUND: The plan document does not exist.
        UNSUPPORTED_DOCUMENT: The plan document's ``schema_version`` is
            absent, invalid, or a family this process does not support. It
            is checked before anything else the document says. Nothing ran
            and nothing was written. A pending finalization of the plan, if
            there was one, was dropped: it is never recorded.
        NOT_AUTHORIZED: The plan is not the caller's to execute: its execution
            authority or owner differs from the caller's claim.
        NOT_ELIGIBLE: The plan is not approved, its execution tokens are not
            a valid pair, or its current request was already served.
        DUPLICATE: The request is the one the plan's previous attempt ran; it
            arrived while that attempt was in flight. It is not run a second
            time: that attempt's result stands for it. An unfinished request
            stays eligible, so a later event still retries it.
        INVALID_DOCUMENT: The plan document is eligible but does not describe
            an executable request: a field of the plan or of one of its steps
            is missing or null, the failure policy is not a known one, the
            plan cannot be deserialized, it embeds another plan's ID, or the
            document has no generation. Nothing ran and nothing was written;
            the plan stays eligible.
        ADMISSION_FAILED: The plan store could not provide the snapshot to
            admit the request from, or no execution ID could be allocated and
            reserved for it. Nothing ran; the plan stays eligible.
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
            could not be recorded: the plan store failed, or the plan's tokens
            are not a valid pair, or would not be once the result is recorded.
            It is retained in memory, and the plan is not executed again while
            it is.
        SUPERSEDED: The attempt finished its request but recording it was
            refused for good: the plan was regenerated or deleted, it is no
            longer of the family it was admitted under, its authority
            changed, or another execution recorded the request first.
    """

    NOT_FOUND = "not_found"
    UNSUPPORTED_DOCUMENT = "unsupported_document"
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
        schema_version: The plan document's family in that snapshot, a
            supported one.
        plan_generation: The plan's generation in that snapshot.
        run_token: The request's run token in that snapshot.
        execution_authority: The plan's execution authority in that snapshot.
        execution_owner: The plan's execution owner in that snapshot.
    """

    plan: Plan
    schema_version: int
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
    # Every pre-contract plan resolves this way until it is regenerated: an
    # operator's task, not a malfunction.
    ExecutionStatus.UNSUPPORTED_DOCUMENT: logging.WARNING,
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


def _log_level(result: ExecutionResult) -> int:
    """Return the level a request's resolution is logged at.

    A finalized request is logged by how its attempt ended. Success is INFO. An
    attempt that drained its dependency graph (COMPLETED) with a failed outcome
    is WARNING: under ``continue_independent`` that is a normal ending, recorded
    truthfully, not a malfunction. Any other finalized ending, such as a
    preflight rejection, or one without a report to judge by, stays ERROR.
    Every other resolution has its level in ``_LOG_LEVELS``.

    Args:
        result: The request's resolution.

    Returns:
        int: The logging level.
    """
    if result.status is not ExecutionStatus.FINALIZED:
        return _LOG_LEVELS[result.status]
    if result.succeeded:
        return logging.INFO
    report = result.report
    if (
        report is not None
        and report.termination_reason is TerminationReason.COMPLETED
        and report.outcome is ExecutionOutcome.FAILED
    ):
        return logging.WARNING
    return logging.ERROR


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
            self._require_supported_family(plan_doc_id, doc)
            if plan_doc_id in self._pending:
                await self._settle_pending(plan_doc_id, doc, cancel_event)
                # Settling may have written the plan; admit from a fresh read.
                doc = await self._read_snapshot(plan_doc_id, cancel_event)
                self._require_supported_family(plan_doc_id, doc)
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
            execution_id, reserved = await self._allocate_execution_id(
                plan_doc_id, admission.plan, cancel_event
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
            return await self._execute(
                plan_doc_id, admission, execution_id, reserved, cancel_event
            )
        except _CycleStopped as stopped:
            return stopped.result

    async def _read_snapshot(
        self, plan_doc_id: str, cancel_event: threading.Event
    ) -> dict[str, Any] | None:
        """Read the plan document every admission decision is taken from.

        A plain read, which writes nothing (``PlanStore.read_plan_document``).
        A body whose storage envelope is not intact is a store failure, never
        a document: nothing is concluded from it, so a pending finalization
        stays pending.

        Args:
            plan_doc_id: The plan document.
            cancel_event: The slot's cooperative cancellation signal.

        Returns:
            dict | None: The current document, or None if the store confirms
            there is none.

        Raises:
            _CycleStopped: ADMISSION_FAILED, if the store cannot provide it.
        """
        try:
            return await self._call_store(
                f"reading plan '{plan_doc_id}'",
                self._store.read_plan_document,
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

    def _require_supported_family(
        self, plan_doc_id: str, doc: dict[str, Any] | None
    ) -> None:
        """Stop the cycle unless the plan document is of a supported family.

        Called after every read a cycle decides from, before anything else is
        concluded from the document: before a pending finalization is settled
        and before admission. A missing document passes; settling and
        admission handle it.

        A document whose ``schema_version`` is absent, invalid or names a
        family this process does not support is not executed. A pending
        finalization of the plan is dropped with a warning: recording it would
        write into a document this process cannot interpret, so it is never
        recorded and never retried. Nothing is written either way.

        Args:
            plan_doc_id: The plan document.
            doc: The document as just read, or None if there is none.

        Raises:
            _CycleStopped: UNSUPPORTED_DOCUMENT, if the document is not of a
                supported family.
        """
        if doc is None:
            return
        version = version_of(PLAN_DOCUMENT, doc)
        if version.state == SUPPORTED:
            return
        pending = self._pending.pop(plan_doc_id, None)
        if pending is not None:
            self._logger.warning(
                "Dropping the pending finalization of execution '%s': the "
                "schema_version of plan '%s' is now %s, so it is %s (supported "
                "plan-document families: %s). The result is not recorded and is "
                "not retried; its report remains in the event spool",
                pending.request.execution_id,
                plan_doc_id,
                describe_schema_version(version),
                version.state,
                sorted(SUPPORTED_FAMILIES[PLAN_DOCUMENT]),
            )
        raise _CycleStopped(
            ExecutionResult(
                plan_doc_id,
                ExecutionStatus.UNSUPPORTED_DOCUMENT,
                f"Plan '{plan_doc_id}' is not executed: "
                f"{version_sentence(PLAN_DOCUMENT, version)}; nothing ran and "
                "nothing was written",
            )
        )

    async def _allocate_execution_id(
        self, plan_doc_id: str, plan: Plan, cancel_event: threading.Event
    ) -> tuple[str, bool]:
        """Allocate and reserve the execution ID of the attempt about to be made.

        The engine's helper is the only way to get one: the coordinator holds
        no allocator of its own and calls ``Engine.reserve_execution_id`` at
        every allocation, so coordinated and direct ``Engine.run`` attempts
        are always ordered by the same allocator and reserved in the same
        spool, however these were injected, and each attempt is reserved
        exactly once. Allocation reads the plan's attempt history and
        reservation creates the attempt's directory, so both run in a worker
        thread, never on the event loop. A reservation that cancellation then
        leaves unused stays behind as an empty directory: it keeps its place in
        the order of the plan's attempts, but shows nothing.

        Args:
            plan_doc_id: The plan document.
            plan: The admitted plan.
            cancel_event: The slot's cooperative cancellation signal.

        Returns:
            tuple[str, bool]: The new execution ID, and whether it was
            reserved.

        Raises:
            _CycleStopped: ADMISSION_FAILED, if no execution ID can be
                allocated and reserved.
        """
        loop = asyncio.get_running_loop()
        allocation = loop.run_in_executor(None, self._engine.reserve_execution_id, plan)
        try:
            return await self._outlast_cancellation(
                allocation,
                cancel_event,
                f"allocating an execution ID for plan '{plan_doc_id}'",
            )
        except Exception as exc:
            raise _CycleStopped(
                ExecutionResult(
                    plan_doc_id,
                    ExecutionStatus.ADMISSION_FAILED,
                    f"Could not allocate and reserve an execution ID for plan "
                    f"'{plan_doc_id}' ({type(exc).__name__}: {exc}); nothing ran, "
                    "it stays eligible",
                )
            ) from exc

    def _admit(
        self, plan_doc_id: str, doc: dict[str, Any] | None, claim: ExecutionClaim
    ) -> _Admission:
        """Capture an execution request from one plan-document snapshot.

        The snapshot's family has already been checked
        (:meth:`_require_supported_family`) and is captured with the request.
        The other checks run in this order: the plan exists; it is the
        caller's to execute; it is eligible, which includes a valid token
        pair; the plan and each of its steps carry every execution field, and
        its failure policy is a known one; it deserializes; it embeds its own
        ID; it has a generation. No default ever stands in for a missing
        execution or control field: those are checked before deserialization
        could supply one. A refused request is not allocated an execution ID,
        and nothing is written.

        Args:
            plan_doc_id: The plan document.
            doc: The snapshot, of a supported family, or None if the plan does
                not exist.
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
        problems = (
            *execution_definition_problems(doc),
            *failure_policy_of(doc).problems,
        )
        if problems:
            # Both checks report a missing or null plan.failure_policy (and a
            # missing plan); each problem is named once.
            named = "; ".join(dict.fromkeys(problems))
            raise stop(
                ExecutionStatus.INVALID_DOCUMENT,
                f"Plan '{plan_doc_id}' does not describe an executable request "
                f"({named}); cannot execute",
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
                f"Plan document '{plan_doc_id}' embeds plan "
                f"{describe_recorded(plan.plan_id)}; cannot execute a plan stored "
                "under another ID",
            )
        generation = generation_of(doc)
        if generation is None:
            raise stop(
                ExecutionStatus.INVALID_DOCUMENT,
                f"Plan '{plan_doc_id}' has no plan_generation to capture; "
                "cannot execute",
            )
        run_token = token_pair(doc).run
        assert run_token is not None  # an eligible plan's tokens are a valid pair
        family = version_of(PLAN_DOCUMENT, doc).family
        assert family is not None  # the family gate admits supported documents only
        return _Admission(
            plan=plan,
            schema_version=family,
            plan_generation=generation,
            run_token=run_token,
            execution_authority=str(doc[EXECUTION_AUTHORITY_FIELD]),
            execution_owner=doc.get(EXECUTION_OWNER_FIELD),
        )

    async def _execute(
        self,
        plan_doc_id: str,
        admission: _Admission,
        execution_id: str,
        reserved: bool,
        cancel_event: threading.Event,
    ) -> ExecutionResult:
        """Run one attempt for an admitted request, then resolve the request.

        Args:
            plan_doc_id: The plan document.
            admission: The captured request.
            execution_id: The execution ID allocated for the attempt.
            reserved: Whether that ID was reserved when it was allocated.
            cancel_event: The slot's cooperative cancellation signal, handed to
                the attempt.

        Returns:
            ExecutionResult: How the request was resolved.
        """
        plan = admission.plan
        context = AttemptContext.for_plan(
            plan,
            execution_id=execution_id,
            plan_generation=admission.plan_generation,
            run_token=admission.run_token,
            execution_authority=admission.execution_authority,
            execution_owner=admission.execution_owner,
            cancel_event=cancel_event,
            execution_id_reserved=reserved,
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
        return await self._resolve_attempt(
            plan_doc_id,
            context,
            raised,
            cancel_event,
            schema_version=admission.schema_version,
        )

    async def _resolve_attempt(
        self,
        plan_doc_id: str,
        context: AttemptContext,
        raised: BaseException | None,
        cancel_event: threading.Event,
        *,
        schema_version: int,
    ) -> ExecutionResult:
        """Record a finished request, or explain why the request is not finished.

        Args:
            plan_doc_id: The plan document.
            context: The ended attempt's context; its report is closed.
            raised: The exception the attempt ended with, if any.
            cancel_event: The slot's cooperative cancellation signal.
            schema_version: The plan document's family captured at admission,
                which the finalizer requires the document to still have.

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

        request = ExecutionFinalization.from_attempt(
            context, schema_version=schema_version
        )
        final = await self._finalize(request, cancel_event)
        if final is None or final.status is FinalizationStatus.INVALID_CONTROL_STATE:
            # Established before _coordinate releases the plan, so no request
            # can slip in between and run the plan again.
            self._pending[plan_doc_id] = _PendingFinalization(request, report)
            refusal = "" if final is None else f" ({final.message})"
            return ExecutionResult(
                plan_doc_id,
                ExecutionStatus.FINALIZATION_PENDING,
                f"Plan '{plan_doc_id}' {_describe_attempt(report)} finished, but "
                f"its result could not be recorded{refusal}; keeping it pending, "
                "and not executing the plan again until it is recorded or "
                "superseded",
                report=report,
                finalization=final,
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

        The family gate has already dropped the result if the document is no
        longer of a supported family (:meth:`_require_supported_family`).
        Otherwise the plan is judged the way the finalizer judges it
        (``check_finalization``), so reading it is enough to settle the
        pending result when the plan was deleted or regenerated, is of a
        family other than the one the result was captured under, its
        authority changed, or the result turns out to be recorded after all.
        Tokens that are not a valid pair, or that recording the result would
        make invalid, keep it pending and retry nothing: the document has to
        change first.
        A regeneration is judged before the tokens, and the authority after
        them, so a regenerated plan settles the result whatever its tokens
        say, and a changed authority settles it only once they are valid.

        A newer run token in the same generation retries the pending
        finalization first: that newer request is the explicit action that
        pending results wait for, and it buys exactly one bounded retry cycle.
        Any other request, for the pending run token or for one that already
        had its cycle, leaves the pending result untouched and retries
        nothing, however often it is repeated.

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
        verdict = check_finalization(doc, request)
        if (
            verdict is not None
            and verdict.status is FinalizationStatus.INVALID_CONTROL_STATE
        ):
            raise still_pending(
                f"recording it is refused ({verdict.message}); not executing the "
                "plan until it is recorded or superseded"
            )
        if verdict is not None:
            del self._pending[plan_doc_id]
            self._logger.warning(
                "Pending finalization of plan '%s' settled without writing: %s",
                plan_doc_id,
                verdict.message,
            )
            return
        requested_token = token_pair(doc).run
        assert requested_token is not None  # judged above: the pair is valid
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
        if final.status is FinalizationStatus.INVALID_CONTROL_STATE:
            raise still_pending(
                f"recording it was refused ({final.message}), so run_token "
                f"{requested_token} waits; only a newer run request retries it "
                "again"
            )
        del self._pending[plan_doc_id]

    async def _finalize(
        self, request: ExecutionFinalization, cancel_event: threading.Event
    ) -> FinalizationResult | None:
        """Record a finished request, retrying within the bound.

        Only a lost race and a retryable store failure are retried. Every
        other result is returned at once, including INVALID_CONTROL_STATE:
        retrying cannot succeed until the plan's tokens change.

        Args:
            request: The finalization to record.
            cancel_event: The slot's cooperative cancellation signal.

        Returns:
            FinalizationResult | None: The recorded, SUPERSEDED or
            INVALID_CONTROL_STATE result; None if no attempt resolved it.
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

        See :func:`~yggdrasil.daemon.worker_futures.outlast_cancellation`. A
        cancellation of this task becomes a cooperative cancellation request
        for the slot, so the attempt starts no further step and the cycle no
        further request; a CancelledError the worker raised itself ends its
        attempt but cancels nothing here.

        Args:
            future: The worker thread's future.
            cancel_event: The slot's cooperative cancellation signal.
            what: What the worker is doing, for logs.

        Returns:
            T: The worker's result.

        Raises:
            BaseException: Whatever the worker raised, CancelledError included.
        """
        return await outlast_cancellation(
            future, functools.partial(self._note_cancellation, cancel_event, what)
        )

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
        self._logger.log(_log_level(result), "%s", result.message)
