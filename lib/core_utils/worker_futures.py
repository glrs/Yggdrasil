"""Wait for work handed to a worker thread without abandoning it on cancellation.

Cancelling a task that awaits a worker thread's future does not stop the
thread, and says nothing about whether it has finished. Work that must not be
left running unobserved (a plan attempt, a finalization, a cycle of snapshot
writes) is therefore awaited through :func:`outlast_cancellation`, which turns
a cancellation of the waiting task into a request the caller handles, and
keeps waiting for the thread's own outcome.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def cancellation_requested() -> bool:
    """Whether the running task has been asked to cancel.

    Tells a cancellation of this task apart from a CancelledError that some
    other code raised, such as a worker's: only ``Task.cancel()`` counts here.
    The count is never withdrawn, so once cancelled, a task stays cancelled
    for this check.

    Returns:
        bool: True if ``cancel()`` has been called on the current task.
    """
    task = asyncio.current_task()
    return task is not None and task.cancelling() > 0


async def outlast_cancellation(
    future: asyncio.Future[T], on_cancel: Callable[[], None]
) -> T:
    """Wait for a worker thread's outcome, even if this task is cancelled.

    The future comes from ``run_in_executor``, not from a task, so shutdown
    routines that cancel every task never cancel it; and awaiting it through
    ``asyncio.shield`` means cancelling this task cannot cancel it either.

    A CancelledError at the await means one of two things, and whether the
    future has finished tells them apart. While it has not, this task was
    cancelled and the worker is still running: ``on_cancel`` is called, and
    the wait goes on. Once it has, the worker's own outcome is returned or
    raised unchanged, including a CancelledError that the worker raised
    itself, which cancels nothing here. A finished future is never awaited
    again: that would re-raise its exception without yielding, and a loop
    doing so would stall the event loop.

    Args:
        future: The worker thread's future.
        on_cancel: Called each time this task is cancelled while the worker
            is still running, so the caller can stop starting further work.

    Returns:
        T: The worker's result.

    Raises:
        BaseException: Whatever the worker raised, CancelledError included.
    """
    while not future.done():
        try:
            await asyncio.shield(future)
        except asyncio.CancelledError:
            # Handled even when the worker finished at the same moment: its
            # outcome still stands, but the caller starts nothing further.
            if cancellation_requested():
                on_cancel()
    return future.result()
