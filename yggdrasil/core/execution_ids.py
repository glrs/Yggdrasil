"""Allocate execution IDs that order the attempts at a plan.

An execution ID is ``exec_<timestamp>_<uuid hex>``, with the timestamp in UTC
at microsecond precision (``%Y%m%dT%H%M%S%fZ``), the shape run IDs already
have. Its fixed width makes lexicographic order the same as timestamp order,
which is what lets a reader pick the most recent attempt at a plan by comparing
IDs alone. The ID is fixed at admission, before any step runs, so replaying or
re-delivering an old attempt's events cannot reorder attempts.

A clock is not monotonic, so the timestamp is allocated rather than read:

- It is never lower than one microsecond above the timestamp the same
  allocator allocated last, for any plan. One allocator's IDs are therefore
  strictly increasing even when the clock stands still or moves backwards.
  That floor lives in the allocator instance: each engine has its own
  allocator, and two allocators share only what their common history records.
- It is never lower than one microsecond above any attempt already recorded
  for the plan: every attempt record the ops consumer would select among,
  including reports whose start record is missing. The history is read on
  every allocation, so this holds across restarts, including a restart after
  which the clock is behind the last recorded attempt, and across sequential
  allocators that share one spool. The price is that each allocation parses
  every attempt record retained for the plan, a cost that grows with the
  plan's history.
- The full UUID suffix keeps two IDs distinct. It supplies uniqueness, not
  order: order comes from the timestamp alone.

Attempts made concurrently by independent processes, or by two allocators at
once, are not ordered against each other: each reads the history before the
other has recorded anything. Without a history (see
:class:`ExecutionIdAllocator`), only the first guarantee holds, so a clock
that is behind after a restart can order a new attempt below an old one. A
missing or pruned history therefore degrades ordering to "correct while the
clock moves forward", never to IDs that collide.

An ID without the allocated shape, which only a caller-built attempt context
can carry, orders below every allocated ID (see :func:`execution_order_key`).
It can therefore never hide an allocated attempt, and needs no floor.

The timestamp in an ID is an ordering key, not a record of when the attempt
started; the attempt's report carries that.
"""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Protocol

EXECUTION_ID_PREFIX = "exec_"

_TIMESTAMP_FORMAT = "%Y%m%dT%H%M%S%fZ"
_TIMESTAMP_LENGTH = len("20260101T000000000000Z")
_RESOLUTION = timedelta(microseconds=1)


class AttemptHistory(Protocol):
    """Where the attempts already recorded for a plan can be read back."""

    def recorded_execution_ids(self, realm: str, plan_id: str) -> list[str]:
        """Return the execution IDs recorded for a plan.

        Args:
            realm: The plan's realm.
            plan_id: The plan.

        Returns:
            list[str]: The recorded execution IDs, in any order.
        """
        ...


def format_execution_id(timestamp: datetime, suffix: str) -> str:
    """Build an execution ID from its timestamp and uniqueness suffix.

    Args:
        timestamp: A timezone-aware timestamp; converted to UTC.
        suffix: The uniqueness suffix, normally a full UUID in hex.

    Returns:
        str: The execution ID.
    """
    stamp = timestamp.astimezone(UTC).strftime(_TIMESTAMP_FORMAT)
    return f"{EXECUTION_ID_PREFIX}{stamp}_{suffix}"


def execution_timestamp(execution_id: str) -> datetime | None:
    """Return the timestamp an execution ID was allocated with.

    Args:
        execution_id: The execution ID.

    Returns:
        datetime | None: The ID's timestamp, in UTC; None if the ID does not
        have the allocated shape.
    """
    if not execution_id.startswith(EXECUTION_ID_PREFIX):
        return None
    stamp = execution_id[len(EXECUTION_ID_PREFIX) :][:_TIMESTAMP_LENGTH]
    try:
        return datetime.strptime(stamp, _TIMESTAMP_FORMAT).replace(tzinfo=UTC)
    except ValueError:
        return None


def execution_order_key(execution_id: str) -> tuple[bool, str]:
    """Return the key attempts at a plan are ordered by.

    Allocated IDs order by their timestamp, which for their fixed-width shape
    is the same as their name order. Any other ID orders below every allocated
    one, then by name.

    Args:
        execution_id: The execution ID.

    Returns:
        tuple[bool, str]: Whether the ID has the allocated shape, and the ID.
    """
    return (execution_timestamp(execution_id) is not None, execution_id)


def _utc_now() -> datetime:
    """Return the current time in UTC."""
    return datetime.now(UTC)


def _new_suffix() -> str:
    """Return a new uniqueness suffix: a full UUID in hex."""
    return uuid.uuid4().hex


class ExecutionIdAllocator:
    """Allocates execution IDs for every attempt made through one engine.

    One allocator must serve every way an attempt can start through an engine,
    both the operational callers and direct ``Engine.run`` calls, which is why
    the engine owns it and callers use the engine's. Two allocators share only
    what their common history records.

    Safe to call from several threads at once.
    """

    def __init__(
        self,
        history: AttemptHistory | None = None,
        *,
        clock: Callable[[], datetime] = _utc_now,
        new_suffix: Callable[[], str] = _new_suffix,
    ) -> None:
        """Initialize the allocator.

        Args:
            history: Where the plan's recorded attempts are read back from;
                normally the spool the engine publishes attempt records to.
                None keeps ordering within this allocator only.
            clock: Returns the current time, timezone-aware; replaceable so
                tests can hold or rewind it.
            new_suffix: Returns a new uniqueness suffix; replaceable for tests.
        """
        self._history = history
        self._clock = clock
        self._new_suffix = new_suffix
        self._lock = threading.Lock()
        self._last: datetime | None = None

    @property
    def history(self) -> AttemptHistory | None:
        """Where recorded attempts are read back from, if anywhere."""
        return self._history

    def allocate(self, realm: str, plan_id: str) -> str:
        """Allocate the execution ID of a new attempt at a plan.

        Reads the plan's history first, which blocks: call it off the event
        loop. The history is read outside the allocator's lock, so a slow read
        for one plan does not hold up allocations for others.

        Args:
            realm: The plan's realm.
            plan_id: The plan.

        Returns:
            str: A new execution ID that orders above every ID this allocator
            has returned and every ID recorded for the plan.

        Raises:
            ValueError: If the clock returns a timezone-naive datetime.
            Exception: Whatever reading the history raised.
        """
        recorded = self._recorded_floor(realm, plan_id)
        with self._lock:
            now = self._clock()
            if now.tzinfo is None:
                raise ValueError(
                    "The execution-ID clock must return a timezone-aware datetime"
                )
            candidates = [now.astimezone(UTC)]
            candidates.extend(
                floor + _RESOLUTION
                for floor in (self._last, recorded)
                if floor is not None
            )
            timestamp = max(candidates)
            self._last = timestamp
            return format_execution_id(timestamp, self._new_suffix())

    def _recorded_floor(self, realm: str, plan_id: str) -> datetime | None:
        """Return the latest timestamp recorded for a plan, if any.

        IDs that do not have the allocated shape order nothing and are ignored.

        Args:
            realm: The plan's realm.
            plan_id: The plan.

        Returns:
            datetime | None: The latest recorded timestamp; None if the plan
            has no history, or the allocator has none.
        """
        if self._history is None:
            return None
        stamps = [
            stamp
            for stamp in map(
                execution_timestamp,
                self._history.recorded_execution_ids(realm, plan_id),
            )
            if stamp is not None
        ]
        return max(stamps, default=None)
