"""Tests for execution-ID allocation.

Each test pins one way an ordering key can fail. The clock tests are the ones
that motivated allocating timestamps rather than reading them: a clock that
stands still, one that moves backwards within a process, and a restart after
which the clock is behind the last recorded attempt. The last one cannot pass
without reading the recorded attempts back.

Clocks are injected; nothing here sleeps. Records are written the way the
engine writes them, through a FileSpoolEmitter under the attempt-record name.
"""

import re
import threading
import unittest
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from tests.execution_support import Clock
from yggdrasil.core.execution_ids import (
    ExecutionIdAllocator,
    execution_timestamp,
    format_execution_id,
)
from yggdrasil.flow.events.attempt_records import (
    ATTEMPT_STARTED_EVENT,
    SpoolAttemptHistory,
    record_filename,
)
from yggdrasil.flow.events.emitter import FileSpoolEmitter

REALM = "test_realm"
PLAN_ID = "pln_ids"
T0 = datetime(2026, 9, 21, 12, 5, tzinfo=UTC)
ONE_MICROSECOND = timedelta(microseconds=1)
ID_SHAPE = re.compile(r"^exec_\d{8}T\d{12}Z_[0-9a-f]{32}$")

# Upper bound for cross-thread handshakes. Never slept on.
WAIT = 5.0


def record_attempt(spool: Path, execution_id: str, plan_id: str = PLAN_ID) -> None:
    """Record an attempt start in a spool, as the engine does."""
    FileSpoolEmitter(spool).emit(
        {
            "type": ATTEMPT_STARTED_EVENT,
            "realm": REALM,
            "plan_id": plan_id,
            "execution_id": execution_id,
            "_spool_path": {
                "realm": REALM,
                "plan_id": plan_id,
                "filename": record_filename(execution_id, ATTEMPT_STARTED_EVENT),
            },
        }
    )


class HistoryStub:
    """A history returning fixed IDs, or raising once given an error."""

    def __init__(self, ids: list[str] | None = None) -> None:
        self.ids = list(ids or [])
        self.error: Exception | None = None

    def recorded_execution_ids(self, realm: str, plan_id: str) -> list[str]:
        if self.error is not None:
            raise self.error
        return list(self.ids)


class SpoolTestCase(unittest.TestCase):
    """Provides a temporary spool directory."""

    def setUp(self) -> None:
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.spool = Path(temp_dir.name) / "spool"


class TestExecutionIdShape(unittest.TestCase):
    """IDs have the run-ID shape, so name order is timestamp order."""

    def test_id_carries_its_timestamp_and_a_full_uuid_suffix(self):
        execution_id = ExecutionIdAllocator(clock=Clock(T0)).allocate(REALM, PLAN_ID)

        self.assertRegex(execution_id, ID_SHAPE)
        self.assertEqual(execution_timestamp(execution_id), T0)

    def test_name_order_is_timestamp_order(self):
        stamps = [
            T0 + timedelta(days=400),
            T0 + ONE_MICROSECOND,
            T0,
            T0 + timedelta(seconds=1),
            T0 - timedelta(hours=10),
        ]
        # Suffixes chosen to disagree with the timestamps' order.
        ids = [
            format_execution_id(stamp, suffix * 32)
            for stamp, suffix in zip(stamps, "0f5a9", strict=True)
        ]

        self.assertEqual(
            sorted(ids), [i for _, i in sorted(zip(stamps, ids, strict=True))]
        )

    def test_ids_without_the_allocated_shape_have_no_timestamp(self):
        for execution_id in (
            "",
            "exec_",
            "exec_sched_plan",
            "exec_2026",
            "run_20260921T120500000000Z_abcdef",
            "exec-20260921T120500000000Z_abc",
        ):
            with self.subTest(execution_id=execution_id):
                self.assertIsNone(execution_timestamp(execution_id))

    def test_timestamp_is_converted_to_utc(self):
        plus_two = datetime(2026, 9, 21, 16, 5, tzinfo=timezone(timedelta(hours=2)))

        self.assertEqual(
            execution_timestamp(format_execution_id(plus_two, "x")),
            datetime(2026, 9, 21, 14, 5, tzinfo=UTC),
        )


class TestClockSafety(SpoolTestCase):
    """Allocation keeps its order whatever the clock does."""

    def test_frozen_clock_still_gives_distinct_ordered_ids(self):
        allocator = ExecutionIdAllocator(clock=Clock(T0))

        first = allocator.allocate(REALM, PLAN_ID)
        second = allocator.allocate(REALM, PLAN_ID)

        self.assertNotEqual(first, second)
        self.assertLess(first, second)
        self.assertEqual(execution_timestamp(second), T0 + ONE_MICROSECOND)

    def test_clock_moving_back_within_a_process_is_clamped(self):
        clock = Clock(T0)
        allocator = ExecutionIdAllocator(clock=clock)
        first = allocator.allocate(REALM, PLAN_ID)
        clock.now = T0 - timedelta(hours=1)

        second = allocator.allocate(REALM, PLAN_ID)

        self.assertLess(first, second)
        self.assertEqual(execution_timestamp(second), T0 + ONE_MICROSECOND)

    def test_clock_moving_forward_again_is_followed(self):
        clock = Clock(T0)
        allocator = ExecutionIdAllocator(clock=clock)
        allocator.allocate(REALM, PLAN_ID)
        clock.now = T0 + timedelta(minutes=3)

        later = allocator.allocate(REALM, PLAN_ID)

        self.assertEqual(execution_timestamp(later), T0 + timedelta(minutes=3))

    def test_order_holds_across_plans_within_a_process(self):
        clock = Clock(T0)
        allocator = ExecutionIdAllocator(clock=clock)
        first = allocator.allocate(REALM, "plan_a")
        clock.now = T0 - timedelta(hours=1)

        second = allocator.allocate(REALM, "plan_b")

        self.assertLess(first, second)

    def test_restart_with_the_clock_behind_the_last_recorded_attempt(self):
        before = ExecutionIdAllocator(SpoolAttemptHistory(self.spool), clock=Clock(T0))
        recorded = before.allocate(REALM, PLAN_ID)
        record_attempt(self.spool, recorded)
        behind = Clock(T0 - timedelta(hours=1))

        # A new allocator is a restarted process: it remembers nothing.
        after = ExecutionIdAllocator(SpoolAttemptHistory(self.spool), clock=behind)
        next_id = after.allocate(REALM, PLAN_ID)

        self.assertGreater(next_id, recorded)
        self.assertEqual(execution_timestamp(next_id), T0 + ONE_MICROSECOND)
        # Without the read-back, the same restart orders the attempt below.
        forgetful = ExecutionIdAllocator(clock=behind)
        self.assertLess(forgetful.allocate(REALM, PLAN_ID), recorded)

    def test_history_is_read_on_every_allocation(self):
        # Another allocator on the same spool records a later attempt after
        # this one last allocated for the plan.
        history = SpoolAttemptHistory(self.spool)
        allocator = ExecutionIdAllocator(history, clock=Clock(T0))
        allocator.allocate(REALM, PLAN_ID)
        elsewhere = format_execution_id(T0 + timedelta(hours=2), "e" * 32)
        record_attempt(self.spool, elsewhere)

        next_id = allocator.allocate(REALM, PLAN_ID)

        self.assertGreater(next_id, elsewhere)

    def test_records_of_other_plans_do_not_move_a_plan_forward(self):
        record_attempt(
            self.spool, format_execution_id(T0 + timedelta(days=1), "e" * 32), "other"
        )
        allocator = ExecutionIdAllocator(
            SpoolAttemptHistory(self.spool), clock=Clock(T0)
        )

        self.assertEqual(execution_timestamp(allocator.allocate(REALM, PLAN_ID)), T0)

    def test_separate_histories_stay_isolated(self):
        other_spool = self.spool.parent / "other_spool"
        record_attempt(
            other_spool, format_execution_id(T0 + timedelta(days=1), "e" * 32)
        )
        allocator = ExecutionIdAllocator(
            SpoolAttemptHistory(self.spool), clock=Clock(T0)
        )

        self.assertEqual(execution_timestamp(allocator.allocate(REALM, PLAN_ID)), T0)

    def test_recorded_ids_without_the_allocated_shape_are_ignored(self):
        history = HistoryStub(["exec_sched_plan", "zzz", "exec_2099"])
        allocator = ExecutionIdAllocator(history, clock=Clock(T0))

        self.assertEqual(execution_timestamp(allocator.allocate(REALM, PLAN_ID)), T0)

    def test_failed_history_read_allocates_nothing(self):
        history = HistoryStub()
        history.error = PermissionError("spool unreadable")
        allocator = ExecutionIdAllocator(history, clock=Clock(T0))

        with self.assertRaises(PermissionError):
            allocator.allocate(REALM, PLAN_ID)

        history.error = None
        self.assertEqual(execution_timestamp(allocator.allocate(REALM, PLAN_ID)), T0)

    def test_naive_clock_is_rejected(self):
        allocator = ExecutionIdAllocator(clock=lambda: datetime(2026, 9, 21, 12, 5))

        with self.assertRaises(ValueError):
            allocator.allocate(REALM, PLAN_ID)


class TestConcurrentAllocation(unittest.TestCase):
    """Allocation is synchronized; a slow history read holds up nobody else."""

    def test_concurrent_allocations_are_distinct_and_ordered(self):
        allocator = ExecutionIdAllocator(clock=Clock(T0))
        ids: list[str] = []
        lock = threading.Lock()
        start = threading.Barrier(8)

        def allocate_many() -> None:
            start.wait(WAIT)
            for _ in range(50):
                allocated = allocator.allocate(REALM, PLAN_ID)
                with lock:
                    ids.append(allocated)

        threads = [threading.Thread(target=allocate_many) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(WAIT)

        self.assertEqual(len(ids), 400)
        stamps = [execution_timestamp(i) for i in ids]
        self.assertEqual(len(set(stamps)), 400, "two allocations shared a timestamp")
        self.assertEqual(
            sorted(ids), [i for _, i in sorted(zip(stamps, ids, strict=True))]
        )

    def test_slow_history_read_for_one_plan_does_not_hold_up_another(self):
        reading, finish = threading.Event(), threading.Event()

        class SlowForOnePlan:
            def recorded_execution_ids(self, realm: str, plan_id: str) -> list[str]:
                if plan_id == "slow":
                    reading.set()
                    if not finish.wait(WAIT):
                        raise TimeoutError("never released")
                return []

        allocator = ExecutionIdAllocator(SlowForOnePlan(), clock=Clock(T0))
        slow = threading.Thread(target=allocator.allocate, args=(REALM, "slow"))
        slow.start()
        self.addCleanup(slow.join, WAIT)
        self.addCleanup(finish.set)
        self.assertTrue(reading.wait(WAIT))

        # Returns while the other plan's read is still in progress.
        self.assertRegex(allocator.allocate(REALM, "fast"), ID_SHAPE)


if __name__ == "__main__":
    unittest.main()
