"""Tests for AttemptContext, the per-attempt carrier.

Its reason to exist is the failure path: an attempt that ends by raising cannot
return anything, so the outcomes determined before the exception have to reach
the caller some other way. These tests pin that down, along with the isolation
that lets one Engine instance run unrelated plans without leaking state.
"""

import threading
import unittest

from yggdrasil.flow.attempt import AttemptContext
from yggdrasil.flow.model import Plan, StepSpec
from yggdrasil.flow.outcomes import (
    AttemptDiagnostic,
    ExecutionOutcome,
    StepFailure,
    StepOutcome,
    TerminationReason,
)


def _plan(*step_ids: str, failure_policy: str = "fail_fast") -> Plan:
    """Build a plan whose steps carry the given IDs, in order."""
    return Plan(
        plan_id="plan_1",
        realm="test",
        scope={"kind": "project", "id": "P1"},
        steps=[
            StepSpec(step_id=sid, name=sid, fn_ref="m:f", params={}) for sid in step_ids
        ],
        failure_policy=failure_policy,
    )


class TestAttemptContextConstruction(unittest.TestCase):
    """for_plan() seeds the report from the plan."""

    def test_for_plan_seeds_the_planned_step_inventory_in_order(self):
        """Without the inventory, 'never reached' would not be computable."""
        ctx = AttemptContext.for_plan(_plan("c", "a", "b"), execution_id="exec_1")

        self.assertEqual(ctx.report.step_ids, ["c", "a", "b"])
        self.assertEqual(ctx.report.unreached_step_ids, ["c", "a", "b"])

    def test_for_plan_captures_identity_and_policy(self):
        ctx = AttemptContext.for_plan(
            _plan("a", failure_policy="continue_independent"),
            execution_id="exec_7",
            plan_generation="gen_7",
            run_token=4,
            execution_authority="run_once",
            execution_owner="run_once:abc",
        )

        self.assertEqual(ctx.execution_id, "exec_7")
        self.assertEqual(ctx.plan_id, "plan_1")
        self.assertEqual(ctx.plan_generation, "gen_7")
        self.assertEqual(ctx.run_token, 4)
        self.assertEqual(ctx.report.failure_policy, "continue_independent")
        self.assertEqual(ctx.execution_authority, "run_once")
        self.assertEqual(ctx.execution_owner, "run_once:abc")

    def test_for_plan_requires_an_explicit_execution_id(self):
        """Allocating the ID is the caller's concern, not the context's."""
        with self.assertRaises(TypeError):
            AttemptContext.for_plan(_plan("a"))

    def test_identity_reads_through_to_the_report(self):
        """One copy of the identity, so nothing can drift out of sync."""
        ctx = AttemptContext.for_plan(
            _plan("a"), execution_id="exec_1", plan_generation="gen_1", run_token=2
        )

        self.assertEqual(ctx.execution_id, ctx.report.execution_id)
        self.assertEqual(ctx.plan_id, ctx.report.plan_id)
        self.assertEqual(ctx.plan_generation, ctx.report.plan_generation)
        self.assertEqual(ctx.run_token, ctx.report.run_token)

    def test_defaults_for_an_unmanaged_direct_execution(self):
        ctx = AttemptContext.for_plan(_plan("a"), execution_id="exec_1")

        self.assertIsNone(ctx.plan_generation)
        self.assertIsNone(ctx.run_token)
        self.assertEqual(ctx.execution_authority, "daemon")
        self.assertIsNone(ctx.execution_owner)


class TestAttemptContextIsolation(unittest.TestCase):
    """Two attempts through one Engine instance must not observe each other."""

    def test_two_contexts_share_no_state(self):
        first = AttemptContext.for_plan(_plan("a"), execution_id="exec_1")
        second = AttemptContext.for_plan(_plan("b"), execution_id="exec_2")

        first.report.record_outcome("a", StepOutcome.SUCCEEDED)
        first.request_cancellation()

        self.assertEqual(second.report.step_outcomes, {})
        self.assertEqual(second.report.unreached_step_ids, ["b"])
        self.assertFalse(second.cancellation_requested)
        self.assertIsNot(first.cancel_event, second.cancel_event)

    def test_a_shared_cancel_event_can_be_injected(self):
        """A caller coordinating shutdown may own the signal itself."""
        shared = threading.Event()
        ctx = AttemptContext.for_plan(
            _plan("a"), execution_id="exec_1", cancel_event=shared
        )

        shared.set()

        self.assertTrue(ctx.cancellation_requested)


class TestAttemptContextCancellation(unittest.TestCase):
    """The cooperative cancellation signal."""

    def test_starts_unset_and_is_idempotent(self):
        ctx = AttemptContext.for_plan(_plan("a"), execution_id="exec_1")

        self.assertFalse(ctx.cancellation_requested)
        ctx.request_cancellation()
        ctx.request_cancellation()
        self.assertTrue(ctx.cancellation_requested)

    def test_can_be_signaled_from_another_thread(self):
        """The engine runs in a worker thread; the caller signals from its own."""
        ctx = AttemptContext.for_plan(_plan("a"), execution_id="exec_1")
        observed: list[bool] = []
        ready = threading.Event()

        def worker() -> None:
            ready.wait(timeout=5)
            observed.append(ctx.cancellation_requested)

        thread = threading.Thread(target=worker)
        thread.start()
        ctx.request_cancellation()
        ready.set()
        thread.join(timeout=5)

        self.assertEqual(observed, [True])


class TestAttemptContextSurvivesExceptions(unittest.TestCase):
    """The context's key guarantee: outcomes survive an attempt that raises."""

    def test_context_still_exposes_outcomes_and_reason_after_a_raise(self):
        ctx = AttemptContext.for_plan(
            _plan("root", "dependent", "independent", "never_reached"),
            execution_id="exec_1",
        )

        def failing_attempt(context: AttemptContext) -> None:
            """Stand-in for an attempt that records, then propagates."""
            context.report.record_failure(
                StepFailure(step_id="root", error="boom", kind="permanent")
            )
            context.report.record_blocked("dependent", ["root"])
            context.report.record_outcome("independent", StepOutcome.SUCCEEDED)
            context.report.finish(TerminationReason.FAILED_FAST)
            raise RuntimeError("attempt aborted")

        with self.assertRaises(RuntimeError):
            failing_attempt(ctx)

        # The caller still holds the context, so everything determined before
        # the exception is readable.
        report = ctx.report
        self.assertEqual(report.step_outcomes["root"], StepOutcome.FAILED)
        self.assertEqual(report.failures["root"].error, "boom")
        self.assertEqual(report.step_outcomes["dependent"], StepOutcome.BLOCKED)
        self.assertEqual(report.direct_blockers["dependent"], ["root"])
        self.assertEqual(report.step_outcomes["independent"], StepOutcome.SUCCEEDED)
        # Work never evaluated stays unreached rather than being called blocked.
        self.assertEqual(report.unreached_step_ids, ["never_reached"])
        self.assertEqual(report.termination_reason, TerminationReason.FAILED_FAST)
        self.assertEqual(report.outcome, ExecutionOutcome.FAILED)
        self.assertFalse(report.is_drained)

    def test_orchestration_failure_is_readable_without_a_failed_step(self):
        ctx = AttemptContext.for_plan(_plan("a", "b"), execution_id="exec_1")

        def failing_attempt(context: AttemptContext) -> None:
            context.report.record_outcome("a", StepOutcome.SUCCEEDED)
            context.report.record_diagnostic(
                AttemptDiagnostic(message="event spool unavailable")
            )
            context.report.finish(TerminationReason.ORCHESTRATION_ERROR)
            raise OSError("spool gone")

        with self.assertRaises(OSError):
            failing_attempt(ctx)

        self.assertEqual(ctx.report.failures, {})
        self.assertEqual(ctx.report.diagnostic.message, "event spool unavailable")
        self.assertEqual(
            ctx.report.termination_reason, TerminationReason.ORCHESTRATION_ERROR
        )
        self.assertEqual(ctx.report.unreached_step_ids, ["b"])


if __name__ == "__main__":
    unittest.main()
