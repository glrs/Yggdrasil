"""Tests for the plan-execution coordinator shared by the daemon and run-once.

Attempts run through a real Engine and real ``@step`` functions against a real
plan store (see ``tests/execution_support.py``). Each test states a guarantee
and would fail if that guarantee were removed:

- a request is admitted from one plan snapshot, whatever prompted it;
- how an attempt ended, not the exception it raised, decides whether its
  request is consumed: a success, a drained ``continue_independent`` failure
  and a ``continue_independent`` preflight rejection are recorded, and every
  other ending leaves the plan eligible;
- at most one attempt per plan is in flight, excluded until its result is
  recorded or retained; a request arriving meanwhile is rechecked, not lost,
  and runs only if it is newer than the request that attempt ran;
- cancellation, of a caller or of the coordination itself, stops new steps but
  never abandons a running worker or a drained result; a coordination
  cancelled before it starts still releases its plan, and a CancelledError a
  step raises itself ends that attempt without stalling anything else;
- finalization is retried within its bound, never re-executes the plan, and a
  result it cannot record is kept and settled before the plan runs again, with
  one more bounded retry cycle per newer run request.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import unittest
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from lib.core_utils.plan_eligibility import is_plan_eligible
from lib.core_utils.plan_execution import (
    DAEMON_CLAIM,
    ExecutionClaim,
    ExecutionStatus,
    PlanExecutionCoordinator,
)
from lib.ops.consumer import build_plan_snapshot
from lib.storage.errors import PlanStoreError
from lib.storage.plan_documents import build_plan_document
from lib.storage.plan_updates import FinalizationStatus, SupersessionReason
from tests.execution_support import (
    PLAN_ID,
    REALM,
    RETRYABLE,
    SCOPE,
    WAIT,
    CapturingEngine,
    Clock,
    CouchBackend,
    ExecutionTestCase,
    Gate,
    chain_plan,
    lanes_plan,
    make_plan,
    run_bounded,
    spec,
)
from yggdrasil.core.execution_ids import ExecutionIdAllocator, execution_timestamp
from yggdrasil.flow.attempt import AttemptContext
from yggdrasil.flow.errors import (
    AttemptCancelledError,
    OrchestrationError,
    PreflightValidationError,
)
from yggdrasil.flow.events.attempt_records import (
    ATTEMPT_REPORT_EVENT,
    ATTEMPT_STARTED_EVENT,
)
from yggdrasil.flow.events.emitter import FileSpoolEmitter
from yggdrasil.flow.model import CONTINUE_INDEPENDENT_POLICY, FAIL_FAST_POLICY, Plan
from yggdrasil.flow.outcomes import (
    AttemptReport,
    ExecutionOutcome,
    StepOutcome,
    TerminationReason,
)

CONTINUE = CONTINUE_INDEPENDENT_POLICY
FAIL_FAST = FAIL_FAST_POLICY
POLICIES = (FAIL_FAST, CONTINUE)

T0 = datetime(2026, 9, 21, 12, 5, tzinfo=UTC)

COORDINATOR_LOGGER = "lib.core_utils.plan_execution.PlanExecutionCoordinator"


def cyclic_plan(policy: str, plan_id: str = PLAN_ID) -> Plan:
    """A plan whose two steps depend on each other: preflight rejects it."""
    return make_plan(spec("a", "b"), spec("b", "a"), policy=policy, plan_id=plan_id)


class ConsumptionScenarios:
    """Which endings consume an execution request, against one backend.

    Mixed into one test case per plan-store backend.
    """

    def test_successful_attempt_is_recorded_under_either_policy(self):
        for policy in POLICIES:
            plan_id = f"{PLAN_ID}_{policy}"
            with self.subTest(policy=policy):
                self.save(chain_plan(policy, plan_id=plan_id))

                result = self.execute(plan_id)

                self.assertEqual(result.status, ExecutionStatus.FINALIZED)
                self.assertTrue(result.succeeded)
                self.assertEqual(
                    result.finalization.status, FinalizationStatus.COMMITTED
                )
                self.assert_recorded(result)
                self.assertFalse(is_plan_eligible(self.stored(plan_id)))

    def test_fail_fast_failure_leaves_the_request_eligible(self):
        self.steps.fail("a", RuntimeError("boom"))
        self.save(chain_plan(FAIL_FAST))

        result = self.execute()

        self.assertEqual(result.status, ExecutionStatus.UNFINISHED)
        self.assertEqual(
            result.report.termination_reason, TerminationReason.FAILED_FAST
        )
        self.assertEqual(self.steps.calls, ["a"])
        self.assertEqual(self.store.calls["finalize"], 0)
        self.assert_unconsumed()
        self.assertTrue(is_plan_eligible(self.stored()))

    def test_fail_fast_preflight_rejection_leaves_the_request_eligible(self):
        self.save(cyclic_plan(FAIL_FAST))

        result = self.execute()

        self.assertEqual(result.status, ExecutionStatus.UNFINISHED)
        self.assertEqual(
            result.report.termination_reason, TerminationReason.PREFLIGHT_REJECTED
        )
        self.assertEqual(self.steps.calls, [])
        self.assert_unconsumed()
        self.assertTrue(is_plan_eligible(self.stored()))

    def test_drained_continuation_failure_consumes_the_request(self):
        # Lane 1 fails and its upload is blocked; lane 2 still completes.
        self.steps.fail("lane1_demux", RuntimeError("bad sample sheet"))
        self.save(lanes_plan(CONTINUE))

        result = self.execute()

        self.assertEqual(result.status, ExecutionStatus.FINALIZED)
        self.assertFalse(result.succeeded)
        report = result.report
        self.assertEqual(report.outcome, ExecutionOutcome.FAILED)
        self.assertEqual(report.termination_reason, TerminationReason.COMPLETED)
        self.assertEqual(
            report.step_outcomes,
            {
                "lane1_demux": StepOutcome.FAILED,
                "lane1_upload": StepOutcome.BLOCKED,
                "lane2_demux": StepOutcome.SUCCEEDED,
                "lane2_upload": StepOutcome.SUCCEEDED,
            },
        )
        self.assert_recorded(result)
        self.assertEqual(self.stored()["last_finalized_execution"]["outcome"], "failed")
        self.assertFalse(is_plan_eligible(self.stored()))

    def test_continuation_preflight_rejection_consumes_the_request(self):
        self.save(cyclic_plan(CONTINUE))

        result = self.execute()

        self.assertEqual(result.status, ExecutionStatus.FINALIZED)
        self.assertEqual(
            result.report.termination_reason, TerminationReason.PREFLIGHT_REJECTED
        )
        self.assertIn("cycle", result.report.diagnostic.message)
        self.assertEqual(self.steps.calls, [])
        self.assert_recorded(result)
        self.assertFalse(is_plan_eligible(self.stored()))

    def test_orchestration_failure_leaves_the_request_eligible(self):
        self.emitter.fail_on.add("step.started")
        for policy in POLICIES:
            plan_id = f"{PLAN_ID}_{policy}"
            with self.subTest(policy=policy):
                self.save(chain_plan(policy, plan_id=plan_id))

                result = self.execute(plan_id)

                self.assertEqual(result.status, ExecutionStatus.UNFINISHED)
                self.assertEqual(
                    result.report.termination_reason,
                    TerminationReason.ORCHESTRATION_ERROR,
                )
                self.assert_unconsumed(plan_id)
        self.assertEqual(self.store.calls["finalize"], 0)

    def test_consumption_follows_the_ending_not_the_exception_type(self):
        # A step body may raise the engine's own control-flow types. They are
        # that step's ordinary failure, so the policy decides, as for any other.
        cases = [
            (FAIL_FAST, PreflightValidationError("raised by a step"), False),
            (CONTINUE, PreflightValidationError("raised by a step"), True),
            (CONTINUE, AttemptCancelledError("raised by a step"), True),
        ]
        for index, (policy, error, consumed) in enumerate(cases):
            plan_id = f"{PLAN_ID}_{index}"
            with self.subTest(policy=policy, error=type(error).__name__):
                self.steps.fail("a", error)
                self.save(chain_plan(policy, plan_id=plan_id))

                result = self.execute(plan_id)

                if consumed:
                    self.assertEqual(result.status, ExecutionStatus.FINALIZED)
                    self.assert_recorded(result)
                else:
                    self.assertEqual(result.status, ExecutionStatus.UNFINISHED)
                    self.assert_unconsumed(plan_id)

    def test_legacy_plan_is_finalized_against_the_generation_it_is_given(self):
        legacy = build_plan_document(
            chain_plan(), REALM, dict(SCOPE), auto_run=True, execution_owner=None
        )
        del legacy["plan_generation"]
        self.backend.raw_put(legacy)

        result = self.execute()

        self.assertEqual(result.status, ExecutionStatus.FINALIZED)
        generation = self.stored()["plan_generation"]
        self.assertTrue(generation)
        self.assertEqual(result.report.plan_generation, generation)
        self.assertEqual(
            self.stored()["last_finalized_execution"]["plan_generation"], generation
        )


class TestConsumptionSQLite(ConsumptionScenarios, ExecutionTestCase):
    """The consumption rule against the SQLite plan store."""


class TestConsumptionCouch(ConsumptionScenarios, ExecutionTestCase):
    """The consumption rule against the CouchDB plan store."""

    backend_type = CouchBackend


class TestAdmission(ExecutionTestCase):
    """Everything about a request is captured from one current snapshot."""

    def test_current_version_is_executed_with_its_own_generation(self):
        stale = self.save(make_plan(spec("a", sample="old")))
        # Regenerated after whatever prompted this execution was observed.
        current = self.save(make_plan(spec("a", sample="new")))
        self.assertNotEqual(stale["plan_generation"], current["plan_generation"])

        result = self.execute()

        self.assertEqual(self.steps.params["a"], {"sample": "new"})
        self.assertEqual(result.report.plan_generation, current["plan_generation"])
        self.assert_recorded(result)

    def test_unapproved_plan_is_not_executed(self):
        self.save(auto_run=False)

        result = self.execute()

        self.assertEqual(result.status, ExecutionStatus.NOT_ELIGIBLE)
        self.assertEqual(self.engine.contexts, [])

    def test_served_request_is_not_executed_again(self):
        self.save()
        self.assertEqual(self.execute().status, ExecutionStatus.FINALIZED)

        result = self.execute()

        self.assertEqual(result.status, ExecutionStatus.NOT_ELIGIBLE)
        self.assertEqual(len(self.engine.contexts), 1)

    def test_plan_under_another_authority_or_owner_is_not_executed(self):
        self.save(authority="run_once", owner="run_once:session-a")

        for claim in (
            DAEMON_CLAIM,
            ExecutionClaim(authority="run_once", owner="run_once:session-b"),
        ):
            with self.subTest(claim=claim):
                result = self.execute(claim=claim)
                self.assertEqual(result.status, ExecutionStatus.NOT_AUTHORIZED)
        self.assertEqual(self.engine.contexts, [])

        owned = self.execute(
            claim=ExecutionClaim(authority="run_once", owner="run_once:session-a")
        )
        self.assertEqual(owned.status, ExecutionStatus.FINALIZED)
        self.assertEqual(self.engine.contexts[0].execution_owner, "run_once:session-a")

    def test_missing_plan_is_not_found(self):
        result = self.execute("pln_missing")

        self.assertEqual(result.status, ExecutionStatus.NOT_FOUND)
        self.assertEqual(self.engine.contexts, [])

    def test_document_without_an_executable_plan_is_not_executed(self):
        # A document under one ID embedding another plan would be finalized
        # into the embedded plan's document.
        misfiled = build_plan_document(chain_plan(), REALM, dict(SCOPE), auto_run=True)
        misfiled["_id"] = "pln_misfiled"
        planless = build_plan_document(chain_plan(), REALM, dict(SCOPE), auto_run=True)
        planless["_id"] = "pln_planless"
        planless["plan"] = {}
        for doc in (misfiled, planless):
            self.backend.raw_put(doc)
            with self.subTest(doc_id=doc["_id"]):
                result = self.execute(doc["_id"])
                self.assertEqual(result.status, ExecutionStatus.INVALID_DOCUMENT)
                self.assertTrue(is_plan_eligible(self.stored(doc["_id"])))
        self.assertEqual(self.engine.contexts, [])

    def test_failed_admission_read_leaves_the_plan_eligible(self):
        self.save()

        def fail(doc_id):
            raise PlanStoreError("database is locked", retryable=True)

        self.store.read_actions.append(fail)

        result = self.execute()

        self.assertEqual(result.status, ExecutionStatus.ADMISSION_FAILED)
        self.assertEqual(self.engine.contexts, [])
        self.assert_unconsumed()

    def test_claim_rejects_an_unknown_authority(self):
        with self.assertRaises(ValueError):
            ExecutionClaim(authority="cron")


class TestExclusion(ExecutionTestCase):
    """One attempt per plan in flight; requests arriving meanwhile are kept."""

    def test_duplicate_requests_run_once_and_the_second_is_rechecked_after(self):
        self.save()

        async def scenario():
            first = self.coordinator.submit(PLAN_ID, DAEMON_CLAIM)
            second = self.coordinator.submit(PLAN_ID, DAEMON_CLAIM)
            return await first, await second

        first, second = run_bounded(scenario())

        self.assertEqual(first.status, ExecutionStatus.FINALIZED)
        # Rechecked from a fresh read once the first was recorded: not lost,
        # and not run a second time.
        self.assertEqual(second.status, ExecutionStatus.NOT_ELIGIBLE)
        self.assertEqual(self.journal, ["read", "attempt:0", "finalize:0", "read"])
        self.assertFalse(self.coordinator.is_in_flight(PLAN_ID))

    def test_newer_request_arriving_during_an_attempt_runs_after_it(self):
        gate = self.steps.block("a")
        self.save()

        async def scenario():
            first = self.coordinator.submit(PLAN_ID, DAEMON_CLAIM)
            await gate.reached()
            await asyncio.to_thread(self.request_rerun)
            second = self.coordinator.submit(PLAN_ID, DAEMON_CLAIM)
            gate.release()
            return await first, await second

        first, second = run_bounded(scenario())

        self.assertEqual(first.report.run_token, 0)
        self.assertEqual(second.status, ExecutionStatus.FINALIZED)
        self.assertEqual(second.report.run_token, 1)
        self.assertEqual(
            self.journal,
            ["read", "attempt:0", "finalize:0", "read", "attempt:1", "finalize:1"],
        )
        self.assertEqual(self.steps.max_running, 1)
        self.assert_recorded(second, run_token=1)

    def test_distinct_plans_are_not_serialized(self):
        gate = self.steps.block("a")
        self.save()
        self.save(make_plan(spec("x"), plan_id="pln_other"))

        async def scenario():
            first = self.coordinator.submit(PLAN_ID, DAEMON_CLAIM)
            await gate.reached()
            # Completes while the first plan is still executing.
            other = await self.coordinator.execute("pln_other", DAEMON_CLAIM)
            self.assertTrue(self.coordinator.is_in_flight(PLAN_ID))
            gate.release()
            return await first, other

        first, other = run_bounded(scenario())

        self.assertTrue(first.succeeded)
        self.assertTrue(other.succeeded)

    def test_plan_stays_excluded_until_its_result_is_recorded(self):
        self.save()
        paused = self.store.pause_finalize()

        async def scenario():
            first = self.coordinator.submit(PLAN_ID, DAEMON_CLAIM)
            await paused.reached()
            # The worker is done; the result is not yet recorded.
            self.assertTrue(self.coordinator.is_in_flight(PLAN_ID))
            second = self.coordinator.submit(PLAN_ID, DAEMON_CLAIM)
            self.assertEqual(len(self.engine.contexts), 1)
            paused.release()
            return await first, await second

        first, second = run_bounded(scenario())

        self.assertEqual(first.status, ExecutionStatus.FINALIZED)
        self.assertEqual(second.status, ExecutionStatus.NOT_ELIGIBLE)
        self.assertEqual(len(self.engine.contexts), 1)

    def test_duplicate_of_an_unfinished_attempt_is_not_run_again(self):
        # The duplicate arrived while the attempt was running, so it asks for
        # the very request that attempt ran. Running it again would retry a
        # failure nobody asked to retry; a later request still retries it.
        gate = Gate()

        def fail_at_gate(ctx):
            gate.pass_through()
            raise RuntimeError("boom")

        self.steps.behaviors["a"] = fail_at_gate
        self.save()

        async def scenario():
            first = self.coordinator.submit(PLAN_ID, DAEMON_CLAIM)
            await gate.reached()
            duplicate = self.coordinator.submit(PLAN_ID, DAEMON_CLAIM)
            gate.release()
            return await first, await duplicate

        first, duplicate = run_bounded(scenario())

        self.assertEqual(first.status, ExecutionStatus.UNFINISHED)
        self.assertEqual(duplicate.status, ExecutionStatus.DUPLICATE)
        self.assertEqual(self.journal, ["read", "attempt:0", "read"])
        self.assert_unconsumed()
        self.assertEqual(self.execute().status, ExecutionStatus.UNFINISHED)
        self.assertEqual(len(self.engine.contexts), 2)

    def test_newer_request_after_an_unfinished_attempt_still_runs(self):
        gate = Gate()
        attempts = []

        def fail_first_attempt_at_gate(ctx):
            attempts.append(ctx.run_id)
            if len(attempts) == 1:
                gate.pass_through()
                raise RuntimeError("boom")

        self.steps.behaviors["a"] = fail_first_attempt_at_gate
        self.save()

        async def scenario():
            first = self.coordinator.submit(PLAN_ID, DAEMON_CLAIM)
            await gate.reached()
            await asyncio.to_thread(self.request_rerun)
            newer = self.coordinator.submit(PLAN_ID, DAEMON_CLAIM)
            gate.release()
            return await first, await newer

        first, newer = run_bounded(scenario())

        self.assertEqual(first.status, ExecutionStatus.UNFINISHED)
        self.assertTrue(newer.succeeded)
        self.assertEqual(newer.report.run_token, 1)
        self.assert_recorded(newer, run_token=1)


class TestCancellation(ExecutionTestCase):
    """Cancellation stops new steps; it never abandons a worker or a result."""

    def test_cancelled_caller_keeps_the_plan_excluded_until_the_worker_ends(self):
        # An interrupted attempt is not a finished request under either policy;
        # under continue_independent it must not pass for a drained failure.
        for policy in POLICIES:
            plan_id = f"{PLAN_ID}_{policy}"
            with self.subTest(policy=policy):
                gate = self.steps.block("a")
                self.save(chain_plan(policy, plan_id=plan_id))
                del self.steps.calls[:]

                async def scenario():
                    caller = asyncio.create_task(
                        self.coordinator.execute(plan_id, DAEMON_CLAIM)
                    )
                    await gate.reached()
                    caller.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await caller
                    # Step a is still running, so the plan must stay excluded.
                    self.assertTrue(self.coordinator.is_in_flight(plan_id))
                    duplicate = self.coordinator.submit(plan_id, DAEMON_CLAIM)
                    self.assertEqual(len(self.engine.contexts), 1)
                    self.assertTrue(self.engine.contexts[0].cancellation_requested)
                    gate.release()
                    return await duplicate

                duplicate = run_bounded(scenario())

                self.assertEqual(duplicate.status, ExecutionStatus.CANCELLED)
                self.assertEqual(self.steps.calls, ["a"])  # b never began
                context = self.engine.contexts.pop()
                self.assertEqual(
                    context.report.termination_reason, TerminationReason.CANCELLED
                )
                self.assertEqual(context.report.unreached_step_ids, ["b"])
                self.assertFalse(self.coordinator.is_in_flight(plan_id))
                self.assert_unconsumed(plan_id)
                self.assertTrue(is_plan_eligible(self.stored(plan_id)))
        self.assertEqual(self.store.calls["finalize"], 0)

    def test_shutdown_cancelling_the_coordination_waits_for_the_worker(self):
        # asyncio.run cancels every task still pending when its main coroutine
        # returns - the coordination task included.
        gate = self.steps.block("a")
        self.save()
        submitted = []

        def release_once_cancellation_is_requested():
            if gate.entered.wait(WAIT):
                self.engine.contexts[0].cancel_event.wait(WAIT)
            gate.release()

        releaser = threading.Thread(target=release_once_cancellation_is_requested)
        releaser.start()

        async def main():
            submitted.append(self.coordinator.submit(PLAN_ID, DAEMON_CLAIM))
            await gate.reached()

        run_bounded(main())
        releaser.join(WAIT)

        result = submitted[0].result()
        self.assertEqual(result.status, ExecutionStatus.CANCELLED)
        self.assertTrue(self.engine.contexts[0].cancellation_requested)
        self.assertEqual(self.steps.calls, ["a"])
        self.assertEqual(self.steps.running, 0)
        self.assertFalse(self.coordinator.is_in_flight(PLAN_ID))
        self.assert_unconsumed()

    def test_cancellation_before_the_attempt_starts_runs_nothing(self):
        self.save()
        paused = self.store.pause_read()

        async def scenario():
            first = self.coordinator.submit(PLAN_ID, DAEMON_CLAIM)
            await paused.reached()
            self.coordinator.request_cancellation(PLAN_ID)
            paused.release()
            return await first

        result = run_bounded(scenario())

        self.assertEqual(result.status, ExecutionStatus.CANCELLED)
        self.assertEqual(self.engine.contexts, [])
        self.assert_unconsumed()

    def test_drained_result_is_recorded_after_its_caller_is_cancelled(self):
        self.save()
        paused = self.store.pause_finalize()

        async def scenario():
            caller = asyncio.create_task(
                self.coordinator.execute(PLAN_ID, DAEMON_CLAIM)
            )
            await paused.reached()
            caller.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await caller
            duplicate = self.coordinator.submit(PLAN_ID, DAEMON_CLAIM)
            self.assertTrue(self.coordinator.is_in_flight(PLAN_ID))
            paused.release()
            return await duplicate

        run_bounded(scenario())

        self.assertEqual(len(self.engine.contexts), 1)
        record = self.stored()["last_finalized_execution"]
        self.assertEqual(record["execution_id"], self.engine.contexts[0].execution_id)
        self.assertEqual(self.stored()["executed_run_token"], 0)

    def test_drained_result_is_retained_after_its_caller_is_cancelled(self):
        self.save()
        paused = self.store.pause_finalize(RETRYABLE)
        self.store.fail_finalize(RETRYABLE, RETRYABLE)

        async def scenario():
            caller = asyncio.create_task(
                self.coordinator.execute(PLAN_ID, DAEMON_CLAIM)
            )
            await paused.reached()
            caller.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await caller
            duplicate = self.coordinator.submit(PLAN_ID, DAEMON_CLAIM)
            paused.release()
            return await duplicate

        run_bounded(scenario())

        self.assertEqual(len(self.engine.contexts), 1)
        self.assertEqual(self.store.calls["finalize"], 3)
        pending = self.coordinator.pending_finalization(PLAN_ID)
        self.assertEqual(pending.execution_id, self.engine.contexts[0].execution_id)
        self.assert_unconsumed()

    def test_shutdown_during_a_finalization_backoff_still_records_the_result(self):
        self.save()
        self.store.fail_finalize(RETRYABLE)
        backing_off = threading.Event()

        async def stalled(delay: float) -> None:
            self.delays.append(delay)
            backing_off.set()
            await asyncio.get_running_loop().create_future()  # never completes

        coordinator = PlanExecutionCoordinator(
            engine=self.engine, plan_store=self.store, sleep=stalled
        )
        submitted = []

        async def main():
            submitted.append(coordinator.submit(PLAN_ID, DAEMON_CLAIM))
            await asyncio.to_thread(backing_off.wait, WAIT)

        run_bounded(main())

        result = submitted[0].result()
        self.assertEqual(result.status, ExecutionStatus.FINALIZED)
        self.assertEqual(self.delays, [0.5])
        self.assertEqual(self.store.calls["finalize"], 2)
        self.assert_recorded(result)

    def test_step_raising_cancelled_error_neither_stalls_nor_holds_the_plan(self):
        # A step's own asyncio.CancelledError, say from asynchronous subwork it
        # ran and cancelled, ends that attempt as cancelled. It is not a
        # cancellation of the coordination: nothing else may stop because of
        # it, least of all the event loop every other plan runs on.
        self.steps.fail("a", asyncio.CancelledError("subwork was cancelled"))
        self.save()
        self.save(make_plan(spec("x"), plan_id="pln_other"))

        async def scenario():
            first = self.coordinator.submit(PLAN_ID, DAEMON_CLAIM)
            other = self.coordinator.submit("pln_other", DAEMON_CLAIM)
            return await first, await other

        first, other = run_bounded(scenario(), limit=WAIT)

        self.assertEqual(first.status, ExecutionStatus.CANCELLED)
        self.assertEqual(first.report.termination_reason, TerminationReason.CANCELLED)
        self.assertTrue(other.succeeded)
        context = next(c for c in self.engine.contexts if c.plan_id == PLAN_ID)
        self.assertFalse(context.cancellation_requested)
        self.assertFalse(self.coordinator.is_in_flight(PLAN_ID))
        self.assert_unconsumed()

    def test_coordination_cancelled_before_it_starts_releases_the_plan(self):
        # Anything that cancels the coordination task before its first step,
        # such as an explicit cancel right after submission, skips its body
        # and with it the cleanup that body would do.
        self.save()

        async def scenario():
            first = self.coordinator.submit(PLAN_ID, DAEMON_CLAIM)
            duplicate = self.coordinator.submit(PLAN_ID, DAEMON_CLAIM)
            coordination = next(
                task
                for task in asyncio.all_tasks()
                if task.get_name() == f"plan-execution:{PLAN_ID}"
            )
            coordination.cancel()
            await self.coordinator.drain()
            self.assertFalse(self.coordinator.is_in_flight(PLAN_ID))
            return await first, await duplicate

        first, duplicate = run_bounded(scenario(), limit=WAIT)

        self.assertEqual(first.status, ExecutionStatus.CANCELLED)
        self.assertEqual(duplicate.status, ExecutionStatus.CANCELLED)
        self.assertEqual(self.journal, [])
        self.assert_unconsumed()
        # The plan is free again: a later request runs normally.
        self.assertTrue(self.execute().succeeded)


class TestFinalizationRetries(ExecutionTestCase):
    """Bounded finalization: retry what can succeed, keep what cannot."""

    def test_lost_race_is_retried_after_the_first_backoff(self):
        self.save()
        self.store.conflict_finalize()

        result = self.execute()

        self.assertEqual(result.status, ExecutionStatus.FINALIZED)
        self.assertEqual(self.store.calls["finalize"], 2)
        self.assertEqual(self.delays, [0.5])
        self.assert_recorded(result)

    def test_transient_failures_are_retried_and_success_is_reported_once(self):
        self.save()
        self.store.fail_finalize(RETRYABLE, RETRYABLE)

        with self.assertLogs(COORDINATOR_LOGGER, level=logging.INFO) as logs:
            result = self.execute()

        self.assertEqual(result.status, ExecutionStatus.FINALIZED)
        self.assertEqual(self.store.calls["finalize"], 3)
        self.assertEqual(self.delays, [0.5, 1.0])
        self.assert_recorded(result)
        successes = [line for line in logs.output if "execution succeeded" in line]
        self.assertEqual(len(successes), 1)

    def test_write_that_landed_before_a_failure_is_recognized_on_retry(self):
        self.save()
        self.store.commit_then_fail_finalize(RETRYABLE)

        result = self.execute()

        self.assertEqual(result.status, ExecutionStatus.FINALIZED)
        self.assertEqual(
            result.finalization.status, FinalizationStatus.ALREADY_COMMITTED
        )
        self.assertEqual(self.store.calls["finalize"], 2)
        self.assert_recorded(result)

    def test_supersession_is_not_retried(self):
        def regenerate(ctx):
            self.backend.plans.save_plan(
                make_plan(spec("a", sample="regenerated")),
                REALM,
                dict(SCOPE),
                auto_run=True,
            )

        self.steps.behaviors["a"] = regenerate
        self.save()

        result = self.execute()

        self.assertEqual(result.status, ExecutionStatus.SUPERSEDED)
        self.assertEqual(
            result.finalization.reason, SupersessionReason.GENERATION_CHANGED
        )
        self.assertEqual(self.store.calls["finalize"], 1)
        self.assertEqual(self.delays, [])
        self.assertIsNone(self.coordinator.pending_finalization(PLAN_ID))
        # The regenerated plan is a new request, still waiting to run.
        self.assert_unconsumed()
        self.assertTrue(is_plan_eligible(self.stored()))

    def test_non_retryable_failure_is_not_retried(self):
        self.save()
        self.store.fail_finalize(PlanStoreError("permission denied"))

        result = self.execute()

        self.assertEqual(result.status, ExecutionStatus.FINALIZATION_PENDING)
        self.assertEqual(self.store.calls["finalize"], 1)
        self.assertEqual(self.delays, [])
        self.assertIsNotNone(self.coordinator.pending_finalization(PLAN_ID))

    def test_exhausted_result_is_retained_and_the_plan_never_reexecuted(self):
        self.save()
        self.store.fail_finalize(RETRYABLE, RETRYABLE, RETRYABLE)

        result = self.execute()

        self.assertEqual(result.status, ExecutionStatus.FINALIZATION_PENDING)
        self.assertEqual(self.store.calls["finalize"], 3)
        self.assertEqual(self.delays, [0.5, 1.0])
        pending = self.coordinator.pending_finalization(PLAN_ID)
        self.assertEqual(pending.execution_id, result.report.execution_id)
        self.assertFalse(self.coordinator.is_in_flight(PLAN_ID))
        # Still eligible in storage, yet a duplicate request neither runs the
        # plan nor starts another retry cycle.
        self.assertTrue(is_plan_eligible(self.stored()))

        duplicate = self.execute()

        self.assertEqual(duplicate.status, ExecutionStatus.FINALIZATION_PENDING)
        self.assertEqual(len(self.engine.contexts), 1)
        self.assertEqual(self.store.calls["finalize"], 3)
        self.assertEqual(self.delays, [0.5, 1.0])

    def test_pending_result_is_established_before_the_plan_is_released(self):
        self.save()
        self.store.fail_finalize(RETRYABLE, RETRYABLE)
        last_attempt = self.store.pause_finalize(RETRYABLE)

        async def scenario():
            first = self.coordinator.submit(PLAN_ID, DAEMON_CLAIM)
            await last_attempt.reached()
            duplicate = self.coordinator.submit(PLAN_ID, DAEMON_CLAIM)
            last_attempt.release()
            return await first, await duplicate

        first, duplicate = run_bounded(scenario())

        self.assertEqual(first.status, ExecutionStatus.FINALIZATION_PENDING)
        self.assertEqual(duplicate.status, ExecutionStatus.FINALIZATION_PENDING)
        self.assertEqual(len(self.engine.contexts), 1)
        self.assertEqual(self.store.calls["finalize"], 3)

    def test_other_plans_progress_while_one_retries(self):
        self.save()
        self.save(make_plan(spec("x"), plan_id="pln_other"))
        self.store.fail_finalize(RETRYABLE, RETRYABLE, RETRYABLE, plan_id=PLAN_ID)
        backing_off = threading.Event()

        async def scenario():
            resume = asyncio.Event()

            async def held_backoff(delay: float) -> None:
                self.delays.append(delay)
                backing_off.set()
                await resume.wait()

            coordinator = PlanExecutionCoordinator(
                engine=self.engine, plan_store=self.store, sleep=held_backoff
            )
            first = coordinator.submit(PLAN_ID, DAEMON_CLAIM)
            await asyncio.to_thread(backing_off.wait, WAIT)
            other = await coordinator.execute("pln_other", DAEMON_CLAIM)
            self.assertTrue(coordinator.is_in_flight(PLAN_ID))
            resume.set()
            return await first, other

        first, other = run_bounded(scenario())

        self.assertTrue(other.succeeded)
        self.assert_recorded(other)
        self.assertEqual(first.status, ExecutionStatus.FINALIZATION_PENDING)

    def test_newer_request_records_the_pending_result_before_running(self):
        self.save()
        self.store.fail_finalize(RETRYABLE, RETRYABLE, RETRYABLE)
        pending = self.execute()
        self.request_rerun()
        del self.journal[:]

        result = self.execute()

        self.assertEqual(result.status, ExecutionStatus.FINALIZED)
        self.assertEqual(result.report.run_token, 1)
        self.assertEqual(
            self.journal, ["read", "finalize:0", "read", "attempt:1", "finalize:1"]
        )
        old, new = self.store.finalizations
        self.assertEqual(old.status, FinalizationStatus.COMMITTED)
        self.assertIn(pending.report.execution_id, old.message)
        self.assertIsNone(self.coordinator.pending_finalization(PLAN_ID))
        self.assert_recorded(result, run_token=1)

    def test_newer_request_waits_while_the_pending_result_cannot_be_recorded(self):
        self.save()
        self.store.fail_finalize(*[RETRYABLE] * 9)
        pending = self.execute()
        self.request_rerun()

        result = self.execute()

        self.assertEqual(result.status, ExecutionStatus.FINALIZATION_PENDING)
        self.assertEqual(len(self.engine.contexts), 1)
        self.assertEqual(self.store.calls["finalize"], 6)
        self.assertEqual(
            self.coordinator.pending_finalization(PLAN_ID).execution_id,
            pending.report.execution_id,
        )
        self.assertEqual(self.stored()["run_token"], 1)
        self.assertEqual(self.stored()["executed_run_token"], -1)

        # Run token 1 has had its retry cycle; duplicates of it get none.
        for _ in range(3):
            duplicate = self.execute()
            self.assertEqual(duplicate.status, ExecutionStatus.FINALIZATION_PENDING)
        self.assertEqual(self.store.calls["finalize"], 6)
        self.assertEqual(len(self.engine.contexts), 1)

        # Only a newer request authorizes one more bounded cycle.
        self.request_rerun()
        self.assertEqual(self.execute().status, ExecutionStatus.FINALIZATION_PENDING)
        self.assertEqual(self.store.calls["finalize"], 9)
        self.execute()
        self.assertEqual(self.store.calls["finalize"], 9)
        self.assertEqual(len(self.engine.contexts), 1)
        self.assertEqual(
            self.coordinator.pending_finalization(PLAN_ID).execution_id,
            pending.report.execution_id,
        )

    def test_regenerated_plan_settles_the_pending_result_without_writing(self):
        self.save()
        self.store.fail_finalize(RETRYABLE, RETRYABLE, RETRYABLE)
        self.execute()
        regenerated = self.save(make_plan(spec("a", sample="regenerated")))
        del self.journal[:]

        result = self.execute()

        self.assertEqual(self.journal, ["read", "read", "attempt:0", "finalize:0"])
        self.assertEqual(result.status, ExecutionStatus.FINALIZED)
        self.assertEqual(result.report.plan_generation, regenerated["plan_generation"])
        self.assertIsNone(self.coordinator.pending_finalization(PLAN_ID))

    def test_pending_result_found_recorded_is_settled_without_writing(self):
        self.save()
        self.store.fail_finalize(RETRYABLE, RETRYABLE)
        self.store.commit_then_fail_finalize(RETRYABLE)
        pending = self.execute()
        self.assertEqual(pending.status, ExecutionStatus.FINALIZATION_PENDING)

        result = self.execute()

        # The last failed call had landed after all: nothing left to record,
        # and the served request is not run again.
        self.assertEqual(result.status, ExecutionStatus.NOT_ELIGIBLE)
        self.assertIsNone(self.coordinator.pending_finalization(PLAN_ID))
        self.assertEqual(self.store.calls["finalize"], 3)
        self.assertEqual(len(self.engine.contexts), 1)
        self.assert_recorded(pending)


class TestAttemptReporting(ExecutionTestCase):
    """Each coordinated attempt is identified by the engine and reported once."""

    def events_of_type(self, event_type: str) -> list[dict]:
        """Published events of one type."""
        return [e for e in self.emitter.events if e.get("type") == event_type]

    def test_execution_id_is_allocated_off_the_event_loop(self):
        threads: list[int] = []

        class RecordingThread:
            def recorded_execution_ids(self, realm: str, plan_id: str) -> list[str]:
                threads.append(threading.get_ident())
                return []

        self.engine.execution_ids = ExecutionIdAllocator(RecordingThread())
        self.save()

        async def scenario():
            loop_thread = threading.get_ident()
            return loop_thread, await self.coordinator.execute(PLAN_ID, DAEMON_CLAIM)

        loop_thread, result = run_bounded(scenario())

        self.assertTrue(result.succeeded)
        self.assertEqual(len(threads), 1)
        self.assertNotEqual(threads[0], loop_thread)

    def test_the_coordinator_has_no_allocator_of_its_own(self):
        with self.assertRaises(TypeError):
            PlanExecutionCoordinator(  # type: ignore[call-arg]
                engine=self.engine,
                plan_store=self.store,  # type: ignore[arg-type]
                execution_ids=ExecutionIdAllocator(),
            )

    def test_coordinated_and_direct_attempts_share_the_engines_allocator(self):
        # Replaced after the coordinator was built: it still follows the
        # engine. With the clock held still, only one shared allocator can
        # keep the two attempts apart and in order.
        self.engine.execution_ids = ExecutionIdAllocator(clock=Clock(T0))
        self.save()

        coordinated = self.execute()
        direct = self.engine.run(chain_plan())

        assert coordinated.report is not None
        self.assertIsNone(direct)
        (direct_start,) = self.events_of_type(ATTEMPT_STARTED_EVENT)[1:]
        self.assertEqual(execution_timestamp(coordinated.report.execution_id), T0)
        self.assertGreater(
            direct_start["execution_id"], coordinated.report.execution_id
        )

    def test_failed_allocation_runs_nothing_and_leaves_the_plan_eligible(self):
        class Unreadable:
            def recorded_execution_ids(self, realm: str, plan_id: str) -> list[str]:
                raise PermissionError("spool unreadable")

        self.engine.execution_ids = ExecutionIdAllocator(Unreadable())
        self.save()

        result = self.execute()

        self.assertEqual(result.status, ExecutionStatus.ADMISSION_FAILED)
        self.assertIn("PermissionError", result.message)
        self.assertEqual(self.engine.contexts, [])
        self.assertEqual(self.emitter.events, [])
        self.assert_unconsumed()
        self.assertFalse(self.coordinator.is_in_flight(PLAN_ID))

    def held_allocation(self) -> tuple[Gate, PlanExecutionCoordinator]:
        """The coordinator, with execution-ID allocation waiting at a gate."""
        gate = Gate()

        class Held:
            def recorded_execution_ids(self, realm: str, plan_id: str) -> list[str]:
                gate.pass_through()
                return []

        self.engine.execution_ids = ExecutionIdAllocator(Held())
        return gate, self.coordinator

    def test_cancellation_during_allocation_starts_no_attempt(self):
        gate, coordinator = self.held_allocation()
        self.save()

        async def scenario():
            first = coordinator.submit(PLAN_ID, DAEMON_CLAIM)
            await gate.reached()
            coordinator.request_cancellation(PLAN_ID)
            gate.release()
            return await first

        result = run_bounded(scenario())

        self.assertEqual(result.status, ExecutionStatus.CANCELLED)
        self.assertEqual(self.engine.contexts, [])
        self.assertEqual(self.emitter.events, [])
        self.assert_unconsumed()

    def test_shutdown_during_allocation_waits_for_it_and_starts_nothing(self):
        # asyncio.run cancels the coordination task while the allocation's
        # worker thread is still reading the plan's history.
        gate, coordinator = self.held_allocation()
        self.save()
        submitted = []

        def release_once_cancellation_is_requested():
            if gate.entered.wait(WAIT):
                coordinator._slots[PLAN_ID].cancel_event.wait(WAIT)
            gate.release()

        releaser = threading.Thread(target=release_once_cancellation_is_requested)
        releaser.start()

        async def main():
            submitted.append(coordinator.submit(PLAN_ID, DAEMON_CLAIM))
            await gate.reached()

        run_bounded(main())
        releaser.join(WAIT)

        self.assertEqual(submitted[0].result().status, ExecutionStatus.CANCELLED)
        self.assertEqual(self.engine.contexts, [])
        self.assertEqual(self.emitter.events, [])
        self.assertFalse(coordinator.is_in_flight(PLAN_ID))
        self.assert_unconsumed()

    def test_each_attempt_publishes_exactly_one_report(self):
        self.save()

        result = self.execute()

        self.assertEqual(result.status, ExecutionStatus.FINALIZED)
        assert result.report is not None
        (started,) = self.events_of_type(ATTEMPT_STARTED_EVENT)
        (published,) = self.events_of_type(ATTEMPT_REPORT_EVENT)
        self.assertEqual(published["report"], result.report.to_dict())
        self.assertEqual(
            {key: started[key] for key in ("plan_generation", "run_token")},
            {
                "plan_generation": self.stored()["plan_generation"],
                "run_token": 0,
            },
        )

    def test_duplicate_request_leaves_no_trace_in_reporting(self):
        with TemporaryDirectory() as temp_dir:
            spool = Path(temp_dir) / "spool"
            engine = CapturingEngine(
                work_root=Path(temp_dir) / "work",
                emitter=FileSpoolEmitter(spool),
                journal=self.journal,
            )
            coordinator = PlanExecutionCoordinator(
                engine=engine,
                plan_store=self.store,  # type: ignore[arg-type]
                sleep=self.record_delay,
            )
            gate = Gate()

            def fail_at_gate(ctx):
                gate.pass_through()
                raise RuntimeError("boom")

            self.steps.behaviors["a"] = fail_at_gate
            self.save()

            async def scenario():
                first = coordinator.submit(PLAN_ID, DAEMON_CLAIM)
                await gate.reached()
                duplicate = coordinator.submit(PLAN_ID, DAEMON_CLAIM)
                gate.release()
                return await first, await duplicate

            first, duplicate = run_bounded(scenario())

            self.assertEqual(duplicate.status, ExecutionStatus.DUPLICATE)
            assert first.report is not None
            plan_dir = spool / REALM / PLAN_ID
            self.assertEqual(len(list(plan_dir.glob("*.json"))), 2)
            snapshot = build_plan_snapshot(plan_dir, REALM, PLAN_ID)
            self.assertEqual(
                snapshot["attempt"]["execution_id"], first.report.execution_id
            )
            self.assertEqual(snapshot["attempt"]["termination_reason"], "failed_fast")
            self.assertEqual(snapshot["steps"]["a"]["state"], "step.failed")


class _ReturnsNothingEngine:
    """Runs the real attempt, then returns None instead of its report."""

    def __init__(self, engine):
        self.engine = engine
        self.execution_ids = engine.execution_ids

    def _run_attempt(self, plan: Plan, *, context: AttemptContext) -> None:
        self.engine._run_attempt(plan, context=context)


class _RefusingEngine:
    """Raises before recording anything, leaving the report open."""

    execution_ids = ExecutionIdAllocator()

    def _run_attempt(self, plan: Plan, *, context: AttemptContext) -> AttemptReport:
        raise OrchestrationError("attempt context refused")


class TestEngineContract(ExecutionTestCase):
    """An attempt the engine did not report properly is never recorded."""

    def test_attempt_not_returned_as_its_report_is_not_recorded(self):
        # Even under fail_fast, _run_attempt returns the report; a None here is
        # the public run() convention leaking into the operational path.
        for engine in (_ReturnsNothingEngine(self.engine), _RefusingEngine()):
            with self.subTest(engine=type(engine).__name__):
                self.save()
                coordinator = PlanExecutionCoordinator(
                    engine=engine, plan_store=self.store, sleep=self.record_delay
                )

                result = run_bounded(coordinator.execute(PLAN_ID, DAEMON_CLAIM))

                self.assertEqual(result.status, ExecutionStatus.UNFINISHED)
                self.assert_unconsumed()
        self.assertEqual(self.store.calls["finalize"], 0)

    def test_invalid_configuration_is_rejected(self):
        with self.assertRaises(ValueError):
            PlanExecutionCoordinator(
                engine=self.engine, plan_store=self.store, finalization_attempts=0
            )
        with self.assertRaises(ValueError):
            PlanExecutionCoordinator(
                engine=self.engine,
                plan_store=self.store,
                finalization_attempts=3,
                finalization_backoff=(0.5,),
            )


if __name__ == "__main__":
    unittest.main()
