"""Tests for the engine's dependency scheduler and failure policies.

These drive real ``@step`` functions through ``Engine.run`` and
``Engine._run_attempt``. Each test states a guarantee from the scheduling and
result contract and would fail if that guarantee were removed:

- dependencies order and gate execution, and a blocked step is never invoked;
- ``fail_fast`` keeps its exact external contract, while its report still
  distinguishes work never reached from work that was blocked;
- ``continue_independent`` contains an ordinary step failure to its dependents
  and drains everything else;
- cancellation, preflight rejection and infrastructure failure each end the
  attempt distinctly, never as a drained failure;
- every ending leaves one closed, published report, and attempts sharing an
  Engine never see each other's state.

Cross-thread tests coordinate with ``threading.Event`` handshakes, never sleeps;
``WAIT`` is only an upper bound that keeps a broken test from hanging.
"""

import json
import os
import threading
import unittest
from collections.abc import Callable
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from yggdrasil.core.engine import ATTEMPT_REPORT_EVENT, Engine
from yggdrasil.flow.attempt import AttemptContext
from yggdrasil.flow.errors import (
    AttemptCancelledError,
    EventPublicationError,
    OrchestrationError,
    PermanentStepError,
    PreflightValidationError,
    StepError,
    TransientStepError,
)
from yggdrasil.flow.events.emitter import EventEmitter, FileSpoolEmitter
from yggdrasil.flow.model import (
    CONTINUE_INDEPENDENT_POLICY,
    FAIL_FAST_POLICY,
    Plan,
    StepResult,
    StepSpec,
)
from yggdrasil.flow.outcomes import (
    AttemptReport,
    ExecutionOutcome,
    StepOutcome,
    TerminationReason,
)
from yggdrasil.flow.step import StepContext, step

CONTINUE = CONTINUE_INDEPENDENT_POLICY
FAIL_FAST = FAIL_FAST_POLICY
POLICIES = (FAIL_FAST, CONTINUE)

# Every step resolves to the same scripted callable; the reference is nominal.
STEP_REF = "tests.scheduling:scripted"

# Upper bound for cross-thread handshakes. Never slept on.
WAIT = 5.0


class RecordingEmitter(EventEmitter):
    """Emitter that records every event and can fail on chosen event types.

    A failing emit is still recorded, so "never attempted" and "attempted but
    failed" can be told apart.
    """

    def __init__(self) -> None:
        self.events: list[dict] = []
        self.fail_on: set[str] = set()
        # What a failing emit raises: an ordinary OSError by default, or an
        # OrchestrationError, as an emitter built on Yggdrasil storage might.
        self.error_type: type[Exception] = OSError
        self._lock = threading.Lock()

    def emit(self, event: dict) -> None:
        with self._lock:
            self.events.append(event)
        if event.get("type") in self.fail_on:
            raise self.error_type(f"event spool unavailable for {event.get('type')}")

    def types(self) -> list[str]:
        """Event types recorded so far, in emission order."""
        return [str(e.get("type", "")) for e in self.events]

    def of_type(self, type_: str) -> list[dict]:
        """Recorded events of one type."""
        return [e for e in self.events if e.get("type") == type_]

    def reports(self) -> list[dict]:
        """Payloads of every attempt report published so far."""
        return [e["report"] for e in self.of_type(ATTEMPT_REPORT_EVENT)]


class ScriptedSteps:
    """One @step callable whose behavior is scripted per step ID.

    Tests state only what differs between steps and read back the order in
    which step bodies actually ran.
    """

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.behaviors: dict[str, Callable[[StepContext], None]] = {}
        calls, behaviors = self.calls, self.behaviors

        @step
        def scripted(ctx: StepContext, **kwargs) -> StepResult:
            calls.append(ctx.step_id)
            behavior = behaviors.get(ctx.step_id)
            if behavior is not None:
                behavior(ctx)
            return StepResult()

        self.fn = scripted

    def fail(self, step_id: str, exc: BaseException) -> None:
        """Make a step raise ``exc`` when invoked."""

        def raise_(ctx: StepContext) -> None:
            raise exc

        self.behaviors[step_id] = raise_


def spec(step_id: str, *deps: str, **params) -> StepSpec:
    """A scripted step with the given dependencies and params."""
    return StepSpec(
        step_id=step_id,
        name=step_id,
        fn_ref=STEP_REF,
        params=dict(params),
        deps=list(deps),
    )


def exception_chain(exc: BaseException) -> list[BaseException]:
    """Every exception reachable from exc through __cause__ and __context__."""
    seen: list[BaseException] = []
    pending = [exc]
    while pending:
        current = pending.pop()
        if any(current is s for s in seen):
            continue
        seen.append(current)
        pending.extend(
            e for e in (current.__cause__, current.__context__) if e is not None
        )
    return seen


class SchedulingTestCase(unittest.TestCase):
    """Shared fixture: an engine on a temp work root with scripted steps."""

    def setUp(self):
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.work_root = Path(temp_dir.name)
        self.emitter = RecordingEmitter()
        self.engine = Engine(work_root=self.work_root, emitter=self.emitter)
        self.steps = ScriptedSteps()
        resolver = patch(
            "yggdrasil.core.engine.resolve_callable", return_value=self.steps.fn
        )
        resolver.start()
        self.addCleanup(resolver.stop)

    def plan(
        self, *specs: StepSpec, policy: str = FAIL_FAST, plan_id: str = "sched_plan"
    ) -> Plan:
        return Plan(
            plan_id=plan_id,
            realm="test",
            scope={"kind": "project", "id": "P1"},
            steps=list(specs),
            failure_policy=policy,
        )

    def run_attempt(
        self, plan: Plan, engine: Engine | None = None
    ) -> tuple[AttemptContext, BaseException | None]:
        """Run one attempt through _run_attempt, capturing how it ended."""
        context = AttemptContext.for_plan(plan, execution_id=f"exec_{plan.plan_id}")
        try:
            (engine or self.engine)._run_attempt(plan, context=context)
        except BaseException as exc:  # KeyboardInterrupt is exercised on purpose
            return context, exc
        return context, None

    def reset_observations(self) -> None:
        """Forget calls and events from earlier attempts in the same test."""
        self.steps.calls.clear()
        self.emitter.events.clear()

    def assert_published_once(self, report: AttemptReport) -> dict:
        """Assert exactly one attempt report was published, matching ``report``."""
        published = self.emitter.reports()
        self.assertEqual(len(published), 1, self.emitter.types())
        self.assertEqual(published[0], report.to_dict())
        return published[0]


class TestDependencyOrderIsEnforced(SchedulingTestCase):
    """Dependencies order and gate execution, not merely pass validation."""

    SHAPES: dict[str, tuple[list[StepSpec], list[str]]] = {
        "chain": (
            [spec("a"), spec("b", "a"), spec("c", "b")],
            ["a", "b", "c"],
        ),
        "chain listed in reverse": (
            [spec("c", "b"), spec("b", "a"), spec("a")],
            ["a", "b", "c"],
        ),
        "fan-out": (
            [spec("root"), spec("x", "root"), spec("y", "root")],
            ["root", "x", "y"],
        ),
        "diamond": (
            [
                spec("root"),
                spec("left", "root"),
                spec("right", "root"),
                spec("join", "left", "right"),
            ],
            ["root", "left", "right", "join"],
        ),
        "independent roots": ([spec("b"), spec("a"), spec("c")], ["b", "a", "c"]),
        "newly runnable step overtakes a later root": (
            [spec("late", "a"), spec("a"), spec("b")],
            ["a", "late", "b"],
        ),
        "empty plan": ([], []),
    }

    def test_forward_reference_runs_the_producer_before_its_consumer(self):
        """List order would run the consumer first and fail on a missing handoff."""
        handoff = self.work_root / "handoff.txt"

        def produce(ctx: StepContext) -> None:
            handoff.write_text("demultiplexed", encoding="utf-8")

        def consume(ctx: StepContext) -> None:
            if handoff.read_text(encoding="utf-8") != "demultiplexed":
                raise PermanentStepError("consumer saw the wrong handoff")

        self.steps.behaviors.update(producer=produce, consumer=consume)

        result = self.engine.run(
            self.plan(spec("consumer", "producer"), spec("producer"))
        )

        self.assertIsNone(result)
        self.assertEqual(self.steps.calls, ["producer", "consumer"])

    def test_every_step_of_a_reversed_chain_is_promoted_and_run(self):
        self.engine.run(
            self.plan(spec("d", "c"), spec("c", "b"), spec("b", "a"), spec("a"))
        )

        self.assertEqual(self.steps.calls, ["a", "b", "c", "d"])

    def test_graph_shapes_under_both_policies_run_each_step_once_in_order(self):
        for name, (steps, expected) in self.SHAPES.items():
            for policy in POLICIES:
                with self.subTest(shape=name, policy=policy):
                    self.reset_observations()
                    plan_id = f"{name.replace(' ', '_')}_{policy}"

                    result = self.engine.run(
                        self.plan(*steps, policy=policy, plan_id=plan_id)
                    )

                    self.assertEqual(self.steps.calls, expected)
                    [report] = self.emitter.reports()
                    self.assertEqual(report["termination_reason"], "completed")
                    self.assertEqual(report["outcome"], "succeeded")
                    self.assertEqual(
                        report["step_outcomes"],
                        {step_id: "succeeded" for step_id in expected},
                    )
                    if policy == FAIL_FAST:
                        self.assertIsNone(result)
                    else:
                        self.assertIsInstance(result, AttemptReport)
                        self.assertEqual(result.to_dict(), report)

    def test_all_reused_plan_drains_successfully_without_running_steps(self):
        steps = [spec("root"), spec("left", "root"), spec("join", "left", "root")]
        for policy in POLICIES:
            with self.subTest(policy=policy):
                plan = self.plan(*steps, policy=policy, plan_id=f"reuse_{policy}")
                self.engine.run(plan)
                self.reset_observations()

                context, exc = self.run_attempt(plan)

                self.assertIsNone(exc)
                self.assertEqual(self.steps.calls, [])
                report = context.report
                self.assertEqual(
                    report.step_outcomes,
                    {sid: StepOutcome.REUSED for sid in ("root", "left", "join")},
                )
                self.assertIs(report.outcome, ExecutionOutcome.SUCCEEDED)
                self.assertEqual(
                    [e["step_id"] for e in self.emitter.of_type("step.skipped")],
                    ["root", "left", "join"],
                )
                self.assert_published_once(report)

    def test_old_success_marker_on_a_blocked_descendant_is_neither_reused_nor_run(
        self,
    ):
        """Readiness is established before any cache marker is read."""
        self.engine.run(
            self.plan(spec("root", version=1), spec("child", "root"), policy=CONTINUE)
        )
        marker = self.work_root / "sched_plan" / "child" / "success.fingerprint"
        self.assertTrue(marker.exists())
        self.reset_observations()
        # Changing root's params makes it re-execute; this time it fails. The
        # child's params are unchanged, so its old marker would still match.
        self.steps.fail("root", PermanentStepError("root broke"))

        report = self.engine.run(
            self.plan(spec("root", version=2), spec("child", "root"), policy=CONTINUE)
        )

        self.assertEqual(self.steps.calls, ["root"])
        self.assertIs(report.step_outcomes["child"], StepOutcome.BLOCKED)
        self.assertNotIn("step.skipped", self.emitter.types())


class TestFailFastCompatibility(SchedulingTestCase):
    """fail_fast keeps its external contract and reports truthfully inside."""

    def test_success_returns_none(self):
        self.assertIsNone(self.engine.run(self.plan(spec("a"), spec("b", "a"))))

    def test_unexpected_failure_propagates_unchanged_and_stops_the_run(self):
        original = RuntimeError("lane 2 exploded")
        self.steps.fail("a", original)

        with self.assertRaises(RuntimeError) as cm:
            self.engine.run(self.plan(spec("a"), spec("b")))

        self.assertIs(cm.exception, original)
        self.assertEqual(self.steps.calls, ["a"])

    def test_transient_failure_still_converts_with_events_in_the_same_order(self):
        self.steps.fail("a", TransientStepError("cluster busy"))

        with self.assertRaises(PermanentStepError) as cm:
            self.engine.run(self.plan(spec("a"), spec("b")))

        self.assertIn("Retry not implemented for transient failure", str(cm.exception))
        self.assertIsInstance(cm.exception.__cause__, TransientStepError)
        self.assertEqual(
            self.emitter.types(),
            [
                "step.started",
                "step.failed",
                "step.retry_unimplemented",
                ATTEMPT_REPORT_EVENT,
            ],
        )
        self.assertEqual(self.steps.calls, ["a"])

    def test_work_never_reached_is_unreached_not_blocked(self):
        """Neither a real dependent nor an unrelated step is relabelled blocked."""
        self.steps.fail("a", RuntimeError("boom"))

        context, exc = self.run_attempt(
            self.plan(spec("a"), spec("dependent", "a"), spec("unrelated"))
        )

        self.assertIsInstance(exc, RuntimeError)
        report = context.report
        self.assertEqual(report.step_outcomes, {"a": StepOutcome.FAILED})
        self.assertEqual(report.unreached_step_ids, ["dependent", "unrelated"])
        self.assertEqual(report.direct_blockers, {})
        self.assertIs(report.termination_reason, TerminationReason.FAILED_FAST)
        self.assertIs(report.outcome, ExecutionOutcome.FAILED)
        self.assertIsNone(report.diagnostic)
        self.assertEqual(report.failures["a"].error_type, "RuntimeError")
        self.assert_published_once(report)

    def test_run_attempt_reports_a_successful_fail_fast_plan_in_full(self):
        context, exc = self.run_attempt(self.plan(spec("b", "a"), spec("a")))

        self.assertIsNone(exc)
        report = context.report
        self.assertIs(report.termination_reason, TerminationReason.COMPLETED)
        self.assertTrue(report.is_drained)
        self.assertEqual(
            report.step_outcomes,
            {"a": StepOutcome.SUCCEEDED, "b": StepOutcome.SUCCEEDED},
        )
        self.assertIsNotNone(report.ended_at)
        self.assert_published_once(report)


class TestContinueIndependent(SchedulingTestCase):
    """An ordinary step failure is contained to the steps that depend on it."""

    def test_branch_failure_blocks_its_lane_while_another_lane_completes(self):
        self.steps.fail("demux_2", PermanentStepError("lane 2 demux failed"))
        plan = self.plan(
            spec("metadata"),
            spec("demux_2", "metadata"),
            spec("collect_2", "demux_2"),
            spec("upload_2", "collect_2"),
            spec("demux_3", "metadata"),
            spec("collect_3", "demux_3"),
            spec("upload_3", "collect_3"),
            policy=CONTINUE,
        )

        report = self.engine.run(plan)

        self.assertEqual(
            self.steps.calls,
            ["metadata", "demux_2", "demux_3", "collect_3", "upload_3"],
        )
        self.assertIs(report.termination_reason, TerminationReason.COMPLETED)
        self.assertTrue(report.is_drained)
        self.assertIs(report.outcome, ExecutionOutcome.FAILED)
        self.assertEqual(
            {sid: o.value for sid, o in report.step_outcomes.items()},
            {
                "metadata": "succeeded",
                "demux_2": "failed",
                "collect_2": "blocked",
                "upload_2": "blocked",
                "demux_3": "succeeded",
                "collect_3": "succeeded",
                "upload_3": "succeeded",
            },
        )
        self.assertEqual(
            report.direct_blockers,
            {"collect_2": ["demux_2"], "upload_2": ["collect_2"]},
        )
        self.assertEqual(
            report.failed_ancestors,
            {"collect_2": ["demux_2"], "upload_2": ["demux_2"]},
        )
        self.assert_published_once(report)

    def test_shared_prerequisite_failure_blocks_all_dependents_but_not_other_roots(
        self,
    ):
        self.steps.fail("metadata", PermanentStepError("LIMS unavailable"))

        report = self.engine.run(
            self.plan(
                spec("metadata"),
                spec("lane_1", "metadata"),
                spec("lane_2", "metadata"),
                spec("unrelated"),
                policy=CONTINUE,
            )
        )

        self.assertEqual(self.steps.calls, ["metadata", "unrelated"])
        self.assertIs(report.step_outcomes["lane_1"], StepOutcome.BLOCKED)
        self.assertIs(report.step_outcomes["lane_2"], StepOutcome.BLOCKED)
        self.assertIs(report.step_outcomes["unrelated"], StepOutcome.SUCCEEDED)

    def test_join_with_one_failed_prerequisite_is_blocked(self):
        self.steps.fail("bad", PermanentStepError("boom"))

        report = self.engine.run(
            self.plan(
                spec("ok"), spec("bad"), spec("join", "ok", "bad"), policy=CONTINUE
            )
        )

        self.assertEqual(self.steps.calls, ["ok", "bad"])
        self.assertIs(report.step_outcomes["ok"], StepOutcome.SUCCEEDED)
        self.assertIs(report.step_outcomes["join"], StepOutcome.BLOCKED)
        self.assertEqual(report.direct_blockers["join"], ["bad"])

    def test_independent_failures_converging_on_a_join_are_all_reported(self):
        """The second failure happens after the join was already blocked."""
        self.steps.fail("x", PermanentStepError("x broke"))
        self.steps.fail("y", PermanentStepError("y broke"))

        report = self.engine.run(
            self.plan(
                spec("x"),
                spec("join", "x", "y"),
                spec("after", "join"),
                spec("y"),
                policy=CONTINUE,
            )
        )

        self.assertEqual(self.steps.calls, ["x", "y"])
        self.assertEqual(report.direct_blockers["join"], ["x", "y"])
        self.assertEqual(report.failed_ancestors["join"], ["x", "y"])
        self.assertEqual(report.direct_blockers["after"], ["join"])
        self.assertEqual(report.failed_ancestors["after"], ["x", "y"])

    def test_ordinary_failures_keep_their_diagnostics_and_healthy_work_continues(
        self,
    ):
        self.steps.fail(
            "perm",
            PermanentStepError(
                "bad sample sheet", code="E_SHEET", advice="fix the sheet"
            ),
        )
        self.steps.fail("trans", TransientStepError("cluster busy", code="E_BUSY"))
        self.steps.fail("unexp", KeyError("lane_7"))

        report = self.engine.run(
            self.plan(
                spec("perm"),
                spec("trans"),
                spec("unexp"),
                spec("healthy"),
                policy=CONTINUE,
            )
        )

        self.assertEqual(self.steps.calls, ["perm", "trans", "unexp", "healthy"])
        self.assertIs(report.step_outcomes["healthy"], StepOutcome.SUCCEEDED)
        failures = {sid: f.to_dict() for sid, f in report.failures.items()}
        self.assertEqual(
            failures["perm"],
            {
                "step_id": "perm",
                "error": "bad sample sheet",
                "kind": "permanent",
                "code": "E_SHEET",
                "advice": "fix the sheet",
                "error_type": "PermanentStepError",
            },
        )
        self.assertEqual(failures["trans"]["kind"], "transient")
        self.assertEqual(failures["trans"]["code"], "E_BUSY")
        self.assertEqual(failures["trans"]["error_type"], "TransientStepError")
        self.assertEqual(failures["unexp"]["kind"], "permanent")
        self.assertEqual(failures["unexp"]["error_type"], "KeyError")
        self.assertEqual(failures["unexp"]["error"], "'lane_7'")
        # One terminal failure event per failing step, from the wrapper only.
        self.assertEqual(
            [e["step_id"] for e in self.emitter.of_type("step.failed")],
            ["perm", "trans", "unexp"],
        )
        # Retry is still unimplemented under either policy, and still says so.
        self.assertEqual(
            [e["step_id"] for e in self.emitter.of_type("step.retry_unimplemented")],
            ["trans"],
        )
        self.assertIs(report.outcome, ExecutionOutcome.FAILED)

    def test_run_returns_the_same_report_it_published(self):
        self.steps.fail("a", PermanentStepError("boom"))

        report = self.engine.run(self.plan(spec("a"), spec("b", "a"), policy=CONTINUE))

        self.assertIsInstance(report, AttemptReport)
        self.assert_published_once(report)

    def test_step_raising_attempt_cancelled_error_is_not_a_cancellation(self):
        """AttemptCancelledError is engine-internal, not an author-facing API."""
        self.steps.fail("a", AttemptCancelledError("a step pretending to cancel"))

        report = self.engine.run(self.plan(spec("a"), spec("b"), policy=CONTINUE))

        self.assertEqual(self.steps.calls, ["a", "b"])
        self.assertIs(report.termination_reason, TerminationReason.COMPLETED)
        self.assertIs(report.step_outcomes["a"], StepOutcome.FAILED)
        self.assertEqual(report.failures["a"].error_type, "AttemptCancelledError")
        self.assertIs(report.step_outcomes["b"], StepOutcome.SUCCEEDED)

    def test_continuation_result_is_never_none_even_when_everything_succeeds(self):
        report = self.engine.run(self.plan(spec("a"), policy=CONTINUE))

        self.assertIsInstance(report, AttemptReport)
        self.assertIs(report.outcome, ExecutionOutcome.SUCCEEDED)

    def test_empty_plan_under_both_policies(self):
        for policy in POLICIES:
            with self.subTest(policy=policy):
                self.reset_observations()

                result = self.engine.run(
                    self.plan(policy=policy, plan_id=f"empty_{policy}")
                )

                [published] = self.emitter.reports()
                self.assertEqual(published["termination_reason"], "completed")
                self.assertEqual(published["outcome"], "succeeded")
                if policy == FAIL_FAST:
                    self.assertIsNone(result)
                else:
                    self.assertEqual(result.to_dict(), published)


class TestAttemptEndings(SchedulingTestCase):
    """Each way an attempt can end is recorded as itself."""

    def test_preflight_rejection_is_reported_under_both_policies(self):
        for policy in POLICIES:
            with self.subTest(policy=policy):
                self.reset_observations()
                plan = self.plan(
                    spec("a"), spec("a"), policy=policy, plan_id=f"dup_{policy}"
                )

                context, exc = self.run_attempt(plan)

                self.assertIsInstance(exc, PreflightValidationError)
                report = context.report
                self.assertIs(
                    report.termination_reason, TerminationReason.PREFLIGHT_REJECTED
                )
                self.assertEqual(report.step_outcomes, {})
                self.assertEqual(
                    report.diagnostic.error_type, "PreflightValidationError"
                )
                self.assertIn("Duplicate step_id", report.diagnostic.message)
                self.assertEqual(self.steps.calls, [])
                self.assertFalse((self.work_root / plan.plan_id).exists())
                self.assert_published_once(report)

    def test_import_failure_during_preflight_is_not_a_rejection(self):
        with patch(
            "yggdrasil.core.engine.resolve_callable",
            side_effect=RuntimeError("database unavailable at import"),
        ):
            context, exc = self.run_attempt(self.plan(spec("a"), policy=CONTINUE))

        self.assertIsInstance(exc, OrchestrationError)
        self.assertIs(
            context.report.termination_reason, TerminationReason.ORCHESTRATION_ERROR
        )
        self.assert_published_once(context.report)

    def test_scheduler_invariant_failure_aborts_instead_of_completing(self):
        """A graph that stalls must not pass for a drained attempt."""
        plan = self.plan(spec("a", "b"), spec("b", "a"), spec("c"), policy=CONTINUE)

        # Let a cycle past preflight, as a scheduler-facing defect would.
        with patch.object(self.engine, "_topo_validate"):
            context, exc = self.run_attempt(plan)

        self.assertIsInstance(exc, OrchestrationError)
        self.assertIn("no possible progress", str(exc))
        report = context.report
        self.assertIs(report.termination_reason, TerminationReason.ORCHESTRATION_ERROR)
        self.assertFalse(report.is_drained)
        self.assertEqual(report.step_outcomes, {"c": StepOutcome.SUCCEEDED})
        self.assertEqual(report.unreached_step_ids, ["a", "b"])
        self.assert_published_once(report)

    def test_context_opened_for_another_plan_is_refused_untouched(self):
        context = AttemptContext.for_plan(
            self.plan(spec("a"), plan_id="plan_a"), execution_id="exec_a"
        )

        with self.assertRaises(OrchestrationError) as cm:
            self.engine._run_attempt(
                self.plan(spec("a"), plan_id="plan_b"), context=context
            )

        self.assertIn("opened for plan 'plan_a'", str(cm.exception))
        self.assertFalse(context.report.is_finished)
        self.assertEqual(self.emitter.events, [])
        self.assertEqual(self.steps.calls, [])

    def test_context_already_used_by_an_earlier_attempt_is_refused(self):
        plan = self.plan(spec("a"))
        context, _ = self.run_attempt(plan)
        finished = context.report.to_dict()
        self.reset_observations()

        with self.assertRaises(OrchestrationError) as cm:
            self.engine._run_attempt(plan, context=context)

        self.assertIn("already used", str(cm.exception))
        self.assertEqual(context.report.to_dict(), finished)
        self.assertEqual(self.emitter.events, [])


class TestCancellation(SchedulingTestCase):
    """Cancellation stops new work between steps and is never a drained failure."""

    def test_cancellation_preserves_failed_blocked_and_completed_work(self):
        plan = self.plan(
            spec("lane_2"),
            spec("lane_2_upload", "lane_2"),
            spec("lane_3"),
            spec("lane_4"),
            policy=CONTINUE,
        )
        context = AttemptContext.for_plan(plan, execution_id="exec_cancel")
        self.steps.fail("lane_2", PermanentStepError("lane 2 failed"))
        # lane_3 is independent work that completes, then cancellation arrives.
        self.steps.behaviors["lane_3"] = lambda ctx: context.request_cancellation()

        with self.assertRaises(AttemptCancelledError) as cm:
            self.engine._run_attempt(plan, context=context)

        self.assertNotIsInstance(cm.exception, StepError)
        self.assertNotIsInstance(cm.exception, OrchestrationError)
        self.assertEqual(self.steps.calls, ["lane_2", "lane_3"])
        report = context.report
        self.assertEqual(
            report.step_outcomes,
            {
                "lane_2": StepOutcome.FAILED,
                "lane_2_upload": StepOutcome.BLOCKED,
                "lane_3": StepOutcome.SUCCEEDED,
            },
        )
        self.assertEqual(report.failed_ancestors["lane_2_upload"], ["lane_2"])
        self.assertEqual(report.unreached_step_ids, ["lane_4"])
        # Distinct from a drained failure, which is what would retire a request:
        # the report is closed, but the attempt did not drain.
        self.assertIs(report.termination_reason, TerminationReason.CANCELLED)
        self.assertTrue(report.is_finished)
        self.assertFalse(report.is_drained)
        self.assertIsNone(report.publication_failure)
        published = self.assert_published_once(report)
        self.assertEqual(published["termination_reason"], "cancelled")
        self.assertEqual(published["unreached_step_ids"], ["lane_4"])

    def test_cancellation_from_another_thread_waits_for_the_running_step(self):
        entered, release = threading.Event(), threading.Event()

        def hold(ctx: StepContext) -> None:
            entered.set()
            if not release.wait(WAIT):
                raise AssertionError("the test never released the running step")

        self.steps.behaviors["slow"] = hold
        plan = self.plan(spec("slow"), spec("next", "slow"))
        context = AttemptContext.for_plan(plan, execution_id="exec_threaded")
        ended: dict[str, BaseException] = {}

        def worker() -> None:
            try:
                self.engine._run_attempt(plan, context=context)
            except BaseException as exc:
                ended["exc"] = exc

        thread = threading.Thread(target=worker)
        thread.start()
        self.assertTrue(entered.wait(WAIT))

        context.request_cancellation()
        # The running step is not interrupted: the attempt is still in flight.
        self.assertTrue(thread.is_alive())
        self.assertFalse(context.report.is_finished)
        release.set()
        thread.join(WAIT)

        self.assertFalse(thread.is_alive())
        self.assertIsInstance(ended.get("exc"), AttemptCancelledError)
        self.assertEqual(self.steps.calls, ["slow"])
        self.assertEqual(context.report.step_outcomes, {"slow": StepOutcome.SUCCEEDED})
        self.assertEqual(context.report.unreached_step_ids, ["next"])

    def test_cancellation_after_the_last_step_does_not_undo_a_drained_attempt(self):
        plan = self.plan(spec("a"), spec("b", "a"), policy=CONTINUE)
        context = AttemptContext.for_plan(plan, execution_id="exec_late_cancel")
        self.steps.behaviors["b"] = lambda ctx: context.request_cancellation()

        report = self.engine._run_attempt(plan, context=context)

        self.assertIs(report.termination_reason, TerminationReason.COMPLETED)
        self.assertTrue(report.is_drained)
        self.assertIs(report.outcome, ExecutionOutcome.SUCCEEDED)

    def test_interrupt_inside_a_step_is_not_contained_as_a_step_failure(self):
        self.steps.fail("a", KeyboardInterrupt())

        context, exc = self.run_attempt(
            self.plan(spec("a"), spec("b"), policy=CONTINUE)
        )

        self.assertIsInstance(exc, KeyboardInterrupt)
        self.assertEqual(self.steps.calls, ["a"])
        report = context.report
        self.assertIs(report.termination_reason, TerminationReason.CANCELLED)
        self.assertEqual(report.step_outcomes, {})
        self.assertEqual(report.unreached_step_ids, ["a", "b"])
        self.assertEqual(report.diagnostic.error_type, "KeyboardInterrupt")
        self.assertEqual(report.diagnostic.details, {"running_step_id": "a"})
        self.assertNotIn("step.failed", self.emitter.types())
        self.assert_published_once(report)


class TestInfrastructureFailuresAbortTheAttempt(SchedulingTestCase):
    """Fault injection at each orchestration boundary, under continuation.

    Every case injects the fault while executing ``x``, with an independent
    ``y`` that would otherwise run next. None of them may be drained as an
    ordinary failed branch: the attempt aborts, ``y`` never runs, and the
    original cause stays visible.
    """

    def plan_xy(self) -> Plan:
        return self.plan(spec("x"), spec("y"), policy=CONTINUE)

    def assert_aborted(
        self,
        context: AttemptContext,
        exc: BaseException | None,
        *,
        fragment: str,
        report_published: bool,
        running_step_id: str | None = "x",
    ) -> None:
        """Assert the attempt aborted as an orchestration failure."""
        self.assertIsInstance(exc, OrchestrationError)
        self.assertIn(fragment, str(exc))
        report = context.report
        self.assertIs(report.termination_reason, TerminationReason.ORCHESTRATION_ERROR)
        self.assertFalse(report.is_drained)
        self.assertNotIn(StepOutcome.FAILED, report.step_outcomes.values())
        self.assertNotIn("y", self.steps.calls)
        self.assertIn("y", report.unreached_step_ids)
        diagnostic = report.diagnostic
        assert diagnostic is not None, "an aborted attempt must carry a diagnostic"
        self.assertEqual(diagnostic.details.get("running_step_id"), running_step_id)
        self.assertEqual(
            ATTEMPT_REPORT_EVENT in self.emitter.types(),
            report_published,
            "publication must be skipped exactly when event publication failed",
        )
        if report_published:
            self.assertIsNone(report.publication_failure)
        else:
            # The caller's report says it was never published, and why.
            failure = report.publication_failure
            assert failure is not None
            self.assertEqual(failure.details, {"publication_skipped": True})
            self.assertEqual(failure.error_type, "EventPublicationError")

    # ----- event publication -----

    def test_failing_step_started_publication(self):
        self.emitter.fail_on = {"step.started"}

        context, exc = self.run_attempt(self.plan_xy())

        self.assertIsInstance(exc, EventPublicationError)
        self.assert_aborted(
            context, exc, fragment="step.started", report_published=False
        )

    def test_failing_step_progress_publication(self):
        self.steps.behaviors["x"] = lambda ctx: ctx.progress(50)
        self.emitter.fail_on = {"step.progress"}

        context, exc = self.run_attempt(self.plan_xy())

        self.assert_aborted(
            context, exc, fragment="step.progress", report_published=False
        )

    def test_failing_step_succeeded_publication(self):
        self.emitter.fail_on = {"step.succeeded"}

        context, exc = self.run_attempt(self.plan_xy())

        self.assert_aborted(
            context, exc, fragment="step.succeeded", report_published=False
        )
        self.assertFalse(
            (self.work_root / "sched_plan" / "x" / "success.fingerprint").exists()
        )

    def test_failing_step_failed_publication_preserves_the_step_error(self):
        self.steps.fail("x", PermanentStepError("x broke"))
        self.emitter.fail_on = {"step.failed"}

        context, exc = self.run_attempt(self.plan_xy())

        self.assert_aborted(
            context, exc, fragment="PermanentStepError", report_published=False
        )
        self.assertIn("x broke", str(exc))

    def test_failing_retry_unimplemented_publication(self):
        self.steps.fail("x", TransientStepError("cluster busy"))
        self.emitter.fail_on = {"step.retry_unimplemented"}

        context, exc = self.run_attempt(self.plan_xy())

        self.assert_aborted(
            context,
            exc,
            fragment="step.retry_unimplemented",
            report_published=False,
        )

    def test_failing_step_skipped_publication(self):
        self.run_attempt(self.plan_xy())  # leaves matching cache markers
        self.reset_observations()
        self.emitter.fail_on = {"step.skipped"}

        context, exc = self.run_attempt(self.plan_xy())

        self.assert_aborted(
            context, exc, fragment="step.skipped", report_published=False
        )

    # ----- engine bookkeeping -----

    def test_failing_plan_file_write(self):
        blocker = self.work_root / "blocker"
        blocker.write_text("not a directory", encoding="utf-8")
        engine = Engine(work_root=blocker / "nested", emitter=self.emitter)

        context, exc = self.run_attempt(self.plan_xy(), engine=engine)

        self.assert_aborted(
            context,
            exc,
            fragment="Writing plan.json",
            report_published=True,
            running_step_id=None,
        )
        self.assertEqual(self.steps.calls, [])

    def test_failing_step_directory_creation(self):
        step_path = self.work_root / "sched_plan" / "x"
        step_path.parent.mkdir(parents=True)
        step_path.write_text("in the way", encoding="utf-8")

        context, exc = self.run_attempt(self.plan_xy())

        self.assert_aborted(
            context, exc, fragment="work directory", report_published=True
        )

    def test_failing_cache_marker_read(self):
        (self.work_root / "sched_plan" / "x" / "success.fingerprint").mkdir(
            parents=True
        )

        context, exc = self.run_attempt(self.plan_xy())

        self.assert_aborted(
            context, exc, fragment="Reading the cache marker", report_published=True
        )

    def test_failing_cache_marker_write(self):
        original_replace = os.replace

        def failing_replace(src, dst, *args, **kwargs):
            if Path(dst).name == "success.fingerprint":
                raise OSError("no space left on device")
            return original_replace(src, dst, *args, **kwargs)

        with patch.object(os, "replace", failing_replace):
            context, exc = self.run_attempt(self.plan_xy())

        self.assert_aborted(
            context, exc, fragment="Writing the cache marker", report_published=True
        )
        self.assertIn("no space left", str(exc))
        step_dir = self.work_root / "sched_plan" / "x"
        self.assertEqual(sorted(p.name for p in step_dir.iterdir()), [])

    def test_failing_execution_context_preparation(self):
        """A broken configuration must not drain as N ordinary step failures."""
        with patch(
            "yggdrasil.flow.data_access.DataAccess",
            side_effect=ValueError("malformed external_systems in main.json"),
        ):
            context, exc = self.run_attempt(self.plan_xy())

        self.assert_aborted(
            context,
            exc,
            fragment="Preparing the execution context",
            report_published=True,
        )
        self.assertIn("malformed external_systems", str(exc))
        self.assertEqual(self.steps.calls, [])


class TestAttemptReportPublication(SchedulingTestCase):
    """Exactly one report per attempt, and a failure to publish it is not hidden."""

    def test_each_attempt_publishes_its_own_report_file_without_overwriting(self):
        spool = self.work_root / "spool"
        engine = Engine(
            work_root=self.work_root / "work", emitter=FileSpoolEmitter(spool)
        )
        plan = self.plan(spec("a"), policy=CONTINUE)

        first = engine.run(plan)
        second = engine.run(plan)

        plan_level = sorted((spool / "test" / "sched_plan").glob("*.json"))
        published = [json.loads(p.read_text(encoding="utf-8")) for p in plan_level]
        self.assertEqual(len(published), 2)
        self.assertEqual({p["type"] for p in published}, {ATTEMPT_REPORT_EVENT})
        self.assertNotEqual(first.execution_id, second.execution_id)
        self.assertEqual(
            {p["execution_id"] for p in published},
            {first.execution_id, second.execution_id},
        )
        by_id = {p["execution_id"]: p["report"] for p in published}
        self.assertEqual(by_id[second.execution_id]["step_outcomes"], {"a": "reused"})

    def assert_publication_failure_recorded(
        self, report: AttemptReport, *, superseded: str | None
    ) -> None:
        """Assert the caller's report records that its publication failed.

        Args:
            report: The report the caller holds.
            superseded: The termination reason it was closed with and lost, or
                None if the closing reason was kept.
        """
        failure = report.publication_failure
        assert failure is not None, "the publication failure must be recorded"
        self.assertEqual(failure.error_type, "EventPublicationError")
        self.assertIn("plan.attempt_report", failure.message)
        self.assertEqual(
            failure.details.get("superseded_termination_reason"), superseded
        )
        self.assertFalse(report.is_drained)
        self.assertIs(report.outcome, ExecutionOutcome.FAILED)

    def test_failed_publication_after_successful_work_is_not_a_completion(self):
        self.emitter.fail_on = {ATTEMPT_REPORT_EVENT}

        context, exc = self.run_attempt(self.plan(spec("a"), policy=CONTINUE))

        self.assertIsInstance(exc, EventPublicationError)
        report = context.report
        self.assertIs(report.termination_reason, TerminationReason.ORCHESTRATION_ERROR)
        self.assert_publication_failure_recorded(report, superseded="completed")
        # The work itself is preserved exactly as it happened.
        self.assertEqual(report.step_outcomes, {"a": StepOutcome.SUCCEEDED})
        self.assertIsNone(report.diagnostic)

    def test_failed_publication_after_a_failed_continuation_keeps_its_breakdown(self):
        self.steps.fail("lane_2", PermanentStepError("lane 2 failed"))
        self.emitter.fail_on = {ATTEMPT_REPORT_EVENT}

        context, exc = self.run_attempt(
            self.plan(
                spec("lane_2"),
                spec("lane_2_upload", "lane_2"),
                spec("lane_3"),
                policy=CONTINUE,
            )
        )

        self.assertIsInstance(exc, EventPublicationError)
        report = context.report
        self.assertIs(report.termination_reason, TerminationReason.ORCHESTRATION_ERROR)
        self.assert_publication_failure_recorded(report, superseded="completed")
        self.assertEqual(
            report.step_outcomes,
            {
                "lane_2": StepOutcome.FAILED,
                "lane_2_upload": StepOutcome.BLOCKED,
                "lane_3": StepOutcome.SUCCEEDED,
            },
        )
        self.assertEqual(report.failures["lane_2"].error, "lane 2 failed")
        self.assertEqual(report.failed_ancestors["lane_2_upload"], ["lane_2"])

    def test_failed_publication_after_preflight_rejection_is_not_a_rejection(self):
        """A caller must not retire the request on a rejection it cannot report."""
        self.emitter.fail_on = {ATTEMPT_REPORT_EVENT}

        context, exc = self.run_attempt(
            self.plan(spec("a"), spec("a"), policy=CONTINUE)
        )

        self.assertIsInstance(exc, EventPublicationError)
        self.assertNotIsInstance(exc, PreflightValidationError)
        self.assertTrue(
            any(isinstance(e, PreflightValidationError) for e in exception_chain(exc))
        )
        report = context.report
        self.assertIs(report.termination_reason, TerminationReason.ORCHESTRATION_ERROR)
        self.assert_publication_failure_recorded(
            report, superseded="preflight_rejected"
        )
        # The rejection stays readable as context.
        diagnostic = report.diagnostic
        assert diagnostic is not None
        self.assertEqual(diagnostic.error_type, "PreflightValidationError")

    def test_failed_publication_keeps_the_fail_fast_step_error_visible(self):
        original = RuntimeError("step broke")
        self.steps.fail("a", original)
        self.emitter.fail_on = {ATTEMPT_REPORT_EVENT}

        context, exc = self.run_attempt(self.plan(spec("a")))

        self.assertIsInstance(exc, EventPublicationError)
        self.assertIn("RuntimeError", str(exc))
        self.assertIn("step broke", str(exc))
        self.assertTrue(any(e is original for e in exception_chain(exc)))
        report = context.report
        self.assertIs(report.termination_reason, TerminationReason.ORCHESTRATION_ERROR)
        self.assert_publication_failure_recorded(report, superseded="failed_fast")
        self.assertEqual(report.failures["a"].error, "step broke")

    def test_failed_publication_never_replaces_an_interrupt(self):
        self.steps.fail("a", KeyboardInterrupt())
        self.emitter.fail_on = {ATTEMPT_REPORT_EVENT}

        context, exc = self.run_attempt(self.plan(spec("a"), policy=CONTINUE))

        self.assertIsInstance(exc, KeyboardInterrupt)
        notes = getattr(exc, "__notes__", [])
        self.assertTrue(any("attempt report also failed" in n for n in notes), notes)
        report = context.report
        self.assertIs(report.termination_reason, TerminationReason.CANCELLED)
        self.assert_publication_failure_recorded(report, superseded=None)
        diagnostic = report.diagnostic
        assert diagnostic is not None
        self.assertEqual(diagnostic.error_type, "KeyboardInterrupt")

    def test_failed_publication_never_replaces_a_cooperative_cancellation(self):
        """The exception the caller receives agrees with the report: cancelled."""
        plan = self.plan(spec("a"), spec("b"), policy=CONTINUE)
        context = AttemptContext.for_plan(plan, execution_id="exec_cancel_publish")
        self.steps.behaviors["a"] = lambda ctx: context.request_cancellation()
        self.emitter.fail_on = {ATTEMPT_REPORT_EVENT}

        with self.assertRaises(AttemptCancelledError) as cm:
            self.engine._run_attempt(plan, context=context)

        notes = getattr(cm.exception, "__notes__", [])
        self.assertTrue(any("attempt report also failed" in n for n in notes), notes)
        report = context.report
        self.assertIs(report.termination_reason, TerminationReason.CANCELLED)
        self.assert_publication_failure_recorded(report, superseded=None)
        self.assertEqual(report.unreached_step_ids, ["b"])


class TestEmitterFailuresAreAlwaysPublicationFailures(SchedulingTestCase):
    """An emitter that raises OrchestrationError has still failed to publish.

    Without this, the engine would not recognize that its reporting channel
    broke: it would publish again through it, or let the publication error
    replace an interrupt.
    """

    def setUp(self):
        super().setUp()
        self.emitter.error_type = OrchestrationError

    def assert_publication_error(self, exc: BaseException | None) -> None:
        """Assert a plain OrchestrationError from the emitter was reclassified."""
        self.assertIsInstance(exc, EventPublicationError)
        assert exc is not None
        self.assertIsInstance(exc.__cause__, OrchestrationError)
        self.assertNotIsInstance(exc.__cause__, EventPublicationError)

    def test_cache_skip_publication_is_not_retried_through_the_broken_emitter(self):
        plan = self.plan(spec("a"), policy=CONTINUE)
        self.run_attempt(plan)  # leaves a matching cache marker
        self.reset_observations()
        self.emitter.fail_on = {"step.skipped"}

        context, exc = self.run_attempt(plan)

        self.assert_publication_error(exc)
        self.assertNotIn(ATTEMPT_REPORT_EVENT, self.emitter.types())
        failure = context.report.publication_failure
        assert failure is not None
        self.assertEqual(failure.details, {"publication_skipped": True})

    def test_retry_unimplemented_publication_is_not_retried_either(self):
        self.steps.fail("a", TransientStepError("cluster busy"))
        self.emitter.fail_on = {"step.retry_unimplemented"}

        context, exc = self.run_attempt(self.plan(spec("a"), policy=CONTINUE))

        self.assert_publication_error(exc)
        self.assertNotIn(ATTEMPT_REPORT_EVENT, self.emitter.types())
        self.assertIsNotNone(context.report.publication_failure)

    def test_report_publication_failure_does_not_replace_an_interrupt(self):
        self.steps.fail("a", KeyboardInterrupt())
        self.emitter.fail_on = {ATTEMPT_REPORT_EVENT}

        context, exc = self.run_attempt(self.plan(spec("a"), policy=CONTINUE))

        self.assertIsInstance(exc, KeyboardInterrupt)
        self.assertIs(context.report.termination_reason, TerminationReason.CANCELLED)
        failure = context.report.publication_failure
        assert failure is not None
        self.assertEqual(failure.error_type, "EventPublicationError")


class TestAttemptIsolation(SchedulingTestCase):
    """Attempts sharing one Engine never observe each other's state."""

    def test_two_plans_interleaved_on_one_engine_keep_separate_results(self):
        plan_one_waiting, plan_two_done = threading.Event(), threading.Event()

        def hold(ctx: StepContext) -> None:
            plan_one_waiting.set()
            if not plan_two_done.wait(WAIT):
                raise AssertionError("plan two never finished")

        self.steps.behaviors["one_hold"] = hold
        self.steps.fail("two_bad", PermanentStepError("lane broke"))
        plan_one = self.plan(
            spec("one_hold"), spec("one_after", "one_hold"), plan_id="plan_one"
        )
        plan_two = self.plan(
            spec("two_bad"),
            spec("two_blocked", "two_bad"),
            spec("two_ok"),
            policy=CONTINUE,
            plan_id="plan_two",
        )
        engine_state = dict(vars(self.engine))
        results: dict[str, object] = {}

        def run_plan_one() -> None:
            try:
                results["one"] = self.engine.run(plan_one)
            except BaseException as exc:
                results["one_error"] = exc

        thread = threading.Thread(target=run_plan_one)
        thread.start()
        self.assertTrue(plan_one_waiting.wait(WAIT))
        # Plan two runs start to finish while plan one is paused mid-step.
        report_two = self.engine.run(plan_two)
        plan_two_done.set()
        thread.join(WAIT)

        self.assertFalse(thread.is_alive())
        self.assertNotIn("one_error", results)
        self.assertIsNone(results["one"])
        published = {r["plan_id"]: r for r in self.emitter.reports()}
        self.assertEqual(set(published), {"plan_one", "plan_two"})
        one = published["plan_one"]
        self.assertEqual(one["step_ids"], ["one_hold", "one_after"])
        self.assertEqual(
            one["step_outcomes"], {"one_hold": "succeeded", "one_after": "succeeded"}
        )
        self.assertEqual(one["failures"], {})
        self.assertEqual(published["plan_two"], report_two.to_dict())
        self.assertEqual(
            {sid: o.value for sid, o in report_two.step_outcomes.items()},
            {"two_bad": "failed", "two_blocked": "blocked", "two_ok": "succeeded"},
        )
        self.assertNotEqual(one["execution_id"], report_two.execution_id)
        # The engine itself carries no attempt state.
        self.assertEqual(vars(self.engine), engine_state)


if __name__ == "__main__":
    unittest.main()
