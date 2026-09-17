"""Tests for DependencyScheduler, the engine's readiness state machine.

These run with no engine, emitter or filesystem. The scheduler only decides which
step may run next and records terminal outcomes into an AttemptReport, so a small
driver below plays the engine's part and every graph shape is exercised through
the same protocol the engine uses: start the next runnable step, resolve it,
and (under continuation) block the dependents of a failure.
"""

import unittest

from yggdrasil.core.scheduler import DependencyScheduler
from yggdrasil.flow.errors import OrchestrationError
from yggdrasil.flow.model import StepSpec
from yggdrasil.flow.outcomes import (
    AttemptReport,
    ExecutionOutcome,
    StepFailure,
    StepOutcome,
    TerminationReason,
)


def spec(step_id: str, *deps: str) -> StepSpec:
    """A step with the given dependencies."""
    return StepSpec(
        step_id=step_id, name=step_id, fn_ref="m:f", params={}, deps=list(deps)
    )


def scheduler_for(*steps: StepSpec) -> tuple[DependencyScheduler, AttemptReport]:
    """A scheduler over steps, recording into a fresh report."""
    report = AttemptReport(
        execution_id="exec_1",
        plan_id="plan_1",
        step_ids=[s.step_id for s in steps],
        failure_policy="continue_independent",
    )
    return DependencyScheduler(steps, report), report


def drain(
    scheduler: DependencyScheduler,
    *,
    failing: tuple[str, ...] = (),
    reused: tuple[str, ...] = (),
    contain: bool = True,
) -> list[str]:
    """Drive the scheduler the way the engine does.

    Args:
        scheduler: The scheduler under test.
        failing: Steps that fail when started.
        reused: Steps resolved as reused rather than succeeded.
        contain: Block a failure's dependents and continue (continuation), or
            stop at the first failure (fail-fast).

    Returns:
        list[str]: Step IDs in the order they were started.
    """
    started: list[str] = []
    while scheduler.has_runnable():
        step_id = scheduler.start_next().step_id
        started.append(step_id)
        if step_id in failing:
            scheduler.record_failure(StepFailure(step_id=step_id, error="boom"))
            if not contain:
                break
            scheduler.block_dependents(step_id)
        elif step_id in reused:
            scheduler.record_success(step_id, StepOutcome.REUSED)
        else:
            scheduler.record_success(step_id, StepOutcome.SUCCEEDED)
    return started


class TestReadinessOrder(unittest.TestCase):
    """Dependencies gate execution; plan position breaks ties deterministically."""

    SHAPES: dict[str, tuple[list[StepSpec], list[str]]] = {
        "chain": (
            [spec("a"), spec("b", "a"), spec("c", "b"), spec("d", "c")],
            ["a", "b", "c", "d"],
        ),
        "chain listed in reverse": (
            [spec("d", "c"), spec("c", "b"), spec("b", "a"), spec("a")],
            ["a", "b", "c", "d"],
        ),
        "fan-out": (
            [spec("root"), spec("x", "root"), spec("y", "root"), spec("z", "root")],
            ["root", "x", "y", "z"],
        ),
        "fan-out listed backwards": (
            [spec("z", "root"), spec("y", "root"), spec("x", "root"), spec("root")],
            ["root", "z", "y", "x"],
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
        "independent roots keep list order": (
            [spec("b"), spec("a"), spec("c")],
            ["b", "a", "c"],
        ),
        "forward reference": (
            [spec("s1", "s2"), spec("s2")],
            ["s2", "s1"],
        ),
        # "late" becomes runnable after "a" and sits earlier in the plan than
        # the still-waiting root "b", so it goes next: the tie-break is plan
        # position among the steps runnable at each decision.
        "newly runnable step overtakes a later root": (
            [spec("late", "a"), spec("a"), spec("b")],
            ["a", "late", "b"],
        ),
        "empty plan": ([], []),
    }

    def test_graph_shapes_run_in_dependency_order(self):
        for name, (steps, expected) in self.SHAPES.items():
            with self.subTest(shape=name):
                scheduler, report = scheduler_for(*steps)

                started = drain(scheduler)

                self.assertEqual(started, expected)
                scheduler.ensure_drained()
                self.assertEqual(
                    report.step_outcomes,
                    {step_id: StepOutcome.SUCCEEDED for step_id in expected},
                )

    def test_order_is_identical_across_repeated_runs(self):
        for name, (steps, _) in self.SHAPES.items():
            with self.subTest(shape=name):
                orders = {tuple(drain(scheduler_for(*steps)[0])) for _ in range(5)}
                self.assertEqual(len(orders), 1)

    def test_correctly_ordered_legacy_plan_keeps_its_list_order(self):
        steps = [
            spec("a"),
            spec("b", "a"),
            spec("c"),
            spec("d", "c"),
            spec("e", "b", "d"),
        ]
        scheduler, _ = scheduler_for(*steps)

        self.assertEqual(drain(scheduler), ["a", "b", "c", "d", "e"])

    def test_reused_steps_satisfy_their_dependents(self):
        scheduler, report = scheduler_for(spec("a"), spec("b", "a"), spec("c", "b"))

        started = drain(scheduler, reused=("a", "b", "c"))

        self.assertEqual(started, ["a", "b", "c"])
        self.assertEqual(set(report.step_outcomes.values()), {StepOutcome.REUSED})

    def test_a_dependency_listed_twice_is_satisfied_once(self):
        scheduler, _ = scheduler_for(spec("a"), spec("b", "a", "a"))

        self.assertEqual(drain(scheduler), ["a", "b"])

    def test_a_step_is_not_runnable_until_every_prerequisite_succeeded(self):
        scheduler, _ = scheduler_for(spec("a"), spec("b"), spec("join", "a", "b"))

        self.assertEqual(scheduler.start_next().step_id, "a")
        scheduler.record_success("a", StepOutcome.SUCCEEDED)
        self.assertEqual(scheduler.start_next().step_id, "b")
        # With "b" still running, "join" has an unsatisfied prerequisite.
        self.assertFalse(scheduler.has_runnable())
        scheduler.record_success("b", StepOutcome.SUCCEEDED)
        self.assertEqual(scheduler.start_next().step_id, "join")


class TestFailureContainment(unittest.TestCase):
    """A failure blocks exactly the steps that require its success."""

    def test_branch_failure_blocks_its_lane_while_another_lane_completes(self):
        scheduler, report = scheduler_for(
            spec("root"),
            spec("demux_2", "root"),
            spec("collect_2", "demux_2"),
            spec("upload_2", "collect_2"),
            spec("demux_3", "root"),
            spec("collect_3", "demux_3"),
            spec("upload_3", "collect_3"),
        )

        started = drain(scheduler, failing=("demux_2",))

        self.assertEqual(
            started,
            ["root", "demux_2", "demux_3", "collect_3", "upload_3"],
        )
        scheduler.ensure_drained()
        self.assertEqual(
            report.step_outcomes,
            {
                "root": StepOutcome.SUCCEEDED,
                "demux_2": StepOutcome.FAILED,
                "collect_2": StepOutcome.BLOCKED,
                "upload_2": StepOutcome.BLOCKED,
                "demux_3": StepOutcome.SUCCEEDED,
                "collect_3": StepOutcome.SUCCEEDED,
                "upload_3": StepOutcome.SUCCEEDED,
            },
        )

    def test_shared_prerequisite_failure_blocks_every_dependent_not_other_roots(self):
        scheduler, report = scheduler_for(
            spec("metadata"),
            spec("lane_1", "metadata"),
            spec("lane_2", "metadata"),
            spec("unrelated"),
        )

        started = drain(scheduler, failing=("metadata",))

        self.assertEqual(started, ["metadata", "unrelated"])
        self.assertIs(report.step_outcomes["lane_1"], StepOutcome.BLOCKED)
        self.assertIs(report.step_outcomes["lane_2"], StepOutcome.BLOCKED)
        self.assertIs(report.step_outcomes["unrelated"], StepOutcome.SUCCEEDED)

    def test_join_is_blocked_by_one_failed_prerequisite_even_if_another_succeeded(
        self,
    ):
        scheduler, report = scheduler_for(
            spec("ok"), spec("bad"), spec("join", "ok", "bad")
        )

        started = drain(scheduler, failing=("bad",))

        self.assertEqual(started, ["ok", "bad"])
        self.assertIs(report.step_outcomes["ok"], StepOutcome.SUCCEEDED)
        self.assertIs(report.step_outcomes["join"], StepOutcome.BLOCKED)
        self.assertEqual(report.direct_blockers["join"], ["bad"])

    def test_blocking_is_transitive_and_returned_in_plan_order(self):
        scheduler, report = scheduler_for(
            spec("a"), spec("z", "b"), spec("b", "a"), spec("c", "a")
        )

        scheduler.start_next()
        scheduler.record_failure(StepFailure(step_id="a", error="boom"))
        newly_blocked = scheduler.block_dependents("a")

        self.assertEqual(newly_blocked, ["z", "b", "c"])
        self.assertFalse(scheduler.has_runnable())
        scheduler.ensure_drained()

    def test_blocking_leaves_resolved_and_unrelated_steps_untouched(self):
        scheduler, report = scheduler_for(
            spec("a"), spec("b"), spec("join", "a", "b"), spec("unrelated")
        )

        scheduler.start_next()
        scheduler.record_success("a", StepOutcome.SUCCEEDED)
        scheduler.start_next()
        scheduler.record_failure(StepFailure(step_id="b", error="boom"))

        self.assertEqual(scheduler.block_dependents("b"), ["join"])
        self.assertIs(report.step_outcomes["a"], StepOutcome.SUCCEEDED)
        self.assertNotIn("unrelated", report.step_outcomes)
        self.assertEqual(scheduler.start_next().step_id, "unrelated")

    def test_a_step_already_blocked_is_not_blocked_again(self):
        scheduler, report = scheduler_for(spec("x"), spec("y"), spec("join", "x", "y"))
        scheduler.start_next()
        scheduler.record_failure(StepFailure(step_id="x", error="boom"))
        self.assertEqual(scheduler.block_dependents("x"), ["join"])
        scheduler.start_next()
        scheduler.record_failure(StepFailure(step_id="y", error="boom"))

        self.assertEqual(scheduler.block_dependents("y"), [])
        self.assertIs(report.step_outcomes["join"], StepOutcome.BLOCKED)

    def test_fail_fast_usage_leaves_dependents_unreached_not_blocked(self):
        """Without blocking, work never evaluated has no outcome at all."""
        scheduler, report = scheduler_for(
            spec("a"), spec("dependent", "a"), spec("other")
        )

        drain(scheduler, failing=("a",), contain=False)
        scheduler.record_blocker_diagnostics()

        self.assertEqual(report.step_outcomes, {"a": StepOutcome.FAILED})
        self.assertEqual(report.unreached_step_ids, ["dependent", "other"])
        self.assertEqual(report.direct_blockers, {})


class TestBlockerDiagnostics(unittest.TestCase):
    """The final pass names every blocker, including ones that failed late."""

    def test_failure_arriving_after_a_join_was_blocked_is_still_reported(self):
        scheduler, report = scheduler_for(
            spec("x"), spec("join", "x", "y"), spec("after", "join"), spec("y")
        )

        started = drain(scheduler, failing=("x", "y"))
        # Before the final pass, the join only knows its first blocker.
        self.assertEqual(report.direct_blockers["join"], ["x"])

        scheduler.record_blocker_diagnostics()

        self.assertEqual(started, ["x", "y"])
        self.assertEqual(report.direct_blockers["join"], ["x", "y"])
        self.assertEqual(report.failed_ancestors["join"], ["x", "y"])
        self.assertEqual(report.direct_blockers["after"], ["join"])
        self.assertEqual(report.failed_ancestors["after"], ["x", "y"])

    def test_failed_ancestors_are_traced_through_chains_of_blocked_steps(self):
        scheduler, report = scheduler_for(
            spec("origin"),
            spec("hop_1", "origin"),
            spec("hop_2", "hop_1"),
            spec("healthy"),
            spec("join", "hop_2", "healthy"),
        )

        drain(scheduler, failing=("origin",))
        scheduler.record_blocker_diagnostics()

        self.assertEqual(report.direct_blockers["join"], ["hop_2"])
        self.assertEqual(report.failed_ancestors["join"], ["origin"])
        self.assertEqual(report.failed_ancestors["hop_1"], ["origin"])

    def test_independent_failures_do_not_leak_into_each_others_ancestors(self):
        scheduler, report = scheduler_for(
            spec("a"), spec("a_child", "a"), spec("b"), spec("b_child", "b")
        )

        drain(scheduler, failing=("a", "b"))
        scheduler.record_blocker_diagnostics()

        self.assertEqual(report.failed_ancestors["a_child"], ["a"])
        self.assertEqual(report.failed_ancestors["b_child"], ["b"])

    def test_diagnostics_pass_is_repeatable(self):
        scheduler, report = scheduler_for(spec("x"), spec("y"), spec("join", "x", "y"))
        drain(scheduler, failing=("x", "y"))

        scheduler.record_blocker_diagnostics()
        first = (dict(report.direct_blockers), dict(report.failed_ancestors))
        scheduler.record_blocker_diagnostics()

        self.assertEqual((report.direct_blockers, report.failed_ancestors), first)

    def test_drained_report_with_blocked_work_finishes_completed_but_failed(self):
        scheduler, report = scheduler_for(spec("a"), spec("b", "a"), spec("c"))
        drain(scheduler, failing=("a",))
        scheduler.ensure_drained()
        scheduler.record_blocker_diagnostics()

        report.finish(TerminationReason.COMPLETED)

        self.assertTrue(report.is_drained)
        self.assertIs(report.outcome, ExecutionOutcome.FAILED)


class TestSchedulerInvariants(unittest.TestCase):
    """Protocol misuse and impossible states are engine defects, not plan defects."""

    def test_cannot_start_a_step_while_another_is_running(self):
        scheduler, _ = scheduler_for(spec("a"), spec("b"))
        scheduler.start_next()

        with self.assertRaises(OrchestrationError) as cm:
            scheduler.start_next()

        self.assertIn("'a' is running", str(cm.exception))

    def test_cannot_start_when_nothing_is_runnable(self):
        scheduler, _ = scheduler_for()

        with self.assertRaises(OrchestrationError):
            scheduler.start_next()

    def test_cannot_resolve_a_step_that_is_not_running(self):
        scheduler, report = scheduler_for(spec("a"), spec("b"))
        scheduler.start_next()

        with self.assertRaises(OrchestrationError):
            scheduler.record_success("b", StepOutcome.SUCCEEDED)
        with self.assertRaises(OrchestrationError):
            scheduler.record_failure(StepFailure(step_id="b", error="boom"))
        self.assertEqual(report.step_outcomes, {})

    def test_success_requires_an_outcome_that_satisfies_dependencies(self):
        scheduler, report = scheduler_for(spec("a"), spec("b", "a"))
        scheduler.start_next()

        for outcome in (StepOutcome.FAILED, StepOutcome.BLOCKED):
            with self.subTest(outcome=outcome):
                with self.assertRaises(OrchestrationError):
                    scheduler.record_success("a", outcome)
        self.assertEqual(report.step_outcomes, {})

    def test_cannot_block_the_dependents_of_a_step_that_did_not_fail(self):
        scheduler, _ = scheduler_for(spec("a"), spec("b", "a"))
        scheduler.start_next()
        scheduler.record_success("a", StepOutcome.SUCCEEDED)

        with self.assertRaises(OrchestrationError):
            scheduler.block_dependents("a")

    def test_a_stalled_graph_is_an_invariant_failure_not_a_completion(self):
        """Preflight rejects cycles; if one got through, it must not look drained."""
        scheduler, report = scheduler_for(spec("a", "b"), spec("b", "a"), spec("c"))

        started = drain(scheduler)

        self.assertEqual(started, ["c"])
        with self.assertRaises(OrchestrationError) as cm:
            scheduler.ensure_drained()
        self.assertIn("no possible progress", str(cm.exception))
        self.assertIn("['a', 'b']", str(cm.exception))
        self.assertEqual(report.unreached_step_ids, ["a", "b"])

    def test_ensure_drained_rejects_a_step_still_running(self):
        scheduler, _ = scheduler_for(spec("a"))
        scheduler.start_next()

        with self.assertRaises(OrchestrationError) as cm:
            scheduler.ensure_drained()

        self.assertIn("still running", str(cm.exception))

    def test_running_step_id_tracks_the_step_in_flight(self):
        scheduler, _ = scheduler_for(spec("a"))

        self.assertIsNone(scheduler.running_step_id)
        scheduler.start_next()
        self.assertEqual(scheduler.running_step_id, "a")
        scheduler.record_success("a", StepOutcome.SUCCEEDED)
        self.assertIsNone(scheduler.running_step_id)


if __name__ == "__main__":
    unittest.main()
