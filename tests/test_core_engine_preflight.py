"""Tests for the engine's preflight pass.

Preflight's promise is that every structural, policy and callable-binding
problem in a plan is found *before* any step's side effects begin. Every
rejection test here therefore asserts two things: that the plan was rejected
with an actionable message, and that an earlier step which would otherwise have
run left nothing behind.

The plans are built so that failure would be visible: step ``s1`` writes a file
when it runs, and the malformed part is always downstream of it.
"""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from yggdrasil.core.engine import Engine
from yggdrasil.flow.errors import OrchestrationError, PreflightValidationError
from yggdrasil.flow.events.emitter import EventEmitter
from yggdrasil.flow.model import Plan, StepResult, StepSpec
from yggdrasil.flow.step import StepContext, step

# fn_ref for the side-effect step below, resolved through the real resolver.
SIDE_EFFECT_REF = "tests.test_core_engine_preflight:side_effect_step"


@step
def side_effect_step(ctx: StepContext, target: str) -> StepResult:
    """Write a file, so that having run is observable from a test."""
    Path(target).write_text("ran", encoding="utf-8")
    return StepResult()


@step
def plain_step(ctx: StepContext, **kwargs) -> StepResult:
    """A step that does nothing."""
    return StepResult()


def undecorated_step(ctx: StepContext, **kwargs) -> StepResult:
    """Deliberately missing @step, to exercise the registration check."""
    return StepResult()


class PreflightTestCase(unittest.TestCase):
    """Shared fixture: an engine on a temp work root and an observable marker."""

    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.work_root = Path(self.temp_dir.name)
        self.marker = self.work_root / "side_effect.txt"
        self.mock_emitter = Mock(spec=EventEmitter)
        self.engine = Engine(work_root=self.work_root, emitter=self.mock_emitter)

    def tearDown(self):
        self.temp_dir.cleanup()

    def side_effect_spec(self, step_id: str = "s1") -> StepSpec:
        """A step that writes the marker file when executed."""
        return StepSpec(
            step_id=step_id,
            name="side_effect",
            fn_ref=SIDE_EFFECT_REF,
            params={"target": str(self.marker)},
        )

    def plan(self, *steps: StepSpec, failure_policy: str = "fail_fast") -> Plan:
        return Plan(
            plan_id="preflight_plan",
            realm="test",
            scope={"kind": "project", "id": "P1"},
            steps=list(steps),
            failure_policy=failure_policy,
        )

    def assert_rejected_without_side_effects(self, plan: Plan, *expected: str):
        """Assert the plan was rejected and nothing downstream of it ran.

        Args:
            plan: The malformed plan to run.
            *expected: Substrings the diagnostic must contain.

        Returns:
            PreflightValidationError: The raised rejection, for further checks.
        """
        with self.assertRaises(PreflightValidationError) as cm:
            self.engine.run(plan)

        message = str(cm.exception)
        for fragment in expected:
            self.assertIn(fragment, message)

        # Preflight rejections are still ValueErrors for existing callers.
        self.assertIsInstance(cm.exception, ValueError)
        # No step ran...
        self.assertFalse(
            self.marker.exists(),
            "a step ran before the plan was rejected",
        )
        # ...and no execution state was written for the plan at all.
        self.assertFalse((self.work_root / plan.plan_id).exists())
        return cm.exception


class TestGraphValidation(PreflightTestCase):
    """Structural defects in the dependency graph."""

    def test_empty_step_id_is_rejected(self):
        plan = self.plan(
            self.side_effect_spec(),
            StepSpec(step_id="", name="nameless", fn_ref=SIDE_EFFECT_REF, params={}),
        )

        self.assert_rejected_without_side_effects(plan, "empty step_id", "position 1")

    def test_duplicate_step_id_is_rejected(self):
        """Previously the later duplicate silently replaced the earlier one."""
        plan = self.plan(
            self.side_effect_spec(),
            self.side_effect_spec(),
        )

        self.assert_rejected_without_side_effects(
            plan, "Duplicate step_id", "'s1'", "must be unique"
        )

    def test_self_dependency_is_rejected(self):
        """The old existence check could never catch this."""
        plan = self.plan(
            self.side_effect_spec(),
            StepSpec(
                step_id="s2",
                name="n2",
                fn_ref=SIDE_EFFECT_REF,
                params={"target": str(self.marker)},
                deps=["s2"],
            ),
        )

        self.assert_rejected_without_side_effects(plan, "Self-dependency in s2")

    def test_direct_cycle_is_rejected(self):
        plan = self.plan(
            self.side_effect_spec(),
            StepSpec(
                step_id="a", name="a", fn_ref=SIDE_EFFECT_REF, params={}, deps=["b"]
            ),
            StepSpec(
                step_id="b", name="b", fn_ref=SIDE_EFFECT_REF, params={}, deps=["a"]
            ),
        )

        error = self.assert_rejected_without_side_effects(
            plan, "Dependency cycle", "can ever become runnable"
        )
        self.assertIn("a -> b -> a", str(error))

    def test_indirect_cycle_is_rejected(self):
        """A longer loop is just as unschedulable as a two-step one."""
        plan = self.plan(
            self.side_effect_spec(),
            StepSpec(
                step_id="a", name="a", fn_ref=SIDE_EFFECT_REF, params={}, deps=["c"]
            ),
            StepSpec(
                step_id="b", name="b", fn_ref=SIDE_EFFECT_REF, params={}, deps=["a"]
            ),
            StepSpec(
                step_id="c", name="c", fn_ref=SIDE_EFFECT_REF, params={}, deps=["b"]
            ),
        )

        error = self.assert_rejected_without_side_effects(plan, "Dependency cycle")
        self.assertIn("a -> c -> b -> a", str(error))

    def test_unknown_dependency_is_rejected(self):
        plan = self.plan(
            self.side_effect_spec(),
            StepSpec(
                step_id="s2",
                name="n2",
                fn_ref=SIDE_EFFECT_REF,
                params={},
                deps=["nonexistent"],
            ),
        )

        self.assert_rejected_without_side_effects(plan, "Unknown deps", "nonexistent")

    def test_cycle_diagnostic_is_deterministic(self):
        """Operators comparing two runs must see the same cycle reported."""

        def build() -> Plan:
            return self.plan(
                StepSpec(
                    step_id="a", name="a", fn_ref=SIDE_EFFECT_REF, params={}, deps=["c"]
                ),
                StepSpec(
                    step_id="b", name="b", fn_ref=SIDE_EFFECT_REF, params={}, deps=["a"]
                ),
                StepSpec(
                    step_id="c", name="c", fn_ref=SIDE_EFFECT_REF, params={}, deps=["b"]
                ),
            )

        messages = set()
        for _ in range(5):
            with self.assertRaises(PreflightValidationError) as cm:
                self.engine._topo_validate(build())
            messages.add(str(cm.exception))

        self.assertEqual(len(messages), 1)


class TestPolicyValidation(PreflightTestCase):
    """The plan-level failure policy is validated before anything runs."""

    def test_unknown_policy_is_rejected(self):
        plan = self.plan(self.side_effect_spec(), failure_policy="carry_on_regardless")

        self.assert_rejected_without_side_effects(
            plan, "Invalid failure_policy", "carry_on_regardless", "fail_fast"
        )

    def test_known_policies_are_accepted(self):
        for policy in ("fail_fast", "continue_independent"):
            with self.subTest(policy=policy):
                plan = self.plan(
                    StepSpec(
                        step_id="s1",
                        name="n1",
                        fn_ref="tests.test_core_engine_preflight:plain_step",
                        params={},
                    ),
                    failure_policy=policy,
                )
                self.assertEqual(set(self.engine._preflight(plan)), {"s1"})


class TestCallableValidation(PreflightTestCase):
    """Resolution, registration and binding, all before execution."""

    def test_unresolvable_fn_ref_is_rejected(self):
        """A bad reference on a *later* step must not let earlier steps run."""
        plan = self.plan(
            self.side_effect_spec(),
            StepSpec(
                step_id="s2",
                name="n2",
                fn_ref="totally_missing_module_xyz:some_fn",
                params={},
            ),
        )

        self.assert_rejected_without_side_effects(
            plan, "Unresolvable fn_ref", "s2", "totally_missing_module_xyz"
        )

    def test_missing_attribute_in_a_real_module_is_rejected(self):
        plan = self.plan(
            StepSpec(
                step_id="s2",
                name="n2",
                fn_ref="tests.test_core_engine_preflight:no_such_function",
                params={},
            ),
        )

        self.assert_rejected_without_side_effects(
            plan, "Unresolvable fn_ref", "does not define"
        )

    def test_malformed_fn_ref_is_rejected(self):
        plan = self.plan(
            StepSpec(step_id="s2", name="n2", fn_ref="no_separator_here", params={}),
        )

        self.assert_rejected_without_side_effects(plan, "Malformed fn_ref", "s2")

    def test_undecorated_callable_is_rejected(self):
        plan = self.plan(
            self.side_effect_spec(),
            StepSpec(
                step_id="s2",
                name="n2",
                fn_ref="tests.test_core_engine_preflight:undecorated_step",
                params={},
            ),
        )

        self.assert_rejected_without_side_effects(
            plan, "Undecorated step function", "s2", "@step"
        )

    def test_unexpected_keyword_binding_mismatch_is_rejected(self):
        """Caught without ever invoking the step body."""
        plan = self.plan(
            self.side_effect_spec(),
            StepSpec(
                step_id="s2",
                name="n2",
                fn_ref=SIDE_EFFECT_REF,
                params={"target": "/tmp/x", "unexpected": 1},
            ),
        )

        self.assert_rejected_without_side_effects(
            plan, "do not match", "s2", "unexpected"
        )

    def test_missing_required_argument_binding_mismatch_is_rejected(self):
        plan = self.plan(
            self.side_effect_spec(),
            StepSpec(step_id="s2", name="n2", fn_ref=SIDE_EFFECT_REF, params={}),
        )

        self.assert_rejected_without_side_effects(plan, "do not match", "s2", "target")

    def test_binding_ignores_types_and_accepts_kwargs_steps(self):
        """Binding checks names, not types; **kwargs steps accept anything."""
        plan = self.plan(
            StepSpec(
                step_id="s1",
                name="n1",
                fn_ref="tests.test_core_engine_preflight:plain_step",
                params={"anything": object(), "at_all": 3},
            ),
        )

        self.assertEqual(set(self.engine._preflight(plan)), {"s1"})

    def test_uninspectable_signature_is_skipped_not_rejected(self):
        """We must not reject a plan we cannot actually prove is malformed.

        Some C callables have no introspectable signature. Binding validation is
        best effort, so those are left to call time rather than rejected.
        """
        plan = self.plan(
            StepSpec(
                step_id="s1",
                name="n1",
                fn_ref="tests.test_core_engine_preflight:plain_step",
                params={"x": 1},
            ),
        )

        with patch(
            "yggdrasil.core.engine.inspect.signature",
            side_effect=ValueError("no signature found for builtin"),
        ):
            resolved = self.engine._preflight(plan)

        self.assertIs(resolved["s1"], plain_step)


class TestResolutionFailureClassification(PreflightTestCase):
    """A malformed reference is not the same as a broken environment.

    A preflight rejection is definitive, and later phases retire the execution
    request on one. An import that blew up for an unrelated reason must not be
    labelled that way, or a transient environment problem would permanently
    retire a perfectly good request.
    """

    def _single_step_plan(self) -> Plan:
        return self.plan(
            StepSpec(step_id="s1", name="n1", fn_ref="real.module:fn", params={}),
        )

    def test_module_named_in_fn_ref_missing_is_a_plan_defect(self):
        error = ModuleNotFoundError("No module named 'real'", name="real")

        with patch("yggdrasil.core.engine.resolve_callable", side_effect=error):
            with self.assertRaises(PreflightValidationError) as cm:
                self.engine.run(self._single_step_plan())

        self.assertIn("Unresolvable fn_ref", str(cm.exception))

    def test_broken_dependency_inside_a_real_module_is_not_a_plan_defect(self):
        """Same exception type, different missing module - not the plan's fault."""
        error = ModuleNotFoundError("No module named 'psycopg2'", name="psycopg2")

        with patch("yggdrasil.core.engine.resolve_callable", side_effect=error):
            with self.assertRaises(OrchestrationError) as cm:
                self.engine.run(self._single_step_plan())

        self.assertIn("failed", str(cm.exception))
        self.assertNotIsInstance(cm.exception, ValueError)

    def test_import_time_runtime_failure_is_not_a_plan_defect(self):
        with patch(
            "yggdrasil.core.engine.resolve_callable",
            side_effect=RuntimeError("database unavailable at import time"),
        ):
            with self.assertRaises(OrchestrationError) as cm:
                self.engine.run(self._single_step_plan())

        self.assertIn("database unavailable", str(cm.exception))
        self.assertNotIsInstance(cm.exception, ValueError)

    def test_unnamed_module_not_found_is_treated_conservatively(self):
        """Without a name we cannot prove a plan defect, so do not claim one."""
        error = ModuleNotFoundError("No module named ?")

        with patch("yggdrasil.core.engine.resolve_callable", side_effect=error):
            with self.assertRaises(OrchestrationError):
                self.engine.run(self._single_step_plan())


class TestPreflightLeavesExistingStateUntouched(PreflightTestCase):
    """A rejected plan must not disturb what a previous attempt wrote."""

    def test_rejected_plan_does_not_overwrite_an_existing_plan_snapshot(self):
        plan_dir = self.work_root / "preflight_plan"
        plan_dir.mkdir(parents=True)
        snapshot = plan_dir / "plan.json"
        snapshot.write_text('{"from": "a previous attempt"}', encoding="utf-8")
        marker = plan_dir / "s1" / "success.fingerprint"
        marker.parent.mkdir(parents=True)
        marker.write_text("sha256:previous", encoding="utf-8")

        bad_plan = self.plan(
            self.side_effect_spec(),
            StepSpec(step_id="s1", name="dup", fn_ref=SIDE_EFFECT_REF, params={}),
        )

        with self.assertRaises(PreflightValidationError):
            self.engine.run(bad_plan)

        self.assertEqual(snapshot.read_text(), '{"from": "a previous attempt"}')
        self.assertEqual(marker.read_text(), "sha256:previous")
        self.assertFalse(self.marker.exists())


class TestValidPlansStillPass(PreflightTestCase):
    """Tightening must reject only graphs that are genuinely invalid."""

    def test_forward_references_remain_valid(self):
        plan = self.plan(
            StepSpec(
                step_id="s1", name="n1", fn_ref=SIDE_EFFECT_REF, params={}, deps=["s2"]
            ),
            StepSpec(step_id="s2", name="n2", fn_ref=SIDE_EFFECT_REF, params={}),
        )

        self.engine._topo_validate(plan)  # must not raise

    def test_diamond_graph_is_valid(self):
        def spec(step_id, deps=()):
            return StepSpec(
                step_id=step_id,
                name=step_id,
                fn_ref=SIDE_EFFECT_REF,
                params={},
                deps=list(deps),
            )

        plan = self.plan(
            spec("root"),
            spec("left", ["root"]),
            spec("right", ["root"]),
            spec("join", ["left", "right"]),
        )

        self.engine._topo_validate(plan)  # must not raise

    def test_empty_plan_is_valid(self):
        self.assertEqual(self.engine._preflight(self.plan()), {})

    def test_preflight_resolves_each_step_exactly_once(self):
        """The run loop reuses these; it must not resolve a second time."""
        plan = self.plan(
            StepSpec(
                step_id="s1",
                name="n1",
                fn_ref="tests.test_core_engine_preflight:plain_step",
                params={},
            ),
            StepSpec(
                step_id="s2",
                name="n2",
                fn_ref="tests.test_core_engine_preflight:plain_step",
                params={},
                deps=["s1"],
            ),
        )

        with patch(
            "yggdrasil.core.engine.resolve_callable", return_value=plain_step
        ) as mock_resolve:
            self.engine.run(plan)

        self.assertEqual(mock_resolve.call_count, 2)


if __name__ == "__main__":
    unittest.main()
