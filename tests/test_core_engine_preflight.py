"""Tests for the engine's preflight pass.

Preflight's promise is that every structural, policy and callable-binding
problem in a plan is found *before* any step's side effects begin. Every
rejection test here therefore asserts two things: that the plan was rejected
with an actionable message, and that an earlier step which would otherwise have
run left nothing behind.

The plans are built so that failure would be visible: step ``s1`` writes a file
when it runs, and the malformed part is always downstream of it.
"""

import importlib
import sys
import unittest
import uuid
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


class TestOutputDeclarationValidation(PreflightTestCase):
    """A malformed output declaration cannot gate reuse, so it rejects the plan."""

    def test_malformed_output_declarations_are_rejected(self):
        cases = {
            "not a dict": ["config.txt"],
            "empty path": {"config": ""},
            "non-string path": {"config": 42},
        }
        for name, outputs in cases.items():
            with self.subTest(name):
                plan = self.plan(
                    self.side_effect_spec(),
                    StepSpec(
                        step_id="s2",
                        name="n2",
                        fn_ref="tests.test_core_engine_preflight:plain_step",
                        params={},
                        outputs=outputs,  # type: ignore[arg-type]
                    ),
                )

                self.assert_rejected_without_side_effects(
                    plan, "Malformed outputs", "'s2'"
                )

    def test_absolute_and_workdir_relative_declarations_are_accepted(self):
        plan = self.plan(
            StepSpec(
                step_id="s1",
                name="n1",
                fn_ref="tests.test_core_engine_preflight:plain_step",
                params={},
                outputs={"config": "demux.config", "report": "/shared/report.html"},
            )
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


class TestRealImportFailureClassification(PreflightTestCase):
    """Import-time failures of genuinely importable modules.

    These use real modules on sys.path rather than a patched resolver, because
    the thing under test is exactly what the import machinery raises and what
    it leaves behind in sys.modules. A mocked resolver cannot reproduce either.
    """

    def setUp(self):
        super().setUp()
        self.module_dir = TemporaryDirectory()
        sys.path.insert(0, self.module_dir.name)
        self._module_names: list[str] = []

    def tearDown(self):
        for name in self._module_names:
            sys.modules.pop(name, None)
        sys.path.remove(self.module_dir.name)
        self.module_dir.cleanup()
        super().tearDown()

    def write_module(self, body: str) -> str:
        """Create an importable module with a unique name and return it."""
        name = f"ygg_preflight_probe_{uuid.uuid4().hex[:8]}"
        Path(self.module_dir.name, f"{name}.py").write_text(body, encoding="utf-8")
        self._module_names.append(name)
        # Without this the finder's cached directory listing hides the new file.
        importlib.invalidate_caches()
        return name

    def plan_with(self, fn_ref: str) -> Plan:
        """A plan whose first step writes the marker and whose second is fn_ref."""
        return self.plan(
            self.side_effect_spec(),
            StepSpec(step_id="s2", name="n2", fn_ref=fn_ref, params={}),
        )

    def assert_nonterminal(self, fn_ref: str) -> OrchestrationError:
        """Assert the reference aborts the attempt without retiring the request."""
        with self.assertRaises(OrchestrationError) as cm:
            self.engine.run(self.plan_with(fn_ref))

        # Nonterminal: a later phase must not read this as a definitive
        # rejection and consume the execution request.
        self.assertNotIsInstance(cm.exception, PreflightValidationError)
        self.assertNotIsInstance(cm.exception, ValueError)
        self.assertFalse(self.marker.exists())
        return cm.exception

    def test_import_raising_value_error_is_not_a_plan_defect(self):
        """A ValueError from a module body is not malformed reference syntax."""
        name = self.write_module("raise ValueError('invalid environment setting')")

        error = self.assert_nonterminal(f"{name}:some_fn")

        self.assertIn("invalid environment setting", str(error))

    def test_import_raising_attribute_error_is_not_a_plan_defect(self):
        """An AttributeError from a module body is not a missing attribute."""
        name = self.write_module("raise AttributeError('broken import initialization')")

        error = self.assert_nonterminal(f"{name}:some_fn")

        self.assertIn("broken import initialization", str(error))

    def test_import_raising_runtime_error_is_not_a_plan_defect(self):
        name = self.write_module("raise RuntimeError('database unavailable')")

        error = self.assert_nonterminal(f"{name}:some_fn")

        self.assertIn("database unavailable", str(error))

    def test_module_with_a_missing_dependency_is_not_a_plan_defect(self):
        name = self.write_module("import ygg_definitely_absent_dependency")

        error = self.assert_nonterminal(f"{name}:some_fn")

        self.assertIn("ygg_definitely_absent_dependency", str(error))

    def test_missing_target_module_is_a_plan_defect(self):
        """The reference's own module is absent: that the plan can be blamed for."""
        plan = self.plan_with("ygg_no_such_module_at_all:some_fn")

        self.assert_rejected_without_side_effects(
            plan, "Unresolvable fn_ref", "ygg_no_such_module_at_all"
        )

    def test_missing_attribute_after_a_successful_import_is_a_plan_defect(self):
        name = self.write_module("VALUE = 1\n")

        plan = self.plan_with(f"{name}:some_fn")

        self.assert_rejected_without_side_effects(
            plan, "does not define", "some_fn", name
        )

    def test_dotted_reference_resolves_the_module_the_importer_would(self):
        """'pkg.mod.fn' imports 'pkg.mod', not 'pkg.mod.fn'."""
        name = self.write_module(
            "from yggdrasil.flow.model import StepResult\n"
            "from yggdrasil.flow.step import step\n"
            "\n"
            "@step\n"
            "def probe(ctx, **kwargs):\n"
            "    return StepResult()\n"
        )
        plan = self.plan(
            StepSpec(step_id="s1", name="n1", fn_ref=f"{name}.probe", params={}),
        )

        resolved = self.engine._preflight(plan)

        self.assertTrue(hasattr(resolved["s1"], "_step_name"))

    def test_dotted_reference_to_a_missing_module_is_a_plan_defect(self):
        plan = self.plan_with("ygg_no_such_module_at_all.some_fn")

        self.assert_rejected_without_side_effects(
            plan, "Unresolvable fn_ref", "ygg_no_such_module_at_all"
        )

    def test_dotted_reference_whose_module_fails_to_import_is_not_a_plan_defect(self):
        """The dotted form must get the same treatment as the colon form."""
        name = self.write_module("raise ValueError('invalid environment setting')")

        error = self.assert_nonterminal(f"{name}.some_fn")

        self.assertIn("invalid environment setting", str(error))


class TestNonCallableReferences(PreflightTestCase):
    """Step metadata on an object does not make it executable."""

    def test_non_callable_carrying_step_metadata_is_rejected(self):
        class NotCallable:
            """Carries the decorator's marker but cannot be invoked."""

            _step_name = "looks_like_a_step"

        plan = self.plan(
            self.side_effect_spec(),
            StepSpec(step_id="s2", name="n2", fn_ref="m:f", params={}),
        )

        def resolve(fn_ref):
            return side_effect_step if fn_ref == SIDE_EFFECT_REF else NotCallable()

        with patch("yggdrasil.core.engine.resolve_callable", side_effect=resolve):
            with self.assertRaises(PreflightValidationError) as cm:
                self.engine.run(plan)

        message = str(cm.exception)
        self.assertIn("non-callable", message)
        self.assertIn("s2", message)
        # The earlier step must not have run before this was discovered.
        self.assertFalse(self.marker.exists())
        self.assertFalse((self.work_root / plan.plan_id).exists())

    def test_non_callable_without_step_metadata_is_also_rejected(self):
        plan = self.plan(
            StepSpec(step_id="s1", name="n1", fn_ref="m:f", params={}),
        )

        with patch(
            "yggdrasil.core.engine.resolve_callable", return_value={"not": "callable"}
        ):
            with self.assertRaises(PreflightValidationError) as cm:
                self.engine.run(plan)

        self.assertIn("non-callable", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
