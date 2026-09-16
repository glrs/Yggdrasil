"""
Unit tests for Plan serialization (to_dict / from_dict).

Tests roundtrip serialization and edge cases for Plan and StepSpec.
"""

import unittest

from yggdrasil.flow.model import Plan, StepSpec


class TestPlanSerialization(unittest.TestCase):
    """Unit tests for Plan serialization methods."""

    def test_plan_to_dict_simple(self):
        """Test simple Plan serialization to dict."""
        plan = Plan(
            plan_id="p1",
            realm="tenx",
            scope={"kind": "project", "id": "P123"},
            steps=[],
        )
        plan_dict = plan.to_dict()

        self.assertEqual(plan_dict["plan_id"], "p1")
        self.assertEqual(plan_dict["realm"], "tenx")
        self.assertEqual(plan_dict["scope"]["id"], "P123")
        self.assertEqual(plan_dict["steps"], [])

    def test_plan_to_dict_with_steps(self):
        """Test Plan serialization with steps."""
        step1 = StepSpec(
            step_id="s1",
            name="preprocess",
            fn_ref="module:preprocess",
            params={"key": "value"},
            deps=["prep"],
            scope={"x": "y"},
            inputs={"input_file": "path/to/input"},
        )
        step2 = StepSpec(
            step_id="s2",
            name="analyze",
            fn_ref="module:analyze",
            params={"threshold": 0.5},
            deps=["s1"],
            scope={},
            inputs={},
        )
        plan = Plan(
            plan_id="p2",
            realm="smartseq3",
            scope={"kind": "flowcell", "id": "FC001"},
            steps=[step1, step2],
        )
        plan_dict = plan.to_dict()

        self.assertEqual(len(plan_dict["steps"]), 2)
        self.assertEqual(plan_dict["steps"][0]["step_id"], "s1")
        self.assertEqual(plan_dict["steps"][0]["fn_ref"], "module:preprocess")
        self.assertEqual(plan_dict["steps"][0]["deps"], ["prep"])
        self.assertEqual(plan_dict["steps"][1]["step_id"], "s2")
        self.assertEqual(plan_dict["steps"][1]["params"]["threshold"], 0.5)

    def test_plan_from_dict_simple(self):
        """Test simple Plan deserialization from dict."""
        plan_dict = {
            "plan_id": "p1",
            "realm": "tenx",
            "scope": {"kind": "project", "id": "P123"},
            "steps": [],
        }
        plan = Plan.from_dict(plan_dict)

        self.assertEqual(plan.plan_id, "p1")
        self.assertEqual(plan.realm, "tenx")
        self.assertEqual(plan.scope["id"], "P123")
        self.assertEqual(len(plan.steps), 0)

    def test_plan_from_dict_with_steps(self):
        """Test Plan deserialization with steps."""
        plan_dict = {
            "plan_id": "p2",
            "realm": "smartseq3",
            "scope": {"kind": "flowcell", "id": "FC001"},
            "steps": [
                {
                    "step_id": "s1",
                    "name": "preprocess",
                    "fn_ref": "module:preprocess",
                    "params": {"key": "value"},
                    "deps": ["prep"],
                    "scope": {"x": "y"},
                    "inputs": {"input_file": "path/to/input"},
                },
                {
                    "step_id": "s2",
                    "name": "analyze",
                    "fn_ref": "module:analyze",
                    "params": {"threshold": 0.5},
                    "deps": ["s1"],
                    "scope": {},
                    "inputs": {},
                },
            ],
        }
        plan = Plan.from_dict(plan_dict)

        self.assertEqual(plan.plan_id, "p2")
        self.assertEqual(len(plan.steps), 2)
        self.assertEqual(plan.steps[0].step_id, "s1")
        self.assertEqual(plan.steps[0].name, "preprocess")
        self.assertEqual(plan.steps[0].fn_ref, "module:preprocess")
        self.assertEqual(plan.steps[0].params["key"], "value")
        self.assertEqual(plan.steps[0].deps, ["prep"])
        self.assertEqual(plan.steps[1].step_id, "s2")

    def test_plan_roundtrip_simple(self):
        """Test roundtrip: Plan -> dict -> Plan -> dict."""
        original = Plan(
            plan_id="p1",
            realm="tenx",
            scope={"kind": "project", "id": "P123"},
            steps=[],
        )

        # Serialize and deserialize
        plan_dict = original.to_dict()
        restored = Plan.from_dict(plan_dict)
        plan_dict2 = restored.to_dict()

        # Check equality
        self.assertEqual(plan_dict, plan_dict2)
        self.assertEqual(original.plan_id, restored.plan_id)
        self.assertEqual(original.realm, restored.realm)
        self.assertEqual(original.scope, restored.scope)

    def test_plan_roundtrip_complex(self):
        """Test roundtrip with complex steps and nested data."""
        original = Plan(
            plan_id="complex_plan",
            realm="multiomics",
            scope={"kind": "sample", "id": "S999", "nested": {"deep": "value"}},
            steps=[
                StepSpec(
                    step_id="qc",
                    name="quality_control",
                    fn_ref="qc.check",
                    params={
                        "thresholds": [0.1, 0.2, 0.3],
                        "config": {"strict": True, "timeout": 300},
                    },
                    deps=["setup"],
                    scope={"workflow": "qc"},
                    inputs={"data": "raw_data.csv"},
                ),
            ],
        )

        # Roundtrip
        plan_dict = original.to_dict()
        restored = Plan.from_dict(plan_dict)

        # Verify restoration
        self.assertEqual(restored.plan_id, original.plan_id)
        self.assertEqual(restored.realm, original.realm)
        self.assertEqual(restored.scope, original.scope)
        self.assertEqual(len(restored.steps), 1)
        self.assertEqual(restored.steps[0].step_id, "qc")
        self.assertEqual(restored.steps[0].params["thresholds"], [0.1, 0.2, 0.3])
        self.assertEqual(restored.steps[0].params["config"]["strict"], True)

    def test_plan_from_dict_missing_optional_fields(self):
        """Test deserialization with missing optional fields (defaults applied)."""
        plan_dict = {
            "plan_id": "p_minimal",
            "realm": "test",
            "scope": {},
            "steps": [
                {
                    "step_id": "s1",
                    "name": "basic",
                    "fn_ref": "module:basic",
                    "params": {},
                    # Missing deps, scope, inputs
                },
            ],
        }
        plan = Plan.from_dict(plan_dict)

        self.assertEqual(len(plan.steps), 1)
        self.assertEqual(plan.steps[0].deps, [])  # Default: empty list
        self.assertEqual(plan.steps[0].scope, {})  # Default: empty dict
        self.assertEqual(plan.steps[0].inputs, {})  # Default: empty dict

    def test_plan_from_dict_missing_scope_key(self):
        """Test deserialization when 'scope' key is missing."""
        plan_dict = {
            "plan_id": "p_no_scope",
            "realm": "test",
            # Missing 'scope' key
            "steps": [],
        }
        plan = Plan.from_dict(plan_dict)

        self.assertEqual(plan.scope, {})  # Default: empty dict

    def test_plan_from_dict_missing_steps_key(self):
        """Test deserialization when 'steps' key is missing."""
        plan_dict = {
            "plan_id": "p_no_steps",
            "realm": "test",
            "scope": {"id": "X"},
            # Missing 'steps' key
        }
        plan = Plan.from_dict(plan_dict)

        self.assertEqual(plan.steps, [])  # Default: empty list

    def test_plan_from_dict_missing_required_field(self):
        """Test deserialization fails when required field is missing."""
        plan_dict = {
            "realm": "test",
            "scope": {},
            # Missing required 'plan_id'
            "steps": [],
        }

        with self.assertRaises(KeyError):
            Plan.from_dict(plan_dict)

    def test_plan_from_dict_missing_realm_field(self):
        """Test deserialization fails when realm is missing."""
        plan_dict = {
            "plan_id": "p1",
            "scope": {},
            # Missing required 'realm'
            "steps": [],
        }

        with self.assertRaises(KeyError):
            Plan.from_dict(plan_dict)

    def test_plan_to_dict_nested_structures(self):
        """Test to_dict handles nested structures correctly."""
        plan = Plan(
            plan_id="nested",
            realm="test",
            scope={"nested": {"deep": {"deeper": "value"}}},
            steps=[],
        )
        plan_dict = plan.to_dict()

        # Verify nested structure is preserved
        self.assertEqual(plan_dict["scope"]["nested"]["deep"]["deeper"], "value")

    def test_plan_from_dict_empty_params(self):
        """Test deserialization with empty params dict."""
        plan_dict = {
            "plan_id": "p1",
            "realm": "test",
            "scope": {},
            "steps": [
                {
                    "step_id": "s1",
                    "name": "test",
                    "fn_ref": "test:test",
                    "params": {},
                    "deps": [],
                    "scope": {},
                    "inputs": {},
                },
            ],
        }
        plan = Plan.from_dict(plan_dict)

        self.assertEqual(plan.steps[0].params, {})

    def test_plan_to_dict_large_params(self):
        """Test to_dict with large params (stress test)."""
        large_params = {f"param_{i}": f"value_{i}" for i in range(1000)}
        step = StepSpec(
            step_id="s_large",
            name="large_step",
            fn_ref="module:large",
            params=large_params,
            deps=[],
            scope={},
            inputs={},
        )
        plan = Plan(
            plan_id="p_large",
            realm="test",
            scope={},
            steps=[step],
        )

        plan_dict = plan.to_dict()
        restored = Plan.from_dict(plan_dict)

        self.assertEqual(len(restored.steps[0].params), 1000)
        self.assertEqual(restored.steps[0].params["param_500"], "value_500")

    def test_stepspec_equality_after_roundtrip(self):
        """Test StepSpec equality is preserved through roundtrip."""
        original_step = StepSpec(
            step_id="s1",
            name="test",
            fn_ref="module:test",
            params={"x": 1},
            deps=["dep1"],
            scope={"s": "1"},
            inputs={"i": "1"},
        )
        plan = Plan(
            plan_id="p",
            realm="test",
            scope={},
            steps=[original_step],
        )

        plan_dict = plan.to_dict()
        restored_plan = Plan.from_dict(plan_dict)
        restored_step = restored_plan.steps[0]

        self.assertEqual(restored_step.step_id, original_step.step_id)
        self.assertEqual(restored_step.name, original_step.name)
        self.assertEqual(restored_step.fn_ref, original_step.fn_ref)
        self.assertEqual(restored_step.params, original_step.params)
        self.assertEqual(restored_step.deps, original_step.deps)
        self.assertEqual(restored_step.scope, original_step.scope)
        self.assertEqual(restored_step.inputs, original_step.inputs)


class TestFailurePolicySerialization(unittest.TestCase):
    """The plan-level failure policy round-trips, and defaults for legacy docs."""

    def test_to_dict_includes_the_policy(self):
        plan = Plan(
            plan_id="p1",
            realm="tenx",
            scope={},
            failure_policy="continue_independent",
        )

        self.assertEqual(plan.to_dict()["failure_policy"], "continue_independent")

    def test_default_policy_is_fail_fast(self):
        plan = Plan(plan_id="p1", realm="tenx", scope={})

        self.assertEqual(plan.failure_policy, "fail_fast")
        self.assertEqual(plan.to_dict()["failure_policy"], "fail_fast")

    def test_absent_policy_deserializes_as_fail_fast(self):
        """Every already-persisted document lacks this key.

        Omitted must be indistinguishable from an explicit "fail_fast", or
        legacy plans would change behavior the moment the field was added.
        """
        legacy_document = {
            "plan_id": "p1",
            "realm": "tenx",
            "scope": {"kind": "project", "id": "P123"},
            "steps": [
                {
                    "step_id": "s1",
                    "name": "n1",
                    "fn_ref": "m:f",
                    "params": {},
                }
            ],
        }

        restored = Plan.from_dict(legacy_document)

        self.assertEqual(restored.failure_policy, "fail_fast")
        explicit = Plan.from_dict({**legacy_document, "failure_policy": "fail_fast"})
        self.assertEqual(restored, explicit)

    def test_policy_survives_a_full_roundtrip(self):
        for policy in ("fail_fast", "continue_independent"):
            with self.subTest(policy=policy):
                original = Plan(
                    plan_id="p1",
                    realm="tenx",
                    scope={"kind": "project", "id": "P123"},
                    steps=[
                        StepSpec(step_id="s1", name="n1", fn_ref="m:f", params={"a": 1})
                    ],
                    failure_policy=policy,
                )

                restored = Plan.from_dict(original.to_dict())

                self.assertEqual(restored, original)
                self.assertEqual(restored.failure_policy, policy)

    def test_an_unknown_policy_still_roundtrips_for_diagnosis(self):
        """Serialization records what was written; preflight is what rejects it."""
        plan = Plan(plan_id="p1", realm="tenx", scope={}, failure_policy="bogus")

        self.assertEqual(Plan.from_dict(plan.to_dict()).failure_policy, "bogus")


class TestStepOutputsSerialization(unittest.TestCase):
    """StepSpec.outputs round-trips exactly the way inputs already does."""

    def test_outputs_default_to_an_empty_mapping(self):
        spec = StepSpec(step_id="s1", name="n1", fn_ref="m:f", params={})

        self.assertEqual(spec.outputs, {})

    def test_outputs_survive_a_full_roundtrip(self):
        original = Plan(
            plan_id="p1",
            realm="tenx",
            scope={},
            steps=[
                StepSpec(
                    step_id="s1",
                    name="n1",
                    fn_ref="m:f",
                    params={},
                    inputs={"src": "/in/a.txt"},
                    outputs={"report": "/out/report.csv", "bam": "/out/x.bam"},
                )
            ],
        )

        restored = Plan.from_dict(original.to_dict())

        self.assertEqual(restored, original)
        self.assertEqual(
            restored.steps[0].outputs,
            {"report": "/out/report.csv", "bam": "/out/x.bam"},
        )

    def test_absent_outputs_deserialize_as_empty(self):
        legacy_step = {
            "step_id": "s1",
            "name": "n1",
            "fn_ref": "m:f",
            "params": {},
            "inputs": {"src": "/in/a.txt"},
        }

        restored = Plan.from_dict(
            {"plan_id": "p1", "realm": "r", "scope": {}, "steps": [legacy_step]}
        )

        self.assertEqual(restored.steps[0].outputs, {})
        self.assertEqual(restored.steps[0].inputs, {"src": "/in/a.txt"})

    def test_outputs_are_independent_between_steps(self):
        first = StepSpec(step_id="s1", name="n1", fn_ref="m:f", params={})
        second = StepSpec(step_id="s2", name="n2", fn_ref="m:f", params={})

        first.outputs["report"] = "/out/report.csv"

        self.assertEqual(second.outputs, {})


if __name__ == "__main__":
    unittest.main()
