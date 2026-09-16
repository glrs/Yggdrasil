import unittest
from dataclasses import fields

from yggdrasil.flow.model import (
    VALID_FAILURE_POLICIES,
    Artifact,
    Plan,
    StepResult,
    StepSpec,
    validate_failure_policy,
)


class TestStepSpec(unittest.TestCase):
    """
    Comprehensive tests for StepSpec dataclass.

    Tests the specification of a workflow step including its identity,
    callable reference, parameters, dependencies, and scope.
    """

    # =====================================================
    # BASIC INITIALIZATION TESTS
    # =====================================================

    def test_stepspec_initialization_required_fields(self):
        """Test StepSpec initialization with only required fields."""
        spec = StepSpec(
            step_id="step_001",
            name="test_step",
            fn_ref="module.path:function",
            params={"key": "value"},
        )

        self.assertEqual(spec.step_id, "step_001")
        self.assertEqual(spec.name, "test_step")
        self.assertEqual(spec.fn_ref, "module.path:function")
        self.assertEqual(spec.params, {"key": "value"})

    def test_stepspec_initialization_all_fields(self):
        """Test StepSpec initialization with all fields."""
        spec = StepSpec(
            step_id="step_002",
            name="complex_step",
            fn_ref="module:func",
            params={"param1": "value1", "param2": 42},
            deps=["step_001"],
            scope={"kind": "project", "id": "P12345"},
            inputs={"input_file": "/path/to/input.txt"},
        )

        self.assertEqual(spec.step_id, "step_002")
        self.assertEqual(spec.name, "complex_step")
        self.assertEqual(spec.fn_ref, "module:func")
        self.assertEqual(spec.params, {"param1": "value1", "param2": 42})
        self.assertEqual(spec.deps, ["step_001"])
        self.assertEqual(spec.scope, {"kind": "project", "id": "P12345"})
        self.assertEqual(spec.inputs, {"input_file": "/path/to/input.txt"})

    def test_stepspec_default_fields(self):
        """Test that default fields are properly initialized."""
        spec = StepSpec(
            step_id="step_003",
            name="default_test",
            fn_ref="module:func",
            params={},
        )

        # Default fields should be empty collections
        self.assertEqual(spec.deps, [])
        self.assertEqual(spec.scope, {})
        self.assertEqual(spec.inputs, {})
        self.assertIsInstance(spec.deps, list)
        self.assertIsInstance(spec.scope, dict)
        self.assertIsInstance(spec.inputs, dict)

    # =====================================================
    # FIELD TYPE TESTS
    # =====================================================

    def test_stepspec_step_id_field(self):
        """Test step_id field with various formats."""
        # Standard format
        spec1 = StepSpec(
            step_id="cellranger_multi__P12345_1001__9b7f",
            name="cellranger",
            fn_ref="module:func",
            params={},
        )
        self.assertIn("__", spec1.step_id)

        # Simple format
        spec2 = StepSpec(step_id="simple_id", name="step", fn_ref="m:f", params={})
        self.assertEqual(spec2.step_id, "simple_id")

    def test_stepspec_fn_ref_formats(self):
        """Test fn_ref with different reference formats."""
        # Colon-separated
        spec1 = StepSpec(step_id="s1", name="n1", fn_ref="module:function", params={})
        self.assertIn(":", spec1.fn_ref)

        # Nested module
        spec2 = StepSpec(
            step_id="s2", name="n2", fn_ref="package.module:function", params={}
        )
        self.assertEqual(spec2.fn_ref.count(":"), 1)

    def test_stepspec_params_types(self):
        """Test params field with various data types."""
        spec = StepSpec(
            step_id="s1",
            name="n1",
            fn_ref="m:f",
            params={
                "string": "value",
                "int": 42,
                "float": 3.14,
                "bool": True,
                "none": None,
                "list": [1, 2, 3],
                "dict": {"nested": "value"},
            },
        )

        self.assertIsInstance(spec.params["string"], str)
        self.assertIsInstance(spec.params["int"], int)
        self.assertIsInstance(spec.params["float"], float)
        self.assertIsInstance(spec.params["bool"], bool)
        self.assertIsNone(spec.params["none"])
        self.assertIsInstance(spec.params["list"], list)
        self.assertIsInstance(spec.params["dict"], dict)

    def test_stepspec_deps_list(self):
        """Test deps field with dependency lists."""
        # Single dependency
        spec1 = StepSpec(step_id="s2", name="n2", fn_ref="m:f", params={}, deps=["s1"])
        self.assertEqual(len(spec1.deps), 1)

        # Multiple dependencies
        spec2 = StepSpec(
            step_id="s3", name="n3", fn_ref="m:f", params={}, deps=["s1", "s2"]
        )
        self.assertEqual(len(spec2.deps), 2)

        # Empty dependencies
        spec3 = StepSpec(step_id="s1", name="n1", fn_ref="m:f", params={}, deps=[])
        self.assertEqual(len(spec3.deps), 0)

    def test_stepspec_scope_dict(self):
        """Test scope field with various scope formats."""
        # Project scope
        spec1 = StepSpec(
            step_id="s1",
            name="n1",
            fn_ref="m:f",
            params={},
            scope={"kind": "project", "id": "P12345"},
        )
        self.assertEqual(spec1.scope["kind"], "project")

        # Flowcell scope
        spec2 = StepSpec(
            step_id="s2",
            name="n2",
            fn_ref="m:f",
            params={},
            scope={"kind": "flowcell", "id": "FC123"},
        )
        self.assertEqual(spec2.scope["kind"], "flowcell")

    def test_stepspec_inputs_dict(self):
        """Test inputs field mapping input names to paths."""
        spec = StepSpec(
            step_id="s1",
            name="n1",
            fn_ref="m:f",
            params={},
            inputs={
                "bcl_dir": "/data/bcl",
                "samplesheet": "/data/samplesheet.csv",
                "ref_path": "/refs/genome",
            },
        )

        self.assertEqual(len(spec.inputs), 3)
        self.assertTrue(all(isinstance(v, str) for v in spec.inputs.values()))

    # =====================================================
    # DATACLASS BEHAVIOR TESTS
    # =====================================================

    def test_stepspec_is_dataclass(self):
        """Test that StepSpec is a proper dataclass."""
        spec = StepSpec(step_id="s1", name="n1", fn_ref="m:f", params={})

        # Should have dataclass fields
        spec_fields = fields(spec)
        field_names = [f.name for f in spec_fields]

        self.assertIn("step_id", field_names)
        self.assertIn("name", field_names)
        self.assertIn("fn_ref", field_names)
        self.assertIn("params", field_names)
        self.assertIn("deps", field_names)
        self.assertIn("scope", field_names)
        self.assertIn("inputs", field_names)

    def test_stepspec_equality(self):
        """Test StepSpec equality comparison."""
        spec1 = StepSpec(step_id="s1", name="n1", fn_ref="m:f", params={"key": "value"})
        spec2 = StepSpec(step_id="s1", name="n1", fn_ref="m:f", params={"key": "value"})

        self.assertEqual(spec1, spec2)

    def test_stepspec_inequality(self):
        """Test StepSpec inequality when fields differ."""
        spec1 = StepSpec(step_id="s1", name="n1", fn_ref="m:f", params={})
        spec2 = StepSpec(step_id="s2", name="n1", fn_ref="m:f", params={})

        self.assertNotEqual(spec1, spec2)

    def test_stepspec_dict_conversion(self):
        """Test converting StepSpec to dict."""
        spec = StepSpec(
            step_id="s1",
            name="n1",
            fn_ref="m:f",
            params={"key": "value"},
            deps=["dep1"],
            scope={"kind": "project"},
            inputs={"in1": "/path"},
        )

        spec_dict = spec.__dict__

        self.assertEqual(spec_dict["step_id"], "s1")
        self.assertEqual(spec_dict["name"], "n1")
        self.assertEqual(spec_dict["deps"], ["dep1"])

    # =====================================================
    # MUTATION TESTS
    # =====================================================

    def test_stepspec_field_mutation(self):
        """Test that StepSpec fields can be mutated (not frozen)."""
        spec = StepSpec(step_id="s1", name="n1", fn_ref="m:f", params={})

        # Should be able to modify fields
        spec.step_id = "s2"
        spec.params["new_key"] = "new_value"
        spec.deps.append("new_dep")

        self.assertEqual(spec.step_id, "s2")
        self.assertIn("new_key", spec.params)
        self.assertIn("new_dep", spec.deps)

    def test_stepspec_default_factory_independence(self):
        """Test that default factories create independent instances."""
        spec1 = StepSpec(step_id="s1", name="n1", fn_ref="m:f", params={})
        spec2 = StepSpec(step_id="s2", name="n2", fn_ref="m:f", params={})

        # Modify spec1's deps
        spec1.deps.append("dep1")

        # spec2's deps should not be affected
        self.assertEqual(len(spec2.deps), 0)
        self.assertNotIn("dep1", spec2.deps)


class TestPlan(unittest.TestCase):
    """
    Comprehensive tests for Plan dataclass.

    Tests the workflow plan structure containing metadata and
    a sequence of step specifications.
    """

    # =====================================================
    # BASIC INITIALIZATION TESTS
    # =====================================================

    def test_plan_initialization_required_fields(self):
        """Test Plan initialization with required fields."""
        plan = Plan(
            plan_id="plan_001",
            realm="test_realm",
            scope={"kind": "project", "id": "P1"},
        )

        self.assertEqual(plan.plan_id, "plan_001")
        self.assertEqual(plan.realm, "test_realm")
        self.assertEqual(plan.scope, {"kind": "project", "id": "P1"})

    def test_plan_initialization_with_steps(self):
        """Test Plan initialization with steps."""
        steps = [
            StepSpec(step_id="s1", name="step1", fn_ref="m:f1", params={}),
            StepSpec(step_id="s2", name="step2", fn_ref="m:f2", params={}),
        ]

        plan = Plan(
            plan_id="plan_002",
            realm="test_realm",
            scope={"kind": "project"},
            steps=steps,
        )

        self.assertEqual(len(plan.steps), 2)
        self.assertEqual(plan.steps[0].step_id, "s1")
        self.assertEqual(plan.steps[1].step_id, "s2")

    def test_plan_default_steps(self):
        """Test that steps default to empty list."""
        plan = Plan(plan_id="plan_003", realm="realm", scope={})

        self.assertEqual(plan.steps, [])
        self.assertIsInstance(plan.steps, list)

    # =====================================================
    # FIELD TYPE TESTS
    # =====================================================

    def test_plan_id_formats(self):
        """Test various plan_id formats."""
        # Timestamp-based
        plan1 = Plan(
            plan_id="plan_20250101T120000", realm="realm", scope={"kind": "test"}
        )
        self.assertIn("T", plan1.plan_id)

        # Simple
        plan2 = Plan(plan_id="test_plan", realm="realm", scope={})
        self.assertEqual(plan2.plan_id, "test_plan")

    def test_plan_realm_values(self):
        """Test various realm values."""
        realms = ["production", "test_realm", "development", "tenx", "ngi"]

        for realm_value in realms:
            plan = Plan(plan_id="p1", realm=realm_value, scope={})
            self.assertEqual(plan.realm, realm_value)

    def test_plan_scope_formats(self):
        """Test various scope formats."""
        # Project scope
        plan1 = Plan(plan_id="p1", realm="r", scope={"kind": "project", "id": "P12345"})
        self.assertEqual(plan1.scope["kind"], "project")

        # Flowcell scope
        plan2 = Plan(
            plan_id="p2",
            realm="r",
            scope={"kind": "flowcell", "id": "FC123", "lane": 1},
        )
        self.assertEqual(plan2.scope["kind"], "flowcell")

        # Custom scope
        plan3 = Plan(
            plan_id="p3",
            realm="r",
            scope={"type": "custom", "metadata": {"key": "value"}},
        )
        self.assertIn("metadata", plan3.scope)

    def test_plan_steps_list(self):
        """Test steps as a list of StepSpec."""
        steps = [
            StepSpec(step_id=f"s{i}", name=f"step{i}", fn_ref="m:f", params={})
            for i in range(5)
        ]

        plan = Plan(plan_id="p1", realm="r", scope={}, steps=steps)

        self.assertEqual(len(plan.steps), 5)
        self.assertTrue(all(isinstance(s, StepSpec) for s in plan.steps))

    # =====================================================
    # DATACLASS BEHAVIOR TESTS
    # =====================================================

    def test_plan_is_dataclass(self):
        """Test that Plan is a proper dataclass."""
        plan = Plan(plan_id="p1", realm="r", scope={})

        # Should have dataclass fields
        plan_fields = fields(plan)
        field_names = [f.name for f in plan_fields]

        self.assertIn("plan_id", field_names)
        self.assertIn("realm", field_names)
        self.assertIn("scope", field_names)
        self.assertIn("steps", field_names)

    def test_plan_equality(self):
        """Test Plan equality comparison."""
        plan1 = Plan(plan_id="p1", realm="r", scope={"kind": "test"})
        plan2 = Plan(plan_id="p1", realm="r", scope={"kind": "test"})

        self.assertEqual(plan1, plan2)

    def test_plan_inequality_different_steps(self):
        """Test Plan inequality when steps differ."""
        plan1 = Plan(
            plan_id="p1",
            realm="r",
            scope={},
            steps=[StepSpec(step_id="s1", name="n1", fn_ref="m:f", params={})],
        )
        plan2 = Plan(plan_id="p1", realm="r", scope={}, steps=[])

        self.assertNotEqual(plan1, plan2)

    def test_plan_dict_conversion(self):
        """Test converting Plan to dict representation."""
        steps = [StepSpec(step_id="s1", name="n1", fn_ref="m:f", params={})]
        plan = Plan(
            plan_id="p1", realm="test_realm", scope={"kind": "project"}, steps=steps
        )

        plan_dict = plan.__dict__

        self.assertEqual(plan_dict["plan_id"], "p1")
        self.assertEqual(plan_dict["realm"], "test_realm")
        self.assertIsInstance(plan_dict["steps"], list)

    # =====================================================
    # MUTATION TESTS
    # =====================================================

    def test_plan_add_steps(self):
        """Test adding steps to a plan."""
        plan = Plan(plan_id="p1", realm="r", scope={})

        # Add steps
        plan.steps.append(StepSpec(step_id="s1", name="n1", fn_ref="m:f", params={}))
        plan.steps.append(StepSpec(step_id="s2", name="n2", fn_ref="m:f", params={}))

        self.assertEqual(len(plan.steps), 2)

    def test_plan_default_factory_independence(self):
        """Test that default factories create independent instances."""
        plan1 = Plan(plan_id="p1", realm="r", scope={})
        plan2 = Plan(plan_id="p2", realm="r", scope={})

        # Modify plan1's steps
        plan1.steps.append(StepSpec(step_id="s1", name="n1", fn_ref="m:f", params={}))

        # plan2's steps should not be affected
        self.assertEqual(len(plan2.steps), 0)

    # =====================================================
    # INTEGRATION TESTS
    # =====================================================

    def test_plan_complete_workflow_structure(self):
        """Test a complete workflow plan structure."""
        plan = Plan(
            plan_id="workflow_001",
            realm="production",
            scope={"kind": "project", "id": "P12345", "name": "Test Project"},
            steps=[
                StepSpec(
                    step_id="demux_001",
                    name="demultiplex",
                    fn_ref="workflows.demux:run",
                    params={"bcl_dir": "/data/bcl"},
                    deps=[],
                    inputs={"bcl_dir": "/data/bcl"},
                ),
                StepSpec(
                    step_id="qc_001",
                    name="quality_control",
                    fn_ref="workflows.qc:run",
                    params={"fastq_dir": "/data/fastq"},
                    deps=["demux_001"],
                    inputs={"fastq_dir": "/data/fastq"},
                ),
            ],
        )

        self.assertEqual(plan.plan_id, "workflow_001")
        self.assertEqual(len(plan.steps), 2)
        self.assertEqual(plan.steps[1].deps, ["demux_001"])


class TestArtifact(unittest.TestCase):
    """
    Comprehensive tests for Artifact dataclass.

    Tests artifact specification including role, path, and digest.
    """

    # =====================================================
    # BASIC INITIALIZATION TESTS
    # =====================================================

    def test_artifact_initialization_required_fields(self):
        """Test Artifact initialization with required fields."""
        artifact = Artifact(key="output", path="/path/to/file.txt")

        self.assertEqual(artifact.key, "output")
        self.assertEqual(artifact.path, "/path/to/file.txt")
        self.assertIsNone(artifact.digest)

    def test_artifact_initialization_with_digest(self):
        """Test Artifact initialization with digest."""
        artifact = Artifact(
            key="input",
            path="/path/to/input.txt",
            digest="sha256:abc123def456",
        )

        self.assertEqual(artifact.key, "input")
        self.assertEqual(artifact.path, "/path/to/input.txt")
        self.assertEqual(artifact.digest, "sha256:abc123def456")

    def test_artifact_digest_defaults_to_none(self):
        """Test that digest defaults to None."""
        artifact = Artifact(key="output", path="/path")
        self.assertIsNone(artifact.digest)

    # =====================================================
    # FIELD TYPE TESTS
    # =====================================================

    def test_artifact_role_values(self):
        """Test various semantic key values."""
        keys = [
            "library_csv",
            "cr_outs",
            "submit_script",
            "output",
            "input",
            "intermediate",
            "report",
        ]

        for key_value in keys:
            artifact = Artifact(key=key_value, path="/path")
            self.assertEqual(artifact.key, key_value)

    def test_artifact_path_formats(self):
        """Test various path formats."""
        # Absolute path
        art1 = Artifact(key="r", path="/absolute/path/to/file.txt")
        self.assertTrue(art1.path.startswith("/"))

        # Relative path
        art2 = Artifact(key="r", path="relative/path/file.txt")
        self.assertFalse(art2.path.startswith("/"))

        # Windows-style path
        art3 = Artifact(key="r", path="C:\\Windows\\path\\file.txt")
        self.assertIn("\\", art3.path)

    def test_artifact_digest_formats(self):
        """Test various digest formats."""
        # SHA256 for file
        art1 = Artifact(key="r", path="p", digest="sha256:abc123")
        self.assertTrue(art1.digest.startswith("sha256:"))  # type: ignore

        # Dirhash for directory
        art2 = Artifact(key="r", path="p", digest="dirhash:def456")
        self.assertTrue(art2.digest.startswith("dirhash:"))  # type: ignore

        # None (not computed yet)
        art3 = Artifact(key="r", path="p", digest=None)
        self.assertIsNone(art3.digest)

    # =====================================================
    # DATACLASS BEHAVIOR TESTS
    # =====================================================

    def test_artifact_is_dataclass(self):
        """Test that Artifact is a proper dataclass."""
        artifact = Artifact(key="r", path="p")

        # Should have dataclass fields
        artifact_fields = fields(artifact)
        field_names = [f.name for f in artifact_fields]

        self.assertIn("key", field_names)
        self.assertIn("path", field_names)
        self.assertIn("digest", field_names)

    def test_artifact_equality(self):
        """Test Artifact equality comparison."""
        art1 = Artifact(key="output", path="/path", digest="sha256:123")
        art2 = Artifact(key="output", path="/path", digest="sha256:123")

        self.assertEqual(art1, art2)

    def test_artifact_inequality(self):
        """Test Artifact inequality when fields differ."""
        art1 = Artifact(key="output", path="/path1")
        art2 = Artifact(key="output", path="/path2")

        self.assertNotEqual(art1, art2)

    def test_artifact_dict_conversion(self):
        """Test converting Artifact to dict."""
        artifact = Artifact(
            key="library_csv", path="/data/library.csv", digest="sha256:abc"
        )

        artifact_dict = artifact.__dict__

        self.assertEqual(artifact_dict["key"], "library_csv")
        self.assertEqual(artifact_dict["path"], "/data/library.csv")
        self.assertEqual(artifact_dict["digest"], "sha256:abc")

    # =====================================================
    # USE CASE TESTS
    # =====================================================

    def test_artifact_file_output(self):
        """Test artifact for file output."""
        artifact = Artifact(
            key="analysis_report",
            path="/outputs/report.html",
            digest="sha256:abc123def456",
        )

        self.assertEqual(artifact.key, "analysis_report")
        self.assertTrue(artifact.path.endswith(".html"))
        self.assertIsNotNone(artifact.digest)

    def test_artifact_directory_output(self):
        """Test artifact for directory output."""
        artifact = Artifact(
            key="cellranger_outs",
            path="/outputs/cellranger_results",
            digest="dirhash:xyz789",
        )

        self.assertEqual(artifact.key, "cellranger_outs")
        self.assertTrue(artifact.digest.startswith("dirhash:"))  # type: ignore

    def test_artifact_pending_digest(self):
        """Test artifact without computed digest (pending)."""
        artifact = Artifact(key="temp_file", path="/tmp/temp.txt", digest=None)

        self.assertIsNone(artifact.digest)


class TestStepResult(unittest.TestCase):
    """
    Comprehensive tests for StepResult dataclass.

    Tests the result structure returned by workflow steps including
    artifacts, metrics, and extra metadata.
    """

    # =====================================================
    # BASIC INITIALIZATION TESTS
    # =====================================================

    def test_stepresult_initialization_empty(self):
        """Test StepResult initialization with no arguments."""
        result = StepResult()

        self.assertEqual(result.artifacts, [])
        self.assertEqual(result.metrics, {})
        self.assertEqual(result.extra, {})

    def test_stepresult_initialization_with_artifacts(self):
        """Test StepResult initialization with artifacts."""
        artifacts = [
            Artifact(key="output", path="/path1"),
            Artifact(key="log", path="/path2"),
        ]

        result = StepResult(artifacts=artifacts)

        self.assertEqual(len(result.artifacts), 2)
        self.assertEqual(result.artifacts[0].key, "output")

    def test_stepresult_initialization_with_metrics(self):
        """Test StepResult initialization with metrics."""
        metrics = {"count": 100, "duration": 45.5, "success_rate": 0.98}

        result = StepResult(metrics=metrics)

        self.assertEqual(result.metrics["count"], 100)
        self.assertEqual(result.metrics["duration"], 45.5)

    def test_stepresult_initialization_with_extra(self):
        """Test StepResult initialization with extra metadata."""
        extra = {"status": "success", "warnings": [], "debug_info": "detailed log"}

        result = StepResult(extra=extra)

        self.assertEqual(result.extra["status"], "success")
        self.assertIsInstance(result.extra["warnings"], list)

    def test_stepresult_initialization_all_fields(self):
        """Test StepResult initialization with all fields."""
        result = StepResult(
            artifacts=[Artifact(key="output", path="/path")],
            metrics={"count": 10},
            extra={"status": "success"},
        )

        self.assertEqual(len(result.artifacts), 1)
        self.assertIn("count", result.metrics)
        self.assertIn("status", result.extra)

    # =====================================================
    # FIELD TYPE TESTS
    # =====================================================

    def test_stepresult_artifacts_list(self):
        """Test artifacts as list of Artifact objects."""
        artifacts = [Artifact(key=f"output{i}", path=f"/path{i}") for i in range(3)]

        result = StepResult(artifacts=artifacts)

        self.assertEqual(len(result.artifacts), 3)
        self.assertTrue(all(isinstance(a, Artifact) for a in result.artifacts))

    def test_stepresult_metrics_dict_types(self):
        """Test metrics with various value types."""
        result = StepResult(
            metrics={
                "int_metric": 42,
                "float_metric": 3.14,
                "string_metric": "value",
                "bool_metric": True,
                "list_metric": [1, 2, 3],
                "none_metric": None,
            }
        )

        self.assertIsInstance(result.metrics["int_metric"], int)
        self.assertIsInstance(result.metrics["float_metric"], float)
        self.assertIsInstance(result.metrics["string_metric"], str)
        self.assertIsInstance(result.metrics["bool_metric"], bool)
        self.assertIsInstance(result.metrics["list_metric"], list)
        self.assertIsNone(result.metrics["none_metric"])

    def test_stepresult_extra_dict_types(self):
        """Test extra with various value types."""
        result = StepResult(
            extra={
                "metadata": {"nested": "value"},
                "warnings": ["warning1", "warning2"],
                "timestamp": "2025-01-01T00:00:00Z",
            }
        )

        self.assertIsInstance(result.extra["metadata"], dict)
        self.assertIsInstance(result.extra["warnings"], list)
        self.assertIsInstance(result.extra["timestamp"], str)

    # =====================================================
    # DATACLASS BEHAVIOR TESTS
    # =====================================================

    def test_stepresult_is_dataclass(self):
        """Test that StepResult is a proper dataclass."""
        result = StepResult()

        # Should have dataclass fields
        result_fields = fields(result)
        field_names = [f.name for f in result_fields]

        self.assertIn("artifacts", field_names)
        self.assertIn("metrics", field_names)
        self.assertIn("extra", field_names)

    def test_stepresult_equality(self):
        """Test StepResult equality comparison."""
        result1 = StepResult(artifacts=[Artifact(key="r", path="p")], metrics={"m": 1})
        result2 = StepResult(artifacts=[Artifact(key="r", path="p")], metrics={"m": 1})

        self.assertEqual(result1, result2)

    def test_stepresult_inequality(self):
        """Test StepResult inequality when fields differ."""
        result1 = StepResult(metrics={"count": 1})
        result2 = StepResult(metrics={"count": 2})

        self.assertNotEqual(result1, result2)

    def test_stepresult_default_factory_independence(self):
        """Test that default factories create independent instances."""
        result1 = StepResult()
        result2 = StepResult()

        # Modify result1
        result1.artifacts.append(Artifact(key="r", path="p"))
        result1.metrics["key"] = "value"

        # result2 should not be affected
        self.assertEqual(len(result2.artifacts), 0)
        self.assertNotIn("key", result2.metrics)

    # =====================================================
    # MUTATION TESTS
    # =====================================================

    def test_stepresult_add_artifacts(self):
        """Test adding artifacts to result."""
        result = StepResult()

        result.artifacts.append(Artifact(key="output1", path="/path1"))
        result.artifacts.append(Artifact(key="output2", path="/path2"))

        self.assertEqual(len(result.artifacts), 2)

    def test_stepresult_update_metrics(self):
        """Test updating metrics in result."""
        result = StepResult()

        result.metrics["processed"] = 100
        result.metrics["failed"] = 5
        result.metrics["success_rate"] = 0.95

        self.assertEqual(len(result.metrics), 3)

    def test_stepresult_update_extra(self):
        """Test updating extra metadata in result."""
        result = StepResult()

        result.extra["status"] = "complete"
        result.extra["warnings"] = []
        result.extra["duration_seconds"] = 120

        self.assertEqual(len(result.extra), 3)

    # =====================================================
    # USE CASE TESTS
    # =====================================================

    def test_stepresult_successful_step(self):
        """Test StepResult for a successful step execution."""
        result = StepResult(
            artifacts=[
                Artifact(key="output", path="/out/result.txt", digest="sha256:abc"),
                Artifact(key="log", path="/out/log.txt", digest="sha256:def"),
            ],
            metrics={
                "files_processed": 1000,
                "duration_seconds": 120.5,
                "memory_mb": 2048,
            },
            extra={"status": "success", "warnings": []},
        )

        self.assertEqual(len(result.artifacts), 2)
        self.assertEqual(result.metrics["files_processed"], 1000)
        self.assertEqual(result.extra["status"], "success")

    def test_stepresult_failed_step_with_partial_results(self):
        """Test StepResult for a partially failed step."""
        result = StepResult(
            artifacts=[Artifact(key="partial_output", path="/out/partial.txt")],
            metrics={"processed": 500, "failed": 500},
            extra={
                "status": "partial_failure",
                "error": "Processing error at line 501",
                "warnings": ["Timeout warning", "Memory warning"],
            },
        )

        self.assertEqual(result.metrics["failed"], 500)
        self.assertEqual(result.extra["status"], "partial_failure")
        self.assertEqual(len(result.extra["warnings"]), 2)

    def test_stepresult_no_artifacts_with_metrics(self):
        """Test StepResult with metrics but no artifacts."""
        result = StepResult(
            artifacts=[],
            metrics={"validation_passed": True, "checks_run": 10},
            extra={"validation_report": "All checks passed"},
        )

        self.assertEqual(len(result.artifacts), 0)
        self.assertTrue(result.metrics["validation_passed"])


class TestModelIntegration(unittest.TestCase):
    """
    Integration tests showing how model components work together.
    """

    def test_complete_plan_with_all_components(self):
        """Test a complete plan structure with all model components."""
        # Create steps
        step1 = StepSpec(
            step_id="preprocess_001",
            name="preprocess",
            fn_ref="workflows.preprocess:run",
            params={"input_dir": "/data/raw"},
            deps=[],
            scope={"kind": "project", "id": "P12345"},
            inputs={"input_dir": "/data/raw"},
        )

        step2 = StepSpec(
            step_id="analysis_001",
            name="analyze",
            fn_ref="workflows.analysis:run",
            params={"processed_dir": "/data/processed"},
            deps=["preprocess_001"],
            scope={"kind": "project", "id": "P12345"},
            inputs={"processed_dir": "/data/processed"},
        )

        # Create plan
        plan = Plan(
            plan_id="complete_workflow_001",
            realm="production",
            scope={"kind": "project", "id": "P12345", "name": "Full Workflow"},
            steps=[step1, step2],
        )

        # Verify structure
        self.assertEqual(len(plan.steps), 2)
        self.assertEqual(plan.steps[1].deps, ["preprocess_001"])
        self.assertEqual(plan.realm, "production")

    def test_stepresult_with_artifact_references(self):
        """Test StepResult containing multiple artifact types."""
        result = StepResult(
            artifacts=[
                Artifact(
                    key="primary_output",
                    path="/outputs/results.csv",
                    digest="sha256:primary123",
                ),
                Artifact(
                    key="analysis_dir",
                    path="/outputs/analysis",
                    digest="dirhash:dir456",
                ),
                Artifact(
                    key="log_file",
                    path="/outputs/execution.log",
                    digest="sha256:log789",
                ),
            ],
            metrics={
                "total_records": 10000,
                "analysis_duration": 300.5,
                "peak_memory_mb": 4096,
            },
            extra={
                "execution_context": {
                    "hostname": "worker-01",
                    "timestamp": "2025-01-01T00:00:00Z",
                },
                "quality_metrics": {"accuracy": 0.98, "precision": 0.95},
            },
        )

        # Verify all artifact types are present
        keys = {a.key for a in result.artifacts}
        self.assertIn("primary_output", keys)
        self.assertIn("analysis_dir", keys)
        self.assertIn("log_file", keys)

        # Verify mixed digest types
        digests = [a.digest for a in result.artifacts]
        self.assertTrue(any(d.startswith("sha256:") for d in digests))  # type: ignore
        self.assertTrue(any(d.startswith("dirhash:") for d in digests))  # type: ignore


class TestFailurePolicyValidation(unittest.TestCase):
    """validate_failure_policy() is the single source of the policy vocabulary."""

    def test_vocabulary_is_exactly_the_two_documented_values(self):
        self.assertEqual(
            set(VALID_FAILURE_POLICIES), {"fail_fast", "continue_independent"}
        )

    def test_known_policies_are_accepted(self):
        for policy in ("fail_fast", "continue_independent"):
            with self.subTest(policy=policy):
                validate_failure_policy(policy)  # must not raise

    def test_unknown_policy_is_rejected_not_downgraded(self):
        """A requested continuation plan must never silently become fail-fast."""
        with self.assertRaises(ValueError) as cm:
            validate_failure_policy("continue")

        message = str(cm.exception)
        self.assertIn("Invalid failure_policy", message)
        self.assertIn("continue", message)
        self.assertIn("fail_fast", message)

    def test_empty_string_is_rejected(self):
        with self.assertRaises(ValueError):
            validate_failure_policy("")

    def test_none_is_rejected(self):
        with self.assertRaises(ValueError):
            validate_failure_policy(None)  # type: ignore[arg-type]

    def test_validation_is_case_sensitive(self):
        with self.assertRaises(ValueError):
            validate_failure_policy("FAIL_FAST")


class TestStepSpecOutputsField(unittest.TestCase):
    """StepSpec.outputs mirrors inputs: same type, same default."""

    def test_outputs_is_a_field_with_an_empty_default(self):
        names = {f.name for f in fields(StepSpec)}
        self.assertIn("outputs", names)
        self.assertEqual(
            StepSpec(step_id="s", name="n", fn_ref="m:f", params={}).outputs, {}
        )

    def test_default_factory_gives_each_instance_its_own_mapping(self):
        first = StepSpec(step_id="s1", name="n", fn_ref="m:f", params={})
        second = StepSpec(step_id="s2", name="n", fn_ref="m:f", params={})

        first.outputs["k"] = "/p"

        self.assertEqual(second.outputs, {})
        self.assertIsNot(first.outputs, second.outputs)


class TestPlanFailurePolicyField(unittest.TestCase):
    """Plan.failure_policy is a plan-level field with a safe default."""

    def test_failure_policy_defaults_to_fail_fast(self):
        self.assertEqual(
            Plan(plan_id="p", realm="r", scope={}).failure_policy, "fail_fast"
        )

    def test_positional_construction_is_unaffected(self):
        """The new field must not break existing positional call sites."""
        plan = Plan("p1", "tenx", {"kind": "project"}, [])

        self.assertEqual(plan.plan_id, "p1")
        self.assertEqual(plan.failure_policy, "fail_fast")


if __name__ == "__main__":
    unittest.main()
