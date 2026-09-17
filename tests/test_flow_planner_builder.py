import os
import unittest
from collections.abc import Callable
from enum import Enum
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated, Any
from unittest.mock import Mock, patch

from yggdrasil.core.engine import Engine
from yggdrasil.flow.artifacts import SimpleArtifactRef
from yggdrasil.flow.events.emitter import EventEmitter
from yggdrasil.flow.model import Plan, StepResult, StepSpec
from yggdrasil.flow.planner.builder import PlanBuilder, _artifact_key, _validate_step_id
from yggdrasil.flow.step import Out, StepContext, step


class TestUtilityFunctions(unittest.TestCase):
    """
    Comprehensive tests for builder utility functions.

    Tests helper functions for validation and key normalization.
    """

    # =====================================================
    # STEP ID VALIDATION TESTS
    # =====================================================

    def test_validate_step_id_valid_simple(self):
        """Test validation with simple valid step IDs."""
        valid_ids = ["step1", "Step2", "my_step", "step-name", "s", "Step_1-test"]

        for step_id in valid_ids:
            # Should not raise
            _validate_step_id(step_id)

    def test_validate_step_id_must_start_with_letter(self):
        """Test that step_id must start with a letter."""
        invalid_ids = ["1step", "_step", "-step", "123"]

        for step_id in invalid_ids:
            with self.assertRaises(ValueError) as context:
                _validate_step_id(step_id)
            self.assertIn("must match", str(context.exception))

    def test_validate_step_id_allows_letters_numbers_underscore_dash(self):
        """Test that step_id allows letters, numbers, underscore, dash."""
        valid_ids = [
            "a1",
            "Step_123",
            "my-step-name",
            "Step_1_2_3",
            "step-with-dashes",
            "MixedCase123",
        ]

        for step_id in valid_ids:
            _validate_step_id(step_id)

    def test_validate_step_id_rejects_empty_string(self):
        """Test that empty string is rejected."""
        with self.assertRaises(ValueError):
            _validate_step_id("")

    def test_validate_step_id_rejects_special_characters(self):
        """Test that special characters are rejected."""
        invalid_ids = [
            "step.name",
            "step name",
            "step@name",
            "step#1",
            "step!",
            "step$",
        ]

        for step_id in invalid_ids:
            with self.assertRaises(ValueError):
                _validate_step_id(step_id)

    # =====================================================
    # ROLE KEY NORMALIZATION TESTS
    # =====================================================

    def test_artifact_key_with_string(self):
        """Test _artifact_key with string input."""
        self.assertEqual(_artifact_key("input"), "input")
        self.assertEqual(_artifact_key("my_role"), "my_role")
        self.assertEqual(_artifact_key(""), "")

    def test_artifact_key_with_enum(self):
        """Test _artifact_key with Enum input."""

        class Role(Enum):
            INPUT = "input_data"
            OUTPUT = "output_data"

        self.assertEqual(_artifact_key(Role.INPUT), "input_data")
        self.assertEqual(_artifact_key(Role.OUTPUT), "output_data")

    def test_artifact_key_with_different_enum_types(self):
        """Test _artifact_key with various Enum types."""

        class FileRole(Enum):
            RAW = "raw_data"
            PROCESSED = "processed_data"

        class Status(Enum):
            READY = "ready"
            DONE = "done"

        self.assertEqual(_artifact_key(FileRole.RAW), "raw_data")
        self.assertEqual(_artifact_key(Status.READY), "ready")


class TestPlanBuilder(unittest.TestCase):
    """
    Comprehensive tests for PlanBuilder class.

    Tests the plan builder including path management, role wiring,
    step addition, and dependency resolution.
    """

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = TemporaryDirectory()
        self.base = Path(self.temp_dir.name) / "work"
        self.builder = PlanBuilder(
            plan_id="test_plan_001",
            realm="test",
            scope={"kind": "project", "id": "P123"},
            base=self.base,
        )

    def tearDown(self):
        """Clean up temporary resources."""
        self.temp_dir.cleanup()

    # =====================================================
    # INITIALIZATION TESTS
    # =====================================================

    def test_plan_builder_initialization(self):
        """Test PlanBuilder initialization."""
        builder = PlanBuilder(
            plan_id="plan_001",
            realm="production",
            scope={"kind": "project", "id": "P456"},
            base=Path("/tmp/work"),
        )

        self.assertEqual(builder.plan_id, "plan_001")
        self.assertEqual(builder.realm, "production")
        self.assertEqual(builder.scope["id"], "P456")
        self.assertEqual(builder.base, Path("/tmp/work"))
        self.assertEqual(builder.steps, [])
        self.assertEqual(builder._artifact_provider, {})
        self.assertEqual(builder._artifact_path, {})

    def test_plan_builder_is_dataclass(self):
        """Test that PlanBuilder is a dataclass."""
        self.assertTrue(hasattr(self.builder, "__dataclass_fields__"))

    # =====================================================
    # PATH HELPER TESTS
    # =====================================================

    def test_artifact_path_creates_paths(self):
        """Test that artifact_path resolves artifact references to paths."""
        from yggdrasil.flow.artifacts import SimpleArtifactRef

        ref = SimpleArtifactRef(
            key_name="input_data", folder="input", filename="data.txt"
        )
        path = self.builder.artifact_path(ref)

        self.assertEqual(path, self.base / "input" / "data.txt")
        self.assertTrue(path.parent.exists())

    def test_artifact_workspace_creates_directory(self):
        """Test that artifact_workspace creates workspace directories."""
        from yggdrasil.flow.artifacts import SimpleArtifactRef

        ref = SimpleArtifactRef(key_name="output", folder="outputs")
        workspace = self.builder.artifact_workspace(ref)

        self.assertTrue(workspace.exists())
        self.assertTrue(workspace.is_dir())
        self.assertEqual(workspace, self.base / "outputs")

    def test_artifact_workspace_with_file_ref(self):
        """Test artifact_workspace when ref resolves to a file."""
        from yggdrasil.flow.artifacts import SimpleArtifactRef

        ref = SimpleArtifactRef(
            key_name="config", folder="configs", filename="app.json"
        )
        workspace = self.builder.artifact_workspace(ref)

        # Should return the parent directory, not the file
        self.assertEqual(workspace, self.base / "configs")
        self.assertTrue(workspace.exists())
        self.assertTrue(workspace.is_dir())

    def test_artifact_path_with_multiple_refs(self):
        """Test artifact_path resolves different refs to different paths."""
        from yggdrasil.flow.artifacts import SimpleArtifactRef

        ref1 = SimpleArtifactRef(key_name="input", folder="inputs", filename="data.csv")
        ref2 = SimpleArtifactRef(
            key_name="output", folder="outputs", filename="result.json"
        )
        ref3 = SimpleArtifactRef(key_name="logs", folder="logs")

        path1 = self.builder.artifact_path(ref1)
        path2 = self.builder.artifact_path(ref2)
        path3 = self.builder.artifact_path(ref3)

        self.assertEqual(path1, self.base / "inputs" / "data.csv")
        self.assertEqual(path2, self.base / "outputs" / "result.json")
        self.assertEqual(path3, self.base / "logs")

        # All parent directories should be created
        self.assertTrue(path1.parent.exists())
        self.assertTrue(path2.parent.exists())
        self.assertTrue(path3.exists())  # This one is a directory

    def test_artifact_workspace_idempotent(self):
        """Test that artifact_workspace is idempotent."""
        from yggdrasil.flow.artifacts import SimpleArtifactRef

        ref = SimpleArtifactRef(key_name="data", folder="datadir")

        ws1 = self.builder.artifact_workspace(ref)
        ws2 = self.builder.artifact_workspace(ref)

        self.assertEqual(ws1, ws2)
        self.assertTrue(ws1.exists())

    def test_artifact_path_creates_nested_directories(self):
        """Test that artifact_path creates nested directory structures."""
        from yggdrasil.flow.artifacts import SimpleArtifactRef

        ref = SimpleArtifactRef(
            key_name="nested", folder="a/b/c/d", filename="file.txt"
        )
        path = self.builder.artifact_path(ref)

        self.assertEqual(path, self.base / "a" / "b" / "c" / "d" / "file.txt")
        self.assertTrue(path.parent.exists())
        self.assertEqual(str(path.parent), str(self.base / "a" / "b" / "c" / "d"))

    # =====================================================
    # ARTIFACT REGISTRATION TESTS
    # =====================================================

    def test_record_artifact_registers_key(self):
        """Test that record_artifact registers artifact key mapping."""
        self.builder.record_artifact("input_data", "/path/to/input", by_step_id="step1")

        self.assertEqual(self.builder._artifact_provider["input_data"], "step1")
        self.assertEqual(self.builder._artifact_path["input_data"], "/path/to/input")

    def test_record_artifact_multiple_keys(self):
        """Test registering multiple artifact keys."""
        self.builder.record_artifact("key1", "/path1", by_step_id="step1")
        self.builder.record_artifact("key2", "/path2", by_step_id="step2")
        self.builder.record_artifact("key3", "/path3", by_step_id="step1")

        self.assertEqual(self.builder._artifact_provider["key1"], "step1")
        self.assertEqual(self.builder._artifact_provider["key2"], "step2")
        self.assertEqual(self.builder._artifact_path["key2"], "/path2")

    def test_record_artifact_with_enum_key(self):
        """Test record_artifact with Enum key."""

        class DataKey(Enum):
            INPUT = "input_data"

        self.builder.record_artifact(DataKey.INPUT, "/path", by_step_id="s1")  # type: ignore

        self.assertEqual(self.builder._artifact_provider["input_data"], "s1")

    def test_path_for_returns_registered_path(self):
        """Test that path_for returns registered path."""
        self.builder.record_artifact("my_input", "/data/input.txt", by_step_id="s1")

        path = self.builder.path_for("my_input")

        self.assertEqual(path, "/data/input.txt")

    def test_path_for_raises_on_missing_key(self):
        """Test that path_for raises KeyError for unregistered artifact key."""
        with self.assertRaises(KeyError) as context:
            self.builder.path_for("nonexistent_key")

        self.assertIn("no known path", str(context.exception))

    def test_path_for_with_enum_key(self):
        """Test path_for with Enum key."""

        class DataKey(Enum):
            OUTPUT = "output_data"

        self.builder.record_artifact(DataKey.OUTPUT, "/out/data", by_step_id="s1")  # type: ignore

        path = self.builder.path_for(DataKey.OUTPUT)  # type: ignore

        self.assertEqual(path, "/out/data")

    # =====================================================
    # STEP ADDITION TESTS
    # =====================================================

    def test_add_step_fn_simple(self):
        """Test adding a step with a function."""

        def my_function():
            pass

        spec = self.builder.add_step_fn(
            my_function, step_id="step1", params={"key": "value"}
        )

        self.assertEqual(spec.step_id, "step1")
        self.assertEqual(spec.name, "my_function")
        self.assertEqual(spec.params["key"], "value")
        self.assertEqual(len(self.builder.steps), 1)

    def test_add_step_fn_generates_default_id(self):
        """Test that add_step_fn generates default step ID."""

        def process_data():
            pass

        spec = self.builder.add_step_fn(process_data, params={})

        # Default ID includes function name and project ID
        self.assertIn("process_data", spec.step_id)
        self.assertIn("P123", spec.step_id)
        self.assertIn("v1", spec.step_id)

    def test_add_step_fn_with_custom_version(self):
        """Test add_step_fn with custom version."""

        def my_func():
            pass

        spec = self.builder.add_step_fn(my_func, params={}, version="v2")

        self.assertIn("v2", spec.step_id)

    def test_add_step_fn_validates_step_id(self):
        """Test that invalid step IDs are rejected."""

        def test_func():
            pass

        with self.assertRaises(ValueError):
            self.builder.add_step_fn(test_func, step_id="123invalid", params={})

    def test_add_step_fn_with_annotated_inputs(self):
        """Test adding step with In[] annotated inputs."""
        from typing import Annotated

        from yggdrasil.flow.artifacts import SimpleArtifactRef
        from yggdrasil.flow.step import In

        # Register an artifact first
        raw_ref = SimpleArtifactRef(
            key_name="raw_data", folder="input", filename="data.csv"
        )
        self.builder.record_artifact(
            "raw_data", str(self.builder.artifact_path(raw_ref)), by_step_id="prep"
        )

        def analyze(ctx, input_data: Annotated[Path, In(raw_ref)]):
            pass

        # Manually set step metadata that @step() would set
        analyze.__step_inputs__ = {"input_data": raw_ref}
        analyze.__step_outputs__ = {}

        spec = self.builder.add_step_fn(
            analyze,
            step_id="analyze_step",
            params={},
        )

        # Should have input mapped
        self.assertIn("raw_data", spec.inputs)
        self.assertEqual(spec.deps, ["prep"])

    def test_add_step_fn_with_annotated_outputs(self):
        """Test adding step that produces outputs via Out[] annotations."""
        from typing import Annotated

        from yggdrasil.flow.artifacts import SimpleArtifactRef
        from yggdrasil.flow.step import Out

        output_ref = SimpleArtifactRef(
            key_name="output_data", folder="output", filename="result.txt"
        )

        def generate_data(ctx, result: Annotated[Path, Out(output_ref)]):
            pass

        # Manually set step metadata that @step() would set
        generate_data.__step_inputs__ = {}
        generate_data.__step_outputs__ = {"result": output_ref}

        spec = self.builder.add_step_fn(
            generate_data,
            step_id="gen_step",
            params={},
        )

        # Artifact should be registered by _add_step after processing outputs
        self.assertIn("output_data", self.builder._artifact_provider)
        self.assertEqual(self.builder._artifact_provider["output_data"], "gen_step")
        # ...and declared on the spec, keyed by artifact key
        self.assertEqual(
            spec.outputs, {"output_data": str(self.base / "output" / "result.txt")}
        )

    def test_add_step_fn_with_requires_artifacts(self):
        """Test adding step with explicit artifact requirements."""

        def step1():
            pass

        step1.__step_inputs__ = {}
        step1.__step_outputs__ = {}

        def step2():
            pass

        step2.__step_inputs__ = {}
        step2.__step_outputs__ = {}

        # Step 1 produces an artifact
        from yggdrasil.flow.artifacts import SimpleArtifactRef

        inter_ref = SimpleArtifactRef(key_name="intermediate", folder="temp")
        self.builder.record_artifact(
            "intermediate", str(self.builder.artifact_path(inter_ref)), by_step_id="s1"
        )

        self.builder.add_step_fn(
            step1,
            step_id="s1",
            params={},
        )

        # Step 2 requires that artifact
        spec2 = self.builder.add_step_fn(
            step2,
            step_id="s2",
            params={},
            requires_artifacts=["intermediate"],
        )

        # Should have dependency on s1
        self.assertIn("s1", spec2.deps)

    def test_add_step_fn_automatic_dependency_from_annotated_inputs(self):
        """Test that dependencies are inferred from In[] annotations."""
        from typing import Annotated

        from yggdrasil.flow.artifacts import SimpleArtifactRef
        from yggdrasil.flow.step import In, Out

        data_ref = SimpleArtifactRef(key_name="data", folder="prep")

        def prep(ctx, output: Annotated[Path, Out(data_ref)]):
            pass

        # Manually set step metadata that @step() would set
        prep.__step_inputs__ = {}
        prep.__step_outputs__ = {"output": data_ref}

        def analyze(ctx, input_data: Annotated[Path, In(data_ref)]):
            pass

        # Manually set step metadata that @step() would set
        analyze.__step_inputs__ = {"input_data": data_ref}
        analyze.__step_outputs__ = {}

        # Add prep first
        self.builder.add_step_fn(
            prep,
            step_id="prep_step",
            params={},
        )

        # Add analyze - should automatically depend on prep_step
        spec = self.builder.add_step_fn(
            analyze,
            step_id="analyze_step",
            params={},
        )

        # Should automatically depend on prep_step
        self.assertIn("prep_step", spec.deps)

    def test_add_step_fn_fails_on_missing_required_artifact(self):
        """Test that adding step fails if required artifact doesn't exist."""

        def my_step():
            pass

        my_step.__step_inputs__ = {}
        my_step.__step_outputs__ = {}

        with self.assertRaises(KeyError) as context:
            self.builder.add_step_fn(
                my_step,
                step_id="step1",
                params={},
                requires_artifacts=["nonexistent_artifact"],
            )

        self.assertIn("no provider", str(context.exception))

    def test_add_step_fn_multiple_dependencies(self):
        """Test step with multiple dependencies."""
        from yggdrasil.flow.artifacts import SimpleArtifactRef

        def step1():
            pass

        step1.__step_inputs__ = {}
        step1.__step_outputs__ = {}

        def step2():
            pass

        step2.__step_inputs__ = {}
        step2.__step_outputs__ = {}

        def step3():
            pass

        step3.__step_inputs__ = {}
        step3.__step_outputs__ = {}

        # Create two artifact providers
        ref1 = SimpleArtifactRef(key_name="key1", folder="data1")
        ref2 = SimpleArtifactRef(key_name="key2", folder="data2")
        self.builder.record_artifact(
            "key1", str(self.builder.artifact_path(ref1)), by_step_id="s1"
        )
        self.builder.record_artifact(
            "key2", str(self.builder.artifact_path(ref2)), by_step_id="s2"
        )

        self.builder.add_step_fn(step1, step_id="s1", params={})
        self.builder.add_step_fn(step2, step_id="s2", params={})

        # Step 3 depends on both
        spec = self.builder.add_step_fn(
            step3,
            step_id="s3",
            params={},
            requires_artifacts=["key1", "key2"],
        )

        self.assertIn("s1", spec.deps)
        self.assertIn("s2", spec.deps)
        self.assertEqual(len(spec.deps), 2)

    def test_add_step_fn_no_self_dependency(self):
        """Test that step doesn't depend on itself."""
        from yggdrasil.flow.artifacts import SimpleArtifactRef

        def step1():
            pass

        step1.__step_inputs__ = {}
        step1.__step_outputs__ = {}

        def step2():
            pass

        step2.__step_inputs__ = {}
        step2.__step_outputs__ = {}

        # Step 1 produces an artifact
        ref = SimpleArtifactRef(key_name="shared", folder="shared")
        self.builder.record_artifact(
            "shared", str(self.builder.artifact_path(ref)), by_step_id="s1"
        )

        self.builder.add_step_fn(step1, step_id="s1", params={})

        # Step 2 requires that artifact and will produce its own version
        spec = self.builder.add_step_fn(
            step2,
            step_id="s2",
            params={},
            requires_artifacts=["shared"],
        )

        # Should depend on s1, not itself
        self.assertIn("s1", spec.deps)
        self.assertNotIn("s2", spec.deps)

    def test_add_step_fn_with_enum_artifact_keys(self):
        """Test adding steps with Enum-based artifact keys."""

        class ArtifactKey(Enum):
            RAW = "raw_data"
            PROCESSED = "processed_data"

        def generate():
            pass

        generate.__step_inputs__ = {}
        generate.__step_outputs__ = {}

        def process():
            pass

        process.__step_inputs__ = {}
        process.__step_outputs__ = {}

        # First step produces raw data
        from yggdrasil.flow.artifacts import SimpleArtifactRef

        raw_ref = SimpleArtifactRef(key_name=ArtifactKey.RAW.value, folder="input")
        self.builder.record_artifact(
            ArtifactKey.RAW.value,
            str(self.builder.artifact_path(raw_ref)),
            by_step_id="generate_step",
        )

        self.builder.add_step_fn(
            generate,
            step_id="generate_step",
            params={},
        )

        # Second step processes it
        spec = self.builder.add_step_fn(
            process,
            step_id="process_step",
            params={},
            requires_artifacts=[ArtifactKey.RAW],
        )

        # Should have dependency
        self.assertIn("generate_step", spec.deps)

    # =====================================================
    # PLAN FINALIZATION TESTS
    # =====================================================

    def test_to_plan_creates_plan(self):
        """Test that to_plan creates Plan object."""

        def step1():
            pass

        self.builder.add_step_fn(step1, step_id="s1", params={})

        plan = self.builder.to_plan()

        self.assertIsInstance(plan, Plan)
        self.assertEqual(plan.plan_id, "test_plan_001")
        self.assertEqual(plan.realm, "test")
        self.assertEqual(plan.scope["id"], "P123")
        self.assertEqual(len(plan.steps), 1)

    def test_to_plan_with_empty_steps(self):
        """Test to_plan with no steps."""
        plan = self.builder.to_plan()

        self.assertEqual(len(plan.steps), 0)
        self.assertEqual(plan.plan_id, "test_plan_001")

    def test_to_plan_preserves_step_order(self):
        """Test that to_plan preserves step order."""

        def step1():
            pass

        def step2():
            pass

        def step3():
            pass

        self.builder.add_step_fn(step1, step_id="s1", params={})
        self.builder.add_step_fn(step2, step_id="s2", params={})
        self.builder.add_step_fn(step3, step_id="s3", params={})

        plan = self.builder.to_plan()

        self.assertEqual(plan.steps[0].step_id, "s1")
        self.assertEqual(plan.steps[1].step_id, "s2")
        self.assertEqual(plan.steps[2].step_id, "s3")

    def test_to_plan_with_dependencies(self):
        """Test to_plan with dependent steps."""
        from yggdrasil.flow.artifacts import SimpleArtifactRef

        def prep():
            pass

        prep.__step_inputs__ = {}
        prep.__step_outputs__ = {}

        def analyze():
            pass

        analyze.__step_inputs__ = {}
        analyze.__step_outputs__ = {}

        # Register artifact
        ref = SimpleArtifactRef(key_name="data", folder="data")
        self.builder.record_artifact(
            "data", str(self.builder.artifact_path(ref)), by_step_id="prep"
        )

        self.builder.add_step_fn(prep, step_id="prep", params={})
        self.builder.add_step_fn(
            analyze,
            step_id="analyze",
            params={},
            requires_artifacts=["data"],
        )

        plan = self.builder.to_plan()

        # Analyze step should depend on prep
        analyze_step = next(s for s in plan.steps if s.step_id == "analyze")
        self.assertIn("prep", analyze_step.deps)

    # =====================================================
    # INTEGRATION TESTS
    # =====================================================

    def test_complete_workflow_linear(self):
        """Test building a complete linear workflow."""
        from yggdrasil.flow.artifacts import SimpleArtifactRef

        def download_data():
            pass

        download_data.__step_inputs__ = {}
        download_data.__step_outputs__ = {}

        def preprocess():
            pass

        preprocess.__step_inputs__ = {}
        preprocess.__step_outputs__ = {}

        def analyze():
            pass

        analyze.__step_inputs__ = {}
        analyze.__step_outputs__ = {}

        # Build linear pipeline with artifact dependencies
        raw_ref = SimpleArtifactRef(
            key_name="raw_data", folder="raw", filename="data.csv"
        )
        clean_ref = SimpleArtifactRef(
            key_name="clean_data", folder="clean", filename="data.csv"
        )
        results_ref = SimpleArtifactRef(
            key_name="results", folder="results", filename="output.json"
        )

        # Register artifacts as they're produced
        self.builder.record_artifact(
            "raw_data", str(self.builder.artifact_path(raw_ref)), by_step_id="download"
        )
        self.builder.record_artifact(
            "clean_data",
            str(self.builder.artifact_path(clean_ref)),
            by_step_id="preprocess",
        )
        self.builder.record_artifact(
            "results",
            str(self.builder.artifact_path(results_ref)),
            by_step_id="analyze",
        )

        self.builder.add_step_fn(
            download_data,
            step_id="download",
            params={"url": "http://example.com/data"},
        )

        self.builder.add_step_fn(
            preprocess,
            step_id="preprocess",
            params={},
            requires_artifacts=["raw_data"],
        )

        self.builder.add_step_fn(
            analyze,
            step_id="analyze",
            params={},
            requires_artifacts=["clean_data"],
        )

        plan = self.builder.to_plan()

        # Verify structure
        self.assertEqual(len(plan.steps), 3)

        # Verify dependencies
        preprocess_step = next(s for s in plan.steps if s.step_id == "preprocess")
        self.assertEqual(preprocess_step.deps, ["download"])

        analyze_step = next(s for s in plan.steps if s.step_id == "analyze")
        self.assertEqual(analyze_step.deps, ["preprocess"])

    def test_complete_workflow_branching(self):
        """Test building a workflow with branching."""
        from yggdrasil.flow.artifacts import SimpleArtifactRef

        def fetch():
            pass

        fetch.__step_inputs__ = {}
        fetch.__step_outputs__ = {}

        def process_a():
            pass

        process_a.__step_inputs__ = {}
        process_a.__step_outputs__ = {}

        def process_b():
            pass

        process_b.__step_inputs__ = {}
        process_b.__step_outputs__ = {}

        def merge():
            pass

        merge.__step_inputs__ = {}
        merge.__step_outputs__ = {}

        # Register artifacts
        data_ref = SimpleArtifactRef(
            key_name="data", folder="input", filename="data.csv"
        )
        result_a_ref = SimpleArtifactRef(
            key_name="result_a", folder="branch_a", filename="result.json"
        )
        result_b_ref = SimpleArtifactRef(
            key_name="result_b", folder="branch_b", filename="result.json"
        )
        merged_ref = SimpleArtifactRef(
            key_name="final", folder="output", filename="merged.json"
        )

        self.builder.record_artifact(
            "data", str(self.builder.artifact_path(data_ref)), by_step_id="fetch"
        )
        self.builder.record_artifact(
            "result_a",
            str(self.builder.artifact_path(result_a_ref)),
            by_step_id="process_a",
        )
        self.builder.record_artifact(
            "result_b",
            str(self.builder.artifact_path(result_b_ref)),
            by_step_id="process_b",
        )
        self.builder.record_artifact(
            "final", str(self.builder.artifact_path(merged_ref)), by_step_id="merge"
        )

        self.builder.add_step_fn(fetch, step_id="fetch", params={})

        self.builder.add_step_fn(
            process_a,
            step_id="process_a",
            params={},
            requires_artifacts=["data"],
        )

        self.builder.add_step_fn(
            process_b,
            step_id="process_b",
            params={},
            requires_artifacts=["data"],
        )

        self.builder.add_step_fn(
            merge,
            step_id="merge",
            params={},
            requires_artifacts=["result_a", "result_b"],
        )

        plan = self.builder.to_plan()

        # Both process steps depend on fetch
        process_a_step = next(s for s in plan.steps if s.step_id == "process_a")
        process_b_step = next(s for s in plan.steps if s.step_id == "process_b")
        self.assertIn("fetch", process_a_step.deps)
        self.assertIn("fetch", process_b_step.deps)

        # Merge depends on both process steps
        merge_step = next(s for s in plan.steps if s.step_id == "merge")
        self.assertIn("process_a", merge_step.deps)
        self.assertIn("process_b", merge_step.deps)

    def test_workflow_with_artifact_paths(self):
        """Test using artifact_path for file management."""
        from yggdrasil.flow.artifacts import SimpleArtifactRef

        def step1():
            pass

        step1.__step_inputs__ = {}
        step1.__step_outputs__ = {}

        def step2():
            pass

        step2.__step_inputs__ = {}
        step2.__step_outputs__ = {}

        # Use artifact_path helpers
        input_ref = SimpleArtifactRef(
            key_name="intermediate", folder="input", filename="data.txt"
        )
        output_ref = SimpleArtifactRef(
            key_name="output", folder="output", filename="result.txt"
        )

        input_file = self.builder.artifact_path(input_ref)
        output_file = self.builder.artifact_path(output_ref)

        self.builder.record_artifact("intermediate", str(input_file), by_step_id="s1")
        self.builder.record_artifact("output", str(output_file), by_step_id="s2")

        self.builder.add_step_fn(
            step1,
            step_id="s1",
            params={"input": str(input_file)},
        )

        self.builder.add_step_fn(
            step2,
            step_id="s2",
            params={"output": str(output_file)},
            requires_artifacts=["intermediate"],
        )

        plan = self.builder.to_plan()

        # Verify paths were created
        self.assertTrue(input_file.parent.exists())
        self.assertTrue(output_file.parent.exists())

        # Verify plan structure
        self.assertEqual(len(plan.steps), 2)
        self.assertIn("s1", plan.steps[1].deps)

    def test_builder_reusability(self):
        """Test that builder can generate multiple plans."""

        def step1():
            pass

        self.builder.add_step_fn(step1, step_id="s1", params={})

        plan1 = self.builder.to_plan()
        plan2 = self.builder.to_plan()

        # Both should be valid but separate objects
        self.assertEqual(plan1.plan_id, plan2.plan_id)
        self.assertEqual(len(plan1.steps), len(plan2.steps))
        self.assertIsNot(plan1, plan2)


class TestPlanBuilderEdgeCases(unittest.TestCase):
    """
    Tests for edge cases and error conditions in PlanBuilder.
    """

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = TemporaryDirectory()
        self.base = Path(self.temp_dir.name) / "work"
        self.builder = PlanBuilder(
            plan_id="edge_case_plan",
            realm="test",
            scope={},
            base=self.base,
        )

    def tearDown(self):
        """Clean up temporary resources."""
        self.temp_dir.cleanup()

    def test_no_circular_self_dependency(self):
        """Test that builder creates valid dependency chains."""
        from yggdrasil.flow.artifacts import SimpleArtifactRef

        def step_a():
            pass

        step_a.__step_inputs__ = {}
        step_a.__step_outputs__ = {}

        def step_b():
            pass

        step_b.__step_inputs__ = {}
        step_b.__step_outputs__ = {}

        # Create linear dependency
        ref_a = SimpleArtifactRef(key_name="key_a", folder="a")
        ref_b = SimpleArtifactRef(key_name="key_b", folder="b")
        self.builder.record_artifact(
            "key_a", str(self.builder.artifact_path(ref_a)), by_step_id="a"
        )
        self.builder.record_artifact(
            "key_b", str(self.builder.artifact_path(ref_b)), by_step_id="b"
        )

        self.builder.add_step_fn(step_a, step_id="a", params={})
        self.builder.add_step_fn(
            step_b,
            step_id="b",
            params={},
            requires_artifacts=["key_a"],
        )

        # This should work (no actual cycle)
        plan = self.builder.to_plan()
        self.assertEqual(len(plan.steps), 2)
        # B depends on A
        self.assertIn("a", plan.steps[1].deps)

    def test_artifact_key_override(self):
        """Test that recording same artifact key twice updates the provider."""
        from yggdrasil.flow.artifacts import SimpleArtifactRef

        ref1 = SimpleArtifactRef(key_name="shared_key", folder="path1")
        ref2 = SimpleArtifactRef(key_name="shared_key", folder="path2")

        self.builder.record_artifact(
            "shared_key", str(self.builder.artifact_path(ref1)), by_step_id="s1"
        )
        self.builder.record_artifact(
            "shared_key", str(self.builder.artifact_path(ref2)), by_step_id="s2"
        )

        # Latest provider wins
        self.assertEqual(self.builder._artifact_provider["shared_key"], "s2")

    def test_empty_params(self):
        """Test adding step with empty params."""

        def simple_step():
            pass

        simple_step.__step_inputs__ = {}
        simple_step.__step_outputs__ = {}

        spec = self.builder.add_step_fn(simple_step, step_id="s1", params={})

        self.assertEqual(spec.params, {})

    def test_complex_params(self):
        """Test adding step with complex params."""
        from yggdrasil.flow.artifacts import SimpleArtifactRef

        def complex_step():
            pass

        complex_step.__step_inputs__ = {}
        complex_step.__step_outputs__ = {}

        # Create an artifact path for testing
        ref = SimpleArtifactRef(key_name="data", folder="data", filename="file.txt")
        artifact_path = self.builder.artifact_path(ref)

        params = {
            "simple": "value",
            "number": 42,
            "list": [1, 2, 3],
            "dict": {"nested": "data"},
            "path": str(artifact_path),
        }

        spec = self.builder.add_step_fn(complex_step, step_id="s1", params=params)

        self.assertEqual(spec.params["simple"], "value")
        self.assertEqual(spec.params["number"], 42)
        self.assertEqual(len(spec.params["list"]), 3)
        self.assertIn("nested", spec.params["dict"])


RESULT_REF = SimpleArtifactRef(key_name="result", folder="results", filename="out.json")


def make_writer(
    received: list[Path], default: Any = None, with_default: bool = False
) -> Callable[..., StepResult]:
    """A @step writing its annotated output parameter, recording where it wrote.

    Args:
        received: Collects the output path each invocation was called with.
        default: The output parameter's concrete default, if with_default.
        with_default: Whether the output parameter has a concrete default.
    """

    def body(ctx: StepContext, result: Path) -> StepResult:
        path = Path(result)
        received.append(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("done", encoding="utf-8")
        return StepResult()

    if with_default:

        def write_result(
            ctx: StepContext, result: Annotated[Path, Out(RESULT_REF)] = default
        ) -> StepResult:
            return body(ctx, result)

    else:

        def write_result(  # type: ignore[misc]
            ctx: StepContext, result: Annotated[Path, Out(RESULT_REF)]
        ) -> StepResult:
            return body(ctx, result)

    return step(write_result)


class TestBuilderPreservesDeclaredOutputs(unittest.TestCase):
    """The output map reaches the StepSpec, and only as absolute paths.

    Formerly a documented gap: the builder computed the map, used it to wire
    dependencies, and dropped it before constructing the StepSpec.
    """

    def setUp(self):
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.base = Path(temp_dir.name)
        self.builder = PlanBuilder(
            plan_id="p1", realm="test", scope={"kind": "project"}, base=self.base
        )

    def test_add_step_preserves_the_computed_output_map(self):
        spec = self.builder._add_step(
            step_id="producer",
            name="producer",
            fn_ref="m:produce",
            params={},
            outputs={"report": str(self.base / "report.csv")},
        )

        self.assertEqual(self.builder._artifact_provider["report"], "producer")
        self.assertEqual(spec.outputs, {"report": str(self.base / "report.csv")})
        self.assertEqual(
            Plan.from_dict(self.builder.to_plan().to_dict()).steps[0].outputs,
            spec.outputs,
        )

    def test_preserved_outputs_still_wire_dependencies(self):
        self.builder._add_step(
            step_id="producer",
            name="producer",
            fn_ref="m:produce",
            params={},
            outputs={"report": str(self.base / "report.csv")},
        )

        consumer = self.builder._add_step(
            step_id="consumer",
            name="consumer",
            fn_ref="m:consume",
            params={},
            inputs={"report": str(self.base / "report.csv")},
        )

        self.assertEqual(consumer.deps, ["producer"])

    def test_add_step_rejects_a_relative_output_path(self):
        """The engine would resolve it against the step workdir a second time."""
        with self.assertRaises(ValueError) as cm:
            self.builder._add_step(
                step_id="producer",
                name="producer",
                fn_ref="m:produce",
                params={},
                outputs={"report": "report.csv"},
            )

        self.assertIn("report", str(cm.exception))
        self.assertEqual(self.builder.steps, [])
        self.assertNotIn("report", self.builder._artifact_provider)

    def test_relative_base_is_made_absolute_on_construction(self):
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(self.base)

        builder = PlanBuilder(plan_id="p1", realm="test", scope={}, base=Path("rel"))

        self.assertTrue(builder.base.is_absolute())
        self.assertEqual(builder.base, Path.cwd() / "rel")


class TestBuilderOutputAgreement(unittest.TestCase):
    """Call param, declaration, registry and the produced file name one path.

    Each case builds a plan with PlanBuilder and runs it through a real Engine,
    so "the file the step actually produces" is observed, not assumed.
    """

    def setUp(self):
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.root = Path(temp_dir.name)
        self.base = self.root / "base"
        self.base.mkdir()
        self.received: list[Path] = []
        self.fn = make_writer(self.received)
        self.engine = Engine(
            work_root=self.root / "work", emitter=Mock(spec=EventEmitter)
        )

    def build(
        self, base: Path, params: dict[str, Any], **kwargs: Any
    ) -> tuple[PlanBuilder, StepSpec]:
        builder = PlanBuilder(
            plan_id="agree", realm="test", scope={"kind": "project"}, base=base
        )
        spec = builder.add_step_fn(self.fn, step_id="producer", params=params, **kwargs)
        return builder, spec

    def run_plan(self, builder: PlanBuilder) -> None:
        with patch("yggdrasil.core.engine.resolve_callable", return_value=self.fn):
            self.engine.run(builder.to_plan())

    def assert_agreement(
        self, builder: PlanBuilder, spec: StepSpec, expected: Path
    ) -> None:
        self.assertTrue(expected.is_absolute())
        self.assertEqual(Path(spec.params["result"]), expected)
        self.assertEqual(spec.outputs, {"result": str(expected)})
        self.assertEqual(builder.path_for("result"), str(expected))
        self.run_plan(builder)
        self.assertEqual(self.received, [expected])
        self.assertEqual(expected.read_text(encoding="utf-8"), "done")

    def test_no_override_declares_the_annotated_path(self):
        builder, spec = self.build(self.base, {})

        self.assert_agreement(builder, spec, self.base / "results" / "out.json")

    def test_absolute_override_is_declared_instead_of_the_annotated_path(self):
        custom = self.root / "elsewhere" / "custom.json"
        params = {"result": str(custom)}

        builder, spec = self.build(self.base, params)

        self.assert_agreement(builder, spec, custom)
        self.assertFalse((self.base / "results" / "out.json").exists())
        self.assertEqual(params, {"result": str(custom)}, "caller params mutated")

    def test_relative_override_resolves_against_the_builder_base(self):
        builder, spec = self.build(self.base, {"result": "custom/out.json"})

        self.assertIsInstance(spec.params["result"], str)
        self.assert_agreement(builder, spec, self.base / "custom" / "out.json")

    def test_relative_builder_base_never_leaks_a_relative_path(self):
        """Built in one working directory and executed from another."""
        self.addCleanup(os.chdir, os.getcwd())
        for name, params, tail in (
            ("no override", {}, ("results", "out.json")),
            (
                "relative override",
                {"result": "custom/out.json"},
                ("custom", "out.json"),
            ),
        ):
            with self.subTest(name):
                self.received.clear()
                os.chdir(self.root)
                builder, spec = self.build(Path("rel_base"), params)
                os.chdir(self.base)

                self.assert_agreement(
                    builder, spec, self.root.joinpath("rel_base", *tail)
                )
                self.assertFalse(self.base.joinpath("rel_base").exists())

    def test_declared_builder_output_gates_reuse(self):
        builder, spec = self.build(self.base, {})
        produced = self.base / "results" / "out.json"
        self.run_plan(builder)

        self.run_plan(builder)
        self.assertEqual(len(self.received), 1, "a present output is reused")

        produced.unlink()
        self.run_plan(builder)
        self.assertEqual(len(self.received), 2, "a missing output is produced again")
        self.assertTrue(produced.exists())


class TestBuilderOutputsWithInjectionDisabled(unittest.TestCase):
    """Nothing is declared from an annotation the step never receives."""

    def setUp(self):
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.root = Path(temp_dir.name)
        self.base = self.root / "base"
        self.received: list[Path] = []
        self.builder = PlanBuilder(
            plan_id="noinject", realm="test", scope={"kind": "project"}, base=self.base
        )

    def test_absolute_concrete_default_is_declared_not_the_annotation(self):
        default = self.root / "concrete" / "default.json"
        fn = make_writer(self.received, default=default, with_default=True)
        for mode in ("none", "inputs"):
            with self.subTest(inject_io=mode):
                self.received.clear()
                builder = PlanBuilder(
                    plan_id=f"noinject_{mode}", realm="test", scope={}, base=self.base
                )

                spec = builder.add_step_fn(
                    fn, step_id="producer", params={}, inject_io=mode
                )

                self.assertNotIn("result", spec.params)
                self.assertEqual(spec.outputs, {"result": str(default)})
                self.assertEqual(builder.path_for("result"), str(default))
                engine = Engine(
                    work_root=self.root / "work", emitter=Mock(spec=EventEmitter)
                )
                with patch("yggdrasil.core.engine.resolve_callable", return_value=fn):
                    engine.run(builder.to_plan())
                self.assertEqual(self.received, [default])
                self.assertFalse((self.base / "results" / "out.json").exists())

    def test_output_injection_mode_alone_still_injects_the_annotation(self):
        fn = make_writer(self.received)

        spec = self.builder.add_step_fn(
            fn, step_id="producer", params={}, inject_io="outputs"
        )

        expected = self.base / "results" / "out.json"
        self.assertEqual(spec.params["result"], expected)
        self.assertEqual(spec.outputs, {"result": str(expected)})

    def test_no_default_requires_an_explicit_path(self):
        fn = make_writer(self.received)

        with self.assertRaises(ValueError) as cm:
            self.builder.add_step_fn(fn, step_id="producer", inject_io="none")

        self.assertIn("no default", str(cm.exception))
        self.assertEqual(self.builder.steps, [])

    def test_relative_default_requires_an_explicit_path(self):
        fn = make_writer(self.received, default="out.json", with_default=True)

        with self.assertRaises(ValueError) as cm:
            self.builder.add_step_fn(fn, step_id="producer", inject_io="none")

        self.assertIn("relative", str(cm.exception))

    def test_non_path_default_requires_an_explicit_path(self):
        fn = make_writer(self.received, default=None, with_default=True)

        with self.assertRaises(ValueError) as cm:
            self.builder.add_step_fn(fn, step_id="producer", inject_io="none")

        self.assertIn("not a path", str(cm.exception))

    def test_explicit_path_is_normalized_and_declared(self):
        fn = make_writer(self.received)

        spec = self.builder.add_step_fn(
            fn, step_id="producer", params={"result": "mine.json"}, inject_io="none"
        )

        expected = str(self.base / "mine.json")
        self.assertEqual(spec.params["result"], expected)
        self.assertEqual(spec.outputs, {"result": expected})


class TestBuilderRejectsNonPathOutputOverrides(unittest.TestCase):
    """An output override must name a path that can be declared."""

    def setUp(self):
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.builder = PlanBuilder(
            plan_id="p1", realm="test", scope={}, base=Path(temp_dir.name)
        )
        self.fn = make_writer([])

    def test_non_path_override_is_rejected(self):
        for value in (None, 42):
            with self.subTest(value=value):
                with self.assertRaises(TypeError):
                    self.builder.add_step_fn(
                        self.fn, step_id="producer", params={"result": value}
                    )

    def test_empty_override_is_rejected(self):
        with self.assertRaises(ValueError):
            self.builder.add_step_fn(self.fn, step_id="producer", params={"result": ""})

        self.assertEqual(self.builder.steps, [])


if __name__ == "__main__":
    unittest.main()
