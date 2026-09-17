"""Tests for the engine's exception classification boundaries.

Exception *location* alone cannot classify a failure: the decorated step function
also publishes events, and author calls such as ``ctx.record_artifact()`` reach
into infrastructure. These tests inject a failure at each call site the
classification table names and assert which side of the line it lands on.

The rule being protected: an infrastructure or reporting failure is never
silently recorded as a branch's ordinary domain failure. It aborts the attempt
instead. A realm's own file error is *not* systemic merely because it is an
OSError.
"""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from yggdrasil.core.engine import Engine, _orchestration_boundary
from yggdrasil.flow.errors import (
    EventPublicationError,
    OrchestrationError,
    PermanentStepError,
    TransientStepError,
)
from yggdrasil.flow.events.emitter import EventEmitter
from yggdrasil.flow.model import Plan, StepResult, StepSpec
from yggdrasil.flow.step import StepContext, step


class RecordingEmitter(EventEmitter):
    """Emitter that records events and can fail on chosen event types."""

    def __init__(self, fail_on: set[str] | None = None):
        self.events: list[dict] = []
        self.fail_on = fail_on or set()

    def emit(self, event: dict) -> None:
        self.events.append(event)
        if event.get("type") in self.fail_on:
            raise OSError(f"event spool unavailable for {event.get('type')}")

    def types(self) -> list[str]:
        """Event types recorded so far, in emission order."""
        return [str(e.get("type", "")) for e in self.events]


class _Ref:
    """Minimal artifact ref satisfying ArtifactRefProtocol."""

    def __init__(self, key: str, path: Path):
        self._key = key
        self._path = path

    def key(self) -> str:
        return self._key

    def resolve_path(self, scope_dir: Path) -> Path:
        return self._path


@step
def ok_step(ctx: StepContext, **kwargs) -> StepResult:
    """A step that always succeeds."""
    return StepResult()


@step
def transient_step(ctx: StepContext, **kwargs) -> StepResult:
    """A step that fails transiently, to reach the retry-unimplemented path."""
    raise TransientStepError("temporary glitch")


class ClassificationTestCase(unittest.TestCase):
    """Shared fixture for classification tests."""

    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.work_root = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def plan(self, *, step_id: str = "s1") -> Plan:
        return Plan(
            plan_id="classify_plan",
            realm="test",
            scope={"kind": "project", "id": "P1"},
            steps=[StepSpec(step_id=step_id, name="n1", fn_ref="m:f", params={})],
        )

    def run_plan(self, fn, emitter, plan=None):
        """Run a single-step plan with fn resolved for every step."""
        engine = Engine(work_root=self.work_root, emitter=emitter)
        with patch("yggdrasil.core.engine.resolve_callable", return_value=fn):
            engine.run(plan or self.plan())
        return engine


class TestStepContextEmitClassification(ClassificationTestCase):
    """Row: StepContext.emit() - covers every ctx.emit(...) call."""

    def test_failing_step_started_emit_aborts_as_orchestration_error(self):
        emitter = RecordingEmitter(fail_on={"step.started"})

        with self.assertRaises(OrchestrationError) as cm:
            self.run_plan(ok_step, emitter)

        self.assertIn("Event publication failed", str(cm.exception))
        self.assertIn("step.started", str(cm.exception))

    def test_failing_step_succeeded_emit_is_not_reported_as_step_failure(self):
        """The wrapper must not report through a channel it knows is broken."""
        emitter = RecordingEmitter(fail_on={"step.succeeded"})

        with self.assertRaises(OrchestrationError):
            self.run_plan(ok_step, emitter)

        self.assertNotIn("step.failed", emitter.types())

    def test_orchestration_error_is_not_a_step_error(self):
        """Ordering, not an isinstance hierarchy, is what has to be right."""
        emitter = RecordingEmitter(fail_on={"step.started"})

        with self.assertRaises(OrchestrationError) as cm:
            self.run_plan(ok_step, emitter)

        self.assertNotIsInstance(cm.exception, PermanentStepError)
        self.assertNotIsInstance(cm.exception, ValueError)

    def test_failing_emit_leaves_no_reusable_success_marker(self):
        emitter = RecordingEmitter(fail_on={"step.succeeded"})

        with self.assertRaises(OrchestrationError):
            self.run_plan(ok_step, emitter)

        marker = self.work_root / "classify_plan" / "s1" / "success.fingerprint"
        self.assertFalse(marker.exists())


class TestEngineDirectEmitClassification(ClassificationTestCase):
    """Row: Engine's direct self.emitter.emit() calls, which bypass ctx."""

    def test_failing_step_skipped_emit_aborts_as_orchestration_error(self):
        # First run populates the cache marker so the second run takes the
        # cache-hit branch, which emits step.skipped from the engine itself.
        self.run_plan(ok_step, RecordingEmitter())

        emitter = RecordingEmitter(fail_on={"step.skipped"})
        with self.assertRaises(OrchestrationError) as cm:
            self.run_plan(ok_step, emitter)

        self.assertIn("step.skipped", str(cm.exception))
        self.assertIn("Publishing", str(cm.exception))

    def test_failing_retry_unimplemented_emit_aborts_as_orchestration_error(self):
        emitter = RecordingEmitter(fail_on={"step.retry_unimplemented"})

        with self.assertRaises(OrchestrationError) as cm:
            self.run_plan(transient_step, emitter)

        self.assertIn("step.retry_unimplemented", str(cm.exception))
        # The step's own failure was published before infrastructure broke.
        self.assertIn("step.failed", emitter.types())

    def test_transient_step_still_converts_when_publication_works(self):
        """The existing transient -> permanent conversion is unchanged."""
        emitter = RecordingEmitter()

        with self.assertRaises(PermanentStepError) as cm:
            self.run_plan(transient_step, emitter)

        self.assertIn("Retry not implemented", str(cm.exception))
        self.assertIn("step.retry_unimplemented", emitter.types())


class TestEngineBookkeepingClassification(ClassificationTestCase):
    """Row: plan.json write, step-dir creation, cache-marker read and write."""

    def test_failing_plan_file_write_aborts_as_orchestration_error(self):
        # A work root nested inside a regular file cannot be created.
        blocker = self.work_root / "blocker"
        blocker.write_text("not a directory", encoding="utf-8")
        engine = Engine(work_root=blocker / "nested", emitter=RecordingEmitter())

        with patch("yggdrasil.core.engine.resolve_callable", return_value=ok_step):
            with self.assertRaises(OrchestrationError) as cm:
                engine.run(self.plan())

        self.assertIn("Writing plan.json", str(cm.exception))

    def test_failing_step_dir_creation_aborts_as_orchestration_error(self):
        # Occupy the step directory's path with a regular file.
        step_path = self.work_root / "classify_plan" / "s1"
        step_path.parent.mkdir(parents=True)
        step_path.write_text("in the way", encoding="utf-8")

        with self.assertRaises(OrchestrationError) as cm:
            self.run_plan(ok_step, RecordingEmitter())

        self.assertIn("work directory", str(cm.exception))
        self.assertIn("s1", str(cm.exception))

    def test_failing_cache_marker_read_aborts_as_orchestration_error(self):
        # A directory where the marker file belongs: exists() is True, but
        # reading it raises.
        marker = self.work_root / "classify_plan" / "s1" / "success.fingerprint"
        marker.mkdir(parents=True)

        with self.assertRaises(OrchestrationError) as cm:
            self.run_plan(ok_step, RecordingEmitter())

        self.assertIn("Reading the cache marker", str(cm.exception))

    def test_failing_cache_marker_write_aborts_and_leaves_no_marker(self):
        """Marker failure aborts even though step success was already published."""
        emitter = RecordingEmitter()
        original_write_text = Path.write_text

        def failing_write_text(self, *args, **kwargs):
            if self.name == "success.fingerprint":
                raise OSError("no space left on device")
            return original_write_text(self, *args, **kwargs)

        with patch.object(Path, "write_text", failing_write_text):
            with self.assertRaises(OrchestrationError) as cm:
                self.run_plan(ok_step, emitter)

        self.assertIn("Writing the cache marker", str(cm.exception))
        # The documented, accepted inconsistency window: success was published,
        # then the marker failed and the attempt aborted.
        self.assertIn("step.succeeded", emitter.types())
        marker = self.work_root / "classify_plan" / "s1" / "success.fingerprint"
        self.assertFalse(marker.exists())


class TestRealmDataFailuresStayOrdinary(ClassificationTestCase):
    """Row: record_artifact()'s hashing of an author-supplied path.

    The path is realm-controlled data, so a failure there is that step's
    ordinary failure - not automatically systemic merely because it is an
    OSError.
    """

    def test_record_artifact_hashing_failure_is_an_ordinary_step_failure(self):
        artifact_path = self.work_root / "artifact.txt"
        artifact_path.write_text("payload", encoding="utf-8")

        @step
        def artifact_step(ctx: StepContext, **kwargs) -> StepResult:
            ctx.record_artifact(_Ref("output", artifact_path))
            return StepResult()

        emitter = RecordingEmitter()

        with patch(
            "yggdrasil.flow.step.sha256_file",
            side_effect=OSError("permission denied reading realm artifact"),
        ):
            with self.assertRaises(OSError) as cm:
                self.run_plan(artifact_step, emitter)

        self.assertNotIsInstance(cm.exception, OrchestrationError)
        self.assertIn("permission denied", str(cm.exception))
        # It went through ordinary step-failure reporting.
        self.assertIn("step.failed", emitter.types())

    def test_author_step_failure_remains_a_step_error(self):
        @step
        def failing_step(ctx: StepContext, **kwargs) -> StepResult:
            raise PermanentStepError("the realm's own work failed")

        emitter = RecordingEmitter()

        with self.assertRaises(PermanentStepError) as cm:
            self.run_plan(failing_step, emitter)

        self.assertNotIsInstance(cm.exception, OrchestrationError)
        self.assertIn("step.failed", emitter.types())


class TestBoundaryClassificationRule(unittest.TestCase):
    """Each boundary keeps its own classification and nothing narrower.

    A bookkeeping boundary leaves an OrchestrationError from within as it is. A
    publication boundary must not: an emitter raising a plain OrchestrationError
    has still failed to publish, and the engine can only stop reporting through
    a broken emitter if it recognizes that failure as EventPublicationError.
    """

    def test_bookkeeping_boundary_wraps_ordinary_failures(self):
        with self.assertRaises(OrchestrationError) as cm:
            with _orchestration_boundary("Writing a marker"):
                raise OSError("disk full")

        self.assertNotIsInstance(cm.exception, EventPublicationError)
        self.assertIsInstance(cm.exception.__cause__, OSError)

    def test_bookkeeping_boundary_leaves_an_orchestration_error_unchanged(self):
        inner = OrchestrationError("already classified")

        with self.assertRaises(OrchestrationError) as cm:
            with _orchestration_boundary("Writing a marker"):
                raise inner

        self.assertIs(cm.exception, inner)

    def test_publication_boundary_reclassifies_a_plain_orchestration_error(self):
        inner = OrchestrationError("storage-backed emitter failed")

        with self.assertRaises(EventPublicationError) as cm:
            with _orchestration_boundary(
                "Publishing an event", error_type=EventPublicationError
            ):
                raise inner

        self.assertIs(cm.exception.__cause__, inner)
        self.assertIn("storage-backed emitter failed", str(cm.exception))

    def test_publication_boundary_leaves_a_publication_error_unchanged(self):
        inner = EventPublicationError("already classified")

        with self.assertRaises(EventPublicationError) as cm:
            with _orchestration_boundary(
                "Publishing an event", error_type=EventPublicationError
            ):
                raise inner

        self.assertIs(cm.exception, inner)


if __name__ == "__main__":
    unittest.main()
