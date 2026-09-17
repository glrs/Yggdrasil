"""Tests for artifact-aware reuse and the success-marker lifecycle.

These drive real ``@step`` functions through ``Engine.run`` and
``Engine._run_attempt``. Each test states a reuse guarantee and would fail if
that guarantee were removed:

- reuse needs a matching fingerprint *and* every declared output; a deleted
  output reruns its producer, and a step declaring no outputs reuses, and
  fingerprints, exactly as before outputs could be declared;
- relative declarations belong to the producing step's own work directory;
- a step that returns without its required outputs has failed, and is reported
  once, as failed;
- a step that is not reused never leaves an earlier success reusable, however
  its execution ends, and a new marker is replaced atomically;
- failing to check an output, or to invalidate or replace a marker, aborts the
  attempt instead of draining as an ordinary failure;
- an old marker never lets a blocked descendant run or be reused.

By default every scripted step writes each of its required outputs, so a test
states only how a step misbehaves.
"""

import errno
import hashlib
import json
import logging
import os
import stat
import unittest
from collections.abc import Callable
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import Mock, patch

from yggdrasil.core.engine import SUCCESS_MARKER, Engine, _default_fingerprint
from yggdrasil.core.engine import _replace_marker as replace_marker
from yggdrasil.flow.attempt import AttemptContext
from yggdrasil.flow.errors import OrchestrationError, PermanentStepError
from yggdrasil.flow.events.emitter import EventEmitter
from yggdrasil.flow.model import (
    CONTINUE_INDEPENDENT_POLICY,
    FAIL_FAST_POLICY,
    Plan,
    StepResult,
    StepSpec,
)
from yggdrasil.flow.outcomes import AttemptReport, StepOutcome, TerminationReason
from yggdrasil.flow.outputs import MISSING_REQUIRED_OUTPUTS_CODE
from yggdrasil.flow.step import StepContext, step

CONTINUE = CONTINUE_INDEPENDENT_POLICY
FAIL_FAST = FAIL_FAST_POLICY
POLICIES = (FAIL_FAST, CONTINUE)

# Every step resolves to the same scripted callable; the reference is nominal.
STEP_REF = "tests.reuse:scripted"


class RecordingEmitter(EventEmitter):
    """Emitter that records every event."""

    def __init__(self) -> None:
        self.events: list[dict] = []

    def emit(self, event: dict) -> None:
        self.events.append(event)

    def types(self) -> list[str]:
        """Event types recorded so far, in emission order."""
        return [str(e.get("type", "")) for e in self.events]

    def step_types(self, step_id: str) -> list[str]:
        """Event types recorded for one step, in emission order."""
        return [str(e["type"]) for e in self.events if e.get("step_id") == step_id]


def write_required_outputs(ctx: StepContext) -> None:
    """What a well-behaved producer does: write every required output."""
    for path in ctx.required_outputs.values():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"produced by {ctx.step_id}", encoding="utf-8")


class ScriptedSteps:
    """One @step callable whose behavior is scripted per step ID.

    Unscripted steps write their required outputs. Every invocation's step ID and
    required outputs are recorded.
    """

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.required: dict[str, dict[str, Path]] = {}
        self.behaviors: dict[str, Callable[[StepContext], None]] = {}
        calls, required, behaviors = self.calls, self.required, self.behaviors

        @step
        def scripted(ctx: StepContext, **kwargs: Any) -> StepResult:
            calls.append(ctx.step_id)
            required[ctx.step_id] = dict(ctx.required_outputs)
            behaviors.get(ctx.step_id, write_required_outputs)(ctx)
            return StepResult()

        self.fn = scripted

    def fail(self, step_id: str, exc: BaseException) -> None:
        """Make a step raise ``exc`` when invoked, without writing anything."""

        def raise_(ctx: StepContext) -> None:
            raise exc

        self.behaviors[step_id] = raise_


def spec(
    step_id: str, *deps: str, outputs: dict[str, str] | None = None, **params: Any
) -> StepSpec:
    """A scripted step with the given dependencies, declared outputs and params."""
    return StepSpec(
        step_id=step_id,
        name=step_id,
        fn_ref=STEP_REF,
        params=dict(params),
        deps=list(deps),
        outputs=dict(outputs or {}),
    )


def legacy_fingerprint(params: dict[str, Any]) -> str:
    """The fingerprint the engine computed before outputs could be declared.

    Written out independently of the engine for a step with no declared inputs,
    so a change to the hashed payload cannot pass unnoticed.
    """
    payload = json.dumps({"params": params}, sort_keys=True, default=str).encode()
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


class ReuseTestCase(unittest.TestCase):
    """Shared fixture: an engine on a temp work root with scripted steps."""

    PLAN_ID = "reuse_plan"

    def setUp(self):
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.root = Path(temp_dir.name)
        self.work_root = self.root / "work"
        self.emitter = RecordingEmitter()
        self.logger = Mock(spec=logging.Logger)
        self.engine = Engine(
            work_root=self.work_root, emitter=self.emitter, logger=self.logger
        )
        self.steps = ScriptedSteps()
        resolver = patch(
            "yggdrasil.core.engine.resolve_callable", return_value=self.steps.fn
        )
        resolver.start()
        self.addCleanup(resolver.stop)

    def plan(self, *specs: StepSpec, policy: str = FAIL_FAST) -> Plan:
        return Plan(
            plan_id=self.PLAN_ID,
            realm="test",
            scope={"kind": "project", "id": "P1"},
            steps=list(specs),
            failure_policy=policy,
        )

    def run_attempt(self, plan: Plan) -> tuple[AttemptReport, BaseException | None]:
        """Run one attempt through _run_attempt, capturing how it ended."""
        context = AttemptContext.for_plan(plan, execution_id="exec_reuse")
        try:
            self.engine._run_attempt(plan, context=context)
        except Exception as exc:
            return context.report, exc
        return context.report, None

    def reset_observations(self) -> None:
        """Forget calls, events and log calls from earlier attempts."""
        self.steps.calls.clear()
        self.emitter.events.clear()
        self.logger.reset_mock()

    def step_dir(self, step_id: str) -> Path:
        return self.work_root / self.PLAN_ID / step_id

    def marker(self, step_id: str) -> Path:
        return self.step_dir(step_id) / SUCCESS_MARKER

    def assert_aborted_as_infrastructure(
        self, report: AttemptReport, exc: BaseException | None, fragment: str
    ) -> None:
        """Assert the attempt aborted instead of draining an ordinary failure."""
        self.assertIsInstance(exc, OrchestrationError)
        self.assertIn(fragment, str(exc))
        self.assertIs(report.termination_reason, TerminationReason.ORCHESTRATION_ERROR)
        self.assertNotIn(StepOutcome.FAILED, report.step_outcomes.values())
        self.assertNotIn("step.failed", self.emitter.types())


class TestFingerprintCompatibility(ReuseTestCase):
    """Declaring no outputs changes nothing, down to the fingerprint's bytes."""

    def test_fingerprint_of_a_step_without_outputs_is_unchanged(self):
        params = {"lane": 2, "flowcell": "A22FN2TLT3"}

        self.assertEqual(
            _default_fingerprint(spec("s", **params), self.steps.fn),
            legacy_fingerprint(params),
        )

    def test_marker_written_before_outputs_existed_is_still_reused(self):
        params = {"lane": 2}
        self.marker("s").parent.mkdir(parents=True)
        self.marker("s").write_text(legacy_fingerprint(params))

        report, exc = self.run_attempt(self.plan(spec("s", **params)))

        self.assertIsNone(exc)
        self.assertEqual(self.steps.calls, [])
        self.assertIs(report.step_outcomes["s"], StepOutcome.REUSED)

    def test_declared_outputs_are_part_of_the_fingerprint(self):
        fn = self.steps.fn
        without = _default_fingerprint(spec("s", k="v"), fn)
        declared = _default_fingerprint(spec("s", outputs={"a": "a.txt"}, k="v"), fn)
        changed = _default_fingerprint(spec("s", outputs={"a": "b.txt"}, k="v"), fn)

        self.assertEqual(len({without, declared, changed}), 3)
        self.assertEqual(
            _default_fingerprint(spec("s", outputs={"a": "a.txt"}, k="v"), fn),
            declared,
        )

    def test_changing_a_declaration_invalidates_only_that_step(self):
        self.engine.run(
            self.plan(
                spec("a", outputs={"x": "x.txt"}), spec("b", outputs={"y": "y.txt"})
            )
        )
        self.reset_observations()

        report, _ = self.run_attempt(
            self.plan(
                spec("a", outputs={"x": "x.txt", "extra": "extra.txt"}),
                spec("b", outputs={"y": "y.txt"}),
            )
        )

        self.assertEqual(self.steps.calls, ["a"])
        self.assertIs(report.step_outcomes["b"], StepOutcome.REUSED)


class TestReuseRequiresDeclaredOutputs(ReuseTestCase):
    """Criterion #5: valid reuse satisfies dependencies; a deleted output reruns."""

    def producer_consumer(self, policy: str) -> Plan:
        return self.plan(
            spec("producer", outputs={"data": "data.csv"}),
            spec("consumer", "producer", outputs={"summary": "summary.txt"}),
            policy=policy,
        )

    def test_present_outputs_allow_reuse_that_satisfies_dependencies(self):
        for policy in POLICIES:
            with self.subTest(policy=policy):
                self.engine.run(self.producer_consumer(policy))
                self.reset_observations()

                report, exc = self.run_attempt(self.producer_consumer(policy))

                self.assertIsNone(exc)
                self.assertEqual(self.steps.calls, [])
                self.assertEqual(
                    report.step_outcomes,
                    {"producer": StepOutcome.REUSED, "consumer": StepOutcome.REUSED},
                )

    def test_deleting_a_declared_output_reruns_its_producer(self):
        for policy in POLICIES:
            with self.subTest(policy=policy):
                self.engine.run(self.producer_consumer(policy))
                data = self.step_dir("producer") / "data.csv"
                data.unlink()
                self.reset_observations()

                report, exc = self.run_attempt(self.producer_consumer(policy))

                self.assertIsNone(exc)
                self.assertEqual(self.steps.calls, ["producer"])
                self.assertEqual(
                    report.step_outcomes,
                    {"producer": StepOutcome.SUCCEEDED, "consumer": StepOutcome.REUSED},
                )
                self.assertNotIn("step.skipped", self.emitter.step_types("producer"))
                self.assertTrue(data.exists())
                # The refused reuse is explained, naming the missing output.
                [warning] = self.logger.warning.call_args_list
                self.assertIn("Not reusing", warning.args[0])
                self.assertEqual(warning.args[1:3], ("producer", self.PLAN_ID))
                self.assertIn(f"data={data}", warning.args[3])

    def test_a_directory_output_is_checked_for_existence_only(self):
        def make_directory(ctx: StepContext) -> None:
            ctx.required_outputs["results"].mkdir()

        self.steps.behaviors["s"] = make_directory
        plan = self.plan(spec("s", outputs={"results": "results"}))
        self.engine.run(plan)
        self.reset_observations()

        report, _ = self.run_attempt(plan)

        self.assertIs(report.step_outcomes["s"], StepOutcome.REUSED)


class TestDeclaredOutputPathResolution(ReuseTestCase):
    """Manually constructed specs: absolute as-is, otherwise the step's own workdir."""

    def test_relative_declaration_belongs_to_the_producing_step_workdir(self):
        name = "extra_config_demultiplex.config"
        plan = self.plan(spec("materialize_config", outputs={"config": name}))
        self.engine.run(plan)
        own = self.step_dir("materialize_config") / name
        self.assertEqual(self.steps.required["materialize_config"], {"config": own})
        self.assertTrue(own.exists())

        # Same-named files anywhere else must not satisfy the declaration.
        own.unlink()
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(self.root)
        for decoy in (
            self.root / name,
            self.work_root / name,
            self.work_root / self.PLAN_ID / name,
            self.step_dir("another_step") / name,
        ):
            decoy.parent.mkdir(parents=True, exist_ok=True)
            decoy.write_text("decoy", encoding="utf-8")
        self.reset_observations()

        report, _ = self.run_attempt(plan)

        self.assertEqual(self.steps.calls, ["materialize_config"])
        self.assertIs(report.step_outcomes["materialize_config"], StepOutcome.SUCCEEDED)
        self.assertEqual(plan.steps[0].outputs, {"config": name}, "spec rewritten")

    def test_same_relative_filename_in_two_steps_is_checked_per_step(self):
        declared = {"config": "settings.config"}
        self.steps.behaviors["lane_2"] = lambda ctx: None  # forgets its output
        plan = self.plan(
            spec("lane_1", outputs=declared),
            spec("lane_2", outputs=declared),
            policy=CONTINUE,
        )

        report, exc = self.run_attempt(plan)

        self.assertIsNone(exc)
        self.assertIs(report.step_outcomes["lane_1"], StepOutcome.SUCCEEDED)
        self.assertIs(report.step_outcomes["lane_2"], StepOutcome.FAILED)
        self.assertIn(
            str(self.step_dir("lane_2") / "settings.config"),
            report.failures["lane_2"].error,
        )

    def test_absolute_declaration_is_used_as_is(self):
        report_path = self.root / "shared" / "report.html"
        plan = self.plan(spec("publish", outputs={"report": str(report_path)}))
        self.engine.run(plan)
        self.assertEqual(self.steps.required["publish"], {"report": report_path})
        self.assertTrue(report_path.exists())
        self.reset_observations()

        reused, _ = self.run_attempt(plan)
        self.assertIs(reused.step_outcomes["publish"], StepOutcome.REUSED)

        report_path.unlink()
        self.reset_observations()
        rerun, _ = self.run_attempt(plan)
        self.assertEqual(self.steps.calls, ["publish"])
        self.assertIs(rerun.step_outcomes["publish"], StepOutcome.SUCCEEDED)


class TestMissingRequiredOutputIsAStepFailure(ReuseTestCase):
    """Criterion #17: one failed terminal event, no success event, no marker."""

    def forgetful_plan(self, policy: str) -> Plan:
        self.steps.behaviors["producer"] = lambda ctx: None
        return self.plan(
            spec("producer", outputs={"data": "data.csv"}),
            spec("consumer", "producer"),
            spec("other"),
            policy=policy,
        )

    def test_contained_under_continuation_like_any_step_failure(self):
        report, exc = self.run_attempt(self.forgetful_plan(CONTINUE))

        self.assertIsNone(exc)
        self.assertIs(report.termination_reason, TerminationReason.COMPLETED)
        self.assertEqual(
            report.step_outcomes,
            {
                "producer": StepOutcome.FAILED,
                "consumer": StepOutcome.BLOCKED,
                "other": StepOutcome.SUCCEEDED,
            },
        )
        failure = report.failures["producer"]
        self.assertEqual(failure.code, MISSING_REQUIRED_OUTPUTS_CODE)
        self.assertEqual(failure.error_type, "PermanentStepError")
        self.assertEqual(
            self.emitter.step_types("producer"), ["step.started", "step.failed"]
        )
        self.assertEqual(self.steps.calls, ["producer", "other"])
        self.assertFalse(self.marker("producer").exists())

    def test_raised_under_fail_fast(self):
        with self.assertRaises(PermanentStepError) as cm:
            self.engine.run(self.forgetful_plan(FAIL_FAST))

        self.assertEqual(cm.exception.code, MISSING_REQUIRED_OUTPUTS_CODE)
        self.assertEqual(self.steps.calls, ["producer"])
        self.assertEqual(
            self.emitter.step_types("producer"), ["step.started", "step.failed"]
        )
        self.assertFalse(self.marker("producer").exists())

    def test_a_previous_success_does_not_survive_a_missing_output(self):
        plan = self.plan(spec("producer", outputs={"data": "data.csv"}))
        self.engine.run(plan)
        (self.step_dir("producer") / "data.csv").unlink()
        self.steps.behaviors["producer"] = lambda ctx: None

        with self.assertRaises(PermanentStepError):
            self.engine.run(plan)

        self.assertFalse(self.marker("producer").exists())


class TestSuccessMarkerLifecycle(ReuseTestCase):
    """Invalidate before re-execution; replace atomically after success."""

    def test_failed_rerun_leaves_no_reusable_success_marker(self):
        """Criterion #5, over outputs the failed rerun partly replaced."""
        plan = self.plan(spec("producer", outputs={"data": "data.csv"}))
        self.engine.run(plan)
        data = self.step_dir("producer") / "data.csv"
        data.unlink()

        def partial_then_fail(ctx: StepContext) -> None:
            data.write_text("half-written", encoding="utf-8")
            raise PermanentStepError("demultiplexing crashed")

        self.steps.behaviors["producer"] = partial_then_fail
        with self.assertRaises(PermanentStepError):
            self.engine.run(plan)
        self.assertFalse(self.marker("producer").exists())
        self.assertTrue(data.exists())

        # The partial output is present and the fingerprint is unchanged, so
        # only the marker's absence stops this from being reused.
        del self.steps.behaviors["producer"]
        self.reset_observations()
        report, _ = self.run_attempt(plan)

        self.assertEqual(self.steps.calls, ["producer"])
        self.assertIs(report.step_outcomes["producer"], StepOutcome.SUCCEEDED)

    def test_interrupted_rerun_leaves_no_reusable_success_marker(self):
        """Invalidation precedes the call; it is not failure handling after it."""
        plan = self.plan(spec("producer", outputs={"data": "data.csv"}))
        self.engine.run(plan)
        data = self.step_dir("producer") / "data.csv"
        data.unlink()

        def partial_then_interrupted(ctx: StepContext) -> None:
            data.write_text("half-written", encoding="utf-8")
            raise KeyboardInterrupt

        self.steps.behaviors["producer"] = partial_then_interrupted
        with self.assertRaises(KeyboardInterrupt):
            self.engine.run(plan)

        self.assertTrue(data.exists())
        self.assertFalse(self.marker("producer").exists())

    def test_marker_is_invalidated_for_steps_without_declared_outputs_too(self):
        """A reverted change must not reuse a success its failed rerun overwrote."""
        self.engine.run(self.plan(spec("s", version=1)))
        self.steps.fail("s", PermanentStepError("version 2 broke"))
        with self.assertRaises(PermanentStepError):
            self.engine.run(self.plan(spec("s", version=2)))
        self.assertFalse(self.marker("s").exists())
        del self.steps.behaviors["s"]
        self.reset_observations()

        report, _ = self.run_attempt(self.plan(spec("s", version=1)))

        self.assertEqual(self.steps.calls, ["s"])
        self.assertIs(report.step_outcomes["s"], StepOutcome.SUCCEEDED)

    def test_successful_rerun_replaces_the_marker_without_leftovers(self):
        plan_v1 = self.plan(spec("s", outputs={"data": "data.csv"}, version=1))
        plan_v2 = self.plan(spec("s", outputs={"data": "data.csv"}, version=2))
        self.engine.run(plan_v1)

        self.engine.run(plan_v2)

        self.assertEqual(
            self.marker("s").read_text(encoding="utf-8"),
            _default_fingerprint(plan_v2.steps[0], self.steps.fn),
        )
        self.assertEqual(
            sorted(p.name for p in self.step_dir("s").iterdir()),
            ["data.csv", SUCCESS_MARKER],
        )

    def test_failed_marker_replacement_after_a_rerun_leaves_no_marker_at_all(self):
        plan = self.plan(spec("s", outputs={"data": "data.csv"}), policy=CONTINUE)
        self.engine.run(plan)
        (self.step_dir("s") / "data.csv").unlink()
        self.reset_observations()
        original_replace = os.replace

        def failing_replace(src, dst, *args, **kwargs):
            if Path(dst).name == SUCCESS_MARKER:
                raise OSError(errno.ENOSPC, "No space left on device")
            return original_replace(src, dst, *args, **kwargs)

        with patch.object(os, "replace", failing_replace):
            report, exc = self.run_attempt(plan)

        self.assert_aborted_as_infrastructure(report, exc, "Writing the cache marker")
        # The accepted window: success was published before the marker failed.
        self.assertIn("step.succeeded", self.emitter.step_types("s"))
        self.assertEqual(
            sorted(p.name for p in self.step_dir("s").iterdir()), ["data.csv"]
        )


class TestReplaceMarker(unittest.TestCase):
    """The marker is swapped in whole, or not at all."""

    def setUp(self):
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.dir = Path(temp_dir.name)
        self.marker = self.dir / SUCCESS_MARKER
        self.marker.write_text("sha256:old", encoding="utf-8")

    def leftovers(self) -> list[str]:
        return sorted(p.name for p in self.dir.iterdir() if p != self.marker)

    def test_new_content_is_complete_before_it_replaces_the_old_marker(self):
        observed: list[tuple[str, str]] = []
        original_replace = os.replace

        def observing_replace(src, dst, *args, **kwargs):
            observed.append(
                (
                    Path(src).read_text(encoding="utf-8"),
                    Path(dst).read_text(encoding="utf-8"),
                )
            )
            return original_replace(src, dst, *args, **kwargs)

        with patch.object(os, "replace", observing_replace):
            replace_marker(self.marker, "sha256:new")

        self.assertEqual(observed, [("sha256:new", "sha256:old")])
        self.assertEqual(self.marker.read_text(encoding="utf-8"), "sha256:new")
        self.assertEqual(self.leftovers(), [])

    def test_failed_rename_keeps_the_previous_marker_and_cleans_up(self):
        with patch.object(os, "replace", side_effect=OSError(errno.EXDEV, "cross")):
            with self.assertRaises(OSError):
                replace_marker(self.marker, "sha256:new")

        self.assertEqual(self.marker.read_text(encoding="utf-8"), "sha256:old")
        self.assertEqual(self.leftovers(), [])

    def test_failed_temporary_write_keeps_the_previous_marker_and_cleans_up(self):
        real_open = open

        def full_disk_open(file, mode="r", *args, **kwargs):
            handle = real_open(file, mode, *args, **kwargs)
            handle.write = Mock(side_effect=OSError(errno.ENOSPC, "No space left"))
            return handle

        with patch("yggdrasil.core.engine.open", full_disk_open, create=True):
            with self.assertRaises(OSError):
                replace_marker(self.marker, "sha256:new")

        self.assertEqual(self.marker.read_text(encoding="utf-8"), "sha256:old")
        self.assertEqual(self.leftovers(), [])

    def test_marker_keeps_the_permissions_a_plain_write_gives_it(self):
        plain = self.dir / "plain"
        plain.write_text("x", encoding="utf-8")

        replace_marker(self.marker, "sha256:new")

        self.assertEqual(
            stat.S_IMODE(self.marker.stat().st_mode), stat.S_IMODE(plain.stat().st_mode)
        )


class TestReuseInfrastructureFailuresAbortTheAttempt(ReuseTestCase):
    """Criterion #17 at the reuse and marker boundaries, under continuation.

    Each injects the fault at ``x``, with an independent ``y`` that would
    otherwise run next.
    """

    def plan_xy(self) -> Plan:
        return self.plan(
            spec("x", outputs={"data": "data.csv"}), spec("y"), policy=CONTINUE
        )

    def deny_stat_of(self, denied: Path) -> Any:
        """Patch Path.stat to fail with permission denied for one path."""
        original_stat = Path.stat

        def stat_(path, *args, **kwargs):
            if path == denied:
                raise PermissionError(errno.EACCES, "Permission denied", str(path))
            return original_stat(path, *args, **kwargs)

        return patch.object(Path, "stat", stat_)

    def test_failing_output_check_during_reuse(self):
        self.engine.run(self.plan_xy())
        self.reset_observations()

        with self.deny_stat_of(self.step_dir("x") / "data.csv"):
            report, exc = self.run_attempt(self.plan_xy())

        self.assert_aborted_as_infrastructure(
            report, exc, "Could not check required output"
        )
        self.assertEqual(self.steps.calls, [])
        self.assertIn("y", report.unreached_step_ids)
        # Nothing was decided about x, so its marker is left alone.
        self.assertTrue(self.marker("x").exists())

    def test_failing_output_check_after_execution(self):
        with self.deny_stat_of(self.step_dir("x") / "data.csv"):
            report, exc = self.run_attempt(self.plan_xy())

        self.assert_aborted_as_infrastructure(
            report, exc, "Could not check required output"
        )
        self.assertEqual(self.steps.calls, ["x"])
        self.assertEqual(self.emitter.step_types("x"), ["step.started"])
        self.assertIn("y", report.unreached_step_ids)
        self.assertFalse(self.marker("x").exists())

    def test_failing_marker_invalidation(self):
        original_unlink = Path.unlink

        def failing_unlink(path, *args, **kwargs):
            if path.name == SUCCESS_MARKER:
                raise PermissionError(errno.EACCES, "Permission denied", str(path))
            return original_unlink(path, *args, **kwargs)

        with patch.object(Path, "unlink", failing_unlink):
            report, exc = self.run_attempt(self.plan_xy())

        self.assert_aborted_as_infrastructure(
            report, exc, "Invalidating the cache marker"
        )
        self.assertEqual(self.steps.calls, [], "x ran over a marker left in place")
        self.assertIn("y", report.unreached_step_ids)


class TestBlockedDescendantIgnoresItsMarker(ReuseTestCase):
    """Criterion #4, with every condition for reuse otherwise met."""

    def test_matching_marker_and_present_outputs_neither_run_nor_reuse(self):
        def lane(version: int) -> Plan:
            return self.plan(
                spec("demux", outputs={"fastq": "lane.fastq"}, version=version),
                spec("upload", "demux", outputs={"receipt": "receipt.json"}),
                policy=CONTINUE,
            )

        self.engine.run(lane(1))
        upload_marker = self.marker("upload").read_text(encoding="utf-8")
        receipt = self.step_dir("upload") / "receipt.json"
        receipt_content = receipt.read_text(encoding="utf-8")
        self.reset_observations()
        self.steps.fail("demux", PermanentStepError("lane 2 demultiplexing failed"))

        report, exc = self.run_attempt(lane(2))

        self.assertIsNone(exc)
        self.assertEqual(self.steps.calls, ["demux"])
        self.assertIs(report.step_outcomes["upload"], StepOutcome.BLOCKED)
        self.assertEqual(self.emitter.step_types("upload"), [])
        # Never evaluated, so never invalidated either.
        self.assertEqual(
            self.marker("upload").read_text(encoding="utf-8"), upload_marker
        )
        self.assertEqual(receipt.read_text(encoding="utf-8"), receipt_content)


if __name__ == "__main__":
    unittest.main()
