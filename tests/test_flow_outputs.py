"""Tests for declared step outputs: validation, path resolution and presence.

The engine's reuse check and the @step wrapper's success check both rely on
these three definitions, so each is pinned here in isolation.
"""

import errno
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from yggdrasil.flow.errors import OrchestrationError
from yggdrasil.flow.outputs import (
    find_missing_outputs,
    resolve_declared_outputs,
    validate_output_declarations,
)


class TestValidateOutputDeclarations(unittest.TestCase):
    """Only a dict of nonempty strings survives serialization and hashing."""

    def test_accepts_empty_and_well_formed_declarations(self):
        validate_output_declarations({})
        validate_output_declarations(
            {"config": "extra_config_demultiplex.config", "report": "/abs/report.html"}
        )

    def test_rejects_malformed_declarations(self):
        cases = {
            "not a dict": ["config.txt"],
            "None": None,
            "non-string key": {1: "config.txt"},
            "empty key": {"": "config.txt"},
            "non-string path": {"config": Path("config.txt")},
            "None path": {"config": None},
            "empty path": {"config": ""},
            "NUL in path": {"config": "conf\0ig.txt"},
        }
        for name, outputs in cases.items():
            with self.subTest(name):
                with self.assertRaises(ValueError):
                    validate_output_declarations(outputs)


class TestResolveDeclaredOutputs(unittest.TestCase):
    """Relative declarations belong to the producing step's own work directory."""

    def test_relative_path_resolves_under_the_step_workdir(self):
        workdir = Path("/work/plan_1/materialize_config")

        resolved = resolve_declared_outputs(
            {"config": "extra_config_demultiplex.config", "nested": "out/data.csv"},
            workdir,
        )

        self.assertEqual(
            resolved,
            {
                "config": workdir / "extra_config_demultiplex.config",
                "nested": workdir / "out" / "data.csv",
            },
        )

    def test_absolute_path_is_used_as_is(self):
        resolved = resolve_declared_outputs(
            {"report": "/shared/reports/report.html"}, Path("/work/plan_1/report")
        )

        self.assertEqual(resolved, {"report": Path("/shared/reports/report.html")})

    def test_same_relative_filename_in_two_steps_does_not_collide(self):
        declared = {"config": "settings.config"}

        lane_1 = resolve_declared_outputs(declared, Path("/work/plan_1/lane_1"))
        lane_2 = resolve_declared_outputs(declared, Path("/work/plan_1/lane_2"))

        self.assertNotEqual(lane_1["config"], lane_2["config"])

    def test_declarations_are_not_rewritten(self):
        declared = {"config": "settings.config"}

        resolve_declared_outputs(declared, Path("/work/plan_1/lane_1"))

        self.assertEqual(declared, {"config": "settings.config"})


class TestFindMissingOutputs(unittest.TestCase):
    """Ordinary absence is missing; failing to find out is infrastructure."""

    def setUp(self):
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.root = Path(temp_dir.name)

    def test_no_required_outputs_is_never_missing_anything(self):
        with patch.object(Path, "stat", side_effect=AssertionError("stat called")):
            self.assertEqual(find_missing_outputs({}, step_id="s"), {})

    def test_present_file_and_directory_are_not_missing(self):
        (self.root / "file.txt").write_text("x", encoding="utf-8")
        (self.root / "dir").mkdir()

        missing = find_missing_outputs(
            {"file": self.root / "file.txt", "dir": self.root / "dir"}, step_id="s"
        )

        self.assertEqual(missing, {})

    def test_absent_paths_are_missing_in_declaration_order(self):
        (self.root / "blocker").write_text("a file, not a directory", encoding="utf-8")
        (self.root / "dangling").symlink_to(self.root / "nowhere")
        (self.root / "present.txt").write_text("x", encoding="utf-8")
        required = {
            "absent": self.root / "absent.txt",
            "present": self.root / "present.txt",
            "under_a_file": self.root / "blocker" / "out.txt",
            "dangling_link": self.root / "dangling",
        }

        missing = find_missing_outputs(required, step_id="s")

        self.assertEqual(list(missing), ["absent", "under_a_file", "dangling_link"])
        self.assertEqual(missing["absent"], self.root / "absent.txt")

    def test_permission_error_is_not_absence(self):
        denied = self.root / "denied.txt"
        original_stat = Path.stat

        def stat(path, *args, **kwargs):
            if path == denied:
                raise PermissionError(errno.EACCES, "Permission denied", str(path))
            return original_stat(path, *args, **kwargs)

        with patch.object(Path, "stat", stat):
            with self.assertRaises(OrchestrationError) as cm:
                find_missing_outputs({"report": denied}, step_id="publish")

        message = str(cm.exception)
        self.assertIn("report", message)
        self.assertIn("publish", message)
        self.assertIn("Permission denied", message)
        self.assertIsInstance(cm.exception.__cause__, PermissionError)

    def test_storage_io_error_is_not_absence(self):
        with patch.object(Path, "stat", side_effect=OSError(errno.EIO, "I/O error")):
            with self.assertRaises(OrchestrationError):
                find_missing_outputs({"report": self.root / "r.txt"}, step_id="s")


if __name__ == "__main__":
    unittest.main()
