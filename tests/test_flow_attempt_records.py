"""Tests for attempt-record naming and reading attempt starts back from a spool."""

import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from yggdrasil.flow.events.attempt_records import (
    ATTEMPT_REPORT_EVENT,
    ATTEMPT_STARTED_EVENT,
    SpoolAttemptHistory,
    attempt_record,
    is_record_key,
    read_attempt_records,
    record_filename,
)
from yggdrasil.flow.events.emitter import FileSpoolEmitter

REALM = "test_realm"
PLAN_ID = "pln_records"
FIRST = "exec_20260921T120500000000Z_" + "a" * 32
SECOND = "exec_20260921T120600000000Z_" + "b" * 32


class TestRecordNames(unittest.TestCase):
    """Record file names embed the attempt, so attempts never collide."""

    def test_name_embeds_the_execution_id_and_the_event_type(self):
        self.assertEqual(
            record_filename(FIRST, ATTEMPT_STARTED_EVENT),
            f"{FIRST}_plan_attempt_started.json",
        )
        self.assertNotEqual(
            record_filename(FIRST, ATTEMPT_STARTED_EVENT),
            record_filename(SECOND, ATTEMPT_STARTED_EVENT),
        )

    def test_only_ids_usable_in_a_file_name_are_record_keys(self):
        self.assertTrue(is_record_key(FIRST))
        self.assertTrue(is_record_key("exec_sched_plan"))
        for key in ("", ".", "..", "a/b", "../escape", "a\\b", None, 7):
            with self.subTest(key=key):
                self.assertFalse(is_record_key(key))


class TestAttemptRecordRecognition(unittest.TestCase):
    """An attempt record is recognized by its content alone."""

    def test_attempt_events_with_a_usable_execution_id_are_records(self):
        for event_type in (ATTEMPT_STARTED_EVENT, ATTEMPT_REPORT_EVENT):
            with self.subTest(type=event_type):
                event = {"type": event_type, "execution_id": FIRST}
                self.assertIs(attempt_record(event), event)

    def test_anything_else_is_not(self):
        for event in (
            {"type": "plan.draft", "execution_id": FIRST},
            {"type": "step.started", "execution_id": FIRST},
            {"type": ATTEMPT_STARTED_EVENT},
            {"type": ATTEMPT_REPORT_EVENT, "execution_id": "a/b"},
            [ATTEMPT_STARTED_EVENT, FIRST],
            None,
        ):
            with self.subTest(event=event):
                self.assertIsNone(attempt_record(event))


class TestSpoolAttemptHistory(unittest.TestCase):
    """Attempt records count by content, and only in the spool given."""

    def setUp(self) -> None:
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.spool = Path(temp_dir.name) / "spool"
        self.emitter = FileSpoolEmitter(self.spool)
        self.history = SpoolAttemptHistory(self.spool)
        self.plan_dir = self.spool / REALM / PLAN_ID

    def record(self, execution_id: str, event_type: str = ATTEMPT_STARTED_EVENT):
        """Publish one attempt record, as the engine does."""
        self.emitter.emit(
            {
                "type": event_type,
                "realm": REALM,
                "plan_id": PLAN_ID,
                "execution_id": execution_id,
                "_spool_path": {
                    "realm": REALM,
                    "plan_id": PLAN_ID,
                    "filename": record_filename(execution_id, event_type),
                },
            }
        )

    def test_plan_without_a_spool_directory_has_no_history(self):
        self.assertEqual(self.history.recorded_execution_ids(REALM, PLAN_ID), [])

    def write(self, name: str, content: object) -> Path:
        """Write a file into the plan's spool directory directly."""
        self.plan_dir.mkdir(parents=True, exist_ok=True)
        path = self.plan_dir / name
        text = content if isinstance(content, str) else json.dumps(content)
        path.write_text(text, encoding="utf-8")
        return path

    def test_reads_every_attempt_record_of_the_plan(self):
        self.record(SECOND)
        self.record(FIRST)
        self.record(SECOND, ATTEMPT_REPORT_EVENT)

        self.assertEqual(
            self.history.recorded_execution_ids(REALM, PLAN_ID), [FIRST, SECOND, SECOND]
        )

    def test_records_count_by_content_whatever_their_file_is_called(self):
        # Reports published before attempt-start records existed were named
        # after their event ID, and a report can outlive its start record.
        self.write(
            "3f2b9c1e-5a6d-4f1e-9b2a-0c7d8e9f1a2b.json",
            {"type": ATTEMPT_REPORT_EVENT, "execution_id": FIRST},
        )
        self.record(SECOND, ATTEMPT_REPORT_EVENT)

        self.assertEqual(
            sorted(self.history.recorded_execution_ids(REALM, PLAN_ID)),
            [FIRST, SECOND],
        )

    def test_records_that_establish_nothing_are_skipped_with_a_warning(self):
        self.record(FIRST)
        self.write("broken.json", "{not json")
        self.write(
            "bad_id.json", {"type": ATTEMPT_STARTED_EVENT, "execution_id": "../x"}
        )
        # Not an attempt record at all, so not worth a warning either.
        self.write("draft.json", {"type": "plan.draft", "execution_id": SECOND})
        self.write("notes.txt", "not an event")

        with self.assertLogs(level="WARNING") as logs:
            ids = self.history.recorded_execution_ids(REALM, PLAN_ID)

        self.assertEqual(ids, [FIRST])
        self.assertEqual(len(logs.records), 2)
        self.assertEqual(
            sorted(
                path.name for path, _ in read_attempt_records(self.plan_dir).problems
            ),
            ["bad_id.json", "broken.json"],
        )

    def test_unreadable_plan_directory_is_raised(self):
        self.record(FIRST)

        with patch(
            "yggdrasil.flow.events.attempt_records.os.scandir",
            side_effect=PermissionError("denied"),
        ):
            with self.assertRaises(PermissionError):
                self.history.recorded_execution_ids(REALM, PLAN_ID)

    def test_unreadable_record_is_raised(self):
        self.record(FIRST)
        unreadable = self.plan_dir / record_filename(FIRST, ATTEMPT_STARTED_EVENT)
        original = Path.read_text

        def read_text(path: Path, *args, **kwargs):
            if path == unreadable:
                raise PermissionError("denied")
            return original(path, *args, **kwargs)

        with patch.object(Path, "read_text", read_text):
            with self.assertRaises(PermissionError):
                self.history.recorded_execution_ids(REALM, PLAN_ID)

    def test_record_pruned_while_being_read_is_skipped(self):
        self.record(FIRST)
        self.record(SECOND)
        pruned = self.plan_dir / record_filename(FIRST, ATTEMPT_STARTED_EVENT)
        original = Path.read_text

        def read_text(path: Path, *args, **kwargs):
            if path == pruned:
                raise FileNotFoundError(str(path))
            return original(path, *args, **kwargs)

        with patch.object(Path, "read_text", read_text):
            self.assertEqual(
                self.history.recorded_execution_ids(REALM, PLAN_ID), [SECOND]
            )

    def test_reads_only_the_spool_it_was_given(self):
        elsewhere = self.spool.parent / "default_spool"
        FileSpoolEmitter(elsewhere).emit(
            {
                "type": ATTEMPT_STARTED_EVENT,
                "execution_id": FIRST,
                "_spool_path": {
                    "realm": REALM,
                    "plan_id": PLAN_ID,
                    "filename": record_filename(FIRST, ATTEMPT_STARTED_EVENT),
                },
            }
        )

        with patch.dict(os.environ, {"YGG_EVENT_SPOOL": str(elsewhere)}):
            self.assertEqual(self.history.recorded_execution_ids(REALM, PLAN_ID), [])


if __name__ == "__main__":
    unittest.main()
