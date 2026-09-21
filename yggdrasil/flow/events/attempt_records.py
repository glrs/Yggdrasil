"""Plan-level attempt records and where the file spool keeps them.

Every execution attempt publishes two plan-level records, and one record per
step that a failure blocks. A :class:`~yggdrasil.flow.events.emitter.FileSpoolEmitter`
files them as::

    <spool>/<realm>/<plan_id>/<execution_id>_plan_attempt_started.json
    <spool>/<realm>/<plan_id>/<execution_id>_plan_attempt_report.json
    <spool>/<realm>/<plan_id>/<step_id>/<execution_id>_step_blocked.json

Step events are nested under a run directory unique to one invocation, so they
can use fixed, sequence-numbered file names. These records have no such
nesting: every attempt at a plan writes into the same directories, and the
emitter replaces a file that already has the requested name. The execution ID
in each file name is what keeps one attempt from overwriting another's records.
That matters beyond tidiness: the attempt-start records are the history the
execution-ID allocator reads back after a restart (see
``yggdrasil.core.execution_ids``).

A blocked step never ran, so its record names no run directory. Inventing one
would make the step look as if it had been invoked.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import TypeGuard

from lib.core_utils.logging_utils import custom_logger

# Published when an attempt is admitted, before its plan is validated or any
# step runs: the execution's identity and its planned step inventory.
ATTEMPT_STARTED_EVENT = "plan.attempt_started"

# Published once when an attempt ends, however it ends: its closed report.
ATTEMPT_REPORT_EVENT = "plan.attempt_report"

# Published once per blocked step, when a failure blocks it.
STEP_BLOCKED_EVENT = "step.blocked"

# The plan-level records that identify an attempt.
ATTEMPT_RECORD_EVENTS = frozenset({ATTEMPT_STARTED_EVENT, ATTEMPT_REPORT_EVENT})


def record_filename(execution_id: str, event_type: str) -> str:
    """Return the spool file name of one attempt's record of one type.

    Args:
        execution_id: The attempt the record belongs to.
        event_type: The record's event type, e.g. ``plan.attempt_started``.

    Returns:
        str: The file name, e.g. ``<execution_id>_plan_attempt_started.json``.
    """
    return f"{execution_id}_{event_type.replace('.', '_')}.json"


def is_record_key(execution_id: object) -> TypeGuard[str]:
    """Whether an execution ID can name attempt records in the spool.

    Record file names embed the execution ID, so an ID that is empty, is a
    relative path component, or contains a path separator would put a record
    somewhere other than beside its plan.

    Args:
        execution_id: The candidate execution ID.

    Returns:
        bool: True if the ID is a nonempty string usable in a file name.
    """
    return (
        isinstance(execution_id, str)
        and execution_id not in ("", ".", "..")
        and "/" not in execution_id
        and "\\" not in execution_id
    )


class SpoolAttemptHistory:
    """Reads back the attempt-start records a file spool holds for a plan.

    This is the history an execution-ID allocator consults so that a new
    attempt sorts above every attempt already recorded, including attempts
    made by an earlier process. It reads only the spool it was given: nothing
    here resolves a default spool location.

    Attributes:
        root: The spool root, as passed to the FileSpoolEmitter that wrote it.
    """

    def __init__(self, root: str | Path, *, logger: logging.Logger | None = None):
        """Initialize the reader.

        Args:
            root: The spool root to read.
            logger: Logger; a module logger is created when omitted.
        """
        self.root = Path(root)
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")

    def recorded_execution_ids(self, realm: str, plan_id: str) -> list[str]:
        """Return the execution IDs of every attempt-start record for a plan.

        A plan with no directory yet has no history. A record that cannot be
        parsed, or does not describe an attempt start, is skipped with a
        warning: it establishes nothing, and it must not stop new attempts.
        Failing to read the directory or a record is different, since the same
        spool is about to receive the new attempt's record; that failure is
        raised for the caller to classify.

        Args:
            realm: The plan's realm.
            plan_id: The plan.

        Returns:
            list[str]: The recorded execution IDs, in file-name order.

        Raises:
            OSError: If the plan's spool directory or a record cannot be read.
        """
        plan_dir = self.root / realm / plan_id
        suffix = record_filename("", ATTEMPT_STARTED_EVENT)
        try:
            with os.scandir(plan_dir) as entries:
                names = sorted(
                    entry.name
                    for entry in entries
                    if entry.name.endswith(suffix) and entry.is_file()
                )
        except (FileNotFoundError, NotADirectoryError):
            return []

        execution_ids: list[str] = []
        for name in names:
            path = plan_dir / name
            try:
                record = json.loads(path.read_text(encoding="utf-8"))
            except ValueError as exc:
                self._logger.warning(
                    "Ignoring attempt-start record %s, which is not valid JSON: %s",
                    path,
                    exc,
                )
                continue
            if not isinstance(record, dict) or (
                record.get("type") != ATTEMPT_STARTED_EVENT
            ):
                self._logger.warning(
                    "Ignoring %s: it is not a %s record", path, ATTEMPT_STARTED_EVENT
                )
                continue
            execution_id = record.get("execution_id")
            if not is_record_key(execution_id):
                self._logger.warning(
                    "Ignoring %s: its execution_id %r is not usable",
                    path,
                    execution_id,
                )
                continue
            execution_ids.append(execution_id)
        return execution_ids
