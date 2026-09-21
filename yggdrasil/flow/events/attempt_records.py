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

A blocked step never ran, so its record names no run directory. Inventing one
would make the step look as if it had been invoked.

**Which files count.** The plan-level records are a plan's history of
attempts. Two readers depend on it: the execution-ID allocator, which orders a
new attempt above every recorded one (see ``yggdrasil.core.execution_ids``),
and the ops consumer, which shows the most recent one. They must agree on what
the history holds, or the allocator can order a new attempt below one the
consumer still shows. Both therefore read it through
:func:`read_attempt_records`, which recognizes records by content, never by
file name: reports published before attempt-start records existed are named
after their event ID, and a report can outlive its start record.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeGuard

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


def attempt_record(event: object) -> dict[str, Any] | None:
    """Return an event if it is an attempt record, and None otherwise.

    An attempt record is a ``plan.attempt_started`` or ``plan.attempt_report``
    event whose execution ID can name records, whatever file holds it.

    Args:
        event: A parsed plan-level event.

    Returns:
        dict[str, Any] | None: The event, if it is an attempt record.
    """
    if (
        isinstance(event, dict)
        and event.get("type") in ATTEMPT_RECORD_EVENTS
        and is_record_key(event.get("execution_id"))
    ):
        return event
    return None


@dataclass(frozen=True)
class AttemptRecordScan:
    """The attempt records one plan's spool directory holds.

    Attributes:
        records: The attempt records, in file-name order.
        problems: Files that may have held a record but yield none, each with
            what went wrong: a file that cannot be read (OSError), one that is
            not valid JSON, or an attempt-typed event whose execution ID cannot
            name records (ValueError). Plan-level events of other types are not
            problems; they are simply not records.
    """

    records: list[dict[str, Any]]
    problems: list[tuple[Path, Exception]]


def read_attempt_records(plan_dir: Path) -> AttemptRecordScan:
    """Read the attempt records in one plan's spool directory.

    Every ``*.json`` file directly in the directory is a candidate, and a
    candidate counts when :func:`attempt_record` accepts its content. A file
    that disappears between listing and reading was pruned, not damaged, and
    is skipped without a problem. What to do about problems is the caller's
    decision: the allocator cannot order against a record it cannot read,
    while the ops consumer shows what it can.

    Args:
        plan_dir: The plan's spool directory, ``<spool>/<realm>/<plan_id>``.

    Returns:
        AttemptRecordScan: The records and the problems found. A plan without
        a spool directory has neither.

    Raises:
        OSError: If the directory exists but cannot be listed.
    """
    try:
        with os.scandir(plan_dir) as entries:
            paths = sorted(
                plan_dir / entry.name
                for entry in entries
                if entry.name.endswith(".json") and entry.is_file()
            )
    except (FileNotFoundError, NotADirectoryError):
        return AttemptRecordScan(records=[], problems=[])

    records: list[dict[str, Any]] = []
    problems: list[tuple[Path, Exception]] = []
    for path in paths:
        try:
            event = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            continue
        except (OSError, ValueError) as exc:
            problems.append((path, exc))
            continue
        record = attempt_record(event)
        if record is not None:
            records.append(record)
        elif isinstance(event, dict) and event.get("type") in ATTEMPT_RECORD_EVENTS:
            problems.append(
                (
                    path,
                    ValueError(
                        f"its execution_id {event.get('execution_id')!r} cannot "
                        f"name attempt records"
                    ),
                )
            )
    return AttemptRecordScan(records=records, problems=problems)


class SpoolAttemptHistory:
    """Reads back the attempts a file spool has recorded for a plan.

    This is the history an execution-ID allocator consults so that a new
    attempt sorts above every attempt already recorded, including attempts
    made by an earlier process. It counts exactly the records the ops consumer
    selects among (see :func:`read_attempt_records`). It reads only the spool
    it was given: nothing here resolves a default spool location.

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
        """Return the execution ID of every attempt recorded for a plan.

        A plan with no directory yet has no history. A file that is not valid
        JSON, or an attempt record whose execution ID is unusable, establishes
        nothing and must not stop new attempts: it is skipped with a warning.
        Failing to read the directory or a file is different, since the same
        spool is about to receive the new attempt's record; that failure is
        raised for the caller to classify.

        Args:
            realm: The plan's realm.
            plan_id: The plan.

        Returns:
            list[str]: The recorded execution IDs, in file-name order. An
            attempt with both a start record and a report appears twice.

        Raises:
            OSError: If the plan's spool directory or a file in it cannot be
                read.
        """
        scan = read_attempt_records(self.root / realm / plan_id)
        for path, problem in scan.problems:
            if isinstance(problem, OSError):
                raise problem
            self._logger.warning(
                "Ignoring %s, which holds no usable attempt record: %s", path, problem
            )
        return [record["execution_id"] for record in scan.records]
