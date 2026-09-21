"""Turn the event spool into ``plan_status`` snapshots.

The consumer reads the layout :class:`~yggdrasil.flow.events.emitter.FileSpoolEmitter`
writes::

    <spool>/<realm>/<plan_id>/*.json                    plan-level attempt records
    <spool>/<realm>/<plan_id>/<step_id>/*.json          blocked-step records
    <spool>/<realm>/<plan_id>/<step_id>/<run_id>/*.json one step run's events

and hands what it finds to :mod:`lib.ops.snapshot`, which decides what the
snapshot shows: one selected attempt, or a labelled legacy projection for
histories without attempt records. A step has at most one run per attempt, so
for each step the consumer looks for the run belonging to the selected
attempt, newest run directory first, and reads only that run in full.

Reading is best effort: unreadable or unparsable files and directories are
skipped rather than stopping the consumer.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from lib.core_utils.logging_utils import custom_logger
from lib.ops.snapshot import (
    PROJECTION_ATTEMPT,
    PROJECTION_LEGACY,
    AttemptEvents,
    SpooledEvent,
    StepRun,
    execution_of,
    project_attempt,
    project_legacy,
    select_execution,
)
from yggdrasil.flow.events.attempt_records import (
    ATTEMPT_REPORT_EVENT,
    ATTEMPT_STARTED_EVENT,
    STEP_BLOCKED_EVENT,
)
from yggdrasil.flow.utils.ygg_time import utcnow_iso

logger = custom_logger(__name__)

PlanFilter = Callable[[str, str], bool] | None


@dataclass
class FileSpoolConsumer:
    spool_root: Path
    writer: Any  # SnapshotWriter, CouchWriter, etc.
    filt: PlanFilter = None

    def consume(self) -> None:
        root = self.spool_root
        if not root.exists():
            return
        for realm_dir in (p for p in root.iterdir() if p.is_dir()):
            realm = realm_dir.name
            for plan_dir in (p for p in realm_dir.iterdir() if p.is_dir()):
                plan_id = plan_dir.name
                if self.filt and not self.filt(realm, plan_id):
                    continue
                snapshot = build_plan_snapshot(plan_dir, realm, plan_id)
                if not snapshot.get("scope"):
                    logger.warning(
                        f"Missing scope in plan.json for {realm}/{plan_id}; skipping..."
                    )
                    continue
                self.writer.write(plan_dir, snapshot)

    # NOTE: For dev purposes, it keeps on consuming
    def follow(self, interval_sec: float = 2.0) -> None:
        while True:
            self.consume()
            time.sleep(interval_sec)


def _safe_load(p: Path) -> dict[str, Any]:
    try:
        return json.loads(p.read_text())
    except Exception:
        return {}


# TODO: Remove old version once verified working well.
# def _find_any_event(plan_spool_dir: Path) -> dict[str, Any] | None:
#     # Walk: <spool>/<realm>/<plan_id>/<step_id>/<run_id>/*.json
#     for step_dir in (p for p in plan_spool_dir.iterdir() if p.is_dir()):
#         for run_dir in (r for r in step_dir.iterdir() if r.is_dir()):
#             # prefer early event if present
#             evs = sorted(
#                 e for e in run_dir.glob("*.json") if not e.name.endswith(".tmp")
#             )
#             if evs:
#                 try:
#                     return json.loads(evs[0].read_text())
#                 except Exception:
#                     continue
#     return None


def _find_any_event(plan_spool_dir: Path) -> dict[str, Any] | None:
    """Locate an early event (prefer 'plan.draft') to extract scope.

    Expected layout:
        <spool>/<realm>/<plan_id>/<step_id>/<run_id>/*.json
    Fallback layout (pre-engine draft):
        <spool>/<realm>/<plan_id>/<step_id>/*.json
    Plan-level records, used when no step event carries a scope (an attempt
    that ran no step has nothing else):
        <spool>/<realm>/<plan_id>/*.json
    """

    def _load_first_json(files: list[Path]) -> dict[str, Any] | None:
        for f in files:
            if f.suffix != ".json" or f.name.endswith(".tmp"):
                continue
            try:
                ev = json.loads(f.read_text(encoding="utf-8"))
                if isinstance(ev, dict) and ev.get("scope"):
                    return ev
            except Exception:
                continue
        return None

    for step_dir in (p for p in plan_spool_dir.iterdir() if p.is_dir()):
        run_dirs = [r for r in step_dir.iterdir() if r.is_dir()]

        # 1) Fallback: files directly under step_dir (e.g., plan.draft)
        if not run_dirs:
            files = sorted(step_dir.glob("*.json"))
            # Prefer plan.draft if present
            draft_first = sorted(
                files, key=lambda p: (0 if "plan_draft" in p.name else 1, p.name)
            )
            ev = _load_first_json(draft_first)
            if ev:
                return ev
            continue

        # 2) Normal case: under run_id/
        for run_dir in run_dirs:
            files = sorted(run_dir.glob("*.json"))
            ev = _load_first_json(files)
            if ev:
                return ev

    # 3) Plan-level records
    return _load_first_json(_event_files(plan_spool_dir))


def _read_scope_from_spool(plan_spool_dir: Path) -> dict[str, Any]:
    ev = _find_any_event(plan_spool_dir)
    return (ev.get("scope") if isinstance(ev, dict) else {}) or {}


def _event_files(directory: Path) -> list[Path]:
    """Return the event files directly in a directory, in name order.

    Args:
        directory: The directory to list.

    Returns:
        list[Path]: Its ``*.json`` files; none if it cannot be listed.
    """
    try:
        return sorted(path for path in directory.iterdir() if path.suffix == ".json")
    except OSError:
        return []


def _subdirectories(directory: Path) -> list[Path]:
    """Return the directories directly in a directory, in name order.

    Args:
        directory: The directory to list.

    Returns:
        list[Path]: Its subdirectories; none if it cannot be listed.
    """
    try:
        return sorted(path for path in directory.iterdir() if path.is_dir())
    except OSError:
        return []


def _load_event(path: Path) -> dict[str, Any] | None:
    """Parse one event file.

    Args:
        path: The event file.

    Returns:
        dict[str, Any] | None: The event; None if the file is unreadable or
        does not hold a JSON object.
    """
    event = _safe_load(path)
    return event if isinstance(event, dict) and event else None


def _load_events(paths: list[Path]) -> list[SpooledEvent]:
    """Parse event files, skipping any that do not hold an event.

    Args:
        paths: The event files.

    Returns:
        list[SpooledEvent]: The events, with their file names.
    """
    loaded: list[SpooledEvent] = []
    for path in paths:
        event = _load_event(path)
        if event is not None:
            loaded.append(SpooledEvent(path.name, event))
    return loaded


def _latest_run(step_dir: Path, execution_id: str | None) -> StepRun | None:
    """Return a step's newest run that belongs to one attempt.

    A run's attempt is read from its first event: every event of one run
    belongs to the same attempt. Run directories are tried newest first, which
    is almost always where the selected attempt's run is; the search does not
    rely on that, since a clock set back can name a newer run lower.

    Args:
        step_dir: The step's spool directory.
        execution_id: The attempt; None for a legacy, uncorrelated run.

    Returns:
        StepRun | None: The run with all its events; None if the step has no
        run of that attempt.
    """
    for run_dir in reversed(_subdirectories(step_dir)):
        files = _event_files(run_dir)
        first = next(
            (event for event in map(_load_event, files) if event is not None), None
        )
        if first is not None and execution_of(first) == execution_id:
            return StepRun(run_dir.name, _load_events(files))
    return None


def _first_record(
    records: list[dict[str, Any]], event_type: str, execution_id: str
) -> dict[str, Any] | None:
    """Return the first plan-level record of one type for one attempt.

    Args:
        records: The plan-level events, in file-name order.
        event_type: The record type.
        execution_id: The attempt.

    Returns:
        dict[str, Any] | None: The record; None if there is none.
    """
    return next(
        (
            record
            for record in records
            if record.get("type") == event_type and execution_of(record) == execution_id
        ),
        None,
    )


def _attempt_events(
    records: list[dict[str, Any]], execution_id: str, step_dirs: list[Path]
) -> AttemptEvents:
    """Gather everything one attempt published.

    Args:
        records: The plan-level events, in file-name order.
        execution_id: The attempt.
        step_dirs: The plan's step directories.

    Returns:
        AttemptEvents: The attempt's records, runs and blocked-step events.
    """
    runs: dict[str, StepRun] = {}
    blocked: dict[str, dict[str, Any]] = {}
    for step_dir in step_dirs:
        run = _latest_run(step_dir, execution_id)
        if run is not None:
            runs[step_dir.name] = run
        for item in _load_events(_event_files(step_dir)):
            if (
                item.event.get("type") == STEP_BLOCKED_EVENT
                and execution_of(item.event) == execution_id
            ):
                blocked[step_dir.name] = item.event
                break
    return AttemptEvents(
        execution_id=execution_id,
        started=_first_record(records, ATTEMPT_STARTED_EVENT, execution_id),
        report=_first_record(records, ATTEMPT_REPORT_EVENT, execution_id),
        runs=runs,
        blocked=blocked,
    )


def _attempt_scope(attempt: AttemptEvents) -> dict[str, Any]:
    """Return the scope an attempt's plan-level records carry, if any.

    Args:
        attempt: The attempt's events.

    Returns:
        dict[str, Any]: The scope; empty if neither record carries one.
    """
    for record in (attempt.started, attempt.report):
        scope = record.get("scope") if record is not None else None
        if isinstance(scope, dict) and scope:
            return scope
    return {}


def build_plan_snapshot(plan_dir: Path, realm: str, plan_id: str) -> dict[str, Any]:
    """Build the ``plan_status`` snapshot of one plan from its spool directory.

    The snapshot shows one attempt: the most recently admitted one, finished or
    not, with every step it planned (see :mod:`lib.ops.snapshot`). A plan whose
    spool holds no attempt records is shown as a legacy projection instead.

    Snapshot fields, beyond ``type``, ``realm``, ``plan_id`` and ``updated_at``:

    - ``scope``: from the attempt's records, else from the first event that
      carries one.
    - ``projection``: ``attempt``, or ``legacy`` for uncorrelated histories,
      whose steps are each step's latest run and need not belong to one
      attempt.
    - ``attempt``: the attempt summary (identity, ``state`` running or
      finished, and, once finished, termination reason, outcome, counts and
      diagnostic); None for a legacy projection.
    - ``steps``: an entry per step, by step ID.

    Args:
        plan_dir: The plan's spool directory.
        realm: The plan's realm.
        plan_id: The plan.

    Returns:
        dict[str, Any]: The snapshot.
    """
    records = [item.event for item in _load_events(_event_files(plan_dir))]
    execution_id = select_execution(records)
    step_dirs = _subdirectories(plan_dir)

    attempt_summary: dict[str, Any] | None = None
    if execution_id is None:
        projection = PROJECTION_LEGACY
        legacy_runs = {
            step_dir.name: run
            for step_dir in step_dirs
            if (run := _latest_run(step_dir, None)) is not None
        }
        steps = project_legacy(legacy_runs)
        scope = _read_scope_from_spool(plan_dir)  # <-- use events, not plan.json
    else:
        projection = PROJECTION_ATTEMPT
        attempt = _attempt_events(records, execution_id, step_dirs)
        attempt_summary, steps = project_attempt(attempt)
        scope = _attempt_scope(attempt) or _read_scope_from_spool(plan_dir)

    # updated_at via your common util if you have it
    try:
        now = utcnow_iso()
    except Exception:
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    return {
        "type": "plan_status",
        "realm": realm,
        "plan_id": plan_id,
        "scope": scope,
        "projection": projection,
        "attempt": attempt_summary,
        "steps": steps,
        "updated_at": now,
    }
