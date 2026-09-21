"""Reduce one plan's spooled events to its operational snapshot.

Pure functions over events that have already been read; walking the spool is
:mod:`lib.ops.consumer`'s job. Keeping the two apart lets the selection and
precedence rules below be checked without a filesystem.

**One attempt per snapshot.** A snapshot describes a single execution attempt:
the one with the highest execution ID among the plan's attempt records,
whether or not it has finished. Execution IDs are allocated in order at
admission (``yggdrasil.core.execution_ids``), so this is the most recently
admitted attempt, and replaying or re-delivering an old attempt's events
cannot change which one is selected. Every step's state comes from that
attempt's own events. A step the attempt has not reached is shown as not
reached, never with an earlier attempt's result. Plan generations are opaque
and run tokens reset on regeneration, so neither is compared to order
attempts.

**Precedence within the attempt.** Once the attempt's report exists, it is
authoritative for step outcomes, failures and blocker diagnostics: a
``step.blocked`` event carries only the blockers known when it was published,
and a delayed or replayed one never narrows the report's lists. Within one
step run, a terminal event (succeeded, skipped, failed) is never replaced by a
later non-terminal one, such as a late progress or artifact event; only a
failure can replace an earlier success. Copies of one event (same ``eid``)
count once.

**Legacy histories.** Events published before attempts were correlated carry
no execution ID, and which of them belonged to the same attempt was never
recorded. A plan whose spool holds no attempt records is therefore shown as a
*legacy projection*: each step's latest uncorrelated run, as before, labelled
so that it is not mistaken for one attempt. Uncorrelated events are never
merged into a correlated attempt.

The snapshot describes the latest observed attempt. Its plan generation is the
one that attempt captured, which is not necessarily the stored plan's current
generation; a view of the stored plan reads that from the plan document.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from yggdrasil.flow.events.attempt_records import (
    ATTEMPT_RECORD_EVENTS,
    STEP_BLOCKED_EVENT,
)
from yggdrasil.flow.outcomes import StepOutcome

# What a snapshot projects.
PROJECTION_ATTEMPT = "attempt"
PROJECTION_LEGACY = "legacy"

# How far the projected attempt has got.
ATTEMPT_RUNNING = "running"
ATTEMPT_FINISHED = "finished"

# The state of a step with no events in the projected attempt: still to come
# while the attempt runs, never reached once it has ended.
STATE_PENDING = "pending"
STATE_UNREACHED = "unreached"

# The state of a legacy step run with no step lifecycle event at all.
STATE_UNKNOWN = "unknown"

# Terminal step events within one run, and the outcome each records.
_TERMINAL_OUTCOMES: dict[str, StepOutcome] = {
    "step.succeeded": StepOutcome.SUCCEEDED,
    "step.skipped": StepOutcome.REUSED,
    "step.failed": StepOutcome.FAILED,
}
_FAILED_EVENT = "step.failed"


@dataclass(frozen=True)
class SpooledEvent:
    """One event as found in the spool.

    Attributes:
        name: The event's file name, which orders events without a ``seq``.
        event: The parsed event.
    """

    name: str
    event: dict[str, Any]


@dataclass(frozen=True)
class StepRun:
    """The events of one evaluation of one step, i.e. one run directory.

    Attributes:
        run_id: The run's ID (its directory name).
        events: The events found in it, in any order.
    """

    run_id: str
    events: list[SpooledEvent]


@dataclass(frozen=True)
class AttemptEvents:
    """What one attempt published, as found in the spool.

    Attributes:
        execution_id: The attempt.
        started: Its ``plan.attempt_started`` record, if found.
        report: Its ``plan.attempt_report`` event, if published yet.
        runs: Its run of each step it reused or executed, by step ID.
        blocked: Its ``step.blocked`` event of each blocked step, by step ID.
    """

    execution_id: str
    started: dict[str, Any] | None = None
    report: dict[str, Any] | None = None
    runs: dict[str, StepRun] = field(default_factory=dict)
    blocked: dict[str, dict[str, Any]] = field(default_factory=dict)


@dataclass
class StepStatus:
    """The snapshot entry of one step.

    Attributes:
        step_name: The step's name.
        state: The step's latest lifecycle event type (e.g. ``step.started``,
            ``step.failed``, ``step.blocked``), or ``pending``/``unreached``
            when the attempt has no event for it; None until resolved.
        outcome: The step's terminal outcome in the attempt (a
            :class:`~yggdrasil.flow.outcomes.StepOutcome` value), or None if
            it has none (yet).
        run_id: The run the state was read from; None if the step never ran.
        fingerprint: The run's fingerprint.
        progress: Progress in percent.
        artifacts: The artifact manifest the step reported on success.
        metrics: The metrics the step reported on success.
        job: Job details, when the run reported any.
        ts: Timestamp of the event the state was read from.
        error: How the step failed, for a failed step.
        direct_blockers: For a blocked step, the failed or blocked
            prerequisites that blocked it.
        failed_ancestors: For a blocked step, the failed steps upstream of it.
    """

    step_name: str = ""
    state: str | None = None
    outcome: str | None = None
    run_id: str | None = None
    fingerprint: str | None = None
    progress: Any = 0
    artifacts: Any = field(default_factory=list)
    metrics: Any = field(default_factory=dict)
    job: Any = None
    ts: str | None = None
    error: dict[str, Any] | None = None
    direct_blockers: list[str] = field(default_factory=list)
    failed_ancestors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return the entry as it appears in a snapshot.

        Returns:
            dict[str, Any]: The entry's fields as plain JSON types.
        """
        return {
            "step_name": self.step_name,
            "state": self.state,
            "outcome": self.outcome,
            "run_id": self.run_id,
            "fingerprint": self.fingerprint,
            "progress": self.progress,
            "artifacts": self.artifacts,
            "metrics": self.metrics,
            "job": self.job,
            "ts": self.ts,
            "error": self.error,
            "direct_blockers": list(self.direct_blockers),
            "failed_ancestors": list(self.failed_ancestors),
        }


# ----- selection -----


def execution_of(event: Mapping[str, Any]) -> str | None:
    """Return the execution an event belongs to, if it is correlated.

    Args:
        event: A spooled event.

    Returns:
        str | None: Its ``execution_id``; None for an uncorrelated event.
    """
    execution_id = event.get("execution_id")
    return execution_id if isinstance(execution_id, str) and execution_id else None


def select_execution(records: Iterable[Mapping[str, Any]]) -> str | None:
    """Select the attempt a plan's snapshot describes.

    Args:
        records: The plan-level events in the plan's spool directory.

    Returns:
        str | None: The highest execution ID among the attempt records,
        finished or not; None if there are no attempt records.
    """
    execution_ids = {
        execution_id
        for record in records
        if record.get("type") in ATTEMPT_RECORD_EVENTS
        and (execution_id := execution_of(record)) is not None
    }
    return max(execution_ids, default=None)


# ----- one step run -----


def order_events(events: Iterable[SpooledEvent]) -> list[dict[str, Any]]:
    """Put one run's events in publication order, counting copies once.

    Events are ordered by ``seq``, then by file name; events without a
    ``seq`` (such as write traces) follow those with one. Of several events
    sharing an ``eid``, the first in that order is kept.

    Args:
        events: The run's events.

    Returns:
        list[dict[str, Any]]: The distinct events, in order.
    """

    def order(item: SpooledEvent) -> tuple[int, int, str]:
        seq = item.event.get("seq")
        if isinstance(seq, int) and not isinstance(seq, bool):
            return (0, seq, item.name)
        return (1, 0, item.name)

    seen: set[str] = set()
    ordered: list[dict[str, Any]] = []
    for item in sorted(events, key=order):
        eid = item.event.get("eid")
        if isinstance(eid, str):
            if eid in seen:
                continue
            seen.add(eid)
        ordered.append(item.event)
    return ordered


def _effective_event(events: Sequence[dict[str, Any]]) -> dict[str, Any] | None:
    """Return the event a run's state is read from.

    That is its terminal event once it has one, and its latest step lifecycle
    event before that. Events that are not ``step.*`` events, such as
    DataAccess write traces, never set a step's state.

    Args:
        events: The run's events, in publication order.

    Returns:
        dict[str, Any] | None: The effective event; None if the run has no
        step lifecycle event.
    """
    effective: dict[str, Any] | None = None
    for event in events:
        event_type = event.get("type")
        if not isinstance(event_type, str) or not event_type.startswith("step."):
            continue
        if effective is not None and effective["type"] in _TERMINAL_OUTCOMES:
            # A terminal outcome stands; only a failure replaces a success.
            if event_type == _FAILED_EVENT and effective["type"] != _FAILED_EVENT:
                effective = event
            continue
        effective = event
    return effective


def fold_run(run: StepRun, execution_id: str | None) -> StepStatus:
    """Reduce one step run to its snapshot entry.

    Only events of ``execution_id`` count, so a foreign event copied into the
    run changes nothing.

    Args:
        run: The run.
        execution_id: The attempt being projected; None for the legacy
            projection, which counts only uncorrelated events.

    Returns:
        StepStatus: The run's entry; its state is None if the run has no step
        lifecycle event of that attempt.
    """
    events = [
        event
        for event in order_events(run.events)
        if execution_of(event) == execution_id
    ]
    status = StepStatus(run_id=run.run_id)
    effective = _effective_event(events)
    if effective is None:
        return status

    event_type = effective["type"]
    outcome = _TERMINAL_OUTCOMES.get(event_type)
    status.state = event_type
    status.outcome = outcome.value if outcome is not None else None
    status.step_name = str(effective.get("step_name") or "")
    status.fingerprint = effective.get("fingerprint")
    completed = outcome is not None and outcome.satisfies_dependency()
    status.progress = effective.get("progress", 100 if completed else 0)
    status.artifacts = effective.get("artifacts", [])
    status.metrics = effective.get("metrics", {})
    status.job = effective.get("job")
    status.ts = effective.get("ts")
    if outcome is StepOutcome.FAILED:
        status.error = {
            key: effective.get(key) for key in ("error", "kind", "code", "advice")
        }
    return status


# ----- one attempt -----


def _mapping(container: Mapping[str, Any] | None, key: str) -> Mapping[str, Any]:
    """Return a mapping field of a record, or an empty mapping if it has none."""
    value = container.get(key) if container is not None else None
    return value if isinstance(value, Mapping) else {}


def _string_list(value: Any) -> list[str]:
    """Return a list of step IDs from a record field, or [] if it is not one."""
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _planned_steps(
    attempt: AttemptEvents, report: Mapping[str, Any] | None
) -> dict[str, str]:
    """Return every step the attempt planned, with its name, in plan order.

    The attempt-start record's inventory, or the report's when the record is
    missing, followed by any other step the attempt published events for.

    Args:
        attempt: The attempt's events.
        report: Its closed report, if published.

    Returns:
        dict[str, str]: Step names by step ID; a name is empty when only the
        report or the step's events name the step.
    """
    planned: dict[str, str] = {}
    steps = attempt.started.get("steps") if attempt.started else None
    if isinstance(steps, list):
        for entry in steps:
            if isinstance(entry, Mapping) and isinstance(entry.get("step_id"), str):
                planned.setdefault(entry["step_id"], str(entry.get("step_name") or ""))
    elif report is not None:
        planned = dict.fromkeys(_string_list(report.get("step_ids")), "")
    for step_id in (*attempt.runs, *attempt.blocked):
        planned.setdefault(step_id, "")
    return planned


def _step_status(
    step_id: str,
    attempt: AttemptEvents,
    report: Mapping[str, Any] | None,
) -> StepStatus:
    """Build one step's entry from the attempt's events, then its report.

    Args:
        step_id: The step.
        attempt: The attempt's events.
        report: Its closed report, if published.

    Returns:
        StepStatus: The step's entry.
    """
    run = attempt.runs.get(step_id)
    status = fold_run(run, attempt.execution_id) if run is not None else StepStatus()

    blocked = attempt.blocked.get(step_id)
    if blocked is not None and status.state is None:
        status.state = STEP_BLOCKED_EVENT
        status.outcome = StepOutcome.BLOCKED.value
        status.step_name = str(blocked.get("step_name") or "")
        status.ts = blocked.get("ts")
        status.direct_blockers = _string_list(blocked.get("direct_blockers"))
        status.failed_ancestors = _string_list(blocked.get("failed_ancestors"))

    if report is not None:
        # The report is the attempt's own account and supersedes its events.
        outcome = _mapping(report, "step_outcomes").get(step_id)
        status.outcome = outcome if isinstance(outcome, str) else None
        if outcome == StepOutcome.BLOCKED.value:
            status.state = STEP_BLOCKED_EVENT
            status.direct_blockers = _string_list(
                _mapping(report, "direct_blockers").get(step_id)
            )
            status.failed_ancestors = _string_list(
                _mapping(report, "failed_ancestors").get(step_id)
            )
        failure = _mapping(report, "failures").get(step_id)
        if isinstance(failure, Mapping):
            status.error = dict(failure)

    if status.state is None:
        status.state = STATE_UNREACHED if report is not None else STATE_PENDING
    return status


def project_attempt(
    attempt: AttemptEvents,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """Project one attempt into a snapshot's attempt summary and steps.

    Every planned step appears, including work not started yet. An attempt
    without a published report is running, as far as the spool shows; one
    that crashed outright stays so until a newer attempt is admitted.

    Args:
        attempt: The attempt's events.

    Returns:
        tuple: The attempt summary and the step entries by step ID.
    """
    report_event = attempt.report
    report: Mapping[str, Any] | None = None
    if report_event is not None and isinstance(report_event.get("report"), Mapping):
        report = report_event["report"]

    steps: dict[str, dict[str, Any]] = {}
    for step_id, step_name in _planned_steps(attempt, report).items():
        status = _step_status(step_id, attempt, report)
        status.step_name = status.step_name or step_name
        steps[step_id] = status.to_dict()

    started = attempt.started
    identity: Mapping[str, Any] = (
        started if started is not None else (report if report is not None else {})
    )
    summary: dict[str, Any] = {
        "execution_id": attempt.execution_id,
        "plan_generation": identity.get("plan_generation"),
        "run_token": identity.get("run_token"),
        "failure_policy": identity.get("failure_policy"),
        "execution_authority": started.get("execution_authority") if started else None,
        "execution_owner": started.get("execution_owner") if started else None,
        "started_at": identity.get("started_at"),
        "state": ATTEMPT_FINISHED if report is not None else ATTEMPT_RUNNING,
        "ended_at": None,
        "termination_reason": None,
        "outcome": None,
        "is_drained": None,
        "counts": None,
        "diagnostic": None,
    }
    if report is not None:
        for key in (
            "ended_at",
            "termination_reason",
            "outcome",
            "is_drained",
            "counts",
            "diagnostic",
        ):
            summary[key] = report.get(key)
    return summary, steps


# ----- legacy -----


def project_legacy(runs: Mapping[str, StepRun]) -> dict[str, dict[str, Any]]:
    """Project uncorrelated histories: each step's latest uncorrelated run.

    Args:
        runs: Each step's latest uncorrelated run, by step ID.

    Returns:
        dict[str, dict[str, Any]]: The step entries by step ID.
    """
    steps: dict[str, dict[str, Any]] = {}
    for step_id, run in runs.items():
        status = fold_run(run, None)
        status.state = status.state or STATE_UNKNOWN
        steps[step_id] = status.to_dict()
    return steps
