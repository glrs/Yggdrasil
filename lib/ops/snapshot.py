"""Reduce one plan's spooled events to its operational snapshot.

Pure functions over events that have already been read; walking the spool is
:mod:`lib.ops.consumer`'s job. Keeping the two apart lets the selection and
precedence rules below be checked without a filesystem.

**One attempt per snapshot.** A snapshot describes a single execution attempt:
the one whose execution ID orders highest among the plan's attempt records
(:func:`~yggdrasil.core.execution_ids.execution_order_key`), whether or not it
has finished. Execution IDs are allocated in order at admission
(``yggdrasil.core.execution_ids``), so this is the most recently admitted
attempt, and replaying or re-delivering an old attempt's events cannot change
which one is selected. Every step's state comes from that attempt's own events
and report. A step the attempt has not reached is shown as not reached, never
with an earlier attempt's result. Plan generations are opaque and run tokens
reset on regeneration, so neither is compared to order attempts.

**Precedence within the attempt.** While the attempt runs, its events are all
there is. Within one step run, a terminal event (succeeded, skipped, failed)
is never replaced by a later non-terminal one, such as a late progress or
artifact event; only a failure can replace an earlier success. Copies of one
event (same ``eid``) count once. Once the attempt's report exists, it decides
each step's outcome, and a step's state follows from its outcome whatever its
events say: a step can fail before its ``@step`` wrapper publishes anything,
and a terminal event can be published for a step whose success the engine then
never established. A ``step.blocked`` event carries only the blockers known
when it was published; the report's lists are the complete ones, and a delayed
or replayed event never narrows them. Metadata the events carry (run, name,
fingerprint, job, time) is kept whenever it does not contradict the report.

**Step states.** A step's ``state`` is the event type that records its
outcome (``step.succeeded``, ``step.skipped``, ``step.failed``,
``step.blocked``), or its latest lifecycle event while it runs. A step with no
outcome is ``pending`` while the attempt runs, ``interrupted`` once the
attempt has ended if it had started, and ``unreached`` if it never started.

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

from yggdrasil.core.execution_ids import execution_order_key
from yggdrasil.flow.events.attempt_records import STEP_BLOCKED_EVENT, attempt_record
from yggdrasil.flow.outcomes import StepOutcome

# What a snapshot projects.
PROJECTION_ATTEMPT = "attempt"
PROJECTION_LEGACY = "legacy"

# How far the projected attempt has got.
ATTEMPT_RUNNING = "running"
ATTEMPT_FINISHED = "finished"

# The state of a step with no outcome in the projected attempt: still to come
# while the attempt runs; once it has ended, cut short if it had started, and
# never reached if it had not.
STATE_PENDING = "pending"
STATE_INTERRUPTED = "interrupted"
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

# The state each outcome is shown with: the event type that records it.
_OUTCOME_STATES: dict[StepOutcome, str] = {
    **{outcome: event_type for event_type, outcome in _TERMINAL_OUTCOMES.items()},
    StepOutcome.BLOCKED: STEP_BLOCKED_EVENT,
}

# Outcomes a step can only reach by being evaluated, so only these leave a run.
_RUN_OUTCOMES = frozenset(
    {StepOutcome.SUCCEEDED, StepOutcome.REUSED, StepOutcome.FAILED}
)

# Fields that identify a step's run rather than describe its outcome.
_IDENTITY_FIELDS = ("step_name", "fingerprint", "job", "ts")


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
        state: The event type recording the step's outcome, its latest
            lifecycle event while it runs, or ``pending``, ``interrupted`` or
            ``unreached`` when it has no outcome; None until resolved.
        outcome: The step's terminal outcome in the attempt (a
            :class:`~yggdrasil.flow.outcomes.StepOutcome` value), or None if
            it has none (yet).
        run_id: The run the state was read from; None if the step never ran.
        fingerprint: The run's fingerprint.
        progress: Progress in percent.
        artifacts: The artifact manifest the step reported on success.
        metrics: The metrics the step reported on success.
        job: Job details, when the run reported any.
        ts: Timestamp of the event the entry was read from.
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
        str | None: The execution ID ordering highest among the attempt
        records, finished or not; None if there are no attempt records.
    """
    execution_ids = {
        record["execution_id"]
        for record in records
        if attempt_record(record) is not None
    }
    return max(execution_ids, key=execution_order_key, default=None)


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


def _run_events(run: StepRun | None, execution_id: str | None) -> list[dict[str, Any]]:
    """Return a run's events of one attempt, in publication order.

    Only events of ``execution_id`` count, so a foreign event copied into the
    run changes nothing.

    Args:
        run: The run, if the step has one.
        execution_id: The attempt; None for uncorrelated (legacy) events.

    Returns:
        list[dict[str, Any]]: The events; none if there is no run.
    """
    if run is None:
        return []
    return [
        event
        for event in order_events(run.events)
        if execution_of(event) == execution_id
    ]


def _effective_index(
    events: Sequence[Mapping[str, Any]], *, terminal: bool = True
) -> int | None:
    """Return the position of the event a run's state is read from.

    That is its terminal event once it has one, and its latest step lifecycle
    event before that. Events that are not ``step.*`` events, such as
    DataAccess write traces, never set a step's state.

    Args:
        events: The run's events, in publication order.
        terminal: False to ignore terminal events altogether, for a step whose
            outcome was never established however its events end.

    Returns:
        int | None: The effective event's position; None if the run has no
        (non-terminal, if so asked) step lifecycle event.
    """
    effective: int | None = None
    for position, event in enumerate(events):
        event_type = event.get("type")
        if not isinstance(event_type, str) or not event_type.startswith("step."):
            continue
        if event_type in _TERMINAL_OUTCOMES and not terminal:
            continue
        if effective is not None and events[effective]["type"] in _TERMINAL_OUTCOMES:
            # A terminal outcome stands; only a failure replaces a success.
            if event_type == _FAILED_EVENT and (
                events[effective]["type"] != _FAILED_EVENT
            ):
                effective = position
            continue
        effective = position
    return effective


def _identity(run_id: str | None, events: Sequence[Mapping[str, Any]]) -> StepStatus:
    """Start an entry with what identifies a step's run, and nothing else.

    Args:
        run_id: The step's run, if it has one.
        events: Events of the step, in order; each identifying field (name,
            fingerprint, job, time) is read from the latest that carries it.

    Returns:
        StepStatus: An entry without state, outcome or results.
    """
    known: dict[str, Any] = {}
    for event in events:
        for key in _IDENTITY_FIELDS:
            if event.get(key) not in (None, ""):
                known[key] = event[key]
    status = StepStatus(run_id=run_id)
    status.step_name = str(known.get("step_name", ""))
    status.fingerprint = known.get("fingerprint")
    status.job = known.get("job")
    status.ts = known.get("ts")
    return status


def _entry(
    run_id: str | None, events: Sequence[Mapping[str, Any]], position: int
) -> StepStatus:
    """Build an entry from the event a step's state is read from.

    Args:
        run_id: The step's run, if it has one.
        events: The run's events of the attempt, in order.
        position: The position of the effective event among them. Nothing
            published after it is read.

    Returns:
        StepStatus: The entry the event describes.
    """
    event = events[position]
    status = _identity(run_id, events[: position + 1])
    event_type = str(event["type"])
    outcome = _TERMINAL_OUTCOMES.get(event_type)
    status.state = event_type
    status.outcome = outcome.value if outcome is not None else None
    completed = outcome is not None and outcome.satisfies_dependency()
    status.progress = event.get("progress", 100 if completed else 0)
    status.artifacts = event.get("artifacts", [])
    status.metrics = event.get("metrics", {})
    if outcome is StepOutcome.FAILED:
        status.error = {
            key: event.get(key) for key in ("error", "kind", "code", "advice")
        }
    return status


def fold_run(run: StepRun, execution_id: str | None) -> StepStatus:
    """Reduce one step run to its snapshot entry, from its events alone.

    Args:
        run: The run.
        execution_id: The attempt being projected; None for the legacy
            projection, which counts only uncorrelated events.

    Returns:
        StepStatus: The run's entry; its state is None if the run has no step
        lifecycle event of that attempt.
    """
    events = _run_events(run, execution_id)
    position = _effective_index(events)
    if position is None:
        return StepStatus(run_id=run.run_id)
    return _entry(run.run_id, events, position)


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


def _report_of(attempt: AttemptEvents) -> Mapping[str, Any] | None:
    """Return the attempt's closed report, if it has been published."""
    report = attempt.report.get("report") if attempt.report is not None else None
    return report if isinstance(report, Mapping) else None


def _outcome_of(report: Mapping[str, Any], step_id: str) -> StepOutcome | None:
    """Return the outcome a report records for a step, if a valid one."""
    value = _mapping(report, "step_outcomes").get(step_id)
    try:
        return StepOutcome(value) if isinstance(value, str) else None
    except ValueError:
        return None


def _running_step_of(report: Mapping[str, Any]) -> str | None:
    """Return the step a report says was running when the attempt ended."""
    running = _mapping(_mapping(report, "diagnostic"), "details").get("running_step_id")
    return running if isinstance(running, str) else None


def _planned_steps(attempt: AttemptEvents) -> dict[str, str] | None:
    """Return the steps the attempt planned, with their names, in plan order.

    Args:
        attempt: The attempt's events.

    Returns:
        dict[str, str] | None: Step names by step ID, from the attempt-start
        record or, when it is missing, from the report (names then empty);
        None if neither lists the attempt's steps.
    """
    steps = attempt.started.get("steps") if attempt.started else None
    if isinstance(steps, list):
        planned: dict[str, str] = {}
        for entry in steps:
            if isinstance(entry, Mapping) and isinstance(entry.get("step_id"), str):
                planned.setdefault(entry["step_id"], str(entry.get("step_name") or ""))
        return planned
    report = _report_of(attempt)
    if report is not None and isinstance(report.get("step_ids"), list):
        return dict.fromkeys(_string_list(report.get("step_ids")), "")
    return None


def planned_step_ids(attempt: AttemptEvents) -> list[str] | None:
    """Return the steps the attempt planned, in plan order.

    No other step can have run in the attempt, so these are the only step
    directories worth reading for it.

    Args:
        attempt: The attempt's records; its runs and blocked events are not
            needed.

    Returns:
        list[str] | None: The planned step IDs; None if no record lists them.
    """
    planned = _planned_steps(attempt)
    return list(planned) if planned is not None else None


def blocked_steps_to_read(attempt: AttemptEvents, step_ids: Iterable[str]) -> list[str]:
    """Return the steps that may have a ``step.blocked`` event in the attempt.

    Args:
        attempt: The attempt's records.
        step_ids: The steps to consider.

    Returns:
        list[str]: Every one of them while the attempt runs; once it has
        ended, only those its report records as blocked.
    """
    report = _report_of(attempt)
    if report is None:
        return list(step_ids)
    return [
        step_id
        for step_id in step_ids
        if _outcome_of(report, step_id) is StepOutcome.BLOCKED
    ]


def steps_to_search_for_runs(
    attempt: AttemptEvents, step_ids: Iterable[str]
) -> list[str]:
    """Return the steps that may have a run in the attempt.

    Finding a step's run of one attempt means looking through that step's run
    directories, all of them if it has none. That search is skipped wherever
    the attempt's own records rule a run out: a blocked step was never
    evaluated, and once the report exists, only a step it records as
    succeeded, reused or failed, or as running when the attempt ended, can
    have been. A preflight rejection records neither, so it searches nothing.

    Args:
        attempt: The attempt's records and blocked-step events.
        step_ids: The steps to consider.

    Returns:
        list[str]: The steps whose runs are worth searching, in the given
        order.
    """
    report = _report_of(attempt)
    if report is None:
        return [step_id for step_id in step_ids if step_id not in attempt.blocked]
    running = _running_step_of(report)
    return [
        step_id
        for step_id in step_ids
        if _outcome_of(report, step_id) in _RUN_OUTCOMES or step_id == running
    ]


def _live_status(
    run_id: str | None,
    events: Sequence[Mapping[str, Any]],
    blocked: Mapping[str, Any] | None,
) -> StepStatus:
    """Build a step's entry while its attempt is still running.

    Args:
        run_id: The step's run in the attempt, if it has one.
        events: That run's events of the attempt, in order.
        blocked: The step's ``step.blocked`` event, if it has one.

    Returns:
        StepStatus: The step's entry, as its events describe it.
    """
    position = _effective_index(events)
    if position is not None:
        return _entry(run_id, events, position)
    if blocked is not None:
        status = _identity(None, [blocked])
        status.state = STEP_BLOCKED_EVENT
        status.outcome = StepOutcome.BLOCKED.value
        status.direct_blockers = _string_list(blocked.get("direct_blockers"))
        status.failed_ancestors = _string_list(blocked.get("failed_ancestors"))
        return status
    status = StepStatus(run_id=run_id)
    status.state = STATE_PENDING
    return status


def _settled_status(
    step_id: str,
    run_id: str | None,
    events: Sequence[Mapping[str, Any]],
    blocked: Mapping[str, Any] | None,
    report: Mapping[str, Any],
) -> StepStatus:
    """Build a step's entry once its attempt has ended, from the report first.

    Args:
        step_id: The step.
        run_id: The step's run in the attempt, if it has one.
        events: That run's events of the attempt, in order.
        blocked: The step's ``step.blocked`` event, if it has one.
        report: The attempt's closed report.

    Returns:
        StepStatus: The step's entry, as the report decides it.
    """
    outcome = _outcome_of(report, step_id)
    if outcome is None:
        # No outcome was established, whatever the events claim: a terminal
        # event can outlive an attempt that failed before recording it.
        latest = _effective_index(events, terminal=False)
        status = (
            _entry(run_id, events, latest)
            if latest is not None
            else _identity(run_id, events)
        )
        reached = bool(events) or step_id == _running_step_of(report)
        status.state = STATE_INTERRUPTED if reached else STATE_UNREACHED
        return status

    state = _OUTCOME_STATES[outcome]
    position = _effective_index(events)
    if position is not None and events[position]["type"] == state:
        status = _entry(run_id, events, position)
    else:
        # The events do not record this outcome: the step failed before its
        # @step wrapper published anything, its terminal event is missing, or
        # only the report survives. Keep what identifies the run.
        identifying: Sequence[Mapping[str, Any]] = events or (
            [blocked] if blocked is not None else []
        )
        status = _identity(run_id, identifying)
        status.state = state
        status.progress = 100 if outcome.satisfies_dependency() else 0
    status.outcome = outcome.value
    if outcome is StepOutcome.BLOCKED:
        status.direct_blockers = _string_list(
            _mapping(report, "direct_blockers").get(step_id)
        )
        status.failed_ancestors = _string_list(
            _mapping(report, "failed_ancestors").get(step_id)
        )
    failure = _mapping(report, "failures").get(step_id)
    if isinstance(failure, Mapping):
        status.error = dict(failure)
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
    report = _report_of(attempt)
    planned = _planned_steps(attempt) or {}
    for step_id in (*attempt.runs, *attempt.blocked):
        planned.setdefault(step_id, "")

    steps: dict[str, dict[str, Any]] = {}
    for step_id, step_name in planned.items():
        run = attempt.runs.get(step_id)
        run_id = run.run_id if run is not None else None
        events = _run_events(run, attempt.execution_id)
        blocked = attempt.blocked.get(step_id)
        status = (
            _live_status(run_id, events, blocked)
            if report is None
            else _settled_status(step_id, run_id, events, blocked, report)
        )
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
