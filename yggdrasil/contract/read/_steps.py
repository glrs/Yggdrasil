"""A snapshot's step entries: each step's status, and what else it records.

A step entry records its ``state``, the type of its latest ``step.*`` event
or, for a step with none, ``pending``, ``interrupted``, ``unreached`` or
``unknown``; and its ``outcome``, written only beside the event that records
it: ``step.succeeded`` records ``succeeded``, ``step.skipped`` ``reused``,
``step.failed`` ``failed`` and ``step.blocked`` ``blocked``.

:func:`step_status` derives the step's status from the two, and says when
they contradict each other. :func:`observed_steps` reads every entry of a
snapshot whose structure has been checked: the status, the time of the
latest event, the reported progress, the error, and the blockers. Both keys
are required on every entry, and an absent one is not ``null``: the entry
records nothing to derive a status from, so its status is ``unknown`` and
nothing is contradicted. Each absent key is reported as missing on its own,
and the entry's other fields are still read, each with its own problems.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from yggdrasil.contract.fields import (
    ADVICE_FIELD,
    BLOCKED,
    CODE_FIELD,
    DIRECT_BLOCKERS_FIELD,
    ERROR_FIELD,
    ERROR_KINDS,
    ERROR_TYPE_FIELD,
    EVENT_STEP_PROGRESS,
    FAILED_ANCESTORS_FIELD,
    KIND_FIELD,
    OUTCOME_EVENTS,
    OUTCOME_FIELD,
    PROGRESS_FIELD,
    STARTED,
    STATE_FIELD,
    STEP_NAME_FIELD,
    STEP_OUTCOMES,
    STEP_STATES,
    STEPS_FIELD,
    TS_FIELD,
    UNKNOWN,
)
from yggdrasil.contract.problems import STEP_RECORD_CONTRADICTION, ContractProblem
from yggdrasil.contract.read import _values
from yggdrasil.contract.read._results import (
    ObservedStep,
    StepError,
    StepRef,
    StepStatus,
)

# The event that records each step outcome.
_EVENT_OF_OUTCOME = {outcome: event for event, outcome in OUTCOME_EVENTS.items()}

# The prefix of every step lifecycle event's type, a realm's own included.
_STEP_EVENT_PREFIX = "step."


def step_status(state: object, outcome: object) -> StepStatus:
    """Derive a step's status from its recorded state and outcome.

    The rule, row by row:

    - a step outcome beside the event that records it: that outcome;
    - a step outcome beside any other state: ``unknown``, and the record
      contradicts itself;
    - an outcome that is present but not a step outcome, of any type:
      ``unknown``;
    - no outcome (``null``) in state ``pending``, ``interrupted``,
      ``unreached`` or ``unknown``: that word;
    - no outcome beside a terminal event (``step.succeeded``,
      ``step.skipped``, ``step.failed``, ``step.blocked``): ``unknown``, and
      the record contradicts itself;
    - no outcome beside another ``step.*`` event: ``started``;
    - no outcome in any other state, one that is not text included:
      ``unknown``.

    ``started`` is recorded past activity, not proof that the step is active
    now; a lifecycle event a realm defines reads as ``started`` too. Both
    values are checked to be text before either is looked up, so a list or
    an object is never hashed or compared as a word.

    A value helper: it checks no document version, and never raises. It
    takes recorded values, so None is ``null``. An entry without its
    ``state`` or ``outcome`` key records no value at all, which is not
    ``null``: :func:`~yggdrasil.contract.read.read_snapshot` reads such an
    entry as ``unknown``, with no contradiction, without this rule.

    Args:
        state: The entry's recorded ``state``, of any type.
        outcome: The entry's recorded ``outcome``, of any type; None for
            ``null``.

    Returns:
        StepStatus: The status, and the contradiction, naming both recorded
        values, when the record contradicts itself.
    """
    if outcome is not None:
        if not isinstance(outcome, str) or outcome not in STEP_OUTCOMES:
            return StepStatus(UNKNOWN, None)
        if not _values.is_word(state, _EVENT_OF_OUTCOME[outcome]):
            return StepStatus(
                UNKNOWN,
                f"the outcome {_values.shown(outcome)} contradicts the state "
                f"{_values.shown(state)}",
            )
        return StepStatus(outcome, None)
    if not isinstance(state, str):
        return StepStatus(UNKNOWN, None)
    if state in STEP_STATES:
        return StepStatus(state, None)
    if state in OUTCOME_EVENTS:
        return StepStatus(
            UNKNOWN,
            f"the state {_values.shown(state)} is a terminal event, but the "
            "outcome is null",
        )
    if state.startswith(_STEP_EVENT_PREFIX):
        return StepStatus(STARTED, None)
    return StepStatus(UNKNOWN, None)


def observed_steps(
    steps: Mapping[str, Mapping[str, Any]],
    *,
    settled: bool | None,
    problems: list[ContractProblem],
) -> tuple[ObservedStep, ...]:
    """Read every entry of a snapshot's ``steps``, whose structure is checked.

    Args:
        steps: The entries, by step ID: each ID a non-empty string, each
            entry an object whose blocker lists are lists of non-empty
            strings.
        settled: Whether the snapshot's attempt is settled; None for a legacy
            projection or an attempt in an unknown state.
        problems: Where problems are appended.

    Returns:
        tuple[ObservedStep, ...]: The steps, in stored order.
    """
    known = set(steps)
    return tuple(
        observed_step(step_id, entry, known=known, settled=settled, problems=problems)
        for step_id, entry in steps.items()
    )


def observed_step(
    step_id: str,
    entry: Mapping[str, Any],
    *,
    known: set[str],
    settled: bool | None,
    problems: list[ContractProblem],
) -> ObservedStep:
    """Read one step entry, each answer from the fields it depends on.

    - status and contradiction from ``state`` and ``outcome``
      (:func:`step_status`); an entry without either key is ``unknown`` and
      contradicts nothing, each absent key reported as ``missing_field`` on
      its own;
    - the time of the latest event from ``ts``, read tolerantly; ``null`` is
      no time and no problem;
    - progress from ``progress``, only in state ``step.progress``: the
      projection writes 0 or 100 beside every other event, which report no
      progress;
    - the error from ``error``, in either stored shape;
    - for a blocked step only, its direct blockers, and, once the attempt is
      settled, its failed upstream steps, which only the report records in
      full.

    A problem with one field leaves every other answer available.

    Args:
        step_id: The step's ID.
        entry: Its entry, an object whose blocker lists are checked.
        known: Every step ID of the snapshot's inventory.
        settled: Whether the attempt is settled.
        problems: Where problems are appended.

    Returns:
        ObservedStep: The step.
    """
    path: _values.Path = (STEPS_FIELD, step_id)
    state = _state(entry, path, problems)
    outcome = _outcome(entry, path, problems)
    if STATE_FIELD in entry and OUTCOME_FIELD in entry:
        status = step_status(state, outcome)
    else:
        # An absent key records nothing, which is not null: there is nothing
        # to derive a status from, and nothing to contradict.
        status = StepStatus(UNKNOWN, None)
    if status.contradiction is not None:
        problems.append(
            _values.problem(
                STEP_RECORD_CONTRADICTION,
                path,
                f"{_values.subject(path)}: {status.contradiction}",
            )
        )
    progress = None
    if _values.is_word(state, EVENT_STEP_PROGRESS):
        progress = _values.percentage(entry, PROGRESS_FIELD, path, problems).value
    direct_blockers: tuple[StepRef, ...] | None = None
    failed_upstream: tuple[StepRef, ...] | None = None
    if status.status == BLOCKED:
        direct_blockers = _refs(entry[DIRECT_BLOCKERS_FIELD], known)
        if settled is True:
            failed_upstream = _refs(entry[FAILED_ANCESTORS_FIELD], known)
    return ObservedStep(
        step_id=step_id,
        name=_values.text(entry, STEP_NAME_FIELD, path, problems, required=True).value,
        status=status.status,
        contradiction=status.contradiction,
        recorded_state=state,
        recorded_outcome=outcome,
        recorded_at=_values.timestamp(
            entry, TS_FIELD, path, problems, required=True, nullable=True
        ).value,
        progress_percent=progress,
        error=step_error(entry, path, problems),
        direct_blockers=direct_blockers,
        failed_upstream=failed_upstream,
        recorded=entry,
    )


def step_error(
    entry: Mapping[str, Any], path: _values.Path, problems: list[ContractProblem]
) -> StepError | None:
    """Read a step entry's ``error``, one reading for both of its stored shapes.

    While the attempt runs, the error is the ``step.failed`` event's (its
    message as ``error``, ``kind``, ``code`` and ``advice``), whose values
    are kept as the step gave them. Once the attempt has ended, it is the
    report's failure record, which also names ``error_type``. Both shapes
    are read alike. The message is text, or ``null`` for none; any other
    value is unknown and of the wrong type, reported at the error's own
    ``error`` key. ``error_type`` is text or ``null`` the same way.
    ``kind``, ``code`` and ``advice`` are open: a value of any type is
    unknown but no problem.

    Args:
        entry: The step entry.
        path: The entry's path.
        problems: Where problems are appended.

    Returns:
        StepError | None: The error; None for ``null``, and for a value that
        is not an object.
    """
    error_path = (*path, ERROR_FIELD)
    if ERROR_FIELD not in entry:
        problems.append(_values.missing(error_path))
        return None
    error = entry[ERROR_FIELD]
    if error is None:
        return None
    if not isinstance(error, Mapping):
        problems.append(
            _values.wrong_type(error_path, error, _values.or_null(_values.OBJECT, True))
        )
        return None

    def open_text(key: str) -> str | None:
        """Read one of the error's open fields, kept as the step gave it."""
        return _values.text(
            error, key, error_path, problems, required=True, typed=False
        ).value

    return StepError(
        message=_values.text(
            error, ERROR_FIELD, error_path, problems, required=True, nullable=True
        ).value,
        kind=_values.word(
            error, KIND_FIELD, error_path, problems, ERROR_KINDS, typed=False
        ),
        code=open_text(CODE_FIELD),
        advice=open_text(ADVICE_FIELD),
        error_type=_values.text(
            error, ERROR_TYPE_FIELD, error_path, problems, nullable=True
        ).value,
    )


def _state(
    entry: Mapping[str, Any], path: _values.Path, problems: list[ContractProblem]
) -> object:
    """Return an entry's recorded state, reporting one that is absent or not text."""
    state_path = (*path, STATE_FIELD)
    if STATE_FIELD not in entry:
        problems.append(_values.missing(state_path))
        return None
    state = entry[STATE_FIELD]
    if not isinstance(state, str):
        problems.append(_values.wrong_type(state_path, state, _values.STRING))
    return state


def _outcome(
    entry: Mapping[str, Any], path: _values.Path, problems: list[ContractProblem]
) -> object:
    """Return an entry's recorded outcome, reporting one that is absent or not text."""
    outcome_path = (*path, OUTCOME_FIELD)
    if OUTCOME_FIELD not in entry:
        problems.append(_values.missing(outcome_path))
        return None
    outcome = entry[OUTCOME_FIELD]
    if outcome is not None and not isinstance(outcome, str):
        problems.append(
            _values.wrong_type(
                outcome_path, outcome, _values.or_null(_values.STRING, True)
            )
        )
    return outcome


def _refs(step_ids: list[str], known: set[str]) -> tuple[StepRef, ...]:
    """Return step IDs as references, each marked with whether it is known."""
    return tuple(
        StepRef(step_id=step_id, found=step_id in known) for step_id in step_ids
    )
