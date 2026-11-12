"""Reading a ``plan_status`` snapshot: what it establishes on its own.

A snapshot is the ops consumer's projection of one plan's latest observed
attempt (``projection: attempt``), or, for a plan whose spool holds no
attempt, of each step's latest run from before attempts were recorded
(``projection: legacy``). It is written at another moment than the plan
document, so it may describe an older request or generation.

:func:`read_snapshot` reads in a fixed order, and stops deriving at the first
step a snapshot fails:

1. presence: a body, or the NoDocument the caller passed for none;
2. a JSON object, else invalid as a whole (``wrong_type`` at the root);
3. a supported family, else inspect-only: one problem at
   ``schema_version``, the body passed through untouched, nothing derived;
4. of the plan and realm it was read for (``plan_id`` and ``realm``
   non-empty strings equal to them), else invalid as a whole
   (``identity_mismatch``): a snapshot of another plan is never part of this
   plan's view;
5. structured as a snapshot, else invalid as a whole
   (``invalid_structure``), with every structural fault reported: a partly
   readable step inventory is never returned;
6. everything else, each answer from the fields it depends on, so a problem
   with one field leaves every other answer available.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from yggdrasil.contract import PLAN_STATUS, versions
from yggdrasil.contract.diagnostics import describe_recorded
from yggdrasil.contract.fields import (
    ATTEMPT,
    ATTEMPT_FIELD,
    ATTEMPT_STATES,
    COUNTS_FIELD,
    DIAGNOSTIC_FIELD,
    DIRECT_BLOCKERS_FIELD,
    ENDED_AT_FIELD,
    EXECUTION_AUTHORITIES,
    EXECUTION_AUTHORITY_FIELD,
    EXECUTION_ID_FIELD,
    EXECUTION_OUTCOMES,
    EXECUTION_OWNER_FIELD,
    FAILED_ANCESTORS_FIELD,
    FAILURE_POLICIES,
    FAILURE_POLICY_FIELD,
    FINISHED,
    IS_DRAINED_FIELD,
    LEGACY,
    OUTCOME_FIELD,
    PLAN_GENERATION_FIELD,
    PLAN_ID_FIELD,
    PROJECTION_FIELD,
    REALM_FIELD,
    RUN_TOKEN_FIELD,
    RUNNING,
    SCHEMA_VERSION_FIELD,
    STARTED_AT_FIELD,
    STATE_FIELD,
    STEPS_FIELD,
    TERMINATION_REASON_FIELD,
    TERMINATION_REASONS,
    UPDATED_AT_FIELD,
)
from yggdrasil.contract.problems import (
    IDENTITY_MISMATCH,
    INVALID_STRUCTURE,
    WRONG_TYPE,
    ContractProblem,
)
from yggdrasil.contract.read import _steps, _values
from yggdrasil.contract.read._results import (
    LOOKUP_IMPOSSIBLE,
    PRESENT,
    Attempt,
    NoDocument,
    SnapshotRead,
)

_ROOT: _values.Path = ()
_ATTEMPT_PATH: _values.Path = (ATTEMPT_FIELD,)
_STEPS_PATH: _values.Path = (STEPS_FIELD,)

# What a blocker list holds, for messages.
_STEP_IDS = "an array of step IDs"


def read_snapshot(document: object, *, plan_id: str, realm: str) -> SnapshotRead:
    """Read what a ``plan_status`` snapshot establishes on its own.

    Checks the version first. An unversioned, invalid or unsupported snapshot
    is inspect-only: the result carries its version, one problem at
    ``schema_version`` and the body untouched, and nothing is derived from
    it. A snapshot of another plan or realm, or one not structured as a
    snapshot, is invalid as a whole. Nothing is defaulted. Pure and silent:
    no storage, clock, configuration or logging, and no stored content makes
    it raise.

    Args:
        document: The stored body, of any shape; or MISSING when the store
            confirmed there is none, or UNAVAILABLE when the read failed.
        plan_id: The plan the snapshot is read for.
        realm: The realm the snapshot is read for: the plan's.

    Returns:
        SnapshotRead: What the snapshot establishes, with every problem found.

    Raises:
        TypeError: If plan_id or realm is not a string.
        ValueError: If plan_id or realm is empty, or document is
            LOOKUP_IMPOSSIBLE: a snapshot that could not be looked up has no
            body to read, and goes to
            :func:`~yggdrasil.contract.read.correlate` as it is.
    """
    _values.require_argument("plan_id", plan_id)
    _values.require_argument("realm", realm)
    if isinstance(document, NoDocument):
        if document is LOOKUP_IMPOSSIBLE:
            raise ValueError(
                "read_snapshot takes a body, MISSING or UNAVAILABLE; pass "
                "LOOKUP_IMPOSSIBLE to correlate as it is"
            )
        return _not_usable(
            plan_id,
            realm,
            presence=document.value,
            version=None,
            document=None,
            problems=(),
        )
    version = versions.version_of(PLAN_STATUS, document)
    found: tuple[ContractProblem, ...] = ()
    if not isinstance(document, Mapping):
        found = (not_an_object(document),)
    elif version.state != versions.SUPPORTED:
        found = (inspect_only(version),)
    else:
        found = identity_mismatches(document, plan_id=plan_id, realm=realm)
        if not found:
            found = structure_faults(document)
        if not found:
            return _usable(document, plan_id, realm, version)
    return _not_usable(
        plan_id,
        realm,
        presence=PRESENT,
        version=version,
        document=document,
        problems=found,
    )


def not_an_object(document: object) -> ContractProblem:
    """Return the problem of a snapshot that is not a JSON object.

    Args:
        document: What was stored, of any type other than an object.

    Returns:
        ContractProblem: A ``wrong_type`` problem at the snapshot's root.
    """
    message = _values.not_of_type("the snapshot", document, _values.OBJECT)
    return _values.problem(WRONG_TYPE, _ROOT, message)


def inspect_only(version: versions.VersionInfo) -> ContractProblem:
    """Return the problem of a snapshot that is not of a supported family.

    Args:
        version: The snapshot's version, in a state other than supported.

    Returns:
        ContractProblem: One problem at ``schema_version``, coded
        ``unversioned``, ``invalid_version`` or ``unsupported_version``.
    """
    return _values.problem(
        _values.INSPECT_ONLY_CODES[version.state],
        (SCHEMA_VERSION_FIELD,),
        "the snapshot is inspect-only: "
        + versions.version_sentence(PLAN_STATUS, version),
    )


def identity_mismatches(
    document: Mapping[str, Any], *, plan_id: str, realm: str
) -> tuple[ContractProblem, ...]:
    """Return the problems of a snapshot that is not of the plan it was read for.

    Args:
        document: The snapshot.
        plan_id: The plan it was read for.
        realm: The realm it was read for.

    Returns:
        tuple[ContractProblem, ...]: An ``identity_mismatch`` problem at
        ``plan_id`` and at ``realm`` for each that is not a string equal to
        what the snapshot was read for; empty when both are.
    """
    return tuple(
        problem
        for problem in (
            _mismatch(document, PLAN_ID_FIELD, plan_id, "plan"),
            _mismatch(document, REALM_FIELD, realm, "realm"),
        )
        if problem is not None
    )


def structure_faults(document: Mapping[str, Any]) -> tuple[ContractProblem, ...]:
    """Return every fault that keeps a snapshot from being read as one.

    A snapshot is structured as one when its ``projection`` is ``attempt``,
    with an attempt object holding a non-empty ``execution_id``, or
    ``legacy``, recording no attempt; and its ``steps`` is an object of step
    entries, each under a non-empty step ID, each an object whose
    ``direct_blockers`` and ``failed_ancestors`` are both lists of non-empty
    strings. The blocker lists are checked on every entry, whatever its
    status, as the schema requires them; what they mean is read only for a
    blocked step.

    Args:
        document: A snapshot of a supported family, of the expected plan.

    Returns:
        tuple[ContractProblem, ...]: An ``invalid_structure`` problem per
        fault found; empty when there is none.
    """
    faults: list[ContractProblem] = []
    _projection_faults(document, faults)
    _steps_faults(document, faults)
    return tuple(faults)


def attempt_of(attempt: Mapping[str, Any], problems: list[ContractProblem]) -> Attempt:
    """Read a snapshot's attempt, whose ``execution_id`` is checked.

    ``null`` is a valid value of every field but the ID, the state and the
    policy, and recognises nothing: the attempt's records lack the field, as
    the six fields copied from the report do while the attempt runs. The
    failure policy is recorded as the plan gave it, so a value of any type is
    unknown but no problem. A recorded end beside a ``running`` state is
    returned as recorded: it is what was observed.

    Args:
        attempt: The attempt object.
        problems: Where problems are appended.

    Returns:
        Attempt: The attempt, each field recorded and recognised.
    """
    path = _ATTEMPT_PATH
    state = _values.word(attempt, STATE_FIELD, path, problems, ATTEMPT_STATES)
    return Attempt(
        execution_id=attempt[EXECUTION_ID_FIELD],
        plan_generation=_values.identifier(
            attempt, PLAN_GENERATION_FIELD, path, problems, nullable=True
        ),
        run_token=_values.run_token(
            attempt, RUN_TOKEN_FIELD, path, problems, nullable=True
        ),
        failure_policy=_values.word(
            attempt, FAILURE_POLICY_FIELD, path, problems, FAILURE_POLICIES, typed=False
        ),
        execution_authority=_values.word(
            attempt,
            EXECUTION_AUTHORITY_FIELD,
            path,
            problems,
            EXECUTION_AUTHORITIES,
            nullable=True,
        ),
        execution_owner=_values.text(
            attempt,
            EXECUTION_OWNER_FIELD,
            path,
            problems,
            required=True,
            nullable=True,
        ),
        state=state,
        started_at=_values.timestamp(
            attempt, STARTED_AT_FIELD, path, problems, required=True, nullable=True
        ),
        ended_at=_values.timestamp(
            attempt, ENDED_AT_FIELD, path, problems, required=True, nullable=True
        ),
        termination_reason=_values.word(
            attempt,
            TERMINATION_REASON_FIELD,
            path,
            problems,
            TERMINATION_REASONS,
            nullable=True,
        ),
        outcome=_values.word(
            attempt, OUTCOME_FIELD, path, problems, EXECUTION_OUTCOMES, nullable=True
        ),
        is_drained=_values.boolean(
            attempt, IS_DRAINED_FIELD, path, problems, nullable=True
        ),
        counts=_values.counts(attempt, COUNTS_FIELD, path, problems, nullable=True),
        diagnostic=_values.diagnostic(attempt, DIAGNOSTIC_FIELD, path, problems),
        settled=settled(state.value),
        recorded=attempt,
    )


def settled(state: str | None) -> bool | None:
    """Say whether an attempt in a recognised state has final results.

    Args:
        state: The attempt's state, if recognised.

    Returns:
        bool | None: True for ``finished``, False for ``running``, None for
        an unrecognised state: an outcome alone does not settle an attempt.
    """
    if state == FINISHED:
        return True
    if state == RUNNING:
        return False
    return None


def _usable(
    document: Mapping[str, Any],
    plan_id: str,
    realm: str,
    version: versions.VersionInfo,
) -> SnapshotRead:
    """Read a structured snapshot of a supported family, of the expected plan."""
    found: list[ContractProblem] = []
    written_at = _values.timestamp(
        document, UPDATED_AT_FIELD, _ROOT, found, required=True
    )
    projection = document[PROJECTION_FIELD]
    attempt = (
        attempt_of(document[ATTEMPT_FIELD], found)
        if _values.is_word(projection, ATTEMPT)
        else None
    )
    steps = _steps.observed_steps(
        document[STEPS_FIELD],
        settled=attempt.settled if attempt is not None else None,
        problems=found,
    )
    return SnapshotRead(
        plan_id=plan_id,
        realm=realm,
        presence=PRESENT,
        version=version,
        usable=True,
        document=document,
        problems=tuple(found),
        projection=projection,
        written_at=written_at,
        attempt=attempt,
        steps=steps,
    )


def _not_usable(
    plan_id: str,
    realm: str,
    *,
    presence: str,
    version: versions.VersionInfo | None,
    document: object,
    problems: tuple[ContractProblem, ...],
) -> SnapshotRead:
    """Return the read of a snapshot from which nothing is derived."""
    return SnapshotRead(
        plan_id=plan_id,
        realm=realm,
        presence=presence,
        version=version,
        usable=False,
        document=document,
        problems=problems,
        projection=None,
        written_at=None,
        attempt=None,
        steps=None,
    )


def _mismatch(
    document: Mapping[str, Any], field: str, expected: str, what: str
) -> ContractProblem | None:
    """Return the problem of a field not naming what the snapshot was read for."""
    path = (field,)
    wanted = describe_recorded(expected)
    if field not in document:
        message = f"{field} is missing; the snapshot was read for {what} {wanted}"
    else:
        value = document[field]
        if isinstance(value, str) and value == expected:
            return None
        if value is None:
            message = f"{field} is null; the snapshot was read for {what} {wanted}"
        else:
            message = (
                f"{field} {describe_recorded(value)} is not {wanted}, the {what} "
                "the snapshot was read for"
            )
    return _values.problem(IDENTITY_MISMATCH, path, message)


def _projection_faults(
    document: Mapping[str, Any], faults: list[ContractProblem]
) -> None:
    """Append the faults of a snapshot's projection and of the attempt it needs."""
    path = (PROJECTION_FIELD,)
    if PROJECTION_FIELD not in document:
        faults.append(_fault(path, f"{PROJECTION_FIELD} is missing"))
        return
    projection = document[PROJECTION_FIELD]
    attempt = document.get(ATTEMPT_FIELD)
    if _values.is_word(projection, LEGACY):
        if attempt is not None:
            faults.append(
                _fault(
                    _ATTEMPT_PATH,
                    f"{ATTEMPT_FIELD} {describe_recorded(attempt)} is recorded, but "
                    f"a {LEGACY} projection records no attempt",
                )
            )
        return
    if not _values.is_word(projection, ATTEMPT):
        faults.append(
            _fault(
                path,
                f"{PROJECTION_FIELD} {_values.shown(projection)} is neither "
                f"'{ATTEMPT}' nor '{LEGACY}'",
            )
        )
        return
    if ATTEMPT_FIELD not in document:
        faults.append(
            _fault(
                _ATTEMPT_PATH,
                f"{ATTEMPT_FIELD} is missing, which an {ATTEMPT} projection records",
            )
        )
    elif not isinstance(attempt, Mapping):
        faults.append(
            _fault(
                _ATTEMPT_PATH,
                _values.not_of_type(ATTEMPT_FIELD, attempt, _values.OBJECT),
            )
        )
    else:
        id_path = (*_ATTEMPT_PATH, EXECUTION_ID_FIELD)
        if EXECUTION_ID_FIELD not in attempt:
            faults.append(_fault(id_path, f"{_values.subject(id_path)} is missing"))
        elif not _values.is_identifier(attempt[EXECUTION_ID_FIELD]):
            faults.append(
                _fault(
                    id_path,
                    _values.not_of_type(
                        _values.subject(id_path),
                        attempt[EXECUTION_ID_FIELD],
                        _values.NON_EMPTY_STRING,
                    ),
                )
            )


def _steps_faults(document: Mapping[str, Any], faults: list[ContractProblem]) -> None:
    """Append the faults of a snapshot's step inventory."""
    if STEPS_FIELD not in document:
        faults.append(_fault(_STEPS_PATH, f"{STEPS_FIELD} is missing"))
        return
    steps = document[STEPS_FIELD]
    if not isinstance(steps, Mapping):
        faults.append(
            _fault(_STEPS_PATH, _values.not_of_type(STEPS_FIELD, steps, _values.OBJECT))
        )
        return
    for step_id, entry in steps.items():
        if not isinstance(step_id, str):
            # No JSON object has such a key; it has no path of its own.
            faults.append(
                _fault(
                    _STEPS_PATH,
                    _values.not_of_type(
                        f"a step ID in {STEPS_FIELD}", step_id, _values.NON_EMPTY_STRING
                    ),
                )
            )
            continue
        path = (*_STEPS_PATH, step_id)
        if not step_id:
            faults.append(
                _fault(
                    path,
                    _values.not_of_type(
                        f"a step ID in {STEPS_FIELD}", step_id, _values.NON_EMPTY_STRING
                    ),
                )
            )
        if not isinstance(entry, Mapping):
            faults.append(
                _fault(
                    path,
                    _values.not_of_type(_values.subject(path), entry, _values.OBJECT),
                )
            )
            continue
        for key in (DIRECT_BLOCKERS_FIELD, FAILED_ANCESTORS_FIELD):
            _blocker_faults(entry, key, path, faults)


def _blocker_faults(
    entry: Mapping[str, Any],
    key: str,
    parent: _values.Path,
    faults: list[ContractProblem],
) -> None:
    """Append the fault of a blocker list that is not a list of step IDs."""
    path = (*parent, key)
    if key not in entry:
        faults.append(_fault(path, f"{_values.subject(path)} is missing"))
        return
    step_ids = entry[key]
    if not isinstance(step_ids, list):
        faults.append(
            _fault(
                path, _values.not_of_type(_values.subject(path), step_ids, _STEP_IDS)
            )
        )
        return
    for index, step_id in enumerate(step_ids):
        if not _values.is_identifier(step_id):
            # The first such entry stands for the list.
            item = (*path, index)
            faults.append(
                _fault(
                    item,
                    _values.not_of_type(
                        _values.subject(item), step_id, _values.NON_EMPTY_STRING
                    ),
                )
            )
            return


def _fault(path: _values.Path, message: str) -> ContractProblem:
    """Return a structural fault at path."""
    return _values.problem(INVALID_STRUCTURE, path, message)
