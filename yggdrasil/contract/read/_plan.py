"""Reading a plan document: what it establishes on its own.

:func:`read_plan` reads in a fixed order, and stops deriving at the first
step a document fails:

1. presence: a body, or the NoDocument the caller passed for none;
2. a JSON object, else invalid as a whole (``wrong_type`` at the root);
3. a supported family, else inspect-only: one problem at
   ``schema_version``, the body passed through untouched, nothing derived;
4. stored under the ID it was read under (``_id`` a non-empty string equal
   to it), else invalid as a whole (``identity_mismatch`` at ``_id``): a
   body never establishes its own identity;
5. everything else, each answer from the fields it depends on, so a problem
   with one field leaves every other answer available.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from yggdrasil.contract import PLAN_DOCUMENT, controls, versions
from yggdrasil.contract.diagnostics import describe_recorded
from yggdrasil.contract.fields import (
    APPROVAL_STATUSES,
    APPROVED_AT_FIELD,
    APPROVED_BY_FIELD,
    CREATED_AT_FIELD,
    DEPS_FIELD,
    EXECUTION_AUTHORITIES,
    EXECUTION_AUTHORITY_FIELD,
    EXECUTION_OWNER_FIELD,
    ID_FIELD,
    LAST_EXECUTED_AT_FIELD,
    NAME_FIELD,
    NOTES_FIELD,
    PLAN_FIELD,
    PLAN_GENERATION_FIELD,
    REALM_FIELD,
    RUN_REQUESTED_AT_FIELD,
    RUN_REQUESTED_BY_FIELD,
    SCOPE_FIELD,
    SCOPE_ID_FIELD,
    SCOPE_KIND_FIELD,
    STATUS_FIELD,
    STEP_ID_FIELD,
    STEPS_FIELD,
    UPDATED_AT_FIELD,
)
from yggdrasil.contract.problems import (
    INVALID_EXECUTION_DEFINITION,
    ContractProblem,
    path_text,
)
from yggdrasil.contract.read import _eligibility, _ids, _requests, _values
from yggdrasil.contract.read._results import (
    IDENTITY_MISMATCH,
    LOOKUP_IMPOSSIBLE,
    PRESENT,
    Eligibility,
    NoDocument,
    PlanFields,
    PlannedStep,
    PlanRead,
    Recorded,
    RequestState,
    StepRef,
)

_ROOT: _values.Path = ()
_STEPS_PATH: _values.Path = (PLAN_FIELD, STEPS_FIELD)


def read_plan(document: object, *, plan_id: str) -> PlanRead:
    """Read what a plan document establishes on its own.

    Checks the version first. An unversioned, invalid or unsupported document
    is inspect-only: the result carries its version, one problem at
    ``schema_version`` and the body untouched, and nothing is derived from
    it. Nothing is defaulted: an absent control field is a problem and an
    unknown answer. Pure and silent: no storage, clock, configuration or
    logging, and no stored content makes it raise.

    Args:
        document: The stored body, of any shape; or MISSING when the store
            confirmed there is none, or UNAVAILABLE when the read failed.
        plan_id: The ID the document was requested or stored under.

    Returns:
        PlanRead: What the document establishes, with every problem found.

    Raises:
        TypeError: If plan_id is not a string.
        ValueError: If plan_id is empty, or document is LOOKUP_IMPOSSIBLE,
            which only a snapshot can be.
    """
    _values.require_argument("plan_id", plan_id)
    if isinstance(document, NoDocument):
        if document is LOOKUP_IMPOSSIBLE:
            raise ValueError(
                "read_plan takes a body, MISSING or UNAVAILABLE; a plan is "
                "always looked up by its own ID"
            )
        return _not_usable(
            plan_id,
            presence=document.value,
            version=None,
            document=None,
            problems=(),
            eligibility=None,
        )
    version = versions.version_of(PLAN_DOCUMENT, document)
    if not isinstance(document, Mapping) or version.state != versions.SUPPORTED:
        refusal = _eligibility.eligibility(document)
        return _not_usable(
            plan_id,
            presence=PRESENT,
            version=version,
            document=document,
            problems=refusal.problems,
            eligibility=refusal,
        )
    mismatch = identity_mismatch(document, plan_id)
    if mismatch is not None:
        refusal = Eligibility(
            eligible=False,
            code=IDENTITY_MISMATCH,
            reason=f"Not eligible: {mismatch.message}",
            problems=(mismatch,),
        )
        return _not_usable(
            plan_id,
            presence=PRESENT,
            version=version,
            document=document,
            problems=(mismatch,),
            eligibility=refusal,
        )
    return _usable(document, plan_id, version)


def request_state(document: object, *, plan_id: str) -> RequestState | None:
    """Return a plan document's current-request state, for a caller holding a body.

    The request state of :func:`read_plan`, which checks the version first. A
    caller that already holds a PlanRead uses its ``request``. The expected ID
    is required: whether the finalized record belongs to the current request
    is judged against the ID the plan was read under, never the body's own.

    Args:
        document: The stored body, of any shape, MISSING or UNAVAILABLE.
        plan_id: The ID the document was requested or stored under.

    Returns:
        RequestState | None: The state; None when the document is not usable.

    Raises:
        TypeError: If plan_id is not a string.
        ValueError: If plan_id is empty, or document is LOOKUP_IMPOSSIBLE.
    """
    return read_plan(document, plan_id=plan_id).request


def identity_mismatch(
    document: Mapping[str, Any], plan_id: str
) -> ContractProblem | None:
    """Return the problem of a document not stored under the ID it was read under.

    Args:
        document: The plan document.
        plan_id: The ID it was read under.

    Returns:
        ContractProblem | None: An ``identity_mismatch`` problem at ``_id``
        unless ``_id`` is a string equal to plan_id.
    """
    path = (ID_FIELD,)
    expected = describe_recorded(plan_id)
    if ID_FIELD not in document:
        message = f"{ID_FIELD} is missing; the plan was read under {expected}"
    else:
        value = document[ID_FIELD]
        if isinstance(value, str) and value == plan_id:
            return None
        if value is None:
            message = f"{ID_FIELD} is null; the plan was read under {expected}"
        else:
            message = (
                f"{ID_FIELD} {describe_recorded(value)} is not {expected}, the ID "
                "the plan was read under"
            )
    return _values.problem(IDENTITY_MISMATCH, path, message)


def plan_fields(
    document: Mapping[str, Any], problems: list[ContractProblem]
) -> PlanFields:
    """Read a plan document's descriptive fields.

    ``realm``, ``scope``, ``status``, ``execution_authority``,
    ``execution_owner``, ``created_at`` and ``updated_at`` are required; the
    rest are optional. A word outside its vocabulary is unknown, kept, and
    not a problem. ``execution_owner`` is text or ``null``: empty text and
    ``null`` name no owner, without a problem.

    Args:
        document: A plan document of a supported family.
        problems: Where problems are appended.

    Returns:
        PlanFields: Each field, recorded and recognised.
    """
    scope_kind, scope_id = _scope(document, problems)
    return PlanFields(
        realm=_values.identifier(document, REALM_FIELD, _ROOT, problems),
        scope_kind=scope_kind,
        scope_id=scope_id,
        status=_values.word(document, STATUS_FIELD, _ROOT, problems, APPROVAL_STATUSES),
        execution_authority=_values.word(
            document, EXECUTION_AUTHORITY_FIELD, _ROOT, problems, EXECUTION_AUTHORITIES
        ),
        execution_owner=_values.text(
            document,
            EXECUTION_OWNER_FIELD,
            _ROOT,
            problems,
            required=True,
            nullable=True,
        ),
        notes=_values.text(document, NOTES_FIELD, _ROOT, problems),
        created_at=_values.timestamp(
            document, CREATED_AT_FIELD, _ROOT, problems, required=True
        ),
        updated_at=_values.timestamp(
            document, UPDATED_AT_FIELD, _ROOT, problems, required=True
        ),
        approved_at=_values.timestamp(
            document, APPROVED_AT_FIELD, _ROOT, problems, required=False
        ),
        approved_by=_values.text(document, APPROVED_BY_FIELD, _ROOT, problems),
        run_requested_at=_values.timestamp(
            document, RUN_REQUESTED_AT_FIELD, _ROOT, problems, required=False
        ),
        run_requested_by=_values.text(
            document, RUN_REQUESTED_BY_FIELD, _ROOT, problems
        ),
        last_executed_at=_values.timestamp(
            document, LAST_EXECUTED_AT_FIELD, _ROOT, problems, required=False
        ),
    )


def planned_steps(
    document: Mapping[str, Any],
    problems: list[ContractProblem],
    named: set[str],
) -> tuple[PlannedStep, ...] | None:
    """Read a plan's planned-step inventory, complete or not at all.

    The inventory cannot be returned when ``plan`` or ``plan.steps`` is
    absent, null or of the wrong type, a step is not an object, a step ID is
    not a non-empty string or repeats, or a step's ``deps`` is absent or not
    a list of strings. A partly readable inventory is never returned. An
    explicit empty ``plan.steps`` is a known empty inventory, and an explicit
    empty ``deps`` a step without dependencies.

    Args:
        document: A plan document of a supported family.
        problems: Where problems are appended.
        named: Paths the execution-definition check already reported; a
            problem at one of them is not reported again.

    Returns:
        tuple[PlannedStep, ...] | None: The steps in stored order, each
        dependency marked with whether it names a step of the inventory;
        None when the inventory is not complete.
    """
    found: list[ContractProblem] = []
    steps = _inventory(document, found)
    problems.extend(problem for problem in found if problem.path not in named)
    return steps


def _usable(
    document: Mapping[str, Any], plan_id: str, version: versions.VersionInfo
) -> PlanRead:
    """Read a plan document of a supported family stored under plan_id."""
    found: list[ContractProblem] = []
    recorded = plan_fields(document, found)
    generation = _generation(document, found)
    tokens = controls.token_pair(document)
    found.extend(tokens.details)
    policy = controls.failure_policy_of(document)
    definition = controls.execution_definition_check(document)
    # Both checks report a missing or null plan.failure_policy, and a plan
    # that is missing or not an object: each such path is named once.
    named: set[str] = set()
    for detail in (*policy.details, *definition.details):
        if detail.path not in named:
            named.add(detail.path)
            found.append(detail)
    steps = planned_steps(document, found, named)
    finalized = _requests.finalized_record(document, found)
    return PlanRead(
        plan_id=plan_id,
        presence=PRESENT,
        version=version,
        usable=True,
        document=document,
        problems=tuple(found),
        eligibility=_eligibility.eligibility(document),
        recorded=recorded,
        generation=generation,
        tokens=tokens,
        failure_policy=policy,
        approval=recorded.status,
        request=_requests.current_request(
            tokens, generation, finalized, plan_id=plan_id
        ),
        finalized=finalized,
        steps=steps,
        snapshot_id=_ids.snapshot_id_for(document, plan_id=plan_id),
    )


def _not_usable(
    plan_id: str,
    *,
    presence: str,
    version: versions.VersionInfo | None,
    document: object,
    problems: tuple[ContractProblem, ...],
    eligibility: Eligibility | None,
) -> PlanRead:
    """Return the read of a plan from which nothing is derived."""
    return PlanRead(
        plan_id=plan_id,
        presence=presence,
        version=version,
        usable=False,
        document=document,
        problems=problems,
        eligibility=eligibility,
        recorded=None,
        generation=None,
        tokens=None,
        failure_policy=None,
        approval=None,
        request=None,
        finalized=None,
        steps=None,
        snapshot_id=None,
    )


def _generation(
    document: Mapping[str, Any], problems: list[ContractProblem]
) -> str | None:
    """Return the plan's generation, reporting why there is none."""
    generation = _ids.generation_of(document)
    if generation is None:
        path = (PLAN_GENERATION_FIELD,)
        if PLAN_GENERATION_FIELD not in document:
            problems.append(_values.missing(path))
        else:
            problems.append(
                _values.wrong_type(
                    path, document[PLAN_GENERATION_FIELD], _values.NON_EMPTY_STRING
                )
            )
    return generation


def _scope(
    document: Mapping[str, Any], problems: list[ContractProblem]
) -> tuple[Recorded[str], Recorded[str]]:
    """Return the scope's kind and ID, reporting a scope that cannot hold them."""
    path = (SCOPE_FIELD,)
    if SCOPE_FIELD not in document:
        problems.append(_values.missing(path))
        return _values.absent(), _values.absent()
    scope = document[SCOPE_FIELD]
    if not isinstance(scope, Mapping):
        problems.append(_values.wrong_type(path, scope, _values.OBJECT))
        return _values.absent(), _values.absent()
    return (
        _values.identifier(scope, SCOPE_KIND_FIELD, path, problems),
        _values.identifier(scope, SCOPE_ID_FIELD, path, problems),
    )


def _inventory(
    document: Mapping[str, Any], problems: list[ContractProblem]
) -> tuple[PlannedStep, ...] | None:
    """Read the steps, reporting every fault; None if any makes them incomplete."""
    if PLAN_FIELD not in document:
        problems.append(_values.missing((PLAN_FIELD,)))
        return None
    plan = document[PLAN_FIELD]
    if not isinstance(plan, Mapping):
        problems.append(_values.wrong_type((PLAN_FIELD,), plan, _values.OBJECT))
        return None
    if STEPS_FIELD not in plan:
        problems.append(_values.missing(_STEPS_PATH))
        return None
    steps = plan[STEPS_FIELD]
    if not isinstance(steps, list):
        problems.append(_values.wrong_type(_STEPS_PATH, steps, _values.ARRAY))
        return None
    complete = True
    seen: set[str] = set()
    read: list[tuple[str, str | None, tuple[str, ...]]] = []
    for index, step in enumerate(steps):
        path = (*_STEPS_PATH, index)
        if not isinstance(step, Mapping):
            problems.append(_values.wrong_type(path, step, _values.OBJECT))
            complete = False
            continue
        step_id = _values.identifier(step, STEP_ID_FIELD, path, problems).value
        name = _values.text(step, NAME_FIELD, path, problems).value
        deps = _dependencies(step, path, problems)
        if step_id is not None and step_id in seen:
            problems.append(
                _values.problem(
                    INVALID_EXECUTION_DEFINITION,
                    (*path, STEP_ID_FIELD),
                    f"{path_text((*path, STEP_ID_FIELD))} "
                    f"{describe_recorded(step_id)} repeats an earlier step's ID",
                )
            )
            complete = False
        elif step_id is not None:
            seen.add(step_id)
        if step_id is None or deps is None:
            complete = False
            continue
        read.append((step_id, name, deps))
    if not complete:
        return None
    return tuple(
        PlannedStep(
            step_id=step_id,
            name=name,
            deps=tuple(StepRef(step_id=dep, found=dep in seen) for dep in deps),
        )
        for step_id, name, deps in read
    )


def _dependencies(
    step: Mapping[str, Any], path: _values.Path, problems: list[ContractProblem]
) -> tuple[str, ...] | None:
    """Return a step's dependencies, or None, reporting why, if not a list of strings."""
    deps_path = (*path, DEPS_FIELD)
    if DEPS_FIELD not in step:
        problems.append(_values.missing(deps_path))
        return None
    deps = step[DEPS_FIELD]
    if not isinstance(deps, list):
        problems.append(_values.wrong_type(deps_path, deps, "an array of strings"))
        return None
    for index, dep in enumerate(deps):
        if not isinstance(dep, str):
            # The first such entry stands for the list.
            problems.append(
                _values.wrong_type((*deps_path, index), dep, _values.STRING)
            )
            return None
    return tuple(deps)
