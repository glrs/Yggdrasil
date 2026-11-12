"""Checks on the control fields that decide whether and how a plan runs.

A missing, null or malformed control field is invalid control state, never a
default: there is no "an absent ``run_token`` means 0", and no "an absent
failure policy means ``fail_fast``". Each function below reads one part of
that state from a stored plan document and returns its verdict together with
one description per problem, naming the field and the recorded value:

- :func:`token_pair`: the execution tokens, under the strict token rule;
- :func:`failure_policy_of`: the plan's failure policy;
- :func:`execution_definition_check`: whether the plan and its steps carry
  every execution field, before deserialization can fill one in with a
  default. :func:`execution_definition_problems` returns only its messages.

Each result carries its problems twice: ``problems``, the descriptions that
the daemon logs, and ``details``, the same problems as
:class:`~yggdrasil.contract.problems.ContractProblem` records, so a caller can
take a problem's path and code without parsing a message. Both hold the same
problems in the same order, and each detail's ``message`` is the description at
its position in ``problems``.

Stored content never makes them raise, whatever its shape, so every caller
that has to decide something about a stored plan gets the same answer from
the same rule.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from yggdrasil.contract.diagnostics import describe_recorded
from yggdrasil.contract.fields import (
    DEPS_FIELD,
    EXECUTED_RUN_TOKEN_FIELD,
    FAILURE_POLICIES,
    FAILURE_POLICY_FIELD,
    FN_REF_FIELD,
    INPUTS_FIELD,
    NAME_FIELD,
    OUTPUTS_FIELD,
    PARAMS_FIELD,
    PLAN_FIELD,
    PLAN_ID_FIELD,
    REALM_FIELD,
    RUN_TOKEN_FIELD,
    SCOPE_FIELD,
    STEP_ID_FIELD,
    STEPS_FIELD,
)
from yggdrasil.contract.problems import (
    INVALID_EXECUTION_DEFINITION,
    INVALID_FAILURE_POLICY,
    INVALID_TOKENS,
    ContractProblem,
    path_text,
)

# The token bounds. ``executed_run_token`` is -1 until a first request is
# recorded. The upper bound, 2**53 - 1, is the largest integer that every JSON
# client, browsers included, represents exactly.
MIN_RUN_TOKEN = 0
MIN_EXECUTED_RUN_TOKEN = -1
MAX_TOKEN = 2**53 - 1

# The fields the plan and each of its steps must carry. Deserialization
# supplies a default for most of them, so their presence is checked first.
PLAN_DEFINITION_FIELDS = (
    PLAN_ID_FIELD,
    REALM_FIELD,
    SCOPE_FIELD,
    STEPS_FIELD,
    FAILURE_POLICY_FIELD,
)
STEP_DEFINITION_FIELDS = (
    STEP_ID_FIELD,
    NAME_FIELD,
    FN_REF_FIELD,
    PARAMS_FIELD,
    DEPS_FIELD,
    SCOPE_FIELD,
    INPUTS_FIELD,
    OUTPUTS_FIELD,
)


@dataclass(frozen=True)
class TokenPair:
    """A plan document's execution tokens, and whether they form a valid pair.

    The pair is valid when both tokens are present JSON integers that are not
    booleans, ``0 <= run <= MAX_TOKEN``, ``-1 <= executed <= MAX_TOKEN``, and
    ``executed <= run``. A decision about the request, such as whether it
    finished or whether the plan is eligible, uses ``run`` and ``executed``
    only when ``valid``. Comparing identity and showing a value may use a
    token that is valid on its own while the pair is not.

    Attributes:
        valid: Whether the pair is valid.
        run: ``run_token``, if it is valid on its own; None otherwise.
        executed: ``executed_run_token``, if it is valid on its own; None
            otherwise.
        problems: One description per problem; empty when the pair is valid.
        details: The same problems as records, with the path of the field and
            the code ``invalid_tokens``; the document itself, with an empty
            path, when it is not an object.
    """

    valid: bool
    run: int | None
    executed: int | None
    problems: tuple[str, ...]
    details: tuple[ContractProblem, ...]


@dataclass(frozen=True)
class FailurePolicyCheck:
    """A plan document's failure policy, and whether it is a known one.

    Attributes:
        valid: Whether ``plan.failure_policy`` is present and one of
            FAILURE_POLICIES.
        policy: The policy, when valid; None otherwise.
        problems: One description per problem; empty when valid.
        details: The same problems as records, with code
            ``invalid_failure_policy`` and the path ``plan.failure_policy``,
            or ``plan`` when the plan is missing, null or not an object, or
            an empty path when the document is not an object.
    """

    valid: bool
    policy: str | None
    problems: tuple[str, ...]
    details: tuple[ContractProblem, ...]


@dataclass(frozen=True)
class DefinitionCheck:
    """Whether a plan document carries every field of its execution definition.

    Attributes:
        valid: Whether the definition is complete.
        problems: One description per problem; empty when valid.
        details: The same problems as records, with code
            ``invalid_execution_definition`` and the path of the field:
            ``plan.<field>``, ``plan.steps``, ``plan.steps[<i>]`` or
            ``plan.steps[<i>].<field>``; ``plan`` when the plan is missing,
            null or not an object, or an empty path when the document is not
            an object.
    """

    valid: bool
    problems: tuple[str, ...]
    details: tuple[ContractProblem, ...]


def token_pair(document: object) -> TokenPair:
    """Read a plan document's execution tokens under the strict token rule.

    The same rule judges a stored pair and the pair a write would produce, so
    a caller that is about to change one token passes the document as it
    would be written.

    Args:
        document: The plan document, of any shape.

    Returns:
        TokenPair: The tokens and the verdict; never raises on stored content.
    """
    if not isinstance(document, Mapping):
        problem = _not_expected((), document, "an object", INVALID_TOKENS)
        return _invalid_pair((problem,))
    run, run_problem = _token(document, RUN_TOKEN_FIELD, MIN_RUN_TOKEN)
    executed, executed_problem = _token(
        document, EXECUTED_RUN_TOKEN_FIELD, MIN_EXECUTED_RUN_TOKEN
    )
    details = [p for p in (run_problem, executed_problem) if p is not None]
    if run is not None and executed is not None and executed > run:
        details.append(
            _problem(
                INVALID_TOKENS,
                (EXECUTED_RUN_TOKEN_FIELD,),
                f"{EXECUTED_RUN_TOKEN_FIELD} {executed} is above "
                f"{RUN_TOKEN_FIELD} {run}",
            )
        )
    return TokenPair(
        valid=not details,
        run=run,
        executed=executed,
        problems=_messages(details),
        details=tuple(details),
    )


def failure_policy_of(document: object) -> FailurePolicyCheck:
    """Read a plan document's failure policy, with no default.

    Valid only when ``plan.failure_policy`` is present and one of
    FAILURE_POLICIES, matched exactly: ``"Fail_Fast"`` is unknown.

    Args:
        document: The plan document, of any shape.

    Returns:
        FailurePolicyCheck: The policy and the verdict; never raises on stored
        content.
    """
    plan = _plan_of(document, INVALID_FAILURE_POLICY)
    if isinstance(plan, ContractProblem):
        return _invalid_policy(plan)
    path = (PLAN_FIELD, FAILURE_POLICY_FIELD)
    absent = _absent(plan, FAILURE_POLICY_FIELD, path, INVALID_FAILURE_POLICY)
    if absent is not None:
        return _invalid_policy(absent)
    value = plan[FAILURE_POLICY_FIELD]
    if not isinstance(value, str):
        return _invalid_policy(
            _not_expected(path, value, "a string", INVALID_FAILURE_POLICY)
        )
    if value not in FAILURE_POLICIES:
        known = sorted(FAILURE_POLICIES)
        return _invalid_policy(
            _problem(
                INVALID_FAILURE_POLICY,
                path,
                f"{path_text(path)} {describe_recorded(value)} is not one of {known}",
            )
        )
    return FailurePolicyCheck(valid=True, policy=value, problems=(), details=())


def execution_definition_problems(document: object) -> tuple[str, ...]:
    """Return what is missing from a plan document's execution definition.

    The descriptions of :func:`execution_definition_check`, for a caller that
    needs only the text.

    Args:
        document: The plan document, of any shape.

    Returns:
        tuple[str, ...]: One problem per missing field, naming its path, such
        as ``plan.steps[2].deps is missing``; empty when the definition is
        complete. Never raises on stored content.
    """
    return execution_definition_check(document).problems


def execution_definition_check(document: object) -> DefinitionCheck:
    """Check that a plan document carries its whole execution definition.

    Checks presence, not values: ``plan`` is an object carrying every field of
    PLAN_DEFINITION_FIELDS, ``plan.steps`` is a list of objects, and each step
    carries every field of STEP_DEFINITION_FIELDS. A field holding ``null``
    counts as missing. What the values hold (types inside containers, step
    IDs, dependency targets) is left to deserialization and preflight.

    Without this check, deserialization fills the gaps with defaults: a plan
    without ``steps`` becomes an empty plan that succeeds, and a step without
    ``deps`` loses its ordering.

    Args:
        document: The plan document, of any shape.

    Returns:
        DefinitionCheck: One problem per missing field, naming its path, such
        as ``plan.steps[2].deps is missing``; none when the definition is
        complete. Never raises on stored content.
    """
    code = INVALID_EXECUTION_DEFINITION
    plan = _plan_of(document, code)
    if isinstance(plan, ContractProblem):
        return _definition((plan,))
    details = _absent_fields(plan, PLAN_DEFINITION_FIELDS, (PLAN_FIELD,), code)
    steps = plan.get(STEPS_FIELD)
    if steps is None:  # missing or null, already reported
        return _definition(details)
    if not isinstance(steps, list):
        details.append(
            _not_expected((PLAN_FIELD, STEPS_FIELD), steps, "an array", code)
        )
        return _definition(details)
    for index, step in enumerate(steps):
        path = (PLAN_FIELD, STEPS_FIELD, index)
        if isinstance(step, Mapping):
            details.extend(_absent_fields(step, STEP_DEFINITION_FIELDS, path, code))
        else:
            details.append(_not_expected(path, step, "an object", code))
    return _definition(details)


def _token(
    document: Mapping[str, Any], field: str, minimum: int
) -> tuple[int | None, ContractProblem | None]:
    """Return one token if it is valid on its own, else the problem with it."""
    path = (field,)
    absent = _absent(document, field, path, INVALID_TOKENS)
    if absent is not None:
        return None, absent
    value = document[field]
    # bool is a subclass of int, and 5.0 == 5: only a true int is a token.
    if type(value) is not int:
        return None, _not_expected(path, value, "a JSON integer", INVALID_TOKENS)
    if value < minimum:
        message = f"{field} {describe_recorded(value)} is below {minimum}"
        return None, _problem(INVALID_TOKENS, path, message)
    if value > MAX_TOKEN:
        message = f"{field} {describe_recorded(value)} is above the maximum {MAX_TOKEN}"
        return None, _problem(INVALID_TOKENS, path, message)
    return value, None


def _invalid_pair(details: tuple[ContractProblem, ...]) -> TokenPair:
    """Return the verdict on a document with no tokens to read."""
    return TokenPair(
        valid=False,
        run=None,
        executed=None,
        problems=_messages(details),
        details=details,
    )


def _invalid_policy(detail: ContractProblem) -> FailurePolicyCheck:
    """Return the verdict on a failure policy that is not valid."""
    return FailurePolicyCheck(
        valid=False, policy=None, problems=(detail.message,), details=(detail,)
    )


def _definition(details: Sequence[ContractProblem]) -> DefinitionCheck:
    """Return the verdict on an execution definition, given its problems."""
    return DefinitionCheck(
        valid=not details, problems=_messages(details), details=tuple(details)
    )


def _plan_of(document: object, code: str) -> Mapping[str, Any] | ContractProblem:
    """Return a plan document's ``plan`` object, or the problem preventing it."""
    if not isinstance(document, Mapping):
        return _not_expected((), document, "an object", code)
    absent = _absent(document, PLAN_FIELD, (PLAN_FIELD,), code)
    if absent is not None:
        return absent
    plan = document[PLAN_FIELD]
    if not isinstance(plan, Mapping):
        return _not_expected((PLAN_FIELD,), plan, "an object", code)
    return plan


def _absent_fields(
    container: Mapping[str, Any],
    fields: tuple[str, ...],
    parent: tuple[str | int, ...],
    code: str,
) -> list[ContractProblem]:
    """Return a problem for each of fields that container lacks or holds as null."""
    return [
        problem
        for field in fields
        if (problem := _absent(container, field, (*parent, field), code)) is not None
    ]


def _absent(
    container: Mapping[str, Any],
    field: str,
    path: tuple[str | int, ...],
    code: str,
) -> ContractProblem | None:
    """Return the problem if field is missing or null in container, else None."""
    if field not in container:
        return _problem(code, path, f"{path_text(path)} is missing")
    if container[field] is None:
        return _problem(code, path, f"{path_text(path)} is null")
    return None


def _not_expected(
    path: tuple[str | int, ...], value: object, expected: str, code: str
) -> ContractProblem:
    """Describe a recorded value of the wrong kind, with its type.

    An empty path names the plan document itself.
    """
    name = type(value).__name__
    article = "an" if name[:1].lower() in "aeiou" else "a"
    subject = path_text(path) or "the plan document"
    message = (
        f"{subject} {describe_recorded(value)} is {article} {name}, not {expected}"
    )
    return _problem(code, path, message)


def _problem(code: str, path: tuple[str | int, ...], message: str) -> ContractProblem:
    """Return a problem at path, which is empty for the document itself."""
    return ContractProblem(path_text(path), code, message)


def _messages(details: Sequence[ContractProblem]) -> tuple[str, ...]:
    """Return the description of each problem, in order."""
    return tuple(detail.message for detail in details)
