"""The contract's published JSON Schemas, and validation against them.

Each document kind has one schema per family, written by hand in JSON Schema
draft 2020-12 and shipped as package data in ``yggdrasil/contract/schemas/``:
``plan_document.v1.json``, ``plan_status.v1.json`` and
``attempt_report.v1.json``. The attempt report has no ``schema_version`` of
its own: the plan document embeds it, and the snapshot reuses parts of it, by
reference to its ``$id``. Each ``$id`` names a kind and a family, such as
``urn:yggdrasil:contract:attempt_report:1``.

The schemas describe what Yggdrasil's builders write, including reports about
plans that were malformed and attempts that were cancelled or failed to
publish. Objects only Yggdrasil writes are closed: an unknown key is a
problem. The plan document's top level also allows fields whose names start
with ``x_``, which the contract never defines. Realm-defined payloads, and
values a snapshot copies from what a step published, are open.

Problems are reported as :class:`~yggdrasil.contract.problems.ContractProblem`
records with the codes of :mod:`yggdrasil.contract.problems`. This module
re-exports the record and the codes validation reports, so they stay
importable from here, and keeps :class:`ContractError`.

:func:`validate` is a development and test tool: no runtime path calls it,
and readers never use it as a gate. It needs the ``jsonschema`` package from
the ``dev`` extra, which it imports only when called; importing this module
loads nothing beyond the standard library. It adds what JSON Schema cannot
say: an integer is a JSON integer, never ``1.0`` or ``true``; a timestamp
names a date and time that exist; and the plan document's
``executed_run_token`` is not above its ``run_token``. It checks no rule
across fields or documents, such as whether a recorded execution matches the
report it carries.
"""

from __future__ import annotations

import copy
import functools
import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from importlib.resources import files
from typing import Any

from yggdrasil.contract import (
    ATTEMPT_REPORT,
    PLAN_DOCUMENT,
    PLAN_STATUS,
    SUPPORTED_FAMILIES,
    versions,
)
from yggdrasil.contract.controls import token_pair
from yggdrasil.contract.diagnostics import describe_recorded
from yggdrasil.contract.fields import EXECUTED_RUN_TOKEN_FIELD, RUN_TOKEN_FIELD
from yggdrasil.contract.problems import (
    BAD_TIMESTAMP,
    INVALID_TOKENS,
    INVALID_VERSION,
    MISSING_FIELD,
    NOT_IN_VOCABULARY,
    OUT_OF_RANGE,
    SCHEMA_RULE,
    UNEXPECTED_FIELD,
    UNSUPPORTED_VERSION,
    UNVERSIONED,
    VALIDATION_CODES,
    WRONG_TYPE,
    ContractProblem,
    path_text,
)
from yggdrasil.contract.timestamps import is_date_time

# The codes validation reports. The code constants imported above keep the
# names they always had here; ``problems`` defines them.
PROBLEM_CODES = VALIDATION_CODES

# The bundled resource of each kind and family.
_RESOURCES: Mapping[tuple[str, int], str] = {
    (PLAN_DOCUMENT, 1): "plan_document.v1.json",
    (PLAN_STATUS, 1): "plan_status.v1.json",
    (ATTEMPT_REPORT, 1): "attempt_report.v1.json",
}

# The report family that this release's plan documents and snapshots embed.
ATTEMPT_REPORT_FAMILY = 1

# The problem code of each version state other than supported.
_VERSION_CODES = {
    versions.UNVERSIONED: UNVERSIONED,
    versions.INVALID: INVALID_VERSION,
    versions.UNSUPPORTED: UNSUPPORTED_VERSION,
}

# How each JSON type is named in a problem description.
_TYPE_NAMES = {
    "null": "null",
    "boolean": "a boolean",
    "integer": "an integer",
    "number": "a number",
    "string": "a string",
    "array": "an array",
    "object": "an object",
}


class ContractError(Exception):
    """A document does not conform to its contract schema.

    Attributes:
        kind: The document kind it was validated as.
        problems: Every problem found, one per path and code, sorted by path.
    """

    def __init__(self, kind: str, problems: Iterable[ContractProblem]) -> None:
        """Record the problems; the message lists every one.

        Args:
            kind: The document kind it was validated as.
            problems: The problems found, in the order to report them.
        """
        self.kind = kind
        self.problems = tuple(problems)
        listed = "".join(
            f"\n  [{problem.code}] {problem.message}" for problem in self.problems
        )
        count = len(self.problems)
        super().__init__(
            f"The {kind} does not conform to its contract "
            f"({count} problem{'' if count == 1 else 's'}):{listed}"
        )


@dataclass(frozen=True)
class _Problem:
    """A problem while validation is still collecting them.

    Attributes:
        path: Where, as the keys and indexes leading to the value.
        code: Why: one of PROBLEM_CODES.
        detail: What is wrong, as it follows the path and the shown value.
        value: The recorded value, shown in the message; _UNSHOWN for none.
    """

    path: tuple[str | int, ...]
    code: str
    detail: str
    value: Any


# Marks a problem whose message shows no recorded value.
_UNSHOWN = object()


def schema_for(kind: str, family: int) -> dict[str, Any]:
    """Return the published schema of one kind and family.

    Args:
        kind: ``plan_document``, ``plan_status`` or ``attempt_report``.
        family: The family, such as 1.

    Returns:
        dict[str, Any]: A copy of the bundled resource, which the caller may
        change.

    Raises:
        LookupError: If no schema is bundled for that kind and family.
    """
    return copy.deepcopy(_parsed(_resource_name(kind, family)))


def validate(kind: str, document: object) -> None:
    """Check a document against the schema of its kind and family.

    For a plan document or a snapshot, ``schema_version`` is read first. An
    unversioned, invalid or unsupported document has no schema to check it
    against, so its one problem is at ``schema_version`` (code
    ``unversioned``, ``invalid_version`` or ``unsupported_version``). A
    supported family selects its schema. An attempt report is checked
    against the family this release's plan documents embed.

    Every problem is reported, once per path and code. Integers must be JSON
    integers, so ``1.0`` and ``true`` are neither tokens nor counts. A
    timestamp must be spelled canonically and name a date and time that
    exist. A plan document's ``executed_run_token`` must not be above its
    ``run_token``. Schema references resolve only to the bundled resources:
    nothing is ever downloaded.

    Args:
        kind: ``plan_document``, ``plan_status`` or ``attempt_report``.
        document: The document, as stored.

    Raises:
        ContractError: If the document does not conform; it lists every
            problem.
        LookupError: If kind is not a document kind of the contract.
        ModuleNotFoundError: If ``jsonschema`` is not installed (it is in the
            ``dev`` extra).
    """
    if kind == ATTEMPT_REPORT:
        family = ATTEMPT_REPORT_FAMILY
    elif kind in SUPPORTED_FAMILIES:
        version = versions.version_of(kind, document)
        if version.state != versions.SUPPORTED:
            problem = _version_problem(kind, document, version)
            raise ContractError(kind, [_published(problem)])
        assert version.family is not None  # a supported document has one
        family = version.family
    else:
        known = sorted({known_kind for known_kind, _ in _RESOURCES})
        raise LookupError(f"No contract schema for kind {kind!r}; known kinds: {known}")

    validator = _validator(_resource_name(kind, family))
    problems = [
        problem
        for error in validator.iter_errors(document)
        for problem in _problems_of(error)
    ]
    if kind == PLAN_DOCUMENT:
        problems.extend(_token_problems(document))
    if problems:
        raise ContractError(kind, _reported(problems))


def _resource_name(kind: str, family: int) -> str:
    """Return the file name of the bundled schema of one kind and family.

    Args:
        kind: The document kind.
        family: The family; only an int names one, never ``True`` or ``1.0``.

    Returns:
        str: The resource's file name in ``schemas/``.

    Raises:
        LookupError: If no schema is bundled for that kind and family.
    """
    name = _RESOURCES.get((kind, family)) if type(family) is int else None
    if name is None:
        raise LookupError(
            f"No contract schema for kind {kind!r}, family {family!r}; bundled: "
            f"{sorted(_RESOURCES)}"
        )
    return name


@functools.cache
def _parsed(name: str) -> dict[str, Any]:
    """Return one bundled resource, parsed once and shared; never change it.

    Args:
        name: The resource's file name in ``schemas/``.

    Returns:
        dict[str, Any]: The schema.
    """
    resource = files("yggdrasil.contract") / "schemas" / name
    return json.loads(resource.read_text(encoding="utf-8"))


@functools.cache
def _registry() -> Any:
    """Return the registry of every bundled resource, by its ``$id``.

    It has no retrieval function, so a reference to anything else cannot be
    resolved and is an error: validation never downloads a schema.

    Returns:
        referencing.Registry: The registry.
    """
    from referencing import Registry, Resource
    from referencing.jsonschema import DRAFT202012

    return Registry().with_resources(
        (
            schema["$id"],
            Resource.from_contents(schema, default_specification=DRAFT202012),
        )
        for schema in map(_parsed, _RESOURCES.values())
    )


@functools.cache
def _validator(name: str) -> Any:
    """Return the validator for one bundled resource.

    It is JSON Schema 2020-12 with two changes. ``integer`` means a JSON
    integer, as ``type(value) is int`` says: plain JSON Schema accepts
    ``1.0``, and Python counts ``True`` as an int. ``date-time`` is the one
    format checked, and is checked explicitly, by
    :func:`~yggdrasil.contract.timestamps.is_date_time`: whether the date and
    time exist. The canonical spelling is each timestamp's ``pattern``.

    Args:
        name: The resource's file name in ``schemas/``.

    Returns:
        jsonschema.protocols.Validator: The validator.
    """
    from jsonschema import Draft202012Validator, FormatChecker
    from jsonschema.validators import extend

    strict = extend(
        Draft202012Validator,
        type_checker=Draft202012Validator.TYPE_CHECKER.redefine(
            "integer", lambda checker, value: type(value) is int
        ),
    )
    formats = FormatChecker(formats=())
    # A value that is not a string is the type keyword's to judge.
    formats.checks("date-time")(
        lambda value: not isinstance(value, str) or is_date_time(value)
    )
    return strict(_parsed(name), registry=_registry(), format_checker=formats)


def _version_problem(
    kind: str, document: object, version: versions.VersionInfo
) -> _Problem:
    """Describe a plan document or snapshot that has no family to check.

    Args:
        kind: The document kind.
        document: The document.
        version: What its ``schema_version`` says; not supported.

    Returns:
        _Problem: The problem at ``schema_version``.
    """
    path = (versions.SCHEMA_VERSION_FIELD,)
    code = _VERSION_CODES[version.state]
    if version.state == versions.UNVERSIONED:
        detail = "is missing, so the document is unversioned (pre-contract)"
        return _Problem(path, code, detail, _UNSHOWN)
    if version.state == versions.UNSUPPORTED:
        supported = sorted(SUPPORTED_FAMILIES[kind])
        detail = (
            f"names a family this release does not support (supported: {supported})"
        )
        return _Problem(path, code, detail, version.recorded)
    if not isinstance(document, Mapping):
        detail = (
            f"cannot be read: the document is {_type_name(document)}, not an object"
        )
        return _Problem(path, code, detail, _UNSHOWN)
    return _Problem(path, code, "is not a positive integer", version.recorded)


def _token_problems(document: object) -> list[_Problem]:
    """Check the one token rule a schema cannot state: executed is not above run.

    Only when each token is valid on its own: a token's type and range are the
    schema's to report, so nothing is reported twice.

    Args:
        document: The plan document.

    Returns:
        list[_Problem]: An ``invalid_tokens`` problem at
        ``executed_run_token``, or nothing.
    """
    pair = token_pair(document)
    if pair.valid or pair.run is None or pair.executed is None:
        return []
    # With both tokens valid on their own, the pair is invalid by their order.
    detail = f"is above {RUN_TOKEN_FIELD} {pair.run}"
    return [
        _Problem((EXECUTED_RUN_TOKEN_FIELD,), INVALID_TOKENS, detail, pair.executed)
    ]


def _problems_of(error: Any) -> list[_Problem]:
    """Translate one schema error into the problems it stands for.

    Args:
        error: A ``jsonschema`` validation error.

    Returns:
        list[_Problem]: Its problems; at least one.
    """
    path = tuple(error.absolute_path)
    keyword = error.validator
    expected = error.validator_value
    value = error.instance
    if keyword in ("anyOf", "oneOf"):
        return _branch_problems(error, path)
    if keyword == "required":
        return [
            _Problem((*path, name), MISSING_FIELD, "is missing", _UNSHOWN)
            for name in expected
            if name not in value
        ]
    if keyword == "dependentRequired":
        return [
            _Problem(
                (*path, name),
                MISSING_FIELD,
                f"is missing, but {field} is present",
                _UNSHOWN,
            )
            for field, needed in expected.items()
            if field in value
            for name in needed
            if name not in value
        ]
    if keyword == "additionalProperties" and expected is False:
        return [
            _Problem((*path, name), UNEXPECTED_FIELD, detail, _UNSHOWN)
            for name, detail in _unexpected_fields(error.schema, value)
        ]
    if keyword == "type":
        return [_Problem(path, WRONG_TYPE, _type_detail(value, expected), value)]
    if keyword == "enum":
        return [_Problem(path, NOT_IN_VOCABULARY, f"is not one of {expected}", value)]
    if keyword == "const":
        detail = f"is not {describe_recorded(expected)}"
        return [_Problem(path, NOT_IN_VOCABULARY, detail, value)]
    if keyword == "minimum":
        return [_Problem(path, OUT_OF_RANGE, f"is below the minimum {expected}", value)]
    if keyword == "maximum":
        detail = f"is above the maximum {expected}"
        return [_Problem(path, OUT_OF_RANGE, detail, value)]
    if _is_timestamp(error.schema) and keyword in ("pattern", "maxLength", "format"):
        detail = (
            "is not a date and time that exists"
            if keyword == "format"
            else "is not a timestamp spelled canonically: whole seconds, in "
            "UTC, ending in Z, as in 2026-10-22T10:00:00Z"
        )
        return [_Problem(path, BAD_TIMESTAMP, detail, value)]
    if keyword == "pattern":
        detail = f"does not match {describe_recorded(expected)}"
        return [_Problem(path, NOT_IN_VOCABULARY, detail, value)]
    return [_Problem(path, SCHEMA_RULE, f"fails the schema's {keyword} rule", value)]


def _branch_problems(error: Any, path: tuple[str | int, ...]) -> list[_Problem]:
    """Translate the error of a value that fits none of a schema's alternatives.

    An alternative the value fails by type alone, such as ``null`` for a
    string, says nothing about what is wrong with it, and is left out. When
    one alternative is left, its problems are the value's. When none is, the
    value has a type no alternative allows. When several are, and they find
    the same problems, those are reported once; otherwise the value matches
    none of the shapes allowed.

    Args:
        error: An ``anyOf`` or ``oneOf`` error.
        path: Where the value is.

    Returns:
        list[_Problem]: The value's problems.
    """
    branches: dict[Any, list[Any]] = {}
    for suberror in error.context or ():
        branches.setdefault(suberror.relative_schema_path[0], []).append(suberror)
    if not branches:  # a value that fits more than one alternative of a oneOf
        return [_Problem(path, SCHEMA_RULE, "fits more than one shape", error.instance)]
    fitting = [
        suberrors
        for suberrors in branches.values()
        if not all(e.validator == "type" and not e.relative_path for e in suberrors)
    ]
    if not fitting:
        expected = [
            name
            for suberrors in branches.values()
            for suberror in suberrors
            for name in _as_list(suberror.validator_value)
        ]
        detail = _type_detail(error.instance, list(dict.fromkeys(expected)))
        return [_Problem(path, WRONG_TYPE, detail, error.instance)]
    found = [
        [problem for suberror in suberrors for problem in _problems_of(suberror)]
        for suberrors in fitting
    ]
    keys = {frozenset((p.path, p.code) for p in problems) for problems in found}
    if len(keys) == 1:
        return _merged([problem for problems in found for problem in problems])
    detail = "matches none of the shapes allowed here"
    return [_Problem(path, SCHEMA_RULE, detail, error.instance)]


def _merged(problems: list[_Problem]) -> list[_Problem]:
    """Join problems at the same path and code into one, keeping every detail.

    Args:
        problems: The problems.

    Returns:
        list[_Problem]: One problem per path and code, in first-seen order.
    """
    first: dict[tuple[Any, ...], _Problem] = {}
    details: dict[tuple[Any, ...], list[str]] = {}
    for problem in problems:
        key = (problem.path, problem.code)
        first.setdefault(key, problem)
        known = details.setdefault(key, [])
        if problem.detail not in known:
            known.append(problem.detail)
    return [
        _Problem(problem.path, problem.code, " and ".join(details[key]), problem.value)
        for key, problem in first.items()
    ]


def _unexpected_fields(schema: Mapping[str, Any], value: Any) -> list[tuple[str, str]]:
    """Return the keys of an object that its closed schema does not allow.

    Args:
        schema: The object's schema, with ``additionalProperties: false``.
        value: The object.

    Returns:
        list[tuple[str, str]]: Each such key, with the problem's detail.
    """
    named = schema.get("properties", {})
    patterns = list(schema.get("patternProperties", {}))
    if patterns:
        detail = f"is not a contract field, and its name matches none of {patterns}"
    else:
        detail = "is not a contract field"
    return [
        (name, detail)
        for name in value
        if name not in named and not any(re.search(p, name) for p in patterns)
    ]


def _is_timestamp(schema: Any) -> bool:
    """Return whether a (sub)schema is a contract timestamp's definition."""
    return isinstance(schema, Mapping) and schema.get("format") == "date-time"


def _type_detail(value: Any, expected: Any) -> str:
    """Describe a value of the wrong JSON type.

    Args:
        value: The recorded value.
        expected: The JSON type, or types, the schema allows.

    Returns:
        str: Such as ``is a number, not an integer``.
    """
    allowed = " or ".join(
        _TYPE_NAMES.get(name) or str(name) for name in _as_list(expected)
    )
    return f"is {_type_name(value)}, not {allowed}"


def _type_name(value: Any) -> str:
    """Name the JSON type of a Python value, such as ``an array``.

    Args:
        value: The value.

    Returns:
        str: Its JSON type with an article, or its class name for a value
        that is not JSON.
    """
    if value is None:
        return _TYPE_NAMES["null"]
    if isinstance(value, bool):
        return _TYPE_NAMES["boolean"]
    if isinstance(value, int):
        return _TYPE_NAMES["integer"]
    if isinstance(value, float):
        return _TYPE_NAMES["number"]
    if isinstance(value, str):
        return _TYPE_NAMES["string"]
    if isinstance(value, list | tuple):
        return _TYPE_NAMES["array"]
    if isinstance(value, Mapping):
        return _TYPE_NAMES["object"]
    return f"a {type(value).__name__}"


def _as_list(value: Any) -> list[Any]:
    """Return a schema keyword's value as a list."""
    return list(value) if isinstance(value, list) else [value]


def _reported(problems: Iterable[_Problem]) -> list[ContractProblem]:
    """Return the problems as reported: one per path and code, sorted by path.

    Args:
        problems: The problems, possibly repeated.

    Returns:
        list[ContractProblem]: The problems to report.
    """
    unique = _merged(list(problems))
    unique.sort(key=lambda problem: (_path_order(problem.path), problem.code))
    return [_published(problem) for problem in unique]


def _published(problem: _Problem) -> ContractProblem:
    """Return the public form of a problem, with its message."""
    path = path_text(problem.path)
    subject = path or "the document"
    if problem.value is not _UNSHOWN:
        subject = f"{subject} {describe_recorded(problem.value)}"
    return ContractProblem(path, problem.code, f"{subject} {problem.detail}")


def _path_order(path: Iterable[str | int]) -> tuple[tuple[int, str | int], ...]:
    """Return a sort key that orders paths part by part, indexes numerically."""
    return tuple(
        (0, part) if isinstance(part, int) else (1, str(part)) for part in path
    )
