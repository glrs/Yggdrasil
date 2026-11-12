"""Reading single recorded values, and describing what is wrong with one.

Each reader takes the object holding a field, the field's key and the path of
that object, and returns a :class:`Recorded` value: what the reader recognised
beside what was stored. A problem with the value is appended to the list the
caller passes, so "absent" and "unreadable" are never confused, and nothing is
coerced, defaulted or guessed. Equality and recognition are typed: ``"5"`` is
not the integer 5, and ``true`` is not the integer 1.

Messages show a recorded value through
:func:`~yggdrasil.contract.diagnostics.describe_recorded` only, so they are
bounded and building one never raises.
"""

from __future__ import annotations

from collections.abc import Mapping, Set
from datetime import datetime
from typing import Any

from yggdrasil.contract import timestamps
from yggdrasil.contract.controls import MAX_TOKEN
from yggdrasil.contract.diagnostics import describe_recorded
from yggdrasil.contract.fields import BLOCKED, FAILED, REUSED, SUCCEEDED, UNREACHED
from yggdrasil.contract.problems import (
    BAD_TIMESTAMP,
    MISSING_FIELD,
    OUT_OF_RANGE,
    WRONG_TYPE,
    ContractProblem,
    path_text,
)
from yggdrasil.contract.read._results import Recorded, StepCounts

# A path into a document: object keys and list indexes.
Path = tuple[str | int, ...]

# The largest integer every JSON client, browsers included, reads exactly: the
# bound of a run token, and of a step count.
MAX_EXACT_INTEGER = MAX_TOKEN

# The keys of a report's counts, in the order StepCounts holds them.
_COUNT_KEYS = (SUCCEEDED, REUSED, FAILED, BLOCKED, UNREACHED)

# What a field of each contract type is expected to hold, for messages.
NON_EMPTY_STRING = "a non-empty string"
STRING = "a string"
OBJECT = "an object"
ARRAY = "an array"
INTEGER = "a JSON integer"


def problem(code: str, path: Path, message: str) -> ContractProblem:
    """Return a problem at path, which is empty for the document itself.

    Args:
        code: The problem code.
        path: Where in the document.
        message: The description.

    Returns:
        ContractProblem: The problem, its path rendered as text.
    """
    return ContractProblem(path_text(path), code, message)


def missing(path: Path) -> ContractProblem:
    """Return the problem of a field the contract requires and the document lacks.

    Args:
        path: The field's path.

    Returns:
        ContractProblem: A ``missing_field`` problem.
    """
    return problem(MISSING_FIELD, path, f"{path_text(path)} is missing")


def wrong_type(path: Path, value: object, expected: str) -> ContractProblem:
    """Return the problem of a recorded value that is not of its field's type.

    Args:
        path: The value's path; empty for the document itself.
        value: The recorded value, of any type.
        expected: What the field holds, such as ``a non-empty string``.

    Returns:
        ContractProblem: A ``wrong_type`` problem naming the value and its type.
    """
    subject = path_text(path) or "the document"
    return problem(WRONG_TYPE, path, not_of_type(subject, value, expected))


def not_of_type(subject: str, value: object, expected: str) -> str:
    """Say that a recorded value is not of the type expected, naming its type.

    Args:
        subject: What holds the value, such as ``realm`` or ``the plan
            document``.
        value: The recorded value, of any type.
        expected: What it should be, such as ``an object``.

    Returns:
        str: The description, with the value shown bounded.
    """
    if value is None:
        return f"{subject} is null, not {expected}"
    if isinstance(value, str) and not value:
        return f"{subject} is an empty string, not {expected}"
    name = type(value).__name__
    article = "an" if name[:1].lower() in "aeiou" else "a"
    return f"{subject} {describe_recorded(value)} is {article} {name}, not {expected}"


def require_argument(name: str, value: object) -> str:
    """Return an ID a caller passed, refusing one that is not a non-empty string.

    The argument comes from the caller's code, not from stored content, so a
    wrong one is a programming error.

    Args:
        name: The parameter's name, for the message.
        value: The argument.

    Returns:
        str: value itself.

    Raises:
        TypeError: If value is not a string.
        ValueError: If value is empty.
    """
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string, not {type(value).__name__}")
    if not value:
        raise ValueError(f"{name} must not be empty")
    return value


def absent() -> Recorded[Any]:
    """Return the record of a field that is not recorded at all."""
    return Recorded(None, None, False)


def is_identifier(value: object) -> bool:
    """Return whether value counts as an ID, a generation or a word: a non-empty string.

    Args:
        value: A recorded value, of any type.

    Returns:
        bool: True for a non-empty string only.
    """
    return isinstance(value, str) and value != ""


def is_run_token(value: object) -> bool:
    """Return whether value counts as a run token on its own.

    Args:
        value: A recorded value, of any type.

    Returns:
        bool: True for an integer from 0 to MAX_EXACT_INTEGER that is not a
        boolean.
    """
    # bool is a subclass of int, and 5.0 == 5: only a true int is a token.
    return type(value) is int and 0 <= value <= MAX_EXACT_INTEGER


def identifier(
    container: Mapping[str, Any],
    key: str,
    parent: Path,
    problems: list[ContractProblem],
    *,
    required: bool = True,
    nullable: bool = False,
) -> Recorded[str]:
    """Read a field that holds an ID or another non-empty string.

    Args:
        container: The object holding the field.
        key: The field.
        parent: The path of container.
        problems: Where a problem is appended.
        required: Whether an absent field is a problem.
        nullable: Whether ``null`` is a valid value, read as no value.

    Returns:
        Recorded[str]: The string, recognised only when it is not empty.
    """
    path = (*parent, key)
    if key not in container:
        if required:
            problems.append(missing(path))
        return absent()
    value = container[key]
    if value is None and nullable:
        return Recorded(None, None, True)
    if is_identifier(value):
        return Recorded(value, value, True)
    expected = f"{NON_EMPTY_STRING} or null" if nullable else NON_EMPTY_STRING
    problems.append(wrong_type(path, value, expected))
    return Recorded(None, value, True)


def text(
    container: Mapping[str, Any],
    key: str,
    parent: Path,
    problems: list[ContractProblem],
) -> Recorded[str]:
    """Read an optional field of free text, such as a name or a note.

    An absent field is no value and no problem. Empty text is valid but says
    nothing, so it is recognised as no value, without a problem.

    Args:
        container: The object holding the field.
        key: The field.
        parent: The path of container.
        problems: Where a problem is appended.

    Returns:
        Recorded[str]: The text, recognised when it is not empty.
    """
    if key not in container:
        return absent()
    value = container[key]
    if isinstance(value, str):
        return Recorded(value or None, value, True)
    problems.append(wrong_type((*parent, key), value, STRING))
    return Recorded(None, value, True)


def word(
    container: Mapping[str, Any],
    key: str,
    parent: Path,
    problems: list[ContractProblem],
    vocabulary: Set[str],
    *,
    required: bool = True,
) -> Recorded[str]:
    """Read a field that holds a word of a vocabulary.

    A word the vocabulary does not hold is unknown: it is kept as recorded,
    is not recognised, and is not a problem, so one new word costs one answer.
    A value that is not text is a problem.

    Args:
        container: The object holding the field.
        key: The field.
        parent: The path of container.
        problems: Where a problem is appended.
        vocabulary: The words recognised, matched exactly.
        required: Whether an absent field is a problem.

    Returns:
        Recorded[str]: The word, recognised only when vocabulary holds it.
    """
    path = (*parent, key)
    if key not in container:
        if required:
            problems.append(missing(path))
        return absent()
    value = container[key]
    if not isinstance(value, str):
        problems.append(wrong_type(path, value, STRING))
        return Recorded(None, value, True)
    return Recorded(value if value in vocabulary else None, value, True)


def timestamp(
    container: Mapping[str, Any],
    key: str,
    parent: Path,
    problems: list[ContractProblem],
    *,
    required: bool,
) -> Recorded[datetime]:
    """Read a timestamp field into the instant it names, in UTC.

    The value is read by
    :func:`~yggdrasil.contract.timestamps.parse_timestamp`, more tolerantly
    than writers write it. A present value naming no instant, ``null``
    included, is ``bad_timestamp``.

    Args:
        container: The object holding the field.
        key: The field.
        parent: The path of container.
        problems: Where a problem is appended.
        required: Whether an absent field is a problem.

    Returns:
        Recorded[datetime]: The instant, recognised when the value names one.
    """
    path = (*parent, key)
    if key not in container:
        if required:
            problems.append(missing(path))
        return absent()
    value = container[key]
    instant = timestamps.parse_timestamp(value)
    if instant is None:
        shown = "null" if value is None else describe_recorded(value)
        problems.append(
            problem(
                BAD_TIMESTAMP,
                path,
                f"{path_text(path)} {shown} is not a timestamp naming an instant",
            )
        )
    return Recorded(instant, value, True)


def run_token(
    container: Mapping[str, Any],
    key: str,
    parent: Path,
    problems: list[ContractProblem],
) -> Recorded[int]:
    """Read a required run token on its own, outside a token pair.

    Args:
        container: The object holding the field.
        key: The field.
        parent: The path of container.
        problems: Where a problem is appended.

    Returns:
        Recorded[int]: The token, recognised only as an integer from 0 to
        MAX_EXACT_INTEGER that is not a boolean.
    """
    path = (*parent, key)
    if key not in container:
        problems.append(missing(path))
        return absent()
    value = container[key]
    if is_run_token(value):
        return Recorded(value, value, True)
    if type(value) is int:
        problems.append(
            problem(
                OUT_OF_RANGE,
                path,
                f"{path_text(path)} {describe_recorded(value)} is not from 0 to "
                f"{MAX_EXACT_INTEGER}",
            )
        )
    else:
        problems.append(wrong_type(path, value, INTEGER))
    return Recorded(None, value, True)


def counts(
    container: Mapping[str, Any],
    key: str,
    parent: Path,
    problems: list[ContractProblem],
) -> StepCounts | None:
    """Read a required set of step counts, all five or none.

    One problem, at the counts' own path, names the first count that cannot
    be read.

    Args:
        container: The object holding the counts.
        key: The field holding them.
        parent: The path of container.
        problems: Where a problem is appended.

    Returns:
        StepCounts | None: The counts, each an integer from 0 to
        MAX_EXACT_INTEGER that is not a boolean; None if any is not.
    """
    path = (*parent, key)
    if key not in container:
        problems.append(missing(path))
        return None
    value = container[key]
    if not isinstance(value, Mapping):
        problems.append(wrong_type(path, value, OBJECT))
        return None
    read: list[int] = []
    for count_key in _COUNT_KEYS:
        fault = _count_fault(value, count_key, path)
        if fault is not None:
            problems.append(fault)
            return None
        read.append(value[count_key])
    return StepCounts(*read)


def _count_fault(
    counts_value: Mapping[str, Any], key: str, path: Path
) -> ContractProblem | None:
    """Return the problem with one count, reported at the counts' path, or None."""
    shown = path_text((*path, key))
    if key not in counts_value:
        return problem(MISSING_FIELD, path, f"{shown} is missing")
    value = counts_value[key]
    if type(value) is not int:
        return problem(WRONG_TYPE, path, not_of_type(shown, value, INTEGER))
    if not 0 <= value <= MAX_EXACT_INTEGER:
        return problem(
            OUT_OF_RANGE,
            path,
            f"{shown} {describe_recorded(value)} is not from 0 to "
            f"{MAX_EXACT_INTEGER}",
        )
    return None
