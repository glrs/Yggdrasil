"""Reading single recorded values, and describing what is wrong with one.

Each reader takes the object holding a field, the field's key and the path of
that object, and returns a :class:`Recorded` value: what the reader recognised
beside what was stored. A problem with the value is appended to the list the
caller passes, so "absent" and "unreadable" are never confused, and nothing is
coerced, defaulted or guessed. Equality and recognition are typed: ``"5"`` is
not the integer 5, and ``true`` is not the integer 1.

By default a reader reads a field as the plan document defines it: ``null`` is
of the wrong type, or for a timestamp no timestamp. A snapshot records
``null`` in several fields on purpose, such as an attempt's outcome while it
runs, so the readers of those fields opt in with ``nullable``: ``null`` is
then recorded, recognises nothing, and is no problem. A field whose type the
contract leaves open, recorded as another program gave it, opts out of type
checks with ``typed=False``: a value of any type is then unknown but no
problem.

Messages show a recorded value through
:func:`~yggdrasil.contract.diagnostics.describe_recorded` only, and name the
path through :func:`subject`, so they are bounded and building one never
raises.
"""

from __future__ import annotations

from collections.abc import Mapping, Set
from datetime import datetime
from typing import Any

from yggdrasil.contract import timestamps, versions
from yggdrasil.contract.controls import MAX_TOKEN
from yggdrasil.contract.diagnostics import MAX_LENGTH, describe_recorded
from yggdrasil.contract.fields import (
    BLOCKED,
    DETAILS_FIELD,
    ERROR_TYPE_FIELD,
    FAILED,
    MESSAGE_FIELD,
    REUSED,
    SUCCEEDED,
    UNREACHED,
)
from yggdrasil.contract.problems import (
    BAD_TIMESTAMP,
    INVALID_VERSION,
    MISSING_FIELD,
    OUT_OF_RANGE,
    UNSUPPORTED_VERSION,
    UNVERSIONED,
    WRONG_TYPE,
    ContractProblem,
    path_text,
)
from yggdrasil.contract.read._results import AttemptDiagnostic, Recorded, StepCounts

# A path into a document: object keys and list indexes.
Path = tuple[str | int, ...]

# The largest integer every JSON client, browsers included, reads exactly: the
# bound of a run token, and of a step count.
MAX_EXACT_INTEGER = MAX_TOKEN

# The bounds of a progress percentage.
MIN_PERCENT = 0
MAX_PERCENT = 100

# The keys of a report's counts, in the order StepCounts holds them.
_COUNT_KEYS = (SUCCEEDED, REUSED, FAILED, BLOCKED, UNREACHED)

# The problem code of each version state that makes a document inspect-only.
INSPECT_ONLY_CODES = {
    versions.UNVERSIONED: UNVERSIONED,
    versions.INVALID: INVALID_VERSION,
    versions.UNSUPPORTED: UNSUPPORTED_VERSION,
}

# What a field of each contract type is expected to hold, for messages.
NON_EMPTY_STRING = "a non-empty string"
STRING = "a string"
OBJECT = "an object"
ARRAY = "an array"
INTEGER = "a JSON integer"
BOOLEAN = "a boolean"
PERCENTAGE = f"a number from {MIN_PERCENT} to {MAX_PERCENT}"


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


def subject(path: Path) -> str:
    """Name a path in a message, bounded like a recorded value.

    A snapshot's paths hold its step IDs, which are stored keys of any
    length, so a message names a path shortened as
    :func:`~yggdrasil.contract.diagnostics.describe_recorded` shortens a
    value. The problem's own ``path`` stays exact.

    Args:
        path: The path.

    Returns:
        str: The path as :func:`~yggdrasil.contract.problems.path_text`
        renders it, cut to at most MAX_LENGTH characters.
    """
    text = path_text(path)
    if len(text) > MAX_LENGTH:
        return text[: MAX_LENGTH - 3] + "..."
    return text


def shown(value: object) -> str:
    """Show a recorded value in a message, ``null`` as JSON spells it.

    Args:
        value: The recorded value, of any type.

    Returns:
        str: ``null`` for None; otherwise the value as
        :func:`~yggdrasil.contract.diagnostics.describe_recorded` shows it.
    """
    return "null" if value is None else describe_recorded(value)


def missing(path: Path) -> ContractProblem:
    """Return the problem of a field the contract requires and the document lacks.

    Args:
        path: The field's path.

    Returns:
        ContractProblem: A ``missing_field`` problem.
    """
    return problem(MISSING_FIELD, path, f"{subject(path)} is missing")


def wrong_type(path: Path, value: object, expected: str) -> ContractProblem:
    """Return the problem of a recorded value that is not of its field's type.

    Args:
        path: The value's path; empty for the document itself.
        value: The recorded value, of any type.
        expected: What the field holds, such as ``a non-empty string``.

    Returns:
        ContractProblem: A ``wrong_type`` problem naming the value and its type.
    """
    return problem(
        WRONG_TYPE, path, not_of_type(subject(path) or "the document", value, expected)
    )


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


def or_null(expected: str, nullable: bool) -> str:
    """Return what a field is expected to hold, saying so when null is valid too.

    Args:
        expected: What a value of the field holds, such as ``a string``.
        nullable: Whether ``null`` is valid as well.

    Returns:
        str: expected, followed by ``or null`` when nullable.
    """
    return f"{expected} or null" if nullable else expected


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


def null() -> Recorded[Any]:
    """Return the record of a field that holds ``null``, where null is valid."""
    return Recorded(None, None, True)


def is_identifier(value: object) -> bool:
    """Return whether value counts as an ID, a generation or a word: a non-empty string.

    Args:
        value: A recorded value, of any type.

    Returns:
        bool: True for a non-empty string only.
    """
    return isinstance(value, str) and value != ""


def is_word(value: object, word: str) -> bool:
    """Return whether value is exactly the text word, never comparing other types.

    Args:
        value: A recorded value, of any type.
        word: The word.

    Returns:
        bool: True only for a string equal to word.
    """
    return isinstance(value, str) and value == word


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
    """Read a field that holds an ID, a generation or another non-empty string.

    Empty text is of the wrong type: it identifies nothing.

    Args:
        container: The object holding the field.
        key: The field.
        parent: The path of container.
        problems: Where a problem is appended.
        required: Whether an absent field is a problem.
        nullable: Whether ``null`` is a valid value, recognising nothing; it
            is of the wrong type otherwise.

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
        return null()
    if is_identifier(value):
        return Recorded(value, value, True)
    problems.append(wrong_type(path, value, or_null(NON_EMPTY_STRING, nullable)))
    return Recorded(None, value, True)


def text(
    container: Mapping[str, Any],
    key: str,
    parent: Path,
    problems: list[ContractProblem],
    *,
    required: bool = False,
    nullable: bool = False,
    typed: bool = True,
) -> Recorded[str]:
    """Read a field of free text, such as a name, a note or a run-once owner.

    Empty text is valid but says nothing, so it is recognised as no value,
    without a problem.

    Args:
        container: The object holding the field.
        key: The field.
        parent: The path of container.
        problems: Where a problem is appended.
        required: Whether an absent field is a problem.
        nullable: Whether ``null`` is a valid value: recorded, recognising
            no text, without a problem.
        typed: Whether the contract gives the field a type. When False, as
            for a value kept as another program gave it, a value that is not
            text is unknown but no problem.

    Returns:
        Recorded[str]: The text, recognised when it is not empty.
    """
    path = (*parent, key)
    if key not in container:
        if required:
            problems.append(missing(path))
        return absent()
    value = container[key]
    if value is None and nullable:
        return null()
    if isinstance(value, str):
        return Recorded(value or None, value, True)
    if typed:
        problems.append(wrong_type(path, value, or_null(STRING, nullable)))
    return Recorded(None, value, True)


def word(
    container: Mapping[str, Any],
    key: str,
    parent: Path,
    problems: list[ContractProblem],
    vocabulary: Set[str],
    *,
    required: bool = True,
    nullable: bool = False,
    typed: bool = True,
) -> Recorded[str]:
    """Read a field that holds a word of a vocabulary.

    A word the vocabulary does not hold is unknown: it is kept as recorded,
    is not recognised, and is not a problem, so one new word costs one answer.
    A value that is not text is a problem, unless the field's type is open.
    Only text is ever looked up, so a list or an object is never hashed or
    compared as if it were a word.

    Args:
        container: The object holding the field.
        key: The field.
        parent: The path of container.
        problems: Where a problem is appended.
        vocabulary: The words recognised, matched exactly.
        required: Whether an absent field is a problem.
        nullable: Whether ``null`` is a valid value, recognising nothing; it
            is of the wrong type otherwise.
        typed: Whether the contract gives the field a type. When False, as
            for a value kept as another program gave it, a value that is not
            text is unknown but no problem.

    Returns:
        Recorded[str]: The word, recognised only when vocabulary holds it.
    """
    path = (*parent, key)
    if key not in container:
        if required:
            problems.append(missing(path))
        return absent()
    value = container[key]
    if value is None and nullable:
        return null()
    if not isinstance(value, str):
        if typed:
            problems.append(wrong_type(path, value, or_null(STRING, nullable)))
        return Recorded(None, value, True)
    return Recorded(value if value in vocabulary else None, value, True)


def timestamp(
    container: Mapping[str, Any],
    key: str,
    parent: Path,
    problems: list[ContractProblem],
    *,
    required: bool,
    nullable: bool = False,
) -> Recorded[datetime]:
    """Read a timestamp field into the instant it names, in UTC.

    The value is read by
    :func:`~yggdrasil.contract.timestamps.parse_timestamp`, more tolerantly
    than writers write it. A present value naming no instant is
    ``bad_timestamp``; so is ``null``, unless the field is nullable.

    Args:
        container: The object holding the field.
        key: The field.
        parent: The path of container.
        problems: Where a problem is appended.
        required: Whether an absent field is a problem.
        nullable: Whether ``null`` is a valid value: no time, without a
            problem.

    Returns:
        Recorded[datetime]: The instant, recognised when the value names one.
    """
    path = (*parent, key)
    if key not in container:
        if required:
            problems.append(missing(path))
        return absent()
    value = container[key]
    if value is None and nullable:
        return null()
    instant = timestamps.parse_timestamp(value)
    if instant is None:
        problems.append(
            problem(
                BAD_TIMESTAMP,
                path,
                f"{subject(path)} {shown(value)} is not a timestamp naming an instant",
            )
        )
    return Recorded(instant, value, True)


def run_token(
    container: Mapping[str, Any],
    key: str,
    parent: Path,
    problems: list[ContractProblem],
    *,
    nullable: bool = False,
) -> Recorded[int]:
    """Read a required run token on its own, outside a token pair.

    Args:
        container: The object holding the field.
        key: The field.
        parent: The path of container.
        problems: Where a problem is appended.
        nullable: Whether ``null`` is a valid value, recognising no token; it
            is of the wrong type otherwise.

    Returns:
        Recorded[int]: The token, recognised only as an integer from 0 to
        MAX_EXACT_INTEGER that is not a boolean.
    """
    path = (*parent, key)
    if key not in container:
        problems.append(missing(path))
        return absent()
    value = container[key]
    if value is None and nullable:
        return null()
    if is_run_token(value):
        return Recorded(value, value, True)
    if type(value) is int:
        problems.append(
            problem(
                OUT_OF_RANGE,
                path,
                f"{subject(path)} {describe_recorded(value)} is not from 0 to "
                f"{MAX_EXACT_INTEGER}",
            )
        )
    else:
        problems.append(wrong_type(path, value, or_null(INTEGER, nullable)))
    return Recorded(None, value, True)


def boolean(
    container: Mapping[str, Any],
    key: str,
    parent: Path,
    problems: list[ContractProblem],
    *,
    nullable: bool = False,
) -> Recorded[bool]:
    """Read a required field that holds ``true`` or ``false``.

    Args:
        container: The object holding the field.
        key: The field.
        parent: The path of container.
        problems: Where a problem is appended.
        nullable: Whether ``null`` is a valid value, recognising nothing; it
            is of the wrong type otherwise.

    Returns:
        Recorded[bool]: The boolean, recognised only as a JSON boolean: the
        integers 0 and 1 are not booleans.
    """
    path = (*parent, key)
    if key not in container:
        problems.append(missing(path))
        return absent()
    value = container[key]
    if value is None and nullable:
        return null()
    if type(value) is bool:
        return Recorded(value, value, True)
    problems.append(wrong_type(path, value, or_null(BOOLEAN, nullable)))
    return Recorded(None, value, True)


def percentage(
    container: Mapping[str, Any],
    key: str,
    parent: Path,
    problems: list[ContractProblem],
) -> Recorded[float]:
    """Read a required progress percentage, taken as recorded.

    The value is a number that is not a boolean, from 0 to 100, which
    excludes the infinities and NaN. It is neither rounded nor clamped, and
    never converted: the range is checked first, which holds for an integer
    of any size, so even one too large to be a float is judged without
    converting it.

    Args:
        container: The object holding the field.
        key: The field.
        parent: The path of container.
        problems: Where a problem is appended.

    Returns:
        Recorded[float]: The number as recorded, an int or a float,
        recognised only from 0 to 100.
    """
    path = (*parent, key)
    if key not in container:
        problems.append(missing(path))
        return absent()
    value = container[key]
    if isinstance(value, bool) or not isinstance(value, int | float):
        problems.append(wrong_type(path, value, PERCENTAGE))
        return Recorded(None, value, True)
    # False for NaN, and for either infinity.
    if not MIN_PERCENT <= value <= MAX_PERCENT:
        problems.append(
            problem(
                OUT_OF_RANGE,
                path,
                f"{subject(path)} {describe_recorded(value)} is not from "
                f"{MIN_PERCENT} to {MAX_PERCENT}",
            )
        )
        return Recorded(None, value, True)
    return Recorded(value, value, True)


def counts(
    container: Mapping[str, Any],
    key: str,
    parent: Path,
    problems: list[ContractProblem],
    *,
    nullable: bool = False,
) -> StepCounts | None:
    """Read a required set of step counts, all five or none.

    One problem, at the counts' own path, names the first count that cannot
    be read.

    Args:
        container: The object holding the counts.
        key: The field holding them.
        parent: The path of container.
        problems: Where a problem is appended.
        nullable: Whether ``null`` is a valid value, standing for no counts;
            it is of the wrong type otherwise.

    Returns:
        StepCounts | None: The counts, each an integer from 0 to
        MAX_EXACT_INTEGER that is not a boolean; None if any is not, or for a
        valid ``null``.
    """
    path = (*parent, key)
    if key not in container:
        problems.append(missing(path))
        return None
    value = container[key]
    if value is None and nullable:
        return None
    if not isinstance(value, Mapping):
        problems.append(wrong_type(path, value, or_null(OBJECT, nullable)))
        return None
    read: list[int] = []
    for count_key in _COUNT_KEYS:
        fault = _count_fault(value, count_key, path)
        if fault is not None:
            problems.append(fault)
            return None
        read.append(value[count_key])
    return StepCounts(*read)


def diagnostic(
    container: Mapping[str, Any],
    key: str,
    parent: Path,
    problems: list[ContractProblem],
) -> AttemptDiagnostic | None:
    """Read a required diagnostic, which ``null`` records as none.

    ``null`` is no diagnostic and no problem: the writers record it for most
    attempts, and always while one runs. Within an object, the message is
    text, the error type text or ``null``, and the details an object; each is
    read on its own, so one unreadable field costs that field only.

    Args:
        container: The object holding the diagnostic.
        key: The field holding it.
        parent: The path of container.
        problems: Where a problem is appended.

    Returns:
        AttemptDiagnostic | None: The diagnostic; None for ``null``, and for
        a value that is not an object.
    """
    path = (*parent, key)
    if key not in container:
        problems.append(missing(path))
        return None
    value = container[key]
    if value is None:
        return None
    if not isinstance(value, Mapping):
        problems.append(wrong_type(path, value, or_null(OBJECT, True)))
        return None
    message = text(value, MESSAGE_FIELD, path, problems, required=True)
    error_type = text(
        value, ERROR_TYPE_FIELD, path, problems, required=True, nullable=True
    )
    details: Mapping[str, Any] | None = None
    details_path = (*path, DETAILS_FIELD)
    if DETAILS_FIELD not in value:
        problems.append(missing(details_path))
    elif isinstance(value[DETAILS_FIELD], Mapping):
        details = value[DETAILS_FIELD]
    else:
        problems.append(wrong_type(details_path, value[DETAILS_FIELD], OBJECT))
    return AttemptDiagnostic(
        message=message.value, error_type=error_type.value, details=details
    )


def _count_fault(
    counts_value: Mapping[str, Any], key: str, path: Path
) -> ContractProblem | None:
    """Return the problem with one count, reported at the counts' path, or None."""
    shown_path = subject((*path, key))
    if key not in counts_value:
        return problem(MISSING_FIELD, path, f"{shown_path} is missing")
    value = counts_value[key]
    if type(value) is not int:
        return problem(WRONG_TYPE, path, not_of_type(shown_path, value, INTEGER))
    if not 0 <= value <= MAX_EXACT_INTEGER:
        return problem(
            OUT_OF_RANGE,
            path,
            f"{shown_path} {describe_recorded(value)} is not from 0 to "
            f"{MAX_EXACT_INTEGER}",
        )
    return None
