"""Timestamps in plan documents and snapshots: how they are written and read.

Writing and reading are separate questions with separate answers.

**Writing: the canonical spelling.** Every timestamp a Yggdrasil writer
creates in a plan document or a snapshot is an RFC 3339 date-time in UTC,
ending in ``Z``, naming a real date and time in the years 0001 to 9999. It is
to the whole second, ``2026-10-22T10:00:00Z``, or has a fraction of one to six
digits whose last digit is not ``0``, ``2026-10-22T10:00:00.25Z``. So each
instant has one spelling, and two canonical timestamps of the same instant are
also equal as text. :func:`is_canonical` checks the spelling, the schemas
publish it as :data:`CANONICAL_PATTERN`, and :func:`canonical_text` writes an
instant in it. The clocks (``yggdrasil.flow.utils.ygg_time.utcnow_iso``)
write whole seconds: a fraction is written only where a caller supplies one.

**Reading: the instant a value names.** :func:`parse_timestamp` reads a
recorded value more tolerantly than writers write it: any RFC 3339 date-time,
and the timezone-aware ISO 8601 forms that Python's
``datetime.fromisoformat`` reads, converted to UTC. Being canonical is not a
condition for being read. A value that names no instant is unknown, never
guessed: no timezone is assumed, and a fraction finer than a microsecond is
never rounded or truncated. A fraction counts only after ``.`` or ``,``,
which narrows what Python reads (see :func:`parse_timestamp`).

Nothing here reads the clock: every function checks, reads or formats the
value it is given.

:data:`CANONICAL_PATTERN` means the same to Python and to the ECMA-262
regular expressions of JSON Schema. It says ``[0-9]``, not ``\\d``, which in
Python also matches non-ASCII digits. It ends with ``(?![\\s\\S])``, "no
character follows", not with ``$``: in Python, ``$`` also matches just before
a final newline, so a validator applying the pattern with Python's
``re.search``, as ``jsonschema`` does, would let ``"2026-10-22T10:00:00Z\\n"``
through. The checks here also match whole strings, with ``re.fullmatch``.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta, timezone

# The canonical spelling of a whole second, the precision the clocks write. A
# canonical fraction has no strftime directive (``%f`` keeps trailing zeros):
# canonical_text writes it.
CANONICAL_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
CANONICAL_PATTERN = (
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{0,5}[1-9])?Z(?![\s\S])"
)
# The longest canonical timestamp, with six fractional digits:
# ``YYYY-MM-DDTHH:MM:SS.ffffffZ``.
CANONICAL_MAX_LENGTH = 27

_CANONICAL = re.compile(CANONICAL_PATTERN)

# The whole-second part every canonical timestamp starts with, and its format.
_SECONDS_LENGTH = len("YYYY-MM-DDTHH:MM:SS")
_SECONDS_FORMAT = "%Y-%m-%dT%H:%M:%S"

# The ``date-time`` grammar of RFC 3339, section 5.6. ABNF literals are
# case-insensitive, so ``t`` and ``z`` are valid spellings too.
_DATE_TIME = re.compile(
    r"(?P<year>[0-9]{4})-(?P<month>[0-9]{2})-(?P<day>[0-9]{2})"
    r"[Tt](?P<hour>[0-9]{2}):(?P<minute>[0-9]{2}):(?P<second>[0-9]{2})"
    r"(?:\.(?P<fraction>[0-9]+))?"
    r"(?:[Zz]|(?P<sign>[+-])(?P<offset_hour>[0-9]{2}):(?P<offset_minute>[0-9]{2}))"
)

# The UTC suffix earlier releases wrote on plan-document timestamps, and the
# only spelling of theirs that converts once the suffix is ``Z``: whole seconds.
_LEGACY_UTC_SUFFIX = "+00:00"
_LEGACY_WHOLE_SECOND = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z"
)

# Microseconds: the finest precision a datetime holds, and so the most
# fractional digits a value may have to be read.
_MAX_FRACTION_DIGITS = 6

# A fraction with more digits than that, after either separator, anywhere: in
# the time or in the offset.
_EXCESS_FRACTION = re.compile(f"[.,][0-9]{{{_MAX_FRACTION_DIGITS + 1}}}")

# Digits after the seconds of a time or an offset that neither "." nor ","
# introduces: after a colon, as in ``10:20:30:5``, or directly, as in
# ``10:20:307`` and the basic ``1020305``. Python's ISO parser reads them as a
# fraction, and drops a single one before the zone. Extended, they follow a
# ``HH:MM:SS``. Basic, a ``HHMMSS`` followed by more digits makes a run of
# seven digits after a character other than a digit, "." or ",", or of more
# digits than a compact date's eight at the start.
_UNMARKED_FRACTION = re.compile(
    r"[0-9]{2}:[0-9]{2}:[0-9]{2}:?[0-9]|(?<=[^0-9.,])[0-9]{7}|^[0-9]{9}"
)

# A value's trailing UTC offset, when it carries a fraction, read as Python's
# ISO parser reads it: two-digit hours, then optionally minutes and seconds,
# separated by colons throughout or by nothing; the fraction follows the last
# component present and counts in microseconds of the offset.
_FRACTIONAL_OFFSET = re.compile(
    r"(?P<sign>[+-])(?P<hours>[0-9]{2})"
    r"(?:(?P<colon>:?)(?P<minutes>[0-9]{2})(?:(?P=colon)(?P<seconds>[0-9]{2}))?)?"
    r"[.,](?P<fraction>[0-9]{1,6})\Z"
)


def is_canonical(value: object) -> bool:
    """Return whether value is a timestamp in the canonical spelling.

    It has to match CANONICAL_PATTERN, and its date and time have to exist.
    The pattern fixes the field widths, the fraction and the ``Z``; the date
    and time are checked separately, because the pattern alone allows
    ``2026-02-30``, hour 24 or a leap second (``:60``).

    Args:
        value: The value to check, of any type.

    Returns:
        bool: True for a string such as ``2026-10-22T10:00:00Z`` or
        ``2026-10-22T10:00:00.25Z``.
    """
    if not isinstance(value, str) or _CANONICAL.fullmatch(value) is None:
        return False
    try:
        datetime.strptime(value[:_SECONDS_LENGTH], _SECONDS_FORMAT)
    except ValueError:
        return False
    return True


def is_date_time(value: object) -> bool:
    """Return whether value is an RFC 3339 date-time naming a real date and time.

    Any valid spelling counts: ``Z`` or a numeric offset, with or without a
    fraction of a second, with ``T`` and ``Z`` in either case. The date and
    time have to exist (no ``2026-02-30``, no hour 24, a year from 0001), and
    an offset's hours and minutes have to be in range. Forms that ISO 8601
    allows and RFC 3339 does not are rejected: a date alone, a space instead
    of ``T``, the compact ``20261022T100000Z``. So is a leap second (``:60``),
    which Python's datetime cannot represent and no Yggdrasil writer produces.

    Whether the spelling is canonical is a separate question, answered by
    :func:`is_canonical`, and so is which instant a value names, answered by
    :func:`parse_timestamp`.

    Args:
        value: The value to check, of any type.

    Returns:
        bool: True for a valid RFC 3339 date-time string.
    """
    if not isinstance(value, str):
        return False
    match = _DATE_TIME.fullmatch(value)
    if match is None:
        return False
    try:
        datetime(
            int(match["year"]),
            int(match["month"]),
            int(match["day"]),
            int(match["hour"]),
            int(match["minute"]),
            int(match["second"]),
        )
    except ValueError:
        return False
    if match["offset_hour"] is None:
        return True
    return int(match["offset_hour"]) <= 23 and int(match["offset_minute"]) <= 59


def canonical_from_legacy(value: object) -> str | None:
    """Return the canonical spelling of a timestamp an earlier release wrote.

    Earlier releases wrote whole-second UTC times ending in ``Z`` or in
    ``+00:00``. Either one, when it names a real date and time, comes back in
    the canonical spelling of the same instant. Anything else is None: another
    offset (``-00:00`` included), a lowercase ``t`` or ``z``, surrounding
    text, a value that is not a string, and a fraction of a second, even one
    the canonical spelling now allows: earlier releases never wrote one, so a
    fraction says the value came from somewhere else. Nothing is truncated or
    guessed.

    Args:
        value: The recorded value, of any type.

    Returns:
        str | None: The canonical spelling, or None if value is not one of the
        two legacy spellings.
    """
    if not isinstance(value, str):
        return None
    if value.endswith(_LEGACY_UTC_SUFFIX):
        value = value.removesuffix(_LEGACY_UTC_SUFFIX) + "Z"
    if _LEGACY_WHOLE_SECOND.fullmatch(value) is None:
        return None
    return value if is_canonical(value) else None


def parse_timestamp(value: object) -> datetime | None:
    """Return the instant a recorded timestamp names, in UTC, if it names one.

    A value is read when it is either of these, naming a real date and time
    whose conversion to UTC stays within the years 0001 to 9999:

    - an RFC 3339 date-time, in any spelling :func:`is_date_time` accepts:
      ``T`` or ``t``, ``Z``, ``z`` or a numeric offset;
    - a timezone-aware ISO 8601 form that Python's ``datetime.fromisoformat``
      reads, given the text unchanged: a space or a lowercase ``t`` between
      date and time, the compact ``20261106T102030Z``, a week date, a time
      without seconds or minutes, a comma before a fraction, an offset with
      seconds. A lowercase ``z`` is read only as RFC 3339 allows it, so
      ``2026-11-06 10:20:30z`` is None.

    Anything else is None: a value that is not a string; text without a
    timezone, since none is assumed; a fraction of more than six digits,
    after ``.`` or ``,``, in the time or in the offset, since nothing is
    rounded or truncated; digits after the seconds that neither ``.`` nor
    ``,`` introduces (below); text containing a NUL, after which Python's ISO
    parser can stop checking (it reads ``2026-11-06T10:20:30Z\\x00junk``); an
    offset of 24 hours or more; an instant outside the years 0001 to 9999 in
    UTC.

    A fraction is read only after ``.`` or ``,``. Python 3.11 also reads, as
    a fraction, digits that follow the seconds of the time or of the offset
    after a colon (``2026-11-06T10:20:30:1234567+00:00``) or directly
    (``2026-11-06T1020301234567+00:00``, ``+000000000001``). It truncates
    such a fraction beyond six digits, loses it from an offset whose whole
    seconds are zero, and drops a single such digit before the zone. So a
    value with digits there is None: a narrow exception to reading what
    Python reads. Without parsing the date, a colon or a digit used between
    the date and the time can look the same, as in
    ``2026-11-06:10:20:30Z``, and such a value is None too.

    Other forms Python reads are read as Python reads them. For example, one
    stray character before the zone is ignored (``2026-11-06T10:20:30xZ``),
    a fraction after the hour or the minute counts in seconds
    (``2026-11-06T10:20.5Z`` is 10:20:00.5), an offset's minutes and seconds
    roll over (``+05:60`` is ``+06:00``), and an empty fraction is ignored
    (``2026-11-06T10:20:30.Z``).

    The fraction of an offset is kept exactly. Python 3.11 drops it when the
    offset's whole seconds are zero, reading ``+00:00:00.000001`` as UTC, so
    the offset is recomputed from the text whenever it has a fraction. If
    Python reads that offset any other way, the value is None rather than
    either reading.

    Never raises, whatever value is, and neither logs nor changes anything.
    Which values a caller reports as a problem, and how, is the caller's to
    decide.

    Args:
        value: The recorded value, of any type.

    Returns:
        datetime | None: The instant, timezone-aware in UTC with exact
        microseconds; None if value names no instant this reads.
    """
    if not isinstance(value, str) or "\x00" in value:
        return None
    if _EXCESS_FRACTION.search(value) or _UNMARKED_FRACTION.search(value):
        return None
    instant = _rfc3339_instant(value)
    return instant if instant is not None else _iso_instant(value)


def canonical_text(instant: datetime) -> str:
    """Return the canonical spelling of an instant.

    The instant is converted to UTC and written with exact microseconds, with
    trailing zeros of the fraction trimmed and no fraction for a whole
    second: ``2026-10-22T10:00:00Z``, ``2026-10-22T10:00:00.25Z``. The year
    always has four digits, whatever the platform's ``strftime`` would write.
    Neither the clock nor the local timezone is read.

    Args:
        instant: A timezone-aware datetime.

    Returns:
        str: The canonical timestamp of the same instant.

    Raises:
        TypeError: If instant is not a datetime; a date alone is not one.
        ValueError: If instant is naive, so names no instant, or is outside
            the years 0001 to 9999 once converted to UTC.
    """
    if not isinstance(instant, datetime):
        raise TypeError(
            f"canonical_text needs a datetime, not {type(instant).__name__}"
        )
    if instant.utcoffset() is None:
        raise ValueError("canonical_text needs a timezone-aware datetime")
    try:
        utc = instant.astimezone(UTC)
    except OverflowError as error:
        raise ValueError(
            f"{instant.isoformat()} is outside the years 0001 to 9999 in UTC"
        ) from error
    text = (
        f"{utc.year:04d}-{utc.month:02d}-{utc.day:02d}"
        f"T{utc.hour:02d}:{utc.minute:02d}:{utc.second:02d}"
    )
    if utc.microsecond:
        text += "." + f"{utc.microsecond:06d}".rstrip("0")
    return text + "Z"


def _rfc3339_instant(value: str) -> datetime | None:
    """Return the instant of an RFC 3339 date-time in UTC; None for anything else.

    Args:
        value: The recorded text.

    Returns:
        datetime | None: The instant, or None if value is not an RFC 3339
        date-time naming a real, representable instant.
    """
    match = _DATE_TIME.fullmatch(value)
    if match is None:
        return None
    fraction = match["fraction"] or ""
    if len(fraction) > _MAX_FRACTION_DIGITS:
        return None
    offset = timedelta(0)
    if match["sign"] is not None:
        hours, minutes = int(match["offset_hour"]), int(match["offset_minute"])
        if hours > 23 or minutes > 59:
            return None
        offset = timedelta(hours=hours, minutes=minutes)
        if match["sign"] == "-":
            offset = -offset
    try:
        local = datetime(
            int(match["year"]),
            int(match["month"]),
            int(match["day"]),
            int(match["hour"]),
            int(match["minute"]),
            int(match["second"]),
            int(fraction.ljust(_MAX_FRACTION_DIGITS, "0")),
            tzinfo=timezone(offset),
        )
        return local.astimezone(UTC)
    except (OverflowError, ValueError):
        return None


def _iso_instant(value: str) -> datetime | None:
    """Return the instant Python's ISO parser reads in value, in UTC, if aware.

    Args:
        value: The recorded text, unchanged.

    Returns:
        datetime | None: The instant, with an offset's fraction kept, or None
        if the parser refuses value, finds no timezone, or the instant leaves
        the calendar in UTC.
    """
    try:
        parsed = datetime.fromisoformat(value)
    except (OverflowError, ValueError):
        return None
    parsed_offset = parsed.utcoffset()
    if parsed_offset is None:
        return None
    offset = _FRACTIONAL_OFFSET.search(value)
    try:
        if offset is not None:
            exact = _offset_of(offset)
            # What Python 3.11 reads: the exact offset, or UTC when its whole
            # seconds are zero.
            kept = exact if abs(exact) >= timedelta(seconds=1) else timedelta(0)
            if parsed_offset != kept:
                return None
            parsed = parsed.replace(tzinfo=timezone(exact))
        return parsed.astimezone(UTC)
    except (OverflowError, ValueError):
        return None


def _offset_of(match: re.Match[str]) -> timedelta:
    """Return the exact UTC offset a matched fractional offset spells."""
    magnitude = timedelta(
        hours=int(match["hours"]),
        minutes=int(match["minutes"] or 0),
        seconds=int(match["seconds"] or 0),
        microseconds=int(match["fraction"].ljust(_MAX_FRACTION_DIGITS, "0")),
    )
    return -magnitude if match["sign"] == "-" else magnitude
