"""Timestamps: the canonical spelling, reading instants, and writing them."""

import unittest
from datetime import UTC, date, datetime, timedelta, timezone, tzinfo
from unittest.mock import patch

import yggdrasil.contract.timestamps
from yggdrasil.contract.timestamps import (
    CANONICAL_FORMAT,
    CANONICAL_MAX_LENGTH,
    CANONICAL_PATTERN,
    canonical_from_legacy,
    canonical_text,
    is_canonical,
    is_date_time,
    parse_timestamp,
)
from yggdrasil.flow.utils.ygg_time import utcnow_iso

CANONICAL = "2026-10-22T10:00:00Z"

# Arabic-Indic and fullwidth digits: digits to Python's \d, never to [0-9].
ARABIC_INDIC = "٢٠٢٦-١٠-٢٢T١٠:٠٠:٠٠Z"
FULLWIDTH = "２０２６-１０-２２T１０:００:００Z"

NOT_STRINGS = (None, 20261022, 1.5, True, b"2026-10-22T10:00:00Z", [CANONICAL])

# Not a real date or time, though every field has the right width.
IMPOSSIBLE = (
    "2026-02-30T00:00:00Z",
    "2025-02-29T00:00:00Z",  # not a leap year
    "2026-13-01T00:00:00Z",
    "2026-00-10T00:00:00Z",
    "2026-10-00T00:00:00Z",
    "2026-10-22T24:00:00Z",
    "2026-10-22T10:60:00Z",
    "2026-10-22T10:00:60Z",  # a leap second
    "0000-01-01T00:00:00Z",
)

# Canonical fractions: one to six digits, the last of them not 0.
CANONICAL_FRACTIONS = (
    "2026-10-22T10:00:00.1Z",
    "2026-10-22T10:00:00.25Z",
    "2026-10-22T10:00:00.125Z",
    "2026-10-22T10:00:00.0625Z",
    "2026-10-22T10:00:00.00001Z",
    "2026-10-22T10:00:00.000001Z",
    "2026-10-22T10:00:00.123456Z",
    "0001-01-01T00:00:00.000001Z",
    "9999-12-31T23:59:59.999999Z",
)

TRAILING_NEWLINE = (
    "2026-10-22T10:00:00Z\n",
    "2026-10-22T10:00:00.5Z\n",
    "2026-10-22T10:00:00.12345Z\n",
    "2026-10-22T10:00:00+00:00\n",
)


def utc(
    year: int,
    month: int,
    day: int,
    hour: int = 0,
    minute: int = 0,
    second: int = 0,
    microsecond: int = 0,
) -> datetime:
    """Return an instant in UTC, from its fields."""
    return datetime(year, month, day, hour, minute, second, microsecond, tzinfo=UTC)


# The instant most cases below name: 2026-11-06T10:20:30Z.
INSTANT = utc(2026, 11, 6, 10, 20, 30)


class TestCanonical(unittest.TestCase):
    """UTC, ``Z``, whole seconds or a trimmed fraction: one spelling per instant."""

    def test_pattern_is_published_as_written(self):
        self.assertEqual(
            CANONICAL_PATTERN,
            "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
            "(?:\\.[0-9]{0,5}[1-9])?Z(?![\\s\\S])",
        )
        self.assertNotIn("\\d", CANONICAL_PATTERN)
        self.assertFalse(CANONICAL_PATTERN.endswith("$"))

    def test_the_longest_spelling_is_the_maximum_length(self):
        longest = "9999-12-31T23:59:59.999999Z"
        self.assertTrue(is_canonical(longest))
        self.assertEqual(len(longest), CANONICAL_MAX_LENGTH)
        self.assertEqual(CANONICAL_MAX_LENGTH, 27)

    def test_format_writes_the_canonical_whole_second(self):
        written = datetime(2026, 10, 22, 10, 0, 0).strftime(CANONICAL_FORMAT)
        self.assertEqual(written, CANONICAL)

    def test_the_clock_writes_canonical_whole_seconds(self):
        written = utcnow_iso()
        self.assertTrue(is_canonical(written))
        self.assertNotIn(".", written)

    def test_accepts_real_whole_second_utc_times(self):
        for value in (
            CANONICAL,
            "2024-02-29T23:59:59Z",
            "0001-01-01T00:00:00Z",
            "9999-12-31T23:59:59Z",
        ):
            with self.subTest(value=value):
                self.assertTrue(is_canonical(value))

    def test_accepts_a_fraction_of_one_to_six_digits(self):
        for value in CANONICAL_FRACTIONS:
            with self.subTest(value=value):
                self.assertTrue(is_canonical(value))

    def test_rejects_a_fraction_that_is_not_canonical(self):
        for value in (
            "2026-10-22T10:00:00.0Z",
            "2026-10-22T10:00:00.10Z",
            "2026-10-22T10:00:00.250Z",
            "2026-10-22T10:00:00.000000Z",
            "2026-10-22T10:00:00.1234567Z",
            "2026-10-22T10:00:00.1000000Z",
            "2026-10-22T10:00:00.Z",
            "2026-10-22T10:00:00,5Z",
            "2026-10-22T10:00:00.5z",
            "2026-10-22T10:00:00.5+00:00",
            "2026-10-22T10:00:00.5-00:00",
            "2026-10-22t10:00:00.5Z",
            "2026-10-22T10:00:00.５Z",
            "2026-10-22T10:00:00.5",
        ):
            with self.subTest(value=value):
                self.assertFalse(is_canonical(value))

    def test_rejects_every_other_spelling(self):
        for value in (
            "2026-10-22T10:00:00+00:00",
            "2026-10-22T10:00:00-00:00",
            "2026-10-22T12:00:00+02:00",
            "2026-10-22T10:00:00.000Z",
            "2026-10-22t10:00:00Z",
            "2026-10-22T10:00:00z",
            "2026-10-22 10:00:00Z",
            "2026-10-22T10:00Z",
            "2026-10-22T10:00:00",
            "2026-10-22",
            "20261022T100000Z",
            "2026-1-22T10:00:00Z",
            "26-10-22T10:00:00Z",
            "2026-10-22T1:00:00Z",
            "+2026-10-22T10:00:00Z",
            " 2026-10-22T10:00:00Z",
            "2026-10-22T10:00:00Z ",
            "2026-10-22T10:00:00Zx",
            "2026-10-22T10:00:00ZZ",
            "",
            ARABIC_INDIC,
            FULLWIDTH,
        ):
            with self.subTest(value=value):
                self.assertFalse(is_canonical(value))

    def test_rejects_impossible_dates_and_times(self):
        for value in IMPOSSIBLE:
            for fraction in ("", ".5"):
                spelled = value.replace("Z", f"{fraction}Z")
                with self.subTest(value=spelled):
                    self.assertFalse(is_canonical(spelled))

    def test_rejects_values_that_are_not_strings(self):
        for value in NOT_STRINGS:
            with self.subTest(value=value):
                self.assertFalse(is_canonical(value))


class TestDateTime(unittest.TestCase):
    """Any valid RFC 3339 spelling, naming a date and time that exist."""

    def test_accepts_every_rfc3339_spelling(self):
        for value in (
            CANONICAL,
            "2026-10-22T10:00:00+00:00",
            "2026-10-22T10:00:00-00:00",
            "2026-10-22T12:00:00+02:00",
            "2026-10-22T04:30:00-05:30",
            "2026-10-22T10:00:00+23:59",
            "2026-10-22T10:00:00.5Z",
            "2026-10-22T10:00:00.123456789+01:00",
            "2026-10-22t10:00:00z",
            "2024-02-29T23:59:59Z",
        ):
            with self.subTest(value=value):
                self.assertTrue(is_date_time(value))

    def test_rejects_iso_forms_rfc3339_does_not_allow(self):
        for value in (
            "2026-10-22",
            "2026-10-22 10:00:00Z",
            "20261022T100000Z",
            "2026-10-22T10:00:00",
            "2026-10-22T10:00Z",
            "2026-10-22T10Z",
            "2026-10-22T10:00:00.Z",
            "2026-10-22T10:00:00,5Z",
            "2026-10-22T10:00:00+0200",
            "2026-10-22T10:00:00+02",
            "2026-W43-4T10:00:00Z",
            "2026-295T10:00:00Z",
            "",
        ):
            with self.subTest(value=value):
                self.assertFalse(is_date_time(value))

    def test_rejects_impossible_dates_times_and_offsets(self):
        for value in (
            *IMPOSSIBLE,
            "2026-10-22T10:00:00+24:00",
            "2026-10-22T10:00:00+05:60",
        ):
            with self.subTest(value=value):
                self.assertFalse(is_date_time(value))

    def test_rejects_non_ascii_digits_and_surrounding_text(self):
        for value in (
            ARABIC_INDIC,
            FULLWIDTH,
            " 2026-10-22T10:00:00Z",
            "2026-10-22T10:00:00Z ",
            "2026-10-22T10:00:00Zx",
        ):
            with self.subTest(value=value):
                self.assertFalse(is_date_time(value))

    def test_rejects_values_that_are_not_strings(self):
        for value in NOT_STRINGS:
            with self.subTest(value=value):
                self.assertFalse(is_date_time(value))


class TestCanonicalFromLegacy(unittest.TestCase):
    """The two whole-second UTC spellings earlier releases wrote, re-spelled."""

    def test_offset_spelling_becomes_z_at_the_same_instant(self):
        legacy = "2026-01-01T00:00:00+00:00"
        canonical = canonical_from_legacy(legacy)
        self.assertEqual(canonical, "2026-01-01T00:00:00Z")
        self.assertEqual(
            datetime.strptime(canonical, CANONICAL_FORMAT).replace(tzinfo=UTC),
            datetime.fromisoformat(legacy),
        )

    def test_canonical_spelling_is_kept(self):
        self.assertEqual(canonical_from_legacy(CANONICAL), CANONICAL)

    def test_anything_else_is_not_converted(self):
        for value in (
            "2026-10-22T10:00:00-00:00",
            "2026-10-22T12:00:00+02:00",
            "2026-10-22T10:00:00.5+00:00",
            "2026-10-22T10:00:00.5Z",
            "2026-10-22t10:00:00+00:00",
            "2026-10-22T10:00:00z",
            "2026-02-30T00:00:00+00:00",
            "2026-02-30T00:00:00Z",
            "2026-10-22T10:00:00+00:00+00:00",
            "2026-10-22T10:00:00Z+00:00",
            "2026-10-22 10:00:00+00:00",
            "2026-10-22",
            "+00:00",
            "Z",
            "",
            ARABIC_INDIC,
        ):
            with self.subTest(value=value):
                self.assertIsNone(canonical_from_legacy(value))

    def test_a_fraction_is_not_converted_even_when_canonical(self):
        # Earlier releases wrote whole seconds only, whatever the canonical
        # spelling allows now.
        for value in (*CANONICAL_FRACTIONS, "2026-10-22T10:00:00.25+00:00"):
            with self.subTest(value=value):
                self.assertIsNone(canonical_from_legacy(value))

    def test_values_that_are_not_strings_are_not_converted(self):
        for value in NOT_STRINGS:
            with self.subTest(value=value):
                self.assertIsNone(canonical_from_legacy(value))


class TestTrailingNewline(unittest.TestCase):
    """``$`` matches before a final newline in Python; no check is fooled by it."""

    def test_every_check_rejects_a_trailing_newline(self):
        for value in TRAILING_NEWLINE:
            with self.subTest(value=value):
                self.assertFalse(is_canonical(value))
                self.assertFalse(is_date_time(value))
                self.assertIsNone(canonical_from_legacy(value))
                self.assertIsNone(parse_timestamp(value))


class ReadingTestCase(unittest.TestCase):
    """Assertions on what parse_timestamp reads, for the classes below."""

    def assert_reads(self, cases: dict[str, datetime]) -> None:
        """Assert each value reads as exactly its instant, aware in UTC."""
        for value, expected in cases.items():
            with self.subTest(value=value):
                instant = parse_timestamp(value)
                self.assertEqual(instant, expected)
                assert instant is not None
                self.assertIs(instant.tzinfo, UTC)
                self.assertEqual(instant.microsecond, expected.microsecond)

    def assert_unread(self, values: dict[str, object]) -> None:
        """Assert no named value names an instant, and none raises."""
        for name, value in values.items():
            with self.subTest(name):
                self.assertIsNone(parse_timestamp(value))


class TestParseTimestamp(ReadingTestCase):
    """The instant a recorded timestamp names, read as these cases pin it.

    These cases are compatibility checks for Python upgrades. The reader
    hands what RFC 3339 does not cover to Python's
    ``datetime.fromisoformat``, so a new Python can change what it reads. A
    case that starts to fail is a change in what the reader accepts, to be
    reviewed as one, never a test to adjust. They detect changes in the cases
    they name, not every change in the parser's grammar. Each asserts the
    instant read, never mere success.
    """

    def test_canonical_values_are_read_exactly(self):
        self.assert_reads(
            {
                "2026-11-06T10:20:30Z": INSTANT,
                "2026-11-06T10:20:30.1Z": utc(2026, 11, 6, 10, 20, 30, 100000),
                "2026-11-06T10:20:30.000001Z": utc(2026, 11, 6, 10, 20, 30, 1),
                "2026-11-06T10:20:30.123456Z": utc(2026, 11, 6, 10, 20, 30, 123456),
            }
        )

    def test_fractional_zeros_are_read_as_the_same_instant(self):
        self.assert_reads(
            {
                "2026-11-06T10:20:30.100Z": utc(2026, 11, 6, 10, 20, 30, 100000),
                "2026-11-06T10:20:30.000000Z": INSTANT,
            }
        )

    def test_numeric_offsets_are_converted_to_utc(self):
        self.assert_reads(
            {
                "2026-11-06T10:20:30+00:00": INSTANT,
                "2026-11-06T10:20:30-00:00": INSTANT,
                "2026-11-06T11:20:30+01:00": INSTANT,
                "2026-11-06T04:50:30-05:30": INSTANT,
                "2026-11-07T10:19:30+23:59": INSTANT,
                "2026-11-06T11:20:30.5+01:00": utc(2026, 11, 6, 10, 20, 30, 500000),
            }
        )

    def test_lowercase_t_and_z_are_read_as_rfc3339_allows(self):
        self.assert_reads(
            {
                "2026-11-06t10:20:30z": INSTANT,
                "2026-11-06t10:20:30Z": INSTANT,
                "2026-11-06T10:20:30z": INSTANT,
                "2026-11-06t11:20:30+01:00": INSTANT,
            }
        )

    def test_iso_forms_python_reads_are_read(self):
        self.assert_reads(
            {
                "2026-11-06 10:20:30Z": INSTANT,
                "20261106T102030Z": INSTANT,
                "20261106t102030Z": INSTANT,
                "2026-W45-5T10:20:30Z": INSTANT,
            }
        )

    def test_shorter_iso_forms_are_read_as_their_instants(self):
        self.assert_reads(
            {
                "2026-11-06T10:20Z": utc(2026, 11, 6, 10, 20, 0),
                "2026-11-06T10Z": utc(2026, 11, 6, 10, 0, 0),
                "2026-11-06T10:20:30,5Z": utc(2026, 11, 6, 10, 20, 30, 500000),
                "2026-11-06T10:20:30+01:00:00": utc(2026, 11, 6, 9, 20, 30),
            }
        )

    def test_six_fractional_digits_are_read(self):
        self.assert_reads(
            {
                "2026-11-06T10:20:30.123456Z": utc(2026, 11, 6, 10, 20, 30, 123456),
                "2026-11-06T10:20:30,123456Z": utc(2026, 11, 6, 10, 20, 30, 123456),
                "2026-11-06T10:20:30+01:00:00.123456": utc(
                    2026, 11, 6, 9, 20, 29, 876544
                ),
                "2026-11-06T10:20:30+01:00:00,123456": utc(
                    2026, 11, 6, 9, 20, 29, 876544
                ),
            }
        )

    def test_a_seventh_fractional_digit_is_never_truncated(self):
        self.assert_unread(
            {
                "seven digits after '.' in the time": "2026-11-06T10:20:30.1234567Z",
                "seven digits after ',' in the time": "2026-11-06T10:20:30,1234567Z",
                "seven digits after '.' in the offset": (
                    "2026-11-06T10:20:30+01:00:00.1234567"
                ),
                "seven digits after ',' in the offset": (
                    "2026-11-06T10:20:30+01:00:00,1234567"
                ),
                "seven zeros": "2026-11-06T10:20:30.0000000Z",
                "seven digits with a numeric offset": (
                    "2026-11-06T11:20:30.1234567+01:00"
                ),
                "many digits": "2026-11-06T10:20:30." + "1" * 100 + "Z",
            }
        )

    def test_a_fraction_needs_a_point_or_a_comma(self):
        # Python 3.11 reads digits after the seconds as a fraction when a
        # colon introduces them or they follow directly, truncating them
        # beyond six digits and losing them from an offset whose whole seconds
        # are zero.
        self.assert_unread(
            {
                "seven digits after a colon in the time": (
                    "2026-11-06T10:20:30:1234567+00:00"
                ),
                "seven digits right after the basic seconds": (
                    "2026-11-06T1020301234567+00:00"
                ),
                "a microsecond after a colon in a zero offset": (
                    "2026-11-06T10:20:30+00:00:00:000001"
                ),
                "a microsecond right after a zero basic offset's seconds": (
                    "2026-11-06T10:20:30+000000000001"
                ),
                "one digit after a colon in the time": "2026-11-06T10:20:30:5Z",
                "six digits after a colon in the time": "2026-11-06T10:20:30:123456Z",
                "six zeros then a one after a colon": "2026-11-06T10:20:30:0000001Z",
                "two digits right after the basic seconds": "2026-11-06T10203012Z",
                "six digits right after the basic seconds": (
                    "2026-11-06T102030123456Z"
                ),
                "after a compact date": "20261106T1020301234567Z",
                "after a compact date and a digit between date and time": (
                    "2026110651020301234567Z"
                ),
                "a digit after a colon in a zero offset": (
                    "2026-11-06T10:20:30+00:00:00:5"
                ),
                "two digits right after a zero basic offset's seconds": (
                    "2026-11-06T10:20:30+00000012"
                ),
                "after a negative zero basic offset's seconds": (
                    "2026-11-06T10:20:30-000000000001"
                ),
                "a digit after a colon in a nonzero offset": (
                    "2026-11-06T10:20:30+01:00:00:5"
                ),
                "a colon fraction followed by other text": (
                    "2026-11-06T10:20:30:123456xZ"
                ),
            }
        )

    def test_a_digit_right_after_the_seconds_is_never_dropped(self):
        # Python 3.11 drops a single such digit before the zone.
        self.assert_unread(
            {
                "after the extended seconds": "2026-11-06T10:20:307Z",
                "after the basic seconds": "2026-11-06T1020307Z",
                "after the seconds, before an offset": "2026-11-06T10:20:307+01:00",
                "after a compact date's basic seconds": "20261106T1020307Z",
            }
        )

    def test_a_colon_or_digit_between_date_and_time_can_look_the_same(self):
        # Python reads each as 10:20:30Z on its date. Without parsing the date,
        # the reader cannot tell its separator from a time's unmarked fraction.
        self.assert_unread(
            {
                "a colon after a date": "2026-11-06:10:20:30Z",
                "a colon after a compact date": "20261106:10:20:30Z",
                "a colon after a week": "2026-W45:10:20:30Z",
                "a digit after a date": "2026-11-065102030Z",
                "a digit after a compact date": "202611065102030Z",
            }
        )

    def test_an_offsets_fraction_is_kept_when_its_whole_seconds_are_zero(self):
        self.assert_reads(
            {
                "2026-11-06T10:20:30+00:00:00.000001": utc(
                    2026, 11, 6, 10, 20, 29, 999999
                ),
                "2026-11-06T10:20:30-00:00:00.000001": utc(2026, 11, 6, 10, 20, 30, 1),
                "2026-11-06T10:20:30+00:00:00,000001": utc(
                    2026, 11, 6, 10, 20, 29, 999999
                ),
                "2026-11-06T10:20:30-00:00:00,000001": utc(2026, 11, 6, 10, 20, 30, 1),
                "2026-11-06T10:20:30.5+00:00:00.25": utc(
                    2026, 11, 6, 10, 20, 30, 250000
                ),
            }
        )

    def test_an_offsets_fraction_is_kept_when_its_whole_seconds_are_not_zero(self):
        self.assert_reads(
            {
                "2026-11-06T10:20:30+00:00:01.5": utc(2026, 11, 6, 10, 20, 28, 500000),
                "2026-11-06T10:20:30-00:00:01.5": utc(2026, 11, 6, 10, 20, 31, 500000),
                "2026-11-06T10:20:30+01:00:00.5": utc(2026, 11, 6, 9, 20, 29, 500000),
            }
        )

    def test_an_offsets_fraction_is_kept_in_the_forms_python_reads(self):
        # Python reads the fraction of an offset as microseconds of the offset,
        # whichever component it follows and with or without colons.
        self.assert_reads(
            {
                "2026-11-06T10:20:30+000000.5": utc(2026, 11, 6, 10, 20, 29, 500000),
                "2026-11-06T10:20:30+00:00.5": utc(2026, 11, 6, 10, 20, 29, 500000),
                "2026-11-06T10:20:30+0000.5": utc(2026, 11, 6, 10, 20, 29, 500000),
                "2026-11-06T10:20:30+00.5": utc(2026, 11, 6, 10, 20, 29, 500000),
                "2026-11-06T10:20:30-00.5": utc(2026, 11, 6, 10, 20, 30, 500000),
            }
        )

    def test_an_offset_python_reads_otherwise_is_unknown(self):
        value = "2026-11-06T10:20:30+00:00.5"

        class Parser(datetime):
            """A datetime whose ISO parser reads offsets differently."""

            @classmethod
            def fromisoformat(cls, date_string: str) -> "Parser":
                """Read the offset's fraction as a fraction of a minute.

                That is how ISO 8601 reads a fraction of its last component.
                """
                return cls(
                    2026, 11, 6, 10, 20, 30, tzinfo=timezone(timedelta(seconds=30))
                )

        self.assertEqual(parse_timestamp(value), utc(2026, 11, 6, 10, 20, 29, 500000))
        with patch.object(yggdrasil.contract.timestamps, "datetime", Parser):
            self.assertIsNone(parse_timestamp(value))

    def test_rejected_forms_are_unknown(self):
        self.assert_unread(
            {
                "an ordinal date": "2026-310T10:20:30Z",
                "hour 24": "2026-11-06T24:00:00Z",
                "an offset of 24 hours": "2026-11-06T10:20:30+24:00",
                "an offset of 24 hours with seconds": "2026-11-06T10:20:30+24:00:00",
                "a space and a lowercase z": "2026-11-06 10:20:30z",
                "no timezone": "2026-11-06T10:20:30",
                "no timezone, with a space": "2026-11-06 10:20:30",
                "no timezone, with a fraction": "2026-11-06T10:20:30.5",
                "a date alone": "2026-11-06",
                "a time alone": "10:20:30Z",
                "a leap second": "2026-11-06T10:20:60Z",
                "an empty string": "",
                "Z alone": "Z",
                "words": "yesterday",
                "surrounding space": " 2026-11-06T10:20:30Z ",
                "a trailing space": "2026-11-06T10:20:30Z ",
            }
        )

    def test_impossible_dates_and_times_are_unknown(self):
        for value in IMPOSSIBLE:
            for spelled in (value, value.replace("T", " ")):
                with self.subTest(value=spelled):
                    self.assertIsNone(parse_timestamp(spelled))

    def test_an_instant_that_leaves_the_calendar_in_utc_is_unknown(self):
        self.assert_unread(
            {
                "year 1, an hour east": "0001-01-01T00:00:00+01:00",
                "year 9999, an hour west": "9999-12-31T23:59:59-01:00",
                "year 1, an hour east, with a space": "0001-01-01 00:00:00+01:00",
                "year 9999, a second west, with a fraction": (
                    "9999-12-31T23:59:59.5-00:00:01"
                ),
                "year 1, a microsecond east": "0001-01-01T00:00:00+00:00:00.000001",
            }
        )

    def test_the_calendars_ends_are_read(self):
        self.assert_reads(
            {
                "0001-01-01T00:00:00Z": utc(1, 1, 1, 0, 0, 0),
                "0001-01-01T01:00:00+01:00": utc(1, 1, 1, 0, 0, 0),
                "9999-12-31T23:59:59.999999Z": utc(9999, 12, 31, 23, 59, 59, 999999),
                "9999-12-31T22:59:59-01:00": utc(9999, 12, 31, 23, 59, 59),
            }
        )

    def test_values_that_are_not_text_are_unknown(self):
        self.assert_unread(
            {
                "null": None,
                "an integer": 1762424430,
                "a float": 1762424430.5,
                "true": True,
                "false": False,
                "bytes": b"2026-11-06T10:20:30Z",
                "a bytearray": bytearray(b"2026-11-06T10:20:30Z"),
                "a list": ["2026-11-06T10:20:30Z"],
                "a dict": {"at": "2026-11-06T10:20:30Z"},
                "a datetime": INSTANT,
                "a huge integer": 10**5000,
            }
        )

    def test_hostile_text_is_unknown(self):
        self.assert_unread(
            {
                "NUL": "\x00",
                "NUL after Z": "2026-11-06T10:20:30Z\x00",
                "NUL then text after Z": "2026-11-06T10:20:30Z\x00junk",
                "NUL before Z": "2026-11-06T10:20:30\x00Z",
                "NUL before an offset": "2026-11-06T10:20:30\x00+01:00",
                "Arabic-Indic digits": "٢٠٢٦-١١-٠٦T١٠:٢٠:٣٠Z",
                "fullwidth digits": "２０２６-１１-０６T１０:２０:３０Z",
                "a non-ASCII digit in the fraction": "2026-11-06T10:20:30.١Z",
                "a lone surrogate": "\ud800",
                "a timestamp with a lone surrogate": "2026-11-06T10:20:30\ud800Z",
                "a long string": "x" * 1_000_000,
                "a long digit string": "9" * 1_000_000,
                "a timestamp followed by spaces": "2026-11-06T10:20:30Z"
                + " " * 100_000,
            }
        )

    def test_reading_is_silent(self):
        with self.assertNoLogs(level="DEBUG"):
            for value in (
                "2026-11-06T10:20:30Z",
                "2026-11-06T10:20:30+00:00:00.000001",
                "2026-11-06T10:20:30.1234567Z",
                "2026-11-06T10:20:30:1234567+00:00",
                "0001-01-01T00:00:00+01:00",
                "2026-11-06T10:20:30\x00Z",
                None,
                10**5000,
            ):
                parse_timestamp(value)


class TestPythonQuirks(ReadingTestCase):
    """Forms outside the named cases, read as Python 3.11 reads them.

    Python's ISO parser reads these four forms in ways ISO 8601 does not
    describe, and the reader keeps its readings. Each case pins the instant
    read today. Like the named cases, they are compatibility checks for
    Python upgrades: a case that starts to fail is a change in what the
    reader accepts, to be reviewed as one, never a test to adjust.
    """

    def test_one_stray_character_before_the_zone_is_ignored(self):
        self.assert_reads(
            {
                "2026-11-06T10:20:30xZ": INSTANT,
                "2026-11-06T10:20:30 Z": INSTANT,
                # A colon after the seconds, with no digit after it.
                "2026-11-06T10:20:30:Z": INSTANT,
                "2026-11-06T10:20xZ": utc(2026, 11, 6, 10, 20),
                "2026-11-06T10:20:30x+01:00": utc(2026, 11, 6, 9, 20, 30),
            }
        )

    def test_a_fraction_after_the_hour_or_the_minute_counts_in_seconds(self):
        self.assert_reads(
            {
                "2026-11-06T10:20.5Z": utc(2026, 11, 6, 10, 20, 0, 500000),
                "2026-11-06T10.5Z": utc(2026, 11, 6, 10, 0, 0, 500000),
                "2026-11-06T10:20:30+01:00.5": utc(2026, 11, 6, 9, 20, 29, 500000),
                "2026-11-06T10:20:30+01.5": utc(2026, 11, 6, 9, 20, 29, 500000),
            }
        )

    def test_an_offsets_minutes_and_seconds_roll_over(self):
        self.assert_reads(
            {
                "2026-11-06T10:20:30+05:60": utc(2026, 11, 6, 4, 20, 30),
                "2026-11-06T10:20:30+01:00:60": utc(2026, 11, 6, 9, 19, 30),
                "2026-11-06T10:20:30-00:99": utc(2026, 11, 6, 11, 59, 30),
            }
        )

    def test_an_empty_fraction_is_ignored(self):
        self.assert_reads(
            {
                "2026-11-06T10:20:30.Z": INSTANT,
                "2026-11-06T10:20:30,Z": INSTANT,
                "2026-11-06T10:20:30.+01:00": utc(2026, 11, 6, 9, 20, 30),
            }
        )


class OffsetWithoutUtcOffset(tzinfo):
    """A timezone that names no offset, so a datetime using it is naive."""

    def utcoffset(self, dt: datetime | None) -> timedelta | None:
        """Return no offset."""
        return None


class TestCanonicalText(unittest.TestCase):
    """An instant, written in the canonical spelling."""

    def test_whole_seconds_have_no_fraction(self):
        self.assertEqual(canonical_text(utc(2026, 10, 22, 10, 0, 0)), CANONICAL)

    def test_a_fraction_is_exact_and_trimmed(self):
        for microsecond, written in (
            (1, "2026-10-22T10:00:00.000001Z"),
            (10, "2026-10-22T10:00:00.00001Z"),
            (100000, "2026-10-22T10:00:00.1Z"),
            (250000, "2026-10-22T10:00:00.25Z"),
            (123456, "2026-10-22T10:00:00.123456Z"),
            (999999, "2026-10-22T10:00:00.999999Z"),
        ):
            with self.subTest(microsecond=microsecond):
                self.assertEqual(
                    canonical_text(utc(2026, 10, 22, 10, 0, 0, microsecond)), written
                )

    def test_the_year_always_has_four_digits(self):
        for year, written in (
            (1, "0001-01-02T03:04:05Z"),
            (999, "0999-01-02T03:04:05Z"),
            (1000, "1000-01-02T03:04:05Z"),
            (9999, "9999-01-02T03:04:05Z"),
        ):
            with self.subTest(year=year):
                self.assertEqual(canonical_text(utc(year, 1, 2, 3, 4, 5)), written)

    def test_other_offsets_are_converted_to_utc(self):
        for instant, written in (
            (
                datetime(2026, 10, 22, 12, 0, tzinfo=timezone(timedelta(hours=2))),
                CANONICAL,
            ),
            (
                datetime(2026, 1, 1, 0, 30, tzinfo=timezone(timedelta(hours=1))),
                "2025-12-31T23:30:00Z",
            ),
            (
                datetime(
                    2026, 10, 22, 10, 0, tzinfo=timezone(timedelta(microseconds=1))
                ),
                "2026-10-22T09:59:59.999999Z",
            ),
            (
                datetime(1, 1, 1, 1, 0, tzinfo=timezone(timedelta(hours=1))),
                "0001-01-01T00:00:00Z",
            ),
        ):
            with self.subTest(written=written):
                self.assertEqual(canonical_text(instant), written)

    def test_a_value_that_is_not_a_datetime_is_a_type_error(self):
        for value in (CANONICAL, date(2026, 10, 22), None, 1761127200):
            with self.subTest(value=value):
                with self.assertRaises(TypeError):
                    canonical_text(value)  # type: ignore[arg-type]

    def test_a_naive_datetime_is_a_value_error(self):
        for value in (
            datetime(2026, 10, 22, 10, 0),
            datetime(2026, 10, 22, 10, 0, tzinfo=OffsetWithoutUtcOffset()),
        ):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    canonical_text(value)

    def test_an_instant_outside_the_calendar_in_utc_is_a_value_error(self):
        for value in (
            datetime(1, 1, 1, 0, 0, tzinfo=timezone(timedelta(hours=1))),
            datetime(9999, 12, 31, 23, 59, 59, tzinfo=timezone(timedelta(hours=-1))),
        ):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    canonical_text(value)

    def test_what_it_writes_is_canonical_and_reads_back_as_the_same_instant(self):
        offsets = (
            timedelta(0),
            timedelta(hours=2),
            timedelta(hours=-5, minutes=-30),
            timedelta(hours=23, minutes=59, seconds=59, microseconds=999999),
            timedelta(microseconds=-1),
        )
        for microsecond in (0, 1, 500000, 123456, 999999):
            for offset in offsets:
                instant = datetime(
                    2026, 10, 22, 10, 0, 0, microsecond, tzinfo=timezone(offset)
                )
                with self.subTest(microsecond=microsecond, offset=offset):
                    written = canonical_text(instant)
                    self.assertTrue(is_canonical(written))
                    self.assertEqual(parse_timestamp(written), instant.astimezone(UTC))

    def test_a_canonical_value_is_written_back_unchanged(self):
        for value in (CANONICAL, *CANONICAL_FRACTIONS):
            with self.subTest(value=value):
                instant = parse_timestamp(value)
                assert instant is not None
                self.assertEqual(canonical_text(instant), value)


if __name__ == "__main__":
    unittest.main()
