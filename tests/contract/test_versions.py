"""Families, and reading ``schema_version`` into one of four states."""

import unittest
from types import MappingProxyType

from yggdrasil.contract import (
    ATTEMPT_REPORT,
    CURRENT_FAMILIES,
    PLAN_DOCUMENT,
    PLAN_STATUS,
    SUPPORTED_FAMILIES,
)
from yggdrasil.contract.versions import (
    INVALID,
    SUPPORTED,
    UNSUPPORTED,
    UNVERSIONED,
    describe_schema_version,
    supports,
    version_of,
    version_sentence,
)

KINDS = (PLAN_DOCUMENT, PLAN_STATUS)


class TestFamilies(unittest.TestCase):
    """What this release writes and supports, per kind."""

    def test_this_release_writes_and_supports_family_one(self):
        self.assertEqual(dict(CURRENT_FAMILIES), {PLAN_DOCUMENT: 1, PLAN_STATUS: 1})
        self.assertEqual(
            dict(SUPPORTED_FAMILIES), {PLAN_DOCUMENT: {1}, PLAN_STATUS: {1}}
        )

    def test_every_current_family_is_supported(self):
        for kind, family in CURRENT_FAMILIES.items():
            with self.subTest(kind=kind):
                self.assertIn(family, SUPPORTED_FAMILIES[kind])

    def test_the_attempt_report_has_no_family_of_its_own(self):
        self.assertNotIn(ATTEMPT_REPORT, CURRENT_FAMILIES)
        self.assertNotIn(ATTEMPT_REPORT, SUPPORTED_FAMILIES)

    def test_support_cannot_be_widened_at_run_time(self):
        with self.assertRaises(TypeError):
            SUPPORTED_FAMILIES[PLAN_DOCUMENT] = frozenset({1, 2})
        with self.assertRaises(TypeError):
            CURRENT_FAMILIES[PLAN_DOCUMENT] = 2
        for kind, families in SUPPORTED_FAMILIES.items():
            with self.subTest(kind=kind):
                self.assertIsInstance(families, frozenset)
        self.assertEqual(SUPPORTED_FAMILIES[PLAN_DOCUMENT], {1})


class TestVersionOf(unittest.TestCase):
    """Absent is unversioned, a positive int is a family, anything else invalid."""

    def assert_version(self, kind, document, state, family, recorded):
        """Assert version_of's answer for one document, recorded value by identity."""
        info = version_of(kind, document)
        self.assertEqual((info.state, info.family), (state, family))
        self.assertIs(info.recorded, recorded)

    def test_marker_states(self):
        huge = 10**400
        nan = float("nan")
        cases = [
            (1, SUPPORTED, 1),
            (2, UNSUPPORTED, 2),
            (huge, UNSUPPORTED, huge),
            ("1", INVALID, None),
            (1.0, INVALID, None),
            (True, INVALID, None),
            (False, INVALID, None),
            (None, INVALID, None),
            (0, INVALID, None),
            (-1, INVALID, None),
            (-huge, INVALID, None),
            (nan, INVALID, None),
            ([1], INVALID, None),
            ({"family": 1}, INVALID, None),
        ]
        for kind in KINDS:
            for value, state, family in cases:
                with self.subTest(kind=kind, value=value):
                    document = {"_id": "doc", "schema_version": value}
                    self.assert_version(kind, document, state, family, value)

    def test_absent_marker_is_unversioned_never_family_one(self):
        for kind in KINDS:
            with self.subTest(kind=kind):
                self.assert_version(kind, {"_id": "doc"}, UNVERSIONED, None, None)
                self.assert_version(kind, {}, UNVERSIONED, None, None)

    def test_any_mapping_is_read(self):
        document = MappingProxyType({"schema_version": 1})
        self.assert_version(PLAN_DOCUMENT, document, SUPPORTED, 1, 1)

    def test_a_document_that_is_not_an_object_is_invalid(self):
        for document in (None, [], [{"schema_version": 1}], "doc", 1, 1.5, True):
            with self.subTest(document=document):
                self.assert_version(PLAN_DOCUMENT, document, INVALID, None, None)

    def test_supports_only_the_supported_state(self):
        for value, expected in ((1, True), (2, False), ("1", False), (1.0, False)):
            with self.subTest(value=value):
                self.assertIs(
                    supports(PLAN_DOCUMENT, {"schema_version": value}), expected
                )
        self.assertFalse(supports(PLAN_STATUS, {}))
        self.assertFalse(supports(PLAN_STATUS, None))

    def test_a_kind_without_a_marker_is_a_programming_error(self):
        for kind in (ATTEMPT_REPORT, "daemon_lease", "plan", ""):
            with self.subTest(kind=kind):
                with self.assertRaises(ValueError):
                    version_of(kind, {"schema_version": 1})
                with self.assertRaises(ValueError):
                    supports(kind, {"schema_version": 1})


class TestDescribingAVersion(unittest.TestCase):
    """One description of a recorded ``schema_version``, for every diagnostic."""

    def test_the_recorded_value_or_absent(self):
        cases = (
            ({}, "absent"),
            ({"schema_version": 2}, "2"),
            ({"schema_version": "1"}, "'1'"),
            ({"schema_version": None}, "None"),
            ({"schema_version": 1.0}, "1.0"),
            ({"schema_version": 10**5000}, "<unrepresentable int>"),
        )
        for document, expected in cases:
            with self.subTest(expected=expected):
                version = version_of(PLAN_DOCUMENT, document)
                self.assertEqual(describe_schema_version(version), expected)

    def test_a_long_recorded_value_is_bounded(self):
        version = version_of(PLAN_DOCUMENT, {"schema_version": "x" * 10_000})
        self.assertLessEqual(len(describe_schema_version(version)), 200)

    def test_the_sentence_names_the_value_the_state_and_the_families(self):
        cases = (
            (
                PLAN_DOCUMENT,
                {},
                "its schema_version is absent, so it is unversioned "
                "(supported plan-document families: [1])",
            ),
            (
                PLAN_DOCUMENT,
                {"schema_version": "1"},
                "its schema_version is '1', so it is invalid "
                "(supported plan-document families: [1])",
            ),
            (
                PLAN_DOCUMENT,
                {"schema_version": 2},
                "its schema_version is 2, so it is unsupported "
                "(supported plan-document families: [1])",
            ),
            (
                PLAN_STATUS,
                {"schema_version": 2},
                "its schema_version is 2, so it is unsupported "
                "(supported plan-status families: [1])",
            ),
        )
        for kind, document, expected in cases:
            with self.subTest(kind=kind, expected=expected):
                version = version_of(kind, document)
                self.assertEqual(version_sentence(kind, version), expected)

    def test_the_sentence_needs_a_kind_with_a_marker(self):
        version = version_of(PLAN_DOCUMENT, {})
        for kind in (ATTEMPT_REPORT, "plan", ""):
            with self.subTest(kind=kind):
                with self.assertRaises(ValueError):
                    version_sentence(kind, version)


if __name__ == "__main__":
    unittest.main()
