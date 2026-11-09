"""The problem type and its codes: one home, the same names everywhere."""

import unittest

from yggdrasil.contract import problems, schema

VALIDATION_NAMES = (
    "UNVERSIONED",
    "INVALID_VERSION",
    "UNSUPPORTED_VERSION",
    "MISSING_FIELD",
    "UNEXPECTED_FIELD",
    "WRONG_TYPE",
    "NOT_IN_VOCABULARY",
    "OUT_OF_RANGE",
    "BAD_TIMESTAMP",
    "INVALID_TOKENS",
    "SCHEMA_RULE",
)
READER_NAMES = (
    "IDENTITY_MISMATCH",
    "INVALID_STRUCTURE",
    "RECORD_REPORT_DISAGREE",
    "CONFLICTING_ENDINGS",
    "IDENTITY_CONFLICT",
    "STEP_RECORD_CONTRADICTION",
    "INVALID_FAILURE_POLICY",
    "INVALID_EXECUTION_DEFINITION",
)


class TestCodes(unittest.TestCase):
    """Every code is spelled once, and the schema module still offers its own."""

    def test_the_schema_module_offers_the_same_objects(self):
        self.assertIs(schema.ContractProblem, problems.ContractProblem)
        for name in VALIDATION_NAMES:
            with self.subTest(name=name):
                self.assertIs(getattr(schema, name), getattr(problems, name))

    def test_the_schema_module_lists_the_validation_codes_only(self):
        self.assertEqual(schema.PROBLEM_CODES, problems.VALIDATION_CODES)
        self.assertEqual(
            problems.VALIDATION_CODES,
            {getattr(problems, name) for name in VALIDATION_NAMES},
        )

    def test_the_reader_codes_are_the_eight_it_adds(self):
        self.assertEqual(
            problems.READER_CODES, {getattr(problems, n) for n in READER_NAMES}
        )
        self.assertEqual(len(problems.READER_CODES), 8)
        self.assertFalse(problems.READER_CODES & problems.VALIDATION_CODES)

    def test_all_codes_are_the_validation_and_the_reader_codes(self):
        self.assertEqual(
            problems.ALL_CODES, problems.VALIDATION_CODES | problems.READER_CODES
        )

    def test_no_two_names_share_a_spelling(self):
        spellings = [getattr(problems, n) for n in (*VALIDATION_NAMES, *READER_NAMES)]
        self.assertEqual(len(spellings), len(set(spellings)))
        self.assertEqual(set(spellings), problems.ALL_CODES)


class TestPathText(unittest.TestCase):
    """A path reads the way a person would write it."""

    def test_names_are_joined_with_dots(self):
        self.assertEqual(
            problems.path_text(("plan", "steps", 0, "deps")), "plan.steps[0].deps"
        )

    def test_the_empty_path_is_the_document_itself(self):
        self.assertEqual(problems.path_text(()), "")

    def test_a_leading_index_has_no_dot(self):
        self.assertEqual(problems.path_text((2, "a")), "[2].a")

    def test_a_key_that_is_not_a_plain_name_is_quoted(self):
        for key, shown in (
            ("lane_1/demux", 'steps["lane_1/demux"].state'),
            ("1st", 'steps["1st"].state'),
            ("", 'steps[""].state'),
            ('a"b', 'steps["a\\"b"].state'),
        ):
            with self.subTest(key=key):
                self.assertEqual(problems.path_text(("steps", key, "state")), shown)

    def test_a_key_that_is_not_text_is_shown_as_recorded(self):
        text = problems.path_text(("steps", None, "state"))  # type: ignore[arg-type]
        self.assertTrue(text.startswith("steps["), text)
        self.assertTrue(text.endswith("].state"), text)
        self.assertIn("None", text)


class TestContractProblem(unittest.TestCase):
    """A problem is a frozen record of where, why and what."""

    def test_it_is_immutable_and_comparable(self):
        one = problems.ContractProblem("a.b", problems.WRONG_TYPE, "a.b is wrong")
        same = problems.ContractProblem("a.b", problems.WRONG_TYPE, "a.b is wrong")
        self.assertEqual(one, same)
        with self.assertRaises(AttributeError):
            one.path = "x"  # type: ignore[misc]


if __name__ == "__main__":
    unittest.main()
