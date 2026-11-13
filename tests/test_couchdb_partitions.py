"""
Comprehensive tests for yggdrasil/storage/partitions.py

Tests the partition_key function for CouchDB partition key generation.
"""

import unittest
from typing import Any

from yggdrasil.storage.partitions import partition_key

# Scopes the writers accept: any truthy kind and ID, of any type.
ACCEPTED: tuple[tuple[str, dict[str, Any]], ...] = (
    ("project", {"kind": "project", "id": "P12345"}),
    ("flowcell", {"kind": "flowcell", "id": "A22FN2"}),
    ("bundle", {"kind": "bundle", "id": "B12345"}),
    ("another kind", {"kind": "sample", "id": "S1"}),
    ("a capitalised kind", {"kind": "Project", "id": "P1"}),
    ("extra keys", {"kind": "project", "id": "P1", "name": "n", "status": "s"}),
    ("an integer ID", {"kind": "project", "id": 5}),
    ("a float ID", {"kind": "flowcell", "id": 1.5}),
    ("a true ID", {"kind": "project", "id": True}),
    ("a list ID", {"kind": "project", "id": ["P1"]}),
    ("an object ID", {"kind": "flowcell", "id": {"id": 1}}),
    ("an integer kind", {"kind": 5, "id": "P1"}),
    ("a true kind", {"kind": True, "id": True}),
    ("a list kind", {"kind": ["project"], "id": "P1"}),
    ("an object kind", {"kind": {"kind": "project"}, "id": "P1"}),
)

# Scopes the writers refuse: a kind or an ID that is absent or falsy.
REFUSED: tuple[tuple[str, dict[str, Any]], ...] = (
    ("empty scope", {}),
    ("kind absent", {"id": "P1"}),
    ("id absent", {"kind": "project"}),
    ("both absent", {"other": "data"}),
    ("kind empty", {"kind": "", "id": "P1"}),
    ("id empty", {"kind": "project", "id": ""}),
    ("kind null", {"kind": None, "id": "P1"}),
    ("id null", {"kind": "project", "id": None}),
    ("kind zero", {"kind": 0, "id": "P1"}),
    ("id zero", {"kind": "project", "id": 0}),
    ("id false", {"kind": "project", "id": False}),
    ("id an empty list", {"kind": "project", "id": []}),
    ("kind an empty object", {"kind": {}, "id": "P1"}),
)


class TestPartitionKey(unittest.TestCase):
    """
    Comprehensive tests for partition_key function.

    Tests partition key generation for various scope types.
    """

    # =====================================================
    # VALID PARTITION KEY TESTS
    # =====================================================

    def test_project_scope(self):
        """Test partition key for project scope."""
        scope = {"kind": "project", "id": "P12345"}
        result = partition_key(scope)
        self.assertEqual(result, "project-P12345")

    def test_flowcell_scope(self):
        """Test partition key for flowcell scope."""
        scope = {"kind": "flowcell", "id": "A22FN2"}
        result = partition_key(scope)
        self.assertEqual(result, "flowcell-A22FN2")

    def test_bundle_scope(self):
        """Test partition key for bundle scope."""
        scope = {"kind": "bundle", "id": "B12345"}
        result = partition_key(scope)
        self.assertEqual(result, "bundle-B12345")

    def test_other_kind_scope(self):
        """Test partition key for other kind types."""
        scope = {"kind": "sample", "id": "S99999"}
        result = partition_key(scope)
        self.assertEqual(result, "sample-S99999")

    def test_custom_kind_scope(self):
        """Test partition key for custom kind."""
        scope = {"kind": "experiment", "id": "EXP001"}
        result = partition_key(scope)
        self.assertEqual(result, "experiment-EXP001")

    # =====================================================
    # ID VARIATIONS
    # =====================================================

    def test_project_with_underscores(self):
        """Test project ID with underscores."""
        scope = {"kind": "project", "id": "P_12345_001"}
        result = partition_key(scope)
        self.assertEqual(result, "project-P_12345_001")

    def test_project_with_dashes(self):
        """Test project ID with dashes."""
        scope = {"kind": "project", "id": "P-12345-001"}
        result = partition_key(scope)
        self.assertEqual(result, "project-P-12345-001")

    def test_project_with_alphanumeric(self):
        """Test project ID with mixed alphanumeric."""
        scope = {"kind": "project", "id": "ABC123XYZ"}
        result = partition_key(scope)
        self.assertEqual(result, "project-ABC123XYZ")

    def test_flowcell_with_complex_id(self):
        """Test flowcell with complex ID."""
        scope = {"kind": "flowcell", "id": "H7VJKDRXX_S1"}
        result = partition_key(scope)
        self.assertEqual(result, "flowcell-H7VJKDRXX_S1")

    # =====================================================
    # ERROR HANDLING TESTS
    # =====================================================

    def test_missing_kind(self):
        """Test error when kind is missing."""
        scope = {"id": "P12345"}
        with self.assertRaises(ValueError) as context:
            partition_key(scope)
        self.assertIn("Bad scope", str(context.exception))

    def test_missing_id(self):
        """Test error when id is missing."""
        scope = {"kind": "project"}
        with self.assertRaises(ValueError) as context:
            partition_key(scope)
        self.assertIn("Bad scope", str(context.exception))

    def test_empty_kind(self):
        """Test error when kind is empty string."""
        scope = {"kind": "", "id": "P12345"}
        with self.assertRaises(ValueError) as context:
            partition_key(scope)
        self.assertIn("Bad scope", str(context.exception))

    def test_empty_id(self):
        """Test error when id is empty string."""
        scope = {"kind": "project", "id": ""}
        with self.assertRaises(ValueError) as context:
            partition_key(scope)
        self.assertIn("Bad scope", str(context.exception))

    def test_none_kind(self):
        """Test error when kind is None."""
        scope = {"kind": None, "id": "P12345"}
        with self.assertRaises(ValueError) as context:
            partition_key(scope)
        self.assertIn("Bad scope", str(context.exception))

    def test_none_id(self):
        """Test error when id is None."""
        scope = {"kind": "project", "id": None}
        with self.assertRaises(ValueError) as context:
            partition_key(scope)
        self.assertIn("Bad scope", str(context.exception))

    def test_empty_scope(self):
        """Test error with empty scope dict."""
        scope = {}
        with self.assertRaises(ValueError) as context:
            partition_key(scope)
        self.assertIn("Bad scope", str(context.exception))

    def test_both_missing(self):
        """Test error when both kind and id are missing."""
        scope = {"other": "data"}
        with self.assertRaises(ValueError) as context:
            partition_key(scope)
        self.assertIn("Bad scope", str(context.exception))

    # =====================================================
    # EDGE CASES
    # =====================================================

    def test_scope_with_extra_fields(self):
        """Test partition key ignores extra fields."""
        scope = {
            "kind": "project",
            "id": "P12345",
            "name": "Test Project",
            "status": "active",
        }
        result = partition_key(scope)
        self.assertEqual(result, "project-P12345")

    def test_single_character_id(self):
        """Test partition key with single character ID."""
        scope = {"kind": "project", "id": "P"}
        result = partition_key(scope)
        self.assertEqual(result, "project-P")

    def test_numeric_only_id(self):
        """Test partition key with numeric-only ID."""
        scope = {"kind": "project", "id": "12345"}
        result = partition_key(scope)
        self.assertEqual(result, "project-12345")

    def test_long_id(self):
        """Test partition key with very long ID."""
        long_id = "P" + "1" * 1000
        scope = {"kind": "project", "id": long_id}
        result = partition_key(scope)
        self.assertEqual(result, f"project-{long_id}")

    def test_special_characters_in_id(self):
        """Test partition key with special characters."""
        scope = {"kind": "project", "id": "P@12345!"}
        result = partition_key(scope)
        self.assertEqual(result, "project-P@12345!")

    def test_unicode_in_id(self):
        """Test partition key with unicode characters."""
        scope = {"kind": "project", "id": "P12345_测试"}
        result = partition_key(scope)
        self.assertEqual(result, "project-P12345_测试")

    # =====================================================
    # CONSISTENCY TESTS
    # =====================================================

    def test_consistent_output(self):
        """Test that same input produces same output."""
        scope = {"kind": "project", "id": "P12345"}
        result1 = partition_key(scope)
        result2 = partition_key(scope)
        self.assertEqual(result1, result2)

    def test_different_inputs_different_outputs(self):
        """Test that different inputs produce different outputs."""
        scope1 = {"kind": "project", "id": "P12345"}
        scope2 = {"kind": "project", "id": "P54321"}
        result1 = partition_key(scope1)
        result2 = partition_key(scope2)
        self.assertNotEqual(result1, result2)

    def test_kind_case_sensitive(self):
        """Test that kind is case sensitive."""
        scope1 = {"kind": "project", "id": "P12345"}
        scope2 = {"kind": "Project", "id": "P12345"}
        result1 = partition_key(scope1)
        result2 = partition_key(scope2)
        # Different kinds should produce different results
        self.assertNotEqual(result1, result2)

    def test_id_case_sensitive(self):
        """Test that id is case sensitive."""
        scope1 = {"kind": "project", "id": "p12345"}
        scope2 = {"kind": "project", "id": "P12345"}
        result1 = partition_key(scope1)
        result2 = partition_key(scope2)
        self.assertNotEqual(result1, result2)


class TestPartitionKeyAcceptance(unittest.TestCase):
    """The writers accept and refuse exactly the scopes they always have.

    Every accepted scope is stored under its kind, as recorded, and its ID.
    """

    def test_every_accepted_scope_is_named_by_its_kind_and_id(self):
        for name, scope in ACCEPTED:
            with self.subTest(name):
                self.assertEqual(partition_key(scope), f"{scope['kind']}-{scope['id']}")

    def test_every_refused_scope_raises_bad_scope(self):
        for name, scope in REFUSED:
            with self.subTest(name):
                with self.assertRaises(ValueError) as raised:
                    partition_key(scope)
                self.assertEqual(str(raised.exception), f"Bad scope: {scope}")


if __name__ == "__main__":
    unittest.main()
