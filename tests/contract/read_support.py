"""Inputs for the reader's tests: corpus documents and named hostile values.

Every plan document a reader test reads is a regression-corpus document
(``documents/current/plan_document/``), which Yggdrasil's own writers built,
or such a document with one field changed (:func:`tests.contract.support.changed`),
so each test shows which field matters.

Hostile values are named, never shown: representing some of them raises (an
integer beyond Python's 4300-digit limit, even inside a container), so a test
passes a case's name to ``subTest``, never the value.
"""

from __future__ import annotations

import json
import logging
import unittest
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

# The current edition of the regression corpus.
CURRENT = Path(__file__).resolve().parent / "documents" / "current"
PLAN_DOCUMENTS = CURRENT / "plan_document"

# Values of awkward shapes, each with a name to report it by.
HOSTILE: tuple[tuple[str, Any], ...] = (
    ("null", None),
    ("true", True),
    ("false", False),
    ("zero", 0),
    ("minus one", -1),
    ("float", 5.0),
    ("nan", float("nan")),
    ("infinity", float("inf")),
    ("huge int", 10**400),
    ("huge negative int", -(10**400)),
    ("unrepresentable int", 10**5000),
    ("empty text", ""),
    ("long text", "x" * 10_000),
    ("text with a nul", "a\x00b"),
    ("bytes", b"approved"),
    ("empty list", []),
    ("wide list", list(range(10_000))),
    ("nested list", [[[[[[[[[[1]]]]]]]]]]),
    ("list of an unrepresentable int", [10**5000]),
    ("empty object", {}),
    ("wide object", {f"k{index}": index for index in range(10_000)}),
    ("nested object", {"a": {"b": {"c": [None, {"d": 10**5000}]}}}),
    ("list of objects", [{"step_id": "s1"}] * 3),
)


def plan_document(name: str) -> dict[str, Any]:
    """Return a fresh copy of a corpus plan document.

    Args:
        name: The file's name without ``.json``, such as ``draft``.

    Returns:
        dict[str, Any]: The document, parsed anew on each call.
    """
    return json.loads((PLAN_DOCUMENTS / f"{name}.json").read_text("utf-8"))


def plan_document_names() -> list[str]:
    """Return the name of every corpus plan document, sorted."""
    return sorted(path.stem for path in PLAN_DOCUMENTS.glob("*.json"))


class ReaderTestCase(unittest.TestCase):
    """A test case that asserts the reader stays silent."""

    @contextmanager
    def assert_silent(self) -> Iterator[None]:
        """Fail if anything is logged, at any level, inside the block.

        Yields:
            None: For the block that reads documents.
        """
        with self.assertNoLogs(level=logging.DEBUG):
            yield
