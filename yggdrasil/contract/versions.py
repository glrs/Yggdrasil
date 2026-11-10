"""Reading a document's ``schema_version``.

A plan document or snapshot is in exactly one of four version states:

- **supported**: its ``schema_version`` is a family this release supports for
  that kind;
- **unsupported**: a family, but not one this release supports, such as one
  written by a newer release;
- **unversioned**: it has no ``schema_version`` at all, like every document
  written before the contract existed. It is never read as family 1;
- **invalid**: anything else, such as ``"1"``, ``1.0``, ``true``, ``null``,
  ``0``, or a document that is not an object. It never falls back to family 1
  either.

Unversioned and invalid are kept apart, so a diagnostic can tell a
pre-contract document from a damaged marker.

Every diagnostic about a document that is not of a supported family describes
it the same way, through :func:`describe_schema_version` and
:func:`version_sentence`, so an operator can search for one phrase.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from yggdrasil.contract import ATTEMPT_REPORT, SUPPORTED_FAMILIES
from yggdrasil.contract.diagnostics import describe_recorded
from yggdrasil.contract.fields import SCHEMA_VERSION_FIELD

# Version states.
SUPPORTED = "supported"
UNSUPPORTED = "unsupported"
UNVERSIONED = "unversioned"
INVALID = "invalid"


@dataclass(frozen=True)
class VersionInfo:
    """What a stored document's ``schema_version`` says.

    Attributes:
        state: ``supported``, ``unsupported``, ``unversioned`` or ``invalid``.
        family: The family, for a supported or unsupported document; None
            otherwise.
        recorded: The stored value exactly as read, for diagnostics. None when
            the field is absent (``unversioned``), when it holds ``null``
            (``invalid``), or when the document is not an object (``invalid``).
    """

    state: str
    family: int | None
    recorded: Any


def version_of(kind: str, document: object) -> VersionInfo:
    """Return the version state of a stored document of one kind.

    Stored content never makes this raise: whatever ``document`` holds, the
    answer is one of the four states.

    Args:
        kind: The document kind, ``plan_document`` or ``plan_status``.
        document: The stored document, of any shape.

    Returns:
        VersionInfo: The state, the family and the recorded value.

    Raises:
        ValueError: If kind is not a kind that carries a version marker.
    """
    supported = _supported_families(kind)
    if not isinstance(document, Mapping):
        return VersionInfo(INVALID, None, None)
    if SCHEMA_VERSION_FIELD not in document:
        return VersionInfo(UNVERSIONED, None, None)
    recorded = document[SCHEMA_VERSION_FIELD]
    # bool is a subclass of int, and 1.0 == 1: only a true int names a family.
    if type(recorded) is not int or recorded < 1:
        return VersionInfo(INVALID, None, recorded)
    state = SUPPORTED if recorded in supported else UNSUPPORTED
    return VersionInfo(state, recorded, recorded)


def supports(kind: str, document: object) -> bool:
    """Return whether a stored document is of a family this release supports.

    Args:
        kind: The document kind, ``plan_document`` or ``plan_status``.
        document: The stored document, of any shape.

    Returns:
        bool: True only for the ``supported`` state.

    Raises:
        ValueError: If kind is not a kind that carries a version marker.
    """
    return version_of(kind, document).state == SUPPORTED


def describe_schema_version(version: VersionInfo) -> str:
    """Show a document's recorded ``schema_version`` in a diagnostic.

    Args:
        version: What the document's ``schema_version`` says.

    Returns:
        str: ``absent`` for an unversioned document; otherwise the recorded
        value, bounded and never raising (see
        :func:`~yggdrasil.contract.diagnostics.describe_recorded`).
    """
    if version.state == UNVERSIONED:
        return "absent"
    return describe_recorded(version.recorded)


def version_sentence(kind: str, version: VersionInfo) -> str:
    """Say what a document's ``schema_version`` is and which families are supported.

    The words every diagnostic about a document's family uses, such as
    ``its schema_version is absent, so it is unversioned (supported
    plan-document families: [1])``. A caller puts its own subject in front
    and its own consequence after.

    Args:
        kind: The document kind, ``plan_document`` or ``plan_status``.
        version: What the document's ``schema_version`` says.

    Returns:
        str: The recorded value as :func:`describe_schema_version` shows it,
        the version state, and the sorted supported families of kind.

    Raises:
        ValueError: If kind is not a kind that carries a version marker.
    """
    supported = sorted(_supported_families(kind))
    noun = kind.replace("_", "-")
    return (
        f"its schema_version is {describe_schema_version(version)}, so it is "
        f"{version.state} (supported {noun} families: {supported})"
    )


def _supported_families(kind: str) -> frozenset[int]:
    """Return the supported families of a kind that carries a version marker.

    Args:
        kind: The document kind.

    Returns:
        frozenset[int]: The families this release supports for kind.

    Raises:
        ValueError: If kind is the attempt report, which is versioned through
            the plan document that embeds it, or not a document kind at all.
    """
    if kind == ATTEMPT_REPORT:
        raise ValueError(
            f"{kind!r} has no version marker of its own: it is versioned "
            "through the plan document that embeds it"
        )
    families = SUPPORTED_FAMILIES.get(kind)
    if families is None:
        raise ValueError(
            f"Unknown document kind: {kind!r}. "
            f"Must be one of: {sorted(SUPPORTED_FAMILIES)}"
        )
    return families
