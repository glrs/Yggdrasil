"""Value helpers for a plan's identity: its generation and its snapshot's ID.

None of them checks the document's version. They are for a caller that has
already established the family, as the daemon's gates do; on a document of
another family they return whatever the named fields hold, which is not a
conclusion about that document. None of them raises on stored content.

A snapshot's ID is built forwards from a plan, never taken apart: there is
no parser.
"""

from __future__ import annotations

from collections.abc import Mapping

from yggdrasil.contract import PLAN_STATUS
from yggdrasil.contract.fields import (
    PLAN_GENERATION_FIELD,
    REALM_FIELD,
    SCOPE_FIELD,
    SCOPE_ID_FIELD,
    SCOPE_KIND_FIELD,
)
from yggdrasil.contract.read import _values

# The partition prefix of a scope kind that has its own; any other kind is its
# own prefix.
_PARTITION_PREFIXES = {"project": "proj", "flowcell": "fc"}


def generation_of(document: object) -> str | None:
    """Return a plan document's ``plan_generation``, if it has a usable one.

    Anything other than a non-empty string is no generation, so a missing,
    empty or mistyped value never matches a generation captured elsewhere.

    Args:
        document: The plan document, of any shape.

    Returns:
        str | None: The generation; None if there is none, or if document is
        not an object.
    """
    if not isinstance(document, Mapping):
        return None
    generation = document.get(PLAN_GENERATION_FIELD)
    return generation if _values.is_identifier(generation) else None


def partition_name(kind: object, scope_id: object) -> str:
    """Format the partition of a scope, validating nothing.

    ``proj-<id>`` for the kind ``project``, ``fc-<id>`` for ``flowcell``,
    ``<kind>-<id>`` for any other. The callers decide what they accept.

    Args:
        kind: The scope's kind.
        scope_id: The scope's ID.

    Returns:
        str: The partition name.
    """
    prefix = _PARTITION_PREFIXES.get(kind) if isinstance(kind, str) else None
    return f"{prefix if prefix is not None else kind}-{scope_id}"


def snapshot_id(
    realm: object, scope_kind: object, scope_id: object, plan_id: object
) -> str | None:
    """Return the ID of a plan's ``plan_status`` snapshot.

    The ID is ``<partition>:plan_status:<realm>:<plan_id>``, with the
    partition of :func:`partition_name`.

    Args:
        realm: The plan's realm.
        scope_kind: The kind of the plan's scope.
        scope_id: The ID of the plan's scope.
        plan_id: The plan's ID.

    Returns:
        str | None: The ID; None unless all four are non-empty strings. Never
        raises.
    """
    if not all(
        _values.is_identifier(value) for value in (realm, scope_kind, scope_id, plan_id)
    ):
        return None
    return f"{partition_name(scope_kind, scope_id)}:{PLAN_STATUS}:{realm}:{plan_id}"


def snapshot_id_for(document: object, *, plan_id: object) -> str | None:
    """Return the ID of the snapshot of the plan a document describes.

    Reads the document's ``realm`` and ``scope``; the plan ID is the one the
    document was read under, never its own ``_id``.

    Args:
        document: The plan document, of any shape.
        plan_id: The ID the document was requested or stored under.

    Returns:
        str | None: The ID, as :func:`snapshot_id` builds it; None when it
        cannot be derived. Never raises.
    """
    if not isinstance(document, Mapping):
        return None
    scope = document.get(SCOPE_FIELD)
    if not isinstance(scope, Mapping):
        return None
    return snapshot_id(
        document.get(REALM_FIELD),
        scope.get(SCOPE_KIND_FIELD),
        scope.get(SCOPE_ID_FIELD),
        plan_id,
    )
