"""The partition a plan's snapshot is stored under, derived from the plan's scope.

The CouchDB ops sink and the SQLite store key each ``plan_status`` snapshot
by the partition of its plan's scope. The name is formatted by
:func:`yggdrasil.contract.read.partition_name`, which every reader that
derives a snapshot's ID uses too; this module adds only what the writers
accept.
"""

from __future__ import annotations

from typing import Any

from yggdrasil.contract.fields import SCOPE_ID_FIELD, SCOPE_KIND_FIELD
from yggdrasil.contract.read import partition_name


def partition_key(scope: dict[str, Any]) -> str:
    """Return the partition a scope's snapshots are stored under.

    Any truthy ``kind`` and ``id`` are accepted, and formatted by
    :func:`~yggdrasil.contract.read.partition_name`::

        {"kind": "project", "id": "P12345"}   -> "proj-P12345"
        {"kind": "flowcell", "id": "A22FN2"}  -> "fc-A22FN2"
        {"kind": "bundle", "id": "B12345"}    -> "bundle-B12345"

    Args:
        scope: The plan's scope.

    Returns:
        str: The partition name.

    Raises:
        ValueError: If the scope's ``kind`` or ``id`` is absent or falsy.
    """
    kind = scope.get(SCOPE_KIND_FIELD)
    scope_id = scope.get(SCOPE_ID_FIELD)
    if not kind or not scope_id:
        raise ValueError(f"Bad scope: {scope}")
    return partition_name(kind, scope_id)
