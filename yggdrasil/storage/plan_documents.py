"""Backend-neutral plan-document construction and interpretation.

Single source of truth for the plan document shape so the CouchDB and SQLite
plan stores cannot drift. Extracted from ``PlanDBManager`` (which now
delegates here).

Plan generation:
    Every document written by ``save_plan`` carries a fresh opaque
    ``plan_generation`` ID. It identifies one planned version of the plan:
    approval changes and run-token increments keep it, while regenerating the
    plan replaces it. It is never a stable default, so a plan that is deleted
    and later recreated under the same ID cannot be mistaken for its earlier
    version. A document of a supported family always carries a generation,
    and one without a usable generation is refused for execution: nothing
    adds a generation to an existing document.

Replacing a plan:
    Planning replaces the whole document in a plan's slot. Whether it may,
    and which ``created_at`` the replacement keeps, is decided by
    ``check_replaceable`` alone. ``build_plan_document`` applies it whenever
    it is given the slot's document, so every store that builds before
    writing refuses the same slots without writing.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from yggdrasil.contract import CURRENT_FAMILIES, PLAN_DOCUMENT, SUPPORTED_FAMILIES
from yggdrasil.contract.timestamps import (
    canonical_from_legacy,
    is_canonical,
    is_date_time,
)
from yggdrasil.contract.versions import (
    INVALID,
    SCHEMA_VERSION_FIELD,
    UNSUPPORTED,
    UNVERSIONED,
    version_of,
)
from yggdrasil.flow.model import Plan
from yggdrasil.flow.utils.ygg_time import utcnow_iso
from yggdrasil.storage.errors import UnsupportedDocumentError

# Valid values for the execution_authority field
VALID_EXECUTION_AUTHORITIES = frozenset({"daemon", "run_once"})

# Document field holding the generation ID of the stored plan version.
PLAN_GENERATION_FIELD = "plan_generation"

# Document field holding the result recorded when an execution request was
# finalized; see ExecutionFinalization in yggdrasil/storage/plan_updates.py.
LAST_FINALIZED_EXECUTION_FIELD = "last_finalized_execution"

# Document field holding the document's creation time, which regeneration
# keeps.
CREATED_AT_FIELD = "created_at"


def json_safe(value: Any) -> Any:
    """Recursively coerce plan documents into JSON-serializable structures.

    Realm planners can return pathlib.Path (or other non-JSON types) inside
    params or preview payloads. This helper walks the structure and converts:
    - Path -> str
    - set/tuple -> list
    - dict/list elements recursively
    """
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {k: json_safe(v) for k, v in value.items()}
    if isinstance(value, list | tuple | set):
        return [json_safe(v) for v in value]
    return value


def validate_execution_authority(authority: str) -> None:
    """Raise ValueError if execution_authority is invalid."""
    if authority not in VALID_EXECUTION_AUTHORITIES:
        raise ValueError(
            f"Invalid execution_authority: {authority!r}. "
            f"Must be one of: {sorted(VALID_EXECUTION_AUTHORITIES)}"
        )


def new_plan_generation() -> str:
    """Return a fresh, globally unique plan generation ID.

    Returns:
        str: An opaque random ID. Generations are compared only for equality;
        they carry no order.
    """
    return uuid.uuid4().hex


def plan_generation_of(doc: dict[str, Any]) -> str | None:
    """Return a plan document's generation ID, if it has a usable one.

    Anything other than a non-empty string counts as no generation, so a
    missing, empty, or mistyped value can never be matched against a
    generation an execution captured.

    Args:
        doc: A persisted plan document.

    Returns:
        str | None: The generation ID, or None if the document has none.
    """
    generation = doc.get(PLAN_GENERATION_FIELD)
    if isinstance(generation, str) and generation:
        return generation
    return None


def check_replaceable(doc_id: str, existing: Mapping[str, Any]) -> str:
    """Return the ``created_at`` that a replacement of a plan's document keeps.

    Planning may replace an unversioned (pre-contract) document or a document
    of a supported family. It refuses a document whose ``schema_version`` is
    invalid or names an unsupported family, such as a newer family that the
    replacement would downgrade. The marker is judged first, so such a
    document is refused for its family whatever its ``created_at`` holds.

    The replacement keeps the document's creation instant, spelled
    canonically. A supported document's value must already be canonical,
    whole seconds (``2026-10-22T10:00:00Z``) or a fraction
    (``2026-10-22T10:00:00.25Z``), and is kept verbatim. An unversioned
    document's value may be either spelling that earlier releases wrote, a
    whole-second UTC time ending in ``Z`` or ``+00:00``, and is re-spelled
    with ``Z``; earlier releases never wrote a fraction, so an unversioned
    document with one is refused. Any other value is refused: the build time
    is never substituted and precision is never truncated.

    Args:
        doc_id: ID of the plan whose slot is being replaced.
        existing: The document in the slot, as read.

    Returns:
        str: The ``created_at`` the replacement carries.

    Raises:
        UnsupportedDocumentError: If planning may not replace existing.
    """
    supported = SUPPORTED_FAMILIES[PLAN_DOCUMENT]
    version = version_of(PLAN_DOCUMENT, existing)
    if version.state in (INVALID, UNSUPPORTED):
        raise UnsupportedDocumentError(
            doc_id=doc_id,
            field=SCHEMA_VERSION_FIELD,
            recorded=version.recorded,
            reason=version.state,
            supported=supported,
        )

    if CREATED_AT_FIELD not in existing:
        raise UnsupportedDocumentError(
            doc_id=doc_id,
            field=CREATED_AT_FIELD,
            recorded=None,
            reason="missing",
            supported=supported,
        )
    recorded = existing[CREATED_AT_FIELD]
    if version.state == UNVERSIONED:
        kept = canonical_from_legacy(recorded)
    else:
        kept = recorded if is_canonical(recorded) else None
    if kept is None:
        raise UnsupportedDocumentError(
            doc_id=doc_id,
            field=CREATED_AT_FIELD,
            recorded=recorded,
            reason="unsupported" if is_date_time(recorded) else "malformed",
            supported=supported,
        )
    return kept


def build_plan_document(
    plan: Plan,
    realm: str,
    scope: dict[str, Any],
    *,
    auto_run: bool = False,
    execution_authority: str = "daemon",
    execution_owner: str | None = None,
    preview: dict[str, Any] | None = None,
    source_doc_id: str | None = None,
    source_doc_rev: str | None = None,
    notes: str | None = None,
    existing: dict[str, Any] | None = None,
    now: str | None = None,
    plan_generation: str | None = None,
) -> dict[str, Any]:
    """Build the canonical plan document for persistence.

    Every call builds a new plan version: the document gets a fresh
    ``plan_generation``, and ``schema_version`` names the family of the
    release that built it (``CURRENT_FAMILIES`` in
    ``yggdrasil/contract/__init__.py``). Backend-specific fields (``_rev``)
    are the caller's responsibility.

    On regeneration (``existing`` provided), only the creation instant
    carries over, as ``check_replaceable`` decides; everything else is
    rebuilt. The execution tokens are reset so the new plan is eligible for
    execution, and approval and request stamps, any recorded execution
    result and fields written by others are dropped. A slot that planning may
    not replace is refused before anything is built.

    Args:
        plan: The Plan object to persist (plan.plan_id is used as _id).
        realm: Realm identifier.
        scope: Scope dict with 'kind' and 'id' keys.
        auto_run: If True, set status='approved'; else status='draft'.
        execution_authority: "daemon" (default) or "run_once".
        execution_owner: Unique token for run_once isolation.
        preview: Optional preview data for UI display.
        source_doc_id: Optional source document ID that triggered this plan.
        source_doc_rev: Optional source document revision.
        notes: Optional notes about the plan.
        existing: The document currently in the plan's slot, as read, if
            there is one.
        now: Timestamp override for ``updated_at``, and for ``created_at``
            when there is no existing document, written as given; defaults
            to utcnow_iso().
        plan_generation: Generation ID override; defaults to a fresh one from
            new_plan_generation().

    Returns:
        dict: JSON-safe plan document including ``_id``.

    Raises:
        ValueError: If execution_authority is invalid or plan.plan_id missing.
        UnsupportedDocumentError: If planning may not replace existing (see
            check_replaceable).
    """
    validate_execution_authority(execution_authority)
    doc_id = plan.plan_id
    if not doc_id:
        raise ValueError("plan.plan_id is required for persistence")

    timestamp = now or utcnow_iso()
    created_at = timestamp if existing is None else check_replaceable(doc_id, existing)

    plan_doc: dict[str, Any] = {
        "_id": doc_id,
        SCHEMA_VERSION_FIELD: CURRENT_FAMILIES[PLAN_DOCUMENT],
        "realm": realm,
        "scope": scope,
        "status": "approved" if auto_run else "draft",
        "plan": plan.to_dict(),
        "preview": preview or {},
        PLAN_GENERATION_FIELD: plan_generation or new_plan_generation(),
        "run_token": 0,
        "executed_run_token": -1,
        "execution_authority": execution_authority,
        "execution_owner": execution_owner,
        CREATED_AT_FIELD: created_at,
        "updated_at": timestamp,
    }

    if source_doc_id:
        plan_doc["source_doc_id"] = source_doc_id
    if source_doc_rev:
        plan_doc["source_doc_rev"] = source_doc_rev
    if notes:
        plan_doc["notes"] = notes

    return json_safe(plan_doc)


def plan_model_from_document(
    doc: dict[str, Any],
    logger: logging.Logger,
) -> Plan | None:
    """Deserialize the ``plan`` field of a plan document into a Plan model.

    Args:
        doc: The persisted plan document.
        logger: Logger for warnings/errors.

    Returns:
        Plan or None if the document has no plan field or fails to parse.
    """
    doc_id = doc.get("_id", "<unknown>")
    plan_data = doc.get("plan")
    if not plan_data:
        logger.warning("Plan document '%s' has no 'plan' field", doc_id)
        return None

    try:
        # Path reconstruction is handled by Engine via
        # coerce_params_to_signature_types() based on step type hints;
        # params remain strings in the persisted document.
        return Plan.from_dict(plan_data)
    except (KeyError, TypeError) as e:
        logger.error("Failed to deserialize plan '%s': %s", doc_id, e)
        return None


def plan_summary_from_document(doc: dict[str, Any]) -> dict[str, Any]:
    """Extract the minimal display summary from a plan document.

    Approval ``status`` and the outcome of the last finalized execution are
    reported as separate fields: an approved plan whose tokens are equal has
    finished its last request, not necessarily succeeded at it.
    """
    last_execution = doc.get(LAST_FINALIZED_EXECUTION_FIELD)
    return {
        "status": doc.get("status", "unknown"),
        "execution_authority": doc.get("execution_authority", "daemon"),
        "execution_owner": doc.get("execution_owner"),
        "updated_at": doc.get("updated_at", "unknown"),
        "realm": doc.get("realm", "unknown"),
        "run_token": doc.get("run_token", 0),
        "executed_run_token": doc.get("executed_run_token", -1),
        "plan_generation": plan_generation_of(doc),
        "last_finalized_outcome": (
            last_execution.get("outcome") if isinstance(last_execution, dict) else None
        ),
    }
