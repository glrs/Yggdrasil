"""Phase-specific CouchDB clients for realm data access.

Provides:
  _CouchDBSyncOps        — internal shared sync read helpers
  CouchDBPlanningClient  — async read-only client for planning phase
  CouchDBExecutionClient — sync read/write client for execution phase
  _CouchDBWriteOps       — internal write helpers (create/update/upsert)
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Literal
from uuid import uuid4

from ibm_cloud_sdk_core.api_exception import ApiException
from requests.exceptions import RequestException
from urllib3.exceptions import HTTPError as Urllib3HTTPError

from yggdrasil.flow.data_access.errors import (
    DataAccessDeniedError,
    DataAccessNotFoundError,
    DataAccessQueryError,
    DataAccessWriteError,
)
from yggdrasil.flow.data_access.models import (
    DataAccessTraceContext,
    DataAccessWriteResult,
)

if TYPE_CHECKING:
    from lib.couchdb.couchdb_connection import CouchDBHandler

_logger = logging.getLogger(__name__)

_DEFAULT_MAX_LIMIT = 200

# Both query and write paths wrap the same transport exception types.
_QUERY_EXCEPTIONS = (ApiException, RequestException, ConnectionError, Urllib3HTTPError)
_WRITE_EXCEPTIONS = (ApiException, RequestException, ConnectionError, Urllib3HTTPError)


# ---------------------------------------------------------------------------
# Internal shared read helpers
# ---------------------------------------------------------------------------


class _CouchDBSyncOps:
    """Internal synchronous CouchDB read helpers shared by both clients.

    Not part of the public API. Both CouchDBPlanningClient (which calls
    these via asyncio.to_thread) and CouchDBExecutionClient (which calls
    these directly) use this class.
    """

    def __init__(self, handler: CouchDBHandler, options: dict[str, Any]) -> None:
        self._handler = handler
        self._max_limit: int = options.get("max_limit", _DEFAULT_MAX_LIMIT)

    def _get_sync(self, doc_id: str) -> dict[str, Any] | None:
        try:
            return self._handler.fetch_document_by_id(doc_id)
        except _QUERY_EXCEPTIONS as exc:
            raise DataAccessQueryError(f"Failed to fetch '{doc_id}': {exc}") from exc

    def _find_sync(
        self, selector: dict[str, Any], *, limit: int | None = None
    ) -> list[dict[str, Any]]:
        effective_limit = (
            min(limit, self._max_limit) if limit is not None else self._max_limit
        )
        try:
            return self._handler.find_documents(selector, limit=effective_limit)
        except _QUERY_EXCEPTIONS as exc:
            raise DataAccessQueryError(f"Query failed: {exc}") from exc


# ---------------------------------------------------------------------------
# Planning client — async reads only
# ---------------------------------------------------------------------------


class CouchDBPlanningClient:
    """Async read-only CouchDB client for planning-phase handlers.

    Exposes get/find/find_one/fetch_by_field/require/require_one as async
    methods. No put() — planning writes are not supported by design.
    """

    def __init__(self, ops: _CouchDBSyncOps) -> None:
        self._ops = ops

    async def get(self, doc_id: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._ops._get_sync, doc_id)

    async def find(
        self, selector: dict[str, Any], *, limit: int | None = None
    ) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._ops._find_sync, selector, limit=limit)

    async def find_one(self, selector: dict[str, Any]) -> dict[str, Any] | None:
        results = await self.find(selector, limit=1)
        return results[0] if results else None

    async def fetch_by_field(
        self, field: str, value: Any, *, limit: int | None = None
    ) -> list[dict[str, Any]]:
        return await self.find({field: {"$eq": value}}, limit=limit)

    async def require(self, doc_id: str) -> dict[str, Any]:
        doc = await self.get(doc_id)
        if doc is None:
            raise DataAccessNotFoundError(f"Document '{doc_id}' not found")
        return doc

    async def require_one(self, selector: dict[str, Any]) -> dict[str, Any]:
        doc = await self.find_one(selector)
        if doc is None:
            raise DataAccessNotFoundError(f"No document matched selector {selector!r}")
        return doc

    # put() is intentionally absent. Planning phase is read-only by design.


# ---------------------------------------------------------------------------
# Internal write helpers
# ---------------------------------------------------------------------------


def _validate_clean_doc(doc: dict[str, Any]) -> None:
    reserved = {"_id", "_rev"} & doc.keys()
    if reserved:
        raise ValueError(
            f"doc must not contain CouchDB metadata fields {sorted(reserved)!r}. "
            "DataAccess manages _id and _rev internally. "
            "Pass a clean body dict, or call client.clean_doc(doc) before save()."
        )


class _CouchDBWriteOps:
    """Internal CouchDB write helpers used only by CouchDBExecutionClient.

    Not part of the public API. Uses CouchDBHandler.put_document() and
    CouchDBHandler.fetch_document_by_id() directly.

    All methods return a (status, old_rev, new_rev) tuple on success and
    raise DataAccessWriteError on expected failure conditions, including
    failures that occur during internal _rev fetches.
    """

    def __init__(self, handler: CouchDBHandler) -> None:
        self._handler = handler

    def _fetch_existing_for_write(self, doc_id: str) -> dict[str, Any] | None:
        """Fetch existing document as part of a write operation.

        This is an adapter-internal read — it does NOT require realm-level
        'read' permission. Wraps transport/API errors as DataAccessWriteError
        (not DataAccessQueryError) because the failure occurs within a write
        operation context.

        Returns:
            The document dict if found, or None if absent.

        Raises:
            DataAccessWriteError: If the fetch fails due to network or API error.
        """
        try:
            return self._handler.fetch_document_by_id(doc_id)
        except _WRITE_EXCEPTIONS as exc:
            raise DataAccessWriteError(
                f"Failed to fetch '{doc_id}' for write pre-check: {exc}"
            ) from exc

    def _create(
        self, doc_id: str, doc: dict[str, Any]
    ) -> tuple[Literal["created", "updated"], str | None, str | None]:
        """Create a document; fail if it already exists.

        Does NOT fetch the existing document first. If the document exists,
        CouchDB returns 409 and we raise immediately (no retry).

        Raises:
            DataAccessWriteError: Document already exists (409).
            DataAccessWriteError: Other backend write failure.
        """
        try:
            result = self._handler.put_document(doc_id, doc)
            return "created", None, result.get("rev")
        except ApiException as exc:
            if exc.status_code == 409:
                raise DataAccessWriteError(
                    f"Cannot create '{doc_id}': document already exists (409 conflict)."
                ) from exc
            raise DataAccessWriteError(
                f"CouchDB error creating '{doc_id}': {exc.status_code} {exc.message}"
            ) from exc
        except _WRITE_EXCEPTIONS as exc:
            raise DataAccessWriteError(
                f"Network/transport error creating '{doc_id}': {exc}"
            ) from exc

    def _update(
        self, doc_id: str, doc: dict[str, Any]
    ) -> tuple[Literal["created", "updated"], str | None, str | None]:
        """Update a document; fail if it does not exist.

        Fetches the existing _rev first. If the document is absent (fetch returns
        None), raises immediately. If the rev changes between fetch and write (409),
        raises immediately — update mode does not retry.

        Raises:
            DataAccessWriteError: Document not found (fetch returned None).
            DataAccessWriteError: Fetch failed due to network/API error.
            DataAccessWriteError: Existing document is missing '_rev'.
            DataAccessWriteError: Rev conflict (409) or other write failure.
        """
        existing = self._fetch_existing_for_write(doc_id)
        if existing is None:
            raise DataAccessWriteError(
                f"Cannot update '{doc_id}': document does not exist."
            )
        old_rev: str | None = existing.get("_rev")
        if not old_rev:
            raise DataAccessWriteError(
                f"Cannot update '{doc_id}': existing document is missing '_rev'. "
                "The document may be malformed."
            )
        try:
            result = self._handler.put_document(doc_id, doc, rev=old_rev)
            return "updated", old_rev, result.get("rev")
        except ApiException as exc:
            if exc.status_code == 409:
                raise DataAccessWriteError(
                    f"Cannot update '{doc_id}': revision conflict (409). "
                    "Document was modified concurrently."
                ) from exc
            raise DataAccessWriteError(
                f"CouchDB error updating '{doc_id}': {exc.status_code} {exc.message}"
            ) from exc
        except _WRITE_EXCEPTIONS as exc:
            raise DataAccessWriteError(
                f"Network/transport error updating '{doc_id}': {exc}"
            ) from exc

    def _upsert(
        self, doc_id: str, doc: dict[str, Any], *, logger: logging.Logger
    ) -> tuple[Literal["created", "updated"], str | None, str | None]:
        """Create or update a document.

        Dispatches to _upsert_absent (document not found at fetch time) or
        _upsert_present (document found at fetch time). Both paths retry once
        on 409 conflict.
        """
        existing = self._fetch_existing_for_write(doc_id)
        if existing is None:
            return self._upsert_absent(doc_id, doc, logger=logger)
        return self._upsert_present(doc_id, doc, existing.get("_rev"), logger=logger)

    def _upsert_absent(
        self, doc_id: str, doc: dict[str, Any], *, logger: logging.Logger
    ) -> tuple[Literal["created", "updated"], str | None, str | None]:
        """Create path: document was absent at initial fetch.

        Tries to create the document. On 409 (another process created it between
        fetch and create), refetches:
        - If now present: updates once.
        - If still absent: calls _create() once more (bounded retry).
        Raises DataAccessWriteError on any non-retriable failure or a second 409.
        """
        try:
            result = self._handler.put_document(doc_id, doc)
            return "created", None, result.get("rev")
        except ApiException as exc:
            if exc.status_code != 409:
                raise DataAccessWriteError(
                    f"CouchDB error creating '{doc_id}' during upsert: "
                    f"{exc.status_code} {exc.message}"
                ) from exc
            # 409: another process created the document between fetch and create.
            # Refetch and write once more.
            logger.warning(
                "upsert create-path conflict on '%s': document appeared after "
                "fetch; refetching.",
                doc_id,
            )
            retry_existing = self._fetch_existing_for_write(doc_id)
            if retry_existing is None:
                return self._create(doc_id, doc)
            retry_rev = retry_existing.get("_rev")
            if not retry_rev:
                raise DataAccessWriteError(
                    f"Cannot upsert '{doc_id}': refetched document has no _rev "
                    "after create-path 409."
                ) from exc
            try:
                retry_result = self._handler.put_document(doc_id, doc, rev=retry_rev)
                return "updated", retry_rev, retry_result.get("rev")
            except ApiException as retry_exc:
                if retry_exc.status_code == 409:
                    raise DataAccessWriteError(
                        f"Cannot upsert '{doc_id}': two consecutive 409 conflicts. "
                        "Document is being modified at high frequency."
                    ) from retry_exc
                raise DataAccessWriteError(
                    f"CouchDB error on upsert create-path retry for '{doc_id}': "
                    f"{retry_exc.status_code} {retry_exc.message}"
                ) from retry_exc
            except _WRITE_EXCEPTIONS as retry_exc:
                raise DataAccessWriteError(
                    f"Network/transport error on upsert create-path retry for '{doc_id}': "
                    f"{retry_exc}"
                ) from retry_exc
        except _WRITE_EXCEPTIONS as exc:
            raise DataAccessWriteError(
                f"Network/transport error creating '{doc_id}' during upsert: {exc}"
            ) from exc

    def _upsert_present(
        self,
        doc_id: str,
        doc: dict[str, Any],
        old_rev: str | None,
        *,
        logger: logging.Logger,
    ) -> tuple[Literal["created", "updated"], str | None, str | None]:
        """Update path: document was present at initial fetch.

        Tries to update the document with the fetched _rev. On 409 (rev changed
        between fetch and write), refetches:
        - If now present: updates once more.
        - If absent: calls _create() (document was deleted concurrently).
        Raises DataAccessWriteError on any non-retriable failure or a second 409.
        """
        if not old_rev:
            raise DataAccessWriteError(
                f"Cannot upsert '{doc_id}': existing document is missing '_rev'."
            )
        try:
            result = self._handler.put_document(doc_id, doc, rev=old_rev)
            return "updated", old_rev, result.get("rev")
        except ApiException as exc:
            if exc.status_code != 409:
                raise DataAccessWriteError(
                    f"CouchDB error upserting '{doc_id}': {exc.status_code} {exc.message}"
                ) from exc
            # 409 conflict — retry once
            logger.warning(
                "upsert conflict on '%s' (rev %s changed between fetch and write); "
                "retrying once.",
                doc_id,
                old_rev,
            )
            retry_existing = self._fetch_existing_for_write(doc_id)
            if retry_existing is None:
                return self._create(doc_id, doc)
            retry_rev: str | None = retry_existing.get("_rev")
            if not retry_rev:
                raise DataAccessWriteError(
                    f"Cannot upsert '{doc_id}': retry document is missing '_rev'."
                )
            try:
                retry_result = self._handler.put_document(doc_id, doc, rev=retry_rev)
                return "updated", retry_rev, retry_result.get("rev")
            except ApiException as retry_exc:
                if retry_exc.status_code == 409:
                    raise DataAccessWriteError(
                        f"Cannot upsert '{doc_id}': two consecutive 409 conflicts. "
                        "Document is being modified at high frequency."
                    ) from retry_exc
                raise DataAccessWriteError(
                    f"CouchDB error on upsert retry for '{doc_id}': "
                    f"{retry_exc.status_code} {retry_exc.message}"
                ) from retry_exc
            except _WRITE_EXCEPTIONS as retry_exc:
                raise DataAccessWriteError(
                    f"Network/transport error on upsert retry for '{doc_id}': {retry_exc}"
                ) from retry_exc
        except _WRITE_EXCEPTIONS as exc:
            raise DataAccessWriteError(
                f"Network/transport error upserting '{doc_id}': {exc}"
            ) from exc

    def _post_new(
        self, doc: dict[str, Any]
    ) -> tuple[str, Literal["created"], None, str | None]:
        """Create a document via HTTP POST, letting CouchDB generate the ID.

        Returns a 4-tuple (resolved_doc_id, "created", None, new_rev).

        Raises:
            DataAccessWriteError: On any backend failure.
        """
        try:
            result = self._handler.post_document(doc)
            return result["id"], "created", None, result.get("rev")
        except _WRITE_EXCEPTIONS as exc:
            raise DataAccessWriteError(
                f"CouchDB error creating document via POST: {exc}"
            ) from exc

    def _find_for_write(
        self, selector: dict[str, Any], *, limit: int
    ) -> list[dict[str, Any]]:
        """Run a Mango selector query as part of a write operation.

        Wraps failures as DataAccessWriteError (not DataAccessQueryError) because
        the failure occurs in a write operation context.
        """
        try:
            return self._handler.find_documents(selector, limit=limit)
        except _WRITE_EXCEPTIONS as exc:
            raise DataAccessWriteError(
                f"Mango query failed during save: {exc}"
            ) from exc

    def _view_for_write(
        self,
        document: str,
        view_name: str,
        *,
        key: Any,
        limit: int,
        include_docs: bool,
    ) -> list[dict[str, Any]]:
        """Query a CouchDB view as part of a write operation.

        Wraps failures as DataAccessWriteError. Always passes reduce=False to
        ensure individual map rows with 'id' fields are returned.
        """
        try:
            result = self._handler.query_view(
                document,
                view_name,
                key=key,
                limit=limit,
                include_docs=include_docs,
                reduce=False,
            )
            return result.get("rows", [])
        except _WRITE_EXCEPTIONS as exc:
            raise DataAccessWriteError(
                f"View query '{document}/{view_name}' failed during save: {exc}"
            ) from exc

    def _save_by_selector(
        self,
        doc: dict[str, Any],
        selector: dict[str, Any],
        *,
        mode: Literal["create", "update", "upsert"],
        logger: logging.Logger,
    ) -> tuple[str, Literal["created", "updated"], str | None, str | None]:
        """Save a document identified by a Mango selector.

        Finds documents matching selector with limit=2, then applies mode:

        create:  0 matches → create via POST; 1+ matches → DataAccessWriteError.
        update:  0 matches → DataAccessWriteError; 1 match → update; >1 → error.
        upsert:  0 matches → create via POST; 1 match → update; >1 → error.

        Returns a 4-tuple (resolved_doc_id, status, old_rev, new_rev).

        Retry on 409 (update path): inherited from _upsert_present, which
        refetches by doc_id. The selector is not re-run on retry; this is
        acceptable because the document's identity is already known from the
        initial match.
        """
        matches = self._find_for_write(selector, limit=2)
        count = len(matches)
        if count > 1:
            raise DataAccessWriteError(
                f"save() by selector matched {count} documents (limit=2); "
                "expected 0 or 1. Refine the selector to match at most one document."
            )
        if count == 0:
            if mode == "update":
                raise DataAccessWriteError(
                    "save() mode='update' by selector: no matching document found."
                )
            return self._post_new(doc)
        # count == 1
        if mode == "create":
            raise DataAccessWriteError(
                "save() mode='create' by selector: a matching document already exists."
            )
        existing = matches[0]
        doc_id: str = existing["_id"]
        old_rev: str | None = existing.get("_rev")
        status, result_old_rev, new_rev = self._upsert_present(
            doc_id, doc, old_rev, logger=logger
        )
        return doc_id, status, result_old_rev, new_rev

    def _save_by_view(
        self,
        doc: dict[str, Any],
        view_spec: dict[str, Any],
        *,
        mode: Literal["create", "update", "upsert"],
        logger: logging.Logger,
    ) -> tuple[str, Literal["created", "updated"], str | None, str | None]:
        """Save a document identified by a CouchDB view row.

        Queries the view with limit=2, then applies mode:

        create:  0 rows → create via POST; 1+ rows → DataAccessWriteError.
        update:  0 rows → DataAccessWriteError; 1 row → update; >1 → error.
        upsert:  0 rows → create via POST; 1 row → update; >1 → error.

        The target view must be a map-only view; reduce=False is always enforced.

        view_spec keys:
            "design":       Design document name (required).
            "view":         View name (required).
            "key":          View key to filter rows (required, must not be None).
            "include_docs": If True (default), embed full docs in rows to avoid
                            an extra fetch per update. If False, _rev is fetched
                            separately.

        Returns a 4-tuple (resolved_doc_id, status, old_rev, new_rev).
        """
        design = view_spec["design"]
        view_name = view_spec["view"]
        key = view_spec["key"]
        include_docs = view_spec.get("include_docs", True)

        rows = self._view_for_write(
            design, view_name, key=key, limit=2, include_docs=include_docs
        )
        count = len(rows)
        if count > 1:
            raise DataAccessWriteError(
                f"save() by view '{design}/{view_name}' matched {count} rows (limit=2); "
                "expected 0 or 1."
            )
        if count == 0:
            if mode == "update":
                raise DataAccessWriteError(
                    f"save() mode='update' by view '{design}/{view_name}': no matching row found."
                )
            return self._post_new(doc)
        # count == 1
        if mode == "create":
            raise DataAccessWriteError(
                f"save() mode='create' by view '{design}/{view_name}': a matching row already exists."
            )
        row = rows[0]
        doc_id = row["id"]
        row_doc = row.get("doc")
        if include_docs and isinstance(row_doc, dict):
            old_rev = row_doc.get("_rev")
        else:
            existing = self._fetch_existing_for_write(doc_id)
            if existing is None:
                if mode == "update":
                    raise DataAccessWriteError(
                        f"save() mode='update' by view: row referenced doc '{doc_id}' "
                        "but it no longer exists."
                    )
                logger.warning(
                    "View row referenced doc '%s' but it no longer exists; creating new document.",
                    doc_id,
                )
                return self._post_new(doc)
            old_rev = existing.get("_rev")

        status, result_old_rev, new_rev = self._upsert_present(
            doc_id, doc, old_rev, logger=logger
        )
        return doc_id, status, result_old_rev, new_rev


# ---------------------------------------------------------------------------
# Execution client — sync reads + writes
# ---------------------------------------------------------------------------


class CouchDBExecutionClient:
    """Sync read/write CouchDB client for execution-phase steps.

    Exposes synchronous get/find/find_one/fetch_by_field/require/require_one
    plus put() for authorized writes. No async wrappers.
    """

    def __init__(
        self,
        ops: _CouchDBSyncOps,
        write_ops: _CouchDBWriteOps,
        *,
        permissions: frozenset[str],
        realm_id: str,
        connection_name: str,
        resource: str,
        trace_context: DataAccessTraceContext | None,
    ) -> None:
        self._ops = ops
        self._write_ops = write_ops
        self._permissions = permissions
        self._realm_id = realm_id
        self._connection_name = connection_name
        self._resource = resource
        self._trace = trace_context

    def _check_read_permission(self) -> None:
        if "read" not in self._permissions:
            raise DataAccessDeniedError(
                f"Realm '{self._realm_id}' does not have 'read' permission "
                f"for connection '{self._connection_name}'."
            )

    # --- Sync reads ---

    def get(self, doc_id: str) -> dict[str, Any] | None:
        self._check_read_permission()
        return self._ops._get_sync(doc_id)

    def find(
        self, selector: dict[str, Any], *, limit: int | None = None
    ) -> list[dict[str, Any]]:
        self._check_read_permission()
        return self._ops._find_sync(selector, limit=limit)

    def find_one(self, selector: dict[str, Any]) -> dict[str, Any] | None:
        results = self.find(selector, limit=1)
        return results[0] if results else None

    def fetch_by_field(
        self, field: str, value: Any, *, limit: int | None = None
    ) -> list[dict[str, Any]]:
        return self.find({field: {"$eq": value}}, limit=limit)

    def require(self, doc_id: str) -> dict[str, Any]:
        doc = self.get(doc_id)
        if doc is None:
            raise DataAccessNotFoundError(f"Document '{doc_id}' not found")
        return doc

    def require_one(self, selector: dict[str, Any]) -> dict[str, Any]:
        doc = self.find_one(selector)
        if doc is None:
            raise DataAccessNotFoundError(f"No document matched selector {selector!r}")
        return doc

    # --- Write trace event emission ---

    def _emit(self, type_: str, **extra: Any) -> None:
        """Emit a data_access event. Silently no-ops if no emitter is configured.

        Emission failures are caught and logged; they never propagate to the caller.
        The backend write result is authoritative regardless of emission outcome.
        Within an execution attempt, the event carries the attempt's correlation
        fields like every other event of the step.
        """
        if self._trace is None or self._trace.emitter is None:
            return
        event: dict[str, Any] = {
            "type": type_,
            "realm": self._trace.realm,
            "phase": self._trace.phase,
            "plan_id": self._trace.plan_id,
            "run_id": self._trace.run_id,
            "step_id": self._trace.step_id,
            "step_name": self._trace.step_name,
            "connection": self._connection_name,
            "backend": "couchdb",
            "_spool_path": {
                "realm": self._trace.realm,
                "plan_id": self._trace.plan_id or "unknown",
                "step_id": self._trace.step_id or "unknown",
                "run_id": self._trace.run_id,
                "filename": f"data_access_write_{uuid4().hex}.json",
            },
            **extra,
        }
        if self._trace.correlation is not None:
            event.update(self._trace.correlation.event_fields())
        try:
            self._trace.emitter.emit(event)
        except Exception:
            _logger.exception(
                "Failed to emit %s event for connection '%s'",
                type_,
                self._connection_name,
            )

    # --- Write ---

    def clean_doc(self, doc: dict[str, Any]) -> dict[str, Any]:
        """Return a shallow copy of doc with CouchDB metadata fields removed.

        Strips '_id' and '_rev', preserving all other fields. Use this before
        passing a document obtained via get() or find() to save().
        """
        return {k: v for k, v in doc.items() if k not in {"_id", "_rev"}}

    def save(
        self,
        doc: dict[str, Any],
        *,
        doc_id: str | None = None,
        selector: dict[str, Any] | None = None,
        view: dict[str, Any] | None = None,
        mode: Literal["create", "update", "upsert"] = "upsert",
    ) -> DataAccessWriteResult:
        """Write a document using one of three identity modes.

        Exactly one of doc_id, selector, or view must be provided.

        Args:
            doc: Document body. Must NOT contain '_id' or '_rev'. Call
                clean_doc(doc) to strip metadata before passing here.
            doc_id: Write by explicit CouchDB document ID.
            selector: Write by Mango selector. Finds with limit=2.
            view: Write by CouchDB view row. Required keys: "design", "view",
                "key" (must not be None). Optional: "include_docs" (default True).
                The target view must be a map-only view; reduce=False is always
                enforced internally. Null-key view lookups are not supported.
            mode: Write intent:
                "create"  — fail if document already exists.
                "update"  — fail if document does not exist.
                "upsert"  — create if absent, update if present (default).

        Mode × identity semantics:

            doc_id + create  → create with provided _id; fail if exists (409).
            doc_id + update  → fetch _rev, update; fail if absent.
            doc_id + upsert  → create if absent, update if present; retry once on 409.

            selector + create  → 0 matches → POST; 1+ matches → DataAccessWriteError.
            selector + update  → 0 matches → DataAccessWriteError; 1 match → update.
            selector + upsert  → 0 matches → POST; 1 match → update; >1 → error.

            view + create/update/upsert → same semantics as selector, via view query.

        Returns:
            DataAccessWriteResult with the resolved doc_id, requested operation,
            and identity indicating which argument was used.

        Raises:
            ValueError: mode is invalid, identity count != 1, doc is dirty,
                or view dict is malformed.
            DataAccessDeniedError: Realm lacks 'write' permission.
            DataAccessWriteError: Constraint violation, backend conflict, or failure.

        Warning — non-atomic operation:
            All three identity modes perform a lookup followed by a write. A
            concurrent writer may create a document between the lookup and the
            write (selector/view modes) or modify the document's revision
            (doc_id mode). The retry-on-409 path handles revision conflicts for
            the update case. For the upsert/create path in selector/view modes,
            a race between two "no match" observations produces two documents; a
            subsequent save() call will detect this via the limit=2 check and
            raise DataAccessWriteError. This is safe when Yggdrasil is the only
            writer for the logical document, or when concurrent creation is rare
            and operationally detectable.
        """
        if mode not in {"create", "update", "upsert"}:
            raise ValueError(
                f"Invalid mode '{mode}'. Must be one of: 'create', 'update', 'upsert'."
            )

        modes_given = sum(x is not None for x in (doc_id, selector, view))
        if modes_given != 1:
            raise ValueError(
                f"save() requires exactly one of: doc_id, selector, view. Got {modes_given}."
            )

        _validate_clean_doc(doc)

        if view is not None:
            missing = {k for k in ("design", "view", "key") if k not in view}
            if missing:
                raise ValueError(
                    f"view dict is missing required keys: {sorted(missing)!r}. "
                    "Expected 'design', 'view', and 'key' keys."
                )
            if view["key"] is None:
                raise ValueError(
                    "'key' in view dict must not be None for save(). "
                    "save() requires a specific key to identify a single document. "
                    "Use save(doc, doc_id=..., mode=...) for operations without a view lookup."
                )

        identity: Literal["doc_id", "selector", "view"] = (
            "doc_id"
            if doc_id is not None
            else "selector" if selector is not None else "view"
        )
        common: dict[str, Any] = {
            "operation": mode,
            "doc_id": doc_id,
            "identity": identity,
        }
        if selector is not None:
            common["selector"] = selector
        elif view is not None:
            common["view"] = view

        if "write" not in self._permissions:
            self._emit(
                "data_access.write.denied",
                reason=f"Realm '{self._realm_id}' lacks 'write' permission.",
                **common,
            )
            raise DataAccessDeniedError(
                f"Realm '{self._realm_id}' does not have 'write' permission "
                f"for connection '{self._connection_name}'."
            )

        try:
            if doc_id is not None:
                dispatch = {
                    "create": lambda: self._write_ops._create(doc_id, doc),
                    "update": lambda: self._write_ops._update(doc_id, doc),
                    "upsert": lambda: self._write_ops._upsert(
                        doc_id, doc, logger=_logger
                    ),
                }
                status, old_rev, new_rev = dispatch[mode]()
                resolved_doc_id: str = doc_id
            elif selector is not None:
                resolved_doc_id, status, old_rev, new_rev = (
                    self._write_ops._save_by_selector(
                        doc, selector, mode=mode, logger=_logger
                    )
                )
            elif view is not None:
                resolved_doc_id, status, old_rev, new_rev = (
                    self._write_ops._save_by_view(doc, view, mode=mode, logger=_logger)
                )
        except DataAccessWriteError as exc:
            self._emit("data_access.write.failed", error=str(exc), **common)
            raise

        result = DataAccessWriteResult(
            backend="couchdb",
            connection_name=self._connection_name,
            resource=self._resource,
            operation=mode,
            identity=identity,
            doc_id=resolved_doc_id,
            status=status,
            old_rev=old_rev,
            new_rev=new_rev,
        )
        self._emit(
            "data_access.write.succeeded",
            status=status,
            old_rev=old_rev,
            new_rev=new_rev,
            **{**common, "doc_id": resolved_doc_id},
        )
        return result
