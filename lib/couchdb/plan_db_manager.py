"""
Plan database manager for yggdrasil_plans DB.

This module provides CRUD operations for plan documents, which store
intent and approval state for workflow execution.

Plan documents are stored in the dedicated `yggdrasil_plans` database,
separate from operational data (yggdrasil_ops) and project data (projects).
"""

import logging
from typing import Any, cast

from ibm_cloud_sdk_core.api_exception import ApiException
from ibmcloudant.cloudant_v1 import Document
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import RequestException, SSLError, Timeout

from lib.core_utils.logging_utils import custom_logger
from lib.couchdb.couchdb_connection import CouchDBHandler
from lib.couchdb.couchdb_defaults import DEFAULT_ENDPOINT, resolve_couchdb_params
from lib.storage.errors import PlanStoreError, RevisionConflictError
from lib.storage.plan_documents import (
    VALID_EXECUTION_AUTHORITIES,
    build_plan_document,
    json_safe,
    plan_model_from_document,
    plan_summary_from_document,
    utc_now_iso,
    validate_execution_authority,
)
from lib.storage.plan_updates import (
    ExecutionFinalization,
    FinalizationResult,
    finalize_execution,
    initialize_plan_generation,
)
from yggdrasil.flow.model import Plan

logger = custom_logger(__name__)

__all__ = ["PlanDBManager", "VALID_EXECUTION_AUTHORITIES"]

# Responses that mean "busy or restarting, ask again" rather than "this
# request is wrong". Matches the statuses the Cloudant SDK itself treats as
# transient, so disabling its retries does not change which failures are
# considered worth another attempt — only who decides to make one.
RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})

# Backwards-compatible aliases; canonical definitions live in
# lib/storage/plan_documents.py, shared with the SQLite backend.
_json_safe = json_safe
_validate_execution_authority = validate_execution_authority
_utc_now_iso = utc_now_iso


class PlanDBManager(CouchDBHandler):
    """
    Manages interactions with the 'yggdrasil_plans' database.

    Provides methods for:
    - Saving plan documents (with status, tokens, metadata)
    - Fetching plan documents by ID
    - Querying approved pending plans (for startup recovery)
    - Updating execution tokens after successful runs
    - Assigning a legacy plan its first generation
    - Finalizing finished execution requests

    Plan Document Schema:
    {
        "_id": "pln_<realm>_<scope_id>_v<version>",
        "realm": "tenx",
        "scope": {"kind": "project", "id": "P36805"},
        "status": "draft" | "approved",
        "plan": { ... serialized Plan ... },
        "preview": { ... optional preview data ... },
        "plan_generation": "<opaque ID, fresh on every save_plan>",
        "run_token": 0,
        "executed_run_token": -1,
        "last_finalized_execution": { ... set by finalize_execution ... },
        "created_at": "ISO-8601",
        "updated_at": "ISO-8601",
        ...
    }
    """

    def __init__(
        self,
        *,
        endpoint: str = DEFAULT_ENDPOINT,
        url: str | None = None,
        user_env: str | None = None,
        pass_env: str | None = None,
        db_name: str = "yggdrasil_plans",
        logger: logging.Logger | None = None,
    ) -> None:
        """Initialize connection to the plans database.

        The client is built without the SDK's automatic retries. Every retry
        of a plan-document write belongs to the caller that owns the attempt
        bound, and has to refetch and recheck generation, token and authority
        before writing again. A transport retry does neither: it would repeat
        a PUT that the shared finalizer intends to attempt exactly once, and
        turn a caller's bounded number of attempts into a multiple of it.
        Reads are left to the same rule for one policy per client; the plan
        change feed built on this handler runs its own bounded retry loop.
        """
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")
        params = resolve_couchdb_params(
            endpoint=endpoint,
            url=url,
            user_env=user_env,
            pass_env=pass_env,
        )

        super().__init__(
            db_name,
            url=params.url,
            user_env=params.user_env,
            pass_env=params.pass_env,
            logger=self._logger,
            enable_retries=False,
        )

    def save_plan(
        self,
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
    ) -> str:
        """
        Persist a plan document to the database.

        Creates or regenerates the plan document with a fresh
        ``plan_generation``. On regeneration, this resets execution tokens to
        ensure the new plan is eligible for execution. The write carries the
        ``_rev`` that was read (none for a new plan), so CouchDB rejects it if
        another writer got there first; the conflict is raised, not retried,
        because this call's planning intent may be older than the winner's.

        The document ID is taken from ``plan.plan_id``, which is owned by the
        realm/planner. PlanDBManager is a generic CRUD layer and never derives
        the document ID from (realm, scope).

        Args:
            plan: The Plan object to persist (plan.plan_id is used as _id)
            realm: Realm identifier
            scope: Scope dict with 'kind' and 'id' keys
            auto_run: If True, set status='approved'; else status='draft'
            execution_authority: Who has authority to execute - "daemon" (default) or "run_once"
            execution_owner: Unique token for run_once isolation (e.g., "run_once:<uuid>")
            preview: Optional preview data for UI display
            source_doc_id: Optional source document ID that triggered this plan
            source_doc_rev: Optional source document revision
            notes: Optional notes about the plan

        Returns:
            str: The document ID of the persisted plan (same as plan.plan_id)

        Raises:
            ValueError: If execution_authority is invalid or plan.plan_id is missing
            RevisionConflictError: If another writer created or changed the plan
                document after it was read (CouchDB 409). Nothing was written.
            ApiException: On other database errors
        """
        # Validate inputs before touching the database
        validate_execution_authority(execution_authority)
        doc_id = plan.plan_id
        if not doc_id:
            raise ValueError("plan.plan_id is required for persistence")

        # Check for existing document to get _rev
        existing = self.fetch_document_by_id(doc_id)
        rev = existing.get("_rev") if existing else None

        # Canonical document shape shared with the SQLite backend
        plan_doc = build_plan_document(
            plan,
            realm,
            scope,
            auto_run=auto_run,
            execution_authority=execution_authority,
            execution_owner=execution_owner,
            preview=preview,
            source_doc_id=source_doc_id,
            source_doc_rev=source_doc_rev,
            notes=notes,
            existing=existing,
        )

        # Include _rev for conflict-safe update (backend-specific field)
        if rev:
            plan_doc["_rev"] = rev

        # Persist to database (build_plan_document returns JSON-safe content)
        try:
            serializable_doc = cast(Document, plan_doc)
            self.server.put_document(
                db=self.db_name,
                doc_id=doc_id,
                document=serializable_doc,
            ).get_result()

            self._logger.info(
                "Saved plan '%s' (realm=%s, status=%s)",
                doc_id,
                realm,
                plan_doc["status"],
            )
            return doc_id

        except ApiException as e:
            self._logger.error("Failed to save plan '%s': %s", doc_id, e)
            if e.status_code == 409:
                raise RevisionConflictError(
                    f"Cannot save plan '{doc_id}': it was "
                    f"{'changed' if rev else 'created'} by another writer after "
                    "it was read",
                    doc_id=doc_id,
                    expected_rev=rev,
                ) from e
            raise

    def fetch_plan(self, doc_id: str) -> dict[str, Any] | None:
        """
        Fetch a plan document by ID.

        Args:
            doc_id: The plan document ID

        Returns:
            dict or None: The plan document, or None if not found
        """
        return self.fetch_document_by_id(doc_id)

    def fetch_plan_as_model(self, doc_id: str) -> Plan | None:
        """
        Fetch a plan document and deserialize to Plan model.

        Args:
            doc_id: The plan document ID

        Returns:
            Plan or None: The deserialized Plan, or None if not found
        """
        doc = self.fetch_document_by_id(doc_id)
        if not doc:
            return None
        return plan_model_from_document(doc, self._logger)

    def update_executed_token(
        self,
        doc_id: str,
        run_token: int,
        *,
        max_retries: int = 3,
    ) -> bool:
        """
        Update executed_run_token after successful plan execution.

        Uses optimistic locking (_rev) to prevent race conditions.
        Retries on conflict (409) up to max_retries times.

        This update does not check the plan generation or record an outcome;
        :meth:`finalize_execution` does both.

        Args:
            doc_id: The plan document ID
            run_token: The run_token value that was just executed
            max_retries: Maximum retry attempts on conflict

        Returns:
            bool: True if update succeeded, False otherwise
        """
        for attempt in range(1, max_retries + 1):
            doc = self.fetch_document_by_id(doc_id)
            if not doc:
                self._logger.error("Cannot update token: plan '%s' not found", doc_id)
                return False

            # Update fields
            doc["executed_run_token"] = run_token
            doc["last_executed_at"] = _utc_now_iso()
            doc["updated_at"] = _utc_now_iso()

            try:
                self.server.put_document(
                    db=self.db_name,
                    doc_id=doc_id,
                    document=cast(Document, doc),
                ).get_result()

                self._logger.debug(
                    "Updated executed_run_token=%d for plan '%s'",
                    run_token,
                    doc_id,
                )
                return True

            except ApiException as e:
                if e.code == 409:
                    self._logger.warning(
                        "Conflict updating plan '%s'; retry %d/%d",
                        doc_id,
                        attempt,
                        max_retries,
                    )
                    continue
                self._logger.error("Failed to update plan '%s': %s", doc_id, e)
                return False

        self._logger.error(
            "Failed to update plan '%s' after %d retries",
            doc_id,
            max_retries,
        )
        return False

    def ensure_plan_generation(self, doc_id: str) -> dict[str, Any] | None:
        """
        Return the current plan document, giving a legacy one a generation.

        See :func:`lib.storage.plan_updates.initialize_plan_generation`.

        Args:
            doc_id: The plan document ID

        Returns:
            dict or None: The current document with its ``plan_generation``,
                or None if the plan does not exist

        Raises:
            RevisionConflictError: If the document kept changing without
                gaining a generation
            PlanStoreError: If CouchDB fails or cannot be reached
        """
        return initialize_plan_generation(
            doc_id,
            fetch=self._fetch_plan_document,
            replace=self._replace_plan_document,
            logger=self._logger,
        )

    def finalize_execution(self, request: ExecutionFinalization) -> FinalizationResult:
        """
        Record a finished execution request in its plan document.

        See :func:`lib.storage.plan_updates.finalize_execution`. The generation,
        token and authority checks run on every read, so a write that CouchDB
        would accept at the current ``_rev`` is still refused on behalf of a
        superseded request.

        Args:
            request: The finished request to record

        Returns:
            FinalizationResult: How this one attempt resolved

        Raises:
            PlanStoreError: If CouchDB fails or cannot be reached
        """
        return finalize_execution(
            request,
            fetch=self._fetch_plan_document,
            replace=self._replace_plan_document,
            logger=self._logger,
        )

    def _plan_store_error(
        self, doc_id: str, action: str, exc: Exception
    ) -> PlanStoreError:
        """
        Wrap a CouchDB failure as a backend-neutral plan-store error.

        Only two kinds of failure are offered for retry: the responses CouchDB
        returns while it is overloaded or restarting, and a request that timed
        out or whose connection failed. Everything else would fail again the
        same way — a rejected credential, a missing database, a malformed
        request, an unusable URL or header, a redirect loop — and so would a
        TLS failure, since certificates and handshakes do not fix themselves;
        ``SSLError`` is a ``ConnectionError`` in Requests, so it is excluded
        before connections are considered transient. A transport failure this
        code does not recognize is left non-retryable too, rather than
        assuming the most convenient explanation for it.

        Args:
            doc_id: The plan document ID
            action: "read" or "write", for the message
            exc: The CouchDB or transport exception to wrap

        Returns:
            PlanStoreError: Carrying the message and retry classification
        """
        status = getattr(exc, "status_code", None)
        if status is not None:
            retryable = status in RETRYABLE_STATUS_CODES
        elif isinstance(exc, SSLError):
            retryable = False
        else:
            retryable = isinstance(exc, Timeout | RequestsConnectionError)
        return PlanStoreError(
            f"CouchDB failed to {action} plan '{doc_id}': {exc}",
            retryable=retryable,
        )

    def _fetch_plan_document(self, doc_id: str) -> dict[str, Any] | None:
        """
        Fetch a plan document for a conditional update.

        Reads the document directly rather than through
        :meth:`fetch_document_by_id`, which reports a malformed response as a
        missing document. A conditional update must not confuse the two: a
        missing plan tells the caller its execution request was superseded and
        can never be recorded, which a proxy returning a non-document body is
        no evidence of.

        Args:
            doc_id: The plan document ID

        Returns:
            dict or None: The document with ``_rev``, or None if CouchDB
                reports it does not exist

        Raises:
            PlanStoreError: If CouchDB fails, cannot be reached, or answers
                with something other than a document
        """
        try:
            document = self.server.get_document(
                db=self.db_name,
                doc_id=doc_id,
            ).get_result()
        except ApiException as exc:
            if exc.status_code == 404:
                self._logger.debug(
                    "Plan '%s' not found in database '%s'", doc_id, self.db_name
                )
                return None
            raise self._plan_store_error(doc_id, "read", exc) from exc
        except RequestException as exc:
            raise self._plan_store_error(doc_id, "read", exc) from exc

        if not isinstance(document, dict):
            raise PlanStoreError(
                f"CouchDB answered with {type(document).__name__} instead of a "
                f"document for plan '{doc_id}'; the plan's existence is unknown"
            )
        return document

    def _replace_plan_document(
        self, doc_id: str, body: dict[str, Any], expected_rev: str
    ) -> str:
        """
        Replace a plan document only if CouchDB still holds expected_rev.

        Args:
            doc_id: The plan document ID
            body: The complete new document body
            expected_rev: The ``_rev`` the body was derived from

        Returns:
            str: The new ``_rev``

        Raises:
            RevisionConflictError: If the document is no longer at expected_rev
            PlanStoreError: If CouchDB fails or cannot be reached; the write
                may or may not have been applied
        """
        document = dict(body)
        document["_id"] = doc_id
        document["_rev"] = expected_rev
        try:
            response = self.server.put_document(
                db=self.db_name,
                doc_id=doc_id,
                document=cast(Document, json_safe(document)),
            ).get_result()
        except ApiException as exc:
            if exc.status_code == 409:
                raise RevisionConflictError(
                    f"Cannot replace plan '{doc_id}' at revision {expected_rev}: "
                    "it has changed since it was read",
                    doc_id=doc_id,
                    expected_rev=expected_rev,
                ) from exc
            raise self._plan_store_error(doc_id, "write", exc) from exc
        except RequestException as exc:
            raise self._plan_store_error(doc_id, "write", exc) from exc

        new_rev = response.get("rev") if isinstance(response, dict) else None
        if not isinstance(new_rev, str):
            raise PlanStoreError(
                f"CouchDB accepted the write of plan '{doc_id}' but returned no "
                "revision"
            )
        return new_rev

    def query_approved_pending(self) -> list[dict[str, Any]]:
        """
        Query all approved plans that are pending execution.

        Reserved for future daemon-startup recovery.
        Returns plans where: status='approved' AND run_token > executed_run_token

        Note: This is a full scan (O(n)). For large databases, consider
        adding a CouchDB view for indexed queries.

        Returns:
            list: Plan documents eligible for execution
        """
        from lib.core_utils.plan_eligibility import is_plan_eligible

        eligible_plans: list[dict[str, Any]] = []

        try:
            # Fetch all documents (with full content)
            response = self.server.post_all_docs(
                db=self.db_name,
                include_docs=True,
            ).get_result()

            result = cast(dict[str, Any], response) if response else {}
            rows = result.get("rows", [])
            for row in rows:
                doc = row.get("doc", {})

                # Skip design documents
                if doc.get("_id", "").startswith("_design/"):
                    continue

                # Check eligibility
                if is_plan_eligible(doc):
                    eligible_plans.append(doc)

            self._logger.info(
                "Found %d eligible plans (of %d total) for recovery",
                len(eligible_plans),
                len(rows),
            )

        except ApiException as e:
            self._logger.error("Failed to query approved pending plans: %s", e)
            # Return empty list on error (caller handles recovery)

        return eligible_plans

    def delete_plan(self, doc_id: str) -> bool:
        """
        Delete a plan document (for testing/cleanup).

        Args:
            doc_id: The plan document ID

        Returns:
            bool: True if deleted, False otherwise
        """
        doc = self.fetch_document_by_id(doc_id)
        if not doc:
            self._logger.warning("Cannot delete: plan '%s' not found", doc_id)
            return False

        rev = doc.get("_rev")
        if not rev:
            self._logger.error("Cannot delete: plan '%s' has no _rev", doc_id)
            return False

        try:
            self.server.delete_document(
                db=self.db_name,
                doc_id=doc_id,
                rev=rev,
            ).get_result()
            self._logger.info("Deleted plan '%s'", doc_id)
            return True

        except ApiException as e:
            self._logger.error("Failed to delete plan '%s': %s", doc_id, e)
            return False

    def plan_exists(self, doc_id: str) -> bool:
        """
        Check if a plan document exists (without fetching full content).

        Used for overwrite detection in CLI mode.

        Args:
            doc_id: The plan document ID

        Returns:
            bool: True if document exists, False otherwise
        """
        return self.fetch_document_by_id(doc_id) is not None

    def get_plan_summary(self, doc_id: str) -> dict[str, Any] | None:
        """
        Fetch minimal plan summary for display.

        Returns key metadata without the full plan content.
        Useful for displaying existing plan info during overwrite warnings.

        Args:
            doc_id: The plan document ID

        Returns:
            dict with keys: status, execution_authority, execution_owner,
                updated_at, realm, run_token, executed_run_token
            or None if not found
        """
        doc = self.fetch_document_by_id(doc_id)
        if not doc:
            return None
        return plan_summary_from_document(doc)
