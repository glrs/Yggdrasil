"""Backend-neutral exceptions raised by internal-storage plan stores.

Both plan-store backends raise these same types, so a caller handles a lost
race or a failed backend call identically whichever backend is configured.
Configuration problems are a separate concern and keep using
``InternalStorageConfigurationError`` (``lib/core_utils/errors.py``).
"""

from __future__ import annotations


class RevisionConflictError(Exception):
    """A conditional write targeted a document state that is no longer current.

    Raised when the revision a writer read has moved on before its write, or
    when a create-if-absent write finds that a live document already exists.
    The rejected write changed nothing: not the document body, not its
    revision, and not the plan-change sequence.

    Callers never resend the document that was rejected. They either surface
    the conflict (a plan regeneration must not replay stale planning intent
    over a newer document) or refetch and reapply their intended field-level
    change to the current state.

    Attributes:
        doc_id: ID of the document whose write was rejected.
        expected_rev: The revision the writer expected, or None when it
            expected no live document to exist.
    """

    def __init__(self, message: str, *, doc_id: str, expected_rev: str | None) -> None:
        """Initialize the conflict with the write that was rejected.

        Args:
            message: Human-readable description of the conflict.
            doc_id: ID of the document whose write was rejected.
            expected_rev: The revision the writer expected, or None when it
                expected no live document to exist.
        """
        super().__init__(message)
        self.doc_id = doc_id
        self.expected_rev = expected_rev


class PlanStoreError(Exception):
    """The storage backend failed while reading or writing a plan document.

    Covers failures of the backend itself (an unreachable server, a locked
    or unreadable database file), never a lost race, which is reported as
    :class:`RevisionConflictError` or as a finalization result instead. When
    this is raised by a write, the write may or may not have been applied.
    Operations that raise it recheck the document on every call, so calling
    them again is how that uncertainty is resolved. The backend's original
    exception is chained as ``__cause__``.

    ``retryable`` says whether trying the same operation again could plausibly
    succeed, so that a caller can decide without catching the backend's own
    exception types — which is the whole point of having one storage boundary.
    The adapter that wraps the failure classifies it: a lost connection, a
    timeout, an overloaded server or a busy database file are retryable; a
    rejected credential, a missing database, a malformed response, and
    anything else whose cause is unknown are not, because retrying those
    repeats a failure rather than waiting out a temporary one.

    A non-retryable failure is not a verdict on the execution it was
    finalizing. The caller keeps the result and its execution exclusion either
    way; only automatic retrying is ruled out.

    Attributes:
        retryable: Whether trying the same operation again could succeed.
    """

    def __init__(self, message: str, *, retryable: bool = False) -> None:
        """Initialize the failure with its retry classification.

        Args:
            message: Human-readable description of the failure.
            retryable: Whether trying the same operation again could succeed.
                Defaults to False, so an unclassified failure is never retried
                automatically.
        """
        super().__init__(message)
        self.retryable = retryable
