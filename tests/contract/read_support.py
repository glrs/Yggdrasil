"""Inputs for the reader's tests: corpus documents and named hostile values.

Every plan document and snapshot a reader test reads is a regression-corpus
document (``documents/current/``), which Yggdrasil's own writers built, or such
a document with one field changed (:func:`tests.contract.support.changed`), so
each test shows which field matters. A history the corpus lacks is made by the
same writers, through real attempts (:class:`HistoryTestCase`).

The manifest's sequences put plan documents and snapshots of one plan's
history together: :func:`sequence_pairs` gives each plan document beside the
latest snapshot of the same history, so their identities match.

Hostile values are named, never shown: representing some of them raises (an
integer beyond Python's 4300-digit limit, even inside a container), so a test
passes a case's name to ``subTest``, never the value.
"""

from __future__ import annotations

import json
import logging
import shutil
import unittest
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from tests.contract.support import (
    SPOOL_PLAN_ID,
    SpoolTestCase,
    StepBody,
    stored_snapshot,
)
from tests.execution_support import REALM, SCOPE, make_plan
from yggdrasil.contract.controls import token_pair
from yggdrasil.contract.read import generation_of
from yggdrasil.core.engine import SUCCESS_MARKER
from yggdrasil.flow.attempt import AttemptContext
from yggdrasil.flow.events.attempt_records import (
    ATTEMPT_STARTED_EVENT,
    record_filename,
    step_events_dir,
)
from yggdrasil.flow.model import FAIL_FAST_POLICY, Plan, StepSpec
from yggdrasil.storage.plan_documents import build_plan_document
from yggdrasil.storage.plan_updates import (
    ExecutionFinalization,
    build_finalized_document,
    check_finalization,
)

# The instants a history's writers are given, as a caller may give them.
PLANNED = "2026-11-12T09:00:00Z"
FINALIZED = "2026-11-12T10:00:00Z"
RERUN_REQUESTED = "2026-11-12T11:00:00Z"

# The generation a history's plan is built with.
GENERATION = "5f0c9d2e7a3b4c61a2d48e0f9b1c7d55"

# The current edition of the regression corpus.
CURRENT = Path(__file__).resolve().parent / "documents" / "current"
PLAN_DOCUMENTS = CURRENT / "plan_document"
SNAPSHOTS = CURRENT / "plan_status"
MANIFEST = CURRENT / "manifest.json"

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


def snapshot(name: str) -> dict[str, Any]:
    """Return a fresh copy of a corpus ``plan_status`` snapshot.

    Args:
        name: The file's name without ``.json``, such as ``running``.

    Returns:
        dict[str, Any]: The snapshot, parsed anew on each call.
    """
    return json.loads((SNAPSHOTS / f"{name}.json").read_text("utf-8"))


def snapshot_names() -> list[str]:
    """Return the name of every corpus snapshot, sorted."""
    return sorted(path.stem for path in SNAPSHOTS.glob("*.json"))


def valid_names(kind: str) -> list[str]:
    """Return the name of every corpus document of a kind that must validate.

    Args:
        kind: ``plan_document`` or ``plan_status``.

    Returns:
        list[str]: The names, without ``.json``, sorted.
    """
    return sorted(
        Path(entry["file"]).stem
        for entry in _manifest_entries()
        if entry["kind"] == kind and entry["expectation"] == "valid"
    )


def sequence_pairs() -> list[tuple[str, str]]:
    """Return every plan and snapshot the manifest's sequences put together.

    Each sequence is walked in position order, keeping the latest plan
    document and the latest snapshot; after each step where both exist,
    they are a pair. Both are of one plan's history, so their identities
    match.

    Returns:
        list[tuple[str, str]]: The pairs, as (plan document, snapshot) names,
        in the order the sequences reach them.
    """
    sequences: dict[str, list[dict[str, Any]]] = {}
    for entry in _manifest_entries():
        if "sequence" in entry:
            sequences.setdefault(entry["sequence"], []).append(entry)
    pairs: list[tuple[str, str]] = []
    for name in sorted(sequences):
        latest: dict[str, str] = {}
        for entry in sorted(sequences[name], key=lambda item: item["position"]):
            latest[entry["kind"]] = Path(entry["file"]).stem
            if len(latest) == 2:
                pairs.append((latest["plan_document"], latest["plan_status"]))
    return pairs


def _manifest_entries() -> list[dict[str, Any]]:
    """Return the corpus manifest's entries."""
    entries: list[dict[str, Any]] = json.loads(MANIFEST.read_text("utf-8"))["entries"]
    return entries


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


class HistoryTestCase(SpoolTestCase, ReaderTestCase):
    """One plan's history, written by Yggdrasil's own writers.

    The plan document is built by ``build_plan_document``. Each attempt is
    admitted from a stored document as the coordinator admits one, its
    generation, run token, authority and owner captured from it, and runs
    through the engine; a result that finishes its request is recorded by
    the finalizer's ``build_finalized_document``, after the finalizer's own
    check; and each snapshot is the ops consumer's, stored with its ``_id``.
    The plan is SPOOL_PLAN_ID, in the realm and scope of the execution tests.
    """

    def planned(
        self, *specs: StepSpec, policy: str = FAIL_FAST_POLICY, **options: Any
    ) -> dict[str, Any]:
        """Return a new plan's document, as stored: approved to run on its own.

        Args:
            *specs: The plan's steps.
            policy: Its failure policy.
            **options: The builder's other options.

        Returns:
            dict[str, Any]: The document, at its first revision.
        """
        plan = make_plan(*specs, policy=policy, plan_id=SPOOL_PLAN_ID)
        document = build_plan_document(
            plan,
            REALM,
            dict(SCOPE),
            auto_run=True,
            now=PLANNED,
            plan_generation=GENERATION,
            **options,
        )
        return {**document, "_rev": "1-history"}

    def attempt(
        self, document: Mapping[str, Any], steps: Mapping[str, StepBody] | None = None
    ) -> AttemptContext:
        """Admit an execution request from a stored document and run its attempt.

        Args:
            document: The plan document, as stored.
            steps: What each step does, by step ID; a step not named here
                succeeds.

        Returns:
            AttemptContext: The attempt's context; its report says how it ended.
        """
        plan = Plan.from_dict(document["plan"])
        execution_id, reserved = self.engine.reserve_execution_id(plan)
        context = AttemptContext.for_plan(
            plan,
            execution_id=execution_id,
            execution_id_reserved=reserved,
            plan_generation=generation_of(document),
            run_token=token_pair(document).run,
            execution_authority=document["execution_authority"],
            execution_owner=document["execution_owner"],
        )
        bodies = dict(steps or {})

        def body(ctx: Any) -> Any:
            work = bodies.get(ctx.step_id)
            return work(ctx) if work is not None else None

        return self.finished(plan, body, context=context)

    def finalized(
        self, document: Mapping[str, Any], context: AttemptContext
    ) -> dict[str, Any]:
        """Return the document the finalizer writes for an attempt that finished its request.

        Args:
            document: The stored document the request was admitted from.
            context: The attempt.

        Returns:
            dict[str, Any]: The finalized document, at its next revision.
        """
        request = ExecutionFinalization.from_attempt(context, schema_version=1)
        self.assertIsNone(check_finalization(dict(document), request))
        finalized = build_finalized_document(dict(document), request, now=FINALIZED)
        return {**finalized, "_rev": _next_revision(document)}

    def rerun_requested(self, document: Mapping[str, Any]) -> dict[str, Any]:
        """Return a document with a rerun requested, as the rerun operation requests one.

        Args:
            document: The stored document.

        Returns:
            dict[str, Any]: The document with its run token raised by one.
        """
        run_token = token_pair(document).run
        assert run_token is not None
        return {
            **document,
            "run_token": run_token + 1,
            "run_requested_at": RERUN_REQUESTED,
            "run_requested_by": "bob",
            "updated_at": RERUN_REQUESTED,
            "_rev": _next_revision(document),
        }

    def stored(self) -> dict[str, Any]:
        """Return the plan's snapshot now, as the consumer builds it and a sink stores it."""
        stored = stored_snapshot(self.snapshot())
        assert stored is not None  # the plan's scope is always stored
        return stored

    def run_again(self, *step_ids: str) -> None:
        """Clear steps' earlier successes from the work directory, so each runs again."""
        for step_id in step_ids:
            (self.engine.work_root / SPOOL_PLAN_ID / step_id / SUCCESS_MARKER).unlink()

    def without_start_record(self, context: AttemptContext) -> None:
        """Delete an attempt's start record, as when it was never published."""
        (self.attempt_dir(context) / record_filename(ATTEMPT_STARTED_EVENT)).unlink()

    def without_step_events(self, context: AttemptContext) -> None:
        """Delete every step event of an attempt, as when none was published."""
        for step_id in context.report.step_ids:
            shutil.rmtree(step_events_dir(self.attempt_dir(context), step_id), True)


def _next_revision(document: Mapping[str, Any]) -> str:
    """Return the revision a store gives a document's next write."""
    return f"{int(str(document['_rev']).split('-', 1)[0]) + 1}-history"
