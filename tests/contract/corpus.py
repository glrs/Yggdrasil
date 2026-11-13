"""The contract's regression corpus: real documents, rebuilt from fixed inputs.

``documents/current/`` holds what this release's writers produce for the
contract's scenarios, each document in its stored form, with a manifest that
declares what each one is and whether it must validate. This module builds
that content:

- plan documents through ``build_plan_document`` and the finalizer's
  ``build_finalized_document``, each result recorded from a real engine
  attempt (``Engine._run_attempt``), admitted the way the coordinator admits
  it and judged by ``check_finalization`` before it is written;
- snapshots through the ops consumer's ``build_plan_snapshot``, over the spool
  the engine published to, with ``_id`` as the sinks compute it.

Some scenarios stand in for writers Yggdrasil does not have: an approval, a
rerun request, a foreign writer or a hand edit. The manifest names every field
they set or remove (``by_hand``). A document changed by hand from another
entry also names that entry (``derived_from``), and differs from it in those
fields only.

The build is deterministic. Every producer reads one frozen clock, set to the
scenario's instant, which is always a whole second; a scenario that needs a
fraction of a second supplies the time to a writer, as a caller may. Run and
execution IDs are numbered per scenario, generations and owners derive from
fixed labels, and storage revisions are fixed. Each scenario has its own
scratch spool and work directory, and no path from them reaches a document.
The build reads no configuration: each step's DataAccess is configured with
no external systems, instead of loading the building machine's ``main.json``.

Regenerate from the repository root with::

    python -m tests.contract.corpus --write

Without ``--write`` it lists the files that differ and writes nothing. It only
ever writes ``documents/current/``: ``documents/released/`` holds frozen
baselines, which nothing regenerates (see ``documents/README.md``).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import datetime, tzinfo
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import patch

import yggdrasil.core.engine
from tests.contract.support import ABSENT, changed, stored_snapshot
from tests.execution_support import REALM, SCOPE, lanes_plan, make_plan, spec
from yggdrasil.contract import PLAN_DOCUMENT
from yggdrasil.contract.controls import (
    execution_definition_problems,
    failure_policy_of,
    token_pair,
)
from yggdrasil.contract.read import generation_of
from yggdrasil.contract.versions import SCHEMA_VERSION_FIELD, SUPPORTED, version_of
from yggdrasil.core.engine import Engine
from yggdrasil.flow.attempt import AttemptContext
from yggdrasil.flow.data_access import DataAccess
from yggdrasil.flow.errors import PermanentStepError
from yggdrasil.flow.events.attempt_records import (
    ATTEMPT_REPORT_EVENT,
    ATTEMPT_STARTED_EVENT,
    attempt_dir,
    record_filename,
)
from yggdrasil.flow.events.emitter import FileSpoolEmitter
from yggdrasil.flow.model import (
    CONTINUE_INDEPENDENT_POLICY,
    Artifact,
    Plan,
    StepResult,
)
from yggdrasil.flow.outcomes import TerminationReason
from yggdrasil.flow.step import StepContext, step
from yggdrasil.flow.utils import ygg_time
from yggdrasil.ops.consumer import build_plan_snapshot
from yggdrasil.storage.plan_documents import build_plan_document
from yggdrasil.storage.plan_eligibility import is_plan_eligible
from yggdrasil.storage.plan_updates import (
    ExecutionFinalization,
    build_finalized_document,
    check_finalization,
)

# The corpus, beside this module.
DOCUMENTS = Path(__file__).resolve().parent / "documents"

# The edition this release's writers produce, and the frozen ones beside it.
CURRENT = "current"
RELEASED = "released"
MANIFEST = "manifest.json"

# What a manifest entry expects of its document.
VALID = "valid"
INVALID = "invalid"

# The sequences: one plan's history, read in order.
REQUEST_LIFECYCLE = "request_lifecycle"
UNVERSIONED_REGENERATION = "unversioned_regeneration"
FRACTIONAL_REGENERATION = "fractional_regeneration"

# When scenarios happen. An attempt, its finalization and the snapshots taken
# of it share one instant: the frozen clock does not move during an attempt.
PLANNED = "2026-10-22T09:00:00Z"
APPROVED = "2026-10-22T09:30:00Z"
EXECUTED = "2026-10-22T10:00:00Z"
RERUN_REQUESTED = "2026-10-22T11:00:00Z"
REGENERATED = "2026-10-23T08:00:00Z"

# When steps published the legacy histories, before attempts were correlated.
LEGACY_PUBLISHED = "2025-01-01T00:00:00Z"

# Times a caller supplied to a writer, with a fraction of a second. The clocks
# write whole seconds, so only a supplied time has a fraction.
FRACTIONAL_PLANNED = "2026-10-22T09:00:00.25Z"
FRACTIONAL_FINALIZED = "2026-10-22T10:00:00.5Z"
FRACTIONAL_REGENERATED = "2026-10-23T08:00:00.123456Z"

# The ticket a foreign writer records, and the actors that approve and rerun.
TICKET = "OPS-1234"
APPROVER = "alice"
RERUN_REQUESTER = "bob"

# A step's work in an attempt: what the scripted step does for one step ID.
StepBody = Callable[[StepContext], StepResult | None]


class CorpusError(Exception):
    """A scenario did not come out the way the corpus declares it."""


@dataclass(frozen=True)
class Entry:
    """One file of the corpus, as its manifest entry declares it.

    Attributes:
        file: Its path in the edition, such as ``plan_document/draft.json``;
            the directory names its kind.
        scenario: What it shows.
        provenance: Which writers and producers made it, and how.
        problems: For a document ``validate`` must reject: every problem it
            reports, as (path, code). Empty for a valid document.
        sequence: The sequence it belongs to, if any.
        position: Its place in that sequence, counted from 1.
        derived_from: For a document the corpus changed by hand from another
            entry's: that entry's file.
        by_hand: The top-level fields the corpus set or removed itself, in
            standing for a writer Yggdrasil does not have.
    """

    file: str
    scenario: str
    provenance: str
    problems: tuple[tuple[str, str], ...] = ()
    sequence: str | None = None
    position: int | None = None
    derived_from: str | None = None
    by_hand: tuple[str, ...] = ()

    @property
    def kind(self) -> str:
        """The document kind, which the file's directory names."""
        return self.file.split("/", 1)[0]

    def to_json(self) -> dict[str, Any]:
        """Return the manifest entry, leaving out what does not apply.

        Returns:
            dict[str, Any]: The entry as the manifest stores it.
        """
        entry: dict[str, Any] = {
            "file": self.file,
            "kind": self.kind,
            "scenario": self.scenario,
            "provenance": self.provenance,
            "expectation": INVALID if self.problems else VALID,
        }
        if self.problems:
            entry["problems"] = [
                {"path": path, "code": code} for path, code in self.problems
            ]
        if self.sequence is not None:
            entry["sequence"] = self.sequence
            entry["position"] = self.position
        if self.derived_from is not None:
            entry["derived_from"] = self.derived_from
        if self.by_hand:
            entry["by_hand"] = sorted(self.by_hand)
        return entry


@dataclass(frozen=True)
class Differences:
    """How a directory's files differ from the files expected there.

    Attributes:
        changed: Files present in both whose bytes differ.
        missing: Files expected but absent.
        unexpected: Files present but not expected.
    """

    changed: tuple[str, ...] = ()
    missing: tuple[str, ...] = ()
    unexpected: tuple[str, ...] = ()

    def __bool__(self) -> bool:
        """Whether any file differs."""
        return bool(self.changed or self.missing or self.unexpected)

    def describe(self) -> str:
        """List the differing files, one per line, grouped by how they differ."""
        lines = [
            f"  {label}: {name}"
            for label, names in (
                ("changed", self.changed),
                ("missing", self.missing),
                ("unexpected", self.unexpected),
            )
            for name in names
        ]
        return "\n".join(lines)


def serialize(document: object) -> bytes:
    """Return a document as the corpus stores it, the same bytes every time.

    Args:
        document: A JSON value.

    Returns:
        bytes: Its JSON, indented by two, keys sorted, UTF-8, with a final
        newline.
    """
    text = json.dumps(document, indent=2, sort_keys=True, ensure_ascii=False)
    return (text + "\n").encode("utf-8")


def edition_files(edition: Path) -> dict[str, bytes]:
    """Return every file of an edition, by its path there.

    Args:
        edition: An edition's directory, such as ``documents/current``; it may
            not exist.

    Returns:
        dict[str, bytes]: Each file's bytes, by its relative POSIX path.
    """
    if not edition.is_dir():
        return {}
    return {
        path.relative_to(edition).as_posix(): path.read_bytes()
        for path in sorted(edition.rglob("*"))
        if path.is_file()
    }


def differences(expected: Mapping[str, bytes], edition: Path) -> Differences:
    """Compare an edition's files with the files expected in it.

    Args:
        expected: The files, by their path in the edition.
        edition: The edition's directory.

    Returns:
        Differences: What differs; nothing when the two agree byte for byte.
    """
    found = edition_files(edition)
    return Differences(
        changed=tuple(
            sorted(
                name
                for name in expected.keys() & found.keys()
                if expected[name] != found[name]
            )
        ),
        missing=tuple(sorted(expected.keys() - found.keys())),
        unexpected=tuple(sorted(found.keys() - expected.keys())),
    )


def build(scratch: Path | None = None) -> dict[str, bytes]:
    """Build the current edition: every document and its manifest.

    Args:
        scratch: The directory under which the scenarios' spools and work
            directories go, in a fresh temporary directory removed afterwards;
            the system's temporary directory when None. Nothing in it reaches
            a document.

    Returns:
        dict[str, bytes]: Every file of ``documents/current/``, by its path
        there.

    Raises:
        CorpusError: If a scenario does not come out the way it is declared,
            such as an attempt that ends differently or a result the
            finalizer would not record.
    """
    corpus = _Corpus()
    clock = _Clock()
    with TemporaryDirectory(dir=scratch) as directory, clock.installed():
        workspaces = _Workspaces(Path(directory), clock)
        _request_lifecycle(corpus, workspaces)
        _unversioned_regeneration(corpus)
        _finalized_plans(corpus, workspaces)
        _malformed_plan_documents(corpus)
        _snapshots(corpus, workspaces)
        _fractional_timestamps(corpus, workspaces)
    return corpus.files()


def write(documents: Path) -> Differences:
    """Rewrite the current edition with what this release's writers produce.

    Only ``current/`` is touched: files the build no longer produces are
    removed, and ``released/`` is never read or written.

    Args:
        documents: The corpus root, holding ``current/``; normally DOCUMENTS.

    Returns:
        Differences: How ``current/`` differed before it was rewritten.
    """
    edition = documents / CURRENT
    expected = build()
    found = differences(expected, edition)
    for name in found.unexpected:
        (edition / name).unlink()
    for name in (*found.changed, *found.missing):
        path = edition / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(expected[name])
    for directory in sorted(edition.rglob("*"), reverse=True):
        if directory.is_dir() and not any(directory.iterdir()):
            directory.rmdir()
    return found


def main(argv: Sequence[str] | None = None) -> int:
    """Compare the current edition with a rebuild, or rewrite it.

    Args:
        argv: The command-line arguments; ``sys.argv[1:]`` when None.

    Returns:
        int: 0 when the edition matches the rebuild or was rewritten; 1 when
        it differs and was left as it is.
    """
    edition = DOCUMENTS / CURRENT
    parser = argparse.ArgumentParser(
        prog="python -m tests.contract.corpus",
        description=(
            f"Rebuild the contract's regression corpus and compare it with {edition}. "
            "Review every difference as a contract change."
        ),
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help=f"rewrite {edition}; nothing else is ever written",
    )
    arguments = parser.parse_args(argv)
    if arguments.write:
        found = write(DOCUMENTS)
        print(f"Rewrote:\n{found.describe()}" if found else "Already up to date.")
        return 0
    found = differences(build(), edition)
    if not found:
        print("The current edition matches what this release's writers produce.")
        return 0
    print("The current edition differs from what this release's writers produce:")
    print(found.describe())
    print("Run with --write to regenerate it, and review the diff.")
    return 1


# ----- building -----


class _Corpus:
    """The entries built so far, each with its document."""

    def __init__(self) -> None:
        """Start with no entries."""
        self._entries: dict[str, tuple[Entry, dict[str, Any]]] = {}
        self._positions: dict[str, int] = {}

    def add(
        self,
        file: str,
        document: dict[str, Any],
        *,
        scenario: str,
        provenance: str,
        problems: Sequence[tuple[str, str]] = (),
        sequence: str | None = None,
        derived_from: str | None = None,
        by_hand: Sequence[str] = (),
    ) -> dict[str, Any]:
        """Add one document, at the next position of its sequence if it has one.

        Args:
            file: Its path in the edition.
            document: The document, in its stored form.
            scenario: What it shows.
            provenance: Which writers and producers made it.
            problems: Every (path, code) validate reports for it; none for a
                valid document.
            sequence: The sequence it belongs to, if any.
            derived_from: The entry it was changed by hand from, if any.
            by_hand: The top-level fields the corpus set or removed itself.

        Returns:
            dict[str, Any]: The document, for the scenarios built from it.

        Raises:
            CorpusError: If the file is already in the corpus, or derived_from
                is not.
        """
        if file in self._entries:
            raise CorpusError(f"{file} is already in the corpus")
        if derived_from is not None and derived_from not in self._entries:
            raise CorpusError(f"{file} is derived from {derived_from}, not built yet")
        position = None
        if sequence is not None:
            position = self._positions[sequence] = self._positions.get(sequence, 0) + 1
        entry = Entry(
            file=file,
            scenario=scenario,
            provenance=provenance,
            problems=tuple(problems),
            sequence=sequence,
            position=position,
            derived_from=derived_from,
            by_hand=tuple(by_hand),
        )
        self._entries[file] = (entry, document)
        return document

    def document(self, file: str) -> dict[str, Any]:
        """Return the document of an entry already built."""
        return self._entries[file][1]

    def files(self) -> dict[str, bytes]:
        """Return every document and the manifest, serialized, by path."""
        files = {
            file: serialize(document) for file, (_, document) in self._entries.items()
        }
        manifest = {"entries": [entry.to_json() for entry, _ in self._entries.values()]}
        files[MANIFEST] = serialize(manifest)
        return files


class _Clock:
    """The one clock every producer reads while the corpus builds.

    Every timestamp Yggdrasil writes comes from ``ygg_time``, and every
    ``ygg_time`` function reads that module's ``datetime``. Replacing it there
    holds them all, including the report's ``started_at``, whose
    ``default_factory`` keeps a reference to ``utcnow_iso`` itself. A read
    outside :meth:`at` is a defect of the corpus, and raises.
    """

    def __init__(self) -> None:
        """Start with no instant set."""
        self._instant: datetime | None = None

    @contextmanager
    def installed(self) -> Iterator[None]:
        """Make ``ygg_time`` read this clock within the block.

        Yields:
            None: For the build.
        """
        clock = self

        class Frozen(datetime):
            """``datetime`` whose ``now`` is the corpus clock's instant."""

            @classmethod
            def now(cls, tz: tzinfo | None = None) -> Frozen:
                """Return the instant the clock is held at."""
                return clock.now(tz)  # type: ignore[return-value]

        with patch.object(ygg_time, "datetime", Frozen):
            yield

    @contextmanager
    def at(self, instant: str) -> Iterator[None]:
        """Hold the clock at an instant within the block.

        Args:
            instant: A canonical timestamp, such as ``2026-10-22T10:00:00Z``.

        Yields:
            None: For the block.
        """
        previous = self._instant
        self._instant = datetime.fromisoformat(instant)
        try:
            yield
        finally:
            self._instant = previous

    def now(self, tz: tzinfo | None) -> datetime:
        """Return the instant the clock is held at, in tz.

        Raises:
            CorpusError: If no instant is set: something read the clock that
                the corpus did not expect to.
        """
        if self._instant is None:
            raise CorpusError("the clock was read outside a scenario's instant")
        return self._instant.astimezone(tz) if tz is not None else self._instant


@dataclass(frozen=True)
class _Attempt:
    """An attempt the corpus ran, and the request it was admitted with.

    Attributes:
        document: The plan document it was admitted from.
        family: The document's family, captured at admission.
        context: The attempt's context; its report says how it ended.
    """

    document: dict[str, Any]
    family: int
    context: AttemptContext


class _Workspaces:
    """Hands each scenario a fresh workspace under one scratch directory."""

    def __init__(self, scratch: Path, clock: _Clock) -> None:
        """Make workspaces under scratch, all reading clock."""
        self._scratch = scratch
        self._clock = clock

    def open(self, name: str) -> _Workspace:
        """Return a new workspace named for its scenario."""
        return _Workspace(self._scratch / name, self._clock)


class _Workspace:
    """One scenario's engine, publishing to a spool of its own.

    Every step of every plan resolves to one scripted ``@step`` function,
    whatever its ``fn_ref`` names: what a step does is the scenario's choice.
    Run IDs and execution IDs are numbered within the workspace, and each
    step's DataAccess knows no external systems.

    Attributes:
        root: The workspace's directory.
        spool: The spool the engine publishes to.
        clock: The clock every producer reads.
        engine: The engine.
    """

    def __init__(self, root: Path, clock: _Clock) -> None:
        """Make an engine publishing to a fresh spool under root."""
        self.root = root
        self.spool = root / "spool"
        self.clock = clock
        self.engine = Engine(
            work_root=root / "work", emitter=FileSpoolEmitter(self.spool)
        )
        self._runs = 0
        self._attempts = 0

    def attempt(
        self,
        document: dict[str, Any],
        ending: TerminationReason,
        steps: Mapping[str, StepBody] | None = None,
    ) -> _Attempt:
        """Admit an execution request from document and run its attempt.

        The request is captured as the coordinator captures it, after the
        same checks; the attempt then runs through the engine at EXECUTED.
        A cancelled attempt is an interrupted one: a step body raises
        KeyboardInterrupt, as an operator's interrupt does.

        Args:
            document: The plan document, as stored.
            ending: How the scenario's attempt must end.
            steps: What each step does, by step ID; a step not named here
                succeeds.

        Returns:
            _Attempt: The attempt.

        Raises:
            CorpusError: If the coordinator would not admit document, or the
                attempt ends otherwise.
        """
        plan, family = _admitted(document)
        self._attempts += 1
        with self.clock.at(EXECUTED):
            context = AttemptContext.for_plan(
                plan,
                execution_id=f"{ygg_time.utcnow_compact()}_{self._attempts:04x}",
                plan_generation=generation_of(document),
                run_token=token_pair(document).run,
                execution_authority=document["execution_authority"],
                execution_owner=document["execution_owner"],
            )
            with self._scripted(steps or {}):
                try:
                    self.engine._run_attempt(plan, context=context)
                except KeyboardInterrupt:
                    if ending is not TerminationReason.CANCELLED:
                        raise
                except Exception:
                    pass  # how the attempt ended is in its report
        if context.report.termination_reason is not ending:
            raise CorpusError(
                f"the attempt at {plan.plan_id} ended "
                f"{context.report.termination_reason}, not {ending}"
            )
        return _Attempt(document=document, family=family, context=context)

    def finalized(self, attempt: _Attempt, *, now: str = EXECUTED) -> dict[str, Any]:
        """Return the document the finalizer writes for a finished attempt.

        Args:
            attempt: An attempt that finished its request.
            now: When the finalizer records it; EXECUTED, the attempt's own
                instant, unless a scenario supplies another time.

        Returns:
            dict[str, Any]: The finalized document, at its next revision.

        Raises:
            CorpusError: If the finalizer would not record the result.
        """
        request = ExecutionFinalization.from_attempt(
            attempt.context, schema_version=attempt.family
        )
        refusal = check_finalization(attempt.document, request)
        if refusal is not None:
            raise CorpusError(f"the finalizer would not record: {refusal.message}")
        document = build_finalized_document(attempt.document, request, now=now)
        return _stored(document, after=attempt.document)

    def snapshot(
        self, plan_id: str, *, after: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        """Return the plan's snapshot as the consumer builds it and a sink stores it.

        Args:
            plan_id: The plan.
            after: The snapshot stored before this one, if any.

        Returns:
            dict[str, Any]: The snapshot, with its ``_id`` and next revision.
        """
        with self.clock.at(EXECUTED):
            snapshot = build_plan_snapshot(self.spool / REALM / plan_id, REALM, plan_id)
        stored = stored_snapshot(snapshot)
        if stored is None:
            raise CorpusError(f"the snapshot of {plan_id} would not be stored")
        return _stored(stored, after=after)

    def remove_record(self, attempt: _Attempt, event_type: str) -> None:
        """Delete one of an attempt's plan-level records from the spool.

        Args:
            attempt: The attempt.
            event_type: ``plan.attempt_started`` or ``plan.attempt_report``.
        """
        directory = attempt_dir(
            self.spool, REALM, attempt.context.plan_id, attempt.context.execution_id
        )
        (directory / record_filename(event_type)).unlink()

    @contextmanager
    def _scripted(self, steps: Mapping[str, StepBody]) -> Iterator[None]:
        """Resolve every step to the scripted function, and number run IDs.

        Each step's DataAccess is the real one, configured with no external
        systems, so an attempt never loads the building machine's configuration.
        """

        @step
        def scripted(ctx: StepContext, **params: Any) -> StepResult | None:
            body = steps.get(ctx.step_id)
            return body(ctx) if body is not None else None

        with ExitStack() as patches:
            patches.enter_context(
                patch.object(
                    yggdrasil.core.engine, "resolve_callable", return_value=scripted
                )
            )
            patches.enter_context(
                patch.object(yggdrasil.core.engine, "_new_run_id", self._new_run_id)
            )
            patches.enter_context(
                patch.object(
                    yggdrasil.core.engine,
                    "DataAccess",
                    _data_access_without_external_systems,
                )
            )
            yield

    def _new_run_id(self) -> str:
        """Return the next run ID, shaped as the engine shapes them."""
        self._runs += 1
        return f"run_{ygg_time.utcnow_compact()}_{self._runs:06x}"


def _data_access_without_external_systems(realm_id: str, **kwargs: Any) -> DataAccess:
    """Build a step's DataAccess as the engine does, with no external systems.

    The engine passes no configuration, so DataAccess would load the one in
    the building machine's ``main.json``. This is the real constructor, called
    with the engine's arguments, trace context included, and with an empty
    configuration instead.

    Args:
        realm_id: The step's realm.
        **kwargs: The engine's other arguments, such as ``trace_context``.

    Returns:
        DataAccess: The step's DataAccess, which knows no connections.
    """
    return DataAccess(realm_id, cfg={"endpoints": {}, "connections": {}}, **kwargs)


def _admitted(document: Mapping[str, Any]) -> tuple[Plan, int]:
    """Check a document the way the coordinator does before it admits a request.

    Args:
        document: The plan document, as stored.

    Returns:
        tuple[Plan, int]: The plan to run, and the family to capture.

    Raises:
        CorpusError: If the coordinator would not admit it.
    """
    plan_id = document.get("_id")
    version = version_of(PLAN_DOCUMENT, document)
    problems = [
        *execution_definition_problems(document),
        *failure_policy_of(document).problems,
    ]
    if version.state != SUPPORTED or version.family is None:
        problems.append(f"schema_version is {version.state}")
    if not is_plan_eligible(dict(document)):
        problems.append("it is not eligible")
    if generation_of(dict(document)) is None:
        problems.append("it has no generation")
    if problems:
        raise CorpusError(f"the coordinator would not admit {plan_id}: {problems}")
    plan = Plan.from_dict(document["plan"])
    if plan.plan_id != plan_id:
        raise CorpusError(f"{plan_id} embeds plan {plan.plan_id!r}")
    assert version.family is not None  # checked above
    return plan, version.family


def _stored(
    document: Mapping[str, Any], *, after: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Return a document as a store holds it once written: with its next ``_rev``.

    Revisions are opaque to the contract. The corpus spells them
    ``<n>-corpus``, counting the writes of one document.

    Args:
        document: The document a writer produced.
        after: The stored document it replaces, if any.

    Returns:
        dict[str, Any]: The document with its ``_rev``.
    """
    previous = int(str(after["_rev"]).split("-", 1)[0]) if after is not None else 0
    return {**document, "_rev": f"{previous + 1}-corpus"}


def _fixed_hex(label: str, length: int = 32) -> str:
    """Return hexadecimal digits that stand in for a random value, fixed by label."""
    return hashlib.sha256(label.encode("utf-8")).hexdigest()[:length]


def _generation(plan_id: str, name: str) -> str:
    """Return a fixed plan generation ID, as opaque as a real one."""
    return _fixed_hex(f"{plan_id} generation {name}")


def _planned(plan: Plan, *, now: str = PLANNED, **kwargs: Any) -> dict[str, Any]:
    """Return the builder's first document of plan, as stored.

    Args:
        plan: The plan.
        now: When it is planned; PLANNED unless a scenario supplies another
            time.
        **kwargs: The builder's other options, such as ``auto_run``.

    Returns:
        dict[str, Any]: The document at its first revision.
    """
    document = build_plan_document(
        plan,
        REALM,
        dict(SCOPE),
        now=now,
        plan_generation=_generation(plan.plan_id, "A"),
        **kwargs,
    )
    return _stored(document)


def _fails(message: str, code: str, advice: str) -> StepBody:
    """Return a step body that fails the way a realm reports a bad input."""

    def body(ctx: StepContext) -> None:
        raise PermanentStepError(message, code=code, advice=advice)

    return body


def _interrupted(ctx: StepContext) -> None:
    """Interrupt the attempt while this step runs, as an operator's interrupt does."""
    raise KeyboardInterrupt


# Lane 1's demultiplexing, failing as a realm reports a bad sample sheet.
_duplicate_lane = _fails(
    "lane 1 is listed twice in the sample sheet",
    code="SAMPLESHEET_DUPLICATE_LANE",
    advice="Correct the sample sheet, then regenerate the plan.",
)


# ----- scenarios -----


def _lifecycle_plan(lanes: list[int]) -> Plan:
    """The lifecycle's plan: demultiplex the given lanes, then check the reads."""
    return make_plan(
        spec("demux", lanes=lanes),
        spec("qc", "demux", min_reads=1000),
        plan_id="pln_lifecycle",
    )


def _request_lifecycle(corpus: _Corpus, workspaces: _Workspaces) -> None:
    """One plan's history: planned, approved, run, rerun, then regenerated.

    The rerun request stays pending: the plan is regenerated before it runs,
    as when corrected inputs arrive, and the new generation starts over.
    """
    workspace = workspaces.open("request_lifecycle")
    plan_id = "pln_lifecycle"
    draft = corpus.add(
        "plan_document/draft.json",
        _planned(_lifecycle_plan([1, 2])),
        scenario="draft, never run",
        provenance="build_plan_document",
        sequence=REQUEST_LIFECYCLE,
    )

    # No Yggdrasil writer approves a plan in this release; the approval is
    # made as the approval operation makes it. approvals_required is
    # informational and written by no Yggdrasil writer either.
    approval = {
        "status": "approved",
        "approved_at": APPROVED,
        "approved_by": APPROVER,
        "approvals_required": ["lab", "bioinformatics"],
        "updated_at": APPROVED,
    }
    approved = corpus.add(
        "plan_document/approved.json",
        _stored({**draft, **approval}, after=draft),
        scenario="approved by an actor",
        provenance=(
            "the draft, approved by hand as the approval operation approves; no "
            "Yggdrasil writer approves in this release"
        ),
        sequence=REQUEST_LIFECYCLE,
        derived_from="plan_document/draft.json",
        by_hand=tuple(approval),
    )

    running: list[dict[str, Any]] = []

    def demux(ctx: StepContext) -> StepResult:
        ctx.progress(50)
        running.append(workspace.snapshot(plan_id))
        reads = Artifact(
            key="reads",
            path="/data/P1/demux/reads",
            digest=f"sha256:{_fixed_hex('reads', 64)}",
        )
        return StepResult(artifacts=[reads], metrics={"reads": 1200})

    attempt = workspace.attempt(approved, TerminationReason.COMPLETED, {"demux": demux})
    corpus.add(
        "plan_status/running.json",
        running[0],
        scenario="attempt running",
        provenance=(
            "the ops consumer's build_plan_snapshot, read while the attempt's "
            "first step ran"
        ),
        sequence=REQUEST_LIFECYCLE,
    )
    finalized = corpus.add(
        "plan_document/approved_finalized.json",
        workspace.finalized(attempt),
        scenario="approved, finalized success",
        provenance=(
            "build_finalized_document, recording the engine's attempt; the "
            "approval stamps are kept as read"
        ),
        sequence=REQUEST_LIFECYCLE,
    )
    corpus.add(
        "plan_status/finished_success.json",
        workspace.snapshot(plan_id, after=running[0]),
        scenario="finished success",
        provenance="the ops consumer's build_plan_snapshot, after the attempt",
        sequence=REQUEST_LIFECYCLE,
    )

    # No Yggdrasil writer requests a rerun in this release; the request is
    # made as the rerun operation makes it: the token read, plus one.
    run_token = token_pair(finalized).run
    assert run_token is not None  # the finalizer wrote a valid pair
    request = {
        "run_token": run_token + 1,
        "run_requested_at": RERUN_REQUESTED,
        "run_requested_by": RERUN_REQUESTER,
        "updated_at": RERUN_REQUESTED,
    }
    rerun_requested = corpus.add(
        "plan_document/rerun_requested.json",
        _stored({**finalized, **request}, after=finalized),
        scenario="rerun requested after a finished request",
        provenance=(
            "the finalized document, with a rerun requested by hand as the rerun "
            "operation requests one; no Yggdrasil writer requests reruns in this "
            "release"
        ),
        sequence=REQUEST_LIFECYCLE,
        derived_from="plan_document/approved_finalized.json",
        by_hand=tuple(request),
    )

    regenerated = build_plan_document(
        _lifecycle_plan([1, 2, 3]),
        REALM,
        dict(SCOPE),
        existing=rerun_requested,
        now=REGENERATED,
        plan_generation=_generation(plan_id, "B"),
    )
    corpus.add(
        "plan_document/regenerated.json",
        _stored(regenerated, after=rerun_requested),
        scenario="regenerated after a finished request",
        provenance=(
            "build_plan_document over the stored document, with lane 3 added to "
            "the plan"
        ),
        sequence=REQUEST_LIFECYCLE,
    )


def _legacy_timestamp(timestamp: str) -> str:
    """Return a canonical timestamp as releases before the contract spelled it."""
    return timestamp.removesuffix("Z") + "+00:00"


def _unversioned_regeneration(corpus: _Corpus) -> None:
    """A plan stored before the contract, then replaced by planning."""
    approved_file = "plan_document/approved.json"
    approved = corpus.document(approved_file)
    timestamps = ("created_at", "updated_at", "approved_at")
    legacy = {
        key: value for key, value in approved.items() if key != SCHEMA_VERSION_FIELD
    }
    legacy.update({field: _legacy_timestamp(approved[field]) for field in timestamps})
    # A tool outside the contract added a field without the x_ prefix.
    legacy["ticket"] = TICKET
    corpus.add(
        "plan_document/pre_contract.json",
        legacy,
        scenario="pre-contract shape (no marker, +00:00)",
        provenance=(
            "the approved document as a release before the contract stored it, "
            "made by hand: no schema_version, +00:00 timestamps, and a field a "
            "tool outside the contract added"
        ),
        problems=[(SCHEMA_VERSION_FIELD, "unversioned")],
        sequence=UNVERSIONED_REGENERATION,
        derived_from=approved_file,
        by_hand=(SCHEMA_VERSION_FIELD, *timestamps, "ticket"),
    )

    regenerated = build_plan_document(
        _lifecycle_plan([1, 2]),
        REALM,
        dict(SCOPE),
        existing=legacy,
        now=REGENERATED,
        plan_generation=_generation("pln_lifecycle", "regenerated over unversioned"),
    )
    corpus.add(
        "plan_document/regenerated_over_unversioned.json",
        _stored(regenerated, after=legacy),
        scenario="regenerated over an unversioned slot",
        provenance="build_plan_document over the stored pre-contract document",
        sequence=UNVERSIONED_REGENERATION,
    )


def _finalized_plans(corpus: _Corpus, workspaces: _Workspaces) -> None:
    """Plan documents recording finished requests of every kind."""
    workspace = workspaces.open("auto_run")
    planned = _planned(
        make_plan(spec("demux"), spec("qc", "demux"), plan_id="pln_auto_run"),
        auto_run=True,
    )
    attempt = workspace.attempt(planned, TerminationReason.COMPLETED)
    corpus.add(
        "plan_document/auto_run_finalized.json",
        workspace.finalized(attempt),
        scenario="auto-run, finalized success",
        provenance=(
            "build_plan_document with auto_run, then build_finalized_document "
            "recording the engine's attempt"
        ),
    )

    workspace = workspaces.open("continue_independent_failures")
    planned = _planned(
        lanes_plan(CONTINUE_INDEPENDENT_POLICY, plan_id="pln_lanes"), auto_run=True
    )
    attempt = workspace.attempt(
        planned, TerminationReason.COMPLETED, {"lane1_demux": _duplicate_lane}
    )
    corpus.add(
        "plan_document/continue_independent_failures.json",
        workspace.finalized(attempt),
        scenario="continue_independent, finalized with failures",
        provenance=(
            "build_finalized_document, recording the engine's attempt, which "
            "drained with lane 1 failed and its upload blocked"
        ),
    )
    corpus.add(
        "plan_status/finished_with_failures.json",
        workspace.snapshot("pln_lanes"),
        scenario="finished with failures and blocked steps",
        provenance="the ops consumer's build_plan_snapshot, after the same attempt",
    )

    workspace = workspaces.open("dependency_cycle")
    cycle = make_plan(
        spec("demux", "qc"),
        spec("qc", "demux"),
        policy=CONTINUE_INDEPENDENT_POLICY,
        plan_id="pln_cycle",
    )
    attempt = workspace.attempt(
        _planned(cycle, auto_run=True), TerminationReason.PREFLIGHT_REJECTED
    )
    corpus.add(
        "plan_document/continue_independent_preflight_rejected.json",
        workspace.finalized(attempt),
        scenario=(
            "continue_independent, finalized preflight rejection (dependency cycle)"
        ),
        provenance=(
            "build_finalized_document, recording the engine's preflight rejection"
        ),
    )

    # A realm planned a step whose ID is not a string. The builder stores the
    # plan as given, the coordinator admits it (the field is present), and
    # preflight rejects it: the document does not conform, its report does.
    workspace = workspaces.open("non_string_step_id")
    malformed = make_plan(
        spec("demux"),
        spec("qc"),
        policy=CONTINUE_INDEPENDENT_POLICY,
        plan_id="pln_step_id",
    )
    malformed.steps[0].step_id = 3  # type: ignore[assignment]
    attempt = workspace.attempt(
        _planned(malformed, auto_run=True), TerminationReason.PREFLIGHT_REJECTED
    )
    corpus.add(
        "plan_document/non_string_step_id.json",
        workspace.finalized(attempt),
        scenario="finalized preflight rejection of a plan with a non-string step ID",
        provenance=(
            "build_plan_document, storing the realm's plan as given, then "
            "build_finalized_document recording the engine's preflight rejection"
        ),
        problems=[("plan.steps[0].step_id", "wrong_type")],
    )

    workspace = workspaces.open("run_once")
    owner = f"run_once:{uuid.UUID(_fixed_hex('pln_run_once owner'))}"
    planned = _planned(
        make_plan(spec("demux"), plan_id="pln_run_once"),
        auto_run=True,
        execution_authority="run_once",
        execution_owner=owner,
    )
    attempt = workspace.attempt(planned, TerminationReason.COMPLETED)
    corpus.add(
        "plan_document/run_once_finalized.json",
        workspace.finalized(attempt),
        scenario="run-once plan, finalized",
        provenance=(
            "build_plan_document with the run-once authority and owner, then "
            "build_finalized_document recording the engine's attempt"
        ),
    )

    # A foreign writer recorded a ticket, under the prefix the contract never
    # defines, before the plan ran; the finalizer keeps it as read.
    workspace = workspaces.open("extension")
    planned = _planned(make_plan(spec("demux"), plan_id="pln_extension"), auto_run=True)
    ticketed = _stored({**planned, "x_ops_ticket": TICKET}, after=planned)
    attempt = workspace.attempt(ticketed, TerminationReason.COMPLETED)
    extended = corpus.add(
        "plan_document/finalized_with_extension.json",
        workspace.finalized(attempt),
        scenario="finalized while carrying x_ops_ticket",
        provenance=(
            "build_plan_document, a field a foreign writer added by hand, then "
            "build_finalized_document recording the engine's attempt"
        ),
        by_hand=("x_ops_ticket",),
    )
    unprefixed = {
        key: value for key, value in extended.items() if key != "x_ops_ticket"
    }
    unprefixed["ticket"] = TICKET
    corpus.add(
        "plan_document/unprefixed_field.json",
        unprefixed,
        scenario="unprefixed ticket",
        provenance=(
            "the finalized document, with the foreign field's x_ prefix removed by "
            "hand"
        ),
        problems=[("ticket", "unexpected_field")],
        derived_from="plan_document/finalized_with_extension.json",
        by_hand=("ticket", "x_ops_ticket"),
    )


def _malformed_plan_documents(corpus: _Corpus) -> None:
    """The draft, changed by hand in one thing each."""
    draft_file = "plan_document/draft.json"
    draft = corpus.document(draft_file)
    for file, scenario, fields, problem in (
        (
            "plan_document/schema_version_string.json",
            'schema_version: "1"',
            {SCHEMA_VERSION_FIELD: "1"},
            (SCHEMA_VERSION_FIELD, "invalid_version"),
        ),
        (
            "plan_document/schema_version_2.json",
            "schema_version: 2",
            {SCHEMA_VERSION_FIELD: 2},
            (SCHEMA_VERSION_FIELD, "unsupported_version"),
        ),
        (
            "plan_document/executed_above_run.json",
            "tokens 4 / 5",
            {"run_token": 4, "executed_run_token": 5},
            ("executed_run_token", "invalid_tokens"),
        ),
    ):
        shown = ", ".join(f"{key} {json.dumps(value)}" for key, value in fields.items())
        corpus.add(
            file,
            {**draft, **fields},
            scenario=scenario,
            provenance=f"the draft, with {shown} set by hand",
            problems=[problem],
            derived_from=draft_file,
            by_hand=tuple(fields),
        )


def _snapshots(corpus: _Corpus, workspaces: _Workspaces) -> None:
    """Snapshots of attempts in every state the consumer projects, and legacy ones."""
    workspace = workspaces.open("running_with_failure")
    captured: list[dict[str, Any]] = []
    planned = _planned(
        lanes_plan(CONTINUE_INDEPENDENT_POLICY, plan_id="pln_lanes_running"),
        auto_run=True,
    )
    workspace.attempt(
        planned,
        TerminationReason.COMPLETED,
        {
            "lane1_demux": _duplicate_lane,
            "lane2_demux": lambda ctx: captured.append(
                workspace.snapshot("pln_lanes_running")
            ),
        },
    )
    corpus.add(
        "plan_status/running_with_failure.json",
        captured[0],
        scenario="running with a live step failure",
        provenance=(
            "the ops consumer's build_plan_snapshot, read while lane 2 ran, after "
            "lane 1 failed and its upload was blocked"
        ),
    )

    for name, removed, scenario, provenance in (
        (
            "report_without_start",
            ATTEMPT_STARTED_EVENT,
            "report but no start record",
            "the ops consumer's build_plan_snapshot over a real attempt's spool, "
            "with its plan.attempt_started record removed",
        ),
        (
            "start_without_report",
            ATTEMPT_REPORT_EVENT,
            "start record but no report (crashed)",
            "the ops consumer's build_plan_snapshot over a real attempt's spool, "
            "with its plan.attempt_report record removed, as an attempt that "
            "crashed before publishing its report leaves it",
        ),
    ):
        workspace = workspaces.open(name)
        plan_id = f"pln_{name}"
        planned = _planned(
            make_plan(spec("demux"), spec("qc", "demux"), plan_id=plan_id),
            auto_run=True,
        )
        workspace.remove_record(
            workspace.attempt(planned, TerminationReason.COMPLETED), removed
        )
        corpus.add(
            f"plan_status/{name}.json",
            workspace.snapshot(plan_id),
            scenario=scenario,
            provenance=provenance,
        )

    workspace = workspaces.open("cancelled")
    planned = _planned(
        make_plan(
            spec("demux"),
            spec("qc", "demux"),
            spec("upload", "qc"),
            plan_id="pln_cancelled",
        ),
        auto_run=True,
    )
    workspace.attempt(planned, TerminationReason.CANCELLED, {"qc": _interrupted})
    corpus.add(
        "plan_status/cancelled.json",
        workspace.snapshot("pln_cancelled"),
        scenario="cancelled",
        provenance=(
            "the ops consumer's build_plan_snapshot of an attempt interrupted "
            "(KeyboardInterrupt) while its second step ran"
        ),
    )

    workspace = workspaces.open("legacy")
    corpus.add(
        "plan_status/legacy.json",
        _legacy_snapshot(workspace),
        scenario="legacy projection",
        provenance=(
            "the ops consumer's build_plan_snapshot over events published through "
            "StepContext outside any attempt, as releases before attempt "
            "correlation published them; the second step's only event is a "
            "DataAccess write trace"
        ),
    )

    finished_file = "plan_status/finished_success.json"
    finished = corpus.document(finished_file)
    corpus.add(
        "plan_status/unversioned.json",
        changed(finished, SCHEMA_VERSION_FIELD, ABSENT),
        scenario="unversioned snapshot",
        provenance="the finished snapshot, with schema_version removed by hand",
        problems=[(SCHEMA_VERSION_FIELD, "unversioned")],
        derived_from=finished_file,
        by_hand=(SCHEMA_VERSION_FIELD,),
    )


def _fractional_timestamps(corpus: _Corpus, workspaces: _Workspaces) -> None:
    """Plan documents whose writers were given times with a fraction of a second.

    The clocks write whole seconds, so every fraction here is a time the
    corpus supplies to a writer, as a caller may. Each canonical one is kept
    verbatim, through planning, regeneration and finalization. Two hand edits
    spell the first document's ``created_at`` in ways the canonical spelling
    does not allow.
    """
    plan = make_plan(spec("demux"), spec("qc", "demux"), plan_id="pln_fractional")
    draft_file = "plan_document/fractional_timestamps.json"
    draft = corpus.add(
        draft_file,
        _planned(plan, now=FRACTIONAL_PLANNED),
        scenario="draft planned at a supplied fractional time",
        provenance="build_plan_document, given a canonical fractional time",
        sequence=FRACTIONAL_REGENERATION,
    )
    regenerated = build_plan_document(
        plan,
        REALM,
        dict(SCOPE),
        existing=draft,
        now=FRACTIONAL_REGENERATED,
        plan_generation=_generation(plan.plan_id, "B"),
    )
    corpus.add(
        "plan_document/fractional_regenerated.json",
        _stored(regenerated, after=draft),
        scenario="regenerated over a document created at a fractional time",
        provenance=(
            "build_plan_document over the stored draft, given another canonical "
            "fractional time; created_at is kept verbatim"
        ),
        sequence=FRACTIONAL_REGENERATION,
    )

    workspace = workspaces.open("fractional_finalized")
    planned = _planned(
        make_plan(spec("demux"), plan_id="pln_fractional_finalized"),
        now=FRACTIONAL_PLANNED,
        auto_run=True,
    )
    attempt = workspace.attempt(planned, TerminationReason.COMPLETED)
    corpus.add(
        "plan_document/fractional_finalized.json",
        workspace.finalized(attempt, now=FRACTIONAL_FINALIZED),
        scenario="auto-run, finalized at a supplied fractional time",
        provenance=(
            "build_plan_document given a canonical fractional time, then "
            "build_finalized_document recording the engine's attempt, given "
            "another"
        ),
    )

    for file, scenario, created_at in (
        (
            "plan_document/noncanonical_fraction.json",
            "created_at with a fraction ending in 0",
            FRACTIONAL_PLANNED.replace(".25Z", ".250Z"),
        ),
        (
            "plan_document/excess_precision.json",
            "created_at with a fraction of seven digits",
            FRACTIONAL_PLANNED.replace(".25Z", ".2500001Z"),
        ),
    ):
        corpus.add(
            file,
            {**draft, "created_at": created_at},
            scenario=scenario,
            provenance=f"the fractional draft, with created_at {created_at} set by hand",
            problems=[("created_at", "bad_timestamp")],
            derived_from=draft_file,
            by_hand=("created_at",),
        )


def _legacy_snapshot(workspace: _Workspace) -> dict[str, Any]:
    """Publish two uncorrelated step runs, and return the plan's snapshot.

    The first runs through the ``@step`` wrapper and succeeds. The second
    published only a DataAccess write trace, which sets no step state.

    Args:
        workspace: The legacy scenario's workspace.

    Returns:
        dict[str, Any]: The stored snapshot.
    """
    plan_id = "pln_legacy"

    @step
    def demux(ctx: StepContext) -> None:
        ctx.progress(50)

    def context(step_id: str, run: int) -> StepContext:
        return StepContext(
            realm=REALM,
            scope=dict(SCOPE),
            plan_id=plan_id,
            step_id=step_id,
            step_name=step_id,
            workdir=workspace.root / "work",
            scope_dir=workspace.root / "work",
            emitter=FileSpoolEmitter(workspace.spool),
            run_id=f"run_{ygg_time.utcnow_compact()}_{run:06x}",
        )

    with workspace.clock.at(LEGACY_PUBLISHED):
        demux(context("demux", 1))
        context("qc", 2).emit("data_access.write.succeeded", connection="projects")
    return workspace.snapshot(plan_id)


if __name__ == "__main__":
    sys.exit(main())
