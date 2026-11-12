"""The shared reader of plan documents and snapshots: one interpretation for all.

Yggdrasil's own decisions and any program that reads the stored documents,
such as an operations view, call these functions instead of re-deriving the
contract's rules, so two readers of the same document cannot disagree.

**Five facts, kept apart.** A plan document is never read as one "plan
status":

1. approval: ``status``;
2. the current request: the plan's ``plan_generation`` and ``run_token``,
   and whether its completion is recorded (``executed_run_token ==
   run_token``);
3. the current request's outcome: known only when its completion is recorded
   and the last finalized result belongs to that same request;
4. the latest observed attempt, from the plan's snapshot, which may belong to
   another request or generation;
5. the last finalized result, ``last_finalized_execution``, which may belong
   to an earlier request.

**Three reads, kept separate.** :func:`read_plan` gives what a plan document
establishes on its own, :func:`read_snapshot` what its ``plan_status``
snapshot establishes on its own, and :func:`correlate` what the two establish
together: whether the observed attempt ran the plan's generation, belongs to
its current request, and is the execution the plan's finalized record
describes. ``correlate`` returns a third record and never changes the other
two, so each recorded fact stays visible beside any qualification of it.

**Observation is not liveness.** A snapshot's recorded ``running`` is what was
observed, not proof that anything runs now, and a step's ``started`` is past
activity. The plan and the snapshot are written and read at different
moments, so disagreement from lag between them is normal, not corruption.

**Version first.** Every whole-document read (:func:`read_plan`,
:func:`read_snapshot`, :func:`request_state`, :func:`eligibility`,
:func:`is_eligible`, :func:`eligibility_reason`, and :func:`correlate` through
its inputs) starts with the document's ``schema_version``. An unversioned,
invalid or unsupported document is inspect-only: the result carries its
version state, one problem at ``schema_version`` and the body untouched, and
nothing is derived from it. A body that is not a JSON object is invalid and
likewise yields no conclusion. The value helpers (:func:`generation_of`,
:func:`finishes_request`, :func:`step_status`, :func:`partition_name`,
:func:`snapshot_id`, :func:`snapshot_id_for`) check no version: they are for a
caller that has already established the family.

**Guarantees.**

- *Pure*: nothing here reads storage, the clock or configuration. A caller
  passes a body, :data:`MISSING` (the store confirmed there is none) or
  :data:`UNAVAILABLE` (the read failed), and for a snapshot whose ID cannot
  be derived, :data:`LOOKUP_IMPOSSIBLE`; missing, unavailable, impossible to
  look up and invalid are different answers.
- *Silent*: nothing here logs. A caller that refuses, skips or omits a
  document on the reader's verdict reports that itself.
- *Never raising on stored content*: no stored value of any shape makes a
  function raise. Programming errors may: a ``plan_id`` or ``realm``
  argument that is not a non-empty string, a NoDocument where a body is
  required, or reads of two different plans given to :func:`correlate`.
- *Nothing filled in*: an absent control field is a reported problem and an
  unknown answer, never a default.
- *Typed equality*: an ID, a generation, a policy, an outcome or a
  termination reason matches only as a non-empty string, and a token only as
  an integer that is not a boolean. ``true`` never equals ``1``, and ``"5"``
  never equals ``5``. A value of the wrong type is reported and treated as
  missing by every comparison, so it neither confirms nor contradicts
  anything.
- *Local problems*: a problem affects only the answers that depend on it. A
  readable, supported plan stays available whatever its snapshot is, and a
  bad step time leaves the step's status and the request state usable.
- *Immutable results*: frozen dataclasses holding tuples, each derived answer
  beside the recorded value it came from.
- *Bounded diagnostics*: messages show recorded values through
  :func:`~yggdrasil.contract.diagnostics.describe_recorded` only, and name a
  path, which may hold a stored step ID, shortened the same way.
- *Standard library only*: importing this package loads neither the CouchDB
  client, the daemon nor any third-party library.
"""

from __future__ import annotations

from yggdrasil.contract.read._correlate import correlate
from yggdrasil.contract.read._eligibility import (
    eligibility,
    eligibility_reason,
    is_eligible,
)
from yggdrasil.contract.read._ids import (
    generation_of,
    partition_name,
    snapshot_id,
    snapshot_id_for,
)
from yggdrasil.contract.read._plan import read_plan, request_state
from yggdrasil.contract.read._requests import finishes_request
from yggdrasil.contract.read._results import (
    ALREADY_EXECUTED,
    CURRENT,
    DIFFERENT_EXECUTION,
    DIFFERENT_GENERATION,
    ELIGIBILITY_CODES,
    ELIGIBLE,
    FINALIZED_OBSERVATION_REASONS,
    FINALIZED_OBSERVATION_VERDICTS,
    IDENTITY_MISMATCH,
    INVALID_DOCUMENT,
    INVALID_TOKENS,
    LOOKUP_IMPOSSIBLE,
    MISSING,
    NOT_APPROVED,
    OTHER,
    PLAN_OBSERVATION_REASONS,
    PLAN_OBSERVATION_VERDICTS,
    PRESENCES,
    PRESENT,
    REQUEST_OBSERVATION_REASONS,
    REQUEST_OBSERVATION_VERDICTS,
    SAME_EXECUTION,
    SAME_GENERATION,
    STATUS_MISSING,
    UNAVAILABLE,
    UNKNOWN,
    UNSUPPORTED_DOCUMENT,
    Attempt,
    AttemptDiagnostic,
    ConflictingEndings,
    Correlation,
    Eligibility,
    Ending,
    FinalizedRecord,
    NoDocument,
    ObservedStep,
    PlanFields,
    PlannedStep,
    PlanRead,
    Recorded,
    RequestState,
    SnapshotRead,
    StepCounts,
    StepError,
    StepRef,
    StepStatus,
    Verdict,
)
from yggdrasil.contract.read._snapshot import read_snapshot
from yggdrasil.contract.read._steps import step_status

__all__ = [
    "ALREADY_EXECUTED",
    "CURRENT",
    "DIFFERENT_EXECUTION",
    "DIFFERENT_GENERATION",
    "ELIGIBILITY_CODES",
    "ELIGIBLE",
    "FINALIZED_OBSERVATION_REASONS",
    "FINALIZED_OBSERVATION_VERDICTS",
    "IDENTITY_MISMATCH",
    "INVALID_DOCUMENT",
    "INVALID_TOKENS",
    "LOOKUP_IMPOSSIBLE",
    "MISSING",
    "NOT_APPROVED",
    "OTHER",
    "PLAN_OBSERVATION_REASONS",
    "PLAN_OBSERVATION_VERDICTS",
    "PRESENCES",
    "PRESENT",
    "REQUEST_OBSERVATION_REASONS",
    "REQUEST_OBSERVATION_VERDICTS",
    "SAME_EXECUTION",
    "SAME_GENERATION",
    "STATUS_MISSING",
    "UNAVAILABLE",
    "UNKNOWN",
    "UNSUPPORTED_DOCUMENT",
    "Attempt",
    "AttemptDiagnostic",
    "ConflictingEndings",
    "Correlation",
    "Eligibility",
    "Ending",
    "FinalizedRecord",
    "NoDocument",
    "ObservedStep",
    "PlanFields",
    "PlanRead",
    "PlannedStep",
    "Recorded",
    "RequestState",
    "SnapshotRead",
    "StepCounts",
    "StepError",
    "StepRef",
    "StepStatus",
    "Verdict",
    "correlate",
    "eligibility",
    "eligibility_reason",
    "finishes_request",
    "generation_of",
    "is_eligible",
    "partition_name",
    "read_plan",
    "read_snapshot",
    "request_state",
    "snapshot_id",
    "snapshot_id_for",
    "step_status",
]
