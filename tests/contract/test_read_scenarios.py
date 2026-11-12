"""What each scenario of a plan's life says, read through the shared reader.

Each scenario is a plan document and its snapshot from one plan's history:

- a pair of the regression corpus that the manifest's sequences put
  together, or ``continue_independent_failures`` with
  ``finished_with_failures``, which record one attempt;
- a corpus snapshot beside the plan document its attempt was admitted from,
  rebuilt by ``build_plan_document`` with the attempt's generation, as the
  corpus built it;
- or a real history the corpus lacks, written by Yggdrasil's own writers
  through real attempts (:class:`tests.contract.read_support.HistoryTestCase`).

The facts asserted are the reader's: verdicts and their reasons, the request
state, the attempt, ``settled``, the step statuses. Associating planned and
observed steps into rows, counting steps and wording are a consumer's, so
where a scenario is about rows, what is asserted is the reader's part of it:
the verdict that allows association, and both inventories readable.
"""

from __future__ import annotations

import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import patch

import yggdrasil.core.engine
from tests.contract.read_support import (
    HistoryTestCase,
    ReaderTestCase,
    plan_document,
    snapshot,
)
from tests.contract.support import ABSENT, SPOOL_PLAN_ID, changed
from tests.execution_support import lanes_plan, make_plan, spec
from yggdrasil.contract.read import (
    LOOKUP_IMPOSSIBLE,
    MISSING,
    Correlation,
    PlanRead,
    Recorded,
    SnapshotRead,
    StepRef,
    Verdict,
    correlate,
    read_plan,
    read_snapshot,
)
from yggdrasil.flow.errors import PermanentStepError, TransientStepError
from yggdrasil.flow.model import CONTINUE_INDEPENDENT_POLICY, Plan
from yggdrasil.flow.step import StepContext
from yggdrasil.storage.plan_documents import build_plan_document

# When the corpus planned the plans whose documents it does not keep.
CORPUS_PLANNED = "2026-10-22T09:00:00Z"


def admitted_from(observed: dict[str, Any], plan: Plan) -> dict[str, Any]:
    """Return the plan document a corpus snapshot's attempt was admitted from.

    The corpus planned the plan with ``build_plan_document``, to run on its
    own, and kept only its snapshot; this builds the document again, with the
    generation the attempt recorded.

    Args:
        observed: The corpus snapshot.
        plan: The plan the corpus ran.

    Returns:
        dict[str, Any]: The plan document, at its first revision.
    """
    attempt = observed["attempt"]
    generation = attempt["plan_generation"] if attempt else "0" * 32
    document = build_plan_document(
        plan,
        observed["realm"],
        dict(observed["scope"]),
        auto_run=True,
        now=CORPUS_PLANNED,
        plan_generation=generation,
    )
    return {**document, "_rev": "1-corpus"}


def corpus_plan(name: str) -> Plan:
    """Return the plan the corpus ran for one of its snapshots."""
    plans: dict[str, Callable[[], Plan]] = {
        "running_with_failure": lambda: lanes_plan(
            CONTINUE_INDEPENDENT_POLICY, plan_id="pln_lanes_running"
        ),
        "start_without_report": lambda: make_plan(
            spec("demux"), spec("qc", "demux"), plan_id="pln_start_without_report"
        ),
        "cancelled": lambda: make_plan(
            spec("demux"),
            spec("qc", "demux"),
            spec("upload", "qc"),
            plan_id="pln_cancelled",
        ),
        "legacy": lambda: make_plan(
            spec("demux"), spec("qc", "demux"), plan_id="pln_legacy"
        ),
    }
    return plans[name]()


def read_pair(plan_body: Any, observed: Any) -> Correlation:
    """Read a plan and its snapshot as a consumer does, and correlate them.

    The snapshot is read under the plan's realm when the plan names a
    snapshot; a plan that names none is correlated with LOOKUP_IMPOSSIBLE, and
    MISSING stands for a snapshot the store confirmed absent.

    Args:
        plan_body: The plan document, stored under its own ``_id``.
        observed: The snapshot, or MISSING.

    Returns:
        Correlation: The pair's correlation.
    """
    plan = read_plan(plan_body, plan_id=plan_body["_id"])
    if plan.snapshot_id is None or plan.recorded is None:
        return correlate(plan, LOOKUP_IMPOSSIBLE)
    if observed is MISSING:
        return correlate(plan, MISSING)
    realm = plan.recorded.realm.value
    assert realm is not None  # a snapshot ID names the realm
    return correlate(plan, read_snapshot(observed, plan_id=plan.plan_id, realm=realm))


def observed_of(correlation: Correlation) -> SnapshotRead:
    """Return the snapshot read of a correlation."""
    assert isinstance(correlation.snapshot, SnapshotRead)
    return correlation.snapshot


def statuses(correlation: Correlation) -> list[tuple[str, str]]:
    """Return each observed step as (step ID, status), in stored order."""
    steps = observed_of(correlation).steps
    assert steps is not None
    return [(step.step_id, step.status) for step in steps]


def planned_ids(plan: PlanRead) -> list[str]:
    """Return the planned step IDs, in stored order."""
    assert plan.steps is not None
    return [step.step_id for step in plan.steps]


def fails(error: Exception) -> Callable[[StepContext], None]:
    """Return a step body that raises error."""

    def body(ctx: StepContext) -> None:
        raise error

    return body


class TestCorpusScenarios(ReaderTestCase):
    """Scenarios the regression corpus holds."""

    def test_the_rebuilt_plans_name_their_snapshots(self):
        """Each rebuilt plan document is of the snapshot's plan, realm, scope and steps."""
        for name in (
            "running_with_failure",
            "start_without_report",
            "cancelled",
            "legacy",
        ):
            with self.subTest(name):
                observed = snapshot(name)
                plan = read_plan(
                    admitted_from(observed, corpus_plan(name)),
                    plan_id=observed["plan_id"],
                )

                self.assertEqual(plan.problems, ())
                self.assertEqual(plan.snapshot_id, observed["_id"])
                self.assertEqual(planned_ids(plan), list(observed["steps"]))
                if observed["attempt"] is not None:
                    self.assertEqual(
                        plan.generation, observed["attempt"]["plan_generation"]
                    )

    def test_draft_never_run(self):
        """No snapshot: no evidence of execution, and nothing invented in its place."""
        correlation = read_pair(plan_document("draft"), MISSING)

        plan = correlation.plan
        assert plan.approval is not None and plan.request is not None
        self.assertEqual(plan.approval.value, "draft")
        self.assertIs(plan.request.finished, False)
        self.assertEqual(
            correlation.plan_and_observation, Verdict("unknown", "no_observation")
        )
        self.assertEqual(
            correlation.finalized_and_observation,
            Verdict("unknown", "nothing_finalized"),
        )
        self.assertEqual(correlation.request_and_observation.reason, "no_observation")
        self.assertEqual(planned_ids(plan), ["demux", "qc"])

    def test_branch_failure_once_settled(self):
        correlation = read_pair(
            plan_document("continue_independent_failures"),
            snapshot("finished_with_failures"),
        )

        plan = correlation.plan
        assert plan.approval is not None and plan.request is not None
        attempt = observed_of(correlation).attempt
        assert attempt is not None
        self.assertEqual(plan.approval.value, "approved")
        self.assertIs(plan.request.finished, True)
        self.assertEqual(plan.request.outcome, "failed")
        self.assertIs(attempt.settled, True)
        self.assertEqual(
            correlation.finalized_and_observation,
            Verdict("same_execution", "execution_ids_equal"),
        )
        self.assertEqual(
            statuses(correlation)[2:],
            [("lane2_demux", "succeeded"), ("lane2_upload", "succeeded")],
        )

    def test_regenerated_plan_keeps_old_results_apart(self):
        correlation = read_pair(
            plan_document("regenerated"), snapshot("finished_success")
        )

        plan = correlation.plan
        attempt = observed_of(correlation).attempt
        assert plan.approval is not None and attempt is not None
        self.assertEqual(plan.approval.value, "draft")
        self.assertIsNone(plan.finalized)
        self.assertEqual(
            correlation.plan_and_observation,
            Verdict("different_generation", "generations_differ"),
        )
        self.assertEqual(
            correlation.request_and_observation, Verdict("other", "generations_differ")
        )
        self.assertEqual(attempt.outcome.value, "succeeded")
        self.assertNotEqual(plan.generation, attempt.plan_generation.value)
        # Step IDs appear on both sides; nothing relates them.
        self.assertEqual(planned_ids(plan), ["demux", "qc"])
        self.assertEqual(
            statuses(correlation), [("demux", "succeeded"), ("qc", "succeeded")]
        )

    def test_legacy_projection_cannot_be_correlated(self):
        observed = snapshot("legacy")
        correlation = read_pair(
            admitted_from(observed, corpus_plan("legacy")), observed
        )

        self.assertEqual(observed_of(correlation).projection, "legacy")
        self.assertEqual(
            correlation.plan_and_observation, Verdict("unknown", "legacy_projection")
        )
        self.assertEqual(
            correlation.request_and_observation, Verdict("unknown", "legacy_projection")
        )
        # The plan has no finalized record, which is judged first.
        self.assertEqual(
            correlation.finalized_and_observation,
            Verdict("unknown", "nothing_finalized"),
        )
        self.assertEqual(
            statuses(correlation), [("demux", "succeeded"), ("qc", "unknown")]
        )

    def test_attempt_recorded_as_running_with_old_events(self):
        """Running was recorded; the snapshot's write time is not when anything ran."""
        observed = snapshot("start_without_report")
        correlation = read_pair(
            admitted_from(observed, corpus_plan("start_without_report")), observed
        )

        read = observed_of(correlation)
        assert (
            read.attempt is not None
            and read.written_at is not None
            and read.steps is not None
        )
        self.assertEqual(read.attempt.state.value, "running")
        self.assertIs(read.attempt.settled, False)
        self.assertIsNotNone(read.written_at.value)
        self.assertTrue(all(step.recorded_at is not None for step in read.steps))
        self.assertEqual(
            statuses(correlation), [("demux", "succeeded"), ("qc", "succeeded")]
        )
        self.assertEqual(correlation.plan_and_observation.verdict, "same_generation")

    def test_finished_attempt_not_recorded_on_the_plan(self):
        """The plan records nothing finished, and nothing is guessed about why."""
        correlation = read_pair(plan_document("approved"), snapshot("finished_success"))

        request = correlation.plan.request
        attempt = observed_of(correlation).attempt
        assert request is not None and attempt is not None
        self.assertEqual(
            (attempt.state.value, attempt.outcome.value), ("finished", "succeeded")
        )
        self.assertEqual((request.finished, request.outcome), (False, None))
        self.assertIsNone(correlation.plan.finalized)
        self.assertEqual(
            correlation.finalized_and_observation,
            Verdict("unknown", "nothing_finalized"),
        )
        self.assertEqual(correlation.request_and_observation.verdict, "current")

    def test_cancelled_attempt_is_neither_a_failed_step_nor_a_finished_request(self):
        """An interruption while a step ran: the attempt ends as cancelled."""
        observed = snapshot("cancelled")
        correlation = read_pair(
            admitted_from(observed, corpus_plan("cancelled")), observed
        )

        read = observed_of(correlation)
        attempt = read.attempt
        assert (
            attempt is not None
            and attempt.diagnostic is not None
            and read.steps is not None
        )
        # The ending and the outcome are two recorded facts, and both are read.
        self.assertEqual(attempt.state.value, "finished")
        self.assertEqual(
            attempt.termination_reason, Recorded("cancelled", "cancelled", True)
        )
        self.assertEqual(attempt.outcome.value, "failed")
        self.assertIs(attempt.is_drained.value, False)
        # A bare interrupt carries its type and no message.
        self.assertEqual(
            (attempt.diagnostic.message, attempt.diagnostic.error_type),
            (None, "KeyboardInterrupt"),
        )
        # The step that was running is interrupted, the one after it never
        # reached: neither failed, and neither is blocked.
        self.assertEqual(
            statuses(correlation),
            [("demux", "succeeded"), ("qc", "interrupted"), ("upload", "unreached")],
        )
        self.assertTrue(
            all(
                step.error is None and step.direct_blockers is None
                for step in read.steps
            )
        )
        # The report counts every step without an outcome as unreached; the
        # snapshot's "interrupted" is the finer word.
        assert attempt.counts is not None
        self.assertEqual(attempt.counts.unreached, 2)
        request = correlation.plan.request
        assert request is not None
        self.assertEqual((request.finished, request.outcome), (False, None))
        self.assertIsNone(correlation.plan.finalized)
        self.assertEqual(
            correlation.finalized_and_observation.reason, "nothing_finalized"
        )
        self.assertIs(attempt.settled, True)

    def test_attempt_without_generation_is_not_associated(self):
        observed = changed(snapshot("running"), "attempt.plan_generation", None)

        correlation = read_pair(plan_document("approved"), observed)

        self.assertEqual(
            correlation.plan_and_observation,
            Verdict("unknown", "attempt_generation_missing"),
        )
        self.assertEqual(
            statuses(correlation), [("demux", "started"), ("qc", "pending")]
        )

    def test_a_plan_without_generation_is_not_associated(self):
        plan_body = changed(plan_document("approved"), "plan_generation", ABSENT)

        correlation = read_pair(plan_body, snapshot("running"))

        self.assertEqual(
            correlation.plan_and_observation.reason, "plan_generation_missing"
        )

    def test_unmatched_steps_keep_their_rows_on_both_sides(self):
        """One generation allows association; it does not prove one inventory."""
        observed = snapshot("running")
        observed["steps"]["lane3_demux"] = observed["steps"].pop("qc")

        correlation = read_pair(plan_document("approved"), observed)

        self.assertEqual(correlation.plan_and_observation.verdict, "same_generation")
        self.assertEqual(planned_ids(correlation.plan), ["demux", "qc"])
        self.assertEqual(
            statuses(correlation), [("demux", "started"), ("lane3_demux", "pending")]
        )

    def test_invalid_snapshot_leaves_the_plan_readable(self):
        observed = changed(snapshot("running"), "projection", "materialised")

        correlation = read_pair(plan_document("approved"), observed)

        plan = correlation.plan
        assert plan.approval is not None
        self.assertFalse(observed_of(correlation).usable)
        self.assertEqual(
            correlation.plan_and_observation, Verdict("unknown", "observation_invalid")
        )
        self.assertEqual(planned_ids(plan), ["demux", "qc"])
        self.assertEqual(plan.approval.value, "approved")

    def test_same_execution_id_with_conflicting_identity_is_not_the_same_execution(
        self,
    ):
        """Equal execution IDs whose recognised generation or token differ share no identity.

        The conflict is between two usable records. A finalized record changed
        apart from its own report is no longer usable, so it is not compared
        at all.
        """
        settled = snapshot("finished_with_failures")
        for name, field, value in (
            ("generation", "plan_generation", "ffffffffffffffffffffffffffffffff"),
            ("run token", "run_token", 7),
        ):
            with self.subTest(name):
                plan_body = plan_document("continue_independent_failures")
                for path in (
                    "last_finalized_execution",
                    "last_finalized_execution.report",
                ):
                    plan_body = changed(plan_body, f"{path}.{field}", value)

                correlation = read_pair(plan_body, settled)

                self.assertEqual(
                    correlation.finalized_and_observation,
                    Verdict("unknown", "identity_conflict"),
                )
                self.assertEqual(
                    [(p.path, p.code) for p in correlation.problems],
                    [(f"last_finalized_execution.{field}", "identity_conflict")],
                )
            with self.subTest(name, changed="the record alone"):
                plan_body = changed(
                    plan_document("continue_independent_failures"),
                    f"last_finalized_execution.{field}",
                    value,
                )

                correlation = read_pair(plan_body, settled)

                self.assertEqual(
                    correlation.finalized_and_observation,
                    Verdict("unknown", "finalized_unusable"),
                )

    def test_missing_metadata_is_not_a_conflict(self):
        """Values the attempt lacks are missing evidence, not a disagreement.

        A finalized record lacking one is unusable, since it no longer agrees
        with its own report, so it is not compared at all.
        """
        settled = changed(snapshot("finished_with_failures"), "attempt.run_token", None)
        settled = changed(settled, "attempt.plan_generation", None)

        correlation = read_pair(plan_document("continue_independent_failures"), settled)

        self.assertEqual(
            correlation.finalized_and_observation,
            Verdict("same_execution", "execution_ids_equal"),
        )
        self.assertEqual(correlation.problems, ())

        plan_body = changed(
            plan_document("continue_independent_failures"),
            "last_finalized_execution.plan_generation",
            ABSENT,
        )
        correlation = read_pair(
            plan_body,
            changed(snapshot("finished_with_failures"), "attempt.run_token", None),
        )
        self.assertEqual(
            correlation.finalized_and_observation,
            Verdict("unknown", "finalized_unusable"),
        )

    def test_a_finalized_record_without_execution_id_is_not_compared(self):
        """A record without its execution ID disagrees with its report: unusable."""
        plan_body = changed(
            plan_document("continue_independent_failures"),
            "last_finalized_execution.execution_id",
            ABSENT,
        )

        correlation = read_pair(plan_body, snapshot("finished_with_failures"))

        self.assertEqual(
            correlation.finalized_and_observation,
            Verdict("unknown", "finalized_unusable"),
        )

    def test_a_plan_without_a_usable_scope_has_no_locatable_snapshot(self):
        plan_body = changed(plan_document("approved"), "scope", {"kind": "flowcell"})

        correlation = read_pair(plan_body, snapshot("running"))

        plan = correlation.plan
        self.assertIsNone(plan.snapshot_id)
        self.assertEqual(
            [(p.path, p.code) for p in plan.problems], [("scope.id", "missing_field")]
        )
        self.assertIs(correlation.snapshot, LOOKUP_IMPOSSIBLE)
        self.assertEqual(
            correlation.plan_and_observation, Verdict("unknown", "lookup_impossible")
        )


class TestCorpusSequences(ReaderTestCase):
    """One plan's documents over time, each frame read from its own documents."""

    def test_regeneration_does_not_carry_results_onto_the_new_plan(self):
        before = read_pair(
            plan_document("approved_finalized"), snapshot("finished_success")
        )
        after = read_pair(plan_document("regenerated"), snapshot("finished_success"))

        self.assertEqual(before.plan.plan_id, after.plan.plan_id)
        self.assertNotEqual(before.plan.generation, after.plan.generation)
        # Before: the attempt ran this generation, and its result is recorded.
        self.assertEqual(
            statuses(before), [("demux", "succeeded"), ("qc", "succeeded")]
        )
        assert before.plan.request is not None and after.plan.request is not None
        self.assertEqual(before.plan.request.outcome, "succeeded")
        self.assertEqual(before.finalized_and_observation.verdict, "same_execution")
        # After: the same snapshot, but none of it applies to the new plan.
        self.assertEqual(observed_of(after).attempt, observed_of(before).attempt)
        self.assertEqual(after.plan_and_observation.verdict, "different_generation")
        self.assertEqual(after.request_and_observation.verdict, "other")
        # Regeneration reset the request and dropped the recorded result.
        assert after.plan.approval is not None
        self.assertEqual(after.plan.approval.value, "draft")
        self.assertEqual(
            (after.plan.request.finished, after.plan.request.outcome), (False, None)
        )
        self.assertIsNone(after.plan.finalized)
        self.assertEqual(after.finalized_and_observation.reason, "nothing_finalized")


class TestHistories(HistoryTestCase):
    """Scenarios the corpus lacks, through real attempts."""

    def lanes(self) -> dict[str, Any]:
        """Plan two independent lanes under continue_independent."""
        return self.planned(
            *lanes_plan(CONTINUE_INDEPENDENT_POLICY, plan_id=SPOOL_PLAN_ID).steps,
            policy=CONTINUE_INDEPENDENT_POLICY,
        )

    def branch_failure(
        self,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
        """Run the lanes with lane 1 failing, keeping a snapshot taken while lane 2 ran.

        Returns:
            The planned document and the snapshot while running; the
            finalized document and the snapshot once settled.
        """
        planned = self.lanes()
        captured: list[dict[str, Any]] = []
        context = self.attempt(
            planned,
            {
                "lane1_demux": fails(
                    PermanentStepError("lane 1 is listed twice", code="DUPLICATE_LANE")
                ),
                "lane2_demux": lambda ctx: captured.append(self.stored()),
            },
        )
        return planned, captured[0], self.finalized(planned, context), self.stored()

    def rerun(
        self,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
        """Run a plan to success, then rerun it, failing fast after a reused step.

        Returns:
            The finalized document and its snapshot; the document with the
            rerun requested and the rerun's snapshot.
        """
        planned = self.planned(
            spec("prepare"), spec("run", "prepare"), spec("collect", "run")
        )
        finalized = self.finalized(planned, self.attempt(planned))
        first = self.stored()
        rerun = self.rerun_requested(finalized)
        self.run_again("run", "collect")
        self.attempt(
            rerun,
            {
                "run": fails(
                    TransientStepError(
                        "the reference is missing", code="REFERENCE_MISSING"
                    )
                )
            },
        )
        return finalized, first, rerun, self.stored()

    def unconfirmed_success(
        self,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        """Run a plan whose first step's success is published but never recorded.

        The engine publishes a step's success before it writes the step's
        success marker. When that write fails, the attempt ends as an
        orchestration error, and its report records no outcome for the step.

        Returns:
            The planned document, the snapshot taken between the published
            success and the failed write, and the snapshot once settled.
        """
        planned = self.planned(spec("prepare"), spec("run", "prepare"))
        captured: list[dict[str, Any]] = []

        def failing_write(marker: Path, fingerprint: str) -> None:
            captured.append(self.stored())
            raise OSError("no space left on device")

        with patch.object(yggdrasil.core.engine, "_replace_marker", failing_write):
            self.attempt(planned)
        return planned, captured[0], self.stored()

    def test_unconfirmed_success_settled_as_interrupted(self):
        planned, _, settled = self.unconfirmed_success()

        correlation = read_pair(planned, settled)

        attempt = observed_of(correlation).attempt
        assert attempt is not None and correlation.plan.request is not None
        self.assertEqual(
            statuses(correlation), [("prepare", "interrupted"), ("run", "unreached")]
        )
        self.assertEqual(attempt.termination_reason.value, "orchestration_error")
        self.assertIs(correlation.plan.request.finished, False)

    def test_a_provisional_success_is_not_kept_once_the_report_disagrees(self):
        planned, running_snapshot, settled_snapshot = self.unconfirmed_success()

        running = read_pair(planned, running_snapshot)
        settled = read_pair(planned, settled_snapshot)

        running_attempt = observed_of(running).attempt
        settled_attempt = observed_of(settled).attempt
        assert running_attempt is not None and settled_attempt is not None
        self.assertEqual(running_attempt.execution_id, settled_attempt.execution_id)
        self.assertIs(running_attempt.settled, False)
        self.assertEqual(
            statuses(running), [("prepare", "succeeded"), ("run", "pending")]
        )
        self.assertIs(settled_attempt.settled, True)
        self.assertEqual(
            statuses(settled), [("prepare", "interrupted"), ("run", "unreached")]
        )

    def test_branch_failure_while_running(self):
        planned, running, _, _ = self.branch_failure()

        correlation = read_pair(planned, running)

        plan = correlation.plan
        attempt = observed_of(correlation).attempt
        assert (
            plan.approval is not None
            and plan.request is not None
            and attempt is not None
        )
        # Approval never says how a run went.
        self.assertEqual(plan.approval.value, "approved")
        self.assertIs(plan.request.finished, False)
        self.assertIsNone(plan.finalized)
        self.assertIs(attempt.settled, False)
        self.assertIsNone(attempt.outcome.value)
        self.assertEqual(correlation.plan_and_observation.verdict, "same_generation")
        self.assertEqual(
            statuses(correlation),
            [
                ("lane1_demux", "failed"),
                ("lane1_upload", "blocked"),
                ("lane2_demux", "started"),
                ("lane2_upload", "pending"),
            ],
        )
        assert plan.steps is not None
        self.assertEqual(plan.steps[1].deps, (StepRef("lane1_demux", True),))
        blocked = observed_of(correlation).steps
        assert blocked is not None
        self.assertEqual(blocked[1].direct_blockers, (StepRef("lane1_demux", True),))
        self.assertIsNone(blocked[1].failed_upstream)

    def test_branch_failure_from_running_to_settled(self):
        planned, running_snapshot, finalized, settled_snapshot = self.branch_failure()

        running = read_pair(planned, running_snapshot)
        settled = read_pair(finalized, settled_snapshot)

        # One plan, one generation, one execution.
        self.assertEqual(running.plan.plan_id, settled.plan.plan_id)
        self.assertEqual(running.plan.generation, settled.plan.generation)
        running_attempt, settled_attempt = (
            observed_of(running).attempt,
            observed_of(settled).attempt,
        )
        assert running_attempt is not None and settled_attempt is not None
        self.assertEqual(running_attempt.execution_id, settled_attempt.execution_id)
        self.assertEqual(
            (running_attempt.settled, settled_attempt.settled), (False, True)
        )
        before, after = dict(statuses(running)), dict(statuses(settled))
        # The healthy branch kept going after the other one failed.
        self.assertEqual(
            (before["lane2_demux"], after["lane2_demux"]), ("started", "succeeded")
        )
        self.assertEqual(
            (before["lane2_upload"], after["lane2_upload"]), ("pending", "succeeded")
        )
        # What had failed or was blocked stays so.
        for step_id in ("lane1_demux", "lane1_upload"):
            self.assertEqual(before[step_id], after[step_id])
        # The approval never changed; the request did.
        assert running.plan.request is not None and settled.plan.request is not None
        self.assertEqual(
            (running.plan.request.finished, settled.plan.request.finished),
            (False, True),
        )
        self.assertEqual(settled.finalized_and_observation.verdict, "same_execution")
        # The failed steps upstream are established only once settled.
        running_blocked = observed_of(running).steps
        settled_blocked = observed_of(settled).steps
        assert running_blocked is not None and settled_blocked is not None
        self.assertIsNone(running_blocked[1].failed_upstream)
        self.assertEqual(
            settled_blocked[1].failed_upstream, (StepRef("lane1_demux", True),)
        )

    def test_older_finalized_success_beside_newer_failed_attempt(self):
        _, _, rerun, rerun_snapshot = self.rerun()

        correlation = read_pair(rerun, rerun_snapshot)

        plan = correlation.plan
        attempt = observed_of(correlation).attempt
        assert plan.tokens is not None and plan.request is not None
        assert plan.finalized is not None and attempt is not None
        self.assertEqual(plan.tokens.run, 1)
        self.assertEqual((plan.request.finished, plan.request.outcome), (False, None))
        self.assertEqual(plan.finalized.run_token.value, 0)
        self.assertEqual(plan.finalized.outcome.value, "succeeded")
        self.assertEqual(attempt.run_token.value, 1)
        self.assertEqual(attempt.outcome.value, "failed")
        self.assertEqual(correlation.plan_and_observation.verdict, "same_generation")
        self.assertEqual(
            correlation.finalized_and_observation,
            Verdict("different_execution", "execution_ids_differ"),
        )
        self.assertEqual(correlation.request_and_observation.verdict, "current")
        self.assertEqual(
            statuses(correlation),
            [("prepare", "reused"), ("run", "failed"), ("collect", "unreached")],
        )

    def test_a_rerun_replaces_the_shown_attempt_as_a_whole(self):
        finalized, first_snapshot, rerun, rerun_snapshot = self.rerun()

        first = read_pair(finalized, first_snapshot)
        second = read_pair(rerun, rerun_snapshot)

        self.assertEqual(first.plan.generation, second.plan.generation)
        first_attempt, second_attempt = (
            observed_of(first).attempt,
            observed_of(second).attempt,
        )
        assert first_attempt is not None and second_attempt is not None
        self.assertNotEqual(first_attempt.execution_id, second_attempt.execution_id)
        self.assertEqual(
            statuses(first),
            [("prepare", "succeeded"), ("run", "succeeded"), ("collect", "succeeded")],
        )
        self.assertEqual(first.finalized_and_observation.verdict, "same_execution")
        # The new attempt's unreached step does not inherit the earlier success.
        self.assertEqual(
            statuses(second),
            [("prepare", "reused"), ("run", "failed"), ("collect", "unreached")],
        )
        # The earlier success is still recorded, as a fact about another execution.
        record = second.plan.finalized
        assert record is not None and second.plan.request is not None
        self.assertEqual(record.execution_id.value, first_attempt.execution_id)
        self.assertIsNone(second.plan.request.outcome)
        self.assertEqual(
            second.finalized_and_observation.verdict, "different_execution"
        )

    def test_preflight_rejection(self):
        """Under continue_independent a preflight rejection finishes the request."""
        planned = self.planned(
            spec("demux", "qc"), spec("qc", "demux"), policy=CONTINUE_INDEPENDENT_POLICY
        )
        finalized = self.finalized(planned, self.attempt(planned))

        correlation = read_pair(finalized, self.stored())

        request = correlation.plan.request
        attempt = observed_of(correlation).attempt
        assert request is not None and attempt is not None
        self.assertEqual((request.finished, request.outcome), (True, "failed"))
        self.assertEqual(
            correlation.finalized_and_observation.verdict, "same_execution"
        )
        self.assertEqual(
            statuses(correlation), [("demux", "unreached"), ("qc", "unreached")]
        )
        self.assertIsNotNone(attempt.diagnostic)

    def test_invalid_planned_steps_leave_the_observation_readable(self):
        """Yggdrasil stores malformed plans and rejects them at preflight."""
        planned = self.planned(
            spec("demux", "qc"), spec("qc", "demux"), policy=CONTINUE_INDEPENDENT_POLICY
        )
        finalized = self.finalized(planned, self.attempt(planned))
        damaged = changed(finalized, "plan.steps.0.step_id", "")

        correlation = read_pair(damaged, self.stored())

        attempt = observed_of(correlation).attempt
        assert attempt is not None
        self.assertIsNone(correlation.plan.steps)
        self.assertEqual(correlation.plan_and_observation.verdict, "same_generation")
        self.assertEqual(len(statuses(correlation)), 2)
        self.assertIsNotNone(attempt.diagnostic)

    def assert_report_only(self, *, events: bool) -> None:
        """An attempt known from its report alone still names its generation.

        Args:
            events: Whether the attempt's step events survived.
        """
        planned = self.planned(
            spec("prepare"), spec("run", "prepare"), spec("collect", "run")
        )
        context = self.attempt(
            planned,
            {
                "run": fails(
                    PermanentStepError("no reference", code="REFERENCE_MISSING")
                )
            },
        )
        self.without_start_record(context)
        if not events:
            self.without_step_events(context)

        correlation = read_pair(planned, self.stored())

        self.assertEqual(correlation.plan_and_observation.verdict, "same_generation")
        self.assertEqual(
            statuses(correlation),
            [("prepare", "succeeded"), ("run", "failed"), ("collect", "unreached")],
        )
        # The plan still supplies the names the snapshot may have lost.
        plan_steps, observed_steps = (
            correlation.plan.steps,
            observed_of(correlation).steps,
        )
        assert plan_steps is not None and observed_steps is not None
        self.assertEqual(plan_steps[0].name, "prepare")
        self.assertEqual(observed_steps[0].name, "prepare" if events else None)
        # A fail-fast failure does not finish its request.
        assert correlation.plan.request is not None
        self.assertIs(correlation.plan.request.finished, False)

    def test_report_only_attempts_are_associated_by_generation(self):
        self.assert_report_only(events=True)

    def test_a_report_only_attempt_without_step_events_is_associated_too(self):
        self.assert_report_only(events=False)

    def test_run_once_plan(self):
        owner = "run_once:20cbf763-754d-f793-2135-ec3d80dde357"
        planned = self.planned(
            spec("demux"), execution_authority="run_once", execution_owner=owner
        )
        finalized = self.finalized(planned, self.attempt(planned))

        correlation = read_pair(finalized, self.stored())

        plan = correlation.plan
        attempt = observed_of(correlation).attempt
        assert (
            plan.recorded is not None
            and plan.request is not None
            and attempt is not None
        )
        self.assertEqual(plan.recorded.execution_authority.value, "run_once")
        self.assertEqual(plan.recorded.execution_owner.value, owner)
        self.assertEqual(attempt.execution_authority.value, "run_once")
        self.assertEqual(attempt.execution_owner.value, owner)
        self.assertEqual(plan.request.outcome, "succeeded")


if __name__ == "__main__":
    unittest.main()
