"""Independent branch execution end to end, through the real callers.

The test realm's branching recipes stand in for the companion demux realm's
plan: shared validation, then a metadata update and one branch per lane, each
branch a chain. In ``branch_failure`` the metadata update is independent of
the branches; in ``branch_failure_metadata_required`` it is a declared
prerequisite of each. Both fail the metadata update and lane 2's processing.

A scenario document goes the whole way. The realm's own WatchSpec turns it
into an event through WatcherManager's routing, the real ``TestRealmHandler``
plans it, and ``YggdrasilCore`` persists the plan. The plan is then executed
through the daemon's plan-event handler or through run-once, by the engine,
coordinator, event spool and plan store the core wires itself, running the
test realm's real steps. Nothing on the execution path is scripted. Snapshots
come from one ops-consumer cycle, as the daemon's ops service runs them and
run-once runs one on exit, and are read back from the backend's snapshot
store.

Every scenario runs on both internal-storage backends: SQLite as a dev daemon
builds it, and CouchDB through the production ``PlanDBManager`` and
``OpsWriter`` on an in-memory client that enforces CouchDB's revision rules.
The CouchDB change feed is the one part not modelled. No scenario needs it:
run-once admits the plans it creates through its recovery pass, and the
daemon receives its plan events as PlanWatcher emits them.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import unittest
from collections.abc import AsyncIterator
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import Mock, patch

import lib.realms.test_realm as test_realm
from lib.core_utils.event_types import EventType
from lib.core_utils.plan_eligibility import is_plan_eligible
from lib.core_utils.plan_execution import ExecutionResult, ExecutionStatus
from lib.core_utils.singleton_decorator import SingletonMeta
from lib.core_utils.yggdrasil_core import YggdrasilCore
from lib.couchdb.partitions import partition_key
from lib.ops.consumer import FileSpoolConsumer
from lib.storage import build_internal_storage
from lib.storage.protocols import InternalStorageBundle
from lib.storage.sqlite import SQLiteInternalStore
from lib.watchers.abstract_watcher import YggdrasilEvent
from lib.watchers.backends.base import RawWatchEvent
from lib.watchers.backends.checkpoint_store import InMemoryCheckpointStore
from lib.watchers.manager import WatcherManager
from lib.watchers.watchspec import BoundWatchSpec
from tests.execution_support import SCENARIO_LIMIT, Watchdog, run_bounded
from tests.plan_store_support import (
    FakeCouchServer,
    ops_writer_on,
    patch_api_exception,
    plan_db_manager_on,
)
from yggdrasil.core.execution_ids import execution_order_key
from yggdrasil.flow.outcomes import (
    AttemptReport,
    ExecutionOutcome,
    StepOutcome,
    TerminationReason,
)

REALM = "test_realm"
SCENARIO_ID = "test_scenario:lanes"
PLAN_ID = f"{REALM}:{SCENARIO_ID}"
SCOPE = {"kind": "test_scenario", "id": SCENARIO_ID}
SNAPSHOT_ID = f"{partition_key(SCOPE)}:plan_status:{REALM}:{PLAN_ID}"
CORE_LOGGER = "lib.core_utils.yggdrasil_core.YggdrasilCore"

SUCCEEDED = StepOutcome.SUCCEEDED
REUSED = StepOutcome.REUSED
FAILED = StepOutcome.FAILED
BLOCKED = StepOutcome.BLOCKED

LANE_STEPS = [
    f"lane_{lane}__{step}"
    for lane in (1, 2)
    for step in ("prepare", "process", "upload")
]

# What each recipe's attempt establishes under continue_independent.
BRANCH_FAILURE_OUTCOMES = {
    "validate_shared": SUCCEEDED,
    "update_metadata": FAILED,
    "lane_1__prepare": SUCCEEDED,
    "lane_1__process": SUCCEEDED,
    "lane_1__upload": SUCCEEDED,
    "lane_2__prepare": SUCCEEDED,
    "lane_2__process": FAILED,
    "lane_2__upload": BLOCKED,
}
METADATA_REQUIRED_OUTCOMES = {
    "validate_shared": SUCCEEDED,
    "update_metadata": FAILED,
    **dict.fromkeys(LANE_STEPS, BLOCKED),
}

# The state a snapshot shows for each outcome: the event type recording it.
STATE_OF = {
    SUCCEEDED: "step.succeeded",
    REUSED: "step.skipped",
    FAILED: "step.failed",
    BLOCKED: "step.blocked",
}


def scenario(recipe: str, **fields: Any) -> dict[str, Any]:
    """A test scenario document, as stored in the scenario database."""
    return {
        "_id": SCENARIO_ID,
        "_rev": "1-scenario",
        "type": "ygg_test_scenario",
        "recipe": recipe,
        **fields,
    }


def plan_event(doc: dict[str, Any]) -> YggdrasilEvent:
    """The event PlanWatcher emits for a plan document it saw."""
    return YggdrasilEvent(
        EventType.PLAN_EXECUTION,
        {"plan_doc_id": doc["_id"], "plan_doc": doc},
        "PlanWatcher",
    )


class IdleChangeSource:
    """A plan change feed on which nothing ever changes."""

    async def stream_changes_continuously(
        self, *, since: str | int, poll_interval_sec: float
    ) -> AsyncIterator[RawWatchEvent]:
        """Wait, without yielding, until the watcher is cancelled.

        Args:
            since: Ignored.
            poll_interval_sec: Ignored.

        Yields:
            RawWatchEvent: Never.
        """
        await asyncio.Event().wait()
        return
        yield


class SQLiteStorage:
    """Internal storage as a dev daemon builds it, on one SQLite file.

    Attributes:
        bundle: The storage the core is given.
    """

    def __init__(self, test: unittest.TestCase, directory: Path) -> None:
        path = directory / "ygg.sqlite3"
        self.bundle = build_internal_storage(
            {"internal_storage": {"backend": "sqlite", "sqlite": {"path": str(path)}}},
            dev_mode=True,
        )
        # A handle of its own on the same file, as an operator's tool opens it.
        self._database = SQLiteInternalStore(path)

    def conditional_put(self, doc: dict[str, Any]) -> None:
        """Replace a plan at the ``_rev`` it carries, as an approval actor does."""
        self._database.put_document(
            "plans", doc["_id"], doc, bump_plan_seq=True, expected_rev=doc["_rev"]
        )

    def snapshot(self, doc_id: str) -> dict[str, Any] | None:
        """Return a stored ops snapshot."""
        return self._database.get_document("operations_snapshots", doc_id)


class CouchStorage:
    """The production CouchDB storage classes, on in-memory clients.

    Attributes:
        bundle: The storage the core is given.
    """

    def __init__(self, test: unittest.TestCase, directory: Path) -> None:
        patch_api_exception(test)
        self._plans = FakeCouchServer()
        self._operations = FakeCouchServer()
        self.bundle = InternalStorageBundle(
            backend="couchdb",
            plans=plan_db_manager_on(self._plans),
            plan_changes=IdleChangeSource(),
            checkpoints=InMemoryCheckpointStore(),
            ops_snapshots=ops_writer_on(self._operations),
        )

    def conditional_put(self, doc: dict[str, Any]) -> None:
        """Replace a plan at the ``_rev`` it carries, as an approval actor does."""
        self._plans.put_document(db="yggdrasil_plans", doc_id=doc["_id"], document=doc)

    def snapshot(self, doc_id: str) -> dict[str, Any] | None:
        """Return a stored ops snapshot."""
        return self._operations.raw(doc_id)


class BranchExecutionTestCase(unittest.TestCase):
    """A YggdrasilCore as the daemon builds it, with the test realm registered.

    Subclasses choose the internal-storage backend with ``storage_type``.
    """

    storage_type: type[SQLiteStorage] | type[CouchStorage] = SQLiteStorage

    def setUp(self) -> None:
        SingletonMeta._instances.clear()
        self.addCleanup(SingletonMeta._instances.clear)
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        directory = Path(temp_dir.name)
        self.spool = directory / "spool"
        environment = patch.dict(os.environ, {"YGG_EVENT_SPOOL": str(self.spool)})
        environment.start()
        self.addCleanup(environment.stop)

        self.storage = self.storage_type(self, directory)
        self.core = YggdrasilCore(
            {"work_root": str(directory / "work")}, storage=self.storage.bundle
        )
        with patch.object(test_realm, "is_test_realm_enabled", return_value=True):
            descriptor = test_realm.get_realm_descriptor()
        assert descriptor is not None
        self.descriptor = descriptor
        self.core._register_realm_handlers(descriptor)

    # ----- planning -----

    def plan(self, doc: dict[str, Any]) -> dict[str, Any]:
        """Deliver a changed scenario document as the daemon's watchers do.

        The realm's WatchSpec filters it and builds its event, WatcherManager
        routes it to the core, and the core has the realm's handler plan it
        and persists the plan.

        Args:
            doc: The scenario document.

        Returns:
            dict[str, Any]: The persisted plan document.
        """
        watchspecs = self.descriptor.watchspecs
        (spec,) = watchspecs() if callable(watchspecs) else watchspecs
        manager = WatcherManager(
            {}, on_event=self.core.handle_event, checkpoint_store=Mock()
        )
        change = RawWatchEvent(id=doc["_id"], doc=doc, seq="1-change", deleted=False)

        async def deliver() -> None:
            planned = asyncio.get_running_loop().create_future()
            generate = self.core._generate_and_persist_plan

            async def observed(handler: Any, payload: dict[str, Any]) -> None:
                try:
                    await generate(handler, payload)
                finally:
                    planned.set_result(None)

            with patch.object(self.core, "_generate_and_persist_plan", observed):
                manager._fan_out(
                    change,
                    [BoundWatchSpec(spec=spec, realm_id=REALM)],
                    "couchdb:yggdrasil_testdocs",
                )
                await planned

        run_bounded(deliver())
        return self.stored()

    def stored(self) -> dict[str, Any]:
        """The plan document as currently stored."""
        doc = self.core.storage.plans.fetch_plan(PLAN_ID)
        assert doc is not None, f"plan {PLAN_ID} is not stored"
        return doc

    def request_rerun(self) -> None:
        """Raise the plan's run token, as an operator requesting a rerun does."""
        doc = self.stored()
        doc["run_token"] += 1
        self.storage.conditional_put(doc)

    # ----- execution -----

    def execute_as_daemon(self) -> ExecutionResult:
        """Hand the daemon the plan's PlanWatcher event; wait for its result.

        Returns:
            ExecutionResult: How the coordinator resolved the request.
        """
        event = plan_event(self.stored())
        coordinator = self.core.plan_executions

        async def deliver() -> ExecutionResult:
            submitted: list[asyncio.Future[ExecutionResult]] = []
            submit = coordinator.submit

            def recording_submit(plan_doc_id: str, claim: Any) -> Any:
                future = submit(plan_doc_id, claim)
                submitted.append(future)
                return future

            with patch.object(coordinator, "submit", recording_submit):
                self.core._handle_plan_execution_event(event)
            (future,) = submitted
            return await future

        return run_bounded(deliver())

    def run_once(self, doc: dict[str, Any]) -> int:
        """Run ``yggdrasil run-doc`` for a scenario document; return the exit code.

        The realm's handler is subscribed to the project events run-doc plans
        from, and the document is served in place of a project document.

        Args:
            doc: The scenario document.

        Returns:
            int: The exit code.
        """
        (handler,) = self.core.subscriptions[EventType.COUCHDB_DOC_CHANGED]
        self.core.register_handler(EventType.PROJECT_CHANGE, handler)
        self.core.pdm = Mock()
        self.core.pdm.fetch_document_by_id.return_value = doc
        with Watchdog(SCENARIO_LIMIT) as watchdog:
            exit_code = self.core.run_once_with_watcher(doc["_id"], timeout_seconds=30)
        self.assertFalse(watchdog.expired)
        return exit_code

    # ----- what was published -----

    def consume_spool(self) -> dict[str, Any]:
        """Run one ops consumer cycle; return the plan's snapshot as stored."""
        FileSpoolConsumer(
            self.core.event_spool, self.core.storage.ops_snapshots
        ).consume()
        return self.stored_snapshot()

    def stored_snapshot(self) -> dict[str, Any]:
        """The plan's ops snapshot as currently stored."""
        snapshot = self.storage.snapshot(SNAPSHOT_ID)
        assert snapshot is not None, f"no snapshot {SNAPSHOT_ID} is stored"
        return snapshot

    def spooled(self, execution_id: str | None = None) -> list[tuple[Path, dict]]:
        """Return the plan's spooled events, each with its path in the spool.

        Args:
            execution_id: Only this attempt's events; all when None.

        Returns:
            list[tuple[Path, dict]]: Each event, with its path relative to the
            plan's spool directory, in path order.
        """
        plan_dir = self.spool / REALM / PLAN_ID
        events = [
            (path.relative_to(plan_dir), json.loads(path.read_text()))
            for path in sorted(plan_dir.rglob("*.json"))
        ]
        return [
            (path, event)
            for path, event in events
            if execution_id is None or event.get("execution_id") == execution_id
        ]

    def events_of_type(self, event_type: str, report: AttemptReport) -> list[Path]:
        """Return where one attempt's events of one type are spooled."""
        return [
            path
            for path, event in self.spooled(report.execution_id)
            if event["type"] == event_type
        ]

    def work_file(self, step_id: str, name: str) -> Path:
        """Return a file in a step's work directory."""
        return self.core.work_root / PLAN_ID / step_id / name

    # ----- assertions -----

    def assert_drained_failure(
        self, result: ExecutionResult, outcomes: dict[str, StepOutcome]
    ) -> AttemptReport:
        """Assert a continuation attempt drained, failed, and was recorded.

        Args:
            result: How the request was resolved.
            outcomes: The outcome expected of every step.

        Returns:
            AttemptReport: The attempt's report.
        """
        self.assertEqual(result.status, ExecutionStatus.FINALIZED)
        report = result.report
        assert report is not None
        self.assertEqual(report.termination_reason, TerminationReason.COMPLETED)
        self.assertTrue(report.is_drained)
        self.assertEqual(report.outcome, ExecutionOutcome.FAILED)
        self.assertEqual(report.step_outcomes, outcomes)
        self.assertEqual(report.unreached_step_ids, [])
        return report

    def assert_recorded(self, report: AttemptReport, *, run_token: int) -> None:
        """Assert the plan document records report as its finished request.

        The request is consumed although the attempt failed, the approval
        status is left as it was, and the plan is no longer eligible.

        Args:
            report: The attempt's report.
            run_token: The run token the attempt served.
        """
        doc = self.stored()
        self.assertEqual(doc["status"], "approved")
        self.assertEqual(doc["executed_run_token"], run_token)
        record = doc["last_finalized_execution"]
        self.assertEqual(record["execution_id"], report.execution_id)
        self.assertEqual(record["run_token"], run_token)
        self.assertEqual(record["outcome"], "failed")
        self.assertEqual(record["report"], report.to_dict())
        self.assertFalse(is_plan_eligible(doc))

    def assert_correlated(self, *reports: AttemptReport) -> None:
        """Assert every spooled event belongs to one of these attempts.

        Each event must carry the execution ID of one of them, and that
        attempt's plan generation and run token.

        Args:
            reports: The reports of every attempt the plan should have had.
        """
        identities = {
            report.execution_id: (report.plan_generation, report.run_token)
            for report in reports
        }
        events = self.spooled()
        self.assertTrue(events)
        for path, event in events:
            with self.subTest(event=str(path)):
                self.assertIn(event.get("execution_id"), identities)
                self.assertEqual(
                    (event["plan_generation"], event["run_token"]),
                    identities[event["execution_id"]],
                )

    def assert_one_record_each(self, report: AttemptReport) -> None:
        """Assert the attempt published its start and its report exactly once."""
        started = self.events_of_type("plan.attempt_started", report)
        published = self.events_of_type("plan.attempt_report", report)
        self.assertEqual(
            started, [Path(f"{report.execution_id}_plan_attempt_started.json")]
        )
        self.assertEqual(
            published, [Path(f"{report.execution_id}_plan_attempt_report.json")]
        )
        ((_, event),) = [
            item
            for item in self.spooled(report.execution_id)
            if item[1]["type"] == "plan.attempt_report"
        ]
        self.assertEqual(event["report"], report.to_dict())

    def assert_snapshot_shows(
        self,
        snapshot: dict[str, Any],
        report: dict[str, Any],
        *,
        authority: str = "daemon",
    ) -> None:
        """Assert a snapshot shows one attempt, and every step as it ended.

        Args:
            snapshot: The stored snapshot.
            report: The attempt's report, serialized, as the plan store
                records it.
            authority: The execution authority the attempt ran under.
        """
        self.assertEqual(snapshot["projection"], "attempt")
        self.assertEqual(snapshot["scope"], SCOPE)
        attempt = snapshot["attempt"]
        for field in (
            "execution_id",
            "plan_generation",
            "run_token",
            "failure_policy",
            "termination_reason",
            "outcome",
            "counts",
        ):
            with self.subTest(field=field):
                self.assertEqual(attempt[field], report[field])
        self.assertEqual(attempt["execution_authority"], authority)
        self.assertEqual(attempt["state"], "finished")

        steps = snapshot["steps"]
        self.assertEqual(sorted(steps), sorted(report["step_ids"]))
        for step_id, outcome in report["step_outcomes"].items():
            with self.subTest(step=step_id):
                self.assertEqual(
                    steps[step_id]["state"], STATE_OF[StepOutcome(outcome)]
                )
                self.assertEqual(steps[step_id]["outcome"], outcome)
        for step_id in report["unreached_step_ids"]:
            with self.subTest(step=step_id):
                self.assertEqual(steps[step_id]["state"], "unreached")
                self.assertIsNone(steps[step_id]["outcome"])
                self.assertIsNone(steps[step_id]["run_id"])

    def assert_runs_belong_to(
        self, snapshot: dict[str, Any], execution_id: str
    ) -> None:
        """Assert every run a snapshot shows is a run of one attempt.

        Args:
            snapshot: The stored snapshot.
            execution_id: The attempt.
        """
        for step_id, step in snapshot["steps"].items():
            if step["run_id"] is None:
                continue
            with self.subTest(step=step_id):
                run_dir = self.spool / REALM / PLAN_ID / step_id / step["run_id"]
                events = [json.loads(p.read_text()) for p in run_dir.glob("*.json")]
                self.assertTrue(events)
                self.assertEqual(
                    {event["execution_id"] for event in events}, {execution_id}
                )


class DaemonScenarios:
    """The daemon's path, from a changed scenario document to its snapshot.

    Mixed into one test case per internal-storage backend.
    """

    def test_failed_branch_blocks_its_dependents_while_the_other_completes(self):
        doc = self.plan(scenario("branch_failure"))
        self.assertEqual(doc["status"], "approved")
        self.assertEqual(doc["plan"]["failure_policy"], "continue_independent")

        result = self.execute_as_daemon()

        report = self.assert_drained_failure(result, BRANCH_FAILURE_OUTCOMES)
        self.assertEqual(report.plan_generation, doc["plan_generation"])
        self.assertEqual(report.run_token, 0)
        self.assertEqual(
            report.direct_blockers, {"lane_2__upload": ["lane_2__process"]}
        )
        self.assertEqual(
            report.failed_ancestors, {"lane_2__upload": ["lane_2__process"]}
        )
        for step_id, message in (
            ("update_metadata", "Planned metadata update failure"),
            ("lane_2__process", "Planned failure processing lane 2"),
        ):
            failure = report.failures[step_id]
            self.assertEqual(failure.error_type, "RuntimeError")
            self.assertIn(message, failure.error)
        # Lane 1's real steps ran, although the metadata update had failed.
        self.assertEqual(
            self.work_file("lane_1__prepare", "lane_1_config.txt").read_text(),
            "Configuration for lane 1",
        )
        self.assert_recorded(report, run_token=0)

        # Events: one attempt, told apart from any other by what it stamped.
        self.assert_correlated(report)
        self.assert_one_record_each(report)
        self.assertEqual(
            self.events_of_type("step.blocked", report),
            [Path("lane_2__upload", f"{report.execution_id}_step_blocked.json")],
        )
        self.assertEqual(
            sorted(
                path.parts[0] for path in self.events_of_type("step.failed", report)
            ),
            ["lane_2__process", "update_metadata"],
        )
        self.assertEqual(
            sorted(
                path.parts[0] for path in self.events_of_type("step.succeeded", report)
            ),
            sorted(
                step_id
                for step_id, outcome in BRANCH_FAILURE_OUTCOMES.items()
                if outcome is SUCCEEDED
            ),
        )
        # A blocked step never ran, so it has no run directory.
        self.assertEqual(
            [
                p.name
                for p in (self.spool / REALM / PLAN_ID / "lane_2__upload").iterdir()
            ],
            [f"{report.execution_id}_step_blocked.json"],
        )

        snapshot = self.consume_spool()
        self.assert_snapshot_shows(snapshot, report.to_dict())
        blocked = snapshot["steps"]["lane_2__upload"]
        self.assertEqual(blocked["direct_blockers"], ["lane_2__process"])
        self.assertEqual(blocked["failed_ancestors"], ["lane_2__process"])
        self.assertIsNone(blocked["run_id"])
        self.assertEqual(
            snapshot["steps"]["update_metadata"]["error"]["error_type"], "RuntimeError"
        )
        self.assertEqual(
            snapshot["attempt"]["counts"],
            {"succeeded": 5, "reused": 0, "failed": 2, "blocked": 1, "unreached": 0},
        )

        # The finished request is not run again by a later event for it.
        again = self.execute_as_daemon()
        self.assertEqual(again.status, ExecutionStatus.NOT_ELIGIBLE)
        self.assert_correlated(report)

    def test_required_metadata_failure_blocks_every_branch(self):
        doc = self.plan(scenario("branch_failure_metadata_required"))
        self.assertEqual(doc["plan"]["failure_policy"], "continue_independent")

        result = self.execute_as_daemon()

        report = self.assert_drained_failure(result, METADATA_REQUIRED_OUTCOMES)
        self.assertEqual(
            report.direct_blockers,
            {
                "lane_1__prepare": ["update_metadata"],
                "lane_1__process": ["lane_1__prepare"],
                "lane_1__upload": ["lane_1__process"],
                "lane_2__prepare": ["update_metadata"],
                "lane_2__process": ["lane_2__prepare"],
                "lane_2__upload": ["lane_2__process"],
            },
        )
        self.assertEqual(
            report.failed_ancestors, dict.fromkeys(LANE_STEPS, ["update_metadata"])
        )
        # Lane 2's failing step is never invoked: it is blocked first.
        self.assertNotIn("lane_2__process", report.failures)
        self.assertFalse(
            self.work_file("lane_1__prepare", "lane_1_config.txt").exists()
        )
        self.assert_recorded(report, run_token=0)

        self.assert_correlated(report)
        self.assert_one_record_each(report)
        self.assertEqual(
            sorted(
                path.parts[0] for path in self.events_of_type("step.blocked", report)
            ),
            sorted(LANE_STEPS),
        )
        self.assertEqual(
            sorted(
                path.parts[0] for path in self.events_of_type("step.started", report)
            ),
            ["update_metadata", "validate_shared"],
        )

        snapshot = self.consume_spool()
        self.assert_snapshot_shows(snapshot, report.to_dict())
        for step_id in ("lane_1__prepare", "lane_2__prepare"):
            self.assertEqual(
                snapshot["steps"][step_id]["direct_blockers"], ["update_metadata"]
            )
        self.assertEqual(
            snapshot["steps"]["lane_2__upload"]["failed_ancestors"],
            ["update_metadata"],
        )

    def test_rerun_reuses_finished_work_and_retries_what_failed(self):
        self.plan(scenario("branch_failure"))
        first = self.assert_drained_failure(
            self.execute_as_daemon(), BRANCH_FAILURE_OUTCOMES
        )
        # Lane 1's declared output goes missing between the attempts.
        self.work_file("lane_1__prepare", "lane_1_config.txt").unlink()

        self.request_rerun()
        result = self.execute_as_daemon()

        report = self.assert_drained_failure(
            result,
            {
                "validate_shared": REUSED,
                "update_metadata": FAILED,
                # Its declared output is missing, so it runs again...
                "lane_1__prepare": SUCCEEDED,
                # ...while its dependents' results still stand.
                "lane_1__process": REUSED,
                "lane_1__upload": REUSED,
                "lane_2__prepare": REUSED,
                "lane_2__process": FAILED,
                "lane_2__upload": BLOCKED,
            },
        )
        self.assertEqual(report.run_token, 1)
        self.assertEqual(report.plan_generation, first.plan_generation)
        self.assertGreater(
            execution_order_key(report.execution_id),
            execution_order_key(first.execution_id),
        )
        self.assertTrue(self.work_file("lane_1__prepare", "lane_1_config.txt").exists())
        self.assert_recorded(report, run_token=1)
        self.assert_correlated(first, report)
        self.assert_one_record_each(report)
        self.assertEqual(
            [
                event["reason"]
                for _, event in self.spooled(report.execution_id)
                if event["type"] == "step.skipped"
            ],
            ["cache_hit"] * 4,
        )

        # The snapshot shows the rerun alone, never the first attempt's steps.
        snapshot = self.consume_spool()
        self.assert_snapshot_shows(snapshot, report.to_dict())
        self.assert_runs_belong_to(snapshot, report.execution_id)

    def test_fail_fast_policy_stops_at_the_first_failure_and_stays_eligible(self):
        # The same plan under the legacy policy, named by the scenario.
        doc = self.plan(scenario("branch_failure", failure_policy="fail_fast"))
        self.assertEqual(doc["plan"]["failure_policy"], "fail_fast")

        with self.assertLogs(CORE_LOGGER, level=logging.ERROR):
            result = self.execute_as_daemon()

        self.assertEqual(result.status, ExecutionStatus.UNFINISHED)
        report = result.report
        assert report is not None
        self.assertEqual(report.termination_reason, TerminationReason.FAILED_FAST)
        self.assertEqual(
            report.step_outcomes,
            {"validate_shared": SUCCEEDED, "update_metadata": FAILED},
        )
        # Work the failure cut short is unreached, not blocked.
        self.assertEqual(report.unreached_step_ids, LANE_STEPS)
        self.assertEqual(self.events_of_type("step.blocked", report), [])
        stored = self.stored()
        self.assertEqual(stored["executed_run_token"], -1)
        self.assertNotIn("last_finalized_execution", stored)
        self.assertTrue(is_plan_eligible(stored))

        self.assert_correlated(report)
        self.assert_one_record_each(report)
        snapshot = self.consume_spool()
        self.assert_snapshot_shows(snapshot, report.to_dict())


class RunOnceScenarios:
    """``yggdrasil run-doc`` from the scenario document to its snapshot.

    Mixed into one test case per internal-storage backend.
    """

    def test_failed_branch_finishes_the_healthy_one_and_exits_nonzero(self):
        exit_code = self.run_once(scenario("branch_failure"))

        self.assertEqual(exit_code, 1)
        doc = self.stored()
        self.assertEqual(doc["execution_authority"], "run_once")
        self.assertTrue(doc["execution_owner"].startswith("run_once:"))
        self.assertEqual(doc["executed_run_token"], 0)
        record = doc["last_finalized_execution"]
        self.assertEqual(record["outcome"], "failed")
        self.assertEqual(
            record["report"]["step_outcomes"],
            {
                step_id: outcome.value
                for step_id, outcome in BRANCH_FAILURE_OUTCOMES.items()
            },
        )
        self.assertTrue(self.work_file("lane_1__prepare", "lane_1_config.txt").exists())

        # Run-once writes the snapshot itself on its way out.
        snapshot = self.stored_snapshot()
        self.assert_snapshot_shows(snapshot, record["report"], authority="run_once")
        self.assertEqual(snapshot["attempt"]["execution_owner"], doc["execution_owner"])
        self.assert_runs_belong_to(snapshot, record["execution_id"])


class TestDaemonBranchesSQLite(DaemonScenarios, BranchExecutionTestCase):
    storage_type = SQLiteStorage


class TestDaemonBranchesCouchDB(DaemonScenarios, BranchExecutionTestCase):
    storage_type = CouchStorage


class TestRunOnceBranchesSQLite(RunOnceScenarios, BranchExecutionTestCase):
    storage_type = SQLiteStorage


class TestRunOnceBranchesCouchDB(RunOnceScenarios, BranchExecutionTestCase):
    storage_type = CouchStorage


if __name__ == "__main__":
    unittest.main()
