import asyncio
import importlib.metadata
import logging
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from lib.core_utils.event_types import EventType
from lib.core_utils.logging_utils import custom_logger
from lib.core_utils.plan_execution import (
    DAEMON_CLAIM,
    ExecutionClaim,
    ExecutionResult,
    ExecutionStatus,
    PlanExecutionCoordinator,
)
from lib.core_utils.runtime_paths import resolve_event_spool, resolve_work_root
from lib.core_utils.singleton_decorator import singleton
from lib.couchdb.project_db_manager import ProjectDBManager
from lib.handlers.base_handler import BaseHandler
from lib.ops.consumer_service import OpsConsumerService
from lib.storage import InternalStorageBundle, build_internal_storage
from lib.watchers.abstract_watcher import YggdrasilEvent
from lib.watchers.manager import WatcherManager
from lib.watchers.plan_watcher import PlanWatcher
from lib.watchers.watchspec import BoundWatchSpec
from yggdrasil.core.engine import Engine
from yggdrasil.core.realm import RealmDescriptor, discover_realms
from yggdrasil.flow.events.emitter import FileSpoolEmitter
from yggdrasil.flow.planner.api import PlanDraft, PlanningContext


def _generate_run_once_owner() -> str:
    """
    Generate unique execution owner token for run-once invocation.

    Format: run_once:<uuid4>

    This token uniquely identifies a CLI session, ensuring concurrent
    run-once invocations don't interfere with each other.
    """
    return f"run_once:{uuid.uuid4()}"


@singleton
class YggdrasilCore:
    """
    Central orchestrator that manages:
    - Multiple watchers (file system, CouchDB, etc.)
    - Event handlers (one or more handlers for specific event types)
    - (future) Semi-automatic (CLI) calls that bypass watchers
    """

    def __init__(
        self,
        config: Mapping[str, Any],
        logger: logging.Logger | None = None,
        *,
        config_path: str | Path | None = None,
        storage: InternalStorageBundle | None = None,
    ):
        """
        Args:
            config: A dictionary of global Yggdrasil settings.
            logger: If not provided, a default named logger is created.
            config_path: Optional path to the loaded config file for diagnostics.
            storage: Optional internal-storage bundle injection (tests).
                Defaults to resolving the ``internal_storage`` config block.
        """
        self.config = config
        self.config_path = Path(config_path) if config_path is not None else None
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")
        self._running = False

        # Internal storage boundary (plans, plan changes, checkpoints, ops
        # snapshots). Resolved once; all internal persistence goes through it.
        self.storage = storage or build_internal_storage(config)

        # ProjectDBManager is external (projects DB) and NOT part of internal
        # storage; it is constructed lazily and only used by run-doc paths.
        self._pdm: ProjectDBManager | None = None

        # Watchers: a list of classes that inherit from AbstractWatcher
        self.watchers: list = []

        # Handlers per event
        self.subscriptions: dict[EventType, list[BaseHandler]] = {}

        # Realm registry: realm_id -> RealmDescriptor
        self._realm_registry: dict[str, RealmDescriptor] = {}

        # Legacy realm class registry: realm_id -> handler class
        # Used only by _derive_realm_id() for legacy handler dedup.
        # Kept separate from _realm_registry (which stores RealmDescriptors).
        self._legacy_realm_class_registry: dict[str, type] = {}

        # Handler identity registry: (realm_id, handler_id) -> handler instance
        self._handler_identity_registry: dict[tuple[str, str], BaseHandler] = {}

        # WatcherManager (Phase 2; set by setup_realms)
        self.watcher_manager: Any = None

        # Ops consumer service (writes plan_status snapshots via the bundle)
        self.ops_consumer = OpsConsumerService(
            interval_sec=2.0, writer=self.storage.ops_snapshots
        )

        # Machine-local runtime paths, resolved once and reused everywhere
        # (engine, planning contexts, spool drain) so one process cannot
        # split across two roots.
        self.work_root = resolve_work_root(config)
        self.event_spool = resolve_event_spool()

        # Engine for plan execution (handlers now return plans; core executes them)
        self.engine = Engine(
            work_root=self.work_root,
            emitter=FileSpoolEmitter(spool_dir=self.event_spool),
        )

        self._init_db_managers()

        # One coordinator for both execution callers (daemon and run-once):
        # admission from one plan snapshot, at most one attempt per plan in
        # flight, result interpretation and bounded finalization.
        self.plan_executions = PlanExecutionCoordinator(
            engine=self.engine,
            plan_store=self.storage.plans,
            logger=self._logger,
        )

        self._logger.info("YggdrasilCore initialized.")

    def _persist_plan_draft(
        self,
        draft: PlanDraft,
        handler_realm: str,
        *,
        execution_authority: str = "daemon",
        execution_owner: str | None = None,
    ) -> str:
        """
        Persist a PlanDraft to the yggdrasil_plans database.

        Creates or updates the plan document with status based on auto_run flag.
        On regeneration, execution tokens are reset (plan becomes eligible again).

        Args:
            draft: The PlanDraft returned by the handler
            handler_realm: The realm_id from the handler that generated this draft
            execution_authority: Who owns execution - "daemon" (default) or "run_once"
            execution_owner: Unique token for run_once isolation (e.g., "run_once:<uuid>")

        Returns:
            str: The database document ID of the persisted plan
        """
        plan_doc_id = self.plan_dbm.save_plan(
            plan=draft.plan,
            realm=handler_realm,
            scope=draft.plan.scope,
            auto_run=draft.auto_run,
            execution_authority=execution_authority,
            execution_owner=execution_owner,
            preview=draft.preview,
            notes=draft.notes,
        )

        self._logger.info(
            "Persisted plan '%s' to yggdrasil_plans (realm=%s, auto_run=%s, authority=%s)",
            plan_doc_id,
            handler_realm,
            draft.auto_run,
            execution_authority,
        )

        return plan_doc_id

    @property
    def pdm(self) -> ProjectDBManager:
        """Lazily constructed ProjectDBManager (external 'projects' DB).

        Only the run-doc paths use it; normal daemon operation never
        constructs it. It is not part of internal storage.
        """
        if self._pdm is None:
            self._logger.info("Initializing ProjectDBManager (run-doc path)...")
            self._pdm = ProjectDBManager()
        return self._pdm

    @pdm.setter
    def pdm(self, value: ProjectDBManager) -> None:
        """Allow explicit injection (tests)."""
        self._pdm = value

    def _init_db_managers(self):
        """
        Wire database access for Core.

        - plan_dbm: PlanStore from the internal-storage bundle
        - pdm: ProjectDBManager is lazy (see the ``pdm`` property) and only
          constructed by the run-doc paths.
        """
        self._logger.info("Initializing DB managers...")

        self.plan_dbm = self.storage.plans

        self._logger.info("DB managers initialized.")

    def _derive_realm_id(self, handler, ep=None) -> None:
        realm_id = getattr(handler, "realm_id", None)
        if not realm_id:
            # deterministic fallback, but still explicit:
            # prefer entry-point dist name; else top-level module
            realm_id = (
                (
                    getattr(getattr(ep, "dist", None), "name", None)
                    or handler.__module__.split(".")[0]
                )
                .replace("-", "_")
                .lower()
            )
            # surface it back to the handler for consistency
            setattr(handler, "realm_id", realm_id)

        # Enforce uniqueness across classes (legacy path only)
        prev = self._legacy_realm_class_registry.get(realm_id)
        if prev and prev is not handler.__class__:
            raise RuntimeError(
                f"Duplicate realm_id '{realm_id}' claimed by "
                f"{prev.__module__}.{prev.__qualname__} and "
                f"{handler.__class__.__module__}.{handler.__class__.__qualname__}. "
                f"Set a unique `realm_id` on your handler."
            )
        self._legacy_realm_class_registry[realm_id] = handler.__class__

    def _make_planning_ctx(
        self, handler, scope: dict[str, Any], *, doc: dict[str, Any], reason: str
    ) -> PlanningContext:
        scope_dir = self.work_root / handler.realm_id / scope["id"]
        emitter = FileSpoolEmitter(spool_dir=self.event_spool)
        # ops_db = os.getenv("OPS_DB", "yggdrasil_ops")
        return handler.build_planning_context(
            scope=scope,
            scope_dir=scope_dir,
            emitter=emitter,
            source_doc=doc,
            reason=reason,
            # ops_db=ops_db,
        )

    # -------------------------------------------------------------------------
    # Phase 2: Realm Discovery & Registration
    # -------------------------------------------------------------------------

    def setup_realms(self) -> None:
        """
        Discover and register all realms, handlers, and watchspecs.

        This replaces setup_handlers() for the new realm-based architecture.
        Called early in startup, before setup_watchers().

        Flow:
            1. Discover realms via ygg.realm entry points
            2. Register legacy ygg.handler entry points (backward compat)
            3. Validate realm_id uniqueness
            4. Instantiate and register handlers from each realm
            5. Collect and validate watchspecs
            6. Wire watchspecs into WatcherManager
        """
        self._logger.info("Discovering realms...")

        # 1. Discover realms via ygg.realm entry points
        descriptors = discover_realms()

        # 2. Legacy ygg.handler support (backward compat)
        legacy_descriptors = self._discover_legacy_handlers()
        descriptors.extend(legacy_descriptors)

        if not descriptors:
            self._logger.warning("No realms discovered.")
            return

        # 3. Validate realm_id uniqueness
        self._validate_realm_id_uniqueness(descriptors)

        # 4. Register handlers from each realm
        all_bound_specs: list[BoundWatchSpec] = []

        for descriptor in descriptors:
            self._realm_registry[descriptor.realm_id] = descriptor
            self._register_realm_handlers(descriptor)

            # 5. Collect watchspecs
            bound_specs = self._collect_realm_watchspecs(descriptor)
            all_bound_specs.extend(bound_specs)

        # 6. Validate watchspec bindings
        if all_bound_specs:
            self._validate_watchspec_bindings(all_bound_specs)
            self._setup_watcher_manager(all_bound_specs)

        # Summary
        self._log_realm_summary()

    def _discover_legacy_handlers(self) -> list[RealmDescriptor]:
        """
        Discover handlers via legacy ygg.handler entry points.

        Wraps each legacy handler into a RealmDescriptor for uniform
        processing.  Logs deprecation notice.

        Returns:
            List of RealmDescriptor (one per legacy handler)
        """
        eps_list = list(importlib.metadata.entry_points(group="ygg.handler"))

        # Deduplicate
        seen: set[tuple[str, str]] = set()
        unique_eps = []
        for ep in eps_list:
            key = (ep.name, ep.value)
            if key not in seen:
                seen.add(key)
                unique_eps.append(ep)

        if not unique_eps:
            return []

        self._logger.info(
            "Found %d legacy 'ygg.handler' entry point(s) "
            "(migrate to 'ygg.realm' entry points)",
            len(unique_eps),
        )

        descriptors: list[RealmDescriptor] = []

        for ep in unique_eps:
            try:
                handler_cls = ep.load()
            except Exception as e:
                self._logger.exception(
                    "✘  Legacy handler '%s' load failed: %s", ep.name, e
                )
                continue

            event_type_raw = getattr(handler_cls, "event_type", None)
            event_type = self._as_event_type(event_type_raw)
            if not event_type:
                self._logger.error(
                    "✘  Legacy handler '%s' skipped: invalid event_type '%r'",
                    ep.name,
                    event_type_raw,
                )
                continue

            # Derive realm_id for legacy handler
            realm_id = getattr(handler_cls, "realm_id", None)
            if not realm_id:
                realm_id = (
                    (
                        getattr(getattr(ep, "dist", None), "name", None)
                        or handler_cls.__module__.split(".")[0]
                    )
                    .replace("-", "_")
                    .lower()
                )

            # Ensure handler_id exists (legacy handlers may not have it)
            if not getattr(handler_cls, "handler_id", None):
                handler_cls.handler_id = ep.name  # type: ignore[attr-defined]
                self._logger.debug(
                    "Assigned handler_id='%s' to legacy handler %s",
                    ep.name,
                    handler_cls.__qualname__,
                )

            desc = RealmDescriptor(
                realm_id=realm_id,
                handler_classes=[handler_cls],
                watchspecs=[],  # Legacy handlers don't provide watchspecs
            )
            descriptors.append(desc)
            self._logger.info(
                "✓  Wrapped legacy handler '%s' as realm '%s'",
                ep.name,
                realm_id,
            )

        return descriptors

    def _validate_realm_id_uniqueness(self, descriptors: list[RealmDescriptor]) -> None:
        """
        Validate that all realm_ids are unique.  Fatal on collision.
        """
        seen: dict[str, int] = {}  # realm_id -> index

        for idx, desc in enumerate(descriptors):
            if desc.realm_id in seen:
                raise RuntimeError(
                    f"Duplicate realm_id '{desc.realm_id}'. "
                    f"Already registered. Each realm must have a unique realm_id."
                )
            seen[desc.realm_id] = idx

    def _register_realm_handlers(self, descriptor: RealmDescriptor) -> None:
        """
        Instantiate and register all handlers from a realm descriptor.

        Handler provisioning (v1):
            - Instantiate with no args: ``handler = handler_cls()``
            - Set ``handler.realm_id`` from descriptor
            - Validate handler_id presence
            - Register with subscription system
        """
        realm_id = descriptor.realm_id

        for handler_cls in descriptor.handler_classes:
            # Validate handler_id
            handler_id = getattr(handler_cls, "handler_id", None)
            if not handler_id:
                raise RuntimeError(
                    f"Handler class {handler_cls.__module__}.{handler_cls.__qualname__} "
                    f"from realm '{realm_id}' missing required 'handler_id' class attribute."
                )

            # Check (realm_id, handler_id) uniqueness
            composite_key = (realm_id, handler_id)
            if composite_key in self._handler_identity_registry:
                existing = self._handler_identity_registry[composite_key]
                raise RuntimeError(
                    f"Duplicate handler identity ({realm_id}, {handler_id}): "
                    f"already registered by {existing.class_qualified_name()}, "
                    f"cannot register {handler_cls.__module__}.{handler_cls.__qualname__}"
                )

            # Validate event_type
            event_type_raw = getattr(handler_cls, "event_type", None)
            event_type = self._as_event_type(event_type_raw)
            if not event_type:
                raise RuntimeError(
                    f"Handler class {handler_cls.__qualname__} from realm '{realm_id}' "
                    f"has invalid or missing 'event_type' class attribute: {event_type_raw!r}"
                )

            # Instantiate (v1: no args)
            try:
                handler = handler_cls()  # type: ignore[call-arg]
            except Exception as e:
                raise RuntimeError(
                    f"Failed to instantiate handler {handler_cls.__qualname__} "
                    f"from realm '{realm_id}': {e}"
                ) from e

            # Set realm_id on instance
            handler.realm_id = realm_id

            # Register in identity registry
            self._handler_identity_registry[composite_key] = handler

            # Register in subscription system
            subs = self.subscriptions.setdefault(event_type, [])
            subs.append(handler)

            self._logger.debug(
                "Registered handler '%s' (id=%s) for '%s' from realm '%s'",
                handler.class_qualified_name(),
                handler_id,
                event_type.name,
                realm_id,
            )

        self._logger.info(
            "✓  Realm '%s': %d handler(s) registered",
            realm_id,
            len(descriptor.handler_classes),
        )

    def _collect_realm_watchspecs(
        self, descriptor: RealmDescriptor
    ) -> list[BoundWatchSpec]:
        """Collect and bind watchspecs from a realm descriptor."""
        specs = descriptor.get_watchspecs()

        bound_specs = []
        for spec in specs:
            bound_specs.append(BoundWatchSpec(spec=spec, realm_id=descriptor.realm_id))
            self._logger.debug(
                "Collected WatchSpec: realm=%s, backend=%s, connection=%s, event_type=%s",
                descriptor.realm_id,
                spec.backend,
                spec.connection,
                spec.event_type.name,
            )

        return bound_specs

    def _validate_watchspec_bindings(self, bound_specs: list[BoundWatchSpec]) -> None:
        """
        Validate WatchSpec → handler bindings.

        Rules (all violations are fatal):
            1. If target_handlers is set: every handler_id must exist
               in that realm.
            2. If target_handlers is None: at least one handler in realm
               must subscribe to spec.event_type (prevents silent no-op).
        """
        for bs in bound_specs:
            realm_id = bs.realm_id
            spec = bs.spec

            realm_handler_ids = self._get_realm_handler_ids(realm_id)
            realm_event_types = self._get_realm_event_types(realm_id)

            if spec.target_handlers:
                # Rule 1: All target_handlers must exist
                for handler_id in spec.target_handlers:
                    if handler_id not in realm_handler_ids:
                        raise RuntimeError(
                            f"WatchSpec from realm '{realm_id}' references unknown "
                            f"handler_id '{handler_id}'. "
                            f"Registered handlers for this realm: {realm_handler_ids}"
                        )
            else:
                # Rule 2: At least one handler subscribes to event_type
                if spec.event_type not in realm_event_types:
                    raise RuntimeError(
                        f"WatchSpec from realm '{realm_id}' has event_type "
                        f"'{spec.event_type.name}' but no handler in this realm "
                        f"subscribes to it. This would produce events with no "
                        f"receivers. Either add target_handlers or add a handler "
                        f"subscribing to '{spec.event_type.name}'."
                    )

    def _get_realm_handler_ids(self, realm_id: str) -> list[str]:
        """Get list of handler_ids registered for a realm."""
        return [
            hid for (rid, hid) in self._handler_identity_registry if rid == realm_id
        ]

    def _get_realm_event_types(self, realm_id: str) -> set[EventType]:
        """Get set of event_types that handlers in this realm subscribe to."""
        event_types: set[EventType] = set()
        for (rid, _), handler in self._handler_identity_registry.items():
            if rid == realm_id:
                event_types.add(handler.event_type)
        return event_types

    def _setup_watcher_manager(self, bound_specs: list[BoundWatchSpec]) -> None:
        """
        Initialize WatcherManager with validated WatchSpecs.

        ``config`` and ``watcher_policy`` are extracted from ``self.config`` and
        passed explicitly. WatcherManager performs no config loading of its own.
        """
        self._logger.info("Setting up WatcherManager...")

        self.watcher_manager = WatcherManager(
            config=self.config.get("external_systems", {}),
            on_event=self.handle_event,
            checkpoint_store=self.storage.checkpoints,
            logger=self._logger,
            watcher_policy=self.config.get("watchers", {}),
            config_path=self.config_path,
        )

        for bound_spec in bound_specs:
            self.watcher_manager.add_watchspec(bound_spec)

        self._logger.info(
            "WatcherManager configured with %d WatchSpec(s)",
            len(bound_specs),
        )

    def _log_realm_summary(self) -> None:
        """Log summary of registered realms and handlers."""
        if not self._realm_registry:
            self._logger.warning("No realms registered.")
            return

        for realm_id, desc in self._realm_registry.items():
            handler_ids = self._get_realm_handler_ids(realm_id)
            spec_count = len(desc.get_watchspecs())
            self._logger.info(
                "Realm '%s': %d handler(s) [%s], %d WatchSpec(s)",
                realm_id,
                len(handler_ids),
                ", ".join(handler_ids),
                spec_count,
            )

        # Subscription summary
        summary = ", ".join(
            f"{et.name}({len(handlers)})" for et, handlers in self.subscriptions.items()
        )
        self._logger.debug("Handler Registrations: %s", summary)

    # -------------------------------------------------------------------------
    # Legacy watcher/handler registration (kept for backward compat)
    # -------------------------------------------------------------------------

    def register_watcher(self, watcher) -> None:
        """
        Attach a watcher (e.g. CouchDBWatcher, PlanWatcher).
        The watchers will be started/stopped by YggdrasilCore.
        """
        self._logger.debug(f"Registering watcher: {watcher}")
        self.watchers.append(watcher)

    def register_handler(self, event_type: EventType, handler: BaseHandler) -> None:
        """
        Register a handler for a given event type.
        `subscriptions` is the mapping: EventType -> [handlers...]
        De-duplicates by handler class (module + qualname) per event.
        """

        subs = self.subscriptions.setdefault(event_type, [])
        handler_key = handler.class_key()  # (module, qualname)

        already_registered = any(handler.class_key() == handler_key for handler in subs)
        if already_registered:
            self._logger.warning(
                "Handler already registered for '%s': '%s'; skipping",
                event_type.name,
                handler.class_qualified_name(),
            )
            return

        subs.append(handler)
        self._logger.debug(
            "Registered handler '%s' for '%s'",
            handler.class_qualified_name(),
            event_type.name,
        )

    def _as_event_type(self, maybe_enum: Any) -> EventType | None:
        """Best-effort normalize unknown enums/strings to our EventType.

        Accepts EventType OR a look-alike enum with same .value OR a raw string.
        """
        try:
            if isinstance(maybe_enum, EventType):
                return maybe_enum
            if hasattr(maybe_enum, "value"):
                val = getattr(maybe_enum, "value")
                return next((e for e in EventType if e.value == val), None)
            if isinstance(maybe_enum, str):
                return next((e for e in EventType if e.value == maybe_enum), None)
        except Exception:
            return None
        return None

    def setup_watchers(self):
        """
        Set up core infrastructure watchers.

        Note: Domain-specific CouchDB/FS watchers are now configured via
        WatchSpecs in setup_realms() and handled by WatcherManager.
        """
        self._logger.info("Setting up watchers...")
        if self.watcher_manager:
            self.watcher_manager.validate_configuration()
        self._setup_plan_watcher()
        self._logger.info("Watchers setup done.")

    def _setup_plan_watcher(self) -> None:
        """
        Set up the PlanWatcher for daemon mode.

        The daemon's PlanWatcher is configured to only process plans with
        execution_authority='daemon', skipping 'run_once' plans which are
        managed by their respective CLI sessions.

        The watcher emits PLAN_EXECUTION_EVENT for eligible plans, which
        hands the plan to the execution coordinator via
        _handle_plan_execution_event().
        """
        poll_interval = self.config.get("plan_watcher_poll_interval", 5.0)

        plan_watcher = PlanWatcher(
            on_event=self._handle_plan_execution_event,
            poll_interval_sec=poll_interval,
            plan_store=self.storage.plans,
            change_source=self.storage.plan_changes,
            checkpoint_store=self.storage.checkpoints,
            execution_authority_filter="daemon",  # Skip run_once plans
        )
        self.register_watcher(plan_watcher)
        self._plan_watcher = plan_watcher  # Keep reference for recovery
        self._logger.info(
            "Registered PlanWatcher for daemon mode (poll_interval=%.1fs, authority_filter='daemon')",
            poll_interval,
        )

    def _handle_plan_execution_event(self, event: YggdrasilEvent) -> None:
        """
        Handle EventType.PLAN_EXECUTION from PlanWatcher.

        This is the callback invoked when PlanWatcher detects an eligible plan.
        It hands the plan to the execution coordinator without waiting. The
        event only prompts a check: the plan is admitted from a fresh read of
        its document, never from the document the event carries, and an event
        for a plan already in flight is checked again once that attempt is
        done rather than run alongside it.

        Args:
            event: YggdrasilEvent with payload containing plan_doc_id
        """
        if event.event_type != EventType.PLAN_EXECUTION:
            self._logger.warning(
                "Unexpected event type in plan execution handler: %s", event.event_type
            )
            return

        payload = event.payload or {}
        plan_doc_id = payload.get("plan_doc_id")

        if not plan_doc_id:
            self._logger.error("Plan execution event missing 'plan_doc_id'")
            return

        self._logger.info(
            "Received plan execution event for '%s' from '%s'",
            plan_doc_id,
            event.source,
        )

        # Non-blocking: the coordinator keeps the execution task, and logs how
        # the request was resolved.
        self.plan_executions.submit(plan_doc_id, DAEMON_CLAIM)

    async def _recover_approved_plans(self) -> None:
        """Queue all eligible plans through the configured daemon watcher.

        This helper is not currently called by :meth:`start` (see Tech Debt
        #17). The watcher's recovery scan emits callbacks, which queue
        execution; this method neither inspects nor initializes a checkpoint.
        A future startup caller must coordinate the scan with the live change
        cursor and deduplicate any overlap.
        """
        if not hasattr(self, "_plan_watcher") or self._plan_watcher is None:
            self._logger.warning("No PlanWatcher configured; skipping recovery")
            return

        self._logger.info("Starting approved plan recovery...")

        try:
            # Use PlanWatcher's recovery method (which queries + emits)
            eligible_plans = await self._plan_watcher.recover_pending_plans()

            self._logger.info(
                "Recovery complete: %d plans queued for execution",
                len(eligible_plans),
            )

        except Exception as exc:
            self._logger.exception("Error during plan recovery: %s", exc)

    async def start(self) -> None:
        """
        Start all watchers in parallel. Typically called once at system startup.
        This will run indefinitely until watchers exit or self.stop() is called.

        Phase 2: Starts WatcherManager (new) alongside legacy watchers.
        """
        if self._running:
            self._logger.warning("YggdrasilCore is already running.")
            return

        self._running = True

        self._logger.info("Starting operations consumer service...")
        self.ops_consumer.start()

        # Start WatcherManager (Phase 2+)
        if self.watcher_manager:
            self._logger.info("Starting WatcherManager...")
            await self.watcher_manager.start()

        self._logger.info("Starting all watchers...")

        # Start legacy watchers as async tasks
        tasks = [asyncio.create_task(w.start()) for w in self.watchers]
        self._logger.info(f"Running {len(tasks)} watchers in parallel.")

        # Wait until all watchers exit (or are stopped)
        await asyncio.gather(*tasks, return_exceptions=True)
        self._logger.info("All watchers have exited or been stopped.")

    async def stop(self) -> None:
        """
        Stop all watchers gracefully. This sets _running=False, so watchers that
        poll or wait in loops will naturally exit. Then we wait for them to finish.

        Plan executions in flight are asked to stop starting new steps before
        anything else, since stopping the watchers can take a while. Once the
        watchers have stopped, every execution still in flight, including one
        a watcher started meanwhile, is asked again and awaited until its
        running step has finished and its result is recorded or retained. The
        ops consumer stops last, so it can still pick up the events those
        executions publish.
        """
        if not self._running:
            self._logger.debug("YggdrasilCore stop called, but not running.")
            return

        self._running = False
        self.plan_executions.request_cancellation()

        self._logger.info("Stopping all watchers...")

        # Stop legacy watchers
        stop_tasks = [asyncio.create_task(w.stop()) for w in self.watchers]
        await asyncio.gather(*stop_tasks)
        self._logger.info("All watchers stopped.")

        # Stop WatcherManager (Phase 2+)
        if self.watcher_manager:
            await self.watcher_manager.stop()
            self._logger.info("WatcherManager stopped.")

        await self.plan_executions.drain()

        # Stop the ops consumer service
        try:
            await self.ops_consumer.stop()
            self._logger.info("Ops consumer service stopped.")
        except asyncio.CancelledError:
            # Task was cancelled during shutdown (expected)
            self._logger.debug("Ops consumer task cancelled (expected during shutdown)")

    # def run_once(self, doc_id: str):
    #     """
    #     Fetch the project doc, build the payload, and synchronously
    #     drive the BestPracticeAnalysisHandler without starting watchers.
    #     """
    #     from lib.core_utils.module_resolver import get_module_location
    #     from lib.couchdb.project_db_manager import ProjectDBManager

    #     pdm = ProjectDBManager()
    #     doc = pdm.fetch_document_by_id(doc_id)
    #     if not doc:
    #         self._logger.error(f"No project with ID {doc_id}")
    #         return

    #     module_loc = get_module_location(doc)
    #     if not module_loc:
    #         self._logger.error(f"No module for project {doc_id}")
    #         return

    #     payload = {"document": doc, "module_location": module_loc}

    #     # Use the appropriate registered earlier
    #     handler = self.handlers.get(EventType.PROJECT_CHANGE)
    #     if not handler:
    #         self._logger.error(
    #             "No handler for '%s' event type", EventType.PROJECT_CHANGE
    #         )
    #         return

    #     if not hasattr(handler, "run_now"):
    #         raise RuntimeError(
    #             f"Handler {handler!r} must implement `.run_now(payload)` for one-off mode"
    #         )
    #     handler.run_now(payload)

    #     # 2) After the step(s) emitted events, do a single consume pass
    #     # TODO: Put the imports at the top when this is stable
    #     import os
    #     from pathlib import Path

    #     from lib.ops.consumer import FileSpoolConsumer
    #     from lib.ops.sinks.couch import OpsWriter

    #     spool = Path(os.environ.get("YGG_EVENT_SPOOL", "/tmp/ygg_events"))
    #     FileSpoolConsumer(
    #         spool, OpsWriter(db_name=os.environ.get("OPS_DB", "yggdrasil_ops"))
    #     ).consume()

    # --------------------------------------------------------------------------
    # CLI Mode Methods (--plan-only, --run-once)
    # --------------------------------------------------------------------------

    def _check_plan_overwrite(
        self,
        plan_doc_id: str,
        force: bool,
    ) -> tuple[str, bool]:
        """
        Check if plan already exists and handle overwrite logic.

        Uses the actual plan_doc_id (from draft.plan.plan_id) to ensure
        the check matches the exact document that will be persisted.

        Args:
            plan_doc_id: The exact plan document ID to check
            force: Whether to force overwrite

        Returns:
            tuple: (plan_doc_id, should_continue)
                - should_continue=False means caller should abort
        """
        summary = self.plan_dbm.get_plan_summary(plan_doc_id)
        if summary is None:
            # No existing plan, proceed
            return plan_doc_id, True

        # Plan exists - display warning
        self._logger.warning(
            "\n╭─ Existing Plan Found ─────────────────────────────────────╮\n"
            "│ Plan ID:    %s\n"
            "│ Status:     %s\n"
            "│ Origin:     %s\n"
            "│ Updated:    %s\n"
            "│ Run Token:  %d (executed: %d)\n"
            "╰───────────────────────────────────────────────────────────╯",
            plan_doc_id,
            summary["status"],
            summary["execution_authority"],
            summary["updated_at"],
            summary["run_token"],
            summary["executed_run_token"],
        )

        if force:
            self._logger.info("--force specified; overwriting existing plan.")
            return plan_doc_id, True

        # Not forced - abort
        self._logger.error(
            "Plan '%s' already exists. Use --force to overwrite.",
            plan_doc_id,
        )
        return plan_doc_id, False

    def create_plan_from_doc(
        self,
        doc_id: str,
        *,
        force_overwrite: bool = False,
    ) -> list[str]:
        """
        Create and persist a plan from a project document (no execution).

        This is the --plan-only mode: creates a plan with execution_authority='daemon'
        for later approval by an external actor and execution by the daemon.

        Args:
            doc_id: Project document ID
            force_overwrite: If True, overwrite existing plan without prompting

        Returns:
            list[str]: Plan document IDs of all persisted plans. Empty list on failure.
        """
        self._logger.info("create_plan_from_doc: fetching project %s", doc_id)
        doc = self.pdm.fetch_document_by_id(doc_id)
        if not doc:
            self._logger.error("No project with ID %s", doc_id)
            return []

        handlers = self.subscriptions.get(EventType.PROJECT_CHANGE) or []
        if not handlers:
            self._logger.error(
                "No handlers registered for %s", EventType.PROJECT_CHANGE.name
            )
            return []

        persisted_ids: list[str] = []

        # Process with first matching handler (typical case: one handler per event;
        # multi-plan fan-out is within a single handler, not across handlers)
        for handler in handlers:
            try:
                # Derive scope
                if not (
                    hasattr(handler, "derive_scope") and callable(handler.derive_scope)
                ):
                    self._logger.error(
                        "Handler %s lacks derive_scope; skipping.",
                        handler.class_qualified_name(),
                    )
                    continue

                scope = handler.derive_scope(doc)
                realm_id = getattr(handler, "realm_id", "unknown_realm")

                # Build planning context
                reason = f"plan-only:{doc.get('project_id') or doc_id}"
                ctx = self._make_planning_ctx(handler, scope, doc=doc, reason=reason)
                payload = {
                    "doc": doc,
                    "reason": reason,
                    "planning_ctx": ctx,
                }

                # Generate plan drafts
                self._logger.info(
                    "Generating plan drafts via %s", handler.class_qualified_name()
                )
                drafts = handler.run_now(payload)

                # Check overwrite and persist each draft individually
                for draft in drafts:
                    _, should_continue = self._check_plan_overwrite(
                        draft.plan.plan_id, force_overwrite
                    )
                    if not should_continue:
                        continue  # Skip conflicting draft; check remaining

                    # Force draft status for plan-only mode
                    draft.auto_run = False

                    # Persist with daemon origin (for later daemon execution)
                    plan_doc_id = self._persist_plan_draft(
                        draft,
                        realm_id,
                        execution_authority="daemon",
                        execution_owner=None,
                    )
                    self._logger.info(
                        "✓ Plan '%s' created (status=draft, authority=daemon). "
                        "Awaiting external approval.",
                        plan_doc_id,
                    )
                    persisted_ids.append(plan_doc_id)

                break  # Only process first handler

            except Exception as e:
                self._logger.exception(
                    "Handler %s raised during create_plan_from_doc: %s",
                    handler.class_qualified_name(),
                    e,
                )

        return persisted_ids

    def run_once_with_watcher(
        self,
        doc_id: str,
        *,
        force_overwrite: bool = False,
        timeout_seconds: int = 1800,
    ) -> int:
        """
        Create and execute plan(s) via scoped PlanWatcher (--run-once mode).

        IMPORTANT: This method uses a single execution route via PlanWatcher
        regardless of auto_run status. The watcher is the sole path to execution.

        Creates plans for ALL matching handlers (not just the first).
        Plans are executed when they become eligible as observed from the DB.

        Args:
            doc_id: Project document ID
            force_overwrite: If True, overwrite existing plans without prompting
            timeout_seconds: Maximum seconds to wait for approval (default 1800)

        Returns:
            int: Exit code (0=success, 1=error, 130=interrupted)
        """
        from lib.ops.consumer import FileSpoolConsumer

        # Generate unique owner token for this entire session
        execution_owner = _generate_run_once_owner()
        self._logger.info(
            "run_once_with_watcher: session owner=%s, timeout=%ds",
            execution_owner,
            timeout_seconds,
        )

        # ─────────────────────────────────────────────────────────────────
        # Phase 1: Fetch document and create plans for ALL matching handlers
        # ─────────────────────────────────────────────────────────────────
        doc = self.pdm.fetch_document_by_id(doc_id)
        if not doc:
            self._logger.error("No project with ID %s", doc_id)
            return 1

        handlers = self.subscriptions.get(EventType.PROJECT_CHANGE) or []
        if not handlers:
            self._logger.error(
                "No handlers registered for %s", EventType.PROJECT_CHANGE.name
            )
            return 1

        # Create plans for ALL handlers (not just first)
        pending_plan_ids: list[str] = []

        for handler in handlers:
            plan_ids = self._create_run_once_plan_for_handler(
                handler=handler,
                doc=doc,
                doc_id=doc_id,
                execution_owner=execution_owner,
                force_overwrite=force_overwrite,
            )
            pending_plan_ids.extend(plan_ids)
            # Continue to next handler even if one fails

        if not pending_plan_ids:
            self._logger.error("Failed to create any plans for doc_id=%s", doc_id)
            return 1

        self._logger.info(
            "Created %d plan(s): %s",
            len(pending_plan_ids),
            ", ".join(pending_plan_ids),
        )

        # ─────────────────────────────────────────────────────────────────
        # Phase 2: Start scoped watcher and wait for all plans to complete
        # ─────────────────────────────────────────────────────────────────
        exit_code = asyncio.run(
            self._run_once_watcher_loop(
                pending_plan_ids=pending_plan_ids,
                execution_owner=execution_owner,
                timeout_seconds=timeout_seconds,
            )
        )

        # ─────────────────────────────────────────────────────────────────
        # Phase 3: Consume event spool
        # ─────────────────────────────────────────────────────────────────
        self._logger.info("Consuming event spool at %s", self.event_spool)
        FileSpoolConsumer(self.event_spool, self.storage.ops_snapshots).consume()

        return exit_code

    def _create_run_once_plan_for_handler(
        self,
        handler: BaseHandler,
        doc: dict[str, Any],
        doc_id: str,
        execution_owner: str,
        force_overwrite: bool,
    ) -> list[str]:
        """
        Create plans for one handler in run-once mode.

        IMPORTANT: Overwrite check uses the actual plan_doc_id from each draft,
        NOT a scope-derived ID, to avoid divergence from persisted _id.

        Args:
            handler: The handler to generate the plans
            doc: Source document
            doc_id: Document ID (fallback for reason string)
            execution_owner: Unique session token
            force_overwrite: Whether to overwrite existing plans

        Returns:
            list[str]: Plan document IDs of persisted plans. Empty list on failure.
        """
        try:
            if not (
                hasattr(handler, "derive_scope") and callable(handler.derive_scope)
            ):
                self._logger.error(
                    "Handler %s lacks derive_scope; skipping.",
                    handler.class_qualified_name(),
                )
                return []

            scope = handler.derive_scope(doc)
            realm_id = getattr(handler, "realm_id", "unknown_realm")

            # Build planning context
            reason = f"run-once:{doc.get('project_id') or doc_id}"
            ctx = self._make_planning_ctx(handler, scope, doc=doc, reason=reason)
            payload = {
                "doc": doc,
                "reason": reason,
                "planning_ctx": ctx,
            }

            # Generate plan drafts
            self._logger.info(
                "Generating plan drafts via %s", handler.class_qualified_name()
            )
            drafts = handler.run_now(payload)

            persisted_ids: list[str] = []
            for draft in drafts:
                # Check overwrite per draft (each has its own plan_doc_id)
                _, should_continue = self._check_plan_overwrite(
                    draft.plan.plan_id, force_overwrite
                )
                if not should_continue:
                    continue  # Skip conflicting draft; check remaining

                # Persist with run_once origin and our owner token
                # NOTE: auto_run determines status (approved/draft), but we do NOT
                # branch on it for execution - watcher handles all execution
                persisted_id = self._persist_plan_draft(
                    draft,
                    realm_id,
                    execution_authority="run_once",
                    execution_owner=execution_owner,
                )
                self._logger.info(
                    "Plan '%s' created (realm=%s, status=%s, owner=%s)",
                    persisted_id,
                    realm_id,
                    "approved" if draft.auto_run else "draft",
                    execution_owner,
                )
                persisted_ids.append(persisted_id)
            return persisted_ids

        except Exception as e:
            self._logger.exception(
                "Handler %s raised during plan creation: %s",
                handler.class_qualified_name(),
                e,
            )
            return []

    async def _run_once_watcher_loop(
        self,
        pending_plan_ids: list[str],
        execution_owner: str,
        timeout_seconds: int,
    ) -> int:
        """
        Run scoped watcher loop until all plans complete, timeout, or interrupt.

        IMPORTANT: This is the SOLE execution path for run-once mode.
        Whether auto_run=True or False, plans are executed here when eligible.

        Eligible plans are executed one at a time, in the order they became
        eligible, through the same execution coordinator as the daemon: each
        is admitted from a fresh read of its document under this session's
        authority and owner, and a finished request is recorded with the
        generation-safe finalizer. Execution runs in a worker thread, so the
        watcher keeps polling meanwhile.

        Completion is tracked in session-local memory only. The following
        behaviors are explicitly prohibited:
        - Writing completion markers back to the plan document
        - Modifying executed_run_token for plans not executed by this process
        - Updating plan status during ownership transfer detection

        A plan is completed, for this session, once its execution request is
        resolved, or once it turns out to be no longer this session's to
        execute. Every resolution other than a recorded success or such a
        transfer is an error, including a ``continue_independent`` execution
        that finished its request with failures, and a result that could not
        be recorded.

        Ctrl+C stops the session from starting further plans, queued ones
        included, and asks the plan in flight to stop starting further steps;
        its running step still finishes, and the plan stays eligible. An
        attempt cancelled without Ctrl+C, by a step raising its own
        ``CancelledError`` for instance, is an error like any other failure.

        The timeout bounds the wait for plans to become executable, such as
        waiting for approval. Once it expires, no newly eligible plan is taken
        on, but plans that are already eligible are still executed, whether
        running or queued behind a running one; none of those was waiting for
        approval. An execution in progress is never interrupted by it.

        Args:
            pending_plan_ids: List of plan doc IDs to track
            execution_owner: Our unique session token
            timeout_seconds: Max wait time

        Returns:
            int: Exit code (0=all completed successfully, 1=error/timeout,
                130=interrupted)
        """
        import signal

        claim = ExecutionClaim(authority="run_once", owner=execution_owner)

        # Track completion: plan_doc_id -> completed (True/False)
        # This is SESSION-LOCAL state only; never persisted to DB
        completion_status: dict[str, bool] = {pid: False for pid in pending_plan_ids}
        completion_event = asyncio.Event()
        # Plans reported eligible and waiting to be executed, in arrival order.
        # None tells the executing task to stop.
        requested: asyncio.Queue[str | None] = asyncio.Queue()
        queued: set[str] = set()
        executing_plan: str | None = None
        # Newly eligible plans are queued only while accepting; the timeout and
        # Ctrl+C both end that. Only Ctrl+C also abandons plans already queued.
        accepting = True
        interrupted = False
        error_occurred = False

        def on_plan_eligible(event: YggdrasilEvent) -> None:
            """
            Callback when watcher detects an eligible plan.

            Queues the plan for execution. Ownership and eligibility are
            checked again, from the plan document, when it is executed.
            """
            # Take on no newly eligible plan once interrupted or timed out;
            # it is left pending in the DB for manual handling.
            if not accepting:
                return

            payload = event.payload or {}
            plan_doc_id = payload.get("plan_doc_id")

            # IMPORTANT: Ignore plans not created in THIS session.
            # This prevents "owner collision" side effects (unlikely with UUID, but safe).
            if plan_doc_id not in completion_status:
                self._logger.debug(
                    "Ignoring event for plan not in this session: %s", plan_doc_id
                )
                return

            if completion_status[plan_doc_id] or plan_doc_id in queued:
                # Already completed (locally), or already waiting its turn
                return

            queued.add(plan_doc_id)
            requested.put_nowait(plan_doc_id)

        def record_result(result: ExecutionResult) -> None:
            """Update session-local completion from how a request was resolved."""
            nonlocal error_occurred
            plan_doc_id = result.plan_doc_id

            if result.status in (
                ExecutionStatus.NOT_ELIGIBLE,
                ExecutionStatus.DUPLICATE,
            ):
                self._logger.debug(
                    "Plan '%s' no longer eligible; will retry on next poll",
                    plan_doc_id,
                )
                return
            if result.status is ExecutionStatus.CANCELLED and interrupted:
                # Stopped by Ctrl+C before it finished: left eligible in the DB.
                return

            completion_status[plan_doc_id] = True
            if result.status is ExecutionStatus.NOT_AUTHORIZED:
                # An external actor transferred execution ownership. Do NOT
                # write anything to DB; just mark locally completed.
                self._logger.info(
                    "Plan '%s' is no longer this session's to execute; marking "
                    "completed (no action)",
                    plan_doc_id,
                )
            elif not result.succeeded:
                error_occurred = True
            _check_all_completed()

        def _check_all_completed() -> None:
            """Signal completion_event if all plans are done."""
            if all(completion_status.values()):
                completion_event.set()

        async def execute_requested() -> None:
            """Execute queued plans, one at a time, until told to stop."""
            nonlocal executing_plan
            while (plan_doc_id := await requested.get()) is not None:
                queued.discard(plan_doc_id)
                if interrupted or completion_status[plan_doc_id]:
                    continue
                self._logger.info("Executing plan '%s' via Engine...", plan_doc_id)
                executing_plan = plan_doc_id
                try:
                    result = await self.plan_executions.execute(plan_doc_id, claim)
                finally:
                    executing_plan = None
                record_result(result)

        # Create scoped watcher (filters by execution_owner only)
        scoped_watcher = PlanWatcher(
            on_event=on_plan_eligible,
            poll_interval_sec=2.0,  # Responsive for interactive use
            plan_store=self.storage.plans,
            change_source=self.storage.plan_changes,
            checkpoint_store=self.storage.checkpoints,
            execution_owner_filter=execution_owner,
            # NOTE: No execution_authority_filter; authority is checked on execution
        )

        # Handle Ctrl+C
        def handle_interrupt(signum: int, frame: Any) -> None:
            nonlocal interrupted, accepting
            interrupted = True
            accepting = False
            # Thread-safe: only sets each attempt's cancellation event.
            for plan_doc_id in pending_plan_ids:
                self.plan_executions.request_cancellation(plan_doc_id)
            completion_event.set()

        original_handler = signal.signal(signal.SIGINT, handle_interrupt)
        executing = asyncio.create_task(execute_requested())

        try:
            # Recovery pass before the live watcher starts: plans were saved
            # in Phase 1, so a fresh change cursor (resolved at "now") would
            # never observe them. Explicit IDs avoid a whole-database scan;
            # the owner filter remains a defense-in-depth boundary. Kept
            # inside the try (after the interrupt handler is installed) so
            # Ctrl+C during recovered execution still routes to the
            # controlled shutdown + spool drain below.
            await scoped_watcher.recover_pending_plans(plan_ids=pending_plan_ids)

            # Start watcher task
            watcher_task = asyncio.create_task(scoped_watcher.start())

            # Wait with timeout
            try:
                await asyncio.wait_for(
                    completion_event.wait(),
                    timeout=float(timeout_seconds),
                )
            except TimeoutError:
                accepting = False
                ready = [
                    pid
                    for pid in pending_plan_ids
                    if pid == executing_plan or pid in queued
                ]
                if ready:
                    self._logger.warning(
                        "Timeout after %ds waiting for plans to become "
                        "executable; executing those already eligible before "
                        "stopping: %s",
                        timeout_seconds,
                        ", ".join(ready),
                    )

            # Stop watcher
            await scoped_watcher.stop()
            watcher_task.cancel()
            try:
                await watcher_task
            except asyncio.CancelledError:
                pass

        finally:
            # Take on nothing new. Plans already queued are still executed,
            # unless interrupted, in which case they are left pending in the DB
            # for manual handling. The plan in flight, if any, ends in full, or
            # after its running step when interrupted.
            accepting = False
            requested.put_nowait(None)
            await executing
            signal.signal(signal.SIGINT, original_handler)

        if interrupted:
            pending = [pid for pid, done in completion_status.items() if not done]
            self._logger.info(
                "\nInterrupted. Plan(s) left in current state: %s",
                ", ".join(pending) if pending else "(all completed)",
            )
            return 130  # Standard SIGINT exit code

        pending = [pid for pid, done in completion_status.items() if not done]
        if pending:
            self._logger.error(
                "Timeout after %ds waiting for plans: %s\n"
                "Plans left in DB for manual handling.",
                timeout_seconds,
                ", ".join(pending),
            )
            return 1

        return 1 if error_occurred else 0

    def handle_event(self, event: YggdrasilEvent) -> None:
        """
        Watchers call this to deliver events.

        Routing logic:
            1. If payload contains 'realm_id', filter to that realm's handlers
            2. If payload contains 'target_handlers', filter to those handler_ids
            3. Otherwise, broadcast to all handlers subscribed to event_type

        After filtering, each handler generates a PlanDraft asynchronously and
        persists it. PlanWatcher later observes approved/runnable plans and
        triggers execution.
        """
        self._logger.info(
            "Received event '%s' from '%s'", event.event_type, event.source
        )

        payload = dict(event.payload) if isinstance(event.payload, dict) else {}

        # Extract routing hints (pop so they don't leak into handler payload)
        route_realm_id: str | None = payload.pop("realm_id", None)
        route_target_handlers: list[str] | None = payload.pop("target_handlers", None)

        # Get all handlers for this event type
        handlers = list(self.subscriptions.get(event.event_type) or [])

        if not handlers:
            self._logger.warning(
                "No subscribers registered for event_type '%s'", event.event_type
            )
            return

        # Filter by realm_id if specified
        if route_realm_id:
            handlers = [
                h for h in handlers if getattr(h, "realm_id", None) == route_realm_id
            ]
            if not handlers:
                self._logger.warning(
                    "No handlers found for realm '%s' and event_type '%s'",
                    route_realm_id,
                    event.event_type,
                )
                return

        # Filter by target_handlers if specified
        if route_target_handlers:
            handlers = [
                h
                for h in handlers
                if getattr(h, "handler_id", None) in route_target_handlers
            ]
            if not handlers:
                self._logger.warning(
                    "No handlers found matching target_handlers=%s",
                    route_target_handlers,
                )
                return

        # Dispatch to filtered handlers
        for handler in handlers:
            try:
                scope = payload.get("scope")
                doc = payload.get("doc", {})

                # If scope not provided by watcher, let realm derive it from doc
                if scope is None:
                    if (
                        doc
                        and hasattr(handler, "derive_scope")
                        and callable(handler.derive_scope)
                    ):
                        scope = handler.derive_scope(doc)
                    else:
                        self._logger.error(
                            "No 'scope' in payload and handler %s has no derive_scope; skipping.",
                            handler.class_qualified_name(),
                        )
                        continue

                reason = (
                    payload.get("reason")
                    or f"{event.event_type.name} from {event.source}"
                )
                handler_payload = dict(payload)
                handler_payload["scope"] = scope
                handler_payload["reason"] = reason
                handler_payload["doc"] = doc
                handler_payload["planning_ctx"] = self._make_planning_ctx(
                    handler, scope, doc=doc, reason=reason
                )

                # Schedule async plan generation (PlanWatcher handles execution)
                self._logger.debug(
                    "Scheduling plan generation for %s via %s",
                    event.event_type.name,
                    handler.class_qualified_name(),
                )
                asyncio.create_task(
                    self._generate_and_persist_plan(handler, handler_payload)
                )

            except Exception as exc:
                self._logger.error(
                    "Error handling '%s' with handler '%s': %s",
                    event.event_type.name,
                    handler.class_qualified_name(),
                    exc,
                    exc_info=True,
                )

    async def _generate_and_persist_plan(
        self, handler: BaseHandler, payload: dict[str, Any]
    ) -> None:
        """
        Generate and persist a plan from a handler (daemon mode).

        In daemon mode, this method ONLY generates and persists the plan.
        Execution is handled exclusively by PlanWatcher, which detects
        eligible plans (approved + run_token > executed_run_token) and
        hands it to the execution coordinator via _handle_plan_execution_event().

        This separation ensures:
        - Single execution path (no double-execution bugs)
        - Consistent execution tracking via executed_run_token
        - Proper support for approval workflows

        Args:
            handler: The handler that will generate the plan
            payload: Event payload with planning_ctx
        """
        try:
            # Step 1: Generate plan drafts
            self._logger.info(
                "Generating plan drafts via %s", handler.class_qualified_name()
            )
            drafts = await handler.generate_plan_drafts(payload)

            # Step 2: Persist each plan to database
            # PlanWatcher will detect eligible plans and trigger execution
            realm_id = getattr(handler, "realm_id", "unknown_realm")
            for draft in drafts:
                plan_doc_id = self._persist_plan_draft(draft, realm_id)

                if draft.auto_run:
                    self._logger.info(
                        "Plan '%s' persisted (status=approved, auto_run=True). ",
                        plan_doc_id,
                    )
                else:
                    self._logger.info(
                        "Plan '%s' persisted (status=draft). "
                        "Awaiting for approval (approvals_required=%s).",
                        plan_doc_id,
                        draft.approvals_required,
                    )

            # NOTE: No inline execution here. PlanWatcher handles all execution
            # in daemon mode, ensuring single execution path and proper tracking.

        except Exception as exc:
            self._logger.exception(
                "Failed to generate/persist plan via %s: %s",
                handler.class_qualified_name(),
                exc,
            )

    # ---------------------------------
    # CLI or Semi-Automatic calls
    # ---------------------------------
    def process_cli_command(self, command_name: str, **kwargs) -> None:
        """
        Example method for manual (CLI-based) triggers that bypass watchers.
        E.g. 'ygg-mule reprocess-flowcell <id>' -> calls this method.
        """
        self._logger.info(f"Processing CLI command '{command_name}' with args={kwargs}")
        # Potentially route or handle an event, or do domain logic directly.
        # E.g. self.handle_event(YggdrasilEvent("manual_trigger", {"flowcell_id": kwargs["flowcell_id"]}, "CLI"))
        # Or run HPC submission logic, etc.
