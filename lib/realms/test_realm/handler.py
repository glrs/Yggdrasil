"""
Test realm handler for processing test scenario documents.

TestRealmHandler responds to COUCHDB_DOC_CHANGED events for scenario documents,
generating execution plans from scenario documents stored in the yggdrasil database.
"""

import logging
from typing import Any, ClassVar, cast

from lib.core_utils.event_types import EventType
from lib.core_utils.logging_utils import custom_logger
from lib.realms.test_realm.recipes import (
    RECIPES,
    data_fetch_plan_steps,
    default_failure_policy,
    get_recipe,
    metadata_harvest_steps,
)
from yggdrasil.flow.base_handler import BaseHandler
from yggdrasil.flow.model import Plan, validate_failure_policy
from yggdrasil.flow.planner import PlanDraft, PlanningContext


class TestRealmHandler(BaseHandler):
    """
    Handler for test scenario documents.

    Processes documents with type="ygg_test_scenario" from the yggdrasil database,
    generating plans based on the specified recipe.

    Document schema:
        {
            "_id": "test_scenario:<unique_id>",
            "type": "ygg_test_scenario",
            "recipe": "happy_path",  # One of RECIPES keys
            "auto_run": true,          # Optional, default True
            "failure_policy": "fail_fast",  # Optional, default: the
                                       # recipe's (default_failure_policy)
            "overrides": {             # Optional step param overrides
                "step_id": {"param": "value"}
            }
        }

    Note: Uses generic COUCHDB_DOC_CHANGED event type. Document filtering
    is handled by the WatchSpec's filter_expr in the realm descriptor.
    """

    event_type: ClassVar[EventType] = EventType.COUCHDB_DOC_CHANGED
    handler_id: ClassVar[str] = "test_scenario_handler"

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")

    def derive_scope(self, doc: dict[str, Any]) -> dict[str, Any]:
        """
        Extract scope from test scenario document.

        Args:
            doc: Scenario document from yggdrasil DB

        Returns:
            Scope dict with kind='test_scenario' and id from document
        """
        # Use _id or fall back to a generated ID
        doc_id = doc.get("_id", doc.get("scenario_id", "unknown"))
        return {"kind": "test_scenario", "id": doc_id}

    def _parse_custom_steps(self, steps_config: list[dict[str, Any]]) -> list:
        """
        Parse custom steps from scenario document.

        Args:
            steps_config: List of step definitions from document

        Returns:
            List of StepSpec objects

        Example step config:
            {
                "step_id": "my_step",
                "name": "My Step",
                "fn_name": "step_echo",
                "params": {"message": "Hello"},
                "deps": ["previous_step"]
            }
        """
        from yggdrasil.flow.model import StepSpec

        _FN_REF_PREFIX = "lib.realms.test_realm.steps"
        steps = []

        for step_cfg in steps_config:
            if not isinstance(step_cfg, dict):
                raise ValueError(f"Step config must be a dict, got: {type(step_cfg)}")

            # Required fields
            step_id = step_cfg.get("step_id")
            fn_name = step_cfg.get("fn_name")

            if not step_id or not fn_name:
                raise ValueError(
                    f"Step config must have 'step_id' and 'fn_name': {step_cfg}"
                )

            # Optional fields - use cast for type safety
            name = cast(str, step_cfg.get("name", step_id))
            params = cast(dict[str, Any], step_cfg.get("params", {}))
            deps = cast(list[str], step_cfg.get("deps", []))

            step = StepSpec(
                step_id=step_id,
                name=name,
                fn_ref=f"{_FN_REF_PREFIX}.{fn_name}",
                params=params,
                deps=deps,
            )
            steps.append(step)

        return steps

    async def _do_plan_time_fetch(self, ctx: PlanningContext) -> dict[str, Any]:
        """
        Fetch the data-access reference document from CouchDB during planning.

        Returns a **structured dict** that gets baked into the generated plan's
        step params as ``ref_doc``.  Using a dict (instead of a formatted string)
        keeps the data queryable in the persisted plan record and demonstrates
        the correct real-realm pattern.

        Return shapes:
            Success:  ``{"doc_id": "...", "message": "...", "value": 13, "missing": False}``
            Missing:  ``{"doc_id": "...", "missing": True}``
            Error:    ``{"doc_id": "...", "error": "...", "error_type": "..."}``

        On any error the method returns a dict with error details rather than
        raising, so the plan is still created (with the error visible in params).
        """
        from yggdrasil.flow.data_access import DataAccessError

        doc_id = "data_access_test:reference_doc"
        try:
            client = ctx.data.connection("yggdrasil_db")
            doc = await client.get(doc_id)
            if doc is None:
                return {"doc_id": doc_id, "missing": True}
            return {
                "doc_id": doc_id,
                "message": doc.get("message", "<no message field>"),
                "value": doc.get("value", None),
                "missing": False,
            }
        except DataAccessError as exc:
            return {
                "doc_id": doc_id,
                "error": str(exc),
                "error_type": type(exc).__name__,
            }
        except Exception as exc:
            return {
                "doc_id": doc_id,
                "error": str(exc),
                "error_type": type(exc).__name__,
            }

    async def generate_plan_drafts(self, payload: dict[str, Any]) -> list[PlanDraft]:
        """
        Generate a PlanDraft from the scenario document.

        Supports four modes (checked in order):

        1. **data_fetch_plan**: Async-fetches a CouchDB document *during
           planning* and bakes the result as a structured dict into step
           params (``ref_doc``).  The persisted plan record is proof the
           fetch happened at plan-generation time.
        2. **metadata_harvest**: Extracts domain fields from the scenario
           document itself (``input_path``, ``mode``, ``priority``,
           ``sample_id``, ``flags``) and bakes them as a structured dict
           into step params (``scenario``).  Demonstrates the real-realm
           pattern of mapping triggering-doc fields into plan params.
        3. **Recipe-based**: Provide a ``recipe`` field (e.g. ``happy_path``).
        4. **Custom steps**: Provide a ``steps`` array directly.

        In every mode, the plan runs under the document's ``failure_policy``,
        or the recipe's default policy when the document names none
        (``fail_fast`` for custom steps and the planning-time modes).

        Args:
            payload: Event payload containing:
                - doc: The scenario document
                - planning_ctx: PlanningContext from YggdrasilCore

        Returns:
            list[PlanDraft] with one draft containing plan, auto_run flag, and notes

        Raises:
            ValueError: If document type is not 'ygg_test_scenario', or its
                failure_policy is not a known policy
        """

        doc = payload.get("doc", {})
        ctx: PlanningContext = payload["planning_ctx"]

        # Guard: Verify document type (defense-in-depth; WatchSpec filters too)
        doc_type = doc.get("type")
        if doc_type != "ygg_test_scenario":
            raise ValueError(
                f"Expected doc.type='ygg_test_scenario', got '{doc_type}'. "
                f"Document _id: {doc.get('_id')}"
            )

        recipe_name = doc.get("recipe")
        custom_steps = doc.get("steps")

        # An explicit policy is never downgraded: an unknown one is rejected.
        failure_policy = doc.get("failure_policy", default_failure_policy(recipe_name))
        validate_failure_policy(failure_policy)

        # --- Mode 1: Planning-time data fetch (async, baked as structured dict) ---
        if recipe_name == "data_fetch_plan":
            self._logger.info(
                "Generating data_fetch_plan for scenario '%s': "
                "fetching reference doc from CouchDB during planning",
                doc.get("_id"),
            )
            ref_dict = await self._do_plan_time_fetch(ctx)
            steps = data_fetch_plan_steps(ref_dict=ref_dict)
            notes = "Planning-time data fetch: reference doc baked into step params as structured dict"
            preview = {
                "recipe": "data_fetch_plan",
                "step_count": len(steps),
                "step_names": [s.name for s in steps],
                "planning_time_fetch": True,
                "ref_doc": ref_dict,
            }

        # --- Mode 2: Metadata harvest (domain fields baked as structured dict) ---
        elif recipe_name == "metadata_harvest":
            self._logger.info(
                "Generating metadata_harvest plan for scenario '%s': "
                "harvesting domain fields from scenario document",
                doc.get("_id"),
            )
            scenario = {
                "input_path": doc.get("input_path", ""),
                "mode": doc.get("mode", "default"),
                "priority": doc.get("priority", 0),
                "sample_id": doc.get("sample_id", ""),
                "flags": doc.get("flags", []),
            }
            steps = metadata_harvest_steps(scenario=scenario)
            notes = "Metadata harvest: domain fields from triggering doc baked into step params"
            preview = {
                "recipe": "metadata_harvest",
                "step_count": len(steps),
                "step_names": [s.name for s in steps],
                "scenario": scenario,
            }

        # --- Mode 3: Standard recipe-based ---
        elif recipe_name:
            if recipe_name not in RECIPES:
                raise ValueError(
                    f"Unknown recipe '{recipe_name}'. "
                    f"Available: {list(RECIPES.keys())}"
                )

            overrides = doc.get("overrides", {})

            self._logger.info(
                "Generating plan from recipe '%s' for scenario '%s'",
                recipe_name,
                doc.get("_id"),
            )
            recipe_fn = get_recipe(recipe_name)
            steps = recipe_fn(overrides=overrides)
            notes = f"Test scenario using recipe '{recipe_name}'"
            preview = {
                "recipe": recipe_name,
                "step_count": len(steps),
                "step_names": [s.name for s in steps],
            }

        # --- Mode 4: Custom steps ---
        elif custom_steps:
            self._logger.info(
                "Generating plan from custom steps for scenario '%s'",
                doc.get("_id"),
            )
            steps = self._parse_custom_steps(custom_steps)
            notes = f"Test scenario with {len(steps)} custom step(s)"
            preview = {
                "recipe": None,
                "step_count": len(steps),
                "step_names": [s.name for s in steps],
            }

        else:
            raise ValueError(
                f"Scenario document must have either 'recipe' or 'steps' field: {doc.get('_id')}"
            )

        preview["failure_policy"] = failure_policy

        # Build Plan
        plan = Plan(
            plan_id=f"test_realm:{ctx.scope['id']}",
            realm=self.realm_id or "test_realm",
            scope=ctx.scope,
            steps=steps,
            failure_policy=failure_policy,
        )

        auto_run = doc.get("auto_run", True)

        return [
            PlanDraft(
                plan=plan,
                auto_run=auto_run,
                approvals_required=[],
                notes=notes,
                preview=preview,
            )
        ]
