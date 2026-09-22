"""
Regression and functional tests for lib/realms/test_realm/steps.py
and lib/realms/test_realm/handler.py.

- Decorator tests: every step is a proper @step-decorated callable.
- Functional tests: new write/denial steps behave correctly with mocked DataAccess.
- Handler tests: _do_plan_time_fetch uses connection() (not couchdb()) and awaits get(),
  and each scenario's plan gets the failure policy it names, else its recipe's.
- Recipe tests: the two branching recipes differ only in the metadata prerequisite.
"""

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock

from yggdrasil.flow.utils.callable_ref import resolve_callable

_FN_REF_PREFIX = "lib.realms.test_realm.steps"

# All fn_ref names referenced from test_realm recipes / custom step parsing.
_ALL_STEP_NAMES = [
    "step_echo",
    "step_sleep",
    "step_fail",
    "step_write_file",
    "step_random_fail",
    "step_fetch_from_db",
    "step_expect_denied",
    "step_write_to_db",
    "step_write_to_db_no_id",
    "step_expect_read_denied",
    "step_exercise_all_fetch_methods",
    "step_verify_limit_clamping",
    "step_emit_metadata",
]


class TestTestRealmStepsAreDecorated(unittest.TestCase):
    """
    Ensure every test realm step resolved via fn_ref carries the _step_name
    attribute that the @step decorator sets. If a step loses its decorator,
    hasattr(fn, '_step_name') will be False and this test will catch it.
    """

    def _fn_ref(self, name: str) -> str:
        return f"{_FN_REF_PREFIX}.{name}"

    def test_all_steps_are_callable(self):
        """resolve_callable must return a callable for every registered step."""
        for name in _ALL_STEP_NAMES:
            with self.subTest(step=name):
                fn = resolve_callable(self._fn_ref(name))
                self.assertTrue(
                    callable(fn),
                    f"resolve_callable('{name}') did not return a callable",
                )

    def test_all_steps_have_step_name_attribute(self):
        """
        Every step must carry _step_name — the attribute set by @step.
        If this fails, the function is a plain def that will never emit
        step.started / step.succeeded / step.failed.
        """
        for name in _ALL_STEP_NAMES:
            with self.subTest(step=name):
                fn = resolve_callable(self._fn_ref(name))
                self.assertTrue(
                    hasattr(fn, "_step_name"),
                    f"Step '{name}' is missing _step_name — did you forget @step?",
                )

    def test_step_name_attribute_matches_function_name(self):
        """_step_name should match the bare function name (decorator default)."""
        for name in _ALL_STEP_NAMES:
            with self.subTest(step=name):
                fn = resolve_callable(self._fn_ref(name))
                if hasattr(fn, "_step_name"):
                    self.assertEqual(
                        fn._step_name,
                        name,
                        f"Step '{name}' has _step_name={fn._step_name!r}, expected {name!r}",
                    )


# ---------------------------------------------------------------------------
# Functional: step_write_to_db
# ---------------------------------------------------------------------------


class TestStepWriteToDb(unittest.TestCase):
    """Functional tests for step_write_to_db."""

    def setUp(self):
        from lib.realms.test_realm.steps import step_write_to_db
        from yggdrasil.flow.data_access.models import DataAccessWriteResult

        self.step_fn = step_write_to_db
        self.WriteResult = DataAccessWriteResult

    def _make_write_result(self, status="created", old_rev=None, new_rev="1-abc"):
        return self.WriteResult(
            backend="couchdb",
            connection_name="test_realm_write_db",
            resource="yggdrasil",
            operation="upsert",
            identity="doc_id",
            doc_id="data_access_test:write_result",
            status=status,
            old_rev=old_rev,
            new_rev=new_rev,
        )

    def _make_ctx(self, write_result):
        ctx = MagicMock()
        ctx.data.connection.return_value.save.return_value = write_result
        return ctx

    def test_save_called_with_clean_body(self):
        """Body passed to save() must not contain _id or _rev."""
        result = self._make_write_result()
        ctx = self._make_ctx(result)

        self.step_fn(ctx)

        save_call = ctx.data.connection.return_value.save.call_args
        body = save_call[0][0]  # first positional argument
        self.assertNotIn("_id", body)
        self.assertNotIn("_rev", body)

    def test_save_called_with_correct_doc_id_and_mode(self):
        result = self._make_write_result()
        ctx = self._make_ctx(result)

        self.step_fn(ctx, doc_id="custom:doc", mode="create")

        save_call = ctx.data.connection.return_value.save.call_args
        self.assertEqual(save_call[1]["doc_id"], "custom:doc")
        self.assertEqual(save_call[1]["mode"], "create")

    def test_metrics_include_write_result_fields_on_create(self):
        result = self._make_write_result(
            status="created", old_rev=None, new_rev="1-abc"
        )
        ctx = self._make_ctx(result)

        step_result = self.step_fn(ctx)

        self.assertEqual(step_result.metrics["write_status"], "created")
        self.assertEqual(step_result.metrics["doc_id"], "data_access_test:write_result")
        self.assertIsNone(step_result.metrics["old_rev"])
        self.assertEqual(step_result.metrics["new_rev"], "1-abc")

    def test_metrics_include_write_result_fields_on_update(self):
        result = self._make_write_result(
            status="updated", old_rev="1-abc", new_rev="2-def"
        )
        ctx = self._make_ctx(result)

        step_result = self.step_fn(ctx)

        self.assertEqual(step_result.metrics["write_status"], "updated")
        self.assertEqual(step_result.metrics["old_rev"], "1-abc")
        self.assertEqual(step_result.metrics["new_rev"], "2-def")

    def test_raises_if_ctx_data_is_none(self):
        ctx = MagicMock()
        ctx.data = None
        with self.assertRaises(RuntimeError):
            self.step_fn(ctx)


# ---------------------------------------------------------------------------
# Functional: step_write_to_db_no_id
# ---------------------------------------------------------------------------


class TestStepWriteToDbNoId(unittest.TestCase):
    """Functional tests for step_write_to_db_no_id."""

    def setUp(self):
        from lib.realms.test_realm.steps import step_write_to_db_no_id
        from yggdrasil.flow.data_access.models import DataAccessWriteResult

        self.step_fn = step_write_to_db_no_id
        self.WriteResult = DataAccessWriteResult

    def _make_write_result(
        self,
        status="created",
        doc_id="auto-generated-id-abc",
        old_rev=None,
        new_rev="1-xyz",
    ):
        return self.WriteResult(
            backend="couchdb",
            connection_name="test_realm_write_db",
            resource="yggdrasil",
            operation="upsert",
            identity="selector",
            doc_id=doc_id,
            status=status,
            old_rev=old_rev,
            new_rev=new_rev,
        )

    def _make_ctx(self, write_result):
        ctx = MagicMock()
        ctx.data.connection.return_value.save.return_value = write_result
        return ctx

    def test_save_called_with_selector_mode_not_doc_id(self):
        """save() must use selector=... and mode=..., not doc_id=..."""
        ctx = self._make_ctx(self._make_write_result())
        self.step_fn(ctx, mode="upsert")
        save_call = ctx.data.connection.return_value.save.call_args
        self.assertIn("selector", save_call[1])
        self.assertEqual(save_call[1]["mode"], "upsert")
        self.assertNotIn("doc_id", save_call[1])

    def test_save_called_with_clean_body(self):
        """Body passed to save() must not contain _id or _rev."""
        ctx = self._make_ctx(self._make_write_result())
        self.step_fn(ctx)
        save_call = ctx.data.connection.return_value.save.call_args
        body = save_call[0][0]
        self.assertNotIn("_id", body)
        self.assertNotIn("_rev", body)

    def test_default_selector_applied_when_none_provided(self):
        """When selector param is None, a non-empty default selector must be used."""
        ctx = self._make_ctx(self._make_write_result())
        self.step_fn(ctx)
        save_call = ctx.data.connection.return_value.save.call_args
        sel = save_call[1]["selector"]
        self.assertIsInstance(sel, dict)
        self.assertTrue(len(sel) > 0)

    def test_custom_selector_overrides_default(self):
        """Explicit selector param must be forwarded to save()."""
        ctx = self._make_ctx(self._make_write_result())
        custom = {"type": "my_type", "batch_id": "b-42"}
        self.step_fn(ctx, selector=custom)
        save_call = ctx.data.connection.return_value.save.call_args
        self.assertEqual(save_call[1]["selector"], custom)

    def test_metrics_include_couchdb_generated_doc_id_and_identity(self):
        """doc_id, identity, and operation in metrics must reflect the write result."""
        result = self._make_write_result(doc_id="couchdb-abc123")
        ctx = self._make_ctx(result)
        step_result = self.step_fn(ctx)
        self.assertEqual(step_result.metrics["doc_id"], "couchdb-abc123")
        self.assertEqual(step_result.metrics["write_status"], "created")
        self.assertEqual(step_result.metrics["identity"], "selector")
        self.assertEqual(step_result.metrics["operation"], "upsert")

    def test_raises_if_ctx_data_is_none(self):
        ctx = MagicMock()
        ctx.data = None
        with self.assertRaises(RuntimeError):
            self.step_fn(ctx)


# ---------------------------------------------------------------------------
# Functional: step_expect_read_denied
# ---------------------------------------------------------------------------


class TestStepExpectReadDenied(unittest.TestCase):
    """Functional tests for step_expect_read_denied."""

    def setUp(self):
        from lib.realms.test_realm.steps import step_expect_read_denied
        from yggdrasil.flow.data_access import DataAccessDeniedError

        self.step_fn = step_expect_read_denied
        self.DeniedError = DataAccessDeniedError

    def test_succeeds_when_get_is_denied(self):
        """Step must return StepResult when get() raises DataAccessDeniedError."""
        ctx = MagicMock()
        ctx.data.connection.return_value.get.side_effect = self.DeniedError(
            "realm has no read permission"
        )

        result = self.step_fn(ctx)

        self.assertTrue(result.metrics["read_correctly_denied"])
        self.assertIn("denial_reason", result.metrics)

    def test_metrics_contain_connection_name(self):
        ctx = MagicMock()
        ctx.data.connection.return_value.get.side_effect = self.DeniedError("no read")

        result = self.step_fn(ctx, connection="test_realm_write_only_db")

        self.assertEqual(result.metrics["connection"], "test_realm_write_only_db")

    def test_fails_hard_if_read_succeeds(self):
        """Step must raise RuntimeError when get() unexpectedly returns a doc."""
        ctx = MagicMock()
        ctx.data.connection.return_value.get.return_value = {"_id": "some_doc"}

        with self.assertRaises(RuntimeError) as cm:
            self.step_fn(ctx)

        self.assertIn("but read succeeded", str(cm.exception))

    def test_connection_is_called_before_get(self):
        """connection() must be called first (write permission allows it)."""
        ctx = MagicMock()
        ctx.data.connection.return_value.get.side_effect = self.DeniedError("no read")

        self.step_fn(ctx, connection="test_realm_write_only_db")

        ctx.data.connection.assert_called_once_with("test_realm_write_only_db")

    def test_raises_if_ctx_data_is_none(self):
        ctx = MagicMock()
        ctx.data = None
        with self.assertRaises(RuntimeError):
            self.step_fn(ctx)


# ---------------------------------------------------------------------------
# Handler: _do_plan_time_fetch uses connection() and awaits get()
# ---------------------------------------------------------------------------


class TestHandlerPlanTimeFetch(unittest.IsolatedAsyncioTestCase):
    """Tests for TestRealmHandler._do_plan_time_fetch."""

    def setUp(self):
        from lib.realms.test_realm.handler import TestRealmHandler

        self.handler = TestRealmHandler()

    async def test_uses_connection_not_couchdb(self):
        """_do_plan_time_fetch must call ctx.data.connection(), not ctx.data.couchdb()."""
        ctx = MagicMock()
        ctx.data.connection.return_value.get = AsyncMock(return_value=None)

        await self.handler._do_plan_time_fetch(ctx)

        ctx.data.connection.assert_called_once_with("yggdrasil_db")
        ctx.data.couchdb.assert_not_called()

    async def test_returns_missing_true_when_doc_absent(self):
        """get() returning None must produce {"doc_id": ..., "missing": True}."""
        ctx = MagicMock()
        ctx.data.connection.return_value.get = AsyncMock(return_value=None)

        result = await self.handler._do_plan_time_fetch(ctx)

        self.assertTrue(result.get("missing"))
        self.assertNotIn("error", result)

    async def test_returns_doc_fields_when_doc_present(self):
        """get() returning a doc must produce structured dict with message and value."""
        ctx = MagicMock()
        ctx.data.connection.return_value.get = AsyncMock(
            return_value={
                "_id": "data_access_test:reference_doc",
                "message": "hello",
                "value": 42,
            }
        )

        result = await self.handler._do_plan_time_fetch(ctx)

        self.assertEqual(result["message"], "hello")
        self.assertEqual(result["value"], 42)
        self.assertFalse(result.get("missing"))
        self.assertNotIn("error", result)

    async def test_get_is_awaited(self):
        """get() must be awaited (planning client returns a coroutine)."""
        ctx = MagicMock()
        async_get = AsyncMock(return_value={"_id": "x", "message": "m", "value": 1})
        ctx.data.connection.return_value.get = async_get

        await self.handler._do_plan_time_fetch(ctx)

        async_get.assert_awaited_once()

    async def test_returns_error_dict_on_data_access_error(self):
        """DataAccessError during get() must be caught and returned as an error dict."""
        from yggdrasil.flow.data_access import DataAccessDeniedError

        ctx = MagicMock()
        ctx.data.connection.return_value.get = AsyncMock(
            side_effect=DataAccessDeniedError("no read permission")
        )

        result = await self.handler._do_plan_time_fetch(ctx)

        self.assertIn("error", result)
        self.assertEqual(result["error_type"], "DataAccessDeniedError")
        self.assertNotIn("missing", result)


# ---------------------------------------------------------------------------
# data_fetch_plan_steps: internal helper used by handler Mode 1
# ---------------------------------------------------------------------------


class TestDataFetchPlanSteps(unittest.TestCase):
    """Unit tests for data_fetch_plan_steps (internal helper called by handler Mode 1).

    Covers the three ref_dict shapes the handler can produce:
    success, doc-absent (missing), and DataAccessError.
    """

    def setUp(self):
        from lib.realms.test_realm.recipes import data_fetch_plan_steps

        self.helper = data_fetch_plan_steps

    def test_returns_two_steps(self):
        steps = self.helper(
            {"doc_id": "x", "missing": False, "message": "hi", "value": 1}
        )
        self.assertEqual(len(steps), 2)
        self.assertEqual(steps[0].step_id, "echo_fetched")
        self.assertEqual(steps[1].step_id, "echo_confirm")

    def test_echo_fetched_uses_step_emit_metadata(self):
        steps = self.helper(
            {"doc_id": "x", "missing": False, "message": "hi", "value": 1}
        )
        self.assertIn("step_emit_metadata", steps[0].fn_ref)

    def test_success_case_bakes_ref_doc_into_params(self):
        ref_dict = {"doc_id": "x", "message": "hi", "value": 7, "missing": False}
        steps = self.helper(ref_dict)
        self.assertEqual(steps[0].params["ref_doc"], ref_dict)

    def test_missing_case_confirm_message_mentions_not_found(self):
        steps = self.helper({"doc_id": "x", "missing": True})
        self.assertIn("not found", steps[1].params["message"])

    def test_error_case_confirm_message_mentions_failed(self):
        steps = self.helper(
            {"doc_id": "x", "error": "denied", "error_type": "DataAccessDeniedError"}
        )
        self.assertIn("failed", steps[1].params["message"])

    def test_data_fetch_plan_not_in_recipes_registry(self):
        from lib.realms.test_realm.recipes import RECIPES

        self.assertNotIn("data_fetch_plan", RECIPES)


# ---------------------------------------------------------------------------
# Handler: the failure policy a scenario's plan runs under
# ---------------------------------------------------------------------------


class TestScenarioFailurePolicy(unittest.TestCase):
    """The failure policy TestRealmHandler gives the plan it drafts."""

    def setUp(self):
        from lib.realms.test_realm.handler import TestRealmHandler

        self.handler = TestRealmHandler()
        self.handler.realm_id = "test_realm"

    def draft(self, **fields):
        """Draft the plan of a scenario document with these fields."""
        doc = {"_id": "test_scenario:x", "type": "ygg_test_scenario", **fields}
        ctx = MagicMock(scope={"kind": "test_scenario", "id": "test_scenario:x"})
        payload = {"doc": doc, "planning_ctx": ctx}
        (draft,) = asyncio.run(self.handler.generate_plan_drafts(payload))
        return draft

    def test_branching_recipes_default_to_continue_independent(self):
        for recipe in ("branch_failure", "branch_failure_metadata_required"):
            with self.subTest(recipe=recipe):
                draft = self.draft(recipe=recipe)

                self.assertEqual(draft.plan.failure_policy, "continue_independent")
                self.assertEqual(
                    draft.preview["failure_policy"], "continue_independent"
                )

    def test_other_recipes_and_custom_steps_default_to_fail_fast(self):
        custom_steps = [{"step_id": "a", "fn_name": "step_echo"}]
        for mode, fields in (
            ("recipe", {"recipe": "fail_mid_plan"}),
            ("custom steps", {"steps": custom_steps}),
        ):
            with self.subTest(mode=mode):
                self.assertEqual(self.draft(**fields).plan.failure_policy, "fail_fast")

    def test_scenario_policy_replaces_the_recipe_default(self):
        for recipe, policy in (
            ("branch_failure", "fail_fast"),
            ("fail_mid_plan", "continue_independent"),
        ):
            with self.subTest(recipe=recipe):
                draft = self.draft(recipe=recipe, failure_policy=policy)

                self.assertEqual(draft.plan.failure_policy, policy)
                self.assertEqual(draft.preview["failure_policy"], policy)

    def test_unknown_policy_is_rejected_not_downgraded(self):
        for policy in ("continue", None):
            with self.subTest(policy=policy):
                with self.assertRaisesRegex(ValueError, "Invalid failure_policy"):
                    self.draft(recipe="branch_failure", failure_policy=policy)


# ---------------------------------------------------------------------------
# Recipes: branch_failure and branch_failure_metadata_required
# ---------------------------------------------------------------------------


class TestBranchingRecipes(unittest.TestCase):
    """The branching recipes, as the integration scenarios rely on them."""

    def test_metadata_prerequisite_is_the_only_difference(self):
        from lib.realms.test_realm.recipes import (
            branch_failure,
            branch_failure_metadata_required,
        )

        independent = branch_failure()
        required = branch_failure_metadata_required()

        self.assertEqual(
            [spec.step_id for spec in independent],
            [spec.step_id for spec in required],
        )
        for alone, joined in zip(independent, required, strict=True):
            with self.subTest(step=alone.step_id):
                self.assertEqual(
                    (alone.fn_ref, alone.params, alone.outputs),
                    (joined.fn_ref, joined.params, joined.outputs),
                )
                if alone.step_id.endswith("__prepare"):
                    self.assertEqual(alone.deps, ["validate_shared"])
                    self.assertEqual(
                        joined.deps, ["validate_shared", "update_metadata"]
                    )
                else:
                    self.assertEqual(alone.deps, joined.deps)

    def test_branch_roots_declare_the_file_they_write_after_overrides(self):
        from lib.realms.test_realm.recipes import branch_failure

        steps = branch_failure(overrides={"lane_1__prepare": {"filename": "own.txt"}})

        self.assertEqual(
            {spec.step_id: spec.outputs for spec in steps if spec.outputs},
            {
                "lane_1__prepare": {"lane_config": "own.txt"},
                "lane_2__prepare": {"lane_config": "lane_2_config.txt"},
            },
        )
