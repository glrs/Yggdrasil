"""
Test Realm - Dev-only internal test infrastructure.

This realm provides controllable test scenarios for validating Yggdrasil's
execution pipeline without external dependencies.

IMPORTANT: This realm is only available when running in dev mode (--dev flag).
In production mode, get_realm_descriptor() returns None, so the realm is not
discovered at all (no handlers, no watchspecs).

Components:
    - TestRealmHandler: Processes COUCHDB_DOC_CHANGED events for scenario docs
    - WatchSpec: Monitors yggdrasil DB for scenario documents (filter_expr based)
    - Recipes: Pre-defined test plans (happy_path, fail_fast, etc.)
    - Steps: Controllable test steps (echo, sleep, fail, write_file, random_fail)

Usage:
    1. Start Yggdrasil in dev mode: `yggdrasil --dev daemon`
    2. Create a scenario document in yggdrasil DB:
        {
            "_id": "test_scenario:my_test",
            "type": "ygg_test_scenario",
            "recipe": "happy_path",
            "auto_run": true
        }
    3. WatcherManager detects the document change via CouchDBBackend
    4. Handler generates a plan from the recipe
    5. Engine executes the plan

Recipes:
    - happy_path: All steps succeed
    - fail_fast: First step fails
    - fail_mid_plan: Middle step fails
    - long_running: Extended sleep (30s default)
    - artifact_write: Writes files and registers artifacts
    - branch_failure: Independent lane branches; one lane and the metadata
      update fail, the other lane completes (continue_independent)
    - branch_failure_metadata_required: The same plan with the metadata
      update a prerequisite of both lanes (continue_independent)

Scenario Document Schema:
    {
        "_id": "test_scenario:<unique_id>",  # Required
        "type": "ygg_test_scenario",          # Required (must be exact)
        "recipe": "<recipe_name>",          # Required
        "auto_run": true | false,             # Optional (default: true)
        "failure_policy": "fail_fast" | "continue_independent",
                                              # Optional (default: the
                                              # recipe's, else fail_fast)
        "overrides": {                        # Optional
            "<step_id>": {"param": "value"}
        }
    }
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from lib.core_utils.event_types import EventType
from lib.core_utils.ygg_session import YggSession

if TYPE_CHECKING:
    from lib.watchers.watchspec import WatchSpec
    from yggdrasil.core.realm import RealmDescriptor


def is_test_realm_enabled() -> bool:
    """
    Check if test realm should be enabled.

    Returns True only when running in dev mode (--dev flag).
    """
    return YggSession.is_dev()


def _build_scope(raw_event: Any) -> dict[str, str]:
    """
    Extract scope from test scenario document.

    Args:
        raw_event: RawWatchEvent from CouchDBBackend

    Returns:
        Scope dict with kind='test_scenario' and id from document
    """
    doc = getattr(raw_event, "doc", None) or {}
    scenario_id = str(
        doc.get("scenario_id") or doc.get("_id") or getattr(raw_event, "id", "unknown")
    )
    return {"kind": "test_scenario", "id": scenario_id}


def _build_payload(raw_event: Any) -> dict[str, Any]:
    """
    Build payload for test scenario event.

    Args:
        raw_event: RawWatchEvent from CouchDBBackend

    Returns:
        Payload dict containing doc and reason
    """
    doc = getattr(raw_event, "doc", None) or {}
    doc_id = doc.get("_id") or getattr(raw_event, "id", "unknown")
    return {
        "doc": doc,
        "reason": f"scenario_change:{doc_id}",
    }


def _get_watchspecs() -> list[WatchSpec]:
    """
    WatchSpec provider for test realm.

    Note: Called unconditionally; dev-mode gating is at get_realm_descriptor().

    Returns:
        List of WatchSpecs for test scenario monitoring.
    """
    from lib.watchers.watchspec import WatchSpec

    return [
        WatchSpec(
            backend="couchdb",
            connection="yggdrasil_testdocs",  # Connection name from config
            event_type=EventType.COUCHDB_DOC_CHANGED,
            filter_expr={
                "and": [
                    {"==": [{"var": "doc.type"}, "ygg_test_scenario"]},
                    {"==": [{"var": "deleted"}, False]},
                ]
            },
            build_scope=_build_scope,
            build_payload=_build_payload,
            target_handlers=["test_scenario_handler"],
        ),
    ]


def get_realm_descriptor() -> RealmDescriptor | None:
    """
    Entry point for ygg.realm discovery.

    Returns:
        RealmDescriptor if dev mode enabled, None otherwise.
        Returning None skips this realm entirely during discovery.

    Gating strategy:
        - Return None when not in dev mode (realm not discovered)
        - This is cleaner than registering handlers that never receive events
    """
    if not is_test_realm_enabled():
        return None

    from lib.realms.test_realm.handler import TestRealmHandler
    from yggdrasil.core.realm import RealmDescriptor

    return RealmDescriptor(
        realm_id="test_realm",
        handler_classes=[TestRealmHandler],  # CLASS, not instance
        watchspecs=_get_watchspecs,  # Callable for deferred loading
    )


# Expose key components for direct import
__all__ = [
    "is_test_realm_enabled",
    "get_realm_descriptor",
]
