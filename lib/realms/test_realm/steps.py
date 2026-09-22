"""
Test realm steps for dev/test scenarios.

These steps are designed for testing the Yggdrasil execution pipeline:
- step_echo: Simple success with message emission
- step_sleep: Configurable delay (for long-running scenarios)
- step_fail: Always fails with configurable message
- step_write_file: Writes a file and registers artifact
- step_random_fail: Probabilistic failure for chaos testing

NOTE: These steps are synchronous (def, not async def) because the
Engine.run() method is synchronous. Using async def would return
unawaited coroutines.

All step functions are decorated with @step so that the Engine emits
step.started, step.succeeded (with metrics/artifacts), and step.failed
lifecycle events automatically. Exceptions still bubble up to the Engine,
which records the step as failed: a fail_fast plan stops there, while a
continue_independent plan blocks the step's dependents and runs the rest.
"""

import random
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from yggdrasil.flow.model import StepResult
from yggdrasil.flow.step import StepContext, step


class _SimpleRef:
    """Simple artifact reference for test realm step registration."""

    def __init__(self, key: str):
        self.key_val = key

    def key(self) -> str:
        return self.key_val

    def resolve_path(self, scope_dir: Path) -> Path:
        # Not used for registration, but required by protocol
        return scope_dir / self.key_val


@step
def step_echo(ctx: StepContext, message: str = "Hello from test realm") -> StepResult:
    """
    Simple echo step that emits a message and succeeds.

    Args:
        ctx: Step execution context
        message: Message to echo (default: "Hello from test realm")

    Returns:
        StepResult with message in metrics
    """
    ctx.emit("step.echo", message=message)
    return StepResult(metrics={"echoed_message": message})


@step
def step_sleep(ctx: StepContext, duration_sec: float = 1.0) -> StepResult:
    """
    Sleep step for simulating long-running operations.

    Emits progress events at 25%, 50%, 75%, and 100%.

    Args:
        ctx: Step execution context
        duration_sec: Sleep duration in seconds (default: 1.0)

    Returns:
        StepResult with actual sleep duration in metrics
    """
    quarter = duration_sec / 4.0

    ctx.emit("step.progress", percent=0, message="Starting sleep")
    time.sleep(quarter)
    ctx.emit("step.progress", percent=25, message="25% complete")
    time.sleep(quarter)
    ctx.emit("step.progress", percent=50, message="50% complete")
    time.sleep(quarter)
    ctx.emit("step.progress", percent=75, message="75% complete")
    time.sleep(quarter)
    ctx.emit("step.progress", percent=100, message="Sleep complete")

    return StepResult(metrics={"slept_seconds": duration_sec})


@step
def step_fail(
    ctx: StepContext, error_message: str = "Intentional failure"
) -> StepResult:
    """
    Always-fail step for testing failure handling.

    Args:
        ctx: Step execution context
        error_message: Error message to raise (default: "Intentional failure")

    Raises:
        RuntimeError: Always raised with the configured message
    """
    ctx.emit("step.failing", reason=error_message)
    raise RuntimeError(error_message)


@step
def step_write_file(
    ctx: StepContext,
    filename: str = "test_output.txt",
    content: str = "Test content from test realm",
) -> StepResult:
    """
    Write a file to workdir and register it as an artifact.

    Args:
        ctx: Step execution context
        filename: Name of file to create (default: "test_output.txt")
        content: Content to write (default: "Test content from test realm")

    Returns:
        StepResult with artifact registered
    """
    # Write to workdir
    output_path = ctx.workdir / filename
    output_path.write_text(content)

    # Register artifact using record_artifact
    # Create a simple artifact ref for registration
    artifact = ctx.record_artifact(
        _SimpleRef(key=f"test_file:{filename}"),
        path=output_path,
    )

    return StepResult(
        artifacts=[artifact],
        metrics={"bytes_written": len(content), "filename": filename},
    )


@step
def step_random_fail(
    ctx: StepContext,
    failure_probability: float = 0.5,
    success_message: str = "Lucky! Survived random failure",
    failure_message: str = "Unlucky! Random failure triggered",
) -> StepResult:
    """
    Probabilistic failure step for chaos testing.

    Args:
        ctx: Step execution context
        failure_probability: Probability of failure, 0.0-1.0 (default: 0.5)
        success_message: Message on success
        failure_message: Message on failure

    Returns:
        StepResult if successful

    Raises:
        RuntimeError: With configured probability
    """
    roll = random.random()
    ctx.emit("step.random_roll", roll=roll, threshold=failure_probability)

    if roll < failure_probability:
        ctx.emit("step.failing", reason=failure_message, roll=roll)
        raise RuntimeError(failure_message)

    return StepResult(
        metrics={
            "roll": roll,
            "threshold": failure_probability,
            "survived": True,
            "message": success_message,
        }
    )


@step
def step_fetch_from_db(
    ctx: StepContext,
    connection: str = "yggdrasil_db",
    doc_id: str = "data_access_test:reference_doc",
) -> StepResult:
    """
    Fetch a document from CouchDB at execution time using DataAccess.

    Demonstrates read-only data access from a step. The fetched document
    is emitted as a step event and returned in the step's metrics so it
    is visible in the execution record.

    Args:
        ctx: Step execution context (ctx.data must be injected by Engine)
        connection: Connection name from external_systems config (must have
            a data_access block with test_realm in its realm_allowlist)
        doc_id: Document _id to fetch from the database

    Returns:
        StepResult with fetched doc in metrics

    Raises:
        RuntimeError: If ctx.data was not injected
        DataAccessDeniedError: If the realm is not allowed to read the connection
    """
    if ctx.data is None:
        raise RuntimeError(
            "ctx.data is None — DataAccess was not injected by the Engine"
        )

    client = ctx.data.connection(connection)
    doc = client.get(doc_id)

    if doc is None:
        ctx.emit("step.fetch_result", status="not_found", doc_id=doc_id)
        return StepResult(metrics={"status": "not_found", "doc_id": doc_id})

    ctx.emit("step.fetch_result", status="found", doc_id=doc_id, doc=doc)
    return StepResult(metrics={"status": "found", "doc_id": doc_id, "doc": doc})


@step
def step_expect_denied(
    ctx: StepContext,
    connection: str = "projects_db",
) -> StepResult:
    """
    Verify DataAccess correctly rejects access to a restricted connection.

    Succeeds (returns StepResult) if any DataAccessError subclass is raised:
    - DataAccessDeniedError: connection exists but realm has no permissions
      configured, or the connection has no data_access policy at all.
    - DataAccessConfigError: connection name does not exist in config at all.

    Fails hard (raises RuntimeError) if access is unexpectedly granted.

    Args:
        ctx: Step execution context
        connection: Connection name that should be denied for this realm

    Returns:
        StepResult confirming access was correctly denied

    Raises:
        RuntimeError: If access was unexpectedly granted
    """
    from yggdrasil.flow.data_access import DataAccessError

    if ctx.data is None:
        raise RuntimeError(
            "ctx.data is None — DataAccess was not injected by the Engine"
        )

    try:
        ctx.data.couchdb(connection)
        raise RuntimeError(
            f"Expected DataAccessError for connection '{connection}' "
            f"but access was granted — check allowlist configuration!"
        )
    except DataAccessError as exc:
        ctx.emit(
            "step.access_denied_as_expected",
            connection=connection,
            error_type=type(exc).__name__,
            denial_reason=str(exc),
        )
        return StepResult(
            metrics={
                "access_correctly_denied": True,
                "connection": connection,
                "error_type": type(exc).__name__,
                "denial_reason": str(exc),
            }
        )


@step
def step_write_to_db(
    ctx: StepContext,
    connection: str = "test_realm_write_db",
    doc_id: str = "data_access_test:write_result",
    mode: str = "upsert",
) -> StepResult:
    """
    Write a document to CouchDB at execution time using DataAccess.

    Demonstrates write access from a step. The connection must have "write"
    permission for the realm in the execution phase. The body dict is kept
    clean (no _id or _rev) — DataAccess manages those internally.

    Args:
        ctx: Step execution context (ctx.data must be injected by Engine)
        connection: Connection name with execution write permission
        doc_id: Document _id to write
        mode: Write mode — "create", "update", or "upsert" (default)

    Returns:
        StepResult with write outcome fields in metrics

    Raises:
        RuntimeError: If ctx.data was not injected
        DataAccessDeniedError: If the realm lacks write permission
        DataAccessWriteError: If the write fails (e.g. conflict)
    """
    if ctx.data is None:
        raise RuntimeError(
            "ctx.data is None — DataAccess was not injected by the Engine"
        )

    body = {
        "type": "ygg_test_write_result",
        "status": "written",
        "written_by": "test_realm/step_write_to_db",
    }
    client = ctx.data.connection(connection)
    result = client.save(body, doc_id=doc_id, mode=mode)

    ctx.emit(
        "step.write_result",
        write_status=result.status,
        doc_id=result.doc_id,
        old_rev=result.old_rev,
        new_rev=result.new_rev,
    )
    return StepResult(
        metrics={
            "write_status": result.status,
            "doc_id": result.doc_id,
            "old_rev": result.old_rev,
            "new_rev": result.new_rev,
        }
    )


@step
def step_write_to_db_no_id(
    ctx: StepContext,
    connection: str = "test_realm_write_db",
    selector: dict[str, Any] | None = None,
    mode: str = "upsert",
) -> StepResult:
    """
    Write a document to CouchDB without providing a _id.

    Uses Mango selector identity so CouchDB auto-generates the document ID on
    create. Demonstrates the selector identity path of save().

    Args:
        ctx: Step execution context
        connection: Connection name with execution write permission
        selector: Mango selector for identity (default: match by type + written_by)
        mode: Write mode — "create", "update", or "upsert" (default)

    Returns:
        StepResult with write outcome fields; doc_id is CouchDB-generated on create

    Raises:
        RuntimeError: If ctx.data was not injected
        DataAccessDeniedError: If the realm lacks write permission
        DataAccessWriteError: If the write fails (e.g. conflict)
    """
    if ctx.data is None:
        raise RuntimeError(
            "ctx.data is None — DataAccess was not injected by the Engine"
        )

    effective_selector = selector or {
        "type": "ygg_test_write_result_no_id",
        "written_by": "test_realm/step_write_to_db_no_id",
    }
    body = {
        "type": "ygg_test_write_result_no_id",
        "status": "written",
        "written_by": "test_realm/step_write_to_db_no_id",
        "write_mode": mode,
        "updated_at": datetime.now(UTC).isoformat(),
    }
    client = ctx.data.connection(connection)
    result = client.save(body, selector=effective_selector, mode=mode)

    ctx.emit(
        "step.write_result",
        write_status=result.status,
        doc_id=result.doc_id,
        old_rev=result.old_rev,
        new_rev=result.new_rev,
        identity=result.identity,
        operation=result.operation,
    )
    return StepResult(
        metrics={
            "write_status": result.status,
            "doc_id": result.doc_id,
            "old_rev": result.old_rev,
            "new_rev": result.new_rev,
            "identity": result.identity,
            "operation": result.operation,
        }
    )


@step
def step_expect_read_denied(
    ctx: StepContext,
    connection: str = "test_realm_write_db",
    doc_id: str = "data_access_test:probe",
) -> StepResult:
    """
    Verify that a write-only connection correctly denies read access.

    The connection() call succeeds (write permission allows it), but
    client.get() must raise DataAccessDeniedError because "read" is absent
    from the connection's execution permissions.

    Succeeds (returns StepResult) only if DataAccessDeniedError is raised on
    get(). Fails hard (raises RuntimeError) if the read unexpectedly succeeds.

    Args:
        ctx: Step execution context
        connection: Connection name configured with write-only execution permission
        doc_id: Document _id to attempt reading

    Returns:
        StepResult confirming read was correctly denied

    Raises:
        RuntimeError: If read was unexpectedly allowed
    """
    from yggdrasil.flow.data_access import DataAccessDeniedError

    if ctx.data is None:
        raise RuntimeError(
            "ctx.data is None — DataAccess was not injected by the Engine"
        )

    client = ctx.data.connection(connection)  # succeeds — write permission exists
    try:
        client.get(doc_id)
        raise RuntimeError(
            f"Expected DataAccessDeniedError for get() on '{connection}' "
            f"but read succeeded — check permissions configuration!"
        )
    except DataAccessDeniedError as exc:
        ctx.emit(
            "step.read_denied_as_expected",
            connection=connection,
            denial_reason=str(exc),
        )
        return StepResult(
            metrics={
                "read_correctly_denied": True,
                "connection": connection,
                "denial_reason": str(exc),
            }
        )


@step
def step_exercise_all_fetch_methods(
    ctx: StepContext,
    connection: str = "yggdrasil_db",
    doc_id: str = "data_access_test:reference_doc",
    selector_type: str = "ygg_test_reference",
) -> StepResult:
    """
    Exercise every read method on CouchDBReadClient in one step.

    Calls get, require, find, find_one, fetch_by_field, and require_one
    against the reference document so that all paths through the DataAccess
    layer are exercised end-to-end in a single scenario run.

    The selector used for Mango queries matches on the 'type' field of the
    reference document. Each method's outcome is emitted and returned in
    the step metrics so results are visible in the execution record.

    Args:
        ctx: Step execution context (ctx.data must be injected by Engine)
        connection: Connection name from external_systems config
        doc_id: Document _id to target for get/require calls
        selector_type: Value of the 'type' field used in Mango selectors

    Returns:
        StepResult with per-method outcomes in metrics

    Raises:
        RuntimeError: If ctx.data was not injected
        DataAccessNotFoundError: If require or require_one finds nothing
    """
    if ctx.data is None:
        raise RuntimeError(
            "ctx.data is None — DataAccess was not injected by the Engine"
        )

    client = ctx.data.connection(connection)
    selector = {"type": {"$eq": selector_type}}
    results: dict = {}

    # 1. get() — returns the doc dict or None
    doc = client.get(doc_id)
    results["get"] = {
        "found": doc is not None,
        "id": doc.get("_id") if doc else None,
    }

    # 2. require() — same as get but raises DataAccessNotFoundError if absent
    doc_req = client.require(doc_id)
    results["require"] = {"id": doc_req.get("_id")}

    # 3. find() — Mango selector, returns list (clamped to effective max_limit from data_access.options)
    docs = client.find(selector)
    results["find"] = {
        "count": len(docs),
        "ids": [d.get("_id") for d in docs],
    }

    # 4. find_one() — Mango selector, returns first match or None
    first = client.find_one(selector)
    results["find_one"] = {
        "found": first is not None,
        "id": first.get("_id") if first else None,
    }

    # 5. fetch_by_field() — equality convenience wrapper around find()
    by_type = client.fetch_by_field("type", selector_type)
    results["fetch_by_field"] = {
        "count": len(by_type),
        "ids": [d.get("_id") for d in by_type],
    }

    # 6. require_one() — Mango selector, raises DataAccessNotFoundError if none
    one = client.require_one(selector)
    results["require_one"] = {"id": one.get("_id")}

    ctx.emit("step.all_fetch_methods", results=results)
    return StepResult(metrics={"all_fetch_methods": results})


@step
def step_verify_limit_clamping(
    ctx: StepContext,
    connection: str = "yggdrasil_db_clamped",
    selector_type: str = "ygg_test_reference",
    request_limit: int = 100,
    expected_max: int = 2,
) -> StepResult:
    """
    Verify that DataAccess clamps find() results to policy.max_limit.

    Requests ``request_limit`` documents (intentionally larger than the
    effective ``max_limit`` from ``data_access.options.max_limit``) and
    asserts that the number of returned documents does not exceed
    ``expected_max`` (which should equal the connection's configured
    ``options.max_limit``).

    The step SUCCEEDS (returns StepResult) when clamping is correctly
    enforced. It FAILS (raises RuntimeError) if the returned count exceeds
    ``expected_max``, which would indicate the policy is not being applied.

    Args:
        ctx: Step execution context (ctx.data must be injected by Engine)
        connection: Connection name with a low max_limit in data_access.options
            (default: yggdrasil_db_clamped)
        selector_type: Value of the 'type' field used in the Mango selector
        request_limit: Limit value to pass to find() — should exceed max_limit
        expected_max: Maximum number of results expected after clamping

    Returns:
        StepResult with actual count and clamping proof in metrics

    Raises:
        RuntimeError: If ctx.data was not injected, or if clamping was not enforced
    """
    if ctx.data is None:
        raise RuntimeError(
            "ctx.data is None — DataAccess was not injected by the Engine"
        )

    client = ctx.data.connection(connection)
    selector = {"type": {"$eq": selector_type}}

    docs = client.find(selector, limit=request_limit)
    actual_count = len(docs)

    ctx.emit(
        "step.limit_clamp_result",
        requested=request_limit,
        expected_max=expected_max,
        actual_count=actual_count,
        clamped=actual_count <= expected_max,
    )

    if actual_count > expected_max:
        raise RuntimeError(
            f"Limit clamping NOT enforced: requested {request_limit}, "
            f"expected at most {expected_max}, got {actual_count}"
        )

    return StepResult(
        metrics={
            "clamping_enforced": True,
            "requested_limit": request_limit,
            "expected_max": expected_max,
            "actual_count": actual_count,
        }
    )


@step
def step_emit_metadata(
    ctx: StepContext,
    scenario: dict | None = None,
    ref_doc: dict | None = None,
) -> StepResult:
    """
    Emit structured metadata that was baked into the plan during generate_plan_drafts.

    Used by two patterns:

    **metadata_harvest** — ``scenario`` carries domain fields harvested from the
    triggering document (e.g. input_path, mode, priority, sample_id, flags).
    The handler extracted these at plan-generation time and baked them as a
    structured dict into ``StepSpec.params``.  This step emits them so they
    are visible in the execution record.

    **data_fetch_plan** — ``ref_doc`` carries the structured result of an async
    CouchDB fetch performed during planning (doc_id, message, value, missing).
    Emitting it here proves in the event log that the data was resolved before
    execution started.

    Args:
        ctx: Step execution context.
        scenario: Domain metadata dict harvested from the triggering document.
        ref_doc: Structured result of a plan-time CouchDB fetch.

    Returns:
        StepResult with whichever of scenario/ref_doc was provided in metrics.
    """
    metrics: dict[str, Any] = {}

    if scenario is not None:
        ctx.emit("step.metadata_harvested", **scenario)
        metrics["scenario"] = scenario

    if ref_doc is not None:
        ctx.emit("step.ref_doc_echoed", **ref_doc)
        metrics["ref_doc"] = ref_doc

    return StepResult(metrics=metrics)


# ---------------------------------------------------------------------------
# Step registry for fn_ref resolution
# ---------------------------------------------------------------------------

STEPS: dict[str, Any] = {
    "step_echo": step_echo,
    "step_sleep": step_sleep,
    "step_fail": step_fail,
    "step_write_file": step_write_file,
    "step_random_fail": step_random_fail,
    "step_fetch_from_db": step_fetch_from_db,
    "step_expect_denied": step_expect_denied,
    "step_write_to_db": step_write_to_db,
    "step_expect_read_denied": step_expect_read_denied,
    "step_exercise_all_fetch_methods": step_exercise_all_fetch_methods,
    "step_verify_limit_clamping": step_verify_limit_clamping,
    "step_emit_metadata": step_emit_metadata,
}


def get_step_fn(name: str):
    """
    Get step function by name.

    Args:
        name: Step function name (e.g., "step_echo")

    Returns:
        The step function (synchronous)

    Raises:
        KeyError: If step name not found
    """
    if name not in STEPS:
        raise KeyError(f"Unknown test realm step: {name}")
    return STEPS[name]
