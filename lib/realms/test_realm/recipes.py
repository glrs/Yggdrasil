"""
Test realm recipes for generating test plans.

Recipes are factory functions that return a list of StepSpec objects.
Each recipe represents a different test scenario:

- happy_path: All steps succeed
- fail_fast: First step fails
- fail_mid_plan: Fails in the middle of execution
- long_running: Extended sleep for timeout testing
- artifact_write: Tests artifact registration
- branch_failure: Independent branches after shared validation; one branch
  and the metadata update fail, the other branch completes
- branch_failure_metadata_required: The same plan, with the metadata update
  a declared prerequisite of every branch

A recipe's plans run under the fail_fast policy unless
RECIPE_FAILURE_POLICIES names another (see default_failure_policy).
"""

from typing import Any

from yggdrasil.flow.model import (
    CONTINUE_INDEPENDENT_POLICY,
    DEFAULT_FAILURE_POLICY,
    StepSpec,
)

# Module path for fn_ref resolution by Engine
_FN_REF_PREFIX = "lib.realms.test_realm.steps"


def _make_step(
    step_id: str,
    name: str,
    fn_name: str,
    params: dict[str, Any] | None = None,
    deps: list[str] | None = None,
) -> StepSpec:
    """Helper to create StepSpec with consistent fn_ref format."""
    return StepSpec(
        step_id=step_id,
        name=name,
        fn_ref=f"{_FN_REF_PREFIX}.{fn_name}",
        params=params or {},
        deps=deps or [],
    )


# ---------------------------------------------------------------------------
# Recipe: happy_path
# ---------------------------------------------------------------------------


def happy_path(
    overrides: dict[str, dict[str, Any]] | None = None,
) -> list[StepSpec]:
    """
    Generate a plan where all steps succeed.

    Steps:
        1. echo_start: Echo "Starting happy path"
        2. brief_sleep: Sleep 0.5s
        3. echo_end: Echo "Happy path complete"

    Args:
        overrides: Optional dict mapping step_id to param overrides

    Returns:
        List of StepSpec for Engine execution
    """
    overrides = overrides or {}

    steps = [
        _make_step(
            step_id="echo_start",
            name="Echo Start",
            fn_name="step_echo",
            params={"message": "Starting happy path"},
        ),
        _make_step(
            step_id="brief_sleep",
            name="Brief Sleep",
            fn_name="step_sleep",
            params={"duration_sec": 0.5},
            deps=["echo_start"],
        ),
        _make_step(
            step_id="echo_end",
            name="Echo End",
            fn_name="step_echo",
            params={"message": "Happy path complete"},
            deps=["brief_sleep"],
        ),
    ]

    # Apply overrides by step_id
    return _apply_overrides(steps, overrides)


# ---------------------------------------------------------------------------
# Recipe: random_fail
# ---------------------------------------------------------------------------


def random_fail(
    overrides: dict[str, dict[str, Any]] | None = None,
) -> list[StepSpec]:
    """
    Generate a plan with probabilistic failure.

    Steps:
        1. echo_start: Echo "Starting random test"
        2. random_step: 50% chance of failure
        3. echo_end: Echo "Random test survived" (only if step 2 succeeds)

    Args:
        overrides: Optional dict mapping step_id to param overrides

    Returns:
        List of StepSpec for Engine execution
    """
    overrides = overrides or {}

    steps = [
        _make_step(
            step_id="echo_start",
            name="Echo Start",
            fn_name="step_echo",
            params={"message": "Starting random failure test"},
        ),
        _make_step(
            step_id="random_step",
            name="Random Failure Step",
            fn_name="step_random_fail",
            params={
                "failure_probability": 0.5,
                "success_message": "Survived random failure!",
                "failure_message": "Random failure triggered",
            },
            deps=["echo_start"],
        ),
        _make_step(
            step_id="echo_end",
            name="Echo End",
            fn_name="step_echo",
            params={"message": "Random test completed successfully"},
            deps=["random_step"],
        ),
    ]

    # Apply overrides by step_id
    return _apply_overrides(steps, overrides)


# ---------------------------------------------------------------------------
# Recipe: fail_fast
# ---------------------------------------------------------------------------


def fail_fast(
    overrides: dict[str, dict[str, Any]] | None = None,
) -> list[StepSpec]:
    """
    Generate a plan that fails on the first step.

    Steps:
        1. fail_immediately: Always fails
        2. never_reached: Would echo but never runs

    Args:
        overrides: Optional dict mapping step_id to param overrides

    Returns:
        List of StepSpec for Engine execution
    """
    overrides = overrides or {}

    steps = [
        _make_step(
            step_id="fail_immediately",
            name="Fail Immediately",
            fn_name="step_fail",
            params={"error_message": "Fail fast: first step failure"},
        ),
        _make_step(
            step_id="never_reached",
            name="Never Reached",
            fn_name="step_echo",
            params={"message": "This should never execute"},
            deps=["fail_immediately"],
        ),
    ]

    return _apply_overrides(steps, overrides)


# ---------------------------------------------------------------------------
# Recipe: fail_mid_plan
# ---------------------------------------------------------------------------


def fail_mid_plan(
    overrides: dict[str, dict[str, Any]] | None = None,
) -> list[StepSpec]:
    """
    Generate a plan that succeeds initially then fails mid-execution.

    Steps:
        1. echo_start: Echo "Starting..."
        2. brief_sleep: Sleep 0.3s
        3. mid_failure: Always fails
        4. never_reached: Would echo but never runs

    Args:
        overrides: Optional dict mapping step_id to param overrides

    Returns:
        List of StepSpec for Engine execution
    """
    overrides = overrides or {}

    steps = [
        _make_step(
            step_id="echo_start",
            name="Echo Start",
            fn_name="step_echo",
            params={"message": "Starting mid-fail scenario"},
        ),
        _make_step(
            step_id="brief_sleep",
            name="Brief Sleep",
            fn_name="step_sleep",
            params={"duration_sec": 0.3},
            deps=["echo_start"],
        ),
        _make_step(
            step_id="mid_failure",
            name="Mid Failure",
            fn_name="step_fail",
            params={"error_message": "Planned mid-execution failure"},
            deps=["brief_sleep"],
        ),
        _make_step(
            step_id="never_reached",
            name="Never Reached",
            fn_name="step_echo",
            params={"message": "This should never execute"},
            deps=["mid_failure"],
        ),
    ]

    return _apply_overrides(steps, overrides)


# ---------------------------------------------------------------------------
# Recipe: long_running
# ---------------------------------------------------------------------------


def long_running(
    overrides: dict[str, dict[str, Any]] | None = None,
) -> list[StepSpec]:
    """
    Generate a plan with extended sleep for timeout/cancellation testing.

    Steps:
        1. echo_start: Echo "Starting long run"
        2. long_sleep: Sleep 30s (configurable via overrides)
        3. echo_end: Echo "Long run complete"

    Args:
        overrides: Optional dict mapping step_id to param overrides
            Tip: Override long_sleep params to adjust duration

    Returns:
        List of StepSpec for Engine execution
    """
    overrides = overrides or {}

    steps = [
        _make_step(
            step_id="echo_start",
            name="Echo Start",
            fn_name="step_echo",
            params={"message": "Starting long-running scenario"},
        ),
        _make_step(
            step_id="long_sleep",
            name="Long Sleep",
            fn_name="step_sleep",
            params={"duration_sec": 30.0},  # Default 30s, override to change duration
            deps=["echo_start"],
        ),
        _make_step(
            step_id="echo_end",
            name="Echo End",
            fn_name="step_echo",
            params={"message": "Long-running scenario complete"},
            deps=["long_sleep"],
        ),
    ]

    return _apply_overrides(steps, overrides)


# ---------------------------------------------------------------------------
# Recipe: artifact_write
# ---------------------------------------------------------------------------


def artifact_write(
    overrides: dict[str, dict[str, Any]] | None = None,
) -> list[StepSpec]:
    """
    Generate a plan that writes files and registers artifacts.

    Steps:
        1. echo_start: Echo "Starting artifact write"
        2. write_file_1: Write test_output_1.txt
        3. write_file_2: Write test_output_2.txt (parallel-eligible)
        4. echo_end: Echo completion message

    Args:
        overrides: Optional dict mapping step_id to param overrides

    Returns:
        List of StepSpec for Engine execution
    """
    overrides = overrides or {}

    steps = [
        _make_step(
            step_id="echo_start",
            name="Echo Start",
            fn_name="step_echo",
            params={"message": "Starting artifact write scenario"},
        ),
        _make_step(
            step_id="write_file_1",
            name="Write File 1",
            fn_name="step_write_file",
            params={
                "filename": "test_output_1.txt",
                "content": "Content from first write step",
            },
            deps=["echo_start"],
        ),
        _make_step(
            step_id="write_file_2",
            name="Write File 2",
            fn_name="step_write_file",
            params={
                "filename": "test_output_2.txt",
                "content": "Content from second write step",
            },
            deps=["echo_start"],  # Both file writes can run in parallel
        ),
        _make_step(
            step_id="echo_end",
            name="Echo End",
            fn_name="step_echo",
            params={"message": "Artifact write complete"},
            deps=["write_file_1", "write_file_2"],
        ),
    ]

    return _apply_overrides(steps, overrides)


# ---------------------------------------------------------------------------
# Recipes: branch_failure and branch_failure_metadata_required
# ---------------------------------------------------------------------------

# The lanes a branching plan has a branch for, and the lane whose processing
# fails.
_BRANCH_LANES = (1, 2)
_FAILING_LANE = 2


def branch_failure(
    overrides: dict[str, dict[str, Any]] | None = None,
) -> list[StepSpec]:
    """
    Generate a plan of independent lane branches in which one branch fails.

    Shared validation comes first. The metadata update and both lane branches
    depend on it, and on nothing else. The metadata update fails, and so does
    lane 2 partway through its branch. Under continue_independent, this
    recipe's default policy, lane 1 still completes, lane 2's upload is
    blocked, and the attempt ends failed.

    Steps:
        1. validate_shared: Echo (succeeds)
        2. update_metadata: Always fails (after validate_shared)
        3. lane_1__prepare: Write lane_1_config.txt (after validate_shared)
        4. lane_1__process: Echo (after lane_1__prepare)
        5. lane_1__upload: Echo (after lane_1__process)
        6. lane_2__prepare: Write lane_2_config.txt (after validate_shared)
        7. lane_2__process: Always fails (after lane_2__prepare)
        8. lane_2__upload: Echo (after lane_2__process), so blocked

    Args:
        overrides: Optional dict mapping step_id to param overrides

    Returns:
        List of StepSpec for Engine execution
    """
    return _branching_steps(metadata_required=False, overrides=overrides or {})


def branch_failure_metadata_required(
    overrides: dict[str, dict[str, Any]] | None = None,
) -> list[StepSpec]:
    """
    Generate the branch_failure plan with the metadata update required.

    The same steps and failures as branch_failure, except that each lane
    branch's first step also depends on update_metadata. Its failure blocks
    both branches under continue_independent, this recipe's default policy,
    so lane 2's processing is never invoked.

    Steps:
        1. validate_shared: Echo (succeeds)
        2. update_metadata: Always fails (after validate_shared)
        3. lane_1__prepare: Write lane_1_config.txt (after validate_shared
           and update_metadata), so blocked, like the rest of lane 1
        4. lane_1__process: Echo (after lane_1__prepare)
        5. lane_1__upload: Echo (after lane_1__process)
        6. lane_2__prepare: Write lane_2_config.txt (after validate_shared
           and update_metadata), so blocked, like the rest of lane 2
        7. lane_2__process: Always fails (after lane_2__prepare)
        8. lane_2__upload: Echo (after lane_2__process)

    Args:
        overrides: Optional dict mapping step_id to param overrides

    Returns:
        List of StepSpec for Engine execution
    """
    return _branching_steps(metadata_required=True, overrides=overrides or {})


def _branching_steps(
    *, metadata_required: bool, overrides: dict[str, dict[str, Any]]
) -> list[StepSpec]:
    """
    Build the plan both branching recipes share.

    Whether the metadata update is a prerequisite of the branches is the only
    difference between the two recipes, and only their dependencies express
    it: the engine gives no step special treatment because of its name.

    Each branch's first step writes a file and declares it as a required
    output, so that step is reused on a rerun only while the file exists. The
    declaration is made after the overrides are applied, so that it names the
    file the step actually writes.

    Args:
        metadata_required: Whether each branch's first step also depends on
            the metadata update.
        overrides: Param overrides by step_id.

    Returns:
        list[StepSpec]: The steps, in plan order.
    """
    branch_prerequisites = ["validate_shared"]
    if metadata_required:
        branch_prerequisites.append("update_metadata")

    steps = [
        _make_step(
            step_id="validate_shared",
            name="Validate Shared Inputs",
            fn_name="step_echo",
            params={"message": "Shared inputs validated"},
        ),
        _make_step(
            step_id="update_metadata",
            name="Update Metadata",
            fn_name="step_fail",
            params={"error_message": "Planned metadata update failure"},
            deps=["validate_shared"],
        ),
    ]
    for lane in _BRANCH_LANES:
        steps.extend(_lane_branch(lane, branch_prerequisites))

    steps = _apply_overrides(steps, overrides)
    for spec in steps:
        if spec.step_id.endswith("__prepare"):
            spec.outputs = {"lane_config": spec.params["filename"]}
    return steps


def _lane_branch(lane: int, prerequisites: list[str]) -> list[StepSpec]:
    """
    Build one lane's branch: prepare, then process, then upload.

    Args:
        lane: The lane number; the failing lane's processing always fails.
        prerequisites: The steps the branch's first step depends on.

    Returns:
        list[StepSpec]: The branch's steps, in dependency order.
    """
    prepare = f"lane_{lane}__prepare"
    process = f"lane_{lane}__process"
    if lane == _FAILING_LANE:
        process_fn = "step_fail"
        process_params = {"error_message": f"Planned failure processing lane {lane}"}
    else:
        process_fn = "step_echo"
        process_params = {"message": f"Lane {lane} processed"}

    return [
        _make_step(
            step_id=prepare,
            name=f"Prepare Lane {lane}",
            fn_name="step_write_file",
            params={
                "filename": f"lane_{lane}_config.txt",
                "content": f"Configuration for lane {lane}",
            },
            deps=list(prerequisites),
        ),
        _make_step(
            step_id=process,
            name=f"Process Lane {lane}",
            fn_name=process_fn,
            params=process_params,
            deps=[prepare],
        ),
        _make_step(
            step_id=f"lane_{lane}__upload",
            name=f"Upload Lane {lane}",
            fn_name="step_echo",
            params={"message": f"Lane {lane} uploaded"},
            deps=[process],
        ),
    ]


# ---------------------------------------------------------------------------
# Recipe: data_fetch_plan
# ---------------------------------------------------------------------------


def data_fetch_plan_steps(ref_dict: dict) -> list[StepSpec]:
    """
    Internal helper called by the handler (Mode 1) and by data_fetch_plan().
    Requires the already-fetched reference data to be passed in.

    The handler performs an async CouchDB fetch during ``generate_plan_drafts``
    and then calls this function so the result is baked into the step params
    as a **structured dict** — not a formatted string.

    The resulting plan therefore carries observable proof that the CouchDB
    fetch happened at planning time: ``echo_fetched.params["ref_doc"]``
    contains the structured doc snapshot with ``doc_id``, ``message``,
    ``value``, and ``missing`` (or ``error``/``error_type`` on failure).

    Args:
        ref_dict: Structured result from the plan-time CouchDB fetch.
            Shape on success:  ``{"doc_id": "...", "message": "...", "value": 13, "missing": False}``
            Shape when absent: ``{"doc_id": "...", "missing": True}``
            Shape on error:    ``{"doc_id": "...", "error": "...", "error_type": "..."}``

    Returns:
        List of StepSpec with fetched data embedded in params as a structured dict.
    """
    doc_id = ref_dict.get("doc_id", "?")
    is_error = "error" in ref_dict
    is_missing = ref_dict.get("missing", False)

    if is_error:
        confirm_message = (
            f"Plan-time fetch of {doc_id!r} failed — error baked into plan params"
        )
    elif is_missing:
        confirm_message = (
            f"Plan-time fetch of {doc_id!r}: doc not found — recorded in plan params"
        )
    else:
        confirm_message = (
            f"Plan-time fetch of {doc_id!r} succeeded — ref_doc baked into plan params"
        )

    return [
        _make_step(
            step_id="echo_fetched",
            name="Echo Plan-Time Fetch",
            fn_name="step_emit_metadata",
            params={"ref_doc": ref_dict},
        ),
        _make_step(
            step_id="echo_confirm",
            name="Confirm Plan-Time Fetch",
            fn_name="step_echo",
            params={"message": confirm_message},
            deps=["echo_fetched"],
        ),
    ]


# ---------------------------------------------------------------------------
# Recipe: metadata_harvest  (planning-time — called from handler, not registry)
# ---------------------------------------------------------------------------


def metadata_harvest_steps(scenario: dict) -> list[StepSpec]:
    """
    Generate steps for the metadata_harvest scenario.

    This function is NOT in the RECIPES registry because it requires metadata
    extracted from the triggering document to be passed in at plan-generation
    time.  The handler harvests domain fields (``input_path``, ``mode``,
    ``priority``, ``sample_id``, ``flags``) from the scenario doc during
    ``generate_plan_drafts`` and calls this function so the metadata is baked
    as a **structured dict** into ``StepSpec.params``.

    This demonstrates the "real realm" pattern: handlers map domain-document
    fields into step params rather than baking them as opaque strings.  The
    resulting plan record documents exactly what metadata drove the run.

    Args:
        scenario: Dict of domain metadata fields harvested from the scenario
            document (e.g. ``input_path``, ``mode``, ``priority``,
            ``sample_id``, ``flags``).

    Returns:
        List of StepSpec with harvested metadata embedded in params as a
        structured dict.
    """
    return [
        _make_step(
            step_id="emit_metadata",
            name="Emit Harvested Metadata",
            fn_name="step_emit_metadata",
            params={"scenario": scenario},
        ),
        _make_step(
            step_id="echo_confirm",
            name="Confirm Metadata Harvested",
            fn_name="step_echo",
            params={
                "message": (
                    f"Metadata harvest complete — "
                    f"sample_id={scenario.get('sample_id', '?')!r}, "
                    f"mode={scenario.get('mode', '?')!r}, "
                    f"priority={scenario.get('priority', '?')}"
                )
            },
            deps=["emit_metadata"],
        ),
    ]


# ---------------------------------------------------------------------------
# Recipe: data_fetch_exec
# ---------------------------------------------------------------------------


def data_fetch_exec(
    overrides: dict[str, dict[str, Any]] | None = None,
) -> list[StepSpec]:
    """
    Generate a plan that fetches from CouchDB at execution time.

    The step_fetch_from_db step uses ctx.data.connection() at runtime, so the
    fetch happens when the Engine runs the step — not during planning. The
    fetched document appears in the step's emitted events and result metrics,
    which makes it visible in the execution record.

    Steps:
        1. fetch_doc: Fetch data_access_test:reference_doc from yggdrasil_db
        2. echo_confirm: Echo confirmation message (depends on fetch_doc)

    Args:
        overrides: Optional dict mapping step_id to param overrides.
            Use to point at a different connection/doc_id if needed.

    Returns:
        List of StepSpec for Engine execution
    """
    overrides = overrides or {}

    steps = [
        _make_step(
            step_id="fetch_doc",
            name="Fetch Reference Doc",
            fn_name="step_fetch_from_db",
            params={
                "connection": "yggdrasil_db",
                "doc_id": "data_access_test:reference_doc",
            },
        ),
        _make_step(
            step_id="echo_confirm",
            name="Confirm Execution-Time Fetch",
            fn_name="step_echo",
            params={"message": "Execution-time CouchDB fetch complete!"},
            deps=["fetch_doc"],
        ),
    ]

    return _apply_overrides(steps, overrides)


# ---------------------------------------------------------------------------
# Recipe: data_access_denied
# ---------------------------------------------------------------------------


def data_access_denied(
    overrides: dict[str, dict[str, Any]] | None = None,
) -> list[StepSpec]:
    """
    Generate a plan that verifies DataAccess correctly rejects unauthorized access.

    Two denial cases are tested in sequence:
      1. projects_db — has no data_access block → DataAccessDeniedError (no policy)
      2. mock_resource — has data_access but test_realm not in realms config
             → DataAccessDeniedError (realm not configured)

    Each step succeeds only if the expected denial is raised; it fails hard
    if access is unexpectedly granted.

    Steps:
        1. verify_no_policy: projects_db has no data_access policy
        2. verify_not_allowlisted: mock_resource allows only tenx
        3. echo_pass: All denials verified

    Args:
        overrides: Optional param overrides by step_id

    Returns:
        List of StepSpec for Engine execution
    """
    overrides = overrides or {}

    steps = [
        _make_step(
            step_id="verify_no_policy",
            name="Verify No-Policy Denial",
            fn_name="step_expect_denied",
            params={"connection": "projects_db"},
        ),
        _make_step(
            step_id="verify_not_configured",
            name="Verify Realm-Not-Configured Denial",
            fn_name="step_expect_denied",
            params={"connection": "mock_resource"},
            deps=["verify_no_policy"],
        ),
        _make_step(
            step_id="echo_pass",
            name="All Denials Verified",
            fn_name="step_echo",
            params={"message": "All data-access denial cases passed as expected!"},
            deps=["verify_not_configured"],
        ),
    ]

    return _apply_overrides(steps, overrides)


# ---------------------------------------------------------------------------
# Recipe: data_fetch_all_methods
# ---------------------------------------------------------------------------


def data_fetch_all_methods(
    overrides: dict[str, dict[str, Any]] | None = None,
) -> list[StepSpec]:
    """
    Exercise every read method on CouchDBReadClient in sequence.

    Uses step_exercise_all_fetch_methods which calls get, require, find,
    find_one, fetch_by_field, and require_one in a single step against the
    phase-aware CouchDBExecutionClient.

    Steps:
        1. exercise_all: Runs all six fetch methods against yggdrasil_db
        2. echo_confirm: Confirms all methods completed without error

    Args:
        overrides: Optional param overrides by step_id.
            Override exercise_all params to target a different connection,
            doc_id, or selector_type.

    Returns:
        List of StepSpec for Engine execution
    """
    overrides = overrides or {}

    steps = [
        _make_step(
            step_id="exercise_all",
            name="Exercise All Fetch Methods",
            fn_name="step_exercise_all_fetch_methods",
            params={
                "connection": "yggdrasil_db",
                "doc_id": "data_access_test:reference_doc",
                "selector_type": "ygg_test_reference",
            },
        ),
        _make_step(
            step_id="echo_confirm",
            name="All Methods Passed",
            fn_name="step_echo",
            params={"message": "All six fetch methods succeeded end-to-end!"},
            deps=["exercise_all"],
        ),
    ]

    return _apply_overrides(steps, overrides)


# ---------------------------------------------------------------------------
# Recipe: data_verify_limit_clamping
# ---------------------------------------------------------------------------


def data_verify_limit_clamping(
    overrides: dict[str, dict[str, Any]] | None = None,
) -> list[StepSpec]:
    """
    Verify that DataAccess clamps find() results to policy.max_limit.

    Uses the ``yggdrasil_db_clamped`` connection (data_access.options.max_limit: 2). The step
    requests 100 documents but expects at most 2 to be returned, proving
    the policy is enforced by CouchDBReadClient regardless of what the
    caller requests.

    Pre-condition: The ``yggdrasil`` database must contain at least 3
    documents with ``type == "ygg_test_reference"`` so that a non-clamped
    query would return more than ``max_limit``.

    Steps:
        1. verify_clamp: Request 100 docs, confirm at most 2 returned
        2. echo_pass: Confirm clamping enforcement passed

    Args:
        overrides: Optional param overrides by step_id.
            Override verify_clamp params (e.g. expected_max) if the
            connection's max_limit differs from the default 2.

    Returns:
        List of StepSpec for Engine execution
    """
    overrides = overrides or {}

    steps = [
        _make_step(
            step_id="verify_clamp",
            name="Verify Limit Clamping",
            fn_name="step_verify_limit_clamping",
            params={
                "connection": "yggdrasil_db_clamped",
                "selector_type": "ygg_test_reference",
                "request_limit": 100,
                "expected_max": 2,
            },
        ),
        _make_step(
            step_id="echo_pass",
            name="Clamping Enforced",
            fn_name="step_echo",
            params={"message": "max_limit clamping enforced — policy is working!"},
            deps=["verify_clamp"],
        ),
    ]

    return _apply_overrides(steps, overrides)


# ---------------------------------------------------------------------------
# Recipe: data_write_exec
# ---------------------------------------------------------------------------


def data_write_exec(
    overrides: dict[str, Any] | None = None,
) -> list[StepSpec]:
    """
    Generate a plan that writes a document to CouchDB at execution time.

    Proves that a step with execution write permission can call
    client.save() successfully via ctx.data.connection(). The write result
    (status, doc_id, old_rev, new_rev) is returned in step metrics and
    emitted as a step.write_result event so it is visible in the execution
    record.

    Steps:
        1. write_doc: Write data_access_test:write_result to test_realm_write_db
        2. echo_confirm: Confirm write completed (depends on write_doc)

    Args:
        overrides: Optional param overrides by step_id.

    Returns:
        List of StepSpec for Engine execution
    """
    overrides = overrides or {}

    steps = [
        _make_step(
            step_id="write_doc",
            name="Write Doc to DB",
            fn_name="step_write_to_db",
            params={
                "connection": "test_realm_write_db",
                "doc_id": "data_access_test:write_result",
                "mode": "upsert",
            },
        ),
        _make_step(
            step_id="echo_confirm",
            name="Confirm Write",
            fn_name="step_echo",
            params={"message": "Execution-time CouchDB write complete!"},
            deps=["write_doc"],
        ),
    ]

    return _apply_overrides(steps, overrides)


# ---------------------------------------------------------------------------
# Recipe: data_write_only_permission
# ---------------------------------------------------------------------------


def data_write_only_permission(
    overrides: dict[str, Any] | None = None,
) -> list[StepSpec]:
    """
    Generate a plan that proves "write" permission does not imply "read".

    Uses a connection configured with execution permissions ["write"] only.
    The first step calls put() — which must succeed. The second step calls
    get() on the same connection — which must raise DataAccessDeniedError.
    The second step succeeds only if that denial is raised.

    Steps:
        1. write_only_put: put() to test_realm_write_only_db — must succeed
        2. write_only_read_denied: get() on same connection — denial expected

    Args:
        overrides: Optional param overrides by step_id.

    Returns:
        List of StepSpec for Engine execution
    """
    overrides = overrides or {}

    steps = [
        _make_step(
            step_id="write_only_put",
            name="Write-Only Put",
            fn_name="step_write_to_db",
            params={
                "connection": "test_realm_write_db",
                "doc_id": "data_access_test:write_only_probe",
                "mode": "upsert",
            },
        ),
        _make_step(
            step_id="write_only_read_denied",
            name="Read Must Be Denied",
            fn_name="step_expect_read_denied",
            params={
                "connection": "test_realm_write_db",
                "doc_id": "data_access_test:write_only_probe",
            },
            deps=["write_only_put"],
        ),
    ]

    return _apply_overrides(steps, overrides)


# ---------------------------------------------------------------------------
# Recipe: data_write_no_id
# ---------------------------------------------------------------------------


def data_write_no_id(
    overrides: dict[str, Any] | None = None,
) -> list[StepSpec]:
    """
    Generate a plan that writes a document to CouchDB without supplying a _id.

    Uses the selector identity path of save() so CouchDB auto-generates the
    document ID on create. The resolved/generated doc_id appears in step metrics,
    along with identity="selector" and operation="upsert" to make the write
    path visible for verification.

    Steps:
        1. write_doc_no_id: Write with selector identity, no explicit _id
        2. echo_confirm: Confirm write completed (depends on write_doc_no_id)

    Args:
        overrides: Optional param overrides by step_id.

    Returns:
        List of StepSpec for Engine execution
    """
    overrides = overrides or {}

    steps = [
        _make_step(
            step_id="write_doc_no_id",
            name="Write Doc Without _id",
            fn_name="step_write_to_db_no_id",
            params={
                "connection": "test_realm_write_db",
                "mode": "upsert",
            },
        ),
        _make_step(
            step_id="echo_confirm",
            name="Confirm Write",
            fn_name="step_echo",
            params={"message": "CouchDB auto-generated ID write complete!"},
            deps=["write_doc_no_id"],
        ),
    ]

    return _apply_overrides(steps, overrides)


# ---------------------------------------------------------------------------
# Helper: Apply parameter overrides
# ---------------------------------------------------------------------------


def _apply_overrides(
    steps: list[StepSpec],
    overrides: dict[str, dict[str, Any]],
) -> list[StepSpec]:
    """
    Apply parameter overrides to steps by step_id.

    Args:
        steps: List of StepSpec to modify
        overrides: Dict mapping step_id to param dict overrides

    Returns:
        Modified list with overrides applied
    """
    if not overrides:
        return steps

    for step in steps:
        if step.step_id in overrides:
            # Merge overrides into existing params
            step.params.update(overrides[step.step_id])

    return steps


# ---------------------------------------------------------------------------
# Recipe registry
# ---------------------------------------------------------------------------

RECIPES: dict[str, Any] = {
    "happy_path": happy_path,
    "random_fail": random_fail,
    "fail_fast": fail_fast,
    "fail_mid_plan": fail_mid_plan,
    "long_running": long_running,
    "artifact_write": artifact_write,
    "branch_failure": branch_failure,
    "branch_failure_metadata_required": branch_failure_metadata_required,
    "data_fetch_exec": data_fetch_exec,
    "data_access_denied": data_access_denied,
    "data_fetch_all_methods": data_fetch_all_methods,
    "data_verify_limit_clamping": data_verify_limit_clamping,
    "data_write_exec": data_write_exec,
    "data_write_only_permission": data_write_only_permission,
    "data_write_no_id": data_write_no_id,
}

# Failure policy of a recipe's plans when the scenario document names none.
# Recipes not listed here build fail_fast plans.
RECIPE_FAILURE_POLICIES: dict[str, str] = {
    "branch_failure": CONTINUE_INDEPENDENT_POLICY,
    "branch_failure_metadata_required": CONTINUE_INDEPENDENT_POLICY,
}


def default_failure_policy(recipe_name: str | None) -> str:
    """
    Get the failure policy a recipe's plans run under by default.

    Args:
        recipe_name: The scenario's recipe; None for custom steps.

    Returns:
        str: The policy RECIPE_FAILURE_POLICIES names for the recipe, else
        fail_fast.
    """
    if recipe_name is None:
        return DEFAULT_FAILURE_POLICY
    return RECIPE_FAILURE_POLICIES.get(recipe_name, DEFAULT_FAILURE_POLICY)


def get_recipe(name: str):
    """
    Get recipe function by name.

    Args:
        name: Recipe name (e.g., "happy_path")

    Returns:
        Recipe builder function

    Raises:
        KeyError: If recipe name not found
    """
    if name not in RECIPES:
        raise KeyError(
            f"Unknown test realm recipe: {name}. " f"Available: {list(RECIPES.keys())}"
        )
    return RECIPES[name]
