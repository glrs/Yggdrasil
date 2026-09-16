from __future__ import annotations

import hashlib
import inspect
import json
import logging
import os
import sys
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from lib.core_utils.logging_utils import custom_logger
from lib.core_utils.runtime_paths import resolve_work_root
from yggdrasil.flow.errors import (
    OrchestrationError,
    PermanentStepError,
    PreflightValidationError,
    TransientStepError,
)
from yggdrasil.flow.events.emitter import EventEmitter, FileSpoolEmitter
from yggdrasil.flow.model import Plan, StepResult, StepSpec, validate_failure_policy
from yggdrasil.flow.step import StepContext
from yggdrasil.flow.utils.callable_ref import resolve_callable
from yggdrasil.flow.utils.hash import dirhash_stats, sha256_file
from yggdrasil.flow.utils.typing_coerce import coerce_params_to_signature_types
from yggdrasil.flow.utils.ygg_time import utcnow_compact, utcnow_iso

logger = custom_logger(__name__)


# ------------ Utilities ------------


def _json_sha256(data: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(data, sort_keys=True, default=str).encode()
    ).hexdigest()


def _short(h: str, n: int = 4) -> str:
    return h[:n]


def _new_run_id() -> str:
    return f"run_{utcnow_compact()}_{uuid.uuid4().hex[:6]}"


def _looks_like_path(v: Any) -> bool:
    return isinstance(v, str) and ("/" in v or "\\" in v)


def _lint_missing_inputs(spec: StepSpec, fn: Any) -> None:
    """Warn if a step has path-like params but no declared inputs."""
    has_declared = bool(spec.inputs) or bool(getattr(fn, "_input_keys", ()))
    if has_declared:
        return
    suspicious = [k for k, v in spec.params.items() if _looks_like_path(v)]
    if suspicious:
        logger.warning(
            '%s has path-like params "%s" but no declared inputs ',
            spec.step_id,
            suspicious,
        )


# Sentinel bound in place of the StepContext when checking that a step's params
# match its signature. Never called, never passed to author code.
_CTX_PLACEHOLDER = object()


@contextmanager
def _orchestration_boundary(description: str) -> Iterator[None]:
    """Classify failures of engine-owned bookkeeping as OrchestrationError.

    Wraps operations on Yggdrasil's own state — the plan file, step
    directories, cache markers, event publication — so that when they fail the
    attempt aborts instead of the failure being contained as a realm's ordinary
    step failure. Deliberately not used around author code: a realm's own file
    or DataAccess error is not systemic merely because it is an OSError.

    Args:
        description: What was being attempted, for the error message.

    Yields:
        None: Control to the wrapped operation.

    Raises:
        OrchestrationError: If the wrapped operation fails. An OrchestrationError
            raised within is re-raised unchanged, so nested boundaries do not
            wrap the same failure twice.
    """
    try:
        yield
    except OrchestrationError:
        raise
    except Exception as exc:
        raise OrchestrationError(f"{description}: {exc}") from exc


def _find_cycle(steps: list[StepSpec]) -> list[str] | None:
    """Return one dependency cycle, or None if the graph is acyclic.

    Iterative depth-first search that visits steps and their dependencies in
    original plan order, so the cycle reported for a given plan is always the
    same one — an operator comparing two runs sees a stable diagnostic.

    Args:
        steps: The plan's steps, in plan order.

    Returns:
        list[str] | None: The cycle as a path whose first and last entries are
        the same step ID, or None when no cycle exists.
    """
    deps_by_id = {spec.step_id: spec.deps for spec in steps}
    # 0 = unvisited, 1 = on the current path, 2 = fully explored
    state: dict[str, int] = {}

    for root in deps_by_id:
        if state.get(root, 0) != 0:
            continue
        # Each frame is (step_id, iterator over its remaining dependencies).
        stack: list[tuple[str, Iterator[str]]] = [(root, iter(deps_by_id[root]))]
        path: list[str] = [root]
        state[root] = 1

        while stack:
            node, remaining = stack[-1]
            advanced = False
            for dep in remaining:
                if dep not in deps_by_id:
                    continue  # unknown deps are reported separately
                if state.get(dep, 0) == 1:
                    return path[path.index(dep) :] + [dep]
                if state.get(dep, 0) == 0:
                    state[dep] = 1
                    path.append(dep)
                    stack.append((dep, iter(deps_by_id[dep])))
                    advanced = True
                    break
            if not advanced:
                state[node] = 2
                path.pop()
                stack.pop()

    return None


def _validate_param_binding(spec: StepSpec, fn: Any) -> None:
    """Reject params that cannot bind to a step's signature.

    Catches missing required arguments and unexpected keywords without invoking
    the step body, so a malformed later step cannot be discovered only after
    earlier steps have already run. Best effort: a signature that cannot be
    introspected is skipped rather than rejected, and binding checks names, not
    types.

    Args:
        spec: The step whose params are checked.
        fn: The resolved step callable.

    Raises:
        PreflightValidationError: If spec.params cannot bind to fn's signature.
    """
    try:
        # functools.wraps sets __wrapped__, so this is the real step signature,
        # not the decorator's (ctx, **kwargs).
        signature = inspect.signature(fn)
    except (TypeError, ValueError):
        # Not introspectable (builtins, some C callables). Leave it to call time
        # rather than rejecting a plan we cannot actually prove is malformed.
        return

    try:
        signature.bind(_CTX_PLACEHOLDER, **spec.params)
    except TypeError as exc:
        raise PreflightValidationError(
            f"Params for step '{spec.step_id}' do not match "
            f"'{spec.fn_ref}'{signature}: {exc}"
        ) from exc


def _parse_fn_ref(fn_ref: str) -> tuple[str, str] | None:
    """Split a step reference into (module, attribute) without importing it.

    Mirrors the two syntaxes ``resolve_callable`` accepts, so the module name
    used for diagnosis is the one that will actually be imported: "pkg.mod:fn"
    imports "pkg.mod", and "pkg.mod.fn" also imports "pkg.mod".

    Args:
        fn_ref: The step's function reference.

    Returns:
        tuple[str, str] | None: (module_name, attribute), or None when fn_ref is
        not a usable reference at all.
    """
    if ":" in fn_ref:
        module_name, attribute = fn_ref.split(":", 1)
    elif "." in fn_ref:
        module_name, attribute = fn_ref.rsplit(".", 1)
    else:
        return None
    if not module_name or not attribute:
        return None
    return module_name, attribute


def _resolve_step_callable(spec: StepSpec) -> Callable[..., Any]:
    """Resolve one step's callable, distinguishing defects from infrastructure.

    Only a *confirmed* defect in the reference becomes a preflight rejection.
    Resolving a reference imports a module, and importing runs arbitrary code:
    a module that raises while initializing is a broken environment, not a
    malformed plan. Getting that wrong is costly rather than merely untidy,
    because a preflight rejection is definitive and would retire an execution
    request that a later attempt could have completed.

    The three stages are therefore diagnosed separately, and each needs its own
    positive evidence:

    1. Reference syntax, checked here before anything is imported.
    2. Module import - a plan defect only when the missing module is the one
       the reference names, never when some dependency of a real module is
       missing.
    3. Attribute lookup - a plan defect only once the module has actually
       imported. An exception raised *during* import leaves no module behind in
       sys.modules, which is what separates the two cases.

    Anything else, and every ambiguous case, stays nonterminal.

    Args:
        spec: The step whose fn_ref is resolved.

    Returns:
        Callable: The resolved step function.

    Raises:
        PreflightValidationError: If fn_ref is not a usable reference, names a
            module that does not exist, or names an attribute that an
            otherwise-importable module does not define.
        OrchestrationError: If resolution fails for any other reason, including
            any failure raised while importing a real module.
    """
    if not isinstance(spec.fn_ref, str):
        raise PreflightValidationError(
            f"Malformed fn_ref for step '{spec.step_id}': expected a "
            f"'module:function' string, got {type(spec.fn_ref).__name__}."
        )

    parsed = _parse_fn_ref(spec.fn_ref)
    if parsed is None:
        raise PreflightValidationError(
            f"Malformed fn_ref for step '{spec.step_id}': {spec.fn_ref!r} "
            f"is not a 'module:function' or 'module.function' reference."
        )
    module_name, attribute = parsed

    try:
        return resolve_callable(spec.fn_ref)
    except ModuleNotFoundError as exc:
        # exc.name is the module that could not be found. It is the reference's
        # own module (or a package prefix of it) when the reference is wrong,
        # and some other module when a real module has a broken dependency.
        missing = exc.name or ""
        if missing and (
            module_name == missing or module_name.startswith(f"{missing}.")
        ):
            raise PreflightValidationError(
                f"Unresolvable fn_ref for step '{spec.step_id}': "
                f"'{spec.fn_ref}' names module '{missing}', which does not exist."
            ) from exc
        # Includes a missing exc.name: no evidence, so keep it nonterminal.
        raise OrchestrationError(
            f"Importing the module for step '{spec.step_id}' "
            f"(fn_ref='{spec.fn_ref}') failed: {exc}"
        ) from exc
    except AttributeError as exc:
        if module_name in sys.modules:
            # The module imported, so this came from the attribute lookup.
            raise PreflightValidationError(
                f"Unresolvable fn_ref for step '{spec.step_id}': module "
                f"'{module_name}' does not define '{attribute}'."
            ) from exc
        # Raised while the module was initializing: a broken environment.
        raise OrchestrationError(
            f"Importing the module for step '{spec.step_id}' "
            f"(fn_ref='{spec.fn_ref}') failed: {exc!r}"
        ) from exc
    except Exception as exc:
        raise OrchestrationError(
            f"Resolving the callable for step '{spec.step_id}' "
            f"(fn_ref='{spec.fn_ref}') failed: {exc!r}"
        ) from exc


def _default_fingerprint(spec: StepSpec, fn: Any) -> str:
    """
    Fingerprint = sha256(JSON of params + digests of declared inputs).
    Inputs are either:
      - spec.inputs (planner-provided), or
      - fn._input_keys taken from params (decorator-declared), or
      - none (params-only fallback).
    """
    enriched: dict[str, Any] = {"params": spec.params}

    # (a) planner-provided inputs
    declared: dict[str, str] = dict(spec.inputs)

    # (b) else, use step-declared input_keys to pull paths from params
    if not declared:
        input_keys = getattr(fn, "_input_keys", ())

        for key in input_keys:
            value = spec.params.get(key)
            # Accept Path or str (defensive: accept Path values if provided)
            if isinstance(value, str | Path):
                declared[key] = str(value)

    # Hash only declared inputs
    for key, path_str in declared.items():
        p = Path(path_str)
        if p.is_dir():
            enriched[f"input:{key}:dirhash"] = dirhash_stats(p)
        elif p.is_file():
            enriched[f"input:{key}:sha256"] = f"sha256:{sha256_file(p)}"
        else:
            enriched[f"input:{key}:missing"] = True

    return f"sha256:{_json_sha256(enriched)}"


# ------------ Engine ------------


class Engine:
    """
    Minimal sequential executor with:
    - plan dir + plan.json
    - per-step workdir
    - fingerprint + cache skip (file-based)
    - event spool emission via StepContext (handled by @step decorator)
    """

    def __init__(
        self,
        work_root: str | Path | None = None,
        emitter: EventEmitter | None = None,
        logger: logging.Logger | None = None,
    ):
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")
        # Default resolution ($YGG_WORK_ROOT → mode default) is centralized
        # in lib.core_utils.runtime_paths.
        self.work_root = Path(work_root) if work_root else resolve_work_root()
        self.emitter = emitter or FileSpoolEmitter()

    def _emit_engine_event(self, description: str, event: dict[str, Any]) -> None:
        """Publish an engine-level event directly, classifying failure.

        Some engine events have no StepContext to route through — the cache-hit
        skip and the retry-unimplemented diagnostic are emitted from the engine
        itself. They are the same reporting infrastructure as ctx.emit() and get
        the same classification.

        Args:
            description: What was being published, for the error message.
            event: The event payload.

        Raises:
            OrchestrationError: If the emitter fails to publish.
        """
        with _orchestration_boundary(f"Publishing {description}"):
            self.emitter.emit(event)

    def _scope_dir(self, plan_dir: Path) -> Path:
        return plan_dir.parent

    def _plan_dir(self, plan: Plan) -> Path:
        return self.work_root / plan.plan_id

    def _step_dir(self, plan_dir: Path, spec: StepSpec) -> Path:
        return plan_dir / spec.step_id

    def _write_plan_file(self, plan: Plan, plan_dir: Path) -> None:
        """Write the plan snapshot into the plan directory.

        Args:
            plan: The plan being executed.
            plan_dir: Directory for this plan's execution state.

        Raises:
            OrchestrationError: If the snapshot cannot be written.
        """
        with _orchestration_boundary(f"Writing plan.json for plan '{plan.plan_id}'"):
            plan_dir.mkdir(parents=True, exist_ok=True)
            (plan_dir / "plan.json").write_text(
                json.dumps(
                    {
                        "plan_id": plan.plan_id,
                        "realm": plan.realm,
                        "scope": plan.scope,
                        "steps": [spec.__dict__ for spec in plan.steps],
                        "failure_policy": plan.failure_policy,
                    },
                    indent=2,
                    sort_keys=True,
                    default=str,
                )
            )

    def _topo_validate(self, plan: Plan) -> None:
        """Validate the plan's dependency graph.

        Checks step identity, dependency references, and acyclicity. Forward
        references stay valid: the engine schedules a valid graph rather than
        requiring the serialized list to be topologically sorted.

        Each of these was unenforceable before. Duplicate IDs were silently
        collapsed by a dict comprehension that kept the last one; a
        self-dependency and every edge of a cycle satisfy "this ID exists in the
        plan", so the old existence check could never detect either.

        Args:
            plan: The plan to validate.

        Raises:
            PreflightValidationError: If any step ID is empty or duplicated, a
                dependency is unknown, a step depends on itself, or the graph
                contains a cycle.
        """
        by_id: dict[str, StepSpec] = {}
        for position, spec in enumerate(plan.steps):
            if not spec.step_id:
                raise PreflightValidationError(
                    f"Step at position {position} in plan '{plan.plan_id}' has an "
                    f"empty step_id. Every step needs a unique, nonempty identity."
                )
            if spec.step_id in by_id:
                raise PreflightValidationError(
                    f"Duplicate step_id '{spec.step_id}' in plan '{plan.plan_id}'. "
                    f"Step IDs must be unique; dependencies cannot name one of two "
                    f"identically identified steps."
                )
            by_id[spec.step_id] = spec

        for spec in plan.steps:
            missing = [d for d in spec.deps if d not in by_id]
            if missing:
                raise PreflightValidationError(
                    f"Unknown deps in {spec.step_id}: {missing}"
                )
            if spec.step_id in spec.deps:
                raise PreflightValidationError(
                    f"Self-dependency in {spec.step_id}: a step cannot require its "
                    f"own success as a prerequisite."
                )

        cycle = _find_cycle(plan.steps)
        if cycle:
            raise PreflightValidationError(
                f"Dependency cycle in plan '{plan.plan_id}': "
                f"{' -> '.join(cycle)}. No step in a cycle can ever become runnable."
            )

    def _preflight(self, plan: Plan) -> dict[str, Callable[..., Any]]:
        """Validate the whole plan before any step's side effects begin.

        Runs graph validation, policy validation, and callable
        resolution/registration/binding as one pass, so that every structural
        problem is found before the first step runs. Callable resolution used to
        happen per step, interleaved with execution, which meant a bad reference
        on the *last* step was discovered only after every earlier step had
        already run.

        Resolved callables are returned rather than re-resolved during execution:
        resolution is the engine's single lookup of each step's function.

        Args:
            plan: The plan about to be executed.

        Returns:
            dict[str, Callable]: Resolved step callables, keyed by step ID.

        Raises:
            PreflightValidationError: If the plan is structurally invalid, its
                failure policy is unknown, or a step's callable is unresolvable,
                non-callable, undecorated, or cannot bind its params.
            OrchestrationError: If resolving a callable fails for a reason that
                is not a defect of the plan, such as a real module raising while
                being imported.
        """
        self._topo_validate(plan)

        try:
            validate_failure_policy(plan.failure_policy)
        except ValueError as exc:
            raise PreflightValidationError(f"Plan '{plan.plan_id}': {exc}") from exc

        resolved: dict[str, Callable[..., Any]] = {}
        for spec in plan.steps:
            fn = _resolve_step_callable(spec)
            if not callable(fn):
                raise PreflightValidationError(
                    f"Step '{spec.step_id}' resolves to a non-callable "
                    f"{type(fn).__name__} (fn_ref='{spec.fn_ref}'). Carrying step "
                    f"metadata does not make an object executable."
                )
            if not hasattr(fn, "_step_name"):
                raise PreflightValidationError(
                    f"Undecorated step function detected for step '{spec.step_id}' "
                    f"(fn_ref='{spec.fn_ref}'). "
                    f"Decorate it with '@step' from 'yggdrasil.flow.step'."
                )
            _validate_param_binding(spec, fn)
            resolved[spec.step_id] = fn
        return resolved

    def run(self, plan: Plan) -> None:
        """Execute a plan's steps.

        Preflight runs before anything is written, so a rejected plan leaves
        existing plan snapshots, artifacts and cache markers untouched.

        Args:
            plan: The plan to execute.

        Raises:
            PreflightValidationError: If the plan is rejected by preflight.
            OrchestrationError: If engine bookkeeping or event publication fails.
            StepError: If a step fails.
        """
        # Validate everything before the first side effect of any kind — this
        # includes the plan snapshot below, which used to be written first.
        resolved = self._preflight(plan)

        plan_dir = self._plan_dir(plan)
        self._write_plan_file(plan, plan_dir)

        for spec in plan.steps:
            step_dir = self._step_dir(plan_dir, spec)
            with _orchestration_boundary(
                f"Creating work directory for step '{spec.step_id}'"
            ):
                step_dir.mkdir(parents=True, exist_ok=True)

            # Resolution, registration and binding were all settled in preflight.
            fn = resolved[spec.step_id]
            _lint_missing_inputs(spec, fn)

            run_id = _new_run_id()

            # compute fingerprint and check cache // pass fn so we can read _input_keys
            fingerprint = _default_fingerprint(spec, fn)
            fp_file = step_dir / "success.fingerprint"

            with _orchestration_boundary(
                f"Reading the cache marker for step '{spec.step_id}'"
            ):
                cache_hit = (
                    fp_file.exists() and fp_file.read_text().strip() == fingerprint
                )

            if cache_hit:
                # emit a 'skipped' event
                self._emit_engine_event(
                    f"step.skipped for step '{spec.step_id}'",
                    {
                        "type": "step.skipped",
                        "realm": plan.realm,
                        "scope": plan.scope,
                        "plan_id": plan.plan_id,
                        "step_id": spec.step_id,
                        "step_name": spec.name,
                        "fingerprint": fingerprint,
                        "seq": 1,
                        "eid": str(uuid.uuid4()),
                        "ts": utcnow_iso(),
                        "_spool_path": {
                            "realm": plan.realm,
                            "plan_id": plan.plan_id,
                            "step_id": spec.step_id,
                            "run_id": run_id,
                            "filename": "0001_step_skipped.json",
                        },
                    },
                )
                continue

            # build context and call the step function
            from yggdrasil.flow.data_access import DataAccess, DataAccessTraceContext

            trace_ctx = DataAccessTraceContext(
                realm=plan.realm,
                phase="execution",
                plan_id=plan.plan_id,
                run_id=run_id,
                step_id=spec.step_id,
                step_name=spec.name,
                scope=spec.scope or plan.scope,
                emitter=self.emitter,
            )

            ctx = StepContext(
                realm=plan.realm,
                scope=spec.scope or plan.scope,
                plan_id=plan.plan_id,
                step_id=spec.step_id,
                step_name=spec.name,
                workdir=step_dir,
                scope_dir=self._scope_dir(plan_dir),
                emitter=self.emitter,
                run_mode=os.environ.get("YGG_RUN_MODE", "auto"),
                fingerprint=fingerprint,
                run_id=run_id,
                data=DataAccess(
                    plan.realm,
                    phase="execution",
                    trace_context=trace_ctx,
                ),
            )

            try:
                # Coerce string params to Path where function signature expects it
                coerced_params = coerce_params_to_signature_types(fn, spec.params)
                # returns StepResult (decorator wraps emissions)
                result = fn(ctx, **coerced_params)
            except TransientStepError as e:
                # The @step wrapper has already emitted "step.failed".
                # Add a precise diagnostic so operators know retry isn't wired yet.
                # TODO: Implement retry.
                self._emit_engine_event(
                    f"step.retry_unimplemented for step '{spec.step_id}'",
                    {
                        "type": "step.retry_unimplemented",
                        "realm": plan.realm,
                        "scope": plan.scope,
                        "plan_id": plan.plan_id,
                        "step_id": spec.step_id,
                        "error": str(e),
                        "kind": "transient",
                        "_spool_path": {
                            "realm": plan.realm,
                            "plan_id": plan.plan_id,
                            "step_id": spec.step_id,
                            "run_id": run_id,
                            "filename": "retry_unimplemented.json",
                        },
                    },
                )
                # Treat transient as permanent until retries are implemented.
                raise PermanentStepError(
                    f"Retry not implemented for transient failure: {e}"
                ) from e

            if result is not None and not isinstance(result, StepResult):
                self._logger.warning(
                    "Step %s returned %r (expected StepResult or None)",
                    spec.step_id,
                    type(result),
                )

            # Success-publication ordering contract (PRD §9). The owner of this
            # sequence is the engine, and both this phase's successors implement
            # parts of it, so the order is recorded here once:
            #
            #   required-output validation
            #     -> step-success publication
            #     -> atomic marker replacement
            #     -> admission of successors
            #
            # A step-success event may already have been published when the
            # marker replacement that follows it fails. That failure aborts the
            # attempt as an OrchestrationError; it is the documented, accepted
            # inconsistency window, not a bug to design around. A failed marker
            # write must never leave a reusable success marker behind.
            with _orchestration_boundary(
                f"Writing the cache marker for step '{spec.step_id}'"
            ):
                # mark success in cache after function returns without exception
                fp_file.write_text(fingerprint)
