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
from yggdrasil.core.scheduler import DependencyScheduler
from yggdrasil.flow.attempt import AttemptContext
from yggdrasil.flow.errors import (
    AttemptCancelledError,
    EventPublicationError,
    OrchestrationError,
    PermanentStepError,
    PreflightValidationError,
    StepError,
    TransientStepError,
)
from yggdrasil.flow.events.emitter import EventEmitter, FileSpoolEmitter
from yggdrasil.flow.model import (
    CONTINUE_INDEPENDENT_POLICY,
    FAIL_FAST_POLICY,
    Plan,
    StepResult,
    StepSpec,
    validate_failure_policy,
)
from yggdrasil.flow.outcomes import (
    AttemptDiagnostic,
    AttemptReport,
    StepFailure,
    StepOutcome,
    TerminationReason,
)
from yggdrasil.flow.step import StepContext
from yggdrasil.flow.utils.callable_ref import resolve_callable
from yggdrasil.flow.utils.hash import dirhash_stats, sha256_file
from yggdrasil.flow.utils.typing_coerce import coerce_params_to_signature_types
from yggdrasil.flow.utils.ygg_time import utcnow_compact, utcnow_iso

logger = custom_logger(__name__)

# Type of the one plan-level event published when an execution attempt ends.
ATTEMPT_REPORT_EVENT = "plan.attempt_report"


# ------------ Utilities ------------


def _json_sha256(data: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(data, sort_keys=True, default=str).encode()
    ).hexdigest()


def _short(h: str, n: int = 4) -> str:
    return h[:n]


def _new_run_id() -> str:
    return f"run_{utcnow_compact()}_{uuid.uuid4().hex[:6]}"


def _new_execution_id() -> str:
    """Allocate an identifier for one execution attempt of a plan.

    Timestamp-prefixed like run IDs, so identifiers sort chronologically while
    the clock moves forward, with a full UUID suffix for uniqueness. Nothing
    here protects that ordering against a clock that moves backwards.

    Returns:
        str: A new execution ID, e.g. "exec_20260916T101500123456Z_<uuid hex>".
    """
    return f"exec_{utcnow_compact()}_{uuid.uuid4().hex}"


def _step_failure(step_id: str, exc: Exception) -> StepFailure:
    """Describe a contained step failure the way the step wrapper reports it.

    Mirrors the ``step.failed`` payload: a StepError carries its kind, code and
    advice, and any other exception counts as permanent. The exception class is
    kept as well, because ``str(exc)`` alone often loses it.

    Args:
        step_id: The step that failed.
        exc: The exception it failed with.

    Returns:
        StepFailure: The failure detail for the attempt report.
    """
    if isinstance(exc, StepError):
        return StepFailure(
            step_id=step_id,
            error=str(exc),
            kind="transient" if isinstance(exc, TransientStepError) else "permanent",
            code=exc.code,
            advice=exc.advice,
            error_type=type(exc).__name__,
        )
    return StepFailure(step_id=step_id, error=str(exc), error_type=type(exc).__name__)


def _unplanned_stop_reason(exc: BaseException) -> TerminationReason:
    """Classify an attempt exit that no deliberate early exit accounted for.

    Preflight rejection, cancellation and fail-fast termination each name their
    own reason before raising, so anything reaching this function was not one of
    them. An Exception here is infrastructure failing, or an engine defect
    outside any step's failure containment; either way the attempt could not be
    carried out reliably. Anything that is not an Exception - KeyboardInterrupt,
    SystemExit - is an external interruption.

    Args:
        exc: The exception ending the attempt.

    Returns:
        TerminationReason: ORCHESTRATION_ERROR for an Exception, CANCELLED for
        any other BaseException.
    """
    if isinstance(exc, Exception):
        return TerminationReason.ORCHESTRATION_ERROR
    return TerminationReason.CANCELLED


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
def _orchestration_boundary(
    description: str, *, error_type: type[OrchestrationError] = OrchestrationError
) -> Iterator[None]:
    """Classify failures of engine-owned bookkeeping as OrchestrationError.

    Wraps operations on Yggdrasil's own state — the plan file, step
    directories, cache markers, event publication — so that when they fail the
    attempt aborts instead of the failure being contained as a realm's ordinary
    step failure. Deliberately not used around author code: a realm's own file
    or DataAccess error is not systemic merely because it is an OSError.

    Args:
        description: What was being attempted, for the error message.
        error_type: The OrchestrationError type to raise; EventPublicationError
            when the wrapped operation is event publication.

    Yields:
        None: Control to the wrapped operation.

    Raises:
        OrchestrationError: If the wrapped operation fails, as ``error_type``.
            An OrchestrationError raised within is re-raised unchanged, so
            nested boundaries do not wrap the same failure twice.
    """
    try:
        yield
    except OrchestrationError:
        raise
    except Exception as exc:
        raise error_type(f"{description}: {exc}") from exc


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
    Sequential, dependency-driven plan executor with:
    - whole-plan preflight before any side effect
    - dependency scheduling: a step starts only once every step it depends on
      has succeeded or been reused, earliest in plan order first
    - failure policies: ``fail_fast`` stops at the first step failure;
      ``continue_independent`` blocks only the steps that depend on it
    - plan dir + plan.json
    - per-step workdir
    - fingerprint + cache skip (file-based)
    - event spool emission via StepContext (handled by @step decorator), plus
      one plan-level attempt report per attempt

    An Engine is long-lived and shared across plans, so it holds no attempt
    state: each attempt's outcomes live in its AttemptContext and a scheduler
    built for that attempt alone.
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
            EventPublicationError: If the emitter fails to publish.
        """
        with _orchestration_boundary(
            f"Publishing {description}", error_type=EventPublicationError
        ):
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

    def run(self, plan: Plan) -> AttemptReport | None:
        """Execute a plan's steps in dependency order.

        Preflight runs before anything is written, so a rejected plan leaves
        existing plan snapshots, artifacts and cache markers untouched.

        The return contract depends on the plan's failure policy. Static typing
        cannot express that, so it is dispatched at runtime:

        - ``fail_fast``: returns None on success and raises on the first step
          failure, exactly as before failure policies existed.
        - ``continue_independent``: runs everything whose prerequisites
          succeeded and returns the drained report. Its outcome may well be
          failed — a returned report is not in itself a successful execution.

        Callers that need the report under both policies, or after an attempt
        that raised, use :meth:`_run_attempt` with a context of their own.

        Args:
            plan: The plan to execute.

        Returns:
            AttemptReport | None: The drained report for a
            ``continue_independent`` plan; None for a successful ``fail_fast``
            plan.

        Raises:
            PreflightValidationError: If the plan is rejected by preflight.
            OrchestrationError: If engine bookkeeping or event publication fails.
            StepError: If a step fails under ``fail_fast``. A TransientStepError
                surfaces as a PermanentStepError, because retries are not
                implemented.
            Exception: Any other exception a step raised under ``fail_fast``,
                unchanged.
        """
        context = AttemptContext.for_plan(plan, execution_id=_new_execution_id())
        report = self._run_attempt(plan, context=context)
        if report.failure_policy == CONTINUE_INDEPENDENT_POLICY:
            return report
        return None

    def _run_attempt(self, plan: Plan, *, context: AttemptContext) -> AttemptReport:
        """Run one execution attempt, recording what happens into its context.

        The one scheduler and the one exit path for both failure policies. The
        policy changes only the response to an ordinary step failure: under
        ``fail_fast`` the attempt stops; under ``continue_independent`` the
        failure is recorded, every step depending on it is blocked, and all
        other runnable work continues.

        A normal return means the attempt drained: every planned step reached a
        terminal outcome. Every other ending raises, but only after the report
        in ``context`` has been closed with the outcomes determined so far, the
        diagnostics, and a termination reason that tells the endings apart — and
        after that report has been published. The caller therefore holds a
        readable report whichever way the attempt ends. Publishing the report
        never consumes an execution request; that decision belongs to the caller.

        Cancellation is cooperative: it is checked only between steps, and only
        while runnable work remains, so it never interrupts a running step and a
        signal arriving after the last step does not undo a drained attempt.

        Args:
            plan: The plan to execute.
            context: A fresh context opened for ``plan``, used by no other attempt.

        Returns:
            AttemptReport: The drained report, ``context.report``. Its outcome is
            failed if any step failed or was blocked.

        Raises:
            PreflightValidationError: If preflight rejects the plan
                (PREFLIGHT_REJECTED).
            AttemptCancelledError: If cancellation was signaled while runnable
                work remained (CANCELLED).
            OrchestrationError: If infrastructure failed or a scheduler invariant
                was violated (ORCHESTRATION_ERROR); or if ``context`` does not
                belong to a fresh attempt at ``plan``, in which case the report
                is left untouched and nothing runs.
            EventPublicationError: If publishing the report itself failed. When
                the attempt was already ending with an exception, that original
                exception is named in the message and kept in the chain.
            StepError: Under ``fail_fast``, the first step failure (FAILED_FAST).
                A TransientStepError surfaces as a PermanentStepError.
            Exception: Under ``fail_fast``, any other first step failure,
                unchanged (FAILED_FAST).
            BaseException: An interruption such as KeyboardInterrupt, unchanged
                (CANCELLED).
        """
        self._check_attempt_context(plan, context)
        report = context.report
        scheduler: DependencyScheduler | None = None
        # Set immediately before each deliberate early exit. An exception that
        # escapes with this still unset is an unplanned abort.
        stop_reason: TerminationReason | None = None

        try:
            try:
                resolved = self._preflight(plan)
            except PreflightValidationError:
                stop_reason = TerminationReason.PREFLIGHT_REJECTED
                raise

            plan_dir = self._plan_dir(plan)
            self._write_plan_file(plan, plan_dir)
            scheduler = DependencyScheduler(plan.steps, report)

            while scheduler.has_runnable():
                if context.cancellation_requested:
                    stop_reason = TerminationReason.CANCELLED
                    raise AttemptCancelledError(
                        f"Attempt '{report.execution_id}' for plan '{plan.plan_id}' "
                        f"was cancelled; no further steps were started."
                    )

                spec = scheduler.start_next()
                try:
                    outcome = self._execute_step(
                        plan, spec, resolved[spec.step_id], plan_dir
                    )
                except OrchestrationError:
                    raise
                except Exception as exc:
                    scheduler.record_failure(_step_failure(spec.step_id, exc))
                    if plan.failure_policy == FAIL_FAST_POLICY:
                        stop_reason = TerminationReason.FAILED_FAST
                        if isinstance(exc, TransientStepError):
                            # Treat transient as permanent until retries are implemented.
                            raise PermanentStepError(
                                f"Retry not implemented for transient failure: {exc}"
                            ) from exc
                        raise
                    blocked = scheduler.block_dependents(spec.step_id)
                    self._logger.warning(
                        "Step '%s' of plan '%s' failed (%s: %s); continuing "
                        "independent work. Blocked dependents: %s",
                        spec.step_id,
                        plan.plan_id,
                        type(exc).__name__,
                        exc,
                        blocked or "none",
                    )
                    continue

                scheduler.record_success(spec.step_id, outcome)

            scheduler.ensure_drained()
            self._close_report(report, scheduler, TerminationReason.COMPLETED)
        except BaseException as exc:
            self._close_report(
                report,
                scheduler,
                stop_reason or _unplanned_stop_reason(exc),
                cause=exc,
            )
            self._publish_attempt_report(plan, report, cause=exc)
            raise

        self._publish_attempt_report(plan, report, cause=None)
        return report

    def _check_attempt_context(self, plan: Plan, context: AttemptContext) -> None:
        """Refuse a context that does not belong to a fresh attempt at ``plan``.

        The report is an attempt's only record. Recording one plan's outcomes
        into a report opened for a different plan or step inventory, or into a
        report an earlier attempt already filled, would make it describe
        something that never happened. Such a context is a caller defect, so it
        is refused before anything runs and left exactly as it was.

        Args:
            plan: The plan about to be executed.
            context: The context the caller supplied.

        Raises:
            OrchestrationError: If the context's report was opened for another
                plan, step inventory or failure policy, or has already been used.
        """
        report = context.report
        problems: list[str] = []
        if report.plan_id != plan.plan_id:
            problems.append(f"its report was opened for plan '{report.plan_id}'")
        if report.step_ids != [spec.step_id for spec in plan.steps]:
            problems.append("its step inventory differs from the plan's steps")
        if report.failure_policy != plan.failure_policy:
            problems.append(
                f"its failure policy is '{report.failure_policy}', "
                f"not '{plan.failure_policy}'"
            )
        if report.is_finished or report.step_outcomes or report.diagnostic:
            problems.append("its report was already used by another attempt")
        if problems:
            raise OrchestrationError(
                f"Attempt context '{report.execution_id}' cannot run plan "
                f"'{plan.plan_id}': {'; '.join(problems)}."
            )

    def _execute_step(
        self,
        plan: Plan,
        spec: StepSpec,
        fn: Callable[..., Any],
        plan_dir: Path,
    ) -> StepOutcome:
        """Reuse or execute one step whose prerequisites have all been satisfied.

        Called only once the scheduler has established readiness, so no cache
        marker is ever read for a step whose prerequisites did not all succeed
        in this attempt.

        Failures are classified at their boundaries rather than here. Engine
        bookkeeping, execution-context preparation and event publication raise
        OrchestrationError. Everything else that escapes is this step's ordinary
        failure, for the failure policy to contain or propagate: the step's own
        exception, but also realm-controlled work done on its behalf, such as
        hashing the input paths it declares.

        Args:
            plan: The plan being executed.
            spec: The step to reuse or execute.
            fn: The step's callable, resolved during preflight.
            plan_dir: Directory for this plan's execution state.

        Returns:
            StepOutcome: REUSED on a cache hit, SUCCEEDED after execution.

        Raises:
            OrchestrationError: If engine bookkeeping, execution-context
                preparation or event publication fails.
            TransientStepError: Unchanged, once step.retry_unimplemented has been
                published; what it means for the attempt is the policy's call.
            Exception: Anything else the step, or realm-controlled work on its
                behalf, raised.
        """
        step_dir = self._step_dir(plan_dir, spec)
        with _orchestration_boundary(
            f"Creating work directory for step '{spec.step_id}'"
        ):
            step_dir.mkdir(parents=True, exist_ok=True)

        _lint_missing_inputs(spec, fn)

        run_id = _new_run_id()

        # compute fingerprint and check cache // pass fn so we can read _input_keys
        fingerprint = _default_fingerprint(spec, fn)
        fp_file = step_dir / "success.fingerprint"

        with _orchestration_boundary(
            f"Reading the cache marker for step '{spec.step_id}'"
        ):
            cache_hit = fp_file.exists() and fp_file.read_text().strip() == fingerprint

        if cache_hit:
            self._emit_step_skipped(plan, spec, fingerprint, run_id)
            return StepOutcome.REUSED

        # Building the context loads Yggdrasil's own external-systems
        # configuration. A broken configuration fails every step alike; it must
        # abort the attempt, not drain as a list of ordinary step failures.
        with _orchestration_boundary(
            f"Preparing the execution context for step '{spec.step_id}'"
        ):
            ctx = self._step_context(
                plan, spec, plan_dir, step_dir, fingerprint, run_id
            )

        try:
            # Coerce string params to Path where function signature expects it
            coerced_params = coerce_params_to_signature_types(fn, spec.params)
            # returns StepResult (decorator wraps emissions)
            result = fn(ctx, **coerced_params)
        except TransientStepError as exc:
            # The @step wrapper has already emitted "step.failed".
            # Add a precise diagnostic so operators know retry isn't wired yet.
            # TODO: Implement retry.
            self._emit_retry_unimplemented(plan, spec, run_id, exc)
            raise

        if result is not None and not isinstance(result, StepResult):
            self._logger.warning(
                "Step %s returned %r (expected StepResult or None)",
                spec.step_id,
                type(result),
            )

        # Success-publication ordering contract (PRD §9). The owner of this
        # sequence is the engine, and later work implements parts of it, so the
        # order is recorded here once:
        #
        #   required-output validation
        #     -> step-success publication
        #     -> atomic marker replacement
        #     -> admission of successors (the scheduler, once this returns)
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

        return StepOutcome.SUCCEEDED

    def _step_context(
        self,
        plan: Plan,
        spec: StepSpec,
        plan_dir: Path,
        step_dir: Path,
        fingerprint: str,
        run_id: str,
    ) -> StepContext:
        """Build the context one step invocation executes with.

        Args:
            plan: The plan being executed.
            spec: The step about to run.
            plan_dir: Directory for this plan's execution state.
            step_dir: The step's work directory.
            fingerprint: The step's fingerprint for this invocation.
            run_id: The ID of this invocation.

        Returns:
            StepContext: The context passed to the step function.
        """
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

        return StepContext(
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

    def _emit_step_skipped(
        self, plan: Plan, spec: StepSpec, fingerprint: str, run_id: str
    ) -> None:
        """Publish the cache-hit event for a reused step.

        Args:
            plan: The plan being executed.
            spec: The reused step.
            fingerprint: The fingerprint its cache marker matched.
            run_id: The ID allocated for this evaluation of the step.

        Raises:
            EventPublicationError: If the emitter fails to publish.
        """
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

    def _emit_retry_unimplemented(
        self, plan: Plan, spec: StepSpec, run_id: str, exc: TransientStepError
    ) -> None:
        """Publish the diagnostic that a transient failure was not retried.

        Args:
            plan: The plan being executed.
            spec: The step that failed transiently.
            run_id: The ID of the failed invocation.
            exc: The transient failure.

        Raises:
            EventPublicationError: If the emitter fails to publish.
        """
        self._emit_engine_event(
            f"step.retry_unimplemented for step '{spec.step_id}'",
            {
                "type": "step.retry_unimplemented",
                "realm": plan.realm,
                "scope": plan.scope,
                "plan_id": plan.plan_id,
                "step_id": spec.step_id,
                "error": str(exc),
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

    def _close_report(
        self,
        report: AttemptReport,
        scheduler: DependencyScheduler | None,
        reason: TerminationReason,
        *,
        cause: BaseException | None = None,
    ) -> None:
        """Close an attempt's report with how the attempt ended.

        Settles the final blocker diagnostics first, so a report closed early —
        by fail-fast termination, cancellation or an abort — is as complete as
        one that drained. A fail-fast step failure is already described by that
        step's failure record; any other exceptional ending is recorded as an
        attempt-level diagnostic, naming the step that was running if there was
        one.

        Args:
            report: The attempt's report.
            scheduler: The attempt's scheduler, or None if the attempt ended
                before scheduling began.
            reason: How the attempt ended.
            cause: The exception ending the attempt, if any.

        Raises:
            OrchestrationError: If ``reason`` is COMPLETED but the report cannot
                truthfully say so.
        """
        if scheduler is not None:
            scheduler.record_blocker_diagnostics()
        if cause is not None and reason is not TerminationReason.FAILED_FAST:
            running = scheduler.running_step_id if scheduler is not None else None
            report.record_diagnostic(
                AttemptDiagnostic.from_exception(
                    cause, details={"running_step_id": running} if running else None
                )
            )
        report.finish(reason)
        self._logger.info(
            "Attempt '%s' for plan '%s' ended (%s): outcome=%s, steps=%s",
            report.execution_id,
            report.plan_id,
            reason.value,
            report.outcome.value if report.outcome else None,
            report.counts,
        )

    def _publish_attempt_report(
        self, plan: Plan, report: AttemptReport, *, cause: BaseException | None
    ) -> None:
        """Publish an attempt's closed report as its one plan-level event.

        Published on every ending, so no attempt is left looking as if it were
        still running. The event names no step and no filename: the spool files
        it directly under the plan, named after its unique event ID, so reports
        from different attempts of one plan never overwrite each other.

        If the attempt is ending *because* event publication failed, publishing
        again would only fail the same way and bury the original cause under a
        second, identical failure, so publication is skipped.

        Args:
            plan: The plan the attempt executed.
            report: The attempt's closed report.
            cause: The exception ending the attempt, or None if it drained.

        Raises:
            EventPublicationError: If publication fails and the attempt drained
                or is ending with an Exception. In the latter case the message
                names that exception and it stays reachable through the chain.
                An interruption that is not an Exception is never replaced: the
                publication failure is attached to it as a note instead.
        """
        if isinstance(cause, EventPublicationError):
            self._logger.error(
                "Not publishing the report of attempt '%s' for plan '%s': event "
                "publication already failed during this attempt.",
                report.execution_id,
                plan.plan_id,
            )
            return

        event: dict[str, Any] = {
            "type": ATTEMPT_REPORT_EVENT,
            "realm": plan.realm,
            "scope": plan.scope,
            "plan_id": plan.plan_id,
            "execution_id": report.execution_id,
            "report": report.to_dict(),
            "eid": str(uuid.uuid4()),
            "ts": utcnow_iso(),
            "_spool_path": {"realm": plan.realm, "plan_id": plan.plan_id},
        }
        try:
            self._emit_engine_event(
                f"the report of attempt '{report.execution_id}' for plan "
                f"'{plan.plan_id}'",
                event,
            )
        except EventPublicationError as publish_error:
            if cause is None:
                raise
            if not isinstance(cause, Exception):
                cause.add_note(
                    f"Publishing the attempt report also failed: {publish_error}"
                )
                self._logger.error("%s", publish_error)
                return
            raise EventPublicationError(
                f"Failed to publish the report of attempt '{report.execution_id}' "
                f"for plan '{plan.plan_id}'; the original "
                f"{type(cause).__name__} is preserved: {cause}"
            ) from publish_error
