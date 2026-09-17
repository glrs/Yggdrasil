from __future__ import annotations

import inspect
import os
import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Literal

from yggdrasil.flow.artifacts import ensure_artifact_ref
from yggdrasil.flow.model import Plan, StepSpec
from yggdrasil.flow.utils.callable_ref import fn_ref_from_callable

_STEP_ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*$")

InjectMode = Literal["auto", "none", "inputs", "outputs"]


def _validate_step_id(step_id: str) -> None:
    if not step_id or not _STEP_ID_RE.match(step_id):
        raise ValueError(
            f"Bad step_id '{step_id}': must match ^[A-Za-z][A-Za-z0-9_-]*$"
        )


def _artifact_key(key: str | Enum) -> str:
    return key.value if isinstance(key, Enum) else str(key)


def _inject_input_params(
    fn: Callable[..., Any],
    params: dict[str, Any],
    base: Path,
    mode: InjectMode = "auto",
) -> dict[str, Any]:
    """Pre-fill unset input parameters with their annotated artifact paths.

    Output parameters are settled by :meth:`PlanBuilder._resolve_output_params`
    instead, because their paths are declared as well as passed.

    Args:
        fn: The step function.
        params: Call params; not mutated.
        base: The builder base that artifact refs resolve against.
        mode: The builder's injection mode; inputs are injected under "auto"
            and "inputs".

    Returns:
        dict[str, Any]: A copy of params with unset input parameters filled.
    """
    p = dict(params)  # don’t mutate caller
    if mode in ("auto", "inputs"):
        for pname, aref in getattr(fn, "__step_inputs__", {}).items():
            p.setdefault(pname, ensure_artifact_ref(aref).resolve_path(base))
    return p


def _explicit_output_path(value: object, pname: str, step_id: str) -> Path:
    """Interpret a caller's explicit value for an output parameter as a path.

    Args:
        value: The value the caller passed.
        pname: The output parameter.
        step_id: The step being added, for diagnostics.

    Returns:
        Path: The value as a path, possibly relative.

    Raises:
        TypeError: If the value is not a str or os.PathLike.
        ValueError: If the value is an empty string.
    """
    if not isinstance(value, str | os.PathLike):
        raise TypeError(
            f"Output parameter '{pname}' of step '{step_id}' must be a path "
            f"(str or os.PathLike), got {type(value).__name__}."
        )
    if value == "":
        raise ValueError(
            f"Output parameter '{pname}' of step '{step_id}' is an empty path."
        )
    return Path(value)


def _absolute_output_default(fn: Callable[..., Any], pname: str, step_id: str) -> Path:
    """Return the path an output parameter falls back to when nothing is injected.

    With output injection disabled and no explicit value, the step writes to its
    own concrete default, so that is the only path that may be declared. It is
    accepted only when absolute: the engine never changes the working directory,
    so where a relative default lands is unknown while planning.

    Args:
        fn: The step function; a decorated one reports the wrapped signature.
        pname: The output parameter.
        step_id: The step being added, for diagnostics.

    Returns:
        Path: The parameter's absolute default path.

    Raises:
        ValueError: If the parameter has no default, or its default is not an
            absolute path.
    """
    try:
        parameter = inspect.signature(fn).parameters.get(pname)
    except (TypeError, ValueError):
        parameter = None
    default = inspect.Parameter.empty if parameter is None else parameter.default

    if default is inspect.Parameter.empty:
        reason = "it has no default"
    elif not isinstance(default, str | os.PathLike):
        reason = f"its default {default!r} is not a path"
    elif not Path(default).is_absolute():
        reason = (
            f"its default '{default}' is relative, and steps do not run from any "
            f"particular working directory"
        )
    else:
        return Path(default)

    raise ValueError(
        f"Cannot declare output '{pname}' of step '{step_id}': output injection is "
        f"disabled and {reason}. Pass an explicit path in params, or enable output "
        f"injection."
    )


@dataclass
class PlanBuilder:
    """
    Small helper that:
      - creates stable paths under a <base> dir by semantic artifact key
      - wires dependencies by declaring which artifact keys a step requires/provides
      - accumulates StepSpecs and emits a Plan

    ``base`` is made absolute on construction, so every path derived from it -
    injected parameters, declared outputs, registry entries - is absolute too.
    """

    plan_id: str
    realm: str
    scope: dict[str, Any]
    base: Path

    steps: list[StepSpec] = field(default_factory=list)
    _artifact_provider: dict[str, str] = field(default_factory=dict)  # key -> step_id
    _artifact_path: dict[str, str] = field(default_factory=dict)  # key -> abs path

    def __post_init__(self) -> None:
        """Make the base absolute before any path is derived from it.

        A relative base would leak relative paths into output declarations, and
        the engine reads a relative output declaration as relative to the step's
        work directory, not to wherever the plan happened to be built.
        """
        self.base = Path(self.base).absolute()

    # ----- path helpers -----
    def artifact_path(self, ref: object) -> Path:
        aref = ensure_artifact_ref(ref)
        return aref.resolve_path(self.base)  # ensures workspace exists

    # Get the *workspace directory* for a given artifact ref
    def artifact_workspace(self, ref: object) -> Path:
        aref = ensure_artifact_ref(ref)
        p = aref.resolve_path(self.base)
        # If the resolved path looks like a file (has a suffix), use its parent.
        # Otherwise treat it as a directory.
        ws = p.parent if p.suffix else p
        ws.mkdir(parents=True, exist_ok=True)
        return ws

    def _default_step_id(self, fn: Callable, version: str = "v1") -> str:
        proj = self.scope.get("id", "na")
        return f"{fn.__name__}__{proj}__{version}"

    def _map_from_refs(self, refs: list[object] | None) -> dict[str, str]:
        m: dict[str, str] = {}
        if not refs:
            return m
        for ref in refs:
            aref = ensure_artifact_ref(ref)
            m[aref.key()] = str(aref.resolve_path(self.base))
        return m

    def _absolute(self, path: Path) -> Path:
        """Resolve a path against the builder base unless it is already absolute.

        Args:
            path: The path to resolve.

        Returns:
            Path: An absolute path.
        """
        return path if path.is_absolute() else self.base / path

    def _resolve_output_params(
        self,
        fn: Callable[..., Any],
        params: dict[str, Any],
        *,
        step_id: str,
        inject: bool,
    ) -> tuple[dict[str, Any], dict[str, str]]:
        """Settle each annotated output's effective path once, for every consumer.

        The path a step writes, the path its spec declares and the path the
        artifact registry records must be one path. Each output parameter's path
        is therefore decided here, once:

        - An explicit value in ``params`` wins. A relative value is resolved
          against the builder base - not the step's work directory, which is
          the convention for manually constructed specs - and the resolved path
          replaces it in the call params, so the step writes exactly where its
          declaration says. A str value stays a str.
        - Otherwise, with output injection enabled, the annotated artifact path
          is injected.
        - Otherwise the step falls back to its own concrete default, which is
          declared only if it is an absolute path. Nothing is declared from an
          annotation the step never receives.

        Args:
            fn: The step function.
            params: Call params; not mutated.
            step_id: The step being added, for diagnostics.
            inject: Whether output injection is enabled.

        Returns:
            tuple[dict[str, Any], dict[str, str]]: The call params with every
            explicit or injected output parameter set to its absolute path, and
            the declared outputs as absolute path strings keyed by artifact key.

        Raises:
            TypeError: If an explicit output value is not a path.
            ValueError: If an explicit output value is empty, or injection is
                disabled and the step has no absolute default for an output
                given no explicit value.
        """
        call_params = dict(params)
        declared: dict[str, str] = {}
        for pname, ref in getattr(fn, "__step_outputs__", {}).items():
            aref = ensure_artifact_ref(ref)
            if pname in call_params:
                value = call_params[pname]
                path = self._absolute(_explicit_output_path(value, pname, step_id))
                call_params[pname] = str(path) if isinstance(value, str) else path
            elif inject:
                path = self._absolute(aref.resolve_path(self.base))
                call_params[pname] = path
            else:
                path = _absolute_output_default(fn, pname, step_id)
            declared[aref.key()] = str(path)
        return call_params, declared

    # ----- Artifact registry: key → (path, producer). Drives deps + path lookups -----
    def record_artifact(self, key: str | Enum, path: str, by_step_id: str) -> None:
        """Remember that step `by_step_id` produced artifact `key` at `path`."""
        k = _artifact_key(key)
        self._artifact_provider[k] = by_step_id
        self._artifact_path[k] = path

    def path_for(self, key: str | Enum) -> str:
        """Return the known path for artifact `key` (must have been produced already)."""
        k = _artifact_key(key)
        try:
            return self._artifact_path[k]
        except KeyError as e:
            raise KeyError(
                f"Artifact '{k}' has no known path (no prior step produced it)."
            ) from e

    # ----- adding steps -----
    def add_step_fn(
        self,
        fn: Callable,
        *,
        step_id: str | None = None,
        params: dict[str, Any] | None = None,
        requires_artifacts: Iterable[str | Enum] = (),  # new wiring
        inject_io: InjectMode = "auto",
        version: str = "v1",
    ) -> StepSpec:
        """Add a step for a @step function, wired by its annotated artifacts.

        The step's declared outputs are the paths its work will actually
        produce: see :meth:`_resolve_output_params`.

        Args:
            fn: The step function.
            step_id: The step's ID; derived from fn, scope and version if None.
            params: Call params. An explicit value for an output parameter
                overrides its annotated path; a relative one is resolved
                against the builder base, not the step's work directory.
            requires_artifacts: Additional artifact keys the step depends on.
            inject_io: Which annotated paths are pre-filled into params when not
                given explicitly: "auto" (inputs and outputs), "inputs",
                "outputs" or "none".
            version: Version suffix for a derived step ID.

        Returns:
            StepSpec: The added step.

        Raises:
            KeyError: If a required artifact has no provider.
            TypeError: If an explicit output value is not a path.
            ValueError: If the step ID is malformed, or an output's path cannot
                be determined.
        """
        sid = step_id or self._default_step_id(fn, version)

        # Ensure a fresh dict (and don’t mutate the caller’s)
        params = {} if params is None else dict(params)

        # _validate_step_id(sid) --> done in _add_step below

        ann_in = list(getattr(fn, "__step_inputs__", {}).values())

        in_map = self._map_from_refs(ann_in)  # {key -> abs path}

        # Optional convenience: pre-fill kwargs with resolved input paths
        call_params = _inject_input_params(fn, params, self.base, mode=inject_io)
        # Outputs are settled once, so call params and declarations agree
        call_params, out_map = self._resolve_output_params(
            fn, call_params, step_id=sid, inject=inject_io in ("auto", "outputs")
        )

        return self._add_step(
            step_id=sid,
            name=fn.__name__,
            fn_ref=fn_ref_from_callable(fn),
            params=call_params,
            inputs=in_map,
            outputs=out_map,
            requires_artifacts=requires_artifacts,
        )

    def _add_step(
        self,
        *,
        step_id: str,
        name: str,
        fn_ref: str,
        params: dict[str, Any] | None = None,
        inputs: dict[str, str] | None = None,
        outputs: dict[str, str] | None = None,
        requires_artifacts: Iterable[str | Enum] = (),
    ) -> StepSpec:
        """Append a step, inferring its dependencies from artifact keys.

        Args:
            step_id: The step's ID.
            name: The step's name.
            fn_ref: Reference to the step function.
            params: Call params.
            inputs: Input artifact paths, keyed by artifact key.
            outputs: Absolute output artifact paths, keyed by artifact key.
                Declared on the spec and recorded in the artifact registry.
            requires_artifacts: Additional artifact keys the step depends on.

        Returns:
            StepSpec: The added step.

        Raises:
            ValueError: If the step ID is malformed, or an output path is
                relative.
            KeyError: If a required artifact has no provider.
        """
        _validate_step_id(step_id)

        params = {} if params is None else params
        inputs = {_artifact_key(k): v for k, v in (inputs or {}).items()}
        outputs = {_artifact_key(k): v for k, v in (outputs or {}).items()}

        # The engine resolves a relative output declaration against the step's
        # work directory; a builder path must never be prefixed a second time.
        relative = sorted(k for k, v in outputs.items() if not Path(v).is_absolute())
        if relative:
            raise ValueError(
                f"Step '{step_id}' declares relative output paths for {relative}; "
                f"builder-declared outputs must be absolute."
            )

        # Infer deps from artifact keys in addition to requires_artifacts
        required_keys: set[str] = {_artifact_key(r) for r in requires_artifacts} | set(
            inputs.keys()
        )
        deps: list[str] = []

        for key in sorted(required_keys):
            prov = self._artifact_provider.get(key)
            if prov:
                if prov != step_id and prov not in deps:
                    deps.append(prov)
            else:
                raise KeyError(
                    f"Step '{name}' requires artifact '{key}' which has no provider."
                )

        spec = StepSpec(
            step_id=step_id,
            name=name,
            fn_ref=fn_ref,
            params=params,
            deps=deps,
            scope=self.scope,
            inputs=inputs,
            outputs=outputs,
        )
        self.steps.append(spec)

        # Register provided artifact keys AFTER we’ve created this step
        for key, path in outputs.items():
            self.record_artifact(key, path, by_step_id=step_id)

        return spec

    # ----- finalize -----
    def to_plan(self) -> Plan:
        return Plan(
            plan_id=self.plan_id, realm=self.realm, scope=self.scope, steps=self.steps
        )
