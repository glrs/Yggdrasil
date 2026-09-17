from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class StepSpec:
    step_id: str  # e.g. "cellranger_multi__P12345_1001__9b7f"
    name: str  # e.g. "cellranger_multi"
    fn_ref: str  # dotted path to the @step function
    params: dict[str, Any]  # kwargs passed to the function
    deps: list[str] = field(default_factory=list)
    scope: dict[str, Any] = field(default_factory=dict)
    inputs: dict[str, str] = field(default_factory=dict)
    # Required output artifact paths this step produces, keyed by artifact key:
    # absolute, or relative to the step's own work directory. The step is reused
    # only while all of them exist (see yggdrasil.flow.outputs).
    outputs: dict[str, str] = field(default_factory=dict)


FAIL_FAST_POLICY = "fail_fast"
CONTINUE_INDEPENDENT_POLICY = "continue_independent"
VALID_FAILURE_POLICIES = frozenset({FAIL_FAST_POLICY, CONTINUE_INDEPENDENT_POLICY})
DEFAULT_FAILURE_POLICY = FAIL_FAST_POLICY


def validate_failure_policy(policy: str) -> None:
    """Raise ValueError if failure_policy is invalid.

    An *omitted* policy is never seen here: ``Plan.failure_policy`` defaults
    to ``"fail_fast"`` and ``Plan.from_dict`` supplies the same default for
    legacy documents, so omission is indistinguishable from an explicit
    ``"fail_fast"``. An explicitly supplied unknown value is rejected rather
    than silently downgraded.

    Args:
        policy: The failure policy to validate.

    Raises:
        ValueError: If policy is not one of VALID_FAILURE_POLICIES.
    """
    if policy not in VALID_FAILURE_POLICIES:
        raise ValueError(
            f"Invalid failure_policy: {policy!r}. "
            f"Must be one of: {sorted(VALID_FAILURE_POLICIES)}"
        )


@dataclass
class Plan:
    plan_id: str
    realm: str
    scope: dict[str, Any]
    steps: list[StepSpec] = field(default_factory=list)
    # "fail_fast" (default) or "continue_independent"; see VALID_FAILURE_POLICIES.
    failure_policy: str = DEFAULT_FAILURE_POLICY

    def to_dict(self) -> dict[str, Any]:
        """
        Serialize Plan to a dictionary for database storage.

        Returns:
            dict: Dictionary representation with all fields, including serialized steps.

        Example:
            >>> plan = Plan(plan_id="p1", realm="tenx", scope={"id": "P123"}, steps=[...])
            >>> plan_dict = plan.to_dict()
            >>> # plan_dict can be stored in CouchDB
        """
        return {
            "plan_id": self.plan_id,
            "realm": self.realm,
            "scope": self.scope,
            "steps": [
                {
                    "step_id": s.step_id,
                    "name": s.name,
                    "fn_ref": s.fn_ref,
                    "params": s.params,
                    "deps": s.deps,
                    "scope": s.scope,
                    "inputs": s.inputs,
                    "outputs": s.outputs,
                }
                for s in self.steps
            ],
            "failure_policy": self.failure_policy,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Plan:
        """
        Deserialize Plan from a dictionary (typically from database).

        Args:
            data: Dictionary with plan fields (expected from to_dict() or DB storage)

        Returns:
            Plan: Reconstructed Plan instance

        Raises:
            KeyError: If required fields (plan_id, realm) are missing
            TypeError: If steps list contains invalid entries (should not occur with
                      proper validation)

        Note:
            Missing optional fields (deps, scope, inputs, outputs) are populated with
            defaults (empty lists/dicts) rather than raising errors. This defensive
            approach handles partial or legacy documents gracefully. A missing
            failure_policy defaults to "fail_fast", so every already-persisted plan
            document keeps exactly the stop-on-first-failure behavior it has today.

        Example:
            >>> plan_dict = {
            ...     "plan_id": "p1",
            ...     "realm": "tenx",
            ...     "scope": {"id": "P123"},
            ...     "steps": [
            ...         {
            ...             "step_id": "s1",
            ...             "name": "preprocess",
            ...             "fn_ref": "module:fn",
            ...             "params": {},
            ...             "deps": [],
            ...             "scope": {},
            ...             "inputs": {},
            ...         }
            ...     ],
            ... }
            >>> plan = Plan.from_dict(plan_dict)
            >>> assert plan.plan_id == "p1"
        """
        steps = [
            StepSpec(
                step_id=s["step_id"],
                name=s["name"],
                fn_ref=s["fn_ref"],
                params=s["params"],
                deps=s.get("deps", []),
                scope=s.get("scope", {}),
                inputs=s.get("inputs", {}),
                outputs=s.get("outputs", {}),
            )
            for s in data.get("steps", [])
        ]
        return cls(
            plan_id=data["plan_id"],
            realm=data["realm"],
            scope=data.get("scope", {}),
            steps=steps,
            failure_policy=data.get("failure_policy", DEFAULT_FAILURE_POLICY),
        )


@dataclass
class Artifact:
    key: str  # semantic label, e.g. "library_csv", "cr_outs", "submit_script"
    path: str  # absolute or workdir-relative path
    digest: str | None = None  # sha256 for files or "dirhash:<hex>" for dirs


@dataclass
class StepResult:
    artifacts: list[Artifact] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    extra: dict[str, Any] = field(default_factory=dict)
