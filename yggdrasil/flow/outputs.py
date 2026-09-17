"""Declared step outputs: validity, path resolution and presence.

A step's ``StepSpec.outputs`` names the artifacts its work must leave behind.
Two parts of Yggdrasil read those declarations and must agree about them
exactly: the engine, deciding whether an earlier success may be reused instead
of running the step, and the ``@step`` wrapper, deciding whether a step that
returned may be reported as succeeded. This module is the one definition both
use.

Path rule. A declared path that is absolute is used as-is. Any other path is
relative to the producing step's own work directory,
``<work_root>/<plan_id>/<step_id>`` - never the plan directory, another step's
directory, or the process working directory, which the engine does not change.
Declarations are resolved where they are checked and never rewritten in the
spec, so a fingerprint covers exactly what the realm declared. ``PlanBuilder``
always declares absolute paths, so its declarations are never prefixed with a
step directory. ``StepSpec.inputs`` is unaffected by this rule.

Presence. A declared output is present when its path exists. Only ordinary
absence counts as missing. Failing to find out - permission denied, an I/O
error - says nothing about whether the output is there, so it is raised as an
:class:`~yggdrasil.flow.errors.OrchestrationError` rather than guessed either
way. Existence is all that is checked: a directory's existence does not
establish that its contents are complete, so a realm that needs that declares a
file or completion sentinel inside it.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from yggdrasil.flow.errors import OrchestrationError

# StepError code carried by the failure of a step that returned without
# producing a required output.
MISSING_REQUIRED_OUTPUTS_CODE = "missing_required_outputs"


def validate_output_declarations(outputs: object) -> None:
    """Raise ValueError if a step's output declarations are malformed.

    Declarations must survive serialization unchanged and hash the same way
    everywhere, so only a plain ``dict`` of nonempty strings is accepted - the
    shape a persisted plan document deserializes to.

    Args:
        outputs: The step's ``outputs`` value.

    Raises:
        ValueError: If outputs is not a dict, a key is not a nonempty string, or
            a path is not a nonempty string without NUL characters.
    """
    if not isinstance(outputs, dict):
        raise ValueError(
            f"outputs must be a dict of artifact key to path, "
            f"got {type(outputs).__name__}."
        )
    for key, path in outputs.items():
        if not isinstance(key, str) or not key:
            raise ValueError(f"output keys must be nonempty strings, got {key!r}.")
        if not isinstance(path, str) or not path:
            raise ValueError(
                f"output '{key}' must be a nonempty path string, got {path!r}."
            )
        if "\0" in path:
            raise ValueError(f"output '{key}' has a NUL character in its path.")


def resolve_declared_outputs(
    outputs: Mapping[str, str], workdir: Path
) -> dict[str, Path]:
    """Resolve a step's output declarations to the paths they name.

    Args:
        outputs: The step's declared outputs, keyed by artifact key.
        workdir: The producing step's own work directory.

    Returns:
        dict[str, Path]: Each declared output's path, in declaration order:
        absolute declarations unchanged, the rest under ``workdir``.
    """
    resolved: dict[str, Path] = {}
    for key, declared in outputs.items():
        path = Path(declared)
        resolved[key] = path if path.is_absolute() else workdir / path
    return resolved


def find_missing_outputs(
    required: Mapping[str, Path], *, step_id: str
) -> dict[str, Path]:
    """Return the required outputs that do not exist.

    A path counts as missing when it, or a directory on the way to it, does not
    exist; a dangling symbolic link is missing too. Every other failure to stat
    a path is raised, not counted.

    Args:
        required: Resolved required outputs, keyed by artifact key.
        step_id: The step the outputs belong to, for diagnostics.

    Returns:
        dict[str, Path]: The missing outputs, in declaration order. Empty when
        every required output exists, including when none are required.

    Raises:
        OrchestrationError: If whether an output exists cannot be determined.
    """
    missing: dict[str, Path] = {}
    for key, path in required.items():
        try:
            path.stat()
        except (FileNotFoundError, NotADirectoryError):
            missing[key] = path
        except (OSError, ValueError) as exc:
            raise OrchestrationError(
                f"Could not check required output '{key}' of step '{step_id}' "
                f"at '{path}': {exc}"
            ) from exc
    return missing
