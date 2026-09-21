"""What ties an event to the execution attempt it belongs to.

A step's ``run_id`` identifies one invocation of that step, not which attempt
of the whole plan the invocation was part of. Every event an attempt publishes
therefore also carries the attempt's :class:`ExecutionCorrelation`, so that a
reader can gather one attempt's events across every step and tell two attempts
at the same plan apart. The run token matters as well as the generation: a
manual rerun raises the run token without regenerating the plan, so two
attempts can share a generation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ExecutionCorrelation:
    """The identity of one execution attempt, as stamped onto its events.

    Attributes:
        execution_id: The attempt. Allocated at admission, and ordered: a later
            attempt at the same plan has a higher ID (see
            ``yggdrasil.core.execution_ids``).
        plan_generation: The plan generation the attempt captured, when its
            caller had one. None for a direct ``Engine.run`` call.
        run_token: The run token the attempt captured, when its caller had
            one. None for a direct ``Engine.run`` call.
    """

    execution_id: str
    plan_generation: str | None = None
    run_token: int | None = None

    def event_fields(self) -> dict[str, Any]:
        """Return the fields this correlation adds to an event envelope.

        All three are always present, so a correlated event can be told from
        an uncorrelated one by ``execution_id`` alone, and a missing generation
        or token reads as "not captured" rather than as a malformed event.

        Returns:
            dict[str, Any]: ``execution_id``, ``plan_generation`` and
            ``run_token``.
        """
        return {
            "execution_id": self.execution_id,
            "plan_generation": self.plan_generation,
            "run_token": self.run_token,
        }
