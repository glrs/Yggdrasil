"""How the contract reports a problem: one record, one set of codes.

A :class:`ContractProblem` names where in a document something is wrong
(``path``), why (``code``) and says so in words (``message``). Validation
against the published schemas, the control checks and the reader of plan
documents and snapshots all report their findings in this one form, so a
caller can act on a problem's path and code without parsing its message.

Codes are stable strings. Two named sets partition them:

- :data:`VALIDATION_CODES`: the codes :func:`yggdrasil.contract.schema.validate`
  reports against the published schemas;
- :data:`READER_CODES`: the codes the reader adds for what no schema can say,
  such as a record that contradicts its report.

:data:`ALL_CODES` is their union. Every code is spelled once, here;
:mod:`yggdrasil.contract.schema` re-exports the validation codes under the
names it has always had.

:func:`path_text` renders a path the one way every module shows it:
``plan.steps[0].deps``, and a key that is not a plain name quoted,
``steps["lane_1/demux"].state``.

The module imports only the standard library and
:mod:`yggdrasil.contract.diagnostics`.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable
from dataclasses import dataclass

from yggdrasil.contract.diagnostics import describe_recorded

# Codes of the problems validation reports.
UNVERSIONED = "unversioned"
INVALID_VERSION = "invalid_version"
UNSUPPORTED_VERSION = "unsupported_version"
MISSING_FIELD = "missing_field"
UNEXPECTED_FIELD = "unexpected_field"
WRONG_TYPE = "wrong_type"
NOT_IN_VOCABULARY = "not_in_vocabulary"
OUT_OF_RANGE = "out_of_range"
BAD_TIMESTAMP = "bad_timestamp"
INVALID_TOKENS = "invalid_tokens"
SCHEMA_RULE = "schema"  # any other rule of the schema

# Codes the reader adds.
IDENTITY_MISMATCH = "identity_mismatch"
INVALID_STRUCTURE = "invalid_structure"
RECORD_REPORT_DISAGREE = "record_report_disagree"
CONFLICTING_ENDINGS = "conflicting_endings"
IDENTITY_CONFLICT = "identity_conflict"
STEP_RECORD_CONTRADICTION = "step_record_contradiction"
INVALID_FAILURE_POLICY = "invalid_failure_policy"
INVALID_EXECUTION_DEFINITION = "invalid_execution_definition"

VALIDATION_CODES = frozenset(
    {
        UNVERSIONED,
        INVALID_VERSION,
        UNSUPPORTED_VERSION,
        MISSING_FIELD,
        UNEXPECTED_FIELD,
        WRONG_TYPE,
        NOT_IN_VOCABULARY,
        OUT_OF_RANGE,
        BAD_TIMESTAMP,
        INVALID_TOKENS,
        SCHEMA_RULE,
    }
)

READER_CODES = frozenset(
    {
        IDENTITY_MISMATCH,
        INVALID_STRUCTURE,
        RECORD_REPORT_DISAGREE,
        CONFLICTING_ENDINGS,
        IDENTITY_CONFLICT,
        STEP_RECORD_CONTRADICTION,
        INVALID_FAILURE_POLICY,
        INVALID_EXECUTION_DEFINITION,
    }
)

ALL_CODES = VALIDATION_CODES | READER_CODES

# Object keys that a path can show after a dot; any other key is quoted.
_PLAIN_KEY = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


@dataclass(frozen=True)
class ContractProblem:
    """One way a document fails its contract.

    Attributes:
        path: Where in the document, such as ``plan.steps[0].deps``; empty for
            the document itself. A key that is not a plain name is quoted:
            ``steps["lane_1/demux"].state``.
        code: Why: one of ALL_CODES.
        message: A description naming the path and, where it helps, the
            recorded value, shortened.
    """

    path: str
    code: str
    message: str


def path_text(path: Iterable[str | int]) -> str:
    """Render a path such as ``plan.steps[0].deps`` or ``steps["a/b"].state``.

    A key that is not text, which no JSON object has, is shown as
    :func:`describe_recorded` renders it.

    Args:
        path: The object keys and list indexes leading to a value.

    Returns:
        str: The path as text; empty for an empty path.
    """
    text = ""
    for part in path:
        if isinstance(part, int):
            text += f"[{part}]"
        elif not isinstance(part, str):
            text += f"[{describe_recorded(part)}]"
        elif _PLAIN_KEY.fullmatch(part):
            text += f".{part}" if text else part
        else:
            text += f"[{json.dumps(part)}]"
    return text
