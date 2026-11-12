"""Whether a plan document is eligible for execution: one decision, in one order.

The checks run in this order, and the first that fails names the refusal:

1. the document is a JSON object (``invalid_document``);
2. its ``schema_version`` names a supported family (``unsupported_document``):
   an unversioned, invalid or unsupported document is inspect-only, so
   nothing about its status or tokens is concluded;
3. its tokens form a valid pair under the strict token rule
   (``invalid_tokens``);
4. it records a ``status`` (``status_missing``);
5. the status is ``approved`` (``not_approved``): ``draft``, ``pending`` and
   any other text are not;
6. its request is not already executed, ``run_token != executed_run_token``
   (``already_executed``).

Otherwise it is ``eligible``. The decision is document-local: it knows no
expected ID, and it does not judge who may execute the plan. Nothing is
defaulted, nothing is logged, and no stored content makes it raise.
"""

from __future__ import annotations

from collections.abc import Mapping

from yggdrasil.contract import PLAN_DOCUMENT, controls, versions
from yggdrasil.contract.diagnostics import describe_recorded
from yggdrasil.contract.fields import (
    APPROVAL_STATUSES,
    APPROVED,
    EXECUTED_RUN_TOKEN_FIELD,
    RUN_TOKEN_FIELD,
    SCHEMA_VERSION_FIELD,
    STATUS_FIELD,
)
from yggdrasil.contract.problems import (
    INVALID_VERSION,
    UNSUPPORTED_VERSION,
    UNVERSIONED,
    WRONG_TYPE,
    ContractProblem,
)
from yggdrasil.contract.read import _values
from yggdrasil.contract.read._results import (
    ALREADY_EXECUTED,
    ELIGIBLE,
    INVALID_DOCUMENT,
    INVALID_TOKENS,
    NOT_APPROVED,
    STATUS_MISSING,
    UNSUPPORTED_DOCUMENT,
    Eligibility,
    NoDocument,
)

# The problem code of each version state that makes a document inspect-only.
_INSPECT_ONLY_CODES = {
    versions.UNVERSIONED: UNVERSIONED,
    versions.INVALID: INVALID_VERSION,
    versions.UNSUPPORTED: UNSUPPORTED_VERSION,
}


def eligibility(document: object) -> Eligibility:
    """Decide whether a plan document is eligible for execution.

    Checks the version first. Whether the document is the one a caller asked
    for is not judged here; :func:`~yggdrasil.contract.read.read_plan`, which
    knows the expected ID, refuses a mismatch as ``identity_mismatch``.

    Args:
        document: The stored plan document, of any shape.

    Returns:
        Eligibility: The first check that fails, or ``eligible``, with the
        reason and the problems that decided it.

    Raises:
        TypeError: If document is a NoDocument, which no store returns as a
            body.
    """
    if isinstance(document, NoDocument):
        raise TypeError(f"eligibility needs a document body, not {document}")
    if not isinstance(document, Mapping):
        problem = not_an_object(document)
        return _refused(INVALID_DOCUMENT, problem.message, (problem,))
    version = versions.version_of(PLAN_DOCUMENT, document)
    if version.state != versions.SUPPORTED:
        return _refused(
            UNSUPPORTED_DOCUMENT,
            versions.version_sentence(PLAN_DOCUMENT, version),
            (inspect_only(version),),
        )
    tokens = controls.token_pair(document)
    if not tokens.valid:
        return _refused(
            INVALID_TOKENS,
            f"invalid tokens ({'; '.join(tokens.problems)})",
            tokens.details,
        )
    found: list[ContractProblem] = []
    status = _values.word(document, STATUS_FIELD, (), found, APPROVAL_STATUSES)
    if not status.present:
        return _refused(
            STATUS_MISSING,
            f"{STATUS_FIELD} is missing (must be '{APPROVED}')",
            tuple(found),
        )
    if status.value != APPROVED:
        return _refused(
            NOT_APPROVED,
            f"{STATUS_FIELD} {describe_recorded(status.recorded)} "
            f"(must be '{APPROVED}')",
            tuple(found),
        )
    # A valid pair never has executed above run: equal tokens are the only
    # pair whose request was already served.
    if tokens.run == tokens.executed:
        return _refused(
            ALREADY_EXECUTED,
            f"{RUN_TOKEN_FIELD}={tokens.run} was already executed "
            f"({EXECUTED_RUN_TOKEN_FIELD}={tokens.executed})",
            (),
        )
    return Eligibility(
        eligible=True,
        code=ELIGIBLE,
        reason=(
            f"Eligible: {STATUS_FIELD}='{APPROVED}', {RUN_TOKEN_FIELD}={tokens.run} "
            f"> {EXECUTED_RUN_TOKEN_FIELD}={tokens.executed}"
        ),
        problems=(),
    )


def is_eligible(document: object) -> bool:
    """Return whether a plan document is eligible for execution.

    Derived from :func:`eligibility`, so it checks the version first.

    Args:
        document: The stored plan document, of any shape.

    Returns:
        bool: True only when the decision's code is ``eligible``.

    Raises:
        TypeError: If document is a NoDocument.
    """
    return eligibility(document).eligible


def eligibility_reason(document: object) -> str:
    """Explain whether a plan document is eligible for execution, for a log line.

    Derived from :func:`eligibility`, so it checks the version first. The
    text is for people; a program decides on the code.

    Args:
        document: The stored plan document, of any shape.

    Returns:
        str: The decision's reason.

    Raises:
        TypeError: If document is a NoDocument.
    """
    return eligibility(document).reason


def not_an_object(document: object) -> ContractProblem:
    """Return the problem of a plan document that is not a JSON object.

    Args:
        document: What was stored, of any type other than an object.

    Returns:
        ContractProblem: A ``wrong_type`` problem at the document's root.
    """
    message = _values.not_of_type("the plan document", document, _values.OBJECT)
    return _values.problem(WRONG_TYPE, (), message)


def inspect_only(version: versions.VersionInfo) -> ContractProblem:
    """Return the problem of a plan document that is not of a supported family.

    Args:
        version: The document's version, in a state other than supported.

    Returns:
        ContractProblem: One problem at ``schema_version``, coded
        ``unversioned``, ``invalid_version`` or ``unsupported_version``.
    """
    return _values.problem(
        _INSPECT_ONLY_CODES[version.state],
        (SCHEMA_VERSION_FIELD,),
        "the plan document is inspect-only: "
        + versions.version_sentence(PLAN_DOCUMENT, version),
    )


def _refused(code: str, why: str, problems: tuple[ContractProblem, ...]) -> Eligibility:
    """Return a refusal with its code, its reason and the problems behind it."""
    return Eligibility(
        eligible=False, code=code, reason=f"Not eligible: {why}", problems=problems
    )
