"""Exception types raised by the Flow engine and step wrapper.

Three families live here, and the boundaries between them are load-bearing:

- ``StepError`` (and its subclasses) — an ordinary failure of the work a step
  was asked to do. Under ``continue_independent`` these are contained: the
  step is recorded as failed, its dependents are blocked, and unrelated
  branches keep running.
- ``OrchestrationError`` — Yggdrasil's own infrastructure failed, so the
  attempt can no longer be tracked or reported reliably. Never contained.
- ``PreflightValidationError`` — the plan itself is malformed and was rejected
  before any step ran.
"""


class StepError(Exception):
    def __init__(self, msg: str, *, code: str | None = None, advice: str | None = None):
        super().__init__(msg)
        self.code = code
        self.advice = advice


class PermanentStepError(StepError): ...


class TransientStepError(StepError): ...


class OrchestrationError(Exception):
    """Infrastructure/reporting failure that must abort the whole attempt.

    Raised when Yggdrasil's own machinery fails — event publication, engine
    bookkeeping (plan file, step directories, cache markers), terminal
    persistence — as opposed to the realm's work failing. Such a failure means
    the attempt can no longer be observed or recorded accurately, so it is
    never contained as an ordinary step failure, under either failure policy.

    Deliberately NOT a ``StepError`` subclass: it must never be caught by
    generic step-failure handling. Call sites check for this type *before*
    treating an exception as an ordinary contained step failure, so exception
    *ordering* is the only thing that has to be right — not a fragile
    ``isinstance`` hierarchy.

    Deliberately outside the ``ValueError`` hierarchy too, so that an
    ``except ValueError`` written to mean "malformed plan" (see
    :class:`PreflightValidationError`) cannot sweep up infrastructure failure.

    For callers deciding whether an execution request is finished: an
    ``OrchestrationError`` means "we do not know whether this ran", so the
    request must stay eligible — unlike :class:`PreflightValidationError`.
    """


class PreflightValidationError(ValueError):
    """A plan was rejected by preflight, before any step ran.

    Raised for structural defects in the plan itself: duplicate, empty or
    unknown step identities, self-dependencies, cycles, an unknown failure
    policy, an unresolvable or undecorated step callable, or parameters that
    cannot bind to a step's signature.

    Subclasses ``ValueError`` because preflight rejections have always raised
    ``ValueError``, and existing callers and tests catch that. The distinct
    type exists because a preflight rejection is *definitive*: the plan is
    malformed and rerunning it unchanged cannot help, so callers may treat the
    captured execution request as terminally finished. That response is
    correct for a malformed plan and dangerous for anything else, which is why
    the distinction is a type rather than a guess.

    Like :class:`OrchestrationError`, this is not a ``StepError`` — it
    describes the plan, not a step.
    """
