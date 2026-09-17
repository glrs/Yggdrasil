"""Exception types raised by the Flow engine and step wrapper.

Three families live here, and the boundaries between them are load-bearing:

- ``StepError`` (and its subclasses) — an ordinary failure of the work a step
  was asked to do. Under ``continue_independent`` these are contained: the
  step is recorded as failed, its dependents are blocked, and unrelated
  branches keep running.
- ``OrchestrationError`` — Yggdrasil's own infrastructure failed, so the
  attempt can no longer be tracked or reported reliably. Never contained.
  ``EventPublicationError`` narrows it to the reporting channel itself.
- ``PreflightValidationError`` — the plan itself is malformed and was rejected
  before any step ran.

``AttemptCancelledError`` sits outside all three: it is a control-flow signal,
not a failure of the plan, a step, or the infrastructure.
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


class EventPublicationError(OrchestrationError):
    """The event emitter failed to publish, so reporting itself is broken.

    A narrower :class:`OrchestrationError` for the one infrastructure failure
    that also disables the channel used to describe failures. It exists so that
    code which would otherwise report through that channel — the engine
    publishing an attempt's final report, for instance — can tell by type that
    publishing again would only fail again, and skip it rather than bury the
    original cause under a second, identical publication failure.

    Everything said about :class:`OrchestrationError` applies unchanged: the
    attempt aborts and the execution request stays eligible.
    """


class AttemptCancelledError(Exception):
    """An execution attempt stopped starting new steps because it was cancelled.

    Raised by the engine between steps, never from inside one, once the attempt's
    cooperative cancellation signal is set and runnable work remains. The work
    already started has finished, and the outcomes determined so far stay in the
    attempt's report.

    Deliberately none of the other three families. It is not a
    :class:`StepError`, because no step failed; not an
    :class:`OrchestrationError`, because nothing is broken; and not a
    ``ValueError``, because the plan is fine. Above all it must never be read as
    a completed attempt that failed: an interrupted attempt did not finish, so
    its execution request must stay eligible.

    Engine-internal. It is how the engine tells whoever started an attempt that
    the attempt stopped; it is not a cancellation API for step authors. The
    engine never reads one raised from inside a step as cancellation — it is
    that step's ordinary failure. Cancellation is requested only through the
    attempt's context (``AttemptContext.request_cancellation``).
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
