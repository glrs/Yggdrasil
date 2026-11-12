"""No stored value makes the reader fail: a sweep over the regression corpus.

The targeted tests name the faults they know about. This sweep does not rely
on knowing them: it puts each of a set of hostile JSON values at every
position of every corpus document, one at a time, and requires that every
reader function still answers, with an answer of its declared type. A
document may become unusable, a field unknown; nothing may raise.

The inputs are every plan document alone, every snapshot alone, and every
plan and snapshot that the manifest's sequences put together. A pair is read
the way a consumer reads one: the plan first, then the snapshot for the
plan's ID, under the realm the plan records, or the pair's own realm when
the plan recognises none, and the two reads correlated. A plan that names no
snapshot ID, because it is not usable or its scope or realm is damaged,
gives a consumer nothing to look the snapshot up by; it is still correlated
with the snapshot already at hand. Each pair is also correlated with every
NoDocument, ``LOOKUP_IMPOSSIBLE`` included.

The damage is deliberate. It says nothing about what Yggdrasil writes.
"""

from __future__ import annotations

import copy
import math
import types
import typing
import unittest
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field, fields, is_dataclass
from typing import Any, NamedTuple
from unittest.mock import patch

from tests.contract.read_support import (
    ReaderTestCase,
    plan_document,
    plan_document_names,
    sequence_pairs,
    snapshot,
    snapshot_names,
    valid_names,
)
from tests.contract.support import ABSENT, changed
from yggdrasil.contract.diagnostics import MAX_LENGTH
from yggdrasil.contract.problems import ALL_CODES, ContractProblem
from yggdrasil.contract.read import (
    LOOKUP_IMPOSSIBLE,
    MISSING,
    PRESENT,
    UNAVAILABLE,
    Correlation,
    Recorded,
    _steps,
    _values,
    correlate,
    eligibility,
    eligibility_reason,
    generation_of,
    is_eligible,
    read_plan,
    read_snapshot,
    request_state,
    snapshot_id_for,
    step_status,
)

# Values of awkward shapes, each with a name to report it by; ABSENT removes
# the key it is put at.
SWEEP_VALUES: tuple[tuple[str, Any], ...] = (
    ("absent", ABSENT),
    ("null", None),
    ("true", True),
    ("false", False),
    ("zero", 0),
    ("minus one", -1),
    ("2**53", 2**53),
    ("10**400", 10**400),
    ("-(10**400)", -(10**400)),
    ("1.5", 1.5),
    ("1e308", 1e308),
    ("infinity", math.inf),
    ("minus infinity", -math.inf),
    ("NaN", math.nan),
    ("empty text", ""),
    ("text", "x"),
    ("an event type", "step.started"),
    ("a time that leaves the calendar in UTC", "9999-12-31T23:59:59-05:00"),
    ("empty list", []),
    ("empty object", {}),
    ("list of a number", [1]),
    ("list of text", ["x"]),
    ("list of a list", [[]]),
    ("object", {"a": 1}),
    ("object with an empty key", {"": None}),
)

# The longest problem message: a path, shown bounded, at most two recorded
# values, each shown bounded, and fixed words.
MAX_MESSAGE = 3 * MAX_LENGTH + 200
# An eligibility reason joins the messages of up to three token problems.
MAX_REASON = 3 * MAX_MESSAGE

# Fields of a correlation that hold reads already checked on their own.
_INPUT_FIELDS = frozenset({"plan", "snapshot"})

# Each answer, as (function, result): the result must be of the type the
# function declares.
Answers = list[tuple[Callable[..., Any], Any]]


class Failure(NamedTuple):
    """A variation the reader did not answer as it must.

    Attributes:
        document: The damaged document's corpus name.
        key: The key or index the hostile value was put at.
        value: The hostile value's name.
        error: What went wrong: the exception's type and message, or the
            answer that broke a rule.
    """

    document: str
    key: object
    value: str
    error: str


@dataclass
class Sweep:
    """The variations a sweep read, and the ones that failed.

    Attributes:
        variations: How many single-value variations were read.
        failures: The variations that raised, or answered wrongly.
    """

    variations: int = 0
    failures: list[Failure] = field(default_factory=list)

    def read(
        self, document: str, key: object, value: str, answer: Callable[[], Answers]
    ) -> None:
        """Read one variation, recording a failure if it raises or answers wrongly.

        Args:
            document: The damaged document's name.
            key: Where the hostile value is.
            value: The hostile value's name.
            answer: Reads the variation, returning every answer.
        """
        self.variations += 1
        try:
            answers = answer()
        except Exception as error:  # the sweep records any exception
            text = f"{type(error).__name__}: {str(error)[:120]}"
            self.failures.append(Failure(document, key, value, text))
            return
        for function, result in answers:
            for fault in faults_of(function, result):
                self.failures.append(Failure(document, key, value, fault))


def positions(node: Any) -> Iterator[tuple[Any, Any]]:
    """Yield every (container, key) position in a JSON value, depth first.

    Args:
        node: The value.

    Yields:
        tuple[Any, Any]: An object and one of its keys, or a list and one of
        its indexes.
    """
    if isinstance(node, dict):
        for key in list(node):
            yield node, key
            yield from positions(node[key])
    elif isinstance(node, list):
        for index in range(len(node)):
            yield node, index
            yield from positions(node[index])


def damaged(root: Any) -> Iterator[tuple[Any, str]]:
    """Damage a document in place, one position and one hostile value at a time.

    Each damage is undone before the next, keys in their stored order, so the
    document is unchanged once the iteration ends.

    Args:
        root: The document.

    Yields:
        tuple[Any, str]: Where the value is, and the value's name, while the
        document holds it.
    """
    for container, key in list(positions(root)):
        original = container[key]
        for name, value in SWEEP_VALUES:
            if value is ABSENT:
                if isinstance(container, list):
                    continue
                items = list(container.items())
                del container[key]
                yield key, name
                container.clear()
                container.update(items)
                continue
            # A fresh container each time, so no variation shares one.
            container[key] = (
                copy.deepcopy(value) if isinstance(value, list | dict) else value
            )
            yield key, name
            container[key] = original


def faults_of(function: Callable[..., Any], result: Any) -> list[str]:
    """Return how an answer breaks the reader's rules; empty when it does not.

    The answer must be of the function's declared return type, every
    dataclass within it of its fields' declared types, every derived number
    finite, every problem a ContractProblem with a known code, a text path
    and a bounded message. A value declared ``object`` or ``Any`` holds what
    was stored, as stored, so nothing in it is a derived answer to check.

    Args:
        function: The reader function.
        result: What it returned.

    Returns:
        list[str]: Each fault found.
    """
    hint = _return_type(function)
    check = _check_of(hint)
    found: list[str] = []
    if check.accepts(result):
        check.inner(result, found)
    else:
        found.append(f"{function.__name__} returned a {type(result).__name__}")
    return found


class _Check(NamedTuple):
    """How to check a value against one declared type.

    Attributes:
        accepts: Whether a value is of the type, judged shallowly.
        inner: Checks what an accepted value holds, appending each fault.
    """

    accepts: Callable[[Any], bool]
    inner: Callable[[Any, list[str]], None]


_CHECKS: dict[Any, _Check] = {}
_RETURN_TYPES: dict[Callable[..., Any], Any] = {}


def _return_type(function: Callable[..., Any]) -> Any:
    """Return a function's declared return type, resolved once."""
    if function not in _RETURN_TYPES:
        _RETURN_TYPES[function] = typing.get_type_hints(function)["return"]
    return _RETURN_TYPES[function]


def _check_of(hint: Any) -> _Check:
    """Return the check of a declared type, compiled once."""
    check = _CHECKS.get(hint)
    if check is None:
        check = _CHECKS[hint] = _compiled(hint)
    return check


def _nothing(value: Any, found: list[str]) -> None:
    """Check nothing more in a value."""


def _compiled(hint: Any) -> _Check:
    """Compile the check of a declared type, as the reader declares its results."""
    if hint is Any or hint is object:
        return _Check(lambda value: True, _nothing)
    if hint is type(None):
        return _Check(lambda value: value is None, _nothing)
    origin = typing.get_origin(hint)
    arguments = typing.get_args(hint)
    if origin is typing.Union or origin is types.UnionType:
        return _union(tuple(_check_of(argument) for argument in arguments))
    if origin is tuple:
        return _tuple_of(_check_of(arguments[0]), arguments[0])
    if origin is Recorded:
        return _recorded_of(_check_of(arguments[0]), arguments[0])
    if origin is not None:
        return _Check(lambda value: isinstance(value, origin), _nothing)
    if hint is bool:
        return _Check(lambda value: type(value) is bool, _nothing)
    if hint is int:
        return _Check(lambda value: type(value) is int, _nothing)
    if hint is float:
        return _Check(
            lambda value: isinstance(value, int | float)
            and not isinstance(value, bool),
            _finite,
        )
    if hint is ContractProblem:
        return _Check(lambda value: isinstance(value, ContractProblem), _problem)
    if isinstance(hint, type) and is_dataclass(hint):
        return _dataclass_of(hint)
    return _Check(lambda value: isinstance(value, hint), _nothing)


def _union(parts: tuple[_Check, ...]) -> _Check:
    """Return the check of a union: of whichever alternative accepts the value."""

    def inner(value: Any, found: list[str]) -> None:
        for part in parts:
            if part.accepts(value):
                part.inner(value, found)
                return

    return _Check(lambda value: any(part.accepts(value) for part in parts), inner)


def _tuple_of(item: _Check, hint: Any) -> _Check:
    """Return the check of a tuple whose every item is of one type."""

    def inner(value: Any, found: list[str]) -> None:
        for each in value:
            if item.accepts(each):
                item.inner(each, found)
            else:
                found.append(f"a tuple holds a {type(each).__name__}, not {hint}")

    return _Check(lambda value: isinstance(value, tuple), inner)


def _recorded_of(recognised: _Check, hint: Any) -> _Check:
    """Return the check of a Recorded value, whose recognised value is typed."""

    def inner(value: Any, found: list[str]) -> None:
        if type(value.present) is not bool:
            found.append("a Recorded value's presence is not a boolean")
        if value.value is None:
            return
        if recognised.accepts(value.value):
            recognised.inner(value.value, found)
        else:
            found.append(
                f"a Recorded value is a {type(value.value).__name__}, not {hint}"
            )

    return _Check(lambda value: isinstance(value, Recorded), inner)


def _dataclass_of(cls: type) -> _Check:
    """Return the check of a result dataclass: each field of its declared type."""
    hints = typing.get_type_hints(cls)
    skipped = _INPUT_FIELDS if cls is Correlation else frozenset()
    checks = [
        (item.name, _check_of(hints[item.name]), hints[item.name])
        for item in fields(cls)
        if item.name not in skipped
    ]
    bounded = [
        (name, limit)
        for name, limit in (("reason", MAX_REASON), ("contradiction", MAX_MESSAGE))
        if name in hints
    ]

    def inner(value: Any, found: list[str]) -> None:
        for name, check, hint in checks:
            held = getattr(value, name)
            if check.accepts(held):
                check.inner(held, found)
            else:
                found.append(
                    f"{cls.__name__}.{name} is a {type(held).__name__}, not {hint}"
                )
        for name, limit in bounded:
            text = getattr(value, name)
            if isinstance(text, str) and len(text) > limit:
                found.append(f"{cls.__name__}.{name} has {len(text)} characters")

    return _Check(lambda value: isinstance(value, cls), inner)


def _finite(value: Any, found: list[str]) -> None:
    """Check that a derived number is finite."""
    if isinstance(value, float) and not math.isfinite(value):
        found.append("a derived number is not finite")


def _problem(problem: ContractProblem, found: list[str]) -> None:
    """Check that a problem has a known code, a text path and a bounded message."""
    if problem.code not in ALL_CODES:
        found.append(f"a problem has the unknown code {problem.code!r}")
    if not isinstance(problem.path, str):
        found.append("a problem's path is not text")
    if not isinstance(problem.message, str) or len(problem.message) > MAX_MESSAGE:
        found.append(f"a problem's message is longer than {MAX_MESSAGE} characters")


def plan_answers(document: Any, plan_id: str) -> Answers:
    """Call every plan-side function on a plan document.

    Args:
        document: The plan document.
        plan_id: The ID it is stored under, undamaged.

    Returns:
        Answers: Each function and its result.
    """
    return [
        (read_plan, read_plan(document, plan_id=plan_id)),
        (request_state, request_state(document, plan_id=plan_id)),
        (eligibility, eligibility(document)),
        (is_eligible, is_eligible(document)),
        (eligibility_reason, eligibility_reason(document)),
        (generation_of, generation_of(document)),
        (snapshot_id_for, snapshot_id_for(document, plan_id=plan_id)),
    ]


def snapshot_answers(document: Any, plan_id: str, realm: str) -> Answers:
    """Read a snapshot, and every step's status from its recorded state and outcome.

    Args:
        document: The snapshot.
        plan_id: The plan it is of, undamaged.
        realm: Its realm, undamaged.

    Returns:
        Answers: Each function and its result.
    """
    answers: Answers = [
        (read_snapshot, read_snapshot(document, plan_id=plan_id, realm=realm))
    ]
    steps = document.get("steps") if isinstance(document, Mapping) else None
    if isinstance(steps, Mapping):
        for entry in steps.values():
            if isinstance(entry, Mapping):
                status = step_status(entry.get("state"), entry.get("outcome"))
                answers.append((step_status, status))
    return answers


def pair_answers(
    plan_body: Any, snapshot_body: Any, plan_id: str, realm: str
) -> Answers:
    """Read a plan and its snapshot as a consumer does, and correlate them.

    The snapshot is read for the plan's ID, under the realm the plan
    records when it recognises one, so that the two reads are of one plan,
    and under the pair's own realm otherwise. The snapshot at hand is read
    whether or not the plan names a snapshot ID: a usable plan whose scope
    or realm is damaged names none, and its reads are correlated all the
    same. Then each NoDocument a consumer may hold instead.

    Args:
        plan_body: The plan document.
        snapshot_body: Its snapshot.
        plan_id: The plan's ID, undamaged.
        realm: The plan's realm, undamaged.

    Returns:
        Answers: Each function and its result.
    """
    plan = read_plan(plan_body, plan_id=plan_id)
    recorded_realm = plan.recorded.realm.value if plan.recorded is not None else None
    read = read_snapshot(
        snapshot_body,
        plan_id=plan_id,
        realm=recorded_realm if recorded_realm is not None else realm,
    )
    answers: Answers = [
        (read_plan, plan),
        (read_snapshot, read),
        (correlate, correlate(plan, read)),
    ]
    for sentinel in (MISSING, UNAVAILABLE, LOOKUP_IMPOSSIBLE):
        answers.append((correlate, correlate(plan, sentinel)))
    return answers


def sweep_plan(name: str) -> Sweep:
    """Sweep one corpus plan document, read alone."""
    sweep = Sweep()
    document = plan_document(name)
    plan_id = document["_id"]
    for key, value in damaged(document):
        sweep.read(name, key, value, lambda: plan_answers(document, plan_id))
    return sweep


def sweep_snapshot(name: str) -> Sweep:
    """Sweep one corpus snapshot, read alone."""
    sweep = Sweep()
    document = snapshot(name)
    plan_id, realm = document["plan_id"], document["realm"]
    for key, value in damaged(document):
        sweep.read(name, key, value, lambda: snapshot_answers(document, plan_id, realm))
    return sweep


def sweep_pair(plan_name: str, snapshot_name: str) -> Sweep:
    """Sweep a plan and its snapshot, damaging either one."""
    sweep = Sweep()
    plan_body, snapshot_body = plan_document(plan_name), snapshot(snapshot_name)
    plan_id, realm = plan_body["_id"], plan_body["realm"]
    for name, document in ((plan_name, plan_body), (snapshot_name, snapshot_body)):
        for key, value in damaged(document):
            sweep.read(
                name,
                key,
                value,
                lambda: pair_answers(plan_body, snapshot_body, plan_id, realm),
            )
    return sweep


def sweep_bodies() -> Sweep:
    """Read each hostile value as a whole plan document and as a whole snapshot."""
    sweep = Sweep()
    for name, value in SWEEP_VALUES:
        if value is ABSENT:
            continue
        sweep.read("the body", None, name, lambda: plan_answers(value, "pln_body"))
        sweep.read(
            "the body", None, name, lambda: snapshot_answers(value, "pln_body", "realm")
        )
    return sweep


class TestSweep(ReaderTestCase):
    """Every hostile value, at every position of every corpus document."""

    def assert_swept(self, sweep: Sweep) -> None:
        """Fail with every variation that raised or answered wrongly."""
        self.assertEqual(sweep.failures[:20], [], f"{len(sweep.failures)} failures")

    def test_no_value_in_a_plan_document_makes_a_read_fail(self):
        with self.assert_silent():
            for name in plan_document_names():
                sweep = sweep_plan(name)
                with self.subTest(name):
                    self.assert_swept(sweep)

    def test_no_value_in_a_snapshot_makes_a_read_fail(self):
        with self.assert_silent():
            for name in snapshot_names():
                sweep = sweep_snapshot(name)
                with self.subTest(name):
                    self.assert_swept(sweep)

    def test_no_value_in_a_pair_makes_a_read_fail(self):
        with self.assert_silent():
            for plan_name, snapshot_name in sequence_pairs():
                sweep = sweep_pair(plan_name, snapshot_name)
                with self.subTest(plan=plan_name, snapshot=snapshot_name):
                    self.assert_swept(sweep)
                    # Thousands of single-value variations per pair.
                    self.assertGreater(sweep.variations, 2000)

    def test_no_whole_body_makes_a_read_fail(self):
        with self.assert_silent():
            self.assert_swept(sweep_bodies())

    def test_every_position_is_read_with_every_value(self):
        """A key gets every value and is also removed; a list item cannot be."""
        for name, document, sweep in (
            ("draft", plan_document("draft"), sweep_plan("draft")),
            ("running", snapshot("running"), sweep_snapshot("running")),
        ):
            with self.subTest(name):
                found = list(positions(document))
                items = sum(isinstance(container, list) for container, _ in found)
                expected = len(found) * len(SWEEP_VALUES) - items

                self.assertEqual(sweep.variations, expected)
                self.assertGreater(len(found), 40)

    def test_the_sweep_leaves_each_document_as_it_found_it(self):
        document = snapshot("running_with_failure")

        for _ in damaged(document):
            pass

        self.assertEqual(document, snapshot("running_with_failure"))
        self.assertEqual(
            list(document["steps"]), list(snapshot("running_with_failure")["steps"])
        )


class TestUndamaged(ReaderTestCase):
    """Undamaged, every valid document and every pair reads without a problem."""

    def test_every_valid_plan_document(self):
        for name in valid_names("plan_document"):
            with self.subTest(name):
                document = plan_document(name)
                plan = read_plan(document, plan_id=document["_id"])
                self.assertEqual(plan.problems, ())
                self.assertIsNotNone(plan.steps)

    def test_every_valid_snapshot(self):
        for name in valid_names("plan_status"):
            with self.subTest(name):
                document = snapshot(name)
                read = read_snapshot(
                    document, plan_id=document["plan_id"], realm=document["realm"]
                )
                self.assertEqual(read.problems, ())
                self.assertIsNotNone(read.steps)

    def test_every_pair(self):
        for plan_name, snapshot_name in sequence_pairs():
            with self.subTest(plan=plan_name, snapshot=snapshot_name):
                document = plan_document(plan_name)
                plan = read_plan(document, plan_id=document["_id"])
                self.assertIsNotNone(plan.snapshot_id)
                assert plan.recorded is not None
                realm = plan.recorded.realm.value
                assert realm is not None
                read = read_snapshot(
                    snapshot(snapshot_name), plan_id=plan.plan_id, realm=realm
                )

                correlation = correlate(plan, read)

                self.assertEqual(
                    (plan.problems, read.problems, correlation.problems), ((), (), ())
                )
                self.assertIsNotNone(read.steps)
                self.assertEqual(faults_of(correlate, correlation), [])


class TestPairReads(ReaderTestCase):
    """A pair is read as a consumer reads one, with the snapshot at hand."""

    def test_a_plan_with_no_snapshot_id_is_correlated_with_the_snapshot_at_hand(self):
        """A damaged scope or realm leaves the plan usable but names no snapshot ID.

        The snapshot is still read and correlated, beside the NoDocuments: under
        the realm the plan records when it recognises one, under the pair's own
        realm otherwise.
        """
        undamaged = plan_document("approved_finalized")
        plan_id, realm = undamaged["_id"], undamaged["realm"]
        for name, path, value in (
            ("no scope", "scope", ABSENT),
            ("a scope without an ID", "scope.id", ""),
            ("a realm that is not text", "realm", 5),
        ):
            with self.subTest(name):
                plan_body = changed(undamaged, path, value)
                plan = read_plan(plan_body, plan_id=plan_id)
                self.assertTrue(plan.usable)
                self.assertIsNone(plan.snapshot_id)

                answers = pair_answers(
                    plan_body, snapshot("finished_success"), plan_id, realm
                )

                reads = [result for kind, result in answers if kind is read_snapshot]
                self.assertEqual(len(reads), 1)
                self.assertEqual(
                    (reads[0].presence, reads[0].usable, reads[0].realm),
                    (PRESENT, True, realm),
                )
                correlations = [result for kind, result in answers if kind is correlate]
                self.assertEqual(
                    [correlation.snapshot for correlation in correlations],
                    [reads[0], MISSING, UNAVAILABLE, LOOKUP_IMPOSSIBLE],
                )
                self.assertEqual(
                    correlations[0].finalized_and_observation.verdict,
                    "same_execution",
                )

    def test_the_snapshot_is_read_under_the_realm_the_plan_records(self):
        """A plan of another realm is paired with no snapshot of the pair's realm."""
        undamaged = plan_document("approved_finalized")
        plan_id, realm = undamaged["_id"], undamaged["realm"]
        plan_body = changed(undamaged, "realm", "another_realm")

        answers = pair_answers(plan_body, snapshot("finished_success"), plan_id, realm)

        (read,) = [result for kind, result in answers if kind is read_snapshot]
        self.assertEqual(read.realm, "another_realm")
        self.assertEqual(
            [(problem.path, problem.code) for problem in read.problems],
            [("realm", "identity_mismatch")],
        )


class TestPlantedFaults(ReaderTestCase):
    """The sweep is not vacuous: put a known fault back and it is reported."""

    def test_a_state_looked_up_before_it_is_known_to_be_text(self):
        real_step_status = _steps.step_status

        def hashing_step_status(state: object, outcome: object) -> Any:
            # Looking a state up without first checking that it is text.
            if state in frozenset({"step.succeeded"}) and outcome is None:
                return real_step_status("step.failed", None)
            return real_step_status(state, outcome)

        with patch.object(_steps, "step_status", hashing_step_status):
            sweep = sweep_snapshot("finished_success")

        self.assertTrue(
            any(
                failure.key == "state" and failure.error.startswith("TypeError")
                for failure in sweep.failures
            ),
            sweep.failures[:5],
        )

    def test_a_percentage_converted_before_its_range_is_checked(self):
        def overflowing_percentage(
            container: Mapping[str, Any], key: str, parent: Any, problems: Any
        ) -> Recorded[float]:
            value = container.get(key)
            number = float(value) if isinstance(value, int | float) else None
            return Recorded(number, value, True)

        with patch.object(_values, "percentage", overflowing_percentage):
            sweep = sweep_snapshot("running")

        self.assertTrue(
            any(
                failure.key == "progress" and failure.error.startswith("OverflowError")
                for failure in sweep.failures
            ),
            sweep.failures[:5],
        )

    def test_the_faults_are_not_found_without_their_plant(self):
        for name in ("finished_success", "running"):
            with self.subTest(name):
                self.assertEqual(sweep_snapshot(name).failures, [])


if __name__ == "__main__":
    unittest.main()
