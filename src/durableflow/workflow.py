"""Workflow programming model and the deterministic replayer.

A workflow is a generator (or async) function `fn(ctx, input)` that yields commands:

    result = yield activity("charge_payment", order_id, amount)
    yield sleep(30 * DAY)
    approval = yield wait_signal("manager_approval", timeout=7 * DAY)
    child = yield child_workflow("order_fulfillment", {...})
    a, b = yield parallel(activity("x"), activity("y"))
    return continue_as_new({...})

Async functions use `await` on the same commands. Each command gets a sequence number
(1, 2, 3, ...) in the order the code issues it. History records the scheduling event and
the resolution event for each number. On replay, a command whose number is already in
history is checked against the recorded one (same kind, same name) and gets the recorded
result. Nothing is executed again. A command past the end of history is new: the replayer
collects it and stops, and the worker turns it into tasks and events.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import Any

from durableflow.errors import (
    ActivityError,
    ChildWorkflowError,
    NonDeterminismError,
    ParallelError,
    SignalTimeout,
)

DEFAULT_VERSION = 0


# ---------------------------------------------------------------- retry policy


@dataclass
class RetryPolicy:
    max_attempts: int = 3
    initial_interval: float = 1.0
    backoff: float = 2.0
    max_interval: float = 300.0
    non_retryable: list[str] = field(default_factory=list)

    def delay(self, attempt: int) -> float:
        """Seconds to wait after failed attempt number `attempt` (1-based)."""
        return min(self.initial_interval * self.backoff ** (attempt - 1), self.max_interval)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict | None) -> RetryPolicy:
        return cls(**d) if d else cls()


# ---------------------------------------------------------------- commands


class Command:
    kind = "command"

    def __await__(self):
        result = yield self
        return result


@dataclass
class ScheduleActivity(Command):
    name: str
    args: list
    kwargs: dict
    retry: RetryPolicy
    start_to_close: float
    heartbeat_timeout: float | None
    kind = "activity"


@dataclass
class StartTimer(Command):
    seconds: float
    kind = "timer"


@dataclass
class WaitSignal(Command):
    name: str
    timeout: float | None
    kind = "signal"


@dataclass
class StartChild(Command):
    workflow_type: str
    input: Any
    workflow_id: str | None
    kind = "child"


@dataclass
class Parallel(Command):
    commands: list
    kind = "parallel"


@dataclass
class ContinueAsNew(Command):
    input: Any
    kind = "continue_as_new"


@dataclass
class Compensated:
    """Return this from a workflow that undid its work on purpose (a finished saga)."""

    reason: str
    details: Any = None


def activity(
    name: str,
    *args: Any,
    retry: RetryPolicy | None = None,
    start_to_close: float = 30.0,
    heartbeat_timeout: float | None = None,
    **kwargs: Any,
) -> ScheduleActivity:
    return ScheduleActivity(
        name, list(args), kwargs, retry or RetryPolicy(), start_to_close, heartbeat_timeout
    )


def sleep(seconds: float) -> StartTimer:
    return StartTimer(float(seconds))


def wait_signal(name: str, timeout: float | None = None) -> WaitSignal:
    return WaitSignal(name, None if timeout is None else float(timeout))


def child_workflow(workflow_type: str, input: Any = None, workflow_id: str | None = None) -> StartChild:
    return StartChild(workflow_type, input, workflow_id)


def parallel(*commands: Command) -> Parallel:
    for c in commands:
        if not isinstance(c, ScheduleActivity | StartTimer | StartChild):
            raise TypeError("parallel() takes activities, timers, and child workflows")
    return Parallel(list(commands))


def continue_as_new(input: Any) -> ContinueAsNew:
    return ContinueAsNew(input)


def describe(cmd: Command) -> str:
    if isinstance(cmd, ScheduleActivity):
        return f"activity {cmd.name}"
    if isinstance(cmd, StartTimer):
        return f"timer {cmd.seconds:g}s"
    if isinstance(cmd, WaitSignal):
        return f"wait_signal {cmd.name}"
    if isinstance(cmd, StartChild):
        return f"child {cmd.workflow_type}"
    return cmd.kind


SCHEDULE_EVENTS = {
    "ActivityScheduled": "activity",
    "TimerStarted": "timer",
    "SignalWaitStarted": "signal",
    "ChildStarted": "child",
}
RESOLUTION_EVENTS = {
    "ActivityCompleted",
    "ActivityFailed",
    "TimerFired",
    "SignalWaitTimedOut",
    "ChildCompleted",
    "ChildFailed",
}


def describe_event(ev: dict) -> str:
    a = ev["attrs"]
    kind = SCHEDULE_EVENTS[ev["type"]]
    if kind == "activity":
        return f"activity {a['name']}"
    if kind == "timer":
        return f"timer {a['seconds']:g}s"
    if kind == "signal":
        return f"wait_signal {a['name']}"
    return f"child {a['workflow_type']}"


def matches(cmd: Command, ev: dict) -> bool:
    a = ev["attrs"]
    kind = SCHEDULE_EVENTS.get(ev["type"])
    if kind != cmd.kind:
        return False
    if kind == "activity":
        return a["name"] == cmd.name
    if kind == "signal":
        return a["name"] == cmd.name
    if kind == "child":
        return a["workflow_type"] == cmd.workflow_type
    return True  # timers: durations may change between versions without breaking replay


# ---------------------------------------------------------------- context and decision


@dataclass
class NewCommand:
    cmd_id: int
    command: Command
    resolved: bool = False  # a signal wait satisfied by an already buffered signal


@dataclass
class Decision:
    """What one workflow task decided. The worker turns it into events and tasks."""

    new_commands: list[NewCommand] = field(default_factory=list)
    markers: list[tuple[str, int]] = field(default_factory=list)
    resolved_waits: list[int] = field(default_factory=list)
    stage: str | None = None
    outcome: str = "blocked"  # blocked | completed | compensated | failed | continue_as_new | nondeterminism
    result: Any = None
    error_type: str | None = None
    error: str | None = None
    commands_replayed: int = 0


class WorkflowContext:
    """Passed to every workflow function. Everything on it is deterministic under replay."""

    def __init__(self, replayer: Replayer, info: dict) -> None:
        self._r = replayer
        self.workflow_id: str = info["workflow_id"]
        self.run_id: str = info["run_id"]
        self.workflow_type: str = info["workflow_type"]

    @property
    def is_replaying(self) -> bool:
        return self._r.counter < self._r.max_cmd

    @property
    def history_length(self) -> int:
        return len(self._r.history)

    def now(self) -> float:
        """Workflow time: the timestamp of the last event the code has observed."""
        return self._r.current_time

    def set_stage(self, stage: str) -> None:
        """A short label for the UI ("awaiting manager approval")."""
        self._r.decision.stage = stage

    def get_version(self, change_id: str, min_supported: int, max_supported: int) -> int:
        """Branch on code version safely.

        New executions record max_supported in a VersionMarker event and get it back.
        Histories that passed this point before the change existed get DEFAULT_VERSION (0).
        A recorded version outside [min_supported, max_supported] fails the task clearly.
        """
        r = self._r
        if change_id in r.markers:
            version = r.markers[change_id]
        elif r.counter < r.max_cmd:
            version = DEFAULT_VERSION
        else:
            version = max_supported
            r.markers[change_id] = version
            r.decision.markers.append((change_id, version))
        if not min_supported <= version <= max_supported:
            r.fatal = NonDeterminismError(
                f"get_version({change_id!r}): history has version {version}, "
                f"code supports {min_supported}..{max_supported}"
            )
            raise r.fatal
        return version


class _Blocked(Exception):
    pass


class Replayer:
    def __init__(self, history: list[dict], info: dict) -> None:
        self.history = history
        self.info = info
        self.scheduled: dict[int, dict] = {}
        self.resolutions: dict[int, dict] = {}
        self.signals: list[dict] = []
        self.markers: dict[str, int] = {}
        for ev in history:
            t, a = ev["type"], ev["attrs"]
            if t in SCHEDULE_EVENTS:
                self.scheduled[a["cmd"]] = ev
            elif t in RESOLUTION_EVENTS:
                self.resolutions.setdefault(a["cmd"], ev)
            elif t == "SignalReceived":
                self.signals.append(ev)
            elif t == "VersionMarker":
                self.markers[a["change_id"]] = a["version"]
        self.max_cmd = max(self.scheduled, default=0)
        self.counter = 0
        self.consumed: set[int] = set()
        self.current_time = history[0]["ts"] if history else 0.0
        self.decision = Decision()
        self.fatal: NonDeterminismError | None = None

    # -- command handling ------------------------------------------------------

    def _register(self, cmd: Command) -> int:
        self.counter += 1
        k = self.counter
        ev = self.scheduled.get(k)
        if ev is not None:
            if not matches(cmd, ev):
                raise NonDeterminismError(
                    f"command #{k}: history has {describe_event(ev)}, code issued {describe(cmd)}"
                )
        else:
            self.decision.new_commands.append(NewCommand(k, cmd))
        return k

    def _observe(self, ev: dict) -> None:
        self.current_time = max(self.current_time, ev["ts"])

    def _resolve(self, k: int, cmd: Command) -> tuple[bool, Any, BaseException | None]:
        """(done, value, error) for a single registered command."""
        if isinstance(cmd, WaitSignal):
            return self._resolve_signal(k, cmd)
        ev = self.resolutions.get(k)
        if ev is None:
            return False, None, None
        self._observe(ev)
        a = ev["attrs"]
        if ev["type"] == "ActivityCompleted":
            return True, a.get("result"), None
        if ev["type"] == "ActivityFailed":
            return True, None, ActivityError(a["name"], a["error_type"], a["message"], a["attempts"])
        if ev["type"] == "TimerFired":
            return True, None, None
        if ev["type"] == "ChildCompleted":
            return True, a.get("result"), None
        if ev["type"] == "ChildFailed":
            return True, None, ChildWorkflowError(a["workflow_type"], a["status"], a.get("detail"))
        raise NonDeterminismError(f"command #{k}: unexpected resolution {ev['type']}")

    def _resolve_signal(self, k: int, cmd: WaitSignal) -> tuple[bool, Any, BaseException | None]:
        timeout_ev = self.resolutions.get(k)
        limit = timeout_ev["seq"] if timeout_ev else None
        for ev in self.signals:
            if ev["seq"] in self.consumed or ev["attrs"]["name"] != cmd.name:
                continue
            if limit is not None and ev["seq"] > limit:
                break
            self.consumed.add(ev["seq"])
            self._observe(ev)
            for nc in self.decision.new_commands:
                if nc.cmd_id == k:
                    nc.resolved = True
            if k in self.scheduled:
                self.decision.resolved_waits.append(k)
            return True, ev["attrs"].get("payload"), None
        if timeout_ev is not None:
            self._observe(timeout_ev)
            return True, None, SignalTimeout(cmd.name, cmd.timeout or 0)
        return False, None, None

    def handle(self, cmd: Command) -> tuple[Any, BaseException | None]:
        if isinstance(cmd, Parallel):
            ids = [self._register(c) for c in cmd.commands]
            states = [self._resolve(k, c) for k, c in zip(ids, cmd.commands, strict=True)]
            if not all(done for done, _, _ in states):
                raise _Blocked()
            results = [v for _, v, _ in states]
            errors = [e for _, _, e in states]
            if any(e is not None for e in errors):
                return None, ParallelError(results, errors)
            return results, None
        if not isinstance(cmd, ScheduleActivity | StartTimer | WaitSignal | StartChild):
            raise TypeError(f"workflow yielded {cmd!r}, expected a command")
        k = self._register(cmd)
        done, value, err = self._resolve(k, cmd)
        if not done:
            raise _Blocked()
        return value, err

    # -- driving the workflow function ----------------------------------------

    def run(self, fn: Callable, input: Any) -> Decision:
        d = self.decision
        ctx = WorkflowContext(self, self.info)
        try:
            gen = fn(ctx, input)
        except Exception as e:  # noqa: BLE001 - a plain function that raised
            return self._fail(e)
        if not (inspect.isgenerator(gen) or inspect.iscoroutine(gen)):
            return self._finish(gen)
        send: Any = None
        throw: BaseException | None = None
        try:
            while True:
                try:
                    cmd = gen.throw(throw) if throw is not None else gen.send(send)
                except StopIteration as stop:
                    if self.fatal:
                        return self._nondeterminism(self.fatal)
                    return self._finish(stop.value)
                except NonDeterminismError as e:
                    return self._nondeterminism(e)
                except Exception as e:  # noqa: BLE001 - workflow code failed
                    if self.fatal:
                        return self._nondeterminism(self.fatal)
                    return self._fail(e)
                if self.fatal:
                    return self._nondeterminism(self.fatal)
                if isinstance(cmd, ContinueAsNew):
                    return self._finish(cmd)
                try:
                    send, throw = self.handle(cmd)
                except _Blocked:
                    d.outcome = "blocked"
                    d.commands_replayed = min(self.counter, self.max_cmd)
                    return d
                except NonDeterminismError as e:
                    return self._nondeterminism(e)
        finally:
            gen.close()

    def _finish(self, value: Any) -> Decision:
        d = self.decision
        d.commands_replayed = min(self.counter, self.max_cmd)
        if self.counter < self.max_cmd:
            return self._nondeterminism(
                NonDeterminismError(
                    f"workflow finished after {self.counter} commands, history has {self.max_cmd}"
                )
            )
        if isinstance(value, ContinueAsNew):
            d.outcome, d.result = "continue_as_new", value.input
        elif isinstance(value, Compensated):
            d.outcome, d.result = "compensated", {"reason": value.reason, "details": value.details}
        else:
            d.outcome, d.result = "completed", value
        return d

    def _fail(self, e: BaseException) -> Decision:
        d = self.decision
        d.outcome = "failed"
        d.error_type = getattr(e, "type", None) or type(e).__name__
        d.error = str(e)
        return d

    def _nondeterminism(self, e: NonDeterminismError) -> Decision:
        d = self.decision
        d.new_commands = []
        d.markers = []
        d.outcome = "nondeterminism"
        d.error_type = "NonDeterminismError"
        d.error = str(e)
        return d


def replay(fn: Callable, history: list[dict], info: dict) -> Decision:
    """Run the workflow code against history and return what it decides next."""
    started = history[0]
    return Replayer(history, info).run(fn, started["attrs"].get("input"))
