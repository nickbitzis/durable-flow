"""Workers poll the engine for timer, workflow, and activity work.

A workflow task replays the workflow function from its full history and commits the
decision. An activity task runs one attempt of an activity function in a thread, watches
its start-to-close and heartbeat timeouts, and reports the outcome. Every report is fenced
by the lease token, so a worker that lost its lease cannot overwrite the newer attempt.
"""

from __future__ import annotations

import hashlib
import logging
import os
import threading
import time
import traceback
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from durableflow.engine import ActivityTask, Engine
from durableflow.errors import ApplicationError
from durableflow.logs import log
from durableflow.workflow import replay

logger = logging.getLogger("durableflow.worker")


class Registry:
    def __init__(self) -> None:
        self.workflows: dict[str, Callable] = {}
        self.activities: dict[str, Callable] = {}

    def workflow(self, name: str | None = None) -> Callable:
        def deco(fn: Callable) -> Callable:
            self.workflows[name or fn.__name__] = fn
            return fn
        return deco

    def activity(self, name: str | None = None) -> Callable:
        def deco(fn: Callable) -> Callable:
            self.activities[name or fn.__name__] = fn
            return fn
        return deco

    def merge(self, other: Registry) -> Registry:
        self.workflows.update(other.workflows)
        self.activities.update(other.activities)
        return self


class ActivityContext:
    """Passed as the first argument to every activity."""

    def __init__(self, engine: Engine, task: ActivityTask) -> None:
        self._engine = engine
        self._task = task
        self.attempt = task.attempt
        self.run_id = task.run_id
        self.workflow_id = task.workflow_id
        self.idempotency_key = task.idempotency_key
        self.started_at = task.started_at
        self.last_heartbeat = task.started_at
        self.lease_lost = False

    def now(self) -> float:
        return self._engine.clock.now()

    def heartbeat(self, details: Any = None) -> bool:
        self.last_heartbeat = self._engine.clock.now()
        ok = self._engine.heartbeat(self._task, details)
        self.lease_lost = not ok
        return ok

    def effect(self, system: str, action: str, subject: str, payload: Any = None) -> dict:
        """Call a fake external system with this activity's idempotency key."""
        _, row = self._engine.apply_side_effect(
            self.idempotency_key, self.workflow_id, system, action, subject, payload)
        return row

    def find_effect(self, system: str, action: str, subject: str) -> dict | None:
        return self._engine.find_side_effect(self.workflow_id, system, action, subject)


@dataclass
class FaultInjector:
    """Deterministic faults keyed on (seed, idempotency key, attempt), so every process
    agrees and a run can be reproduced. `crash` exits the process after the activity's
    side effect but before the result is recorded: the worst moment to die."""

    fail_rate: float = 0.0
    crash_rate: float = 0.0
    seed: int = 0
    allow_crash: bool = False

    def _roll(self, tag: str, key: str, attempt: int) -> float:
        h = hashlib.sha256(f"{self.seed}:{tag}:{key}:{attempt}".encode()).digest()
        return int.from_bytes(h[:8], "big") / 2**64

    def before(self, task: ActivityTask) -> None:
        if self._roll("fail", task.idempotency_key, task.attempt) < self.fail_rate:
            raise ApplicationError("injected transient failure", type="TransientError")

    def after(self, task: ActivityTask) -> None:
        if self.allow_crash and self._roll("crash", task.idempotency_key, task.attempt) < self.crash_rate:
            log(logger, "injected crash after side effect", level=logging.WARNING,
                run_id=task.run_id, cmd=task.cmd, attempt=task.attempt)
            os._exit(137)

    @classmethod
    def from_env(cls) -> FaultInjector | None:
        spec = os.environ.get("DURABLEFLOW_FAULTS")
        if not spec:
            return None
        kv = dict(part.split("=", 1) for part in spec.split(",") if "=" in part)
        return cls(float(kv.get("fail", 0)), float(kv.get("crash", 0)), int(kv.get("seed", 0)),
                   allow_crash=kv.get("allow_crash", "0") == "1")


class Worker:
    def __init__(self, engine: Engine, registry: Registry, worker_id: str | None = None,
                 faults: FaultInjector | None = None, poll_interval: float = 0.01) -> None:
        self.engine = engine
        self.registry = registry
        self.worker_id = worker_id or f"worker-{os.getpid()}-{uuid.uuid4().hex[:4]}"
        self.faults = faults
        self.poll_interval = poll_interval
        self.stats = {"workflow_tasks": 0, "activity_attempts": 0, "timers": 0}

    # -- one unit of work each -------------------------------------------------

    def run_timers(self) -> int:
        n = self.engine.fire_due_timers(self.worker_id)
        self.stats["timers"] += n
        return n

    def run_workflow_task(self) -> bool:
        task = self.engine.poll_workflow_task(self.worker_id)
        if task is None:
            return False
        run = self.engine.run(task.run_id)
        hist = self.engine.history(task.run_id)
        fn = self.registry.workflows.get(run["workflow_type"])
        if fn is None:
            from durableflow.workflow import Decision
            d = Decision(outcome="failed", error_type="UnknownWorkflowType",
                         error=f"no workflow registered as {run['workflow_type']!r}")
        else:
            d = replay(fn, hist, run)
        ok = self.engine.complete_workflow_task(task, self.worker_id, d)
        self.stats["workflow_tasks"] += 1
        if d.outcome == "nondeterminism":
            log(logger, "non-determinism detected", level=logging.ERROR, run_id=task.run_id,
                workflow_id=run["workflow_id"], error=d.error)
        elif d.outcome != "blocked":
            log(logger, "workflow closed", run_id=task.run_id, workflow_id=run["workflow_id"],
                outcome=d.outcome, worker=self.worker_id)
        if not ok:
            log(logger, "workflow task lease lost, decision discarded", level=logging.WARNING,
                run_id=task.run_id)
        return True

    def run_activity_task(self) -> bool:
        task = self.engine.poll_activity_task(self.worker_id)
        if task is None:
            return False
        self.stats["activity_attempts"] += 1
        fn = self.registry.activities.get(task.name)
        if fn is None:
            self.engine.fail_activity(task, "UnknownActivity", f"no activity named {task.name!r}",
                                      non_retryable=True)
            return True
        ctx = ActivityContext(self.engine, task)
        box: dict[str, Any] = {}

        def target() -> None:
            try:
                if self.faults:
                    self.faults.before(task)
                box["result"] = fn(ctx, *task.args, **task.kwargs)
            except BaseException as e:  # noqa: BLE001 - reported as an activity failure
                box["error"] = e
                box["tb"] = traceback.format_exc(limit=3)

        t = threading.Thread(target=target, daemon=True, name=f"act-{task.name}")
        t.start()
        timeout_kind = None
        while True:
            t.join(0.002)
            timeout_kind = self._timed_out(task, ctx)
            if not t.is_alive() or timeout_kind:
                break
        if timeout_kind:
            outcome = self.engine.fail_activity(task, timeout_kind, "activity exceeded its timeout")
            log(logger, "activity timed out", level=logging.WARNING, activity=task.name,
                run_id=task.run_id, attempt=task.attempt, kind=timeout_kind, outcome=outcome)
            return True
        if "error" in box:
            e = box["error"]
            etype = getattr(e, "type", None) or type(e).__name__
            outcome = self.engine.fail_activity(task, etype, str(e),
                                                non_retryable=getattr(e, "non_retryable", False))
            log(logger, "activity attempt failed", level=logging.INFO, activity=task.name,
                run_id=task.run_id, attempt=task.attempt, error_type=etype, outcome=outcome)
            return True
        if self.faults:
            self.faults.after(task)
        self.engine.complete_activity(task, box.get("result"))
        return True

    def _timed_out(self, task: ActivityTask, ctx: ActivityContext) -> str | None:
        now = self.engine.clock.now()
        if now - task.started_at > task.start_to_close:
            return "StartToCloseTimeout"
        if task.heartbeat_timeout and now - ctx.last_heartbeat > task.heartbeat_timeout:
            return "HeartbeatTimeout"
        return None

    # -- loops -----------------------------------------------------------------

    def run_once(self) -> bool:
        did = self.run_timers() > 0
        did = self.run_workflow_task() or did
        did = self.run_activity_task() or did
        return did

    def run_until_idle(self, max_steps: int = 100_000) -> int:
        steps = 0
        while steps < max_steps and self.run_once():
            steps += 1
        return steps

    def run_forever(self, stop: threading.Event | None = None) -> None:
        stop = stop or threading.Event()
        log(logger, "worker started", worker=self.worker_id, pid=os.getpid())
        while not stop.is_set():
            try:
                if not self.run_once():
                    time.sleep(self.poll_interval)
            except Exception as e:  # noqa: BLE001 - keep polling; the lease makes the task safe to retry
                log(logger, "worker loop error", level=logging.ERROR, error=repr(e))
                time.sleep(0.05)


def drain(engine: Engine, worker: Worker, clock: Any, until: Callable[[], bool] | None = None,
          max_rounds: int = 10_000) -> None:
    """Run a single in-process worker and skip virtual time forward whenever it goes idle.
    Used by tests and the demo seeder."""
    for _ in range(max_rounds):
        worker.run_until_idle()
        if until is not None and until():
            return
        nxt = engine.next_wakeup()
        if nxt is None:
            return
        clock.set(max(clock.now(), nxt) + 1e-6)
