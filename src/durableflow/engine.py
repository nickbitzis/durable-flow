"""The engine: SQLite history store, task queues with leases, timer service, client API.

Every state change happens inside one `BEGIN IMMEDIATE` transaction, so appending an event,
creating the tasks it implies, and releasing a lease commit together or not at all.
Several worker processes can share one database file (WAL mode).
"""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from durableflow.clock import Clock, SystemClock
from durableflow.errors import NotFound, NotRunning
from durableflow.workflow import (
    SCHEDULE_EVENTS,
    Decision,
    RetryPolicy,
    ScheduleActivity,
    StartChild,
    StartTimer,
    WaitSignal,
)

TERMINAL = ("COMPLETED", "COMPENSATED", "FAILED", "TERMINATED", "CONTINUED_AS_NEW")

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    workflow_type TEXT NOT NULL,
    status TEXT NOT NULL,
    input TEXT,
    result TEXT,
    error TEXT,
    stage TEXT,
    parent_run_id TEXT,
    parent_cmd INTEGER,
    prev_run_id TEXT,
    started_at REAL NOT NULL,
    closed_at REAL,
    updated_at REAL NOT NULL,
    next_seq INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS runs_wf ON runs(workflow_id, started_at);
CREATE INDEX IF NOT EXISTS runs_status ON runs(status, workflow_type);
CREATE TABLE IF NOT EXISTS history (
    run_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    type TEXT NOT NULL,
    attrs TEXT NOT NULL,
    ts REAL NOT NULL,
    PRIMARY KEY (run_id, seq)
);
CREATE TABLE IF NOT EXISTS wf_tasks (
    run_id TEXT PRIMARY KEY,
    ready_at REAL NOT NULL,
    lease_owner TEXT,
    lease_token TEXT,
    lease_expires REAL,
    dirty INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS act_tasks (
    run_id TEXT NOT NULL,
    cmd INTEGER NOT NULL,
    name TEXT NOT NULL,
    args TEXT NOT NULL,
    kwargs TEXT NOT NULL,
    retry TEXT NOT NULL,
    start_to_close REAL NOT NULL,
    heartbeat_timeout REAL,
    attempt INTEGER NOT NULL DEFAULT 1,
    ready_at REAL NOT NULL,
    scheduled_at REAL NOT NULL,
    started_at REAL,
    lease_owner TEXT,
    lease_token TEXT,
    lease_expires REAL,
    last_heartbeat REAL,
    heartbeat_details TEXT,
    PRIMARY KEY (run_id, cmd)
);
CREATE TABLE IF NOT EXISTS attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    cmd INTEGER NOT NULL,
    name TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    worker TEXT NOT NULL,
    ready_at REAL NOT NULL,
    started_at REAL NOT NULL,
    ended_at REAL,
    outcome TEXT,
    error TEXT
);
CREATE INDEX IF NOT EXISTS attempts_run ON attempts(run_id, cmd);
CREATE TABLE IF NOT EXISTS timers (
    run_id TEXT NOT NULL,
    cmd INTEGER NOT NULL,
    kind TEXT NOT NULL,
    fire_at REAL NOT NULL,
    created_at REAL NOT NULL,
    fired_at REAL,
    fire_order INTEGER,
    canceled INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, cmd)
);
CREATE INDEX IF NOT EXISTS timers_due ON timers(fired_at, canceled, fire_at);
CREATE TABLE IF NOT EXISTS leaders (
    name TEXT PRIMARY KEY,
    owner TEXT,
    expires REAL
);
CREATE TABLE IF NOT EXISTS counters (
    name TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS wf_task_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    worker TEXT NOT NULL,
    ready_at REAL NOT NULL,
    started_at REAL NOT NULL,
    finished_at REAL NOT NULL,
    outcome TEXT NOT NULL,
    replayed INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS side_effects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    idem_key TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    system TEXT NOT NULL,
    action TEXT NOT NULL,
    subject TEXT NOT NULL,
    payload TEXT,
    applied_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS side_effects_key ON side_effects(idem_key);
CREATE INDEX IF NOT EXISTS side_effects_wf ON side_effects(workflow_id);
"""


def _dumps(v: Any) -> str:
    return json.dumps(v, separators=(",", ":"), default=str)


def _loads(s: str | None) -> Any:
    return None if s is None else json.loads(s)


@dataclass
class EngineConfig:
    wf_task_lease: float = 20.0  # seconds a worker may hold a workflow task
    leader_lease: float = 10.0  # timer service leadership
    lease_grace: float = 2.0  # extra time on activity leases before takeover


@dataclass
class WorkflowTask:
    run_id: str
    token: str
    ready_at: float
    started_at: float
    takeover: bool


@dataclass
class ActivityTask:
    run_id: str
    cmd: int
    workflow_id: str
    name: str
    args: list
    kwargs: dict
    retry: RetryPolicy
    start_to_close: float
    heartbeat_timeout: float | None
    attempt: int
    token: str
    ready_at: float
    started_at: float

    @property
    def idempotency_key(self) -> str:
        return f"{self.run_id}:{self.cmd}"


class Engine:
    def __init__(self, db_path: str, clock: Clock | None = None, config: EngineConfig | None = None):
        self.db_path = db_path
        self.clock = clock or SystemClock()
        self.config = config or EngineConfig()
        self._conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("PRAGMA busy_timeout=30000")
            self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------ plumbing

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            c = self._conn
            c.execute("BEGIN IMMEDIATE")
            try:
                yield c
            except BaseException:
                c.execute("ROLLBACK")
                raise
            else:
                c.execute("COMMIT")

    def _q(self, sql: str, args: tuple = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, args).fetchall()

    def _bump(self, c: sqlite3.Connection, name: str, n: int = 1) -> int:
        row = c.execute(
            "INSERT INTO counters(name, value) VALUES (?, ?) ON CONFLICT(name) "
            "DO UPDATE SET value = value + excluded.value RETURNING value",
            (name, n),
        ).fetchone()
        return row[0]

    def counter(self, name: str) -> int:
        rows = self._q("SELECT value FROM counters WHERE name = ?", (name,))
        return rows[0][0] if rows else 0

    def _append(self, c: sqlite3.Connection, run_id: str, type_: str, attrs: dict) -> int:
        seq = c.execute(
            "UPDATE runs SET next_seq = next_seq + 1, updated_at = ? WHERE run_id = ? "
            "RETURNING next_seq - 1",
            (self.clock.now(), run_id),
        ).fetchone()[0]
        c.execute(
            "INSERT INTO history(run_id, seq, type, attrs, ts) VALUES (?, ?, ?, ?, ?)",
            (run_id, seq, type_, _dumps(attrs), self.clock.now()),
        )
        return seq

    def _schedule_wf(self, c: sqlite3.Connection, run_id: str) -> None:
        now = self.clock.now()
        c.execute(
            "INSERT INTO wf_tasks(run_id, ready_at) VALUES (?, ?) ON CONFLICT(run_id) "
            "DO UPDATE SET dirty = 1, ready_at = MIN(ready_at, excluded.ready_at)",
            (run_id, now),
        )

    def _new_run(
        self,
        c: sqlite3.Connection,
        workflow_type: str,
        input: Any,
        workflow_id: str,
        parent_run_id: str | None = None,
        parent_cmd: int | None = None,
        prev_run_id: str | None = None,
    ) -> str:
        run_id = "run_" + uuid.uuid4().hex[:16]
        now = self.clock.now()
        c.execute(
            "INSERT INTO runs(run_id, workflow_id, workflow_type, status, input, parent_run_id,"
            " parent_cmd, prev_run_id, started_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (run_id, workflow_id, workflow_type, "RUNNING", _dumps(input), parent_run_id,
             parent_cmd, prev_run_id, now, now),
        )
        attrs: dict[str, Any] = {"workflow_type": workflow_type, "workflow_id": workflow_id,
                                 "input": input}
        if parent_run_id:
            attrs["parent_run_id"] = parent_run_id
        if prev_run_id:
            attrs["continued_from"] = prev_run_id
        self._append(c, run_id, "WorkflowStarted", attrs)
        self._schedule_wf(c, run_id)
        self._bump(c, "workflows_started")
        return run_id

    # ------------------------------------------------------------ client API

    def start_workflow(self, workflow_type: str, input: Any = None, workflow_id: str | None = None) -> dict:
        """Start a workflow. Starting an id that is already running returns the running one,
        so a client may retry a start safely."""
        workflow_id = workflow_id or f"{workflow_type}-{uuid.uuid4().hex[:10]}"
        with self.tx() as c:
            row = c.execute(
                "SELECT run_id FROM runs WHERE workflow_id = ? AND status = 'RUNNING'", (workflow_id,)
            ).fetchone()
            if row:
                return {"workflow_id": workflow_id, "run_id": row[0], "created": False}
            run_id = self._new_run(c, workflow_type, input, workflow_id)
        return {"workflow_id": workflow_id, "run_id": run_id, "created": True}

    def _current_run(self, c: sqlite3.Connection, workflow_id: str) -> sqlite3.Row:
        row = c.execute(
            "SELECT * FROM runs WHERE workflow_id = ? OR run_id = ? "
            "ORDER BY started_at DESC, rowid DESC LIMIT 1",
            (workflow_id, workflow_id),
        ).fetchone()
        if row is None:
            raise NotFound(f"workflow {workflow_id} not found")
        return row

    def signal(self, workflow_id: str, name: str, payload: Any = None) -> int:
        with self.tx() as c:
            run = self._current_run(c, workflow_id)
            if run["status"] != "RUNNING":
                raise NotRunning(f"workflow {workflow_id} is {run['status']}")
            seq = self._append(c, run["run_id"], "SignalReceived", {"name": name, "payload": payload})
            self._schedule_wf(c, run["run_id"])
        return seq

    def terminate(self, workflow_id: str, reason: str = "terminated by operator") -> None:
        with self.tx() as c:
            run = self._current_run(c, workflow_id)
            if run["status"] != "RUNNING":
                raise NotRunning(f"workflow {workflow_id} is {run['status']}")
            rid = run["run_id"]
            self._append(c, rid, "WorkflowTerminated", {"reason": reason})
            self._close_run(c, rid, "TERMINATED", error=reason)

    def _close_run(self, c: sqlite3.Connection, run_id: str, status: str,
                   result: Any = None, error: str | None = None) -> None:
        now = self.clock.now()
        c.execute(
            "UPDATE runs SET status = ?, result = ?, error = ?, closed_at = ?, updated_at = ? "
            "WHERE run_id = ?",
            (status, _dumps(result), error, now, now, run_id),
        )
        c.execute("DELETE FROM wf_tasks WHERE run_id = ?", (run_id,))
        c.execute("DELETE FROM act_tasks WHERE run_id = ?", (run_id,))
        c.execute("UPDATE timers SET canceled = 1 WHERE run_id = ? AND fired_at IS NULL", (run_id,))
        run = c.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if run["parent_run_id"] and status != "CONTINUED_AS_NEW":
            parent = c.execute(
                "SELECT status FROM runs WHERE run_id = ?", (run["parent_run_id"],)
            ).fetchone()
            if parent and parent["status"] == "RUNNING":
                if status == "COMPLETED":
                    self._append(c, run["parent_run_id"], "ChildCompleted",
                                 {"cmd": run["parent_cmd"], "result": result,
                                  "workflow_type": run["workflow_type"],
                                  "child_workflow_id": run["workflow_id"]})
                else:
                    self._append(c, run["parent_run_id"], "ChildFailed",
                                 {"cmd": run["parent_cmd"], "status": status,
                                  "detail": result if status == "COMPENSATED" else error,
                                  "workflow_type": run["workflow_type"],
                                  "child_workflow_id": run["workflow_id"]})
                self._schedule_wf(c, run["parent_run_id"])

    # ------------------------------------------------------------ reads

    def run(self, run_id: str) -> dict:
        rows = self._q("SELECT * FROM runs WHERE run_id = ?", (run_id,))
        if not rows:
            raise NotFound(run_id)
        return self._run_dict(rows[0])

    def current_run(self, workflow_id: str) -> dict:
        with self._lock:
            return self._run_dict(self._current_run(self._conn, workflow_id))

    @staticmethod
    def _run_dict(r: sqlite3.Row) -> dict:
        d = dict(r)
        d["input"] = _loads(d["input"])
        d["result"] = _loads(d["result"])
        d["history_length"] = d.pop("next_seq") - 1
        return d

    def history(self, run_id: str) -> list[dict]:
        rows = self._q(
            "SELECT seq, type, attrs, ts FROM history WHERE run_id = ? ORDER BY seq", (run_id,)
        )
        return [{"seq": r[0], "type": r[1], "attrs": json.loads(r[2]), "ts": r[3]} for r in rows]

    def runs_of(self, workflow_id: str) -> list[dict]:
        rows = self._q("SELECT * FROM runs WHERE workflow_id = ? ORDER BY started_at, rowid",
                       (workflow_id,))
        return [self._run_dict(r) for r in rows]

    def describe(self, workflow_id: str) -> dict:
        run = self.current_run(workflow_id)
        hist = self.history(run["run_id"])
        return {
            **run,
            "pending": pending_from_history(hist),
            "runs": [
                {"run_id": r["run_id"], "status": r["status"], "started_at": r["started_at"],
                 "history_length": r["history_length"]}
                for r in self.runs_of(run["workflow_id"])
            ],
            "children": [
                self._run_dict(r) | {"history_length": None}
                for r in self._q("SELECT * FROM runs WHERE parent_run_id = ? ORDER BY started_at",
                                 (run["run_id"],))
            ],
        }

    def list_workflows(self, status: str | None = None, workflow_type: str | None = None,
                       q: str | None = None, limit: int = 50, offset: int = 0,
                       top_level: bool = False) -> dict:
        """Latest run per workflow id, newest first."""
        where = [("r.rowid = (SELECT r2.rowid FROM runs r2 WHERE r2.workflow_id = r.workflow_id "
                  "ORDER BY r2.started_at DESC, r2.rowid DESC LIMIT 1)")]
        args: list[Any] = []
        if status:
            where.append("r.status = ?")
            args.append(status.upper())
        if workflow_type:
            where.append("r.workflow_type = ?")
            args.append(workflow_type)
        if q:
            where.append("r.workflow_id LIKE ?")
            args.append(f"%{q}%")
        if top_level:
            where.append("r.parent_run_id IS NULL")
        w = " AND ".join(where)
        total = self._q(f"SELECT COUNT(*) FROM runs r WHERE {w}", tuple(args))[0][0]
        rows = self._q(
            f"SELECT r.* FROM runs r WHERE {w} ORDER BY r.started_at DESC, r.rowid DESC "
            "LIMIT ? OFFSET ?",
            (*args, limit, offset),
        )
        return {"total": total, "items": [self._run_dict(r) for r in rows]}

    def status_counts(self) -> dict[str, int]:
        rows = self._q(
            "SELECT status, COUNT(*) FROM runs r WHERE r.rowid = (SELECT r2.rowid FROM runs r2 "
            "WHERE r2.workflow_id = r.workflow_id ORDER BY r2.started_at DESC, r2.rowid DESC "
            "LIMIT 1) GROUP BY status"
        )
        return {r[0]: r[1] for r in rows}

    # ------------------------------------------------------------ scheduling helpers

    def next_wakeup(self) -> float | None:
        """Earliest future time at which something becomes runnable."""
        rows = self._q(
            "SELECT MIN(t) FROM ("
            " SELECT MIN(fire_at) t FROM timers WHERE fired_at IS NULL AND canceled = 0"
            " UNION ALL SELECT MIN(COALESCE(lease_expires, ready_at)) FROM wf_tasks"
            " UNION ALL SELECT MIN(CASE WHEN lease_owner IS NULL THEN ready_at"
            "   ELSE lease_expires END) FROM act_tasks)"
        )
        return rows[0][0]

    def is_idle(self) -> bool:
        """True when no task is runnable now and no worker holds a live lease."""
        now = self.clock.now()
        rows = self._q(
            "SELECT"
            " (SELECT COUNT(*) FROM wf_tasks WHERE ready_at <= ?1 OR lease_expires > ?1)"
            " + (SELECT COUNT(*) FROM act_tasks WHERE (lease_owner IS NULL AND ready_at <= ?1)"
            "    OR lease_expires IS NOT NULL)"
            " + (SELECT COUNT(*) FROM timers WHERE fired_at IS NULL AND canceled = 0"
            "    AND fire_at <= ?1)",
            (now,),
        )
        return rows[0][0] == 0

    # ------------------------------------------------------------ workflow tasks

    def poll_workflow_task(self, worker: str) -> WorkflowTask | None:
        now = self.clock.now()
        token = uuid.uuid4().hex
        with self.tx() as c:
            row = c.execute(
                "SELECT run_id, ready_at, lease_owner FROM wf_tasks WHERE ready_at <= ?1 AND "
                "(lease_owner IS NULL OR lease_expires < ?1) ORDER BY ready_at LIMIT 1",
                (now,),
            ).fetchone()
            if row is None:
                return None
            takeover = row["lease_owner"] is not None
            if takeover:
                self._bump(c, "wf_lease_takeovers")
            c.execute(
                "UPDATE wf_tasks SET lease_owner = ?, lease_token = ?, lease_expires = ?, dirty = 0 "
                "WHERE run_id = ?",
                (worker, token, now + self.config.wf_task_lease, row["run_id"]),
            )
        return WorkflowTask(row["run_id"], token, row["ready_at"], now, takeover)

    def complete_workflow_task(self, task: WorkflowTask, worker: str, d: Decision) -> bool:
        """Commit a decision. Returns False if the lease was lost (fencing)."""
        now = self.clock.now()
        with self.tx() as c:
            held = c.execute(
                "SELECT dirty FROM wf_tasks WHERE run_id = ? AND lease_token = ?",
                (task.run_id, task.token),
            ).fetchone()
            if held is None:
                self._bump(c, "fenced_completions")
                return False
            run = c.execute("SELECT * FROM runs WHERE run_id = ?", (task.run_id,)).fetchone()
            if run["status"] != "RUNNING":
                c.execute("DELETE FROM wf_tasks WHERE run_id = ?", (task.run_id,))
                return True
            rid = task.run_id
            for change_id, version in d.markers:
                self._append(c, rid, "VersionMarker", {"change_id": change_id, "version": version})
            for nc in d.new_commands:
                self._emit_command(c, run, nc.cmd_id, nc.command, nc.resolved, now)
            for k in d.resolved_waits:
                c.execute("UPDATE timers SET canceled = 1 WHERE run_id = ? AND cmd = ? "
                          "AND fired_at IS NULL", (rid, k))
            if d.stage is not None:
                c.execute("UPDATE runs SET stage = ? WHERE run_id = ?", (d.stage, rid))
            terminal = d.outcome != "blocked"
            if d.outcome == "completed":
                self._append(c, rid, "WorkflowCompleted", {"result": d.result})
                self._close_run(c, rid, "COMPLETED", result=d.result)
            elif d.outcome == "compensated":
                self._append(c, rid, "WorkflowCompensated", d.result)
                self._close_run(c, rid, "COMPENSATED", result=d.result)
            elif d.outcome == "failed":
                self._append(c, rid, "WorkflowFailed", {"error_type": d.error_type, "message": d.error})
                self._close_run(c, rid, "FAILED", error=f"{d.error_type}: {d.error}")
            elif d.outcome == "nondeterminism":
                self._bump(c, "nondeterminism_errors")
                self._append(c, rid, "WorkflowTaskFailed",
                             {"error_type": "NonDeterminismError", "message": d.error})
                self._close_run(c, rid, "FAILED", error=f"NonDeterminismError: {d.error}")
            elif d.outcome == "continue_as_new":
                new_rid = self._new_run(c, run["workflow_type"], d.result, run["workflow_id"],
                                        run["parent_run_id"], run["parent_cmd"], prev_run_id=rid)
                self._append(c, rid, "WorkflowContinuedAsNew", {"new_run_id": new_rid,
                                                                 "input": d.result})
                self._close_run(c, rid, "CONTINUED_AS_NEW", result={"new_run_id": new_rid})
                self._bump(c, "continued_as_new")
            if not terminal:
                if held["dirty"]:
                    c.execute("UPDATE wf_tasks SET lease_owner = NULL, lease_token = NULL, "
                              "lease_expires = NULL, dirty = 0, ready_at = ? WHERE run_id = ?",
                              (now, rid))
                else:
                    c.execute("DELETE FROM wf_tasks WHERE run_id = ?", (rid,))
            c.execute(
                "INSERT INTO wf_task_log(run_id, worker, ready_at, started_at, finished_at, outcome,"
                " replayed) VALUES (?,?,?,?,?,?,?)",
                (rid, worker, task.ready_at, task.started_at, now, d.outcome, d.commands_replayed),
            )
            self._bump(c, "workflow_tasks")
            if d.commands_replayed:
                self._bump(c, "commands_replayed", d.commands_replayed)
        return True

    def _emit_command(self, c: sqlite3.Connection, run: sqlite3.Row, k: int, cmd: Any,
                      resolved: bool, now: float) -> None:
        rid = run["run_id"]
        if isinstance(cmd, ScheduleActivity):
            self._append(c, rid, "ActivityScheduled", {
                "cmd": k, "name": cmd.name, "args": cmd.args, "kwargs": cmd.kwargs,
                "retry": cmd.retry.to_dict(), "start_to_close": cmd.start_to_close,
                "heartbeat_timeout": cmd.heartbeat_timeout})
            c.execute(
                "INSERT INTO act_tasks(run_id, cmd, name, args, kwargs, retry, start_to_close,"
                " heartbeat_timeout, ready_at, scheduled_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (rid, k, cmd.name, _dumps(cmd.args), _dumps(cmd.kwargs),
                 _dumps(cmd.retry.to_dict()), cmd.start_to_close, cmd.heartbeat_timeout, now, now))
        elif isinstance(cmd, StartTimer):
            fire_at = now + cmd.seconds
            self._append(c, rid, "TimerStarted", {"cmd": k, "seconds": cmd.seconds,
                                                  "fire_at": fire_at})
            c.execute("INSERT INTO timers(run_id, cmd, kind, fire_at, created_at) VALUES (?,?,?,?,?)",
                      (rid, k, "timer", fire_at, now))
        elif isinstance(cmd, WaitSignal):
            timeout_at = None if cmd.timeout is None else now + cmd.timeout
            self._append(c, rid, "SignalWaitStarted", {"cmd": k, "name": cmd.name,
                                                       "timeout": cmd.timeout,
                                                       "timeout_at": timeout_at,
                                                       "buffered": resolved})
            if timeout_at is not None and not resolved:
                c.execute("INSERT INTO timers(run_id, cmd, kind, fire_at, created_at) "
                          "VALUES (?,?,?,?,?)", (rid, k, "signal_timeout", timeout_at, now))
        elif isinstance(cmd, StartChild):
            child_wid = cmd.workflow_id or f"{run['workflow_id']}/{cmd.workflow_type}-{k}"
            child_rid = self._new_run(c, cmd.workflow_type, cmd.input, child_wid,
                                      parent_run_id=rid, parent_cmd=k)
            self._append(c, rid, "ChildStarted", {"cmd": k, "workflow_type": cmd.workflow_type,
                                                  "child_workflow_id": child_wid,
                                                  "child_run_id": child_rid})
        else:  # pragma: no cover
            raise TypeError(cmd)

    # ------------------------------------------------------------ activity tasks

    def poll_activity_task(self, worker: str) -> ActivityTask | None:
        token = uuid.uuid4().hex
        with self.tx() as c:
            while True:
                now = self.clock.now()
                row = c.execute(
                    "SELECT a.*, r.workflow_id FROM act_tasks a JOIN runs r USING (run_id) "
                    "WHERE (a.lease_owner IS NULL AND a.ready_at <= ?1) OR a.lease_expires < ?1 "
                    "ORDER BY a.ready_at LIMIT 1",
                    (now,),
                ).fetchone()
                if row is None:
                    return None
                retry = RetryPolicy.from_dict(_loads(row["retry"]))
                attempt = row["attempt"]
                if row["lease_owner"] is not None:
                    # The previous worker died or hung: its attempt timed out.
                    kind = ("HeartbeatTimeout" if row["heartbeat_timeout"] and
                            (row["last_heartbeat"] or 0) + row["heartbeat_timeout"] <= now
                            else "StartToCloseTimeout")
                    c.execute("UPDATE attempts SET outcome = 'lease_expired', error = ?, ended_at = ?"
                              " WHERE run_id = ? AND cmd = ? AND attempt = ? AND outcome IS NULL",
                              (kind, now, row["run_id"], row["cmd"], attempt))
                    self._bump(c, "activity_lease_takeovers")
                    if attempt >= retry.max_attempts:
                        self._activity_failed(c, row, kind, "lease expired on the last attempt",
                                              attempt)
                        continue
                    self._bump(c, "activity_retries")
                    attempt += 1
                hb = row["heartbeat_timeout"]
                lease = now + (min(hb, row["start_to_close"]) if hb else row["start_to_close"])
                c.execute(
                    "UPDATE act_tasks SET lease_owner = ?, lease_token = ?, lease_expires = ?,"
                    " attempt = ?, started_at = ?, last_heartbeat = ? WHERE run_id = ? AND cmd = ?",
                    (worker, token, lease + self.config.lease_grace, attempt, now, now,
                     row["run_id"], row["cmd"]),
                )
                c.execute(
                    "INSERT INTO attempts(run_id, cmd, name, attempt, worker, ready_at, started_at)"
                    " VALUES (?,?,?,?,?,?,?)",
                    (row["run_id"], row["cmd"], row["name"], attempt, worker, row["ready_at"], now),
                )
                return ActivityTask(
                    row["run_id"], row["cmd"], row["workflow_id"], row["name"],
                    _loads(row["args"]), _loads(row["kwargs"]), retry, row["start_to_close"],
                    hb, attempt, token, row["ready_at"], now)

    def _activity_failed(self, c: sqlite3.Connection, row: Any, error_type: str, message: str,
                         attempt: int) -> None:
        self._append(c, row["run_id"], "ActivityFailed", {
            "cmd": row["cmd"], "name": row["name"], "error_type": error_type,
            "message": message, "attempts": attempt})
        c.execute("DELETE FROM act_tasks WHERE run_id = ? AND cmd = ?", (row["run_id"], row["cmd"]))
        self._schedule_wf(c, row["run_id"])
        self._bump(c, "activities_failed")

    def heartbeat(self, task: ActivityTask, details: Any = None) -> bool:
        """Extend the lease. Returns False if this worker no longer owns the task."""
        now = self.clock.now()
        with self.tx() as c:
            row = c.execute("SELECT started_at FROM act_tasks WHERE run_id = ? AND cmd = ? AND "
                            "lease_token = ?", (task.run_id, task.cmd, task.token)).fetchone()
            if row is None:
                return False
            hb = task.heartbeat_timeout or task.start_to_close
            lease = min(now + hb, row["started_at"] + task.start_to_close)
            c.execute("UPDATE act_tasks SET last_heartbeat = ?, heartbeat_details = ?, "
                      "lease_expires = ? WHERE run_id = ? AND cmd = ?",
                      (now, _dumps(details), lease + self.config.lease_grace, task.run_id, task.cmd))
        return True

    def complete_activity(self, task: ActivityTask, result: Any) -> bool:
        now = self.clock.now()
        with self.tx() as c:
            if not self._owns(c, task):
                c.execute("UPDATE attempts SET outcome = 'fenced', ended_at = ? WHERE run_id = ? "
                          "AND cmd = ? AND attempt = ? AND outcome IS NULL",
                          (now, task.run_id, task.cmd, task.attempt))
                self._bump(c, "fenced_completions")
                return False
            c.execute("UPDATE attempts SET outcome = 'ok', ended_at = ? WHERE run_id = ? AND cmd = ? "
                      "AND attempt = ? AND outcome IS NULL", (now, task.run_id, task.cmd, task.attempt))
            self._append(c, task.run_id, "ActivityCompleted", {
                "cmd": task.cmd, "name": task.name, "result": result, "attempt": task.attempt})
            c.execute("DELETE FROM act_tasks WHERE run_id = ? AND cmd = ?", (task.run_id, task.cmd))
            self._schedule_wf(c, task.run_id)
            self._bump(c, "activities_completed")
        return True

    def fail_activity(self, task: ActivityTask, error_type: str, message: str,
                      non_retryable: bool = False) -> str:
        """Record a failed attempt. Returns 'retry', 'failed', or 'fenced'."""
        now = self.clock.now()
        with self.tx() as c:
            if not self._owns(c, task):
                self._bump(c, "fenced_completions")
                return "fenced"
            c.execute("UPDATE attempts SET outcome = 'failed', error = ?, ended_at = ? WHERE "
                      "run_id = ? AND cmd = ? AND attempt = ? AND outcome IS NULL",
                      (f"{error_type}: {message}", now, task.run_id, task.cmd, task.attempt))
            stop = (non_retryable or error_type in task.retry.non_retryable
                    or task.attempt >= task.retry.max_attempts)
            if stop:
                row = {"run_id": task.run_id, "cmd": task.cmd, "name": task.name}
                self._activity_failed(c, row, error_type, message, task.attempt)
                return "failed"
            c.execute(
                "UPDATE act_tasks SET attempt = attempt + 1, ready_at = ?, lease_owner = NULL, "
                "lease_token = NULL, lease_expires = NULL WHERE run_id = ? AND cmd = ?",
                (now + task.retry.delay(task.attempt), task.run_id, task.cmd))
            self._bump(c, "activity_retries")
        return "retry"

    @staticmethod
    def _owns(c: sqlite3.Connection, task: ActivityTask) -> bool:
        return c.execute("SELECT 1 FROM act_tasks WHERE run_id = ? AND cmd = ? AND lease_token = ?",
                         (task.run_id, task.cmd, task.token)).fetchone() is not None

    # ------------------------------------------------------------ timer service

    def fire_due_timers(self, owner: str, limit: int = 200) -> int:
        """Fire due timers in fire_at order. Only the leader (a leased row) fires, so the
        global firing order is a single sequence even with many workers."""
        now = self.clock.now()
        with self.tx() as c:
            row = c.execute("SELECT owner, expires FROM leaders WHERE name = 'timers'").fetchone()
            if row and row["owner"] != owner and row["expires"] >= now:
                return 0
            c.execute("INSERT INTO leaders(name, owner, expires) VALUES ('timers', ?, ?) "
                      "ON CONFLICT(name) DO UPDATE SET owner = excluded.owner, "
                      "expires = excluded.expires", (owner, now + self.config.leader_lease))
            due = c.execute(
                "SELECT t.run_id, t.cmd, t.kind, t.fire_at, r.status FROM timers t "
                "JOIN runs r USING (run_id) WHERE t.fired_at IS NULL AND t.canceled = 0 "
                "AND t.fire_at <= ? ORDER BY t.fire_at, t.created_at, t.run_id, t.cmd LIMIT ?",
                (now, limit),
            ).fetchall()
            for t in due:
                if t["status"] != "RUNNING":
                    c.execute("UPDATE timers SET canceled = 1 WHERE run_id = ? AND cmd = ?",
                              (t["run_id"], t["cmd"]))
                    continue
                order = self._bump(c, "timer_fire_order")
                c.execute("UPDATE timers SET fired_at = ?, fire_order = ? WHERE run_id = ? AND cmd = ?",
                          (now, order, t["run_id"], t["cmd"]))
                ev = "TimerFired" if t["kind"] == "timer" else "SignalWaitTimedOut"
                self._append(c, t["run_id"], ev, {"cmd": t["cmd"], "fire_at": t["fire_at"]})
                self._schedule_wf(c, t["run_id"])
            if due:
                self._bump(c, "timers_fired", len(due))
        return len(due)

    # ------------------------------------------------------------ fake external systems

    def apply_side_effect(self, idem_key: str, workflow_id: str, system: str, action: str,
                          subject: str, payload: Any = None) -> tuple[bool, dict]:
        """A stand-in for an external API that honours idempotency keys: the first call with
        a key applies the effect, later calls with the same key return the first result."""
        now = self.clock.now()
        with self.tx() as c:
            row = c.execute("SELECT * FROM side_effects WHERE idem_key = ? AND action = ?",
                            (idem_key, action)).fetchone()
            if row:
                self._bump(c, "side_effects_deduped")
                return False, dict(row)
            c.execute(
                "INSERT INTO side_effects(idem_key, workflow_id, system, action, subject, payload,"
                " applied_at) VALUES (?,?,?,?,?,?,?)",
                (idem_key, workflow_id, system, action, subject, _dumps(payload), now))
            row = c.execute("SELECT * FROM side_effects WHERE id = last_insert_rowid()").fetchone()
            return True, dict(row)

    def find_side_effect(self, workflow_id: str, system: str, action: str,
                         subject: str) -> dict | None:
        rows = self._q("SELECT * FROM side_effects WHERE workflow_id = ? AND system = ? AND "
                       "action = ? AND subject = ? ORDER BY id LIMIT 1",
                       (workflow_id, system, action, subject))
        return dict(rows[0]) if rows else None

    def side_effects(self, workflow_id: str | None = None) -> list[dict]:
        if workflow_id:
            rows = self._q("SELECT * FROM side_effects WHERE workflow_id = ? ORDER BY id",
                           (workflow_id,))
        else:
            rows = self._q("SELECT * FROM side_effects ORDER BY id")
        return [dict(r) for r in rows]

    # ------------------------------------------------------------ metrics source

    def queue_depths(self) -> dict[str, int]:
        now = self.clock.now()
        r = self._q(
            "SELECT"
            " (SELECT COUNT(*) FROM wf_tasks WHERE lease_owner IS NULL AND ready_at <= ?1),"
            " (SELECT COUNT(*) FROM wf_tasks WHERE lease_owner IS NOT NULL),"
            " (SELECT COUNT(*) FROM act_tasks WHERE lease_owner IS NULL AND ready_at <= ?1),"
            " (SELECT COUNT(*) FROM act_tasks WHERE lease_owner IS NULL AND ready_at > ?1),"
            " (SELECT COUNT(*) FROM act_tasks WHERE lease_owner IS NOT NULL),"
            " (SELECT COUNT(*) FROM timers WHERE fired_at IS NULL AND canceled = 0)",
            (now,),
        )[0]
        return {"workflow_ready": r[0], "workflow_leased": r[1], "activity_ready": r[2],
                "activity_backoff": r[3], "activity_leased": r[4], "timers_pending": r[5]}

    def task_latencies(self, limit: int = 5000) -> dict[str, list[float]]:
        wf = self._q("SELECT started_at - ready_at FROM wf_task_log ORDER BY id DESC LIMIT ?",
                     (limit,))
        act = self._q("SELECT started_at - ready_at FROM attempts ORDER BY id DESC LIMIT ?",
                      (limit,))
        return {"workflow": [max(0.0, r[0]) for r in wf], "activity": [max(0.0, r[0]) for r in act]}


def pending_from_history(hist: list[dict]) -> list[dict]:
    """Commands that were scheduled and not yet resolved, for the 'current state' view."""
    scheduled: dict[int, dict] = {}
    resolved: set[int] = set()
    consumed_waits: set[int] = set()
    signals = [e for e in hist if e["type"] == "SignalReceived"]
    for e in hist:
        if e["type"] in SCHEDULE_EVENTS:
            scheduled[e["attrs"]["cmd"]] = e
        elif "cmd" in e["attrs"] and e["type"] not in ("VersionMarker",):
            resolved.add(e["attrs"]["cmd"])
    terminal = hist and hist[-1]["type"].startswith("Workflow") and hist[-1]["type"] != "WorkflowStarted"
    if terminal:
        return []
    # A wait is pending until a later workflow task consumes a signal; approximate by
    # matching signals that arrived after the wait started.
    for k, e in scheduled.items():
        if e["type"] == "SignalWaitStarted" and (e["attrs"].get("buffered") or any(
            s["attrs"]["name"] == e["attrs"]["name"] and s["seq"] > e["seq"] for s in signals
        )):
            consumed_waits.add(k)
    out = []
    for k, e in sorted(scheduled.items()):
        if k in resolved or k in consumed_waits:
            continue
        a = e["attrs"]
        kind = SCHEDULE_EVENTS[e["type"]]
        item = {"cmd": k, "kind": kind, "since": e["ts"]}
        if kind == "activity":
            item["name"] = a["name"]
        elif kind == "timer":
            item["fire_at"] = a["fire_at"]
        elif kind == "signal":
            item["name"] = a["name"]
            item["timeout_at"] = a["timeout_at"]
        else:
            item["name"] = a["workflow_type"]
            item["child_workflow_id"] = a["child_workflow_id"]
        out.append(item)
    return out
