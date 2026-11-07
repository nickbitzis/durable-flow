"""Chaos run: several worker processes, random activity failures, workers killed with
SIGKILL mid-run, virtual time that skips ahead when nothing is runnable. Afterwards the
invariants in invariants.py must all hold."""

from __future__ import annotations

import os
import random
import sqlite3
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from durableflow.clock import DAY, EPOCH, DbClock
from durableflow.engine import Engine, EngineConfig
from durableflow.invariants import Check, check_all


@dataclass
class ChaosConfig:
    workflows: int = 200
    workers: int = 4
    seed: int = 7
    fail_rate: float = 0.2
    crash_rate: float = 0.02
    kill_every: float = 1.5  # real seconds between SIGKILLs
    rate: float = 20.0  # virtual seconds per real second
    timeout: float = 600.0
    db: str | None = None
    log_dir: str | None = None


@dataclass
class ChaosReport:
    config: ChaosConfig
    checks: list[Check] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return bool(self.checks) and all(c.ok for c in self.checks)

    def render(self) -> str:
        s = self.stats
        lines = [
            f"chaos: {s['workflows']} workflows, {self.config.workers} workers, seed {self.config.seed}",
            (f"  real time {s['real_seconds']:.1f}s, virtual time {s['virtual_days']:.1f} days, "
             f"{s['time_skips']} time skips"),
            f"  workers SIGKILLed {s['kills']}, self-crashed after a side effect {s['crashes']}",
            (f"  activity attempts {s['attempts']}, retries {s['retries']}, lease takeovers "
             f"{s['activity_lease_takeovers']} activity / {s['wf_lease_takeovers']} workflow, "
             f"fenced late reports {s['fenced']}"),
            (f"  workflow tasks {s['workflow_tasks']}, commands replayed from history "
             f"{s['commands_replayed']}, signals sent {s['signals']}"),
            "",
            "invariants:",
        ]
        for c in self.checks:
            lines.append(f"  [{'PASS' if c.ok else 'FAIL'}] {c.name}: {c.detail}")
        lines.append("")
        lines.append("ALL INVARIANTS HOLD" if self.ok else "INVARIANT VIOLATION")
        return "\n".join(lines)


def plan_workflows(n: int, seed: int) -> list[dict]:
    """A seeded mix of the three example workflows with realistic failure cases."""
    rng = random.Random(seed)
    out = []
    for i in range(n):
        roll = rng.random()
        if roll < 0.40:
            r = rng.random()
            inp = {"order_id": f"ord-{i:04d}", "sku": rng.choice(["TENT-2", "PACK-45", "STOVE-1"]),
                   "qty": rng.randint(1, 3), "amount_cents": rng.randint(2_000, 40_000)}
            if r < 0.08:
                inp["card"] = "declined"
            elif r < 0.13:
                inp["sku"] = "OOS-LANTERN"
            elif r < 0.18:
                inp["address"] = "invalid"
            out.append({"type": "order_fulfillment", "id": f"order-{i:04d}", "input": inp,
                        "signal": None})
        elif roll < 0.75:
            r = rng.random()
            plan = "approve" if r < 0.75 else ("reject" if r < 0.85 else "never")
            emp = f"emp-{i:04d}" + ("-seatlimit" if rng.random() < 0.05 else "")
            out.append({"type": "employee_onboarding", "id": f"onboard-{i:04d}",
                        "input": {"employee": emp, "manager": f"mgr-{rng.randint(1, 9)}",
                                  "address": "invalid" if rng.random() < 0.05 else "ok"},
                        "signal": plan})
        else:
            card = "expires-after-2" if rng.random() < 0.15 else "ok"
            out.append({"type": "subscription_renewal", "id": f"sub-{i:04d}",
                        "input": {"customer": f"cust-{i:04d}", "card": card, "max_renewals": 4,
                                  "renewals_per_run": 2, "amount_cents": 1900},
                        "signal": None})
    return out


def _spawn(db: str, wid: str, cfg: ChaosConfig, log_dir: Path) -> subprocess.Popen:
    env = {**os.environ,
           "DURABLEFLOW_FAULTS": f"fail={cfg.fail_rate},crash={cfg.crash_rate},seed={cfg.seed},"
                                 "allow_crash=1",
           "PYTHONUNBUFFERED": "1"}
    logf = open(log_dir / f"{wid}.log", "ab")  # noqa: SIM115 - handed to the child
    return subprocess.Popen(
        [sys.executable, "-m", "durableflow", "worker", "--db", db, "--clock", "db", "--id", wid],
        env=env, stdout=logf, stderr=logf)


def run_chaos(cfg: ChaosConfig, echo: Callable[[str], None] = print) -> ChaosReport:
    tmp = Path(tempfile.mkdtemp(prefix="durableflow-chaos-"))
    db = cfg.db or str(tmp / "chaos.db")
    for suffix in ("", "-wal", "-shm"):
        Path(db + suffix).unlink(missing_ok=True)
    log_dir = Path(cfg.log_dir) if cfg.log_dir else tmp
    log_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db)
    DbClock.init(conn, EPOCH, cfg.rate)
    conn.close()
    clock = DbClock(db)
    engine = Engine(db, clock, EngineConfig())
    rng = random.Random(cfg.seed + 1)

    plan = plan_workflows(cfg.workflows, cfg.seed)
    for p in plan:
        engine.start_workflow(p["type"], p["input"], p["id"])
    pending_signal = {p["id"]: p["signal"] for p in plan if p["signal"] in ("approve", "reject")}
    ids = [p["id"] for p in plan]

    procs = {f"w{i + 1}": _spawn(db, f"w{i + 1}", cfg, log_dir) for i in range(cfg.workers)}
    kills = crashes = skips = signals = 0
    t0 = time.time()
    last_kill = t0
    last_echo = t0
    try:
        while True:
            now_real = time.time()
            if now_real - t0 > cfg.timeout:
                echo("chaos: timed out waiting for workflows")
                break
            for wid, p in list(procs.items()):
                if p.poll() is not None:
                    crashes += p.returncode == 137
                    procs[wid] = _spawn(db, wid, cfg, log_dir)
            if cfg.kill_every and now_real - last_kill >= cfg.kill_every:
                victim = rng.choice(sorted(procs))
                procs[victim].kill()
                procs[victim].wait()
                procs[victim] = _spawn(db, victim, cfg, log_dir)
                kills += 1
                last_kill = now_real
            if pending_signal:
                waiting = engine._q(
                    "SELECT workflow_id FROM runs WHERE status = 'RUNNING' AND "
                    "workflow_type = 'employee_onboarding' AND stage = 'awaiting manager approval'")
                for (wid,) in waiting:
                    plan_sig = pending_signal.pop(wid, None)
                    if plan_sig:
                        engine.signal(wid, "manager_approval",
                                      {"approved": plan_sig == "approve", "by": "manager"})
                        signals += 1
            counts = engine.status_counts()
            running = counts.get("RUNNING", 0)
            if running == 0:
                break
            if engine.is_idle():
                nxt = engine.next_wakeup()
                if nxt is not None:
                    clock.jump_to(nxt + 0.001)
                    skips += 1
            if now_real - last_echo > 5:
                echo(f"  ... {running} running, virtual day "
                     f"{(clock.now() - EPOCH) / DAY:.1f}, kills {kills}")
                last_echo = now_real
            time.sleep(0.02)
    finally:
        for p in procs.values():
            p.kill()
            p.wait()

    report = ChaosReport(cfg)
    report.checks = check_all(engine)
    top = [engine.current_run(i)["status"] for i in ids]
    report.stats = {
        "workflows": len(ids),
        "top_level_completed": top.count("COMPLETED"),
        "top_level_compensated": top.count("COMPENSATED"),
        "real_seconds": time.time() - t0,
        "virtual_days": (clock.now() - EPOCH) / DAY,
        "time_skips": skips,
        "kills": kills,
        "crashes": crashes,
        "signals": signals,
        "attempts": engine._q("SELECT COUNT(*) FROM attempts")[0][0],
        "retries": engine.counter("activity_retries"),
        "activity_lease_takeovers": engine.counter("activity_lease_takeovers"),
        "wf_lease_takeovers": engine.counter("wf_lease_takeovers"),
        "fenced": engine.counter("fenced_completions"),
        "workflow_tasks": engine.counter("workflow_tasks"),
        "commands_replayed": engine.counter("commands_replayed"),
        "db": db,
    }
    engine.close()
    return report
