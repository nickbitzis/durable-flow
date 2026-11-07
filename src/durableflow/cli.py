"""Command line: server, worker, demo, chaos, replay, invariants, healthcheck."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import urllib.request

from durableflow.clock import Clock, DbClock, SystemClock

DEFAULT_DB = os.environ.get("DURABLEFLOW_DB", "data/durableflow.db")


def _clock(db: str, kind: str) -> Clock:
    if kind == "auto":
        try:
            conn = sqlite3.connect(db)
            has = conn.execute("SELECT 1 FROM sqlite_master WHERE name = 'clock'").fetchone()
            conn.close()
        except sqlite3.Error:
            has = None
        kind = "db" if has else "system"
    return DbClock(db) if kind == "db" else SystemClock()


def _registry():
    from durableflow.examples import examples
    return examples


def cmd_worker(a: argparse.Namespace) -> int:
    from durableflow.engine import Engine
    from durableflow.logs import setup_logging
    from durableflow.worker import FaultInjector, Worker

    setup_logging(a.log_level)
    os.makedirs(os.path.dirname(os.path.abspath(a.db)), exist_ok=True)
    engine = Engine(a.db, _clock(a.db, a.clock))
    Worker(engine, _registry(), a.id, FaultInjector.from_env()).run_forever()
    return 0


def cmd_server(a: argparse.Namespace) -> int:
    import uvicorn

    from durableflow.api import create_app
    from durableflow.engine import Engine
    from durableflow.logs import setup_logging

    setup_logging(a.log_level)
    os.makedirs(os.path.dirname(os.path.abspath(a.db)), exist_ok=True)
    engine = Engine(a.db, _clock(a.db, a.clock))
    app = create_app(engine, _registry(), embedded_workers=a.workers)
    uvicorn.run(app, host=a.host, port=a.port, log_level="warning", access_log=False)
    return 0


def cmd_demo(a: argparse.Namespace) -> int:
    from durableflow.demo import seed_demo

    counts = seed_demo(a.db, fresh=True)
    print(f"seeded {a.db}: " + ", ".join(f"{k.lower()} {v}" for k, v in sorted(counts.items())))
    print(f"next: durableflow server --db {a.db} --workers 1 --port 8823")
    return 0


def cmd_chaos(a: argparse.Namespace) -> int:
    from durableflow.chaos import ChaosConfig, run_chaos

    cfg = ChaosConfig(workflows=a.workflows, workers=a.workers, seed=a.seed,
                      fail_rate=a.fail_rate, crash_rate=a.crash_rate, kill_every=a.kill_every,
                      rate=a.rate, timeout=a.timeout, db=a.db, log_dir=a.log_dir)
    report = run_chaos(cfg)
    text = report.render()
    print(text)
    if a.out:
        with open(a.out, "w") as f:
            f.write(text + "\n")
    return 0 if report.ok else 1


def cmd_replay(a: argparse.Namespace) -> int:
    """Replay recorded histories against the current code: the check to run before a deploy."""
    from durableflow.engine import Engine
    from durableflow.workflow import replay

    engine = Engine(a.db, SystemClock())
    reg = _registry()
    ids = [a.workflow_id] if a.workflow_id else [
        r["run_id"] for r in engine._q("SELECT run_id FROM runs")]
    bad = 0
    for rid in ids:
        run = engine.current_run(rid) if a.workflow_id else engine.run(rid)
        hist = engine.history(run["run_id"])
        if hist and hist[-1]["type"] == "WorkflowTaskFailed":
            continue
        # Replay only the part of history the code produced, not the terminal event.
        body = [e for e in hist if not e["type"].startswith("Workflow") or e["seq"] == 1]
        d = replay(reg.workflows[run["workflow_type"]], body, run)
        status = "nondeterministic" if d.outcome == "nondeterminism" else "ok"
        bad += status != "ok"
        if a.workflow_id or status != "ok":
            print(f"{run['workflow_id']} {run['run_id']}: {status} "
                  f"({d.commands_replayed} commands replayed){' ' + d.error if d.error else ''}")
    print(f"replayed {len(ids)} runs, {bad} nondeterministic")
    return 1 if bad else 0


def cmd_invariants(a: argparse.Namespace) -> int:
    from durableflow.engine import Engine
    from durableflow.invariants import check_all

    engine = Engine(a.db, SystemClock())
    checks = check_all(engine)
    for c in checks:
        print(f"[{'PASS' if c.ok else 'FAIL'}] {c.name}: {c.detail}")
    return 0 if all(c.ok for c in checks) else 1


def cmd_healthcheck(a: argparse.Namespace) -> int:
    if a.url:
        try:
            with urllib.request.urlopen(a.url, timeout=3) as r:
                return 0 if json.load(r).get("ok") else 1
        except (OSError, ValueError):
            return 1
    try:  # worker mode: the database must be reachable
        sqlite3.connect(a.db, timeout=3).execute("SELECT 1")
        return 0
    except sqlite3.Error:
        return 1


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(prog="durableflow", description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--db", default=DEFAULT_DB)
        p.add_argument("--log-level", default=os.environ.get("DURABLEFLOW_LOG_LEVEL", "INFO"))

    p = sub.add_parser("worker", help="poll and run workflow, activity, and timer tasks")
    common(p)
    p.add_argument("--clock", choices=["auto", "system", "db"], default="auto")
    p.add_argument("--id", default=None)
    p.set_defaults(fn=cmd_worker)

    p = sub.add_parser("server", help="HTTP API and web UI")
    common(p)
    p.add_argument("--host", default=os.environ.get("DURABLEFLOW_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("DURABLEFLOW_PORT", "8823")))
    p.add_argument("--workers", type=int, default=0, help="worker threads inside the server")
    p.add_argument("--clock", choices=["auto", "system", "db"], default="auto")
    p.set_defaults(fn=cmd_server)

    p = sub.add_parser("demo", help="seed a database with example workflows in every state")
    common(p)
    p.set_defaults(fn=cmd_demo)

    p = sub.add_parser("chaos", help="many workers, random failures, kills; check invariants")
    p.add_argument("--workflows", type=int, default=200)
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--fail-rate", type=float, default=0.2)
    p.add_argument("--crash-rate", type=float, default=0.02)
    p.add_argument("--kill-every", type=float, default=1.5)
    p.add_argument("--rate", type=float, default=20.0)
    p.add_argument("--timeout", type=float, default=600.0)
    p.add_argument("--db", default=None)
    p.add_argument("--log-dir", default=None)
    p.add_argument("--out", default=None, help="also write the report to this file")
    p.set_defaults(fn=cmd_chaos)

    p = sub.add_parser("replay", help="replay histories against current code")
    common(p)
    p.add_argument("--workflow-id", default=None)
    p.set_defaults(fn=cmd_replay)

    p = sub.add_parser("invariants", help="check invariants on a database")
    common(p)
    p.set_defaults(fn=cmd_invariants)

    p = sub.add_parser("healthcheck", help="exit 0 when healthy (for Docker HEALTHCHECK)")
    common(p)
    p.add_argument("--url", default=None)
    p.set_defaults(fn=cmd_healthcheck)

    a = ap.parse_args(argv)
    sys.exit(a.fn(a))


if __name__ == "__main__":
    main()
