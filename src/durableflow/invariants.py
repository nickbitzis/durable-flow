"""Invariant checks run after the chaos test (and in tests). Each returns a named pass/fail
with a detail line, so CI can gate on them and a person can read why."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from itertools import pairwise

from durableflow.engine import Engine
from durableflow.examples import UNDO_OF

OK_STATUSES = {"COMPLETED", "COMPENSATED"}


@dataclass
class Check:
    name: str
    ok: bool
    detail: str


def _latest_runs(engine: Engine) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for r in engine._q("SELECT run_id, workflow_id, workflow_type, status, parent_run_id, result "
                       "FROM runs ORDER BY started_at, rowid"):
        out[r["workflow_id"]] = dict(r)
    return out


def check_terminal(engine: Engine) -> Check:
    latest = _latest_runs(engine)
    counts: dict[str, int] = defaultdict(int)
    for r in latest.values():
        counts[r["status"]] += 1
    bad = [w for w, r in latest.items() if r["status"] not in OK_STATUSES]
    detail = ", ".join(f"{k.lower()} {v}" for k, v in sorted(counts.items()))
    if bad:
        detail += f"; not finished cleanly: {bad[:5]}"
    return Check("every workflow completed or compensated", not bad,
                 f"{len(latest)} workflows (incl. children): {detail}")


def check_activity_resolution(engine: Engine) -> Check:
    sched: dict[tuple, int] = defaultdict(int)
    resolved: dict[tuple, int] = defaultdict(int)
    for r in engine._q("SELECT h.run_id, h.type, h.attrs FROM history h JOIN runs USING (run_id) "
                       "WHERE h.type IN ('ActivityScheduled','ActivityCompleted','ActivityFailed')"):
        key = (r["run_id"], json.loads(r["attrs"])["cmd"])
        if r["type"] == "ActivityScheduled":
            sched[key] += 1
        else:
            resolved[key] += 1
    open_runs = {r[0] for r in engine._q("SELECT run_id FROM runs WHERE status = 'RUNNING'")}
    bad = [k for k in sched if sched[k] != 1 or (k[0] not in open_runs and resolved[k] != 1)]
    bad += [k for k in resolved if k not in sched or resolved[k] > 1]
    return Check("each activity scheduled once and resolved once", not bad,
                 f"{len(sched)} activities, {len(bad)} with a bad event count")


def check_effects_once(engine: Engine) -> Check:
    dup = engine._q("SELECT idem_key, action, COUNT(*) n FROM side_effects GROUP BY idem_key, action "
                    "HAVING n > 1")
    ok_attempts = {(r[0], r[1]): r[2] for r in engine._q(
        "SELECT run_id, cmd, COUNT(*) FROM attempts WHERE outcome = 'ok' GROUP BY run_id, cmd")}
    multi_ok = [k for k, n in ok_attempts.items() if n > 1]
    compensated = {r[0] for r in engine._q("SELECT run_id FROM runs WHERE status = 'COMPENSATED'")}
    effects = engine._q("SELECT idem_key, workflow_id, system, action, subject FROM side_effects")
    present = {(r[1], r[2], r[3], r[4]) for r in effects}
    orphan = undone = 0
    for key, wid, system, action, subject in effects:
        run_id, cmd = key.rsplit(":", 1)
        if ok_attempts.get((run_id, int(cmd)), 0) == 1:
            continue
        # The attempt applied its effect and then died before reporting, and every later
        # attempt failed. That is only acceptable if the saga undid it (checked below).
        if run_id in compensated and (wid, system, UNDO_OF.get(action), subject) in present:
            undone += 1
        else:
            orphan += 1
    attempts = engine._q("SELECT COUNT(*) FROM attempts")[0][0]
    deduped = engine.counter("side_effects_deduped")
    ok = not dup and not multi_ok and orphan == 0
    return Check(
        "side effects applied at most once per successful attempt", ok,
        f"{len(effects)} effects, {attempts} attempts, {deduped} repeat calls absorbed by "
        f"idempotency keys, {len(dup)} duplicates, {undone} unreported effects undone by "
        f"compensation, {orphan} unaccounted effects")


def check_compensation(engine: Engine) -> Check:
    latest = _latest_runs(engine)
    by_wf: dict[str, list[dict]] = defaultdict(list)
    for e in engine.side_effects():
        by_wf[e["workflow_id"]].append(e)
    undo_actions = set(UNDO_OF.values())
    bad: list[str] = []
    compensated = 0
    for wid, r in latest.items():
        effs = by_wf.get(wid, [])
        # Forward steps in the order the workflow issued them (command number), so the
        # branches of a parallel block count in code order rather than finish order.
        fwd = sorted((e for e in effs if e["action"] in UNDO_OF),
                     key=lambda e: int(e["idem_key"].rsplit(":", 1)[1]))
        undo = [e for e in effs if e["action"] in undo_actions]
        if r["status"] == "COMPLETED":
            if undo:
                bad.append(f"{wid}: completed but has undo effects")
            continue
        if r["status"] != "COMPENSATED" or not fwd:
            continue
        compensated += 1
        expected = [(e["system"], UNDO_OF[e["action"]], e["subject"]) for e in reversed(fwd)]
        got = [(e["system"], e["action"], e["subject"]) for e in undo]
        if got != expected or min(u["id"] for u in undo) < max(f["id"] for f in fwd):
            bad.append(f"{wid}: undo {got} expected {expected}")
    return Check("compensations ran for every step, in reverse order", not bad,
                 f"{compensated} compensated workflows with side effects checked"
                 + (f"; {bad[:2]}" if bad else ""))


def check_timers(engine: Engine) -> Check:
    rows = engine._q("SELECT run_id, cmd, kind, fire_at, fired_at, fire_order, canceled "
                     "FROM timers ORDER BY fire_order")
    fired = [r for r in rows if r["fired_at"] is not None]
    early = [r for r in fired if r["fired_at"] < r["fire_at"] - 1e-6]
    out_of_order = sum(1 for a, b in pairwise(fired)
                       if b["fire_at"] < a["fire_at"] - 1e-6)
    open_runs = {r[0] for r in engine._q("SELECT run_id FROM runs WHERE status = 'RUNNING'")}
    lost = [r for r in rows if r["fired_at"] is None and not r["canceled"]
            and r["run_id"] not in open_runs]
    started = defaultdict(int)
    fired_ev = defaultdict(int)
    for r in engine._q("SELECT run_id, type, attrs FROM history WHERE type IN "
                       "('TimerStarted','TimerFired')"):
        k = (r["run_id"], json.loads(r["attrs"])["cmd"])
        (started if r["type"] == "TimerStarted" else fired_ev)[k] += 1
    plain = [k for k in started if started[k] != 1 or fired_ev[k] > 1]
    ok = not early and out_of_order == 0 and not lost and not plain
    return Check("timers fired once, never early, in fire-time order", ok,
                 f"{len(fired)} fired ({sum(1 for r in fired if r['kind'] == 'timer')} sleeps, "
                 f"{sum(1 for r in fired if r['kind'] != 'timer')} signal timeouts), "
                 f"{sum(r['canceled'] for r in rows)} canceled, {len(early)} early, "
                 f"{out_of_order} out of order, {len(lost)} lost")


def check_history(engine: Engine, cap: int = 60) -> Check:
    bad = engine._q("SELECT run_id FROM runs r WHERE (SELECT COUNT(*) FROM history h WHERE "
                    "h.run_id = r.run_id) != r.next_seq - 1 OR (SELECT MAX(seq) FROM history h "
                    "WHERE h.run_id = r.run_id) != r.next_seq - 1")
    longest = engine._q("SELECT MAX(next_seq - 1) FROM runs")[0][0] or 0
    chains = engine._q("SELECT COUNT(*) FROM runs WHERE status = 'CONTINUED_AS_NEW'")[0][0]
    ok = not bad and longest <= cap
    return Check("histories contiguous and capped by continue-as-new", ok,
                 f"longest history {longest} events (cap {cap}), {chains} continue-as-new hand-offs, "
                 f"{len(bad)} runs with gaps")


def check_all(engine: Engine) -> list[Check]:
    return [check_terminal(engine), check_activity_resolution(engine), check_effects_once(engine),
            check_compensation(engine), check_timers(engine), check_history(engine)]
