"""Seed a database with example workflows in every state, for the UI and screenshots."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from durableflow.clock import DAY, EPOCH, DbClock
from durableflow.engine import Engine
from durableflow.examples import examples
from durableflow.worker import FaultInjector, Worker


def _advance_to(engine: Engine, worker: Worker, clock: DbClock, target: float) -> None:
    while True:
        worker.run_until_idle()
        nxt = engine.next_wakeup()
        if nxt is None or nxt > target:
            clock.jump_to(target)
            worker.run_until_idle()
            return
        clock.jump_to(nxt + 0.001)


def seed_demo(db: str, fresh: bool = True) -> dict:
    if fresh:
        for suffix in ("", "-wal", "-shm"):
            Path(db + suffix).unlink(missing_ok=True)
    Path(db).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db)
    DbClock.init(conn, EPOCH, rate=1.0)
    conn.close()
    clock = DbClock(db, cache_s=0)
    engine = Engine(db, clock)
    # A quarter of first attempts fail, so the timelines show retries and backoff.
    worker = Worker(engine, examples, "demo-seeder", FaultInjector(fail_rate=0.25, seed=3))

    start = engine.start_workflow
    start("order_fulfillment", {"order_id": "ord-1001", "sku": "TENT-2", "qty": 1,
                                "amount_cents": 18900}, "order-1001")
    start("order_fulfillment", {"order_id": "ord-1002", "sku": "PACK-45", "qty": 2,
                                "amount_cents": 25800, "card": "declined"}, "order-1002")
    start("order_fulfillment", {"order_id": "ord-1003", "sku": "STOVE-1", "qty": 1,
                                "amount_cents": 7400, "address": "invalid"}, "order-1003")
    start("order_fulfillment", {"order_id": "ord-1004", "sku": "OOS-LANTERN", "qty": 1,
                                "amount_cents": 3900}, "order-1004")
    start("employee_onboarding", {"employee": "dana.okafor", "manager": "lee.marsh"},
          "onboard-dana.okafor")
    start("employee_onboarding", {"employee": "sam.ruiz", "manager": "lee.marsh"},
          "onboard-sam.ruiz")
    start("employee_onboarding", {"employee": "kim.tran", "manager": "ola.berg"},
          "onboard-kim.tran")
    start("employee_onboarding", {"employee": "jo.patel-seatlimit", "manager": "ola.berg"},
          "onboard-jo.patel")
    start("subscription_renewal", {"customer": "cust-77", "max_renewals": 12,
                                   "renewals_per_run": 3, "amount_cents": 1900}, "sub-cust-77")
    start("subscription_renewal", {"customer": "cust-78", "max_renewals": 12,
                                   "renewals_per_run": 3, "card": "expires-after-2",
                                   "amount_cents": 4900}, "sub-cust-78")
    worker.run_until_idle()
    clock.jump_to(clock.now() + 3 * 3600)
    engine.signal("onboard-dana.okafor", "manager_approval", {"approved": True, "by": "lee.marsh"})
    engine.signal("onboard-kim.tran", "manager_approval",
                  {"approved": False, "by": "ola.berg", "note": "role was filled internally"})
    _advance_to(engine, worker, clock, EPOCH + 100 * DAY)

    start("employee_onboarding", {"employee": "ren.adeyemi", "manager": "lee.marsh"},
          "onboard-ren.adeyemi")
    start("employee_onboarding", {"employee": "max.weber", "manager": "ola.berg"},
          "onboard-max.weber")
    start("order_fulfillment", {"order_id": "ord-1005", "sku": "PACK-45", "qty": 1,
                                "amount_cents": 12900}, "order-1005")
    worker.run_until_idle()
    engine.terminate("onboard-max.weber", "duplicate request, see onboard-ren.adeyemi")
    out = engine.status_counts()
    engine.close()
    return out
