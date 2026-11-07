"""The chaos run at small size, plus checks that the invariant checker can fail."""

from durableflow import Engine, FaultInjector, VirtualClock, Worker, drain
from durableflow.chaos import ChaosConfig, plan_workflows, run_chaos
from durableflow.engine import ActivityTask
from durableflow.examples import examples
from durableflow.invariants import check_all, check_effects_once, check_timers
from durableflow.workflow import RetryPolicy


def test_chaos_small_run_holds_all_invariants(tmp_path):
    cfg = ChaosConfig(workflows=24, workers=3, seed=11, kill_every=0.6, timeout=90,
                      db=str(tmp_path / "chaos.db"), log_dir=str(tmp_path / "logs"))
    report = run_chaos(cfg, echo=lambda s: None)
    assert report.ok, report.render()
    assert report.stats["kills"] >= 1
    assert report.stats["retries"] > 0
    assert "ALL INVARIANTS HOLD" in report.render()


def test_plan_is_seeded():
    assert plan_workflows(30, 5) == plan_workflows(30, 5)
    kinds = {p["type"] for p in plan_workflows(60, 5)}
    assert kinds == {"order_fulfillment", "employee_onboarding", "subscription_renewal"}


def test_fault_injector_is_deterministic():
    task = ActivityTask("r", 1, "w", "a", [], {}, RetryPolicy(), 30, None, 1, "t", 0, 0)
    f = FaultInjector(fail_rate=0.5, seed=1)
    rolls = [f._roll("fail", f"k{i}", 1) for i in range(200)]
    assert rolls == [FaultInjector(fail_rate=0.5, seed=1)._roll("fail", f"k{i}", 1)
                     for i in range(200)]
    assert 70 < sum(r < 0.5 for r in rolls) < 130
    FaultInjector(fail_rate=0.0).before(task)  # never raises at rate 0


def _finished_db(tmp_path):
    clock = VirtualClock()
    engine = Engine(str(tmp_path / "inv.db"), clock)
    worker = Worker(engine, examples, "w1", FaultInjector(fail_rate=0.3, seed=2))
    for p in plan_workflows(12, 3):
        engine.start_workflow(p["type"], p["input"], p["id"])
    worker.run_until_idle()
    for r in engine._q("SELECT workflow_id FROM runs WHERE stage = 'awaiting manager approval'"):
        engine.signal(r[0], "manager_approval", {"approved": True})
    drain(engine, worker, clock)
    return engine


def test_invariants_hold_in_process_with_faults(tmp_path):
    engine = _finished_db(tmp_path)
    checks = check_all(engine)
    assert all(c.ok for c in checks), [c for c in checks if not c.ok]


def test_checker_catches_a_duplicate_side_effect(tmp_path):
    engine = _finished_db(tmp_path)
    with engine.tx() as c:
        c.execute("INSERT INTO side_effects(idem_key, workflow_id, system, action, subject,"
                  " applied_at) SELECT idem_key, workflow_id, system, action, subject, applied_at"
                  " FROM side_effects LIMIT 1")
    assert check_effects_once(engine).ok is False


def test_checker_catches_out_of_order_timers(tmp_path):
    engine = _finished_db(tmp_path)
    rows = engine._q("SELECT run_id, cmd, fire_order FROM timers WHERE fire_order IS NOT NULL "
                     "ORDER BY fire_order LIMIT 2")
    assert len(rows) == 2
    with engine.tx() as c:
        c.execute("UPDATE timers SET fire_at = fire_at + 1e9 WHERE run_id = ? AND cmd = ?",
                  (rows[0][0], rows[0][1]))
    assert check_timers(engine).ok is False
