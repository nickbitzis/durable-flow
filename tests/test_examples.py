"""The Kestrel Supply example workflows: sagas, approvals, continue-as-new."""

from durableflow import DAY, Engine, VirtualClock, Worker, drain
from durableflow.examples import examples


def make(tmp_path):
    clock = VirtualClock()
    engine = Engine(str(tmp_path / "ex.db"), clock)
    worker = Worker(engine, examples, "w1")
    return engine, worker, clock


def effects(engine, wid):
    return [(e["system"], e["action"]) for e in engine.side_effects(wid)]


ORDER = {"order_id": "o1", "sku": "TENT-2", "qty": 1, "amount_cents": 18900}


def test_order_happy_path(tmp_path):
    engine, worker, clock = make(tmp_path)
    wid = engine.start_workflow("order_fulfillment", ORDER, "o1")["workflow_id"]
    drain(engine, worker, clock)
    run = engine.current_run(wid)
    assert run["status"] == "COMPLETED"
    assert effects(engine, wid) == [("inventory", "reserve"), ("payments", "charge"),
                                    ("shipping", "ship")]


def test_card_declined_releases_inventory(tmp_path):
    engine, worker, clock = make(tmp_path)
    wid = engine.start_workflow("order_fulfillment", {**ORDER, "card": "declined"})["workflow_id"]
    drain(engine, worker, clock)
    run = engine.current_run(wid)
    assert run["status"] == "COMPENSATED"
    assert run["result"]["reason"] == "charge_payment failed: CardDeclined"
    # The refund step ran (registered before the charge) but had nothing to undo.
    assert effects(engine, wid) == [("inventory", "reserve"), ("inventory", "release")]


def test_shipping_failure_undoes_in_reverse_order(tmp_path):
    engine, worker, clock = make(tmp_path)
    wid = engine.start_workflow("order_fulfillment", {**ORDER, "address": "invalid"})["workflow_id"]
    drain(engine, worker, clock)
    assert effects(engine, wid) == [("inventory", "reserve"), ("payments", "charge"),
                                    ("payments", "refund"), ("inventory", "release")]


def test_onboarding_approved_end_to_end(tmp_path):
    engine, worker, clock = make(tmp_path)
    wid = engine.start_workflow("employee_onboarding", {"employee": "dana", "manager": "lee"},
                                "ob")["workflow_id"]
    worker.run_until_idle()
    assert engine.current_run(wid)["stage"] == "awaiting manager approval"
    clock.advance(2 * DAY)
    engine.signal(wid, "manager_approval", {"approved": True, "by": "lee"})
    drain(engine, worker, clock)
    run = engine.current_run(wid)
    assert run["status"] == "COMPLETED"
    assert len(run["result"]["accounts"]) == 4
    assert engine.current_run("ob/order_fulfillment-6")["status"] == "COMPLETED"
    types = [e["type"] for e in engine.history(run["run_id"])]
    assert "VersionMarker" in types and "ChildCompleted" in types
    assert ("shipping", "ship_welcome_kit") in effects(engine, wid)


def test_onboarding_times_out_after_seven_days(tmp_path):
    engine, worker, clock = make(tmp_path)
    wid = engine.start_workflow("employee_onboarding",
                                {"employee": "sam", "manager": "lee"})["workflow_id"]
    drain(engine, worker, clock)
    run = engine.current_run(wid)
    assert run["status"] == "COMPENSATED"
    assert run["result"]["reason"] == "manager approval timed out after 7 days"
    assert run["closed_at"] - run["started_at"] >= 7 * DAY
    deletes = [e["system"] for e in engine.side_effects(wid) if e["action"] == "delete_account"]
    assert deletes == ["chat", "payroll", "email", "directory"]


def test_onboarding_rejected(tmp_path):
    engine, worker, clock = make(tmp_path)
    wid = engine.start_workflow("employee_onboarding",
                                {"employee": "kim", "manager": "ola"})["workflow_id"]
    worker.run_until_idle()
    engine.signal(wid, "manager_approval", {"approved": False})
    drain(engine, worker, clock)
    assert engine.current_run(wid)["result"]["reason"] == "rejected by manager"


def test_onboarding_partial_account_failure_compensates(tmp_path):
    engine, worker, clock = make(tmp_path)
    wid = engine.start_workflow("employee_onboarding",
                                {"employee": "jo-seatlimit", "manager": "ola"})["workflow_id"]
    drain(engine, worker, clock)
    run = engine.current_run(wid)
    assert run["result"]["reason"] == "account creation failed in chat"
    e = effects(engine, wid)
    assert e.count(("chat", "create_account")) == 0
    for s in ("directory", "email", "payroll"):
        assert (s, "delete_account") in e


def test_onboarding_laptop_failure_compensates(tmp_path):
    engine, worker, clock = make(tmp_path)
    wid = engine.start_workflow("employee_onboarding", {"employee": "al", "manager": "ola",
                                                        "address": "invalid"})["workflow_id"]
    worker.run_until_idle()
    engine.signal(wid, "manager_approval", {"approved": True})
    drain(engine, worker, clock)
    run = engine.current_run(wid)
    assert run["result"]["reason"] == "laptop could not be provisioned"


def test_subscription_continues_as_new(tmp_path):
    engine, worker, clock = make(tmp_path)
    wid = engine.start_workflow("subscription_renewal",
                                {"customer": "c1", "max_renewals": 7,
                                 "renewals_per_run": 3})["workflow_id"]
    drain(engine, worker, clock)
    runs = engine.runs_of(wid)
    assert [r["status"] for r in runs] == ["CONTINUED_AS_NEW", "CONTINUED_AS_NEW", "COMPLETED"]
    assert runs[-1]["result"] == {"customer": "c1", "renewals": 7}
    assert max(r["history_length"] for r in runs) <= 3 * 4 + 3
    assert len(engine.side_effects(wid)) == 7
    assert clock.now() - runs[0]["started_at"] >= 7 * 30 * DAY
    assert runs[1]["prev_run_id"] == runs[0]["run_id"]


def test_subscription_payment_failure_cancels(tmp_path):
    engine, worker, clock = make(tmp_path)
    wid = engine.start_workflow("subscription_renewal",
                                {"customer": "c2", "card": "expires-after-2"})["workflow_id"]
    drain(engine, worker, clock)
    run = engine.current_run(wid)
    assert run["status"] == "COMPENSATED"
    assert run["result"]["details"] == {"renewals_done": 2}
    assert effects(engine, wid)[-1] == ("billing", "cancel")


def test_start_is_idempotent_on_workflow_id(tmp_path):
    engine, _, _ = make(tmp_path)
    a = engine.start_workflow("order_fulfillment", ORDER, "same")
    b = engine.start_workflow("order_fulfillment", ORDER, "same")
    assert a["run_id"] == b["run_id"] and b["created"] is False
