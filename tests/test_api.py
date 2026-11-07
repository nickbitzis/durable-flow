"""HTTP API and UI."""

import pytest
from fastapi.testclient import TestClient

from durableflow import Engine, VirtualClock, Worker, drain
from durableflow.api import create_app
from durableflow.examples import examples


@pytest.fixture
def api(tmp_path):
    clock = VirtualClock()
    engine = Engine(str(tmp_path / "api.db"), clock)
    client = TestClient(create_app(engine, examples))
    worker = Worker(engine, examples, "w1")
    yield client, engine, worker, clock
    engine.close()


def test_health_and_ui(api):
    client, *_ = api
    assert client.get("/healthz").json()["ok"] is True
    html = client.get("/").text
    assert "<title>durable-flow</title>" in html


def test_start_signal_describe_history(api):
    client, engine, worker, clock = api
    r = client.post("/api/workflows", json={"workflow_type": "employee_onboarding",
                                            "workflow_id": "ob-1",
                                            "input": {"employee": "dana", "manager": "lee"}})
    assert r.status_code == 201 and r.json()["created"] is True
    worker.run_until_idle()
    d = client.get("/api/workflows/ob-1").json()
    assert d["stage"] == "awaiting manager approval"
    assert d["pending"][0]["name"] == "manager_approval"
    assert len(d["effects"]) == 4
    r = client.post("/api/workflows/ob-1/signal", json={"name": "manager_approval",
                                                        "payload": {"approved": True}})
    assert r.json()["ok"] is True
    drain(engine, worker, clock)
    d = client.get("/api/workflows/ob-1").json()
    assert d["status"] == "COMPLETED"
    child = client.get("/api/workflows/ob-1/order_fulfillment-6").json()
    assert child["status"] == "COMPLETED"
    h = client.get("/api/workflows/ob-1/history").json()
    assert h["events"][0]["type"] == "WorkflowStarted"
    assert h["events"][-1]["type"] == "WorkflowCompleted"


def test_terminate_and_conflict(api):
    client, *_ = api
    client.post("/api/workflows", json={"workflow_type": "subscription_renewal",
                                        "workflow_id": "s1", "input": {"customer": "c"}})
    assert client.post("/api/workflows/s1/terminate", json={"reason": "test"}).status_code == 200
    assert client.post("/api/workflows/s1/terminate").status_code == 409
    assert client.post("/api/workflows/s1/signal", json={"name": "x"}).status_code == 409
    assert client.get("/api/workflows/s1").json()["status"] == "TERMINATED"


def test_errors(api):
    client, *_ = api
    assert client.get("/api/workflows/missing").status_code == 404
    r = client.post("/api/workflows", json={"workflow_type": "nope"})
    assert r.status_code == 400


def test_list_with_filters(api):
    client, engine, worker, clock = api
    for i in range(3):
        client.post("/api/workflows", json={
            "workflow_type": "order_fulfillment", "workflow_id": f"o{i}",
            "input": {"order_id": f"o{i}", "sku": "TENT-2", "amount_cents": 100,
                      "card": "declined" if i == 0 else "ok"}})
    drain(engine, worker, clock)
    body = client.get("/api/workflows", params={"status": "compensated"}).json()
    assert [w["workflow_id"] for w in body["items"]] == ["o0"]
    body = client.get("/api/workflows", params={"type": "order_fulfillment", "limit": 2}).json()
    assert body["total"] == 3 and len(body["items"]) == 2
    assert body["counts"] == {"COMPLETED": 2, "COMPENSATED": 1}


def test_metrics(api):
    client, _engine, worker, _clock = api
    client.post("/api/workflows", json={"workflow_type": "order_fulfillment",
                                        "input": {"order_id": "o", "sku": "TENT-2",
                                                  "amount_cents": 1}})
    worker.run_workflow_task()
    text = client.get("/metrics").text
    assert 'durableflow_task_queue_depth{queue="activity",state="ready"} 1' in text
    assert 'durableflow_workflows{status="running"} 1' in text
    assert "durableflow_activity_retries_total 0" in text
    assert 'durableflow_task_schedule_to_start_seconds_count{queue="workflow"} 1' in text
