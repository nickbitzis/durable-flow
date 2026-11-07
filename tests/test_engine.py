"""Workflow task leases, fencing, termination, listing, and the clock."""

import sqlite3

from durableflow import DbClock, activity, sleep, wait_signal
from durableflow.engine import pending_from_history


def simple(env):
    @env.act()
    def step(ctx):
        return "ok"

    @env.reg.workflow()
    def flow(ctx, inp):
        yield activity("step")
        yield wait_signal("go")
        return "done"


def test_workflow_task_lease_takeover_and_fencing(env):
    simple(env)
    wid = env.start("flow")
    stale = env.engine.poll_workflow_task("A")  # A takes the task and stalls
    assert env.engine.poll_workflow_task("B") is None
    env.clock.advance(env.engine.config.wf_task_lease + 1)
    env.new_worker("B").run_workflow_task()  # B takes over and schedules the activity
    from durableflow.workflow import replay
    d = replay(env.reg.workflows["flow"], env.events(wid), env.run(wid))
    assert env.engine.complete_workflow_task(stale, "A", d) is False
    assert env.types(wid).count("ActivityScheduled") == 1
    assert env.engine.counter("wf_lease_takeovers") == 1


def test_event_during_a_task_reschedules_it(env):
    simple(env)
    wid = env.start("flow")
    env.worker.run_until_idle()  # waiting for "go"
    task = env.engine.poll_workflow_task("A")
    assert task is None
    env.engine.signal(wid, "go")
    task = env.engine.poll_workflow_task("A")
    hist = env.events(wid)
    env.engine.signal(wid, "extra")  # arrives while A holds the task
    from durableflow.workflow import Decision
    env.engine.complete_workflow_task(task, "A", Decision())
    assert env.engine.queue_depths()["workflow_ready"] == 1
    assert len(hist) < len(env.events(wid))
    env.drain()
    assert env.status(wid) == "COMPLETED"


def test_terminate_removes_tasks(env):
    simple(env)
    wid = env.start("flow")
    env.worker.run_workflow_task()
    env.engine.terminate(wid, "operator said so")
    run = env.run(wid)
    assert run["status"] == "TERMINATED" and run["error"] == "operator said so"
    assert env.engine.queue_depths()["activity_ready"] == 0
    assert env.types(wid)[-1] == "WorkflowTerminated"


def test_history_sequence_is_contiguous(env):
    simple(env)
    wid = env.start("flow")
    env.worker.run_until_idle()
    env.engine.signal(wid, "go")
    env.drain()
    seqs = [e["seq"] for e in env.events(wid)]
    assert seqs == list(range(1, len(seqs) + 1))
    assert env.run(wid)["history_length"] == len(seqs)


def test_list_filters_and_pages(env):
    simple(env)

    @env.reg.workflow()
    def other(ctx, inp):
        yield sleep(1)

    for i in range(5):
        env.start("flow", None, f"flow-{i}")
    env.start("other", None, "other-1")
    env.drain()
    e = env.engine
    assert e.list_workflows(workflow_type="flow")["total"] == 5
    assert e.list_workflows(status="completed")["total"] == 1
    assert e.list_workflows(q="flow-3")["items"][0]["workflow_id"] == "flow-3"
    page = e.list_workflows(limit=2, offset=2)
    assert len(page["items"]) == 2 and page["total"] == 6
    assert e.status_counts() == {"RUNNING": 5, "COMPLETED": 1}


def test_pending_shows_what_the_workflow_waits_for(env):
    simple(env)
    wid = env.start("flow")
    env.worker.run_workflow_task()
    assert [p["kind"] for p in pending_from_history(env.events(wid))] == ["activity"]
    env.worker.run_until_idle()
    pend = pending_from_history(env.events(wid))
    assert pend == [{"cmd": 2, "kind": "signal", "since": pend[0]["since"], "name": "go",
                     "timeout_at": None}]


def test_db_clock_is_shared_between_connections(tmp_path):
    db = str(tmp_path / "c.db")
    conn = sqlite3.connect(db)
    DbClock.init(conn, start=1000.0, rate=0.0)
    a, b = DbClock(db, cache_s=0), DbClock(db, cache_s=0)
    assert a.now() == b.now() == 1000.0
    a.jump_to(5000.0)
    assert b.now() == 5000.0
    a.jump_to(10.0)  # never backward
    assert b.now() == 5000.0
