"""Start-to-close and heartbeat timeouts, and lease expiry when a worker dies."""

from durableflow import ActivityError, RetryPolicy, activity


def wf(env, **kw):
    @env.reg.workflow()
    def timed(ctx, inp):
        try:
            return (yield activity("slow", **kw))
        except ActivityError as e:
            return {"failed": e.error_type, "attempts": e.attempts}


def test_start_to_close_timeout_then_retry_succeeds(env):
    @env.act()
    def slow(ctx):
        if ctx.attempt == 1:
            env.clock.advance(100)  # this attempt takes 100 virtual seconds
        return "fast enough"

    wf(env, start_to_close=30, retry=RetryPolicy(max_attempts=3))
    wid = env.start("timed")
    env.drain()
    assert env.result(wid) == "fast enough"
    rows = env.engine._q("SELECT outcome, error FROM attempts ORDER BY attempt")
    assert rows[0]["error"].startswith("StartToCloseTimeout")
    assert rows[1]["outcome"] == "ok"


def test_start_to_close_timeout_exhausts_attempts(env):
    @env.act()
    def slow(ctx):
        env.clock.advance(100)
        return "late"

    wf(env, start_to_close=30, retry=RetryPolicy(max_attempts=2))
    wid = env.start("timed")
    env.drain()
    assert env.result(wid) == {"failed": "StartToCloseTimeout", "attempts": 2}


def test_missing_heartbeats_time_out(env):
    @env.act()
    def slow(ctx):
        env.clock.advance(50)  # no heartbeat for 50s
        return "done"

    wf(env, start_to_close=600, heartbeat_timeout=10, retry=RetryPolicy(max_attempts=1))
    wid = env.start("timed")
    env.drain()
    assert env.result(wid) == {"failed": "HeartbeatTimeout", "attempts": 1}


def test_heartbeats_keep_a_long_activity_alive(env):
    @env.act()
    def slow(ctx):
        for i in range(10):
            env.clock.advance(5)
            assert ctx.heartbeat({"progress": i})
        return "done"

    wf(env, start_to_close=600, heartbeat_timeout=10)
    wid = env.start("timed")
    env.drain()
    assert env.result(wid) == "done"
    assert env.calls["slow"] == 1


def _one_activity(env):
    @env.act()
    def slow(ctx):
        return f"attempt {ctx.attempt}"

    wf(env, start_to_close=30, retry=RetryPolicy(max_attempts=3))
    wid = env.start("timed")
    a = env.new_worker("A")
    a.run_workflow_task()
    return wid


def test_lease_expiry_lets_another_worker_take_over(env):
    wid = _one_activity(env)
    dead = env.engine.poll_activity_task("A")  # worker A takes the task, then dies
    assert dead.attempt == 1
    b = env.new_worker("B")
    assert env.engine.poll_activity_task("B") is None  # lease still live
    env.clock.advance(31 + env.engine.config.lease_grace)
    b.run_activity_task()
    env.drain()
    assert env.result(wid) == "attempt 2"
    rows = env.engine._q("SELECT worker, outcome, error FROM attempts ORDER BY attempt")
    assert [(r[0], r[1]) for r in rows] == [("A", "lease_expired"), ("B", "ok")]
    assert rows[0][2] == "StartToCloseTimeout"
    assert env.engine.counter("activity_lease_takeovers") == 1


def test_late_report_from_the_old_worker_is_fenced(env):
    wid = _one_activity(env)
    zombie = env.engine.poll_activity_task("A")
    env.clock.advance(40)
    env.new_worker("B").run_activity_task()
    assert env.engine.complete_activity(zombie, "stale result") is False
    env.drain()
    assert env.result(wid) == "attempt 2"
    assert env.engine.counter("fenced_completions") == 1


def test_lease_expiry_on_last_attempt_fails_the_activity(env):
    @env.act()
    def slow(ctx):
        return "never"

    wf(env, start_to_close=30, retry=RetryPolicy(max_attempts=1))
    wid = env.start("timed")
    env.worker.run_workflow_task()
    env.engine.poll_activity_task("A")
    env.clock.advance(60)
    assert env.engine.poll_activity_task("B") is None  # failed instead of handed out
    env.drain()
    assert env.result(wid) == {"failed": "StartToCloseTimeout", "attempts": 1}


def test_heartbeat_extends_the_lease(env):
    wid = _one_activity(env)
    task = env.engine.poll_activity_task("A")
    task.heartbeat_timeout = 10
    for _ in range(5):
        env.clock.advance(5)
        assert env.engine.heartbeat(task, {"pct": 50})
    # 25s passed, start_to_close is 30s: still A's task.
    assert env.engine.poll_activity_task("B") is None
    assert env.engine.complete_activity(task, "ok") is True
    env.drain()
    assert env.result(wid) == "ok"
