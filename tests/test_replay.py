"""Replay: recorded results come back without running activities again."""

from durableflow import activity, sleep
from durableflow.workflow import replay


def setup_two_step(env):
    @env.act()
    def add(ctx, a, b):
        return a + b

    @env.reg.workflow()
    def calc(ctx, inp):
        x = yield activity("add", inp, 1)
        yield sleep(60)
        y = yield activity("add", x, 10)
        return {"x": x, "y": y}


def test_replay_returns_recorded_results_without_reexecuting(env):
    setup_two_step(env)
    wid = env.start("calc", 5)
    env.drain()
    assert env.result(wid) == {"x": 6, "y": 16}
    # Each activity ran once even though the workflow was replayed on every task.
    assert env.calls["add"] == 2
    assert env.engine.counter("workflow_tasks") >= 3
    assert env.engine.counter("commands_replayed") >= 3


def test_replay_of_full_history_needs_no_activities(env):
    setup_two_step(env)
    wid = env.start("calc", 5)
    env.drain()
    run = env.run(wid)
    hist = env.events(wid)

    def boom(*a, **k):
        raise AssertionError("activity must not run during replay")
    env.reg.activities["add"] = boom
    d = replay(env.reg.workflows["calc"], hist, run)
    assert d.outcome == "completed"
    assert d.result == {"x": 6, "y": 16}
    assert d.new_commands == []
    assert d.commands_replayed == 3


def test_replay_of_partial_history_stops_at_the_frontier(env):
    setup_two_step(env)
    wid = env.start("calc", 5)
    env.worker.run_until_idle()  # blocked on the 60s timer
    run = env.run(wid)
    d = replay(env.reg.workflows["calc"], env.events(wid), run)
    assert d.outcome == "blocked"
    assert d.new_commands == []  # the timer is already in history
    assert env.calls["add"] == 1


def test_async_workflows_replay_too(env):
    @env.act()
    def greet(ctx, name):
        return f"hello {name}"

    @env.reg.workflow()
    async def hello(ctx, inp):
        a = await activity("greet", inp)
        await sleep(5)
        b = await activity("greet", a)
        return b

    wid = env.start("hello", "kestrel")
    env.drain()
    assert env.result(wid) == "hello hello kestrel"
    assert env.calls["greet"] == 2


def test_plain_function_workflow_completes(env):
    env.reg.workflows["noop"] = lambda ctx, inp: {"echo": inp}
    wid = env.start("noop", 3)
    env.drain()
    assert env.status(wid) == "COMPLETED"
    assert env.result(wid) == {"echo": 3}


def test_uncaught_workflow_exception_fails_the_workflow(env):
    @env.reg.workflow()
    def broken(ctx, inp):
        yield sleep(1)
        raise ValueError("bad input")

    wid = env.start("broken")
    env.drain()
    assert env.status(wid) == "FAILED"
    assert "ValueError: bad input" in env.run(wid)["error"]
    assert env.types(wid)[-1] == "WorkflowFailed"


def test_workflow_time_is_deterministic(env):
    seen = []

    @env.reg.workflow()
    def clocky(ctx, inp):
        seen.append(ctx.now())
        yield sleep(3600)
        seen.append(ctx.now())
        return ctx.now()

    wid = env.start("clocky")
    env.drain()
    fired = next(e for e in env.events(wid) if e["type"] == "TimerFired")
    assert env.result(wid) == fired["ts"]
    started = env.events(wid)[0]["ts"]
    # Every replay sees the same values at the same points.
    assert seen == [started, started, fired["ts"]]


def test_is_replaying_flag(env):
    flags = []

    @env.act()
    def step(ctx):
        return 1

    @env.reg.workflow()
    def flagged(ctx, inp):
        flags.append(("before", ctx.is_replaying))
        yield activity("step")
        flags.append(("after", ctx.is_replaying))

    env.start("flagged")
    env.drain()
    # First task: not replaying. Second task: replaying before the activity, live after.
    assert flags == [("before", False), ("before", True), ("after", False)]


def test_stage_is_saved_on_the_run(env):
    @env.reg.workflow()
    def staged(ctx, inp):
        ctx.set_stage("napping")
        yield sleep(10)

    wid = env.start("staged")
    env.worker.run_until_idle()
    assert env.run(wid)["stage"] == "napping"
