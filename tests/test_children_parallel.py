"""Child workflows and parallel branches."""

import pytest

from durableflow import (
    ActivityError,
    ChildWorkflowError,
    Compensated,
    ParallelError,
    RetryPolicy,
    activity,
    child_workflow,
    parallel,
    sleep,
    wait_signal,
)


def kids(env):
    @env.act()
    def double(ctx, x):
        return x * 2

    @env.reg.workflow()
    def child(ctx, inp):
        if inp == "fail":
            raise RuntimeError("child broke")
        if inp == "undo":
            return Compensated("undone")
        yield sleep(10)
        return (yield activity("double", inp))

    @env.reg.workflow()
    def parent(ctx, inp):
        try:
            return {"child": (yield child_workflow("child", inp))}
        except ChildWorkflowError as e:
            return {"child_status": e.status}


def test_child_result_flows_to_parent(env):
    kids(env)
    wid = env.start("parent", 21, "p1")
    env.drain()
    assert env.result(wid) == {"child": 42}
    started = next(e for e in env.events(wid) if e["type"] == "ChildStarted")
    assert started["attrs"]["child_workflow_id"] == "p1/child-1"
    child_run = env.run("p1/child-1")
    assert child_run["parent_run_id"] == env.run(wid)["run_id"]
    assert env.types(wid)[-2] == "ChildCompleted"


@pytest.mark.parametrize("inp,status", [("fail", "FAILED"), ("undo", "COMPENSATED")])
def test_child_failure_raises_in_parent(env, inp, status):
    kids(env)
    wid = env.start("parent", inp)
    env.drain()
    assert env.result(wid) == {"child_status": status}


def test_describe_lists_children(env):
    kids(env)
    wid = env.start("parent", 1, "p2")
    env.worker.run_until_idle()
    d = env.engine.describe(wid)
    assert [c["workflow_id"] for c in d["children"]] == ["p2/child-1"]
    assert d["pending"][0]["kind"] == "child"


def par(env, fail=()):
    @env.act()
    def work(ctx, name):
        if name in fail:
            raise ValueError(f"{name} failed")
        return name.upper()


def test_parallel_results_keep_order(env):
    par(env)

    @env.reg.workflow()
    def fan(ctx, inp):
        return (yield parallel(*[activity("work", n) for n in "abcd"]))

    wid = env.start("fan")
    env.drain()
    assert env.result(wid) == ["A", "B", "C", "D"]


def test_parallel_branches_are_scheduled_together(env):
    par(env)

    @env.reg.workflow()
    def fan(ctx, inp):
        return (yield parallel(*[activity("work", n) for n in "abc"]))

    wid = env.start("fan")
    env.worker.run_workflow_task()
    assert env.types(wid) == ["WorkflowStarted"] + ["ActivityScheduled"] * 3
    assert env.engine.queue_depths()["activity_ready"] == 3


def test_parallel_failure_reports_each_branch(env):
    par(env, fail=("b",))

    @env.reg.workflow()
    def fan(ctx, inp):
        try:
            yield parallel(*[activity("work", n, retry=RetryPolicy(max_attempts=1)) for n in "abc"])
        except ParallelError as e:
            return {"results": e.results,
                    "errors": [None if x is None else x.error_type for x in e.errors]}

    wid = env.start("fan")
    env.drain()
    assert env.result(wid) == {"results": ["A", None, "C"], "errors": [None, "ValueError", None]}


def test_parallel_mixes_timers_and_activities(env):
    par(env)

    @env.reg.workflow()
    def mix(ctx, inp):
        start = ctx.now()
        r = yield parallel(activity("work", "x"), sleep(60))
        return [r[0], r[1], ctx.now() - start >= 60]

    wid = env.start("mix")
    env.drain()
    assert env.result(wid) == ["X", None, True]


def test_parallel_rejects_signal_waits():
    with pytest.raises(TypeError):
        parallel(wait_signal("x"))


def test_activity_error_carries_details(env):
    par(env, fail=("z",))

    @env.reg.workflow()
    def one(ctx, inp):
        try:
            yield activity("work", "z", retry=RetryPolicy(max_attempts=2, initial_interval=1))
        except ActivityError as e:
            return [e.name, e.error_type, e.message, e.attempts]

    wid = env.start("one")
    env.drain()
    assert env.result(wid) == ["work", "ValueError", "z failed", 2]
