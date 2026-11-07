"""get_version lets code change while old histories still replay."""

from durableflow import DEFAULT_VERSION, activity, sleep


def _acts(env):
    def make(name):
        def fn(ctx):
            env.calls[name] += 1
            return name
        return fn

    for n in ("old_step", "new_step", "first", "last"):
        env.reg.activities[n] = make(n)


def test_new_run_records_max_version(env):
    _acts(env)

    @env.reg.workflow()
    def flow(ctx, inp):
        v = ctx.get_version("add-new-step", DEFAULT_VERSION, 1)
        yield activity("new_step" if v == 1 else "old_step")
        return v

    wid = env.start("flow")
    env.drain()
    assert env.result(wid) == 1
    markers = [e for e in env.events(wid) if e["type"] == "VersionMarker"]
    assert markers[0]["attrs"] == {"change_id": "add-new-step", "version": 1}
    assert env.calls["new_step"] == 1


def test_old_history_keeps_the_old_branch(env):
    _acts(env)

    @env.reg.workflow("flow")
    def v0(ctx, inp):
        yield activity("first")
        yield activity("old_step")
        yield sleep(100)
        yield activity("last")

    wid = env.start("flow")
    env.worker.run_until_idle()  # first + old_step done, sleeping

    @env.reg.workflow("flow")
    def v1(ctx, inp):
        yield activity("first")
        if ctx.get_version("swap", DEFAULT_VERSION, 1) == DEFAULT_VERSION:
            yield activity("old_step")
        else:
            yield activity("new_step")
        yield sleep(100)
        yield activity("last")

    env.drain()
    assert env.status(wid) == "COMPLETED"
    assert env.calls["new_step"] == 0
    assert "VersionMarker" not in env.types(wid)


def test_run_blocked_before_the_change_point_takes_the_new_branch(env):
    _acts(env)

    @env.reg.workflow("flow")
    def v0(ctx, inp):
        yield sleep(100)
        yield activity("old_step")

    env.start("flow")
    env.worker.run_until_idle()  # sleeping before the step

    @env.reg.workflow("flow")
    def v1(ctx, inp):
        yield sleep(100)
        v = ctx.get_version("swap", DEFAULT_VERSION, 1)
        yield activity("new_step" if v else "old_step")

    env.drain()
    assert env.calls["new_step"] == 1 and env.calls["old_step"] == 0


def test_recorded_version_is_returned_on_replay(env):
    _acts(env)
    seen = []

    @env.reg.workflow()
    def flow(ctx, inp):
        seen.append(ctx.get_version("c", DEFAULT_VERSION, 2))
        yield sleep(5)
        yield sleep(5)

    env.start("flow")
    env.drain()
    assert seen == [2, 2, 2]


def test_raising_max_version_keeps_recorded_version(env):
    _acts(env)

    @env.reg.workflow("flow")
    def v1(ctx, inp):
        v = ctx.get_version("c", DEFAULT_VERSION, 1)
        yield sleep(5)
        return v

    wid = env.start("flow")
    env.worker.run_until_idle()

    @env.reg.workflow("flow")
    def v2(ctx, inp):
        v = ctx.get_version("c", DEFAULT_VERSION, 2)
        yield sleep(5)
        return v

    env.drain()
    assert env.result(wid) == 1
