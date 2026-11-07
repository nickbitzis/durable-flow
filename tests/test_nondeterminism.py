"""Changing workflow code under a running history must fail clearly, not silently diverge."""

import contextlib

from durableflow import DEFAULT_VERSION, activity, sleep, wait_signal


def _base(env):
    @env.act()
    def a(ctx):
        return "a"

    @env.act()
    def b(ctx):
        return "b"

    @env.reg.workflow("flow")
    def v1(ctx, inp):
        yield activity("a")
        yield sleep(60)
        yield activity("a")
        return "done"


def test_renamed_activity_is_detected(env):
    _base(env)
    wid = env.start("flow")
    env.worker.run_until_idle()  # a completed, sleeping

    @env.reg.workflow("flow")
    def v2(ctx, inp):
        yield activity("b")  # changed the first command
        yield sleep(60)
        return "done"

    env.drain()
    run = env.run(wid)
    assert run["status"] == "FAILED"
    assert "NonDeterminismError" in run["error"]
    assert "history has activity a, code issued activity b" in run["error"]
    assert env.types(wid)[-1] == "WorkflowTaskFailed"
    assert env.engine.counter("nondeterminism_errors") == 1


def test_changed_command_kind_is_detected(env):
    _base(env)
    wid = env.start("flow")
    env.worker.run_until_idle()

    @env.reg.workflow("flow")
    def v2(ctx, inp):
        yield activity("a")
        yield wait_signal("go")  # history has a timer here
        return "done"

    env.drain()
    assert "history has timer 60s, code issued wait_signal go" in env.run(wid)["error"]


def test_code_that_stops_early_is_detected(env):
    _base(env)
    wid = env.start("flow")
    env.worker.run_until_idle()

    @env.reg.workflow("flow")
    def v2(ctx, inp):
        return "short"
        yield  # pragma: no cover

    env.drain()
    assert "finished after 0 commands, history has 2" in env.run(wid)["error"]


def test_changing_a_timer_duration_is_allowed(env):
    _base(env)
    wid = env.start("flow")
    env.worker.run_until_idle()

    @env.reg.workflow("flow")
    def v2(ctx, inp):
        yield activity("a")
        yield sleep(120)
        yield activity("a")
        return "done"

    env.drain()
    assert env.status(wid) == "COMPLETED"


def test_unsupported_version_cannot_be_swallowed(env):
    @env.reg.workflow("vflow")
    def v1(ctx, inp):
        ctx.get_version("change", DEFAULT_VERSION, 1)
        yield sleep(10)
        return "ok"

    wid = env.start("vflow")
    env.worker.run_until_idle()

    @env.reg.workflow("vflow")
    def v3(ctx, inp):
        with contextlib.suppress(Exception):  # user code tries to hide the error
            ctx.get_version("change", 2, 3)  # dropped support for version 1
        yield sleep(10)
        return "ok"

    env.drain()
    run = env.run(wid)
    assert run["status"] == "FAILED"
    assert "history has version 1, code supports 2..3" in run["error"]


def test_replay_cli_flags_incompatible_code(env, tmp_path, capsys, monkeypatch):
    from durableflow import cli
    from durableflow.examples import examples
    from durableflow.worker import Registry

    _base(env)
    wid = env.start("flow")
    env.worker.run_until_idle()
    reg = Registry()

    def changed(ctx, inp):
        yield activity("b")
    reg.workflows["flow"] = changed
    monkeypatch.setattr(cli, "_registry", lambda: reg)
    rc = cli.cmd_replay(type("A", (), {"db": env.engine.db_path, "workflow_id": wid})())
    out = capsys.readouterr().out
    assert rc == 1
    assert "nondeterministic" in out
    monkeypatch.setattr(cli, "_registry", lambda: examples)
