"""Signals, waits with timeouts, buffering."""

import pytest

from durableflow import SignalTimeout, wait_signal
from durableflow.errors import NotFound, NotRunning


def waiter(env, timeout=3600):
    @env.reg.workflow()
    def approve(ctx, inp):
        try:
            got = yield wait_signal("approval", timeout=timeout)
        except SignalTimeout:
            return "timed out"
        return got


def test_signal_before_timeout_delivers_payload(env):
    waiter(env)
    wid = env.start("approve")
    env.worker.run_until_idle()
    env.clock.advance(600)
    env.engine.signal(wid, "approval", {"ok": True})
    env.drain()
    assert env.result(wid) == {"ok": True}
    # The timeout timer was canceled, not fired.
    assert env.engine._q("SELECT canceled FROM timers")[0][0] == 1


def test_wait_times_out(env):
    waiter(env)
    wid = env.start("approve")
    env.drain()
    assert env.result(wid) == "timed out"
    assert "SignalWaitTimedOut" in env.types(wid)


def test_signal_after_timeout_does_not_change_the_outcome(env):
    @env.reg.workflow()
    def two(ctx, inp):
        try:
            yield wait_signal("approval", timeout=10)
            first = "signal"
        except SignalTimeout:
            first = "timeout"
        second = yield wait_signal("approval", timeout=10)
        return [first, second]

    wid = env.start("two")
    env.worker.run_until_idle()
    env.clock.advance(11)
    env.worker.run_timers()  # timeout recorded first
    env.engine.signal(wid, "approval", "late")
    env.drain()
    assert env.result(wid) == ["timeout", "late"]


def test_signal_sent_before_the_wait_is_buffered(env):
    from durableflow import sleep

    @env.reg.workflow()
    def later(ctx, inp):
        yield sleep(100)
        return (yield wait_signal("approval", timeout=50))

    wid = env.start("later")
    env.worker.run_until_idle()
    env.engine.signal(wid, "approval", "early bird")
    env.drain()
    assert env.result(wid) == "early bird"
    wait = next(e for e in env.events(wid) if e["type"] == "SignalWaitStarted")
    assert wait["attrs"]["buffered"] is True
    assert env.engine._q("SELECT COUNT(*) FROM timers WHERE kind = 'signal_timeout'")[0][0] == 0


def test_signals_are_consumed_in_order(env):
    @env.reg.workflow()
    def collect(ctx, inp):
        out = []
        for _ in range(3):
            out.append((yield wait_signal("item")))
        return out

    wid = env.start("collect")
    for i in range(3):
        env.engine.signal(wid, "item", i)
    env.drain()
    assert env.result(wid) == [0, 1, 2]


def test_signals_with_other_names_are_ignored(env):
    waiter(env, timeout=None)
    wid = env.start("approve")
    env.engine.signal(wid, "noise", 1)
    env.drain()
    assert env.status(wid) == "RUNNING"
    env.engine.signal(wid, "approval", 2)
    env.drain()
    assert env.result(wid) == 2


def test_signal_unknown_workflow(env):
    with pytest.raises(NotFound):
        env.engine.signal("nope", "approval")


def test_signal_closed_workflow(env):
    waiter(env, timeout=1)
    wid = env.start("approve")
    env.drain()
    with pytest.raises(NotRunning):
        env.engine.signal(wid, "approval")
