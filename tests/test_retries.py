"""Retry policies: attempts, exponential backoff, non-retryable errors."""

from itertools import pairwise

import pytest

from durableflow import ActivityError, ApplicationError, RetryPolicy, activity


def flaky(env, fail_times, error=None):
    state = {"n": 0}

    @env.act("flaky")
    def _flaky(ctx):
        state["n"] += 1
        if state["n"] <= fail_times:
            raise error or ConnectionError("upstream 503")
        return {"attempt": ctx.attempt}


def wf(env, policy):
    @env.reg.workflow()
    def retrying(ctx, inp):
        try:
            return (yield activity("flaky", retry=policy))
        except ActivityError as e:
            return {"failed": e.error_type, "attempts": e.attempts}


def test_retries_until_success(env):
    flaky(env, 2)
    wf(env, RetryPolicy(max_attempts=5))
    wid = env.start("retrying")
    env.drain()
    assert env.result(wid) == {"attempt": 3}
    done = next(e for e in env.events(wid) if e["type"] == "ActivityCompleted")
    assert done["attrs"]["attempt"] == 3
    assert env.engine.counter("activity_retries") == 2
    # Retries do not add history events: the timeline stays one scheduled + one completed.
    assert env.types(wid).count("ActivityScheduled") == 1


def test_backoff_is_exponential(env):
    flaky(env, 3)
    wf(env, RetryPolicy(max_attempts=5, initial_interval=2.0, backoff=3.0))
    env.start("retrying")
    env.drain()
    rows = env.engine._q("SELECT attempt, started_at FROM attempts ORDER BY attempt")
    gaps = [round(b[1] - a[1], 3) for a, b in pairwise(rows)]
    assert gaps == [2.0, 6.0, 18.0]


def test_backoff_respects_max_interval():
    p = RetryPolicy(initial_interval=1, backoff=10, max_interval=50)
    assert [p.delay(i) for i in (1, 2, 3, 4)] == [1, 10, 50, 50]


def test_gives_up_after_max_attempts(env):
    flaky(env, 99)
    wf(env, RetryPolicy(max_attempts=3, initial_interval=1))
    wid = env.start("retrying")
    env.drain()
    assert env.result(wid) == {"failed": "ConnectionError", "attempts": 3}
    assert env.calls["flaky"] == 3
    failed = next(e for e in env.events(wid) if e["type"] == "ActivityFailed")
    assert failed["attrs"]["message"] == "upstream 503"


def test_non_retryable_type_stops_immediately(env):
    flaky(env, 99, ApplicationError("card declined", type="CardDeclined"))
    wf(env, RetryPolicy(max_attempts=5, non_retryable=["CardDeclined"]))
    wid = env.start("retrying")
    env.drain()
    assert env.result(wid) == {"failed": "CardDeclined", "attempts": 1}
    assert env.calls["flaky"] == 1


def test_non_retryable_flag_on_the_error(env):
    flaky(env, 99, ApplicationError("bad request", type="Invalid", non_retryable=True))
    wf(env, RetryPolicy(max_attempts=5))
    wid = env.start("retrying")
    env.drain()
    assert env.result(wid)["attempts"] == 1


def test_builtin_exception_type_can_be_non_retryable(env):
    flaky(env, 99, ValueError("nope"))
    wf(env, RetryPolicy(max_attempts=5, non_retryable=["ValueError"]))
    wid = env.start("retrying")
    env.drain()
    assert env.result(wid) == {"failed": "ValueError", "attempts": 1}


def test_unknown_activity_fails_without_retries(env):
    wf(env, RetryPolicy(max_attempts=5))
    wid = env.start("retrying")
    env.drain()
    assert env.result(wid) == {"failed": "UnknownActivity", "attempts": 1}


@pytest.mark.parametrize("attempts", [1, 2, 4])
def test_attempt_rows_match_attempts(env, attempts):
    flaky(env, attempts - 1)
    wf(env, RetryPolicy(max_attempts=5, initial_interval=0.5))
    env.start("retrying")
    env.drain()
    rows = env.engine._q("SELECT outcome FROM attempts ORDER BY attempt")
    assert [r[0] for r in rows] == ["failed"] * (attempts - 1) + ["ok"]
