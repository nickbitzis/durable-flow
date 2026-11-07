"""Durable timers on the virtual clock."""

from durableflow import DAY, sleep
from durableflow.clock import EPOCH


def sleeper(env):
    @env.reg.workflow()
    def napper(ctx, inp):
        yield sleep(inp)
        return ctx.now()


def test_timer_fires_at_its_time_not_before(env):
    sleeper(env)
    wid = env.start("napper", 30 * DAY)
    env.worker.run_until_idle()
    env.clock.advance(30 * DAY - 1)
    env.worker.run_until_idle()
    assert env.status(wid) == "RUNNING"
    env.clock.advance(1)
    env.worker.run_until_idle()
    assert env.status(wid) == "COMPLETED"
    assert env.result(wid) >= EPOCH + 30 * DAY


def test_timer_survives_a_new_engine_and_worker(env, tmp_path):
    from durableflow import Engine, Worker

    sleeper(env)
    wid = env.start("napper", 3600)
    env.worker.run_until_idle()
    env.engine.close()
    # "Deploy": new process, same database.
    env.engine = Engine(env.engine.db_path, env.clock)
    env._worker = Worker(env.engine, env.reg, "w2")
    env.drain()
    assert env.status(wid) == "COMPLETED"


def test_timers_fire_in_fire_time_order(env):
    sleeper(env)
    ids = [env.start("napper", s) for s in (500, 100, 300, 200, 400)]
    env.worker.run_until_idle()
    env.clock.advance(1000)
    env.worker.run_until_idle()
    rows = env.engine._q("SELECT fire_at FROM timers ORDER BY fire_order")
    fire_at = [r[0] for r in rows]
    assert fire_at == sorted(fire_at)
    assert all(env.status(i) == "COMPLETED" for i in ids)


def test_only_the_leader_fires_timers(env):
    sleeper(env)
    env.start("napper", 25)
    env.worker.run_until_idle()  # w1 holds leadership until t=10
    a, b = env.new_worker("A"), env.new_worker("B")
    env.clock.advance(20)
    assert a.run_timers() == 0  # nothing due yet, but A is now the leader until t=30
    env.clock.advance(6)
    assert b.run_timers() == 0  # timer is due, but A's leadership lease is still live
    assert a.run_timers() == 1


def test_leadership_moves_when_the_leader_dies(env):
    sleeper(env)
    env.start("napper", 60)
    env.worker.run_until_idle()
    a, b = env.new_worker("A"), env.new_worker("B")
    a.run_timers()  # A leads, then dies
    env.clock.advance(61)
    assert b.run_timers() == 1


def test_terminate_cancels_pending_timers(env):
    sleeper(env)
    wid = env.start("napper", 100)
    env.worker.run_until_idle()
    env.engine.terminate(wid, "test")
    env.clock.advance(200)
    env.worker.run_until_idle()
    assert env.status(wid) == "TERMINATED"
    assert env.engine._q("SELECT canceled, fired_at FROM timers")[0][:] == (1, None)


def test_next_wakeup_points_at_the_earliest_timer(env):
    sleeper(env)
    env.start("napper", 300)
    env.start("napper", 100)
    env.worker.run_until_idle()
    assert env.engine.next_wakeup() == EPOCH + 100
    assert env.engine.is_idle()
