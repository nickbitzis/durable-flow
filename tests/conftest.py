from __future__ import annotations

from collections import Counter

import pytest

from durableflow import Engine, Registry, VirtualClock, Worker, drain


class Env:
    """One SQLite engine on a virtual clock, a registry, and helpers."""

    def __init__(self, path: str) -> None:
        self.clock = VirtualClock()
        self.engine = Engine(path, self.clock)
        self.reg = Registry()
        self.calls: Counter[str] = Counter()
        self._worker: Worker | None = None

    @property
    def worker(self) -> Worker:
        if self._worker is None:
            self._worker = Worker(self.engine, self.reg, "w1")
        return self._worker

    def new_worker(self, wid: str, **kw) -> Worker:
        return Worker(self.engine, self.reg, wid, **kw)

    def act(self, name: str | None = None):
        """Register an activity that also counts its executions."""
        def deco(fn):
            n = name or fn.__name__

            def wrapped(ctx, *a, **k):
                self.calls[n] += 1
                return fn(ctx, *a, **k)
            self.reg.activities[n] = wrapped
            return fn
        return deco

    def start(self, wf_type: str, inp=None, wid: str | None = None) -> str:
        return self.engine.start_workflow(wf_type, inp, wid)["workflow_id"]

    def drain(self, until=None) -> None:
        drain(self.engine, self.worker, self.clock, until)

    def run(self, wid: str) -> dict:
        return self.engine.current_run(wid)

    def status(self, wid: str) -> str:
        return self.run(wid)["status"]

    def result(self, wid: str):
        return self.run(wid)["result"]

    def events(self, wid: str) -> list[dict]:
        return self.engine.history(self.run(wid)["run_id"])

    def types(self, wid: str) -> list[str]:
        return [e["type"] for e in self.events(wid)]


@pytest.fixture
def env(tmp_path) -> Env:
    e = Env(str(tmp_path / "t.db"))
    yield e
    e.engine.close()
