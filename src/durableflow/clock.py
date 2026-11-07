"""Clocks. Business time (timers, backoff, timeouts, leases) always comes from a Clock.

SystemClock is wall time. VirtualClock is moved by hand in tests. DbClock is shared by
several processes through one SQLite row, runs at `rate` virtual seconds per real second,
and can jump forward when nothing is runnable (the same idea as Temporal's time skipping).
"""

from __future__ import annotations

import sqlite3
import threading
import time
from datetime import UTC, datetime

# 2026-01-05 00:00:00 UTC. Virtual clocks start here so demo dates look sensible.
EPOCH = 1767571200.0
DAY = 86400.0


class Clock:
    def now(self) -> float:  # pragma: no cover - interface
        raise NotImplementedError


class SystemClock(Clock):
    def now(self) -> float:
        return time.time()


class VirtualClock(Clock):
    def __init__(self, start: float = EPOCH) -> None:
        self._t = start
        self._lock = threading.Lock()

    def now(self) -> float:
        with self._lock:
            return self._t

    def advance(self, seconds: float) -> float:
        with self._lock:
            self._t += seconds
            return self._t

    def set(self, t: float) -> None:
        with self._lock:
            self._t = max(self._t, t)


class DbClock(Clock):
    """Virtual time stored in the `clock` table so every worker process agrees on it."""

    def __init__(self, db_path: str, cache_s: float = 0.01) -> None:
        self._conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
        self._lock = threading.Lock()
        self._cache_s = cache_s
        self._base: tuple[float, float, float] | None = None
        self._read_at = 0.0

    @staticmethod
    def init(conn: sqlite3.Connection, start: float = EPOCH, rate: float = 20.0) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS clock (id INTEGER PRIMARY KEY CHECK (id = 1),"
            " virt_base REAL NOT NULL, real_base REAL NOT NULL, rate REAL NOT NULL)"
        )
        conn.execute(
            "INSERT OR REPLACE INTO clock VALUES (1, ?, ?, ?)", (start, time.time(), rate)
        )
        conn.commit()

    def _row(self) -> tuple[float, float, float]:
        real = time.time()
        if self._base is None or real - self._read_at > self._cache_s:
            row = self._conn.execute(
                "SELECT virt_base, real_base, rate FROM clock WHERE id = 1"
            ).fetchone()
            if row is None:
                raise RuntimeError("clock table is empty; run DbClock.init first")
            self._base = (row[0], row[1], row[2])
            self._read_at = real
        return self._base

    def now(self) -> float:
        with self._lock:
            virt, real_base, rate = self._row()
            return virt + (time.time() - real_base) * rate

    def jump_to(self, t: float) -> None:
        """Move virtual time forward to t (never backward)."""
        with self._lock:
            cur = self._conn.execute(
                "SELECT virt_base, real_base, rate FROM clock WHERE id = 1"
            ).fetchone()
            real = time.time()
            current = cur[0] + (real - cur[1]) * cur[2]
            if t > current:
                self._conn.execute(
                    "UPDATE clock SET virt_base = ?, real_base = ? WHERE id = 1", (t, real)
                )
                self._conn.commit()
            self._base = None


def iso(t: float | None) -> str | None:
    if t is None:
        return None
    return datetime.fromtimestamp(t, UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
