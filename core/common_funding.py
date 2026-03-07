#!/usr/bin/env python3
"""Shared helpers for funding collectors."""

from __future__ import annotations

import sqlite3
import time
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

DEFAULT_SQLITE_BUSY_TIMEOUT_MS = 15_000


def tune_sqlite_connection(
    conn: sqlite3.Connection,
    *,
    busy_timeout_ms: int = DEFAULT_SQLITE_BUSY_TIMEOUT_MS,
) -> None:
    """Apply shared SQLite runtime pragmas for better writer/reader concurrency."""
    timeout_ms = max(1000, int(busy_timeout_ms))
    conn.execute(f"PRAGMA busy_timeout={timeout_ms}")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")


def to_plain_str(val: Any) -> str | None:
    try:
        dec = Decimal(str(val))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return format(dec.normalize(), "f")


def ensure_column(conn: sqlite3.Connection, table: str, column: str, column_type: str) -> None:
    cur = conn.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cur.fetchall()}
    if column in existing:
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")


def ensure_baseinfo_table(conn: sqlite3.Connection, table: str) -> None:
    tune_sqlite_connection(conn)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            symbol TEXT PRIMARY KEY,
            adjustedFundingRateCap TEXT,
            adjustedFundingRateFloor TEXT,
            fundingIntervalHours INTEGER,
            markPrice TEXT,
            lastFundingRate TEXT,
            openInterest TEXT,
            insuranceBalance TEXT,
            updated_at INTEGER
        )
        """
    )
    ensure_column(conn, table, "insuranceBalance", "TEXT")
    conn.commit()


def ensure_history_table(conn: sqlite3.Connection, table: str) -> None:
    tune_sqlite_connection(conn)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            symbol TEXT NOT NULL,
            fundingTime INTEGER NOT NULL,
            fundingRate TEXT,
            updated_at INTEGER,
            PRIMARY KEY (symbol, fundingTime)
        )
        """
    )
    conn.commit()


def fetch_existing_symbols(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.execute(f"SELECT symbol FROM {table}")
    return {row[0] for row in cur.fetchall() if row[0]}


def load_symbols(conn: sqlite3.Connection, table: str) -> list[str]:
    cur = conn.execute(f"SELECT symbol FROM {table}")
    symbols = sorted(row[0] for row in cur.fetchall() if row[0])
    if not symbols:
        raise RuntimeError(f"{table} 中没有交易对数据，请先同步该表")
    return symbols


def delete_obsolete_symbols(conn: sqlite3.Connection, table: str, symbols: set[str]) -> list[str]:
    if not symbols:
        return []
    placeholders = ",".join("?" for _ in symbols)
    conn.execute(f"DELETE FROM {table} WHERE symbol IN ({placeholders})", tuple(symbols))
    return sorted(symbols)


def delete_older_than(
    conn: sqlite3.Connection,
    history_table: str,
    cutoff_ms: int,
    symbols: Iterable[str],
) -> None:
    syms = list(set(symbols))
    if not syms:
        return
    placeholders = ",".join("?" for _ in syms)
    conn.execute(
        f"DELETE FROM {history_table} WHERE fundingTime < ? AND symbol IN ({placeholders})",
        (cutoff_ms, *syms),
    )


def normalize_ts_to_ms(ts: Any) -> int | None:
    try:
        t = int(ts)
    except (TypeError, ValueError):
        return None
    if t < 1_000_000_000_000:
        return t * 1000
    return t


def parse_iso_to_ms(ts: str) -> int | None:
    s = ts.strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


class RateLimiter:
    """Simple weighted sliding-window limiter."""

    def __init__(self, capacity: int, period: int) -> None:
        self.capacity = max(1, capacity)
        self.period = max(1, period)
        self.events: deque[tuple[float, int]] = deque()
        self.total_weight = 0

    def _purge(self, now: float) -> None:
        while self.events and now - self.events[0][0] >= self.period:
            _, weight = self.events.popleft()
            self.total_weight -= weight

    def acquire(self, weight: int = 1) -> None:
        w = max(1, weight)
        if w > self.capacity:
            raise ValueError(f"weight {w} exceeds limiter capacity {self.capacity}")
        while True:
            now = time.monotonic()
            self._purge(now)
            if self.total_weight + w <= self.capacity:
                self.events.append((now, w))
                self.total_weight += w
                return
            if not self.events:
                time.sleep(0.1)
                continue
            sleep_for = self.period - (now - self.events[0][0]) + 0.05
            time.sleep(max(0.1, sleep_for))
