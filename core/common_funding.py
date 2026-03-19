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
SCHEMA_META_TABLE = "schema_meta"
BASEINFO_NUMERIC_SHADOW_COLUMNS: dict[str, str] = {
    "adjustedFundingRateCap": "adjustedFundingRateCapNum",
    "adjustedFundingRateFloor": "adjustedFundingRateFloorNum",
    "markPrice": "markPriceNum",
    "lastFundingRate": "lastFundingRateNum",
    "openInterest": "openInterestNum",
    "insuranceBalance": "insuranceBalanceNum",
}
HISTORY_NUMERIC_SHADOW_COLUMNS: dict[str, str] = {
    "fundingRate": "fundingRateNum",
}


def _collector_log_message(
    exchange_label: str,
    kind: str,
    stage: str,
    *,
    detail: str | None = None,
    current: int | None = None,
    total: int | None = None,
) -> str:
    head = f"{exchange_label} {kind} 信息获取{stage}"
    if stage == "进度" and current is not None and total is not None:
        head += f" [{current}/{total}]"
    if detail:
        if stage == "进度":
            head += f" {detail}"
        else:
            head += f"（{detail}）"
    return f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {head}"


def collector_log_start(exchange_label: str, kind: str, *, detail: str | None = None) -> None:
    print(_collector_log_message(exchange_label, kind, "开始", detail=detail), flush=True)


def collector_log_progress(
    exchange_label: str,
    kind: str,
    *,
    detail: str | None = None,
    current: int | None = None,
    total: int | None = None,
) -> None:
    print(
        _collector_log_message(
            exchange_label,
            kind,
            "进度",
            detail=detail,
            current=current,
            total=total,
        ),
        flush=True,
    )


def collector_log_end(exchange_label: str, kind: str, *, detail: str | None = None) -> None:
    print(_collector_log_message(exchange_label, kind, "结束", detail=detail), flush=True)


def tune_sqlite_connection(
    conn: sqlite3.Connection,
    *,
    busy_timeout_ms: int = DEFAULT_SQLITE_BUSY_TIMEOUT_MS,
) -> None:
    """Apply shared SQLite runtime pragmas for better writer/reader concurrency."""
    timeout_ms = max(1000, int(busy_timeout_ms))
    conn.execute(f"PRAGMA busy_timeout={timeout_ms}")
    try:
        conn.execute("PRAGMA journal_mode=WAL")
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute("PRAGMA synchronous=NORMAL")
    except sqlite3.OperationalError:
        pass


def to_plain_str(val: Any) -> str | None:
    try:
        dec = Decimal(str(val))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return format(dec.normalize(), "f")


def bps_to_decimal_str(val: Any) -> str | None:
    plain = to_plain_str(val)
    if plain is None:
        return None
    return to_plain_str(Decimal(plain) / Decimal("10000"))


def pct_to_decimal_str(val: Any) -> str | None:
    plain = to_plain_str(val)
    if plain is None:
        return None
    return to_plain_str(Decimal(plain) / Decimal("100"))


def annualized_decimal_to_interval_decimal_str(val: Any, interval_seconds: Any) -> str | None:
    plain = to_plain_str(val)
    if plain is None:
        return None
    try:
        interval_s = Decimal(str(interval_seconds))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if interval_s <= 0:
        return None
    periods_per_year = (Decimal("365") * Decimal("24") * Decimal("3600")) / interval_s
    if periods_per_year <= 0:
        return None
    return to_plain_str(Decimal(plain) / periods_per_year)


def ensure_column(conn: sqlite3.Connection, table: str, column: str, column_type: str) -> None:
    cur = conn.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cur.fetchall()}
    if column in existing:
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")


def _ensure_schema_meta_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_META_TABLE} (
            key TEXT PRIMARY KEY,
            updated_at INTEGER
        )
        """
    )


def _schema_migration_applied(conn: sqlite3.Connection, key: str) -> bool:
    _ensure_schema_meta_table(conn)
    row = conn.execute(f"SELECT 1 FROM {SCHEMA_META_TABLE} WHERE key=? LIMIT 1", (key,)).fetchone()
    return row is not None


def _mark_schema_migration_applied(conn: sqlite3.Connection, key: str) -> None:
    _ensure_schema_meta_table(conn)
    conn.execute(
        f"""
        INSERT INTO {SCHEMA_META_TABLE} (key, updated_at)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET
            updated_at=excluded.updated_at
        """,
        (key, int(time.time() * 1000)),
    )


def _numeric_shadow_expr(column: str) -> str:
    return f"CASE WHEN {column} IS NULL OR TRIM({column}) = '' THEN NULL ELSE CAST({column} AS REAL) END"


def _ensure_numeric_shadow_columns(
    conn: sqlite3.Connection,
    table: str,
    mapping: dict[str, str],
) -> None:
    for shadow_column in mapping.values():
        ensure_column(conn, table, shadow_column, "REAL")


def _ensure_numeric_shadow_triggers(
    conn: sqlite3.Connection,
    table: str,
    mapping: dict[str, str],
) -> None:
    if not mapping:
        return
    assignments = ", ".join(f"{shadow}=({_numeric_shadow_expr(source)})" for source, shadow in mapping.items())
    watched_columns = ", ".join(mapping.keys())
    insert_trigger = f"trg_{table}_sync_num_ai"
    update_trigger = f"trg_{table}_sync_num_au"
    conn.execute(
        f"""
        CREATE TRIGGER IF NOT EXISTS {insert_trigger}
        AFTER INSERT ON {table}
        BEGIN
            UPDATE {table}
            SET {assignments}
            WHERE rowid = NEW.rowid;
        END
        """
    )
    conn.execute(
        f"""
        CREATE TRIGGER IF NOT EXISTS {update_trigger}
        AFTER UPDATE OF {watched_columns} ON {table}
        BEGIN
            UPDATE {table}
            SET {assignments}
            WHERE rowid = NEW.rowid;
        END
        """
    )


def _backfill_numeric_shadow_columns(
    conn: sqlite3.Connection,
    table: str,
    mapping: dict[str, str],
) -> None:
    if not mapping:
        return
    assignments = ", ".join(f"{shadow}=({_numeric_shadow_expr(source)})" for source, shadow in mapping.items())
    conn.execute(f"UPDATE {table} SET {assignments}")


def ensure_numeric_shadow_layout(
    conn: sqlite3.Connection,
    table: str,
    mapping: dict[str, str],
    *,
    version: str = "v1",
) -> None:
    if not mapping:
        return
    _ensure_numeric_shadow_columns(conn, table, mapping)
    _ensure_numeric_shadow_triggers(conn, table, mapping)
    migration_key = f"{version}:numeric_shadow:{table}"
    if not _schema_migration_applied(conn, migration_key):
        _backfill_numeric_shadow_columns(conn, table, mapping)
        _mark_schema_migration_applied(conn, migration_key)


def ensure_baseinfo_numeric_layout(conn: sqlite3.Connection, table: str) -> None:
    ensure_numeric_shadow_layout(conn, table, BASEINFO_NUMERIC_SHADOW_COLUMNS)


def ensure_history_numeric_layout(conn: sqlite3.Connection, table: str) -> None:
    ensure_numeric_shadow_layout(conn, table, HISTORY_NUMERIC_SHADOW_COLUMNS)


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
    ensure_baseinfo_numeric_layout(conn, table)
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
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_fundingTime ON {table}(fundingTime)")
    ensure_history_numeric_layout(conn, table)
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


def stamp_rows_updated_at(
    rows: list[tuple[Any, ...]],
    *,
    updated_at_ms: int | None = None,
) -> list[tuple[Any, ...]]:
    """Overwrite the trailing updated_at field with a single batch completion timestamp."""
    completed_at_ms = int(updated_at_ms if updated_at_ms is not None else time.time() * 1000)
    return [(*row[:-1], completed_at_ms) for row in rows]


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
