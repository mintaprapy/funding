#!/usr/bin/env python3
"""从 Aster 获取永续合约近 30 天资金费率，存入 SQLite，并保留 60 天内数据。"""

from __future__ import annotations

import os
import sqlite3
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

import requests

DEFAULT_BASE_URL = "https://fapi.asterdex.com"
BASE_URL = (os.getenv("ASTER_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")
FUNDING_RATE_PATH = "/fapi/v1/fundingRate"
REQUEST_TIMEOUT = 15

ROOT_DIR = next(parent for parent in Path(__file__).resolve().parents if (parent / "start_all_funding.sh").exists())
DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT_DIR / "funding.db")).expanduser().resolve()
INFO_TABLE = "aster_funding_baseinfo"
HISTORY_TABLE = "aster_funding_history"

DAYS_TO_FETCH = 30
DAYS_TO_KEEP = 60
MAX_RECORDS = 720

WINDOW_SECONDS = 400
WINDOW_CAPACITY = int(500 * 0.8)


def tune_sqlite_connection(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA busy_timeout=15000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")


class RateLimiter:
    def __init__(self, capacity: int, period: int) -> None:
        self.capacity = capacity
        self.period = period
        self.events: list[float] = []

    def acquire(self) -> None:
        now = time.monotonic()
        self.events = [t for t in self.events if now - t < self.period]
        if len(self.events) >= self.capacity:
            sleep_for = self.period - (now - self.events[0]) + 0.01
            time.sleep(max(0.05, sleep_for))
        self.events.append(time.monotonic())


def require_base_url() -> str:
    if not BASE_URL:
        raise RuntimeError("请设置 ASTER_BASE_URL（Aster 合约 API 基地址，例如 https://fapi.asterdex.com ）")
    return BASE_URL


def to_plain_str(val: Any) -> str | None:
    try:
        dec = Decimal(str(val))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return format(dec.normalize(), "f")


def fetch_json(session: requests.Session, url: str, params: dict[str, Any]) -> Any:
    resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def load_symbols(conn: sqlite3.Connection) -> list[str]:
    cur = conn.execute(f"SELECT symbol FROM {INFO_TABLE}")
    symbols = sorted(row[0] for row in cur.fetchall() if row[0])
    if not symbols:
        raise RuntimeError(f"{INFO_TABLE} 中没有交易对数据，请先同步该表")
    return symbols


def ensure_history_table(conn: sqlite3.Connection) -> None:
    tune_sqlite_connection(conn)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (
            symbol TEXT NOT NULL,
            fundingTime INTEGER NOT NULL,
            fundingRate TEXT,
            updated_at INTEGER,
            PRIMARY KEY (symbol, fundingTime)
        )
        """
    )
    conn.commit()


def delete_older_than(conn: sqlite3.Connection, cutoff_ms: int, symbols: Iterable[str]) -> None:
    symbols = list(set(symbols))
    if not symbols:
        return
    placeholders = ",".join("?" for _ in symbols)
    conn.execute(
        f"DELETE FROM {HISTORY_TABLE} WHERE fundingTime < ? AND symbol IN ({placeholders})",
        (cutoff_ms, *symbols),
    )


def delete_orphan_history(conn: sqlite3.Connection) -> int:
    """清理 history 中已不在 baseinfo 的符号数据，返回删除行数。"""
    cur = conn.execute(
        f"""
        DELETE FROM {HISTORY_TABLE}
        WHERE symbol NOT IN (SELECT symbol FROM {INFO_TABLE})
        """
    )
    return int(cur.rowcount or 0)


def save_history(
    conn: sqlite3.Connection,
    symbol: str,
    records: list[dict[str, Any]],
    *,
    now_ms: int,
) -> tuple[int, int]:
    rows: list[tuple[Any, ...]] = []
    invalid_count = 0
    for entry in records:
        raw_funding_time = entry.get("fundingTime")
        try:
            funding_time = int(raw_funding_time)
        except (TypeError, ValueError):
            invalid_count += 1
            continue
        rows.append(
            (
                symbol,
                funding_time,
                to_plain_str(entry.get("fundingRate")),
                now_ms,
            )
        )
    if not rows:
        return 0, invalid_count
    conn.executemany(
        f"""
        INSERT INTO {HISTORY_TABLE} (symbol, fundingTime, fundingRate, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(symbol, fundingTime) DO UPDATE SET
            fundingRate=excluded.fundingRate,
            updated_at=excluded.updated_at
        """,
        rows,
    )
    return len(rows), invalid_count


def fetch_symbol_history(
    session: requests.Session,
    limiter: RateLimiter,
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> list[dict[str, Any]]:
    base = require_base_url()
    limiter.acquire()
    data = fetch_json(
        session,
        f"{base}{FUNDING_RATE_PATH}",
        {"symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": MAX_RECORDS},
    )
    if not isinstance(data, list):
        raise RuntimeError(f"{symbol} fundingRate 返回格式异常")
    data.sort(key=lambda item: item.get("fundingTime", 0))
    return [item for item in data if isinstance(item, dict)]


def main() -> None:
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - DAYS_TO_FETCH * 24 * 60 * 60 * 1000
    cutoff_ms = end_ms - DAYS_TO_KEEP * 24 * 60 * 60 * 1000
    limiter = RateLimiter(WINDOW_CAPACITY, WINDOW_SECONDS)

    with sqlite3.connect(DB_PATH) as conn, requests.Session() as session:
        ensure_history_table(conn)
        symbols = load_symbols(conn)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}]共 {len(symbols)} 个交易对，开始拉取近 {DAYS_TO_FETCH} 天资金费率")

        for idx, symbol in enumerate(symbols, 1):
            try:
                records = fetch_symbol_history(session, limiter, symbol, start_ms, end_ms)
            except Exception as exc:  # noqa: BLE001
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} 获取失败：{exc}")
                continue
            try:
                saved_count, invalid_count = save_history(conn, symbol, records, now_ms=end_ms)
                delete_older_than(conn, cutoff_ms, [symbol])
                conn.commit()
            except Exception as exc:  # noqa: BLE001
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} 入库失败：{exc}")
                conn.rollback()
                continue

            log_line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][{idx}/{len(symbols)}] {symbol} 入库 {saved_count} 条"
            if invalid_count > 0:
                log_line += f"（跳过 fundingTime 非法 {invalid_count} 条）"
            print(log_line)

        orphan_deleted = delete_orphan_history(conn)
        conn.commit()
        if orphan_deleted > 0:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}]清理孤儿历史记录 {orphan_deleted} 条")

    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}]同步完成")


if __name__ == "__main__":
    main()
