#!/usr/bin/env python3
"""从 Bybit v5 获取 USDT Linear 合约近 30 天资金费率，存入 SQLite，并保留 60 天内数据。"""

from __future__ import annotations

import os
import sqlite3
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

import requests

BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com").rstrip("/")
FUNDING_HISTORY_PATH = "/v5/market/funding/history"
REQUEST_TIMEOUT = 15

ROOT_DIR = next(parent for parent in Path(__file__).resolve().parents if (parent / "start_all_funding.sh").exists())
DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT_DIR / "funding.db")).expanduser().resolve()
INFO_TABLE = "bybit_funding_baseinfo"
HISTORY_TABLE = "bybit_funding_history"
CATEGORY = "linear"

DAYS_TO_FETCH = 30
DAYS_TO_KEEP = 60

# Bybit v5 公共接口有频控（不同节点不同），这里做一个保守的滑动窗口。
WINDOW_SECONDS = 60
WINDOW_CAPACITY = 300


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
            sleep_for = self.period - (now - self.events[0]) + 0.05
            time.sleep(max(0.1, sleep_for))
        self.events.append(time.monotonic())


def to_plain_str(val: Any) -> str | None:
    try:
        dec = Decimal(str(val))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return format(dec.normalize(), "f")


def bybit_get(
    session: requests.Session,
    path: str,
    *,
    params: dict[str, Any],
) -> dict[str, Any]:
    url = f"{BASE_URL}{path}"
    resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"{path} 返回格式异常（非 dict）")
    if data.get("retCode") != 0:
        raise RuntimeError(f"{path} retCode={data.get('retCode')} retMsg={data.get('retMsg')}")
    result = data.get("result")
    if not isinstance(result, dict):
        raise RuntimeError(f"{path} result 返回格式异常（非 dict）")
    return result


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


def save_history(
    conn: sqlite3.Connection,
    symbol: str,
    records: list[dict[str, Any]],
    *,
    now_ms: int,
) -> None:
    rows = []
    for entry in records:
        ts = (
            entry.get("fundingRateTimestamp")
            or entry.get("fundingTime")
            or entry.get("time")
            or entry.get("timestamp")
        )
        if ts is None:
            continue
        try:
            funding_time = int(ts)
        except (TypeError, ValueError):
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
        return
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


def fetch_symbol_history(
    session: requests.Session,
    limiter: RateLimiter,
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> list[dict[str, Any]]:
    all_records: list[dict[str, Any]] = []
    cursor: str | None = None
    while True:
        limiter.acquire()
        params: dict[str, Any] = {
            "category": CATEGORY,
            "symbol": symbol,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": 200,
        }
        if cursor:
            params["cursor"] = cursor
        last_exc: Exception | None = None
        for attempt in range(5):
            try:
                result = bybit_get(session, FUNDING_HISTORY_PATH, params=params)
                break
            except RuntimeError as exc:
                last_exc = exc
                if "retCode=10006" in str(exc):
                    time.sleep(min(2.0 * (attempt + 1), 10.0))
                    continue
                raise
        else:
            raise RuntimeError(f"{symbol} funding history 重试失败：{last_exc}") from last_exc
        items = result.get("list", [])
        if not isinstance(items, list):
            raise RuntimeError(f"{symbol} funding history 返回格式异常（非 list）")
        for item in items:
            if isinstance(item, dict):
                all_records.append(item)

        cursor_val = result.get("nextPageCursor")
        cursor = cursor_val if isinstance(cursor_val, str) and cursor_val else None
        if not cursor:
            break

        # 防止异常情况下无限翻页
        if len(all_records) > 2000:
            break

    all_records.sort(
        key=lambda item: int(
            item.get("fundingRateTimestamp")
            or item.get("fundingTime")
            or item.get("time")
            or 0
        )
    )
    return all_records


def main() -> None:
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - DAYS_TO_FETCH * 24 * 60 * 60 * 1000
    cutoff_ms = end_ms - DAYS_TO_KEEP * 24 * 60 * 60 * 1000

    limiter = RateLimiter(WINDOW_CAPACITY, WINDOW_SECONDS)

    with sqlite3.connect(DB_PATH) as conn, requests.Session() as session:
        ensure_history_table(conn)
        symbols = load_symbols(conn)
        print(
            f"[{time.strftime('%Y-%m-%d %H:%M:%S')}]共 {len(symbols)} 个交易对，开始拉取近 {DAYS_TO_FETCH} 天资金费率"
        )

        for idx, symbol in enumerate(symbols, 1):
            try:
                records = fetch_symbol_history(session, limiter, symbol, start_ms, end_ms)
            except Exception as exc:  # noqa: BLE001
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} 获取失败：{exc}")
                continue

            save_history(conn, symbol, records, now_ms=end_ms)
            delete_older_than(conn, cutoff_ms, [symbol])
            conn.commit()
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][{idx}/{len(symbols)}] {symbol} 入库 {len(records)} 条")

    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}]同步完成")


if __name__ == "__main__":
    main()
