#!/usr/bin/env python3
"""Fetch Backpack perp funding history into SQLite."""

from __future__ import annotations

import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

import requests

ROOT_DIR = next(parent for parent in Path(__file__).resolve().parents if (parent / "start_all_funding.sh").exists())
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from core.common_funding import (
    RateLimiter,
    delete_older_than,
    ensure_history_table,
    load_symbols,
    parse_iso_to_ms,
    to_plain_str,
)

BASE_URL = os.getenv("BACKPACK_BASE_URL", "https://api.backpack.exchange").rstrip("/")
FUNDING_RATES_PATH = "/api/v1/fundingRates"
REQUEST_TIMEOUT = 15

DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT_DIR / "funding.db")).expanduser().resolve()
INFO_TABLE = "backpack_funding_baseinfo"
HISTORY_TABLE = "backpack_funding_history"

DAYS_TO_FETCH = 30
DAYS_TO_KEEP = 60
MAX_RECORDS = 1200

WINDOW_SECONDS = 60
WINDOW_CAPACITY = 600


def bp_get(
    session: requests.Session,
    path: str,
    *,
    params: dict[str, Any] | None = None,
) -> Any:
    url = f"{BASE_URL}{path}"
    resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def save_history(
    conn: sqlite3.Connection,
    symbol: str,
    records: list[dict[str, Any]],
    *,
    now_ms: int,
    start_ms: int,
    end_ms: int,
) -> int:
    rows: list[tuple[Any, ...]] = []
    for entry in records:
        if not isinstance(entry, dict):
            continue
        ts = entry.get("intervalEndTimestamp") or entry.get("fundingTime")
        if isinstance(ts, str):
            funding_time = parse_iso_to_ms(ts)
        else:
            funding_time = None
        if funding_time is None:
            continue
        if funding_time < start_ms or funding_time > end_ms:
            continue
        rows.append((symbol, funding_time, to_plain_str(entry.get("fundingRate")), now_ms))

    if not rows:
        return 0
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
    return len(rows)


def fetch_symbol_history(
    session: requests.Session,
    limiter: RateLimiter,
    symbol: str,
) -> list[dict[str, Any]]:
    limiter.acquire()
    data = bp_get(session, FUNDING_RATES_PATH, params={"symbol": symbol, "limit": MAX_RECORDS})
    if not isinstance(data, list):
        raise RuntimeError(f"{symbol} fundingRates 返回格式异常（非 list）")
    out = [item for item in data if isinstance(item, dict)]
    out.sort(key=lambda x: parse_iso_to_ms(str(x.get("intervalEndTimestamp") or "")) or 0)
    return out


def main() -> None:
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - DAYS_TO_FETCH * 24 * 60 * 60 * 1000
    cutoff_ms = end_ms - DAYS_TO_KEEP * 24 * 60 * 60 * 1000
    limiter = RateLimiter(WINDOW_CAPACITY, WINDOW_SECONDS)

    with sqlite3.connect(DB_PATH) as conn, requests.Session() as session:
        ensure_history_table(conn, HISTORY_TABLE)
        symbols = load_symbols(conn, INFO_TABLE)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}]共 {len(symbols)} 个交易对，开始拉取近 {DAYS_TO_FETCH} 天资金费率")

        for idx, symbol in enumerate(symbols, 1):
            try:
                records = fetch_symbol_history(session, limiter, symbol)
            except Exception as exc:  # noqa: BLE001
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} 获取失败：{exc}")
                continue
            inserted = save_history(
                conn,
                symbol,
                records,
                now_ms=end_ms,
                start_ms=start_ms,
                end_ms=end_ms,
            )
            delete_older_than(conn, HISTORY_TABLE, cutoff_ms, [symbol])
            conn.commit()
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][{idx}/{len(symbols)}] {symbol} 入库 {inserted} 条")

    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}]同步完成")


if __name__ == "__main__":
    main()
