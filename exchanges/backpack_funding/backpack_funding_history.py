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
    collector_log_end,
    collector_log_progress,
    collector_log_start,
    RateLimiter,
    delete_older_than,
    ensure_history_table,
    load_symbols,
    parse_iso_to_ms,
    to_plain_str,
)
from exchanges.backpack_funding.backpack_http import (
    BackpackCircuitOpen,
    BackpackFailureTracker,
    backpack_get,
    summarize_failed_items,
)

BASE_URL = os.getenv("BACKPACK_BASE_URL", "https://api.backpack.exchange").rstrip("/")
FUNDING_RATES_PATH = "/api/v1/fundingRates"

DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT_DIR / "funding.db")).expanduser().resolve()
INFO_TABLE = "backpack_funding_baseinfo"
HISTORY_TABLE = "backpack_funding_history"

DAYS_TO_FETCH = 30
DAYS_TO_KEEP = 60
MAX_RECORDS = 1200

WINDOW_SECONDS = 60
WINDOW_CAPACITY = 600


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
    data = backpack_get(session, BASE_URL, FUNDING_RATES_PATH, params={"symbol": symbol, "limit": MAX_RECORDS})
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
        collector_log_start("Backpack", "history", detail=f"{len(symbols)} 个交易对，近 {DAYS_TO_FETCH} 天资金费率")
        failure_tracker = BackpackFailureTracker("Backpack history")
        aborted_reason: str | None = None
        failed_symbols: list[str] = []

        for idx, symbol in enumerate(symbols, 1):
            try:
                records = fetch_symbol_history(session, limiter, symbol)
            except Exception as exc:  # noqa: BLE001
                failed_symbols.append(symbol)
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} 获取失败：{exc}")
                try:
                    failure_tracker.record_failure(item_label=symbol, exc=exc)
                except BackpackCircuitOpen as circuit_exc:
                    aborted_reason = str(circuit_exc)
                    break
                continue
            failure_tracker.record_success()
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
            collector_log_progress("Backpack", "history", detail=f"{symbol} 入库 {inserted} 条", current=idx, total=len(symbols))

    if aborted_reason:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {aborted_reason}")
    if failed_symbols:
        preview = summarize_failed_items(failed_symbols)
        print(
            f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] "
            f"Backpack history 存在 {len(failed_symbols)} 个失败交易对，将以非零状态退出触发调度重试：{preview}"
        )
        raise SystemExit(1)

    collector_log_end("Backpack", "history")


if __name__ == "__main__":
    main()
