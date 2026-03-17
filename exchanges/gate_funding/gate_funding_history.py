#!/usr/bin/env python3
"""Fetch Gate.io USDT perpetual funding history into SQLite."""

from __future__ import annotations

import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

import requests
from requests import RequestException

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
    normalize_ts_to_ms,
    to_plain_str,
)

BASE_URL = os.getenv("GATE_BASE_URL", "https://api.gateio.ws").rstrip("/")
FUNDING_HISTORY_PATH = "/api/v4/futures/usdt/funding_rate"
REQUEST_TIMEOUT = 20
MAX_HTTP_ATTEMPTS = 4

DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT_DIR / "funding.db")).expanduser().resolve()
INFO_TABLE = "gate_funding_baseinfo"
HISTORY_TABLE = "gate_funding_history"

DAYS_TO_FETCH = 30
DAYS_TO_KEEP = 60
MAX_RECORDS = 1000

WINDOW_SECONDS = 60
WINDOW_CAPACITY = 600


def gate_get(session: requests.Session, path: str, *, params: dict[str, Any] | None = None) -> Any:
    url = f"{BASE_URL}{path}"
    last_exc: Exception | None = None
    for attempt in range(1, MAX_HTTP_ATTEMPTS + 1):
        try:
            resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code >= 500:
                raise RuntimeError(f"{path} http {resp.status_code}")
            resp.raise_for_status()
            return resp.json()
        except (RequestException, RuntimeError) as exc:
            last_exc = exc
            if attempt >= MAX_HTTP_ATTEMPTS:
                break
            time.sleep(min(1.0 * attempt, 3.0))
    raise RuntimeError(f"Gate 请求失败: {path}, err={last_exc}") from last_exc


def fetch_symbol_history(
    session: requests.Session,
    limiter: RateLimiter,
    symbol: str,
) -> list[dict[str, Any]]:
    limiter.acquire()
    data = gate_get(session, FUNDING_HISTORY_PATH, params={"contract": symbol, "limit": MAX_RECORDS})
    if not isinstance(data, list):
        raise RuntimeError(f"{symbol} funding history 返回格式异常（非 list）")
    out = [item for item in data if isinstance(item, dict)]
    out.sort(key=lambda x: normalize_ts_to_ms(x.get("t") or x.get("time")) or 0)
    return out


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
    for item in records:
        funding_time = normalize_ts_to_ms(item.get("t") or item.get("time"))
        if funding_time is None or funding_time < start_ms or funding_time > end_ms:
            continue
        funding_rate = to_plain_str(item.get("r") or item.get("funding_rate") or item.get("fundingRate"))
        rows.append((symbol, funding_time, funding_rate, now_ms))

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


def main() -> None:
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - DAYS_TO_FETCH * 24 * 60 * 60 * 1000
    cutoff_ms = end_ms - DAYS_TO_KEEP * 24 * 60 * 60 * 1000

    limiter = RateLimiter(WINDOW_CAPACITY, WINDOW_SECONDS)

    with sqlite3.connect(DB_PATH) as conn, requests.Session() as session:
        ensure_history_table(conn, HISTORY_TABLE)
        symbols = load_symbols(conn, INFO_TABLE)
        collector_log_start("Gate", "history", detail=f"{len(symbols)} 个交易对，近 {DAYS_TO_FETCH} 天资金费率")

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
            collector_log_progress("Gate", "history", detail=f"{symbol} 入库 {inserted} 条", current=idx, total=len(symbols))

    collector_log_end("Gate", "history")


if __name__ == "__main__":
    main()
