#!/usr/bin/env python3
"""Fetch GRVT perp funding history into SQLite."""

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
    to_plain_str,
)

BASE_URL = os.getenv("GRVT_BASE_URL", "https://market-data.grvt.io").rstrip("/")
FUNDING_HISTORY_PATH = "/full/v1/funding"
REQUEST_TIMEOUT = 20
MAX_REQUEST_ATTEMPTS = 4
RETRY_BASE_SLEEP = 1.0

DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT_DIR / "funding.db")).expanduser().resolve()
INFO_TABLE = "grvt_funding_baseinfo"
HISTORY_TABLE = "grvt_funding_history"

DAYS_TO_FETCH = 30
DAYS_TO_KEEP = 60

WINDOW_SECONDS = 60
WINDOW_CAPACITY = 180


def _epoch_to_ms(ts: Any) -> int | None:
    try:
        t = int(ts)
    except (TypeError, ValueError):
        return None
    # ns -> ms
    if t >= 1_000_000_000_000_000_000:
        return t // 1_000_000
    # us -> ms
    if t >= 1_000_000_000_000_000:
        return t // 1000
    # s -> ms
    if t < 1_000_000_000_000:
        return t * 1000
    return t


def grvt_post(session: requests.Session, path: str, payload: dict[str, Any]) -> Any:
    url = f"{BASE_URL}{path}"
    last_exc: Exception | None = None
    for attempt in range(1, MAX_REQUEST_ATTEMPTS + 1):
        try:
            resp = session.post(url, json=payload, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
            if attempt >= MAX_REQUEST_ATTEMPTS:
                break
            time.sleep(min(10.0, RETRY_BASE_SLEEP * attempt))
    raise RuntimeError(f"请求失败: {url}; last_error={last_exc}")


def extract_data_list(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    result = payload.get("result")
    if isinstance(result, list):
        return [item for item in result if isinstance(item, dict)]
    if isinstance(result, dict):
        data = result.get("data")
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
    data = payload.get("data")
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    return []


def fetch_symbol_history(
    session: requests.Session,
    limiter: RateLimiter,
    symbol: str,
) -> list[dict[str, Any]]:
    limiter.acquire()
    payload = grvt_post(session, FUNDING_HISTORY_PATH, {"instrument": symbol})
    items = extract_data_list(payload)
    items.sort(key=lambda x: _epoch_to_ms(x.get("funding_time")) or 0)
    return items


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
        funding_time = _epoch_to_ms(item.get("funding_time") or item.get("timestamp"))
        if funding_time is None or funding_time < start_ms or funding_time > end_ms:
            continue
        rows.append(
            (
                symbol,
                funding_time,
                to_plain_str(item.get("funding_rate") or item.get("rate")),
                now_ms,
            )
        )
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
        session.trust_env = False
        ensure_history_table(conn, HISTORY_TABLE)
        symbols = load_symbols(conn, INFO_TABLE)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}]共 {len(symbols)} 个交易对，开始拉取近 {DAYS_TO_FETCH} 天资金费率")

        wrote_any = False
        failed_symbols: list[str] = []
        for idx, symbol in enumerate(symbols, 1):
            try:
                records = fetch_symbol_history(session, limiter, symbol)
            except Exception as exc:  # noqa: BLE001
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} 获取失败：{exc}")
                failed_symbols.append(symbol)
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
            wrote_any = wrote_any or inserted > 0
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][{idx}/{len(symbols)}] {symbol} 入库 {inserted} 条")

        if not wrote_any:
            raise RuntimeError("GRVT history 未写入任何记录")
        if failed_symbols:
            preview = ",".join(failed_symbols[:10])
            suffix = "" if len(failed_symbols) <= 10 else f" ... total={len(failed_symbols)}"
            raise RuntimeError(f"GRVT history 存在未成功拉取的交易对: {preview}{suffix}")

    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}]同步完成")


if __name__ == "__main__":
    main()
