#!/usr/bin/env python3
"""Fetch Lighter perp funding history into SQLite."""

from __future__ import annotations

import os
import sqlite3
import sys
import time
from decimal import Decimal, InvalidOperation
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

BASE_URL = os.getenv("LIGHTER_BASE_URL", "https://mainnet.zklighter.elliot.ai").rstrip("/")
ORDER_BOOKS_PATH = "/api/v1/orderBooks"
FUNDINGS_PATH = "/api/v1/fundings"
REQUEST_TIMEOUT = 15

DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT_DIR / "funding.db")).expanduser().resolve()
INFO_TABLE = "lighter_funding_baseinfo"
HISTORY_TABLE = "lighter_funding_history"

DAYS_TO_FETCH = 30
DAYS_TO_KEEP = 60
RESOLUTION = "1h"

WINDOW_SECONDS = 60
WINDOW_CAPACITY = 80


def lighter_get(
    session: requests.Session,
    path: str,
    *,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    url = f"{BASE_URL}{path}"
    resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"{path} 返回格式异常（非 dict）")
    return data


def fetch_market_ids(session: requests.Session) -> dict[str, int]:
    data = lighter_get(session, ORDER_BOOKS_PATH)
    items = data.get("order_books")
    if not isinstance(items, list):
        raise RuntimeError("orderBooks 返回格式异常（缺少 order_books）")
    out: dict[str, int] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        if str(item.get("market_type") or "").lower() != "perp":
            continue
        if str(item.get("status") or "").lower() not in ("active", "open", "trading"):
            continue
        symbol = item.get("symbol")
        market_id = item.get("market_id")
        if isinstance(symbol, str) and symbol and isinstance(market_id, int):
            out[symbol] = market_id
    return out


def _to_signed_rate(rate: Any, direction: Any) -> str | None:
    rate_s = to_plain_str(rate)
    if rate_s is None:
        return None
    direction_s = str(direction or "").strip().lower()
    if direction_s not in ("long", "short"):
        return rate_s
    try:
        dec = Decimal(rate_s)
    except (InvalidOperation, TypeError, ValueError):
        return rate_s
    # Lighter 返回 direction + 正值 rate：约定 direction=short 记为负值。
    if direction_s == "short":
        dec = -dec
    return to_plain_str(dec)


def fetch_symbol_history(
    session: requests.Session,
    limiter: RateLimiter,
    *,
    market_id: int,
    start_sec: int,
    end_sec: int,
    count_back: int,
) -> list[dict[str, Any]]:
    params = {
        "market_id": market_id,
        "resolution": RESOLUTION,
        "start_timestamp": start_sec,
        "end_timestamp": end_sec,
        "count_back": count_back,
    }
    backoff = 1.5
    for attempt in range(6):
        limiter.acquire()
        try:
            data = lighter_get(session, FUNDINGS_PATH, params=params)
        except requests.HTTPError as exc:
            status = getattr(exc.response, "status_code", None)
            if status == 429 and attempt < 5:
                retry_after = None
                if exc.response is not None:
                    retry_after = exc.response.headers.get("Retry-After")
                try:
                    sleep_for = float(retry_after) if retry_after else backoff
                except (TypeError, ValueError):
                    sleep_for = backoff
                time.sleep(min(30.0, max(1.0, sleep_for)))
                backoff = min(30.0, backoff * 2)
                continue
            raise
        code = data.get("code")
        if code == 200:
            items = data.get("fundings")
            if not isinstance(items, list):
                raise RuntimeError("fundings 返回格式异常（fundings 非 list）")
            out = [item for item in items if isinstance(item, dict)]
            out.sort(key=lambda x: int(x.get("timestamp") or 0))
            return out
        if code == 20001 and attempt < 5:
            # 临时参数校验/限流错误时，短暂退避后重试
            time.sleep(min(20.0, backoff))
            backoff = min(30.0, backoff * 2)
            continue
        msg = data.get("message")
        raise RuntimeError(f"fundings code={code} message={msg}")
    return []


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
        ts = item.get("timestamp")
        try:
            funding_time = int(ts) * 1000
        except (TypeError, ValueError):
            continue
        if funding_time < start_ms or funding_time > end_ms:
            continue
        rows.append(
            (
                symbol,
                funding_time,
                _to_signed_rate(item.get("rate"), item.get("direction")),
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
    start_sec = start_ms // 1000
    end_sec = end_ms // 1000
    expected_hours = DAYS_TO_FETCH * 24
    limiter = RateLimiter(WINDOW_CAPACITY, WINDOW_SECONDS)

    with sqlite3.connect(DB_PATH) as conn, requests.Session() as session:
        ensure_history_table(conn, HISTORY_TABLE)
        symbols = load_symbols(conn, INFO_TABLE)
        market_ids = fetch_market_ids(session)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}]共 {len(symbols)} 个交易对，开始拉取近 {DAYS_TO_FETCH} 天资金费率")

        for idx, symbol in enumerate(symbols, 1):
            market_id = market_ids.get(symbol)
            if market_id is None:
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} 缺少 market_id，跳过")
                continue
            try:
                records = fetch_symbol_history(
                    session,
                    limiter,
                    market_id=market_id,
                    start_sec=start_sec,
                    end_sec=end_sec,
                    count_back=expected_hours,
                )
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
