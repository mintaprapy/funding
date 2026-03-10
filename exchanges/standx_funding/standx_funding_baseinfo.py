#!/usr/bin/env python3
"""Fetch StandX perp funding base info into SQLite."""

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
    delete_obsolete_symbols,
    ensure_baseinfo_table,
    fetch_existing_symbols,
    to_plain_str,
)

BASE_URL = os.getenv("STANDX_BASE_URL", "https://perps.standx.com").rstrip("/")
SYMBOL_INFO_PATH = "/api/query_symbol_info"
SYMBOL_MARKET_PATH = "/api/query_symbol_market"
REQUEST_TIMEOUT = 15
MAX_REQUEST_ATTEMPTS = 4
RETRY_BASE_SLEEP = 1.0

DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT_DIR / "funding.db")).expanduser().resolve()
TABLE_NAME = "standx_funding_baseinfo"
DEFAULT_FUNDING_INTERVAL_HOURS = 1

WINDOW_SECONDS = 60
WINDOW_CAPACITY = 300


def sx_get(
    session: requests.Session,
    path: str,
    *,
    params: dict[str, Any] | None = None,
) -> Any:
    url = f"{BASE_URL}{path}"
    last_exc: Exception | None = None
    for attempt in range(1, MAX_REQUEST_ATTEMPTS + 1):
        try:
            resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
            if attempt >= MAX_REQUEST_ATTEMPTS:
                break
            time.sleep(min(10.0, RETRY_BASE_SLEEP * attempt))
    raise RuntimeError(f"请求失败: {url}; last_error={last_exc}") from last_exc


def fetch_symbols(session: requests.Session) -> dict[str, dict[str, Any]]:
    payload = sx_get(session, SYMBOL_INFO_PATH)
    if not isinstance(payload, list):
        raise RuntimeError("query_symbol_info 返回格式异常")

    out: dict[str, dict[str, Any]] = {}
    for item in payload:
        if not isinstance(item, dict):
            continue
        symbol = item.get("symbol")
        if not isinstance(symbol, str) or not symbol:
            continue
        status = str(item.get("status") or "").upper()
        if status and status not in ("ACTIVE", "TRADING"):
            continue
        out[symbol] = item
    if not out:
        raise RuntimeError("未获取到 StandX 交易对")
    return out


def save_records(conn: sqlite3.Connection, rows: list[tuple[Any, ...]]) -> None:
    conn.executemany(
        f"""
        INSERT INTO {TABLE_NAME} (
            symbol, adjustedFundingRateCap, adjustedFundingRateFloor,
            fundingIntervalHours, markPrice, lastFundingRate, openInterest, insuranceBalance, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol) DO UPDATE SET
            adjustedFundingRateCap=excluded.adjustedFundingRateCap,
            adjustedFundingRateFloor=excluded.adjustedFundingRateFloor,
            fundingIntervalHours=excluded.fundingIntervalHours,
            markPrice=excluded.markPrice,
            lastFundingRate=excluded.lastFundingRate,
            openInterest=excluded.openInterest,
            insuranceBalance=excluded.insuranceBalance,
            updated_at=excluded.updated_at
        """,
        rows,
    )
    conn.commit()


def main() -> None:
    now_ms = int(time.time() * 1000)
    limiter = RateLimiter(WINDOW_CAPACITY, WINDOW_SECONDS)

    with requests.Session() as session, sqlite3.connect(DB_PATH) as conn:
        session.trust_env = False
        ensure_baseinfo_table(conn, TABLE_NAME)
        symbols_meta = fetch_symbols(session)
        symbols = sorted(symbols_meta.keys())
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 获取 {len(symbols)} 个交易对（StandX）")

        rows: list[tuple[Any, ...]] = []
        for idx, symbol in enumerate(symbols, 1):
            limiter.acquire()
            try:
                market = sx_get(session, SYMBOL_MARKET_PATH, params={"symbol": symbol})
                if not isinstance(market, dict):
                    market = {}
            except Exception as exc:  # noqa: BLE001
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} query_symbol_market 获取失败：{exc}")
                market = {}

            meta = symbols_meta[symbol]
            rows.append(
                (
                    symbol,
                    to_plain_str(meta.get("funding_rate_cap") or meta.get("fundingRateCap")),
                    to_plain_str(meta.get("funding_rate_floor") or meta.get("fundingRateFloor")),
                    DEFAULT_FUNDING_INTERVAL_HOURS,
                    to_plain_str(market.get("mark_price") or market.get("markPrice")),
                    to_plain_str(market.get("funding_rate") or market.get("fundingRate")),
                    to_plain_str(market.get("open_interest") or market.get("openInterest")),
                    None,
                    now_ms,
                )
            )
            if idx % 30 == 0 or idx == len(symbols):
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][{idx}/{len(symbols)}] 处理中")

        existing = fetch_existing_symbols(conn, TABLE_NAME)
        current = {row[0] for row in rows}
        deleted = delete_obsolete_symbols(conn, TABLE_NAME, existing - current)
        if deleted:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 删除已下架交易对 {len(deleted)} 个")

        save_records(conn, rows)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 入库 {len(rows)} 条到 {TABLE_NAME}")


if __name__ == "__main__":
    main()
