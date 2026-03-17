#!/usr/bin/env python3
"""Fetch Backpack perp funding base info into SQLite."""

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
    bps_to_decimal_str,
    delete_obsolete_symbols,
    ensure_baseinfo_table,
    fetch_existing_symbols,
    to_plain_str,
)

BASE_URL = os.getenv("BACKPACK_BASE_URL", "https://api.backpack.exchange").rstrip("/")
MARKETS_PATH = "/api/v1/markets"
TICKER_PATH = "/api/v1/ticker"
OPEN_INTEREST_PATH = "/api/v1/openInterest"
FUNDING_RATES_PATH = "/api/v1/fundingRates"
REQUEST_TIMEOUT = 15

DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT_DIR / "funding.db")).expanduser().resolve()
TABLE_NAME = "backpack_funding_baseinfo"

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


def fetch_perp_markets(session: requests.Session) -> dict[str, dict[str, Any]]:
    data = bp_get(session, MARKETS_PATH)
    if not isinstance(data, list):
        raise RuntimeError("Backpack markets 返回格式异常（非 list）")
    out: dict[str, dict[str, Any]] = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        symbol = item.get("symbol")
        if not isinstance(symbol, str) or not symbol:
            continue
        if str(item.get("marketType") or "").upper() != "PERP":
            continue
        if item.get("visible") is False:
            continue
        state = str(item.get("orderBookState") or "").lower()
        if state and state not in ("open", "trading", "active"):
            continue
        out[symbol] = item
    if not out:
        raise RuntimeError("未获取到 Backpack PERP 交易对")
    return out


def fetch_ticker(session: requests.Session, symbol: str) -> dict[str, Any] | None:
    data = bp_get(session, TICKER_PATH, params={"symbol": symbol})
    if isinstance(data, dict):
        return data
    return None


def fetch_open_interest(session: requests.Session, symbol: str) -> str | None:
    data = bp_get(session, OPEN_INTEREST_PATH, params={"symbol": symbol})
    if isinstance(data, list) and data:
        first = data[0]
        if isinstance(first, dict):
            return to_plain_str(first.get("openInterest"))
    return None


def fetch_latest_funding_rate(session: requests.Session, symbol: str) -> str | None:
    data = bp_get(session, FUNDING_RATES_PATH, params={"symbol": symbol, "limit": 1})
    if not isinstance(data, list) or not data:
        return None
    first = data[0]
    if not isinstance(first, dict):
        return None
    return to_plain_str(first.get("fundingRate"))


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
        ensure_baseinfo_table(conn, TABLE_NAME)

        markets = fetch_perp_markets(session)
        symbols = sorted(markets.keys())
        collector_log_start("Backpack", "base", detail=f"{len(symbols)} 个交易对")

        rows: list[tuple[Any, ...]] = []
        for idx, symbol in enumerate(symbols, 1):
            limiter.acquire()
            market = markets[symbol]
            ticker: dict[str, Any] | None = None
            oi_s: str | None = None
            last_rate_s: str | None = None

            try:
                ticker = fetch_ticker(session, symbol)
            except Exception as exc:  # noqa: BLE001
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} ticker 获取失败：{exc}")
            try:
                oi_s = fetch_open_interest(session, symbol)
            except Exception as exc:  # noqa: BLE001
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} openInterest 获取失败：{exc}")
            try:
                last_rate_s = fetch_latest_funding_rate(session, symbol)
            except Exception as exc:  # noqa: BLE001
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} fundingRate 获取失败：{exc}")

            interval_ms = market.get("fundingInterval")
            funding_hours = 8
            if isinstance(interval_ms, (int, float)):
                funding_hours = max(1, int(round(float(interval_ms) / 3_600_000)))

            mark_price = None
            if ticker:
                mark_price = to_plain_str(
                    ticker.get("markPrice") or ticker.get("lastPrice") or ticker.get("indexPrice")
                )

            rows.append(
                (
                    symbol,
                    bps_to_decimal_str(market.get("fundingRateUpperBound")),
                    bps_to_decimal_str(market.get("fundingRateLowerBound")),
                    funding_hours,
                    mark_price,
                    last_rate_s,
                    oi_s,
                    None,
                    now_ms,
                )
            )
            if idx % 20 == 0 or idx == len(symbols):
                collector_log_progress("Backpack", "base", detail="处理中", current=idx, total=len(symbols))

        existing = fetch_existing_symbols(conn, TABLE_NAME)
        current = {row[0] for row in rows}
        deleted = delete_obsolete_symbols(conn, TABLE_NAME, existing - current)
        if deleted:
            collector_log_progress("Backpack", "base", detail=f"删除已下架交易对 {len(deleted)} 个")

        save_records(conn, rows)
        collector_log_end("Backpack", "base", detail=f"入库 {len(rows)} 条到 {TABLE_NAME}")


if __name__ == "__main__":
    main()
