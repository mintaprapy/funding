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
    stamp_rows_updated_at,
    to_plain_str,
)
from exchanges.backpack_funding.backpack_http import (
    BackpackCircuitOpen,
    BackpackFailureTracker,
    backpack_get,
    summarize_failed_items,
)

BASE_URL = os.getenv("BACKPACK_BASE_URL", "https://api.backpack.exchange").rstrip("/")
MARKETS_PATH = "/api/v1/markets"
TICKER_PATH = "/api/v1/ticker"
OPEN_INTEREST_PATH = "/api/v1/openInterest"
FUNDING_RATES_PATH = "/api/v1/fundingRates"

DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT_DIR / "funding.db")).expanduser().resolve()
TABLE_NAME = "backpack_funding_baseinfo"

WINDOW_SECONDS = 60
WINDOW_CAPACITY = 600


def fetch_perp_markets(session: requests.Session) -> dict[str, dict[str, Any]]:
    data = backpack_get(session, BASE_URL, MARKETS_PATH)
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
    data = backpack_get(session, BASE_URL, TICKER_PATH, params={"symbol": symbol})
    if isinstance(data, dict):
        return data
    return None


def fetch_open_interest(session: requests.Session, symbol: str) -> str | None:
    data = backpack_get(session, BASE_URL, OPEN_INTEREST_PATH, params={"symbol": symbol})
    if isinstance(data, list) and data:
        first = data[0]
        if isinstance(first, dict):
            return to_plain_str(first.get("openInterest"))
    return None


def fetch_latest_funding_rate(session: requests.Session, symbol: str) -> str | None:
    data = backpack_get(session, BASE_URL, FUNDING_RATES_PATH, params={"symbol": symbol, "limit": 1})
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
            fundingIntervalHours, markPrice, lastFundingRate, openInterest, insuranceBalance,
            volume24h, turnover24h, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol) DO UPDATE SET
            adjustedFundingRateCap=excluded.adjustedFundingRateCap,
            adjustedFundingRateFloor=excluded.adjustedFundingRateFloor,
            fundingIntervalHours=excluded.fundingIntervalHours,
            markPrice=excluded.markPrice,
            lastFundingRate=excluded.lastFundingRate,
            openInterest=excluded.openInterest,
            insuranceBalance=excluded.insuranceBalance,
            volume24h=excluded.volume24h,
            turnover24h=excluded.turnover24h,
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
        failure_tracker = BackpackFailureTracker("Backpack base")
        failed_requests: list[str] = []
        failed_symbols: set[str] = set()
        aborted_reason: str | None = None

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
                failed_requests.append(f"{symbol}:ticker")
                failed_symbols.add(symbol)
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} ticker 获取失败：{exc}")
                try:
                    failure_tracker.record_failure(item_label=f"{symbol}:ticker", exc=exc)
                except BackpackCircuitOpen as circuit_exc:
                    aborted_reason = str(circuit_exc)
                    break
                continue
            failure_tracker.record_success()

            try:
                oi_s = fetch_open_interest(session, symbol)
            except Exception as exc:  # noqa: BLE001
                failed_requests.append(f"{symbol}:openInterest")
                failed_symbols.add(symbol)
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} openInterest 获取失败：{exc}")
                try:
                    failure_tracker.record_failure(item_label=f"{symbol}:openInterest", exc=exc)
                except BackpackCircuitOpen as circuit_exc:
                    aborted_reason = str(circuit_exc)
                    break
                continue
            failure_tracker.record_success()

            try:
                last_rate_s = fetch_latest_funding_rate(session, symbol)
            except Exception as exc:  # noqa: BLE001
                failed_requests.append(f"{symbol}:fundingRate")
                failed_symbols.add(symbol)
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} fundingRate 获取失败：{exc}")
                try:
                    failure_tracker.record_failure(item_label=f"{symbol}:fundingRate", exc=exc)
                except BackpackCircuitOpen as circuit_exc:
                    aborted_reason = str(circuit_exc)
                    break
                continue
            failure_tracker.record_success()

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
                    to_plain_str((ticker or {}).get("volume")),
                    to_plain_str((ticker or {}).get("quoteVolume")),
                    now_ms,
                )
            )
            if idx % 20 == 0 or idx == len(symbols):
                collector_log_progress("Backpack", "base", detail="处理中", current=idx, total=len(symbols))

        if aborted_reason:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {aborted_reason}")
        if failed_requests:
            # Keep the previous Backpack snapshot intact and let the scheduler retry.
            preview = summarize_failed_items(failed_requests)
            print(
                f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] "
                f"Backpack base 存在 {len(failed_requests)} 个失败请求，涉及 {len(failed_symbols)} 个交易对，"
                f"将以非零状态退出触发调度重试：{preview}"
            )
            raise SystemExit(1)

        existing = fetch_existing_symbols(conn, TABLE_NAME)
        current = {row[0] for row in rows}
        deleted = delete_obsolete_symbols(conn, TABLE_NAME, existing - current)
        if deleted:
            collector_log_progress("Backpack", "base", detail=f"删除已下架交易对 {len(deleted)} 个")

        rows = stamp_rows_updated_at(rows)
        save_records(conn, rows)
        collector_log_end("Backpack", "base", detail=f"入库 {len(rows)} 条到 {TABLE_NAME}")


if __name__ == "__main__":
    main()
