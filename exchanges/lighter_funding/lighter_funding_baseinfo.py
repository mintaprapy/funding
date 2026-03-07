#!/usr/bin/env python3
"""Fetch Lighter perp funding base info into SQLite."""

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
    ensure_baseinfo_table,
    fetch_existing_symbols,
    delete_obsolete_symbols,
    to_plain_str,
)

BASE_URL = os.getenv("LIGHTER_BASE_URL", "https://mainnet.zklighter.elliot.ai").rstrip("/")
ORDER_BOOKS_PATH = "/api/v1/orderBooks"
ORDER_BOOK_DETAILS_PATH = "/api/v1/orderBookDetails"
EXCHANGE_STATS_PATH = "/api/v1/exchangeStats"
FUNDING_RATES_PATH = "/api/v1/funding-rates"
REQUEST_TIMEOUT = 15

DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT_DIR / "funding.db")).expanduser().resolve()
TABLE_NAME = "lighter_funding_baseinfo"
DEFAULT_FUNDING_INTERVAL_HOURS = 1


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


def fetch_order_books(session: requests.Session) -> dict[str, dict[str, Any]]:
    data = lighter_get(session, ORDER_BOOKS_PATH)
    items = data.get("order_books")
    if not isinstance(items, list):
        raise RuntimeError("orderBooks 返回格式异常（缺少 order_books）")
    out: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        symbol = item.get("symbol")
        if not isinstance(symbol, str) or not symbol:
            continue
        if str(item.get("market_type") or "").lower() != "perp":
            continue
        if str(item.get("status") or "").lower() not in ("active", "open", "trading"):
            continue
        out[symbol] = item
    if not out:
        raise RuntimeError("未获取到 Lighter perp 交易对")
    return out


def fetch_stats_by_symbol(session: requests.Session) -> dict[str, dict[str, Any]]:
    data = lighter_get(session, EXCHANGE_STATS_PATH)
    items = data.get("order_book_stats")
    if not isinstance(items, list):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        symbol = item.get("symbol")
        if isinstance(symbol, str) and symbol:
            out[symbol] = item
    return out


def fetch_funding_rate_by_symbol(session: requests.Session) -> dict[str, str]:
    data = lighter_get(session, FUNDING_RATES_PATH)
    items = data.get("funding_rates")
    if not isinstance(items, list):
        return {}
    out: dict[str, str] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        symbol = item.get("symbol")
        if not isinstance(symbol, str) or not symbol:
            continue
        rate_s = to_plain_str(item.get("rate"))
        if rate_s is not None:
            out[symbol] = rate_s
    return out


def fetch_open_interest_by_symbol(session: requests.Session) -> dict[str, str]:
    data = lighter_get(session, ORDER_BOOK_DETAILS_PATH, params={"filter": "perp"})
    items = data.get("order_book_details")
    if not isinstance(items, list):
        return {}
    out: dict[str, str] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        symbol = item.get("symbol")
        if not isinstance(symbol, str) or not symbol:
            continue
        oi_s = to_plain_str(item.get("open_interest"))
        if oi_s is not None:
            out[symbol] = oi_s
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
    with requests.Session() as session, sqlite3.connect(DB_PATH) as conn:
        ensure_baseinfo_table(conn, TABLE_NAME)

        order_books = fetch_order_books(session)
        stats_by_symbol = fetch_stats_by_symbol(session)
        rates_by_symbol = fetch_funding_rate_by_symbol(session)
        try:
            oi_by_symbol = fetch_open_interest_by_symbol(session)
        except Exception as exc:  # noqa: BLE001
            oi_by_symbol = {}
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] 获取 open_interest 失败：{exc}")
        symbols = sorted(order_books.keys())
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 获取 {len(symbols)} 个交易对（Lighter）")

        rows: list[tuple[Any, ...]] = []
        for symbol in symbols:
            stats = stats_by_symbol.get(symbol, {})
            rows.append(
                (
                    symbol,
                    None,
                    None,
                    DEFAULT_FUNDING_INTERVAL_HOURS,
                    to_plain_str(stats.get("last_trade_price")),
                    rates_by_symbol.get(symbol),
                    oi_by_symbol.get(symbol),
                    None,
                    now_ms,
                )
            )

        existing = fetch_existing_symbols(conn, TABLE_NAME)
        current = {row[0] for row in rows}
        deleted = delete_obsolete_symbols(conn, TABLE_NAME, existing - current)
        if deleted:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 删除已下架交易对 {len(deleted)} 个")

        save_records(conn, rows)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 入库 {len(rows)} 条到 {TABLE_NAME}")


if __name__ == "__main__":
    main()
