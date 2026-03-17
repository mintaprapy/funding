#!/usr/bin/env python3
"""Fetch Lighter perp funding base info into SQLite."""

from __future__ import annotations

import os
import json
import sqlite3
import sys
import time
import ssl
import asyncio
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import requests

try:
    import websockets
except Exception:  # noqa: BLE001
    websockets = None

try:
    import certifi
except Exception:  # noqa: BLE001
    certifi = None

ROOT_DIR = next(parent for parent in Path(__file__).resolve().parents if (parent / "start_all_funding.sh").exists())
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from core.common_funding import (
    collector_log_end,
    collector_log_progress,
    collector_log_start,
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
STREAM_URL = "wss://mainnet.zklighter.elliot.ai/stream"
REQUEST_TIMEOUT = 15
MAX_REQUEST_ATTEMPTS = 4
RETRY_BASE_SLEEP = 1.0
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT_DIR / "funding.db")).expanduser().resolve()
TABLE_NAME = "lighter_funding_baseinfo"
DEFAULT_FUNDING_INTERVAL_HOURS = 1
FUNDING_RATES_EQUIVALENT_HOURS = Decimal("8")


def lighter_get(
    session: requests.Session,
    path: str,
    *,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    url = f"{BASE_URL}{path}"
    for attempt in range(1, MAX_REQUEST_ATTEMPTS + 1):
        try:
            resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict):
                raise RuntimeError(f"{path} 返回格式异常（非 dict）")
            return data
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status not in RETRYABLE_STATUS_CODES or attempt >= MAX_REQUEST_ATTEMPTS:
                raise
            sleep_for = min(20.0, RETRY_BASE_SLEEP * attempt)
            print(
                f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {path} HTTP {status}，"
                f"第 {attempt}/{MAX_REQUEST_ATTEMPTS} 次失败，{sleep_for:.1f}s 后重试"
            )
            time.sleep(sleep_for)
        except (requests.RequestException, RuntimeError) as exc:
            if attempt >= MAX_REQUEST_ATTEMPTS:
                raise
            sleep_for = min(20.0, RETRY_BASE_SLEEP * attempt)
            print(
                f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {path} 请求失败：{exc}，"
                f"第 {attempt}/{MAX_REQUEST_ATTEMPTS} 次失败，{sleep_for:.1f}s 后重试"
            )
            time.sleep(sleep_for)
    raise RuntimeError(f"{path} 请求失败")


def _normalize_rest_funding_rate(rate: Any) -> str | None:
    plain = to_plain_str(rate)
    if plain is None:
        return None
    try:
        value = Decimal(plain)
        interval_hours = Decimal(str(DEFAULT_FUNDING_INTERVAL_HOURS))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if interval_hours <= 0:
        return None
    # /api/v1/funding-rates 返回的是 8h 等效资金费率，这里换回实际 1h 结算口径。
    factor = FUNDING_RATES_EQUIVALENT_HOURS / interval_hours
    if factor <= 0:
        return None
    return to_plain_str(value / factor)


def _extract_active_perp_markets(items: Any, *, source_name: str) -> dict[str, dict[str, Any]]:
    if not isinstance(items, list):
        raise RuntimeError(f"{source_name} 返回格式异常（缺少列表）")
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
        raise RuntimeError(f"未获取到 Lighter perp 交易对（来源：{source_name}）")
    return out


def fetch_order_books(session: requests.Session) -> dict[str, dict[str, Any]]:
    try:
        data = lighter_get(session, ORDER_BOOKS_PATH)
        return _extract_active_perp_markets(data.get("order_books"), source_name="orderBooks")
    except Exception as exc:  # noqa: BLE001
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] 获取 orderBooks 失败，回退到 orderBookDetails：{exc}")
        data = lighter_get(session, ORDER_BOOK_DETAILS_PATH, params={"filter": "perp"})
        return _extract_active_perp_markets(data.get("order_book_details"), source_name="orderBookDetails")


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


async def _fetch_market_stats_via_websocket() -> dict[str, dict[str, Any]]:
    if websockets is None:
        raise RuntimeError("websockets 不可用")
    ssl_ctx = (
        ssl.create_default_context(cafile=certifi.where())
        if certifi is not None
        else ssl.create_default_context()
    )
    async with websockets.connect(
        STREAM_URL,
        ssl=ssl_ctx,
        open_timeout=REQUEST_TIMEOUT,
        close_timeout=5,
        ping_interval=None,
    ) as ws:
        await ws.send(json.dumps({"type": "subscribe", "channel": "market_stats/all"}))
        deadline = time.monotonic() + REQUEST_TIMEOUT
        while time.monotonic() < deadline:
            timeout = max(1.0, deadline - time.monotonic())
            raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            payload = json.loads(raw)
            items = payload.get("market_stats")
            if not isinstance(items, dict):
                continue
            out: dict[str, dict[str, Any]] = {}
            for item in items.values():
                if not isinstance(item, dict):
                    continue
                symbol = item.get("symbol")
                if isinstance(symbol, str) and symbol:
                    out[symbol] = item
            if out:
                return out
    raise RuntimeError("market_stats websocket 未返回可用数据")


def fetch_market_stats_by_symbol() -> dict[str, dict[str, Any]]:
    return asyncio.run(_fetch_market_stats_via_websocket())


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
        rate_s = _normalize_rest_funding_rate(item.get("rate"))
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
        try:
            market_stats_by_symbol = fetch_market_stats_by_symbol()
        except Exception as exc:  # noqa: BLE001
            market_stats_by_symbol = {}
            level = "info" if "websockets 不可用" in str(exc) else "warn"
            print(
                f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][{level}] "
                f"获取 market_stats 失败，回退到 last_trade_price：{exc}"
            )
        rates_by_symbol = fetch_funding_rate_by_symbol(session)
        try:
            oi_by_symbol = fetch_open_interest_by_symbol(session)
        except Exception as exc:  # noqa: BLE001
            oi_by_symbol = {}
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] 获取 open_interest 失败：{exc}")
        symbols = sorted(order_books.keys())
        collector_log_start("Lighter", "base", detail=f"{len(symbols)} 个交易对")

        rows: list[tuple[Any, ...]] = []
        for symbol in symbols:
            stats = stats_by_symbol.get(symbol, {})
            market_stats = market_stats_by_symbol.get(symbol, {})
            rows.append(
                (
                    symbol,
                    None,
                    None,
                    DEFAULT_FUNDING_INTERVAL_HOURS,
                    to_plain_str(market_stats.get("mark_price") or market_stats.get("markPrice") or stats.get("last_trade_price")),
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
            collector_log_progress("Lighter", "base", detail=f"删除已下架交易对 {len(deleted)} 个")

        save_records(conn, rows)
        collector_log_end("Lighter", "base", detail=f"入库 {len(rows)} 条到 {TABLE_NAME}")


if __name__ == "__main__":
    main()
