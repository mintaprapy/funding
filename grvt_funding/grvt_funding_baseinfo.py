#!/usr/bin/env python3
"""Fetch GRVT perp funding base info into SQLite."""

from __future__ import annotations

import os
import sqlite3
import sys
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import requests

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from common_funding import (
    RateLimiter,
    delete_obsolete_symbols,
    ensure_baseinfo_table,
    fetch_existing_symbols,
    to_plain_str,
)

BASE_URL = os.getenv("GRVT_BASE_URL", "https://market-data.grvt.io").rstrip("/")
SYMBOLS_PATH = "/full/v1/all_instruments"
TICKER_PATH = "/full/v1/ticker"
REQUEST_TIMEOUT = 20

DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (Path(__file__).resolve().parent.parent / "funding.db")).expanduser().resolve()
TABLE_NAME = "grvt_funding_baseinfo"
DEFAULT_FUNDING_INTERVAL_HOURS = 8

WINDOW_SECONDS = 60
WINDOW_CAPACITY = 180


def grvt_post(
    session: requests.Session,
    path: str,
    payload: dict[str, Any] | None = None,
) -> Any:
    url = f"{BASE_URL}{path}"
    resp = session.post(url, json=payload or {}, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


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


def extract_data_object(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    result = payload.get("result")
    if isinstance(result, dict):
        return result
    data = payload.get("data")
    if isinstance(data, dict):
        return data
    return payload


def fetch_symbols(session: requests.Session) -> dict[str, dict[str, Any]]:
    payload = grvt_post(session, SYMBOLS_PATH, {})
    items = extract_data_list(payload)
    out: dict[str, dict[str, Any]] = {}
    for item in items:
        instrument = item.get("instrument")
        if not isinstance(instrument, str) or not instrument:
            continue
        quote = str(item.get("quote") or "").upper()
        kind = str(item.get("kind") or "").upper()
        if quote != "USDT":
            continue
        if "PERP" not in kind:
            continue
        out[instrument] = item
    if not out:
        raise RuntimeError("未获取到 GRVT USDT 永续交易对")
    return out


def fetch_tickers(session: requests.Session, symbols: list[str], limiter: RateLimiter) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for symbol in symbols:
        try:
            limiter.acquire()
            payload = grvt_post(session, TICKER_PATH, {"instrument": symbol})
        except Exception as exc:  # noqa: BLE001
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} ticker 获取失败：{exc}")
            continue
        obj = extract_data_object(payload) or {}
        if isinstance(obj, dict):
            out[symbol] = obj
    return out


def _extract_interval_hours(item: dict[str, Any]) -> int:
    for key in ("funding_interval_hours", "fundingIntervalHours", "funding_interval", "fundingInterval"):
        v = item.get(key)
        if v is None:
            continue
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if f >= 60:
            return max(1, int(round(f / 60)))
        return max(1, int(round(f)))
    return DEFAULT_FUNDING_INTERVAL_HOURS


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
        tickers = fetch_tickers(session, symbols, limiter)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 获取 {len(symbols)} 个交易对（GRVT）")

        rows: list[tuple[Any, ...]] = []
        for idx, symbol in enumerate(symbols, 1):
            meta = symbols_meta[symbol]
            ticker = tickers.get(symbol, {})

            mark_price = to_plain_str(ticker.get("mark_price") or ticker.get("markPrice"))
            open_interest = to_plain_str(ticker.get("open_interest") or ticker.get("openInterest"))
            if open_interest is None:
                oi_qty = to_plain_str(ticker.get("open_interest_qty") or ticker.get("openInterestQty"))
                if oi_qty is not None and mark_price is not None:
                    try:
                        open_interest = to_plain_str(Decimal(oi_qty) * Decimal(mark_price))
                    except (InvalidOperation, TypeError, ValueError):
                        open_interest = None

            rows.append(
                (
                    symbol,
                    to_plain_str(meta.get("adjusted_funding_rate_cap") or meta.get("funding_rate_upper_limit")),
                    to_plain_str(meta.get("adjusted_funding_rate_floor") or meta.get("funding_rate_lower_limit")),
                    _extract_interval_hours(meta),
                    mark_price,
                    to_plain_str(ticker.get("funding_rate") or ticker.get("funding")),
                    open_interest,
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
