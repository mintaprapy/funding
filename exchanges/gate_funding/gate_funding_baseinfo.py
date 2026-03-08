#!/usr/bin/env python3
"""Fetch Gate.io USDT perpetual funding base info into SQLite."""

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
    RateLimiter,
    delete_obsolete_symbols,
    ensure_baseinfo_table,
    fetch_existing_symbols,
    normalize_ts_to_ms,
    to_plain_str,
)

BASE_URL = os.getenv("GATE_BASE_URL", "https://api.gateio.ws").rstrip("/")
CONTRACTS_PATH = "/api/v4/futures/usdt/contracts"
TICKERS_PATH = "/api/v4/futures/usdt/tickers"
INSURANCE_PATH = "/api/v4/futures/usdt/insurance"
REQUEST_TIMEOUT = 20
MAX_HTTP_ATTEMPTS = 4

DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT_DIR / "funding.db")).expanduser().resolve()
TABLE_NAME = "gate_funding_baseinfo"

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


def fetch_contracts(session: requests.Session) -> dict[str, dict[str, Any]]:
    data = gate_get(session, CONTRACTS_PATH)
    if not isinstance(data, list):
        raise RuntimeError("Gate contracts 返回格式异常（非 list）")
    out: dict[str, dict[str, Any]] = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        symbol = item.get("name")
        if not isinstance(symbol, str) or not symbol:
            continue
        in_delisting = bool(item.get("in_delisting"))
        if in_delisting:
            continue
        if "_" not in symbol:
            continue
        out[symbol] = item
    if not out:
        raise RuntimeError("未获取到 Gate USDT 永续交易对")
    return out


def fetch_tickers(session: requests.Session) -> dict[str, dict[str, Any]]:
    data = gate_get(session, TICKERS_PATH)
    if not isinstance(data, list):
        raise RuntimeError("Gate tickers 返回格式异常（非 list）")
    out: dict[str, dict[str, Any]] = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        symbol = item.get("contract")
        if isinstance(symbol, str) and symbol:
            out[symbol] = item
    return out


def fetch_insurance_balance(session: requests.Session) -> str | None:
    try:
        data = gate_get(session, INSURANCE_PATH)
    except Exception:
        return None
    if isinstance(data, dict):
        return to_plain_str(data.get("balance") or data.get("total") or data.get("available"))
    if isinstance(data, list) and data:
        first = data[0]
        if isinstance(first, dict):
            return to_plain_str(first.get("balance") or first.get("total") or first.get("available"))
    return None


def calc_open_interest_notional(contract: dict[str, Any], ticker: dict[str, Any] | None) -> str | None:
    if not ticker:
        return None
    direct = to_plain_str(ticker.get("open_interest_usd") or ticker.get("open_interest_value"))
    if direct is not None:
        return direct

    oi_cnt = to_plain_str(ticker.get("total_size") or ticker.get("open_interest"))
    mark = to_plain_str(ticker.get("mark_price") or ticker.get("last") or ticker.get("index_price"))
    mult = to_plain_str(contract.get("quanto_multiplier") or contract.get("multiplier") or 1)
    if oi_cnt is None or mark is None:
        return None
    try:
        from decimal import Decimal

        return to_plain_str(Decimal(oi_cnt) * Decimal(mark) * Decimal(mult or "1"))
    except Exception:  # noqa: BLE001
        return None


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

        limiter.acquire()
        contracts = fetch_contracts(session)
        symbols = sorted(contracts.keys())
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 获取 {len(symbols)} 个交易对（Gate）")

        limiter.acquire()
        tickers = fetch_tickers(session)

        insurance_balance = fetch_insurance_balance(session)

        rows: list[tuple[Any, ...]] = []
        for symbol in symbols:
            contract = contracts[symbol]
            ticker = tickers.get(symbol)

            funding_hours = 8
            interval = contract.get("funding_interval")
            if isinstance(interval, (int, float)):
                funding_hours = max(1, int(interval))

            cap_s = to_plain_str(contract.get("funding_cap_ratio") or contract.get("funding_rate_cap"))
            floor_s = to_plain_str(contract.get("funding_floor_ratio") or contract.get("funding_rate_floor"))

            mark = None
            last_rate = None
            if ticker:
                mark = to_plain_str(ticker.get("mark_price") or ticker.get("last") or ticker.get("index_price"))
                last_rate = to_plain_str(ticker.get("funding_rate") or ticker.get("funding_rate_indicative"))

            rows.append(
                (
                    symbol,
                    cap_s,
                    floor_s,
                    funding_hours,
                    mark,
                    last_rate,
                    calc_open_interest_notional(contract, ticker),
                    insurance_balance,
                    normalize_ts_to_ms(now_ms),
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
