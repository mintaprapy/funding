#!/usr/bin/env python3
"""Fetch Bitget USDT perpetual funding base info into SQLite."""

from __future__ import annotations

import os
import sqlite3
import sys
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

import requests

ROOT_DIR = next(parent for parent in Path(__file__).resolve().parents if (parent / "start_all_funding.sh").exists())
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from core.common_funding import (
    delete_obsolete_symbols,
    ensure_baseinfo_table,
    fetch_existing_symbols,
    to_plain_str,
)

BASE_URL = os.getenv("BITGET_BASE_URL", "https://api.bitget.com").rstrip("/")
CONTRACTS_PATH = "/api/v2/mix/market/contracts"
TICKERS_PATH = "/api/v2/mix/market/tickers"
REQUEST_TIMEOUT = 15

PRODUCT_TYPE = os.getenv("BITGET_PRODUCT_TYPE", "USDT-FUTURES")

DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT_DIR / "funding.db")).expanduser().resolve()
TABLE_NAME = "bitget_funding_baseinfo"


def bitget_get(session: requests.Session, path: str, *, params: dict[str, Any]) -> dict[str, Any]:
    url = f"{BASE_URL}{path}"
    resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"{path} 返回格式异常（非 dict）")
    if str(data.get("code")) != "00000":
        raise RuntimeError(f"{path} code={data.get('code')} msg={data.get('msg')}")
    return data


def fetch_contracts(session: requests.Session) -> dict[str, dict[str, Any]]:
    payload = bitget_get(session, CONTRACTS_PATH, params={"productType": PRODUCT_TYPE})
    items = payload.get("data")
    if not isinstance(items, list):
        raise RuntimeError("Bitget contracts data 返回格式异常（非 list）")

    out: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        symbol = item.get("symbol")
        if not isinstance(symbol, str) or not symbol:
            continue
        status = str(item.get("symbolStatus") or item.get("status") or "").lower()
        if status and status not in ("normal", "listed", "trading", "online"):
            continue
        out[symbol] = item

    if not out:
        raise RuntimeError("未获取到 Bitget USDT 永续交易对")
    return out


def fetch_tickers(session: requests.Session) -> dict[str, dict[str, Any]]:
    payload = bitget_get(session, TICKERS_PATH, params={"productType": PRODUCT_TYPE})
    items = payload.get("data")
    if not isinstance(items, list):
        raise RuntimeError("Bitget tickers data 返回格式异常（非 list）")

    out: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        symbol = item.get("symbol")
        if isinstance(symbol, str) and symbol:
            out[symbol] = item
    return out


def calc_open_interest_notional(contract: dict[str, Any], ticker: dict[str, Any]) -> str | None:
    direct = to_plain_str(
        ticker.get("openInterestUsd") or ticker.get("holdingAmountUsd") or ticker.get("openInterestValue")
    )
    if direct is not None:
        return direct

    oi = to_plain_str(ticker.get("holdingAmount") or ticker.get("openInterest") or ticker.get("size"))
    mark = to_plain_str(ticker.get("markPrice") or ticker.get("lastPr") or ticker.get("indexPrice"))
    if oi is None or mark is None:
        return None

    multiplier = to_plain_str(
        contract.get("sizeMultiplier") or contract.get("contractSize") or contract.get("minTradeNum") or 1
    )
    try:
        return to_plain_str(Decimal(oi) * Decimal(mark) * Decimal(multiplier or "1"))
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
    with requests.Session() as session, sqlite3.connect(DB_PATH) as conn:
        ensure_baseinfo_table(conn, TABLE_NAME)

        contracts = fetch_contracts(session)
        symbols = sorted(contracts.keys())
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 获取 {len(symbols)} 个交易对（Bitget）")

        tickers = fetch_tickers(session)

        rows: list[tuple[Any, ...]] = []
        missing = 0
        for symbol in symbols:
            contract = contracts[symbol]
            ticker = tickers.get(symbol)
            if not ticker:
                missing += 1
                continue

            interval_hours = 8
            interval = contract.get("fundInterval") or contract.get("fundingInterval")
            if isinstance(interval, str):
                try:
                    iv = float(interval)
                    if iv >= 60:
                        interval_hours = max(1, int(round(iv / 60)))
                    else:
                        interval_hours = max(1, int(round(iv)))
                except ValueError:
                    pass
            elif isinstance(interval, (int, float)):
                iv = float(interval)
                if iv >= 60:
                    interval_hours = max(1, int(round(iv / 60)))
                else:
                    interval_hours = max(1, int(round(iv)))

            rows.append(
                (
                    symbol,
                    to_plain_str(contract.get("maxFundingRate") or contract.get("fundingRateCap")),
                    to_plain_str(contract.get("minFundingRate") or contract.get("fundingRateFloor")),
                    interval_hours,
                    to_plain_str(ticker.get("markPrice") or ticker.get("lastPr") or ticker.get("indexPrice")),
                    to_plain_str(ticker.get("fundingRate") or ticker.get("capitalRate")),
                    calc_open_interest_notional(contract, ticker),
                    None,
                    now_ms,
                )
            )

        if not rows:
            raise RuntimeError("未获取到任何可写入的 Bitget 交易对记录")

        existing = fetch_existing_symbols(conn, TABLE_NAME)
        current = {row[0] for row in rows}

        if missing > 0:
            print(
                f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] 本轮 tickers 缺失 {missing} 条，为避免误删，跳过下架清理"
            )
        else:
            deleted = delete_obsolete_symbols(conn, TABLE_NAME, existing - current)
            if deleted:
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 删除已下架交易对 {len(deleted)} 个")

        save_records(conn, rows)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 入库 {len(rows)} 条到 {TABLE_NAME}")


if __name__ == "__main__":
    main()
