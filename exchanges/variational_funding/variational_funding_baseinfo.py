#!/usr/bin/env python3
"""Fetch Variational funding base info into SQLite."""

from __future__ import annotations

import os
import sqlite3
import sys
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import requests
from requests import RequestException

ROOT_DIR = next(parent for parent in Path(__file__).resolve().parents if (parent / "start_all_funding.sh").exists())
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from core.common_funding import (
    annualized_decimal_to_interval_decimal_str,
    delete_obsolete_symbols,
    ensure_baseinfo_table,
    fetch_existing_symbols,
    to_plain_str,
)

BASE_URL = os.getenv("VARIATIONAL_BASE_URL", "https://omni-client-api.prod.ap-northeast-1.variational.io").rstrip("/")
STATS_PATH = "/metadata/stats"
REQUEST_TIMEOUT = 20
MAX_HTTP_ATTEMPTS = 4

DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT_DIR / "funding.db")).expanduser().resolve()
TABLE_NAME = "variational_funding_baseinfo"


def variational_get(session: requests.Session, path: str) -> dict[str, Any]:
    url = f"{BASE_URL}{path}"
    last_exc: Exception | None = None
    for attempt in range(1, MAX_HTTP_ATTEMPTS + 1):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code >= 500:
                raise RuntimeError(f"{path} http {resp.status_code}")
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict):
                raise RuntimeError(f"{path} 返回格式异常（非 dict）")
            return data
        except (RequestException, RuntimeError) as exc:
            last_exc = exc
            if attempt >= MAX_HTTP_ATTEMPTS:
                break
            time.sleep(min(1.0 * attempt, 3.0))
    raise RuntimeError(f"Variational 请求失败: {path}, err={last_exc}") from last_exc


def calc_open_interest_notional(item: dict[str, Any]) -> str | None:
    oi = item.get("open_interest")
    if not isinstance(oi, dict):
        return None
    long_s = to_plain_str(oi.get("long_open_interest") or 0)
    short_s = to_plain_str(oi.get("short_open_interest") or 0)
    if long_s is None and short_s is None:
        return None
    try:
        long_v = Decimal(long_s or "0")
        short_v = Decimal(short_s or "0")
    except (InvalidOperation, TypeError, ValueError):
        return None
    # Inference from official /metadata/stats payloads: platform-level open_interest is
    # exactly 2 * sum(listing.long_open_interest + listing.short_open_interest), so we
    # scale per-market notional the same way to keep exchange totals internally consistent.
    return to_plain_str((long_v + short_v) * Decimal("2"))


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
        stats = variational_get(session, STATS_PATH)
        listings = stats.get("listings")
        if not isinstance(listings, list):
            raise RuntimeError("Variational stats 返回格式异常（缺少 listings）")

        insurance_balance = None
        loss_refund = stats.get("loss_refund")
        if isinstance(loss_refund, dict):
            insurance_balance = to_plain_str(loss_refund.get("pool_size"))

        rows: list[tuple[Any, ...]] = []
        for item in listings:
            if not isinstance(item, dict):
                continue
            symbol = item.get("ticker")
            if not isinstance(symbol, str) or not symbol:
                continue
            interval_hours = 8
            interval_s = item.get("funding_interval_s")
            try:
                interval_hours = max(1, int(round(float(interval_s) / 3600)))
            except (TypeError, ValueError):
                pass
            rows.append(
                (
                    symbol,
                    None,
                    None,
                    interval_hours,
                    to_plain_str(item.get("mark_price")),
                    annualized_decimal_to_interval_decimal_str(item.get("funding_rate"), item.get("funding_interval_s")),
                    calc_open_interest_notional(item),
                    insurance_balance,
                    now_ms,
                )
            )

        if not rows:
            raise RuntimeError("未获取到任何可写入的 Variational 交易对记录")

        existing = fetch_existing_symbols(conn, TABLE_NAME)
        current = {row[0] for row in rows}
        deleted = delete_obsolete_symbols(conn, TABLE_NAME, existing - current)
        if deleted:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 删除已下架交易对 {len(deleted)} 个")

        save_records(conn, rows)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 入库 {len(rows)} 条到 {TABLE_NAME}")


if __name__ == "__main__":
    main()
