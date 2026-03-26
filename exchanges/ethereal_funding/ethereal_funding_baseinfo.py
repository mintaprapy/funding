#!/usr/bin/env python3
"""Fetch Ethereal perp funding base info into SQLite."""

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
    collector_log_end,
    collector_log_progress,
    collector_log_start,
    delete_obsolete_symbols,
    ensure_baseinfo_table,
    fetch_existing_symbols,
    stamp_rows_updated_at,
    to_plain_str,
)

PRODUCTS_PATH = "/v1/product"
MARKET_PRICE_PATH = "/v1/product/market-price"
REQUEST_TIMEOUT = 15

DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT_DIR / "funding.db")).expanduser().resolve()
TABLE_NAME = "ethereal_funding_baseinfo"
DEFAULT_FUNDING_INTERVAL_HOURS = 1


def _candidate_base_urls() -> list[str]:
    vals = [
        os.getenv("ETHEREAL_BASE_URL"),
        os.getenv("ETHEREAL_GATEWAY_BASE_URL"),
        "https://api.ethereal.trade",
    ]
    out: list[str] = []
    for v in vals:
        if not v:
            continue
        b = v.rstrip("/")
        if b not in out:
            out.append(b)
    return out


def eth_get(
    session: requests.Session,
    base_url: str,
    path: str,
    *,
    params: list[tuple[str, str]] | dict[str, Any] | None = None,
) -> Any:
    url = f"{base_url}{path}"
    resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def resolve_base_url(session: requests.Session) -> str:
    params = {"order": "asc", "orderBy": "createdAt"}
    for base_url in _candidate_base_urls():
        try:
            payload = eth_get(session, base_url, PRODUCTS_PATH, params=params)
            data = payload.get("data") if isinstance(payload, dict) else None
            if isinstance(data, list):
                return base_url
        except Exception:  # noqa: BLE001
            continue
    raise RuntimeError("无法访问 Ethereal API，请设置 ETHEREAL_BASE_URL 指向可用网关")


def fetch_products(session: requests.Session, base_url: str) -> dict[str, dict[str, Any]]:
    payload = eth_get(session, base_url, PRODUCTS_PATH, params={"order": "asc", "orderBy": "createdAt"})
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, list):
        raise RuntimeError("Ethereal /v1/product 返回格式异常")

    out: dict[str, dict[str, Any]] = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        status = str(item.get("status") or "").upper()
        if status and status not in ("ACTIVE", "TRADING"):
            continue
        pid = item.get("id")
        ticker = item.get("ticker")
        if not isinstance(pid, str) or not pid:
            continue
        if not isinstance(ticker, str) or not ticker:
            continue
        out[pid] = item
    if not out:
        raise RuntimeError("未获取到 Ethereal 交易对")
    return out


def fetch_market_prices(
    session: requests.Session,
    base_url: str,
    product_ids: list[str],
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    chunk_size = 50
    for i in range(0, len(product_ids), chunk_size):
        chunk = product_ids[i : i + chunk_size]
        params: list[tuple[str, str]] = [("productIds", pid) for pid in chunk]
        payload = eth_get(session, base_url, MARKET_PRICE_PATH, params=params)
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, list):
            continue
        for item in data:
            if not isinstance(item, dict):
                continue
            pid = item.get("productId")
            if isinstance(pid, str) and pid:
                out[pid] = item
    return out


def _extract_cap_floor(item: dict[str, Any]) -> tuple[str | None, str | None]:
    cap = to_plain_str(item.get("fundingClampApr"))
    floor = None
    if cap is not None:
        try:
            floor = to_plain_str(-Decimal(cap))
        except (InvalidOperation, TypeError, ValueError):
            floor = None
    return cap, floor


def _mul_plain_values(left: Any, right: Any) -> str | None:
    left_plain = to_plain_str(left)
    right_plain = to_plain_str(right)
    if left_plain is None or right_plain is None:
        return None
    try:
        return to_plain_str(Decimal(left_plain) * Decimal(right_plain))
    except (InvalidOperation, TypeError, ValueError):
        return None


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
    with requests.Session() as session, sqlite3.connect(DB_PATH) as conn:
        session.trust_env = False
        ensure_baseinfo_table(conn, TABLE_NAME)
        base_url = resolve_base_url(session)
        collector_log_start("Ethereal", "base", detail=f"使用 API {base_url}")

        products = fetch_products(session, base_url)
        prices = fetch_market_prices(session, base_url, list(products.keys()))
        collector_log_progress("Ethereal", "base", detail=f"获取 {len(products)} 个交易对")

        rows: list[tuple[Any, ...]] = []
        for pid, product in sorted(products.items(), key=lambda x: str(x[1].get("ticker") or "")):
            symbol = str(product.get("ticker") or "")
            if not symbol:
                continue
            price = prices.get(pid, {})
            cap, floor = _extract_cap_floor(product)
            mark_price = to_plain_str(price.get("oraclePrice") or price.get("markPrice"))
            volume_24h = to_plain_str(product.get("volume24h"))
            rows.append(
                (
                    symbol,
                    cap,
                    floor,
                    DEFAULT_FUNDING_INTERVAL_HOURS,
                    mark_price,
                    to_plain_str(product.get("fundingRate1h")),
                    to_plain_str(product.get("openInterest")),
                    None,
                    volume_24h,
                    _mul_plain_values(volume_24h, mark_price),
                    now_ms,
                )
            )

        if not rows:
            raise RuntimeError("Ethereal baseinfo 未获取到可写入记录")

        existing = fetch_existing_symbols(conn, TABLE_NAME)
        current = {row[0] for row in rows}
        deleted = delete_obsolete_symbols(conn, TABLE_NAME, existing - current)
        if deleted:
            collector_log_progress("Ethereal", "base", detail=f"删除已下架交易对 {len(deleted)} 个")

        rows = stamp_rows_updated_at(rows)
        save_records(conn, rows)
        collector_log_end("Ethereal", "base", detail=f"入库 {len(rows)} 条到 {TABLE_NAME}")


if __name__ == "__main__":
    main()
