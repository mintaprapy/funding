#!/usr/bin/env python3
"""Sync Ethereal funding history into SQLite via the public funding API."""

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
    collector_log_end,
    collector_log_progress,
    collector_log_start,
    delete_older_than,
    ensure_history_table,
    load_symbols,
    normalize_ts_to_ms,
    to_plain_str,
)

PRODUCTS_PATH = "/v1/product"
FUNDING_PATH = "/v1/funding"
REQUEST_TIMEOUT = 15
MAX_HTTP_ATTEMPTS = 4
PAGE_LIMIT = 100

DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT_DIR / "funding.db")).expanduser().resolve()
INFO_TABLE = "ethereal_funding_baseinfo"
HISTORY_TABLE = "ethereal_funding_history"

DAYS_TO_KEEP = 60
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
    raise RuntimeError(f"Ethereal 请求失败: {path}, err={last_exc}") from last_exc


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


def fetch_products_by_symbol(session: requests.Session, base_url: str) -> dict[str, dict[str, Any]]:
    payload = eth_get(session, base_url, PRODUCTS_PATH, params={"order": "asc", "orderBy": "createdAt"})
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, list):
        raise RuntimeError("Ethereal /v1/product 返回格式异常")

    out: dict[str, dict[str, Any]] = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        ticker = item.get("ticker")
        if not isinstance(ticker, str) or not ticker:
            continue
        status = str(item.get("status") or "").upper()
        if status and status not in ("ACTIVE", "TRADING"):
            continue
        out[ticker] = item
    return out


def fetch_funding_rows(
    session: requests.Session,
    base_url: str,
    *,
    product_id: str,
) -> list[tuple[int, str]]:
    rows: list[tuple[int, str]] = []
    cursor: str | None = None

    while True:
        params: list[tuple[str, str]] = [
            ("productId", product_id),
            ("range", "MONTH"),
            ("order", "asc"),
            ("orderBy", "createdAt"),
            ("limit", str(PAGE_LIMIT)),
        ]
        if cursor:
            params.append(("cursor", cursor))

        payload = eth_get(session, base_url, FUNDING_PATH, params=params)
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, list):
            raise RuntimeError("Ethereal /v1/funding 返回格式异常")

        for item in data:
            if not isinstance(item, dict):
                continue
            funding_time = normalize_ts_to_ms(item.get("createdAt"))
            funding_rate = to_plain_str(item.get("fundingRate1h"))
            if funding_time is None or funding_rate is None:
                continue
            rows.append((funding_time, funding_rate))

        if not payload.get("hasNext"):
            break
        next_cursor = payload.get("nextCursor")
        if not isinstance(next_cursor, str) or not next_cursor:
            break
        cursor = next_cursor

    return rows


def main() -> None:
    now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - DAYS_TO_KEEP * 24 * 60 * 60 * 1000

    with sqlite3.connect(DB_PATH) as conn, requests.Session() as session:
        session.trust_env = False
        ensure_history_table(conn, HISTORY_TABLE)
        symbols = load_symbols(conn, INFO_TABLE)
        base_url = resolve_base_url(session)
        collector_log_start("Ethereal", "history", detail=f"使用 API {base_url}")

        products_by_symbol = fetch_products_by_symbol(session, base_url)
        collector_log_progress("Ethereal", "history", detail=f"{len(symbols)} 个交易对待同步")

        inserted = 0
        for idx, symbol in enumerate(symbols, 1):
            item = products_by_symbol.get(symbol)
            if not item:
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} 未匹配到 product，跳过")
                continue

            product_id = item.get("id")
            if not isinstance(product_id, str) or not product_id:
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} product id 为空，跳过")
                continue

            funding_rows = fetch_funding_rows(session, base_url, product_id=product_id)
            if not funding_rows:
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} 未返回 funding history，跳过")
                continue

            conn.executemany(
                f"""
                INSERT INTO {HISTORY_TABLE} (symbol, fundingTime, fundingRate, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(symbol, fundingTime) DO UPDATE SET
                    fundingRate=excluded.fundingRate,
                    updated_at=excluded.updated_at
                """,
                [(symbol, funding_time, funding_rate, now_ms) for funding_time, funding_rate in funding_rows],
            )
            delete_older_than(conn, HISTORY_TABLE, cutoff_ms, [symbol])
            conn.commit()
            inserted += len(funding_rows)
            collector_log_progress(
                "Ethereal",
                "history",
                detail=f"{symbol} 入库 {len(funding_rows)} 条",
                current=idx,
                total=len(symbols),
            )

        if inserted == 0:
            raise RuntimeError("Ethereal history 未写入任何记录")

    collector_log_end("Ethereal", "history", detail=f"累计写入 {inserted} 条")


if __name__ == "__main__":
    main()
