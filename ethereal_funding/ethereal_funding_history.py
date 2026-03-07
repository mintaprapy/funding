#!/usr/bin/env python3
"""Sync Ethereal funding history snapshots into SQLite.

Ethereal public API exposes real-time funding fields on /v1/product. This script
captures the latest funding snapshot per symbol and appends/upserts by timestamp.
"""

from __future__ import annotations

import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

import requests

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from common_funding import (
    delete_older_than,
    ensure_history_table,
    load_symbols,
    normalize_ts_to_ms,
    to_plain_str,
)

PRODUCTS_PATH = "/v1/product"
REQUEST_TIMEOUT = 15

DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (Path(__file__).resolve().parent.parent / "funding.db")).expanduser().resolve()
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
    params: dict[str, Any] | None = None,
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


def pick_funding_time_ms(item: dict[str, Any], now_ms: int) -> int:
    ts = normalize_ts_to_ms(item.get("fundingUpdatedAt") or item.get("updatedAt"))
    if ts is not None:
        return ts
    return now_ms


def main() -> None:
    now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - DAYS_TO_KEEP * 24 * 60 * 60 * 1000

    with sqlite3.connect(DB_PATH) as conn, requests.Session() as session:
        session.trust_env = False
        ensure_history_table(conn, HISTORY_TABLE)
        symbols = load_symbols(conn, INFO_TABLE)
        base_url = resolve_base_url(session)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 使用 Ethereal API: {base_url}")

        products_by_symbol = fetch_products_by_symbol(session, base_url)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}]共 {len(symbols)} 个交易对，开始同步 Ethereal 最新资金费率快照")

        inserted = 0
        for idx, symbol in enumerate(symbols, 1):
            item = products_by_symbol.get(symbol)
            if not item:
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} 未匹配到 product，跳过")
                continue

            funding_rate = to_plain_str(item.get("fundingRate1h"))
            if funding_rate is None:
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} fundingRate1h 为空，跳过")
                continue

            funding_time = pick_funding_time_ms(item, now_ms)
            conn.execute(
                f"""
                INSERT INTO {HISTORY_TABLE} (symbol, fundingTime, fundingRate, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(symbol, fundingTime) DO UPDATE SET
                    fundingRate=excluded.fundingRate,
                    updated_at=excluded.updated_at
                """,
                (symbol, funding_time, funding_rate, now_ms),
            )
            delete_older_than(conn, HISTORY_TABLE, cutoff_ms, [symbol])
            conn.commit()
            inserted += 1
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][{idx}/{len(symbols)}] {symbol} 入库 1 条")

        if inserted == 0:
            raise RuntimeError("Ethereal history 未写入任何记录")

    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}]同步完成")


if __name__ == "__main__":
    main()
