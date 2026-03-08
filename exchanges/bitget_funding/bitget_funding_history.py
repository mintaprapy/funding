#!/usr/bin/env python3
"""Fetch Bitget USDT perpetual funding history into SQLite."""

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
    delete_older_than,
    ensure_history_table,
    load_symbols,
    normalize_ts_to_ms,
    to_plain_str,
)

BASE_URL = os.getenv("BITGET_BASE_URL", "https://api.bitget.com").rstrip("/")
FUNDING_HISTORY_PATH = "/api/v2/mix/market/history-fund-rate"
REQUEST_TIMEOUT = 15

PRODUCT_TYPE = os.getenv("BITGET_PRODUCT_TYPE", "USDT-FUTURES")

DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT_DIR / "funding.db")).expanduser().resolve()
INFO_TABLE = "bitget_funding_baseinfo"
HISTORY_TABLE = "bitget_funding_history"

DAYS_TO_FETCH = 30
DAYS_TO_KEEP = 60
MAX_PAGES = 15
PAGE_SIZE = 200


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


def fetch_symbol_history(session: requests.Session, symbol: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    # 分页拉取，兼容部分节点 pageNo / pageSize 生效的实现
    for page_no in range(1, MAX_PAGES + 1):
        payload = bitget_get(
            session,
            FUNDING_HISTORY_PATH,
            params={
                "symbol": symbol,
                "productType": PRODUCT_TYPE,
                "pageNo": page_no,
                "pageSize": PAGE_SIZE,
            },
        )
        items = payload.get("data")
        if not isinstance(items, list) or not items:
            break
        out.extend(item for item in items if isinstance(item, dict))
        if len(items) < PAGE_SIZE:
            break
        time.sleep(0.05)

    out.sort(key=lambda x: normalize_ts_to_ms(x.get("fundingTime") or x.get("timestamp")) or 0)
    return out


def save_history(
    conn: sqlite3.Connection,
    symbol: str,
    records: list[dict[str, Any]],
    *,
    now_ms: int,
    start_ms: int,
    end_ms: int,
) -> int:
    rows: list[tuple[Any, ...]] = []
    for item in records:
        funding_time = normalize_ts_to_ms(item.get("fundingTime") or item.get("timestamp"))
        if funding_time is None or funding_time < start_ms or funding_time > end_ms:
            continue
        rows.append(
            (
                symbol,
                funding_time,
                to_plain_str(item.get("fundingRate") or item.get("fundRate") or item.get("rate")),
                now_ms,
            )
        )

    if not rows:
        return 0

    conn.executemany(
        f"""
        INSERT INTO {HISTORY_TABLE} (symbol, fundingTime, fundingRate, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(symbol, fundingTime) DO UPDATE SET
            fundingRate=excluded.fundingRate,
            updated_at=excluded.updated_at
        """,
        rows,
    )
    return len(rows)


def main() -> None:
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - DAYS_TO_FETCH * 24 * 60 * 60 * 1000
    cutoff_ms = end_ms - DAYS_TO_KEEP * 24 * 60 * 60 * 1000

    with sqlite3.connect(DB_PATH) as conn, requests.Session() as session:
        ensure_history_table(conn, HISTORY_TABLE)
        symbols = load_symbols(conn, INFO_TABLE)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}]共 {len(symbols)} 个交易对，开始拉取近 {DAYS_TO_FETCH} 天资金费率")

        for idx, symbol in enumerate(symbols, 1):
            try:
                records = fetch_symbol_history(session, symbol)
            except Exception as exc:  # noqa: BLE001
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} 获取失败：{exc}")
                continue

            inserted = save_history(
                conn,
                symbol,
                records,
                now_ms=end_ms,
                start_ms=start_ms,
                end_ms=end_ms,
            )
            delete_older_than(conn, HISTORY_TABLE, cutoff_ms, [symbol])
            conn.commit()
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][{idx}/{len(symbols)}] {symbol} 入库 {inserted} 条")

    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}]同步完成")


if __name__ == "__main__":
    main()
