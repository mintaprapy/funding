#!/usr/bin/env python3
"""Fetch edgeX perpetual funding history into SQLite."""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

ROOT_DIR = next(parent for parent in Path(__file__).resolve().parents if (parent / "start_all_funding.sh").exists())
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from core.common_funding import (
    RateLimiter,
    delete_older_than,
    ensure_history_table,
    load_symbols,
    normalize_ts_to_ms,
    to_plain_str,
)

BASE_URL = os.getenv("EDGEX_BASE_URL", "https://pro.edgex.exchange").rstrip("/")
METADATA_PATH = "/api/v1/public/meta/getMetaData"
FUNDING_HISTORY_PATH = "/api/v1/public/funding/getFundingRatePage"
REQUEST_TIMEOUT = 20
MAX_HTTP_ATTEMPTS = 4

DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT_DIR / "funding.db")).expanduser().resolve()
INFO_TABLE = "edgex_funding_baseinfo"
HISTORY_TABLE = "edgex_funding_history"

DAYS_TO_FETCH = 30
DAYS_TO_KEEP = 60
PAGE_SIZE = 100
WINDOW_SECONDS = 60
WINDOW_CAPACITY = 240


def curl_headers() -> tuple[str, ...]:
    return (
        "User-Agent: Mozilla/5.0",
        "Accept: application/json,text/plain,*/*",
        f"Referer: {BASE_URL}/trade/BTCUSDT",
        f"Origin: {BASE_URL}",
    )


def edgex_get(path: str, *, params: dict[str, Any] | None = None) -> Any:
    url = f"{BASE_URL}{path}"
    if params:
        from urllib.parse import urlencode

        url = f"{url}?{urlencode(params)}"
    last_exc: Exception | None = None
    for attempt in range(1, MAX_HTTP_ATTEMPTS + 1):
        try:
            cmd = ["curl", "-sS", "--compressed", "--max-time", str(REQUEST_TIMEOUT)]
            for header in curl_headers():
                cmd.extend(["-H", header])
            cmd.extend([url])
            proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
            if proc.returncode != 0:
                raise RuntimeError(f"{path} curl exit {proc.returncode}: {proc.stderr.strip()}")
            import json

            data = json.loads(proc.stdout)
            if not isinstance(data, dict):
                raise RuntimeError(f"{path} 返回格式异常（非 dict）")
            if str(data.get("code") or "").upper() != "SUCCESS":
                raise RuntimeError(f"{path} code={data.get('code')} msg={data.get('msg')}")
            return data.get("data")
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt >= MAX_HTTP_ATTEMPTS:
                break
            time.sleep(min(1.0 * attempt, 3.0))
    raise RuntimeError(f"edgeX 请求失败: {path}, err={last_exc}") from last_exc


def fetch_contract_ids() -> dict[str, str]:
    data = edgex_get(METADATA_PATH)
    if not isinstance(data, dict):
        raise RuntimeError("edgeX metadata 返回格式异常（非 dict）")
    items = data.get("contractList")
    if not isinstance(items, list):
        raise RuntimeError("edgeX metadata 返回格式异常（缺少 contractList）")
    out: dict[str, str] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        if not item.get("enableDisplay") or not item.get("enableTrade"):
            continue
        symbol = item.get("contractName")
        contract_id = item.get("contractId")
        if isinstance(symbol, str) and symbol and isinstance(contract_id, str) and contract_id:
            out[symbol] = contract_id
    return out


def fetch_symbol_history(
    limiter: RateLimiter,
    contract_id: str,
    *,
    start_ms: int,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    offset_data: str | None = None

    while True:
        params: dict[str, Any] = {
            "contractId": contract_id,
            "size": PAGE_SIZE,
            "filterSettlementFundingRate": "true",
        }
        if offset_data:
            params["offsetData"] = offset_data

        limiter.acquire()
        payload = edgex_get(FUNDING_HISTORY_PATH, params=params)
        if not isinstance(payload, dict):
            raise RuntimeError(f"{contract_id} funding history 返回格式异常（非 dict）")
        items = payload.get("dataList")
        if not isinstance(items, list) or not items:
            break

        rows = [item for item in items if isinstance(item, dict)]
        out.extend(rows)

        last_ts = normalize_ts_to_ms(rows[-1].get("fundingTime") or rows[-1].get("fundingTimestamp")) if rows else None
        if last_ts is not None and last_ts < start_ms:
            break

        next_offset = payload.get("nextPageOffsetData")
        if not isinstance(next_offset, str) or not next_offset:
            break
        if next_offset == offset_data:
            break
        offset_data = next_offset

    out.sort(key=lambda x: normalize_ts_to_ms(x.get("fundingTime") or x.get("fundingTimestamp")) or 0)
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
        funding_time = normalize_ts_to_ms(item.get("fundingTime") or item.get("fundingTimestamp"))
        if funding_time is None or funding_time < start_ms or funding_time > end_ms:
            continue
        rows.append(
            (
                symbol,
                funding_time,
                to_plain_str(item.get("fundingRate")),
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
    limiter = RateLimiter(WINDOW_CAPACITY, WINDOW_SECONDS)

    with sqlite3.connect(DB_PATH) as conn:
        ensure_history_table(conn, HISTORY_TABLE)
        symbols = load_symbols(conn, INFO_TABLE)
        contract_ids = fetch_contract_ids()
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}]共 {len(symbols)} 个交易对，开始拉取近 {DAYS_TO_FETCH} 天资金费率")

        for idx, symbol in enumerate(symbols, 1):
            contract_id = contract_ids.get(symbol)
            if not contract_id:
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} 缺少 contractId，跳过")
                continue
            try:
                records = fetch_symbol_history(limiter, contract_id, start_ms=start_ms)
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
