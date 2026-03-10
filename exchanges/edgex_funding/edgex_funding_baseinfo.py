#!/usr/bin/env python3
"""Fetch edgeX perpetual funding base info into SQLite."""

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
    delete_obsolete_symbols,
    ensure_baseinfo_table,
    fetch_existing_symbols,
    to_plain_str,
)

BASE_URL = os.getenv("EDGEX_BASE_URL", "https://pro.edgex.exchange").rstrip("/")
METADATA_PATH = "/api/v1/public/meta/getMetaData"
TICKER_PATH = "/api/v1/public/quote/getTicker"
REQUEST_TIMEOUT = 20
MAX_HTTP_ATTEMPTS = 4

DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT_DIR / "funding.db")).expanduser().resolve()
TABLE_NAME = "edgex_funding_baseinfo"

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


def fetch_contracts() -> list[dict[str, Any]]:
    data = edgex_get(METADATA_PATH)
    if not isinstance(data, dict):
        raise RuntimeError("edgeX metadata 返回格式异常（非 dict）")
    items = data.get("contractList")
    if not isinstance(items, list):
        raise RuntimeError("edgeX metadata 返回格式异常（缺少 contractList）")

    out: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        symbol = item.get("contractName")
        contract_id = item.get("contractId")
        if not isinstance(symbol, str) or not symbol:
            continue
        if not isinstance(contract_id, str) or not contract_id:
            continue
        if not item.get("enableDisplay"):
            continue
        if not item.get("enableTrade"):
            continue
        out.append(item)

    if not out:
        raise RuntimeError("未获取到 edgeX 永续交易对")
    return sorted(out, key=lambda x: str(x.get("contractName") or ""))


def fetch_ticker(limiter: RateLimiter, contract_id: str) -> dict[str, Any] | None:
    limiter.acquire()
    data = edgex_get(TICKER_PATH, params={"contractId": contract_id})
    if not isinstance(data, list):
        raise RuntimeError(f"getTicker({contract_id}) 返回格式异常（非 list）")
    if not data:
        return None
    item = data[0]
    if not isinstance(item, dict):
        return None
    return item


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

    with sqlite3.connect(DB_PATH) as conn:
        ensure_baseinfo_table(conn, TABLE_NAME)
        contracts = fetch_contracts()
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 获取 {len(contracts)} 个交易对（edgeX）")

        rows: list[tuple[Any, ...]] = []
        missing = 0
        for idx, contract in enumerate(contracts, 1):
            symbol = str(contract["contractName"])
            contract_id = str(contract["contractId"])
            try:
                ticker = fetch_ticker(limiter, contract_id)
            except Exception as exc:  # noqa: BLE001
                missing += 1
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} ticker 获取失败：{exc}")
                continue
            if not ticker:
                missing += 1
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} ticker 为空")
                continue

            interval_hours = 8
            interval_min = contract.get("fundingRateIntervalMin")
            try:
                interval_hours = max(1, int(round(float(interval_min) / 60)))
            except (TypeError, ValueError):
                pass

            rows.append(
                (
                    symbol,
                    to_plain_str(contract.get("fundingMaxRate")),
                    to_plain_str(contract.get("fundingMinRate")),
                    interval_hours,
                    to_plain_str(ticker.get("markPrice") or ticker.get("lastPrice") or ticker.get("indexPrice")),
                    to_plain_str(ticker.get("fundingRate")),
                    to_plain_str(ticker.get("openInterest")),
                    None,
                    now_ms,
                )
            )
            if idx % 50 == 0:
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][{idx}/{len(contracts)}] 已获取")

        if not rows:
            raise RuntimeError("未获取到任何可写入的 edgeX 交易对记录")

        existing = fetch_existing_symbols(conn, TABLE_NAME)
        current = {row[0] for row in rows}
        if missing > 0:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] 本轮 ticker 缺失 {missing} 条，为避免误删，跳过下架清理")
        else:
            deleted = delete_obsolete_symbols(conn, TABLE_NAME, existing - current)
            if deleted:
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 删除已下架交易对 {len(deleted)} 个")

        save_records(conn, rows)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 入库 {len(rows)} 条到 {TABLE_NAME}")


if __name__ == "__main__":
    main()
