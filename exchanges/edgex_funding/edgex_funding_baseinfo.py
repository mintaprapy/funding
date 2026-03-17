#!/usr/bin/env python3
"""Fetch edgeX perpetual funding base info into SQLite."""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
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
_REQUESTS_SESSION: requests.Session | None = None


def request_headers() -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
        "Referer": f"{BASE_URL}/trade/BTCUSDT",
        "Origin": BASE_URL,
    }


def requests_session() -> requests.Session:
    global _REQUESTS_SESSION
    if _REQUESTS_SESSION is None:
        _REQUESTS_SESSION = requests.Session()
        _REQUESTS_SESSION.trust_env = False
    return _REQUESTS_SESSION


def curl_get_json(url: str) -> Any:
    curl_bin = shutil.which("curl")
    if not curl_bin:
        raise RuntimeError("curl binary not found")
    cmd = [curl_bin, "-sS", "--compressed", "--max-time", str(REQUEST_TIMEOUT)]
    for key, value in request_headers().items():
        cmd.extend(["-H", f"{key}: {value}"])
    cmd.append(url)
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"curl exit {proc.returncode}: {proc.stderr.strip()}")
    return json.loads(proc.stdout)


def requests_get_json(url: str) -> Any:
    resp = requests_session().get(url, headers=request_headers(), timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def edgex_get(path: str, *, params: dict[str, Any] | None = None) -> Any:
    url = f"{BASE_URL}{path}"
    if params:
        from urllib.parse import urlencode

        url = f"{url}?{urlencode(params)}"
    last_exc: Exception | None = None
    for attempt in range(1, MAX_HTTP_ATTEMPTS + 1):
        try:
            try:
                data = curl_get_json(url)
            except Exception as curl_exc:  # noqa: BLE001
                try:
                    data = requests_get_json(url)
                except Exception as requests_exc:  # noqa: BLE001
                    raise RuntimeError(f"curl={curl_exc}; requests={requests_exc}") from requests_exc
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
        collector_log_start("edgeX", "base", detail=f"{len(contracts)} 个交易对")

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
                collector_log_progress("edgeX", "base", detail="已获取", current=idx, total=len(contracts))

        if not rows:
            raise RuntimeError("未获取到任何可写入的 edgeX 交易对记录")

        existing = fetch_existing_symbols(conn, TABLE_NAME)
        current = {row[0] for row in rows}
        if missing > 0:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] 本轮 ticker 缺失 {missing} 条，为避免误删，跳过下架清理")
        else:
            deleted = delete_obsolete_symbols(conn, TABLE_NAME, existing - current)
            if deleted:
                collector_log_progress("edgeX", "base", detail=f"删除已下架交易对 {len(deleted)} 个")

        save_records(conn, rows)
        collector_log_end("edgeX", "base", detail=f"入库 {len(rows)} 条到 {TABLE_NAME}")


if __name__ == "__main__":
    main()
