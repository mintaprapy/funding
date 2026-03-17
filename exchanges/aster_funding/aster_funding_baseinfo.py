#!/usr/bin/env python3

"""
从 Aster 获取：处于交易状态的 USDT 永续合约交易对与基础信息，并写入 SQLite。

说明：
- 该脚本按「Binance Futures 兼容接口」实现（/fapi/v1/...）。
- 请先确认 Aster 的合约 API 是否与 Binance Futures 兼容，并通过环境变量设置基地址：
  export ASTER_BASE_URL="https://<your-aster-futures-host>"

数据落库到共享 funding.db 的 aster_funding_baseinfo 表：
- symbol
- adjustedFundingRateCap / adjustedFundingRateFloor
- fundingIntervalHours
- markPrice / lastFundingRate
- openInterest
"""

from __future__ import annotations

import os
import sqlite3
import sys
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import requests

DEFAULT_BASE_URL = "https://fapi.asterdex.com"
BASE_URL = (os.getenv("ASTER_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")
EXCHANGE_INFO_PATH = "/fapi/v1/exchangeInfo"
FUNDING_INFO_PATH = "/fapi/v1/fundingInfo"
PREMIUM_INDEX_PATH = "/fapi/v1/premiumIndex"
OPEN_INTEREST_PATH = "/fapi/v1/openInterest"
INSURANCE_BALANCE_PATH = "/fapi/v1/insuranceBalance"
REQUEST_TIMEOUT = 15

ROOT_DIR = next(parent for parent in Path(__file__).resolve().parents if (parent / "start_all_funding.sh").exists())
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from core.common_funding import (
    collector_log_end,
    collector_log_progress,
    collector_log_start,
    stamp_rows_updated_at,
)

DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT_DIR / "funding.db")).expanduser().resolve()
TABLE_NAME = "aster_funding_baseinfo"


def tune_sqlite_connection(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA busy_timeout=15000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")


def to_plain_str(val: Any) -> str | None:
    try:
        dec = Decimal(str(val))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return format(dec.normalize(), "f")


def ensure_column(conn: sqlite3.Connection, table: str, column: str, column_type: str) -> None:
    cur = conn.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cur.fetchall()}
    if column in existing:
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")


def fetch_json(
    url: str,
    *,
    params: dict[str, Any] | None = None,
    session: requests.Session | None = None,
) -> Any:
    requester = session or requests
    resp = requester.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def require_base_url() -> str:
    if not BASE_URL:
        raise RuntimeError("请设置 ASTER_BASE_URL（Aster 合约 API 基地址，例如 https://fapi.asterdex.com ）")
    return BASE_URL


def get_trading_usdt_perpetual_symbols() -> list[str]:
    base = require_base_url()
    data = fetch_json(f"{base}{EXCHANGE_INFO_PATH}")
    symbols: list[str] = []
    for item in data.get("symbols", []):
        if (
            item.get("status") == "TRADING"
            and (item.get("quoteAsset") or "").upper() == "USDT"
            and str(item.get("contractType") or "").upper() in ("PERPETUAL", "TRADIFI_PERPETUAL")
        ):
            sym = item.get("symbol")
            if isinstance(sym, str) and sym:
                symbols.append(sym)
    symbols.sort()
    if not symbols:
        raise RuntimeError("未获取到符合条件的交易对（TRADING / USDT / PERPETUAL）")
    return symbols


def get_funding_info() -> dict[str, dict[str, Any]]:
    base = require_base_url()
    data = fetch_json(f"{base}{FUNDING_INFO_PATH}")
    if not isinstance(data, list):
        raise RuntimeError("fundingInfo 返回格式异常")
    out: dict[str, dict[str, Any]] = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        symbol = item.get("symbol")
        if isinstance(symbol, str) and symbol:
            out[symbol] = item
    return out


def get_premium_index() -> dict[str, dict[str, Any]]:
    base = require_base_url()
    data = fetch_json(f"{base}{PREMIUM_INDEX_PATH}")
    if not isinstance(data, list):
        raise RuntimeError("premiumIndex 返回格式异常")
    out: dict[str, dict[str, Any]] = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        symbol = item.get("symbol")
        if isinstance(symbol, str) and symbol:
            out[symbol] = item
    return out


def get_open_interest(symbols: list[str]) -> dict[str, Any]:
    base = require_base_url()
    open_interest: dict[str, Any] = {}
    if not symbols:
        return open_interest
    with requests.Session() as session:
        for sym in symbols:
            try:
                data = fetch_json(
                    f"{base}{OPEN_INTEREST_PATH}",
                    params={"symbol": sym},
                    session=session,
                )
            except Exception as exc:  # noqa: BLE001
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {sym} openInterest 获取失败：{exc}")
                continue
            if not isinstance(data, dict):
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {sym} openInterest 返回格式异常")
                continue
            interest = data.get("openInterest")
            if interest is not None:
                open_interest[sym] = interest
    return open_interest


def get_insurance_balance() -> list[dict[str, Any]]:
    """获取风险保障基金余额（若交易所支持 /fapi/v1/insuranceBalance）。"""
    base = require_base_url()
    try:
        data = fetch_json(f"{base}{INSURANCE_BALANCE_PATH}")
    except requests.HTTPError as exc:
        status = getattr(exc.response, "status_code", None)
        # 部分 Binance 兼容交易所未实现该可选接口。
        if status in (404, 405, 501):
            return []
        raise
    if not isinstance(data, list):
        raise RuntimeError("insuranceBalance 返回格式异常")
    out: list[dict[str, Any]] = []
    for item in data:
        if isinstance(item, dict):
            out.append(item)
    return out


def ensure_table(conn: sqlite3.Connection) -> None:
    tune_sqlite_connection(conn)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            symbol TEXT PRIMARY KEY,
            adjustedFundingRateCap TEXT,
            adjustedFundingRateFloor TEXT,
            fundingIntervalHours INTEGER,
            markPrice TEXT,
            lastFundingRate TEXT,
            openInterest TEXT,
            insuranceBalance TEXT,
            updated_at INTEGER
        )
        """
    )
    ensure_column(conn, TABLE_NAME, "insuranceBalance", "TEXT")
    conn.commit()


def fetch_existing_symbols(conn: sqlite3.Connection) -> set[str]:
    cur = conn.execute(f"SELECT symbol FROM {TABLE_NAME}")
    return {row[0] for row in cur.fetchall()}


def delete_obsolete_symbols(conn: sqlite3.Connection, symbols: set[str]) -> list[str]:
    if not symbols:
        return []
    placeholders = ",".join("?" for _ in symbols)
    conn.execute(f"DELETE FROM {TABLE_NAME} WHERE symbol IN ({placeholders})", tuple(symbols))
    return sorted(symbols)


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
    symbols = get_trading_usdt_perpetual_symbols()
    collector_log_start("Aster", "base", detail=f"{len(symbols)} 个交易对")

    funding_info = get_funding_info()
    collector_log_progress("Aster", "base", detail="获取资金费上下限、结算周期")

    premium_index = get_premium_index()
    collector_log_progress("Aster", "base", detail="获取标记价格、最新资金费率")

    open_interest = get_open_interest(symbols)
    collector_log_progress("Aster", "base", detail="获取未平仓合约数")

    try:
        insurance_items = get_insurance_balance()
        if insurance_items:
            collector_log_progress("Aster", "base", detail=f"获取风险保障基金（{len(insurance_items)} 条）")
        else:
            collector_log_progress("Aster", "base", detail="风险保障基金接口未提供或无数据，已跳过")
    except Exception as exc:  # noqa: BLE001
        insurance_items = []
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] 风险保障基金获取失败：{exc}")

    now_ms = int(time.time() * 1000)
    rows: list[tuple[Any, ...]] = []

    insurance_by_symbol: dict[str, Decimal] = {}
    if insurance_items:
        for group in insurance_items:
            symbols_group = group.get("symbols")
            assets_group = group.get("assets")
            if isinstance(symbols_group, list) and isinstance(assets_group, list):
                total = Decimal("0")
                have_value = False
                for asset_item in assets_group:
                    if not isinstance(asset_item, dict):
                        continue
                    mb = asset_item.get("marginBalance")
                    if mb is None:
                        mb = asset_item.get("balance")
                    if mb is None:
                        continue
                    try:
                        total += Decimal(str(mb))
                        have_value = True
                    except (InvalidOperation, TypeError, ValueError):
                        continue
                if not have_value:
                    continue
                for sym in symbols_group:
                    if isinstance(sym, str) and sym:
                        insurance_by_symbol[sym] = insurance_by_symbol.get(sym, Decimal("0")) + total
                continue

            sym_single = group.get("symbol")
            if isinstance(sym_single, str) and sym_single:
                mb = group.get("marginBalance")
                if mb is None:
                    mb = group.get("balance")
                if mb is not None:
                    try:
                        insurance_by_symbol[sym_single] = insurance_by_symbol.get(sym_single, Decimal("0")) + Decimal(
                            str(mb)
                        )
                    except (InvalidOperation, TypeError, ValueError):
                        pass

    for sym in symbols:
        entry = funding_info.get(sym) or {}
        premium = premium_index.get(sym) or {}
        rows.append(
            (
                sym,
                to_plain_str(entry.get("adjustedFundingRateCap") or entry.get("fundingRateCap")),
                to_plain_str(entry.get("adjustedFundingRateFloor") or entry.get("fundingRateFloor")),
                entry.get("fundingIntervalHours") or entry.get("fundingInterval") or 8,
                to_plain_str(premium.get("markPrice")),
                to_plain_str(premium.get("lastFundingRate")),
                to_plain_str(open_interest.get(sym)),
                to_plain_str(insurance_by_symbol.get(sym)),
                now_ms,
            )
        )

    with sqlite3.connect(DB_PATH) as conn:
        ensure_table(conn)
        existing_symbols = fetch_existing_symbols(conn)
        current_symbols = {row[0] for row in rows}

        removed = delete_obsolete_symbols(conn, existing_symbols - current_symbols)
        added = sorted(current_symbols - existing_symbols)

        rows = stamp_rows_updated_at(rows)
        save_records(conn, rows)

    if removed:
        collector_log_progress("Aster", "base", detail=f"删除下架交易对：{', '.join(removed)}")
    if added:
        collector_log_progress("Aster", "base", detail=f"新增交易对：{', '.join(added)}")
    collector_log_end("Aster", "base", detail=f"更新 {len(rows)} 条记录到 {TABLE_NAME}")


if __name__ == "__main__":
    main()
