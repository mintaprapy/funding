#!/usr/bin/env python3

"""
从 Bybit v5 获取 USDT 永续（Linear）合约基础信息，并写入 SQLite。

数据落库到共享 funding.db 的 bybit_funding_baseinfo 表：
- symbol
- markPrice（标记价格）
- lastFundingRate（最新资金费率）
- fundingIntervalHours（结算周期小时，缺省按 8h）
- adjustedFundingRateCap / adjustedFundingRateFloor（上/下限，若接口不可得则为空）
- openInterest（未平仓价值，USDT；优先使用 tickers.openInterestValue）
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

BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com").rstrip("/")
INSTRUMENTS_INFO_PATH = "/v5/market/instruments-info"
TICKERS_PATH = "/v5/market/tickers"
INSURANCE_PATH = "/v5/market/insurance"
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
TABLE_NAME = "bybit_funding_baseinfo"
CATEGORY = "linear"
DEFAULT_FUNDING_INTERVAL_HOURS = 8


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


def bybit_get(
    session: requests.Session,
    path: str,
    *,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    url = f"{BASE_URL}{path}"
    resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"{path} 返回格式异常（非 dict）")
    if data.get("retCode") != 0:
        raise RuntimeError(f"{path} retCode={data.get('retCode')} retMsg={data.get('retMsg')}")
    result = data.get("result")
    if not isinstance(result, dict):
        raise RuntimeError(f"{path} result 返回格式异常（非 dict）")
    return result


def _split_symbols_field(val: Any) -> list[str]:
    if val is None:
        return []
    if isinstance(val, list):
        return [s for s in val if isinstance(s, str) and s]
    if isinstance(val, str):
        return [s.strip() for s in val.split(",") if s.strip()]
    return []


def fetch_insurance_by_symbol(session: requests.Session) -> dict[str, str]:
    """从 Bybit 公共接口获取风险保障基金，并映射到 symbols。"""
    candidates = [
        {"category": CATEGORY, "coin": "USDT"},
        {"category": CATEGORY},
        {"coin": "USDT"},
        None,
    ]
    last_exc: Exception | None = None
    for params in candidates:
        try:
            result = bybit_get(session, INSURANCE_PATH, params=params)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            continue

        items = result.get("list", [])
        if not isinstance(items, list):
            continue
        out: dict[str, str] = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            symbols_field = item.get("symbols") or item.get("symbol")
            symbols = _split_symbols_field(symbols_field)
            if not symbols:
                continue

            raw = item.get("value")
            if raw is None:
                raw = item.get("balance") or item.get("marginBalance")
            val = to_plain_str(raw)
            if val is None:
                continue
            for sym in symbols:
                out[sym] = val
        return out
    if last_exc is not None:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] 保险基金余额获取失败：{last_exc}")
    return {}


def fetch_instruments(session: requests.Session) -> dict[str, dict[str, Any]]:
    instruments: dict[str, dict[str, Any]] = {}
    cursor: str | None = None
    use_status_param = True
    while True:
        params: dict[str, Any] = {"category": CATEGORY, "limit": 1000}
        if use_status_param:
            params["status"] = "Trading"
        if cursor:
            params["cursor"] = cursor
        try:
            result = bybit_get(session, INSTRUMENTS_INFO_PATH, params=params)
        except RuntimeError:
            # 部分节点不支持 status 参数，fallback 一次
            if use_status_param and not cursor:
                use_status_param = False
                continue
            raise
        items = result.get("list", [])
        if not isinstance(items, list):
            raise RuntimeError("instruments-info list 返回格式异常（非 list）")
        for item in items:
            if not isinstance(item, dict):
                continue
            symbol = item.get("symbol")
            if not symbol or not isinstance(symbol, str):
                continue
            quote = (item.get("quoteCoin") or item.get("quoteCurrency") or "").upper()
            status = (item.get("status") or "").lower()
            if quote != "USDT":
                continue
            if status and status not in ("trading", "trade"):
                continue
            contract_type = str(item.get("contractType") or item.get("contractTypeName") or "").lower()
            if contract_type and "perpetual" not in contract_type:
                continue
            if "-" in symbol:
                # 交割/季度等合约通常带有日期后缀，资金费率不适用
                continue
            # 若无 status 字段（或为空），仍按 USDT 线性合约纳入
            instruments[symbol] = item

        cursor_val = result.get("nextPageCursor")
        cursor = cursor_val if isinstance(cursor_val, str) and cursor_val else None
        if not cursor:
            break
    if not instruments:
        raise RuntimeError("未获取到符合条件的 Bybit USDT Linear 交易对")
    return instruments


def fetch_tickers(session: requests.Session) -> dict[str, dict[str, Any]]:
    result = bybit_get(session, TICKERS_PATH, params={"category": CATEGORY})
    items = result.get("list", [])
    if not isinstance(items, list):
        raise RuntimeError("tickers list 返回格式异常（非 list）")
    out: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        symbol = item.get("symbol")
        if isinstance(symbol, str) and symbol:
            out[symbol] = item
    return out


def extract_interval_hours(instrument: dict[str, Any]) -> int:
    candidates = [
        instrument.get("fundingInterval"),
        instrument.get("fundingRateInterval"),
        instrument.get("fundingIntervalHours"),
    ]
    for val in candidates:
        if val is None:
            continue
        if isinstance(val, (int, float)):
            # 有的字段单位是分钟（例如 480），有的字段可能直接是小时（8）
            v = float(val)
            if v >= 60:
                return max(1, int(round(v / 60)))
            return max(1, int(round(v)))
        if isinstance(val, str):
            s = val.strip().lower()
            if s.endswith("h"):
                s = s[:-1].strip()
            try:
                v = float(s)
            except ValueError:
                continue
            if v >= 60:
                return max(1, int(round(v / 60)))
            return max(1, int(round(v)))
    return DEFAULT_FUNDING_INTERVAL_HOURS


def extract_cap_floor(instrument: dict[str, Any]) -> tuple[str | None, str | None]:
    cap = (
        instrument.get("fundingRateUpperLimit")
        or instrument.get("upperFundingRate")
        or instrument.get("fundingRateUpperBound")
    )
    floor = (
        instrument.get("fundingRateLowerLimit")
        or instrument.get("lowerFundingRate")
        or instrument.get("fundingRateLowerBound")
    )
    cap_s = to_plain_str(cap) if cap is not None else None
    floor_s = to_plain_str(floor) if floor is not None else None
    if cap_s is None and floor_s is None:
        clamp = instrument.get("fundingRateClamp") or instrument.get("fundingRateClampRatio")
        clamp_s = to_plain_str(clamp) if clamp is not None else None
        if clamp_s is not None:
            cap_s = clamp_s
            try:
                floor_s = to_plain_str(-Decimal(clamp_s))
            except (InvalidOperation, TypeError, ValueError):
                floor_s = None
    return cap_s, floor_s


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
            volume24h TEXT,
            turnover24h TEXT,
            updated_at INTEGER
        )
        """
    )
    ensure_column(conn, TABLE_NAME, "insuranceBalance", "TEXT")
    ensure_column(conn, TABLE_NAME, "volume24h", "TEXT")
    ensure_column(conn, TABLE_NAME, "turnover24h", "TEXT")
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
        ensure_table(conn)

        instruments = fetch_instruments(session)
        symbols = sorted(instruments.keys())
        collector_log_start("Bybit", "base", detail=f"{len(symbols)} 个 USDT Linear 交易对")

        tickers = fetch_tickers(session)
        collector_log_progress("Bybit", "base", detail="获取 tickers（markPrice / fundingRate / openInterest）")

        insurance_by_symbol: dict[str, str] = {}
        try:
            insurance_by_symbol = fetch_insurance_by_symbol(session)
            if insurance_by_symbol:
                collector_log_progress("Bybit", "base", detail=f"获取风险保障基金（覆盖 {len(insurance_by_symbol)} 个交易对）")
        except Exception as exc:  # noqa: BLE001
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] 风险保障基金解析失败：{exc}")

        rows: list[tuple[Any, ...]] = []
        missing = 0
        for sym in symbols:
            inst = instruments.get(sym, {})
            ticker = tickers.get(sym)
            if not ticker:
                missing += 1
                continue

            interval_hours = extract_interval_hours(inst)
            if ticker.get("fundingIntervalHour") is not None:
                try:
                    interval_hours = max(1, int(float(ticker.get("fundingIntervalHour"))))
                except (TypeError, ValueError):
                    pass

            cap_s, floor_s = extract_cap_floor(inst)
            if cap_s is None and floor_s is None and ticker.get("fundingCap") is not None:
                cap_s = to_plain_str(ticker.get("fundingCap"))
                if cap_s is not None:
                    try:
                        floor_s = to_plain_str(-Decimal(cap_s))
                    except (InvalidOperation, TypeError, ValueError):
                        floor_s = None

            mark_price = to_plain_str(ticker.get("markPrice"))
            last_funding_rate = to_plain_str(ticker.get("fundingRate"))

            # 优先使用 openInterestValue（USDT 价值）；若无则回退 openInterest * markPrice
            oi_value = to_plain_str(ticker.get("openInterestValue"))
            if oi_value is None:
                oi_raw = to_plain_str(ticker.get("openInterest"))
                if oi_raw is not None and mark_price is not None:
                    try:
                        oi_value = to_plain_str(Decimal(oi_raw) * Decimal(mark_price))
                    except (InvalidOperation, TypeError, ValueError):
                        oi_value = None

            rows.append(
                (
                    sym,
                    cap_s,
                    floor_s,
                    interval_hours,
                    mark_price,
                    last_funding_rate,
                    oi_value,
                    insurance_by_symbol.get(sym),
                    to_plain_str(ticker.get("volume24h")),
                    to_plain_str(ticker.get("turnover24h")),
                    now_ms,
                )
            )

        if not rows:
            raise RuntimeError("未获取到任何可写入的交易对记录（tickers 可能为空或字段变更）")

        existing = fetch_existing_symbols(conn)
        current = {r[0] for r in rows}

        deleted: list[str] = []
        if missing > 0:
            print(
                f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] 本轮 tickers 缺失 {missing} 条，"
                "为避免误删，跳过下架清理"
            )
        else:
            deleted = delete_obsolete_symbols(conn, existing - current)
            if deleted:
                collector_log_progress("Bybit", "base", detail=f"删除已下架交易对 {len(deleted)} 个")

        rows = stamp_rows_updated_at(rows)
        save_records(conn, rows)
        collector_log_end("Bybit", "base", detail=f"入库 {len(rows)} 条，tickers 缺失 {missing} 条")


if __name__ == "__main__":
    main()
