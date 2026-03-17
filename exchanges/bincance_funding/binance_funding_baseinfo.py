#!/usr/bin/env python3

"""
从 exchangeInfo 获取：处于交易状态的 Binance USDT 永续合约交易对 【 1 次请求，权重 1 】
从 fundingInfo 获取： 资金费率上下限、结算周期【 1 次请求，权重 1 】
从 premiumIndex 获取：标记价格、最新的资金费率【 1 次请求，权重 10 】【还有指数价格】
根据获取的交易对，从 openInterest 获取：未平仓合约数【 每个交易对 1 次请求，权重 1 】
将以上获取的信息，写入 SQLite 数据库 funding.db 的 binance_funding_baseinfo 数据表 
"""

from __future__ import annotations

import os
import sqlite3
import sys
import time
from pathlib import Path
from decimal import Decimal, InvalidOperation
from typing import Any

import requests

BASE_URL = "https://fapi.binance.com"
EXCHANGE_INFO_PATH = "/fapi/v1/exchangeInfo"
FUNDING_INFO_PATH = "/fapi/v1/fundingInfo"
PREMIUM_INDEX_PATH = "/fapi/v1/premiumIndex"
OPEN_INTEREST_PATH = "/fapi/v1/openInterest"
INSURANCE_BALANCE_PATH = "/fapi/v1/insuranceBalance"
REQUEST_TIMEOUT = 15
MAX_HTTP_RETRIES = 6
RETRY_BASE_SLEEP = 1.0

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
TABLE_NAME = "binance_funding_baseinfo"


def tune_sqlite_connection(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA busy_timeout=15000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")


def to_plain_str(val: Any) -> str | None:
    """把科学计数法转成普通小数表示，保持字符串写入数据库。"""
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


def column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cur.fetchall())


def fetch_json(
    url: str,
    *,
    params: dict[str, Any] | None = None,
    session: requests.Session | None = None,
) -> Any:
    """统一封装 HTTP GET 请求（可复用 requests.Session），设置超时、raise_for_status()，并返回 json() 结果。"""
    requester = session or requests
    last_exc: Exception | None = None
    for attempt in range(1, MAX_HTTP_RETRIES + 1):
        try:
            resp = requester.get(url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt >= MAX_HTTP_RETRIES:
                break
            time.sleep(min(20.0, RETRY_BASE_SLEEP * attempt))
    raise RuntimeError(f"请求失败: {url}; last_error={last_exc}")


def get_trading_usdt_perpetual_symbols(*, session: requests.Session | None = None) -> list[str]:
    """从 exchangeInfo 获取 TRADING 状态的 USDT PERPETUAL 交易对，排序返回。"""
    data = fetch_json(f"{BASE_URL}{EXCHANGE_INFO_PATH}", session=session)
    symbols = []
    for item in data.get("symbols", []):
        if (
            item.get("status") == "TRADING"
            and item.get("quoteAsset") == "USDT"
            and item.get("contractType") in ("TRADIFI_PERPETUAL", "PERPETUAL")
        ):
            sym = item.get("symbol")
            if sym:
                symbols.append(sym)
    symbols.sort()
    if not symbols:
        raise RuntimeError("未获取到符合条件的交易对")
    return symbols


def get_funding_info(*, session: requests.Session | None = None) -> dict[str, dict[str, Any]]:
    """从 fundingInfo 获取所有交易对的资金费率上限/下限、结算周期。"""
    data = fetch_json(f"{BASE_URL}{FUNDING_INFO_PATH}", session=session)
    if not isinstance(data, list):
        raise RuntimeError("fundingInfo 返回格式异常")
    funding_info = {}
    for item in data:
        symbol = item.get("symbol")
        if not symbol:
            continue
        funding_info[symbol] = item
    return funding_info


def get_premium_index(*, session: requests.Session | None = None) -> dict[str, dict[str, Any]]:
    """从 premiumIndex 获取标记价格和最近资金费率。"""
    data = fetch_json(f"{BASE_URL}{PREMIUM_INDEX_PATH}", session=session)
    if not isinstance(data, list):
        raise RuntimeError("premiumIndex 返回格式异常")
    premium_index = {}
    for item in data:
        symbol = item.get("symbol")
        if not symbol:
            continue
        premium_index[symbol] = item
    return premium_index


def get_open_interest(symbols: list[str]) -> dict[str, Any]:
    """获取每个交易对未平仓合约数。"""
    open_interest: dict[str, Any] = {}
    if not symbols:
        return open_interest
    with requests.Session() as session:
        session.trust_env = False
        for sym in symbols:
            try:
                data = fetch_json(
                    f"{BASE_URL}{OPEN_INTEREST_PATH}",
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


def get_insurance_balance(*, session: requests.Session | None = None) -> list[dict[str, Any]]:
    """获取 Binance USDⓈ-M 期货保险基金余额。"""
    data = fetch_json(f"{BASE_URL}{INSURANCE_BALANCE_PATH}", session=session)
    if not isinstance(data, list):
        raise RuntimeError("insuranceBalance 返回格式异常")
    out: list[dict[str, Any]] = []
    for item in data:
        if isinstance(item, dict):
            out.append(item)
    return out


def ensure_table(conn: sqlite3.Connection) -> None:
    # 确保 SQLite 数据库 funding.db 中存在 binance_funding_baseinfo 表，若不存在则创建表，避免每次启动清空数据
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
    """从表里读出已有的 symbol 集合，用于后续判断新增/下架"""
    cur = conn.execute(f"SELECT symbol FROM {TABLE_NAME}")
    return {row[0] for row in cur.fetchall()}


def delete_obsolete_symbols(conn: sqlite3.Connection, symbols: set[str]) -> list[str]:
    """把传入的 symbols（通常是“已下架的符号集合”）从表中删除，返回删除列表（排序后）"""
    if not symbols:
        return []
    placeholders = ",".join("?" for _ in symbols)
    conn.execute(
        f"DELETE FROM {TABLE_NAME} WHERE symbol IN ({placeholders})", tuple(symbols)
    )
    return sorted(symbols)


def save_records(conn: sqlite3.Connection, rows: list[tuple[Any, ...]]) -> None:
    """把本轮拉取到的数据批量 upsert 到表中（ON CONFLICT(symbol) DO UPDATE），并 commit()"""
    conn.executemany(
        f"""
        INSERT INTO {TABLE_NAME} (
            symbol, adjustedFundingRateCap, adjustedFundingRateFloor,
            fundingIntervalHours, markPrice, lastFundingRate, openInterest,
            insuranceBalance, updated_at
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
    if column_exists(conn, TABLE_NAME, "insuranceBalanceTime"):
        conn.execute(
            f"UPDATE {TABLE_NAME} SET insuranceBalanceTime=NULL WHERE insuranceBalanceTime IS NOT NULL"
        )
        conn.commit()


def main() -> None:
    with requests.Session() as session:
        session.trust_env = False
        symbols = get_trading_usdt_perpetual_symbols(session=session)
        collector_log_start("Binance", "base", detail=f"{len(symbols)} 个交易对")

        funding_info = get_funding_info(session=session)
        collector_log_progress("Binance", "base", detail="获取资金费上下限、结算周期")

        premium_index = get_premium_index(session=session)
        collector_log_progress("Binance", "base", detail="获取标记价格、最新资金费率")

        open_interest = get_open_interest(symbols)
        collector_log_progress("Binance", "base", detail="获取未平仓合约数")

        try:
            insurance_items = get_insurance_balance(session=session)
            collector_log_progress("Binance", "base", detail=f"获取保险基金余额（{len(insurance_items)} 条）")
        except Exception as exc:  # noqa: BLE001
            insurance_items = []
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] 保险基金余额获取失败：{exc}")

    # 获取当前时间的毫秒级时间戳
    now_ms = int(time.time() * 1000)
    rows: list[tuple[Any, ...]] = []
    insurance_by_symbol: dict[str, Decimal] = {}
    if insurance_items:
        for group in insurance_items:
            # format A: {"symbols": [...], "assets": [{"asset": "...", "marginBalance": "..."}]}
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
                    if not isinstance(sym, str) or not sym:
                        continue
                    insurance_by_symbol[sym] = insurance_by_symbol.get(sym, Decimal("0")) + total
                continue

            # format B: {"symbol": "BTCUSDT", "marginBalance": "..."}
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

        if insurance_items and not insurance_by_symbol:
            first = insurance_items[0]
            keys = list(first.keys()) if isinstance(first, dict) else type(first).__name__
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] insuranceBalance 解析为空；首条 keys={keys}")

    missing_funding_info = 0
    for sym in symbols:
        entry = funding_info.get(sym)
        if not entry:
            missing_funding_info += 1
            continue
        premium = premium_index.get(sym, {})
        insurance_balance = to_plain_str(insurance_by_symbol.get(sym))
        rows.append(
            (
                sym,
                to_plain_str(entry.get("adjustedFundingRateCap")),
                to_plain_str(entry.get("adjustedFundingRateFloor")),
                entry.get("fundingIntervalHours"),
                to_plain_str(premium.get("markPrice")),
                to_plain_str(premium.get("lastFundingRate")),
                to_plain_str(open_interest.get(sym)),
                insurance_balance,
                now_ms,
            )
        )

    if not rows:
        raise RuntimeError("未匹配到任何 fundingInfo 记录")
    if insurance_items and all(r[7] is None for r in rows):
        sample_symbols = sorted(list(insurance_by_symbol.keys()))[:10]
        print(
            f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] insuranceBalance 未能映射到任何交易对；"
            f"insuranceBalance symbols样例={sample_symbols}"
        )

    with sqlite3.connect(DB_PATH) as conn:
        ensure_table(conn)
        existing_symbols = fetch_existing_symbols(conn)
        current_symbols = {row[0] for row in rows}

        # 仅在快照完整时清理下架符号，避免上游接口短暂不全导致误删
        removed: list[str] = []
        if missing_funding_info > 0:
            print(
                f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] fundingInfo 缺失 {missing_funding_info} 个交易对，"
                "为避免误删，跳过下架清理"
            )
        else:
            removed = delete_obsolete_symbols(conn, existing_symbols - current_symbols)

        added = sorted(current_symbols - existing_symbols)

        rows = stamp_rows_updated_at(rows)
        save_records(conn, rows)

    if removed:
        collector_log_progress("Binance", "base", detail=f"删除下架交易对：{', '.join(removed)}")
    if added:
        collector_log_progress("Binance", "base", detail=f"新增交易对：{', '.join(added)}")
    if insurance_items:
        collector_log_progress("Binance", "base", detail="已将保险基金余额写入每个交易对记录（insuranceBalance）")
    collector_log_end("Binance", "base", detail=f"更新 {len(rows)} 条记录到 {TABLE_NAME}")


if __name__ == "__main__":
    main()
