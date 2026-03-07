#!/usr/bin/env python3

"""
从 Hyperliquid 公共 API 获取永续合约基础信息，并写入 SQLite。

数据落库到共享 funding.db 的 hyperliquid_funding_baseinfo 表：
- symbol（这里用 coin 名称，例如 BTC / ETH）
- markPrice（标记价格）
- lastFundingRate（最新资金费率；通常为每小时费率）
- fundingIntervalHours（结算周期小时，默认 1h）
- adjustedFundingRateCap / adjustedFundingRateFloor（Hyperliquid 公共接口通常不提供，上/下限留空）
- openInterest（未平仓价值，USD；优先使用接口的 openInterestUsd，否则用 openInterest * markPrice 估算）
"""

from __future__ import annotations

import os
import sqlite3
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import requests

BASE_URL = os.getenv("HYPERLIQUID_BASE_URL", "https://api.hyperliquid.xyz").rstrip("/")
INFO_PATH = "/info"
REQUEST_TIMEOUT = 15

ROOT_DIR = next(parent for parent in Path(__file__).resolve().parents if (parent / "start_all_funding.sh").exists())
DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT_DIR / "funding.db")).expanduser().resolve()
TABLE_NAME = "hyperliquid_funding_baseinfo"
DEFAULT_FUNDING_INTERVAL_HOURS = 1


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


def hl_post(session: requests.Session, payload: dict[str, Any]) -> Any:
    url = f"{BASE_URL}{INFO_PATH}"
    resp = session.post(url, json=payload, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def fetch_meta_and_asset_ctxs(session: requests.Session) -> tuple[list[str], dict[str, dict[str, Any]]]:
    data = hl_post(session, {"type": "metaAndAssetCtxs"})
    if not isinstance(data, list) or len(data) < 2:
        raise RuntimeError("metaAndAssetCtxs 返回格式异常（期望 [meta, assetCtxs]）")

    meta, asset_ctxs = data[0], data[1]
    universe = meta.get("universe") if isinstance(meta, dict) else None
    if not isinstance(universe, list) or not isinstance(asset_ctxs, list):
        raise RuntimeError("metaAndAssetCtxs 返回字段异常（universe / assetCtxs）")

    symbols: list[str] = []
    ctx_map: dict[str, dict[str, Any]] = {}

    # assetCtxs 通常与 universe 按索引一一对应（assetCtxs 项里未必包含 coin/name 字段）。
    # 因此优先用索引映射，避免 ctx_map 为空导致所有字段缺失。
    for idx, item in enumerate(universe):
        if not isinstance(item, dict):
            continue
        if item.get("isDelisted") or item.get("delisted"):
            continue
        name = item.get("name") or item.get("coin") or item.get("symbol")
        if isinstance(name, str) and name:
            symbols.append(name)
            if idx < len(asset_ctxs) and isinstance(asset_ctxs[idx], dict):
                ctx_map[name] = asset_ctxs[idx]

    symbols = sorted(set(symbols))
    if not symbols:
        raise RuntimeError("未获取到任何可用交易对（universe 为空）")

    # 若索引映射未命中（或 API 形态变化），回退到基于 coin/name/symbol 字段的映射。
    if not ctx_map:
        for ctx in asset_ctxs:
            if not isinstance(ctx, dict):
                continue
            key = ctx.get("coin") or ctx.get("name") or ctx.get("symbol")
            if isinstance(key, str) and key:
                ctx_map[key] = ctx

    return symbols, ctx_map


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


def extract_mark_price(ctx: dict[str, Any]) -> str | None:
    return to_plain_str(ctx.get("markPx") or ctx.get("markPrice") or ctx.get("mark"))


def extract_last_funding_rate(ctx: dict[str, Any]) -> str | None:
    return to_plain_str(
        ctx.get("funding")
        or ctx.get("fundingRate")
        or ctx.get("lastFundingRate")
        or ctx.get("fundingRateNow")
    )


def extract_open_interest_notional(ctx: dict[str, Any], mark_price: str | None) -> str | None:
    oi_usd = ctx.get("openInterestUsd") or ctx.get("openInterestUSD") or ctx.get("openInterestValue")
    oi_usd_s = to_plain_str(oi_usd) if oi_usd is not None else None
    if oi_usd_s is not None:
        return oi_usd_s

    oi = ctx.get("openInterest") or ctx.get("oi")
    oi_s = to_plain_str(oi) if oi is not None else None
    if oi_s is None or mark_price is None:
        return None
    try:
        return to_plain_str(Decimal(oi_s) * Decimal(mark_price))
    except (InvalidOperation, TypeError, ValueError):
        return None


def main() -> None:
    now_ms = int(time.time() * 1000)
    with requests.Session() as session:
        symbols, ctx_map = fetch_meta_and_asset_ctxs(session)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 获取 {len(symbols)} 个交易对（Hyperliquid）")

        rows: list[tuple[Any, ...]] = []
        for sym in symbols:
            ctx = ctx_map.get(sym, {})
            mark_price = extract_mark_price(ctx)
            rows.append(
                (
                    sym,
                    None,
                    None,
                    DEFAULT_FUNDING_INTERVAL_HOURS,
                    mark_price,
                    extract_last_funding_rate(ctx),
                    extract_open_interest_notional(ctx, mark_price),
                    None,
                    now_ms,
                )
            )

        with sqlite3.connect(DB_PATH) as conn:
            ensure_table(conn)
            existing_symbols = fetch_existing_symbols(conn)
            current_symbols = {row[0] for row in rows}

            removed = delete_obsolete_symbols(conn, existing_symbols - current_symbols)
            added = sorted(current_symbols - existing_symbols)
            save_records(conn, rows)

        if removed:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}]删除下架交易对：{', '.join(removed)}")
        if added:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}]新增交易对：{', '.join(added)}")
        print(
            f"[{time.strftime('%Y-%m-%d %H:%M:%S')}]更新 {len(rows)} 条记录到 {DB_PATH} 的表 {TABLE_NAME}"
        )


if __name__ == "__main__":
    main()
