#!/usr/bin/env python3
"""Fetch GRVT perp funding base info into SQLite."""

from __future__ import annotations

import os
import sqlite3
import sys
import time
from decimal import Decimal, InvalidOperation
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
    pct_to_decimal_str,
    stamp_rows_updated_at,
    to_plain_str,
)

BASE_URL = os.getenv("GRVT_BASE_URL", "https://market-data.grvt.io").rstrip("/")
SYMBOLS_PATH = "/full/v1/all_instruments"
TICKER_PATH = "/full/v1/ticker"
REQUEST_TIMEOUT = 20
MAX_REQUEST_ATTEMPTS = 4
RETRY_BASE_SLEEP = 1.0
MAX_TICKER_RECOVERY_ROUNDS = 3
TICKER_RECOVERY_SLEEP_BASE = 3.0

DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT_DIR / "funding.db")).expanduser().resolve()
TABLE_NAME = "grvt_funding_baseinfo"
DEFAULT_FUNDING_INTERVAL_HOURS = 8

WINDOW_SECONDS = 60
WINDOW_CAPACITY = 180


def grvt_post(
    session: requests.Session,
    path: str,
    payload: dict[str, Any] | None = None,
) -> Any:
    url = f"{BASE_URL}{path}"
    last_exc: Exception | None = None
    for attempt in range(1, MAX_REQUEST_ATTEMPTS + 1):
        try:
            resp = session.post(url, json=payload or {}, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
            if attempt >= MAX_REQUEST_ATTEMPTS:
                break
            time.sleep(min(10.0, RETRY_BASE_SLEEP * attempt))
    raise RuntimeError(f"请求失败: {url}; last_error={last_exc}") from last_exc


def extract_data_list(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    result = payload.get("result")
    if isinstance(result, list):
        return [item for item in result if isinstance(item, dict)]
    if isinstance(result, dict):
        data = result.get("data")
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
    data = payload.get("data")
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    return []


def extract_data_object(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    result = payload.get("result")
    if isinstance(result, dict):
        return result
    data = payload.get("data")
    if isinstance(data, dict):
        return data
    return payload


def fetch_symbols(session: requests.Session) -> dict[str, dict[str, Any]]:
    payload = grvt_post(session, SYMBOLS_PATH, {})
    items = extract_data_list(payload)
    out: dict[str, dict[str, Any]] = {}
    for item in items:
        instrument = item.get("instrument")
        if not isinstance(instrument, str) or not instrument:
            continue
        quote = str(item.get("quote") or "").upper()
        kind = str(item.get("kind") or "").upper()
        if quote != "USDT":
            continue
        if "PERP" not in kind:
            continue
        out[instrument] = item
    if not out:
        raise RuntimeError("未获取到 GRVT USDT 永续交易对")
    return out


def fetch_tickers(
    session: requests.Session, symbols: list[str], limiter: RateLimiter
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    out: dict[str, dict[str, Any]] = {}
    failed_symbols: list[str] = []
    for symbol in symbols:
        try:
            limiter.acquire()
            payload = grvt_post(session, TICKER_PATH, {"instrument": symbol})
        except Exception as exc:  # noqa: BLE001
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} ticker 获取失败：{exc}")
            failed_symbols.append(symbol)
            continue
        obj = extract_data_object(payload) or {}
        if isinstance(obj, dict):
            out[symbol] = obj
    return out, failed_symbols


def recover_failed_tickers(
    session: requests.Session,
    tickers: dict[str, dict[str, Any]],
    failed_symbols: list[str],
    limiter: RateLimiter,
) -> list[str]:
    remaining = failed_symbols[:]
    for round_idx in range(1, MAX_TICKER_RECOVERY_ROUNDS + 1):
        if not remaining:
            break
        sleep_s = min(15.0, TICKER_RECOVERY_SLEEP_BASE * round_idx)
        collector_log_progress(
            "GRVT",
            "base",
            detail=(
                f"ticker 补抓 round {round_idx}/{MAX_TICKER_RECOVERY_ROUNDS}，"
                f"待补 {len(remaining)} 个；sleep {sleep_s:.1f}s"
            ),
        )
        time.sleep(sleep_s)
        recovered, remaining = fetch_tickers(session, remaining, limiter)
        tickers.update(recovered)
    return remaining


def _extract_interval_hours(item: dict[str, Any]) -> int:
    for key in ("funding_interval_hours", "fundingIntervalHours", "funding_interval", "fundingInterval"):
        v = item.get(key)
        if v is None:
            continue
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if f >= 60:
            return max(1, int(round(f / 60)))
        return max(1, int(round(f)))
    return DEFAULT_FUNDING_INTERVAL_HOURS


def sum_plain_values(*values: Any) -> str | None:
    total = Decimal("0")
    has_value = False
    for value in values:
        plain = to_plain_str(value)
        if plain is None:
            continue
        try:
            total += Decimal(plain)
            has_value = True
        except (InvalidOperation, TypeError, ValueError):
            continue
    return to_plain_str(total) if has_value else None


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
    limiter = RateLimiter(WINDOW_CAPACITY, WINDOW_SECONDS)

    with requests.Session() as session, sqlite3.connect(DB_PATH) as conn:
        session.trust_env = False
        ensure_baseinfo_table(conn, TABLE_NAME)
        symbols_meta = fetch_symbols(session)
        symbols = sorted(symbols_meta.keys())
        tickers, failed_symbols = fetch_tickers(session, symbols, limiter)
        if failed_symbols:
            failed_symbols = recover_failed_tickers(session, tickers, failed_symbols, limiter)
        collector_log_start("GRVT", "base", detail=f"{len(symbols)} 个交易对")

        rows: list[tuple[Any, ...]] = []
        for idx, symbol in enumerate(symbols, 1):
            meta = symbols_meta[symbol]
            ticker = tickers.get(symbol, {})

            mark_price = to_plain_str(ticker.get("mark_price") or ticker.get("markPrice"))
            open_interest = to_plain_str(ticker.get("open_interest") or ticker.get("openInterest"))
            if open_interest is None:
                open_interest = to_plain_str(ticker.get("open_interest_qty") or ticker.get("openInterestQty"))

            rows.append(
                (
                    symbol,
                    pct_to_decimal_str(meta.get("adjusted_funding_rate_cap") or meta.get("funding_rate_upper_limit")),
                    pct_to_decimal_str(meta.get("adjusted_funding_rate_floor") or meta.get("funding_rate_lower_limit")),
                    _extract_interval_hours(meta),
                    mark_price,
                    pct_to_decimal_str(ticker.get("funding_rate") or ticker.get("funding")),
                    open_interest,
                    None,
                    sum_plain_values(ticker.get("buy_volume_24h_b"), ticker.get("sell_volume_24h_b")),
                    sum_plain_values(ticker.get("buy_volume_24h_q"), ticker.get("sell_volume_24h_q")),
                    now_ms,
                )
            )
            if idx % 30 == 0 or idx == len(symbols):
                collector_log_progress("GRVT", "base", detail="处理中", current=idx, total=len(symbols))

        existing = fetch_existing_symbols(conn, TABLE_NAME)
        current = {row[0] for row in rows}
        deleted = delete_obsolete_symbols(conn, TABLE_NAME, existing - current)
        if deleted:
            collector_log_progress("GRVT", "base", detail=f"删除已下架交易对 {len(deleted)} 个")

        rows = stamp_rows_updated_at(rows)
        save_records(conn, rows)
        collector_log_end("GRVT", "base", detail=f"入库 {len(rows)} 条到 {TABLE_NAME}")
        if failed_symbols:
            preview = ",".join(failed_symbols[:10])
            suffix = "" if len(failed_symbols) <= 10 else f" ... total={len(failed_symbols)}"
            raise RuntimeError(f"GRVT baseinfo 存在 ticker 未成功拉取的交易对: {preview}{suffix}")


if __name__ == "__main__":
    main()
