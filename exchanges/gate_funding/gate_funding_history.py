#!/usr/bin/env python3
"""Fetch Gate.io USDT perpetual funding history into SQLite."""

from __future__ import annotations

import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

import requests
from requests import RequestException

ROOT_DIR = next(parent for parent in Path(__file__).resolve().parents if (parent / "start_all_funding.sh").exists())
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from core.common_funding import (
    collector_log_end,
    collector_log_progress,
    collector_log_start,
    RateLimiter,
    delete_older_than,
    ensure_history_table,
    load_symbols,
    normalize_ts_to_ms,
    to_plain_str,
)

BASE_URL = os.getenv("GATE_BASE_URL", "https://api.gateio.ws").rstrip("/")
FUNDING_HISTORY_PATH = "/api/v4/futures/usdt/funding_rate"
REQUEST_TIMEOUT = 20
MAX_HTTP_ATTEMPTS = 4
MAX_FAILED_SYMBOLS_IN_LOG = 10

DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT_DIR / "funding.db")).expanduser().resolve()
INFO_TABLE = "gate_funding_baseinfo"
HISTORY_TABLE = "gate_funding_history"

DAYS_TO_FETCH = 30
DAYS_TO_KEEP = 60
MAX_RECORDS = 1000

WINDOW_SECONDS = float(os.getenv("GATE_HISTORY_RATE_LIMIT_WINDOW_SECONDS", "1"))
WINDOW_CAPACITY = int(os.getenv("GATE_HISTORY_RATE_LIMIT_CAPACITY", "4"))


class GateRequestError(RuntimeError):
    def __init__(self, message: str, *, retryable: bool) -> None:
        super().__init__(message)
        self.retryable = retryable


def gate_retry_delay_seconds(resp: requests.Response | None, attempt: int) -> float:
    if resp is not None:
        retry_after = resp.headers.get("Retry-After")
        if retry_after:
            try:
                seconds = float(retry_after)
            except ValueError:
                seconds = 0.0
            if seconds > 0:
                return min(seconds, 60.0)
    return min(max(2.0, 3.0 * attempt), 30.0)


def gate_get(session: requests.Session, path: str, *, params: dict[str, Any] | None = None) -> Any:
    url = f"{BASE_URL}{path}"
    last_exc: GateRequestError | None = None
    for attempt in range(1, MAX_HTTP_ATTEMPTS + 1):
        try:
            resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        except RequestException as exc:
            last_exc = GateRequestError(f"{path} network error: {exc}", retryable=True)
        else:
            if resp.status_code == 429:
                last_exc = GateRequestError(f"{path} http 429", retryable=True)
                if attempt < MAX_HTTP_ATTEMPTS:
                    time.sleep(gate_retry_delay_seconds(resp, attempt))
                    continue
                break
            if resp.status_code >= 500:
                last_exc = GateRequestError(f"{path} http {resp.status_code}", retryable=True)
            elif resp.status_code >= 400:
                last_exc = GateRequestError(f"{path} http {resp.status_code}", retryable=False)
            else:
                try:
                    return resp.json()
                except ValueError as exc:
                    last_exc = GateRequestError(f"{path} invalid json: {exc}", retryable=True)

        if attempt >= MAX_HTTP_ATTEMPTS or (last_exc is not None and not last_exc.retryable):
            break
        time.sleep(gate_retry_delay_seconds(None, attempt))
    raise GateRequestError(
        f"Gate 请求失败: {path}, err={last_exc}",
        retryable=bool(last_exc.retryable if last_exc is not None else True),
    ) from last_exc


def fetch_symbol_history(
    session: requests.Session,
    limiter: RateLimiter,
    symbol: str,
) -> list[dict[str, Any]]:
    limiter.acquire()
    data = gate_get(session, FUNDING_HISTORY_PATH, params={"contract": symbol, "limit": MAX_RECORDS})
    if not isinstance(data, list):
        raise GateRequestError(f"{symbol} funding history 返回格式异常（非 list）", retryable=False)
    out = [item for item in data if isinstance(item, dict)]
    out.sort(key=lambda x: normalize_ts_to_ms(x.get("t") or x.get("time")) or 0)
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
        funding_time = normalize_ts_to_ms(item.get("t") or item.get("time"))
        if funding_time is None or funding_time < start_ms or funding_time > end_ms:
            continue
        funding_rate = to_plain_str(item.get("r") or item.get("funding_rate") or item.get("fundingRate"))
        rows.append((symbol, funding_time, funding_rate, now_ms))

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

    with sqlite3.connect(DB_PATH) as conn, requests.Session() as session:
        ensure_history_table(conn, HISTORY_TABLE)
        symbols = load_symbols(conn, INFO_TABLE)
        collector_log_start("Gate", "history", detail=f"{len(symbols)} 个交易对，近 {DAYS_TO_FETCH} 天资金费率")
        failed_symbols: list[str] = []

        for idx, symbol in enumerate(symbols, 1):
            try:
                records = fetch_symbol_history(session, limiter, symbol)
            except Exception as exc:  # noqa: BLE001
                failed_symbols.append(symbol)
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
            collector_log_progress("Gate", "history", detail=f"{symbol} 入库 {inserted} 条", current=idx, total=len(symbols))

    if failed_symbols:
        preview = ", ".join(failed_symbols[:MAX_FAILED_SYMBOLS_IN_LOG])
        remaining = len(failed_symbols) - min(len(failed_symbols), MAX_FAILED_SYMBOLS_IN_LOG)
        suffix = "" if remaining <= 0 else f" ... +{remaining}"
        print(
            f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] "
            f"Gate history 存在 {len(failed_symbols)} 个失败交易对，将以非零状态退出触发调度重试："
            f"{preview}{suffix}"
        )
        raise SystemExit(1)

    collector_log_end("Gate", "history")


if __name__ == "__main__":
    main()
