#!/usr/bin/env python3
"""从 Hyperliquid 获取永续合约近 30 天资金费率，存入 SQLite，并保留 60 天内数据。"""

from __future__ import annotations

import os
import sqlite3
import time
from collections import deque
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

import requests

BASE_URL = os.getenv("HYPERLIQUID_BASE_URL", "https://api.hyperliquid.xyz").rstrip("/")
INFO_PATH = "/info"
REQUEST_TIMEOUT = 15

ROOT_DIR = next(parent for parent in Path(__file__).resolve().parents if (parent / "start_all_funding.sh").exists())
DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT_DIR / "funding.db")).expanduser().resolve()
INFO_TABLE = "hyperliquid_funding_baseinfo"
HISTORY_TABLE = "hyperliquid_funding_history"

DAYS_TO_FETCH = 30
DAYS_TO_KEEP = 60

# Hyperliquid fundingHistory 单次返回可能被截断（实测上限约 500 条），按时间分块请求。
# 20 天 * 24 小时 ≈ 480 条，通常可避免触发截断并减少请求次数。
CHUNK_DAYS = 20

# Hyperliquid Docs: REST aggregated weight limit is 1200 per minute (per IP).
# fundingHistory 属于 info 请求：基础权重 20；且每返回 20 条记录额外增加 1 权重。
# https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits
WINDOW_SECONDS = 60
WINDOW_CAPACITY = 1200
INFO_WEIGHT_DEFAULT = 20
FUNDING_HISTORY_EXTRA_WEIGHT_PER_ITEMS = 20
MAX_ITEMS_PER_RESPONSE = 500
HOUR_MS = 3_600_000

# 增量回补时回退一些小时，避免边界误差（例如整点附近的 timestamp 偏移）
INCREMENTAL_OVERLAP_HOURS = 2


def tune_sqlite_connection(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA busy_timeout=15000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")


class WeightedRateLimiter:
    def __init__(self, capacity: int, period: int) -> None:
        self.capacity = capacity
        self.period = period
        self.events: deque[tuple[float, int]] = deque()
        self.total_weight = 0

    def _purge(self, now: float) -> None:
        while self.events and now - self.events[0][0] >= self.period:
            _, weight = self.events.popleft()
            self.total_weight -= weight

    def acquire(self, weight: int) -> None:
        if weight <= 0:
            return
        if weight > self.capacity:
            raise ValueError(f"weight {weight} exceeds limiter capacity {self.capacity}")

        while True:
            now = time.monotonic()
            self._purge(now)
            if self.total_weight + weight <= self.capacity:
                self.events.append((now, weight))
                self.total_weight += weight
                return
            if not self.events:
                time.sleep(0.1)
                continue
            sleep_for = self.period - (now - self.events[0][0]) + 0.05
            time.sleep(max(0.1, sleep_for))


def to_plain_str(val: Any) -> str | None:
    try:
        dec = Decimal(str(val))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return format(dec.normalize(), "f")


def hl_post(session: requests.Session, payload: dict[str, Any]) -> Any:
    url = f"{BASE_URL}{INFO_PATH}"
    resp = session.post(url, json=payload, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def load_symbols(conn: sqlite3.Connection) -> list[str]:
    cur = conn.execute(f"SELECT symbol FROM {INFO_TABLE}")
    symbols = sorted(row[0] for row in cur.fetchall() if row[0])
    if not symbols:
        raise RuntimeError(f"{INFO_TABLE} 中没有交易对数据，请先同步该表")
    return symbols


def ensure_history_table(conn: sqlite3.Connection) -> None:
    tune_sqlite_connection(conn)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (
            symbol TEXT NOT NULL,
            fundingTime INTEGER NOT NULL,
            fundingRate TEXT,
            updated_at INTEGER,
            PRIMARY KEY (symbol, fundingTime)
        )
        """
    )
    conn.commit()


def delete_older_than(conn: sqlite3.Connection, cutoff_ms: int, symbols: Iterable[str]) -> None:
    symbols = list(set(symbols))
    if not symbols:
        return
    placeholders = ",".join("?" for _ in symbols)
    conn.execute(
        f"DELETE FROM {HISTORY_TABLE} WHERE fundingTime < ? AND symbol IN ({placeholders})",
        (cutoff_ms, *symbols),
    )


def normalize_ts_to_ms(ts: Any) -> int | None:
    try:
        t = int(ts)
    except (TypeError, ValueError):
        return None
    # 一些接口会返回秒级时间戳；这里做一个简单判别（< 10^12 认为是秒）
    if t < 1_000_000_000_000:
        return t * 1000
    return t


def load_existing_buckets(
    conn: sqlite3.Connection,
    symbol: str,
    *,
    window_start_ms: int,
    window_end_ms_exclusive: int,
) -> set[int]:
    """Return a set of hour buckets in [window_start_ms, window_end_ms_exclusive) for this symbol."""
    cur = conn.execute(
        f"""
        SELECT fundingTime
        FROM {HISTORY_TABLE}
        WHERE symbol = ? AND fundingTime >= ? AND fundingTime < ?
        """,
        (symbol, window_start_ms, window_end_ms_exclusive),
    )
    buckets: set[int] = set()
    for (funding_time,) in cur.fetchall():
        if funding_time is None:
            continue
        try:
            buckets.add(int(funding_time) // HOUR_MS)
        except (TypeError, ValueError):
            continue
    return buckets


def save_history(
    conn: sqlite3.Connection,
    symbol: str,
    records: list[dict[str, Any]],
    *,
    now_ms: int,
) -> int:
    rows: list[tuple[Any, ...]] = []
    for entry in records:
        ts = entry.get("time") or entry.get("fundingTime") or entry.get("timestamp")
        funding_time = normalize_ts_to_ms(ts)
        if funding_time is None:
            continue
        rows.append(
            (
                symbol,
                funding_time,
                to_plain_str(entry.get("fundingRate") or entry.get("funding") or entry.get("rate")),
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


def fetch_symbol_history(
    session: requests.Session,
    limiter: WeightedRateLimiter,
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> list[dict[str, Any]]:
    all_records: list[dict[str, Any]] = []
    chunk_ms = CHUNK_DAYS * 24 * 60 * 60 * 1000
    cursor = start_ms
    while cursor < end_ms:
        chunk_end = min(end_ms, cursor + chunk_ms)
        # 估算本次 fundingHistory 的权重：20 + ceil(items/20)
        duration_hours = max(1, int((chunk_end - cursor + HOUR_MS - 1) // HOUR_MS))
        expected_items = min(MAX_ITEMS_PER_RESPONSE, duration_hours)
        extra_weight = (expected_items + FUNDING_HISTORY_EXTRA_WEIGHT_PER_ITEMS - 1) // FUNDING_HISTORY_EXTRA_WEIGHT_PER_ITEMS
        req_weight = INFO_WEIGHT_DEFAULT + extra_weight
        limiter.acquire(req_weight)
        payload = {"type": "fundingHistory", "coin": symbol, "startTime": cursor, "endTime": chunk_end}
        data = _hl_post_with_retry(session, payload)
        if not isinstance(data, list):
            raise RuntimeError(f"{symbol} fundingHistory 返回格式异常（非 list）")
        for item in data:
            if isinstance(item, dict):
                all_records.append(item)
        cursor = chunk_end

    all_records.sort(key=lambda item: normalize_ts_to_ms(item.get("time") or item.get("fundingTime") or 0) or 0)
    return all_records


def _hl_post_with_retry(session: requests.Session, payload: dict[str, Any]) -> Any:
    backoff = 2.0
    max_backoff = 60.0
    for attempt in range(6):
        try:
            return hl_post(session, payload)
        except requests.HTTPError as exc:
            status = getattr(exc.response, "status_code", None)
            if status != 429 or attempt >= 5:
                raise
            retry_after = None
            if exc.response is not None:
                retry_after = exc.response.headers.get("Retry-After")
            try:
                sleep_for = float(retry_after) if retry_after else backoff
            except (TypeError, ValueError):
                sleep_for = backoff
            time.sleep(min(max_backoff, max(1.0, sleep_for)))
            backoff = min(max_backoff, backoff * 2)


def main() -> None:
    end_ms = int(time.time() * 1000)
    end_bucket = end_ms // HOUR_MS
    expected_buckets = int(DAYS_TO_FETCH * 24)
    start_bucket = end_bucket - expected_buckets + 1

    window_start_ms = start_bucket * HOUR_MS
    window_end_ms_exclusive = (end_bucket + 1) * HOUR_MS
    cutoff_ms = end_ms - DAYS_TO_KEEP * 24 * 60 * 60 * 1000

    limiter = WeightedRateLimiter(WINDOW_CAPACITY, WINDOW_SECONDS)

    with sqlite3.connect(DB_PATH) as conn, requests.Session() as session:
        ensure_history_table(conn)
        symbols = load_symbols(conn)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}]共 {len(symbols)} 个交易对，开始拉取近 {DAYS_TO_FETCH} 天资金费率")

        for idx, symbol in enumerate(symbols, 1):
            existing = load_existing_buckets(
                conn,
                symbol,
                window_start_ms=window_start_ms,
                window_end_ms_exclusive=window_end_ms_exclusive,
            )
            missing = [b for b in range(start_bucket, end_bucket + 1) if b not in existing]
            if not missing:
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][{idx}/{len(symbols)}] {symbol} 数据齐全，跳过")
                continue

            # 为确保数据 100% 齐全：从最早缺失的小时开始回补（并带一点 overlap 避免边界误差）
            first_missing_bucket = missing[0]
            symbol_start_ms = max(window_start_ms, (first_missing_bucket - INCREMENTAL_OVERLAP_HOURS) * HOUR_MS)

            try:
                records = fetch_symbol_history(session, limiter, symbol, symbol_start_ms, end_ms)
            except Exception as exc:  # noqa: BLE001
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} 获取失败：{exc}")
                continue

            inserted = save_history(conn, symbol, records, now_ms=end_ms)
            delete_older_than(conn, cutoff_ms, [symbol])
            conn.commit()
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][{idx}/{len(symbols)}] {symbol} 入库 {inserted} 条")

            # 回补后再检查一次是否仍有缺口（用于提醒是否仍需等待最新数据产生）
            after = load_existing_buckets(
                conn,
                symbol,
                window_start_ms=window_start_ms,
                window_end_ms_exclusive=window_end_ms_exclusive,
            )
            missing_after = [b for b in range(start_bucket, end_bucket + 1) if b not in after]
            if missing_after:
                # 若缺口全部发生在“该标的最早可得记录之前”（通常意味着新上线/历史不可得），无需报错提示。
                if after and max(missing_after) < min(after):
                    continue
                first = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(missing_after[0] * HOUR_MS / 1000))
                last = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(missing_after[-1] * HOUR_MS / 1000))
                print(
                    f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} 仍缺 {len(missing_after)} 个小时数据：{first} ~ {last}"
                )

    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}]同步完成")


if __name__ == "__main__":
    main()
