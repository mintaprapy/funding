#!/usr/bin/env python3
"""Persist Variational funding snapshots as interval-bucketed history."""

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
    annualized_decimal_to_interval_decimal_str,
    delete_older_than,
    ensure_history_table,
    parse_iso_to_ms,
)

BASE_URL = os.getenv("VARIATIONAL_BASE_URL", "https://omni-client-api.prod.ap-northeast-1.variational.io").rstrip("/")
STATS_PATH = "/metadata/stats"
REQUEST_TIMEOUT = 20
MAX_HTTP_ATTEMPTS = 4

DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT_DIR / "funding.db")).expanduser().resolve()
HISTORY_TABLE = "variational_funding_history"

DAYS_TO_KEEP = 60


def variational_get(session: requests.Session, path: str) -> dict[str, Any]:
    url = f"{BASE_URL}{path}"
    last_exc: Exception | None = None
    for attempt in range(1, MAX_HTTP_ATTEMPTS + 1):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code >= 500:
                raise RuntimeError(f"{path} http {resp.status_code}")
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict):
                raise RuntimeError(f"{path} 返回格式异常（非 dict）")
            return data
        except (RequestException, RuntimeError) as exc:
            last_exc = exc
            if attempt >= MAX_HTTP_ATTEMPTS:
                break
            time.sleep(min(1.0 * attempt, 3.0))
    raise RuntimeError(f"Variational 请求失败: {path}, err={last_exc}") from last_exc


def bucket_funding_time(updated_ms: int, interval_s: Any) -> int | None:
    try:
        interval_ms = int(float(interval_s) * 1000)
    except (TypeError, ValueError):
        return None
    if interval_ms <= 0:
        return None
    # Variational does not document a public funding-history endpoint. We snapshot the
    # currently published funding rate into the current interval bucket so repeated runs
    # within the same interval overwrite instead of over-counting, while the bucket
    # remains queryable immediately on a fresh database.
    return (updated_ms // interval_ms) * interval_ms


def main() -> None:
    now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - DAYS_TO_KEEP * 24 * 60 * 60 * 1000

    with sqlite3.connect(DB_PATH) as conn, requests.Session() as session:
        ensure_history_table(conn, HISTORY_TABLE)
        collector_log_start("Variational", "history", detail="快照累积模式")
        stats = variational_get(session, STATS_PATH)
        listings = stats.get("listings")
        if not isinstance(listings, list):
            raise RuntimeError("Variational stats 返回格式异常（缺少 listings）")

        rows: list[tuple[Any, ...]] = []
        touched: list[str] = []
        for item in listings:
            if not isinstance(item, dict):
                continue
            symbol = item.get("ticker")
            if not isinstance(symbol, str) or not symbol:
                continue
            quotes = item.get("quotes") if isinstance(item.get("quotes"), dict) else {}
            updated_ms = parse_iso_to_ms(str(quotes.get("updated_at") or "")) or now_ms
            funding_time = bucket_funding_time(updated_ms, item.get("funding_interval_s"))
            if funding_time is None:
                continue
            rows.append(
                (
                    symbol,
                    funding_time,
                    annualized_decimal_to_interval_decimal_str(item.get("funding_rate"), item.get("funding_interval_s")),
                    now_ms,
                )
            )
            touched.append(symbol)

        if not rows:
            raise RuntimeError("未获取到任何可写入的 Variational funding 快照")

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
        delete_older_than(conn, HISTORY_TABLE, cutoff_ms, touched)
        conn.commit()
        collector_log_progress("Variational", "history", detail=f"写入 {len(rows)} 条到 {HISTORY_TABLE}（快照累积模式）")
        collector_log_end("Variational", "history", detail=f"写入 {len(rows)} 条")


if __name__ == "__main__":
    main()
