#!/usr/bin/env python3
"""Simple web dashboard for combined funding data stored in funding.db."""

from __future__ import annotations

import os
import argparse
import json
import sqlite3
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.common_funding import to_plain_str, tune_sqlite_connection
from core.funding_exchanges import dashboard_exchange_meta

DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT / "funding.db")).expanduser().resolve()
HOST = "127.0.0.1"
PORT = 5000

# key, window in ms, label for UI
WINDOWS = [
    ("h24", 24 * 60 * 60 * 1000, "24 小时"),
    ("d3", 3 * 24 * 60 * 60 * 1000, "3 天"),
    ("d7", 7 * 24 * 60 * 60 * 1000, "7 天"),
    ("d15", 15 * 24 * 60 * 60 * 1000, "15 天"),
    ("d30", 30 * 24 * 60 * 60 * 1000, "30 天"),
]

EXCHANGES: dict[str, dict[str, Any]] = dashboard_exchange_meta(ROOT)

MAX_WINDOW_MS = max(span for _, span, _ in WINDOWS)
PAYLOAD_CACHE_TTL_SEC = 10.0

_SCHEMA_LOCK = threading.Lock()
_SCHEMA_PREPARED = False
_INFO_TABLE_BY_EXCHANGE: dict[str, str] = {}

_PAYLOAD_CACHE_LOCK = threading.Lock()
_PAYLOAD_CACHE: dict[str, Any] | None = None
_PAYLOAD_CACHE_TS = 0.0

APP_META_TABLE = "app_meta"


def _to_number(val: Any) -> float | None:
    try:
        if val is None:
            return None
        return float(val)
    except (TypeError, ValueError):
        return None


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    tune_sqlite_connection(conn)
    return conn


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def _ensure_app_meta_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {APP_META_TABLE} (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at INTEGER
        )
        """
    )


def _migration_applied(conn: sqlite3.Connection, key: str) -> bool:
    _ensure_app_meta_table(conn)
    row = conn.execute(f"SELECT 1 FROM {APP_META_TABLE} WHERE key=? LIMIT 1", (key,)).fetchone()
    return row is not None


def _mark_migration_applied(conn: sqlite3.Connection, key: str) -> None:
    _ensure_app_meta_table(conn)
    conn.execute(
        f"""
        INSERT INTO {APP_META_TABLE} (key, value, updated_at)
        VALUES (?, '1', ?)
        ON CONFLICT(key) DO UPDATE SET
            value=excluded.value,
            updated_at=excluded.updated_at
        """,
        (key, int(time.time() * 1000)),
    )
    conn.commit()


def _normalize_legacy_bps_columns(
    conn: sqlite3.Connection,
    table: str,
    columns: list[str],
    *,
    threshold: float,
) -> None:
    if not _table_exists(conn, table):
        return
    max_abs = 0.0
    for column in columns:
        row = conn.execute(
            f"SELECT MAX(ABS(CAST({column} AS REAL))) FROM {table} WHERE {column} IS NOT NULL"
        ).fetchone()
        current = row[0]
        if current is None:
            continue
        max_abs = max(max_abs, abs(float(current)))
    if max_abs <= threshold:
        return

    cur = conn.execute(f"SELECT rowid, {', '.join(columns)} FROM {table}")
    updates: list[tuple[Any, ...]] = []
    for row in cur.fetchall():
        converted = []
        for column in columns:
            value = _to_number(row[column])
            converted.append(None if value is None else to_plain_str(value / 10000.0))
        updates.append((*converted, row["rowid"]))

    assignments = ", ".join(f"{column}=?" for column in columns)
    conn.executemany(f"UPDATE {table} SET {assignments} WHERE rowid=?", updates)
    conn.commit()


def _normalize_legacy_lighter_history_percent(conn: sqlite3.Connection) -> None:
    migration_key = "lighter_funding_history_rate_pct_to_decimal_v1"
    table = "lighter_funding_history"
    if _migration_applied(conn, migration_key) or not _table_exists(conn, table):
        return

    evidence = conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE fundingRate IS NOT NULL AND ABS(CAST(fundingRate AS REAL)) > 0.005"
    ).fetchone()
    if not evidence or int(evidence[0] or 0) <= 0:
        return

    rows = conn.execute(f"SELECT rowid, fundingRate FROM {table} WHERE fundingRate IS NOT NULL").fetchall()
    updates: list[tuple[Any, ...]] = []
    for row in rows:
        value = _to_number(row["fundingRate"])
        if value is None:
            continue
        updates.append((to_plain_str(value / 100.0), row["rowid"]))
    if updates:
        conn.executemany(f"UPDATE {table} SET fundingRate=? WHERE rowid=?", updates)
        conn.commit()
    _mark_migration_applied(conn, migration_key)


def _normalize_legacy_lighter_baseinfo_8h_equivalent(conn: sqlite3.Connection) -> None:
    migration_key = "lighter_funding_baseinfo_8h_equivalent_to_hourly_v1"
    table = "lighter_funding_baseinfo"
    history_table = "lighter_funding_history"
    if _migration_applied(conn, migration_key) or not _table_exists(conn, table) or not _table_exists(conn, history_table):
        return

    rows = conn.execute(
        f"""
        WITH latest_history AS (
            SELECT h.symbol, h.fundingRate
            FROM {history_table} h
            JOIN (
                SELECT symbol, MAX(fundingTime) AS max_time
                FROM {history_table}
                GROUP BY symbol
            ) latest
              ON latest.symbol = h.symbol
             AND latest.max_time = h.fundingTime
        )
        SELECT
            b.rowid,
            b.lastFundingRate,
            latest_history.fundingRate AS latestFundingRate
        FROM {table} b
        LEFT JOIN latest_history ON latest_history.symbol = b.symbol
        WHERE b.lastFundingRate IS NOT NULL
        """
    ).fetchall()
    if not rows:
        return

    ratios: list[float] = []
    updates: list[tuple[Any, ...]] = []
    for row in rows:
        base_value = _to_number(row["lastFundingRate"])
        if base_value is None:
            continue
        latest_history_value = _to_number(row["latestFundingRate"])
        if latest_history_value is not None and abs(latest_history_value) > 1e-9 and abs(base_value) > 1e-9:
            ratios.append(abs(base_value) / abs(latest_history_value))
        updates.append((to_plain_str(base_value / 8.0), row["rowid"]))

    if len(ratios) < 10:
        return

    ratios.sort()
    median_ratio = ratios[len(ratios) // 2]
    legacy_like = sum(1 for value in ratios if 6.0 <= value <= 10.0)
    normalized_like = sum(1 for value in ratios if 0.5 <= value <= 1.5)
    if median_ratio < 4.0 or legacy_like <= normalized_like:
        return

    conn.executemany(f"UPDATE {table} SET lastFundingRate=? WHERE rowid=?", updates)
    conn.commit()
    _mark_migration_applied(conn, migration_key)


def _normalize_legacy_percent_columns(
    conn: sqlite3.Connection,
    table: str,
    columns: list[str],
    *,
    threshold: float,
    migration_key: str,
) -> None:
    if _migration_applied(conn, migration_key) or not _table_exists(conn, table):
        return

    max_abs = 0.0
    for column in columns:
        row = conn.execute(
            f"SELECT MAX(ABS(CAST({column} AS REAL))) FROM {table} WHERE {column} IS NOT NULL"
        ).fetchone()
        current = row[0]
        if current is None:
            continue
        max_abs = max(max_abs, abs(float(current)))
    if max_abs <= threshold:
        return

    cur = conn.execute(f"SELECT rowid, {', '.join(columns)} FROM {table}")
    updates: list[tuple[Any, ...]] = []
    for row in cur.fetchall():
        converted = []
        for column in columns:
            value = _to_number(row[column])
            converted.append(None if value is None else to_plain_str(value / 100.0))
        updates.append((*converted, row["rowid"]))

    assignments = ", ".join(f"{column}=?" for column in columns)
    conn.executemany(f"UPDATE {table} SET {assignments} WHERE rowid=?", updates)
    conn.commit()
    _mark_migration_applied(conn, migration_key)


def _variational_legacy_interval_factor(interval_hours: Any) -> float | None:
    hours = _to_number(interval_hours)
    if hours is None or hours <= 0:
        return None
    return ((24.0 / hours) * 365.0) / 100.0


def _normalize_legacy_variational_annualized_baseinfo(conn: sqlite3.Connection) -> None:
    migration_key = "variational_funding_baseinfo_annualized_to_interval_v1"
    table = "variational_funding_baseinfo"
    if _migration_applied(conn, migration_key) or not _table_exists(conn, table):
        return

    evidence = conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE lastFundingRate IS NOT NULL AND ABS(CAST(lastFundingRate AS REAL)) > 0.05"
    ).fetchone()
    if not evidence or int(evidence[0] or 0) <= 0:
        return

    rows = conn.execute(
        f"SELECT rowid, lastFundingRate, fundingIntervalHours FROM {table} WHERE lastFundingRate IS NOT NULL"
    ).fetchall()
    updates: list[tuple[Any, ...]] = []
    for row in rows:
        value = _to_number(row["lastFundingRate"])
        factor = _variational_legacy_interval_factor(row["fundingIntervalHours"])
        if value is None or factor is None:
            continue
        updates.append((to_plain_str(value / factor), row["rowid"]))
    if updates:
        conn.executemany(f"UPDATE {table} SET lastFundingRate=? WHERE rowid=?", updates)
        conn.commit()
    _mark_migration_applied(conn, migration_key)


def _normalize_legacy_variational_annualized_history(conn: sqlite3.Connection) -> None:
    migration_key = "variational_funding_history_annualized_to_interval_v1"
    table = "variational_funding_history"
    info_table = "variational_funding_baseinfo"
    if _migration_applied(conn, migration_key) or not _table_exists(conn, table):
        return

    evidence = conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE fundingRate IS NOT NULL AND ABS(CAST(fundingRate AS REAL)) > 0.05"
    ).fetchone()
    if not evidence or int(evidence[0] or 0) <= 0:
        return

    interval_by_symbol: dict[str, Any] = {}
    if _table_exists(conn, info_table):
        interval_by_symbol = {
            str(row["symbol"]): row["fundingIntervalHours"]
            for row in conn.execute(f"SELECT symbol, fundingIntervalHours FROM {info_table}")
        }

    rows = conn.execute(f"SELECT rowid, symbol, fundingRate FROM {table} WHERE fundingRate IS NOT NULL").fetchall()
    updates: list[tuple[Any, ...]] = []
    for row in rows:
        value = _to_number(row["fundingRate"])
        factor = _variational_legacy_interval_factor(interval_by_symbol.get(str(row["symbol"]), 8))
        if value is None or factor is None:
            continue
        updates.append((to_plain_str(value / factor), row["rowid"]))
    if updates:
        conn.executemany(f"UPDATE {table} SET fundingRate=? WHERE rowid=?", updates)
        conn.commit()
    _mark_migration_applied(conn, migration_key)


def normalize_legacy_units(conn: sqlite3.Connection) -> None:
    _normalize_legacy_bps_columns(
        conn,
        "grvt_funding_baseinfo",
        ["adjustedFundingRateCap", "adjustedFundingRateFloor", "lastFundingRate"],
        threshold=0.005,
    )
    _normalize_legacy_bps_columns(
        conn,
        "grvt_funding_history",
        ["fundingRate"],
        threshold=0.005,
    )
    _normalize_legacy_bps_columns(
        conn,
        "backpack_funding_baseinfo",
        ["adjustedFundingRateCap", "adjustedFundingRateFloor"],
        threshold=1.0,
    )
    _normalize_legacy_percent_columns(
        conn,
        "variational_funding_baseinfo",
        ["lastFundingRate"],
        threshold=1.0,
        migration_key="variational_funding_baseinfo_rate_pct_to_decimal_v1",
    )
    _normalize_legacy_percent_columns(
        conn,
        "variational_funding_history",
        ["fundingRate"],
        threshold=1.0,
        migration_key="variational_funding_history_rate_pct_to_decimal_v1",
    )
    _normalize_legacy_variational_annualized_baseinfo(conn)
    _normalize_legacy_variational_annualized_history(conn)
    _normalize_legacy_lighter_history_percent(conn)
    _normalize_legacy_lighter_baseinfo_8h_equivalent(conn)


def ensure_column(conn: sqlite3.Connection, table: str, column: str, column_type: str) -> None:
    cur = conn.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cur.fetchall()}
    if column in existing:
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")


def ensure_info_table(conn: sqlite3.Connection, table: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
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
    ensure_column(conn, table, "insuranceBalance", "TEXT")


def ensure_history_table(conn: sqlite3.Connection, table: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            symbol TEXT NOT NULL,
            fundingTime INTEGER NOT NULL,
            fundingRate TEXT,
            updated_at INTEGER,
            PRIMARY KEY (symbol, fundingTime)
        )
        """
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_fundingTime ON {table}(fundingTime)")


def resolve_info_table(conn: sqlite3.Connection, candidates: list[str]) -> str:
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    existing = {row[0] for row in cur.fetchall()}
    for name in candidates:
        if name in existing:
            ensure_info_table(conn, name)
            return name
    primary = candidates[0]
    ensure_info_table(conn, primary)
    return primary


def prepare_schema(conn: sqlite3.Connection) -> None:
    global _SCHEMA_PREPARED, _INFO_TABLE_BY_EXCHANGE
    if _SCHEMA_PREPARED:
        return
    with _SCHEMA_LOCK:
        if _SCHEMA_PREPARED:
            return
        mapping: dict[str, str] = {}
        for exchange, meta in EXCHANGES.items():
            info_table = resolve_info_table(conn, list(meta["info_table_candidates"]))
            history_table = str(meta["history_table"])
            ensure_history_table(conn, history_table)
            mapping[exchange] = info_table
        normalize_legacy_units(conn)
        _INFO_TABLE_BY_EXCHANGE = mapping
        _SCHEMA_PREPARED = True


def fetch_base_info(conn: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    cur = conn.execute(
        f"""
        SELECT symbol, adjustedFundingRateCap, adjustedFundingRateFloor,
               fundingIntervalHours, markPrice, lastFundingRate, openInterest,
               insuranceBalance, updated_at
        FROM {table}
        ORDER BY symbol
        """
    )
    rows: list[dict[str, Any]] = []
    for row in cur.fetchall():
        rows.append(
            {
                "symbol": row["symbol"],
                "adjustedFundingRateCap": _to_number(row["adjustedFundingRateCap"]),
                "adjustedFundingRateFloor": _to_number(row["adjustedFundingRateFloor"]),
                "fundingIntervalHours": row["fundingIntervalHours"],
                "markPrice": _to_number(row["markPrice"]),
                "lastFundingRate": _to_number(row["lastFundingRate"]),
                "openInterest": _to_number(row["openInterest"]),
                "insuranceBalance": _to_number(row["insuranceBalance"]),
                "updated_at": row["updated_at"],
            }
        )
    return rows


def fetch_cumulative_rates(
    conn: sqlite3.Connection, *, history_table: str, now_ms: int
) -> dict[str, dict[str, float | None]]:
    params: list[int] = []
    case_parts = [
        f"COALESCE(SUM(CASE WHEN fundingTime >= ? AND fundingTime <= ? THEN CAST(fundingRate AS REAL) ELSE 0 END), 0) AS {key}"
        for key, span, _ in WINDOWS
    ]
    for _, span, _ in WINDOWS:
        params.extend([now_ms - span, now_ms])
    sql = f"""
        SELECT symbol,
               MIN(fundingTime) AS oldestFundingTime,
               {", ".join(case_parts)}
        FROM {history_table}
        WHERE fundingTime <= ?
        GROUP BY symbol
    """
    cur = conn.execute(sql, [*params, now_ms])
    result: dict[str, dict[str, float | None]] = {}
    for row in cur.fetchall():
        oldest = row["oldestFundingTime"]
        oldest_ms = int(oldest) if oldest is not None else None
        sums: dict[str, float | None] = {}
        for key, span, _ in WINDOWS:
            mature = oldest_ms is not None and oldest_ms <= now_ms - span
            sums[key] = float(row[key]) if mature else None
        result[row["symbol"]] = sums
    return result


def _build_payload_uncached() -> dict[str, Any]:
    now_ms = int(time.time() * 1000)
    items: list[dict[str, Any]] = []
    with connect_db() as conn:
        prepare_schema(conn)
        for exchange, meta in EXCHANGES.items():
            info_table = _INFO_TABLE_BY_EXCHANGE.get(exchange)
            if info_table is None:
                info_table = resolve_info_table(conn, list(meta["info_table_candidates"]))
                _INFO_TABLE_BY_EXCHANGE[exchange] = info_table
            history_table = str(meta["history_table"])
            base_rows = fetch_base_info(conn, info_table)
            sums_by_symbol = fetch_cumulative_rates(conn, history_table=history_table, now_ms=now_ms)
            default_sums = {key: None for key, _, _ in WINDOWS}

            for row in base_rows:
                sums = sums_by_symbol.get(row["symbol"], default_sums)
                oi = row.get("openInterest")
                mp = row.get("markPrice")
                if bool(meta["open_interest_is_notional"]):
                    notional = oi
                else:
                    notional = oi * mp if oi is not None and mp is not None else None
                items.append(
                    {
                        **row,
                        "exchange": exchange,
                        "exchangeLabel": str(meta["label"]),
                        "openInterestNotional": notional,
                        "sums": sums,
                    }
                )

    return {
        "generatedAt": now_ms,
        "windows": [{"key": key, "label": label, "spanMs": span} for key, span, label in WINDOWS],
        "exchanges": [{"key": k, "label": str(v["label"])} for k, v in EXCHANGES.items()],
        "items": items,
    }


def build_payload(*, force_refresh: bool = False) -> dict[str, Any]:
    global _PAYLOAD_CACHE, _PAYLOAD_CACHE_TS
    with _PAYLOAD_CACHE_LOCK:
        now = time.monotonic()
        if (
            not force_refresh
            and _PAYLOAD_CACHE is not None
            and now - _PAYLOAD_CACHE_TS < PAYLOAD_CACHE_TTL_SEC
        ):
            return _PAYLOAD_CACHE
        payload = _build_payload_uncached()
        _PAYLOAD_CACHE = payload
        _PAYLOAD_CACHE_TS = time.monotonic()
        return payload


def render_html() -> str:
    return _render_html(static_payload_json="null")


def render_html_static(payload: dict[str, Any]) -> str:
    static_payload_json = json.dumps(payload, ensure_ascii=False)
    return _render_html(static_payload_json=static_payload_json)


def _render_html(*, static_payload_json: str) -> str:
    windows_meta = json.dumps(
        [{"key": key, "label": label, "spanMs": span} for key, span, label in WINDOWS],
        ensure_ascii=False,
    )
    exchanges_meta = json.dumps(
        [{"key": k, "label": str(v["label"])} for k, v in EXCHANGES.items()],
        ensure_ascii=False,
    )
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Funding Dashboard (All)</title>
  <style>
    :root {{
      --bg: #0e172a;
      --card: rgba(255,255,255,0.04);
      --card-strong: rgba(255,255,255,0.08);
      --accent: #22d3ee;
      --accent-2: #7c3aed;
      --text: #e2e8f0;
      --muted: #94a3b8;
      --danger: #f43f5e;
      --success: #34d399;
      --border: rgba(255,255,255,0.08);
      --font: 'Space Grotesk', 'Manrope', 'SF Pro Display', 'Segoe UI', sans-serif;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: radial-gradient(circle at 20% 20%, rgba(34,211,238,0.08), transparent 35%),
                  radial-gradient(circle at 80% 0%, rgba(124,58,237,0.06), transparent 35%),
                  var(--bg);
      color: var(--text);
      font-family: var(--font);
      min-height: 100vh;
      overflow-x: hidden;
    }}
    .shell {{
      max-width: 100%;
      margin: 0 auto;
      padding: 22px 12px 60px;
    }}
    header {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 22px;
    }}
    .hero {{
      display: flex;
      flex-direction: column;
      gap: 8px;
    }}
    h1 {{
      margin: 0;
      font-size: 32px;
      letter-spacing: -0.5px;
    }}
    .sub {{
      color: var(--muted);
      font-size: 15px;
      max-width: 900px;
      line-height: 1.5;
    }}
    .chip {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 12px;
      border-radius: 12px;
      background: linear-gradient(120deg, rgba(34,211,238,0.1), rgba(124,58,237,0.1));
      border: 1px solid var(--border);
      font-size: 14px;
    }}
    .controls {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      margin-bottom: 18px;
    }}
    .pill {{
      background: var(--card);
      border: 1px solid var(--border);
      padding: 10px 12px;
      border-radius: 12px;
      display: flex;
      align-items: center;
      gap: 10px;
      min-width: 220px;
    }}
    .pill label {{
      color: var(--muted);
      font-size: 13px;
      white-space: nowrap;
    }}
    .pill input, .pill select {{
      flex: 1;
      background: transparent;
      border: none;
      color: var(--text);
      font-size: 14px;
      outline: none;
    }}
    .pill input::placeholder {{
      color: var(--muted);
    }}
    .exchange-pill {{
      flex-direction: column;
      align-items: stretch;
      gap: 10px;
      min-width: 420px;
    }}
    .exchange-tools {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }}
    .exchange-action {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      padding: 6px 10px;
      border-radius: 999px;
      border: 1px solid var(--border);
      background: rgba(255,255,255,0.04);
      color: var(--text);
      font-size: 12px;
      cursor: pointer;
    }}
    .exchange-group {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }}
    .exchange-choice {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 10px;
      border-radius: 10px;
      border: 1px solid var(--border);
      background: rgba(255,255,255,0.03);
      user-select: none;
      transition: background 0.15s ease, border-color 0.15s ease;
    }}
    .exchange-choice.active {{
      background: rgba(34,211,238,0.12);
      border-color: rgba(34,211,238,0.45);
    }}
    .exchange-main {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      cursor: pointer;
    }}
    .exchange-choice input {{
      width: 14px;
      height: 14px;
      margin: 0;
      flex: none;
      accent-color: var(--accent);
    }}
    .exchange-choice .choice-text {{
      color: var(--text);
      font-size: 13px;
      line-height: 1;
    }}
    .exchange-choice .choice-count {{
      color: var(--muted);
      font-size: 12px;
      font-family: 'Menlo', 'SFMono-Regular', Consolas, monospace;
      line-height: 1;
    }}
    .badge {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 6px 10px;
      border-radius: 10px;
      background: var(--card);
      border: 1px solid var(--border);
      font-size: 13px;
    }}
    .range-pill {{
      flex-direction: column;
      align-items: stretch;
      gap: 8px;
      min-width: 260px;
    }}
    .range-title {{
      display: flex;
      align-items: baseline;
      justify-content: flex-start;
      gap: 8px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1;
    }}
    .range-title span {{
      color: var(--text);
      font-family: 'Menlo', 'SFMono-Regular', Consolas, monospace;
    }}
    .range-pill input[type="range"] {{
      width: 100%;
      height: 14px;
      accent-color: var(--accent);
    }}
    .range-pill input[type="range"]::-webkit-slider-runnable-track {{
      height: 4px;
      border-radius: 999px;
      background: rgba(255,255,255,0.12);
    }}
    .range-pill input[type="range"]::-webkit-slider-thumb {{
      -webkit-appearance: none;
      appearance: none;
      width: 14px;
      height: 14px;
      border-radius: 50%;
      background: rgba(255,255,255,0.92);
      border: 2px solid rgba(34,211,238,0.55);
      margin-top: -5px;
      box-shadow: 0 8px 18px rgba(0,0,0,0.35);
    }}
    .range-pill input[type="range"]::-moz-range-track {{
      height: 4px;
      border-radius: 999px;
      background: rgba(255,255,255,0.12);
    }}
    .range-pill input[type="range"]::-moz-range-thumb {{
      width: 14px;
      height: 14px;
      border-radius: 50%;
      background: rgba(255,255,255,0.92);
      border: 2px solid rgba(34,211,238,0.55);
      box-shadow: 0 8px 18px rgba(0,0,0,0.35);
    }}
    .table-wrap {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 16px;
      overflow: visible;
      box-shadow: 0 20px 50px rgba(0,0,0,0.28);
    }}
    table {{
      width: 100%;
      table-layout: fixed;
      border-collapse: collapse;
    }}
    thead {{
      background: rgba(255,255,255,0.04);
    }}
    th, td {{
      padding: 10px 10px;
      border-bottom: 1px solid var(--border);
      font-size: 13px;
      white-space: nowrap;
    }}
    th {{
      text-align: center;
      font-size: 13px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--muted);
      position: sticky;
      top: 0;
      backdrop-filter: blur(8px);
      z-index: 1;
    }}
    tbody tr:hover {{
      background: var(--card-strong);
    }}
    .num {{
      font-family: 'Menlo', 'SFMono-Regular', Consolas, monospace;
      text-align: right;
    }}
    .pos {{ color: var(--success); }}
    .neg {{ color: var(--danger); }}
    .dim {{ color: var(--muted); }}
    .tag {{
      display: inline-flex;
      padding: 4px 8px;
      border-radius: 8px;
      font-size: 12px;
      border: 1px solid var(--border);
      background: rgba(255,255,255,0.03);
    }}
    .stack {{
      display: flex;
      flex-direction: column;
      align-items: flex-end;
      gap: 2px;
    }}
    .mini {{
      font-size: 11px;
      color: var(--muted);
    }}
    .footer {{
      margin-top: 18px;
      color: var(--muted);
      font-size: 13px;
    }}
    @media (max-width: 900px) {{
      header {{ flex-direction: column; align-items: flex-start; }}
      .controls {{ flex-direction: column; }}
      .pill {{ width: 100%; }}
      .exchange-pill {{ min-width: 0; }}
      .shell {{ padding: 20px 16px 40px; }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <header>
      <div class="hero">
        <h1>Funding Dashboard</h1>
        <div class="sub"> Binance / Bybit / Aster / Hyperliquid / Backpack / Ethereal / GRVT / StandX / Lighter / Gate / Bitget / Variational / edgeX 永续合约资金费率 </div>
      </div>
      <div class="chip" id="summaryChip">加载中…</div>
    </header>

    <div class="controls">
      <div class="pill exchange-pill">
        <label>交易所</label>
        <div class="exchange-tools">
          <button type="button" class="exchange-action" id="exchangeSelectAll">全选</button>
          <button type="button" class="exchange-action" id="exchangeInvert">反选</button>
        </div>
        <div class="exchange-group" id="exchangeChoices"></div>
      </div>
      <div class="pill">
        <label for="searchBox">搜索交易对</label>
        <input id="searchBox" type="text" placeholder="如 BTC，精确搜索用 BTC/" />
      </div>
      <div class="pill range-pill">
        <div class="range-title">持仓量 &gt; <span id="oiFilterLabel">0</span>M</div>
        <input id="oiFilter" type="range" min="0" max="7" step="1" value="0" />
      </div>
      <div class="badge" id="lastUpdated">更新中…</div>
    </div>

    <div class="table-wrap">
      <table>
        <colgroup id="colgroup">
          <col data-key="exchange" />
          <col data-key="symbol" />
          <col data-key="markPrice" />
          <col data-key="openInterestNotional" />
          <col data-key="insuranceBalance" />
          <col data-key="lastFundingRate" />
          <col data-key="fundingIntervalHours" />
          <col data-key="bounds" />
          {''.join(f'<col data-key="{key}" />' for key, _, _ in WINDOWS)}
        </colgroup>
        <thead>
          <tr>
            <th data-key="exchange" data-label="交易所">交易所</th>
            <th data-key="symbol" data-label="交易对">交易对</th>
            <th data-key="markPrice" data-label="标记价格">标记价格</th>
            <th data-key="openInterestNotional" data-label="持仓量(M$)">持仓量(M$)</th>
            <th data-key="insuranceBalance" data-label="风险金(M)">风险金(M)</th>
            <th data-key="lastFundingRate" data-label="最新资金费率">最新资金费率</th>
            <th data-key="fundingIntervalHours" data-label="周期">周期</th>
            <th data-key="bounds" data-label="上下限">上下限</th>
            {''.join(f'<th data-key="{key}" data-label="{label}">{label}</th>' for key, _, label in WINDOWS)}
          </tr>
        </thead>
        <tbody id="table-body"></tbody>
      </table>
    </div>
    <div class="footer" id="footerText"></div>
  </div>

  <script>
    const STATIC_PAYLOAD = {static_payload_json};
    const WINDOWS_META = {windows_meta};
    const EXCHANGES_META = {exchanges_meta};
    const WINDOW_KEYS = WINDOWS_META.map(w => w.key);
    const WINDOW_DAYS_BY_KEY = Object.fromEntries(WINDOWS_META.map(w => [w.key, w.spanMs / 86400000]));
    const STR_COLLATOR = new Intl.Collator(undefined, {{ numeric: true, sensitivity: 'base' }});
    const RENDER_CHUNK_SIZE = 180;
    const toPct = (v) => v == null || Number.isNaN(v) ? '—' : (v * 100).toFixed(4) + '%';
    const toPctFixed = (v, digits = 2) => v == null || Number.isNaN(v) ? '—' : (v * 100).toFixed(digits) + '%';
    const toNum = (v, digits = 2) => v == null || Number.isNaN(v) ? '—' : Number(v).toLocaleString(undefined, {{ maximumFractionDigits: digits }});
    const toNumFixed = (v, digits = 2) => v == null || Number.isNaN(v) ? '—' : Number(v).toLocaleString(undefined, {{ minimumFractionDigits: digits, maximumFractionDigits: digits }});
    const formatMarkPrice = (v) => {{
      if (v == null || Number.isNaN(v)) return '—';
      const absV = Math.abs(Number(v));
      if (absV > 0 && absV < 0.0001) return Number(v).toFixed(8);
      if (absV > 1) return toNumFixed(v, 2);
      if (absV >= 0.01) return toNum(v, 4);
      return toNum(v, 6);
    }};
    const classFor = (v) => v > 0 ? 'pos' : v < 0 ? 'neg' : 'dim';

    const OI_THRESHOLDS = [0, 1, 3, 5, 10, 30, 50, 100]; // million USDT

    let dataCache = [];
    let sortKey = 'symbol';
    let sortDir = 'asc';
    let oiThresholdIdx = 0;
    let selectedExchanges = new Set(EXCHANGES_META.map(x => x.key));
    let exchangeCounts = {{}};
    let fixedColumnsApplied = false;
    let renderVersion = 0;
    let scheduledRender = null;

    function exchangeLabel(key) {{
      const hit = EXCHANGES_META.find(x => x.key === key);
      return hit ? hit.label : key;
    }}

    function displaySymbol(sym) {{
      const s = String(sym || '').trim();
      const pairs = [
        '_USDT_PERP',
        '_USDC_PERP',
        '_USD_PERP',
        '_USDT',
        '_USDC',
        '_USD',
        '_PERP',
        'USDT',
        'USDC',
      ];
      const upper = s.toUpperCase();
      for (const suffix of pairs) {{
        if (upper.endsWith(suffix)) {{
          return s.slice(0, s.length - suffix.length);
        }}
      }}
      return s;
    }}

    function prepareItems(items) {{
      return items.map(item => {{
        const rawSymbol = String(item.symbol || '');
        const display = displaySymbol(rawSymbol);
        return {{
          ...item,
          _rawSym: rawSymbol.toUpperCase(),
          _displaySym: display,
          _sortSym: display.toUpperCase(),
          _sortExchange: String(item.exchangeLabel || item.exchange || ''),
        }};
      }});
    }}

    function allExchangesSelected() {{
      return selectedExchanges.size === EXCHANGES_META.length;
    }}

    function renderExchangeChoices() {{
      const group = document.getElementById('exchangeChoices');
      if (!group) return;
      const totalCount = dataCache.length;
      const items = [
        {{ key: 'all', label: '全部', count: totalCount, checked: allExchangesSelected() }},
        ...EXCHANGES_META.map(x => ({{
          key: x.key,
          label: x.label,
          count: exchangeCounts[x.key] || 0,
          checked: selectedExchanges.has(x.key),
        }})),
      ];
      group.innerHTML = items.map(item => `
        <div class="exchange-choice ${'{'}item.checked ? 'active' : ''{'}'}" data-key="${'{'}item.key{'}'}">
          <label class="exchange-main">
            <input type="checkbox" ${'{'}item.checked ? 'checked' : ''{'}'} />
            <span class="choice-text">${'{'}item.label{'}'}</span>
            <span class="choice-count">${'{'}item.count{'}'}</span>
          </label>
        </div>
      `).join('');
      group.querySelectorAll('.exchange-main input').forEach(input => {{
        input.addEventListener('change', handleExchangeChoiceChange);
      }});
    }}

    function handleExchangeChoiceChange(event) {{
      const input = event.currentTarget;
      const wrapper = input.closest('.exchange-choice');
      const key = wrapper ? wrapper.dataset.key : null;
      const checked = !!input.checked;
      if (!key) return;
      if (key === 'all') {{
        selectedExchanges = checked ? new Set(EXCHANGES_META.map(x => x.key)) : new Set();
      }} else {{
        if (checked) {{
          selectedExchanges.add(key);
        }} else {{
          selectedExchanges.delete(key);
        }}
      }}
      renderExchangeChoices();
      render();
    }}

    function applyFixedColumnWidths() {{
      if (fixedColumnsApplied) return;
      const table = document.querySelector('.table-wrap table');
      const colgroup = document.getElementById('colgroup');
      if (!table || !colgroup) return;
      const cols = Array.from(colgroup.querySelectorAll('col'));
      if (!cols.length) return;

      const widths = new Array(cols.length).fill(0);
      const rows = table.querySelectorAll('thead tr, tbody tr');
      rows.forEach(row => {{
        const cells = Array.from(row.children);
        for (let i = 0; i < Math.min(cells.length, widths.length); i++) {{
          const w = cells[i].scrollWidth;
          if (w > widths[i]) widths[i] = w;
        }}
      }});

      const extra = 4; // 小余量，避免像素级截断
      let total = 0;
      widths.forEach((w, i) => {{
        const colW = Math.max(1, Math.ceil(w + extra));
        widths[i] = colW;
        total += colW;
      }});
      if (!total) return;
      widths.forEach((w, i) => {{
        cols[i].style.width = `${'{'}(w / total * 100).toFixed(4){'}'}%`;
      }});
      table.style.tableLayout = 'fixed';
      table.style.width = '100%';
      fixedColumnsApplied = true;
    }}

    async function load() {{
      if (STATIC_PAYLOAD) {{
        dataCache = prepareItems(STATIC_PAYLOAD.items || []);
        updateMeta(STATIC_PAYLOAD);
        render();
        return;
      }}
      const res = await fetch('/api/data');
      if (!res.ok) throw new Error('数据获取失败');
      const payload = await res.json();
      dataCache = prepareItems(payload.items || []);
      updateMeta(payload);
      render();
    }}

    function updateMeta(payload) {{
      const chip = document.getElementById('summaryChip');
      const footer = document.getElementById('footerText');
      const updated = new Date(payload.generatedAt || Date.now());
      const total = dataCache.length;
      const counts = {{}};
      dataCache.forEach(it => counts[it.exchange] = (counts[it.exchange] || 0) + 1);
      exchangeCounts = counts;
      const parts = EXCHANGES_META.map(x => `${'{'}x.label{'}'} ${{counts[x.key] || 0}}`).join(' · ');
      chip.textContent = `共 ${{total}} 个交易对（${'{'}parts{'}'}）`;
      document.getElementById('lastUpdated').textContent = `生成时间：${'{'}updated.toLocaleString(){'}'}`;
      footer.textContent = '历史数据 1 小时更新一次，其他数据 10 分钟更新一次';
      renderExchangeChoices();
    }}

    function getSortValue(item, key) {{
      if (!key || key === 'symbol') return item._sortSym || '';
      if (key === 'exchange') return item._sortExchange || '';
      if (['lastFundingRate', 'openInterestNotional', 'insuranceBalance', 'markPrice', 'fundingIntervalHours', 'adjustedFundingRateCap', 'adjustedFundingRateFloor'].includes(key)) {{
        return item[key] == null ? null : Number(item[key]);
      }}
      if (key === 'bounds') {{
        return item.adjustedFundingRateCap == null ? null : Number(item.adjustedFundingRateCap);
      }}
      if (item.sums && key in item.sums) {{
        return item.sums[key] == null ? null : Number(item.sums[key]);
      }}
      return null;
    }}

    function updateSortIndicators() {{
      document.querySelectorAll('th[data-key]').forEach(th => {{
        const base = th.dataset.label || th.textContent.trim();
        const arrow = th.dataset.key === sortKey ? (sortDir === 'asc' ? '↑' : '↓') : '';
        th.textContent = arrow ? `${'{'}base{'}'} ${'{'}arrow{'}'}` : base;
      }});
    }}

    function renderRow(item) {{
      const sums = item.sums || {{}};
      const windowCells = WINDOW_KEYS.map(key => {{
        const v = sums[key];
        const days = WINDOW_DAYS_BY_KEY[key] || null;
        const ann = days && v != null ? (v / days) * 365 : null;
        return `<td class="num"><div class="stack"><span class="${'{'}classFor(v){'}'}">${'{'}toPct(v){'}'}</span><span class="mini ${'{'}classFor(ann){'}'}">APR ${'{'}toPctFixed(ann, 2){'}'}</span></div></td>`;
      }}).join('');
      const notionalDisplay = item.openInterestNotional == null ? null : item.openInterestNotional / 1_000_000;
      const insuranceDisplay = item.insuranceBalance == null ? null : item.insuranceBalance / 1_000_000;
      const latestAnn = item.fundingIntervalHours ? (item.lastFundingRate ?? 0) * (24 / item.fundingIntervalHours) * 365 : null;
      return `
        <tr>
          <td><span class="tag">${'{'}exchangeLabel(item.exchange){'}'}</span></td>
          <td><span class="tag">${'{'}item._displaySym || displaySymbol(item.symbol){'}'}</span></td>
          <td class="num">${'{'}formatMarkPrice(item.markPrice){'}'}</td>
          <td class="num">${'{'}toNumFixed(notionalDisplay, 2){'}'}</td>
          <td class="num">${'{'}toNumFixed(insuranceDisplay, 2){'}'}</td>
          <td class="num"><div class="stack"><span class="${'{'}classFor(item.lastFundingRate){'}'}">${'{'}toPct(item.lastFundingRate){'}'}</span><span class="mini ${'{'}classFor(latestAnn){'}'}">APR ${'{'}toPctFixed(latestAnn, 2){'}'}</span></div></td>
          <td class="num">${'{'}item.fundingIntervalHours ? item.fundingIntervalHours + 'h' : '—'{'}'}</td>
          <td class="num"><div class="stack"><span class="mini">${'{'}toPctFixed(item.adjustedFundingRateCap, 2){'}'}</span><span class="mini">${'{'}toPctFixed(item.adjustedFundingRateFloor, 2){'}'}</span></div></td>
          ${'{'}windowCells{'}'}
        </tr>
      `;
    }}

    function appendRowsInChunks(body, rows, version, offset = 0) {{
      if (version !== renderVersion) return;
      const chunk = rows.slice(offset, offset + RENDER_CHUNK_SIZE);
      if (!chunk.length) {{
        if (!fixedColumnsApplied) applyFixedColumnWidths();
        return;
      }}
      body.insertAdjacentHTML('beforeend', chunk.map(renderRow).join(''));
      const nextOffset = offset + chunk.length;
      if (nextOffset >= rows.length) {{
        if (!fixedColumnsApplied) applyFixedColumnWidths();
        return;
      }}
      requestAnimationFrame(() => appendRowsInChunks(body, rows, version, nextOffset));
    }}

    function renderNow() {{
      const body = document.getElementById('table-body');
      renderVersion += 1;
      const version = renderVersion;
      const rawQuery = document.getElementById('searchBox').value.trim().toUpperCase();
      const exactSearch = rawQuery.endsWith('/');
      const q = exactSearch ? rawQuery.slice(0, -1).trim() : rawQuery;
      const threshold = OI_THRESHOLDS[oiThresholdIdx] * 1_000_000;
      const filtered = dataCache.filter(item => {{
        const hitExchange = selectedExchanges.size > 0 && selectedExchanges.has(item.exchange);
        const rawSym = item._rawSym || '';
        const dispSym = item._sortSym || '';
        const hitSymbol = !q || (
          exactSearch
            ? rawSym === q || dispSym === q
            : rawSym.includes(q) || dispSym.includes(q)
        );
        const notional = item.openInterestNotional ?? 0;
        const hideDefaultZeroOiGrvt = item.exchange === 'grvt' && !q && notional <= 0;
        const hitOi = exactSearch && q ? true : notional >= threshold;
        return hitExchange && hitSymbol && hitOi && !hideDefaultZeroOiGrvt;
      }});

      const sorted = filtered.sort((a, b) => {{
        const va = getSortValue(a, sortKey);
        const vb = getSortValue(b, sortKey);
        if (typeof va === 'string' && typeof vb === 'string') {{
          const base = sortDir === 'asc' ? STR_COLLATOR.compare(va, vb) : STR_COLLATOR.compare(vb, va);
          if (base !== 0) return base;
          const sa = a._rawSym || '';
          const sb = b._rawSym || '';
          return STR_COLLATOR.compare(sa, sb);
        }}
        const aMissing = va == null || Number.isNaN(va);
        const bMissing = vb == null || Number.isNaN(vb);
        if (aMissing && bMissing) {{
          const base = STR_COLLATOR.compare(a._sortSym || '', b._sortSym || '');
          if (base !== 0) return base;
          return STR_COLLATOR.compare(a._rawSym || '', b._rawSym || '');
        }}
        if (aMissing) return 1;
        if (bMissing) return -1;
        const diff = sortDir === 'asc' ? va - vb : vb - va;
        if (diff !== 0) return diff;
        const base = STR_COLLATOR.compare(a._sortSym || '', b._sortSym || '');
        if (base !== 0) return base;
        return STR_COLLATOR.compare(a._rawSym || '', b._rawSym || '');
      }});

      if (!sorted.length) {{
        body.innerHTML = '<tr><td colspan="99" class="dim">暂无数据：请先运行采集脚本写入 funding.db</td></tr>';
        updateSortIndicators();
        return;
      }}

      updateSortIndicators();
      body.innerHTML = '';
      appendRowsInChunks(body, sorted, version);
    }}

    function render() {{
      if (scheduledRender) cancelAnimationFrame(scheduledRender);
      scheduledRender = requestAnimationFrame(() => {{
        scheduledRender = null;
        renderNow();
      }});
    }}

    let searchDebounce = null;
    document.getElementById('searchBox').addEventListener('input', () => {{
      if (searchDebounce) clearTimeout(searchDebounce);
      searchDebounce = setTimeout(render, 80);
    }});
    document.getElementById('exchangeSelectAll').addEventListener('click', () => {{
      selectedExchanges = new Set(EXCHANGES_META.map(x => x.key));
      renderExchangeChoices();
      render();
    }});
    document.getElementById('exchangeInvert').addEventListener('click', () => {{
      const inverted = EXCHANGES_META.filter(x => !selectedExchanges.has(x.key)).map(x => x.key);
      selectedExchanges = new Set(inverted);
      renderExchangeChoices();
      render();
    }});

    const oiFilter = document.getElementById('oiFilter');
    const oiFilterLabel = document.getElementById('oiFilterLabel');
    const updateOiLabel = () => {{
      const val = OI_THRESHOLDS[oiThresholdIdx];
      oiFilterLabel.textContent = `${'{'}val{'}'}`;
    }};
    oiFilter.addEventListener('input', (e) => {{
      oiThresholdIdx = Number(e.target.value) || 0;
      updateOiLabel();
      render();
    }});
    updateOiLabel();

    document.querySelectorAll('th[data-key]').forEach(th => {{
      th.style.cursor = 'pointer';
      th.addEventListener('click', () => {{
        const key = th.dataset.key;
        if (key === sortKey) {{
          sortDir = sortDir === 'asc' ? 'desc' : 'asc';
        }} else {{
          sortKey = key;
          sortDir = (key === 'symbol' || key === 'exchange') ? 'asc' : 'desc';
        }}
        render();
      }});
    }});
    updateSortIndicators();

    load().catch(err => {{
      document.getElementById('table-body').innerHTML = `<tr><td colspan="99" class="dim">加载失败：${'{'}err.message{'}'}</td></tr>`;
    }});
  </script>
</body>
</html>
"""


class DashboardHandler(BaseHTTPRequestHandler):
    def _send_json(self, payload: dict[str, Any], status: int = 200) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_html(self, content: str) -> None:
        data = content.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        if path in ("/", "/index.html"):
            self._send_html(render_html())
            return
        if path == "/api/data":
            try:
                payload = build_payload()
            except Exception as exc:  # noqa: BLE001
                self._send_json({"error": str(exc)}, status=500)
                return
            self._send_json(payload)
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return


def write_static_file(output: Path) -> None:
    payload = build_payload(force_refresh=True)
    html = render_html_static(payload)
    output.write_text(html, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Funding dashboard (all exchanges) for the shared funding.db")
    parser.add_argument("--host", default=HOST, help="Bind host (server mode)")
    parser.add_argument("--port", type=int, default=PORT, help="Bind port (server mode)")
    parser.add_argument(
        "--static",
        action="store_true",
        help="Generate a static HTML file instead of starting an HTTP server",
    )
    parser.add_argument(
        "--output",
        default=str(Path(__file__).resolve().parent / "funding_dashboard_static.html"),
        help="Static HTML output path (static mode)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.static:
        output = Path(args.output)
        write_static_file(output)
        print(f"Wrote static dashboard to: {output}")
        return

    try:
        server = ThreadingHTTPServer((args.host, args.port), DashboardHandler)
    except OSError as exc:
        output = Path(args.output)
        write_static_file(output)
        print(
            f"Failed to bind http://{args.host}:{args.port} ({exc}). "
            f"Fallback: wrote static dashboard to: {output}"
        )
        return

    print(f"Serving dashboard at http://{args.host}:{args.port} (Ctrl+C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping server...")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
