#!/usr/bin/env python3
"""Simple web dashboard for combined funding data stored in funding.db."""

from __future__ import annotations

import os
import argparse
import gzip
import hashlib
import hmac
import json
import sqlite3
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urlparse

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.common_funding import (
    ensure_baseinfo_numeric_layout,
    ensure_history_table as ensure_shared_history_table,
    to_plain_str,
    tune_sqlite_connection,
)
from core.funding_exchanges import dashboard_exchange_meta

DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT / "funding.db")).expanduser().resolve()
HOST = "127.0.0.1"
PORT = 5000
ALERT_CONFIG_ENV = "FUNDING_ALERT_CONFIG"
DASHBOARD_ADMIN_TOKEN_ENV = "FUNDING_DASHBOARD_ADMIN_TOKEN"
DEFAULT_ALERT_CONFIG_PATH = ROOT / "config" / "alerts.json"
DASHBOARD_ADMIN_TOKEN_CONFIG_KEY = "dashboard_admin_token"

WindowDef = tuple[str, int, str]

# key, window in ms, label for UI
WINDOWS: tuple[WindowDef, ...] = (
    ("h24", 24 * 60 * 60 * 1000, "24 小时"),
    ("d3", 3 * 24 * 60 * 60 * 1000, "3 天"),
    ("d7", 7 * 24 * 60 * 60 * 1000, "7 天"),
    ("d15", 15 * 24 * 60 * 60 * 1000, "15 天"),
    ("d30", 30 * 24 * 60 * 60 * 1000, "30 天"),
)
BASE_WINDOW_KEYS = frozenset(key for key, _, _ in WINDOWS)
MATERIALIZED_EXTRA_WINDOWS: tuple[WindowDef, ...] = (
    ("h4", 4 * 60 * 60 * 1000, "4 小时"),
)

EXCHANGES: dict[str, dict[str, Any]] = dashboard_exchange_meta(ROOT)

MAX_WINDOW_MS = max(span for _, span, _ in WINDOWS)
PAYLOAD_CACHE_TTL_SEC = 10.0
GZIP_MIN_BYTES = 1400
GZIP_COMPRESSLEVEL = 5
SLOW_META_BUILD_MS = 1000.0
SLOW_PAYLOAD_BUILD_MS = 3000.0
SLOW_HTTP_META_MS = 1500.0
SLOW_HTTP_DATA_MS = 5000.0
BASEINFO_INTERVAL_MS = 10 * 60 * 1000
EXCHANGE_FRESH_LAG_MS = 2 * 60 * 1000

_SCHEMA_LOCK = threading.Lock()
_SCHEMA_PREPARED = False
_INFO_TABLE_BY_EXCHANGE: dict[str, str] = {}

_PAYLOAD_CACHE_LOCK = threading.Lock()
_PAYLOAD_CACHE_COND = threading.Condition(_PAYLOAD_CACHE_LOCK)
_PAYLOAD_CACHE: dict[str, Any] | None = None
_PAYLOAD_CACHE_JSON: bytes | None = None
_PAYLOAD_CACHE_TS = 0.0
_PAYLOAD_CACHE_BASEINFO_BATCH_COMPLETED_AT: int | None = None
_PAYLOAD_CACHE_HISTORY_BATCH_COMPLETED_AT: int | None = None
_PAYLOAD_CACHE_WINDOWS_SIGNATURE: tuple[tuple[str, int], ...] | None = None
_PAYLOAD_CACHE_ETAG: str | None = None
_PAYLOAD_BUILD_IN_PROGRESS = False

_HISTORY_SUMS_CACHE_LOCK = threading.Lock()
_HISTORY_SUMS_CACHE_BATCH_COMPLETED_AT: int | None = None
_HISTORY_SUMS_CACHE_ANCHOR_MS: int | None = None
_HISTORY_SUMS_CACHE_WINDOWS_SIGNATURE: tuple[tuple[str, int], ...] | None = None
_HISTORY_SUMS_CACHE: dict[str, dict[str, dict[str, Any]]] | None = None

_LEGACY_MIGRATIONS_LOCK = threading.Lock()
_LEGACY_MIGRATIONS_PREPARED = False

APP_META_TABLE = "app_meta"
HISTORY_WINDOW_SUMMARIES_TABLE = "history_window_summaries"
CLIENT_DISCONNECT_ERRORS = (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)
BASEINFO_BATCH_COMPLETED_AT_KEY = "baseinfo_batch_completed_at"
HISTORY_BATCH_COMPLETED_AT_KEY = "history_batch_completed_at"
HISTORY_SUMMARIES_WINDOW_SIG_KEY = "history_summaries_window_sig"
HISTORY_SUMMARIES_BATCH_COMPLETED_AT_KEY = "history_summaries_batch_completed_at"
HISTORY_SUMMARIES_ANCHOR_MS_KEY = "history_summaries_anchor_ms"
ALERT_BLACKLIST_META_KEY = "alert_row_blacklist_v1"
DISPLAY_SYMBOL_SUFFIXES = (
    "_USDT_PERP",
    "_USDC_PERP",
    "_USD_PERP",
    "_USDT",
    "_USDC",
    "_USD",
    "_PERP",
    "USDT",
    "USDC",
)


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


def _to_int(val: Any) -> int | None:
    try:
        if val is None:
            return None
        return int(val)
    except (TypeError, ValueError):
        return None


def _normalize_symbol_text(symbol: Any) -> str:
    return str(symbol or "").strip().upper()


def display_symbol_for_trade(symbol: Any) -> str:
    normalized = _normalize_symbol_text(symbol)
    for suffix in DISPLAY_SYMBOL_SUFFIXES:
        if normalized.endswith(suffix):
            return normalized[: -len(suffix)]
    return normalized


def _grvt_trade_symbol(symbol: Any) -> str:
    normalized = _normalize_symbol_text(symbol)
    if normalized.endswith("_PERP"):
        normalized = normalized[:-5]
    return normalized.replace("_", "-")


def trade_page_url(exchange: Any, symbol: Any) -> str | None:
    exchange_key = str(exchange or "").strip().lower()
    original_symbol = str(symbol or "").strip()
    raw_symbol = _normalize_symbol_text(symbol)
    if not exchange_key or not raw_symbol:
        return None

    display_symbol = display_symbol_for_trade(raw_symbol)
    quoted_raw_symbol = quote(raw_symbol, safe="")
    quoted_display_symbol = quote(display_symbol, safe="")

    if exchange_key == "binance":
        return f"https://www.binance.com/en/futures/{quoted_raw_symbol}"
    if exchange_key == "bybit":
        return f"https://www.bybit.com/trade/usdt/{quoted_raw_symbol}"
    if exchange_key == "aster":
        return f"https://www.asterdex.com/en/trade/pro/futures/{quoted_raw_symbol}"
    if exchange_key == "hyperliquid":
        trade_symbol = original_symbol or raw_symbol
        return f"https://app.hyperliquid.xyz/trade/{quote(trade_symbol, safe=':')}"
    if exchange_key == "backpack":
        return f"https://backpack.exchange/trade/{quote(f'{display_symbol}_USD_PERP', safe='')}"
    if exchange_key == "ethereal":
        return f"https://app.ethereal.trade/{quoted_raw_symbol}"
    if exchange_key == "grvt":
        return f"https://grvt.io/exchange/perpetual/{quote(_grvt_trade_symbol(raw_symbol), safe='')}"
    if exchange_key == "standx":
        return f"https://standx.com/perps?symbol={quoted_raw_symbol}"
    if exchange_key == "lighter":
        return f"https://app.lighter.xyz/trade/{quoted_display_symbol}"
    if exchange_key == "gate":
        return f"https://www.gate.com/futures/USDT/{quoted_raw_symbol}"
    if exchange_key == "bitget":
        return f"https://www.bitget.com/futures/usdt/{quoted_raw_symbol}"
    if exchange_key == "variational":
        return f"https://omni.variational.io/perpetual/{quoted_display_symbol}"
    if exchange_key == "edgex":
        return f"https://pro.edgex.exchange/?symbol={quoted_raw_symbol}"
    return None


def merge_windows(extra_windows: tuple[WindowDef, ...] | None = None) -> tuple[WindowDef, ...]:
    merged = list(WINDOWS)
    seen = {key for key, _, _ in WINDOWS}
    for key, span, label in extra_windows or ():
        if key in seen:
            continue
        merged.append((key, span, label))
        seen.add(key)
    return tuple(merged)


def windows_signature(windows: tuple[WindowDef, ...]) -> tuple[tuple[str, int], ...]:
    return tuple((key, span) for key, span, _ in windows)


def materialized_windows_for(windows: tuple[WindowDef, ...]) -> tuple[WindowDef, ...]:
    requested_extra_windows = tuple((key, span, label) for key, span, label in windows if key not in BASE_WINDOW_KEYS)
    return merge_windows(MATERIALIZED_EXTRA_WINDOWS + requested_extra_windows)


def _etag_for_bytes(data: bytes) -> str:
    return f'W/"{hashlib.blake2b(data, digest_size=16).hexdigest()}"'


def _dashboard_log(message: str) -> None:
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] [dashboard] {message}", flush=True)


def _log_if_slow(name: str, total_ms: float, threshold_ms: float, **metrics: Any) -> None:
    if total_ms < threshold_ms:
        return
    parts = [f"{key}={value}" for key, value in metrics.items()]
    suffix = f" {' '.join(parts)}" if parts else ""
    _dashboard_log(f"slow {name}: {total_ms:.1f}ms{suffix}")


def _fetch_app_meta_timestamp(conn: sqlite3.Connection, key: str) -> int | None:
    if not _table_exists(conn, APP_META_TABLE):
        return None
    row = conn.execute(
        f"SELECT value, updated_at FROM {APP_META_TABLE} WHERE key=? LIMIT 1",
        (key,),
    ).fetchone()
    if row is None:
        return None
    ts = _to_int(row["value"])
    if ts is None:
        ts = _to_int(row["updated_at"])
    return ts


def _fetch_app_meta_value(conn: sqlite3.Connection, key: str) -> str | None:
    if not _table_exists(conn, APP_META_TABLE):
        return None
    row = conn.execute(
        f"SELECT value FROM {APP_META_TABLE} WHERE key=? LIMIT 1",
        (key,),
    ).fetchone()
    if row is None:
        return None
    value = row["value"]
    return None if value is None else str(value)


def _upsert_app_meta_value(conn: sqlite3.Connection, key: str, value: str, *, updated_at_ms: int | None = None) -> None:
    _ensure_app_meta_table(conn)
    ts = int(time.time() * 1000) if updated_at_ms is None else int(updated_at_ms)
    conn.execute(
        f"""
        INSERT INTO {APP_META_TABLE} (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value=excluded.value,
            updated_at=excluded.updated_at
        """,
        (key, value, ts),
    )


def normalize_alert_row_key(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw or "::" not in raw:
        return None
    exchange, symbol = raw.split("::", 1)
    exchange = exchange.strip().lower()
    symbol = symbol.strip().upper()
    if not exchange or not symbol:
        return None
    return f"{exchange}::{symbol}"


def alert_row_key_for(exchange: Any, symbol: Any) -> str | None:
    exchange_text = str(exchange or "").strip().lower()
    symbol_text = str(symbol or "").strip().upper()
    if not exchange_text or not symbol_text:
        return None
    return f"{exchange_text}::{symbol_text}"


def _fetch_alert_blacklist_row_keys(conn: sqlite3.Connection) -> set[str]:
    raw = _fetch_app_meta_value(conn, ALERT_BLACKLIST_META_KEY)
    if not raw:
        return set()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return set()
    if not isinstance(parsed, list):
        return set()
    out: set[str] = set()
    for value in parsed:
        row_key = normalize_alert_row_key(value)
        if row_key:
            out.add(row_key)
    return out


def _store_alert_blacklist_row_keys(
    conn: sqlite3.Connection,
    row_keys: set[str],
    *,
    updated_at_ms: int | None = None,
) -> set[str]:
    normalized = sorted({row_key for row_key in (normalize_alert_row_key(value) for value in row_keys) if row_key})
    _upsert_app_meta_value(
        conn,
        ALERT_BLACKLIST_META_KEY,
        json.dumps(normalized, ensure_ascii=False),
        updated_at_ms=updated_at_ms,
    )
    return set(normalized)


def load_alert_blacklist_row_keys() -> set[str]:
    with connect_db() as conn:
        return _fetch_alert_blacklist_row_keys(conn)


def update_alert_blacklist_row_keys(
    *,
    row_key: str | None = None,
    blocked: bool | None = None,
    clear_all: bool = False,
    replace_all_row_keys: set[str] | None = None,
) -> set[str]:
    with connect_db() as conn:
        if replace_all_row_keys is not None:
            row_keys = set(replace_all_row_keys)
        else:
            row_keys = _fetch_alert_blacklist_row_keys(conn)
        if clear_all:
            row_keys.clear()
        elif replace_all_row_keys is None:
            normalized = normalize_alert_row_key(row_key)
            if normalized is None or blocked is None:
                raise ValueError("invalid row key update")
            if blocked:
                row_keys.add(normalized)
            else:
                row_keys.discard(normalized)
        stored = _store_alert_blacklist_row_keys(conn, row_keys)
        conn.commit()
        return stored


def load_dashboard_admin_token() -> str:
    env_value = str(os.getenv(DASHBOARD_ADMIN_TOKEN_ENV) or "").strip()
    if env_value:
        return env_value
    config_path = Path(os.getenv(ALERT_CONFIG_ENV) or DEFAULT_ALERT_CONFIG_PATH).expanduser().resolve()
    if not config_path.exists():
        return ""
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ""
    if not isinstance(payload, dict):
        return ""
    raw = payload.get(DASHBOARD_ADMIN_TOKEN_CONFIG_KEY)
    return str(raw).strip() if raw not in (None, "") else ""


def fetch_exchange_baseinfo_completed_at(conn: sqlite3.Connection) -> dict[str, int]:
    if not _table_exists(conn, APP_META_TABLE):
        return {}
    cur = conn.execute(
        f"""
        SELECT key, value, updated_at
        FROM {APP_META_TABLE}
        WHERE key LIKE 'exchange_baseinfo_completed_at:%'
        ORDER BY key
        """
    )
    out: dict[str, int] = {}
    for row in cur.fetchall():
        key = str(row["key"] or "")
        _, _, exchange = key.partition(":")
        if not exchange:
            continue
        ts = _to_int(row["value"])
        if ts is None:
            ts = _to_int(row["updated_at"])
        if ts is not None:
            out[exchange] = ts
    return out


def fetch_baseinfo_batch_completed_at(conn: sqlite3.Connection) -> int | None:
    return _fetch_app_meta_timestamp(conn, BASEINFO_BATCH_COMPLETED_AT_KEY)


def fetch_history_batch_completed_at(conn: sqlite3.Connection) -> int | None:
    return _fetch_app_meta_timestamp(conn, HISTORY_BATCH_COMPLETED_AT_KEY)


def fetch_batch_completion_markers(conn: sqlite3.Connection) -> tuple[int | None, int | None]:
    return (
        fetch_baseinfo_batch_completed_at(conn),
        fetch_history_batch_completed_at(conn),
    )


def build_exchange_freshness(
    exchange_updated_at: dict[str, int],
    *,
    baseinfo_batch_completed_at: int | None,
    now_ms: int,
) -> dict[str, dict[str, int | str | None]]:
    freshness: dict[str, dict[str, int | str | None]] = {}
    for exchange in EXCHANGES:
        updated_at = exchange_updated_at.get(exchange)
        if updated_at is None:
            freshness[exchange] = {
                "status": "missing",
                "updatedAt": None,
                "lagMs": None,
                "ageMs": None,
            }
            continue
        age_ms = max(0, now_ms - updated_at)
        lag_ms = None if baseinfo_batch_completed_at is None else max(0, baseinfo_batch_completed_at - updated_at)
        status = "unknown"
        if lag_ms is not None:
            if lag_ms <= EXCHANGE_FRESH_LAG_MS:
                status = "fresh"
            elif lag_ms <= BASEINFO_INTERVAL_MS:
                status = "lagging"
            else:
                status = "stale"
        freshness[exchange] = {
            "status": status,
            "updatedAt": updated_at,
            "lagMs": lag_ms,
            "ageMs": age_ms,
        }
    return freshness


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


def _normalize_legacy_grvt_overdivided_baseinfo(conn: sqlite3.Connection) -> None:
    migration_key = "grvt_funding_baseinfo_decimal_scale_fix_v1"
    table = "grvt_funding_baseinfo"
    columns = ["adjustedFundingRateCap", "adjustedFundingRateFloor", "lastFundingRate"]
    if _migration_applied(conn, migration_key) or not _table_exists(conn, table):
        return

    row = conn.execute(
        f"""
        SELECT MAX(ABS(CAST(adjustedFundingRateCap AS REAL))) AS max_cap
        FROM {table}
        WHERE adjustedFundingRateCap IS NOT NULL
        """
    ).fetchone()
    max_cap = _to_number(row["max_cap"] if row else None)
    if max_cap is None or max_cap <= 0.0 or max_cap > 0.001:
        return

    rows = conn.execute(f"SELECT rowid, {', '.join(columns)} FROM {table}").fetchall()
    updates: list[tuple[Any, ...]] = []
    for row in rows:
        converted = []
        for column in columns:
            value = _to_number(row[column])
            converted.append(None if value is None else to_plain_str(value * 100.0))
        updates.append((*converted, row["rowid"]))

    assignments = ", ".join(f"{column}=?" for column in columns)
    conn.executemany(f"UPDATE {table} SET {assignments} WHERE rowid=?", updates)
    conn.commit()
    _mark_migration_applied(conn, migration_key)


def _normalize_legacy_grvt_overdivided_history(conn: sqlite3.Connection) -> None:
    migration_key = "grvt_funding_history_decimal_scale_fix_v1"
    table = "grvt_funding_history"
    info_table = "grvt_funding_baseinfo"
    if _migration_applied(conn, migration_key) or not _table_exists(conn, table) or not _table_exists(conn, info_table):
        return

    rows = conn.execute(
        f"""
        WITH latest_history AS (
            SELECT h.rowid, h.symbol, h.fundingRate
            FROM {table} h
            JOIN (
                SELECT symbol, MAX(fundingTime) AS max_time
                FROM {table}
                GROUP BY symbol
            ) latest
              ON latest.symbol = h.symbol
             AND latest.max_time = h.fundingTime
        )
        SELECT latest_history.rowid, latest_history.symbol, latest_history.fundingRate, b.lastFundingRate
        FROM latest_history
        JOIN {info_table} b ON b.symbol = latest_history.symbol
        WHERE latest_history.fundingRate IS NOT NULL AND b.lastFundingRate IS NOT NULL
        """
    ).fetchall()
    if len(rows) < 10:
        return

    ratios: list[float] = []
    for row in rows:
        history_value = _to_number(row["fundingRate"])
        base_value = _to_number(row["lastFundingRate"])
        if history_value is None or base_value is None:
            continue
        if abs(history_value) <= 1e-12 or abs(base_value) <= 1e-12:
            continue
        ratios.append(abs(history_value) / abs(base_value))
    if len(ratios) < 10:
        return

    ratios.sort()
    median_ratio = ratios[len(ratios) // 2]
    overdivided_like = sum(1 for value in ratios if 0.005 <= value <= 0.02)
    normalized_like = sum(1 for value in ratios if 0.5 <= value <= 1.5)
    if median_ratio >= 0.2 or overdivided_like <= normalized_like:
        return

    updates: list[tuple[Any, ...]] = []
    for row in conn.execute(f"SELECT rowid, fundingRate FROM {table} WHERE fundingRate IS NOT NULL").fetchall():
        value = _to_number(row["fundingRate"])
        if value is None:
            continue
        updates.append((to_plain_str(value * 100.0), row["rowid"]))
    if updates:
        conn.executemany(f"UPDATE {table} SET fundingRate=? WHERE rowid=?", updates)
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
    _normalize_legacy_grvt_overdivided_baseinfo(conn)
    _normalize_legacy_percent_columns(
        conn,
        "grvt_funding_baseinfo",
        ["adjustedFundingRateCap", "adjustedFundingRateFloor", "lastFundingRate"],
        threshold=0.05,
        migration_key="grvt_funding_baseinfo_rate_pct_to_decimal_v1",
    )
    _normalize_legacy_grvt_overdivided_history(conn)
    _normalize_legacy_percent_columns(
        conn,
        "grvt_funding_history",
        ["fundingRate"],
        threshold=0.05,
        migration_key="grvt_funding_history_rate_pct_to_decimal_v1",
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
    ensure_baseinfo_numeric_layout(conn, table)


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
            ensure_baseinfo_numeric_layout(conn, info_table)
            ensure_shared_history_table(conn, history_table)
            mapping[exchange] = info_table
        _INFO_TABLE_BY_EXCHANGE = mapping
        _SCHEMA_PREPARED = True


def prepare_legacy_migrations(conn: sqlite3.Connection) -> None:
    global _LEGACY_MIGRATIONS_PREPARED
    if _LEGACY_MIGRATIONS_PREPARED:
        return
    with _LEGACY_MIGRATIONS_LOCK:
        if _LEGACY_MIGRATIONS_PREPARED:
            return
        normalize_legacy_units(conn)
        _LEGACY_MIGRATIONS_PREPARED = True


def initialize_dashboard_runtime(*, apply_legacy_migrations: bool = False) -> None:
    with connect_db() as conn:
        prepare_schema(conn)
        if apply_legacy_migrations:
            prepare_legacy_migrations(conn)


def fetch_base_info(conn: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    cur = conn.execute(
        f"""
        SELECT symbol,
               COALESCE(adjustedFundingRateCapNum, CASE WHEN adjustedFundingRateCap IS NULL OR TRIM(adjustedFundingRateCap) = '' THEN NULL ELSE CAST(adjustedFundingRateCap AS REAL) END) AS adjustedFundingRateCap,
               COALESCE(adjustedFundingRateFloorNum, CASE WHEN adjustedFundingRateFloor IS NULL OR TRIM(adjustedFundingRateFloor) = '' THEN NULL ELSE CAST(adjustedFundingRateFloor AS REAL) END) AS adjustedFundingRateFloor,
               fundingIntervalHours,
               COALESCE(markPriceNum, CASE WHEN markPrice IS NULL OR TRIM(markPrice) = '' THEN NULL ELSE CAST(markPrice AS REAL) END) AS markPrice,
               COALESCE(lastFundingRateNum, CASE WHEN lastFundingRate IS NULL OR TRIM(lastFundingRate) = '' THEN NULL ELSE CAST(lastFundingRate AS REAL) END) AS lastFundingRate,
               COALESCE(openInterestNum, CASE WHEN openInterest IS NULL OR TRIM(openInterest) = '' THEN NULL ELSE CAST(openInterest AS REAL) END) AS openInterest,
               COALESCE(insuranceBalanceNum, CASE WHEN insuranceBalance IS NULL OR TRIM(insuranceBalance) = '' THEN NULL ELSE CAST(insuranceBalance AS REAL) END) AS insuranceBalance,
               updated_at
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
    conn: sqlite3.Connection,
    *,
    history_table: str,
    now_ms: int,
    allow_partial: bool = False,
    windows: tuple[WindowDef, ...] = WINDOWS,
) -> dict[str, dict[str, Any]]:
    params: list[int] = []
    case_parts = [
        f"COALESCE(SUM(CASE WHEN fundingTime >= ? AND fundingTime <= ? THEN COALESCE(fundingRateNum, CASE WHEN fundingRate IS NULL OR TRIM(fundingRate) = '' THEN NULL ELSE CAST(fundingRate AS REAL) END) ELSE 0 END), 0) AS {key}"
        for key, span, _ in windows
    ]
    for _, span, _ in windows:
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
    result: dict[str, dict[str, Any]] = {}
    for row in cur.fetchall():
        oldest = row["oldestFundingTime"]
        oldest_ms = int(oldest) if oldest is not None else None
        sums: dict[str, float | None] = {}
        partial: dict[str, bool] = {}
        coverage_ms: dict[str, int | None] = {}
        for key, span, _ in windows:
            mature = oldest_ms is not None and oldest_ms <= now_ms - span
            observed_ms = None if oldest_ms is None else max(0, now_ms - oldest_ms)
            use_partial = bool(allow_partial and oldest_ms is not None and not mature)
            sums[key] = float(row[key]) if (mature or use_partial) else None
            partial[key] = use_partial
            coverage_ms[key] = span if mature else observed_ms
        result[row["symbol"]] = {
            "values": sums,
            "partial": partial,
            "coverageMs": coverage_ms,
        }
    return result


def _ensure_history_window_summaries_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {HISTORY_WINDOW_SUMMARIES_TABLE} (
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            window_key TEXT NOT NULL,
            window_span_ms INTEGER NOT NULL,
            batch_completed_at INTEGER,
            anchor_ms INTEGER NOT NULL,
            sum_value REAL,
            is_partial INTEGER NOT NULL,
            coverage_ms INTEGER,
            PRIMARY KEY (exchange, symbol, window_key, window_span_ms)
        )
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{HISTORY_WINDOW_SUMMARIES_TABLE}_exchange_symbol
        ON {HISTORY_WINDOW_SUMMARIES_TABLE}(exchange, symbol)
        """
    )


def _history_summary_window_sig(windows: tuple[WindowDef, ...]) -> str:
    return json.dumps(windows_signature(windows), separators=(",", ":"), ensure_ascii=False)


def _materialized_history_summaries_are_current(
    conn: sqlite3.Connection,
    *,
    history_batch_completed_at: int | None,
    history_anchor_ms: int,
    windows: tuple[WindowDef, ...],
) -> bool:
    if history_batch_completed_at is None:
        return False
    return (
        _fetch_app_meta_value(conn, HISTORY_SUMMARIES_WINDOW_SIG_KEY) == _history_summary_window_sig(windows)
        and _to_int(_fetch_app_meta_value(conn, HISTORY_SUMMARIES_BATCH_COMPLETED_AT_KEY)) == history_batch_completed_at
        and _to_int(_fetch_app_meta_value(conn, HISTORY_SUMMARIES_ANCHOR_MS_KEY)) == history_anchor_ms
    )


def _load_materialized_history_summaries(
    conn: sqlite3.Connection,
    *,
    windows: tuple[WindowDef, ...],
) -> dict[str, dict[str, dict[str, Any]]]:
    _ensure_history_window_summaries_table(conn)
    result: dict[str, dict[str, dict[str, Any]]] = {}
    default_sums = {key: None for key, _, _ in windows}
    default_partial = {key: False for key, _, _ in windows}
    default_coverage = {key: None for key, _, _ in windows}
    expected_keys = {key for key, _, _ in windows}

    rows = conn.execute(
        f"""
        SELECT exchange, symbol, window_key, sum_value, is_partial, coverage_ms
        FROM {HISTORY_WINDOW_SUMMARIES_TABLE}
        ORDER BY exchange, symbol, window_key
        """
    ).fetchall()
    for row in rows:
        exchange = str(row["exchange"])
        symbol = str(row["symbol"])
        window_key = str(row["window_key"])
        if window_key not in expected_keys:
            continue
        exchange_rows = result.setdefault(exchange, {})
        bundle = exchange_rows.setdefault(
            symbol,
            {
                "values": dict(default_sums),
                "partial": dict(default_partial),
                "coverageMs": dict(default_coverage),
            },
        )
        bundle["values"][window_key] = _to_number(row["sum_value"])
        bundle["partial"][window_key] = bool(row["is_partial"])
        bundle["coverageMs"][window_key] = _to_int(row["coverage_ms"])
    return result


def _store_materialized_history_summaries(
    conn: sqlite3.Connection,
    *,
    history_sums_by_exchange: dict[str, dict[str, dict[str, Any]]],
    history_batch_completed_at: int,
    history_anchor_ms: int,
    windows: tuple[WindowDef, ...],
) -> None:
    _ensure_history_window_summaries_table(conn)
    now_ms = int(time.time() * 1000)
    rows: list[tuple[Any, ...]] = []
    for exchange, symbol_map in history_sums_by_exchange.items():
        for symbol, bundle in symbol_map.items():
            values = bundle.get("values") or {}
            partial = bundle.get("partial") or {}
            coverage = bundle.get("coverageMs") or {}
            for key, span, _ in windows:
                rows.append(
                    (
                        exchange,
                        symbol,
                        key,
                        span,
                        history_batch_completed_at,
                        history_anchor_ms,
                        values.get(key),
                        1 if partial.get(key) else 0,
                        coverage.get(key),
                    )
                )

    conn.execute(f"DELETE FROM {HISTORY_WINDOW_SUMMARIES_TABLE}")
    if rows:
        conn.executemany(
            f"""
            INSERT INTO {HISTORY_WINDOW_SUMMARIES_TABLE} (
                exchange, symbol, window_key, window_span_ms,
                batch_completed_at, anchor_ms, sum_value, is_partial, coverage_ms
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    _upsert_app_meta_value(
        conn,
        HISTORY_SUMMARIES_WINDOW_SIG_KEY,
        _history_summary_window_sig(windows),
        updated_at_ms=now_ms,
    )
    _upsert_app_meta_value(
        conn,
        HISTORY_SUMMARIES_BATCH_COMPLETED_AT_KEY,
        str(history_batch_completed_at),
        updated_at_ms=now_ms,
    )
    _upsert_app_meta_value(
        conn,
        HISTORY_SUMMARIES_ANCHOR_MS_KEY,
        str(history_anchor_ms),
        updated_at_ms=now_ms,
    )
    conn.commit()


def _build_history_sums_by_exchange(
    conn: sqlite3.Connection,
    *,
    history_anchor_ms: int,
    windows: tuple[WindowDef, ...],
) -> dict[str, dict[str, dict[str, Any]]]:
    history_sums_by_exchange: dict[str, dict[str, dict[str, Any]]] = {}
    for exchange, meta in EXCHANGES.items():
        history_sums_by_exchange[exchange] = fetch_cumulative_rates(
            conn,
            history_table=str(meta["history_table"]),
            now_ms=history_anchor_ms,
            allow_partial=bool(meta.get("allow_partial_window_sums")),
            windows=windows,
        )
    return history_sums_by_exchange


def get_history_sums_by_exchange(
    conn: sqlite3.Connection,
    *,
    history_batch_completed_at: int | None,
    history_anchor_ms: int,
    windows: tuple[WindowDef, ...],
) -> dict[str, dict[str, dict[str, Any]]]:
    global _HISTORY_SUMS_CACHE
    global _HISTORY_SUMS_CACHE_ANCHOR_MS
    global _HISTORY_SUMS_CACHE_BATCH_COMPLETED_AT
    global _HISTORY_SUMS_CACHE_WINDOWS_SIGNATURE

    materialized_windows = materialized_windows_for(windows)
    signature = windows_signature(materialized_windows)

    with _HISTORY_SUMS_CACHE_LOCK:
        if (
            _HISTORY_SUMS_CACHE is not None
            and _HISTORY_SUMS_CACHE_BATCH_COMPLETED_AT == history_batch_completed_at
            and _HISTORY_SUMS_CACHE_ANCHOR_MS == history_anchor_ms
            and _HISTORY_SUMS_CACHE_WINDOWS_SIGNATURE == signature
        ):
            return _HISTORY_SUMS_CACHE

    if _materialized_history_summaries_are_current(
        conn,
        history_batch_completed_at=history_batch_completed_at,
        history_anchor_ms=history_anchor_ms,
        windows=materialized_windows,
    ):
        history_sums_by_exchange = _load_materialized_history_summaries(
            conn,
            windows=materialized_windows,
        )
        if not history_sums_by_exchange:
            history_sums_by_exchange = _build_history_sums_by_exchange(
                conn,
                history_anchor_ms=history_anchor_ms,
                windows=materialized_windows,
            )
    else:
        history_sums_by_exchange = _build_history_sums_by_exchange(
            conn,
            history_anchor_ms=history_anchor_ms,
            windows=materialized_windows,
        )
        if history_batch_completed_at is not None:
            _store_materialized_history_summaries(
                conn,
                history_sums_by_exchange=history_sums_by_exchange,
                history_batch_completed_at=history_batch_completed_at,
                history_anchor_ms=history_anchor_ms,
                windows=materialized_windows,
            )
    with _HISTORY_SUMS_CACHE_LOCK:
        if (
            _HISTORY_SUMS_CACHE is None
            or _HISTORY_SUMS_CACHE_BATCH_COMPLETED_AT != history_batch_completed_at
            or _HISTORY_SUMS_CACHE_ANCHOR_MS != history_anchor_ms
            or _HISTORY_SUMS_CACHE_WINDOWS_SIGNATURE != signature
        ):
            _HISTORY_SUMS_CACHE = history_sums_by_exchange
            _HISTORY_SUMS_CACHE_BATCH_COMPLETED_AT = history_batch_completed_at
            _HISTORY_SUMS_CACHE_ANCHOR_MS = history_anchor_ms
            _HISTORY_SUMS_CACHE_WINDOWS_SIGNATURE = signature
        return _HISTORY_SUMS_CACHE


def refresh_materialized_history_summaries(*, force: bool = False) -> bool:
    initialize_dashboard_runtime(apply_legacy_migrations=True)
    with connect_db() as conn:
        prepare_schema(conn)
        _, history_batch_completed_at = fetch_batch_completion_markers(conn)
        if history_batch_completed_at is None:
            return False
        history_anchor_ms = history_batch_completed_at
        windows = materialized_windows_for(WINDOWS)
        if (
            not force
            and _materialized_history_summaries_are_current(
                conn,
                history_batch_completed_at=history_batch_completed_at,
                history_anchor_ms=history_anchor_ms,
                windows=windows,
            )
        ):
            return False
        history_sums_by_exchange = _build_history_sums_by_exchange(
            conn,
            history_anchor_ms=history_anchor_ms,
            windows=windows,
        )
        _store_materialized_history_summaries(
            conn,
            history_sums_by_exchange=history_sums_by_exchange,
            history_batch_completed_at=history_batch_completed_at,
            history_anchor_ms=history_anchor_ms,
            windows=windows,
        )
    with _HISTORY_SUMS_CACHE_LOCK:
        global _HISTORY_SUMS_CACHE
        global _HISTORY_SUMS_CACHE_ANCHOR_MS
        global _HISTORY_SUMS_CACHE_BATCH_COMPLETED_AT
        global _HISTORY_SUMS_CACHE_WINDOWS_SIGNATURE
        _HISTORY_SUMS_CACHE = history_sums_by_exchange
        _HISTORY_SUMS_CACHE_BATCH_COMPLETED_AT = history_batch_completed_at
        _HISTORY_SUMS_CACHE_ANCHOR_MS = history_anchor_ms
        _HISTORY_SUMS_CACHE_WINDOWS_SIGNATURE = windows_signature(windows)
    return True


def _build_payload_uncached(
    *,
    markers: tuple[int | None, int | None] | None = None,
    windows: tuple[WindowDef, ...] = WINDOWS,
    perf: dict[str, float] | None = None,
) -> dict[str, Any]:
    now_ms = int(time.time() * 1000)
    items: list[dict[str, Any]] = []
    prepare_schema_ms = 0.0
    exchange_markers_ms = 0.0
    marker_read_ms = 0.0
    history_sums_ms = 0.0
    base_rows_ms = 0.0
    item_assembly_ms = 0.0
    with connect_db() as conn:
        stage_started = time.perf_counter()
        prepare_schema(conn)
        prepare_schema_ms = (time.perf_counter() - stage_started) * 1000.0

        stage_started = time.perf_counter()
        exchange_updated_at = fetch_exchange_baseinfo_completed_at(conn)
        exchange_markers_ms = (time.perf_counter() - stage_started) * 1000.0

        if markers is None:
            stage_started = time.perf_counter()
            markers = fetch_batch_completion_markers(conn)
            marker_read_ms = (time.perf_counter() - stage_started) * 1000.0
        baseinfo_batch_completed_at, history_batch_completed_at = markers
        history_anchor_ms = history_batch_completed_at or now_ms
        stage_started = time.perf_counter()
        history_sums_by_exchange = get_history_sums_by_exchange(
            conn,
            history_batch_completed_at=history_batch_completed_at,
            history_anchor_ms=history_anchor_ms,
            windows=windows,
        )
        history_sums_ms = (time.perf_counter() - stage_started) * 1000.0
        fallback_exchange_updated_at: dict[str, int] = {}
        for exchange, meta in EXCHANGES.items():
            info_table = _INFO_TABLE_BY_EXCHANGE.get(exchange)
            if info_table is None:
                info_table = resolve_info_table(conn, list(meta["info_table_candidates"]))
                _INFO_TABLE_BY_EXCHANGE[exchange] = info_table
            stage_started = time.perf_counter()
            base_rows = fetch_base_info(conn, info_table)
            base_rows_ms += (time.perf_counter() - stage_started) * 1000.0
            sums_by_symbol = history_sums_by_exchange.get(exchange, {})
            default_sums = {key: None for key, _, _ in windows}
            default_partial = {key: False for key, _, _ in windows}
            default_coverage = {key: None for key, _, _ in windows}

            stage_started = time.perf_counter()
            for row in base_rows:
                sum_bundle = sums_by_symbol.get(
                    row["symbol"],
                    {"values": default_sums, "partial": default_partial, "coverageMs": default_coverage},
                )
                oi = row.get("openInterest")
                mp = row.get("markPrice")
                if bool(meta["open_interest_is_notional"]):
                    notional = oi
                else:
                    notional = oi * mp if oi is not None and mp is not None else None
                multiplier = _to_number(meta.get("open_interest_notional_multiplier")) or 1.0
                if notional is not None:
                    notional *= multiplier
                items.append(
                    {
                        **row,
                        "exchange": exchange,
                        "exchangeLabel": str(meta["label"]),
                        "tradeUrl": trade_page_url(exchange, row["symbol"]),
                        "openInterestNotional": notional,
                        "sums": dict(sum_bundle["values"]),
                        "sumsPartial": dict(sum_bundle["partial"]),
                        "sumsCoverageMs": dict(sum_bundle["coverageMs"]),
                    }
                )
                if row.get("updated_at") is not None:
                    fallback_exchange_updated_at[exchange] = max(
                        fallback_exchange_updated_at.get(exchange, 0),
                        int(row["updated_at"]),
                    )
            item_assembly_ms += (time.perf_counter() - stage_started) * 1000.0
        for exchange, ts in fallback_exchange_updated_at.items():
            exchange_updated_at.setdefault(exchange, ts)

    exchange_freshness = build_exchange_freshness(
        exchange_updated_at,
        baseinfo_batch_completed_at=baseinfo_batch_completed_at,
        now_ms=now_ms,
    )
    if perf is not None:
        perf["prepare_schema_ms"] = round(prepare_schema_ms, 1)
        perf["exchange_markers_ms"] = round(exchange_markers_ms, 1)
        perf["marker_read_ms"] = round(marker_read_ms, 1)
        perf["history_sums_ms"] = round(history_sums_ms, 1)
        perf["base_rows_ms"] = round(base_rows_ms, 1)
        perf["item_assembly_ms"] = round(item_assembly_ms, 1)
        perf["item_count"] = float(len(items))

    return {
        "generatedAt": now_ms,
        "baseinfoBatchCompletedAt": baseinfo_batch_completed_at,
        "historyBatchCompletedAt": history_batch_completed_at,
        "windows": [{"key": key, "label": label, "spanMs": span} for key, span, label in windows],
        "exchanges": [{"key": k, "label": str(v["label"])} for k, v in EXCHANGES.items()],
        "exchangeUpdatedAt": exchange_updated_at,
        "exchangeFreshness": exchange_freshness,
        "items": items,
    }


def build_meta_payload() -> dict[str, Any]:
    started = time.perf_counter()
    with connect_db() as conn:
        marker_started = time.perf_counter()
        baseinfo_batch_completed_at, history_batch_completed_at = fetch_batch_completion_markers(conn)
        marker_read_ms = (time.perf_counter() - marker_started) * 1000.0
        payload = {
            "generatedAt": int(time.time() * 1000),
            "baseinfoBatchCompletedAt": baseinfo_batch_completed_at,
            "historyBatchCompletedAt": history_batch_completed_at,
        }
    _log_if_slow(
        "build_meta_payload",
        (time.perf_counter() - started) * 1000.0,
        SLOW_META_BUILD_MS,
        marker_read_ms=f"{marker_read_ms:.1f}",
    )
    return payload


def _payload_cache_matches_markers(
    markers: tuple[int | None, int | None],
    *,
    windows_sig: tuple[tuple[str, int], ...],
) -> bool:
    if _PAYLOAD_CACHE is None:
        return False
    baseinfo_batch_completed_at, history_batch_completed_at = markers
    if baseinfo_batch_completed_at is None and history_batch_completed_at is None:
        return False
    return (
        _PAYLOAD_CACHE_BASEINFO_BATCH_COMPLETED_AT == baseinfo_batch_completed_at
        and _PAYLOAD_CACHE_HISTORY_BATCH_COMPLETED_AT == history_batch_completed_at
        and _PAYLOAD_CACHE_WINDOWS_SIGNATURE == windows_sig
    )


def _read_payload_cache_markers() -> tuple[int | None, int | None]:
    with connect_db() as conn:
        return fetch_batch_completion_markers(conn)


def build_payload(
    *,
    force_refresh: bool = False,
    force_rebuild: bool = False,
    extra_windows: tuple[WindowDef, ...] | None = None,
) -> dict[str, Any]:
    global _PAYLOAD_BUILD_IN_PROGRESS
    global _PAYLOAD_CACHE
    global _PAYLOAD_CACHE_BASEINFO_BATCH_COMPLETED_AT
    global _PAYLOAD_CACHE_HISTORY_BATCH_COMPLETED_AT
    global _PAYLOAD_CACHE_WINDOWS_SIGNATURE
    global _PAYLOAD_CACHE_ETAG
    global _PAYLOAD_CACHE_JSON
    global _PAYLOAD_CACHE_TS

    windows = merge_windows(extra_windows)
    windows_sig = windows_signature(windows)
    markers: tuple[int | None, int | None] | None = None
    while True:
        now = time.monotonic()
        with _PAYLOAD_CACHE_COND:
            if (
                not force_refresh
                and not force_rebuild
                and _PAYLOAD_CACHE is not None
                and now - _PAYLOAD_CACHE_TS < PAYLOAD_CACHE_TTL_SEC
                and _PAYLOAD_CACHE_WINDOWS_SIGNATURE == windows_sig
            ):
                return _PAYLOAD_CACHE
            if (
                not force_rebuild
                and markers is not None
                and _payload_cache_matches_markers(markers, windows_sig=windows_sig)
            ):
                return _PAYLOAD_CACHE
            if _PAYLOAD_BUILD_IN_PROGRESS:
                _PAYLOAD_CACHE_COND.wait(timeout=1.0)
                markers = None
                continue

        if markers is None:
            markers = _read_payload_cache_markers()

        with _PAYLOAD_CACHE_COND:
            now = time.monotonic()
            if (
                not force_refresh
                and not force_rebuild
                and _PAYLOAD_CACHE is not None
                and now - _PAYLOAD_CACHE_TS < PAYLOAD_CACHE_TTL_SEC
                and _PAYLOAD_CACHE_WINDOWS_SIGNATURE == windows_sig
            ):
                return _PAYLOAD_CACHE
            if not force_rebuild and _payload_cache_matches_markers(markers, windows_sig=windows_sig):
                return _PAYLOAD_CACHE
            if _PAYLOAD_BUILD_IN_PROGRESS:
                _PAYLOAD_CACHE_COND.wait(timeout=1.0)
                markers = None
                continue
            _PAYLOAD_BUILD_IN_PROGRESS = True
            break

    build_started = time.perf_counter()
    perf: dict[str, float] = {}
    try:
        payload = _build_payload_uncached(markers=markers, windows=windows, perf=perf)
        encode_started = time.perf_counter()
        payload_json = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        perf["json_encode_ms"] = round((time.perf_counter() - encode_started) * 1000.0, 1)
        payload_etag = _etag_for_bytes(payload_json)
    except Exception:
        with _PAYLOAD_CACHE_COND:
            _PAYLOAD_BUILD_IN_PROGRESS = False
            _PAYLOAD_CACHE_COND.notify_all()
        raise

    total_build_ms = (time.perf_counter() - build_started) * 1000.0
    _log_if_slow(
        "build_payload",
        total_build_ms,
        SLOW_PAYLOAD_BUILD_MS,
        force_refresh=int(force_refresh),
        force_rebuild=int(force_rebuild),
        windows=",".join(key for key, _, _ in windows),
        items=int(perf.get("item_count", 0)),
        prepare_schema_ms=f"{perf.get('prepare_schema_ms', 0.0):.1f}",
        exchange_markers_ms=f"{perf.get('exchange_markers_ms', 0.0):.1f}",
        marker_read_ms=f"{perf.get('marker_read_ms', 0.0):.1f}",
        history_sums_ms=f"{perf.get('history_sums_ms', 0.0):.1f}",
        base_rows_ms=f"{perf.get('base_rows_ms', 0.0):.1f}",
        item_assembly_ms=f"{perf.get('item_assembly_ms', 0.0):.1f}",
        json_encode_ms=f"{perf.get('json_encode_ms', 0.0):.1f}",
    )

    with _PAYLOAD_CACHE_COND:
        _PAYLOAD_CACHE = payload
        _PAYLOAD_CACHE_JSON = payload_json
        _PAYLOAD_CACHE_TS = time.monotonic()
        _PAYLOAD_CACHE_BASEINFO_BATCH_COMPLETED_AT = payload.get("baseinfoBatchCompletedAt")
        _PAYLOAD_CACHE_HISTORY_BATCH_COMPLETED_AT = payload.get("historyBatchCompletedAt")
        _PAYLOAD_CACHE_WINDOWS_SIGNATURE = windows_sig
        _PAYLOAD_CACHE_ETAG = payload_etag
        _PAYLOAD_BUILD_IN_PROGRESS = False
        _PAYLOAD_CACHE_COND.notify_all()
        return payload


def build_payload_json(
    *,
    force_refresh: bool = False,
    force_rebuild: bool = False,
    extra_windows: tuple[WindowDef, ...] | None = None,
) -> bytes:
    payload = build_payload(
        force_refresh=force_refresh,
        force_rebuild=force_rebuild,
        extra_windows=extra_windows,
    )
    with _PAYLOAD_CACHE_COND:
        if payload is _PAYLOAD_CACHE and _PAYLOAD_CACHE_JSON is not None:
            return _PAYLOAD_CACHE_JSON
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def build_payload_json_and_etag(
    *,
    force_refresh: bool = False,
    force_rebuild: bool = False,
    extra_windows: tuple[WindowDef, ...] | None = None,
) -> tuple[bytes, str]:
    payload = build_payload(
        force_refresh=force_refresh,
        force_rebuild=force_rebuild,
        extra_windows=extra_windows,
    )
    with _PAYLOAD_CACHE_COND:
        if (
            payload is _PAYLOAD_CACHE
            and _PAYLOAD_CACHE_JSON is not None
            and _PAYLOAD_CACHE_ETAG is not None
        ):
            return _PAYLOAD_CACHE_JSON, _PAYLOAD_CACHE_ETAG
    payload_json = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    return payload_json, _etag_for_bytes(payload_json)


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
  <title>资金费率</title>
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
      gap: 16px;
      margin-bottom: 22px;
    }}
    .hero {{
      display: flex;
      align-items: center;
      gap: 12px;
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
    .header-controls {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      flex: 1;
      justify-content: flex-end;
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
    .exchange-choice.abnormal {{
      background: rgba(239, 68, 68, 0.06);
      border-color: rgba(239, 68, 68, 0.22);
    }}
    .exchange-choice.abnormal.active {{
      background: rgba(239, 68, 68, 0.1);
      border-color: rgba(239, 68, 68, 0.3);
    }}
    .exchange-choice.abnormal .choice-text,
    .exchange-choice.abnormal .choice-count,
    .exchange-choice.abnormal .choice-meta {{
      color: #fbc5c5;
    }}
    .exchange-main {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      cursor: pointer;
    }}
    .exchange-copy {{
      display: inline-flex;
      flex-direction: column;
      align-items: flex-start;
      gap: 4px;
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
    .exchange-choice .choice-meta {{
      color: var(--muted);
      font-size: 11px;
      line-height: 1.2;
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
    .blacklist-pill {{
      flex-direction: row;
      align-items: center;
      gap: 10px;
      min-width: 0;
      flex-wrap: wrap;
    }}
    .blacklist-title {{
      display: flex;
      align-items: baseline;
      gap: 6px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1;
      white-space: nowrap;
    }}
    .blacklist-title strong {{
      color: var(--text);
      font-family: 'Menlo', 'SFMono-Regular', Consolas, monospace;
      font-size: 14px;
    }}
    .blacklist-tools {{
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
    }}
    .tool-actions {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }}
    .blacklist-toggle {{
      gap: 0;
      min-height: 28px;
    }}
    .toggle-inline {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      color: var(--text);
      font-size: 13px;
      cursor: pointer;
      user-select: none;
    }}
    .toggle-inline input {{
      width: 14px;
      height: 14px;
      margin: 0;
      accent-color: var(--accent);
    }}
    .sr-only {{
      position: absolute;
      width: 1px;
      height: 1px;
      padding: 0;
      margin: -1px;
      overflow: hidden;
      clip: rect(0, 0, 0, 0);
      white-space: nowrap;
      border: 0;
    }}
    .ghost-btn,
    .row-toggle-btn {{
      appearance: none;
      border: 1px solid var(--border);
      background: rgba(255,255,255,0.04);
      color: var(--text);
      border-radius: 8px;
      font-size: 12px;
      line-height: 1;
      cursor: pointer;
      transition: background 0.15s ease, border-color 0.15s ease, color 0.15s ease;
    }}
    .ghost-btn {{
      padding: 7px 10px;
    }}
    .ghost-btn:disabled {{
      cursor: default;
      opacity: 0.45;
    }}
    .ghost-btn:not(:disabled):hover,
    .row-toggle-btn:hover {{
      background: rgba(34,211,238,0.12);
      border-color: rgba(34,211,238,0.45);
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
    .symbol-link {{
      color: inherit;
      text-decoration: none;
      transition: background 140ms ease, border-color 140ms ease, transform 140ms ease;
    }}
    .symbol-link:hover {{
      background: rgba(34,211,238,0.1);
      border-color: rgba(34,211,238,0.38);
      transform: translateY(-1px);
    }}
    .stack {{
      display: flex;
      flex-direction: column;
      align-items: flex-end;
      gap: 2px;
    }}
    .symbol-cell {{
      display: flex;
      align-items: center;
      gap: 8px;
      min-width: 0;
    }}
    .row-toggle-btn {{
      padding: 5px 8px;
      flex: none;
      white-space: nowrap;
    }}
    .row-toggle-btn.active {{
      color: var(--danger);
      border-color: rgba(239, 68, 68, 0.35);
      background: rgba(239, 68, 68, 0.08);
    }}
    tbody tr.row-blacklisted {{
      opacity: 0.55;
    }}
    .modal-backdrop {{
      position: fixed;
      inset: 0;
      background: rgba(6, 10, 18, 0.72);
      backdrop-filter: blur(8px);
      display: none;
      align-items: center;
      justify-content: center;
      padding: 24px;
      z-index: 30;
    }}
    .modal-backdrop.open {{
      display: flex;
    }}
    .modal-panel {{
      width: min(920px, 100%);
      max-height: min(82vh, 900px);
      overflow: auto;
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 18px;
      box-shadow: 0 30px 80px rgba(0,0,0,0.42);
      padding: 18px 18px 16px;
    }}
    .modal-header {{
      position: relative;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 16px;
      margin-bottom: 14px;
      min-height: 40px;
    }}
    .modal-title {{
      display: flex;
      flex-direction: column;
      gap: 4px;
      align-items: center;
      text-align: center;
    }}
    .modal-title h2 {{
      margin: 0;
      font-size: 18px;
    }}
    .modal-title span {{
      color: var(--muted);
      font-size: 12px;
    }}
    .modal-actions {{
      position: absolute;
      right: 0;
      top: 0;
      display: inline-flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }}
    .modal-table {{
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
    }}
    .modal-table th,
    .modal-table td {{
      padding: 10px 8px;
      border-bottom: 1px solid var(--border);
      font-size: 13px;
      vertical-align: middle;
    }}
    .modal-table th {{
      color: var(--muted);
      text-align: left;
      font-weight: 600;
      letter-spacing: 0.02em;
      text-transform: none;
      position: static;
      backdrop-filter: none;
    }}
    .note-input {{
      width: 100%;
      height: 34px;
      border: 1px solid var(--border);
      border-radius: 8px;
      background: rgba(255,255,255,0.03);
      color: var(--text);
      padding: 0 10px;
      font-size: 13px;
      box-sizing: border-box;
      outline: none;
    }}
    .note-input:focus {{
      border-color: rgba(34,211,238,0.5);
      box-shadow: 0 0 0 3px rgba(34,211,238,0.12);
    }}
    .empty-blacklist {{
      padding: 28px 8px 12px;
      color: var(--muted);
      font-size: 13px;
      text-align: center;
    }}
    .row-actions {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
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
      .hero {{ align-items: flex-start; }}
      .header-controls {{ width: 100%; justify-content: flex-start; }}
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
        <div class="header-controls">
          <div class="pill">
            <label for="searchBox">搜索</label>
            <input id="searchBox" type="text" placeholder="BTC || BTC/" />
          </div>
          <div class="pill range-pill">
            <div class="range-title">持仓量 &gt; <span id="oiFilterLabel">3</span>M</div>
            <input id="oiFilter" type="range" min="0" max="7" step="1" value="2" />
          </div>
          <div class="pill blacklist-pill">
            <div class="blacklist-title">黑名单 <strong id="blacklistCount">0</strong></div>
            <div class="blacklist-tools">
              <label class="toggle-inline blacklist-toggle" title="显示已隐藏">
                <input id="showBlacklisted" type="checkbox" aria-label="显示已隐藏" />
                <span class="sr-only">显示已隐藏</span>
              </label>
              <div class="tool-actions">
                <button id="viewBlacklistBtn" class="ghost-btn" type="button">查看黑名单</button>
                <button id="syncAlertBlacklistBtn" class="ghost-btn" type="button">同步通知</button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </header>

    <div class="controls">
      <div class="pill exchange-pill">
        <div class="exchange-group" id="exchangeChoices"></div>
      </div>
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
  <div class="modal-backdrop" id="blacklistModal">
    <div class="modal-panel">
      <div class="modal-header">
        <div class="modal-title">
          <h2>黑名单</h2>
          <span>仅影响你当前浏览器；备注也只保存在本机</span>
        </div>
        <div class="modal-actions">
          <button id="modalClearBlacklistBtn" class="ghost-btn" type="button">清空黑名单</button>
          <button id="closeBlacklistBtn" class="ghost-btn" type="button">关闭</button>
        </div>
      </div>
      <div id="blacklistModalBody"></div>
    </div>
  </div>

  <script>
    const STATIC_PAYLOAD = {static_payload_json};
    const WINDOWS_META = {windows_meta};
    const EXCHANGES_META = {exchanges_meta};
    const WINDOW_KEYS = WINDOWS_META.map(w => w.key);
    const WINDOW_DAYS_BY_KEY = Object.fromEntries(WINDOWS_META.map(w => [w.key, w.spanMs / 86400000]));
    const STR_COLLATOR = new Intl.Collator(undefined, {{ numeric: true, sensitivity: 'base' }});
    const RENDER_CHUNK_SIZE = 180;
    const BLACKLIST_STORAGE_KEY = 'funding_dashboard_blacklist_v1';
    const BLACKLIST_NOTES_STORAGE_KEY = 'funding_dashboard_blacklist_notes_v1';
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
    const META_POLL_INTERVAL_MS = 60000;

    let dataCache = [];
    let sortKey = 'symbol';
    let sortDir = 'asc';
    let oiThresholdIdx = 2;
    let selectedExchanges = new Set(EXCHANGES_META.map(x => x.key));
    let blacklistedRowKeys = new Set();
    let blacklistNotesByKey = {{}};
    let showBlacklisted = false;
    let exchangeCounts = {{}};
    let exchangeUpdatedAt = {{}};
    let exchangeFreshness = {{}};
    let fixedColumnsApplied = false;
    let renderVersion = 0;
    let scheduledRender = null;
    let pendingRenderPreferFullReplace = false;
    let baseinfoBatchCompletedAt = null;
    let dataLoadInFlight = false;
    let metaPollInFlight = false;
    let autoRefreshHandle = null;

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
        const rowKey = `${'{'}String(item.exchange || '').toLowerCase(){'}'}::${'{'}rawSymbol.toUpperCase(){'}'}`;
        return {{
          ...item,
          _rawSym: rawSymbol.toUpperCase(),
          _displaySym: display,
          _sortSym: display.toUpperCase(),
          _sortExchange: String(item.exchangeLabel || item.exchange || ''),
          _rowKey: rowKey,
        }};
      }});
    }}

    function loadBlacklistedRowKeys() {{
      try {{
        const raw = window.localStorage.getItem(BLACKLIST_STORAGE_KEY);
        if (!raw) return new Set();
        const parsed = JSON.parse(raw);
        if (!Array.isArray(parsed)) return new Set();
        return new Set(parsed.filter(value => typeof value === 'string' && value));
      }} catch (_err) {{
        return new Set();
      }}
    }}

    function persistBlacklistedRowKeys() {{
      try {{
        window.localStorage.setItem(BLACKLIST_STORAGE_KEY, JSON.stringify(Array.from(blacklistedRowKeys).sort()));
      }} catch (_err) {{
        return;
      }}
    }}

    function loadBlacklistNotesByKey() {{
      try {{
        const raw = window.localStorage.getItem(BLACKLIST_NOTES_STORAGE_KEY);
        if (!raw) return {{}};
        const parsed = JSON.parse(raw);
        if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return {{}};
        const out = {{}};
        Object.entries(parsed).forEach(([rowKey, note]) => {{
          const normalized = typeof rowKey === 'string' ? rowKey : '';
          if (!normalized || typeof note !== 'string') return;
          out[normalized] = note;
        }});
        return out;
      }} catch (_err) {{
        return {{}};
      }}
    }}

    function persistBlacklistNotesByKey() {{
      try {{
        const payload = {{}};
        Object.keys(blacklistNotesByKey).sort().forEach(rowKey => {{
          const note = String(blacklistNotesByKey[rowKey] || '').trim();
          if (!note) return;
          payload[rowKey] = note;
        }});
        window.localStorage.setItem(BLACKLIST_NOTES_STORAGE_KEY, JSON.stringify(payload));
      }} catch (_err) {{
        return;
      }}
    }}

    function updateFooterText() {{
      const footer = document.getElementById('footerText');
      if (!footer) return;
      let text = '历史数据 1 小时更新一次，其他数据 10 分钟更新一次';
      if (blacklistedRowKeys.size) {{
        text += ` · 黑名单 ${'{'}blacklistedRowKeys.size{'}'} 项`;
      }}
      footer.textContent = text;
    }}

    function describeRowKey(rowKey) {{
      const [exchange, symbol] = String(rowKey || '').split('::');
      return {{
        exchange: exchangeLabel(exchange || ''),
        symbol: symbol || '',
      }};
    }}

    function renderBlacklistModal() {{
      const body = document.getElementById('blacklistModalBody');
      if (!body) return;
      const rowKeys = Array.from(blacklistedRowKeys).sort((a, b) => STR_COLLATOR.compare(a, b));
      if (!rowKeys.length) {{
        body.innerHTML = '<div class="empty-blacklist">当前没有黑名单标的</div>';
        return;
      }}
      body.innerHTML = `
        <table class="modal-table">
          <thead>
            <tr>
              <th style="width: 22%;">交易所</th>
              <th style="width: 24%;">标的</th>
              <th>备注</th>
              <th style="width: 72px;">操作</th>
            </tr>
          </thead>
          <tbody>
            ${'{'}rowKeys.map(rowKey => {{
              const entry = describeRowKey(rowKey);
              const note = String(blacklistNotesByKey[rowKey] || '');
              return `
                <tr>
                  <td>${'{'}entry.exchange{'}'}</td>
                  <td><span class="tag">${'{'}entry.symbol{'}'}</span></td>
                  <td>
                    <input class="note-input" data-note-key="${'{'}rowKey{'}'}" type="text" value="${'{'}note{'}'}" placeholder="备注（仅本机可见）" />
                  </td>
                  <td>
                    <div class="row-actions">
                      <button type="button" class="ghost-btn" data-remove-blacklist-key="${'{'}rowKey{'}'}">移出</button>
                    </div>
                  </td>
                </tr>
              `;
            }}).join(''){'}'}
          </tbody>
        </table>
      `;
    }}

    function openBlacklistModal() {{
      renderBlacklistModal();
      const modal = document.getElementById('blacklistModal');
      if (modal) modal.classList.add('open');
    }}

    function closeBlacklistModal() {{
      const modal = document.getElementById('blacklistModal');
      if (modal) modal.classList.remove('open');
    }}

    function updateBlacklistControls() {{
      const countEl = document.getElementById('blacklistCount');
      const viewEl = document.getElementById('viewBlacklistBtn');
      const modalClearEl = document.getElementById('modalClearBlacklistBtn');
      if (countEl) countEl.textContent = `${'{'}blacklistedRowKeys.size{'}'}`;
      if (viewEl) viewEl.disabled = false;
      if (modalClearEl) modalClearEl.disabled = blacklistedRowKeys.size === 0;
      updateFooterText();
    }}

    async function syncAlertBlacklistToServer() {{
      const token = window.prompt('输入服务器设置的管理口令，将当前黑名单同步到服务器通知黑名单。');
      if (token == null) return false;
      const trimmed = String(token || '').trim();
      if (!trimmed) return false;
      try {{
        const res = await fetch('/api/alert-blacklist', {{
          method: 'POST',
          cache: 'no-store',
          headers: {{
            'Content-Type': 'application/json',
            'X-Dashboard-Admin-Token': trimmed,
          }},
          body: JSON.stringify({{ replaceAll: true, rowKeys: Array.from(blacklistedRowKeys).sort() }}),
        }});
        if (res.status === 401) {{
          window.alert('管理口令无效，未同步到服务器。');
          return false;
        }}
        if (!res.ok) {{
          const payload = await res.json().catch(() => ({{}}));
          window.alert(`同步通知失败：${'{'}payload.error || res.status{'}'}`);
          return false;
        }}
        window.alert('黑名单已同步到服务器通知黑名单。');
        return true;
      }} catch (err) {{
        window.alert(`同步通知失败：${'{'}err.message{'}'}`);
        return false;
      }}
    }}

    async function toggleBlacklistedRowKey(rowKey) {{
      if (!rowKey) return;
      if (blacklistedRowKeys.has(rowKey)) {{
        blacklistedRowKeys.delete(rowKey);
        delete blacklistNotesByKey[rowKey];
      }} else {{
        blacklistedRowKeys.add(rowKey);
      }}
      persistBlacklistedRowKeys();
      persistBlacklistNotesByKey();
      updateBlacklistControls();
      render();
      if (document.getElementById('blacklistModal')?.classList.contains('open')) {{
        renderBlacklistModal();
      }}
    }}

    async function clearBlacklistedRowKeys() {{
      if (!blacklistedRowKeys.size) return;
      blacklistedRowKeys = new Set();
      blacklistNotesByKey = {{}};
      persistBlacklistedRowKeys();
      persistBlacklistNotesByKey();
      updateBlacklistControls();
      render({{ preferFullReplace: true }});
      renderBlacklistModal();
    }}

    function updateBlacklistNote(rowKey, note) {{
      const normalized = String(rowKey || '').trim();
      if (!normalized) return;
      const value = String(note || '').trim();
      if (!blacklistedRowKeys.has(normalized)) return;
      if (value) {{
        blacklistNotesByKey[normalized] = value;
      }} else {{
        delete blacklistNotesByKey[normalized];
      }}
      persistBlacklistNotesByKey();
    }}

    function allExchangesSelected() {{
      return selectedExchanges.size === EXCHANGES_META.length;
    }}

    function formatExchangeUpdatedAt(ts) {{
      if (!ts) return '—';
      const d = new Date(ts);
      const mm = String(d.getMonth() + 1).padStart(2, '0');
      const dd = String(d.getDate()).padStart(2, '0');
      const hh = String(d.getHours()).padStart(2, '0');
      const mi = String(d.getMinutes()).padStart(2, '0');
      return `${'{'}mm{'}'}${'{'}dd{'}'} ${'{'}hh{'}'}:${'{'}mi{'}'}`;
    }}

    function normalizeTimestamp(ts) {{
      const num = Number(ts);
      return Number.isFinite(num) && num > 0 ? num : null;
    }}

    function renderExchangeChoices() {{
      const group = document.getElementById('exchangeChoices');
      if (!group) return;
      const totalCount = dataCache.length;
      const items = [
        {{ key: 'all', label: '全部', count: totalCount, checked: allExchangesSelected(), meta: '', abnormal: false }},
        ...EXCHANGES_META.map(x => ({{
          key: x.key,
          label: x.label,
          count: exchangeCounts[x.key] || 0,
          checked: selectedExchanges.has(x.key),
          meta: formatExchangeUpdatedAt(exchangeUpdatedAt[x.key]),
          abnormal: !!(exchangeFreshness[x.key] && exchangeFreshness[x.key].status && exchangeFreshness[x.key].status !== 'fresh'),
        }})),
      ];
      group.innerHTML = items.map(item => `
        <div class="exchange-choice ${'{'}item.checked ? 'active' : ''{'}'} ${'{'}item.abnormal ? 'abnormal' : ''{'}'}" data-key="${'{'}item.key{'}'}">
          <label class="exchange-main">
            <input type="checkbox" ${'{'}item.checked ? 'checked' : ''{'}'} />
            <span class="exchange-copy">
              <span>
                <span class="choice-text">${'{'}item.label{'}'}</span>
                <span class="choice-count">${'{'}item.count{'}'}</span>
              </span>
              ${'{'}item.meta ? `<span class="choice-meta">${'{'}item.meta{'}'}</span>` : ''{'}'}
            </span>
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

    function applyPayload(payload, {{ preferFullReplace = false }} = {{}}) {{
      dataCache = prepareItems(payload.items || []);
      baseinfoBatchCompletedAt = normalizeTimestamp(payload.baseinfoBatchCompletedAt);
      updateMeta(payload);
      render({{ preferFullReplace }});
    }}

    async function load({{ forceRefresh = false, preferFullReplace = false }} = {{}}) {{
      if (STATIC_PAYLOAD) {{
        applyPayload(STATIC_PAYLOAD, {{ preferFullReplace }});
        return;
      }}
      if (dataLoadInFlight) return;
      dataLoadInFlight = true;
      try {{
        const url = forceRefresh ? '/api/data?refresh=1' : '/api/data';
        const res = await fetch(url, {{ cache: 'no-store' }});
        if (!res.ok) throw new Error('数据获取失败');
        const payload = await res.json();
        applyPayload(payload, {{ preferFullReplace }});
      }} finally {{
        dataLoadInFlight = false;
      }}
    }}

    function updateMeta(payload) {{
      const counts = {{}};
      const updatedMap = {{ ...(payload.exchangeUpdatedAt || {{}}) }};
      exchangeFreshness = {{ ...(payload.exchangeFreshness || {{}}) }};
      dataCache.forEach(it => counts[it.exchange] = (counts[it.exchange] || 0) + 1);
      dataCache.forEach(it => {{
        if (it.updated_at == null) return;
        if (updatedMap[it.exchange] == null) {{
          updatedMap[it.exchange] = Number(it.updated_at);
        }} else if (!(payload.exchangeUpdatedAt && Object.prototype.hasOwnProperty.call(payload.exchangeUpdatedAt, it.exchange))) {{
          updatedMap[it.exchange] = Math.max(updatedMap[it.exchange], Number(it.updated_at));
        }}
      }});
      exchangeCounts = counts;
      exchangeUpdatedAt = updatedMap;
      updateFooterText();
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
      const isBlacklisted = blacklistedRowKeys.has(item._rowKey);
      const sums = item.sums || {{}};
      const sumsPartial = item.sumsPartial || {{}};
      const sumsCoverageMs = item.sumsCoverageMs || {{}};
      const symbolText = item._displaySym || displaySymbol(item.symbol);
      const symbolTag = item.tradeUrl
        ? `<a class="tag symbol-link" href="${'{'}item.tradeUrl{'}'}" target="_blank" rel="noopener noreferrer" title="打开 ${'{'}symbolText{'}'} 交易页">${'{'}symbolText{'}'}</a>`
        : `<span class="tag">${'{'}symbolText{'}'}</span>`;
      const windowCells = WINDOW_KEYS.map(key => {{
        const v = sums[key];
        const partial = !!sumsPartial[key];
        const coverageMs = sumsCoverageMs[key];
        const observedDays = coverageMs == null ? null : coverageMs / 86_400_000;
        const fallbackDays = WINDOW_DAYS_BY_KEY[key] || null;
        const annDays = observedDays && observedDays > 0 ? observedDays : fallbackDays;
        const ann = annDays && v != null ? (v / annDays) * 365 : null;
        const valueText = partial && v != null ? `~${'{'}toPct(v){'}'}` : toPct(v);
        const aprText = partial && ann != null ? `APR ${'{'}toPctFixed(ann, 2){'}'} · 已观测` : `APR ${'{'}toPctFixed(ann, 2){'}'}`;
        return `<td class="num"><div class="stack"><span class="${'{'}classFor(v){'}'}">${'{'}valueText{'}'}</span><span class="mini ${'{'}classFor(ann){'}'}">${'{'}aprText{'}'}</span></div></td>`;
      }}).join('');
      const notionalDisplay = item.openInterestNotional == null ? null : item.openInterestNotional / 1_000_000;
      const insuranceDisplay = item.insuranceBalance == null ? null : item.insuranceBalance / 1_000_000;
      const latestAnn = item.fundingIntervalHours ? (item.lastFundingRate ?? 0) * (24 / item.fundingIntervalHours) * 365 : null;
      const rowClass = isBlacklisted ? 'row-blacklisted' : '';
      const blacklistActionLabel = isBlacklisted ? '恢复' : '拉黑';
      const blacklistActionTitle = isBlacklisted ? '移出黑名单' : '加入黑名单';
      return `
        <tr class="${'{'}rowClass{'}'}">
          <td><span class="tag">${'{'}exchangeLabel(item.exchange){'}'}</span></td>
          <td>
            <div class="symbol-cell">
              ${'{'}symbolTag{'}'}
              <button
                type="button"
                class="row-toggle-btn ${'{'}isBlacklisted ? 'active' : ''{'}'}"
                data-blacklist-key="${'{'}item._rowKey{'}'}"
                title="${'{'}blacklistActionTitle{'}'}"
              >${'{'}blacklistActionLabel{'}'}</button>
            </div>
          </td>
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

    function renderNow({{ preferFullReplace = false }} = {{}}) {{
      const body = document.getElementById('table-body');
      renderVersion += 1;
      const version = renderVersion;
      const rawQuery = document.getElementById('searchBox').value.trim().toUpperCase();
      const exactSearch = rawQuery.endsWith('/');
      const q = exactSearch ? rawQuery.slice(0, -1).trim() : rawQuery;
      const threshold = OI_THRESHOLDS[oiThresholdIdx] * 1_000_000;
      const filtered = dataCache.filter(item => {{
        const isBlacklisted = blacklistedRowKeys.has(item._rowKey);
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
        return hitExchange && hitSymbol && hitOi && !hideDefaultZeroOiGrvt && (!isBlacklisted || showBlacklisted);
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
        body.innerHTML = '<tr><td colspan="99" class="dim">暂无可显示数据：可能被筛选条件或黑名单隐藏</td></tr>';
        updateSortIndicators();
        return;
      }}

      updateSortIndicators();
      if (preferFullReplace) {{
        fixedColumnsApplied = false;
        body.innerHTML = sorted.map(renderRow).join('');
        applyFixedColumnWidths();
        return;
      }}
      body.innerHTML = '';
      appendRowsInChunks(body, sorted, version);
    }}

    function render({{ preferFullReplace = false }} = {{}}) {{
      pendingRenderPreferFullReplace = pendingRenderPreferFullReplace || preferFullReplace;
      if (scheduledRender) cancelAnimationFrame(scheduledRender);
      scheduledRender = requestAnimationFrame(() => {{
        const useFullReplace = pendingRenderPreferFullReplace;
        pendingRenderPreferFullReplace = false;
        scheduledRender = null;
        renderNow({{ preferFullReplace: useFullReplace }});
      }});
    }}

    async function pollMetaAndRefresh() {{
      if (STATIC_PAYLOAD || document.hidden || metaPollInFlight) return;
      metaPollInFlight = true;
      try {{
        const res = await fetch('/api/meta', {{ cache: 'no-store' }});
        if (!res.ok) return;
        const payload = await res.json();
        const nextCompletedAt = normalizeTimestamp(payload.baseinfoBatchCompletedAt);
        if (nextCompletedAt == null || nextCompletedAt === baseinfoBatchCompletedAt) return;
        await load({{ forceRefresh: true, preferFullReplace: true }});
      }} catch (_err) {{
        return;
      }} finally {{
        metaPollInFlight = false;
      }}
    }}

    function startAutoRefresh() {{
      if (STATIC_PAYLOAD || autoRefreshHandle) return;
      autoRefreshHandle = setInterval(() => {{
        pollMetaAndRefresh();
      }}, META_POLL_INTERVAL_MS);
      document.addEventListener('visibilitychange', () => {{
        if (!document.hidden) {{
          pollMetaAndRefresh();
        }}
      }});
    }}

    let searchDebounce = null;
    document.getElementById('searchBox').addEventListener('input', () => {{
      if (searchDebounce) clearTimeout(searchDebounce);
      searchDebounce = setTimeout(render, 80);
    }});
    document.getElementById('table-body').addEventListener('click', (event) => {{
      const button = event.target.closest('[data-blacklist-key]');
      if (!button) return;
      void toggleBlacklistedRowKey(button.dataset.blacklistKey || '');
    }});
    document.getElementById('blacklistModalBody').addEventListener('click', (event) => {{
      const button = event.target.closest('[data-remove-blacklist-key]');
      if (!button) return;
      void toggleBlacklistedRowKey(button.dataset.removeBlacklistKey || '');
    }});
    document.getElementById('blacklistModalBody').addEventListener('change', (event) => {{
      const input = event.target.closest('[data-note-key]');
      if (!input) return;
      updateBlacklistNote(input.dataset.noteKey || '', input.value || '');
    }});
    document.getElementById('blacklistModalBody').addEventListener('focusout', (event) => {{
      const input = event.target.closest('[data-note-key]');
      if (!input) return;
      updateBlacklistNote(input.dataset.noteKey || '', input.value || '');
    }});
    document.getElementById('blacklistModalBody').addEventListener('keydown', (event) => {{
      const input = event.target.closest('[data-note-key]');
      if (!input || event.key !== 'Enter') return;
      event.preventDefault();
      updateBlacklistNote(input.dataset.noteKey || '', input.value || '');
      input.blur();
    }});
    document.getElementById('viewBlacklistBtn').addEventListener('click', () => {{
      openBlacklistModal();
    }});
    document.getElementById('syncAlertBlacklistBtn').addEventListener('click', () => {{
      void syncAlertBlacklistToServer();
    }});
    document.getElementById('closeBlacklistBtn').addEventListener('click', () => {{
      closeBlacklistModal();
    }});
    document.getElementById('modalClearBlacklistBtn').addEventListener('click', () => {{
      void clearBlacklistedRowKeys();
    }});
    document.getElementById('blacklistModal').addEventListener('click', (event) => {{
      if (event.target?.id === 'blacklistModal') {{
        closeBlacklistModal();
      }}
    }});
    window.addEventListener('keydown', (event) => {{
      if (event.key === 'Escape') {{
        closeBlacklistModal();
      }}
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
    document.getElementById('showBlacklisted').addEventListener('change', (event) => {{
      showBlacklisted = !!event.target.checked;
      render({{ preferFullReplace: true }});
    }});

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
    blacklistedRowKeys = loadBlacklistedRowKeys();
    blacklistNotesByKey = loadBlacklistNotesByKey();
    updateBlacklistControls();

    startAutoRefresh();
    load().catch(err => {{
      document.getElementById('table-body').innerHTML = `<tr><td colspan="99" class="dim">加载失败：${'{'}err.message{'}'}</td></tr>`;
    }});
  </script>
</body>
</html>
"""


class DashboardHandler(BaseHTTPRequestHandler):
    def handle_one_request(self) -> None:
        try:
            super().handle_one_request()
        except CLIENT_DISCONNECT_ERRORS:
            self.close_connection = True

    def _client_accepts_gzip(self) -> bool:
        return "gzip" in str(self.headers.get("Accept-Encoding") or "").lower()

    def _send_bytes(
        self,
        data: bytes,
        *,
        status: int,
        content_type: str,
        cache_control: str | None = None,
        etag: str | None = None,
        allow_compression: bool = True,
    ) -> None:
        try:
            if etag is not None and str(self.headers.get("If-None-Match") or "").strip() == etag:
                self.send_response(304)
                self.send_header("Content-Type", content_type)
                if cache_control is not None:
                    self.send_header("Cache-Control", cache_control)
                self.send_header("ETag", etag)
                if allow_compression:
                    self.send_header("Vary", "Accept-Encoding")
                self.end_headers()
                self._last_response_status = 304
                self._last_response_size = 0
                self._last_response_compressed = False
                return

            compressed = False
            body = data
            if allow_compression and len(data) >= GZIP_MIN_BYTES and self._client_accepts_gzip():
                body = gzip.compress(data, compresslevel=GZIP_COMPRESSLEVEL)
                compressed = True

            self.send_response(status)
            self.send_header("Content-Type", content_type)
            if cache_control is not None:
                self.send_header("Cache-Control", cache_control)
            if etag is not None:
                self.send_header("ETag", etag)
            if allow_compression:
                self.send_header("Vary", "Accept-Encoding")
            if compressed:
                self.send_header("Content-Encoding", "gzip")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            self._last_response_status = status
            self._last_response_size = len(body)
            self._last_response_compressed = compressed
        except CLIENT_DISCONNECT_ERRORS:
            self.close_connection = True

    def _send_json(self, payload: dict[str, Any], status: int = 200, *, etag: str | None = None) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self._send_json_bytes(data, status=status, etag=etag)

    def _send_json_bytes(self, data: bytes, *, status: int = 200, etag: str | None = None) -> None:
        self._send_bytes(
            data,
            status=status,
            content_type="application/json; charset=utf-8",
            cache_control="no-store",
            etag=etag,
        )

    def _send_html(self, content: str) -> None:
        data = content.encode("utf-8")
        self._send_bytes(
            data,
            status=200,
            content_type="text/html; charset=utf-8",
            cache_control="no-cache",
            etag=_etag_for_bytes(data),
        )

    def _dashboard_admin_authorized(self) -> tuple[bool, str]:
        configured = load_dashboard_admin_token()
        if not configured:
            return False, "管理口令未配置"
        provided = str(self.headers.get("X-Dashboard-Admin-Token") or "").strip()
        if not provided:
            return False, "缺少管理口令"
        if not hmac.compare_digest(provided, configured):
            return False, "管理口令无效"
        return True, ""

    def _read_json_body(self) -> dict[str, Any]:
        length = int(str(self.headers.get("Content-Length") or "0") or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(min(length, 64 * 1024))
        if not raw:
            return {}
        payload = json.loads(raw.decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("JSON body must be an object")
        return payload

    def _log_http_request(
        self,
        *,
        path: str,
        started_at: float,
        threshold_ms: float,
        force_refresh: bool = False,
        force_rebuild: bool = False,
    ) -> None:
        _log_if_slow(
            "http_request",
            (time.perf_counter() - started_at) * 1000.0,
            threshold_ms,
            path=path,
            status=getattr(self, "_last_response_status", 0),
            bytes=getattr(self, "_last_response_size", 0),
            gzip=int(bool(getattr(self, "_last_response_compressed", False))),
            refresh=int(force_refresh),
            rebuild=int(force_rebuild),
        )

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query, keep_blank_values=True)
        if path in ("/", "/index.html"):
            request_started = time.perf_counter()
            self._send_html(render_html())
            self._log_http_request(path=path, started_at=request_started, threshold_ms=SLOW_HTTP_META_MS)
            return
        if path == "/api/data":
            request_started = time.perf_counter()
            try:
                force_refresh = query.get("refresh", ["0"])[0] not in ("", "0", "false", "False")
                force_rebuild = query.get("rebuild", ["0"])[0] not in ("", "0", "false", "False")
                payload_json, payload_etag = build_payload_json_and_etag(
                    force_refresh=force_refresh,
                    force_rebuild=force_rebuild,
                )
            except Exception as exc:  # noqa: BLE001
                self._send_json({"error": str(exc)}, status=500)
                return
            self._send_json_bytes(payload_json, etag=payload_etag)
            self._log_http_request(
                path=path,
                started_at=request_started,
                threshold_ms=SLOW_HTTP_DATA_MS,
                force_refresh=force_refresh,
                force_rebuild=force_rebuild,
            )
            return
        if path == "/api/meta":
            request_started = time.perf_counter()
            try:
                payload = build_meta_payload()
            except Exception as exc:  # noqa: BLE001
                self._send_json({"error": str(exc)}, status=500)
                return
            payload_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self._send_json_bytes(payload_bytes, etag=_etag_for_bytes(payload_bytes))
            self._log_http_request(path=path, started_at=request_started, threshold_ms=SLOW_HTTP_META_MS)
            return
        if path == "/api/alert-blacklist":
            ok, message = self._dashboard_admin_authorized()
            if not ok:
                status = 503 if message == "管理口令未配置" else 401
                self._send_json({"error": message}, status=status)
                return
            try:
                row_keys = sorted(load_alert_blacklist_row_keys())
            except Exception as exc:  # noqa: BLE001
                self._send_json({"error": str(exc)}, status=500)
                return
            self._send_json({"rowKeys": row_keys, "count": len(row_keys)})
            return

        self._send_bytes(b"", status=404, content_type="text/plain; charset=utf-8")

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        if path != "/api/alert-blacklist":
            self._send_bytes(b"", status=404, content_type="text/plain; charset=utf-8")
            return
        ok, message = self._dashboard_admin_authorized()
        if not ok:
            status = 503 if message == "管理口令未配置" else 401
            self._send_json({"error": message}, status=status)
            return
        try:
            payload = self._read_json_body()
            clear_all = bool(payload.get("clearAll"))
            if clear_all:
                row_keys = sorted(update_alert_blacklist_row_keys(clear_all=True))
                self._send_json({"ok": True, "rowKeys": row_keys, "count": len(row_keys), "cleared": True})
                return
            replace_all = bool(payload.get("replaceAll"))
            if replace_all:
                raw_row_keys = payload.get("rowKeys")
                if not isinstance(raw_row_keys, list):
                    self._send_json({"error": "invalid rowKeys"}, status=400)
                    return
                normalized = {
                    row_key
                    for row_key in (normalize_alert_row_key(value) for value in raw_row_keys)
                    if row_key is not None
                }
                row_keys = sorted(update_alert_blacklist_row_keys(replace_all_row_keys=normalized))
                self._send_json({"ok": True, "rowKeys": row_keys, "count": len(row_keys), "replaced": True})
                return
            row_key = normalize_alert_row_key(payload.get("rowKey"))
            blocked_raw = payload.get("blocked")
            if row_key is None or not isinstance(blocked_raw, bool):
                self._send_json({"error": "invalid rowKey/blocked"}, status=400)
                return
            row_keys = sorted(update_alert_blacklist_row_keys(row_key=row_key, blocked=blocked_raw))
            self._send_json(
                {
                    "ok": True,
                    "rowKey": row_key,
                    "blocked": blocked_raw,
                    "rowKeys": row_keys,
                    "count": len(row_keys),
                }
            )
        except Exception as exc:  # noqa: BLE001
            self._send_json({"error": str(exc)}, status=500)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return


def write_static_file(output: Path) -> None:
    initialize_dashboard_runtime(apply_legacy_migrations=True)
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
    initialize_dashboard_runtime(apply_legacy_migrations=True)
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
