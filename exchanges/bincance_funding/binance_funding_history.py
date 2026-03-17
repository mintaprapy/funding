#!/usr/bin/env python3
"""从 Binance 获取永续合约近 30 天资金费率，存入 SQLite，并保留 60 天内数据。"""

from __future__ import annotations

import os
import sqlite3
import sys
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

import requests

# ===== 配置区 =====
# 说明：
# - 资金费率接口以毫秒时间戳（ms）为单位；本脚本统一用 ms 计算窗口。
# - 本脚本依赖 `binance_funding_baseinfo.py` 先把交易对信息写入 `INFO_TABLE`。
# - 数据落库到共享 `funding.db`，并对历史表做“保留 60 天”清理。

BASE_URL = "https://fapi.binance.com"
FUNDING_RATE_PATH = "/fapi/v1/fundingRate"
ROOT_DIR = next(parent for parent in Path(__file__).resolve().parents if (parent / "start_all_funding.sh").exists())
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from core.common_funding import (
    collector_log_end,
    collector_log_progress,
    collector_log_start,
)

DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT_DIR / "funding.db")).expanduser().resolve()
INFO_TABLE = "binance_funding_baseinfo"
HISTORY_TABLE = "binance_funding_history"
REQUEST_TIMEOUT = 15
MAX_HTTP_RETRIES = 6
RETRY_BASE_SLEEP = 1.0

# 拉取与清理窗口：拉取近 30 天，并仅保留近 60 天记录（用于控制 SQLite 体积）
DAYS_TO_FETCH = 30
DAYS_TO_KEEP = 60
MAX_RECORDS = 720  # 接口上限，30 天内约 720 条

# 接口限频：500/5min/IP，预留一点余量
WINDOW_SECONDS = 400
WINDOW_CAPACITY = int(500 * 0.8)


def tune_sqlite_connection(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA busy_timeout=15000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")


class RateLimiter:
    """简单滑动窗口限频器。"""

    def __init__(self, capacity: int, period: int) -> None:
        self.capacity = capacity
        self.period = period
        self.events: list[float] = []

    def acquire(self) -> None:
        now = time.monotonic()
        # 清理窗口外事件
        self.events = [t for t in self.events if now - t < self.period]
        # 若达到容量上限，则等待到最早事件滑出窗口后再继续
        if len(self.events) >= self.capacity:
            sleep_for = self.period - (now - self.events[0]) + 0.01
            time.sleep(max(0.05, sleep_for))
        self.events.append(time.monotonic())


def fetch_json(session: requests.Session, url: str, params: dict[str, Any]) -> Any:
    # 统一封装 HTTP 请求与错误处理（非 2xx 直接抛异常）
    last_exc: Exception | None = None
    for attempt in range(1, MAX_HTTP_RETRIES + 1):
        try:
            resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt >= MAX_HTTP_RETRIES:
                break
            time.sleep(min(20.0, RETRY_BASE_SLEEP * attempt))
    raise RuntimeError(f"请求失败: {url}; last_error={last_exc}")


def load_symbols(conn: sqlite3.Connection) -> list[str]:
    # 从基础信息表读取交易对列表；该表由 `binance_funding_baseinfo.py` 维护
    cur = conn.execute(f"SELECT symbol FROM {INFO_TABLE}")
    symbols = sorted(row[0] for row in cur.fetchall() if row[0])
    if not symbols:
        raise RuntimeError(f"{INFO_TABLE} 中没有交易对数据，请先同步该表")
    return symbols


def ensure_history_table(conn: sqlite3.Connection) -> None:
    # 资金费率历史表：以 (symbol, fundingTime) 做主键，重复写入时走 upsert 更新
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


def to_plain_str(val: Any) -> str | None:
    """将科学计数法转为普通小数表示；转换失败返回 None。"""
    # Binance fundingRate 常见为字符串/数字，统一转 Decimal 再格式化，便于入库与展示
    try:
        dec = Decimal(str(val))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return format(dec.normalize(), "f")


def delete_older_than(conn: sqlite3.Connection, cutoff_ms: int, symbols: Iterable[str]) -> None:
    # 按交易对清理历史记录：仅删除指定 symbols 且早于 cutoff 的数据
    symbols = list(set(symbols))
    if not symbols:
        return
    placeholders = ",".join("?" for _ in symbols)
    conn.execute(
        f"DELETE FROM {HISTORY_TABLE} WHERE fundingTime < ? AND symbol IN ({placeholders})",
        (cutoff_ms, *symbols),
    )


def delete_orphan_history(conn: sqlite3.Connection) -> int:
    """清理 history 中已不在 baseinfo 的符号数据，返回删除行数。"""
    cur = conn.execute(
        f"""
        DELETE FROM {HISTORY_TABLE}
        WHERE symbol NOT IN (SELECT symbol FROM {INFO_TABLE})
        """
    )
    return int(cur.rowcount or 0)


def save_history(
    conn: sqlite3.Connection,
    symbol: str,
    records: list[dict[str, Any]],
    *,
    now_ms: int,
) -> tuple[int, int]:
    # 批量写入（或更新）资金费率历史；fundingRate 以“普通小数”字符串落库
    rows = []
    invalid_count = 0
    for entry in records:
        raw_funding_time = entry.get("fundingTime")
        try:
            funding_time = int(raw_funding_time)
        except (TypeError, ValueError):
            invalid_count += 1
            continue
        rows.append(
            (
                symbol,
                funding_time,
                to_plain_str(entry.get("fundingRate")),
                now_ms,
            )
        )
    if not rows:
        return 0, invalid_count
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
    return len(rows), invalid_count


def fetch_symbol_history(
    session: requests.Session,
    limiter: RateLimiter,
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> list[dict[str, Any]]:
    # Binance 接口一次最多返回 720 条（约 30 天 * 24 小时），这里直接一次性拉取
    limiter.acquire()
    data = fetch_json(
        session,
        f"{BASE_URL}{FUNDING_RATE_PATH}",
        {
            "symbol": symbol,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": MAX_RECORDS,
        },
    )
    if not isinstance(data, list):
        raise RuntimeError(f"{symbol} fundingRate 返回格式异常")
    data.sort(key=lambda item: item.get("fundingTime", 0))
    return data


def main() -> None:
    # 计算拉取窗口与清理窗口（均为毫秒时间戳）
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - DAYS_TO_FETCH * 24 * 60 * 60 * 1000
    cutoff_ms = end_ms - DAYS_TO_KEEP * 24 * 60 * 60 * 1000
    limiter = RateLimiter(WINDOW_CAPACITY, WINDOW_SECONDS)

    with sqlite3.connect(DB_PATH) as conn, requests.Session() as session:
        session.trust_env = False
        ensure_history_table(conn)
        symbols = load_symbols(conn)
        collector_log_start("Binance", "history", detail=f"{len(symbols)} 个交易对，近 {DAYS_TO_FETCH} 天资金费率")

        for idx, symbol in enumerate(symbols, 1):
            try:
                records = fetch_symbol_history(session, limiter, symbol, start_ms, end_ms)
            except Exception as exc:  # noqa: BLE001
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} 获取失败：{exc}")
                continue
            # 入库 + 清理 + 提交：每个交易对独立 commit，避免单个失败影响整体
            try:
                saved_count, invalid_count = save_history(conn, symbol, records, now_ms=end_ms)
                delete_older_than(conn, cutoff_ms, [symbol])
                conn.commit()
            except Exception as exc:  # noqa: BLE001
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}][warn] {symbol} 入库失败：{exc}")
                conn.rollback()
                continue

            log_detail = f"{symbol} 入库 {saved_count} 条"
            if invalid_count > 0:
                log_detail += f"（跳过 fundingTime 非法 {invalid_count} 条）"
            collector_log_progress("Binance", "history", detail=log_detail, current=idx, total=len(symbols))

        orphan_deleted = delete_orphan_history(conn)
        conn.commit()
        if orphan_deleted > 0:
            collector_log_progress("Binance", "history", detail=f"清理孤儿历史记录 {orphan_deleted} 条")

    collector_log_end("Binance", "history")


if __name__ == "__main__":
    main()
