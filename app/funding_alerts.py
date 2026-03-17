#!/usr/bin/env python3
"""Threshold-based funding alerts for Telegram / Feishu."""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.allfunding_dashboard import build_payload
from core.common_funding import tune_sqlite_connection
from core.funding_exchanges import dashboard_exchange_meta

DEFAULT_ALERT_CONFIG = ROOT / "config" / "alerts.json"
ALERT_CONFIG_ENV = "FUNDING_ALERT_CONFIG"
FUNDING_DB_PATH_ENV = "FUNDING_DB_PATH"
DB_PATH = Path(os.getenv(FUNDING_DB_PATH_ENV) or (ROOT / "funding.db")).expanduser().resolve()
H4_WINDOW_MS = 4 * 60 * 60 * 1000
EXCHANGE_META = dashboard_exchange_meta(ROOT)


@dataclass(frozen=True)
class AlertHit:
    dedupe_keys: tuple[str, ...]
    exchange: str
    symbol: str
    latest_value: float | None
    h4_value: float | None
    interval_hours: int | None
    open_interest_musd: float | None


def now_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as fp:
        data = json.load(fp)
    if not isinstance(data, dict):
        raise RuntimeError(f"配置格式错误（需为 JSON object）: {path}")
    return data


def resolve_state_path(config: dict[str, Any], config_path: Path) -> Path:
    raw = str(config.get("state_path") or "logs/alert_state.json").strip()
    path = Path(raw)
    if not path.is_absolute():
        path = (ROOT / path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"sent": {}}
    try:
        data = load_json(path)
    except Exception:
        return {"sent": {}}
    sent = data.get("sent")
    if not isinstance(sent, dict):
        sent = {}
    return {"sent": sent}


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        json.dump(state, fp, ensure_ascii=False, indent=2, sort_keys=True)


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    tune_sqlite_connection(conn)
    return conn


def fetch_h4_sums(now_ms: int) -> dict[tuple[str, str], float | None]:
    result: dict[tuple[str, str], float | None] = {}
    lower = now_ms - H4_WINDOW_MS
    with connect_db() as conn:
        for exchange_key, meta in EXCHANGE_META.items():
            history_table = str(meta["history_table"])
            try:
                rows = conn.execute(
                    f"""
                    SELECT symbol,
                           MIN(fundingTime) AS oldestFundingTime,
                           COALESCE(SUM(CASE WHEN fundingTime >= ? AND fundingTime <= ? THEN CAST(fundingRate AS REAL) ELSE 0 END), 0) AS h4
                    FROM {history_table}
                    WHERE fundingTime <= ?
                    GROUP BY symbol
                    """,
                    (lower, now_ms, now_ms),
                ).fetchall()
            except sqlite3.OperationalError:
                continue
            for row in rows:
                oldest = row["oldestFundingTime"]
                oldest_ms = int(oldest) if oldest is not None else None
                mature = oldest_ms is not None and oldest_ms <= lower
                result[(exchange_key, str(row["symbol"]))] = float(row["h4"]) if mature else None
    return result


def parse_threshold(config: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        raw = config.get(key)
        if raw in (None, ""):
            continue
        try:
            return float(raw)
        except (TypeError, ValueError):
            continue
    return None


def collect_hits(payload: dict[str, Any], config: dict[str, Any]) -> list[AlertHit]:
    latest_threshold_gte_pct = parse_threshold(config, "latest_pct_gte", "latest_abs_pct_gte")
    latest_threshold_lte_pct = parse_threshold(config, "latest_pct_lte", "latest_abs_pct_lte")
    h4_threshold_gte_pct = parse_threshold(config, "h4_pct_gte", "h4_abs_pct_gte")
    h4_threshold_lte_pct = parse_threshold(config, "h4_pct_lte", "h4_abs_pct_lte")
    oi_min_musd = config.get("open_interest_min_musd")
    try:
        oi_min_musd_value = max(0.0, float(oi_min_musd)) if oi_min_musd is not None else None
    except (TypeError, ValueError):
        oi_min_musd_value = None
    generated_at = int(payload.get("generatedAt") or time.time() * 1000)
    h4_enabled = h4_threshold_gte_pct is not None or h4_threshold_lte_pct is not None
    h4_sums = fetch_h4_sums(generated_at) if h4_enabled else {}

    hits: list[AlertHit] = []
    for item in payload.get("items", []):
        if not isinstance(item, dict):
            continue
        exchange = str(item.get("exchangeLabel") or item.get("exchange") or "")
        exchange_key = str(item.get("exchange") or "")
        symbol = str(item.get("symbol") or "")
        interval = item.get("fundingIntervalHours")
        try:
            interval_hours = int(interval) if interval is not None else None
        except (TypeError, ValueError):
            interval_hours = None
        open_interest_notional = item.get("openInterestNotional")
        try:
            open_interest_musd = float(open_interest_notional) / 1_000_000.0 if open_interest_notional is not None else None
        except (TypeError, ValueError):
            open_interest_musd = None
        if oi_min_musd_value is not None:
            if open_interest_musd is None or open_interest_musd < oi_min_musd_value:
                continue

        latest = item.get("lastFundingRate")
        latest_value = float(latest) if isinstance(latest, (int, float)) else None
        h4_raw = h4_sums.get((exchange_key, symbol)) if h4_enabled else None
        h4_value = float(h4_raw) if isinstance(h4_raw, (int, float)) else None

        dedupe_keys: list[str] = []
        if latest_value is not None:
            latest_pct = latest_value * 100.0
            if latest_threshold_gte_pct is not None and latest_pct >= latest_threshold_gte_pct:
                dedupe_keys.append(f"{exchange}:{symbol}:latest_gte")
            if latest_threshold_lte_pct is not None and latest_pct <= latest_threshold_lte_pct:
                dedupe_keys.append(f"{exchange}:{symbol}:latest_lte")

        if h4_value is not None:
            h4_pct = h4_value * 100.0
            if h4_threshold_gte_pct is not None and h4_pct >= h4_threshold_gte_pct:
                dedupe_keys.append(f"{exchange}:{symbol}:h4_gte")
            if h4_threshold_lte_pct is not None and h4_pct <= h4_threshold_lte_pct:
                dedupe_keys.append(f"{exchange}:{symbol}:h4_lte")

        if dedupe_keys:
            hits.append(
                AlertHit(
                    dedupe_keys=tuple(dedupe_keys),
                    exchange=exchange,
                    symbol=symbol,
                    latest_value=latest_value,
                    h4_value=h4_value,
                    interval_hours=interval_hours,
                    open_interest_musd=open_interest_musd,
                )
            )

    hits.sort(key=lambda item: (item.exchange, -(abs(item.latest_value) if item.latest_value is not None else -1.0), item.symbol))
    return hits


def filter_hits_by_cooldown(
    hits: list[AlertHit],
    state: dict[str, Any],
    *,
    cooldown_minutes: int,
    force: bool,
) -> list[AlertHit]:
    if force:
        return hits
    sent = state.setdefault("sent", {})
    now_ms = int(time.time() * 1000)
    cooldown_ms = max(0, cooldown_minutes) * 60 * 1000
    due: list[AlertHit] = []
    for hit in hits:
        due_keys: list[str] = []
        for key in hit.dedupe_keys:
            last_sent = sent.get(key)
            try:
                last_sent_ms = int(last_sent)
            except (TypeError, ValueError):
                last_sent_ms = None
            if last_sent_ms is None or now_ms - last_sent_ms >= cooldown_ms:
                due_keys.append(key)
        if due_keys:
            due.append(
                AlertHit(
                    dedupe_keys=tuple(due_keys),
                    exchange=hit.exchange,
                    symbol=hit.symbol,
                    latest_value=hit.latest_value,
                    h4_value=hit.h4_value,
                    interval_hours=hit.interval_hours,
                    open_interest_musd=hit.open_interest_musd,
                )
            )
    return due


def fmt_pct(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value * 100:.2f}%"


def build_message(hits: list[AlertHit], payload: dict[str, Any], *, max_items: int) -> str:
    exchange_groups: dict[str, list[AlertHit]] = {}
    for hit in hits:
        exchange_groups.setdefault(hit.exchange, []).append(hit)

    shown_count = 0
    lines: list[str] = []
    for exchange in sorted(exchange_groups):
        if shown_count >= max_items:
            break
        group = sorted(
            exchange_groups[exchange],
            key=lambda item: (
                -(abs(item.latest_value) if item.latest_value is not None else -1.0),
                item.symbol,
            ),
        )
        remaining_slots = max_items - shown_count
        shown_group = group[:remaining_slots]
        if not shown_group:
            continue
        lines.append(f"{exchange}")
        for hit in shown_group:
            interval_text = f"{hit.interval_hours}h" if hit.interval_hours else "—"
            oi_text = f"{hit.open_interest_musd:.1f}M" if hit.open_interest_musd is not None else "—"
            lines.append(
                f"- {hit.symbol} | 最新 {fmt_pct(hit.latest_value)} | 4H累计 {fmt_pct(hit.h4_value)} | 周期 {interval_text} | 持量$ {oi_text}"
            )
        shown_count += len(shown_group)
        lines.append("")

    if len(hits) > shown_count:
        lines.append(f"... 其余 {len(hits) - shown_count} 项未展开")
    while lines and lines[-1] == "":
        lines.pop()
    return "\n".join(lines)


def send_telegram(message: str, cfg: dict[str, Any]) -> None:
    token = str(cfg.get("bot_token") or "").strip()
    chat_id = str(cfg.get("chat_id") or "").strip()
    if not token or not chat_id:
        raise RuntimeError("Telegram 未配置 bot_token/chat_id")
    payload: dict[str, Any] = {"chat_id": chat_id, "text": message}
    thread_id = cfg.get("message_thread_id")
    if thread_id not in (None, ""):
        payload["message_thread_id"] = int(thread_id)
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    resp = requests.post(url, json=payload, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict) or not data.get("ok"):
        raise RuntimeError(f"Telegram 发送失败: {data}")


def _feishu_sign(secret: str, timestamp: int) -> str:
    string_to_sign = f"{timestamp}\n{secret}".encode("utf-8")
    digest = hmac.new(string_to_sign, digestmod=hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def send_feishu(message: str, cfg: dict[str, Any]) -> None:
    webhook_url = str(cfg.get("webhook_url") or "").strip()
    if not webhook_url:
        raise RuntimeError("Feishu 未配置 webhook_url")
    payload: dict[str, Any] = {
        "msg_type": "text",
        "content": {"text": message},
    }
    secret = str(cfg.get("secret") or "").strip()
    if secret:
        timestamp = int(time.time())
        payload["timestamp"] = str(timestamp)
        payload["sign"] = _feishu_sign(secret, timestamp)
    resp = requests.post(webhook_url, json=payload, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict) or data.get("code", 0) not in (0, "0", None):
        raise RuntimeError(f"Feishu 发送失败: {data}")


def notify(message: str, config: dict[str, Any], *, dry_run: bool) -> bool:
    providers = config.get("providers") or {}
    if not isinstance(providers, dict):
        providers = {}
    if dry_run:
        print(message)
        return True

    successes = 0
    errors: list[str] = []

    telegram_cfg = providers.get("telegram")
    if isinstance(telegram_cfg, dict) and telegram_cfg.get("enabled"):
        try:
            send_telegram(message, telegram_cfg)
            successes += 1
        except Exception as exc:  # noqa: BLE001
            errors.append(f"telegram: {exc}")

    feishu_cfg = providers.get("feishu")
    if isinstance(feishu_cfg, dict) and feishu_cfg.get("enabled"):
        try:
            send_feishu(message, feishu_cfg)
            successes += 1
        except Exception as exc:  # noqa: BLE001
            errors.append(f"feishu: {exc}")

    if successes > 0:
        print(f"[{now_str()}] alerts sent via {successes} provider(s)")
        return True

    if errors:
        raise RuntimeError("; ".join(errors))
    print(f"[{now_str()}] alerts enabled but no provider is configured/enabled")
    return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Funding threshold alerts")
    parser.add_argument(
        "--config",
        default=os.getenv(ALERT_CONFIG_ENV, str(DEFAULT_ALERT_CONFIG)),
        help="Alert config path (defaults to FUNDING_ALERT_CONFIG or config/alerts.json)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print alerts without sending")
    parser.add_argument("--force", action="store_true", help="Ignore cooldown and send immediately")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config_path = Path(args.config).expanduser().resolve()
    config = load_json(config_path)
    if not config.get("enabled"):
        return

    state_path = resolve_state_path(config, config_path)
    state = load_state(state_path)
    cooldown_minutes = int(config.get("cooldown_minutes") or 120)
    max_items = max(1, int(config.get("max_items_per_run") or 20))

    payload = build_payload(force_refresh=True)
    hits = collect_hits(payload, config)
    due_hits = filter_hits_by_cooldown(hits, state, cooldown_minutes=cooldown_minutes, force=args.force)
    if not due_hits:
        return

    message = build_message(due_hits, payload, max_items=max_items)
    sent = notify(message, config, dry_run=args.dry_run)
    if sent:
        sent_state = state.setdefault("sent", {})
        now_ms = int(time.time() * 1000)
        for hit in due_hits:
            for key in hit.dedupe_keys:
                sent_state[key] = now_ms
        save_state(state_path, state)


if __name__ == "__main__":
    main()
