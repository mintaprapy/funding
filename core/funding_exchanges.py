#!/usr/bin/env python3
"""Shared exchange registry for scheduler and dashboard."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ExchangeDef:
    key: str
    label: str
    folder: str
    baseinfo_script: str
    history_script: str
    info_table_candidates: tuple[str, ...]
    history_table: str
    open_interest_is_notional: bool
    open_interest_notional_multiplier: float = 1.0
    allow_partial_window_sums: bool = False


EXCHANGES_DIR = "exchanges"
EXCHANGE_CONFIG_ENV = "FUNDING_EXCHANGE_CONFIG"
DEFAULT_EXCHANGE_CONFIG_RELATIVE_PATH = Path("config/exchanges.json")


EXCHANGES: tuple[ExchangeDef, ...] = (
    ExchangeDef(
        key="binance",
        label="Binance",
        folder=f"{EXCHANGES_DIR}/bincance_funding",
        baseinfo_script="binance_funding_baseinfo.py",
        history_script="binance_funding_history.py",
        info_table_candidates=("binance_funding_baseinfo", "binance_funding_info"),
        history_table="binance_funding_history",
        open_interest_is_notional=False,
    ),
    ExchangeDef(
        key="bybit",
        label="Bybit",
        folder=f"{EXCHANGES_DIR}/bybit_funding",
        baseinfo_script="bybit_funding_baseinfo.py",
        history_script="bybit_funding_history.py",
        info_table_candidates=("bybit_funding_baseinfo",),
        history_table="bybit_funding_history",
        open_interest_is_notional=True,
    ),
    ExchangeDef(
        key="aster",
        label="Aster",
        folder=f"{EXCHANGES_DIR}/aster_funding",
        baseinfo_script="aster_funding_baseinfo.py",
        history_script="aster_funding_history.py",
        info_table_candidates=("aster_funding_baseinfo",),
        history_table="aster_funding_history",
        open_interest_is_notional=False,
        open_interest_notional_multiplier=2.0,
    ),
    ExchangeDef(
        key="hyperliquid",
        label="Hyperliquid",
        folder=f"{EXCHANGES_DIR}/hyperliquid_funding",
        baseinfo_script="hyperliquid_funding_baseinfo.py",
        history_script="hyperliquid_funding_history.py",
        info_table_candidates=("hyperliquid_funding_baseinfo",),
        history_table="hyperliquid_funding_history",
        open_interest_is_notional=True,
    ),
    ExchangeDef(
        key="backpack",
        label="Backpack",
        folder=f"{EXCHANGES_DIR}/backpack_funding",
        baseinfo_script="backpack_funding_baseinfo.py",
        history_script="backpack_funding_history.py",
        info_table_candidates=("backpack_funding_baseinfo",),
        history_table="backpack_funding_history",
        open_interest_is_notional=False,
    ),
    ExchangeDef(
        key="ethereal",
        label="Ethereal",
        folder=f"{EXCHANGES_DIR}/ethereal_funding",
        baseinfo_script="ethereal_funding_baseinfo.py",
        history_script="ethereal_funding_history.py",
        info_table_candidates=("ethereal_funding_baseinfo",),
        history_table="ethereal_funding_history",
        open_interest_is_notional=False,
    ),
    ExchangeDef(
        key="grvt",
        label="GRVT",
        folder=f"{EXCHANGES_DIR}/grvt_funding",
        baseinfo_script="grvt_funding_baseinfo.py",
        history_script="grvt_funding_history.py",
        info_table_candidates=("grvt_funding_baseinfo",),
        history_table="grvt_funding_history",
        open_interest_is_notional=False,
    ),
    ExchangeDef(
        key="standx",
        label="StandX",
        folder=f"{EXCHANGES_DIR}/standx_funding",
        baseinfo_script="standx_funding_baseinfo.py",
        history_script="standx_funding_history.py",
        info_table_candidates=("standx_funding_baseinfo",),
        history_table="standx_funding_history",
        open_interest_is_notional=False,
    ),
    ExchangeDef(
        key="lighter",
        label="Lighter",
        folder=f"{EXCHANGES_DIR}/lighter_funding",
        baseinfo_script="lighter_funding_baseinfo.py",
        history_script="lighter_funding_history.py",
        info_table_candidates=("lighter_funding_baseinfo",),
        history_table="lighter_funding_history",
        open_interest_is_notional=False,
        open_interest_notional_multiplier=2.0,
    ),
    ExchangeDef(
        key="gate",
        label="Gate",
        folder=f"{EXCHANGES_DIR}/gate_funding",
        baseinfo_script="gate_funding_baseinfo.py",
        history_script="gate_funding_history.py",
        info_table_candidates=("gate_funding_baseinfo",),
        history_table="gate_funding_history",
        open_interest_is_notional=True,
    ),
    ExchangeDef(
        key="bitget",
        label="Bitget",
        folder=f"{EXCHANGES_DIR}/bitget_funding",
        baseinfo_script="bitget_funding_baseinfo.py",
        history_script="bitget_funding_history.py",
        info_table_candidates=("bitget_funding_baseinfo",),
        history_table="bitget_funding_history",
        open_interest_is_notional=True,
    ),
    ExchangeDef(
        key="variational",
        label="Variational",
        folder=f"{EXCHANGES_DIR}/variational_funding",
        baseinfo_script="variational_funding_baseinfo.py",
        history_script="variational_funding_history.py",
        info_table_candidates=("variational_funding_baseinfo",),
        history_table="variational_funding_history",
        open_interest_is_notional=True,
    ),
    ExchangeDef(
        key="edgex",
        label="edgeX",
        folder=f"{EXCHANGES_DIR}/edgex_funding",
        baseinfo_script="edgex_funding_baseinfo.py",
        history_script="edgex_funding_history.py",
        info_table_candidates=("edgex_funding_baseinfo",),
        history_table="edgex_funding_history",
        open_interest_is_notional=False,
    ),
)

EXCHANGES_BY_KEY: dict[str, ExchangeDef] = {item.key: item for item in EXCHANGES}


def default_exchange_config_path(root: Path) -> Path:
    return (root / DEFAULT_EXCHANGE_CONFIG_RELATIVE_PATH).resolve()


def resolve_exchange_config_path(root: Path, config_path: str | Path | None = None) -> Path:
    if config_path is not None:
        return Path(config_path).expanduser().resolve()
    env_path = os.getenv(EXCHANGE_CONFIG_ENV)
    if env_path:
        return Path(env_path).expanduser().resolve()
    return default_exchange_config_path(root)


def _load_enabled_exchange_keys(root: Path, config_path: str | Path | None = None) -> tuple[str, ...]:
    path = resolve_exchange_config_path(root, config_path)
    if not path.exists():
        return tuple(item.key for item in EXCHANGES)

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid exchange config JSON: {path}") from exc
    if not isinstance(data, dict):
        raise RuntimeError(f"exchange config must be a JSON object: {path}")

    enabled = data.get("enabled_exchanges")
    if enabled is None:
        return tuple(item.key for item in EXCHANGES)
    if not isinstance(enabled, list) or not all(isinstance(item, str) for item in enabled):
        raise RuntimeError(f"'enabled_exchanges' must be a string list: {path}")

    requested = [item.strip().lower() for item in enabled if item.strip()]
    unknown = sorted({item for item in requested if item not in EXCHANGES_BY_KEY})
    if unknown:
        raise RuntimeError(f"unknown exchanges in config {path}: {', '.join(unknown)}")

    enabled_set = set(requested)
    selected = tuple(item.key for item in EXCHANGES if item.key in enabled_set)
    if not selected:
        raise RuntimeError(f"exchange config {path} enabled 0 exchanges")
    return selected


def enabled_exchanges(root: Path, config_path: str | Path | None = None) -> tuple[ExchangeDef, ...]:
    enabled_keys = set(_load_enabled_exchange_keys(root, config_path))
    return tuple(item for item in EXCHANGES if item.key in enabled_keys)


def baseinfo_script_paths(root: Path, config_path: str | Path | None = None) -> list[Path]:
    return [root / item.folder / item.baseinfo_script for item in enabled_exchanges(root, config_path)]


def history_script_paths(root: Path, config_path: str | Path | None = None) -> list[Path]:
    return [root / item.folder / item.history_script for item in enabled_exchanges(root, config_path)]


def dashboard_exchange_meta(root: Path, config_path: str | Path | None = None) -> dict[str, dict[str, Any]]:
    return {
        item.key: {
            "label": item.label,
            "info_table_candidates": list(item.info_table_candidates),
            "history_table": item.history_table,
            "open_interest_is_notional": item.open_interest_is_notional,
            "open_interest_notional_multiplier": item.open_interest_notional_multiplier,
            "allow_partial_window_sums": item.allow_partial_window_sums,
        }
        for item in enabled_exchanges(root, config_path)
    }
