#!/usr/bin/env python3
"""Shared exchange registry for scheduler and dashboard."""

from __future__ import annotations

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


EXCHANGES_DIR = "exchanges"


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
        open_interest_is_notional=True,
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
    ),
)

EXCHANGES_BY_KEY: dict[str, ExchangeDef] = {item.key: item for item in EXCHANGES}


def baseinfo_script_paths(root: Path) -> list[Path]:
    return [root / item.folder / item.baseinfo_script for item in EXCHANGES]


def history_script_paths(root: Path) -> list[Path]:
    return [root / item.folder / item.history_script for item in EXCHANGES]


def dashboard_exchange_meta() -> dict[str, dict[str, Any]]:
    return {
        item.key: {
            "label": item.label,
            "info_table_candidates": list(item.info_table_candidates),
            "history_table": item.history_table,
            "open_interest_is_notional": item.open_interest_is_notional,
        }
        for item in EXCHANGES
    }
