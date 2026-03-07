#!/usr/bin/env python3
"""Periodic health monitor for funding collectors."""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import subprocess
import time
from collections import deque
from datetime import datetime
from pathlib import Path

from common_funding import tune_sqlite_connection

DEFAULT_DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (Path(__file__).resolve().parent / "funding.db")).expanduser().resolve()

FATAL_PATTERNS = [
    re.compile(r"\[fail\]", re.IGNORECASE),
    re.compile(r"\[warn\]\s+[A-Za-z0-9_./-]+\.py\s+exited with code\s+-?\d+", re.IGNORECASE),
    re.compile(r"脚本失败"),
    re.compile(r"Traceback \(most recent call last\):"),
    re.compile(r"reached max-rounds=.*stop with failures", re.IGNORECASE),
]


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monitor funding stack health periodically.")
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH), help="SQLite database path.")
    parser.add_argument("--log-dir", default="/tmp", help="Directory containing supervisor logs.")
    parser.add_argument("--interval-sec", type=int, default=600, help="Check interval in seconds.")
    parser.add_argument("--tail-lines", type=int, default=600, help="Lines scanned from latest log tail.")
    return parser.parse_args()


def check_processes() -> list[str]:
    required = [
        "long_run_supervisor.py",
        "run_all_funding_stack.py",
        "allfunding_dashboard.py",
    ]
    try:
        proc = subprocess.run(
            ["ps", "-Ao", "command"],
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception as exc:  # noqa: BLE001
        return [f"process_check_error:{exc}"]

    text = proc.stdout or ""
    missing = [name for name in required if name not in text]
    return [f"missing_process:{name}" for name in missing]


def check_db(db_path: Path) -> list[str]:
    if not db_path.exists():
        return [f"missing_db:{db_path}"]

    issues: list[str] = []
    with sqlite3.connect(db_path) as conn:
        tune_sqlite_connection(conn)
        cur = conn.cursor()
        base_tables = [
            row[0]
            for row in cur.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name LIKE '%_funding_baseinfo' ORDER BY name"
            )
        ]
        if not base_tables:
            return ["missing_baseinfo_tables"]

        for table in base_tables:
            rows, null_mark, null_rate, null_oi = cur.execute(
                f"""
                SELECT
                    COUNT(*),
                    SUM(CASE WHEN markPrice IS NULL OR TRIM(markPrice)='' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN lastFundingRate IS NULL OR TRIM(lastFundingRate)='' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN openInterest IS NULL OR TRIM(openInterest)='' THEN 1 ELSE 0 END)
                FROM {table}
                """
            ).fetchone()
            if rows <= 0:
                issues.append(f"{table}:empty")
                continue
            if (null_mark or 0) > 0:
                issues.append(f"{table}:null_markPrice={null_mark}")
            if (null_rate or 0) > 0:
                issues.append(f"{table}:null_lastFundingRate={null_rate}")
            if (null_oi or 0) > 0:
                issues.append(f"{table}:null_openInterest={null_oi}")

            history_table = table.replace("_funding_baseinfo", "_funding_history")
            exists = cur.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (history_table,),
            ).fetchone()
            if not exists:
                issues.append(f"{history_table}:missing")
                continue
            h_rows, h_null_rate = cur.execute(
                f"""
                SELECT
                    COUNT(*),
                    SUM(CASE WHEN fundingRate IS NULL OR TRIM(fundingRate)='' THEN 1 ELSE 0 END)
                FROM {history_table}
                """
            ).fetchone()
            if h_rows <= 0:
                issues.append(f"{history_table}:empty")
            if (h_null_rate or 0) > 0:
                issues.append(f"{history_table}:null_fundingRate={h_null_rate}")

    return issues


def scan_latest_log(log_dir: Path, tail_lines: int) -> tuple[str | None, list[str]]:
    candidates = sorted(log_dir.glob("funding_supervisor_round*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        return None, ["missing_supervisor_log"]

    latest = candidates[0]
    lines: deque[str] = deque(maxlen=max(50, tail_lines))
    try:
        with latest.open("r", encoding="utf-8", errors="replace") as fp:
            for line in fp:
                lines.append(line.rstrip("\n"))
    except Exception as exc:  # noqa: BLE001
        return str(latest), [f"log_read_error:{exc}"]

    hits: list[str] = []
    for line in lines:
        if any(p.search(line) for p in FATAL_PATTERNS):
            hits.append(line)
    return str(latest), hits


def main() -> None:
    args = parse_args()
    db_path = Path(args.db).resolve()
    log_dir = Path(args.log_dir).resolve()
    try:
        while True:
            proc_issues = check_processes()
            db_issues = check_db(db_path)
            latest_log, log_hits = scan_latest_log(log_dir, args.tail_lines)

            status = "ALERT" if (proc_issues or db_issues or log_hits) else "OK"
            summary = {
                "ts": now_str(),
                "status": status,
                "process_issues": len(proc_issues),
                "db_issues": len(db_issues),
                "fatal_log_hits": len(log_hits),
                "latest_log": latest_log,
            }
            print(json.dumps(summary, ensure_ascii=False), flush=True)

            if proc_issues:
                for item in proc_issues:
                    print(f"[{now_str()}] ALERT {item}", flush=True)
            if db_issues:
                for item in db_issues[:20]:
                    print(f"[{now_str()}] ALERT {item}", flush=True)
                if len(db_issues) > 20:
                    print(f"[{now_str()}] ALERT db_issues_truncated={len(db_issues)}", flush=True)
            if log_hits:
                for item in log_hits[-20:]:
                    print(f"[{now_str()}] ALERT log:{item}", flush=True)

            time.sleep(max(30, int(args.interval_sec)))
    except KeyboardInterrupt:
        print(f"[{now_str()}] monitor stopped", flush=True)


if __name__ == "__main__":
    main()
