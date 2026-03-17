#!/usr/bin/env python3
"""Export a 12h runtime + database diagnostic bundle and tarball."""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
import tarfile
import time
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.funding_exchanges import ExchangeDef, enabled_exchanges


DEFAULT_SERVICE = "funding-stack"
DEFAULT_HOURS = 12
DEFAULT_OUTPUT_ROOT = Path("/tmp")
DEFAULT_API_URL = "http://127.0.0.1:5000/api/data"
DEFAULT_DB_PATH = ROOT / "funding.db"
WINDOW_KEYS = ("h24", "d3", "d7", "d15", "d30")


@dataclass
class CommandResult:
    cmd: list[str]
    returncode: int
    stdout_path: str
    stderr_path: str


def now_local() -> datetime:
    return datetime.now().astimezone()


def now_ms() -> int:
    return int(time.time() * 1000)


def to_iso_local(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    if ts_ms > 10**12:
        ts = ts_ms / 1000.0
    else:
        ts = float(ts_ms)
    return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone().isoformat()


def sanitize_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip()) or "unknown"


def write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def run_command(cmd: list[str], output_dir: Path, stem: str) -> CommandResult:
    stdout_path = output_dir / f"{stem}.out.txt"
    stderr_path = output_dir / f"{stem}.err.txt"
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        write_text(stdout_path, proc.stdout)
        write_text(stderr_path, proc.stderr)
        returncode = proc.returncode
    except FileNotFoundError as exc:
        write_text(stdout_path, "")
        write_text(stderr_path, f"{exc}\n")
        returncode = 127
    return CommandResult(cmd=cmd, returncode=returncode, stdout_path=str(stdout_path.name), stderr_path=str(stderr_path.name))


def collect_system_info(output_dir: Path) -> None:
    env = {
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "python": sys.version,
        "cwd": os.getcwd(),
        "root": str(ROOT),
        "timestamp": now_local().isoformat(),
        "funding_db_path_env": os.getenv("FUNDING_DB_PATH"),
        "funding_exchange_config_env": os.getenv("FUNDING_EXCHANGE_CONFIG"),
        "funding_alert_config_env": os.getenv("FUNDING_ALERT_CONFIG"),
    }
    write_text(output_dir / "environment.json", json.dumps(env, ensure_ascii=False, indent=2))


def collect_service_artifacts(service: str, hours: int, output_dir: Path) -> dict[str, Any]:
    results = {
        "status": run_command(["systemctl", "status", service, "--no-pager"], output_dir, "systemctl_status"),
        "is_active": run_command(["systemctl", "is-active", service], output_dir, "systemctl_is_active"),
        "show": run_command(["systemctl", "show", service], output_dir, "systemctl_show"),
        "cat": run_command(["systemctl", "cat", service], output_dir, "systemctl_cat"),
        "journal": run_command(
            ["journalctl", "-u", service, "--since", f"{hours} hours ago", "--no-pager"],
            output_dir,
            f"journal_last_{hours}h",
        ),
    }
    return {key: asdict(value) for key, value in results.items()}


def summarize_journal(journal_path: Path, output_dir: Path) -> dict[str, Any]:
    if not journal_path.exists():
        summary = {"exists": False}
        write_text(output_dir / "journal_summary.json", json.dumps(summary, ensure_ascii=False, indent=2))
        return summary

    lines = journal_path.read_text(encoding="utf-8", errors="replace").splitlines()
    lower_markers = ("error", "traceback", "exception", "failed", "timeout", "timed out", "403", "429", "refused")
    relevant_lines: list[str] = []
    warn_counter: Counter[str] = Counter()

    def normalize(line: str) -> str:
        line = re.sub(r"^.*?(python\[\d+\]:|systemd\[\d+\]:)\s*", "", line)
        line = re.sub(r"^\[[0-9:\- ]+\]\s*", "", line)
        return line.strip()

    for line in lines:
        lower = line.lower()
        if "[warn]" in line or any(marker in lower for marker in lower_markers):
            relevant_lines.append(line)
        if "[warn]" in line:
            warn_counter[normalize(line)] += 1

    summary = {
        "exists": True,
        "line_count": len(lines),
        "warn_count": sum(1 for line in lines if "[warn]" in line),
        "error_like_count": sum(1 for line in lines if any(marker in line.lower() for marker in lower_markers)),
        "service_start_count": sum("Started funding-stack.service" in line for line in lines),
        "service_stop_count": sum("Stopping funding-stack.service" in line for line in lines),
        "service_main_exit_count": sum("Main process exited" in line for line in lines),
        "top_warn_messages": warn_counter.most_common(30),
    }
    write_text(output_dir / "journal_summary.json", json.dumps(summary, ensure_ascii=False, indent=2))
    write_text(output_dir / "journal_relevant.txt", "\n".join(relevant_lines) + ("\n" if relevant_lines else ""))
    return summary


def copy_optional_file(src: Path, dst: Path) -> None:
    if src.exists():
        shutil.copy2(src, dst)


def collect_repo_artifacts(output_dir: Path) -> None:
    copy_optional_file(ROOT / "config" / "exchanges.json", output_dir / "exchanges.json")
    copy_optional_file(ROOT / "systemd" / "funding-stack.service", output_dir / "funding-stack.repo.service")
    copy_optional_file(Path("/etc/systemd/system/funding-stack.service"), output_dir / "funding-stack.installed.service")

    logs_dir = ROOT / "logs"
    if not logs_dir.exists():
        return

    listing = []
    for path in sorted(logs_dir.iterdir()):
        stat = path.stat()
        listing.append(
            {
                "name": path.name,
                "is_dir": path.is_dir(),
                "size": stat.st_size,
                "mtime": to_iso_local(int(stat.st_mtime * 1000)),
            }
        )
        if path.is_file():
            shutil.copy2(path, output_dir / f"logs.{path.name}")
    write_text(output_dir / "logs_listing.json", json.dumps(listing, ensure_ascii=False, indent=2))


def backup_database(db_path: Path, output_dir: Path) -> Path | None:
    if not db_path.exists():
        return None
    snapshot_path = output_dir / "funding.db.snapshot"
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as src, sqlite3.connect(snapshot_path) as dst:
        src.backup(dst)
    return snapshot_path


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def existing_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()}


def resolve_info_table(conn: sqlite3.Connection, exchange: ExchangeDef) -> str | None:
    for candidate in exchange.info_table_candidates:
        if table_exists(conn, candidate):
            return candidate
    return None


def analyze_database(db_path: Path, output_dir: Path) -> dict[str, Any]:
    if not db_path.exists():
        summary = {"exists": False, "db_path": str(db_path)}
        write_text(output_dir / "db_summary.json", json.dumps(summary, ensure_ascii=False, indent=2))
        return summary

    bundle: dict[str, Any] = {"exists": True, "db_path": str(db_path), "generated_at": now_local().isoformat()}
    exchange_rows: list[dict[str, Any]] = []
    missing_symbols: dict[str, list[str]] = {}
    app_meta: list[dict[str, Any]] = []

    with sqlite3.connect(db_path) as conn:
        for exchange in enabled_exchanges(ROOT):
            info_table = resolve_info_table(conn, exchange)
            history_table = exchange.history_table if table_exists(conn, exchange.history_table) else None
            item: dict[str, Any] = {
                "key": exchange.key,
                "label": exchange.label,
                "info_table": info_table,
                "history_table": history_table,
                "baseinfo_rows": 0,
                "history_rows": 0,
                "history_symbols": 0,
                "missing_history_symbols": 0,
                "missing_mark_price": None,
                "missing_last_funding_rate": None,
                "missing_open_interest": None,
                "baseinfo_last_updated_at": None,
                "history_min_funding_time": None,
                "history_max_funding_time": None,
            }

            if info_table:
                columns = existing_columns(conn, info_table)
                item["baseinfo_rows"] = conn.execute(f'SELECT COUNT(*) FROM "{info_table}"').fetchone()[0]
                if "updated_at" in columns:
                    item["baseinfo_last_updated_at"] = to_iso_local(
                        conn.execute(f'SELECT MAX(updated_at) FROM "{info_table}"').fetchone()[0]
                    )
                if "markPrice" in columns:
                    item["missing_mark_price"] = conn.execute(
                        f'SELECT COUNT(*) FROM "{info_table}" WHERE markPrice IS NULL OR TRIM(markPrice)=""'
                    ).fetchone()[0]
                if "lastFundingRate" in columns:
                    item["missing_last_funding_rate"] = conn.execute(
                        f'SELECT COUNT(*) FROM "{info_table}" WHERE lastFundingRate IS NULL OR TRIM(lastFundingRate)=""'
                    ).fetchone()[0]
                if "openInterest" in columns:
                    item["missing_open_interest"] = conn.execute(
                        f'SELECT COUNT(*) FROM "{info_table}" WHERE openInterest IS NULL OR TRIM(openInterest)=""'
                    ).fetchone()[0]

            if history_table:
                item["history_rows"] = conn.execute(f'SELECT COUNT(*) FROM "{history_table}"').fetchone()[0]
                item["history_symbols"] = conn.execute(
                    f'SELECT COUNT(DISTINCT symbol) FROM "{history_table}"'
                ).fetchone()[0]
                item["history_min_funding_time"] = to_iso_local(
                    conn.execute(f'SELECT MIN(fundingTime) FROM "{history_table}"').fetchone()[0]
                )
                item["history_max_funding_time"] = to_iso_local(
                    conn.execute(f'SELECT MAX(fundingTime) FROM "{history_table}"').fetchone()[0]
                )

            if info_table and history_table:
                rows = conn.execute(
                    f'''
                    SELECT b.symbol
                    FROM "{info_table}" b
                    LEFT JOIN (SELECT DISTINCT symbol FROM "{history_table}") h
                      ON b.symbol = h.symbol
                    WHERE h.symbol IS NULL
                    ORDER BY b.symbol
                    '''
                ).fetchall()
                symbols = [str(row[0]) for row in rows]
                item["missing_history_symbols"] = len(symbols)
                if symbols:
                    missing_symbols[exchange.key] = symbols

            exchange_rows.append(item)

        if table_exists(conn, "app_meta"):
            for key, value, updated_at in conn.execute(
                "SELECT key, value, updated_at FROM app_meta ORDER BY key"
            ).fetchall():
                app_meta.append(
                    {
                        "key": key,
                        "value": value,
                        "updated_at": to_iso_local(updated_at),
                    }
                )

    bundle["exchanges"] = exchange_rows
    bundle["missing_symbols"] = missing_symbols
    bundle["app_meta"] = app_meta
    write_text(output_dir / "db_summary.json", json.dumps(bundle, ensure_ascii=False, indent=2))

    lines = [
        f"db_path = {db_path}",
        "",
        "exchange\tbaseinfo_rows\thistory_rows\thistory_symbols\tmissing_history_symbols\tbaseinfo_last_updated_at\thistory_max_funding_time",
    ]
    for row in exchange_rows:
        lines.append(
            "\t".join(
                [
                    row["key"],
                    str(row["baseinfo_rows"]),
                    str(row["history_rows"]),
                    str(row["history_symbols"]),
                    str(row["missing_history_symbols"]),
                    str(row["baseinfo_last_updated_at"] or ""),
                    str(row["history_max_funding_time"] or ""),
                ]
            )
        )
    write_text(output_dir / "db_summary.txt", "\n".join(lines) + "\n")

    if missing_symbols:
        text_lines = []
        for key, symbols in sorted(missing_symbols.items()):
            text_lines.append(f"[{key}] {len(symbols)}")
            text_lines.extend(symbols)
            text_lines.append("")
        write_text(output_dir / "db_missing_history_symbols.txt", "\n".join(text_lines).rstrip() + "\n")
    else:
        write_text(output_dir / "db_missing_history_symbols.txt", "")

    return bundle


def fetch_api_payload(api_url: str, output_dir: Path) -> dict[str, Any]:
    try:
        with urlopen(api_url, timeout=10) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        error = {"ok": False, "api_url": api_url, "error": str(exc)}
        write_text(output_dir / "api_summary.json", json.dumps(error, ensure_ascii=False, indent=2))
        write_text(output_dir / "api_summary.txt", f"api_url = {api_url}\nerror = {exc}\n")
        return error

    write_text(output_dir / "api_data.json", json.dumps(payload, ensure_ascii=False))

    items = payload.get("items", [])
    exchanges = payload.get("exchanges", [])
    items_by_exchange = Counter()
    null_windows: dict[str, Counter[str]] = defaultdict(Counter)

    for item in items:
        exchange = str(item.get("exchange") or "")
        items_by_exchange[exchange] += 1
        sums = item.get("sums") or {}
        for key in WINDOW_KEYS:
            if sums.get(key) is None:
                null_windows[exchange][key] += 1

    summary = {
        "ok": True,
        "api_url": api_url,
        "generatedAt": payload.get("generatedAt"),
        "item_count": len(items),
        "exchange_count": len(exchanges),
        "items_by_exchange": dict(sorted(items_by_exchange.items())),
        "null_windows_by_exchange": {key: dict(value) for key, value in sorted(null_windows.items())},
    }
    write_text(output_dir / "api_summary.json", json.dumps(summary, ensure_ascii=False, indent=2))

    lines = [
        f"api_url = {api_url}",
        f"item_count = {summary['item_count']}",
        f"exchange_count = {summary['exchange_count']}",
        "",
        "items_by_exchange",
    ]
    for key, value in sorted(items_by_exchange.items()):
        lines.append(f"- {key}: {value}")
    lines.append("")
    lines.append("null_windows_by_exchange")
    for key, value in sorted(null_windows.items()):
        lines.append(f"- {key}: {dict(value)}")
    write_text(output_dir / "api_summary.txt", "\n".join(lines) + "\n")
    return summary


def build_report(
    output_dir: Path,
    *,
    service: str,
    hours: int,
    db_path: Path,
    tar_name: str,
    service_summary: dict[str, Any],
    journal_summary: dict[str, Any],
    db_summary: dict[str, Any],
    api_summary: dict[str, Any],
) -> None:
    lines = [
        "# Funding Runtime Diagnostic Report",
        "",
        f"- generated_at: {now_local().isoformat()}",
        f"- service: `{service}`",
        f"- window_hours: `{hours}`",
        f"- db_path: `{db_path}`",
        f"- archive: `{tar_name}`",
        "",
        "## Service",
        "",
        f"- status_returncode: `{service_summary['status']['returncode']}`",
        f"- is_active_returncode: `{service_summary['is_active']['returncode']}`",
        f"- journal_returncode: `{service_summary['journal']['returncode']}`",
        "",
        "## Journal Summary",
        "",
    ]
    if journal_summary.get("exists"):
        lines.extend(
            [
                f"- line_count: `{journal_summary['line_count']}`",
                f"- warn_count: `{journal_summary['warn_count']}`",
                f"- error_like_count: `{journal_summary['error_like_count']}`",
                f"- service_start_count: `{journal_summary['service_start_count']}`",
                f"- service_stop_count: `{journal_summary['service_stop_count']}`",
                f"- service_main_exit_count: `{journal_summary['service_main_exit_count']}`",
            ]
        )
        top_warn = journal_summary.get("top_warn_messages") or []
        if top_warn:
            lines.extend(["", "### Top Warnings", ""])
            for message, count in top_warn[:15]:
                lines.append(f"- `{count}` x {message}")
    else:
        lines.append("- journal missing")

    lines.extend(["", "## Database Summary", ""])
    if db_summary.get("exists"):
        for row in db_summary.get("exchanges", []):
            lines.append(
                "- "
                + f"{row['key']}: base={row['baseinfo_rows']}, history_rows={row['history_rows']}, "
                + f"history_symbols={row['history_symbols']}, missing_history={row['missing_history_symbols']}, "
                + f"history_max={row['history_max_funding_time'] or 'n/a'}"
            )
    else:
        lines.append("- database not found")

    lines.extend(["", "## API Summary", ""])
    if api_summary.get("ok"):
        lines.extend(
            [
                f"- item_count: `{api_summary['item_count']}`",
                f"- exchange_count: `{api_summary['exchange_count']}`",
            ]
        )
        for key, value in sorted(api_summary.get("items_by_exchange", {}).items()):
            lines.append(f"- {key}: {value}")
    else:
        lines.append(f"- api fetch failed: `{api_summary.get('error')}`")

    write_text(output_dir / "report.md", "\n".join(lines) + "\n")


def build_tarball(output_root: Path, bundle_dir: Path) -> Path:
    tar_path = output_root / f"{bundle_dir.name}.tar.gz"
    with tarfile.open(tar_path, "w:gz") as tar:
        tar.add(bundle_dir, arcname=bundle_dir.name)
    return tar_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export a funding runtime diagnostic bundle.")
    parser.add_argument("--hours", type=int, default=DEFAULT_HOURS, help="Journal lookback window in hours.")
    parser.add_argument("--service", default=DEFAULT_SERVICE, help="systemd service name.")
    parser.add_argument("--db-path", default=os.getenv("FUNDING_DB_PATH") or str(DEFAULT_DB_PATH), help="SQLite DB path.")
    parser.add_argument("--api-url", default=DEFAULT_API_URL, help="API endpoint for dashboard payload snapshot.")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT), help="Directory to place output bundle and tarball.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    timestamp = now_local().strftime("%Y%m%d_%H%M%S")
    output_root = Path(args.output_root).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    bundle_dir = output_root / f"funding_check_{timestamp}"
    bundle_dir.mkdir(parents=True, exist_ok=False)

    db_path = Path(args.db_path).expanduser().resolve()

    collect_system_info(bundle_dir)
    service_summary = collect_service_artifacts(args.service, args.hours, bundle_dir)
    journal_path = bundle_dir / f"journal_last_{args.hours}h.out.txt"
    journal_summary = summarize_journal(journal_path, bundle_dir)
    collect_repo_artifacts(bundle_dir)
    snapshot_path = backup_database(db_path, bundle_dir)
    db_summary = analyze_database(snapshot_path or db_path, bundle_dir)
    api_summary = fetch_api_payload(args.api_url, bundle_dir)

    tar_name = f"{bundle_dir.name}.tar.gz"
    build_report(
        bundle_dir,
        service=args.service,
        hours=args.hours,
        db_path=db_path,
        tar_name=tar_name,
        service_summary=service_summary,
        journal_summary=journal_summary,
        db_summary=db_summary,
        api_summary=api_summary,
    )
    tar_path = build_tarball(output_root, bundle_dir)

    print(f"bundle_dir={bundle_dir}")
    print(f"tar_path={tar_path}")
    print(f"db_snapshot={snapshot_path if snapshot_path else 'missing'}")


if __name__ == "__main__":
    main()
