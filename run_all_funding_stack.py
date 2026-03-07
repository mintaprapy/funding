#!/usr/bin/env python3
"""One-command scheduler for all funding collectors + dashboard."""

from __future__ import annotations

import argparse
import fcntl
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import time
import webbrowser
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, TextIO

from common_funding import tune_sqlite_connection
from funding_exchanges import baseinfo_script_paths, history_script_paths

ROOT = Path(__file__).resolve().parent
FUNDING_DB_PATH_ENV = "FUNDING_DB_PATH"
DEFAULT_DB_PATH = ROOT / "funding.db"

BASEINFO_SCRIPTS = baseinfo_script_paths(ROOT)
HISTORY_SCRIPTS = history_script_paths(ROOT)


@dataclass
class Job:
    name: str
    minutes: list[int]
    scripts: list[Path]
    next_run: datetime | None = None


def log(message: str) -> None:
    print(message, flush=True)


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def parse_minutes(raw: str) -> list[int]:
    vals: set[int] = set()
    for part in raw.split(","):
        item = part.strip()
        if not item:
            continue
        minute = int(item)
        if minute < 0 or minute > 59:
            raise ValueError(f"minute out of range: {minute}")
        vals.add(minute)
    out = sorted(vals)
    if not out:
        raise ValueError("minute list is empty")
    return out


def compute_next_run(after: datetime, minutes: list[int]) -> datetime:
    anchor = after.replace(second=0, microsecond=0)
    for minute in minutes:
        candidate = anchor.replace(minute=minute)
        if candidate > after:
            return candidate
    return (anchor + timedelta(hours=1)).replace(minute=minutes[0], second=0, microsecond=0)


def choose_available_port(host: str, preferred_port: int, max_scan: int = 20) -> int:
    for offset in range(max_scan + 1):
        port = preferred_port + offset
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind((host, port))
            except OSError:
                continue
            return port
    raise RuntimeError(f"no available port found in range {preferred_port}-{preferred_port + max_scan}")


def start_dashboard(python_bin: str, host: str, preferred_port: int) -> tuple[subprocess.Popen[str], int]:
    port = choose_available_port(host, preferred_port)
    cmd = [
        python_bin,
        str(ROOT / "allfunding_dashboard.py"),
        "--host",
        host,
        "--port",
        str(port),
    ]
    log(f"[{now_str()}] starting dashboard: {' '.join(cmd)}")
    proc = subprocess.Popen(cmd, cwd=ROOT, text=True)
    return proc, port


def run_script(
    python_bin: str,
    script: Path,
    *,
    max_attempts: int,
    retry_wait_s: float,
    timeout_s: float,
) -> bool:
    if not script.exists():
        log(f"[{now_str()}] [error] missing script: {script}")
        return False
    cmd = [python_bin, str(script)]
    attempts = max(1, max_attempts)
    timeout_val = timeout_s if timeout_s > 0 else None
    for attempt in range(1, attempts + 1):
        suffix = f" (attempt {attempt}/{attempts})" if attempts > 1 else ""
        log(f"[{now_str()}] run {script.relative_to(ROOT)}{suffix}")
        started = time.monotonic()
        try:
            result = subprocess.run(cmd, cwd=ROOT, text=True, timeout=timeout_val)
            code = int(result.returncode)
            timed_out = False
        except subprocess.TimeoutExpired:
            code = 124
            timed_out = True
        cost = time.monotonic() - started
        timeout_note = f" (timeout {timeout_val:g}s)" if timed_out and timeout_val else ""
        if code == 0:
            log(f"[{now_str()}] done {script.name} in {cost:.1f}s")
            return True
        if attempt < attempts:
            wait_s = max(0.5, retry_wait_s * attempt)
            log(
                f"[{now_str()}] [warn] {script.name} exited with code {code} after {cost:.1f}s{timeout_note}; "
                f"retry in {wait_s:.1f}s"
            )
            time.sleep(wait_s)
            continue
        log(f"[{now_str()}] [warn] {script.name} exited with code {code} after {cost:.1f}s{timeout_note}")
    return False


def run_batch(
    name: str,
    python_bin: str,
    scripts: list[Path],
    *,
    should_stop: Callable[[], bool] | None = None,
    max_attempts: int = 1,
    retry_wait_s: float = 3.0,
    script_timeout_s: float = 1800.0,
) -> None:
    log(f"[{now_str()}] ===== {name} batch start ({len(scripts)} scripts) =====")
    ok = 0
    started = time.monotonic()
    for script in scripts:
        if should_stop is not None and should_stop():
            log(f"[{now_str()}] [warn] stop requested, interrupting {name} batch")
            break
        if run_script(
            python_bin,
            script,
            max_attempts=max_attempts,
            retry_wait_s=retry_wait_s,
            timeout_s=script_timeout_s,
        ):
            ok += 1
    cost = time.monotonic() - started
    log(f"[{now_str()}] ===== {name} batch end: {ok}/{len(scripts)} success, {cost:.1f}s =====")


def stop_dashboard(proc: subprocess.Popen[str] | None) -> None:
    if proc is None:
        return
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=8)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=3)


def acquire_instance_lock(lock_file: Path) -> TextIO:
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    fp = lock_file.open("a+", encoding="utf-8")
    try:
        fcntl.flock(fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        fp.seek(0)
        holder = fp.read().strip() or "unknown"
        fp.close()
        raise RuntimeError(f"another scheduler instance is running (lock={lock_file}, holder={holder})")
    fp.seek(0)
    fp.truncate(0)
    fp.write(f"pid={os.getpid()} started_at={now_str()} cwd={ROOT}\n")
    fp.flush()
    return fp


def release_instance_lock(fp: TextIO | None) -> None:
    if fp is None:
        return
    try:
        fcntl.flock(fp.fileno(), fcntl.LOCK_UN)
    except OSError:
        pass
    try:
        fp.close()
    except OSError:
        pass


def prepare_sqlite_runtime(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        tune_sqlite_connection(conn)
        conn.commit()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="One-command funding scheduler + dashboard launcher")
    parser.add_argument("--python", default=sys.executable, help="Python interpreter path")
    parser.add_argument("--host", default="127.0.0.1", help="Dashboard host")
    parser.add_argument("--port", type=int, default=5000, help="Preferred dashboard port")
    parser.add_argument(
        "--baseinfo-minutes",
        default="1,11,21,31,41,51",
        help="Comma-separated minute list for baseinfo jobs",
    )
    parser.add_argument(
        "--history-minutes",
        default="3",
        help="Comma-separated minute list for history jobs",
    )
    parser.add_argument("--no-open-browser", action="store_true", help="Do not auto-open browser")
    parser.add_argument("--no-run-on-start", action="store_true", help="Do not run jobs immediately on startup")
    parser.add_argument("--skip-dashboard", action="store_true", help="Do not launch dashboard server")
    parser.add_argument("--once", action="store_true", help="Run startup batches once and exit")
    parser.add_argument(
        "--lock-file",
        default=str(ROOT / ".run_all_funding_stack.lock"),
        help="Single-instance lock file path",
    )
    parser.add_argument(
        "--script-timeout-seconds",
        type=float,
        default=1800.0,
        help="Per-script timeout in seconds (<=0 disables timeout)",
    )
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="Shared SQLite path used by the scheduler, collectors, and dashboard",
    )
    parser.add_argument(
        "--script-max-attempts",
        type=int,
        default=8,
        help="Max attempts per script before marking failure",
    )
    parser.add_argument(
        "--script-retry-wait",
        type=float,
        default=5.0,
        help="Base retry backoff seconds between failed attempts",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    baseinfo_minutes = parse_minutes(args.baseinfo_minutes)
    history_minutes = parse_minutes(args.history_minutes)

    stop_requested = False

    def _signal_handler(signum: int, _frame: object) -> None:
        nonlocal stop_requested
        stop_requested = True
        log(f"[{now_str()}] received signal {signum}, shutting down...")

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    dashboard_proc: subprocess.Popen[str] | None = None
    dashboard_port = args.port
    browser_opened = False
    lock_fp: TextIO | None = None

    jobs = [
        Job(name="baseinfo", minutes=baseinfo_minutes, scripts=BASEINFO_SCRIPTS),
        Job(name="history", minutes=history_minutes, scripts=HISTORY_SCRIPTS),
    ]

    try:
        try:
            lock_fp = acquire_instance_lock(Path(args.lock_file).resolve())
        except RuntimeError as exc:
            log(f"[{now_str()}] [error] {exc}")
            raise SystemExit(1)
        db_path = Path(args.db_path).expanduser().resolve()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        os.environ[FUNDING_DB_PATH_ENV] = str(db_path)
        log(f"[{now_str()}] runtime database: {db_path}")
        prepare_sqlite_runtime(db_path)

        if not args.skip_dashboard:
            dashboard_proc, dashboard_port = start_dashboard(args.python, args.host, args.port)
            time.sleep(0.8)
            if dashboard_proc.poll() is not None:
                raise RuntimeError("dashboard process exited immediately")
            if not args.no_open_browser:
                dashboard_url = f"http://{args.host}:{dashboard_port}"
                webbrowser.open(dashboard_url)
                browser_opened = True
                log(f"[{now_str()}] opened browser: {dashboard_url}")

        if not args.no_run_on_start:
            run_batch(
                "baseinfo(startup)",
                args.python,
                BASEINFO_SCRIPTS,
                should_stop=lambda: stop_requested,
                max_attempts=args.script_max_attempts,
                retry_wait_s=args.script_retry_wait,
                script_timeout_s=args.script_timeout_seconds,
            )
            if stop_requested:
                return
            run_batch(
                "history(startup)",
                args.python,
                HISTORY_SCRIPTS,
                should_stop=lambda: stop_requested,
                max_attempts=args.script_max_attempts,
                retry_wait_s=args.script_retry_wait,
                script_timeout_s=args.script_timeout_seconds,
            )
            if stop_requested:
                return

        if args.once:
            return

        now = datetime.now()
        for job in jobs:
            job.next_run = compute_next_run(now, job.minutes)
            log(f"[{now_str()}] next {job.name} run at {job.next_run.strftime('%Y-%m-%d %H:%M:%S')}")

        while not stop_requested:
            if dashboard_proc is not None and dashboard_proc.poll() is not None:
                log(f"[{now_str()}] [warn] dashboard exited with code {dashboard_proc.returncode}, restarting...")
                dashboard_proc, dashboard_port = start_dashboard(args.python, args.host, args.port)
                time.sleep(0.8)
                if not browser_opened and not args.no_open_browser:
                    dashboard_url = f"http://{args.host}:{dashboard_port}"
                    webbrowser.open(dashboard_url)
                    browser_opened = True
                    log(f"[{now_str()}] opened browser: {dashboard_url}")

            now = datetime.now()
            due_jobs = [job for job in jobs if job.next_run and now >= job.next_run]
            if due_jobs:
                due_jobs.sort(key=lambda j: j.next_run or now)
                for job in due_jobs:
                    run_batch(
                        job.name,
                        args.python,
                        job.scripts,
                        should_stop=lambda: stop_requested,
                        max_attempts=args.script_max_attempts,
                        retry_wait_s=args.script_retry_wait,
                        script_timeout_s=args.script_timeout_seconds,
                    )
                    if stop_requested:
                        break
                    job.next_run = compute_next_run(datetime.now(), job.minutes)
                    log(f"[{now_str()}] next {job.name} run at {job.next_run.strftime('%Y-%m-%d %H:%M:%S')}")
                if stop_requested:
                    break
                continue

            nearest = min(job.next_run for job in jobs if job.next_run is not None)
            wait_s = max(0.2, min((nearest - now).total_seconds(), 30.0))
            time.sleep(wait_s)

    finally:
        stop_dashboard(dashboard_proc)
        release_instance_lock(lock_fp)


if __name__ == "__main__":
    main()
