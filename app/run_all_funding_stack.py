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

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.common_funding import tune_sqlite_connection
from core.funding_exchanges import (
    EXCHANGE_CONFIG_ENV,
    baseinfo_script_paths,
    default_exchange_config_path,
    enabled_exchanges,
    history_script_paths,
)

FUNDING_DB_PATH_ENV = "FUNDING_DB_PATH"
FUNDING_ALERT_CONFIG_ENV = "FUNDING_ALERT_CONFIG"
DEFAULT_DB_PATH = ROOT / "funding.db"
DEFAULT_ALERT_CONFIG = ROOT / "config" / "alerts.json"
BASEINFO_FRAGILE_EXCHANGES = frozenset({"grvt", "standx"})
HISTORY_FRAGILE_EXCHANGES = frozenset({"grvt", "edgex"})
HISTORY_DEDICATED_LANES = {"binance": "binance"}

@dataclass
class Job:
    name: str
    minutes: list[int]
    scripts: list[Path]
    next_run: datetime | None = None


@dataclass(frozen=True)
class BatchTask:
    script: Path
    exchange_key: str
    lane: str
    order: int


@dataclass
class PendingTask:
    task: BatchTask
    attempt: int
    ready_at: float = 0.0


@dataclass
class RunningTask:
    pending: PendingTask
    proc: subprocess.Popen[str]
    started_at: float


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
        "-m",
        "app.allfunding_dashboard",
        "--host",
        host,
        "--port",
        str(port),
    ]
    log(f"[{now_str()}] starting dashboard: {' '.join(cmd)}")
    proc = subprocess.Popen(cmd, cwd=ROOT, text=True)
    return proc, port


def normalize_worker_count(value: int) -> int:
    return max(1, int(value))


def batch_lane_limits(
    batch_name: str,
    *,
    baseinfo_general_workers: int,
    history_general_workers: int,
    fragile_workers: int,
) -> dict[str, int]:
    if batch_name.startswith("history"):
        return {
            "binance": 1,
            "fragile": normalize_worker_count(fragile_workers),
            "general": normalize_worker_count(history_general_workers),
        }
    return {
        "fragile": normalize_worker_count(fragile_workers),
        "general": normalize_worker_count(baseinfo_general_workers),
    }


def task_lane(batch_name: str, exchange_key: str) -> str:
    if batch_name.startswith("history"):
        dedicated = HISTORY_DEDICATED_LANES.get(exchange_key)
        if dedicated is not None:
            return dedicated
        if exchange_key in HISTORY_FRAGILE_EXCHANGES:
            return "fragile"
        return "general"
    if exchange_key in BASEINFO_FRAGILE_EXCHANGES:
        return "fragile"
    return "general"


def build_batch_tasks(
    batch_name: str,
    scripts: list[Path],
    script_exchange_keys: dict[Path, str] | None,
) -> list[BatchTask]:
    tasks: list[BatchTask] = []
    for index, script in enumerate(scripts):
        exchange_key = (
            script_exchange_keys.get(script.resolve(), script.parent.name)
            if script_exchange_keys is not None
            else script.parent.name
        )
        tasks.append(
            BatchTask(
                script=script,
                exchange_key=exchange_key,
                lane=task_lane(batch_name, exchange_key),
                order=index,
            )
        )
    return tasks


def stop_subprocess(proc: subprocess.Popen[str], *, grace_seconds: float = 3.0) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=grace_seconds)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=grace_seconds)


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
    script_exchange_keys: dict[Path, str] | None = None,
    baseinfo_general_workers: int = 4,
    history_general_workers: int = 2,
    fragile_workers: int = 1,
) -> None:
    log(f"[{now_str()}] ===== {name} batch start ({len(scripts)} scripts) =====")
    if not scripts:
        log(f"[{now_str()}] ===== {name} batch end: 0/0 success, 0.0s =====")
        return
    ok = 0
    started = time.monotonic()
    timeout_val = script_timeout_s if script_timeout_s > 0 else None
    lane_limits = batch_lane_limits(
        name,
        baseinfo_general_workers=baseinfo_general_workers,
        history_general_workers=history_general_workers,
        fragile_workers=fragile_workers,
    )
    pending: list[PendingTask] = [
        PendingTask(task=task, attempt=1)
        for task in build_batch_tasks(name, scripts, script_exchange_keys)
    ]
    running: list[RunningTask] = []
    stop_logged = False
    max_attempts = max(1, max_attempts)

    while pending or running:
        stop_requested = should_stop is not None and should_stop()
        if stop_requested and not stop_logged:
            log(f"[{now_str()}] [warn] stop requested, interrupting {name} batch")
            stop_logged = True
            for job in running:
                stop_subprocess(job.proc)

        now_mono = time.monotonic()
        made_progress = False

        for job in running[:]:
            code = job.proc.poll()
            timed_out = False
            if code is None and timeout_val is not None and now_mono - job.started_at >= timeout_val:
                stop_subprocess(job.proc)
                code = 124
                timed_out = True
            if code is None:
                continue

            running.remove(job)
            made_progress = True
            cost = time.monotonic() - job.started_at
            timeout_note = f" (timeout {timeout_val:g}s)" if timed_out and timeout_val else ""
            if code == 0:
                ok += 1
                log(f"[{now_str()}] done {job.pending.task.script.name} in {cost:.1f}s")
                continue

            if not stop_requested and job.pending.attempt < max_attempts:
                wait_s = max(0.5, retry_wait_s * job.pending.attempt)
                log(
                    f"[{now_str()}] [warn] {job.pending.task.script.name} exited with code {code} after {cost:.1f}s"
                    f"{timeout_note}; retry in {wait_s:.1f}s"
                )
                pending.append(
                    PendingTask(
                        task=job.pending.task,
                        attempt=job.pending.attempt + 1,
                        ready_at=time.monotonic() + wait_s,
                    )
                )
                continue

            log(f"[{now_str()}] [warn] {job.pending.task.script.name} exited with code {code} after {cost:.1f}s{timeout_note}")

        if stop_requested:
            if running:
                time.sleep(0.2)
                continue
            break

        pending.sort(key=lambda item: (item.ready_at, item.task.order))
        lane_counts: dict[str, int] = {}
        active_exchanges = set()
        for job in running:
            lane_counts[job.pending.task.lane] = lane_counts.get(job.pending.task.lane, 0) + 1
            active_exchanges.add(job.pending.task.exchange_key)

        for item in pending[:]:
            if item.ready_at > now_mono:
                continue
            lane_limit = lane_limits.get(item.task.lane, 1)
            if lane_counts.get(item.task.lane, 0) >= lane_limit:
                continue
            if item.task.exchange_key in active_exchanges:
                continue
            if not item.task.script.exists():
                log(f"[{now_str()}] [error] missing script: {item.task.script}")
                pending.remove(item)
                made_progress = True
                continue

            cmd = [python_bin, str(item.task.script)]
            suffix = f" (attempt {item.attempt}/{max_attempts}, lane={item.task.lane})" if max_attempts > 1 else f" (lane={item.task.lane})"
            log(f"[{now_str()}] run {item.task.script.relative_to(ROOT)}{suffix}")
            proc = subprocess.Popen(
                cmd,
                cwd=ROOT,
                text=True,
                stdin=subprocess.DEVNULL,
            )
            running.append(RunningTask(pending=item, proc=proc, started_at=time.monotonic()))
            lane_counts[item.task.lane] = lane_counts.get(item.task.lane, 0) + 1
            active_exchanges.add(item.task.exchange_key)
            pending.remove(item)
            made_progress = True

        if not made_progress:
            time.sleep(0.2)

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
        "--exchange-config",
        default=str(default_exchange_config_path(ROOT)),
        help="JSON config path controlling enabled exchanges (defaults to config/exchanges.json)",
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
    parser.add_argument(
        "--baseinfo-general-workers",
        type=int,
        default=4,
        help="Concurrent workers for non-fragile baseinfo scripts",
    )
    parser.add_argument(
        "--history-general-workers",
        type=int,
        default=2,
        help="Concurrent workers for non-fragile history scripts",
    )
    parser.add_argument(
        "--fragile-workers",
        type=int,
        default=1,
        help="Concurrent workers for fragile exchange lanes",
    )
    parser.add_argument(
        "--alert-config",
        default=os.getenv(FUNDING_ALERT_CONFIG_ENV, str(DEFAULT_ALERT_CONFIG)),
        help="JSON config path controlling alert thresholds/providers (defaults to FUNDING_ALERT_CONFIG or config/alerts.json)",
    )
    parser.add_argument(
        "--alert-minutes",
        default="0,5,10,15,20,25,30,35,40,45,50,55",
        help="Comma-separated minute list for alert checks",
    )
    parser.add_argument("--disable-alerts", action="store_true", help="Do not run alert checks")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    baseinfo_minutes = parse_minutes(args.baseinfo_minutes)
    history_minutes = parse_minutes(args.history_minutes)
    alert_minutes = parse_minutes(args.alert_minutes)
    exchange_config_path = Path(args.exchange_config).expanduser().resolve()
    alert_config_path = Path(args.alert_config).expanduser().resolve()
    try:
        selected_exchanges = enabled_exchanges(ROOT, exchange_config_path)
    except RuntimeError as exc:
        log(f"[{now_str()}] [error] {exc}")
        raise SystemExit(1)
    baseinfo_scripts = baseinfo_script_paths(ROOT, exchange_config_path)
    history_scripts = history_script_paths(ROOT, exchange_config_path)
    script_exchange_keys: dict[Path, str] = {}
    for item in selected_exchanges:
        script_exchange_keys[(ROOT / item.folder / item.baseinfo_script).resolve()] = item.key
        script_exchange_keys[(ROOT / item.folder / item.history_script).resolve()] = item.key
    alert_script = (ROOT / "app" / "funding_alerts.py").resolve()
    script_exchange_keys[alert_script] = "__alerts__"

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
        Job(name="baseinfo", minutes=baseinfo_minutes, scripts=baseinfo_scripts),
        Job(name="history", minutes=history_minutes, scripts=history_scripts),
    ]
    if not args.disable_alerts:
        jobs.append(Job(name="alerts", minutes=alert_minutes, scripts=[alert_script]))

    try:
        try:
            lock_fp = acquire_instance_lock(Path(args.lock_file).resolve())
        except RuntimeError as exc:
            log(f"[{now_str()}] [error] {exc}")
            raise SystemExit(1)
        db_path = Path(args.db_path).expanduser().resolve()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        os.environ[FUNDING_DB_PATH_ENV] = str(db_path)
        os.environ[EXCHANGE_CONFIG_ENV] = str(exchange_config_path)
        os.environ[FUNDING_ALERT_CONFIG_ENV] = str(alert_config_path)
        log(f"[{now_str()}] runtime database: {db_path}")
        log(
            f"[{now_str()}] enabled exchanges ({len(selected_exchanges)}): "
            + ", ".join(item.label for item in selected_exchanges)
        )
        log(f"[{now_str()}] exchange config: {exchange_config_path}")
        if not args.disable_alerts:
            log(f"[{now_str()}] alert config: {alert_config_path}")
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
                baseinfo_scripts,
                should_stop=lambda: stop_requested,
                max_attempts=args.script_max_attempts,
                retry_wait_s=args.script_retry_wait,
                script_timeout_s=args.script_timeout_seconds,
                script_exchange_keys=script_exchange_keys,
                baseinfo_general_workers=args.baseinfo_general_workers,
                history_general_workers=args.history_general_workers,
                fragile_workers=args.fragile_workers,
            )
            if stop_requested:
                return
            run_batch(
                "history(startup)",
                args.python,
                history_scripts,
                should_stop=lambda: stop_requested,
                max_attempts=args.script_max_attempts,
                retry_wait_s=args.script_retry_wait,
                script_timeout_s=args.script_timeout_seconds,
                script_exchange_keys=script_exchange_keys,
                baseinfo_general_workers=args.baseinfo_general_workers,
                history_general_workers=args.history_general_workers,
                fragile_workers=args.fragile_workers,
            )
            if stop_requested:
                return
            if not args.disable_alerts:
                run_batch(
                    "alerts(startup)",
                    args.python,
                    [alert_script],
                    should_stop=lambda: stop_requested,
                    max_attempts=1,
                    retry_wait_s=args.script_retry_wait,
                    script_timeout_s=30.0,
                    script_exchange_keys=script_exchange_keys,
                    baseinfo_general_workers=args.baseinfo_general_workers,
                    history_general_workers=args.history_general_workers,
                    fragile_workers=args.fragile_workers,
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
                        script_exchange_keys=script_exchange_keys,
                        baseinfo_general_workers=args.baseinfo_general_workers,
                        history_general_workers=args.history_general_workers,
                        fragile_workers=args.fragile_workers,
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
