#!/usr/bin/env python3
"""Long-run supervisor for funding stack.

Runs `start_all_funding.sh` for N hours, monitors logs in real time, and
optionally repeats rounds until no script-level failures are observed.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import select
import signal
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TextIO

FAIL_RE = re.compile(r"\[warn\]\s+([A-Za-z0-9_./-]+\.py)\s+exited with code\s+(-?\d+)")
DONE_RE = re.compile(r"\bdone\s+([A-Za-z0-9_./-]+\.py)\s+in\b")
BATCH_END_RE = re.compile(r"=+\s+([a-z]+\(startup\))\s+batch end:\s+(\d+)/(\d+)\s+success")


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


@dataclass
class RoundResult:
    round_idx: int
    runtime_sec: float
    returncode: int
    fail_count: int
    fail_scripts: list[str]
    log_path: Path
    baseinfo_batch_line: str | None
    history_batch_line: str | None

    @property
    def clean(self) -> bool:
        return self.returncode == 0 and self.fail_count == 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run and monitor funding stack for long duration.")
    parser.add_argument("--duration-hours", type=float, default=5.0, help="Run duration per round.")
    parser.add_argument("--max-rounds", type=int, default=6, help="Maximum rounds when until-clean is enabled.")
    parser.add_argument(
        "--until-clean",
        action="store_true",
        help="Repeat rounds until fail_count=0 (or max-rounds reached).",
    )
    parser.add_argument(
        "--root",
        default=str(Path(__file__).resolve().parent),
        help="Funding project root directory.",
    )
    parser.add_argument(
        "--out-dir",
        default="/tmp",
        help="Directory to write supervisor logs and summaries.",
    )
    parser.add_argument("--no-open-browser", action="store_true", help="Pass --no-open-browser to launcher.")
    parser.add_argument(
        "--launcher-extra",
        action="append",
        default=[],
        help="Extra arg passed through to start_all_funding.sh (can be repeated).",
    )
    return parser.parse_args()


def _send_signal_to_group(proc: subprocess.Popen[str], sig: int) -> None:
    try:
        os.killpg(proc.pid, sig)
    except ProcessLookupError:
        return


def stop_process_group(proc: subprocess.Popen[str], *, grace_sec: float = 90.0) -> int:
    if proc.poll() is not None:
        return int(proc.returncode or 0)

    _send_signal_to_group(proc, signal.SIGINT)
    t0 = time.monotonic()
    while time.monotonic() - t0 < grace_sec:
        if proc.poll() is not None:
            return int(proc.returncode or 0)
        time.sleep(0.2)

    _send_signal_to_group(proc, signal.SIGTERM)
    t1 = time.monotonic()
    while time.monotonic() - t1 < 20.0:
        if proc.poll() is not None:
            return int(proc.returncode or 0)
        time.sleep(0.2)

    _send_signal_to_group(proc, signal.SIGKILL)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass
    return int(proc.returncode or -9)


def monitor_round(
    *,
    round_idx: int,
    root: Path,
    out_dir: Path,
    duration_hours: float,
    no_open_browser: bool,
    launcher_extra: list[str],
) -> RoundResult:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = out_dir / f"funding_supervisor_round{round_idx}_{ts}.log"
    duration_sec = max(1.0, duration_hours * 3600.0)

    cmd = ["./start_all_funding.sh"]
    if no_open_browser:
        cmd.append("--no-open-browser")
    if launcher_extra:
        cmd.extend(launcher_extra)

    with log_path.open("w", encoding="utf-8") as log_fp:
        log_fp.write(f"[{now_str()}] supervisor start round={round_idx} duration_hours={duration_hours}\n")
        log_fp.flush()

        proc = subprocess.Popen(
            cmd,
            cwd=root,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            preexec_fn=os.setsid,
        )

        script_last_status: dict[str, str] = {}
        baseinfo_batch_line: str | None = None
        history_batch_line: str | None = None
        started = time.monotonic()
        stopped_by_timeout = False

        assert proc.stdout is not None
        while True:
            now = time.monotonic()
            if now - started >= duration_sec:
                stopped_by_timeout = True
                break

            if proc.poll() is not None:
                break

            readable, _, _ = select.select([proc.stdout], [], [], 0.5)
            if not readable:
                continue

            line = proc.stdout.readline()
            if not line:
                continue
            log_fp.write(line)
            log_fp.flush()

            m = FAIL_RE.search(line)
            if m:
                script_last_status[m.group(1)] = "fail"
            d = DONE_RE.search(line)
            if d:
                script_last_status[d.group(1)] = "success"

            b = BATCH_END_RE.search(line)
            if b:
                if b.group(1).startswith("baseinfo"):
                    baseinfo_batch_line = line.strip()
                if b.group(1).startswith("history"):
                    history_batch_line = line.strip()

        if stopped_by_timeout:
            log_fp.write(f"[{now_str()}] supervisor timeout reached, stopping process group\n")
            log_fp.flush()
            returncode = stop_process_group(proc)
        else:
            returncode = int(proc.wait(timeout=30))

        # Drain remaining stdout quickly.
        try:
            while True:
                line = proc.stdout.readline()
                if not line:
                    break
                log_fp.write(line)
                m = FAIL_RE.search(line)
                if m:
                    script_last_status[m.group(1)] = "fail"
                d = DONE_RE.search(line)
                if d:
                    script_last_status[d.group(1)] = "success"
                b = BATCH_END_RE.search(line)
                if b:
                    if b.group(1).startswith("baseinfo"):
                        baseinfo_batch_line = line.strip()
                    if b.group(1).startswith("history"):
                        history_batch_line = line.strip()
        except Exception:
            pass
        log_fp.flush()

    unresolved_fail = sorted(name for name, status in script_last_status.items() if status == "fail")
    runtime_sec = time.monotonic() - started
    return RoundResult(
        round_idx=round_idx,
        runtime_sec=runtime_sec,
        returncode=returncode,
        fail_count=len(unresolved_fail),
        fail_scripts=unresolved_fail,
        log_path=log_path,
        baseinfo_batch_line=baseinfo_batch_line,
        history_batch_line=history_batch_line,
    )


def write_summary(out_dir: Path, results: list[RoundResult]) -> Path:
    summary_path = out_dir / "funding_supervisor_summary.json"
    payload = {
        "generated_at": now_str(),
        "rounds": [
            {
                "round_idx": r.round_idx,
                "runtime_sec": round(r.runtime_sec, 1),
                "returncode": r.returncode,
                "clean": r.clean,
                "fail_count": r.fail_count,
                "fail_scripts": r.fail_scripts,
                "log_path": str(r.log_path),
                "baseinfo_batch": r.baseinfo_batch_line,
                "history_batch": r.history_batch_line,
            }
            for r in results
        ],
    }
    summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary_path


def print_round(fp: TextIO, result: RoundResult) -> None:
    fp.write(
        f"[{now_str()}] round={result.round_idx} runtime={result.runtime_sec:.1f}s "
        f"returncode={result.returncode} clean={result.clean} fail_count={result.fail_count} "
        f"log={result.log_path}\n"
    )
    if result.baseinfo_batch_line:
        fp.write(f"[{now_str()}] {result.baseinfo_batch_line}\n")
    if result.history_batch_line:
        fp.write(f"[{now_str()}] {result.history_batch_line}\n")
    if result.fail_scripts:
        fp.write(f"[{now_str()}] fail_scripts={','.join(result.fail_scripts)}\n")
    fp.flush()


def main() -> None:
    args = parse_args()
    root = Path(args.root).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not (root / "start_all_funding.sh").exists():
        raise FileNotFoundError(f"start_all_funding.sh not found under {root}")

    rounds = max(1, args.max_rounds)
    results: list[RoundResult] = []
    for idx in range(1, rounds + 1):
        result = monitor_round(
            round_idx=idx,
            root=root,
            out_dir=out_dir,
            duration_hours=float(args.duration_hours),
            no_open_browser=bool(args.no_open_browser),
            launcher_extra=[str(x) for x in args.launcher_extra],
        )
        results.append(result)
        print_round(fp=os.sys.stdout, result=result)
        summary_path = write_summary(out_dir, results)
        print(f"[{now_str()}] summary={summary_path}", flush=True)

        if result.clean:
            print(f"[{now_str()}] round={idx} clean, stop", flush=True)
            return
        if result.returncode != 0:
            print(f"[{now_str()}] round={idx} launcher exited with returncode={result.returncode}", flush=True)
        if not args.until_clean:
            print(f"[{now_str()}] until-clean disabled, stop with failures", flush=True)
            return
        if idx >= rounds:
            print(f"[{now_str()}] reached max-rounds={rounds}, stop with failures", flush=True)
            return

        # Small cooldown before next round.
        time.sleep(8.0)


if __name__ == "__main__":
    main()
