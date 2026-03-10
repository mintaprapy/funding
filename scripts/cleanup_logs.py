#!/usr/bin/env python3
"""Clean generated runtime artifacts from local test log directories."""

from __future__ import annotations

import shutil
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
LOGS_DIR = ROOT_DIR / "logs"

CONTROL_FILENAMES = {
    "monitor_local_run.py",
    "controller.sh",
    "launcher.pid",
    "monitor.pid",
    "caffeinate.pid",
    "service.plist",
    "monitor.plist",
    "caffeinate.plist",
    "caffeinate.log",
}


def cleanup_run_dir(run_dir: Path) -> tuple[str, list[str]]:
    report_exists = (run_dir / "report.md").exists()
    if not report_exists:
        shutil.rmtree(run_dir)
        return "deleted_dir", [str(run_dir)]

    removed: list[str] = []
    for path in sorted(run_dir.iterdir()):
        if path.is_file() and path.name in CONTROL_FILENAMES:
            path.unlink()
            removed.append(path.name)
    return "cleaned_dir", removed


def main() -> None:
    if not LOGS_DIR.exists():
        print("logs directory not found, nothing to do")
        return

    total_deleted = 0
    total_cleaned = 0
    for run_dir in sorted(LOGS_DIR.glob("local_*")):
        if not run_dir.is_dir():
            continue
        action, removed = cleanup_run_dir(run_dir)
        if action == "deleted_dir":
            total_deleted += 1
            print(f"deleted: {removed[0]}")
        else:
            total_cleaned += 1
            if removed:
                print(f"cleaned: {run_dir} -> {', '.join(removed)}")

    print(f"summary: cleaned={total_cleaned} deleted={total_deleted}")


if __name__ == "__main__":
    main()
