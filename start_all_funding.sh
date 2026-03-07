#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"
export PYTHONUNBUFFERED=1

exec python3 "$ROOT_DIR/run_all_funding_stack.py" "$@"
