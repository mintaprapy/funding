#!/usr/bin/env python3
"""Simple web dashboard for Backpack funding data stored in funding.db."""

from __future__ import annotations

import os
import json
import sqlite3
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT_DIR = next(parent for parent in Path(__file__).resolve().parents if (parent / "start_all_funding.sh").exists())
DB_PATH = Path(os.getenv("FUNDING_DB_PATH") or (ROOT_DIR / "funding.db")).expanduser().resolve()
INFO_TABLE_CANDIDATES = ["backpack_funding_baseinfo"]
HISTORY_TABLE = "backpack_funding_history"
HOST = "0.0.0.0"
PORT = 9000

# key, window in ms, label for UI
WINDOWS = [
    ("h24", 24 * 60 * 60 * 1000, "过去 24 小时"),
    ("d3", 3 * 24 * 60 * 60 * 1000, "过去 3 天"),
    ("d7", 7 * 24 * 60 * 60 * 1000, "过去 7 天"),
    ("d15", 15 * 24 * 60 * 60 * 1000, "过去 15 天"),
    ("d30", 30 * 24 * 60 * 60 * 1000, "过去 30 天"),
]


def _to_number(val: Any) -> float | None:
    try:
        if val is None:
            return None
        return float(val)
    except (TypeError, ValueError):
        return None


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_base_info(conn: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    cur = conn.execute(
        f"""
        SELECT symbol, adjustedFundingRateCap, adjustedFundingRateFloor,
               fundingIntervalHours, markPrice, lastFundingRate, openInterest, updated_at
        FROM {table}
        ORDER BY symbol
        """
    )
    rows: list[dict[str, Any]] = []
    for row in cur.fetchall():
        rows.append(
            {
                "symbol": row["symbol"],
                "adjustedFundingRateCap": _to_number(row["adjustedFundingRateCap"]),
                "adjustedFundingRateFloor": _to_number(row["adjustedFundingRateFloor"]),
                "fundingIntervalHours": row["fundingIntervalHours"],
                "markPrice": _to_number(row["markPrice"]),
                "lastFundingRate": _to_number(row["lastFundingRate"]),
                "openInterest": _to_number(row["openInterest"]),
                "updated_at": row["updated_at"],
            }
        )
    return rows


def ensure_info_table(conn: sqlite3.Connection, table: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            symbol TEXT PRIMARY KEY,
            adjustedFundingRateCap TEXT,
            adjustedFundingRateFloor TEXT,
            fundingIntervalHours INTEGER,
            markPrice TEXT,
            lastFundingRate TEXT,
            openInterest TEXT,
            updated_at INTEGER
        )
        """
    )
    conn.commit()


def ensure_history_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (
            symbol TEXT NOT NULL,
            fundingTime INTEGER NOT NULL,
            fundingRate TEXT,
            updated_at INTEGER,
            PRIMARY KEY (symbol, fundingTime)
        )
        """
    )
    conn.commit()


def resolve_info_table(conn: sqlite3.Connection) -> str:
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    existing = {row[0] for row in cur.fetchall()}
    for name in INFO_TABLE_CANDIDATES:
        if name in existing:
            return name
    primary = INFO_TABLE_CANDIDATES[0]
    ensure_info_table(conn, primary)
    return primary


def fetch_cumulative_rates(conn: sqlite3.Connection, *, now_ms: int) -> dict[str, dict[str, float]]:
    params = [now_ms - span for _, span, _ in WINDOWS]
    case_parts = [
        f"COALESCE(SUM(CASE WHEN fundingTime >= ? THEN CAST(fundingRate AS REAL) ELSE 0 END), 0) AS {key}"
        for key, _, _ in WINDOWS
    ]
    sql = f"""
        SELECT symbol, {", ".join(case_parts)}
        FROM {HISTORY_TABLE}
        GROUP BY symbol
    """
    cur = conn.execute(sql, params)
    result: dict[str, dict[str, float]] = {}
    for row in cur.fetchall():
        result[row["symbol"]] = {key: float(row[key]) for key, _, _ in WINDOWS}
    return result


def build_payload() -> dict[str, Any]:
    now_ms = int(time.time() * 1000)
    with connect_db() as conn:
        info_table = resolve_info_table(conn)
        ensure_history_table(conn)
        base_rows = fetch_base_info(conn, info_table)
        sums_by_symbol = fetch_cumulative_rates(conn, now_ms=now_ms)

    items = []
    for row in base_rows:
        sums = sums_by_symbol.get(row["symbol"], {key: 0.0 for key, _, _ in WINDOWS})
        oi = row.get("openInterest")
        mp = row.get("markPrice")
        notional = oi * mp if oi is not None and mp is not None else None
        items.append({**row, "openInterestNotional": notional, "sums": sums})

    return {
        "generatedAt": now_ms,
        "windows": [{"key": key, "label": label, "spanMs": span} for key, span, label in WINDOWS],
        "items": items,
    }


def render_html() -> str:
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Backpack Funding Dashboard</title>
  <style>
    :root {{
      --bg: #0e172a;
      --card: rgba(255,255,255,0.04);
      --card-strong: rgba(255,255,255,0.08);
      --accent: #22d3ee;
      --accent-2: #7c3aed;
      --text: #e2e8f0;
      --muted: #94a3b8;
      --danger: #f43f5e;
      --success: #34d399;
      --border: rgba(255,255,255,0.08);
      --font: 'Space Grotesk', 'Manrope', 'SF Pro Display', 'Segoe UI', sans-serif;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: radial-gradient(circle at 20% 20%, rgba(34,211,238,0.08), transparent 35%),
                  radial-gradient(circle at 80% 0%, rgba(124,58,237,0.06), transparent 35%),
                  var(--bg);
      color: var(--text);
      font-family: var(--font);
      min-height: 100vh;
    }}
    .shell {{
      max-width: 1400px;
      margin: 0 auto;
      padding: 28px 20px 60px;
    }}
    header {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 22px;
    }}
    .hero {{
      display: flex;
      flex-direction: column;
      gap: 8px;
    }}
    h1 {{
      margin: 0;
      font-size: 32px;
      letter-spacing: -0.5px;
    }}
    .sub {{
      color: var(--muted);
      font-size: 15px;
      max-width: 820px;
      line-height: 1.5;
    }}
    .chip {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 12px;
      border-radius: 12px;
      background: linear-gradient(120deg, rgba(34,211,238,0.1), rgba(124,58,237,0.1));
      border: 1px solid var(--border);
      font-size: 14px;
    }}
    .controls {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      margin-bottom: 18px;
    }}
    .pill {{
      background: var(--card);
      border: 1px solid var(--border);
      padding: 10px 12px;
      border-radius: 12px;
      display: flex;
      align-items: center;
      gap: 10px;
      min-width: 220px;
    }}
    .pill label {{
      color: var(--muted);
      font-size: 13px;
    }}
    .pill input, .pill select {{
      flex: 1;
      background: transparent;
      border: none;
      color: var(--text);
      font-size: 14px;
      outline: none;
    }}
    .pill input::placeholder {{
      color: var(--muted);
    }}
    .badge {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 6px 10px;
      border-radius: 10px;
      background: var(--card);
      border: 1px solid var(--border);
      font-size: 13px;
    }}
    .range-row {{
      display: flex;
      flex-direction: column;
      gap: 6px;
      flex: 1;
      color: var(--muted);
      font-size: 13px;
    }}
    .range-row input[type="range"] {{
      width: 100%;
      accent-color: var(--accent);
    }}
    .table-wrap {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 16px;
      overflow: hidden;
      box-shadow: 0 20px 50px rgba(0,0,0,0.28);
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      min-width: 900px;
    }}
    thead {{
      background: rgba(255,255,255,0.04);
    }}
    th, td {{
      padding: 12px 14px;
      border-bottom: 1px solid var(--border);
      font-size: 14px;
    }}
    th {{
      text-align: center;
      font-size: 13px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--muted);
      position: sticky;
      top: 0;
      backdrop-filter: blur(8px);
      z-index: 1;
    }}
    tbody tr:hover {{
      background: var(--card-strong);
    }}
    .num {{
      font-family: 'Menlo', 'SFMono-Regular', Consolas, monospace;
      text-align: right;
    }}
    .pos {{ color: var(--success); }}
    .neg {{ color: var(--danger); }}
    .dim {{ color: var(--muted); }}
    .tag {{
      display: inline-flex;
      padding: 4px 8px;
      border-radius: 8px;
      font-size: 12px;
      border: 1px solid var(--border);
      background: rgba(255,255,255,0.03);
    }}
    .stack {{
      display: flex;
      flex-direction: column;
      align-items: flex-end;
      gap: 2px;
    }}
    .mini {{
      font-size: 12px;
      color: var(--muted);
    }}
    .footer {{
      margin-top: 18px;
      color: var(--muted);
      font-size: 13px;
    }}
    @media (max-width: 900px) {{
      header {{ flex-direction: column; align-items: flex-start; }}
      .controls {{ flex-direction: column; }}
      .pill {{ width: 100%; }}
      .shell {{ padding: 20px 16px 40px; }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <header>
      <div class="hero">
        <h1>Backpack Funding Dashboard</h1>
        <div class="sub">查看 Backpack 永续合约的基础信息、最新资金费率，以及过去 24 小时 / 3 天 / 7 天 / 15 天 / 30 天的累计资金费率。</div>
      </div>
      <div class="chip" id="summaryChip">加载中…</div>
    </header>

    <div class="controls">
      <div class="pill">
        <label for="searchBox">搜索交易对</label>
        <input id="searchBox" type="text" placeholder="如 BTCUSDT" />
      </div>
      <div class="pill">
        <label for="sortKey">排序</label>
        <select id="sortKey">
          <option value="symbol">按交易对</option>
          <option value="lastFundingRate">最新资金费率</option>
          <option value="openInterestNotional">未平仓价值</option>
          <option value="h24">24 小时累计</option>
          <option value="d3">3 天累计</option>
          <option value="d7">7 天累计</option>
          <option value="d15">15 天累计</option>
          <option value="d30">30 天累计</option>
        </select>
      </div>
      <div class="badge" id="lastUpdated">更新中…</div>
      <div class="range-row">
        <div>持仓量过滤 (百万 USDT)：<span id="oiFilterLabel">0</span>+</div>
        <input id="oiFilter" type="range" min="0" max="6" step="1" value="0" list="oiTicks" />
        <datalist id="oiTicks">
          <option value="0" label="0"></option>
          <option value="1" label="1"></option>
          <option value="2" label="3"></option>
          <option value="3" label="10"></option>
          <option value="4" label="30"></option>
          <option value="5" label="50"></option>
          <option value="6" label="100"></option>
        </datalist>
      </div>
    </div>

    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th data-key="symbol" data-label="交易对">交易对</th>
            <th data-key="markPrice" data-label="Mark">Mark</th>
            <th data-key="openInterestNotional" data-label="OI (M)">OI (M)</th>
            <th data-key="lastFundingRate" data-label="最新资金费">最新资金费</th>
            <th data-key="fundingIntervalHours" data-label="周期">周期</th>
            <th data-key="bounds" data-label="上/下限">上/下限</th>
            {''.join(f'<th data-key="{key}" data-label="{label}">{label}</th>' for key, _, label in WINDOWS)}
          </tr>
        </thead>
        <tbody id="table-body"></tbody>
      </table>
    </div>
    <div class="footer" id="footerText"></div>
  </div>

  <script>
    const WINDOWS_META = {json.dumps([{"key": key, "label": label, "spanMs": span} for key, span, label in WINDOWS])};
    const WINDOW_KEYS = WINDOWS_META.map(w => w.key);
    const toPct = (v) => v == null || Number.isNaN(v) ? '—' : (v * 100).toFixed(4) + '%';
    const toPctFixed = (v, digits = 2) => v == null || Number.isNaN(v) ? '—' : (v * 100).toFixed(digits) + '%';
    const toNum = (v, digits = 2) => v == null || Number.isNaN(v) ? '—' : Number(v).toLocaleString(undefined, {{ maximumFractionDigits: digits }});
    const formatMarkPrice = (v) => {{
      if (v == null || Number.isNaN(v)) return '—';
      const absV = Math.abs(Number(v));
      if (absV > 0 && absV < 0.0001) {{
        return Number(v).toFixed(8);
      }}
      return toNum(v, 4);
    }};
    const classFor = (v) => v > 0 ? 'pos' : v < 0 ? 'neg' : 'dim';

    const OI_THRESHOLDS = [0, 1, 3, 10, 30, 50, 100]; // million

    let dataCache = [];
    let sortKey = 'symbol';
    let sortDir = 'asc';
    let oiThresholdIdx = 0;

    async function load() {{
      const res = await fetch('/api/data');
      if (!res.ok) throw new Error('数据获取失败');
      const payload = await res.json();
      dataCache = payload.items || [];
      updateMeta(payload);
      render();
    }}

    function updateMeta(payload) {{
      const chip = document.getElementById('summaryChip');
      const footer = document.getElementById('footerText');
      const updated = new Date(payload.generatedAt || Date.now());
      const count = dataCache.length;
      chip.textContent = `跟踪 ${{count}} 个交易对`;
      const timeStr = updated.toLocaleString();
      document.getElementById('lastUpdated').textContent = `生成时间：${{timeStr}}`;
      footer.textContent = '数据来源：共享 funding.db；资金费率为自然数值 (0.0001 = 0.01%)，累计为窗口内求和。';
    }}

    function getSortValue(item, key) {{
      if (!key || key === 'symbol') return item.symbol;
      if (['lastFundingRate', 'openInterestNotional', 'markPrice', 'fundingIntervalHours'].includes(key)) {{
        return Number(item[key] ?? 0);
      }}
      if (key === 'bounds') {{
        return Number(item.adjustedFundingRateCap ?? 0);
      }}
      if (item.sums && key in item.sums) {{
        return Number(item.sums[key] ?? 0);
      }}
      return 0;
    }}

    function updateSortIndicators() {{
      document.querySelectorAll('th[data-key]').forEach(th => {{
        const base = th.dataset.label || th.textContent.trim();
        const arrow = th.dataset.key === sortKey ? (sortDir === 'asc' ? '↑' : '↓') : '';
        th.textContent = arrow ? `${{base}} ${{arrow}}` : base;
      }});
    }}

    function render() {{
      const body = document.getElementById('table-body');
      const q = document.getElementById('searchBox').value.trim().toUpperCase();
      const threshold = OI_THRESHOLDS[oiThresholdIdx] * 1_000_000;
      const filtered = dataCache.filter(item => {{
        const hitSymbol = !q || (item.symbol || '').toUpperCase().includes(q);
        const notional = item.openInterestNotional ?? 0;
        const hitOi = notional >= threshold;
        return hitSymbol && hitOi;
      }});
      const sorted = filtered.sort((a, b) => {{
        const va = getSortValue(a, sortKey);
        const vb = getSortValue(b, sortKey);
        if (typeof va === 'string' && typeof vb === 'string') return sortDir === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va);
        return sortDir === 'asc' ? va - vb : vb - va;
      }});

      if (!sorted.length) {{
        body.innerHTML = '<tr><td colspan="99" class="dim">暂无数据，先运行 backpack_funding_baseinfo.py 与 backpack_funding_history.py</td></tr>';
        return;
      }}

      body.innerHTML = sorted.map(item => {{
        const sums = item.sums || {{}};
        const windowCells = WINDOW_KEYS.map(key => {{
          const v = sums[key];
          const meta = WINDOWS_META.find(w => w.key === key) || {{}};
          const days = meta.spanMs ? meta.spanMs / 86400000 : null;
          const ann = days ? (v ?? 0) / days * 365 : null;
          return `<td class="num"><div class="stack"><span class="${'{'}classFor(v){'}'}">${'{'}toPct(v){'}'}</span><span class="mini ${'{'}classFor(ann){'}'}">APR ${'{'}toPctFixed(ann, 2){'}'}</span></div></td>`;
        }}).join('');
        const notionalDisplay = item.openInterestNotional == null ? null : item.openInterestNotional / 1_000_000;
        const latestAnn = item.fundingIntervalHours ? (item.lastFundingRate ?? 0) * (24 / item.fundingIntervalHours) * 365 : null;
        return `
          <tr>
            <td><span class="tag">${{item.symbol}}</span></td>
            <td class="num">${{formatMarkPrice(item.markPrice)}}</td>
            <td class="num">${{toNum(notionalDisplay, 2)}}</td>
            <td class="num"><div class="stack"><span class="${'{'}classFor(item.lastFundingRate){'}'}">${'{'}toPct(item.lastFundingRate){'}'}</span><span class="mini ${'{'}classFor(latestAnn){'}'}">APR ${'{'}toPctFixed(latestAnn, 2){'}'}</span></div></td>
            <td class="num">${{item.fundingIntervalHours ? item.fundingIntervalHours + 'h' : '—'}}</td>
            <td class="num">${{toPctFixed(item.adjustedFundingRateCap, 2)}} / ${{toPctFixed(item.adjustedFundingRateFloor, 2)}}</td>
            ${'{'}windowCells{'}'}
          </tr>
        `;
      }}).join('');
      updateSortIndicators();
    }}

    document.getElementById('searchBox').addEventListener('input', render);
    const dropdown = document.getElementById('sortKey');
    dropdown.addEventListener('change', (e) => {{
      sortKey = e.target.value;
      sortDir = sortKey === 'symbol' ? 'asc' : 'desc';
      render();
    }});

    const oiFilter = document.getElementById('oiFilter');
    const oiFilterLabel = document.getElementById('oiFilterLabel');
    const updateOiLabel = () => {{
      const val = OI_THRESHOLDS[oiThresholdIdx];
      oiFilterLabel.textContent = `${{val}}`;
    }};
    oiFilter.addEventListener('input', (e) => {{
      oiThresholdIdx = Number(e.target.value) || 0;
      updateOiLabel();
      render();
    }});
    updateOiLabel();

    document.querySelectorAll('th[data-key]').forEach(th => {{
      th.style.cursor = 'pointer';
      th.addEventListener('click', () => {{
        const key = th.dataset.key;
        if (key === sortKey) {{
          sortDir = sortDir === 'asc' ? 'desc' : 'asc';
        }} else {{
          sortKey = key;
          sortDir = key === 'symbol' ? 'asc' : 'desc';
          dropdown.value = key;
        }}
        render();
      }});
    }});
    updateSortIndicators();
    load().catch(err => {{
      document.getElementById('table-body').innerHTML = `<tr><td colspan="99" class="dim">加载失败：${{err.message}}</td></tr>`;
    }});
  </script>
</body>
</html>
"""


class DashboardHandler(BaseHTTPRequestHandler):
    def _send_json(self, payload: dict[str, Any], status: int = 200) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_html(self, content: str) -> None:
        data = content.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        if path in ("/", "/index.html"):
            self._send_html(render_html())
            return
        if path == "/api/data":
            try:
                payload = build_payload()
            except Exception as exc:  # noqa: BLE001
                self._send_json({"error": str(exc)}, status=500)
                return
            self._send_json(payload)
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return


def main() -> None:
    server = ThreadingHTTPServer((HOST, PORT), DashboardHandler)
    print(f"Serving dashboard at http://{HOST}:{PORT} (Ctrl+C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping server...")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()

