#!/usr/bin/env python3

from __future__ import annotations

import gzip
import http.client
import json
import sqlite3
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

import app.allfunding_dashboard as dashboard
import app.funding_alerts as funding_alerts


class DashboardOptimizationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "funding_test.db"
        self.now_ms = 1_773_905_485_000
        self.orig_db_path = dashboard.DB_PATH
        self.orig_exchanges = dashboard.EXCHANGES
        self.orig_alert_db_path = getattr(funding_alerts, "DB_PATH", None)
        dashboard.DB_PATH = self.db_path
        dashboard.EXCHANGES = {
            "demo": {
                "label": "Demo",
                "info_table_candidates": ["demo_baseinfo"],
                "history_table": "demo_history",
                "open_interest_is_notional": True,
                "open_interest_notional_multiplier": 1.0,
                "allow_partial_window_sums": False,
            }
        }
        self._reset_dashboard_state()
        dashboard.initialize_dashboard_runtime(apply_legacy_migrations=True)
        self.addCleanup(self._cleanup)

    def _cleanup(self) -> None:
        dashboard.DB_PATH = self.orig_db_path
        dashboard.EXCHANGES = self.orig_exchanges
        if self.orig_alert_db_path is not None:
            funding_alerts.DB_PATH = self.orig_alert_db_path
        self._reset_dashboard_state()
        self.tmpdir.cleanup()

    def _reset_dashboard_state(self) -> None:
        dashboard._SCHEMA_PREPARED = False
        dashboard._INFO_TABLE_BY_EXCHANGE = {}
        dashboard._PAYLOAD_CACHE = None
        dashboard._PAYLOAD_CACHE_JSON = None
        dashboard._PAYLOAD_CACHE_ETAG = None
        dashboard._PAYLOAD_CACHE_TS = 0.0
        dashboard._PAYLOAD_CACHE_BASEINFO_BATCH_COMPLETED_AT = None
        dashboard._PAYLOAD_CACHE_HISTORY_BATCH_COMPLETED_AT = None
        dashboard._PAYLOAD_CACHE_WINDOWS_SIGNATURE = None
        dashboard._PAYLOAD_BUILD_IN_PROGRESS = False
        dashboard._HISTORY_SUMS_CACHE = None
        dashboard._HISTORY_SUMS_CACHE_BATCH_COMPLETED_AT = None
        dashboard._HISTORY_SUMS_CACHE_ANCHOR_MS = None
        dashboard._HISTORY_SUMS_CACHE_WINDOWS_SIGNATURE = None
        dashboard._LEGACY_MIGRATIONS_PREPARED = False

    def _seed_demo_data(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            dashboard.tune_sqlite_connection(conn)
            dashboard.prepare_schema(conn)
            conn.execute(
                """
                INSERT INTO demo_baseinfo (
                    symbol, adjustedFundingRateCap, adjustedFundingRateFloor,
                    fundingIntervalHours, markPrice, lastFundingRate, openInterest,
                    insuranceBalance, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "BTC_USDT",
                    "0.01",
                    "-0.01",
                    8,
                    "123.45",
                    "0.0025",
                    "2500000",
                    "150000",
                    self.now_ms,
                ),
            )
            for funding_time, funding_rate in (
                (self.now_ms - 5 * 60 * 60 * 1000, "0.0010"),
                (self.now_ms - 60 * 60 * 1000, "0.0020"),
            ):
                conn.execute(
                    """
                    INSERT INTO demo_history (symbol, fundingTime, fundingRate, updated_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    ("BTC_USDT", funding_time, funding_rate, self.now_ms),
                )
            dashboard._upsert_app_meta_value(
                conn,
                dashboard.BASEINFO_BATCH_COMPLETED_AT_KEY,
                str(self.now_ms),
                updated_at_ms=self.now_ms,
            )
            dashboard._upsert_app_meta_value(
                conn,
                "exchange_baseinfo_completed_at:demo",
                str(self.now_ms),
                updated_at_ms=self.now_ms,
            )
            dashboard._upsert_app_meta_value(
                conn,
                dashboard.HISTORY_BATCH_COMPLETED_AT_KEY,
                str(self.now_ms),
                updated_at_ms=self.now_ms,
            )
            conn.commit()

    def test_build_payload_uses_numeric_shadow_and_materialized_summaries(self) -> None:
        self._seed_demo_data()

        payload = dashboard.build_payload(force_refresh=True)
        item = payload["items"][0]
        self.assertAlmostEqual(item["markPrice"], 123.45)
        self.assertAlmostEqual(item["lastFundingRate"], 0.0025)
        self.assertEqual(payload["exchangeFreshness"]["demo"]["status"], "fresh")

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            base_row = conn.execute(
                "SELECT markPriceNum, lastFundingRateNum, openInterestNum FROM demo_baseinfo"
            ).fetchone()
            self.assertIsNotNone(base_row["markPriceNum"])
            self.assertIsNotNone(base_row["lastFundingRateNum"])
            self.assertIsNotNone(base_row["openInterestNum"])
            history_row = conn.execute(
                "SELECT fundingRateNum FROM demo_history ORDER BY fundingTime DESC LIMIT 1"
            ).fetchone()
            self.assertIsNotNone(history_row["fundingRateNum"])
            summary_count = conn.execute(
                f"SELECT COUNT(*) FROM {dashboard.HISTORY_WINDOW_SUMMARIES_TABLE}"
            ).fetchone()[0]
            self.assertGreater(summary_count, 0)

        dashboard._PAYLOAD_CACHE = None
        dashboard._PAYLOAD_CACHE_JSON = None
        dashboard._PAYLOAD_CACHE_ETAG = None
        dashboard._PAYLOAD_CACHE_TS = 0.0
        dashboard._HISTORY_SUMS_CACHE = None
        dashboard._HISTORY_SUMS_CACHE_BATCH_COMPLETED_AT = None
        dashboard._HISTORY_SUMS_CACHE_ANCHOR_MS = None
        dashboard._HISTORY_SUMS_CACHE_WINDOWS_SIGNATURE = None

        original_builder = dashboard._build_history_sums_by_exchange
        dashboard._build_history_sums_by_exchange = lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("materialized summaries should be reused")
        )
        try:
            reused = dashboard.build_payload(force_refresh=True)
        finally:
            dashboard._build_history_sums_by_exchange = original_builder

        self.assertEqual(reused["items"][0]["symbol"], "BTC_USDT")

    def test_build_payload_does_not_run_legacy_migrations(self) -> None:
        self._seed_demo_data()
        calls: list[str] = []
        original = dashboard.normalize_legacy_units

        def spy(conn: sqlite3.Connection) -> None:
            calls.append("called")
            original(conn)

        dashboard.normalize_legacy_units = spy
        try:
            self._reset_dashboard_state()
            with dashboard.connect_db() as conn:
                dashboard.prepare_schema(conn)
            dashboard.build_payload(force_refresh=True)
            self.assertEqual(calls, [])
            dashboard.initialize_dashboard_runtime(apply_legacy_migrations=True)
            self.assertEqual(calls, ["called"])
        finally:
            dashboard.normalize_legacy_units = original

    def test_http_api_supports_gzip_etag_and_rebuild(self) -> None:
        self._seed_demo_data()
        original_gzip_min_bytes = dashboard.GZIP_MIN_BYTES
        dashboard.GZIP_MIN_BYTES = 1
        server = dashboard.ThreadingHTTPServer(("127.0.0.1", 0), dashboard.DashboardHandler)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=20)
            conn.request("GET", "/api/data", headers={"Accept-Encoding": "gzip"})
            resp = conn.getresponse()
            compressed_body = resp.read()
            self.assertEqual(resp.status, 200)
            self.assertEqual(resp.getheader("Content-Encoding"), "gzip")
            etag = resp.getheader("ETag")
            self.assertTrue(etag)
            payload = json.loads(gzip.decompress(compressed_body))
            self.assertEqual(payload["items"][0]["symbol"], "BTC_USDT")
            conn.close()

            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=20)
            conn.request("GET", "/api/data", headers={"If-None-Match": etag})
            resp = conn.getresponse()
            self.assertEqual(resp.status, 304)
            resp.read()
            conn.close()

            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=20)
            conn.request("GET", "/api/data?refresh=1&rebuild=1")
            resp = conn.getresponse()
            self.assertEqual(resp.status, 200)
            self.assertGreater(len(resp.read()), 0)
            conn.close()
        finally:
            dashboard.GZIP_MIN_BYTES = original_gzip_min_bytes
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_build_exchange_freshness_statuses(self) -> None:
        original_exchanges = dashboard.EXCHANGES
        dashboard.EXCHANGES = {
            "demo": original_exchanges["demo"],
            "lag": original_exchanges["demo"],
            "old": original_exchanges["demo"],
        }
        try:
            freshness = dashboard.build_exchange_freshness(
                {
                    "demo": self.now_ms - 30_000,
                    "lag": self.now_ms - 5 * 60 * 1000,
                    "old": self.now_ms - 20 * 60 * 1000,
                },
                baseinfo_batch_completed_at=self.now_ms,
                now_ms=self.now_ms,
            )
        finally:
            dashboard.EXCHANGES = original_exchanges
        self.assertEqual(freshness["demo"]["status"], "fresh")
        self.assertEqual(freshness["lag"]["status"], "lagging")
        self.assertEqual(freshness["old"]["status"], "stale")

    def test_render_html_contains_virtualized_table_markers(self) -> None:
        html = dashboard.render_html()
        self.assertIn("pollMetaAndRefresh", html)
        self.assertIn("appendRowsInChunks", html)
        self.assertNotIn("choice-state", html)
        self.assertIn("funding_dashboard_blacklist_v1", html)
        self.assertIn("viewBlacklistBtn", html)
        self.assertIn("syncAlertBlacklistBtn", html)
        self.assertIn("modalClearBlacklistBtn", html)
        self.assertIn("showBlacklisted", html)
        self.assertIn("/api/alert-blacklist", html)
        self.assertNotIn("setAdminTokenBtn", html)
        self.assertNotIn("data-save-note-key", html)
        self.assertNotIn("当前只做本机隐藏；未同步飞书告警", html)

    def test_alerts_h4_threshold_reads_payload_sums(self) -> None:
        payload = {
            "generatedAt": self.now_ms,
            "items": [
                {
                    "exchange": "demo",
                    "exchangeLabel": "Demo",
                    "symbol": "BTC_USDT",
                    "fundingIntervalHours": 8,
                    "openInterestNotional": 5_000_000,
                    "lastFundingRate": 0.001,
                    "sums": {"h4": 0.0035},
                }
            ],
        }
        config: dict[str, Any] = {
            "h4_pct_gte": 0.3,
            "open_interest_min_musd": 1,
        }
        hits = funding_alerts.collect_hits(payload, config)
        self.assertEqual(len(hits), 1)
        self.assertAlmostEqual(hits[0].h4_value or 0.0, 0.0035)

    def test_alert_blacklist_api_blocks_alert_hits(self) -> None:
        self._seed_demo_data()
        server = dashboard.ThreadingHTTPServer(("127.0.0.1", 0), dashboard.DashboardHandler)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with mock.patch.object(dashboard, "load_dashboard_admin_token", return_value="secret-token"):
                conn = http.client.HTTPConnection("127.0.0.1", port, timeout=20)
                conn.request(
                    "POST",
                    "/api/alert-blacklist",
                    body=json.dumps({"rowKey": "demo::BTC_USDT", "blocked": True}),
                    headers={
                        "Content-Type": "application/json",
                        "X-Dashboard-Admin-Token": "secret-token",
                    },
                )
                resp = conn.getresponse()
                payload = json.loads(resp.read())
                self.assertEqual(resp.status, 200)
                self.assertTrue(payload["ok"])
                conn.close()

            blocked = dashboard.load_alert_blacklist_row_keys()
            self.assertEqual(blocked, {"demo::BTC_USDT"})
            hits = funding_alerts.collect_hits(
                {
                    "generatedAt": self.now_ms,
                    "items": [
                        {
                            "exchange": "demo",
                            "exchangeLabel": "Demo",
                            "symbol": "BTC_USDT",
                            "fundingIntervalHours": 8,
                            "openInterestNotional": 5_000_000,
                            "lastFundingRate": 0.002,
                            "sums": {"h4": 0.0035},
                        }
                    ],
                },
                {"latest_pct_gte": 0.1},
                blocked_row_keys=blocked,
            )
            self.assertEqual(hits, [])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_alerts_do_not_apply_cooldown_between_runs(self) -> None:
        payload = {
            "generatedAt": self.now_ms,
            "items": [
                {
                    "exchange": "demo",
                    "exchangeLabel": "Demo",
                    "symbol": "BTC_USDT",
                    "fundingIntervalHours": 8,
                    "openInterestNotional": 5_000_000,
                    "lastFundingRate": 0.002,
                    "sums": {"h4": 0.0035},
                }
            ],
        }
        config_path = Path(self.tmpdir.name) / "alerts.json"
        config_path.write_text(
            json.dumps(
                {
                    "enabled": True,
                    "latest_pct_gte": 0.1,
                    "max_items_per_run": 20,
                    "providers": {},
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        messages: list[str] = []
        with (
            mock.patch.object(funding_alerts, "initialize_dashboard_runtime"),
            mock.patch.object(funding_alerts, "build_payload", return_value=payload),
            mock.patch.object(
                funding_alerts,
                "notify",
                side_effect=lambda message, config, dry_run: messages.append(message) or True,
            ),
            mock.patch.object(sys, "argv", ["funding_alerts.py", "--config", str(config_path), "--dry-run"]),
        ):
            funding_alerts.main()
            funding_alerts.main()
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0], messages[1])


if __name__ == "__main__":
    unittest.main()
