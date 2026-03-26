from __future__ import annotations

import unittest

from exchanges.backpack_funding.backpack_http import (
    BackpackCircuitOpen,
    BackpackFailureTracker,
    BackpackRequestError,
    summarize_failed_items,
)


class BackpackResilienceTests(unittest.TestCase):
    def test_failure_tracker_opens_circuit_after_consecutive_retryable_failures(self) -> None:
        tracker = BackpackFailureTracker("Backpack history", max_consecutive_transient_failures=3)
        exc = BackpackRequestError("timeout", retryable=True, timed_out=True)
        tracker.record_failure(item_label="BTC_USDC_PERP", exc=exc)
        tracker.record_failure(item_label="ETH_USDC_PERP", exc=exc)
        with self.assertRaises(BackpackCircuitOpen):
            tracker.record_failure(item_label="SOL_USDC_PERP", exc=exc)

    def test_failure_tracker_resets_after_success(self) -> None:
        tracker = BackpackFailureTracker("Backpack base", max_consecutive_transient_failures=2)
        exc = BackpackRequestError("timeout", retryable=True, timed_out=True)
        tracker.record_failure(item_label="BTC_USDC_PERP:ticker", exc=exc)
        tracker.record_success()
        tracker.record_failure(item_label="ETH_USDC_PERP:ticker", exc=exc)
        tracker.record_success()

    def test_non_retryable_failures_do_not_open_circuit(self) -> None:
        tracker = BackpackFailureTracker("Backpack base", max_consecutive_transient_failures=2)
        exc = BackpackRequestError("http 404", retryable=False)
        tracker.record_failure(item_label="BTC_USDC_PERP:ticker", exc=exc)
        tracker.record_failure(item_label="ETH_USDC_PERP:ticker", exc=exc)

    def test_summarize_failed_items_caps_preview(self) -> None:
        text = summarize_failed_items(["a", "b", "c", "d"], max_items=2)
        self.assertEqual(text, "a, b ... +2")


if __name__ == "__main__":
    unittest.main()
