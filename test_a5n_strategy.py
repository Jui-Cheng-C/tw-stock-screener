import unittest
from email.header import Header
from pathlib import Path

import pandas as pd

from a5n_strategy import A5_N_CONFIG, _completed_60k, _completed_daily
from a5n_variant_b import A5_N_B_CONFIG


class A5NLookaheadTests(unittest.TestCase):
    def test_variant_b_is_independent_and_only_changes_platform_window(self):
        self.assertEqual(A5_N_CONFIG["platform_lookback_days"], 12)
        self.assertEqual(A5_N_CONFIG["platform_exclude_recent_days"], 2)
        self.assertEqual(A5_N_B_CONFIG["platform_lookback_days"], 7)
        self.assertEqual(A5_N_B_CONFIG["platform_exclude_recent_days"], 0)
        self.assertEqual(A5_N_B_CONFIG["platform_min_days"], 6)
        for key, value in A5_N_CONFIG.items():
            if key not in {"parameter_status", "platform_lookback_days", "platform_exclude_recent_days", "platform_min_days"}:
                self.assertEqual(A5_N_B_CONFIG[key], value)
        self.assertFalse(A5_N_B_CONFIG["notification_enabled"])

    def test_variant_b_zero_exclusion_keeps_seven_completed_days(self):
        source = Path("a5n_strategy.py").read_text(encoding="utf-8")
        self.assertIn("window_end = None if exclude == 0 else -exclude", source)

    def test_premarket_empty_data_is_audited(self):
        source = Path("tw_stock_screener.py").read_text(encoding="utf-8")
        self.assertIn('"reject_reason": ["A_DATA_EMPTY"]', source)
        self.assertIn('"missing_count": len(mother) - len(audit_rows)', source)
    def test_ntfy_unicode_title_is_ascii_header_safe(self):
        encoded = Header("🧪 A5-N 新策略測試", "utf-8").encode()
        encoded.encode("latin-1")
        self.assertIn("=?utf-8?", encoded.lower())
    def test_daily_excludes_same_day_partial_bar(self):
        frame = pd.DataFrame({
            "date": pd.to_datetime(["2026-08-12", "2026-08-13"]),
            "close": [74.0, 78.2],
        })
        got = _completed_daily(frame, pd.Timestamp("2026-08-13 10:42"))
        self.assertEqual(got["date"].max().date(), pd.Timestamp("2026-08-12").date())
        self.assertNotIn(78.2, got["close"].tolist())

    def test_hourly_excludes_forming_1000_bar_at_1042(self):
        frame = pd.DataFrame({
            "date": pd.to_datetime(["2026-08-13 09:00", "2026-08-13 10:00"]),
            "close": [75.0, 78.2],
        })
        got = _completed_60k(frame, pd.Timestamp("2026-08-13 10:42"))
        self.assertEqual(got["date"].max(), pd.Timestamp("2026-08-13 09:00"))

    def test_legacy_is_disabled(self):
        source = Path("tw_stock_screener.py").read_text(encoding="utf-8")
        self.assertIn("A5_LEGACY_ENABLED = False", source)

    def test_empty_scan_still_builds_ntfy(self):
        source = Path("tw_stock_screener.py").read_text(encoding="utf-8")
        self.assertIn("本次沒有符合進場條件的股票", source)
        self.assertIn("僅供人工核對，非自動下單", source)

    def test_a5n_ntfy_uses_traditional_chinese_labels(self):
        from tw_stock_screener import a5n_gate_summary, a5n_reason_summary
        row = {
            "A": {"A1": {"passed": True}, "A2": {"passed": False}},
            "B": {"B1": {"passed": True}},
            "C": {},
            "reject_reason": ["C1_NO_BREAKOUT", "C5"],
        }
        self.assertEqual(a5n_gate_summary(row), "日K 1/5｜60分K 1/5｜5分K 0/5")
        self.assertEqual(a5n_reason_summary(row), "尚未突破、時效、風險或報酬比複驗未過")

    def test_notification_is_capped_at_four_and_excess_is_retained(self):
        from tw_stock_screener import format_a5n_ntfy_message
        rows = []
        for i in range(6):
            rows.append({
                "stock_id": f"00{i}", "stock_name": f"測試{i}",
                "strategy_state": "ENTRY_VALIDATED",
                "A": {f"A{j}": {"passed": True, "raw": {}} for j in range(1, 6)},
                "B": {f"B{j}": {"passed": True, "raw": {}} for j in range(1, 6)},
                "C": {f"C{j}": {"passed": True, "raw": {}} for j in range(1, 6)},
            })
        message = format_a5n_ntfy_message(rows)
        selected = [x for x in rows if x.get("notification_selected")]
        suppressed = [x for x in rows if x.get("notification_suppressed_reason")]
        self.assertEqual(len(selected), A5_N_CONFIG["max_ntfy_entries_per_scan"])
        self.assertEqual(len(suppressed), 2)
        self.assertIn("超過每次4檔上限", message)
        self.assertTrue(all(x["strategy_state"] == "ENTRY_VALIDATED" for x in suppressed))


if __name__ == "__main__":
    unittest.main()
