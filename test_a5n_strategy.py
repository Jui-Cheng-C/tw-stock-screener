import unittest
from email.header import Header
from pathlib import Path

import pandas as pd

from a5n_strategy import A5_N_CONFIG, _completed_60k, _completed_daily


class A5NLookaheadTests(unittest.TestCase):
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
        self.assertIn("本次A5-N無正式合格標的", source)
        self.assertIn("新策略測試訊號，僅供人工核對，非自動下單。", source)

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
