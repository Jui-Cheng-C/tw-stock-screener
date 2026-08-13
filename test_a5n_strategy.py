import unittest
from pathlib import Path

import pandas as pd

from a5n_strategy import _completed_60k, _completed_daily


class A5NLookaheadTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
