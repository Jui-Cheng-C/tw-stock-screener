import unittest
from pathlib import Path

import pandas as pd

from a5n_fixed_pool import (
    A5_N_FIXED_POOL_CONFIG,
    evaluate_fixed_pool_candidate,
    fixed_pool_rank_key,
)


def bars(close=20.0, volume=3_100_000):
    dates = pd.bdate_range(end="2026-08-07", periods=70)
    return pd.DataFrame({
        "date": dates, "open": close, "max": close * 1.01,
        "min": close * .99, "close": close,
        "Trading_Volume": volume,
    })


def indicator_with(dif, signal, hist):
    def add(frame, timeframe="daily"):
        out = frame.copy()
        n = len(out)
        out["dif"] = ([dif[0]] * max(0, n-len(dif)) + list(dif))[-n:]
        out["macd"] = ([signal[0]] * max(0, n-len(signal)) + list(signal))[-n:]
        out["hist"] = ([hist[0]] * max(0, n-len(hist)) + list(hist))[-n:]
        return out
    return add


class FixedPoolRuleTests(unittest.TestCase):
    def test_low_band_volume_and_momentum_a_pass(self):
        add = indicator_with([.1]*5, [.05]*5, [.05]*5)
        got = evaluate_fixed_pool_candidate(
            bars(), anchor_date=pd.Timestamp("2026-08-07"), add_indicators=add,
            official_daytrade_ok=True, official_status={"source": "test"})
        self.assertTrue(got["passed"])
        self.assertTrue(got["gates"]["F4_MOMENTUM_A"]["passed"])

    def test_high_band_uses_800_lot_threshold(self):
        add = indicator_with([.1]*5, [.05]*5, [.05]*5)
        got = evaluate_fixed_pool_candidate(
            bars(close=150, volume=800_000), anchor_date=pd.Timestamp("2026-08-07"),
            add_indicators=add, official_daytrade_ok=True, official_status={})
        self.assertTrue(got["passed"])
        self.assertEqual(got["gates"]["F2_LIQUIDITY"]["raw"]["minimum_shares"], 800_000)

    def test_green_histogram_must_contract_from_recent_low(self):
        add = indicator_with([-.2]*5, [-.1]*5, [-.1, -.2, -.5, -.3, -.1])
        got = evaluate_fixed_pool_candidate(
            bars(), anchor_date=pd.Timestamp("2026-08-07"), add_indicators=add,
            official_daytrade_ok=True, official_status={})
        self.assertFalse(got["gates"]["F4_MOMENTUM_A"]["passed"])
        self.assertTrue(got["gates"]["F5_MOMENTUM_B"]["passed"])
        self.assertTrue(got["passed"])

    def test_official_status_is_fail_closed(self):
        add = indicator_with([.1]*5, [.05]*5, [.05]*5)
        got = evaluate_fixed_pool_candidate(
            bars(), anchor_date=pd.Timestamp("2026-08-07"), add_indicators=add,
            official_daytrade_ok=False, official_status={"reason": "missing"})
        self.assertFalse(got["passed"])
        self.assertIn("F3_OFFICIAL_STATUS", got["reject_reason"])

    def test_ranking_priority_then_volume_then_midpoint(self):
        a = {"fixed_pool": {"ranking": {"momentum_a_priority": 1, "average_volume_20d_shares": 1, "distance_to_band_midpoint": 9}}}
        b = {"fixed_pool": {"ranking": {"momentum_a_priority": 0, "average_volume_20d_shares": 9_000_000, "distance_to_band_midpoint": 0}}}
        self.assertGreater(fixed_pool_rank_key(a), fixed_pool_rank_key(b))

    def test_config_keeps_requested_cap_policy(self):
        self.assertEqual(A5_N_FIXED_POOL_CONFIG["hard_cap_trigger_count"], 180)
        self.assertEqual(A5_N_FIXED_POOL_CONFIG["hard_cap_count"], 150)

    def test_momentum_rank_shadow_is_isolated_from_formal_pool(self):
        source = Path("tw_stock_screener.py").read_text(encoding="utf-8")
        self.assertIn('"research_shadow_momentum_rank_only"', source)
        self.assertIn('"momentum_required"] = False', source)
        self.assertIn('"shadow_only": True, "ntfy_eligible": False', source)
        self.assertIn("run_a5n_fixed_momentum_rank_shadow_scan(cfg)", source)

    def test_shadow_does_not_change_formal_pass_definition(self):
        source = Path("a5n_fixed_pool.py").read_text(encoding="utf-8")
        self.assertIn("official_daytrade_ok and momentum_ok", source)


if __name__ == "__main__":
    unittest.main()
