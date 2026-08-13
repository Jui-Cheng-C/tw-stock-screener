"""A5-N weekly expanded/fixed-pool rules (research default).

Pure calculations only: callers provide completed daily bars and official
eligibility.  This keeps the weekly pool independent from the strict A pool.
"""
from __future__ import annotations

from typing import Any, Callable

import pandas as pd


A5_N_FIXED_POOL_VERSION = "A5-N-fixed-pool-research-v0.1-20260813"
A5_N_FIXED_POOL_CONFIG: dict[str, Any] = {
    "parameter_status": "research_default",
    "price_bands": [
        {"minimum": 10.0, "maximum": 24.9, "avg_volume_20d_min_shares": 3_000_000},
        {"minimum": 100.0, "maximum": 199.9, "avg_volume_20d_min_shares": 800_000},
    ],
    "liquidity_days": 20,
    "week_bars": 5,
    "green_histogram_contraction_bars": 3,
    "recent_histogram_low_lookback": 5,
    "target_min_count": 80,
    "target_max_count": 150,
    "hard_cap_trigger_count": 180,
    "hard_cap_count": 150,
    "midpoint_distance_tiebreaker": True,
    "weekly_refresh": "Friday after close; valid following Monday-Friday",
}


def evaluate_fixed_pool_candidate(
    daily: pd.DataFrame,
    *,
    anchor_date: pd.Timestamp,
    add_indicators: Callable[..., pd.DataFrame],
    official_daytrade_ok: bool,
    official_status: dict[str, Any],
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = dict(A5_N_FIXED_POOL_CONFIG if config is None else config)
    d = daily.copy()
    if d.empty or "date" not in d.columns:
        return {
            "strategy_version": A5_N_FIXED_POOL_VERSION,
            "parameter_status": cfg["parameter_status"],
            "anchor_date": str(pd.Timestamp(anchor_date).date()),
            "gates": {}, "passed": False,
            "reject_reason": ["FIXED_DATA_EMPTY"],
        }
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    d = d[d["date"].dt.date <= pd.Timestamp(anchor_date).date()].sort_values("date")
    result: dict[str, Any] = {
        "strategy_version": A5_N_FIXED_POOL_VERSION,
        "parameter_status": cfg["parameter_status"],
        "anchor_date": str(pd.Timestamp(anchor_date).date()),
        "gates": {}, "passed": False, "reject_reason": [],
    }
    if len(d) < 65:
        result["reject_reason"] = ["FIXED_DATA_INSUFFICIENT"]
        return result
    di = add_indicators(d, timeframe="daily")
    latest = di.iloc[-1]
    close = float(latest["close"])
    band = next((b for b in cfg["price_bands"] if b["minimum"] <= close <= b["maximum"]), None)
    result["gates"]["F1_PRICE_BAND"] = {"passed": band is not None, "raw": {"close": close, "matched_band": band}}
    if band is None:
        result["reject_reason"].append("F1_PRICE_BAND")

    volumes = d.tail(int(cfg["liquidity_days"]))["Trading_Volume"].astype(float)
    avg_volume = float(volumes.mean()) if len(volumes) == int(cfg["liquidity_days"]) else 0.0
    threshold = int(band["avg_volume_20d_min_shares"]) if band else None
    liquid = threshold is not None and avg_volume >= threshold
    result["gates"]["F2_LIQUIDITY"] = {"passed": liquid, "raw": {
        "average_volume_20d_shares": round(avg_volume), "minimum_shares": threshold,
        "completed_bars": len(volumes)}}
    if not liquid:
        result["reject_reason"].append("F2_LIQUIDITY")

    result["gates"]["F3_OFFICIAL_STATUS"] = {"passed": bool(official_daytrade_ok), "raw": official_status}
    if not official_daytrade_ok:
        result["reject_reason"].append("F3_OFFICIAL_STATUS")

    week_n = int(cfg["week_bars"])
    week = di.tail(week_n)
    dif = week["dif"].astype(float)
    signal = week["macd"].astype(float)
    above_now = bool(dif.iloc[-1] > 0 and signal.iloc[-1] > 0)
    crossed_this_week = any(
        (dif.iloc[i - 1] <= 0 or signal.iloc[i - 1] <= 0)
        and dif.iloc[i] > 0 and signal.iloc[i] > 0
        for i in range(1, len(week))
    )
    momentum_a = above_now or crossed_this_week
    result["gates"]["F4_MOMENTUM_A"] = {"passed": momentum_a, "raw": {
        "definition": "DIF_and_signal_both_above_zero_or_crossed_this_week",
        "dif": float(dif.iloc[-1]), "signal": float(signal.iloc[-1]),
        "above_zero_now": above_now, "crossed_this_week": crossed_this_week}}

    hist = di["hist"].astype(float)
    n = int(cfg["green_histogram_contraction_bars"])
    recent_n = int(cfg["recent_histogram_low_lookback"])
    seq = hist.tail(n)
    recent = hist.tail(recent_n)
    contraction = bool(len(seq) == n and seq.iloc[-1] <= 0
                       and all(seq.iloc[i] < seq.iloc[i + 1] for i in range(n - 1))
                       and seq.iloc[0] == recent.min())
    result["gates"]["F5_MOMENTUM_B"] = {"passed": contraction, "raw": {
        "definition": "negative_histogram_strictly_contracts_from_recent_5bar_low",
        "histogram_sequence": [float(x) for x in seq]}}
    momentum_ok = momentum_a or contraction
    if not momentum_ok:
        result["reject_reason"].append("F4_OR_F5_MOMENTUM")
    midpoint = ((band["minimum"] + band["maximum"]) / 2) if band else 0.0
    result["ranking"] = {"momentum_a_priority": int(momentum_a),
        "average_volume_20d_shares": round(avg_volume),
        "distance_to_band_midpoint": abs(close - midpoint) if band else 999.0}
    result["passed"] = bool(band and liquid and official_daytrade_ok and momentum_ok)
    return result


def fixed_pool_rank_key(item: dict[str, Any]) -> tuple[float, ...]:
    r = item.get("fixed_pool", item).get("ranking", {})
    return (float(r.get("momentum_a_priority", 0)),
            float(r.get("average_volume_20d_shares", 0)),
            -float(r.get("distance_to_band_midpoint", 999)))
