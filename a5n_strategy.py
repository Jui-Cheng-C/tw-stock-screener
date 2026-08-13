"""A5-N research strategy: platform accumulation -> breakout -> first pullback.

This module is deliberately independent from A1-A4.  It only consumes completed
bars supplied by the caller and never downloads data itself.
"""
from __future__ import annotations

import datetime as dt
from typing import Any, Callable

import pandas as pd


A5_N_VERSION = "A5-N-research-default-v0.1-20260813"
A5_N_STATES = (
    "DAILY_CANDIDATE", "HOURLY_CONFIRMED", "BREAKOUT_DETECTED",
    "WAITING_PULLBACK", "ENTRY_VALIDATED", "EXPIRED", "REJECTED",
)

# These are research defaults, not optimized or validated best parameters.
A5_N_CONFIG: dict[str, Any] = {
    "parameter_status": "research_default",
    "platform_lookback_days": 12,
    "platform_exclude_recent_days": 2,
    "daily_near_high_pct": 4.0,
    "daily_ma_cluster_pct": 5.0,
    "daily_max_ma20_distance_pct": 8.0,
    "hourly_structure_bars": 5,
    "hourly_near_breakout_pct": 4.0,
    "hourly_max_ema5_distance_pct": 3.0,
    "breakout_buffer_pct": 0.0,
    "pullback_touch_tolerance_pct": 0.8,
    "pullback_break_tolerance_pct": 0.5,
    "relative_volume_min": 1.2,
    "max_signal_age_seconds": 900,
    "max_breakout_extension_pct": 2.0,
    "max_stop_risk_pct": 1.5,
    "min_reward_risk": 2.0,
}


def _num(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if not pd.isna(value) else default
    except (TypeError, ValueError):
        return default


def _gate(passed: bool, **raw: Any) -> dict[str, Any]:
    return {"passed": bool(passed), "raw": raw}


def _completed_daily(daily: pd.DataFrame, as_of: pd.Timestamp) -> pd.DataFrame:
    out = daily.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    return out[out["date"].dt.date < as_of.date()].sort_values("date")


def _completed_60k(hourly: pd.DataFrame, as_of: pd.Timestamp) -> pd.DataFrame:
    out = hourly.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    cutoff = as_of.floor("h") - pd.Timedelta(hours=1)
    return out[out["date"] <= cutoff].sort_values("date")


def evaluate_a5n(
    *, row: pd.Series, daily: pd.DataFrame, hourly: pd.DataFrame,
    five_min: pd.DataFrame, as_of: dt.datetime | pd.Timestamp,
    add_indicators: Callable[..., pd.DataFrame], keep_completed_5m: Callable[..., pd.DataFrame],
    daytrade_ok: bool, daytrade_reasons: list[str], max_price: float,
    min_volume_shares: int, min_turnover: float,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = dict(A5_N_CONFIG if config is None else config)
    now = pd.Timestamp(as_of)
    if now.tzinfo is not None:
        now = now.tz_localize(None)
    result: dict[str, Any] = {
        "strategy_version": A5_N_VERSION, "config": cfg, "strategy_state": "REJECTED",
        "reject_reason": [], "A": {}, "B": {}, "C": {},
        "symbol_evaluated_at": str(now), "breakout_level": None,
        "breakout_timestamp": None, "pullback_low": None, "signal_timestamp": None,
        "signal_price": None, "recheck_timestamp": None, "recheck_price": None,
        "signal_age_seconds": None,
    }
    d = _completed_daily(daily, now)
    h = _completed_60k(hourly, now)
    f = five_min.copy()
    f["date"] = pd.to_datetime(f["date"], errors="coerce")
    result["raw_last_timestamp"] = str(f["date"].max()) if not f.empty else None
    f = keep_completed_5m(f, as_of=now)
    result["daily_data_date"] = str(d["date"].max()) if not d.empty else None
    result["last_completed_60k_timestamp"] = str(h["date"].max()) if not h.empty else None
    result["last_completed_5k_timestamp"] = str(f["date"].max()) if not f.empty else None
    result["expected_last_completed_timestamp"] = str(now.floor("5min") - pd.Timedelta(minutes=5))
    if len(d) < 65:
        result["reject_reason"].append("A_DATA_INSUFFICIENT")
        return result

    di = add_indicators(d, timeframe="daily")
    latest = di.iloc[-1]
    lookback = int(cfg["platform_lookback_days"])
    exclude = int(cfg["platform_exclude_recent_days"])
    window = di.iloc[-(lookback + exclude):-exclude]
    platform_high, platform_low = _num(window["max"].max()), _num(window["min"].min())
    close = _num(latest["close"])
    near_high = (platform_high - close) / platform_high * 100 if platform_high else 999
    lows = window["min"].astype(float)
    low_not_falling = _num(lows.tail(max(2, len(lows)//2)).min()) >= _num(lows.head(max(2, len(lows)//2)).min()) * .98
    a1 = low_not_falling and -1.0 <= near_high <= cfg["daily_near_high_pct"]
    result["A"]["A1"] = _gate(a1, platform_low=platform_low, platform_high=platform_high, distance_to_high_pct=round(near_high, 2), low_not_falling=low_not_falling)
    ma5, ma10, ma20 = (_num(latest[x]) for x in ("ma5", "ma10", "ma20"))
    ma20_prev = _num(di.iloc[-4]["ma20"])
    cluster = (max(ma5, ma10, ma20)-min(ma5, ma10, ma20))/close*100 if close else 999
    a2 = close > ma20 and ma20 >= ma20_prev and not (_num(di.iloc[-1]["ma5"]) < _num(di.iloc[-3]["ma5"]) and _num(di.iloc[-1]["ma10"]) < _num(di.iloc[-3]["ma10"])) and cluster <= cfg["daily_ma_cluster_pct"]
    result["A"]["A2"] = _gate(a2, close=close, ma5=ma5, ma10=ma10, ma20=ma20, ma20_prior=ma20_prev, cluster_pct=round(cluster,2))
    ma20_dist = (close/ma20-1)*100 if ma20 else 999
    a3 = ma20_dist <= cfg["daily_max_ma20_distance_pct"]
    result["A"]["A3"] = _gate(a3, ma20_distance_pct=round(ma20_dist,2), pressure=platform_high)
    hist = di["hist"].astype(float)
    dif = di["dif"].astype(float)
    macd_reason = "none"
    if hist.iloc[-2] <= 0 < hist.iloc[-1]: macd_reason = "hist_turn_positive"
    elif dif.iloc[-2] <= _num(di.iloc[-2]["macd"]) and dif.iloc[-1] > _num(di.iloc[-1]["macd"]): macd_reason = "dif_golden_cross"
    elif hist.iloc[-1] > hist.iloc[-2] > 0: macd_reason = "positive_hist_expanding"
    elif hist.iloc[-3] < hist.iloc[-2] < hist.iloc[-1] <= 0: macd_reason = "negative_hist_contracting"
    result["A"]["A4"] = _gate(macd_reason != "none", reason=macd_reason, dif=_num(dif.iloc[-1]), histogram=_num(hist.iloc[-1]))
    price = _num(row.get("last_close"), close)
    volume = _num(row.get("Trading_Volume"))
    turnover = price * volume
    a5 = daytrade_ok and price <= max_price and volume >= min_volume_shares and turnover >= min_turnover
    result["A"]["A5"] = _gate(a5, price=price, max_price=max_price, volume=volume, min_volume=min_volume_shares, turnover=turnover, min_turnover=min_turnover, eligibility_source="existing_daytrade_gate; official daytrade/disposition/spread fields unavailable", reasons=daytrade_reasons)
    if not (a1 and a2 and a5):
        result["reject_reason"] += [k for k in ("A1","A2","A5") if not result["A"][k]["passed"]]
        return result
    result["strategy_state"] = "DAILY_CANDIDATE"
    if len(h) < 25:
        result["reject_reason"].append("B_DATA_INSUFFICIENT")
        return result

    hi = add_indicators(h, timeframe="60k")
    hl = hi.iloc[-1]
    tail = hi.tail(int(cfg["hourly_structure_bars"]))
    hclose, hma20, hma60 = _num(hl["close"]), _num(hl["ma20"]), _num(hl["ma60"])
    low_series = tail["min"].astype(float)
    b1 = _num(low_series.iloc[-1]) >= _num(low_series.iloc[0]) * .98 and hclose >= _num(tail["max"].max()) * (1-cfg["hourly_near_breakout_pct"]/100)
    result["B"]["B1"] = _gate(b1, first_low=_num(low_series.iloc[0]), last_low=_num(low_series.iloc[-1]), range_high=_num(tail["max"].max()))
    b2 = hclose >= hma20 and _num(hi.iloc[-1]["ma20"]) >= _num(hi.iloc[-4]["ma20"])*.995 and _num(hi.iloc[-1]["ma60"]) >= _num(hi.iloc[-4]["ma60"])*.995
    result["B"]["B2"] = _gate(b2, close=hclose, ma20=hma20, ma60=hma60)
    ema5 = hi["close"].astype(float).ewm(span=5, adjust=False).mean()
    ema10 = hi["close"].astype(float).ewm(span=10, adjust=False).mean()
    b3 = ema5.iloc[-1] > ema5.iloc[-3] and ema10.iloc[-1] >= ema10.iloc[-3]*.995 and abs(hclose/ema5.iloc[-1]-1)*100 <= cfg["hourly_max_ema5_distance_pct"]
    result["B"]["B3"] = _gate(b3, ema5=_num(ema5.iloc[-1]), ema5_prior=_num(ema5.iloc[-3]), ema10=_num(ema10.iloc[-1]))
    hh = hi["hist"].astype(float); hd = hi["dif"].astype(float)
    b4 = (hh.iloc[-1] > 0 and hh.iloc[-1] >= hh.iloc[-2]) or (hh.iloc[-3] < hh.iloc[-2] < hh.iloc[-1] <= 0 and hd.iloc[-1] > hd.iloc[-2])
    result["B"]["B4"] = _gate(b4, dif=_num(hd.iloc[-1]), histogram=_num(hh.iloc[-1]), histogram_prior=_num(hh.iloc[-2]))
    candidates = {"daily_platform_high": platform_high, "hourly_range_high": _num(tail.iloc[:-1]["max"].max()), "previous_day_high": _num(d.iloc[-1]["max"])}
    breakout_level = max(candidates.values())
    result["breakout_level"] = breakout_level
    result["B"]["B5"] = _gate(hclose >= breakout_level*(1-cfg["hourly_near_breakout_pct"]/100), candidates=candidates, selected=breakout_level, distance_pct=round((breakout_level/hclose-1)*100,2))
    if not (b1 and b2 and (b3 or b4)):
        result["reject_reason"] += [k for k in ("B1","B2") if not result["B"][k]["passed"]]
        if not (b3 or b4): result["reject_reason"].append("B3_OR_B4")
        return result
    result["strategy_state"] = "HOURLY_CONFIRMED"

    if f.empty:
        result["reject_reason"].append("C_DATA_INSUFFICIENT")
        return result
    fi = add_indicators(f.sort_values("date"), timeframe="5k")
    today = fi[fi["date"].dt.date == now.date()].copy()
    if len(today) < 3:
        result["reject_reason"].append("C_NOT_ENOUGH_COMPLETED_5K")
        return result
    level = breakout_level*(1+cfg["breakout_buffer_pct"]/100)
    hits = today.index[(today["close"].astype(float) > level) & (today["max"].astype(float) > level)]
    c1 = len(hits) > 0
    result["C"]["C1"] = _gate(c1, breakout_level=breakout_level, completed_bars=len(today))
    if not c1:
        result["reject_reason"].append("C1_NO_BREAKOUT")
        return result
    breakout_idx = hits[0]; breakout_pos = today.index.get_loc(breakout_idx)
    breakout_bar = today.loc[breakout_idx]
    result["strategy_state"] = "BREAKOUT_DETECTED"
    result["breakout_timestamp"] = str(breakout_bar["date"])
    later = today.iloc[breakout_pos+1:]
    if later.empty:
        result["strategy_state"] = "WAITING_PULLBACK"; result["reject_reason"].append("C2_WAITING_FIRST_PULLBACK")
        return result
    touch = later[later["min"].astype(float) <= breakout_level*(1+cfg["pullback_touch_tolerance_pct"]/100)]
    if touch.empty:
        result["strategy_state"] = "WAITING_PULLBACK"; result["reject_reason"].append("C2_WAITING_FIRST_PULLBACK")
        return result
    pb = touch.iloc[0]; pullback_low = _num(pb["min"])
    c2 = pullback_low >= breakout_level*(1-cfg["pullback_break_tolerance_pct"]/100) and _num(pb["close"]) >= breakout_level and _num(pb["close"]) >= _num(pb["open"])*.99
    result["pullback_low"] = pullback_low
    result["C"]["C2"] = _gate(c2, pullback_low=pullback_low, pullback_close=_num(pb["close"]), breakout_level=breakout_level)
    fem5=fi["close"].astype(float).ewm(span=5,adjust=False).mean(); fem10=fi["close"].astype(float).ewm(span=10,adjust=False).mean(); fem20=fi["close"].astype(float).ewm(span=20,adjust=False).mean()
    pbi=pb.name; c3=_num(pb["close"])>=_num(fem5.loc[pbi]) and _num(fem5.loc[pbi])>_num(fem5.iloc[max(0,fi.index.get_loc(pbi)-2)]) and (_num(fem5.loc[pbi])>=_num(fem10.loc[pbi]) or _num(fem5.loc[pbi])>_num(fem20.loc[pbi]))
    result["C"]["C3"]=_gate(c3, close=_num(pb["close"]),ema5=_num(fem5.loc[pbi]),ema10=_num(fem10.loc[pbi]),ema20=_num(fem20.loc[pbi]))
    bhpos=fi.index.get_loc(breakout_idx); prevvol=fi.iloc[max(0,bhpos-10):bhpos]["Trading_Volume"].astype(float); relvol=_num(breakout_bar["Trading_Volume"])/(float(prevvol.median()) or 1)
    h5=fi["hist"].astype(float); d5=fi["dif"].astype(float); c4=((h5.loc[pbi]>0 or h5.loc[pbi]>h5.iloc[max(0,fi.index.get_loc(pbi)-2)]) and d5.loc[pbi]>d5.iloc[max(0,fi.index.get_loc(pbi)-2)] and relvol>=cfg["relative_volume_min"])
    reason="positive_hist" if h5.loc[pbi]>0 else "hist_improving"
    result["C"]["C4"]=_gate(c4,macd_reason=reason,dif=_num(d5.loc[pbi]),histogram=_num(h5.loc[pbi]),relative_volume=round(relvol,2))
    signal_price=_num(pb["close"]); stop=min(pullback_low,breakout_level); risk=(signal_price-stop)/signal_price*100 if signal_price else 999; pressure=max(platform_high,_num(today["max"].max())); reward=max(0,pressure-signal_price); rr=reward/(signal_price-stop) if signal_price>stop else 0
    signal_ts=pd.Timestamp(pb["date"]); age=max(0,(now-signal_ts).total_seconds()); latest=_num(today.iloc[-1]["close"]); latest_ema5=_num(fem5.loc[today.index[-1]]); extension=(latest/breakout_level-1)*100
    c5=age<=cfg["max_signal_age_seconds"] and latest>=breakout_level*(1-cfg["pullback_break_tolerance_pct"]/100) and latest>=latest_ema5*.995 and extension<=cfg["max_breakout_extension_pct"] and risk<=cfg["max_stop_risk_pct"] and rr>=cfg["min_reward_risk"]
    result.update(signal_timestamp=str(signal_ts),signal_price=signal_price,recheck_timestamp=str(now),recheck_price=latest,signal_age_seconds=int(age),structural_stop=round(stop,2),stop_risk_pct=round(risk,2),reward_risk=round(rr,2))
    result["C"]["C5"]=_gate(c5,latest_price=latest,latest_ema5=latest_ema5,extension_pct=round(extension,2),stop_risk_pct=round(risk,2),reward_risk=round(rr,2),signal_age_seconds=int(age))
    if c2 and c3 and c4 and c5:
        result["strategy_state"]="ENTRY_VALIDATED"
    else:
        result["strategy_state"]="EXPIRED" if not c5 else "REJECTED"
        result["reject_reason"] += [k for k in ("C2","C3","C4","C5") if not result["C"].get(k,{}).get("passed",False)]
    return result

