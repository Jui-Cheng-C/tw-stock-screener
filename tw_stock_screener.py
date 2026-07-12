#!/usr/bin/env python3
"""
Taiwan stock auto screener using yfinance plus FinMind API.

Notes:
- LINE Notify officially ended service on 2025-03-31. The legacy sender is
  kept for compatibility, but email is the recommended notification path.
- K-line data and the first volume filter use Yahoo Finance/yfinance to avoid
  FinMind free-plan all-market TaiwanStockPrice limits.
- FinMind is used only for per-stock chip and revenue datasets.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import os
import smtplib
import sys
import time
import traceback
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any
from urllib import error, parse, request

import pandas as pd
import yfinance as yf


FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"


def env_str(name: str, default: str = "") -> str:
    return os.getenv(name, default)


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def load_env_file(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


@dataclasses.dataclass(frozen=True)
class Config:
    finmind_token: str = dataclasses.field(default_factory=lambda: env_str("FINMIND_TOKEN"))
    line_notify_token: str = dataclasses.field(default_factory=lambda: env_str("LINE_NOTIFY_TOKEN"))
    smtp_host: str = dataclasses.field(default_factory=lambda: env_str("SMTP_HOST"))
    smtp_port: int = dataclasses.field(default_factory=lambda: env_int("SMTP_PORT", 587))
    smtp_user: str = dataclasses.field(default_factory=lambda: env_str("SMTP_USER"))
    smtp_password: str = dataclasses.field(default_factory=lambda: env_str("SMTP_PASSWORD"))
    email_from: str = dataclasses.field(default_factory=lambda: env_str("EMAIL_FROM"))
    email_to: str = dataclasses.field(default_factory=lambda: env_str("EMAIL_TO"))
    min_volume_shares: int = dataclasses.field(
        default_factory=lambda: env_int("MIN_VOLUME_SHARES", 1000000)
    )
    request_sleep_sec: float = dataclasses.field(
        default_factory=lambda: env_float("REQUEST_SLEEP_SEC", 0.25)
    )
    max_stocks: int = dataclasses.field(default_factory=lambda: env_int("MAX_STOCKS", 0))
    enable_intraday_check: bool = dataclasses.field(
        default_factory=lambda: env_bool("ENABLE_INTRADAY_CHECK", True)
    )
    enable_big_holder_check: bool = dataclasses.field(
        default_factory=lambda: env_bool("ENABLE_BIG_HOLDER_CHECK", True)
    )
    intraday_days: int = dataclasses.field(default_factory=lambda: env_int("INTRADAY_DAYS", 18))
    stop_loss_lookback_days: int = dataclasses.field(
        default_factory=lambda: env_int("STOP_LOSS_LOOKBACK_DAYS", 20)
    )
    stop_loss_buffer_pct: float = dataclasses.field(
        default_factory=lambda: env_float("STOP_LOSS_BUFFER_PCT", 0.5)
    )
    atr_period: int = dataclasses.field(default_factory=lambda: env_int("ATR_PERIOD", 14))
    atr_multiplier: float = dataclasses.field(
        default_factory=lambda: env_float("ATR_MULTIPLIER", 1.5)
    )
    yahoo_batch_size: int = dataclasses.field(default_factory=lambda: env_int("YAHOO_BATCH_SIZE", 80))
    only_short_entry: bool = dataclasses.field(
        default_factory=lambda: env_bool("ONLY_SHORT_ENTRY", False)
    )
    report_date: str = dataclasses.field(default_factory=lambda: env_str("REPORT_DATE"))
    min_price: float = dataclasses.field(default_factory=lambda: env_float("MIN_PRICE", 20.0))
    min_turnover: float = dataclasses.field(
        default_factory=lambda: env_float("MIN_TURNOVER", 100_000_000.0)
    )
    max_daily_gain_pct: float = dataclasses.field(
        default_factory=lambda: env_float("MAX_DAILY_GAIN_PCT", 8.0)
    )
    max_3d_gain_pct: float = dataclasses.field(
        default_factory=lambda: env_float("MAX_3D_GAIN_PCT", 18.0)
    )
    max_ma20_distance_pct: float = dataclasses.field(
        default_factory=lambda: env_float("MAX_MA20_DISTANCE_PCT", 15.0)
    )
    max_stop_loss_risk_pct: float = dataclasses.field(
        default_factory=lambda: env_float("MAX_STOP_LOSS_RISK_PCT", 5.0)
    )


def finmind_get(
    dataset: str,
    *,
    token: str,
    data_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    timeout: int = 30,
) -> pd.DataFrame:
    params: dict[str, str] = {"dataset": dataset}
    if data_id:
        params["data_id"] = data_id
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date

    headers = {"Authorization": f"Bearer {token}"} if token else {}
    url = f"{FINMIND_URL}?{parse.urlencode(params)}"
    req = request.Request(url, headers=headers, method="GET")
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            payload: dict[str, Any] = json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        if "token_tail" in detail:
            detail = "FinMind request failed; sensitive token details hidden."
        raise RuntimeError(f"FinMind HTTP {exc.code} for {dataset}: {detail}") from exc
    if payload.get("status") not in (None, 200, "200"):
        raise RuntimeError(f"FinMind error for {dataset}: {payload}")
    return pd.DataFrame(payload.get("data", []))


def today_str() -> str:
    return dt.date.today().isoformat()


def cfg_date(cfg: Config) -> str:
    return cfg.report_date or today_str()


def date_days_ago(days: int, anchor: str | None = None) -> str:
    base = dt.date.fromisoformat(anchor) if anchor else dt.date.today()
    return (base - dt.timedelta(days=days)).isoformat()


def normalize_daily_price(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"])
    for col in ("open", "max", "min", "close", "Trading_Volume"):
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out.sort_values("date").dropna(subset=["close"])


def yahoo_symbol(stock_id: str, market_type: str | None = None) -> str:
    suffix = ".TWO" if str(market_type).lower() == "tpex" else ".TW"
    return f"{stock_id}{suffix}"


def normalize_yahoo_history(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    out = df.copy()
    if isinstance(out.columns, pd.MultiIndex):
        first_level = list(out.columns.get_level_values(0))
        if {"Open", "High", "Low", "Close", "Volume"}.issubset(set(first_level)):
            out.columns = out.columns.get_level_values(0)
        else:
            out.columns = out.columns.get_level_values(-1)
    out = out.reset_index()
    date_col = "Datetime" if "Datetime" in out.columns else "Date"
    rename_map = {
        date_col: "date",
        "Open": "open",
        "High": "max",
        "Low": "min",
        "Close": "close",
        "Volume": "Trading_Volume",
    }
    out = out.rename(columns=rename_map)
    required = ["date", "open", "max", "min", "close", "Trading_Volume"]
    missing = [col for col in required if col not in out.columns]
    if missing:
        raise RuntimeError(f"Yahoo data is missing columns: {missing}")
    out = out[required].copy()
    out["date"] = pd.to_datetime(out["date"]).dt.tz_localize(None)
    for col in ("open", "max", "min", "close", "Trading_Volume"):
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out.sort_values("date").dropna(subset=["close"])


def filter_by_report_date(df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    if df.empty or not cfg.report_date:
        return df
    cutoff = pd.Timestamp(cfg.report_date) + pd.Timedelta(days=1)
    return df[pd.to_datetime(df["date"]) < cutoff].copy()


def get_yahoo_daily(
    stock_id: str,
    market_type: str | None = None,
    cfg: Config | None = None,
) -> pd.DataFrame:
    symbol = yahoo_symbol(stock_id, market_type)
    raw = yf.Ticker(symbol).history(
        period="10y",
        interval="1d",
        auto_adjust=False,
    )
    out = normalize_yahoo_history(raw)
    return filter_by_report_date(out, cfg) if cfg else out


def get_yahoo_intraday(
    stock_id: str,
    market_type: str | None = None,
    cfg: Config | None = None,
) -> pd.DataFrame:
    symbol = yahoo_symbol(stock_id, market_type)
    raw = yf.Ticker(symbol).history(
        period="60d",
        interval="60m",
        auto_adjust=False,
    )
    out = normalize_yahoo_history(raw)
    return filter_by_report_date(out, cfg) if cfg else out


def add_indicators(df: pd.DataFrame, atr_period: int = 14) -> pd.DataFrame:
    out = df.copy()
    close = out["close"].astype(float)
    high = out["max"].astype(float)
    low = out["min"].astype(float)
    prev_close = close.shift(1)
    true_range = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    out["ma5"] = close.rolling(5).mean()
    out["ma10"] = close.rolling(10).mean()
    out["ma20"] = close.rolling(20).mean()
    out["ma60"] = close.rolling(60).mean()
    out["atr"] = true_range.rolling(atr_period).mean()
    low9 = low.rolling(9).min()
    high9 = high.rolling(9).max()
    rsv = (close - low9) / (high9 - low9) * 100
    out["kd_k"] = rsv.ewm(alpha=1 / 3, adjust=False).mean()
    out["kd_d"] = out["kd_k"].ewm(alpha=1 / 3, adjust=False).mean()
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    out["dif"] = ema12 - ema26
    out["macd"] = out["dif"].ewm(span=9, adjust=False).mean()
    out["hist"] = out["dif"] - out["macd"]
    return out


def resample_ohlcv(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    if df.empty:
        return df
    frame = df.set_index("date").sort_index()
    resampled = frame.resample(rule).agg(
        {
            "open": "first",
            "max": "max",
            "min": "min",
            "close": "last",
            "Trading_Volume": "sum",
        }
    )
    return resampled.dropna(subset=["close"]).reset_index()


def trend_and_macd_ok(df: pd.DataFrame) -> bool:
    if len(df) < 60:
        return False
    latest = add_indicators(df).iloc[-1]
    values = latest[["ma5", "ma20", "ma60", "dif", "macd"]]
    if values.isna().any():
        return False
    return bool(
        latest["ma5"] > latest["ma20"] > latest["ma60"]
        and latest["dif"] > 0
        and latest["macd"] > 0
    )


def ma_alignment_ok(df: pd.DataFrame) -> bool:
    if len(df) < 60:
        return False
    latest = add_indicators(df).iloc[-1]
    values = latest[["ma5", "ma20", "ma60"]]
    if values.isna().any():
        return False
    return bool(latest["ma5"] > latest["ma20"] > latest["ma60"])


def macd_above_zero_ok(df: pd.DataFrame) -> bool:
    if len(df) < 35:
        return False
    latest = add_indicators(df).iloc[-1]
    values = latest[["dif", "macd"]]
    if values.isna().any():
        return False
    return bool(latest["dif"] > 0 and latest["macd"] > 0)


def all_ma_alignment_ok(daily: pd.DataFrame) -> bool:
    weekly = resample_ohlcv(daily, "W-FRI")
    monthly = resample_ohlcv(daily, "ME")
    return ma_alignment_ok(daily) and ma_alignment_ok(weekly) and ma_alignment_ok(monthly)


def all_macd_above_zero_ok(daily: pd.DataFrame) -> bool:
    weekly = resample_ohlcv(daily, "W-FRI")
    monthly = resample_ohlcv(daily, "ME")
    return (
        macd_above_zero_ok(daily)
        and macd_above_zero_ok(weekly)
        and macd_above_zero_ok(monthly)
    )


def all_big_timeframes_ok(daily: pd.DataFrame) -> bool:
    weekly = resample_ohlcv(daily, "W-FRI")
    monthly = resample_ohlcv(daily, "ME")
    return (
        trend_and_macd_ok(daily)
        and trend_and_macd_ok(weekly)
        and trend_and_macd_ok(monthly)
    )


def calculate_stop_loss(daily: pd.DataFrame, cfg: Config) -> dict[str, Any]:
    ind = add_indicators(daily, cfg.atr_period)
    latest = ind.iloc[-1]
    close = float(latest["close"])
    buffer = cfg.stop_loss_buffer_pct / 100
    lookback = max(cfg.stop_loss_lookback_days, 5)
    recent = ind.tail(lookback)

    candidates: list[tuple[str, float]] = []
    swing_low = float(recent["min"].min())
    candidates.append(("swing_low", swing_low * (1 - buffer)))

    if not pd.isna(latest["atr"]):
        candidates.append(("atr", close - float(latest["atr"]) * cfg.atr_multiplier))

    if not pd.isna(latest["ma20"]):
        candidates.append(("ma20", float(latest["ma20"]) * (1 - buffer)))

    valid = [(name, price) for name, price in candidates if price > 0 and price < close]
    if not valid:
        return {
            "last_close": close,
            "stop_loss": None,
            "stop_loss_risk_pct": None,
            "stop_loss_method": "n/a",
        }

    method, stop_price = max(valid, key=lambda item: item[1])
    return {
        "last_close": close,
        "stop_loss": round(stop_price, 2),
        "stop_loss_risk_pct": round((close - stop_price) / close * 100, 2),
        "stop_loss_method": method,
    }


def intraday_entry_ok(kbar: pd.DataFrame) -> bool:
    if kbar.empty:
        return False
    df = kbar.copy()
    df["date"] = pd.to_datetime(df["date"])
    for col in ("open", "max", "min", "close", "Trading_Volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    hourly = (
        df.set_index("date")
        .sort_index()
        .resample("60min")
        .agg(
            {
                "open": "first",
                "max": "max",
                "min": "min",
                "close": "last",
                "Trading_Volume": "sum",
            }
        )
        .dropna(subset=["close"])
        .reset_index()
    )
    if len(hourly) < 35:
        return False
    ind = add_indicators(hourly)
    tail = ind.tail(4)
    latest = tail.iloc[-1]
    if pd.isna(latest["dif"]) or pd.isna(latest["macd"]):
        return False
    if not (latest["dif"] > 0 and latest["macd"] > 0):
        return False

    h = tail["hist"].tolist()
    crossed_up_now = h[-2] <= 0 < h[-1]
    crossed_up_prev = len(h) >= 3 and h[-3] <= 0 < h[-2] and h[-1] > 0
    red_growing_1 = h[-2] > 0 and h[-1] > h[-2]
    red_growing_2 = len(h) >= 3 and h[-3] > 0 and h[-2] > h[-3] and h[-1] > h[-2]
    return bool(crossed_up_now or crossed_up_prev or red_growing_1 or red_growing_2)


def intraday_short_entry_signal(kbar: pd.DataFrame) -> tuple[bool, str, int]:
    if kbar.empty:
        return False, "", 0
    df = kbar.copy()
    df["date"] = pd.to_datetime(df["date"])
    for col in ("open", "max", "min", "close", "Trading_Volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    hourly = (
        df.set_index("date")
        .sort_index()
        .resample("60min")
        .agg(
            {
                "open": "first",
                "max": "max",
                "min": "min",
                "close": "last",
                "Trading_Volume": "sum",
            }
        )
        .dropna(subset=["close"])
        .reset_index()
    )
    if len(hourly) < 35:
        return False, "", 0
    ind = add_indicators(hourly)
    tail = ind.tail(4)
    if tail[["dif", "macd", "hist"]].isna().any().any():
        return False, "", 0

    dif = tail["dif"].tolist()
    macd = tail["macd"].tolist()
    hist = tail["hist"].tolist()

    golden_now = dif[-2] <= macd[-2] and dif[-1] > macd[-1]
    golden_prev = len(dif) >= 3 and dif[-3] <= macd[-3] and dif[-2] > macd[-2]

    hist_turn_now = hist[-3] < hist[-2] < 0 < hist[-1]
    hist_turn_prev = len(hist) >= 4 and hist[-4] < hist[-3] < 0 < hist[-2] and hist[-1] > 0

    latest_above_zero = dif[-1] > 0 and macd[-1] > 0
    prev_signal_above_zero = len(dif) >= 3 and dif[-2] > 0 and macd[-2] > 0
    priority = 2 if (
        (golden_now and latest_above_zero)
        or (golden_prev and prev_signal_above_zero)
        or (hist_turn_now and latest_above_zero)
        or (hist_turn_prev and prev_signal_above_zero)
    ) else 1

    if golden_now or golden_prev:
        return True, "60分K黃金交叉", priority
    if hist_turn_now or hist_turn_prev:
        return True, "60分K綠柱轉紅", priority
    return False, "", 0


def daily_common_gate(daily: pd.DataFrame) -> bool:
    return macd_above_zero_ok(daily)


def price_change_pct(daily: pd.DataFrame) -> float:
    if len(daily) < 2:
        return 0.0
    close = pd.to_numeric(daily["close"], errors="coerce")
    latest = float(close.iloc[-1])
    previous = float(close.iloc[-2])
    if previous <= 0:
        return 0.0
    return (latest - previous) / previous * 100


def recent_gain_pct(daily: pd.DataFrame, days: int = 3) -> float:
    if len(daily) <= days:
        return 0.0
    close = pd.to_numeric(daily["close"], errors="coerce")
    latest = float(close.iloc[-1])
    base = float(close.iloc[-days - 1])
    if base <= 0:
        return 0.0
    return (latest - base) / base * 100


def ma20_distance_pct(daily: pd.DataFrame) -> float:
    if len(daily) < 20:
        return 999.0
    latest = add_indicators(daily).iloc[-1]
    if pd.isna(latest["ma20"]) or float(latest["ma20"]) <= 0:
        return 999.0
    return (float(latest["close"]) - float(latest["ma20"])) / float(latest["ma20"]) * 100


def common_trade_filter_ok(
    daily: pd.DataFrame,
    cfg: Config,
    row: pd.Series,
    stop: dict[str, Any],
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    last_close = float(stop["last_close"])
    turnover = float(row["Trading_Volume"]) * last_close
    daily_gain = price_change_pct(daily)
    gain_3d = recent_gain_pct(daily, 3)
    ma20_distance = ma20_distance_pct(daily)
    stop_risk = stop.get("stop_loss_risk_pct")

    if last_close < cfg.min_price:
        reasons.append("股價過低")
    if turnover < cfg.min_turnover:
        reasons.append("成交金額不足")
    if daily_gain > cfg.max_daily_gain_pct:
        reasons.append("今日漲幅過熱")
    if daily_gain >= 9.5:
        reasons.append("接近漲停鎖死")
    if gain_3d > cfg.max_3d_gain_pct:
        reasons.append("近3日漲幅過熱")
    if ma20_distance > cfg.max_ma20_distance_pct:
        reasons.append("離月線過遠")
    if stop_risk is None or float(stop_risk) > cfg.max_stop_loss_risk_pct:
        reasons.append("停損距離過遠")
    return not reasons, reasons


def support_pullback_ok(daily: pd.DataFrame) -> bool:
    if len(daily) < 60:
        return False
    ind = add_indicators(daily)
    latest = ind.iloc[-1]
    previous = ind.iloc[-2]
    required = ["close", "open", "ma5", "ma10", "ma20", "ma60"]
    if latest[required].isna().any():
        return False
    close = float(latest["close"])
    moving_averages = [float(latest["ma5"]), float(latest["ma10"]), float(latest["ma20"])]
    near_support = any(abs(close - ma) / ma <= 0.03 for ma in moving_averages if ma > 0)
    stopped_falling = close >= float(latest["open"]) or close >= float(previous["close"])
    return bool(float(latest["ma20"]) > float(latest["ma60"]) and near_support and stopped_falling)


def intraday_kd_low_golden_cross(kbar: pd.DataFrame) -> bool:
    if kbar.empty:
        return False
    df = kbar.copy()
    df["date"] = pd.to_datetime(df["date"])
    for col in ("open", "max", "min", "close", "Trading_Volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    hourly = (
        df.set_index("date")
        .sort_index()
        .resample("60min")
        .agg(
            {
                "open": "first",
                "max": "max",
                "min": "min",
                "close": "last",
                "Trading_Volume": "sum",
            }
        )
        .dropna(subset=["close"])
        .reset_index()
    )
    if len(hourly) < 15:
        return False
    ind = add_indicators(hourly)
    tail = ind.tail(4)
    if tail[["kd_k", "kd_d"]].isna().any().any():
        return False
    k = tail["kd_k"].tolist()
    d = tail["kd_d"].tolist()
    golden_now = k[-2] <= d[-2] and k[-1] > d[-1]
    golden_prev = len(k) >= 3 and k[-3] <= d[-3] and k[-2] > d[-2]
    low_zone_now = min(k[-2], d[-2], k[-1], d[-1]) <= 25
    low_zone_prev = len(k) >= 3 and min(k[-3], d[-3], k[-2], d[-2]) <= 25
    return bool((golden_now and low_zone_now) or (golden_prev and low_zone_prev))


def breakout_platform_ok(daily: pd.DataFrame) -> bool:
    if len(daily) < 40:
        return False
    ind = add_indicators(daily)
    latest = ind.iloc[-1]
    required = ["open", "close", "max", "ma5", "ma10", "ma20", "Trading_Volume"]
    if latest[required].isna().any():
        return False

    recent20 = ind.tail(20).copy()
    before_today = recent20.iloc[:-1]
    if before_today.empty:
        return False
    high_label = before_today["max"].astype(float).idxmax()
    high_iloc = before_today.index.get_loc(high_label)
    days_since_high = len(recent20) - 1 - high_iloc
    if not 3 <= days_since_high <= 10:
        return False

    platform = recent20.iloc[high_iloc:-1]
    if len(platform) < 3:
        return False
    platform_high = float(platform["max"].max())
    platform_low = float(platform["min"].min())
    tight_platform = platform_low > 0 and (platform_high - platform_low) / platform_low <= 0.12

    close = float(latest["close"])
    open_price = float(latest["open"])
    above_short_ma = (
        close > float(latest["ma5"])
        and close > float(latest["ma10"])
        and close > float(latest["ma20"])
    )
    red_body = close > open_price and (close - open_price) / open_price >= 0.018
    today_volume = float(latest["Trading_Volume"])
    platform_avg_volume = float(platform["Trading_Volume"].mean())
    recent_avg_volume = float(ind.tail(5)["Trading_Volume"].mean())
    prior_volume = ind.iloc[max(0, high_iloc - 10) : high_iloc]["Trading_Volume"]
    prior_avg_volume = float(prior_volume.mean()) if not prior_volume.empty else platform_avg_volume
    volume_breakout = today_volume > platform_avg_volume * 1.25 or today_volume > recent_avg_volume * 1.25
    volume_shrank = platform_avg_volume <= prior_avg_volume * 1.1
    return bool(tight_platform and above_short_ma and (red_body or volume_breakout) and volume_shrank)


def institutional_single_day_momentum(stock_id: str, cfg: Config) -> tuple[bool, int, int, int]:
    df = finmind_get(
        "TaiwanStockInstitutionalInvestorsBuySellWide",
        token=cfg.finmind_token,
        data_id=stock_id,
        start_date=date_days_ago(10, cfg_date(cfg)),
        end_date=cfg_date(cfg),
    )
    if df.empty:
        return False, 0, 0, 0
    df = df.sort_values("date").tail(1)
    for col in df.columns:
        if col.endswith("_buy") or col.endswith("_sell"):
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    latest = df.iloc[-1]

    def value(name: str) -> float:
        return float(latest[name]) if name in df.columns else 0.0

    foreign_net = (
        value("Foreign_Investor_buy")
        + value("Foreign_Dealer_Self_buy")
        - value("Foreign_Investor_sell")
        - value("Foreign_Dealer_Self_sell")
    )
    trust_net = value("Investment_Trust_buy") - value("Investment_Trust_sell")
    total_net = sum(value(col) for col in df.columns if col.endswith("_buy")) - sum(
        value(col) for col in df.columns if col.endswith("_sell")
    )
    ok = foreign_net > 1_500_000 or trust_net > 1_000_000
    return bool(ok), int(foreign_net), int(trust_net), int(total_net)


def institutional_signals(stock_id: str, cfg: Config) -> dict[str, Any]:
    df = finmind_get(
        "TaiwanStockInstitutionalInvestorsBuySellWide",
        token=cfg.finmind_token,
        data_id=stock_id,
        start_date=date_days_ago(14, cfg_date(cfg)),
        end_date=cfg_date(cfg),
    )
    if df.empty:
        return {
            "foreign_5d_net": 0,
            "trust_5d_net": 0,
            "inst_5d_total_net": 0,
            "foreign_today_net": 0,
            "trust_today_net": 0,
            "inst_today_total_net": 0,
            "inst_today_ok": False,
        }
    df = df.sort_values("date")
    for col in df.columns:
        if col.endswith("_buy") or col.endswith("_sell"):
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    def net_values(frame: pd.DataFrame) -> tuple[int, int, int]:
        def sum_col(name: str) -> float:
            return float(frame[name].sum()) if name in frame.columns else 0.0

        foreign_net = (
            sum_col("Foreign_Investor_buy")
            + sum_col("Foreign_Dealer_Self_buy")
            - sum_col("Foreign_Investor_sell")
            - sum_col("Foreign_Dealer_Self_sell")
        )
        trust_net = sum_col("Investment_Trust_buy") - sum_col("Investment_Trust_sell")
        buy_total = sum(sum_col(col) for col in frame.columns if col.endswith("_buy"))
        sell_total = sum(sum_col(col) for col in frame.columns if col.endswith("_sell"))
        return int(foreign_net), int(trust_net), int(buy_total - sell_total)

    foreign_5d, trust_5d, total_5d = net_values(df.tail(5))
    foreign_today, trust_today, total_today = net_values(df.tail(1))
    return {
        "foreign_5d_net": foreign_5d,
        "trust_5d_net": trust_5d,
        "inst_5d_total_net": total_5d,
        "foreign_today_net": foreign_today,
        "trust_today_net": trust_today,
        "inst_today_total_net": total_today,
        "inst_today_ok": foreign_today > 1_500_000 or trust_today > 1_000_000,
    }


def institutional_summary(stock_id: str, cfg: Config) -> tuple[bool, int, int, int]:
    df = finmind_get(
        "TaiwanStockInstitutionalInvestorsBuySellWide",
        token=cfg.finmind_token,
        data_id=stock_id,
        start_date=date_days_ago(14, cfg_date(cfg)),
        end_date=cfg_date(cfg),
    )
    if df.empty:
        return False, 0, 0, 0
    df = df.sort_values("date").tail(5)
    for col in df.columns:
        if col.endswith("_buy") or col.endswith("_sell"):
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    def sum_col(name: str) -> float:
        return float(df[name].sum()) if name in df.columns else 0.0

    foreign_net = (
        sum_col("Foreign_Investor_buy")
        + sum_col("Foreign_Dealer_Self_buy")
        - sum_col("Foreign_Investor_sell")
        - sum_col("Foreign_Dealer_Self_sell")
    )
    trust_net = sum_col("Investment_Trust_buy") - sum_col("Investment_Trust_sell")
    buy_total = sum(sum_col(col) for col in df.columns if col.endswith("_buy"))
    sell_total = sum(sum_col(col) for col in df.columns if col.endswith("_sell"))
    total_net = buy_total - sell_total
    return bool(foreign_net > 0 and trust_net > 0), int(foreign_net), int(trust_net), int(total_net)


def score_short_candidate(item: dict[str, Any]) -> dict[str, Any]:
    score = 50
    reasons: list[str] = []
    warnings: list[str] = []

    category_keys = set(item.get("category_keys", []))
    if len(category_keys) >= 2:
        score += 12
        reasons.append("同時符合多類訊號")
    if item.get("short_entry_ok"):
        score += 15
        reasons.append("60K剛轉強")
    if int(item.get("short_entry_priority") or 0) >= 2:
        score += 8
        reasons.append("60K位於0軸上")

    risk = item.get("stop_loss_risk_pct")
    if risk is not None:
        risk = float(risk)
        if 2 <= risk <= 4:
            score += 15
            reasons.append("停損距離漂亮")
        elif risk <= 5:
            score += 8
            reasons.append("停損可控")
        else:
            score -= 25
            warnings.append("停損偏遠")

    turnover = float(item.get("turnover") or 0)
    if turnover >= 300_000_000:
        score += 10
        reasons.append("成交金額充足")
    elif turnover < 100_000_000:
        score -= 15
        warnings.append("成交金額不足")

    inst_net = int(item.get("inst_5d_total_net") or 0)
    if inst_net > 0:
        score += 8
        reasons.append("法人近期偏買")
    if int(item.get("trust_5d_net") or 0) > 0:
        score += 6
        reasons.append("投信買盤支撐")

    if item.get("support_ok"):
        score += 8
        reasons.append("回檔支撐轉強")
    if item.get("breakout_ok"):
        score += 8
        reasons.append("整理後再突破")

    daily_pct = float(item.get("daily_pct") or 0)
    gain_3d = float(item.get("gain_3d_pct") or 0)
    ma20_dist = float(item.get("ma20_distance_pct") or 0)
    if daily_pct > 7:
        score -= 15
        warnings.append("今日漲幅偏高")
    if gain_3d > 15:
        score -= 20
        warnings.append("近3日漲幅偏高")
    if ma20_dist > 12:
        score -= 20
        warnings.append("離月線偏遠")

    item["short_score"] = max(0, min(100, int(round(score))))
    item["score_reasons"] = reasons
    item["score_warnings"] = warnings
    item["top_reason"] = build_top_reason(item)
    return item


def build_top_reason(item: dict[str, Any]) -> str:
    reasons = item.get("score_reasons", [])
    warnings = item.get("score_warnings", [])
    main = "、".join(reasons[:3]) if reasons else "型態符合短線候選條件"
    risk = f"；提醒：{warnings[0]}，避免追價。" if warnings else "；停損線需嚴格執行。"
    return (main + risk)[:80]


def institutional_ok(stock_id: str, cfg: Config) -> tuple[bool, int, int]:
    ok, foreign_net, trust_net, _ = institutional_summary(stock_id, cfg)
    return ok, foreign_net, trust_net


def holder_level_floor(level: str) -> int | None:
    text = str(level).replace(",", "")
    if "-" in text:
        left = text.split("-", 1)[0]
        return int(left) if left.isdigit() else None
    if text.endswith("+") and text[:-1].isdigit():
        return int(text[:-1])
    return int(text) if text.isdigit() else None


def big_holder_ok(stock_id: str, cfg: Config, threshold_lots: int = 400) -> tuple[bool, float, float]:
    df = finmind_get(
        "TaiwanStockHoldingSharesPer",
        token=cfg.finmind_token,
        data_id=stock_id,
        start_date=date_days_ago(30, cfg_date(cfg)),
        end_date=cfg_date(cfg),
    )
    if df.empty:
        return False, 0.0, 0.0
    df["date"] = pd.to_datetime(df["date"])
    df["percent"] = pd.to_numeric(df["percent"], errors="coerce").fillna(0.0)
    df["floor"] = df["HoldingSharesLevel"].map(holder_level_floor)
    big = df[df["floor"].fillna(0) >= threshold_lots * 1000]
    by_date = big.groupby("date", as_index=False)["percent"].sum().sort_values("date")
    if len(by_date) < 2:
        return False, 0.0, 0.0
    prev = float(by_date.iloc[-2]["percent"])
    latest = float(by_date.iloc[-1]["percent"])
    return bool(latest > prev), latest, prev


def revenue_ok(stock_id: str, cfg: Config) -> tuple[bool, float | None, float | None]:
    df = finmind_get(
        "TaiwanStockMonthRevenue",
        token=cfg.finmind_token,
        data_id=stock_id,
        start_date=date_days_ago(430, cfg_date(cfg)),
        end_date=cfg_date(cfg),
    )
    if df.empty:
        return False, None, None
    df = df.sort_values(["revenue_year", "revenue_month"])
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")
    latest = df.iloc[-1]
    prev_month = df.iloc[-2] if len(df) >= 2 else None
    same_month_last_year = df[
        (df["revenue_year"] == int(latest["revenue_year"]) - 1)
        & (df["revenue_month"] == int(latest["revenue_month"]))
    ]
    mom = None
    yoy = None
    if prev_month is not None and prev_month["revenue"]:
        mom = (latest["revenue"] - prev_month["revenue"]) / prev_month["revenue"] * 100
    if not same_month_last_year.empty and same_month_last_year.iloc[-1]["revenue"]:
        base = same_month_last_year.iloc[-1]["revenue"]
        yoy = (latest["revenue"] - base) / base * 100
    return bool((mom is not None and mom > 0) or (yoy is not None and yoy > 0)), mom, yoy


def get_universe(cfg: Config) -> pd.DataFrame:
    if not cfg.finmind_token:
        raise RuntimeError("FINMIND_TOKEN is empty. Fill it in .env before running the screener.")
    info = finmind_get("TaiwanStockInfo", token=cfg.finmind_token)
    info = info[
        info["type"].isin(["twse", "tpex"])
        & info["stock_id"].str.fullmatch(r"\d{4}", na=False)
        & ~info["industry_category"].isin(["ETF", "大盤", "Index", "所有證券"])
    ].drop_duplicates(subset=["stock_id"]).copy()
    universe = build_universe_by_yahoo_volume(info, cfg)
    if cfg.max_stocks > 0:
        universe = universe.head(cfg.max_stocks)
    return universe


def build_universe_by_yahoo_volume(info: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    symbols = [yahoo_symbol(row["stock_id"], row["type"]) for _, row in info.iterrows()]
    lookup = {yahoo_symbol(row["stock_id"], row["type"]): row for _, row in info.iterrows()}
    for start in range(0, len(symbols), cfg.yahoo_batch_size):
        batch = symbols[start : start + cfg.yahoo_batch_size]
        print(f"[volume] yfinance batch {start + 1}-{start + len(batch)} / {len(symbols)}")
        try:
            raw = yf.download(
                " ".join(batch),
                period="5d",
                interval="1d",
                auto_adjust=False,
                group_by="ticker",
                progress=False,
                threads=True,
            )
        except Exception as exc:
            print(f"[warn] yfinance batch failed: {exc}", file=sys.stderr)
            continue

        for symbol in batch:
            try:
                if isinstance(raw.columns, pd.MultiIndex):
                    if symbol not in raw.columns.get_level_values(0):
                        continue
                    hist = raw[symbol].dropna(how="all")
                else:
                    hist = raw.dropna(how="all")
                if hist.empty or "Volume" not in hist.columns:
                    continue
                hist = hist.dropna(subset=["Close"])
                if cfg.report_date:
                    cutoff = pd.Timestamp(cfg.report_date) + pd.Timedelta(days=1)
                    idx = pd.to_datetime(hist.index)
                    if getattr(idx, "tz", None) is not None:
                        idx = idx.tz_localize(None)
                    hist = hist[idx < cutoff]
                if hist.empty:
                    continue
                latest = hist.iloc[-1]
                volume = float(latest["Volume"])
                if volume < cfg.min_volume_shares:
                    continue
                source = lookup[symbol]
                rows.append(
                    {
                        "stock_id": source["stock_id"],
                        "stock_name": source["stock_name"],
                        "industry_category": source.get("industry_category", ""),
                        "type": source.get("type", ""),
                        "yahoo_symbol": symbol,
                        "Trading_Volume": volume,
                    }
                )
            except Exception as exc:
                print(f"[skip-volume] {symbol}: {exc}", file=sys.stderr)
        time.sleep(cfg.request_sleep_sec)

    if not rows:
        return pd.DataFrame(columns=["stock_id", "stock_name", "type", "Trading_Volume"])
    return pd.DataFrame(rows).sort_values("Trading_Volume", ascending=False)


def screen_stock(row: pd.Series, cfg: Config) -> dict[str, dict[str, Any]]:
    stock_id = row["stock_id"]
    stock_name = row["stock_name"]
    market_type = row.get("type", "")
    try:
        daily = get_yahoo_daily(stock_id, market_type, cfg)
        if cfg.only_short_entry:
            return screen_short_entry_only(row, cfg, daily)

        if not daily_common_gate(daily):
            return {}

        stop = calculate_stop_loss(daily, cfg)
        if stop["stop_loss"] is None:
            return {}
        filter_ok, filter_reasons = common_trade_filter_ok(daily, cfg, row, stop)
        if not filter_ok:
            return {}

        kd_pullback_ok = False
        short_entry_ok = False
        short_entry_reason = ""
        short_entry_priority = 0
        if cfg.enable_intraday_check:
            intraday = get_yahoo_intraday(stock_id, market_type, cfg)
            kd_pullback_ok = intraday_kd_low_golden_cross(intraday)
            short_entry_ok, short_entry_reason, short_entry_priority = intraday_short_entry_signal(
                intraday
            )

        foreign_net = 0
        trust_net = 0
        inst_total_net = 0
        today_foreign_net = 0
        today_trust_net = 0
        today_inst_total_net = 0
        inst_today_ok = False
        try:
            inst = institutional_signals(stock_id, cfg)
            foreign_net = inst["foreign_5d_net"]
            trust_net = inst["trust_5d_net"]
            inst_total_net = inst["inst_5d_total_net"]
            today_foreign_net = inst["foreign_today_net"]
            today_trust_net = inst["trust_today_net"]
            today_inst_total_net = inst["inst_today_total_net"]
            inst_today_ok = inst["inst_today_ok"]
        except Exception as exc:
            print(f"[chip-warn] {stock_id} institutional data unavailable: {exc}", file=sys.stderr)

        support_ok = support_pullback_ok(daily)
        breakout_ok = breakout_platform_ok(daily)
        daily_pct = price_change_pct(daily)
        gain_3d = recent_gain_pct(daily, 3)
        ma20_dist = ma20_distance_pct(daily)

        base = {
            "stock_id": stock_id,
            "stock_name": stock_name,
            "last_close": stop["last_close"],
            "stop_loss": stop["stop_loss"],
            "stop_loss_risk_pct": stop["stop_loss_risk_pct"],
            "stop_loss_method": stop["stop_loss_method"],
            "volume_lots": round(row["Trading_Volume"] / 1000),
            "daily_pct": daily_pct,
            "gain_3d_pct": gain_3d,
            "ma20_distance_pct": ma20_dist,
            "support_ok": support_ok,
            "kd_pullback_ok": kd_pullback_ok,
            "breakout_ok": breakout_ok,
            "short_entry_ok": short_entry_ok,
            "short_entry_reason": short_entry_reason,
            "short_entry_priority": short_entry_priority,
            "foreign_5d_net": foreign_net,
            "trust_5d_net": trust_net,
            "inst_5d_total_net": inst_total_net,
            "foreign_today_net": today_foreign_net,
            "trust_today_net": today_trust_net,
            "inst_today_total_net": today_inst_total_net,
            "turnover": float(row["Trading_Volume"]) * stop["last_close"],
            "filter_reasons": filter_reasons,
        }

        categories: dict[str, dict[str, Any]] = {}
        if support_ok and kd_pullback_ok:
            categories["strong_continuation"] = {
                **base,
                "category": "強勢續攻股",
                "subtype": "回檔支撐型",
            }
        if breakout_ok:
            categories["relay_breakout"] = {
                **base,
                "category": "中繼再漲股",
                "subtype": "平台突破型",
            }
        if (inst_today_ok or foreign_net > 0 or trust_net > 0) and daily_pct >= 0:
            categories["institutional_watch"] = {
                **base,
                "category": "法人資金認養股",
                "subtype": "觀察追蹤",
            }
        if short_entry_ok:
            categories["precision_entry"] = {
                **base,
                "category": "60K精準進場股",
                "subtype": short_entry_reason,
            }
        return categories
    except Exception as exc:
        print(f"[skip] {stock_id} {stock_name}: {exc}", file=sys.stderr)
        return {}
    finally:
        time.sleep(cfg.request_sleep_sec)


def screen_short_entry_only(
    row: pd.Series,
    cfg: Config,
    daily: pd.DataFrame,
) -> dict[str, dict[str, Any]]:
    stock_id = row["stock_id"]
    stock_name = row["stock_name"]
    market_type = row.get("type", "")
    if not daily_common_gate(daily):
        return {}

    short_entry_ok, short_entry_reason, short_entry_priority = intraday_short_entry_signal(
        get_yahoo_intraday(stock_id, market_type, cfg)
    )
    if not short_entry_ok:
        return {}

    stop = calculate_stop_loss(daily, cfg)
    if stop["stop_loss"] is None:
        return {}

    try:
        inst = institutional_signals(stock_id, cfg)
        foreign_net = inst["foreign_5d_net"]
        trust_net = inst["trust_5d_net"]
        inst_total_net = inst["inst_5d_total_net"]
    except Exception as exc:
        print(f"[chip-warn] {stock_id} institutional data unavailable: {exc}", file=sys.stderr)
        foreign_net = 0
        trust_net = 0
        inst_total_net = 0

    item = {
        "stock_id": stock_id,
        "stock_name": stock_name,
        "last_close": stop["last_close"],
        "stop_loss": stop["stop_loss"],
        "stop_loss_risk_pct": stop["stop_loss_risk_pct"],
        "stop_loss_method": stop["stop_loss_method"],
        "volume_lots": round(row["Trading_Volume"] / 1000),
        "short_entry_ok": True,
        "short_entry_reason": short_entry_reason,
        "short_entry_priority": short_entry_priority,
        "foreign_5d_net": foreign_net,
        "trust_5d_net": trust_net,
        "inst_5d_total_net": inst_total_net,
        "turnover": float(row["Trading_Volume"]) * stop["last_close"],
        "category": "60K精準進場股",
        "subtype": short_entry_reason,
    }
    return {"precision_entry": item}


CATEGORY_TITLES = {
    "strong_continuation": "第一類：強勢續攻股（回檔支撐型）",
    "relay_breakout": "第二類：中繼再漲股（平台突破型）",
    "institutional_watch": "第三類：法人資金認養股（觀察追蹤）",
    "precision_entry": "第四類：60K精準進場股",
}

LIMITED_CATEGORY_COUNTS = {
    "strong_continuation": 3,
    "relay_breakout": 3,
    "institutional_watch": 3,
    "precision_entry": 3,
    "shortlist": 9,
}


def format_report(results: dict[str, list[dict[str, Any]]], cfg: Config) -> tuple[str, str]:
    report_date = cfg_date(cfg)
    subject = f"台股短線精選報告 - {report_date}"
    total = sum(len(items) for items in results.values())
    text_sections: list[str] = [
        f"台股短線精選報告日期：{report_date}，本次共篩出 {total} 筆分類結果。",
        "提醒：若今日為週末、國定假日、颱風休市或市場未交易，資料來源可能回傳最近一個交易日的最新可取得資料。",
    ]
    html_sections: list[str] = [
        "<html><body>",
        f"<h2>台股短線精選報告 - {report_date}</h2>",
        f"<p>本次共篩出 {total} 筆分類結果。</p>",
        "<p>提醒：若今日為週末、國定假日、颱風休市或市場未交易，資料來源可能回傳最近一個交易日的最新可取得資料。</p>",
    ]
    keys = [key for key in CATEGORY_TITLES if key in results]
    for key in keys:
        title = CATEGORY_TITLES[key]
        rows = results.get(key, [])
        text, html = format_category_section(title, rows)
        text_sections.append(text)
        html_sections.append(html)
    shortlist = results.get("shortlist", [])
    text, html = format_shortlist_section(shortlist)
    text_sections.append(text)
    html_sections.append(html)
    text, html = format_top_reason_section(shortlist[:3])
    text_sections.append(text)
    html_sections.append(html)
    html_sections.append("</body></html>")
    return subject, "\n\n".join(text_sections) + "\n\nHTML_TABLE:\n" + "\n".join(html_sections)


def format_status_report(error_text: str, cfg: Config) -> tuple[str, str]:
    report_date = cfg_date(cfg)
    subject = f"台股每日排程狀態通知 - {report_date}"
    safe_error = error_text.replace(os.getenv("FINMIND_TOKEN", ""), "[hidden]")
    plain = "\n\n".join(
        [
            f"台股每日排程已於 {report_date} 啟動，但本次未能完成正式四大類選股報告。",
            "你仍收到這封信，代表每日通知機制有啟動；請稍後檢查資料源、FinMind 額度、GitHub Actions 或 Gmail SMTP 設定。",
            "可能原因：休市資料尚未更新、FinMind 免費額度上限、Yahoo Finance 暫時無回應、網路或 SMTP 驗證失敗。",
            "錯誤摘要：",
            safe_error[-3000:],
        ]
    )
    html = f"""
<html><body>
<h2>台股每日排程狀態通知 - {report_date}</h2>
<p>今日排程已啟動，但未能完成正式四大類選股報告。</p>
<p>可能原因：休市資料尚未更新、FinMind 免費額度上限、Yahoo Finance 暫時無回應、網路或 SMTP 驗證失敗。</p>
<pre>{safe_error[-3000:]}</pre>
</body></html>
"""
    return subject, plain + "\n\nHTML_TABLE:\n" + html


def format_category_section(title: str, rows: list[dict[str, Any]]) -> tuple[str, str]:
    if not rows:
        empty_df = empty_report_dataframe()
        text = f"{title}\n0 項\n{dataframe_to_markdown(empty_df)}"
        html_table = empty_df.to_html(index=False, border=1, escape=False)
        html = f"<h3>{title}</h3><p>0 項</p>{html_table}"
        return text, html

    df = report_display_dataframe(rows)
    text = f"{title}\n{len(rows)} 項\n{dataframe_to_markdown(df)}"
    html_table = df.to_html(index=False, border=1, escape=False)
    html = f"<h3>{title}</h3><p>{len(rows)} 項</p>{html_table}"
    return text, html


def format_shortlist_section(rows: list[dict[str, Any]]) -> tuple[str, str]:
    title = "今日短線精選排名（不含法人觀察股）"
    if not rows:
        empty_df = pd.DataFrame(
            [{"排名": "今日無符合標的", "股票代號": "", "股名": "", "所屬類別": "", "短線分數": ""}],
            columns=["排名", "股票代號", "股名", "所屬類別", "短線分數"],
        )
        text = f"{title}\n0 項\n{dataframe_to_markdown(empty_df)}"
        html = f"<h3>{title}</h3><p>0 項</p>{empty_df.to_html(index=False, border=1, escape=False)}"
        return text, html
    df = shortlist_dataframe(rows)
    text = f"{title}\n{len(rows)} 項\n{dataframe_to_markdown(df)}"
    html = f"<h3>{title}</h3><p>{len(rows)} 項</p>{df.to_html(index=False, border=1, escape=False)}"
    return text, html


def format_top_reason_section(rows: list[dict[str, Any]]) -> tuple[str, str]:
    title = "今日短線精選 Top 3 操作理由"
    if not rows:
        df = pd.DataFrame(
            [{"排名": "今日無符合標的", "股票": "", "操作理由": ""}],
            columns=["排名", "股票", "操作理由"],
        )
    else:
        df = pd.DataFrame(
            [
                {
                    "排名": i,
                    "股票": f"{row.get('stock_id', '')} {row.get('stock_name', '')}",
                    "操作理由": row.get("top_reason", ""),
                }
                for i, row in enumerate(rows, start=1)
            ],
            columns=["排名", "股票", "操作理由"],
        )
    text = f"{title}\n{dataframe_to_markdown(df)}"
    html = f"<h3>{title}</h3>{df.to_html(index=False, border=1, escape=False)}"
    return text, html


def report_display_dataframe(rows: list[dict[str, Any]]) -> pd.DataFrame:
    clean_rows = []
    for row in rows:
        clean_rows.append(
            {
                "股票代號": str(row.get("stock_id", "")),
                "股名": str(row.get("stock_name", "")),
                "今日收盤價": format_number(row.get("last_close")),
                "今日成交量(張)": format_integer(row.get("volume_lots")),
                "建議停損價(防守線)": format_number(row.get("stop_loss")),
            }
        )
    return pd.DataFrame(
        clean_rows,
        columns=["股票代號", "股名", "今日收盤價", "今日成交量(張)", "建議停損價(防守線)"],
    ).reset_index(drop=True)


def shortlist_dataframe(rows: list[dict[str, Any]]) -> pd.DataFrame:
    clean_rows = []
    for i, row in enumerate(rows, start=1):
        clean_rows.append(
            {
                "排名": i,
                "股票代號": str(row.get("stock_id", "")),
                "股名": str(row.get("stock_name", "")),
                "所屬類別": "、".join(row.get("category_names", [row.get("category", "")])),
                "短線分數": format_integer(row.get("short_score")),
                "今日收盤價": format_number(row.get("last_close")),
                "今日成交量(張)": format_integer(row.get("volume_lots")),
                "建議停損價(防守線)": format_number(row.get("stop_loss")),
            }
        )
    return pd.DataFrame(
        clean_rows,
        columns=[
            "排名",
            "股票代號",
            "股名",
            "所屬類別",
            "短線分數",
            "今日收盤價",
            "今日成交量(張)",
            "建議停損價(防守線)",
        ],
    )


def empty_report_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "股票代號": "今日無符合標的",
                "股名": "",
                "今日收盤價": "",
                "今日成交量(張)": "",
                "建議停損價(防守線)": "",
            }
        ],
        columns=["股票代號", "股名", "今日收盤價", "今日成交量(張)", "建議停損價(防守線)"],
    )


def format_number(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return f"{float(value):.2f}"


def format_integer(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return f"{int(round(float(value)))}"


def dataframe_to_markdown(df: pd.DataFrame) -> str:
    rows = [list(df.columns)] + df.astype(str).values.tolist()
    widths = [max(len(row[i]) for row in rows) for i in range(len(rows[0]))]

    def fmt(row: list[str]) -> str:
        cells = [str(value).ljust(widths[i]) for i, value in enumerate(row)]
        return "| " + " | ".join(cells) + " |"

    separator = "| " + " | ".join("-" * width for width in widths) + " |"
    return "\n".join([fmt(rows[0]), separator, *[fmt(row) for row in rows[1:]]])


def send_line_notify_legacy(message: str, cfg: Config) -> None:
    if not cfg.line_notify_token:
        return
    data = parse.urlencode({"message": message[:950]}).encode("utf-8")
    req = request.Request(
        "https://notify-api.line.me/api/notify",
        headers={"Authorization": f"Bearer {cfg.line_notify_token}"},
        data=data,
        method="POST",
    )
    with request.urlopen(req, timeout=20):
        pass


def send_email(subject: str, body: str, cfg: Config) -> None:
    if not (cfg.smtp_host and cfg.email_from and cfg.email_to):
        return
    plain, _, html = body.partition("\n\nHTML_TABLE:\n")
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = cfg.email_from
    msg["To"] = cfg.email_to
    msg.attach(MIMEText(plain, "plain", "utf-8"))
    if html:
        msg.attach(MIMEText(html, "html", "utf-8"))

    with smtplib.SMTP(cfg.smtp_host, cfg.smtp_port) as server:
        server.starttls()
        if cfg.smtp_user:
            server.login(cfg.smtp_user, cfg.smtp_password)
        server.sendmail(cfg.email_from, [x.strip() for x in cfg.email_to.split(",")], msg.as_string())
    print(f"[email] sent to {cfg.email_to}")


def notify(subject: str, body: str, cfg: Config) -> None:
    with open("last_report.txt", "w", encoding="utf-8") as fh:
        fh.write(subject)
        fh.write("\n\n")
        fh.write(body.split("\n\nHTML_TABLE:\n")[0])

    sent = False
    if cfg.email_from and cfg.email_to and cfg.smtp_host:
        send_email(subject, body, cfg)
        sent = True
    if cfg.line_notify_token:
        send_line_notify_legacy(subject + "\n" + body.split("\n\nHTML_TABLE:\n")[0], cfg)
        sent = True
    if not sent:
        print(subject)
        print(body.split("\n\nHTML_TABLE:\n")[0])


def run(cfg: Config) -> dict[str, list[dict[str, Any]]]:
    universe = get_universe(cfg)
    print(f"Universe size after volume filter: {len(universe)}")
    active_keys = ["precision_entry"] if cfg.only_short_entry else list(CATEGORY_TITLES)
    results: dict[str, list[dict[str, Any]]] = {key: [] for key in active_keys}
    for i, (_, row) in enumerate(universe.iterrows(), start=1):
        print(f"[{i}/{len(universe)}] screening {row['stock_id']} {row['stock_name']}")
        categorized = screen_stock(row, cfg)
        for key, item in categorized.items():
            results[key].append(item)
        if categorized:
            labels = ", ".join(item["category"] for item in categorized.values())
            print(f"  -> matched {row['stock_id']} {row['stock_name']} [{labels}]")
    finalize_results(results)
    return results


def finalize_results(results: dict[str, list[dict[str, Any]]]) -> None:
    for key, rows in results.items():
        if key == "shortlist":
            continue
        for row in rows:
            row["category_keys"] = [key]
            row["category_names"] = [row.get("category", "")]
            score_short_candidate(row)

    for key, rows in results.items():
        if key == "shortlist":
            continue
        if key == "precision_entry":
            rows.sort(
                key=lambda item: (
                    int(item.get("short_score") or 0),
                    int(item.get("short_entry_priority") or 0),
                    float(item.get("turnover") or 0),
                    int(item.get("inst_5d_total_net") or 0),
                ),
                reverse=True,
            )
        else:
            rows.sort(
                key=lambda item: (
                    int(item.get("short_score") or 0),
                    float(item.get("turnover") or 0),
                    int(item.get("inst_5d_total_net") or 0),
                ),
                reverse=True,
            )
        limit = LIMITED_CATEGORY_COUNTS.get(key)
        if limit is not None:
            results[key] = rows[:limit]

    shortlist_pool: dict[str, dict[str, Any]] = {}
    for key in ("strong_continuation", "relay_breakout", "precision_entry"):
        for item in results.get(key, []):
            stock_id = str(item.get("stock_id"))
            existing = shortlist_pool.get(stock_id)
            if existing is None:
                clone = dict(item)
                clone["category_keys"] = [key]
                clone["category_names"] = [item.get("category", "")]
                shortlist_pool[stock_id] = clone
            else:
                existing["category_keys"].append(key)
                existing["category_names"].append(item.get("category", ""))
                if int(item.get("short_score") or 0) > int(existing.get("short_score") or 0):
                    for field in ("short_score", "top_reason", "score_reasons", "score_warnings"):
                        existing[field] = item.get(field)

    shortlist = [score_short_candidate(item) for item in shortlist_pool.values()]
    shortlist.sort(
        key=lambda item: (
            int(item.get("short_score") or 0),
            float(item.get("turnover") or 0),
            int(item.get("inst_5d_total_net") or 0),
        ),
        reverse=True,
    )
    results["shortlist"] = shortlist[: LIMITED_CATEGORY_COUNTS["shortlist"]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Strict Taiwan stock auto screener")
    parser.add_argument("--max-stocks", type=int, default=None, help="Limit stocks for a quick test")
    parser.add_argument("--no-intraday", action="store_true", help="Skip 60-minute K entry check")
    parser.add_argument("--no-big-holder", action="store_true", help="Skip big-holder percentage check")
    parser.add_argument("--no-notify", action="store_true", help="Print result only")
    parser.add_argument("--only-short-entry", action="store_true", help="Run only category 4")
    parser.add_argument("--report-date", default=None, help="Use data up to YYYY-MM-DD for review/backtest")
    return parser.parse_args()


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    args = parse_args()
    load_env_file()
    cfg = Config()
    if args.max_stocks is not None:
        cfg = dataclasses.replace(cfg, max_stocks=args.max_stocks)
    if args.no_intraday:
        cfg = dataclasses.replace(cfg, enable_intraday_check=False)
    if args.no_big_holder:
        cfg = dataclasses.replace(cfg, enable_big_holder_check=False)
    if args.only_short_entry:
        cfg = dataclasses.replace(cfg, only_short_entry=True)
    if args.report_date:
        cfg = dataclasses.replace(cfg, report_date=args.report_date)

    try:
        results = run(cfg)
        subject, body = format_report(results, cfg)
    except Exception:
        subject, body = format_status_report(traceback.format_exc(), cfg)

    if args.no_notify:
        print(subject)
        print(body.split("\n\nHTML_TABLE:\n")[0])
    else:
        notify(subject, body, cfg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

