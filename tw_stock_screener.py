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
import subprocess
import sys
import time
import traceback
import uuid
from zoneinfo import ZoneInfo
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header
from html import escape
from pathlib import Path
from typing import Any
from urllib import error, parse, request

import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup
from a5n_strategy import A5_N_CONFIG, A5_N_VERSION, a5n_rank_key, evaluate_a5n
from a5n_variant_b import A5_N_B_CONFIG, A5_N_B_VERSION
from a5n_fixed_pool import (
    A5_N_FIXED_POOL_CONFIG, A5_N_FIXED_POOL_VERSION,
    evaluate_fixed_pool_candidate, fixed_pool_rank_key,
)


FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"
TRADE_JOURNAL_PATH = Path("trade_journal.csv")
REPORT_DIR = Path("reports")
SENT_MARKER_DIR = Path("sent_reports")
LEDGER_DIR = Path("ledgers")
RUN_LEDGER_PATH = LEDGER_DIR / "run_ledger.jsonl"
RAW_SIGNAL_SNAPSHOT_LEDGER_PATH = LEDGER_DIR / "raw_signal_snapshot_ledger.jsonl"
SIGNAL_EVENT_DETAIL_LEDGER_PATH = LEDGER_DIR / "signal_event_detail_ledger.jsonl"
TAIPEI_TZ = ZoneInfo("Asia/Taipei")

LEDGER_SCHEMA_VERSION = "raw-signal-ledger-v1.0"
STRATEGY_VERSION = "baseline-2026-08-11-current-code"
MASTER_STRATEGY_SPEC_VERSION = "Master v1.0.2"
STRATEGY_KEY_TO_ID = {
    "strong_continuation": "A1",
    "relay_breakout": "A2",
    "prepare_turn": "A3",
    "precision_entry": "A4",
    "extreme_daytrade": "A5",
}
STRATEGY_VERSIONS = {
    "A1": "A1-current-baseline-2026-08-11",
    "A2": "A2-current-baseline-2026-08-11",
    "A3": "A3-60k-radar-current-baseline-2026-08-11",
    "A4": "A4-60k-precision-current-baseline-2026-08-11",
    "A5": A5_N_VERSION,
}
TECH_PARAM_REGISTRY_VERSION = "tech-v1.0-20260811"
TIMEFRAME_DAILY = "daily"
TIMEFRAME_60K = "60k"
TIMEFRAME_5K = "5k"
TIMEFRAME_WEEKLY = "weekly"
TIMEFRAME_MONTHLY = "monthly"

TECH_PARAM_REGISTRY: dict[str, dict[str, Any]] = {
    TIMEFRAME_DAILY: {
        "macd": {"fast": 8, "slow": 17, "signal": 9},
        "kd": {"period": 10, "k_smoothing": 4, "d_smoothing": 4},
        "j_calculated": True,
    },
    TIMEFRAME_60K: {
        "macd": {"fast": 8, "slow": 17, "signal": 9},
        "kd": {"period": 10, "k_smoothing": 4, "d_smoothing": 4},
        "j_calculated": True,
    },
    TIMEFRAME_5K: {
        "macd": {"fast": 12, "slow": 26, "signal": 9},
        "kd": {"period": 9, "k_smoothing": 3, "d_smoothing": 3},
        "j_calculated": True,
    },
    # Legacy helpers still resample daily data into weekly/monthly views.
    TIMEFRAME_WEEKLY: {
        "macd": {"fast": 8, "slow": 17, "signal": 9},
        "kd": {"period": 10, "k_smoothing": 4, "d_smoothing": 4},
        "j_calculated": True,
        "compatibility_alias_of": TIMEFRAME_DAILY,
    },
    TIMEFRAME_MONTHLY: {
        "macd": {"fast": 8, "slow": 17, "signal": 9},
        "kd": {"period": 10, "k_smoothing": 4, "d_smoothing": 4},
        "j_calculated": True,
        "compatibility_alias_of": TIMEFRAME_DAILY,
    },
}
FUTURE_5K_MACD_CANDIDATES = [
    {"fast": 8, "slow": 17, "signal": 9},
    {"fast": 5, "slow": 13, "signal": 8},
    {"fast": 5, "slow": 34, "signal": 5},
]
SWING_STOP_BASELINE_REVIEW_THRESHOLD_PCT = 4.0
SWING_STOP_BASELINE_WARNING_TEXT = "超過個人波段3–4% Baseline，需人工覆核"
SWING_STOP_BASELINE_WARNING_CODE = "SWING_STOP_RISK_GT_PERSONAL_BASELINE_REVIEW_REQUIRED"
A3_OBSERVATION_CATEGORY = "60K起漲雷達【觀察／準備型，非正式進場確認】"
A5_SIGNAL_PRICE_RULE = "completed_5k_signal_bar_close"
A5_N_LEDGER_PATH = LEDGER_DIR / "a5n_signal_ledger.jsonl"
A5_N_POOL_PATH = LEDGER_DIR / "a5n_daily_candidate_pool.json"
A5_N_PREMARKET_LEDGER_PATH = LEDGER_DIR / "a5n_premarket_ledger.jsonl"
A5_N_B_POOL_PATH = LEDGER_DIR / "a5n_b_shadow_candidate_pool.json"
A5_N_B_PREMARKET_LEDGER_PATH = LEDGER_DIR / "a5n_b_shadow_premarket_ledger.jsonl"
A5_N_B_SIGNAL_LEDGER_PATH = LEDGER_DIR / "a5n_b_shadow_signal_ledger.jsonl"
A5_N_FIXED_POOL_PATH = LEDGER_DIR / "a5n_weekly_fixed_pool.json"
A5_N_FIXED_POOL_LEDGER_PATH = LEDGER_DIR / "a5n_weekly_fixed_pool_ledger.jsonl"
A5_N_FIXED_SHADOW_POOL_PATH = LEDGER_DIR / "a5n_weekly_fixed_pool_momentum_rank_shadow.json"
A5_N_FIXED_SHADOW_POOL_LEDGER_PATH = LEDGER_DIR / "a5n_weekly_fixed_pool_momentum_rank_shadow_ledger.jsonl"
A5_N_FIXED_SHADOW_SIGNAL_LEDGER_PATH = LEDGER_DIR / "a5n_fixed_pool_momentum_rank_shadow_signal_ledger.jsonl"
A5_N_NOTIFICATION_SLOT_PATH = LEDGER_DIR / "a5n_notification_slots.json"
A5_N_RUN_ROWS: list[dict[str, Any]] = []


def a5n_notification_slot(now: dt.datetime | None = None) -> str:
    configured = os.getenv("A5N_NOTIFICATION_SLOT", "").strip()
    if configured in {"09:16", "09:26", "09:31"}:
        return configured
    current = now or now_taipei()
    minute_of_day = current.hour * 60 + current.minute
    if minute_of_day <= 9 * 60 + 21:
        return "09:16"
    if minute_of_day <= 9 * 60 + 30:
        return "09:26"
    return "09:31"


def a5n_slot_already_sent(slot: str, now: dt.datetime | None = None) -> bool:
    current = now or now_taipei()
    if not A5_N_NOTIFICATION_SLOT_PATH.exists():
        return False
    try:
        payload = json.loads(A5_N_NOTIFICATION_SLOT_PATH.read_text(encoding="utf-8"))
        return bool(payload.get(str(current.date()), {}).get(slot))
    except (json.JSONDecodeError, OSError):
        return False


def mark_a5n_slot_sent(slot: str, now: dt.datetime | None = None) -> None:
    current = now or now_taipei()
    payload: dict[str, Any] = {}
    if A5_N_NOTIFICATION_SLOT_PATH.exists():
        try:
            payload = json.loads(A5_N_NOTIFICATION_SLOT_PATH.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            payload = {}
    payload = {day: slots for day, slots in payload.items()
               if pd.Timestamp(day).date() >= current.date() - dt.timedelta(days=14)}
    payload.setdefault(str(current.date()), {})[slot] = current.isoformat()
    LEDGER_DIR.mkdir(parents=True, exist_ok=True)
    A5_N_NOTIFICATION_SLOT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def get_technical_params(timeframe: str) -> dict[str, Any]:
    key = str(timeframe).strip().lower()
    if key not in TECH_PARAM_REGISTRY:
        raise ValueError(f"Unknown indicator timeframe: {timeframe!r}")
    return TECH_PARAM_REGISTRY[key]


def technical_parameter_snapshot() -> dict[str, Any]:
    return {
        "registry_version": TECH_PARAM_REGISTRY_VERSION,
        "active": TECH_PARAM_REGISTRY,
        "future_candidates": {"5k_macd": FUTURE_5K_MACD_CANDIDATES},
        "source": "central Technical Parameter Registry",
    }


TECHNICAL_PARAMETER_SNAPSHOT = technical_parameter_snapshot()


def ledger_parameter_fields() -> dict[str, Any]:
    return {
        "parameter_registry_version": TECH_PARAM_REGISTRY_VERSION,
        "daily_macd_params": TECH_PARAM_REGISTRY[TIMEFRAME_DAILY]["macd"],
        "minute60_macd_params": TECH_PARAM_REGISTRY[TIMEFRAME_60K]["macd"],
        "five_k_macd_params": TECH_PARAM_REGISTRY[TIMEFRAME_5K]["macd"],
        "daily_kd_params": TECH_PARAM_REGISTRY[TIMEFRAME_DAILY]["kd"],
        "minute60_kd_params": TECH_PARAM_REGISTRY[TIMEFRAME_60K]["kd"],
        "five_k_kd_params": TECH_PARAM_REGISTRY[TIMEFRAME_5K]["kd"],
        "j_calculated": all(bool(v.get("j_calculated")) for v in TECH_PARAM_REGISTRY.values()),
    }


LEDGER_PARAMETER_FIELDS = ledger_parameter_fields()
LEDGER_CONTEXT: dict[str, Any] = {}


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


def now_taipei() -> dt.datetime:
    return dt.datetime.now(TAIPEI_TZ)


def json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        if pd.isna(value):
            return None
        return value
    if isinstance(value, (dt.datetime, dt.date)):
        if isinstance(value, dt.datetime) and value.tzinfo is not None:
            return value.astimezone(TAIPEI_TZ).isoformat()
        return value.isoformat()
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        if value.tzinfo is not None:
            return value.tz_convert(TAIPEI_TZ).isoformat()
        return value.isoformat()
    if isinstance(value, pd.Series):
        return {str(k): json_safe(v) for k, v in value.to_dict().items()}
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(v) for v in value]
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if hasattr(value, "item"):
        try:
            return json_safe(value.item())
        except Exception:
            pass
    return str(value)


def append_jsonl_fail_open(path: Path, record: dict[str, Any], ledger_name: str) -> bool:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(json_safe(record), ensure_ascii=False, separators=(",", ":")))
            fh.write("\n")
        return True
    except Exception as exc:
        print(f"[ledger-warn] {ledger_name} write failed: {exc}", file=sys.stderr)
        return False


def current_code_commit_sha() -> str:
    github_sha = os.getenv("GITHUB_SHA", "").strip()
    if github_sha:
        return github_sha
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.stdout.strip()
    except Exception:
        return "unknown"


def build_execution_context(cfg: "Config", run_mode: str, market: dict[str, Any] | None = None) -> dict[str, Any]:
    started = now_taipei()
    github_run_id = os.getenv("GITHUB_RUN_ID", "").strip()
    github_attempt = os.getenv("GITHUB_RUN_ATTEMPT", "").strip()
    if github_run_id:
        execution_id = f"github-{github_run_id}-{github_attempt or '1'}"
    else:
        execution_id = f"local-{started.strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"

    if cfg.intraday_alert_only:
        scheduled_slot_id = f"{cfg_date(cfg)}-intraday-{started.strftime('%H%M')}"
    elif cfg.only_short_entry:
        scheduled_slot_id = f"{cfg_date(cfg)}-only-a4"
    elif cfg.only_prepare_turn:
        scheduled_slot_id = f"{cfg_date(cfg)}-only-a3"
    else:
        scheduled_slot_id = f"{cfg_date(cfg)}-after-close"

    market = market or {}
    return {
        "schema_version": LEDGER_SCHEMA_VERSION,
        "execution_id": execution_id,
        "scheduled_slot_id": scheduled_slot_id,
        "run_date": cfg_date(cfg),
        "run_mode": run_mode,
        "started_at": started.isoformat(),
        "report_date": cfg_date(cfg),
        "code_commit_sha": current_code_commit_sha(),
        "strategy_version": STRATEGY_VERSION,
        "strategy_versions": dict(STRATEGY_VERSIONS),
        "technical_parameters": dict(TECHNICAL_PARAMETER_SNAPSHOT),
        **LEDGER_PARAMETER_FIELDS,
        "source_vendor": "FinMind+yfinance",
        "timezone": "Asia/Taipei",
        "trigger_source": os.getenv("GITHUB_EVENT_NAME", "local"),
        "github_workflow": os.getenv("GITHUB_WORKFLOW", ""),
        "github_run_id": github_run_id,
        "market_regime": {
            "label": "allow_intraday" if market.get("ok") else ("blocked_intraday" if market else "unknown"),
            "raw_features": {
                "symbol": market.get("symbol"),
                "daily_pct": market.get("daily_pct"),
                "intraday_pct": market.get("intraday_pct"),
                "reason": market.get("reason"),
                "ok": market.get("ok"),
            },
        },
        "raw_snapshots": [],
        "event_details": [],
        "mother_universe_seen": set(),
        "screened_snapshot_seen": set(),
        "ledger_stats": {
            "snapshots_collected": 0,
            "event_details_collected": 0,
            "snapshots_written": 0,
            "event_details_written": 0,
        },
    }


def reset_ledger_context(cfg: "Config", run_mode: str, market: dict[str, Any] | None = None) -> None:
    LEDGER_CONTEXT.clear()
    LEDGER_CONTEXT.update(build_execution_context(cfg, run_mode, market))


def latest_feature_snapshot(df: pd.DataFrame, timeframe: str) -> dict[str, Any]:
    if df is None or df.empty:
        return {
            "timeframe": timeframe,
            "data_quality_flags": ["EMPTY_DATAFRAME"],
        }
    enriched = add_indicators(df, timeframe=timeframe)
    latest = enriched.iloc[-1]
    index_value = enriched.index[-1] if len(enriched.index) else None
    return {
        "timeframe": timeframe,
        "timestamp": json_safe(index_value),
        "close": latest.get("close"),
        "volume": latest.get("volume"),
        "ma5": latest.get("ma5"),
        "ma10": latest.get("ma10"),
        "ma20": latest.get("ma20"),
        "ma60": latest.get("ma60"),
        "dif": latest.get("dif"),
        "macd_signal": latest.get("macd"),
        "histogram": latest.get("hist"),
        "k": latest.get("kd_k"),
        "d": latest.get("kd_d"),
        "j": latest.get("kd_j"),
    }


def bool_state(value: Any) -> bool:
    return bool(value) if value is not None else False


def pass_reject_codes(states: dict[str, dict[str, Any]], filter_reasons: dict[str, Any]) -> dict[str, list[str]]:
    codes: dict[str, list[str]] = {}
    for strategy_id, state in states.items():
        strategy_codes: list[str] = []
        if state.get("raw_signal"):
            strategy_codes.append(f"{strategy_id}_RAW_SIGNAL")
        else:
            strategy_codes.append(f"{strategy_id}_NO_RAW_SIGNAL")
        if state.get("filter_passed"):
            strategy_codes.append(f"{strategy_id}_FILTER_PASSED")
        else:
            strategy_codes.append(f"{strategy_id}_FILTER_FAILED")
        if state.get("eligible"):
            strategy_codes.append(f"{strategy_id}_ELIGIBLE")
        else:
            strategy_codes.append(f"{strategy_id}_NOT_ELIGIBLE")
        codes[strategy_id] = strategy_codes
    if filter_reasons:
        codes["FILTER_DETAIL"] = [str(item) for item in filter_reasons.get("common", [])]
    return codes


def float_or_none(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def stop_risk_exceeds_swing_baseline(value: Any) -> bool:
    risk = float_or_none(value)
    return risk is not None and risk > SWING_STOP_BASELINE_REVIEW_THRESHOLD_PCT


def append_swing_stop_warning_codes(
    codes: dict[str, list[str]],
    states: dict[str, dict[str, Any]],
    stop: dict[str, Any] | None,
    relay_stop: dict[str, Any] | None,
) -> dict[str, list[str]]:
    """Mark swing candidates whose risk exceeds the user's manual baseline without filtering them."""
    strategy_stop_risks = {
        "A1": (stop or {}).get("stop_loss_risk_pct"),
        "A2": (relay_stop or {}).get("stop_loss_risk_pct"),
        "A3": (stop or {}).get("stop_loss_risk_pct"),
        "A4": (stop or {}).get("stop_loss_risk_pct"),
    }
    warning_details: list[str] = []
    for strategy_id, risk in strategy_stop_risks.items():
        if not states.get(strategy_id, {}).get("eligible"):
            continue
        if not stop_risk_exceeds_swing_baseline(risk):
            continue
        codes.setdefault(strategy_id, []).append(SWING_STOP_BASELINE_WARNING_CODE)
        warning_details.append(f"{strategy_id}:{SWING_STOP_BASELINE_WARNING_CODE}")
    if warning_details:
        codes.setdefault("WARNING_DETAIL", []).extend(warning_details)
    return codes


def attach_swing_stop_baseline_warning(item: dict[str, Any]) -> dict[str, Any]:
    if str(item.get("category", "")) == "5K早盤當沖雷達股":
        return item
    if not stop_risk_exceeds_swing_baseline(item.get("stop_loss_risk_pct")):
        return item
    warnings = list(item.get("manual_review_warnings") or [])
    if SWING_STOP_BASELINE_WARNING_TEXT not in warnings:
        warnings.append(SWING_STOP_BASELINE_WARNING_TEXT)
    item["manual_review_warnings"] = warnings
    warning_codes = list(item.get("warning_codes") or [])
    if SWING_STOP_BASELINE_WARNING_CODE not in warning_codes:
        warning_codes.append(SWING_STOP_BASELINE_WARNING_CODE)
    item["warning_codes"] = warning_codes
    return item


def collect_mother_universe_snapshots(info: pd.DataFrame) -> None:
    """Record the pre-volume-filter market mother universe for research completeness."""
    try:
        if not LEDGER_CONTEXT:
            return
        LEDGER_CONTEXT["mother_universe_count"] = int(len(info))
        seen = LEDGER_CONTEXT.setdefault("mother_universe_seen", set())
        base_states: dict[str, dict[str, Any]] = {}
        for strategy_id, strategy_key in {
            "A1": "strong_continuation",
            "A2": "relay_breakout",
            "A3": "prepare_turn",
            "A4": "precision_entry",
            "A5": "extreme_daytrade",
        }.items():
            base_states[strategy_id] = {
                "strategy_key": strategy_key,
                "raw_signal": False,
                "filter_passed": None,
                "eligible": False,
                "ranked": False,
                "selected": False,
                "category_selected": False,
                "category_excluded_by_top_n": False,
                "shortlist_selected": False,
                "deduped_out": False,
                "selection_status": "not_evaluated",
                "rank_before_limit": None,
                "rank_after_limit": None,
                "strategy_version": STRATEGY_VERSIONS[strategy_id],
            }
        for _, row in info.iterrows():
            stock_id = str(row.get("stock_id", ""))
            dedupe_key = ("mother_universe", stock_id)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            record = {
                "ledger_type": "raw_signal_snapshot",
                "schema_version": LEDGER_SCHEMA_VERSION,
                "execution_id": LEDGER_CONTEXT.get("execution_id"),
                "scheduled_slot_id": LEDGER_CONTEXT.get("scheduled_slot_id"),
                "run_date": LEDGER_CONTEXT.get("run_date"),
                "signal_timestamp": now_taipei().isoformat(),
                "snapshot_stage": "mother_universe",
                "stock_id": stock_id,
                "stock_name": row.get("stock_name"),
                "market_type": row.get("type"),
                "industry_category": row.get("industry_category"),
                "strategy_version": STRATEGY_VERSION,
                "strategy_versions": STRATEGY_VERSIONS,
                "technical_parameters": TECHNICAL_PARAMETER_SNAPSHOT,
                **LEDGER_PARAMETER_FIELDS,
                "strategy_states": base_states,
                "raw_signal": False,
                "passed_common_filter": None,
                "filter_passed": None,
                "eligible": False,
                "ranked": False,
                "selected": False,
                "category_selected": False,
                "category_excluded_by_top_n": False,
                "shortlist_selected": False,
                "deduped_out": False,
                "selection_status": "not_evaluated",
                "rank_before_limit": {},
                "rank_after_limit": {},
                "entry_price_rule": None,
                "stop_price": None,
                "data_until": None,
                "last_raw_k_timestamp": None,
                "last_completed_k_timestamp": None,
                "kbar_completed_time": None,
                "forming_k_excluded": None,
                "source_vendor": "FinMind",
                "timezone": "Asia/Taipei",
                "timeframe": "stock_info",
                "data_quality_flags": ["MOTHER_UNIVERSE_PRE_VOLUME_FILTER"],
                "market_regime": LEDGER_CONTEXT.get("market_regime"),
                "current_close": None,
                "signal_price": None,
                "volume": None,
                "ma5": None,
                "ma10": None,
                "ma20": None,
                "ma60": None,
                "dif": None,
                "macd_signal": None,
                "histogram": None,
                "k": None,
                "d": None,
                "j": None,
                "strategy_score": None,
                "pass_reject_codes": {
                    strategy_id: [f"{strategy_id}_NOT_EVALUATED_AT_MOTHER_UNIVERSE_STAGE"]
                    for strategy_id in base_states
                },
                "reject_reason": {"mother_universe": ["NOT_YET_VOLUME_PREFILTERED_OR_STRATEGY_SCREENED"]},
                "pass_reason": [],
                "code_commit_sha": LEDGER_CONTEXT.get("code_commit_sha"),
                "shortlist_rank": None,
            }
            collect_raw_signal_snapshot(json_safe(record))
    except Exception as exc:
        print(f"[ledger-warn] mother universe snapshot collect failed: {exc}", file=sys.stderr)


def collect_raw_signal_snapshot(record: dict[str, Any]) -> None:
    try:
        if not LEDGER_CONTEXT:
            return
        LEDGER_CONTEXT.setdefault("raw_snapshots", []).append(record)
        LEDGER_CONTEXT.setdefault("ledger_stats", {})["snapshots_collected"] = len(
            LEDGER_CONTEXT.get("raw_snapshots", [])
        )
    except Exception as exc:
        print(f"[ledger-warn] snapshot collect failed: {exc}", file=sys.stderr)


def collect_signal_event_detail(record: dict[str, Any]) -> None:
    try:
        if not LEDGER_CONTEXT:
            return
        LEDGER_CONTEXT.setdefault("event_details", []).append(record)
        LEDGER_CONTEXT.setdefault("ledger_stats", {})["event_details_collected"] = len(
            LEDGER_CONTEXT.get("event_details", [])
        )
    except Exception as exc:
        print(f"[ledger-warn] event detail collect failed: {exc}", file=sys.stderr)


def strategy_state_map(
    *,
    filter_ok: bool,
    relay_filter_ok: bool,
    precision_filter_ok: bool,
    reclaim_ok: bool,
    support_ok: bool,
    kd_pullback_ok: bool,
    daily_macd_ok: bool,
    breakout_ok: bool,
    daily_prepare_ok: bool,
    prepare_turn_ok: bool,
    short_entry_ok: bool,
    daily_daytrade_ok: bool,
    daytrade_direction_ok: bool,
    five_k_ok: bool,
    intraday_volume_ok: bool,
    extreme_daytrade_ok: bool,
) -> dict[str, dict[str, Any]]:
    states = {
        "A1": {
            "strategy_key": "strong_continuation",
            "raw_signal": bool_state(reclaim_ok or (support_ok and kd_pullback_ok)),
            "filter_passed": bool_state(filter_ok),
        },
        "A2": {
            "strategy_key": "relay_breakout",
            "raw_signal": bool_state(daily_macd_ok and breakout_ok),
            "filter_passed": bool_state(relay_filter_ok),
        },
        "A3": {
            "strategy_key": "prepare_turn",
            "raw_signal": bool_state(daily_prepare_ok and prepare_turn_ok),
            "filter_passed": bool_state(filter_ok),
        },
        "A4": {
            "strategy_key": "precision_entry",
            "raw_signal": bool_state(daily_macd_ok and short_entry_ok),
            "filter_passed": bool_state(precision_filter_ok),
        },
        "A5": {
            "strategy_key": "extreme_daytrade",
            "raw_signal": bool_state(daily_daytrade_ok and daytrade_direction_ok and five_k_ok),
            "filter_passed": bool_state(intraday_volume_ok),
            "forming_k_excluded": True,
        },
    }
    for strategy_id, state in states.items():
        if strategy_id == "A5":
            state["eligible"] = bool_state(extreme_daytrade_ok)
        else:
            state["eligible"] = bool_state(state["raw_signal"] and state["filter_passed"])
        state["ranked"] = False
        state["selected"] = False
        state["category_selected"] = False
        state["category_excluded_by_top_n"] = False
        state["shortlist_selected"] = False
        state["deduped_out"] = False
        state["selection_status"] = "eligible" if state["eligible"] else "not_eligible"
        state["rank_before_limit"] = None
        state["rank_after_limit"] = None
        state["strategy_version"] = STRATEGY_VERSIONS[strategy_id]
    return states


def infer_forming_k_excluded(five_k_info: dict[str, Any]) -> bool | None:
    raw_last = five_k_info.get("5k_raw_last_timestamp")
    signal_ts = five_k_info.get("5k_signal_timestamp")
    cutoff = five_k_info.get("5k_completed_cutoff")
    if raw_last and signal_ts:
        return str(raw_last) != str(signal_ts)
    if cutoff and signal_ts:
        return True
    return None


def collect_stock_ledgers(
    *,
    row: pd.Series,
    cfg: "Config",
    daily: pd.DataFrame,
    intraday: pd.DataFrame | None,
    states: dict[str, dict[str, Any]],
    stop: dict[str, Any] | None,
    relay_stop: dict[str, Any] | None,
    filter_reasons: list[str],
    relay_filter_reasons: list[str],
    precision_filter_reasons: list[str],
    five_k_info: dict[str, Any],
    prepare_turn_info: dict[str, Any],
    short_entry_reason: str,
    prepare_turn_reason: str,
    extreme_daytrade_info: dict[str, Any],
    data_quality_flags: list[str] | None = None,
) -> None:
    try:
        if not LEDGER_CONTEXT:
            return
        stock_id = str(row.get("stock_id", ""))
        seen = LEDGER_CONTEXT.setdefault("screened_snapshot_seen", set())
        dedupe_key = ("screened", stock_id)
        if dedupe_key in seen:
            return
        seen.add(dedupe_key)
        daily_features = latest_feature_snapshot(daily, "daily")
        intraday_features = latest_feature_snapshot(intraday, "60k") if intraday is not None and not intraday.empty else {}
        forming_k_excluded = infer_forming_k_excluded(five_k_info)
        quality_flags = list(data_quality_flags or [])
        if forming_k_excluded is None:
            quality_flags.append("FIVE_K_COMPLETION_STATUS_UNKNOWN")
        elif forming_k_excluded:
            quality_flags.append("FORMING_5K_EXCLUDED")
        else:
            quality_flags.append("LATEST_5K_WAS_COMPLETED_SIGNAL_BAR")
        if intraday is None or intraday.empty:
            quality_flags.append("NO_60K_DATA")
        codes = pass_reject_codes(
            states,
            {
                "common": filter_reasons,
                "relay": relay_filter_reasons,
                "precision": precision_filter_reasons,
            },
        )
        append_swing_stop_warning_codes(codes, states, stop, relay_stop)
        warning_codes = sorted(
            {
                code
                for per_strategy_codes in codes.values()
                for code in per_strategy_codes
                if code == SWING_STOP_BASELINE_WARNING_CODE
            }
        )

        snapshot = {
            "ledger_type": "raw_signal_snapshot",
            "schema_version": LEDGER_SCHEMA_VERSION,
            "execution_id": LEDGER_CONTEXT.get("execution_id"),
            "scheduled_slot_id": LEDGER_CONTEXT.get("scheduled_slot_id"),
            "run_date": cfg_date(cfg),
            "signal_timestamp": now_taipei().isoformat(),
            "snapshot_stage": "screened",
            "stock_id": stock_id,
            "stock_name": str(row.get("stock_name", "")),
            "market_type": str(row.get("type", "")),
            "industry_category": str(row.get("industry_category", "") or ""),
            "strategy_version": STRATEGY_VERSION,
            "strategy_versions": dict(STRATEGY_VERSIONS),
            "technical_parameters": dict(TECHNICAL_PARAMETER_SNAPSHOT),
            **LEDGER_PARAMETER_FIELDS,
            "strategy_states": states,
            "raw_signal": any(bool(state.get("raw_signal")) for state in states.values()),
            "passed_common_filter": bool(states["A1"].get("filter_passed")),
            "filter_passed": any(bool(state.get("filter_passed")) for state in states.values()),
            "eligible": any(bool(state.get("eligible")) for state in states.values()),
            "ranked": False,
            "selected": False,
            "category_selected": False,
            "category_excluded_by_top_n": False,
            "shortlist_selected": False,
            "deduped_out": False,
            "selection_status": "not_selected_yet",
            "rank_before_limit": {},
            "rank_after_limit": {},
            "entry_price_rule": "latest_daily_close",
            "stop_price": (stop or {}).get("stop_loss"),
            "relay_stop_price": (relay_stop or {}).get("stop_loss"),
            "data_until": cfg_date(cfg),
            "last_raw_k_timestamp": five_k_info.get("5k_raw_last_timestamp") or intraday_features.get("timestamp"),
            "last_completed_k_timestamp": five_k_info.get("5k_signal_timestamp") or intraday_features.get("timestamp"),
            "kbar_completed_time": five_k_info.get("5k_signal_timestamp"),
            "forming_k_excluded": forming_k_excluded,
            "source_vendor": "yfinance+FinMind",
            "timezone": "Asia/Taipei",
            "timeframe": "daily/60k/5k",
            "data_quality_flags": quality_flags,
            "market_regime": LEDGER_CONTEXT.get("market_regime"),
            "daily": daily_features,
            "minute60": intraday_features,
            "five_k": {
                "raw_last_timestamp": five_k_info.get("5k_raw_last_timestamp"),
                "completed_cutoff": five_k_info.get("5k_completed_cutoff"),
                "signal_timestamp": five_k_info.get("5k_signal_timestamp"),
                "previous_timestamp": five_k_info.get("5k_previous_timestamp"),
                "above_ema5": five_k_info.get("5k_above_ema5"),
                "golden_cross": five_k_info.get("5k_golden_cross"),
                "hist_contracting_2": five_k_info.get("5k_hist_contracting_2"),
                "hist_turn_red": five_k_info.get("5k_hist_turn_red"),
                "dif_turning_up": five_k_info.get("5k_dif_turning_up"),
                "signal_open": five_k_info.get("5k_signal_open"),
                "signal_high": five_k_info.get("5k_signal_high"),
                "signal_low": five_k_info.get("5k_signal_low"),
                "signal_close": five_k_info.get("5k_signal_close"),
                "signal_ema5": five_k_info.get("5k_signal_ema5"),
                "signal_dif": five_k_info.get("5k_signal_dif"),
                "signal_macd": five_k_info.get("5k_signal_macd"),
                "signal_histogram": five_k_info.get("5k_signal_histogram"),
                "previous_close": five_k_info.get("5k_previous_close"),
                "previous_dif": five_k_info.get("5k_previous_dif"),
                "previous_macd": five_k_info.get("5k_previous_macd"),
                "previous_histogram": five_k_info.get("5k_previous_histogram"),
                "stop_price": five_k_info.get("5k_stop_price"),
                "stop_risk_pct": five_k_info.get("5k_stop_risk_pct"),
                "stop_risk_basis": five_k_info.get("5k_stop_risk_basis"),
            },
            "current_close": daily_features.get("close"),
            "signal_price": daily_features.get("close"),
            "volume": row.get("Trading_Volume"),
            "ma5": daily_features.get("ma5"),
            "ma10": daily_features.get("ma10"),
            "ma20": daily_features.get("ma20"),
            "ma60": daily_features.get("ma60"),
            "dif": daily_features.get("dif"),
            "macd_signal": daily_features.get("macd_signal"),
            "histogram": daily_features.get("histogram"),
            "k": daily_features.get("k"),
            "d": daily_features.get("d"),
            "j": daily_features.get("j"),
            "strategy_score": None,
            "pass_reject_codes": codes,
            "warning_codes": warning_codes,
            "reject_reason": {
                "common": filter_reasons,
                "relay": relay_filter_reasons,
                "precision": precision_filter_reasons,
            },
            "pass_reason": [],
            "code_commit_sha": LEDGER_CONTEXT.get("code_commit_sha"),
        }
        collect_raw_signal_snapshot(snapshot)

        for strategy_id, state in states.items():
            if not (state.get("raw_signal") or state.get("eligible")):
                continue
            if strategy_id == "A5":
                a5_signal_price = five_k_info.get("5k_signal_close") or snapshot["signal_price"]
                a5_stop_price = (
                    five_k_info.get("5k_stop_price")
                    or five_k_info.get("open_low_stop")
                    or snapshot["stop_price"]
                )
                a5_stop_risk = (
                    five_k_info.get("5k_stop_risk_pct")
                    or five_k_info.get("open_low_stop_risk_pct")
                )
                strategy_overrides = {
                    "entry_price_rule": A5_SIGNAL_PRICE_RULE,
                    "stop_price": a5_stop_price,
                    "stop_loss_risk_pct": a5_stop_risk,
                    "current_close": a5_signal_price,
                    "signal_price": a5_signal_price,
                    "last_completed_k_timestamp": five_k_info.get("5k_signal_timestamp"),
                    "kbar_completed_time": five_k_info.get("5k_signal_timestamp"),
                    "timeframe": "5k",
                }
            else:
                strategy_overrides = {}
            detail = {
                **snapshot,
                **strategy_overrides,
                "ledger_type": "signal_event_detail",
                "strategy_id": strategy_id,
                "strategy_key": state.get("strategy_key"),
                "strategy_version": state.get("strategy_version"),
                "raw_signal": state.get("raw_signal"),
                "filter_passed": state.get("filter_passed"),
                "eligible": state.get("eligible"),
                "ranked": False,
                "selected": False,
                "category_selected": False,
                "category_excluded_by_top_n": False,
                "shortlist_selected": False,
                "deduped_out": False,
                "selection_status": "not_selected_yet",
                "rank_before_limit": None,
                "rank_after_limit": None,
                "event_detail": {
                    "short_entry_reason": short_entry_reason,
                    "prepare_turn_reason": prepare_turn_reason,
                    "prepare_turn_info": prepare_turn_info,
                    "extreme_daytrade_info": extreme_daytrade_info,
                    "a5_signal_price_consistency": {
                        "raw_5k_timestamp": five_k_info.get("5k_raw_last_timestamp"),
                        "completed_cutoff": five_k_info.get("5k_completed_cutoff"),
                        "completed_signal_timestamp": five_k_info.get("5k_signal_timestamp"),
                        "signal_bar_close": five_k_info.get("5k_signal_close"),
                        "stop_price": five_k_info.get("5k_stop_price")
                        or five_k_info.get("open_low_stop"),
                        "stop_risk_pct": five_k_info.get("5k_stop_risk_pct")
                        or five_k_info.get("open_low_stop_risk_pct"),
                        "stop_risk_basis": five_k_info.get("5k_stop_risk_basis"),
                    }
                    if strategy_id == "A5"
                    else None,
                },
            }
            collect_signal_event_detail(detail)
    except Exception as exc:
        print(f"[ledger-warn] stock ledger collect failed: {exc}", file=sys.stderr)


def collect_stock_failure_snapshot(
    row: pd.Series,
    cfg: "Config",
    *,
    stage: str,
    error_text: str,
) -> None:
    try:
        if not LEDGER_CONTEXT:
            return
        states = strategy_state_map(
            filter_ok=False,
            relay_filter_ok=False,
            precision_filter_ok=False,
            reclaim_ok=False,
            support_ok=False,
            kd_pullback_ok=False,
            daily_macd_ok=False,
            breakout_ok=False,
            daily_prepare_ok=False,
            prepare_turn_ok=False,
            short_entry_ok=False,
            daily_daytrade_ok=False,
            daytrade_direction_ok=False,
            five_k_ok=False,
            intraday_volume_ok=False,
            extreme_daytrade_ok=False,
        )
        collect_stock_ledgers(
            row=row,
            cfg=cfg,
            daily=pd.DataFrame(),
            intraday=None,
            states=states,
            stop=None,
            relay_stop=None,
            filter_reasons=[stage, "DATA_UNAVAILABLE"],
            relay_filter_reasons=[stage, "DATA_UNAVAILABLE"],
            precision_filter_reasons=[stage, "DATA_UNAVAILABLE"],
            five_k_info={},
            prepare_turn_info={},
            short_entry_reason="",
            prepare_turn_reason="",
            extreme_daytrade_info={},
            data_quality_flags=[stage, "STOCK_SCREEN_EXCEPTION", f"ERROR:{error_text[:180]}"],
        )
    except Exception as exc:
        print(f"[ledger-warn] failure snapshot collect failed: {exc}", file=sys.stderr)


def annotate_ledger_prelimit_rank(strategy_key: str, rows: list[dict[str, Any]]) -> None:
    try:
        strategy_id = STRATEGY_KEY_TO_ID.get(strategy_key)
        if not strategy_id or not LEDGER_CONTEXT:
            return
        ranks = {str(item.get("stock_id")): rank for rank, item in enumerate(rows, start=1)}
        for snapshot in LEDGER_CONTEXT.get("raw_snapshots", []):
            stock_id = str(snapshot.get("stock_id"))
            if stock_id in ranks:
                snapshot.setdefault("rank_before_limit", {})[strategy_id] = ranks[stock_id]
                snapshot["ranked"] = True
                if strategy_id in snapshot.get("strategy_states", {}):
                    snapshot["strategy_states"][strategy_id]["ranked"] = True
                    snapshot["strategy_states"][strategy_id]["rank_before_limit"] = ranks[stock_id]
        for detail in LEDGER_CONTEXT.get("event_details", []):
            if detail.get("strategy_id") == strategy_id and str(detail.get("stock_id")) in ranks:
                detail["rank_before_limit"] = ranks[str(detail.get("stock_id"))]
                detail["ranked"] = True
    except Exception as exc:
        print(f"[ledger-warn] prelimit rank annotate failed: {exc}", file=sys.stderr)


def flush_signal_ledgers(results: dict[str, list[dict[str, Any]]]) -> None:
    try:
        if not LEDGER_CONTEXT or LEDGER_CONTEXT.get("signal_ledgers_flushed"):
            return
        selected: dict[tuple[str, str], int] = {}
        shortlist_ranks: dict[str, int] = {}
        shortlist_strategy_ids: dict[str, list[str]] = {}
        shortlist_primary_strategy: dict[str, str] = {}
        for strategy_key, rows in results.items():
            if strategy_key == "shortlist":
                continue
            strategy_id = STRATEGY_KEY_TO_ID.get(strategy_key)
            if not strategy_id:
                continue
            for rank, item in enumerate(rows, start=1):
                selected[(strategy_id, str(item.get("stock_id")))] = rank
        for rank, item in enumerate(results.get("shortlist", []), start=1):
            stock_id = str(item.get("stock_id"))
            shortlist_ranks[stock_id] = rank
            strategy_ids = [
                STRATEGY_KEY_TO_ID[key]
                for key in item.get("category_keys", [])
                if key in STRATEGY_KEY_TO_ID
            ]
            shortlist_strategy_ids[stock_id] = strategy_ids
            if strategy_ids:
                shortlist_primary_strategy[stock_id] = strategy_ids[0]

        snapshots = LEDGER_CONTEXT.get("raw_snapshots", [])
        event_details = LEDGER_CONTEXT.get("event_details", [])
        for snapshot in snapshots:
            if snapshot.get("snapshot_stage") != "screened":
                append_jsonl_fail_open(RAW_SIGNAL_SNAPSHOT_LEDGER_PATH, snapshot, "raw_signal_snapshot_ledger")
                continue
            stock_id = str(snapshot.get("stock_id"))
            rank_after: dict[str, int] = {}
            deduped_any = False
            top_n_excluded_any = False
            shortlist_selected_any = False
            for strategy_id in STRATEGY_VERSIONS:
                selected_rank = selected.get((strategy_id, stock_id))
                state = snapshot.get("strategy_states", {}).get(strategy_id)
                rank_before = None
                if state:
                    rank_before = state.get("rank_before_limit")
                if selected_rank is not None:
                    rank_after[strategy_id] = selected_rank
                    if state:
                        state["selected"] = True
                        state["category_selected"] = True
                        state["rank_after_limit"] = selected_rank
                        state["selection_status"] = "category_selected"
                elif rank_before is not None:
                    top_n_excluded_any = True
                    if state:
                        state["category_excluded_by_top_n"] = True
                        state["selection_status"] = "category_excluded_by_top_n"

                if stock_id in shortlist_ranks and strategy_id in shortlist_strategy_ids.get(stock_id, []):
                    if shortlist_primary_strategy.get(stock_id) == strategy_id:
                        shortlist_selected_any = True
                        if state:
                            state["shortlist_selected"] = True
                            state["selection_status"] = "shortlist_selected"
                    else:
                        deduped_any = True
                        if state:
                            state["deduped_out"] = True
                            state["selection_status"] = "deduped_out_by_shortlist_stock_dedupe"
            snapshot["rank_after_limit"] = rank_after
            snapshot["selected"] = bool(rank_after)
            snapshot["shortlist_rank"] = shortlist_ranks.get(stock_id)
            snapshot["shortlist_selected"] = shortlist_selected_any
            snapshot["category_excluded_by_top_n"] = top_n_excluded_any
            snapshot["deduped_out"] = deduped_any
            if deduped_any:
                snapshot["selection_status"] = "deduped_out_by_shortlist_stock_dedupe"
            elif shortlist_selected_any:
                snapshot["selection_status"] = "shortlist_selected"
            elif bool(rank_after):
                snapshot["selection_status"] = "category_selected"
            elif top_n_excluded_any:
                snapshot["selection_status"] = "category_excluded_by_top_n"
            else:
                snapshot["selection_status"] = "not_selected"
            append_jsonl_fail_open(RAW_SIGNAL_SNAPSHOT_LEDGER_PATH, snapshot, "raw_signal_snapshot_ledger")

        for detail in event_details:
            stock_id = str(detail.get("stock_id"))
            strategy_id = str(detail.get("strategy_id"))
            selected_rank = selected.get((strategy_id, stock_id))
            if selected_rank is not None:
                detail["selected"] = True
                detail["category_selected"] = True
                detail["rank_after_limit"] = selected_rank
                detail["selection_status"] = "category_selected"
            elif detail.get("rank_before_limit") is not None:
                detail["category_excluded_by_top_n"] = True
                detail["selection_status"] = "category_excluded_by_top_n"
            detail["shortlist_rank"] = shortlist_ranks.get(stock_id)
            if stock_id in shortlist_ranks and strategy_id in shortlist_strategy_ids.get(stock_id, []):
                if shortlist_primary_strategy.get(stock_id) == strategy_id:
                    detail["shortlist_selected"] = True
                    detail["selection_status"] = "shortlist_selected"
                else:
                    detail["deduped_out"] = True
                    detail["selection_status"] = "deduped_out_by_shortlist_stock_dedupe"
            append_jsonl_fail_open(SIGNAL_EVENT_DETAIL_LEDGER_PATH, detail, "signal_event_detail_ledger")

        LEDGER_CONTEXT["signal_ledgers_flushed"] = True
        stats = LEDGER_CONTEXT.setdefault("ledger_stats", {})
        stats["snapshots_written"] = len(snapshots)
        stats["event_details_written"] = len(event_details)
    except Exception as exc:
        print(f"[ledger-warn] signal ledger flush failed: {exc}", file=sys.stderr)


def write_run_ledger(
    *,
    cfg: "Config",
    status: str,
    universe_count: int,
    active_keys: list[str],
    error_text: str | None = None,
) -> None:
    try:
        if not LEDGER_CONTEXT:
            return
        finished = now_taipei()
        started_raw = LEDGER_CONTEXT.get("started_at")
        duration_sec = None
        if started_raw:
            try:
                started = dt.datetime.fromisoformat(str(started_raw))
                duration_sec = (finished - started).total_seconds()
            except Exception:
                duration_sec = None
        record = {
            "ledger_type": "run",
            "schema_version": LEDGER_SCHEMA_VERSION,
            "execution_id": LEDGER_CONTEXT.get("execution_id"),
            "scheduled_slot_id": LEDGER_CONTEXT.get("scheduled_slot_id"),
            "run_date": cfg_date(cfg),
            "run_mode": LEDGER_CONTEXT.get("run_mode"),
            "status": status,
            "started_at": LEDGER_CONTEXT.get("started_at"),
            "finished_at": finished.isoformat(),
            "duration_sec": duration_sec,
            "code_commit_sha": LEDGER_CONTEXT.get("code_commit_sha"),
            "strategy_version": STRATEGY_VERSION,
            "strategy_versions": dict(STRATEGY_VERSIONS),
            "technical_parameters": dict(TECHNICAL_PARAMETER_SNAPSHOT),
            **LEDGER_PARAMETER_FIELDS,
            "active_strategy_keys": active_keys,
            "mother_universe_count": LEDGER_CONTEXT.get("mother_universe_count"),
            "screened_universe_count": universe_count,
            "universe_count": universe_count,
            "source_vendor": LEDGER_CONTEXT.get("source_vendor"),
            "timezone": LEDGER_CONTEXT.get("timezone"),
            "market_regime": LEDGER_CONTEXT.get("market_regime"),
            "ledger_stats": LEDGER_CONTEXT.get("ledger_stats", {}),
            "error": error_text,
            "quality_flags": ["LEDGER_FAIL_OPEN"],
        }
        append_jsonl_fail_open(RUN_LEDGER_PATH, record, "run_ledger")
    except Exception as exc:
        print(f"[ledger-warn] run ledger failed: {exc}", file=sys.stderr)


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
    only_prepare_turn: bool = dataclasses.field(
        default_factory=lambda: env_bool("ONLY_PREPARE_TURN", False)
    )
    intraday_alert_only: bool = dataclasses.field(
        default_factory=lambda: env_bool("INTRADAY_ALERT_ONLY", False)
    )
    prepare_turn_fallback_volume_shares: int = dataclasses.field(
        default_factory=lambda: env_int("PREPARE_TURN_FALLBACK_VOLUME_SHARES", 800000)
    )
    report_date: str = dataclasses.field(default_factory=lambda: env_str("REPORT_DATE"))
    min_price: float = dataclasses.field(default_factory=lambda: env_float("MIN_PRICE", 20.0))
    max_price: float = dataclasses.field(default_factory=lambda: env_float("MAX_PRICE", 700.0))
    min_turnover: float = dataclasses.field(
        default_factory=lambda: env_float("MIN_TURNOVER", 100_000_000.0)
    )
    daytrade_min_volume_shares: int = dataclasses.field(
        default_factory=lambda: env_int("DAYTRADE_MIN_VOLUME_SHARES", 500000)
    )
    daytrade_min_turnover: float = dataclasses.field(
        default_factory=lambda: env_float("DAYTRADE_MIN_TURNOVER", 50_000_000.0)
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
    relay_max_stop_loss_risk_pct: float = dataclasses.field(
        default_factory=lambda: env_float("RELAY_MAX_STOP_LOSS_RISK_PCT", 5.0)
    )
    precision_max_stop_loss_risk_pct: float = dataclasses.field(
        default_factory=lambda: env_float("PRECISION_MAX_STOP_LOSS_RISK_PCT", 3.0)
    )
    ntfy_server: str = dataclasses.field(default_factory=lambda: env_str("NTFY_SERVER", "https://ntfy.sh"))
    ntfy_topic: str = dataclasses.field(default_factory=lambda: env_str("NTFY_TOPIC"))
    enable_ntfy_intraday_alerts: bool = dataclasses.field(
        default_factory=lambda: env_bool("ENABLE_NTFY_INTRADAY_ALERTS", True)
    )
    market_filter_symbol: str = dataclasses.field(
        default_factory=lambda: env_str("MARKET_FILTER_SYMBOL", "^TWII")
    )
    market_min_daily_pct: float = dataclasses.field(
        default_factory=lambda: env_float("MARKET_MIN_DAILY_PCT", -1.2)
    )
    market_min_intraday_pct: float = dataclasses.field(
        default_factory=lambda: env_float("MARKET_MIN_INTRADAY_PCT", -0.8)
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
    out = pd.DataFrame()
    try:
        raw_5m = yf.Ticker(symbol).history(
            period="60d",
            interval="5m",
            auto_adjust=False,
        )
        five_min = normalize_yahoo_history(raw_5m)
        out = rebuild_taiwan_60k_from_5m(five_min)
    except Exception as exc:
        print(f"[60k] {symbol} failed to rebuild from 5m: {exc}")
    if out.empty:
        raw = yf.Ticker(symbol).history(
            period="60d",
            interval="60m",
            auto_adjust=False,
        )
        out = normalize_yahoo_history(raw)
        if not out.empty:
            print(f"[60k] {symbol} fallback to yahoo native 60m")
    return filter_by_report_date(out, cfg) if cfg else out


def get_yahoo_5m_intraday(
    stock_id: str,
    market_type: str | None = None,
    cfg: Config | None = None,
) -> pd.DataFrame:
    symbol = yahoo_symbol(stock_id, market_type)
    raw = yf.Ticker(symbol).history(
        period="5d",
        interval="5m",
        auto_adjust=False,
    )
    out = normalize_yahoo_history(raw)
    return filter_by_report_date(out, cfg) if cfg else out


def completed_5m_cutoff(as_of: dt.datetime | pd.Timestamp | None = None) -> pd.Timestamp:
    """Return the latest Yahoo 5m bar start that should be fully closed in Taiwan time."""
    if as_of is None:
        now = dt.datetime.now(TAIPEI_TZ)
    else:
        ts = pd.Timestamp(as_of)
        if ts.tzinfo is None:
            now = ts.to_pydatetime().replace(tzinfo=TAIPEI_TZ)
        else:
            now = ts.tz_convert(TAIPEI_TZ).to_pydatetime()
    naive_now = pd.Timestamp(now.replace(tzinfo=None))
    return naive_now.floor("5min") - pd.Timedelta(minutes=5)


def keep_completed_5m_bars(
    five_min: pd.DataFrame,
    *,
    as_of: dt.datetime | pd.Timestamp | None = None,
) -> pd.DataFrame:
    if five_min.empty or "date" not in five_min.columns:
        return five_min
    out = five_min.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    if getattr(out["date"].dt, "tz", None) is not None:
        out["date"] = out["date"].dt.tz_convert(TAIPEI_TZ).dt.tz_localize(None)
    if out["date"].dropna().empty:
        return out.iloc[0:0].copy()
    latest_day = out["date"].dropna().max().date()
    today = dt.datetime.now(TAIPEI_TZ).date()
    if as_of is not None:
        as_of_ts = pd.Timestamp(as_of)
        latest_as_of = (
            as_of_ts.tz_convert(TAIPEI_TZ).date()
            if as_of_ts.tzinfo is not None
            else as_of_ts.date()
        )
    else:
        latest_as_of = today
    if latest_day != latest_as_of:
        return out.sort_values("date")
    cutoff = completed_5m_cutoff(as_of)
    return out[out["date"] <= cutoff].sort_values("date")


def taiwan_60k_slot(ts: pd.Timestamp) -> pd.Timestamp | None:
    t = ts.time()
    if t < dt.time(9, 0) or t > dt.time(13, 30):
        return None
    day = ts.normalize()
    if t < dt.time(10, 0):
        return day + pd.Timedelta(hours=9)
    if t < dt.time(11, 0):
        return day + pd.Timedelta(hours=10)
    if t < dt.time(12, 0):
        return day + pd.Timedelta(hours=11)
    if t < dt.time(13, 0):
        return day + pd.Timedelta(hours=12)
    return day + pd.Timedelta(hours=13)


def rebuild_taiwan_60k_from_5m(five_min: pd.DataFrame) -> pd.DataFrame:
    if five_min.empty or "date" not in five_min.columns:
        return pd.DataFrame()
    df = five_min.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for col in ("open", "max", "min", "close", "Trading_Volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["date", "open", "max", "min", "close"])
    if df.empty:
        return pd.DataFrame()
    df["slot"] = df["date"].map(taiwan_60k_slot)
    df = df.dropna(subset=["slot"]).sort_values("date")
    if df.empty:
        return pd.DataFrame()
    hourly = (
        df.groupby("slot", sort=True)
        .agg(
            {
                "open": "first",
                "max": "max",
                "min": "min",
                "close": "last",
                "Trading_Volume": "sum",
            }
        )
        .reset_index()
        .rename(columns={"slot": "date"})
    )
    return hourly.sort_values("date").dropna(subset=["close"])


def expected_taiwan_60k_bars_for_now(report_day: dt.date) -> int:
    today = dt.date.today()
    if report_day != today:
        return 5
    now = dt.datetime.now().time()
    if now < dt.time(9, 0):
        return 0
    if now < dt.time(10, 0):
        return 1
    if now < dt.time(11, 0):
        return 2
    if now < dt.time(12, 0):
        return 3
    if now < dt.time(13, 0):
        return 4
    return 5


def intraday_session_is_current(kbar: pd.DataFrame, cfg: Config, min_bars: int = 4) -> bool:
    if kbar.empty or "date" not in kbar.columns:
        return False
    report_day = dt.date.fromisoformat(cfg_date(cfg))
    dates = pd.to_datetime(kbar["date"], errors="coerce")
    if dates.isna().all():
        return False
    last_date = dates.dropna().max().date()
    if last_date != report_day:
        return False
    today_bars = kbar[dates.dt.date == report_day]
    required_bars = min(min_bars, expected_taiwan_60k_bars_for_now(report_day))
    return len(today_bars) >= max(1, required_bars)


def add_indicators(df: pd.DataFrame, timeframe: str, atr_period: int = 14) -> pd.DataFrame:
    params = get_technical_params(timeframe)
    macd_params = params["macd"]
    kd_params = params["kd"]
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
    kd_period = int(kd_params["period"])
    k_smoothing = int(kd_params["k_smoothing"])
    d_smoothing = int(kd_params["d_smoothing"])
    rolling_low = low.rolling(kd_period).min()
    rolling_high = high.rolling(kd_period).max()
    rsv = (close - rolling_low) / (rolling_high - rolling_low) * 100
    out["kd_k"] = rsv.ewm(alpha=1 / k_smoothing, adjust=False).mean()
    out["kd_d"] = out["kd_k"].ewm(alpha=1 / d_smoothing, adjust=False).mean()
    out["kd_j"] = 3 * out["kd_k"] - 2 * out["kd_d"]
    ema_fast = close.ewm(span=int(macd_params["fast"]), adjust=False).mean()
    ema_slow = close.ewm(span=int(macd_params["slow"]), adjust=False).mean()
    out["dif"] = ema_fast - ema_slow
    out["macd"] = out["dif"].ewm(span=int(macd_params["signal"]), adjust=False).mean()
    out["hist"] = out["dif"] - out["macd"]
    std20 = close.rolling(20).std()
    out["bb_upper"] = out["ma20"] + 2 * std20
    out["bb_lower"] = out["ma20"] - 2 * std20
    out["bb_width"] = (out["bb_upper"] - out["bb_lower"]) / out["ma20"].replace(0, pd.NA)
    return out


def ma_deduction_up(ind: pd.DataFrame, lookback: int) -> bool:
    if len(ind) < lookback + 1 or "close" not in ind.columns:
        return False
    latest = float(ind.iloc[-1]["close"])
    compare = float(ind.iloc[-lookback - 1]["close"])
    return latest >= compare


def ma_slope_flat_or_up(ind: pd.DataFrame, column: str = "ma20", days: int = 3, tolerance_pct: float = 0.15) -> bool:
    if len(ind) < days + 1 or column not in ind.columns:
        return False
    series = pd.to_numeric(ind[column], errors="coerce").dropna()
    if len(series) < days + 1:
        return False
    latest = float(series.iloc[-1])
    previous = float(series.iloc[-2])
    anchor = float(series.iloc[-days - 1])
    if latest <= 0 or anchor <= 0:
        return False
    one_day_flat = (latest - previous) / latest * 100 >= -tolerance_pct
    multi_day_flat = (latest - anchor) / anchor * 100 >= -tolerance_pct * days
    return bool(one_day_flat and multi_day_flat)


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


def trend_and_macd_ok(df: pd.DataFrame, timeframe: str = TIMEFRAME_DAILY) -> bool:
    if len(df) < 60:
        return False
    latest = add_indicators(df, timeframe=timeframe).iloc[-1]
    values = latest[["ma5", "ma20", "ma60", "dif", "macd"]]
    if values.isna().any():
        return False
    return bool(
        latest["ma5"] > latest["ma20"] > latest["ma60"]
        and latest["dif"] > 0
        and latest["macd"] > 0
    )


def ma_alignment_ok(df: pd.DataFrame, timeframe: str = TIMEFRAME_DAILY) -> bool:
    if len(df) < 60:
        return False
    latest = add_indicators(df, timeframe=timeframe).iloc[-1]
    values = latest[["ma5", "ma20", "ma60"]]
    if values.isna().any():
        return False
    return bool(latest["ma5"] > latest["ma20"] > latest["ma60"])


def macd_above_zero_ok(df: pd.DataFrame, timeframe: str = TIMEFRAME_DAILY) -> bool:
    if len(df) < 35:
        return False
    latest = add_indicators(df, timeframe=timeframe).iloc[-1]
    values = latest[["dif", "macd"]]
    if values.isna().any():
        return False
    return bool(latest["dif"] > 0 and latest["macd"] > 0)


def all_ma_alignment_ok(daily: pd.DataFrame) -> bool:
    weekly = resample_ohlcv(daily, "W-FRI")
    monthly = resample_ohlcv(daily, "ME")
    return (
        ma_alignment_ok(daily, timeframe=TIMEFRAME_DAILY)
        and ma_alignment_ok(weekly, timeframe=TIMEFRAME_WEEKLY)
        and ma_alignment_ok(monthly, timeframe=TIMEFRAME_MONTHLY)
    )


def all_macd_above_zero_ok(daily: pd.DataFrame) -> bool:
    weekly = resample_ohlcv(daily, "W-FRI")
    monthly = resample_ohlcv(daily, "ME")
    return (
        macd_above_zero_ok(daily, timeframe=TIMEFRAME_DAILY)
        and macd_above_zero_ok(weekly, timeframe=TIMEFRAME_WEEKLY)
        and macd_above_zero_ok(monthly, timeframe=TIMEFRAME_MONTHLY)
    )


def all_big_timeframes_ok(daily: pd.DataFrame) -> bool:
    weekly = resample_ohlcv(daily, "W-FRI")
    monthly = resample_ohlcv(daily, "ME")
    return (
        trend_and_macd_ok(daily, timeframe=TIMEFRAME_DAILY)
        and trend_and_macd_ok(weekly, timeframe=TIMEFRAME_WEEKLY)
        and trend_and_macd_ok(monthly, timeframe=TIMEFRAME_MONTHLY)
    )


def calculate_stop_loss(daily: pd.DataFrame, cfg: Config) -> dict[str, Any]:
    ind = add_indicators(daily, timeframe=TIMEFRAME_DAILY, atr_period=cfg.atr_period)
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


def calculate_relay_stop_loss(daily: pd.DataFrame, cfg: Config) -> dict[str, Any]:
    """Second-category structural stop: breakout point or red-candle midpoint."""
    ind = add_indicators(daily, timeframe=TIMEFRAME_DAILY, atr_period=cfg.atr_period)
    if len(ind) < 25:
        return calculate_stop_loss(daily, cfg)
    latest = ind.iloc[-1]
    close = float(latest["close"])
    open_price = float(latest["open"])
    buffer = cfg.stop_loss_buffer_pct / 100

    recent20 = ind.tail(20).copy()
    before_today = recent20.iloc[:-1]
    platform_high = float(before_today["max"].max()) if not before_today.empty else 0.0
    body_mid = (open_price + close) / 2 if close > open_price else 0.0
    ma5 = float(latest["ma5"]) if not pd.isna(latest["ma5"]) else 0.0
    ma10 = float(latest["ma10"]) if not pd.isna(latest["ma10"]) else 0.0

    candidates: list[tuple[str, float]] = []
    if 0 < platform_high < close:
        candidates.append(("relay_breakout_point", platform_high * (1 - buffer)))
    if 0 < body_mid < close:
        candidates.append(("relay_body_midpoint", body_mid))
    for name, value in (("relay_ma5", ma5), ("relay_ma10", ma10)):
        if 0 < value < close:
            candidates.append((name, value * (1 - buffer)))

    valid = [(name, price) for name, price in candidates if price > 0 and price < close]
    if not valid:
        return calculate_stop_loss(daily, cfg)

    ideal = [
        (name, price)
        for name, price in valid
        if 2.5 <= (close - price) / close * 100 <= 4.5
    ]
    method, stop_price = max(ideal or valid, key=lambda item: item[1])
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
    ind = add_indicators(hourly, timeframe=TIMEFRAME_60K)
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
    ind = add_indicators(hourly, timeframe=TIMEFRAME_60K)
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
    return macd_above_zero_ok(daily, timeframe=TIMEFRAME_DAILY)


def daily_trend_protection_ok(daily: pd.DataFrame) -> tuple[bool, dict[str, Any]]:
    info: dict[str, Any] = {}
    if len(daily) < 65:
        return False, info
    ind = add_indicators(daily, timeframe=TIMEFRAME_DAILY)
    latest = ind.iloc[-1]
    required = ["close", "ma5", "ma10", "ma20", "ma60", "dif", "macd", "hist"]
    if latest[required].isna().any():
        return False, info
    close = float(latest["close"])
    ma5 = float(latest["ma5"])
    ma10 = float(latest["ma10"])
    ma20 = float(latest["ma20"])
    ma60 = float(latest["ma60"])
    dif = float(latest["dif"])
    macd = float(latest["macd"])
    hist = float(latest["hist"])
    above_all_ma = close > ma5 and close > ma10 and close > ma20 and close > ma60
    daily_macd_above_zero = dif > 0 and macd > 0
    daily_momentum_ok = hist >= float(ind.iloc[-2]["hist"]) if not pd.isna(ind.iloc[-2]["hist"]) else False
    info.update(
        {
            "above_all_daily_ma": above_all_ma,
            "daily_macd_above_zero": daily_macd_above_zero,
            "daily_momentum_ok": daily_momentum_ok,
        }
    )
    return bool(above_all_ma and daily_macd_above_zero), info


def daily_prepare_turn_gate_ok(daily: pd.DataFrame) -> tuple[bool, dict[str, Any]]:
    info: dict[str, Any] = {}
    if len(daily) < 25:
        return False, info
    ind = add_indicators(daily, timeframe=TIMEFRAME_DAILY)
    latest = ind.iloc[-1]
    previous = ind.iloc[-2]
    required = ["close", "ma5", "ma10", "ma20", "dif", "macd", "hist"]
    if latest[required].isna().any() or previous[["close", "ma5", "hist"]].isna().any():
        return False, info

    close = float(latest["close"])
    ma5 = float(latest["ma5"])
    ma10 = float(latest["ma10"])
    ma20 = float(latest["ma20"])
    dif = float(latest["dif"])
    macd = float(latest["macd"])
    hist = float(latest["hist"])
    prev_hist = float(previous["hist"])
    prev_close = float(previous["close"])
    prev_ma5 = float(previous["ma5"])

    above_ma10_or_20 = close > ma10 or close > ma20
    reclaimed_ma5_today = close > ma5 and prev_close <= prev_ma5
    daily_structure_ok = above_ma10_or_20 or reclaimed_ma5_today
    daily_momentum_ok = dif > macd or hist >= prev_hist or (dif > 0 and macd > 0)

    info.update(
        {
            "above_daily_ma10_or_20": above_ma10_or_20,
            "reclaimed_daily_ma5_today": reclaimed_ma5_today,
            "daily_prepare_momentum_ok": daily_momentum_ok,
        }
    )
    return bool(daily_structure_ok and daily_momentum_ok), info


def weekly_macd_above_zero_from_daily(daily: pd.DataFrame) -> bool:
    weekly = resample_ohlcv(daily, "W-FRI")
    return macd_above_zero_ok(weekly, timeframe=TIMEFRAME_WEEKLY)


def daily_ma_cluster_breakout_info(daily: pd.DataFrame) -> dict[str, Any]:
    info = {
        "ma_cluster_tight": False,
        "ma_cluster_breakout_2pct": False,
        "ma_cluster_width_pct": None,
        "ma_cluster_breakout_pct": None,
    }
    if len(daily) < 25:
        return info
    ind = add_indicators(daily, timeframe=TIMEFRAME_DAILY)
    latest = ind.iloc[-1]
    required = ["close", "ma5", "ma10", "ma20"]
    if latest[required].isna().any():
        return info
    close = float(latest["close"])
    mas = [float(latest["ma5"]), float(latest["ma10"]), float(latest["ma20"])]
    ma_low = min(mas)
    ma_high = max(mas)
    if ma_low <= 0:
        return info
    cluster_width = (ma_high - ma_low) / ma_low * 100
    breakout_pct = (close / ma_high - 1) * 100 if ma_high > 0 else 0
    info.update(
        {
            "ma_cluster_tight": cluster_width <= 3.0,
            "ma_cluster_breakout_2pct": cluster_width <= 3.0 and close >= ma_high * 1.02,
            "ma_cluster_width_pct": round(cluster_width, 2),
            "ma_cluster_breakout_pct": round(breakout_pct, 2),
        }
    )
    return info


def daily_daytrade_protection_ok(daily: pd.DataFrame) -> tuple[bool, dict[str, Any]]:
    info: dict[str, Any] = {}
    if len(daily) < 65:
        return False, info
    ind = add_indicators(daily, timeframe=TIMEFRAME_DAILY)
    latest = ind.iloc[-1]
    previous = ind.iloc[-2]
    required = ["close", "ma5", "ma10", "dif", "macd"]
    if latest[required].isna().any() or pd.isna(previous["close"]) or pd.isna(previous["ma5"]):
        return False, info
    close = float(latest["close"])
    ma5 = float(latest["ma5"])
    ma10 = float(latest["ma10"]) if not pd.isna(latest["ma10"]) else 0.0
    dif = float(latest["dif"])
    macd = float(latest["macd"])
    above_ma5 = close > ma5
    reclaimed_ma5 = close > ma5 and float(previous["close"]) <= float(previous["ma5"])
    above_ma10 = ma10 > 0 and close > ma10
    macd_above_zero = dif > 0 and macd > 0
    ma_cluster = daily_ma_cluster_breakout_info(daily)
    info.update(
        {
            "above_daily_ma5": above_ma5,
            "reclaimed_daily_ma5": reclaimed_ma5,
            "above_daily_ma10": above_ma10,
            "daily_macd_above_zero": macd_above_zero,
            **ma_cluster,
        }
    )
    return bool((above_ma5 or reclaimed_ma5) and macd_above_zero), info


def intraday_60k_daytrade_direction_ok(kbar: pd.DataFrame) -> tuple[bool, dict[str, Any]]:
    ok, _reason, priority, info = intraday_prepare_turn_signal(kbar)
    if ok:
        info["direction_priority"] = priority
        return True, info
    if kbar.empty:
        return False, info
    ind = add_indicators(kbar, timeframe=TIMEFRAME_60K)
    tail = ind.tail(5)
    required = ["close", "ma5", "ma10", "dif", "macd", "hist"]
    if len(tail) < 5 or tail[required].isna().any().any():
        return False, info
    latest = tail.iloc[-1]
    dif = tail["dif"].astype(float).tolist()
    hist = tail["hist"].astype(float).tolist()
    close = float(latest["close"])
    ma5 = float(latest["ma5"])
    ma10 = float(latest["ma10"])
    hist_contracting = hist[-4] < hist[-3] < hist[-2] < hist[-1]
    dif_turning_up = dif[-1] > dif[-2] > dif[-3]
    above_ma5_or_ma10 = close > ma5 or close > ma10
    above_zero = float(latest["dif"]) > 0 and float(latest["macd"]) > 0
    info.update(
        {
            "hist_contracting": hist_contracting,
            "dif_turning_up": dif_turning_up,
            "above_60m_ma5_or_ma10": above_ma5_or_ma10,
            "above_60m_zero": above_zero,
            "direction_priority": int(above_zero) + int(above_ma5_or_ma10) + int(hist_contracting or dif_turning_up),
        }
    )
    return bool(above_ma5_or_ma10 and (hist_contracting or dif_turning_up or above_zero)), info


def intraday_5k_daytrade_signal_legacy(
    five_min: pd.DataFrame,
    *,
    as_of: dt.datetime | pd.Timestamp | None = None,
) -> tuple[bool, str, int, dict[str, Any]]:
    info: dict[str, Any] = {}
    if five_min.empty:
        return False, "", 0, info
    df = five_min.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for col in ("open", "max", "min", "close", "Trading_Volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["date", "open", "max", "min", "close"]).sort_values("date")
    raw_last_timestamp = df["date"].dropna().max() if not df["date"].dropna().empty else None
    df = keep_completed_5m_bars(df, as_of=as_of)
    if df.empty:
        return False, "", 0, info
    completed_cutoff = completed_5m_cutoff(as_of)
    report_day = df["date"].dropna().max().date()
    today = df[df["date"].dt.date == report_day].copy()
    today = today[(today["date"].dt.time >= dt.time(9, 5)) & (today["date"].dt.time <= dt.time(13, 30))]
    if len(today) < 2:
        return False, "", 0, info
    ind = add_indicators(df, timeframe=TIMEFRAME_5K)
    today_ind = ind[ind["date"].dt.date == report_day].copy()
    today_ind = today_ind[(today_ind["date"].dt.time >= dt.time(9, 5)) & (today_ind["date"].dt.time <= dt.time(13, 30))]
    if len(today_ind) < 2:
        return False, "", 0, info
    latest = today_ind.iloc[-1]
    recent = today_ind.tail(3)
    required = ["close", "open", "max", "min", "dif", "macd", "hist", "Trading_Volume"]
    if recent[required].isna().any().any():
        return False, "", 0, info
    previous = recent.iloc[-2]
    ema5 = ind["close"].astype(float).ewm(span=5, adjust=False).mean().loc[latest.name]
    hist = recent["hist"].astype(float).tolist()
    dif = recent["dif"].astype(float).tolist()
    macd = recent["macd"].astype(float).tolist()
    close = float(latest["close"])
    open_price = float(latest["open"])
    high = float(latest["max"])
    low_today = float(today_ind["min"].min())
    volume_today = float(today_ind["Trading_Volume"].sum())
    turnover_today = float((today_ind["Trading_Volume"].astype(float) * today_ind["close"].astype(float)).sum())
    stands_above_ema5 = close > float(ema5)
    hist_contracting_2 = len(hist) >= 3 and hist[-3] < hist[-2] < hist[-1] and hist[-1] <= 0
    hist_turn_red = len(hist) >= 2 and hist[-2] <= 0 < hist[-1]
    golden_cross = len(dif) >= 2 and dif[-2] <= macd[-2] and dif[-1] > macd[-1]
    dif_turning_up = len(dif) >= 3 and dif[-1] > dif[-2] > dif[-3]
    red_body = close > open_price
    close_near_high = high > 0 and (high - close) / high <= 0.018
    stop_risk_pct = (close - low_today) / close * 100 if close > 0 and low_today > 0 else 999
    signal_stop = round(low_today, 2)
    signal_stop_risk = round(stop_risk_pct, 2)
    info.update(
        {
            "5k_raw_last_timestamp": str(raw_last_timestamp) if raw_last_timestamp is not None else "",
            "5k_completed_cutoff": str(completed_cutoff),
            "5k_signal_timestamp": str(latest["date"]),
            "5k_previous_timestamp": str(recent.iloc[-2]["date"]) if len(recent) >= 2 else "",
            "5k_signal_open": round(open_price, 2),
            "5k_signal_high": round(high, 2),
            "5k_signal_low": round(float(latest["min"]), 2),
            "5k_signal_close": round(close, 2),
            "5k_signal_ema5": round(float(ema5), 4),
            "5k_signal_dif": round(float(dif[-1]), 6),
            "5k_signal_macd": round(float(macd[-1]), 6),
            "5k_signal_histogram": round(float(hist[-1]), 6),
            "5k_previous_close": round(float(previous["close"]), 2),
            "5k_previous_dif": round(float(dif[-2]), 6),
            "5k_previous_macd": round(float(macd[-2]), 6),
            "5k_previous_histogram": round(float(hist[-2]), 6),
            "5k_above_ema5": stands_above_ema5,
            "5k_hist_contracting_2": hist_contracting_2,
            "5k_hist_turn_red": hist_turn_red,
            "5k_golden_cross": golden_cross,
            "5k_dif_turning_up": dif_turning_up,
            "5k_red_body": red_body,
            "5k_close_near_high": close_near_high,
            "intraday_volume_shares": int(volume_today),
            "intraday_turnover": int(turnover_today),
            "open_low_stop": signal_stop,
            "open_low_stop_risk_pct": signal_stop_risk,
            "5k_stop_price": signal_stop,
            "5k_stop_risk_pct": signal_stop_risk,
            "5k_stop_risk_basis": A5_SIGNAL_PRICE_RULE,
        }
    )
    momentum = hist_contracting_2 or hist_turn_red or golden_cross or dif_turning_up
    if not (stands_above_ema5 and momentum and red_body and stop_risk_pct <= 3.0):
        return False, "", 0, info
    priority = 1
    if hist_turn_red or golden_cross:
        priority += 2
    if hist_contracting_2:
        priority += 1
    if close_near_high:
        priority += 1
    if stop_risk_pct <= 2.5:
        priority += 1
    reason = "5K站上5EMA且MACD綠柱收斂"
    if hist_turn_red:
        reason = "5K站上5EMA且MACD翻紅"
    if golden_cross:
        reason = "5K站上5EMA且DIF黃金交叉"
    return True, reason, priority, info


def intraday_prepare_turn_signal(kbar: pd.DataFrame) -> tuple[bool, str, int, dict[str, Any]]:
    info: dict[str, Any] = {}
    if kbar.empty:
        return False, "", 0, info
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
    if len(hourly) < 65:
        return False, "", 0, info
    ind = add_indicators(hourly, timeframe=TIMEFRAME_60K)
    tail = ind.tail(5)
    required = ["close", "open", "max", "ma5", "ma10", "ma20", "ma60", "dif", "macd", "hist", "kd_k", "kd_d"]
    if tail[required].isna().any().any():
        return False, "", 0, info

    latest = tail.iloc[-1]
    previous = tail.iloc[-2]
    close = float(latest["close"])
    open_price = float(latest["open"])
    high = float(latest["max"])
    ma5 = float(latest["ma5"])
    ma10 = float(latest["ma10"])
    ma20 = float(latest["ma20"])
    ma60 = float(latest["ma60"])
    dif = tail["dif"].astype(float).tolist()
    macd = tail["macd"].astype(float).tolist()
    hist = tail["hist"].astype(float).tolist()

    reclaimed_ma5 = close > ma5 and float(previous["close"]) <= float(previous["ma5"])
    holds_ma5 = close > ma5 and float(previous["close"]) > float(previous["ma5"]) and close >= float(previous["close"])
    above_ma10 = close > ma10
    near_or_above_ma20 = close >= ma20 * 0.995
    from_ma20_or_ma60_support = float(tail["min"].tail(3).min()) <= max(ma20, ma60) * 1.015
    hist_contracting = hist[-4] < hist[-3] < hist[-2] < hist[-1] and hist[-1] <= 0
    hist_turn_red = hist[-2] <= 0 < hist[-1]
    dif_turning_up = dif[-1] > dif[-2] > dif[-3]
    dif_above_macd = dif[-1] > macd[-1]
    above_zero = dif[-1] > 0 and macd[-1] > 0
    red_body = close > open_price
    close_near_high = high > 0 and (high - close) / high <= 0.025
    kd_not_overheated = float(latest["kd_k"]) < 85 or float(latest["kd_d"]) < 85

    info.update(
        {
            "reclaimed_60m_ma5": reclaimed_ma5,
            "holds_60m_ma5": holds_ma5,
            "above_60m_ma10": above_ma10,
            "near_or_above_60m_ma20": near_or_above_ma20,
            "from_60m_support": from_ma20_or_ma60_support,
            "hist_contracting": hist_contracting,
            "hist_turn_red": hist_turn_red,
            "dif_turning_up": dif_turning_up,
            "dif_above_macd": dif_above_macd,
            "above_60m_zero": above_zero,
            "red_body": red_body,
            "close_near_high": close_near_high,
            "kd_not_overheated": kd_not_overheated,
        }
    )

    core_reclaim = close > ma5 and (reclaimed_ma5 or holds_ma5)
    momentum_ready = hist_contracting or hist_turn_red or dif_turning_up or dif_above_macd
    structure_ok = above_ma10 or near_or_above_ma20 or above_zero
    quality_ok = structure_ok
    if not (core_reclaim and momentum_ready and quality_ok):
        return False, "", 0, info

    priority = 1
    if above_zero:
        priority += 1
    if dif_above_macd or hist_turn_red:
        priority += 1
    if red_body and close_near_high:
        priority += 1
    if kd_not_overheated:
        priority += 1
    reason = "60K重新站上SMA5且動能收斂"
    if above_zero:
        reason = "60K零軸上起漲雷達"
    return True, reason, priority, info


def intraday_extreme_daytrade_signal_legacy(kbar: pd.DataFrame) -> tuple[bool, str, int, dict[str, Any]]:
    info: dict[str, Any] = {}
    if kbar.empty:
        return False, "", 0, info
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
        return False, "", 0, info
    ind = add_indicators(hourly, timeframe=TIMEFRAME_60K)
    tail = ind.tail(20)
    latest3 = ind.tail(3)
    required = ["open", "close", "ma5", "ma10", "ma20", "dif", "macd", "hist", "Trading_Volume"]
    if tail[required].isna().any().any() or latest3[required].isna().any().any():
        return False, "", 0, info

    correction_window = tail.iloc[:-3]
    below_ma5_ratio = float(
        (correction_window["close"].astype(float) < correction_window["ma5"].astype(float)).mean()
    )
    latest3_above_ma5_ma10 = bool(
        (
            (latest3["close"].astype(float) > latest3["ma5"].astype(float))
            & (latest3["close"].astype(float) > latest3["ma10"].astype(float))
        ).all()
    )

    hist = tail["hist"].astype(float).tolist()
    dif = tail["dif"].astype(float).tolist()
    macd = tail["macd"].astype(float).tolist()
    hist_contracting = hist[-3] < hist[-2] < hist[-1] and hist[-1] <= 0
    hist_turn_red = hist[-2] <= 0 < hist[-1]
    dif_toward_zero = dif[-1] > dif[-2] > dif[-3] or abs(dif[-1]) < abs(dif[-3])
    macd_toward_zero = macd[-1] > macd[-2] or abs(macd[-1]) < abs(macd[-3])
    momentum_ready = hist_contracting or hist_turn_red or (dif_toward_zero and macd_toward_zero)

    latest = tail.iloc[-1]
    previous = tail.iloc[-2]
    close = float(latest["close"])
    open_price = float(latest["open"])
    ma20 = float(latest["ma20"])
    red_body = close > open_price
    hourly_volume_accel = float(latest["Trading_Volume"]) >= float(previous["Trading_Volume"]) * 0.9
    above_ma20 = close > ma20

    info.update(
        {
            "below_60m_ma5_ratio": round(below_ma5_ratio, 2),
            "latest3_above_60m_ma5_ma10": latest3_above_ma5_ma10,
            "hist_contracting": hist_contracting,
            "hist_turn_red": hist_turn_red,
            "dif_toward_zero": dif_toward_zero,
            "macd_toward_zero": macd_toward_zero,
            "red_body": red_body,
            "hourly_volume_accel": hourly_volume_accel,
            "above_60m_ma20": above_ma20,
        }
    )

    if not (below_ma5_ratio >= 0.55 and latest3_above_ma5_ma10 and momentum_ready):
        return False, "", 0, info

    priority = 1
    if hist_turn_red:
        priority += 2
    if hist_contracting:
        priority += 1
    if above_ma20:
        priority += 1
    if red_body:
        priority += 1
    if hourly_volume_accel:
        priority += 1
    return True, "60K連續修正後強勢收復", priority, info


def daytrade_filter_ok(row: pd.Series, cfg: Config, stop: dict[str, Any]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    last_close = float(stop["last_close"])
    volume = float(row["Trading_Volume"])
    turnover = volume * last_close
    if last_close > cfg.max_price:
        reasons.append("股價超過700元")
    if volume < cfg.daytrade_min_volume_shares:
        reasons.append("當沖成交量不足")
    if turnover < cfg.daytrade_min_turnover:
        reasons.append("當沖成交金額不足")
    return not reasons, reasons


def elite_reclaim_setup(daily: pd.DataFrame) -> tuple[bool, dict[str, Any]]:
    """Catch early short-MA reclaim setups after a brief bearish washout."""
    info: dict[str, Any] = {}
    if len(daily) < 65:
        return False, info
    ind = add_indicators(daily, timeframe=TIMEFRAME_DAILY)
    latest = ind.iloc[-1]
    previous = ind.iloc[-2]
    ma60_5ago = ind.iloc[-6]["ma60"] if len(ind) >= 66 else None
    close_60ago = ind.iloc[-61]["close"] if len(ind) >= 66 else None
    required = [
        "open",
        "close",
        "max",
        "min",
        "ma5",
        "ma10",
        "ma20",
        "ma60",
        "Trading_Volume",
        "dif",
        "macd",
        "hist",
        "kd_k",
        "kd_d",
        "bb_width",
    ]
    if latest[required].isna().any() or previous[required].isna().any():
        return False, info

    close = float(latest["close"])
    open_price = float(latest["open"])
    low = float(latest["min"])
    ma5 = float(latest["ma5"])
    ma10 = float(latest["ma10"])
    ma20 = float(latest["ma20"])
    ma60 = float(latest["ma60"])
    if min(ma5, ma10, ma20, ma60) <= 0:
        return False, info
    if ma60_5ago is None or pd.isna(ma60_5ago) or close_60ago is None or pd.isna(close_60ago):
        return False, info
    ma60_rising = ma60 >= float(ma60_5ago) * 0.997
    season_deduction_ok = close > float(close_60ago)
    above_season_line = close > ma60

    recent = ind.tail(11).iloc[:-1]
    below_all_short = (
        (recent["close"].astype(float) < recent[["ma5", "ma10", "ma20"]].astype(float).min(axis=1))
        | (recent["min"].astype(float) < recent[["ma5", "ma10", "ma20"]].astype(float).min(axis=1))
    )
    had_washout = bool(below_all_short.tail(10).any())
    reclaim_ma5_ma10 = close > ma5 and close > ma10
    reclaim_ma20 = close > ma20
    reclaimed_today = close > ma20 and float(previous["close"]) <= float(previous["ma20"])
    red_or_strong = close > open_price or close >= float(previous["close"]) * 1.015

    vol5 = float(ind.tail(6).iloc[:-1]["Trading_Volume"].mean())
    vol20 = float(ind.tail(21).iloc[:-1]["Trading_Volume"].mean())
    today_vol = float(latest["Trading_Volume"])
    volume_ok = today_vol >= vol5 * 1.3 and today_vol >= vol20 * 1.3

    hist_tail = ind["hist"].tail(4).astype(float).tolist()
    hist_improving = len(hist_tail) >= 4 and hist_tail[-1] > hist_tail[-2] > hist_tail[-3]
    hist_cross_red = hist_tail[-2] <= 0 < hist_tail[-1]
    dif_rising = float(latest["dif"]) > float(previous["dif"])
    macd_constructive = bool(hist_improving or hist_cross_red or (dif_rising and float(latest["hist"]) > 0))

    kd_k = float(latest["kd_k"])
    kd_d = float(latest["kd_d"])
    prev_k = float(previous["kd_k"])
    prev_d = float(previous["kd_d"])
    kd_turning = kd_k > kd_d and kd_k > prev_k
    kd_not_overheated = kd_k <= 85 and kd_d <= 80

    ma5_deduction_ok = ma_deduction_up(ind, 5)
    ma20_flat_or_up = ma_slope_flat_or_up(ind, "ma20", days=3, tolerance_pct=0.12)
    bb_width_ok = float(latest["bb_width"]) >= 0.10
    not_extended = (close - ma20) / ma20 <= 0.08
    price_not_chasing = (close - low) / close <= 0.055
    trend_floor = ma20 >= ma60 * 0.96 and above_season_line and ma60_rising and season_deduction_ok

    ok = all(
        [
            had_washout,
            reclaim_ma5_ma10,
            reclaim_ma20,
            ma5_deduction_ok,
            ma20_flat_or_up,
            red_or_strong,
            volume_ok,
            bb_width_ok,
            macd_constructive,
            kd_turning,
            kd_not_overheated,
            not_extended,
            price_not_chasing,
            trend_floor,
        ]
    )
    info = {
        "had_washout": had_washout,
        "reclaimed_today": reclaimed_today,
        "reclaim_ma20": reclaim_ma20,
        "volume_ratio_5d": round(today_vol / vol5, 2) if vol5 > 0 else 0,
        "volume_ratio_20d": round(today_vol / vol20, 2) if vol20 > 0 else 0,
        "ma5_deduction_ok": ma5_deduction_ok,
        "ma20_flat_or_up": ma20_flat_or_up,
        "bb_width_pct": round(float(latest["bb_width"]) * 100, 2),
        "hist_improving": hist_improving,
        "hist_cross_red": hist_cross_red,
        "kd_k": round(kd_k, 2),
        "kd_d": round(kd_d, 2),
        "ma20_distance_pct": round((close - ma20) / ma20 * 100, 2),
        "above_season_line": above_season_line,
        "ma60_rising": ma60_rising,
        "season_deduction_ok": season_deduction_ok,
    }
    return bool(ok), info


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
    latest = add_indicators(daily, timeframe=TIMEFRAME_DAILY).iloc[-1]
    if pd.isna(latest["ma20"]) or float(latest["ma20"]) <= 0:
        return 999.0
    return (float(latest["close"]) - float(latest["ma20"])) / float(latest["ma20"]) * 100


def common_trade_filter_ok(
    daily: pd.DataFrame,
    cfg: Config,
    row: pd.Series,
    stop: dict[str, Any],
    max_stop_loss_risk_pct: float | None = None,
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    last_close = float(stop["last_close"])
    turnover = float(row["Trading_Volume"]) * last_close
    daily_gain = price_change_pct(daily)
    gain_3d = recent_gain_pct(daily, 3)
    ma20_distance = ma20_distance_pct(daily)
    stop_risk = stop.get("stop_loss_risk_pct")

    if last_close > cfg.max_price:
        reasons.append("股價超過700元")
    if float(row["Trading_Volume"]) < cfg.min_volume_shares:
        reasons.append("成交量低於1000張")
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
    max_stop = cfg.max_stop_loss_risk_pct if max_stop_loss_risk_pct is None else max_stop_loss_risk_pct
    if stop_risk is None or float(stop_risk) > max_stop:
        reasons.append("停損距離過遠")
    return not reasons, reasons


def support_pullback_ok(daily: pd.DataFrame) -> bool:
    if len(daily) < 60:
        return False
    ind = add_indicators(daily, timeframe=TIMEFRAME_DAILY)
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
    ind = add_indicators(hourly, timeframe=TIMEFRAME_60K)
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
    ind = add_indicators(daily, timeframe=TIMEFRAME_DAILY)
    latest = ind.iloc[-1]
    required = ["open", "close", "max", "min", "ma5", "ma10", "ma20", "Trading_Volume"]
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
    if not 5 <= len(platform) <= 15:
        return False
    platform_high = float(platform["max"].max())
    platform_low = float(platform["min"].min())
    tight_platform = platform_low > 0 and (platform_high - platform_low) / platform_low <= 0.15

    close = float(latest["close"])
    open_price = float(latest["open"])
    high_price = float(latest["max"])
    low_price = float(latest["min"])
    above_short_ma = (
        close > float(latest["ma5"])
        and close > float(latest["ma10"])
        and close > float(latest["ma20"])
    )
    candle_range = high_price - low_price
    if candle_range <= 0 or open_price <= 0:
        return False
    red_body = close > open_price and (close - open_price) / candle_range >= 0.40
    upper_shadow_ok = (high_price - close) / candle_range < 0.30
    previous_close = float(ind.iloc[-2]["close"])
    gap_ok = previous_close > 0 and (open_price - previous_close) / previous_close <= 0.05
    today_volume = float(latest["Trading_Volume"])
    platform_avg_volume = float(platform["Trading_Volume"].mean())
    recent20_avg_volume = float(ind.tail(21).iloc[:-1]["Trading_Volume"].mean())
    previous_volume = float(ind.iloc[-2]["Trading_Volume"])
    platform_last3_avg = float(platform.tail(3)["Trading_Volume"].mean())
    breakout_seed_volume = float(before_today.loc[high_label, "Trading_Volume"])
    volume_breakout = (
        today_volume >= previous_volume * 1.5
        and today_volume > recent20_avg_volume
        and today_volume > platform_avg_volume * 1.4
    )
    volume_shrank = (
        platform_last3_avg <= recent20_avg_volume * 0.85
        or float(platform["Trading_Volume"].max()) <= breakout_seed_volume * 0.80
    )
    close_near_high = high_price > 0 and (high_price - close) / high_price <= 0.03
    return bool(
        tight_platform
        and above_short_ma
        and red_body
        and upper_shadow_ok
        and gap_ok
        and volume_breakout
        and volume_shrank
        and close_near_high
    )


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
            "trust_buy_streak": 0,
            "foreign_buy_streak": 0,
            "total_inst_buy_streak": 0,
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
    trust_daily = []
    foreign_daily = []
    total_daily = []
    for _, trust_row in df.tail(5).iterrows():
        trust_buy = float(trust_row["Investment_Trust_buy"]) if "Investment_Trust_buy" in df.columns else 0.0
        trust_sell = float(trust_row["Investment_Trust_sell"]) if "Investment_Trust_sell" in df.columns else 0.0
        foreign_buy = (
            (float(trust_row["Foreign_Investor_buy"]) if "Foreign_Investor_buy" in df.columns else 0.0)
            + (
                float(trust_row["Foreign_Dealer_Self_buy"])
                if "Foreign_Dealer_Self_buy" in df.columns
                else 0.0
            )
        )
        foreign_sell = (
            (float(trust_row["Foreign_Investor_sell"]) if "Foreign_Investor_sell" in df.columns else 0.0)
            + (
                float(trust_row["Foreign_Dealer_Self_sell"])
                if "Foreign_Dealer_Self_sell" in df.columns
                else 0.0
            )
        )
        row_buy_total = sum(
            float(trust_row[col]) for col in df.columns if col.endswith("_buy")
        )
        row_sell_total = sum(
            float(trust_row[col]) for col in df.columns if col.endswith("_sell")
        )
        trust_daily.append(int(trust_buy - trust_sell))
        foreign_daily.append(int(foreign_buy - foreign_sell))
        total_daily.append(int(row_buy_total - row_sell_total))

    def consecutive_positive(values: list[int]) -> int:
        streak = 0
        for value in reversed(values):
            if value > 0:
                streak += 1
            else:
                break
        return streak

    trust_buy_streak = consecutive_positive(trust_daily)
    foreign_buy_streak = consecutive_positive(foreign_daily)
    total_inst_buy_streak = consecutive_positive(total_daily)
    return {
        "foreign_5d_net": foreign_5d,
        "trust_5d_net": trust_5d,
        "inst_5d_total_net": total_5d,
        "foreign_today_net": foreign_today,
        "trust_today_net": trust_today,
        "inst_today_total_net": total_today,
        "inst_today_ok": foreign_today > 1_500_000 or trust_today > 1_000_000,
        "trust_buy_streak": trust_buy_streak,
        "foreign_buy_streak": foreign_buy_streak,
        "total_inst_buy_streak": total_inst_buy_streak,
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
    if item.get("prepare_turn_ok"):
        score += 14
        reasons.append("60K起漲雷達")
        prepare_info = item.get("prepare_turn_info") or {}
        if int(item.get("prepare_turn_priority") or 0) >= 2 or prepare_info.get("above_60m_zero"):
            score += 8
            reasons.append("60K雷達訊號在0軸上")
        if prepare_info.get("from_60m_support"):
            score += 5
            reasons.append("60K支撐後收復")
        daily_prepare_info = item.get("daily_prepare_info") or {}
        if daily_prepare_info.get("reclaimed_daily_ma5_today"):
            score += 8
            reasons.append("日K剛收復5日線")
        if daily_prepare_info.get("above_daily_ma10_or_20"):
            score += 8
            reasons.append("日K站上10/20日線")
        if daily_prepare_info.get("daily_prepare_momentum_ok"):
            score += 5
            reasons.append("日K動能改善")
        if item.get("daily_trend_ok"):
            score += 8
            reasons.append("日K多頭保護")
        if item.get("weekly_macd_ok"):
            score += 5
            reasons.append("周K在0軸上")
    if item.get("breakout_ok") and item.get("short_entry_ok") and int(item.get("short_entry_priority") or 0) >= 2:
        score += 25
        reasons.append("日K突破與60K共振")

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
        if item.get("category") != "5K早盤當沖雷達股" and stop_risk_exceeds_swing_baseline(risk):
            if SWING_STOP_BASELINE_WARNING_TEXT not in warnings:
                warnings.append(SWING_STOP_BASELINE_WARNING_TEXT)

    turnover = float(item.get("turnover") or 0)
    if turnover >= 300_000_000:
        score += 10
        reasons.append("成交金額充足")
    elif turnover < 100_000_000:
        score -= 15
        warnings.append("成交金額不足")

    last_close = float(item.get("last_close") or 0)
    if 0 < last_close < 20:
        score -= 4
        warnings.append("低價股波動較高")

    inst_net = int(item.get("inst_5d_total_net") or 0)
    if inst_net > 0:
        score += 8
        reasons.append("法人近期偏買")
    trust_buy_streak = int(item.get("trust_buy_streak") or 0)
    trust_today_ratio = float(item.get("trust_today_ratio") or 0)
    if int(item.get("trust_5d_net") or 0) > 0:
        score += 6
        reasons.append("投信買盤支撐")
    if trust_buy_streak >= 3:
        score += 12
        reasons.append("投信連買")
    elif trust_buy_streak >= 2:
        score += 8
        reasons.append("投信連買")
    if int(item.get("foreign_buy_streak") or 0) >= 3:
        score += 4
        reasons.append("外資連買")
    if int(item.get("total_inst_buy_streak") or 0) >= 3:
        score += 6
        reasons.append("三大法人連買")
    if trust_today_ratio >= 5:
        score += 12
        reasons.append("投信認養比重高")
    elif trust_today_ratio >= 3:
        score += 6
        reasons.append("投信認養比重提高")

    if item.get("reclaim_ok"):
        score += 16
        reasons.append("跌破均線後重新收復")
        reclaim_info = item.get("reclaim_info") or {}
        if reclaim_info.get("reclaimed_today"):
            score += 6
            reasons.append("今日剛站回月線")
        if float(reclaim_info.get("volume_ratio_5d") or 0) >= 1.3:
            score += 5
            reasons.append("量能明顯放大")
        if reclaim_info.get("hist_cross_red"):
            score += 5
            reasons.append("MACD綠柱翻紅")
        if reclaim_info.get("above_season_line") and reclaim_info.get("ma60_rising"):
            score += 8
            reasons.append("站上季線且季線翻揚")
    elif item.get("support_ok"):
        score += 8
        reasons.append("回檔支撐轉強")
    if item.get("breakout_ok"):
        score += 8
        reasons.append("整理後再突破")
    ma_cluster_info = item.get("daily_ma_cluster_info") or {}
    if ma_cluster_info.get("ma_cluster_breakout_2pct"):
        bonus = 10 if "relay_breakout" in category_keys or "precision_entry" in category_keys else 8
        score += bonus
        reasons.append("日K均線糾結突破2%")

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
    priority = [
        "跌破均線後重新收復",
        "今日剛站回月線",
        "MACD綠柱翻紅",
        "站上季線且季線翻揚",
        "日K突破與60K共振",
        "60K起漲雷達",
        "60K雷達訊號在0軸上",
        "60K支撐後收復",
        "日K剛收復5日線",
        "日K站上10/20日線",
        "日K動能改善",
        "日K多頭保護",
        "60K剛轉強",
        "60K位於0軸上",
        "周K在0軸上",
        "同時符合多類訊號",
        "整理後再突破",
        "日K均線糾結突破2%",
        "投信連買",
        "投信認養比重高",
        "投信認養比重提高",
        "停損距離漂亮",
        "停損可控",
        "量能明顯放大",
        "成交金額充足",
        "法人近期偏買",
        "投信買盤支撐",
        "三大法人連買",
        "外資連買",
    ]
    ordered = sorted(
        reasons,
        key=lambda value: priority.index(value) if value in priority else len(priority),
    )
    main = "、".join(ordered[:3]) if ordered else "型態符合短線候選條件"
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
        raise RuntimeError(
            "FINMIND_TOKEN is empty. Fill it in .env for local runs, or add FINMIND_TOKEN "
            "to GitHub Actions Secrets for cloud runs."
        )
    info = finmind_get("TaiwanStockInfo", token=cfg.finmind_token)
    info = info[
        info["type"].isin(["twse", "tpex"])
        & info["stock_id"].str.fullmatch(r"\d{4}", na=False)
        & ~info["industry_category"].isin(["ETF", "大盤", "Index", "所有證券"])
    ].drop_duplicates(subset=["stock_id"]).copy()
    collect_mother_universe_snapshots(info)
    universe = build_universe_by_yahoo_volume(info, cfg)
    if cfg.max_stocks > 0:
        universe = universe.head(cfg.max_stocks)
    return universe


def get_mother_universe(cfg: Config) -> pd.DataFrame:
    """Unfiltered listed common-stock mother universe for the T-1 A-pool build."""
    if not cfg.finmind_token:
        raise RuntimeError("FINMIND_TOKEN is required to build the A5-N premarket pool.")
    info = finmind_get("TaiwanStockInfo", token=cfg.finmind_token)
    info = info[
        info["type"].isin(["twse", "tpex"])
        & info["stock_id"].str.fullmatch(r"\d{4}", na=False)
        & ~info["industry_category"].isin(["ETF", "大盤", "Index", "所有證券"])
    ].drop_duplicates(subset=["stock_id"]).copy()
    if cfg.max_stocks > 0:
        info = info.head(cfg.max_stocks)
    return info


def a5n_official_daytrade_eligibility(stock_id: str, cfg: Config, scan_date: dt.date) -> tuple[bool, dict[str, Any]]:
    """Use FinMind's exchange-sourced same-day premarket day-trading list."""
    end = scan_date + dt.timedelta(days=1)
    frame = finmind_get("TaiwanStockDayTrading", token=cfg.finmind_token, data_id=stock_id,
                        start_date=scan_date.isoformat(), end_date=end.isoformat())
    if frame.empty:
        return False, {"source": "FinMind/TaiwanStockDayTrading", "date": scan_date.isoformat(),
                       "reason": "NOT_IN_OFFICIAL_DAYTRADE_LIST"}
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    same_day = frame[frame["date"].dt.date == scan_date]
    if same_day.empty:
        return False, {"source": "FinMind/TaiwanStockDayTrading", "date": scan_date.isoformat(),
                       "reason": "OFFICIAL_LIST_DATE_MISSING"}
    latest = same_day.iloc[-1]
    return True, {"source": "FinMind/TaiwanStockDayTrading", "date": scan_date.isoformat(),
                  "BuyAfterSale": str(latest.get("BuyAfterSale", "")),
                  "note": "BuyAfterSale=* still permits buy-then-sell; live bid/ask spread unavailable"}


def build_a5n_weekly_fixed_pool(
    cfg: Config, as_of: dt.datetime | None = None, anchor_date: str | None = None,
) -> list[dict[str, Any]]:
    """Build Friday T-1 fixed pool for the following Monday-Friday."""
    now = as_of or dt.datetime.now(TAIPEI_TZ)
    anchor = pd.Timestamp(anchor_date or pd.Timestamp(now).date())
    if anchor.weekday() != 4:
        raise ValueError(f"Fixed-pool anchor must be Friday, got {anchor.date()}")
    if pd.Timestamp(now).date() < anchor.date():
        raise ValueError("Fixed-pool anchor cannot be in the future")
    mother = get_mother_universe(cfg)
    qualified: list[dict[str, Any]] = []
    audit: list[dict[str, Any]] = []
    shadow_qualified: list[dict[str, Any]] = []
    shadow_audit: list[dict[str, Any]] = []
    run_id = str(uuid.uuid4())
    for i, (_, source) in enumerate(mother.iterrows(), start=1):
        stock_id, market_type = str(source["stock_id"]), str(source.get("type", ""))
        print(f"[fixed-pool {i}/{len(mother)}] {stock_id} {source.get('stock_name','')}")
        base = {"run_id": run_id, "built_at": pd.Timestamp(now).isoformat(),
                "stock_id": stock_id, "stock_name": str(source.get("stock_name", "")),
                "market_type": market_type}
        try:
            daily = get_yahoo_daily(stock_id, market_type, cfg)
            # Cheap technical precheck first; official per-stock API only for
            # names that pass price, liquidity and momentum.
            probe = evaluate_fixed_pool_candidate(
                daily, anchor_date=anchor, add_indicators=add_indicators,
                official_daytrade_ok=True,
                official_status={"reason": "PRECHECK_BEFORE_OFFICIAL_LOOKUP"},
            )
            base_ok = all(probe.get("gates", {}).get(k, {}).get("passed", False)
                          for k in ("F1_PRICE_BAND", "F2_LIQUIDITY"))
            if base_ok:
                eligible, status = a5n_official_daytrade_eligibility(stock_id, cfg, anchor.date())
                probe = evaluate_fixed_pool_candidate(
                    daily, anchor_date=anchor, add_indicators=add_indicators,
                    official_daytrade_ok=eligible, official_status=status)
            record = {**base, "fixed_pool": probe}
            audit.append(record)
            if probe.get("passed"):
                qualified.append(record)
            shadow_probe = json.loads(json.dumps(probe, ensure_ascii=False, default=str))
            shadow_probe["strategy_version"] = A5_N_FIXED_POOL_VERSION + "-momentum-rank-shadow"
            shadow_probe["parameter_status"] = "research_shadow_momentum_rank_only"
            shadow_probe["shadow_only"] = True
            shadow_probe["ntfy_eligible"] = False
            shadow_probe["momentum_required"] = False
            shadow_probe["passed"] = bool(base_ok and probe.get("gates", {}).get("F3_OFFICIAL_STATUS", {}).get("passed"))
            shadow_probe["reject_reason"] = [x for x in probe.get("reject_reason", []) if x != "F4_OR_F5_MOMENTUM"]
            shadow_record = {**base, "fixed_pool": shadow_probe}
            shadow_audit.append(shadow_record)
            if shadow_probe["passed"]:
                shadow_qualified.append(shadow_record)
        except Exception as exc:
            audit.append({**base, "fixed_pool": {"strategy_version": A5_N_FIXED_POOL_VERSION,
                "passed": False, "reject_reason": [f"FIXED_BUILD_ERROR:{exc}"]}})
            shadow_audit.append({**base, "fixed_pool": {"strategy_version": A5_N_FIXED_POOL_VERSION + "-momentum-rank-shadow",
                "shadow_only": True, "ntfy_eligible": False, "passed": False,
                "reject_reason": [f"FIXED_BUILD_ERROR:{exc}"]}})
    qualified.sort(key=fixed_pool_rank_key, reverse=True)
    pre_cap_count = len(qualified)
    cap_applied = pre_cap_count > int(A5_N_FIXED_POOL_CONFIG["hard_cap_trigger_count"])
    kept = qualified[:int(A5_N_FIXED_POOL_CONFIG["hard_cap_count"])] if cap_applied else qualified
    shadow_qualified.sort(key=fixed_pool_rank_key, reverse=True)
    shadow_pre_cap_count = len(shadow_qualified)
    shadow_cap_applied = shadow_pre_cap_count > int(A5_N_FIXED_POOL_CONFIG["hard_cap_trigger_count"])
    shadow_kept = shadow_qualified[:int(A5_N_FIXED_POOL_CONFIG["hard_cap_count"])] if shadow_cap_applied else shadow_qualified
    valid_from = anchor + pd.Timedelta(days=3)
    valid_through = valid_from + pd.Timedelta(days=4)
    reject_counts: dict[str, int] = {}
    for rec in audit:
        for reason in rec.get("fixed_pool", {}).get("reject_reason", []):
            reject_counts[reason] = reject_counts.get(reason, 0) + 1
    gate_counts = {key: sum(bool(x.get("fixed_pool", {}).get("gates", {}).get(key, {}).get("passed")) for x in audit)
                   for key in ("F1_PRICE_BAND", "F2_LIQUIDITY", "F3_OFFICIAL_STATUS", "F4_MOMENTUM_A", "F5_MOMENTUM_B")}
    # F3 is an expensive official lookup performed only after technical gates;
    # do not count the PRECHECK placeholder as an official pass.
    gate_counts["F3_OFFICIAL_STATUS"] = sum(
        bool(x.get("fixed_pool", {}).get("gates", {}).get("F3_OFFICIAL_STATUS", {}).get("passed"))
        and x.get("fixed_pool", {}).get("gates", {}).get("F3_OFFICIAL_STATUS", {}).get("raw", {}).get("reason") != "PRECHECK_BEFORE_OFFICIAL_LOOKUP"
        for x in audit)
    official_lookup_count = sum(
        "F3_OFFICIAL_STATUS" in x.get("fixed_pool", {}).get("gates", {})
        and x.get("fixed_pool", {}).get("gates", {}).get("F3_OFFICIAL_STATUS", {}).get("raw", {}).get("reason") != "PRECHECK_BEFORE_OFFICIAL_LOOKUP"
        for x in audit)
    payload = {"strategy_version": A5_N_FIXED_POOL_VERSION,
        "parameter_status": A5_N_FIXED_POOL_CONFIG["parameter_status"],
        "built_at": pd.Timestamp(now).isoformat(), "anchor_date": str(anchor.date()),
        "valid_from": str(valid_from.date()), "valid_through": str(valid_through.date()),
        "data_cutoff_rule": "completed daily bars through Friday close only",
        "official_status_note": "Friday official status; revalidated on the 09:31 scan date",
        "config": A5_N_FIXED_POOL_CONFIG, "mother_count": len(mother),
        "evaluated_count": len(audit), "qualified_count_before_cap": pre_cap_count,
        "kept_count": len(kept), "cap_applied": cap_applied,
        "below_target_warning": len(kept) < int(A5_N_FIXED_POOL_CONFIG["target_min_count"]),
        "above_target_warning": len(kept) > int(A5_N_FIXED_POOL_CONFIG["target_max_count"]),
        "gate_pass_counts": gate_counts, "official_lookup_count": official_lookup_count,
        "reject_reason_counts": reject_counts,
        "candidates": kept}
    LEDGER_DIR.mkdir(parents=True, exist_ok=True)
    A5_N_FIXED_POOL_PATH.write_text(json.dumps(payload, ensure_ascii=False, default=str, indent=2), encoding="utf-8")
    def replace_same_anchor(path: Path, records: list[dict[str, Any]]) -> None:
        retained: list[str] = []
        if path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                try:
                    old = json.loads(line)
                    old_anchor = old.get("fixed_pool", {}).get("anchor_date")
                    if old_anchor != str(anchor.date()):
                        retained.append(line)
                except json.JSONDecodeError:
                    retained.append(line)
        retained.extend(json.dumps(x, ensure_ascii=False, default=str) for x in records)
        path.write_text("\n".join(retained) + "\n", encoding="utf-8")

    replace_same_anchor(A5_N_FIXED_POOL_LEDGER_PATH, audit)
    shadow_payload = {**payload,
        "strategy_version": A5_N_FIXED_POOL_VERSION + "-momentum-rank-shadow",
        "parameter_status": "research_shadow_momentum_rank_only",
        "shadow_only": True, "ntfy_enabled": False, "momentum_required": False,
        "qualified_count_before_cap": shadow_pre_cap_count,
        "kept_count": len(shadow_kept), "cap_applied": shadow_cap_applied,
        "below_target_warning": len(shadow_kept) < int(A5_N_FIXED_POOL_CONFIG["target_min_count"]),
        "above_target_warning": len(shadow_kept) > int(A5_N_FIXED_POOL_CONFIG["target_max_count"]),
        "candidates": shadow_kept}
    A5_N_FIXED_SHADOW_POOL_PATH.write_text(json.dumps(shadow_payload, ensure_ascii=False, default=str, indent=2), encoding="utf-8")
    replace_same_anchor(A5_N_FIXED_SHADOW_POOL_LEDGER_PATH, shadow_audit)
    print(f"[fixed-pool] qualified={pre_cap_count} kept={len(kept)} valid={valid_from.date()}..{valid_through.date()}")
    print(f"[fixed-pool-momentum-rank-shadow] qualified={shadow_pre_cap_count} kept={len(shadow_kept)} ntfy=false")
    return kept


def load_a5n_fixed_pool_universe(cfg: Config) -> pd.DataFrame:
    if not A5_N_FIXED_POOL_PATH.exists():
        raise RuntimeError(f"A5-N fixed pool missing: {A5_N_FIXED_POOL_PATH}")
    payload = json.loads(A5_N_FIXED_POOL_PATH.read_text(encoding="utf-8"))
    today = pd.Timestamp.now(tz=TAIPEI_TZ).date() if not cfg.report_date else pd.Timestamp(cfg.report_date).date()
    if not (pd.Timestamp(payload["valid_from"]).date() <= today <= pd.Timestamp(payload["valid_through"]).date()):
        raise RuntimeError(f"A5-N fixed pool not valid on {today}: {payload['valid_from']}..{payload['valid_through']}")
    rows = []
    for x in payload.get("candidates", []):
        # Friday eligibility is not trusted for a later scan.  This lookup is
        # fail-closed, covering delisting/status changes between refreshes.
        eligible, status = a5n_official_daytrade_eligibility(str(x["stock_id"]), cfg, today)
        if not eligible:
            continue
        rows.append({"stock_id": x["stock_id"], "stock_name": x["stock_name"],
            "type": x["market_type"], "Trading_Volume": x["fixed_pool"]["ranking"]["average_volume_20d_shares"],
            "a5n_daytrade_eligible": True, "a5n_candidate_source": "A5_N_FIXED_POOL",
            "a5n_fixed_qualification": x["fixed_pool"], "a5n_current_official_status": status})
    return pd.DataFrame(rows)


def run_a5n_fixed_momentum_rank_shadow_scan(
    cfg: Config, as_of: dt.datetime | None = None,
) -> list[dict[str, Any]]:
    """Run the expanded momentum-as-ranking pool; never enters formal ntfy."""
    if not A5_N_FIXED_SHADOW_POOL_PATH.exists():
        raise RuntimeError(f"A5-N fixed shadow pool missing: {A5_N_FIXED_SHADOW_POOL_PATH}")
    payload = json.loads(A5_N_FIXED_SHADOW_POOL_PATH.read_text(encoding="utf-8"))
    now = as_of or dt.datetime.now(TAIPEI_TZ)
    today = pd.Timestamp(now).date()
    if not (pd.Timestamp(payload["valid_from"]).date() <= today <= pd.Timestamp(payload["valid_through"]).date()):
        raise RuntimeError(f"A5-N fixed shadow pool not valid on {today}")
    run_id = str(uuid.uuid4())
    rows: list[dict[str, Any]] = []
    for item in payload.get("candidates", []):
        stock_id, market_type = str(item["stock_id"]), str(item["market_type"])
        try:
            eligible, status = a5n_official_daytrade_eligibility(stock_id, cfg, today)
            if not eligible:
                rows.append({"stock_id": stock_id, "stock_name": item.get("stock_name", ""),
                    "strategy_state": "REJECTED", "reject_reason": ["F3_CURRENT_DAY_OFFICIAL_STATUS"],
                    "official_daytrade_eligibility": status})
                continue
            daily = get_yahoo_daily(stock_id, market_type, cfg)
            hourly = get_yahoo_intraday(stock_id, market_type, cfg)
            five = get_yahoo_5m_intraday(stock_id, market_type, cfg)
            qualification = item.get("fixed_pool", {})
            row = pd.Series({"stock_id": stock_id, "stock_name": item.get("stock_name", ""),
                "type": market_type, "last_close": qualification.get("gates", {}).get("F1_PRICE_BAND", {}).get("raw", {}).get("close", 0),
                "Trading_Volume": qualification.get("ranking", {}).get("average_volume_20d_shares", 0)})
            result = evaluate_a5n(row=row, daily=daily, hourly=hourly, five_min=five,
                as_of=now, add_indicators=add_indicators, keep_completed_5m=keep_completed_5m_bars,
                daytrade_ok=True, daytrade_reasons=[], max_price=cfg.max_price,
                min_volume_shares=cfg.daytrade_min_volume_shares,
                min_turnover=cfg.daytrade_min_turnover, daily_prequalified=qualification)
            result.update({"stock_id": stock_id, "stock_name": item.get("stock_name", ""),
                "market_type": market_type, "run_id": run_id,
                "scan_started_at": pd.Timestamp(now).isoformat(),
                "strategy_version": A5_N_FIXED_POOL_VERSION + "-momentum-rank-shadow",
                "shadow_only": True, "ntfy_eligible": False})
            rows.append(result)
        except Exception as exc:
            rows.append({"stock_id": stock_id, "stock_name": item.get("stock_name", ""),
                "run_id": run_id, "scan_started_at": pd.Timestamp(now).isoformat(),
                "strategy_version": A5_N_FIXED_POOL_VERSION + "-momentum-rank-shadow",
                "strategy_state": "REJECTED", "shadow_only": True, "ntfy_eligible": False,
                "reject_reason": [f"FIXED_SHADOW_SCAN_ERROR:{exc}"]})
    LEDGER_DIR.mkdir(parents=True, exist_ok=True)
    with A5_N_FIXED_SHADOW_SIGNAL_LEDGER_PATH.open("a", encoding="utf-8") as fh:
        for result in rows:
            fh.write(json.dumps(result, ensure_ascii=False, default=str) + "\n")
    counts = pd.Series([x.get("strategy_state") for x in rows]).value_counts().to_dict()
    print(f"[fixed-pool-momentum-rank-shadow-scan] count={len(rows)} states={counts} ntfy=false")
    return rows


def build_a5n_premarket_pool(cfg: Config, as_of: dt.datetime | None = None) -> list[dict[str, Any]]:
    """Build and persist the ranked A pool from completed T-1 daily bars only."""
    now = as_of or dt.datetime.now(TAIPEI_TZ)
    mother = get_mother_universe(cfg)
    candidates: list[dict[str, Any]] = []
    audit_rows: list[dict[str, Any]] = []
    b_candidates: list[dict[str, Any]] = []
    b_audit_rows: list[dict[str, Any]] = []
    build_run_id = str(uuid.uuid4())
    for i, (_, source) in enumerate(mother.iterrows(), start=1):
        stock_id, market_type = str(source["stock_id"]), str(source.get("type", ""))
        print(f"[A-pool {i}/{len(mother)}] {stock_id} {source.get('stock_name','')}")
        try:
            daily = get_yahoo_daily(stock_id, market_type, cfg)
            if daily.empty:
                empty_base = {"run_id": build_run_id, "scan_started_at": pd.Timestamp(now).isoformat(),
                    "stock_id": stock_id, "stock_name": str(source.get("stock_name", "")),
                    "market_type": market_type, "strategy_state": "REJECTED",
                    "reject_reason": ["A_DATA_EMPTY"]}
                audit_rows.append(empty_base)
                b_audit_rows.append({**empty_base, "strategy_version": A5_N_B_VERSION,
                    "shadow_only": True, "ntfy_eligible": False})
                continue
            t1 = daily[pd.to_datetime(daily["date"]).dt.date < pd.Timestamp(now).date()]
            last = t1.iloc[-1] if not t1.empty else None
            row = pd.Series({**source.to_dict(), "last_close": float(last["close"]) if last is not None else 0,
                "Trading_Volume": float(last["Trading_Volume"]) if last is not None else 0})
            # The existing eligibility source has no official disposition/spread fields;
            # this limitation is retained explicitly instead of inventing availability.
            probe = evaluate_a5n(row=row, daily=daily, hourly=pd.DataFrame(columns=["date"]),
                five_min=pd.DataFrame(columns=["date"]), as_of=now, add_indicators=add_indicators,
                keep_completed_5m=keep_completed_5m_bars, daytrade_ok=True,
                daytrade_reasons=["PRECHECK_BEFORE_OFFICIAL_DAYTRADE_LOOKUP"],
                max_price=cfg.max_price, min_volume_shares=cfg.daytrade_min_volume_shares,
                min_turnover=cfg.daytrade_min_turnover)
            if all(probe.get("A", {}).get(k, {}).get("passed", False) for k in ("A1", "A2", "A5")):
                eligible, eligibility = a5n_official_daytrade_eligibility(stock_id, cfg, pd.Timestamp(now).date())
                probe = evaluate_a5n(row=row, daily=daily, hourly=pd.DataFrame(columns=["date"]),
                    five_min=pd.DataFrame(columns=["date"]), as_of=now, add_indicators=add_indicators,
                    keep_completed_5m=keep_completed_5m_bars, daytrade_ok=eligible,
                    daytrade_reasons=[] if eligible else [str(eligibility.get("reason"))],
                    max_price=cfg.max_price, min_volume_shares=cfg.daytrade_min_volume_shares,
                    min_turnover=cfg.daytrade_min_turnover)
                probe["official_daytrade_eligibility"] = eligibility
            probe.update({"stock_id": stock_id, "stock_name": str(source.get("stock_name", "")),
                "market_type": market_type, "run_id": build_run_id, "scan_started_at": pd.Timestamp(now).isoformat()})
            audit_rows.append(probe)
            if all(probe.get("A", {}).get(k, {}).get("passed", False) for k in ("A1", "A2", "A5")):
                candidates.append(probe)
            b_probe = evaluate_a5n(row=row, daily=daily, hourly=pd.DataFrame(columns=["date"]),
                five_min=pd.DataFrame(columns=["date"]), as_of=now, add_indicators=add_indicators,
                keep_completed_5m=keep_completed_5m_bars, daytrade_ok=True,
                daytrade_reasons=["PRECHECK_BEFORE_OFFICIAL_DAYTRADE_LOOKUP"],
                max_price=cfg.max_price, min_volume_shares=cfg.daytrade_min_volume_shares,
                min_turnover=cfg.daytrade_min_turnover, config=A5_N_B_CONFIG)
            if all(b_probe.get("A", {}).get(k, {}).get("passed", False) for k in ("A1", "A2", "A5")):
                b_eligible, b_eligibility = a5n_official_daytrade_eligibility(stock_id, cfg, pd.Timestamp(now).date())
                b_probe = evaluate_a5n(row=row, daily=daily, hourly=pd.DataFrame(columns=["date"]),
                    five_min=pd.DataFrame(columns=["date"]), as_of=now, add_indicators=add_indicators,
                    keep_completed_5m=keep_completed_5m_bars, daytrade_ok=b_eligible,
                    daytrade_reasons=[] if b_eligible else [str(b_eligibility.get("reason"))],
                    max_price=cfg.max_price, min_volume_shares=cfg.daytrade_min_volume_shares,
                    min_turnover=cfg.daytrade_min_turnover, config=A5_N_B_CONFIG)
                b_probe["official_daytrade_eligibility"] = b_eligibility
            b_probe.update({"stock_id": stock_id, "stock_name": str(source.get("stock_name", "")),
                "market_type": market_type, "run_id": build_run_id, "scan_started_at": pd.Timestamp(now).isoformat(),
                "strategy_version": A5_N_B_VERSION, "shadow_only": True, "ntfy_eligible": False})
            b_audit_rows.append(b_probe)
            if all(b_probe.get("A", {}).get(k, {}).get("passed", False) for k in ("A1", "A2", "A5")):
                b_candidates.append(b_probe)
        except Exception as exc:
            print(f"[A-pool-skip] {stock_id}: {exc}", file=sys.stderr)
            audit_rows.append({"run_id": build_run_id, "scan_started_at": pd.Timestamp(now).isoformat(),
                "stock_id": stock_id, "stock_name": str(source.get("stock_name", "")),
                "market_type": market_type, "strategy_state": "REJECTED",
                "reject_reason": [f"A_BUILD_ERROR:{exc}"]})
            b_audit_rows.append({"run_id": build_run_id, "scan_started_at": pd.Timestamp(now).isoformat(),
                "stock_id": stock_id, "stock_name": str(source.get("stock_name", "")),
                "market_type": market_type, "strategy_state": "REJECTED", "shadow_only": True,
                "strategy_version": A5_N_B_VERSION, "reject_reason": [f"A_BUILD_ERROR:{exc}"]})
    candidates.sort(key=a5n_rank_key, reverse=True)
    kept = candidates[:int(A5_N_CONFIG["a_pool_size"])]
    b_candidates.sort(key=a5n_rank_key, reverse=True)
    b_kept = b_candidates[:int(A5_N_B_CONFIG["a_pool_size"])]
    payload = {"strategy_version": A5_N_VERSION, "parameter_status": A5_N_CONFIG["parameter_status"],
        "built_at": pd.Timestamp(now).isoformat(), "data_cutoff_rule": "strictly before scan date (T-1)",
        "config": A5_N_CONFIG, "mother_count": len(mother), "qualified_count": len(candidates),
        "kept_count": len(kept), "evaluated_count": len(audit_rows),
        "missing_count": len(mother) - len(audit_rows), "candidates": kept}
    LEDGER_DIR.mkdir(parents=True, exist_ok=True)
    A5_N_POOL_PATH.write_text(json.dumps(payload, ensure_ascii=False, default=str, indent=2), encoding="utf-8")
    b_payload = {"strategy_version": A5_N_B_VERSION, "parameter_status": A5_N_B_CONFIG["parameter_status"],
        "built_at": pd.Timestamp(now).isoformat(), "data_cutoff_rule": "strictly before scan date (T-1)",
        "config": A5_N_B_CONFIG, "mother_count": len(mother), "qualified_count": len(b_candidates),
        "kept_count": len(b_kept), "evaluated_count": len(b_audit_rows),
        "missing_count": len(mother) - len(b_audit_rows), "shadow_only": True,
        "ntfy_enabled": False, "candidates": b_kept}
    A5_N_B_POOL_PATH.write_text(json.dumps(b_payload, ensure_ascii=False, default=str, indent=2), encoding="utf-8")
    with A5_N_PREMARKET_LEDGER_PATH.open("a", encoding="utf-8") as fh:
        for record in audit_rows:
            fh.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
    with A5_N_B_PREMARKET_LEDGER_PATH.open("a", encoding="utf-8") as fh:
        for record in b_audit_rows:
            fh.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
    print(f"[A-pool] qualified={len(candidates)} kept={len(kept)} path={A5_N_POOL_PATH}")
    print(f"[B-shadow-pool] qualified={len(b_candidates)} kept={len(b_kept)} path={A5_N_B_POOL_PATH}")
    return kept


def run_a5n_b_shadow_scan(cfg: Config, as_of: dt.datetime | None = None) -> list[dict[str, Any]]:
    """Evaluate B-variant candidates only; never returns rows to formal results/ntfy."""
    if not A5_N_B_POOL_PATH.exists():
        raise RuntimeError(f"A5-N B shadow pool missing: {A5_N_B_POOL_PATH}")
    payload = json.loads(A5_N_B_POOL_PATH.read_text(encoding="utf-8"))
    now = as_of or dt.datetime.now(TAIPEI_TZ)
    if pd.Timestamp(payload["built_at"]).date() != pd.Timestamp(now).date():
        raise RuntimeError(f"A5-N B shadow pool is stale: {payload['built_at']}")
    run_id = str(uuid.uuid4())
    rows: list[dict[str, Any]] = []
    for item in payload.get("candidates", []):
        stock_id, market_type = str(item["stock_id"]), str(item["market_type"])
        try:
            daily = get_yahoo_daily(stock_id, market_type, cfg)
            hourly = get_yahoo_intraday(stock_id, market_type, cfg)
            five = get_yahoo_5m_intraday(stock_id, market_type, cfg)
            a5 = item.get("A", {}).get("A5", {}).get("raw", {})
            row = pd.Series({"stock_id": stock_id, "stock_name": item.get("stock_name", ""),
                "type": market_type, "last_close": a5.get("price", 0),
                "Trading_Volume": a5.get("median_volume_20d", 0)})
            result = evaluate_a5n(row=row, daily=daily, hourly=hourly, five_min=five,
                as_of=now, add_indicators=add_indicators, keep_completed_5m=keep_completed_5m_bars,
                daytrade_ok=bool(item.get("official_daytrade_eligibility")), daytrade_reasons=[],
                max_price=cfg.max_price, min_volume_shares=cfg.daytrade_min_volume_shares,
                min_turnover=cfg.daytrade_min_turnover, config=A5_N_B_CONFIG)
            result.update({"stock_id": stock_id, "stock_name": item.get("stock_name", ""),
                "market_type": market_type, "run_id": run_id, "scan_started_at": pd.Timestamp(now).isoformat(),
                "strategy_version": A5_N_B_VERSION, "shadow_only": True, "ntfy_eligible": False})
            rows.append(result)
        except Exception as exc:
            rows.append({"stock_id": stock_id, "stock_name": item.get("stock_name", ""),
                "run_id": run_id, "scan_started_at": pd.Timestamp(now).isoformat(),
                "strategy_version": A5_N_B_VERSION, "strategy_state": "REJECTED",
                "shadow_only": True, "ntfy_eligible": False, "reject_reason": [f"B_SHADOW_SCAN_ERROR:{exc}"]})
    with A5_N_B_SIGNAL_LEDGER_PATH.open("a", encoding="utf-8") as fh:
        for result in rows:
            fh.write(json.dumps(result, ensure_ascii=False, default=str) + "\n")
    counts = pd.Series([x.get("strategy_state") for x in rows]).value_counts().to_dict()
    print(f"[B-shadow-scan] count={len(rows)} states={counts} ntfy=false")
    return rows


def load_a5n_premarket_universe(cfg: Config) -> pd.DataFrame:
    if not A5_N_POOL_PATH.exists():
        raise RuntimeError(f"A5-N premarket pool missing: {A5_N_POOL_PATH}")
    payload = json.loads(A5_N_POOL_PATH.read_text(encoding="utf-8"))
    built = pd.Timestamp(payload["built_at"])
    today = pd.Timestamp.now(tz=TAIPEI_TZ).date() if not cfg.report_date else pd.Timestamp(cfg.report_date).date()
    if built.date() != today:
        raise RuntimeError(f"A5-N premarket pool is stale: built_at={built}, expected={today}")
    rows = [{"stock_id": x["stock_id"], "stock_name": x["stock_name"], "type": x["market_type"],
             "Trading_Volume": x.get("A", {}).get("A5", {}).get("raw", {}).get("median_volume_20d", 0),
             "a5n_daytrade_eligible": bool(x.get("official_daytrade_eligibility"))}
            for x in payload.get("candidates", [])]
    return pd.DataFrame(rows)


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
                volume_floor = min(cfg.min_volume_shares, cfg.daytrade_min_volume_shares)
                if volume < volume_floor:
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
    intraday: pd.DataFrame | None = None
    try:
        daily = get_yahoo_daily(stock_id, market_type, cfg)
        if cfg.only_short_entry:
            return screen_short_entry_only(row, cfg, daily)

        reclaim_ok, reclaim_info = elite_reclaim_setup(daily)
        daily_macd_ok = daily_common_gate(daily)
        daily_trend_ok, daily_trend_info = daily_trend_protection_ok(daily)
        daily_prepare_ok, daily_prepare_info = daily_prepare_turn_gate_ok(daily)
        daily_daytrade_ok, daily_daytrade_info = daily_daytrade_protection_ok(daily)
        daily_ma_cluster_info = daily_ma_cluster_breakout_info(daily)
        weekly_macd_ok = weekly_macd_above_zero_from_daily(daily)

        stop = calculate_stop_loss(daily, cfg)
        if stop["stop_loss"] is None:
            empty_states = strategy_state_map(
                filter_ok=False,
                relay_filter_ok=False,
                precision_filter_ok=False,
                reclaim_ok=False,
                support_ok=False,
                kd_pullback_ok=False,
                daily_macd_ok=False,
                breakout_ok=False,
                daily_prepare_ok=False,
                prepare_turn_ok=False,
                short_entry_ok=False,
                daily_daytrade_ok=False,
                daytrade_direction_ok=False,
                five_k_ok=False,
                intraday_volume_ok=False,
                extreme_daytrade_ok=False,
            )
            collect_stock_ledgers(
                row=row,
                cfg=cfg,
                daily=daily,
                intraday=None,
                states=empty_states,
                stop=stop,
                relay_stop=None,
                filter_reasons=["STOP_LOSS_UNAVAILABLE"],
                relay_filter_reasons=["STOP_LOSS_UNAVAILABLE"],
                precision_filter_reasons=["STOP_LOSS_UNAVAILABLE"],
                five_k_info={},
                prepare_turn_info={},
                short_entry_reason="",
                prepare_turn_reason="",
                extreme_daytrade_info={},
                data_quality_flags=["STOP_LOSS_UNAVAILABLE"],
            )
            return {}
        filter_ok, filter_reasons = common_trade_filter_ok(daily, cfg, row, stop)
        relay_stop = calculate_relay_stop_loss(daily, cfg)
        relay_filter_ok, relay_filter_reasons = common_trade_filter_ok(
            daily,
            cfg,
            row,
            relay_stop,
            cfg.relay_max_stop_loss_risk_pct,
        )
        precision_filter_ok, precision_filter_reasons = common_trade_filter_ok(
            daily,
            cfg,
            row,
            stop,
            cfg.precision_max_stop_loss_risk_pct,
        )
        support_ok = support_pullback_ok(daily)
        breakout_ok = breakout_platform_ok(daily)

        kd_pullback_ok = False
        short_entry_ok = False
        short_entry_reason = ""
        short_entry_priority = 0
        prepare_turn_ok = False
        prepare_turn_reason = ""
        prepare_turn_priority = 0
        prepare_turn_info: dict[str, Any] = {}
        extreme_daytrade_ok = False
        extreme_daytrade_reason = ""
        extreme_daytrade_priority = 0
        extreme_daytrade_info: dict[str, Any] = {}
        daytrade_direction_ok = False
        daytrade_direction_info: dict[str, Any] = {}
        five_k_ok = False
        five_k_reason = ""
        five_k_priority = 0
        five_k_info: dict[str, Any] = {}
        intraday_volume_ok = False
        daytrade_ok, daytrade_reasons = daytrade_filter_ok(row, cfg, stop)
        if "a5n_daytrade_eligible" in row and not bool(row.get("a5n_daytrade_eligible")):
            daytrade_ok = False
            daytrade_reasons.append("NOT_IN_PREMARKET_OFFICIAL_DAYTRADE_LIST")
        if cfg.enable_intraday_check:
            intraday = get_yahoo_intraday(stock_id, market_type, cfg)
            if intraday_session_is_current(intraday, cfg):
                kd_pullback_ok = intraday_kd_low_golden_cross(intraday)
                short_entry_ok, short_entry_reason, short_entry_priority = intraday_short_entry_signal(
                    intraday
                )
                prepare_turn_ok, prepare_turn_reason, prepare_turn_priority, prepare_turn_info = (
                    intraday_prepare_turn_signal(intraday)
                )
                daytrade_direction_ok, daytrade_direction_info = intraday_60k_daytrade_direction_ok(
                    intraday
                )
                five_min = get_yahoo_5m_intraday(stock_id, market_type, cfg)
                intraday_volume_ok = (
                    int(five_k_info.get("intraday_volume_shares") or 0)
                    >= cfg.daytrade_min_volume_shares
                )
                a5n_row = row.copy()
                a5n_row["last_close"] = stop["last_close"]
                extreme_daytrade_info = evaluate_a5n(
                        row=a5n_row, daily=daily, hourly=intraday, five_min=five_min,
                        as_of=dt.datetime.now(TAIPEI_TZ), add_indicators=add_indicators,
                        keep_completed_5m=keep_completed_5m_bars,
                        daytrade_ok=daytrade_ok, daytrade_reasons=daytrade_reasons,
                        max_price=cfg.max_price,
                        min_volume_shares=cfg.daytrade_min_volume_shares,
                        min_turnover=cfg.daytrade_min_turnover,
                        daily_prequalified=(row.get("a5n_fixed_qualification")
                            if row.get("a5n_candidate_source") == "A5_N_FIXED_POOL" else None),
                )
                extreme_daytrade_info.update({
                        "stock_id": str(stock_id), "stock_name": str(stock_name),
                        "market_type": str(market_type),
                })
                A5_N_RUN_ROWS.append(extreme_daytrade_info)
                extreme_daytrade_ok = extreme_daytrade_info.get("strategy_state") == "ENTRY_VALIDATED"
                daily_daytrade_ok = bool(
                        extreme_daytrade_info.get("candidate_source") == "A5_N_FIXED_POOL"
                        or all(extreme_daytrade_info.get("A", {}).get(k, {}).get("passed", False)
                               for k in ("A1", "A2", "A5")))
                b = extreme_daytrade_info.get("B", {})
                daytrade_direction_ok = bool(
                        b.get("B1", {}).get("passed") and b.get("B2", {}).get("passed")
                        and (b.get("B3", {}).get("passed") or b.get("B4", {}).get("passed"))
                    )
                five_k_ok = extreme_daytrade_ok
                intraday_volume_ok = bool(
                        extreme_daytrade_info.get("A", {}).get("A5", {}).get("passed", False)
                    )
                extreme_daytrade_reason = "平台突破後首次回測驗證"
                extreme_daytrade_priority = sum(
                        int(v.get("passed", False))
                        for layer in ("A", "B", "C")
                        for v in extreme_daytrade_info.get(layer, {}).values()
                    )

        states = strategy_state_map(
            filter_ok=filter_ok,
            relay_filter_ok=relay_filter_ok,
            precision_filter_ok=precision_filter_ok,
            reclaim_ok=reclaim_ok,
            support_ok=support_ok,
            kd_pullback_ok=kd_pullback_ok,
            daily_macd_ok=daily_macd_ok,
            breakout_ok=breakout_ok,
            daily_prepare_ok=daily_prepare_ok,
            prepare_turn_ok=prepare_turn_ok,
            short_entry_ok=short_entry_ok,
            daily_daytrade_ok=daily_daytrade_ok,
            daytrade_direction_ok=daytrade_direction_ok,
            five_k_ok=five_k_ok,
            intraday_volume_ok=intraday_volume_ok,
            extreme_daytrade_ok=extreme_daytrade_ok,
        )
        collect_stock_ledgers(
            row=row,
            cfg=cfg,
            daily=daily,
            intraday=intraday,
            states=states,
            stop=stop,
            relay_stop=relay_stop,
            filter_reasons=filter_reasons,
            relay_filter_reasons=relay_filter_reasons,
            precision_filter_reasons=precision_filter_reasons,
            five_k_info=five_k_info,
            prepare_turn_info=prepare_turn_info,
            short_entry_reason=short_entry_reason,
            prepare_turn_reason=prepare_turn_reason,
            extreme_daytrade_info=extreme_daytrade_info,
        )
        normal_signal_possible = daily_macd_ok or reclaim_ok or daily_trend_ok or daily_prepare_ok
        normal_filter_possible = filter_ok or relay_filter_ok or precision_filter_ok
        if not ((normal_signal_possible and normal_filter_possible) or extreme_daytrade_ok):
            return {}

        foreign_net = 0
        trust_net = 0
        inst_total_net = 0
        today_foreign_net = 0
        today_trust_net = 0
        today_inst_total_net = 0
        inst_today_ok = False
        trust_buy_streak = 0
        foreign_buy_streak = 0
        total_inst_buy_streak = 0
        try:
            inst = institutional_signals(stock_id, cfg)
            foreign_net = inst["foreign_5d_net"]
            trust_net = inst["trust_5d_net"]
            inst_total_net = inst["inst_5d_total_net"]
            today_foreign_net = inst["foreign_today_net"]
            today_trust_net = inst["trust_today_net"]
            today_inst_total_net = inst["inst_today_total_net"]
            inst_today_ok = inst["inst_today_ok"]
            trust_buy_streak = inst["trust_buy_streak"]
            foreign_buy_streak = inst.get("foreign_buy_streak", 0)
            total_inst_buy_streak = inst.get("total_inst_buy_streak", 0)
        except Exception as exc:
            print(f"[chip-warn] {stock_id} institutional data unavailable: {exc}", file=sys.stderr)

        daily_pct = price_change_pct(daily)
        gain_3d = recent_gain_pct(daily, 3)
        ma20_dist = ma20_distance_pct(daily)
        trust_today_ratio = (
            today_trust_net / float(row["Trading_Volume"]) * 100
            if float(row["Trading_Volume"]) > 0
            else 0.0
        )

        base = {
            "stock_id": stock_id,
            "stock_name": stock_name,
            "industry_category": str(row.get("industry_category", "") or ""),
            "market_type": market_type,
            "last_close": stop["last_close"],
            "stop_loss": stop["stop_loss"],
            "stop_loss_risk_pct": stop["stop_loss_risk_pct"],
            "stop_loss_method": stop["stop_loss_method"],
            "volume_lots": round(row["Trading_Volume"] / 1000),
            "daily_pct": daily_pct,
            "gain_3d_pct": gain_3d,
            "ma20_distance_pct": ma20_dist,
            "support_ok": support_ok,
            "reclaim_ok": reclaim_ok,
            "reclaim_info": reclaim_info,
            "daily_trend_ok": daily_trend_ok,
            "daily_trend_info": daily_trend_info,
            "daily_prepare_ok": daily_prepare_ok,
            "daily_prepare_info": daily_prepare_info,
            "daily_daytrade_ok": daily_daytrade_ok,
            "daily_daytrade_info": daily_daytrade_info,
            "daily_ma_cluster_info": daily_ma_cluster_info,
            "weekly_macd_ok": weekly_macd_ok,
            "daily_macd_ok": daily_macd_ok,
            "kd_pullback_ok": kd_pullback_ok,
            "breakout_ok": breakout_ok,
            "prepare_turn_ok": prepare_turn_ok,
            "prepare_turn_reason": prepare_turn_reason,
            "prepare_turn_priority": prepare_turn_priority,
            "prepare_turn_info": prepare_turn_info,
            "short_entry_ok": short_entry_ok,
            "short_entry_reason": short_entry_reason,
            "short_entry_priority": short_entry_priority,
            "foreign_5d_net": foreign_net,
            "trust_5d_net": trust_net,
            "inst_5d_total_net": inst_total_net,
            "foreign_today_net": today_foreign_net,
            "trust_today_net": today_trust_net,
            "inst_today_total_net": today_inst_total_net,
            "trust_buy_streak": trust_buy_streak,
            "foreign_buy_streak": foreign_buy_streak,
            "total_inst_buy_streak": total_inst_buy_streak,
            "trust_today_ratio": trust_today_ratio,
            "turnover": float(row["Trading_Volume"]) * stop["last_close"],
            "filter_reasons": filter_reasons,
            "relay_filter_reasons": relay_filter_reasons,
            "precision_filter_reasons": precision_filter_reasons,
            "extreme_daytrade_ok": extreme_daytrade_ok,
            "extreme_daytrade_reason": extreme_daytrade_reason,
            "extreme_daytrade_priority": extreme_daytrade_priority,
            "extreme_daytrade_info": extreme_daytrade_info,
            "daytrade_filter_reasons": daytrade_reasons,
        }

        categories: dict[str, dict[str, Any]] = {}
        relay_base = {
            **base,
            "stop_loss": relay_stop["stop_loss"],
            "stop_loss_risk_pct": relay_stop["stop_loss_risk_pct"],
            "stop_loss_method": relay_stop["stop_loss_method"],
            "filter_reasons": relay_filter_reasons,
        }
        precision_base = {
            **base,
            "filter_reasons": precision_filter_reasons,
        }

        if filter_ok and (reclaim_ok or (support_ok and kd_pullback_ok)):
            categories["strong_continuation"] = {
                **base,
                "category": "均線收復轉強股",
                "subtype": "跌破均線後重新站回5/10/20日線" if reclaim_ok else "回檔支撐型",
            }
        if relay_filter_ok and daily_macd_ok and breakout_ok:
            categories["relay_breakout"] = {
                **relay_base,
                "category": "中繼再漲股",
                "subtype": "平台突破型",
            }
        if filter_ok and daily_prepare_ok and prepare_turn_ok:
            categories["prepare_turn"] = {
                **base,
                "category": A3_OBSERVATION_CATEGORY,
                "subtype": prepare_turn_reason,
            }
        if precision_filter_ok and daily_macd_ok and short_entry_ok:
            categories["precision_entry"] = {
                **precision_base,
                "category": "60K精準翻紅股",
                "subtype": short_entry_reason,
            }
        if extreme_daytrade_ok:
            categories["extreme_daytrade"] = {
                **base,
                "category": "A5-N平台蓄勢—早盤突破回測",
                "subtype": extreme_daytrade_reason,
                "signal_price": extreme_daytrade_info.get("signal_price") or base["last_close"],
                "entry_price_rule": A5_SIGNAL_PRICE_RULE,
                "signal_timestamp": extreme_daytrade_info.get("signal_timestamp"),
                "last_completed_k_timestamp": extreme_daytrade_info.get("last_completed_5k_timestamp"),
                "stop_loss": extreme_daytrade_info.get("structural_stop")
                or base["stop_loss"],
                "stop_loss_risk_pct": extreme_daytrade_info.get("stop_risk_pct")
                or base["stop_loss_risk_pct"],
                "stop_loss_method": "A5-N首次回測結構防守",
            }
        for category_item in categories.values():
            attach_swing_stop_baseline_warning(category_item)
        return categories
    except Exception as exc:
        collect_stock_failure_snapshot(row, cfg, stage="SCREEN_STOCK_EXCEPTION", error_text=str(exc))
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

    intraday = get_yahoo_intraday(stock_id, market_type, cfg)
    if not intraday_session_is_current(intraday, cfg):
        return {}
    short_entry_ok, short_entry_reason, short_entry_priority = intraday_short_entry_signal(intraday)
    if not short_entry_ok:
        return {}

    stop = calculate_stop_loss(daily, cfg)
    if stop["stop_loss"] is None:
        return {}
    filter_ok, _ = common_trade_filter_ok(daily, cfg, row, stop, cfg.precision_max_stop_loss_risk_pct)
    if not filter_ok:
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
        "industry_category": str(row.get("industry_category", "") or ""),
        "market_type": market_type,
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
        "category": "60K精準翻紅股",
        "subtype": short_entry_reason,
    }
    return {"precision_entry": item}


CATEGORY_TITLES = {
    "strong_continuation": "第一類：均線收復轉強股（空翻多精英型）",
    "relay_breakout": "第二類：中繼再漲股（平台突破型）",
    "prepare_turn": f"第三類：{A3_OBSERVATION_CATEGORY}",
    "precision_entry": "第四類：60K精準翻紅股",
    "extreme_daytrade": "第五類：A5-N平台蓄勢—早盤突破回測【研究測試】",
}

LIMITED_CATEGORY_COUNTS = {
    "strong_continuation": 3,
    "relay_breakout": 3,
    "prepare_turn": 3,
    "precision_entry": 3,
    "extreme_daytrade": int(A5_N_CONFIG["max_ntfy_entries_per_scan"]),
    "shortlist": 9,
}


def is_friday(report_date: str) -> bool:
    try:
        return dt.date.fromisoformat(report_date).weekday() == 4
    except ValueError:
        return False


def trade_journal_columns() -> list[str]:
    return [
        "選股日期",
        "排名",
        "股票代號",
        "股名",
        "市場別",
        "所屬類別",
        "進場價",
        "停損價",
        "短線分數",
        "操作理由",
    ]


def load_trade_journal() -> pd.DataFrame:
    if not TRADE_JOURNAL_PATH.exists():
        return pd.DataFrame(columns=trade_journal_columns())
    try:
        return pd.read_csv(TRADE_JOURNAL_PATH, dtype={"股票代號": str})
    except Exception as exc:
        print(f"[journal-warn] cannot read {TRADE_JOURNAL_PATH}: {exc}", file=sys.stderr)
        return pd.DataFrame(columns=trade_journal_columns())


def record_top3_journal(shortlist: list[dict[str, Any]], cfg: Config) -> None:
    report_date = cfg_date(cfg)
    rows = []
    for rank, row in enumerate(shortlist[:3], start=1):
        rows.append(
            {
                "選股日期": report_date,
                "排名": rank,
                "股票代號": str(row.get("stock_id", "")),
                "股名": str(row.get("stock_name", "")),
                "市場別": str(row.get("market_type", "")),
                "所屬類別": "、".join(row.get("category_names", [row.get("category", "")])),
                "進場價": format_number(row.get("last_close")),
                "停損價": format_number(row.get("stop_loss")),
                "短線分數": format_integer(row.get("short_score")),
                "操作理由": str(row.get("top_reason", "")),
            }
        )

    if not rows:
        return

    current = load_trade_journal()
    next_df = pd.DataFrame(rows, columns=trade_journal_columns())
    if not current.empty:
        current = current[
            ~(
                (current["選股日期"].astype(str) == report_date)
                & (current["排名"].astype(str).isin(["1", "2", "3"]))
            )
        ]
        next_df = pd.concat([current, next_df], ignore_index=True)
    next_df.to_csv(TRADE_JOURNAL_PATH, index=False, encoding="utf-8-sig")


def weekly_review_dates(report_date: str) -> list[str]:
    anchor = dt.date.fromisoformat(report_date)
    this_monday = anchor - dt.timedelta(days=anchor.weekday())
    dates = [
        this_monday - dt.timedelta(days=4),  # last Thursday
        this_monday - dt.timedelta(days=3),  # last Friday
        this_monday,
        this_monday + dt.timedelta(days=1),
        this_monday + dt.timedelta(days=2),
    ]
    return [day.isoformat() for day in dates]


def history_after_entry(stock_id: str, market_type: str, entry_date: str, end_date: str) -> pd.DataFrame:
    start = dt.date.fromisoformat(entry_date) + dt.timedelta(days=1)
    end = dt.date.fromisoformat(end_date) + dt.timedelta(days=1)
    candidates = [yahoo_symbol(stock_id, market_type), f"{stock_id}.TW", f"{stock_id}.TWO"]
    for symbol in dict.fromkeys(candidates):
        try:
            raw = yf.download(
                symbol,
                start=start.isoformat(),
                end=end.isoformat(),
                interval="1d",
                auto_adjust=False,
                progress=False,
                threads=False,
            )
            hist = normalize_yahoo_history(raw)
            if not hist.empty:
                return hist
        except Exception as exc:
            print(f"[backtest-warn] {symbol}: {exc}", file=sys.stderr)
    return pd.DataFrame()


def evaluate_trade_path(
    stock_id: str,
    market_type: str,
    entry_date: str,
    end_date: str,
    entry_price: float,
) -> dict[str, Any]:
    target_price = entry_price * 1.06
    stop_price = entry_price * 0.97
    hist = history_after_entry(stock_id, market_type, entry_date, end_date)
    if hist.empty:
        return {
            "status": "資料不足",
            "max_high": None,
            "min_low": None,
            "latest_close": None,
            "hit_date": "",
            "return_pct": None,
        }

    high_col = "high" if "high" in hist.columns else "max"
    low_col = "low" if "low" in hist.columns else "min"
    if high_col not in hist.columns or low_col not in hist.columns or "close" not in hist.columns:
        return {
            "status": "資料欄位不足",
            "max_high": None,
            "min_low": None,
            "latest_close": None,
            "hit_date": "",
            "return_pct": None,
        }

    max_high = float(hist[high_col].max())
    min_low = float(hist[low_col].min())
    latest_close = float(hist.iloc[-1]["close"])
    status = "尚未觸發"
    hit_date = ""
    for _, bar in hist.iterrows():
        high_hit = float(bar[high_col]) >= target_price
        low_hit = float(bar[low_col]) <= stop_price
        bar_date = str(pd.to_datetime(bar["date"]).date())
        if high_hit and low_hit:
            status = "同日觸及，需人工判斷"
            hit_date = bar_date
            break
        if high_hit:
            status = "獲利達標 6%"
            hit_date = bar_date
            break
        if low_hit:
            status = "觸及停損 3%"
            hit_date = bar_date
            break

    return {
        "status": status,
        "max_high": max_high,
        "min_low": min_low,
        "latest_close": latest_close,
        "hit_date": hit_date,
        "return_pct": (latest_close / entry_price - 1) * 100,
    }


def build_weekly_backtest(cfg: Config) -> tuple[str, str]:
    report_date = cfg_date(cfg)
    if not is_friday(report_date):
        return "", ""

    journal = load_trade_journal()
    title = "本週策略勝率與達標率總體檢報告"
    if journal.empty:
        text = f"{title}\n目前尚無足夠 Top 3 選股紀錄可回測。"
        html = f"<section class='card'><h3>{title}</h3><p>目前尚無足夠 Top 3 選股紀錄可回測。</p></section>"
        return text, html

    review_dates = set(weekly_review_dates(report_date))
    pool = journal[journal["選股日期"].astype(str).isin(review_dates)].copy()
    if pool.empty:
        text = f"{title}\n本週回看區間尚無紀錄：{', '.join(sorted(review_dates))}"
        html = (
            f"<section class='card'><h3>{title}</h3>"
            f"<p>本週回看區間尚無紀錄：{escape(', '.join(sorted(review_dates)))}</p></section>"
        )
        return text, html

    rows = []
    for _, record in pool.iterrows():
        try:
            entry_price = float(record["進場價"])
            result = evaluate_trade_path(
                str(record["股票代號"]),
                str(record.get("市場別", "")),
                str(record["選股日期"]),
                report_date,
                entry_price,
            )
            rows.append(
                {
                    "選股日期": record["選股日期"],
                    "排名": record["排名"],
                    "股票代號": record["股票代號"],
                    "股名": record["股名"],
                    "進場價": format_number(entry_price),
                    "區間最高價": format_number(result["max_high"]),
                    "區間最低價": format_number(result["min_low"]),
                    "最新收盤價": format_number(result["latest_close"]),
                    "狀態": result["status"],
                    "觸發日期": result["hit_date"],
                    "目前報酬%": format_number(result["return_pct"]),
                }
            )
        except Exception as exc:
            rows.append(
                {
                    "選股日期": record.get("選股日期", ""),
                    "排名": record.get("排名", ""),
                    "股票代號": record.get("股票代號", ""),
                    "股名": record.get("股名", ""),
                    "進場價": record.get("進場價", ""),
                    "區間最高價": "",
                    "區間最低價": "",
                    "最新收盤價": "",
                    "狀態": f"回測失敗：{exc}",
                    "觸發日期": "",
                    "目前報酬%": "",
                }
            )

    df = pd.DataFrame(rows)
    total = len(df)
    wins = int((df["狀態"] == "獲利達標 6%").sum()) if total else 0
    stops = int((df["狀態"] == "觸及停損 3%").sum()) if total else 0
    win_rate = wins / total * 100 if total else 0
    stop_rate = stops / total * 100 if total else 0
    text = "\n".join(
        [
            title,
            f"回測樣本：{total} 筆，6% 達標：{wins} 筆，3% 停損：{stops} 筆。",
            f"達標率：{win_rate:.1f}%，停損率：{stop_rate:.1f}%。",
            dataframe_to_markdown(df),
        ]
    )
    styled = df.copy()
    styled["狀態"] = styled["狀態"].map(format_backtest_status)
    html = (
        f"<section class='card'><h3>{title}</h3>"
        f"<p>回測樣本：{total} 筆，<span class='hit'>6% 達標：{wins} 筆（{win_rate:.1f}%）</span>，"
        f"<span class='risk'>3% 停損：{stops} 筆（{stop_rate:.1f}%）</span>。</p>"
        f"{styled.to_html(index=False, border=0, escape=False, classes='report-table')}</section>"
    )
    return text, html


def format_backtest_status(value: Any) -> str:
    text = str(value)
    if "獲利達標" in text:
        return f"<span class='hit'>{escape(text)}</span>"
    if "停損" in text:
        return f"<span class='risk'>{escape(text)}</span>"
    return escape(text)


def request_html(url: str, *, method: str = "GET", data: dict[str, Any] | None = None) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
        ),
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.7",
    }
    try:
        if method.upper() == "POST":
            resp = requests.post(url, data=data or {}, headers=headers, timeout=15)
        else:
            resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or "utf-8"
        return resp.text
    except Exception as exc:
        print(f"[event-warn] {url}: {exc}", file=sys.stderr)
        return ""


def fetch_mops_material_events(stock_id: str, report_date: str) -> list[str]:
    try:
        day = dt.date.fromisoformat(report_date)
    except ValueError:
        day = dt.date.today()
    roc_year = str(day.year - 1911)
    payload = {
        "encodeURIComponent": "1",
        "step": "1",
        "firstin": "1",
        "off": "1",
        "keyword4": "",
        "code1": "",
        "TYPEK2": "",
        "checkbtn": "",
        "queryName": "co_id",
        "inpuType": "co_id",
        "TYPEK": "all",
        "co_id": stock_id,
        "year": roc_year,
        "month": f"{day.month:02d}",
        "day": f"{day.day:02d}",
    }
    html = request_html("https://mops.twse.com.tw/mops/web/ajax_t05st02", method="POST", data=payload)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    if any(marker in text for marker in ("查無", "無符合", "No data")):
        return []
    rows = []
    for tr in soup.select("tr"):
        cells = [cell.get_text(" ", strip=True) for cell in tr.select("td")]
        joined = " ".join(cells)
        if stock_id in joined and len(joined) > 20:
            rows.append(joined[:180])
    if rows:
        return rows[:3]
    if stock_id in text and len(text) > 30:
        return [text[:180]]
    return []


def fetch_yahoo_event_headlines(stock_id: str, stock_name: str) -> list[str]:
    html = ""
    for suffix in ("TW", "TWO"):
        html = request_html(f"https://tw.stock.yahoo.com/quote/{stock_id}.{suffix}/news")
        if html:
            break
    if not html:
        query = parse.quote(f"{stock_id} {stock_name} 法說會 除權息 重大訊息")
        html = request_html(f"https://tw.stock.yahoo.com/search?p={query}")
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    headlines: list[str] = []
    keywords = ("法說", "除權", "除息", "重大訊息", "處置", "注意", "財報", "減資", "庫藏股")
    for node in soup.find_all(["h3", "a", "span"]):
        text = node.get_text(" ", strip=True)
        if len(text) < 8 or len(text) > 90:
            continue
        if (stock_id in text or stock_name in text) and any(keyword in text for keyword in keywords):
            if text not in headlines:
                headlines.append(text)
        if len(headlines) >= 3:
            break
    return headlines


def build_event_alerts(candidates: list[dict[str, Any]], cfg: Config) -> tuple[str, str]:
    title = "🚨 警示！候選股近期大事"
    if not candidates:
        text = f"{title}\n今日無候選股可檢查。"
        html = f"<section class='card alert-card'><h3>{title}</h3><p>今日無候選股可檢查。</p></section>"
        return text, html

    rows = []
    report_date = cfg_date(cfg)
    checked: set[str] = set()
    for item in candidates[:12]:
        stock_id = str(item.get("stock_id", ""))
        if not stock_id or stock_id in checked:
            continue
        checked.add(stock_id)
        stock_name = str(item.get("stock_name", ""))
        alerts: list[str] = []
        alerts.extend(fetch_mops_material_events(stock_id, report_date))
        if not alerts:
            alerts.extend(fetch_yahoo_event_headlines(stock_id, stock_name))
        rows.append(
            {
                "股票代號": stock_id,
                "股名": stock_name,
                "警示內容": "；".join(alerts) if alerts else "目前未偵測到重大訊息、法說會或除權息關鍵警示",
            }
        )
        time.sleep(0.2)

    df = pd.DataFrame(rows, columns=["股票代號", "股名", "警示內容"])
    text = f"{title}\n{dataframe_to_markdown(df)}"
    html_df = df.copy()
    html_df["警示內容"] = html_df["警示內容"].map(
        lambda value: (
            f"<span class='risk'>{escape(str(value))}</span>"
            if "未偵測" not in str(value)
            else escape(str(value))
        )
    )
    html = (
        f"<section class='card alert-card'><h3>{title}</h3>"
        "<p class='muted'>以公開資訊觀測站重大訊息為主，Yahoo 股市新聞關鍵字為備援；警示僅作為盤前風控提醒。</p>"
        f"{html_df.to_html(index=False, border=0, escape=False, classes='report-table')}</section>"
    )
    return text, html


def minimalist_html_start(report_date: str, total: int) -> list[str]:
    return [
        "<html><head><meta charset='utf-8'>",
        "<style>",
        "body{font-family:'Microsoft JhengHei','Noto Sans TC',Arial,sans-serif;background:#f7f4ef;color:#24302f;margin:0;padding:24px;line-height:1.65;}",
        ".wrap{max-width:980px;margin:0 auto;}",
        "h2{font-size:28px;margin:0 0 8px;color:#1d2d2b;}",
        "h3{font-size:19px;margin:0 0 12px;color:#24302f;}",
        ".subtitle,.muted{color:#6f7a76;font-size:14px;}",
        ".card{background:#fffdfa;border:1px solid #eadfce;border-radius:14px;padding:18px 20px;margin:18px 0;box-shadow:0 6px 18px rgba(73,58,38,.06);}",
        ".alert-card{border-color:#e7b8b2;background:#fff8f6;}",
        ".report-table{width:100%;border-collapse:collapse;font-size:14px;background:white;}",
        ".report-table th{background:#efe7da;color:#38433f;text-align:left;padding:10px;border-bottom:1px solid #ddcfbd;}",
        ".report-table td{padding:10px;border-bottom:1px solid #eee5d9;vertical-align:top;}",
        ".rank-one{background:#fff2c2;border-radius:999px;padding:2px 8px;font-weight:700;color:#765b00;}",
        ".hit{color:#b23b3b;font-weight:700;}",
        ".risk{color:#b72f2f;font-weight:700;}",
        ".note{background:#eef6f4;border-left:4px solid #7ea89a;padding:10px 12px;border-radius:8px;color:#40504c;}",
        "</style></head><body><div class='wrap'>",
        f"<h2>台股短線精選報告 - {report_date}</h2>",
        f"<p class='subtitle'>本次共篩出 {total} 筆分類結果。若遇休市，資料來源可能回傳最近一個交易日。</p>",
        "<p class='note'>策略紀律：波段目標以 6% 至 8% 為主、停損 3% 至 4%；當沖／極短線目標以 4% 至 6% 為主、停損 2% 至 3%。停損防守線需優先於期待報酬。</p>",
        f"<p class='note'>Strategy Baseline: {MASTER_STRATEGY_SPEC_VERSION}<br>"
        f"Tech Registry: {TECH_PARAM_REGISTRY_VERSION}<br>"
        "Daily/60K MACD 8/17/9 | KDJ 10/4/4<br>"
        "5K MACD 12/26/9 | KDJ 9/3/3</p>",
    ]


def format_report(
    results: dict[str, list[dict[str, Any]]],
    cfg: Config,
    event_sections: tuple[str, str] | None = None,
    weekly_sections: tuple[str, str] | None = None,
) -> tuple[str, str]:
    report_date = cfg_date(cfg)
    subject = f"台股短線精選報告 - {report_date}｜{MASTER_STRATEGY_SPEC_VERSION} 新參數重掃"
    total = sum(len(items) for items in results.values())
    text_sections: list[str] = [
        f"台股短線精選報告日期：{report_date}，本次共篩出 {total} 筆分類結果。",
        "提醒：若今日為週末、國定假日、颱風休市或市場未交易，資料來源可能回傳最近一個交易日的最新可取得資料。",
    ]
    html_sections: list[str] = minimalist_html_start(report_date, total)
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

    if event_sections:
        event_text, event_html = event_sections
        if event_text:
            text_sections.append(event_text)
        if event_html:
            html_sections.append(event_html)

    if weekly_sections:
        weekly_text, weekly_html = weekly_sections
        if weekly_text:
            text_sections.append(weekly_text)
        if weekly_html:
            html_sections.append(weekly_html)

    html_sections.append("</div></body></html>")
    return subject, "\n\n".join(text_sections) + "\n\nHTML_TABLE:\n" + "\n".join(html_sections)


def summarize_status_error(error_text: str) -> tuple[str, str, str]:
    lowered = error_text.lower()
    if "finmind_token is empty" in lowered:
        return (
            "GitHub Actions 尚未設定 FINMIND_TOKEN",
            "請到 GitHub 倉庫 Settings > Secrets and variables > Actions 新增 FINMIND_TOKEN。",
            "雲端不會讀取本機 .env，所以本機能跑不代表 GitHub Actions 能抓 FinMind 資料。",
        )
    if "smtp" in lowered or "authentication" in lowered:
        return (
            "Email SMTP 驗證或寄送失敗",
            "請確認 GitHub Actions Secrets 中的 GMAIL_USER 與 GMAIL_PASSWORD 是否正確。",
            "若 Gmail 應用程式密碼被重設或撤銷，雲端會無法寄出正式報告。",
        )
    if "yfinance" in lowered or "yahoo" in lowered:
        return (
            "Yahoo Finance 資料源暫時無回應",
            "通常稍後重新執行即可；若遇休市或資料延遲，當日可能只有狀態通知。",
            "這類問題多半不是策略失效，而是外部行情資料尚未完整更新。",
        )
    if "finmind" in lowered or "http 400" in lowered or "http 429" in lowered:
        return (
            "FinMind 資料源或免費額度異常",
            "請稍後重新執行，或確認 FinMind Token 仍有效且未超過免費額度。",
            "籌碼、營收或股東分級資料依賴 FinMind，該資料源異常時會影響正式報告。",
        )
    if "no data" in lowered or "empty" in lowered:
        return (
            "今日行情資料不足或尚未更新",
            "若今天是休市、國定假日、颱風停市或盤後資料尚未同步，收到狀態通知屬正常防呆。",
            "程式會避免產生錯誤表格，等下一次有完整資料時再寄正式報告。",
        )
    return (
        "排程已啟動，但正式報告未完成",
        "請到 GitHub Actions 查看最新 workflow log，或稍後再手動觸發一次 cron-job.org TEST RUN。",
        "程式已隱藏技術錯誤細節，避免 Email 出現亂碼或敏感資訊。",
    )


def format_status_report(error_text: str, cfg: Config) -> tuple[str, str]:
    report_date = cfg_date(cfg)
    subject = f"台股每日排程狀態通知 - {report_date}"
    safe_error = error_text
    finmind_token = os.getenv("FINMIND_TOKEN", "")
    if finmind_token:
        safe_error = safe_error.replace(finmind_token, "[hidden]")
    for secret_name in ("SMTP_PASSWORD", "GMAIL_PASSWORD", "LINE_NOTIFY_TOKEN"):
        secret_value = os.getenv(secret_name, "")
        if secret_value:
            safe_error = safe_error.replace(secret_value, "[hidden]")
    reason, action, note = summarize_status_error(safe_error)
    plain = "\n\n".join(
        [
            f"台股每日排程已於 {report_date} 啟動，但本次未能完成正式五大類選股報告。",
            f"原因判斷：{reason}",
            f"建議處理：{action}",
            f"補充說明：{note}",
        ]
    )
    html = f"""
<html><body style="font-family:'Microsoft JhengHei',Arial,sans-serif;line-height:1.7;color:#243042;">
<div style="max-width:760px;margin:0 auto;padding:24px;">
<h2 style="margin:0 0 16px;">台股每日排程狀態通知 - {report_date}</h2>
<p>今日排程已啟動，但本次未能完成正式五大類選股報告。</p>
<div style="border-left:4px solid #d97706;background:#fff7ed;padding:14px 16px;margin:18px 0;">
<p><strong>原因判斷：</strong>{escape(reason)}</p>
<p><strong>建議處理：</strong>{escape(action)}</p>
<p><strong>補充說明：</strong>{escape(note)}</p>
</div>
<p style="color:#64748b;font-size:13px;">本通知已自動隱藏技術錯誤與敏感資訊，不會再附上亂碼 traceback。</p>
</div>
</body></html>
"""
    return subject, plain + "\n\nHTML_TABLE:\n" + html


def format_category_section(title: str, rows: list[dict[str, Any]]) -> tuple[str, str]:
    if not rows:
        empty_df = empty_report_dataframe()
        text = f"{title}\n0 項\n{dataframe_to_markdown(empty_df)}"
        html_table = empty_df.to_html(index=False, border=0, escape=False, classes="report-table")
        html = f"<section class='card'><h3>{title}</h3><p>0 項</p>{html_table}</section>"
        return text, html

    df = report_display_dataframe(rows)
    text = f"{title}\n{len(rows)} 項\n{dataframe_to_markdown(df)}"
    html_table = df.to_html(index=False, border=0, escape=False, classes="report-table")
    html = f"<section class='card'><h3>{title}</h3><p>{len(rows)} 項</p>{html_table}</section>"
    return text, html


def format_shortlist_section(rows: list[dict[str, Any]]) -> tuple[str, str]:
    title = "今日短線精選排名（不含第五類當沖股）"
    if not rows:
        empty_df = pd.DataFrame(
            [{"排名": "今日無符合標的", "股票代號": "", "股名": "", "所屬類別": "", "短線分數": ""}],
            columns=["排名", "股票代號", "股名", "所屬類別", "短線分數"],
        )
        text = f"{title}\n0 項\n{dataframe_to_markdown(empty_df)}"
        html = f"<section class='card'><h3>{title}</h3><p>0 項</p>{empty_df.to_html(index=False, border=0, escape=False, classes='report-table')}</section>"
        return text, html
    df = shortlist_dataframe(rows)
    text = f"{title}\n{len(rows)} 項\n{dataframe_to_markdown(df)}"
    html_df = df.copy()
    if not html_df.empty:
        html_df["排名"] = html_df["排名"].astype(str)
        html_df.loc[html_df["排名"].astype(str) == "1", "排名"] = "<span class='rank-one'>1</span>"
    html = f"<section class='card'><h3>{title}</h3><p>{len(rows)} 項</p>{html_df.to_html(index=False, border=0, escape=False, classes='report-table')}</section>"
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
    html = f"<section class='card'><h3>{title}</h3>{df.to_html(index=False, border=0, escape=False, classes='report-table')}</section>"
    return text, html


def industry_topic_text(row: dict[str, Any]) -> str:
    industry = str(row.get("industry_category", "") or "").strip()
    if not industry:
        return "未分類"
    return industry.replace("　", " ").replace("業", "")


def trend_direction_text(row: dict[str, Any]) -> str:
    category = str(row.get("category", "") or "")
    parts: list[str] = []
    if category == "均線收復轉強股":
        if row.get("reclaim_ok"):
            parts.append("日K收復短均")
        else:
            parts.append("日K回檔支撐")
        if row.get("kd_pullback_ok"):
            parts.append("60K低檔轉強")
    elif category == "中繼再漲股":
        parts.extend(["日K平台突破", "量價轉強"])
    elif "60K起漲雷達" in category:
        parts.extend(["日K保護", "60K起漲觀察"])
    elif category == "60K精準翻紅股":
        parts.extend(["日K多方", "60K翻紅"])
    elif category == "5K早盤當沖雷達股":
        parts.extend(["日K多方", "5K觸發"])
    else:
        subtype = str(row.get("subtype", "") or "").strip()
        if subtype:
            parts.append(subtype)

    if row.get("weekly_macd_ok") and category != "5K早盤當沖雷達股":
        parts.append("周K順風")
    if (row.get("daily_ma_cluster_info") or {}).get("ma_cluster_breakout_2pct"):
        parts.append("均線糾結突破")
    if category != "5K早盤當沖雷達股" and stop_risk_exceeds_swing_baseline(
        row.get("stop_loss_risk_pct")
    ):
        parts.append(SWING_STOP_BASELINE_WARNING_TEXT)
    for warning in row.get("manual_review_warnings") or []:
        parts.append(str(warning))

    if not parts:
        return "趨勢待觀察"
    return "，".join(dict.fromkeys(parts))


def display_price_for_report(row: dict[str, Any]) -> Any:
    category = str(row.get("category", "") or "")
    if row.get("entry_price_rule") == A5_SIGNAL_PRICE_RULE or category.startswith("A5-N"):
        return row.get("signal_price") or row.get("last_close")
    return row.get("last_close")


def report_display_dataframe(rows: list[dict[str, Any]]) -> pd.DataFrame:
    clean_rows = []
    for row in rows:
        clean_rows.append(
            {
                "股票代號": str(row.get("stock_id", "")),
                "股名": str(row.get("stock_name", "")),
                "今日收盤價": format_number(display_price_for_report(row)),
                "今日成交量(張)": format_integer(row.get("volume_lots")),
                "建議停損價": format_number(row.get("stop_loss")),
                "產業/題材": industry_topic_text(row),
                "趨勢方向": trend_direction_text(row),
            }
        )
    return pd.DataFrame(
        clean_rows,
        columns=[
            "股票代號",
            "股名",
            "今日收盤價",
            "今日成交量(張)",
            "建議停損價",
            "產業/題材",
            "趨勢方向",
        ],
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
                "今日收盤價": format_number(display_price_for_report(row)),
                "今日成交量(張)": format_integer(row.get("volume_lots")),
                "建議停損價": format_number(row.get("stop_loss")),
                "產業/題材": industry_topic_text(row),
                "趨勢方向": trend_direction_text(row),
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
            "建議停損價",
            "產業/題材",
            "趨勢方向",
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
                "建議停損價": "",
                "產業/題材": "",
                "趨勢方向": "",
            }
        ],
        columns=[
            "股票代號",
            "股名",
            "今日收盤價",
            "今日成交量(張)",
            "建議停損價",
            "產業/題材",
            "趨勢方向",
        ],
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


def market_state(cfg: Config) -> dict[str, Any]:
    symbol = cfg.market_filter_symbol or "^TWII"
    state: dict[str, Any] = {
        "symbol": symbol,
        "ok": False,
        "reason": "大盤資料不足",
        "daily_pct": None,
        "intraday_pct": None,
    }
    try:
        daily_raw = yf.Ticker(symbol).history(period="5d", interval="1d", auto_adjust=False)
        daily = normalize_yahoo_history(daily_raw)
        if len(daily) >= 2:
            latest = float(daily.iloc[-1]["close"])
            previous = float(daily.iloc[-2]["close"])
            if previous > 0:
                state["daily_pct"] = (latest / previous - 1) * 100
        intraday_raw = yf.Ticker(symbol).history(period="1d", interval="5m", auto_adjust=False)
        intraday = normalize_yahoo_history(intraday_raw)
        if not intraday.empty:
            first = float(intraday.iloc[0]["open"])
            latest = float(intraday.iloc[-1]["close"])
            if first > 0:
                state["intraday_pct"] = (latest / first - 1) * 100
        daily_ok = state["daily_pct"] is not None and float(state["daily_pct"]) >= cfg.market_min_daily_pct
        intraday_ok = (
            state["intraday_pct"] is not None
            and float(state["intraday_pct"]) >= cfg.market_min_intraday_pct
        )
        state["ok"] = bool(daily_ok and intraday_ok)
        if state["ok"]:
            state["reason"] = "大盤狀態允許盤中推播"
        else:
            state["reason"] = (
                f"大盤偏弱或資料不足：日漲跌 {format_number(state['daily_pct'])}%、"
                f"盤中 {format_number(state['intraday_pct'])}%"
            )
    except Exception as exc:
        state["reason"] = f"大盤資料讀取失敗：{exc}"
    return state


def send_ntfy(message: str, cfg: Config, *, title: str = "TW Stock 60K Alert", priority: str = "4") -> bool:
    if not cfg.enable_ntfy_intraday_alerts:
        print("[ntfy] disabled by ENABLE_NTFY_INTRADAY_ALERTS")
        return False
    if not cfg.ntfy_topic:
        print("[ntfy] disabled or missing topic: NTFY_TOPIC is empty")
        return False
    topic = cfg.ntfy_topic.strip().strip("/")
    url = f"{cfg.ntfy_server.rstrip('/')}/{topic}"
    headers = {
        "Title": Header(title, "utf-8").encode(),
        "Priority": str(priority),
        "Tags": "chart_with_upwards_trend",
    }
    try:
        requests.post(
            url,
            data=message.encode("utf-8"),
            headers=headers,
            timeout=15,
        ).raise_for_status()
        print(f"[ntfy] sent to topic {topic[:4]}***")
        return True
    except Exception as exc:
        print(f"[ntfy-warn] send failed: {exc}", file=sys.stderr)
        return False


def format_intraday_ntfy_message(results: dict[str, list[dict[str, Any]]], market: dict[str, Any]) -> str:
    rows = []
    for key in ("precision_entry",):
        for item in results.get(key, []):
            rows.append(
                (
                    key,
                    str(item.get("stock_id", "")),
                    str(item.get("stock_name", "")),
                    float(display_price_for_report(item) or 0),
                    float(item.get("stop_loss") or 0),
                    int(item.get("short_score") or 0),
                    str(item.get("subtype", "")),
                )
            )
    if not rows:
        lines = [
            "台股60K盤中檢查完成",
            f"大盤：{market.get('symbol')} 日{format_number(market.get('daily_pct'))}% / 盤中{format_number(market.get('intraday_pct'))}%",
            "本次第四類（60K精準翻紅）沒有符合標的。",
            "這代表程式有正常執行，只是條件未觸發。",
        ]
        return "\n".join(lines)
    label = {"precision_entry": "第四類60K翻紅", "extreme_daytrade": "第五類5K當沖"}
    lines = [
        "台股60K盤中提醒",
        f"大盤：{market.get('symbol')} 日{format_number(market.get('daily_pct'))}% / 盤中{format_number(market.get('intraday_pct'))}%",
    ]
    for key, stock_id, stock_name, close, stop, score, subtype in rows[:8]:
        score_text = f" 分數{score}" if key != "extreme_daytrade" and score else ""
        lines.append(
            f"{label.get(key, key)}｜{stock_id} {stock_name}｜收{close:.2f}｜防守{stop:.2f}{score_text}｜{subtype}"
        )
    lines.append("僅供盤中觀察，仍需看委買賣、量能與大盤，不追高。")
    return "\n".join(lines)


def a5n_gate_summary(item: dict[str, Any]) -> str:
    labels = {"A": "日K", "B": "60分K", "C": "5分K"}
    parts = []
    for layer in ("A", "B", "C"):
        gates = item.get(layer, {})
        passed = sum(int(v.get("passed", False)) for v in gates.values())
        parts.append(f"{labels[layer]} {passed}/5")
    return "｜".join(parts)


A5N_STATE_ZH = {
    "DAILY_CANDIDATE": "日K候選",
    "HOURLY_CONFIRMED": "60分K確認",
    "BREAKOUT_DETECTED": "已突破，等待回測",
    "WAITING_PULLBACK": "等待首次回測",
    "ENTRY_VALIDATED": "進場條件成立",
    "EXPIRED": "訊號失效",
    "REJECTED": "條件未通過",
}

A5N_REASON_ZH = {
    "A1": "日K平台條件",
    "A2": "日K均線結構",
    "A5": "流動性或當沖資格",
    "B1": "60分K低點結構",
    "B2": "60分K均線結構",
    "B3_OR_B4": "60分K動能",
    "B_DATA_INSUFFICIENT": "60分K資料不足",
    "C_DATA_INSUFFICIENT": "5分K資料不足",
    "C_NOT_ENOUGH_COMPLETED_5K": "已完成5分K不足",
    "C1_NO_BREAKOUT": "尚未突破",
    "C2_WAITING_FIRST_PULLBACK": "等待首次回測",
    "C2": "回測未守穩",
    "C3": "短均線未轉強",
    "C4": "量價或MACD未確認",
    "C5": "時效、風險或報酬比複驗未過",
}


def a5n_reason_summary(item: dict[str, Any]) -> str:
    reasons = item.get("reject_reason") or []
    translated = [A5N_REASON_ZH.get(str(reason), "其他條件未通過") for reason in reasons]
    return "、".join(dict.fromkeys(translated)) if translated else "尚未觸發"


def format_a5n_ntfy_message(rows: list[dict[str, Any]]) -> str:
    latest_by_symbol: dict[str, dict[str, Any]] = {}
    for x in rows:
        symbol = str(x.get("stock_id") or "")
        if not symbol:
            continue
        current = latest_by_symbol.get(symbol)
        if current is None or (x.get("revalidation") and not current.get("revalidation")):
            latest_by_symbol[symbol] = x
    effective_rows = list(latest_by_symbol.values())
    validated = sorted([x for x in effective_rows if x.get("strategy_state") == "ENTRY_VALIDATED"], key=a5n_rank_key, reverse=True)
    max_entries = int(A5_N_CONFIG["max_ntfy_entries_per_scan"])
    selected_ids = {id(x) for x in validated[:max_entries]}
    for rank, x in enumerate(validated, start=1):
        x["notification_rank"] = rank
        x["notification_selected"] = id(x) in selected_ids
        x["notification_suppressed_reason"] = None if id(x) in selected_ids else "ENTRY_VALIDATED_OVER_SCAN_LIMIT"
    lines = ["📈 A5-N 第五類當沖測試"]
    if validated:
        for x in validated[:max_entries]:
            lines.append(
                f"符合進場｜{x.get('stock_id')} {x.get('stock_name')}｜"
                f"參考價 {x.get('signal_price')}｜防守 {x.get('structural_stop')}｜{a5n_gate_summary(x)}"
            )
        if len(validated) > max_entries:
            lines.append(f"另有 {len(validated)-max_entries} 檔合格但超過每次{max_entries}檔上限，已保留於Ledger。")
    else:
        lines.append("本次沒有符合進場條件的股票")
        ranked = sorted(effective_rows, key=a5n_rank_key, reverse=True)[:5]
        for x in ranked:
            state = A5N_STATE_ZH.get(str(x.get("strategy_state")), "觀察中")
            lines.append(f"接近｜{x.get('stock_id')} {x.get('stock_name')}｜{state}｜{a5n_gate_summary(x)}｜原因：{a5n_reason_summary(x)}")
    lines.append(f"每次最多通知 {max_entries} 檔｜僅供人工核對，非自動下單")
    return "\n".join(lines)


def write_a5n_ledger(ntfy_sent_at: str | None = None) -> None:
    if not A5_N_RUN_ROWS:
        return
    LEDGER_DIR.mkdir(parents=True, exist_ok=True)
    run_id = LEDGER_CONTEXT.get("execution_id")
    started = LEDGER_CONTEXT.get("started_at")
    with A5_N_LEDGER_PATH.open("a", encoding="utf-8") as fh:
        for row in A5_N_RUN_ROWS:
            payload = {"run_id": run_id, "scan_started_at": started, **row, "ntfy_sent_at": ntfy_sent_at}
            fh.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")


def revalidate_a5n_entries(results: dict[str, list[dict[str, Any]]], cfg: Config) -> None:
    """Re-fetch every provisional ENTRY_VALIDATED name immediately before ntfy."""
    provisional = list(results.get("extreme_daytrade", []))
    if not provisional:
        return
    kept: list[dict[str, Any]] = []
    for item in provisional:
        stock_id = str(item.get("stock_id", ""))
        market_type = str(item.get("market_type", ""))
        try:
            daily = get_yahoo_daily(stock_id, market_type, cfg)
            hourly = get_yahoo_intraday(stock_id, market_type, cfg)
            five = get_yahoo_5m_intraday(stock_id, market_type, cfg)
            raw_a5 = (item.get("extreme_daytrade_info", {}).get("A", {}).get("A5", {}).get("raw", {}))
            volume = float(raw_a5.get("volume") or item.get("volume_lots", 0) * 1000)
            row = pd.Series({
                "stock_id": stock_id, "stock_name": item.get("stock_name", ""),
                "type": market_type, "Trading_Volume": volume,
                "last_close": float(item.get("signal_price") or item.get("last_close") or 0),
            })
            stop = {"last_close": row["last_close"]}
            ok, reasons = daytrade_filter_ok(row, cfg, stop)
            checked = evaluate_a5n(
                row=row, daily=daily, hourly=hourly, five_min=five,
                as_of=dt.datetime.now(TAIPEI_TZ), add_indicators=add_indicators,
                keep_completed_5m=keep_completed_5m_bars, daytrade_ok=ok,
                daytrade_reasons=reasons, max_price=cfg.max_price,
                min_volume_shares=cfg.daytrade_min_volume_shares,
                min_turnover=cfg.daytrade_min_turnover,
                daily_prequalified=(item.get("extreme_daytrade_info", {}).get("fixed_pool_qualification")
                    if item.get("extreme_daytrade_info", {}).get("candidate_source") == "A5_N_FIXED_POOL" else None),
            )
            checked.update({"stock_id": stock_id, "stock_name": item.get("stock_name", ""), "market_type": market_type, "revalidation": True})
            A5_N_RUN_ROWS.append(checked)
            if checked.get("strategy_state") == "ENTRY_VALIDATED":
                item["extreme_daytrade_info"] = checked
                item["signal_price"] = checked.get("signal_price")
                kept.append(item)
        except Exception as exc:
            failed = {"stock_id": stock_id, "stock_name": item.get("stock_name", ""), "strategy_state": "EXPIRED", "reject_reason": [f"C5_RECHECK_ERROR:{exc}"], "revalidation": True}
            A5_N_RUN_ROWS.append(failed)
    results["extreme_daytrade"] = kept


def html_from_body(body: str) -> str:
    _, _, html = body.partition("\n\nHTML_TABLE:\n")
    return html


def save_html_report(subject: str, body: str, cfg: Config) -> Path | None:
    html = html_from_body(body)
    if not html:
        return None
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    safe_date = cfg_date(cfg).replace("/", "-")
    path = REPORT_DIR / f"screener_report_{safe_date}.html"
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(html)
    return path


def sent_marker_path(cfg: Config) -> Path:
    safe_date = cfg_date(cfg).replace("/", "-")
    return SENT_MARKER_DIR / f"email_sent_{safe_date}.txt"


def already_sent_today(cfg: Config) -> bool:
    return sent_marker_path(cfg).exists()


def mark_sent_today(subject: str, cfg: Config) -> None:
    SENT_MARKER_DIR.mkdir(parents=True, exist_ok=True)
    with open(sent_marker_path(cfg), "w", encoding="utf-8") as fh:
        fh.write(subject)
        fh.write("\n")
        fh.write(dt.datetime.now().isoformat(timespec="seconds"))
        fh.write("\n")


def send_email(subject: str, body: str, cfg: Config, attachments: list[Path] | None = None) -> None:
    if not (cfg.smtp_host and cfg.email_from and cfg.email_to):
        return
    plain, _, html = body.partition("\n\nHTML_TABLE:\n")
    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = cfg.email_from
    msg["To"] = cfg.email_to
    alternative = MIMEMultipart("alternative")
    alternative.attach(MIMEText(plain, "plain", "utf-8"))
    if html:
        alternative.attach(MIMEText(html, "html", "utf-8"))
    msg.attach(alternative)

    for path in attachments or []:
        if not path or not path.exists():
            continue
        part = MIMEApplication(path.read_bytes(), _subtype="html")
        part.add_header("Content-Disposition", "attachment", filename=path.name)
        msg.attach(part)

    with smtplib.SMTP(cfg.smtp_host, cfg.smtp_port) as server:
        server.starttls()
        if cfg.smtp_user:
            server.login(cfg.smtp_user, cfg.smtp_password)
        server.sendmail(cfg.email_from, [x.strip() for x in cfg.email_to.split(",")], msg.as_string())
    print(f"[email] sent to {cfg.email_to}")


def notify(subject: str, body: str, cfg: Config) -> bool:
    with open("last_report.txt", "w", encoding="utf-8") as fh:
        fh.write(subject)
        fh.write("\n\n")
        fh.write(body.split("\n\nHTML_TABLE:\n")[0])

    html_path = save_html_report(subject, body, cfg)
    sent = False
    if cfg.email_from and cfg.email_to and cfg.smtp_host:
        send_email(subject, body, cfg, [html_path] if html_path else None)
        sent = True
    if cfg.line_notify_token:
        send_line_notify_legacy(subject + "\n" + body.split("\n\nHTML_TABLE:\n")[0], cfg)
        sent = True
    if not sent:
        print(subject)
        print(body.split("\n\nHTML_TABLE:\n")[0])
    return sent


def active_category_keys(cfg: Config) -> list[str]:
    if cfg.only_short_entry:
        return ["precision_entry"]
    if cfg.only_prepare_turn:
        return ["prepare_turn"]
    if cfg.intraday_alert_only:
        return ["precision_entry", "extreme_daytrade"]
    return list(CATEGORY_TITLES)


def run_mode_name(cfg: Config) -> str:
    if cfg.intraday_alert_only:
        return "intraday_ntfy"
    if cfg.only_short_entry:
        return "only_short_entry"
    if cfg.only_prepare_turn:
        return "only_prepare_turn"
    return "formal_report"


def run(cfg: Config, market: dict[str, Any] | None = None, universe_override: pd.DataFrame | None = None) -> dict[str, list[dict[str, Any]]]:
    A5_N_RUN_ROWS.clear()
    active_keys = active_category_keys(cfg)
    reset_ledger_context(cfg, run_mode_name(cfg), market)
    universe = universe_override.copy() if universe_override is not None else get_universe(cfg)
    print(f"Universe size after volume filter: {len(universe)}")
    results: dict[str, list[dict[str, Any]]] = {key: [] for key in active_keys}
    for i, (_, row) in enumerate(universe.iterrows(), start=1):
        print(f"[{i}/{len(universe)}] screening {row['stock_id']} {row['stock_name']}")
        categorized = screen_stock(row, cfg)
        for key, item in categorized.items():
            if key in results:
                results[key].append(item)
        if categorized:
            labels = ", ".join(item["category"] for item in categorized.values())
            print(f"  -> matched {row['stock_id']} {row['stock_name']} [{labels}]")

    if (
        "prepare_turn" in results
        and not results["prepare_turn"]
        and cfg.min_volume_shares > cfg.prepare_turn_fallback_volume_shares
    ):
        fallback_cfg = dataclasses.replace(
            cfg,
            min_volume_shares=cfg.prepare_turn_fallback_volume_shares,
        )
        fallback_universe = get_universe(fallback_cfg)
        seen = {str(row["stock_id"]) for _, row in universe.iterrows()}
        fallback_universe = fallback_universe[
            ~fallback_universe["stock_id"].astype(str).isin(seen)
        ]
        print(
            "Prepare-turn empty; fallback volume filter "
            f"{cfg.prepare_turn_fallback_volume_shares // 1000} lots adds "
            f"{len(fallback_universe)} stocks."
        )
        for i, (_, row) in enumerate(fallback_universe.iterrows(), start=1):
            print(
                f"[fallback {i}/{len(fallback_universe)}] screening "
                f"{row['stock_id']} {row['stock_name']}"
            )
            categorized = screen_stock(row, fallback_cfg)
            if "prepare_turn" in categorized:
                results["prepare_turn"].append(categorized["prepare_turn"])
                print(f"  -> matched {row['stock_id']} {row['stock_name']} [60K起漲雷達股]")

    finalize_results(results)
    write_run_ledger(
        cfg=cfg,
        status="success",
        universe_count=len(universe),
        active_keys=active_keys,
    )
    return results


def finalize_results(results: dict[str, list[dict[str, Any]]]) -> None:
    for key, rows in results.items():
        if key == "shortlist":
            continue
        for row in rows:
            row["category_keys"] = [key]
            row["category_names"] = [row.get("category", "")]
            if key == "extreme_daytrade":
                row["short_score"] = 0
                row["score_reasons"] = ["5K早盤當沖訊號"]
                row["score_warnings"] = ["當沖股不列入短線精選排名"]
                row["top_reason"] = (
                    "日K守多方、60K方向轉強，5K站上5EMA並出現MACD早盤觸發。"
                )
            else:
                score_short_candidate(row)

    for key, rows in results.items():
        if key == "shortlist":
            continue
        if key == "extreme_daytrade":
            rows.sort(
                key=lambda item: (
                    int(item.get("extreme_daytrade_priority") or 0),
                    int((item.get("extreme_daytrade_info") or {}).get("intraday_volume_shares") or 0),
                    float(item.get("turnover") or 0),
                    int(item.get("volume_lots") or 0),
                ),
                reverse=True,
            )
        elif key == "precision_entry":
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
        annotate_ledger_prelimit_rank(key, rows)
        limit = LIMITED_CATEGORY_COUNTS.get(key)
        if limit is not None:
            results[key] = rows[:limit]

    shortlist_pool: dict[str, dict[str, Any]] = {}
    for key in ("strong_continuation", "relay_breakout", "prepare_turn", "precision_entry"):
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
    flush_signal_ledgers(results)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Strict Taiwan stock auto screener")
    parser.add_argument("--max-stocks", type=int, default=None, help="Limit stocks for a quick test")
    parser.add_argument("--no-intraday", action="store_true", help="Skip 60-minute K entry check")
    parser.add_argument("--no-big-holder", action="store_true", help="Skip big-holder percentage check")
    parser.add_argument("--no-notify", action="store_true", help="Print result only")
    parser.add_argument("--only-short-entry", action="store_true", help="Run only category 4")
    parser.add_argument("--only-prepare-turn", action="store_true", help="Run only category 3")
    parser.add_argument("--intraday-ntfy", action="store_true", help="Run category 4/5 intraday alert mode and push ntfy only")
    parser.add_argument("--a5n-test", action="store_true", help="Run A5-N research scan now and send its dedicated test ntfy")
    parser.add_argument("--a5n-build-pool", action="store_true", help="Build today's A5-N T-1 premarket candidate pool")
    parser.add_argument("--a5n-scan-pool", action="store_true", help="Scan only today's persisted A5-N candidate pool")
    parser.add_argument("--a5n-build-fixed-pool", action="store_true", help="Build weekly A5-N fixed pool from a completed Friday")
    parser.add_argument("--a5n-scan-fixed-pool", action="store_true", help="09:31 scan of the valid weekly A5-N fixed pool")
    parser.add_argument("--fixed-pool-anchor", default=None, help="Friday YYYY-MM-DD anchor for fixed-pool build/replay")
    parser.add_argument("--report-date", default=None, help="Use data up to YYYY-MM-DD for review/backtest")
    parser.add_argument(
        "--skip-if-sent",
        action="store_true",
        help="Skip cloud catch-up runs after today's formal report was already sent",
    )
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
    if args.only_prepare_turn:
        cfg = dataclasses.replace(cfg, only_prepare_turn=True)
    if args.intraday_ntfy or args.a5n_test or args.a5n_scan_pool or args.a5n_scan_fixed_pool:
        cfg = dataclasses.replace(cfg, intraday_alert_only=True)
    if args.report_date:
        cfg = dataclasses.replace(cfg, report_date=args.report_date)

    if args.a5n_build_pool:
        build_a5n_premarket_pool(cfg)
        return 0
    if args.a5n_build_fixed_pool:
        build_a5n_weekly_fixed_pool(cfg, anchor_date=args.fixed_pool_anchor)
        return 0

    if args.intraday_ntfy or args.a5n_test or args.a5n_scan_pool or args.a5n_scan_fixed_pool:
        current_taipei = now_taipei()
        if (args.a5n_scan_pool or args.a5n_scan_fixed_pool) and current_taipei.weekday() >= 5:
            print("[A5-N skip] 週末不執行盤中掃描或發送通知。")
            return 0
        market = market_state(cfg)
        if not market.get("ok") and not (args.a5n_test or args.a5n_scan_pool or args.a5n_scan_fixed_pool):
            reason = str(market.get("reason") or "大盤狀態未通過")
            print(f"[market-skip] {reason}")
            reset_ledger_context(cfg, run_mode_name(cfg), market)
            send_ntfy(
                "台股60K盤中檢查暫停\n"
                f"原因：{reason}\n"
                "本次不發第四類/第五類進場訊號，避免逆勢硬做。",
                cfg,
                title="TW Stock 60K Market Skip",
                priority="3",
            )
            write_run_ledger(
                cfg=cfg,
                status="market_skipped",
                universe_count=0,
                active_keys=active_category_keys(cfg),
                error_text=reason,
            )
            return 0
        pool_universe = (load_a5n_fixed_pool_universe(cfg) if args.a5n_scan_fixed_pool
                         else load_a5n_premarket_universe(cfg) if args.a5n_scan_pool else None)
        results = run(cfg, market, universe_override=pool_universe)
        if args.a5n_scan_pool:
            run_a5n_b_shadow_scan(cfg)
        if args.a5n_scan_fixed_pool:
            run_a5n_fixed_momentum_rank_shadow_scan(cfg)
        revalidate_a5n_entries(results, cfg)
        if args.intraday_ntfy:
            message = format_intraday_ntfy_message(results, market)
            send_ntfy(
                message,
                cfg,
                title="TW Stock 60K Alert",
                priority="4",
            )
        a5n_message = format_a5n_ntfy_message(A5_N_RUN_ROWS)
        sent_at = now_taipei().isoformat()
        slot = a5n_notification_slot(current_taipei)
        duplicate_slot = ((args.a5n_scan_pool or args.a5n_scan_fixed_pool)
                          and a5n_slot_already_sent(slot, current_taipei))
        if duplicate_slot:
            print(f"[A5-N duplicate suppressed] {current_taipei.date()} {slot} 已成功發送過。")
            sent = False
        else:
            sent = False if args.no_notify else send_ntfy(
                a5n_message, cfg, title="A5-N 第五類當沖測試", priority="4")
            if sent and (args.a5n_scan_pool or args.a5n_scan_fixed_pool):
                mark_a5n_slot_sent(slot, current_taipei)
        write_a5n_ledger(sent_at if sent else None)
        print(a5n_message)
        return 0

    if args.skip_if_sent and already_sent_today(cfg):
        print(f"[skip] Formal report already sent for {cfg_date(cfg)}.")
        return 0

    formal_report_ready = False
    try:
        results = run(cfg)
        record_top3_journal(results.get("shortlist", []), cfg)
        try:
            event_sections = build_event_alerts(results.get("shortlist", [])[:3], cfg)
        except Exception:
            event_sections = (
                "🚨 警示！候選股近期大事\n事件警示模組暫時無法完成，請人工留意重大訊息。",
                "<section class='card alert-card'><h3>🚨 警示！候選股近期大事</h3><p class='risk'>事件警示模組暫時無法完成，請人工留意重大訊息。</p></section>",
            )
        try:
            weekly_sections = build_weekly_backtest(cfg)
        except Exception as exc:
            weekly_sections = (
                f"本週策略勝率與達標率總體檢報告\n週回測模組暫時無法完成：{exc}",
                f"<section class='card'><h3>本週策略勝率與達標率總體檢報告</h3><p class='risk'>週回測模組暫時無法完成：{escape(str(exc))}</p></section>",
            )
        subject, body = format_report(results, cfg, event_sections, weekly_sections)
        formal_report_ready = True
    except Exception:
        error_text = traceback.format_exc()
        write_run_ledger(
            cfg=cfg,
            status="error",
            universe_count=0,
            active_keys=active_category_keys(cfg),
            error_text=error_text,
        )
        subject, body = format_status_report(error_text, cfg)

    if args.no_notify:
        print(subject)
        print(body.split("\n\nHTML_TABLE:\n")[0])
    else:
        notification_sent = notify(subject, body, cfg)
        if notification_sent and formal_report_ready:
            mark_sent_today(subject, cfg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

