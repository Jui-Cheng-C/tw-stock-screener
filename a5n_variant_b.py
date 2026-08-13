"""A5-N shadow variant B: fixed seven-day local platform.

This configuration is research-only.  It never replaces A5_N_CONFIG and must
not feed the formal candidate pool or ntfy output.
"""
from __future__ import annotations

from a5n_strategy import A5_N_CONFIG


A5_N_B_VERSION = "A5-N-B-local-platform-7d-v0.1-20260813"
A5_N_B_CONFIG = {
    **A5_N_CONFIG,
    "parameter_status": "shadow_research_variant_b",
    "platform_lookback_days": 7,
    "platform_exclude_recent_days": 0,
    "platform_min_days": 6,
    "variant_name": "B_fixed_7d_local_platform",
    "variant_scope": "A1 platform definition only; A2-A5/B/C unchanged",
    "notification_enabled": False,
}
