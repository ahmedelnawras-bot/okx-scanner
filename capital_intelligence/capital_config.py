"""
Capital Intelligence Layer - configuration v1

Shadow-mode configuration only. This module does not execute trades, does not
change scoring, and does not touch main.py.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class CapitalIntelligenceConfig:
    """Weights and thresholds for Capital Bid calculation."""

    model_name: str = "capital_intelligence_shadow_v1"
    shadow_mode: bool = True

    # Component caps. Sum is intentionally 100.
    setup_max_points: float = 40.0
    mtf_max_points: float = 15.0
    pa_max_points: float = 15.0
    nour_max_points: float = 10.0
    resistance_max_points: float = 10.0
    context_max_points: float = 10.0

    # Trade class thresholds.
    class_a_plus_min: float = 90.0
    class_a_min: float = 80.0
    class_b_min: float = 70.0
    class_c_min: float = 0.0

    # Selection thresholds for future auction mode. In v1 these are advisory.
    min_bid_for_core_slot: float = 70.0
    min_bid_for_elite_slot: float = 85.0

    # Setup quality weights.
    setup_weights: dict[str, float] = field(default_factory=lambda: {
        "higher_low_continuation": 38.0,
        "clean_higher_low_structure": 40.0,
        "breakout_pullback_acceptance": 39.0,
        "compression_release_continuation": 36.0,
        "trend_continuation_after_pause": 34.0,
        "wave_3": 33.0,
        "retest_breakout_confirmed": 31.0,
        "vwap_reclaim": 24.0,
        "liquidity_sweep_reclaim": 28.0,
        "sweep_reclaim_continuation": 29.0,
        "support_bounce_confirmed": 21.0,
        "relative_strength_vs_btc": 8.0,  # confirmation only, not standalone alpha
        "bot_order_restored_position": 0.0,
        "okx_recovered_position": 0.0,
    })

    # Additive modifiers from tags / analytics tags.
    tag_bonus: dict[str, float] = field(default_factory=lambda: {
        "elite": 3.0,
        "whitelist": 1.5,
        "rs_confirmation": 2.0,
        "relative_strength_vs_btc": 2.0,
        "breakout": 2.0,
        "continuation": 2.0,
        "liquid": 1.5,
        "major": 1.5,
    })
    tag_penalty: dict[str, float] = field(default_factory=lambda: {
        "near_resistance": -5.0,
        "rebound": -2.0,
    })

    # Resistance scoring tiers based on 4H resistance distance.
    resistance_very_near_max_pct: float = 0.75
    resistance_near_max_pct: float = 2.0
    resistance_watch_max_pct: float = 4.0
    resistance_clear_min_pct: float = 5.0


DEFAULT_CONFIG = CapitalIntelligenceConfig()
