"""
Capital Intelligence Layer - configuration v1.7

Shadow-mode configuration only. This module does not execute trades, does not
change scoring, and does not touch main.py.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class CapitalIntelligenceConfig:
    """Weights and thresholds for Capital Bid calculation."""

    model_name: str = "capital_intelligence_shadow_v1_7"
    shadow_mode: bool = True

    # Component caps. Sum is intentionally 100 after V1.7 calibration.
    # Core signal remains dominant, while synergy / market mode are explicit
    # bounded components instead of hidden extra points above 100.
    setup_max_points: float = 35.0
    mtf_max_points: float = 12.0
    pa_max_points: float = 13.0
    nour_max_points: float = 10.0
    resistance_max_points: float = 10.0
    context_max_points: float = 10.0
    synergy_max_points: float = 5.0
    market_mode_max_points: float = 5.0
    symbol_memory_max_abs_points: float = 6.0

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
        "clean_higher_low_structure": 35.0,
        "breakout_pullback_acceptance": 34.0,
        "higher_low_continuation": 33.0,
        "compression_release_continuation": 31.0,
        "trend_continuation_after_pause": 30.0,
        "wave_3": 28.0,
        "retest_breakout_confirmed": 27.0,
        "sweep_reclaim_continuation": 26.0,
        "liquidity_sweep_reclaim": 25.0,
        "vwap_reclaim": 22.0,
        "support_bounce_confirmed": 20.0,
        "relative_strength_vs_btc": 6.0,  # confirmation only, not standalone alpha
        "bot_order_restored_position": 0.0,
        "okx_recovered_position": 0.0,
    })

    # Additive modifiers from tags / analytics tags.
    tag_bonus: dict[str, float] = field(default_factory=lambda: {
        "elite": 2.5,
        "whitelist": 1.5,
        "rs_confirmation": 1.0,
        "relative_strength_vs_btc": 0.5,
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
