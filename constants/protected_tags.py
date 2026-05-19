"""Protected Tags and Setup Types — Phase 3.

هذه الثوابت محمية ولا يجوز تعديلها من خارج هذا الملف.
أي إضافة أو تعديل لازم يتم هنا فقط وينعكس على كل الـ system.

الهدف:
- منع drift بين Replay و Live
- ضمان consistency في الـ tagging
- single source of truth للـ tags والـ setups
"""
from __future__ import annotations

# ── Pair Tags (pair_selection.py + engine.py) ──────────────────────────────────
# هذه الـ tags بتتولد في pair_selection.py للـ Live
# وفي engine.py/_pair_from_candle للـ Replay
# لازم يكونوا متطابقين في الاتنين

PAIR_TAG_LIQUID = "liquid"
PAIR_TAG_MAJOR = "major"
PAIR_TAG_MOMENTUM = "momentum"
PAIR_TAG_BREAKOUT = "breakout"
PAIR_TAG_REBOUND = "rebound"
PAIR_TAG_COMPRESSION = "compression"
PAIR_TAG_CONTINUATION = "continuation"
PAIR_TAG_RS_BTC = "rs_btc"
PAIR_TAG_NEAR_RESISTANCE = "near_resistance"

ALL_PAIR_TAGS: frozenset[str] = frozenset({
    PAIR_TAG_LIQUID,
    PAIR_TAG_MAJOR,
    PAIR_TAG_MOMENTUM,
    PAIR_TAG_BREAKOUT,
    PAIR_TAG_REBOUND,
    PAIR_TAG_COMPRESSION,
    PAIR_TAG_CONTINUATION,
    PAIR_TAG_RS_BTC,
    PAIR_TAG_NEAR_RESISTANCE,
})

# ── Execution Tags (execution_candidate.py) ────────────────────────────────────
EXEC_TAG_WHITELIST = "whitelist"
EXEC_TAG_ELITE = "elite"
EXEC_TAG_RECOVERY = "recovery_execution"
EXEC_TAG_BLOCK_EXCEPTION = "block_exception"

ALL_EXECUTION_TAGS: frozenset[str] = frozenset({
    EXEC_TAG_WHITELIST,
    EXEC_TAG_ELITE,
    EXEC_TAG_RECOVERY,
    EXEC_TAG_BLOCK_EXCEPTION,
})

# ── Setup Types (scoring.py) ────────────────────────────────────────────────────
SETUP_VWAP_RECLAIM = "vwap_reclaim"
SETUP_RETEST_BREAKOUT = "retest_breakout_confirmed"
SETUP_WAVE_3 = "wave_3"
SETUP_RS_BTC = "relative_strength_vs_btc"
SETUP_SUPPORT_BOUNCE = "support_bounce_confirmed"
SETUP_FAILED_BREAKDOWN = "failed_breakdown_trap"
SETUP_HIGHER_LOW = "higher_low_continuation"
SETUP_LIQUIDITY_SWEEP = "liquidity_sweep_reclaim"

ALL_SETUP_TYPES: frozenset[str] = frozenset({
    SETUP_VWAP_RECLAIM,
    SETUP_RETEST_BREAKOUT,
    SETUP_WAVE_3,
    SETUP_RS_BTC,
    SETUP_SUPPORT_BOUNCE,
    SETUP_FAILED_BREAKDOWN,
    SETUP_HIGHER_LOW,
    SETUP_LIQUIDITY_SWEEP,
})

WHITELIST_SETUPS: frozenset[str] = frozenset({
    SETUP_VWAP_RECLAIM,
    SETUP_RETEST_BREAKOUT,
    SETUP_WAVE_3,
    SETUP_RS_BTC,
})

ELITE_SETUPS: frozenset[str] = frozenset({
    SETUP_RETEST_BREAKOUT,
    SETUP_WAVE_3,
    SETUP_RS_BTC,
})

BLOCK_EXCEPTION_SETUPS: frozenset[str] = frozenset({
    SETUP_RS_BTC,
    SETUP_RETEST_BREAKOUT,
})

NORMAL_LONG_EXTRA_SETUPS: frozenset[str] = frozenset({
    SETUP_FAILED_BREAKDOWN,
    SETUP_HIGHER_LOW,
    SETUP_SUPPORT_BOUNCE,
    SETUP_LIQUIDITY_SWEEP,
})

# ── Setup Weights ───────────────────────────────────────────────────────────────
SETUP_WEIGHTS: dict[str, int] = {
    SETUP_VWAP_RECLAIM:     3,
    SETUP_RETEST_BREAKOUT:  3,
    SETUP_LIQUIDITY_SWEEP:  2,
    SETUP_RS_BTC:           2,
    SETUP_WAVE_3:           2,
    SETUP_SUPPORT_BOUNCE:   2,
    SETUP_FAILED_BREAKDOWN: 2,
    SETUP_HIGHER_LOW:       2,
}

# ── Major Symbols ───────────────────────────────────────────────────────────────
MAJOR_SYMBOL_PREFIXES: tuple[str, ...] = (
    "BTC-", "ETH-", "SOL-", "XRP-", "DOGE-", "BNB-", "AVAX-", "LINK-",
)

RS_MAJOR_PREFIXES: tuple[str, ...] = (
    "BTC-", "ETH-", "SOL-", "LINK-", "AVAX-",
)
