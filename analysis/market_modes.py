"""Unified market mode engine with old-core BLOCK entry/exit logic.

v129 focus:
- Market mode uses broad Market Guard snapshot from main.py.
- BLOCK_LONGS entry is harder: breadth-only weakness is usually STRONG.
- BLOCK_LONGS exit has two paths:
  1) fast exit to RECOVERY_LONG when rebound edge is clear;
  2) safe exit to STRONG_LONG_ONLY when market is no longer crashing.
- RECOVERY_LONG is not mandatory; it is a temporary alternative to STRONG.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone

from utils.constants import *


@dataclass
class MarketSnapshot:
    btc_change_15m: float = 0.0
    red_ratio_15m: float = 0.5
    avg_change_15m: float = 0.0
    strong_coins_count: int = 0
    fast_rebound: bool = False
    btc_reclaim: bool = False
    breadth_improving: bool = False


@dataclass
class MarketModeState:
    mode: str = MODE_NORMAL_LONG
    changed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    recovery_cycle_started_at: datetime | None = None
    recovery_trade_count: int = 0
    reminder_count: int = 0
    consecutive_improvement_scans: int = 0
    consecutive_weak_scans: int = 0


# Keep transitions stable without trapping the bot inside BLOCK.
MODE_CHANGE_COOLDOWN_MINUTES = 10
BLOCK_EXIT_CONFIRM_SCANS = 2
STRONG_TO_BLOCK_CONFIRM_SCANS = 2

# Old-core style thresholds.
BLOCK_RED_RATIO = 0.68
BLOCK_AVG_CHANGE = -1.20
BLOCK_BTC_CHANGE = -0.70
BLOCK_BTC_RED_RATIO = 0.55
ALT_WEAK_RED_RATIO = 0.60

NO_LONGER_CRASHING_RED_RATIO = 0.62
NO_LONGER_CRASHING_AVG = -0.75
NO_LONGER_CRASHING_BTC = -0.45

RECOVERY_READY_RED_RATIO = 0.58
RECOVERY_READY_AVG = -0.55
RECOVERY_READY_BTC = -0.32

NORMAL_READY_RED_RATIO = 0.62
NORMAL_READY_AVG = 0.05
NORMAL_READY_BTC = -0.10


def _values(snapshot: MarketSnapshot) -> tuple[float, float, float, int]:
    return (
        float(snapshot.red_ratio_15m or 0.0),
        float(snapshot.avg_change_15m or 0.0),
        float(snapshot.btc_change_15m or 0.0),
        int(snapshot.strong_coins_count or 0),
    )


def _is_no_longer_crashing(snapshot: MarketSnapshot) -> bool:
    red_ratio, avg, btc, strong = _values(snapshot)
    old_core_safe = (
        red_ratio < NO_LONGER_CRASHING_RED_RATIO
        and avg > NO_LONGER_CRASHING_AVG
        and btc > NO_LONGER_CRASHING_BTC
    )
    stabilization_safe = bool(
        snapshot.breadth_improving
        or snapshot.btc_reclaim
        or (btc > -0.65 and avg > -1.05 and red_ratio < 0.68)
        or (strong >= 5 and avg > -0.90 and btc > -0.65)
    )
    return bool(old_core_safe or stabilization_safe)


def _is_recovery_ready(snapshot: MarketSnapshot) -> bool:
    red_ratio, avg, btc, strong = _values(snapshot)
    old_core_recovery = (
        red_ratio < RECOVERY_READY_RED_RATIO
        and avg > RECOVERY_READY_AVG
        and btc > RECOVERY_READY_BTC
    )
    fast_recovery_edge = bool(
        snapshot.fast_rebound
        and snapshot.btc_reclaim
        and snapshot.breadth_improving
        and red_ratio <= 0.62
        and avg > -0.55
        and btc > -0.40
        and strong >= 5
    )
    return bool(old_core_recovery and (snapshot.fast_rebound or snapshot.btc_reclaim or strong >= 6) or fast_recovery_edge)


def _is_normal_ready(snapshot: MarketSnapshot) -> bool:
    red_ratio, avg, btc, strong = _values(snapshot)
    # v135: Red Ratio is contextual, not a standalone NORMAL blocker.
    # A market with positive avg 15m, non-bearish BTC, and enough strong coins
    # can return to NORMAL_LONG even with moderate red breadth around 58-62%.
    return bool(
        red_ratio <= NORMAL_READY_RED_RATIO
        and avg >= NORMAL_READY_AVG
        and btc >= NORMAL_READY_BTC
        and strong >= 8
    )


def _risk_flags(snapshot: MarketSnapshot) -> dict:
    """Classify market risk using old-core entry/exit paths.

    BLOCK entry paths:
    1) broad market crash: red_ratio high + avg change sharply negative;
    2) BTC breakdown: BTC sharply negative + enough red breadth;
    3) alt weakness only becomes BLOCK if it has breakdown pressure and no stabilization.
    """
    red_ratio, avg, btc, strong = _values(snapshot)

    broad_market_crash = red_ratio >= BLOCK_RED_RATIO and avg <= BLOCK_AVG_CHANGE
    btc_breakdown = btc <= BLOCK_BTC_CHANGE and red_ratio >= BLOCK_BTC_RED_RATIO
    alt_weak_pressure = red_ratio >= ALT_WEAK_RED_RATIO and avg <= -0.85 and btc <= -0.45

    no_longer_crashing = _is_no_longer_crashing(snapshot)
    recovery_ready = _is_recovery_ready(snapshot)
    normal_ready = _is_normal_ready(snapshot)

    # Stabilization prevents breadth-only BLOCK and pushes the system toward STRONG.
    stabilizing = bool(no_longer_crashing or snapshot.breadth_improving or snapshot.btc_reclaim)

    real_block = bool((broad_market_crash or btc_breakdown or alt_weak_pressure) and not stabilizing)
    # v135: moderate red breadth alone should not trap the bot in STRONG
    # when average move is positive and enough strong coins exist.
    weak_breadth = bool(
        red_ratio >= 0.62
        or (red_ratio >= 0.56 and (avg <= 0.0 or strong < 8))
        or avg <= -0.30
        or btc <= -0.25
        or strong <= 4
    )

    return {
        "broad_market_crash": broad_market_crash,
        "btc_breakdown": btc_breakdown,
        "alt_weak_pressure": alt_weak_pressure,
        "weak_breadth": weak_breadth,
        "stabilizing": stabilizing,
        "no_longer_crashing": no_longer_crashing,
        "recovery_ready": recovery_ready,
        "normal_ready": normal_ready,
        "real_block": real_block,
    }


def _base_mode(snapshot: MarketSnapshot) -> str:
    flags = _risk_flags(snapshot)
    if flags["real_block"]:
        return MODE_BLOCK_LONGS
    if flags["recovery_ready"]:
        return MODE_RECOVERY_LONG
    if flags["weak_breadth"]:
        return MODE_STRONG_LONG_ONLY
    return MODE_NORMAL_LONG


def decide_market_mode(snapshot: MarketSnapshot, previous: MarketModeState | None = None, now: datetime | None = None) -> MarketModeState:
    now = now or datetime.now(timezone.utc)
    previous = previous or MarketModeState()
    minutes_in_mode = int((now - previous.changed_at).total_seconds() // 60)
    flags = _risk_flags(snapshot)
    raw = _base_mode(snapshot)

    next_state = replace(previous)
    improving = flags["no_longer_crashing"] or flags["recovery_ready"] or flags["stabilizing"]
    weakening = flags["real_block"]
    next_state.consecutive_improvement_scans = previous.consecutive_improvement_scans + 1 if improving else 0
    next_state.consecutive_weak_scans = previous.consecutive_weak_scans + 1 if weakening else 0

    candidate_mode = previous.mode

    if previous.mode == MODE_BLOCK_LONGS:
        # Fast exit path: recovery edge confirmed.
        if flags["recovery_ready"]:
            candidate_mode = MODE_RECOVERY_LONG
        # Slow/safe exit path: dump stopped; move to STRONG after confirmation.
        elif flags["no_longer_crashing"] or flags["stabilizing"] or not flags["real_block"]:
            if next_state.consecutive_improvement_scans >= BLOCK_EXIT_CONFIRM_SCANS:
                candidate_mode = MODE_STRONG_LONG_ONLY
            else:
                candidate_mode = MODE_BLOCK_LONGS
        else:
            candidate_mode = MODE_BLOCK_LONGS

    elif previous.mode == MODE_STRONG_LONG_ONLY:
        # Enter BLOCK from STRONG only after confirmed real breakdown.
        if flags["real_block"] and next_state.consecutive_weak_scans >= STRONG_TO_BLOCK_CONFIRM_SCANS:
            candidate_mode = MODE_BLOCK_LONGS
        elif flags["recovery_ready"]:
            candidate_mode = MODE_RECOVERY_LONG
        elif flags["normal_ready"]:
            candidate_mode = MODE_NORMAL_LONG
        else:
            candidate_mode = MODE_STRONG_LONG_ONLY

    elif previous.mode == MODE_RECOVERY_LONG:
        if flags["real_block"]:
            candidate_mode = MODE_BLOCK_LONGS
        elif previous.recovery_cycle_started_at and now - previous.recovery_cycle_started_at >= timedelta(minutes=RECOVERY_WINDOW_MINUTES):
            candidate_mode = MODE_STRONG_LONG_ONLY
        elif flags["recovery_ready"]:
            candidate_mode = MODE_RECOVERY_LONG
        elif flags["normal_ready"]:
            candidate_mode = MODE_NORMAL_LONG
        else:
            # Recovery is not a separate permanent path. If rebound edge fades,
            # fall back to STRONG, not BLOCK unless a real breakdown returns.
            candidate_mode = MODE_STRONG_LONG_ONLY

    else:  # NORMAL_LONG
        if flags["real_block"]:
            candidate_mode = MODE_BLOCK_LONGS
        elif flags["recovery_ready"]:
            candidate_mode = MODE_RECOVERY_LONG
        elif flags["weak_breadth"]:
            candidate_mode = MODE_STRONG_LONG_ONLY
        else:
            candidate_mode = raw

    # Anti-flapping cooldown. Do not trap BLOCK on cooldown when recovery/stability appears.
    if candidate_mode != previous.mode and minutes_in_mode < MODE_CHANGE_COOLDOWN_MINUTES:
        if previous.mode == MODE_BLOCK_LONGS and candidate_mode in (MODE_STRONG_LONG_ONLY, MODE_RECOVERY_LONG):
            pass
        elif candidate_mode == MODE_BLOCK_LONGS and flags["real_block"]:
            pass
        elif candidate_mode != MODE_BLOCK_LONGS:
            candidate_mode = previous.mode

    changed = candidate_mode != previous.mode
    next_state.mode = candidate_mode
    next_state.changed_at = now if changed else previous.changed_at
    next_state.reminder_count = 0 if changed else previous.reminder_count

    if candidate_mode == MODE_RECOVERY_LONG:
        if previous.mode != MODE_RECOVERY_LONG:
            next_state.recovery_cycle_started_at = now
            next_state.recovery_trade_count = 0
        else:
            next_state.recovery_cycle_started_at = previous.recovery_cycle_started_at
            next_state.recovery_trade_count = previous.recovery_trade_count
    else:
        next_state.recovery_cycle_started_at = None
        next_state.recovery_trade_count = 0

    return next_state


def increment_reminder_count(state: MarketModeState) -> MarketModeState:
    return replace(state, reminder_count=state.reminder_count + 1)


def register_recovery_trade(state: MarketModeState) -> MarketModeState:
    if state.mode != MODE_RECOVERY_LONG:
        return state
    return replace(state, recovery_trade_count=min(MAX_RECOVERY_TRADES_PER_CYCLE, state.recovery_trade_count + 1))


def recovery_slots_remaining(state: MarketModeState) -> int:
    if state.mode != MODE_RECOVERY_LONG:
        return MAX_RECOVERY_TRADES_PER_CYCLE
    return max(0, MAX_RECOVERY_TRADES_PER_CYCLE - state.recovery_trade_count)


def block_protection_status(state: MarketModeState, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    if state.mode != MODE_BLOCK_LONGS:
        return {"level": 0, "current": "inactive", "next": "inactive", "remaining_minutes": 0}
    minutes_in_mode = int((now - state.changed_at).total_seconds() // 60)
    if minutes_in_mode < 15:
        return {"level": 1, "current": "LEVEL 1 — Monitor Only", "next": "Soft Protection", "remaining_minutes": 15 - minutes_in_mode}
    if minutes_in_mode < 30:
        return {"level": 2, "current": "LEVEL 2 — Soft Protection", "next": "Defensive Protection", "remaining_minutes": 30 - minutes_in_mode}
    if minutes_in_mode < 40:
        return {"level": 3, "current": "LEVEL 3 — Defensive Protection", "next": "Max protection active", "remaining_minutes": 40 - minutes_in_mode}
    return {"level": 3, "current": "LEVEL 3 — Defensive Protection", "next": "Max protection active", "remaining_minutes": 0}
