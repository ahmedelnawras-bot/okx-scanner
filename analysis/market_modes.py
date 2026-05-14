"""Unified market mode engine with fast rebound, anti-flapping, and block protection metadata."""
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


MODE_CHANGE_COOLDOWN_MINUTES = 10
BLOCK_EXIT_CONFIRM_SCANS = 2
STRONG_TO_BLOCK_CONFIRM_SCANS = 2


def _risk_flags(snapshot: MarketSnapshot) -> dict:
    """Classify whether the market is weak, breaking down, or stabilizing.

    The old problem was entering BLOCK on breadth alone and then staying there
    after BTC started stabilizing. These flags separate weak breadth from a real
    ongoing breakdown.
    """
    red_ratio = float(snapshot.red_ratio_15m or 0.0)
    avg = float(snapshot.avg_change_15m or 0.0)
    btc = float(snapshot.btc_change_15m or 0.0)
    strong = int(snapshot.strong_coins_count or 0)

    btc_breakdown = btc <= -1.20
    severe_breadth = red_ratio >= 0.78 and avg <= -0.95
    broad_dump = red_ratio >= 0.70 and avg <= -1.35
    weak_breadth = red_ratio >= 0.60 or strong <= 3 or avg <= -0.35 or btc <= -0.45

    stabilizing = (
        snapshot.breadth_improving
        or snapshot.btc_reclaim
        or snapshot.fast_rebound
        or (btc > -0.55 and avg > -0.65)
        or (strong >= 5 and avg > -0.85)
    )
    real_block = (btc_breakdown or severe_breadth or broad_dump) and not stabilizing
    fast_recovery = snapshot.fast_rebound and snapshot.btc_reclaim and snapshot.breadth_improving

    return {
        "btc_breakdown": btc_breakdown,
        "severe_breadth": severe_breadth,
        "broad_dump": broad_dump,
        "weak_breadth": weak_breadth,
        "stabilizing": stabilizing,
        "real_block": real_block,
        "fast_recovery": fast_recovery,
    }


def _base_mode(snapshot: MarketSnapshot) -> str:
    flags = _risk_flags(snapshot)
    if flags["real_block"]:
        return MODE_BLOCK_LONGS
    if flags["fast_recovery"]:
        # Recovery is a conditional alternative to STRONG, not a mandatory path.
        return MODE_RECOVERY_LONG
    if flags["weak_breadth"]:
        return MODE_STRONG_LONG_ONLY
    return MODE_NORMAL_LONG


def decide_market_mode(snapshot: MarketSnapshot, previous: MarketModeState | None = None, now: datetime | None = None) -> MarketModeState:
    now = now or datetime.now(timezone.utc)
    previous = previous or MarketModeState()
    minutes_in_mode = int((now - previous.changed_at).total_seconds() // 60)
    raw = _base_mode(snapshot)
    flags = _risk_flags(snapshot)

    next_state = replace(previous)

    improving = flags["stabilizing"] or flags["fast_recovery"]
    weakening = flags["real_block"]
    next_state.consecutive_improvement_scans = previous.consecutive_improvement_scans + 1 if improving else 0
    next_state.consecutive_weak_scans = previous.consecutive_weak_scans + 1 if weakening else 0

    candidate_mode = previous.mode

    if previous.mode == MODE_BLOCK_LONGS:
        # Exit block quickly when the dump stops. If rebound is fast and confirmed,
        # Recovery replaces Strong temporarily; otherwise Strong is the default exit.
        if flags["fast_recovery"]:
            candidate_mode = MODE_RECOVERY_LONG
        elif flags["stabilizing"] or not flags["real_block"]:
            candidate_mode = MODE_STRONG_LONG_ONLY
        else:
            candidate_mode = MODE_BLOCK_LONGS

    elif previous.mode == MODE_STRONG_LONG_ONLY:
        if flags["real_block"] and next_state.consecutive_weak_scans >= STRONG_TO_BLOCK_CONFIRM_SCANS:
            candidate_mode = MODE_BLOCK_LONGS
        elif flags["fast_recovery"]:
            candidate_mode = MODE_RECOVERY_LONG
        elif raw == MODE_NORMAL_LONG and snapshot.strong_coins_count >= 6 and snapshot.red_ratio_15m < 0.52 and snapshot.avg_change_15m > -0.15:
            candidate_mode = MODE_NORMAL_LONG
        else:
            candidate_mode = MODE_STRONG_LONG_ONLY

    elif previous.mode == MODE_RECOVERY_LONG:
        if flags["real_block"]:
            candidate_mode = MODE_BLOCK_LONGS
        elif previous.recovery_cycle_started_at and now - previous.recovery_cycle_started_at >= timedelta(minutes=RECOVERY_WINDOW_MINUTES):
            candidate_mode = MODE_STRONG_LONG_ONLY
        elif flags["fast_recovery"]:
            candidate_mode = MODE_RECOVERY_LONG
        elif raw == MODE_NORMAL_LONG and snapshot.strong_coins_count >= 7 and snapshot.red_ratio_15m < 0.48 and snapshot.avg_change_15m >= 0:
            candidate_mode = MODE_NORMAL_LONG
        else:
            candidate_mode = MODE_STRONG_LONG_ONLY

    else:
        if flags["real_block"]:
            candidate_mode = MODE_BLOCK_LONGS
        elif flags["fast_recovery"]:
            candidate_mode = MODE_RECOVERY_LONG
        else:
            candidate_mode = raw

    # Keep a small cooldown to avoid noisy flips, but never keep BLOCK just
    # because of cooldown when market has stabilized.
    if candidate_mode != previous.mode and minutes_in_mode < MODE_CHANGE_COOLDOWN_MINUTES:
        if previous.mode == MODE_BLOCK_LONGS and candidate_mode in (MODE_STRONG_LONG_ONLY, MODE_RECOVERY_LONG):
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
