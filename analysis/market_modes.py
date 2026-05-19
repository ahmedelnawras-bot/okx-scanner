"""Unified market mode engine with old-core BLOCK entry/exit logic.

v129 focus:
- Market mode uses broad Market Guard snapshot from main.py.
- BLOCK_LONGS entry is harder: breadth-only weakness is usually STRONG.
- BLOCK_LONGS exit has two paths:
  1) fast exit to RECOVERY_LONG when rebound edge is clear;
  2) safe exit to STRONG_LONG_ONLY when market is no longer crashing.
- RECOVERY_LONG is temporary and should only be reached from BLOCK/confirmed crash rebound paths.
- NORMAL_LONG remains the healthy/green market mode; STRONG_LONG_ONLY is defensive pressure mode.
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
    hourly_ma5_pressure: bool = False
    btc_1h_close: float = 0.0
    btc_1h_ma5: float = 0.0
    btc_1h_ma5_gap_pct: float = 0.0


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
NORMAL_EXIT_COOLDOWN_MINUTES = 4
RETURN_TO_NORMAL_COOLDOWN_MINUTES = 8

# BLOCK should be defensive: fast in on real breakdown, slower out after pressure.
# Recovery can still exit BLOCK immediately when a clear fast rebound appears.
BLOCK_MIN_HOLD_MINUTES = 10
BLOCK_EXIT_CONFIRM_SCANS = 3
STRONG_TO_BLOCK_CONFIRM_SCANS = 1

# Old-core style thresholds.
BLOCK_RED_RATIO = 0.68
BLOCK_AVG_CHANGE = -1.20
BLOCK_BTC_CHANGE = -0.70
BLOCK_BTC_RED_RATIO = 0.55
ALT_WEAK_RED_RATIO = 0.60

# Fast emergency BLOCK path.
FAST_BLOCK_BTC_15M = -1.10
FAST_BLOCK_RED_RATIO = 0.80
FAST_BLOCK_AVG = -1.80

NO_LONGER_CRASHING_RED_RATIO = 0.62
NO_LONGER_CRASHING_AVG = -0.75
NO_LONGER_CRASHING_BTC = -0.45

RECOVERY_READY_RED_RATIO = 0.58
RECOVERY_READY_AVG = -0.55
RECOVERY_READY_BTC = -0.32

NORMAL_READY_RED_RATIO = 0.52
NORMAL_READY_AVG = -0.25
NORMAL_READY_BTC = -0.15

RECOVERY_SOFT_FAIL_RED_RATIO = 0.62
RECOVERY_SOFT_FAIL_AVG = -0.45
RECOVERY_SOFT_FAIL_BTC = -0.45
RECOVERY_HARD_FAIL_RED_RATIO = 0.70
RECOVERY_HARD_FAIL_AVG = -0.75
RECOVERY_HARD_FAIL_BTC = -0.75

# Recovery is a temporary post-BLOCK fast-rebound mode.
# NORMAL_LONG is the healthy/green market; STRONG_LONG_ONLY is the defensive
# pressure mode. These thresholds only help Recovery leave quickly when the
# rebound is no longer a fresh bounce but the market is still not green enough.
RECOVERY_TO_STRONG_RED_RATIO = 0.58
RECOVERY_TO_STRONG_AVG = -0.35
RECOVERY_TO_STRONG_BTC = -0.30
RECOVERY_TO_STRONG_MIN_STRONG_COINS = 4


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
    controlled_rebound_edge = bool(
        snapshot.btc_reclaim
        and snapshot.breadth_improving
        and red_ratio <= 0.58
        and avg > -0.55
        and btc > -0.35
        and strong >= 5
    )
    # Do not treat a plain healthy/strong snapshot as Recovery. Recovery is a
    # fast post-crash rebound path, not a replacement for NORMAL/STRONG.
    return bool(old_core_recovery and (snapshot.fast_rebound or controlled_rebound_edge) or fast_recovery_edge)

def _has_hourly_ma5_pressure(snapshot: MarketSnapshot) -> bool:
    return bool(getattr(snapshot, "hourly_ma5_pressure", False))


def _is_normal_ready(snapshot: MarketSnapshot) -> bool:
    red_ratio, avg, btc, strong = _values(snapshot)
    return bool(
        red_ratio < NORMAL_READY_RED_RATIO
        and avg > NORMAL_READY_AVG
        and btc > NORMAL_READY_BTC
        and strong >= 6
        and not _has_hourly_ma5_pressure(snapshot)
    )


def _is_recovery_to_strong_ready(snapshot: MarketSnapshot) -> bool:
    red_ratio, avg, btc, strong = _values(snapshot)
    pressure_but_stable = (
        red_ratio <= RECOVERY_TO_STRONG_RED_RATIO
        and avg >= RECOVERY_TO_STRONG_AVG
        and btc >= RECOVERY_TO_STRONG_BTC
        and strong >= RECOVERY_TO_STRONG_MIN_STRONG_COINS
    )
    recovery_edge_fading = bool(not snapshot.fast_rebound or snapshot.breadth_improving or snapshot.btc_reclaim)
    return bool(pressure_but_stable and recovery_edge_fading)

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

    # Fast emergency panic path.
    fast_block_trigger = bool(
        (
            btc <= FAST_BLOCK_BTC_15M
            and red_ratio >= FAST_BLOCK_RED_RATIO
        )
        or
        (
            avg <= FAST_BLOCK_AVG
            and red_ratio >= 0.85
        )
    )

    no_longer_crashing = _is_no_longer_crashing(snapshot)
    recovery_ready = _is_recovery_ready(snapshot)
    normal_ready = _is_normal_ready(snapshot)
    recovery_to_strong_ready = _is_recovery_to_strong_ready(snapshot)

    # Stabilization prevents breadth-only BLOCK and pushes the system toward STRONG.
    stabilizing = bool(no_longer_crashing or snapshot.breadth_improving or snapshot.btc_reclaim)

    real_block = bool((broad_market_crash or btc_breakdown or alt_weak_pressure) and not stabilizing)
    hourly_ma5_pressure = _has_hourly_ma5_pressure(snapshot)
    weak_breadth = bool(
        red_ratio >= 0.56
        or avg <= -0.30
        or btc <= -0.25
        or strong <= 4
        or hourly_ma5_pressure
    )

    return {
        "broad_market_crash": broad_market_crash,
        "btc_breakdown": btc_breakdown,
        "alt_weak_pressure": alt_weak_pressure,
        "fast_block_trigger": fast_block_trigger,
        "weak_breadth": weak_breadth,
        "stabilizing": stabilizing,
        "no_longer_crashing": no_longer_crashing,
        "recovery_ready": recovery_ready,
        "normal_ready": normal_ready,
        "hourly_ma5_pressure": hourly_ma5_pressure,
        "recovery_to_strong_ready": bool(recovery_to_strong_ready and not real_block),
        "real_block": real_block,
    }



def _recovery_hard_fail(snapshot: MarketSnapshot) -> bool:
    red_ratio, avg, btc, strong = _values(snapshot)
    return bool(
        red_ratio >= RECOVERY_HARD_FAIL_RED_RATIO
        or avg <= RECOVERY_HARD_FAIL_AVG
        or btc <= RECOVERY_HARD_FAIL_BTC
    )


def _recovery_soft_fail(snapshot: MarketSnapshot) -> bool:
    red_ratio, avg, btc, strong = _values(snapshot)
    return bool(
        red_ratio >= RECOVERY_SOFT_FAIL_RED_RATIO
        or avg <= RECOVERY_SOFT_FAIL_AVG
        or btc <= RECOVERY_SOFT_FAIL_BTC
        or strong <= 3
    )

def _base_mode(snapshot: MarketSnapshot) -> str:
    flags = _risk_flags(snapshot)

    # Fast emergency BLOCK path.
    if flags["fast_block_trigger"]:
        return MODE_BLOCK_LONGS

    if flags["real_block"]:
        return MODE_BLOCK_LONGS

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
    improving = flags["no_longer_crashing"] or flags["recovery_ready"] or flags["recovery_to_strong_ready"] or flags["stabilizing"]
    weakening = flags["real_block"]
    next_state.consecutive_improvement_scans = previous.consecutive_improvement_scans + 1 if improving else 0
    next_state.consecutive_weak_scans = previous.consecutive_weak_scans + 1 if weakening else 0

    candidate_mode = previous.mode

    if previous.mode == MODE_BLOCK_LONGS:
        # Fast exit path: BLOCK -> RECOVERY only on a real rebound edge.
        if flags["recovery_ready"]:
            candidate_mode = MODE_RECOVERY_LONG
        # Keep BLOCK stable for a short minimum hold unless a real rebound is present.
        elif minutes_in_mode < BLOCK_MIN_HOLD_MINUTES:
            candidate_mode = MODE_BLOCK_LONGS
        # Slow/safe exit path: crash stopped but market is still pressured -> STRONG.
        elif flags["no_longer_crashing"] or flags["stabilizing"] or not flags["real_block"]:
            if next_state.consecutive_improvement_scans >= BLOCK_EXIT_CONFIRM_SCANS:
                candidate_mode = MODE_STRONG_LONG_ONLY
            else:
                candidate_mode = MODE_BLOCK_LONGS
        else:
            candidate_mode = MODE_BLOCK_LONGS

    elif previous.mode == MODE_STRONG_LONG_ONLY:
        # Enter BLOCK from STRONG immediately on confirmed real breakdown.
        if flags["real_block"] and next_state.consecutive_weak_scans >= STRONG_TO_BLOCK_CONFIRM_SCANS:
            candidate_mode = MODE_BLOCK_LONGS
        elif flags["normal_ready"]:
            candidate_mode = MODE_NORMAL_LONG
        else:
            # STRONG is the pressure mode. Do not jump to RECOVERY unless a
            # BLOCK/crash path happened first.
            candidate_mode = MODE_STRONG_LONG_ONLY

    elif previous.mode == MODE_RECOVERY_LONG:
        if flags["real_block"] or _recovery_hard_fail(snapshot):
            candidate_mode = MODE_BLOCK_LONGS
        elif flags["normal_ready"]:
            candidate_mode = MODE_NORMAL_LONG
        elif _recovery_soft_fail(snapshot) and not flags["recovery_ready"]:
            candidate_mode = MODE_STRONG_LONG_ONLY
        elif previous.recovery_cycle_started_at and now - previous.recovery_cycle_started_at >= timedelta(minutes=RECOVERY_WINDOW_MINUTES):
            candidate_mode = MODE_STRONG_LONG_ONLY
        elif flags["recovery_to_strong_ready"] and not flags["recovery_ready"]:
            candidate_mode = MODE_STRONG_LONG_ONLY
        elif flags["recovery_ready"]:
            candidate_mode = MODE_RECOVERY_LONG
        else:
            # Recovery is temporary. If the bounce fades without a new crash,
            # fall back to STRONG pressure mode.
            candidate_mode = MODE_STRONG_LONG_ONLY

    else:  # NORMAL_LONG
        if flags["real_block"]:
            candidate_mode = MODE_BLOCK_LONGS
        elif flags["weak_breadth"]:
            # NORMAL is the healthy/green market. Weakness from NORMAL becomes
            # STRONG pressure mode. Rebound-like noise alone does not create
            # RECOVERY from NORMAL.
            candidate_mode = MODE_STRONG_LONG_ONLY
        else:
            candidate_mode = MODE_NORMAL_LONG

    # Anti-flapping cooldown with faster defensive exits from NORMAL and slower return to NORMAL.
    if candidate_mode != previous.mode:
        required_cooldown = MODE_CHANGE_COOLDOWN_MINUTES
        if previous.mode == MODE_NORMAL_LONG and candidate_mode == MODE_STRONG_LONG_ONLY:
            required_cooldown = NORMAL_EXIT_COOLDOWN_MINUTES
        elif previous.mode in (MODE_STRONG_LONG_ONLY, MODE_RECOVERY_LONG) and candidate_mode == MODE_NORMAL_LONG:
            required_cooldown = RETURN_TO_NORMAL_COOLDOWN_MINUTES

        if minutes_in_mode < required_cooldown:
            if previous.mode == MODE_BLOCK_LONGS and candidate_mode in (MODE_STRONG_LONG_ONLY, MODE_RECOVERY_LONG):
                pass
            elif candidate_mode == MODE_BLOCK_LONGS and (flags["real_block"] or previous.mode == MODE_RECOVERY_LONG):
                pass
            elif previous.mode == MODE_RECOVERY_LONG and candidate_mode == MODE_STRONG_LONG_ONLY and (flags["recovery_to_strong_ready"] or _recovery_soft_fail(snapshot)):
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
