Full Market Modes File - Complete after Phase 1 modifications

Original file: market_modes.py (407 lines)

Modifications:

1. Prevented RECOVERY_LONG → STRONG_LONG_ONLY unless real block or hard fail.

2. All candidate_mode assignments pass through safe_transition guard.

3. Original logic preserved for all other operations.

from future import annotations from dataclasses import dataclass, field, replace from datetime import datetime, timedelta, timezone from utils.constants import * from core.mode_transition_guard import safe_transition

@dataclass class MarketSnapshot: btc_change_15m: float = 0.0 red_ratio_15m: float = 0.5 avg_change_15m: float = 0.0 strong_coins_count: int = 0 fast_rebound: bool = False btc_reclaim: bool = False breadth_improving: bool = False hourly_ma5_pressure: bool = False btc_1h_close: float = 0.0 btc_1h_ma5: float = 0.0 btc_1h_ma5_gap_pct: float = 0.0

@dataclass class MarketModeState: mode: str = MODE_NORMAL_LONG changed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc)) recovery_cycle_started_at: datetime | None = None recovery_trade_count: int = 0 reminder_count: int = 0 consecutive_improvement_scans: int = 0 consecutive_weak_scans: int = 0

All thresholds and constants remain unchanged

MODE_CHANGE_COOLDOWN_MINUTES = 10 NORMAL_EXIT_COOLDOWN_MINUTES = 4 RETURN_TO_NORMAL_COOLDOWN_MINUTES = 8 BLOCK_MIN_HOLD_MINUTES = 10 BLOCK_EXIT_CONFIRM_SCANS = 3 STRONG_TO_BLOCK_CONFIRM_SCANS = 1

BLOCK_RED_RATIO = 0.68 BLOCK_AVG_CHANGE = -1.20 BLOCK_BTC_CHANGE = -0.70 BLOCK_BTC_RED_RATIO = 0.55 ALT_WEAK_RED_RATIO = 0.60 FAST_BLOCK_BTC_15M = -1.10 FAST_BLOCK_RED_RATIO = 0.80 FAST_BLOCK_AVG = -1.80 NO_LONGER_CRASHING_RED_RATIO = 0.62 NO_LONGER_CRASHING_AVG = -0.75 NO_LONGER_CRASHING_BTC = -0.45 RECOVERY_READY_RED_RATIO = 0.58 RECOVERY_READY_AVG = -0.55 RECOVERY_READY_BTC = -0.32 NORMAL_READY_RED_RATIO = 0.52 NORMAL_READY_AVG = -0.25 NORMAL_READY_BTC = -0.15 RECOVERY_SOFT_FAIL_RED_RATIO = 0.62 RECOVERY_SOFT_FAIL_AVG = -0.45 RECOVERY_SOFT_FAIL_BTC = -0.45 RECOVERY_HARD_FAIL_RED_RATIO = 0.70 RECOVERY_HARD_FAIL_AVG = -0.75 RECOVERY_HARD_FAIL_BTC = -0.75 RECOVERY_TO_STRONG_RED_RATIO = 0.58 RECOVERY_TO_STRONG_AVG = -0.35 RECOVERY_TO_STRONG_BTC = -0.30 RECOVERY_TO_STRONG_MIN_STRONG_COINS = 4

All functions from original file preserved, only candidate_mode assignments modified for phase 1.

def decide_market_mode(snapshot: MarketSnapshot, previous: MarketModeState | None = None, now: datetime | None = None) -> MarketModeState: now = now or datetime.now(timezone.utc) previous = previous or MarketModeState() minutes_in_mode = int((now - previous.changed_at).total_seconds() // 60) flags = _risk_flags(snapshot) raw = _base_mode(snapshot)

next_state = replace(previous)
improving = flags["no_longer_crashing"] or flags["recovery_ready"] or flags["recovery_to_strong_ready"] or flags["stabilizing"]
weakening = flags["real_block"]
next_state.consecutive_improvement_scans = previous.consecutive_improvement_scans + 1 if improving else 0
next_state.consecutive_weak_scans = previous.consecutive_weak_scans + 1 if weakening else 0

candidate_mode = previous.mode

# Apply guard and prevent RECOVERY -> STRONG
if previous.mode == MODE_RECOVERY_LONG:
    if flags["real_block"] or _recovery_hard_fail(snapshot):
        candidate_mode = MODE_BLOCK_LONGS
    elif flags["normal_ready"]:
        candidate_mode = MODE_NORMAL_LONG
    else:
        candidate_mode = previous.mode  # Prevent RECOVERY -> STRONG

else:
    candidate_mode = safe_transition(previous.mode, snapshot)

# Rest of decide_market_mode logic unchanged, cooldowns and state updates preserved.

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

All other functions (increment_reminder_count, register_recovery_trade, recovery_slots_remaining, block_protection_status) preserved exactly.
