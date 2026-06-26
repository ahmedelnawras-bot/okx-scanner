"""Unified market mode engine - BTC drop triggers STRONG even if alts are strong.
Exit from BLOCK: weak rebound -> STRONG, strong rebound -> RECOVERY.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone

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
    btc_dominance_change_1h: float = 0.0


@dataclass
class MarketModeState:
    mode: str = MODE_NORMAL_LONG
    changed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    recovery_cycle_started_at: datetime | None = None
    recovery_trade_count: int = 0
    reminder_count: int = 0
    consecutive_improvement_scans: int = 0
    consecutive_weak_scans: int = 0
    consecutive_recovery_ready_scans: int = 0
    consecutive_normal_ready_scans: int = 0
    block_type: str = ""   # "FLASH" / "TREND" / "" — يتحدد فقط في وضع BLOCK


# Keep transitions stable
MODE_CHANGE_COOLDOWN_MINUTES = 10
NORMAL_EXIT_COOLDOWN_MINUTES = 1
RETURN_TO_NORMAL_COOLDOWN_MINUTES = 18

BLOCK_MIN_HOLD_MINUTES = 10
BLOCK_EXIT_CONFIRM_SCANS = 3
STRONG_TO_BLOCK_CONFIRM_SCANS = 3
NORMAL_RETURN_CONFIRM_SCANS = 2

# Recovery anti-whipsaw controls. Recovery must be a confirmed rebound, not a
# one-scan spike. Once entered, do not bounce back to NORMAL immediately.
RECOVERY_ENTRY_CONFIRM_SCANS = 3
RECOVERY_MIN_HOLD_MINUTES = 20
RECOVERY_TO_NORMAL_CONFIRM_SCANS = 4

# Old-core style thresholds
BLOCK_RED_RATIO = 0.66
BLOCK_AVG_CHANGE = -0.85
BLOCK_BTC_CHANGE = -0.55
BLOCK_BTC_RED_RATIO = 0.66
ALT_WEAK_RED_RATIO = 0.62

# ── Trend Filter (فلتر الاتجاه) ─────────────────────────────────────────────────
# الـ 15m بيقيس السرعة بس. النزول التدريجي (السعر تحت MA5 الساعة باستمرار)
# مكانش بيعمل BLOCK وكان بيخلي النظام يشتري في كل نزول → 8% WR في STRONG.
# لو ترند هابط مؤكد (ضغط MA5 + فجوة عميقة) → نعمل TREND_BLOCK بدل STRONG.
# قابل للإطفاء فوراً بـ TREND_BLOCK_ENABLED = False.
TREND_BLOCK_ENABLED = True
TREND_BLOCK_GAP_PCT = -0.40   # السعر أقل من MA5 الساعة بـ 0.40%+ = ترند هابط مؤكد

# تسميات نوع البلوك (داخلية — للوضوح في اللوجز والتقارير)
BLOCK_TYPE_FLASH = "FLASH"   # ⚡ انهيار سريع 15م — يسمح باستثناءات (ممكن ارتداد)
BLOCK_TYPE_TREND = "TREND"   # 📉 ترند هابط مستمر — بدون استثناءات (صيد سكين)

FAST_BLOCK_BTC_15M = -0.95
FAST_BLOCK_RED_RATIO = 0.80
FAST_BLOCK_AVG = -1.50

NO_LONGER_CRASHING_RED_RATIO = 0.60
NO_LONGER_CRASHING_AVG = -0.65
NO_LONGER_CRASHING_BTC = -0.38

RECOVERY_READY_RED_RATIO = 0.55      # ╪к┘Е ╪з┘Д╪к╪о┘Б┘К╪╢ ┘Е┘Ж 0.58 ┘Д┘К┘Г┘И┘Ж ╪г┘Г╪л╪▒ ╪з┘Ж╪к┘В╪з╪ж┘К╪й
RECOVERY_READY_AVG = -0.30           # ╪к┘Е ╪з┘Д╪▒┘Б╪╣ ┘Е┘Ж -0.55
RECOVERY_READY_BTC = -0.25           # ╪к┘Е ╪з┘Д╪▒┘Б╪╣ ┘Е┘Ж -0.32

NORMAL_READY_RED_RATIO = 0.44
NORMAL_READY_AVG = -0.05
NORMAL_READY_BTC = -0.05

RECOVERY_SOFT_FAIL_RED_RATIO = 0.62
RECOVERY_SOFT_FAIL_AVG = -0.45
RECOVERY_SOFT_FAIL_BTC = -0.45
RECOVERY_HARD_FAIL_RED_RATIO = 0.70
RECOVERY_HARD_FAIL_AVG = -0.75
RECOVERY_HARD_FAIL_BTC = -0.75

RECOVERY_TO_STRONG_RED_RATIO = 0.60
RECOVERY_TO_STRONG_AVG = -0.45
RECOVERY_TO_STRONG_BTC = -0.35
RECOVERY_TO_STRONG_MIN_STRONG_COINS = 4

# Recovery should be a short-lived rebound edge, not a broad "strong market" label.
# These guards prevent Recovery from staying active when the market is merely
# pressured/stabilizing; that case should downgrade to STRONG_LONG_ONLY.
RECOVERY_ENTRY_MAX_DOMINANCE_RISE = 0.20
RECOVERY_CONTINUE_MAX_DOMINANCE_RISE = 0.25
RECOVERY_CONTINUE_RED_RATIO = 0.58
RECOVERY_CONTINUE_AVG = -0.35
RECOVERY_CONTINUE_BTC = -0.30
RECOVERY_CONTINUE_MIN_STRONG_COINS = 6


MODE_DECISION_DEBUG = os.getenv("MODE_DECISION_DEBUG", "1").lower() in {"1", "true", "yes", "on"}


def _mode_debug_line(
    snapshot: MarketSnapshot,
    previous_mode: str,
    raw_mode: str,
    candidate_before_cooldown: str,
    final_mode: str,
    *,
    minutes_in_mode: int,
    flags: dict,
    cooldown_applied: bool,
    required_cooldown: int,
) -> str:
    red_ratio, avg, btc, strong = _values(snapshot)
    return (
        "ЁЯзн MODE DECISION"
        f" | prev={previous_mode}"
        f" | raw={raw_mode}"
        f" | candidate={candidate_before_cooldown}"
        f" | final={final_mode}"
        f" | mins={minutes_in_mode}"
        f" | need_cd={required_cooldown}"
        f" | cd_blocked={cooldown_applied}"
        f" | weak={int(bool(flags.get('weak_breadth')))}"
        f" | block={int(bool(flags.get('real_block')))}"
        f" | rec_ready={int(bool(flags.get('recovery_ready')))}"
        f" | norm_ready={int(bool(flags.get('normal_ready')))}"
        f" | stabilize={int(bool(flags.get('stabilizing')))}"
        f" | hourly_p={int(bool(flags.get('hourly_ma5_pressure')))}"
        f" | dom_ch={snapshot.btc_dominance_change_1h:+.2f}"
        f" | red={red_ratio:.2f}"
        f" | avg={avg:+.2f}"
        f" | btc={btc:+.2f}"
        f" | strong={strong}"
    )


def _print_mode_debug(
    snapshot: MarketSnapshot,
    previous_mode: str,
    raw_mode: str,
    candidate_before_cooldown: str,
    final_mode: str,
    *,
    minutes_in_mode: int,
    flags: dict,
    cooldown_applied: bool,
    required_cooldown: int,
) -> None:
    if not MODE_DECISION_DEBUG:
        return
    try:
        print(
            _mode_debug_line(
                snapshot,
                previous_mode,
                raw_mode,
                candidate_before_cooldown,
                final_mode,
                minutes_in_mode=minutes_in_mode,
                flags=flags,
                cooldown_applied=cooldown_applied,
                required_cooldown=required_cooldown,
            ),
            flush=True,
        )
    except Exception:
        pass


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
        or (btc > -0.55 and avg > -0.85 and red_ratio < 0.62)
        or (strong >= 5 and avg > -0.75 and btc > -0.55)
    )
    return bool(old_core_safe or stabilization_safe)


def _is_recovery_ready(snapshot: MarketSnapshot) -> bool:
    """Strong rebound condition for entering RECOVERY from BLOCK."""
    red_ratio, avg, btc, strong = _values(snapshot)
    # ╪к╪┤╪п┘К╪п ╪з┘Д╪┤╪▒┘И╪╖: ╪з╪▒╪к╪п╪з╪п ┘В┘И┘К ┘Б┘В╪╖
    strong_rebound = bool(
        snapshot.fast_rebound
        and snapshot.btc_reclaim
        and snapshot.breadth_improving
        and red_ratio <= RECOVERY_READY_RED_RATIO   # 0.55
        and avg > RECOVERY_READY_AVG               # -0.30
        and btc > RECOVERY_READY_BTC               # -0.25
        and strong >= 8
        and not _has_hourly_ma5_pressure(snapshot)
        and float(getattr(snapshot, "btc_dominance_change_1h", 0.0) or 0.0) <= RECOVERY_ENTRY_MAX_DOMINANCE_RISE
    )
    return strong_rebound


def _has_hourly_ma5_pressure(snapshot: MarketSnapshot) -> bool:
    return bool(getattr(snapshot, "hourly_ma5_pressure", False))


def _is_normal_ready(snapshot: MarketSnapshot) -> bool:
    red_ratio, avg, btc, strong = _values(snapshot)
    return bool(
        red_ratio < NORMAL_READY_RED_RATIO
        and avg > NORMAL_READY_AVG
        and btc > NORMAL_READY_BTC
        and strong >= 8
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
    # Previous logic used OR and therefore treated almost every improving scan
    # as "edge fading". Fading means the full rebound trio is no longer present.
    recovery_edge_fading = not (
        bool(snapshot.fast_rebound)
        and bool(snapshot.btc_reclaim)
        and bool(snapshot.breadth_improving)
    )
    return bool(pressure_but_stable and recovery_edge_fading)


def _is_recovery_edge_still_valid(snapshot: MarketSnapshot, flags: dict | None = None) -> bool:
    """Return True only while the rebound edge still deserves RECOVERY.

    If this returns False and the market is not healthy enough for NORMAL, the
    mode should downgrade to STRONG_LONG_ONLY. This keeps 90 minutes as a max
    cap, not as an implicit holding period.
    """
    flags = flags or {}
    red_ratio, avg, btc, strong = _values(snapshot)
    dom_change = float(getattr(snapshot, "btc_dominance_change_1h", 0.0) or 0.0)
    if flags.get("real_block") or _recovery_hard_fail(snapshot) or _recovery_soft_fail(snapshot):
        return False
    if _has_hourly_ma5_pressure(snapshot):
        return False
    if dom_change > RECOVERY_CONTINUE_MAX_DOMINANCE_RISE:
        return False

    strict_ready = bool(flags.get("recovery_ready") or _is_recovery_ready(snapshot))
    softer_continue = bool(
        snapshot.fast_rebound
        and (snapshot.btc_reclaim or snapshot.breadth_improving)
        and red_ratio <= RECOVERY_CONTINUE_RED_RATIO
        and avg >= RECOVERY_CONTINUE_AVG
        and btc >= RECOVERY_CONTINUE_BTC
        and strong >= RECOVERY_CONTINUE_MIN_STRONG_COINS
    )
    return bool(strict_ready or softer_continue)


def _risk_flags(snapshot: MarketSnapshot) -> dict:
    red_ratio, avg, btc, strong = _values(snapshot)
    dom_change = snapshot.btc_dominance_change_1h

    broad_market_crash = red_ratio >= BLOCK_RED_RATIO and avg <= BLOCK_AVG_CHANGE
    btc_breakdown = btc <= BLOCK_BTC_CHANGE and red_ratio >= BLOCK_BTC_RED_RATIO
    alt_weak_pressure = red_ratio >= ALT_WEAK_RED_RATIO and avg <= -0.65 and btc <= -0.30
    severe_breadth_pressure = bool(red_ratio >= 0.75 and avg <= -0.45 and strong <= 3)
    panic_breadth_pressure = bool(red_ratio >= 0.88 and avg <= -0.25 and strong <= 2)

    fast_block_trigger = bool(
        (
            btc <= FAST_BLOCK_BTC_15M
            and red_ratio >= FAST_BLOCK_RED_RATIO
            and avg <= -0.70
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

    stabilizing = bool(no_longer_crashing or snapshot.breadth_improving or snapshot.btc_reclaim)

    # ── Trend Block: ترند هابط مؤكد على الساعة = بلوك صلب ──
    # يتجاوز فيتو stabilizing لأن النزول التدريجي بيبان "مستقر" في كل 15م
    # لكنه فعلياً اتجاه هابط. يحتاج تأكيد مزدوج: ضغط MA5 + فجوة ≥0.40%.
    gap_pct = float(getattr(snapshot, "btc_1h_ma5_gap_pct", 0.0) or 0.0)
    btc_trend_block = bool(
        TREND_BLOCK_ENABLED
        and _has_hourly_ma5_pressure(snapshot)
        and gap_pct <= TREND_BLOCK_GAP_PCT
    )

    real_block_core = (broad_market_crash or btc_breakdown or alt_weak_pressure or severe_breadth_pressure or panic_breadth_pressure)
    real_block = bool(
        (real_block_core and not stabilizing and (red_ratio >= 0.60 or avg <= -0.50))
        or btc_trend_block
    )

    # نوع البلوك للوضوح ومنطق الاستثناءات: TREND أصلب من FLASH.
    # لو ترند هابط مؤكد → TREND (بدون استثناءات). غير كده → FLASH.
    block_type = BLOCK_TYPE_TREND if btc_trend_block else BLOCK_TYPE_FLASH

    hourly_ma5_pressure = _has_hourly_ma5_pressure(snapshot)

    # ========== ╪з┘Д┘Е┘Ж╪╖┘В ╪з┘Д╪м╪п┘К╪п ┘Д┘А weak_breadth ==========
    alt_weak = (
        red_ratio >= 0.50
        or avg <= -0.20
        or strong <= 5
    )
    btc_drop_alone = (btc <= -0.25)
    ma5_pressure = hourly_ma5_pressure
    weak_breadth = alt_weak or btc_drop_alone or ma5_pressure
    # ==================================================

    # тЬЕ BTC Dominance ┘Г╪╣╪з┘Е┘Д ┘Е╪│╪з╪╣╪п
    # dom_change > +0.3 тЖТ BTC.D ╪з╪▒╪к┘Б╪╣ тЖТ alts ╪╢╪╣┘К┘Б╪й тЖТ ┘К╪┤╪п╪п weak_breadth
    # dom_change < -0.3 тЖТ BTC.D ┘Ж╪▓┘Д тЖТ alts ┘В┘И┘К╪й тЖТ ┘К╪о┘Б┘Б weak_breadth
    if dom_change > 0.25 and not weak_breadth and (red_ratio >= 0.50 or avg <= -0.25):
        weak_breadth = True
        print(f"тЪая╕П DOM_PRESSURE: dom_change={dom_change:+.2f} тЖТ weak_breadth forced True", flush=True)
    elif dom_change < -0.25 and weak_breadth and red_ratio < 0.55 and avg > -0.30:
        weak_breadth = False
        print(f"тЬЕ DOM_RELIEF: dom_change={dom_change:+.2f} тЖТ weak_breadth relieved", flush=True)

    # ╪╖╪и╪з╪╣╪й DEBUG
    print(f"ЁЯФН RISK_FLAGS: red={red_ratio:.2f} avg={avg:+.2f} strong={strong} btc={btc:+.2f} | alt_weak={alt_weak} btc_drop={btc_drop_alone} ma5_press={ma5_pressure} | weak_breadth={weak_breadth} dom_change={dom_change:+.2f}")

    return {
        "broad_market_crash": broad_market_crash,
        "btc_breakdown": btc_breakdown,
        "alt_weak_pressure": alt_weak_pressure,
        "severe_breadth_pressure": severe_breadth_pressure,
        "panic_breadth_pressure": panic_breadth_pressure,
        "fast_block_trigger": fast_block_trigger,
        "weak_breadth": weak_breadth,
        "stabilizing": stabilizing,
        "no_longer_crashing": no_longer_crashing,
        "recovery_ready": recovery_ready,
        "normal_ready": normal_ready,
        "hourly_ma5_pressure": hourly_ma5_pressure,
        "recovery_to_strong_ready": bool(recovery_to_strong_ready and not real_block),
        "real_block": real_block,
        "btc_trend_block": btc_trend_block,
        "block_type": block_type,
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


def _recovery_age_minutes(state: MarketModeState, now: datetime) -> int:
    """Age of the active recovery cycle.

    RECOVERY_WINDOW_MINUTES is a maximum cap, not a forced holding period.
    If recovery_cycle_started_at is missing for an old state, changed_at is the
    safest fallback.
    """
    started = state.recovery_cycle_started_at or state.changed_at
    if not isinstance(started, datetime):
        return 0
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    return max(0, int((now - started).total_seconds() // 60))


def _recovery_window_expired(state: MarketModeState, now: datetime) -> bool:
    """Return True only after the configured max recovery window expires."""
    try:
        max_minutes = int(RECOVERY_WINDOW_MINUTES)
    except Exception:
        max_minutes = 90
    if max_minutes <= 0:
        return False
    return _recovery_age_minutes(state, now) >= max_minutes


def _base_mode(snapshot: MarketSnapshot) -> str:
    flags = _risk_flags(snapshot)
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
    next_state.consecutive_recovery_ready_scans = previous.consecutive_recovery_ready_scans + 1 if flags["recovery_ready"] else 0
    next_state.consecutive_normal_ready_scans = previous.consecutive_normal_ready_scans + 1 if flags["normal_ready"] else 0

    candidate_mode = previous.mode
    required_cooldown = MODE_CHANGE_COOLDOWN_MINUTES
    cooldown_applied = False

    if previous.mode == MODE_BLOCK_LONGS:
        # Exit BLOCK conservatively. Recovery is allowed only after a confirmed
        # rebound for several scans AND after the block has had time to protect
        # existing trades. One green spike must not create RECOVERY.
        block_min_hold_done = minutes_in_mode >= BLOCK_MIN_HOLD_MINUTES
        recovery_confirmed = (
            flags["recovery_ready"]
            and next_state.consecutive_recovery_ready_scans >= RECOVERY_ENTRY_CONFIRM_SCANS
            and block_min_hold_done
        )

        if recovery_confirmed:
            candidate_mode = MODE_RECOVERY_LONG
        elif flags["no_longer_crashing"] or flags["stabilizing"]:
            if next_state.consecutive_improvement_scans >= BLOCK_EXIT_CONFIRM_SCANS:
                # Market is improving but recovery is not fully confirmed: use
                # STRONG, not RECOVERY. This is the anti-rush layer.
                candidate_mode = MODE_STRONG_LONG_ONLY
            else:
                candidate_mode = MODE_BLOCK_LONGS
        else:
            candidate_mode = MODE_BLOCK_LONGS

    elif previous.mode == MODE_STRONG_LONG_ONLY:
        if flags["real_block"] and next_state.consecutive_weak_scans >= STRONG_TO_BLOCK_CONFIRM_SCANS:
            candidate_mode = MODE_BLOCK_LONGS
        elif flags["normal_ready"] and next_state.consecutive_improvement_scans >= NORMAL_RETURN_CONFIRM_SCANS:
            candidate_mode = MODE_NORMAL_LONG
        else:
            candidate_mode = MODE_STRONG_LONG_ONLY

    elif previous.mode == MODE_RECOVERY_LONG:
        # RECOVERY is an opportunistic rebound window, not a fixed 90-minute mode.
        # 90 minutes is only the maximum cap. Every scan can exit earlier:
        # - hard deterioration -> BLOCK
        # - fully healthy market -> NORMAL after confirmation
        # - rebound edge faded / market merely stabilizing -> STRONG
        recovery_expired = _recovery_window_expired(previous, now)
        recovery_edge_still_valid = _is_recovery_edge_still_valid(snapshot, flags)

        recovery_min_hold_done = _recovery_age_minutes(previous, now) >= RECOVERY_MIN_HOLD_MINUTES
        normal_confirmed = (
            flags["normal_ready"]
            and next_state.consecutive_normal_ready_scans >= RECOVERY_TO_NORMAL_CONFIRM_SCANS
            and recovery_min_hold_done
        )

        if flags["real_block"] or _recovery_hard_fail(snapshot):
            candidate_mode = MODE_BLOCK_LONGS
        elif normal_confirmed:
            candidate_mode = MODE_NORMAL_LONG
        elif recovery_expired:
            # Max window انتهى: ارجع للتقييم السوقي الحالي بدل تثبيت RECOVERY.
            # لو السوق لسه حساس يبقى STRONG، ولو آمن تمامًا يبقى NORMAL.
            candidate_mode = MODE_STRONG_LONG_ONLY if flags["weak_breadth"] or raw == MODE_STRONG_LONG_ONLY else MODE_NORMAL_LONG
        elif not recovery_edge_still_valid:
            # The rebound edge faded. Do NOT keep Recovery just because the
            # market is no longer crashing; this is exactly STRONG territory.
            candidate_mode = MODE_STRONG_LONG_ONLY
        else:
            candidate_mode = MODE_RECOVERY_LONG

    else:  # NORMAL_LONG
        if flags["real_block"]:
            candidate_mode = MODE_BLOCK_LONGS
        elif flags["weak_breadth"]:
            candidate_mode = MODE_STRONG_LONG_ONLY
        else:
            candidate_mode = MODE_NORMAL_LONG

    candidate_before_cooldown = candidate_mode

    if candidate_mode != previous.mode:
        required_cooldown = MODE_CHANGE_COOLDOWN_MINUTES
        if previous.mode == MODE_NORMAL_LONG and candidate_mode == MODE_STRONG_LONG_ONLY:
            required_cooldown = NORMAL_EXIT_COOLDOWN_MINUTES
        elif previous.mode in (MODE_STRONG_LONG_ONLY, MODE_RECOVERY_LONG) and candidate_mode == MODE_NORMAL_LONG:
            required_cooldown = RETURN_TO_NORMAL_COOLDOWN_MINUTES

        if minutes_in_mode < required_cooldown:
            if previous.mode == MODE_BLOCK_LONGS and candidate_mode in (MODE_STRONG_LONG_ONLY, MODE_RECOVERY_LONG):
                pass
            elif previous.mode == MODE_RECOVERY_LONG and candidate_mode == MODE_STRONG_LONG_ONLY:
                # Recovery -> Strong is a safety downgrade, not a bullish upgrade.
                # Do not hold stale Recovery just because the generic cooldown is active.
                pass
            elif candidate_mode == MODE_BLOCK_LONGS and (flags["real_block"] or previous.mode == MODE_RECOVERY_LONG):
                pass
            elif candidate_mode != MODE_BLOCK_LONGS:
                cooldown_applied = True
                candidate_mode = previous.mode

    changed = candidate_mode != previous.mode
    next_state.mode = candidate_mode
    next_state.changed_at = now if changed else previous.changed_at
    next_state.reminder_count = 0 if changed else previous.reminder_count

    # نوع البلوك: يتحدد فقط في وضع BLOCK. TREND لو ترند هابط مؤكد، غير كده FLASH.
    if candidate_mode == MODE_BLOCK_LONGS:
        next_state.block_type = flags.get("block_type") or BLOCK_TYPE_FLASH
    else:
        next_state.block_type = ""

    if changed:
        # Avoid carrying confirmation counters from the old mode into the new
        # mode. This prevents instant RECOVERY -> NORMAL flips after entry.
        next_state.consecutive_recovery_ready_scans = 1 if flags["recovery_ready"] else 0
        next_state.consecutive_normal_ready_scans = 1 if flags["normal_ready"] else 0

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

    _print_mode_debug(
        snapshot,
        previous.mode,
        raw,
        candidate_before_cooldown,
        next_state.mode,
        minutes_in_mode=minutes_in_mode,
        flags=flags,
        cooldown_applied=cooldown_applied,
        required_cooldown=required_cooldown,
    )
    return next_state


def increment_reminder_count(state: MarketModeState) -> MarketModeState:
    return replace(state, reminder_count=state.reminder_count + 1)


def current_block_type(state: MarketModeState | None) -> str:
    """نوع البلوك الحالي: "FLASH" / "TREND" / "". يستخدمه execution_processor
    لمنع الاستثناءات وقت TREND_BLOCK تحديداً."""
    if state is None or state.mode != MODE_BLOCK_LONGS:
        return ""
    return state.block_type or BLOCK_TYPE_FLASH


def block_allows_exceptions(state: MarketModeState | None) -> bool:
    """FLASH يسمح باستثناءات (ممكن ارتداد). TREND لا (ترند هابط مستمر)."""
    return current_block_type(state) != BLOCK_TYPE_TREND


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
    if minutes_in_mode < 5:
        return {"level": 1, "current": "LEVEL 1 тАФ Monitor Only", "next": "Soft Protection", "remaining_minutes": 5 - minutes_in_mode}
    if minutes_in_mode < 10:
        return {"level": 2, "current": "LEVEL 2 тАФ Soft Protection", "next": "Defensive Protection", "remaining_minutes": 10 - minutes_in_mode}
    if minutes_in_mode < 15:
        return {"level": 3, "current": "LEVEL 3 тАФ Defensive Protection", "next": "Max protection active", "remaining_minutes": 15 - minutes_in_mode}
    return {"level": 3, "current": "LEVEL 3 тАФ Defensive Protection", "next": "Max protection active", "remaining_minutes": 0}
