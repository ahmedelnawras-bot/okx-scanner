from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from utils.constants import (
    TRAILING_STOP_AFTER_TP2_PCT,
    BREAKEVEN_BUFFER_PCT,
    TP1_CLOSE_PCT,
    TP2_CLOSE_PCT,
    RUNNER_CLOSE_PCT,
)

try:
    from config.risk_config import (
        TRAILING_ADAPTIVE_MULTIPLIER,
        TRAILING_ADAPTIVE_FLOOR_PCT,
        TRAILING_ADAPTIVE_CEILING_PCT,
        RUNNER_HARD_STOP_RAW_PCT,
    )
except Exception:
    TRAILING_ADAPTIVE_MULTIPLIER = 1.3
    TRAILING_ADAPTIVE_FLOOR_PCT = 2.0
    TRAILING_ADAPTIVE_CEILING_PCT = 4.5
    RUNNER_HARD_STOP_RAW_PCT = -8.0
from .models import TrackedTrade


_CLOSED_STATUSES = {"closed_win", "closed_loss", "breakeven_after_tp1", "protected_entry_exit", "trailing_hit", "expired"}

_FILLED_STATES = {
    "filled",
    "full_fill",
    "fully_filled",
    "completed",
    "complete",
}
_PARTIAL_STATES = {
    "partially_filled",
    "partial_fill",
    "partiallyfilled",
    "partialfilled",
}


def _safe_setattr(obj: Any, name: str, value: Any) -> None:
    try:
        setattr(obj, name, value)
    except Exception:
        pass


def _pnl_pct(entry: float, price: float) -> float:
    return ((price - entry) / entry) * 100.0 if entry else 0.0


def _normalize_state(value: Any) -> str:
    return str(value or "").strip().lower()


def _is_filled_state(value: Any) -> bool:
    return _normalize_state(value) in _FILLED_STATES


def _is_partial_state(value: Any) -> bool:
    return _normalize_state(value) in _PARTIAL_STATES


def _has_exchange_tp_metadata(trade: TrackedTrade) -> bool:
    return bool(
        getattr(trade, "tp1_order_id", None)
        or getattr(trade, "tp2_order_id", None)
        or getattr(trade, "tp1_exchange_state", None)
        or getattr(trade, "tp2_exchange_state", None)
    )


def _entry_is_live_or_filled(trade: TrackedTrade) -> bool:
    fill_ratio = float(getattr(trade, "entry_fill_ratio", 0.0) or 0.0)
    fill_state = _normalize_state(getattr(trade, "entry_fill_state", "") or getattr(trade, "exchange_sync_state", ""))
    if fill_ratio > 0:
        return True
    if any(token in fill_state for token in ("filled", "partial", "live", "executed", "open")):
        return True
    # Backward-compatible fallback for old tracked trades that do not store exchange fill metadata.
    return True


def _tp1_confirmed(trade: TrackedTrade, current_price: float) -> tuple[bool, str]:
    state = _normalize_state(getattr(trade, "tp1_exchange_state", ""))
    if _has_exchange_tp_metadata(trade):
        if _is_filled_state(state):
            return True, "exchange"
        if _is_partial_state(state):
            return False, "exchange_partial"
        return False, "exchange_wait"
    return current_price >= trade.tp1, "price"


def _tp2_confirmed(trade: TrackedTrade, current_price: float) -> tuple[bool, str]:
    state = _normalize_state(getattr(trade, "tp2_exchange_state", ""))
    if _has_exchange_tp_metadata(trade):
        if _is_filled_state(state):
            return True, "exchange"
        if _is_partial_state(state):
            return False, "exchange_partial"
        return False, "exchange_wait"
    return current_price >= trade.tp2, "price"


def _stamp_once(trade: TrackedTrade, field_name: str, now: datetime | None = None) -> None:
    """Set a timestamp field only once, if the enhanced model supports it."""
    try:
        if getattr(trade, field_name, None) is None:
            setattr(trade, field_name, now or datetime.now(timezone.utc))
    except Exception:
        pass


def _update_exit_analysis(trade: TrackedTrade) -> None:
    """Best-effort analytics only; does not affect trade management."""
    try:
        mfe = max(0.0, float(getattr(trade, "max_favorable_pct", 0.0) or 0.0))
        realized = float(getattr(trade, "realized_pnl_pct", 0.0) or 0.0)
        if mfe > 0:
            efficiency = max(0.0, min(100.0, (realized / mfe) * 100.0))
            _safe_setattr(trade, "exit_efficiency_pct", round(efficiency, 4))
            _safe_setattr(trade, "missed_runner_profit_pct", round(max(0.0, mfe - max(0.0, realized)), 4))
        else:
            _safe_setattr(trade, "exit_efficiency_pct", 0.0)
            _safe_setattr(trade, "missed_runner_profit_pct", 0.0)
    except Exception:
        pass


def _mark_closed(trade: TrackedTrade, status: str) -> TrackedTrade:
    trade.status = status
    trade.closed_at = trade.closed_at or datetime.now(timezone.utc)
    trade.updated_at = datetime.now(timezone.utc)
    trade.slot_exempt = True
    trade.daily_open_risk_exempt = True
    trade.same_symbol_block_exempt = True
    trade.runner_active = False
    trade.protected_runner = False
    _update_exit_analysis(trade)
    if not trade.slot_exempt_reason:
        trade.slot_exempt_reason = status
    return trade


def _mark_protected_runner(trade: TrackedTrade) -> TrackedTrade:
    """After TP2, the remaining runner stays open for tracking only.

    SL بيتنقل لـ TP1 (مش entry) — هذا يضمن ربح محمي على الـ runner.
    """
    if trade.tp2_hit:
        # ✅ تحصين: protected_sl لازم يتحط لـ TP1 بعد TP2 حتى لو sl_moved_to_entry
        # مش متفعّل (صفقات قديمة / مسار غير متوقع). الشرط القديم كان مزدوجاً
        # وممكن يفشل فيترك الـ runner بدون حماية.
        trade.protected_runner = True
        trade.slot_exempt = True
        trade.daily_open_risk_exempt = True
        trade.same_symbol_block_exempt = True
        trade.slot_exempt_reason = "tp2_protected_runner"
        trade.sl_moved_to_entry = True
        # SL ينتقل لـ TP1 بعد TP2 — أعلى من entry = حماية أفضل للـ runner
        tp1_price = float(getattr(trade, "tp1", 0.0) or 0.0)
        fallback = float(trade.entry or 0.0)
        sl_floor = tp1_price if tp1_price > fallback else fallback
        trade.protected_sl = max(float(trade.protected_sl or 0.0), sl_floor)
    return trade


def apply_block_protection(trade: TrackedTrade, protection_level: int) -> TrackedTrade:
    if protection_level <= 1 or trade.status in _CLOSED_STATUSES:
        return trade
    if protection_level >= 2 and trade.pnl_pct > 0:
        trade.protected_on_block = True
        trade.protection_level = max(trade.protection_level, 2)
        trade.protected_reason = "market_mode_block_longs"
        buffered_entry = trade.entry * (1 + BREAKEVEN_BUFFER_PCT / 100.0)
        if trade.tp2_hit:
            if not trade.trailing_tightened:
                _stamp_once(trade, "trailing_tightened_at")
            trade.trailing_tightened = True
            trade.protected_sl = max(trade.protected_sl or 0.0, buffered_entry)
        elif not trade.tp1_hit:
            trade.protected_sl = max(trade.protected_sl or 0.0, trade.entry)
        else:
            trade.protected_sl = max(trade.protected_sl or 0.0, buffered_entry)
    if protection_level >= 3 and trade.pnl_pct > 0:
        trade.protection_level = 3
        if trade.tp2_hit and not trade.trailing_tightened:
            _stamp_once(trade, "trailing_tightened_at")
        trade.trailing_tightened = trade.tp2_hit or trade.trailing_tightened
        trade.protected_sl = max(trade.protected_sl or 0.0, trade.entry * (1 + BREAKEVEN_BUFFER_PCT / 100.0))
    return trade


def _apply_tp1_partial(trade: TrackedTrade, tp1_close_pct: float, source: str) -> TrackedTrade:
    if trade.tp1_hit:
        return trade
    trade.tp1_hit = True
    _stamp_once(trade, "tp1_hit_at")
    trade.closed_portion_pct = tp1_close_pct
    trade.realized_pnl_pct += _pnl_pct(trade.entry, trade.tp1) * (tp1_close_pct / 100.0)
    trade.status = "tp1_partial"
    _safe_setattr(trade, "exchange_sync_state", "tp1_filled_exchange" if source == "exchange" else "tp1_touched")
    return trade


def _apply_tp2_partial(trade: TrackedTrade, tp1_close_pct: float, tp2_close_pct: float, source: str) -> TrackedTrade:
    if not trade.tp1_hit:
        trade = _apply_tp1_partial(trade, tp1_close_pct, source)
    if trade.tp2_hit:
        return trade
    trade.tp2_hit = True
    _stamp_once(trade, "tp2_hit_at")
    trade.trailing_active = True
    _stamp_once(trade, "trailing_started_at")
    trade.runner_active = True
    trade.sl_moved_to_entry = True   # backward compat — يفضل True عشان post_tp1_sl logic
    _stamp_once(trade, "sl_move_to_entry_at")
    trade.sl_moved_to_tp1 = True     # ✅ الـ flag الجديد — SL انتقل لـ TP1
    _stamp_once(trade, "sl_move_to_tp1_at")
    trade.closed_portion_pct = tp1_close_pct + tp2_close_pct
    trade.realized_pnl_pct += _pnl_pct(trade.entry, trade.tp2) * (tp2_close_pct / 100.0)
    trade.status = "tp2_partial"
    _safe_setattr(trade, "exchange_sync_state", "tp2_filled_exchange" if source == "exchange" else "tp2_touched")
    return _mark_protected_runner(trade)


def update_trade_with_price(trade: TrackedTrade, current_price: float, protection_level: int = 0) -> TrackedTrade:
    if trade.status in _CLOSED_STATUSES:
        return trade

    # ✅ FIX #8 guard: لو السعر غير صالح (<= 0) لا تتخذ قرار SL/TP على سعر فاسد.
    # سيب الصفقة زي ما هي وعلّمها stale بدل ما تتقفل Direct SL غلط.
    if not (current_price and current_price > 0):
        trade.updated_at = datetime.now(timezone.utc)
        _safe_setattr(trade, "price_stale", True)
        _safe_setattr(trade, "price_stale_reason", "invalid_price_skipped_lifecycle")
        return trade

    if not _entry_is_live_or_filled(trade):
        trade.updated_at = datetime.now(timezone.utc)
        trade.current_price = current_price
        _safe_setattr(trade, "exchange_sync_state", _normalize_state(getattr(trade, "exchange_sync_state", "")) or "entry_wait")
        return trade

    now = datetime.now(timezone.utc)
    trade.updated_at = now
    trade.current_price = current_price
    trade.highest_price = max(trade.highest_price or trade.entry, current_price)
    trade.pnl_pct = _pnl_pct(trade.entry, current_price)
    trade.max_favorable_pct = max(float(trade.max_favorable_pct or 0.0), float(trade.pnl_pct or 0.0))
    trade.max_adverse_pct = min(float(trade.max_adverse_pct or 0.0), float(trade.pnl_pct or 0.0))

    trade = apply_block_protection(trade, protection_level)
    active_sl = max(trade.sl, trade.protected_sl or 0.0)

    # Direct SL before TP1: full loss/BE depending on active protection.
    if current_price <= active_sl and not trade.tp1_hit:
        # Do not label this as breakeven_after_tp1 unless TP1 was actually hit.
        # A protected/entry exit before TP1 gets its own status for cleaner reports.
        status = "closed_loss" if active_sl < trade.entry else "protected_entry_exit"
        trade.closed_portion_pct = 100.0
        trade.realized_pnl_pct = _pnl_pct(trade.entry, active_sl)
        trade.runner_pnl_pct = 0.0
        _safe_setattr(trade, "exchange_sync_state", "stopped_before_tp1")
        return _mark_closed(trade, status)

    # ✅ FIX #14: fallback كان 40/40/20 (قديم) ومخالف للمعتمد.
    # standard = 30/50/20 ، recovery = 50/25/25. نشتق الافتراضي من model/path الصفقة
    # (getattr بـ default آمن لو الحقول مش موجودة).
    _target_model = str(
        getattr(trade, "target_model", "")
        or getattr(trade, "execution_path", "")
        or ""
    ).lower()
    if "recovery" in _target_model or "50_25_25" in _target_model:
        _def_tp1, _def_tp2, _def_runner = 50.0, 25.0, 25.0
    else:
        _def_tp1, _def_tp2, _def_runner = TP1_CLOSE_PCT, TP2_CLOSE_PCT, RUNNER_CLOSE_PCT
    tp1_close_pct = float(trade.tp1_close_pct or _def_tp1)
    tp2_close_pct = float(trade.tp2_close_pct or _def_tp2)
    runner_close_pct = float(trade.runner_close_pct or _def_runner)

    tp1_ready, tp1_source = _tp1_confirmed(trade, current_price)
    if not trade.tp1_hit and tp1_ready:
        trade = _apply_tp1_partial(trade, tp1_close_pct, tp1_source)

    post_tp1_sl = max(active_sl, trade.entry if trade.sl_moved_to_entry else active_sl)
    if trade.tp1_hit and not trade.tp2_hit and current_price <= post_tp1_sl:
        trade.closed_portion_pct = 100.0
        remaining_pct = max(0.0, 100.0 - tp1_close_pct)
        trade.realized_pnl_pct += max(0.0, _pnl_pct(trade.entry, post_tp1_sl)) * (remaining_pct / 100.0)
        trade.runner_pnl_pct = 0.0
        _safe_setattr(trade, "exchange_sync_state", "post_tp1_breakeven")
        return _mark_closed(trade, "breakeven_after_tp1")

    tp2_ready, tp2_source = _tp2_confirmed(trade, current_price)
    if trade.tp1_hit and not trade.tp2_hit and tp2_ready:
        trade = _apply_tp2_partial(trade, tp1_close_pct, tp2_close_pct, tp2_source)

    if trade.tp2_hit:
        _stamp_once(trade, "trailing_started_at")
        trade = _mark_protected_runner(trade)

        # ✅ Runner Hard Stop (خط دفاع مطلق):
        # لو السعر انهار تحت entry بما يتجاوز RUNNER_HARD_STOP_RAW_PCT،
        # نقفل الـ runner فوراً — حماية ضد انهيار سريع أو فشل protected_sl
        # (صفقات قديمة / أخطاء sync). مستقل تماماً عن الـ trailing.
        _runner_raw_pnl = _pnl_pct(trade.entry, current_price)
        if _runner_raw_pnl <= RUNNER_HARD_STOP_RAW_PCT:
            runner_close_pct = float(trade.runner_close_pct or _def_runner)
            trade.closed_portion_pct = 100.0
            trade.realized_pnl_pct += _runner_raw_pnl * (runner_close_pct / 100.0)
            trade.runner_pnl_pct = 0.0
            trade.runner_active = False
            trade.protected_runner = False
            _safe_setattr(trade, "exchange_sync_state", "runner_hard_stop")
            print(
                f"🛑 RUNNER_HARD_STOP | {trade.symbol} | "
                f"raw_pnl={_runner_raw_pnl:.2f}% <= {RUNNER_HARD_STOP_RAW_PCT}% | إغلاق فوري",
                flush=True,
            )
            return _mark_closed(trade, "closed_loss")

        # ✅ Adaptive trailing: trail_pct يتكيّف مع تقلب العملة.
        # المصدر: entry_avg_range_pct (متوسط مدى الشمعة وقت الدخول).
        # عملة هادئة → trailing ضيّق؛ عملة متقلبة → trailing أوسع (مايضربش بسهولة).
        # fallback للثابت لو avg_range مش متوفر.
        _avg_range = float(getattr(trade, "entry_avg_range_pct", 0.0) or 0.0)
        if _avg_range > 0:
            _adaptive = _avg_range * TRAILING_ADAPTIVE_MULTIPLIER
            _base_trail = min(
                TRAILING_ADAPTIVE_CEILING_PCT,
                max(TRAILING_ADAPTIVE_FLOOR_PCT, _adaptive),
            )
        else:
            _base_trail = TRAILING_STOP_AFTER_TP2_PCT
        trail_pct = max(0.9, _base_trail - (0.6 if trade.trailing_tightened else 0.0))
        trail_anchor = max(trade.highest_price, trade.tp2)
        trailing_stop_price = max(trail_anchor * (1 - trail_pct / 100.0), trade.protected_sl or trade.entry)

        # ✅ FIX: حدّث protected_sl بالـ trailing stop الحالي كل scan
        # هذا يضمن إن _sync_stop_loss_to_exchange يبعت القيمة الصح للمنصة
        # الـ trailing stop بيتحرك للأعلى مع السعر → SL على المنصة بيتحرك معاه
        trade.protected_sl = max(float(trade.protected_sl or 0.0), trailing_stop_price)

        trade.runner_pnl_pct = _pnl_pct(trade.entry, current_price) * (runner_close_pct / 100.0)
        if current_price <= trailing_stop_price:
            trade.closed_portion_pct = 100.0
            trade.realized_pnl_pct += _pnl_pct(trade.entry, trailing_stop_price) * (runner_close_pct / 100.0)
            trade.runner_pnl_pct = 0.0
            trade.runner_active = False
            trade.protected_runner = False
            _safe_setattr(trade, "exchange_sync_state", "runner_trailing_hit")
            return _mark_closed(trade, "trailing_hit")
        trade.status = "runner"
        _safe_setattr(trade, "exchange_sync_state", _normalize_state(getattr(trade, "exchange_sync_state", "")) or "runner_active")

    return trade
