"""Trade lifecycle with partial exits, protected runners, and path-specific target splits."""
from __future__ import annotations

from datetime import datetime, timezone

from utils.constants import TRAILING_STOP_AFTER_TP2_PCT, BREAKEVEN_BUFFER_PCT
from .models import TrackedTrade


_CLOSED_STATUSES = {"closed_win", "closed_loss", "breakeven_after_tp1", "trailing_hit", "expired"}


def _pnl_pct(entry: float, price: float) -> float:
    return ((price - entry) / entry) * 100.0 if entry else 0.0


def _mark_closed(trade: TrackedTrade, status: str) -> TrackedTrade:
    trade.status = status
    trade.closed_at = trade.closed_at or datetime.now(timezone.utc)
    trade.updated_at = datetime.now(timezone.utc)
    trade.slot_exempt = True
    trade.daily_open_risk_exempt = True
    trade.same_symbol_block_exempt = True
    if not trade.slot_exempt_reason:
        trade.slot_exempt_reason = status
    return trade


def _mark_protected_runner(trade: TrackedTrade) -> TrackedTrade:
    """After TP2 + SL at entry/better, the remaining runner is risk-exempt."""
    if trade.tp2_hit and trade.sl_moved_to_entry:
        trade.protected_runner = True
        trade.slot_exempt = True
        trade.daily_open_risk_exempt = True
        trade.same_symbol_block_exempt = True
        trade.slot_exempt_reason = "tp2_protected_runner"
        buffered_entry = trade.entry * (1 + BREAKEVEN_BUFFER_PCT / 100.0)
        trade.protected_sl = max(float(trade.protected_sl or 0.0), float(buffered_entry or trade.entry or 0.0))
    return trade


def apply_block_protection(trade: TrackedTrade, protection_level: int) -> TrackedTrade:
    if protection_level <= 1 or trade.status in _CLOSED_STATUSES:
        return trade
    if protection_level >= 2 and trade.pnl_pct > 0:
        trade.protected_on_block = True
        trade.protection_level = max(trade.protection_level, 2)
        trade.protected_reason = "market_mode_block_longs"
        buffered_entry = trade.entry * (1 + BREAKEVEN_BUFFER_PCT / 100.0)
        # v135 lifecycle: TP1 alone does not move SL to entry. BLOCK soft protection
        # tightens only TP2 protected runners; no panic move for TP1-only trades.
        if trade.tp2_hit:
            trade.trailing_tightened = True
            trade.protected_sl = max(trade.protected_sl or 0.0, buffered_entry)
    if protection_level >= 3 and trade.pnl_pct > 0:
        trade.protection_level = 3
        if trade.tp2_hit:
            trade.trailing_tightened = True
            trade.protected_sl = max(trade.protected_sl or 0.0, trade.entry * (1 + BREAKEVEN_BUFFER_PCT / 100.0))
    return trade


def update_trade_with_price(trade: TrackedTrade, current_price: float, protection_level: int = 0) -> TrackedTrade:
    if trade.status in _CLOSED_STATUSES:
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
        status = "closed_loss" if active_sl < trade.entry else "breakeven_after_tp1"
        trade.closed_portion_pct = 100.0
        trade.realized_pnl_pct = _pnl_pct(trade.entry, active_sl)
        trade.runner_pnl_pct = 0.0
        return _mark_closed(trade, status)

    tp1_close_pct = float(trade.tp1_close_pct or 40.0)
    tp2_close_pct = float(trade.tp2_close_pct or 40.0)
    runner_close_pct = float(trade.runner_close_pct or 20.0)

    if not trade.tp1_hit and current_price >= trade.tp1:
        trade.tp1_hit = True
        # v135 official lifecycle: TP1 closes 40% only. SL is NOT moved to entry here.
        trade.sl_moved_to_entry = False
        trade.closed_portion_pct = tp1_close_pct
        trade.realized_pnl_pct += _pnl_pct(trade.entry, trade.tp1) * (tp1_close_pct / 100.0)
        trade.status = "tp1_partial"

    post_tp1_sl = active_sl
    if trade.tp1_hit and not trade.tp2_hit and current_price <= post_tp1_sl:
        trade.closed_portion_pct = 100.0
        remaining_pct = max(0.0, 100.0 - tp1_close_pct)
        # TP1-only then original SL = partial win + remaining loss, not breakeven.
        trade.realized_pnl_pct += _pnl_pct(trade.entry, post_tp1_sl) * (remaining_pct / 100.0)
        trade.runner_pnl_pct = 0.0
        final_status = "closed_win" if float(trade.realized_pnl_pct or 0.0) > 0 else "closed_loss"
        return _mark_closed(trade, final_status)

    if trade.tp1_hit and not trade.tp2_hit and current_price >= trade.tp2:
        trade.tp2_hit = True
        trade.trailing_active = True
        trade.runner_active = True
        trade.sl_moved_to_entry = True
        trade.closed_portion_pct = tp1_close_pct + tp2_close_pct
        trade.realized_pnl_pct += _pnl_pct(trade.entry, trade.tp2) * (tp2_close_pct / 100.0)
        trade.status = "tp2_partial"
        trade = _mark_protected_runner(trade)

    if trade.tp2_hit:
        trade = _mark_protected_runner(trade)
        trail_pct = max(0.9, TRAILING_STOP_AFTER_TP2_PCT - (0.6 if trade.trailing_tightened else 0.0))
        trail_anchor = max(trade.highest_price, trade.tp2)
        trailing_stop_price = max(trail_anchor * (1 - trail_pct / 100.0), trade.protected_sl or trade.entry)
        trade.runner_pnl_pct = _pnl_pct(trade.entry, current_price) * (runner_close_pct / 100.0)
        if current_price <= trailing_stop_price:
            trade.closed_portion_pct = 100.0
            trade.realized_pnl_pct += _pnl_pct(trade.entry, trailing_stop_price) * (runner_close_pct / 100.0)
            trade.runner_pnl_pct = 0.0
            trade.runner_active = False
            trade.protected_runner = False
            return _mark_closed(trade, "trailing_hit")
        trade.status = "runner"

    return trade
