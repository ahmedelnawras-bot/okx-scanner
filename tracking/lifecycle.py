"""Lifecycle model preserves 40/40/20 understanding with partial exits and runner state."""
from __future__ import annotations

from utils.constants import TP1_CLOSE_PCT, TP2_CLOSE_PCT, RUNNER_CLOSE_PCT, TRAILING_STOP_AFTER_TP2_PCT, BREAKEVEN_BUFFER_PCT
from .models import TrackedTrade


def _pnl_pct(entry: float, price: float) -> float:
    return ((price - entry) / entry) * 100.0 if entry else 0.0


def apply_block_protection(trade: TrackedTrade, protection_level: int) -> TrackedTrade:
    if protection_level <= 1 or trade.status in {"closed_win", "closed_loss", "breakeven_after_tp1", "trailing_hit"}:
        return trade
    if protection_level >= 2 and trade.pnl_pct > 0:
        trade.protected_on_block = True
        trade.protection_level = max(trade.protection_level, 2)
        trade.protected_reason = "market_mode_block_longs"
        buffered_entry = trade.entry * (1 + BREAKEVEN_BUFFER_PCT / 100.0)
        if trade.tp2_hit:
            trade.trailing_tightened = True
            trade.protected_sl = max(trade.protected_sl or 0.0, buffered_entry)
        elif not trade.tp1_hit:
            trade.protected_sl = max(trade.protected_sl or 0.0, trade.entry)
        else:
            trade.protected_sl = max(trade.protected_sl or 0.0, buffered_entry)
    if protection_level >= 3 and trade.pnl_pct > 0:
        trade.protection_level = 3
        trade.trailing_tightened = trade.tp2_hit or trade.trailing_tightened
        trade.protected_sl = max(trade.protected_sl or 0.0, trade.entry * (1 + BREAKEVEN_BUFFER_PCT / 100.0))
    return trade


def update_trade_with_price(trade: TrackedTrade, current_price: float, protection_level: int = 0) -> TrackedTrade:
    if trade.status in {"closed_win", "closed_loss", "breakeven_after_tp1", "trailing_hit"}:
        return trade
    trade.current_price = current_price
    trade.highest_price = max(trade.highest_price or trade.entry, current_price)
    trade.pnl_pct = _pnl_pct(trade.entry, current_price)
    trade = apply_block_protection(trade, protection_level)
    active_sl = max(trade.sl, trade.protected_sl or 0.0)
    if current_price <= active_sl and not trade.tp1_hit:
        trade.status = "closed_loss" if active_sl <= trade.entry else "breakeven_after_tp1"
        trade.closed_portion_pct = 100.0
        trade.realized_pnl_pct = _pnl_pct(trade.entry, active_sl)
        trade.runner_pnl_pct = 0.0
        return trade
    if not trade.tp1_hit and current_price >= trade.tp1:
        trade.tp1_hit = True
        trade.sl_moved_to_entry = True
        trade.closed_portion_pct = TP1_CLOSE_PCT
        trade.realized_pnl_pct += _pnl_pct(trade.entry, trade.tp1) * (TP1_CLOSE_PCT / 100.0)
        trade.status = "tp1_partial"
    post_tp1_sl = max(active_sl, trade.entry if trade.sl_moved_to_entry else active_sl)
    if trade.tp1_hit and not trade.tp2_hit and current_price <= post_tp1_sl:
        trade.status = "breakeven_after_tp1"
        trade.closed_portion_pct = 100.0
        trade.realized_pnl_pct += max(0.0, _pnl_pct(trade.entry, post_tp1_sl)) * (1 - TP1_CLOSE_PCT / 100.0)
        trade.runner_pnl_pct = 0.0
        return trade
    if trade.tp1_hit and not trade.tp2_hit and current_price >= trade.tp2:
        trade.tp2_hit = True
        trade.trailing_active = True
        trade.runner_active = True
        trade.closed_portion_pct = TP1_CLOSE_PCT + TP2_CLOSE_PCT
        trade.realized_pnl_pct += _pnl_pct(trade.entry, trade.tp2) * (TP2_CLOSE_PCT / 100.0)
        trade.status = "tp2_partial"
    if trade.tp2_hit:
        trail_pct = max(0.9, TRAILING_STOP_AFTER_TP2_PCT - (0.6 if trade.trailing_tightened else 0.0))
        trail_anchor = max(trade.highest_price, trade.tp2)
        trailing_stop_price = max(trail_anchor * (1 - trail_pct / 100.0), trade.protected_sl or 0.0)
        trade.runner_pnl_pct = _pnl_pct(trade.entry, current_price) * (RUNNER_CLOSE_PCT / 100.0)
        if current_price <= trailing_stop_price:
            trade.status = "trailing_hit"
            trade.closed_portion_pct = 100.0
            trade.realized_pnl_pct += _pnl_pct(trade.entry, trailing_stop_price) * (RUNNER_CLOSE_PCT / 100.0)
            trade.runner_active = False
            return trade
        trade.status = "runner"
    return trade
