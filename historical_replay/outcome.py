from __future__ import annotations

from typing import Any

from utils.constants import MODE_RECOVERY_LONG

TRAILING_STOP_AFTER_TP2_PCT = 2.0


def _pct(from_price: float, to_price: float) -> float:
    try:
        return ((float(to_price) / float(from_price)) - 1.0) * 100.0 if float(from_price) > 0 else 0.0
    except Exception:
        return 0.0


def _exit_splits(market_mode: str = "") -> tuple[float, float, float, str]:
    """Return TP1/TP2/runner closing percentages matching live trade registry."""
    if str(market_mode or "") == MODE_RECOVERY_LONG:
        return 50.0, 25.0, 25.0, "recovery_50_25_25"
    return 40.0, 40.0, 20.0, "standard_40_40_20"


def evaluate_trade_outcome(
    future_candles: list[Any],
    entry: float,
    tp1: float,
    tp2: float,
    sl: float,
    horizon_bars: int = 96,
    market_mode: str = "",
) -> dict:
    """Evaluate long trade lifecycle using the live bot's partial-exit model.

    The previous replay outcome only checked whether TP1/TP2/SL were touched.
    This version models the agreed live management rules:
    - Normal/Strong: close 40% at TP1, 40% at TP2, leave 20% runner.
    - Recovery: close 50% at TP1, 25% at TP2, leave 25% runner.
    - Before TP1: SL closes 100% as loss.
    - After TP1: SL moves to entry, remaining size is protected at breakeven.
    - After TP2: runner uses a 2% trailing stop and is protected at least at entry.

    When a candle touches both the protective stop and a target, the conservative
    ordering checks the stop first to avoid overstating historical quality.
    """
    entry = float(entry or 0.0)
    tp1 = float(tp1 or 0.0)
    tp2 = float(tp2 or 0.0)
    sl = float(sl or 0.0)
    if entry <= 0:
        return {}

    tp1_close_pct, tp2_close_pct, runner_pct, exit_model = _exit_splits(market_mode)
    rows = list(future_candles or [])[: max(1, int(horizon_bars))]

    max_gain = 0.0
    max_drawdown = 0.0
    hit_tp1 = False
    hit_tp2 = False
    hit_sl = False
    runner_active = False
    runner_closed = False
    trailing_stop_price = 0.0
    highest_after_tp2 = 0.0

    time_to_tp1_min: int | None = None
    time_to_tp2_min: int | None = None
    time_to_sl_min: int | None = None
    time_to_runner_exit_min: int | None = None
    first_event = ""
    final_label = "flat"
    realized_weighted_pct = 0.0
    runner_exit_pct = 0.0
    runner_max_gain_pct = 0.0

    last_close = entry

    for idx, candle in enumerate(rows, start=1):
        high = float(getattr(candle, "high", 0.0) or 0.0)
        low = float(getattr(candle, "low", 0.0) or 0.0)
        close = float(getattr(candle, "close", 0.0) or 0.0)
        if close > 0:
            last_close = close
        if high > 0:
            max_gain = max(max_gain, _pct(entry, high))
        if low > 0:
            max_drawdown = min(max_drawdown, _pct(entry, low))

        # Stage 0: no TP1 yet. Conservative: SL before TP1 inside same candle.
        if not hit_tp1:
            if sl > 0 and low > 0 and low <= sl:
                hit_sl = True
                time_to_sl_min = idx * 15
                first_event = first_event or "sl"
                realized_weighted_pct = _pct(entry, sl)  # 100% closed at SL.
                final_label = "stopped_before_tp1"
                break
            if tp1 > 0 and high > 0 and high >= tp1:
                hit_tp1 = True
                time_to_tp1_min = idx * 15
                first_event = first_event or "tp1"
                realized_weighted_pct += _pct(entry, tp1) * (tp1_close_pct / 100.0)
            else:
                continue

        # Stage 1: after TP1, remaining position is protected at entry.
        if hit_tp1 and not hit_tp2:
            breakeven_stop = entry
            # ✅ FIX Replay Realism: conservative ordering —
            # لو نفس الكانيلة عندها low تحت BE وhigh فوق TP2،
            # بنفترض الـ BE stop اتضرب الأول (worst case)
            if low > 0 and low <= breakeven_stop:
                time_to_sl_min = time_to_sl_min or idx * 15
                final_label = "breakeven_after_tp1"
                break
            if tp2 > 0 and high > 0 and high >= tp2:
                hit_tp2 = True
                time_to_tp2_min = idx * 15
                realized_weighted_pct += _pct(entry, tp2) * (tp2_close_pct / 100.0)
                runner_active = runner_pct > 0
                highest_after_tp2 = max(tp2, high)
                runner_max_gain_pct = max(runner_max_gain_pct, _pct(entry, highest_after_tp2))
                trailing_stop_price = max(entry, highest_after_tp2 * (1.0 - TRAILING_STOP_AFTER_TP2_PCT / 100.0))
                final_label = "tp2_runner_open"
                continue

        # Stage 2: after TP2, runner is protected by 2% trailing stop, not below entry.
        if runner_active and not runner_closed:
            if high > 0:
                highest_after_tp2 = max(highest_after_tp2, high)
                runner_max_gain_pct = max(runner_max_gain_pct, _pct(entry, highest_after_tp2))
            trailing_stop_price = max(entry, highest_after_tp2 * (1.0 - TRAILING_STOP_AFTER_TP2_PCT / 100.0))
            if low > 0 and low <= trailing_stop_price:
                runner_closed = True
                time_to_runner_exit_min = idx * 15
                runner_exit_pct = _pct(entry, trailing_stop_price)
                realized_weighted_pct += runner_exit_pct * (runner_pct / 100.0)
                final_label = "runner_trailing_exit"
                break

    if rows and not hit_sl and not hit_tp1:
        final_label = "open_no_target"
        realized_weighted_pct = _pct(entry, last_close)
    elif hit_tp1 and not hit_tp2 and final_label not in {"breakeven_after_tp1"}:
        # Remaining 60/50% is marked at final close only for replay analytics.
        # The live bot would keep managing it; TP1 profit is already realized.
        final_label = "tp1_open_at_horizon"
        remaining_pct = max(0.0, 100.0 - tp1_close_pct)
        mark_price = max(last_close, entry) if last_close > 0 else entry
        realized_weighted_pct += _pct(entry, mark_price) * (remaining_pct / 100.0)
    elif hit_tp2 and runner_active and not runner_closed:
        final_label = "tp2_runner_open_at_horizon"
        runner_mark_pct = _pct(entry, max(last_close, entry) if last_close > 0 else entry)
        runner_exit_pct = runner_mark_pct
        realized_weighted_pct += runner_mark_pct * (runner_pct / 100.0)
    elif hit_tp2 and not runner_active:
        final_label = "win_tp2"
    elif hit_tp1 and final_label == "breakeven_after_tp1":
        pass

    return {
        "outcome_model": "partial_lifecycle_v2_40_40_20_recovery_50_25_25",
        "exit_model": exit_model,
        "horizon_bars": len(rows),
        "hit_tp1": hit_tp1,
        "hit_tp2": hit_tp2,
        "hit_sl": hit_sl,
        "first_event": first_event,
        "max_gain_24h": round(max_gain, 4),
        "max_drawdown_24h": round(max_drawdown, 4),
        "time_to_tp1_min": time_to_tp1_min,
        "time_to_tp2_min": time_to_tp2_min,
        "time_to_sl_min": time_to_sl_min,
        "time_to_runner_exit_min": time_to_runner_exit_min,
        "tp1_close_pct": tp1_close_pct,
        "tp2_close_pct": tp2_close_pct,
        "runner_pct": runner_pct,
        "runner_active": runner_active,
        "runner_closed": runner_closed,
        "runner_max_gain_pct": round(runner_max_gain_pct, 4),
        "runner_exit_pct": round(runner_exit_pct, 4),
        "weighted_trade_result_pct": round(realized_weighted_pct, 4),
        "trailing_stop_after_tp2_pct": TRAILING_STOP_AFTER_TP2_PCT,
        "final_label": final_label,
    }
