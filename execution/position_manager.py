from execution.config import (
    OKX_SIMULATED,
    TRADING_MODE,
    TP1_CLOSE_PCT,
    TP2_CLOSE_PCT,
    TRAILING_POSITION_PCT,
    TRAILING_PCT,
    MOVE_SL_TO_ENTRY_AFTER_TP1,
    TP2_PROTECTED_SL_BUFFER_PCT,
    PROTECT_ON_BLOCK_MIN_PROFIT_PCT,
    PROTECT_ON_BLOCK_BUFFER_PCT,
    MODE_BLOCK_LONGS,
)


def build_position_management_plan(order: dict) -> dict:
    """
    خطة إدارة الصفقة بعد الدخول.
    لا ترسل أوامر حقيقية حتى الآن.
    """

    entry = float(order.get("entry", 0.0) or 0.0)
    sl = float(order.get("sl", 0.0) or 0.0)
    tp1 = float(order.get("tp1", 0.0) or 0.0)
    tp2 = float(order.get("tp2", 0.0) or 0.0)

    return {
        "symbol": order.get("symbol"),
        "entry": entry,
        "initial_sl": sl,
        "current_sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "steps": [
            {
                "trigger": "tp1_hit",
                "action": "close_partial_only",
                "close_pct": TP1_CLOSE_PCT,
                "move_sl_to": "keep",
                "note": "TP1 no longer moves SL; this avoids early breakeven exits.",
            },
            {
                "trigger": "tp2_hit",
                "action": "close_partial_and_protect_runner",
                "close_pct": TP2_CLOSE_PCT,
                "move_sl_to": "tp1_or_tp1_plus_buffer",
                "buffer_pct": TP2_PROTECTED_SL_BUFFER_PCT,
            },
            {
                "trigger": "after_tp2",
                "action": "activate_trailing",
                "position_pct": TRAILING_POSITION_PCT,
                "trailing_pct": TRAILING_PCT,
                "note": "Last 20% closes if price retraces trailing_pct from post-TP2 high/low.",
            },
        ],
        "block_protection": {
            "enabled": True,
            "trigger_mode": MODE_BLOCK_LONGS,
            "reminder_1": "monitor_only",
            "reminder_2": "soft_protection_for_winners_and_tp2_runners",
            "reminder_3": "defensive_protection",
            "min_profit_pct": PROTECT_ON_BLOCK_MIN_PROFIT_PCT,
            "buffer_pct": PROTECT_ON_BLOCK_BUFFER_PCT,
            "action": "tighten_trailing_or_lock_tp1_for_tp2_runner",
            "losers": "no_panic_close; emergency_sl_compression_only_when_severe",
        },
        "status": "management_plan_preview",
        "real_orders_sent": False,
    }


def calculate_long_profit_pct(entry: float, current_price: float) -> float:
    entry = float(entry or 0.0)
    current_price = float(current_price or 0.0)

    if entry <= 0 or current_price <= 0:
        return 0.0

    return ((current_price - entry) / entry) * 100.0


def calculate_protected_sl_for_long(entry: float) -> float:
    """
    يحسب SL محمي للونج:
    entry + buffer
    """

    entry = float(entry or 0.0)

    if entry <= 0:
        return 0.0

    protected_sl = entry * (1 + (PROTECT_ON_BLOCK_BUFFER_PCT / 100.0))
    return protected_sl



def calculate_tp2_protected_sl_for_long(tp1: float) -> float:
    """After TP2, the remaining 20% runner is protected at TP1 or TP1 + buffer."""
    tp1 = float(tp1 or 0.0)
    if tp1 <= 0:
        return 0.0
    return tp1 * (1 + (TP2_PROTECTED_SL_BUFFER_PCT / 100.0))

def build_block_protection_preview(trade: dict, current_price: float, market_mode: str, reminder_count: int = 2) -> dict:
    """
    Preview for BLOCK_LONGS protection.
    Reminder #1 = monitor only.
    Reminder #2 = soft protection for profitable/TP2 runner trades.
    Reminder #3+ = defensive protection.
    This function does not send OKX orders by itself; live callers should pass the result to the OKX client.
    """

    symbol = trade.get("symbol")
    entry = float(trade.get("effective_entry") or trade.get("entry") or 0.0)
    tp1 = float(trade.get("tp1", 0.0) or 0.0)
    current_sl = float(trade.get("sl", 0.0) or 0.0)
    current_price = float(current_price or 0.0)
    tp2_hit = bool(trade.get("tp2_hit") or trade.get("trailing_active"))

    result = {
        "symbol": symbol,
        "market_mode": market_mode,
        "reminder_count": int(reminder_count or 0),
        "eligible": False,
        "action": "none",
        "reason": "",
        "entry": entry,
        "tp1": tp1,
        "current_price": current_price,
        "current_sl": current_sl,
        "protected_sl": current_sl,
        "profit_pct": 0.0,
        "real_order_sent": False,
    }

    if str(market_mode or "").strip() != MODE_BLOCK_LONGS:
        result["reason"] = "market_mode_not_block_longs"
        return result
    if int(reminder_count or 0) <= 1:
        result["reason"] = "reminder_1_monitor_only"
        result["action"] = "monitor_only"
        return result
    if entry <= 0 or current_price <= 0:
        result["reason"] = "invalid_entry_or_price"
        return result

    profit_pct = calculate_long_profit_pct(entry, current_price)
    result["profit_pct"] = profit_pct

    # Main protection target is TP2 protected runner, not TP1-only.
    if tp2_hit and tp1 > 0:
        protected_sl = calculate_tp2_protected_sl_for_long(tp1)
        if protected_sl > current_sl:
            result.update({
                "eligible": True,
                "action": "lock_runner_at_tp1_or_better",
                "reason": "block_longs_tp2_runner_protection",
                "protected_sl": protected_sl,
                "protected_on_block": True,
                "protected_reason": "market_mode_block_longs_tp2_runner",
            })
            return result
        result["reason"] = "tp2_runner_already_protected"
        return result

    if profit_pct >= PROTECT_ON_BLOCK_MIN_PROFIT_PCT and int(reminder_count or 0) >= 3:
        protected_sl = calculate_protected_sl_for_long(entry)
        if protected_sl > current_sl:
            result.update({
                "eligible": True,
                "action": "defensive_profit_lock",
                "reason": "block_longs_defensive_profit_protection",
                "protected_sl": protected_sl,
                "protected_on_block": True,
                "protected_reason": "market_mode_block_longs_defensive",
            })
            return result

    if profit_pct < 0:
        result["reason"] = "loser_monitor_only_no_panic_close"
    else:
        result["reason"] = "not_eligible_before_tp2"
    return result



def _truthy(value) -> bool:
    return str(value or "").strip().lower() in ("1", "true", "yes", "y", "on")


def _symbol_to_okx_inst_id(symbol: str) -> str:
    symbol = str(symbol or "").strip().upper()
    if not symbol:
        return ""
    if symbol.endswith("-SWAP"):
        return symbol
    if symbol.endswith("USDT") and "-" not in symbol:
        base = symbol[:-4]
        return f"{base}-USDT-SWAP"
    if "-" in symbol:
        return symbol
    return symbol


def apply_block_protection_to_okx(trade: dict, protection: dict, okx_client=None) -> dict:
    """Apply a BLOCK_LONGS protection preview to OKX when live mode is enabled.

    The tracking layer decides *whether* protection is eligible and what SL/floor
    should be used. This function only bridges that decision to the platform.
    In simulation/paper/scanner modes it returns a tracking-only result.
    """
    protection = protection or {}
    trade = trade or {}

    if not protection.get("eligible") and not protection.get("protected_on_block"):
        return {
            "ok": False,
            "mode": "none",
            "status": "not_eligible",
            "reason": protection.get("reason", "not_eligible"),
        }

    simulated = _truthy(OKX_SIMULATED) or str(TRADING_MODE or "").lower() not in ("live", "live_small")
    if simulated:
        return {
            "ok": True,
            "mode": "simulation",
            "status": "tracking_only",
            "reason": "OKX_SIMULATED or non-live trading mode",
        }

    try:
        if okx_client is None:
            from execution.okx_trade_client import OKXTradeClient
            okx_client = OKXTradeClient()

        inst_id = _symbol_to_okx_inst_id(
            trade.get("instId") or trade.get("inst_id") or trade.get("symbol") or protection.get("symbol")
        )
        stop_price = float(protection.get("protected_sl") or trade.get("protected_sl") or trade.get("sl") or 0.0)
        side = str(trade.get("side") or "long").lower()
        size = str(trade.get("runner_size") or trade.get("remaining_size") or trade.get("sz") or "")
        td_mode = str(trade.get("tdMode") or trade.get("td_mode") or "cross")
        if side == "short":
            result = okx_client.protect_short_position_sl(inst_id, stop_price, size=size, td_mode=td_mode)
        else:
            result = okx_client.protect_long_position_sl(inst_id, stop_price, size=size, td_mode=td_mode)
        return {
            "ok": bool(result.get("ok")),
            "mode": "live",
            "status": "okx_sl_update_sent" if result.get("ok") else "okx_sl_update_failed",
            "reason": result.get("msg") or result.get("code") or "",
            "raw": result,
        }
    except Exception as e:
        return {
            "ok": False,
            "mode": "live",
            "status": "okx_sl_update_error",
            "reason": str(e),
        }
