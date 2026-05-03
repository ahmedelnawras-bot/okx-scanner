from execution.config import (
    TP1_CLOSE_PCT,
    TP2_CLOSE_PCT,
    TRAILING_POSITION_PCT,
    TRAILING_PCT,
    MOVE_SL_TO_ENTRY_AFTER_TP1,
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
                "action": "close_partial",
                "close_pct": TP1_CLOSE_PCT,
                "move_sl_to": "entry" if MOVE_SL_TO_ENTRY_AFTER_TP1 else "keep",
            },
            {
                "trigger": "tp2_hit",
                "action": "close_partial",
                "close_pct": TP2_CLOSE_PCT,
            },
            {
                "trigger": "after_tp2",
                "action": "activate_trailing",
                "position_pct": TRAILING_POSITION_PCT,
                "trailing_pct": TRAILING_PCT,
            },
        ],
        "block_protection": {
            "enabled": True,
            "trigger_mode": MODE_BLOCK_LONGS,
            "min_profit_pct": PROTECT_ON_BLOCK_MIN_PROFIT_PCT,
            "buffer_pct": PROTECT_ON_BLOCK_BUFFER_PCT,
            "action": "move_sl_to_breakeven_or_better",
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


def build_block_protection_preview(trade: dict, current_price: float, market_mode: str) -> dict:
    """
    Preview فقط لحماية الصفقة عند BLOCK_LONGS.
    لا يرسل أمر حقيقي لـ OKX.
    """

    symbol = trade.get("symbol")
    entry = float(
        trade.get("effective_entry")
        or trade.get("entry")
        or 0.0
    )

    current_sl = float(trade.get("sl", 0.0) or 0.0)
    current_price = float(current_price or 0.0)

    result = {
        "symbol": symbol,
        "market_mode": market_mode,
        "eligible": False,
        "action": "none",
        "reason": "",
        "entry": entry,
        "current_price": current_price,
        "current_sl": current_sl,
        "protected_sl": current_sl,
        "profit_pct": 0.0,
        "real_order_sent": False,
    }

    if str(market_mode or "").strip() != MODE_BLOCK_LONGS:
        result["reason"] = "market_mode_not_block_longs"
        return result

    if entry <= 0 or current_price <= 0:
        result["reason"] = "invalid_entry_or_price"
        return result

    profit_pct = calculate_long_profit_pct(entry, current_price)
    result["profit_pct"] = profit_pct

    if profit_pct < PROTECT_ON_BLOCK_MIN_PROFIT_PCT:
        result["reason"] = "profit_below_protection_threshold"
        return result

    protected_sl = calculate_protected_sl_for_long(entry)

    if protected_sl <= current_sl:
        result["reason"] = "current_sl_already_better_or_equal"
        result["protected_sl"] = current_sl
        return result

    result.update({
        "eligible": True,
        "action": "move_sl_to_protected_breakeven",
        "reason": "market_mode_block_longs",
        "protected_sl": protected_sl,
        "protected_on_block": True,
        "protected_reason": "market_mode_block_longs",
    })

    return result
