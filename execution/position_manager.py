from execution.config import (
    TP1_CLOSE_PCT,
    TP2_CLOSE_PCT,
    TRAILING_POSITION_PCT,
    TRAILING_PCT,
    MOVE_SL_TO_ENTRY_AFTER_TP1,
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
        "status": "management_plan_preview",
        "real_orders_sent": False,
    }
