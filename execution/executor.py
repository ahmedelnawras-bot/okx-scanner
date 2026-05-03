from execution.config import TRADING_MODE
from execution.risk_manager import can_execute_trade
from execution.order_builder import build_order_preview
from execution.execution_state import (
    set_active_trade,
    register_order,
)

import time


def process_trade_candidate(redis_client, symbol: str, candidate: dict) -> dict:
    """
    المعالج الرئيسي للتنفيذ (حالياً preview فقط)
    """

    # 1) Risk check
    decision = can_execute_trade(redis_client, symbol, candidate)

    if not decision.get("allowed"):
        return {
            "status": "rejected",
            "reason": decision.get("reason"),
        }

    # 2) Build order preview
    order = build_order_preview(symbol, candidate)

    # 3) Generate fake order id
    order_id = f"sim_{symbol}_{int(time.time())}"

    # 4) Save execution state
    set_active_trade(
        redis_client,
        symbol,
        {
            "symbol": symbol,
            "order_id": order_id,
            "mode": TRADING_MODE,
            "created_ts": int(time.time()),
        },
    )

    register_order(
        redis_client,
        order_id,
        {
            "symbol": symbol,
            "status": "preview",
        },
    )

    return {
        "status": "accepted_preview",
        "order_id": order_id,
        "order": order,
    }
