import time
import logging

from execution.config import (
    TRADING_MODE,
    LIVE_EXECUTION_SETUP_WHITELIST_ENABLED,
    LIVE_EXECUTION_SETUP_WHITELIST,
    LIVE_EXECUTION_SETUP_KEYWORDS,
)
from execution.risk_manager import can_execute_trade
from execution.order_builder import build_order_preview
from execution.execution_state import (
    set_active_trade,
    register_order,
)

logger = logging.getLogger("okx-scanner")


def is_setup_allowed_for_execution(candidate: dict) -> dict:
    setup_type = str(candidate.get("setup_type", "") or "").strip()

    if not LIVE_EXECUTION_SETUP_WHITELIST_ENABLED:
        return {
            "allowed": True,
            "reason": "whitelist_disabled",
            "setup_type": setup_type,
        }

    exact_allowed = setup_type in LIVE_EXECUTION_SETUP_WHITELIST
    keyword_allowed = any(
        keyword in setup_type
        for keyword in LIVE_EXECUTION_SETUP_KEYWORDS
    )

    if exact_allowed or keyword_allowed:
        return {
            "allowed": True,
            "reason": "setup_allowed",
            "setup_type": setup_type,
        }

    return {
        "allowed": False,
        "reason": "setup_not_whitelisted",
        "setup_type": setup_type,
    }


def process_trade_candidate(redis_client, symbol: str, candidate: dict) -> dict:
    """
    المعالج الرئيسي للتنفيذ.
    حالياً preview فقط، ولا يرسل أي أوامر حقيقية إلى OKX.
    """

    # 1) Setup whitelist gate
    setup_decision = is_setup_allowed_for_execution(candidate)

    if not setup_decision.get("allowed"):
        logger.info(
            f"⏭ Execution blocked | Setup not allowed | "
            f"{symbol} | {setup_decision.get('setup_type')}"
        )
        return {
            "status": "skipped",
            "reason": setup_decision.get("reason"),
            "setup_type": setup_decision.get("setup_type"),
        }

    # 2) Risk check
    decision = can_execute_trade(redis_client, symbol, candidate)

    if not decision.get("allowed"):
        return {
            "status": "rejected",
            "reason": decision.get("reason"),
            "setup_type": setup_decision.get("setup_type"),
        }

    # 3) Build order preview
    order = build_order_preview(symbol, candidate)

    # 4) Generate fake order id
    order_id = f"sim_{symbol}_{int(time.time())}"

    # 5) Save execution state
    set_active_trade(
        redis_client,
        symbol,
        {
            "symbol": symbol,
            "order_id": order_id,
            "mode": TRADING_MODE,
            "setup_type": setup_decision.get("setup_type"),
            "created_ts": int(time.time()),
        },
    )

    register_order(
        redis_client,
        order_id,
        {
            "symbol": symbol,
            "status": "preview",
            "setup_type": setup_decision.get("setup_type"),
        },
    )

    return {
        "status": "accepted_preview",
        "order_id": order_id,
        "order": order,
        "setup_type": setup_decision.get("setup_type"),
    }
