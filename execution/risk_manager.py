from execution.config import (
    TRADING_MODE,
    EXECUTION_ENABLED,
    MAX_OPEN_POSITIONS,
    MIN_EXECUTION_SCORE,
)

from execution.okx_trade_client import OKXTradeClient
from execution.execution_state import is_symbol_in_execution


def can_execute_trade(redis_client, symbol: str, candidate: dict) -> dict:
    """
    يقرر هل مسموح تنفيذ الصفقة أم لا
    """

    # 1) Execution disabled
    if not EXECUTION_ENABLED:
        return {
            "allowed": False,
            "reason": "execution_disabled",
        }

    # 2) Trading mode
    if TRADING_MODE not in ("demo", "paper", "live_small"):
        return {
            "allowed": False,
            "reason": "mode_not_allowed",
        }

    # 3) Score filter
    score = float(candidate.get("score", 0.0))
    if score < MIN_EXECUTION_SCORE:
        return {
            "allowed": False,
            "reason": "low_score",
        }

    # 4) Already executing same symbol
    if is_symbol_in_execution(redis_client, symbol):
        return {
            "allowed": False,
            "reason": "already_in_execution",
        }

    # 5) Open positions limit
    client = OKXTradeClient()
    open_positions = client.get_open_positions_count()

    if open_positions >= MAX_OPEN_POSITIONS:
        return {
            "allowed": False,
            "reason": "max_positions_reached",
        }

    # ✅ Allowed
    return {
        "allowed": True,
        "reason": "ok",
    }
