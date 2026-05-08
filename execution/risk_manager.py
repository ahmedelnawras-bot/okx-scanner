import os
import json
import ast
from execution.config import (
    TRADING_MODE,
    EXECUTION_ENABLED,
    MAX_OPEN_POSITIONS,
    MIN_EXECUTION_SCORE,
)

from execution.execution_state import is_symbol_blocking_execution, count_active_execution_trades


def _safe_float(value, default=0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _configured_max_positions() -> int:
    # Execution active limit follows config default (100). Env can lower/raise if explicitly set later.
    try:
        return int(os.getenv("EXECUTION_MAX_ACTIVE_TRADES", str(MAX_OPEN_POSITIONS)))
    except Exception:
        return int(MAX_OPEN_POSITIONS)


def _candidate_score(candidate: dict) -> float:
    return _safe_float(candidate.get("score", candidate.get("effective_score", 0.0)), 0.0)


def can_execute_trade(redis_client, symbol: str, candidate: dict, market_mode: str = "") -> dict:
    """
    Final execution risk gate.

    Agreed rules:
    - main.py sends only Execution Candidates.
    - BLOCK_LONGS does not block execution candidates.
    - No setup whitelist here.
    - Max active execution trades = configured MAX_OPEN_POSITIONS / EXECUTION_MAX_ACTIVE_TRADES.
    - A trade above TP1 with SL moved to entry does not count toward the active limit.
    - Same symbol is blocked only while an active execution trade exists and has not reached TP2.
    - Daily drawdown lock is handled in main.py by setting the same stop-trading pause key.
    """

    if not EXECUTION_ENABLED:
        return {"allowed": False, "reason": "execution_disabled"}

    if TRADING_MODE not in ("demo", "paper", "live_small"):
        return {"allowed": False, "reason": "mode_not_allowed"}

    # No score/risk/market-mode block here: main.py already decided the signal is an Execution Candidate.

    if is_symbol_blocking_execution(redis_client, symbol):
        return {"allowed": False, "reason": "same_symbol_open"}

    max_positions = _configured_max_positions()
    active_count = count_active_execution_trades(redis_client)
    if active_count >= max_positions:
        return {
            "allowed": False,
            "reason": "max_positions_reached",
            "active_count": active_count,
            "max_positions": max_positions,
        }

    return {
        "allowed": True,
        "reason": "ok",
        "active_count": active_count,
        "max_positions": max_positions,
    }
