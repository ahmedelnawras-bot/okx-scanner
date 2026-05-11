import os
import json
import ast
import time
from execution.config import (
    TRADING_MODE,
    EXECUTION_ENABLED,
    MAX_OPEN_POSITIONS,
    MIN_EXECUTION_SCORE,
    DYNAMIC_POSITION_SIZING_ENABLED,
    START_OF_DAY_BALANCE_FALLBACK_USD,
    MAX_CAPITAL_IN_USE_PCT,
    DYNAMIC_POSITION_BRACKETS,
)

from execution.execution_state import is_symbol_blocking_execution, count_active_execution_trades


def _safe_float(value, default=0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _execution_day_key() -> str:
    return time.strftime("%Y%m%d", time.localtime())


def _get_start_of_day_balance(redis_client=None) -> float:
    """Best-effort start-of-day balance.
    Prefers Redis day-start value, then env/config fallback. This keeps sizing stable during the day.
    """
    fallback = _safe_float(os.getenv("START_OF_DAY_BALANCE_FALLBACK_USD", START_OF_DAY_BALANCE_FALLBACK_USD), 1000.0)
    if redis_client is None:
        return fallback
    try:
        day_key = _execution_day_key()
        key_day = "execution:dynamic_sizing:day"
        key_balance = "execution:dynamic_sizing:start_balance"
        stored_day = str(redis_client.get(key_day) or "")
        stored_balance = redis_client.get(key_balance)
        if stored_day == day_key and stored_balance not in (None, ""):
            bal = _safe_float(stored_balance, fallback)
            if bal > 0:
                return bal
        redis_client.set(key_day, day_key)
        redis_client.set(key_balance, str(fallback))
        return fallback
    except Exception:
        return fallback


def _dynamic_max_positions_for_balance(balance: float) -> int:
    try:
        balance = float(balance or 0.0)
        for low, high, slots in DYNAMIC_POSITION_BRACKETS:
            if balance >= float(low) and balance <= float(high):
                return int(slots)
        if balance < 500:
            return 3
        return int(MAX_OPEN_POSITIONS)
    except Exception:
        return int(MAX_OPEN_POSITIONS)


def _build_dynamic_position_plan(redis_client=None) -> dict:
    start_balance = _get_start_of_day_balance(redis_client)
    max_capital = start_balance * (float(MAX_CAPITAL_IN_USE_PCT) / 100.0)
    dynamic_slots = _dynamic_max_positions_for_balance(start_balance)
    env_override = os.getenv("EXECUTION_MAX_ACTIVE_TRADES", "").strip()
    if env_override:
        try:
            dynamic_slots = int(env_override)
        except Exception:
            pass
    margin_per_trade = max_capital / dynamic_slots if dynamic_slots > 0 else 0.0
    return {
        "start_balance": round(start_balance, 2),
        "max_capital_in_use": round(max_capital, 2),
        "max_capital_pct": float(MAX_CAPITAL_IN_USE_PCT),
        "max_positions": int(dynamic_slots),
        "margin_per_trade": round(margin_per_trade, 2),
        "dynamic": bool(DYNAMIC_POSITION_SIZING_ENABLED),
    }


def _configured_max_positions(redis_client=None) -> int:
    if DYNAMIC_POSITION_SIZING_ENABLED:
        return int(_build_dynamic_position_plan(redis_client).get("max_positions", MAX_OPEN_POSITIONS))
    try:
        return int(os.getenv("EXECUTION_MAX_ACTIVE_TRADES", str(MAX_OPEN_POSITIONS)))
    except Exception:
        return int(MAX_OPEN_POSITIONS)


def _is_tp2_protected_runner(trade: dict) -> bool:
    if not isinstance(trade, dict):
        return False
    status = str(trade.get("status") or trade.get("execution_status") or "").lower()
    return bool(
        trade.get("tp2_hit")
        or trade.get("trailing_active")
        or status in ("tp2_partial", "trailing_open", "trailing")
    )


def _is_execution_managed_trade(trade: dict) -> bool:
    if not isinstance(trade, dict):
        return False
    status = str(trade.get("execution_status") or trade.get("execution_result_status") or trade.get("status") or "").lower()
    if status in ("accepted_preview", "pending_pullback_preview", "live_execute", "live_executed", "order_placed", "executed"):
        return True
    return bool(trade.get("execution_candidate_badged") or trade.get("execution_order_id") or trade.get("order_id"))


def _load_open_execution_trades_from_tracking(redis_client) -> list:
    """Read tracking open trades so TP2 runners can be excluded from slot count.
    This is deliberately best-effort and falls back to execution_state counters if unavailable.
    """
    trades = []
    if redis_client is None:
        return trades
    try:
        keys = redis_client.smembers("open_trades:futures:long") or []
        for key in keys:
            try:
                if isinstance(key, bytes):
                    key = key.decode()
                raw = redis_client.get(key)
                if not raw:
                    continue
                if isinstance(raw, bytes):
                    raw = raw.decode()
                data = json.loads(raw)
                if _is_execution_managed_trade(data):
                    trades.append(data)
            except Exception:
                continue
    except Exception:
        return []
    return trades


def count_counted_execution_trades(redis_client) -> int:
    trades = _load_open_execution_trades_from_tracking(redis_client)
    if trades:
        return sum(1 for t in trades if not _is_tp2_protected_runner(t))
    try:
        return int(count_active_execution_trades(redis_client))
    except Exception:
        return 0


def is_symbol_blocking_execution_dynamic(redis_client, symbol: str) -> bool:
    symbol = str(symbol or "").upper().strip()
    trades = _load_open_execution_trades_from_tracking(redis_client)
    if trades:
        for t in trades:
            if str(t.get("symbol") or "").upper().strip() == symbol and not _is_tp2_protected_runner(t):
                return True
        return False
    try:
        return bool(is_symbol_blocking_execution(redis_client, symbol))
    except Exception:
        return False


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

    position_plan = _build_dynamic_position_plan(redis_client)
    max_positions = int(position_plan.get("max_positions", _configured_max_positions(redis_client)))
    active_count = count_counted_execution_trades(redis_client)
    remaining_slots = max(0, max_positions - active_count)

    if is_symbol_blocking_execution_dynamic(redis_client, symbol):
        return {
            "allowed": False,
            "reason": "same_symbol_open",
            "active_count": active_count,
            "max_positions": max_positions,
            "remaining_slots": remaining_slots,
            "position_plan": position_plan,
        }

    if active_count >= max_positions:
        return {
            "allowed": False,
            "reason": "max_positions_reached",
            "active_count": active_count,
            "max_positions": max_positions,
            "remaining_slots": 0,
            "position_plan": position_plan,
        }

    return {
        "allowed": True,
        "reason": "ok",
        "active_count": active_count,
        "max_positions": max_positions,
        "remaining_slots": remaining_slots,
        "position_plan": position_plan,
    }
