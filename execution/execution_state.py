import time
import json
import ast

# مفاتيح Redis (نفس النمط عندك في main)
EXECUTION_STATE_PREFIX = "exec:state"
EXECUTION_ORDER_PREFIX = "exec:order"


def get_exec_state_key(symbol: str) -> str:
    return f"{EXECUTION_STATE_PREFIX}:{symbol}"


def get_exec_order_key(order_id: str) -> str:
    return f"{EXECUTION_ORDER_PREFIX}:{order_id}"


def _encode(data: dict) -> str:
    return json.dumps(data or {}, ensure_ascii=False)


def _decode(raw) -> dict:
    if not raw:
        return {}
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="ignore")
    if isinstance(raw, dict):
        return raw
    try:
        return json.loads(raw)
    except Exception:
        try:
            value = ast.literal_eval(str(raw))
            return value if isinstance(value, dict) else {}
        except Exception:
            return {}


def set_active_trade(redis_client, symbol: str, data: dict, ttl: int = 86400) -> bool:
    """تسجيل صفقة تنفيذ نشطة."""
    if not redis_client:
        return False
    key = get_exec_state_key(symbol)
    try:
        payload = dict(data or {})
        payload["updated_ts"] = int(time.time())
        redis_client.set(key, _encode(payload), ex=ttl)
        return True
    except Exception:
        return False


def get_active_trade(redis_client, symbol: str) -> dict:
    """قراءة صفقة تنفيذ نشطة."""
    if not redis_client:
        return {}
    try:
        return _decode(redis_client.get(get_exec_state_key(symbol)))
    except Exception:
        return {}


def clear_active_trade(redis_client, symbol: str) -> bool:
    """حذف الصفقة من حالة التنفيذ."""
    if not redis_client:
        return False
    try:
        redis_client.delete(get_exec_state_key(symbol))
        return True
    except Exception:
        return False


def _boolish(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in ("1", "true", "yes", "y", "on")


def _trade_execution_status(trade: dict) -> str:
    diag = trade.get("diagnostics", {}) or {}
    return str(trade.get("execution_status") or diag.get("execution_status") or trade.get("execution_result_status") or diag.get("execution_result_status") or "").lower()


def _is_protected_after_tp1(trade: dict) -> bool:
    return _boolish(trade.get("tp1_hit")) and _boolish(trade.get("sl_moved_to_entry"))


def _has_reached_tp2(trade: dict) -> bool:
    status = str(trade.get("status", "") or "").lower()
    result = str(trade.get("result", "") or "").lower()
    return (
        _boolish(trade.get("tp2_hit"))
        or status in ("tp2_partial", "trailing", "trailing_open")
        or result in ("tp2_win", "trailing_win")
    )


def _is_runner_only_after_tp2(trade: dict) -> bool:
    """Return True when only the protected 20% runner remains after TP2.

    Business rule:
    - Do not change the 2.5% trailing logic.
    - Do not close/delete the old trade from tracking.
    - Allow a new execution candidate on the same symbol once the old trade
      reached TP2 and only the trailing runner is left.
    """
    return _has_reached_tp2(trade)


def _is_execution_trade_active(trade: dict) -> bool:
    status = str(trade.get("status", "") or "").lower()
    result = str(trade.get("result", "") or "").lower()
    exec_status = _trade_execution_status(trade)
    if exec_status in ("candidate_only", "preview_rejected", "rejected_invalid_order", "rejected_limit", "daily_drawdown_lock", "not_candidate"):
        return False
    if result in ("loss", "tp1_win", "tp2_win", "trailing_win", "breakeven", "expired", "pending_expired"):
        return False
    return status in ("open", "partial", "pending_pullback", "tp2_partial", "trailing", "trailing_open") or exec_status in ("accepted_preview", "pending_pullback_preview")


def _load_execution_trades_from_performance(redis_client) -> list:
    if not redis_client:
        return []
    out = []
    seen = set()
    for pattern in ("trade:futures:long:*", "trade_history:futures:long:*"):
        try:
            for key in redis_client.scan_iter(pattern):
                key_s = key.decode() if isinstance(key, bytes) else str(key)
                if key_s in seen:
                    continue
                seen.add(key_s)
                trade = _decode(redis_client.get(key))
                if not trade:
                    continue
                exec_status = _trade_execution_status(trade)
                if exec_status or _boolish((trade.get("diagnostics", {}) or {}).get("block_exception")):
                    out.append(trade)
        except Exception:
            continue
    return out


def count_active_execution_trades(redis_client) -> int:
    """
    Count active execution trades for the 7-trade limit.
    Trades that hit TP1 and moved SL to entry are protected and do not count.
    """
    count = 0
    for trade in _load_execution_trades_from_performance(redis_client):
        if not _is_execution_trade_active(trade):
            continue
        if _is_protected_after_tp1(trade):
            continue
        count += 1
    return count


def is_symbol_blocking_execution(redis_client, symbol: str) -> bool:
    """Block same symbol only before TP2.

    Once an existing trade reaches TP2, the remaining 20% runner keeps trailing
    normally but no longer blocks a fresh execution candidate for the same symbol.
    """
    symbol = str(symbol or "").strip()
    if not symbol:
        return False
    for trade in _load_execution_trades_from_performance(redis_client):
        if str(trade.get("symbol") or "").strip() != symbol:
            continue
        if not _is_execution_trade_active(trade):
            continue
        if _is_runner_only_after_tp2(trade):
            continue
        return True

    # Fallback to lightweight execution state if performance trade was not registered yet.
    state = get_active_trade(redis_client, symbol)
    if not state:
        return False
    if _is_runner_only_after_tp2(state):
        return False
    status = str(state.get("status", "") or "").lower()
    return status in ("accepted_preview", "pending_pullback_preview", "open", "partial", "pending_pullback")


def is_symbol_in_execution(redis_client, symbol: str) -> bool:
    """Backward-compatible alias."""
    return is_symbol_blocking_execution(redis_client, symbol)


def register_order(redis_client, order_id: str, data: dict, ttl: int = 86400) -> bool:
    """تسجيل order."""
    if not redis_client:
        return False
    try:
        payload = dict(data or {})
        payload["created_ts"] = int(time.time())
        redis_client.set(get_exec_order_key(order_id), _encode(payload), ex=ttl)
        return True
    except Exception:
        return False


def get_order(redis_client, order_id: str) -> dict:
    """قراءة order."""
    if not redis_client:
        return {}
    try:
        return _decode(redis_client.get(get_exec_order_key(order_id)))
    except Exception:
        return {}
