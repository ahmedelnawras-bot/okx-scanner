import time

# مفاتيح Redis (نفس النمط عندك في main)
EXECUTION_STATE_PREFIX = "exec:state"
EXECUTION_ORDER_PREFIX = "exec:order"


def get_exec_state_key(symbol: str) -> str:
    return f"{EXECUTION_STATE_PREFIX}:{symbol}"


def get_exec_order_key(order_id: str) -> str:
    return f"{EXECUTION_ORDER_PREFIX}:{order_id}"


def set_active_trade(redis_client, symbol: str, data: dict, ttl: int = 86400) -> bool:
    """
    تسجيل صفقة نشطة
    """
    if not redis_client:
        return False

    key = get_exec_state_key(symbol)

    try:
        data["updated_ts"] = int(time.time())
        redis_client.set(key, str(data), ex=ttl)
        return True
    except Exception:
        return False


def get_active_trade(redis_client, symbol: str) -> dict:
    """
    قراءة صفقة نشطة
    """
    if not redis_client:
        return {}

    key = get_exec_state_key(symbol)

    try:
        val = redis_client.get(key)
        if not val:
            return {}
        return eval(val)
    except Exception:
        return {}


def clear_active_trade(redis_client, symbol: str) -> bool:
    """
    حذف الصفقة
    """
    if not redis_client:
        return False

    key = get_exec_state_key(symbol)

    try:
        redis_client.delete(key)
        return True
    except Exception:
        return False


def is_symbol_in_execution(redis_client, symbol: str) -> bool:
    """
    هل فيه صفقة شغالة على العملة؟
    """
    if not redis_client:
        return False

    key = get_exec_state_key(symbol)

    try:
        return bool(redis_client.exists(key))
    except Exception:
        return False


def register_order(redis_client, order_id: str, data: dict, ttl: int = 86400) -> bool:
    """
    تسجيل order
    """
    if not redis_client:
        return False

    key = get_exec_order_key(order_id)

    try:
        data["created_ts"] = int(time.time())
        redis_client.set(key, str(data), ex=ttl)
        return True
    except Exception:
        return False


def get_order(redis_client, order_id: str) -> dict:
    """
    قراءة order
    """
    if not redis_client:
        return {}

    key = get_exec_order_key(order_id)

    try:
        val = redis_client.get(key)
        if not val:
            return {}
        return eval(val)
    except Exception:
        return {}
