import json
import time
import logging
import requests

logger = logging.getLogger("okx-scanner")

OKX_CANDLES_URL = "https://www.okx.com/api/v5/market/candles"


def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def get_trade_key(signal_type: str, symbol: str, candle_time: int) -> str:
    return f"trade:{signal_type}:{symbol}:{candle_time}"


def get_open_trades_set_key(signal_type: str = "long") -> str:
    return f"open_trades:{signal_type}"


def get_stats_key(signal_type: str = "long") -> str:
    return f"stats:{signal_type}"


def calc_tp1(entry: float, sl: float) -> float:
    """
    TP1 = 1R
    """
    risk = entry - sl
    return round(entry + risk, 6)


def calc_tp2(entry: float, sl: float) -> float:
    """
    TP2 = 2R
    """
    risk = entry - sl
    return round(entry + (risk * 2), 6)


def fetch_recent_candles(symbol: str, timeframe: str = "15m", limit: int = 100):
    try:
        res = requests.get(
            OKX_CANDLES_URL,
            params={
                "instId": symbol,
                "bar": timeframe,
                "limit": limit,
            },
            timeout=15,
        ).json()

        return res.get("data", [])
    except Exception as e:
        logger.error(f"performance.fetch_recent_candles error on {symbol}: {e}")
        return []


def normalize_candles(raw):
    candles = []
    for row in raw:
        try:
            candles.append({
                "ts": int(float(row[0])),
                "open": safe_float(row[1]),
                "high": safe_float(row[2]),
                "low": safe_float(row[3]),
                "close": safe_float(row[4]),
                "volume": safe_float(row[5]),
                "confirm": str(row[8]),
            })
        except Exception:
            continue

    candles.sort(key=lambda x: x["ts"])
    return candles


def register_trade(
    redis_client,
    symbol: str,
    signal_type: str,
    candle_time: int,
    entry: float,
    sl: float,
    score: float,
    timeframe: str = "15m",
    btc_mode: str = "🟡 محايد",
    funding_label: str = "🟡 محايد",
):
    """
    يسجل الصفقة لو كانت جديدة فقط
    """
    if redis_client is None:
        return False

    trade_key = get_trade_key(signal_type, symbol, candle_time)
    open_set_key = get_open_trades_set_key(signal_type)

    trade_data = {
        "symbol": symbol,
        "signal_type": signal_type,
        "timeframe": timeframe,
        "candle_time": int(candle_time),
        "entry": round(float(entry), 6),
        "sl": round(float(sl), 6),
        "tp1": calc_tp1(float(entry), float(sl)),
        "tp2": calc_tp2(float(entry), float(sl)),
        "score": round(float(score), 2),
        "btc_mode": btc_mode,
        "funding_label": funding_label,
        "status": "open",
        "created_at": int(time.time()),
        "updated_at": int(time.time()),
        "closed_at": None,
        "result": None,
    }

    try:
        created = redis_client.set(
            trade_key,
            json.dumps(trade_data, ensure_ascii=False),
            nx=True,
            ex=60 * 60 * 24 * 7,  # 7 days
        )

        if not created:
            return False

        redis_client.sadd(open_set_key, trade_key)
        return True

    except Exception as e:
        logger.error(f"register_trade error on {symbol}: {e}")
        return False


def load_trade(redis_client, trade_key: str):
    if redis_client is None:
        return None

    try:
        raw = redis_client.get(trade_key)
        if not raw:
            return None
        return json.loads(raw)
    except Exception as e:
        logger.error(f"load_trade error on {trade_key}: {e}")
        return None


def save_trade(redis_client, trade_key: str, trade_data: dict):
    if redis_client is None:
        return False

    try:
        trade_data["updated_at"] = int(time.time())
        redis_client.set(
            trade_key,
            json.dumps(trade_data, ensure_ascii=False),
            ex=60 * 60 * 24 * 7,
        )
        return True
    except Exception as e:
        logger.error(f"save_trade error on {trade_key}: {e}")
        return False


def mark_trade_closed(redis_client, trade_key: str, trade_data: dict, result: str):
    """
    result = win / loss / expired
    """
    if redis_client is None:
        return False

    signal_type = trade_data.get("signal_type", "long")
    open_set_key = get_open_trades_set_key(signal_type)

    try:
        trade_data["status"] = "closed"
        trade_data["result"] = result
        trade_data["closed_at"] = int(time.time())
        trade_data["updated_at"] = int(time.time())

        redis_client.set(
            trade_key,
            json.dumps(trade_data, ensure_ascii=False),
            ex=60 * 60 * 24 * 7,
        )
        redis_client.srem(open_set_key, trade_key)

        stats_key = get_stats_key(signal_type)
        if result == "win":
            redis_client.hincrby(stats_key, "wins", 1)
        elif result == "loss":
            redis_client.hincrby(stats_key, "losses", 1)
        elif result == "expired":
            redis_client.hincrby(stats_key, "expired", 1)

        return True

    except Exception as e:
        logger.error(f"mark_trade_closed error on {trade_key}: {e}")
        return False


def update_open_trades(redis_client, signal_type: str = "long", timeframe: str = "15m", max_age_hours: int = 24):
    """
    يراجع الصفقات المفتوحة:
    - لو السعر لمس TP1 أولًا => win
    - لو لمس SL أولًا => loss
    - لو عدى عليها وقت طويل => expired
    """
    if redis_client is None:
        return

    open_set_key = get_open_trades_set_key(signal_type)

    try:
        trade_keys = list(redis_client.smembers(open_set_key))
    except Exception as e:
        logger.error(f"update_open_trades set read error: {e}")
        return

    now_ts = int(time.time())
    max_age_seconds = max_age_hours * 3600

    for trade_key in trade_keys:
        trade = load_trade(redis_client, trade_key)
        if not trade:
            try:
                redis_client.srem(open_set_key, trade_key)
            except Exception:
                pass
            continue

        if trade.get("status") != "open":
            try:
                redis_client.srem(open_set_key, trade_key)
            except Exception:
                pass
            continue

        symbol = trade["symbol"]
        entry = safe_float(trade["entry"])
        sl = safe_float(trade["sl"])
        tp1 = safe_float(trade["tp1"])
        created_at = int(trade.get("created_at", now_ts))

        # Expired
        if now_ts - created_at > max_age_seconds:
            mark_trade_closed(redis_client, trade_key, trade, "expired")
            logger.info(f"{symbol} → trade expired")
            continue

        raw_candles = fetch_recent_candles(symbol, timeframe=timeframe, limit=100)
        candles = normalize_candles(raw_candles)

        if not candles:
            continue

        result = None

        # نراجع الشموع بعد وقت الإنشاء
        for candle in candles:
            candle_ts = candle["ts"]
            if candle_ts > 10_000_000_000:
                candle_ts = candle_ts // 1000

            if candle_ts < created_at:
                continue

            low = safe_float(candle["low"])
            high = safe_float(candle["high"])

            # Long logic
            if low <= sl:
                result = "loss"
                break

            if high >= tp1:
                result = "win"
                break

        if result:
            mark_trade_closed(redis_client, trade_key, trade, result)
            logger.info(f"{symbol} → trade closed as {result}")


def get_winrate_summary(redis_client, signal_type: str = "long"):
    if redis_client is None:
        return {
            "wins": 0,
            "losses": 0,
            "expired": 0,
            "open": 0,
            "winrate": 0.0,
        }

    stats_key = get_stats_key(signal_type)
    open_set_key = get_open_trades_set_key(signal_type)

    try:
        stats = redis_client.hgetall(stats_key) or {}
        wins = int(stats.get("wins", 0))
        losses = int(stats.get("losses", 0))
        expired = int(stats.get("expired", 0))
        open_count = int(redis_client.scard(open_set_key) or 0)

        decided = wins + losses
        winrate = round((wins / decided) * 100, 2) if decided > 0 else 0.0

        return {
            "wins": wins,
            "losses": losses,
            "expired": expired,
            "open": open_count,
            "winrate": winrate,
        }

    except Exception as e:
        logger.error(f"get_winrate_summary error: {e}")
        return {
            "wins": 0,
            "losses": 0,
            "expired": 0,
            "open": 0,
            "winrate": 0.0,
        }


def format_winrate_summary(summary: dict) -> str:
    return (
        f"Win rate: {summary['winrate']}% | "
        f"Wins: {summary['wins']} | "
        f"Losses: {summary['losses']} | "
        f"Expired: {summary['expired']} | "
        f"Open: {summary['open']}"
    )
