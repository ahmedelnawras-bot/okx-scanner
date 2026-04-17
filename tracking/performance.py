import json
import time
import logging
import requests

logger = logging.getLogger("okx-scanner")

OKX_CANDLES_URL = "https://www.okx.com/api/v5/market/candles"
TRADE_TTL_SECONDS = 60 * 60 * 24 * 30  # 30 days


def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def normalize_market_type(market_type: str) -> str:
    market_type = (market_type or "futures").strip().lower()
    if market_type not in ("futures", "spot"):
        return "futures"
    return market_type


def normalize_side(side: str) -> str:
    side = (side or "long").strip().lower()
    if side not in ("long", "short"):
        return "long"
    return side


def get_trade_key(market_type: str, side: str, symbol: str, candle_time: int) -> str:
    market_type = normalize_market_type(market_type)
    side = normalize_side(side)
    return f"trade:{market_type}:{side}:{symbol}:{candle_time}"


def get_open_trades_set_key(market_type: str = "futures", side: str = "long") -> str:
    market_type = normalize_market_type(market_type)
    side = normalize_side(side)
    return f"open_trades:{market_type}:{side}"


def get_stats_key(market_type: str = "futures", side: str = "long") -> str:
    market_type = normalize_market_type(market_type)
    side = normalize_side(side)
    return f"stats:{market_type}:{side}"


def get_all_trades_set_key() -> str:
    return "trades:all"


def calc_tp1(entry: float, sl: float, side: str = "long") -> float:
    side = normalize_side(side)
    risk = abs(entry - sl)

    if side == "short":
        return round(entry - risk, 6)
    return round(entry + risk, 6)


def calc_tp2(entry: float, sl: float, side: str = "long") -> float:
    side = normalize_side(side)
    risk = abs(entry - sl)

    if side == "short":
        return round(entry - (risk * 2), 6)
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
    market_type: str,
    side: str,
    candle_time: int,
    entry: float,
    sl: float,
    score: float,
    timeframe: str = "15m",
    btc_mode: str = "🟡 محايد",
    funding_label: str = "🟡 محايد",
):
    if redis_client is None:
        return False

    market_type = normalize_market_type(market_type)
    side = normalize_side(side)

    entry = round(float(entry), 6)
    sl = round(float(sl), 6)
    tp1 = calc_tp1(entry, sl, side=side)
    tp2 = calc_tp2(entry, sl, side=side)

    trade_key = get_trade_key(market_type, side, symbol, candle_time)
    open_set_key = get_open_trades_set_key(market_type, side)
    all_trades_key = get_all_trades_set_key()

    now_ts = int(time.time())

    trade_data = {
        "symbol": symbol,
        "market_type": market_type,
        "side": side,
        "timeframe": timeframe,
        "candle_time": int(candle_time),
        "entry": entry,
        "sl": sl,
        "initial_sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "score": round(float(score), 2),
        "btc_mode": btc_mode,
        "funding_label": funding_label,
        "status": "open",
        "tp1_hit": False,
        "tp1_hit_at": None,
        "created_at": now_ts,
        "updated_at": now_ts,
        "closed_at": None,
        "result": None,
    }

    try:
        created = redis_client.set(
            trade_key,
            json.dumps(trade_data, ensure_ascii=False),
            nx=True,
            ex=TRADE_TTL_SECONDS,
        )

        if not created:
            return False

        redis_client.sadd(open_set_key, trade_key)
        redis_client.sadd(all_trades_key, trade_key)
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
            ex=TRADE_TTL_SECONDS,
        )
        return True
    except Exception as e:
        logger.error(f"save_trade error on {trade_key}: {e}")
        return False


def mark_trade_closed(redis_client, trade_key: str, trade_data: dict, result: str):
    if redis_client is None:
        return False

    market_type = normalize_market_type(trade_data.get("market_type", "futures"))
    side = normalize_side(trade_data.get("side", "long"))
    open_set_key = get_open_trades_set_key(market_type, side)

    try:
        trade_data["status"] = "closed"
        trade_data["result"] = result
        trade_data["closed_at"] = int(time.time())
        trade_data["updated_at"] = int(time.time())

        redis_client.set(
            trade_key,
            json.dumps(trade_data, ensure_ascii=False),
            ex=TRADE_TTL_SECONDS,
        )
        redis_client.srem(open_set_key, trade_key)

        stats_key = get_stats_key(market_type, side)
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


def mark_tp1_hit(redis_client, trade_key: str, trade_data: dict):
    if redis_client is None:
        return False

    try:
        if trade_data.get("tp1_hit"):
            return True

        trade_data["tp1_hit"] = True
        trade_data["tp1_hit_at"] = int(time.time())
        trade_data["status"] = "partial"
        trade_data["sl"] = trade_data["entry"]
        trade_data["updated_at"] = int(time.time())

        market_type = normalize_market_type(trade_data.get("market_type", "futures"))
        side = normalize_side(trade_data.get("side", "long"))
        stats_key = get_stats_key(market_type, side)
        redis_client.hincrby(stats_key, "tp1_hits", 1)

        return save_trade(redis_client, trade_key, trade_data)
    except Exception as e:
        logger.error(f"mark_tp1_hit error on {trade_key}: {e}")
        return False


def evaluate_trade_on_candle(trade: dict, candle: dict):
    side = normalize_side(trade.get("side", "long"))
    sl = safe_float(trade["sl"])
    tp1 = safe_float(trade["tp1"])
    tp2 = safe_float(trade["tp2"])
    tp1_hit = bool(trade.get("tp1_hit", False))

    low = safe_float(candle["low"])
    high = safe_float(candle["high"])

    result = None
    tp1_now = False

    if side == "long":
        if not tp1_hit:
            if low <= sl:
                result = "loss"
            elif high >= tp1:
                tp1_now = True
                if high >= tp2:
                    result = "win"
        else:
            if high >= tp2:
                result = "win"
            elif low <= sl:
                result = "win"
    else:  # short
        if not tp1_hit:
            if high >= sl:
                result = "loss"
            elif low <= tp1:
                tp1_now = True
                if low <= tp2:
                    result = "win"
        else:
            if low <= tp2:
                result = "win"
            elif high >= sl:
                result = "win"

    return result, tp1_now


def update_open_trades(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    timeframe: str = "15m",
    max_age_hours: int = 24,
):
    if redis_client is None:
        return

    market_type = normalize_market_type(market_type)
    side = normalize_side(side)
    open_set_key = get_open_trades_set_key(market_type, side)

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

        if trade.get("status") == "closed":
            try:
                redis_client.srem(open_set_key, trade_key)
            except Exception:
                pass
            continue

        symbol = trade["symbol"]
        created_at = int(trade.get("created_at", now_ts))

        if now_ts - created_at > max_age_seconds:
            mark_trade_closed(redis_client, trade_key, trade, "expired")
            logger.info(f"{symbol} → trade expired")
            continue

        raw_candles = fetch_recent_candles(symbol, timeframe=timeframe, limit=100)
        candles = normalize_candles(raw_candles)

        if not candles:
            continue

        result = None
        state_changed = False

        for candle in candles:
            candle_ts = candle["ts"]
            if candle_ts > 10_000_000_000:
                candle_ts = candle_ts // 1000

            if candle_ts < created_at:
                continue

            result, tp1_now = evaluate_trade_on_candle(trade, candle)

            if tp1_now and not trade.get("tp1_hit"):
                ok = mark_tp1_hit(redis_client, trade_key, trade)
                if ok:
                    trade = load_trade(redis_client, trade_key) or trade
                    state_changed = True
                    logger.info(f"{symbol} → TP1 hit, SL moved to entry")
                else:
                    logger.error(f"{symbol} → failed to mark TP1")
                    break

                if result == "win":
                    break

            if result:
                break

        if result:
            mark_trade_closed(redis_client, trade_key, trade, result)
            logger.info(f"{symbol} → trade closed as {result}")
        elif state_changed:
            logger.info(f"{symbol} → trade updated")


def get_winrate_summary(redis_client, market_type: str = "futures", side: str = "long"):
    if redis_client is None:
        return {
            "wins": 0,
            "losses": 0,
            "expired": 0,
            "open": 0,
            "tp1_hits": 0,
            "tp1_rate": 0.0,
            "winrate": 0.0,
            "market_type": normalize_market_type(market_type),
            "side": normalize_side(side),
        }

    market_type = normalize_market_type(market_type)
    side = normalize_side(side)

    stats_key = get_stats_key(market_type, side)
    open_set_key = get_open_trades_set_key(market_type, side)

    try:
        stats = redis_client.hgetall(stats_key) or {}
        wins = int(stats.get("wins", 0))
        losses = int(stats.get("losses", 0))
        expired = int(stats.get("expired", 0))
        tp1_hits = int(stats.get("tp1_hits", 0))
        open_count = int(redis_client.scard(open_set_key) or 0)

        decided = wins + losses
        total_closed = wins + losses + expired

        winrate = round((wins / decided) * 100, 2) if decided > 0 else 0.0
        tp1_rate = round((tp1_hits / total_closed) * 100, 2) if total_closed > 0 else 0.0

        return {
            "wins": wins,
            "losses": losses,
            "expired": expired,
            "open": open_count,
            "tp1_hits": tp1_hits,
            "tp1_rate": tp1_rate,
            "winrate": winrate,
            "market_type": market_type,
            "side": side,
        }

    except Exception as e:
        logger.error(f"get_winrate_summary error: {e}")
        return {
            "wins": 0,
            "losses": 0,
            "expired": 0,
            "open": 0,
            "tp1_hits": 0,
            "tp1_rate": 0.0,
            "winrate": 0.0,
            "market_type": market_type,
            "side": side,
        }


def get_trade_summary(
    redis_client,
    market_type: str = None,
    side: str = None,
    since_ts: int = None,
):
    if redis_client is None:
        return {
            "total": 0,
            "wins": 0,
            "losses": 0,
            "expired": 0,
            "open": 0,
            "tp1_hits": 0,
            "tp1_rate": 0.0,
            "winrate": 0.0,
        }

    all_trades_key = get_all_trades_set_key()

    try:
        trade_keys = list(redis_client.smembers(all_trades_key))
    except Exception as e:
        logger.error(f"get_trade_summary set read error: {e}")
        return {
            "total": 0,
            "wins": 0,
            "losses": 0,
            "expired": 0,
            "open": 0,
            "tp1_hits": 0,
            "tp1_rate": 0.0,
            "winrate": 0.0,
        }

    wins = 0
    losses = 0
    expired = 0
    open_count = 0
    tp1_hits = 0
    total = 0

    for trade_key in trade_keys:
        trade = load_trade(redis_client, trade_key)
        if not trade:
            continue

        trade_market = normalize_market_type(trade.get("market_type", "futures"))
        trade_side = normalize_side(trade.get("side", "long"))
        created_at = int(trade.get("created_at", 0))

        if market_type and trade_market != normalize_market_type(market_type):
            continue

        if side and trade_side != normalize_side(side):
            continue

        if since_ts is not None and created_at < int(since_ts):
            continue

        total += 1

        if trade.get("tp1_hit"):
            tp1_hits += 1

        status = trade.get("status")
        result = trade.get("result")

        if status == "open" or status == "partial":
            open_count += 1
        elif result == "win":
            wins += 1
        elif result == "loss":
            losses += 1
        elif result == "expired":
            expired += 1

    decided = wins + losses
    total_closed = wins + losses + expired

    winrate = round((wins / decided) * 100, 2) if decided > 0 else 0.0
    tp1_rate = round((tp1_hits / total_closed) * 100, 2) if total_closed > 0 else 0.0

    return {
        "total": total,
        "wins": wins,
        "losses": losses,
        "expired": expired,
        "open": open_count,
        "tp1_hits": tp1_hits,
        "tp1_rate": tp1_rate,
        "winrate": winrate,
    }


def get_period_summary(redis_client, period: str = "all", market_type: str = None, side: str = None):
    now_ts = int(time.time())

    if period == "1h":
        since_ts = now_ts - (1 * 3600)
    elif period == "today":
        local_now = time.localtime(now_ts)
        since_ts = int(time.mktime((
            local_now.tm_year, local_now.tm_mon, local_now.tm_mday,
            0, 0, 0, local_now.tm_wday, local_now.tm_yday, local_now.tm_isdst
        )))
    elif period == "1d":
        since_ts = now_ts - (24 * 3600)
    elif period == "7d":
        since_ts = now_ts - (7 * 24 * 3600)
    elif period == "30d":
        since_ts = now_ts - (30 * 24 * 3600)
    else:
        since_ts = None

    return get_trade_summary(
        redis_client=redis_client,
        market_type=market_type,
        side=side,
        since_ts=since_ts,
    )


def format_winrate_summary(summary: dict) -> str:
    market_type = summary.get("market_type")
    side = summary.get("side")

    prefix = ""
    if market_type and side:
        prefix = f"[{market_type}/{side}] "

    return (
        f"{prefix}Win rate: {summary['winrate']}% | "
        f"TP1 Rate: {summary.get('tp1_rate', 0)}% | "
        f"Wins: {summary['wins']} | "
        f"Losses: {summary['losses']} | "
        f"TP1 Hits: {summary['tp1_hits']} | "
        f"Expired: {summary['expired']} | "
        f"Open: {summary['open']}"
    )


def format_period_summary(title: str, summary: dict) -> str:
    decided = summary["wins"] + summary["losses"] + summary["expired"]

    return (
        f"📊 {title}\n"
        f"Signals: {summary['total']}\n"
        f"Closed: {decided}\n"
        f"TP hits: {summary['wins']}\n"
        f"SL hits: {summary['losses']}\n"
        f"TP1 hits: {summary['tp1_hits']}\n"
        f"TP1 rate: {summary.get('tp1_rate', 0)}%\n"
        f"Expired: {summary['expired']}\n"
        f"Open: {summary['open']}\n"
        f"Win rate: {summary['winrate']}%"
    )
