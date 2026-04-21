import json
import time
import logging
import requests

logger = logging.getLogger("okx-scanner")

OKX_CANDLES_URL = "https://www.okx.com/api/v5/market/candles"
TRADE_TTL_SECONDS = 60 * 60 * 24 * 30       # 30 days — للتقارير العادية
TRADE_HISTORY_TTL_SECONDS = 60 * 60 * 24 * 90  # 90 days — للـ Hybrid Label history


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


def get_trade_history_key(market_type: str, side: str, symbol: str, candle_time: int) -> str:
    """
    Key منفصل للـ Hybrid Label history.
    لا يتأثر بـ reset_stats — TTL 90 يوم.
    """
    market_type = normalize_market_type(market_type)
    side = normalize_side(side)
    return f"trade_history:{market_type}:{side}:{symbol}:{candle_time}"


def calc_tp1(entry: float, sl: float, side: str = "long") -> float:
    """
    TP1 = 1.5× risk
    عند 47% win rate → رابح بدل breakeven
    """
    side = normalize_side(side)
    risk = abs(entry - sl)

    if side == "short":
        return round(entry - (risk * 1.5), 6)
    return round(entry + (risk * 1.5), 6)


def calc_tp2(entry: float, sl: float, side: str = "long") -> float:
    """
    TP2 = 3× risk
    هدف بعيد للصفقات القوية
    """
    side = normalize_side(side)
    risk = abs(entry - sl)

    if side == "short":
        return round(entry - (risk * 3.0), 6)
    return round(entry + (risk * 3.0), 6)


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


def cleanup_missing_trades_from_index(redis_client) -> int:
    """
    ينظف trades:all من المفاتيح التي لم تعد موجودة فعلياً في Redis.
    مفيد بعد reset جزئي للشورت أو اللونج.
    """
    if redis_client is None:
        return 0

    all_trades_key = get_all_trades_set_key()

    try:
        trade_keys = list(redis_client.smembers(all_trades_key))
    except Exception as e:
        logger.error(f"cleanup_missing_trades_from_index read error: {e}")
        return 0

    removed = 0

    for trade_key in trade_keys:
        try:
            if not redis_client.exists(trade_key):
                redis_client.srem(all_trades_key, trade_key)
                removed += 1
        except Exception:
            continue

    if removed > 0:
        logger.info(f"cleanup_missing_trades_from_index → removed {removed} stale keys")

    return removed


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
    reasons=None,
    pre_breakout: bool = False,
    breakout: bool = False,
    vol_ratio: float = 0.0,
    candle_strength: float = 0.0,
    mtf_confirmed: bool = False,
    is_new: bool = False,
    btc_dominance_proxy: str = "🟡 محايد",
    change_24h: float = 0.0,
    tp1: float = None,
    tp2: float = None,
    setup_type: str = None,
):
    if redis_client is None:
        return False

    market_type = normalize_market_type(market_type)
    side = normalize_side(side)

    entry = round(float(entry), 6)
    sl = round(float(sl), 6)
    tp1 = round(float(tp1), 6) if tp1 is not None else calc_tp1(entry, sl, side=side)
    tp2 = round(float(tp2), 6) if tp2 is not None else calc_tp2(entry, sl, side=side)

    trade_key = get_trade_key(market_type, side, symbol, candle_time)
    history_key = get_trade_history_key(market_type, side, symbol, candle_time)
    open_set_key = get_open_trades_set_key(market_type, side)
    all_trades_key = get_all_trades_set_key()

    now_ts = int(time.time())

    if reasons is None:
        reasons = []

    pre_signal = bool(pre_breakout)
    break_signal = bool(breakout)

    if side == "short":
        signal_event = "breakdown"
    else:
        signal_event = "breakout"

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

        # extra fields for backtesting / analytics
        "reasons": list(reasons),

        # legacy fields
        "pre_breakout": pre_signal,
        "breakout": break_signal,

        # new generic fields
        "signal_event": signal_event,
        "pre_signal": pre_signal,
        "break_signal": break_signal,

        "vol_ratio": round(float(vol_ratio), 4),
        "candle_strength": round(float(candle_strength), 4),
        "mtf_confirmed": bool(mtf_confirmed),
        "is_new": bool(is_new),
        "btc_dominance_proxy": btc_dominance_proxy,
        "change_24h": round(float(change_24h), 2),

        # setup type للـ Hybrid Label
        "setup_type": setup_type or "unknown",
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

        # كتابة trade_history — لا يتأثر بـ reset_stats
        history_data = {
            "symbol": symbol,
            "market_type": market_type,
            "side": side,
            "setup_type": setup_type or "unknown",
            "score": round(float(score), 2),
            "entry": entry,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "created_at": now_ts,
            "status": "open",
            "result": None,
            "tp1_hit": False,
        }
        redis_client.set(
            history_key,
            json.dumps(history_data, ensure_ascii=False),
            ex=TRADE_HISTORY_TTL_SECONDS,
        )

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

        if result == "tp1_win":
            redis_client.hincrby(stats_key, "tp1_wins", 1)
            redis_client.hincrby(stats_key, "wins", 1)
        elif result == "tp2_win":
            redis_client.hincrby(stats_key, "tp2_wins", 1)
            redis_client.hincrby(stats_key, "wins", 1)
        elif result == "loss":
            redis_client.hincrby(stats_key, "losses", 1)
        elif result == "expired":
            redis_client.hincrby(stats_key, "expired", 1)

        # تحديث trade_history بالنتيجة النهائية
        try:
            symbol = trade_data.get("symbol", "")
            candle_time = trade_data.get("candle_time", 0)
            history_key = get_trade_history_key(market_type, side, symbol, candle_time)
            raw_history = redis_client.get(history_key)
            if raw_history:
                history_data = json.loads(raw_history)
                history_data["status"] = "closed"
                history_data["result"] = result
                history_data["tp1_hit"] = bool(trade_data.get("tp1_hit", False))
                history_data["closed_at"] = int(time.time())
                redis_client.set(
                    history_key,
                    json.dumps(history_data, ensure_ascii=False),
                    ex=TRADE_HISTORY_TTL_SECONDS,
                )
        except Exception as he:
            logger.warning(f"mark_trade_closed: failed to update history key: {he}")

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

        ok = save_trade(redis_client, trade_key, trade_data)

        # تحديث tp1_hit في trade_history
        try:
            symbol = trade_data.get("symbol", "")
            candle_time = trade_data.get("candle_time", 0)
            history_key = get_trade_history_key(market_type, side, symbol, candle_time)
            raw_history = redis_client.get(history_key)
            if raw_history:
                history_data = json.loads(raw_history)
                history_data["tp1_hit"] = True
                redis_client.set(
                    history_key,
                    json.dumps(history_data, ensure_ascii=False),
                    ex=TRADE_HISTORY_TTL_SECONDS,
                )
        except Exception as he:
            logger.warning(f"mark_tp1_hit: failed to update history key: {he}")

        return ok
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
                    result = "tp2_win"
        else:
            if high >= tp2:
                result = "tp2_win"
            elif low <= sl:
                result = "tp1_win"

    else:  # short
        if not tp1_hit:
            if high >= sl:
                result = "loss"
            elif low <= tp1:
                tp1_now = True
                if low <= tp2:
                    result = "tp2_win"
        else:
            if low <= tp2:
                result = "tp2_win"
            elif high >= sl:
                result = "tp1_win"

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

                if result == "tp2_win":
                    break

            if result:
                break

        if result:
            mark_trade_closed(redis_client, trade_key, trade, result)
            logger.info(f"{symbol} → trade closed as {result}")
        elif state_changed:
            logger.info(f"{symbol} → trade updated")


def get_setup_type_stats(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    setup_type: str = None,
    since_ts: int = None,
) -> dict:
    """
    يقرأ من trade_history (مش trade) — محمي من الـ reset.
    يرجع إحصائيات الـ setup_type للـ Hybrid Label.
    since_ts: يفلتر الـ trades القديمة قبل آخر reset.
    """
    summary = {
        "setup_type": setup_type,
        "total": 0,
        "closed": 0,
        "wins": 0,
        "losses": 0,
        "expired": 0,
        "tp1_hits": 0,
        "winrate": 0.0,
        "tp1_rate": 0.0,
    }

    if not redis_client or not setup_type:
        return summary

    market_type = normalize_market_type(market_type)
    side = normalize_side(side)

    try:
        pattern = f"trade_history:{market_type}:{side}:*"
        for key in redis_client.scan_iter(pattern):
            raw = redis_client.get(key)
            if not raw:
                continue

            try:
                trade = json.loads(raw)
            except Exception:
                continue

            if str(trade.get("setup_type", "unknown")) != str(setup_type):
                continue

            # فلتر الـ trades القديمة قبل آخر reset
            if since_ts is not None:
                created_at = int(trade.get("created_at", 0) or 0)
                if created_at < since_ts:
                    continue

            summary["total"] += 1

            status = str(trade.get("status", "")).lower().strip()
            result = str(trade.get("result", "") or "").lower().strip()
            tp1_hit = bool(trade.get("tp1_hit", False))

            if tp1_hit:
                summary["tp1_hits"] += 1

            if result in ("tp1_win", "tp2_win", "loss", "expired"):
                summary["closed"] += 1

            if result in ("tp1_win", "tp2_win"):
                summary["wins"] += 1
            elif result == "loss":
                summary["losses"] += 1
            elif result == "expired":
                summary["expired"] += 1

        if summary["closed"] > 0:
            summary["winrate"] = round(
                (summary["wins"] / summary["closed"]) * 100, 2
            )
            summary["tp1_rate"] = round(
                (summary["tp1_hits"] / summary["closed"]) * 100, 2
            )

        return summary

    except Exception as e:
        logger.error(f"get_setup_type_stats error: {e}")
        return summary


def get_winrate_summary(redis_client, market_type: str = "futures", side: str = "long"):
    if redis_client is None:
        return {
            "wins": 0,
            "tp1_wins": 0,
            "tp2_wins": 0,
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
        tp1_wins = int(stats.get("tp1_wins", 0))
        tp2_wins = int(stats.get("tp2_wins", 0))
        losses = int(stats.get("losses", 0))
        expired = int(stats.get("expired", 0))
        tp1_hits = int(stats.get("tp1_hits", 0))
        open_count = int(redis_client.scard(open_set_key) or 0)

        decided = wins + losses
        total_closed = wins + losses + expired
        total_signals = total_closed + open_count

        winrate = round((wins / decided) * 100, 2) if decided > 0 else 0.0
        tp1_rate = round((tp1_hits / total_signals) * 100, 2) if total_signals > 0 else 0.0

        return {
            "wins": wins,
            "tp1_wins": tp1_wins,
            "tp2_wins": tp2_wins,
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
            "tp1_wins": 0,
            "tp2_wins": 0,
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
            "tp1_wins": 0,
            "tp2_wins": 0,
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
            "tp1_wins": 0,
            "tp2_wins": 0,
            "losses": 0,
            "expired": 0,
            "open": 0,
            "tp1_hits": 0,
            "tp1_rate": 0.0,
            "winrate": 0.0,
        }

    wins = 0
    tp1_wins = 0
    tp2_wins = 0
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

        if status in ("open", "partial"):
            open_count += 1
        elif result == "tp1_win":
            wins += 1
            tp1_wins += 1
        elif result == "tp2_win":
            wins += 1
            tp2_wins += 1
        elif result == "loss":
            losses += 1
        elif result == "expired":
            expired += 1

    decided = wins + losses
    winrate = round((wins / decided) * 100, 2) if decided > 0 else 0.0
    tp1_rate = round((tp1_hits / total) * 100, 2) if total > 0 else 0.0

    return {
        "total": total,
        "wins": wins,
        "tp1_wins": tp1_wins,
        "tp2_wins": tp2_wins,
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
        f"TP1 Wins: {summary.get('tp1_wins', 0)} | "
        f"TP2 Wins: {summary.get('tp2_wins', 0)} | "
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
        f"Wins (TP1+): {summary['wins']}\n"
        f"• Full Wins (TP2): {summary.get('tp2_wins', 0)}\n"
        f"• TP1 Only: {summary.get('tp1_wins', 0)}\n"
        f"Losses: {summary['losses']}\n"
        f"Expired: {summary['expired']}\n"
        f"Open: {summary['open']}\n"
        f"Win rate: {summary['winrate']}%"
    )
