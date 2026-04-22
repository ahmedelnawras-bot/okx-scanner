import json
import time
import logging
import requests
from collections import Counter, defaultdict

logger = logging.getLogger("okx-scanner")

OKX_CANDLES_URL = "https://www.okx.com/api/v5/market/candles"
TRADE_TTL_SECONDS = 60 * 60 * 24 * 30           # 30 days — للتقارير العادية
TRADE_HISTORY_TTL_SECONDS = 60 * 60 * 24 * 90   # 90 days — للـ Hybrid Label history


# =========================
# BASIC HELPERS
# =========================
def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def safe_int(value, default=0):
    try:
        return int(float(value))
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


def normalize_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "y")
    return bool(value)


def normalize_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


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


# =========================
# TP HELPERS
# =========================
def calc_tp1(entry: float, sl: float, side: str = "long") -> float:
    """
    TP1 = 1.5× risk
    """
    side = normalize_side(side)
    risk = abs(float(entry) - float(sl))

    if side == "short":
        return round(float(entry) - (risk * 1.5), 6)
    return round(float(entry) + (risk * 1.5), 6)


def calc_tp2(entry: float, sl: float, side: str = "long") -> float:
    """
    TP2 = 3× risk
    """
    side = normalize_side(side)
    risk = abs(float(entry) - float(sl))

    if side == "short":
        return round(float(entry) - (risk * 3.0), 6)
    return round(float(entry) + (risk * 3.0), 6)


# =========================
# OKX CANDLES
# =========================
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


# =========================
# REDIS INDEX CLEANUP
# =========================
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


# =========================
# SERIALIZATION HELPERS
# =========================
def build_trade_diagnostics(extra_fields: dict = None) -> dict:
    """
    حاوية مرنة للحقول الإضافية الخاصة بالتحليل.
    الهدف: نقدر نضيف أي حقول من main لاحقًا بدون كسر النسخة الحالية.
    """
    extra_fields = extra_fields or {}

    diagnostics = {
        "raw_score": safe_float(extra_fields.get("raw_score"), 0.0),
        "effective_score": safe_float(extra_fields.get("effective_score"), 0.0),
        "dynamic_threshold": safe_float(extra_fields.get("dynamic_threshold"), 0.0),
        "required_min_score": safe_float(extra_fields.get("required_min_score"), 0.0),
        "dist_ma": safe_float(extra_fields.get("dist_ma"), 0.0),
        "rank_volume_24h": safe_float(extra_fields.get("rank_volume_24h"), 0.0),
        "market_state": str(extra_fields.get("market_state", "") or ""),
        "market_state_label": str(extra_fields.get("market_state_label", "") or ""),
        "market_bias_label": str(extra_fields.get("market_bias_label", "") or ""),
        "alt_mode": str(extra_fields.get("alt_mode", "") or ""),
        "entry_timing": str(extra_fields.get("entry_timing", "") or ""),
        "opportunity_type": str(extra_fields.get("opportunity_type", "") or ""),
        "early_priority": str(extra_fields.get("early_priority", "") or ""),
        "breakout_quality": str(extra_fields.get("breakout_quality", "") or ""),
        "alert_id": str(extra_fields.get("alert_id", "") or ""),
        "fake_signal": normalize_bool(extra_fields.get("fake_signal", False)),
        "is_reverse": normalize_bool(extra_fields.get("is_reverse", False)),
        "reversal_4h_confirmed": normalize_bool(extra_fields.get("reversal_4h_confirmed", False)),
        "has_high_impact_news": normalize_bool(extra_fields.get("has_high_impact_news", False)),
        "news_titles": normalize_list(extra_fields.get("news_titles", [])),
        "warning_reasons": normalize_list(extra_fields.get("warning_reasons", [])),
    }

    return diagnostics


def build_history_snapshot(trade_data: dict) -> dict:
    """
    نسخة أخف للتخزين في trade_history.
    مفيدة لاحقًا للتحليل بدون الاحتياج لكل تفاصيل الصفقة.
    """
    diagnostics = trade_data.get("diagnostics", {}) or {}

    return {
        "symbol": trade_data.get("symbol", ""),
        "market_type": trade_data.get("market_type", "futures"),
        "side": trade_data.get("side", "long"),
        "setup_type": trade_data.get("setup_type", "unknown"),
        "score": safe_float(trade_data.get("score"), 0.0),
        "entry": safe_float(trade_data.get("entry"), 0.0),
        "sl": safe_float(trade_data.get("sl"), 0.0),
        "tp1": safe_float(trade_data.get("tp1"), 0.0),
        "tp2": safe_float(trade_data.get("tp2"), 0.0),
        "created_at": safe_int(trade_data.get("created_at"), 0),
        "status": trade_data.get("status", "open"),
        "result": trade_data.get("result"),
        "tp1_hit": normalize_bool(trade_data.get("tp1_hit", False)),

        "market_state": diagnostics.get("market_state", ""),
        "alt_mode": diagnostics.get("alt_mode", ""),
        "entry_timing": diagnostics.get("entry_timing", ""),
        "opportunity_type": diagnostics.get("opportunity_type", ""),
        "early_priority": diagnostics.get("early_priority", ""),
        "breakout_quality": diagnostics.get("breakout_quality", ""),
        "fake_signal": diagnostics.get("fake_signal", False),
        "is_reverse": diagnostics.get("is_reverse", False),
        "reversal_4h_confirmed": diagnostics.get("reversal_4h_confirmed", False),
    }


# =========================
# TRADE STORAGE
# =========================
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

    # === حقول تحليلية إضافية اختيارية ===
    raw_score: float = None,
    effective_score: float = None,
    dynamic_threshold: float = None,
    required_min_score: float = None,
    dist_ma: float = None,
    entry_timing: str = None,
    opportunity_type: str = None,
    market_state: str = None,
    market_state_label: str = None,
    market_bias_label: str = None,
    alt_mode: str = None,
    early_priority: str = None,
    breakout_quality: str = None,
    fake_signal: bool = False,
    is_reverse_signal: bool = False,
    reversal_4h_confirmed: bool = False,
    rank_volume_24h: float = None,
    alert_id: str = None,
    has_high_impact_news: bool = False,
    news_titles=None,
    warning_reasons=None,
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

    signal_event = "breakdown" if side == "short" else "breakout"

    diagnostics = build_trade_diagnostics({
        "raw_score": score if raw_score is None else raw_score,
        "effective_score": score if effective_score is None else effective_score,
        "dynamic_threshold": dynamic_threshold,
        "required_min_score": required_min_score,
        "dist_ma": dist_ma,
        "rank_volume_24h": rank_volume_24h,
        "market_state": market_state,
        "market_state_label": market_state_label,
        "market_bias_label": market_bias_label,
        "alt_mode": alt_mode,
        "entry_timing": entry_timing,
        "opportunity_type": opportunity_type,
        "early_priority": early_priority,
        "breakout_quality": breakout_quality,
        "alert_id": alert_id,
        "fake_signal": fake_signal,
        "is_reverse": is_reverse_signal,
        "reversal_4h_confirmed": reversal_4h_confirmed,
        "has_high_impact_news": has_high_impact_news,
        "news_titles": news_titles or [],
        "warning_reasons": warning_reasons or [],
    })

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
        "warning_reasons": list(warning_reasons or []),

        # legacy fields
        "pre_breakout": pre_signal,
        "breakout": break_signal,

        # generic fields
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

        # diagnostics container
        "diagnostics": diagnostics,
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

        history_data = build_history_snapshot(trade_data)
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


def update_trade_history_snapshot(redis_client, trade_data: dict):
    if redis_client is None or not trade_data:
        return False

    try:
        market_type = normalize_market_type(trade_data.get("market_type", "futures"))
        side = normalize_side(trade_data.get("side", "long"))
        symbol = trade_data.get("symbol", "")
        candle_time = safe_int(trade_data.get("candle_time"), 0)

        history_key = get_trade_history_key(market_type, side, symbol, candle_time)
        history_data = build_history_snapshot(trade_data)

        redis_client.set(
            history_key,
            json.dumps(history_data, ensure_ascii=False),
            ex=TRADE_HISTORY_TTL_SECONDS,
        )
        return True

    except Exception as e:
        logger.warning(f"update_trade_history_snapshot error: {e}")
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

        update_trade_history_snapshot(redis_client, trade_data)
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
        update_trade_history_snapshot(redis_client, trade_data)
        return ok

    except Exception as e:
        logger.error(f"mark_tp1_hit error on {trade_key}: {e}")
        return False


# =========================
# TRADE EVALUATION
# =========================
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
            elif high <= sl:
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


# =========================
# SUMMARY HELPERS
# =========================
def _empty_summary(setup_type=None):
    return {
        "setup_type": setup_type,
        "total": 0,
        "closed": 0,
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


def _apply_trade_to_summary(summary: dict, trade: dict):
    summary["total"] += 1

    if normalize_bool(trade.get("tp1_hit", False)):
        summary["tp1_hits"] += 1

    status = str(trade.get("status", "") or "").lower().strip()
    result = str(trade.get("result", "") or "").lower().strip()

    if status in ("open", "partial"):
        summary["open"] += 1
        return

    if result in ("tp1_win", "tp2_win", "loss", "expired"):
        summary["closed"] += 1

    if result == "tp1_win":
        summary["wins"] += 1
        summary["tp1_wins"] += 1
    elif result == "tp2_win":
        summary["wins"] += 1
        summary["tp2_wins"] += 1
    elif result == "loss":
        summary["losses"] += 1
    elif result == "expired":
        summary["expired"] += 1


def _finalize_summary(summary: dict):
    decided = summary["wins"] + summary["losses"]
    base_for_tp1 = summary["total"]

    summary["winrate"] = round((summary["wins"] / decided) * 100, 2) if decided > 0 else 0.0
    summary["tp1_rate"] = round((summary["tp1_hits"] / base_for_tp1) * 100, 2) if base_for_tp1 > 0 else 0.0
    return summary


# =========================
# SETUP TYPE STATS
# =========================
def get_setup_type_stats(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    setup_type: str = None,
    since_ts: int = None,
) -> dict:
    """
    يقرأ من trade_history — محمي من الـ reset.
    """
    summary = _empty_summary(setup_type=setup_type)

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

            if since_ts is not None:
                created_at = safe_int(trade.get("created_at", 0), 0)
                if created_at < int(since_ts):
                    continue

            _apply_trade_to_summary(summary, trade)

        return _finalize_summary(summary)

    except Exception as e:
        logger.error(f"get_setup_type_stats error: {e}")
        return summary


# =========================
# WINRATE / PERIOD REPORTS
# =========================
def get_winrate_summary(redis_client, market_type: str = "futures", side: str = "long"):
    if redis_client is None:
        return {
            "wins": 0,
            "tp1_wins": 0,
            "tp2_wins": 0,
            "losses": 0,
            "expired": 0,
            "open": 0,
            "closed": 0,
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
        closed = wins + losses + expired
        total_signals = closed + open_count

        winrate = round((wins / decided) * 100, 2) if decided > 0 else 0.0
        tp1_rate = round((tp1_hits / total_signals) * 100, 2) if total_signals > 0 else 0.0

        return {
            "wins": wins,
            "tp1_wins": tp1_wins,
            "tp2_wins": tp2_wins,
            "losses": losses,
            "expired": expired,
            "open": open_count,
            "closed": closed,
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
            "closed": 0,
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
        return _empty_summary()

    all_trades_key = get_all_trades_set_key()

    try:
        trade_keys = list(redis_client.smembers(all_trades_key))
    except Exception as e:
        logger.error(f"get_trade_summary set read error: {e}")
        return _empty_summary()

    summary = _empty_summary()

    for trade_key in trade_keys:
        trade = load_trade(redis_client, trade_key)
        if not trade:
            continue

        trade_market = normalize_market_type(trade.get("market_type", "futures"))
        trade_side = normalize_side(trade.get("side", "long"))
        created_at = safe_int(trade.get("created_at", 0), 0)

        if market_type and trade_market != normalize_market_type(market_type):
            continue

        if side and trade_side != normalize_side(side):
            continue

        if since_ts is not None and created_at < int(since_ts):
            continue

        _apply_trade_to_summary(summary, trade)

    return _finalize_summary(summary)


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


# =========================
# DATA EXTRACTION FOR DIAGNOSTICS
# =========================
def get_all_trades_data(
    redis_client,
    market_type: str = None,
    side: str = None,
    since_ts: int = None,
    use_history: bool = False,
):
    """
    use_history=False -> يقرأ من trade:* (أغنى في الداتا)
    use_history=True  -> يقرأ من trade_history:* (أخف وأسرع لبعض التحليلات)
    """
    if redis_client is None:
        return []

    trades = []

    try:
        if use_history:
            pattern = "trade_history:*"
            for key in redis_client.scan_iter(pattern):
                raw = redis_client.get(key)
                if not raw:
                    continue
                try:
                    trade = json.loads(raw)
                except Exception:
                    continue

                trade_market = normalize_market_type(trade.get("market_type", "futures"))
                trade_side = normalize_side(trade.get("side", "long"))
                created_at = safe_int(trade.get("created_at", 0), 0)

                if market_type and trade_market != normalize_market_type(market_type):
                    continue
                if side and trade_side != normalize_side(side):
                    continue
                if since_ts is not None and created_at < int(since_ts):
                    continue

                trades.append(trade)
            return trades

        trade_keys = list(redis_client.smembers(get_all_trades_set_key()))
        for trade_key in trade_keys:
            trade = load_trade(redis_client, trade_key)
            if not trade:
                continue

            trade_market = normalize_market_type(trade.get("market_type", "futures"))
            trade_side = normalize_side(trade.get("side", "long"))
            created_at = safe_int(trade.get("created_at", 0), 0)

            if market_type and trade_market != normalize_market_type(market_type):
                continue
            if side and trade_side != normalize_side(side):
                continue
            if since_ts is not None and created_at < int(since_ts):
                continue

            trades.append(trade)

    except Exception as e:
        logger.error(f"get_all_trades_data error: {e}")

    return trades


def summarize_by_field(
    redis_client,
    field_name: str,
    market_type: str = None,
    side: str = None,
    since_ts: int = None,
    min_closed: int = 1,
    use_history: bool = False,
):
    """
    دالة عامة مفيدة للملف التحليلي القادم.
    """
    rows = get_all_trades_data(
        redis_client=redis_client,
        market_type=market_type,
        side=side,
        since_ts=since_ts,
        use_history=use_history,
    )

    grouped = defaultdict(lambda: _empty_summary())

    for trade in rows:
        diagnostics = trade.get("diagnostics", {}) or {}

        if field_name in trade:
            field_value = trade.get(field_name)
        else:
            field_value = diagnostics.get(field_name)

        field_value = str(field_value if field_value not in (None, "") else "unknown")
        _apply_trade_to_summary(grouped[field_value], trade)

    output = []
    for value, summary in grouped.items():
        _finalize_summary(summary)
        if summary["closed"] < min_closed:
            continue
        output.append({
            "field_value": value,
            **summary,
        })

    output.sort(key=lambda x: (x["winrate"], x["closed"], x["wins"]), reverse=True)
    return output


def get_common_loss_reasons(
    redis_client,
    market_type: str = None,
    side: str = None,
    since_ts: int = None,
    top_n: int = 10,
):
    rows = get_all_trades_data(
        redis_client=redis_client,
        market_type=market_type,
        side=side,
        since_ts=since_ts,
        use_history=False,
    )

    reasons_counter = Counter()

    for trade in rows:
        result = str(trade.get("result", "") or "").lower().strip()
        if result != "loss":
            continue

        reasons = normalize_list(trade.get("reasons", []))
        warning_reasons = normalize_list(trade.get("warning_reasons", []))
        merged = list(reasons) + list(warning_reasons)

        for reason in merged:
            rr = str(reason or "").strip()
            if rr:
                reasons_counter[rr] += 1

    return reasons_counter.most_common(top_n)


# =========================
# TEXT FORMATTERS
# =========================
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
