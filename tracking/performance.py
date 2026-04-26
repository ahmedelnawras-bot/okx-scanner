# tracking/performance.py
"""
وحدة تتبع الأداء والتقارير المالية لبوت OKX Scanner.

تعتمد على:
- tracking/summary_helpers.py (دوال التلخيص الآمنة)

جميع الدوال العامة محفوظة للتوافق مع main.py دون أي تغيير في أسمائها.

تم التعديل لدعم تقارير الشورت بخطة إدارة رأس مال مستقلة:
- الشورت: 10 صفقات، مارجن 20$ لكل صفقة، رافعة 15x، إجمالي مارجن 200$، تعرض اسمي 3000$.
- اللونج: يبقى على الإعدادات القديمة (35% من رصيد 1000$).

تعديل مهم:
- تم استبدال التقريب الثابت round(..., 6) بدالة round_price()
  حتى لا تتحول أسعار العملات الصغيرة جداً إلى 0.000000.
- إضافة دوال تحليل الخروج والأداء اليومي وتقارير فورية.
"""

import json
import time
import logging
import requests
from collections import Counter, defaultdict
from typing import Optional, List, Dict

from tracking.summary_helpers import (
    safe_float,
    safe_int,
    safe_bool,
    normalize_side,
    calc_long_pct,
    calc_short_pct,
    calc_trade_result_pct,
    build_empty_summary,
    apply_trade_to_summary,
    finalize_summary,
    summarize_trades,
    _empty_summary,
    _apply_trade_to_summary,
    _finalize_summary,
    summarize_exits,
    summarize_trades_by_day,
    summarize_today,
    get_trade_created_ts,
    get_local_day_key,
)

logger = logging.getLogger("okx-scanner")

# ------------------------------------------------------------
# ثوابت
# ------------------------------------------------------------
OKX_CANDLES_URL = "https://www.okx.com/api/v5/market/candles"
TRADE_TTL_SECONDS = 60 * 60 * 24 * 30
TRADE_HISTORY_TTL_SECONDS = 60 * 60 * 24 * 90

# إعدادات اللونج
REPORT_ACCOUNT_BALANCE_USD = 1000.0
REPORT_MAX_CAPITAL_USAGE_PCT = 35.0
REPORT_DAILY_MAX_DRAWDOWN_PCT = 20.0
REPORT_ACTIVE_TRADE_SLOTS = 10
REPORT_LEVERAGE = 15.0

# إعدادات الشورت
SHORT_REPORT_MARGIN_PER_TRADE_USD = 20.0
SHORT_REPORT_ACTIVE_TRADE_SLOTS = 10
SHORT_REPORT_LEVERAGE = 15.0
SHORT_REPORT_TOTAL_MARGIN_USD = 200.0
SHORT_REPORT_NOTIONAL_PER_TRADE_USD = 300.0


# ------------------------------------------------------------
# دوال مساعدة عامة
# ------------------------------------------------------------
def normalize_market_type(market_type: str) -> str:
    market_type = (market_type or "futures").strip().lower()
    if market_type not in ("futures", "spot"):
        return "futures"
    return market_type


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


def normalize_text(value, default="unknown") -> str:
    text = str(value or "").strip()
    return text if text else default


def redis_hash_get(stats: dict, key: str, default=0):
    """
    يدعم Redis hgetall سواء كان decode_responses=True أو False.
    """
    if not isinstance(stats, dict):
        return default
    return stats.get(key, stats.get(key.encode(), default))


def round_price(value, default=0.0) -> float:
    """
    تقريب ذكي للأسعار.

    السبب:
    round(price, 6) يقتل العملات الصغيرة جداً ويحولها إلى 0.000000.
    لذلك نستخدم عدد منازل أكبر حسب حجم السعر.

    أمثلة:
    - BTC / ETH: 2-6 منازل كفاية
    - MASK / SOL: 6 منازل
    - SATS / PEPE / SHIB: 10-12 منزلة
    """
    price = safe_float(value, default)
    if price <= 0:
        return default

    abs_price = abs(price)

    if abs_price >= 100:
        return round(price, 4)
    if abs_price >= 1:
        return round(price, 6)
    if abs_price >= 0.01:
        return round(price, 8)
    if abs_price >= 0.0001:
        return round(price, 10)

    return round(price, 12)


# ------------------------------------------------------------
# مفاتيح Redis
# ------------------------------------------------------------
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
    market_type = normalize_market_type(market_type)
    side = normalize_side(side)
    return f"trade_history:{market_type}:{side}:{symbol}:{candle_time}"


# ------------------------------------------------------------
# TP HELPERS
# ------------------------------------------------------------
def calc_tp1(entry: float, sl: float, side: str = "long") -> float:
    side = normalize_side(side)
    entry = safe_float(entry, 0.0)
    sl = safe_float(sl, 0.0)
    risk = abs(entry - sl)

    if side == "short":
        return round_price(entry - (risk * 1.5))
    return round_price(entry + (risk * 1.5))


def calc_tp2(entry: float, sl: float, side: str = "long") -> float:
    side = normalize_side(side)
    entry = safe_float(entry, 0.0)
    sl = safe_float(sl, 0.0)
    risk = abs(entry - sl)

    if side == "short":
        return round_price(entry - (risk * 3.0))
    return round_price(entry + (risk * 3.0))


def recalc_targets_from_effective_entry(trade: dict, effective_entry: float) -> dict:
    side = normalize_side(trade.get("side", "long"))
    diagnostics = trade.get("diagnostics", {}) or {}
    rr1 = safe_float(diagnostics.get("rr1"), 1.5)
    rr2 = safe_float(diagnostics.get("rr2"), 3.0)
    effective_entry = safe_float(effective_entry, 0.0)
    sl = safe_float(trade.get("sl"), 0.0)

    if effective_entry <= 0 or sl <= 0:
        return trade

    risk = abs(effective_entry - sl)
    if risk <= 0:
        return trade

    if side == "short":
        trade["tp1"] = round_price(effective_entry - (risk * rr1))
        trade["tp2"] = round_price(effective_entry - (risk * rr2))
    else:
        trade["tp1"] = round_price(effective_entry + (risk * rr1))
        trade["tp2"] = round_price(effective_entry + (risk * rr2))

    diagnostics["rr1"] = rr1
    diagnostics["rr2"] = rr2
    trade["diagnostics"] = diagnostics
    return trade


# ------------------------------------------------------------
# OKX CANDLES
# ------------------------------------------------------------
def fetch_recent_candles(symbol: str, timeframe: str = "15m", limit: int = 100):
    try:
        res = requests.get(
            OKX_CANDLES_URL,
            params={"instId": symbol, "bar": timeframe, "limit": limit},
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


# ------------------------------------------------------------
# REDIS INDEX CLEANUP
# ------------------------------------------------------------
def cleanup_missing_trades_from_index(redis_client) -> int:
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


# ------------------------------------------------------------
# SERIALIZATION HELPERS
# ------------------------------------------------------------
def build_trade_diagnostics(extra_fields: dict = None) -> dict:
    extra_fields = extra_fields or {}

    pullback_entry = extra_fields.get("pullback_entry")
    pullback_low = extra_fields.get("pullback_low")
    pullback_high = extra_fields.get("pullback_high")
    effective_entry = extra_fields.get("effective_entry")

    diagnostics = {
        "raw_score": safe_float(extra_fields.get("raw_score"), 0.0),
        "effective_score": safe_float(extra_fields.get("effective_score"), 0.0),
        "dynamic_threshold": safe_float(extra_fields.get("dynamic_threshold"), 0.0),
        "required_min_score": safe_float(extra_fields.get("required_min_score"), 0.0),
        "dist_ma": safe_float(extra_fields.get("dist_ma"), 0.0),
        "rank_volume_24h": safe_float(extra_fields.get("rank_volume_24h"), 0.0),

        "market_state": normalize_text(extra_fields.get("market_state"), "unknown"),
        "market_state_label": normalize_text(extra_fields.get("market_state_label"), "unknown"),
        "market_bias_label": normalize_text(extra_fields.get("market_bias_label"), "unknown"),
        "alt_mode": normalize_text(extra_fields.get("alt_mode"), "unknown"),
        "entry_timing": normalize_text(extra_fields.get("entry_timing"), "unknown"),
        "opportunity_type": normalize_text(extra_fields.get("opportunity_type"), "unknown"),
        "early_priority": normalize_text(extra_fields.get("early_priority"), "unknown"),
        "breakout_quality": normalize_text(extra_fields.get("breakout_quality"), "unknown"),
        "risk_level": normalize_text(extra_fields.get("risk_level"), "unknown"),
        "alert_id": normalize_text(extra_fields.get("alert_id"), ""),

        "fake_signal": normalize_bool(extra_fields.get("fake_signal", False)),
        "is_reverse": normalize_bool(extra_fields.get("is_reverse", False)),
        "reversal_4h_confirmed": normalize_bool(extra_fields.get("reversal_4h_confirmed", False)),
        "has_high_impact_news": normalize_bool(extra_fields.get("has_high_impact_news", False)),

        "news_titles": normalize_list(extra_fields.get("news_titles", [])),
        "warning_reasons": normalize_list(extra_fields.get("warning_reasons", [])),

        "pullback_entry": round_price(pullback_entry) if pullback_entry is not None else None,
        "pullback_low": round_price(pullback_low) if pullback_low is not None else None,
        "pullback_high": round_price(pullback_high) if pullback_high is not None else None,
        "pullback_triggered": normalize_bool(extra_fields.get("pullback_triggered", False)),
        "effective_entry": round_price(effective_entry) if effective_entry is not None else None,

        "rr1": safe_float(extra_fields.get("rr1"), 1.5),
        "rr2": safe_float(extra_fields.get("rr2"), 3.0),
    }

    return diagnostics


def build_history_snapshot(trade_data: dict) -> dict:
    diagnostics = trade_data.get("diagnostics", {}) or {}

    return {
        "symbol": trade_data.get("symbol", ""),
        "market_type": trade_data.get("market_type", "futures"),
        "side": trade_data.get("side", "long"),
        "setup_type": trade_data.get("setup_type", "unknown"),

        "score": safe_float(trade_data.get("score"), 0.0),
        "entry": safe_float(trade_data.get("entry"), 0.0),
        "sl": safe_float(trade_data.get("sl"), 0.0),
        "initial_sl": safe_float(trade_data.get("initial_sl"), 0.0),
        "tp1": safe_float(trade_data.get("tp1"), 0.0),
        "tp2": safe_float(trade_data.get("tp2"), 0.0),

        "created_at": safe_int(trade_data.get("created_at"), 0),
        "status": trade_data.get("status", "open"),
        "result": trade_data.get("result"),
        "tp1_hit": normalize_bool(trade_data.get("tp1_hit", False)),

        "market_state": diagnostics.get("market_state", "unknown"),
        "market_state_label": diagnostics.get("market_state_label", "unknown"),
        "market_bias_label": diagnostics.get("market_bias_label", "unknown"),
        "alt_mode": diagnostics.get("alt_mode", "unknown"),
        "entry_timing": diagnostics.get("entry_timing", "unknown"),
        "opportunity_type": diagnostics.get("opportunity_type", "unknown"),
        "early_priority": diagnostics.get("early_priority", "unknown"),
        "breakout_quality": diagnostics.get("breakout_quality", "unknown"),
        "risk_level": diagnostics.get("risk_level", "unknown"),

        "dist_ma": safe_float(diagnostics.get("dist_ma"), 0.0),
        "raw_score": safe_float(diagnostics.get("raw_score"), 0.0),
        "effective_score": safe_float(diagnostics.get("effective_score"), 0.0),
        "dynamic_threshold": safe_float(diagnostics.get("dynamic_threshold"), 0.0),
        "required_min_score": safe_float(diagnostics.get("required_min_score"), 0.0),

        "fake_signal": diagnostics.get("fake_signal", False),
        "is_reverse": diagnostics.get("is_reverse", False),
        "reversal_4h_confirmed": diagnostics.get("reversal_4h_confirmed", False),
        "has_high_impact_news": diagnostics.get("has_high_impact_news", False),

        "warning_reasons": normalize_list(trade_data.get("warning_reasons", [])),
        "news_titles": normalize_list(diagnostics.get("news_titles", [])),

        "pullback_entry": diagnostics.get("pullback_entry"),
        "pullback_low": diagnostics.get("pullback_low"),
        "pullback_high": diagnostics.get("pullback_high"),
        "pullback_triggered": diagnostics.get("pullback_triggered", False),
        "effective_entry": diagnostics.get("effective_entry"),

        "rr1": safe_float(diagnostics.get("rr1"), 1.5),
        "rr2": safe_float(diagnostics.get("rr2"), 3.0),
        "diagnostics": diagnostics,
    }


# ------------------------------------------------------------
# TRADE STORAGE
# ------------------------------------------------------------
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
    risk_level: str = None,
    fake_signal: bool = False,
    is_reverse_signal: bool = False,
    reversal_4h_confirmed: bool = False,
    rank_volume_24h: float = None,
    alert_id: str = None,
    has_high_impact_news: bool = False,
    news_titles=None,
    warning_reasons=None,
    pullback_entry: float = None,
    pullback_low: float = None,
    pullback_high: float = None,
    rr1: float = 1.5,
    rr2: float = 3.0,
    **kwargs,
):
    if redis_client is None:
        return False

    market_type = normalize_market_type(market_type)
    side = normalize_side(side)

    entry = round_price(entry)
    sl = round_price(sl)

    tp1 = round_price(tp1) if tp1 is not None else calc_tp1(entry, sl, side=side)
    tp2 = round_price(tp2) if tp2 is not None else calc_tp2(entry, sl, side=side)

    pullback_entry = round_price(pullback_entry) if pullback_entry is not None else None
    pullback_low = round_price(pullback_low) if pullback_low is not None else None
    pullback_high = round_price(pullback_high) if pullback_high is not None else None

    trade_key = get_trade_key(market_type, side, symbol, candle_time)
    history_key = get_trade_history_key(market_type, side, symbol, candle_time)
    open_set_key = get_open_trades_set_key(market_type, side)
    all_trades_key = get_all_trades_set_key()

    now_ts = int(time.time())
    if reasons is None:
        reasons = []

    # توافق مع أسماء بديلة قد يرسلها main.py
    if "is_reverse" in kwargs and not is_reverse_signal:
        is_reverse_signal = normalize_bool(kwargs.get("is_reverse"))

    if "reverse_signal" in kwargs and not is_reverse_signal:
        is_reverse_signal = normalize_bool(kwargs.get("reverse_signal"))

    if warning_reasons is None and "warnings" in kwargs:
        warning_reasons = normalize_list(kwargs.get("warnings"))

    if not has_high_impact_news and "news_nearby" in kwargs:
        has_high_impact_news = normalize_bool(kwargs.get("news_nearby"))

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
        "risk_level": risk_level,
        "alert_id": alert_id,
        "fake_signal": fake_signal,
        "is_reverse": is_reverse_signal,
        "reversal_4h_confirmed": reversal_4h_confirmed,
        "has_high_impact_news": has_high_impact_news,
        "news_titles": news_titles or [],
        "warning_reasons": warning_reasons or [],
        "pullback_entry": pullback_entry,
        "pullback_low": pullback_low,
        "pullback_high": pullback_high,
        "pullback_triggered": False,
        "effective_entry": entry,
        "rr1": rr1,
        "rr2": rr2,
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

        "score": round(safe_float(score), 2),
        "btc_mode": btc_mode,
        "funding_label": funding_label,

        "status": "open",
        "tp1_hit": False,
        "tp1_hit_at": None,
        "created_at": now_ts,
        "updated_at": now_ts,
        "closed_at": None,
        "result": None,

        "reasons": list(reasons),
        "warning_reasons": list(warning_reasons or []),

        "pre_breakout": pre_signal,
        "breakout": break_signal,
        "signal_event": signal_event,
        "pre_signal": pre_signal,
        "break_signal": break_signal,

        "vol_ratio": round(safe_float(vol_ratio), 4),
        "candle_strength": round(safe_float(candle_strength), 4),
        "mtf_confirmed": bool(mtf_confirmed),
        "is_new": bool(is_new),
        "btc_dominance_proxy": btc_dominance_proxy,
        "change_24h": round(safe_float(change_24h), 2),

        "setup_type": setup_type or "unknown",
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

        diagnostics = trade_data.get("diagnostics", {}) or {}
        effective_entry = safe_float(
            diagnostics.get("effective_entry"),
            safe_float(trade_data.get("entry"), 0.0)
        )

        trade_data["tp1_hit"] = True
        trade_data["tp1_hit_at"] = int(time.time())
        trade_data["status"] = "partial"
        trade_data["sl"] = round_price(effective_entry) if effective_entry > 0 else trade_data["entry"]
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


# ------------------------------------------------------------
# TRADE EVALUATION
# ------------------------------------------------------------
def evaluate_trade_on_candle(trade: dict, candle: dict):
    side = normalize_side(trade.get("side", "long"))
    diagnostics = trade.get("diagnostics", {}) or {}

    entry = safe_float(trade.get("entry"), 0.0)
    effective_entry = safe_float(diagnostics.get("effective_entry"), entry)

    sl = safe_float(trade["sl"])
    tp1 = safe_float(trade["tp1"])
    tp2 = safe_float(trade["tp2"])
    tp1_hit = bool(trade.get("tp1_hit", False))

    low = safe_float(candle["low"])
    high = safe_float(candle["high"])

    result = None
    tp1_now = False

    pullback_entry = diagnostics.get("pullback_entry")
    pullback_high = diagnostics.get("pullback_high")
    pullback_low = diagnostics.get("pullback_low")
    pullback_triggered = normalize_bool(diagnostics.get("pullback_triggered", False))

    has_pullback_plan = (
        side == "long"
        and pullback_entry is not None
        and pullback_high is not None
    )

    if has_pullback_plan and not pullback_triggered:
        pb_entry = safe_float(pullback_entry, 0.0)

        if not (pb_entry > 0 and low <= pb_entry):
            return None, False, trade

        diagnostics["pullback_triggered"] = True
        diagnostics["effective_entry"] = round_price(pb_entry)
        trade["diagnostics"] = diagnostics

        trade = recalc_targets_from_effective_entry(trade, pb_entry)

        effective_entry = pb_entry
        tp1 = safe_float(trade["tp1"])
        tp2 = safe_float(trade["tp2"])

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

    else:
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

    return result, tp1_now, trade


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

            result, tp1_now, updated_trade = evaluate_trade_on_candle(trade, candle)
            trade = updated_trade

            if tp1_now and not trade.get("tp1_hit"):
                ok = mark_tp1_hit(redis_client, trade_key, trade)

                if ok:
                    trade = load_trade(redis_client, trade_key) or trade
                    state_changed = True
                    logger.info(f"{symbol} → TP1 hit, SL moved to entry/effective entry")
                else:
                    logger.error(f"{symbol} → failed to mark TP1")
                    break

                if result == "tp2_win":
                    break

            else:
                diagnostics = trade.get("diagnostics", {}) or {}
                if diagnostics.get("pullback_triggered") and not state_changed:
                    save_trade(redis_client, trade_key, trade)
                    update_trade_history_snapshot(redis_client, trade)
                    state_changed = True

            if result:
                break

        if result:
            mark_trade_closed(redis_client, trade_key, trade, result)
            logger.info(f"{symbol} → trade closed as {result}")
        elif state_changed:
            logger.info(f"{symbol} → trade updated")


# ------------------------------------------------------------
# LOAD TRADES
# ------------------------------------------------------------
def load_trades(
    redis_client,
    market_type: Optional[str] = None,
    side: Optional[str] = None,
    since_ts: Optional[int] = None,
    include_open: bool = True,
) -> List[dict]:
    if redis_client is None:
        return []

    mt = normalize_market_type(market_type) if market_type else "*"
    sd = normalize_side(side) if side else "*"
    pattern = f"trade:{mt}:{sd}:*"

    trades = []

    try:
        for key in redis_client.scan_iter(pattern):
            raw = redis_client.get(key)
            if not raw:
                continue

            try:
                trade = json.loads(raw)
            except Exception as e:
                logger.warning(f"load_trades: corrupted JSON for key {key}: {e}")
                continue

            if since_ts is not None:
                ts = get_trade_created_ts(trade)
                if not ts:
                    ts = safe_int(trade.get("candle_time", 0), 0)

                if ts < since_ts:
                    continue

            if not include_open:
                status = str(trade.get("status", "") or "").strip().lower()
                if status in ("open", "partial"):
                    continue

            trades.append(trade)

    except Exception as e:
        logger.error(f"load_trades error: {e}")

    return trades


# ------------------------------------------------------------
# SETUP TYPE STATS
# ------------------------------------------------------------
def get_setup_type_stats(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    setup_type: str = None,
    since_ts: int = None,
) -> dict:
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


# ------------------------------------------------------------
# WINRATE / PERIOD REPORTS
# ------------------------------------------------------------
def get_winrate_summary(redis_client, market_type: str = "futures", side: str = "long"):
    market_type = normalize_market_type(market_type)
    side = normalize_side(side)
    leverage = SHORT_REPORT_LEVERAGE if side == "short" else REPORT_LEVERAGE

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
            "market_type": market_type,
            "side": side,
            "leverage": leverage,
            "realized_raw_pnl_pct": 0.0,
            "realized_leveraged_pnl_pct": 0.0,
            "realized_pnl_pct": 0.0,
            "gross_profit_pct": 0.0,
            "gross_loss_pct": 0.0,
            "avg_win_pct": 0.0,
            "avg_loss_pct": 0.0,
            "best_trade_pct": 0.0,
            "worst_trade_pct": 0.0,
            "risk_status": "normal",
            "tp2_hits": 0,
            "tp2_rate": 0.0,
            "tp1_to_tp2_rate": 0.0,
            "net_profit_pct": 0.0,
        }

    stats_key = get_stats_key(market_type, side)
    open_set_key = get_open_trades_set_key(market_type, side)

    try:
        stats = redis_client.hgetall(stats_key) or {}

        wins = safe_int(redis_hash_get(stats, "wins", 0))
        tp1_wins = safe_int(redis_hash_get(stats, "tp1_wins", 0))
        tp2_wins = safe_int(redis_hash_get(stats, "tp2_wins", 0))
        losses = safe_int(redis_hash_get(stats, "losses", 0))
        expired = safe_int(redis_hash_get(stats, "expired", 0))
        tp1_hits = safe_int(redis_hash_get(stats, "tp1_hits", 0))
        open_count = safe_int(redis_client.scard(open_set_key) or 0)

        financial_summary = get_trade_summary(
            redis_client,
            market_type=market_type,
            side=side,
        )

        # financial_summary أدق لأنه مبني على الصفقات نفسها، وليس العدادات فقط.
        wins = safe_int(financial_summary.get("wins", wins))
        losses = safe_int(financial_summary.get("losses", losses))
        expired = safe_int(financial_summary.get("expired", expired))
        tp1_hits = safe_int(financial_summary.get("tp1_hits", tp1_hits))
        tp1_wins = safe_int(financial_summary.get("tp1_wins", tp1_wins))
        tp2_wins = safe_int(financial_summary.get("tp2_wins", tp2_wins))
        open_count = safe_int(financial_summary.get("open", open_count))

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
            "leverage": leverage,

            "realized_raw_pnl_pct": financial_summary.get("realized_raw_pnl_pct", 0.0),
            "realized_leveraged_pnl_pct": financial_summary.get("realized_leveraged_pnl_pct", 0.0),
            "realized_pnl_pct": financial_summary.get("realized_pnl_pct", 0.0),
            "gross_profit_pct": financial_summary.get("gross_profit_pct", 0.0),
            "gross_loss_pct": financial_summary.get("gross_loss_pct", 0.0),

            "avg_win_pct": financial_summary.get("avg_win_pct", 0.0),
            "avg_loss_pct": financial_summary.get("avg_loss_pct", 0.0),
            "best_trade_pct": financial_summary.get("best_trade_pct", 0.0),
            "worst_trade_pct": financial_summary.get("worst_trade_pct", 0.0),
            "risk_status": financial_summary.get("risk_status", "normal"),

            "tp2_hits": safe_int(financial_summary.get("tp2_hits", tp2_wins)),
            "tp2_rate": safe_float(financial_summary.get("tp2_rate", 0.0)),
            "tp1_to_tp2_rate": safe_float(financial_summary.get("tp1_to_tp2_rate", 0.0)),
            "net_profit_pct": safe_float(financial_summary.get(
                "net_profit_pct",
                financial_summary.get("realized_leveraged_pnl_pct", 0.0)
            )),
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
            "leverage": leverage,
            "realized_raw_pnl_pct": 0.0,
            "realized_leveraged_pnl_pct": 0.0,
            "realized_pnl_pct": 0.0,
            "gross_profit_pct": 0.0,
            "gross_loss_pct": 0.0,
            "avg_win_pct": 0.0,
            "avg_loss_pct": 0.0,
            "best_trade_pct": 0.0,
            "worst_trade_pct": 0.0,
            "risk_status": "normal",
            "tp2_hits": 0,
            "tp2_rate": 0.0,
            "tp1_to_tp2_rate": 0.0,
            "net_profit_pct": 0.0,
        }


def get_trade_summary(
    redis_client,
    market_type: Optional[str] = None,
    side: Optional[str] = None,
    since_ts: Optional[int] = None,
) -> dict:
    if redis_client is None:
        summary = _empty_summary()
        summary["market_type"] = market_type or "futures"
        summary["side"] = normalize_side(side or "long")
        return summary

    side_norm = normalize_side(side or "long")

    trades = load_trades(
        redis_client,
        market_type=market_type,
        side=side_norm,
        since_ts=since_ts,
        include_open=True,
    )

    summary = summarize_trades(trades)

    summary["market_type"] = market_type or "futures"
    summary["side"] = side_norm

    plan = get_report_sizing_plan(side_norm)
    for key in [
        "leverage",
        "margin_per_trade_usd",
        "active_trade_slots",
        "total_margin_used_usd",
        "notional_per_trade_usd",
        "total_notional_exposure_usd",
    ]:
        summary[key] = plan[key]

    return summary


# ------------------------------------------------------------
# PERIOD HELPER
# ------------------------------------------------------------
def get_period_since_ts(period: str) -> Optional[int]:
    """
    ترجع طابع زمني لبداية الفترة المطلوبة أو None لـ all.
    تدعم: 1h, today, 1d, 7d, 30d, all
    """
    period = str(period or "all").strip().lower()
    now_ts = int(time.time())

    if period == "1h":
        return now_ts - 3600

    if period == "today":
        local_now = time.localtime(now_ts)
        return int(time.mktime((
            local_now.tm_year,
            local_now.tm_mon,
            local_now.tm_mday,
            0,
            0,
            0,
            local_now.tm_wday,
            local_now.tm_yday,
            local_now.tm_isdst,
        )))

    if period == "1d":
        return now_ts - (24 * 3600)

    if period == "7d":
        return now_ts - (7 * 24 * 3600)

    if period == "30d":
        return now_ts - (30 * 24 * 3600)

    if period == "all":
        return None

    return now_ts - (24 * 3600)


def get_period_summary(
    redis_client,
    period: str = "all",
    market_type: Optional[str] = None,
    side: Optional[str] = None,
) -> dict:
    since_ts = get_period_since_ts(period)

    return get_trade_summary(
        redis_client=redis_client,
        market_type=market_type,
        side=side,
        since_ts=since_ts,
    )


# ------------------------------------------------------------
# EXIT SUMMARY
# ------------------------------------------------------------
def get_exit_summary(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    since_ts: Optional[int] = None,
    use_history: bool = False,
) -> dict:
    market_type = normalize_market_type(market_type)
    side = normalize_side(side)

    logger.info(f"get_exit_summary {market_type}/{side} use_history={use_history}")

    trades = get_all_trades_data(
        redis_client,
        market_type=market_type,
        side=side,
        since_ts=since_ts,
        use_history=use_history,
    )

    exit_data = summarize_exits(trades)
    exit_data["market_type"] = market_type
    exit_data["side"] = side
    exit_data["trades_count"] = len(trades)
    exit_data["since_ts"] = since_ts

    return exit_data


def format_exit_summary(title: str, summary: dict) -> str:
    signals = safe_int(summary.get("signals", summary.get("trades_count", 0)))
    closed = safe_int(summary.get("closed", 0))
    open_trades = safe_int(summary.get("open", 0))
    tp1_hits = safe_int(summary.get("tp1_hits", 0))
    tp2_hits = safe_int(summary.get("tp2_hits", 0))
    tp1_rate = safe_float(summary.get("tp1_rate", 0))
    tp2_rate = safe_float(summary.get("tp2_rate", 0))
    tp1_to_tp2_rate = safe_float(summary.get("tp1_to_tp2_rate", 0))
    sl_before_tp1 = safe_int(summary.get("sl_before_tp1", 0))
    sl_before_tp1_rate = safe_float(summary.get("sl_before_tp1_rate", 0))
    tp1_only = safe_int(summary.get("tp1_only", 0))
    expired = safe_int(summary.get("expired", 0))
    exit_quality = summary.get("exit_quality", "unknown")

    quality_ar = {
        "good": "جيد ✅",
        "exit_problem": "المشكلة في الخروج بعد TP1 ⚠️",
        "entry_problem": "المشكلة في الدخول/SL قبل TP1 🔴",
        "weak_entries": "جودة الدخول ضعيفة 🔴",
        "mixed": "مختلط 🟡",
        "unknown": "غير كافٍ",
    }
    quality_text = quality_ar.get(exit_quality, exit_quality)

    lines = [f"<b>{title}</b>", ""]
    lines.append(f"📊 Signals: {signals} | Closed: {closed} | Open: {open_trades}")
    lines.append(f"🎯 TP1 Hits: {tp1_hits} | TP2 Hits: {tp2_hits}")
    lines.append(f"📈 TP1 Rate: {tp1_rate:.1f}% | TP2 Rate: {tp2_rate:.1f}%")
    lines.append(f"🔄 TP1 → TP2 Rate: {tp1_to_tp2_rate:.1f}%")
    lines.append(f"🛑 SL Before TP1: {sl_before_tp1} ({sl_before_tp1_rate:.1f}%)")
    lines.append(f"🔄 TP1 Only / رجوع Entry: {tp1_only}")
    lines.append(f"⏳ Expired: {expired}")
    lines.append(f"🔍 Exit Quality: {quality_text}")
    lines.append("")

    if exit_quality == "exit_problem":
        lines.append("ℹ️ الإشارات بتلمس TP1 كويس، لكن نسبة الوصول لـ TP2 ضعيفة. راجع إدارة الخروج أو قرب TP2.")
    elif exit_quality == "entry_problem":
        lines.append("ℹ️ نسبة SL قبل TP1 عالية. راجع الفلاتر قبل الدخول والـ SL.")
    elif exit_quality == "weak_entries":
        lines.append("ℹ️ TP1 Rate ضعيف. المشكلة غالبًا من جودة الدخول نفسها.")
    elif exit_quality == "good":
        lines.append("ℹ️ الخروج متوازن نسبيًا.")

    return "\n".join(lines)


# ------------------------------------------------------------
# DAILY PERFORMANCE
# ------------------------------------------------------------
def get_daily_performance_summary(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    days: int = 7,
    use_history: bool = False,
) -> List[dict]:
    market_type = normalize_market_type(market_type)
    side = normalize_side(side)

    logger.info(f"get_daily_performance_summary {market_type}/{side} days={days} history={use_history}")

    trades = get_all_trades_data(
        redis_client,
        market_type=market_type,
        side=side,
        use_history=use_history,
    )

    daily = summarize_trades_by_day(trades, days=days)
    rows = []

    if isinstance(daily, dict):
        for day_key, day_trades in daily.items():
            if not day_trades:
                continue

            summary = summarize_trades(day_trades)
            exit_data = summarize_exits(day_trades)

            rows.append({
                "day": day_key,
                "summary": summary,
                "exit_summary": exit_data,
            })

    elif isinstance(daily, list):
        for row in daily:
            if not isinstance(row, dict):
                continue

            day = row.get("day")
            summary = row.get("summary") or _empty_summary()
            exit_summary = row.get("exit_summary") or {}

            if not exit_summary:
                day_trades = row.get("trades")
                if isinstance(day_trades, list):
                    exit_summary = summarize_exits(day_trades)
                else:
                    exit_summary = summarize_exits([])

            rows.append({
                "day": day,
                "summary": summary,
                "exit_summary": exit_summary,
            })

    else:
        return []

    rows.sort(key=lambda r: str(r.get("day", "")))
    return rows


def format_daily_performance_report(title: str, rows: list, side: str = "long") -> str:
    side = normalize_side(side)

    if not rows:
        return "لا توجد بيانات كافية"

    rows = rows[-30:]

    lines = [f"<b>{title}</b>", ""]
    for row in rows:
        day = row.get("day", "")
        summary = row.get("summary", {}) or {}
        exits = row.get("exit_summary", {}) or {}

        signals = safe_int(summary.get("signals", 0))
        closed = safe_int(summary.get("closed", 0))
        open_t = safe_int(summary.get("open", 0))
        wins = safe_int(summary.get("wins", 0))
        losses = safe_int(summary.get("losses", 0))
        expired = safe_int(summary.get("expired", 0))
        winrate = safe_float(summary.get("winrate", 0))
        tp1_rate = safe_float(summary.get("tp1_rate", 0))
        tp2_rate = safe_float(summary.get("tp2_rate", 0))
        net_pnl = safe_float(summary.get("realized_leveraged_pnl_pct", summary.get("realized_pnl_pct", 0)))

        wallet_pct, wallet_usd = estimate_wallet_pnl(summary, side=side)

        quality = exits.get("exit_quality", "")
        quality_short = {
            "good": "✅",
            "exit_problem": "⚠️",
            "entry_problem": "🔴",
            "weak_entries": "🔴",
            "mixed": "🟡",
            "unknown": "—",
        }.get(quality, "—")

        lines.append(
            f"📅 {day} | 🎯{signals}/{closed}/{open_t} | "
            f"✅{wins} ❌{losses} ⏳{expired} | WR:{winrate:.0f}% | "
            f"TP1:{tp1_rate:.0f}% TP2:{tp2_rate:.0f}% | "
            f"Net:{net_pnl:+.1f}% | محفظة:{wallet_pct:+.1f}% {wallet_usd:+.1f}$ | "
            f"خروج:{quality_short}"
        )

    return "\n".join(lines)


# ------------------------------------------------------------
# TODAY PERFORMANCE
# ------------------------------------------------------------
def get_today_performance_summary(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    use_history: bool = False,
) -> dict:
    market_type = normalize_market_type(market_type)
    side = normalize_side(side)

    logger.info(f"get_today_performance_summary {market_type}/{side} history={use_history}")

    trades = get_all_trades_data(
        redis_client,
        market_type=market_type,
        side=side,
        use_history=use_history,
    )

    today_data = summarize_today(trades) or {}

    return {
        "day": today_data.get("day", ""),
        "summary": today_data.get("summary", _empty_summary()),
        "exit_summary": today_data.get("exit_summary", {}),
        "market_type": market_type,
        "side": side,
    }


def format_today_performance_report(title: str, data: dict, side: str = "long") -> str:
    side = normalize_side(side)

    summary = data.get("summary", {}) or {}
    exit_summary = data.get("exit_summary", {}) or {}
    day = data.get("day", "")

    if not summary or safe_int(summary.get("signals", 0)) == 0:
        return f"<b>{title}</b>\nلا توجد صفقات اليوم."

    wallet_pct, wallet_usd = estimate_wallet_pnl(summary, side=side)

    lines = [f"<b>{title}</b>", f"📅 {day}", ""]

    signals = safe_int(summary.get("signals", 0))
    closed = safe_int(summary.get("closed", 0))
    open_t = safe_int(summary.get("open", 0))
    wins = safe_int(summary.get("wins", 0))
    losses = safe_int(summary.get("losses", 0))
    expired = safe_int(summary.get("expired", 0))

    lines.append(f"إشارات: {signals} | مغلقة: {closed} | مفتوحة: {open_t}")
    lines.append(f"فوز: {wins} | خسارة: {losses} | منتهية: {expired}")
    lines.append(f"نسبة الفوز: {safe_float(summary.get('winrate', 0)):.1f}%")
    lines.append(
        f"TP1 Rate: {safe_float(summary.get('tp1_rate', 0)):.1f}% | "
        f"TP2 Rate: {safe_float(summary.get('tp2_rate', 0)):.1f}%"
    )

    net_pnl = safe_float(summary.get("realized_leveraged_pnl_pct", summary.get("realized_pnl_pct", 0)))
    lines.append(f"صافي بعد الرافعة: {net_pnl:+.2f}%")
    lines.append(f"تأثير المحفظة: {wallet_pct:+.2f}% = {wallet_usd:+.2f}$")

    if exit_summary:
        lines.append("")
        lines.append("<b>🎯 تحليل الخروج:</b>")
        quality = exit_summary.get("exit_quality", "unknown")
        quality_ar = {
            "good": "جيد ✅",
            "exit_problem": "مشكلة خروج ⚠️",
            "entry_problem": "مشكلة دخول 🔴",
            "weak_entries": "جودة دخول ضعيفة 🔴",
            "mixed": "مختلط 🟡",
            "unknown": "غير كافٍ",
        }.get(quality, quality)

        lines.append(f"الجودة: {quality_ar}")
        lines.append(
            f"TP1: {safe_int(exit_summary.get('tp1_hits', 0))} | "
            f"TP2: {safe_int(exit_summary.get('tp2_hits', 0))}"
        )
        lines.append(f"SL قبل TP1: {safe_int(exit_summary.get('sl_before_tp1', 0))}")

    return "\n".join(lines)


# ------------------------------------------------------------
# DATA EXTRACTION FOR DIAGNOSTICS
# ------------------------------------------------------------
def get_all_trades_data(
    redis_client,
    market_type: str = None,
    side: str = None,
    since_ts: int = None,
    use_history: bool = False,
):
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
                created_ts = get_trade_created_ts(trade)
                ts_for_filter = created_ts if created_ts else safe_int(trade.get("candle_time", 0))

                if market_type and trade_market != normalize_market_type(market_type):
                    continue
                if side and trade_side != normalize_side(side):
                    continue
                if since_ts is not None and ts_for_filter < since_ts:
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
            created_ts = get_trade_created_ts(trade)
            ts_for_filter = created_ts if created_ts else safe_int(trade.get("candle_time", 0))

            if market_type and trade_market != normalize_market_type(market_type):
                continue
            if side and trade_side != normalize_side(side):
                continue
            if since_ts is not None and ts_for_filter < since_ts:
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

        output.append({"field_value": value, **summary})

    output.sort(key=lambda x: (x["winrate"], x["closed"], x["wins"]), reverse=True)
    return output


def get_common_loss_reasons(
    redis_client=None,
    market_type: str = None,
    side: str = None,
    since_ts: int = None,
    top_n: int = 10,
    trades: Optional[List[dict]] = None,
):
    if trades is None:
        trades = load_trades(
            redis_client,
            market_type=market_type,
            side=side,
            since_ts=since_ts,
            include_open=False,
        )

    reasons_counter = Counter()

    for trade in (trades or []):
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


# ------------------------------------------------------------
# TEXT FORMATTERS
# ------------------------------------------------------------
def format_winrate_summary(summary: dict) -> str:
    market_type = summary.get("market_type")
    side = summary.get("side")
    prefix = f"[{market_type}/{side}] " if market_type and side else ""

    net_pnl = safe_float(
        summary.get(
            "net_profit_pct",
            summary.get("realized_leveraged_pnl_pct", summary.get("realized_pnl_pct", 0.0))
        ),
        0.0,
    )

    return (
        f"{prefix}Win rate: {summary.get('winrate', 0)}% | "
        f"TP1 Rate: {summary.get('tp1_rate', 0)}% | "
        f"TP2 Rate: {summary.get('tp2_rate', 0)}% | "
        f"TP1→TP2: {summary.get('tp1_to_tp2_rate', 0)}% | "
        f"Net PnL: {net_pnl:+.2f}% | "
        f"Wins: {summary.get('wins', 0)} | "
        f"TP1 Wins: {summary.get('tp1_wins', 0)} | "
        f"TP2 Wins: {summary.get('tp2_wins', 0)} | "
        f"Losses: {summary.get('losses', 0)} | "
        f"TP1 Hits: {summary.get('tp1_hits', 0)} | "
        f"Expired: {summary.get('expired', 0)} | "
        f"Open: {summary.get('open', 0)}"
    )


def format_period_summary(title: str, summary: dict) -> str:
    side = normalize_side(summary.get("side", "long"))

    wins = safe_int(summary.get("wins", 0))
    losses = safe_int(summary.get("losses", 0))
    expired = safe_int(summary.get("expired", 0))
    decided = wins + losses + expired

    gross_profit = safe_float(summary.get("gross_profit_pct", 0.0))
    gross_loss = safe_float(summary.get("gross_loss_pct", 0.0))
    net_pnl = safe_float(summary.get(
        "realized_leveraged_pnl_pct",
        summary.get("realized_pnl_pct", 0.0)
    ))
    raw_pnl = safe_float(summary.get("realized_raw_pnl_pct", 0.0))

    avg_win = safe_float(summary.get("avg_win_pct", 0.0))
    avg_loss = safe_float(summary.get("avg_loss_pct", 0.0))
    best = safe_float(summary.get("best_trade_pct", 0.0))
    worst = safe_float(summary.get("worst_trade_pct", 0.0))
    risk_status = summary.get("risk_status", "normal")

    tp1_hits = safe_int(summary.get("tp1_hits", 0))
    tp2_hits = safe_int(summary.get("tp2_hits", summary.get("tp2_wins", 0)))
    tp1_only = safe_int(summary.get("tp1_only", summary.get("tp1_wins", 0)))
    full_wins = safe_int(summary.get("tp2_wins", tp2_hits))
    tp1_rate = safe_float(summary.get("tp1_rate", 0))
    tp2_rate = safe_float(summary.get("tp2_rate", 0))
    tp1_to_tp2_rate = safe_float(summary.get("tp1_to_tp2_rate", 0))

    plan = get_report_sizing_plan(side)
    leverage = plan["leverage"]
    active_trade_slots = plan["active_trade_slots"]
    margin_per_trade = plan["margin_per_trade_usd"]

    gross_profit_usd = estimate_pct_to_usd(gross_profit, side=side)
    gross_loss_usd = estimate_pct_to_usd(gross_loss, side=side)
    net_pnl_usd = estimate_pct_to_usd(net_pnl, side=side)

    wallet_pnl_pct, wallet_pnl_usd = estimate_wallet_pnl(summary, side=side)

    if side == "short":
        settings_block = (
            f"\n⚙️ <b>إعدادات حساب الشورت:</b>\n"
            f"• عدد الصفقات القصوى: {active_trade_slots}\n"
            f"• مارجن الصفقة الواحدة: {margin_per_trade:.2f}$\n"
            f"• الرافعة: {leverage:.0f}x\n"
            f"• الحجم الاسمي للصفقة: {plan['notional_per_trade_usd']:.2f}$\n"
            f"• إجمالي المارجن عند الامتلاء: {plan['total_margin_used_usd']:.2f}$\n"
            f"• إجمالي التعرض الاسمي: {plan['total_notional_exposure_usd']:.2f}$\n"
            f"• ملاحظة: حساب الدولار تقديري على أساس مارجن {margin_per_trade:.2f}$ للصفقة.\n"
        )
    else:
        capital_used_usd = plan.get("capital_used_usd", 0.0)
        settings_block = (
            f"\n⚙️ <b>إعدادات الحساب:</b>\n"
            f"• الرافعة المستخدمة: {leverage:.0f}x\n"
            f"• طريقة الحساب: {active_trade_slots} صفقات مفتوحة كحد أقصى\n"
            f"• رأس المال المستخدم من المحفظة: {capital_used_usd:.2f}$ من أصل {REPORT_ACCOUNT_BALANCE_USD:.0f}$\n"
            f"• حجم المارجن للصفقة الواحدة تقديريًا: {margin_per_trade:.2f}$\n"
        )

    status_ar = {
        "normal": "آمن ✅",
        "warning": "تحذير ⚠️",
        "danger": "خطر 🔴",
    }
    status_text = status_ar.get(risk_status, risk_status)

    risk_lines = [
        f"• حد إيقاف/تحذير الخسارة: -{REPORT_DAILY_MAX_DRAWDOWN_PCT:.0f}% من إجمالي المحفظة",
        f"• الحالة: {status_text}",
    ]
    if side == "long":
        risk_lines.insert(0, f"• الحد الأقصى لاستخدام المحفظة: {REPORT_MAX_CAPITAL_USAGE_PCT:.0f}%")

    performance_block = (
        f"\n\n🎯 <b>أداء الأهداف:</b>\n"
        f"• TP1 Hits: {tp1_hits} | TP2 Hits: {tp2_hits}\n"
        f"• TP1 Only: {tp1_only} | Full Wins TP2: {full_wins}\n"
        f"• TP1 Rate: {tp1_rate:.1f}% | TP2 Rate: {tp2_rate:.1f}%\n"
        f"• TP1 → TP2 Rate: {tp1_to_tp2_rate:.1f}%"
    )

    financial_block = (
        f"\n\n💰 <b>ملخص الربح والخسارة بعد الرافعة</b>\n"
        f"━━━━━━━━━━━━━━\n"
        f"🟢 <b>إجمالي الربح:</b> {gross_profit:+.2f}% = {gross_profit_usd:+.2f}$\n"
        f"🔴 <b>إجمالي الخسارة:</b> {gross_loss:+.2f}% = {gross_loss_usd:+.2f}$\n"
        f"⚖️ <b>صافي الربح/الخسارة:</b> {net_pnl:+.2f}% = {net_pnl_usd:+.2f}$\n"
        f"💼 <b>التأثير الحقيقي على المحفظة:</b> {wallet_pnl_pct:+.2f}% = {wallet_pnl_usd:+.2f}$\n"
        f"━━━━━━━━━━━━━━\n"
        + settings_block +
        f"\n📊 <b>تفاصيل إضافية:</b>\n"
        f"• صافي حركة السعر بدون رافعة: {raw_pnl:+.2f}%\n"
        f"• متوسط الصفقة الرابحة: {avg_win:+.2f}%\n"
        f"• متوسط الصفقة الخاسرة: {avg_loss:+.2f}%\n"
        f"• أفضل صفقة: {best:+.2f}%\n"
        f"• أسوأ صفقة: {worst:+.2f}%\n"
        f"\n🧯 <b>إدارة المخاطرة:</b>\n"
        + "\n".join(risk_lines) + "\n"
    )

    return (
        f"📊 {title}\n"
        f"Signals: {summary.get('signals', 0)}\n"
        f"Closed: {decided}\n"
        f"Wins (TP1+): {wins}\n"
        f"• Full Wins (TP2): {full_wins}\n"
        f"• TP1 Only: {tp1_only}\n"
        f"Losses: {losses}\n"
        f"Expired: {expired}\n"
        f"Open: {summary.get('open', 0)}\n"
        f"Win rate: {summary.get('winrate', 0)}%"
        + performance_block
        + financial_block
    )


# ------------------------------------------------------------
# POSITION SIZING & WALLET ESTIMATION
# ------------------------------------------------------------
def get_report_sizing_plan(side: str = "long") -> dict:
    side = normalize_side(side)

    if side == "short":
        return {
            "account_balance_usd": REPORT_ACCOUNT_BALANCE_USD,
            "margin_per_trade_usd": SHORT_REPORT_MARGIN_PER_TRADE_USD,
            "active_trade_slots": SHORT_REPORT_ACTIVE_TRADE_SLOTS,
            "leverage": SHORT_REPORT_LEVERAGE,
            "total_margin_used_usd": SHORT_REPORT_TOTAL_MARGIN_USD,
            "notional_per_trade_usd": SHORT_REPORT_NOTIONAL_PER_TRADE_USD,
            "total_notional_exposure_usd": SHORT_REPORT_NOTIONAL_PER_TRADE_USD * SHORT_REPORT_ACTIVE_TRADE_SLOTS,
        }

    capital_used_usd = REPORT_ACCOUNT_BALANCE_USD * (REPORT_MAX_CAPITAL_USAGE_PCT / 100.0)
    margin_per_trade = capital_used_usd / REPORT_ACTIVE_TRADE_SLOTS if REPORT_ACTIVE_TRADE_SLOTS else 0.0

    return {
        "account_balance_usd": REPORT_ACCOUNT_BALANCE_USD,
        "max_capital_usage_pct": REPORT_MAX_CAPITAL_USAGE_PCT,
        "capital_used_usd": capital_used_usd,
        "margin_per_trade_usd": margin_per_trade,
        "active_trade_slots": REPORT_ACTIVE_TRADE_SLOTS,
        "leverage": REPORT_LEVERAGE,
        "total_margin_used_usd": capital_used_usd,
        "notional_per_trade_usd": margin_per_trade * REPORT_LEVERAGE,
        "total_notional_exposure_usd": capital_used_usd * REPORT_LEVERAGE,
    }


def get_position_sizing_plan(side: str = "long"):
    return get_report_sizing_plan(side=side)


def estimate_pct_to_usd(pct_value: float, side: str = "long") -> float:
    plan = get_report_sizing_plan(side)
    margin_per_trade = plan["margin_per_trade_usd"]
    return margin_per_trade * (safe_float(pct_value, 0.0) / 100.0)


def estimate_wallet_pnl(summary: dict, side: str = "long", max_capital_usage_pct: float = None) -> tuple:
    """
    حساب تأثير المحفظة باستخدام مارجن الصفقة الواحدة.
    تُرجع (نسبة المحفظة %, القيمة بالدولار).
    """
    side = normalize_side(side)

    realized_pct_sum = safe_float(
        summary.get("realized_leveraged_pnl_pct", summary.get("realized_pnl_pct", 0.0)),
        0.0,
    )

    if side == "short":
        margin_per_trade = SHORT_REPORT_MARGIN_PER_TRADE_USD
    else:
        plan = get_report_sizing_plan("long")
        margin_per_trade = plan["margin_per_trade_usd"]

    wallet_pnl_usd = margin_per_trade * (realized_pct_sum / 100.0)
    wallet_pnl_pct = (
        (wallet_pnl_usd / REPORT_ACCOUNT_BALANCE_USD) * 100.0
        if REPORT_ACCOUNT_BALANCE_USD > 0
        else 0.0
    )

    return wallet_pnl_pct, wallet_pnl_usd
