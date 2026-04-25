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
# إعدادات التقرير المالي (لا تتحكم في تنفيذ الصفقات)
# =========================
REPORT_ACCOUNT_BALANCE_USD = 1000.0
REPORT_MAX_CAPITAL_USAGE_PCT = 35.0
REPORT_DAILY_MAX_DRAWDOWN_PCT = 20.0
REPORT_ACTIVE_TRADE_SLOTS = 10          # عدد الصفقات المفتوحة المُفترض وجودها في نفس الوقت
REPORT_LEVERAGE = 15.0                  # الرافعة المالية المستخدمة في التقارير فقط


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


def normalize_text(value, default="unknown") -> str:
    text = str(value or "").strip()
    return text if text else default


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


def recalc_targets_from_effective_entry(trade: dict, effective_entry: float) -> dict:
    """
    تعيد حساب TP1 و TP2 بناءً على effective_entry ونفس SL الأصلي.
    تستخدم rr1/rr2 المخزنين في diagnostics إن وجدوا.
    لو غير موجودين تستخدم الافتراضي القديم: TP1 = 1.5R و TP2 = 3R.
    """
    side = normalize_side(trade.get("side", "long"))
    diagnostics = trade.get("diagnostics", {}) or {}

    rr1 = safe_float(diagnostics.get("rr1"), 1.5)
    rr2 = safe_float(diagnostics.get("rr2"), 3.0)
    sl = safe_float(trade.get("sl"), 0.0)

    if effective_entry <= 0 or sl <= 0:
        return trade

    risk = abs(effective_entry - sl)
    if risk <= 0:
        return trade

    if side == "short":
        trade["tp1"] = round(effective_entry - (risk * rr1), 6)
        trade["tp2"] = round(effective_entry - (risk * rr2), 6)
    else:
        trade["tp1"] = round(effective_entry + (risk * rr1), 6)
        trade["tp2"] = round(effective_entry + (risk * rr2), 6)

    diagnostics["rr1"] = rr1
    diagnostics["rr2"] = rr2
    trade["diagnostics"] = diagnostics

    return trade


# =========================
# دوال النسب المئوية الحقيقية للصفقة (بدون رافعة)
# =========================
def calc_long_pct(entry: float, exit_price: float) -> float:
    """نسبة حركة اللونج بدون رافعة"""
    if entry <= 0:
        return 0.0
    return ((exit_price - entry) / entry) * 100


def calc_short_pct(entry: float, exit_price: float) -> float:
    """نسبة حركة الشورت بدون رافعة"""
    if entry <= 0:
        return 0.0
    return ((entry - exit_price) / entry) * 100


def calc_trade_result_pct(trade: dict) -> float | None:
    """
    تحسب النسبة المئوية الإجمالية لنتيجة الصفقة بعد الأخذ في الاعتبار
    الإغلاق الجزئي 50% عند TP1 وتحريك الوقف للدخول.
    """
    direction = trade.get("direction", "long") if "direction" in trade else trade.get("side", "long")
    direction = normalize_side(direction)

    diagnostics = trade.get("diagnostics", {}) or {}

    # إذا كانت الصفقة تعتمد على بول باك ولم يتم تفعيله بعد، لا تحسب نتيجة مالية
    if diagnostics.get("pullback_entry") is not None and not normalize_bool(diagnostics.get("pullback_triggered", False)):
        return None

    # استخدام سعر الدخول الفعلي (effective_entry) إذا وُجد
    entry = safe_float(diagnostics.get("effective_entry"), safe_float(trade.get("entry"), 0.0))

    sl = safe_float(trade.get("sl"), 0.0)
    tp1 = safe_float(trade.get("tp1"), 0.0)
    tp2 = safe_float(trade.get("tp2"), 0.0)
    tp1_hit = normalize_bool(trade.get("tp1_hit", False))
    result = str(trade.get("result", "") or "").lower().strip()

    if entry <= 0 or result in ("open", "partial", "breakeven", ""):
        return None

    if result == "expired":
        # إذا ضرب TP1 قبل expiry نعاملها كـ tp1_win (نصف ربح)
        if tp1_hit and tp1 > 0:
            if direction == "long":
                return 0.5 * calc_long_pct(entry, tp1)
            return 0.5 * calc_short_pct(entry, tp1)
        return 0.0

    if result == "loss":
        # خسارة كاملة على SL
        if direction == "long":
            return calc_long_pct(entry, sl)
        return calc_short_pct(entry, sl)

    if result == "tp1_win":
        # ضرب TP1 ثم ارتداد إلى entry → نصف الصفقة ربح TP1 والنصف الآخر تعادل
        if tp1_hit and tp1 > 0:
            if direction == "long":
                return 0.5 * calc_long_pct(entry, tp1)
            return 0.5 * calc_short_pct(entry, tp1)
        return 0.0

    if result == "tp2_win":
        if tp1_hit and tp1 > 0:
            # نصف عند TP1 + نصف عند TP2
            if direction == "long":
                return 0.5 * calc_long_pct(entry, tp1) + 0.5 * calc_long_pct(entry, tp2)
            return 0.5 * calc_short_pct(entry, tp1) + 0.5 * calc_short_pct(entry, tp2)
        else:
            # ضرب TP2 مباشرة
            if direction == "long":
                return calc_long_pct(entry, tp2)
            return calc_short_pct(entry, tp2)

    return None


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

        # pullback fields
        "pullback_entry": (
            round(float(extra_fields.get("pullback_entry")), 6)
            if extra_fields.get("pullback_entry") is not None
            else None
        ),
        "pullback_low": (
            round(float(extra_fields.get("pullback_low")), 6)
            if extra_fields.get("pullback_low") is not None
            else None
        ),
        "pullback_high": (
            round(float(extra_fields.get("pullback_high")), 6)
            if extra_fields.get("pullback_high") is not None
            else None
        ),
        "pullback_triggered": normalize_bool(extra_fields.get("pullback_triggered", False)),
        "effective_entry": (
            round(float(extra_fields.get("effective_entry")), 6)
            if extra_fields.get("effective_entry") is not None
            else None
        ),

        # rr1 / rr2 للتوافق المستقبلي
        "rr1": safe_float(extra_fields.get("rr1"), 1.5),
        "rr2": safe_float(extra_fields.get("rr2"), 3.0),
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

        # pullback snapshot
        "pullback_entry": diagnostics.get("pullback_entry"),
        "pullback_low": diagnostics.get("pullback_low"),
        "pullback_high": diagnostics.get("pullback_high"),
        "pullback_triggered": diagnostics.get("pullback_triggered", False),
        "effective_entry": diagnostics.get("effective_entry"),

        # rr snapshot
        "rr1": safe_float(diagnostics.get("rr1"), 1.5),
        "rr2": safe_float(diagnostics.get("rr2"), 3.0),

        # full diagnostics snapshot for history / Hybrid Label / future analytics
        "diagnostics": diagnostics,
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
    risk_level: str = None,
    fake_signal: bool = False,
    is_reverse_signal: bool = False,
    reversal_4h_confirmed: bool = False,
    rank_volume_24h: float = None,
    alert_id: str = None,
    has_high_impact_news: bool = False,
    news_titles=None,
    warning_reasons=None,

    # === pullback fields ===
    pullback_entry: float = None,
    pullback_low: float = None,
    pullback_high: float = None,

    # === RR fields (اختياري للتوافق المستقبلي) ===
    rr1: float = 1.5,
    rr2: float = 3.0,

    **kwargs,
):
    if redis_client is None:
        return False

    market_type = normalize_market_type(market_type)
    side = normalize_side(side)

    entry = round(float(entry), 6)
    sl = round(float(sl), 6)
    tp1 = round(float(tp1), 6) if tp1 is not None else calc_tp1(entry, sl, side=side)
    tp2 = round(float(tp2), 6) if tp2 is not None else calc_tp2(entry, sl, side=side)

    pullback_entry = round(float(pullback_entry), 6) if pullback_entry is not None else None
    pullback_low = round(float(pullback_low), 6) if pullback_low is not None else None
    pullback_high = round(float(pullback_high), 6) if pullback_high is not None else None

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
        "effective_entry": entry,  # يبدأ بسعر الإشارة الأصلي، لا يفترض دخول البول باك
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

        diagnostics = trade_data.get("diagnostics", {}) or {}
        effective_entry = safe_float(diagnostics.get("effective_entry"), safe_float(trade_data.get("entry"), 0.0))

        trade_data["tp1_hit"] = True
        trade_data["tp1_hit_at"] = int(time.time())
        trade_data["status"] = "partial"
        trade_data["sl"] = round(effective_entry, 6) if effective_entry > 0 else trade_data["entry"]
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

    # pullback logic - للـ Long فقط حاليًا
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

        # لا نعتبر الصفقة دخلت إلا لو السعر وصل لسعر الدخول الفعلي للبول باك
        if not (pb_entry > 0 and low <= pb_entry):
            return None, False, trade

        # السعر لمس سعر الدخول الفعلي للبول باك → تفعيل الدخول وإعادة حساب الأهداف
        diagnostics["pullback_triggered"] = True
        diagnostics["effective_entry"] = pb_entry
        trade["diagnostics"] = diagnostics

        # إعادة حساب TP1 / TP2 من effective_entry الجديد (باستخدام rr1/rr2 المخزنة)
        trade = recalc_targets_from_effective_entry(trade, pb_entry)
        effective_entry = pb_entry
        tp1 = safe_float(trade["tp1"])
        tp2 = safe_float(trade["tp2"])

    # تقييم النتيجة بناءً على السعر الفعلي
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
                # حفظ أي تغيير في حالة البول باك أو غيره
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

        # === حقول مالية - خام بدون رافعة ===
        "realized_raw_pnl_pct": 0.0,
        "realized_leveraged_pnl_pct": 0.0,
        "gross_profit_raw_pct": 0.0,
        "gross_loss_raw_pct": 0.0,
        "gross_profit_leveraged_pct": 0.0,
        "gross_loss_leveraged_pct": 0.0,

        # === حقول مالية - متوافقة مع الإصدارات السابقة (تستخدم المرفوعة) ===
        "realized_pnl_pct": 0.0,
        "gross_profit_pct": 0.0,
        "gross_loss_pct": 0.0,
        "avg_win_pct": 0.0,
        "avg_loss_pct": 0.0,
        "best_trade_pct": 0.0,
        "worst_trade_pct": 0.0,
        "max_capital_usage_pct": REPORT_MAX_CAPITAL_USAGE_PCT,
        "risk_status": "normal",
        "leverage": REPORT_LEVERAGE,
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

    # === حساب النسبة المئوية للصفقة (خام و مرفوعة) ===
    raw_trade_pct = calc_trade_result_pct(trade)
    if raw_trade_pct is not None and result in ("tp1_win", "tp2_win", "loss", "expired"):
        leveraged_trade_pct = raw_trade_pct * REPORT_LEVERAGE

        # تحديث الخام
        summary["realized_raw_pnl_pct"] += raw_trade_pct
        if raw_trade_pct > 0:
            summary["gross_profit_raw_pct"] += raw_trade_pct
        elif raw_trade_pct < 0:
            summary["gross_loss_raw_pct"] += raw_trade_pct

        # تحديث المرفوع
        summary["realized_leveraged_pnl_pct"] += leveraged_trade_pct
        if leveraged_trade_pct > 0:
            summary["gross_profit_leveraged_pct"] += leveraged_trade_pct
        elif leveraged_trade_pct < 0:
            summary["gross_loss_leveraged_pct"] += leveraged_trade_pct

        # الحقول القديمة (للتوافق) نستخدم المرفوعة
        summary["realized_pnl_pct"] += leveraged_trade_pct
        if leveraged_trade_pct > 0:
            summary["gross_profit_pct"] += leveraged_trade_pct
            if leveraged_trade_pct > summary["best_trade_pct"]:
                summary["best_trade_pct"] = leveraged_trade_pct
        elif leveraged_trade_pct < 0:
            summary["gross_loss_pct"] += leveraged_trade_pct
            if leveraged_trade_pct < summary["worst_trade_pct"]:
                summary["worst_trade_pct"] = leveraged_trade_pct


def _finalize_summary(summary: dict):
    decided = summary["wins"] + summary["losses"]
    base_for_tp1 = summary["total"]

    summary["winrate"] = round((summary["wins"] / decided) * 100, 2) if decided > 0 else 0.0
    summary["tp1_rate"] = round((summary["tp1_hits"] / base_for_tp1) * 100, 2) if base_for_tp1 > 0 else 0.0

    # === متوسطات النسب المئوية (باستخدام المرفوعة) ===
    wins_count = summary["wins"]
    losses_count = summary["losses"]
    if wins_count > 0:
        summary["avg_win_pct"] = summary["gross_profit_pct"] / wins_count
    if losses_count > 0:
        summary["avg_loss_pct"] = summary["gross_loss_pct"] / losses_count

    # === حالة المخاطرة: تُحسب على تأثير المحفظة الفعلي باستخدام المرفوعة ===
    wallet_pnl_pct, wallet_pnl_usd = estimate_wallet_pnl(summary)

    if wallet_pnl_pct <= -REPORT_DAILY_MAX_DRAWDOWN_PCT:
        summary["risk_status"] = "danger"
    elif wallet_pnl_pct <= -(REPORT_DAILY_MAX_DRAWDOWN_PCT / 2):
        summary["risk_status"] = "warning"
    else:
        summary["risk_status"] = "normal"

    return summary


# =========================
# دوال حساب حجم الصفقة والأرباح الجديدة
# =========================
def get_position_sizing_plan():
    capital_used_usd = REPORT_ACCOUNT_BALANCE_USD * (REPORT_MAX_CAPITAL_USAGE_PCT / 100.0)
    per_trade_usd = capital_used_usd / REPORT_ACTIVE_TRADE_SLOTS if REPORT_ACTIVE_TRADE_SLOTS > 0 else 0.0

    return {
        "account_balance_usd": REPORT_ACCOUNT_BALANCE_USD,
        "max_capital_usage_pct": REPORT_MAX_CAPITAL_USAGE_PCT,
        "capital_used_usd": capital_used_usd,
        "active_trade_slots": REPORT_ACTIVE_TRADE_SLOTS,
        "per_trade_usd": per_trade_usd,
        "leverage": REPORT_LEVERAGE,
    }


def estimate_pct_to_usd(pct_value: float) -> float:
    """
    يحول نسبة أداء مرفوعة إلى دولار بناءً على حجم المارجن للصفقة الواحدة.
    """
    sizing = get_position_sizing_plan()
    per_trade_usd = float(sizing.get("per_trade_usd", 0.0) or 0.0)
    return per_trade_usd * (float(pct_value or 0.0) / 100.0)


def estimate_wallet_pnl(summary: dict, max_capital_usage_pct: float = None) -> tuple:
    """
    تقدير تأثير الصفقات المغلقة على المحفظة بنظام 10 صفقات مفتوحة كحد أقصى.
    تستخدم realized_leveraged_pnl_pct (إن وجد) وإلا ترجع لـ realized_pnl_pct.
    """
    if max_capital_usage_pct is None:
        max_capital_usage_pct = REPORT_MAX_CAPITAL_USAGE_PCT

    active_slots = max(1, int(REPORT_ACTIVE_TRADE_SLOTS))
    account_balance = float(REPORT_ACCOUNT_BALANCE_USD)

    capital_used_usd = account_balance * (float(max_capital_usage_pct) / 100.0)
    per_trade_usd = capital_used_usd / active_slots

    # يفضل استخدام المرفوع إن وجد
    realized_pct_sum = float(
        summary.get("realized_leveraged_pnl_pct",
                    summary.get("realized_pnl_pct", 0.0)
                   ) or 0.0
    )

    # الربح بالدولار = مجموع نسب الصفقات المغلقة (مرفوعة) × حجم الصفقة الواحدة
    wallet_pnl_usd = per_trade_usd * (realized_pct_sum / 100.0)

    # تأثير الربح على إجمالي المحفظة
    wallet_pnl_pct = (wallet_pnl_usd / account_balance) * 100.0 if account_balance > 0 else 0.0

    return wallet_pnl_pct, wallet_pnl_usd


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
            "leverage": REPORT_LEVERAGE,
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

        # نستخدم get_trade_summary للحصول على الأرقام المالية المحدثة
        financial_summary = get_trade_summary(redis_client, market_type=market_type, side=side)

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
            "leverage": REPORT_LEVERAGE,
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
            "leverage": REPORT_LEVERAGE,
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
        }


def get_trade_summary(
    redis_client,
    market_type: str = None,
    side: str = None,
    since_ts: int = None,
):
    if redis_client is None:
        return _empty_summary()

    # === تنظيف الفهرس قبل القراءة ===
    cleanup_missing_trades_from_index(redis_client)

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

    # استخراج القيم المالية المرفوعة
    gross_profit = summary.get("gross_profit_pct", 0.0)
    gross_loss = summary.get("gross_loss_pct", 0.0)
    net_pnl = summary.get("realized_leveraged_pnl_pct", summary.get("realized_pnl_pct", 0.0))
    wallet_pnl_pct, wallet_pnl_usd = estimate_wallet_pnl(summary)

    # تحويل النسب المرفوعة إلى دولار
    gross_profit_usd = estimate_pct_to_usd(gross_profit)
    gross_loss_usd = estimate_pct_to_usd(gross_loss)
    net_pnl_usd = estimate_pct_to_usd(net_pnl)

    # قيم إضافية
    raw_pnl = summary.get("realized_raw_pnl_pct", 0.0)
    avg_win = summary.get("avg_win_pct", 0.0)
    avg_loss = summary.get("avg_loss_pct", 0.0)
    best = summary.get("best_trade_pct", 0.0)
    worst = summary.get("worst_trade_pct", 0.0)
    risk_status = summary.get("risk_status", "normal")
    leverage = summary.get("leverage", REPORT_LEVERAGE)

    # بيانات حجم الصفقات
    sizing = get_position_sizing_plan()
    capital_used_usd = sizing["capital_used_usd"]
    per_trade_usd = sizing["per_trade_usd"]
    active_trade_slots = sizing["active_trade_slots"]

    status_ar = {"normal": "آمن ✅", "warning": "تحذير ⚠️", "danger": "خطر 🔴"}
    status_text = status_ar.get(risk_status, risk_status)

    # بناء الكتلة المالية بتصميم مميز
    financial_block = (
        f"\n\n💰 <b>ملخص الربح والخسارة بعد الرافعة</b>\n"
        f"━━━━━━━━━━━━━━\n"
        f"🟢 <b>إجمالي الربح:</b> {gross_profit:+.2f}% = {gross_profit_usd:+.2f}$\n"
        f"🔴 <b>إجمالي الخسارة:</b> {gross_loss:+.2f}% = {gross_loss_usd:+.2f}$\n"
        f"⚖️ <b>صافي الربح/الخسارة:</b> {net_pnl:+.2f}% = {net_pnl_usd:+.2f}$\n"
        f"💼 <b>التأثير الحقيقي على المحفظة:</b> {wallet_pnl_pct:+.2f}% = {wallet_pnl_usd:+.2f}$\n"
        f"━━━━━━━━━━━━━━\n"
        f"\n⚙️ <b>إعدادات الحساب:</b>\n"
        f"• الرافعة المستخدمة: {leverage:.0f}x\n"
        f"• طريقة الحساب: {active_trade_slots} صفقات مفتوحة كحد أقصى\n"
        f"• رأس المال المستخدم من المحفظة: {capital_used_usd:.2f}$ من أصل {REPORT_ACCOUNT_BALANCE_USD:.0f}$\n"
        f"• حجم المارجن للصفقة الواحدة تقديريًا: {per_trade_usd:.2f}$\n"
        f"\n📊 <b>تفاصيل إضافية:</b>\n"
        f"• صافي حركة السعر بدون رافعة: {raw_pnl:+.2f}%\n"
        f"• متوسط الصفقة الرابحة: {avg_win:+.2f}%\n"
        f"• متوسط الصفقة الخاسرة: {avg_loss:+.2f}%\n"
        f"• أفضل صفقة: {best:+.2f}%\n"
        f"• أسوأ صفقة: {worst:+.2f}%\n"
        f"\n🧯 <b>إدارة المخاطرة:</b>\n"
        f"• الحد الأقصى لاستخدام المحفظة: {REPORT_MAX_CAPITAL_USAGE_PCT:.0f}%\n"
        f"• حد إيقاف/تحذير الخسارة: -{REPORT_DAILY_MAX_DRAWDOWN_PCT:.0f}% من إجمالي المحفظة\n"
        f"• الحالة: {status_text}\n"
    )

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
        + financial_block
    )
