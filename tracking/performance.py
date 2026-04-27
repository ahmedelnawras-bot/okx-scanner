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
- تم إصلاح دالة normalize_market_type لتدعم "swap" كمرادف لـ "futures".
- estimate_wallet_pnl تستخدم margin_per_trade كأساس للحساب (وليس per_trade_usd القديم).
- إضافة دالة diagnose_performance_problem لتشخيص مشاكل الأداء.
- إصلاح تشخيص exit_summary وعرض المشكلة الأساسية في جميع التقارير.
- دعم الحقول الجديدة للتخزين وحماية نقطة التعادل.
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
    if market_type in ("futures", "swap"):
        return "futures"
    if market_type == "spot":
        return "spot"
    return "futures"


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

        # الحقول الجديدة
        "setup_type_base": normalize_text(extra_fields.get("setup_type_base"), ""),
        "final_threshold": safe_float(extra_fields.get("final_threshold"), 0.0),
        "adjustments_log": normalize_list(extra_fields.get("adjustments_log")),
        "warning_penalty": safe_float(extra_fields.get("warning_penalty"), 0.0),
        "warning_penalty_details": normalize_list(extra_fields.get("warning_penalty_details")),
        "warning_high_count": safe_int(extra_fields.get("warning_high_count"), 0),
        "warning_medium_count": safe_int(extra_fields.get("warning_medium_count"), 0),
        "tp1_close_pct": safe_float(extra_fields.get("tp1_close_pct"), 50.0),
        "tp2_close_pct": safe_float(extra_fields.get("tp2_close_pct"), 50.0),
        "move_sl_to_entry_after_tp1": normalize_bool(extra_fields.get("move_sl_to_entry_after_tp1", True)),
        "fib_position": normalize_text(extra_fields.get("fib_position"), "unknown"),
        "fib_position_ratio": safe_float(extra_fields.get("fib_position_ratio")),
        "fib_label": normalize_text(extra_fields.get("fib_label"), ""),
        "had_pullback": normalize_bool(extra_fields.get("had_pullback", False)),
        "pullback_pct": safe_float(extra_fields.get("pullback_pct")),
        "pullback_label": normalize_text(extra_fields.get("pullback_label"), ""),
        "wave_estimate": safe_int(extra_fields.get("wave_estimate"), 0),
        "wave_peaks": safe_int(extra_fields.get("wave_peaks"), 0),
        "wave_label": normalize_text(extra_fields.get("wave_label"), ""),
        "entry_maturity": normalize_text(extra_fields.get("entry_maturity"), "unknown"),
        "maturity_penalty": safe_float(extra_fields.get("maturity_penalty"), 0.0),
        "maturity_bonus": safe_float(extra_fields.get("maturity_bonus"), 0.0),
        "falling_knife_risk": normalize_bool(extra_fields.get("falling_knife_risk", False)),
        "falling_knife_reasons": normalize_list(extra_fields.get("falling_knife_reasons")),
        "target_method": normalize_text(extra_fields.get("target_method"), "unknown"),
        "nearest_resistance": round_price(extra_fields.get("nearest_resistance")) if extra_fields.get("nearest_resistance") is not None else None,
        "nearest_support": round_price(extra_fields.get("nearest_support")) if extra_fields.get("nearest_support") is not None else None,
        "resistance_warning": normalize_text(extra_fields.get("resistance_warning"), ""),
        "support_warning": normalize_text(extra_fields.get("support_warning"), ""),
        "target_notes": normalize_list(extra_fields.get("target_notes")),
        "sl_method": normalize_text(extra_fields.get("sl_method"), "unknown"),
        "sl_notes": normalize_text(extra_fields.get("sl_notes"), ""),
        "wave_context": normalize_text(extra_fields.get("wave_context"), ""),
        "setup_context": normalize_text(extra_fields.get("setup_context"), ""),
        "reversal_quality": normalize_text(extra_fields.get("reversal_quality"), ""),
        "reversal_structure_confirmed": normalize_bool(extra_fields.get("reversal_structure_confirmed", False)),
        "strong_bull_pullback": normalize_bool(extra_fields.get("strong_bull_pullback", False)),
        "strong_breakout_exception": normalize_bool(extra_fields.get("strong_breakout_exception", False)),
        "protected_breakeven": normalize_bool(extra_fields.get("protected_breakeven", False)),
        "breakeven_protection_reason": normalize_text(extra_fields.get("breakeven_protection_reason"), ""),
        "breakeven_protected_ts": (
            safe_int(extra_fields.get("breakeven_protected_ts"), 0)
            if extra_fields.get("breakeven_protected_ts") is not None else None
        ),
        "original_sl_before_breakeven": round_price(extra_fields.get("original_sl_before_breakeven")) if extra_fields.get("original_sl_before_breakeven") is not None else None,
        "protected_breakeven_exit": normalize_bool(extra_fields.get("protected_breakeven_exit", False)),
        "sl_moved_to_entry": normalize_bool(extra_fields.get("sl_moved_to_entry", False)),
        "sl_move_reason": normalize_text(extra_fields.get("sl_move_reason"), ""),
    }

    return diagnostics


def build_history_snapshot(trade_data: dict) -> dict:
    diagnostics = trade_data.get("diagnostics", {}) or {}

    def get_field(name, default=None):
        val = trade_data.get(name)
        if val is None:
            val = diagnostics.get(name)
        return val if val is not None else default

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

        # الحقول الجديدة المهمة للتاريخ
        "setup_type_base": get_field("setup_type_base", ""),
        "final_threshold": safe_float(get_field("final_threshold", 0.0)),
        "target_method": get_field("target_method", "unknown"),
        "nearest_resistance": get_field("nearest_resistance"),
        "nearest_support": get_field("nearest_support"),
        "resistance_warning": get_field("resistance_warning", ""),
        "support_warning": get_field("support_warning", ""),
        "target_notes": normalize_list(get_field("target_notes", [])),
        "sl_method": get_field("sl_method", "unknown"),
        "sl_notes": get_field("sl_notes", ""),
        "falling_knife_risk": normalize_bool(get_field("falling_knife_risk", False)),
        "falling_knife_reasons": normalize_list(get_field("falling_knife_reasons", [])),
        "wave_context": get_field("wave_context", ""),
        "setup_context": get_field("setup_context", ""),
        "reversal_quality": get_field("reversal_quality", ""),
        "reversal_structure_confirmed": normalize_bool(get_field("reversal_structure_confirmed", False)),
        "strong_bull_pullback": normalize_bool(get_field("strong_bull_pullback", False)),
        "strong_breakout_exception": normalize_bool(get_field("strong_breakout_exception", False)),
        "tp1_close_pct": safe_float(get_field("tp1_close_pct", 50.0)),
        "tp2_close_pct": safe_float(get_field("tp2_close_pct", 50.0)),
        "move_sl_to_entry_after_tp1": normalize_bool(get_field("move_sl_to_entry_after_tp1", True)),
        "fib_position": get_field("fib_position", "unknown"),
        "fib_position_ratio": safe_float(get_field("fib_position_ratio")),
        "fib_label": get_field("fib_label", ""),
        "had_pullback": normalize_bool(get_field("had_pullback", False)),
        "pullback_pct": safe_float(get_field("pullback_pct")),
        "pullback_label": get_field("pullback_label", ""),
        "wave_estimate": safe_int(get_field("wave_estimate", 0)),
        "wave_peaks": safe_int(get_field("wave_peaks", 0)),
        "wave_label": get_field("wave_label", ""),
        "entry_maturity": get_field("entry_maturity", "unknown"),
        "maturity_penalty": safe_float(get_field("maturity_penalty", 0.0)),
        "maturity_bonus": safe_float(get_field("maturity_bonus", 0.0)),
        "warning_penalty": safe_float(get_field("warning_penalty", 0.0)),
        "warning_penalty_details": normalize_list(get_field("warning_penalty_details", [])),
        "adjustments_log": normalize_list(get_field("adjustments_log", [])),
        "protected_breakeven": normalize_bool(get_field("protected_breakeven", False)),
        "breakeven_protection_reason": get_field("breakeven_protection_reason", ""),
        "breakeven_protected_ts": (
            safe_int(get_field("breakeven_protected_ts"), 0)
            if get_field("breakeven_protected_ts") is not None else None
        ),
        "original_sl_before_breakeven": get_field("original_sl_before_breakeven"),
        "protected_breakeven_exit": normalize_bool(get_field("protected_breakeven_exit", False)),
        "sl_moved_to_entry": normalize_bool(get_field("sl_moved_to_entry", False)),
        "sl_move_reason": get_field("sl_move_reason", ""),
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
    # New fields (optional)
    final_threshold: float = None,
    target_method: str = None,
    nearest_resistance: float = None,
    nearest_support: float = None,
    resistance_warning: str = None,
    support_warning: str = None,
    target_notes: list = None,
    sl_method: str = None,
    sl_notes: str = None,
    falling_knife_risk: bool = False,
    falling_knife_reasons: list = None,
    reversal_quality: str = None,
    wave_context: str = None,
    setup_context: str = None,
    reversal_structure_confirmed: bool = False,
    warning_penalty: float = None,
    warning_high_count: int = None,
    warning_medium_count: int = None,
    warning_penalty_details: list = None,
    adjustments_log: list = None,
    protected_breakeven: bool = False,
    breakeven_protection_reason: str = None,
    breakeven_protected_ts: int = None,
    original_sl_before_breakeven: float = None,
    protected_breakeven_exit: bool = False,
    # More optional fields
    tp1_close_pct: float = None,
    tp2_close_pct: float = None,
    move_sl_to_entry_after_tp1: bool = None,
    setup_type_base: str = None,
    fib_position: str = None,
    fib_position_ratio: float = None,
    fib_label: str = None,
    had_pullback: bool = None,
    pullback_pct: float = None,
    pullback_label: str = None,
    wave_estimate: float = None,
    wave_peaks: float = None,
    wave_label: str = None,
    entry_maturity: str = None,
    maturity_penalty: float = None,
    maturity_bonus: float = None,
    strong_bull_pullback: bool = None,
    strong_breakout_exception: bool = None,
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

    # الحصول على الحقول الإضافية من kwargs إذا لم تُمرر في الـ signature
    def _get_kwarg(name, default=None):
        return kwargs.get(name, default)

    _tp1_close_pct = safe_float(tp1_close_pct) if tp1_close_pct is not None else safe_float(_get_kwarg("tp1_close_pct", 50.0))
    _tp2_close_pct = safe_float(tp2_close_pct) if tp2_close_pct is not None else safe_float(_get_kwarg("tp2_close_pct", 50.0))
    _move_sl_to_entry_after_tp1 = normalize_bool(move_sl_to_entry_after_tp1) if move_sl_to_entry_after_tp1 is not None else normalize_bool(_get_kwarg("move_sl_to_entry_after_tp1", True))
    _setup_type_base = setup_type_base or _get_kwarg("setup_type_base", "")
    _fib_position = fib_position or _get_kwarg("fib_position", "unknown")
    _fib_position_ratio = safe_float(fib_position_ratio) if fib_position_ratio is not None else safe_float(_get_kwarg("fib_position_ratio"))
    _fib_label = fib_label or _get_kwarg("fib_label", "")
    _had_pullback = normalize_bool(had_pullback) if had_pullback is not None else normalize_bool(_get_kwarg("had_pullback", False))
    _pullback_pct = safe_float(pullback_pct) if pullback_pct is not None else safe_float(_get_kwarg("pullback_pct"))
    _pullback_label = pullback_label or _get_kwarg("pullback_label", "")
    _wave_estimate = safe_int(wave_estimate) if wave_estimate is not None else safe_int(_get_kwarg("wave_estimate", 0))
    _wave_peaks = safe_int(wave_peaks) if wave_peaks is not None else safe_int(_get_kwarg("wave_peaks", 0))
    _wave_label = wave_label or _get_kwarg("wave_label", "")
    _entry_maturity = entry_maturity if entry_maturity is not None else _get_kwarg("entry_maturity", "unknown")
    _entry_maturity = normalize_text(_entry_maturity, "unknown")
    _maturity_penalty = safe_float(maturity_penalty) if maturity_penalty is not None else safe_float(_get_kwarg("maturity_penalty", 0.0))
    _maturity_bonus = safe_float(maturity_bonus) if maturity_bonus is not None else safe_float(_get_kwarg("maturity_bonus", 0.0))
    _strong_bull_pullback = normalize_bool(strong_bull_pullback) if strong_bull_pullback is not None else normalize_bool(_get_kwarg("strong_bull_pullback", False))
    _strong_breakout_exception = normalize_bool(strong_breakout_exception) if strong_breakout_exception is not None else normalize_bool(_get_kwarg("strong_breakout_exception", False))
    _falling_knife_reasons = normalize_list(falling_knife_reasons) if falling_knife_reasons is not None else normalize_list(_get_kwarg("falling_knife_reasons"))

    pre_signal = bool(pre_breakout)
    break_signal = bool(breakout)
    signal_event = "breakdown" if side == "short" else "breakout"

    docs = build_trade_diagnostics({
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
        # إضافة الحقول الجديدة
        "setup_type_base": _setup_type_base,
        "final_threshold": final_threshold,
        "adjustments_log": adjustments_log,
        "warning_penalty": warning_penalty,
        "warning_high_count": warning_high_count,
        "warning_medium_count": warning_medium_count,
        "warning_penalty_details": warning_penalty_details,
        "tp1_close_pct": _tp1_close_pct,
        "tp2_close_pct": _tp2_close_pct,
        "move_sl_to_entry_after_tp1": _move_sl_to_entry_after_tp1,
        "fib_position": _fib_position,
        "fib_position_ratio": _fib_position_ratio,
        "fib_label": _fib_label,
        "had_pullback": _had_pullback,
        "pullback_pct": _pullback_pct,
        "pullback_label": _pullback_label,
        "wave_estimate": _wave_estimate,
        "wave_peaks": _wave_peaks,
        "wave_label": _wave_label,
        "entry_maturity": _entry_maturity,
        "maturity_penalty": _maturity_penalty,
        "maturity_bonus": _maturity_bonus,
        "falling_knife_risk": falling_knife_risk,
        "falling_knife_reasons": _falling_knife_reasons,
        "target_method": target_method,
        "nearest_resistance": nearest_resistance,
        "nearest_support": nearest_support,
        "resistance_warning": resistance_warning,
        "support_warning": support_warning,
        "target_notes": target_notes,
        "sl_method": sl_method,
        "sl_notes": sl_notes,
        "wave_context": wave_context,
        "setup_context": setup_context,
        "reversal_quality": reversal_quality,
        "reversal_structure_confirmed": reversal_structure_confirmed,
        "strong_bull_pullback": _strong_bull_pullback,
        "strong_breakout_exception": _strong_breakout_exception,
        "protected_breakeven": protected_breakeven,
        "breakeven_protection_reason": breakeven_protection_reason,
        "breakeven_protected_ts": breakeven_protected_ts,
        "original_sl_before_breakeven": original_sl_before_breakeven,
        "protected_breakeven_exit": protected_breakeven_exit,
        "sl_moved_to_entry": False,
        "sl_move_reason": "",
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
        "diagnostics": docs,

        # الحقول الجديدة كـ top-level
        "setup_type_base": _setup_type_base or "",
        "final_threshold": safe_float(final_threshold) if final_threshold is not None else safe_float(score),
        "target_method": target_method or "unknown",
        "nearest_resistance": round_price(nearest_resistance) if nearest_resistance is not None else None,
        "nearest_support": round_price(nearest_support) if nearest_support is not None else None,
        "resistance_warning": resistance_warning or "",
        "support_warning": support_warning or "",
        "target_notes": normalize_list(target_notes),
        "sl_method": sl_method or "unknown",
        "sl_notes": sl_notes or "",
        "falling_knife_risk": normalize_bool(falling_knife_risk),
        "falling_knife_reasons": _falling_knife_reasons,
        "wave_context": wave_context or "",
        "setup_context": setup_context or "",
        "reversal_quality": reversal_quality or "",
        "reversal_structure_confirmed": bool(reversal_structure_confirmed),
        "strong_bull_pullback": _strong_bull_pullback,
        "strong_breakout_exception": _strong_breakout_exception,
        "tp1_close_pct": _tp1_close_pct,
        "tp2_close_pct": _tp2_close_pct,
        "move_sl_to_entry_after_tp1": _move_sl_to_entry_after_tp1,
        "protected_breakeven": bool(protected_breakeven),
        "breakeven_protection_reason": breakeven_protection_reason or "",
        "breakeven_protected_ts": breakeven_protected_ts or None,
        "original_sl_before_breakeven": round_price(original_sl_before_breakeven) if original_sl_before_breakeven is not None else None,
        "protected_breakeven_exit": bool(protected_breakeven_exit),
        "sl_moved_to_entry": False,
        "sl_move_reason": "",
        # حقول إضافية اختيارية
        "fib_position": _fib_position,
        "fib_position_ratio": _fib_position_ratio,
        "fib_label": _fib_label,
        "had_pullback": _had_pullback,
        "pullback_pct": _pullback_pct,
        "pullback_label": _pullback_label,
        "wave_estimate": _wave_estimate,
        "wave_peaks": _wave_peaks,
        "wave_label": _wave_label,
        "entry_maturity": _entry_maturity,
        "maturity_penalty": _maturity_penalty,
        "maturity_bonus": _maturity_bonus,
        "adjustments_log": normalize_list(adjustments_log),
        "warning_penalty": safe_float(warning_penalty) if warning_penalty is not None else 0.0,
        "warning_high_count": safe_int(warning_high_count, 0) if warning_high_count is not None else 0,
        "warning_medium_count": safe_int(warning_medium_count, 0) if warning_medium_count is not None else 0,
        "warning_penalty_details": normalize_list(warning_penalty_details),
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

        if result == "breakeven":
            trade_data["protected_breakeven_exit"] = True
            if not trade_data.get("exit_reason"):
                trade_data["exit_reason"] = "breakeven_exit"

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
        elif result == "breakeven":
            redis_client.hincrby(stats_key, "breakeven_exits", 1)

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

        # استخدام tp1_close_pct من trade_data أو diagnostics، الافتراضي 50
        tp1_close_pct = safe_float(
            trade_data.get("tp1_close_pct", diagnostics.get("tp1_close_pct", 50.0)),
            50.0
        )
        remaining_pct = max(0.0, 100.0 - tp1_close_pct)

        trade_data["tp1_hit"] = True
        trade_data["tp1_hit_at"] = int(time.time())
        trade_data["status"] = "partial"
        trade_data["partial_close_pct"] = tp1_close_pct
        trade_data["remaining_position_pct"] = remaining_pct
        trade_data["updated_at"] = int(time.time())

        # هل ننقل SL لنقطة الدخول؟
        move_sl = normalize_bool(
            trade_data.get("move_sl_to_entry_after_tp1",
                          diagnostics.get("move_sl_to_entry_after_tp1", True))
        )
        if move_sl:
            trade_data["sl"] = round_price(effective_entry) if effective_entry > 0 else trade_data["entry"]
            trade_data["sl_moved_to_entry"] = True
            trade_data["sl_move_reason"] = "TP1 hit - protect remaining position"
        else:
            trade_data["sl_moved_to_entry"] = False
            trade_data["sl_move_reason"] = "TP1 hit - SL unchanged by config"

        diagnostics = trade_data.get("diagnostics", {}) or {}
        diagnostics["sl_moved_to_entry"] = trade_data["sl_moved_to_entry"]
        diagnostics["sl_move_reason"] = trade_data["sl_move_reason"]
        diagnostics["partial_close_pct"] = tp1_close_pct
        diagnostics["remaining_position_pct"] = remaining_pct
        trade_data["diagnostics"] = diagnostics

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
    # --- Breakeven protection params ---
    market_mode: str = None,
    protect_breakeven_on_block: bool = False,
    breakeven_min_profit_pct: float = 0.15,
    reason: str = "",
    **kwargs,
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

        # --- Breakeven protection logic (معدل لتحديد الجهة الصحيحة) ---
        mode_upper = str(market_mode or "").upper()
        block_this_side = (
            "BLOCK_ALL" in mode_upper
            or mode_upper.strip() == "BLOCK"
            or (side == "long" and "BLOCK_LONGS" in mode_upper)
            or (side == "short" and "BLOCK_SHORTS" in mode_upper)
        )

        if protect_breakeven_on_block and block_this_side:
            try:
                current_price = candles[-1]["close"]
                entry_price = safe_float(trade.get("entry"), 0.0)
                if entry_price > 0:
                    if side == "long":
                        current_profit_pct = ((current_price - entry_price) / entry_price) * 100
                    else:
                        current_profit_pct = ((entry_price - current_price) / entry_price) * 100
                    if current_profit_pct >= breakeven_min_profit_pct:
                        if not trade.get("protected_breakeven"):
                            current_sl = safe_float(trade.get("sl"), 0.0)
                            need_move = True
                            if side == "long":
                                if current_sl >= entry_price:
                                    need_move = False
                            else:
                                if current_sl <= entry_price:
                                    need_move = False
                            if need_move:
                                if trade.get("original_sl_before_breakeven") is None:
                                    trade["original_sl_before_breakeven"] = current_sl
                                trade["sl"] = entry_price
                                trade["protected_breakeven"] = True
                                trade["breakeven_protection_reason"] = reason or "market_block_protection"
                                trade["breakeven_protected_ts"] = now_ts
                                trade["sl_moved_to_entry"] = True
                                trade["sl_move_reason"] = reason or "market_block_protection"

                                diagnostics = trade.get("diagnostics", {}) or {}
                                diagnostics["protected_breakeven"] = True
                                diagnostics["breakeven_protection_reason"] = reason or "market_block_protection"
                                diagnostics["breakeven_protected_ts"] = now_ts
                                diagnostics["original_sl_before_breakeven"] = current_sl
                                diagnostics["sl_moved_to_entry"] = True
                                diagnostics["sl_move_reason"] = reason or "market_block_protection"
                                trade["diagnostics"] = diagnostics

                                save_trade(redis_client, trade_key, trade)
                                update_trade_history_snapshot(redis_client, trade)
                                logger.info(f"{symbol} → breakeven protected (SL moved to entry)")
            except Exception as e:
                logger.warning(f"Breakeven protection check failed for {symbol}: {e}")

        # --- Normal trade evaluation with evaluation_start_ts ---
        evaluation_start_ts = created_at
        protected_ts = trade.get("breakeven_protected_ts")
        if trade.get("protected_breakeven") and protected_ts is not None:
            evaluation_start_ts = safe_int(protected_ts, created_at)

        result = None
        state_changed = False

        for candle in candles:
            candle_ts = candle["ts"]
            if candle_ts > 10_000_000_000:
                candle_ts = candle_ts // 1000

            if candle_ts < evaluation_start_ts:
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
            if result == "loss" and trade.get("protected_breakeven") and \
               abs(safe_float(trade.get("sl"), 0.0) - safe_float(trade.get("entry"), 0.0)) < 1e-12:
                trade["result"] = "breakeven"
                trade["protected_breakeven_exit"] = True
                trade["exit_reason"] = "breakeven_protected_sl"
                mark_trade_closed(redis_client, trade_key, trade, "breakeven")
                logger.info(f"{symbol} → trade closed as breakeven (protected)")
            else:
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

            trade_setup = str(trade.get("setup_type", "unknown"))
            requested_setup = str(setup_type)

            if trade_setup != requested_setup and not trade_setup.startswith(requested_setup + "|"):
                continue

            if since_ts is not None:
                created_at = safe_int(trade.get("created_at"), 0)
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
            "breakeven_exits": 0,
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

        wins = safe_int(financial_summary.get("wins", wins))
        losses = safe_int(financial_summary.get("losses", losses))
        expired = safe_int(financial_summary.get("expired", expired))
        tp1_hits = safe_int(financial_summary.get("tp1_hits", tp1_hits))
        tp1_wins = safe_int(financial_summary.get("tp1_wins", tp1_wins))
        tp2_wins = safe_int(financial_summary.get("tp2_wins", tp2_wins))
        open_count = safe_int(financial_summary.get("open", open_count))
        breakeven_exits = safe_int(financial_summary.get("breakeven_exits", 0))

        decided = wins + losses + breakeven_exits
        closed = wins + losses + expired + breakeven_exits
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
            "breakeven_exits": breakeven_exits,
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
            "breakeven_exits": 0,
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
    tp1_only = safe_int(summary.get("tp1_then_entry", summary.get("tp1_only", summary.get("tp1_wins", 0))))
    expired = safe_int(summary.get("expired", 0))
    exit_quality = summary.get("exit_quality", "unknown")
    breakeven_exits = safe_int(summary.get("breakeven_exits", 0))

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
    if breakeven_exits > 0:
        lines.append(f"🔷 Breakeven Exits: {breakeven_exits}")
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

    diagnosis = diagnose_performance_problem(summary, is_exit_summary=True)
    lines.append("")
    lines.append(f"🧠 <b>تشخيص الأداء:</b> {diagnosis['emoji']} <b>{diagnosis['problem_label']}</b>")
    lines.append(f"• المشكلة الأساسية: {diagnosis['problem_label']}")
    lines.append(f"• السبب: {diagnosis['explanation']}")
    lines.append(f"• الإجراء المقترح: {diagnosis['action']}")

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
        breakeven_exits = safe_int(summary.get("breakeven_exits", 0))

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

        diagnosis = diagnose_performance_problem(summary, exits)
        diag_short = f" | مشكلة:{diagnosis['emoji']} {diagnosis['problem_label']}"

        be_str = f" 🔷BE:{breakeven_exits}" if breakeven_exits > 0 else ""

        lines.append(
            f"📅 {day} | 🎯{signals}/{closed}/{open_t} | "
            f"✅{wins} ❌{losses} ⏳{expired}{be_str} | WR:{winrate:.0f}% | "
            f"TP1:{tp1_rate:.0f}% TP2:{tp2_rate:.0f}% | "
            f"Net:{net_pnl:+.1f}% | محفظة:{wallet_pct:+.1f}% {wallet_usd:+.1f}$ | "
            f"خروج:{quality_short}"
            + diag_short
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
    breakeven_exits = safe_int(summary.get("breakeven_exits", 0))

    lines.append(f"إشارات: {signals} | مغلقة: {closed} | مفتوحة: {open_t}")
    lines.append(f"فوز: {wins} | خسارة: {losses} | منتهية: {expired}")
    if breakeven_exits > 0:
        lines.append(f"تعادل محمي: {breakeven_exits}")
    lines.append(f"نسبة الفوز: {safe_float(summary.get('winrate', 0)):.1f}%")
    lines.append(
        f"TP1 Rate: {safe_float(summary.get('tp1_rate', 0)):.1f}% | "
        f"TP2 Rate: {safe_float(summary.get('tp2_rate', 0)):.1f}%"
    )

    net_pnl = safe_float(summary.get("realized_leveraged_pnl_pct", summary.get("realized_pnl_pct", 0)))
    lines.append(f"صافي بعد الرافعة: {net_pnl:+.2f}%")
    lines.append(f"تأثير المحفظة: {wallet_pct:+.2f}% = {wallet_usd:+.2f}$")

    diagnosis = diagnose_performance_problem(summary, exit_summary)
    lines.append("")
    lines.append(f"🧠 <b>تشخيص الأداء:</b> {diagnosis['emoji']} <b>{diagnosis['problem_label']}</b>")
    lines.append(f"• المشكلة الأساسية: {diagnosis['problem_label']}")
    lines.append(f"• السبب: {diagnosis['explanation']}")
    lines.append(f"• الإجراء المقترح: {diagnosis['action']}")

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
                ts_for_filter = created_ts if created_ts else safe_int(trade.get("candle_time", 0), 0)

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
            ts_for_filter = created_ts if created_ts else safe_int(trade.get("candle_time", 0), 0)

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

    breakeven_str = ""
    if safe_int(summary.get("breakeven_exits", 0)) > 0:
        breakeven_str = f" | BE Exits: {summary['breakeven_exits']}"

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
        + breakeven_str
    )


def format_period_summary(title: str, summary: dict) -> str:
    side = normalize_side(summary.get("side", "long"))

    wins = safe_int(summary.get("wins", 0))
    losses = safe_int(summary.get("losses", 0))
    expired = safe_int(summary.get("expired", 0))
    breakeven_exits = safe_int(summary.get("breakeven_exits", 0))
    decided = wins + losses + expired + breakeven_exits
    closed = decided

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
    tp1_only = safe_int(summary.get("tp1_then_entry", summary.get("tp1_only", summary.get("tp1_wins", 0))))
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
    if breakeven_exits > 0:
        performance_block += f"\n• Breakeven Protected Exits: {breakeven_exits}"

    diagnosis = diagnose_performance_problem(summary)
    diagnosis_block = (
        f"\n🧠 <b>تشخيص الأداء:</b> {diagnosis['emoji']} <b>{diagnosis['problem_label']}</b>\n"
        f"• المشكلة الأساسية: {diagnosis['problem_label']}\n"
        f"• السبب: {diagnosis['explanation']}\n"
        f"• الإجراء المقترح: {diagnosis['action']}"
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
        f"Closed: {closed}\n"
        f"Wins (TP1+): {wins}\n"
        f"• Full Wins (TP2): {full_wins}\n"
        f"• TP1 Only: {tp1_only}\n"
        f"Losses: {losses}\n"
        f"Expired: {expired}\n"
        + (f"Breakeven Exits: {breakeven_exits}\n" if breakeven_exits > 0 else "") +
        f"Open: {summary.get('open', 0)}\n"
        f"Win rate: {summary.get('winrate', 0)}%"
        + performance_block
        + diagnosis_block
        + financial_block
    )


# ------------------------------------------------------------
# PERFORMANCE DIAGNOSIS
# ------------------------------------------------------------
def diagnose_performance_problem(
    summary: dict,
    exit_summary: dict = None,
    is_exit_summary: bool = False,
) -> dict:
    signals = safe_int(summary.get("signals", summary.get("trades_count", 0)))
    closed = safe_int(summary.get("closed", 0))
    losses = safe_int(summary.get("losses", 0))

    if is_exit_summary:
        tp1_hits = safe_int(summary.get("tp1_hits", 0))
        tp2_hits = safe_int(summary.get("tp2_hits", 0))
        tp1_rate = safe_float(summary.get("tp1_rate", 0))
        tp2_rate = safe_float(summary.get("tp2_rate", 0))
        tp1_to_tp2_rate = safe_float(summary.get("tp1_to_tp2_rate", 0))
        sl_before_tp1_rate = safe_float(summary.get("sl_before_tp1_rate", 0))
        winrate = None
    else:
        tp1_hits = safe_int(summary.get("tp1_hits", 0))
        tp2_hits = safe_int(summary.get("tp2_hits", summary.get("tp2_wins", 0)))
        tp1_rate = safe_float(summary.get("tp1_rate", 0))
        tp2_rate = safe_float(summary.get("tp2_rate", 0))
        tp1_to_tp2_rate = safe_float(summary.get("tp1_to_tp2_rate", 0))
        winrate = safe_float(summary.get("winrate", 0))
        sl_before_tp1_rate = safe_float(
            (exit_summary or {}).get("sl_before_tp1_rate", 0)
            if exit_summary else 0.0
        )

    loss_rate_closed = (losses / closed * 100) if closed > 0 else 0.0

    if tp1_to_tp2_rate == 0 and tp1_hits > 0:
        tp1_to_tp2_rate = (tp2_hits / tp1_hits) * 100

    if signals == 0 or closed == 0:
        return {
            "problem_type": "no_data",
            "problem_label": "بيانات غير كافية",
            "severity": "unknown",
            "emoji": "⚪",
            "explanation": "لا توجد صفقات مغلقة كافية للحكم.",
            "action": "انتظر بيانات أكثر قبل تعديل الاستراتيجية."
        }

    sl_condition = sl_before_tp1_rate >= 45
    if not sl_condition and not is_exit_summary and loss_rate_closed >= 60 and 35 <= tp1_rate <= 50:
        sl_condition = True

    if sl_condition:
        return {
            "problem_type": "sl_problem",
            "problem_label": "مشكلة SL / وقف قبل الحركة",
            "severity": "danger",
            "emoji": "🛑",
            "explanation": "نسبة كبيرة من الصفقات تضرب SL قبل الوصول إلى TP1.",
            "action": "راجع مكان SL، ATR multiplier، وتجنب الدخول في شموع ممتدة أو بعد Pump."
        }

    entry_problem_condition = False
    if is_exit_summary:
        if tp1_rate < 35 and loss_rate_closed > 55:
            entry_problem_condition = True
    else:
        if (tp1_rate < 35 or safe_float(winrate, 0) < 40) and loss_rate_closed > 55:
            entry_problem_condition = True

    if entry_problem_condition:
        return {
            "problem_type": "entry_problem",
            "problem_label": "مشكلة دخول",
            "severity": "danger",
            "emoji": "🔴",
            "explanation": "نسبة الوصول إلى TP1 ضعيفة، وهذا غالبًا يعني أن جودة الدخول أو فلترة الإشارات تحتاج تحسين.",
            "action": "راجع شروط الدخول، الفوليوم، التوقيت، وبعد السعر عن MA/VWAP، وفلتر الإشارات المتأخرة."
        }

    if tp1_rate >= 50 and tp1_to_tp2_rate < 30:
        return {
            "problem_type": "exit_problem",
            "problem_label": "مشكلة خروج بعد TP1",
            "severity": "warning",
            "emoji": "🟡",
            "explanation": "الإشارات تصل إلى TP1 بنسبة مقبولة، لكن نسبة التحول من TP1 إلى TP2 ضعيفة.",
            "action": "راجع قرب TP2، أو استخدم خروج تدريجي، أو Trailing Stop بعد TP1."
        }

    if is_exit_summary:
        good_condition = tp1_rate >= 55 and tp1_to_tp2_rate >= 35
    else:
        good_condition = tp1_rate >= 55 and tp1_to_tp2_rate >= 35 and safe_float(winrate, 0) >= 50

    if good_condition:
        return {
            "problem_type": "good",
            "problem_label": "الأداء جيد نسبيًا",
            "severity": "normal",
            "emoji": "🟢",
            "explanation": "نسبة الوصول إلى TP1 والتحول إلى TP2 مقبولة.",
            "action": "استمر في جمع البيانات ولا تعدل بعنف."
        }

    return {
        "problem_type": "mixed_problem",
        "problem_label": "مشكلة مختلطة",
        "severity": "warning",
        "emoji": "🟡",
        "explanation": "الأرقام لا تشير إلى سبب واحد واضح، وقد تكون المشكلة خليط بين الدخول والخروج والـ SL.",
        "action": "افحص الأداء حسب setup_type و entry_timing و market_state."
    }


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
