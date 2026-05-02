# tracking/performance.py
"""
وحدة تتبع الأداء والتقارير المالية لبوت OKX Scanner.

تعتمد على:
- tracking/summary_helpers.py (دوال التلخيص الآمنة)

جميع الدوال العامة محفوظة للتوافق مع main.py دون أي تغيير في أسمائها.

دعم كامل لتقارير الشورت بخطة إدارة رأس مال مستقلة:
- الشورت: 10 صفقات، مارجن 20$ لكل صفقة، رافعة 15x، إجمالي مارجن 200$، تعرض اسمي 3000$.
- اللونج: يبقى على الإعدادات القديمة (35% من رصيد 1000$).

تم استبدال التقريب الثابت round(..., 6) بدالة round_price()
حتى لا تتحول أسعار العملات الصغيرة جداً إلى 0.000000.
إضافة دوال تحليل الخروج والأداء اليومي وتقارير فورية.
إصلاح دالة normalize_market_type لتدعم "swap" كمرادف لـ "futures".
estimate_wallet_pnl تستخدم margin_per_trade كأساس للحساب.
إضافة دالة diagnose_performance_problem لتشخيص مشاكل الأداء.
إصلاح تشخيص exit_summary وعرض المشكلة الأساسية في جميع التقارير.
دعم الحقول الجديدة للتخزين وحماية نقطة التعادل.

**إصدار 3.1 – توحيد الطوابع الزمنية وإصلاح تتبع الشموع**
- جميع الطوابع الزمنية تُوحد إلى ثوانٍ عبر safe_timestamp().
- حقل last_processed_candle_ts أصبح top-level فقط، ولم يعد داخل diagnostics.
- mark_tp1_hit يقبل processed_candle_ts لتثبيت آخر شمعة معالجة.
- update_open_trades يمنع إعادة معالجة الشموع القديمة نهائياً.
- حماية breakeven لا تغيّر نقطة بدء التقييم.
- توافق تام مع الصفقات القديمة و main.py.

**إصدار 3.2 – توافق كامل مع main-10.py**
- إضافة entry_mode, market_entry, recommended_entry, has_pullback_plan لـ register_trade.
- إصلاح effective_entry: يُحسب من pullback_entry لما entry_mode == "pullback_pending".
- إصلاح pullback_triggered: كان هارد كود False، دلوقتي بييجي من القيمة الفعلية.
- إضافة الحقول الجديدة لـ build_trade_diagnostics وbuild_history_snapshot وtrade_data top-level.

**إصدار 3.3 – إصلاح منطق pullback_pending**
- صفقات البولباك المعلّقة لا تُقيَّم (SL / TP) حتى يلمس السعر pullback_entry.
- status = "pending_pullback" حتى التفعيل، ثم يتحول إلى "open".
- عند تفعيل البولباك تُحدَّث الأهداف وتُحفظ التغييرات فوراً.
- إضافة تتبع تحليلي كامل للصفقات المعلّقة.

**إصدار 3.4 – إصلاحات دقة التتبع (Refactor)**
- 🔴 إصلاح حساب نقطة التعادل باستخدام effective_entry بدلاً من entry.
- 🟡 حساب PnL مرجح للمخارج الجزئية (TP1 + TP2).
- 🟢 تجاهل صفقات pullback_pending من جميع الإحصائيات.
- 🟣 تحديث history snapshot بعد كل تغيير في الصفقة.
- 🟤 تصنيف breakeven عند تساوي SL مع effective_entry.

**إصدار 3.5 – تصحيحات Strict: Breakeven، عرض الخسائر، pullback، Winrate، تحويل TP2 إلى Trailing**
"""

import json
import time
import logging
import requests
from collections import Counter, defaultdict
from typing import Optional, List, Dict, Union

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
TRADE_TTL_SECONDS = 60 * 60 * 24 * 30        # 30 يومًا للمفاتيح النشطة
TRADE_HISTORY_TTL_SECONDS = 60 * 60 * 24 * 90  # 90 يومًا للتاريخ

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
# دوال مساعدة عامة (safe & normalization)
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


def normalize_list(value) -> list:
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
    أمثلة:
    - BTC / ETH: 4-6 منازل
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


def safe_timestamp(ts_value, default=0) -> int:
    """يحول timestamp بأمان إلى int (ثوانٍ) مع دعم تقسيم الميلي ثانية."""
    try:
        ts = int(float(ts_value))
        if ts > 10_000_000_000:
            ts = ts // 1000
        return ts
    except (ValueError, TypeError):
        return default


def get_trade_effective_entry(trade: dict) -> float:
    """
    ترجع سعر الدخول الفعلي للصفقة بغض النظر عن حالتها.
    الأولوية: trade["effective_entry"] → diagnostics["effective_entry"] → trade["entry"]
    """
    diagnostics = trade.get("diagnostics", {}) or {}
    eff = trade.get("effective_entry")
    if eff is not None and safe_float(eff, 0.0) > 0:
        return safe_float(eff, 0.0)
    eff = diagnostics.get("effective_entry")
    if eff is not None and safe_float(eff, 0.0) > 0:
        return safe_float(eff, 0.0)
    return safe_float(trade.get("entry"), 0.0)


# ------------------------------------------------------------
# مفاتيح Redis
# ------------------------------------------------------------
def get_trade_key(market_type: str, side: str, symbol: str, candle_time: int) -> str:
    market_type = normalize_market_type(market_type)
    side = normalize_side(side)
    candle_time = int(candle_time)  # من المهم أن يكون بالثواني بالفعل بعد safe_timestamp
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
# SERIALIZATION HELPERS (build diagnostics & history snapshot)
# ------------------------------------------------------------
def build_trade_diagnostics(extra_fields: dict = None) -> dict:
    """
    بناء diagnostics dict من الحقول الإضافية مع الحفاظ على القيم الآمنة.
    تدعم جميع الحقول الجديدة والقديمة.
    *** لا يحتوي على last_processed_candle_ts، بل يُحفظ top-level فقط. ***
    """
    extra = extra_fields or {}

    def _get(key, default=None):
        val = extra.get(key)
        return val if val is not None else default

    pullback_entry = _get("pullback_entry")
    pullback_low = _get("pullback_low")
    pullback_high = _get("pullback_high")
    effective_entry = _get("effective_entry")

    diagnostics = {
        "raw_score": safe_float(_get("raw_score"), 0.0),
        "effective_score": safe_float(_get("effective_score"), 0.0),
        "dynamic_threshold": safe_float(_get("dynamic_threshold"), 0.0),
        "required_min_score": safe_float(_get("required_min_score"), 0.0),
        "dist_ma": safe_float(_get("dist_ma"), 0.0),
        "rank_volume_24h": safe_float(_get("rank_volume_24h"), 0.0),

        "market_state": normalize_text(_get("market_state"), "unknown"),
        "market_state_label": normalize_text(_get("market_state_label"), "unknown"),
        "market_bias_label": normalize_text(_get("market_bias_label"), "unknown"),
        "alt_mode": normalize_text(_get("alt_mode"), "unknown"),
        "entry_timing": normalize_text(_get("entry_timing"), "unknown"),
        "opportunity_type": normalize_text(_get("opportunity_type"), "unknown"),
        "early_priority": normalize_text(_get("early_priority"), "unknown"),
        "breakout_quality": normalize_text(_get("breakout_quality"), "unknown"),
        "risk_level": normalize_text(_get("risk_level"), "unknown"),
        "alert_id": normalize_text(_get("alert_id"), ""),

        "fake_signal": normalize_bool(_get("fake_signal", False)),
        "is_reverse": normalize_bool(_get("is_reverse", False)),
        "reversal_4h_confirmed": normalize_bool(_get("reversal_4h_confirmed", False)),
        "has_high_impact_news": normalize_bool(_get("has_high_impact_news", False)),

        "news_titles": normalize_list(_get("news_titles")),
        "warning_reasons": normalize_list(_get("warning_reasons")),

        "pullback_entry": round_price(pullback_entry) if pullback_entry is not None else None,
        "pullback_low": round_price(pullback_low) if pullback_low is not None else None,
        "pullback_high": round_price(pullback_high) if pullback_high is not None else None,
        "pullback_triggered": normalize_bool(_get("pullback_triggered", False)),
        "effective_entry": round_price(effective_entry) if effective_entry is not None else None,

        "rr1": safe_float(_get("rr1"), 1.5),
        "rr2": safe_float(_get("rr2"), 3.0),

        # الحقول الجديدة (v2)
        "setup_type_base": normalize_text(_get("setup_type_base"), ""),
        "final_threshold": safe_float(_get("final_threshold"), 0.0),
        "adjustments_log": normalize_list(_get("adjustments_log")),
        "warning_penalty": safe_float(_get("warning_penalty"), 0.0),
        "warning_penalty_details": normalize_list(_get("warning_penalty_details")),
        "warning_high_count": safe_int(_get("warning_high_count"), 0),
        "warning_medium_count": safe_int(_get("warning_medium_count"), 0),
        "tp1_close_pct": safe_float(_get("tp1_close_pct"), 50.0),
        "tp2_close_pct": safe_float(_get("tp2_close_pct"), 50.0),
        "move_sl_to_entry_after_tp1": normalize_bool(_get("move_sl_to_entry_after_tp1", True)),
        "fib_position": normalize_text(_get("fib_position"), "unknown"),
        "fib_position_ratio": safe_float(_get("fib_position_ratio")),
        "fib_label": normalize_text(_get("fib_label"), ""),
        "had_pullback": normalize_bool(_get("had_pullback", False)),
        "pullback_pct": safe_float(_get("pullback_pct")),
        "pullback_label": normalize_text(_get("pullback_label"), ""),
        "wave_estimate": safe_int(_get("wave_estimate"), 0),
        "wave_peaks": safe_int(_get("wave_peaks"), 0),
        "wave_label": normalize_text(_get("wave_label"), ""),
        "entry_maturity": normalize_text(_get("entry_maturity"), "unknown"),
        "maturity_penalty": safe_float(_get("maturity_penalty"), 0.0),
        "maturity_bonus": safe_float(_get("maturity_bonus"), 0.0),
        "falling_knife_risk": normalize_bool(_get("falling_knife_risk", False)),
        "falling_knife_reasons": normalize_list(_get("falling_knife_reasons")),
        "target_method": normalize_text(_get("target_method"), "unknown"),
        "nearest_resistance": round_price(_get("nearest_resistance")) if _get("nearest_resistance") is not None else None,
        "nearest_support": round_price(_get("nearest_support")) if _get("nearest_support") is not None else None,
        "resistance_warning": normalize_text(_get("resistance_warning"), ""),
        "support_warning": normalize_text(_get("support_warning"), ""),
        "target_notes": normalize_list(_get("target_notes")),
        "sl_method": normalize_text(_get("sl_method"), "unknown"),
        "sl_notes": normalize_list(_get("sl_notes")),
        "wave_context": normalize_text(_get("wave_context"), ""),
        "setup_context": normalize_text(_get("setup_context"), ""),
        "reversal_quality": normalize_text(_get("reversal_quality"), ""),
        "reversal_structure_confirmed": normalize_bool(_get("reversal_structure_confirmed", False)),
        "strong_bull_pullback": normalize_bool(_get("strong_bull_pullback", False)),
        "strong_breakout_exception": normalize_bool(_get("strong_breakout_exception", False)),

        # Extra Strong Setup
        "has_extra_strong_setup": normalize_bool(_get("has_extra_strong_setup", False)),
        "extra_setup_names": normalize_list(_get("extra_setup_names")),
        "extra_setup_bonus": safe_float(_get("extra_setup_bonus"), 0.0),
        "primary_extra_setup": normalize_text(_get("primary_extra_setup"), ""),
        "extra_setups_details": _get("extra_setups_details") or {},

        "protected_breakeven": normalize_bool(_get("protected_breakeven", False)),
        "breakeven_protection_reason": normalize_text(_get("breakeven_protection_reason"), ""),
        "breakeven_protected_ts": safe_int(_get("breakeven_protected_ts"), 0) if _get("breakeven_protected_ts") is not None else None,
        "original_sl_before_breakeven": round_price(_get("original_sl_before_breakeven")) if _get("original_sl_before_breakeven") is not None else None,
        "protected_breakeven_exit": normalize_bool(_get("protected_breakeven_exit", False)),
        "sl_moved_to_entry": normalize_bool(_get("sl_moved_to_entry", False)),
        "sl_move_reason": normalize_text(_get("sl_move_reason"), ""),
        "exit_reason": normalize_text(_get("exit_reason"), ""),

        # حقول الدخول الجديدة (v3.2 - main-10 compatibility) + v3.3 pullback tracking
        "entry_mode": normalize_text(_get("entry_mode"), "market"),
        "market_entry": round_price(_get("market_entry")) if _get("market_entry") is not None else None,
        "recommended_entry": round_price(_get("recommended_entry")) if _get("recommended_entry") is not None else None,
        "has_pullback_plan": normalize_bool(_get("has_pullback_plan", False)),

        # حقول تتبع تفعيل البولباك
        "activated_at": safe_int(_get("activated_at")) if _get("activated_at") is not None else None,
        "activated_candle_ts": safe_int(_get("activated_candle_ts")) if _get("activated_candle_ts") is not None else None,
        "pending_pullback_expired": normalize_bool(_get("pending_pullback_expired", False)),
        "pending_pullback_expire_reason": normalize_text(_get("pending_pullback_expire_reason"), ""),
    }

    return diagnostics


def build_history_snapshot(trade_data: dict) -> dict:
    """
    يبني snapshot تاريخي من trade_data مع الأولوية للمفاتيح top-level ثم diagnostics.
    """
    diagnostics = trade_data.get("diagnostics", {}) or {}

    def _get_field(name, default=None):
        # نبحث أولاً في top-level ثم diagnostics
        val = trade_data.get(name)
        if val is None:
            val = diagnostics.get(name)
        return val if val is not None else default

    snapshot = {
        "symbol": trade_data.get("symbol", ""),
        "market_type": trade_data.get("market_type", "futures"),
        "side": trade_data.get("side", "long"),
        "setup_type": trade_data.get("setup_type", "unknown"),
        "timeframe": trade_data.get("timeframe", "15m"),
        "candle_time": safe_timestamp(trade_data.get("candle_time"), 0),

        "score": safe_float(trade_data.get("score"), 0.0),
        "entry": safe_float(trade_data.get("entry"), 0.0),
        "sl": safe_float(trade_data.get("sl"), 0.0),
        "initial_sl": safe_float(trade_data.get("initial_sl"), 0.0),
        "tp1": safe_float(trade_data.get("tp1"), 0.0),
        "tp2": safe_float(trade_data.get("tp2"), 0.0),

        "created_at": safe_timestamp(trade_data.get("created_at"), 0),
        "status": trade_data.get("status", "open"),
        "result": trade_data.get("result"),
        "tp1_hit": normalize_bool(trade_data.get("tp1_hit", False)),
        "tp2_hit": normalize_bool(trade_data.get("tp2_hit", False)),
        "tp2_hit_at": safe_timestamp(trade_data.get("tp2_hit_at")),

        "market_state": _get_field("market_state", "unknown"),
        "market_state_label": _get_field("market_state_label", "unknown"),
        "market_bias_label": _get_field("market_bias_label", "unknown"),
        "alt_mode": _get_field("alt_mode", "unknown"),
        "entry_timing": _get_field("entry_timing", "unknown"),
        "opportunity_type": _get_field("opportunity_type", "unknown"),
        "early_priority": _get_field("early_priority", "unknown"),
        "breakout_quality": _get_field("breakout_quality", "unknown"),
        "risk_level": _get_field("risk_level", "unknown"),

        "dist_ma": safe_float(_get_field("dist_ma"), 0.0),
        "raw_score": safe_float(_get_field("raw_score"), 0.0),
        "effective_score": safe_float(_get_field("effective_score"), 0.0),
        "dynamic_threshold": safe_float(_get_field("dynamic_threshold"), 0.0),
        "required_min_score": safe_float(_get_field("required_min_score"), 0.0),

        "fake_signal": _get_field("fake_signal", False),
        "is_reverse": _get_field("is_reverse", False),
        "reversal_4h_confirmed": _get_field("reversal_4h_confirmed", False),
        "has_high_impact_news": _get_field("has_high_impact_news", False),

        "warning_reasons": normalize_list(trade_data.get("warning_reasons", [])),
        "news_titles": normalize_list(_get_field("news_titles", [])),

        "pullback_entry": _get_field("pullback_entry"),
        "pullback_low": _get_field("pullback_low"),
        "pullback_high": _get_field("pullback_high"),
        "pullback_triggered": _get_field("pullback_triggered", False),
        "effective_entry": _get_field("effective_entry"),

        "rr1": safe_float(_get_field("rr1"), 1.5),
        "rr2": safe_float(_get_field("rr2"), 3.0),

        # حقول جديدة
        "setup_type_base": _get_field("setup_type_base", ""),
        "final_threshold": safe_float(_get_field("final_threshold"), 0.0),
        "target_method": _get_field("target_method", "unknown"),
        "nearest_resistance": _get_field("nearest_resistance"),
        "nearest_support": _get_field("nearest_support"),
        "resistance_warning": _get_field("resistance_warning", ""),
        "support_warning": _get_field("support_warning", ""),
        "target_notes": normalize_list(_get_field("target_notes", [])),
        "sl_method": _get_field("sl_method", "unknown"),
        "sl_notes": normalize_list(_get_field("sl_notes", [])),
        "falling_knife_risk": normalize_bool(_get_field("falling_knife_risk", False)),
        "falling_knife_reasons": normalize_list(_get_field("falling_knife_reasons", [])),
        "wave_context": _get_field("wave_context", ""),
        "setup_context": _get_field("setup_context", ""),
        "reversal_quality": _get_field("reversal_quality", ""),
        "reversal_structure_confirmed": normalize_bool(_get_field("reversal_structure_confirmed", False)),
        "strong_bull_pullback": normalize_bool(_get_field("strong_bull_pullback", False)),
        "strong_breakout_exception": normalize_bool(_get_field("strong_breakout_exception", False)),

        # Extra Strong Setup
        "has_extra_strong_setup": normalize_bool(_get_field("has_extra_strong_setup", False)),
        "extra_setup_names": normalize_list(_get_field("extra_setup_names", [])),
        "extra_setup_bonus": safe_float(_get_field("extra_setup_bonus"), 0.0),
        "primary_extra_setup": _get_field("primary_extra_setup", ""),
        "extra_setups_details": _get_field("extra_setups_details", {}) or {},

        "tp1_close_pct": safe_float(_get_field("tp1_close_pct"), 50.0),
        "tp2_close_pct": safe_float(_get_field("tp2_close_pct"), 50.0),
        "move_sl_to_entry_after_tp1": normalize_bool(_get_field("move_sl_to_entry_after_tp1", True)),
        "fib_position": _get_field("fib_position", "unknown"),
        "fib_position_ratio": safe_float(_get_field("fib_position_ratio")),
        "fib_label": _get_field("fib_label", ""),
        "had_pullback": normalize_bool(_get_field("had_pullback", False)),
        "pullback_pct": safe_float(_get_field("pullback_pct")),
        "pullback_label": _get_field("pullback_label", ""),
        "wave_estimate": safe_int(_get_field("wave_estimate"), 0),
        "wave_peaks": safe_int(_get_field("wave_peaks"), 0),
        "wave_label": _get_field("wave_label", ""),
        "entry_maturity": _get_field("entry_maturity", "unknown"),
        "maturity_penalty": safe_float(_get_field("maturity_penalty"), 0.0),
        "maturity_bonus": safe_float(_get_field("maturity_bonus"), 0.0),
        "warning_penalty": safe_float(_get_field("warning_penalty"), 0.0),
        "warning_penalty_details": normalize_list(_get_field("warning_penalty_details", [])),
        "adjustments_log": normalize_list(_get_field("adjustments_log", [])),
        "protected_breakeven": normalize_bool(_get_field("protected_breakeven", False)),
        "breakeven_protection_reason": _get_field("breakeven_protection_reason", ""),
        "breakeven_protected_ts": safe_int(_get_field("breakeven_protected_ts")) if _get_field("breakeven_protected_ts") is not None else None,
        "original_sl_before_breakeven": _get_field("original_sl_before_breakeven"),
        "protected_breakeven_exit": normalize_bool(_get_field("protected_breakeven_exit", False)),
        "sl_moved_to_entry": normalize_bool(_get_field("sl_moved_to_entry", False)),
        "sl_moved_to_tp1": normalize_bool(_get_field("sl_moved_to_tp1", False)),
        "sl_move_reason": _get_field("sl_move_reason", ""),
        "exit_reason": _get_field("exit_reason", ""),
        "last_processed_candle_ts": safe_timestamp(trade_data.get("last_processed_candle_ts"), 0),

        # حقول الدخول الجديدة (v3.2)
        "entry_mode": _get_field("entry_mode", "market"),
        "market_entry": _get_field("market_entry"),
        "recommended_entry": _get_field("recommended_entry"),
        "has_pullback_plan": normalize_bool(_get_field("has_pullback_plan", False)),

        # حقول تتبع تفعيل البولباك (v3.3)
        "activated_at": _get_field("activated_at"),
        "activated_candle_ts": _get_field("activated_candle_ts"),
        "pending_pullback_expired": normalize_bool(_get_field("pending_pullback_expired", False)),
        "pending_pullback_expire_reason": _get_field("pending_pullback_expire_reason", ""),

        "diagnostics": diagnostics,
    }

    return snapshot


# ------------------------------------------------------------
# TRADE STORAGE (register / load / save / mark)
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
    # الحقول الجديدة (اختيارية)
    final_threshold: float = None,
    target_method: str = None,
    nearest_resistance: float = None,
    nearest_support: float = None,
    resistance_warning: str = None,
    support_warning: str = None,
    target_notes: list = None,
    sl_method: str = None,
    sl_notes: list = None,
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
    has_extra_strong_setup: bool = None,
    extra_setup_names: list = None,
    extra_setup_bonus: float = None,
    primary_extra_setup: str = None,
    extra_setups_details: dict = None,
    # حقول الدخول الجديدة (v3.2 - main-10 compatibility)
    entry_mode: str = "market",
    market_entry: float = None,
    recommended_entry: float = None,
    has_pullback_plan: bool = False,
    **kwargs,
):
    """
    تسجيل صفقة جديدة في Redis مع دعم جميع الحقول الجديدة و backward compatibility.
    الأولوية للـ arguments المباشرة على kwargs.
    """
    if redis_client is None:
        return False

    market_type = normalize_market_type(market_type)
    side = normalize_side(side)

    # تجميع كل الـ arguments المباشرة في قاموس للمقارنة مع kwargs
    direct_args = {
        "raw_score": raw_score,
        "effective_score": effective_score,
        "dynamic_threshold": dynamic_threshold,
        "required_min_score": required_min_score,
        "dist_ma": dist_ma,
        "entry_timing": entry_timing,
        "opportunity_type": opportunity_type,
        "market_state": market_state,
        "market_state_label": market_state_label,
        "market_bias_label": market_bias_label,
        "alt_mode": alt_mode,
        "early_priority": early_priority,
        "breakout_quality": breakout_quality,
        "risk_level": risk_level,
        "fake_signal": fake_signal,
        "is_reverse_signal": is_reverse_signal,
        "reversal_4h_confirmed": reversal_4h_confirmed,
        "rank_volume_24h": rank_volume_24h,
        "alert_id": alert_id,
        "has_high_impact_news": has_high_impact_news,
        "news_titles": news_titles,
        "warning_reasons": warning_reasons,
        "pullback_entry": pullback_entry,
        "pullback_low": pullback_low,
        "pullback_high": pullback_high,
        "rr1": rr1,
        "rr2": rr2,
        "final_threshold": final_threshold,
        "target_method": target_method,
        "nearest_resistance": nearest_resistance,
        "nearest_support": nearest_support,
        "resistance_warning": resistance_warning,
        "support_warning": support_warning,
        "target_notes": target_notes,
        "sl_method": sl_method,
        "sl_notes": sl_notes,
        "falling_knife_risk": falling_knife_risk,
        "falling_knife_reasons": falling_knife_reasons,
        "reversal_quality": reversal_quality,
        "wave_context": wave_context,
        "setup_context": setup_context,
        "reversal_structure_confirmed": reversal_structure_confirmed,
        "warning_penalty": warning_penalty,
        "warning_high_count": warning_high_count,
        "warning_medium_count": warning_medium_count,
        "warning_penalty_details": warning_penalty_details,
        "adjustments_log": adjustments_log,
        "protected_breakeven": protected_breakeven,
        "breakeven_protection_reason": breakeven_protection_reason,
        "breakeven_protected_ts": breakeven_protected_ts,
        "original_sl_before_breakeven": original_sl_before_breakeven,
        "protected_breakeven_exit": protected_breakeven_exit,
        "tp1_close_pct": tp1_close_pct,
        "tp2_close_pct": tp2_close_pct,
        "move_sl_to_entry_after_tp1": move_sl_to_entry_after_tp1,
        "setup_type_base": setup_type_base,
        "fib_position": fib_position,
        "fib_position_ratio": fib_position_ratio,
        "fib_label": fib_label,
        "had_pullback": had_pullback,
        "pullback_pct": pullback_pct,
        "pullback_label": pullback_label,
        "wave_estimate": wave_estimate,
        "wave_peaks": wave_peaks,
        "wave_label": wave_label,
        "entry_maturity": entry_maturity,
        "maturity_penalty": maturity_penalty,
        "maturity_bonus": maturity_bonus,
        "strong_bull_pullback": strong_bull_pullback,
        "strong_breakout_exception": strong_breakout_exception,
        "has_extra_strong_setup": has_extra_strong_setup,
        "extra_setup_names": extra_setup_names,
        "extra_setup_bonus": extra_setup_bonus,
        "primary_extra_setup": primary_extra_setup,
        "extra_setups_details": extra_setups_details,
        # حقول الدخول الجديدة
        "entry_mode": entry_mode,
        "market_entry": market_entry,
        "recommended_entry": recommended_entry,
        "has_pullback_plan": has_pullback_plan,
    }

    def _get_val(key, default=None):
        """الأولوية للـ direct_args ثم kwargs"""
        if key in direct_args and direct_args[key] is not None:
            return direct_args[key]
        return kwargs.get(key, default)

    # --- توحيد الطوابع الزمنية ---
    now_ts = int(time.time())
    normalized_candle_time = safe_timestamp(candle_time, now_ts)

    # --- أسعار أساسية ---
    entry = round_price(entry)
    sl = round_price(sl)

    # TP1 / TP2
    if tp1 is None:
        tp1 = calc_tp1(entry, sl, side=side)
    else:
        tp1 = round_price(tp1)
    if tp2 is None:
        tp2 = calc_tp2(entry, sl, side=side)
    else:
        tp2 = round_price(tp2)

    # توحيد بعض الحقول المتشابهة
    if _get_val("is_reverse_signal") is False and kwargs.get("is_reverse"):
        is_reverse_signal = normalize_bool(kwargs["is_reverse"])
    if _get_val("warning_reasons") is None and kwargs.get("warnings"):
        warning_reasons = normalize_list(kwargs["warnings"])

    # --- حساب effective_entry بناءً على entry_mode ---
    _entry_mode = str(_get_val("entry_mode", "market") or "market")
    _pullback_entry = _get_val("pullback_entry")
    _has_pullback_plan = normalize_bool(_get_val("has_pullback_plan", False))

    if _entry_mode == "pullback_pending" and _pullback_entry is not None and safe_float(_pullback_entry, 0.0) > 0:
        _effective_entry = round_price(_pullback_entry)
        _pullback_triggered = False
        _status = "pending_pullback"
    else:
        _effective_entry = entry
        _pullback_triggered = False
        _status = "open"

    # --- بناء diagnostics (بدون last_processed_candle_ts) ---
    docs = build_trade_diagnostics({
        "raw_score": _get_val("raw_score", score),
        "effective_score": _get_val("effective_score", score),
        "dynamic_threshold": _get_val("dynamic_threshold"),
        "required_min_score": _get_val("required_min_score"),
        "dist_ma": _get_val("dist_ma"),
        "rank_volume_24h": _get_val("rank_volume_24h"),
        "market_state": _get_val("market_state"),
        "market_state_label": _get_val("market_state_label"),
        "market_bias_label": _get_val("market_bias_label"),
        "alt_mode": _get_val("alt_mode"),
        "entry_timing": _get_val("entry_timing"),
        "opportunity_type": _get_val("opportunity_type"),
        "early_priority": _get_val("early_priority"),
        "breakout_quality": _get_val("breakout_quality"),
        "risk_level": _get_val("risk_level"),
        "alert_id": _get_val("alert_id"),
        "fake_signal": _get_val("fake_signal"),
        "is_reverse": _get_val("is_reverse_signal"),
        "reversal_4h_confirmed": _get_val("reversal_4h_confirmed"),
        "has_high_impact_news": _get_val("has_high_impact_news"),
        "news_titles": _get_val("news_titles", []),
        "warning_reasons": warning_reasons or [],
        "pullback_entry": _get_val("pullback_entry"),
        "pullback_low": _get_val("pullback_low"),
        "pullback_high": _get_val("pullback_high"),
        "pullback_triggered": _pullback_triggered,
        "effective_entry": _effective_entry,
        "rr1": _get_val("rr1", rr1),
        "rr2": _get_val("rr2", rr2),
        # حقول جديدة
        "setup_type_base": _get_val("setup_type_base", ""),
        "final_threshold": _get_val("final_threshold"),
        "adjustments_log": _get_val("adjustments_log"),
        "warning_penalty": _get_val("warning_penalty"),
        "warning_high_count": _get_val("warning_high_count"),
        "warning_medium_count": _get_val("warning_medium_count"),
        "warning_penalty_details": _get_val("warning_penalty_details"),
        "tp1_close_pct": _get_val("tp1_close_pct", 50.0),
        "tp2_close_pct": _get_val("tp2_close_pct", 50.0),
        "move_sl_to_entry_after_tp1": _get_val("move_sl_to_entry_after_tp1", True),
        "fib_position": _get_val("fib_position", "unknown"),
        "fib_position_ratio": _get_val("fib_position_ratio"),
        "fib_label": _get_val("fib_label", ""),
        "had_pullback": _get_val("had_pullback", False),
        "pullback_pct": _get_val("pullback_pct"),
        "pullback_label": _get_val("pullback_label", ""),
        "wave_estimate": _get_val("wave_estimate", 0),
        "wave_peaks": _get_val("wave_peaks", 0),
        "wave_label": _get_val("wave_label", ""),
        "entry_maturity": _get_val("entry_maturity", "unknown"),
        "maturity_penalty": _get_val("maturity_penalty", 0.0),
        "maturity_bonus": _get_val("maturity_bonus", 0.0),
        "falling_knife_risk": _get_val("falling_knife_risk", False),
        "falling_knife_reasons": _get_val("falling_knife_reasons", []),
        "target_method": _get_val("target_method", "unknown"),
        "nearest_resistance": _get_val("nearest_resistance"),
        "nearest_support": _get_val("nearest_support"),
        "resistance_warning": _get_val("resistance_warning", ""),
        "support_warning": _get_val("support_warning", ""),
        "target_notes": _get_val("target_notes", []),
        "sl_method": _get_val("sl_method", "unknown"),
        "sl_notes": _get_val("sl_notes", []),
        "wave_context": _get_val("wave_context", ""),
        "setup_context": _get_val("setup_context", ""),
        "reversal_quality": _get_val("reversal_quality", ""),
        "reversal_structure_confirmed": _get_val("reversal_structure_confirmed", False),
        "strong_bull_pullback": _get_val("strong_bull_pullback", False),
        "strong_breakout_exception": _get_val("strong_breakout_exception", False),
        # Extra Strong
        "has_extra_strong_setup": _get_val("has_extra_strong_setup", False),
        "extra_setup_names": _get_val("extra_setup_names", []),
        "extra_setup_bonus": _get_val("extra_setup_bonus", 0.0),
        "primary_extra_setup": _get_val("primary_extra_setup", ""),
        "extra_setups_details": _get_val("extra_setups_details", {}) or {},
        # حقول الدخول الجديدة (v3.2)
        "entry_mode": _entry_mode,
        "market_entry": _get_val("market_entry"),
        "recommended_entry": _get_val("recommended_entry"),
        "has_pullback_plan": _has_pullback_plan,
        # حماية
        "protected_breakeven": _get_val("protected_breakeven", False),
        "breakeven_protection_reason": _get_val("breakeven_protection_reason", ""),
        "breakeven_protected_ts": _get_val("breakeven_protected_ts"),
        "original_sl_before_breakeven": _get_val("original_sl_before_breakeven"),
        "protected_breakeven_exit": _get_val("protected_breakeven_exit", False),
        "sl_moved_to_entry": False,
        "sl_move_reason": "",
        # حقول تتبع البولباك (v3.3)
        "activated_at": None,
        "activated_candle_ts": None,
        "pending_pullback_expired": False,
        "pending_pullback_expire_reason": "",
    })

    # بناء trade_data
    trade_data = {
        "symbol": symbol,
        "market_type": market_type,
        "side": side,
        "timeframe": timeframe,
        "candle_time": normalized_candle_time,

        "entry": entry,
        "sl": sl,
        "initial_sl": sl,
        "tp1": tp1,
        "tp2": tp2,

        "score": round(safe_float(score), 2),
        "btc_mode": btc_mode,
        "funding_label": funding_label,

        "status": _status,
        "tp1_hit": False,
        "tp1_hit_at": None,
        "tp2_hit": False,
        "tp2_hit_at": None,
        "created_at": now_ts,
        "updated_at": now_ts,
        "closed_at": None,
        "result": None,
        "last_processed_candle_ts": normalized_candle_time,

        "reasons": list(reasons) if reasons else [],
        "warning_reasons": list(warning_reasons) if warning_reasons else [],

        "pre_breakout": bool(pre_breakout),
        "breakout": bool(breakout),
        "signal_event": "breakdown" if side == "short" else "breakout",
        "pre_signal": bool(pre_breakout),
        "break_signal": bool(breakout),

        "vol_ratio": round(safe_float(vol_ratio), 4),
        "candle_strength": round(safe_float(candle_strength), 4),
        "mtf_confirmed": bool(mtf_confirmed),
        "is_new": bool(is_new),
        "btc_dominance_proxy": btc_dominance_proxy,
        "change_24h": round(safe_float(change_24h), 2),

        "setup_type": setup_type or "unknown",
        "diagnostics": docs,

        # top-level لحقول إضافية هامة للتقارير
        "setup_type_base": _get_val("setup_type_base", ""),
        "final_threshold": safe_float(_get_val("final_threshold", score)),
        "target_method": _get_val("target_method", "unknown"),
        "nearest_resistance": round_price(_get_val("nearest_resistance")) if _get_val("nearest_resistance") is not None else None,
        "nearest_support": round_price(_get_val("nearest_support")) if _get_val("nearest_support") is not None else None,
        "resistance_warning": _get_val("resistance_warning", ""),
        "support_warning": _get_val("support_warning", ""),
        "target_notes": normalize_list(_get_val("target_notes")),
        "sl_method": _get_val("sl_method", "unknown"),
        "sl_notes": normalize_list(_get_val("sl_notes")),
        "falling_knife_risk": normalize_bool(_get_val("falling_knife_risk", False)),
        "falling_knife_reasons": normalize_list(_get_val("falling_knife_reasons")),
        "wave_context": _get_val("wave_context", ""),
        "setup_context": _get_val("setup_context", ""),
        "reversal_quality": _get_val("reversal_quality", ""),
        "reversal_structure_confirmed": bool(_get_val("reversal_structure_confirmed", False)),
        "strong_bull_pullback": _get_val("strong_bull_pullback", False),
        "strong_breakout_exception": _get_val("strong_breakout_exception", False),

        "has_extra_strong_setup": _get_val("has_extra_strong_setup", False),
        "extra_setup_names": _get_val("extra_setup_names", []),
        "extra_setup_bonus": _get_val("extra_setup_bonus", 0.0),
        "primary_extra_setup": _get_val("primary_extra_setup", ""),
        "extra_setups_details": _get_val("extra_setups_details", {}) or {},

        "tp1_close_pct": safe_float(_get_val("tp1_close_pct", 50.0)),
        "tp2_close_pct": safe_float(_get_val("tp2_close_pct", 50.0)),
        "move_sl_to_entry_after_tp1": normalize_bool(_get_val("move_sl_to_entry_after_tp1", True)),
        "protected_breakeven": bool(_get_val("protected_breakeven", False)),
        "breakeven_protection_reason": _get_val("breakeven_protection_reason", ""),
        "breakeven_protected_ts": _get_val("breakeven_protected_ts"),
        "original_sl_before_breakeven": round_price(_get_val("original_sl_before_breakeven")) if _get_val("original_sl_before_breakeven") is not None else None,
        "protected_breakeven_exit": bool(_get_val("protected_breakeven_exit", False)),
        "sl_moved_to_entry": False,
        "sl_moved_to_tp1": False,
        "sl_move_reason": "",
        # Fib/pullback/wave etc
        "fib_position": _get_val("fib_position", "unknown"),
        "fib_position_ratio": safe_float(_get_val("fib_position_ratio")),
        "fib_label": _get_val("fib_label", ""),
        "had_pullback": _get_val("had_pullback", False),
        "pullback_pct": safe_float(_get_val("pullback_pct")),
        "pullback_label": _get_val("pullback_label", ""),
        "wave_estimate": safe_int(_get_val("wave_estimate", 0)),
        "wave_peaks": safe_int(_get_val("wave_peaks", 0)),
        "wave_label": _get_val("wave_label", ""),
        "entry_maturity": _get_val("entry_maturity", "unknown"),
        "maturity_penalty": safe_float(_get_val("maturity_penalty", 0.0)),
        "maturity_bonus": safe_float(_get_val("maturity_bonus", 0.0)),
        "adjustments_log": normalize_list(_get_val("adjustments_log")),
        "warning_penalty": safe_float(_get_val("warning_penalty", 0.0)),
        "warning_high_count": safe_int(_get_val("warning_high_count", 0)),
        "warning_medium_count": safe_int(_get_val("warning_medium_count", 0)),
        "warning_penalty_details": normalize_list(_get_val("warning_penalty_details")),

        # حقول الدخول الجديدة top-level (v3.2)
        "entry_mode": _entry_mode,
        "market_entry": round_price(_get_val("market_entry")) if _get_val("market_entry") is not None else None,
        "recommended_entry": round_price(_get_val("recommended_entry")) if _get_val("recommended_entry") is not None else None,
        "has_pullback_plan": _has_pullback_plan,

        # حقول تتبع تفعيل البولباك (v3.3)
        "pullback_triggered": _pullback_triggered,
        "effective_entry": _effective_entry,
        "activated_at": None,
        "activated_candle_ts": None,
        "pending_pullback_expired": False,
        "pending_pullback_expire_reason": "",
    }

    trade_key = get_trade_key(market_type, side, symbol, normalized_candle_time)
    history_key = get_trade_history_key(market_type, side, symbol, normalized_candle_time)
    open_set_key = get_open_trades_set_key(market_type, side)
    all_trades_key = get_all_trades_set_key()

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
        candle_time = safe_timestamp(trade_data.get("candle_time"), 0)
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


def mark_tp2_hit(redis_client, trade_key: str, trade_data: dict, processed_candle_ts: int = None):
    """
    تسجيل وصول السعر إلى TP2 وتحويل SL إلى TP1 (trailing stop).
    لا يغلق الصفقة، بل يبقيها مفتوحة بحالة tp2_partial.
    """
    if redis_client is None:
        return False
    if trade_data.get("tp2_hit"):
        return True
    try:
        if processed_candle_ts is not None:
            trade_data["last_processed_candle_ts"] = safe_timestamp(
                processed_candle_ts,
                trade_data.get("last_processed_candle_ts", 0)
            )
        side = normalize_side(trade_data.get("side", "long"))
        tp1 = safe_float(trade_data["tp1"])
        current_sl = safe_float(trade_data["sl"])

        trade_data["tp2_hit"] = True
        trade_data["tp2_hit_at"] = int(time.time())
        trade_data["status"] = "tp2_partial"
        trade_data["sl_moved_to_tp1"] = True
        trade_data["sl_move_reason"] = "TP2 hit - SL moved to TP1"       # إضافة سبب الحركة
        if side == "long":
            trade_data["sl"] = max(current_sl, tp1)
        else:
            trade_data["sl"] = min(current_sl, tp1)

        diagnostics = trade_data.get("diagnostics", {}) or {}
        diagnostics["tp2_hit"] = True
        diagnostics["tp2_hit_at"] = trade_data["tp2_hit_at"]
        diagnostics["sl_moved_to_tp1"] = True
        diagnostics["sl_move_reason"] = "TP2 hit - SL moved to TP1"      # إضافة في diagnostics
        trade_data["diagnostics"] = diagnostics

        market_type = normalize_market_type(trade_data.get("market_type", "futures"))
        stats_key = get_stats_key(market_type, side)
        redis_client.hincrby(stats_key, "tp2_hits", 1)

        ok = save_trade(redis_client, trade_key, trade_data)
        update_trade_history_snapshot(redis_client, trade_data)
        return ok
    except Exception as e:
        logger.error(f"mark_tp2_hit error on {trade_key}: {e}")
        return False


def mark_trade_closed(redis_client, trade_key: str, trade_data: dict, result: str):
    if redis_client is None:
        return False

    market_type = normalize_market_type(trade_data.get("market_type", "futures"))
    side = normalize_side(trade_data.get("side", "long"))
    open_set_key = get_open_trades_set_key(market_type, side)

    # معالجة breakeven / tp1_win عند ضرب SL قريب من effective_entry
    effective_entry = get_trade_effective_entry(trade_data)
    if result == "loss":
        sl = safe_float(trade_data.get("sl"), 0.0)
        if effective_entry > 0:
            diff_pct = abs(sl - effective_entry) / effective_entry * 100
            if diff_pct <= 0.03:
                if trade_data.get("tp1_hit"):
                    result = "tp1_win"
                else:
                    result = "breakeven"
                trade_data["protected_breakeven_exit"] = True
                if not trade_data.get("exit_reason"):
                    trade_data["exit_reason"] = "breakeven_protected_sl"

    try:
        trade_data["status"] = "closed"
        trade_data["result"] = result
        trade_data["closed_at"] = int(time.time())
        trade_data["updated_at"] = int(time.time())

        if result == "breakeven":
            trade_data["protected_breakeven_exit"] = True
            if not trade_data.get("exit_reason"):
                trade_data["exit_reason"] = "breakeven_exit"
        elif result == "pending_expired":
            trade_data["pending_pullback_expired"] = True
            trade_data["pending_pullback_expire_reason"] = trade_data.get(
                "pending_pullback_expire_reason",
                "not_triggered_within_max_age"
            )

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
        elif result == "pending_expired":
            redis_client.hincrby(stats_key, "pending_expired", 1)

        update_trade_history_snapshot(redis_client, trade_data)
        return True

    except Exception as e:
        logger.error(f"mark_trade_closed error on {trade_key}: {e}")
        return False


def mark_tp1_hit(redis_client, trade_key: str, trade_data: dict, processed_candle_ts: int = None):
    if redis_client is None:
        return False
    try:
        if trade_data.get("tp1_hit"):
            return True

        if processed_candle_ts is not None:
            trade_data["last_processed_candle_ts"] = safe_timestamp(
                processed_candle_ts,
                trade_data.get("last_processed_candle_ts", 0)
            )

        diagnostics = trade_data.get("diagnostics", {}) or {}
        effective_entry = get_trade_effective_entry(trade_data)

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

        move_sl = normalize_bool(
            trade_data.get("move_sl_to_entry_after_tp1",
                          diagnostics.get("move_sl_to_entry_after_tp1", True))
        )
        if move_sl:
            new_sl = round_price(effective_entry) if effective_entry > 0 else trade_data["entry"]
            trade_data["sl"] = new_sl
            trade_data["sl_moved_to_entry"] = True
            trade_data["sl_move_reason"] = "TP1 hit - protect remaining position"
        else:
            trade_data["sl_moved_to_entry"] = False
            trade_data["sl_move_reason"] = "TP1 hit - SL unchanged by config"

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
# TRADE EVALUATION (per candle)
# ------------------------------------------------------------
def evaluate_trade_on_candle(trade: dict, candle: dict):
    """
    تقييم الصفقة على شمعة واحدة. لا يغلق تلقائياً عند TP2، فقط يسجل الحاجة لتفعيل mark_tp2_hit.
    """
    side = normalize_side(trade.get("side", "long"))
    diagnostics = trade.get("diagnostics", {}) or {}

    entry = safe_float(trade.get("entry"), 0.0)
    effective_entry = get_trade_effective_entry(trade)

    sl = safe_float(trade["sl"])
    tp1 = safe_float(trade["tp1"])
    tp2 = safe_float(trade["tp2"])
    tp1_hit = bool(trade.get("tp1_hit", False))
    tp2_hit = bool(trade.get("tp2_hit", False))

    low = safe_float(candle["low"])
    high = safe_float(candle["high"])

    result = None
    tp1_now = False
    tp2_now = False  # لتحديد الحاجة لmark_tp2_hit

    pullback_entry = diagnostics.get("pullback_entry")
    pullback_triggered = normalize_bool(trade.get("pullback_triggered", diagnostics.get("pullback_triggered", False)))
    entry_mode = str(trade.get("entry_mode", diagnostics.get("entry_mode", "market")) or "market")
    has_pullback_plan_flag = normalize_bool(trade.get("has_pullback_plan", diagnostics.get("has_pullback_plan", False)))

    # منطق pullback pending
    if (
        side == "long"
        and entry_mode == "pullback_pending"
        and has_pullback_plan_flag
        and not pullback_triggered
    ):
        pb_entry = safe_float(pullback_entry, 0.0)
        if pb_entry <= 0:
            trade["status"] = "open"
            trade["entry_mode"] = "market"
        elif low <= pb_entry:
            now_ts = int(time.time())
            candle_ts = safe_timestamp(candle["ts"])
            trade["pullback_triggered"] = True
            trade["entry_mode"] = "pullback_triggered"
            trade["effective_entry"] = round_price(pb_entry)
            trade["activated_at"] = now_ts
            trade["activated_candle_ts"] = candle_ts
            diagnostics["pullback_triggered"] = True
            diagnostics["entry_mode"] = "pullback_triggered"
            diagnostics["effective_entry"] = round_price(pb_entry)
            diagnostics["activated_at"] = now_ts
            diagnostics["activated_candle_ts"] = candle_ts
            trade["status"] = "open"
            trade["diagnostics"] = diagnostics
            trade = recalc_targets_from_effective_entry(trade, pb_entry)
            effective_entry = pb_entry
            tp1 = safe_float(trade["tp1"])
            tp2 = safe_float(trade["tp2"])
            pullback_triggered = True
        else:
            trade["status"] = "pending_pullback"
            diagnostics["entry_mode"] = "pullback_pending"
            diagnostics["pullback_triggered"] = False
            trade["diagnostics"] = diagnostics
            return None, False, trade

    # التقييم العادي
    if side == "long":
        if not tp1_hit:
            if low <= sl:
                result = "loss"
            elif high >= tp1:
                tp1_now = True
                if high >= tp2 and not tp2_hit:
                    tp2_now = True
        else:
            if not tp2_hit and high >= tp2:
                tp2_now = True
            if low <= sl:
                if tp2_hit:
                    result = "tp2_win"
                else:
                    result = "tp1_win"
    else:  # short
        if not tp1_hit:
            if high >= sl:
                result = "loss"
            elif low <= tp1:
                tp1_now = True
                if low <= tp2 and not tp2_hit:
                    tp2_now = True
        else:
            if not tp2_hit and low <= tp2:
                tp2_now = True
            if high >= sl:
                if tp2_hit:
                    result = "tp2_win"
                else:
                    result = "tp1_win"

    # نمرر إشارة tp2_now لتُنفذ في update_open_trades، ولا نغلق
    trade["_tp2_now"] = tp2_now
    return result, tp1_now, trade


# ------------------------------------------------------------
# UPDATE OPEN TRADES
# ------------------------------------------------------------
def update_open_trades(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    timeframe: str = "15m",
    max_age_hours: int = 24,
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

        # --- استخراج trade_side الخاص بالصفقة الحالية ---
        trade_side = normalize_side(trade.get("side", side))

        symbol = trade["symbol"]
        created_at = safe_timestamp(trade.get("created_at", now_ts))

        # --- منطق انتهاء الصلاحية الجديد ---
        if now_ts - created_at > max_age_seconds:
            if trade.get("status") == "pending_pullback" and not trade.get("pullback_triggered"):
                trade["pending_pullback_expired"] = True
                trade["pending_pullback_expire_reason"] = "not_triggered_within_max_age"
                trade.pop("_tp2_now", None)
                mark_trade_closed(redis_client, trade_key, trade, "pending_expired")
                logger.info(f"{symbol} → pending pullback expired without activation")
            elif normalize_bool(trade.get("tp2_hit", False)):
                trade.pop("_tp2_now", None)
                mark_trade_closed(redis_client, trade_key, trade, "tp2_win")
                logger.info(f"{symbol} → trade expired after TP2, closed as tp2_win")
            elif normalize_bool(trade.get("tp1_hit", False)):
                trade.pop("_tp2_now", None)
                mark_trade_closed(redis_client, trade_key, trade, "tp1_win")
                logger.info(f"{symbol} → trade expired after TP1, closed as tp1_win")
            else:
                trade.pop("_tp2_now", None)
                mark_trade_closed(redis_client, trade_key, trade, "expired")
                logger.info(f"{symbol} → trade expired")
            continue

        raw_candles = fetch_recent_candles(symbol, timeframe=timeframe, limit=100)
        candles = normalize_candles(raw_candles)
        if not candles:
            continue

        current_price = candles[-1]["close"]

        last_proc = safe_timestamp(trade.get("last_processed_candle_ts"), 0)
        candle_ts_base = safe_timestamp(trade.get("candle_time"), 0)
        eval_start_ts = last_proc if last_proc > 0 else (candle_ts_base if candle_ts_base > 0 else created_at)

        new_candles = []
        for c in candles:
            c_ts = safe_timestamp(c["ts"])
            if c_ts > eval_start_ts:
                new_candles.append((c_ts, c))
        new_candles.sort(key=lambda x: x[0])

        result = None
        state_changed = False

        for c_ts, candle in new_candles:
            trade["last_processed_candle_ts"] = c_ts

            result, tp1_now, updated_trade = evaluate_trade_on_candle(trade, candle)
            trade = updated_trade

            # معالجة TP1
            if tp1_now and not trade.get("tp1_hit"):
                trade.pop("_tp2_now", None)  # إزالة المفتاح المؤقت قبل الحفظ
                ok = mark_tp1_hit(redis_client, trade_key, trade, processed_candle_ts=c_ts)
                if ok:
                    trade = load_trade(redis_client, trade_key) or trade
                    state_changed = True
                    logger.info(f"{symbol} → TP1 hit")
                else:
                    logger.error(f"{symbol} → failed to mark TP1")
                    break

                # بعد TP1، ربما TP2 أيضاً في نفس الشمعة
                if trade_side == "long" and safe_float(candle["high"], 0.0) >= safe_float(trade["tp2"], 0.0):
                    if not trade.get("tp2_hit"):
                        trade.pop("_tp2_now", None)
                        ok2 = mark_tp2_hit(redis_client, trade_key, trade, processed_candle_ts=c_ts)
                        if ok2:
                            trade = load_trade(redis_client, trade_key) or trade
                            state_changed = True
                            logger.info(f"{symbol} → TP2 hit right after TP1")
                elif trade_side == "short" and safe_float(candle["low"], 0.0) <= safe_float(trade["tp2"], 0.0):
                    if not trade.get("tp2_hit"):
                        trade.pop("_tp2_now", None)
                        ok2 = mark_tp2_hit(redis_client, trade_key, trade, processed_candle_ts=c_ts)
                        if ok2:
                            trade = load_trade(redis_client, trade_key) or trade
                            state_changed = True
                            logger.info(f"{symbol} → TP2 hit right after TP1")
                if result:
                    break
            else:
                # فحص TP2 عند عدم TP1_now (بعد TP1 سابق)
                tp2_now = trade.pop("_tp2_now", False)
                if tp2_now and not trade.get("tp2_hit"):
                    trade.pop("_tp2_now", None)  # مضمون الحذف
                    ok = mark_tp2_hit(redis_client, trade_key, trade, processed_candle_ts=c_ts)
                    if ok:
                        trade = load_trade(redis_client, trade_key) or trade
                        state_changed = True
                        logger.info(f"{symbol} → TP2 hit")
                # pullback activation save
                if trade.get("pullback_triggered") and not state_changed:
                    trade.pop("_tp2_now", None)
                    save_trade(redis_client, trade_key, trade)
                    update_trade_history_snapshot(redis_client, trade)
                    state_changed = True

            if result:
                break

        # حماية breakeven باستخدام trade_side
        if not result and trade.get("status") not in ("closed",):
            if trade.get("last_processed_candle_ts") != (eval_start_ts if eval_start_ts > 0 else None):
                state_changed = True

            mode_upper = str(market_mode or "").upper()
            block_this_side = (
                "BLOCK_ALL" in mode_upper
                or mode_upper.strip() == "BLOCK"
                or (side == "long" and "BLOCK_LONGS" in mode_upper)
                or (side == "short" and "BLOCK_SHORTS" in mode_upper)
            )

            if protect_breakeven_on_block and block_this_side:
                try:
                    entry_price = get_trade_effective_entry(trade)
                    if entry_price > 0:
                        if trade_side == "long":
                            current_profit_pct = ((current_price - entry_price) / entry_price) * 100
                        else:
                            current_profit_pct = ((entry_price - current_price) / entry_price) * 100

                        if current_profit_pct >= breakeven_min_profit_pct:
                            if not trade.get("protected_breakeven"):
                                current_sl = safe_float(trade.get("sl"), 0.0)
                                need_move = True
                                if trade_side == "long" and current_sl >= entry_price:
                                    need_move = False
                                elif trade_side == "short" and current_sl <= entry_price:
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
                                    state_changed = True
                                    logger.info(f"{symbol} → breakeven protected")
                except Exception as e:
                    logger.warning(f"Breakeven protection check failed for {symbol}: {e}")

        # حفظ التغييرات النهائية
        if state_changed and trade.get("status") not in ("closed",):
            trade.pop("_tp2_now", None)  # تأكيد حذف المفتاح قبل الحفظ
            save_trade(redis_client, trade_key, trade)
            update_trade_history_snapshot(redis_client, trade)

        if result:
            trade.pop("_tp2_now", None)
            mark_trade_closed(redis_client, trade_key, trade, result)
            logger.info(f"{symbol} → trade closed as {result}")


# ------------------------------------------------------------
# LOAD TRADES (with/without history)
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
                    ts = safe_timestamp(trade.get("candle_time"), 0)
                if ts < since_ts:
                    continue

            if not include_open:
                status = str(trade.get("status", "") or "").strip().lower()
                if status in ("open", "partial", "pending_pullback", "tp2_partial"):
                    continue

            trades.append(trade)

    except Exception as e:
        logger.error(f"load_trades error: {e}")

    return trades


def load_trades_with_history(
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
    pattern_trade = f"trade:{mt}:{sd}:*"
    pattern_history = f"trade_history:{mt}:{sd}:*"

    trades_dict = {}

    def _dedup_key(trade_dict):
        m = normalize_market_type(trade_dict.get("market_type", "futures"))
        s = normalize_side(trade_dict.get("side", "long"))
        sym = trade_dict.get("symbol", "")
        ctime = safe_timestamp(trade_dict.get("candle_time"), 0)
        return (m, s, sym, ctime)

    try:
        for key in redis_client.scan_iter(pattern_trade):
            raw = redis_client.get(key)
            if not raw:
                continue
            try:
                trade = json.loads(raw)
            except Exception:
                continue
            trades_dict[_dedup_key(trade)] = trade
    except Exception as e:
        logger.error(f"load_trades_with_history trade scan error: {e}")

    try:
        for key in redis_client.scan_iter(pattern_history):
            raw = redis_client.get(key)
            if not raw:
                continue
            try:
                trade = json.loads(raw)
            except Exception:
                continue
            dedup = _dedup_key(trade)
            if dedup not in trades_dict:
                trades_dict[dedup] = trade
    except Exception as e:
        logger.error(f"load_trades_with_history history scan error: {e}")

    trades = []
    for trade in trades_dict.values():
        if since_ts is not None:
            ts = get_trade_created_ts(trade)
            if not ts:
                ts = safe_timestamp(trade.get("candle_time"), 0)
            if ts < since_ts:
                continue

        if not include_open:
            status = str(trade.get("status", "") or "").strip().lower()
            if status in ("open", "partial", "pending_pullback", "tp2_partial"):
                continue

        trades.append(trade)

    trades.sort(
        key=lambda x: (
            get_trade_created_ts(x) or safe_timestamp(x.get("candle_time"), 0)
        ),
        reverse=True,
    )

    return trades


def get_all_trades_data(
    redis_client,
    market_type: str = None,
    side: str = None,
    since_ts: int = None,
    use_history: bool = False,
):
    if redis_client is None:
        return []

    try:
        cleanup_missing_trades_from_index(redis_client)
    except Exception:
        pass

    if use_history:
        return load_trades_with_history(
            redis_client=redis_client,
            market_type=market_type,
            side=side,
            since_ts=since_ts,
            include_open=True,
        )

    return load_trades(
        redis_client=redis_client,
        market_type=market_type,
        side=side,
        since_ts=since_ts,
        include_open=True,
    )


# ------------------------------------------------------------
# حساب PnL مرجح للصفقة (للإصلاحات)
# ------------------------------------------------------------
def _compute_weighted_trade_pnl(trade: dict) -> float:
    """
    حساب الربح/الخسارة بعد الرافعة لصفقة مغلقة، مع الأخذ في الاعتبار
    المخارج الجزئية (tp1_close_pct, tp2_close_pct).
    تُرجع النسبة المئوية بعد الرافعة.
    """
    side = normalize_side(trade.get("side", "long"))
    leverage = SHORT_REPORT_LEVERAGE if side == "short" else REPORT_LEVERAGE
    effective_entry = get_trade_effective_entry(trade)
    result = trade.get("result", "")

    if not result or result in ("expired", "pending_expired"):
        return 0.0

    tp1_close_pct = safe_float(trade.get("tp1_close_pct", 50.0), 50.0) / 100.0
    tp2_close_pct = safe_float(trade.get("tp2_close_pct", 50.0), 50.0) / 100.0
    tp1_hit = normalize_bool(trade.get("tp1_hit", False))
    tp2_hit = normalize_bool(trade.get("tp2_hit", False))
    tp1 = safe_float(trade.get("tp1"), 0.0)
    tp2 = safe_float(trade.get("tp2"), 0.0)
    sl = safe_float(trade.get("sl"), 0.0)

    def long_pct(entry, exit_price):
        if entry <= 0: return 0.0
        return ((exit_price - entry) / entry) * 100.0
    def short_pct(entry, exit_price):
        if entry <= 0: return 0.0
        return ((entry - exit_price) / entry) * 100.0
    calc = long_pct if side == "long" else short_pct

    if result == "tp2_win":
        # جزء TP1 + جزء TP2 + باقي عند SL (الذي تم رفعه)
        remaining_pct = max(0.0, 1.0 - tp1_close_pct - tp2_close_pct)
        pnl_tp1 = calc(effective_entry, tp1) * tp1_close_pct if tp1_hit else 0.0
        pnl_tp2 = calc(effective_entry, tp2) * tp2_close_pct  # لأن tp2_hit صحيح
        pnl_rem = calc(effective_entry, sl) * remaining_pct
        pnl_raw = pnl_tp1 + pnl_tp2 + pnl_rem
    elif result == "tp1_win":
        remaining_pct = max(0.0, 1.0 - tp1_close_pct)
        pnl_tp1 = calc(effective_entry, tp1) * tp1_close_pct if tp1_hit else 0.0
        pnl_rem = calc(effective_entry, sl) * remaining_pct
        pnl_raw = pnl_tp1 + pnl_rem
    elif result == "loss":
        pnl_raw = calc(effective_entry, sl)
    elif result == "breakeven":
        if tp1_hit:
            pnl_tp1 = calc(effective_entry, tp1) * tp1_close_pct
            pnl_raw = pnl_tp1
        else:
            pnl_raw = 0.0
    else:
        pnl_raw = 0.0

    return pnl_raw * leverage


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

            if trade.get("status") == "pending_pullback":
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
            "wins": 0, "tp1_wins": 0, "tp2_wins": 0, "losses": 0,
            "expired": 0, "open": 0, "closed": 0, "tp1_hits": 0,
            "tp1_rate": 0.0, "winrate": 0.0, "market_type": market_type,
            "side": side, "leverage": leverage,
            "realized_raw_pnl_pct": 0.0, "realized_leveraged_pnl_pct": 0.0,
            "realized_pnl_pct": 0.0, "gross_profit_pct": 0.0,
            "gross_loss_pct": 0.0, "avg_win_pct": 0.0, "avg_loss_pct": 0.0,
            "best_trade_pct": 0.0, "worst_trade_pct": 0.0,
            "risk_status": "normal", "tp2_hits": 0, "tp2_rate": 0.0,
            "tp1_to_tp2_rate": 0.0, "net_profit_pct": 0.0,
            "breakeven_exits": 0,
        }

    stats_key = get_stats_key(market_type, side)
    open_set_key = get_open_trades_set_key(market_type, side)

    try:
        stats = redis_client.hgetall(stats_key) or {}
        wins = safe_int(redis_hash_get(stats, "wins", 0))
        losses = safe_int(redis_hash_get(stats, "losses", 0))
        expired = safe_int(redis_hash_get(stats, "expired", 0))
        open_count = safe_int(redis_client.scard(open_set_key) or 0)

        financial_summary = get_trade_summary(
            redis_client,
            market_type=market_type,
            side=side,
        )

        wins = safe_int(financial_summary.get("wins", wins))
        losses = safe_int(financial_summary.get("losses", losses))
        expired = safe_int(financial_summary.get("expired", expired))
        open_count = safe_int(financial_summary.get("open", open_count))
        breakeven_exits = safe_int(financial_summary.get("breakeven_exits", 0))
        tp1_hits = safe_int(financial_summary.get("tp1_hits", 0))
        tp2_hits_total = safe_int(financial_summary.get("tp2_hits", 0))
        tp1_wins = safe_int(financial_summary.get("tp1_wins", 0))
        tp2_wins = safe_int(financial_summary.get("tp2_wins", 0))

        decided = wins + losses
        closed = wins + losses + expired + breakeven_exits
        total_signals = closed + open_count

        winrate = round((wins / decided) * 100, 2) if decided > 0 else 0.0
        tp1_rate = round((tp1_hits / total_signals) * 100, 2) if total_signals > 0 else 0.0
        tp2_rate = round((tp2_hits_total / total_signals) * 100, 2) if total_signals > 0 else 0.0
        tp1_to_tp2_rate = round((tp2_hits_total / tp1_hits) * 100, 2) if tp1_hits > 0 else 0.0

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
            "tp2_hits": tp2_hits_total,
            "tp2_rate": tp2_rate,
            "tp1_to_tp2_rate": tp1_to_tp2_rate,
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
            "net_profit_pct": safe_float(financial_summary.get("net_profit_pct", 0.0)),
            "breakeven_exits": breakeven_exits,
        }

    except Exception as e:
        logger.error(f"get_winrate_summary error: {e}")
        return {
            "wins": 0, "tp1_wins": 0, "tp2_wins": 0, "losses": 0,
            "expired": 0, "open": 0, "closed": 0, "tp1_hits": 0,
            "tp1_rate": 0.0, "winrate": 0.0, "market_type": market_type,
            "side": side, "leverage": leverage,
            "realized_raw_pnl_pct": 0.0, "realized_leveraged_pnl_pct": 0.0,
            "realized_pnl_pct": 0.0, "gross_profit_pct": 0.0,
            "gross_loss_pct": 0.0, "avg_win_pct": 0.0, "avg_loss_pct": 0.0,
            "best_trade_pct": 0.0, "worst_trade_pct": 0.0,
            "risk_status": "normal", "tp2_hits": 0, "tp2_rate": 0.0,
            "tp1_to_tp2_rate": 0.0, "net_profit_pct": 0.0,
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

    trades = load_trades_with_history(
        redis_client,
        market_type=market_type,
        side=side_norm,
        since_ts=since_ts,
        include_open=True,
    )

    # فلترة pending_pullback
    filtered_trades = [t for t in trades if t.get("status") != "pending_pullback"]

    summary = summarize_trades(filtered_trades)
    summary["market_type"] = market_type or "futures"
    summary["side"] = side_norm

    closed_trades = [t for t in filtered_trades if t.get("result") in ("tp1_win", "tp2_win", "loss", "breakeven")]
    gross_profit = 0.0
    gross_loss = 0.0
    total_pnl = 0.0
    win_pnls = []
    loss_pnls = []
    all_pnls = []

    # عدادات TP2 بناءً على الحقل tp2_hit
    tp2_hits_count = sum(1 for t in filtered_trades if normalize_bool(t.get("tp2_hit")))
    tp2_wins_count = sum(1 for t in closed_trades if t.get("result") == "tp2_win")

    for trade in closed_trades:
        pnl = _compute_weighted_trade_pnl(trade)
        all_pnls.append(pnl)
        total_pnl += pnl
        if pnl > 0:
            gross_profit += pnl
            win_pnls.append(pnl)
        elif pnl < 0:
            gross_loss += abs(pnl)
            loss_pnls.append(pnl)

    summary["realized_leveraged_pnl_pct"] = round(total_pnl, 4)
    summary["gross_profit_pct"] = round(gross_profit, 4)
    summary["gross_loss_pct"] = round(gross_loss, 4)
    summary["net_profit_pct"] = round(total_pnl, 4)
    summary["avg_win_pct"] = round(sum(win_pnls) / len(win_pnls), 4) if win_pnls else 0.0
    summary["avg_loss_pct"] = round(sum(loss_pnls) / len(loss_pnls), 4) if loss_pnls else 0.0
    summary["best_trade_pct"] = round(max(all_pnls), 4) if all_pnls else 0.0
    summary["worst_trade_pct"] = round(min(all_pnls), 4) if all_pnls else 0.0

    leverage = SHORT_REPORT_LEVERAGE if side_norm == "short" else REPORT_LEVERAGE
    summary["realized_raw_pnl_pct"] = round(total_pnl / leverage, 4) if leverage else 0.0

    # إضافة عدادات TP2 محدثة
    summary["tp2_hits"] = tp2_hits_count
    summary["tp2_wins"] = tp2_wins_count

    # --- حساب TP2 rates ---
    signals = safe_int(summary.get("signals", 0))
    tp1_hits = safe_int(summary.get("tp1_hits", 0))
    summary["tp2_rate"] = round((tp2_hits_count / signals) * 100, 2) if signals > 0 else 0.0
    summary["tp1_to_tp2_rate"] = round((tp2_hits_count / tp1_hits) * 100, 2) if tp1_hits > 0 else 0.0

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
            0, 0, 0,
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

    trades = get_all_trades_data(
        redis_client,
        market_type=market_type,
        side=side,
        since_ts=since_ts,
        use_history=use_history,
    )
    # استبعاد pending_pullback
    trades = [t for t in trades if t.get("status") != "pending_pullback"]

    exit_data = summarize_exits(trades)
    exit_data["market_type"] = market_type
    exit_data["side"] = side
    exit_data["trades_count"] = len(trades)
    exit_data["since_ts"] = since_ts
    # تحديث TP2 hits بناءً على الحقل
    exit_data["tp2_hits"] = sum(1 for t in trades if normalize_bool(t.get("tp2_hit")))

    # --- حساب TP2 rates ---
    signals = safe_int(exit_data.get("signals", exit_data.get("trades_count", 0)))
    tp1_hits = safe_int(exit_data.get("tp1_hits", 0))
    tp2_hits = safe_int(exit_data.get("tp2_hits", 0))
    exit_data["tp2_rate"] = round((tp2_hits / signals) * 100, 2) if signals > 0 else 0.0
    exit_data["tp1_to_tp2_rate"] = round((tp2_hits / tp1_hits) * 100, 2) if tp1_hits > 0 else 0.0

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

    trades = get_all_trades_data(
        redis_client,
        market_type=market_type,
        side=side,
        use_history=use_history,
    )
    # استبعاد pending_pullback
    trades = [t for t in trades if t.get("status") != "pending_pullback"]

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

    trades = get_all_trades_data(
        redis_client,
        market_type=market_type,
        side=side,
        use_history=use_history,
    )
    trades = [t for t in trades if t.get("status") != "pending_pullback"]

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
    rows = [t for t in rows if t.get("status") != "pending_pullback"]

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
        trades = load_trades_with_history(
            redis_client,
            market_type=market_type,
            side=side,
            since_ts=since_ts,
            include_open=False,
        )
    trades = [t for t in trades if t.get("status") != "pending_pullback"]

    reasons_counter = Counter()

    for trade in trades:
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
    closed = wins + losses + expired + breakeven_exits

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
    tp2_hits = safe_int(summary.get("tp2_hits", 0))
    tp1_only = safe_int(summary.get("tp1_then_entry", summary.get("tp1_only", summary.get("tp1_wins", 0))))
    full_wins = safe_int(summary.get("tp2_wins", 0))
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

    # عرض الخسارة بالسالب
    gross_loss_display = -gross_loss
    gross_loss_usd_display = -gross_loss_usd

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
        f"🔴 <b>إجمالي الخسارة:</b> {gross_loss_display:+.2f}% = {gross_loss_usd_display:+.2f}$\n"
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
        tp2_hits = safe_int(summary.get("tp2_hits", 0))
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


# ------------------------------------------------------------
# PULLBACK PENDING TRACKING
# ------------------------------------------------------------
def get_pending_pullback_summary(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    since_ts: Optional[int] = None,
    use_history: bool = True,
) -> dict:
    market_type = normalize_market_type(market_type)
    side = normalize_side(side)
    trades = get_all_trades_data(
        redis_client,
        market_type=market_type,
        side=side,
        since_ts=since_ts,
        use_history=use_history,
    )
    pullback_trades = []
    for trade in trades:
        entry_mode = str(trade.get("entry_mode", "") or "").lower()
        has_plan = normalize_bool(trade.get("has_pullback_plan", False))
        if has_plan or entry_mode.startswith("pullback"):
            pullback_trades.append(trade)

    total_plans = len(pullback_trades)
    pending_now = sum(1 for t in pullback_trades if t.get("status") == "pending_pullback")
    triggered_count = sum(
        1 for t in pullback_trades
        if normalize_bool(t.get("pullback_triggered", False))
        or str(t.get("entry_mode", "")).lower() == "pullback_triggered"
    )
    expired_pending_count = sum(1 for t in pullback_trades if t.get("result") == "pending_expired")

    triggered_closed_trades = [
        t for t in pullback_trades
        if (normalize_bool(t.get("pullback_triggered", False)) or
            str(t.get("entry_mode", "")).lower() == "pullback_triggered")
        and t.get("result") in ("tp1_win", "tp2_win", "loss", "breakeven")
    ]
    triggered_wins = sum(1 for t in triggered_closed_trades if t.get("result") in ("tp1_win", "tp2_win"))
    triggered_losses = sum(1 for t in triggered_closed_trades if t.get("result") == "loss")
    triggered_tp1_hits = sum(1 for t in triggered_closed_trades if normalize_bool(t.get("tp1_hit")))
    triggered_tp2_hits = sum(1 for t in triggered_closed_trades if normalize_bool(t.get("tp2_hit")))
    decided_triggered = triggered_wins + triggered_losses
    triggered_winrate = (triggered_wins / decided_triggered * 100) if decided_triggered > 0 else 0.0

    delays = []
    for t in pullback_trades:
        activated_at = safe_timestamp(t.get("activated_at"))
        created_at = safe_timestamp(t.get("created_at"))
        if activated_at > 0 and created_at > 0:
            delays.append((activated_at - created_at) / 60.0)
    avg_delay = round(sum(delays) / len(delays), 1) if delays else 0.0

    best_pct = 0.0
    worst_pct = 0.0
    for t in triggered_closed_trades:
        pnl = _compute_weighted_trade_pnl(t)
        if pnl > best_pct:
            best_pct = pnl
        if pnl < worst_pct:
            worst_pct = pnl

    triggered_rate = (triggered_count / total_plans * 100) if total_plans > 0 else 0.0
    expired_rate = (expired_pending_count / total_plans * 100) if total_plans > 0 else 0.0

    return {
        "market_type": market_type,
        "side": side,
        "total_pullback_plans": total_plans,
        "pending_now": pending_now,
        "triggered_count": triggered_count,
        "expired_pending_count": expired_pending_count,
        "triggered_rate": round(triggered_rate, 2),
        "expired_rate": round(expired_rate, 2),
        "triggered_wins": triggered_wins,
        "triggered_losses": triggered_losses,
        "triggered_tp1_hits": triggered_tp1_hits,
        "triggered_tp2_hits": triggered_tp2_hits,
        "triggered_winrate": round(triggered_winrate, 2),
        "avg_trigger_delay_minutes": avg_delay,
        "best_triggered_trade_pct": best_pct,
        "worst_triggered_trade_pct": worst_pct,
    }


def format_pending_pullback_summary(title: str, summary: dict) -> str:
    total = summary["total_pullback_plans"]
    pending = summary["pending_now"]
    triggered = summary["triggered_count"]
    expired = summary["expired_pending_count"]
    triggered_rate = summary["triggered_rate"]
    expired_rate = summary["expired_rate"]
    wins = summary["triggered_wins"]
    losses = summary["triggered_losses"]
    tp1_hits = summary["triggered_tp1_hits"]
    tp2_hits = summary["triggered_tp2_hits"]
    winrate = summary["triggered_winrate"]
    avg_delay = summary["avg_trigger_delay_minutes"]
    best = summary["best_triggered_trade_pct"]
    worst = summary["worst_triggered_trade_pct"]

    lines = [
        f"<b>{title}</b>",
        "",
        f"📊 <b>إجمالي خطط البولباك:</b> {total}",
        f"⏳ <b>ما زالت معلّقة:</b> {pending}",
        f"✅ <b>تم التفعيل:</b> {triggered}",
        f"⌛ <b>انتهت بدون تفعيل:</b> {expired}",
        "",
        f"📈 <b>Trigger Rate:</b> {triggered_rate:.1f}%",
        f"📉 <b>Expired Rate:</b> {expired_rate:.1f}%",
        "",
        "<b>🎯 أداء الصفقات بعد التفعيل:</b>",
        f"• Wins: {wins}",
        f"• Losses: {losses}",
        f"• TP1 Hits: {tp1_hits}",
        f"• TP2 Hits: {tp2_hits}",
        f"• Winrate بعد التفعيل: {winrate:.1f}%",
    ]
    if avg_delay > 0:
        lines.append(f"⏱ <b>متوسط وقت التفعيل:</b> {avg_delay:.1f} دقيقة")
    if best != 0.0 or worst != 0.0:
        lines.append(f"• أفضل صفقة مفعلة: {best:+.2f}% | أسوأ صفقة: {worst:+.2f}%")
    lines.append("")
    lines.append("<b>🧠 التفسير:</b>")
    if total == 0:
        lines.append("• لا توجد بيانات كافية.")
    else:
        if triggered_rate < 30:
            lines.append("• ⚠️ Trigger Rate منخفض جدًا، منطقة البولباك بعيدة أو شروط الاختيار محتاجة مراجعة.")
        elif triggered_rate > 70:
            lines.append("• ✅ Trigger Rate ممتاز، مناطق الدخول قريبة وفعالة.")
        if expired_rate > 40:
            lines.append("• 🔴 Expired Rate عالي جدًا، يمكن تقليل صلاحية الصفقة أو تقريب pullback_entry.")
        if winrate < 40 and triggered > 0:
            lines.append("• 🔴 Winrate بعد التفعيل ضعيف، راجع جودة المنطقة أو SL/TP بعد الدخول.")
        elif winrate > 60 and triggered >= 5:
            lines.append("• ✅ Winrate بعد التفعيل جيد، استمر.")
        if avg_delay > 60:
            lines.append("• ⏳ متوسط وقت التفعيل طويل (> 60 دقيقة)، قد تحتاج صبر أكبر أو تعديل زمن الصلاحية.")
    return "\n".join(lines)
