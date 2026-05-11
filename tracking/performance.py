# tracking/performance.py
# Version: performance_v57_ui_intelligence_open_polish
# Base: performance_v49_report_command_html_fix
# Changes: UI/reporting only: stronger open-trades dashboard summary, compact spacing, smart sampling.
# Fix: header/version corrected; dashboard context fields are displayed in the report.
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

**إصدار 3.6 – إصلاح جذري لحساب الأداء وإلغاء الازدواجية**
- إصلاح TP1/TP2 hits و rate.
- منع تكرار الصفقات عند تحميلها من redis.
- تصحيح PnL واستخدام effective_entry.
- دعم trailing فعلي بعد TP2 مع النسب 40/40/20.
- Breakeven بعد TP1 يعتبر فوز جزئي وليس خسارة.
- Full Wins TP2 = عدد صفقات tp2_win الحقيقية.
- حقل alert_id/trade_id لتجنب التكرار.

**إصدار 3.7 – تثبيت حقول التنفيذ والتحليل**
- حفظ execution_entry/execution_sl/execution_tp1/execution_tp2 top-level وداخل diagnostics/history.
- حفظ block_exception وrelative_strength_* حتى تظهر في /report_execution و/ report_setups بعد Redis.
- حفظ nearest_resistance_source وخطة 40/40/20 كاملة بدون تغيير register_trade signature.
"""

import json
import html
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

# نسب الإغلاق الجزئي
TP1_CLOSE_PCT = 40.0
TP2_CLOSE_PCT = 40.0
TRAILING_PCT = 2.5   # نسبة التراجع للـ trailing stop بعد TP2 — موحد مع main.py
TP2_PROTECTED_SL_BUFFER_PCT = 0.0  # بعد TP2: حماية آخر 20% عند TP1 أو TP1+buffer

# Dedup index
ALERT_ID_INDEX_PREFIX = "alert_id_index:"
PENDING_PULLBACK_MAX_CANDLES = 30  # max شموع قبل انتهاء pending_pullback

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


def get_trade_field(trade: dict, key: str, default=None):
    """جلب قيمة من trade أو diagnostics بالأولوية لـ root."""
    if not isinstance(trade, dict):
        return default
    val = trade.get(key)
    if val is None:
        val = (trade.get("diagnostics") or {}).get(key)
    return val if val is not None else default


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
        "tp1_close_pct": safe_float(_get("tp1_close_pct"), TP1_CLOSE_PCT),
        "tp2_close_pct": safe_float(_get("tp2_close_pct"), TP2_CLOSE_PCT),
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

        # حقول التنفيذ والتحليل (v3.7)
        "execution_entry": round_price(_get("execution_entry")) if _get("execution_entry") is not None else None,
        "execution_sl": round_price(_get("execution_sl")) if _get("execution_sl") is not None else None,
        "execution_tp1": round_price(_get("execution_tp1")) if _get("execution_tp1") is not None else None,
        "execution_tp2": round_price(_get("execution_tp2")) if _get("execution_tp2") is not None else None,
        "execution_risk": safe_float(_get("execution_risk")) if _get("execution_risk") is not None else None,
        "block_exception": normalize_bool(_get("block_exception", False)),
        "relative_strength_short": safe_float(_get("relative_strength_short"), 0.0),
        "relative_strength_24": safe_float(_get("relative_strength_24"), 0.0),
        "relative_strength_vs_btc": normalize_bool(_get("relative_strength_vs_btc", False)),
        "nearest_resistance_source": normalize_text(_get("nearest_resistance_source"), ""),
        "tp1_plan_pct": safe_float(_get("tp1_plan_pct"), safe_float(_get("tp1_close_pct"), TP1_CLOSE_PCT)),
        "tp2_plan_pct": safe_float(_get("tp2_plan_pct"), safe_float(_get("tp2_close_pct"), TP2_CLOSE_PCT)),
        "trailing_position_pct": safe_float(_get("trailing_position_pct"), 20.0),

        # حقول تتبع تفعيل البولباك
        "activated_at": safe_int(_get("activated_at")) if _get("activated_at") is not None else None,
        "activated_candle_ts": safe_int(_get("activated_candle_ts")) if _get("activated_candle_ts") is not None else None,
        "pending_pullback_expired": normalize_bool(_get("pending_pullback_expired", False)),
        "pending_pullback_expire_reason": normalize_text(_get("pending_pullback_expire_reason"), ""),

        # trailing حقول
        "trailing_active": normalize_bool(_get("trailing_active", False)),
        "trailing_high": round_price(_get("trailing_high")) if _get("trailing_high") is not None else None,
        "trailing_low": round_price(_get("trailing_low")) if _get("trailing_low") is not None else None,
        "trailing_sl": round_price(_get("trailing_sl")) if _get("trailing_sl") is not None else None,
        "trailing_pct": safe_float(_get("trailing_pct"), TRAILING_PCT),
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

        "tp1_close_pct": safe_float(_get_field("tp1_close_pct"), TP1_CLOSE_PCT),
        "tp2_close_pct": safe_float(_get_field("tp2_close_pct"), TP2_CLOSE_PCT),
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

        # حقول التنفيذ والتحليل (v3.7)
        "execution_entry": _get_field("execution_entry"),
        "execution_sl": _get_field("execution_sl"),
        "execution_tp1": _get_field("execution_tp1"),
        "execution_tp2": _get_field("execution_tp2"),
        "execution_risk": _get_field("execution_risk"),
        "block_exception": normalize_bool(_get_field("block_exception", False)),
        "relative_strength_short": safe_float(_get_field("relative_strength_short"), 0.0),
        "relative_strength_24": safe_float(_get_field("relative_strength_24"), 0.0),
        "relative_strength_vs_btc": normalize_bool(_get_field("relative_strength_vs_btc", False)),
        "nearest_resistance_source": _get_field("nearest_resistance_source", ""),
        "tp1_plan_pct": safe_float(_get_field("tp1_plan_pct"), safe_float(_get_field("tp1_close_pct"), TP1_CLOSE_PCT)),
        "tp2_plan_pct": safe_float(_get_field("tp2_plan_pct"), safe_float(_get_field("tp2_close_pct"), TP2_CLOSE_PCT)),
        "trailing_position_pct": safe_float(_get_field("trailing_position_pct"), 20.0),

        # حقول تتبع تفعيل البولباك (v3.3)
        "activated_at": _get_field("activated_at"),
        "activated_candle_ts": _get_field("activated_candle_ts"),
        "pending_pullback_expired": normalize_bool(_get_field("pending_pullback_expired", False)),
        "pending_pullback_expire_reason": _get_field("pending_pullback_expire_reason", ""),

        # trailing حقول
        "trailing_active": normalize_bool(_get_field("trailing_active", False)),
        "trailing_high": _get_field("trailing_high"),
        "trailing_low": _get_field("trailing_low"),
        "trailing_sl": _get_field("trailing_sl"),
        "trailing_pct": safe_float(_get_field("trailing_pct"), TRAILING_PCT),

        "diagnostics": diagnostics,
    }

    return snapshot


# ------------------------------------------------------------
# TRADE STORAGE (register / load / save / mark)
# ------------------------------------------------------------
def check_alert_id_duplicate(redis_client, alert_id: str) -> bool:
    """يتحقق إذا alert_id موجود مسبقاً → True = مكرر."""
    if not redis_client or not alert_id:
        return False
    try:
        key = f"{ALERT_ID_INDEX_PREFIX}{alert_id}"
        return bool(redis_client.exists(key))
    except Exception:
        return False


def register_alert_id_index(redis_client, alert_id: str, trade_key: str):
    """يسجل alert_id → trade_key في Redis."""
    if not redis_client or not alert_id:
        return
    try:
        key = f"{ALERT_ID_INDEX_PREFIX}{alert_id}"
        redis_client.set(key, trade_key, ex=TRADE_TTL_SECONDS)
    except Exception as e:
        logger.warning(f"register_alert_id_index failed for {alert_id}: {e}")


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

    # ── Alert ID dedup (simplified) ────────────────────────────
    _check_alert_id = str(alert_id or kwargs.get("alert_id", "") or "").strip() or None

    if _check_alert_id and check_alert_id_duplicate(redis_client, _check_alert_id):
        logger.debug(f"register_trade: duplicate alert_id={_check_alert_id} for {symbol}, skipping")
        return False

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
        # حقول التنفيذ والتحليل (v3.7) — تأتي غالبًا من kwargs في main.py
        "execution_entry": kwargs.get("execution_entry"),
        "execution_sl": kwargs.get("execution_sl"),
        "execution_tp1": kwargs.get("execution_tp1"),
        "execution_tp2": kwargs.get("execution_tp2"),
        "execution_risk": kwargs.get("execution_risk"),
        "block_exception": kwargs.get("block_exception"),
        "relative_strength_short": kwargs.get("relative_strength_short"),
        "relative_strength_24": kwargs.get("relative_strength_24"),
        "relative_strength_vs_btc": kwargs.get("relative_strength_vs_btc"),
        "nearest_resistance_source": kwargs.get("nearest_resistance_source"),
        "tp1_plan_pct": kwargs.get("tp1_plan_pct"),
        "tp2_plan_pct": kwargs.get("tp2_plan_pct"),
        "trailing_position_pct": kwargs.get("trailing_position_pct"),
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
        "tp1_close_pct": _get_val("tp1_close_pct", TP1_CLOSE_PCT),
        "tp2_close_pct": _get_val("tp2_close_pct", TP2_CLOSE_PCT),
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
        # حقول التنفيذ والتحليل (v3.7)
        "execution_entry": _get_val("execution_entry"),
        "execution_sl": _get_val("execution_sl"),
        "execution_tp1": _get_val("execution_tp1"),
        "execution_tp2": _get_val("execution_tp2"),
        "execution_risk": _get_val("execution_risk"),
        "block_exception": _get_val("block_exception", False),
        "relative_strength_short": _get_val("relative_strength_short", 0.0),
        "relative_strength_24": _get_val("relative_strength_24", 0.0),
        "relative_strength_vs_btc": _get_val("relative_strength_vs_btc", False),
        "nearest_resistance_source": _get_val("nearest_resistance_source", ""),
        "tp1_plan_pct": _get_val("tp1_plan_pct", _get_val("tp1_close_pct", TP1_CLOSE_PCT)),
        "tp2_plan_pct": _get_val("tp2_plan_pct", _get_val("tp2_close_pct", TP2_CLOSE_PCT)),
        "trailing_position_pct": _get_val("trailing_position_pct", 20.0),
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
        # trailing
        "trailing_active": False,
        "trailing_high": None,
        "trailing_low": None,
        "trailing_sl": None,
        "trailing_pct": TRAILING_PCT,
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

        "tp1_close_pct": safe_float(_get_val("tp1_close_pct", TP1_CLOSE_PCT)),
        "tp2_close_pct": safe_float(_get_val("tp2_close_pct", TP2_CLOSE_PCT)),
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

        # حقول التنفيذ والتحليل top-level (v3.7)
        "execution_entry": round_price(_get_val("execution_entry")) if _get_val("execution_entry") is not None else None,
        "execution_sl": round_price(_get_val("execution_sl")) if _get_val("execution_sl") is not None else None,
        "execution_tp1": round_price(_get_val("execution_tp1")) if _get_val("execution_tp1") is not None else None,
        "execution_tp2": round_price(_get_val("execution_tp2")) if _get_val("execution_tp2") is not None else None,
        "execution_risk": safe_float(_get_val("execution_risk")) if _get_val("execution_risk") is not None else None,
        "block_exception": normalize_bool(_get_val("block_exception", False)),
        "relative_strength_short": safe_float(_get_val("relative_strength_short"), 0.0),
        "relative_strength_24": safe_float(_get_val("relative_strength_24"), 0.0),
        "relative_strength_vs_btc": normalize_bool(_get_val("relative_strength_vs_btc", False)),
        "nearest_resistance_source": _get_val("nearest_resistance_source", ""),
        "tp1_plan_pct": safe_float(_get_val("tp1_plan_pct", _get_val("tp1_close_pct", TP1_CLOSE_PCT))),
        "tp2_plan_pct": safe_float(_get_val("tp2_plan_pct", _get_val("tp2_close_pct", TP2_CLOSE_PCT))),
        "trailing_position_pct": safe_float(_get_val("trailing_position_pct", 20.0)),

        # حقول تتبع تفعيل البولباك (v3.3)
        "pullback_triggered": _pullback_triggered,
        "effective_entry": _effective_entry,
        "activated_at": None,
        "activated_candle_ts": None,
        "pending_pullback_expired": False,
        "pending_pullback_expire_reason": "",

        # trailing حقول
        "trailing_active": False,
        "trailing_high": None,
        "trailing_low": None,
        "trailing_sl": None,
        "trailing_pct": TRAILING_PCT,
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
        if created is None:
            logger.warning(
                f"register_trade: Redis returned None for {symbol} @ {normalized_candle_time} "
                f"— treating as failure (possible connection issue)"
            )
            return False
        if not created:
            logger.debug(f"register_trade: trade already exists for {symbol} @ {normalized_candle_time}")
            return False

        # إضافة للـ sets مع logging لو فشل
        try:
            redis_client.sadd(open_set_key, trade_key)
        except Exception as e:
            logger.error(f"register_trade: failed to add to open_set for {symbol}: {e}")

        try:
            redis_client.sadd(all_trades_key, trade_key)
        except Exception as e:
            logger.error(f"register_trade: failed to add to all_trades for {symbol}: {e}")

        history_data = build_history_snapshot(trade_data)
        redis_client.set(
            history_key,
            json.dumps(history_data, ensure_ascii=False),
            ex=TRADE_HISTORY_TTL_SECONDS,
        )
        logger.info(f"register_trade: ✅ {symbol} registered | key={trade_key} | status={_status}")

        # تسجيل alert_id index لمنع التكرار مستقبلاً
        if _check_alert_id:
            register_alert_id_index(redis_client, _check_alert_id, trade_key)

        return True

    except Exception as e:
        logger.error(f"register_trade error on {symbol}: {e}", exc_info=True)
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


def mark_tp2_hit(redis_client, trade_key: str, trade_data: dict, processed_candle_ts: int = None, candle: dict = None):
    """
    تسجيل وصول السعر إلى TP2 وتفعيل trailing stop للـ 20% المتبقية.
    النسب: 40% TP1، 40% TP2، 20% trailing.
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

        trade_data["tp2_hit"] = True
        trade_data["tp2_hit_at"] = int(time.time())
        trade_data["status"] = "tp2_partial"   # الجزء المتبقي في trailing
        trade_data["tp2_protected_runner"] = True
        trade_data["tp1_only_unprotected"] = False

        # After TP2, protect the remaining 20% at TP1 or TP1 + small buffer.
        _tp1_price_for_sl = safe_float(trade_data.get("tp1"), 0.0)
        _current_sl_for_tp2 = safe_float(trade_data.get("sl"), 0.0)
        _buffer = TP2_PROTECTED_SL_BUFFER_PCT / 100.0
        if _tp1_price_for_sl > 0:
            if side == "long":
                _tp2_protected_sl = round_price(_tp1_price_for_sl * (1.0 + _buffer))
                if _tp2_protected_sl > _current_sl_for_tp2:
                    trade_data["original_sl_before_tp2_protection"] = _current_sl_for_tp2
                    trade_data["sl"] = _tp2_protected_sl
            else:
                _tp2_protected_sl = round_price(_tp1_price_for_sl * (1.0 - _buffer))
                if _current_sl_for_tp2 <= 0 or _tp2_protected_sl < _current_sl_for_tp2:
                    trade_data["original_sl_before_tp2_protection"] = _current_sl_for_tp2
                    trade_data["sl"] = _tp2_protected_sl
            trade_data["sl_moved_to_entry"] = False
            trade_data["sl_moved_to_tp1_after_tp2"] = True
            trade_data["sl_move_reason"] = "TP2 hit - protect runner at TP1"

        # تفعيل trailing للـ 20% المتبقية
        trade_data["trailing_active"] = True
        _trailing_pct_val = safe_float(
            trade_data.get("trailing_pct", TRAILING_PCT), TRAILING_PCT
        )
        trade_data["trailing_pct"] = _trailing_pct_val
        _side = normalize_side(trade_data.get("side", "long"))
        _tp2_price = safe_float(trade_data.get("tp2"), 0.0)

        if _side == "long":
            # أعلى سعر: من الشمعة الحالية أو سعر TP2
            _init_high = safe_float(candle.get("high"), _tp2_price) if candle else _tp2_price
            _init_high = max(_init_high, _tp2_price)
            trade_data["trailing_high"] = round_price(_init_high) if _init_high > 0 else None
            trade_data["trailing_low"]  = None
            trade_data["trailing_sl"]   = round_price(
                _init_high * (1 - _trailing_pct_val / 100.0)
            ) if _init_high > 0 else None
        else:
            # أدنى سعر: من الشمعة الحالية أو سعر TP2
            _init_low = safe_float(candle.get("low"), _tp2_price) if candle else _tp2_price
            _init_low = min(_init_low, _tp2_price) if _tp2_price > 0 else _init_low
            trade_data["trailing_high"] = None
            trade_data["trailing_low"]  = round_price(_init_low) if _init_low > 0 else None
            trade_data["trailing_sl"]   = round_price(
                _init_low * (1 + _trailing_pct_val / 100.0)
            ) if _init_low > 0 else None

        diagnostics = trade_data.get("diagnostics", {}) or {}
        diagnostics["tp2_hit"] = True
        diagnostics["tp2_hit_at"] = trade_data["tp2_hit_at"]
        diagnostics["tp2_protected_runner"] = True
        diagnostics["sl_moved_to_tp1_after_tp2"] = trade_data.get("sl_moved_to_tp1_after_tp2", False)
        diagnostics["sl_move_reason"] = trade_data.get("sl_move_reason")
        diagnostics["trailing_active"] = True
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

    # معالجة breakeven بعد TP1: إذا تم تفعيل TP1 ثم رجع السعر إلى effective_entry، تعتبر فوز جزئي وليس خسارة
    effective_entry = get_trade_effective_entry(trade_data)
    tp1_hit = normalize_bool(trade_data.get("tp1_hit", False))
    tp2_hit = normalize_bool(trade_data.get("tp2_hit", False)) or normalize_bool(trade_data.get("tp2_protected_runner", False))

    if result == "loss" and tp2_hit:
        # After TP2, remaining runner SL is expected at TP1/TP1+buffer; classify as TP2 protected win.
        result = "tp2_win"
        trade_data["tp2_protected_runner_exit"] = True
        if not trade_data.get("exit_reason"):
            trade_data["exit_reason"] = "tp2_runner_protected_sl"

    if result == "loss" and tp1_hit:
        # التأكد إن SL الحالي يساوي effective_entry تقريباً (breakeven protected)
        sl = safe_float(trade_data.get("sl"), 0.0)
        if effective_entry > 0 and abs(sl - effective_entry) / effective_entry <= 0.001:
            # هذا breakeven بعد TP1 -> نعتبره tp1_win
            result = "tp1_win"
            trade_data["protected_breakeven_exit"] = True
            if not trade_data.get("exit_reason"):
                trade_data["exit_reason"] = "breakeven_after_tp1"

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

        # إيقاف trailing إذا كان نشطاً
        trade_data["trailing_active"] = False

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
        elif result == "trailing_win":
            redis_client.hincrby(stats_key, "trailing_wins", 1)
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

        # استخدام النسب الافتراضية 40%
        tp1_close_pct = safe_float(
            trade_data.get("tp1_close_pct", diagnostics.get("tp1_close_pct", TP1_CLOSE_PCT)),
            TP1_CLOSE_PCT
        )
        remaining_pct = max(0.0, 100.0 - tp1_close_pct)

        trade_data["tp1_hit"] = True
        trade_data["tp1_hit_at"] = int(time.time())
        trade_data["status"] = "partial"
        trade_data["partial_close_pct"] = tp1_close_pct
        trade_data["remaining_position_pct"] = remaining_pct
        trade_data["updated_at"] = int(time.time())

        # New lifecycle: TP1 closes 40% only. Do NOT move SL here.
        # Protection starts after TP2 to avoid early breakeven exits before continuation.
        trade_data["sl_moved_to_entry"] = False
        trade_data["sl_move_reason"] = "TP1 hit - SL unchanged; protection starts after TP2"
        trade_data["tp1_only_unprotected"] = True

        diagnostics["sl_moved_to_entry"] = False
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
    تقييم الصفقة على شمعة واحدة. يدعم trailing بعد TP2.
    """
    side = normalize_side(trade.get("side", "long"))
    diagnostics = trade.get("diagnostics", {}) or {}

    effective_entry = get_trade_effective_entry(trade)

    sl = safe_float(trade["sl"])
    tp1 = safe_float(trade["tp1"])
    tp2 = safe_float(trade["tp2"])
    tp1_hit = bool(trade.get("tp1_hit", False))
    tp2_hit = bool(trade.get("tp2_hit", False))
    trailing_active = normalize_bool(trade.get("trailing_active", False))

    low = safe_float(candle["low"])
    high = safe_float(candle["high"])

    result = None
    tp1_now = False
    tp2_now = False

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
            trade["last_processed_candle_ts"] = candle_ts
            diagnostics["pullback_triggered"] = True
            diagnostics["entry_mode"] = "pullback_triggered"
            diagnostics["effective_entry"] = round_price(pb_entry)
            diagnostics["activated_at"] = now_ts
            diagnostics["activated_candle_ts"] = candle_ts
            trade["status"] = "open"
            trade["diagnostics"] = diagnostics
            trade = recalc_targets_from_effective_entry(trade, pb_entry)
            # ── منع تقييم TP/SL على نفس شمعة التفعيل ──────────────
            # ترتيب الحركة داخل الشمعة غير معروف، نبدأ من الشمعة التالية
            logger.info(
                f"{trade.get('symbol', '?')} → pullback triggered @ {pb_entry} "
                f"| skip same-candle TP/SL evaluation"
            )
            return None, False, trade
        else:
            trade["status"] = "pending_pullback"
            diagnostics["entry_mode"] = "pullback_pending"
            diagnostics["pullback_triggered"] = False
            trade["diagnostics"] = diagnostics
            return None, False, trade

    # التقييم العادي مع trailing
    if side == "long":
        # trailing نشط بعد TP2
        if trailing_active and tp2_hit:
            trailing_pct_val = safe_float(trade.get("trailing_pct", TRAILING_PCT), TRAILING_PCT) / 100.0
            current_trailing_high = safe_float(trade.get("trailing_high"), 0.0)
            # لو trailing_high غير مهيأ → هيأه من TP2
            if current_trailing_high <= 0:
                _tp2 = safe_float(trade.get("tp2"), 0.0)
                current_trailing_high = max(high, _tp2) if _tp2 > 0 else high
                trade["trailing_high"] = round_price(current_trailing_high)
                trade["trailing_sl"]   = round_price(current_trailing_high * (1 - trailing_pct_val))
            # تحديث أعلى سعر
            if high > current_trailing_high:
                current_trailing_high = high
                trade["trailing_high"] = round_price(current_trailing_high)
                trade["trailing_sl"]   = round_price(current_trailing_high * (1 - trailing_pct_val))
            # فحص كسر trailing SL
            trailing_sl_val = safe_float(trade.get("trailing_sl"), 0.0)
            if trailing_sl_val > 0 and low <= trailing_sl_val:
                trade["trailing_exit_price"] = trailing_sl_val
                diagnostics = trade.get("diagnostics", {}) or {}
                diagnostics["trailing_exit_price"] = trailing_sl_val
                diagnostics["trailing_high"]       = trade.get("trailing_high")
                diagnostics["trailing_sl"]         = trade.get("trailing_sl")
                trade["diagnostics"] = diagnostics
                return "trailing_win", False, trade

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
                result = "tp2_win" if tp2_hit else "tp1_win"

    else:  # short
        if trailing_active and tp2_hit:
            trailing_pct_val = safe_float(trade.get("trailing_pct", TRAILING_PCT), TRAILING_PCT) / 100.0
            current_trailing_low = safe_float(trade.get("trailing_low"), 0.0)
            if current_trailing_low <= 0:
                _tp2 = safe_float(trade.get("tp2"), 0.0)
                current_trailing_low = min(low, _tp2) if _tp2 > 0 else low
                trade["trailing_low"] = round_price(current_trailing_low)
                trade["trailing_sl"]  = round_price(current_trailing_low * (1 + trailing_pct_val))
            if low < current_trailing_low:
                current_trailing_low = low
                trade["trailing_low"] = round_price(current_trailing_low)
                trade["trailing_sl"]  = round_price(current_trailing_low * (1 + trailing_pct_val))
            trailing_sl_val = safe_float(trade.get("trailing_sl"), 0.0)
            if trailing_sl_val > 0 and high >= trailing_sl_val:
                trade["trailing_exit_price"] = trailing_sl_val
                diagnostics = trade.get("diagnostics", {}) or {}
                diagnostics["trailing_exit_price"] = trailing_sl_val
                diagnostics["trailing_low"]        = trade.get("trailing_low")
                diagnostics["trailing_sl"]         = trade.get("trailing_sl")
                trade["diagnostics"] = diagnostics
                return "trailing_win", False, trade

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
                result = "tp2_win" if tp2_hit else "tp1_win"

    # نمرر إشارة tp2_now لتُنفذ في update_open_trades
    trade["_tp2_now"] = tp2_now
    return result, tp1_now, trade




def _is_execution_managed_trade_for_block_protection(trade: dict) -> bool:
    """Return True only for trades that reached the execution-candidate layer.

    BLOCK_LONGS protection is intended for execution-managed/candidate trades only,
    not for normal tracking-only Telegram signals.
    """
    if not isinstance(trade, dict):
        return False

    diagnostics = trade.get("diagnostics", {}) or {}
    if not isinstance(diagnostics, dict):
        diagnostics = {}

    for key in (
        "execution_candidate",
        "is_execution_candidate",
        "execution_preview",
        "execution_message_sent",
    ):
        if normalize_bool(trade.get(key, False)) or normalize_bool(diagnostics.get(key, False)):
            return True

    non_candidate_values = {"", "none", "null", "false", "0", "not_candidate", "not candidate"}
    for key in (
        "execution_status",
        "execution_result_status",
        "execution_order_id",
        "execution_id",
        "execution_preview_id",
        "execution_reject_reason",
    ):
        for source in (trade, diagnostics):
            value = str(source.get(key, "") or "").strip().lower()
            if value and value not in non_candidate_values:
                return True

    return False


def _new_block_protection_summary(market_mode: str = "", reason: str = "") -> dict:
    return {
        "enabled": False,
        "mode": str(market_mode or ""),
        "reason": str(reason or ""),
        "open_seen": 0,
        "execution_seen": 0,
        "ignored_tracking_only": 0,
        "protected_winners": 0,
        "risk_compressed": 0,
        "monitoring_only": 0,
        "already_protected": 0,
        "skipped_close_to_sl": 0,
        "skipped_not_eligible": 0,
        "updated_symbols": [],
        "platform_updates_sent": 0,
        "platform_updates_failed": 0,
        "platform_update_errors": [],
        "tracking_only": True,
    }


def _sl_is_better(side: str, new_sl: float, current_sl: float) -> bool:
    side = normalize_side(side)
    new_sl = safe_float(new_sl, 0.0)
    current_sl = safe_float(current_sl, 0.0)
    if new_sl <= 0:
        return False
    if current_sl <= 0:
        return True
    if side == "short":
        return new_sl < current_sl
    return new_sl > current_sl


def _entry_buffer_sl(entry_price: float, side: str, buffer_pct: float) -> float:
    entry_price = safe_float(entry_price, 0.0)
    buffer_pct = max(0.0, safe_float(buffer_pct, 0.0))
    if entry_price <= 0:
        return 0.0
    if normalize_side(side) == "short":
        return entry_price * (1.0 - buffer_pct / 100.0)
    return entry_price * (1.0 + buffer_pct / 100.0)


def _is_severe_block_pressure(kwargs: dict) -> bool:
    try:
        level = str(kwargs.get("block_pressure_level", "") or kwargs.get("market_guard_level", "") or "").lower()
        red_ratio = safe_float(kwargs.get("block_red_ratio", kwargs.get("red_ratio", 0.0)), 0.0)
        avg_change = safe_float(kwargs.get("block_avg_change", kwargs.get("avg_change", 0.0)), 0.0)
        btc_change = safe_float(kwargs.get("block_btc_change", kwargs.get("btc_change", 0.0)), 0.0)
        if any(x in level for x in ("danger", "block", "panic", "crash", "hard")):
            return True
        if red_ratio >= 0.65:
            return True
        if avg_change <= -0.50:
            return True
        if btc_change <= -0.80:
            return True
    except Exception:
        pass
    return False


def _set_block_protection_fields(trade: dict, diagnostics: dict, *, now_ts: int, new_sl: float, old_sl: float,
                                 reason: str, protection_type: str, note: str = "") -> None:
    trade["protected_on_block"] = True
    trade["protected_sl"] = new_sl
    trade["block_protection_type"] = protection_type
    trade["block_protection_reason"] = reason or "market_mode_block_longs"
    trade["block_protection_ts"] = now_ts
    trade["block_protection_tracking_only"] = True
    trade["platform_sl_update_status"] = "tracking_only"
    trade["platform_sl_update_required"] = True
    if note:
        trade["block_protection_note"] = note
    if trade.get("original_sl_before_block_protection") is None:
        trade["original_sl_before_block_protection"] = old_sl

    diagnostics["protected_on_block"] = True
    diagnostics["protected_sl"] = new_sl
    diagnostics["block_protection_type"] = protection_type
    diagnostics["block_protection_reason"] = reason or "market_mode_block_longs"
    diagnostics["block_protection_ts"] = now_ts
    diagnostics["block_protection_tracking_only"] = True
    diagnostics["platform_sl_update_status"] = "tracking_only"
    diagnostics["platform_sl_update_required"] = True
    if note:
        diagnostics["block_protection_note"] = note
    if diagnostics.get("original_sl_before_block_protection") is None:
        diagnostics["original_sl_before_block_protection"] = old_sl


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
    block_protection_summary = _new_block_protection_summary(market_mode, reason)
    block_protection_summary["enabled"] = bool(protect_breakeven_on_block)

    if redis_client is None:
        return block_protection_summary

    market_type = normalize_market_type(market_type)
    side = normalize_side(side)
    open_set_key = get_open_trades_set_key(market_type, side)

    try:
        trade_keys = list(redis_client.smembers(open_set_key))
    except Exception as e:
        logger.error(f"update_open_trades set read error: {e}")
        return block_protection_summary

    now_ts = int(time.time())
    max_age_seconds = max_age_hours * 3600
    breakeven_buffer_pct = max(0.0, safe_float(kwargs.get("breakeven_buffer_pct", 0.0), 0.0))
    loss_compression_buffer_pct = max(0.05, safe_float(kwargs.get("block_loss_compression_buffer_pct", 0.30), 0.30))
    severe_block_pressure = _is_severe_block_pressure(kwargs)
    block_live_protection_callback = kwargs.get("block_live_protection_callback")

    def _record_platform_result(trade_obj: dict, diagnostics_obj: dict, protection_payload: dict):
        if not callable(block_live_protection_callback):
            return
        try:
            result_payload = block_live_protection_callback(trade_obj, protection_payload) or {}
            status = str(result_payload.get("status") or "").strip()
            mode = str(result_payload.get("mode") or "").strip()
            ok = bool(result_payload.get("ok"))
            trade_obj["platform_sl_update_status"] = status or ("ok" if ok else "failed")
            trade_obj["platform_sl_update_mode"] = mode
            trade_obj["platform_sl_update_result"] = result_payload
            diagnostics_obj["platform_sl_update_status"] = trade_obj["platform_sl_update_status"]
            diagnostics_obj["platform_sl_update_mode"] = mode
            diagnostics_obj["platform_sl_update_result"] = result_payload
            if ok:
                block_protection_summary["platform_updates_sent"] += 1
                if mode != "simulation":
                    block_protection_summary["tracking_only"] = False
            else:
                block_protection_summary["platform_updates_failed"] += 1
                block_protection_summary["platform_update_errors"].append({
                    "symbol": trade_obj.get("symbol"),
                    "status": status,
                    "reason": result_payload.get("reason") or result_payload.get("msg") or result_payload.get("code"),
                })
        except Exception as _platform_exc:
            trade_obj["platform_sl_update_status"] = "callback_error"
            diagnostics_obj["platform_sl_update_status"] = "callback_error"
            block_protection_summary["platform_updates_failed"] += 1
            block_protection_summary["platform_update_errors"].append({
                "symbol": trade_obj.get("symbol"),
                "status": "callback_error",
                "reason": str(_platform_exc),
            })

    for trade_key in trade_keys:
        try:
            trade = load_trade(redis_client, trade_key)
            if not trade:
                logger.warning(f"update_open_trades: trade_key not found in Redis, removing from set: {trade_key}")
                try:
                    redis_client.srem(open_set_key, trade_key)
                except Exception as e2:
                    logger.error(f"update_open_trades: failed to remove stale key {trade_key}: {e2}")
                continue

            if trade.get("status") == "closed":
                try:
                    redis_client.srem(open_set_key, trade_key)
                except Exception:
                    pass
                continue

            trade_side = normalize_side(trade.get("side", side))
            symbol = trade.get("symbol", trade_key)
            created_at = safe_timestamp(trade.get("created_at", now_ts))

            # ── Stuck trade warning ──────────────────────────────
            updated_at = safe_timestamp(trade.get("updated_at", created_at))
            if now_ts - updated_at > 3600 and trade.get("status") not in ("closed", "pending_pullback"):
                logger.warning(
                    f"{symbol} → stuck trade: no update for "
                    f"{(now_ts - updated_at) // 60}م | status={trade.get('status')}"
                )

            # --- انتهاء الصلاحية ---
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
                logger.debug(f"{symbol} → no candles fetched, skipping")
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
                    trade.pop("_tp2_now", None)
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
                            ok2 = mark_tp2_hit(redis_client, trade_key, trade, processed_candle_ts=c_ts, candle=candle)
                            if ok2:
                                trade = load_trade(redis_client, trade_key) or trade
                                state_changed = True
                                logger.info(f"{symbol} → TP2 hit right after TP1")
                    elif trade_side == "short" and safe_float(candle["low"], 0.0) <= safe_float(trade["tp2"], 0.0):
                        if not trade.get("tp2_hit"):
                            trade.pop("_tp2_now", None)
                            ok2 = mark_tp2_hit(redis_client, trade_key, trade, processed_candle_ts=c_ts, candle=candle)
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
                        trade.pop("_tp2_now", None)
                        ok = mark_tp2_hit(redis_client, trade_key, trade, processed_candle_ts=c_ts, candle=candle)
                        if ok:
                            trade = load_trade(redis_client, trade_key) or trade
                            state_changed = True
                            logger.info(f"{symbol} → TP2 hit")

                    # حفظ تحديثات trailing (تم تحديثها في evaluate_trade_on_candle)
                    if trade.get("trailing_active") and not result:
                        save_trade(redis_client, trade_key, trade)
                        update_trade_history_snapshot(redis_client, trade)
                        state_changed = True

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
                        block_protection_summary["open_seen"] += 1

                        if not _is_execution_managed_trade_for_block_protection(trade):
                            block_protection_summary["ignored_tracking_only"] += 1
                        else:
                            block_protection_summary["execution_seen"] += 1
                            entry_price = get_trade_effective_entry(trade)
                            if entry_price > 0 and current_price > 0:
                                if trade_side == "long":
                                    current_profit_pct = ((current_price - entry_price) / entry_price) * 100
                                else:
                                    current_profit_pct = ((entry_price - current_price) / entry_price) * 100

                                current_sl = safe_float(trade.get("sl"), 0.0)
                                diagnostics = trade.get("diagnostics", {}) or {}
                                if not isinstance(diagnostics, dict):
                                    diagnostics = {}

                                # New lifecycle: BLOCK protection mainly acts on TP2 protected runners.
                                is_tp2_runner = normalize_bool(trade.get("tp2_hit", False)) and normalize_bool(trade.get("trailing_active", False))
                                is_tp1_only = normalize_bool(trade.get("tp1_hit", False)) and not normalize_bool(trade.get("tp2_hit", False))
                                if is_tp2_runner:
                                    tp1_price = safe_float(trade.get("tp1"), 0.0)
                                    target_sl = round_price(tp1_price) if tp1_price > 0 else _entry_buffer_sl(entry_price, trade_side, breakeven_buffer_pct)
                                    moved_anything = False

                                    if _sl_is_better(trade_side, target_sl, current_sl):
                                        if trade.get("original_sl_before_breakeven") is None:
                                            trade["original_sl_before_breakeven"] = current_sl
                                        trade["sl"] = target_sl
                                        trade["protected_on_block"] = True
                                        trade["tp2_runner_block_protected"] = True
                                        trade["breakeven_protection_reason"] = reason or "market_block_protection"
                                        trade["breakeven_protected_ts"] = now_ts
                                        trade["sl_moved_to_entry"] = False
                                        trade["sl_moved_to_tp1_after_tp2"] = True
                                        trade["sl_move_reason"] = reason or "market_block_tp2_runner_protection"
                                        diagnostics["protected_on_block"] = True
                                        diagnostics["tp2_runner_block_protected"] = True
                                        diagnostics["breakeven_protection_reason"] = reason or "market_block_protection"
                                        diagnostics["breakeven_protected_ts"] = now_ts
                                        diagnostics["original_sl_before_breakeven"] = current_sl
                                        diagnostics["sl_moved_to_entry"] = False
                                        diagnostics["sl_moved_to_tp1_after_tp2"] = True
                                        diagnostics["sl_move_reason"] = reason or "market_block_tp2_runner_protection"
                                        moved_anything = True

                                    # After TP2, keep trailing active but never below Entry + buffer.
                                    if normalize_bool(trade.get("tp2_hit", False)) and normalize_bool(trade.get("trailing_active", False)):
                                        trailing_sl = safe_float(trade.get("trailing_sl"), 0.0)
                                        if _sl_is_better(trade_side, target_sl, trailing_sl):
                                            trade["trailing_sl"] = target_sl
                                            diagnostics["trailing_sl_floor_on_block"] = target_sl
                                            moved_anything = True

                                    if moved_anything:
                                        _set_block_protection_fields(
                                            trade, diagnostics, now_ts=now_ts, new_sl=target_sl, old_sl=current_sl,
                                            reason=reason or "market_block_protection",
                                            protection_type="tp2_runner_block_protection",
                                            note="TP2 runner protected at TP1 / tightened trailing",
                                        )
                                        _record_platform_result(trade, diagnostics, {
                                            "eligible": True,
                                            "symbol": symbol,
                                            "protected_sl": target_sl,
                                            "action": "lock_runner_at_tp1_or_better",
                                            "reason": reason or "market_block_protection",
                                        })
                                        trade["diagnostics"] = diagnostics
                                        state_changed = True
                                        block_protection_summary["protected_winners"] += 1
                                        block_protection_summary["updated_symbols"].append(str(symbol))
                                        logger.info(f"{symbol} → BLOCK TP2 runner protection applied")
                                    else:
                                        block_protection_summary["already_protected"] += 1

                                # Losing trades: reduce damage only under clear pressure, never widen SL.
                                elif current_profit_pct < 0:
                                    loss_abs = abs(current_profit_pct)
                                    if 0.30 <= loss_abs <= 0.80 and severe_block_pressure:
                                        if current_sl > 0:
                                            if trade_side == "long":
                                                distance_to_sl_pct = ((current_price - current_sl) / entry_price) * 100
                                                compression_sl = current_price * (1.0 - loss_compression_buffer_pct / 100.0)
                                            else:
                                                distance_to_sl_pct = ((current_sl - current_price) / entry_price) * 100
                                                compression_sl = current_price * (1.0 + loss_compression_buffer_pct / 100.0)

                                            if distance_to_sl_pct <= 0.25:
                                                block_protection_summary["skipped_close_to_sl"] += 1
                                            elif _sl_is_better(trade_side, compression_sl, current_sl):
                                                trade["sl"] = compression_sl
                                                _set_block_protection_fields(
                                                    trade, diagnostics, now_ts=now_ts, new_sl=compression_sl, old_sl=current_sl,
                                                    reason=reason or "market_block_risk_compression",
                                                    protection_type="risk_compression",
                                                    note="Loser risk compressed because BLOCK pressure is severe",
                                                )
                                                _record_platform_result(trade, diagnostics, {
                                                    "eligible": True,
                                                    "symbol": symbol,
                                                    "protected_sl": compression_sl,
                                                    "action": "emergency_risk_compression",
                                                    "reason": reason or "market_block_risk_compression",
                                                })
                                                trade["diagnostics"] = diagnostics
                                                state_changed = True
                                                block_protection_summary["risk_compressed"] += 1
                                                block_protection_summary["updated_symbols"].append(str(symbol))
                                                logger.info(f"{symbol} → BLOCK risk compression applied")
                                            else:
                                                block_protection_summary["monitoring_only"] += 1
                                        else:
                                            block_protection_summary["monitoring_only"] += 1
                                    else:
                                        block_protection_summary["monitoring_only"] += 1
                                else:
                                    block_protection_summary["skipped_not_eligible"] += 1
                    except Exception as e:
                        logger.warning(f"BLOCK protection check failed for {symbol}: {e}")

            # حفظ التغييرات النهائية
            if state_changed and trade.get("status") not in ("closed",):
                trade.pop("_tp2_now", None)
                save_trade(redis_client, trade_key, trade)
                update_trade_history_snapshot(redis_client, trade)

            if result:
                trade.pop("_tp2_now", None)
                _result_effective = result
                # لو SL ضرب بعد TP1 وSL كان على entry → tp1_win بدل loss
                if result == "loss" and normalize_bool(trade.get("tp1_hit", False)):
                    _entry_price = get_trade_effective_entry(trade)
                    _sl_now = safe_float(trade.get("sl"), 0.0)
                    if _entry_price > 0 and _sl_now > 0 and abs(_sl_now - _entry_price) / _entry_price <= 0.001:
                        _result_effective = "tp1_win"
                        trade["protected_breakeven_exit"] = True
                        trade["exit_reason"] = "breakeven_after_tp1"
                        logger.info(f"{symbol} → SL hit on entry → reclassified as tp1_win")
                mark_trade_closed(redis_client, trade_key, trade, _result_effective)
                logger.info(f"{symbol} → trade closed as {_result_effective}")

        except Exception as _trade_exc:
            logger.error(f"update_open_trades: exception processing {trade_key}: {_trade_exc}", exc_info=True)

    return block_protection_summary


# ------------------------------------------------------------
# LOAD TRADES (with/without history) – إصلاح الازدواجية
# ------------------------------------------------------------
def _trade_dedupe_key(trade: dict):
    """مفتاح فريد لمنع التكرار عند دمج active + history."""
    # الأولوية لـ alert_id
    alert_id = str(trade.get("alert_id") or "").strip()
    if alert_id:
        return ("alert_id", alert_id)
    # ثم trade_id
    trade_id = str(trade.get("trade_id") or "").strip()
    if trade_id:
        return ("trade_id", trade_id)
    # وإلا symbol + side + candle_time
    symbol = str(trade.get("symbol", ""))
    side = normalize_side(trade.get("side", "long"))
    ctime = safe_timestamp(trade.get("candle_time"), 0)
    return ("symbol_side_time", f"{symbol}:{side}:{ctime}")


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
    seen = set()

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

            dk = _trade_dedupe_key(trade)
            if dk in seen:
                continue
            seen.add(dk)

            if since_ts is not None:
                ts = get_trade_created_ts(trade)
                if not ts:
                    ts = safe_timestamp(trade.get("candle_time"), 0)
                if ts < since_ts:
                    continue

            if not include_open:
                status = str(trade.get("status", "") or "").strip().lower()
                if status in ("open", "partial", "pending_pullback", "tp2_partial", "trailing_open"):
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

    seen = set()
    trades = []

    def _add_if_new(trade_dict):
        dk = _trade_dedupe_key(trade_dict)
        if dk in seen:
            return
        seen.add(dk)
        trades.append(trade_dict)

    try:
        for key in redis_client.scan_iter(pattern_trade):
            raw = redis_client.get(key)
            if not raw:
                continue
            try:
                trade = json.loads(raw)
            except Exception:
                continue
            _add_if_new(trade)
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
            _add_if_new(trade)
    except Exception as e:
        logger.error(f"load_trades_with_history history scan error: {e}")

    # فلترة حسب الوقت والحالة
    filtered = []
    for trade in trades:
        if since_ts is not None:
            ts = get_trade_created_ts(trade)
            if not ts:
                ts = safe_timestamp(trade.get("candle_time"), 0)
            if ts < since_ts:
                continue

        if not include_open:
            status = str(trade.get("status", "") or "").strip().lower()
            if status in ("open", "partial", "pending_pullback", "tp2_partial", "trailing_open"):
                continue

        filtered.append(trade)

    filtered.sort(
        key=lambda x: (
            get_trade_created_ts(x) or safe_timestamp(x.get("candle_time"), 0)
        ),
        reverse=True,
    )

    return filtered


def load_all_trades_for_report(
    redis_client,
    market_type: Optional[str] = None,
    side: Optional[str] = None,
    since_ts: Optional[int] = None,
    include_open: bool = True,
) -> List[dict]:
    """
    مصدر موحد لتحميل كل الصفقات للتقارير.
    يمنع التكرار بين trade:* و trade_history:*.
    كل التقارير يجب أن تستخدم هذه الدالة لضمان تطابق الأعداد.
    """
    trades = load_trades_with_history(
        redis_client=redis_client,
        market_type=market_type,
        side=side,
        since_ts=since_ts,
        include_open=include_open,
    )

    # dedupe بـ alert_id أو symbol+candle_time+side
    seen = set()
    deduped = []
    for trade in trades:
        diagnostics = trade.get("diagnostics", {}) or {}
        alert_id = (
            trade.get("alert_id")
            or diagnostics.get("alert_id")
        )
        if alert_id:
            key = f"alert:{alert_id}"
        else:
            sym   = trade.get("symbol", "")
            ct    = safe_timestamp(trade.get("candle_time"), 0)
            sd    = normalize_side(trade.get("side", "long"))
            key   = f"{sym}:{ct}:{sd}"

        if key in seen:
            continue
        seen.add(key)

        # فلتر pending_pullback من الإحصائيات المالية لو مش include_open
        if not include_open:
            status = str(trade.get("status", "") or "").lower()
            if status == "pending_pullback":
                continue

        deduped.append(trade)

    return deduped


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
# حساب PnL مرجح للصفقة (الإصدار الجديد 3.6)
# ------------------------------------------------------------
def _is_tp1_hit(trade: dict) -> bool:
    """تحديد ما إذا كانت الصفقة قد حققت TP1."""
    if normalize_bool(trade.get("tp1_hit")):
        return True
    result = str(trade.get("result", "")).strip().lower()
    status = str(trade.get("status", "")).strip().lower()
    return result in ("tp1_win", "tp2_win") or status in ("tp2_partial", "trailing_open", "trailing_closed")


def _is_tp2_hit(trade: dict) -> bool:
    """تحديد ما إذا كانت الصفقة قد حققت TP2."""
    if normalize_bool(trade.get("tp2_hit")):
        return True
    result = str(trade.get("result", "")).strip().lower()
    status = str(trade.get("status", "")).strip().lower()
    return result == "tp2_win" or status in ("tp2_partial", "trailing_open", "trailing_closed", "tp2_win")


def _compute_weighted_trade_pnl(trade: dict) -> float:
    """
    حساب الربح/الخسارة بعد الرافعة لصفقة مغلقة.
    يستخدم calc_trade_result_pct من summary_helpers.
    """
    side = normalize_side(trade.get("side", "long"))
    leverage = SHORT_REPORT_LEVERAGE if side == "short" else REPORT_LEVERAGE
    result = str(trade.get("result", "") or "").lower()

    if not result or result in ("expired", "pending_expired"):
        return 0.0

    # validation: effective_entry لازم يكون > 0
    effective_entry = get_trade_effective_entry(trade)
    if effective_entry <= 0:
        logger.warning(f"_compute_weighted_trade_pnl: invalid effective_entry={effective_entry} for {trade.get('symbol', '?')}, skipping")
        return 0.0

    raw_pct = calc_trade_result_pct(trade)
    if raw_pct is None:
        return 0.0

    return raw_pct * leverage


def calculate_trade_pnl(trade: dict) -> dict:
    """
    Single source of truth لحساب PnL الصفقة.

    يرجع dict يحتوي:
    - pnl_pct         : الربح/الخسارة الخام بدون رافعة
    - pnl_leveraged   : الربح/الخسارة بعد الرافعة
    - is_win          : True لو tp1_win / tp2_win / trailing_win
    - is_loss         : True لو loss
    - is_breakeven    : True لو breakeven
    - is_tp1          : True لو tp1 أو أعلى
    - is_tp2          : True لو tp2 أو trailing_win
    - is_pending      : True لو pending_pullback (لا تدخل في إحصائيات)
    - skipped         : True لو الصفقة مش مكتملة

    القواعد:
    - يستخدم effective_entry دائماً
    - pending_pullback → skipped
    - SL == effective_entry → breakeven (إذا لم يكن TP1 قد لمس)
    - TP1 hit ثم breakeven → win
    - 40% TP1 / 40% TP2 / 20% trailing
    """
    if not isinstance(trade, dict):
        return _empty_pnl_result()

    side        = normalize_side(trade.get("side", "long"))
    leverage    = SHORT_REPORT_LEVERAGE if side == "short" else REPORT_LEVERAGE
    result      = str(trade.get("result", "") or "").lower()
    status      = str(trade.get("status", "") or "").lower()
    tp1_hit     = normalize_bool(trade.get("tp1_hit", False))

    # pending_pullback → مش داخل في الإحصائيات
    if status == "pending_pullback" or result == "pending_expired":
        return _empty_pnl_result(is_pending=True)

    # صفقات مفتوحة أو غير مكتملة
    if status in ("open", "partial", "tp2_partial", "trailing_open", "trailing") and not result:
        return _empty_pnl_result(skipped=True)

    # validation
    effective_entry = get_trade_effective_entry(trade)
    if effective_entry <= 0:
        return _empty_pnl_result(skipped=True)

    sl          = safe_float(trade.get("sl"), 0.0)
    initial_sl  = safe_float(trade.get("initial_sl", sl), 0.0)
    tp1         = safe_float(trade.get("tp1"), 0.0)
    tp2         = safe_float(trade.get("tp2"), 0.0)

    # فحص breakeven: استخدم SL الحالي أولاً لأن SL ينتقل إلى entry بعد TP1
    _sl_for_check = sl if sl > 0 else initial_sl
    _is_sl_on_entry = (
        _sl_for_check > 0
        and effective_entry > 0
        and abs(_sl_for_check - effective_entry) / effective_entry <= 0.001
    )

    # تصنيف خاص: loss لكن SL على entry → breakeven أو tp1_win
    _effective_result = result
    if result == "loss" and _is_sl_on_entry:
        if tp1_hit:
            _effective_result = "tp1_win"  # TP1 ثم رجع entry → win
        else:
            _effective_result = "breakeven"

    # احسب PnL من calc_trade_result_pct
    _trade_for_calc = dict(trade)
    _trade_for_calc["result"] = _effective_result
    raw_pct = calc_trade_result_pct(_trade_for_calc)
    if raw_pct is None:
        raw_pct = 0.0

    pnl_leveraged = raw_pct * leverage

    is_win       = _effective_result in ("tp1_win", "tp2_win", "trailing_win")
    is_loss      = _effective_result == "loss"
    is_breakeven = _effective_result == "breakeven"
    is_tp1       = tp1_hit or _effective_result in ("tp1_win", "tp2_win", "trailing_win")
    is_tp2       = normalize_bool(trade.get("tp2_hit", False)) or _effective_result in ("tp2_win", "trailing_win")

    return {
        "pnl_pct":         round(raw_pct, 4),
        "pnl_leveraged":   round(pnl_leveraged, 4),
        "is_win":          is_win,
        "is_loss":         is_loss,
        "is_breakeven":    is_breakeven,
        "is_tp1":          is_tp1,
        "is_tp2":          is_tp2,
        "is_pending":      False,
        "skipped":         False,
        "effective_result": _effective_result,
        "leverage":        leverage,
    }


def _empty_pnl_result(is_pending: bool = False, skipped: bool = False) -> dict:
    return {
        "pnl_pct":         0.0,
        "pnl_leveraged":   0.0,
        "is_win":          False,
        "is_loss":         False,
        "is_breakeven":    False,
        "is_tp1":          False,
        "is_tp2":          False,
        "is_pending":      is_pending,
        "skipped":         skipped,
        "effective_result": "",
        "leverage":        REPORT_LEVERAGE,
    }


def debug_trade_pnl(trade: dict) -> dict:
    """
    تقرير تشخيصي كامل لحساب PnL صفقة واحدة.
    مفيد للـ debugging والتحقق من صحة الأرقام.
    """
    if not isinstance(trade, dict):
        return {"error": "invalid trade"}

    side            = normalize_side(trade.get("side", "long"))
    leverage        = SHORT_REPORT_LEVERAGE if side == "short" else REPORT_LEVERAGE
    entry           = safe_float(trade.get("entry"), 0.0)
    effective_entry = get_trade_effective_entry(trade)
    sl              = safe_float(trade.get("sl"), 0.0)
    initial_sl      = safe_float(trade.get("initial_sl", sl), 0.0)
    tp1             = safe_float(trade.get("tp1"), 0.0)
    tp2             = safe_float(trade.get("tp2"), 0.0)
    result          = str(trade.get("result", "") or "")
    status          = str(trade.get("status", "") or "")
    tp1_hit         = normalize_bool(trade.get("tp1_hit", False))
    tp2_hit         = normalize_bool(trade.get("tp2_hit", False))
    trailing_active = normalize_bool(trade.get("trailing_active", False))
    trailing_high   = safe_float(trade.get("trailing_high"), 0.0)
    trailing_sl_v   = safe_float(trade.get("trailing_sl"), 0.0)
    trailing_exit   = safe_float(trade.get("trailing_exit_price"), 0.0)

    pnl_data = calculate_trade_pnl(trade)

    # حساب نسب الأهداف
    tp1_pct = tp2_pct = trailing_pct_gain = 0.0
    if effective_entry > 0:
        if side == "long":
            tp1_pct           = ((tp1 - effective_entry) / effective_entry * 100) if tp1 > 0 else 0.0
            tp2_pct           = ((tp2 - effective_entry) / effective_entry * 100) if tp2 > 0 else 0.0
            trailing_pct_gain = ((trailing_exit - effective_entry) / effective_entry * 100) if trailing_exit > 0 else 0.0
        else:
            tp1_pct           = ((effective_entry - tp1) / effective_entry * 100) if tp1 > 0 else 0.0
            tp2_pct           = ((effective_entry - tp2) / effective_entry * 100) if tp2 > 0 else 0.0
            trailing_pct_gain = ((effective_entry - trailing_exit) / effective_entry * 100) if trailing_exit > 0 else 0.0

    sl_pct = 0.0
    _sl_ref = initial_sl if initial_sl > 0 else sl
    if effective_entry > 0 and _sl_ref > 0:
        if side == "long":
            sl_pct = ((_sl_ref - effective_entry) / effective_entry * 100)
        else:
            sl_pct = ((effective_entry - _sl_ref) / effective_entry * 100)

    return {
        "symbol":             trade.get("symbol", "?"),
        "side":               side,
        "status":             status,
        "result":             result,
        "effective_result":   pnl_data["effective_result"],
        "entry":              entry,
        "effective_entry":    effective_entry,
        "sl":                 sl,
        "initial_sl":         initial_sl,
        "sl_pct":             round(sl_pct, 3),
        "tp1":                tp1,
        "tp1_pct":            round(tp1_pct, 3),
        "tp2":                tp2,
        "tp2_pct":            round(tp2_pct, 3),
        "tp1_hit":            tp1_hit,
        "tp2_hit":            tp2_hit,
        "trailing_active":    trailing_active,
        "trailing_high":      trailing_high,
        "trailing_sl":        trailing_sl_v,
        "trailing_exit_price": trailing_exit,
        "trailing_pct_gain":  round(trailing_pct_gain, 3),
        "pnl_raw_pct":        pnl_data["pnl_pct"],
        "pnl_leveraged_pct":  pnl_data["pnl_leveraged"],
        "leverage":           leverage,
        "is_win":             pnl_data["is_win"],
        "is_loss":            pnl_data["is_loss"],
        "is_breakeven":       pnl_data["is_breakeven"],
        "is_tp1":             pnl_data["is_tp1"],
        "is_tp2":             pnl_data["is_tp2"],
        "is_pending":         pnl_data["is_pending"],
        "skipped":            pnl_data["skipped"],
        "breakdown": {
            "tp1_40pct":      round(tp1_pct * 0.4, 3) if tp1_hit else 0.0,
            "tp2_40pct":      round(tp2_pct * 0.4, 3) if tp2_hit else 0.0,
            "trailing_20pct": round(trailing_pct_gain * 0.2, 3) if trailing_exit > 0 else 0.0,
            "total_raw":      pnl_data["pnl_pct"],
        },
    }


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




def _partial_lifecycle_realized_pnl_for_report(trade: dict) -> float:
    """Return leveraged realized/locked PnL for non-closed lifecycle states.

    Reporting only: TP1 contributes 40%, TP2 contributes 40%, trailing 20% is
    counted only when a trailing/current/exit price exists.
    """
    try:
        if not isinstance(trade, dict):
            return 0.0
        status = str(trade.get("status", "") or "").lower()
        result = str(trade.get("result", "") or "").lower()
        if status == "pending_pullback" or result in ("loss", "breakeven", "tp1_win", "tp2_win", "trailing_win"):
            return 0.0
        side = normalize_side(trade.get("side", "long"))
        leverage = SHORT_REPORT_LEVERAGE if side == "short" else REPORT_LEVERAGE
        entry = get_trade_effective_entry(trade)
        tp1 = safe_float(trade.get("tp1"), 0.0)
        tp2 = safe_float(trade.get("tp2"), 0.0)
        if entry <= 0:
            return 0.0
        def pct_to(target):
            target = safe_float(target, 0.0)
            if target <= 0:
                return 0.0
            if side == "short":
                return ((entry - target) / entry) * 100.0
            return ((target - entry) / entry) * 100.0
        raw = 0.0
        if _is_tp1_hit(trade):
            raw += pct_to(tp1) * 0.40
        if _is_tp2_hit(trade):
            raw += pct_to(tp2) * 0.40
            trailing_price = safe_float(trade.get("current_price") or trade.get("trailing_exit_price") or trade.get("exit_price"), 0.0)
            if trailing_price > 0:
                raw += pct_to(trailing_price) * 0.20
        return round(raw * leverage, 4)
    except Exception:
        return 0.0

def build_trade_summary_from_trades(
    trades: list,
    market_type: Optional[str] = "futures",
    side: Optional[str] = "long",
) -> dict:
    """Build the same financial/TP report summary from an already-filtered trade list.

    Used for specialized reports such as execution-candidate trades without changing
    the normal all-trades report logic.
    """
    side_norm = normalize_side(side or "long")
    filtered_trades = [
        t for t in (trades or [])
        if isinstance(t, dict) and t.get("status") != "pending_pullback"
    ]

    summary = summarize_trades(filtered_trades)
    summary["market_type"] = market_type or "futures"
    summary["side"] = side_norm

    closed_trades = [
        t for t in filtered_trades
        if str(t.get("result", "")).lower() in (
            "tp1_win", "tp2_win", "trailing_win", "loss", "breakeven"
        )
    ]

    gross_profit = 0.0
    gross_loss = 0.0
    total_pnl = 0.0
    win_pnls = []
    loss_pnls = []
    all_pnls = []

    tp1_hits = sum(1 for t in filtered_trades if _is_tp1_hit(t))
    tp2_hits = sum(1 for t in filtered_trades if _is_tp2_hit(t))
    tp2_wins = sum(1 for t in closed_trades if t.get("result") in ("tp2_win", "trailing_win"))
    trailing_wins = sum(1 for t in closed_trades if t.get("result") == "trailing_win")

    partial_lifecycle_pnl = 0.0
    partial_lifecycle_count = 0
    for _trade in filtered_trades:
        _p = _partial_lifecycle_realized_pnl_for_report(_trade)
        if abs(_p) > 0:
            partial_lifecycle_pnl += _p
            partial_lifecycle_count += 1

    for trade in closed_trades:
        pnl_data = calculate_trade_pnl(trade)
        if pnl_data["skipped"] or pnl_data["is_pending"]:
            continue
        pnl = pnl_data["pnl_leveraged"]
        all_pnls.append(pnl)
        total_pnl += pnl
        if pnl > 0:
            gross_profit += pnl
            win_pnls.append(pnl)
        elif pnl < 0:
            gross_loss += abs(pnl)
            loss_pnls.append(pnl)

    summary["realized_leveraged_pnl_pct"] = round(total_pnl, 4)
    summary["realized_pnl_pct"] = round(total_pnl, 4)
    summary["gross_profit_pct"] = round(gross_profit, 4)
    summary["gross_loss_pct"] = round(-gross_loss, 4)
    summary["net_profit_pct"] = round(total_pnl, 4)
    summary["avg_win_pct"] = round(sum(win_pnls) / len(win_pnls), 4) if win_pnls else 0.0
    summary["avg_loss_pct"] = round(sum(loss_pnls) / len(loss_pnls), 4) if loss_pnls else 0.0
    summary["best_trade_pct"] = round(max(all_pnls), 4) if all_pnls else 0.0
    summary["worst_trade_pct"] = round(min(all_pnls), 4) if all_pnls else 0.0

    leverage = SHORT_REPORT_LEVERAGE if side_norm == "short" else REPORT_LEVERAGE
    summary["realized_raw_pnl_pct"] = round(total_pnl / leverage, 4) if leverage else 0.0
    summary["partial_lifecycle_leveraged_pnl_pct"] = round(partial_lifecycle_pnl, 4)
    summary["partial_lifecycle_raw_pnl_pct"] = round(partial_lifecycle_pnl / leverage, 4) if leverage else 0.0
    summary["partial_lifecycle_count"] = partial_lifecycle_count
    summary["total_lifecycle_leveraged_pnl_pct"] = round(total_pnl + partial_lifecycle_pnl, 4)

    summary["tp1_hits"] = tp1_hits
    summary["tp2_hits"] = tp2_hits
    summary["tp2_wins"] = tp2_wins
    summary["trailing_wins"] = trailing_wins

    signals = safe_int(summary.get("signals", 0))
    summary["tp1_rate"] = round((tp1_hits / signals) * 100, 2) if signals > 0 else 0.0
    summary["tp2_rate"] = round((tp2_hits / signals) * 100, 2) if signals > 0 else 0.0
    summary["tp1_to_tp2_rate"] = round((tp2_hits / tp1_hits) * 100, 2) if tp1_hits > 0 else 0.0

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

    summary["recent_winning_trades"] = sorted(
        [t for t in closed_trades if calculate_trade_pnl(t).get("pnl_leveraged", 0.0) > 0],
        key=lambda t: safe_timestamp(t.get("closed_at") or t.get("updated_at") or t.get("created_at"), 0),
        reverse=True,
    )[:5]

    return summary


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

    trades = load_all_trades_for_report(
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

    # إعادة حساب PnL عبر _compute_weighted_trade_pnl (يستخدم summary_helpers الآن)
    closed_trades = [
        t for t in filtered_trades
        if str(t.get("result", "")).lower() in (
            "tp1_win", "tp2_win", "trailing_win", "loss", "breakeven"
        )
    ]
    gross_profit = 0.0
    gross_loss = 0.0
    total_pnl = 0.0
    win_pnls = []
    loss_pnls = []
    all_pnls = []

    # حساب TP1/TP2 hits بالشكل الصحيح
    tp1_hits = sum(1 for t in filtered_trades if _is_tp1_hit(t))
    tp2_hits = sum(1 for t in filtered_trades if _is_tp2_hit(t))
    tp2_wins = sum(1 for t in closed_trades if t.get("result") in ("tp2_win", "trailing_win"))
    trailing_wins = sum(1 for t in closed_trades if t.get("result") == "trailing_win")

    partial_lifecycle_pnl = 0.0
    partial_lifecycle_count = 0
    for _trade in filtered_trades:
        _p = _partial_lifecycle_realized_pnl_for_report(_trade)
        if abs(_p) > 0:
            partial_lifecycle_pnl += _p
            partial_lifecycle_count += 1

    for trade in closed_trades:
        pnl_data = calculate_trade_pnl(trade)
        if pnl_data["skipped"] or pnl_data["is_pending"]:
            continue
        pnl = pnl_data["pnl_leveraged"]
        all_pnls.append(pnl)
        total_pnl += pnl
        if pnl > 0:
            gross_profit += pnl
            win_pnls.append(pnl)
        elif pnl < 0:
            gross_loss += abs(pnl)
            loss_pnls.append(pnl)

    summary["realized_leveraged_pnl_pct"] = round(total_pnl, 4)
    summary["realized_pnl_pct"] = round(total_pnl, 4)
    summary["gross_profit_pct"] = round(gross_profit, 4)
    summary["gross_loss_pct"] = round(-gross_loss, 4)
    summary["net_profit_pct"] = round(total_pnl, 4)
    summary["avg_win_pct"] = round(sum(win_pnls) / len(win_pnls), 4) if win_pnls else 0.0
    summary["avg_loss_pct"] = round(sum(loss_pnls) / len(loss_pnls), 4) if loss_pnls else 0.0
    summary["best_trade_pct"] = round(max(all_pnls), 4) if all_pnls else 0.0
    summary["worst_trade_pct"] = round(min(all_pnls), 4) if all_pnls else 0.0

    leverage = SHORT_REPORT_LEVERAGE if side_norm == "short" else REPORT_LEVERAGE
    summary["realized_raw_pnl_pct"] = round(total_pnl / leverage, 4) if leverage else 0.0
    summary["partial_lifecycle_leveraged_pnl_pct"] = round(partial_lifecycle_pnl, 4)
    summary["partial_lifecycle_raw_pnl_pct"] = round(partial_lifecycle_pnl / leverage, 4) if leverage else 0.0
    summary["partial_lifecycle_count"] = partial_lifecycle_count
    summary["total_lifecycle_leveraged_pnl_pct"] = round(total_pnl + partial_lifecycle_pnl, 4)

    # hits و rates
    summary["tp1_hits"] = tp1_hits
    summary["tp2_hits"] = tp2_hits
    summary["tp2_wins"] = tp2_wins
    summary["trailing_wins"] = trailing_wins

    signals = safe_int(summary.get("signals", 0))
    summary["tp1_rate"] = round((tp1_hits / signals) * 100, 2) if signals > 0 else 0.0
    summary["tp2_rate"] = round((tp2_hits / signals) * 100, 2) if signals > 0 else 0.0
    summary["tp1_to_tp2_rate"] = round((tp2_hits / tp1_hits) * 100, 2) if tp1_hits > 0 else 0.0

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

    summary["recent_winning_trades"] = sorted(
        [t for t in closed_trades if calculate_trade_pnl(t).get("pnl_leveraged", 0.0) > 0],
        key=lambda t: safe_timestamp(t.get("closed_at") or t.get("updated_at") or t.get("created_at"), 0),
        reverse=True,
    )[:5]

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

    # حساب TP1/TP2 Hits بالطريقة الصحيحة
    tp1_hits = sum(1 for t in trades if _is_tp1_hit(t))
    tp2_hits = sum(1 for t in trades if _is_tp2_hit(t))
    exit_data["tp1_hits"] = tp1_hits
    exit_data["tp2_hits"] = tp2_hits

    signals = safe_int(exit_data.get("signals", exit_data.get("trades_count", 0)))
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


def _format_recent_report_trade_lines(trade: dict) -> list:
    """Compact lines for recent trades inside period reports. UI only; no calculations changed."""
    try:
        symbol = str(trade.get("symbol", "?") or "?")
        pnl_data = calculate_trade_pnl(trade)
        pnl_pct = safe_float(pnl_data.get("pnl_leveraged", 0.0), 0.0)
        result = str(trade.get("result", "") or "")
        if result == "trailing_win":
            result_label = "TP2 + Trail Win"
        elif result == "tp2_win":
            result_label = "TP2 Win"
        elif result == "tp1_win":
            result_label = "TP1 Only"
        elif result == "breakeven":
            result_label = "Breakeven"
        else:
            result_label = result or "Closed"

        current = safe_float(trade.get("current_price") or trade.get("exit_price") or trade.get("trailing_exit_price"), 0.0)
        tp1 = safe_float(trade.get("tp1"), 0.0)
        tp2 = safe_float(trade.get("tp2"), 0.0)
        sl = safe_float(trade.get("sl"), 0.0)
        mode = str(trade.get("market_mode") or trade.get("current_mode") or trade.get("mode") or "").strip()
        btc_mode = str(trade.get("btc_mode") or "").strip()
        market_state = str(trade.get("market_state") or trade.get("market_state_label") or "").strip()
        reason = str(trade.get("primary_extra_setup") or trade.get("setup_type") or "").strip()
        score = safe_float(trade.get("score"), 0.0)

        tv_link = ""
        try:
            clean_sym = symbol.replace("-SWAP", "").replace("-USDT", "USDT")
            tv_link = f"https://www.tradingview.com/chart/?symbol=OKX:{clean_sym}.P&interval=15"
        except Exception:
            tv_link = ""

        lines = [
            f"🟢 <b>{html.escape(symbol)}</b>",
            f"{pnl_pct:+.2f}% Exposure | {html.escape(result_label)}",
            f"TP1: {_fmt_price_perf(tp1)} | 🎯 الحالي: {_fmt_price_perf(current)}",
            f"🏁 TP2: {_fmt_price_perf(tp2)} | 🛑 SL: {_fmt_price_perf(sl)}",
        ]
        context_parts = [x for x in (mode, btc_mode, market_state) if x]
        if context_parts:
            lines.append("🌍 " + " | ".join(html.escape(x) for x in context_parts))
        if reason:
            lines.append("⚡ " + html.escape(reason))
        if score > 0:
            tv = f' | <a href="{html.escape(tv_link, quote=True)}">TradingView</a>' if tv_link else ""
            lines.append(f"⭐ {score:g}{tv}")
        return lines
    except Exception as e:
        return [f"🟢 <b>{html.escape(str(trade.get('symbol', '?')))}</b>", f"⚠️ تعذر تنسيق الصفقة: {html.escape(str(e))}"]


def format_period_summary(title: str, summary: dict) -> str:
    """Official compact period report style. Formatting only; calculations remain unchanged."""
    side = normalize_side(summary.get("side", "long"))

    signals = safe_int(summary.get("signals", summary.get("trades_count", 0)))
    wins = safe_int(summary.get("wins", 0))
    losses = safe_int(summary.get("losses", 0))
    expired = safe_int(summary.get("expired", 0))
    breakeven_exits = safe_int(summary.get("breakeven_exits", 0))
    closed = max(safe_int(summary.get("closed", 0)), wins + losses + expired + breakeven_exits)
    open_count = safe_int(summary.get("open", 0))

    gross_profit = safe_float(summary.get("gross_profit_pct", 0.0))
    gross_loss = safe_float(summary.get("gross_loss_pct", 0.0))
    net_pnl = safe_float(summary.get("realized_leveraged_pnl_pct", summary.get("realized_pnl_pct", 0.0)))
    raw_pnl = safe_float(summary.get("realized_raw_pnl_pct", 0.0))
    avg_win = safe_float(summary.get("avg_win_pct", 0.0))
    avg_loss = safe_float(summary.get("avg_loss_pct", 0.0))
    best = safe_float(summary.get("best_trade_pct", 0.0))
    worst = safe_float(summary.get("worst_trade_pct", 0.0))

    tp1_hits = safe_int(summary.get("tp1_hits", 0))
    tp2_hits = safe_int(summary.get("tp2_hits", 0))
    tp1_only = safe_int(summary.get("tp1_then_entry", summary.get("tp1_only", summary.get("tp1_wins", 0))))
    full_wins = safe_int(summary.get("tp2_wins", 0))
    trailing_wins = safe_int(summary.get("trailing_wins", 0))
    trailing_open_count = safe_int(summary.get("trailing_open", 0))
    tp2_reached = max(tp2_hits, full_wins + trailing_wins + trailing_open_count)
    wins = max(wins, tp1_only + full_wins + trailing_wins)
    closed = max(closed, wins + losses + expired + breakeven_exits)

    tp1_rate = safe_float(summary.get("tp1_rate", 0))
    tp2_rate = safe_float(summary.get("tp2_rate", 0))
    tp1_to_tp2_rate = safe_float(summary.get("tp1_to_tp2_rate", 0))
    winrate = safe_float(summary.get("winrate", 0))

    plan = get_report_sizing_plan(side)
    leverage = plan["leverage"]
    active_trade_slots = plan["active_trade_slots"]
    margin_per_trade = plan["margin_per_trade_usd"]

    gross_profit_usd = estimate_pct_to_usd(gross_profit, side=side)
    gross_loss_usd = estimate_pct_to_usd(abs(gross_loss), side=side)
    net_pnl_usd = estimate_pct_to_usd(net_pnl, side=side)
    wallet_pnl_pct, wallet_pnl_usd = estimate_wallet_pnl(summary, side=side)

    partial_lifecycle_pnl = safe_float(summary.get("partial_lifecycle_leveraged_pnl_pct", 0.0))
    partial_lifecycle_count = safe_int(summary.get("partial_lifecycle_count", 0))
    total_lifecycle_pnl = safe_float(summary.get("total_lifecycle_leveraged_pnl_pct", net_pnl))

    diagnosis = diagnose_performance_problem(summary)
    net_icon = "🟢" if net_pnl >= 0 else "🔴"
    wallet_icon = "🟢" if wallet_pnl_usd >= 0 else "🔴"
    sep = "━━━━━━━━━━━━"

    lines = [
        f"📊 <b>{title}</b>",
        sep,
        "⚡ جميع نسب الأداء محسوبة على رافعة 15x",
        "📊 <b>Quick Stats</b>",
        f"• Signals: {signals}",
        f"• Open: {open_count}",
        f"• Closed: {closed}",
        f"🏆 Win Rate: <b>{winrate:.1f}%</b>",
        f"🟢 Winners: {wins}",
        f"🔴 Losers: {losses}",
    ]
    if expired:
        lines.append(f"⚫ Expired: {expired}")
    if breakeven_exits:
        lines.append(f"⚪ Breakeven: {breakeven_exits}")

    lines += [
        sep,
        "💰 <b>Wallet Impact</b>",
        "✅ الصفقات المغلقة",
        "📈 الأرباح",
        f"{gross_profit:+.2f}%",
        f"{gross_profit_usd:+.2f}$",
        "📉 الخسائر",
        f"{-abs(gross_loss):+.2f}%",
        f"{-abs(gross_loss_usd):+.2f}$",
        "⚖️ الصافي",
        f"<b>{net_icon} {net_pnl:+.2f}%</b>",
        f"<b>{net_pnl_usd:+.2f}$</b>",
        "",
        "💼 <b>التأثير الحالي على المحفظة</b>",
        f"<b>{wallet_icon} {wallet_pnl_usd:+.2f}$</b>",
        f"<b>{wallet_pnl_pct:+.2f}%</b>",
        sep,
        "🧠 <b>Execution Behavior Summary</b>",
        "📦 Model: 40/40/20",
        f"📈 Avg Winner: {avg_win:+.2f}%",
        f"📉 Avg Loser: {avg_loss:+.2f}%",
        f"🎯 TP1 Rate: {tp1_rate:.1f}%",
        f"🏁 TP2 Rate: {tp2_rate:.1f}%",
        f"🔁 TP1 → TP2: {tp1_to_tp2_rate:.1f}%",
        f"🔄 Trailing Wins: {trailing_wins}",
        f"⚖️ Partial Open PnL: {partial_lifecycle_pnl:+.2f}%",
        f"💼 Total Closed + Partial: {total_lifecycle_pnl:+.2f}%",
        sep,
        "🎯 <b>أداء الأهداف</b>",
        f"• TP1 Hits: {tp1_hits}",
        f"• TP2 Reached: {tp2_reached}",
        f"• TP1 Only: {tp1_only}",
        f"• Full TP2 Closed: {full_wins}",
        sep,
        "⚙️ <b>إعدادات الحساب</b>",
        f"• الرافعة: {leverage:.0f}x",
        f"• الحد الأقصى للصفقات: {active_trade_slots}",
        f"• مارجن الصفقة الواحدة: {margin_per_trade:.2f}$",
        f"• الحركة بدون رافعة: {raw_pnl:+.2f}%",
        sep,
        "📌 <b>تفاصيل إضافية</b>",
        f"• أفضل صفقة: {best:+.2f}%",
        f"• أسوأ صفقة: {worst:+.2f}%",
        sep,
        f"🧠 <b>تشخيص الأداء:</b> {diagnosis['emoji']} <b>{diagnosis['problem_label']}</b>",
        f"• السبب: {diagnosis['explanation']}",
        f"• الإجراء المقترح: {diagnosis['action']}",
    ]

    recent_winning_trades = summary.get("recent_winning_trades") or []
    if recent_winning_trades:
        trade_separator = "┄┄┄┄┄┄"
        lines.extend([sep, "🏆 <b>آخر 5 صفقات رابحة</b>"])
        for idx, trade in enumerate(recent_winning_trades[:5]):
            lines.extend(_format_recent_report_trade_lines(trade))
            if idx < len(recent_winning_trades[:5]) - 1:
                lines.append(trade_separator)

    return "\n".join(lines)


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
# الصفقات المفتوحة - /open_trades command
# ------------------------------------------------------------
def get_open_trades_summary(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
) -> List[dict]:
    """
    جلب الصفقات المفتوحة مع بيانات كاملة لعرضها في /open_trades.
    تُرجع قائمة مرتبة من الأحدث للأقدم.
    """
    if redis_client is None:
        return []

    market_type = normalize_market_type(market_type)
    side_norm   = normalize_side(side)
    open_set_key = get_open_trades_set_key(market_type, side_norm)

    try:
        trade_keys = list(redis_client.smembers(open_set_key))
    except Exception as e:
        logger.error(f"get_open_trades_summary read error: {e}")
        return []

    open_trades = []
    now_ts = int(time.time())

    for trade_key in trade_keys:
        trade = load_trade(redis_client, trade_key)
        if not trade:
            continue
        if trade.get("status") == "closed":
            continue

        symbol         = trade.get("symbol", "")
        entry          = safe_float(trade.get("entry"), 0.0)
        effective_entry = get_trade_effective_entry(trade)
        sl             = safe_float(trade.get("sl"), 0.0)
        initial_sl     = safe_float(trade.get("initial_sl", sl), 0.0)
        tp1            = safe_float(trade.get("tp1"), 0.0)
        tp2            = safe_float(trade.get("tp2"), 0.0)
        score          = safe_float(trade.get("score"), 0.0)
        status         = str(trade.get("status", "open")).lower()
        tp1_hit        = normalize_bool(trade.get("tp1_hit", False))
        tp2_hit        = normalize_bool(trade.get("tp2_hit", False))
        sl_moved       = normalize_bool(trade.get("sl_moved_to_entry", False))
        trailing_active = normalize_bool(trade.get("trailing_active", False))
        trailing_high  = safe_float(trade.get("trailing_high"), 0.0)
        trailing_sl_v  = safe_float(trade.get("trailing_sl"), 0.0)
        created_at     = safe_timestamp(trade.get("created_at"), now_ts)
        timeframe      = trade.get("timeframe", "15m")
        entry_mode     = str(trade.get("entry_mode", "market"))

        # ── Phase detection (محسّن) ─────────────────────────────
        # لا نعتمد على status == "partial" وحده لأن بعض الصفقات قد تحمل status
        # قديم/مضلل بدون أن يكون TP1 قد تحقق فعليًا. مرحلة "بعد TP1" تثبت فقط
        # من flags حقيقية مثل tp1_hit أو تحريك SL إلى Entry أو TP2/Trailing.
        real_tp1_phase = bool(tp1_hit or sl_moved or tp2_hit or trailing_active)

        if status in ("tp2_partial", "trailing_open", "trailing") and real_tp1_phase:
            phase = "trailing"
        elif trailing_active and tp2_hit:
            phase = "trailing"
        elif tp2_hit:
            phase = "tp2_hit"
        elif real_tp1_phase:
            phase = "tp1_hit"
        elif status == "pending_pullback":
            phase = "pending_pullback"
        else:
            phase = "open"

        # ── المدة ───────────────────────────────────────────────
        age_seconds = now_ts - created_at
        age_minutes = age_seconds // 60
        if age_minutes < 60:
            age_str = f"{age_minutes}د"
        elif age_minutes < 1440:
            age_str = f"{age_minutes // 60}س {age_minutes % 60}د"
        else:
            age_str = f"{age_minutes // 1440}ي {(age_minutes % 1440) // 60}س"

        # ── السعر الحالي من OKX ──────────────────────────────────
        current_price = 0.0
        try:
            raw = fetch_recent_candles(symbol, timeframe="1m", limit=1)
            if raw:
                candles = normalize_candles(raw)
                if candles:
                    current_price = safe_float(candles[-1]["close"], 0.0)
        except Exception:
            pass

        # ── PnL الحالي + Weighted PnL الفعلي ────────────────────────
        current_pnl_pct = 0.0
        current_pnl_leveraged = 0.0
        weighted_pnl_pct = 0.0
        weighted_pnl_leveraged = 0.0
        pnl_label = "تعادل"
        pnl_emoji = "🟡"

        # pending_pullback لا تُحسب PnL
        if status == "pending_pullback":
            current_pnl_pct = 0.0
            current_pnl_leveraged = 0.0
            weighted_pnl_pct = 0.0
            weighted_pnl_leveraged = 0.0
        elif effective_entry > 0 and current_price > 0:
            if side_norm == "long":
                current_pnl_pct = ((current_price - effective_entry) / effective_entry) * 100
            else:
                current_pnl_pct = ((effective_entry - current_price) / effective_entry) * 100
            current_pnl_leveraged = current_pnl_pct * REPORT_LEVERAGE

            # Weighted PnL الفعلي حسب 40/40/20
            _tp1_pct_move = ((tp1 - effective_entry) / effective_entry * 100) if tp1 > 0 and effective_entry > 0 else 0.0
            _tp2_pct_move = ((tp2 - effective_entry) / effective_entry * 100) if tp2 > 0 and effective_entry > 0 else 0.0
            if side_norm == "short":
                _tp1_pct_move = -_tp1_pct_move
                _tp2_pct_move = -_tp2_pct_move

            if phase == "trailing":
                # TP1 40% + TP2 40% + current 20%
                weighted_pnl_pct = (_tp1_pct_move * 0.40) + (_tp2_pct_move * 0.40) + (current_pnl_pct * 0.20)
            elif phase == "tp1_hit":
                # TP1 40% + current 60%
                weighted_pnl_pct = (_tp1_pct_move * 0.40) + (current_pnl_pct * 0.60)
            else:
                # قبل TP1: الربح = حركة السعر الحالية 100%
                weighted_pnl_pct = current_pnl_pct

            weighted_pnl_leveraged = weighted_pnl_pct * REPORT_LEVERAGE

            if current_pnl_pct > 0.3:
                pnl_label = "رابح"
                pnl_emoji = "🟢"
            elif current_pnl_pct < -0.3:
                pnl_label = "خاسر"
                pnl_emoji = "🔴"

        # ── SL% ──────────────────────────────────────────────────
        sl_pct = 0.0
        sl_is_entry = False
        if effective_entry > 0 and sl > 0:
            if side_norm == "long":
                sl_pct = ((sl - effective_entry) / effective_entry) * 100
            else:
                sl_pct = ((effective_entry - sl) / effective_entry) * 100
            # لو SL اتنقل لـ entry بعد TP1
            if sl_moved or abs(sl_pct) < 0.05:
                sl_is_entry = True

        # ── TP1% / TP2% ──────────────────────────────────────────
        tp1_pct = tp2_pct = 0.0
        if effective_entry > 0:
            if side_norm == "long":
                tp1_pct = ((tp1 - effective_entry) / effective_entry) * 100 if tp1 > 0 else 0.0
                tp2_pct = ((tp2 - effective_entry) / effective_entry) * 100 if tp2 > 0 else 0.0
            else:
                tp1_pct = ((effective_entry - tp1) / effective_entry) * 100 if tp1 > 0 else 0.0
                tp2_pct = ((effective_entry - tp2) / effective_entry) * 100 if tp2 > 0 else 0.0

        # ── TradingView link ─────────────────────────────────────
        clean_sym = symbol.replace("-SWAP", "").replace("-USDT", "USDT")
        tv_sym    = f"OKX:{clean_sym}.P"
        tf_map    = {"1m": "1", "3m": "3", "5m": "5", "15m": "15",
                     "30m": "30", "1H": "60", "4H": "240"}
        tv_tf     = tf_map.get(timeframe, "15")
        tv_link   = f"https://www.tradingview.com/chart/?symbol={tv_sym}&interval={tv_tf}"

        open_trades.append({
            "symbol":               symbol,
            "entry":                entry,
            "effective_entry":      effective_entry,
            "sl":                   sl,
            "sl_pct":               sl_pct,
            "sl_is_entry":          sl_is_entry,
            "initial_sl":           initial_sl,
            "tp1":                  tp1,
            "tp1_pct":              tp1_pct,
            "tp2":                  tp2,
            "tp2_pct":              tp2_pct,
            "score":                score,
            "status":               status,
            "phase":                phase,
            "tp1_hit":              tp1_hit,
            "tp2_hit":              tp2_hit,
            "sl_is_entry":          sl_is_entry,
            "trailing_active":      trailing_active,
            "trailing_high":        trailing_high,
            "trailing_sl":          trailing_sl_v,
            "trailing_pct":         safe_float(trade.get("trailing_pct"), TRAILING_PCT),
            "current_price":        current_price,
            "current_pnl_pct":        round(current_pnl_pct, 2),
            "current_pnl_leveraged":  round(current_pnl_leveraged, 2),
            "weighted_pnl_pct":       round(weighted_pnl_pct, 2),
            "weighted_pnl_leveraged": round(weighted_pnl_leveraged, 2),
            "pnl_label":              pnl_label,
            "pnl_emoji":              pnl_emoji,
            "created_at":           created_at,
            "age_str":              age_str,
            "timeframe":            timeframe,
            "entry_mode":           entry_mode,
            "tv_link":              tv_link,
            "setup_type":           trade.get("setup_type", ""),
        })

    open_trades.sort(key=lambda x: x["created_at"], reverse=True)
    return open_trades


def _format_open_trade_compact_card(t: dict, index: int = 0) -> List[str]:
    """Compact trade card using the official Telegram report UI style."""
    phase = str(t.get("phase", "open") or "open")
    sym = str(t.get("symbol", "?")).replace("-SWAP", "")
    score = safe_float(t.get("score", 0.0), 0.0)
    age = str(t.get("age_str", "") or "")
    sl = safe_float(t.get("sl", 0.0), 0.0)
    tp1 = safe_float(t.get("tp1", 0.0), 0.0)
    tp2 = safe_float(t.get("tp2", 0.0), 0.0)
    pnl = safe_float(t.get("weighted_pnl_pct", t.get("current_pnl_pct", 0.0)), 0.0)
    lev = safe_float(t.get("weighted_pnl_leveraged", t.get("current_pnl_leveraged", 0.0)), 0.0)
    sl_is_entry = bool(t.get("sl_is_entry", False))
    tv_link = str(t.get("tv_link", "") or "")
    setup_raw = str(t.get("setup_type", "") or "")

    setup_parts = [p.strip() for p in setup_raw.replace(",", "|").split("|") if p.strip()]
    preferred = [
        "vwap_reclaim", "retest_breakout_confirmed", "higher_low_continuation",
        "relative_strength_vs_btc", "wave_3", "support_bounce_confirmed",
        "failed_breakdown_trap", "liquidity_sweep_reclaim",
    ]
    setup = next((p for p in preferred if p in setup_parts), setup_parts[-1] if setup_parts else "Setup N/A")
    mapping = {
        "vwap_reclaim": "VWAP Reclaim",
        "retest_breakout_confirmed": "Retest Breakout",
        "higher_low_continuation": "Higher Low",
        "relative_strength_vs_btc": "RS vs BTC",
        "wave_3": "Wave 3",
        "support_bounce_confirmed": "Support Bounce",
        "failed_breakdown_trap": "Failed Breakdown Trap",
        "liquidity_sweep_reclaim": "Liquidity Sweep",
    }
    setup = mapping.get(str(setup), setup.replace("_", " ").title())

    if phase == "trailing":
        phase_line = "Trailing Active"
    elif phase == "tp2_hit":
        phase_line = "TP2 Hit"
    elif phase == "tp1_hit":
        phase_line = "TP1 Hit"
    elif phase == "pending_pullback":
        phase_line = "Pending Pullback"
    else:
        phase_line = "Before TP1"

    pnl_icon = "🟢" if pnl > 0.05 else "🔴" if pnl < -0.05 else "🟡"
    header = f"{pnl_icon} <b>{sym}</b> | {pnl:+.2f}%"
    lines = [
        header,
        f"⏱️ {age} | 📍 {phase_line}",
        f"🎯 TP1: {_fmt_price_perf(tp1)} | 🏁 TP2: {_fmt_price_perf(tp2)}",
    ]
    if sl_is_entry:
        lines.append(f"🛡 SL Entry | ⭐ {score:.2f}")
    else:
        lines.append(f"🛡 SL: {_fmt_price_perf(sl)} | ⭐ {score:.2f}")
    lines.append(f"🧠 {setup}")
    if tv_link:
        lines.append(f'🔗 <a href="{tv_link}">TradingView</a>')
    return lines


def _open_trade_near_tp1(t: dict) -> bool:
    try:
        if t.get("phase") != "open":
            return False
        tp1_pct = abs(safe_float(t.get("tp1_pct", 0.0), 0.0))
        cur = safe_float(t.get("current_pnl_pct", 0.0), 0.0)
        return tp1_pct > 0 and cur >= max(0.5, tp1_pct * 0.75)
    except Exception:
        return False


def _open_trade_is_danger(t: dict) -> bool:
    try:
        phase = str(t.get("phase", "open") or "open")
        if phase == "pending_pullback":
            return False
        pnl = safe_float(t.get("current_pnl_pct", 0.0), 0.0)
        sl_pct = safe_float(t.get("sl_pct", 0.0), 0.0)
        return pnl <= -0.75 or (sl_pct < 0 and pnl <= abs(sl_pct) * -0.70)
    except Exception:
        return False


def _format_open_trades_section(title: str, items: List[dict], start_index: int = 1, limit: int = 8) -> List[str]:
    lines = []
    if not items:
        return lines
    lines.extend(["", f"{title}", "┄┄┄┄┄┄"])
    for offset, trade in enumerate(items[:limit], start=start_index):
        lines.extend(_format_open_trade_compact_card(trade, index=offset))
        if offset < start_index + min(len(items), limit) - 1:
            lines.append("┄┄┄┄┄┄")
    if len(items) > limit:
        lines.append(f"… +{len(items) - limit} صفقات أخرى")
    return lines


def format_open_trades_message(
    trades: List[dict],
    side: str = "long",
    market_mode: str = None,
    weak_drift: str = None,
    execution_status: str = None,
    period_label: str = "الآن",
) -> str:
    """Official compact /open_trades dashboard style."""
    if not trades:
        return "📋 <b>لا توجد صفقات مفتوحة حاليًا</b>"

    total = len(trades)
    winners = [t for t in trades if safe_float(t.get("weighted_pnl_pct", t.get("current_pnl_pct", 0.0)), 0.0) > 0.05]
    losers = [t for t in trades if safe_float(t.get("weighted_pnl_pct", t.get("current_pnl_pct", 0.0)), 0.0) < -0.05]
    pending = [t for t in trades if t.get("phase") == "pending_pullback"]
    tp1_protected = [t for t in trades if t.get("phase") in ("tp1_hit", "tp2_hit", "trailing") or bool(t.get("sl_is_entry", False))]
    tp1_hit_count = sum(1 for t in trades if t.get("phase") in ("tp1_hit", "tp2_hit", "trailing") or bool(t.get("tp1_hit", False)))
    tp2_hit_count = sum(1 for t in trades if t.get("phase") in ("tp2_hit", "trailing") or bool(t.get("tp2_hit", False)))
    trailing_count = sum(1 for t in trades if t.get("phase") == "trailing")
    near_tp1 = [t for t in trades if _open_trade_near_tp1(t)]
    danger = [t for t in trades if _open_trade_is_danger(t)]

    def _wp(t):
        return safe_float(t.get("weighted_pnl_pct", t.get("current_pnl_pct", 0.0)), 0.0)

    def _wl(t):
        return safe_float(t.get("weighted_pnl_leveraged", t.get("current_pnl_leveraged", 0.0)), 0.0)

    def _avg_age(items):
        if not items:
            return "0m"
        now = int(time.time())
        ages = []
        for item in items:
            ts = safe_timestamp(item.get("created_at", 0), 0)
            if ts > 0:
                ages.append(max(0, now - ts))
        if not ages:
            return "0m"
        mins = int(sum(ages) / len(ages)) // 60
        if mins < 60:
            return f"{mins}m"
        if mins < 1440:
            return f"{mins // 60}h {mins % 60}m"
        return f"{mins // 1440}d {(mins % 1440) // 60}h"

    net_pnl = sum(_wp(t) for t in trades)
    net_lev = sum(_wl(t) for t in trades)
    best = max(trades, key=_wp) if trades else None
    worst = min(trades, key=_wp) if trades else None
    best_text = f"{str(best.get('symbol','?')).replace('-SWAP','')} {_wp(best):+.2f}%" if best else "—"
    worst_text = f"{str(worst.get('symbol','?')).replace('-SWAP','')} {_wp(worst):+.2f}%" if worst else "—"

    mode_text = market_mode or "N/A"
    drift_text = (weak_drift or "N/A").replace("🔴 ", "").replace("🟢 ", "")
    exec_text = execution_status or "N/A"
    win_rate = (len(winners) / total * 100.0) if total else 0.0
    avg_score_vals = [safe_float(t.get("score"), 0.0) for t in trades if safe_float(t.get("score"), 0.0) > 0]
    avg_score = (sum(avg_score_vals) / len(avg_score_vals)) if avg_score_vals else 0.0
    protected_count = len(tp1_protected)
    danger_count = len(danger)

    lines = [
        "📋 <b>تقرير الصفقات المفتوحة</b>",
        f"📅 {html.escape(str(period_label or 'الآن'))}",
        "━━━━━━━━━━━━",
        "⚡ جميع نسب الأداء محسوبة على رافعة 15x",
        "📊 <b>Quick Stats</b>",
        f"• Open: {total}",
        f"• Winners: {len(winners)}",
        f"• Losers: {len(losers)}",
        f"• Win Rate: <b>{win_rate:.1f}%</b>",
        f"• Net Floating: <b>{net_pnl:+.2f}%</b>",
        f"🎯 TP1: {tp1_hit_count}",
        f"🏁 TP2: {tp2_hit_count}",
        f"📍 Trailing: {trailing_count}",
        f"🛡 Protected: {protected_count}",
        f"⚠️ Danger: {danger_count}",
        f"⏱ Avg Time: {_avg_age(trades)}",
        f"⭐ Avg Score: {avg_score:.2f}",
        f"🔥 Best: <b>{html.escape(best_text)}</b>",
        f"⚠️ Worst: <b>{html.escape(worst_text)}</b>",
        "━━━━━━━━━━━━",
        "💰 <b>Open Portfolio</b>",
        f"⚖️ Net: <b>{net_pnl:+.2f}%</b> | <b>{net_lev:+.1f}% Exposure</b>",
        f"🧠 Mode: <code>{html.escape(mode_text)}</code> | Weak Drift: <code>{html.escape(drift_text)}</code>",
        f"⚡ Execution: <code>{html.escape(exec_text)}</code>",
        "━━━━━━━━━━━━",
        "📂 <b>Open Trades</b>",
        f"🟢 Open Winners: {len(winners)}",
        f"🔴 Open Losers: {len(losers)}",
    ]

    separator = "┄┄┄┄┄┄"
    winners_s = sorted(winners, key=_wp, reverse=True)
    losers_s = sorted(losers, key=_wp)
    pending_s = sorted(pending, key=lambda t: safe_timestamp(t.get("created_at", 0), 0), reverse=True)

    if winners_s:
        lines.append("")
        for idx, trade in enumerate(winners_s[:5]):
            if idx > 0:
                lines.append(separator)
            lines.extend(_format_open_trade_compact_card(trade))
        if len(winners_s) > 5:
            lines.append(f"📂 +{len(winners_s) - 5} صفقات رابحة مفتوحة أخرى")
    else:
        lines.append("لا توجد صفقات مفتوحة رابحة حاليًا.")

    lines.extend(["━━━━━━━━━━━━", "🔴 <b>Open Losers</b>"])
    if losers_s:
        for idx, trade in enumerate(losers_s[:5]):
            if idx > 0:
                lines.append(separator)
            lines.extend(_format_open_trade_compact_card(trade))
        if len(losers_s) > 5:
            lines.append(f"📂 +{len(losers_s) - 5} صفقات خاسرة مفتوحة أخرى")
    else:
        lines.append("لا توجد صفقات مفتوحة خاسرة حاليًا.")

    if pending_s:
        lines.extend(["━━━━━━━━━━━━", "⏳ <b>Pending Pullback</b>"])
        for idx, trade in enumerate(pending_s[:5]):
            if idx > 0:
                lines.append(separator)
            lines.extend(_format_open_trade_compact_card(trade))
        if len(pending_s) > 5:
            lines.append(f"📂 +{len(pending_s) - 5} صفقات Pending أخرى")

    lines.extend(["━━━━━━━━━━━━", "💡 يعتمد على نظام إدارة 40/40/20"])

    return "\n".join(lines)


def _fmt_price_perf(value) -> str:
    """تنسيق سعر ديناميكي."""
    v = safe_float(value, 0.0)
    if v <= 0:
        return "—"
    if v >= 100:
        return f"{v:.4f}"
    if v >= 1:
        return f"{v:.6f}"
    return f"{v:.8f}"


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
    triggered_tp1_hits = sum(1 for t in triggered_closed_trades if _is_tp1_hit(t))
    triggered_tp2_hits = sum(1 for t in triggered_closed_trades if _is_tp2_hit(t))
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
