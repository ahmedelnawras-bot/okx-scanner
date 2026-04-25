import time
import logging
from collections import Counter, defaultdict

from tracking.performance import (
    get_all_trades_data,
    summarize_by_field,
    get_common_loss_reasons,
)

from tracking.summary_helpers import (
    safe_float,
    safe_int,
    safe_bool,
    build_empty_summary,
    apply_trade_to_summary,
    finalize_summary,
)

logger = logging.getLogger("okx-scanner")


# =========================
# BASIC HELPERS
# =========================
def normalize_market_type(market_type: str) -> str:
    market_type = (market_type or "futures").strip().lower()
    if market_type not in ("futures", "spot"):
        return "futures"
    return market_type


def normalize_bool(value) -> bool:
    return safe_bool(value, default=False)


def safe_str(value, default="unknown") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def safe_pct(value) -> str:
    try:
        return f"{float(value):.2f}%".replace(".00%", ".0%")
    except Exception:
        return "0.0%"


def get_period_since_ts(period: str):
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

    if period in ("30d", "month"):
        return now_ts - (30 * 24 * 3600)

    return None


def format_period_ar(period: str) -> str:
    mapping = {
        "1h": "آخر ساعة",
        "today": "اليوم",
        "1d": "آخر 24 ساعة",
        "7d": "آخر 7 أيام",
        "30d": "آخر 30 يوم",
        "month": "آخر 30 يوم",
        "all": "كل الفترة",
    }
    return mapping.get(str(period or "all"), str(period or "all"))


def format_market_side_ar(market_type: str, side: str) -> str:
    market_map = {
        "futures": "فيوتشر",
        "spot": "سبوت",
    }
    side_map = {
        "long": "لونج",
        "short": "شورت",
    }
    return f"{market_map.get(market_type, market_type)}/{side_map.get(side, side)}"


# =========================
# ARABIC FORMAT HELPERS
# =========================
def format_setup_type_ar(setup_type) -> str:
    if not setup_type:
        return "غير معروف"

    mapping = {
        "continuation": "استمرار",
        "breakout": "اختراق",
        "pre_breakout": "اختراق مبكر",
        "reverse": "ارتداد عكسي",
        "recovery": "ريكافري",

        "mtf_yes": "تأكيد 1H: نعم",
        "mtf_no": "تأكيد 1H: لا",

        "vol_high": "فوليوم عالي",
        "vol_mid": "فوليوم متوسط",
        "vol_low": "فوليوم ضعيف",

        "bull_market": "سوق صاعد",
        "alt_season": "موسم ألت",
        "mixed": "سوق مختلط",
        "btc_leading": "BTC يقود",
        "risk_off": "سوق دفاعي",
        "post_crash": "بعد كراش",
        "unknown": "غير معروف",
    }

    parts = str(setup_type).split("|")
    formatted = [mapping.get(part.strip(), part.strip()) for part in parts if part.strip()]
    return " | ".join(formatted) if formatted else "غير معروف"


def format_market_state_ar(value) -> str:
    if value is None:
        return "غير معروف"

    text = str(value).strip()
    if not text:
        return "غير معروف"

    mapping = {
        "bull_market": "سوق صاعد",
        "alt_season": "موسم ألت",
        "mixed": "سوق مختلط",
        "btc_leading": "BTC يقود",
        "risk_off": "سوق دفاعي",
        "post_crash": "بعد كراش",
        "unknown": "غير معروف",
    }
    return mapping.get(text, text)


def format_entry_timing_ar(value) -> str:
    if value is None:
        return "غير معروف"

    text = str(value).strip()
    if not text:
        return "غير معروف"

    if any(x in text for x in ["مبكر", "متوسط", "متأخر", "ريكافري", "🟢", "🟡", "🔴", "🛟"]):
        return text

    lower = text.lower()

    if "recovery" in lower:
        return "🛟 دخول ريكافري"
    if "early" in lower:
        return "🟢 مبكر"
    if "late" in lower:
        return "🔴 متأخر"
    if "medium" in lower or "mid" in lower:
        return "🟡 متوسط"

    return text


def format_opportunity_ar(value) -> str:
    if value is None:
        return "غير معروف"

    text = str(value).strip()
    if not text:
        return "غير معروف"

    mapping = {
        "continuation": "استمرار",
        "استمرار": "استمرار",
        "breakout": "اختراق",
        "Breakout": "اختراق",
        "pre_breakout": "اختراق مبكر",
        "Breakout مبكر": "اختراق مبكر",
        "reverse": "ارتداد عكسي",
        "Oversold Reversal": "ارتداد من تشبع بيع",
        "Recovery Long": "ريكافري لونج",
    }
    return mapping.get(text, text)


def format_reason_ar(value) -> str:
    if value is None:
        return "غير معروف"

    text = str(value).strip()
    if not text:
        return "غير معروف"

    mapping = {
        "Volume Spike": "فوليوم انفجاري",
        "volume_spike": "فوليوم انفجاري",
        "High Volume": "فوليوم عالي",
        "فوليوم انفجار": "فوليوم انفجاري",
        "فوليوم انفجاري": "فوليوم انفجاري",
        "فوليوم داعم": "فوليوم داعم",
        "فوليوم قوي": "فوليوم قوي",

        "Above MA": "فوق المتوسط",
        "فوق MA": "فوق المتوسط",
        "فوق المتوسط": "فوق المتوسط",
        "أسفل المتوسط": "أسفل المتوسط",

        "MTF Confirmed": "تأكيد فريم الساعة",
        "تأكيد فريم الساعة": "تأكيد فريم الساعة",

        "Early Trend": "بداية ترند مبكرة",
        "بداية ترند مبكرة": "بداية ترند مبكرة",

        "RSI Strong": "RSI صاعد بقوة",
        "RSI صاعد بقوة": "RSI صاعد بقوة",
        "RSI High": "RSI عالي",
        "RSI عالي": "RSI عالي",
        "RSI صحي": "RSI في منطقة صحية",
        "RSI جيد": "RSI جيد",
        "RSI مرتفع لكن بزخم": "RSI مرتفع بزخم",

        "Late Pump Risk": "خطر مطاردة Pump متأخر",
        "Bull Market Continuation Risk": "استمرار في سوق صاعد بعد امتداد خطر",
        "Momentum Exhaustion Trap": "خطر نهاية الزخم بعد Pump",
        "Weak Historical Setup": "نوع إشارة ضعيف تاريخيًا",

        "far_from_vwap": "بعيد عن VWAP",
        "rsi_slope_weak": "RSI بدأ يضعف",
        "macd_hist_falling": "زخم MACD يتراجع",
        "macd_hist_negative": "MACD سلبي",

        "RECOVERY_LONG": "ريكافري لونج",
        "POST_CRASH_REBOUND": "ارتداد بعد كراش",
        "OVERSOLD_REVERSAL": "ارتداد من تشبع بيع",

        "BTC داعم": "BTC داعم",
        "BTC غير داعم": "BTC غير داعم",
        "هيمنة داعمة": "هيمنة داعمة للألت",
        "هيمنة ضد الألت": "هيمنة ضد الألت",
        "تمويل سلبي": "تمويل سلبي",
        "تمويل إيجابي": "تمويل إيجابي",
        "عملة جديدة": "عملة جديدة",
        "اختراق": "اختراق",
        "اختراق مبكر جداً": "اختراق مبكر",
        "اختراق متأخر": "اختراق متأخر",
        "اختراق قوي مؤكد": "اختراق قوي مؤكد",
        "شمعة جيدة": "شمعة جيدة",
        "شمعة قوية": "شمعة قوية",
    }

    return mapping.get(text, text)


def format_score_bucket_ar(value) -> str:
    if value is None:
        return "غير معروف"

    text = str(value).strip()
    if not text:
        return "غير معروف"

    replacements = {
        "under": "أقل من",
        "above": "أعلى من",
        "to": "إلى",
    }

    out = text
    for old, new in replacements.items():
        out = out.replace(old, new)

    return out


def format_label_by_kind(label, kind: str) -> str:
    if kind == "setup":
        return format_setup_type_ar(label)
    if kind == "market":
        return format_market_state_ar(label)
    if kind == "timing":
        return format_entry_timing_ar(label)
    if kind == "score":
        return format_score_bucket_ar(label)
    if kind == "opportunity":
        return format_opportunity_ar(label)
    if kind == "reason":
        return format_reason_ar(label)
    return safe_str(label, "غير معروف")


# =========================
# SUMMARY WRAPPERS
# =========================
def _empty_summary(label="unknown"):
    summary = build_empty_summary()
    summary["label"] = label
    summary["total"] = summary.get("signals", 0)
    summary["tp1_wins"] = summary.get("tp1_wins", 0)
    summary["tp2_wins"] = summary.get("tp2_wins", 0)
    return summary


def _apply_trade_to_summary(summary: dict, trade: dict):
    if not isinstance(summary, dict):
        return summary

    if not trade or not isinstance(trade, dict):
        return summary

    apply_trade_to_summary(summary, trade)

    summary["total"] = summary.get("signals", summary.get("total", 0))
    summary["tp1_wins"] = summary.get("tp1_wins", 0)
    summary["tp2_wins"] = summary.get("tp2_wins", 0)

    return summary


def _finalize_summary(summary: dict):
    if not isinstance(summary, dict):
        return summary

    finalize_summary(summary)

    summary["total"] = summary.get("signals", summary.get("total", 0))
    summary["tp1_wins"] = summary.get("tp1_wins", 0)
    summary["tp2_wins"] = summary.get("tp2_wins", summary.get("tp2_hits", 0))

    return summary


def _normalize_summary_row(row: dict, label=None) -> dict:
    if not isinstance(row, dict):
        row = {}

    safe_label = label
    if safe_label is None:
        safe_label = row.get("label", row.get("field_value", "unknown"))

    signals = safe_int(row.get("signals", row.get("total", 0)), 0)
    total = safe_int(row.get("total", signals), signals)

    return {
        "label": safe_str(safe_label, "unknown"),
        "total": total,
        "signals": signals,
        "closed": safe_int(row.get("closed", 0), 0),
        "wins": safe_int(row.get("wins", 0), 0),
        "tp1_wins": safe_int(row.get("tp1_wins", 0), 0),
        "tp2_wins": safe_int(row.get("tp2_wins", row.get("tp2_hits", 0)), 0),
        "losses": safe_int(row.get("losses", 0), 0),
        "expired": safe_int(row.get("expired", 0), 0),
        "open": safe_int(row.get("open", 0), 0),
        "tp1_hits": safe_int(row.get("tp1_hits", 0), 0),
        "tp2_hits": safe_int(row.get("tp2_hits", row.get("tp2_wins", 0)), 0),
        "winrate": round(safe_float(row.get("winrate", 0.0), 0.0), 2),
        "tp1_rate": round(safe_float(row.get("tp1_rate", 0.0), 0.0), 2),
    }


def _sort_summary_rows(rows):
    normalized = [_normalize_summary_row(row) for row in (rows or [])]

    return sorted(
        normalized,
        key=lambda x: (
            x.get("winrate", 0.0),
            x.get("closed", 0),
            x.get("wins", 0),
            -x.get("losses", 0),
        ),
        reverse=True,
    )


def _sort_worst_summary_rows(rows):
    normalized = [_normalize_summary_row(row) for row in (rows or [])]

    return sorted(
        normalized,
        key=lambda x: (
            x.get("winrate", 0.0),
            -x.get("losses", 0),
            -x.get("closed", 0),
            x.get("wins", 0),
        ),
    )


def _score_bucket_sort_key(row):
    label = str(row.get("label", ""))
    if label.startswith("<"):
        return 0
    if "5.5" in label:
        return 1
    if "6.0" in label:
        return 2
    if "6.5" in label:
        return 3
    if "7.0" in label:
        return 4
    if "7.5" in label:
        return 5
    if "8.0" in label or "+8" in label:
        return 6
    return 99


def _format_summary_line(row: dict, label_kind: str = "") -> str:
    row = _normalize_summary_row(row)
    label = format_label_by_kind(row.get("label"), label_kind)

    return (
        f"• {label}\n"
        f"  مغلقة: {row.get('closed', 0)} | "
        f"رابحة: {row.get('wins', 0)} | "
        f"خاسرة: {row.get('losses', 0)} | "
        f"نسبة النجاح: {safe_pct(row.get('winrate', 0.0))}"
    )


def _format_named_rows_ar(rows, label_kind: str = "", top_n=5):
    if not rows:
        return "لا توجد بيانات كافية"

    lines = []
    for row in rows[:top_n]:
        lines.append(_format_summary_line(row, label_kind=label_kind))

    return "\n\n".join(lines)


def _format_counter_rows_ar(rows, icon="•", empty_text="لا توجد بيانات", label_kind="reason"):
    if not rows:
        return empty_text

    lines = []
    for label, count in rows:
        formatted_label = format_label_by_kind(label, label_kind)
        lines.append(f"{icon} {formatted_label}: {safe_int(count, 0)}")

    return "\n".join(lines)


def _diagnostics_value(trade: dict, field_name: str, default="unknown"):
    if not isinstance(trade, dict):
        return default

    if field_name in trade:
        value = trade.get(field_name)
    else:
        diagnostics = trade.get("diagnostics", {}) or {}
        value = diagnostics.get(field_name)

    return safe_str(value, default=default)


def _bucket_score(score: float) -> str:
    s = safe_float(score, 0.0)

    if s < 5.5:
        return "< 5.5"
    if s < 6.0:
        return "5.5 - 5.99"
    if s < 6.5:
        return "6.0 - 6.49"
    if s < 7.0:
        return "6.5 - 6.99"
    if s < 7.5:
        return "7.0 - 7.49"
    if s < 8.0:
        return "7.5 - 7.99"

    return "8.0+"


def _bucket_dist_ma(dist_ma: float) -> str:
    d = safe_float(dist_ma, 0.0)

    if d <= -6:
        return "<= -6%"
    if d <= -4:
        return "-6% إلى -4%"
    if d <= -2:
        return "-4% إلى -2%"
    if d < 0:
        return "-2% إلى 0%"
    if d <= 2:
        return "0% إلى 2%"
    if d <= 4:
        return "2% إلى 4%"
    if d <= 6:
        return "4% إلى 6%"

    return "> 6%"


def _bucket_vol_ratio(vol_ratio: float) -> str:
    v = safe_float(vol_ratio, 0.0)

    if v < 1.0:
        return "< 1.0"
    if v < 1.2:
        return "1.0 - 1.19"
    if v < 1.5:
        return "1.2 - 1.49"
    if v < 2.0:
        return "1.5 - 1.99"

    return "2.0+"


# =========================
# CORE SEGMENTATION
# =========================
def summarize_trades_by_custom_bucket(
    redis_client,
    bucket_getter,
    market_type: str = "futures",
    side: str = "long",
    since_ts: int = None,
    min_closed: int = 1,
):
    rows = get_all_trades_data(
        redis_client=redis_client,
        market_type=market_type,
        side=side,
        since_ts=since_ts,
        use_history=False,
    )

    grouped = defaultdict(lambda: _empty_summary())

    for trade in rows or []:
        try:
            label = safe_str(bucket_getter(trade), "unknown")
        except Exception:
            label = "unknown"

        grouped[label]["label"] = label
        _apply_trade_to_summary(grouped[label], trade)

    output = []

    for label, summary in grouped.items():
        summary["label"] = label
        _finalize_summary(summary)

        if safe_int(summary.get("closed", 0), 0) < min_closed:
            continue

        output.append(_normalize_summary_row(summary, label=label))

    return _sort_summary_rows(output)


def get_setup_diagnostics(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    since_ts: int = None,
    min_closed: int = 3,
):
    rows = summarize_by_field(
        redis_client=redis_client,
        field_name="setup_type",
        market_type=market_type,
        side=side,
        since_ts=since_ts,
        min_closed=min_closed,
        use_history=True,
    )

    output = []

    for row in rows or []:
        label = row.get("field_value", row.get("label", "unknown"))
        normalized = _normalize_summary_row(row, label=label)

        if normalized["closed"] < min_closed:
            continue

        output.append(normalized)

    return _sort_summary_rows(output)


def get_score_diagnostics(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    since_ts: int = None,
    min_closed: int = 3,
):
    return summarize_trades_by_custom_bucket(
        redis_client=redis_client,
        bucket_getter=lambda trade: _bucket_score(trade.get("score", 0)),
        market_type=market_type,
        side=side,
        since_ts=since_ts,
        min_closed=min_closed,
    )


def get_market_state_diagnostics(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    since_ts: int = None,
    min_closed: int = 3,
):
    return summarize_trades_by_custom_bucket(
        redis_client=redis_client,
        bucket_getter=lambda trade: _diagnostics_value(trade, "market_state"),
        market_type=market_type,
        side=side,
        since_ts=since_ts,
        min_closed=min_closed,
    )


def get_entry_timing_diagnostics(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    since_ts: int = None,
    min_closed: int = 3,
):
    return summarize_trades_by_custom_bucket(
        redis_client=redis_client,
        bucket_getter=lambda trade: _diagnostics_value(trade, "entry_timing"),
        market_type=market_type,
        side=side,
        since_ts=since_ts,
        min_closed=min_closed,
    )


def get_opportunity_type_diagnostics(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    since_ts: int = None,
    min_closed: int = 3,
):
    return summarize_trades_by_custom_bucket(
        redis_client=redis_client,
        bucket_getter=lambda trade: _diagnostics_value(trade, "opportunity_type"),
        market_type=market_type,
        side=side,
        since_ts=since_ts,
        min_closed=min_closed,
    )


def get_early_priority_diagnostics(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    since_ts: int = None,
    min_closed: int = 3,
):
    return summarize_trades_by_custom_bucket(
        redis_client=redis_client,
        bucket_getter=lambda trade: _diagnostics_value(trade, "early_priority"),
        market_type=market_type,
        side=side,
        since_ts=since_ts,
        min_closed=min_closed,
    )


def get_breakout_quality_diagnostics(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    since_ts: int = None,
    min_closed: int = 3,
):
    return summarize_trades_by_custom_bucket(
        redis_client=redis_client,
        bucket_getter=lambda trade: _diagnostics_value(trade, "breakout_quality"),
        market_type=market_type,
        side=side,
        since_ts=since_ts,
        min_closed=min_closed,
    )


def get_dist_ma_diagnostics(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    since_ts: int = None,
    min_closed: int = 3,
):
    return summarize_trades_by_custom_bucket(
        redis_client=redis_client,
        bucket_getter=lambda trade: _bucket_dist_ma(
            (trade.get("diagnostics", {}) or {}).get("dist_ma", 0.0)
        ),
        market_type=market_type,
        side=side,
        since_ts=since_ts,
        min_closed=min_closed,
    )


def get_volume_ratio_diagnostics(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    since_ts: int = None,
    min_closed: int = 3,
):
    return summarize_trades_by_custom_bucket(
        redis_client=redis_client,
        bucket_getter=lambda trade: _bucket_vol_ratio(trade.get("vol_ratio", 0.0)),
        market_type=market_type,
        side=side,
        since_ts=since_ts,
        min_closed=min_closed,
    )


def get_boolean_flag_diagnostics(
    redis_client,
    field_name: str,
    market_type: str = "futures",
    side: str = "long",
    since_ts: int = None,
    min_closed: int = 3,
):
    def _bucket(trade):
        if not isinstance(trade, dict):
            return "no"

        if field_name in trade:
            value = trade.get(field_name)
        else:
            value = (trade.get("diagnostics", {}) or {}).get(field_name)

        return "yes" if normalize_bool(value) else "no"

    return summarize_trades_by_custom_bucket(
        redis_client=redis_client,
        bucket_getter=_bucket,
        market_type=market_type,
        side=side,
        since_ts=since_ts,
        min_closed=min_closed,
    )


# =========================
# ADVANCED INSIGHTS
# =========================
def get_top_and_bottom_setups(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    since_ts: int = None,
    min_closed: int = 5,
    top_n: int = 5,
):
    rows = get_setup_diagnostics(
        redis_client=redis_client,
        market_type=market_type,
        side=side,
        since_ts=since_ts,
        min_closed=min_closed,
    )

    best = _sort_summary_rows(rows)[:top_n]
    worst = _sort_worst_summary_rows(rows)[:top_n]

    return {
        "best": best,
        "worst": worst,
    }


def get_loss_clusters(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    since_ts: int = None,
    top_n: int = 10,
):
    trades = get_all_trades_data(
        redis_client=redis_client,
        market_type=market_type,
        side=side,
        since_ts=since_ts,
        use_history=False,
    )

    market_counter = Counter()
    timing_counter = Counter()
    setup_counter = Counter()
    opp_counter = Counter()

    loss_trades = []

    for trade in trades or []:
        result = safe_str(trade.get("result", ""), "").lower()
        if result != "loss":
            continue

        loss_trades.append(trade)

        diagnostics = trade.get("diagnostics", {}) or {}

        market_counter[safe_str(diagnostics.get("market_state"))] += 1
        timing_counter[safe_str(diagnostics.get("entry_timing"))] += 1
        setup_counter[safe_str(trade.get("setup_type"))] += 1
        opp_counter[safe_str(diagnostics.get("opportunity_type"))] += 1

    return {
        "market_states": market_counter.most_common(top_n),
        "entry_timing": timing_counter.most_common(top_n),
        "setup_types": setup_counter.most_common(top_n),
        "opportunity_types": opp_counter.most_common(top_n),
        "loss_reasons": get_common_loss_reasons(
            redis_client=redis_client,
            market_type=market_type,
            side=side,
            since_ts=since_ts,
            top_n=top_n,
            trades=loss_trades,
        ),
    }


def build_quick_diagnostics_snapshot(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    since_ts: int = None,
):
    score_rows = get_score_diagnostics(
        redis_client=redis_client,
        market_type=market_type,
        side=side,
        since_ts=since_ts,
        min_closed=3,
    )

    market_rows = get_market_state_diagnostics(
        redis_client=redis_client,
        market_type=market_type,
        side=side,
        since_ts=since_ts,
        min_closed=3,
    )

    timing_rows = get_entry_timing_diagnostics(
        redis_client=redis_client,
        market_type=market_type,
        side=side,
        since_ts=since_ts,
        min_closed=3,
    )

    best_score_rows = _sort_summary_rows(score_rows)
    worst_score_rows = _sort_worst_summary_rows(score_rows)

    best_market_rows = _sort_summary_rows(market_rows)
    worst_market_rows = _sort_worst_summary_rows(market_rows)

    best_timing_rows = _sort_summary_rows(timing_rows)
    worst_timing_rows = _sort_worst_summary_rows(timing_rows)

    return {
        "best_score_bucket": best_score_rows[0] if best_score_rows else None,
        "worst_score_bucket": worst_score_rows[0] if worst_score_rows else None,
        "best_market_state": best_market_rows[0] if best_market_rows else None,
        "worst_market_state": worst_market_rows[0] if worst_market_rows else None,
        "best_entry_timing": best_timing_rows[0] if best_timing_rows else None,
        "worst_entry_timing": worst_timing_rows[0] if worst_timing_rows else None,
    }


def get_overall_summary(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    since_ts: int = None,
):
    rows = get_all_trades_data(
        redis_client=redis_client,
        market_type=market_type,
        side=side,
        since_ts=since_ts,
        use_history=False,
    )

    summary = _empty_summary("overall")

    for trade in rows or []:
        _apply_trade_to_summary(summary, trade)

    _finalize_summary(summary)
    return _normalize_summary_row(summary, label="overall")


# =========================
# REPORT BUILDERS
# =========================
def build_setups_report(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    period: str = "all",
):
    try:
        since_ts = get_period_since_ts(period)

        data = get_top_and_bottom_setups(
            redis_client=redis_client,
            market_type=market_type,
            side=side,
            since_ts=since_ts,
            min_closed=5,
            top_n=5,
        )

        best_rows = [r for r in data.get("best", []) if safe_int(r.get("closed", 0), 0) > 0]
        worst_rows = [r for r in data.get("worst", []) if safe_int(r.get("closed", 0), 0) > 0]

        best_text = _format_named_rows_ar(best_rows, label_kind="setup", top_n=5)
        worst_text = _format_named_rows_ar(worst_rows, label_kind="setup", top_n=5)

        return (
            f"🧩 <b>تحليل أنواع الإشارات</b>\n"
            f"السوق: {format_market_side_ar(market_type, side)}\n"
            f"الفترة: {format_period_ar(period)}\n\n"
            f"🟢 <b>أفضل الأنواع:</b>\n"
            f"{best_text}\n\n"
            f"🔴 <b>أضعف الأنواع:</b>\n"
            f"{worst_text}"
        )

    except Exception as e:
        logger.error(f"build_setups_report error: {e}")
        return "❌ حصل خطأ أثناء بناء التقرير"


def build_scores_report(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    period: str = "all",
):
    try:
        since_ts = get_period_since_ts(period)

        rows = get_score_diagnostics(
            redis_client=redis_client,
            market_type=market_type,
            side=side,
            since_ts=since_ts,
            min_closed=3,
        )

        rows = sorted(rows or [], key=_score_bucket_sort_key)

        if not rows:
            score_text = "لا توجد بيانات كافية لهذا التقرير بعد."
        else:
            score_text = _format_named_rows_ar(rows, label_kind="score", top_n=10)

        return (
            f"⭐ <b>تحليل الأداء حسب السكور</b>\n"
            f"السوق: {format_market_side_ar(market_type, side)}\n"
            f"الفترة: {format_period_ar(period)}\n\n"
            f"{score_text}"
        )

    except Exception as e:
        logger.error(f"build_scores_report error: {e}")
        return "❌ حصل خطأ أثناء بناء التقرير"


def build_market_report(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    period: str = "all",
):
    try:
        since_ts = get_period_since_ts(period)

        market_rows = get_market_state_diagnostics(
            redis_client=redis_client,
            market_type=market_type,
            side=side,
            since_ts=since_ts,
            min_closed=3,
        )

        timing_rows = get_entry_timing_diagnostics(
            redis_client=redis_client,
            market_type=market_type,
            side=side,
            since_ts=since_ts,
            min_closed=3,
        )

        market_text = _format_named_rows_ar(market_rows, label_kind="market", top_n=6)
        timing_text = _format_named_rows_ar(timing_rows, label_kind="timing", top_n=6)

        return (
            f"🌍 <b>تحليل الأداء حسب حالة السوق والدخول</b>\n"
            f"السوق: {format_market_side_ar(market_type, side)}\n"
            f"الفترة: {format_period_ar(period)}\n\n"
            f"📌 <b>حسب حالة السوق:</b>\n"
            f"{market_text}\n\n"
            f"⏱ <b>حسب توقيت الدخول:</b>\n"
            f"{timing_text}"
        )

    except Exception as e:
        logger.error(f"build_market_report error: {e}")
        return "❌ حصل خطأ أثناء بناء التقرير"


def build_losses_report(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    period: str = "all",
):
    try:
        since_ts = get_period_since_ts(period)

        clusters = get_loss_clusters(
            redis_client=redis_client,
            market_type=market_type,
            side=side,
            since_ts=since_ts,
            top_n=10,
        )

        reason_text = _format_counter_rows_ar(
            clusters.get("loss_reasons", []),
            icon="•",
            empty_text="لا توجد بيانات كافية",
            label_kind="reason",
        )

        market_text = _format_counter_rows_ar(
            clusters.get("market_states", []),
            icon="•",
            empty_text="لا توجد بيانات كافية",
            label_kind="market",
        )

        timing_text = _format_counter_rows_ar(
            clusters.get("entry_timing", []),
            icon="•",
            empty_text="لا توجد بيانات كافية",
            label_kind="timing",
        )

        setup_text = _format_counter_rows_ar(
            clusters.get("setup_types", []),
            icon="•",
            empty_text="لا توجد بيانات كافية",
            label_kind="setup",
        )

        return (
            f"⚠️ <b>تحليل أسباب الخسارة</b>\n"
            f"السوق: {format_market_side_ar(market_type, side)}\n"
            f"الفترة: {format_period_ar(period)}\n\n"
            f"🔴 <b>أكثر الأسباب تكرارًا في الصفقات الخاسرة:</b>\n"
            f"{reason_text}\n\n"
            f"🌍 <b>الخسائر حسب حالة السوق:</b>\n"
            f"{market_text}\n\n"
            f"⏱ <b>الخسائر حسب توقيت الدخول:</b>\n"
            f"{timing_text}\n\n"
            f"🧩 <b>الخسائر حسب نوع الإشارة:</b>\n"
            f"{setup_text}\n\n"
            f"📌 <b>قراءة سريعة:</b>\n"
            f"لو تكرر سبب مثل فوليوم انفجاري + فوق المتوسط كثيرًا في الخسائر، فهذا غالبًا يعني دخول متأخر بعد Pump."
        )

    except Exception as e:
        logger.error(f"build_losses_report error: {e}")
        return "❌ حصل خطأ أثناء بناء التقرير"


def build_full_diagnostics_report(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    period: str = "all",
):
    try:
        since_ts = get_period_since_ts(period)

        snapshot = build_quick_diagnostics_snapshot(
            redis_client=redis_client,
            market_type=market_type,
            side=side,
            since_ts=since_ts,
        )

        setup_data = get_top_and_bottom_setups(
            redis_client=redis_client,
            market_type=market_type,
            side=side,
            since_ts=since_ts,
            min_closed=5,
            top_n=3,
        )

        losses = get_loss_clusters(
            redis_client=redis_client,
            market_type=market_type,
            side=side,
            since_ts=since_ts,
            top_n=5,
        )

        best_score = snapshot.get("best_score_bucket")
        worst_score = snapshot.get("worst_score_bucket")
        best_market = snapshot.get("best_market_state")
        worst_market = snapshot.get("worst_market_state")
        best_timing = snapshot.get("best_entry_timing")
        worst_timing = snapshot.get("worst_entry_timing")

        def _best_worst_line(row, prefix: str, kind: str):
            if not row:
                return f"• {prefix}: لا توجد بيانات"
            label = format_label_by_kind(row.get("label"), kind)
            return f"• {prefix}: {label} | نسبة النجاح: {safe_pct(row.get('winrate', 0.0))}"

        best_setups_text = _format_named_rows_ar(
            setup_data.get("best", []),
            label_kind="setup",
            top_n=3,
        )

        worst_setups_text = _format_named_rows_ar(
            setup_data.get("worst", []),
            label_kind="setup",
            top_n=3,
        )

        loss_reasons_text = _format_counter_rows_ar(
            losses.get("loss_reasons", []),
            icon="•",
            empty_text="لا توجد بيانات كافية",
            label_kind="reason",
        )

        lines = [
            f"🧠 <b>تقرير تشخيصي شامل</b>",
            f"السوق: {format_market_side_ar(market_type, side)}",
            f"الفترة: {format_period_ar(period)}",
            "",
            "🎯 <b>الأفضل / الأسوأ حسب السكور:</b>",
            _best_worst_line(best_score, "الأفضل", "score"),
            _best_worst_line(worst_score, "الأسوأ", "score"),
            "",
            "🌍 <b>الأفضل / الأسوأ حسب حالة السوق:</b>",
            _best_worst_line(best_market, "الأفضل", "market"),
            _best_worst_line(worst_market, "الأسوأ", "market"),
            "",
            "⏱ <b>الأفضل / الأسوأ حسب توقيت الدخول:</b>",
            _best_worst_line(best_timing, "الأفضل", "timing"),
            _best_worst_line(worst_timing, "الأسوأ", "timing"),
            "",
            "🟢 <b>أفضل أنواع الإشارات:</b>",
            best_setups_text,
            "",
            "🔴 <b>أضعف أنواع الإشارات:</b>",
            worst_setups_text,
            "",
            "⚠️ <b>أكثر أسباب الخسارة:</b>",
            loss_reasons_text,
        ]

        return "\n".join(lines)

    except Exception as e:
        logger.error(f"build_full_diagnostics_report error: {e}")
        return "❌ حصل خطأ أثناء بناء التقرير"


def build_deep_report(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    period: str = "all",
):
    try:
        since_ts = get_period_since_ts(period)

        overall = get_overall_summary(
            redis_client=redis_client,
            market_type=market_type,
            side=side,
            since_ts=since_ts,
        )

        setup_data = get_top_and_bottom_setups(
            redis_client=redis_client,
            market_type=market_type,
            side=side,
            since_ts=since_ts,
            min_closed=5,
            top_n=1,
        )

        losses = get_loss_clusters(
            redis_client=redis_client,
            market_type=market_type,
            side=side,
            since_ts=since_ts,
            top_n=1,
        )

        best_setup = setup_data.get("best", [None])[0] if setup_data.get("best") else None
        worst_setup = setup_data.get("worst", [None])[0] if setup_data.get("worst") else None
        top_loss_reason = losses.get("loss_reasons", [None])[0] if losses.get("loss_reasons") else None

        best_setup_text = "لا توجد بيانات كافية"
        if best_setup:
            best_setup_text = (
                f"{format_setup_type_ar(best_setup.get('label'))} | "
                f"نسبة النجاح: {safe_pct(best_setup.get('winrate', 0.0))}"
            )

        worst_setup_text = "لا توجد بيانات كافية"
        if worst_setup:
            worst_setup_text = (
                f"{format_setup_type_ar(worst_setup.get('label'))} | "
                f"نسبة النجاح: {safe_pct(worst_setup.get('winrate', 0.0))}"
            )

        top_loss_text = "لا توجد بيانات كافية"
        if top_loss_reason:
            reason, count = top_loss_reason
            top_loss_text = f"{format_reason_ar(reason)}: {safe_int(count, 0)}"

        return (
            f"📊 <b>تحليل متقدم للأداء</b>\n"
            f"السوق: {format_market_side_ar(market_type, side)}\n"
            f"الفترة: {format_period_ar(period)}\n\n"
            f"📌 <b>الملخص:</b>\n"
            f"• إجمالي الإشارات: {overall.get('signals', 0)}\n"
            f"• الصفقات المغلقة: {overall.get('closed', 0)}\n"
            f"• الرابحة: {overall.get('wins', 0)}\n"
            f"• الخاسرة: {overall.get('losses', 0)}\n"
            f"• المفتوحة: {overall.get('open', 0)}\n"
            f"• المنتهية: {overall.get('expired', 0)}\n"
            f"• نسبة النجاح: {safe_pct(overall.get('winrate', 0.0))}\n"
            f"• TP1 Rate: {safe_pct(overall.get('tp1_rate', 0.0))}\n\n"
            f"🟢 <b>أفضل نوع إشارة:</b>\n"
            f"{best_setup_text}\n\n"
            f"🔴 <b>أضعف نوع إشارة:</b>\n"
            f"{worst_setup_text}\n\n"
            f"⚠️ <b>أكثر سبب خسارة:</b>\n"
            f"{top_loss_text}"
        )

    except Exception as e:
        logger.error(f"build_deep_report error: {e}")
        return "❌ حصل خطأ أثناء بناء التقرير"
