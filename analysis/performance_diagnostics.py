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
    normalize_side,
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

    if period == "30d":
        return now_ts - (30 * 24 * 3600)

    return None


# =========================
# SUMMARY WRAPPERS
# =========================
def _empty_summary(label="unknown"):
    """
    Wrapper متوافق مع التقارير التشخيصية.
    يعتمد على build_empty_summary من tracking.summary_helpers
    ويضيف حقول label / total / tp1_wins / tp2_wins للتوافق مع التنسيق القديم.
    """
    summary = build_empty_summary()
    summary["label"] = label
    summary["total"] = summary.get("signals", 0)

    # حقول توافقية قديمة
    summary["tp1_wins"] = summary.get("tp1_wins", 0)
    summary["tp2_wins"] = summary.get("tp2_wins", 0)

    return summary


def _apply_trade_to_summary(summary: dict, trade: dict):
    """
    تطبيق الصفقة على summary باستخدام helper المركزي.
    لا نعيد حساب tp1_wins/tp2_wins هنا حتى لا يحصل Double Count.
    """
    if not isinstance(summary, dict):
        return summary

    if not trade or not isinstance(trade, dict):
        return summary

    # مهم: apply_trade_to_summary هي التي تزود wins/losses/tp1_hits/tp1_wins/tp2_wins لو الحقول موجودة
    apply_trade_to_summary(summary, trade)

    # compatibility field
    summary["total"] = summary.get("signals", summary.get("total", 0))
    summary["tp1_wins"] = summary.get("tp1_wins", 0)
    summary["tp2_wins"] = summary.get("tp2_wins", 0)

    return summary


def _finalize_summary(summary: dict):
    """
    Finalize مركزي مع المحافظة على total.
    """
    if not isinstance(summary, dict):
        return summary

    finalize_summary(summary)

    summary["total"] = summary.get("signals", summary.get("total", 0))
    summary["tp1_wins"] = summary.get("tp1_wins", 0)
    summary["tp2_wins"] = summary.get("tp2_wins", summary.get("tp2_hits", 0))

    return summary


def _normalize_summary_row(row: dict, label=None) -> dict:
    """
    يحول أي summary row سواء جاء من performance.py أو من diagnostics
    إلى شكل موحد آمن للعرض.
    """
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
    """
    ترتيب الأفضل:
    أعلى winrate ثم عدد closed ثم wins ثم الأقل losses.
    """
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
    """
    ترتيب الأسوأ:
    أقل winrate، ثم الأكثر خسائر، ثم الأكثر closed.
    """
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


def _format_summary_rows(title: str, rows: list, top_n: int = 8) -> str:
    if not rows:
        return f"📊 {title}\nلا توجد بيانات كافية"

    lines = [f"📊 {title}", ""]

    for row in rows[:top_n]:
        row = _normalize_summary_row(row)
        lines.append(
            f"• {row.get('label', 'unknown')}: "
            f"WR {row.get('winrate', 0.0)}% | "
            f"Closed {row.get('closed', 0)} | "
            f"W {row.get('wins', 0)} | "
            f"L {row.get('losses', 0)} | "
            f"TP1 Rate {row.get('tp1_rate', 0.0)}%"
        )

    return "\n".join(lines)


def _format_named_rows(rows, icon="•", top_n=5):
    if not rows:
        return "لا توجد بيانات كافية"

    lines = []

    for row in rows[:top_n]:
        row = _normalize_summary_row(row)
        lines.append(
            f"{icon} {row.get('label', 'unknown')}: "
            f"WR {row.get('winrate', 0.0)}% | "
            f"Closed {row.get('closed', 0)} | "
            f"W {row.get('wins', 0)} | "
            f"L {row.get('losses', 0)}"
        )

    return "\n".join(lines)


def _format_counter_rows(rows, icon="•", empty_text="لا توجد بيانات"):
    if not rows:
        return empty_text

    return "\n".join(
        f"{icon} {safe_str(label, 'unknown')}: {safe_int(count, 0)}"
        for label, count in rows
    )


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
        return "-6% to -4%"
    if d <= -2:
        return "-4% to -2%"
    if d < 0:
        return "-2% to 0%"
    if d <= 2:
        return "0% to 2%"
    if d <= 4:
        return "2% to 4%"
    if d <= 6:
        return "4% to 6%"

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


# =========================
# REPORT BUILDERS
# =========================
def build_setups_report(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    period: str = "all",
):
    since_ts = get_period_since_ts(period)

    data = get_top_and_bottom_setups(
        redis_client=redis_client,
        market_type=market_type,
        side=side,
        since_ts=since_ts,
        min_closed=5,
        top_n=5,
    )

    best_text = _format_named_rows(data.get("best", []), icon="🟢", top_n=5)
    worst_text = _format_named_rows(data.get("worst", []), icon="🔴", top_n=5)

    return (
        f"📊 Setup Diagnostics ({market_type}/{side})\n"
        f"Period: {period}\n\n"
        f"🟢 Best Setups:\n{best_text}\n\n"
        f"🔴 Weak Setups:\n{worst_text}"
    )


def build_scores_report(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    period: str = "all",
):
    since_ts = get_period_since_ts(period)

    rows = get_score_diagnostics(
        redis_client=redis_client,
        market_type=market_type,
        side=side,
        since_ts=since_ts,
        min_closed=3,
    )

    return _format_summary_rows(
        title=f"Score Diagnostics ({market_type}/{side}) | {period}",
        rows=rows,
        top_n=10,
    )


def build_market_report(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    period: str = "all",
):
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

    market_text = _format_named_rows(market_rows, icon="🌍", top_n=6)
    timing_text = _format_named_rows(timing_rows, icon="⏱", top_n=6)

    return (
        f"📊 Market Diagnostics ({market_type}/{side})\n"
        f"Period: {period}\n\n"
        f"🌍 By Market State:\n{market_text}\n\n"
        f"⏱ By Entry Timing:\n{timing_text}"
    )


def build_losses_report(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    period: str = "all",
):
    since_ts = get_period_since_ts(period)

    clusters = get_loss_clusters(
        redis_client=redis_client,
        market_type=market_type,
        side=side,
        since_ts=since_ts,
        top_n=7,
    )

    market_text = _format_counter_rows(clusters.get("market_states", []), icon="🌍")
    timing_text = _format_counter_rows(clusters.get("entry_timing", []), icon="⏱")
    setup_text = _format_counter_rows(clusters.get("setup_types", []), icon="🧩")
    reason_text = _format_counter_rows(clusters.get("loss_reasons", []), icon="⚠️")

    return (
        f"📉 Loss Diagnostics ({market_type}/{side})\n"
        f"Period: {period}\n\n"
        f"🌍 Losses by Market State:\n{market_text}\n\n"
        f"⏱ Losses by Entry Timing:\n{timing_text}\n\n"
        f"🧩 Losses by Setup Type:\n{setup_text}\n\n"
        f"⚠️ Most Common Loss Reasons:\n{reason_text}"
    )


def build_full_diagnostics_report(
    redis_client,
    market_type: str = "futures",
    side: str = "long",
    period: str = "all",
):
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

    lines = [
        f"🧠 Full Diagnostics ({market_type}/{side})",
        f"Period: {period}",
        "",
        "🎯 Best / Worst Score Bucket:",
        (
            f"• Best: {best_score.get('label')} | WR {best_score.get('winrate')}%"
            if best_score else
            "• Best: لا توجد بيانات"
        ),
        (
            f"• Worst: {worst_score.get('label')} | WR {worst_score.get('winrate')}%"
            if worst_score else
            "• Worst: لا توجد بيانات"
        ),
        "",
        "🌍 Best / Worst Market State:",
        (
            f"• Best: {best_market.get('label')} | WR {best_market.get('winrate')}%"
            if best_market else
            "• Best: لا توجد بيانات"
        ),
        (
            f"• Worst: {worst_market.get('label')} | WR {worst_market.get('winrate')}%"
            if worst_market else
            "• Worst: لا توجد بيانات"
        ),
        "",
        "⏱ Best / Worst Entry Timing:",
        (
            f"• Best: {best_timing.get('label')} | WR {best_timing.get('winrate')}%"
            if best_timing else
            "• Best: لا توجد بيانات"
        ),
        (
            f"• Worst: {worst_timing.get('label')} | WR {worst_timing.get('winrate')}%"
            if worst_timing else
            "• Worst: لا توجد بيانات"
        ),
        "",
        "🟢 Top Setups:",
        _format_named_rows(setup_data.get("best", []), icon="•", top_n=3),
        "",
        "🔴 Weak Setups:",
        _format_named_rows(setup_data.get("worst", []), icon="•", top_n=3),
        "",
        "⚠️ Top Loss Reasons:",
        _format_counter_rows(losses.get("loss_reasons", []), icon="•"),
    ]

    return "\n".join(lines)
