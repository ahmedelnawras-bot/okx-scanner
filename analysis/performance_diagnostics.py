import time
import logging
from collections import Counter, defaultdict

from tracking.performance import (
    get_all_trades_data,
    summarize_by_field,
    get_common_loss_reasons,
)

logger = logging.getLogger("okx-scanner")


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
            local_now.tm_year, local_now.tm_mon, local_now.tm_mday,
            0, 0, 0,
            local_now.tm_wday, local_now.tm_yday, local_now.tm_isdst
        )))
    if period == "1d":
        return now_ts - (24 * 3600)
    if period == "7d":
        return now_ts - (7 * 24 * 3600)
    if period == "30d":
        return now_ts - (30 * 24 * 3600)

    return None


def _empty_summary(label="unknown"):
    return {
        "label": label,
        "total": 0,
        "closed": 0,
        "wins": 0,
        "tp1_wins": 0,
        "tp2_wins": 0,
        "losses": 0,
        "expired": 0,
        "open": 0,
        "tp1_hits": 0,
        "winrate": 0.0,
        "tp1_rate": 0.0,
    }


def _apply_trade_to_summary(summary: dict, trade: dict):
    summary["total"] += 1

    if normalize_bool(trade.get("tp1_hit", False)):
        summary["tp1_hits"] += 1

    status = safe_str(trade.get("status", ""), "").lower()
    result = safe_str(trade.get("result", ""), "").lower()

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
    summary["winrate"] = round((summary["wins"] / decided) * 100, 2) if decided > 0 else 0.0
    summary["tp1_rate"] = round((summary["tp1_hits"] / summary["total"]) * 100, 2) if summary["total"] > 0 else 0.0
    return summary


def _sort_summary_rows(rows):
    return sorted(
        rows,
        key=lambda x: (
            x.get("winrate", 0.0),
            x.get("closed", 0),
            x.get("wins", 0),
            -x.get("losses", 0),
        ),
        reverse=True,
    )


def _format_summary_rows(title: str, rows: list, top_n: int = 8) -> str:
    if not rows:
        return f"📊 {title}\nلا توجد بيانات كافية"

    lines = [f"📊 {title}", ""]

    for row in rows[:top_n]:
        lines.append(
            f"• {row['label']}: "
            f"WR {row['winrate']}% | "
            f"Closed {row['closed']} | "
            f"W {row['wins']} | "
            f"L {row['losses']} | "
            f"TP1 Rate {row['tp1_rate']}%"
        )

    return "\n".join(lines)


def _diagnostics_value(trade: dict, field_name: str, default="unknown"):
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

    for trade in rows:
        label = safe_str(bucket_getter(trade), "unknown")
        grouped[label]["label"] = label
        _apply_trade_to_summary(grouped[label], trade)

    output = []
    for label, summary in grouped.items():
        _finalize_summary(summary)
        if summary["closed"] < min_closed:
            continue
        output.append(summary)

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
    for row in rows:
        output.append({
            "label": row["field_value"],
            "total": row["total"],
            "closed": row["closed"],
            "wins": row["wins"],
            "tp1_wins": row["tp1_wins"],
            "tp2_wins": row["tp2_wins"],
            "losses": row["losses"],
            "expired": row["expired"],
            "open": row["open"],
            "tp1_hits": row["tp1_hits"],
            "winrate": row["winrate"],
            "tp1_rate": row["tp1_rate"],
        })

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

    best = rows[:top_n]
    worst = sorted(
        rows,
        key=lambda x: (
            x.get("winrate", 0.0),
            -x.get("closed", 0),
            x.get("losses", 0),
        )
    )[:top_n]

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

    for trade in trades:
        result = safe_str(trade.get("result", ""), "").lower()
        if result != "loss":
            continue

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

    return {
        "best_score_bucket": score_rows[0] if score_rows else None,
        "worst_score_bucket": score_rows[-1] if score_rows else None,
        "best_market_state": market_rows[0] if market_rows else None,
        "worst_market_state": market_rows[-1] if market_rows else None,
        "best_entry_timing": timing_rows[0] if timing_rows else None,
        "worst_entry_timing": timing_rows[-1] if timing_rows else None,
    }


# =========================
# FORMATTERS
# =========================
def _format_named_rows(rows, icon="•", top_n=5):
    if not rows:
        return "لا توجد بيانات كافية"

    lines = []
    for row in rows[:top_n]:
        lines.append(
            f"{icon} {row['label']}: "
            f"WR {row['winrate']}% | "
            f"Closed {row['closed']} | "
            f"W {row['wins']} | L {row['losses']}"
        )
    return "\n".join(lines)


def _format_counter_rows(rows, icon="•", empty_text="لا توجد بيانات"):
    if not rows:
        return empty_text
    return "\n".join(f"{icon} {label}: {count}" for label, count in rows)


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

    best_text = _format_named_rows(data["best"], icon="🟢", top_n=5)
    worst_text = _format_named_rows(data["worst"], icon="🔴", top_n=5)

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

    market_text = _format_counter_rows(clusters["market_states"], icon="🌍")
    timing_text = _format_counter_rows(clusters["entry_timing"], icon="⏱")
    setup_text = _format_counter_rows(clusters["setup_types"], icon="🧩")
    reason_text = _format_counter_rows(clusters["loss_reasons"], icon="⚠️")

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
        f"• Best: {best_score['label']} | WR {best_score['winrate']}%" if best_score else "• Best: لا توجد بيانات",
        f"• Worst: {worst_score['label']} | WR {worst_score['winrate']}%" if worst_score else "• Worst: لا توجد بيانات",
        "",
        "🌍 Best / Worst Market State:",
        f"• Best: {best_market['label']} | WR {best_market['winrate']}%" if best_market else "• Best: لا توجد بيانات",
        f"• Worst: {worst_market['label']} | WR {worst_market['winrate']}%" if worst_market else "• Worst: لا توجد بيانات",
        "",
        "⏱ Best / Worst Entry Timing:",
        f"• Best: {best_timing['label']} | WR {best_timing['winrate']}%" if best_timing else "• Best: لا توجد بيانات",
        f"• Worst: {worst_timing['label']} | WR {worst_timing['winrate']}%" if worst_timing else "• Worst: لا توجد بيانات",
        "",
        "🟢 Top Setups:",
        _format_named_rows(setup_data["best"], icon="•", top_n=3),
        "",
        "🔴 Weak Setups:",
        _format_named_rows(setup_data["worst"], icon="•", top_n=3),
        "",
        "⚠️ Top Loss Reasons:",
        _format_counter_rows(losses["loss_reasons"], icon="•"),
    ]

    return "\n".join(lines)
