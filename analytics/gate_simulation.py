"""Passive gate simulation reports for AI/replay research.

This module is read-only:
- it does not change live scoring, filters, TP/SL, market modes, or execution,
- it compares proposed gates against Historical Replay and Live Snapshot data,
- it is intended for diagnostics before any real execution changes.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Iterable

from analytics.technical_dataset import load_snapshot_records
from historical_replay.dataset_writer import iter_records as iter_replay_records


MODES = {
    "normal": "NORMAL_LONG",
    "recovery": "RECOVERY_LONG",
    "strong": "STRONG_LONG_ONLY",
}

GOOD_CONTINUATION_SETUPS = {"higher_low_continuation", "support_bounce_confirmed", "vwap_reclaim"}
NORMAL_ALLOWED_SETUPS = GOOD_CONTINUATION_SETUPS | {"wave_3"}
RECOVERY_ALLOWED_SETUPS = GOOD_CONTINUATION_SETUPS
STRONG_ALLOWED_SETUPS = {"higher_low_continuation", "vwap_reclaim", "wave_3", "support_bounce_confirmed"}
HIGH_RISK_SYMBOLS = {
    "RAVE-USDT-SWAP",
    "BSB-USDT-SWAP",
    "LAB-USDT-SWAP",
    "BASED-USDT-SWAP",
    "RLS-USDT-SWAP",
    "SIGN-USDT-SWAP",
}


def _pct(part: int | float, total: int | float) -> str:
    total = float(total or 0)
    if total <= 0:
        return "0.0%"
    return f"{(float(part or 0) / total) * 100.0:.1f}%"


def _num(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _features(record: dict[str, Any]) -> dict[str, Any]:
    value = record.get("features")
    return value if isinstance(value, dict) else {}


def _setup_tags(record: dict[str, Any]) -> set[str]:
    features = _features(record)
    tags = features.get("setup_tags") or []
    if not isinstance(tags, list):
        tags = []
    setup_type = str(features.get("setup_type") or "")
    result = {str(x) for x in tags if x}
    if setup_type:
        result.add(setup_type)
    return result


def _pair_tags(record: dict[str, Any]) -> set[str]:
    tags = _features(record).get("pair_tags") or []
    if not isinstance(tags, list):
        tags = []
    return {str(x) for x in tags if x}


def _warnings_text(record: dict[str, Any]) -> str:
    features = _features(record)
    warnings = features.get("warnings") or []
    if not isinstance(warnings, list):
        warnings = [warnings]
    return " ".join([str(features.get("resistance_warning") or "")] + [str(x) for x in warnings])


def _near_resistance(record: dict[str, Any]) -> bool:
    text = _warnings_text(record).lower()
    tags = _setup_tags(record) | _pair_tags(record)
    return bool(
        "near_resistance" in tags
        or "resistance" in text
        or "مقاوم" in text
    )


def _breakout_confirmed(record: dict[str, Any]) -> bool:
    features = _features(record)
    return bool(features.get("breakout") and str(features.get("breakout_quality") or "").lower() in {"good", "strong"})


def _score(record: dict[str, Any]) -> float:
    features = _features(record)
    return _num(features.get("effective_score"), _num(features.get("score"), _num(features.get("raw_score"))))


def _vol_ratio(record: dict[str, Any]) -> float:
    return _num(_features(record).get("vol_ratio"), 1.0)


def _setup_type(record: dict[str, Any]) -> str:
    return str(_features(record).get("setup_type") or "")


def _is_relative_strength(record: dict[str, Any]) -> bool:
    tags = _setup_tags(record) | _pair_tags(record)
    return bool("relative_strength_vs_btc" in tags or "rs_btc" in tags)


def _fail(reason: str) -> tuple[bool, str]:
    return False, reason


def _pass(reason: str) -> tuple[bool, str]:
    return True, reason


def evaluate_gate(record: dict[str, Any], gate: str) -> tuple[bool, str]:
    """Return pass/fail and reason for the passive proposed gate."""
    score = _score(record)
    setup = _setup_type(record)
    tags = _setup_tags(record)
    near_res = _near_resistance(record)
    breakout_ok = _breakout_confirmed(record)
    symbol = str(record.get("symbol") or "")
    vol = _vol_ratio(record)
    mtf = bool(_features(record).get("mtf_confirmed"))

    if near_res and not breakout_ok:
        return _fail("near_resistance")

    if symbol in HIGH_RISK_SYMBOLS and score < 8.7 and not mtf:
        return _fail("high_risk_symbol_without_edge")

    if score >= 9.2 and not (breakout_ok or mtf):
        return _fail("overextended_high_score")

    if gate == "normal":
        if str(record.get("mode") or "") != MODES["normal"]:
            return _fail("wrong_mode")
        if score < 6.4:
            return _fail("score_too_low")
        if score > 8.8 and not (mtf or breakout_ok):
            return _fail("normal_score_overextended")
        if setup in GOOD_CONTINUATION_SETUPS:
            return _pass("clean_continuation_setup")
        if setup == "wave_3" and (_is_relative_strength(record) or mtf or vol >= 1.25):
            return _pass("wave3_with_strength")
        if _is_relative_strength(record) and vol >= 1.2:
            return _pass("relative_strength_with_volume")
        return _fail("weak_normal_setup")

    if gate == "recovery":
        if str(record.get("mode") or "") != MODES["recovery"]:
            return _fail("wrong_mode")
        if score < 6.2:
            return _fail("score_too_low")
        if score > 8.9 and not (setup == "support_bounce_confirmed" or mtf):
            return _fail("late_recovery_pump")
        if setup in RECOVERY_ALLOWED_SETUPS:
            return _pass("confirmed_recovery_setup")
        if bool(_features(record).get("recovery_relative_bounce")) and vol >= 1.1:
            return _pass("relative_recovery_bounce")
        return _fail("unconfirmed_recovery")

    if gate == "strong":
        if str(record.get("mode") or "") != MODES["strong"]:
            return _fail("wrong_mode")
        if score < 6.6:
            return _fail("score_too_low")
        if str(record.get("legacy_gate_reason") or "") == "strong_execution_pass":
            if score > 9.3 and not (breakout_ok or mtf):
                return _fail("strong_overextended")
            return _pass("legacy_strong_execution_pass")
        if setup in STRONG_ALLOWED_SETUPS and (_is_relative_strength(record) or mtf or breakout_ok or vol >= 1.25):
            return _pass("strong_setup_confirmed")
        return _fail("weak_strong_setup")

    return _fail("unknown_gate")


def _new_stats() -> dict[str, Any]:
    return {
        "total": 0,
        "quality": 0,
        "execution": 0,
        "blocked": 0,
        "outcomes": 0,
        "first_tp1": 0,
        "first_sl": 0,
        "tp1": 0,
        "tp2": 0,
        "sl": 0,
        "max_gain_sum": 0.0,
        "drawdown_sum": 0.0,
        "weighted_sum": 0.0,
        "weighted_count": 0,
    }


def _update_stats(stats: dict[str, Any], record: dict[str, Any]) -> None:
    stats["total"] += 1
    if record.get("quality_candidate"):
        stats["quality"] += 1
    if record.get("execution_candidate"):
        stats["execution"] += 1
    if record.get("blocked_by_limit"):
        stats["blocked"] += 1
    outcome = record.get("outcome") if isinstance(record.get("outcome"), dict) else {}
    if outcome:
        stats["outcomes"] += 1
        if outcome.get("hit_tp1"):
            stats["tp1"] += 1
        if outcome.get("hit_tp2"):
            stats["tp2"] += 1
        if outcome.get("hit_sl"):
            stats["sl"] += 1
        if outcome.get("first_event") == "tp1":
            stats["first_tp1"] += 1
        if outcome.get("first_event") == "sl":
            stats["first_sl"] += 1
        stats["max_gain_sum"] += _num(outcome.get("max_gain_24h"))
        stats["drawdown_sum"] += _num(outcome.get("max_drawdown_24h"))
        weighted = outcome.get("weighted_result_pct")
        if weighted is not None:
            stats["weighted_sum"] += _num(weighted)
            stats["weighted_count"] += 1


def _format_stats(label: str, stats: dict[str, Any]) -> list[str]:
    total = int(stats.get("total") or 0)
    outcomes = int(stats.get("outcomes") or 0)
    avg_gain = (float(stats.get("max_gain_sum") or 0.0) / outcomes) if outcomes else 0.0
    avg_dd = (float(stats.get("drawdown_sum") or 0.0) / outcomes) if outcomes else 0.0
    rows = [
        f"{label}: {total}",
        f"- quality/exe/blocked: {stats.get('quality', 0)} / {stats.get('execution', 0)} / {stats.get('blocked', 0)}",
    ]
    if outcomes:
        rows.extend([
            f"- first TP1/SL: {_pct(stats.get('first_tp1', 0), outcomes)} / {_pct(stats.get('first_sl', 0), outcomes)}",
            f"- TP1/TP2/SL touch: {_pct(stats.get('tp1', 0), outcomes)} / {_pct(stats.get('tp2', 0), outcomes)} / {_pct(stats.get('sl', 0), outcomes)}",
            f"- avg max/DD: {avg_gain:.2f}% / {avg_dd:.2f}%",
        ])
        if stats.get("weighted_count"):
            avg_weighted = float(stats.get("weighted_sum") or 0.0) / max(1, int(stats.get("weighted_count") or 0))
            rows.append(f"- avg weighted result: {avg_weighted:.2f}%")
    return rows


def _analyze_records(records: Iterable[dict[str, Any]], gate: str, mode: str) -> dict[str, Any]:
    before = _new_stats()
    after = _new_stats()
    rejected = _new_stats()
    reasons: Counter[str] = Counter()
    setup_counter: Counter[str] = Counter()
    pass_setup_counter: Counter[str] = Counter()

    for record in records:
        if str(record.get("mode") or "") != mode:
            continue
        # Only simulate candidate pressure. Normal rows are still counted in before total.
        _update_stats(before, record)
        setup_counter[_setup_type(record) or "unknown"] += 1
        passed, reason = evaluate_gate(record, gate)
        if passed:
            _update_stats(after, record)
            pass_setup_counter[_setup_type(record) or "unknown"] += 1
        else:
            _update_stats(rejected, record)
            reasons[reason] += 1

    return {
        "before": before,
        "after": after,
        "rejected": rejected,
        "reasons": reasons,
        "setups": setup_counter,
        "pass_setups": pass_setup_counter,
    }


def _format_analysis_block(title: str, analysis: dict[str, Any]) -> list[str]:
    before = analysis["before"]
    after = analysis["after"]
    rejected = analysis["rejected"]
    total = int(before.get("total") or 0)
    passed = int(after.get("total") or 0)
    rows = [title]
    rows.extend(_format_stats("Before", before))
    rows.extend(_format_stats("After gate", after))
    rows.append(f"- rejected to normal: {rejected.get('total', 0)} | reduction: {_pct(max(total - passed, 0), total)}")
    reasons = analysis.get("reasons") or Counter()
    if reasons:
        rows.append("- top reject reasons: " + ", ".join([f"{k}:{v}" for k, v in reasons.most_common(4)]))
    pass_setups = analysis.get("pass_setups") or Counter()
    if pass_setups:
        rows.append("- top pass setups: " + ", ".join([f"{k}:{v}" for k, v in pass_setups.most_common(4)]))
    return rows


def build_gate_sim_report(gate: str, settings: Any | None = None, redis_client: Any | None = None, live_limit: int = 50000) -> str:
    gate = str(gate or "").lower().strip()
    if gate not in MODES:
        return "⚠️ Gate simulation غير معروف. استخدم normal / recovery / strong."
    mode = MODES[gate]

    replay_analysis = _analyze_records(iter_replay_records(redis_client=redis_client), gate, mode)
    live_records = load_snapshot_records(settings=settings, limit=live_limit, redis_client=redis_client)
    live_analysis = _analyze_records(live_records, gate, mode)

    rows = [
        f"🧪 Gate Simulation — {mode}",
        "┄┄┄┄┄┄┄┄",
        "تحليل فقط: لا يغير execution أو scoring.",
        "",
    ]
    rows.extend(_format_analysis_block("📚 Historical Replay", replay_analysis))
    rows.append("")
    rows.extend(_format_analysis_block("🟢 Live Snapshot", live_analysis))
    rows.extend([
        "",
        "📌 الحكم النهائي يحتاج مقارنة بعد إعادة Replay بمنطق TP/SL النهائي لو تم تغييره.",
    ])
    return "\n".join(rows)


def build_gate_sim_all_report(settings: Any | None = None, redis_client: Any | None = None) -> str:
    live_records = load_snapshot_records(settings=settings, limit=50000, redis_client=redis_client)
    replay_records = list(iter_replay_records(redis_client=redis_client))
    rows = [
        "🧪 Gate Simulation — All Modes",
        "┄┄┄┄┄┄┄┄",
        "تحليل مختصر فقط؛ استخدم أوامر كل مود للتفاصيل.",
        "",
    ]
    for gate, mode in MODES.items():
        replay = _analyze_records(replay_records, gate, mode)
        live = _analyze_records(live_records, gate, mode)
        rb, ra = replay["before"], replay["after"]
        lb, la = live["before"], live["after"]
        rows.extend([
            f"{mode}",
            f"- Replay pass: {ra.get('total', 0)}/{rb.get('total', 0)} | reduction {_pct(max(int(rb.get('total') or 0)-int(ra.get('total') or 0),0), rb.get('total',0))}",
            f"- Replay first TP1/SL after: {_pct(ra.get('first_tp1', 0), ra.get('outcomes', 0))} / {_pct(ra.get('first_sl', 0), ra.get('outcomes', 0))}",
            f"- Live pass: {la.get('total', 0)}/{lb.get('total', 0)} | reduction {_pct(max(int(lb.get('total') or 0)-int(la.get('total') or 0),0), lb.get('total',0))}",
            "",
        ])
    rows.append("📌 لا يتم تطبيق أي Gate على البوت الحقيقي من هذه الأوامر.")
    return "\n".join(rows)
