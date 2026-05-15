from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

from .dataset_writer import create_dataset_zip, iter_records, read_recent_records
from .engine import start_replay
from .state import clear_replay, data_count, get_log, get_status, request_stop


def _pct(part: int | float, total: int | float) -> str:
    total = float(total or 0)
    if total <= 0:
        return "0.0%"
    return f"{(float(part or 0) / total) * 100.0:.1f}%"


def _bytes_label(value: int | float) -> str:
    value = float(value or 0)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return f"{value:.1f} GB"


def build_historical_replay_help() -> str:
    return "\n".join([
        "🕰 Historical Replay Engine",
        "┄┄┄┄┄┄┄┄",
        "/replay_start_30d",
        "↳ يشغل Replay آخر 30 يوم باستخدام بيانات OKX التاريخية.",
        "/replay_start_45d",
        "↳ يشغل Replay آخر 45 يوم عند الحاجة لتحليل أوسع.",
        "/replay_status",
        "↳ يعرض حالة المحرك والتقدم وعدد السجلات.",
        "/replay_stop",
        "↳ يطلب إيقافًا آمنًا عند أقرب checkpoint.",
        "/replay_export",
        "↳ يعرض معاينة ومسار داتا Replay الخام.",
        "/replay_export_file",
        "↳ يرسل ملف Replay dataset مضغوط ZIP.",
        "/replay_summary",
        "↳ يعرض ملخص نتائج Replay و TP/SL عند توفرها.",
        "/replay_clear",
        "↳ يمسح داتا Replay فقط ولا يلمس live snapshots.",
        "/compare_live_vs_replay",
        "↳ يقارن Live Snapshot مع Replay عند توفر الداتا.",
        "",
        "🔒 يستخدم مفاتيح Redis منفصلة replay:* ولا يلمس صفقات البوت الحي.",
    ])


def build_replay_start_report(settings=None, redis_client: Any | None = None, days: int = 30) -> str:
    result = start_replay(days=days, symbols_limit=int(getattr(settings, "scan_limit", 200) or 200), timeframe=str(getattr(settings, "timeframe", "15m") or "15m"), redis_client=redis_client, settings=settings)
    status = result.get("status") or {}
    icon = "✅" if result.get("ok") else "⚠️"
    return "\n".join([
        f"{icon} Historical Replay Start",
        "┄┄┄┄┄┄┄┄",
        str(result.get("message") or ""),
        f"Run ID: {status.get('run_id') or 'starting'}",
        f"Days: {status.get('days', days)}",
        f"Symbols limit: {status.get('symbols_limit', getattr(settings, 'scan_limit', 200) if settings else 200)}",
        f"Timeframe: {status.get('timeframe', getattr(settings, 'timeframe', '15m') if settings else '15m')}",
        "",
        "المحرك الحقيقي يعمل الآن في background runner منفصل داخل historical_replay/ باستخدام مفاتيح replay:* ولا يلمس تنفيذ البوت الحي.",
    ])


def build_replay_status_report(settings=None, redis_client: Any | None = None) -> str:
    status = get_status(redis_client)
    rows = [
        "🕰 Historical Replay Status",
        "┄┄┄┄┄┄┄┄",
        f"State: {status.get('state', 'idle')}",
        f"Running: {'YES' if status.get('running') else 'NO'}",
        f"Run ID: {status.get('run_id') or 'n/a'}",
        f"Days: {status.get('days', 30)}",
        f"Timeframe: {status.get('timeframe', '15m')}",
        f"Progress: {float(status.get('progress_pct') or 0):.1f}%",
        f"Symbols: {status.get('symbols_done', 0)} / {status.get('symbols_total', 0)}",
        f"Expected candles/symbol: {status.get('expected_candles_per_symbol', 'n/a')}",
        f"Current symbol: {status.get('current_symbol') or 'n/a'}",
        f"Current candles: {status.get('current_symbol_candles', 0)} / {status.get('expected_candles_per_symbol', 'n/a')}",
        f"Records: {status.get('records', data_count(redis_client))}",
        f"Normal: {status.get('normal', 0)}",
        f"Quality Candidates: {status.get('quality_candidates', 0)}",
        f"Execution Candidates: {status.get('execution_candidates', 0)}",
        f"Blocked by limits: {status.get('blocked_by_limits', 0)}",
        f"Data Store: {status.get('data_store', 'Redis' if redis_client else 'file/env fallback')}",
        f"Output: {status.get('output_path', 'data/replay_signals_dataset.jsonl')}",
        "",
        f"Message: {status.get('message', '')}",
    ]
    logs = get_log(redis_client, limit=3)
    if logs:
        rows.extend(["", "Last log:"])
        rows.extend([f"- {item.get('message', '')}" for item in logs if item])
    return "\n".join(rows)


def build_replay_stop_report(settings=None, redis_client: Any | None = None) -> str:
    status = request_stop(redis_client)
    return "\n".join([
        "⏸ Historical Replay Stop",
        "┄┄┄┄┄┄┄┄",
        f"State: {status.get('state')}",
        str(status.get("message") or "Stop requested."),
    ])


def build_replay_export_report(settings=None, redis_client: Any | None = None) -> str:
    status = get_status(redis_client)
    records = data_count(redis_client)
    recent = read_recent_records(redis_client, limit=3)
    rows = [
        "📦 Historical Replay Export",
        "┄┄┄┄┄┄┄┄",
        f"Data Store: {status.get('data_store', 'Redis' if redis_client else 'file/env fallback')}",
        f"Redis/File Records: {records}",
        f"Output: {status.get('output_path', 'data/replay_signals_dataset.jsonl')}",
        "",
        "لإرسال الملف فعليًا استخدم:",
        "/replay_export_file",
    ]
    if recent:
        rows.extend(["", "Recent records preview:"])
        for item in recent:
            rows.append(f"- {item.get('time', 'n/a')} {item.get('symbol', 'n/a')} {item.get('mode', 'n/a')}")
    else:
        rows.extend(["", "لا توجد داتا Replay بعد."])
    return "\n".join(rows)


def build_replay_export_file(settings=None, redis_client: Any | None = None) -> dict[str, Any]:
    status = get_status(redis_client)
    if bool(status.get("running")):
        return {"ok": False, "message": "Replay is still running. انتظر اكتماله قبل التصدير."}
    records = data_count(redis_client)
    if records <= 0:
        return {"ok": False, "message": "لا توجد داتا Replay للتصدير."}
    result = create_dataset_zip(redis_client=redis_client, output_path=status.get("output_path") or None)
    return {
        "ok": True,
        "path": result.get("zip_path"),
        "records": result.get("records"),
        "zip_size_bytes": result.get("zip_size_bytes"),
        "jsonl_size_bytes": result.get("jsonl_size_bytes"),
        "caption": "\n".join([
            "📦 Historical Replay Dataset",
            f"Records: {result.get('records')}",
            f"JSONL: {_bytes_label(result.get('jsonl_size_bytes') or 0)}",
            f"ZIP: {_bytes_label(result.get('zip_size_bytes') or 0)}",
        ]),
    }


def _group_stats() -> dict[str, Any]:
    return {"total": 0, "outcomes": 0, "tp1": 0, "tp2": 0, "sl": 0, "first_tp1": 0, "first_sl": 0, "max_gain_sum": 0.0, "drawdown_sum": 0.0}


def _update_group(group: dict[str, Any], rec: dict[str, Any]) -> None:
    group["total"] += 1
    outcome = rec.get("outcome") if isinstance(rec.get("outcome"), dict) else {}
    if not outcome:
        return
    group["outcomes"] += 1
    if outcome.get("hit_tp1"):
        group["tp1"] += 1
    if outcome.get("hit_tp2"):
        group["tp2"] += 1
    if outcome.get("hit_sl"):
        group["sl"] += 1
    if outcome.get("first_event") == "tp1":
        group["first_tp1"] += 1
    if outcome.get("first_event") == "sl":
        group["first_sl"] += 1
    try:
        group["max_gain_sum"] += float(outcome.get("max_gain_24h") or 0.0)
        group["drawdown_sum"] += float(outcome.get("max_drawdown_24h") or 0.0)
    except Exception:
        pass


def _format_group(name: str, group: dict[str, Any]) -> list[str]:
    outcomes = int(group.get("outcomes") or 0)
    avg_gain = (float(group.get("max_gain_sum") or 0.0) / outcomes) if outcomes else 0.0
    avg_dd = (float(group.get("drawdown_sum") or 0.0) / outcomes) if outcomes else 0.0
    return [
        f"{name}: {group.get('total', 0)}",
        f"  TP1: {_pct(group.get('tp1', 0), outcomes)} | TP2: {_pct(group.get('tp2', 0), outcomes)} | SL: {_pct(group.get('sl', 0), outcomes)}",
        f"  First TP1/SL: {_pct(group.get('first_tp1', 0), outcomes)} / {_pct(group.get('first_sl', 0), outcomes)}",
        f"  Avg max/DD: {avg_gain:.2f}% / {avg_dd:.2f}%",
    ]


def build_replay_summary_report(settings=None, redis_client: Any | None = None) -> str:
    status = get_status(redis_client)
    total_records = data_count(redis_client)
    groups = {
        "all": _group_stats(),
        "execution": _group_stats(),
        "blocked": _group_stats(),
        "normal": _group_stats(),
    }
    modes: Counter[str] = Counter()
    mode_tp1: dict[str, dict[str, Any]] = defaultdict(_group_stats)
    symbols: Counter[str] = Counter()

    for rec in iter_records(redis_client):
        _update_group(groups["all"], rec)
        mode = str(rec.get("mode") or "unknown")
        symbol = str(rec.get("symbol") or "unknown")
        modes[mode] += 1
        symbols[symbol] += 1
        _update_group(mode_tp1[mode], rec)
        if rec.get("execution_candidate"):
            _update_group(groups["execution"], rec)
        elif rec.get("blocked_by_limit"):
            _update_group(groups["blocked"], rec)
        elif rec.get("normal_signal"):
            _update_group(groups["normal"], rec)

    outcome_available = int(groups["all"].get("outcomes") or 0)
    rows = [
        "📊 Historical Replay Summary",
        "┄┄┄┄┄┄┄┄",
        f"Run ID: {status.get('run_id') or 'n/a'}",
        f"State: {status.get('state', 'idle')}",
        f"Records: {status.get('records', total_records)}",
        f"Normal: {status.get('normal', 0)}",
        f"Quality Candidates: {status.get('quality_candidates', 0)}",
        f"Execution Candidates: {status.get('execution_candidates', 0)}",
        f"Blocked by limits: {status.get('blocked_by_limits', 0)}",
        f"Outcomes available: {outcome_available}",
        "",
        "🎯 TP/SL Outcomes",
    ]
    rows.extend(_format_group("All", groups["all"]))
    rows.extend(_format_group("Execution", groups["execution"]))
    rows.extend(_format_group("Blocked", groups["blocked"]))

    if modes:
        rows.extend(["", "🌐 By Mode:"])
        for mode, count in modes.most_common(5):
            g = mode_tp1[mode]
            rows.append(f"- {mode}: {count} | TP1 {_pct(g.get('tp1', 0), g.get('outcomes', 0))} | SL {_pct(g.get('sl', 0), g.get('outcomes', 0))}")
    if symbols:
        rows.extend(["", "🔝 Top Symbols by records:"])
        rows.extend([f"- {sym}: {cnt}" for sym, cnt in symbols.most_common(5)])
    rows.extend(["", "ملاحظة: داخل نفس الشمعة يتم الحساب بتحفظ، SL قبل TP لتجنب تضخيم النتائج."])
    return "\n".join(rows)


def build_replay_clear_report(settings=None, redis_client: Any | None = None) -> str:
    result = clear_replay(redis_client, clear_local=True)
    return "\n".join([
        "🧹 Historical Replay Clear",
        "┄┄┄┄┄┄┄┄",
        "تم مسح داتا Replay فقط.",
        f"Redis keys deleted: {result.get('redis_keys_deleted', 0)}",
        f"Local deleted: {', '.join(result.get('local_deleted', []) or ['none'])}",
        "",
        "لم يتم لمس live snapshots أو الصفقات أو tracking.",
    ])


def build_compare_live_vs_replay_report(settings=None, redis_client: Any | None = None) -> str:
    return "\n".join([
        "🔬 Live vs Replay Comparison",
        "┄┄┄┄┄┄┄┄",
        "المقارنة ستعمل بعد توفر Replay records وLive snapshot export.",
        "Live Snapshot موجود في Technical Dataset، وReplay يستخدم replay:* منفصل.",
    ])
