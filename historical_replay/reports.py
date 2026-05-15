from __future__ import annotations

from typing import Any

from .dataset_writer import read_recent_records
from .engine import start_replay
from .state import clear_replay, data_count, get_log, get_status, request_stop


def build_historical_replay_help() -> str:
    return "\n".join([
        "🕰 Historical Replay Engine",
        "┄┄┄┄┄┄┄┄",
        "/replay_start_45d — تجهيز/تشغيل Replay آخر 45 يوم",
        "/replay_status — حالة المحرك والتقدم",
        "/replay_stop — إيقاف آمن عند أقرب نقطة",
        "/replay_export — حالة ملف/Redis داتا Replay",
        "/replay_summary — ملخص نتائج Replay",
        "/replay_clear — مسح داتا Replay فقط",
        "/compare_live_vs_replay — مقارنة Live Snapshot مع Replay عند توفر الداتا",
        "",
        "🔒 يستخدم مفاتيح Redis منفصلة replay:* ولا يلمس صفقات البوت الحي.",
    ])


def build_replay_start_report(settings=None, redis_client: Any | None = None, days: int = 45) -> str:
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
        f"Days: {status.get('days', 45)}",
        f"Timeframe: {status.get('timeframe', '15m')}",
        f"Progress: {float(status.get('progress_pct') or 0):.1f}%",
        f"Symbols: {status.get('symbols_done', 0)} / {status.get('symbols_total', 0)}",
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
    ]
    if recent:
        rows.extend(["", "Recent records preview:"])
        for item in recent:
            rows.append(f"- {item.get('time', 'n/a')} {item.get('symbol', 'n/a')} {item.get('mode', 'n/a')}")
    else:
        rows.extend(["", "لا توجد داتا Replay بعد."])
    return "\n".join(rows)


def build_replay_summary_report(settings=None, redis_client: Any | None = None) -> str:
    status = get_status(redis_client)
    return "\n".join([
        "📊 Historical Replay Summary",
        "┄┄┄┄┄┄┄┄",
        f"Run ID: {status.get('run_id') or 'n/a'}",
        f"State: {status.get('state', 'idle')}",
        f"Records: {status.get('records', data_count(redis_client))}",
        f"Normal: {status.get('normal', 0)}",
        f"Quality Candidates: {status.get('quality_candidates', 0)}",
        f"Execution Candidates: {status.get('execution_candidates', 0)}",
        f"Blocked by limits: {status.get('blocked_by_limits', 0)}",
        "",
        "TP/SL outcomes تظهر بعد اكتمال/تقدم الـ replay. استخدم /replay_status لمتابعة التقدم.",
    ])


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
        "المقارنة ستعمل بعد توفر Replay records.",
        "Live Snapshot موجود بالفعل في Technical Dataset، وReplay سيستخدم replay:* منفصل.",
    ])
