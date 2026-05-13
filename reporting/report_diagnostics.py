from __future__ import annotations

from collections import Counter


def build_diagnostics_report(signals: list[dict], execution_results: list[dict], title: str = "🧠 تشخيص التنفيذ") -> str:
    setup_counter = Counter(item["signal"].setup_type for item in signals)
    status_counter = Counter(item.get("status") for item in execution_results)
    reason_counter = Counter(item.get("reason") for item in execution_results if item.get("reason"))
    path_counter = Counter(item.get("path") for item in execution_results if item.get("path"))
    warning_counter = Counter()
    score_buckets = Counter()
    for item in signals:
        signal = item['signal']
        for warning in signal.warnings:
            warning_counter[warning] += 1
        score_buckets[f"{int(signal.score)}x"] += 1
    lines = [title, "━━━━━━━━━━━━", "📊 Execution Status"]
    for name, count in status_counter.most_common():
        lines.append(f"• {name}: {count}")
    if path_counter:
        lines.append("🛣 Routing Paths")
        for name, count in path_counter.most_common():
            lines.append(f"• {name}: {count}")
    lines.append("🧩 Setup Mix")
    for name, count in setup_counter.most_common(5):
        lines.append(f"• {name}: {count}")
    if score_buckets:
        lines.append("⭐ Score Bands")
        for name, count in score_buckets.most_common():
            lines.append(f"• {name}: {count}")
    if reason_counter:
        lines.append("⚠️ Top Reasons")
        for name, count in reason_counter.most_common(5):
            lines.append(f"• {name}: {count}")
    if warning_counter:
        lines.append("🪫 Warnings Seen")
        for name, count in warning_counter.most_common(4):
            lines.append(f"• {name}: {count}")
    return "\n".join(lines)
