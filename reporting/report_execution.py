from __future__ import annotations

from utils.constants import LEVERAGE_NOTE_AR


def build_execution_report(execution_results: list[dict], title: str = "🚀 تقرير التنفيذ") -> str:
    accepted = [r for r in execution_results if r.get("status") == "accepted_preview"]
    pending = [r for r in execution_results if r.get("status") == "pending_pullback_preview"]
    candidate_only = [r for r in execution_results if r.get("status") == "candidate_only"]
    rejected = [r for r in execution_results if str(r.get("status", "")).startswith("rejected")]
    block = [r for r in execution_results if r.get("path") == "block_exception"]
    recovery = [r for r in execution_results if r.get("path") == "recovery"]
    strong = [r for r in execution_results if r.get("path") == "elite_or_whitelist"]
    whitelist = [r for r in execution_results if r.get("path") == "whitelist"]
    total = len(execution_results)
    acc_rate = (len(accepted) / max(1, total)) * 100 if total else 0.0

    lines = [title, "━━━━━━━━━━━━", LEVERAGE_NOTE_AR]
    lines.append(f"📊 Candidates: {total} | ✅ Accepted: {len(accepted)} | ⏳ Pending: {len(pending)}")
    lines.append(f"⚠️ Candidate Only: {len(candidate_only)} | ❌ Rejected: {len(rejected)} | Accept Rate: {acc_rate:.0f}%")
    lines.append(f"🛣 Whitelist: {len(whitelist)} | Strong: {len(strong)} | Recovery: {len(recovery)} | Block: {len(block)}")

    if accepted:
        lines.append("✅ Latest Accepted")
        for item in accepted[:5]:
            order = item.get("order") or {}
            slots = item.get("slots") or {}
            score = order.get("score") or item.get("score") or "?"
            lines.append(
                f"• {order.get('symbol', '?')} | {item.get('path')} | ⭐ {score} | slots {slots.get('remaining', '?')}/{slots.get('max', '?')}"
            )

    if rejected:
        lines.append("📉 Top Rejections")
        reason_counts: dict[str, int] = {}
        for item in rejected:
            reason = item.get("reason", "unknown")
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
        for reason, count in sorted(reason_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
            lines.append(f"• {reason}: {count}")

    return "\n".join(lines)
