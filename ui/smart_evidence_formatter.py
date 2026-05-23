from __future__ import annotations

from typing import Any


def _yes_no_icon(value: Any) -> str:
    return "✅" if bool(value) else "❌"


def extract_smart_evidence_from_signal(signal: Any) -> dict:
    """Safely extract smart_evidence from a SignalCandidate-like object."""
    try:
        meta = getattr(signal, "meta", {}) or {}
        evidence = meta.get("smart_evidence") or {}
        return evidence if isinstance(evidence, dict) else {}
    except Exception:
        return {}


def format_smart_evidence_block(evidence: dict | None) -> str:
    """Format Smart Evidence for Telegram signal messages in Arabic.

    Display-only:
    - Does not affect score.
    - Does not affect modes.
    - Does not affect Nour filters.
    - Does not affect execution decisions.
    """
    if not isinstance(evidence, dict):
        return ""

    if not evidence.get("available"):
        return ""

    lines = [
        "",
        "🧠 قراءة السوق",
        f"• تمدد قوي {_yes_no_icon(evidence.get('displacement_hint'))}",
        f"• قبول سعري {_yes_no_icon(evidence.get('auction_acceptance_hint'))}",
        f"• اختراق ضعيف {'⚠️' if evidence.get('failed_breakout_risk') else '❌'}",
    ]

    if evidence.get("sweep_reclaim_hint"):
        lines.append("• سحب سيولة ✅")

    if evidence.get("compression_release_hint"):
        lines.append("• فك ضغط ✅")

    return "\n".join(lines)
