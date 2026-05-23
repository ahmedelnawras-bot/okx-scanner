from __future__ import annotations

from typing import Any


def _yes_no_icon(value: Any) -> str:
    return "✅" if bool(value) else "❌"


def _pa_read_label(evidence: dict) -> str:
    """Return a short human-readable Price Action label.

    This is display-only. It does not affect score, mode, Nour filters,
    or execution decisions.
    """
    if not isinstance(evidence, dict) or not evidence.get("available"):
        return ""

    displacement = bool(evidence.get("displacement_hint"))
    acceptance = bool(evidence.get("auction_acceptance_hint"))
    sweep = bool(evidence.get("sweep_reclaim_hint"))
    compression = bool(evidence.get("compression_release_hint"))
    failed = bool(evidence.get("failed_breakout_risk"))

    if failed:
        return "Potential Trap"

    if sweep and acceptance:
        return "Liquidity Reclaim"

    if displacement and acceptance:
        return "Healthy Expansion"

    if compression and acceptance:
        return "Compression Break"

    if acceptance:
        return "Accepted Move"

    if displacement:
        return "Impulse Without Acceptance"

    return "No Clear PA Edge"


def format_smart_evidence_block(evidence: dict | None) -> str:
    """Format Smart Evidence for Telegram signal messages.

    Rules:
    - Silent when evidence is unavailable.
    - Short and glanceable.
    - Display-only: no execution logic here.
    """
    if not isinstance(evidence, dict):
        return ""

    if not evidence.get("available"):
        return ""

    pa_read = _pa_read_label(evidence)

    lines = [
        "",
        "🧠 <b>Smart Evidence</b>",
    ]

    if pa_read:
        lines.append(f"• PA Read: <b>{pa_read}</b>")

    lines.extend([
        f"• Displacement: {_yes_no_icon(evidence.get('displacement_hint'))}",
        f"• Acceptance: {_yes_no_icon(evidence.get('auction_acceptance_hint'))}",
        f"• Sweep Reclaim: {_yes_no_icon(evidence.get('sweep_reclaim_hint'))}",
        f"• Compression: {_yes_no_icon(evidence.get('compression_release_hint'))}",
        f"• Failed Breakout: {_yes_no_icon(evidence.get('failed_breakout_risk'))}",
    ])

    return "\n".join(lines)


def extract_smart_evidence_from_signal(signal: Any) -> dict:
    """Safely extract smart_evidence from a SignalCandidate-like object."""
    try:
        meta = getattr(signal, "meta", {}) or {}
        evidence = meta.get("smart_evidence") or {}
        return evidence if isinstance(evidence, dict) else {}
    except Exception:
        return {}
