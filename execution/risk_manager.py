"""Execution risk manager — Phase 2 update.

التغييرات:
- max_positions الافتراضي بقى 7 بدل 10
- إضافة drawdown_status check
- unified مع config/risk_config.py
"""
from __future__ import annotations

from config.risk_config import MAX_DAILY_OPEN_TRADES
from risk.drawdown_monitor import DrawdownStatus


def evaluate_execution_risk(
    score: float,
    max_open_positions: int,
    current_open_positions: int,
    min_execution_score: float,
    drawdown_status: DrawdownStatus | None = None,
) -> dict:
    """بيحدد هل الصفقة مسموح بيها أم لا.

    الترتيب:
    1. Drawdown protection (الأعلى أولوية)
    2. Max positions check
    3. Score check
    """
    remaining = max(0, max_open_positions - current_open_positions)

    # ── 1. Drawdown Protection ─────────────────────────────────────────────────
    if drawdown_status is not None and not drawdown_status.allowed:
        return {
            "allowed": False,
            "reason": drawdown_status.reason,
            "drawdown_level": drawdown_status.level,
            "drawdown_pct": drawdown_status.drawdown_pct,
            "slots": {
                "allowed": max_open_positions,
                "counted": current_open_positions,
                "remaining": remaining,
            },
        }

    # ── 2. Max Positions ───────────────────────────────────────────────────────
    if current_open_positions >= max_open_positions:
        return {
            "allowed": False,
            "reason": "max_positions_reached",
            "slots": {
                "allowed": max_open_positions,
                "counted": current_open_positions,
                "remaining": 0,
            },
        }

    # ── 3. Score Check ─────────────────────────────────────────────────────────
    if score < min_execution_score:
        return {
            "allowed": False,
            "reason": "score_too_low",
            "slots": {
                "allowed": max_open_positions,
                "counted": current_open_positions,
                "remaining": remaining,
            },
        }

    # ── All checks passed ──────────────────────────────────────────────────────
    return {
        "allowed": True,
        "reason": "risk_pass",
        "drawdown_level": getattr(drawdown_status, "level", 0),
        "slots": {
            "allowed": max_open_positions,
            "counted": current_open_positions,
            "remaining": remaining,
        },
    }
