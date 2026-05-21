risk_manager_6.py (تم تصحيح اسم الملف واستبدال الشرطات)

تم تصحيح مشكلة SyntaxError: invalid decimal literal

position_size = 35.0 max_daily_trades = 7 reference_portfolio = 1000.0 max_drawdown = 35.0 leverage = 15

from future import annotations from risk.drawdown_monitor import DrawdownStatus

def evaluate_execution_risk( score: float, max_open_positions: int, current_open_positions: int, min_execution_score: float, drawdown_status: DrawdownStatus | None = None, ) -> dict: """يحدد هل الصفقة مسموح بها أم لا.

الترتيب:
1. Drawdown protection (الأعلى أولوية)
2. Max open positions check
3. Score check
"""
remaining = max(0, int(max_open_positions) - int(current_open_positions))
drawdown_level = getattr(drawdown_status, "level", 0)
drawdown_pct = getattr(drawdown_status, "drawdown_pct", 0.0)

# ── 1. Drawdown Protection ────────────────────────────────────────────────
if drawdown_status is not None and not drawdown_status.allowed:
    return {
        "allowed": False,
        "reason": drawdown_status.reason,
        "drawdown_level": drawdown_level,
        "drawdown_pct": drawdown_pct,
        "slots": {
            "allowed": max_open_positions,
            "counted": current_open_positions,
            "remaining": remaining,
        },
    }

# ── 2. Max Open Positions ─────────────────────────────────────────────────
if current_open_positions >= max_open_positions:
    return {
        "allowed": False,
        "reason": "max_positions_reached",
        "drawdown_level": drawdown_level,
        "drawdown_pct": drawdown_pct,
        "slots": {
            "allowed": max_open_positions,
            "counted": current_open_positions,
            "remaining": 0,
        },
    }

# ── 3. Score Check ────────────────────────────────────────────────────────
if score < min_execution_score:
    return {
        "allowed": False,
        "reason": "score_too_low",
        "drawdown_level": drawdown_level,
        "drawdown_pct": drawdown_pct,
        "slots": {
            "allowed": max_open_positions,
            "counted": current_open_positions,
            "remaining": remaining,
        },
    }

# ── All checks passed ─────────────────────────────────────────────────────
return {
    "allowed": True,
    "reason": "risk_pass",
    "drawdown_level": drawdown_level,
    "drawdown_pct": drawdown_pct,
    "slots": {
        "allowed": max_open_positions,
        "counted": current_open_positions,
        "remaining": remaining,
    },
}
