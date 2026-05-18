"""Drawdown Monitor — Portfolio Drawdown Protection Layer.

Phase 2 — 3 مستويات حماية:
  LEVEL 0: Normal — drawdown < 20%
  LEVEL 1: Warning — drawdown >= 20%
  LEVEL 2: Soft Stop — drawdown >= 28% (تقليل التنفيذ)
  LEVEL 3: Hard Stop — drawdown >= 35% (وقف كامل)

الحساب من START OF DAY PORTFOLIO BALANCE دايماً.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from config.risk_config import (
    DRAWDOWN_WARNING_PCT,
    DRAWDOWN_SOFT_STOP_PCT,
    DRAWDOWN_HARD_STOP_PCT,
    MAX_DAILY_DRAWDOWN_PCT,
)
from risk.portfolio_state import PortfolioState


@dataclass(frozen=True)
class DrawdownStatus:
    level: int          # 0=normal, 1=warning, 2=soft_stop, 3=hard_stop
    allowed: bool       # هل التنفيذ مسموح؟
    reason: str         # سبب الحالة
    drawdown_pct: float # نسبة الخسارة الحالية
    drawdown_usdt: float
    current_equity: float
    start_of_day_balance: float
    message_ar: str     # رسالة للمستخدم


def evaluate_drawdown(portfolio: PortfolioState) -> DrawdownStatus:
    """بيحدد مستوى الـ drawdown protection."""
    dd = portfolio.drawdown_pct
    equity = portfolio.current_equity
    sod = portfolio.start_of_day_balance

    if dd >= DRAWDOWN_HARD_STOP_PCT:
        return DrawdownStatus(
            level=3,
            allowed=False,
            reason="hard_stop_drawdown_limit_reached",
            drawdown_pct=round(dd, 2),
            drawdown_usdt=round(portfolio.drawdown_usdt, 2),
            current_equity=round(equity, 2),
            start_of_day_balance=round(sod, 2),
            message_ar=(
                f"🔴 Hard Stop — الخسارة اليومية وصلت {dd:.1f}% "
                f"(الحد الأقصى {DRAWDOWN_HARD_STOP_PCT}%). "
                "تنفيذ صفقات جديدة متوقف حتى اليوم التالي."
            ),
        )

    if dd >= DRAWDOWN_SOFT_STOP_PCT:
        return DrawdownStatus(
            level=2,
            allowed=False,
            reason="soft_stop_drawdown_approaching_limit",
            drawdown_pct=round(dd, 2),
            drawdown_usdt=round(portfolio.drawdown_usdt, 2),
            current_equity=round(equity, 2),
            start_of_day_balance=round(sod, 2),
            message_ar=(
                f"🟠 Soft Stop — الخسارة اليومية {dd:.1f}% "
                f"(تحذير عند {DRAWDOWN_SOFT_STOP_PCT}%). "
                "تم تقليل التنفيذ حماية للمحفظة."
            ),
        )

    if dd >= DRAWDOWN_WARNING_PCT:
        return DrawdownStatus(
            level=1,
            allowed=True,
            reason="warning_zone_elevated_drawdown",
            drawdown_pct=round(dd, 2),
            drawdown_usdt=round(portfolio.drawdown_usdt, 2),
            current_equity=round(equity, 2),
            start_of_day_balance=round(sod, 2),
            message_ar=(
                f"🟡 تحذير — الخسارة اليومية {dd:.1f}% "
                f"(الحد {DRAWDOWN_HARD_STOP_PCT}%). "
                "التنفيذ مستمر مع مراقبة مكثفة."
            ),
        )

    return DrawdownStatus(
        level=0,
        allowed=True,
        reason="normal_drawdown_within_limits",
        drawdown_pct=round(dd, 2),
        drawdown_usdt=round(portfolio.drawdown_usdt, 2),
        current_equity=round(equity, 2),
        start_of_day_balance=round(sod, 2),
        message_ar=f"✅ محفظة سليمة — خسارة يومية {dd:.1f}%",
    )


def build_drawdown_report(portfolio: PortfolioState) -> str:
    """تقرير مختصر للـ Telegram."""
    status = evaluate_drawdown(portfolio)
    lines = [
        "💼 Portfolio Protection Status",
        "┄┄┄┄┄┄┄┄",
        f"Start of Day: ${status.start_of_day_balance:.2f}",
        f"Current Equity: ${status.current_equity:.2f}",
        f"Realized PnL: ${portfolio.realized_pnl_usdt:+.2f}",
        f"Unrealized PnL: ${portfolio.unrealized_pnl_usdt:+.2f}",
        f"Daily Drawdown: {status.drawdown_pct:.1f}% (${abs(status.drawdown_usdt):.2f})",
        "",
        f"Protection Level: {status.level} — {['Normal', 'Warning', 'Soft Stop', 'Hard Stop'][status.level]}",
        status.message_ar,
        "",
        f"Trades opened today: {portfolio.trades_opened_today}",
        f"Execution: {'✅ Allowed' if status.allowed else '🚫 Halted'}",
        "",
        f"Thresholds: Warning={DRAWDOWN_WARNING_PCT}% | "
        f"Soft={DRAWDOWN_SOFT_STOP_PCT}% | "
        f"Hard={DRAWDOWN_HARD_STOP_PCT}%",
    ]
    return "\n".join(lines)


def to_dict(status: DrawdownStatus) -> dict[str, Any]:
    return {
        "level": status.level,
        "allowed": status.allowed,
        "reason": status.reason,
        "drawdown_pct": status.drawdown_pct,
        "drawdown_usdt": status.drawdown_usdt,
        "current_equity": status.current_equity,
        "start_of_day_balance": status.start_of_day_balance,
        "message_ar": status.message_ar,
        "thresholds": {
            "warning": DRAWDOWN_WARNING_PCT,
            "soft_stop": DRAWDOWN_SOFT_STOP_PCT,
            "hard_stop": DRAWDOWN_HARD_STOP_PCT,
        },
    }
