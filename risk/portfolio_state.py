"""Portfolio state tracker for daily drawdown protection.

Phase 2 — يتتبع:
- start_of_day_balance: رصيد بداية اليوم
- realized_pnl: الأرباح/الخسائر المحققة
- unrealized_pnl: الأرباح/الخسائر غير المحققة
- current_equity: الرصيد الحالي
- drawdown_pct: نسبة الخسارة من بداية اليوم
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from config.risk_config import (
    REFERENCE_PORTFOLIO_USDT,
    PAPER_MARGIN_PER_TRADE_USDT,
    DEFAULT_LEVERAGE,
    DRAWDOWN_WARNING_PCT,
    DRAWDOWN_SOFT_STOP_PCT,
    DRAWDOWN_HARD_STOP_PCT,
)


@dataclass
class PortfolioState:
    """حالة المحفظة الكاملة للـ drawdown protection."""

    reference_portfolio: float = REFERENCE_PORTFOLIO_USDT
    start_of_day_balance: float = REFERENCE_PORTFOLIO_USDT
    realized_pnl_usdt: float = 0.0
    unrealized_pnl_usdt: float = 0.0
    day_started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_updated: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    trades_opened_today: int = 0
    execution_halted: bool = False
    halt_reason: str = ""

    @property
    def current_equity(self) -> float:
        """الرصيد الحالي = بداية اليوم + محقق + غير محقق."""
        return self.start_of_day_balance + self.realized_pnl_usdt + self.unrealized_pnl_usdt

    @property
    def drawdown_usdt(self) -> float:
        """الخسارة بالدولار من بداية اليوم."""
        return min(0.0, self.current_equity - self.start_of_day_balance)

    @property
    def drawdown_pct(self) -> float:
        """نسبة الخسارة من بداية اليوم (%)."""
        if self.start_of_day_balance <= 0:
            return 0.0
        return abs(self.drawdown_usdt / self.start_of_day_balance) * 100.0

    @property
    def is_warning_zone(self) -> bool:
        return self.drawdown_pct >= DRAWDOWN_WARNING_PCT

    @property
    def is_soft_stop_zone(self) -> bool:
        return self.drawdown_pct >= DRAWDOWN_SOFT_STOP_PCT

    @property
    def is_hard_stop_zone(self) -> bool:
        return self.drawdown_pct >= DRAWDOWN_HARD_STOP_PCT

    def to_dict(self) -> dict[str, Any]:
        return {
            "reference_portfolio": self.reference_portfolio,
            "start_of_day_balance": round(self.start_of_day_balance, 2),
            "realized_pnl_usdt": round(self.realized_pnl_usdt, 2),
            "unrealized_pnl_usdt": round(self.unrealized_pnl_usdt, 2),
            "current_equity": round(self.current_equity, 2),
            "drawdown_usdt": round(self.drawdown_usdt, 2),
            "drawdown_pct": round(self.drawdown_pct, 2),
            "is_warning_zone": self.is_warning_zone,
            "is_soft_stop_zone": self.is_soft_stop_zone,
            "is_hard_stop_zone": self.is_hard_stop_zone,
            "day_started_at": self.day_started_at.isoformat(),
            "last_updated": self.last_updated.isoformat(),
            "trades_opened_today": self.trades_opened_today,
            "execution_halted": self.execution_halted,
            "halt_reason": self.halt_reason,
        }


def _trade_is_closed(trade) -> bool:
    """اعتبر TP2 = closed حتى لو status لم يتحدث بعد."""
    try:
        if bool(getattr(trade, "tp2_hit", False)):
            return True
        if getattr(trade, "closed_at", None) is not None:
            return True
        if bool(getattr(trade, "is_closed", False)):
            return True
        status = str(getattr(trade, "status", "") or "").lower()
        return status in {
            "closed_loss",
            "breakeven_after_tp1",
            "trailing_hit",
            "closed_win",
            "expired",
        }
    except Exception:
        return False


def _same_utc_day(value: datetime | None, day_ref: datetime) -> bool:
    if value is None:
        return False
    try:
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.date() == day_ref.date()
    except Exception:
        return False


def build_portfolio_state_from_trades(
    trades: list,
    reference_portfolio: float = REFERENCE_PORTFOLIO_USDT,
    margin_per_trade: float = PAPER_MARGIN_PER_TRADE_USDT,
    leverage: int = DEFAULT_LEVERAGE,
    start_of_day_balance: float | None = None,
    day_started_at: datetime | None = None,
) -> PortfolioState:
    """بيبني PortfolioState من الـ trades الحالية.

    بيحسب:
    - realized_pnl من الصفقات المغلقة
    - unrealized_pnl من الصفقات المفتوحة
    - TP2 تعتبر صفقة مغلقة
    """
    now = datetime.now(timezone.utc)
    realized = 0.0
    unrealized = 0.0
    opened_today = 0

    actual_start_of_day_balance = float(
        start_of_day_balance
        if start_of_day_balance is not None
        else reference_portfolio
    )

    actual_day_started_at = (
        day_started_at.astimezone(timezone.utc)
        if isinstance(day_started_at, datetime) and day_started_at.tzinfo is not None
        else day_started_at.replace(tzinfo=timezone.utc)
        if isinstance(day_started_at, datetime)
        else now.replace(hour=0, minute=0, second=0, microsecond=0)
    )

    trade_value = float(margin_per_trade) * float(leverage)

    for trade in trades or []:
        is_closed = _trade_is_closed(trade)
        opened_at = getattr(trade, "opened_at", None)
        pnl_pct = float(getattr(trade, "pnl_pct", 0.0) or 0.0)
        realized_pnl_pct = float(getattr(trade, "realized_pnl_pct", 0.0) or 0.0)

        pnl_usdt = (pnl_pct / 100.0) * trade_value

        if is_closed:
            realized_usdt = (realized_pnl_pct / 100.0) * trade_value
            realized += realized_usdt
        else:
            unrealized += pnl_usdt

        if _same_utc_day(opened_at, actual_day_started_at):
            opened_today += 1

    return PortfolioState(
        reference_portfolio=float(reference_portfolio),
        start_of_day_balance=round(actual_start_of_day_balance, 4),
        realized_pnl_usdt=round(realized, 4),
        unrealized_pnl_usdt=round(unrealized, 4),
        day_started_at=actual_day_started_at,
        last_updated=now,
        trades_opened_today=opened_today,
    )
