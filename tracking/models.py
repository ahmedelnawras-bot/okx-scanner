from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class TrackedTrade:
    symbol: str
    entry: float
    sl: float
    tp1: float
    tp2: float
    setup_type: str = "unknown"
    market_mode: str = "NORMAL_LONG"
    score: float = 0.0
    execution_setup_tags: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    # v124 path separation metadata.
    # Normal signals are always tracked in the normal path even when execution is checked/rejected.
    trade_source: str = "normal"          # normal | execution
    tracking_bucket: str = "normal"      # normal | execution
    execution_checked: bool = False
    execution_status: str = "normal_signal_only"
    execution_reason: str = ""
    execution_path: str = ""
    execution_trade: bool = False

    opened_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    tp1_hit: bool = False
    tp2_hit: bool = False
    trailing_active: bool = False
    runner_active: bool = False
    sl_moved_to_entry: bool = False
    status: str = "open"
    current_price: float = 0.0
    pnl_pct: float = 0.0
    realized_pnl_pct: float = 0.0
    runner_pnl_pct: float = 0.0
    closed_portion_pct: float = 0.0
    highest_price: float = 0.0
    protected_on_block: bool = False
    protection_level: int = 0
    protected_reason: str = ""
    protected_sl: float = 0.0
    trailing_tightened: bool = False

    @property
    def stage_label(self) -> str:
        if self.status == "closed_loss":
            return "SL"
        if self.status == "closed_win":
            return "Closed Win"
        if self.status == "breakeven_after_tp1":
            return "BE after TP1"
        if self.status == "trailing_hit":
            return "Trailing"
        if self.tp2_hit:
            return "TP2 / Runner"
        if self.tp1_hit:
            return "TP1"
        return "OPEN"
