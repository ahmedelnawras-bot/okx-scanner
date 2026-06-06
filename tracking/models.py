from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


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

    # Stable persistence identity.
    trade_id: str = ""

    # v124 path separation metadata.
    # Normal signals are always tracked in the normal path even when execution is checked/rejected.
    trade_source: str = "normal"          # normal | execution
    tracking_bucket: str = "normal"      # normal | execution
    execution_checked: bool = False
    execution_status: str = "normal_signal_only"
    execution_reason: str = ""
    execution_path: str = ""
    execution_trade: bool = False

    # Position plan / lifecycle metadata.
    target_model: str = "standard_40_40_20"  # standard_40_40_20 | recovery_50_25_25
    tp1_close_pct: float = 40.0
    tp2_close_pct: float = 40.0
    runner_close_pct: float = 20.0
    protected_runner: bool = False
    slot_exempt: bool = False
    slot_exempt_reason: str = ""
    daily_open_risk_exempt: bool = False
    same_symbol_block_exempt: bool = False

    # Exchange-managed execution metadata.
    # These fields let the bot know exactly what reached OKX and what still
    # needs sync/amend/cancel later.
    exchange_order_ok: bool = False
    exchange_order_reason: str = ""
    exchange_sync_state: str = "not_submitted"   # not_submitted | submitted | partial_live | live | sync_failed
    last_exchange_error: str = ""
    last_exchange_sync_at: datetime | None = None

    entry_order_id: str = ""
    entry_client_order_id: str = ""
    entry_order_payload: dict[str, Any] = field(default_factory=dict)

    sl_attached_on_entry: bool = False
    sl_attached_payload: list[dict[str, Any]] = field(default_factory=list)
    live_stop_loss_px: float = 0.0
    last_sl_amend_reason: str = ""
    last_sl_amend_at: datetime | None = None

    tp_split_ok: bool = False
    tp_split_reason: str = ""
    tp1_order_id: str = ""
    tp2_order_id: str = ""
    tp1_client_order_id: str = ""
    tp2_client_order_id: str = ""

    runner_expected_size: str = ""
    runner_requires_trailing_after_tp2: bool = False
    runner_algo_id: str = ""
    runner_algo_client_order_id: str = ""
    managed_trade_plan: dict[str, Any] = field(default_factory=dict)

    opened_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    closed_at: datetime | None = None
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

    runner_target_price: float = 0.0
    runner_target_hit_at: datetime | None = None
    closed_portion_pct: float = 0.0
    highest_price: float = 0.0
    max_favorable_pct: float = 0.0
    max_adverse_pct: float = 0.0
    protected_on_block: bool = False
    protection_level: int = 0
    protected_reason: str = ""
    protected_sl: float = 0.0
    trailing_tightened: bool = False

    # Position sizing metadata.
    # Stored per trade so wallet reports can use the real historical margin
    # instead of a fixed/fallback margin. Does not affect trading logic.
    used_margin_usdt: float = 0.0
    simulation_margin_usdt: float = 0.0
    margin_usdt: float = 0.0
    allocated_margin_usdt: float = 0.0
    position_notional_usdt: float = 0.0
    simulation_balance_reference: float = 0.0
    effective_leverage: float = 0.0

    # AI analytics / research metadata (export-only; does not affect trading logic).
    # Filled progressively by registry/updater/exporter when data is available.
    decision_trace_id: str = ""
    strategy_version: str = ""
    config_hash: str = ""
    entry_reason: str = ""
    acceptance_path: str = ""
    risk_mode: str = ""

    # Entry candle / price-action evidence snapshot.
    entry_pattern: str = ""
    reversal_detected: bool = False
    reversal_type: str = ""
    wick_ratio: float = 0.0
    body_ratio: float = 0.0
    candle_strength: float = 0.0
    last_3_candles: list[str] = field(default_factory=list)

    # Lifecycle timestamps for later AI analysis.
    tp1_hit_at: datetime | None = None
    tp2_hit_at: datetime | None = None
    sl_move_to_entry_at: datetime | None = None
    sl_move_to_tp1_at: datetime | None = None
    trailing_started_at: datetime | None = None
    trailing_tightened_at: datetime | None = None

    # Entry quality / execution quality metrics.
    volume_spike_ratio: float = 0.0
    spread_pct: float = 0.0
    slippage_pct: float = 0.0
    distance_from_vwap_pct: float = 0.0
    distance_from_ema20_pct: float = 0.0

    # Exit/post-exit analytics.
    exit_efficiency_pct: float = 0.0
    missed_runner_profit_pct: float = 0.0
    price_after_5m_pct: float = 0.0
    price_after_15m_pct: float = 0.0
    price_after_1h_pct: float = 0.0

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
        if self.protected_runner:
            return "Protected Runner"
        if self.tp2_hit:
            return "TP2 / Runner"
        if self.tp1_hit:
            return "TP1"
        return "OPEN"

    @property
    def is_closed(self) -> bool:
        return self.status in {
            "closed_loss",
            "breakeven_after_tp1",
            "trailing_hit",
            "closed_win",
            "expired",
        }

    @property
    def has_open_runner(self) -> bool:
        return bool(self.tp2_hit and not self.is_closed)

    @property
    def counts_as_active_slot(self) -> bool:
        return (not self.is_closed) and (not self.tp2_hit) and (not self.slot_exempt)

    @property
    def counts_as_daily_open_risk(self) -> bool:
        return (not self.is_closed) and (not self.tp2_hit) and (not self.daily_open_risk_exempt)

    @property
    def blocks_same_symbol_reentry(self) -> bool:
        return (not self.is_closed) and (not self.tp2_hit) and (not self.same_symbol_block_exempt)
