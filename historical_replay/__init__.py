"""Historical replay package.

This package is intentionally isolated from the live trading loop.  It uses
`replay:*` Redis keys and never touches open trades, execution state, live
tracking, Telegram trade sending, scoring thresholds, TP/SL, or market-mode
runtime state.
"""
from __future__ import annotations

__all__ = []
