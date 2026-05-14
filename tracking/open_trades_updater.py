from __future__ import annotations

from .lifecycle import update_trade_with_price
from .models import TrackedTrade


def update_open_trades(trades: list[TrackedTrade], price_map: dict[str, float], protection_level: int = 0) -> list[TrackedTrade]:
    updated = []
    for trade in trades:
        current_price = float(price_map.get(trade.symbol, trade.current_price or trade.entry))
        updated.append(update_trade_with_price(trade, current_price, protection_level=protection_level))
    return updated
