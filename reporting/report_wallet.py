from __future__ import annotations

from tracking.models import TrackedTrade
from reporting.report_format import SEP, wallet_impact_lines


def build_wallet_report(trades: list[TrackedTrade], starting_balance: float = 1000.0, title: str = "ðŸ’¼ Wallet Impact") -> str:
    lines = [title, "ðŸ“… Since Start / Snapshot", SEP]
    lines.extend(wallet_impact_lines(trades, starting_balance=starting_balance, title="Wallet Impact"))
    return "\n".join(lines)
