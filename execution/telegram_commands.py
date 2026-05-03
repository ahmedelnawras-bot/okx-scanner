import html

from execution.config import (
    TRADING_MODE,
    EXECUTION_ENABLED,
    OKX_SIMULATED,
)
from execution.okx_trade_client import OKXTradeClient


def build_exec_status_message() -> str:
    try:
        client = OKXTradeClient()

        balance = client.get_balance()
        positions = client.get_positions()

        lines = [
            "🤖 <b>OKX Execution Status</b>",
            "",
            f"🧭 <b>Mode:</b> {html.escape(str(TRADING_MODE))}",
            f"🔐 <b>Execution Enabled:</b> {html.escape(str(EXECUTION_ENABLED))}",
            f"🧪 <b>Simulated:</b> {html.escape(str(OKX_SIMULATED))}",
            "",
        ]

        if balance.get("ok"):
            lines.append("💰 <b>Balance API:</b> ✅ Connected")
        else:
            lines.append("💰 <b>Balance API:</b> ❌ Failed")
            lines.append(f"سبب: {html.escape(str(balance.get('msg', 'unknown')))}")

        if positions.get("ok"):
            pos_data = positions.get("data", []) or []
            open_count = 0

            for p in pos_data:
                try:
                    if abs(float(p.get("pos", 0))) > 0:
                        open_count += 1
                except Exception:
                    continue

            lines.append(f"📊 <b>Open Positions:</b> ✅ {open_count}")
        else:
            lines.append("📊 <b>Positions API:</b> ❌ Failed")
            lines.append(f"سبب: {html.escape(str(positions.get('msg', 'unknown')))}")

        return "\n".join(lines)

    except Exception as e:
        return f"❌ فشل اختبار OKX API\nالسبب: {html.escape(str(e))}"


def build_exec_mode_message() -> str:
    return "\n".join([
        "⚙️ <b>Execution Mode</b>",
        "",
        f"🧭 <b>TRADING_MODE:</b> {html.escape(str(TRADING_MODE))}",
        f"🔐 <b>EXECUTION_ENABLED:</b> {html.escape(str(EXECUTION_ENABLED))}",
        f"🧪 <b>OKX_SIMULATED:</b> {html.escape(str(OKX_SIMULATED))}",
        "",
        "📌 الوضع الحالي آمن طالما EXECUTION_ENABLED=false",
    ])
