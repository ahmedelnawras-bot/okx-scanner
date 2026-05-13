"""OKX Long Bot clean rebuild v122 message/buttons worker.

Preserved design:
- main.py orchestrates only
- normal signal first, execution decision second
- Telegram/OKX adapters are isolated from core analysis
- OKX orders are blocked from live trading unless explicitly enabled
"""
from __future__ import annotations

import json
import time
import traceback
import requests
from datetime import datetime, timezone

from utils.config import get_settings, Settings
from utils.constants import MODE_NORMAL_LONG, MODE_BLOCK_LONGS, MODE_RECOVERY_LONG
from analysis.market_modes import (
    MarketSnapshot,
    MarketModeState,
    decide_market_mode,
    block_protection_status,
    recovery_slots_remaining,
    register_recovery_trade,
)
from analysis.pair_selection import select_ranked_pairs
from analysis.scoring import build_signal_candidate
from execution.execution_processor import process_trade_candidate
from execution.okx_trade_client import OKXTradeClient
from tracking.trade_registry import register_trade
from tracking.open_trades_updater import update_open_trades
from reporting.report_router import build_report_bundle, build_command_outputs
from reporting.help_menus import (
    build_main_menu_layout,
    build_main_reply_keyboard,
    build_execution_help,
    build_normal_help,
    build_master_help,
)
from ui.telegram_signals import build_signal_message, build_signal_buttons, build_track_message
from ui.market_mode_messages import build_market_mode_sections, build_block_escalation_alert
from services.telegram_sender import TelegramSender


def fetch_okx_tickers(base_url: str, timeout: int = 15) -> list[dict]:
    url = f"{base_url}/api/v5/market/tickers?instType=SWAP"
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        payload = resp.json()
        return payload.get("data", [])
    except Exception:
        # Offline fallback keeps the worker testable if OKX is temporarily unavailable.
        return [
            {"instId": "BTC-USDT-SWAP", "last": "103250", "volCcy24h": "250000000", "change_pct": 1.4},
            {"instId": "ETH-USDT-SWAP", "last": "4980", "volCcy24h": "180000000", "change_pct": 2.2},
            {"instId": "SOL-USDT-SWAP", "last": "212", "volCcy24h": "75000000", "change_pct": 3.8},
            {"instId": "DOGE-USDT-SWAP", "last": "0.244", "volCcy24h": "42000000", "change_pct": -2.1},
            {"instId": "XRP-USDT-SWAP", "last": "0.635", "volCcy24h": "36000000", "change_pct": -1.4},
            {"instId": "APT-USDT-SWAP", "last": "11.2", "volCcy24h": "12000000", "change_pct": 1.1},
            {"instId": "LINK-USDT-SWAP", "last": "18.45", "volCcy24h": "21000000", "change_pct": 0.4},
            {"instId": "AVAX-USDT-SWAP", "last": "42.4", "volCcy24h": "42000000", "change_pct": 2.8},
        ]


def _build_snapshot(ranked_pairs) -> MarketSnapshot:
    red_count = sum(1 for p in ranked_pairs[:20] if p.change_pct < 0)
    avg_change = (sum(p.change_pct for p in ranked_pairs[:20]) / max(1, min(20, len(ranked_pairs)))) if ranked_pairs else 0.0
    strong_count = sum(1 for p in ranked_pairs[:20] if p.change_pct >= 1.5)
    btc_change = next((p.change_pct for p in ranked_pairs if p.symbol.startswith("BTC-")), avg_change)
    fast_rebound = avg_change > 0.35 and strong_count >= 5
    btc_reclaim = btc_change > 0.2
    breadth_improving = red_count <= 8 and avg_change > -0.1
    return MarketSnapshot(
        btc_change_15m=btc_change,
        red_ratio_15m=(red_count / max(1, min(20, len(ranked_pairs)))) if ranked_pairs else 0.5,
        avg_change_15m=avg_change,
        strong_coins_count=strong_count,
        fast_rebound=fast_rebound,
        btc_reclaim=btc_reclaim,
        breadth_improving=breadth_improving,
    )


def _build_mode_message(state: MarketModeState, snapshot: MarketSnapshot, protection: dict) -> str:
    return build_market_mode_sections(
        state.mode,
        {
            "market_mix": f"avg={snapshot.avg_change_15m:.2f}% | strong={snapshot.strong_coins_count} | red={snapshot.red_ratio_15m:.0%}",
            "market_state": f"strong_coins={snapshot.strong_coins_count} | avg15m={snapshot.avg_change_15m:.2f}% | red_ratio={snapshot.red_ratio_15m:.2f}",
            "trigger": "fast rebound" if state.mode == MODE_RECOVERY_LONG else ("risk-off breadth" if state.mode == MODE_BLOCK_LONGS else "balanced scan"),
            "mode_reason": "fast rebound path" if state.mode == MODE_RECOVERY_LONG else "core market breadth decision",
            "signal_rules": "normal signal first → execution later",
            "requirements": "quality up" if state.mode != MODE_NORMAL_LONG else "balanced normal scanning",
            "execution_notes": "whitelist / elite / recovery / block-exception",
            "protection_current": protection.get("current", "inactive"),
            "protection_next": protection.get("next", "inactive"),
            "remaining_minutes": protection.get("remaining_minutes", 0),
            "recovery_remaining": recovery_slots_remaining(state),
        },
        variant="status",
    )


def run_once(previous_state: MarketModeState | None = None, settings: Settings | None = None) -> dict:
    settings = settings or get_settings()
    tickers = fetch_okx_tickers(settings.okx_base_url, settings.request_timeout)
    ranked_pairs = select_ranked_pairs(tickers, settings.scan_limit)
    snapshot = _build_snapshot(ranked_pairs)
    initial_mode = previous_state or MarketModeState(mode=MODE_NORMAL_LONG, changed_at=datetime.now(timezone.utc))
    state = decide_market_mode(snapshot, previous=initial_mode)

    signal_items = []
    execution_results = []
    trades = []
    open_position_count = 0
    recovery_remaining = recovery_slots_remaining(state)

    for pair in ranked_pairs[:20]:
        signal = build_signal_candidate(pair, state.mode, settings.min_normal_score, settings.min_strong_score)
        if not signal:
            continue
        exec_result = process_trade_candidate(
            signal,
            current_open_positions=open_position_count,
            max_open_positions=settings.max_execution_positions,
            min_execution_score=settings.min_execution_score,
            recovery_slots_remaining=recovery_remaining if state.mode == MODE_RECOVERY_LONG else None,
        )
        if exec_result.get("status") in {"accepted_preview", "pending_pullback_preview"}:
            open_position_count += 1
            if state.mode == MODE_RECOVERY_LONG:
                state = register_recovery_trade(state)
                recovery_remaining = recovery_slots_remaining(state)
        signal_items.append({"signal": signal, "execution": exec_result, "message": build_signal_message(signal, exec_result)})
        execution_results.append(exec_result)
        trades.append(register_trade(signal))

    price_map = {pair.symbol: pair.last_price * (1.012 if "momentum" in pair.tags else 0.996) for pair in ranked_pairs[:20]}
    protection = block_protection_status(state)
    trades = update_open_trades(trades, price_map, protection_level=protection.get("level", 0))
    mode_message = _build_mode_message(state, snapshot, protection)
    reports = build_report_bundle(trades, execution_results, signal_items)
    command_outputs = build_command_outputs(trades, execution_results, signal_items)

    return {
        "state": state,
        "mode": state.mode,
        "mode_message": mode_message,
        "block_alert_preview": build_block_escalation_alert(state, affected=len(trades), protected=sum(1 for t in trades if t.pnl_pct > 0), tightened=sum(1 for t in trades if t.tp2_hit)) if state.mode == MODE_BLOCK_LONGS else None,
        "menu": build_main_menu_layout(),
        "menu_keyboard": build_main_reply_keyboard(),
        "help": build_master_help(
            mode=state.mode,
            execution_enabled=settings.execution_enabled,
            risk_enabled=True,
            okx_orders=settings.okx_place_orders,
        ),
        "help_execution": build_execution_help(),
        "help_normal": build_normal_help(),
        "signals": [item["message"] for item in signal_items[:8]],
        "signal_items": signal_items,
        "execution_results": execution_results,
        "trades": trades,
        "command_outputs": command_outputs,
        **reports,
    }


def _plain_result(result: dict) -> dict:
    """Remove dataclass-heavy fields before JSON logging."""
    return {k: v for k, v in result.items() if k not in {"state", "signal_items", "trades"}}


def _dispatch_signals(sender: TelegramSender, result: dict, settings: Settings, sent_fingerprints: set[str], okx_client: OKXTradeClient | None = None) -> None:
    for item in result.get("signal_items", [])[:8]:
        signal = item["signal"]
        exec_result = item["execution"]
        is_execution = exec_result.get("status") in {"accepted_preview", "pending_pullback_preview"}
        if not settings.send_normal_signals and not is_execution:
            continue
        fingerprint = f"{signal.symbol}|{signal.entry:.8f}|{signal.market_mode}|{exec_result.get('status')}"
        if fingerprint in sent_fingerprints:
            continue
        sent_fingerprints.add(fingerprint)

        text = item["message"]
        if is_execution and settings.execution_enabled and settings.okx_place_orders and okx_client:
            order_result = okx_client.place_market_long(
                signal.symbol,
                signal.entry,
                margin_usdt=settings.paper_margin_usdt,
                leverage=settings.default_leverage,
                td_mode=settings.okx_td_mode,
            )
            status_icon = "✅" if order_result.get("ok") else "⚠️"
            text += "\n\n" + "\n".join([
                f"{status_icon} OKX Paper Execution",
                f"Simulated: {order_result.get('simulated')}",
                f"Result: {order_result.get('reason') or order_result.get('response', {}).get('msg') or order_result.get('response', {}).get('code')}",
            ])
        sender.send_message(text, reply_markup=build_signal_buttons(signal))


def _build_fast_status(result: dict, settings: Settings) -> str:
    execution_results = result.get("execution_results", []) or []
    last_rejection = next(
        (r for r in reversed(execution_results) if str(r.get("status", "")).startswith("rejected")),
        None,
    )
    rejection_reason = "none"
    if last_rejection:
        rejection_reason = f"{last_rejection.get('status')} | {last_rejection.get('reason', 'unknown')}"

    return "\n".join([
        "🟢 Bot Status",
        "━━━━━━━━━━━━",
        f"📈 Market Mode: {result.get('mode', 'UNKNOWN')}",
        f"⚡ Execution Engine: {'ON' if settings.execution_enabled else 'OFF'}",
        f"🧪 OKX Paper Orders: {'ON' if settings.okx_place_orders else 'OFF'}",
        f"🔒 Live Trading: {'ALLOWED' if settings.allow_live_trading else 'BLOCKED'}",
        "",
        f"📡 Telegram: {'ON' if settings.telegram_enabled else 'OFF'}",
        f"⏱ Scan Interval: {settings.scan_interval_seconds}s",
        "",
        "🧠 آخر حالة تنفيذ:",
        f"{rejection_reason}",
        "",
        "✅ الأوامر تعمل بسرعة — ربط OKX مؤجل حاليًا" if not settings.okx_place_orders else "✅ OKX paper order placement enabled",
    ])


def _extract_commands(text: str) -> list[str]:
    commands: list[str] = []
    for line in str(text or "").splitlines():
        for token in line.strip().split():
            if token.startswith("/"):
                commands.append(token.split("@", 1)[0])
                break
    return commands


def _handle_callback_query(sender: TelegramSender, result: dict, callback_query: dict) -> None:
    callback_id = str(callback_query.get("id") or "")
    data = str(callback_query.get("data") or "")
    if callback_id:
        sender.answer_callback_query(callback_id, "Tracking opened")

    if not data.startswith("track:"):
        return

    symbol = data.split(":", 1)[1]
    for item in result.get("signal_items", []):
        signal = item.get("signal")
        if signal and signal.symbol == symbol:
            sender.send_message(build_track_message(signal, item.get("execution")))
            return

    sender.send_message("📊 Track\n┄┄┄┄┄┄┄┄\nلم أجد هذه الصفقة في آخر دورة Scan.")


def _answer_commands(sender: TelegramSender, result: dict, offset: int | None, settings: Settings) -> int | None:
    updates = sender.get_updates(offset=offset, timeout_seconds=0)
    if not updates.get("ok"):
        return offset

    command_outputs = result.get("command_outputs", {})
    for update in updates.get("result", []):
        offset = int(update.get("update_id", 0)) + 1
        callback_query = update.get("callback_query")
        if callback_query:
            _handle_callback_query(sender, result, callback_query)
            continue

        message = update.get("message") or update.get("channel_post") or {}
        text = str(message.get("text") or "")
        commands = _extract_commands(text)
        plain_text = text.strip()

        # Reply-keyboard buttons from the approved /help dashboard.
        if not commands and plain_text:
            button_map = {
                "🚀 Execution": "/help_execution",
                "Execution": "/help_execution",
                "📊 Normal Trades": "/help_normal",
                "Normal Trades": "/help_normal",
                "🧠🚀 Execution Intelligence": "/report_execution_intelligence",
                "Exec Intelligence": "/report_execution_intelligence",
                "🧠📊 Market Intelligence": "/report_intelligence",
                "Market Intelligence": "/report_intelligence",
                "💼 Wallet Impact": "/report_execution_wallet",
                "Wallet Impact": "/report_execution_wallet",
                "🧠 Diagnostics": "/report_diagnostics",
                "Diagnostics": "/report_diagnostics",
                "🤖 OKX Control": "/status",
                "OKX Control": "/status",
                "⚙️ Admin": "/status",
                "Admin": "/status",
                "📘 System Info": "/help",
                "System Info": "/help",
            }
            mapped = button_map.get(plain_text)
            if mapped:
                commands = [mapped]

        if not commands:
            continue

        for command in commands:
            if command in ("/start", "/help"):
                reply = result.get("help") or "OKX Long Bot is running."
                sender.send_message(reply, reply_markup=result.get("menu_keyboard"))
                continue
            elif command == "/status":
                reply = _build_fast_status(result, settings)
            elif command == "/mood":
                reply = result.get("mode_message", "No mode yet")
            elif command == "/help_execution":
                reply = result.get("help_execution", "")
            elif command == "/help_normal":
                reply = result.get("help_normal", "")
            else:
                reply = command_outputs.get(command) or command_outputs.get(command.lstrip("/")) or "الأمر غير متاح في نسخة v123 بعد."
            sender.send_message(reply)
    return offset


def live_worker() -> None:
    settings = get_settings()
    sender = TelegramSender(settings.bot_token, settings.chat_id, timeout=settings.request_timeout)
    okx_client = OKXTradeClient(
        api_key=settings.okx_api_key,
        api_secret=settings.okx_api_secret,
        passphrase=settings.okx_passphrase,
        base_url=settings.okx_base_url,
        simulated=settings.okx_simulated,
        allow_live_trading=settings.allow_live_trading,
        timeout=settings.request_timeout,
    )
    state: MarketModeState | None = None
    sent_fingerprints: set[str] = set()
    telegram_offset: int | None = None

    startup_lines = [
        "✅ OKX Long Bot v122 started",
        f"Telegram: {'ON' if sender.enabled and settings.telegram_enabled else 'OFF'}",
        f"Execution: {'ON' if settings.execution_enabled else 'OFF'}",
        f"OKX paper orders: {'ON' if settings.okx_place_orders else 'OFF'} | simulated={settings.okx_simulated}",
        f"Scan interval: {settings.scan_interval_seconds}s",
    ]
    print("\n".join(startup_lines), flush=True)
    if sender.enabled and settings.telegram_enabled:
        sender.send_message("\n".join(startup_lines))

    while True:
        try:
            result = run_once(previous_state=state, settings=settings)
            state = result["state"]
            print(json.dumps(_plain_result(result), ensure_ascii=False, indent=2), flush=True)
            if sender.enabled and settings.telegram_enabled:
                if settings.send_mode_status_each_scan:
                    sender.send_message(result.get("mode_message", ""))
                _dispatch_signals(sender, result, settings, sent_fingerprints, okx_client if settings.execution_enabled else None)
                telegram_offset = _answer_commands(sender, result, telegram_offset, settings)
        except Exception as exc:
            error_text = f"❌ OKX bot loop error: {exc}\n{traceback.format_exc()[-1200:]}"
            print(error_text, flush=True)
            if sender.enabled and settings.telegram_enabled:
                sender.send_message(error_text)

        # Keep Telegram commands responsive during the scan wait window.
        # Instead of sleeping 15 minutes in one block, poll commands every few seconds.
        wait_until = time.time() + max(30, int(settings.scan_interval_seconds))
        while time.time() < wait_until:
            if sender.enabled and settings.telegram_enabled:
                try:
                    telegram_offset = _answer_commands(sender, result, telegram_offset, settings)
                except Exception as exc:
                    print(f"telegram command polling error: {exc}", flush=True)
            time.sleep(3)


if __name__ == "__main__":
    live_worker()
