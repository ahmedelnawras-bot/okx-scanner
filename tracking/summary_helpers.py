"""
وحدات مساعدة مشتركة لتلخيص الصفقات وحسابات الربح/الخسارة.

تُستخدم من:
- tracking/performance.py
- analysis/performance_diagnostics.py

مهم:
- هذا الملف لا يعتمد على tracking.performance نهائياً.
- performance.py يمكنه استيراد الدوال من هنا، لكن العكس غير مسموح.
- الملف مشترك بين اللونج والشورت.
- حساب PnL هنا يتم كنسبة مئوية فقط.
- حسابات الدولار تتم داخل tracking/performance.py حسب خطة إدارة رأس المال.

**إصدار 2.0 - إصلاحات شاملة:**
- دعم trailing_win / breakeven / tp2_partial / trailing_open
- calc_trade_result_pct يدعم 40/40/20 و trailing_exit_price
- cap الخسارة بـ -initial_sl فقط (منع inflation)
- normalize_trade_result/status يعرف كل الحالات الجديدة
"""

import time
import logging
from collections import defaultdict
from typing import List, Dict, Optional, Tuple

logger = logging.getLogger("okx-scanner")


# -------------------------------
# ثوابت عامة للتقرير المالي
# -------------------------------
REPORT_LEVERAGE = 15.0
REPORT_MAX_CAPITAL_USAGE_PCT = 35.0
REPORT_DAILY_MAX_DRAWDOWN_PCT = 20.0
REPORT_ACCOUNT_BALANCE_USD = 1000.0
REPORT_ACTIVE_TRADE_SLOTS = 10

# نسب الإغلاق الافتراضية (40/40/20)
DEFAULT_TP1_CLOSE_PCT = 40.0
DEFAULT_TP2_CLOSE_PCT = 40.0
DEFAULT_TRAILING_POSITION_PCT = 20.0
DEFAULT_TRAILING_PCT = 2.5


# -------------------------------
# SAFE CONVERTERS
# -------------------------------
def safe_float(value, default=0.0) -> float:
    if value is None:
        return default
    try:
        f = float(value)
        if f != f:  # NaN
            return default
        return f
    except (ValueError, TypeError):
        return default


def safe_int(value, default=0) -> int:
    if value is None:
        return default
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


def safe_bool(value, default=False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "y", "on")
    try:
        return bool(value)
    except Exception:
        return default


def normalize_side(side: str) -> str:
    side = (side or "long").strip().lower()
    if side not in ("long", "short"):
        return "long"
    return side


def normalize_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


# -------------------------------
# PROFIT / LOSS CALCULATIONS
# -------------------------------
def calc_long_pct(entry: float, exit_price: float) -> float:
    entry = safe_float(entry, 0.0)
    exit_price = safe_float(exit_price, 0.0)
    if entry <= 0:
        return 0.0
    return ((exit_price - entry) / entry) * 100.0


def calc_short_pct(entry: float, exit_price: float) -> float:
    entry = safe_float(entry, 0.0)
    exit_price = safe_float(exit_price, 0.0)
    if entry <= 0:
        return 0.0
    return ((entry - exit_price) / entry) * 100.0


def calc_side_pct(side: str, entry: float, exit_price: float) -> float:
    side = normalize_side(side)
    if side == "short":
        return calc_short_pct(entry, exit_price)
    return calc_long_pct(entry, exit_price)


def get_effective_entry(trade: dict) -> float:
    """
    سعر الدخول الفعلي — أولوية:
    1. trade["effective_entry"]
    2. diagnostics["effective_entry"]
    3. trade["entry"]
    """
    diagnostics = trade.get("diagnostics", {}) or {}
    eff = safe_float(trade.get("effective_entry"), 0.0)
    if eff > 0:
        return eff
    eff = safe_float(diagnostics.get("effective_entry"), 0.0)
    if eff > 0:
        return eff
    return safe_float(trade.get("entry"), 0.0)


def is_pullback_not_triggered(trade: dict) -> bool:
    """لو الصفقة pullback_pending ولم تتفعل → لا تُحسب مالياً."""
    entry_mode = str(trade.get("entry_mode", "") or "").lower()
    status = str(trade.get("status", "") or "").lower()
    if status == "pending_pullback":
        return True
    if entry_mode == "pullback_pending":
        diagnostics = trade.get("diagnostics", {}) or {}
        triggered = safe_bool(
            trade.get("pullback_triggered",
            diagnostics.get("pullback_triggered", False))
        )
        return not triggered
    return False


def _get_close_pcts(trade: dict) -> tuple:
    """
    إرجاع (tp1_pct, tp2_pct, trailing_pct) كأرقام بين 0 و 1.
    افتراضي: 40% / 40% / 20%
    """
    diagnostics = trade.get("diagnostics", {}) or {}

    def _get(key, default):
        v = trade.get(key)
        if v is None:
            v = diagnostics.get(key)
        return safe_float(v, default)

    tp1_pct   = _get("tp1_close_pct", DEFAULT_TP1_CLOSE_PCT) / 100.0
    tp2_pct   = _get("tp2_close_pct", DEFAULT_TP2_CLOSE_PCT) / 100.0
    trail_pct = _get("trailing_position_pct", DEFAULT_TRAILING_POSITION_PCT) / 100.0

    # normalize لو المجموع مش 100%
    total = tp1_pct + tp2_pct + trail_pct
    if total > 0 and abs(total - 1.0) > 0.01:
        tp1_pct   = tp1_pct / total
        tp2_pct   = tp2_pct / total
        trail_pct = trail_pct / total

    return tp1_pct, tp2_pct, trail_pct


def is_valid_trade_for_pnl(trade: dict) -> bool:
    """
    يتحقق من صلاحية الصفقة للدخول في حساب PnL.
    يرجع False لو أي سعر أساسي = 0 أو None أو الصفقة pending غير مفعّلة.
    """
    if not isinstance(trade, dict):
        return False

    status = str(trade.get("status", "") or "").lower()
    result = str(trade.get("result", "") or "").lower()

    # pending_pullback غير مفعّلة → لا تدخل في PnL
    if status == "pending_pullback":
        return False
    if result == "pending_expired":
        return False
    if result == "unknown" or (not result and status in ("open", "partial", "trailing_open", "tp2_partial")):
        return False

    effective_entry = safe_float(
        trade.get("effective_entry") or
        (trade.get("diagnostics") or {}).get("effective_entry") or
        trade.get("entry"),
        0.0
    )
    if effective_entry <= 0:
        return False

    # فحص حسب نوع النتيجة
    if result == "loss":
        initial_sl = safe_float(trade.get("initial_sl", trade.get("sl")), 0.0)
        if initial_sl <= 0:
            return False

    if result in ("tp1_win",):
        if safe_float(trade.get("tp1"), 0.0) <= 0:
            return False

    if result in ("tp2_win", "trailing_win"):
        if safe_float(trade.get("tp2"), 0.0) <= 0:
            return False

    return True


def calc_trade_result_pct(trade: dict) -> Optional[float]:
    """
    النسبة المئوية الخام لنتيجة الصفقة بدون رافعة.

    نسب الإغلاق: 40% عند TP1 / 40% عند TP2 / 20% trailing

    القواعد:
    - open / partial / tp2_partial / trailing_open / pending_pullback:
        لا تدخل في PnL النهائي → None
    - pullback لم يتفعل → None
    - loss:
        من entry إلى initial_sl (مش sl الحالي)
        cap: لا تتجاوز -100% (لو initial_sl = 0 ترجع None)
    - tp1_win:
        40% على TP1 + 60% محسوبين على entry (SL اتحرك لـ entry = صفر)
        فعلياً: ربح = tp1_pct × حركة_TP1
    - tp2_win:
        40% على TP1 + 40% على TP2 + 20% على trailing_exit_price أو TP2
    - trailing_win:
        40% على TP1 + 40% على TP2 + 20% على trailing_exit_price
    - expired:
        لو tp1_hit → ربح tp1_pct × حركة_TP1
        غير ذلك → 0.0
    - breakeven → 0.0
    """
    if not isinstance(trade, dict):
        return None

    if is_pullback_not_triggered(trade):
        return None

    side    = normalize_side(trade.get("direction", trade.get("side", "long")))
    result  = normalize_trade_result(trade)
    status  = normalize_trade_status(trade)

    # الحالات المفتوحة لا تدخل في PnL
    if status in ("open", "partial", "tp2_partial", "trailing_open", "pending_pullback"):
        return None
    if result in ("open", "partial", "unknown"):
        return None

    entry       = get_effective_entry(trade)
    initial_sl  = safe_float(trade.get("initial_sl"), 0.0)
    current_sl  = safe_float(trade.get("sl"), 0.0)
    # للخسارة نستخدم initial_sl دائماً
    sl_for_loss = initial_sl if initial_sl > 0 else current_sl

    tp1         = safe_float(trade.get("tp1"), 0.0)
    tp2         = safe_float(trade.get("tp2"), 0.0)
    tp1_hit     = safe_bool(trade.get("tp1_hit", False))
    tp2_hit     = safe_bool(trade.get("tp2_hit", False))

    # trailing exit price
    diagnostics = trade.get("diagnostics", {}) or {}
    trailing_exit = safe_float(
        trade.get("trailing_exit_price",
        diagnostics.get("trailing_exit_price")), 0.0
    )
    trailing_sl_price = safe_float(
        trade.get("trailing_sl",
        diagnostics.get("trailing_sl")), 0.0
    )

    tp1_w, tp2_w, trail_w = _get_close_pcts(trade)

    if entry <= 0:
        return None

    # ─────────────────────────────────────────
    # breakeven
    # ─────────────────────────────────────────
    if result == "breakeven":
        return 0.0

    # ─────────────────────────────────────────
    # expired
    # ─────────────────────────────────────────
    if result == "expired":
        if tp1_hit and tp1 > 0:
            return tp1_w * calc_side_pct(side, entry, tp1)
        return 0.0

    # ─────────────────────────────────────────
    # loss
    # ─────────────────────────────────────────
    if result == "loss":
        if sl_for_loss <= 0:
            return None
        raw = calc_side_pct(side, entry, sl_for_loss)
        # cap: لا تتجاوز -100% (مستحيل تخسر أكتر من رأس مال الصفقة)
        return max(raw, -100.0)

    # ─────────────────────────────────────────
    # tp1_win: 40% TP1 + 60% رجع لـ entry (= صفر)
    # ─────────────────────────────────────────
    if result == "tp1_win":
        if tp1 <= 0:
            return 0.0
        return tp1_w * calc_side_pct(side, entry, tp1)

    # ─────────────────────────────────────────
    # trailing_win: 40% TP1 + 40% TP2 + 20% trailing_exit
    # ─────────────────────────────────────────
    if result == "trailing_win":
        pnl = 0.0
        if tp1 > 0:
            pnl += tp1_w * calc_side_pct(side, entry, tp1)
        if tp2 > 0:
            pnl += tp2_w * calc_side_pct(side, entry, tp2)
        # trailing exit: نستخدم trailing_exit_price أو trailing_sl كاحتياطي
        t_exit = trailing_exit if trailing_exit > 0 else trailing_sl_price
        if t_exit > 0:
            pnl += trail_w * calc_side_pct(side, entry, t_exit)
        elif tp2 > 0:
            # fallback: لو مفيش trailing exit خد TP2 للـ 20%
            pnl += trail_w * calc_side_pct(side, entry, tp2)
        return pnl

    # ─────────────────────────────────────────
    # tp2_win: 40% TP1 + 40% TP2 + 20% trailing أو TP2
    # ─────────────────────────────────────────
    if result == "tp2_win":
        if tp2 <= 0:
            return None
        pnl = 0.0
        if tp1_hit and tp1 > 0:
            pnl += tp1_w * calc_side_pct(side, entry, tp1)
        elif tp1 > 0:
            # tp1 مش مسجل لكن وصلنا TP2 → احسبه
            pnl += tp1_w * calc_side_pct(side, entry, tp1)
        pnl += tp2_w * calc_side_pct(side, entry, tp2)
        # 20% trailing: لو فيه trailing_exit خده، غير كده TP2
        t_exit = trailing_exit if trailing_exit > 0 else trailing_sl_price
        if t_exit > 0:
            pnl += trail_w * calc_side_pct(side, entry, t_exit)
        else:
            pnl += trail_w * calc_side_pct(side, entry, tp2)
        return pnl

    return None


# -------------------------------
# STATUS / RESULT NORMALIZATION
# -------------------------------
def normalize_trade_status(trade: Optional[Dict]) -> str:
    """
    القيم المدعومة:
    open, partial, tp2_partial, trailing_open,
    pending_pullback, closed, expired, unknown
    """
    if not isinstance(trade, dict):
        return "unknown"

    status = str(trade.get("status") or "").strip().lower()

    if status in (
        "open", "partial", "tp2_partial", "trailing_open",
        "trailing", "closed", "expired", "pending_pullback"
    ):
        # normalize aliases
        if status == "trailing":
            return "trailing_open"
        return status

    result = str(trade.get("result") or "").strip().lower()
    if result in ("tp1_win", "tp2_win", "trailing_win", "loss", "breakeven"):
        return "closed"
    if result == "expired":
        return "expired"
    if result == "pending_expired":
        return "closed"

    return "unknown"


def normalize_trade_result(trade: Optional[Dict]) -> str:
    """
    القيم المدعومة:
    tp1_win, tp2_win, trailing_win, loss,
    expired, breakeven, pending_expired,
    open, partial, unknown
    """
    if not isinstance(trade, dict):
        return "unknown"

    result = str(trade.get("result") or "").strip().lower()

    if result in (
        "tp1_win", "tp2_win", "trailing_win",
        "loss", "expired", "breakeven", "pending_expired"
    ):
        return result

    status = normalize_trade_status(trade)
    if status in ("open", "partial", "tp2_partial",
                  "trailing_open", "pending_pullback"):
        return "open"

    return "unknown"


def is_closed_result(result: str) -> bool:
    result = str(result or "").strip().lower()
    return result in ("tp1_win", "tp2_win", "trailing_win",
                      "loss", "expired", "breakeven", "pending_expired")


def is_win_result(result: str) -> bool:
    result = str(result or "").strip().lower()
    return result in ("tp1_win", "tp2_win", "trailing_win")


# -------------------------------
# SUMMARY BUILDING
# -------------------------------
def build_empty_summary() -> dict:
    return {
        "signals": 0,
        "closed": 0,
        "open": 0,
        "trailing_open": 0,

        "wins": 0,
        "tp1_wins": 0,
        "tp2_wins": 0,
        "trailing_wins": 0,
        "losses": 0,
        "expired": 0,
        "breakeven_exits": 0,

        "tp1_hits": 0,
        "tp2_hits": 0,

        "winrate": 0.0,
        "tp1_rate": 0.0,
        "tp2_rate": 0.0,
        "tp1_to_tp2_rate": 0.0,

        "net_profit_pct": 0.0,
        "avg_profit_pct": 0.0,
        "avg_loss_pct": 0.0,
        "avg_win_pct": 0.0,

        "best_trade_pct": 0.0,
        "worst_trade_pct": 0.0,

        "realized_raw_pnl_pct": 0.0,
        "gross_profit_raw_pct": 0.0,
        "gross_loss_raw_pct": 0.0,

        "realized_leveraged_pnl_pct": 0.0,
        "gross_profit_leveraged_pct": 0.0,
        "gross_loss_leveraged_pct": 0.0,

        # حقول قديمة للتوافق
        "realized_pnl_pct": 0.0,
        "gross_profit_pct": 0.0,
        "gross_loss_pct": 0.0,

        "max_capital_usage_pct": REPORT_MAX_CAPITAL_USAGE_PCT,
        "risk_status": "normal",
        "leverage": REPORT_LEVERAGE,
        "setup_type": None,
    }


def apply_trade_to_summary(summary: dict, trade: Optional[dict]) -> dict:
    if not isinstance(summary, dict):
        summary = build_empty_summary()
    if not trade or not isinstance(trade, dict):
        return summary

    summary["signals"] = summary.get("signals", 0) + 1

    status = normalize_trade_status(trade)
    result = normalize_trade_result(trade)

    # TP1 hit flag
    tp1_flag = (
        safe_bool(trade.get("tp1_hit", False))
        or result in ("tp1_win", "tp2_win", "trailing_win")
    )
    if tp1_flag:
        summary["tp1_hits"] = summary.get("tp1_hits", 0) + 1

    # TP2 hit flag
    tp2_flag = (
        safe_bool(trade.get("tp2_hit", False))
        or result in ("tp2_win", "trailing_win")
    )
    if tp2_flag:
        summary["tp2_hits"] = summary.get("tp2_hits", 0) + 1

    # الصفقات المفتوحة
    if status in ("open", "partial"):
        summary["open"] = summary.get("open", 0) + 1
        return summary

    if status == "trailing_open":
        summary["open"] = summary.get("open", 0) + 1
        summary["trailing_open"] = summary.get("trailing_open", 0) + 1
        return summary

    if status == "pending_pullback":
        summary["open"] = summary.get("open", 0) + 1
        return summary

    # الصفقات المغلقة
    if is_closed_result(result):
        summary["closed"] = summary.get("closed", 0) + 1

        if result == "tp1_win":
            summary["wins"]     = summary.get("wins", 0) + 1
            summary["tp1_wins"] = summary.get("tp1_wins", 0) + 1
        elif result == "tp2_win":
            summary["wins"]     = summary.get("wins", 0) + 1
            summary["tp2_wins"] = summary.get("tp2_wins", 0) + 1
        elif result == "trailing_win":
            summary["wins"]          = summary.get("wins", 0) + 1
            summary["trailing_wins"] = summary.get("trailing_wins", 0) + 1
        elif result == "loss":
            summary["losses"] = summary.get("losses", 0) + 1
        elif result == "expired":
            summary["expired"] = summary.get("expired", 0) + 1
        elif result == "breakeven":
            summary["breakeven_exits"] = summary.get("breakeven_exits", 0) + 1
        elif result == "pending_expired":
            summary["expired"] = summary.get("expired", 0) + 1

        raw_pct = calc_trade_result_pct(trade)

        if raw_pct is not None:
            leveraged_pct = raw_pct * REPORT_LEVERAGE

            summary["realized_raw_pnl_pct"] = (
                summary.get("realized_raw_pnl_pct", 0.0) + raw_pct
            )

            if raw_pct > 0:
                summary["gross_profit_raw_pct"] = (
                    summary.get("gross_profit_raw_pct", 0.0) + raw_pct
                )
            elif raw_pct < 0:
                summary["gross_loss_raw_pct"] = (
                    summary.get("gross_loss_raw_pct", 0.0) + raw_pct
                )

            summary["realized_leveraged_pnl_pct"] = (
                summary.get("realized_leveraged_pnl_pct", 0.0) + leveraged_pct
            )
            summary["realized_pnl_pct"] = summary["realized_leveraged_pnl_pct"]

            if leveraged_pct > 0:
                summary["gross_profit_leveraged_pct"] = (
                    summary.get("gross_profit_leveraged_pct", 0.0) + leveraged_pct
                )
                summary["gross_profit_pct"] = summary["gross_profit_leveraged_pct"]
                if leveraged_pct > summary.get("best_trade_pct", 0.0):
                    summary["best_trade_pct"] = leveraged_pct
            elif leveraged_pct < 0:
                summary["gross_loss_leveraged_pct"] = (
                    summary.get("gross_loss_leveraged_pct", 0.0) + leveraged_pct
                )
                summary["gross_loss_pct"] = summary["gross_loss_leveraged_pct"]
                if leveraged_pct < summary.get("worst_trade_pct", 0.0):
                    summary["worst_trade_pct"] = leveraged_pct

    return summary


def finalize_summary(summary: dict) -> dict:
    if not isinstance(summary, dict):
        summary = build_empty_summary()

    wins           = safe_int(summary.get("wins", 0))
    tp1_wins       = safe_int(summary.get("tp1_wins", 0))
    tp2_wins       = safe_int(summary.get("tp2_wins", 0))
    trailing_wins  = safe_int(summary.get("trailing_wins", 0))
    losses         = safe_int(summary.get("losses", 0))
    expired        = safe_int(summary.get("expired", 0))
    breakeven      = safe_int(summary.get("breakeven_exits", 0))
    closed         = safe_int(summary.get("closed", 0))
    total_signals  = safe_int(summary.get("signals", 0))
    tp1_hits       = safe_int(summary.get("tp1_hits", 0))
    tp2_hits       = safe_int(summary.get("tp2_hits", 0))

    decided = wins + losses  # breakeven مش win ولا loss للـ WR

    summary["winrate"]         = round((wins / decided) * 100.0, 2) if decided > 0 else 0.0
    summary["tp1_rate"]        = round((tp1_hits / total_signals) * 100.0, 2) if total_signals > 0 else 0.0
    summary["tp2_rate"]        = round((tp2_hits / total_signals) * 100.0, 2) if total_signals > 0 else 0.0
    summary["tp1_to_tp2_rate"] = round((tp2_hits / tp1_hits) * 100.0, 2) if tp1_hits > 0 else 0.0

    summary["tp1_wins"]       = tp1_wins
    summary["tp2_wins"]       = tp2_wins
    summary["trailing_wins"]  = trailing_wins
    summary["losses"]         = losses
    summary["expired"]        = expired
    summary["closed"]         = closed
    summary["breakeven_exits"] = breakeven

    gross_profit = safe_float(summary.get("gross_profit_pct", 0.0))
    gross_loss   = safe_float(summary.get("gross_loss_pct", 0.0))

    avg_profit = round(gross_profit / wins, 4) if wins > 0 else 0.0
    avg_loss   = round(gross_loss / losses, 4) if losses > 0 else 0.0

    summary["avg_profit_pct"] = avg_profit
    summary["avg_loss_pct"]   = avg_loss
    summary["avg_win_pct"]    = avg_profit

    summary["net_profit_pct"] = safe_float(
        summary.get("realized_leveraged_pnl_pct",
        summary.get("realized_pnl_pct", 0.0))
    )

    # normalize للتوافق
    summary["realized_pnl_pct"]             = safe_float(summary.get("realized_pnl_pct"))
    summary["gross_profit_pct"]             = safe_float(summary.get("gross_profit_pct"))
    summary["gross_loss_pct"]               = safe_float(summary.get("gross_loss_pct"))
    summary["realized_leveraged_pnl_pct"]   = safe_float(summary.get("realized_leveraged_pnl_pct"))
    summary["gross_profit_leveraged_pct"]   = safe_float(summary.get("gross_profit_leveraged_pct"))
    summary["gross_loss_leveraged_pct"]     = safe_float(summary.get("gross_loss_leveraged_pct"))
    summary["realized_raw_pnl_pct"]         = safe_float(summary.get("realized_raw_pnl_pct"))
    summary["gross_profit_raw_pct"]         = safe_float(summary.get("gross_profit_raw_pct"))
    summary["gross_loss_raw_pct"]           = safe_float(summary.get("gross_loss_raw_pct"))
    summary["best_trade_pct"]               = safe_float(summary.get("best_trade_pct"))
    summary["worst_trade_pct"]              = safe_float(summary.get("worst_trade_pct"))

    # risk status بناءً على wallet impact
    active_slots = max(1, safe_int(REPORT_ACTIVE_TRADE_SLOTS, 10))
    wallet_pnl_pct = (
        summary.get("realized_leveraged_pnl_pct", 0.0)
        * (REPORT_MAX_CAPITAL_USAGE_PCT / 100.0)
        / active_slots
    )
    if wallet_pnl_pct <= -REPORT_DAILY_MAX_DRAWDOWN_PCT:
        summary["risk_status"] = "danger"
    elif wallet_pnl_pct <= -(REPORT_DAILY_MAX_DRAWDOWN_PCT / 2.0):
        summary["risk_status"] = "warning"
    else:
        summary["risk_status"] = "normal"

    return summary


def summarize_trades(trades: List[dict]) -> dict:
    summary = build_empty_summary()
    if not trades:
        return finalize_summary(summary)
    for trade in trades:
        apply_trade_to_summary(summary, trade)
    return finalize_summary(summary)


# -------------------------------
# EXIT QUALITY SUMMARY
# -------------------------------
def build_empty_exit_summary() -> dict:
    return {
        "signals": 0,
        "closed": 0,
        "open": 0,
        "trailing_open": 0,

        "tp1_hits": 0,
        "tp2_hits": 0,

        "tp1_wins": 0,
        "tp2_wins": 0,
        "trailing_wins": 0,
        "losses": 0,
        "expired": 0,
        "breakeven_exits": 0,

        "sl_before_tp1": 0,
        "tp1_then_entry": 0,
        "tp1_to_tp2": 0,

        "tp1_rate": 0.0,
        "tp2_rate": 0.0,
        "tp1_to_tp2_rate": 0.0,
        "sl_before_tp1_rate": 0.0,

        "exit_quality": "unknown",
    }


def apply_trade_to_exit_summary(summary: dict, trade: Optional[dict]) -> dict:
    if not isinstance(summary, dict):
        summary = build_empty_exit_summary()
    if not trade or not isinstance(trade, dict):
        return summary

    summary["signals"] += 1

    status = normalize_trade_status(trade)
    result = normalize_trade_result(trade)
    tp1_hit = (
        safe_bool(trade.get("tp1_hit", False))
        or result in ("tp1_win", "tp2_win", "trailing_win")
    )

    if status in ("open", "partial"):
        summary["open"] += 1
    elif status == "trailing_open":
        summary["open"] += 1
        summary["trailing_open"] += 1
    elif status == "pending_pullback":
        summary["open"] += 1

    if is_closed_result(result):
        summary["closed"] += 1

    if tp1_hit:
        summary["tp1_hits"] += 1

    if result in ("tp2_win", "trailing_win"):
        summary["tp2_hits"]    += 1
        summary["tp2_wins"]    += 1
        summary["tp1_to_tp2"]  += 1
        if result == "trailing_win":
            summary["trailing_wins"] += 1

    elif result == "tp1_win":
        summary["tp1_wins"]      += 1
        summary["tp1_then_entry"] += 1

    elif result == "loss":
        summary["losses"] += 1
        if not tp1_hit:
            summary["sl_before_tp1"] += 1

    elif result == "expired":
        summary["expired"] += 1

    elif result == "breakeven":
        summary["breakeven_exits"] += 1

    return summary


def finalize_exit_summary(summary: dict) -> dict:
    if not isinstance(summary, dict):
        summary = build_empty_exit_summary()

    signals       = safe_int(summary.get("signals", 0))
    closed        = safe_int(summary.get("closed", 0))
    tp1_hits      = safe_int(summary.get("tp1_hits", 0))
    tp2_hits      = safe_int(summary.get("tp2_hits", 0))
    sl_before_tp1 = safe_int(summary.get("sl_before_tp1", 0))

    summary["tp1_rate"]          = round((tp1_hits / signals) * 100.0, 2) if signals > 0 else 0.0
    summary["tp2_rate"]          = round((tp2_hits / signals) * 100.0, 2) if signals > 0 else 0.0
    summary["tp1_to_tp2_rate"]   = round((tp2_hits / tp1_hits) * 100.0, 2) if tp1_hits > 0 else 0.0
    summary["sl_before_tp1_rate"] = round((sl_before_tp1 / closed) * 100.0, 2) if closed > 0 else 0.0

    if signals == 0:
        quality = "unknown"
    elif summary["sl_before_tp1_rate"] >= 45:
        quality = "entry_problem"
    elif summary["tp1_rate"] >= 55 and summary["tp1_to_tp2_rate"] < 25:
        quality = "exit_problem"
    elif summary["tp1_rate"] >= 55 and summary["tp1_to_tp2_rate"] >= 35:
        quality = "good"
    elif summary["tp1_rate"] < 35:
        quality = "weak_entries"
    else:
        quality = "mixed"

    summary["exit_quality"] = quality
    return summary


def summarize_exits(trades: List[dict]) -> dict:
    summary = build_empty_exit_summary()
    for trade in trades or []:
        apply_trade_to_exit_summary(summary, trade)
    return finalize_exit_summary(summary)


# -------------------------------
# DAILY PERFORMANCE HELPERS
# -------------------------------
def get_trade_created_ts(trade: dict) -> int:
    if not isinstance(trade, dict):
        return 0
    return safe_int(
        trade.get("created_at",
        trade.get("candle_time",
        trade.get("opened_at", 0))),
        0,
    )


def get_local_day_key(ts: int) -> str:
    ts = safe_int(ts, 0)
    if ts <= 0:
        return "unknown"
    try:
        return time.strftime("%Y-%m-%d", time.localtime(ts))
    except Exception:
        return "unknown"


def summarize_trades_by_day(trades: List[dict], days: int = 7) -> List[dict]:
    now_ts   = int(time.time())
    days     = max(1, safe_int(days, 7))
    since_ts = now_ts - (days * 24 * 3600)

    grouped = defaultdict(list)
    for trade in trades or []:
        ts = get_trade_created_ts(trade)
        if ts <= 0 or ts < since_ts:
            continue
        grouped[get_local_day_key(ts)].append(trade)

    rows = []
    for day_key, day_trades in grouped.items():
        rows.append({
            "day": day_key,
            "summary": summarize_trades(day_trades),
            "exit_summary": summarize_exits(day_trades),
        })

    rows.sort(key=lambda x: x["day"], reverse=True)
    return rows


def summarize_today(trades: List[dict]) -> dict:
    now_ts     = int(time.time())
    local_now  = time.localtime(now_ts)
    day_start  = int(time.mktime((
        local_now.tm_year, local_now.tm_mon, local_now.tm_mday,
        0, 0, 0,
        local_now.tm_wday, local_now.tm_yday, local_now.tm_isdst,
    )))

    today_trades = [
        t for t in (trades or [])
        if get_trade_created_ts(t) >= day_start
    ]

    return {
        "day": get_local_day_key(now_ts),
        "summary": summarize_trades(today_trades),
        "exit_summary": summarize_exits(today_trades),
    }


# -------------------------------
# FIELD GROUPING HELPERS
# -------------------------------
def get_trade_field_value(trade: dict, field_name: str, default="unknown"):
    if not isinstance(trade, dict):
        return default
    diagnostics = trade.get("diagnostics", {}) or {}
    value = trade.get(field_name)
    if value is None:
        value = diagnostics.get(field_name)
    if value is None or value == "":
        return default
    return value


def summarize_by_field_from_trades(
    trades: List[dict],
    field_name: str,
    min_closed: int = 1,
) -> List[dict]:
    grouped = defaultdict(lambda: build_empty_summary())
    for trade in trades or []:
        value = str(get_trade_field_value(trade, field_name, "unknown"))
        apply_trade_to_summary(grouped[value], trade)

    rows = []
    for value, summary in grouped.items():
        finalized = finalize_summary(summary)
        if finalized.get("closed", 0) < min_closed:
            continue
        rows.append({"field_value": value, **finalized})

    rows.sort(
        key=lambda x: (
            safe_float(x.get("winrate"), 0.0),
            safe_int(x.get("closed"), 0),
            safe_float(x.get("realized_leveraged_pnl_pct"), 0.0),
        ),
        reverse=True,
    )
    return rows


# -----------------------------------
# Wrappers للتوافق مع الأسماء القديمة
# -----------------------------------
def _empty_summary(setup_type=None):
    summary = build_empty_summary()
    summary["setup_type"] = setup_type
    return summary


def _apply_trade_to_summary(summary, trade):
    return apply_trade_to_summary(summary, trade)


def _finalize_summary(summary):
    return finalize_summary(summary)
