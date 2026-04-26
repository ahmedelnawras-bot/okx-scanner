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


# -------------------------------
# SAFE CONVERTERS
# -------------------------------
def safe_float(value, default=0.0) -> float:
    """تحويل آمن إلى float مع معالجة None و strings و NaN."""
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
    """تحويل آمن إلى int من float أو str."""
    if value is None:
        return default

    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


def safe_bool(value, default=False) -> bool:
    """تحويل آمن إلى bool."""
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
    """تطبيع الاتجاه long/short مع قيمة افتراضية long."""
    side = (side or "long").strip().lower()
    if side not in ("long", "short"):
        return "long"
    return side


def normalize_result(result: str) -> str:
    result = str(result or "").strip().lower()
    if result in ("tp1_win", "tp2_win", "loss", "expired", "open", "partial", "breakeven"):
        return result
    return ""


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
    """نسبة حركة اللونج بدون رافعة."""
    entry = safe_float(entry, 0.0)
    exit_price = safe_float(exit_price, 0.0)

    if entry <= 0:
        return 0.0

    return ((exit_price - entry) / entry) * 100.0


def calc_short_pct(entry: float, exit_price: float) -> float:
    """نسبة حركة الشورت بدون رافعة."""
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
    استخراج سعر الدخول الفعلي.
    لو الصفقة Pullback وتفعلت، نستخدم effective_entry.
    غير ذلك نستخدم entry.
    """
    diagnostics = trade.get("diagnostics", {}) or {}

    effective_entry = safe_float(
        diagnostics.get("effective_entry"),
        safe_float(trade.get("entry"), 0.0),
    )

    if effective_entry > 0:
        return effective_entry

    return safe_float(trade.get("entry"), 0.0)


def is_pullback_not_triggered(trade: dict) -> bool:
    """
    لو الصفقة مبنية على Pullback ولم يتفعل الدخول،
    لا نحسبها مالياً كربح أو خسارة.
    """
    diagnostics = trade.get("diagnostics", {}) or {}

    has_pullback_plan = (
        diagnostics.get("pullback_entry") is not None
        or diagnostics.get("pullback_low") is not None
        or diagnostics.get("pullback_high") is not None
    )

    if not has_pullback_plan:
        return False

    return not safe_bool(diagnostics.get("pullback_triggered", False))


def calc_trade_result_pct(trade: dict) -> Optional[float]:
    """
    تحسب النسبة المئوية الخام لنتيجة الصفقة بدون رافعة.

    القواعد:
    - open / partial:
        لا تدخل في PnL النهائي.

    - pullback لم يتفعل:
        لا يدخل في PnL النهائي.

    - loss:
        يتم الحساب من entry/effective_entry إلى initial_sl.
        مهم: نستخدم initial_sl لأن sl قد يتحرك إلى entry بعد TP1.

    - tp1_win:
        نصف الصفقة أُغلق على TP1 والنصف رجع Entry.
        الربح = 50% من حركة TP1.

    - tp2_win:
        لو tp1_hit=True:
            50% على TP1 + 50% على TP2.
        لو tp1_hit=False:
            نحسب كامل الحركة إلى TP2 كاحتياطي.

    - expired:
        لو TP1 كان اتلمس:
            نصف ربح TP1.
        غير ذلك:
            0%.
    """
    if not isinstance(trade, dict):
        return None

    if is_pullback_not_triggered(trade):
        return None

    side = normalize_side(trade.get("direction", trade.get("side", "long")))
    result = normalize_trade_result(trade)

    entry = get_effective_entry(trade)
    initial_sl = safe_float(trade.get("initial_sl"), 0.0)
    current_sl = safe_float(trade.get("sl"), 0.0)
    sl = initial_sl if initial_sl > 0 else current_sl

    tp1 = safe_float(trade.get("tp1"), 0.0)
    tp2 = safe_float(trade.get("tp2"), 0.0)
    tp1_hit = safe_bool(trade.get("tp1_hit", False))

    if entry <= 0:
        return None

    if result in ("open", "partial", "breakeven", "unknown"):
        return None

    if result == "expired":
        if tp1_hit and tp1 > 0:
            return 0.5 * calc_side_pct(side, entry, tp1)
        return 0.0

    if result == "loss":
        if sl <= 0:
            return None
        return calc_side_pct(side, entry, sl)

    if result == "tp1_win":
        if tp1 > 0:
            return 0.5 * calc_side_pct(side, entry, tp1)
        return 0.0

    if result == "tp2_win":
        if tp2 <= 0:
            return None

        if tp1_hit and tp1 > 0:
            return (
                0.5 * calc_side_pct(side, entry, tp1)
                + 0.5 * calc_side_pct(side, entry, tp2)
            )

        return calc_side_pct(side, entry, tp2)

    return None


# -------------------------------
# STATUS / RESULT NORMALIZATION
# -------------------------------
def normalize_trade_status(trade: Optional[Dict]) -> str:
    """
    استخراج حالة الصفقة status بشكل آمن.

    القيم:
    - open
    - partial
    - closed
    - expired
    - unknown
    """
    if not isinstance(trade, dict):
        return "unknown"

    status = str(trade.get("status") or "").strip().lower()

    if status in ("open", "partial", "closed", "expired"):
        return status

    result = str(trade.get("result") or "").strip().lower()

    if result in ("tp1_win", "tp2_win", "loss"):
        return "closed"

    if result == "expired":
        return "expired"

    return "unknown"


def normalize_trade_result(trade: Optional[Dict]) -> str:
    """
    استخراج نتيجة الصفقة result بشكل آمن.

    لا نفترض أبداً أن closed بدون result معناها loss.
    """
    if not isinstance(trade, dict):
        return "unknown"

    result = str(trade.get("result") or "").strip().lower()

    if result in ("tp1_win", "tp2_win", "loss", "expired"):
        return result

    status = normalize_trade_status(trade)

    if status in ("open", "partial"):
        return "open"

    return "unknown"


def is_closed_result(result: str) -> bool:
    result = str(result or "").strip().lower()
    return result in ("tp1_win", "tp2_win", "loss", "expired")


def is_win_result(result: str) -> bool:
    result = str(result or "").strip().lower()
    return result in ("tp1_win", "tp2_win")


# -------------------------------
# SUMMARY BUILDING
# -------------------------------
def build_empty_summary() -> dict:
    """
    إنشاء ملخص فارغ بالحقول الكاملة المعتمدة في التقارير.
    """
    return {
        "signals": 0,
        "closed": 0,
        "open": 0,

        "wins": 0,
        "tp1_wins": 0,
        "tp2_wins": 0,
        "losses": 0,
        "expired": 0,

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
    """
    تطبيق صفقة واحدة على ملخص التداولات.

    القواعد:
    - كل trade صحيحة تُحسب signal.
    - open/partial تزيد open فقط ولا تدخل في PnL النهائي.
    - pullback غير مفعل لا يدخل في PnL، لكنه يظل signal.
    - wins/losses لا تُحسب إلا لو result واضح.
    """
    if not isinstance(summary, dict):
        summary = build_empty_summary()

    if not trade or not isinstance(trade, dict):
        return summary

    summary["signals"] = summary.get("signals", 0) + 1

    status = normalize_trade_status(trade)
    result = normalize_trade_result(trade)

    tp1_flag = safe_bool(trade.get("tp1_hit", False)) or result in ("tp1_win", "tp2_win")
    if tp1_flag:
        summary["tp1_hits"] = summary.get("tp1_hits", 0) + 1

    if result == "tp2_win":
        summary["tp2_hits"] = summary.get("tp2_hits", 0) + 1

    if status in ("open", "partial") or result == "open":
        summary["open"] = summary.get("open", 0) + 1
        return summary

    if result in ("tp1_win", "tp2_win", "loss", "expired"):
        summary["closed"] = summary.get("closed", 0) + 1

        if result == "tp1_win":
            summary["wins"] = summary.get("wins", 0) + 1
            summary["tp1_wins"] = summary.get("tp1_wins", 0) + 1

        elif result == "tp2_win":
            summary["wins"] = summary.get("wins", 0) + 1
            summary["tp2_wins"] = summary.get("tp2_wins", 0) + 1

        elif result == "loss":
            summary["losses"] = summary.get("losses", 0) + 1

        elif result == "expired":
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

            if leveraged_pct > 0:
                summary["gross_profit_leveraged_pct"] = (
                    summary.get("gross_profit_leveraged_pct", 0.0) + leveraged_pct
                )
                summary["gross_profit_pct"] = (
                    summary.get("gross_profit_pct", 0.0) + leveraged_pct
                )
                if leveraged_pct > summary.get("best_trade_pct", 0.0):
                    summary["best_trade_pct"] = leveraged_pct

            elif leveraged_pct < 0:
                summary["gross_loss_leveraged_pct"] = (
                    summary.get("gross_loss_leveraged_pct", 0.0) + leveraged_pct
                )
                summary["gross_loss_pct"] = (
                    summary.get("gross_loss_pct", 0.0) + leveraged_pct
                )
                if leveraged_pct < summary.get("worst_trade_pct", 0.0):
                    summary["worst_trade_pct"] = leveraged_pct

            summary["realized_pnl_pct"] = (
                summary.get("realized_pnl_pct", 0.0) + leveraged_pct
            )

    return summary


def finalize_summary(summary: dict) -> dict:
    """
    حساب النسب النهائية بعد تجميع كل الصفقات.
    """
    if not isinstance(summary, dict):
        summary = build_empty_summary()

    wins = safe_int(summary.get("wins", 0), 0)
    tp1_wins = safe_int(summary.get("tp1_wins", 0), 0)
    tp2_wins = safe_int(summary.get("tp2_wins", 0), 0)
    losses = safe_int(summary.get("losses", 0), 0)
    expired = safe_int(summary.get("expired", 0), 0)
    closed = safe_int(summary.get("closed", 0), 0)

    decided = wins + losses
    total_signals = safe_int(summary.get("signals", 0), 0)

    tp1_hits = safe_int(summary.get("tp1_hits", 0), 0)
    tp2_hits = safe_int(summary.get("tp2_hits", 0), 0)

    summary["winrate"] = round((wins / decided) * 100.0, 2) if decided > 0 else 0.0
    summary["tp1_rate"] = round((tp1_hits / total_signals) * 100.0, 2) if total_signals > 0 else 0.0
    summary["tp2_rate"] = round((tp2_hits / total_signals) * 100.0, 2) if total_signals > 0 else 0.0
    summary["tp1_to_tp2_rate"] = round((tp2_hits / tp1_hits) * 100.0, 2) if tp1_hits > 0 else 0.0

    summary["tp1_wins"] = tp1_wins
    summary["tp2_wins"] = tp2_wins
    summary["losses"] = losses
    summary["expired"] = expired
    summary["closed"] = closed

    gross_profit = safe_float(summary.get("gross_profit_pct", 0.0), 0.0)
    gross_loss = safe_float(summary.get("gross_loss_pct", 0.0), 0.0)

    avg_profit = round(gross_profit / wins, 4) if wins > 0 else 0.0
    avg_loss = round(gross_loss / losses, 4) if losses > 0 else 0.0

    summary["avg_profit_pct"] = avg_profit
    summary["avg_loss_pct"] = avg_loss
    summary["avg_win_pct"] = avg_profit

    summary["net_profit_pct"] = safe_float(
        summary.get(
            "realized_leveraged_pnl_pct",
            summary.get("realized_pnl_pct", 0.0),
        ),
        0.0,
    )

    summary["realized_pnl_pct"] = safe_float(summary.get("realized_pnl_pct"), 0.0)
    summary["gross_profit_pct"] = safe_float(summary.get("gross_profit_pct"), 0.0)
    summary["gross_loss_pct"] = safe_float(summary.get("gross_loss_pct"), 0.0)

    summary["realized_leveraged_pnl_pct"] = safe_float(summary.get("realized_leveraged_pnl_pct"), 0.0)
    summary["gross_profit_leveraged_pct"] = safe_float(summary.get("gross_profit_leveraged_pct"), 0.0)
    summary["gross_loss_leveraged_pct"] = safe_float(summary.get("gross_loss_leveraged_pct"), 0.0)

    summary["realized_raw_pnl_pct"] = safe_float(summary.get("realized_raw_pnl_pct"), 0.0)
    summary["gross_profit_raw_pct"] = safe_float(summary.get("gross_profit_raw_pct"), 0.0)
    summary["gross_loss_raw_pct"] = safe_float(summary.get("gross_loss_raw_pct"), 0.0)

    summary["best_trade_pct"] = safe_float(summary.get("best_trade_pct"), 0.0)
    summary["worst_trade_pct"] = safe_float(summary.get("worst_trade_pct"), 0.0)

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
    """
    أخذ قائمة من الصفقات وإرجاع ملخص نهائي جاهز للعرض.
    """
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

        "tp1_hits": 0,
        "tp2_hits": 0,

        "tp1_wins": 0,
        "tp2_wins": 0,
        "losses": 0,
        "expired": 0,

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
    tp1_hit = safe_bool(trade.get("tp1_hit", False)) or result in ("tp1_win", "tp2_win")

    if status in ("open", "partial") or result == "open":
        summary["open"] += 1

    if result in ("tp1_win", "tp2_win", "loss", "expired"):
        summary["closed"] += 1

    if tp1_hit:
        summary["tp1_hits"] += 1

    if result == "tp2_win":
        summary["tp2_hits"] += 1
        summary["tp2_wins"] += 1
        summary["tp1_to_tp2"] += 1

    elif result == "tp1_win":
        summary["tp1_wins"] += 1
        summary["tp1_then_entry"] += 1

    elif result == "loss":
        summary["losses"] += 1
        if not tp1_hit:
            summary["sl_before_tp1"] += 1

    elif result == "expired":
        summary["expired"] += 1

    return summary


def finalize_exit_summary(summary: dict) -> dict:
    if not isinstance(summary, dict):
        summary = build_empty_exit_summary()

    signals = safe_int(summary.get("signals", 0), 0)
    closed = safe_int(summary.get("closed", 0), 0)
    tp1_hits = safe_int(summary.get("tp1_hits", 0), 0)
    tp2_hits = safe_int(summary.get("tp2_hits", 0), 0)
    sl_before_tp1 = safe_int(summary.get("sl_before_tp1", 0), 0)

    summary["tp1_rate"] = round((tp1_hits / signals) * 100.0, 2) if signals > 0 else 0.0
    summary["tp2_rate"] = round((tp2_hits / signals) * 100.0, 2) if signals > 0 else 0.0
    summary["tp1_to_tp2_rate"] = round((tp2_hits / tp1_hits) * 100.0, 2) if tp1_hits > 0 else 0.0
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
        trade.get(
            "created_at",
            trade.get("candle_time", trade.get("opened_at", 0)),
        ),
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
    """
    تلخيص الصفقات يوم بيوم.
    مفيد لأوامر:
    - /report_daily
    - /report_days_7
    - /report_days_30
    """
    now_ts = int(time.time())
    days = max(1, safe_int(days, 7))
    since_ts = now_ts - (days * 24 * 3600)

    grouped = defaultdict(list)

    for trade in trades or []:
        ts = get_trade_created_ts(trade)

        if ts <= 0:
            continue

        if ts < since_ts:
            continue

        day_key = get_local_day_key(ts)
        grouped[day_key].append(trade)

    rows = []

    for day_key, day_trades in grouped.items():
        summary = summarize_trades(day_trades)
        exit_summary = summarize_exits(day_trades)

        rows.append({
            "day": day_key,
            "summary": summary,
            "exit_summary": exit_summary,
        })

    rows.sort(key=lambda x: x["day"], reverse=True)
    return rows


def summarize_today(trades: List[dict]) -> dict:
    now_ts = int(time.time())
    local_now = time.localtime(now_ts)

    day_start = int(time.mktime((
        local_now.tm_year,
        local_now.tm_mon,
        local_now.tm_mday,
        0,
        0,
        0,
        local_now.tm_wday,
        local_now.tm_yday,
        local_now.tm_isdst,
    )))

    today_trades = []

    for trade in trades or []:
        ts = get_trade_created_ts(trade)
        if ts >= day_start:
            today_trades.append(trade)

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

    if field_name in trade:
        value = trade.get(field_name)
    else:
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

        rows.append({
            "field_value": value,
            **finalized,
        })

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
