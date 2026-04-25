# tracking/summary_helpers.py
"""
وحدات مساعدة مشتركة لتلخيص الصفقات وحسابات الربح/الخسارة.
تُستخدم من tracking/performance.py و analysis/performance_diagnostics.py
لتجنب التكرار وتحسين الأمان ضد القيم المفقودة.

مهم:
- هذا الملف لا يعتمد على tracking.performance نهائياً.
- performance.py يمكنه استيراد الدوال من هنا، لكن العكس غير مسموح.
"""

import logging
from typing import List, Dict, Optional

logger = logging.getLogger("okx-scanner")

# -------------------------------
# ثوابت التقرير المالي
# -------------------------------
# ملاحظة:
# لو عندك نفس الثوابت في tracking/performance.py بنفس القيم، تمام.
# الأفضل لاحقاً نقلها إلى ملف مستقل tracking/report_config.py
# واستيرادها من الملفين لتجنب اختلاف القيم.
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
        return value.strip().lower() in ("1", "true", "yes", "y")

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


# -------------------------------
# PROFIT / LOSS CALCULATIONS
# -------------------------------
def calc_long_pct(entry: float, exit_price: float) -> float:
    """نسبة حركة اللونج بدون رافعة."""
    entry = safe_float(entry, 0.0)
    exit_price = safe_float(exit_price, 0.0)

    if entry <= 0:
        return 0.0

    return ((exit_price - entry) / entry) * 100


def calc_short_pct(entry: float, exit_price: float) -> float:
    """نسبة حركة الشورت بدون رافعة."""
    entry = safe_float(entry, 0.0)
    exit_price = safe_float(exit_price, 0.0)

    if entry <= 0:
        return 0.0

    return ((entry - exit_price) / entry) * 100


def calc_trade_result_pct(trade: dict) -> Optional[float]:
    """
    تحسب النسبة المئوية الخام لنتيجة الصفقة بدون رافعة.

    المنطق:
    - loss:
        Long  = من entry إلى SL
        Short = من entry إلى SL بعكس الاتجاه

    - tp1_win:
        يتم اعتبار 50% فقط أغلقت على TP1
        والنصف الثاني رجع Entry، إذن الربح = نصف حركة TP1

    - tp2_win:
        لو tp1_hit = True:
            50% على TP1 + 50% على TP2
        لو tp1_hit = False:
            نحسب كامل الحركة إلى TP2 كحالة احتياطية

    - expired:
        لو TP1 اتلمس قبل الانتهاء:
            نصف ربح TP1
        غير كده:
            0%

    - open / partial / breakeven / نتيجة ناقصة:
        None
    """
    if not isinstance(trade, dict):
        return None

    direction = trade.get("direction", trade.get("side", "long"))
    direction = normalize_side(direction)

    diagnostics = trade.get("diagnostics", {}) or {}

    # لو الصفقة مبنية على pullback ولم يتفعل الدخول، لا نحسبها مالياً
    if (
        diagnostics.get("pullback_entry") is not None
        and not safe_bool(diagnostics.get("pullback_triggered", False))
    ):
        return None

    entry = safe_float(
        diagnostics.get("effective_entry"),
        safe_float(trade.get("entry"), 0.0)
    )

    sl = safe_float(trade.get("sl"), 0.0)
    tp1 = safe_float(trade.get("tp1"), 0.0)
    tp2 = safe_float(trade.get("tp2"), 0.0)

    tp1_hit = safe_bool(trade.get("tp1_hit", False))
    result = str(trade.get("result", "") or "").lower().strip()

    if entry <= 0:
        return None

    if result in ("", "open", "partial", "breakeven"):
        return None

    if result == "expired":
        if tp1_hit and tp1 > 0:
            if direction == "long":
                return 0.5 * calc_long_pct(entry, tp1)
            return 0.5 * calc_short_pct(entry, tp1)
        return 0.0

    if result == "loss":
        if sl <= 0:
            return None

        if direction == "long":
            return calc_long_pct(entry, sl)
        return calc_short_pct(entry, sl)

    if result == "tp1_win":
        if tp1_hit and tp1 > 0:
            if direction == "long":
                return 0.5 * calc_long_pct(entry, tp1)
            return 0.5 * calc_short_pct(entry, tp1)
        return 0.0

    if result == "tp2_win":
        if tp2 <= 0:
            return None

        if tp1_hit and tp1 > 0:
            if direction == "long":
                return (
                    0.5 * calc_long_pct(entry, tp1)
                    + 0.5 * calc_long_pct(entry, tp2)
                )
            return (
                0.5 * calc_short_pct(entry, tp1)
                + 0.5 * calc_short_pct(entry, tp2)
            )

        if direction == "long":
            return calc_long_pct(entry, tp2)
        return calc_short_pct(entry, tp2)

    return None


# -------------------------------
# NORMALIZATION HELPERS
# -------------------------------
def normalize_trade_status(trade: Optional[Dict]) -> str:
    """
    استخراج حالة الصفقة status بشكل آمن.

    القيم المعتمدة:
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

    القيم الممكنة:
    - tp1_win
    - tp2_win
    - loss
    - expired
    - open
    - unknown

    مهم:
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


# -------------------------------
# SUMMARY BUILDING
# -------------------------------
def build_empty_summary() -> dict:
    """
    إنشاء ملخص فارغ بالحقول الكاملة المعتمدة في التقارير.

    signals:
        عدد جميع الإشارات المسجلة.

    closed:
        الصفقات التي انتهت بنتيجة واضحة.

    open:
        الصفقات المفتوحة حالياً أو partial.

    wins:
        عدد الصفقات الرابحة: tp1_win + tp2_win.

    losses:
        عدد الصفقات الخاسرة.

    expired:
        عدد الصفقات المنتهية بالانتهاء.

    tp1_hits:
        عدد الصفقات التي لمست TP1.

    tp2_hits:
        عدد الصفقات التي لمست TP2.

    winrate:
        wins / (wins + losses).

    tp1_rate:
        tp1_hits / signals.

    net_profit_pct:
        صافي الربح/الخسارة المرفوع بالرافعة.

    avg_profit_pct:
        متوسط ربح الصفقة الرابحة، مرفوع.

    avg_loss_pct:
        متوسط خسارة الصفقة الخاسرة، مرفوع.

    best_trade_pct:
        أفضل صفقة، مرفوعة.

    worst_trade_pct:
        أسوأ صفقة، مرفوعة.
    """
    return {
        "signals": 0,
        "closed": 0,
        "open": 0,
        "wins": 0,
        "losses": 0,
        "expired": 0,
        "tp1_hits": 0,
        "tp2_hits": 0,

        "winrate": 0.0,
        "tp1_rate": 0.0,

        "net_profit_pct": 0.0,
        "avg_profit_pct": 0.0,
        "avg_loss_pct": 0.0,
        "best_trade_pct": 0.0,
        "worst_trade_pct": 0.0,

        # حقول خام بدون رافعة
        "realized_raw_pnl_pct": 0.0,
        "gross_profit_raw_pct": 0.0,
        "gross_loss_raw_pct": 0.0,

        # حقول مرفوعة بالرافعة
        "realized_leveraged_pnl_pct": 0.0,
        "gross_profit_leveraged_pct": 0.0,
        "gross_loss_leveraged_pct": 0.0,

        # حقول قديمة للتوافق مع التقارير الحالية
        "realized_pnl_pct": 0.0,
        "gross_profit_pct": 0.0,
        "gross_loss_pct": 0.0,

        # إعدادات مالية معروضة
        "max_capital_usage_pct": REPORT_MAX_CAPITAL_USAGE_PCT,
        "risk_status": "normal",
        "leverage": REPORT_LEVERAGE,
        "setup_type": None,
    }


def apply_trade_to_summary(summary: dict, trade: Optional[dict]) -> dict:
    """
    تطبيق صفقة واحدة على ملخص التداولات.

    القواعد:
    - كل trade صحيحة تُحسب كـ signal.
    - الصفقات open/partial تزيد open فقط ولا تدخل في الربح والخسارة.
    - لا نحسب wins/losses إلا لو result واضح.
    - tp1_hits يزيد إذا:
        tp1_hit = True
        أو result = tp1_win / tp2_win
    - tp2_hits يزيد فقط إذا result = tp2_win.
    """
    if not isinstance(summary, dict):
        summary = build_empty_summary()

    if not trade or not isinstance(trade, dict):
        return summary

    summary["signals"] = summary.get("signals", 0) + 1

    status = normalize_trade_status(trade)
    result = normalize_trade_result(trade)

    # TP hits
    tp1_flag = safe_bool(trade.get("tp1_hit", False)) or result in ("tp1_win", "tp2_win")
    if tp1_flag:
        summary["tp1_hits"] = summary.get("tp1_hits", 0) + 1

    if result == "tp2_win":
        summary["tp2_hits"] = summary.get("tp2_hits", 0) + 1

    # الصفقات المفتوحة
    if status in ("open", "partial") or result == "open":
        summary["open"] = summary.get("open", 0) + 1
        return summary

    # الصفقات المغلقة بنتيجة واضحة
    if result in ("tp1_win", "tp2_win", "loss", "expired"):
        summary["closed"] = summary.get("closed", 0) + 1

        if result == "tp1_win":
            summary["wins"] = summary.get("wins", 0) + 1
            if "tp1_wins" in summary:
                summary["tp1_wins"] = summary.get("tp1_wins", 0) + 1

        elif result == "tp2_win":
            summary["wins"] = summary.get("wins", 0) + 1
            if "tp2_wins" in summary:
                summary["tp2_wins"] = summary.get("tp2_wins", 0) + 1

        elif result == "loss":
            summary["losses"] = summary.get("losses", 0) + 1

        elif result == "expired":
            summary["expired"] = summary.get("expired", 0) + 1

        raw_pct = calc_trade_result_pct(trade)

        if raw_pct is not None:
            leveraged_pct = raw_pct * REPORT_LEVERAGE

            # Raw PnL
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

            # Leveraged PnL
            summary["realized_leveraged_pnl_pct"] = (
                summary.get("realized_leveraged_pnl_pct", 0.0) + leveraged_pct
            )

            if leveraged_pct > 0:
                summary["gross_profit_leveraged_pct"] = (
                    summary.get("gross_profit_leveraged_pct", 0.0) + leveraged_pct
                )
            elif leveraged_pct < 0:
                summary["gross_loss_leveraged_pct"] = (
                    summary.get("gross_loss_leveraged_pct", 0.0) + leveraged_pct
                )

            # Old compatible fields
            summary["realized_pnl_pct"] = (
                summary.get("realized_pnl_pct", 0.0) + leveraged_pct
            )

            if leveraged_pct > 0:
                summary["gross_profit_pct"] = (
                    summary.get("gross_profit_pct", 0.0) + leveraged_pct
                )

                if leveraged_pct > summary.get("best_trade_pct", 0.0):
                    summary["best_trade_pct"] = leveraged_pct

            elif leveraged_pct < 0:
                summary["gross_loss_pct"] = (
                    summary.get("gross_loss_pct", 0.0) + leveraged_pct
                )

                if leveraged_pct < summary.get("worst_trade_pct", 0.0):
                    summary["worst_trade_pct"] = leveraged_pct

    return summary


def finalize_summary(summary: dict) -> dict:
    """
    حساب النسب النهائية بعد تجميع كل الصفقات.

    لا تستخدم هذه الدالة أي imports من performance.py
    لتجنب circular imports.
    """
    if not isinstance(summary, dict):
        summary = build_empty_summary()

    wins = safe_int(summary.get("wins", 0), 0)
    losses = safe_int(summary.get("losses", 0), 0)
    decided = wins + losses

    total_signals = safe_int(summary.get("signals", 0), 0)
    tp1_hits = safe_int(summary.get("tp1_hits", 0), 0)

    summary["winrate"] = round((wins / decided) * 100, 2) if decided > 0 else 0.0
    summary["tp1_rate"] = round((tp1_hits / total_signals) * 100, 2) if total_signals > 0 else 0.0

    gross_profit = safe_float(summary.get("gross_profit_pct", 0.0), 0.0)
    gross_loss = safe_float(summary.get("gross_loss_pct", 0.0), 0.0)

    summary["avg_profit_pct"] = round(gross_profit / wins, 4) if wins > 0 else 0.0
    summary["avg_loss_pct"] = round(gross_loss / losses, 4) if losses > 0 else 0.0

    summary["net_profit_pct"] = safe_float(
        summary.get(
            "realized_leveraged_pnl_pct",
            summary.get("realized_pnl_pct", 0.0)
        ),
        0.0
    )

    # تقدير تأثير الصفقات على المحفظة بدون الاعتماد على performance.py
    # المعادلة:
    # PnL مرفوع × نسبة رأس المال المستخدمة ÷ عدد الخانات النشطة
    active_slots = max(1, safe_int(REPORT_ACTIVE_TRADE_SLOTS, 10))
    wallet_pnl_pct = (
        summary.get("realized_leveraged_pnl_pct", 0.0)
        * (REPORT_MAX_CAPITAL_USAGE_PCT / 100.0)
        / active_slots
    )

    if wallet_pnl_pct <= -REPORT_DAILY_MAX_DRAWDOWN_PCT:
        summary["risk_status"] = "danger"
    elif wallet_pnl_pct <= -(REPORT_DAILY_MAX_DRAWDOWN_PCT / 2):
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


# -----------------------------------
# Wrappers للتوافق مع الأسماء القديمة
# -----------------------------------
def _empty_summary(setup_type=None):
    """
    Wrapper للحفاظ على التوافق مع الاستدعاءات القديمة في performance.py
    أو performance_diagnostics.py.
    """
    summary = build_empty_summary()
    summary["setup_type"] = setup_type
    return summary


def _apply_trade_to_summary(summary, trade):
    """
    Wrapper لتجنب كسر أي كود قديم يستخدم الاسم القديم.
    """
    return apply_trade_to_summary(summary, trade)


def _finalize_summary(summary):
    """
    Wrapper لتجنب كسر أي كود قديم يستخدم الاسم القديم.
    """
    return finalize_summary(summary)
