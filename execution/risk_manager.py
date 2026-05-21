# risk_manager_6.py
"""
نسخة محدثة: الحد الأقصى للبوت
- NORMAL + STRONG = 7 صفقات إجمالي
- BLOCK و RECOVERY = 3 صفقات إضافية خارج الحد 7
"""

from __future__ import annotations
from risk.drawdown_monitor import DrawdownStatus

# ====================== المتغيرات العامة ======================
reference_portfolio = None   # سيتم تعيينه بعد جلب الرصيد من OKX
position_size = None
position_pct = None

max_portion_pct = 24.0
leverage = 15

# الحدود حسب نوع الصفقات
max_positions_total_normal_strong = 7
max_positions_block = 3          # إضافية في وضع BLOCK
max_positions_recovery = 3       # إضافية في وضع RECOVERY


# ====================== دوال مساعدة ======================
def get_reference_portfolio() -> float:
    """جلب رصيد أول اليوم من OKX"""
    try:
        balance = get_okx_balance()   # دالة OKX API
        if balance <= 0:
            raise ValueError("رصيد الحساب غير صالح")
        return balance
    except Exception as e:
        send_alert_with_demo_option(
            f"خطأ: لم يتم جلب رصيد أول اليوم من OKX. التفاصيل: {str(e)}"
        )
        stop_all_trades()
        print("⚠️ Working in SAFE FALLBACK mode")
        return 1000.0   # قيمة افتراضية آمنة


def reload_reference_portfolio():
    """إعادة تحميل الرصيد (يمكن استدعاؤها يدويًا أو كل 24 ساعة)"""
    global reference_portfolio, position_size, position_pct
    
    reference_portfolio = get_reference_portfolio()
    total_allocation = reference_portfolio * max_portion_pct / 100
    position_size = total_allocation / max_positions_total_normal_strong
    position_pct = position_size / reference_portfolio * 100


# ====================== تهيئة البوت ======================
reference_portfolio = get_reference_portfolio()

total_allocation = reference_portfolio * max_portion_pct / 100
position_size = total_allocation / max_positions_total_normal_strong
position_pct = position_size / reference_portfolio * 100

risk_mode = "normal"   # normal | strong | block | recovery


# ====================== دالة تقييم المخاطر الرئيسية ======================
def evaluate_execution_risk(
    score: float,
    current_open_positions: int,
    min_execution_score: float,
    risk_mode: str = "normal",
    drawdown_status: DrawdownStatus | None = None,
) -> dict:
    
    drawdown_level = getattr(drawdown_status, "level", 0)
    drawdown_pct = getattr(drawdown_status, "drawdown_pct", 0.0)

    # فحص Drawdown العام
    if drawdown_status is not None and not drawdown_status.allowed:
        return {
            "allowed": False,
            "reason": drawdown_status.reason,
            "drawdown_level": drawdown_level,
            "drawdown_pct": drawdown_pct,
            "slots": {"allowed": 0, "counted": current_open_positions, "remaining": 0},
        }

    # تحديد الحد الأقصى حسب الوضع
    if risk_mode in ["block", "recovery"]:
        max_open_positions = max_positions_total_normal_strong + max_positions_block
    else:
        max_open_positions = max_positions_total_normal_strong

    remaining = max(0, max_open_positions - current_open_positions)

    # فحص عدد الصفقات المفتوحة
    if current_open_positions >= max_open_positions:
        return {
            "allowed": False,
            "reason": "max_positions_reached",
            "drawdown_level": drawdown_level,
            "drawdown_pct": drawdown_pct,
            "slots": {"allowed": max_open_positions, "counted": current_open_positions, "remaining": 0},
        }

    # فحص قوة الإشارة
    if score < min_execution_score:
        return {
            "allowed": False,
            "reason": "score_too_low",
            "drawdown_level": drawdown_level,
            "drawdown_pct": drawdown_pct,
            "slots": {"allowed": max_open_positions, "counted": current_open_positions, "remaining": remaining},
        }

    # كل شيء تمام
    return {
        "allowed": True,
        "reason": "risk_pass",
        "drawdown_level": drawdown_level,
        "drawdown_pct": drawdown_pct,
        "slots": {"allowed": max_open_positions, "counted": current_open_positions, "remaining": remaining},
    }
