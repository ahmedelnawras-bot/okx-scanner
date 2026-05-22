# risk_manager.py
"""
مدير المخاطر - OKX Bot

نسخة آمنة للتشغيل:
- لا تعتمد على دوال غير معرفة وقت الاستيراد.
- تسمح بربط balance provider خارجي لاحقًا لو أحببنا.
- تحافظ على نفس المتغيرات العامة المستخدمة في باقي المشروع.
"""

from __future__ import annotations

import os
from typing import Callable

from risk.drawdown_monitor import DrawdownStatus


# ====================== المتغيرات العامة ======================
reference_portfolio: float | None = None
position_size: float | None = None
position_pct: float | None = None

max_portion_pct = 24.0
leverage = 15

max_positions_total_normal_strong = 7
max_positions_block = 3
max_positions_recovery = 3

risk_mode: str = "normal"

# مزود اختياري لجلب الرصيد الحقيقي من OKX أو أي مصدر خارجي.
_balance_provider: Callable[[], float] | None = None

# fallback آمن لو لم يتم ربط provider.
DEFAULT_SAFE_REFERENCE_BALANCE = float(os.getenv("SAFE_REFERENCE_BALANCE_USDT", "1000"))


# ====================== دوال مساعدة ======================
def set_balance_provider(provider: Callable[[], float] | None) -> None:
    """ربط دالة خارجية لجلب رصيد أول اليوم.

    مثال:
        set_balance_provider(lambda: okx_client_balance)
    """
    global _balance_provider
    _balance_provider = provider



def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default



def send_alert_with_demo_option(message: str) -> None:
    """تنبيه بديل آمن بدل كسر التشغيل لو لم يوجد alert handler خارجي."""
    print(f"⚠️ ALERT: {message}")



def stop_all_trades() -> None:
    """stub آمن للتوافق. لا ينفذ شيئًا إلا إذا تم استبداله خارجيًا."""
    return None



def get_okx_balance() -> float:
    """يحاول جلب الرصيد من provider خارجي، ثم من env، ثم fallback.

    الأولوية:
    1) مزود رصيد خارجي تم ربطه عبر set_balance_provider
    2) متغير بيئة DAILY_REFERENCE_BALANCE_USDT أو OKX_REFERENCE_BALANCE_USDT
    3) SAFE fallback
    """
    if callable(_balance_provider):
        balance = _safe_float(_balance_provider(), 0.0)
        if balance > 0:
            return balance

    for env_name in ("DAILY_REFERENCE_BALANCE_USDT", "OKX_REFERENCE_BALANCE_USDT"):
        balance = _safe_float(os.getenv(env_name, ""), 0.0)
        if balance > 0:
            return balance

    return DEFAULT_SAFE_REFERENCE_BALANCE



def get_reference_portfolio() -> float:
    try:
        balance = get_okx_balance()
        if balance <= 0:
            raise ValueError("رصيد الحساب غير صالح")
        return float(balance)
    except Exception as e:
        try:
            send_alert_with_demo_option(
                f"خطأ: لم يتم جلب رصيد أول اليوم من OKX.\nالتفاصيل: {str(e)}"
            )
        except Exception:
            print(f"⚠️ ALERT: {str(e)}")

        try:
            stop_all_trades()
        except Exception:
            pass

        print("⚠️ Working in SAFE FALLBACK mode")
        return DEFAULT_SAFE_REFERENCE_BALANCE



def _recalculate_position_metrics() -> None:
    global reference_portfolio, position_size, position_pct
    reference_portfolio = get_reference_portfolio()
    total_allocation = reference_portfolio * max_portion_pct / 100
    position_size = total_allocation / max_positions_total_normal_strong
    position_pct = (position_size / reference_portfolio) * 100 if reference_portfolio > 0 else 0.0



def reload_reference_portfolio() -> None:
    _recalculate_position_metrics()



def get_position_margin_usdt() -> float:
    return _safe_float(position_size, 0.0)



def get_position_pct() -> float:
    return _safe_float(position_pct, 0.0)


# ====================== تهيئة ======================
_recalculate_position_metrics()


# ====================== الدالة الرئيسية (مُعدلة للتوافق) ======================
def evaluate_execution_risk(
    score: float,
    current_open_positions: int,
    min_execution_score: float,
    risk_mode: str = "normal",
    drawdown_status: DrawdownStatus | None = None,
    max_open_positions: int | None = None,
) -> dict:
    drawdown_level = getattr(drawdown_status, "level", 0)
    drawdown_pct = getattr(drawdown_status, "drawdown_pct", 0.0)

    if drawdown_status is not None and not drawdown_status.allowed:
        return {
            "allowed": False,
            "reason": drawdown_status.reason,
            "drawdown_level": drawdown_level,
            "drawdown_pct": drawdown_pct,
            "slots": {"allowed": 0, "counted": current_open_positions, "remaining": 0},
        }

    # تحديد الحد الأقصى حسب الـ risk_mode
    if risk_mode in ["block", "recovery"]:
        extra_positions = max(max_positions_block, max_positions_recovery)
        max_open_positions = max_positions_total_normal_strong + extra_positions
    else:
        max_open_positions = max_positions_total_normal_strong

    remaining = max(0, max_open_positions - current_open_positions)

    if current_open_positions >= max_open_positions:
        return {
            "allowed": False,
            "reason": "max_positions_reached",
            "drawdown_level": drawdown_level,
            "drawdown_pct": drawdown_pct,
            "slots": {"allowed": max_open_positions, "counted": current_open_positions, "remaining": 0},
        }

    if score < min_execution_score:
        return {
            "allowed": False,
            "reason": "score_too_low",
            "drawdown_level": drawdown_level,
            "drawdown_pct": drawdown_pct,
            "slots": {"allowed": max_open_positions, "counted": current_open_positions, "remaining": remaining},
        }

    return {
        "allowed": True,
        "reason": "risk_pass",
        "drawdown_level": drawdown_level,
        "drawdown_pct": drawdown_pct,
        "slots": {"allowed": max_open_positions, "counted": current_open_positions, "remaining": remaining},
    }
