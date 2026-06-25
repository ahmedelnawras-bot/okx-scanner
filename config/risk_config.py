"""Centralized risk configuration for OKX Futures Long Bot.

Phase 2 — Single source of truth لكل الـ risk parameters.
أي تغيير في الـ limits بيتعمل هنا فقط وبيتطبق في كل مكان.
"""
from __future__ import annotations

# ── Position Limits ────────────────────────────────────────────────────────────
MAX_DAILY_OPEN_TRADES: int = 7          # الحد الأقصى للصفقات المفتوحة يومياً
MAX_EXECUTION_POSITIONS: int = 7        # نفس الرقم — unified
MAX_RECOVERY_TRADES_PER_CYCLE: int = 3  # أقصى صفقات recovery في الدورة
MAX_BLOCK_EXCEPTION_TRADES: int = 3     # أقصى block exceptions

# ── Portfolio Settings ─────────────────────────────────────────────────────────
REFERENCE_PORTFOLIO_USDT: float = 1000.0   # المحفظة المرجعية للحسابات
PAPER_MARGIN_PER_TRADE_USDT: float = 35.0  # هامش ثابت لكل صفقة
DEFAULT_LEVERAGE: int = 15                  # الرافعة الثابتة للنظام

# ── Drawdown Protection ────────────────────────────────────────────────────────
MAX_DAILY_DRAWDOWN_PCT: float = 35.0    # أقصى خسارة من بداية اليوم (%)
DRAWDOWN_WARNING_PCT: float = 20.0      # تحذير عند هذه النسبة
DRAWDOWN_SOFT_STOP_PCT: float = 28.0    # تقليل التنفيذ عند هذه النسبة
DRAWDOWN_HARD_STOP_PCT: float = 35.0    # وقف كامل عند هذه النسبة

# ── Score Thresholds ───────────────────────────────────────────────────────────
# المصدر الموحّد الوحيد لكل عتبات السكور. execution_candidate يقرأ من هنا.
MIN_NORMAL_SCORE: float = 6.2
MIN_STRONG_SCORE: float = 7.5
MIN_EXECUTION_SCORE: float = 6.6

# عتبات تفصيلية لكل مسار/وضع (Phase 2 — توحيد)
SCORE_THRESHOLDS: dict[str, dict[str, float]] = {
    "normal": {
        "strict": 6.5,
        "elite": 7.2,
        "extra": 7.8,
        "recovery_quality": 6.8,
    },
    "strong": {
        "strict": 7.3,
        "elite": 7.5,
        "support_bounce": 8.0,
        "general": 7.75,
    },
}

# ── Trade Structure ────────────────────────────────────────────────────────────
# ✅ المعتمد: standard = 30/50/20 ، recovery = 50/25/25
TP1_CLOSE_PCT: float = 30.0
TP2_CLOSE_PCT: float = 50.0
RUNNER_CLOSE_PCT: float = 20.0
RECOVERY_TP1_CLOSE_PCT: float = 50.0
RECOVERY_TP2_CLOSE_PCT: float = 25.0
RECOVERY_RUNNER_PCT: float = 25.0

# Trailing fallback ثابت — يُستخدم فقط لو الـ adaptive trailing مش متاح
# (الـ adaptive يتحسب من avg_range_pct للعملة في lifecycle).
TRAILING_STOP_AFTER_TP2_PCT: float = 2.5
TRAILING_ADAPTIVE_MULTIPLIER: float = 1.3   # trail = avg_range × هذا
TRAILING_ADAPTIVE_FLOOR_PCT: float = 2.0
TRAILING_ADAPTIVE_CEILING_PCT: float = 4.5
BREAKEVEN_BUFFER_PCT: float = 0.10

# ── BTC Micro-Gate ─────────────────────────────────────────────────────────────
# لو BTC نازل أكثر من هذه العتبة (15m) → نرفض الدخول في NORMAL/STRONG.
# RECOVERY مستثنى لأنه أصلاً rebound متوقع.
BTC_MICRO_GATE_15M_DROP_PCT: float = -0.50

# ── Recovery Window ────────────────────────────────────────────────────────────
RECOVERY_WINDOW_MINUTES: int = 90
