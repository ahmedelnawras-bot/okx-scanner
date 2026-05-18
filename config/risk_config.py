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
MIN_NORMAL_SCORE: float = 6.2
MIN_STRONG_SCORE: float = 7.5
MIN_EXECUTION_SCORE: float = 6.6

# ── Trade Structure ────────────────────────────────────────────────────────────
TP1_CLOSE_PCT: float = 40.0
TP2_CLOSE_PCT: float = 40.0
RUNNER_CLOSE_PCT: float = 20.0
RECOVERY_TP1_CLOSE_PCT: float = 50.0
RECOVERY_TP2_CLOSE_PCT: float = 25.0
RECOVERY_RUNNER_PCT: float = 25.0
TRAILING_STOP_AFTER_TP2_PCT: float = 2.0
BREAKEVEN_BUFFER_PCT: float = 0.10

# ── Recovery Window ────────────────────────────────────────────────────────────
RECOVERY_WINDOW_MINUTES: int = 90
