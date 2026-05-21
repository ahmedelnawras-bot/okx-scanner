risk_manager-6.py — نسخة معدلة كاملة

دعم ديناميكي لحجم الصفقة وعدد الصفقات حسب رصيد بداية اليوم

كل الكود الأصلي محفوظ، مع إضافة الحماية حسب drawdown وmax_open_positions

from future import annotations from risk.drawdown_monitor import DrawdownStatus

class RiskManager: def init(self, reference_portfolio=1000.0, leverage=15): self.reference_portfolio = reference_portfolio self.leverage = leverage self.max_drawdown_pct = 0.35

def calculate_position_size(self, start_of_day_balance, trade_percent=0.035):
    """حجم الصفقة ديناميكي حسب رصيد بداية اليوم"""
    return start_of_day_balance * trade_percent

def calculate_max_open_positions(self, start_of_day_balance, position_size, max_portfolio_pct=0.25):
    """أقصى عدد صفقات مفتوحة حسب الرصيد المتاح"""
    max_portfolio_usage = start_of_day_balance * max_portfolio_pct
    return int(max_portfolio_usage / position_size)

def evaluate_execution_risk(
    self,
    score: float,
    max_open_positions: int,
    current_open_positions: int,
    min_execution_score: float,
    drawdown_status: DrawdownStatus | None = None,
) -> dict:
    """يحدد هل الصفقة مسموح بها أم لا. ترتيب الفحص: Drawdown -> Max Open -> Score"""

    remaining = max(0, int(max_open_positions) - int(current_open_positions))
    drawdown_level = getattr(drawdown_status, "level", 0)
    drawdown_pct = getattr(drawdown_status, "drawdown_pct", 0.0)

    # 1. Drawdown Protection
    if drawdown_status is not None and not drawdown_status.allowed:
        return {
            "allowed": False,
            "reason": drawdown_status.reason,
            "drawdown_level": drawdown_level,
            "drawdown_pct": drawdown_pct,
            "slots": {
                "allowed": max_open_positions,
                "counted": current_open_positions,
                "remaining": remaining,
            },
        }

    # 2. Max Open Positions
    if current_open_positions >= max_open_positions:
        return {
            "allowed": False,
            "reason": "max_positions_reached",
            "drawdown_level": drawdown_level,
            "drawdown_pct": drawdown_pct,
            "slots": {
                "allowed": max_open_positions,
                "counted": current_open_positions,
                "remaining": 0,
            },
        }

    # 3. Score Check
    if score < min_execution_score:
        return {
            "allowed": False,
            "reason": "score_too_low",
            "drawdown_level": drawdown_level,
            "drawdown_pct": drawdown_pct,
            "slots": {
                "allowed": max_open_positions,
                "counted": current_open_positions,
                "remaining": remaining,
            },
        }

    # كل الشروط متوفرة
    return {
        "allowed": True,
        "reason": "risk_pass",
        "drawdown_level": drawdown_level,
        "drawdown_pct": drawdown_pct,
        "slots": {
            "allowed": max_open_positions,
            "counted": current_open_positions,
            "remaining": remaining,
        },
    }

مثال للاختبار

if name == "main": rm = RiskManager(reference_portfolio=1000.0) start_balance = 1000.0 pos_size = rm.calculate_position_size(start_balance) max_pos = rm.calculate_max_open_positions(start_balance, pos_size) print(f"Position Size: {pos_size}, Max Open Positions: {max_pos}")
