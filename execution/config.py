import os

TRADING_MODE = os.getenv("TRADING_MODE", "scanner").strip().lower()
EXECUTION_ENABLED = os.getenv("EXECUTION_ENABLED", "false").strip().lower() == "true"

OKX_API_KEY = os.getenv("OKX_API_KEY", "").strip()
OKX_API_SECRET = os.getenv("OKX_API_SECRET", "").strip()
OKX_PASSPHRASE = os.getenv("OKX_PASSPHRASE", "").strip()
OKX_SIMULATED = os.getenv("OKX_SIMULATED", "1").strip()

DEFAULT_LEVERAGE = int(os.getenv("DEFAULT_LEVERAGE", "15"))
# Live risk defaults. MAX_OPEN_POSITIONS remains as a hard/manual override fallback.
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "6"))
MIN_EXECUTION_SCORE = float(os.getenv("MIN_EXECUTION_SCORE", "7.0"))
OKX_BASE_URL = os.getenv("OKX_BASE_URL", "https://www.okx.com").strip()
REQUEST_TIMEOUT = 15

SAFE_MODES = {"scanner", "paper", "demo", "live_small"}
if TRADING_MODE not in SAFE_MODES:
    TRADING_MODE = "scanner"

# Partial Take Profit Management
TP1_CLOSE_PCT = float(os.getenv("TP1_CLOSE_PCT", "40"))
TP2_CLOSE_PCT = float(os.getenv("TP2_CLOSE_PCT", "40"))
TRAILING_POSITION_PCT = float(os.getenv("TRAILING_POSITION_PCT", "20"))
TRAILING_PCT = float(os.getenv("TRAILING_PCT", "2.5"))
# New TP lifecycle: TP1 closes 40% only; SL is protected after TP2.
MOVE_SL_TO_ENTRY_AFTER_TP1 = os.getenv(
    "MOVE_SL_TO_ENTRY_AFTER_TP1", "false"
).strip().lower() == "true"
TP2_PROTECTED_SL_BUFFER_PCT = float(os.getenv("TP2_PROTECTED_SL_BUFFER_PCT", "0.00"))


# Dynamic live risk sizing by start-of-day balance
DYNAMIC_POSITION_SIZING_ENABLED = os.getenv("DYNAMIC_POSITION_SIZING_ENABLED", "true").strip().lower() == "true"
START_OF_DAY_BALANCE_FALLBACK_USD = float(os.getenv("START_OF_DAY_BALANCE_FALLBACK_USD", os.getenv("EXECUTION_WALLET_FALLBACK_USD", "1000")))
MAX_CAPITAL_IN_USE_PCT = float(os.getenv("MAX_CAPITAL_IN_USE_PCT", "30"))
EXECUTION_DAILY_DRAWDOWN_LIMIT_PCT = float(os.getenv("EXECUTION_DAILY_DRAWDOWN_LIMIT_PCT", "35"))
CONSECUTIVE_SL_SOFT_WARNING = int(os.getenv("CONSECUTIVE_SL_SOFT_WARNING", "4"))
CONSECUTIVE_SL_HARD_PAUSE = int(os.getenv("CONSECUTIVE_SL_HARD_PAUSE", "6"))

# Step brackets: (min_balance, max_balance, max_positions)
DYNAMIC_POSITION_BRACKETS = [
    (500.0, 799.99, 4),
    (800.0, 1199.99, 6),
    (1200.0, 1799.99, 7),
    (1800.0, 2499.99, 8),
    (2500.0, 10**12, 10),
]

# Market Block Protection
MODE_BLOCK_LONGS = "BLOCK_LONGS"
PROTECT_ON_BLOCK_MIN_PROFIT_PCT = float(
    os.getenv("PROTECT_ON_BLOCK_MIN_PROFIT_PCT", "0.15")
)
PROTECT_ON_BLOCK_BUFFER_PCT = float(
    os.getenv("PROTECT_ON_BLOCK_BUFFER_PCT", "0.10")
)

# Single source of truth for execution whitelist.
# Signals outside this list remain normal tracking/report signals.
EXECUTION_SETUP_WHITELIST = {
    "vwap_reclaim",
    "retest_breakout_confirmed",
    "wave_3",
    "relative_strength_vs_btc",
    "elite_long_opportunity",
    "higher_low_continuation",
    "support_bounce_confirmed",
    "failed_breakdown_trap",
}

# Backward-compatible names used by older execution files.
LIVE_EXECUTION_SETUP_WHITELIST_ENABLED = os.getenv(
    "LIVE_EXECUTION_SETUP_WHITELIST_ENABLED", "true"
).strip().lower() == "true"
LIVE_EXECUTION_SETUP_WHITELIST = set(EXECUTION_SETUP_WHITELIST)
LIVE_EXECUTION_SETUP_KEYWORDS = set(EXECUTION_SETUP_WHITELIST)
