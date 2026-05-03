import os

TRADING_MODE = os.getenv("TRADING_MODE", "scanner").strip().lower()
EXECUTION_ENABLED = os.getenv("EXECUTION_ENABLED", "false").strip().lower() == "true"

OKX_API_KEY = os.getenv("OKX_API_KEY", "").strip()
OKX_API_SECRET = os.getenv("OKX_API_SECRET", "").strip()
OKX_PASSPHRASE = os.getenv("OKX_PASSPHRASE", "").strip()

OKX_SIMULATED = os.getenv("OKX_SIMULATED", "1").strip()

DEFAULT_LEVERAGE = int(os.getenv("DEFAULT_LEVERAGE", "15"))
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "3"))
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
MOVE_SL_TO_ENTRY_AFTER_TP1 = os.getenv(
    "MOVE_SL_TO_ENTRY_AFTER_TP1",
    "true"
).strip().lower() == "true"


# Market Block Protection
MODE_BLOCK_LONGS = "BLOCK_LONGS"

PROTECT_ON_BLOCK_MIN_PROFIT_PCT = float(
    os.getenv("PROTECT_ON_BLOCK_MIN_PROFIT_PCT", "0.15")
)

PROTECT_ON_BLOCK_BUFFER_PCT = float(
    os.getenv("PROTECT_ON_BLOCK_BUFFER_PCT", "0.10")
)
