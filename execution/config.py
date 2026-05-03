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
