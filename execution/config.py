import os

TRADING_MODE = os.getenv("TRADING_MODE", "scanner").lower()

OKX_API_KEY = os.getenv("OKX_API_KEY")
OKX_API_SECRET = os.getenv("OKX_API_SECRET")
OKX_PASSPHRASE = os.getenv("OKX_PASSPHRASE")

OKX_SIMULATED = os.getenv("OKX_SIMULATED", "1")  # 1 = demo

EXECUTION_ENABLED = os.getenv("EXECUTION_ENABLED", "false").lower() == "true"

DEFAULT_LEVERAGE = 15
MAX_OPEN_POSITIONS = 3
MIN_EXECUTION_SCORE = 7.0
