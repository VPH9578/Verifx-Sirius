"""
config.py — All settings for the Signal Copier Bot
Fill in your credentials and adjust settings as needed.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ═══════════════════════════════════════════════
#  TELEGRAM (Telethon)
# ═══════════════════════════════════════════════
# Get these from https://my.telegram.org
TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "")
TELEGRAM_PHONE = os.getenv("TELEGRAM_PHONE", "")

# The signal channel to monitor
# Use the @username (without @) or the channel ID (negative number)
SIGNAL_CHANNEL = os.getenv("SIGNAL_CHANNEL", "Forex")
SIGNAL_TOPIC_ID = int(os.getenv("SIGNAL_TOPIC_ID", "0"))

# ═══════════════════════════════════════════════
#  METATRADER 5
# ═══════════════════════════════════════════════
MT5_ACCOUNT = int(os.getenv("MT5_ACCOUNT", "1301110952"))
MT5_PASSWORD = os.getenv("MT5_PASSWORD", "your_password")
MT5_SERVER = os.getenv("MT5_SERVER", "XMGlobal-MT5 6")
MT5_PATH = os.getenv("MT5_PATH", "")

# Symbol mapping — signal channels say "Gold" or "XAUUSD"
# but your broker might call it "GOLD" or "XAUUSD" or "GOLD.s"
SYMBOL = "XAUUSD.sc"

# ═══════════════════════════════════════════════
#  GEMINI AI (Signal Parsing)
# ═══════════════════════════════════════════════
_raw_gemini_keys = os.getenv("GEMINI_API_KEYS", "")
if _raw_gemini_keys:
    GEMINI_API_KEYS = [k.strip() for k in _raw_gemini_keys.split(",") if k.strip()]
else:
    _single_gemini_key = os.getenv("GEMINI_API_KEY", "")
    GEMINI_API_KEYS = [_single_gemini_key] if _single_gemini_key else []
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

# ═══════════════════════════════════════════════
#  TRADE SETTINGS
# ═══════════════════════════════════════════════
DEFAULT_LOT_SIZE = 0.03         # total lot — splits across TPs
MIN_LOT = 0.01                  # broker minimum
MAGIC_NUMBER = 400401           # unique ID for this bot's trades
DEVIATION = 20                  # max slippage in points

# If entry price is within this many points of current price,
# use market order. Otherwise use pending (limit/stop).
MARKET_ORDER_THRESHOLD = 50     # points

# ═══════════════════════════════════════════════
#  DASHBOARD
# ═══════════════════════════════════════════════
REFRESH_INTERVAL = 2            # seconds between dashboard refreshes

# ═══════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════
TRADE_LOG_FILE = "trade_log.json"

# ═══════════════════════════════════════════════
#  SESSION FILE
# ═══════════════════════════════════════════════
# Telethon saves your login session here
# After first login you won't need to verify again
SESSION_FILE = "signal_copier_session"

# ═══════════════════════════════════════════════
#  BACKTESTER ISOLATION & NON-INTERFERENCE RULE
# ═══════════════════════════════════════════════
# STRICT ARCHITECTURAL DIRECTIVE:
# While building and running the Backtester, it MUST NOT interfere with,
# modify, or block any Sirius Copier live trading code (mt5_executor.py,
# signal_listener.py, signal_parser.py, db.py / stats.db, or main.py).
#
# Allowed Integration Points ONLY:
# 1. UI Adjustment: Adding the "Backtester" navigation tab under "Dashboard"
#    in the left-hand pull-out sidebar and its view panel in web/templates/index.html.
# 2. Web Router: Mounting the isolated `backtester` FastAPI router in web_server.py.
# 3. Read-Only Access: Reading historical messages via Telegram and historical
#    OHLC candles via MT5 (copy_rates_range / copy_rates_from_pos). Never send
#    live orders or mutate live trade state from the backtester.
BACKTESTER_ENABLED = True
BACKTESTER_DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "backtester", "backtest.db")
BACKTESTER_DEFAULT_BALANCE = 10000.0
BACKTESTER_DEFAULT_PENDING_EXPIRY_MINS = 30

