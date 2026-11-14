"""
symbol_mapper.py — Centralized Symbol Mapping and Discovery Engine for Sirius Copier.
Automatically pulls symbol tickers from connected brokers (cTrader and MT5),
maps all provider signal tickers, acronyms, and aliases to broker-specific symbols,
and caches mappings per account for sub-millisecond lookups.
"""

import os
import json
import re
from typing import Dict, List, Optional, Any

# Directory to persist per-account symbol maps
SYMBOL_MAPS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "symbol_maps")
os.makedirs(SYMBOL_MAPS_DIR, exist_ok=True)

# Comprehensive Dictionary of Canonical Instruments and all Known Aliases
# Format: CANONICAL_KEY: [list of known provider tickers, variations, and broker names]
CANONICAL_ALIASES: Dict[str, List[str]] = {
    # ═══════════════════════════════════════════════
    #  CRYPTOCURRENCIES
    # ═══════════════════════════════════════════════
    "BTCUSD": [
        "BTCUSD", "BTC", "BITCOIN", "BTC/USD", "XBTUSD", "BTCUSDT", "XBT/USD", 
        "BTCUSD.P", "BTCUSD.SC", "BTCUSD.RAW", "BTCUSDM", "BTC-USD"
    ],
    "ETHUSD": [
        "ETHUSD", "ETH", "ETHEREUM", "ETH/USD", "ETHUSDT", 
        "ETHUSD.P", "ETHUSD.SC", "ETHUSD.RAW", "ETHUSDM", "ETH-USD"
    ],
    "SOLUSD": [
        "SOLUSD", "SOL", "SOLANA", "SOL/USD", "SOLUSDT", 
        "SOLUSD.P", "SOLUSD.SC", "SOLUSD.RAW", "SOLUSDM", "SOL-USD"
    ],
    "XRPUSD": [
        "XRPUSD", "XRP", "RIPPLE", "XRP/USD", "XRPUSDT", 
        "XRPUSD.P", "XRPUSD.SC", "XRPUSD.RAW", "XRPUSDM", "XRP-USD"
    ],
    "DOGEUSD": [
        "DOGEUSD", "DOGE", "DOGECOIN", "DOGE/USD", "DOGEUSDT", 
        "DOGEUSD.P", "DOGEUSD.SC", "DOGEUSD.RAW", "DOGEUSDM", "DOGE-USD"
    ],
    "ADAUSD": [
        "ADAUSD", "ADA", "CARDANO", "ADA/USD", "ADAUSDT", 
        "ADAUSD.P", "ADAUSD.SC", "ADAUSD.RAW", "ADAUSDM", "ADA-USD"
    ],
    "DOTUSD": [
        "DOTUSD", "DOT", "POLKADOT", "DOT/USD", "DOTUSDT", 
        "DOTUSD.P", "DOTUSD.SC", "DOTUSD.RAW", "DOTUSDM", "DOT-USD"
    ],
    "MATICUSD": [
        "MATICUSD", "MATIC", "POLYGON", "POL", "MATIC/USD", "MATICUSDT", 
        "POLUSD", "POLYGONUSD"
    ],
    "LTCUSD": [
        "LTCUSD", "LTC", "LITECOIN", "LTC/USD", "LTCUSDT", 
        "LTCUSD.P", "LTCUSD.SC", "LTCUSD.RAW", "LTCUSDM", "LTC-USD"
    ],
    "LINKUSD": [
        "LINKUSD", "LINK", "CHAINLINK", "LINK/USD", "LINKUSDT", 
        "LINKUSD.P", "LINKUSD.SC", "LINK-USD"
    ],
    "BCHUSD": [
        "BCHUSD", "BCH", "BITCOINCASH", "BITCOIN CASH", "BCH/USD", "BCHUSDT", 
        "BCHUSD.P", "BCHUSD.SC"
    ],
    "XLMUSD": [
        "XLMUSD", "XLM", "STELLAR", "XLM/USD", "XLMUSDT", "STELLARUSD"
    ],
    "BNBUSD": [
        "BNBUSD", "BNB", "BINANCECOIN", "BINANCE", "BNB/USD", "BNBUSDT"
    ],
    "AVAXUSD": [
        "AVAXUSD", "AVAX", "AVALANCHE", "AVAX/USD", "AVAXUSDT"
    ],
    "EOSUSD": [
        "EOSUSD", "EOS", "EOS/USD", "EOSUSDT"
    ],
    "KSMUSD": [
        "KSMUSD", "KSM", "KUSAMA", "KSM/USD", "KUSAMAUSD"
    ],

    # ═══════════════════════════════════════════════
    #  METALS & COMMODITIES
    # ═══════════════════════════════════════════════
    "XAUUSD": [
        "XAUUSD", "GOLD", "XAU", "XAU/USD", "GOLDUSD", "SPOTGOLD", "GOLDSPOT", 
        "XAUUSD.SC", "XAUUSD.P", "XAUUSD.RAW", "XAUUSD247", "GCXX.V"
    ],
    "XAGUSD": [
        "XAGUSD", "SILVER", "XAG", "XAG/USD", "SILVERUSD", "SPOTSILVER", 
        "XAGUSD.SC", "XAGUSD.P"
    ],
    "USOUSD": [
        "USOUSD", "OIL", "WTI", "CRUDE", "CRUDEOIL", "USOIL", "CL", "CL-OIL", 
        "WTIUSD", "OILUSD", "USOIL.V", "USOUSD.SC", "CL-OIL.SC", "USO"
    ],
    "UKOIL": [
        "UKOIL", "BRENT", "BRENTOIL", "UKOIL.V", "BRENTUSD", "BRENTSPOT", "EB"
    ],
    "NATGAS": [
        "NATGAS", "NGAS", "GAS", "NATURALGAS", "XNGUSD", "GASOIL"
    ],
    "COPPER": [
        "COPPER", "HG", "COPPERUSD"
    ],

    # ═══════════════════════════════════════════════
    #  INDICES
    # ═══════════════════════════════════════════════
    "NAS100": [
        "NAS100", "NQ", "USTEC", "NDX", "NASDAQ", "NASDAQ100", "US100", "UT100", 
        "NAS", "UT100.V", "NAS100.S", "NAS100FT.S", "US100CASH", "USTECH"
    ],
    "US30": [
        "US30", "DJ30", "WS30", "DOW", "DOWJONES", "DJI", "WALLSTREET", 
        "US30CASH", "US30.V", "DJIA"
    ],
    "US500": [
        "US500", "SPX500", "SP500", "SPX", "SPY", "US500CASH", "US500.V", 
        "SPXUSD", "SP500CASH"
    ],
    "GER40": [
        "GER40", "GER30", "DAX", "DAX40", "DAX30", "DE30", "DE40", "GERMAN40", 
        "DE30.V", "GER40.S", "GER40FT.S"
    ],
    "UK100": [
        "UK100", "FTSE", "FTSE100", "UK100CASH", "UK100.V"
    ],
    "JP225": [
        "JP225", "NIKKEI", "NIKKEI225", "N225", "JPN225", "JP225.V"
    ],
    "EU50": [
        "EU50", "EUROSTOXX", "EUROSTOXX50", "SX5E", "ESTX50", "EU50.V"
    ],
    "AU200": [
        "AU200", "ASX200", "AUS200", "AU200CASH", "AU200.V"
    ],
    "CN50": [
        "CN50", "CHINA50", "A50", "FTSECHINA50", "CN50.V"
    ],

    # ═══════════════════════════════════════════════
    #  FOREX MAJOR & CROSS PAIRS
    # ═══════════════════════════════════════════════
    "EURUSD": ["EURUSD", "EUR/USD"],
    "GBPUSD": ["GBPUSD", "GBP/USD"],
    "USDJPY": ["USDJPY", "USD/JPY"],
    "USDCHF": ["USDCHF", "USD/CHF"],
    "USDCAD": ["USDCAD", "USD/CAD"],
    "AUDUSD": ["AUDUSD", "AUD/USD"],
    "NZDUSD": ["NZDUSD", "NZD/USD"],
    "EURGBP": ["EURGBP", "EUR/GBP"],
    "EURJPY": ["EURJPY", "EUR/JPY"],
    "GBPJPY": ["GBPJPY", "GBP/JPY"],
    "EURCHF": ["EURCHF", "EUR/CHF"],
    "AUDJPY": ["AUDJPY", "AUD/JPY"],
    "CADJPY": ["CADJPY", "CAD/JPY"],
    "CHFJPY": ["CHFJPY", "CHF/JPY"],
    "NZDJPY": ["NZDJPY", "NZD/JPY"],
    "GBPAUD": ["GBPAUD", "GBP/AUD"],
    "GBPCAD": ["GBPCAD", "GBP/CAD"],
    "GBPCHF": ["GBPCHF", "GBP/CHF"],
    "GBPNZD": ["GBPNZD", "GBP/NZD"],
    "EURAUD": ["EURAUD", "EUR/AUD"],
    "EURCAD": ["EURCAD", "EUR/CAD"],
    "EURNZD": ["EURNZD", "EUR/NZD"],
    "AUDCAD": ["AUDCAD", "AUD/CAD"],
    "AUDCHF": ["AUDCHF", "AUD/CHF"],
    "AUDNZD": ["AUDNZD", "AUD/NZD"],
    "CADCHF": ["CADCHF", "CAD/CHF"],
    "NZDCAD": ["NZDCAD", "NZD/CAD"],
    "NZDCHF": ["NZDCHF", "NZD/CHF"],
    "USDMXN": ["USDMXN", "USD/MXN"],
    "USDZAR": ["USDZAR", "USD/ZAR"],
    "USDTRY": ["USDTRY", "USD/TRY"],
    "USDSEK": ["USDSEK", "USD/SEK"],
    "USDNOK": ["USDNOK", "USD/NOK"],
    "EURNOK": ["EURNOK", "EUR/NOK"],
    "EURSEK": ["EURSEK", "EUR/SEK"],
    "EURPLN": ["EURPLN", "EUR/PLN"],
    "AUDSGD": ["AUDSGD", "AUD/SGD"],
    "CHFSGD": ["CHFSGD", "CHF/SGD"],
    "EURSGD": ["EURSGD", "EUR/SGD"],
    "GBPSGD": ["GBPSGD", "GBP/SGD"],
    "USDSGD": ["USDSGD", "USD/SGD"],
    "USDDKK": ["USDDKK", "USD/DKK"],
    "NOKSEK": ["NOKSEK", "NOK/SEK"]
}

def clean_key(s: str) -> str:
    """Normalize any string by removing slashes, dashes, dots, spaces, underscores, hashtags and uppercasing."""
    if not s:
        return ""
    return re.sub(r'[^A-Za-z0-9]', '', str(s)).upper()

# Build Reverse Lookup Index: Clean String -> Canonical Key
REVERSE_ALIAS_INDEX: Dict[str, str] = {}
for canonical, aliases in CANONICAL_ALIASES.items():
    REVERSE_ALIAS_INDEX[clean_key(canonical)] = canonical
    for alias in aliases:
        REVERSE_ALIAS_INDEX[clean_key(alias)] = canonical


class SymbolMapper:
    """
    Centralized Symbol Mapper managing auto-discovered broker symbols
    and resolving signal tickers to broker symbols.
    """
    _cache: Dict[str, Dict[str, str]] = {}  # account_id -> {clean_alias: broker_symbol}
    _raw_symbols_cache: Dict[str, List[str]] = {}  # account_id -> [raw broker symbols]

    @classmethod
    def get_map_file_path(cls, account_id: str) -> str:
        safe_id = clean_key(account_id) or "default"
        return os.path.join(SYMBOL_MAPS_DIR, f"{safe_id}.json")

    @classmethod
    def load_cached_map(cls, account_id: str) -> Dict[str, str]:
        """Loads cached symbol map for an account from disk or memory."""
        if account_id in cls._cache:
            return cls._cache[account_id]

        file_path = cls.get_map_file_path(account_id)
        if os.path.exists(file_path):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    cls._cache[account_id] = data.get("mappings", {})
                    cls._raw_symbols_cache[account_id] = data.get("raw_symbols", [])
                    return cls._cache[account_id]
            except Exception as e:
                print(f"[SymbolMapper] Error reading {file_path}: {e}")

        cls._cache[account_id] = {}
        return cls._cache[account_id]

    @classmethod
    def build_account_symbol_map(cls, account_id: str, raw_broker_symbols: List[str], platform: str = "") -> Dict[str, Any]:
        """
        Builds a comprehensive symbol mapping dictionary for an account from the broker's symbol list.
        Maps all aliases, tickers, and variations to the exact broker symbol.
        Persists to disk and caches in memory.
        """
        mapping: Dict[str, str] = {}
        broker_symbols_clean: Dict[str, str] = {}

        # 1. Index all raw broker symbols
        for b_sym in raw_broker_symbols:
            b_sym_str = str(b_sym).strip()
            if not b_sym_str:
                continue
            
            # Direct exact match
            mapping[b_sym_str.upper()] = b_sym_str
            clean_b = clean_key(b_sym_str)
            mapping[clean_b] = b_sym_str
            broker_symbols_clean[clean_b] = b_sym_str

            # Strip broker prefix/suffix (e.g., XAUUSD.sc -> XAUUSD, US30.v -> US30, Bitcoin -> Bitcoin)
            base_part = b_sym_str.split(".")[0].split("-")[0].split("_")[0]
            clean_base = clean_key(base_part)
            if clean_base:
                mapping[clean_base] = b_sym_str
                # Index clean_base into broker_symbols_clean if not set or prefer .sc/.v/.p/.raw over bare ticker
                if clean_base not in broker_symbols_clean or ("." in b_sym_str and "." not in broker_symbols_clean[clean_base]):
                    broker_symbols_clean[clean_base] = b_sym_str

                # For MT4 brokers with 'x' suffix (e.g. EURUSDx, XAUUSDx, GBPUSDx), map root pair to the suffix symbol
                if clean_base.endswith("X") and len(clean_base) >= 5:
                    core = clean_base[:-1]
                    mapping[core] = b_sym_str
                    broker_symbols_clean[core] = b_sym_str

        # 2. Match against Canonical Aliases
        for canonical, aliases in CANONICAL_ALIASES.items():
            matched_broker_sym = None

            # Check if canonical itself matches any broker symbol
            clean_canonical = clean_key(canonical)
            if clean_canonical in broker_symbols_clean:
                matched_broker_sym = broker_symbols_clean[clean_canonical]
            else:
                # Check if any known alias matches a broker symbol
                for alias in aliases:
                    c_alias = clean_key(alias)
                    if c_alias in broker_symbols_clean:
                        matched_broker_sym = broker_symbols_clean[c_alias]
                        break

            # If a broker symbol was identified for this canonical instrument:
            # Map ALL known aliases to this exact broker symbol!
            if matched_broker_sym:
                mapping[clean_canonical] = matched_broker_sym
                for alias in aliases:
                    c_alias = clean_key(alias)
                    mapping[c_alias] = matched_broker_sym

        # Cache in memory
        cls._cache[account_id] = mapping
        cls._raw_symbols_cache[account_id] = raw_broker_symbols

        # Save to disk
        file_path = cls.get_map_file_path(account_id)
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump({
                    "account_id": account_id,
                    "platform": platform,
                    "broker_symbols_count": len(raw_broker_symbols),
                    "mapped_aliases_count": len(mapping),
                    "raw_symbols": raw_broker_symbols,
                    "mappings": mapping
                }, f, indent=2)
        except Exception as e:
            print(f"[SymbolMapper] Failed to persist {file_path}: {e}")

        # Compute sample verification mappings
        samples = {}
        for sample_key in ["XAUUSD", "BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD", "USOUSD", "NAS100", "US30", "EURUSD"]:
            res = mapping.get(clean_key(sample_key))
            if res:
                samples[sample_key] = res

        print(f"[SymbolMapper] [{account_id}] Mapped {len(raw_broker_symbols)} broker symbols to {len(mapping)} tradeable aliases.")
        return {
            "account_id": account_id,
            "platform": platform,
            "broker_symbols_count": len(raw_broker_symbols),
            "mapped_aliases_count": len(mapping),
            "sample_mappings": samples
        }

    @classmethod
    def resolve_symbol(cls, account_id: str, raw_symbol: str) -> Optional[str]:
        """
        Resolves any provider raw symbol/ticker to the broker's exact symbol name.
        Uses the account's compiled mapping.
        """
        if not raw_symbol or not str(raw_symbol).strip():
            return None

        sym_str = str(raw_symbol).strip()
        account_map = cls.load_cached_map(account_id)
        
        # 1. Direct clean lookup
        clean = clean_key(sym_str)
        if clean in account_map:
            return account_map[clean]

        # 2. Check canonical reverse index
        if clean in REVERSE_ALIAS_INDEX:
            canonical = REVERSE_ALIAS_INDEX[clean]
            canonical_clean = clean_key(canonical)
            if canonical_clean in account_map:
                return account_map[canonical_clean]

        # 3. Check exact case-insensitive raw symbol
        upper_raw = sym_str.upper()
        if upper_raw in account_map:
            return account_map[upper_raw]

        # 4. Fallback: check if symbol exists directly in raw broker symbols
        raw_list = cls._raw_symbols_cache.get(account_id, [])
        for b_sym in raw_list:
            if clean_key(b_sym) == clean:
                # Add to cache for future lookups
                account_map[clean] = b_sym
                return b_sym

        return None

    @classmethod
    def sync_account_symbols_from_broker(cls, account_id: str, acc_config: dict) -> Dict[str, Any]:
        """
        Actively pulls symbols from the broker based on account type (cTrader socket or MT5 terminal)
        and builds/updates the account's symbol map.
        """
        platform = str(acc_config.get("platform", "mt5")).lower()

        if platform == "ctrader":
            host = acc_config.get("ctrader_host", "127.0.0.1")
            port = int(acc_config.get("ctrader_port", 8888))
            try:
                import socket
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(5.0)
                s.connect((host, port))
                req = json.dumps({"cmd": "GET_SYMBOLS"}) + "\n"
                s.sendall(req.encode("utf-8"))
                
                buf = b""
                while True:
                    chunk = s.recv(4096)
                    if not chunk:
                        break
                    buf += chunk
                    if b"\n" in chunk:
                        break
                s.close()
                res = json.loads(buf.decode("utf-8").strip())
                symbols = res.get("symbols", [])
                if symbols:
                    return cls.build_account_symbol_map(account_id, symbols, platform="ctrader")
                else:
                    return {"account_id": account_id, "error": "cTrader bridge returned no symbols"}
            except Exception as e:
                return {"account_id": account_id, "error": f"Failed to connect to cTrader bridge on {host}:{port}: {e}"}

        elif platform == "mt5":
            try:
                import MetaTrader5 as mt5
                # Check if MT5 is already initialized
                is_init = False
                try:
                    if mt5.terminal_info() is not None:
                        is_init = True
                except Exception:
                    pass

                if not is_init:
                    login = acc_config.get("mt5_login")
                    password = acc_config.get("mt5_password")
                    server = acc_config.get("mt5_server")
                    path = acc_config.get("mt5_path")
                    init_kwargs = {}
                    if path:
                        init_kwargs["path"] = path
                    if not mt5.initialize(**init_kwargs):
                        return {"account_id": account_id, "error": f"MT5 initialize failed: {mt5.last_error()}"}
                    if login and password and server:
                        mt5.login(int(login), password=password, server=server)

                symbols = mt5.symbols_get()
                if symbols:
                    # Filter for active or tradeable symbols
                    tradeable_names = [s.name for s in symbols if getattr(s, "trade_mode", 1) != 0]
                    return cls.build_account_symbol_map(account_id, tradeable_names, platform="mt5")
                else:
                    return {"account_id": account_id, "error": "MT5 symbols_get() returned no symbols"}
            except Exception as e:
                return {"account_id": account_id, "error": f"MT5 symbol discovery exception: {e}"}

        elif platform == "mt4":
            host = acc_config.get("mt4_host", "127.0.0.1")
            port = int(acc_config.get("mt4_port", 9001))
            try:
                from mt4_client import MT4Client
                client = MT4Client(host=host, port=port, account_str_id=account_id)
                if client.connect(timeout=3.0):
                    sym_res = client.send_command({"cmd": "GET_SYMBOLS"}, timeout=5.0)
                    client.disconnect()
                    if sym_res and sym_res.get("ok"):
                        symbols = sym_res.get("symbols", [])
                        if symbols:
                            return cls.build_account_symbol_map(account_id, symbols, platform="mt4")
                        else:
                            return {"account_id": account_id, "error": "MT4 bridge returned no symbols"}
                return {"account_id": account_id, "error": f"Failed to connect to MT4 bridge on {host}:{port}: {client.last_error()}"}
            except Exception as e:
                return {"account_id": account_id, "error": f"MT4 symbol discovery exception: {e}"}

        return {"account_id": account_id, "error": f"Unsupported platform: {platform}"}
