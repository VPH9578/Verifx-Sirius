"""
ctrader_client.py — Lightweight Local TCP Bridge Driver for Varianse / cTrader Desktop.
Complies with Sirius Copier's CORE MANDATE: EXTREME RAM EFFICIENCY.

Connects directly to the `SiriusBridge` cBot running inside the user's local
cTrader Desktop terminal over loopback TCP (default: 127.0.0.1:8888).
  - ZERO Spotware Open API KYC or token approvals required.
  - ZERO external library dependencies (pure Python socket + json).
  - Drop-in compatible with CTraderAccountWorker and backtester/engine.py.
"""

import json
import time
import socket
import threading
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Tuple


# =====================================================================
# Timeframe Constants (used by backtester/engine.py & ctrader_client)
# =====================================================================
CT_PERIOD_M1 = 1
CT_PERIOD_M5 = 5
CT_PERIOD_M15 = 7
CT_PERIOD_M30 = 8
CT_PERIOD_H1 = 9
CT_PERIOD_H4 = 10
CT_PERIOD_D1 = 12

_PERIOD_TO_TF_STR = {
    CT_PERIOD_M1: "M1",
    CT_PERIOD_M5: "M5",
    CT_PERIOD_M15: "M15",
    CT_PERIOD_M30: "M30",
    CT_PERIOD_H1: "H1",
    CT_PERIOD_H4: "H4",
    CT_PERIOD_D1: "D1",
}


@dataclass
class CTraderSymbolInfo:
    """Drop-in compatible with MT5 SymbolInfo attributes used by Sirius Copier & Backtester."""
    name: str
    symbol_id: int = 1
    digits: int = 2
    point: float = 0.01
    pip_position: int = 1
    lot_size: int = 100
    volume_min: float = 0.01
    volume_max: float = 100.0
    volume_step: float = 0.01
    trade_tick_size: float = 0.01
    trade_tick_value: float = 1.0
    spread: int = 15
    visible: bool = True
    trade_mode: int = 4
    trade_stops_level: int = 0
    trade_freeze_level: int = 0


@dataclass
class CTraderTick:
    """Drop-in compatible with MT5 Tick attributes (.bid, .ask, .last, .time)."""
    time: int
    bid: float
    ask: float
    last: float = 0.0
    volume: int = 1
    time_msc: int = 0


@dataclass
class CTraderAccountInfo:
    """Drop-in compatible with MT5 AccountInfo attributes."""
    login: int
    server: str
    balance: float
    equity: float
    margin: float
    margin_free: float
    profit: float
    currency: str = "USD"
    leverage: int = 100
    ctid_trader_account_id: int = 0


@dataclass
class CTraderPosition:
    """Drop-in compatible with MT5 Position attributes."""
    ticket: int
    symbol: str
    type: int  # 0 = BUY, 1 = SELL
    volume: float
    price_open: float
    price_current: float
    sl: float
    tp: float
    profit: float
    magic: int
    comment: str
    time: int
    swap: float = 0.0
    commission: float = 0.0


class CTraderClient:
    """
    High-speed Local TCP Bridge Client talking to the `SiriusBridge` cBot in cTrader Desktop.
    Provides drop-in equivalents to MetaTrader5 functions for both live copying and backtesting.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8888,
        account_id: int = 0,
        env: str = "demo",
        commission_per_lot: float = 6.0,
        # Legacy kwargs accepted for backward compatibility with existing configs
        client_id: str = "",
        client_secret: str = "",
        access_token: str = "",
        account_str_id: str = "",
    ):
        self.host = str(host or "127.0.0.1").strip()
        self.port = int(port or 8888)
        self.account_id = int(account_id or 0)
        self.account_str_id = str(account_str_id or "").strip()
        self.ctid_trader_account_id: int = int(account_id or 0)
        self.trader_login: int = int(account_id or 0)
        self.env = str(env or "demo").lower()
        self.commission_per_lot = float(commission_per_lot)

        self.sock: Optional[socket.socket] = None
        self.sock_file = None
        self._io_lock = threading.Lock()
        self.is_authorized = False

        self.symbols_by_name: Dict[str, CTraderSymbolInfo] = {}
        self.symbols_by_id: Dict[int, CTraderSymbolInfo] = {}
        self.clean_symbol_map: Dict[str, str] = {}
        self._last_error: str = ""
        self._last_state: Optional[dict] = None

    def last_error(self) -> str:
        return self._last_error

    def connect(self) -> bool:
        """Connects to the local SiriusBridge cBot over TCP and loads account & symbol state."""
        with self._io_lock:
            self._close_socket_unlocked()
            try:
                s = socket.create_connection((self.host, self.port), timeout=4.0)
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                s.settimeout(12.0)
                self.sock = s
                self.sock_file = s.makefile("r", encoding="utf-8")
            except Exception as e:
                self._last_error = (
                    f"Could not connect to SiriusBridge cBot on {self.host}:{self.port} ({e}). "
                    f"Make sure cTrader Desktop is open and the SiriusBridge cBot is running (Play button clicked)."
                )
                self._close_socket_unlocked()
                return False

        # Verify handshake with GET_STATE
        state = self.send_command({"cmd": "GET_STATE"}, timeout=6.0)
        if not state or not state.get("ok"):
            err = state.get("error", "No response from cBot") if state else "Empty response from cBot"
            self._last_error = f"cBot handshake failed: {err}"
            self.disconnect()
            return False

        self.is_authorized = True
        self._last_state = state
        self.trader_login = int(state.get("login") or self.account_id or 0)
        self.ctid_trader_account_id = self.trader_login
        self.account_id = self.trader_login
        if "is_live" in state:
            self.env = "live" if state.get("is_live") else "demo"

        self._load_symbols_catalog()
        return True

    def _close_socket_unlocked(self):
        self.is_authorized = False
        if self.sock_file:
            try:
                self.sock_file.close()
            except Exception:
                pass
            self.sock_file = None
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None

    def disconnect(self):
        with self._io_lock:
            self._close_socket_unlocked()

    def send_command(self, payload: dict, timeout: float = 10.0) -> Optional[dict]:
        """Sends a single-line JSON command to the cBot and parses the single-line JSON reply."""
        with self._io_lock:
            if not self.sock or not self.sock_file:
                return None
            try:
                self.sock.settimeout(timeout)
                line = json.dumps(payload) + "\n"
                self.sock.sendall(line.encode("utf-8"))
                resp_line = self.sock_file.readline()
                if not resp_line:
                    self._last_error = "cBot closed connection."
                    self._close_socket_unlocked()
                    return None
                return json.loads(resp_line.strip())
            except Exception as e:
                self._last_error = f"cBot communication error: {e}"
                self._close_socket_unlocked()
                return None

    def _load_symbols_catalog(self):
        res = self.send_command({"cmd": "GET_SYMBOLS"}, timeout=8.0)
        if not res or not res.get("ok"):
            return

        names = res.get("symbols") or []
        self.symbols_by_name.clear()
        self.symbols_by_id.clear()
        self.clean_symbol_map.clear()

        for idx, s_name in enumerate(names, start=1):
            s_name = str(s_name).strip()
            if not s_name:
                continue
            upper = s_name.upper()
            base = upper.split(".")[0].replace("/", "").replace("-", "").replace("_", "")
            is_metal = "XAU" in base or "GOLD" in base
            is_jpy = "JPY" in base
            digits = 2 if is_metal else (3 if is_jpy else 5)
            point = round(10 ** (-digits), digits)

            info = CTraderSymbolInfo(
                name=s_name,
                symbol_id=idx,
                digits=digits,
                point=point,
                pip_position=max(0, digits - 1),
                lot_size=100 if is_metal else 100000,
                volume_min=0.01,
                volume_max=100.0,
                volume_step=0.01,
                trade_tick_size=point,
                trade_tick_value=1.0,
                spread=15,
            )
            self.symbols_by_name[s_name] = info
            self.symbols_by_id[idx] = info
            if base not in self.clean_symbol_map or s_name.upper() == base:
                self.clean_symbol_map[base] = s_name
                if base == "XAUUSD":
                    self.clean_symbol_map["GOLD"] = s_name
                    self.clean_symbol_map["XAU"] = s_name

        try:
            from symbol_mapper import SymbolMapper
            acc_key = self.account_str_id or (f"acc_{self.account_id}" if self.account_id else "acc_1793656436")
            SymbolMapper.build_account_symbol_map(acc_key, names, platform="ctrader")
        except Exception as e:
            print(f"[cTrader] SymbolMapper integration notice: {e}")

    def resolve_symbol(self, raw_symbol: str) -> Optional[str]:
        if not raw_symbol or not str(raw_symbol).strip():
            return None

        # Check centralized SymbolMapper first
        try:
            from symbol_mapper import SymbolMapper
            acc_key = self.account_str_id or (f"acc_{self.account_id}" if self.account_id else "acc_1793656436")
            resolved = SymbolMapper.resolve_symbol(acc_key, raw_symbol)
            if resolved:
                return resolved
        except Exception:
            pass

        clean = (
            str(raw_symbol)
            .upper()
            .replace("/", "")
            .replace(" ", "")
            .replace("-", "")
            .replace("_", "")
            .split(".")[0]
        )
        if clean in self.clean_symbol_map:
            return self.clean_symbol_map[clean]
        if raw_symbol in self.symbols_by_name:
            return raw_symbol
        raw_upper = str(raw_symbol).strip().upper()
        if raw_upper in self.symbols_by_name:
            return raw_upper
        # Strict match: do NOT fallback to XAUUSD or unverified symbols
        return None

    def get_symbol_info(self, symbol_name: str) -> Optional[CTraderSymbolInfo]:
        resolved = self.resolve_symbol(symbol_name)
        if not resolved:
            return None
        info = self.symbols_by_name.get(resolved) or self.symbols_by_name.get(symbol_name)

        if not info or not getattr(info, "_enriched", False):
            res = self.send_command({"cmd": "GET_SYMBOL_INFO", "symbol": resolved}, timeout=6.0)
            if res and res.get("ok"):
                actual_name = str(res.get("name", resolved))
                digits = int(res.get("digits", 2))
                point = float(res.get("point") or round(10 ** (-digits), digits))
                info = CTraderSymbolInfo(
                    name=actual_name,
                    symbol_id=info.symbol_id if info else 1,
                    digits=digits,
                    point=point,
                    pip_position=max(0, digits - 1),
                    lot_size=int(float(res.get("lot_size") or 100)),
                    volume_min=float(res.get("volume_min") or 0.01),
                    volume_max=float(res.get("volume_max") or 100.0),
                    volume_step=float(res.get("volume_step") or 0.01),
                    trade_tick_size=float(res.get("tick_size") or point),
                    trade_tick_value=float(res.get("tick_value") or 1.0),
                    spread=int(res.get("spread") or 15),
                )
                setattr(info, "_enriched", True)
                self.symbols_by_name[actual_name] = info
                self.symbols_by_name[resolved] = info
        return info

    def get_tick(self, symbol_name: str) -> Optional[CTraderTick]:
        resolved = self.resolve_symbol(symbol_name)
        if not resolved:
            return None
        res = self.send_command({"cmd": "GET_TICK", "symbol": resolved}, timeout=5.0)
        if not res or not res.get("ok"):
            return None
        bid = float(res.get("bid") or 0.0)
        ask = float(res.get("ask") or bid)
        ts_sec = int(res.get("time") or int(time.time()))
        ts_msc = int(res.get("time_msc") or (ts_sec * 1000))
        return CTraderTick(time=ts_sec, bid=bid, ask=ask, last=bid, time_msc=ts_msc)

    def get_full_state(self, magic: int = None) -> Tuple[Optional[CTraderAccountInfo], List[CTraderPosition]]:
        """Fetches account info and open positions in a single roundtrip to the cBot bridge."""
        res = self.send_command({"cmd": "GET_STATE"}, timeout=6.0)
        if not res or not res.get("ok"):
            return None, []
        self._last_state = res
        login_num = int(res.get("login") or self.trader_login or 0)
        broker = str(res.get("broker") or "cTrader")
        env_tag = "LIVE" if res.get("is_live") else "DEMO"
        info = CTraderAccountInfo(
            login=login_num,
            server=f"{broker} ({env_tag})",
            balance=float(res.get("balance") or 0.0),
            equity=float(res.get("equity") or 0.0),
            margin=float(res.get("margin") or 0.0),
            margin_free=float(res.get("margin_free") or 0.0),
            profit=float(res.get("profit") or 0.0),
            currency=str(res.get("currency") or "USD"),
            leverage=int(float(res.get("leverage") or 100)),
            ctid_trader_account_id=login_num,
        )

        raw_positions = res.get("positions") or []
        positions: List[CTraderPosition] = []
        for p in raw_positions:
            label = str(p.get("label") or "")
            pos_magic = int(label[1:]) if (label.startswith("M") and label[1:].isdigit()) else 0
            if magic is not None and pos_magic != magic:
                continue

            positions.append(
                CTraderPosition(
                    ticket=int(p.get("ticket") or 0),
                    symbol=str(p.get("symbol") or "XAUUSD"),
                    type=int(p.get("type") or 0),
                    volume=float(p.get("volume") or 0.01),
                    price_open=float(p.get("price_open") or 0.0),
                    price_current=float(p.get("price_current") or 0.0),
                    sl=float(p.get("sl") or 0.0),
                    tp=float(p.get("tp") or 0.0),
                    profit=float(p.get("profit") or 0.0),
                    magic=pos_magic,
                    comment=str(p.get("comment") or ""),
                    time=int(p.get("time") or int(time.time())),
                    swap=float(p.get("swap") or 0.0),
                    commission=float(p.get("commission") or 0.0),
                )
            )
        return info, positions

    def get_account_info(self) -> Optional[CTraderAccountInfo]:
        res = self.send_command({"cmd": "GET_STATE"}, timeout=6.0)
        if not res or not res.get("ok"):
            return None
        self._last_state = res
        login_num = int(res.get("login") or self.trader_login or 0)
        broker = str(res.get("broker") or "cTrader")
        env_tag = "LIVE" if res.get("is_live") else "DEMO"
        return CTraderAccountInfo(
            login=login_num,
            server=f"{broker} ({env_tag})",
            balance=float(res.get("balance") or 0.0),
            equity=float(res.get("equity") or 0.0),
            margin=float(res.get("margin") or 0.0),
            margin_free=float(res.get("margin_free") or 0.0),
            profit=float(res.get("profit") or 0.0),
            currency=str(res.get("currency") or "USD"),
            leverage=int(float(res.get("leverage") or 100)),
            ctid_trader_account_id=login_num,
        )

    def get_open_positions(self, magic: int = None) -> List[CTraderPosition]:
        res = self.send_command({"cmd": "GET_STATE"}, timeout=6.0)
        if not res or not res.get("ok"):
            return []
        self._last_state = res
        raw_positions = res.get("positions") or []
        out: List[CTraderPosition] = []

        for p in raw_positions:
            label = str(p.get("label") or "")
            pos_magic = int(label[1:]) if (label.startswith("M") and label[1:].isdigit()) else 0
            if magic is not None and pos_magic != magic:
                continue

            out.append(
                CTraderPosition(
                    ticket=int(p.get("ticket") or 0),
                    symbol=str(p.get("symbol") or "XAUUSD"),
                    type=int(p.get("type") or 0),
                    volume=float(p.get("volume") or 0.01),
                    price_open=float(p.get("price_open") or 0.0),
                    price_current=float(p.get("price_current") or 0.0),
                    sl=float(p.get("sl") or 0.0),
                    tp=float(p.get("tp") or 0.0),
                    profit=float(p.get("profit") or 0.0),
                    magic=pos_magic,
                    comment=str(p.get("comment") or ""),
                    time=int(p.get("time") or int(time.time())),
                    swap=float(p.get("swap") or 0.0),
                    commission=float(p.get("commission") or 0.0),
                )
            )
        return out

    def get_pending_orders(self, magic: int = None, use_cached: bool = False) -> List[dict]:
        if use_cached and self._last_state and self._last_state.get("ok"):
            res = self._last_state
        else:
            res = self.send_command({"cmd": "GET_STATE"}, timeout=6.0)
            if not res or not res.get("ok"):
                return []
            self._last_state = res
        raw_orders = res.get("orders") or []
        out = []
        for o in raw_orders:
            label = str(o.get("label") or "")
            ord_magic = int(label[1:]) if (label.startswith("M") and label[1:].isdigit()) else 0
            if magic is not None and ord_magic != magic:
                continue
            out.append({
                "ticket": int(o.get("ticket") or 0),
                "symbol": str(o.get("symbol") or ""),
                "type": int(o.get("type") or 0),
                "volume": float(o.get("volume") or 0.01),
                "price": float(o.get("price") or 0.0),
                "sl": float(o.get("sl") or 0.0),
                "tp": float(o.get("tp") or 0.0),
                "label": label,
                "magic": ord_magic,
                "comment": str(o.get("comment") or ""),
            })
        return out

    def get_history(self, limit: int = 50) -> List[dict]:
        """Fetches closed historical trades from cTrader."""
        res = self.send_command({"cmd": "GET_HISTORY", "limit": limit}, timeout=6.0)
        if res and res.get("ok"):
            return res.get("history") or []
        if self._last_state and isinstance(self._last_state.get("history"), list):
            return self._last_state["history"][:limit]
        return []

    def place_order(
        self,
        symbol: str,
        direction: str,
        order_Category: str,
        volume_lots: float,
        price: float = 0.0,
        sl: float = 0.0,
        tp: float = 0.0,
        magic: int = 400000,
        comment: str = "",
    ) -> Optional[dict]:
        """Sends a PLACE_ORDER command to the SiriusBridge cBot."""
        resolved = self.resolve_symbol(symbol)
        if not resolved:
            self._last_error = f"Strict symbol match failed for '{symbol}'"
            return None
        res = self.send_command(
            {
                "cmd": "PLACE_ORDER",
                "symbol": resolved,
                "direction": direction.upper(),
                "category": order_Category.upper(),
                "volume_lots": float(volume_lots),
                "price": float(price or 0.0),
                "sl": float(sl or 0.0),
                "tp": float(tp or 0.0),
                "label": f"M{magic}",
                "comment": str(comment or ""),
            },
            timeout=12.0,
        )
        if not res or not res.get("ok"):
            self._last_error = res.get("error", "Order rejected by cBot") if res else "Order timeout"
            return None

        return {
            "ticket": int(res.get("ticket") or 0),
            "position_id": int(res.get("position_id") or 0),
            "order_id": int(res.get("order_id") or 0),
            "symbol": str(res.get("symbol") or resolved),
            "volume": float(res.get("volume") or volume_lots),
            "price": float(res.get("price") or price or 0.0),
            "action": str(res.get("action") or "FILLED"),
        }

    def modify_position_sltp(self, position_id: int, sl: float = 0.0, tp: float = 0.0, digits: int = 2) -> bool:
        res = self.send_command(
            {
                "cmd": "MODIFY_POSITION",
                "position_id": int(position_id),
                "sl": round(float(sl), digits) if (sl and sl > 0) else 0.0,
                "tp": round(float(tp), digits) if (tp and tp > 0) else 0.0,
            },
            timeout=8.0,
        )
        return bool(res and res.get("ok"))

    def close_position(self, position_id: int, volume_lots: float = 0.0, symbol_name: str = "XAUUSD") -> bool:
        res = self.send_command(
            {
                "cmd": "CLOSE_POSITION",
                "position_id": int(position_id),
            },
            timeout=10.0,
        )
        return bool(res and res.get("ok"))

    def cancel_order(self, order_id: int) -> bool:
        res = self.send_command(
            {
                "cmd": "CANCEL_ORDER",
                "order_id": int(order_id),
            },
            timeout=8.0,
        )
        return bool(res and res.get("ok"))

    # =====================================================================
    # Historical Data Endpoints for Sirius Backtester (Candles & Ticks)
    # =====================================================================
    def fetch_trendbars(
        self,
        symbol_name: str,
        period_const: int,
        from_ts_sec: int,
        to_ts_sec: int,
    ) -> List[dict]:
        """Fetches historical OHLC bars directly from cTrader Desktop via SiriusBridge cBot."""
        resolved = self.resolve_symbol(symbol_name)
        tf_str = _PERIOD_TO_TF_STR.get(period_const, "M1")
        res = self.send_command(
            {
                "cmd": "GET_TRENDBARS",
                "symbol": resolved,
                "timeframe": tf_str,
                "from_ts": int(from_ts_sec),
                "to_ts": int(to_ts_sec),
            },
            timeout=15.0,
        )
        if not res or not res.get("ok"):
            return []
        bars = res.get("bars") or []
        bars.sort(key=lambda x: x["time"])
        return bars

    def fetch_tick_data(self, symbol_name: str, from_ts_sec: int, to_ts_sec: int) -> List[dict]:
        """Fetches historical Bid/Ask ticks directly from cTrader Desktop via SiriusBridge cBot."""
        resolved = self.resolve_symbol(symbol_name)
        bounded_to_sec = min(int(to_ts_sec), int(from_ts_sec) + (12 * 3600))
        res = self.send_command(
            {
                "cmd": "GET_TICKS",
                "symbol": resolved,
                "from_ts": int(from_ts_sec),
                "to_ts": int(bounded_to_sec),
            },
            timeout=15.0,
        )
        if not res or not res.get("ok"):
            return []
        return res.get("ticks") or []
