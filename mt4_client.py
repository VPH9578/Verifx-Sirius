"""
mt4_client.py — High-Speed Local TCP Bridge Driver for MetaTrader 4 (MT4).
Complies with Sirius Copier's CORE MANDATE: EXTREME RAM EFFICIENCY.

Connects to the `SiriusBridge.mq4` Expert Advisor running inside any local
MetaTrader 4 terminal over loopback TCP (default: 127.0.0.1:9001).
  - ZERO external library or DLL dependencies (pure Python socket + json).
  - Supports both Server-mode listening and Client-mode connection.
  - Drop-in compatible with MT4AccountWorker and SymbolMapper.
"""

import json
import time
import socket
import select
import threading
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Tuple


@dataclass
class MT4SymbolInfo:
    """Drop-in compatible with MT5/cTrader SymbolInfo attributes used by Sirius Copier."""
    name: str
    symbol_id: int = 1
    digits: int = 2
    point: float = 0.01
    pip_position: int = 1
    lot_size: int = 100000
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
class MT4Tick:
    """Drop-in compatible with MT5 Tick attributes (.bid, .ask, .last, .time)."""
    time: int
    bid: float
    ask: float
    last: float = 0.0
    volume: int = 1
    time_msc: int = 0


@dataclass
class MT4AccountInfo:
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


@dataclass
class MT4Position:
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


class MT4Client:
    """
    High-speed Local TCP Bridge Client talking to the `SiriusBridge.mq4` EA in MetaTrader 4.
    Provides drop-in equivalents to MetaTrader5 functions for live signal execution and monitoring.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9001,
        account_id: int = 0,
        account_str_id: str = "",
    ):
        self.host = str(host or "127.0.0.1").strip()
        self.port = int(port or 9001)
        self.account_id = int(account_id or 0)
        self.account_str_id = str(account_str_id or "").strip()
        self.trader_login: int = int(account_id or 0)

        self._server_sock: Optional[socket.socket] = None
        self._client_sock: Optional[socket.socket] = None
        self._client_file = None
        self._io_lock = threading.Lock()
        self.is_authorized = False

        self.symbols_by_name: Dict[str, MT4SymbolInfo] = {}
        self.clean_symbol_map: Dict[str, str] = {}
        self._last_error: str = ""
        self._last_state: Optional[dict] = None

    def last_error(self) -> str:
        return self._last_error

    def connect(self, timeout: float = 3.0) -> bool:
        """
        Binds listening socket (if not already open) and accepts connection from SiriusBridge.mq4.
        Also attempts outbound connect in case MT4 is acting as server.
        """
        with self._io_lock:
            # 1. If already connected, test connection
            if self._client_sock and self._client_file:
                try:
                    self._client_sock.sendall(b'{"cmd":"PING"}\n')
                    resp = self._client_file.readline()
                    if resp:
                        d = json.loads(resp.strip())
                        if d.get("ok"):
                            self.is_authorized = True
                            return True
                except Exception:
                    self._close_sockets_unlocked()

            # 2. Try binding server socket to accept MT4 connection
            if not self._server_sock:
                try:
                    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    srv.bind((self.host, self.port))
                    srv.listen(2)
                    srv.settimeout(timeout)
                    self._server_sock = srv
                except OSError as e:
                    # Port might already be listened to by MT4 acting as server
                    self._server_sock = None
                    try:
                        cli = socket.create_connection((self.host, self.port), timeout=timeout)
                        cli.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                        cli.settimeout(8.0)
                        self._client_sock = cli
                        self._client_file = cli.makefile("r", encoding="utf-8")
                    except Exception as e2:
                        self._last_error = f"Cannot bind or connect on {self.host}:{self.port} ({e2})"
                        return False

            # If we are listening, wait for MT4 EA to connect
            if self._server_sock and not self._client_sock:
                try:
                    self._server_sock.settimeout(timeout)
                    cli, addr = self._server_sock.accept()
                    cli.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    cli.settimeout(8.0)
                    self._client_sock = cli
                    self._client_file = cli.makefile("r", encoding="utf-8")
                except socket.timeout:
                    self._last_error = f"Timed out waiting for SiriusBridge.mq4 to connect on {self.host}:{self.port}. Make sure MT4 is open and SiriusBridge EA is attached to a chart."
                    return False
                except Exception as ex:
                    self._last_error = f"Accept error on {self.host}:{self.port}: {ex}"
                    return False

        # Verify handshake with PING
        state = self.send_command({"cmd": "PING"}, timeout=4.0)
        if not state or not state.get("ok"):
            err = state.get("error", "No response from MT4 EA") if state else "Empty response from MT4 EA"
            self._last_error = f"MT4 EA handshake failed: {err}"
            self.disconnect()
            return False

        self.is_authorized = True
        self._last_state = state
        self.trader_login = int(state.get("login") or self.account_id or 0)
        self.account_id = self.trader_login

        self._load_symbols_catalog()
        return True

    def _close_sockets_unlocked(self):
        self.is_authorized = False
        if self._client_file:
            try:
                self._client_file.close()
            except Exception:
                pass
            self._client_file = None
        if self._client_sock:
            try:
                self._client_sock.close()
            except Exception:
                pass
            self._client_sock = None
        if self._server_sock:
            try:
                self._server_sock.close()
            except Exception:
                pass
            self._server_sock = None

    def disconnect(self):
        with self._io_lock:
            self._close_sockets_unlocked()

    def send_command(self, payload: dict, timeout: float = 6.0) -> Optional[dict]:
        """Sends a single-line JSON command to SiriusBridge.mq4 and parses the reply."""
        with self._io_lock:
            if not self._client_sock or not self._client_file:
                return None
            try:
                self._client_sock.settimeout(timeout)
                line = json.dumps(payload) + "\n"
                self._client_sock.sendall(line.encode("utf-8"))
                resp_line = self._client_file.readline()
                if not resp_line:
                    self._last_error = "MT4 EA closed socket."
                    self.is_authorized = False
                    return None
                return json.loads(resp_line.strip())
            except Exception as e:
                self._last_error = f"MT4 EA communication error: {e}"
                self.is_authorized = False
                return None

    def _load_symbols_catalog(self):
        res = self.send_command({"cmd": "GET_SYMBOLS"}, timeout=6.0)
        if not res or not res.get("ok"):
            return

        names = res.get("symbols") or []
        self.symbols_by_name.clear()
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

            info = MT4SymbolInfo(
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
            if base not in self.clean_symbol_map or s_name.upper() == base:
                self.clean_symbol_map[base] = s_name
                if base == "XAUUSD":
                    self.clean_symbol_map["GOLD"] = s_name
                    self.clean_symbol_map["XAU"] = s_name
            if base.endswith("X") and len(base) >= 5:
                core = base[:-1]
                if core not in self.clean_symbol_map or not self.clean_symbol_map[core].endswith("x"):
                    self.clean_symbol_map[core] = s_name

        try:
            from symbol_mapper import SymbolMapper
            acc_key = self.account_str_id or (f"acc_{self.account_id}" if self.account_id else "acc_mt4")
            SymbolMapper.build_account_symbol_map(acc_key, names, platform="mt4")
        except Exception as e:
            print(f"[MT4] SymbolMapper integration notice: {e}")

    def resolve_symbol(self, raw_symbol: str) -> Optional[str]:
        if not raw_symbol or not str(raw_symbol).strip():
            return None

        # Check centralized SymbolMapper first
        try:
            from symbol_mapper import SymbolMapper
            acc_key = self.account_str_id or (f"acc_{self.account_id}" if self.account_id else "acc_mt4")
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
        return None

    def get_symbol_info(self, symbol_name: str) -> Optional[MT4SymbolInfo]:
        resolved = self.resolve_symbol(symbol_name)
        if not resolved:
            return None
        info = self.symbols_by_name.get(resolved) or self.symbols_by_name.get(symbol_name)

        if not info or not getattr(info, "_enriched", False):
            res = self.send_command({"cmd": "GET_SYMBOL_INFO", "symbol": resolved}, timeout=5.0)
            if res and res.get("ok"):
                actual_name = str(res.get("symbol", resolved))
                digits = int(res.get("digits", 2))
                point = float(res.get("point") or round(10 ** (-digits), digits))
                info = MT4SymbolInfo(
                    name=actual_name,
                    symbol_id=info.symbol_id if info else 1,
                    digits=digits,
                    point=point,
                    pip_position=max(0, digits - 1),
                    lot_size=int(float(res.get("lot_size") or 100000)),
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

    def get_tick(self, symbol_name: str) -> Optional[MT4Tick]:
        resolved = self.resolve_symbol(symbol_name)
        if not resolved:
            return None
        res = self.send_command({"cmd": "GET_TICK", "symbol": resolved}, timeout=4.0)
        if not res or not res.get("ok"):
            return None
        bid = float(res.get("bid") or 0.0)
        ask = float(res.get("ask") or bid)
        ts_sec = int(res.get("time") or int(time.time()))
        ts_msc = int(res.get("time_msc") or (ts_sec * 1000))
        return MT4Tick(time=ts_sec, bid=bid, ask=ask, last=bid, time_msc=ts_msc)

    def get_full_state(self, magic: int = None) -> Tuple[Optional[MT4AccountInfo], List[MT4Position]]:
        """Fetches account info and open positions in a single roundtrip to MT4."""
        res = self.send_command({"cmd": "GET_STATE"}, timeout=5.0)
        if not res or not res.get("ok"):
            return None, []
        self._last_state = res
        login_num = int(res.get("login") or self.trader_login or 0)
        broker = str(res.get("broker") or "MetaTrader 4")
        server = str(res.get("server") or broker)

        info = MT4AccountInfo(
            login=login_num,
            server=f"{broker} ({server})",
            balance=float(res.get("balance") or 0.0),
            equity=float(res.get("equity") or 0.0),
            margin=float(res.get("margin") or 0.0),
            margin_free=float(res.get("margin_free") or 0.0),
            profit=float(res.get("profit") or 0.0),
            currency=str(res.get("currency") or "USD"),
            leverage=int(float(res.get("leverage") or 100)),
        )

        raw_positions = res.get("positions") or []
        positions: List[MT4Position] = []
        for p in raw_positions:
            pos_magic = int(p.get("magic") or 0)
            if magic is not None and pos_magic != magic:
                continue

            positions.append(
                MT4Position(
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

    def get_account_info(self) -> Optional[MT4AccountInfo]:
        info, _ = self.get_full_state()
        return info

    def get_open_positions(self, magic: int = None) -> List[MT4Position]:
        _, pos = self.get_full_state(magic=magic)
        return pos

    def get_pending_orders(self, magic: int = None) -> List[dict]:
        res = self.send_command({"cmd": "GET_STATE"}, timeout=5.0)
        if not res or not res.get("ok"):
            return []
        raw_orders = res.get("orders") or []
        out = []
        for o in raw_orders:
            ord_magic = int(o.get("magic") or 0)
            if magic is not None and ord_magic != magic:
                continue
            out.append({
                "ticket": int(o.get("ticket") or 0),
                "symbol": str(o.get("symbol") or ""),
                "type": int(o.get("type") or 0),
                "magic": ord_magic,
                "comment": str(o.get("comment") or ""),
                "price": float(o.get("price") or 0.0),
                "volume": float(o.get("volume") or 0.0),
            })
        return out

    def get_history(self, limit: int = 50) -> List[dict]:
        res = self.send_command({"cmd": "GET_HISTORY", "limit": limit}, timeout=5.0)
        if res and res.get("ok"):
            return res.get("history") or []
        return []

    def place_order(
        self,
        symbol: str,
        direction: str,
        order_type: str,
        volume: float,
        price: float = 0.0,
        sl: float = 0.0,
        tp: float = 0.0,
        magic: int = 400000,
        comment: str = "",
    ) -> Optional[dict]:
        resolved = self.resolve_symbol(symbol)
        if not resolved:
            self._last_error = f"Strict symbol match failed for '{symbol}' in MT4"
            return None

        # Normalize direction/order_type
        direction = direction.upper().strip()
        order_type = order_type.upper().strip()
        base_dir = "BUY" if "BUY" in direction else ("SELL" if "SELL" in direction else "BUY")

        if order_type in ("BUY", "SELL", "BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP"):
            cmd_type = order_type
        elif "LIMIT" in order_type:
            cmd_type = f"{base_dir}_LIMIT"
        elif "STOP" in order_type:
            cmd_type = f"{base_dir}_STOP"
        else:
            cmd_type = base_dir

        payload = {
            "cmd": "PLACE_ORDER",
            "symbol": resolved,
            "type": cmd_type,
            "volume": float(volume),
            "price": float(price),
            "sl": float(sl),
            "tp": float(tp),
            "magic": int(magic),
            "comment": str(comment or "Sirius MT4"),
        }
        res = self.send_command(payload, timeout=8.0)
        if res and res.get("ok"):
            return res
        self._last_error = res.get("error", "Order failed") if res else "No response from MT4 EA"
        return None

    def modify_position(self, ticket: int, sl: float, tp: float) -> Optional[dict]:
        payload = {
            "cmd": "MODIFY_POSITION",
            "ticket": int(ticket),
            "sl": float(sl),
            "tp": float(tp),
        }
        return self.send_command(payload, timeout=5.0)

    def close_position(self, ticket: int, volume: float = 0.0) -> Optional[dict]:
        payload = {
            "cmd": "CLOSE_POSITION",
            "ticket": int(ticket),
            "volume": float(volume),
        }
        return self.send_command(payload, timeout=6.0)

    def cancel_order(self, ticket: int) -> Optional[dict]:
        payload = {
            "cmd": "CANCEL_ORDER",
            "ticket": int(ticket),
        }
        return self.send_command(payload, timeout=5.0)
