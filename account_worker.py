"""
account_worker.py — Standalone Process Worker for an MT5 Account.
Each account runs in its own process, maintaining its own MT5 connection,
its own order execution, state persistence, and trailing stop loop.
"""

import os
import sys
import time
import json
import traceback
from typing import Optional, List, Dict, Any
from multiprocessing import Queue
import MetaTrader5 as mt5

class MT5AccountWorker:
    def __init__(self, account_config: dict, in_queue: Queue, out_queue: Queue):
        self.account_id = account_config["id"]
        self.config = account_config
        self.account_config = account_config
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.is_running = True
        
        self.state_file = f"trade_state_{self.account_id}.json"
        self.message_to_tickets = {}
        self.active_trade_groups = {}
        self.trade_state = {}
        self.recent_exec_fingerprints = {}
        self.trade_counter = 0
        self.mt5_symbol_map = {}
        self.deviation = 20
        self.magic_base = int(account_config.get("magic_base", 400000))
        self.last_exec_diag = {}

        self.load_state()

    def _set_diag(self, status: str, reason: str, extra: dict = None):
        self.last_exec_diag = {
            "account_id": self.account_id,
            "status": str(status),
            "reason": str(reason),
            "extra": extra or {},
            "timestamp": time.time(),
        }

    def load_state(self):
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding="utf-8") as f:
                    data = json.load(f)
                    self.message_to_tickets = {int(k): v for k, v in data.get("message_to_tickets", {}).items()}
                    self.active_trade_groups = {int(k): v for k, v in data.get("active_trade_groups", {}).items()}
                    self.trade_counter = data.get("trade_counter", 0)
            except Exception as e:
                print(f"[{self.account_id}] Failed to load trade state: {e}")

    def save_state(self):
        try:
            with open(self.state_file, 'w', encoding="utf-8") as f:
                json.dump({
                    "message_to_tickets": self.message_to_tickets,
                    "active_trade_groups": self.active_trade_groups,
                    "trade_counter": self.trade_counter
                }, f, indent=4)
        except Exception as e:
            print(f"[{self.account_id}] Failed to save trade state: {e}")

    def connect(self):
        path = self.config.get("mt5_path")
        if path:
            init_ok = mt5.initialize(path=path)
        else:
            init_ok = mt5.initialize()

        if not init_ok:
            print(f"[{self.account_id}] MT5 initialize failed: {mt5.last_error()}")
            return False

        login_ok = mt5.login(
            int(self.config["mt5_login"]),
            password=str(self.config["mt5_password"]),
            server=str(self.config["mt5_server"])
        )
        if not login_ok:
            print(f"[{self.account_id}] MT5 login failed: {mt5.last_error()}")
            mt5.shutdown()
            return False

        # Build symbol map via SymbolMapper
        self.mt5_symbol_map = {}
        symbols = mt5.symbols_get()
        if symbols:
            tradeable_names = [s.name for s in symbols if getattr(s, 'trade_mode', 0) != 0]
            try:
                from symbol_mapper import SymbolMapper
                SymbolMapper.build_account_symbol_map(self.account_id, tradeable_names, platform="mt5")
            except Exception as e:
                print(f"[{self.account_id}] SymbolMapper init exception: {e}")

            for s in symbols:
                if getattr(s, 'trade_mode', 0) == 0:
                    continue
                base_name = s.name.split('.')[0].replace('-', '').replace('_', '').upper()
                self.mt5_symbol_map[base_name] = s.name
                if base_name == "XAUUSD":
                    self.mt5_symbol_map["GOLD"] = s.name
                    self.mt5_symbol_map["XAU"] = s.name

        print(f"[{self.account_id}] Connected successfully to MT5 Account {self.config['mt5_login']} ({self.config['mt5_server']})")
        return True

    def resolve_symbol(self, raw_symbol: str) -> Optional[str]:
        if not raw_symbol or not str(raw_symbol).strip():
            return None

        # Check centralized SymbolMapper first
        try:
            from symbol_mapper import SymbolMapper
            resolved = SymbolMapper.resolve_symbol(self.account_id, raw_symbol)
            if resolved:
                return resolved
        except Exception:
            pass

        clean = raw_symbol.upper().replace('/', '').replace(' ', '').replace('-', '').replace('_', '').split('.')[0]
        if clean in self.mt5_symbol_map:
            return self.mt5_symbol_map[clean]
        raw_clean = raw_symbol.strip()
        if raw_clean in self.mt5_symbol_map:
            return self.mt5_symbol_map[raw_clean]
        if mt5.symbol_info(raw_clean) is not None:
            return raw_clean
        return None

    def run(self):
        """Worker process loop."""
        try:
            if self.connect():
                try:
                    self.sync_closed_trades_to_db()
                except Exception as e:
                    print(f"[{self.account_id}] Initial sync_closed_trades_to_db error: {e}")
            last_ts_check = time.time()
            last_sync_check = time.time()

            while self.is_running:
                # 1. Process incoming commands from queue
                while not self.in_queue.empty():
                    msg = self.in_queue.get_nowait()
                    cmd = msg.get("cmd")
                    req_id = msg.get("req_id")
                    payload = msg.get("payload", {})

                    if cmd == "STOP":
                        self.is_running = False
                        break
                    elif cmd == "EXECUTE_SIGNAL":
                        self.last_exec_diag = {}
                        res = self.execute_signal(payload.get("signal", {}), payload.get("msg_id"), payload.get("provider_id", "1"), payload.get("settings", {}))
                        self.out_queue.put({"account_id": self.account_id, "req_id": req_id, "result": res, "diag": self.last_exec_diag})
                    elif cmd == "CLOSE_ALL":
                        res = self.close_all_positions(payload.get("provider_id"))
                        self.out_queue.put({"account_id": self.account_id, "req_id": req_id, "result": res})
                    elif cmd == "CLOSE_AND_CANCEL_ALL":
                        res = self.close_and_cancel_all(provider_id=payload.get("provider_id"), provider_ids=payload.get("provider_ids"))
                        self.out_queue.put({"account_id": self.account_id, "req_id": req_id, "result": res})
                    elif cmd == "CLOSE_TRADE":
                        res = self.close_trade_by_reply(payload.get("reply_id"))
                        self.out_queue.put({"account_id": self.account_id, "req_id": req_id, "result": res})
                    elif cmd == "CANCEL_TRADE":
                        res = self.cancel_trade_by_reply(payload.get("reply_id"))
                        self.out_queue.put({"account_id": self.account_id, "req_id": req_id, "result": res})
                    elif cmd == "MODIFY_SL":
                        res = self.modify_sl_by_reply(payload.get("reply_id"), payload.get("new_sl"))
                        self.out_queue.put({"account_id": self.account_id, "req_id": req_id, "result": res})
                    elif cmd == "MODIFY_SIGNAL_LEVELS":
                        res = self.modify_signal_levels_by_msg(
                            payload.get("message_id"),
                            payload.get("new_tps", []),
                            payload.get("new_sl", 0.0),
                            payload.get("new_entry", 0.0),
                            payload.get("provider_id"),
                            payload.get("entry_min"),
                            payload.get("entry_max"),
                        )
                        self.out_queue.put({"account_id": self.account_id, "req_id": req_id, "result": res})
                    elif cmd == "SET_BE":
                        res = self.set_be_for_provider(payload.get("provider_id"))
                        self.out_queue.put({"account_id": self.account_id, "req_id": req_id, "result": res})
                    elif cmd == "GET_STATE":
                        state = self.get_account_state()
                        self.out_queue.put({"account_id": self.account_id, "req_id": req_id, "state": state})
                    elif cmd == "GET_CANDLES":
                        res = self.get_candles(payload)
                        self.out_queue.put({"account_id": self.account_id, "req_id": req_id, "result": res})
                    elif cmd == "RESET_STATE":
                        target_pids = payload.get("provider_ids")
                        acc_pids = set(str(p) for p in self.account_config.get("providers", []))
                        if target_pids:
                            target_pids = set(str(p) for p in target_pids)
                            if not acc_pids or target_pids.issuperset(acc_pids):
                                self.active_trade_groups = {}
                                self.message_to_tickets = {}
                            else:
                                removed_tickets = set()
                                for gid in list(self.active_trade_groups.keys()):
                                    grp = self.active_trade_groups[gid]
                                    if str(grp.get("provider_id")) in target_pids:
                                        for t_k in ["tp1_ticket", "tp2_ticket", "tp3_ticket"]:
                                            t_val = grp.get(t_k)
                                            if t_val:
                                                removed_tickets.add(int(t_val))
                                        for t_info_k in (grp.get("tickets_info") or {}).keys():
                                            try:
                                                removed_tickets.add(int(t_info_k))
                                            except Exception:
                                                pass
                                        del self.active_trade_groups[gid]

                                new_msg_to_tkts = {}
                                for mid, tkts in self.message_to_tickets.items():
                                    if isinstance(tkts, list):
                                        rem = [t for t in tkts if int(t) not in removed_tickets]
                                        if rem:
                                            new_msg_to_tkts[mid] = rem
                                    elif isinstance(tkts, dict):
                                        if str(tkts.get("provider_id")) not in target_pids:
                                            new_msg_to_tkts[mid] = tkts
                                self.message_to_tickets = new_msg_to_tkts
                        else:
                            self.active_trade_groups = {}
                            self.message_to_tickets = {}
                        self.trade_state["active_trade_groups"] = self.active_trade_groups
                        self.trade_state["message_to_tickets"] = self.message_to_tickets
                        self.save_state()
                        self.out_queue.put({"account_id": self.account_id, "req_id": req_id, "result": True})

                # 2. Run trailing stop and breakeven manager every 2 seconds
                now = time.time()
                if now - last_ts_check >= 2.0:
                    last_ts_check = now
                    try:
                        self.manage_trailing_stops()
                    except Exception as e:
                        pass

                # 3. Sync closed deals and signals to SQLite DB every 5 seconds
                if now - last_sync_check >= 5.0:
                    last_sync_check = now
                    try:
                        self.sync_closed_trades_to_db()
                    except Exception as e:
                        pass

                time.sleep(0.05)

        except Exception as e:
            print(f"[{self.account_id}] Worker Exception: {e}\n{traceback.format_exc()}")
        finally:
            try:
                mt5.shutdown()
            except:
                pass
            print(f"[{self.account_id}] Worker terminated cleanly.")

    def get_account_state(self):
        info = mt5.account_info()
        positions = mt5.positions_get()
        pos_list = []
        if positions:
            for p in positions:
                pos_list.append({
                    "ticket": p.ticket,
                    "symbol": p.symbol,
                    "type": "BUY" if p.type == mt5.ORDER_TYPE_BUY else "SELL",
                    "volume": p.volume,
                    "open_price": p.price_open,
                    "current_price": p.price_current,
                    "sl": p.sl,
                    "tp": p.tp,
                    "profit": p.profit,
                    "magic": p.magic,
                    "comment": p.comment,
                    "open_time": p.time
                })

            return {
            "account_id": self.account_id,
            "connected": info is not None,
            "login": getattr(info, "login", self.config["mt5_login"]),
            "server": getattr(info, "server", self.config["mt5_server"]),
            "balance": getattr(info, "balance", 0.0),
            "equity": getattr(info, "equity", 0.0),
            "margin": getattr(info, "margin", 0.0),
            "margin_free": getattr(info, "margin_free", 0.0),
            "open_positions": pos_list
        }

    def get_candles(self, payload: dict) -> dict:
        try:
            from datetime import datetime, timezone
            raw_sym = str(payload.get("symbol") or "").strip()
            tf_str = str(payload.get("timeframe") or "M15").upper()
            from_ts = int(payload.get("from_ts") or 0)
            to_ts = int(payload.get("to_ts") or int(time.time()))

            tf_map = {
                "M1": (mt5.TIMEFRAME_M1, 60),
                "M5": (mt5.TIMEFRAME_M5, 300),
                "M15": (mt5.TIMEFRAME_M15, 900),
                "M30": (mt5.TIMEFRAME_M30, 1800),
                "H1": (mt5.TIMEFRAME_H1, 3600),
                "H4": (mt5.TIMEFRAME_H4, 14400),
                "D1": (mt5.TIMEFRAME_D1, 86400),
            }
            tf_const, tf_sec = tf_map.get(tf_str, (mt5.TIMEFRAME_M15, 900))
            sym = self.resolve_symbol(raw_sym) or raw_sym
            s_info = mt5.symbol_info(sym)
            if s_info and not s_info.visible:
                mt5.symbol_select(sym, True)
            digits = int(getattr(s_info, "digits", 2) or 2) if s_info else 2

            dt_from = datetime.fromtimestamp(max(0, from_ts), tz=timezone.utc)
            dt_to = datetime.fromtimestamp(max(from_ts + tf_sec, to_ts), tz=timezone.utc)
            rates = mt5.copy_rates_range(sym, tf_const, dt_from, dt_to)
            if rates is None or len(rates) == 0:
                rates = mt5.copy_rates_from(sym, tf_const, dt_to, 300)

            candles = []
            seen_t = set()
            if rates is not None and len(rates) > 0:
                for r in rates:
                    t_val = int(r["time"])
                    if t_val in seen_t:
                        continue
                    seen_t.add(t_val)
                    candles.append({
                        "time": t_val,
                        "open": float(r["open"]),
                        "high": float(r["high"]),
                        "low": float(r["low"]),
                        "close": float(r["close"]),
                    })
                candles.sort(key=lambda x: x["time"])
            return {"symbol": sym, "timeframe": tf_str, "digits": digits, "candles": candles}
        except Exception as e:
            return {"error": str(e), "candles": []}

    def execute_signal(self, signal: dict, message_id: int = None, provider_id: str = "1", settings: dict = None) -> list:
        import copy
        signal = copy.deepcopy(signal) if isinstance(signal, dict) else {}
        if settings is None:
            settings = {}

        if message_id is not None and int(message_id) in self.message_to_tickets and self.message_to_tickets[int(message_id)]:
            mid_int = int(message_id)
            prev_tkts = self.message_to_tickets.get(mid_int, [])
            still_valid = False
            for _g in self.active_trade_groups.values():
                if _g.get("message_id") == mid_int and _g.get("status") not in ("CLOSED", "CANCELLED"):
                    still_valid = True
                    break
            if not still_valid:
                try:
                    open_pos = {p.ticket for p in (mt5.positions_get() or [])}
                    pend_ord = {o.ticket for o in (mt5.orders_get() or [])}
                    for _t in prev_tkts:
                        if not _t:
                            continue
                        if int(_t) in open_pos or int(_t) in pend_ord:
                            still_valid = True
                            break
                        _deals = mt5.history_deals_get(position=int(_t))
                        if not _deals:
                            _deals = mt5.history_deals_get(ticket=int(_t))
                        if _deals and len(_deals) > 0:
                            still_valid = True
                            break
                except Exception:
                    still_valid = True

            if still_valid:
                print(f"[{self.account_id}] [SANITY-GUARD] Message ID {message_id} already executed. Skipping duplicate.")
                self._set_diag("ALREADY_EXECUTED", f"Message ID {message_id} already has active/filled tickets {prev_tkts}.")
                return []
            else:
                print(f"[{self.account_id}] [SANITY-GUARD] Previous pending orders {prev_tkts} for Message ID {message_id} were cancelled unfilled. Allowing fresh execution on edit.")
                self.message_to_tickets.pop(mid_int, None)

        raw_sym = signal.get("instrument") or signal.get("symbol")
        if not raw_sym:
            print(f"[{self.account_id}] [BLOCKED] Signal missing instrument/symbol. Aborting execution.")
            self._set_diag("BLOCKED_INVALID_SIGNAL", "Signal missing instrument/symbol.")
            try:
                from trade_logger import log_error
                log_error("INVALID_SIGNAL", "Signal missing instrument/symbol. Aborting execution.", provider_id=provider_id, raw_message=signal.get("raw_message", ""))
            except Exception:
                pass
            return []

        symbol = self.resolve_symbol(raw_sym)
        sym_info = mt5.symbol_info(symbol) if symbol else None
        if not symbol or not sym_info:
            err = f"Symbol '{raw_sym}' (resolved: '{symbol}') not found in MT5. Strict matching enforced: trade skipped."
            print(f"[{self.account_id}] [BLOCKED] {err}")
            self._set_diag("BLOCKED_SYMBOL_NOT_FOUND", err, {"raw_symbol": raw_sym, "resolved": symbol})
            try:
                from trade_logger import log_error
                log_error("SYMBOL_NOT_FOUND", err, provider_id=provider_id, symbol=symbol or raw_sym, raw_message=signal.get("raw_message", ""))
            except Exception:
                pass
            return []

        direction = signal.get("direction", "").upper()
        sig_order_type = signal.get("order_type", "").upper()
        entry_price = signal.get("entry")
        sl_price = signal.get("sl")

        # Safety Rule 3: No Stop Loss, No Trade
        if direction != "CLOSE" and (not sl_price or float(sl_price) <= 0):
            err = "Signal rejected: Missing mandatory Stop Loss (SL <= 0). No stop loss, no trade rule enforced."
            print(f"[{self.account_id}] [BLOCKED] {err}")
            self._set_diag("BLOCKED_MISSING_SL", err, {"symbol": symbol})
            try:
                from trade_logger import log_error
                log_error("MISSING_STOP_LOSS", err, provider_id=provider_id, symbol=symbol, raw_message=signal.get("raw_message", ""))
            except Exception:
                pass
            return []

        if not sym_info.visible:
            mt5.symbol_select(symbol, True)

        tick = mt5.symbol_info_tick(symbol)
        if not tick:
            err = f"Could not retrieve live price tick for {symbol} in MT5"
            print(f"[{self.account_id}] {err}")
            self._set_diag("ERROR_NO_TICK", err, {"symbol": symbol})
            return []

        digits = sym_info.digits

        # Apply shared direction/SL/hundreds-digit auto-fix BEFORE sanity guards
        try:
            from signal_parser import normalize_and_autofix_signal
            signal = normalize_and_autofix_signal(signal, tick.bid, tick.ask, digits)
            direction = signal.get("direction", direction)
            entry_price = float(signal.get("entry") or 0)
            sl_price = float(signal.get("sl") or 0)
        except Exception as _af_e:
            print(f"[{self.account_id}] [WARN] normalize_and_autofix_signal error: {_af_e}")

        live_price = tick.ask if direction == "BUY" else tick.bid
        if not live_price or live_price <= 0:
            print(f"[{self.account_id}] [BLOCKED] Invalid live price {live_price} for {symbol}.")
            return []

        # Rule 1: Price Consistency Sanity Guard (prevent mismatched prices executing on wrong symbol)
        if sl_price and sl_price > 0:
            sl_pct_diff = abs(live_price - sl_price) / live_price
            if sl_pct_diff > 0.20:
                err = f"Signal SL {sl_price} deviates {sl_pct_diff*100:.1f}% from live market price {live_price:.2f} for {symbol}. Price/symbol mismatch! Aborting."
                print(f"[{self.account_id}] [BLOCKED] {err}")
                self._set_diag("BLOCKED_PRICE_MISMATCH", err, {"symbol": symbol, "sl": sl_price, "live_price": live_price})
                try:
                    from trade_logger import log_error
                    log_error("PRICE_MISMATCH", err, provider_id=provider_id, symbol=symbol, raw_message=signal.get("raw_message", ""))
                except Exception:
                    pass
                return []
        if entry_price and entry_price > 0:
            ent_pct_diff = abs(live_price - entry_price) / live_price
            if ent_pct_diff > 0.20:
                err = f"Signal Entry {entry_price} deviates {ent_pct_diff*100:.1f}% from live market price {live_price:.2f} for {symbol}. Price/symbol mismatch! Aborting."
                print(f"[{self.account_id}] [BLOCKED] {err}")
                self._set_diag("BLOCKED_PRICE_MISMATCH", err, {"symbol": symbol, "entry": entry_price, "live_price": live_price})
                try:
                    from trade_logger import log_error
                    log_error("PRICE_MISMATCH", err, provider_id=provider_id, symbol=symbol, raw_message=signal.get("raw_message", ""))
                except Exception:
                    pass
                return []

        # Entry-zone mode: when the provider gives a range, pick nearest / mid / furthest level
        try:
            e_min = signal.get("entry_min")
            e_max = signal.get("entry_max")
            if entry_price and entry_price > 0 and e_min is not None and e_max is not None:
                lo, hi = min(float(e_min), float(e_max)), max(float(e_min), float(e_max))
                if hi > lo and lo - 0.01 <= entry_price <= hi + 0.01:
                    zmode = str(settings.get("entry_zone_mode", "mid")).lower()
                    if zmode == "nearest":
                        entry_price = lo if direction == "SELL" else hi
                    elif zmode == "furthest":
                        entry_price = hi if direction == "SELL" else lo
                    entry_price = round(entry_price, digits)
                    signal["entry"] = entry_price
                    print(f"[{self.account_id}] [ENTRY-ZONE] Zone {lo}-{hi}, mode={zmode} -> entry {entry_price}")
        except Exception as e:
            print(f"[{self.account_id}] [WARN] Entry-zone resolution failed: {e}")

        # Support both 'tp' list from parser and individual tp1, tp2, tp3 keys
        tp_list = signal.get("tp") if isinstance(signal.get("tp"), list) else []
        tp1 = signal.get("tp1") or (tp_list[0] if len(tp_list) > 0 else None)
        tp2 = signal.get("tp2") or (tp_list[1] if len(tp_list) > 1 else None)
        tp3 = signal.get("tp3") or (tp_list[2] if len(tp_list) > 2 else None)

        # Part B: Sanity check to prevent placing an identical trade (matching Symbol, Direction, Entry, SL, and TP levels)
        norm_entry = round(float(entry_price or 0), digits)
        norm_sl = round(float(sl_price or 0), digits)
        norm_tp1 = round(float(tp1 or 0), digits)
        norm_tp2 = round(float(tp2 or 0), digits)
        exec_fp = (str(provider_id), symbol, direction, norm_entry, norm_sl, norm_tp1, norm_tp2)

        for grp in list(self.active_trade_groups.values()):
            if grp.get("status") not in ("CLOSED", "CANCELLED"):
                g_sym = grp.get("symbol")
                g_dir = grp.get("direction")
                g_ent = round(float(grp.get("signal_entry", grp.get("entry") or 0)), digits)
                g_sl = round(float(grp.get("sl_price") or 0), digits)
                g_tp1 = round(float(grp.get("tp1_price") or 0), digits)
                g_tp2 = round(float(grp.get("tp2_price") or 0), digits)
                entry_matches = (norm_entry > 0 and abs(g_ent - norm_entry) <= 2 * sym_info.point) or (norm_entry == 0 and g_sl == norm_sl and g_tp1 == norm_tp1)
                if g_sym == symbol and g_dir == direction and entry_matches and g_sl == norm_sl and g_tp1 == norm_tp1 and g_tp2 == norm_tp2:
                    err_msg = f"Duplicate trade blocked: active group already has identical levels ({symbol} {direction} Entry={norm_entry}, SL={norm_sl}, TP1={norm_tp1})."
                    print(f"[{self.account_id}] [SANITY-GUARD] {err_msg}")
                    self._set_diag("BLOCKED_DUPLICATE", err_msg)
                    return []

        now_ts = time.time()
        if exec_fp in self.recent_exec_fingerprints and (now_ts - self.recent_exec_fingerprints[exec_fp] < 1800):
            err_msg = f"Duplicate trade blocked: identical signal ({symbol} {direction} Entry={norm_entry}, SL={norm_sl}, TP1={norm_tp1}) was executed recently."
            print(f"[{self.account_id}] [SANITY-GUARD] {err_msg}")
            self._set_diag("BLOCKED_DUPLICATE", err_msg)
            return []

        # Spread compensation parameters & Unified Order Type / Price calculation (computed BEFORE risk/lot sizing)
        spread_offset_entry = bool(settings.get("spread_offset_entry", True))
        spread_offset_tp = bool(settings.get("spread_offset_tp", True))
        spread_offset_sl = bool(settings.get("spread_offset_sl", True))
        spread_val = (tick.ask - tick.bid) if tick else ((sym_info.spread * sym_info.point) if sym_info else 0.0)

        eff_entry = entry_price
        if entry_price and entry_price > 0 and spread_offset_entry and spread_val > 0:
            if direction == "BUY":
                eff_entry = entry_price + spread_val

        eff_sl = sl_price
        if sl_price and sl_price > 0 and spread_offset_sl and spread_val > 0:
            if direction == "SELL":
                eff_sl = sl_price + spread_val

        min_stop_dist = max((getattr(sym_info, 'trade_stops_level', 0) if sym_info else 0) * sym_info.point, spread_val + 5 * sym_info.point)
        current_price = tick.ask if direction == "BUY" else tick.bid
        threshold = 50 * sym_info.point
        e_min = signal.get("entry_min")
        e_max = signal.get("entry_max")

        # Collect adjusted TPs for unified stop checks
        adj_tps = []
        for tp_val in [tp1, tp2, tp3]:
            if tp_val and tp_val > 0:
                cand_tp = tp_val
                if spread_offset_tp and spread_val > 0 and direction == "SELL":
                    cand_tp = tp_val + spread_val
                    if eff_entry and eff_entry > 0 and cand_tp >= eff_entry:
                        cand_tp = tp_val
                adj_tps.append(round(cand_tp, digits))

        # Unified order type decision across ALL tiers for the entire trade
        if direction == "BUY":
            if eff_entry and eff_entry > 0:
                if e_min is not None and e_max is not None:
                    z_lo, z_hi = min(float(e_min), float(e_max)), max(float(e_min), float(e_max))
                    in_entry_zone = (z_lo - threshold) <= current_price <= (z_hi + threshold)
                else:
                    in_entry_zone = abs(current_price - eff_entry) <= threshold
            else:
                in_entry_zone = True

            tp_violated = any(t > 0 and t <= current_price + min_stop_dist for t in adj_tps)
            sl_violated = (eff_sl and eff_sl > 0 and eff_sl >= current_price - min_stop_dist)

            if sig_order_type not in ("LIMIT", "STOP") and in_entry_zone and not tp_violated and not sl_violated:
                unified_mt5_type = mt5.ORDER_TYPE_BUY
                unified_price = current_price
            elif eff_entry and eff_entry > 0:
                if eff_entry < current_price:
                    unified_mt5_type = mt5.ORDER_TYPE_BUY_LIMIT
                    unified_price = eff_entry
                    if unified_price >= current_price - min_stop_dist:
                        unified_price = round(current_price - min_stop_dist - sym_info.point, digits)
                else:
                    unified_mt5_type = mt5.ORDER_TYPE_BUY_STOP
                    unified_price = eff_entry
                    if unified_price <= current_price + min_stop_dist:
                        unified_price = round(current_price + min_stop_dist + sym_info.point, digits)
            else:
                unified_mt5_type = mt5.ORDER_TYPE_BUY
                unified_price = current_price

        elif direction == "SELL":
            if eff_entry and eff_entry > 0:
                if e_min is not None and e_max is not None:
                    z_lo, z_hi = min(float(e_min), float(e_max)), max(float(e_min), float(e_max))
                    in_entry_zone = (z_lo - threshold) <= current_price <= (z_hi + threshold)
                else:
                    in_entry_zone = abs(current_price - eff_entry) <= threshold
            else:
                in_entry_zone = True

            tp_violated = any(t > 0 and t >= current_price - min_stop_dist for t in adj_tps)
            sl_violated = (eff_sl and eff_sl > 0 and eff_sl <= current_price + min_stop_dist)

            if sig_order_type not in ("LIMIT", "STOP") and in_entry_zone and not tp_violated and not sl_violated:
                unified_mt5_type = mt5.ORDER_TYPE_SELL
                unified_price = current_price
            elif eff_entry and eff_entry > 0:
                if eff_entry > current_price:
                    unified_mt5_type = mt5.ORDER_TYPE_SELL_LIMIT
                    unified_price = eff_entry
                    if unified_price <= current_price + min_stop_dist:
                        unified_price = round(current_price + min_stop_dist + sym_info.point, digits)
                else:
                    unified_mt5_type = mt5.ORDER_TYPE_SELL_STOP
                    unified_price = eff_entry
                    if unified_price >= current_price - min_stop_dist:
                        unified_price = round(current_price - min_stop_dist - sym_info.point, digits)
            else:
                unified_mt5_type = mt5.ORDER_TYPE_SELL
                unified_price = current_price
        else:
            return []

        # Risk / lot sizing calculation using actual planned order price (unified_price)
        acc_info = mt5.account_info()
        balance = getattr(acc_info, "balance", 1000.0)
        risk_mode = settings.get("risk_mode", "percent")
        risk_val = float(settings.get("risk_value", 1.0))

        step = sym_info.volume_step if sym_info.volume_step > 0 else 0.01
        min_lot = sym_info.volume_min if sym_info.volume_min > 0 else 0.01
        pt_val = getattr(sym_info, "trade_tick_value", 1.0)
        pt_size = getattr(sym_info, "trade_tick_size", sym_info.point)
        if pt_val <= 0:
            pt_val = 1.0
        if pt_size <= 0:
            pt_size = sym_info.point if sym_info.point > 0 else 0.01

        ref_p = unified_price if unified_price and unified_price > 0 else (entry_price if entry_price and entry_price > 0 else live_price)

        if risk_mode == "fixed":
            total_lot = max(min_lot, round(risk_val / step) * step)
        else:
            # Risk percent
            risk_amt = balance * (risk_val / 100.0)
            if sl_price and ref_p and abs(ref_p - sl_price) > 0:
                dist = abs(ref_p - sl_price)
                pts = dist / pt_size if pt_size > 0 else dist
                lot_calc = risk_amt / (pts * pt_val) if (pts * pt_val) > 0 else min_lot
                total_lot = max(min_lot, min(round(lot_calc / step) * step, sym_info.volume_max))
            else:
                total_lot = max(min_lot, 0.03)

        # Safety Rule 2: Maximum Lot Cap per Provider (default: 10.0 lots)
        max_lot = float(settings.get("max_lot", 10.0))
        if total_lot > max_lot:
            print(f"[{self.account_id}] [RISK-CAP] Total lot {total_lot:.2f} exceeded max lot limit {max_lot:.2f} for Provider {provider_id}. Clamping to {max_lot:.2f} lots.")
            total_lot = max_lot

        # Rule 2: Pre-flight Real Dollar Risk Verification for total planned position
        if risk_mode == "percent" and sl_price and sl_price > 0:
            allowed_budget = balance * (risk_val / 100.0)
            max_allowed_budget = allowed_budget * 1.25  # 25% tolerance buffer for spread/rounding
            est_dist = abs(ref_p - sl_price)
            est_pts = est_dist / pt_size if pt_size > 0 else est_dist
            total_dollar_risk = est_pts * pt_val * total_lot
            if total_dollar_risk > max_allowed_budget:
                err_msg = f"Total trade real risk ${total_dollar_risk:.2f} exceeds allocated budget ${allowed_budget:.2f} (max: ${max_allowed_budget:.2f}) at ref={ref_p}, sl={sl_price}."
                print(f"[{self.account_id}] [RISK-GUARD BLOCKED] {err_msg}")
                self._set_diag("BLOCKED_RISK_GUARD", err_msg, {"balance": balance, "risk_dollar": total_dollar_risk, "max_budget": max_allowed_budget, "total_lot": total_lot})
                return []

        # Split across TPs
        tp1_pct = float(settings.get("tp1_percent", 33.3))
        tp2_pct = float(settings.get("tp2_percent", 33.3))
        tp3_pct = float(settings.get("tp3_percent", 33.4))
        total_p = (tp1_pct + tp2_pct + tp3_pct) if (tp1_pct + tp2_pct + tp3_pct) > 0 else 100.0

        lot1 = round(max(min_lot, round(((total_lot * (tp1_pct / total_p))) / step) * step), 2)
        lot2 = round(max(min_lot, round(((total_lot * (tp2_pct / total_p))) / step) * step), 2)
        lot3 = round(max(min_lot, round(((total_lot * (tp3_pct / total_p))) / step) * step), 2)

        try:
            magic = int(settings.get("magic_number") or (self.magic_base + int(provider_id)))
        except Exception:
            magic = self.magic_base + (int(provider_id) if str(provider_id).isdigit() else 0)
        results = []
        placed_tickets = []
        tier_errors = []

        tiers = [(lot1, tp1, "TP1"), (lot2, tp2, "TP2"), (lot3, tp3, "TP3")]
        active_tiers = [(v, t, l) for v, t, l in tiers if t]
        if risk_mode == "percent" and sl_price and sl_price > 0 and len(active_tiers) > 1:
            allowed_budget = balance * (risk_val / 100.0)
            max_allowed_budget = allowed_budget * 1.25
            est_dist = abs(ref_p - sl_price)
            est_pts = est_dist / pt_size if pt_size > 0 else est_dist
            while len(active_tiers) > 1 and round(sum(v for v, _, _ in active_tiers), 4) > round(total_lot, 4) and (est_pts * pt_val * sum(v for v, _, _ in active_tiers)) > max_allowed_budget:
                active_tiers.pop()
        tiers = active_tiers

        for vol, tp_val, lbl in tiers:
            if not tp_val:
                continue

            eff_tp = tp_val
            if tp_val and tp_val > 0 and spread_offset_tp and spread_val > 0 and direction == "SELL":
                cand_tp = tp_val + spread_val
                if eff_entry and eff_entry > 0 and cand_tp >= eff_entry:
                    cand_tp = tp_val
                eff_tp = cand_tp

            mt5_type = unified_mt5_type
            order_price = unified_price

            # Option 2 Execution Guard: Prevent absurd TPs (>12% away from order price) from executing on MT5
            if eff_tp and eff_tp > 0 and order_price > 0:
                tp_dist = abs(order_price - eff_tp)
                if (tp_dist / order_price) > 0.12:
                    print(f"[{self.account_id}] [TP-GUARD BLOCKED] TP {eff_tp} is {tp_dist:.1f} points ({tp_dist/order_price*100:.1f}%) away from order price {order_price}. Clamped to 0 (Open TP) for safety.")
                    eff_tp = 0.0

            # Validate SL side and Rule 2: Per-Tier Real-World Dollar Risk Verification
            if eff_sl and eff_sl > 0:
                if direction == "SELL" and eff_sl <= order_price:
                    print(f"[{self.account_id}] [RISK-GUARD BLOCKED] Stop loss {eff_sl} is below/equal to SELL order price {order_price}. Aborting tier {lbl}!")
                    continue
                elif direction == "BUY" and eff_sl >= order_price:
                    print(f"[{self.account_id}] [RISK-GUARD BLOCKED] Stop loss {eff_sl} is above/equal to BUY order price {order_price}. Aborting tier {lbl}!")
                    continue

                tier_dist = abs(order_price - eff_sl)
                tier_pts = tier_dist / pt_size if pt_size > 0 else tier_dist
                tier_real_risk = tier_pts * pt_val * vol
                if risk_mode == "percent":
                    allowed_budget = balance * (risk_val / 100.0)
                    max_allowed_budget = allowed_budget * 1.25
                    if tier_real_risk > max_allowed_budget:
                        print(f"[{self.account_id}] [RISK-GUARD BLOCKED] Tier {lbl} real risk ${tier_real_risk:.2f} exceeds allocated budget ${allowed_budget:.2f} (max: ${max_allowed_budget:.2f}). Order aborted!")
                        continue

            action = mt5.TRADE_ACTION_DEAL if mt5_type in (mt5.ORDER_TYPE_BUY, mt5.ORDER_TYPE_SELL) else mt5.TRADE_ACTION_PENDING

            req = {
                "action": action,
                "symbol": symbol,
                "volume": vol,
                "type": mt5_type,
                "price": round(order_price, digits),
                "sl": round(eff_sl, digits) if eff_sl else 0.0,
                "tp": round(eff_tp, digits) if eff_tp else 0.0,
                "deviation": self.deviation,
                "magic": magic,
                "comment": f"P{provider_id}_{lbl}",
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC
            }

            res = mt5.order_send(req)
            if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                placed_tickets.append(res.order)
                results.append({
                    "ticket": res.order,
                    "symbol": symbol,
                    "volume": vol,
                    "tier": lbl,
                    "action": "FILLED" if action == mt5.TRADE_ACTION_DEAL else "PENDING",
                    "price": round(order_price, digits),
                    "account_id": self.account_id,
                    "platform": "mt5",
                    "provider_id": str(provider_id)
                })
                print(f"[{self.account_id}] Order placed successfully for {lbl}: Ticket={res.order}, Vol={vol}, Price={order_price}")
            else:
                err_code = res.retcode if res else mt5.last_error()
                err_comment = getattr(res, "comment", "") if res else ""
                tier_errors.append(f"{lbl}: retcode={err_code} ({err_comment})")
                print(f"[{self.account_id}] [ERROR] Order send failed for {lbl}: retcode={err_code}, comment={err_comment}")

        if placed_tickets:
            self.recent_exec_fingerprints[exec_fp] = time.time()
            self.trade_counter += 1
            if message_id:
                mid = int(message_id)
                existing_tkts = self.message_to_tickets.get(mid, [])
                self.message_to_tickets[mid] = existing_tkts + [t for t in placed_tickets if t not in existing_tkts]

            self.active_trade_groups[self.trade_counter] = {
                "message_id": mid if message_id else 0,
                "symbol": symbol,
                "provider_id": str(provider_id),
                "direction": direction,
                "entry": round(order_price, digits) if 'order_price' in locals() else current_price,
                "signal_entry": norm_entry,
                "sl_price": sl_price,
                "tp1_price": tp1,
                "tp2_price": tp2,
                "tp3_price": tp3,
                "tp1_ticket": placed_tickets[0] if len(placed_tickets) > 0 else None,
                "tp2_ticket": placed_tickets[1] if len(placed_tickets) > 1 else None,
                "tp3_ticket": placed_tickets[2] if len(placed_tickets) > 2 else None,
                "settings": settings,
                "created_at": int(time.time()),
                "status": "OPEN"
            }
            self.save_state()
            try:
                self.sync_closed_trades_to_db()
            except Exception:
                pass
            self._set_diag("COPIED", f"Placed {len(placed_tickets)} order(s) on {symbol}.", {"tickets": placed_tickets, "symbol": symbol, "tier_errors": tier_errors})
        else:
            self._set_diag("ORDER_SEND_FAILED", "; ".join(tier_errors) if tier_errors else f"All order tiers aborted/failed for {symbol}.", {"symbol": symbol, "tier_errors": tier_errors})

        return results

    def _get_allowed_magics_for_pids(self, pids) -> set:
        magics = set()
        p_settings = self.config.get("provider_settings") or {}
        for p in pids:
            p_str = str(p).strip()
            if p_str.isdigit():
                p_int = int(p_str)
                magics.add(self.magic_base + p_int)
                magics.add(400000 + p_int)
                magics.add(400400 + p_int)
            cfg_m = (p_settings.get(p_str) or {}).get("magic_number")
            if cfg_m:
                try:
                    magics.add(int(cfg_m))
                except Exception:
                    pass
        return magics

    def _matches_provider(self, pos_or_order, target_pids) -> bool:
        if not target_pids:
            return True
        target_set = {str(p).strip() for p in target_pids}
        allowed_magics = self._get_allowed_magics_for_pids(target_set)
        if getattr(pos_or_order, "magic", None) in allowed_magics:
            return True
        comm = str(getattr(pos_or_order, "comment", "") or "")
        for p_str in target_set:
            if f"P{p_str}_" in comm or comm.upper().startswith(f"P{p_str}"):
                return True
        return False

    def close_all_positions(self, provider_id=None):
        target_pids = [str(provider_id)] if provider_id else None
        positions = mt5.positions_get()
        if not positions:
            return []
        
        results = []
        for pos in positions:
            if target_pids and not self._matches_provider(pos, target_pids):
                continue
            tick = mt5.symbol_info_tick(pos.symbol)
            if not tick:
                continue
            close_type = mt5.ORDER_TYPE_SELL if pos.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
            close_price = tick.bid if pos.type == mt5.ORDER_TYPE_BUY else tick.ask
            sym_info = mt5.symbol_info(pos.symbol)
            digits = sym_info.digits if sym_info else 2

            req = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": pos.symbol,
                "volume": pos.volume,
                "type": close_type,
                "price": round(close_price, digits),
                "deviation": self.deviation,
                "magic": pos.magic,
                "comment": "Sirius Close All",
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC,
                "position": pos.ticket
            }
            res = mt5.order_send(req)
            if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                results.append({"ticket": pos.ticket, "status": "CLOSED", "profit": pos.profit})
        return results

    def close_and_cancel_all(self, provider_id=None, provider_ids=None) -> dict:
        target_pids = []
        if provider_id:
            target_pids = [str(provider_id)]
        elif provider_ids:
            target_pids = [str(p) for p in provider_ids]

        closed_positions = []
        positions = mt5.positions_get()
        if positions:
            for pos in positions:
                if target_pids and not self._matches_provider(pos, target_pids):
                    continue
                tick = mt5.symbol_info_tick(pos.symbol)
                if not tick:
                    continue
                close_type = mt5.ORDER_TYPE_SELL if pos.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
                close_price = tick.bid if pos.type == mt5.ORDER_TYPE_BUY else tick.ask
                sym_info = mt5.symbol_info(pos.symbol)
                digits = sym_info.digits if sym_info else 2
                req = {
                    "action": mt5.TRADE_ACTION_DEAL,
                    "symbol": pos.symbol,
                    "volume": pos.volume,
                    "type": close_type,
                    "price": round(close_price, digits),
                    "deviation": self.deviation,
                    "magic": pos.magic,
                    "comment": "Sirius Close All",
                    "type_time": mt5.ORDER_TIME_GTC,
                    "type_filling": mt5.ORDER_FILLING_IOC,
                    "position": pos.ticket,
                }
                res = mt5.order_send(req)
                if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                    closed_positions.append(pos.ticket)

        cancelled_orders = []
        orders = mt5.orders_get()
        if orders:
            for o in orders:
                if target_pids and not self._matches_provider(o, target_pids):
                    continue
                req = {
                    "action": mt5.TRADE_ACTION_REMOVE,
                    "order": o.ticket,
                }
                res = mt5.order_send(req)
                if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                    cancelled_orders.append(o.ticket)

        return {
            "closed_positions_count": len(closed_positions),
            "cancelled_orders_count": len(cancelled_orders),
            "closed_positions": closed_positions,
            "cancelled_orders": cancelled_orders
        }

    def close_trade_by_reply(self, reply_id: int):
        tickets = self.message_to_tickets.get(int(reply_id), [])
        if not tickets:
            return False
        success = True
        for t in tickets:
            pos = mt5.positions_get(ticket=t)
            if pos:
                p = pos[0]
                tick = mt5.symbol_info_tick(p.symbol)
                close_type = mt5.ORDER_TYPE_SELL if p.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
                close_price = tick.bid if p.type == mt5.ORDER_TYPE_BUY else tick.ask
                req = {
                    "action": mt5.TRADE_ACTION_DEAL,
                    "symbol": p.symbol,
                    "volume": p.volume,
                    "type": close_type,
                    "price": close_price,
                    "position": t,
                    "magic": p.magic
                }
                res = mt5.order_send(req)
                if not (res and res.retcode == mt5.TRADE_RETCODE_DONE):
                    success = False
        return success

    def cancel_trade_by_reply(self, reply_id: int):
        tickets = self.message_to_tickets.get(int(reply_id), [])
        if not tickets:
            return False
        success = True
        for t in tickets:
            req = {"action": mt5.TRADE_ACTION_REMOVE, "order": t}
            res = mt5.order_send(req)
            if not (res and res.retcode == mt5.TRADE_RETCODE_DONE):
                success = False
        return success

    def modify_sl_by_reply(self, reply_id: int, new_sl: float):
        tickets = self.message_to_tickets.get(int(reply_id), [])
        # If not matched to reply_id directly, apply to active group positions if relevant
        if not tickets:
            for g in self.active_trade_groups.values():
                if g.get("status") == "OPEN":
                    for key in ["tp1_ticket", "tp2_ticket", "tp3_ticket"]:
                        t = g.get(key)
                        if t: tickets.append(t)
        if not tickets:
            return False
        success = True
        for t in tickets:
            pos = mt5.positions_get(ticket=t)
            if pos:
                p = pos[0]
                req = {
                    "action": mt5.TRADE_ACTION_SLTP,
                    "symbol": p.symbol,
                    "sl": new_sl,
                    "tp": p.tp,
                    "position": t
                }
                res = mt5.order_send(req)
                if not (res and res.retcode == mt5.TRADE_RETCODE_DONE):
                    success = False
        return success

    def modify_signal_levels_by_msg(self, message_id: int, new_tps: list = None, new_sl: float = 0.0, new_entry: float = 0.0, provider_id: str = None, entry_min: float = None, entry_max: float = None) -> list:
        """
        Modifies TP and SL levels on active positions and pending orders
        for the given message_id on this MT5 account.
        """
        if not message_id:
            return []
        mid = int(message_id)
        new_tps = [float(x) for x in (new_tps or []) if float(x) > 0]
        new_sl = float(new_sl or 0.0)
        new_entry = float(new_entry or 0.0)

        matched_groups = []
        for gid, g in list(self.active_trade_groups.items()):
            g_mid = g.get("message_id")
            if g_mid and int(g_mid) == mid:
                matched_groups.append((gid, g))
            elif mid in self.message_to_tickets:
                tkts = self.message_to_tickets[mid]
                if any(g.get(k) in tkts for k in ["tp1_ticket", "tp2_ticket", "tp3_ticket"]):
                    matched_groups.append((gid, g))

        tickets_fallback = self.message_to_tickets.get(mid, [])
        tier_map = [("tp1_ticket", 0, "tp1_price"), ("tp2_ticket", 1, "tp2_price"), ("tp3_ticket", 2, "tp3_price")]
        modified_results = []

        if matched_groups:
            for gid, group in matched_groups:
                direction = group.get("direction", "BUY")
                g_symbol = group.get("symbol", "XAUUSD")
                prov_settings = group.get("settings") or {}
                spread_offset_tp = prov_settings.get("spread_offset_tp", True)
                spread_offset_entry = prov_settings.get("spread_offset_entry", True)
                spread_offset_sl = prov_settings.get("spread_offset_sl", True)

                grp_tps = list(new_tps)
                grp_sl = new_sl
                grp_entry = new_entry
                try:
                    sym_info_g = mt5.symbol_info(g_symbol)
                    digits_g = sym_info_g.digits if sym_info_g else 2
                    tick_g = mt5.symbol_info_tick(g_symbol)
                    if tick_g and getattr(tick_g, "bid", 0) > 0 and getattr(tick_g, "ask", 0) > 0:
                        from signal_parser import normalize_and_autofix_signal
                        temp_sig = {
                            "direction": direction,
                            "entry": grp_entry if grp_entry > 0 else float(group.get("signal_entry") or group.get("entry") or 0.0),
                            "entry_min": entry_min,
                            "entry_max": entry_max,
                            "sl": grp_sl if grp_sl > 0 else float(group.get("sl_price") or 0.0),
                            "tp": grp_tps,
                        }
                        fixed_sig = normalize_and_autofix_signal(temp_sig, tick_g.bid, tick_g.ask, digits_g)
                        if grp_entry > 0:
                            grp_entry = float(fixed_sig.get("entry") or grp_entry)
                            e_min_f = fixed_sig.get("entry_min")
                            e_max_f = fixed_sig.get("entry_max")
                            if e_min_f is not None and e_max_f is not None:
                                lo, hi = min(float(e_min_f), float(e_max_f)), max(float(e_min_f), float(e_max_f))
                                if hi > lo and lo - 0.01 <= grp_entry <= hi + 0.01:
                                    zmode = str(prov_settings.get("entry_zone_mode", "mid")).lower()
                                    if zmode == "nearest":
                                        grp_entry = lo if direction == "SELL" else hi
                                    elif zmode == "furthest":
                                        grp_entry = hi if direction == "SELL" else lo
                                    grp_entry = round(grp_entry, digits_g)
                        if grp_sl > 0:
                            grp_sl = float(fixed_sig.get("sl") or grp_sl)
                        if grp_tps:
                            grp_tps = list(fixed_sig.get("tp") or grp_tps)
                except Exception as _me:
                    print(f"[{self.account_id}] [WARN] modify_signal_levels_by_msg autofix error: {_me}")

                for t_key, t_idx, price_key in tier_map:
                    ticket = group.get(t_key)
                    if not ticket:
                        continue

                    target_tp = grp_tps[t_idx] if t_idx < len(grp_tps) else (grp_tps[-1] if grp_tps else 0.0)

                    # Check open positions
                    pos = mt5.positions_get(ticket=ticket)
                    if pos:
                        p = pos[0]
                        sym_info = mt5.symbol_info(p.symbol)
                        digits = sym_info.digits if sym_info else 2
                        tick = mt5.symbol_info_tick(p.symbol)
                        spread_val = (tick.ask - tick.bid) if tick else ((sym_info.spread * sym_info.point) if sym_info else 0.0)

                        eff_tp = target_tp
                        if target_tp > 0 and spread_offset_tp and direction == "SELL" and spread_val > 0:
                            eff_tp = target_tp + spread_val

                        eff_sl = grp_sl
                        if grp_sl > 0 and spread_offset_sl and direction == "SELL" and spread_val > 0:
                            eff_sl = grp_sl + spread_val

                        # Option 2 execution guard
                        if eff_tp > 0 and p.price_open > 0 and abs(p.price_open - eff_tp) / p.price_open > 0.12:
                            eff_tp = 0.0

                        req = {
                            "action": mt5.TRADE_ACTION_SLTP,
                            "position": ticket,
                            "symbol": p.symbol,
                            "sl": round(eff_sl, digits) if eff_sl > 0 else p.sl,
                            "tp": round(eff_tp, digits) if eff_tp > 0 else p.tp
                        }
                        res = mt5.order_send(req)
                        if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                            group[price_key] = target_tp
                            if grp_sl > 0:
                                group["sl_price"] = grp_sl
                            modified_results.append({
                                "ticket": ticket,
                                "tier": f"TP{t_idx+1}",
                                "action": "MODIFIED_POSITION",
                                "sl": req["sl"],
                                "tp": req["tp"]
                            })
                            print(f"[{self.account_id}] [EDIT-MODIFY] Position #{ticket} updated: SL={req['sl']}, TP={req['tp']}")
                        else:
                            err_comm = getattr(res, "comment", "") if res else str(mt5.last_error())
                            print(f"[{self.account_id}] [WARN] Failed to modify Position #{ticket}: {err_comm}")

                    # Check pending orders
                    order = mt5.orders_get(ticket=ticket)
                    if order:
                        o = order[0]
                        sym_info = mt5.symbol_info(o.symbol)
                        digits = sym_info.digits if sym_info else 2
                        tick = mt5.symbol_info_tick(o.symbol)
                        spread_val = (tick.ask - tick.bid) if tick else ((sym_info.spread * sym_info.point) if sym_info else 0.0)

                        raw_price = grp_entry if (grp_entry and grp_entry > 0) else o.price_open
                        if grp_entry and grp_entry > 0 and spread_offset_entry and direction == "BUY" and spread_val > 0:
                            raw_price = grp_entry + spread_val
                        eff_price = round(raw_price, digits)

                        eff_tp = target_tp
                        if target_tp > 0 and spread_offset_tp and direction == "SELL" and spread_val > 0:
                            eff_tp = target_tp + spread_val

                        eff_sl = grp_sl
                        if grp_sl > 0 and spread_offset_sl and direction == "SELL" and spread_val > 0:
                            eff_sl = grp_sl + spread_val

                        if eff_tp > 0 and eff_price > 0 and abs(eff_price - eff_tp) / eff_price > 0.12:
                            eff_tp = 0.0

                        req = {
                            "action": mt5.TRADE_ACTION_MODIFY,
                            "order": ticket,
                            "symbol": o.symbol,
                            "price": eff_price,
                            "sl": round(eff_sl, digits) if eff_sl > 0 else o.sl,
                            "tp": round(eff_tp, digits) if eff_tp > 0 else o.tp
                        }
                        res = mt5.order_send(req)
                        if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                            group[price_key] = target_tp
                            if grp_entry > 0:
                                group["entry"] = eff_price
                                group["signal_entry"] = grp_entry
                            if grp_sl > 0:
                                group["sl_price"] = grp_sl
                            modified_results.append({
                                "ticket": ticket,
                                "tier": f"TP{t_idx+1}",
                                "action": "MODIFIED_PENDING",
                                "price": req["price"],
                                "sl": req["sl"],
                                "tp": req["tp"]
                            })
                            print(f"[{self.account_id}] [EDIT-MODIFY] Pending Order #{ticket} updated: Price={req['price']}, SL={req['sl']}, TP={req['tp']}")
                        else:
                            err_comm = getattr(res, "comment", "") if res else str(mt5.last_error())
                            print(f"[{self.account_id}] [WARN] Failed to modify Pending Order #{ticket}: {err_comm}")
        elif tickets_fallback:
            for idx, ticket in enumerate(tickets_fallback):
                target_tp = new_tps[idx] if idx < len(new_tps) else (new_tps[-1] if new_tps else 0.0)
                pos = mt5.positions_get(ticket=ticket)
                if pos:
                    p = pos[0]
                    sym_info = mt5.symbol_info(p.symbol)
                    digits = sym_info.digits if sym_info else 2
                    req = {
                        "action": mt5.TRADE_ACTION_SLTP,
                        "position": ticket,
                        "symbol": p.symbol,
                        "sl": round(new_sl, digits) if new_sl > 0 else p.sl,
                        "tp": round(target_tp, digits) if target_tp > 0 else p.tp
                    }
                    res = mt5.order_send(req)
                    if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                        modified_results.append({
                            "ticket": ticket,
                            "tier": f"TP{idx+1}",
                            "action": "MODIFIED_POSITION",
                            "sl": req["sl"],
                            "tp": req["tp"]
                        })

        self.save_state()
        return modified_results

    def set_be_for_provider(self, provider_id: str = None):
        """Moves all open positions for this provider (or latest active group) to true Break-Even ($0 PnL accounting for spread).
        If a position is in profit, moves SL to BE. If a position is in the negative (underwater), moves TP to BE
        so the trade closes automatically when price recovers to Break-Even (with a polling fallback if within stops_level).
        """
        positions = mt5.positions_get()
        if not positions:
            return False
        
        target_pids = [str(provider_id)] if provider_id else None
        success = False
        state_changed = False
        for pos in positions:
            if target_pids and not self._matches_provider(pos, target_pids):
                continue
            sym_info = mt5.symbol_info(pos.symbol)
            digits = sym_info.digits if sym_info else 2
            point = sym_info.point if (sym_info and sym_info.point) else (10 ** -digits)
            entry = pos.price_open
            tick = mt5.symbol_info_tick(pos.symbol)
            spread_val = (tick.ask - tick.bid) if tick else ((sym_info.spread * point) if sym_info else 0.0)
            be_price = round(entry + spread_val if pos.type == mt5.ORDER_TYPE_BUY else entry - spread_val, digits)
            curr_price = (tick.bid if pos.type == mt5.ORDER_TYPE_BUY else tick.ask) if tick else getattr(pos, "price_current", entry)
            is_in_profit = (pos.type == mt5.ORDER_TYPE_BUY and curr_price > be_price) or (pos.type == mt5.ORDER_TYPE_SELL and curr_price < be_price)

            for grp in self.active_trade_groups.values():
                grp_tkts = [grp.get("tp1_ticket"), grp.get("tp2_ticket"), grp.get("tp3_ticket")]
                if pos.ticket in grp_tkts:
                    grp.setdefault("be_targets", {})[str(pos.ticket)] = {
                        "price": be_price,
                        "mode": "SL" if is_in_profit else "TP",
                    }
                    if not is_in_profit:
                        grp["be_recovery"] = True
                    state_changed = True
                    success = True

            if is_in_profit:
                if pos.sl == 0 or (pos.type == mt5.ORDER_TYPE_BUY and be_price > pos.sl) or (pos.type == mt5.ORDER_TYPE_SELL and be_price < pos.sl):
                    req = {
                        "action": mt5.TRADE_ACTION_SLTP,
                        "symbol": pos.symbol,
                        "sl": round(be_price, digits),
                        "tp": pos.tp,
                        "position": pos.ticket
                    }
                    res = mt5.order_send(req)
                    if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                        success = True
            else:
                if (pos.type == mt5.ORDER_TYPE_BUY and curr_price >= be_price) or (pos.type == mt5.ORDER_TYPE_SELL and curr_price <= be_price):
                    close_type = mt5.ORDER_TYPE_SELL if pos.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
                    req = {
                        "action": mt5.TRADE_ACTION_DEAL,
                        "symbol": pos.symbol,
                        "volume": pos.volume,
                        "type": close_type,
                        "price": round(curr_price, digits),
                        "deviation": self.deviation,
                        "magic": pos.magic,
                        "comment": "Sirius BE Close",
                        "type_time": mt5.ORDER_TIME_GTC,
                        "type_filling": mt5.ORDER_FILLING_IOC,
                        "position": pos.ticket,
                    }
                    res = mt5.order_send(req)
                    if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                        success = True
                else:
                    req = {
                        "action": mt5.TRADE_ACTION_SLTP,
                        "symbol": pos.symbol,
                        "sl": pos.sl,
                        "tp": round(be_price, digits),
                        "position": pos.ticket
                    }
                    res = mt5.order_send(req)
                    if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                        success = True
        if state_changed:
            self.save_state()
        return success

    def manage_trailing_stops(self):
        positions = mt5.positions_get()
        if positions is None:
            return
        orders = mt5.orders_get()
        open_tickets = {p.ticket: p for p in positions}
        pending_tickets = {o.ticket: o for o in (orders or [])}

        state_changed = False
        for group_id, group in list(self.active_trade_groups.items()):
            if group.get("status") == "CLOSED":
                continue

            symbol = group["symbol"]
            sym_info = mt5.symbol_info(symbol)
            if not sym_info:
                continue

            digits = sym_info.digits
            tp1_ticket = group.get("tp1_ticket")
            tp2_ticket = group.get("tp2_ticket")
            tp3_ticket = group.get("tp3_ticket")

            tp1_open = tp1_ticket in open_tickets if tp1_ticket else False
            tp2_open = tp2_ticket in open_tickets if tp2_ticket else False
            tp3_open = tp3_ticket in open_tickets if tp3_ticket else False

            tp1_pending = tp1_ticket in pending_tickets if tp1_ticket else False
            tp2_pending = tp2_ticket in pending_tickets if tp2_ticket else False
            tp3_pending = tp3_ticket in pending_tickets if tp3_ticket else False

            if not (tp1_open or tp2_open or tp3_open or tp1_pending or tp2_pending or tp3_pending):
                had_filled_deal = False
                for _tkt in [tp1_ticket, tp2_ticket, tp3_ticket]:
                    if not _tkt:
                        continue
                    try:
                        _deals = mt5.history_deals_get(position=int(_tkt))
                        if not _deals:
                            _deals = mt5.history_deals_get(ticket=int(_tkt))
                        if _deals and len(_deals) > 0:
                            had_filled_deal = True
                            break
                    except Exception:
                        pass
                if not had_filled_deal:
                    group["status"] = "CANCELLED"
                    mid_val = group.get("message_id")
                    if mid_val and int(mid_val) in self.message_to_tickets:
                        grp_tkts = {t for t in [tp1_ticket, tp2_ticket, tp3_ticket] if t}
                        rem_tkts = [t for t in self.message_to_tickets.get(int(mid_val), []) if t not in grp_tkts]
                        if rem_tkts:
                            self.message_to_tickets[int(mid_val)] = rem_tkts
                        else:
                            self.message_to_tickets.pop(int(mid_val), None)
                else:
                    group["status"] = "CLOSED"
                state_changed = True
                continue

            # Enforce any pending BE targets (underwater BE recovery via TP or stops_level fallback)
            be_targets = group.get("be_targets") or {}
            if be_targets:
                tick_be = mt5.symbol_info_tick(symbol)
                point_be = sym_info.point if (sym_info and sym_info.point) else (10 ** -digits)
                min_stop_be = max((sym_info.trade_stops_level if sym_info else 0) * point_be, 2 * point_be)
                for _tkt in [tp1_ticket, tp2_ticket, tp3_ticket]:
                    if not _tkt or _tkt not in open_tickets:
                        continue
                    t_cfg = be_targets.get(str(_tkt))
                    if not t_cfg:
                        continue
                    pos_be = open_tickets[_tkt]
                    be_p = round(float(t_cfg.get("price") or 0.0), digits)
                    be_mode = t_cfg.get("mode", "TP")
                    if be_p <= 0:
                        continue
                    curr_p = (tick_be.bid if pos_be.type == mt5.ORDER_TYPE_BUY else tick_be.ask) if tick_be else getattr(pos_be, "price_current", pos_be.price_open)
                    if be_mode == "TP":
                        if (pos_be.type == mt5.ORDER_TYPE_BUY and curr_p >= be_p) or (pos_be.type == mt5.ORDER_TYPE_SELL and curr_p <= be_p):
                            close_type = mt5.ORDER_TYPE_SELL if pos_be.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
                            mt5.order_send({
                                "action": mt5.TRADE_ACTION_DEAL,
                                "symbol": symbol,
                                "volume": pos_be.volume,
                                "type": close_type,
                                "price": round(curr_p, digits),
                                "deviation": self.deviation,
                                "magic": pos_be.magic,
                                "comment": "Sirius BE Close",
                                "type_time": mt5.ORDER_TIME_GTC,
                                "type_filling": mt5.ORDER_FILLING_IOC,
                                "position": pos_be.ticket,
                            })
                        elif abs((pos_be.tp or 0.0) - be_p) > point_be * 0.5:
                            if (pos_be.type == mt5.ORDER_TYPE_BUY and be_p >= curr_p + min_stop_be) or (pos_be.type == mt5.ORDER_TYPE_SELL and be_p <= curr_p - min_stop_be):
                                mt5.order_send({
                                    "action": mt5.TRADE_ACTION_SLTP,
                                    "position": pos_be.ticket,
                                    "symbol": symbol,
                                    "sl": pos_be.sl,
                                    "tp": be_p,
                                })
                    elif be_mode == "SL":
                        if (pos_be.type == mt5.ORDER_TYPE_BUY and curr_p <= be_p) or (pos_be.type == mt5.ORDER_TYPE_SELL and curr_p >= be_p):
                            close_type = mt5.ORDER_TYPE_SELL if pos_be.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
                            mt5.order_send({
                                "action": mt5.TRADE_ACTION_DEAL,
                                "symbol": symbol,
                                "volume": pos_be.volume,
                                "type": close_type,
                                "price": round(curr_p, digits),
                                "deviation": self.deviation,
                                "magic": pos_be.magic,
                                "comment": "Sirius BE Close",
                                "type_time": mt5.ORDER_TIME_GTC,
                                "type_filling": mt5.ORDER_FILLING_IOC,
                                "position": pos_be.ticket,
                            })
                        elif pos_be.sl == 0 or (pos_be.type == mt5.ORDER_TYPE_BUY and be_p > pos_be.sl + point_be * 0.5) or (pos_be.type == mt5.ORDER_TYPE_SELL and be_p < pos_be.sl - point_be * 0.5):
                            if (pos_be.type == mt5.ORDER_TYPE_BUY and be_p <= curr_p - min_stop_be) or (pos_be.type == mt5.ORDER_TYPE_SELL and be_p >= curr_p + min_stop_be):
                                mt5.order_send({
                                    "action": mt5.TRADE_ACTION_SLTP,
                                    "position": pos_be.ticket,
                                    "symbol": symbol,
                                    "sl": be_p,
                                    "tp": pos_be.tp,
                                })

            # Check if TP1 closed and move remaining to Break-Even / Entry
            settings = group.get("settings", {})
            be_at = settings.get("sl_breakeven_at", "TP1")
            entry = group.get("entry") or 0.0
            tp1_is_be_tp = str(tp1_ticket) in be_targets and be_targets[str(tp1_ticket)].get("mode") == "TP"

            if not tp1_open and not tp1_pending and tp1_ticket and not group.get("tp1_processed", False) and not tp1_is_be_tp:
                group["tp1_processed"] = True
                state_changed = True
                if be_at == "TP1":
                    tick = mt5.symbol_info_tick(symbol)
                    spread_val = (tick.ask - tick.bid) if tick else ((sym_info.spread * sym_info.point) if sym_info else 0.0)
                    for t in [tp2_ticket, tp3_ticket]:
                        if t and t in open_tickets:
                            pos = open_tickets[t]
                            base_entry = pos.price_open if pos.price_open > 0 else entry
                            be_price = base_entry + spread_val if pos.type == mt5.ORDER_TYPE_BUY else base_entry - spread_val
                            if pos.sl == 0 or (pos.type == mt5.ORDER_TYPE_BUY and be_price > pos.sl) or (pos.type == mt5.ORDER_TYPE_SELL and be_price < pos.sl):
                                req = {
                                    "action": mt5.TRADE_ACTION_SLTP,
                                    "symbol": symbol,
                                    "sl": round(be_price, digits),
                                    "tp": pos.tp,
                                    "position": t
                                }
                                mt5.order_send(req)

        if state_changed:
            self.save_state()

    def sync_closed_trades_to_db(self):
        """Synchronizes MT5 deal history and trade signals for this worker account into stats.db."""
        try:
            import re
            from datetime import datetime, timedelta
            import db

            info = mt5.account_info()
            if info is None:
                return

            # Reload current account config from accounts_config.json to keep provider list fresh
            acc_pids = set(str(p).strip() for p in (self.config.get("providers") or []))
            p_settings = dict(self.config.get("provider_settings") or {})
            if os.path.exists("accounts_config.json"):
                try:
                    with open("accounts_config.json", "r", encoding="utf-8") as f:
                        all_accs = json.load(f)
                        live_cfg = all_accs.get(self.account_id, {})
                        if live_cfg:
                            self.config = live_cfg
                            self.account_config = live_cfg
                            acc_pids = set(str(p).strip() for p in (live_cfg.get("providers") or []))
                            p_settings = dict(live_cfg.get("provider_settings") or {})
                except Exception:
                    pass

            magic_to_pid = {}
            for p_str in acc_pids:
                if p_str.isdigit():
                    p_int = int(p_str)
                    magic_to_pid[self.magic_base + p_int] = p_str
                    magic_to_pid[400000 + p_int] = p_str
                    magic_to_pid[400400 + p_int] = p_str
                cfg_m = (p_settings.get(p_str) or {}).get("magic_number")
                if cfg_m:
                    try:
                        magic_to_pid[int(cfg_m)] = p_str
                    except Exception:
                        pass

            reset_ts = int(float(db.get_metadata(f"reset_time_{self.account_id}") or 0))
            start_dt = datetime.fromtimestamp(reset_ts) if reset_ts > 0 else datetime(2020, 1, 1)
            deals = mt5.history_deals_get(start_dt, datetime.now() + timedelta(days=2))
            if not deals:
                return

            by_pos = {}
            for d in deals:
                if getattr(d, "type", -1) not in (0, 1):
                    continue
                pos_id = getattr(d, "position_id", 0)
                if not pos_id:
                    continue
                by_pos.setdefault(pos_id, {"in": [], "out": []})
                if getattr(d, "entry", -1) == 0:
                    by_pos[pos_id]["in"].append(d)
                elif getattr(d, "entry", -1) in (1, 2, 3):
                    by_pos[pos_id]["out"].append(d)

            open_positions = mt5.positions_get() or []
            open_pos_ids = {p.ticket for p in open_positions}

            def _resolve_pid_and_tier(entry_deal):
                comm = str(getattr(entry_deal, "comment", "") or "")
                magic = int(getattr(entry_deal, "magic", 0) or 0)
                matched_pid = None
                tp_lvl = 1

                m_comm = re.search(r"P(\d+)", comm, re.IGNORECASE)
                if m_comm:
                    cand = m_comm.group(1)
                    if not acc_pids or cand in acc_pids:
                        matched_pid = cand
                if not matched_pid and magic in magic_to_pid:
                    matched_pid = magic_to_pid[magic]
                if not matched_pid and 400000 < magic < 500000:
                    cand = str(magic - 400400 if magic > 400400 else magic - 400000)
                    if not acc_pids or cand in acc_pids:
                        matched_pid = cand

                if "TP2" in comm.upper():
                    tp_lvl = 2
                elif "TP3" in comm.upper():
                    tp_lvl = 3
                return matched_pid, tp_lvl

            def _is_deal_tp(cl_deal):
                if not cl_deal:
                    return False
                comm = str(getattr(cl_deal, "comment", "") or "").lower()
                if "[tp" in comm:
                    return True
                if "[sl" in comm or "[so" in comm:
                    return False
                reason = getattr(cl_deal, "reason", None)
                if reason == 5:
                    return True
                if reason in (4, 6):
                    return False
                return False

            conn = db.get_db()
            existing_rows = conn.execute(
                "SELECT ticket, close_reason, COALESCE(sl, 0.0) as sl, COALESCE(tp, 0.0) as tp FROM trades WHERE account_id = ?",
                (self.account_id,)
            ).fetchall()
            existing_tickets = {int(r["ticket"]): (r["close_reason"], float(r["sl"] or 0.0), float(r["tp"] or 0.0)) for r in existing_rows}
            conn.close()

            orders = mt5.history_orders_get(start_dt, datetime.now() + timedelta(days=2)) or []
            orders_by_pos = {}
            for o in orders:
                opid = int(getattr(o, "position_id", 0) or getattr(o, "ticket", 0) or 0)
                if opid:
                    orders_by_pos.setdefault(opid, []).append(o)

            all_pos_records = []
            new_trades_saved = 0

            for pos_id, grp in by_pos.items():
                if not grp["in"]:
                    continue
                entry = sorted(grp["in"], key=lambda x: (x.time, x.ticket))[0]
                matched_pid, tp_lvl = _resolve_pid_and_tier(entry)
                if not matched_pid:
                    continue

                in_vol = sum(float(x.volume) for x in grp["in"])
                out_vol = sum(float(x.volume) for x in grp["out"])
                is_closed = (in_vol > 0 and out_vol + 1e-9 >= in_vol and pos_id not in open_pos_ids and len(grp["out"]) > 0)

                direction = "BUY" if entry.type == 0 else "SELL"
                open_time = int(entry.time)
                open_price = sum(float(x.price) * float(x.volume) for x in grp["in"]) / in_vol if in_vol > 0 else float(entry.price)
                magic_val = int(entry.magic) if entry.magic else (self.magic_base + int(matched_pid) if matched_pid.isdigit() else self.magic_base)

                close_deal = None
                close_time = 0
                close_price = 0.0
                profit = 0.0
                close_reason = f"TP{tp_lvl}"

                if is_closed:
                    exits = sorted(grp["out"], key=lambda x: (x.time, x.ticket))
                    close_deal = exits[-1]
                    close_time = int(close_deal.time)
                    if reset_ts > 0 and close_time < reset_ts:
                        continue
                    close_price = sum(float(x.price) * float(x.volume) for x in exits) / out_vol if out_vol > 0 else float(close_deal.price)
                    all_d = grp["in"] + exits
                    profit = round(sum(
                        float(getattr(x, "profit", 0.0)) +
                        float(getattr(x, "commission", 0.0)) +
                        float(getattr(x, "swap", 0.0)) +
                        float(getattr(x, "fee", 0.0))
                        for x in all_d
                    ), 2)

                    out_comm = str(getattr(close_deal, "comment", "") or "").lower()
                    if "sirius be close" in out_comm:
                        close_reason = "BE"
                    elif _is_deal_tp(close_deal):
                        close_reason = "BE" if abs(profit) < 0.5 else f"TP{tp_lvl}"
                    elif "[sl" in out_comm or getattr(close_deal, "reason", None) == 4:
                        close_reason = f"TP{tp_lvl}" if profit > 0 else ("BE" if abs(profit) < 0.5 else "SL")
                    elif "manual" in out_comm or "sirius close" in out_comm:
                        close_reason = "MANUAL"
                    else:
                        close_reason = "BE" if abs(profit) < 0.5 else (f"TP{tp_lvl}" if profit > 0 else "SL")

                    pos_orders = orders_by_pos.get(int(pos_id), [])
                    sl_val = next((float(o.sl) for o in pos_orders if getattr(o, "sl", 0.0) and float(o.sl) > 0), 0.0)
                    tp_val = next((float(o.tp) for o in pos_orders if getattr(o, "tp", 0.0) and float(o.tp) > 0), 0.0)

                    deal_tkt = int(close_deal.ticket)
                    ex_info = existing_tickets.get(deal_tkt)
                    needs_save = (
                        ex_info is None
                        or ex_info[0] != close_reason
                        or (ex_info[1] == 0.0 and sl_val > 0)
                        or (ex_info[2] == 0.0 and tp_val > 0)
                    )
                    if needs_save:
                        db.save_trade(
                            ticket=deal_tkt,
                            provider_id=matched_pid,
                            symbol=entry.symbol,
                            trade_type=direction,
                            lots=round(in_vol, 2),
                            open_time=open_time,
                            close_time=close_time,
                            open_price=round(open_price, 5),
                            close_price=round(close_price, 5),
                            profit=profit,
                            magic=magic_val,
                            close_reason=close_reason,
                            account_id=self.account_id,
                            sl=round(sl_val, 5),
                            tp=round(tp_val, 5)
                        )
                        existing_tickets[deal_tkt] = (close_reason, sl_val, tp_val)
                        new_trades_saved += 1

                all_pos_records.append({
                    "pos_id": pos_id,
                    "provider_id": matched_pid,
                    "symbol": entry.symbol,
                    "direction": direction,
                    "lots": round(in_vol, 2),
                    "open_time": open_time,
                    "open_price": open_price,
                    "open_deal": entry,
                    "is_closed": is_closed,
                    "close_deal": close_deal,
                    "close_time": close_time,
                    "profit": profit,
                    "tp_lvl": tp_lvl,
                    "close_reason": close_reason,
                })

            # Group positions into originating signals by (provider_id, symbol, direction) within 60s open_time window
            all_pos_records.sort(key=lambda x: (x["open_time"], x["pos_id"]))
            clusters = []
            for rec in all_pos_records:
                placed = False
                for cl in clusters:
                    if (
                        cl["provider_id"] == rec["provider_id"]
                        and cl["symbol"] == rec["symbol"]
                        and cl["direction"] == rec["direction"]
                        and abs(rec["open_time"] - cl["created_at"]) <= 60
                    ):
                        cl["positions"].append(rec)
                        placed = True
                        break
                if not placed:
                    clusters.append({
                        "provider_id": rec["provider_id"],
                        "symbol": rec["symbol"],
                        "direction": rec["direction"],
                        "created_at": rec["open_time"],
                        "positions": [rec],
                    })

            for cl in clusters:
                closed_legs = [p for p in cl["positions"] if p["is_closed"]]
                if not closed_legs:
                    # Do not insert purely open positions into closed signal statistics yet
                    continue

                pid = cl["provider_id"]
                symbol = cl["symbol"]
                direction = cl["direction"]
                created_at = cl["created_at"]
                sig_id = f"{self.account_id}_P{pid}_{created_at}"

                tot_lots = round(sum(p["lots"] for p in cl["positions"]), 2)
                tot_profit = round(sum(p["profit"] for p in closed_legs), 2)
                closed_at = max(p["close_time"] for p in closed_legs)
                all_closed = len(closed_legs) == len(cl["positions"])
                status = "CLOSED" if all_closed else "OPEN"

                tp1_hit = any(p["tp_lvl"] == 1 and p["close_reason"] == "TP1" for p in closed_legs)
                tp2_hit = any(p["tp_lvl"] == 2 and p["close_reason"] == "TP2" for p in closed_legs)
                tp3_hit = any(p["tp_lvl"] == 3 and p["close_reason"] == "TP3" for p in closed_legs)

                sl_hit = False
                if not tp1_hit and not tp2_hit and not tp3_hit:
                    if all(p["close_reason"] == "SL" or (p["profit"] < -0.5 and p["close_reason"] != "BE") for p in closed_legs):
                        sl_hit = True

                risk_amt = 0.0
                if sl_hit and tot_profit < 0:
                    risk_amt = abs(tot_profit)
                else:
                    p_cfg = p_settings.get(str(pid)) or {}
                    r_val = float(p_cfg.get("risk_value", 1.0) or 1.0)
                    bal = float(getattr(info, "balance", 0.0) or 1000.0)
                    risk_amt = round(bal * (r_val / 100.0), 2) if p_cfg.get("risk_mode", "percent") == "percent" else 10.0

                db.save_or_update_signal(
                    signal_id=sig_id,
                    provider_id=pid,
                    symbol=symbol,
                    trade_type=direction,
                    lots=tot_lots,
                    tp1_hit=1 if tp1_hit else 0,
                    tp2_hit=1 if tp2_hit else 0,
                    tp3_hit=1 if tp3_hit else 0,
                    sl_hit=1 if sl_hit else 0,
                    profit=tot_profit,
                    status=status,
                    created_at=created_at,
                    closed_at=closed_at if all_closed else 0,
                    risk_amount=risk_amt,
                    account_id=self.account_id
                )

            if new_trades_saved > 0:
                try:
                    import provider_analytics as pa
                    pa.sync_from_trades_db(reset_ts=reset_ts)
                except Exception as _pa_e:
                    print(f"[{self.account_id}] pa.sync_from_trades_db error: {_pa_e}")
        except Exception as e:
            print(f"[{self.account_id}] sync_closed_trades_to_db error: {e}")

def run_worker_process(account_config: dict, in_q: Queue, out_q: Queue):
    worker = MT5AccountWorker(account_config, in_q, out_q)
    worker.run()
