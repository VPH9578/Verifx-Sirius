"""
mt4_worker.py — Standalone Process Worker for an MT4 Account.
Mirrors account_worker.py and ctrader_worker.py with an identical queue command
interface so Sirius Copier can run MT5, cTrader, and MT4 accounts side-by-side.
"""

import os
import time
import json
import math
import traceback
from datetime import datetime
from multiprocessing import Queue
from mt4_client import MT4Client, MT4Position, MT4SymbolInfo


class MT4AccountWorker:
    def __init__(self, account_config: dict, in_queue: Queue, out_queue: Queue):
        self.account_id = account_config["id"]
        self.account_config = account_config
        self.config = account_config
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.is_running = True

        self.state_file = f"trade_state_{self.account_id}.json"
        self.message_to_tickets = {}
        self.active_trade_groups = {}
        self.trade_counter = 0
        self.magic_base = int(account_config.get("magic_base", 400000))

        self.client: MT4Client = MT4Client(
            host=str(account_config.get("mt4_host") or "127.0.0.1"),
            port=int(account_config.get("mt4_port") or 9001),
            account_id=int(account_config.get("mt4_login") or account_config.get("mt4_account_id") or 0),
            account_str_id=self.account_id,
        )

        self.trade_state = {}
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
                with open(self.state_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.trade_state = data
                    self.message_to_tickets = {int(k): v for k, v in data.get("message_to_tickets", {}).items()}
                    self.active_trade_groups = {int(k): v for k, v in data.get("active_trade_groups", {}).items()}
                    self.trade_counter = data.get("trade_counter", 0)
            except Exception as e:
                print(f"[{self.account_id}] Failed to load MT4 trade state: {e}")

    def save_state(self):
        try:
            with open(self.state_file, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "message_to_tickets": self.message_to_tickets,
                        "active_trade_groups": self.active_trade_groups,
                        "trade_counter": self.trade_counter,
                    },
                    f,
                    indent=4,
                )
        except Exception as e:
            print(f"[{self.account_id}] Failed to save MT4 trade state: {e}")

    def connect(self) -> bool:
        ok = self.client.connect()
        if ok:
            print(f"[{self.account_id}] Connected to MT4 Account Login={self.client.trader_login} on Port {self.client.port}")
        else:
            print(f"[{self.account_id}] MT4 connection waiting: {self.client.last_error()}")
        return ok

    def run(self):
        """Worker process loop."""
        try:
            self.connect()
            last_ts_check = time.time()
            last_reconnect_attempt = time.time()

            while self.is_running:
                # Auto-reconnect every 3s if EA bridge is disconnected
                if not self.client.is_authorized and (time.time() - last_reconnect_attempt >= 3.0):
                    last_reconnect_attempt = time.time()
                    self.connect()

                while not self.in_queue.empty():
                    msg = self.in_queue.get_nowait()
                    cmd = msg.get("cmd")
                    req_id = msg.get("req_id")
                    payload = msg.get("payload", {})

                    try:
                        if cmd == "STOP":
                            self.is_running = False
                            break
                        elif cmd == "EXECUTE_SIGNAL":
                            self.last_exec_diag = {}
                            res = self.execute_signal(
                                payload.get("signal", {}),
                                payload.get("msg_id"),
                                payload.get("provider_id", "1"),
                                payload.get("settings", {}),
                            )
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
                        elif cmd == "CANCEL_PENDING_PROVIDER":
                            res = self.cancel_all_pending_for_provider(payload.get("provider_id"))
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
                            self.out_queue.put({"account_id": self.account_id, "req_id": req_id, "state": state, "result": state})
                        elif cmd == "RESET_STATE":
                            self.active_trade_groups = {}
                            self.message_to_tickets = {}
                            self.save_state()
                            self.out_queue.put({"account_id": self.account_id, "req_id": req_id, "result": True})
                        elif cmd == "SYNC_SYMBOLS":
                            sym_res = self.client.send_command({"cmd": "GET_SYMBOLS"}, timeout=6.0)
                            symbols = sym_res.get("symbols", []) if (sym_res and sym_res.get("ok")) else list(self.client.symbols_by_name.keys())
                            from symbol_mapper import SymbolMapper
                            res = SymbolMapper.build_account_symbol_map(self.account_id, symbols, platform="mt4")
                            self.out_queue.put({"account_id": self.account_id, "req_id": req_id, "result": res})
                    except Exception as cmd_err:
                        print(f"[{self.account_id}] Command '{cmd}' handler exception: {cmd_err}\n{traceback.format_exc()}")
                        self.out_queue.put({"account_id": self.account_id, "req_id": req_id, "result": [], "error": str(cmd_err)})

                now = time.time()
                if now - last_ts_check >= 2.0:
                    last_ts_check = now
                    try:
                        if self.client.is_authorized:
                            self.manage_trailing_stops()
                    except Exception:
                        pass

                time.sleep(0.04)

        except Exception as e:
            print(f"[{self.account_id}] MT4 Worker Exception: {e}\n{traceback.format_exc()}")
        finally:
            try:
                self.client.disconnect()
            except Exception:
                pass
            print(f"[{self.account_id}] MT4 Worker terminated cleanly.")

    def get_account_state(self) -> dict:
        if not self.client.is_authorized:
            return {
                "account_id": self.account_id,
                "connected": False,
                "balance": 0.0,
                "equity": 0.0,
                "login": self.account_config.get("mt4_login") or "Disconnected",
                "open_positions": [],
            }

        info, positions = self.client.get_full_state()
        pos_list = []
        for p in positions:
            pos_list.append({
                "ticket": p.ticket,
                "symbol": p.symbol,
                "type": "LONG" if p.type == 0 else "SHORT",
                "volume": p.volume,
                "price_open": p.price_open,
                "sl": p.sl,
                "tp": p.tp,
                "profit": p.profit,
                "time": p.time,
                "magic": p.magic,
                "comment": p.comment,
            })

        bal = getattr(info, "balance", 0.0) if info else 0.0
        eq = getattr(info, "equity", bal) if info else bal

        return {
            "account_id": self.account_id,
            "connected": True,
            "balance": bal,
            "equity": eq,
            "login": getattr(info, "login", self.client.trader_login) if info else self.client.trader_login,
            "server": getattr(info, "server", "MetaTrader 4") if info else "MetaTrader 4",
            "open_positions": pos_list,
        }

    def execute_signal(self, signal: dict, message_id: int, provider_id: str = "1", settings: dict = None) -> list:
        import copy
        signal = copy.deepcopy(signal) if isinstance(signal, dict) else {}
        try:
            with open("mt4_debug.log", "a", encoding="utf-8") as _df:
                _df.write(f"[{datetime.now()}] [execute_signal entry] signal={signal}, msg_id={message_id}, prov={provider_id}\n")
        except Exception:
            pass

        if message_id is not None and int(message_id) in self.message_to_tickets and self.message_to_tickets[int(message_id)]:
            mid_int = int(message_id)
            prev_tkts = self.message_to_tickets.get(mid_int, [])
            still_valid = False
            for _g in self.active_trade_groups.values():
                g_tkts = {_g.get("tp1_ticket"), _g.get("tp2_ticket"), _g.get("tp3_ticket")}
                if (_g.get("message_id") == mid_int or any(int(_t) in g_tkts for _t in prev_tkts if _t)) and _g.get("status") not in ("CLOSED", "CANCELLED"):
                    still_valid = True
                    break
            if not still_valid:
                try:
                    open_pos = {p.ticket for p in (self.client.get_open_positions() or [])}
                    pend_ord = {int(o.get("ticket", 0)) for o in (self.client.get_pending_orders() or []) if o.get("ticket")}
                    hist_deals = {int(h.get("ticket", 0)) for h in (self.client.get_history(100) or []) if h.get("ticket")}
                    for _t in prev_tkts:
                        if not _t:
                            continue
                        _ti = int(_t)
                        if _ti in open_pos or _ti in pend_ord or _ti in hist_deals:
                            still_valid = True
                            break
                except Exception:
                    still_valid = True

            if still_valid:
                msg = f"[{self.account_id}] [SANITY-GUARD] Message ID {message_id} already executed. Skipping duplicate."
                print(msg)
                self._set_diag("ALREADY_EXECUTED", msg)
                try:
                    with open("mt4_debug.log", "a", encoding="utf-8") as _df:
                        _df.write(f"{msg}\n")
                except Exception:
                    pass
                return []
            else:
                self.message_to_tickets.pop(mid_int, None)

        if not self.client.is_authorized:
            if not self.connect():
                msg = f"[{self.account_id}] Cannot execute signal: MT4 EA bridge is disconnected."
                print(msg)
                self._set_diag("DISCONNECTED", msg)
                try:
                    with open("mt4_debug.log", "a", encoding="utf-8") as _df:
                        _df.write(f"{msg}\n")
                except Exception:
                    pass
                return []

        raw_dir = signal.get("direction", "").upper().strip()
        raw_type = signal.get("order_type", "MARKET").upper().strip()

        # Determine base direction
        if "BUY" in raw_dir:
            direction = "BUY"
        elif "SELL" in raw_dir:
            direction = "SELL"
        else:
            direction = "BUY" if "BUY" in raw_type else "SELL"

        raw_symbol = signal.get("instrument") or signal.get("symbol") or ""
        symbol = self.client.resolve_symbol(raw_symbol)

        if not symbol:
            msg = f"[{self.account_id}] Symbol '{raw_symbol}' could not be resolved for MT4."
            print(msg)
            self._set_diag("BLOCKED_SYMBOL_NOT_FOUND", msg, {"raw_symbol": raw_symbol})
            try:
                with open("mt4_debug.log", "a", encoding="utf-8") as _df:
                    _df.write(f"{msg}\n")
            except Exception:
                pass
            return []

        sym_info = self.client.get_symbol_info(symbol)
        if not sym_info:
            msg = f"[{self.account_id}] Failed to get symbol info for {symbol}"
            print(msg)
            self._set_diag("BLOCKED_SYMBOL_NOT_FOUND", msg, {"symbol": symbol})
            try:
                with open("mt4_debug.log", "a", encoding="utf-8") as _df:
                    _df.write(f"{msg}\n")
            except Exception:
                pass
            return []

        digits = sym_info.digits
        tick = self.client.get_tick(symbol)
        if not tick or getattr(tick, "bid", 0.0) <= 0 or getattr(tick, "ask", 0.0) <= 0:
            alt = symbol + "x" if not symbol.endswith("x") else symbol[:-1]
            alt_tick = self.client.get_tick(alt)
            if alt_tick and getattr(alt_tick, "bid", 0.0) > 0 and getattr(alt_tick, "ask", 0.0) > 0:
                print(f"[{self.account_id}] Symbol '{symbol}' has zero bid/ask in MT4. Switched to active '{alt}'.")
                symbol = alt
                tick = alt_tick
                alt_info = self.client.get_symbol_info(symbol)
                if alt_info:
                    sym_info = alt_info
                    digits = sym_info.digits
            else:
                msg = f"[{self.account_id}] No live tick available for {symbol}"
                print(msg)
                try:
                    with open("mt4_debug.log", "a", encoding="utf-8") as _df:
                        _df.write(f"{msg}\n")
                except Exception:
                    pass
                return []

        # Apply shared direction/SL/hundreds-digit auto-fix BEFORE sanity guards
        try:
            from signal_parser import normalize_and_autofix_signal
            signal = normalize_and_autofix_signal(signal, tick.bid, tick.ask, digits)
            direction = signal.get("direction", direction)
        except Exception as _af_e:
            print(f"[{self.account_id}] [WARN] normalize_and_autofix_signal error: {_af_e}")

        curr_price = tick.ask if direction == "BUY" else tick.bid
        if curr_price <= 0:
            msg = f"[{self.account_id}] Invalid price {curr_price} for {symbol}"
            print(msg)
            try:
                with open("mt4_debug.log", "a", encoding="utf-8") as _df:
                    _df.write(f"{msg}\n")
            except Exception:
                pass
            return []

        signal_entry = float(signal.get("entry") or 0.0)
        sl_price = float(signal.get("sl") or 0.0)

        # Safety Rule 3: No Stop Loss, No Trade
        if direction != "CLOSE" and sl_price <= 0:
            err_m = f"[{self.account_id}] [BLOCKED] Signal rejected: Missing mandatory Stop Loss (SL <= 0)."
            print(err_m)
            self._set_diag("BLOCKED_MISSING_SL", err_m, {"symbol": symbol})
            return []

        # Rule 1: Price Consistency Sanity Guard
        if sl_price > 0 and abs(curr_price - sl_price) / curr_price > 0.20:
            err_m = f"[{self.account_id}] [BLOCKED] Signal SL {sl_price} deviates >20% from live market price {curr_price:.2f} for {symbol}. Aborting."
            print(err_m)
            self._set_diag("BLOCKED_PRICE_MISMATCH", err_m, {"symbol": symbol, "sl": sl_price, "live_price": curr_price})
            return []
        if signal_entry > 0 and abs(curr_price - signal_entry) / curr_price > 0.20:
            err_m = f"[{self.account_id}] [BLOCKED] Signal Entry {signal_entry} deviates >20% from live market price {curr_price:.2f} for {symbol}. Aborting."
            print(err_m)
            self._set_diag("BLOCKED_PRICE_MISMATCH", err_m, {"symbol": symbol, "entry": signal_entry, "live_price": curr_price})
            return []

        settings = settings or self.account_config.get("provider_settings", {}).get(str(provider_id)) or {}

        # Entry-zone mode resolution
        e_min = signal.get("entry_min")
        e_max = signal.get("entry_max")
        if signal_entry > 0 and e_min is not None and e_max is not None:
            try:
                lo, hi = min(float(e_min), float(e_max)), max(float(e_min), float(e_max))
                if hi > lo and lo - 0.01 <= signal_entry <= hi + 0.01:
                    zmode = str(settings.get("entry_zone_mode", "mid")).lower()
                    if zmode == "nearest":
                        signal_entry = lo if direction == "SELL" else hi
                    elif zmode == "furthest":
                        signal_entry = hi if direction == "SELL" else lo
                    signal_entry = round(signal_entry, digits)
                    signal["entry"] = signal_entry
            except Exception:
                pass

        tp_list = [float(x) for x in (signal.get("tp") or []) if float(x) > 0]
        if not tp_list:
            tp_list = [
                float(signal[k]) for k in ("tp1", "tp2", "tp3")
                if signal.get(k) is not None and float(signal.get(k) or 0) > 0
            ]
        if not tp_list:
            tp_list = [0.0]

        spread_offset_entry = bool(settings.get("spread_offset_entry", True))
        spread_offset_tp = bool(settings.get("spread_offset_tp", True))
        spread_offset_sl = bool(settings.get("spread_offset_sl", True))
        spread_val = (tick.ask - tick.bid) if tick else (sym_info.spread * sym_info.point)

        eff_entry = signal_entry
        if signal_entry > 0 and spread_offset_entry and spread_val > 0 and direction == "BUY":
            eff_entry = signal_entry + spread_val

        eff_sl = sl_price
        if sl_price > 0 and spread_offset_sl and spread_val > 0 and direction == "SELL":
            eff_sl = sl_price + spread_val

        # Minimum stop level buffer (convert points to price distance)
        raw_stop_pts = getattr(sym_info, "trade_stops_level", 0)
        if not isinstance(raw_stop_pts, (int, float)) or raw_stop_pts <= 0:
            raw_stop_pts = getattr(sym_info, "stops_level", 10)
        min_stop_pts = float(raw_stop_pts) if isinstance(raw_stop_pts, (int, float)) and raw_stop_pts > 0 else 10.0
        min_stop_dist = max(min_stop_pts * sym_info.point, spread_val + 5 * sym_info.point)
        threshold = 50 * sym_info.point

        adj_tps = []
        for tp_v in tp_list:
            if tp_v > 0:
                cand_tp = tp_v
                if spread_offset_tp and spread_val > 0 and direction == "SELL":
                    cand_tp = tp_v + spread_val
                    if eff_entry > 0 and cand_tp >= eff_entry:
                        cand_tp = tp_v
                adj_tps.append(round(cand_tp, digits))
            else:
                adj_tps.append(0.0)

        sig_order_type = "LIMIT" if "LIMIT" in raw_type or "LIMIT" in raw_dir else ("STOP" if "STOP" in raw_type or "STOP" in raw_dir else "MARKET")

        if direction == "BUY":
            if eff_entry > 0:
                if e_min is not None and e_max is not None:
                    z_lo, z_hi = min(float(e_min), float(e_max)), max(float(e_min), float(e_max))
                    in_entry_zone = (z_lo - threshold) <= curr_price <= (z_hi + threshold)
                else:
                    in_entry_zone = abs(curr_price - eff_entry) <= threshold
            else:
                in_entry_zone = True

            tp_violated = any(t > 0 and t <= curr_price + min_stop_dist for t in adj_tps)
            sl_violated = (eff_sl > 0 and eff_sl >= curr_price - min_stop_dist)

            if sig_order_type not in ("LIMIT", "STOP") and in_entry_zone and not tp_violated and not sl_violated:
                order_type = "MARKET"
                entry_price = curr_price
            elif eff_entry > 0:
                if eff_entry < curr_price:
                    order_type = "BUY_LIMIT"
                    entry_price = eff_entry
                    if entry_price >= curr_price - min_stop_dist:
                        entry_price = round(curr_price - min_stop_dist - sym_info.point, digits)
                else:
                    order_type = "BUY_STOP"
                    entry_price = eff_entry
                    if entry_price <= curr_price + min_stop_dist:
                        entry_price = round(curr_price + min_stop_dist + sym_info.point, digits)
            else:
                order_type = "MARKET"
                entry_price = curr_price
        else:
            if eff_entry > 0:
                if e_min is not None and e_max is not None:
                    z_lo, z_hi = min(float(e_min), float(e_max)), max(float(e_min), float(e_max))
                    in_entry_zone = (z_lo - threshold) <= curr_price <= (z_hi + threshold)
                else:
                    in_entry_zone = abs(curr_price - eff_entry) <= threshold
            else:
                in_entry_zone = True

            tp_violated = any(t > 0 and t >= curr_price - min_stop_dist for t in adj_tps)
            sl_violated = (eff_sl > 0 and eff_sl <= curr_price + min_stop_dist)

            if sig_order_type not in ("LIMIT", "STOP") and in_entry_zone and not tp_violated and not sl_violated:
                order_type = "MARKET"
                entry_price = curr_price
            elif eff_entry > 0:
                if eff_entry > curr_price:
                    order_type = "SELL_LIMIT"
                    entry_price = eff_entry
                    if entry_price <= curr_price + min_stop_dist:
                        entry_price = round(curr_price + min_stop_dist + sym_info.point, digits)
                else:
                    order_type = "SELL_STOP"
                    entry_price = eff_entry
                    if entry_price >= curr_price - min_stop_dist:
                        entry_price = round(curr_price - min_stop_dist - sym_info.point, digits)
            else:
                order_type = "MARKET"
                entry_price = curr_price

        entry_price = round(entry_price, digits)
        sl_price = round(eff_sl, digits) if eff_sl > 0 else 0.0
        if sl_price > 0:
            # Sanity guard: prevent reversed SL
            if direction == "BUY" and sl_price >= entry_price:
                print(f"[{self.account_id}] [RISK-GUARD BLOCKED] BUY Stop Loss {sl_price} >= Entry {entry_price}. Aborting.")
                return []
            elif direction == "SELL" and sl_price <= entry_price:
                print(f"[{self.account_id}] [RISK-GUARD BLOCKED] SELL Stop Loss {sl_price} <= Entry {entry_price}. Aborting.")
                return []

        risk_mode = settings.get("risk_mode", "percent")
        risk_value = float(settings.get("risk_value", 1.0))
        max_lot = float(settings.get("max_lot", 10.0))

        # Calculate total lots
        info = self.client.get_account_info()
        balance = getattr(info, "balance", 1000.0) if info else 1000.0
        step = sym_info.volume_step or 0.01
        min_lot = sym_info.volume_min or 0.01
        pt_val = sym_info.trade_tick_value if getattr(sym_info, "trade_tick_value", 0) > 0 else 1.0
        pt_size = sym_info.trade_tick_size if getattr(sym_info, "trade_tick_size", 0) > 0 else (sym_info.point if sym_info.point > 0 else 0.01)

        if risk_mode == "fixed":
            total_lots = max(min_lot, round(risk_value / step) * step)
        else:
            # Risk % based on SL distance from actual planned order price
            sl_distance = abs(entry_price - sl_price) if sl_price > 0 else 0.0
            if sl_distance > 0:
                risk_amount = (risk_value / 100.0) * balance
                pts = sl_distance / pt_size if pt_size > 0 else sl_distance
                loss_per_lot = pts * pt_val if (pts * pt_val) > 0 else 1.0
                total_lots = risk_amount / loss_per_lot
            else:
                total_lots = min_lot

        total_lots = max(min_lot, min(max_lot, total_lots))
        total_lots = round(round(total_lots / step) * step, 2)

        # Rule 2: Pre-flight Real Dollar Risk Verification
        if risk_mode == "percent" and sl_price > 0:
            allowed_budget = balance * (risk_value / 100.0)
            max_allowed_budget = allowed_budget * 1.25
            est_dist = abs(entry_price - sl_price)
            est_pts = est_dist / pt_size if pt_size > 0 else est_dist
            total_dollar_risk = est_pts * pt_val * total_lots
            if total_dollar_risk > max_allowed_budget:
                err_m = f"[{self.account_id}] [RISK-GUARD BLOCKED] Total trade real risk ${total_dollar_risk:.2f} exceeds allocated budget ${allowed_budget:.2f} (max: ${max_allowed_budget:.2f}) at entry={entry_price}, sl={sl_price}."
                print(err_m)
                self._set_diag("BLOCKED_RISK_GUARD", err_m, {"balance": balance, "risk_dollar": total_dollar_risk, "max_budget": max_allowed_budget})
                return []

        # TP Volume split
        tp1_p = float(settings.get("tp1_percent", 60.0))
        tp2_p = float(settings.get("tp2_percent", 30.0))
        tp3_p = float(settings.get("tp3_percent", 10.0))
        adj_tps = adj_tps[:3]
        tp_list = tp_list[:3]
        num_tps = len(adj_tps)
        if num_tps == 1:
            vol_shares = [total_lots]
        elif num_tps == 2:
            tot_p = (tp1_p + tp2_p) if (tp1_p + tp2_p) > 0 else 100.0
            vol_shares = [
                round(total_lots * (tp1_p / tot_p), 2),
                round(total_lots * (tp2_p / tot_p), 2),
            ]
        else:
            tot_p = (tp1_p + tp2_p + tp3_p) if (tp1_p + tp2_p + tp3_p) > 0 else 100.0
            vol_shares = [
                round(total_lots * (tp1_p / tot_p), 2),
                round(total_lots * (tp2_p / tot_p), 2),
                round(total_lots * (tp3_p / tot_p), 2),
            ]

        # Ensure volume constraints and volume_step compliance
        vol_shares = [max(min_lot, round(round(v / step) * step, 2)) for v in vol_shares]
        if risk_mode == "percent" and sl_price > 0 and len(vol_shares) > 1:
            allowed_budget = balance * (risk_value / 100.0)
            max_allowed_budget = allowed_budget * 1.25
            est_dist = abs(entry_price - sl_price)
            est_pts = est_dist / pt_size if pt_size > 0 else est_dist
            while len(vol_shares) > 1 and round(sum(vol_shares), 4) > round(total_lots, 4) and (est_pts * pt_val * sum(vol_shares)) > max_allowed_budget:
                vol_shares.pop()
                adj_tps = adj_tps[:len(vol_shares)]
                tp_list = tp_list[:len(vol_shares)]

        provider_magic = self.magic_base + int(provider_id if str(provider_id).isdigit() else 1)
        results = []
        placed_tickets = []
        placed_vols = []
        tier_errors = []

        for idx, (vol, tp_target) in enumerate(zip(vol_shares, adj_tps), start=1):
            if tp_target > 0:
                tp_target = round(tp_target, digits)
                # Option 2 Execution Guard: Prevent absurd TPs (>12% away from entry) from executing on MT4
                if entry_price > 0 and (abs(entry_price - tp_target) / entry_price > 0.12):
                    print(f"[{self.account_id}] [TP-GUARD BLOCKED] TP {tp_target} is >12% away from order price {entry_price}. Clamped to 0 (Open TP).")
                    tp_target = 0.0
                elif direction == "BUY" and tp_target <= entry_price:
                    print(f"[{self.account_id}] [WARN] BUY TP {tp_target} <= Entry {entry_price}. Clamped to 0.")
                    tp_target = 0.0
                elif direction == "SELL" and tp_target >= entry_price:
                    print(f"[{self.account_id}] [WARN] SELL TP {tp_target} >= Entry {entry_price}. Clamped to 0.")
                    tp_target = 0.0

            comment = f"P{provider_id}_T{idx}"
            try:
                with open("mt4_debug.log", "a", encoding="utf-8") as _df:
                    _df.write(f"[{datetime.now()}] Placing order: sym={symbol}, dir={direction}, type={order_type}, vol={vol}, price={entry_price}, sl={sl_price}, tp={tp_target}, magic={provider_magic}\n")
            except Exception:
                pass

            order_res = self.client.place_order(
                symbol=symbol,
                direction=direction,
                order_type=order_type,
                volume=vol,
                price=entry_price,
                sl=sl_price,
                tp=tp_target,
                magic=provider_magic,
                comment=comment,
            )

            try:
                with open("mt4_debug.log", "a", encoding="utf-8") as _df:
                    _df.write(f"[{datetime.now()}] Order result: {order_res}\n")
            except Exception:
                pass

            if order_res and order_res.get("ok"):
                tkt = order_res.get("ticket")
                placed_tickets.append(tkt)
                placed_vols.append(vol)
                results.append({
                    "ticket": tkt,
                    "symbol": symbol,
                    "direction": direction,
                    "volume": vol,
                    "price": order_res.get("price", entry_price),
                    "sl": sl_price,
                    "tp": tp_target,
                    "status": "OPEN",
                    "account_id": self.account_id,
                    "platform": "mt4",
                    "provider_id": str(provider_id),
                })
                print(f"[{self.account_id}] MT4 Order Placed: Ticket #{tkt} ({symbol} {direction} {vol} lots @ {entry_price})")
            else:
                err = order_res.get("error") if order_res else self.client.last_error()
                tier_errors.append(f"{comment}: {err}")
                print(f"[{self.account_id}] [ERROR] MT4 Order Failed ({comment}): {err}")

        if placed_tickets:
            self.trade_counter += 1
            now_ts = int(time.time())
            if message_id:
                mid = int(message_id)
                existing_tkts = self.message_to_tickets.get(mid, [])
                self.message_to_tickets[mid] = existing_tkts + [t for t in placed_tickets if t not in existing_tkts]

            sig_id = f"{self.account_id}_P{provider_id}_{now_ts}"
            tot_placed_lots = round(sum(placed_vols), 2)
            risk_amt_val = round(balance * (risk_value / 100.0), 2) if risk_mode == "percent" else round(tot_placed_lots * 10.0, 2)

            self.active_trade_groups[self.trade_counter] = {
                "message_id": int(message_id) if message_id else 0,
                "signal_id": sig_id,
                "symbol": symbol,
                "provider_id": str(provider_id),
                "direction": direction,
                "entry": entry_price,
                "signal_entry": signal_entry,
                "sl_price": sl_price,
                "tp1_price": tp_list[0] if len(tp_list) > 0 else 0.0,
                "tp2_price": tp_list[1] if len(tp_list) > 1 else 0.0,
                "tp3_price": tp_list[2] if len(tp_list) > 2 else 0.0,
                "tp1_ticket": placed_tickets[0] if len(placed_tickets) > 0 else None,
                "tp2_ticket": placed_tickets[1] if len(placed_tickets) > 1 else None,
                "tp3_ticket": placed_tickets[2] if len(placed_tickets) > 2 else None,
                "lots": tot_placed_lots,
                "risk_amount": risk_amt_val,
                "created_at": now_ts,
                "settings": settings,
                "status": "OPEN",
            }
            self.save_state()
            self._set_diag("COPIED", f"Placed {len(placed_tickets)} MT4 order(s) on {symbol}.", {"tickets": placed_tickets, "symbol": symbol, "tier_errors": tier_errors})
        else:
            self._set_diag("ORDER_SEND_FAILED", "; ".join(tier_errors) if tier_errors else f"All MT4 order tiers failed for {symbol}.", {"symbol": symbol, "tier_errors": tier_errors})

        return results

    def close_all_positions(self, provider_id=None) -> list:
        if not self.client.is_authorized:
            return []

        target_magic = (self.magic_base + int(provider_id)) if (provider_id and str(provider_id).isdigit()) else None
        positions = self.client.get_open_positions(magic=target_magic)
        results = []

        for p in positions:
            res = self.client.close_position(p.ticket, p.volume)
            if res and res.get("ok"):
                results.append({"ticket": p.ticket, "status": "CLOSED", "profit": p.profit})

        return results

    def close_and_cancel_all(self, provider_id=None, provider_ids=None) -> dict:
        if not self.client.is_authorized:
            return {"closed_positions_count": 0, "cancelled_orders_count": 0}

        target_magics = set()
        if provider_id and str(provider_id).isdigit():
            target_magics.add(self.magic_base + int(provider_id))
        elif provider_ids:
            for pid in provider_ids:
                if str(pid).isdigit():
                    target_magics.add(self.magic_base + int(pid))

        # Close market positions
        positions = self.client.get_open_positions()
        closed_count = 0
        for p in positions:
            if target_magics and p.magic not in target_magics:
                continue
            res = self.client.close_position(p.ticket, p.volume)
            if res and res.get("ok"):
                closed_count += 1

        # Delete pending orders
        orders = self.client.get_pending_orders()
        cancelled_count = 0
        for o in orders:
            if target_magics and o.get("magic") not in target_magics:
                continue
            res = self.client.cancel_order(o.get("ticket"))
            if res and res.get("ok"):
                cancelled_count += 1

        return {"closed_positions_count": closed_count, "cancelled_orders_count": cancelled_count}

    def close_trade_by_reply(self, reply_id: int) -> bool:
        if not reply_id:
            return False
        tickets = self.message_to_tickets.get(int(reply_id)) or self.message_to_tickets.get(str(reply_id)) or []
        if not tickets:
            return False
        success = True
        for t in tickets:
            res = self.client.close_position(t)
            if not res or not res.get("ok"):
                # Try cancel in case it's a pending order
                res_del = self.client.cancel_order(t)
                if not res_del or not res_del.get("ok"):
                    success = False
        return success

    def cancel_trade_by_reply(self, reply_id: int) -> bool:
        if not reply_id:
            return False
        tickets = self.message_to_tickets.get(int(reply_id)) or self.message_to_tickets.get(str(reply_id)) or []
        if not tickets:
            return False
        success = True
        for t in tickets:
            res = self.client.cancel_order(t)
            if not res or not res.get("ok"):
                # Try close in case order already triggered
                res_cls = self.client.close_position(t)
                if not res_cls or not res_cls.get("ok"):
                    success = False
        return success

    def cancel_all_pending_for_provider(self, provider_id: str) -> bool:
        if not self.client.is_authorized:
            return False
        p_magic = self.magic_base + int(provider_id) if str(provider_id).isdigit() else None
        orders = self.client.get_pending_orders(magic=p_magic)
        for o in orders:
            self.client.cancel_order(o.get("ticket"))
        return True

    def modify_sl_by_reply(self, reply_id: int, new_sl: float) -> bool:
        tickets = self.message_to_tickets.get(int(reply_id), [])
        if not tickets:
            for g in self.active_trade_groups.values():
                if g.get("status") == "OPEN":
                    for key in ["tp1_ticket", "tp2_ticket", "tp3_ticket"]:
                        t = g.get(key)
                        if t and t not in tickets:
                            tickets.append(t)
        if not tickets:
            return False
        positions = {p.ticket: p for p in self.client.get_open_positions()}
        orders = {int(o.get("ticket")): o for o in self.client.get_pending_orders() if o.get("ticket")}
        success = False
        for t in tickets:
            if t in positions:
                p = positions[t]
                res = self.client.modify_position(t, sl=new_sl, tp=p.tp)
                if res and res.get("ok"):
                    success = True
            elif t in orders:
                o = orders[t]
                res = self.client.modify_position(t, sl=new_sl, tp=float(o.get("tp") or 0.0))
                if res and res.get("ok"):
                    success = True
        return success

    def modify_signal_levels_by_msg(self, message_id: int, new_tps: list = None, new_sl: float = 0.0, new_entry: float = 0.0, provider_id: str = None, entry_min: float = None, entry_max: float = None) -> list:
        if not message_id:
            return []
        mid = int(message_id)
        new_tps = [float(x) for x in (new_tps or []) if float(x) > 0]
        new_sl = float(new_sl or 0.0)
        new_entry = float(new_entry or 0.0)

        val = self.message_to_tickets.get(mid) or self.message_to_tickets.get(str(mid)) or []
        tickets = list(val) if isinstance(val, list) else []
        matched_grp = None
        for g in self.active_trade_groups.values():
            g_tkts = [g.get("tp1_ticket"), g.get("tp2_ticket"), g.get("tp3_ticket")]
            if (g.get("message_id") and int(g.get("message_id")) == mid) or (tickets and any(t in tickets for t in g_tkts if t)):
                matched_grp = g
                if not tickets:
                    tickets = [t for t in g_tkts if t]
                break
        if not tickets:
            for g in self.active_trade_groups.values():
                if g.get("status") == "OPEN" and (not provider_id or str(g.get("provider_id")) == str(provider_id)):
                    matched_grp = g
                    for key in ["tp1_ticket", "tp2_ticket", "tp3_ticket"]:
                        t = g.get(key)
                        if t:
                            tickets.append(t)
                    break
        if not tickets:
            return []

        if matched_grp:
            try:
                g_sym = matched_grp.get("symbol", "XAUUSD")
                g_dir = matched_grp.get("direction", "BUY")
                sym_info_g = self.client.get_symbol_info(g_sym)
                digits_g = sym_info_g.digits if sym_info_g else 2
                tick_g = self.client.get_tick(g_sym)
                if tick_g and getattr(tick_g, "bid", 0) > 0 and getattr(tick_g, "ask", 0) > 0:
                    from signal_parser import normalize_and_autofix_signal
                    temp_sig = {
                        "direction": g_dir,
                        "entry": new_entry if new_entry > 0 else float(matched_grp.get("signal_entry") or matched_grp.get("entry") or 0.0),
                        "entry_min": entry_min,
                        "entry_max": entry_max,
                        "sl": new_sl if new_sl > 0 else float(matched_grp.get("sl_price") or 0.0),
                        "tp": new_tps,
                    }
                    fixed_sig = normalize_and_autofix_signal(temp_sig, tick_g.bid, tick_g.ask, digits_g)
                    if new_entry > 0:
                        new_entry = float(fixed_sig.get("entry") or new_entry)
                    if new_sl > 0:
                        new_sl = float(fixed_sig.get("sl") or new_sl)
                        matched_grp["sl_price"] = new_sl
                    if new_tps:
                        new_tps = list(fixed_sig.get("tp") or new_tps)
            except Exception as _me:
                print(f"[{self.account_id}] [WARN] modify_signal_levels_by_msg autofix error: {_me}")

        positions = {p.ticket: p for p in self.client.get_open_positions()}
        orders = {int(o.get("ticket")): o for o in self.client.get_pending_orders() if o.get("ticket")}
        modified_results = []
        for idx, t in enumerate(tickets):
            open_price = 0.0
            curr_sl = 0.0
            curr_tp = 0.0
            found = False
            if t in positions:
                p = positions[t]
                open_price = p.price_open
                curr_sl = p.sl
                curr_tp = p.tp
                found = True
            elif t in orders:
                o = orders[t]
                open_price = float(o.get("price") or o.get("open_price") or 0.0)
                curr_sl = float(o.get("sl") or 0.0)
                curr_tp = float(o.get("tp") or 0.0)
                found = True

            if found:
                target_tp = new_tps[idx] if idx < len(new_tps) else (new_tps[-1] if new_tps else 0.0)
                # Option 2 execution guard
                if target_tp > 0 and open_price > 0 and abs(open_price - target_tp) / open_price > 0.12:
                    target_tp = 0.0
                res = self.client.modify_position(t, sl=new_sl if new_sl > 0 else curr_sl, tp=target_tp if target_tp > 0 else curr_tp)
                if res and res.get("ok"):
                    if matched_grp and idx < 3:
                        matched_grp[f"tp{idx+1}_price"] = target_tp
                    modified_results.append({
                        "ticket": t,
                        "tier": f"TP{idx+1}",
                        "action": "MODIFIED_POSITION",
                        "sl": new_sl,
                        "tp": target_tp
                    })
                    print(f"[{self.account_id}] [EDIT-MODIFY] MT4 Order #{t} updated: SL={new_sl}, TP={target_tp}")
        if modified_results:
            self.save_state()
        return modified_results

    def set_be_for_provider(self, provider_id: str = None) -> int:
        if not self.client.is_authorized:
            return 0
        p_magic = self.magic_base + int(provider_id) if (provider_id and str(provider_id).isdigit()) else None
        positions = self.client.get_open_positions(magic=p_magic)
        count = 0
        state_changed = False
        for p in positions:
            sym_info = self.client.get_symbol_info(p.symbol)
            digits = sym_info.digits if sym_info else 2
            tick = self.client.get_tick(p.symbol)
            spread = (tick.ask - tick.bid) if (tick and getattr(tick, "ask", 0) > 0 and getattr(tick, "bid", 0) > 0) else ((sym_info.spread * sym_info.point) if sym_info else 0.0)
            be_price = round(p.price_open + spread if p.type == 0 else p.price_open - spread, digits)
            curr_price = (tick.bid if p.type == 0 else tick.ask) if (tick and getattr(tick, "bid", 0) > 0) else getattr(p, "price_current", p.price_open)
            is_in_profit = (p.type == 0 and curr_price > be_price) or (p.type == 1 and curr_price < be_price)

            for grp in self.active_trade_groups.values():
                grp_tkts = [grp.get("tp1_ticket"), grp.get("tp2_ticket"), grp.get("tp3_ticket")]
                if p.ticket in grp_tkts:
                    grp.setdefault("be_targets", {})[str(p.ticket)] = {
                        "price": be_price,
                        "mode": "SL" if is_in_profit else "TP",
                    }
                    if not is_in_profit:
                        grp["be_recovery"] = True
                    state_changed = True

            if is_in_profit:
                if p.sl == 0 or (p.type == 0 and be_price > p.sl) or (p.type == 1 and be_price < p.sl):
                    res = self.client.modify_position(p.ticket, sl=be_price, tp=p.tp)
                    if res and res.get("ok"):
                        count += 1
            else:
                if (p.type == 0 and curr_price >= be_price) or (p.type == 1 and curr_price <= be_price):
                    res = self.client.close_position(p.ticket, p.volume)
                    if res and res.get("ok"):
                        count += 1
                else:
                    res = self.client.modify_position(p.ticket, sl=p.sl, tp=be_price)
                    if res and res.get("ok"):
                        count += 1
        if state_changed:
            self.save_state()
        return count

    def sync_closed_trades_to_db(self, positions: list = None, pending_orders: list = None, history: list = None):
        """Synchronizes closed MT4 trades and signals from MT4 history into stats.db for dashboard analytics."""
        try:
            import re
            import db
            if not self.client.is_authorized:
                return

            if positions is None:
                positions = self.client.get_open_positions()
            if pending_orders is None:
                pending_orders = self.client.get_pending_orders()
            if history is None:
                history = self.client.get_history(limit=300)

            open_tickets = {p.ticket: p for p in (positions or [])}
            pending_tickets = {int(o.get("ticket", 0)): o for o in (pending_orders or []) if o.get("ticket")}
            hist_by_ticket = {int(h.get("ticket", 0)): h for h in (history or []) if h.get("ticket")}

            reset_ts = int(float(db.get_metadata(f"reset_time_{self.account_id}") or 0))
            assigned_pids = {str(p) for p in (self.account_config.get("providers") or [])}
            p_settings = self.account_config.get("provider_settings") or {}

            conn = db.get_db()
            existing_rows = conn.execute("SELECT ticket FROM trades WHERE account_id = ?", (self.account_id,)).fetchall()
            existing_tickets = {int(r["ticket"]) for r in existing_rows}
            conn.close()

            new_trades_saved = 0
            state_changed = False

            def _resolve_pid_and_tier(magic_val: int, comment_str: str):
                pid = None
                tier_num = 1
                comm = str(comment_str or "")
                m = re.search(r"P(\d+)", comm)
                if m:
                    pid = m.group(1)
                tm = re.search(r"_T(?:P)?(\d+)", comm, re.IGNORECASE)
                if tm:
                    tier_num = int(tm.group(1))
                if not pid and magic_val:
                    if magic_val > self.magic_base:
                        pid = str(magic_val - self.magic_base)
                    elif 400000 < magic_val < 500000:
                        pid = str(magic_val - 400400 if magic_val > 400400 else magic_val - 400000)
                return pid, tier_num

            # 1. Sync all closed trades from MT4 history into `trades` table
            all_hist_records = []
            for h in (history or []):
                tkt = int(h.get("ticket") or 0)
                if not tkt:
                    continue
                close_time = int(h.get("close_time") or 0)
                open_time = int(h.get("open_time") or close_time)
                if close_time > 0 and close_time < reset_ts:
                    continue

                magic_val = int(h.get("magic") or 0)
                comm = str(h.get("comment") or "")
                pid, tier_num = _resolve_pid_and_tier(magic_val, comm)
                if not pid:
                    continue
                if assigned_pids and pid not in assigned_pids:
                    continue

                symbol = str(h.get("symbol") or "XAUUSD")
                direction = "BUY" if int(h.get("type") or 0) == 0 else "SELL"
                lots = float(h.get("volume") or 0.01)
                open_price = float(h.get("price_open") or 0.0)
                close_price = float(h.get("price_close") or 0.0)
                net_profit = round(float(h.get("profit") or 0.0) + float(h.get("swap") or 0.0) + float(h.get("commission") or 0.0), 2)

                comm_l = comm.lower()
                if abs(net_profit) < 0.5:
                    close_reason = "BE"
                elif "[tp]" in comm_l or net_profit > 0:
                    close_reason = f"TP{tier_num}"
                elif "[sl]" in comm_l or net_profit < 0:
                    close_reason = "SL"
                else:
                    close_reason = "BE"

                if tkt not in existing_tickets:
                    db.save_trade(
                        ticket=tkt,
                        provider_id=pid,
                        symbol=symbol,
                        trade_type=direction,
                        lots=lots,
                        open_time=open_time,
                        close_time=close_time,
                        open_price=open_price,
                        close_price=close_price,
                        profit=net_profit,
                        magic=magic_val,
                        close_reason=close_reason,
                        account_id=self.account_id,
                    )
                    existing_tickets.add(tkt)
                    new_trades_saved += 1
                    print(f"[{self.account_id}] Synced closed MT4 trade to DB: Ticket=#{tkt}, P={pid}, {close_reason}, Profit=${net_profit:.2f}")

                all_hist_records.append({
                    "ticket": tkt,
                    "provider_id": pid,
                    "symbol": symbol,
                    "direction": direction,
                    "lots": lots,
                    "open_time": open_time,
                    "close_time": close_time,
                    "open_price": open_price,
                    "close_price": close_price,
                    "profit": net_profit,
                    "tier_num": tier_num,
                    "close_reason": close_reason,
                    "is_closed": True,
                })

            # Also include currently open positions for accurate signal cluster status
            for p in (positions or []):
                pid, tier_num = _resolve_pid_and_tier(int(p.magic or 0), str(p.comment or ""))
                if not pid or (assigned_pids and pid not in assigned_pids):
                    continue
                if int(p.time or 0) > 0 and int(p.time or 0) < reset_ts:
                    continue
                all_hist_records.append({
                    "ticket": int(p.ticket),
                    "provider_id": pid,
                    "symbol": str(p.symbol or "XAUUSD"),
                    "direction": "BUY" if int(p.type or 0) == 0 else "SELL",
                    "lots": float(p.volume or 0.01),
                    "open_time": int(p.time or time.time()),
                    "close_time": 0,
                    "open_price": float(p.price_open or 0.0),
                    "close_price": 0.0,
                    "profit": round(float(p.profit or 0.0), 2),
                    "tier_num": tier_num,
                    "close_reason": "",
                    "is_closed": False,
                })

            # 2. Update active_trade_groups statuses
            for gid, grp in list(self.active_trade_groups.items()):
                t1 = grp.get("tp1_ticket")
                t2 = grp.get("tp2_ticket")
                t3 = grp.get("tp3_ticket")
                any_open = any((t and (int(t) in open_tickets or int(t) in pending_tickets)) for t in [t1, t2, t3])
                new_status = "OPEN" if any_open else "CLOSED"
                if grp.get("status") != new_status:
                    grp["status"] = new_status
                    state_changed = True

            # 3. Group all records into originating signals by (provider_id, symbol, direction) within 60s open_time window
            all_hist_records.sort(key=lambda x: (x["open_time"], x["ticket"]))
            clusters = []
            for rec in all_hist_records:
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

            info = self.client.get_account_info()
            bal = float(getattr(info, "balance", 0.0) or 1000.0) if info else 1000.0

            for cl in clusters:
                closed_legs = [p for p in cl["positions"] if p["is_closed"]]
                if not closed_legs:
                    continue

                pid = cl["provider_id"]
                symbol = cl["symbol"]
                direction = cl["direction"]
                created_at = cl["created_at"]
                sig_id = f"{self.account_id}_P{pid}_{created_at}"

                # Match to active_trade_groups signal_id if one exists for these tickets
                cl_tkts = {p["ticket"] for p in cl["positions"]}
                for grp in self.active_trade_groups.values():
                    g_tkts = {int(grp[k]) for k in ("tp1_ticket", "tp2_ticket", "tp3_ticket") if grp.get(k)}
                    if g_tkts & cl_tkts and grp.get("signal_id"):
                        sig_id = grp["signal_id"]
                        break

                tot_lots = round(sum(p["lots"] for p in cl["positions"]), 2)
                tot_profit = round(sum(p["profit"] for p in closed_legs), 2)
                closed_at = max(p["close_time"] for p in closed_legs)
                all_closed = len(closed_legs) == len(cl["positions"])
                status = "CLOSED" if all_closed else "OPEN"

                tp1_hit = any(p["tier_num"] == 1 and (p["close_reason"].startswith("TP") or p["profit"] > 0) for p in closed_legs)
                tp2_hit = any(p["tier_num"] == 2 and (p["close_reason"].startswith("TP") or p["profit"] > 0) for p in closed_legs)
                tp3_hit = any(p["tier_num"] == 3 and (p["close_reason"].startswith("TP") or p["profit"] > 0) for p in closed_legs)
                sl_hit = (not tp1_hit and not tp2_hit and not tp3_hit and all(p["profit"] < 0 for p in closed_legs))

                p_cfg = p_settings.get(str(pid)) or {}
                r_val = float(p_cfg.get("risk_value", 1.0) or 1.0)
                risk_amt = abs(tot_profit) if (sl_hit and tot_profit < 0) else (round(bal * (r_val / 100.0), 2) if p_cfg.get("risk_mode", "percent") == "percent" else 10.0)

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
                    account_id=self.account_id,
                )

            if state_changed:
                self.save_state()

            if new_trades_saved > 0 or state_changed:
                try:
                    import provider_analytics as pa
                    pa.sync_from_trades_db(reset_ts=reset_ts)
                except Exception as _pa_e:
                    print(f"[{self.account_id}] pa.sync_from_trades_db error: {_pa_e}")
        except Exception as e:
            print(f"[{self.account_id}] sync_closed_trades_to_db error: {e}")

    def manage_trailing_stops(self):
        """Monitors open trade groups, applies breakeven/trailing stops, and syncs closed trades to stats.db."""
        raw_positions = self.client.get_open_positions()
        positions = {p.ticket: p for p in (raw_positions or [])}
        pending_orders = self.client.get_pending_orders()
        pending_map = {int(o.get("ticket", 0)): o for o in (pending_orders or []) if o.get("ticket")}
        state_changed = False

        for gid, grp in list(self.active_trade_groups.items()):
            t1 = grp.get("tp1_ticket")
            t2 = grp.get("tp2_ticket")
            t3 = grp.get("tp3_ticket")

            t1_open = (t1 in positions or t1 in pending_map) if t1 else False
            t2_open = (t2 in positions or t2 in pending_map) if t2 else False
            t3_open = (t3 in positions or t3 in pending_map) if t3 else False

            if not (t1_open or t2_open or t3_open):
                if grp.get("status") != "CLOSED":
                    grp["status"] = "CLOSED"
                    state_changed = True
                continue

            # Enforce any pending BE targets (underwater BE recovery via TP or stops_level fallback)
            be_targets = grp.get("be_targets") or {}
            symbol = grp.get("symbol", "")
            if be_targets:
                sym_info_be = self.client.get_symbol_info(symbol) if symbol else None
                digits_be = sym_info_be.digits if sym_info_be else 2
                point_be = sym_info_be.point if (sym_info_be and sym_info_be.point) else (10 ** -digits_be)
                tick_be = self.client.get_tick(symbol) if symbol else None
                for _tkt in [t1, t2, t3]:
                    if not _tkt or _tkt not in positions:
                        continue
                    t_cfg = be_targets.get(str(_tkt))
                    if not t_cfg:
                        continue
                    pos_be = positions[_tkt]
                    be_p = round(float(t_cfg.get("price") or 0.0), digits_be)
                    be_mode = t_cfg.get("mode", "TP")
                    if be_p <= 0:
                        continue
                    curr_p = (tick_be.bid if pos_be.type == 0 else tick_be.ask) if (tick_be and getattr(tick_be, "bid", 0) > 0) else getattr(pos_be, "price_current", pos_be.price_open)
                    if be_mode == "TP":
                        if (pos_be.type == 0 and curr_p >= be_p) or (pos_be.type == 1 and curr_p <= be_p):
                            self.client.close_position(pos_be.ticket, pos_be.volume)
                        elif abs((pos_be.tp or 0.0) - be_p) > point_be * 0.5:
                            self.client.modify_position(pos_be.ticket, sl=pos_be.sl, tp=be_p)
                    elif be_mode == "SL":
                        if (pos_be.type == 0 and curr_p <= be_p) or (pos_be.type == 1 and curr_p >= be_p):
                            self.client.close_position(pos_be.ticket, pos_be.volume)
                        elif pos_be.sl == 0 or (pos_be.type == 0 and be_p > pos_be.sl + point_be * 0.5) or (pos_be.type == 1 and be_p < pos_be.sl - point_be * 0.5):
                            self.client.modify_position(pos_be.ticket, sl=be_p, tp=pos_be.tp)

            # If TP1 has closed and TP2 or TP3 are still open -> move to breakeven if configured
            settings = grp.get("settings") or self.account_config.get("provider_settings", {}).get(str(grp.get("provider_id"))) or {}
            be_rule = settings.get("sl_breakeven_at", "TP1")
            entry = grp.get("entry", 0.0)
            t1_is_be_tp = str(t1) in be_targets and be_targets[str(t1)].get("mode") == "TP"

            if not t1_open and t1 and not grp.get("tp1_be_done", False) and not t1_is_be_tp:
                grp["tp1_be_done"] = True
                state_changed = True
                if be_rule == "TP1":
                    sym_info = self.client.get_symbol_info(symbol)
                    spread = (sym_info.spread * sym_info.point) if sym_info else 0.0
                    for rem_tkt in [t2, t3]:
                        if rem_tkt and rem_tkt in positions:
                            pos = positions[rem_tkt]
                            base_entry = pos.price_open if pos.price_open > 0 else entry
                            be_sl = base_entry + spread if pos.type == 0 else base_entry - spread
                            self.client.modify_position(pos.ticket, sl=be_sl, tp=pos.tp)

        if state_changed:
            self.save_state()

        self.sync_closed_trades_to_db(positions=raw_positions, pending_orders=pending_orders)


def run_mt4_worker_process(account_config: dict, in_q: Queue, out_q: Queue):
    worker = MT4AccountWorker(account_config, in_q, out_q)
    worker.run()
