"""
ctrader_worker.py — Standalone Process Worker for a cTrader Account.
Mirrors account_worker.py (MT5AccountWorker) with an identical queue command
interface so Sirius Copier can run MT5 and cTrader accounts side-by-side.
"""

import os
import time
import json
import math
import traceback
from multiprocessing import Queue
from ctrader_client import CTraderClient


class CTraderAccountWorker:
    def __init__(self, account_config: dict, in_queue: Queue, out_queue: Queue):
        self.account_id = account_config["id"]
        self.config = account_config
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.is_running = True

        self.state_file = f"trade_state_{self.account_id}.json"
        self.message_to_tickets = {}
        self.active_trade_groups = {}
        self.trade_counter = 0
        self.magic_base = int(account_config.get("magic_base", 400000))
        self.last_exec_diag = {}

        self.client: CTraderClient = CTraderClient(
            host=str(account_config.get("ctrader_host") or "127.0.0.1"),
            port=int(account_config.get("ctrader_port") or 8888),
            account_id=int(account_config.get("ctrader_account_id") or account_config.get("mt5_login") or 0),
            env=str(account_config.get("ctrader_env", "demo")),
            commission_per_lot=float(account_config.get("commission_per_lot", 6.0)),
            account_str_id=self.account_id,
        )

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
                    self.message_to_tickets = {int(k): v for k, v in data.get("message_to_tickets", {}).items()}
                    self.active_trade_groups = {int(k): v for k, v in data.get("active_trade_groups", {}).items()}
                    self.trade_counter = data.get("trade_counter", 0)
            except Exception as e:
                print(f"[{self.account_id}] Failed to load cTrader trade state: {e}")

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
            print(f"[{self.account_id}] Failed to save cTrader trade state: {e}")

    def connect(self) -> bool:
        ok = self.client.connect()
        if ok:
            print(
                f"[{self.account_id}] Connected to cTrader ({self.client.env.upper()}) "
                f"Account Login={self.client.trader_login} (ctid={self.client.ctid_trader_account_id})"
            )
        else:
            print(f"[{self.account_id}] cTrader connection failed: {self.client.last_error()}")
        return ok

    def run(self):
        """Worker process loop."""
        try:
            self.connect()
            if self.client.is_authorized:
                try:
                    self.sync_closed_trades_to_db()
                except Exception:
                    pass
            last_ts_check = time.time()
            last_reconnect_attempt = time.time()

            while self.is_running:
                # Auto-reconnect every 3s if cBot bridge is disconnected
                if not self.client.is_authorized and (time.time() - last_reconnect_attempt >= 3.0):
                    last_reconnect_attempt = time.time()
                    self.connect()

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
                        self.out_queue.put({"account_id": self.account_id, "req_id": req_id, "state": state})
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

                now = time.time()
                if now - last_ts_check >= 2.0:
                    last_ts_check = now
                    try:
                        if self.client.is_authorized:
                            self.manage_trailing_stops()
                    except Exception:
                        pass

                time.sleep(0.05)

        except Exception as e:
            print(f"[{self.account_id}] cTrader Worker Exception: {e}\n{traceback.format_exc()}")
        finally:
            try:
                self.client.disconnect()
            except Exception:
                pass
            print(f"[{self.account_id}] cTrader Worker terminated cleanly.")

    def get_account_state(self) -> dict:
        if not self.client.is_authorized:
            self.connect()

        if not self.client.is_authorized:
            return {
                "account_id": self.account_id,
                "platform": "ctrader",
                "connected": False,
                "login": self.config.get("ctrader_account_id") or self.config.get("name", "cTrader"),
                "server": f"cTrader-{str(self.config.get('ctrader_env', 'demo')).upper()}",
                "balance": 0.0,
                "equity": 0.0,
                "margin": 0.0,
                "margin_free": 0.0,
                "open_positions": [],
                "last_error": self.client.last_error(),
            }

        info, positions = self.client.get_full_state()
        if info is None:
            self.connect()
            info, positions = self.client.get_full_state()

        # Build ticket-to-info map from active_trade_groups
        ticket_map = {}
        for gid, g in list(self.active_trade_groups.items()):
            pid = str(g.get("provider_id") or "")
            g_sl = g.get("sl_price") or g.get("sl")
            for t_str, t_info in (g.get("tickets_info") or {}).items():
                try:
                    ticket_map[int(t_str)] = {
                        "provider_id": pid,
                        "sl": t_info.get("sl") or g_sl,
                        "tp": t_info.get("target_tp") or g.get("tp1_price"),
                        "tier": t_info.get("tier"),
                    }
                except Exception:
                    pass
            for key, tier_lbl in [("tp1_ticket", "TP1"), ("tp2_ticket", "TP2"), ("tp3_ticket", "TP3")]:
                t = g.get(key)
                if t:
                    try:
                        t_int = int(t)
                        if t_int not in ticket_map:
                            tp_tier_key = tier_lbl.lower() + "_price"
                            ticket_map[t_int] = {
                                "provider_id": pid,
                                "sl": g_sl,
                                "tp": g.get(tp_tier_key) or g.get("tp1_price"),
                                "tier": tier_lbl,
                            }
                    except Exception:
                        pass
            for t in g.get("tickets", []):
                try:
                    t_int = int(t)
                    if t_int not in ticket_map:
                        ticket_map[t_int] = {
                            "provider_id": pid,
                            "sl": g_sl,
                            "tp": g.get("tp1_price"),
                            "tier": None,
                        }
                except Exception:
                    pass

        pos_list = []
        for p in positions:
            t_data = ticket_map.get(p.ticket, {})
            pid = t_data.get("provider_id")
            tier = t_data.get("tier")

            if not pid and p.comment:
                comm = str(p.comment)
                if comm.startswith("P") and "_" in comm:
                    part = comm[1:].split("_")[0]
                    if part.isdigit():
                        pid = part
                    tier_part = comm[1:].split("_")[1] if len(comm[1:].split("_")) > 1 else None
                    if tier_part and not tier:
                        tier = tier_part
            if not pid and p.magic:
                mag = int(p.magic)
                if 400000 < mag < 500000:
                    pid = str(mag - 400000)
                elif mag > 400400:
                    pid = str(mag - 400400)

            sl_val = p.sl if (p.sl and float(p.sl) > 0) else (t_data.get("sl") or 0.0)
            tp_val = p.tp if (p.tp and float(p.tp) > 0) else (t_data.get("tp") or 0.0)

            pos_list.append(
                {
                    "ticket": p.ticket,
                    "symbol": p.symbol,
                    "type": "BUY" if p.type == 0 else "SELL",
                    "volume": p.volume,
                    "open_price": p.price_open,
                    "price_open": p.price_open,
                    "current_price": p.price_current,
                    "price_current": p.price_current,
                    "sl": float(sl_val) if sl_val else 0.0,
                    "tp": float(tp_val) if tp_val else 0.0,
                    "profit": p.profit,
                    "magic": p.magic,
                    "comment": p.comment,
                    "open_time": p.time,
                    "platform": "ctrader",
                    "provider_id": str(pid) if pid else None,
                    "tier": tier,
                }
            )

        return {
            "account_id": self.account_id,
            "platform": "ctrader",
            "connected": info is not None,
            "login": getattr(info, "login", self.client.trader_login),
            "server": getattr(info, "server", f"cTrader-{self.client.env.upper()}"),
            "balance": getattr(info, "balance", 0.0),
            "equity": getattr(info, "equity", 0.0),
            "margin": getattr(info, "margin", 0.0),
            "margin_free": getattr(info, "margin_free", 0.0),
            "open_positions": pos_list,
        }

    def execute_signal(self, signal: dict, message_id: int = None, provider_id: str = "1", settings: dict = None) -> list:
        import copy
        signal = copy.deepcopy(signal) if isinstance(signal, dict) else {}

        if message_id is not None and (int(message_id) in self.message_to_tickets or str(message_id) in self.message_to_tickets):
            mid_int = int(message_id)
            prev_tkts = self.message_to_tickets.get(mid_int) or self.message_to_tickets.get(str(mid_int)) or []
            if isinstance(prev_tkts, dict):
                prev_tkts = list(prev_tkts.get("tickets", []))
            if prev_tkts:
                still_valid = False
                for _g in self.active_trade_groups.values():
                    g_tkts = {_g.get("tp1_ticket"), _g.get("tp2_ticket"), _g.get("tp3_ticket")}
                    if any(int(_t) in g_tkts for _t in prev_tkts if _t):
                        if _g.get("status") not in ("CLOSED", "CANCELLED"):
                            still_valid = True
                            break
                        if _g.get("closed_tickets"):
                            still_valid = True
                            break
                if not still_valid:
                    try:
                        open_pos = {p.ticket for p in (self.client.get_open_positions() or [])}
                        pend_ord = {int(o.get("ticket", 0)) for o in (self.client.get_pending_orders(use_cached=True) or []) if o.get("ticket")}
                        hist_deals = {int(h.get("position_id") or h.get("ticket") or 0) for h in (self.client.get_history(100) or [])}
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
                    print(f"[{self.account_id}] [SANITY-GUARD] Message ID {message_id} already executed. Skipping duplicate.")
                    self._set_diag("ALREADY_EXECUTED", f"Message ID {message_id} already has active/filled orders {prev_tkts}.")
                    return []
                else:
                    print(f"[{self.account_id}] [SANITY-GUARD] Previous pending orders {prev_tkts} for Message ID {message_id} were cancelled unfilled. Allowing fresh execution on edit.")
                    self.message_to_tickets.pop(mid_int, None)
                    self.message_to_tickets.pop(str(mid_int), None)

        if not self.client.is_authorized:
            self._set_diag("DISCONNECTED", "cTrader cBot bridge is not authorized/connected.")
            return []
        if settings is None:
            settings = {}

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

        symbol = self.client.resolve_symbol(raw_sym)
        sym_info = self.client.get_symbol_info(symbol) if symbol else None
        if not symbol or not sym_info:
            err = f"Symbol '{raw_sym}' (resolved: '{symbol}') not found in cTrader. Strict matching enforced: trade skipped."
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

        tick = self.client.get_tick(symbol)
        if not tick:
            err = f"Could not retrieve live price tick for {symbol} on cTrader"
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

        e_min = signal.get("entry_min")
        e_max = signal.get("entry_max")
        if entry_price and entry_price > 0 and e_min is not None and e_max is not None:
            try:
                lo, hi = min(float(e_min), float(e_max)), max(float(e_min), float(e_max))
                if hi > lo and lo - 0.01 <= float(entry_price) <= hi + 0.01:
                    zmode = str(settings.get("entry_zone_mode", "mid")).lower()
                    if zmode == "nearest":
                        entry_price = lo if direction == "SELL" else hi
                    elif zmode == "furthest":
                        entry_price = hi if direction == "SELL" else lo
                    entry_price = round(float(entry_price), digits)
                    signal["entry"] = entry_price
            except Exception:
                pass

        tp_list = signal.get("tp") if isinstance(signal.get("tp"), list) else []
        tp1 = signal.get("tp1") or (tp_list[0] if len(tp_list) > 0 else None)
        tp2 = signal.get("tp2") or (tp_list[1] if len(tp_list) > 1 else None)
        tp3 = signal.get("tp3") or (tp_list[2] if len(tp_list) > 2 else None)

        spread_offset_entry = bool(settings.get("spread_offset_entry", True))
        spread_offset_tp = bool(settings.get("spread_offset_tp", True))
        spread_offset_sl = bool(settings.get("spread_offset_sl", True))
        spread_val = (tick.ask - tick.bid) if tick else (sym_info.spread * sym_info.point)

        eff_entry = entry_price
        if entry_price and entry_price > 0 and spread_offset_entry and spread_val > 0 and direction == "BUY":
            eff_entry = entry_price + spread_val

        eff_sl = sl_price
        if sl_price and sl_price > 0 and spread_offset_sl and spread_val > 0 and direction == "SELL":
            eff_sl = sl_price + spread_val

        min_stop_dist = max((getattr(sym_info, 'trade_stops_level', 0) if sym_info else 0) * sym_info.point, spread_val + 5 * sym_info.point)
        threshold = 50 * sym_info.point
        curr_price = tick.ask if direction == "BUY" else tick.bid

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

        # Unified order category and price across ALL tiers (computed BEFORE risk/lot sizing so ref_p uses actual planned order price)
        if direction == "BUY":
            if eff_entry and eff_entry > 0:
                if e_min is not None and e_max is not None:
                    z_lo, z_hi = min(float(e_min), float(e_max)), max(float(e_min), float(e_max))
                    in_entry_zone = (z_lo - threshold) <= curr_price <= (z_hi + threshold)
                else:
                    in_entry_zone = abs(curr_price - eff_entry) <= threshold
            else:
                in_entry_zone = True

            tp_violated = any(t > 0 and t <= curr_price + min_stop_dist for t in adj_tps)
            sl_violated = (eff_sl and eff_sl > 0 and eff_sl >= curr_price - min_stop_dist)

            if sig_order_type not in ("LIMIT", "STOP") and in_entry_zone and not tp_violated and not sl_violated:
                unified_cat = "MARKET"
                unified_price = curr_price
            elif eff_entry and eff_entry > 0:
                if eff_entry < curr_price:
                    unified_cat = "LIMIT"
                    unified_price = eff_entry
                else:
                    unified_cat = "STOP"
                    unified_price = eff_entry
            else:
                unified_cat = "MARKET"
                unified_price = curr_price

        elif direction == "SELL":
            if eff_entry and eff_entry > 0:
                if e_min is not None and e_max is not None:
                    z_lo, z_hi = min(float(e_min), float(e_max)), max(float(e_min), float(e_max))
                    in_entry_zone = (z_lo - threshold) <= curr_price <= (z_hi + threshold)
                else:
                    in_entry_zone = abs(curr_price - eff_entry) <= threshold
            else:
                in_entry_zone = True

            tp_violated = any(t > 0 and t >= curr_price - min_stop_dist for t in adj_tps)
            sl_violated = (eff_sl and eff_sl > 0 and eff_sl <= curr_price + min_stop_dist)

            if sig_order_type not in ("LIMIT", "STOP") and in_entry_zone and not tp_violated and not sl_violated:
                unified_cat = "MARKET"
                unified_price = curr_price
            elif eff_entry and eff_entry > 0:
                if eff_entry > curr_price:
                    unified_cat = "LIMIT"
                    unified_price = eff_entry
                else:
                    unified_cat = "STOP"
                    unified_price = eff_entry
            else:
                unified_cat = "MARKET"
                unified_price = curr_price
        else:
            return []

        acc_info = self.client.get_account_info()
        balance = getattr(acc_info, "balance", 10000.0)
        risk_mode = settings.get("risk_mode", "percent")
        risk_val = float(settings.get("risk_value", 1.0))

        step = sym_info.volume_step if sym_info.volume_step > 0 else 0.01
        min_lot = sym_info.volume_min if sym_info.volume_min > 0 else 0.01
        pt_val = sym_info.trade_tick_value if sym_info.trade_tick_value > 0 else 1.0
        pt_size = sym_info.trade_tick_size if sym_info.trade_tick_size > 0 else (sym_info.point if sym_info.point > 0 else 0.01)

        ref_p = unified_price if unified_price and unified_price > 0 else (entry_price if entry_price and entry_price > 0 else live_price)

        if risk_mode == "fixed":
            total_lot = max(min_lot, round(risk_val / step) * step)
        else:
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

        tp1_pct = float(settings.get("tp1_percent", 33.3))
        tp2_pct = float(settings.get("tp2_percent", 33.3))
        tp3_pct = float(settings.get("tp3_percent", 33.4))
        total_p = (tp1_pct + tp2_pct + tp3_pct) if (tp1_pct + tp2_pct + tp3_pct) > 0 else 100.0

        lot1 = round(max(min_lot, round(((total_lot * (tp1_pct / total_p))) / step) * step), 2)
        lot2 = round(max(min_lot, round(((total_lot * (tp2_pct / total_p))) / step) * step), 2)
        lot3 = round(max(min_lot, round(((total_lot * (tp3_pct / total_p))) / step) * step), 2)

        magic = self.magic_base + int(provider_id)
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

            order_cat = unified_cat
            order_price = unified_price

            # Ensure TP and SL are on the valid sides of order_price for broker
            broker_tp = eff_tp
            if broker_tp and broker_tp > 0:
                if direction == "SELL" and broker_tp >= order_price:
                    broker_tp = 0.0
                elif direction == "BUY" and broker_tp <= order_price:
                    broker_tp = 0.0
                # Option 2 Execution Guard: Prevent absurd TPs (>12% away from order price) from executing on cTrader
                elif order_price > 0 and (abs(order_price - broker_tp) / order_price > 0.12):
                    print(f"[{self.account_id}] [TP-GUARD BLOCKED] TP {broker_tp} is >12% away from order price {order_price}. Clamped to 0 (Open TP).")
                    broker_tp = 0.0

            broker_sl = eff_sl
            if broker_sl and broker_sl > 0:
                if direction == "SELL" and broker_sl <= order_price:
                    print(f"[{self.account_id}] [RISK-GUARD BLOCKED] Stop loss {broker_sl} is below/equal to SELL order price {order_price}. Aborting tier {lbl}!")
                    continue
                elif direction == "BUY" and broker_sl >= order_price:
                    print(f"[{self.account_id}] [RISK-GUARD BLOCKED] Stop loss {broker_sl} is above/equal to BUY order price {order_price}. Aborting tier {lbl}!")
                    continue

            # Rule 2: Per-Tier Real-World Dollar Risk Verification
            if broker_sl and broker_sl > 0:
                tier_dist = abs(order_price - broker_sl)
                tier_pts = tier_dist / pt_size if pt_size > 0 else tier_dist
                tier_real_risk = tier_pts * pt_val * vol
                if risk_mode == "percent":
                    allowed_budget = balance * (risk_val / 100.0)
                    max_allowed_budget = allowed_budget * 1.25
                    if tier_real_risk > max_allowed_budget:
                        print(f"[{self.account_id}] [RISK-GUARD BLOCKED] Tier {lbl} real risk ${tier_real_risk:.2f} exceeds allocated budget ${allowed_budget:.2f} (max: ${max_allowed_budget:.2f}). Order aborted!")
                        continue

            next_gid = self.trade_counter + 1
            res = self.client.place_order(
                symbol=symbol,
                direction=direction,
                order_Category=order_cat,
                volume_lots=vol,
                price=round(order_price, digits),
                sl=round(broker_sl, digits) if broker_sl else 0.0,
                tp=round(broker_tp, digits) if broker_tp else 0.0,
                magic=magic,
                comment=f"P{provider_id}_{lbl}_G{next_gid}",
            )
            if res and res.get("ticket"):
                t_id = int(res["ticket"])
                placed_tickets.append(t_id)
                results.append(
                    {
                        "ticket": t_id,
                        "symbol": symbol,
                        "volume": vol,
                        "tier": lbl,
                        "action": res.get("action", "FILLED"),
                        "price": res.get("price", round(order_price, digits)),
                        "account_id": self.account_id,
                        "platform": "ctrader",
                    }
                )
                print(f"[{self.account_id}] cTrader Order placed for {lbl}: Ticket={t_id}, Vol={vol}")
            else:
                ct_err = self.client.last_error()
                tier_errors.append(f"{lbl}: {ct_err}")
                print(f"[{self.account_id}] [ERROR] cTrader Order failed for {lbl}: {ct_err}")

        if placed_tickets:
            self.trade_counter += 1
            if message_id:
                self.message_to_tickets[int(message_id)] = placed_tickets

            tickets_info = {}
            for res_item in results:
                t_id = res_item["ticket"]
                lbl = res_item["tier"]
                target_tp = tp1 if lbl == "TP1" else (tp2 if lbl == "TP2" else tp3)
                tickets_info[str(t_id)] = {
                    "ticket": t_id,
                    "tier": lbl,
                    "volume": res_item["volume"],
                    "entry_price": res_item["price"],
                    "action": res_item.get("action", "FILLED"),
                    "open_time": int(time.time()),
                    "target_tp": target_tp,
                    "sl": sl_price,
                    "magic": magic
                }

            actual_group_entry = results[0]["price"] if results and results[0].get("price") else curr_price
            self.active_trade_groups[self.trade_counter] = {
                "symbol": symbol,
                "provider_id": provider_id,
                "direction": direction,
                "entry": actual_group_entry,
                "sl_price": sl_price,
                "tp1_price": tp1,
                "tp2_price": tp2,
                "tp3_price": tp3,
                "tp1_ticket": placed_tickets[0] if len(placed_tickets) > 0 else None,
                "tp2_ticket": placed_tickets[1] if len(placed_tickets) > 1 else None,
                "tp3_ticket": placed_tickets[2] if len(placed_tickets) > 2 else None,
                "tickets_info": tickets_info,
                "settings": settings,
                "status": "OPEN",
                "created_at": int(time.time()),
            }
            self.save_state()
            self._set_diag("COPIED", f"Placed {len(placed_tickets)} order(s) on {symbol} via cTrader.", {"tickets": placed_tickets, "symbol": symbol, "tier_errors": tier_errors})
        else:
            self._set_diag("ORDER_SEND_FAILED", "; ".join(tier_errors) if tier_errors else f"All cTrader order tiers aborted/failed for {symbol}.", {"symbol": symbol, "tier_errors": tier_errors})

        return results

    def close_all_positions(self, provider_id=None) -> list:
        magic = (self.magic_base + int(provider_id)) if provider_id else None
        positions = self.client.get_open_positions(magic=magic)
        results = []
        for pos in positions:
            if self.client.close_position(pos.ticket, pos.volume, pos.symbol):
                results.append({"ticket": pos.ticket, "status": "CLOSED", "profit": pos.profit})
        return results

    def close_and_cancel_all(self, provider_id=None, provider_ids=None) -> dict:
        target_pids = []
        if provider_id:
            target_pids = [str(provider_id)]
        elif provider_ids:
            target_pids = [str(p) for p in provider_ids]

        allowed_magics = None
        if target_pids:
            allowed_magics = set(self.magic_base + int(p) for p in target_pids if str(p).isdigit())

        closed_positions = []
        positions = self.client.get_open_positions()
        for pos in positions:
            if allowed_magics is not None and pos.magic not in allowed_magics:
                continue
            if self.client.close_position(pos.ticket, pos.volume, pos.symbol):
                closed_positions.append({"ticket": pos.ticket, "status": "CLOSED", "profit": pos.profit, "symbol": pos.symbol})

        cancelled_orders = []
        orders = self.client.get_pending_orders()
        for o in orders:
            o_magic = int(o.get("magic") or 0)
            if allowed_magics is not None and o_magic not in allowed_magics:
                continue
            ticket = int(o.get("ticket") or o.get("order_id") or 0)
            if ticket and self.client.cancel_order(ticket):
                cancelled_orders.append(ticket)

        return {
            "closed_positions_count": len(closed_positions),
            "cancelled_orders_count": len(cancelled_orders),
            "closed_positions": closed_positions,
            "cancelled_orders": cancelled_orders
        }

    def close_trade_by_reply(self, reply_id: int) -> bool:
        val = self.message_to_tickets.get(int(reply_id)) or self.message_to_tickets.get(str(reply_id)) or []
        tickets = list(val.get("tickets", [])) if isinstance(val, dict) else list(val)
        if not tickets:
            return False
        positions = {p.ticket: p for p in self.client.get_open_positions()}
        success = True
        for t in tickets:
            pos = positions.get(int(t))
            if pos:
                if not self.client.close_position(pos.ticket, pos.volume, pos.symbol):
                    success = False
        return success

    def cancel_trade_by_reply(self, reply_id: int) -> bool:
        val = self.message_to_tickets.get(int(reply_id)) or self.message_to_tickets.get(str(reply_id)) or []
        tickets = list(val.get("tickets", [])) if isinstance(val, dict) else list(val)
        if not tickets:
            return False
        success = True
        for t in tickets:
            if not self.client.cancel_order(int(t)):
                success = False
        return success

    def cancel_all_pending_for_provider(self, provider_id: str = None) -> bool:
        magic = (self.magic_base + int(provider_id)) if provider_id else None
        orders = self.client.get_pending_orders(magic=magic)
        if not orders:
            return False
        for o in orders:
            self.client.cancel_order(int(o["ticket"]))
        return True

    def modify_sl_by_reply(self, reply_id: int, new_sl: float) -> bool:
        val = self.message_to_tickets.get(int(reply_id)) or self.message_to_tickets.get(str(reply_id)) or []
        tickets = list(val.get("tickets", [])) if isinstance(val, dict) else list(val)
        if not tickets:
            for g in self.active_trade_groups.values():
                if g.get("status") == "OPEN":
                    for key in ["tp1_ticket", "tp2_ticket", "tp3_ticket"]:
                        t = g.get(key)
                        if t:
                            tickets.append(t)
        if not tickets:
            return False

        positions = {p.ticket: p for p in self.client.get_open_positions()}
        success = True
        for t in tickets:
            pos = positions.get(int(t))
            if pos:
                sym_info = self.client.get_symbol_info(pos.symbol)
                digits = sym_info.digits if sym_info else 2
                if not self.client.modify_position_sltp(pos.ticket, sl=new_sl, tp=pos.tp, digits=digits):
                    success = False
        return success

    def modify_signal_levels_by_msg(self, message_id: int, new_tps: list = None, new_sl: float = 0.0, new_entry: float = 0.0, provider_id: str = None, entry_min: float = None, entry_max: float = None) -> list:
        if not message_id:
            return []
        mid = int(message_id)
        new_tps = [float(x) for x in (new_tps or []) if float(x) > 0]
        new_sl = float(new_sl or 0.0)
        new_entry = float(new_entry or 0.0)

        val = self.message_to_tickets.get(mid) or self.message_to_tickets.get(str(mid)) or []
        tickets = list(val.get("tickets", [])) if isinstance(val, dict) else list(val)
        matched_grp = None
        for g in self.active_trade_groups.values():
            g_tkts = [g.get("tp1_ticket"), g.get("tp2_ticket"), g.get("tp3_ticket")]
            if tickets and any(t in tickets for t in g_tkts if t):
                matched_grp = g
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

        # Apply normalize_and_autofix_signal against live tick if group/symbol is known
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
                        "entry": new_entry if new_entry > 0 else float(matched_grp.get("entry") or 0.0),
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
        modified_results = []
        for idx, t in enumerate(tickets):
            pos = positions.get(int(t))
            if pos:
                target_tp = new_tps[idx] if idx < len(new_tps) else (new_tps[-1] if new_tps else 0.0)
                # Option 2 execution guard
                if target_tp > 0 and pos.price_open > 0 and abs(pos.price_open - target_tp) / pos.price_open > 0.12:
                    target_tp = 0.0
                sym_info = self.client.get_symbol_info(pos.symbol)
                digits = sym_info.digits if sym_info else 2
                ok = self.client.modify_position_sltp(
                    pos.ticket,
                    sl=new_sl if new_sl > 0 else pos.sl,
                    tp=target_tp if target_tp > 0 else pos.tp,
                    digits=digits
                )
                if ok:
                    if matched_grp and idx < 3:
                        matched_grp[f"tp{idx+1}_price"] = target_tp
                    modified_results.append({
                        "ticket": pos.ticket,
                        "tier": f"TP{idx+1}",
                        "action": "MODIFIED_POSITION",
                        "sl": new_sl,
                        "tp": target_tp
                    })
                    print(f"[{self.account_id}] [EDIT-MODIFY] cTrader Position #{pos.ticket} updated: SL={new_sl}, TP={target_tp}")
        if modified_results:
            self.save_state()
        return modified_results

    def set_be_for_provider(self, provider_id: str = None) -> bool:
        magic = (self.magic_base + int(provider_id)) if provider_id else None
        positions = self.client.get_open_positions(magic=magic)
        if not positions:
            return False

        success = False
        for pos in positions:
            sym_info = self.client.get_symbol_info(pos.symbol)
            digits = sym_info.digits if sym_info else 2
            tick = self.client.get_tick(pos.symbol)
            spread_val = (tick.ask - tick.bid) if tick else ((sym_info.spread * sym_info.point) if sym_info else 0.0)
            be_price = pos.price_open + spread_val if pos.type == 0 else pos.price_open - spread_val
            if pos.sl == 0 or (pos.type == 0 and be_price > pos.sl) or (pos.type == 1 and be_price < pos.sl):
                if self.client.modify_position_sltp(pos.ticket, sl=be_price, tp=pos.tp, digits=digits):
                    success = True
        return success

    def reconcile_pending_and_positions(self, positions: list, pending_orders: list, history: list) -> bool:
        """
        Reconciles cTrader PendingOrder IDs -> filled Position IDs when pending orders trigger,
        and cancels stale pending orders (>12h old) so they cannot gap-fill days later.
        Returns True if active_trade_groups state was modified.
        """
        open_tickets = {p.ticket: p for p in (positions or [])}
        pending_tickets = {int(o["ticket"]): o for o in (pending_orders or []) if o.get("ticket")}

        claimed_tickets = set()
        for g in self.active_trade_groups.values():
            for tier_name in ["tp1", "tp2", "tp3"]:
                t = g.get(f"{tier_name}_ticket")
                if t:
                    claimed_tickets.add(int(t))
            for k in (g.get("closed_tickets") or {}).keys():
                if str(k).isdigit():
                    claimed_tickets.add(int(k))

        state_changed = False
        now_ts = int(time.time())

        for group_id, group in list(self.active_trade_groups.items()):
            provider_id = str(group.get("provider_id", "1"))
            symbol = group.get("symbol", "XAUUSD")
            created_at = int(group.get("created_at") or 0)
            tickets_info = group.setdefault("tickets_info", {})
            cancelled_tickets = group.setdefault("cancelled_tickets", {})

            for tier_name in ["tp1", "tp2", "tp3"]:
                tkt = group.get(f"{tier_name}_ticket")
                if not tkt:
                    continue
                tkt = int(tkt)
                t_str = str(tkt)
                tier_lbl = tier_name.upper()
                t_info = tickets_info.setdefault(t_str, {"ticket": tkt, "tier": tier_lbl})

                if tkt in open_tickets:
                    if t_info.get("action") == "PENDING":
                        t_info["action"] = "FILLED"
                        t_info["entry_price"] = open_tickets[tkt].price_open
                        state_changed = True
                    continue

                if tkt in pending_tickets:
                    if t_info.get("action") != "PENDING":
                        t_info["action"] = "PENDING"
                        state_changed = True
                    # Expire stale pending orders older than 12 hours to prevent weekend/overnight gap hazards
                    max_ttl_sec = int(float(group.get("settings", {}).get("pending_ttl_hours", 12)) * 3600)
                    if created_at > 0 and max_ttl_sec > 0 and (now_ts - created_at) > max_ttl_sec:
                        if self.client.cancel_order(tkt):
                            cancelled_tickets[t_str] = now_ts
                            pending_tickets.pop(tkt, None)
                            state_changed = True
                            print(f"[{self.account_id}] [TTL-EXPIRED] Cancelled stale pending order #{tkt} ({symbol} {tier_lbl}, age={(now_ts - created_at)//3600}h)")
                    continue

                if t_str in cancelled_tickets or t_str in (group.get("closed_tickets") or {}):
                    continue

                # Ticket is neither in open_tickets nor pending_tickets.
                # If it was a PendingOrder, check if it triggered into a new Position ID (in open positions or history)
                exact_comm = f"P{provider_id}_{tier_lbl}_G{group_id}"
                legacy_comm = f"P{provider_id}_{tier_lbl}"

                matched_new_tkt = None
                matched_entry_price = None

                # 1. Search open positions
                for p in (positions or []):
                    if p.ticket in claimed_tickets:
                        continue
                    p_comm = str(p.comment or "")
                    if p_comm == exact_comm or (
                        p_comm == legacy_comm
                        and p.symbol == symbol
                        and (created_at == 0 or (created_at - 120) <= p.time <= (created_at + 43200))
                    ):
                        matched_new_tkt = int(p.ticket)
                        matched_entry_price = float(p.price_open)
                        break

                # 2. Search closed deal history if it filled and already closed
                if not matched_new_tkt:
                    for h in (history or []):
                        h_pos_id = int(h.get("position_id") or h.get("ticket") or 0)
                        if not h_pos_id or h_pos_id in claimed_tickets:
                            continue
                        h_comm = str(h.get("comment") or "")
                        h_close_ts = int(h.get("close_time") or 0)
                        if h_comm == exact_comm or (
                            h_comm == legacy_comm
                            and str(h.get("symbol") or "") == symbol
                            and (created_at == 0 or (created_at - 120) <= h_close_ts <= (created_at + 43200))
                        ):
                            matched_new_tkt = h_pos_id
                            matched_entry_price = float(h.get("entry_price") or group.get("entry") or 0.0)
                            break

                if matched_new_tkt:
                    claimed_tickets.add(matched_new_tkt)
                    group[f"{tier_name}_ticket"] = matched_new_tkt
                    new_info = dict(t_info)
                    new_info["ticket"] = matched_new_tkt
                    new_info["pending_order_id"] = tkt
                    new_info["action"] = "FILLED"
                    if matched_entry_price and matched_entry_price > 0:
                        new_info["entry_price"] = matched_entry_price
                        if tier_name == "tp1":
                            group["entry"] = matched_entry_price
                    tickets_info.pop(t_str, None)
                    tickets_info[str(matched_new_tkt)] = new_info

                    # Update message_to_tickets mapping as well
                    for m_key, m_val in list(self.message_to_tickets.items()):
                        if isinstance(m_val, list) and tkt in m_val:
                            self.message_to_tickets[m_key] = [matched_new_tkt if x == tkt else x for x in m_val]
                    state_changed = True
                    print(f"[{self.account_id}] [RECONCILED] Pending order #{tkt} filled as Position #{matched_new_tkt} ({symbol} {tier_lbl})")

        return state_changed

    def sync_closed_trades_to_db(self, positions: list = None, pending_orders: list = None, history: list = None):
        """Synchronizes closed positions and signals from active_trade_groups and cTrader deal history into stats.db."""
        try:
            import db
            if not self.client.is_authorized:
                return

            if positions is None:
                positions = self.client.get_open_positions()
            if pending_orders is None:
                pending_orders = self.client.get_pending_orders(use_cached=True)
            if history is None:
                history = self.client.get_history(300)

            reconciled = self.reconcile_pending_and_positions(positions, pending_orders, history)

            open_tickets = {p.ticket: p for p in (positions or [])}
            pending_tickets = {int(o["ticket"]): o for o in (pending_orders or []) if o.get("ticket")}

            hist_by_ticket = {}
            for h in (history or []):
                t_key = h.get("position_id") or h.get("ticket") or h.get("deal_id")
                if t_key:
                    hist_by_ticket[int(t_key)] = h

            reset_ts = int(float(db.get_metadata(f"reset_time_{self.account_id}") or 0))
            state_changed = reconciled

            for group_id, group in list(self.active_trade_groups.items()):
                group.setdefault("closed_tickets", {})
                cancelled_tickets = group.setdefault("cancelled_tickets", {})
                provider_id = str(group.get("provider_id", "1"))
                symbol = group.get("symbol", "XAUUSD")
                direction = str(group.get("direction", "BUY")).upper()
                entry = float(group.get("entry", 0.0))
                sl_price = float(group.get("sl_price") or 0.0)
                magic = self.magic_base + int(provider_id) if provider_id.isdigit() else self.magic_base
                sig_id = f"P{provider_id}_T{group_id}"

                tickets_info = group.get("tickets_info", {})
                group_tickets = []
                for tier_name in ["tp1", "tp2", "tp3"]:
                    tkt = group.get(f"{tier_name}_ticket")
                    if tkt:
                        group_tickets.append((int(tkt), tier_name.upper(), float(group.get(f"{tier_name}_price") or 0.0)))

                if not group_tickets:
                    continue

                any_open = False
                for tkt, tier_lbl, target_tp in group_tickets:
                    t_str = str(tkt)
                    t_info = tickets_info.get(t_str, {})
                    vol = float(t_info.get("volume") or t_info.get("lots") or 0.01)

                    # Active open position or active pending order -> NOT closed!
                    if tkt in open_tickets or tkt in pending_tickets:
                        any_open = True
                        if t_str in group["closed_tickets"]:
                            del group["closed_tickets"][t_str]
                            state_changed = True
                        continue

                    if t_str in cancelled_tickets:
                        continue

                    if t_str in group["closed_tickets"]:
                        continue

                    h_deal = hist_by_ticket.get(tkt)
                    if not h_deal:
                        # If this ticket was a pending order (or >= 2000000 PendingOrder ID) that disappeared without filling in history,
                        # it was cancelled/expired unfilled -> do NOT fabricate a closed trade!
                        if t_info.get("action") == "PENDING" or tkt >= 2000000:
                            cancelled_tickets[t_str] = int(time.time())
                            state_changed = True
                            continue
                        # Even for market positions, only record once confirmed in cTrader deal history
                        continue

                    close_price = float(h_deal.get("close_price") or h_deal.get("price_close") or target_tp or entry)
                    open_price_val = float(h_deal.get("entry_price") or t_info.get("entry_price") or entry)
                    close_time = int(h_deal.get("close_time") or time.time())
                    profit = round(float(h_deal.get("profit") or 0.0), 2)
                    vol = float(h_deal.get("volume") or vol)

                    if profit > 0:
                        reason = tier_lbl
                    elif group.get("tp1_processed") and sl_price > 0 and open_price_val > 0 and abs(close_price - open_price_val) <= abs(open_price_val - sl_price) * 0.25:
                        reason = "BE"
                    else:
                        reason = "SL"

                    group["closed_tickets"][t_str] = {
                        "ticket": tkt,
                        "close_price": close_price,
                        "close_time": close_time,
                        "profit": profit,
                        "lots": vol,
                        "reason": reason
                    }
                    state_changed = True

                    if close_time >= reset_ts:
                        db.save_trade(
                            ticket=tkt,
                            provider_id=provider_id,
                            symbol=symbol,
                            trade_type=direction,
                            lots=vol,
                            open_time=int(group.get("created_at") or close_time),
                            close_time=close_time,
                            open_price=open_price_val,
                            close_price=close_price,
                            profit=profit,
                            magic=magic,
                            close_reason=reason,
                            account_id=self.account_id
                        )
                        print(f"[{self.account_id}] Synced closed trade to DB: Ticket={tkt}, P={provider_id}, {tier_lbl}, Profit=${profit:.2f}, Reason={reason}")

                sig_status = "OPEN" if any_open else "CLOSED"
                if group.get("status") != sig_status:
                    group["status"] = sig_status
                    state_changed = True

                if group["closed_tickets"]:
                    valid_closed = [c for c in group["closed_tickets"].values() if int(c.get("close_time") or 0) >= reset_ts]
                    if valid_closed:
                        tot_profit = round(sum(float(c["profit"]) for c in valid_closed), 2)
                        tot_lots = round(sum(float(c.get("lots") or 0.01) for c in valid_closed), 2)
                        reasons = {c.get("reason", "") for c in valid_closed}
                        latest_close = max(int(c.get("close_time", 0)) for c in valid_closed)
                        created_at = int(group.get("created_at") or latest_close)

                        db.save_or_update_signal(
                            signal_id=sig_id,
                            provider_id=provider_id,
                            symbol=symbol,
                            trade_type=direction,
                            lots=tot_lots,
                            tp1_hit=1 if ("TP1" in reasons) else 0,
                            tp2_hit=1 if ("TP2" in reasons) else 0,
                            tp3_hit=1 if ("TP3" in reasons) else 0,
                            sl_hit=1 if ("SL" in reasons) else 0,
                            profit=tot_profit,
                            status=sig_status,
                            created_at=created_at,
                            closed_at=latest_close if sig_status == "CLOSED" else 0,
                            account_id=self.account_id
                        )

            # Also ensure any real cTrader closed deals since reset_ts with a Sirius provider tag are in stats.db
            try:
                conn = db.get_db()
                existing_rows = conn.execute("SELECT ticket FROM trades WHERE account_id = ?", (self.account_id,)).fetchall()
                existing_tickets = {int(r["ticket"]) for r in existing_rows}
                conn.close()

                for h in (history or []):
                    c_time = int(h.get("close_time") or 0)
                    if c_time < reset_ts:
                        continue
                    pos_id = int(h.get("position_id") or h.get("ticket") or 0)
                    if not pos_id or pos_id in existing_tickets:
                        continue
                    comm = str(h.get("comment") or "")
                    lbl = str(h.get("label") or "")
                    pid = None
                    tier_lbl = "TP1"
                    if comm.startswith("P") and "_" in comm:
                        parts = comm[1:].split("_")
                        if parts[0].isdigit():
                            pid = parts[0]
                        if len(parts) > 1 and parts[1].startswith("TP"):
                            tier_lbl = parts[1]
                    if not pid and lbl.startswith("M") and lbl[1:].isdigit():
                        mag_val = int(lbl[1:])
                        if 400000 < mag_val < 500000:
                            pid = str(mag_val - 400400 if mag_val > 400400 else mag_val - 400000)
                    if not pid:
                        continue
                    prof = round(float(h.get("profit") or 0.0), 2)
                    reason = tier_lbl if prof > 0 else "SL"
                    magic_val = self.magic_base + int(pid) if pid.isdigit() else self.magic_base
                    db.save_trade(
                        ticket=pos_id,
                        provider_id=pid,
                        symbol=str(h.get("symbol") or "XAUUSD"),
                        trade_type="BUY" if int(h.get("type") or 0) == 0 else "SELL",
                        lots=float(h.get("volume") or 0.01),
                        open_time=c_time,
                        close_time=c_time,
                        open_price=float(h.get("entry_price") or h.get("close_price") or 0.0),
                        close_price=float(h.get("close_price") or 0.0),
                        profit=prof,
                        magic=magic_val,
                        close_reason=reason,
                        account_id=self.account_id
                    )
                    existing_tickets.add(pos_id)
                    state_changed = True
            except Exception as _he:
                print(f"[{self.account_id}] Orphan history sync notice: {_he}")

            if state_changed:
                self.save_state()
                try:
                    import provider_analytics as pa
                    pa.sync_from_trades_db(reset_ts=reset_ts)
                except Exception as _sync_e:
                    print(f"[{self.account_id}] pa.sync_from_trades_db error: {_sync_e}")
        except Exception as e:
            print(f"[{self.account_id}] sync_closed_trades_to_db exception: {e}\n{traceback.format_exc()}")

    def manage_trailing_stops(self):
        if not self.client.is_authorized:
            return

        positions = self.client.get_open_positions()
        pending_orders = self.client.get_pending_orders(use_cached=True)
        history = self.client.get_history(300)

        state_changed = self.reconcile_pending_and_positions(positions, pending_orders, history)

        open_tickets = {p.ticket: p for p in (positions or [])}
        pending_tickets = {int(o["ticket"]): o for o in (pending_orders or []) if o.get("ticket")}

        for group_id, group in list(self.active_trade_groups.items()):
            symbol = group.get("symbol", "XAUUSD")
            sym_info = self.client.get_symbol_info(symbol)
            digits = sym_info.digits if sym_info else 2

            tp1_ticket = group.get("tp1_ticket")
            tp2_ticket = group.get("tp2_ticket")
            tp3_ticket = group.get("tp3_ticket")

            tp1_open = tp1_ticket in open_tickets if tp1_ticket else False
            tp2_open = tp2_ticket in open_tickets if tp2_ticket else False
            tp3_open = tp3_ticket in open_tickets if tp3_ticket else False

            tp1_pending = tp1_ticket in pending_tickets if tp1_ticket else False
            tp2_pending = tp2_ticket in pending_tickets if tp2_ticket else False
            tp3_pending = tp3_ticket in pending_tickets if tp3_ticket else False

            any_active = (tp1_open or tp2_open or tp3_open or tp1_pending or tp2_pending or tp3_pending)

            if any_active:
                if group.get("status") != "OPEN":
                    group["status"] = "OPEN"
                    state_changed = True
            else:
                if group.get("status") != "CLOSED":
                    group["status"] = "CLOSED"
                    state_changed = True

            # Post-Fill Market Gap & Missing-SL Protection for open positions in this group
            sl_price = float(group.get("sl_price") or 0.0)
            entry = float(group.get("entry") or 0.0)
            intended_sl_dist = abs(entry - sl_price) if (entry > 0 and sl_price > 0) else 0.0

            for t in [tp1_ticket, tp2_ticket, tp3_ticket]:
                if not t or t not in open_tickets:
                    continue
                pos = open_tickets[t]
                if sl_price > 0 and (not pos.sl or pos.sl <= 0):
                    # Position filled on a gap where cTrader stripped the SL
                    valid_sl = (pos.type == 0 and sl_price < pos.price_current) or (pos.type == 1 and sl_price > pos.price_current)
                    if valid_sl:
                        self.client.modify_position_sltp(pos.ticket, sl=sl_price, tp=pos.tp, digits=digits)
                    elif intended_sl_dist > 0:
                        # Price gapped beyond original SL on fill -> apply emergency SL at intended distance or close immediately
                        emerg_sl = round(pos.price_open - intended_sl_dist, digits) if pos.type == 0 else round(pos.price_open + intended_sl_dist, digits)
                        if (pos.type == 0 and pos.price_current <= emerg_sl) or (pos.type == 1 and pos.price_current >= emerg_sl):
                            self.client.close_position(pos.ticket, pos.volume, pos.symbol)
                            print(f"[{self.account_id}] [GAP-GUARD] Closed unprotected gap-filled position #{pos.ticket} ({pos.symbol})")
                        else:
                            self.client.modify_position_sltp(pos.ticket, sl=emerg_sl, tp=pos.tp, digits=digits)
                            print(f"[{self.account_id}] [GAP-GUARD] Applied emergency SL {emerg_sl} to gap-filled position #{pos.ticket}")
                elif pos.sl and pos.sl > 0 and intended_sl_dist > 0:
                    actual_sl_dist = abs(pos.price_open - pos.sl)
                    if actual_sl_dist > intended_sl_dist * 2.2:
                        # Favorable gap fill stretched SL distance >2.2x intended risk -> clamp SL to intended distance from fill
                        clamped_sl = round(pos.price_open - intended_sl_dist, digits) if pos.type == 0 else round(pos.price_open + intended_sl_dist, digits)
                        if (pos.type == 0 and clamped_sl < pos.price_current and clamped_sl > pos.sl) or (pos.type == 1 and clamped_sl > pos.price_current and clamped_sl < pos.sl):
                            self.client.modify_position_sltp(pos.ticket, sl=clamped_sl, tp=pos.tp, digits=digits)
                            print(f"[{self.account_id}] [GAP-GUARD] Clamped stretched SL on #{pos.ticket} from {pos.sl} to {clamped_sl} (intended dist={intended_sl_dist:.2f})")

            settings = group.get("settings", {})
            be_at = settings.get("sl_breakeven_at", "TP1")
            cancelled_tickets = group.get("cancelled_tickets") or {}
            tp1_was_cancelled = str(tp1_ticket) in cancelled_tickets if tp1_ticket else False

            if not tp1_open and not tp1_pending and not tp1_was_cancelled and tp1_ticket and not group.get("tp1_processed", False):
                group["tp1_processed"] = True
                state_changed = True
                if be_at == "TP1":
                    tick = self.client.get_tick(symbol)
                    spread_val = (tick.ask - tick.bid) if tick else ((sym_info.spread * sym_info.point) if sym_info else 0.0)
                    for t in [tp2_ticket, tp3_ticket]:
                        if t and t in open_tickets:
                            pos = open_tickets[t]
                            base_entry = pos.price_open if pos.price_open > 0 else entry
                            be_price = base_entry + spread_val if pos.type == 0 else base_entry - spread_val
                            if pos.sl == 0 or (pos.type == 0 and be_price > pos.sl) or (pos.type == 1 and be_price < pos.sl):
                                self.client.modify_position_sltp(pos.ticket, sl=be_price, tp=pos.tp, digits=digits)

        if state_changed:
            self.save_state()

        # Always sync closed trades with DB reusing the state already fetched
        self.sync_closed_trades_to_db(positions=positions, pending_orders=pending_orders, history=history)


def run_ctrader_worker_process(account_config: dict, in_q: Queue, out_q: Queue):
    worker = CTraderAccountWorker(account_config, in_q, out_q)
    worker.run()
