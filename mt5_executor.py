"""
mt5_executor.py — Connects to MetaTrader 5, places trades,
manages positions, and handles order types.
"""

import MetaTrader5 as mt5
import threading
import time
import json
import os
import config

STATE_FILE = "trade_state.json"

trade_counter = 0
message_to_tickets = {}
active_trade_groups = {}
_exec_lock = threading.Lock()
_recent_exec_fingerprints = {}  # (provider_id, symbol, direction, entry, sl, tp1, tp2, tp3) -> float timestamp
_last_exec_diagnostics = {}  # (account_id, str(msg_id), str(prov_id)) -> dict


def _record_primary_diag(message_id, provider_id, status: str, reason: str, extra: dict = None):
    key = ("acc_1", str(message_id or 0), str(provider_id or ""))
    _last_exec_diagnostics[key] = {
        "account_id": "acc_1",
        "status": str(status),
        "reason": str(reason),
        "extra": extra or {},
        "timestamp": time.time(),
    }
    if len(_last_exec_diagnostics) > 120:
        _last_exec_diagnostics.pop(next(iter(_last_exec_diagnostics)), None)


def get_last_exec_diagnostics(message_id=None, provider_id=None) -> dict:
    """Returns per-account diagnostics collected from the latest execution run."""
    out = {}
    try:
        from account_manager import AccountManager
        acc_mgr = AccountManager.get_instance()
        for aid, d in (getattr(acc_mgr, "last_dispatch_diagnostics", None) or {}).items():
            out[aid] = d
    except Exception:
        pass
    key = ("acc_1", str(message_id or 0), str(provider_id or ""))
    if key in _last_exec_diagnostics:
        out["acc_1"] = _last_exec_diagnostics[key]
    return out

def load_state():
    global message_to_tickets, active_trade_groups, trade_counter
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                data = json.load(f)
                message_to_tickets = {int(k): v for k, v in data.get("message_to_tickets", {}).items()}
                active_trade_groups = {int(k): v for k, v in data.get("active_trade_groups", {}).items()}
                trade_counter = data.get("trade_counter", 0)
        except Exception as e:
            print(f"[ERROR] Failed to load trade state: {e}")

def save_state():
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump({
                "message_to_tickets": message_to_tickets,
                "active_trade_groups": active_trade_groups,
                "trade_counter": trade_counter
            }, f, indent=4)
    except Exception as e:
        print(f"[ERROR] Failed to save trade state: {e}")

load_state()

SETTINGS_FILE = "providers_config.json"

def get_provider_settings(provider_id: str = "1") -> dict:
    """Load real-time provider rules configured from dashboard."""
    defaults = {
        "name": "Sirius Copier (Default)",
        "risk_mode": "percent",
        "risk_value": 1.0,
        "tp1_percent": 60.0,
        "tp2_percent": 30.0,
        "tp3_percent": 10.0,
        "tp1_close_percent": 60.0,
        "sl_tp1_strategy": "ENTRY",
        "sl_tp2_strategy": "TP1",
        "sl_tp3_strategy": "TP2",
        "sl_breakeven_at": "TP1",
        "spread_offset_entry": True,
        "spread_offset_tp": True,
        "spread_offset_sl": True,
        "entry_zone_mode": "mid",
        "max_lot": 10.0
    }
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                prov = data.get(str(provider_id), {})
                merged = {**defaults, **prov}
                try:
                    from account_manager import AccountManager
                    acc_mgr = AccountManager.get_instance()
                    overrides = acc_mgr.accounts_config.get("acc_1", {}).get("provider_settings", {}).get(str(provider_id), {})
                    if overrides:
                        merged = {**merged, **overrides}
                except Exception:
                    pass
                return merged
        except Exception as e:
            print(f"[WARN] Failed to read {SETTINGS_FILE}: {e}")
    return defaults


def is_provider_enabled_for_primary_mt5(provider_id: str) -> bool:
    """Checks whether provider_id is assigned to the primary MT5 account (acc_1)."""
    try:
        from account_manager import AccountManager
        acc_mgr = AccountManager.get_instance()
        acc_mgr.load_accounts()
        acc1 = acc_mgr.accounts_config.get("acc_1", {})
        if not acc1.get("enabled", True):
            return False
        return acc_mgr.is_provider_assigned_to_account("acc_1", provider_id)
    except Exception:
        return True

def connect():
    """Initialize MT5 and log in, plus launch any cTrader / multi-account workers."""
    try:
        from account_manager import AccountManager
        AccountManager.get_instance().start_all_workers(skip_primary_mt5=True)
    except Exception as e:
        print(f"[WARN] Could not start secondary/cTrader workers: {e}")

    if config.MT5_PATH:
        init_success = mt5.initialize(path=config.MT5_PATH)
    else:
        init_success = mt5.initialize()

    if not init_success:
        print(f"[ERROR] MT5 initialize failed: {mt5.last_error()}")
        try:
            from account_manager import AccountManager
            if AccountManager.get_instance().workers:
                print("[INFO] Running with active cTrader / secondary account worker(s).")
                return True
        except Exception:
            pass
        return False

    if not mt5.login(config.MT5_ACCOUNT, password=config.MT5_PASSWORD, server=config.MT5_SERVER):
        print(f"[ERROR] MT5 login failed: {mt5.last_error()}")
        mt5.shutdown()
        try:
            from account_manager import AccountManager
            if AccountManager.get_instance().workers:
                print("[INFO] Running with active cTrader / secondary account worker(s).")
                return True
        except Exception:
            pass
        return False

    global mt5_symbol_map
    mt5_symbol_map = {}
    symbols = mt5.symbols_get()
    if symbols:
        tradeable_names = [s.name for s in symbols if getattr(s, 'trade_mode', 0) != 0]
        try:
            from symbol_mapper import SymbolMapper
            SymbolMapper.build_account_symbol_map("acc_1", tradeable_names, platform="mt5")
        except Exception as e:
            print(f"[MT5] SymbolMapper init notice: {e}")
        for s in symbols:
            if getattr(s, 'trade_mode', 0) == 0:
                continue
            base_name = s.name.split('.')[0].replace('-', '').replace('_', '').upper()
            mt5_symbol_map[base_name] = s.name
            if base_name == "XAUUSD":
                mt5_symbol_map["GOLD"] = s.name
                mt5_symbol_map["XAU"] = s.name
    return True

def disconnect():
    try:
        from account_manager import AccountManager
        AccountManager.get_instance().stop_all_workers()
    except Exception:
        pass
    mt5.shutdown()

def is_connected():
    info = mt5.account_info()
    if info is None:
        try:
            if config.MT5_PATH:
                mt5.initialize(path=config.MT5_PATH)
            else:
                mt5.initialize()
            if config.MT5_ACCOUNT and config.MT5_PASSWORD:
                mt5.login(config.MT5_ACCOUNT, password=config.MT5_PASSWORD, server=config.MT5_SERVER)
            info = mt5.account_info()
        except:
            pass
    return info is not None

def reconnect():
    disconnect()
    time.sleep(2)
    return connect()

def get_account_info():
    return mt5.account_info()

def get_tick(symbol_name):
    return mt5.symbol_info_tick(symbol_name)

def get_symbol_info(symbol_name):
    return mt5.symbol_info(symbol_name)

def get_open_positions(symbol_name=None, magic=None):
    if isinstance(symbol_name, (int, float)):
        magic = int(symbol_name)
        symbol_name = None
    elif isinstance(symbol_name, str) and symbol_name.isdigit():
        magic = int(symbol_name)
        symbol_name = None

    if symbol_name:
        positions = mt5.positions_get(symbol=symbol_name)
    else:
        positions = mt5.positions_get()
    if positions is None:
        return []
    if magic is not None:
        return [p for p in positions if p.magic == magic]
    # Otherwise return any positions matching any known bot magic
    bot_magics = [config.MAGIC_NUMBER]
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                for pid, pdata in data.items():
                    m = pdata.get("magic_number") or (400400 + int(pid))
                    bot_magics.append(int(m))
        except:
            pass
    return [p for p in positions if p.magic in bot_magics]

def execute_signal(signal: dict, message_id: int = None, provider_id: str = "1") -> list:
    with _exec_lock:
        return _execute_signal_locked(signal, message_id, provider_id)


def _execute_signal_locked(signal: dict, message_id: int = None, provider_id: str = "1") -> list:
    import copy
    signal = copy.deepcopy(signal) if isinstance(signal, dict) else {}

    # Fan out to any active cTrader / secondary account workers in parallel
    worker_results = []
    try:
        from account_manager import AccountManager
        acc_mgr = AccountManager.get_instance()
        acc_mgr.load_accounts()
        acc_mgr.ensure_workers_running(skip_primary_mt5=True)
        if acc_mgr.workers:
            worker_results = acc_mgr.dispatch_signal_and_collect(provider_id, copy.deepcopy(signal), message_id)
    except Exception as e:
        print(f"[WARN] Secondary/cTrader worker dispatch error: {e}")

    # Only execute on primary MT5 (acc_1) if this provider is assigned to acc_1
    if not is_provider_enabled_for_primary_mt5(provider_id):
        _record_primary_diag(message_id, provider_id, "NOT_ASSIGNED", f"Provider {provider_id} is not assigned to primary MT5 (acc_1).")
        return worker_results

    # Check if primary MT5 account (acc_1) is specifically paused
    try:
        from account_manager import AccountManager
        acc_mgr = AccountManager.get_instance()
        if acc_mgr.is_account_paused("acc_1"):
            print(f"[PAUSED] Primary MT5 (acc_1) is PAUSED. Skipping MT5 execution.")
            _record_primary_diag(message_id, provider_id, "PAUSED", "Primary MT5 (acc_1) is paused by user.")
            return worker_results
    except Exception as e:
        print(f"[WARN] Error checking MT5 pause state: {e}")

    # Primary MT5 deduplication sanity check:
    if message_id is not None and int(message_id) in message_to_tickets and message_to_tickets[int(message_id)]:
        mid_int = int(message_id)
        existing_tkts = message_to_tickets[mid_int]
        has_live_or_filled = False
        try:
            open_pos_tkts = {p.ticket for p in (get_open_positions() or [])}
            pend_ord_tkts = {o.ticket for o in (mt5.orders_get() or [])}
            for tkt in existing_tkts:
                if tkt in open_pos_tkts or tkt in pend_ord_tkts:
                    has_live_or_filled = True
                    break
                deals = mt5.history_deals_get(position=tkt) or mt5.history_deals_get(ticket=tkt)
                if deals and len(deals) > 0:
                    has_live_or_filled = True
                    break
            for g in active_trade_groups.values():
                if g.get("message_id") == mid_int and g.get("status") not in ("CLOSED", "CANCELLED"):
                    has_live_or_filled = True
                    break
        except Exception:
            has_live_or_filled = True

        if has_live_or_filled:
            print(f"[acc_1] [SANITY-GUARD] Message ID {message_id} already has active/placed tickets on Primary MT5 {existing_tkts}. Skipping duplicate execution on Primary MT5.")
            _record_primary_diag(message_id, provider_id, "ALREADY_EXECUTED", f"Message {message_id} already has active/filled tickets {existing_tkts}.")
            return worker_results
        else:
            print(f"[acc_1] [RE-EXEC] Message ID {message_id} had previously cancelled/unfilled pending orders {existing_tkts}. Allowing fresh execution on edit.")
            message_to_tickets.pop(mid_int, None)

    if not is_connected():
        if not reconnect():
            print("[ERROR] Cannot reconnect to MT5")
            _record_primary_diag(message_id, provider_id, "DISCONNECTED", "Primary MT5 (acc_1) terminal is disconnected and reconnect failed.")
            return worker_results

    direction = signal.get("direction", "")
    order_type = signal.get("order_type", "")
    entry = signal.get("entry", 0)
    sl = signal.get("sl", 0)
    tp_list = list(signal.get("tp", []))

    if direction == "CLOSE" or order_type == "CLOSE":
        return close_all_positions(provider_id=provider_id)

    if not tp_list:
        tp_list = [0]

    raw_inst = signal.get("instrument") or signal.get("symbol") or ""
    instrument = raw_inst.upper().replace("/", "").replace("-", "").replace("_", "").replace("#", "").strip()
    if not instrument:
        err = "Signal did not contain a valid instrument. Aborting execution."
        print(f"[BLOCKED] {err}")
        _record_primary_diag(message_id, provider_id, "BLOCKED_INVALID_SIGNAL", err)
        try:
            from trade_logger import log_error
            log_error("INVALID_SIGNAL", err, provider_id=provider_id, raw_message=signal.get("raw_message", ""))
        except Exception:
            pass
        return worker_results

    # Strict symbol matching: verify symbol exists in SymbolMapper, mt5_symbol_map or broker catalog
    actual_symbol = None
    try:
        from symbol_mapper import SymbolMapper
        actual_symbol = SymbolMapper.resolve_symbol("acc_1", raw_inst) or SymbolMapper.resolve_symbol("acc_1", instrument)
    except Exception:
        pass
    if not actual_symbol:
        actual_symbol = mt5_symbol_map.get(instrument)
    if not actual_symbol:
        raw_clean = raw_inst.strip()
        if raw_clean in mt5_symbol_map:
            actual_symbol = mt5_symbol_map[raw_clean]
        elif mt5.symbol_info(raw_clean) is not None:
            actual_symbol = raw_clean
        elif mt5.symbol_info(instrument) is not None:
            actual_symbol = instrument
        else:
            actual_symbol = None

    sym_info = get_symbol_info(actual_symbol) if actual_symbol else None
    if not actual_symbol or sym_info is None:
        err = f"Symbol '{raw_inst}' (resolved: '{actual_symbol}') not found in MT5. Strict matching enforced: trade skipped."
        print(f"[BLOCKED] {err}")
        _record_primary_diag(message_id, provider_id, "BLOCKED_SYMBOL_NOT_FOUND", err, {"raw_symbol": raw_inst, "resolved": actual_symbol})
        try:
            from trade_logger import log_error
            log_error("SYMBOL_NOT_FOUND", err, provider_id=provider_id, symbol=actual_symbol or raw_inst, raw_message=signal.get("raw_message", ""))
        except Exception:
            pass
        return worker_results

    # Safety Rule 3: No Stop Loss, No Trade
    if direction != "CLOSE" and (not sl or float(sl) <= 0):
        err = "Signal rejected: Missing mandatory Stop Loss (SL <= 0). No stop loss, no trade rule enforced."
        print(f"[BLOCKED] {err}")
        _record_primary_diag(message_id, provider_id, "BLOCKED_MISSING_SL", err, {"symbol": actual_symbol})
        try:
            from trade_logger import log_error
            log_error("MISSING_STOP_LOSS", err, provider_id=provider_id, symbol=actual_symbol or raw_inst, raw_message=signal.get("raw_message", ""))
        except Exception:
            pass
        return worker_results

    if not mt5.symbol_select(actual_symbol, True):
        err = f"Failed to select symbol {actual_symbol} in Market Watch."
        print(f"[ERROR] {err}")
        _record_primary_diag(message_id, provider_id, "ERROR_SYMBOL_SELECT", err, {"symbol": actual_symbol})
        return worker_results

    tick = get_tick(actual_symbol)
    if tick is None:
        err = f"Could not retrieve live tick for {actual_symbol}."
        print(f"[ERROR] {err}")
        _record_primary_diag(message_id, provider_id, "ERROR_NO_TICK", err, {"symbol": actual_symbol})
        return worker_results

    digits = sym_info.digits

    # Apply shared direction/SL/hundreds-digit auto-fix BEFORE sanity guards (also shifts entry_min/entry_max)
    try:
        from signal_parser import normalize_and_autofix_signal
        signal = normalize_and_autofix_signal(signal, tick.bid, tick.ask, digits)
        direction = signal.get("direction", direction)
        entry = float(signal.get("entry") or 0)
        sl = float(signal.get("sl") or 0)
        tp_list = list(signal.get("tp") or tp_list)
        if not tp_list:
            tp_list = [0]
    except Exception as _af_e:
        print(f"[WARN] normalize_and_autofix_signal error: {_af_e}")

    live_price = tick.ask if direction == "BUY" else tick.bid
    if not live_price or live_price <= 0:
        print(f"[BLOCKED] Invalid live price {live_price} for {actual_symbol}.")
        return worker_results

    # Rule 1: Price Consistency Sanity Guard (prevent mismatched prices executing on wrong symbol)
    if sl > 0:
        sl_pct_diff = abs(live_price - sl) / live_price
        if sl_pct_diff > 0.20:
            err = f"Signal SL {sl} deviates {sl_pct_diff*100:.1f}% from live market price {live_price:.2f} for {actual_symbol}. Price/symbol mismatch! Aborting."
            print(f"[BLOCKED] {err}")
            _record_primary_diag(message_id, provider_id, "BLOCKED_PRICE_MISMATCH", err, {"symbol": actual_symbol, "sl": sl, "live_price": live_price})
            try:
                from trade_logger import log_error
                log_error("PRICE_MISMATCH", err, provider_id=provider_id, symbol=actual_symbol, raw_message=signal.get("raw_message", ""))
            except Exception:
                pass
            return worker_results
    if entry > 0:
        ent_pct_diff = abs(live_price - entry) / live_price
        if ent_pct_diff > 0.20:
            err = f"Signal Entry {entry} deviates {ent_pct_diff*100:.1f}% from live market price {live_price:.2f} for {actual_symbol}. Price/symbol mismatch! Aborting."
            print(f"[BLOCKED] {err}")
            _record_primary_diag(message_id, provider_id, "BLOCKED_PRICE_MISMATCH", err, {"symbol": actual_symbol, "entry": entry, "live_price": live_price})
            try:
                from trade_logger import log_error
                log_error("PRICE_MISMATCH", err, provider_id=provider_id, symbol=actual_symbol, raw_message=signal.get("raw_message", ""))
            except Exception:
                pass
            return worker_results

    ref_price = entry if entry > 0 else (tick.ask if direction == "BUY" else tick.bid)
    
    # Load dynamic provider rules from dashboard settings
    prov_settings = get_provider_settings(provider_id)
    if not prov_settings.get("trading_enabled", True):
        print(f"[BLOCKED] Provider {provider_id} trading is disabled in settings. Skipping order execution.")
        _record_primary_diag(message_id, provider_id, "TRADING_DISABLED", f"Provider {provider_id} trading_enabled is False.")
        return worker_results
    risk_mode = prov_settings.get("risk_mode", "percent")
    risk_value = float(prov_settings.get("risk_value", 1.0))

    # Entry-zone mode: when the provider gives a range, pick nearest / mid / furthest level
    try:
        e_min = signal.get("entry_min")
        e_max = signal.get("entry_max")
        if entry > 0 and e_min is not None and e_max is not None:
            lo, hi = min(float(e_min), float(e_max)), max(float(e_min), float(e_max))
            if hi > lo and lo - 0.01 <= entry <= hi + 0.01:
                zmode = str(prov_settings.get("entry_zone_mode", "mid")).lower()
                if zmode == "nearest":
                    entry = lo if direction == "SELL" else hi
                elif zmode == "furthest":
                    entry = hi if direction == "SELL" else lo
                entry = round(entry, digits)
                signal["entry"] = entry
                ref_price = entry
                print(f"[ENTRY-ZONE] Zone {lo}-{hi}, mode={zmode} -> entry {entry}")
    except Exception as e:
        print(f"[WARN] Entry-zone resolution failed: {e}")

    # Part B: Sanity check to prevent placing an identical trade (matching Symbol, Direction, Entry, SL, and TP levels)
    norm_entry = round(float(entry or 0), digits)
    norm_sl = round(float(sl or 0), digits)
    norm_tp1 = round(float(tp_list[0] if len(tp_list) > 0 and tp_list[0] else 0), digits)
    norm_tp2 = round(float(tp_list[1] if len(tp_list) > 1 and tp_list[1] else 0), digits)
    norm_tp3 = round(float(tp_list[2] if len(tp_list) > 2 and tp_list[2] else 0), digits)
    exec_fp = (str(provider_id), actual_symbol, direction, norm_entry, norm_sl, norm_tp1, norm_tp2)

    # B1: Check against active trade groups that are still OPEN / active
    for grp_id, grp in list(active_trade_groups.items()):
        if grp.get("status") not in ("CLOSED",):
            g_sym = grp.get("symbol")
            g_dir = grp.get("trade_type")
            g_ent = round(float(grp.get("entry") or 0), digits)
            g_sl = round(float(grp.get("initial_sl") or 0), digits)
            g_tp1 = round(float(grp.get("tp1_price") or 0), digits)
            g_tp2 = round(float(grp.get("tp2_price") or 0), digits)
            # If entry, SL, and TP1/TP2 match an already active trade group, block duplicate
            entry_matches = (norm_entry > 0 and abs(g_ent - norm_entry) <= 2 * sym_info.point) or (norm_entry == 0 and g_sl == norm_sl and g_tp1 == norm_tp1)
            if g_sym == actual_symbol and g_dir == direction and entry_matches and g_sl == norm_sl and g_tp1 == norm_tp1 and g_tp2 == norm_tp2:
                err_msg = f"Duplicate trade blocked: active group {grp.get('signal_id')} already has identical levels ({actual_symbol} {direction} Entry={norm_entry}, SL={norm_sl})."
                print(f"[SANITY-GUARD] {err_msg}")
                _record_primary_diag(message_id, provider_id, "BLOCKED_DUPLICATE", err_msg)
                return worker_results

    # B2: Check against recent execution fingerprint cache (within last 30 minutes)
    now_ts = time.time()
    if exec_fp in _recent_exec_fingerprints and (now_ts - _recent_exec_fingerprints[exec_fp] < 1800):
        err_msg = f"Duplicate trade blocked: identical signal ({actual_symbol} {direction} Entry={norm_entry}, SL={norm_sl}) executed recently."
        print(f"[SANITY-GUARD] {err_msg}")
        _record_primary_diag(message_id, provider_id, "BLOCKED_DUPLICATE", err_msg)
        return worker_results

    threshold = config.MARKET_ORDER_THRESHOLD * sym_info.point

    # Spread compensation parameters
    spread_offset_entry = bool(prov_settings.get("spread_offset_entry", True))
    spread_offset_tp = bool(prov_settings.get("spread_offset_tp", True))
    spread_offset_sl = bool(prov_settings.get("spread_offset_sl", True))
    spread_val = (tick.ask - tick.bid) if tick else ((sym_info.spread * sym_info.point) if sym_info else 0.0)

    # Calculate effective entry price with spread compensation
    effective_entry = entry
    if entry > 0 and spread_offset_entry and spread_val > 0:
        if direction == "BUY":
            effective_entry = entry + spread_val
        elif direction == "SELL":
            effective_entry = entry

    # Calculate spread-compensated SL
    effective_sl = sl
    if sl and sl > 0 and spread_offset_sl and spread_val > 0:
        if direction == "SELL":
            effective_sl = sl + spread_val

    # Dynamic TP level allocation percentages
    p1 = float(prov_settings.get("tp1_percent", prov_settings.get("tp1_close_percent", 60.0)))
    p2 = float(prov_settings.get("tp2_percent", 30.0))
    p3 = float(prov_settings.get("tp3_percent", 10.0))
    if len(tp_list) > 3:
        tp_list = tp_list[:3]

    # Calculate spread-compensated TP list
    adjusted_tp_list = []
    for tp_val in tp_list:
        if tp_val > 0 and spread_offset_tp and spread_val > 0:
            if direction == "SELL":
                adjusted_tp = tp_val + spread_val
                if effective_entry > 0 and adjusted_tp >= effective_entry:
                    adjusted_tp = tp_val
                adjusted_tp_list.append(round(adjusted_tp, digits))
            else:
                adjusted_tp_list.append(round(tp_val, digits))
        else:
            adjusted_tp_list.append(round(tp_val, digits) if tp_val else 0)

    min_stop_dist = max((sym_info.trade_stops_level if sym_info else 0) * sym_info.point, spread_val + 5 * sym_info.point)

    current_price = tick.ask if direction == "BUY" else tick.bid
    sig_order_type = str(order_type or "MARKET").upper()
    e_min = signal.get("entry_min")
    e_max = signal.get("entry_max")

    # Unified order type decision across ALL tiers for the entire trade (computed BEFORE risk/lot sizing so ref_price matches actual planned order price)
    if direction == "BUY":
        if effective_entry > 0:
            if e_min is not None and e_max is not None:
                z_lo, z_hi = min(float(e_min), float(e_max)), max(float(e_min), float(e_max))
                in_entry_zone = (z_lo - threshold) <= current_price <= (z_hi + threshold)
            else:
                in_entry_zone = abs(current_price - effective_entry) <= threshold
        else:
            in_entry_zone = True

        tp_violated = any(t > 0 and t <= current_price + min_stop_dist for t in adjusted_tp_list)
        sl_violated = (effective_sl > 0 and effective_sl >= current_price - min_stop_dist)

        if sig_order_type not in ("LIMIT", "STOP") and in_entry_zone and not tp_violated and not sl_violated:
            unified_mt5_type = mt5.ORDER_TYPE_BUY
            unified_price = current_price
        elif effective_entry > 0:
            if effective_entry < current_price:
                unified_mt5_type = mt5.ORDER_TYPE_BUY_LIMIT
                unified_price = effective_entry
                if unified_price >= current_price - min_stop_dist:
                    unified_price = round(current_price - min_stop_dist - sym_info.point, digits)
            else:
                unified_mt5_type = mt5.ORDER_TYPE_BUY_STOP
                unified_price = effective_entry
                if unified_price <= current_price + min_stop_dist:
                    unified_price = round(current_price + min_stop_dist + sym_info.point, digits)
        else:
            unified_mt5_type = mt5.ORDER_TYPE_BUY
            unified_price = current_price

    elif direction == "SELL":
        if effective_entry > 0:
            if e_min is not None and e_max is not None:
                z_lo, z_hi = min(float(e_min), float(e_max)), max(float(e_min), float(e_max))
                in_entry_zone = (z_lo - threshold) <= current_price <= (z_hi + threshold)
            else:
                in_entry_zone = abs(current_price - effective_entry) <= threshold
        else:
            in_entry_zone = True

        tp_violated = any(t > 0 and t >= current_price - min_stop_dist for t in adjusted_tp_list)
        sl_violated = (effective_sl > 0 and effective_sl <= current_price + min_stop_dist)

        if sig_order_type not in ("LIMIT", "STOP") and in_entry_zone and not tp_violated and not sl_violated:
            unified_mt5_type = mt5.ORDER_TYPE_SELL
            unified_price = current_price
        elif effective_entry > 0:
            if effective_entry > current_price:
                unified_mt5_type = mt5.ORDER_TYPE_SELL_LIMIT
                unified_price = effective_entry
                if unified_price <= current_price + min_stop_dist:
                    unified_price = round(current_price + min_stop_dist + sym_info.point, digits)
            else:
                unified_mt5_type = mt5.ORDER_TYPE_SELL_STOP
                unified_price = effective_entry
                if unified_price >= current_price - min_stop_dist:
                    unified_price = round(current_price - min_stop_dist - sym_info.point, digits)
        else:
            unified_mt5_type = mt5.ORDER_TYPE_SELL
            unified_price = current_price
    else:
        return worker_results

    # Use actual planned order price as reference price for SL distance & risk calculation
    ref_price = unified_price if unified_price > 0 else (entry if entry > 0 else current_price)

    global trade_counter
    trade_counter += 1
    current_trade_num = trade_counter
    provider_magic = int(prov_settings.get("magic_number") or (400400 + int(provider_id) if str(provider_id) != "1" else config.MAGIC_NUMBER))

    acc = get_account_info()
    step = sym_info.volume_step if sym_info.volume_step > 0 else 0.01

    if risk_mode == "fixed":
        total_lot = max(config.MIN_LOT, round(risk_value / step) * step)
    else:
        # Default risk percentage mode
        risk_pct = risk_value / 100.0
        total_lot = config.DEFAULT_LOT_SIZE
        if acc and sl > 0 and ref_price > 0:
            risk_amount = acc.balance * risk_pct
            sl_distance_points = abs(ref_price - sl) / sym_info.point
            if sl_distance_points > 0 and sym_info.trade_tick_size > 0 and sym_info.trade_tick_value > 0:
                loss_per_lot = (sl_distance_points * sym_info.point / sym_info.trade_tick_size) * sym_info.trade_tick_value
                if loss_per_lot > 0:
                    calculated_lot = risk_amount / loss_per_lot
                    total_lot = max(config.MIN_LOT, round(calculated_lot / step) * step)

    # Safety Rule 2: Maximum Lot Cap per Provider (default: 10.0 lots)
    max_lot = float(prov_settings.get("max_lot", 10.0))
    if total_lot > max_lot:
        print(f"[RISK-CAP] Total lot {total_lot:.2f} exceeded max lot limit {max_lot:.2f} for Provider {provider_id}. Clamping to {max_lot:.2f} lots.")
        total_lot = max_lot

    num_splits = len(adjusted_tp_list)
    lots = []
    if num_splits >= 3:
        total_p = (p1 + p2 + p3) if (p1 + p2 + p3) > 0 else 100.0
        lot1 = max(config.MIN_LOT, round(((total_lot * (p1 / total_p))) / step) * step)
        lot2 = max(config.MIN_LOT, round(((total_lot * (p2 / total_p))) / step) * step)
        lot3 = max(config.MIN_LOT, round(((total_lot * (p3 / total_p))) / step) * step)
        lots = [lot1, lot2, lot3]
    elif num_splits == 2:
        total_p = (p1 + p2) if (p1 + p2) > 0 else 100.0
        lot1 = max(config.MIN_LOT, round(((total_lot * (p1 / total_p))) / step) * step)
        lot2 = max(config.MIN_LOT, round(((total_lot * (p2 / total_p))) / step) * step)
        lots = [lot1, lot2]
    else:
        lots = [max(config.MIN_LOT, round(total_lot / step) * step)]

    # Rule 2: Pre-flight Real Dollar Risk Verification for total planned position
    pt_val = getattr(sym_info, "trade_tick_value", 1.0)
    pt_size = getattr(sym_info, "trade_tick_size", sym_info.point)
    if pt_val <= 0:
        pt_val = 1.0
    if pt_size <= 0:
        pt_size = sym_info.point if sym_info.point > 0 else 0.01

    if risk_mode == "percent" and sl > 0 and acc:
        allowed_budget = acc.balance * (risk_value / 100.0)
        max_allowed_budget = allowed_budget * 1.25  # 25% tolerance buffer for spread/rounding
        est_dist = abs(ref_price - sl)
        est_pts = est_dist / pt_size if pt_size > 0 else est_dist
        # Cap active tiers if forcing MIN_LOT across 3 tiers would inflate total risk above max_allowed_budget
        while len(lots) > 1 and round(sum(lots), 4) > round(total_lot, 4) and (est_pts * pt_val * sum(lots)) > max_allowed_budget:
            lots.pop()
            adjusted_tp_list = adjusted_tp_list[:len(lots)]
            tp_list = tp_list[:len(lots)]
        total_dollar_risk = est_pts * pt_val * total_lot
        if total_dollar_risk > max_allowed_budget:
            err_msg = f"Risk guard blocked: total risk ${total_dollar_risk:.2f} exceeds budget ${allowed_budget:.2f} (max ${max_allowed_budget:.2f}) at ref={ref_price}, sl={sl}."
            print(f"[RISK-GUARD BLOCKED] {err_msg}")
            _record_primary_diag(message_id, provider_id, "BLOCKED_RISK_GUARD", err_msg, {"balance": acc.balance, "risk_dollar": total_dollar_risk, "max_budget": max_allowed_budget, "total_lot": total_lot})
            return worker_results

    print(f"[ORDER] Provider {provider_id} Rules: Magic={provider_magic}, Risk={risk_mode}:{risk_value}, TotalLot={total_lot:.2f}, Lots={lots} for TPs={tp_list}")
    print(f"[ORDER-PLAN] Provider {provider_id} Unified Order Type: {get_order_type_name(unified_mt5_type)} @ {unified_price} (Market={current_price}, Entry={effective_entry}, InZone={in_entry_zone if effective_entry > 0 else 'N/A'}, TPViolated={tp_violated if effective_entry > 0 else False})")

    results = []
    tier_errors = []

    for i, tp in enumerate(adjusted_tp_list):
        mt5_type = unified_mt5_type
        price = unified_price

        # Validate SL and TP against order type and current price to prevent MT5 10016 [Invalid stops]
        order_sl = round(effective_sl, digits) if effective_sl else 0
        order_tp = round(tp, digits) if tp else 0

        if mt5_type in (mt5.ORDER_TYPE_BUY, mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_BUY_STOP):
            if order_sl > 0 and order_sl >= price:
                tier_errors.append(f"TP{i+1}: Buy SL {order_sl} >= order price {price}")
                print(f"[RISK-GUARD BLOCKED] Buy Stop loss {order_sl} is above/equal to order price {price}. Aborting TP{i+1}!")
                continue
            if order_tp > 0 and order_tp <= price + min_stop_dist:
                order_tp = 0
        elif mt5_type in (mt5.ORDER_TYPE_SELL, mt5.ORDER_TYPE_SELL_LIMIT, mt5.ORDER_TYPE_SELL_STOP):
            if order_sl > 0 and order_sl <= price:
                tier_errors.append(f"TP{i+1}: Sell SL {order_sl} <= order price {price}")
                print(f"[RISK-GUARD BLOCKED] Sell Stop loss {order_sl} is below/equal to order price {price}. Aborting TP{i+1}!")
                continue
            if order_tp > 0 and order_tp >= price - min_stop_dist:
                order_tp = 0

        # Option 2 Execution Guard: Prevent absurd TPs (>12% away from price) from executing on MT5
        if order_tp > 0 and price > 0:
            tp_dist = abs(price - order_tp)
            if (tp_dist / price) > 0.12:
                print(f"[TP-GUARD BLOCKED] TP{i+1} {order_tp} is {tp_dist:.1f} points ({tp_dist/price*100:.1f}%) away from order price {price}. Clamped to 0 (Open TP) for safety.")
                order_tp = 0.0

        # Rule 2: Per-Tier Real-World Dollar Risk Verification
        if order_sl > 0:
            tier_dist = abs(price - order_sl)
            tier_pts = tier_dist / pt_size if pt_size > 0 else tier_dist
            tier_real_risk = tier_pts * pt_val * lots[i]
            if risk_mode == "percent" and acc:
                allowed_budget = acc.balance * (risk_value / 100.0)
                max_allowed_budget = allowed_budget * 1.25
                if tier_real_risk > max_allowed_budget:
                    tier_errors.append(f"TP{i+1}: risk ${tier_real_risk:.2f} > max ${max_allowed_budget:.2f}")
                    print(f"[RISK-GUARD BLOCKED] TP{i+1} real risk ${tier_real_risk:.2f} exceeds allocated budget ${allowed_budget:.2f} (max: ${max_allowed_budget:.2f}). Order aborted!")
                    continue

        request = {
            "action": mt5.TRADE_ACTION_DEAL if mt5_type in (mt5.ORDER_TYPE_BUY, mt5.ORDER_TYPE_SELL) else mt5.TRADE_ACTION_PENDING,
            "symbol": actual_symbol,
            "volume": lots[i],
            "type": mt5_type,
            "price": round(price, digits),
            "sl": order_sl,
            "tp": order_tp,
            "deviation": config.DEVIATION,
            "magic": provider_magic,
            "comment": f"P{provider_id} T{current_trade_num} TP{i+1}",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }

        result = mt5.order_send(request)
        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
            results.append({
                "ticket": result.order,
                "type": get_order_type_name(mt5_type),
                "direction": direction,
                "entry": round(price, digits),
                "sl": round(sl, digits) if sl else 0,
                "tp": round(tp, digits) if tp else 0,
                "lot": lots[i],
                "tp_number": i + 1,
                "account_id": "acc_1",
                "platform": "mt5",
            })
        else:
            err_code = result.retcode if result else "None"
            err_comm = result.comment if result else str(mt5.last_error())
            tier_errors.append(f"TP{i+1}: retcode={err_code} ({err_comm})")
            print(f"[ERROR] MT5 order_send failed for {actual_symbol} {get_order_type_name(mt5_type)} TP{i+1} @ {price} (SL:{sl}, TP:{tp}): retcode={err_code} ({err_comm})")

    # Calculate planned 1R risk amount in currency
    ref_price = unified_price
    signal_risk_amount = 0.0
    if sl > 0 and ref_price > 0 and sym_info.trade_tick_size > 0 and sym_info.trade_tick_value > 0:
        sl_dist = abs(ref_price - sl)
        signal_risk_amount = round((sl_dist / sym_info.trade_tick_size) * sym_info.trade_tick_value * sum(lots), 2)
    elif acc and risk_mode == "percent":
        signal_risk_amount = round(acc.balance * (risk_value / 100.0), 2)

    if results:
        _recent_exec_fingerprints[exec_fp] = time.time()
        group_id = int(time.time() * 1000)
        while group_id in active_trade_groups:
            group_id += 1
        sig_id = f"P{provider_id}_T{current_trade_num}"
        group_data = {
            "signal_id": sig_id,
            "provider_id": str(provider_id),
            "symbol": actual_symbol,
            "trade_type": direction,
            "lots": sum(lots),
            "entry": ref_price,
            "initial_sl": sl,
            "risk_amount": signal_risk_amount,
            "sl_is_trailing": False,
            "tp1_price": tp_list[0] if len(tp_list) > 0 else 0,
            "tp2_price": tp_list[1] if len(tp_list) > 1 else 0,
            "tp3_price": tp_list[2] if len(tp_list) > 2 else 0,
            "settings": prov_settings,
            "status": "OPEN",
            "tp1_ticket": None,
            "tp2_ticket": None,
            "tp3_ticket": None,
            "tp1_hit": 0,
            "tp2_hit": 0,
            "tp3_hit": 0,
            "sl_hit": 0,
            "message_id": int(message_id) if message_id else 0,
            "created_at": int(time.time()),
        }
        for r in results:
            if r["tp_number"] == 1: group_data["tp1_ticket"] = r["ticket"]
            elif r["tp_number"] == 2: group_data["tp2_ticket"] = r["ticket"]
            elif r["tp_number"] == 3: group_data["tp3_ticket"] = r["ticket"]
        
        active_trade_groups[group_id] = group_data
        
        if message_id:
            mid = int(message_id)
            existing_tkts = message_to_tickets.get(mid, [])
            new_tkts = [r["ticket"] for r in results if r["ticket"] not in existing_tkts]
            message_to_tickets[mid] = existing_tkts + new_tkts
            
        save_state()

        try:
            import db
            db.save_or_update_signal(
                signal_id=sig_id,
                provider_id=str(provider_id),
                symbol=actual_symbol,
                trade_type=direction,
                lots=round(sum(lots), 2),
                tp1_hit=0,
                tp2_hit=0,
                tp3_hit=0,
                sl_hit=0,
                profit=0.0,
                status="OPEN",
                created_at=int(time.time()),
                risk_amount=signal_risk_amount
            )
        except Exception as e:
            print("[WARN] Error logging initial signal:", e)

        _record_primary_diag(
            message_id,
            provider_id,
            "COPIED",
            f"Placed {len(results)} order(s) ({get_order_type_name(unified_mt5_type)} @ {unified_price}) on {actual_symbol}.",
            {"tickets": [r["ticket"] for r in results], "symbol": actual_symbol, "order_type": get_order_type_name(unified_mt5_type), "tier_errors": tier_errors},
        )
    else:
        _record_primary_diag(
            message_id,
            provider_id,
            "ORDER_SEND_FAILED",
            "; ".join(tier_errors) if tier_errors else f"All tiers aborted or failed on MT5 for {actual_symbol}.",
            {"symbol": actual_symbol, "order_type": get_order_type_name(unified_mt5_type), "price": unified_price, "tier_errors": tier_errors},
        )

    if worker_results:
        results.extend(worker_results)
    return results

def close_all_positions(provider_id: str = None, provider_ids: list = None, broadcast: bool = True) -> list:
    if broadcast and provider_id:
        try:
            from account_manager import AccountManager
            AccountManager.get_instance().send_broadcast_command("CLOSE_ALL", {"provider_id": provider_id})
        except Exception:
            pass

    target_pids = []
    if provider_id:
        if not is_provider_enabled_for_primary_mt5(provider_id):
            return []
        target_pids = [str(provider_id)]
    elif provider_ids:
        target_pids = [str(p) for p in provider_ids if is_provider_enabled_for_primary_mt5(str(p))]
        if not target_pids:
            return []

    magic_filter = None
    if target_pids:
        magic_filter = set()
        for pid in target_pids:
            p_settings = get_provider_settings(pid)
            m = int(p_settings.get("magic_number") or (400400 + int(pid) if str(pid) != "1" else config.MAGIC_NUMBER))
            magic_filter.add(m)

    positions = get_open_positions()
    if magic_filter is not None:
        positions = [p for p in positions if p.magic in magic_filter]

    results = []
    for pos in positions:
        actual_symbol = pos.symbol
        tick = get_tick(actual_symbol)
        if not tick: continue
        close_type = mt5.ORDER_TYPE_SELL if pos.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
        close_price = tick.bid if pos.type == mt5.ORDER_TYPE_BUY else tick.ask
        sym_info = get_symbol_info(actual_symbol)
        digits = sym_info.digits if sym_info else 2

        req = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": actual_symbol,
            "volume": pos.volume,
            "type": close_type,
            "price": round(close_price, digits),
            "deviation": config.DEVIATION,
            "magic": pos.magic,
            "comment": f"P{provider_id or 'All'} Close",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
            "position": pos.ticket,
        }
        res = mt5.order_send(req)
        if res and res.retcode == mt5.TRADE_RETCODE_DONE:
            results.append({
                "ticket": pos.ticket,
                "direction": "LONG" if pos.type == mt5.ORDER_TYPE_BUY else "SHORT",
                "entry": pos.price_open,
                "exit": close_price,
                "pnl": pos.profit,
                "action": "CLOSED",
            })
    return results

def cancel_all_pending_orders(provider_id: str = None, provider_ids: list = None) -> list:
    target_pids = []
    if provider_id:
        target_pids = [str(provider_id)]
    elif provider_ids:
        target_pids = [str(p) for p in provider_ids]

    magic_filter = None
    if target_pids:
        magic_filter = set()
        for pid in target_pids:
            p_settings = get_provider_settings(pid)
            m = int(p_settings.get("magic_number") or (400400 + int(pid) if str(pid) != "1" else config.MAGIC_NUMBER))
            magic_filter.add(m)

    orders = mt5.orders_get()
    if not orders:
        return []
    cancelled = []
    bot_magics = {config.MAGIC_NUMBER}
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                for pid, pdata in data.items():
                    m = pdata.get("magic_number") or (400400 + int(pid))
                    bot_magics.add(int(m))
        except Exception:
            pass

    for o in orders:
        if magic_filter is not None:
            if o.magic not in magic_filter:
                continue
        else:
            if o.magic not in bot_magics:
                continue
        req = {
            "action": mt5.TRADE_ACTION_REMOVE,
            "order": o.ticket,
        }
        res = mt5.order_send(req)
        if res and res.retcode == mt5.TRADE_RETCODE_DONE:
            cancelled.append(o.ticket)
    return cancelled

def close_all_positions_and_orders(provider_id: str = None, provider_ids: list = None, broadcast: bool = False) -> dict:
    closed = close_all_positions(provider_id=provider_id, provider_ids=provider_ids, broadcast=broadcast)
    cancelled = cancel_all_pending_orders(provider_id=provider_id, provider_ids=provider_ids)
    return {
        "closed_positions_count": len(closed),
        "cancelled_orders_count": len(cancelled),
        "closed_positions": closed,
        "cancelled_orders": cancelled
    }

def manage_trailing_stops():
    global active_trade_groups
    positions = get_open_positions()
    open_tickets = {p.ticket: p for p in positions}
    
    orders = mt5.orders_get()
    if orders is None:
        orders = []
    bot_magics = {config.MAGIC_NUMBER}
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                for pid, pdata in data.items():
                    m = pdata.get("magic_number") or (400400 + int(pid))
                    bot_magics.add(int(m))
        except Exception:
            pass
    pending_tickets = {o.ticket: o for o in orders if o.magic in bot_magics}

    state_changed = False
    
    for group_id, group in list(active_trade_groups.items()):
        symbol = group["symbol"]
        sym_info = get_symbol_info(symbol)
        if not sym_info: continue
            
        digits = sym_info.digits
        tp1_ticket = group.get("tp1_ticket")
        tp2_ticket = group.get("tp2_ticket")
        tp3_ticket = group.get("tp3_ticket")
        
        status = group["status"]
        entry = group["entry"]
        tp1_price = group["tp1_price"]
        
        tp1_open = tp1_ticket in open_tickets
        tp2_open = tp2_ticket in open_tickets
        tp3_open = tp3_ticket in open_tickets

        tp1_pending = tp1_ticket in pending_tickets
        tp2_pending = tp2_ticket in pending_tickets
        tp3_pending = tp3_ticket in pending_tickets

        if not (tp1_open or tp1_pending) and not (tp2_open or tp2_pending) and not (tp3_open or tp3_pending):
            # All positions/orders in this trade group are no longer open or pending.
            # Check if any ticket actually opened/filled a position in deal history.
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

            if not had_filled_deal and group.get("tp1_hit", 0) == 0 and group.get("tp2_hit", 0) == 0 and group.get("tp3_hit", 0) == 0:
                # Pending orders were cancelled/expired without ever filling -> do NOT mark as SL hit!
                group["status"] = "CANCELLED"
                mid_val = group.get("message_id")
                if mid_val and int(mid_val) in message_to_tickets:
                    grp_tkts = {t for t in [tp1_ticket, tp2_ticket, tp3_ticket] if t}
                    rem_tkts = [t for t in message_to_tickets.get(int(mid_val), []) if t not in grp_tkts]
                    if rem_tkts:
                        message_to_tickets[int(mid_val)] = rem_tkts
                    else:
                        message_to_tickets.pop(int(mid_val), None)
                try:
                    import db
                    import sqlite3
                    sig_id_to_del = group.get("signal_id", f"GRP_{group_id}")
                    conn = sqlite3.connect(db.DB_FILE)
                    conn.execute("DELETE FROM trade_signals WHERE signal_id = ? AND status = 'OPEN'", (sig_id_to_del,))
                    conn.commit()
                    conn.close()
                except Exception:
                    pass
                del active_trade_groups[group_id]
                state_changed = True
                continue

            # If at least one position filled and no TP was hit and trailing SL was not activated, it was an initial SL hit
            if group.get("tp1_hit", 0) == 0 and group.get("tp2_hit", 0) == 0 and group.get("tp3_hit", 0) == 0 and not group.get("sl_is_trailing", False):
                group["sl_hit"] = 1
            group["status"] = "CLOSED"
            try:
                import db
                db.save_or_update_signal(
                    signal_id=group.get("signal_id", f"GRP_{group_id}"),
                    provider_id=group.get("provider_id", "1"),
                    symbol=symbol,
                    trade_type=group.get("trade_type", "BUY"),
                    lots=group.get("lots", 0.0),
                    tp1_hit=group.get("tp1_hit", 0),
                    tp2_hit=group.get("tp2_hit", 0),
                    tp3_hit=group.get("tp3_hit", 0),
                    sl_hit=group.get("sl_hit", 0),
                    profit=0.0,
                    status="CLOSED",
                    closed_at=int(time.time()),
                    risk_amount=group.get("risk_amount", 0.0)
                )
            except Exception as e:
                pass
            del active_trade_groups[group_id]
            state_changed = True
            continue
            
        # Retrieve provider rule strategies
        prov_rules = group.get("settings") or get_provider_settings(group.get("provider_id", "1"))
        sl_tp1_strat = prov_rules.get("sl_tp1_strategy", "ENTRY")
        sl_tp2_strat = prov_rules.get("sl_tp2_strategy", "TP1")
        tp2_price = group.get("tp2_price", 0)

        def _check_ticket_tp_hit(tkt):
            if not tkt:
                return False
            orders = mt5.history_orders_get(ticket=tkt)
            pos_id = orders[0].position_id if orders else tkt
            deals = mt5.history_deals_get(position=pos_id)
            if not deals:
                deals = mt5.history_deals_get(ticket=tkt)
            if deals:
                exits = [d for d in deals if d.entry == 1]
                if exits:
                    cl = exits[-1]
                    comm = (cl.comment or '').lower()
                    if '[tp' in comm or getattr(cl, 'reason', None) == 5:
                        return True
                    if '[sl' in comm or getattr(cl, 'reason', None) in (4, 6):
                        return False
            return False

        # TP1 Hit event
        if not tp1_open and not tp1_pending and status == "OPEN":
            # Verify that tp1 closed via actual TP, not manual or SL
            is_real_tp1 = _check_ticket_tp_hit(tp1_ticket)
            if is_real_tp1:
                group["status"] = "TP1_HIT"
                group["tp1_hit"] = 1
                group["sl_is_trailing"] = True
                state_changed = True
                
                try:
                    import db
                    db.save_or_update_signal(
                        signal_id=group.get("signal_id", f"GRP_{group_id}"),
                        provider_id=group.get("provider_id", "1"),
                        symbol=symbol,
                        trade_type=group.get("trade_type", "BUY"),
                        lots=group.get("lots", 0.0),
                        tp1_hit=1,
                        tp2_hit=group.get("tp2_hit", 0),
                        tp3_hit=group.get("tp3_hit", 0),
                        sl_hit=0,
                        profit=0.0,
                        status="TP1_HIT"
                    )
                except Exception as e:
                    pass

                # Execute configured SL strategy for TP1 hit (Break Even)
                if sl_tp1_strat != "none":
                    for tkt in [tp2_ticket, tp3_ticket]:
                        if tkt and (tkt in open_tickets):
                            pos = open_tickets[tkt]
                            tick = get_tick(symbol)
                            spread_val = (tick.ask - tick.bid) if tick else (sym_info.spread * sym_info.point)
                            be_price = pos.price_open + spread_val if pos.type == mt5.ORDER_TYPE_BUY else pos.price_open - spread_val
                            min_stop = max((sym_info.trade_stops_level if sym_info else 0) * sym_info.point, 2 * sym_info.point)
                            if tick:
                                if pos.type == mt5.ORDER_TYPE_BUY and be_price >= tick.bid - min_stop:
                                    be_price = min(be_price, tick.bid - min_stop)
                                elif pos.type == mt5.ORDER_TYPE_SELL and be_price <= tick.ask + min_stop:
                                    be_price = max(be_price, tick.ask + min_stop)
                            if pos.sl == 0 or (pos.type == mt5.ORDER_TYPE_BUY and be_price > pos.sl) or (pos.type == mt5.ORDER_TYPE_SELL and be_price < pos.sl):
                                req = {
                                    "action": mt5.TRADE_ACTION_SLTP,
                                    "position": tkt,
                                    "symbol": symbol,
                                    "sl": round(be_price, digits),
                                    "tp": pos.tp
                                }
                                mt5.order_send(req)
                        
        # TP2 Hit event
        if not tp1_open and not tp1_pending and not tp2_open and not tp2_pending and status in ("TP1_HIT", "OPEN") and tp3_open:
            is_real_tp2 = _check_ticket_tp_hit(tp2_ticket)
            if is_real_tp2:
                group["status"] = "TP2_HIT"
                group["tp2_hit"] = 1
                group["sl_is_trailing"] = True
                state_changed = True

                try:
                    import db
                    db.save_or_update_signal(
                        signal_id=group.get("signal_id", f"GRP_{group_id}"),
                        provider_id=group.get("provider_id", "1"),
                        symbol=symbol,
                        trade_type=group.get("trade_type", "BUY"),
                        lots=group.get("lots", 0.0),
                        tp1_hit=group.get("tp1_hit", 1),
                        tp2_hit=1,
                        tp3_hit=group.get("tp3_hit", 0),
                        sl_hit=0,
                        profit=0.0,
                        status="TP2_HIT"
                    )
                except Exception as e:
                    pass

                pos = open_tickets[tp3_ticket]
                
                # Execute configured SL strategy for TP2 hit (SL to TP1)
                target_sl_price = 0
                if sl_tp2_strat == "TP1" and tp1_price > 0:
                    target_sl_price = tp1_price
                elif sl_tp2_strat == "ENTRY" and entry > 0:
                    tick = get_tick(symbol)
                    spread_val = (tick.ask - tick.bid) if tick else (sym_info.spread * sym_info.point)
                    target_sl_price = entry + spread_val if pos.type == mt5.ORDER_TYPE_BUY else entry - spread_val
                
                if target_sl_price > 0:
                    if pos.sl == 0 or (pos.type == mt5.ORDER_TYPE_BUY and target_sl_price > pos.sl) or (pos.type == mt5.ORDER_TYPE_SELL and target_sl_price < pos.sl):
                        req = {
                            "action": mt5.TRADE_ACTION_SLTP,
                            "position": tp3_ticket,
                            "symbol": symbol,
                            "sl": round(target_sl_price, digits),
                            "tp": pos.tp
                        }
                        mt5.order_send(req)
                    
    if state_changed:
        save_state()

def cancel_trade(message_id: int) -> bool:
    try:
        from account_manager import AccountManager
        AccountManager.get_instance().send_broadcast_command("CANCEL_TRADE", {"reply_id": message_id})
    except Exception:
        pass
    if message_id not in message_to_tickets:
        return False
    tickets = message_to_tickets[message_id]
    success = False
    for ticket in tickets:
        order = mt5.orders_get(ticket=ticket)
        if order:
            req = {
                "action": mt5.TRADE_ACTION_REMOVE,
                "order": ticket,
            }
            res = mt5.order_send(req)
            if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                success = True
    return success

def close_trade(message_id: int) -> bool:
    try:
        from account_manager import AccountManager
        AccountManager.get_instance().send_broadcast_command("CLOSE_TRADE", {"reply_id": message_id})
    except Exception:
        pass
    if message_id not in message_to_tickets:
        return False
    tickets = message_to_tickets[message_id]
    success = False
    for ticket in tickets:
        pos = mt5.positions_get(ticket=ticket)
        if pos:
            pos = pos[0]
            actual_symbol = pos.symbol
            tick = get_tick(actual_symbol)
            if not tick: continue
            
            if pos.type == mt5.ORDER_TYPE_BUY:
                close_type = mt5.ORDER_TYPE_SELL
                close_price = tick.bid
            else:
                close_type = mt5.ORDER_TYPE_BUY
                close_price = tick.ask
                
            sym_info = get_symbol_info(actual_symbol)
            digits = sym_info.digits if sym_info else 2

            req = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": actual_symbol,
                "volume": pos.volume,
                "type": close_type,
                "price": round(close_price, digits),
                "deviation": config.DEVIATION,
                "magic": config.MAGIC_NUMBER,
                "comment": "SigCopy Close Reply",
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC,
                "position": pos.ticket,
            }
            res = mt5.order_send(req)
            if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                success = True
    return success

def modify_sl(message_id: int, new_sl: float) -> bool:
    try:
        from account_manager import AccountManager
        AccountManager.get_instance().send_broadcast_command("MODIFY_SL", {"reply_id": message_id, "new_sl": new_sl})
    except Exception:
        pass
    if message_id not in message_to_tickets:
        return False
    tickets = message_to_tickets[message_id]
    success = False
    for ticket in tickets:
        pos = mt5.positions_get(ticket=ticket)
        if pos:
            pos = pos[0]
            sym_info = get_symbol_info(pos.symbol)
            req = {
                "action": mt5.TRADE_ACTION_SLTP,
                "position": ticket,
                "symbol": pos.symbol,
                "sl": round(new_sl, sym_info.digits),
                "tp": pos.tp
            }
            res = mt5.order_send(req)
            if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                success = True
                
        order = mt5.orders_get(ticket=ticket)
        if order:
            order = order[0]
            sym_info = get_symbol_info(order.symbol)
            req = {
                "action": mt5.TRADE_ACTION_MODIFY,
                "order": ticket,
                "symbol": order.symbol,
                "sl": round(new_sl, sym_info.digits),
                "tp": order.tp,
                "price": order.price_open
            }
            res = mt5.order_send(req)
            if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                success = True
    return success


def modify_signal_levels(message_id: int, new_tps: list = None, new_sl: float = 0.0, new_entry: float = 0.0, provider_id: str = None, entry_min: float = None, entry_max: float = None) -> list:
    """
    Modifies Take Profit and Stop Loss levels across all active positions / pending orders
    associated with an edited Telegram signal message.
    Broadcasts across all account workers and updates local primary MT5 trades.
    """
    new_tps = [float(x) for x in (new_tps or []) if float(x) > 0]
    new_sl = float(new_sl or 0.0)
    new_entry = float(new_entry or 0.0)
    mid = int(message_id)

    # 1. Broadcast command to all other running account workers
    try:
        from account_manager import AccountManager
        AccountManager.get_instance().send_broadcast_command("MODIFY_SIGNAL_LEVELS", {
            "message_id": mid,
            "provider_id": str(provider_id or ""),
            "new_tps": list(new_tps),
            "new_sl": new_sl,
            "new_entry": new_entry,
            "entry_min": entry_min,
            "entry_max": entry_max,
        })
    except Exception as e:
        print(f"[WARN] Failed to broadcast MODIFY_SIGNAL_LEVELS: {e}")

    # 2. Modify primary MT5 trades
    modified_results = []

    matched_groups = []
    for gid, g in list(active_trade_groups.items()):
        g_mid = g.get("message_id")
        if g_mid and int(g_mid) == mid:
            matched_groups.append((gid, g))
        elif mid in message_to_tickets:
            tkts = message_to_tickets[mid]
            if any(g.get(k) in tkts for k in ["tp1_ticket", "tp2_ticket", "tp3_ticket"]):
                matched_groups.append((gid, g))

    tier_map = [("tp1_ticket", 0, "tp1_price"), ("tp2_ticket", 1, "tp2_price"), ("tp3_ticket", 2, "tp3_price")]

    for gid, group in matched_groups:
        direction = group.get("trade_type", "BUY")
        g_symbol = group.get("symbol", "XAUUSD")
        g_prov_id = str(group.get("provider_id") or provider_id or "1")
        prov_settings = group.get("settings") or get_provider_settings(g_prov_id) or {}
        spread_offset_tp = prov_settings.get("spread_offset_tp", True)
        spread_offset_entry = prov_settings.get("spread_offset_entry", True)
        spread_offset_sl = prov_settings.get("spread_offset_sl", True)

        # Normalize & auto-fix new levels against live market tick so edited messages with digit typos don't corrupt orders
        grp_tps = list(new_tps)
        grp_sl = new_sl
        grp_entry = new_entry
        try:
            sym_info_g = get_symbol_info(g_symbol)
            digits_g = sym_info_g.digits if sym_info_g else 2
            tick_g = get_tick(g_symbol)
            if tick_g and getattr(tick_g, "bid", 0) > 0 and getattr(tick_g, "ask", 0) > 0:
                from signal_parser import normalize_and_autofix_signal
                temp_sig = {
                    "direction": direction,
                    "entry": grp_entry if grp_entry > 0 else float(group.get("entry") or 0.0),
                    "entry_min": entry_min,
                    "entry_max": entry_max,
                    "sl": grp_sl if grp_sl > 0 else float(group.get("initial_sl") or 0.0),
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
            print(f"[WARN] modify_signal_levels autofix error: {_me}")

        for t_key, t_idx, price_key in tier_map:
            ticket = group.get(t_key)
            if not ticket:
                continue

            target_tp = grp_tps[t_idx] if t_idx < len(grp_tps) else (grp_tps[-1] if grp_tps else 0.0)

            # Check open positions
            pos = mt5.positions_get(ticket=ticket)
            if pos:
                p = pos[0]
                sym_info = get_symbol_info(p.symbol)
                digits = sym_info.digits if sym_info else 2
                tick = get_tick(p.symbol)
                spread_val = (tick.ask - tick.bid) if tick else ((sym_info.spread * sym_info.point) if sym_info else 0.0)

                # Spread offset if configured
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
                        group["initial_sl"] = grp_sl
                    modified_results.append({
                        "ticket": ticket,
                        "tier": f"TP{t_idx+1}",
                        "action": "MODIFIED_POSITION",
                        "sl": req["sl"],
                        "tp": req["tp"]
                    })
                    print(f"[EDIT-MODIFY] Primary MT5 Position #{ticket} updated: SL={req['sl']}, TP={req['tp']}")
                else:
                    err_comm = getattr(res, "comment", "") if res else str(mt5.last_error())
                    print(f"[WARN] Failed to modify Position #{ticket}: {err_comm}")

            # Check pending orders
            order = mt5.orders_get(ticket=ticket)
            if order:
                o = order[0]
                sym_info = get_symbol_info(o.symbol)
                digits = sym_info.digits if sym_info else 2
                tick = get_tick(o.symbol)
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
                        group["entry"] = grp_entry
                    if grp_sl > 0:
                        group["initial_sl"] = grp_sl
                    modified_results.append({
                        "ticket": ticket,
                        "tier": f"TP{t_idx+1}",
                        "action": "MODIFIED_PENDING",
                        "price": req["price"],
                        "sl": req["sl"],
                        "tp": req["tp"]
                    })
                    print(f"[EDIT-MODIFY] Primary MT5 Pending Order #{ticket} updated: Price={req['price']}, SL={req['sl']}, TP={req['tp']}")
                else:
                    err_comm = getattr(res, "comment", "") if res else str(mt5.last_error())
                    print(f"[WARN] Failed to modify Pending Order #{ticket}: {err_comm}")

    save_state()
    return modified_results


def get_order_type_name(mt5_type):
    names = {
        mt5.ORDER_TYPE_BUY: "Market Buy",
        mt5.ORDER_TYPE_SELL: "Market Sell",
        mt5.ORDER_TYPE_BUY_LIMIT: "Buy Limit",
        mt5.ORDER_TYPE_SELL_LIMIT: "Sell Limit",
        mt5.ORDER_TYPE_BUY_STOP: "Buy Stop",
        mt5.ORDER_TYPE_SELL_STOP: "Sell Stop",
    }
    return names.get(mt5_type, "Unknown")

def set_breakeven_for_provider(provider_id: str = None) -> bool:
    """Moves all open positions for this provider to true Break-Even price ($0 PnL accounting for spread)."""
    try:
        from account_manager import AccountManager
        AccountManager.get_instance().send_broadcast_command("SET_BE", {"provider_id": provider_id})
    except Exception:
        pass
    if provider_id and not is_provider_enabled_for_primary_mt5(provider_id):
        return True
    magic_filter = None
    if provider_id:
        p_settings = get_provider_settings(provider_id)
        magic_filter = int(p_settings.get("magic_number") or (400400 + int(provider_id) if str(provider_id) != "1" else config.MAGIC_NUMBER))

    positions = get_open_positions(magic=magic_filter)
    if not positions:
        return False

    success = False
    for pos in positions:
        sym_info = get_symbol_info(pos.symbol)
        digits = sym_info.digits if sym_info else 2
        entry = pos.price_open
        tick = get_tick(pos.symbol)
        spread_val = (tick.ask - tick.bid) if tick else ((sym_info.spread * sym_info.point) if sym_info else 0.0)
        be_price = entry + spread_val if pos.type == mt5.ORDER_TYPE_BUY else entry - spread_val
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
    return success

def cancel_all_pending_for_provider(provider_id: str = None) -> bool:
    """Removes all pending orders for this provider."""
    try:
        from account_manager import AccountManager
        AccountManager.get_instance().send_broadcast_command("CANCEL_PENDING_PROVIDER", {"provider_id": provider_id})
    except Exception:
        pass
    if provider_id and not is_provider_enabled_for_primary_mt5(provider_id):
        return True
    if not is_connected():
        return False
    magic_filter = None
    if provider_id:
        p_settings = get_provider_settings(provider_id)
        magic_filter = int(p_settings.get("magic_number") or (400400 + int(provider_id) if str(provider_id) != "1" else config.MAGIC_NUMBER))

    orders = mt5.orders_get()
    if not orders:
        return False

    success = False
    for o in orders:
        if magic_filter is None or o.magic == magic_filter:
            req = {
                "action": mt5.TRADE_ACTION_REMOVE,
                "order": o.ticket,
            }
            res = mt5.order_send(req)
            if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                success = True
    return success

def link_message_ids(old_id: int, new_id: int):
    if old_id in message_to_tickets:
        message_to_tickets[new_id] = message_to_tickets[old_id]
        save_state()


def has_executed_trades_for_message(message_id: int) -> bool:
    """
    Checks if a Telegram message ID has any actual executed trades or orders
    across primary MT5 or any secondary/cTrader accounts.
    """
    if not message_id:
        return False
    mid_int = int(message_id)
    mid_str = str(message_id)

    # 1. Primary MT5 in-memory & active groups
    if mid_int in message_to_tickets and message_to_tickets[mid_int]:
        return True
    for g in active_trade_groups.values():
        if g.get("message_id") == mid_int:
            return True

    # 2. Check all trade_state*.json files (cTrader, secondary MT5, MT4)
    import glob
    base_dir = os.path.dirname(os.path.abspath(__file__))
    for fn in glob.glob(os.path.join(base_dir, "trade_state*.json")):
        try:
            with open(fn, "r", encoding="utf-8") as f:
                d = json.load(f)
                m2t = d.get("message_to_tickets") or {}
                tkts = m2t.get(mid_str) or m2t.get(mid_int)
                if tkts:
                    return True
                atg = d.get("active_trade_groups") or {}
                for g in atg.values():
                    if g.get("message_id") in (mid_int, mid_str):
                        return True
        except Exception:
            pass

    return False

