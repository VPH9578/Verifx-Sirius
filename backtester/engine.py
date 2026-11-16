"""
engine.py — Historical MT5 Signal Simulation Engine for the Backtester.
100% read-only against MetaTrader 5 (uses copy_rates_range / copy_rates_from_pos).
Implements:
  1. 4-Point Directional Candle Path (Bullish: Open->Low->High->Close, Bearish: Open->High->Low->Close)
  2. Provider Reply Updates (Cancel, Close, Set BE, Modify SL) + Adjustable Pending Order Expiry (default 30m)
  3. Multi-TP Lot Splitting (TP1, TP2, TP3) & Cascading Stop Loss Strategies (Entry / TP1 / TP2 / None)
  4. Automatic Multi-Timeframe Fallback (M1 -> M5 -> M15 -> H1) for deep 1-year lookbacks
"""

import os
import json
import time
from datetime import datetime, timezone
import MetaTrader5 as mt5
import config
from ctrader_client import (
    CTraderClient,
    CTraderSymbolInfo,
    CT_PERIOD_M1,
    CT_PERIOD_M5,
    CT_PERIOD_M15,
    CT_PERIOD_H1,
)

_ct_bt_client_cache = {"client": None, "ts": 0}


def _get_ctrader_backtest_client():
    """Returns a connected read-only CTraderClient if the local SiriusBridge cBot is running in cTrader Desktop."""
    cached = _ct_bt_client_cache.get("client")
    if cached and cached.is_authorized:
        return cached

    host = os.getenv("CTRADER_HOST", "127.0.0.1")
    port = int(os.getenv("CTRADER_PORT", "8888") or 8888)

    if os.path.exists("accounts_config.json"):
        try:
            with open("accounts_config.json", "r", encoding="utf-8") as f:
                accs = json.load(f)
                for cfg in accs.values():
                    if str(cfg.get("platform", "")).lower() == "ctrader":
                        host = str(cfg.get("ctrader_host") or host)
                        port = int(cfg.get("ctrader_port") or port)
                        break
        except Exception:
            pass

    try:
        client = CTraderClient(host=host, port=port)
        if client.connect():
            _ct_bt_client_cache["client"] = client
            _ct_bt_client_cache["ts"] = time.time()
            return client
    except Exception:
        pass
    return None


def _ensure_mt5_connected() -> bool:
    info = mt5.account_info()
    if info is not None:
        return True
    try:
        if config.MT5_PATH:
            mt5.initialize(path=config.MT5_PATH)
        else:
            mt5.initialize()
        info = mt5.account_info()
        if info is None and config.MT5_ACCOUNT and config.MT5_PASSWORD and config.MT5_PASSWORD != "your_password":
            mt5.login(config.MT5_ACCOUNT, password=config.MT5_PASSWORD, server=config.MT5_SERVER)
            info = mt5.account_info()
    except Exception:
        pass
    return info is not None


def _build_symbol_map() -> dict:
    """Build a mapping from clean base symbol names (XAUUSD, GOLD, EURUSD) to broker symbol names."""
    sym_map = {}
    symbols = mt5.symbols_get()
    if not symbols:
        return sym_map

    # Sort so standard/visible symbols (like XAUUSD.p, XAUUSD.sc, XAUUSD) are processed after .crp
    # or explicitly score each symbol candidate
    best_score = {}

    for s in symbols:
        if getattr(s, "trade_mode", 0) == 0:
            continue
        name = s.name
        upper = name.upper()
        base = upper.split(".")[0].replace("-", "").replace("_", "")

        score = 10
        if getattr(s, "visible", False):
            score += 20
        if ".CRP" in upper:
            score -= 15
        if upper == base or upper.endswith(".P") or upper.endswith(".SC") or upper.endswith(".RAW") or upper.endswith(".PRO"):
            score += 10

        if base not in best_score or score > best_score[base]:
            best_score[base] = score
            sym_map[base] = name
            if base == "XAUUSD":
                sym_map["GOLD"] = name
                sym_map["XAU"] = name

    return sym_map


def _get_broker_offset_sec(default_symbol: str = None) -> int:
    """
    Determine the broker server's timezone offset from UTC in seconds (e.g. +10800 for UTC+3).
    Rounds to the nearest 1800 seconds (30 minutes).
    """
    now_utc = time.time()
    candidates = ["BTCUSD", "ETHUSD", "BTCUSD.p", "BTCUSD.sc", "EURUSD", "XAUUSD.p", "XAUUSD.sc"]
    if default_symbol:
        candidates.insert(0, default_symbol)

    for sym in candidates:
        try:
            mt5.symbol_select(sym, True)
            tick = mt5.symbol_info_tick(sym)
            if tick and tick.time > 0:
                diff = tick.time - now_utc
                # If tick is from today/live (within 12 hours of UTC clock), round to nearest 30m
                if abs(diff) <= 43200:
                    return int(round(diff / 1800.0) * 1800)
        except Exception:
            pass

    # Fallback: standard MT5 EEST/EET server offset (+3 hours = 10800s)
    return 10800


def _prewarm_symbol_timeframes(actual_symbol: str):
    """Ensure MT5 has synced historical buffers for M1, M5, M15, and H1."""
    mt5.symbol_select(actual_symbol, True)
    for tf in (mt5.TIMEFRAME_M1, mt5.TIMEFRAME_M5, mt5.TIMEFRAME_M15, mt5.TIMEFRAME_H1):
        try:
            r = mt5.copy_rates_from_pos(actual_symbol, tf, 0, 50000)
            if r is None:
                time.sleep(0.15)
                mt5.copy_rates_from_pos(actual_symbol, tf, 0, 50000)
        except Exception:
            pass


_ct_trendbar_cache = {}


def _fetch_candles_for_window(actual_symbol: str, t_broker_start: int, t_broker_end: int, ct_client: CTraderClient = None):
    """
    Fetch OHLC bars for [t_broker_start, t_broker_end] in Broker Time (or UTC for cTrader).
    Uses M1 (1-minute) by default; automatically falls back to M5, M15, or H1.
    Returns (rates_array, timeframe_label, timeframe_seconds).
    """
    if ct_client and ct_client.is_authorized:
        ct_timeframes = [
            (CT_PERIOD_M1, "M1", 60),
            (CT_PERIOD_M5, "M5", 300),
            (CT_PERIOD_M15, "M15", 900),
            (CT_PERIOD_H1, "H1", 3600),
        ]
        now_ts = int(time.time())
        q_start = max(0, t_broker_start - 120)
        q_end = t_broker_end + 300
        for p_const, tf_name, tf_sec in ct_timeframes:
            cache_key = (actual_symbol, p_const)
            cached = _ct_trendbar_cache.get(cache_key)
            if cached and (now_ts - cached["ts"] <= 120) and cached["min_req"] <= q_start:
                all_bars = cached["bars"]
            else:
                # Fetch a 40-day bulk window from SiriusBridge cBot once and slice locally in <1ms
                bulk_start = min(q_start, now_ts - 40 * 86400)
                all_bars = ct_client.fetch_trendbars(actual_symbol, p_const, bulk_start, now_ts + 300) or []
                if all_bars:
                    _ct_trendbar_cache[cache_key] = {
                        "ts": now_ts,
                        "min_req": bulk_start,
                        "bars": all_bars,
                    }
            bars = [b for b in all_bars if q_start <= int(b["time"]) <= q_end] if all_bars else []
            if bars and len(bars) > 0:
                if int(bars[0]["time"]) <= (t_broker_start + 230400):
                    return bars, tf_name, tf_sec
        return None, "NONE", 60

    dt_from = datetime.fromtimestamp(max(0, t_broker_start - 120), tz=timezone.utc)
    dt_to = datetime.fromtimestamp(t_broker_end + 300, tz=timezone.utc)

    timeframes = [
        (mt5.TIMEFRAME_M1, "M1", 60),
        (mt5.TIMEFRAME_M5, "M5", 300),
        (mt5.TIMEFRAME_M15, "M15", 900),
        (mt5.TIMEFRAME_H1, "H1", 3600),
    ]

    for tf_const, tf_name, tf_sec in timeframes:
        rates = mt5.copy_rates_range(actual_symbol, tf_const, dt_from, dt_to)
        if rates is None or len(rates) == 0:
            # Brief retry in case MT5 terminal cache was syncing
            rates = mt5.copy_rates_range(actual_symbol, tf_const, dt_from, dt_to)

        if rates is not None and len(rates) > 0:
            first_bar_time = int(rates[0]["time"])
            # Check that the earliest bar returned isn't days after the signal start
            # (allow up to 64 hours gap in case signal was sent over a weekend)
            if first_bar_time <= (t_broker_start + 230400):
                return rates, tf_name, tf_sec

    return None, "NONE", 60


_mt5_tick_symbol_cache = {}
_mt5_offset_cache = {"offset": None, "ts": 0}


def _fetch_surgical_1m_ticks(
    actual_symbol: str,
    bar_time_utc: int,
    tf_sec: int = 60,
    ct_client: CTraderClient = None,
    candle_cache: dict = None,
) -> list:
    """
    Surgical 1-Minute Tick Drilldown:
    Fetches millisecond-level ticks ONLY for the specific 1-minute candle [bar_time_utc, bar_time_utc + tf_sec]
    that touched an Entry, TP, or SL level.
      1. Checks in-memory `candle_cache` first (0ms reuse across 104 AutoTester permutations).
      2. If `ct_client` is active and the candle is within cTrader's live tick buffer, fetches from SiriusBridge cBot.
      3. Otherwise, automatically queries MT5's indexed random-access tick archive (`copy_ticks_range`) for that exact
         60-second window so historical cTrader M1 backtests get true intrabar tick resolution.
    Returns a list of (tick_utc_sec, bid_price) tuples.
    """
    cache_key = ("surgical_1m_ticks", actual_symbol, int(bar_time_utc), int(tf_sec))
    if candle_cache is not None and cache_key in candle_cache:
        return candle_cache[cache_key]

    out_ticks = []

    # 1. Try SiriusBridge cBot for recent candles (within cTrader's fast in-memory tick window)
    if ct_client and ct_client.is_authorized and (time.time() - bar_time_utc) <= 3600:
        try:
            raw_ct = ct_client.fetch_tick_data(actual_symbol, bar_time_utc, bar_time_utc + tf_sec)
            if raw_ct:
                for tk in raw_ct:
                    t_sec = int(tk.get("time") or 0)
                    if bar_time_utc <= t_sec <= (bar_time_utc + tf_sec):
                        out_ticks.append((t_sec, float(tk["bid"])))
        except Exception:
            pass

    # 2. If not in cBot's recent buffer (or running on MT5), fetch the exact 60-second window from MT5's tick archive
    if not out_ticks:
        try:
            if _ensure_mt5_connected():
                clean_base = actual_symbol.upper().split(".")[0].replace("/", "").replace("-", "").replace("_", "")
                if clean_base not in _mt5_tick_symbol_cache:
                    smap = _build_symbol_map()
                    _mt5_tick_symbol_cache[clean_base] = smap.get(clean_base) or getattr(config, "SYMBOL", actual_symbol)
                mt5_sym = _mt5_tick_symbol_cache[clean_base]

                if _mt5_offset_cache["offset"] is None or (time.time() - _mt5_offset_cache["ts"] > 600):
                    _mt5_offset_cache["offset"] = _get_broker_offset_sec(mt5_sym)
                    _mt5_offset_cache["ts"] = time.time()
                mt5_off = _mt5_offset_cache["offset"]

                t_start_broker = bar_time_utc + mt5_off
                dt_from = datetime.fromtimestamp(max(0, t_start_broker), tz=timezone.utc)
                dt_to = datetime.fromtimestamp(t_start_broker + tf_sec + 1, tz=timezone.utc)
                ticks = mt5.copy_ticks_range(mt5_sym, dt_from, dt_to, mt5.COPY_TICKS_ALL)
                if ticks is not None and len(ticks) > 0:
                    for tk in ticks:
                        t_utc = int(tk["time"]) - mt5_off
                        if bar_time_utc <= t_utc <= (bar_time_utc + tf_sec):
                            bid_val = float(tk["bid"])
                            if bid_val > 0:
                                out_ticks.append((t_utc, bid_val))
        except Exception:
            pass

    if candle_cache is not None:
        candle_cache[cache_key] = out_ticks
    return out_ticks


def _fetch_ticks_for_window(actual_symbol: str, t_broker_start: int, t_broker_end: int, ct_client: CTraderClient = None):
    """
    Fetch exact millisecond-level tick data for [t_broker_start, t_broker_end] in Broker Time (or UTC for cTrader).
    Returns (ticks_array, 'TICKS', 0) if available, or (None, 'NONE', 0) if broker has no ticks.
    """
    if ct_client and ct_client.is_authorized:
        try:
            ticks = ct_client.fetch_tick_data(actual_symbol, max(0, t_broker_start - 60), t_broker_end + 120)
            if ticks and len(ticks) > 0:
                return ticks, "TICKS", 0
        except Exception:
            pass
        return None, "NONE", 0

    dt_from = datetime.fromtimestamp(max(0, t_broker_start - 60), tz=timezone.utc)
    dt_to = datetime.fromtimestamp(t_broker_end + 120, tz=timezone.utc)
    try:
        ticks = mt5.copy_ticks_range(actual_symbol, dt_from, dt_to, mt5.COPY_TICKS_ALL)
        if ticks is not None and len(ticks) > 0:
            return ticks, "TICKS", 0
    except Exception:
        pass
    return None, "NONE", 0


ENTRY_ZONE_MODES = ("nearest", "mid", "furthest")


def _derive_entry_range(sig: dict, sig_entry: float) -> tuple[float, float]:
    """Return (entry_min, entry_max). Re-parses raw_text for older cached signals lacking range fields."""
    e_min, e_max = sig.get("entry_min"), sig.get("entry_max")
    if (e_min is None or e_max is None) and sig.get("raw_text"):
        try:
            from signal_parser import parse_with_regex
            reparsed = parse_with_regex(sig["raw_text"]) or {}
            # Only trust the re-parse if its midpoint matches the stored entry
            if reparsed.get("entry_min") is not None and abs(float(reparsed.get("entry") or 0) - sig_entry) < 0.01:
                e_min, e_max = reparsed["entry_min"], reparsed["entry_max"]
        except Exception:
            pass
    if e_min is None or e_max is None or float(e_min) <= 0 or float(e_max) <= 0:
        return sig_entry, sig_entry
    return min(float(e_min), float(e_max)), max(float(e_min), float(e_max))


def _resolve_entry_zone(direction: str, sig_entry: float, e_min: float, e_max: float,
                        mode: str, threshold_price: float) -> tuple[float, float, float]:
    """
    Pick the fill level inside a provider's entry zone and the price band that activates it.
    Returns (fill_level, band_low, band_high).
      nearest  = first price touched (least profit): SELL -> zone low,  BUY -> zone high
      mid      = midpoint of the zone
      furthest = last price in zone (most profit):    SELL -> zone high, BUY -> zone low
    Band: from the chosen level out to the far edge of the zone (+threshold), so price
    must actually reach the chosen level, and fills never occur beyond the zone.
    """
    if sig_entry <= 0 or e_max - e_min <= 0:
        return sig_entry, sig_entry - threshold_price, sig_entry + threshold_price
    mode = (mode or "mid").lower()
    if mode == "nearest":
        level = e_min if direction == "SELL" else e_max
    elif mode == "furthest":
        level = e_max if direction == "SELL" else e_min
    else:
        level = round((e_min + e_max) / 2.0, 5)
    if direction == "SELL":
        return level, level, e_max + threshold_price
    return level, e_min - threshold_price, level


def _calculate_lots_and_splits(
    balance: float,
    fill_price: float,
    sl_price: float,
    sym_info,
    rules: dict,
    num_tps: int,
) -> tuple[list[float], float]:
    """
    Compute the lot splits [lot1, lot2, lot3] and planned 1R risk_amount ($)
    matching Sirius Copier's mt5_executor.py logic.
    """
    risk_mode = rules.get("risk_mode", "percent")
    risk_value = float(rules.get("risk_value", 1.0))

    step = sym_info.volume_step if (sym_info and sym_info.volume_step > 0) else 0.01
    min_lot = getattr(config, "MIN_LOT", 0.01)
    point = sym_info.point if (sym_info and sym_info.point > 0) else 0.01
    tick_size = sym_info.trade_tick_size if (sym_info and sym_info.trade_tick_size > 0) else point
    tick_value = sym_info.trade_tick_value if (sym_info and sym_info.trade_tick_value > 0) else 1.0

    sl_dist = abs(fill_price - sl_price) if (sl_price > 0 and fill_price > 0) else 0.0
    loss_per_lot = ((sl_dist / tick_size) * tick_value) if (sl_dist > 0 and tick_size > 0) else 0.0

    if risk_mode == "fixed":
        total_lot = max(min_lot, round(risk_value / step) * step)
    else:
        risk_pct = risk_value / 100.0
        risk_amount_target = balance * risk_pct
        if loss_per_lot > 0:
            calculated_lot = risk_amount_target / loss_per_lot
            total_lot = max(min_lot, round(calculated_lot / step) * step)
        else:
            total_lot = getattr(config, "DEFAULT_LOT_SIZE", 0.03)

    p1 = max(0.0, float(rules.get("tp1_percent", 60.0)))
    p2 = max(0.0, float(rules.get("tp2_percent", 30.0)))
    p3 = max(0.0, float(rules.get("tp3_percent", 10.0)))

    raw_weights = [p1, p2, p3][:max(1, min(3, num_tps))]
    total_w = sum(raw_weights)
    if total_w <= 0:
        raw_weights = [100.0 / len(raw_weights)] * len(raw_weights)
        total_w = 100.0

    lots = []
    for w in raw_weights:
        if w <= 0:
            lots.append(0.0)
        else:
            leg_lot = max(min_lot, round(((total_lot * (w / total_w))) / step) * step)
            lots.append(leg_lot)

    actual_total_lot = sum(lots)
    planned_risk_dollar = round(loss_per_lot * actual_total_lot, 2) if loss_per_lot > 0 else round(balance * (risk_value / 100.0), 2)
    return lots, planned_risk_dollar


def _compute_leg_profit(direction: str, fill_price: float, exit_price: float, lot: float, sym_info) -> float:
    if lot <= 0:
        return 0.0
    point = sym_info.point if (sym_info and sym_info.point > 0) else 0.01
    tick_size = sym_info.trade_tick_size if (sym_info and sym_info.trade_tick_size > 0) else point
    tick_value = sym_info.trade_tick_value if (sym_info and sym_info.trade_tick_value > 0) else 1.0

    price_diff = (exit_price - fill_price) if direction == "BUY" else (fill_price - exit_price)
    pnl = (price_diff / tick_size) * tick_value * lot
    return round(pnl, 2)


def _simulate_signal_ticks(
    sig: dict,
    actual_symbol: str,
    sym_info,
    broker_offset_sec: int,
    rules: dict,
    current_balance: float,
    ticks,
    sim_spread: float,
    spread_mode: str,
) -> dict:
    """
    Precision tick-by-tick simulation using actual broker Bid/Ask tick stream.
    Applies real live spread or user-specified manual stress-test spread.
    """
    direction = sig["direction"]
    order_type = sig["order_type"]
    sig_entry = float(sig.get("entry") or 0.0)
    sig_entry_min = float(sig.get("entry_min") or sig_entry) if sig.get("entry_min") is not None else sig_entry
    sig_entry_max = float(sig.get("entry_max") or sig_entry) if sig.get("entry_max") is not None else sig_entry
    initial_sl = float(sig.get("sl") or 0.0)
    tp_list = [float(x) for x in (sig.get("tp") or []) if float(x) > 0][:3]
    sig_utc = int(sig.get("timestamp") or 0)
    sig_broker_time = sig_utc + broker_offset_sec

    # Pre-simulation Sanity Guard: enforce clean TP ladder & SL direction
    if sig_entry > 0 and tp_list:
        sl_dist = abs(sig_entry - initial_sl) if initial_sl > 0 else 0
        max_tp_dist = max(sig_entry * 0.08, sl_dist * 15.0 if sl_dist > 0 else 60.0)
        clean_tps = []
        for t_val in tp_list:
            if abs(t_val - sig_entry) > max_tp_dist:
                continue
            if direction == "BUY" and t_val <= sig_entry:
                continue
            if direction == "SELL" and t_val >= sig_entry:
                continue
            clean_tps.append(t_val)

        if clean_tps:
            seen = set()
            deduped = [x for x in clean_tps if not (x in seen or seen.add(x))]
            tp_list = sorted(deduped) if direction == "BUY" else sorted(deduped, reverse=True)
        else:
            tp_list = []

    if not tp_list or initial_sl <= 0 or ticks is None or len(ticks) == 0:
        return {
            "msg_id": sig["msg_id"],
            "timestamp": sig_utc,
            "symbol": actual_symbol,
            "direction": direction,
            "order_type": order_type,
            "entry": sig_entry,
            "sl": initial_sl,
            "tp": tp_list,
            "status": "SKIPPED",
            "status_detail": "No historical ticks or invalid/malformed SL/TP",
            "timeframe": "TICKS",
            "lots": 0.0,
            "risk_amount": 0.0,
            "profit": 0.0,
            "realized_r": 0.0,
            "tp1_hit": 0,
            "tp2_hit": 0,
            "tp3_hit": 0,
            "sl_hit": 0,
            "be_hit": 0,
            "activated_at": 0,
            "closed_at": 0,
            "legs": [],
            "raw_text": sig.get("raw_text", ""),
            "updates_applied": [],
        }

    point = sym_info.point if (sym_info and sym_info.point > 0) else 0.01
    threshold_price = getattr(config, "MARKET_ORDER_THRESHOLD", 50) * point

    pending_expiry_mins = float(rules.get("pending_expiry_minutes", 30.0))
    sl_tp1_strat = rules.get("sl_tp1_strategy", "ENTRY")
    sl_tp2_strat = rules.get("sl_tp2_strategy", "TP1")
    sl_tp3_strat = rules.get("sl_tp3_strategy", "TP2")

    updates = sorted(sig.get("updates") or [], key=lambda x: x["timestamp"])
    upd_idx = 0
    updates_applied = []

    # Find the starting tick (first tick at or after sig_broker_time)
    start_tick_idx = 0
    for i in range(len(ticks)):
        if int(ticks[i]['time']) >= sig_broker_time:
            start_tick_idx = i
            break
    t0 = ticks[start_tick_idx]
    t0_time = int(t0['time'])

    raw_b0 = float(t0['bid'])
    raw_a0 = float(t0['ask'])
    if spread_mode == "manual" and sim_spread > 0:
        mid0 = (raw_b0 + raw_a0) / 2.0
        t0_bid = mid0 - (sim_spread / 2.0)
        t0_ask = mid0 + (sim_spread / 2.0)
    else:
        t0_bid = raw_b0
        t0_ask = raw_a0

    is_pending = False
    fill_price = 0.0
    activated_at_broker = 0

    # Entry-zone mode: nearest / mid / furthest price of the provider's stated range
    sig_entry_min, sig_entry_max = _derive_entry_range(sig, sig_entry)
    sig_entry, zone_lo, zone_hi = _resolve_entry_zone(
        direction, sig_entry, sig_entry_min, sig_entry_max,
        rules.get("entry_zone_mode", "mid"), threshold_price,
    )

    if sig_entry > 0:
        # Fill only once price has reached the chosen level inside the stated range
        in_entry_range = zone_lo <= (t0_ask if direction == "BUY" else t0_bid) <= zone_hi
        if in_entry_range:
            is_pending = False
            fill_price = sig_entry
            activated_at_broker = t0_time
        else:
            # Price has not reached stated entry range (or drifted past it); set pending order
            is_pending = True
    else:
        # No price level specified in signal; immediate market execution
        fill_price = t0_ask if direction == "BUY" else t0_bid
        activated_at_broker = t0_time
        is_pending = False

    if not is_pending:
        if (direction == "BUY" and initial_sl >= fill_price) or (direction == "SELL" and initial_sl <= fill_price):
            if sig_entry > 0 and ((direction == "BUY" and initial_sl < sig_entry) or (direction == "SELL" and initial_sl > sig_entry)):
                fill_price = sig_entry
            else:
                return {
                    "msg_id": sig["msg_id"],
                    "timestamp": sig_utc,
                    "symbol": actual_symbol,
                    "direction": direction,
                    "order_type": order_type,
                    "entry": sig_entry,
                    "sl": initial_sl,
                    "tp": tp_list,
                    "status": "SKIPPED",
                    "status_detail": "Invalid SL relative to Entry",
                    "timeframe": "TICKS",
                    "lots": 0.0,
                    "risk_amount": 0.0,
                    "profit": 0.0,
                    "realized_r": 0.0,
                    "tp1_hit": 0,
                    "tp2_hit": 0,
                    "tp3_hit": 0,
                    "sl_hit": 0,
                    "be_hit": 0,
                    "activated_at": 0,
                    "closed_at": 0,
                    "legs": [],
                    "raw_text": sig.get("raw_text", ""),
                    "updates_applied": [],
                }

    legs = []
    lots = []
    risk_amount = 0.0

    def _init_open_legs(f_price: float, f_time_broker: int):
        nonlocal legs, lots, risk_amount
        lots, risk_amount = _calculate_lots_and_splits(
            current_balance, f_price, initial_sl, sym_info, rules, len(tp_list)
        )
        legs = []
        for idx, tp_val in enumerate(tp_list):
            leg_lot = lots[idx] if idx < len(lots) else 0.0
            if leg_lot <= 0:
                continue
            legs.append({
                "tp_num": idx + 1,
                "lot": leg_lot,
                "tp_price": tp_val,
                "sl_price": initial_sl,
                "sl_tag": "SL",
                "status": "OPEN",
                "open_price": f_price,
                "open_time": f_time_broker - broker_offset_sec,
                "close_price": 0.0,
                "close_time": 0,
                "close_reason": "",
                "profit": 0.0,
            })

    if not is_pending:
        _init_open_legs(fill_price, activated_at_broker)

    tp1_hit = 0
    tp2_hit = 0
    tp3_hit = 0
    sl_hit = 0
    be_hit = 0

    def _apply_tp_cascade(tp_num: int, exit_p: float):
        nonlocal tp1_hit, tp2_hit, tp3_hit
        if tp_num == 1:
            tp1_hit = 1
            if sl_tp1_strat == "ENTRY":
                for rem in legs:
                    if rem["status"] == "OPEN":
                        if (direction == "BUY" and fill_price > rem["sl_price"] and fill_price <= exit_p) or \
                           (direction == "SELL" and fill_price < rem["sl_price"] and fill_price >= exit_p):
                            rem["sl_price"] = fill_price
                            rem["sl_tag"] = "BE1"
        elif tp_num == 2:
            tp2_hit = 1
            target_sl = None
            tag = "BE2"
            if sl_tp2_strat == "TP1" and len(tp_list) >= 1:
                cand_sl = tp_list[0]
                if (direction == "BUY" and cand_sl < exit_p) or (direction == "SELL" and cand_sl > exit_p):
                    target_sl = cand_sl
                    tag = "BE2"
                else:
                    if (direction == "BUY" and fill_price <= exit_p) or (direction == "SELL" and fill_price >= exit_p):
                        target_sl = fill_price
                        tag = "BE1"
            elif sl_tp2_strat == "ENTRY":
                if (direction == "BUY" and fill_price <= exit_p) or (direction == "SELL" and fill_price >= exit_p):
                    target_sl = fill_price
                    tag = "BE1"
            if target_sl is not None:
                for rem in legs:
                    if rem["status"] == "OPEN":
                        if (direction == "BUY" and target_sl > rem["sl_price"]) or (direction == "SELL" and target_sl < rem["sl_price"]):
                            rem["sl_price"] = target_sl
                            rem["sl_tag"] = tag
        elif tp_num == 3:
            tp3_hit = 1
            target_sl = None
            tag = "BE3"
            if sl_tp3_strat == "TP2" and len(tp_list) >= 2:
                cand_sl = tp_list[1]
                if (direction == "BUY" and cand_sl < exit_p) or (direction == "SELL" and cand_sl > exit_p):
                    target_sl = cand_sl
                    tag = "BE3"
            elif sl_tp3_strat == "TP1" and len(tp_list) >= 1:
                cand_sl = tp_list[0]
                if (direction == "BUY" and cand_sl < exit_p) or (direction == "SELL" and cand_sl > exit_p):
                    target_sl = cand_sl
                    tag = "BE2"
            elif sl_tp3_strat == "ENTRY":
                if (direction == "BUY" and fill_price <= exit_p) or (direction == "SELL" and fill_price >= exit_p):
                    target_sl = fill_price
                    tag = "BE1"
            if target_sl is not None:
                for rem in legs:
                    if rem["status"] == "OPEN":
                        if (direction == "BUY" and target_sl > rem["sl_price"]) or (direction == "SELL" and target_sl < rem["sl_price"]):
                            rem["sl_price"] = target_sl
                            rem["sl_tag"] = tag

    last_tick_time_utc = sig_utc
    last_tick_bid = t0_bid
    last_tick_ask = t0_ask

    # Process ticks from start_tick_idx forward
    for i in range(start_tick_idx, len(ticks)):
        t = ticks[i]
        t_time_broker = int(t['time'])
        t_time_utc = t_time_broker - broker_offset_sec
        raw_b = float(t['bid'])
        raw_a = float(t['ask'])

        if spread_mode == "manual" and sim_spread > 0:
            mid = (raw_b + raw_a) / 2.0
            t_bid = mid - (sim_spread / 2.0)
            t_ask = mid + (sim_spread / 2.0)
        else:
            t_bid = raw_b
            t_ask = raw_a

        last_tick_time_utc = t_time_utc
        last_tick_bid = t_bid
        last_tick_ask = t_ask

        # 1. Apply any provider updates
        while upd_idx < len(updates) and (updates[upd_idx]["timestamp"] + broker_offset_sec) <= t_time_broker:
            u = updates[upd_idx]
            upd_idx += 1
            u_act = u["action"]

            if is_pending:
                if u_act in ("CANCEL", "CLOSE"):
                    updates_applied.append(f"{u_act}: {u.get('text', '')}")
                    return {
                        "msg_id": sig["msg_id"],
                        "timestamp": sig_utc,
                        "symbol": actual_symbol,
                        "direction": direction,
                        "order_type": order_type,
                        "entry": sig_entry,
                        "sl": initial_sl,
                        "tp": tp_list,
                        "status": "CANCELLED",
                        "status_detail": "Cancelled by provider before activation",
                        "timeframe": "TICKS",
                        "lots": 0.0,
                        "risk_amount": 0.0,
                        "profit": 0.0,
                        "realized_r": 0.0,
                        "tp1_hit": 0,
                        "tp2_hit": 0,
                        "tp3_hit": 0,
                        "sl_hit": 0,
                        "be_hit": 0,
                        "activated_at": 0,
                        "closed_at": u["timestamp"],
                        "legs": [],
                        "raw_text": sig.get("raw_text", ""),
                        "updates_applied": updates_applied,
                    }
                elif u_act == "MODIFY_SL" and u.get("new_sl"):
                    cand_sl = float(u["new_sl"])
                    ref_p = fill_price or initial_sl
                    if ref_p > 0 and abs(cand_sl - ref_p) / ref_p <= 0.10:
                        initial_sl = cand_sl
                        updates_applied.append(f"Adjust SL -> {initial_sl}")
            else:
                if u_act == "CLOSE":
                    updates_applied.append(f"CLOSE: {u.get('text', '')}")
                    exit_m = t_bid if direction == "BUY" else t_ask
                    for leg in legs:
                        if leg["status"] == "OPEN":
                            leg["status"] = "CLOSED"
                            leg["close_price"] = exit_m
                            leg["close_time"] = u["timestamp"]
                            leg["close_reason"] = "MANUAL"
                            leg["profit"] = _compute_leg_profit(direction, leg["open_price"], exit_m, leg["lot"], sym_info)
                elif u_act == "SET_BE":
                    updates_applied.append("SET_BE")
                    curr_m = t_bid if direction == "BUY" else t_ask
                    is_in_profit = (direction == "BUY" and curr_m > fill_price) or (direction == "SELL" and curr_m < fill_price)
                    for leg in legs:
                        if leg["status"] == "OPEN":
                            if is_in_profit:
                                if (direction == "BUY" and fill_price > leg["sl_price"]) or (direction == "SELL" and fill_price < leg["sl_price"]):
                                    leg["sl_price"] = fill_price
                                    leg["sl_tag"] = "BE1"
                            else:
                                if (direction == "BUY" and fill_price < leg["tp_price"]) or (direction == "SELL" and fill_price > leg["tp_price"]):
                                    leg["tp_price"] = fill_price
                                    leg["tp_tag"] = "BE1"
                elif u_act == "MODIFY_SL" and u.get("new_sl"):
                    cand_sl = float(u["new_sl"])
                    ref_p = fill_price or initial_sl
                    if ref_p > 0 and abs(cand_sl - ref_p) / ref_p <= 0.10:
                        updates_applied.append(f"Adjust SL -> {cand_sl}")
                        for leg in legs:
                            if leg["status"] == "OPEN":
                                leg["sl_price"] = cand_sl

        if not is_pending and all(l["status"] == "CLOSED" for l in legs):
            break

        # 2. Check pending expiry or activation
        if is_pending:
            if pending_expiry_mins > 0 and (t_time_broker - sig_broker_time) >= (pending_expiry_mins * 60):
                return {
                    "msg_id": sig["msg_id"],
                    "timestamp": sig_utc,
                    "symbol": actual_symbol,
                    "direction": direction,
                    "order_type": order_type,
                    "entry": sig_entry,
                    "sl": initial_sl,
                    "tp": tp_list,
                    "status": "EXPIRED",
                    "status_detail": f"Pending order expired after {int(pending_expiry_mins)}m",
                    "timeframe": "TICKS",
                    "lots": 0.0,
                    "risk_amount": 0.0,
                    "profit": 0.0,
                    "realized_r": 0.0,
                    "tp1_hit": 0,
                    "tp2_hit": 0,
                    "tp3_hit": 0,
                    "sl_hit": 0,
                    "be_hit": 0,
                    "activated_at": 0,
                    "closed_at": t_time_utc,
                    "legs": [],
                    "raw_text": sig.get("raw_text", ""),
                    "updates_applied": updates_applied,
                }

            # Check activation: price must reach the chosen level within the stated entry range
            if direction == "BUY":
                triggered = (zone_lo <= t_ask <= zone_hi)
            else: # SELL
                triggered = (zone_lo <= t_bid <= zone_hi)

            if triggered:
                is_pending = False
                fill_price = sig_entry
                activated_at_broker = t_time_broker
                _init_open_legs(fill_price, activated_at_broker)

        # 3. Check open trade TP / SL on this tick
        if not is_pending:
            for leg in legs:
                if leg["status"] != "OPEN":
                    continue

                if direction == "BUY":
                    # Check SL (bid dropped into sl)
                    if t_bid <= leg["sl_price"]:
                        leg["status"] = "CLOSED"
                        leg["close_price"] = leg["sl_price"]
                        leg["close_time"] = t_time_utc
                        leg["close_reason"] = leg["sl_tag"]
                        leg["profit"] = _compute_leg_profit(direction, leg["open_price"], leg["close_price"], leg["lot"], sym_info)
                        if leg["sl_tag"].startswith("BE"):
                            be_hit = 1
                        continue
                    # Check TP (bid reached tp)
                    if t_bid >= leg["tp_price"]:
                        leg["status"] = "CLOSED"
                        leg["close_price"] = leg["tp_price"]
                        leg["close_time"] = t_time_utc
                        leg["profit"] = _compute_leg_profit(direction, leg["open_price"], leg["close_price"], leg["lot"], sym_info)
                        if leg.get("tp_tag", "").startswith("BE"):
                            leg["close_reason"] = leg["tp_tag"]
                            be_hit = 1
                        else:
                            tp_num = leg["tp_num"]
                            leg["close_reason"] = f"TP{tp_num}"
                            _apply_tp_cascade(tp_num, leg["tp_price"])
                else: # SELL
                    # Check SL (ask rose into sl)
                    if t_ask >= leg["sl_price"]:
                        leg["status"] = "CLOSED"
                        leg["close_price"] = leg["sl_price"]
                        leg["close_time"] = t_time_utc
                        leg["close_reason"] = leg["sl_tag"]
                        leg["profit"] = _compute_leg_profit(direction, leg["open_price"], leg["close_price"], leg["lot"], sym_info)
                        if leg["sl_tag"].startswith("BE"):
                            be_hit = 1
                        continue
                    # Check TP (ask dropped to tp)
                    if t_ask <= leg["tp_price"]:
                        leg["status"] = "CLOSED"
                        leg["close_price"] = leg["tp_price"]
                        leg["close_time"] = t_time_utc
                        leg["profit"] = _compute_leg_profit(direction, leg["open_price"], leg["close_price"], leg["lot"], sym_info)
                        if leg.get("tp_tag", "").startswith("BE"):
                            leg["close_reason"] = leg["tp_tag"]
                            be_hit = 1
                        else:
                            tp_num = leg["tp_num"]
                            leg["close_reason"] = f"TP{tp_num}"
                            _apply_tp_cascade(tp_num, leg["tp_price"])

            if all(l["status"] == "CLOSED" for l in legs):
                break

    if is_pending:
        return {
            "msg_id": sig["msg_id"],
            "timestamp": sig_utc,
            "symbol": actual_symbol,
            "direction": direction,
            "order_type": order_type,
            "entry": sig_entry,
            "sl": initial_sl,
            "tp": tp_list,
            "status": "EXPIRED",
            "status_detail": "Order never triggered within tick simulation window",
            "timeframe": "TICKS",
            "lots": 0.0,
            "risk_amount": 0.0,
            "profit": 0.0,
            "realized_r": 0.0,
            "tp1_hit": 0,
            "tp2_hit": 0,
            "tp3_hit": 0,
            "sl_hit": 0,
            "be_hit": 0,
            "activated_at": 0,
            "closed_at": last_tick_time_utc,
            "legs": [],
            "raw_text": sig.get("raw_text", ""),
            "updates_applied": updates_applied,
        }

    # Mark-to-market close any remaining open legs at final tick
    exit_final = last_tick_bid if direction == "BUY" else last_tick_ask
    for leg in legs:
        if leg["status"] == "OPEN":
            leg["status"] = "CLOSED"
            leg["close_price"] = exit_final
            leg["close_time"] = last_tick_time_utc
            leg["close_reason"] = "END_OF_DATA"
            leg["profit"] = _compute_leg_profit(direction, leg["open_price"], exit_final, leg["lot"], sym_info)

    # Apply ECN Commission if configured (e.g. cTrader RAW ECN default $6.00/lot round-turn)
    platform_mode = str(rules.get("platform", "mt5")).lower()
    comm_per_lot = float(rules.get("commission_per_lot", 6.0 if platform_mode == "ctrader" else 0.0) or 0.0)
    if comm_per_lot > 0:
        for leg in legs:
            comm = round(leg["lot"] * comm_per_lot, 2)
            leg["commission"] = -comm
            leg["profit"] = round(leg["profit"] - comm, 2)

    total_profit = round(sum(l["profit"] for l in legs), 2)
    total_lots = round(sum(l["lot"] for l in legs), 2)
    closed_at_utc = max((l["close_time"] for l in legs), default=sig_utc)

    if tp1_hit == 0 and tp2_hit == 0 and tp3_hit == 0:
        if any(l["close_reason"] == "SL" for l in legs) or (not be_hit and total_profit < 0):
            sl_hit = 1

    realized_r = round(total_profit / risk_amount, 2) if risk_amount > 0 else (-1.0 if sl_hit else 0.0)

    if tp3_hit:
        outcome_badge = "TP3"
    elif tp2_hit:
        outcome_badge = "TP2"
    elif tp1_hit:
        outcome_badge = "TP1"
    elif sl_hit:
        outcome_badge = "SL"
    elif be_hit:
        outcome_badge = "BE1"
    else:
        outcome_badge = legs[-1]["close_reason"] if legs else "CLOSED"

    return {
        "msg_id": sig["msg_id"],
        "timestamp": sig_utc,
        "symbol": actual_symbol,
        "direction": direction,
        "order_type": order_type,
        "entry": round(fill_price, sym_info.digits if sym_info else 2),
        "signal_entry": sig_entry,
        "sl": initial_sl,
        "tp": tp_list,
        "status": "CLOSED",
        "outcome_badge": outcome_badge,
        "timeframe": "TICKS",
        "lots": total_lots,
        "risk_amount": risk_amount,
        "profit": total_profit,
        "realized_r": realized_r,
        "tp1_hit": tp1_hit,
        "tp2_hit": tp2_hit,
        "tp3_hit": tp3_hit,
        "sl_hit": sl_hit,
        "be_hit": be_hit,
        "activated_at": activated_at_broker - broker_offset_sec,
        "closed_at": closed_at_utc,
        "legs": legs,
        "raw_text": sig.get("raw_text", ""),
        "provider_name": sig.get("provider_name", ""),
        "updates_applied": updates_applied,
    }


def _simulate_single_signal(
    sig: dict,
    sym_map: dict,
    sym_info_cache: dict,
    broker_offset_sec: int,
    rules: dict,
    current_balance: float,
    candle_cache: dict = None,
    ct_client: CTraderClient = None,
) -> dict:
    """
    Simulate a single historical signal on MT5 OHLC candles.
    If candle_cache is provided, reuses pre-fetched (rates, tf_name, tf_sec) by msg_id.
    """
    direction = sig["direction"]
    order_type = sig["order_type"]
    sig_entry = float(sig.get("entry") or 0.0)
    sig_entry_min = float(sig.get("entry_min") or sig_entry) if sig.get("entry_min") is not None else sig_entry
    sig_entry_max = float(sig.get("entry_max") or sig_entry) if sig.get("entry_max") is not None else sig_entry
    initial_sl = float(sig.get("sl") or 0.0)
    tp_list = [float(x) for x in (sig.get("tp") or []) if float(x) > 0][:3]
    sig_utc = int(sig.get("timestamp") or 0)
    sig_broker_time = sig_utc + broker_offset_sec

    raw_inst = (sig.get("instrument") or "XAUUSD").upper().replace("/", "").replace("-", "").replace("_", "").replace("#", "")
    actual_symbol = sym_map.get(raw_inst)

    # Symbol filtering: If the instrument cannot be mapped to any broker symbol, skip it cleanly
    # (prevents foreign symbols like EURUSD at 1.08 from simulating against gold at 4000)
    if not actual_symbol:
        if raw_inst in ("XAUUSD", "GOLD", "XAU"):
            actual_symbol = sym_map.get("XAUUSD") or getattr(config, "SYMBOL", "XAUUSD")
        else:
            return {
                "msg_id": sig["msg_id"],
                "timestamp": sig_utc,
                "symbol": raw_inst,
                "direction": direction,
                "order_type": order_type,
                "entry": sig_entry,
                "sl": initial_sl,
                "tp": tp_list,
                "status": "SKIPPED",
                "status_detail": f"Unmapped instrument: {raw_inst}",
                "timeframe": "NONE",
                "lots": 0.0,
                "risk_amount": 0.0,
                "profit": 0.0,
                "realized_r": 0.0,
                "tp1_hit": 0,
                "tp2_hit": 0,
                "tp3_hit": 0,
                "sl_hit": 0,
                "be_hit": 0,
                "activated_at": 0,
                "closed_at": 0,
                "legs": [],
                "raw_text": sig.get("raw_text", ""),
                "updates_applied": [],
            }

    if actual_symbol not in sym_info_cache:
        if ct_client and ct_client.is_authorized:
            sym_info_cache[actual_symbol] = ct_client.get_symbol_info(actual_symbol) or CTraderSymbolInfo(name=actual_symbol, symbol_id=1)
        else:
            mt5.symbol_select(actual_symbol, True)
            sym_info_cache[actual_symbol] = mt5.symbol_info(actual_symbol)
    sym_info = sym_info_cache[actual_symbol]

    # Spread and Fidelity Configuration
    platform_mode = str(rules.get("platform", "mt5")).lower()
    execution_fidelity = str(rules.get("execution_fidelity", "ticks")).lower()
    spread_mode = str(rules.get("spread_mode", "auto")).lower()
    manual_spread_pips = max(0.0, float(rules.get("manual_spread_pips", 0.0) or 0.0))

    point = sym_info.point if (sym_info and sym_info.point > 0) else 0.01
    pip_size = point * 10.0
    if spread_mode == "manual" and manual_spread_pips > 0:
        sim_spread = manual_spread_pips * pip_size
    else:
        spread_pts = getattr(sym_info, "spread", 0) if sym_info else 0
        base_spread = (spread_pts * point) if spread_pts > 0 else (point * 50)
        # cTrader RAW ECN accounts have ~60% tighter raw spreads than standard retail MT5 accounts (offset by commission)
        sim_spread = round(base_spread * 0.4, 5) if platform_mode == "ctrader" else base_spread

    # Hybrid 1-Minute Candle + Surgical 1-Minute Tick Drilldown:
    # Always fetch 1-minute (M1) candles first (up to 5 days from signal timestamp, or from pre-fetched cache).
    # When execution_fidelity == "ticks", the simulator walks M1 candles and surgically fetches millisecond-level
    # tick data ONLY for the specific 1-minute candles whose [Low, High] touches Entry, TP1/TP2/TP3, or SL.
    max_hold_sec = 5 * 86400
    cache_key = (sig["msg_id"], actual_symbol, platform_mode)
    if candle_cache is not None and cache_key in candle_cache:
        valid_bars, tf_name, tf_sec = candle_cache[cache_key]
    else:
        rates, tf_name, tf_sec = _fetch_candles_for_window(actual_symbol, sig_broker_time, sig_broker_time + max_hold_sec, ct_client=ct_client)
        if rates is not None and len(rates) > 0:
            start_bar_floor = (sig_broker_time // tf_sec) * tf_sec
            raw_valid = [b for b in rates if int(b["time"]) >= start_bar_floor]
            if not raw_valid:
                raw_valid = list(rates)
            # Pre-convert to plain Python tuples: (t_broker, open, high, low, close, segments)
            valid_bars = []
            for b in raw_valid:
                bt = int(b["time"])
                bo = float(b["open"])
                bh = float(b["high"])
                bl = float(b["low"])
                bc = float(b["close"])
                if bc >= bo:
                    segs = ((bo, bl), (bl, bh), (bh, bc))
                else:
                    segs = ((bo, bh), (bh, bl), (bl, bc))
                valid_bars.append((bt, bo, bh, bl, bc, segs))
        else:
            valid_bars = []
        if candle_cache is not None:
            candle_cache[cache_key] = (valid_bars, tf_name, tf_sec)

    # Pre-simulation Sanity Guard: enforce clean TP ladder & SL direction
    if sig_entry > 0 and tp_list:
        sl_dist = abs(sig_entry - initial_sl) if initial_sl > 0 else 0
        max_tp_dist = max(sig_entry * 0.08, sl_dist * 15.0 if sl_dist > 0 else 60.0)
        clean_tps = []
        for t_val in tp_list:
            if abs(t_val - sig_entry) > max_tp_dist:
                continue
            if direction == "BUY" and t_val <= sig_entry:
                continue
            if direction == "SELL" and t_val >= sig_entry:
                continue
            clean_tps.append(t_val)

        if clean_tps:
            seen = set()
            deduped = [x for x in clean_tps if not (x in seen or seen.add(x))]
            tp_list = sorted(deduped) if direction == "BUY" else sorted(deduped, reverse=True)
        else:
            tp_list = []

    if not valid_bars or not tp_list or initial_sl <= 0:
        return {
            "msg_id": sig["msg_id"],
            "timestamp": sig_utc,
            "symbol": actual_symbol,
            "direction": direction,
            "order_type": order_type,
            "entry": sig_entry,
            "sl": initial_sl,
            "tp": tp_list,
            "status": "SKIPPED",
            "status_detail": "No historical bars or invalid/malformed SL/TP",
            "timeframe": tf_name,
            "lots": 0.0,
            "risk_amount": 0.0,
            "profit": 0.0,
            "realized_r": 0.0,
            "tp1_hit": 0,
            "tp2_hit": 0,
            "tp3_hit": 0,
            "sl_hit": 0,
            "be_hit": 0,
            "activated_at": 0,
            "closed_at": 0,
            "legs": [],
            "raw_text": sig.get("raw_text", ""),
            "updates_applied": [],
        }

    first_bar = valid_bars[0]
    first_open = first_bar[1]
    first_high = first_bar[2]
    first_low = first_bar[3]
    point = sym_info.point if (sym_info and sym_info.point > 0) else 0.01
    threshold_price = getattr(config, "MARKET_ORDER_THRESHOLD", 50) * point

    # Spread compensation for backtester (sim_spread already computed from spread_mode / manual_spread_pips)
    spread_offset_entry = bool(rules.get("spread_offset_entry", True))
    spread_offset_tp = bool(rules.get("spread_offset_tp", True))

    # Entry-zone mode: nearest / mid / furthest price of the provider's stated range
    sig_entry_min, sig_entry_max = _derive_entry_range(sig, sig_entry)
    sig_entry, zone_lo, zone_hi = _resolve_entry_zone(
        direction, sig_entry, sig_entry_min, sig_entry_max,
        rules.get("entry_zone_mode", "mid"), threshold_price,
    )

    if spread_offset_entry and sig_entry > 0 and direction == "BUY":
        sig_entry = sig_entry + sim_spread
        zone_lo += sim_spread
        zone_hi += sim_spread

    if spread_offset_tp and tp_list and direction == "SELL":
        adjusted_tps = []
        for tp_val in tp_list:
            cand_tp = tp_val + sim_spread
            if sig_entry > 0 and cand_tp >= sig_entry:
                cand_tp = tp_val
            adjusted_tps.append(cand_tp)
        tp_list = adjusted_tps

    # Determine whether order activates immediately at market or waits as PENDING
    is_pending = False
    fill_price = 0.0
    activated_at_broker = 0
    surgical_ticks_used = False

    if sig_entry > 0:
        # Fill if the first candle opens inside the chosen band or trades through the chosen level
        if (zone_lo <= first_open <= zone_hi) or (first_low <= sig_entry <= first_high):
            if execution_fidelity == "ticks":
                # Let the main loop surgically inspect the first 1-minute candle's exact ticks at Entry/TP/SL
                is_pending = True
            else:
                fill_price = sig_entry
                activated_at_broker = first_bar[0]
                is_pending = False
        else:
            is_pending = True
    else:
        fill_price = first_open
        activated_at_broker = first_bar[0]
        is_pending = False

    # Sanity check on SL vs Entry direction for immediate fills
    if not is_pending:
        if (direction == "BUY" and initial_sl >= fill_price) or (direction == "SELL" and initial_sl <= fill_price):
            if sig_entry > 0 and ((direction == "BUY" and initial_sl < sig_entry) or (direction == "SELL" and initial_sl > sig_entry)):
                fill_price = sig_entry
            else:
                return {
                    "msg_id": sig["msg_id"],
                    "timestamp": sig_utc,
                    "symbol": actual_symbol,
                    "direction": direction,
                    "order_type": order_type,
                    "entry": sig_entry,
                    "sl": initial_sl,
                    "tp": tp_list,
                    "status": "SKIPPED",
                    "status_detail": "Invalid SL relative to Entry",
                    "timeframe": tf_name,
                    "lots": 0.0,
                    "risk_amount": 0.0,
                    "profit": 0.0,
                    "realized_r": 0.0,
                    "tp1_hit": 0,
                    "tp2_hit": 0,
                    "tp3_hit": 0,
                    "sl_hit": 0,
                    "be_hit": 0,
                    "activated_at": 0,
                    "closed_at": 0,
                    "legs": [],
                    "raw_text": sig.get("raw_text", ""),
                    "updates_applied": [],
                }

    pending_expiry_mins = float(rules.get("pending_expiry_minutes", 30.0))
    sl_tp1_strat = rules.get("sl_tp1_strategy", "ENTRY")
    sl_tp2_strat = rules.get("sl_tp2_strategy", "TP1")
    sl_tp3_strat = rules.get("sl_tp3_strategy", "TP2")

    # Sort provider updates chronologically and convert timestamps to broker time
    updates = sorted(sig.get("updates") or [], key=lambda x: x["timestamp"])
    upd_idx = 0
    updates_applied = []

    legs = []
    lots = []
    risk_amount = 0.0

    def _init_open_legs(f_price: float, f_time_broker: int):
        nonlocal legs, lots, risk_amount
        lots, risk_amount = _calculate_lots_and_splits(
            current_balance, f_price, initial_sl, sym_info, rules, len(tp_list)
        )
        legs = []
        for idx, tp_val in enumerate(tp_list):
            leg_lot = lots[idx] if idx < len(lots) else 0.0
            if leg_lot <= 0:
                continue
            legs.append({
                "tp_num": idx + 1,
                "lot": leg_lot,
                "tp_price": tp_val,
                "sl_price": initial_sl,
                "sl_tag": "SL",
                "status": "OPEN",
                "open_price": f_price,
                "open_time": f_time_broker - broker_offset_sec,
                "close_price": 0.0,
                "close_time": 0,
                "close_reason": "",
                "profit": 0.0,
            })

    if not is_pending:
        _init_open_legs(fill_price, activated_at_broker)

    tp1_hit = 0
    tp2_hit = 0
    tp3_hit = 0
    sl_hit = 0
    be_hit = 0

    def _evaluate_open_segment(p_start: float, p_end: float, bar_time_utc: int):
        nonlocal tp1_hit, tp2_hit, tp3_hit, sl_hit, be_hit
        seg_low = p_start if p_start < p_end else p_end
        seg_high = p_end if p_start < p_end else p_start

        for leg in legs:
            if leg["status"] != "OPEN":
                continue

            # 1. Check Stop Loss hit on this monotonic segment
            sl_p = leg["sl_price"]
            sl_triggered = (direction == "BUY" and seg_low <= sl_p) or (direction == "SELL" and seg_high >= sl_p)
            if sl_triggered:
                # Sanity boundary check: exit price cannot exceed the candle's price bounds
                if direction == "BUY":
                    exit_p = min(sl_p, seg_high)
                    if sl_p > seg_high:
                        exit_p = seg_low
                else:
                    exit_p = max(sl_p, seg_low)
                    if sl_p < seg_low:
                        exit_p = seg_high
                leg["status"] = "CLOSED"
                leg["close_price"] = exit_p
                leg["close_time"] = bar_time_utc
                leg["close_reason"] = leg["sl_tag"]
                leg["profit"] = _compute_leg_profit(direction, leg["open_price"], exit_p, leg["lot"], sym_info)
                if leg["sl_tag"].startswith("BE"):
                    be_hit = 1
                continue

            # 2. Check Take Profit hit on this monotonic segment
            tp_p = leg["tp_price"]
            tp_triggered = (direction == "BUY" and seg_high >= tp_p) or (direction == "SELL" and seg_low <= tp_p)
            if tp_triggered:
                exit_p = tp_p
                leg["status"] = "CLOSED"
                leg["close_price"] = exit_p
                leg["close_time"] = bar_time_utc
                leg["profit"] = _compute_leg_profit(direction, leg["open_price"], exit_p, leg["lot"], sym_info)
                if leg.get("tp_tag", "").startswith("BE"):
                    leg["close_reason"] = leg["tp_tag"]
                    be_hit = 1
                    continue
                tp_num = leg["tp_num"]
                leg["close_reason"] = f"TP{tp_num}"

                if tp_num == 1:
                    tp1_hit = 1
                    # Apply TP1 SL strategy to remaining open legs
                    if sl_tp1_strat == "ENTRY":
                        for rem in legs:
                            if rem["status"] == "OPEN":
                                if (direction == "BUY" and fill_price > rem["sl_price"] and fill_price <= exit_p) or \
                                   (direction == "SELL" and fill_price < rem["sl_price"] and fill_price >= exit_p):
                                    rem["sl_price"] = fill_price
                                    rem["sl_tag"] = "BE1"
                elif tp_num == 2:
                    tp2_hit = 1
                    # Apply TP2 SL strategy to remaining open legs
                    target_sl = None
                    tag = "BE2"
                    if sl_tp2_strat == "TP1" and len(tp_list) >= 1:
                        cand_sl = tp_list[0]
                        # Crucial Sanity Check: For BUY, target SL must be < TP2 exit_p; For SELL, > TP2 exit_p
                        if (direction == "BUY" and cand_sl < exit_p) or (direction == "SELL" and cand_sl > exit_p):
                            target_sl = cand_sl
                            tag = "BE2"
                        else:
                            if (direction == "BUY" and fill_price <= exit_p) or (direction == "SELL" and fill_price >= exit_p):
                                target_sl = fill_price
                                tag = "BE1"
                    elif sl_tp2_strat == "ENTRY":
                        if (direction == "BUY" and fill_price <= exit_p) or (direction == "SELL" and fill_price >= exit_p):
                            target_sl = fill_price
                            tag = "BE1"
                    if target_sl is not None:
                        for rem in legs:
                            if rem["status"] == "OPEN":
                                if (direction == "BUY" and target_sl > rem["sl_price"]) or (direction == "SELL" and target_sl < rem["sl_price"]):
                                    rem["sl_price"] = target_sl
                                    rem["sl_tag"] = tag
                elif tp_num == 3:
                    tp3_hit = 1
                    target_sl = None
                    tag = "BE3"
                    if sl_tp3_strat == "TP2" and len(tp_list) >= 2:
                        cand_sl = tp_list[1]
                        if (direction == "BUY" and cand_sl < exit_p) or (direction == "SELL" and cand_sl > exit_p):
                            target_sl = cand_sl
                            tag = "BE3"
                    elif sl_tp3_strat == "TP1" and len(tp_list) >= 1:
                        cand_sl = tp_list[0]
                        if (direction == "BUY" and cand_sl < exit_p) or (direction == "SELL" and cand_sl > exit_p):
                            target_sl = cand_sl
                            tag = "BE2"
                    elif sl_tp3_strat == "ENTRY":
                        if (direction == "BUY" and fill_price <= exit_p) or (direction == "SELL" and fill_price >= exit_p):
                            target_sl = fill_price
                            tag = "BE1"
                    if target_sl is not None:
                        for rem in legs:
                            if rem["status"] == "OPEN":
                                if (direction == "BUY" and target_sl > rem["sl_price"]) or (direction == "SELL" and target_sl < rem["sl_price"]):
                                    rem["sl_price"] = target_sl
                                    rem["sl_tag"] = tag

    # Step bar-by-bar using pre-converted tuples
    for b_time_broker, b_open, b_high, b_low, b_close, segments in valid_bars:
        b_time_utc = b_time_broker - broker_offset_sec

        # Process any provider updates that arrived up to the end of this candle
        while upd_idx < len(updates) and (updates[upd_idx]["timestamp"] + broker_offset_sec) <= (b_time_broker + tf_sec):
            u = updates[upd_idx]
            upd_idx += 1
            u_act = u["action"]

            if is_pending:
                if u_act in ("CANCEL", "CLOSE"):
                    updates_applied.append(f"{u_act}: {u.get('text', '')}")
                    return {
                        "msg_id": sig["msg_id"],
                        "timestamp": sig_utc,
                        "symbol": actual_symbol,
                        "direction": direction,
                        "order_type": order_type,
                        "entry": sig_entry,
                        "sl": initial_sl,
                        "tp": tp_list,
                        "status": "CANCELLED",
                        "status_detail": "Cancelled by provider before activation",
                        "timeframe": tf_name,
                        "lots": 0.0,
                        "risk_amount": 0.0,
                        "profit": 0.0,
                        "realized_r": 0.0,
                        "tp1_hit": 0,
                        "tp2_hit": 0,
                        "tp3_hit": 0,
                        "sl_hit": 0,
                        "be_hit": 0,
                        "activated_at": 0,
                        "closed_at": u["timestamp"],
                        "legs": [],
                        "raw_text": sig.get("raw_text", ""),
                        "updates_applied": updates_applied,
                    }
                elif u_act == "MODIFY_SL" and u.get("new_sl"):
                    cand_sl = float(u["new_sl"])
                    ref_p = fill_price or initial_sl
                    if ref_p > 0 and abs(cand_sl - ref_p) / ref_p <= 0.10:
                        initial_sl = cand_sl
                        updates_applied.append(f"Adjust SL -> {initial_sl}")
            else:
                # Trade is already OPEN
                if u_act == "CLOSE":
                    updates_applied.append(f"CLOSE: {u.get('text', '')}")
                    for leg in legs:
                        if leg["status"] == "OPEN":
                            leg["status"] = "CLOSED"
                            leg["close_price"] = b_open
                            leg["close_time"] = u["timestamp"]
                            leg["close_reason"] = "MANUAL"
                            leg["profit"] = _compute_leg_profit(direction, leg["open_price"], b_open, leg["lot"], sym_info)
                elif u_act == "SET_BE":
                    updates_applied.append("SET_BE")
                    is_in_profit = (direction == "BUY" and b_open > fill_price) or (direction == "SELL" and b_open < fill_price)
                    for leg in legs:
                        if leg["status"] == "OPEN":
                            if is_in_profit:
                                if (direction == "BUY" and fill_price > leg["sl_price"]) or (direction == "SELL" and fill_price < leg["sl_price"]):
                                    leg["sl_price"] = fill_price
                                    leg["sl_tag"] = "BE1"
                            else:
                                if (direction == "BUY" and fill_price < leg["tp_price"]) or (direction == "SELL" and fill_price > leg["tp_price"]):
                                    leg["tp_price"] = fill_price
                                    leg["tp_tag"] = "BE1"
                elif u_act == "MODIFY_SL" and u.get("new_sl"):
                    cand_sl = float(u["new_sl"])
                    ref_p = fill_price or initial_sl
                    # Sanity check: must be on correct side and within 10% of price
                    is_realistic = (ref_p > 0 and abs(cand_sl - ref_p) / ref_p <= 0.10)
                    if is_realistic:
                        updates_applied.append(f"Adjust SL -> {cand_sl}")
                        for leg in legs:
                            if leg["status"] == "OPEN":
                                leg["sl_price"] = cand_sl

        # Check if all legs were closed by a provider update
        if not is_pending and all(l["status"] == "CLOSED" for l in legs):
            break

        if is_pending:
            # Rule 2: Check pending order expiry timer (if not already cancelled by provider)
            if pending_expiry_mins > 0 and (b_time_broker - sig_broker_time) >= (pending_expiry_mins * 60):
                return {
                    "msg_id": sig["msg_id"],
                    "timestamp": sig_utc,
                    "symbol": actual_symbol,
                    "direction": direction,
                    "order_type": order_type,
                    "entry": sig_entry,
                    "sl": initial_sl,
                    "tp": tp_list,
                    "status": "EXPIRED",
                    "status_detail": f"Pending order expired after {int(pending_expiry_mins)}m",
                    "timeframe": tf_name,
                    "lots": 0.0,
                    "risk_amount": 0.0,
                    "profit": 0.0,
                    "realized_r": 0.0,
                    "tp1_hit": 0,
                    "tp2_hit": 0,
                    "tp3_hit": 0,
                    "sl_hit": 0,
                    "be_hit": 0,
                    "activated_at": 0,
                    "closed_at": b_time_utc,
                    "legs": [],
                    "raw_text": sig.get("raw_text", ""),
                    "updates_applied": updates_applied,
                }

            # Check if this 1-minute candle touches the entry level or entry zone
            candle_touches_entry = (b_low <= sig_entry <= b_high) or (b_low <= zone_hi and b_high >= zone_lo)
            if candle_touches_entry and execution_fidelity == "ticks":
                bar_ticks = _fetch_surgical_1m_ticks(actual_symbol, b_time_utc, tf_sec, ct_client=ct_client, candle_cache=candle_cache)
                if bar_ticks:
                    surgical_ticks_used = True
                    prev_bid = b_open
                    for tk_utc, tk_bid in bar_ticks:
                        if tk_utc < sig_utc:
                            prev_bid = tk_bid
                            continue
                        if is_pending:
                            s_min = prev_bid if prev_bid < tk_bid else tk_bid
                            s_max = tk_bid if prev_bid < tk_bid else prev_bid
                            if (s_min <= sig_entry <= s_max) or (zone_lo <= tk_bid <= zone_hi):
                                is_pending = False
                                fill_price = sig_entry if (s_min <= sig_entry <= s_max) else tk_bid
                                activated_at_broker = tk_utc + broker_offset_sec
                                _init_open_legs(fill_price, activated_at_broker)
                                _evaluate_open_segment(fill_price, tk_bid, tk_utc)
                        else:
                            _evaluate_open_segment(prev_bid, tk_bid, tk_utc)
                            if all(l["status"] == "CLOSED" for l in legs):
                                break
                        prev_bid = tk_bid

            # Walk the 3 directional segments (or act as boundary safety net if cTrader candle wick slightly exceeded tick feed)
            if is_pending or not all(l["status"] == "CLOSED" for l in legs):
                for seg_start, seg_end in segments:
                    if is_pending:
                        s_min = seg_start if seg_start < seg_end else seg_end
                        s_max = seg_end if seg_start < seg_end else seg_start
                        if (s_min <= sig_entry <= s_max) or (zone_lo <= seg_start <= zone_hi):
                            is_pending = False
                            fill_price = sig_entry
                            activated_at_broker = b_time_broker
                            _init_open_legs(fill_price, activated_at_broker)
                            # Evaluate the remainder of this segment from fill_price -> seg_end
                            _evaluate_open_segment(fill_price, seg_end, b_time_utc)
                    else:
                        _evaluate_open_segment(seg_start, seg_end, b_time_utc)
                        if all(l["status"] == "CLOSED" for l in legs):
                            break
        else:
            # Trade is OPEN: check if this 1-minute candle touches ANY open leg's SL or TP
            if execution_fidelity == "ticks":
                candle_touches_level = False
                for leg in legs:
                    if leg["status"] != "OPEN":
                        continue
                    sl_p = leg["sl_price"]
                    tp_p = leg["tp_price"]
                    if (direction == "BUY" and (b_low <= sl_p or b_high >= tp_p)) or \
                       (direction == "SELL" and (b_high >= sl_p or b_low <= tp_p)):
                        candle_touches_level = True
                        break
                if candle_touches_level:
                    bar_ticks = _fetch_surgical_1m_ticks(actual_symbol, b_time_utc, tf_sec, ct_client=ct_client, candle_cache=candle_cache)
                    if bar_ticks:
                        surgical_ticks_used = True
                        prev_bid = b_open
                        for tk_utc, tk_bid in bar_ticks:
                            _evaluate_open_segment(prev_bid, tk_bid, tk_utc)
                            prev_bid = tk_bid
                            if all(l["status"] == "CLOSED" for l in legs):
                                break

            # Walk the 3 directional segments (or act as boundary safety net if cTrader candle wick slightly exceeded tick feed)
            if not all(l["status"] == "CLOSED" for l in legs):
                for seg_start, seg_end in segments:
                    _evaluate_open_segment(seg_start, seg_end, b_time_utc)
                    if all(l["status"] == "CLOSED" for l in legs):
                        break

        if not is_pending and all(l["status"] == "CLOSED" for l in legs):
            break

    # If still pending at the end of all bars, mark EXPIRED
    if is_pending:
        return {
            "msg_id": sig["msg_id"],
            "timestamp": sig_utc,
            "symbol": actual_symbol,
            "direction": direction,
            "order_type": order_type,
            "entry": sig_entry,
            "sl": initial_sl,
            "tp": tp_list,
            "status": "EXPIRED",
            "status_detail": "Order never triggered within simulation window",
            "timeframe": tf_name,
            "lots": 0.0,
            "risk_amount": 0.0,
            "profit": 0.0,
            "realized_r": 0.0,
            "tp1_hit": 0,
            "tp2_hit": 0,
            "tp3_hit": 0,
            "sl_hit": 0,
            "be_hit": 0,
            "activated_at": 0,
            "closed_at": valid_bars[-1][0] - broker_offset_sec,
            "legs": [],
            "raw_text": sig.get("raw_text", ""),
            "updates_applied": updates_applied,
        }

    # If any legs are still open at the end of the 5-day window (or current market time), mark-to-market close at last bar close
    last_bar = valid_bars[-1]
    last_close_price = last_bar[4]
    last_close_utc = last_bar[0] - broker_offset_sec
    for leg in legs:
        if leg["status"] == "OPEN":
            leg["status"] = "CLOSED"
            leg["close_price"] = last_close_price
            leg["close_time"] = last_close_utc
            leg["close_reason"] = "END_OF_DATA"
            leg["profit"] = _compute_leg_profit(direction, leg["open_price"], last_close_price, leg["lot"], sym_info)

    # Apply ECN Commission if configured (e.g. cTrader RAW ECN default $6.00/lot round-turn)
    platform_mode = str(rules.get("platform", "mt5")).lower()
    comm_per_lot = float(rules.get("commission_per_lot", 6.0 if platform_mode == "ctrader" else 0.0) or 0.0)
    if comm_per_lot > 0:
        for leg in legs:
            comm = round(leg["lot"] * comm_per_lot, 2)
            leg["commission"] = -comm
            leg["profit"] = round(leg["profit"] - comm, 2)

    total_profit = round(sum(l["profit"] for l in legs), 2)
    total_lots = round(sum(l["lot"] for l in legs), 2)
    closed_at_utc = max((l["close_time"] for l in legs), default=sig_utc)

    # Determine sl_hit matching Sirius Copier db.py / mt5_executor.py rules:
    # SL is counted if NO TP was hit and trade closed in a loss (or at SL)
    if tp1_hit == 0 and tp2_hit == 0 and tp3_hit == 0:
        if any(l["close_reason"] == "SL" for l in legs) or (not be_hit and total_profit < 0):
            sl_hit = 1

    realized_r = round(total_profit / risk_amount, 2) if risk_amount > 0 else (-1.0 if sl_hit else 0.0)

    # Create concise outcome badge (e.g. TP3, TP2+BE, TP1+BE, SL, BE1, MANUAL)
    if tp3_hit:
        outcome_badge = "TP3"
    elif tp2_hit:
        outcome_badge = "TP2"
    elif tp1_hit:
        outcome_badge = "TP1"
    elif sl_hit:
        outcome_badge = "SL"
    elif be_hit:
        outcome_badge = "BE1"
    else:
        outcome_badge = legs[-1]["close_reason"] if legs else "CLOSED"

    return {
        "msg_id": sig["msg_id"],
        "timestamp": sig_utc,
        "symbol": actual_symbol,
        "direction": direction,
        "order_type": order_type,
        "entry": round(fill_price, sym_info.digits if sym_info else 2),
        "signal_entry": sig_entry,
        "sl": initial_sl,
        "tp": tp_list,
        "status": "CLOSED",
        "outcome_badge": outcome_badge,
        "timeframe": f"{tf_name}+TICKS" if surgical_ticks_used else tf_name,
        "lots": total_lots,
        "risk_amount": risk_amount,
        "profit": total_profit,
        "realized_r": realized_r,
        "tp1_hit": tp1_hit,
        "tp2_hit": tp2_hit,
        "tp3_hit": tp3_hit,
        "sl_hit": sl_hit,
        "be_hit": be_hit,
        "activated_at": activated_at_broker - broker_offset_sec,
        "closed_at": closed_at_utc,
        "legs": legs,
        "raw_text": sig.get("raw_text", ""),
        "provider_name": sig.get("provider_name", ""),
        "updates_applied": updates_applied,
    }


def run_simulation(
    signals: list,
    provider_name: str,
    rules: dict,
    total_messages_scanned: int = 0,
    progress_cb=None,
) -> tuple[dict, dict, list, list]:
    """
    Run full historical MT5 or cTrader simulation across a chronological list of signals.
    Returns (summary_dict, graph_dict, simulated_signals_list, deals_list).
    """
    platform_mode = str(rules.get("platform", "mt5")).lower()
    ct_client = _get_ctrader_backtest_client() if platform_mode == "ctrader" else None

    if ct_client and ct_client.is_authorized:
        sym_map = ct_client.clean_symbol_map
        primary_gold = sym_map.get("XAUUSD") or "XAUUSD"
        broker_offset_sec = 0  # cTrader Open API historical timestamps are UTC
    else:
        if not _ensure_mt5_connected():
            raise RuntimeError("Neither cTrader Open API nor MetaTrader 5 is connected. Please connect an account.")
        sym_map = _build_symbol_map()
        primary_gold = sym_map.get("XAUUSD") or config.SYMBOL
        broker_offset_sec = _get_broker_offset_sec(primary_gold)

        # Pre-warm symbols present in the signal set
        unique_actual_symbols = set()
        for s in signals:
            raw_inst = (s.get("instrument") or "XAUUSD").upper().replace("/", "").replace("-", "").replace("_", "").replace("#", "")
            act_sym = sym_map.get(raw_inst) or (primary_gold if raw_inst in ("XAUUSD", "GOLD", "XAU") else None)
            if act_sym:
                unique_actual_symbols.add(act_sym)

        for act_sym in unique_actual_symbols:
            if progress_cb:
                progress_cb(f"Syncing historical candles for {act_sym}...")
            _prewarm_symbol_timeframes(act_sym)

    starting_balance = float(rules.get("starting_balance") or getattr(config, "BACKTESTER_DEFAULT_BALANCE", 10000.0))
    if starting_balance <= 0:
        starting_balance = 10000.0

    sym_info_cache = {}
    simulated_signals = []
    all_deals = []

    total_sigs = len(signals)
    engine_label = "cTrader" if platform_mode == "ctrader" else "MT5"
    for idx, sig in enumerate(signals):
        if progress_cb and (idx % 10 == 0 or idx == total_sigs - 1):
            progress_cb(f"Simulating signal {idx + 1} of {total_sigs} on {engine_label} historical data...")

        sim_res = _simulate_single_signal(
            sig=sig,
            sym_map=sym_map,
            sym_info_cache=sym_info_cache,
            broker_offset_sec=broker_offset_sec,
            rules=rules,
            current_balance=starting_balance,
            ct_client=ct_client,
        )
        simulated_signals.append(sim_res)

        if sim_res["status"] == "CLOSED":
            for leg in sim_res["legs"]:
                all_deals.append({
                    "msg_id": sim_res["msg_id"],
                    "symbol": sim_res["symbol"],
                    "trade_type": sim_res["direction"],
                    "provider_name": sim_res.get("provider_name", ""),
                    "tp_num": leg["tp_num"],
                    "lots": leg["lot"],
                    "open_price": leg["open_price"],
                    "close_price": leg["close_price"],
                    "open_time": leg["open_time"],
                    "close_time": leg["close_time"],
                    "close_reason": leg["close_reason"],
                    "profit": leg["profit"],
                })

    # Compute summary statistics & graph curves
    executed_signals = [s for s in simulated_signals if s["status"] == "CLOSED"]
    cancelled_signals = [s for s in simulated_signals if s["status"] == "CANCELLED"]
    expired_signals = [s for s in simulated_signals if s["status"] == "EXPIRED"]
    skipped_signals = [s for s in simulated_signals if s["status"] == "SKIPPED"]

    # Sort executed signals by closed_at for accurate equity curve & drawdown
    executed_sorted = sorted(executed_signals, key=lambda x: (x["closed_at"] or x["timestamp"], x["msg_id"]))

    trade_by_trade = []
    daily = {}
    weekly = {}
    monthly = {}

    cum_profit = 0.0
    cum_curve = [0.0]
    peak_equity = 0.0
    max_dd_dollar = 0.0

    for idx, s in enumerate(executed_sorted):
        p_val = float(s["profit"] or 0.0)
        cum_profit = round(cum_profit + p_val, 2)
        cum_curve.append(cum_profit)

        if cum_profit > peak_equity:
            peak_equity = cum_profit
        dd = peak_equity - cum_profit
        if dd > max_dd_dollar:
            max_dd_dollar = dd

        c_time = s["closed_at"] or s["timestamp"]
        dt = datetime.fromtimestamp(c_time)

        trade_by_trade.append({
            "trade_num": idx + 1,
            "msg_id": s["msg_id"],
            "time": dt.strftime("%Y-%m-%d %H:%M"),
            "timestamp": c_time,
            "symbol": s["symbol"],
            "direction": s["direction"],
            "entry": s["entry"],
            "sl": s["sl"],
            "tp": s["tp"],
            "lots": s["lots"],
            "outcome": s.get("outcome_badge", "CLOSED"),
            "deal_profit": round(p_val, 2),
            "profit": cum_profit,
            "realized_r": s["realized_r"],
        })

        day_str = dt.strftime("%Y-%m-%d")
        daily[day_str] = round(daily.get(day_str, 0.0) + p_val, 2)

        week_str = dt.strftime("%Y-W%V")
        weekly[week_str] = round(weekly.get(week_str, 0.0) + p_val, 2)

        month_str = dt.strftime("%Y-%m")
        monthly[month_str] = round(monthly.get(month_str, 0.0) + p_val, 2)

    tp1_count = sum(s["tp1_hit"] for s in executed_signals)
    tp2_count = sum(s["tp2_hit"] for s in executed_signals)
    tp3_count = sum(s["tp3_hit"] for s in executed_signals)
    sl_count = sum(s["sl_hit"] for s in executed_signals)
    be_count = sum(s["be_hit"] for s in executed_signals)
    total_lots = round(sum(s["lots"] for s in executed_signals), 2)

    completed_for_wr = tp1_count + sl_count
    if completed_for_wr > 0:
        win_rate = round((tp1_count / completed_for_wr) * 100.0, 1)
    elif executed_signals:
        wins = len([s for s in executed_signals if s["profit"] > 0])
        win_rate = round((wins / len(executed_signals)) * 100.0, 1)
    else:
        win_rate = 0.0

    wins_list = [s["profit"] for s in executed_signals if s["profit"] > 0]
    losses_list = [abs(s["profit"]) for s in executed_signals if s["profit"] < 0]
    avg_win_dollar = (sum(wins_list) / len(wins_list)) if wins_list else 0.0
    avg_loss_dollar = (sum(losses_list) / len(losses_list)) if losses_list else 0.0

    gross_profit = sum(wins_list)
    gross_loss = sum(losses_list)
    profit_factor = round(gross_profit / gross_loss, 2) if gross_loss > 0 else (99.9 if gross_profit > 0 else 0.0)

    # R:R calculation matching db.py
    if avg_loss_dollar > 0 and avg_win_dollar > 0:
        ratio_val = avg_win_dollar / avg_loss_dollar
        if ratio_val >= 1.0:
            if abs(ratio_val - round(ratio_val)) < 0.15:
                rr_display = f"{int(round(ratio_val))}:1"
            else:
                rr_display = f"{ratio_val:.1f}:1"
        else:
            inv = 1.0 / ratio_val
            if abs(inv - round(inv)) < 0.15:
                rr_display = f"1:{int(round(inv))}"
            else:
                rr_display = f"1:{inv:.1f}"
        avg_rr = round(ratio_val, 2)
    else:
        rr_display = "1:1"
        avg_rr = 1.0

    low_to_high_profit = round(max(0.0, max(cum_curve) - min(cum_curve)), 2)
    gain_pct = round((cum_profit / starting_balance) * 100.0, 2) if starting_balance > 0 else 0.0
    low_to_high_pct = round((low_to_high_profit / starting_balance) * 100.0, 2) if starting_balance > 0 else 0.0
    max_dd_pct = round((max_dd_dollar / starting_balance) * 100.0, 2) if starting_balance > 0 else 0.0

    risk_mode = rules.get("risk_mode", "percent")
    risk_val = float(rules.get("risk_value", 1.0))
    risk_display = f"{risk_val}%" if risk_mode == "percent" else f"{risk_val:.2f}"

    summary = {
        "provider_name": provider_name,
        "starting_balance": round(starting_balance, 2),
        "ending_balance": round(starting_balance + cum_profit, 2),
        "total_messages_scanned": total_messages_scanned,
        "total_signals_found": len(simulated_signals),
        "trades_count": len(executed_signals),
        "cancelled_count": len(cancelled_signals),
        "expired_count": len(expired_signals),
        "skipped_count": len(skipped_signals),
        "deal_count": len(all_deals),
        "lots": total_lots,
        "risk_display": risk_display,
        "avg_rr": avg_rr,
        "rr_display": rr_display,
        "tp1_hits": tp1_count,
        "tp2_hits": tp2_count,
        "tp3_hits": tp3_count,
        "be_hits": be_count,
        "sl_hits": sl_count,
        "win_rate": win_rate,
        "profit": cum_profit,
        "gain_pct": gain_pct,
        "low_to_high_profit": low_to_high_profit,
        "low_to_high_pct": low_to_high_pct,
        "max_drawdown_dollar": round(max_dd_dollar, 2),
        "max_drawdown_pct": max_dd_pct,
        "profit_factor": profit_factor,
    }

    graph = {
        "trade_by_trade": trade_by_trade,
        "daily": daily,
        "weekly": weekly,
        "monthly": monthly,
    }

    return summary, graph, simulated_signals, all_deals


def run_autotester_grid(
    signals: list,
    provider_name: str,
    base_rules: dict,
    total_messages_scanned: int = 0,
    progress_cb=None,
) -> dict:
    """
    AutoTester / Parameter Optimizer:
    1. Pre-fetches and caches MT5 historical candle data for all signals ONCE.
    2. Evaluates every combination of:
       - TP Allocation Splits (6 profiles)
       - Cascading SL Strategies (5 profiles)
       - Pending Order Expiry Timers (4 profiles)
       - Entry-Range Fill Price (nearest / mid / furthest; only when signals give ranges)
    3. Ranks combinations by Net Profit ($), Profit Factor, and Stability Score.
    """
    platform_mode = str(base_rules.get("platform", "mt5")).lower()
    ct_client = _get_ctrader_backtest_client() if platform_mode == "ctrader" else None

    if ct_client and ct_client.is_authorized:
        sym_map = ct_client.clean_symbol_map
        primary_gold = sym_map.get("XAUUSD") or "XAUUSD"
        broker_offset_sec = 0
    else:
        if not _ensure_mt5_connected():
            raise RuntimeError("Neither cTrader Open API nor MetaTrader 5 is connected. Please connect an account.")
        sym_map = _build_symbol_map()
        primary_gold = sym_map.get("XAUUSD") or config.SYMBOL
        broker_offset_sec = _get_broker_offset_sec(primary_gold)

        unique_actual_symbols = set()
        for s in signals:
            raw_inst = (s.get("instrument") or "XAUUSD").upper().replace("/", "").replace("-", "").replace("_", "").replace("#", "")
            act_sym = sym_map.get(raw_inst) or (primary_gold if raw_inst in ("XAUUSD", "GOLD", "XAU") else None)
            if act_sym:
                unique_actual_symbols.add(act_sym)

        for act_sym in unique_actual_symbols:
            if progress_cb:
                progress_cb(f"AutoTester: Syncing historical candles for {act_sym}...")
            _prewarm_symbol_timeframes(act_sym)

    starting_balance = float(base_rules.get("starting_balance") or getattr(config, "BACKTESTER_DEFAULT_BALANCE", 10000.0))
    if starting_balance <= 0:
        starting_balance = 10000.0

    risk_mode = base_rules.get("risk_mode", "percent")
    risk_value = float(base_rules.get("risk_value", 1.0))

    sym_info_cache = {}
    candle_cache = {}

    # Step 1: Pre-populate sym_info_cache (candle_cache will be populated with fast Python tuples on combo #1)
    for sig in signals:
        raw_inst = (sig.get("instrument") or "XAUUSD").upper().replace("/", "").replace("-", "").replace("_", "").replace("#", "")
        actual_symbol = sym_map.get(raw_inst) or primary_gold
        if actual_symbol not in sym_info_cache:
            if ct_client and ct_client.is_authorized:
                sym_info_cache[actual_symbol] = ct_client.get_symbol_info(actual_symbol) or CTraderSymbolInfo(name=actual_symbol, symbol_id=1)
            else:
                mt5.symbol_select(actual_symbol, True)
                sym_info_cache[actual_symbol] = mt5.symbol_info(actual_symbol)

    # Step 2: Define Parameter Grid
    tp_profiles = [
        {"name": "Standard (60 / 30 / 10)", "tp1": 60.0, "tp2": 30.0, "tp3": 10.0},
        {"name": "Equal Split (33.4 / 33.3 / 33.3)", "tp1": 33.4, "tp2": 33.3, "tp3": 33.3},
        {"name": "Front-Loaded (50 / 30 / 20)", "tp1": 50.0, "tp2": 30.0, "tp3": 20.0},
        {"name": "Runner-Loaded (20 / 30 / 50)", "tp1": 20.0, "tp2": 30.0, "tp3": 50.0},
        {"name": "TP1 + TP2 Only (50 / 50 / 0)", "tp1": 50.0, "tp2": 50.0, "tp3": 0.0},
        {"name": "100% Scalper at TP1 (100 / 0 / 0)", "tp1": 100.0, "tp2": 0.0, "tp3": 0.0},
    ]

    sl_profiles = [
        {"name": "TP1→BE, TP2→TP1", "sl1": "ENTRY", "sl2": "TP1", "sl3": "TP2"},
        {"name": "TP1→BE, TP2→BE", "sl1": "ENTRY", "sl2": "ENTRY", "sl3": "TP2"},
        {"name": "Hold Initial SL until TP2→BE", "sl1": "none", "sl2": "ENTRY", "sl3": "TP2"},
        {"name": "Hold Initial SL until TP2→TP1", "sl1": "none", "sl2": "TP1", "sl3": "TP2"},
        {"name": "No SL Trail (Full SL or TP)", "sl1": "none", "sl2": "none", "sl3": "none"},
    ]

    expiry_profiles = [15, 30, 60, 0]

    # Entry-zone modes only matter if at least one signal gives a price range
    has_ranges = False
    for s in signals:
        s_entry = float(s.get("entry") or 0.0)
        lo, hi = _derive_entry_range(s, s_entry)
        if hi - lo > 0:
            has_ranges = True
            break
    zone_modes = list(ENTRY_ZONE_MODES) if has_ranges else [str(base_rules.get("entry_zone_mode", "mid"))]

    combinations = []
    for tp_p in tp_profiles:
        for sl_p in sl_profiles:
            # Skip redundant SL trailing profiles when 100% is closed at TP1
            if tp_p["tp1"] == 100.0 and sl_p["name"] != "TP1→BE, TP2→TP1":
                continue
            for exp_m in expiry_profiles:
                for z_mode in zone_modes:
                    combinations.append((tp_p, sl_p, exp_m, z_mode))

    total_combos = len(combinations)
    results = []

    for c_idx, (tp_p, sl_p, exp_m, z_mode) in enumerate(combinations):
        if progress_cb and (c_idx % 8 == 0 or c_idx == total_combos - 1):
            progress_cb(f"AutoTester: Evaluating combination {c_idx + 1} of {total_combos}...")

        combo_rules = {
            "starting_balance": starting_balance,
            "risk_mode": risk_mode,
            "risk_value": risk_value,
            "pending_expiry_minutes": exp_m,
            "tp1_percent": tp_p["tp1"],
            "tp2_percent": tp_p["tp2"],
            "tp3_percent": tp_p["tp3"],
            "sl_tp1_strategy": sl_p["sl1"],
            "sl_tp2_strategy": sl_p["sl2"],
            "sl_tp3_strategy": sl_p["sl3"],
            "entry_zone_mode": z_mode,
            "spread_offset_entry": base_rules.get("spread_offset_entry", True),
            "spread_offset_tp": base_rules.get("spread_offset_tp", True),
            "spread_mode": base_rules.get("spread_mode", "auto"),
            "manual_spread_pips": float(base_rules.get("manual_spread_pips", 0.0) or 0.0),
            "execution_fidelity": base_rules.get("execution_fidelity", "ticks"),
            "platform": platform_mode,
            "commission_per_lot": float(base_rules.get("commission_per_lot", 6.0 if platform_mode == "ctrader" else 0.0) or 0.0),
        }

        sim_sigs = []
        for sig in signals:
            s_res = _simulate_single_signal(
                sig=sig,
                sym_map=sym_map,
                sym_info_cache=sym_info_cache,
                broker_offset_sec=broker_offset_sec,
                rules=combo_rules,
                current_balance=starting_balance,
                candle_cache=candle_cache,
                ct_client=ct_client,
            )
            sim_sigs.append(s_res)

        exec_sigs = [s for s in sim_sigs if s["status"] == "CLOSED"]
        exec_sorted = sorted(exec_sigs, key=lambda x: (x["closed_at"] or x["timestamp"], x["msg_id"]))

        cum_pnl = 0.0
        peak_eq = 0.0
        max_dd = 0.0
        wins_pnl = []
        loss_pnl = []

        for s in exec_sorted:
            p = float(s["profit"] or 0.0)
            cum_pnl = round(cum_pnl + p, 2)
            if cum_pnl > peak_eq:
                peak_eq = cum_pnl
            dd = peak_eq - cum_pnl
            if dd > max_dd:
                max_dd = dd
            if p > 0:
                wins_pnl.append(p)
            elif p < 0:
                loss_pnl.append(abs(p))

        tp1_c = sum(s["tp1_hit"] for s in exec_sigs)
        tp2_c = sum(s["tp2_hit"] for s in exec_sigs)
        tp3_c = sum(s["tp3_hit"] for s in exec_sigs)
        sl_c = sum(s["sl_hit"] for s in exec_sigs)
        be_c = sum(s["be_hit"] for s in exec_sigs)

        comp_wr = tp1_c + sl_c
        if comp_wr > 0:
            wr = round((tp1_c / comp_wr) * 100.0, 1)
        elif exec_sigs:
            wr = round((len(wins_pnl) / len(exec_sigs)) * 100.0, 1)
        else:
            wr = 0.0

        gross_w = sum(wins_pnl)
        gross_l = sum(loss_pnl)
        pf = round(gross_w / gross_l, 2) if gross_l > 0 else (99.9 if gross_w > 0 else 0.0)
        gain_pct = round((cum_pnl / starting_balance) * 100.0, 2) if starting_balance > 0 else 0.0
        max_dd_pct = round((max_dd / starting_balance) * 100.0, 2) if starting_balance > 0 else 0.0

        # Stability / Recovery Score: Rewards high profit with low drawdown
        recovery_ratio = round(cum_pnl / max(1.0, max_dd), 2) if cum_pnl > 0 else round(cum_pnl / max(1.0, starting_balance * 0.01), 2)

        results.append({
            "tp_profile_name": tp_p["name"],
            "sl_profile_name": sl_p["name"],
            "expiry_minutes": exp_m,
            "expiry_label": f"{exp_m}m" if exp_m > 0 else "No Expiry",
            "entry_zone_mode": z_mode,
            "entry_zone_label": {"nearest": "Nearest (first fill)", "mid": "Mid-Zone", "furthest": "Furthest (best price)"}.get(z_mode, z_mode),
            "rules": combo_rules,
            "trades_count": len(exec_sigs),
            "profit": cum_pnl,
            "gain_pct": gain_pct,
            "win_rate": wr,
            "max_drawdown_dollar": round(max_dd, 2),
            "max_drawdown_pct": max_dd_pct,
            "profit_factor": pf,
            "recovery_ratio": recovery_ratio,
            "tp1_hits": tp1_c,
            "tp2_hits": tp2_c,
            "tp3_hits": tp3_c,
            "be_hits": be_c,
            "sl_hits": sl_c,
        })

    # Sort primarily by Net Profit descending, then lowest drawdown, then Profit Factor
    results.sort(key=lambda r: (r["profit"], -r["max_drawdown_dollar"], r["profit_factor"]), reverse=True)
    for idx, item in enumerate(results):
        item["rank"] = idx + 1

    # Also identify the "Best Risk-Adjusted / Recommended" if different from Rank #1
    best_stability = max(results, key=lambda r: (r["recovery_ratio"], r["profit"])) if results else None
    if best_stability:
        best_stability["recommended_badge"] = True

    return {
        "provider_name": provider_name,
        "total_combinations_tested": total_combos,
        "total_signals_found": len(signals),
        "total_messages_scanned": total_messages_scanned,
        "risk_mode": risk_mode,
        "risk_value": risk_value,
        "starting_balance": starting_balance,
        "leaderboard": results,
    }

