"""
provider_analytics.py
---------------------
Deep per-provider statistics for the "View Statistics" popup.

Design notes
* Completely self-contained: it owns its own SQLite table (`trade_analytics`)
  inside stats.db and never modifies the existing `trades` / `trade_signals`
  tables or the existing sync loop in web_server.py.
* One row per *closed MT5 position* (each TP leg of a signal is its own
  position, because the executor opens one position per TP).
* The provider is resolved from the magic number of the ENTRY deal, so
  positions that were closed manually (exit deal magic = 0) are still
  attributed to the right provider.
* All timestamps are stored exactly as MT5 returns them (broker server time
  expressed as epoch seconds). Hour-of-day / day-of-week buckets are therefore
  in broker time, which is what MT5 itself displays.
"""

import re
import time
import threading
from datetime import datetime, timezone, timedelta

import db

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

# A position closed by its stop-loss within this many pips of entry is
# classified as "SL BE" (break-even). The executor places BE at entry +/- spread.
BE_TOLERANCE_PIPS = 5.0

# MT5 deal constants (kept local so this module can be imported without MT5)
DEAL_TYPE_BUY = 0
DEAL_TYPE_SELL = 1
DEAL_TYPE_BALANCE = 2
DEAL_ENTRY_IN = 0
DEAL_ENTRY_OUT = 1
DEAL_ENTRY_INOUT = 2
DEAL_ENTRY_OUT_BY = 3
DEAL_REASON_CLIENT = 0
DEAL_REASON_MOBILE = 1
DEAL_REASON_WEB = 2
DEAL_REASON_EXPERT = 3
DEAL_REASON_SL = 4
DEAL_REASON_TP = 5
DEAL_REASON_SO = 6

CLOSE_REASON_ORDER = ["SL", "SL BE", "SL PROFIT", "TP1", "TP2", "TP3", "TP",
                      "COPIER CLOSE", "MANUAL", "STOP OUT", "OTHER"]
WEEKDAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

_sync_lock = threading.Lock()

_FX_CCY = {"USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD", "SEK", "NOK",
           "DKK", "SGD", "HKD", "ZAR", "MXN", "TRY", "PLN", "CNH", "HUF", "CZK"}
_INDEX_CRYPTO_KEYS = ("US30", "US100", "US500", "NAS", "SPX", "SP500", "DJI", "DJ30", "DOW",
                      "GER", "DAX", "DE40", "UK100", "FTSE", "JP225", "NIK", "HK50",
                      "AUS200", "FRA40", "EU50", "STOXX", "BTC", "ETH", "LTC", "XRP",
                      "BCH", "SOL", "DOGE", "ADA", "OIL", "WTI", "BRENT", "USOIL", "UKOIL")


# --------------------------------------------------------------------------
# Schema
# --------------------------------------------------------------------------

def ensure_schema(conn=None):
    own = conn is None
    if own:
        conn = db.get_db()
    conn.execute('''
        CREATE TABLE IF NOT EXISTS trade_analytics (
            position_id   INTEGER PRIMARY KEY,
            provider_id   TEXT,
            magic         INTEGER,
            symbol        TEXT,
            direction     TEXT,
            lots          REAL,
            open_time     INTEGER,
            close_time    INTEGER,
            open_price    REAL,
            close_price   REAL,
            profit        REAL,
            commission    REAL DEFAULT 0.0,
            swap          REAL DEFAULT 0.0,
            pips          REAL,
            pip_size      REAL,
            mae_pips      REAL,
            mfe_pips      REAL,
            duration_sec  INTEGER,
            close_reason  TEXT,
            tp_level      INTEGER,
            signal_ref    TEXT,
            comment       TEXT,
            synced_at     INTEGER
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_ta_provider_close ON trade_analytics(provider_id, close_time)')
    try:
        conn.execute("ALTER TABLE trade_analytics ADD COLUMN account_id TEXT DEFAULT ''")
    except Exception:
        pass
    conn.execute('CREATE INDEX IF NOT EXISTS idx_ta_account_provider ON trade_analytics(account_id, provider_id)')
    conn.commit()
    if own:
        conn.close()


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def get_pip_size(symbol, point=None, digits=None):
    """Return the price distance of one pip for a symbol.

    Gold = 0.10, Silver = 0.01, JPY pairs = 0.01, other FX = 0.0001,
    indices / crypto / oil = 1.0. Unknown symbols fall back to the broker's
    point size (x10 for 3/5-digit quotes).
    """
    s = (symbol or "").upper()
    if "XAU" in s or "GOLD" in s:
        return 0.1
    if "XAG" in s or "SILVER" in s:
        return 0.01
    letters = re.sub(r"[^A-Z]", "", s)[:6]
    if len(letters) == 6 and letters[:3] in _FX_CCY and letters[3:] in _FX_CCY:
        return 0.01 if "JPY" in letters else 0.0001
    if any(k in s for k in _INDEX_CRYPTO_KEYS):
        return 1.0
    if point:
        return point * 10 if digits in (3, 5) else point
    return 0.0001


def build_magic_map(settings, default_magic):
    """Map magic number -> provider_id using the same rules as web_server.py."""
    magic_to_provider = {}
    for pid, pdata in (settings or {}).items():
        p_magic = (pdata or {}).get("magic_number")
        if not p_magic:
            try:
                p_magic = 400400 + int(pid)
            except Exception:
                p_magic = int(default_magic)
        magic_to_provider[int(p_magic)] = str(pid)
    magic_to_provider[int(default_magic)] = "1"
    return magic_to_provider


def classify_close(reason_code, exit_comment, pips, tp_level):
    c = (exit_comment or "").lower()
    if reason_code == DEAL_REASON_TP or c.startswith("[tp"):
        return f"TP{tp_level}" if tp_level in (1, 2, 3) else "TP"
    if reason_code == DEAL_REASON_SL or c.startswith("[sl"):
        if abs(pips) <= BE_TOLERANCE_PIPS:
            return "SL BE"
        return "SL PROFIT" if pips > 0 else "SL"
    if reason_code == DEAL_REASON_SO or c.startswith("[so"):
        return "STOP OUT"
    if reason_code == DEAL_REASON_EXPERT:
        return "COPIER CLOSE"
    if reason_code in (DEAL_REASON_CLIENT, DEAL_REASON_MOBILE, DEAL_REASON_WEB):
        return "MANUAL"
    return "OTHER"


def _utc_dt(raw_ts):
    """MT5 times are broker-time epoch seconds; a tz-aware UTC datetime built
    from them round-trips to the same integer inside the MT5 library."""
    return datetime.fromtimestamp(int(raw_ts), tz=timezone.utc)


def _attr(obj, name, default=0):
    try:
        v = getattr(obj, name)
        return default if v is None else v
    except Exception:
        return default


def _compute_excursions(mt5, symbol, direction, open_time, close_time, open_price, pip):
    """Max adverse / favourable excursion in pips using M1 bars (M15 for long trades)."""
    try:
        duration = max(0, int(close_time) - int(open_time))
        tf = mt5.TIMEFRAME_M1 if duration <= 5 * 86400 else mt5.TIMEFRAME_M15
        step = 60 if tf == mt5.TIMEFRAME_M1 else 900
        start = int(open_time) - (int(open_time) % step)
        rates = mt5.copy_rates_range(symbol, tf, _utc_dt(start), _utc_dt(int(close_time)))
        if rates is None or len(rates) == 0:
            return None, None
        lows = [float(r["low"]) for r in rates]
        highs = [float(r["high"]) for r in rates]
        if direction == "BUY":
            mae = max(0.0, open_price - min(lows)) / pip
            mfe = max(0.0, max(highs) - open_price) / pip
        else:
            mae = max(0.0, max(highs) - open_price) / pip
            mfe = max(0.0, open_price - min(lows)) / pip
        return round(mae, 2), round(mfe, 2)
    except Exception:
        return None, None


# --------------------------------------------------------------------------
# Sync: MT5 deal history -> trade_analytics
# --------------------------------------------------------------------------

def sync_from_mt5(mt5, magic_map, reset_ts=0, compute_excursions=True, max_excursion_fetches=400):
    """Read MT5 deal history and store every newly closed bot position.

    Read-only towards MT5 (history + rates only, never sends orders).
    Returns a small summary dict.
    """
    if not _sync_lock.acquire(blocking=False):
        return {"status": "busy"}
    try:
        conn = db.get_db()
        ensure_schema(conn)
        known = {r[0] for r in conn.execute("SELECT position_id FROM trade_analytics").fetchall()}

        start_dt = datetime.fromtimestamp(reset_ts) if reset_ts and reset_ts > 0 else datetime(2020, 1, 1)
        deals = mt5.history_deals_get(start_dt, datetime.now() + timedelta(days=2))
        if deals is None:
            conn.close()
            return {"status": "no_history", "error": str(_attr(mt5, "last_error", lambda: "")())}

        by_pos = {}
        for d in deals:
            if _attr(d, "type") not in (DEAL_TYPE_BUY, DEAL_TYPE_SELL):
                continue
            pid = _attr(d, "position_id")
            if not pid:
                continue
            by_pos.setdefault(pid, {"in": [], "out": []})
            if _attr(d, "entry") == DEAL_ENTRY_IN:
                by_pos[pid]["in"].append(d)
            else:
                by_pos[pid]["out"].append(d)

        sym_cache = {}
        inserted = 0
        fetches = 0
        now_i = int(time.time())

        for pos_id, grp in by_pos.items():
            if pos_id in known or not grp["in"] or not grp["out"]:
                continue
            entry = grp["in"][0]
            magic = int(_attr(entry, "magic"))
            if magic not in magic_map:
                continue
            in_vol = sum(float(_attr(x, "volume", 0.0)) for x in grp["in"])
            out_vol = sum(float(_attr(x, "volume", 0.0)) for x in grp["out"])
            if in_vol <= 0 or out_vol + 1e-9 < in_vol:
                continue  # still (partially) open

            exits = sorted(grp["out"], key=lambda x: (_attr(x, "time"), _attr(x, "ticket")))
            last_exit = exits[-1]
            close_time = int(_attr(last_exit, "time"))
            if reset_ts and close_time < reset_ts:
                continue

            symbol = _attr(entry, "symbol", "")
            direction = "BUY" if _attr(entry, "type") == DEAL_TYPE_BUY else "SELL"
            open_time = int(min(_attr(x, "time") for x in grp["in"]))
            open_price = sum(float(_attr(x, "price", 0.0)) * float(_attr(x, "volume", 0.0)) for x in grp["in"]) / in_vol
            close_price = sum(float(_attr(x, "price", 0.0)) * float(_attr(x, "volume", 0.0)) for x in exits) / out_vol
            all_deals = grp["in"] + exits
            profit = sum(float(_attr(x, "profit", 0.0)) for x in all_deals)
            commission = sum(float(_attr(x, "commission", 0.0)) + float(_attr(x, "fee", 0.0)) for x in all_deals)
            swap = sum(float(_attr(x, "swap", 0.0)) for x in all_deals)

            if symbol not in sym_cache:
                info = None
                try:
                    info = mt5.symbol_info(symbol)
                except Exception:
                    info = None
                sym_cache[symbol] = get_pip_size(symbol, _attr(info, "point", None) if info else None,
                                                 _attr(info, "digits", None) if info else None)
            pip = sym_cache[symbol]
            sign = 1 if direction == "BUY" else -1
            pips = round((close_price - open_price) * sign / pip, 2)

            entry_comment = _attr(entry, "comment", "") or ""
            m_tp = re.search(r"TP\s*(\d)", entry_comment, re.IGNORECASE)
            tp_level = int(m_tp.group(1)) if m_tp else 0
            m_sig = re.search(r"(P\d+\s*T\d+|Trade\d+)", entry_comment, re.IGNORECASE)
            signal_ref = m_sig.group(1).upper().replace(" ", "") if m_sig else None

            reason = classify_close(int(_attr(last_exit, "reason", -1)), _attr(last_exit, "comment", ""), pips, tp_level)

            mae = mfe = None
            if compute_excursions and fetches < max_excursion_fetches:
                fetches += 1
                mae, mfe = _compute_excursions(mt5, symbol, direction, open_time, close_time, open_price, pip)
                if mae is not None:
                    # A losing trade's drawdown is at least the loss it realised
                    mae = round(max(mae, -pips if pips < 0 else 0.0), 2)
                    mfe = round(max(mfe, pips if pips > 0 else 0.0), 2)

            conn.execute('''
                INSERT OR REPLACE INTO trade_analytics
                (position_id, provider_id, magic, symbol, direction, lots, open_time, close_time,
                 open_price, close_price, profit, commission, swap, pips, pip_size, mae_pips, mfe_pips,
                 duration_sec, close_reason, tp_level, signal_ref, comment, synced_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ''', (int(pos_id), magic_map[magic], magic, symbol, direction, round(in_vol, 2), open_time, close_time,
                  open_price, close_price, round(profit, 2), round(commission, 2), round(swap, 2), pips, pip,
                  mae, mfe, max(0, close_time - open_time), reason, tp_level, signal_ref, entry_comment, now_i))
            inserted += 1

        # Retry excursions that could not be computed earlier (e.g. rates not yet downloaded)
        retried = 0
        if compute_excursions:
            pending = conn.execute('''SELECT position_id, symbol, direction, open_time, close_time, open_price, pip_size, pips
                                      FROM trade_analytics WHERE mae_pips IS NULL LIMIT 50''').fetchall()
            for r in pending:
                mae, mfe = _compute_excursions(mt5, r["symbol"], r["direction"], r["open_time"], r["close_time"],
                                               r["open_price"], r["pip_size"] or 0.1)
                if mae is not None:
                    p = r["pips"] or 0.0
                    mae = round(max(mae, -p if p < 0 else 0.0), 2)
                    mfe = round(max(mfe, p if p > 0 else 0.0), 2)
                    conn.execute("UPDATE trade_analytics SET mae_pips=?, mfe_pips=? WHERE position_id=?",
                                 (mae, mfe, r["position_id"]))
                    retried += 1

        conn.commit()
        conn.close()
        return {"status": "ok", "inserted": inserted, "excursions_retried": retried}
    finally:
        _sync_lock.release()


def sync_from_trades_db(conn=None, reset_ts=0):
    """Sync closed trades from the general `trades` table into `trade_analytics`.
    This guarantees that both cTrader and MT5 trades recorded in `trades` table
    are immediately reflected in trade analysis, even without MT5 connectivity."""
    own = conn is None
    if own:
        conn = db.get_db()
    try:
        ensure_schema(conn)
        c = conn.cursor()
        known = {r[0] for r in c.execute("SELECT position_id FROM trade_analytics").fetchall()}

        # Build signatures (pid, close_time, round(profit, 2), acc_id, tp_level) of already recorded positions
        # ONLY for MT5 position_ids uploaded via sync_from_mt5 (where account_id is empty/acc_1),
        # so multi-leg TP1/TP2/TP3 deals closing in the same second with identical profit aren't skipped!
        existing_signatures = set()
        for row in c.execute("SELECT provider_id, close_time, round(profit, 2), COALESCE(account_id, ''), COALESCE(tp_level, 0) FROM trade_analytics").fetchall():
            if row[0] is not None and row[1] is not None and row[2] is not None:
                existing_signatures.add((str(row[0]), int(row[1]), round(float(row[2]), 2), str(row[3] or ""), int(row[4] or 0)))

        q = "SELECT * FROM trades"
        params = []
        if reset_ts and reset_ts > 0:
            q += " WHERE close_time >= ?"
            params.append(int(reset_ts))

        c.execute(q, tuple(params))
        all_trades = [dict(r) for r in c.fetchall()]

        inserted = 0
        now_ts = int(time.time())
        for t in all_trades:
            tkt = int(t.get("ticket") or 0)
            pid = str(t.get("provider_id") or "1")
            acc_id = str(t.get("account_id") or "")
            close_time = int(t.get("close_time") or 0)
            profit = float(t.get("profit") or 0.0)
            close_reason = str(t.get("close_reason") or "TP/SL")

            tp_level = 0
            m_tp = re.search(r"TP\s*(\d)", close_reason, re.IGNORECASE)
            if m_tp:
                tp_level = int(m_tp.group(1))
            elif "TP1" in close_reason:
                tp_level = 1
            elif "TP2" in close_reason:
                tp_level = 2
            elif "TP3" in close_reason:
                tp_level = 3

            sig = (pid, close_time, round(profit, 2), acc_id, tp_level)
            if not tkt or tkt in known:
                continue
            # Only apply signature deduplication for primary MT5 (empty/acc_1) where sync_from_mt5 may have inserted by position_id
            if acc_id in ("", "acc_1") and sig in existing_signatures:
                continue

            symbol = str(t.get("symbol") or "XAUUSD")
            direction = str(t.get("trade_type") or "BUY").upper()
            lots = float(t.get("lots") or 0.0)
            open_time = int(t.get("open_time") or 0)
            open_price = float(t.get("open_price") or 0.0)
            close_price = float(t.get("close_price") or 0.0)
            magic = int(t.get("magic") or 0)

            pip_size = get_pip_size(symbol)
            sign = 1 if direction == "BUY" else -1
            pips = round((close_price - open_price) * sign / pip_size, 2) if (open_price > 0 and close_price > 0 and pip_size > 0) else 0.0
            duration_sec = max(0, close_time - open_time) if (open_time > 0 and close_time > open_time) else 0

            signal_ref = f"P{pid}_T{tkt}"
            comment = f"Trade #{tkt} ({close_reason})"

            conn.execute('''
                INSERT OR REPLACE INTO trade_analytics (
                    position_id, provider_id, magic, symbol, direction, lots,
                    open_time, close_time, open_price, close_price, profit,
                    commission, swap, pips, pip_size, mae_pips, mfe_pips,
                    duration_sec, close_reason, tp_level, signal_ref, comment, synced_at, account_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0.0, 0.0, ?, ?, NULL, NULL, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                tkt, pid, magic, symbol, direction, lots,
                open_time, close_time, open_price, close_price, profit,
                pips, pip_size, duration_sec, close_reason, tp_level, signal_ref, comment, now_ts, acc_id
            ))
            inserted += 1
            known.add(tkt)
            existing_signatures.add(sig)

        if inserted > 0:
            conn.commit()
        return {"status": "ok", "inserted": inserted}
    finally:
        if own:
            conn.close()


# --------------------------------------------------------------------------
# Broker clock
# --------------------------------------------------------------------------

def get_broker_offset(mt5=None, symbols=()):
    """Seconds between broker server time and real UTC epoch (e.g. +10800 for GMT+3)."""
    if mt5 is not None:
        for sym in list(symbols) + ["BTCUSD", "ETHUSD"]:
            try:
                tick = mt5.symbol_info_tick(sym)
                if tick and tick.time:
                    off = tick.time - time.time()
                    rounded = round(off / 1800.0) * 1800
                    if abs(off - rounded) < 300:  # fresh tick -> trustworthy
                        db.set_metadata("broker_utc_offset", int(rounded))
                        return int(rounded)
            except Exception:
                pass
    try:
        return int(float(db.get_metadata("broker_utc_offset", 0) or 0))
    except Exception:
        return 0


# --------------------------------------------------------------------------
# Statistics
# --------------------------------------------------------------------------

def _r2(x):
    return round(float(x or 0.0), 2)


def _summary(rows, base_balance):
    n = len(rows)
    wins = [r for r in rows if r["profit"] > 0]
    losses = [r for r in rows if r["profit"] < 0]
    profit = sum(r["profit"] for r in rows)
    gross_win = sum(r["profit"] for r in wins)
    gross_loss = -sum(r["profit"] for r in losses)
    maes = [r["mae_pips"] for r in rows if r["mae_pips"] is not None]
    if gross_loss > 0:
        pf = round(gross_win / gross_loss, 2)
    else:
        pf = None if gross_win == 0 else "inf"
    return {
        "trades": n,
        "lots": _r2(sum(r["lots"] for r in rows)),
        "wins": len(wins),
        "losses": len(losses),
        "longs_won": len([r for r in wins if r["direction"] == "BUY"]),
        "shorts_won": len([r for r in wins if r["direction"] == "SELL"]),
        "accuracy": _r2(len(wins) / n * 100) if n else 0.0,
        "pips": _r2(sum(r["pips"] for r in rows)),
        "profit": _r2(profit),
        "gain": _r2(profit / base_balance * 100) if base_balance and base_balance > 0 else 0.0,
        "avg_win": _r2(gross_win / len(wins)) if wins else 0.0,
        "avg_loss": _r2(-gross_loss / len(losses)) if losses else 0.0,
        "avg_drawdown": _r2(-sum(maes) / len(maes)) if maes else None,
        "profit_factor": pf,
    }


def _bucket(rows, key_fn):
    out = {}
    for r in rows:
        out.setdefault(key_fn(r), []).append(r)
    return out


def _fmt_duration(sec):
    if sec is None:
        return None
    sec = int(sec)
    d, rem = divmod(sec, 86400)
    h, rem = divmod(rem, 3600)
    m, s = divmod(rem, 60)
    base = f"{h:02d}:{m:02d}:{s:02d}"
    return f"{d}d {base}" if d else base


def compute_provider_statistics(provider_id, base_balance=0.0, broker_offset=0, now_ts=None,
                                reset_ts=0, start_time=None, end_time=None, account_id=None):
    conn = db.get_db()
    ensure_schema(conn)
    q = "SELECT * FROM trade_analytics WHERE provider_id = ?"
    params = [str(provider_id)]
    if account_id:
        q += " AND account_id = ?"
        params.append(str(account_id))
    if reset_ts:
        q += " AND close_time >= ?"
        params.append(int(reset_ts))
    if start_time is not None:
        q += " AND close_time >= ?"
        params.append(int(start_time))
    if end_time is not None:
        q += " AND close_time <= ?"
        params.append(int(end_time))
    q += " ORDER BY close_time ASC, position_id ASC"
    rows = [dict(r) for r in conn.execute(q, tuple(params)).fetchall()]
    conn.close()

    for r in rows:
        r["profit"] = float(r["profit"] or 0.0)
        r["pips"] = float(r["pips"] or 0.0)
        r["lots"] = float(r["lots"] or 0.0)

    # ---- Period performance (by close time, broker calendar) ----
    now_broker = _utc_dt((now_ts if now_ts is not None else time.time()) + broker_offset)
    day0 = now_broker.replace(hour=0, minute=0, second=0, microsecond=0)
    week0 = day0 - timedelta(days=day0.weekday())
    month0 = day0.replace(day=1)
    year0 = day0.replace(month=1, day=1)
    periods = []
    for label, start in (("Today", day0), ("This Week", week0), ("This Month", month0), ("This Year", year0)):
        st = int(start.timestamp())
        periods.append({"label": label, **_summary([r for r in rows if r["close_time"] >= st], base_balance)})

    overall = _summary(rows, base_balance)

    # ---- Extremes ----
    best = max(rows, key=lambda r: r["profit"]) if rows else None
    worst = min(rows, key=lambda r: r["profit"]) if rows else None
    best_p = max(rows, key=lambda r: r["pips"]) if rows else None
    worst_p = min(rows, key=lambda r: r["pips"]) if rows else None
    with_mae = [r for r in rows if r["mae_pips"] is not None]
    durations = [r["duration_sec"] for r in rows if r["duration_sec"] is not None]

    # ---- Daily losses (broker calendar day of close) ----
    by_day = _bucket(rows, lambda r: _utc_dt(r["close_time"]).strftime("%Y-%m-%d"))
    max_loss_count, max_loss_day, max_loss_last_ts = 0, None, None
    worst_day, worst_day_profit, worst_day_pips = None, 0.0, 0.0
    for day, drs in by_day.items():
        losers = [r for r in drs if r["profit"] < 0]
        if len(losers) > max_loss_count or (len(losers) == max_loss_count and losers and day > (max_loss_day or "")):
            max_loss_count, max_loss_day = len(losers), day
            max_loss_last_ts = max(r["close_time"] for r in losers) if losers else None
        dp = sum(r["profit"] for r in drs)
        if dp < worst_day_profit:
            worst_day, worst_day_profit, worst_day_pips = day, dp, sum(r["pips"] for r in drs)

    cards = {
        "total_orders": overall["trades"],
        "profitability": overall["accuracy"],
        "total_pips": overall["pips"],
        "avg_win": overall["avg_win"],
        "avg_loss": overall["avg_loss"],
        "total_lots": overall["lots"],
        "longs_won": overall["longs_won"],
        "shorts_won": overall["shorts_won"],
        "best_trade_profit": _r2(best["profit"]) if best else 0.0,
        "worst_trade_profit": _r2(worst["profit"]) if worst else 0.0,
        "best_trade_pips": _r2(best_p["pips"]) if best_p else 0.0,
        "worst_trade_pips": _r2(worst_p["pips"]) if worst_p else 0.0,
        "lowest_drawdown_pips": _r2(-min(r["mae_pips"] for r in with_mae)) if with_mae else None,
        "highest_drawdown_pips": _r2(-max(r["mae_pips"] for r in with_mae)) if with_mae else None,
        "longest_trade": _fmt_duration(max(durations)) if durations else None,
        "shortest_trade": _fmt_duration(min(durations)) if durations else None,
        "max_daily_losses": max_loss_count,
        "max_daily_losses_time": (_utc_dt(max_loss_last_ts).strftime("%B %d, %Y at %H:%M:%S")
                                  if max_loss_last_ts else None),
        "worst_day": worst_day,
        "worst_day_profit": _r2(worst_day_profit),
        "worst_day_pips": _r2(worst_day_pips),
        "worst_day_pct": _r2(worst_day_profit / base_balance * 100) if base_balance and base_balance > 0 else 0.0,
        "profit_factor": overall["profit_factor"],
        "commission_total": _r2(sum(float(r["commission"] or 0) for r in rows)),
        "swap_total": _r2(sum(float(r["swap"] or 0) for r in rows)),
    }

    # ---- Breakdowns ----
    by_symbol = [{"key": k, **_summary(v, base_balance)}
                 for k, v in sorted(_bucket(rows, lambda r: r["symbol"]).items(), key=lambda kv: -len(kv[1]))]
    by_hour = [{"key": k, **_summary(v, base_balance)}
               for k, v in sorted(_bucket(rows, lambda r: _utc_dt(r["open_time"]).hour).items())]
    by_day_w = [{"key": WEEKDAY_NAMES[k], **_summary(v, base_balance)}
                for k, v in sorted(_bucket(rows, lambda r: _utc_dt(r["open_time"]).weekday()).items())]
    reasons = _bucket(rows, lambda r: r["close_reason"] or "OTHER")
    ordered = [k for k in CLOSE_REASON_ORDER if k in reasons] + sorted(k for k in reasons if k not in CLOSE_REASON_ORDER)
    by_reason = [{"key": k, **_summary(reasons[k], base_balance)} for k in ordered]

    return {
        "provider_id": str(provider_id),
        "base_balance": _r2(base_balance),
        "broker_utc_offset_hours": round(broker_offset / 3600.0, 2),
        "generated_at": int(time.time()),
        "coverage": {
            "positions": len(rows),
            "with_drawdown": len(with_mae),
            "first_close": _utc_dt(rows[0]["close_time"]).strftime("%Y-%m-%d %H:%M") if rows else None,
            "last_close": _utc_dt(rows[-1]["close_time"]).strftime("%Y-%m-%d %H:%M") if rows else None,
        },
        "periods": periods,
        "cards": cards,
        "by_symbol": by_symbol,
        "by_hour": by_hour,
        "by_weekday": by_day_w,
        "by_close_reason": by_reason,
    }
