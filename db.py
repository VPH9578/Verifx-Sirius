import sqlite3
import os
import time
from datetime import datetime

DB_FILE = "stats.db"

def get_db():
    conn = sqlite3.connect(DB_FILE, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS providers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            provider_id TEXT UNIQUE,
            name TEXT
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            ticket INTEGER PRIMARY KEY,
            provider_id TEXT,
            symbol TEXT,
            trade_type TEXT,
            lots REAL,
            open_time INTEGER,
            close_time INTEGER,
            open_price REAL,
            close_price REAL,
            profit REAL,
            magic INTEGER,
            close_reason TEXT,
            FOREIGN KEY(provider_id) REFERENCES providers(provider_id)
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS trade_signals (
            signal_id TEXT PRIMARY KEY,
            provider_id TEXT,
            symbol TEXT,
            trade_type TEXT,
            lots REAL DEFAULT 0.0,
            risk_amount REAL DEFAULT 0.0,
            realized_r REAL DEFAULT 0.0,
            tp1_hit INTEGER DEFAULT 0,
            tp2_hit INTEGER DEFAULT 0,
            tp3_hit INTEGER DEFAULT 0,
            sl_hit INTEGER DEFAULT 0,
            profit REAL DEFAULT 0.0,
            status TEXT DEFAULT 'OPEN',
            created_at INTEGER DEFAULT 0,
            closed_at INTEGER DEFAULT 0,
            FOREIGN KEY(provider_id) REFERENCES providers(provider_id)
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS app_metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS execution_errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            error_type TEXT,
            reason TEXT,
            provider_id TEXT,
            symbol TEXT,
            raw_message TEXT,
            created_at INTEGER
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS signal_watch_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_key TEXT UNIQUE,
            message_id INTEGER,
            provider_id TEXT,
            provider_name TEXT,
            channel_id TEXT,
            topic_id INTEGER DEFAULT 0,
            symbol TEXT,
            direction TEXT,
            event_kind TEXT DEFAULT 'NEW_SIGNAL',
            raw_message TEXT,
            parsed_signal_json TEXT DEFAULT '{}',
            expected_accounts_json TEXT DEFAULT '[]',
            account_statuses_json TEXT DEFAULT '{}',
            overall_status TEXT DEFAULT 'COPIED',
            has_discrepancy INTEGER DEFAULT 0,
            report_status TEXT DEFAULT 'NONE',
            ai_summary TEXT DEFAULT '',
            ai_report_md TEXT DEFAULT '',
            antigravity_prompt TEXT DEFAULT '',
            ai_confidence TEXT DEFAULT 'high',
            evidence_json TEXT DEFAULT '{}',
            alert_sent INTEGER DEFAULT 0,
            created_at INTEGER,
            updated_at INTEGER
        )
    ''')
    c.execute("CREATE INDEX IF NOT EXISTS idx_sw_prov_created ON signal_watch_events(provider_id, created_at DESC)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sw_issue_created ON signal_watch_events(has_discrepancy, created_at DESC)")
    try:
        c.execute("ALTER TABLE signal_watch_events ADD COLUMN ai_confidence TEXT DEFAULT 'high'")
    except:
        pass
    # Migrate columns if existing
    try:
        c.execute('ALTER TABLE trade_signals ADD COLUMN risk_amount REAL DEFAULT 0.0')
    except:
        pass
    try:
        c.execute('ALTER TABLE trade_signals ADD COLUMN realized_r REAL DEFAULT 0.0')
    except:
        pass
    try:
        c.execute("ALTER TABLE trades ADD COLUMN account_id TEXT DEFAULT ''")
    except:
        pass
    try:
        c.execute("ALTER TABLE trade_signals ADD COLUMN account_id TEXT DEFAULT ''")
    except:
        pass
    try:
        c.execute("ALTER TABLE trade_analytics ADD COLUMN account_id TEXT DEFAULT ''")
    except:
        pass

    c.execute("CREATE INDEX IF NOT EXISTS idx_trades_acc_prov ON trades(account_id, provider_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_signals_acc_prov ON trade_signals(account_id, provider_id)")

    # One-time migration: assign legacy records to their proper accounts
    # Provider 543 -> acc_1 (PUPrime MT5 #33849921)
    # All other existing providers -> acc_1793656436 (Varianse cTrader #10010341)
    # Target account ending in 357 (acc_1794159206) -> 0 records (clean fresh slate)
    try:
        c.execute("UPDATE trades SET account_id = 'acc_1' WHERE (account_id IS NULL OR account_id = '') AND provider_id = '543'")
        c.execute("UPDATE trades SET account_id = 'acc_1793656436' WHERE (account_id IS NULL OR account_id = '') AND provider_id != '543'")
        c.execute("UPDATE trade_signals SET account_id = 'acc_1' WHERE (account_id IS NULL OR account_id = '') AND provider_id = '543'")
        c.execute("UPDATE trade_signals SET account_id = 'acc_1793656436' WHERE (account_id IS NULL OR account_id = '') AND provider_id != '543'")
        try:
            c.execute("UPDATE trade_analytics SET account_id = 'acc_1' WHERE (account_id IS NULL OR account_id = '') AND provider_id = '543'")
            c.execute("UPDATE trade_analytics SET account_id = 'acc_1793656436' WHERE (account_id IS NULL OR account_id = '') AND provider_id != '543'")
        except Exception:
            pass
    except Exception as _e:
        print("[DB] Migration notice:", _e)

    conn.commit()
    conn.close()

def get_metadata(key, default=None):
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT value FROM app_metadata WHERE key = ?', (key,))
    row = c.fetchone()
    conn.close()
    return row['value'] if row else default

def set_metadata(key, value):
    conn = get_db()
    c = conn.cursor()
    c.execute('INSERT OR REPLACE INTO app_metadata (key, value) VALUES (?, ?)', (key, str(value)))
    conn.commit()
    conn.close()

def log_execution_error(error_type: str, reason: str, provider_id: str = None, symbol: str = None, raw_message: str = "") -> dict:
    """Records a skipped trade or execution error for frontend and auditing."""
    conn = get_db()
    c = conn.cursor()
    ts = datetime.now().isoformat()
    now_int = int(time.time())
    c.execute('''
        INSERT INTO execution_errors (timestamp, error_type, reason, provider_id, symbol, raw_message, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (ts, str(error_type), str(reason), str(provider_id or ""), str(symbol or ""), str(raw_message or "")[:500], now_int))
    conn.commit()
    conn.close()
    return {
        "timestamp": ts,
        "error_type": error_type,
        "reason": reason,
        "provider_id": provider_id,
        "symbol": symbol,
        "raw_message": raw_message[:500] if raw_message else "",
        "created_at": now_int
    }

def get_recent_errors(limit: int = 50, provider_id: str = None) -> list:
    """Returns recent skipped trades and execution errors."""
    conn = get_db()
    c = conn.cursor()
    try:
        if provider_id:
            c.execute('SELECT * FROM execution_errors WHERE provider_id = ? ORDER BY id DESC LIMIT ?', (str(provider_id), limit))
        else:
            c.execute('SELECT * FROM execution_errors ORDER BY id DESC LIMIT ?', (limit,))
        rows = [dict(r) for r in c.fetchall()]
    except Exception:
        rows = []
    finally:
        conn.close()
    return rows

def save_trade(ticket, provider_id, symbol, trade_type, lots, open_time, close_time, open_price, close_price, profit, magic, close_reason="TP/SL", account_id=""):
    conn = get_db()
    c = conn.cursor()
    c.execute('''
        INSERT OR REPLACE INTO trades (ticket, provider_id, symbol, trade_type, lots, open_time, close_time, open_price, close_price, profit, magic, close_reason, account_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (ticket, str(provider_id), symbol, trade_type, lots, open_time, close_time, open_price, close_price, profit, magic, close_reason, str(account_id or "")))
    conn.commit()
    conn.close()

def save_or_update_signal(signal_id, provider_id, symbol, trade_type, lots, tp1_hit, tp2_hit, tp3_hit, sl_hit, profit, status='OPEN', created_at=0, closed_at=0, risk_amount=0.0, account_id=""):
    conn = get_db()
    c = conn.cursor()
    req_acc = str(account_id or "").strip()
    sig_str = str(signal_id)

    # Check if signal_id already exists and prevent cross-account signal_id collision
    c.execute('SELECT risk_amount, account_id FROM trade_signals WHERE signal_id = ?', (sig_str,))
    existing = c.fetchone()
    if existing and existing['account_id'] and req_acc and str(existing['account_id']) != req_acc:
        if not sig_str.startswith(f"{req_acc}_"):
            sig_str = f"{req_acc}_{sig_str}"
            c.execute('SELECT risk_amount, account_id FROM trade_signals WHERE signal_id = ?', (sig_str,))
            existing = c.fetchone()

    stored_risk = existing['risk_amount'] if existing and existing['risk_amount'] > 0 else float(risk_amount)
    stored_acc = existing['account_id'] if (existing and existing['account_id']) else req_acc

    # Calculate realized_r if risk is known and trade is closing or has profit
    calc_r = 0.0
    if stored_risk > 0:
        calc_r = round(float(profit) / stored_risk, 2)
    elif sl_hit:
        # If trade hit pure SL and risk wasn't explicitly set, default to -1.0R
        calc_r = -1.0

    c.execute('''
        INSERT INTO trade_signals (signal_id, provider_id, symbol, trade_type, lots, risk_amount, realized_r, tp1_hit, tp2_hit, tp3_hit, sl_hit, profit, status, created_at, closed_at, account_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(signal_id) DO UPDATE SET
            lots=excluded.lots,
            risk_amount=CASE WHEN excluded.risk_amount > 0 THEN excluded.risk_amount ELSE trade_signals.risk_amount END,
            realized_r=CASE WHEN excluded.risk_amount > 0 AND excluded.risk_amount > 0 THEN ROUND(excluded.profit / excluded.risk_amount, 2)
                            WHEN trade_signals.risk_amount > 0 THEN ROUND(excluded.profit / trade_signals.risk_amount, 2)
                            WHEN excluded.sl_hit = 1 THEN -1.0
                            ELSE trade_signals.realized_r END,
            tp1_hit=MAX(trade_signals.tp1_hit, excluded.tp1_hit),
            tp2_hit=MAX(trade_signals.tp2_hit, excluded.tp2_hit),
            tp3_hit=MAX(trade_signals.tp3_hit, excluded.tp3_hit),
            sl_hit=MAX(trade_signals.sl_hit, excluded.sl_hit),
            profit=excluded.profit,
            status=excluded.status,
            closed_at=MAX(trade_signals.closed_at, excluded.closed_at),
            account_id=CASE WHEN trade_signals.account_id != '' THEN trade_signals.account_id ELSE excluded.account_id END
    ''', (sig_str, str(provider_id), symbol, trade_type, lots, float(stored_risk), float(calc_r), int(tp1_hit), int(tp2_hit), int(tp3_hit), int(sl_hit), float(profit), status, int(created_at), int(closed_at), stored_acc))
    conn.commit()
    conn.close()

def get_provider_stats(provider_id, start_time=None, end_time=None, account_id=None):
    conn = get_db()
    c = conn.cursor()
    
    # Base signal query filters
    sig_filters = ['provider_id = ?']
    sig_params = [str(provider_id)]
    if account_id:
        sig_filters.append('account_id = ?')
        sig_params.append(str(account_id))
    if start_time is not None:
        sig_filters.append('(closed_at >= ? OR (closed_at = 0 AND created_at >= ?))')
        sig_params.extend([int(start_time), int(start_time)])
    if end_time is not None:
        sig_filters.append('(created_at <= ?)')
        sig_params.append(int(end_time))

    sig_where = " AND ".join(sig_filters)
    c.execute(f'''
        SELECT 
            COUNT(*) as total_signals,
            COALESCE(SUM(lots), 0.0) as total_lots,
            COALESCE(SUM(tp1_hit), 0) as tp1_total,
            COALESCE(SUM(tp2_hit), 0) as tp2_total,
            COALESCE(SUM(tp3_hit), 0) as tp3_total,
            COALESCE(SUM(sl_hit), 0) as sl_total,
            COALESCE(SUM(profit), 0.0) as total_profit,
            AVG(CASE WHEN profit > 0 THEN profit ELSE NULL END) as avg_win_profit,
            AVG(CASE WHEN profit < 0 THEN ABS(profit) ELSE NULL END) as avg_loss_profit,
            AVG(CASE WHEN profit > 0 AND realized_r > 0 THEN realized_r ELSE NULL END) as avg_win_r,
            AVG(CASE WHEN profit < 0 THEN ABS(realized_r) ELSE NULL END) as avg_loss_r
        FROM trade_signals WHERE {sig_where}
    ''', tuple(sig_params))
    sig_row = c.fetchone()
    
    # Trades table query filters
    tr_filters = ['provider_id = ?']
    tr_params = [str(provider_id)]
    if account_id:
        tr_filters.append('account_id = ?')
        tr_params.append(str(account_id))
    if start_time is not None:
        tr_filters.append('close_time >= ?')
        tr_params.append(int(start_time))
    if end_time is not None:
        tr_filters.append('close_time <= ?')
        tr_params.append(int(end_time))
    tr_where = " AND ".join(tr_filters)
    c.execute(f'SELECT * FROM trades WHERE {tr_where}', tuple(tr_params))
    trades = c.fetchall()
    
    deal_trades = len(trades)
    trade_profit = sum(t['profit'] for t in trades) if trades else 0.0
    trade_lots = sum(t['lots'] for t in trades) if trades else 0.0
    winning_deals = len([t for t in trades if t['profit'] > 0])
    
    total_signals = sig_row['total_signals'] if sig_row else 0
    tp1_count = int(sig_row['tp1_total']) if sig_row else 0
    tp2_count = int(sig_row['tp2_total']) if sig_row else 0
    tp3_count = int(sig_row['tp3_total']) if sig_row else 0
    sl_count = int(sig_row['sl_total']) if sig_row else 0
    lots_count = float(sig_row['total_lots']) if sig_row and sig_row['total_lots'] > 0 else float(trade_lots)
    profit = trade_profit if trades else (float(sig_row['total_profit']) if sig_row else 0.0)

    # Clean Reward : Risk calculation (Trader format, e.g. 2:1, 1.5:1, or 1:1.2)
    avg_win_dollar = float(sig_row['avg_win_profit']) if sig_row and sig_row['avg_win_profit'] else 0.0
    avg_loss_dollar = float(sig_row['avg_loss_profit']) if sig_row and sig_row['avg_loss_profit'] else 0.0
    avg_win_r = float(sig_row['avg_win_r']) if sig_row and sig_row['avg_win_r'] else 0.0

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
    elif avg_win_r > 0:
        rr_display = f"{avg_win_r:.1f}:1"
    else:
        rr_display = "1:1"

    # Win rate calculation
    completed_signals = tp1_count + sl_count
    if completed_signals > 0:
        win_rate = (tp1_count / completed_signals) * 100
    elif deal_trades > 0:
        win_rate = (winning_deals / deal_trades) * 100
    else:
        win_rate = 0.0
    
    # Calculate low-to-high profit curve
    low_to_high_profit = 0.0
    if trades:
        # Sort trades chronologically by close_time
        sorted_trades = sorted(trades, key=lambda x: x['close_time'] or 0)
        c_curve = [0.0]
        c_val = 0.0
        for tr in sorted_trades:
            c_val += tr['profit']
            c_curve.append(c_val)
        min_c = min(c_curve)
        max_c = max(c_curve)
        low_to_high_profit = max(0.0, max_c - min_c)
    elif total_signals > 0:
        c.execute(f'SELECT profit FROM trade_signals WHERE {sig_where} ORDER BY closed_at ASC, created_at ASC', tuple(sig_params))
        sig_profits = c.fetchall()
        c_curve = [0.0]
        c_val = 0.0
        for sp in sig_profits:
            c_val += sp['profit']
            c_curve.append(c_val)
        low_to_high_profit = max(0.0, max(c_curve) - min(c_curve))

    conn.close()
    
    return {
        "trades_count": total_signals if total_signals > 0 else deal_trades,
        "deal_count": deal_trades,
        "lots": round(lots_count, 2),
        "avg_rr": round(avg_win_dollar / avg_loss_dollar, 2) if avg_loss_dollar > 0 else (avg_win_r if avg_win_r > 0 else 1.0),
        "rr_display": rr_display,
        "tp1_hits": tp1_count,
        "tp2_hits": tp2_count,
        "tp3_hits": tp3_count,
        "sl_hits": sl_count,
        "win_rate": round(win_rate, 1),
        "profit": round(profit, 2),
        "low_to_high_profit": round(low_to_high_profit, 2),
        "gain": 0.0
    }

def get_all_providers_stats(provider_ids=None, start_time=None, end_time=None, account_id=None) -> dict:
    """High-speed batch query returning stats for multiple/all providers in a single DB roundtrip, scoped to account_id."""
    conn = get_db()
    c = conn.cursor()

    # 1. Trade signals query with optional provider and account filtering
    sig_filters = []
    sig_params = []
    if provider_ids:
        pids_list = [str(p) for p in provider_ids]
        placeholders = ",".join(["?"] * len(pids_list))
        sig_filters.append(f"provider_id IN ({placeholders})")
        sig_params.extend(pids_list)
    if account_id:
        sig_filters.append("account_id = ?")
        sig_params.append(str(account_id))
    if start_time is not None:
        sig_filters.append("(closed_at >= ? OR (closed_at = 0 AND created_at >= ?))")
        sig_params.extend([int(start_time), int(start_time)])
    if end_time is not None:
        sig_filters.append("(created_at <= ?)")
        sig_params.append(int(end_time))

    sig_where = ("WHERE " + " AND ".join(sig_filters)) if sig_filters else ""
    c.execute(f'''
        SELECT 
            provider_id,
            COUNT(*) as total_signals,
            COALESCE(SUM(lots), 0.0) as total_lots,
            COALESCE(SUM(tp1_hit), 0) as tp1_total,
            COALESCE(SUM(tp2_hit), 0) as tp2_total,
            COALESCE(SUM(tp3_hit), 0) as tp3_total,
            COALESCE(SUM(sl_hit), 0) as sl_total,
            COALESCE(SUM(profit), 0.0) as total_profit,
            AVG(CASE WHEN profit > 0 THEN profit ELSE NULL END) as avg_win_profit,
            AVG(CASE WHEN profit < 0 THEN ABS(profit) ELSE NULL END) as avg_loss_profit,
            AVG(CASE WHEN profit > 0 AND realized_r > 0 THEN realized_r ELSE NULL END) as avg_win_r,
            AVG(CASE WHEN profit < 0 THEN ABS(realized_r) ELSE NULL END) as avg_loss_r
        FROM trade_signals
        {sig_where}
        GROUP BY provider_id
    ''', tuple(sig_params))
    sig_rows = {str(r["provider_id"]): r for r in c.fetchall()}

    # 2. Trades query
    tr_filters = []
    tr_params = []
    if provider_ids:
        pids_list = [str(p) for p in provider_ids]
        placeholders = ",".join(["?"] * len(pids_list))
        tr_filters.append(f"provider_id IN ({placeholders})")
        tr_params.extend(pids_list)
    if account_id:
        tr_filters.append("account_id = ?")
        tr_params.append(str(account_id))
    if start_time is not None:
        tr_filters.append("close_time >= ?")
        tr_params.append(int(start_time))
    if end_time is not None:
        tr_filters.append("close_time <= ?")
        tr_params.append(int(end_time))

    tr_where = ("WHERE " + " AND ".join(tr_filters)) if tr_filters else ""
    c.execute(f"SELECT provider_id, profit, lots, close_time FROM trades {tr_where} ORDER BY close_time ASC", tuple(tr_params))
    all_trades = c.fetchall()
    conn.close()

    trades_by_pid = {}
    for t in all_trades:
        pid = str(t["provider_id"])
        if pid not in trades_by_pid:
            trades_by_pid[pid] = []
        trades_by_pid[pid].append(t)

    target_pids = set(str(p) for p in provider_ids) if provider_ids else (set(sig_rows.keys()) | set(trades_by_pid.keys()))
    out = {}

    for pid in target_pids:
        sig_row = sig_rows.get(pid)
        trades = trades_by_pid.get(pid, [])

        deal_trades = len(trades)
        trade_profit = sum(t["profit"] for t in trades) if trades else 0.0
        trade_lots = sum(t["lots"] for t in trades) if trades else 0.0
        winning_deals = len([t for t in trades if t["profit"] > 0])

        total_signals = sig_row["total_signals"] if sig_row else 0
        tp1_count = int(sig_row["tp1_total"]) if sig_row else 0
        tp2_count = int(sig_row["tp2_total"]) if sig_row else 0
        tp3_count = int(sig_row["tp3_total"]) if sig_row else 0
        sl_count = int(sig_row["sl_total"]) if sig_row else 0
        lots_count = float(sig_row["total_lots"]) if sig_row and sig_row["total_lots"] > 0 else float(trade_lots)
        profit = trade_profit if trades else (float(sig_row["total_profit"]) if sig_row else 0.0)

        avg_win_dollar = float(sig_row["avg_win_profit"]) if sig_row and sig_row["avg_win_profit"] else 0.0
        avg_loss_dollar = float(sig_row["avg_loss_profit"]) if sig_row and sig_row["avg_loss_profit"] else 0.0
        avg_win_r = float(sig_row["avg_win_r"]) if sig_row and sig_row["avg_win_r"] else 0.0

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
        elif avg_win_r > 0:
            rr_display = f"{avg_win_r:.1f}:1"
        else:
            rr_display = "1:1"

        completed_signals = tp1_count + sl_count
        if completed_signals > 0:
            win_rate = (tp1_count / completed_signals) * 100
        elif deal_trades > 0:
            win_rate = (winning_deals / deal_trades) * 100
        else:
            win_rate = 0.0

        low_to_high_profit = 0.0
        if trades:
            c_curve = [0.0]
            c_val = 0.0
            for tr in trades:
                c_val += tr["profit"]
                c_curve.append(c_val)
            low_to_high_profit = max(0.0, max(c_curve) - min(c_curve))
        elif total_signals > 0:
            conn_sig = get_db()
            sig_profits = conn_sig.execute(
                f"SELECT profit FROM trade_signals {sig_where} AND provider_id = ? ORDER BY closed_at ASC, created_at ASC",
                tuple(sig_params + [str(pid)]) if sig_where else (str(pid),)
            ).fetchall()
            conn_sig.close()
            c_curve = [0.0]
            c_val = 0.0
            for sp in sig_profits:
                c_val += float(sp["profit"] or 0.0)
                c_curve.append(c_val)
            low_to_high_profit = max(0.0, max(c_curve) - min(c_curve))

        out[pid] = {
            "trades_count": total_signals if total_signals > 0 else deal_trades,
            "deal_count": deal_trades,
            "lots": round(lots_count, 2),
            "avg_rr": round(avg_win_dollar / avg_loss_dollar, 2) if avg_loss_dollar > 0 else (avg_win_r if avg_win_r > 0 else 1.0),
            "rr_display": rr_display,
            "tp1_hits": tp1_count,
            "tp2_hits": tp2_count,
            "tp3_hits": tp3_count,
            "sl_hits": sl_count,
            "win_rate": round(win_rate, 1),
            "profit": round(profit, 2),
            "low_to_high_profit": round(low_to_high_profit, 2),
            "gain": 0.0
        }
    return out

def get_recent_trades(limit=10):
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT * FROM trades ORDER BY close_time DESC LIMIT ?', (limit,))
    trades = [dict(t) for t in c.fetchall()]
    
    if not trades:
        conn.close()
        return []

    # Correlate with trade_signals to resolve exact TP1, TP2, TP3, BE1, BE2, SL status
    c.execute('SELECT * FROM trade_signals ORDER BY created_at ASC')
    signals = [dict(s) for s in c.fetchall()]
    
    # Also fetch all trades related to these signals to evaluate order within signal
    min_time = min(t['close_time'] for t in trades) - 86400
    c.execute('SELECT ticket, symbol, trade_type, lots, profit, close_time, close_reason FROM trades WHERE close_time >= ? ORDER BY close_time ASC', (min_time,))
    relevant_trades = [dict(t) for t in c.fetchall()]
    conn.close()

    # Map all relevant trades to their signals
    sig_trade_map = {}
    for rt in relevant_trades:
        # Match signal
        matching = [
            s for s in signals
            if s['created_at'] <= rt['close_time'] <= (s['closed_at'] or rt['close_time']) + 60
            and (s['symbol'] == rt['symbol'] if s.get('symbol') and rt.get('symbol') else True)
            and (s['trade_type'] == rt['trade_type'] if s.get('trade_type') and rt.get('trade_type') else True)
        ]
        if not matching:
            matching = [
                s for s in signals
                if s['created_at'] <= rt['close_time'] <= (s['closed_at'] or rt['close_time']) + 60
            ]
        
        if matching:
            best_sig = min(matching, key=lambda s: abs((s['closed_at'] or s['created_at']) - rt['close_time']))
            sig_trade_map.setdefault(best_sig['signal_id'], []).append(rt)

    # Determine status for each trade ticket
    ticket_status = {}
    for s in signals:
        sid = s['signal_id']
        s_trades = sig_trade_map.get(sid, [])
        s_trades = sorted(s_trades, key=lambda x: (x['close_time'], x['ticket']))
        
        tp1 = bool(s.get('tp1_hit', 0))
        tp2 = bool(s.get('tp2_hit', 0))
        tp3 = bool(s.get('tp3_hit', 0))
        sl = bool(s.get('sl_hit', 0))
        
        if sl and not tp1 and not tp2 and not tp3:
            for tr in s_trades:
                ticket_status[tr['ticket']] = 'SL'
        elif tp3:
            idx = 1
            for tr in s_trades:
                ticket_status[tr['ticket']] = f"TP{idx}" if idx <= 3 else "TP3"
                idx += 1
        elif tp2 and not tp3:
            # TP1, TP2 hit, subsequent trade closed at break-even (TP1 level) -> BE2
            tp1_assigned = False
            tp2_assigned = False
            for tr in s_trades:
                if tr['profit'] > 0 and not tp1_assigned:
                    ticket_status[tr['ticket']] = 'TP1'
                    tp1_assigned = True
                elif tr['profit'] > 0 and not tp2_assigned:
                    ticket_status[tr['ticket']] = 'TP2'
                    tp2_assigned = True
                else:
                    ticket_status[tr['ticket']] = 'BE2'
        elif tp1 and not tp2 and not tp3:
            # TP1 hit, subsequent trade closed at break-even -> BE1
            tp1_assigned = False
            for tr in s_trades:
                if tr['profit'] > 0 and not tp1_assigned:
                    ticket_status[tr['ticket']] = 'TP1'
                    tp1_assigned = True
                else:
                    ticket_status[tr['ticket']] = 'BE1'
        else:
            for tr in s_trades:
                ticket_status[tr['ticket']] = 'TP' if tr['profit'] >= 0 else 'SL'

    # Assign determined close_reason to the return list
    for t in trades:
        tkt = t['ticket']
        curr_reason = t.get('close_reason')
        if curr_reason and curr_reason.upper() == 'MANUAL':
            continue
        if tkt in ticket_status:
            t['close_reason'] = ticket_status[tkt]
        else:
            t['close_reason'] = 'TP' if t['profit'] >= 0 else 'SL'

    return trades

def get_consolidated_history(limit=10, provider_ids=None, account_id=None):
    """
    Consolidates individual trade executions (TP1, TP2, TP3, BE, SL) by their originating signal.
    Returns 1 row per originating signal with consolidated metrics (total lots, total profit),
    and highlights the primary signal achievement (e.g. TP1, TP2, TP3, SL), scoped to account_id.
    """
    conn = get_db()
    c = conn.cursor()

    sig_filters = []
    sig_params = []
    tr_filters = []
    tr_params = []

    if provider_ids:
        p_list = [str(p) for p in provider_ids]
        placeholders = ','.join('?' for _ in p_list)
        sig_filters.append(f"provider_id IN ({placeholders})")
        sig_params.extend(p_list)
        tr_filters.append(f"provider_id IN ({placeholders})")
        tr_params.extend(p_list)

    if account_id:
        sig_filters.append("account_id = ?")
        sig_params.append(str(account_id))
        tr_filters.append("account_id = ?")
        tr_params.append(str(account_id))

    sig_where = ("WHERE " + " AND ".join(sig_filters)) if sig_filters else ""
    tr_where = ("WHERE " + " AND ".join(tr_filters)) if tr_filters else ""

    c.execute(f'SELECT * FROM trade_signals {sig_where} ORDER BY closed_at DESC, created_at DESC LIMIT ?', (*sig_params, max(20, limit * 2)))
    signals = [dict(s) for s in c.fetchall()]
    c.execute(f'SELECT * FROM trades {tr_where} ORDER BY close_time ASC', tuple(tr_params))
    all_trades = [dict(t) for t in c.fetchall()]
    conn.close()

    matched_tickets = set()
    consolidated = []

    for s in signals:
        sid = s['signal_id']
        s_created = s.get('created_at') or 0
        s_closed = s.get('closed_at') or 0
        s_symbol = s.get('symbol')

        candidate_trades = []
        for t in all_trades:
            if t['ticket'] in matched_tickets:
                continue
            if s_symbol and t.get('symbol') and s_symbol != t.get('symbol'):
                continue
            if s.get('provider_id') and t.get('provider_id') and str(s.get('provider_id')) != str(t.get('provider_id')):
                continue
            t_close = t.get('close_time') or 0
            # Allow window around signal creation and closing (treat open signal as extending to now + broker offset buffer)
            eff_closed = s_closed if (s_closed and s_closed > 0) else max(int(s_created), int(time.time()) + 14400)
            if (s_created - 300) <= t_close <= (eff_closed + 300):
                candidate_trades.append(t)

        if candidate_trades:
            for ct in candidate_trades:
                matched_tickets.add(ct['ticket'])
            total_profit = round(sum(ct['profit'] for ct in candidate_trades), 2)
            total_lots = round(sum(ct['lots'] for ct in candidate_trades), 2)
            latest_close = max(ct['close_time'] for ct in candidate_trades)
            
            # Determine highlighted signal result
            if s.get('tp3_hit'):
                status = 'TP3'
            elif s.get('tp2_hit'):
                status = 'TP2'
            elif s.get('tp1_hit'):
                status = 'TP1'
            elif s.get('sl_hit'):
                status = 'SL'
            else:
                status = 'TP' if total_profit >= 0 else 'SL'

            consolidated.append({
                'signal_id': sid,
                'provider_id': s.get('provider_id'),
                'symbol': s.get('symbol') or candidate_trades[0]['symbol'],
                'trade_type': s.get('trade_type') or candidate_trades[0]['trade_type'],
                'lots': total_lots if total_lots > 0 else s.get('lots', 0.0),
                'profit': total_profit,
                'status': status,
                'close_time': latest_close,
                'trades_count': len(candidate_trades),
                'sub_trades': candidate_trades
            })
        else:
            status = 'TP3' if s.get('tp3_hit') else ('TP2' if s.get('tp2_hit') else ('TP1' if s.get('tp1_hit') else ('SL' if s.get('sl_hit') else 'CLOSED')))
            consolidated.append({
                'signal_id': sid,
                'provider_id': s.get('provider_id'),
                'symbol': s.get('symbol'),
                'trade_type': s.get('trade_type'),
                'lots': s.get('lots', 0.0),
                'profit': round(float(s.get('profit') or 0.0), 2),
                'status': status,
                'close_time': s.get('closed_at') or s.get('created_at'),
                'trades_count': 1,
                'sub_trades': []
            })

    # Catch any orphan trades not linked to recorded trade_signals
    for t in all_trades:
        if t['ticket'] not in matched_tickets:
            consolidated.append({
                'signal_id': 'T_' + str(t['ticket']),
                'provider_id': t.get('provider_id'),
                'symbol': t.get('symbol'),
                'trade_type': t.get('trade_type'),
                'lots': t.get('lots', 0.0),
                'profit': round(float(t.get('profit') or 0.0), 2),
                'status': t.get('close_reason') or ('TP' if t.get('profit', 0) >= 0 else 'SL'),
                'close_time': t.get('close_time'),
                'trades_count': 1,
                'sub_trades': [t]
            })

    consolidated.sort(key=lambda x: x['close_time'] or 0, reverse=True)
    return consolidated[:limit]

def reset_account_data(provider_ids=None, keep_providers=True, new_starting_balance=None, account_id=None):
    """
    Resets trades, signals, and analytics.
    - provider_ids: optional list of provider IDs. If provided, only trades belonging to these providers are cleared.
    - keep_providers: bool. If True, provider records in DB are not touched. If False, they are deleted.
    - new_starting_balance: float. Live broker balance to benchmark as the new baseline starting balance.
    - account_id: str. Optional account ID for per-account metadata.
    """
    conn = get_db()
    c = conn.cursor()
    
    if account_id:
        c.execute('DELETE FROM trades WHERE account_id = ?', (str(account_id),))
        c.execute('DELETE FROM trade_signals WHERE account_id = ?', (str(account_id),))
        try:
            c.execute('DELETE FROM trade_analytics WHERE account_id = ?', (str(account_id),))
        except Exception:
            pass
        if provider_ids:
            p_list = [str(p) for p in provider_ids]
            placeholders = ','.join('?' for _ in p_list)
            try:
                c.execute(f'DELETE FROM execution_errors WHERE provider_id IN ({placeholders})', p_list)
            except Exception:
                pass
            if not keep_providers:
                try:
                    c.execute(f'DELETE FROM providers WHERE provider_id IN ({placeholders})', p_list)
                except Exception:
                    pass
    elif provider_ids:
        p_list = [str(p) for p in provider_ids]
        placeholders = ','.join('?' for _ in p_list)
        c.execute(f'DELETE FROM trades WHERE provider_id IN ({placeholders})', p_list)
        c.execute(f'DELETE FROM trade_signals WHERE provider_id IN ({placeholders})', p_list)
        try:
            c.execute(f'DELETE FROM trade_analytics WHERE provider_id IN ({placeholders})', p_list)
        except Exception:
            pass
        try:
            c.execute(f'DELETE FROM execution_errors WHERE provider_id IN ({placeholders})', p_list)
        except Exception:
            pass
        if not keep_providers:
            try:
                c.execute(f'DELETE FROM providers WHERE provider_id IN ({placeholders})', p_list)
            except Exception:
                pass
    else:
        c.execute('DELETE FROM trades')
        c.execute('DELETE FROM trade_signals')
        try:
            c.execute('DELETE FROM trade_analytics')
        except Exception:
            pass
        try:
            c.execute('DELETE FROM execution_errors')
        except Exception:
            pass
        if not keep_providers:
            c.execute('DELETE FROM providers')

    now_ts = int(datetime.now().timestamp())
    if account_id:
        c.execute("INSERT OR REPLACE INTO app_metadata (key, value) VALUES (?, ?)", (f'reset_time_{account_id}', str(now_ts)))
        if account_id == "acc_1":
            c.execute("INSERT OR REPLACE INTO app_metadata (key, value) VALUES ('reset_time', ?)", (str(now_ts),))
    else:
        c.execute("INSERT OR REPLACE INTO app_metadata (key, value) VALUES ('reset_time', ?)", (str(now_ts),))

    if new_starting_balance is not None:
        bal_str = str(round(float(new_starting_balance), 2))
        if account_id:
            c.execute("INSERT OR REPLACE INTO app_metadata (key, value) VALUES (?, ?)", (f'starting_balance_{account_id}', bal_str))
            if account_id == "acc_1":
                c.execute("INSERT OR REPLACE INTO app_metadata (key, value) VALUES ('starting_balance', ?)", (bal_str,))
        else:
            c.execute("INSERT OR REPLACE INTO app_metadata (key, value) VALUES ('starting_balance', ?)", (bal_str,))

    conn.commit()
    conn.close()
    return now_ts


def _deserialize_signal_watch_row(row) -> dict:
    """Converts a sqlite3.Row from signal_watch_events into a clean dict with parsed JSON fields."""
    import json
    d = dict(row)
    for jkey, target_key, default_val in (
        ("parsed_signal_json", "parsed_signal", {}),
        ("expected_accounts_json", "expected_accounts", []),
        ("account_statuses_json", "account_statuses", {}),
        ("evidence_json", "evidence", {}),
    ):
        raw_val = d.get(jkey)
        try:
            d[target_key] = json.loads(raw_val) if raw_val else default_val
        except Exception:
            d[target_key] = default_val
    return d


def upsert_signal_watch_event(event: dict) -> int:
    """Inserts or updates a Signal Watch event record and returns its row id."""
    import json
    conn = get_db()
    c = conn.cursor()
    now_ts = int(event.get("created_at") or time.time())
    upd_ts = int(event.get("updated_at") or time.time())
    event_key = str(event.get("event_key") or f"{event.get('provider_id', '')}_{event.get('message_id', 0)}_{now_ts}")

    parsed_json = json.dumps(event.get("parsed_signal") or {}, ensure_ascii=False)
    expected_json = json.dumps(event.get("expected_accounts") or [], ensure_ascii=False)
    statuses_json = json.dumps(event.get("account_statuses") or {}, ensure_ascii=False)
    evidence_json = json.dumps(event.get("evidence") or {}, ensure_ascii=False)

    c.execute('''
        INSERT INTO signal_watch_events (
            event_key, message_id, provider_id, provider_name, channel_id, topic_id,
            symbol, direction, event_kind, raw_message, parsed_signal_json,
            expected_accounts_json, account_statuses_json, overall_status, has_discrepancy,
            report_status, ai_summary, ai_report_md, antigravity_prompt, ai_confidence, evidence_json,
            alert_sent, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_key) DO UPDATE SET
            provider_name=excluded.provider_name,
            symbol=CASE WHEN excluded.symbol != '' THEN excluded.symbol ELSE signal_watch_events.symbol END,
            direction=CASE WHEN excluded.direction != '' THEN excluded.direction ELSE signal_watch_events.direction END,
            event_kind=excluded.event_kind,
            raw_message=excluded.raw_message,
            parsed_signal_json=excluded.parsed_signal_json,
            expected_accounts_json=excluded.expected_accounts_json,
            account_statuses_json=excluded.account_statuses_json,
            overall_status=excluded.overall_status,
            has_discrepancy=excluded.has_discrepancy,
            report_status=excluded.report_status,
            ai_summary=CASE WHEN excluded.ai_summary != '' THEN excluded.ai_summary ELSE signal_watch_events.ai_summary END,
            ai_report_md=CASE WHEN excluded.ai_report_md != '' THEN excluded.ai_report_md ELSE signal_watch_events.ai_report_md END,
            antigravity_prompt=CASE WHEN excluded.antigravity_prompt != '' THEN excluded.antigravity_prompt ELSE signal_watch_events.antigravity_prompt END,
            ai_confidence=CASE WHEN excluded.ai_confidence != '' THEN excluded.ai_confidence ELSE signal_watch_events.ai_confidence END,
            evidence_json=CASE WHEN excluded.evidence_json != '{}' THEN excluded.evidence_json ELSE signal_watch_events.evidence_json END,
            updated_at=excluded.updated_at
    ''', (
        event_key,
        int(event.get("message_id") or 0),
        str(event.get("provider_id") or ""),
        str(event.get("provider_name") or ""),
        str(event.get("channel_id") or ""),
        int(event.get("topic_id") or 0),
        str(event.get("symbol") or ""),
        str(event.get("direction") or ""),
        str(event.get("event_kind") or "NEW_SIGNAL"),
        str(event.get("raw_message") or "")[:2000],
        parsed_json,
        expected_json,
        statuses_json,
        str(event.get("overall_status") or "COPIED"),
        int(bool(event.get("has_discrepancy", False))),
        str(event.get("report_status") or "NONE"),
        str(event.get("ai_summary") or ""),
        str(event.get("ai_report_md") or ""),
        str(event.get("antigravity_prompt") or ""),
        str(event.get("ai_confidence") or "high"),
        evidence_json,
        int(bool(event.get("alert_sent", False))),
        now_ts,
        upd_ts,
    ))
    c.execute("SELECT id FROM signal_watch_events WHERE event_key = ?", (event_key,))
    row = c.fetchone()
    watch_id = int(row["id"]) if row else int(c.lastrowid or 0)
    conn.commit()
    conn.close()
    return watch_id


def update_signal_watch_report(
    watch_id: int,
    report_status: str,
    ai_summary: str = "",
    ai_report_md: str = "",
    antigravity_prompt: str = "",
    ai_confidence: str = None,
    evidence: dict = None,
    alert_sent: int = None,
):
    """Updates AI investigation outputs for a Signal Watch event."""
    import json
    conn = get_db()
    c = conn.cursor()
    fields = ["report_status = ?", "updated_at = ?"]
    params = [str(report_status), int(time.time())]
    if ai_summary is not None:
        fields.append("ai_summary = ?")
        params.append(str(ai_summary))
    if ai_report_md is not None:
        fields.append("ai_report_md = ?")
        params.append(str(ai_report_md))
    if antigravity_prompt is not None:
        fields.append("antigravity_prompt = ?")
        params.append(str(antigravity_prompt))
    if ai_confidence is not None:
        fields.append("ai_confidence = ?")
        params.append(str(ai_confidence).lower())
    if evidence is not None:
        fields.append("evidence_json = ?")
        params.append(json.dumps(evidence, ensure_ascii=False))
    if alert_sent is not None:
        fields.append("alert_sent = ?")
        params.append(int(bool(alert_sent)))
    params.append(int(watch_id))
    c.execute(f"UPDATE signal_watch_events SET {', '.join(fields)} WHERE id = ?", params)
    conn.commit()
    conn.close()


def get_signal_watch_events(
    limit: int = 100,
    provider_id: str = None,
    status: str = None,
    only_issues: bool = False,
) -> list:
    """Returns recent Signal Watch events filtered optionally by provider or status."""
    conn = get_db()
    c = conn.cursor()
    where = []
    params = []
    if provider_id and str(provider_id).lower() != "all":
        where.append("provider_id = ?")
        params.append(str(provider_id))
    if only_issues:
        where.append("has_discrepancy = 1")
    elif status and str(status).upper() != "ALL":
        st_u = str(status).upper()
        if st_u == "ISSUES":
            where.append("has_discrepancy = 1")
        elif st_u == "INVESTIGATING":
            where.append("report_status = 'INVESTIGATING'")
        else:
            where.append("overall_status = ?")
            params.append(st_u)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    try:
        c.execute(
            f"SELECT * FROM signal_watch_events {where_sql} ORDER BY created_at DESC, id DESC LIMIT ?",
            params + [int(limit)],
        )
        rows = [_deserialize_signal_watch_row(r) for r in c.fetchall()]
    except Exception:
        rows = []
    finally:
        conn.close()
    return rows


def get_signal_watch_event(watch_id: int) -> dict:
    """Returns a single Signal Watch event by id, or None."""
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("SELECT * FROM signal_watch_events WHERE id = ?", (int(watch_id),))
        row = c.fetchone()
        return _deserialize_signal_watch_row(row) if row else None
    finally:
        conn.close()


def delete_signal_watch_event(watch_id: int) -> bool:
    conn = get_db()
    c = conn.cursor()
    c.execute("DELETE FROM signal_watch_events WHERE id = ?", (int(watch_id),))
    changed = c.rowcount > 0
    conn.commit()
    conn.close()
    return changed


def clear_signal_watch_events() -> bool:
    conn = get_db()
    c = conn.cursor()
    c.execute("DELETE FROM signal_watch_events")
    conn.commit()
    conn.close()
    return True


# Initialize on import
init_db()


