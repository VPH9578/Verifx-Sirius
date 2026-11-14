"""
bt_db.py — Isolated SQLite storage for the Backtester.
Stores cached historical Telegram messages (for instant re-runs with new trading rules)
and saved backtest run results. Never touches live stats.db.
"""

import sqlite3
import json
import os
import time
import config

BT_DB_FILE = getattr(
    config,
    "BACKTESTER_DB_FILE",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "backtest.db"),
)


def get_bt_db():
    os.makedirs(os.path.dirname(os.path.abspath(BT_DB_FILE)), exist_ok=True)
    conn = sqlite3.connect(BT_DB_FILE, timeout=60.0, check_same_thread=False)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        conn.execute("PRAGMA synchronous=NORMAL")
    except Exception:
        pass
    conn.row_factory = sqlite3.Row
    return conn


def init_bt_db():
    conn = get_bt_db()
    c = conn.cursor()

    # Cache of historical Telegram messages per channel + topic
    c.execute('''
        CREATE TABLE IF NOT EXISTS cached_messages (
            channel_id TEXT,
            topic_id INTEGER,
            msg_id INTEGER,
            msg_time INTEGER,
            reply_to_msg_id INTEGER,
            text TEXT,
            parsed_json TEXT,
            PRIMARY KEY (channel_id, topic_id, msg_id)
        )
    ''')
    c.execute('''
        CREATE INDEX IF NOT EXISTS idx_cached_msgs_lookup
        ON cached_messages (channel_id, topic_id, msg_time ASC)
    ''')

    # Track scraped coverage ranges per channel + topic
    c.execute('''
        CREATE TABLE IF NOT EXISTS scrape_ranges (
            channel_id TEXT,
            topic_id INTEGER,
            min_time INTEGER,
            max_time INTEGER,
            updated_at INTEGER,
            channel_title TEXT,
            PRIMARY KEY (channel_id, topic_id)
        )
    ''')

    # Store completed backtest runs
    c.execute('''
        CREATE TABLE IF NOT EXISTS backtest_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at INTEGER,
            provider_name TEXT,
            channel_id TEXT,
            topic_id INTEGER,
            start_time INTEGER,
            end_time INTEGER,
            rules_json TEXT,
            summary_json TEXT,
            graph_json TEXT,
            signals_json TEXT,
            deals_json TEXT
        )
    ''')

    # Fleet Scanner: checkpoints and league table state
    c.execute('''
        CREATE TABLE IF NOT EXISTS fleet_scans (
            scan_id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at INTEGER,
            completed_at INTEGER DEFAULT 0,
            status TEXT,
            total_dialogs INTEGER DEFAULT 0,
            probed_count INTEGER DEFAULT 0,
            viable_count INTEGER DEFAULT 0,
            completed_count INTEGER DEFAULT 0,
            current_dialog TEXT,
            settings_json TEXT
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS fleet_league (
            channel_id TEXT,
            topic_id INTEGER,
            channel_title TEXT,
            dialog_type TEXT,
            scanned_at INTEGER,
            status TEXT,
            messages_scanned INTEGER,
            signals_found INTEGER,
            trades_count INTEGER,
            win_rate REAL,
            net_profit REAL,
            gain_pct REAL,
            max_drawdown_dollar REAL,
            max_drawdown_pct REAL,
            profit_factor REAL,
            recovery_ratio REAL,
            composite_score REAL,
            best_tp_profile TEXT,
            best_sl_profile TEXT,
            best_expiry TEXT,
            best_rules_json TEXT,
            flags_json TEXT,
            is_added_to_copier INTEGER DEFAULT 0,
            scan_id INTEGER DEFAULT 0,
            run_id INTEGER DEFAULT 0,
            PRIMARY KEY (channel_id, topic_id, scan_id)
        )
    ''')

    # Migrate legacy fleet_league tables that only had PRIMARY KEY (channel_id, topic_id)
    # so multiple Fleet Scans can store independent rows per scan_id without overwriting previous scans
    c.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='fleet_league'")
    fl_schema_row = c.fetchone()
    if fl_schema_row and fl_schema_row[0] and "PRIMARY KEY (channel_id, topic_id, scan_id)" not in fl_schema_row[0]:
        c.execute('''
            CREATE TABLE IF NOT EXISTS fleet_league_v2 (
                channel_id TEXT,
                topic_id INTEGER,
                channel_title TEXT,
                dialog_type TEXT,
                scanned_at INTEGER,
                status TEXT,
                messages_scanned INTEGER,
                signals_found INTEGER,
                trades_count INTEGER,
                win_rate REAL,
                net_profit REAL,
                gain_pct REAL,
                max_drawdown_dollar REAL,
                max_drawdown_pct REAL,
                profit_factor REAL,
                recovery_ratio REAL,
                composite_score REAL,
                best_tp_profile TEXT,
                best_sl_profile TEXT,
                best_expiry TEXT,
                best_rules_json TEXT,
                flags_json TEXT,
                is_added_to_copier INTEGER DEFAULT 0,
                scan_id INTEGER DEFAULT 0,
                run_id INTEGER DEFAULT 0,
                PRIMARY KEY (channel_id, topic_id, scan_id)
            )
        ''')
        c.execute('''
            INSERT OR REPLACE INTO fleet_league_v2
            (channel_id, topic_id, channel_title, dialog_type, scanned_at, status,
             messages_scanned, signals_found, trades_count, win_rate, net_profit, gain_pct,
             max_drawdown_dollar, max_drawdown_pct, profit_factor, recovery_ratio, composite_score,
             best_tp_profile, best_sl_profile, best_expiry, best_rules_json, flags_json, is_added_to_copier,
             scan_id, run_id)
            SELECT channel_id, topic_id, channel_title, dialog_type, scanned_at, status,
                   messages_scanned, signals_found, trades_count, win_rate, net_profit, gain_pct,
                   max_drawdown_dollar, max_drawdown_pct, profit_factor, recovery_ratio, composite_score,
                   best_tp_profile, best_sl_profile, best_expiry, best_rules_json, flags_json, is_added_to_copier,
                   COALESCE(scan_id, 0), COALESCE(run_id, 0)
            FROM fleet_league
        ''')
        c.execute("DROP TABLE fleet_league")
        c.execute("ALTER TABLE fleet_league_v2 RENAME TO fleet_league")

    conn.commit()
    conn.close()


def save_cached_messages(channel_id: str, topic_id: int, messages: list, min_time: int, max_time: int, channel_title: str = ""):
    """Batch save scraped Telegram messages into the local SQLite cache."""
    conn = get_bt_db()
    c = conn.cursor()
    rows = []
    for m in messages:
        rows.append((
            str(channel_id),
            int(topic_id),
            int(m["msg_id"]),
            int(m["msg_time"]),
            int(m["reply_to_msg_id"]) if m.get("reply_to_msg_id") else 0,
            str(m.get("text") or ""),
            json.dumps(m.get("parsed")) if m.get("parsed") else "",
        ))
    if rows:
        c.executemany('''
            INSERT OR REPLACE INTO cached_messages
            (channel_id, topic_id, msg_id, msg_time, reply_to_msg_id, text, parsed_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', rows)

    c.execute('SELECT min_time, max_time FROM scrape_ranges WHERE channel_id = ? AND topic_id = ?', (str(channel_id), int(topic_id)))
    existing = c.fetchone()
    if existing:
        new_min = min(existing["min_time"], int(min_time))
        new_max = max(existing["max_time"], int(max_time))
    else:
        new_min = int(min_time)
        new_max = int(max_time)

    c.execute('''
        INSERT OR REPLACE INTO scrape_ranges
        (channel_id, topic_id, min_time, max_time, updated_at, channel_title)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (str(channel_id), int(topic_id), new_min, new_max, int(time.time()), str(channel_title)))

    conn.commit()
    conn.close()


def get_cached_messages(channel_id: str, topic_id: int, start_time: int, end_time: int):
    """Retrieve cached messages if available for the requested range."""
    conn = get_bt_db()
    c = conn.cursor()
    c.execute('''
        SELECT msg_id, msg_time, reply_to_msg_id, text, parsed_json
        FROM cached_messages
        WHERE channel_id = ? AND topic_id = ? AND msg_time >= ? AND msg_time <= ?
        ORDER BY msg_time ASC, msg_id ASC
    ''', (str(channel_id), int(topic_id), int(start_time), int(end_time)))
    rows = c.fetchall()
    conn.close()

    result = []
    for r in rows:
        parsed = None
        if r["parsed_json"]:
            try:
                parsed = json.loads(r["parsed_json"])
            except Exception:
                parsed = None
        result.append({
            "msg_id": r["msg_id"],
            "msg_time": r["msg_time"],
            "reply_to_msg_id": r["reply_to_msg_id"] if r["reply_to_msg_id"] else None,
            "text": r["text"],
            "parsed": parsed,
        })
    return result


def get_scrape_coverage(channel_id: str, topic_id: int):
    conn = get_bt_db()
    c = conn.cursor()
    c.execute('''
        SELECT min_time, max_time, updated_at, channel_title
        FROM scrape_ranges
        WHERE channel_id = ? AND topic_id = ?
    ''', (str(channel_id), int(topic_id)))
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None


def save_backtest_run(provider_name: str, channel_id: str, topic_id: int, start_time: int, end_time: int, rules: dict, summary: dict, graph: dict, signals: list, deals: list) -> int:
    conn = get_bt_db()
    c = conn.cursor()
    c.execute('''
        INSERT INTO backtest_runs
        (created_at, provider_name, channel_id, topic_id, start_time, end_time, rules_json, summary_json, graph_json, signals_json, deals_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        int(time.time()),
        str(provider_name),
        str(channel_id),
        int(topic_id),
        int(start_time),
        int(end_time),
        json.dumps(rules),
        json.dumps(summary),
        json.dumps(graph),
        json.dumps(signals),
        json.dumps(deals),
    ))
    run_id = c.lastrowid

    # Keep up to the 500 most recent runs in the Memory Log (supports large Fleet Scans)
    c.execute('''
        DELETE FROM backtest_runs
        WHERE run_id NOT IN (
            SELECT run_id FROM backtest_runs ORDER BY run_id DESC LIMIT 500
        )
    ''')
    conn.commit()
    conn.close()
    return run_id


def get_latest_backtest_run():
    conn = get_bt_db()
    c = conn.cursor()
    c.execute('SELECT * FROM backtest_runs ORDER BY run_id DESC LIMIT 1')
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    return _deserialize_run(row)


def get_backtest_run(run_id: int):
    conn = get_bt_db()
    c = conn.cursor()
    c.execute('SELECT * FROM backtest_runs WHERE run_id = ?', (int(run_id),))
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    return _deserialize_run(row)


def delete_backtest_run(run_id: int) -> bool:
    conn = get_bt_db()
    c = conn.cursor()
    c.execute('DELETE FROM backtest_runs WHERE run_id = ?', (int(run_id),))
    deleted = c.rowcount > 0
    conn.commit()
    conn.close()
    return deleted


def clear_all_backtest_runs() -> int:
    conn = get_bt_db()
    c = conn.cursor()
    c.execute('DELETE FROM backtest_runs')
    count = c.rowcount
    conn.commit()
    conn.close()
    return count


def list_recent_backtest_runs(limit: int = 50):
    conn = get_bt_db()
    c = conn.cursor()
    c.execute('''
        SELECT run_id, created_at, provider_name, channel_id, topic_id, start_time, end_time, rules_json, summary_json
        FROM backtest_runs
        WHERE run_id NOT IN (
            SELECT run_id FROM fleet_league WHERE run_id IS NOT NULL AND run_id > 0
        )
        ORDER BY run_id DESC LIMIT ?
    ''', (int(limit),))
    rows = c.fetchall()
    conn.close()
    items = []
    for r in rows:
        try:
            summary = json.loads(r["summary_json"] or "{}")
            rules = json.loads(r["rules_json"] or "{}")
        except Exception:
            summary = {}
            rules = {}
        items.append({
            "run_id": r["run_id"],
            "created_at": r["created_at"],
            "provider_name": r["provider_name"],
            "channel_id": r["channel_id"],
            "topic_id": r["topic_id"],
            "start_time": r["start_time"],
            "end_time": r["end_time"],
            "rules": rules,
            "summary": summary,
        })
    return items


def format_timescale_label(start_time: int, end_time: int) -> str:
    """Convert a unix timestamp range into a human-friendly timescale label (e.g. '1 Week', '1 Month', '1 Year')."""
    if not start_time or not end_time or end_time <= start_time:
        return "Custom Range"
    days = round((end_time - start_time) / 86400.0)
    if days <= 2:
        return f"{max(1, int(days))} Day" if days <= 1 else f"{int(days)} Days"
    if 5 <= days <= 9:
        return "1 Week"
    if 12 <= days <= 16:
        return "2 Weeks"
    if 25 <= days <= 35:
        return "1 Month"
    if 55 <= days <= 66:
        return "2 Months"
    if 80 <= days <= 100:
        return "3 Months"
    if 170 <= days <= 195:
        return "6 Months"
    if 350 <= days <= 380:
        return "1 Year"
    return f"{max(1, int(days))} Days"


def _deserialize_run(row):
    return {
        "run_id": row["run_id"],
        "created_at": row["created_at"],
        "provider_name": row["provider_name"],
        "channel_id": row["channel_id"],
        "topic_id": row["topic_id"],
        "start_time": row["start_time"],
        "end_time": row["end_time"],
        "rules": json.loads(row["rules_json"] or "{}"),
        "summary": json.loads(row["summary_json"] or "{}"),
        "graph": json.loads(row["graph_json"] or "{}"),
        "signals": json.loads(row["signals_json"] or "[]"),
        "deals": json.loads(row["deals_json"] or "[]"),
    }


def create_fleet_scan(total_dialogs: int, settings: dict) -> int:
    """Create a new fleet scan record and return scan_id."""
    conn = get_bt_db()
    c = conn.cursor()
    c.execute('''
        INSERT INTO fleet_scans (created_at, status, total_dialogs, probed_count, viable_count, completed_count, settings_json)
        VALUES (?, 'RUNNING', ?, 0, 0, 0, ?)
    ''', (int(time.time()), total_dialogs, json.dumps(settings or {})))
    scan_id = c.lastrowid
    conn.commit()
    conn.close()
    return scan_id


def update_fleet_scan_status(scan_id: int, status: str, probed: int, viable: int, completed: int, current_dialog: str = ""):
    """Update progress metrics of a fleet scan."""
    if not scan_id:
        return
    conn = get_bt_db()
    c = conn.cursor()
    completed_at = int(time.time()) if status in ("COMPLETED", "STOPPED", "FAILED") else 0
    c.execute('''
        UPDATE fleet_scans
        SET status = ?, probed_count = ?, viable_count = ?, completed_count = ?, current_dialog = ?,
            completed_at = CASE WHEN ? > 0 THEN ? ELSE completed_at END
        WHERE scan_id = ?
    ''', (status, probed, viable, completed, current_dialog, completed_at, completed_at, scan_id))
    conn.commit()
    conn.close()


def get_fleet_scans_history():
    """List all fleet scan sessions in chronological order (newest first) with timescale metadata."""
    conn = get_bt_db()
    c = conn.cursor()
    c.execute('''
        SELECT fs.*,
               (SELECT COUNT(*) FROM fleet_league fl WHERE fl.scan_id = fs.scan_id) AS league_count,
               (SELECT MAX(fl.net_profit) FROM fleet_league fl WHERE fl.scan_id = fs.scan_id) AS top_profit,
               (SELECT fl.channel_title FROM fleet_league fl WHERE fl.scan_id = fs.scan_id ORDER BY fl.composite_score DESC, fl.net_profit DESC LIMIT 1) AS top_provider
        FROM fleet_scans fs
        ORDER BY fs.scan_id DESC
    ''')
    rows = c.fetchall()
    conn.close()
    scans = []
    for r in rows:
        settings = json.loads(r["settings_json"] or "{}")
        start_ts = int(settings.get("start_time") or 0)
        end_ts = int(settings.get("end_time") or 0)
        timescale_label = format_timescale_label(start_ts, end_ts) if (start_ts and end_ts) else "1 Year"
        rules = settings.get("rules") or {}
        platform = str(rules.get("platform") or "mt5").lower()
        ranked_cnt = int(r["league_count"] or r["completed_count"] or 0)
        scans.append({
            "scan_id": r["scan_id"],
            "created_at": r["created_at"],
            "completed_at": r["completed_at"] if "completed_at" in r.keys() else 0,
            "status": r["status"],
            "total_dialogs": r["total_dialogs"],
            "probed_count": r["probed_count"],
            "viable_count": r["viable_count"],
            "completed_count": ranked_cnt,
            "current_dialog": r["current_dialog"],
            "settings": settings,
            "start_time": start_ts,
            "end_time": end_ts,
            "timescale_label": timescale_label,
            "platform": platform,
            "top_profit": float(r["top_profit"] or 0.0),
            "top_provider": str(r["top_provider"] or ""),
            "display_title": f"Fleet Scan, {timescale_label}",
        })
    return scans


def save_fleet_provider_result(data: dict):
    """Save or update a provider result in the fleet league table."""
    conn = get_bt_db()
    c = conn.cursor()
    c.execute('''
        INSERT OR REPLACE INTO fleet_league
        (channel_id, topic_id, channel_title, dialog_type, scanned_at, status,
         messages_scanned, signals_found, trades_count, win_rate, net_profit, gain_pct,
         max_drawdown_dollar, max_drawdown_pct, profit_factor, recovery_ratio, composite_score,
         best_tp_profile, best_sl_profile, best_expiry, best_rules_json, flags_json, is_added_to_copier,
         scan_id, run_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        str(data.get("channel_id")),
        int(data.get("topic_id") or 0),
        str(data.get("channel_title") or ""),
        str(data.get("dialog_type") or "channel"),
        int(data.get("scanned_at") or time.time()),
        str(data.get("status") or "COMPLETED"),
        int(data.get("messages_scanned") or 0),
        int(data.get("signals_found") or 0),
        int(data.get("trades_count") or 0),
        float(data.get("win_rate") or 0.0),
        float(data.get("net_profit") or 0.0),
        float(data.get("gain_pct") or 0.0),
        float(data.get("max_drawdown_dollar") or 0.0),
        float(data.get("max_drawdown_pct") or 0.0),
        float(data.get("profit_factor") or 0.0),
        float(data.get("recovery_ratio") or 0.0),
        float(data.get("composite_score") or 0.0),
        str(data.get("best_tp_profile") or ""),
        str(data.get("best_sl_profile") or ""),
        str(data.get("best_expiry") or ""),
        json.dumps(data.get("best_rules") or {}),
        json.dumps(data.get("flags") or []),
        int(data.get("is_added_to_copier") or 0),
        int(data.get("scan_id") or 0),
        int(data.get("run_id") or 0),
    ))
    conn.commit()
    conn.close()


def get_fleet_league_table(scan_id: int = None):
    """Retrieve ranked providers from fleet_league sorted by composite_score / net_profit.
    Strictly requires a valid scan_id > 0 so historical scans never bleed into a fresh scan.
    """
    if scan_id is None or int(scan_id) <= 0:
        return []
    conn = get_bt_db()
    c = conn.cursor()
    c.execute('''
        SELECT * FROM fleet_league
        WHERE scan_id = ?
        ORDER BY composite_score DESC, net_profit DESC, recovery_ratio DESC
    ''', (int(scan_id),))
    rows = c.fetchall()
    conn.close()
    items = []
    for r in rows:
        items.append({
            "channel_id": r["channel_id"],
            "topic_id": r["topic_id"],
            "channel_title": r["channel_title"],
            "dialog_type": r["dialog_type"],
            "scanned_at": r["scanned_at"],
            "status": r["status"],
            "messages_scanned": r["messages_scanned"],
            "signals_found": r["signals_found"],
            "trades_count": r["trades_count"],
            "win_rate": r["win_rate"],
            "net_profit": r["net_profit"],
            "gain_pct": r["gain_pct"],
            "max_drawdown_dollar": r["max_drawdown_dollar"],
            "max_drawdown_pct": r["max_drawdown_pct"],
            "profit_factor": r["profit_factor"],
            "recovery_ratio": r["recovery_ratio"],
            "composite_score": r["composite_score"],
            "best_tp_profile": r["best_tp_profile"],
            "best_sl_profile": r["best_sl_profile"],
            "best_expiry": r["best_expiry"],
            "best_rules": json.loads(r["best_rules_json"] or "{}"),
            "flags": json.loads(r["flags_json"] or "[]"),
            "is_added_to_copier": r["is_added_to_copier"],
            "scan_id": r["scan_id"] if "scan_id" in r.keys() else 0,
            "run_id": r["run_id"] if "run_id" in r.keys() else 0,
        })
    return items


def clear_fleet_league(scan_id: int = None):
    conn = get_bt_db()
    c = conn.cursor()
    if scan_id is not None:
        c.execute('DELETE FROM fleet_league WHERE scan_id = ?', (scan_id,))
        c.execute('DELETE FROM fleet_scans WHERE scan_id = ?', (scan_id,))
    else:
        c.execute('DELETE FROM fleet_league')
        c.execute('DELETE FROM fleet_scans')
    conn.commit()
    conn.close()


init_bt_db()
