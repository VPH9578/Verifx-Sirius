"""
trade_logger.py — Logs every trade to a JSON file for review.
"""

import json
import os
from datetime import datetime
import config


def log_trade(signal: dict, trade_results: list, raw_message: str):
    """
    Log a trade event to the JSON file.

    Args:
        signal: The parsed signal from Claude/Gemini
        trade_results: List of execution results from MT5/cTrader
        raw_message: The original Telegram message text
    """
    entry = {
        "timestamp": datetime.now().isoformat(),
        "raw_message": raw_message[:500],  # trim long messages
        "parsed_signal": signal,
        "executions": trade_results,
    }

    logs = _load_logs()
    logs.append(entry)
    _save_logs(logs)


def log_close(close_results: list, raw_message: str):
    """Log a close event."""
    entry = {
        "timestamp": datetime.now().isoformat(),
        "raw_message": raw_message[:500],
        "action": "CLOSE_ALL",
        "results": close_results,
    }

    logs = _load_logs()
    logs.append(entry)
    _save_logs(logs)


def log_signal_edit(message_id: int, provider_id: str, updated_signal: dict, mod_results: list, raw_message: str):
    """Log an edited signal event and its broker level modifications."""
    entry = {
        "timestamp": datetime.now().isoformat(),
        "action": "SIGNAL_EDIT",
        "message_id": int(message_id),
        "provider_id": str(provider_id),
        "updated_signal": updated_signal,
        "modifications": mod_results,
        "raw_message": (raw_message or "")[:500]
    }

    logs = _load_logs()
    logs.append(entry)
    _save_logs(logs)


def log_error(error_type: str, reason: str, provider_id: str = None, symbol: str = None, raw_message: str = "", details: dict = None):
    """
    Log an error or skipped trade event to JSON and SQLite.
    """
    entry = {
        "timestamp": datetime.now().isoformat(),
        "action": "ERROR",
        "error_type": str(error_type),
        "reason": str(reason),
        "provider_id": str(provider_id or ""),
        "symbol": str(symbol or ""),
        "raw_message": (raw_message or "")[:500],
        "details": details or {},
    }

    logs = _load_logs()
    logs.append(entry)
    _save_logs(logs)

    try:
        import db
        db.log_execution_error(error_type, reason, provider_id, symbol, raw_message)
    except Exception as e:
        print(f"[WARN] Failed to write error to SQLite DB: {e}")


def get_recent_errors(count: int = 20) -> list:
    """Return the last N error/skipped trade entries."""
    try:
        import db
        return db.get_recent_errors(count)
    except Exception:
        logs = _load_logs()
        errors = [l for l in logs if l.get("action") == "ERROR"]
        return errors[-count:] if errors else []


def get_recent_trades(count: int = 10) -> list:
    """Return the last N trade entries."""
    logs = _load_logs()
    return logs[-count:] if logs else []


def get_trade_count() -> int:
    """Return total number of logged trades."""
    return len(_load_logs())


def _load_logs() -> list:
    """Load existing logs from file."""
    if not os.path.exists(config.TRADE_LOG_FILE):
        return []
    try:
        with open(config.TRADE_LOG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except (json.JSONDecodeError, IOError):
        return []


def _save_logs(logs: list):
    """Save logs to file with atomic write."""
    tmp = config.TRADE_LOG_FILE + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(logs, f, indent=2, ensure_ascii=False)
        os.replace(tmp, config.TRADE_LOG_FILE)
    except IOError as e:
        print(f"[ERROR] Could not write trade log: {e}")
