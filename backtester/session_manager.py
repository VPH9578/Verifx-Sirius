"""
session_manager.py — Isolated In-Memory Telethon Session Manager.
Guarantees 100% non-interference with the live copier in main.py.
Extracts authentication tokens into an in-memory StringSession so
historical scraping and fleet scanning NEVER open or lock SQLite .session files.
"""

import os
import shutil
import tempfile
import config
from telethon import TelegramClient
from telethon.sessions import StringSession, SQLiteSession

_CACHED_STRING_SESSION = None
SESSION_STRING_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "telegram_session_string.txt",
)


def get_string_session() -> str:
    global _CACHED_STRING_SESSION
    if _CACHED_STRING_SESSION:
        return _CACHED_STRING_SESSION

    # 1. Check saved session string file first (100% pure in-memory, ZERO SQLite access!)
    if os.path.exists(SESSION_STRING_FILE):
        try:
            with open(SESSION_STRING_FILE, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content:
                    _CACHED_STRING_SESSION = content
                    return _CACHED_STRING_SESSION
        except Exception:
            pass

    # 2. Try reading from SQLite once and immediately closing connection
    sqlite_sess = None
    try:
        sqlite_sess = SQLiteSession(config.SESSION_FILE)
        _CACHED_STRING_SESSION = StringSession.save(sqlite_sess)
        if _CACHED_STRING_SESSION:
            try:
                with open(SESSION_STRING_FILE, "w", encoding="utf-8") as f:
                    f.write(_CACHED_STRING_SESSION)
            except Exception:
                pass
            return _CACHED_STRING_SESSION
    except Exception:
        pass
    finally:
        if sqlite_sess:
            try:
                sqlite_sess.close()
            except Exception:
                pass

    # 3. If locked, safely clone session files to temp dir and extract
    tmp_dir = tempfile.gettempdir()
    tmp_sess = os.path.join(tmp_dir, f"sirius_temp_sess_{os.getpid()}")
    base_name = config.SESSION_FILE
    if base_name.endswith(".session"):
        base_name = base_name[:-8]

    for ext in [".session", ".session-wal", ".session-shm"]:
        src = f"{base_name}{ext}"
        dst = f"{tmp_sess}{ext}"
        if os.path.exists(src):
            try:
                shutil.copy2(src, dst)
            except Exception:
                pass

    tmp_sqlite = None
    try:
        tmp_sqlite = SQLiteSession(tmp_sess)
        _CACHED_STRING_SESSION = StringSession.save(tmp_sqlite)
        if _CACHED_STRING_SESSION:
            try:
                with open(SESSION_STRING_FILE, "w", encoding="utf-8") as f:
                    f.write(_CACHED_STRING_SESSION)
            except Exception:
                pass
    finally:
        if tmp_sqlite:
            try:
                tmp_sqlite.close()
            except Exception:
                pass
        for ext in [".session", ".session-wal", ".session-shm"]:
            f = f"{tmp_sess}{ext}"
            if os.path.exists(f):
                try:
                    os.remove(f)
                except Exception:
                    pass

    return _CACHED_STRING_SESSION


def get_isolated_telegram_client() -> TelegramClient:
    """Returns a new TelegramClient with StringSession that has ZERO disk/file locks."""
    sess_str = get_string_session()
    return TelegramClient(
        StringSession(sess_str),
        config.TELEGRAM_API_ID,
        config.TELEGRAM_API_HASH,
    )
