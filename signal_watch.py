"""
signal_watch.py — Autonomous Observer & AI Investigator for Sirius Copier.

Passively observes every signal entering Sirius from Telegram providers across
all connected trading accounts (MT5, cTrader, MT4). Runs in a dedicated
non-blocking background daemon queue so it adds ZERO latency to live execution.

When a signal fails to reach any assigned, active account (or fails parsing/SL),
Signal Watch immediately:
  1. Collects Telegram context, Sirius execution diagnostics/config, broker
     terminal journal logs (MT5, MT4, cTrader), and relevant Sirius code context.
  2. Invokes Gemini AI to perform an Antigravity-style root cause investigation
     and research concrete solutions.
  3. Persists a structured report + ready-to-paste Antigravity prompt in stats.db.
  4. Sends an instant Telegram notification via MyClonerAlerts (alert.py).
"""

import asyncio
import glob
import json
import os
import queue
import random
import re
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import config
import db

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

TIMEOUT_GRACE_SECONDS = 12.0
TELEGRAM_ALERTS_ENABLED = False  # Reports stay on Dashboard -> Signal Watch; Telegram push buzz disabled.
PRE_EXEC_ISSUE_KINDS = ("MISSING_STOP_LOSS", "PARSER_MISSED", "SL_TYPO_UNFIXABLE")

_watch_queue: queue.Queue = queue.Queue(maxsize=250)
_worker_thread: Optional[threading.Thread] = None
_worker_lock = threading.Lock()
_ws_notifier = None  # Optional callable/coroutine hook registered by web_server


def set_ws_notifier(fn):
    """Registers a callback invoked whenever Signal Watch state updates."""
    global _ws_notifier
    _ws_notifier = fn


def _notify_dashboard():
    if _ws_notifier is None:
        return
    try:
        _ws_notifier()
    except Exception:
        pass


def start_observer():
    """Starts the dedicated Signal Watch daemon thread if not already running."""
    global _worker_thread
    with _worker_lock:
        if _worker_thread is not None and _worker_thread.is_alive():
            return _worker_thread
        _worker_thread = threading.Thread(
            target=_observer_loop,
            name="SignalWatchObserver",
            daemon=True,
        )
        _worker_thread.start()
        print("[SIGNAL-WATCH] Autonomous observer daemon started.")
        return _worker_thread


def _observer_loop():
    while True:
        try:
            event = _watch_queue.get()
            if event is None:
                break
            try:
                process_signal_event(event)
            except Exception as exc:
                print(f"[SIGNAL-WATCH] Error processing watch event: {exc}")
            finally:
                _watch_queue.task_done()
        except Exception as e:
            print(f"[SIGNAL-WATCH] Observer loop error: {e}")
            time.sleep(1)


def enqueue_signal_event(
    provider_id: str,
    provider_name: str,
    channel_id: Any,
    topic_id: int,
    message_id: int,
    raw_message: str,
    parsed_signal: Optional[dict] = None,
    exec_results: Optional[list] = None,
    account_diagnostics: Optional[dict] = None,
    event_kind: str = "NEW_SIGNAL",
    force_issue_reason: str = "",
) -> bool:
    """
    Non-blocking enqueue into the Signal Watch observer daemon.
    Never blocks live trade execution.
    """
    start_observer()
    payload = {
        "provider_id": str(provider_id),
        "provider_name": str(provider_name or f"Provider {provider_id}"),
        "channel_id": str(channel_id or ""),
        "topic_id": int(topic_id or 0),
        "message_id": int(message_id or 0),
        "raw_message": str(raw_message or ""),
        "parsed_signal": dict(parsed_signal) if isinstance(parsed_signal, dict) else {},
        "exec_results": list(exec_results) if isinstance(exec_results, list) else [],
        "account_diagnostics": dict(account_diagnostics) if isinstance(account_diagnostics, dict) else {},
        "event_kind": str(event_kind or "NEW_SIGNAL"),
        "force_issue_reason": str(force_issue_reason or ""),
        "timestamp": int(time.time()),
    }
    try:
        _watch_queue.put_nowait(payload)
        return True
    except queue.Full:
        print("[SIGNAL-WATCH] Observer queue full; dropping oldest check or skipping.")
        return False


def looks_like_unparsed_signal(text: str) -> bool:
    """
    Heuristic filter to spot probable trade signals that `parse_signal()` missed,
    without triggering false alarms on casual provider chatter.
    """
    if not text or len(text.strip()) < 8:
        return False
    low = text.lower()
    # Must contain a directional / trade action cue AND either SL/TP cue or multiple numeric levels
    has_dir = bool(re.search(r"\b(buy|sell|long|short)\b", low))
    has_risk_word = bool(re.search(r"\b(sl|stop\s*loss|tp\s*1?|take\s*profit|entry|zone)\b", low))
    nums = re.findall(r"\b\d+(?:\.\d+)?\b", low)
    if has_dir and has_risk_word and len(nums) >= 2:
        return True
    if has_dir and len(nums) >= 3 and any(sym in low for sym in ("gold", "xau", "btc", "eth", "sol", "us30", "nas", "eur", "gbp", "jpy")):
        return True
    return False


def _read_text_auto_encoding(path: str, max_bytes: int = 262144) -> str:
    """Reads log tail handling UTF-16LE (MT4/MT5 native logs) and UTF-8 seamlessly."""
    if not os.path.exists(path):
        return ""
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as f:
            if size > max_bytes:
                f.seek(size - max_bytes)
            raw = f.read()
        if raw.startswith(b"\xff\xfe") or b"\x00" in raw[:200]:
            text = raw.decode("utf-16-le", errors="replace")
        else:
            text = raw.decode("utf-8", errors="replace")
        return text.lstrip("\ufeff")
    except Exception as e:
        return f"[Error reading {path}: {e}]"


def _filter_log_lines(
    text: str,
    keywords: List[str],
    tail_lines: int = 40,
    max_matches: int = 45,
) -> List[str]:
    if not text:
        return []
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return []
    kw_lower = [k.lower() for k in keywords if k]
    matched = []
    for ln in lines:
        low = ln.lower()
        if any(k in low for k in kw_lower) or any(w in low for w in ("error", "fail", "reject", "invalid", "retcode", "blocked", "sirius", "timeout", "off quotes", "slippage", "deviation", "too far")):
            matched.append(ln)
    if matched:
        return matched[-max_matches:]
    return lines[-tail_lines:]


def _find_metaquotes_terminal_dir(mt_path: str = "", platform: str = "mt5", metaquotes_root: str = None) -> Optional[str]:
    """Maps an MT5/MT4 executable path to its `%APPDATA%/MetaQuotes/Terminal/<HASH>` data folder."""
    root = metaquotes_root or os.path.expandvars(r"%APPDATA%\MetaQuotes\Terminal")
    if not os.path.isdir(root):
        return None
    target_norm = ""
    if mt_path:
        norm = os.path.normpath(mt_path).lower()
        if norm.endswith(".exe"):
            norm = os.path.dirname(norm)
        target_norm = norm

    candidates = []
    with os.scandir(root) as it:
        for entry in it:
            if not entry.is_dir():
                continue
            origin_file = os.path.join(entry.path, "origin.txt")
            if os.path.exists(origin_file):
                origin_txt = _read_text_auto_encoding(origin_file, max_bytes=4096).strip()
                origin_norm = os.path.normpath(origin_txt).lower() if origin_txt else ""
                if target_norm and origin_norm == target_norm:
                    return entry.path
            if platform == "mt4" and os.path.isdir(os.path.join(entry.path, "MQL4")):
                candidates.append(entry.path)
            elif platform == "mt5" and os.path.isdir(os.path.join(entry.path, "MQL5")):
                candidates.append(entry.path)

    # Pick most recently modified today log among candidates if exact origin didn't match (e.g. MT4 without path in config)
    best_path = None
    best_mtime = -1.0
    for cpath in candidates:
        for sub in ("logs", "MQL4\\Logs", "MQL5\\Logs"):
            sub_dir = os.path.join(cpath, sub)
            if os.path.isdir(sub_dir):
                for fn in os.listdir(sub_dir):
                    if fn.endswith(".log"):
                        fp = os.path.join(sub_dir, fn)
                        try:
                            mt = os.path.getmtime(fp)
                            if mt > best_mtime:
                                best_mtime = mt
                                best_path = cpath
                        except Exception:
                            pass
    return best_path


def read_mt5_journal_logs(
    acc_cfg: dict,
    target_ts: int,
    keywords: List[str],
    metaquotes_root: str = None,
) -> Dict[str, Any]:
    """Reads MT5 terminal system & MQL5 Expert logs for today's date around target_ts."""
    term_dir = _find_metaquotes_terminal_dir(acc_cfg.get("mt5_path", ""), platform="mt5", metaquotes_root=metaquotes_root)
    day_str = datetime.fromtimestamp(target_ts or time.time()).strftime("%Y%m%d")
    out = {"terminal_dir": term_dir or "", "journal_lines": [], "mql5_lines": []}
    if not term_dir:
        return out

    sys_log = os.path.join(term_dir, "logs", f"{day_str}.log")
    if os.path.exists(sys_log):
        out["journal_lines"] = _filter_log_lines(_read_text_auto_encoding(sys_log), keywords)

    ea_log = os.path.join(term_dir, "MQL5", "Logs", f"{day_str}.log")
    if os.path.exists(ea_log):
        out["mql5_lines"] = _filter_log_lines(_read_text_auto_encoding(ea_log), keywords)
    return out


def read_mt4_journal_logs(
    acc_cfg: dict,
    target_ts: int,
    keywords: List[str],
    metaquotes_root: str = None,
    workspace_dir: str = None,
) -> Dict[str, Any]:
    """Reads MT4 bridge debug log + MT4 terminal journal & MQL4/Logs."""
    w_dir = workspace_dir or BASE_DIR
    debug_log = os.path.join(w_dir, "mt4_debug.log")
    out = {"terminal_dir": "", "mt4_debug_lines": [], "journal_lines": [], "mql4_lines": []}
    if os.path.exists(debug_log):
        out["mt4_debug_lines"] = _filter_log_lines(_read_text_auto_encoding(debug_log, max_bytes=131072), keywords, tail_lines=30, max_matches=35)

    term_dir = _find_metaquotes_terminal_dir(acc_cfg.get("mt4_path", ""), platform="mt4", metaquotes_root=metaquotes_root)
    if term_dir:
        out["terminal_dir"] = term_dir
        day_str = datetime.fromtimestamp(target_ts or time.time()).strftime("%Y%m%d")
        sys_log = os.path.join(term_dir, "logs", f"{day_str}.log")
        if os.path.exists(sys_log):
            out["journal_lines"] = _filter_log_lines(_read_text_auto_encoding(sys_log), keywords)
        ea_log = os.path.join(term_dir, "MQL4", "Logs", f"{day_str}.log")
        if os.path.exists(ea_log):
            out["mql4_lines"] = _filter_log_lines(_read_text_auto_encoding(ea_log), keywords)
    return out


def read_ctrader_journal_logs(
    acc_cfg: dict,
    keywords: List[str],
    spotware_root: str = None,
) -> Dict[str, Any]:
    """Reads latest Spotware cTrader desktop/bridge log tail."""
    root = spotware_root or os.path.expandvars(r"%LOCALAPPDATA%\Spotware\cTrader")
    out = {"log_file": "", "lines": []}
    if not os.path.isdir(root):
        return out
    try:
        candidates = glob.glob(os.path.join(root, "**", "*.log"), recursive=True)
        if not candidates:
            return out
        candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        latest = candidates[0]
        out["log_file"] = latest
        out["lines"] = _filter_log_lines(_read_text_auto_encoding(latest, max_bytes=131072), keywords, tail_lines=25, max_matches=30)
    except Exception as e:
        out["lines"] = [f"[Error reading cTrader logs: {e}]"]
    return out


def fetch_recent_telegram_messages(channel_id: str, topic_id: int = 0, limit: int = 5) -> List[dict]:
    """Safely fetches the last few messages from the provider channel/topic via active Telethon loop."""
    if not channel_id:
        return []
    try:
        import signal_listener
        client = getattr(signal_listener, "telegram_client", None)
        loop = getattr(signal_listener, "telegram_loop", None)
        if not client or not loop or not loop.is_running():
            return []

        async def _pull():
            cid = int(channel_id) if str(channel_id).lstrip("-").isdigit() else channel_id
            msgs = await client.get_messages(cid, limit=limit, reply_to=int(topic_id) if topic_id else None)
            res = []
            for m in msgs:
                res.append({
                    "id": getattr(m, "id", 0),
                    "date": m.date.isoformat() if getattr(m, "date", None) else "",
                    "text": (getattr(m, "text", None) or "")[:400],
                    "edit_date": m.edit_date.isoformat() if getattr(m, "edit_date", None) else "",
                })
            return res

        fut = asyncio.run_coroutine_threadsafe(_pull(), loop)
        return fut.result(timeout=2.5) or []
    except Exception:
        return []


def collect_relevant_code_context(
    failure_reasons: List[str],
    platforms: List[str],
    broker_journals: Optional[dict] = None,
) -> str:
    """Extracts concise Sirius code snippets ONLY when concrete error signatures exist in diagnostics or broker logs."""
    journal_blobs = []
    for j in (broker_journals or {}).values():
        if isinstance(j, dict):
            for k, v in j.items():
                if k.endswith("_lines") or k == "lines":
                    journal_blobs.extend(v or [])
    joined = (" ".join(failure_reasons) + " " + " ".join(journal_blobs)).lower()
    snippets = []

    def _grab(rel_path: str, pattern: str, before: int = 2, after: int = 12):
        full = os.path.join(BASE_DIR, rel_path)
        if not os.path.exists(full):
            return
        try:
            with open(full, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
            for idx, ln in enumerate(lines):
                if re.search(pattern, ln, re.I):
                    s = max(0, idx - before)
                    e = min(len(lines), idx + after)
                    block = "".join(f"{i+1}: {lines[i]}" for i in range(s, e))
                    snippets.append(f"--- {rel_path} (L{s+1}-L{e}) ---\n{block.strip()}")
                    break
        except Exception:
            pass

    if "blocked_risk_guard" in joined or "budget" in joined or "exceeds allocated" in joined:
        _grab("mt5_executor.py", r"Rule 2: Pre-flight Real Dollar Risk", before=2, after=18)
        if "ctrader" in platforms:
            _grab("ctrader_worker.py", r"Rule 2: Pre-flight Real Dollar Risk", before=2, after=14)
        if "mt4" in platforms:
            _grab("mt4_worker.py", r"Rule 2: Pre-flight Real Dollar Risk", before=2, after=14)

    if any(tok in joined for tok in ("deviation", "too far", "slippage", "10004", "off quotes", "price_too_far")):
        _grab("config.py", r"DEVIATION\s*=", before=1, after=8)
        if "ctrader" in platforms:
            _grab("ctrader_worker.py", r"place_order\(", before=2, after=12)

    if "symbol_not_found" in joined or "resolve_symbol" in joined or "wrong symbol" in joined:
        _grab("symbol_mapper.py", r"def resolve_symbol", before=1, after=18)

    if any(tok in joined for tok in ("parser_missed", "sl_typo", "missing_stop_loss", "blocked_price_mismatch", "10016")):
        _grab("signal_parser.py", r"def parse_with_regex", before=1, after=16)

    return "\n\n".join(snippets[:4])


def collect_investigation_evidence(event: dict, account_statuses: dict, expected_accounts: list) -> dict:
    """Gathers multi-source evidence: Telegram, Sirius config/errors, Broker journals, and code pointers."""
    prov_id = str(event.get("provider_id") or "")
    msg_id = int(event.get("message_id") or 0)
    parsed = event.get("parsed_signal") or {}
    raw_sym = str(parsed.get("instrument") or parsed.get("symbol") or "")
    target_ts = int(event.get("timestamp") or time.time())

    from account_manager import AccountManager
    acc_mgr = AccountManager.get_instance()
    acc_mgr.load_accounts()

    keywords = [k for k in (raw_sym, f"P{prov_id}", str(msg_id) if msg_id else "", prov_id) if k]

    # 1. Telegram context
    recent_tg = fetch_recent_telegram_messages(
        event.get("channel_id", ""),
        topic_id=int(event.get("topic_id") or 0),
        limit=5,
    )

    # 2. Sirius errors & SymbolMapper checks + account states
    recent_db_errors = db.get_recent_errors(limit=15, provider_id=prov_id or None)
    account_snapshots = {}
    broker_journals = {}
    failure_reasons = []
    failed_platforms = []

    for exp in expected_accounts:
        aid = exp["account_id"]
        plat = exp["platform"]
        st = account_statuses.get(aid, {})
        if st.get("status") not in ("COPIED", "PAUSED", "TRADING_DISABLED", "ALREADY_EXECUTED"):
            failure_reasons.append(f"{aid}: {st.get('status')} - {st.get('reason')}")
            failed_platforms.append(plat)

        cfg = acc_mgr.accounts_config.get(aid, {})
        resolved_sym = None
        if raw_sym:
            try:
                from symbol_mapper import SymbolMapper
                resolved_sym = SymbolMapper.resolve_symbol(aid, raw_sym)
            except Exception:
                pass

        acc_state = {}
        try:
            if aid == "acc_1" and plat == "mt5":
                import mt5_executor
                info = mt5_executor.get_account_info()
                acc_state = {
                    "connected": mt5_executor.is_connected(),
                    "balance": getattr(info, "balance", None) if info else None,
                    "equity": getattr(info, "equity", None) if info else None,
                }
            else:
                acc_state = acc_mgr.cached_states.get(aid, {}) or {}
        except Exception:
            pass

        account_snapshots[aid] = {
            "name": exp.get("name"),
            "platform": plat,
            "login": exp.get("login"),
            "resolved_symbol": resolved_sym,
            "provider_settings": {
                k: exp.get("settings", {}).get(k)
                for k in ("risk_mode", "risk_value", "max_lot", "entry_zone_mode", "spread_offset_entry", "spread_offset_tp", "spread_offset_sl", "trading_enabled")
            },
            "runtime_state": {
                k: acc_state.get(k) for k in ("connected", "balance", "equity") if k in acc_state
            },
            "status_record": st,
        }

        # 3. Broker journal logs for failing accounts (or all if global parser/SL failure)
        is_failed_acc = st.get("status") not in ("COPIED", "PAUSED", "TRADING_DISABLED", "ALREADY_EXECUTED")
        if is_failed_acc or event.get("event_kind") in PRE_EXEC_ISSUE_KINDS:
            if plat == "mt5":
                broker_journals[aid] = read_mt5_journal_logs(cfg, target_ts, keywords)
            elif plat == "mt4":
                broker_journals[aid] = read_mt4_journal_logs(cfg, target_ts, keywords)
            elif plat == "ctrader":
                broker_journals[aid] = read_ctrader_journal_logs(cfg, keywords)

        # Include ground-truth tickets from trade_state_<acc>.json so AI sees actual order state
        try:
            saved_tickets = acc_mgr.read_account_tickets_for_message(aid, msg_id)
        except Exception:
            saved_tickets = []
        account_snapshots[aid]["trade_state_tickets"] = saved_tickets

    if event.get("force_issue_reason"):
        failure_reasons.append(event["force_issue_reason"])

    code_snippets = collect_relevant_code_context(failure_reasons, failed_platforms, broker_journals=broker_journals)

    return {
        "telegram": {
            "message_id": msg_id,
            "channel_id": event.get("channel_id"),
            "topic_id": event.get("topic_id"),
            "raw_message": event.get("raw_message"),
            "parsed_signal": parsed,
            "recent_topic_messages": recent_tg,
        },
        "sirius": {
            "event_kind": event.get("event_kind"),
            "provider_id": prov_id,
            "provider_name": event.get("provider_name"),
            "account_snapshots": account_snapshots,
            "recent_db_errors": recent_db_errors[:8],
            "deviation_setting": getattr(config, "DEVIATION", 20),
            "market_order_threshold_points": getattr(config, "MARKET_ORDER_THRESHOLD", 50),
        },
        "broker_journals": broker_journals,
        "code_context": code_snippets,
    }


def _parse_gemini_json(raw_text: str) -> Optional[dict]:
    """Parses Gemini JSON output, tolerating markdown code fences or single-item lists."""
    txt = (raw_text or "").strip()
    if not txt:
        return None
    if txt.startswith("```"):
        txt = re.sub(r"^```(?:json)?\s*", "", txt, flags=re.I)
        txt = re.sub(r"\s*```$", "", txt).strip()
    parsed = json.loads(txt)
    if isinstance(parsed, list) and parsed and isinstance(parsed[0], dict):
        parsed = parsed[0]
    return parsed if isinstance(parsed, dict) else None


def _call_gemini_investigator(prompt_text: str) -> Optional[dict]:
    """Calls Gemini using configured API keys (google.genai first, httpx fallback)."""
    keys = list(getattr(config, "GEMINI_API_KEYS", []) or [])
    env_key = os.getenv("GEMINI_API_KEY")
    if env_key and env_key not in keys:
        keys.insert(0, env_key)
    if not keys:
        return None

    model_name = getattr(config, "GEMINI_MODEL", "gemini-2.5-flash")
    random.shuffle(keys)

    for api_key in keys[:5]:
        # Primary path: official google-genai SDK
        try:
            from google import genai
            from google.genai import types
            client = genai.Client(api_key=api_key)
            resp = client.models.generate_content(
                model=model_name,
                contents=prompt_text,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    temperature=0.2,
                ),
            )
            parsed = _parse_gemini_json(getattr(resp, "text", "") or "")
            if parsed:
                return parsed
        except Exception:
            pass

        # Fallback path: direct REST via httpx
        try:
            import httpx
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"
            body = {
                "contents": [{"parts": [{"text": prompt_text}]}],
                "generationConfig": {
                    "responseMimeType": "application/json",
                    "temperature": 0.2,
                },
            }
            r = httpx.post(url, json=body, timeout=25.0)
            if r.status_code == 200:
                data = r.json()
                txt = data["candidates"][0]["content"]["parts"][0]["text"]
                parsed = _parse_gemini_json(txt)
                if parsed:
                    return parsed
        except Exception:
            continue
    return None


def _build_deterministic_fallback_report(event: dict, evidence: dict, account_statuses: dict) -> dict:
    """Produces an accurate, structured report & Antigravity prompt even if Gemini API is offline."""
    prov_id = event.get("provider_id")
    prov_name = event.get("provider_name")
    msg_id = event.get("message_id")
    parsed = event.get("parsed_signal") or {}
    sym = parsed.get("instrument") or "Unknown"
    direction = parsed.get("direction") or event.get("event_kind")

    failed_lines = []
    for aid, st in account_statuses.items():
        if st.get("status") not in ("COPIED", "PAUSED", "TRADING_DISABLED", "ALREADY_EXECUTED"):
            failed_lines.append(f"- **{aid} ({st.get('account_name', aid)} / {st.get('platform', '').upper()})**: `{st.get('status')}` — {st.get('reason')}")

    short_summary = "; ".join(
        f"{aid}: {st.get('status')} ({st.get('reason')})"
        for aid, st in account_statuses.items()
        if st.get("status") not in ("COPIED", "PAUSED", "TRADING_DISABLED", "ALREADY_EXECUTED")
    ) or event.get("force_issue_reason") or "Signal failed to execute on one or more expected accounts."

    report_md = (
        f"## Signal Watch Auto-Investigation (Fallback Mode)\n\n"
        f"- **Provider:** {prov_name} (`P{prov_id}`)\n"
        f"- **Telegram Message ID:** `{msg_id}`\n"
        f"- **Symbol / Action:** `{sym}` `{direction}`\n\n"
        f"### Failed Accounts\n" + ("\n".join(failed_lines) if failed_lines else f"- {short_summary}") + "\n\n"
        f"### Raw Telegram Signal\n```\n{event.get('raw_message', '')}\n```\n\n"
        f"### Next Steps\nReview the collected Sirius diagnostics and broker journal logs in the Evidence panel and paste the Antigravity prompt below to apply and sandbox-test a targeted fix."
    )

    antigravity_prompt = (
        f"Investigate and fix why Signal Watch flagged Telegram message {msg_id} from Provider {prov_id} ({prov_name}) "
        f"for {sym} {direction}.\n\n"
        f"Observed Failure:\n{chr(10).join(failed_lines) if failed_lines else short_summary}\n\n"
        f"Raw Signal:\n{event.get('raw_message', '')}\n\n"
        f"Please check the relevant Sirius execution/parser code and broker logs, implement the root-cause fix, and verify in tests_sandbox/."
    )
    has_unconfirmed_timeout = any(
        st.get("status") in ("TIMEOUT", "NOT_REACHED")
        for st in account_statuses.values()
    )
    return {
        "short_summary": short_summary[:300],
        "report_markdown": report_md,
        "antigravity_prompt": antigravity_prompt,
        "confidence": "medium" if has_unconfirmed_timeout else "high",
    }


def _normalize_confidence(val: Any, default: str = "medium") -> str:
    s = str(val or "").strip().lower()
    if s in ("high", "medium", "low"):
        return s
    return default


def run_ai_investigation(event: dict, evidence: dict, account_statuses: dict, ai_caller=None) -> dict:
    """Runs Gemini investigation (or injected ai_caller in sandbox tests) over multi-source evidence."""
    prompt = (
        "You are an expert algorithmic trading systems engineer debugging Sirius TSC (Telegram Signal Copier).\n"
        "A signal from Telegram failed to reach one or more connected trading accounts.\n"
        "Analyze ALL evidence below (Telegram raw signal + recent topic messages, Sirius parsed JSON + account/provider configs + execution diagnostics + ground-truth trade_state_tickets + DB errors, broker MT5/MT4/cTrader journal logs, and Sirius code context).\n\n"
        "STRICT EVIDENCE & ANTI-HALLUCINATION RULES:\n"
        "- Only claim a root cause that is DIRECTLY supported by an explicit log line, diagnostic field, parsed_signal value, or quote in the evidence.\n"
        "- Quote the exact log line or diagnostic string for each claim.\n"
        "- NEVER invent unverified causes (such as magic number mismatches or symbol errors) unless explicitly shown in an error/log line.\n"
        "- NEVER say an account failed if its `tickets` list or `trade_state_tickets` is non-empty or its status is COPIED / ALREADY_EXECUTED.\n"
        "- If logs are empty or do not show why a worker timed out, state clearly that the cause is unconfirmed from logs, mark any theories as hypotheses, and set `confidence` to \"low\".\n\n"
        "Return a valid JSON object with EXACTLY these 4 string keys:\n"
        "1. \"short_summary\": 1-2 crisp sentences explaining the evidenced root cause and affected accounts.\n"
        "2. \"report_markdown\": A thorough Markdown report with sections:\n"
        "   - ## Incident Overview\n"
        "   - ## Multi-Source Evidence (quote exact Telegram/Sirius/Journal lines)\n"
        "   - ## Root Cause Analysis (distinguish confirmed cause vs hypothesis)\n"
        "   - ## Recommended Fix & Configuration Changes\n"
        "3. \"antigravity_prompt\": A self-contained, actionable copy-paste prompt for Antigravity coding agent describing the exact failure, affected file paths/parameters, proposed fix, and asking to implement and sandbox-test it.\n"
        "4. \"confidence\": One of \"high\", \"medium\", or \"low\". Use \"high\" when logs/diagnostics directly prove the cause, \"medium\" when partial evidence exists, \"low\" when logs lack concrete error proof.\n\n"
        f"ACCOUNT EXECUTION MATRIX:\n{json.dumps(account_statuses, indent=2, ensure_ascii=False)}\n\n"
        f"COLLECTED EVIDENCE:\n{json.dumps(evidence, indent=2, ensure_ascii=False)[:24000]}"
    )

    caller = ai_caller or _call_gemini_investigator
    res = None
    try:
        res = caller(prompt)
    except Exception as e:
        print(f"[SIGNAL-WATCH] AI caller error: {e}")

    if isinstance(res, dict) and res.get("report_markdown"):
        return {
            "short_summary": str(res.get("short_summary") or "")[:400],
            "report_markdown": str(res.get("report_markdown") or ""),
            "antigravity_prompt": str(res.get("antigravity_prompt") or ""),
            "confidence": _normalize_confidence(res.get("confidence"), default="medium"),
        }
    return _build_deterministic_fallback_report(event, evidence, account_statuses)


def send_watch_telegram_alert(event: dict, watch_id: int, account_statuses: dict, ai_summary: str) -> bool:
    """Sends a concise Signal Watch incident notification via MyClonerAlerts (disabled when TELEGRAM_ALERTS_ENABLED=False)."""
    if not TELEGRAM_ALERTS_ENABLED:
        return False
    try:
        import alert
        prov_id = event.get("provider_id")
        prov_name = event.get("provider_name")
        parsed = event.get("parsed_signal") or {}
        sym = parsed.get("instrument") or "UNKNOWN"
        direction = parsed.get("direction") or event.get("event_kind")
        msg_id = event.get("message_id")

        lines = [
            f"🚨 Sirius Signal Watch Alert (#{watch_id})",
            f"Provider: {prov_name} (P{prov_id})",
            f"Signal: {sym} {direction} | Msg ID: {msg_id}",
            "",
            "Account Status:",
        ]
        for aid, st in account_statuses.items():
            icon = "✅" if st.get("status") == "COPIED" else ("⏸️" if st.get("status") in ("PAUSED", "TRADING_DISABLED") else "❌")
            lines.append(f"  {icon} {st.get('account_name', aid)} ({st.get('platform', '').upper()}): {st.get('status')}")
            if st.get("status") not in ("COPIED", "PAUSED", "TRADING_DISABLED") and st.get("reason"):
                lines.append(f"     Reason: {st['reason'][:160]}")

        lines.extend([
            "",
            f"AI Diagnosis: {ai_summary[:600]}",
            "",
            "Open Dashboard -> Signal Watch to copy the full Antigravity fix prompt.",
        ])
        msg_text = "\n".join(lines)[:3800]
        alert.send_telegram_alert(msg_text)
        return True
    except Exception as e:
        print(f"[SIGNAL-WATCH] Telegram alert error: {e}")
        return False


def _summarize_statuses(account_statuses: dict, event_kind: str) -> tuple:
    """Computes (copied_count, failed_count, paused_or_disabled_count, has_discrepancy, overall_status)."""
    copied_count = 0
    failed_count = 0
    paused_or_disabled_count = 0
    for st in account_statuses.values():
        s = st.get("status")
        if s in ("COPIED", "ALREADY_EXECUTED"):
            copied_count += 1
        elif s in ("PAUSED", "TRADING_DISABLED"):
            paused_or_disabled_count += 1
        else:
            failed_count += 1

    has_discrepancy = failed_count > 0 or event_kind in PRE_EXEC_ISSUE_KINDS
    if event_kind in PRE_EXEC_ISSUE_KINDS:
        overall_status = "FAILED"
    elif failed_count == 0 and copied_count > 0:
        overall_status = "COPIED"
    elif failed_count > 0 and copied_count > 0:
        overall_status = "PARTIAL_FAIL"
    elif failed_count > 0:
        overall_status = "FAILED"
    else:
        overall_status = "SKIPPED"
    return copied_count, failed_count, paused_or_disabled_count, has_discrepancy, overall_status


def _recheck_timed_out_accounts(
    acc_mgr,
    account_statuses: dict,
    pending_accs: list,
    msg_id: int,
    grace_seconds: float,
):
    """
    Waits up to `grace_seconds` checking late worker replies and trade_state_<acc>.json
    so slow cTrader/broker executions aren't misreported as TIMEOUT failures.
    """
    remaining = set(pending_accs)
    deadline = time.time() + max(0.0, grace_seconds)
    while remaining:
        for aid in list(remaining):
            st_rec = account_statuses.get(aid, {})
            req_id = str((st_rec.get("extra") or {}).get("req_id") or "")
            res = acc_mgr.recheck_late_dispatch(aid, req_id=req_id, message_id=msg_id)
            if res:
                if res.get("confirmed"):
                    st_rec["status"] = res.get("status") or "COPIED"
                    st_rec["reason"] = res.get("reason") or "Confirmed late execution."
                    if res.get("tickets"):
                        st_rec["tickets"] = res["tickets"]
                    st_rec.setdefault("extra", {})["confirmed_late"] = True
                    st_rec["extra"]["confirm_source"] = res.get("source", "")
                else:
                    # Worker returned an explicit non-timeout failure late
                    st_rec["status"] = res.get("status") or "FAILED"
                    st_rec["reason"] = res.get("reason") or st_rec.get("reason", "")
                remaining.discard(aid)
        if not remaining or time.time() >= deadline:
            break
        time.sleep(min(0.35, max(0.05, deadline - time.time())))


def process_signal_event(
    event: dict,
    ai_caller=None,
    alert_sender=None,
    skip_journals: bool = False,
) -> dict:
    """
    Core Signal Watch pipeline (invoked asynchronously by observer thread, or
    directly by sandbox tests).
    """
    from account_manager import AccountManager
    acc_mgr = AccountManager.get_instance()

    prov_id = str(event.get("provider_id") or "")
    prov_name = str(event.get("provider_name") or f"Provider {prov_id}")
    msg_id = int(event.get("message_id") or 0)
    event_kind = str(event.get("event_kind") or "NEW_SIGNAL")
    parsed = event.get("parsed_signal") or {}
    exec_results = event.get("exec_results") or []
    account_diagnostics = event.get("account_diagnostics") or {}

    expected_accounts = acc_mgr.get_expected_accounts_for_provider(prov_id)
    results_by_acc: Dict[str, list] = {}
    for r in exec_results:
        if isinstance(r, dict):
            aid = str(r.get("account_id") or "acc_1")
            results_by_acc.setdefault(aid, []).append(r)

    account_statuses: Dict[str, dict] = {}
    for exp in expected_accounts:
        aid = exp["account_id"]
        plat = exp["platform"]
        name = exp["name"]
        placed = results_by_acc.get(aid, [])
        diag = account_diagnostics.get(aid) or {}

        if event_kind in PRE_EXEC_ISSUE_KINDS:
            st = event_kind
            reason = event.get("force_issue_reason") or f"Signal failed before execution ({event_kind})."
        elif placed:
            st = "COPIED"
            reason = diag.get("reason") or f"Executed {len(placed)} order(s)."
        elif exp.get("is_paused") or diag.get("status") == "PAUSED":
            st = "PAUSED"
            reason = diag.get("reason") or f"Account {name} is paused."
        elif not exp.get("trading_enabled", True) or diag.get("status") == "TRADING_DISABLED":
            st = "TRADING_DISABLED"
            reason = diag.get("reason") or f"Provider P{prov_id} trading disabled on {name}."
        elif diag.get("status") == "ALREADY_EXECUTED":
            st = "ALREADY_EXECUTED"
            reason = diag.get("reason") or "Already executed on this account."
        else:
            st = diag.get("status") or "NOT_REACHED"
            reason = diag.get("reason") or f"No orders placed on {name} and no execution confirmation received."

        account_statuses[aid] = {
            "account_id": aid,
            "account_name": name,
            "platform": plat,
            "login": exp.get("login"),
            "status": st,
            "reason": reason,
            "tickets": [r.get("ticket") for r in placed if r.get("ticket")],
            "extra": dict(diag.get("extra", {})),
        }

    sym = str(parsed.get("instrument") or parsed.get("symbol") or "")
    direction = str(parsed.get("direction") or "")
    now_ts = int(event.get("timestamp") or time.time())
    event_key = f"{prov_id}_{msg_id}_{event_kind}" if msg_id else f"{prov_id}_0_{now_ts}"
    exp_summary = [
        {"account_id": e["account_id"], "name": e["name"], "platform": e["platform"], "login": e["login"]}
        for e in expected_accounts
    ]

    # E1: Before declaring TIMEOUT / NOT_REACHED a failure, wait briefly and recheck late replies + trade_state
    pending_accs = [
        aid for aid, st_rec in account_statuses.items()
        if st_rec.get("status") in ("TIMEOUT", "NOT_REACHED") and event_kind not in PRE_EXEC_ISSUE_KINDS
    ]
    grace_val = event.get("grace_seconds")
    grace_seconds = float(TIMEOUT_GRACE_SECONDS if grace_val is None else grace_val)
    if pending_accs and grace_seconds > 0:
        db.upsert_signal_watch_event({
            "event_key": event_key,
            "message_id": msg_id,
            "provider_id": prov_id,
            "provider_name": prov_name,
            "channel_id": str(event.get("channel_id") or ""),
            "topic_id": int(event.get("topic_id") or 0),
            "symbol": sym,
            "direction": direction,
            "event_kind": event_kind,
            "raw_message": event.get("raw_message", ""),
            "parsed_signal": parsed,
            "expected_accounts": exp_summary,
            "account_statuses": account_statuses,
            "overall_status": "PENDING_CONFIRM",
            "has_discrepancy": False,
            "report_status": "WAITING",
            "created_at": now_ts,
            "updated_at": now_ts,
        })
        _notify_dashboard()
        _recheck_timed_out_accounts(acc_mgr, account_statuses, pending_accs, msg_id, grace_seconds)

    _, _, _, has_discrepancy, overall_status = _summarize_statuses(account_statuses, event_kind)

    watch_id = db.upsert_signal_watch_event({
        "event_key": event_key,
        "message_id": msg_id,
        "provider_id": prov_id,
        "provider_name": prov_name,
        "channel_id": str(event.get("channel_id") or ""),
        "topic_id": int(event.get("topic_id") or 0),
        "symbol": sym,
        "direction": direction,
        "event_kind": event_kind,
        "raw_message": event.get("raw_message", ""),
        "parsed_signal": parsed,
        "expected_accounts": exp_summary,
        "account_statuses": account_statuses,
        "overall_status": overall_status,
        "has_discrepancy": has_discrepancy,
        "report_status": "INVESTIGATING" if has_discrepancy else "NONE",
        "created_at": now_ts,
        "updated_at": int(time.time()),
    })
    _notify_dashboard()

    report_out = {}
    if has_discrepancy:
        evidence = collect_investigation_evidence(event, account_statuses, expected_accounts) if not skip_journals else {
            "telegram": {"message_id": msg_id, "raw_message": event.get("raw_message"), "parsed_signal": parsed},
            "sirius": {"provider_id": prov_id, "provider_name": prov_name, "event_kind": event_kind},
            "broker_journals": {},
            "code_context": "",
        }
        report_out = run_ai_investigation(event, evidence, account_statuses, ai_caller=ai_caller)
        confidence = report_out.get("confidence", "high")

        alert_ok = False
        if TELEGRAM_ALERTS_ENABLED and confidence != "low":
            sender = alert_sender or send_watch_telegram_alert
            try:
                alert_ok = bool(sender(event, watch_id, account_statuses, report_out.get("short_summary", "")))
            except Exception:
                alert_ok = False

        db.update_signal_watch_report(
            watch_id=watch_id,
            report_status="READY",
            ai_summary=report_out.get("short_summary", ""),
            ai_report_md=report_out.get("report_markdown", ""),
            antigravity_prompt=report_out.get("antigravity_prompt", ""),
            ai_confidence=confidence,
            evidence=evidence,
            alert_sent=1 if alert_ok else 0,
        )
        _notify_dashboard()

    return {
        "watch_id": watch_id,
        "overall_status": overall_status,
        "has_discrepancy": has_discrepancy,
        "account_statuses": account_statuses,
        "report": report_out,
    }


def reinvestigate_watch_event(watch_id: int, ai_caller=None) -> Optional[dict]:
    """Re-runs AI investigation on an existing Signal Watch event row."""
    row = db.get_signal_watch_event(watch_id)
    if not row:
        return None
    db.update_signal_watch_report(watch_id, report_status="INVESTIGATING")
    _notify_dashboard()

    from account_manager import AccountManager
    acc_mgr = AccountManager.get_instance()
    expected = acc_mgr.get_expected_accounts_for_provider(row.get("provider_id", ""))
    event = {
        "provider_id": row.get("provider_id"),
        "provider_name": row.get("provider_name"),
        "channel_id": row.get("channel_id"),
        "topic_id": row.get("topic_id"),
        "message_id": row.get("message_id"),
        "raw_message": row.get("raw_message"),
        "parsed_signal": row.get("parsed_signal") or {},
        "event_kind": row.get("event_kind"),
        "timestamp": row.get("created_at") or int(time.time()),
    }
    account_statuses = row.get("account_statuses") or {}
    evidence = collect_investigation_evidence(event, account_statuses, expected)
    report_out = run_ai_investigation(event, evidence, account_statuses, ai_caller=ai_caller)
    db.update_signal_watch_report(
        watch_id=watch_id,
        report_status="READY",
        ai_summary=report_out.get("short_summary", ""),
        ai_report_md=report_out.get("report_markdown", ""),
        antigravity_prompt=report_out.get("antigravity_prompt", ""),
        ai_confidence=report_out.get("confidence", "medium"),
        evidence=evidence,
    )
    _notify_dashboard()
    return db.get_signal_watch_event(watch_id)
