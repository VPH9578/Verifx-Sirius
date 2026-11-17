"""
main.py — CodePips Telegram Signal Copier
Ties together the signal listener, MT5 executor, and dashboard.

Usage: python main.py
"""

import asyncio
import threading
import sys
import re
import os
import msvcrt
import traceback
from datetime import datetime

# Prevent Windows console UnicodeEncodeErrors from Telegram emojis
if sys.platform == "win32":
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

_single_instance_file = None
_services_started = False

def acquire_instance_lock(lock_path="sirius_copier.lock"):
    """Enforce single instance using Windows kernel file locking."""
    global _single_instance_file
    try:
        _single_instance_file = open(lock_path, "w")
        msvcrt.locking(_single_instance_file.fileno(), msvcrt.LK_NBLCK, 1)
    except (IOError, OSError, PermissionError):
        print(f"[FATAL] Another instance of Sirius Copier is already running ({lock_path} locked). Terminating this process.")
        print("[HINT] If an old instance is stuck in the background, run 'Stop Sirius.bat' to clear it.")
        sys.exit(0)

import config
from mt5_executor import connect as mt5_connect, disconnect as mt5_disconnect, get_account_info
from signal_listener import listener_state
from dashboard import run_dashboard_loop
from mt5_executor import manage_trailing_stops, is_connected
import time

def run_trailing_stop_manager():
    """Background thread to manage trailing stops."""
    while True:
        try:
            if is_connected():
                manage_trailing_stops()
        except Exception as e:
            pass
        time.sleep(2)


def print_banner():
    """Print the startup banner."""
    print()
    print("=" * 60)
    print("  Verifx Official — Telegram Signal Copier")
    print("=" * 60)

    acc = get_account_info()
    if acc:
        print(f"  Account:    {acc.login}")
        print(f"  Balance:    ${acc.balance:,.2f}")
        print(f"  Server:     {config.MT5_SERVER}")

    print(f"  Symbol:     {config.SYMBOL}")
    print(f"  Group:      {config.SIGNAL_CHANNEL}")
    if config.SIGNAL_TOPIC_ID != 0:
        print(f"  Subtopic:   VIP Topic ({config.SIGNAL_TOPIC_ID})")
    print(f"  Lot Size:   {config.DEFAULT_LOT_SIZE}")
    print(f"  AI Parser:  Gemini ({config.GEMINI_MODEL})")
    print("=" * 60)
    print()


def validate_config():
    """Check that all required settings are filled in."""
    errors = []

    if config.TELEGRAM_API_ID == 0:
        errors.append("TELEGRAM_API_ID is not set in .env")
    if not config.TELEGRAM_API_HASH:
        errors.append("TELEGRAM_API_HASH is not set in .env")
    if not config.TELEGRAM_PHONE:
        errors.append("TELEGRAM_PHONE is not set in .env")
    if not hasattr(config, 'GEMINI_API_KEYS') or not config.GEMINI_API_KEYS:
        errors.append("GEMINI_API_KEYS is not set in config.py")
    if config.MT5_PASSWORD == "your_password":
        errors.append("MT5_PASSWORD is still the placeholder in .env")

    if errors:
        print("[ERROR] Missing configuration:")
        for e in errors:
            print(f"  - {e}")
        print()
        print("Create a .env file with the required values. See .env")
        return False

    return True


def main():
    acquire_instance_lock()
    if not validate_config():
        sys.exit(1)

    print("[INFO] Connecting to MetaTrader 5...")
    if not mt5_connect():
        print("[ERROR] Could not connect to MT5. Check your credentials in .env")
        sys.exit(1)

    print_banner()

    print("[INFO] Starting Telegram signal listener...")
    print("[INFO] On first run you'll be asked for your phone number and a verification code.")
    print("[INFO] Press Ctrl+C to stop.")
    print()

    while True:
        try:
            asyncio.run(start_with_dashboard())
            break
        except KeyboardInterrupt:
            print("\n[INFO] Shutting down...")
            break
        except Exception as e:
            traceback.print_exc()
            print(f"\n[WARN] Telegram listener stopped unexpectedly: {e}. Reconnecting in 5 seconds...")
            time.sleep(5)
    
    mt5_disconnect()
    print("[OK] Signal Copier stopped.")


async def start_with_dashboard():
    """Connect to Telegram first, then start dashboard."""
    from telethon import TelegramClient, events
    import logging
    logging.getLogger('telethon').setLevel(logging.CRITICAL)

    from telethon.sessions import StringSession
    from backtester.session_manager import get_string_session

    sess_str = get_string_session()
    client = TelegramClient(
        StringSession(sess_str),
        config.TELEGRAM_API_ID,
        config.TELEGRAM_API_HASH,
    )
    await client.connect()
    if not await client.is_user_authorized():
        await client.start(phone=config.TELEGRAM_PHONE)
    print("[OK] Telegram connected!")
    listener_state["connected"] = True

    import signal_listener
    signal_listener.telegram_client = client
    signal_listener.telegram_loop = asyncio.get_running_loop()

    from web_server import load_settings

    # Track resolved channels: dict mapping channel_id -> list of { "provider_id": pid, "topic_id": tid, "title": title }
    resolved_channels = {}
    last_providers_mtime = (0.0, 0.0)
    warned_unresolvable = set()

    async def refresh_channels(force: bool = False):
        nonlocal resolved_channels, last_providers_mtime
        cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "providers_config.json")
        acc_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "accounts_config.json")
        try:
            p_mtime = os.path.getmtime(cfg_path) if os.path.exists(cfg_path) else 0.0
        except Exception:
            p_mtime = 0.0
        try:
            a_mtime = os.path.getmtime(acc_path) if os.path.exists(acc_path) else 0.0
        except Exception:
            a_mtime = 0.0
        curr_mtime = (p_mtime, a_mtime)

        if not force and resolved_channels and curr_mtime == last_providers_mtime:
            return
        last_providers_mtime = curr_mtime

        cfg = load_settings()
        new_resolved = {}
        cached_dialog_titles = {}
        try:
            cache_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "telegram_dialogs_cache.json")
            if os.path.exists(cache_file):
                with open(cache_file, "r", encoding="utf-8") as f:
                    dialog_list = json.load(f)
                    for d in dialog_list:
                        cid = d.get("channel_id")
                        if cid is not None:
                            cached_dialog_titles[int(cid)] = d.get("channel_name") or d.get("display_name")
        except Exception:
            pass

        unresolved_entries = []

        for pid, pdata in cfg.items():
            ch_ident = pdata.get("channel") or config.SIGNAL_CHANNEL
            topic_id = int(pdata.get("topic_id") or (config.SIGNAL_TOPIC_ID if str(pid) == "1" else 0))
            is_numeric = False
            try:
                ch_ident_int = int(ch_ident)
                is_numeric = True
            except (ValueError, TypeError):
                is_numeric = False

            if is_numeric:
                c_id = ch_ident_int
                title = pdata.get("name") or cached_dialog_titles.get(c_id, str(ch_ident))
                prov_entry = {
                    "provider_id": str(pid),
                    "topic_id": topic_id,
                    "title": title
                }
                if c_id not in new_resolved:
                    new_resolved[c_id] = []
                new_resolved[c_id].append(prov_entry)
            else:
                unresolved_entries.append((pid, pdata, ch_ident, topic_id))

        # If any providers used non-numeric handles (e.g. @channel), resolve via Telethon
        for pid, pdata, ch_ident, topic_id in unresolved_entries:
            try:
                entity = await client.get_entity(ch_ident)
                c_id = entity.id
                if getattr(entity, 'megagroup', False) or getattr(entity, 'broadcast', False):
                    if c_id > 0 and not str(c_id).startswith("100"):
                        c_id = int(f"-100{c_id}")
                    elif c_id > 0 and str(c_id).startswith("100"):
                        c_id = int(f"-{c_id}")
                title = pdata.get("name") or getattr(entity, 'title', str(ch_ident))
                prov_entry = {
                    "provider_id": str(pid),
                    "topic_id": topic_id,
                    "title": title
                }
                if c_id not in new_resolved:
                    new_resolved[c_id] = []
                new_resolved[c_id].append(prov_entry)
            except Exception as e:
                warn_key = f"{pid}:{ch_ident}"
                if force or warn_key not in warned_unresolvable:
                    warned_unresolvable.add(warn_key)
                    print(f"[WARN] Could not resolve channel for Provider {pid} ({ch_ident}): {e}")

        resolved_channels = new_resolved
        if force:
            print(f"[OK] Instantly loaded {len(cfg)} providers across {len(resolved_channels)} channels.")

    await refresh_channels(force=True)
    if resolved_channels:
        first_list = list(resolved_channels.values())[0]
        if first_list:
            listener_state["channel_name"] = first_list[0]["title"]
    else:
        listener_state["channel_name"] = str(config.SIGNAL_CHANNEL)

    dashboard_thread = threading.Thread(target=run_dashboard_loop, daemon=True)
    dashboard_thread.start()

    from web_server import run_web_server
    web_thread = threading.Thread(target=run_web_server, daemon=True)
    web_thread.start()

    def open_edge():
        import time
        import subprocess
        time.sleep(2)  # Wait for server to boot
        try:
            # Start Microsoft Edge explicitly
            subprocess.Popen(['start', 'msedge', 'http://127.0.0.1:7770'], shell=True)
        except Exception as e:
            print(f"[WARN] Could not auto-launch Edge: {e}")

    edge_thread = threading.Thread(target=open_edge, daemon=True)
    edge_thread.start()

    ts_thread = threading.Thread(target=run_trailing_stop_manager, daemon=True)
    ts_thread.start()

    try:
        from signal_watch import start_observer
        start_observer()
    except Exception as _sw_e:
        print(f"[WARN] Could not start Signal Watch observer: {_sw_e}")

    ignored_chats = set()
    executed_message_ids = set()
    in_flight_message_ids = set()
    handler_lock = asyncio.Lock()
    latest_provider_signal = {}  # prov_id -> {"signal": dict, "msg_id": int, "text": str, "time": float}
    provider_last_exec_time = {}  # prov_id -> float timestamp of last trade execution
    recent_signal_fingerprints = {}  # (prov_id, inst, dir, entry, sl, tps) -> float timestamp
    try:
        import glob
        from mt5_executor import message_to_tickets
        for mid_key in message_to_tickets.keys():
            try:
                executed_message_ids.add(int(mid_key))
            except Exception:
                pass
        # Also seed from secondary/cTrader trade_state files
        base_dir = os.path.dirname(os.path.abspath(__file__))
        for fn in glob.glob(os.path.join(base_dir, "trade_state*.json")):
            try:
                with open(fn, "r", encoding="utf-8") as sf:
                    sdata = json.load(sf)
                    for k in (sdata.get("message_to_tickets") or {}).keys():
                        try:
                            executed_message_ids.add(int(k))
                        except Exception:
                            pass
            except Exception:
                pass
    except Exception:
        pass

    @client.on(events.NewMessage)
    @client.on(events.MessageEdited)
    async def handler(event):
        try:
            # Dynamically refresh channels if providers or accounts config changed
            await refresh_channels()

            if event.chat_id in ignored_chats:
                if event.chat_id in resolved_channels:
                    ignored_chats.remove(event.chat_id)
                else:
                    return

            prov_list = resolved_channels.get(event.chat_id)
            if not prov_list:
                ignored_chats.add(event.chat_id)
                return
    
            # Determine topic ID of the incoming message if in a forum
            msg = event.message
            msg_topic = 0
            if msg.reply_to and getattr(msg.reply_to, 'forum_topic', False):
                msg_topic = getattr(msg.reply_to, 'reply_to_top_id', None) or msg.reply_to.reply_to_msg_id
    
            # Find all matching providers based on topic_id (supports separate providers per account on same channel/topic)
            matched_provs = [p for p in prov_list if p.get("topic_id", 0) == msg_topic]

            if not matched_provs:
                return

            message_text = event.message.text
            if not message_text:
                return

            is_edit = isinstance(event, events.MessageEdited.Event)
            # Fast synchronous pre-check before acquiring lock
            if (msg.id in executed_message_ids or msg.id in in_flight_message_ids) and not is_edit:
                return
            in_flight_message_ids.add(msg.id)

            try:
                async with handler_lock:
                    if msg.id in executed_message_ids and not is_edit:
                        return

                    # If all accounts are paused, skip all executions
                    from account_manager import AccountManager
                    acc_mgr = AccountManager.get_instance()
                    if listener_state.get("is_paused", False) or acc_mgr.are_all_accounts_paused():
                        first_title = matched_provs[0]["title"]
                        print(f"[PAUSED] All accounts are PAUSED. Ignored signal/message from {first_title}.")
                        listener_state["signals_skipped"] += 1
                        listener_state["last_message"] = f"[ALL PAUSED] {message_text[:180]}"
                        listener_state["last_message_time"] = datetime.now()
                        return

                    from signal_parser import parse_signal, is_performance_recap
                    from mt5_executor import execute_signal, close_all_positions, cancel_trade, close_trade, modify_sl, modify_signal_levels, link_message_ids, set_breakeven_for_provider, cancel_all_pending_for_provider, get_open_positions, get_provider_settings, has_executed_trades_for_message, get_last_exec_diagnostics
                    from trade_logger import log_trade, log_close, log_error, log_signal_edit
                    from signal_watch import enqueue_signal_event, looks_like_unparsed_signal

                    # Handle edited messages for already executed trades
                    # Only treat as a level adjustment if this message actually originated executed trades
                    if is_edit and has_executed_trades_for_message(msg.id):
                        for matched_prov in matched_provs:
                            prov_id = matched_prov["provider_id"]
                            prov_title = matched_prov["title"]
                            print(f"[EDIT-DETECTED] Message {msg.id} was edited by Provider {prov_id} ({prov_title}). Checking for level adjustments...")

                            # Check recap
                            is_recap, recap_reason = is_performance_recap(message_text)
                            if is_recap:
                                print(f"[RECAP-FILTER] Ignored recap in edited message {msg.id}: {recap_reason}")
                                continue

                            # Check close/cancel keywords in edited message
                            msg_lower = message_text.lower().strip()
                            if re.search(r'\b(close\s+all\s+trades|close\s+all\s+positions|close\s+fully\s+now|take\s+the\s+\d+(?:-\d+)?\s*pip\s+(?:sl|loss))\b', msg_lower):
                                results = await asyncio.to_thread(close_all_positions, prov_id)
                                print(f"[EDIT-UPDATE] Close all positions triggered by edited message for Provider {prov_id}")
                                continue
                            elif msg_lower in ("cancel", "cancelled", "canceled"):
                                await asyncio.to_thread(cancel_all_pending_for_provider, prov_id)
                                print(f"[EDIT-UPDATE] Cancelled pending orders triggered by edited message for Provider {prov_id}")
                                continue

                            # Parse the updated signal
                            updated_signal = await asyncio.to_thread(
                                parse_signal,
                                message_text,
                                getattr(event.message.peer_id, "channel_id", None) or event.chat_id,
                                msg_topic or 0
                            )

                            if updated_signal and updated_signal.get("is_signal"):
                                new_tps = updated_signal.get("tp") or []
                                new_sl = float(updated_signal.get("sl") or 0.0)
                                new_entry = float(updated_signal.get("entry") or 0.0)
                                e_min = updated_signal.get("entry_min")
                                e_max = updated_signal.get("entry_max")

                                if new_sl > 0 or new_tps:
                                    mod_results = await asyncio.to_thread(
                                        modify_signal_levels,
                                        msg.id,
                                        new_tps,
                                        new_sl,
                                        new_entry,
                                        prov_id,
                                        e_min,
                                        e_max,
                                    )
                                    print(f"[EDIT-APPLIED] Applied updated levels for Provider {prov_id} (Msg {msg.id}): TP={new_tps}, SL={new_sl} -> Modified {len(mod_results)} ticket(s)")
                                    log_signal_edit(msg.id, prov_id, updated_signal, mod_results, message_text)
                                    listener_state["last_message"] = f"[EDITED] P{prov_id} TP={new_tps}"
                                    listener_state["last_message_time"] = datetime.now()

                                # Also dispatch execute_signal for any account on this provider that had NOT executed msg.id yet
                                # (or whose pending orders were cancelled unfilled); accounts that already executed msg.id skip via message_to_tickets.
                                if new_sl > 0:
                                    prov_settings = get_provider_settings(prov_id)
                                    if prov_settings.get("trading_enabled", True):
                                        exec_res = await asyncio.to_thread(execute_signal, updated_signal, msg.id, prov_id)
                                        if exec_res:
                                            listener_state["last_trade_results"] = exec_res
                                            listener_state["signals_executed"] += 1
                                            log_trade(updated_signal, exec_res, f"[Edited Signal Execution] {message_text}")
                                            print(f"[EDIT-EXEC] Executed edited signal for Provider {prov_id} (Msg {msg.id}) on remaining accounts: {len(exec_res)} order(s)")
                                        enqueue_signal_event(
                                            provider_id=prov_id,
                                            provider_name=prov_title,
                                            channel_id=getattr(event.message.peer_id, "channel_id", None) or event.chat_id,
                                            topic_id=msg_topic or 0,
                                            message_id=msg.id,
                                            raw_message=message_text,
                                            parsed_signal=updated_signal,
                                            exec_results=exec_res or [],
                                            account_diagnostics=get_last_exec_diagnostics(msg.id, prov_id),
                                            event_kind="EDITED_SIGNAL",
                                        )
                        return

                    parsed_signal_cache = None
                    parsed_signal_attempted = False

                    for matched_prov in matched_provs:
                        prov_id = matched_prov["provider_id"]
                        prov_title = matched_prov["title"]
                        print(f"[DEBUG] Valid {'Edited ' if is_edit else ''}Message (ID={msg.id}) from Provider {prov_id} ({prov_title}) — {message_text[:50]}")

                        # Check if message is a performance recap / results report
                        is_recap, recap_reason = is_performance_recap(message_text)
                        if is_recap:
                            print(f"[RECAP-FILTER] Ignored performance recap report from Provider {prov_id} ({prov_title}): {recap_reason}")
                            listener_state["signals_skipped"] += 1
                            log_error(
                                "RECAP_IGNORED",
                                f"Performance recap report detected and skipped: {recap_reason}",
                                provider_id=prov_id,
                                raw_message=message_text
                            )
                            continue

                        # Check if trading is disabled for this specific provider
                        prov_settings = get_provider_settings(prov_id)
                        if not prov_settings.get("trading_enabled", True):
                            print(f"[DISABLED] Trading disabled for Provider {prov_id} ({prov_title}). Signal skipped.")
                            listener_state["signals_skipped"] += 1
                            listener_state["last_message"] = f"[DISABLED: P{prov_id}] {message_text[:180]}"
                            listener_state["last_message_time"] = datetime.now()
                            continue

                        timestamp = datetime.now()
                        listener_state["last_message"] = message_text[:200]
                        listener_state["last_message_time"] = timestamp
                        listener_state["channel_name"] = prov_title

                        msg_lower = message_text.lower().strip()
                        update_handled = False

                        # 1. Check for Set Break-Even / Entry commands (e.g. "Set BE", "Setting BE guys", "Set BE if you wish", "Make sure BE is set", "move sl to be", "putting BE", "close at BE")
                        if len(msg_lower) < 120 and re.search(r'\b(set(?:ting)?\s*be|be\s+is\s+set|move\s*sl\s*to\s*be|sl\s*(?:to|at)?\s*be|sl\s*to\s*entry|(?:move\s*)?tp\s*(?:to|at)?\s*(?:be|entry)|(?:close|exit)\s+(?:at|on)\s+(?:be|break\s*even|breakeven|entry)|break\s*even|breakeven|put(?:ting)?\s+be|go(?:ing)?\s+be|lock(?:ing)?\s+(?:in\s+)?be|be\s+now)\b', msg_lower):
                            ok = await asyncio.to_thread(set_breakeven_for_provider, prov_id)
                            print(f"[UPDATE] Set BE executed for Provider {prov_id}: {ok}")
                            update_handled = True
                        # 2. Check for explicit "CLOSE ALL TRADES", "CLOSE FULLY NOW", or "take the XX pip SL/loss" broadcast in channel/topic
                        elif re.search(r'\b(close\s+all\s+trades|close\s+all\s+positions|close\s+fully\s+now|take\s+the\s+\d+(?:-\d+)?\s*pip\s+(?:sl|loss)|not\s+getting\s+the\s+reaction\s+i\s+wanted)\b', msg_lower):
                            results = await asyncio.to_thread(close_all_positions, prov_id)
                            print(f"[UPDATE] Close all positions executed for Provider {prov_id}: {len(results)} closed")
                            update_handled = True
                        # 3. Check for standalone "cancelled" / "cancel" broadcast in topic
                        elif msg_lower in ("cancel", "cancelled", "canceled"):
                            ok = await asyncio.to_thread(cancel_all_pending_for_provider, prov_id)
                            if prov_id in latest_provider_signal:
                                del latest_provider_signal[prov_id]
                            print(f"[UPDATE] Cancelled pending orders/setup for Provider {prov_id}: {ok}")
                            update_handled = True
                        # 4. Check for "IM IN" confirmation message (re-attempts/triggers the latest Trade Idea if not currently in a trade)
                        elif re.search(r'\b(im\s*in|i\'?m\s*in|i\u2019?m\s*in|i\s+am\s+in|immin)\b', msg_lower):
                            last_info = latest_provider_signal.get(prov_id)
                            if last_info and (time.time() - last_info["time"] <= 1800):
                                p_settings = get_provider_settings(prov_id)
                                p_magic = int(p_settings.get("magic_number") or (400400 + int(prov_id) if str(prov_id) != "1" else config.MAGIC_NUMBER))
                                open_pos = await asyncio.to_thread(get_open_positions, magic=p_magic)

                                # Guard against duplicate entries: if provider already has open positions or signal was executed, skip!
                                if open_pos:
                                    print(f"[INFO] 'IM IN' ignored: Provider {prov_id} already has {len(open_pos)} open position(s)")
                                    update_handled = True
                                    continue

                                # Cancel any un-triggered pending orders first, then execute at market
                                await asyncio.to_thread(cancel_all_pending_for_provider, prov_id)
                                sig_copy = dict(last_info["signal"])
                                sig_copy["order_type"] = "MARKET"
                                sig_copy["entry"] = last_info["signal"].get("entry", 0)  # Preserve original entry for stop validation / pending fallback
                                results = await asyncio.to_thread(execute_signal, sig_copy, last_info["msg_id"], prov_id)
                                if results:
                                    provider_last_exec_time[prov_id] = time.time()
                                    listener_state["last_trade_results"] = results
                                    listener_state["signals_executed"] += 1
                                    log_trade(sig_copy, results, f"[IM IN Trigger] {last_info['text']}")
                                    print(f"[EXEC] 'IM IN' triggered execution for Provider {prov_id}: {len(results)} order(s) placed")
                                enqueue_signal_event(
                                    provider_id=prov_id,
                                    provider_name=prov_title,
                                    channel_id=getattr(event.message.peer_id, "channel_id", None) or event.chat_id,
                                    topic_id=msg_topic or 0,
                                    message_id=last_info["msg_id"],
                                    raw_message=last_info["text"],
                                    parsed_signal=sig_copy,
                                    exec_results=results or [],
                                    account_diagnostics=get_last_exec_diagnostics(last_info["msg_id"], prov_id),
                                    event_kind="IM_IN_TRIGGER",
                                )
                            update_handled = True
                        elif event.is_reply:
                            reply_id = event.reply_to_msg_id
                            # Only treat as a direct reply to a specific signal if reply_id is not the forum topic root ID itself
                            if reply_id and reply_id != msg_topic:
                                if "cancel" in msg_lower:
                                    ok = await asyncio.to_thread(cancel_trade, reply_id)
                                    print(f"[UPDATE] Cancel reply for msg {reply_id}: {ok}")
                                    update_handled = True
                                elif "closed" in msg_lower or "close" in msg_lower:
                                    ok = await asyncio.to_thread(close_trade, reply_id)
                                    print(f"[UPDATE] Close reply for msg {reply_id}: {ok}")
                                    update_handled = True
                                elif "adjust sl" in msg_lower:
                                    match = re.search(r'adjust sl to ([\d.]+)', msg_lower)
                                    if match:
                                        new_sl = float(match.group(1))
                                        ok = await asyncio.to_thread(modify_sl, reply_id, new_sl)
                                        print(f"[UPDATE] Adjust SL reply for msg {reply_id} -> {new_sl}: {ok}")
                                        update_handled = True

                        if update_handled:
                            continue

                        if not parsed_signal_attempted:
                            parsed_signal_cache = await asyncio.to_thread(
                                parse_signal,
                                message_text,
                                getattr(event.message.peer_id, "channel_id", None) or event.chat_id,
                                msg_topic or 0
                            )
                            parsed_signal_attempted = True
                            if parsed_signal_cache and parsed_signal_cache.get("is_signal", False):
                                listener_state["signals_received"] += 1
                                listener_state["last_signal"] = parsed_signal_cache
                            else:
                                listener_state["signals_skipped"] += 1

                        signal = parsed_signal_cache
                        if signal is None or not signal.get("is_signal", False):
                            if isinstance(signal, dict) and signal.get("error_code") == "SL_TYPO_UNFIXABLE":
                                reason_sl = signal.get("error_reason") or "SL typo unfixable / failed minimum stop distance."
                                print(f"[BLOCKED] {reason_sl}")
                                log_error(
                                    "SL_TYPO_UNFIXABLE",
                                    reason_sl,
                                    provider_id=prov_id,
                                    symbol=signal.get("instrument"),
                                    raw_message=message_text,
                                )
                                enqueue_signal_event(
                                    provider_id=prov_id,
                                    provider_name=prov_title,
                                    channel_id=getattr(event.message.peer_id, "channel_id", None) or event.chat_id,
                                    topic_id=msg_topic or 0,
                                    message_id=msg.id,
                                    raw_message=message_text,
                                    parsed_signal=signal,
                                    exec_results=[],
                                    account_diagnostics={},
                                    event_kind="SL_TYPO_UNFIXABLE",
                                    force_issue_reason=reason_sl,
                                )
                            elif looks_like_unparsed_signal(message_text):
                                enqueue_signal_event(
                                    provider_id=prov_id,
                                    provider_name=prov_title,
                                    channel_id=getattr(event.message.peer_id, "channel_id", None) or event.chat_id,
                                    topic_id=msg_topic or 0,
                                    message_id=msg.id,
                                    raw_message=message_text,
                                    parsed_signal=signal or {},
                                    exec_results=[],
                                    account_diagnostics={},
                                    event_kind="PARSER_MISSED",
                                    force_issue_reason="Message looked like a trade signal (direction + SL/TP/price levels) but parse_signal() returned is_signal=False.",
                                )
                            continue

                        direction = signal.get("direction", "")

                        if direction == "CLOSE":
                            results = await asyncio.to_thread(close_all_positions, prov_id)
                            listener_state["last_trade_results"] = results
                            listener_state["signals_executed"] += 1
                            log_close(results, message_text)
                        else:
                            # Cooldown guard scoped to (provider, instrument, direction) so multi-coin bursts (e.g. BTC + SOL) execute simultaneously
                            cooldown_key = (
                                str(prov_id),
                                str(signal.get("instrument") or "XAUUSD").upper(),
                                str(direction).upper()
                            )
                            if cooldown_key in provider_last_exec_time and (time.time() - provider_last_exec_time[cooldown_key] < 10):
                                print(f"[INFO] Ignored duplicate/rapid signal from Provider {prov_id} for {cooldown_key[1]} {cooldown_key[2]} (cooldown active)")
                                continue

                            # Safety Rule 3: No Stop Loss, No Trade
                            sl_val = float(signal.get("sl") or 0.0)
                            if sl_val <= 0:
                                print(f"[BLOCKED] Signal from Provider {prov_id} rejected: Missing Stop Loss (SL={sl_val}). Skipping execution.")
                                listener_state["signals_skipped"] += 1
                                log_error(
                                    "MISSING_STOP_LOSS",
                                    "Signal rejected: Missing mandatory Stop Loss (SL=0). Trade skipped for account protection.",
                                    provider_id=prov_id,
                                    symbol=signal.get("instrument") or signal.get("symbol"),
                                    raw_message=message_text
                                )
                                enqueue_signal_event(
                                    provider_id=prov_id,
                                    provider_name=prov_title,
                                    channel_id=getattr(event.message.peer_id, "channel_id", None) or event.chat_id,
                                    topic_id=msg_topic or 0,
                                    message_id=msg.id,
                                    raw_message=message_text,
                                    parsed_signal=signal,
                                    exec_results=[],
                                    account_diagnostics={},
                                    event_kind="MISSING_STOP_LOSS",
                                    force_issue_reason="Safety Rule 3 rejected signal: Missing Stop Loss (SL=0) or parser failed to extract SL.",
                                )
                                continue

                            # Part B (Listener-level sanity check): Check if exact same signal levels were already received recently
                            sig_fp = (
                                str(prov_id),
                                str(signal.get("instrument", "")).upper(),
                                str(direction).upper(),
                                round(float(signal.get("entry") or 0), 5),
                                round(float(signal.get("sl") or 0), 5),
                                tuple(round(float(x), 5) for x in (signal.get("tp") or [])[:3])
                            )
                            now_ts = time.time()
                            if sig_fp in recent_signal_fingerprints and (now_ts - recent_signal_fingerprints[sig_fp] < 1800):
                                print(f"[SANITY-GUARD] Ignored duplicate signal from Provider {prov_id}: identical Entry/SL/TP levels already processed ({sig_fp}).")
                                continue

                            # Claim fingerprint and cooldown BEFORE awaiting execution
                            recent_signal_fingerprints[sig_fp] = now_ts
                            provider_last_exec_time[cooldown_key] = now_ts

                            latest_provider_signal[prov_id] = {
                                "signal": dict(signal),
                                "msg_id": msg.id,
                                "text": message_text,
                                "time": now_ts
                            }
                            results = await asyncio.to_thread(execute_signal, signal, event.message.id, prov_id)
                            listener_state["last_trade_results"] = results
                            if results:
                                provider_last_exec_time[cooldown_key] = time.time()
                                listener_state["signals_executed"] += 1
                                log_trade(signal, results, message_text)
                                print(f"[EXEC] Signal executed successfully for Provider {prov_id}: {len(results)} order(s) placed")
                            else:
                                # If execution returned empty (e.g. rejected/skipped), allow retry if needed by clearing fingerprint & cooldown
                                recent_signal_fingerprints.pop(sig_fp, None)
                                provider_last_exec_time.pop(cooldown_key, None)

                            enqueue_signal_event(
                                provider_id=prov_id,
                                provider_name=prov_title,
                                channel_id=getattr(event.message.peer_id, "channel_id", None) or event.chat_id,
                                topic_id=msg_topic or 0,
                                message_id=event.message.id,
                                raw_message=message_text,
                                parsed_signal=signal,
                                exec_results=results or [],
                                account_diagnostics=get_last_exec_diagnostics(event.message.id, prov_id),
                                event_kind="EDITED_SIGNAL" if is_edit else "NEW_SIGNAL",
                            )

                    executed_message_ids.add(msg.id)
            finally:
                in_flight_message_ids.discard(msg.id)
        except Exception as e:
            try:
                print(f"[WARN] Error handling Telegram message event: {e}")
            except Exception:
                pass

    async def outgoing_poll_worker():
        """
        Background poller for self-sent (outgoing) messages in configured channels/topics.
        Telegram MTProto does not push incoming event updates to user sessions when messages
        are sent by the user themselves in supergroups/channels.
        This poller fetches recent messages every 2.5s and dispatches any outgoing messages
        to the handler if they haven't been processed yet.
        """
        seen_poller_message_ids = set()
        # Initial seed of recent message IDs so we don't replay outgoing history on startup
        try:
            for ch_id, p_list in list(resolved_channels.items()):
                for p in p_list:
                    t_id = p.get("topic_id", 0)
                    try:
                        recent = await client.get_messages(ch_id, limit=5, reply_to=t_id if t_id else None)
                        for m in recent:
                            seen_poller_message_ids.add(m.id)
                    except Exception:
                        pass
        except Exception:
            pass

        while True:
            try:
                await asyncio.sleep(2.5)
                from account_manager import AccountManager
                acc_mgr = AccountManager.get_instance()
                if listener_state.get("is_paused", False) or acc_mgr.are_all_accounts_paused():
                    continue

                for ch_id, p_list in list(resolved_channels.items()):
                    for p in p_list:
                        t_id = p.get("topic_id", 0)
                        try:
                            # Fetch last 3 messages for this topic
                            msgs = await client.get_messages(ch_id, limit=3, reply_to=t_id if t_id else None)
                            for m in reversed(msgs):  # process older first
                                if not m or not m.text:
                                    continue
                                if m.id in seen_poller_message_ids or m.id in executed_message_ids:
                                    continue
                                seen_poller_message_ids.add(m.id)
                                # Check if this is an outgoing message (sent by user)
                                if getattr(m, "out", False):
                                    event = events.NewMessage.Event(m)
                                    await handler(event)
                        except Exception:
                            pass
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(5)

    poller_task = asyncio.create_task(outgoing_poll_worker())

    print(f"[OK] Multi-channel listening active for {len(resolved_channels)} provider(s) (with self-signal poller)")
    try:
        await client.run_until_disconnected()
    finally:
        poller_task.cancel()


if __name__ == "__main__":
    main()