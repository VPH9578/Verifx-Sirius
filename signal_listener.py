"""
signal_listener.py — Monitors a Telegram channel for new messages
using Telethon (user client, not bot API).
Passes each message to Claude for parsing, then executes on MT5.
"""

import asyncio
import re
import os
import json
from telethon import TelegramClient, events
from datetime import datetime

import config
from signal_parser import parse_signal
from mt5_executor import execute_signal, close_all_positions, cancel_trade, close_trade, modify_sl
from trade_logger import log_trade, log_close

telegram_client = None
telegram_loop = None

CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "telegram_dialogs_cache.json")

def load_cached_dialogs():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list) and len(data) > 0:
                    return data
        except Exception as e:
            print("[WARN] Error loading dialogs cache:", e)
    return []

async def fetch_dialogs_and_topics(force_refresh=False):
    if not force_refresh:
        cached = load_cached_dialogs()
        if cached:
            return cached

    client = telegram_client
    if not client or not client.is_connected():
        return load_cached_dialogs()

    from telethon.tl.functions.messages import GetForumTopicsRequest
    items = []
    try:
        async for d in client.iter_dialogs():
            if d.is_channel or d.is_group:
                is_forum = getattr(d.entity, 'forum', False)
                if is_forum:
                    try:
                        res = await client(GetForumTopicsRequest(peer=d.input_entity, offset_date=None, offset_id=0, offset_topic=0, limit=100))
                        topics_found = False
                        for t in res.topics:
                            if hasattr(t, 'title') and t.title:
                                topics_found = True
                                items.append({
                                    "channel_id": d.id,
                                    "channel_name": d.name,
                                    "topic_id": t.id,
                                    "topic_name": t.title,
                                    "display_name": f"{d.name} » {t.title}",
                                    "type": "topic"
                                })
                        if not topics_found:
                            items.append({
                                "channel_id": d.id,
                                "channel_name": d.name,
                                "topic_id": 0,
                                "topic_name": "General",
                                "display_name": d.name,
                                "type": "forum"
                            })
                    except Exception:
                        items.append({
                            "channel_id": d.id,
                            "channel_name": d.name,
                            "topic_id": 0,
                            "topic_name": None,
                            "display_name": d.name,
                            "type": "group"
                        })
                else:
                    items.append({
                        "channel_id": d.id,
                        "channel_name": d.name,
                        "topic_id": 0,
                        "topic_name": None,
                        "display_name": d.name,
                        "type": "channel" if d.is_channel else "group"
                    })
        items.sort(key=lambda x: str(x["display_name"]).lower())
        try:
            with open(CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(items, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print("[WARN] Error saving dialogs cache:", e)
        return items
    except Exception as e:
        print(f"[WARN] Error fetching dialogs: {e}")
        return load_cached_dialogs()

# Shared state for the dashboard to read
listener_state = {
    "connected": False,
    "channel_name": config.SIGNAL_CHANNEL,
    "last_message": "",
    "last_message_time": None,
    "last_signal": None,
    "last_trade_results": [],
    "signals_received": 0,
    "signals_executed": 0,
    "signals_skipped": 0,
    "is_paused": False,
}


async def start_listener(dashboard_callback=None):
    """
    Start the Telethon client and listen for new messages
    from the configured signal channel.
    """
    client = TelegramClient(
        config.SESSION_FILE,
        config.TELEGRAM_API_ID,
        config.TELEGRAM_API_HASH,
    )

    await client.start(phone=config.TELEGRAM_PHONE)
    listener_state["connected"] = True

    try:
        channel = await client.get_entity(config.SIGNAL_CHANNEL)
        listener_state["channel_name"] = getattr(channel, "title", config.SIGNAL_CHANNEL)
    except Exception as e:
        print(f"[ERROR] Could not resolve channel '{config.SIGNAL_CHANNEL}': {e}")
        print("[INFO] Make sure you are subscribed to this channel on your Telegram account")
        return

    @client.on(events.NewMessage(chats=channel))
    async def handler(event):
        message_text = event.message.text
        if not message_text:
            return

        timestamp = datetime.now()

        listener_state["last_message"] = message_text[:200]
        listener_state["last_message_time"] = timestamp

        # Handle Replies for Trade Updates
        if event.is_reply:
            reply_id = event.reply_to_msg_id
            msg_lower = message_text.lower()
            
            update_handled = False
            
            if "cancel" in msg_lower:
                success = await asyncio.to_thread(cancel_trade, reply_id)
                print(f"[UPDATE] Cancel reply detected for msg {reply_id}. Success: {success}")
                update_handled = True
            elif "closed" in msg_lower or "close" in msg_lower:
                success = await asyncio.to_thread(close_trade, reply_id)
                print(f"[UPDATE] Close reply detected for msg {reply_id}. Success: {success}")
                update_handled = True
            elif "adjust sl" in msg_lower:
                match = re.search(r'adjust sl to ([\d.]+)', msg_lower)
                if match:
                    new_sl = float(match.group(1))
                    success = await asyncio.to_thread(modify_sl, reply_id, new_sl)
                    print(f"[UPDATE] Adjust SL reply detected for msg {reply_id} -> {new_sl}. Success: {success}")
                    update_handled = True
            
            if update_handled:
                if dashboard_callback:
                    dashboard_callback()
                return # Don't parse this as a new signal

        # Send to Claude for parsing
        signal = await asyncio.to_thread(parse_signal, message_text)

        if signal is None:
            listener_state["signals_skipped"] += 1
            return

        if not signal.get("is_signal", False):
            listener_state["signals_skipped"] += 1
            return

        listener_state["signals_received"] += 1
        listener_state["last_signal"] = signal

        # Execute the signal on MT5
        direction = signal.get("direction", "")

        if direction == "CLOSE":
            results = await asyncio.to_thread(close_all_positions)
            listener_state["last_trade_results"] = results
            listener_state["signals_executed"] += 1
            log_close(results, message_text)
        else:
            # Pass the message ID to track it
            results = await asyncio.to_thread(execute_signal, signal, event.message.id)
            listener_state["last_trade_results"] = results
            if results:
                listener_state["signals_executed"] += 1
                log_trade(signal, results, message_text)

        if dashboard_callback:
            dashboard_callback()

    print(f"[OK] Listening to channel: {listener_state['channel_name']}")
    await client.run_until_disconnected()
