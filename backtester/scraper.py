"""
scraper.py — Historical Telegram Message & Signal Scraper for the Backtester.
Supports standard channels, megagroups, and specific Forum Group Topics.
Caches scraped messages in backtest.db so rule tweaks re-simulate in milliseconds.
"""

import asyncio
import re
import time
from datetime import datetime, timezone
from telethon import TelegramClient

import config
from signal_parser import parse_with_regex, parse_signal
from backtester import bt_db
from backtester.session_manager import get_isolated_telegram_client


def _safe_float(val, default: float = 0.0) -> float:
    if val is None:
        return default
    if isinstance(val, (int, float)):
        return float(val)
    try:
        s = str(val).strip().rstrip(".")
        return float(s) if s else default
    except (ValueError, TypeError):
        return default


def _safe_parse_regex(text: str, channel_id: str | int = None, topic_id: int = 0) -> dict | None:
    try:
        if channel_id:
            try:
                from provider_parser_manager import get_profile, parse_with_profile
                prof = get_profile(channel_id, topic_id)
                if prof:
                    res = parse_with_profile(text, prof)
                    if res and res.get("is_signal"):
                        return res
            except Exception:
                pass
        return parse_with_regex(text)
    except Exception:
        return None


def _reconstruct_abbreviated_price(target_val: float, reference_price: float) -> float:
    """
    Reconstruct 2-digit shorthand prices commonly sent in Telegram chats.
    E.g. Gold reference at 4020.15 and provider says 'Move SL to 27' -> 4027.0.
    """
    if reference_price <= 0 or target_val <= 0:
        return target_val
    s_tgt = str(int(round(target_val)))
    s_ref = str(int(round(reference_price)))
    if len(s_tgt) < len(s_ref) and len(s_tgt) <= 2:
        try:
            reconstructed = float(s_ref[:-len(s_tgt)] + s_tgt)
            if abs(reconstructed - reference_price) / reference_price <= 0.05:
                return reconstructed
        except Exception:
            pass
    return target_val


def classify_update_message(text: str) -> dict | None:
    """
    Detect if a non-signal message is a trade management instruction:
    - CANCEL: Cancel pending order
    - CLOSE: Close active trade(s) early
    - SET_BE: Move Stop Loss to Entry (Break-Even)
    - MODIFY_SL: Adjust Stop Loss to a specific price level
    """
    if not text or not text.strip():
        return None

    clean = text.strip()
    lower = clean.lower()

    # 1. Explicit SL adjustment: e.g. "Adjust SL to 4185.1", "Move SL to 2655", "New SL: 4180"
    sl_mod_match = re.search(r'(?:adjust|move|change|update|new)\s+sl\s*(?:to|at|:|=)?\s*(\d+(?:\.\d+)?)', lower)
    if sl_mod_match:
        new_sl = _safe_float(sl_mod_match.group(1), 0.0)
        if new_sl > 0:
            return {"action": "MODIFY_SL", "new_sl": new_sl, "text": clean[:120]}

    # 2. Explicit Break-Even command: e.g. "Set BE", "Set BE if you wish", "Move SL to BE", "SL to entry", "Close at BE"
    if re.search(r'\b(set\s*be|be\s+is\s+set|move\s*sl\s*to\s*be|sl\s*(?:to|at)?\s*be|sl\s*to\s*entry|(?:move\s*)?tp\s*(?:to|at)?\s*(?:be|entry)|(?:close|exit)\s+(?:at|on)\s+(?:be|break\s*even|breakeven|entry)|break\s*even|breakeven|put(?:ting)?\s+be|be\s+now)\b', lower):
        return {"action": "SET_BE", "text": clean[:120]}

    # Ignore automated TP/SL hit announcements, recaps, or running pip commentary
    if any(w in lower for w in ["hit", "smashed", "pips", "recap", "activated", "running", "congrats", "target"]):
        return None

    # 3. Cancel pending order command (short message or explicit cancel)
    if len(clean) < 80 and re.search(r'\b(cancel|cancelled|canceled|delete\s+order|delete\s+limit|invalid|void)\b', lower):
        return {"action": "CANCEL", "text": clean[:120]}

    # 4. Close trade command
    if len(clean) < 100 and re.search(r'\b(close\s+now|close\s+all|close\s+full|close\s+trade|close\s+position|closed|close|exit\s+now)\b', lower):
        # Avoid false positives like "market close" or "weekly close"
        if not re.search(r'\b(market|daily|weekly|monthly|candle|bar|partial|half)\s+close\b', lower):
            return {"action": "CLOSE", "text": clean[:120]}

    return None


async def _async_scrape_telegram(
    client: TelegramClient,
    channel_id: str | int,
    topic_id: int,
    start_time: int,
    end_time: int,
    use_ai_fallback: bool = False,
    progress_cb=None,
    max_messages: int = 50000,
) -> tuple[list, str]:
    """
    Internal coroutine that iterates Telegram messages between [start_time, end_time]
    for a channel or forum topic using a connected Telethon client.
    """
    # Resolve channel entity
    ch_query = channel_id
    try:
        ch_query = int(channel_id)
    except (ValueError, TypeError):
        ch_query = str(channel_id).strip()

    try:
        entity = await asyncio.wait_for(client.get_entity(ch_query), timeout=10.0)
    except Exception:
        # Fallback: refresh dialogs cache in Telethon session and retry
        await asyncio.wait_for(client.get_dialogs(limit=200), timeout=15.0)
        entity = await asyncio.wait_for(client.get_entity(ch_query), timeout=10.0)

    channel_title = getattr(entity, "title", str(channel_id))
    dt_end = datetime.fromtimestamp(int(end_time) + 60, tz=timezone.utc)

    iter_kwargs = {"offset_date": dt_end}
    if topic_id and int(topic_id) > 0:
        iter_kwargs["reply_to"] = int(topic_id)

    raw_msgs = []
    scanned = 0

    try:
        async for msg in client.iter_messages(entity, **iter_kwargs):
            if not msg or not msg.date:
                continue
            m_ts = int(msg.date.timestamp())
            if m_ts > end_time:
                continue
            if m_ts < start_time:
                break

            scanned += 1
            if progress_cb and scanned % 100 == 0:
                progress_cb(f"Scanning Telegram history... ({scanned:,} messages read)")

            text = msg.text or ""
            if not text.strip():
                continue

            # Determine reply_to_msg_id (ignoring the forum topic root ID itself)
            reply_id = None
            if msg.reply_to:
                r_mid = getattr(msg.reply_to, "reply_to_msg_id", None)
                if r_mid and int(r_mid) != int(topic_id or 0):
                    reply_id = int(r_mid)

            raw_msgs.append({
                "msg_id": int(msg.id),
                "msg_time": m_ts,
                "reply_to_msg_id": reply_id,
                "text": text,
                "parsed": None,
            })

            # Safety ceiling for extreme high-volume chat rooms
            if scanned >= max_messages:
                break
    except Exception as e:
        # If reply_to=1 on General forum topic fails, fallback without reply_to
        if topic_id and int(topic_id) == 1 and not raw_msgs:
            iter_kwargs.pop("reply_to", None)
            async for msg in client.iter_messages(entity, **iter_kwargs):
                if not msg or not msg.date:
                    continue
                m_ts = int(msg.date.timestamp())
                if m_ts > end_time:
                    continue
                if m_ts < start_time:
                    break
                scanned += 1
                text = msg.text or ""
                if not text.strip():
                    continue
                raw_msgs.append({
                    "msg_id": int(msg.id),
                    "msg_time": m_ts,
                    "reply_to_msg_id": getattr(msg.reply_to, "reply_to_msg_id", None) if msg.reply_to else None,
                    "text": text,
                    "parsed": None,
                })
                if scanned >= max_messages:
                    break
        else:
            raise e

    # Reverse into chronological order (oldest -> newest)
    raw_msgs.reverse()

    # Parse messages for trade signals
    total_msgs = len(raw_msgs)
    for idx, m in enumerate(raw_msgs):
        txt = m["text"]
        parsed = _safe_parse_regex(txt, channel_id=channel_id, topic_id=topic_id)
        if parsed is None and use_ai_fallback:
            try:
                parsed = await asyncio.to_thread(parse_signal, txt, channel_id, topic_id)
            except Exception:
                parsed = None
        if parsed and parsed.get("is_signal"):
            m["parsed"] = parsed
        if progress_cb and idx % 200 == 0 and total_msgs > 200:
            progress_cb(f"Parsing signals... ({idx:,} / {total_msgs:,} messages)")

    return raw_msgs, channel_title


def _build_signals_from_messages(messages: list) -> list:
    """Build structured signals with linked reply/broadcast updates from raw messages."""
    signals_by_id = {}
    ordered_signals = []

    for m in messages:
        msg_id = m["msg_id"]
        msg_time = m["msg_time"]
        text = m["text"]
        reply_id = m.get("reply_to_msg_id")
        parsed = m.get("parsed")

        # Re-check regex if cached before a parser update
        if not parsed:
            parsed = _safe_parse_regex(text)

        if parsed and parsed.get("is_signal"):
            direction = (parsed.get("direction") or "").upper()
            if direction == "CLOSE":
                # A standalone CLOSE signal acts as a broadcast CLOSE update to recent signals
                for prev_sig in reversed(ordered_signals[-3:]):
                    if 0 <= (msg_time - prev_sig["timestamp"]) <= 86400:
                        prev_sig["updates"].append({
                            "msg_id": msg_id,
                            "timestamp": msg_time,
                            "action": "CLOSE",
                            "text": text[:120],
                        })
                continue

            sl_val = _safe_float(parsed.get("sl"), 0.0)
            entry_val = _safe_float(parsed.get("entry"), 0.0)
            raw_tps = [_safe_float(x, 0.0) for x in (parsed.get("tp") or []) if _safe_float(x, 0.0) > 0]

            # Validate that TP prices are mathematically on the correct side of entry & SL
            valid_tps = []
            for tp_val in raw_tps:
                if direction == "BUY":
                    if tp_val > entry_val and (sl_val <= 0 or tp_val > sl_val):
                        valid_tps.append(tp_val)
                elif direction == "SELL":
                    if tp_val < entry_val and (sl_val <= 0 or tp_val < sl_val):
                        valid_tps.append(tp_val)

            if sl_val <= 0 or not valid_tps:
                continue

            # Sanity rule: validate SL is on the correct side of entry and within realistic bounds
            if direction == "BUY":
                if sl_val >= entry_val or (entry_val - sl_val) / entry_val > 0.15:
                    continue
            elif direction == "SELL":
                if sl_val <= entry_val or (sl_val - entry_val) / entry_val > 0.15:
                    continue

            sig_obj = {
                "msg_id": msg_id,
                "timestamp": msg_time,
                "direction": direction,
                "order_type": (parsed.get("order_type") or "MARKET").upper(),
                "entry": entry_val,
                "sl": sl_val,
                "tp": valid_tps,
                "instrument": (parsed.get("instrument") or "XAUUSD").upper(),
                "raw_text": text[:300],
                "updates": [],
            }
            signals_by_id[msg_id] = sig_obj
            ordered_signals.append(sig_obj)
            continue

        # Check if this non-signal message is a reply update or broadcast management command
        upd = classify_update_message(text)
        if upd:
            target_sig = None
            if reply_id and reply_id in signals_by_id:
                target_sig = signals_by_id[reply_id]
            elif not reply_id and ordered_signals:
                last_sig = ordered_signals[-1]
                if 0 <= (msg_time - last_sig["timestamp"]) <= 43200:
                    target_sig = last_sig

            if target_sig:
                raw_new_sl = upd.get("new_sl")
                processed_sl = None
                if raw_new_sl is not None and raw_new_sl > 0:
                    ref_p = target_sig.get("entry") or target_sig.get("sl") or 0.0
                    reconstructed = _reconstruct_abbreviated_price(raw_new_sl, ref_p)
                    # Sanity check: must be within 10% of reference price
                    if ref_p > 0 and abs(reconstructed - ref_p) / ref_p <= 0.10:
                        processed_sl = reconstructed
                    else:
                        processed_sl = None  # Discard unrealistic SL update

                if upd["action"] == "MODIFY_SL" and processed_sl is None:
                    continue  # Discard malformed/unrealistic SL modification

                upd_entry = {
                    "msg_id": msg_id,
                    "timestamp": msg_time,
                    "action": upd["action"],
                    "new_sl": processed_sl,
                    "text": upd.get("text", ""),
                }
                target_sig["updates"].append(upd_entry)

    return ordered_signals


async def async_fetch_and_build_signals(
    client: TelegramClient,
    channel_id: str | int,
    topic_id: int,
    start_time: int,
    end_time: int,
    force_refresh: bool = False,
    use_ai_fallback: bool = False,
    progress_cb=None,
    max_messages: int = 10000,
) -> tuple[list, str, int]:
    """
    Async version of fetch_and_build_signals that reuses an already-connected Telethon client
    and performs incremental tail/head scraping when partial cache coverage exists.
    """
    ch_str = str(channel_id).strip()
    t_id = int(topic_id or 0)
    s_time = int(start_time)
    e_time = int(end_time)

    coverage = bt_db.get_scrape_coverage(ch_str, t_id)
    cache_covers = (
        not force_refresh
        and coverage is not None
        and coverage["min_time"] <= (s_time + 43200)
        and coverage["max_time"] >= (e_time - 43200)
    )

    if cache_covers:
        messages = await asyncio.to_thread(bt_db.get_cached_messages, ch_str, t_id, s_time, e_time)
        channel_title = coverage.get("channel_title") or ch_str
    elif not force_refresh and coverage is not None and coverage["min_time"] <= (s_time + 43200) and coverage["max_time"] < (e_time - 43200):
        # Incremental tail scrape: only fetch missing recent messages from [coverage["max_time"] - 3600, e_time]
        tail_start = max(s_time, int(coverage["max_time"]) - 3600)
        try:
            new_msgs, channel_title = await _async_scrape_telegram(
                client,
                ch_str,
                t_id,
                tail_start,
                e_time,
                use_ai_fallback=use_ai_fallback,
                progress_cb=progress_cb,
                max_messages=max_messages,
            )
            await asyncio.to_thread(
                bt_db.save_cached_messages,
                ch_str,
                t_id,
                new_msgs,
                coverage["min_time"],
                e_time,
                channel_title,
            )
        except Exception:
            # If the user left the channel or Telegram entity lookup fails, gracefully fall back to existing cached history
            channel_title = coverage.get("channel_title") or ch_str
        messages = await asyncio.to_thread(bt_db.get_cached_messages, ch_str, t_id, s_time, e_time)
    else:
        messages, channel_title = await _async_scrape_telegram(
            client,
            ch_str,
            t_id,
            s_time,
            e_time,
            use_ai_fallback=use_ai_fallback,
            progress_cb=progress_cb,
            max_messages=max_messages,
        )
        await asyncio.to_thread(
            bt_db.save_cached_messages,
            ch_str,
            t_id,
            messages,
            s_time,
            e_time,
            channel_title,
        )

    ordered_signals = await asyncio.to_thread(_build_signals_from_messages, messages)
    return ordered_signals, channel_title, len(messages)


def fetch_and_build_signals(
    channel_id: str | int,
    topic_id: int,
    start_time: int,
    end_time: int,
    force_refresh: bool = False,
    use_ai_fallback: bool = False,
    progress_cb=None,
) -> tuple[list, str, int]:
    """
    Fetches messages for (channel_id, topic_id) in [start_time, end_time],
    using the local SQLite cache if already covered (and force_refresh=False),
    or scraping via Telethon otherwise.
    Returns (signals_list, channel_title, total_messages_scanned).
    """
    ch_str = str(channel_id).strip()
    t_id = int(topic_id or 0)
    s_time = int(start_time)
    e_time = int(end_time)

    coverage = bt_db.get_scrape_coverage(ch_str, t_id)
    # Allow a 12-hour tolerance at boundaries so repeated clicks on "1 Month" / "1 Year" hit cache
    cache_covers = (
        not force_refresh
        and coverage is not None
        and coverage["min_time"] <= (s_time + 43200)
        and coverage["max_time"] >= (e_time - 43200)
    )

    if cache_covers:
        if progress_cb:
            progress_cb("Loading signals from local cache...")
        messages = bt_db.get_cached_messages(ch_str, t_id, s_time, e_time)
        channel_title = coverage.get("channel_title") or ch_str
    elif not force_refresh and coverage is not None and coverage["min_time"] <= (s_time + 43200) and coverage["max_time"] < (e_time - 43200):
        if progress_cb:
            progress_cb("Syncing recent messages from Telegram...")
        tail_start = max(s_time, int(coverage["max_time"]) - 3600)

        async def _tail_runner():
            client = get_isolated_telegram_client()
            await client.connect()
            try:
                return await _async_scrape_telegram(
                    client,
                    ch_str,
                    t_id,
                    tail_start,
                    e_time,
                    use_ai_fallback=use_ai_fallback,
                    progress_cb=progress_cb,
                )
            finally:
                if client.is_connected():
                    await client.disconnect()

        try:
            new_msgs, channel_title = asyncio.run(_tail_runner())
            bt_db.save_cached_messages(ch_str, t_id, new_msgs, coverage["min_time"], e_time, channel_title=channel_title)
        except Exception:
            channel_title = coverage.get("channel_title") or ch_str
        messages = bt_db.get_cached_messages(ch_str, t_id, s_time, e_time)
    else:
        if progress_cb:
            progress_cb("Connecting to Telegram to fetch channel history...")

        # Strict Isolation: Backtester NEVER touches or locks disk session files.
        # Uses an isolated in-memory client.
        async def _standalone_runner():
            client = get_isolated_telegram_client()
            await client.connect()
            try:
                return await _async_scrape_telegram(
                    client,
                    ch_str,
                    t_id,
                    s_time,
                    e_time,
                    use_ai_fallback=use_ai_fallback,
                    progress_cb=progress_cb,
                )
            finally:
                if client.is_connected():
                    await client.disconnect()

        messages, channel_title = asyncio.run(_standalone_runner())

        # Save to SQLite cache for instant re-simulation when changing rules
        bt_db.save_cached_messages(ch_str, t_id, messages, s_time, e_time, channel_title=channel_title)

    ordered_signals = _build_signals_from_messages(messages)
    return ordered_signals, channel_title, len(messages)
