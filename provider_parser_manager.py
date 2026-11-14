"""
provider_parser_manager.py — AI-Powered Signal Profile & Regex Manager for Sirius.

Learns, synthesizes, tests, and persists custom regular expressions for each
signal provider and Telegram topic.
Provides instant 0ms regex execution for:
  1. The Strategy Backtester (parses thousands of historical messages in milliseconds)
  2. The Live Copier (executes incoming Telegram signals in < 1ms)
"""

import os
import re
import json
import time
import random
import unicodedata
import httpx
import config

PROFILES_FILE = os.path.join(os.path.dirname(__file__), "provider_parsers.json")

# In-memory cache of compiled provider regex profiles: key = (str(channel_id), int(topic_id))
_compiled_profiles_cache = {}


def load_all_profiles() -> dict:
    """Load all saved provider parsing profiles from disk."""
    if not os.path.exists(PROFILES_FILE):
        return {}
    try:
        with open(PROFILES_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[WARN] Error loading {PROFILES_FILE}: {e}")
        return {}


def save_all_profiles(profiles: dict) -> bool:
    """Persist all provider parsing profiles to disk."""
    try:
        with open(PROFILES_FILE, "w", encoding="utf-8") as f:
            json.dump(profiles, f, indent=2, ensure_ascii=False)
        _compiled_profiles_cache.clear()
        return True
    except Exception as e:
        print(f"[ERROR] Error saving {PROFILES_FILE}: {e}")
        return False


def get_profile(channel_id: str | int, topic_id: int = 0) -> dict | None:
    """Get profile by channel_id and topic_id, falling back to channel-wide profile (topic_id=0)."""
    profiles = load_all_profiles()
    s_chan = str(channel_id)
    i_topic = int(topic_id or 0)

    # 1. Exact match (channel + topic)
    key_topic = f"{s_chan}:{i_topic}"
    if key_topic in profiles:
        return profiles[key_topic]

    # 2. Channel-wide fallback (topic = 0)
    key_chan = f"{s_chan}:0"
    if key_chan in profiles:
        return profiles[key_chan]

    # 3. Direct channel key
    if s_chan in profiles:
        return profiles[s_chan]

    return None


def _compile_profile(prof: dict) -> dict:
    """Compile raw regex strings into regex objects with fail-safe error handling."""
    compiled = {"raw": prof}
    try:
        compiled["re_action"] = re.compile(prof.get("re_action") or r'\b(BUY|SELL)(?:\s+(NOW|LIMIT|STOP))?\b', re.IGNORECASE)
    except Exception:
        compiled["re_action"] = re.compile(r'\b(BUY|SELL)(?:\s+(NOW|LIMIT|STOP))?\b', re.IGNORECASE)

    try:
        compiled["re_sl"] = re.compile(prof.get("re_sl") or r'(?:Stop\s*loss|SL|Invalidation)\s*[:=@-]?\s*([0-9]+(?:\.[0-9]+)?)', re.IGNORECASE)
    except Exception:
        compiled["re_sl"] = re.compile(r'(?:Stop\s*loss|SL|Invalidation)\s*[:=@-]?\s*([0-9]+(?:\.[0-9]+)?)', re.IGNORECASE)

    try:
        compiled["re_tp"] = re.compile(prof.get("re_tp") or r'(?:TP\s*(?:#\s*[0-9]+|[¹²³⁴⁵1-5]\b)?|Target)\s*[:=@-]?\s*([0-9]+(?:\.[0-9]+)?)', re.IGNORECASE)
    except Exception:
        compiled["re_tp"] = re.compile(r'(?:TP\s*(?:#\s*[0-9]+|[¹²³⁴⁵1-5]\b)?|Target)\s*[:=@-]?\s*([0-9]+(?:\.[0-9]+)?)', re.IGNORECASE)

    try:
        compiled["re_entry"] = re.compile(prof.get("re_entry") or r'(?:ENTRY|ZONE|AT|@|FROM|AROUND|BETWEEN|NOW|LIMIT|STOP)\s*[:=@-]?\s*([0-9]+(?:\.[0-9]+)?)(?:\s*(?:[-/–]|TO)\s*([0-9]+(?:\.[0-9]+)?))?', re.IGNORECASE)
    except Exception:
        compiled["re_entry"] = re.compile(r'(?:ENTRY|ZONE|AT|@|FROM|AROUND|BETWEEN|NOW|LIMIT|STOP)\s*[:=@-]?\s*([0-9]+(?:\.[0-9]+)?)(?:\s*(?:[-/–]|TO)\s*([0-9]+(?:\.[0-9]+)?))?', re.IGNORECASE)

    default_inst_pattern = (
        r'#?\b(GOLD|XAUUSD|XAU/USD|SILVER|XAGUSD|USOUSD|OIL|WTI|BRENT|BTCUSD|BTC|ETHUSD|ETH|US30|WS30|DJ30|NAS100|USTEC|NDX|SPX500|US500|SP500|GER30|GER40|DAX|DAX40|UK100|'
        r'(?:USD|EUR|GBP|JPY|AUD|NZD|CAD|CHF|CNH|HKD|SGD|SEK|NOK|ZAR|MXN|TRY)/?'
        r'(?:USD|EUR|GBP|JPY|AUD|NZD|CAD|CHF|CNH|HKD|SGD|SEK|NOK|ZAR|MXN|TRY))\b'
    )
    try:
        compiled["re_instrument"] = re.compile(prof.get("re_instrument") or default_inst_pattern, re.IGNORECASE)
    except Exception:
        compiled["re_instrument"] = re.compile(default_inst_pattern, re.IGNORECASE)

    return compiled


def parse_with_profile(text: str, prof: dict) -> dict | None:
    """
    Parse a message using a tailored provider profile in < 1ms.
    Returns standard signal dict or None if not matching.
    """
    if not text or len(text.strip()) < 5:
        return None

    clean_text = unicodedata.normalize('NFKD', text)

    prof_key = id(prof)
    if prof_key not in _compiled_profiles_cache:
        _compiled_profiles_cache[prof_key] = _compile_profile(prof)
    c = _compiled_profiles_cache[prof_key]

    # 1. Extract Stop Loss (mandatory)
    sl_match = c["re_sl"].search(clean_text)
    if not sl_match:
        return None
    try:
        sl = float(sl_match.group(1))
    except Exception:
        return None

    # 2. Extract Take Profits
    tp_matches = list(c["re_tp"].finditer(clean_text))
    raw_tps = []
    for m in tp_matches:
        try:
            val = float(m.group(1))
            if val > 0:
                raw_tps.append(val)
        except Exception:
            continue

    # 3. Action / Direction
    direction = None
    order_type = "MARKET"
    act_match = c["re_action"].search(clean_text)
    if act_match:
        # Safeguard: if AI pattern didn't enclose in parentheses, group(0) is the entire match
        act_str = (act_match.group(1) if act_match.lastindex and act_match.lastindex >= 1 else act_match.group(0)).upper()
        if "BUY" in act_str or "LONG" in act_str:
            direction = "BUY"
        elif "SELL" in act_str or "SHORT" in act_str:
            direction = "SELL"
        if "LIMIT" in act_str:
            order_type = "LIMIT"
        elif "STOP" in act_str:
            order_type = "STOP"
        elif act_match.lastindex and act_match.lastindex >= 2 and act_match.group(2):
            ot_cand = act_match.group(2).upper()
            if ot_cand in ["LIMIT", "STOP"]:
                order_type = ot_cand

    # Mathematical inference if direction not explicitly stated
    if not direction and raw_tps:
        direction = "BUY" if raw_tps[0] > sl else "SELL"
    if not direction:
        return None

    # 4. Entry Price
    entry = None
    entry_min = None
    entry_max = None
    entry_match = c["re_entry"].search(clean_text)
    if entry_match:
        try:
            e1 = float(entry_match.group(1))
            e2_raw = entry_match.group(2) if entry_match.lastindex and entry_match.lastindex >= 2 else None
            if e2_raw:
                if len(e2_raw) <= 2:
                    prefix = int(e1) // (10 ** len(e2_raw))
                    suffix = int(e2_raw)
                    candidates = [
                        (prefix - 1) * (10 ** len(e2_raw)) + suffix,
                        prefix * (10 ** len(e2_raw)) + suffix,
                        (prefix + 1) * (10 ** len(e2_raw)) + suffix,
                    ]
                    e2 = float(min(candidates, key=lambda cand: abs(cand - e1)))
                else:
                    e2 = float(e2_raw)
                entry = round((e1 + e2) / 2.0, 5)
                entry_min = min(e1, e2)
                entry_max = max(e1, e2)
            else:
                entry = e1
                entry_min = e1
                entry_max = e1
        except Exception:
            pass

    if entry is None:
        try:
            from signal_parser import RE_ENTRY_INLINE, RE_ENTRY_EXPLICIT
            fb_match = RE_ENTRY_INLINE.search(clean_text) or RE_ENTRY_EXPLICIT.search(clean_text)
            if fb_match:
                e1 = float(fb_match.group(1))
                e2_raw = fb_match.group(2) if fb_match.lastindex and fb_match.lastindex >= 2 else None
                if e2_raw:
                    e2 = float(e2_raw)
                    entry = round((e1 + e2) / 2.0, 5)
                    entry_min = min(e1, e2)
                    entry_max = max(e1, e2)
                else:
                    entry = e1
                    entry_min = e1
                    entry_max = e1
        except Exception:
            pass

    if entry is None:
        entry = 0.0
        entry_min = None
        entry_max = None

    # 5. Instrument (Rule 1: Fail-Closed)
    instrument = None
    inst_match = c["re_instrument"].search(clean_text)
    if inst_match:
        raw_inst = inst_match.group(1).upper().replace("#", "").replace("/", "")
        if raw_inst not in {"BUY", "SELL", "LONG", "SHORT", "NOW", "LIMIT", "STOP", "MARKET", "ENTRY", "ZONE", "SL", "TP", "TP1", "TP2", "TP3", "OPEN", "HOLD"}:
            instrument = "XAUUSD" if raw_inst in ["GOLD", "XAUUSD", "XAU"] else raw_inst

    if not instrument:
        try:
            from signal_parser import RE_INST
            fb_inst = RE_INST.search(clean_text)
            if fb_inst:
                raw_inst = fb_inst.group(1).upper().replace("#", "").replace("/", "")
                instrument = "XAUUSD" if raw_inst in ["GOLD", "XAUUSD", "XAU"] else raw_inst
        except Exception:
            pass

    if not instrument:
        # Magnitude heuristics only for unmistakable commodities
        if 2000 <= sl <= 6000 or (raw_tps and 2000 <= raw_tps[0] <= 6000):
            instrument = "XAUUSD"
        elif 40 <= sl <= 150:
            instrument = "USOUSD"
        else:
            # Rule 1: Unknown instrument. Fail-closed instead of defaulting to XAUUSD.
            return None

    # Deduplicate & sanitize TP ladder with Option 1 & Option 2
    try:
        from signal_parser import sanitize_and_autocorrect_tps
        final_tps = sanitize_and_autocorrect_tps(raw_tps, direction, entry, sl, instrument)
    except Exception:
        clean_tps = []
        for t_val in raw_tps:
            if direction == "BUY" and t_val <= sl:
                continue
            if direction == "SELL" and t_val >= sl:
                continue
            clean_tps.append(t_val)
        seen = set()
        deduped = [x for x in clean_tps if not (x in seen or seen.add(x))]
        final_tps = sorted(deduped) if direction == "BUY" else sorted(deduped, reverse=True)

    return {
        "is_signal": True,
        "direction": direction,
        "order_type": order_type,
        "entry": entry,
        "entry_min": entry_min,
        "entry_max": entry_max,
        "sl": sl,
        "tp": final_tps,
        "instrument": instrument,
        "parser_source": prof.get("channel_name", "custom_profile"),
    }


def synthesize_profile_from_samples(sample_messages: list, channel_name: str = "") -> dict | None:
    """
    Send sample messages from a channel to Gemini AI to synthesize a customized regex profile.
    Automatically cleans and tests the returned pattern.
    """
    if not sample_messages:
        return None

    if not hasattr(config, "GEMINI_API_KEYS") or not config.GEMINI_API_KEYS:
        print("[ERROR] No Gemini API keys configured.")
        return None

    prompt = f"""You are an expert forex signal regex synthesizer.
Analyze these sample Telegram messages from a signal channel named '{channel_name}':
{json.dumps(sample_messages[:15], indent=2, ensure_ascii=False)}

TASK:
Synthesize Python regular expressions tailored to this provider's exact handwriting, punctuation, and layout.
Notice delimiters like @, :, -, /, zones, and whether TP has # or numbers (e.g., 'TP #1: 4108', 'Sl@4545', 'Buy now@4560').

Return a JSON object with this EXACT structure:
{{
  "channel_name": "{channel_name}",
  "notes": "brief explanation of formatting specifics",
  "re_action": "regex pattern matching BUY, SELL, BUY LIMIT, SELL LIMIT, etc.",
  "re_entry": "regex pattern capturing entry price or range",
  "re_sl": "regex pattern capturing stop loss price in group 1",
  "re_tp": "regex pattern capturing take profit price in group 1",
  "re_instrument": "regex pattern capturing symbol name or default symbols"
}}

Respond ONLY with raw valid JSON, no markdown, no backticks, no comments.
"""

    for attempt in range(6):
        api_key = random.choice(config.GEMINI_API_KEYS)
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{config.GEMINI_MODEL}:generateContent?key={api_key}"
        try:
            resp = httpx.post(
                url,
                headers={"Content-Type": "application/json"},
                json={"contents": [{"parts": [{"text": prompt}]}]},
                timeout=35.0,
            )
            if resp.status_code == 200:
                data = resp.json()
                if "candidates" in data and data["candidates"]:
                    raw_text = data["candidates"][0]["content"]["parts"][0]["text"].strip()
                    cleaned_json = raw_text.replace("```json", "").replace("```", "").strip()
                    parsed = json.loads(cleaned_json)
                    if isinstance(parsed, dict) and parsed.get("re_sl") and parsed.get("re_tp"):
                        return parsed
            else:
                time.sleep(0.5)
        except Exception:
            time.sleep(1)
            continue

    return None


def learn_and_save_provider_profile(channel_id: str | int, topic_id: int, sample_messages: list, channel_name: str = "") -> dict | None:
    """
    High-level entrypoint: Takes sample messages, synthesizes regex via Gemini,
    validates accuracy, saves to provider_parsers.json, and returns summary stats.
    """
    profile = synthesize_profile_from_samples(sample_messages, channel_name)
    if not profile:
        return None

    # Benchmark accuracy against the provided samples
    matches = 0
    test_results = []
    for msg in sample_messages:
        res = parse_with_profile(msg, profile)
        if res and res.get("is_signal"):
            matches += 1
            if len(test_results) < 5:
                test_results.append({"text": msg[:120], "parsed": res})

    profile["sample_matches"] = matches
    profile["sample_total"] = len(sample_messages)
    profile["accuracy_pct"] = round((matches / len(sample_messages)) * 100, 1) if sample_messages else 0
    profile["updated_at"] = int(time.time())

    # Save to disk
    s_chan = str(channel_id)
    i_topic = int(topic_id or 0)
    key = f"{s_chan}:{i_topic}"

    all_profs = load_all_profiles()
    all_profs[key] = profile
    save_all_profiles(all_profs)

    return {
        "key": key,
        "profile": profile,
        "matches": matches,
        "total": len(sample_messages),
        "test_results": test_results,
    }
