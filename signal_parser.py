"""
signal_parser.py — Sends Telegram messages to Gemini AI or regex engine for parsing.
Extracts direction, entry, SL, TP, and order type with strict safety guards.
"""

import json
import httpx
import config
import re
import unicodedata


def normalize_price_formatting(text: str) -> str:
    """
    Normalizes comma/whitespace thousand-separators in price numbers and splits
    joined order-type keywords (e.g. 'SELLLIMIT:51.20' -> 'SELL LIMIT:51.20')
    so downstream regex and AI extraction parse cleanly.
    Examples:
      - '73,800' -> '73800'
      - '1,897.35' -> '1897.35'
      - '4 116' -> '4116'
      - '52 680' -> '52680'
      - 'SELLLIMIT:51.20780' -> 'SELL LIMIT:51.20780'
    Leaves valid list delimiters intact (e.g. 'TP: 100, 200, 300' remains separate).
    """
    if not text:
        return text

    # 0. Split joined direction+order-type words (e.g. SELLLIMIT, BUYLIMIT, SELLSTOP, BUYSTOP)
    text = re.sub(r'\b(BUY|SELL|LONG|SHORT)(LIMIT|STOP|NOW|MARKET)\b', r'\1 \2', text, flags=re.IGNORECASE)

    # 1. Normalize comma thousand-separators in prices: 73,800 -> 73800, 1,897.35 -> 1897.35
    # Lookbehind for digit, comma, lookahead for exactly 3 digits followed by non-digit or end
    text = re.sub(r'(?<=\d),(?=\d{3}(?:[^\d]|$))', '', text)

    # 2. Normalize whitespace thousand-separators in prices: 4 116 -> 4116, 52 680 -> 52680
    # Must only match 1-3 digits followed by space and exactly 3 digits (to avoid collapsing lists of 4-digit prices)
    for _ in range(2):
        text = re.sub(r'(^|[^\d])(\d{1,3})[ \t](\d{3})(?=\.\d+|\b)', r'\1\2\3', text)

    return text


PARSE_PROMPT = """You are a forex signal parser. Analyze the following message from a Telegram forex signal channel and determine if it contains a tradeable signal.

RULES:
1. If the message is a TRADEABLE SIGNAL (contains a clear buy/sell instruction with price levels), extract the data.
2. If the message is commentary, analysis, news, trade updates (like "TP hit", "running in profit"), motivational posts, retrospective reports, or anything that is NOT a new trade instruction, return is_signal: false.
3. If the message says "close" or "close all" or "exit", that's a CLOSE signal.
4. "Buy now" or "Sell now" = market order (entry is 0, meaning use current market price)
5. "Buy limit" = pending buy below current price
6. "Sell limit" = pending sell above current price
7. "Buy stop" = pending buy above current price
8. "Sell stop" = pending sell below current price
9. If there are multiple take profits (TP1, TP2, TP3), include all of them.
10. Extract the base instrument name (e.g., EURUSD, GBPJPY, XAUUSD, BTCUSD).
11. If Stop Loss (SL) is missing or 0, return is_signal: false (No Stop Loss, No Trade rule).

Respond ONLY with valid JSON, no markdown, no backticks, no explanation. Just the raw JSON object.

If it IS a signal:
{"is_signal": true, "direction": "BUY" or "SELL", "order_type": "MARKET" or "LIMIT" or "STOP", "entry": 4820.00, "sl": 4800.00, "tp": [4840.00, 4860.00], "instrument": "XAUUSD"}

If it's a CLOSE signal:
{"is_signal": true, "direction": "CLOSE", "order_type": "CLOSE", "entry": 0, "sl": 0, "tp": [], "instrument": "XAUUSD"}

If it is NOT a signal:
{"is_signal": false}

Here is the message to analyze:

"""

# Regex components for universal signal extraction
RE_CLEAN = re.compile(r'[\*`~_\ufe0f]')
RE_SL = re.compile(r'(?:Stop\s*loss|SL|Invalidation|Stop)\s*(?:\([^)]*\))?[\s:=@➡➤►•-]*([0-9]+(?:\.[0-9]+)?)', re.IGNORECASE)
RE_TP = re.compile(
    r'(?:TP|Target(?:\s*Level)?|Take\s*profit(?:\s*Level)?|\bT(?=\s*[1-9]\b))s?\s*(?:[#-]?\s*([¹²³⁴⁵1-9])\b)?\s*(?:\([^)]*\))?[ \t:=@➡➤►👉📍🎯•-]*(?:\r?\n[ \t]*)?'
    r'(?![0-9]\b\s*(?:[:=@➡➤►👉📍•-]|\(|open\b|hold\b|runner\b|max\b|tbd\b|pips?\b))'
    r'([0-9]+(?:\.[0-9]+)?)(?:[ \t]*(?:[-/–_]|TO)[ \t]*([0-9]+(?:\.[0-9]+)?))?',
    re.IGNORECASE
)
RE_TP_MULTI_SPACE = re.compile(
    r'^\s*(?:[^\w\n]*\s*)?(?:TP|Target|Take\s*profit)s?\b(?![\s#-]*[2-9]\b)[\s:=@➡➤►•-]+'
    r'([0-9]+(?:\.[0-9]+)?(?:[ \t]+(?:[-/]\s*)?[0-9]+(?:\.[0-9]+)?)+)',
    re.IGNORECASE | re.MULTILINE
)

# Universal Forex, Commodity, Index, and Crypto Symbol matching
FOREX_CURRENCIES = r'(?:USD|EUR|GBP|JPY|AUD|NZD|CAD|CHF|CNH|HKD|SGD|SEK|NOK|ZAR|MXN|TRY)'
FOREX_PAIR = rf'{FOREX_CURRENCIES}/?{FOREX_CURRENCIES}'
KNOWN_SYMBOLS = (
    r'GOLD|XAUUSD|XAU/USD|SILVER|XAGUSD|USOUSD|OIL|WTI|CRUDE|BRENT|UKOIL|USOIL|'
    r'BTCUSD|BTC|BITCOIN|ETHUSD|ETH|ETHEREUM|SOLUSD|SOL|SOLANA|XRPUSD|XRP|RIPPLE|'
    r'DOGEUSD|DOGE|DOGECOIN|ADAUSD|ADA|CARDANO|DOTUSD|DOT|POLKADOT|MATICUSD|MATIC|'
    r'POLYGON|POL|LTCUSD|LTC|LITECOIN|LINKUSD|LINK|CHAINLINK|BCHUSD|BCH|XLMUSD|XLM|'
    r'BNBUSD|BNB|AVAXUSD|AVAX|EOSUSD|EOS|KSMUSD|KSM|'
    r'US30|WS30|DJ30|DOW|NAS100|USTEC|NDX|NQ|NASDAQ|UT100|'
    r'SPX500|US500|SP500|SPX|GER30|GER40|DAX|DAX40|DE30|DE40|UK100|FTSE100|'
    r'JP225|NIKKEI|EU50|AU200|CN50'
)
RE_INST = re.compile(rf'#?\b({KNOWN_SYMBOLS}|{FOREX_PAIR})\b', re.IGNORECASE)
RE_ACTION = re.compile(r'\b(BUY(?:ING|S)?|LONG|SELL(?:ING|S)?|SHORT)(?:\s+(NOW|LIMIT|STOP))?\b', re.IGNORECASE)
RE_ORDER_TYPE = re.compile(r'\b(LIMIT|(?<!TRAILING\s)STOP(?!\s*LOSS\b)|NOW|MARKET)\b', re.IGNORECASE)

# Flexible entry matching (ranges like 3996-3998, 4085-90, 4056_4050, 4672/4666, or single level)
RE_ENTRY_EXPLICIT = re.compile(
    r'\b(?:ENTRY(?:\s*(?:PRICE|ZONE|POINT|LEVEL))?|ZONE|PE|OPEN\s*PRICE|(?<!Target\s)(?<!Stop\s)(?<!TP\s)(?<!SL\s)LEVEL)\b'
    r'[ \t:=@➡➤►👉📍🎯•-]*'
    r'(?:(?:\(?NOW\)?|\(?MARKET\)?)[ \t:=@➡➤►👉📍🎯•-]*)?'
    r'([0-9]+(?:\.[0-9]+)?)(?:[ \t]*(?:[-/–_]|TO)[ \t]*([0-9]+(?:\.[0-9]+)?))?',
    re.IGNORECASE
)
RE_ENTRY_INLINE = re.compile(
    r'\b(?:BUY(?:ING|S)?|LONG|SELL(?:ING|S)?|SHORT)\b'
    r'(?:[ \t]+(?!(?:TP[0-9¹²³⁴⁵]*|SL|STOP\s*LOSS|TARGET|TAKE\s*PROFIT)\b)(?:NOW|LIMIT|STOP|MARKET|HERE|ZONE|AT|FROM|AROUND|#[A-Z0-9_]+|[A-Z]{2,10}(?:/[A-Z]{3})?))*'
    r'[ \t:=@➡➤►👉📍🎯•-]*'
    r'([0-9]+(?:\.[0-9]+)?)(?:[ \t]*(?:[-/–_]|TO)[ \t]*([0-9]+(?:\.[0-9]+)?))?',
    re.IGNORECASE
)
RE_ENTRY_AT = re.compile(
    r'(?<!\bTP)(?<!\bTP1)(?<!\bTP2)(?<!\bTP3)(?<!\bTP4)(?<!\bTP5)(?<!\bSL)(?<!\bSTOP)'
    r'(?:@|\b(?:AT|FROM|AROUND|BETWEEN)\b)'
    r'[ \t:=@➡➤►👉📍🎯•-]*'
    r'([0-9]+(?:\.[0-9]+)?)(?:[ \t]*(?:[-/–_]|TO)[ \t]*([0-9]+(?:\.[0-9]+)?))?',
    re.IGNORECASE
)
RE_CLOSE_COMMAND = re.compile(r'\b(close\s+now|close\s+all|close\s+full|close\s+trade|close\s+position|exit\s+now)\b', re.IGNORECASE)


def is_performance_recap(message_text: str) -> tuple[bool, str]:
    """
    Detects retrospective performance recaps, daily/weekly summary reports, and result posts.
    Returns (True, reason) if the message is a report/recap and not a new trade instruction.
    """
    if not message_text or len(message_text.strip()) < 10:
        return False, ""
    lower = message_text.lower()

    # 1. Header keywords indicating a retrospective report/summary
    header_patterns = [
        r'\b(?:daily|weekly|monthly|session|today(?:[\'’]s)?)\s+(?:recap|report|results|summary|performance)\b',
        r'\b(?:premium|vip)\s+(?:recap|report|results|summary)\b',
        r'\b(?:recap|report|summary)\s+(?:of\s+the\s+day|of\s+the\s+week)\b',
        r'\btotal\s+(?:pips|profit|gain|loss)\b',
        r'\bnet\s+(?:pips|profit)\b',
        r'\btrade\s+recap\b',
    ]
    for hp in header_patterns:
        if re.search(hp, lower):
            return True, "Daily/Weekly performance recap report detected"

    # 2. Retrospective result lines with outcome emojis (e.g. 'TP1 +20 Pips✅', 'SL -20 Pips❌')
    recap_lines = re.findall(r'(?:tp\d*|sl|target)\s*[+-]?\d+\s*(?:[x×]\d+)?\s*pips?\s*[✅❌✔✕🏆]', lower)
    if len(recap_lines) >= 1:
        return True, "Past trade results / pips report detected"

    return False, ""




def sanitize_and_autocorrect_tps(
    raw_tps: list[float],
    direction: str,
    entry: float | None,
    sl: float | None,
    instrument: str = "XAUUSD"
) -> list[float]:
    """
    Sanitizes and auto-corrects Take Profit levels:
      1. Preserves valid, properly proportioned TPs.
      2. If a TP is a typo (e.g. missing a digit like '490' or '409' instead of '4090'),
         attempts Option 1: Reconstruct by inserting any digit 0-9 at any position,
         and picks the candidate that aligns with direction, reasonable distance (< 10%),
         and monotonic ladder progression.
      3. If Option 1 cannot produce a reasonable level (e.g. '50' or '99999'),
         falls back to Option 2: Extrapolates based on previous TP step / R:R,
         or safely drops the outlier.
    """
    if not raw_tps or not direction:
        return []

    direction = direction.upper()
    sl_val = float(sl or 0.0)
    ent_val = float(entry or 0.0)

    # Determine reference price for magnitude / proximity checks
    if ent_val > 0:
        ref_price = ent_val
    elif sl_val > 0:
        ref_price = sl_val
    elif any(t > 0 for t in raw_tps):
        ref_price = [t for t in raw_tps if t > 0][0]
    else:
        ref_price = 0.0

    MAX_TP_DISTANCE_PCT = 0.10  # 10% maximum distance from reference price

    def is_tp_valid_and_reasonable(tp: float, prev_tps: list[float]) -> bool:
        if tp <= 0:
            return False
        # Direction check against SL
        if sl_val > 0:
            if direction == "BUY" and tp <= sl_val:
                return False
            if direction == "SELL" and tp >= sl_val:
                return False
        # Direction check against Entry
        if ent_val > 0:
            if direction == "BUY" and tp < ent_val * 0.998:
                return False
            if direction == "SELL" and tp > ent_val * 1.002:
                return False
        # Proximity check
        if ref_price > 0:
            if abs(tp - ref_price) / ref_price > MAX_TP_DISTANCE_PCT:
                return False
        # Monotonicity check against previous TPs
        if prev_tps:
            last_tp = prev_tps[-1]
            if direction == "BUY" and tp <= last_tp:
                return False
            if direction == "SELL" and tp >= last_tp:
                return False
        return True

    def score_candidate(cand: float, expected: float, prev_tps: list[float]) -> float:
        """Score candidate for Option 1; lower penalty is better."""
        penalty = abs(cand - expected)

        # Financial roundness preference (ending in 0 or 5 is standard in trading signals)
        if ref_price >= 100:
            cand_int = int(round(cand))
            if abs(cand - cand_int) < 1e-4:
                if cand_int % 10 == 0:
                    penalty -= 20.0  # Big bonus for round numbers ending in 0 (e.g. 4090, 75000)
                elif cand_int % 5 == 0:
                    penalty -= 10.0  # Bonus for numbers ending in 5 (e.g. 4095)
                else:
                    penalty += 15.0  # Penalty for non-round numbers (e.g. 4099)
        elif ref_price > 0:
            # Forex/Oil: pip-level roundness
            pip_unit = 0.01 if "JPY" in (instrument or "").upper() else 0.0001
            pips = round(cand / pip_unit)
            if pips % 10 == 0:
                penalty -= 20.0
            elif pips % 5 == 0:
                penalty -= 10.0

        # Step alignment with previous TPs in ladder
        if prev_tps and len(prev_tps) >= 2:
            prev_step = abs(prev_tps[-1] - prev_tps[-2])
            if prev_step > 0:
                step_from_last = abs(cand - prev_tps[-1])
                rem = step_from_last % prev_step
                rem_dist = min(rem, prev_step - rem)
                penalty += rem_dist * 5.0

        return penalty

    def try_option_1_digit_insertion(t_val: float, prev_tps: list[float]) -> float | None:
        """Option 1: Try inserting any digit 0-9 at any position to reconstruct typo."""
        s = str(int(round(t_val))) if abs(t_val - round(t_val)) < 1e-4 else str(t_val)
        dec_part = ("." + s.split(".")[1]) if "." in s else ""
        int_part = s.split(".")[0]

        candidates = []
        for pos in range(len(int_part) + 1):
            for digit in "0123456789":
                cand_int = int_part[:pos] + digit + int_part[pos:]
                if cand_int.startswith("0") and len(cand_int) > 1:
                    continue
                try:
                    cand_val = float(cand_int + dec_part)
                    candidates.append(cand_val)
                except ValueError:
                    continue

        # Filter candidates that satisfy direction, SL, entry, and proximity
        valid_cands = []
        for c in set(candidates):
            if is_tp_valid_and_reasonable(c, prev_tps):
                valid_cands.append(c)

        if not valid_cands:
            return None

        # Determine expected TP level to pick best candidate
        if prev_tps:
            if len(prev_tps) >= 2:
                step = abs(prev_tps[-1] - prev_tps[-2])
            else:
                step = abs(prev_tps[-1] - (ent_val or ref_price))
            expected = prev_tps[-1] + step if direction == "BUY" else prev_tps[-1] - step
        else:
            if sl_val > 0 and ent_val > 0:
                expected = ent_val + abs(ent_val - sl_val) if direction == "BUY" else ent_val - abs(ent_val - sl_val)
            else:
                expected = ref_price

        # Sort valid candidates by score
        valid_cands.sort(key=lambda c: score_candidate(c, expected, prev_tps))
        return valid_cands[0]

    def try_option_2_extrapolate(prev_tps: list[float]) -> float | None:
        """Option 2: Fall back to extrapolating from previous TP step or R:R."""
        if prev_tps:
            if len(prev_tps) >= 2:
                step = abs(prev_tps[-1] - prev_tps[-2])
            else:
                step = abs(prev_tps[-1] - (ent_val or ref_price))

            # Sanity cap on step: max 3% of price
            if step <= 0 or (ref_price > 0 and step / ref_price > 0.03):
                if sl_val > 0 and ent_val > 0:
                    step = abs(ent_val - sl_val)
                elif ref_price > 0:
                    step = ref_price * 0.005  # 0.5% default step
                else:
                    step = 5.0

            cand = round(prev_tps[-1] + step, 5) if direction == "BUY" else round(prev_tps[-1] - step, 5)
            if is_tp_valid_and_reasonable(cand, prev_tps):
                return cand
        elif sl_val > 0 and ent_val > 0:
            step = abs(ent_val - sl_val)
            if step > 0 and (ref_price > 0 and step / ref_price <= 0.05):
                cand = round(ent_val + step, 5) if direction == "BUY" else round(ent_val - step, 5)
                if is_tp_valid_and_reasonable(cand, prev_tps):
                    return cand

        return None

    # Process each TP in sequence
    corrected_tps: list[float] = []
    for idx, t_val in enumerate(raw_tps):
        # 1. Check if already completely valid & reasonable
        if is_tp_valid_and_reasonable(t_val, corrected_tps):
            corrected_tps.append(round(t_val, 5))
            continue

        # 2. Try Option 1: Reconstruct typo with digit insertion (0-9)
        opt1_cand = try_option_1_digit_insertion(t_val, corrected_tps)
        if opt1_cand is not None:
            print(f"[PARSER-AUTOFIX] TP tier {idx+1} typo '{t_val}' reconstructed via Option 1 (digits 0-9) -> {opt1_cand}")
            corrected_tps.append(round(opt1_cand, 5))
            continue

        # 3. Default to Option 2: Extrapolate step or drop
        opt2_cand = try_option_2_extrapolate(corrected_tps)
        if opt2_cand is not None:
            print(f"[PARSER-AUTOFIX] TP tier {idx+1} typo '{t_val}' unfixable via Option 1. Defaulted to Option 2 extrapolated level -> {opt2_cand}")
            corrected_tps.append(round(opt2_cand, 5))
        else:
            print(f"[PARSER-AUTOFIX] TP tier {idx+1} typo '{t_val}' is invalid and could not be extrapolated. Dropped.")

    # Final sort and deduplicate
    seen = set()
    deduped = [x for x in corrected_tps if not (x in seen or seen.add(x))]
    final_tps = sorted(deduped) if direction == "BUY" else sorted(deduped, reverse=True)
    return final_tps


def _autofix_wrong_side_sl(
    direction: str,
    entry: float,
    entry_min: float | None,
    entry_max: float | None,
    sl: float,
    tp_list: list[float],
    digits: int = 5,
) -> dict:
    """
    Replaces repeated +-10 subtraction with single-digit typo mutation + distance sanity bounds.
    Only acts when SL sits on the wrong side of Entry while ALL TPs clearly agree on direction.
    Returns dict: {"status": "OK"|"FIXED"|"UNFIXABLE", "sl": float, "orig_sl": float, "reason": str}
    """
    valid_tps = [float(t) for t in (tp_list or []) if t and float(t) > 0]
    if not (entry > 0 and sl > 0 and valid_tps):
        return {"status": "OK", "sl": sl, "orig_sl": sl, "reason": ""}

    ref_lo = float(entry_min) if entry_min is not None else entry
    ref_hi = float(entry_max) if entry_max is not None else entry

    is_wrong_buy = (direction == "BUY" and sl >= ref_lo and all(t > ref_hi for t in valid_tps))
    is_wrong_sell = (direction == "SELL" and sl <= ref_hi and all(t < ref_lo for t in valid_tps))
    if not (is_wrong_buy or is_wrong_sell):
        return {"status": "OK", "sl": sl, "orig_sl": sl, "reason": ""}

    tp1_dist = abs(valid_tps[0] - entry)
    tp_last_dist = abs(valid_tps[-1] - entry)
    # Require auto-corrected SL to have at least 0.1% of entry or 40% of TP1 distance
    min_sl_dist = max(entry * 0.001, tp1_dist * 0.4)
    max_sl_dist = max(entry * 0.05, tp_last_dist * 3.0)
    target_dist = max(tp_last_dist, tp1_dist * 2.0, min_sl_dist * 2.0)

    sl_str = f"{sl:.{max(digits, 5)}f}".rstrip("0").rstrip(".") or "0"
    candidates = []
    for idx, ch in enumerate(sl_str):
        if not ch.isdigit():
            continue
        for rep in "0123456789":
            if rep == ch:
                continue
            cand_s = sl_str[:idx] + rep + sl_str[idx + 1:]
            try:
                cand_val = float(cand_s)
            except Exception:
                continue
            if cand_val <= 0:
                continue
            if is_wrong_buy and cand_val >= ref_lo:
                continue
            if is_wrong_sell and cand_val <= ref_hi:
                continue
            dist = abs(entry - cand_val)
            if min_sl_dist <= dist <= max_sl_dist:
                candidates.append(cand_val)

    if not candidates:
        reason = (
            f"SL_TYPO_UNFIXABLE: {direction} SL {sl} sits on wrong side of entry {entry} "
            f"and no single-digit fix meets min distance {min_sl_dist:.4g}."
        )
        return {"status": "UNFIXABLE", "sl": sl, "orig_sl": sl, "reason": reason}

    best = min(candidates, key=lambda c: abs(abs(entry - c) - target_dist))
    best = round(best, digits)
    return {
        "status": "FIXED",
        "sl": best,
        "orig_sl": sl,
        "reason": f"SL typo fixed via single-digit correction: {sl} -> {best}",
    }


def normalize_and_autofix_signal(signal: dict, live_bid: float, live_ask: float, digits: int = 2) -> dict:
    """
    Deep-copies a signal dictionary and applies consistent price normalization and
    typo auto-fixes against live broker market prices across all platforms (MT5, cTrader, MT4):
      1. Auto-corrects direction if SL and TP levels clearly indicate the opposite direction.
      2. Auto-corrects single-digit SL typo when Entry and all TPs agree on direction.
      3. Auto-corrects hundreds/thousands digit typos (e.g. Entry 4072.5 when market is 3968.96),
         consistently shifting entry, entry_min, entry_max, sl, and tp levels.
    """
    import copy
    sig = copy.deepcopy(signal) if isinstance(signal, dict) else {}
    direction = str(sig.get("direction", "") or "").upper().strip()
    if "BUY" in direction or "LONG" in direction:
        direction = "BUY"
    elif "SELL" in direction or "SHORT" in direction:
        direction = "SELL"
    sig["direction"] = direction

    entry = float(sig.get("entry") or 0.0)
    sl = float(sig.get("sl") or 0.0)
    raw_tp = sig.get("tp")
    if isinstance(raw_tp, list):
        tp_list = [float(x) for x in raw_tp if x is not None and float(x) > 0]
    else:
        tp_list = [
            float(sig[k]) for k in ("tp1", "tp2", "tp3")
            if sig.get(k) is not None and float(sig.get(k) or 0) > 0
        ]

    # 1. Auto-correct single-digit SL typo when Entry and all TPs clearly agree on direction
    if entry > 0 and sl > 0 and tp_list and tp_list[0] > 0:
        sl_fix = _autofix_wrong_side_sl(
            direction, entry, sig.get("entry_min"), sig.get("entry_max"), sl, tp_list, digits=max(digits, 5)
        )
        if sl_fix["status"] == "FIXED":
            orig_sl = sl
            sl = round(sl_fix["sl"], digits)
            sig["sl"] = sl
            sig["sl_original"] = orig_sl
            sig["sl_autofix_note"] = sl_fix["reason"]
            print(f"[AUTO-FIX] Corrected {direction} SL typo from {orig_sl} to {sl} (Entry={entry}, TP1={tp_list[0]})")

    # 2. Auto-correct direction only if SL and TP levels BOTH clearly indicate the opposite direction
    if sl > 0 and tp_list and tp_list[0] > 0:
        if sl < tp_list[0] and (entry <= 0 or (sl < entry and all(t > entry for t in tp_list))) and direction == "SELL":
            print(f"[AUTO-FIX] Signal direction corrected from SELL to BUY based on SL ({sl}) < TP1 ({tp_list[0]})")
            direction = "BUY"
            sig["direction"] = "BUY"
        elif sl > tp_list[0] and (entry <= 0 or (sl > entry and all(t < entry for t in tp_list))) and direction == "BUY":
            print(f"[AUTO-FIX] Signal direction corrected from BUY to SELL based on SL ({sl}) > TP1 ({tp_list[0]})")
            direction = "SELL"
            sig["direction"] = "SELL"

    # 3. Auto-correct hundreds/thousands digit typos (e.g. provider writes 4070/75 when market is 3968.96)
    live_mid = (float(live_ask or 0.0) + float(live_bid or 0.0)) / 2.0
    if entry > 0 and live_mid > 500 and abs(entry - live_mid) >= 50:
        shift = round((live_mid - entry) / 100.0) * 100.0
        if shift != 0 and abs((entry + shift) - live_mid) <= 25:
            print(f"[AUTO-FIX] Detected {shift:+.0f} digit typo in signal (Entry {entry} vs Market {live_mid:.2f}). Shifting levels by {shift:+.0f}.")
            entry = round(entry + shift, digits)
            sig["entry"] = entry
            if sig.get("entry_min") is not None and float(sig.get("entry_min") or 0) > 0:
                sig["entry_min"] = round(float(sig["entry_min"]) + shift, digits)
            if sig.get("entry_max") is not None and float(sig.get("entry_max") or 0) > 0:
                sig["entry_max"] = round(float(sig["entry_max"]) + shift, digits)
            if sl > 0 and abs((sl + shift) - entry) < 50:
                sl = round(sl + shift, digits)
                sig["sl"] = sl
            tp_list = [round(tp + shift, digits) if (tp > 0 and abs((tp + shift) - entry) < 100) else tp for tp in tp_list]
            sig["tp"] = tp_list
    elif entry <= 0 and sl > 0 and live_mid > 500 and abs(sl - live_mid) >= 60:
        shift = round((live_mid - sl) / 100.0) * 100.0
        if shift != 0 and abs((sl + shift) - live_mid) <= 35:
            print(f"[AUTO-FIX] Detected {shift:+.0f} digit typo in market-entry signal (SL {sl} vs Market {live_mid:.2f}). Shifting SL/TP by {shift:+.0f}.")
            sl = round(sl + shift, digits)
            sig["sl"] = sl
            tp_list = [round(tp + shift, digits) if (tp > 0 and abs((tp + shift) - live_mid) < 100) else tp for tp in tp_list]
            sig["tp"] = tp_list

    sig["entry"] = entry
    sig["sl"] = sl
    sig["tp"] = tp_list
    if tp_list:
        sig["tp1"] = tp_list[0] if len(tp_list) > 0 else None
        sig["tp2"] = tp_list[1] if len(tp_list) > 1 else None
        sig["tp3"] = tp_list[2] if len(tp_list) > 2 else None
    return sig



def parse_with_regex(message_text: str) -> dict | None:
    """
    High-velocity dynamic regex signal extraction.
    Identifies trade signals across any natural phrasing (standard, conversational, or shorthand)
    using structural price invariants, semantic tokens, and mathematical consistency.
    """
    if not message_text or len(message_text.strip()) < 5:
        return None

    # Safety Guard: Filter out performance recap/reports before parsing
    is_recap, _ = is_performance_recap(message_text)
    if is_recap:
        return None

    # Normalize unicode characters (NFKD converts mathematical formatting, bold text, non-breaking spaces)
    norm_text = unicodedata.normalize('NFKD', message_text)
    # Normalize price formatting (thousands comma and whitespace separators)
    norm_text = normalize_price_formatting(norm_text)
    # Normalize unicode dashes (em-dash, en-dash, minus) to standard hyphen
    norm_text = re.sub(r'[\u2010-\u2015\u2212]', '-', norm_text)
    # Pre-process underscores between digits into standard hyphen ranges (e.g. 4056_4050 -> 4056-4050)
    norm_text = re.sub(r'(\d)_+(\d)', r'\1-\2', norm_text)
    clean_text = RE_CLEAN.sub('', norm_text)

    # 1. Check for explicit standalone CLOSE signals
    if len(clean_text) < 120 and RE_CLOSE_COMMAND.search(clean_text):
        if not re.search(r'\b(market|daily|weekly|monthly|candle|bar|partial|half)\s+close\b', clean_text, re.IGNORECASE):
            inst_match = RE_INST.search(clean_text)
            inst = inst_match.group(1).upper().replace('#', '').replace('/', '') if inst_match else "XAUUSD"
            return {
                "is_signal": True,
                "direction": "CLOSE",
                "order_type": "CLOSE",
                "entry": 0.0,
                "sl": 0.0,
                "tp": [],
                "instrument": "XAUUSD" if inst in ["GOLD", "XAUUSD"] else inst
            }

    # 2. Extract Structural Invariants (Stop Loss & Take Profits)
    sl_match = RE_SL.search(clean_text)
    tp_matches = list(RE_TP.finditer(clean_text))

    if not sl_match or not tp_matches:
        return None

    sl = float(sl_match.group(1))
    # Safety Rule 3: No Stop Loss, No Trade. SL must be greater than zero.
    if sl <= 0:
        return None

    raw_tps = []
    # Check for single-line space-separated TPs first (e.g. "TP 4113 4111 4107 OPEN")
    multi_tp_match = RE_TP_MULTI_SPACE.search(clean_text)
    if multi_tp_match and len(tp_matches) == 1:
        nums = re.findall(r'[0-9]+(?:\.[0-9]+)?', multi_tp_match.group(1))
        for n_str in nums:
            n_val = float(n_str)
            if sl > 15 and "." not in n_str and 1 <= n_val <= 9:
                continue
            raw_tps.append(n_val)

    if not raw_tps:
        for m in tp_matches:
            val_str = m.group(2)
            if not val_str:
                continue
            e1 = float(val_str)
            # Guard against bare tier number (1..9) matched as a TP price
            if sl > 15 and "." not in val_str and 1 <= e1 <= 9:
                continue
            raw_tps.append(e1)
            if m.group(3):
                e2_raw = m.group(3)
                if len(e2_raw) <= 2:
                    prefix = int(e1) // (10 ** len(e2_raw))
                    suffix = int(e2_raw)
                    candidates = [
                        (prefix - 1) * (10 ** len(e2_raw)) + suffix,
                        prefix * (10 ** len(e2_raw)) + suffix,
                        (prefix + 1) * (10 ** len(e2_raw)) + suffix,
                    ]
                    e2 = float(min(candidates, key=lambda c: abs(c - e1)))
                else:
                    e2 = float(e2_raw)
                raw_tps.append(e2)

    if not raw_tps:
        return None

    # 3. Detect Action / Direction with Mathematical Invariant Validation
    direction = None
    action_match = RE_ACTION.search(clean_text)
    if action_match:
        act_str = action_match.group(1).upper()
        if act_str.startswith('BUY') or act_str == 'LONG':
            direction = 'BUY'
        elif act_str.startswith('SELL') or act_str == 'SHORT':
            direction = 'SELL'

    tp_first = raw_tps[0]
    if direction is None:
        # Infer direction directly from price levels: TP > SL means BUY, TP < SL means SELL
        if tp_first > sl:
            direction = 'BUY'
        elif tp_first < sl:
            direction = 'SELL'
        else:
            return None

    # 4. Resolve Instrument (Rule 1: Fail-Closed)
    inst_match = RE_INST.search(clean_text)
    if inst_match:
        raw_inst = inst_match.group(1).upper().replace('#', '').replace('/', '')
        if raw_inst in ['GOLD', 'XAUUSD', 'XAU']:
            instrument = 'XAUUSD'
        elif raw_inst in ['OIL', 'USOUSD', 'WTI', 'CRUDE', 'USOIL']:
            instrument = 'USOUSD'
        elif raw_inst in ['BRENT', 'UKOIL']:
            instrument = 'UKOIL'
        elif raw_inst in ['BTC', 'BITCOIN', 'BTCUSD']:
            instrument = 'BTCUSD'
        elif raw_inst in ['ETH', 'ETHEREUM', 'ETHUSD']:
            instrument = 'ETHUSD'
        elif raw_inst in ['SOL', 'SOLANA', 'SOLUSD']:
            instrument = 'SOLUSD'
        elif raw_inst in ['XRP', 'RIPPLE', 'XRPUSD']:
            instrument = 'XRPUSD'
        elif raw_inst in ['DOGE', 'DOGECOIN', 'DOGEUSD']:
            instrument = 'DOGEUSD'
        elif raw_inst in ['ADA', 'CARDANO', 'ADAUSD']:
            instrument = 'ADAUSD'
        elif raw_inst in ['DOT', 'POLKADOT', 'DOTUSD']:
            instrument = 'DOTUSD'
        elif raw_inst in ['MATIC', 'POLYGON', 'POL', 'MATICUSD']:
            instrument = 'MATICUSD'
        elif raw_inst in ['LTC', 'LITECOIN', 'LTCUSD']:
            instrument = 'LTCUSD'
        elif raw_inst in ['LINK', 'CHAINLINK', 'LINKUSD']:
            instrument = 'LINKUSD'
        elif raw_inst in ['BCH', 'BITCOINCASH', 'BCHUSD']:
            instrument = 'BCHUSD'
        elif raw_inst in ['NAS100', 'NQ', 'USTEC', 'NDX', 'NASDAQ', 'UT100']:
            instrument = 'NAS100'
        elif raw_inst in ['US30', 'DJ30', 'WS30', 'DOW']:
            instrument = 'US30'
        elif raw_inst in ['US500', 'SPX500', 'SP500', 'SPX']:
            instrument = 'US500'
        elif raw_inst in ['GER40', 'GER30', 'DAX', 'DE30']:
            instrument = 'GER40'
        else:
            instrument = raw_inst
    else:
        # Contextual inference only from high-confidence price magnitude
        if 2000 <= sl <= 6000 or (raw_tps and 2000 <= raw_tps[0] <= 6000):
            instrument = 'XAUUSD'
        else:
            # Rule 1: Cannot resolve symbol with certainty. Fail-closed instead of guessing USOUSD or XAUUSD.
            return None

    # 5. Extract Entry Price
    entry = 0.0
    entry_min = None
    entry_max = None
    entry_match = RE_ENTRY_EXPLICIT.search(clean_text)
    if not entry_match:
        entry_match = RE_ENTRY_INLINE.search(clean_text)
    if not entry_match:
        entry_match = RE_ENTRY_AT.search(clean_text)

    if entry_match:
        e1 = float(entry_match.group(1))
        e2_raw = entry_match.group(2)
        if e2_raw:
            if len(e2_raw) <= 2 and "." not in str(entry_match.group(1)):
                # Shorthand range notation (e.g. 4085-90, 4360/22, 4120-15)
                prefix = int(e1) // (10 ** len(e2_raw))
                suffix = int(e2_raw)
                candidates = [
                    (prefix - 1) * (10 ** len(e2_raw)) + suffix,
                    prefix * (10 ** len(e2_raw)) + suffix,
                    (prefix + 1) * (10 ** len(e2_raw)) + suffix,
                ]
                e2 = float(min(candidates, key=lambda c: abs(c - e1)))
            else:
                e2 = float(e2_raw)
            entry = round((e1 + e2) / 2.0, 5)
            entry_min = min(e1, e2)
            entry_max = max(e1, e2)
        else:
            entry = e1
            entry_min = e1
            entry_max = e1

    # 5b. Validate SL vs Entry & TPs (Single-digit typo fix + minimum-distance guard)
    sl_original = None
    sl_autofix_note = None
    sl_fix = _autofix_wrong_side_sl(direction, entry, entry_min, entry_max, sl, raw_tps, digits=5)
    if sl_fix["status"] == "FIXED":
        sl_original = sl
        sl = sl_fix["sl"]
        sl_autofix_note = sl_fix["reason"]
    elif sl_fix["status"] == "UNFIXABLE":
        return {
            "is_signal": False,
            "error_code": "SL_TYPO_UNFIXABLE",
            "error_reason": sl_fix["reason"],
            "instrument": instrument,
            "direction": direction,
            "entry": entry,
            "sl": sl,
            "tp": raw_tps,
        }

    # Cross-validate direction against final Entry, SL, and TPs
    if entry > 0 and entry_min is not None and entry_max is not None and len(raw_tps) >= 2 and all(t > entry_max for t in raw_tps) and sl < entry_min:
        direction = 'BUY'
    elif entry > 0 and entry_min is not None and entry_max is not None and len(raw_tps) >= 2 and all(t < entry_min for t in raw_tps) and sl > entry_max:
        direction = 'SELL'
    elif sl < tp_first and direction == 'SELL' and (entry <= 0 or sl < entry):
        direction = 'BUY'
    elif sl > tp_first and direction == 'BUY' and (entry <= 0 or sl > entry):
        direction = 'SELL'

    # 6. Detect Order Type
    order_type = 'MARKET'
    ot_match = RE_ORDER_TYPE.search(clean_text)
    if ot_match:
        ot_str = ot_match.group(1).upper()
        if ot_str in ['LIMIT', 'STOP']:
            order_type = ot_str

    # 7. Deduplicate & Sanitize Take Profit Array with Option 1 (0-9) & Option 2 (Extrapolate/Drop)
    final_tps = sanitize_and_autocorrect_tps(raw_tps, direction, entry, sl, instrument)
    if not final_tps:
        final_tps = [tp_first]

    res_out = {
        "is_signal": True,
        "direction": direction,
        "order_type": order_type,
        "entry": entry,
        "entry_min": entry_min,
        "entry_max": entry_max,
        "sl": sl,
        "tp": final_tps,
        "instrument": instrument
    }
    if sl_original is not None:
        res_out["sl_original"] = sl_original
        res_out["sl_autofix_note"] = sl_autofix_note
    return res_out


def parse_signal(message_text: str, channel_id: str | int = None, topic_id: int = 0) -> dict | None:
    """
    Parse a signal:
      1. Safety check for performance recap/summary reports (skips immediately).
      2. Checks learned provider-specific profile from provider_parsers.json (0ms execution).
      3. Falls back to fast universal Regex (0ms execution).
      4. Falls back to Gemini AI if message contains trade structure.
    Returns the parsed signal dict or None if parsing fails.
    """
    if not message_text or not message_text.strip():
        return None

    # Pre-process numbers (thousands comma and whitespace separators)
    message_text = normalize_price_formatting(message_text)

    # Safety Guard: Pre-flight check for retrospective reports/summaries
    is_recap, reason = is_performance_recap(message_text)
    if is_recap:
        print(f"[PARSER] Blocked retrospective message: {reason}")
        return None

    # Step 1: Try learned provider profile if channel/topic provided
    if channel_id:
        try:
            from provider_parser_manager import get_profile, parse_with_profile
            prof = get_profile(channel_id, topic_id)
            if prof:
                prof_res = parse_with_profile(message_text, prof)
                if prof_res and prof_res.get("is_signal"):
                    # Safety Rule 3: No Stop Loss, No Trade
                    if float(prof_res.get("sl") or 0.0) <= 0:
                        print("[PARSER] Profile returned signal with SL <= 0. Dropping signal (No Stop Loss, No Trade).")
                        return None
                    if prof_res.get("tp"):
                        prof_res["tp"] = sanitize_and_autocorrect_tps(
                            prof_res["tp"],
                            prof_res.get("direction", ""),
                            prof_res.get("entry"),
                            prof_res.get("sl"),
                            prof_res.get("instrument", "XAUUSD")
                        )
                    return prof_res
        except Exception:
            pass

    # Step 2: Try enhanced universal Regex
    regex_result = parse_with_regex(message_text)
    if regex_result:
        if regex_result.get("is_signal") or regex_result.get("error_code") == "SL_TYPO_UNFIXABLE":
            return regex_result

    # Plan B: Send to Gemini if message contains TP/Target AND SL/Stop Loss cues
    msg_upper = message_text.upper()
    has_tp_cue = any(w in msg_upper for w in ("TP", "TARGET", "TAKE PROFIT", "T1"))
    has_sl_cue = any(w in msg_upper for w in ("SL", "STOP LOSS", "STOP-LOSS", "INVALIDATION"))
    if not (has_tp_cue and has_sl_cue):
        # Not a signal, don't waste API quota
        return None

    if not hasattr(config, "GEMINI_API_KEYS") or not config.GEMINI_API_KEYS:
        print("[ERROR] No Gemini API keys set in config")
        return None

    import random
    api_key = random.choice(config.GEMINI_API_KEYS)

    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{config.GEMINI_MODEL}:generateContent?key={api_key}"
        
        response = httpx.post(
            url,
            headers={"Content-Type": "application/json"},
            json={
                "contents": [
                    {
                        "parts": [
                            {"text": PARSE_PROMPT + message_text}
                        ]
                    }
                ]
            },
            timeout=15.0,
        )

        if response.status_code != 200:
            print(f"[ERROR] Gemini API returned {response.status_code}: {response.text}")
            return None

        data = response.json()
        
        if "candidates" not in data or len(data["candidates"]) == 0:
            print("[ERROR] No candidates returned from Gemini.")
            return None
            
        text = data["candidates"][0]["content"]["parts"][0]["text"].strip()

        # Clean up in case Gemini wraps it in backticks
        text = text.replace("```json", "").replace("```", "").strip()

        parsed = json.loads(text)
        if parsed and parsed.get("is_signal") and parsed.get("tp") and parsed.get("sl"):
            sl_val = float(parsed["sl"])
            # Safety Rule 3: No Stop Loss, No Trade
            if sl_val <= 0:
                print("[PARSER] Gemini returned signal with SL <= 0. Dropping signal (No Stop Loss, No Trade).")
                return None

            tp0_val = float(parsed["tp"][0])
            ent_val = float(parsed.get("entry") or 0)
            if sl_val < tp0_val and (ent_val == 0 or sl_val < ent_val):
                parsed["direction"] = "BUY"
            elif sl_val > tp0_val and (ent_val == 0 or sl_val > ent_val):
                parsed["direction"] = "SELL"

            # Rule 1: Validate Gemini instrument (fail-closed if missing / unknown)
            g_inst = (parsed.get("instrument") or "").strip().upper()
            if not g_inst or g_inst in ["UNKNOWN", "NONE", "NULL"]:
                im = RE_INST.search(message_text)
                if im:
                    parsed["instrument"] = im.group(1).upper().replace('#', '').replace('/', '')
                elif 2000 <= sl_val <= 6000 or 2000 <= tp0_val <= 6000:
                    parsed["instrument"] = "XAUUSD"
                else:
                    print(f"[PARSER] Gemini returned signal without resolvable instrument ({text}). Dropping signal.")
                    return None
            else:
                # Sanitize instrument name
                parsed["instrument"] = g_inst.replace('#', '').replace('/', '')

            # Sanitize & auto-correct TPs from Gemini with Option 1 & Option 2
            parsed["tp"] = sanitize_and_autocorrect_tps(
                parsed["tp"],
                parsed.get("direction", ""),
                parsed.get("entry"),
                parsed.get("sl"),
                parsed.get("instrument", "XAUUSD")
            )
            return parsed
        return None

    except json.JSONDecodeError as e:
        print(f"[ERROR] Failed to parse Gemini response as JSON: {e}")
        return None
    except httpx.TimeoutException:
        print("[ERROR] Gemini API request timed out")
        return None
    except Exception as e:
        print(f"[ERROR] Signal parsing failed: {e}")
        return None
