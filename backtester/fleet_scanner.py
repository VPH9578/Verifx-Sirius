"""
fleet_scanner.py — Phase 1: High-Velocity Fleet Strategy Scanner & Provider Discovery.
Systematically tests all Telegram channels & forum topics, ranks them in the Fleet League,
and enables one-click addition with their discovered optimal settings.
"""

import asyncio
import time
import re
import traceback
from datetime import datetime, timezone

from telethon import TelegramClient
import config
from backtester import bt_db
from backtester.scraper import fetch_and_build_signals, async_fetch_and_build_signals, _safe_parse_regex
from backtester.session_manager import get_isolated_telegram_client
from backtester.engine import run_autotester_grid, run_simulation

# Negative keyword filter for obvious non-trading noise channels
DEFAULT_BLACKLIST_KEYWORDS = [
    "goat welfare", "anime", "manga", "meme", "memes", "politics", "islam", "christian",
    "music", "video", "clips", "relaxing", "voice only", "qr code", "stickers", "chat room"
]

# Priority keywords: channels matching these are scanned first
PRIORITY_KEYWORDS = ["vip", "gold", "signal", "sniper", "layer", "trader", "forex", "xauusd", "storm"]

fleet_state = {
    "is_running": False,
    "is_paused": False,
    "should_stop": False,
    "status_text": "Idle",
    "current_channel": "",
    "scan_id": 0,
    "total_dialogs": 0,
    "probed_count": 0,
    "viable_count": 0,
    "completed_count": 0,
    "progress_pct": 0,
    "start_time_ts": 0,
    "elapsed_seconds": 0,
    "last_error": None,
}


async def _run_thread_serialized(fn, timeout: float):
    """Run a blocking function in a worker thread with a timeout.

    Threads cannot be cancelled, so on timeout we still wait for the orphaned
    thread to finish before returning. This prevents runaway threads from piling
    up and competing for MT5/cTrader/CPU (which slowed the whole app down).
    """
    task = asyncio.ensure_future(asyncio.to_thread(fn))
    try:
        return await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
    except asyncio.TimeoutError:
        try:
            await task  # drain orphan thread, discard result
        except Exception:
            pass
        raise TimeoutError(f"timed out after {int(timeout)}s")


def get_fleet_status():
    if fleet_state["is_running"] and fleet_state["start_time_ts"] > 0:
        fleet_state["elapsed_seconds"] = int(time.time() - fleet_state["start_time_ts"])
    return fleet_state


def stop_fleet_scan():
    if fleet_state["is_running"]:
        fleet_state["should_stop"] = True
        fleet_state["status_text"] = "Stopping fleet scan..."
    return {"status": "stopping"}


def pause_fleet_scan():
    fleet_state["is_paused"] = not fleet_state["is_paused"]
    fleet_state["status_text"] = "Paused" if fleet_state["is_paused"] else "Resumed"
    return {"is_paused": fleet_state["is_paused"]}


async def probe_dialog_for_signals(client: TelegramClient, channel_id: str | int, topic_id: int) -> int:
    """
    Stage 1 Probe: Fetches up to 30 recent messages and checks if ANY contain a valid trade signal.
    Returns the count of detected signals in the sample (0 means non-trading chat).
    Guarded with strict timeouts to prevent hanging on dead or rate-limited channels.
    """
    ch_query = channel_id
    try:
        ch_query = int(channel_id)
    except (ValueError, TypeError):
        ch_query = str(channel_id).strip()

    try:
        entity = await asyncio.wait_for(client.get_entity(ch_query), timeout=8.0)
    except Exception:
        return 0

    iter_kwargs = {"limit": 30}
    if topic_id and int(topic_id) > 0:
        iter_kwargs["reply_to"] = int(topic_id)

    signal_count = 0
    try:
        async def _scan_recent():
            cnt = 0
            async for msg in client.iter_messages(entity, **iter_kwargs):
                if not msg or not msg.text:
                    continue
                parsed = _safe_parse_regex(msg.text)
                if parsed and parsed.get("is_signal"):
                    cnt += 1
                    if cnt >= 2:
                        break
            return cnt

        signal_count = await asyncio.wait_for(_scan_recent(), timeout=12.0)
    except Exception:
        pass

    return signal_count


def calculate_data_quality_flags(signals: list, best_combo: dict) -> list[str]:
    """Inspect signals & results for anomalies and generate transparent audit flags."""
    flags = []
    trades_count = best_combo.get("trades_count", 0)
    
    # 1. Minimum Sample Size
    if trades_count < 30:
        flags.append(f"Low Sample ({trades_count} trades)")
        
    # 2. Check for single-trade profit dominance (outlier windfall)
    # If profit factor is suspiciously huge (> 8.0) or recovery ratio > 15.0 on low trades
    if best_combo.get("profit_factor", 0) >= 8.0 and trades_count < 50:
        flags.append("High Outlier Sensitivity")

    # 3. Instrument mix
    instruments = set((s.get("instrument") or "XAUUSD").upper() for s in signals)
    if len(instruments) > 1:
        flags.append(f"Multi-Asset ({', '.join(list(instruments)[:2])})")
    elif "XAUUSD" in instruments or "GOLD" in instruments:
        flags.append("Gold Specialist")

    # 4. Win rate extremity check
    wr = best_combo.get("win_rate", 0)
    if wr >= 85.0 and trades_count >= 20:
        flags.append("Exceptional Win Rate")
    elif wr < 40.0:
        flags.append("Low Win Rate (Trend Runner)")

    return flags


def calculate_composite_score(combo: dict) -> float:
    """
    Computes a risk-adjusted composite score balancing:
    - Recovery Ratio (Profit / Max DD) [Weight: 40%]
    - Profit Factor [Weight: 30%]
    - Win Rate & Sample Consistency [Weight: 30%]
    Penalizes extremely low sample sizes (< 25 trades).
    """
    trades = combo.get("trades_count", 0)
    if trades < 5:
        return 0.0

    net_profit = combo.get("profit", 0.0)
    if net_profit <= 0:
        return max(0.0, round(net_profit / 1000.0, 2))

    max_dd = max(10.0, combo.get("max_drawdown_dollar", 50.0))
    recovery = net_profit / max_dd
    pf = min(10.0, combo.get("profit_factor", 1.0))
    wr = combo.get("win_rate", 50.0)

    # Sample dampener: 0 trades = 0, 50+ trades = 1.0
    sample_mult = min(1.0, trades / 40.0)

    score = ((recovery * 15.0) + (pf * 20.0) + (wr * 0.5)) * sample_mult
    return round(score, 1)


async def run_fleet_scan_pipeline(
    dialogs_list: list,
    start_time: int,
    end_time: int,
    base_rules: dict,
    blacklist_keywords: list = None,
    progress_cb=None,
    precreated_scan_id: int = None,
):
    """
    Main background pipeline that scans all provided dialogs through:
    1. Blacklist & Keyword Filter
    2. Stage 1 Signal Probe (30 msgs)
    3. Stage 2 Full Scrape & Caching
    4. Stage 3 104-Combo Grid Optimization
    5. Scoring, Quality Flagging, and League Table Persistence
    """
    fleet_state["is_running"] = True
    fleet_state["is_paused"] = False
    fleet_state["should_stop"] = False
    fleet_state["start_time_ts"] = time.time()
    fleet_state["last_error"] = None
    fleet_state["total_dialogs"] = len(dialogs_list)
    fleet_state["probed_count"] = 0
    fleet_state["viable_count"] = 0
    fleet_state["completed_count"] = 0
    fleet_state["progress_pct"] = 0
    fleet_state["status_text"] = "Starting new fleet scan..."
    fleet_state["current_channel"] = ""

    # Create persistent scan record in database for this new scan session (or use precreated_scan_id)
    if precreated_scan_id and int(precreated_scan_id) > 0:
        scan_id = int(precreated_scan_id)
    else:
        scan_id = bt_db.create_fleet_scan(
            total_dialogs=len(dialogs_list),
            settings={"start_time": start_time, "end_time": end_time, "rules": base_rules}
        )
    fleet_state["scan_id"] = scan_id

    bl = blacklist_keywords if blacklist_keywords is not None else DEFAULT_BLACKLIST_KEYWORDS

    # Sort dialogs so VIP / Signals / Gold appear first
    def _priority_score(d):
        name = (d.get("display_name") or "").lower()
        for kw in PRIORITY_KEYWORDS:
            if kw in name:
                return 0
        return 1

    sorted_dialogs = sorted(dialogs_list, key=_priority_score)

    scan_client = None
    try:
        # Strict Isolation Directive: Use dedicated independent in-memory TelegramClient for fleet scans.
        # NEVER touches disk session files, guaranteeing zero file locks.
        scan_client = get_isolated_telegram_client()
        await scan_client.connect()

        for idx, d in enumerate(sorted_dialogs):
            if fleet_state["should_stop"]:
                fleet_state["status_text"] = "Fleet scan stopped by user."
                break

            while fleet_state["is_paused"]:
                fleet_state["status_text"] = "Fleet scan paused..."
                await asyncio.sleep(1)
                if fleet_state["should_stop"]:
                    break

            ch_id = str(d.get("channel_id"))
            t_id = int(d.get("topic_id") or 0)
            d_name = d.get("display_name") or f"Chat {ch_id}"
            d_type = d.get("type") or "channel"
            d_low = d_name.lower()

            fleet_state["current_channel"] = d_name
            fleet_state["probed_count"] = idx + 1
            fleet_state["progress_pct"] = int((idx / max(1, len(sorted_dialogs))) * 100)

            # Stage 1A: Keyword Blacklist
            if any(k in d_low for k in bl):
                continue

            # Ensure client is still connected to Telegram before probing
            if scan_client and not scan_client.is_connected():
                try:
                    await asyncio.wait_for(scan_client.connect(), timeout=8.0)
                except Exception:
                    pass

            # Stage 1B: Fast Probe Check (if not already cached)
            coverage = bt_db.get_scrape_coverage(ch_id, t_id)
            has_cache = coverage is not None

            if not has_cache:
                fleet_state["status_text"] = f"Probing {idx+1}/{len(sorted_dialogs)}: {d_name}..."
                try:
                    probe_sigs = await asyncio.wait_for(probe_dialog_for_signals(scan_client, ch_id, t_id), timeout=15.0)
                except Exception:
                    probe_sigs = 0

                # Brief polite delay between probe calls to stay 100% compliant with Telegram rate limits
                await asyncio.sleep(1.0)

                if probe_sigs == 0:
                    continue  # Non-signal chatroom, skip cleanly

            fleet_state["viable_count"] += 1
            fleet_state["status_text"] = f"Analyzing #{fleet_state['viable_count']}: {d_name}..."

            # Stage 2: Fetch & Build Signals (reusing connected scan_client directly on async loop)
            try:
                signals, ch_title, msg_count = await asyncio.wait_for(
                    async_fetch_and_build_signals(
                        client=scan_client,
                        channel_id=ch_id,
                        topic_id=t_id,
                        start_time=start_time,
                        end_time=end_time,
                        force_refresh=False,
                        use_ai_fallback=False,
                        progress_cb=None,
                        max_messages=5000,
                    ),
                    timeout=60.0,
                )
            except Exception as scrape_err:
                print(f"[WARN] Fleet scan scrape error/timeout on {d_name}: {scrape_err}")
                continue

            display_title = d_name if d_name != ch_id else ch_title

            if len(signals) < 3:
                # Not enough trading signals for valid strategy ranking
                continue

            # Stage 3: AutoTester Parameter Grid Optimization
            # The grid evaluates ~120 combinations; tick-level fidelity here is far too slow
            # (it caused mass timeouts), so the grid always uses fast M1 candles.
            grid_rules = dict(base_rules or {})
            grid_rules["execution_fidelity"] = "m1"

            def _sync_opt():
                return run_autotester_grid(
                    signals=signals,
                    provider_name=display_title,
                    base_rules=grid_rules,
                    total_messages_scanned=msg_count,
                    progress_cb=None,
                )

            try:
                opt_res = await _run_thread_serialized(_sync_opt, timeout=180.0)
            except Exception as opt_err:
                print(f"[WARN] Fleet scan optimization error/timeout on {d_name}: {opt_err}")
                continue
            leaderboard = opt_res.get("leaderboard") or []
            if not leaderboard:
                continue

            best_combo = leaderboard[0]
            flags = calculate_data_quality_flags(signals, best_combo)
            composite = calculate_composite_score(best_combo)

            # Check if this provider is already configured in live copier
            all_live = bt_db.get_bt_db()
            is_added = 0
            try:
                import json as _j, os as _os
                sc_path = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), "providers_config.json")
                if _os.path.exists(sc_path):
                    with open(sc_path, "r", encoding="utf-8") as f:
                        l_cfg = _j.load(f)
                        for p in l_cfg.values():
                            if str(p.get("channel")) == str(ch_id) and int(p.get("topic_id") or 0) == t_id:
                                is_added = 1
                                break
            except Exception:
                pass

            # Stage 4: Run full simulation for the optimal strategy and persist as a backtest run
            run_id = 0
            try:
                from backtester.engine import run_simulation
                # Final accuracy pass for the winning combo uses the user's chosen fidelity
                # (e.g. ticks); falls back to M1 if that is too slow.
                final_rules = dict(best_combo.get("rules", {}) or {})
                final_rules["execution_fidelity"] = (base_rules or {}).get("execution_fidelity", "m1")

                def _sync_final(r=final_rules):
                    return run_simulation(signals=signals, provider_name=display_title, rules=r,
                                          total_messages_scanned=msg_count, progress_cb=None)
                try:
                    sim_summary, sim_graph, sim_signals, sim_deals = await _run_thread_serialized(_sync_final, timeout=120.0)
                except Exception:
                    final_rules["execution_fidelity"] = "m1"
                    sim_summary, sim_graph, sim_signals, sim_deals = await _run_thread_serialized(_sync_final, timeout=60.0)
                run_id = bt_db.save_backtest_run(
                    provider_name=display_title,
                    channel_id=ch_id,
                    topic_id=t_id,
                    start_time=start_time,
                    end_time=end_time,
                    rules=best_combo.get("rules", {}),
                    summary=sim_summary,
                    graph=sim_graph,
                    signals=sim_signals,
                    deals=sim_deals
                )
            except Exception as run_err:
                print(f"[WARN] Fleet scan run save error for {d_name}: {run_err}")

            # Stage 5: Save to Fleet League Table linked to this scan_id and run_id
            provider_record = {
                "channel_id": ch_id,
                "topic_id": t_id,
                "channel_title": display_title,
                "dialog_type": d_type,
                "scanned_at": int(time.time()),
                "status": "RANKED",
                "messages_scanned": msg_count,
                "signals_found": len(signals),
                "trades_count": best_combo.get("trades_count", 0),
                "win_rate": best_combo.get("win_rate", 0.0),
                "net_profit": best_combo.get("profit", 0.0),
                "gain_pct": best_combo.get("gain_pct", 0.0),
                "max_drawdown_dollar": best_combo.get("max_drawdown_dollar", 0.0),
                "max_drawdown_pct": best_combo.get("max_drawdown_pct", 0.0),
                "profit_factor": best_combo.get("profit_factor", 0.0),
                "recovery_ratio": best_combo.get("recovery_ratio", 0.0),
                "composite_score": composite,
                "best_tp_profile": best_combo.get("tp_profile_name", "Standard"),
                "best_sl_profile": best_combo.get("sl_profile_name", "Default"),
                "best_expiry": best_combo.get("expiry_label", "No Expiry"),
            "best_entry_zone": best_combo.get("entry_zone_label", "Mid-Zone"),
                "best_rules": best_combo.get("rules", {}),
                "flags": flags,
                "is_added_to_copier": is_added,
                "scan_id": scan_id,
                "run_id": run_id,
            }

            bt_db.save_fleet_provider_result(provider_record)
            fleet_state["completed_count"] += 1
            bt_db.update_fleet_scan_status(
                scan_id=scan_id,
                status="RUNNING",
                probed=fleet_state["probed_count"],
                viable=fleet_state["viable_count"],
                completed=fleet_state["completed_count"],
                current_dialog=d_name
            )

            # Give a polite breath before next channel
            await asyncio.sleep(0.5)

        fleet_state["progress_pct"] = 100
        fleet_state["status_text"] = f"Fleet scan finished. {fleet_state['completed_count']} providers ranked."
        bt_db.update_fleet_scan_status(
            scan_id=scan_id,
            status="COMPLETED" if not fleet_state["should_stop"] else "STOPPED",
            probed=fleet_state["probed_count"],
            viable=fleet_state["viable_count"],
            completed=fleet_state["completed_count"],
            current_dialog=""
        )
    except Exception as e:
        traceback.print_exc()
        fleet_state["last_error"] = str(e)
        fleet_state["status_text"] = f"Error: {e}"
        if scan_id:
            bt_db.update_fleet_scan_status(
                scan_id=scan_id,
                status="FAILED",
                probed=fleet_state["probed_count"],
                viable=fleet_state["viable_count"],
                completed=fleet_state["completed_count"],
                current_dialog=""
            )
    finally:
        if scan_client:
            try:
                await scan_client.disconnect()
            except Exception:
                pass
        fleet_state["is_running"] = False
        fleet_state["is_paused"] = False
