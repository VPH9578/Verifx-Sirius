"""
router.py — FastAPI APIRouter for the Sirius Backtester (/api/backtest/*).
Completely isolated from live trading endpoints.
"""

import asyncio
import traceback
import re
from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse, HTMLResponse

from backtester import bt_db
from backtester.scraper import fetch_and_build_signals
from backtester.engine import run_simulation, run_autotester_grid
from backtester.report_generator import generate_html_report
from backtester.fleet_scanner import (
    get_fleet_status,
    run_fleet_scan_pipeline,
    stop_fleet_scan,
    pause_fleet_scan,
)

import time

router = APIRouter(prefix="/api/backtest", tags=["Backtester"])

backtest_state = {
    "is_running": False,
    "progress_text": "Idle",
    "progress_pct": 0,
    "heartbeat_ts": 0.0,
    "started_at": 0.0,
    "last_error": None,
    "last_run_id": None,
}


def _set_progress(msg: str, pct: int = None):
    backtest_state["progress_text"] = msg
    backtest_state["heartbeat_ts"] = round(time.time(), 2)
    if pct is not None:
        backtest_state["progress_pct"] = max(0, min(100, int(pct)))
    else:
        # Infer approximate percentage from message if not explicitly passed
        m_low = msg.lower()
        if "scanning" in m_low or "connecting" in m_low or "cache" in m_low:
            backtest_state["progress_pct"] = max(backtest_state["progress_pct"], 15)
        elif "parsing" in m_low:
            backtest_state["progress_pct"] = max(backtest_state["progress_pct"], 30)
        elif "syncing mt5" in m_low:
            backtest_state["progress_pct"] = max(backtest_state["progress_pct"], 40)
        elif "combination" in m_low or "simulating signal" in m_low:
            import re
            match = re.search(r'(\d+)\s+of\s+(\d+)', msg)
            if match:
                cur, tot = int(match.group(1)), int(match.group(2))
                if tot > 0:
                    backtest_state["progress_pct"] = min(98, 45 + int((cur / tot) * 52))
        elif "completed" in m_low:
            backtest_state["progress_pct"] = 100



@router.get("/status")
async def get_backtest_status():
    return JSONResponse(backtest_state)


@router.get("/latest")
async def get_latest_backtest():
    try:
        run = bt_db.get_latest_backtest_run()
        return JSONResponse({"run": run, "status": backtest_state})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/runs")
async def list_backtest_runs(limit: int = 50):
    try:
        runs = bt_db.list_recent_backtest_runs(limit)
        fleet_scans = bt_db.get_fleet_scans_history()
        return JSONResponse({"runs": runs, "fleet_scans": fleet_scans})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/runs/{run_id}")
async def get_specific_backtest_run(run_id: int):
    try:
        run = bt_db.get_backtest_run(run_id)
        if not run:
            return JSONResponse({"error": "Backtest run not found"}, status_code=404)
        return JSONResponse({"run": run})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


def _clean_export_filename(text: str) -> str:
    cleaned = re.sub(r'[^\w\-_.]', '_', str(text).strip())
    return cleaned or "Backtest"


@router.get("/runs/{run_id}/export-html")
async def export_specific_backtest_run_html(run_id: int, alias: str = None):
    try:
        run = bt_db.get_backtest_run(run_id)
        if not run:
            return JSONResponse({"error": "Backtest run not found"}, status_code=404)

        html_content = generate_html_report(run, alias=alias)
        chosen_provider = alias.strip() if (alias and alias.strip()) else (run.get("provider_name") or f"run_{run_id}")
        provider = _clean_export_filename(chosen_provider)
        date_str = time.strftime("%Y%m%d_%H%M%S")
        filename = f"Sirius_Backtest_{provider}_{date_str}.html"

        return HTMLResponse(
            content=html_content,
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"'
            }
        )
    except Exception as e:
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/export-html")
async def export_active_backtest_run_html(request: Request, alias: str = None):
    try:
        payload = await request.json()
        if not payload:
            return JSONResponse({"error": "No backtest data provided for export"}, status_code=400)

        # Allow alias to be passed either in query param or in the payload body
        chosen_alias = alias or payload.get("alias")
        html_content = generate_html_report(payload, alias=chosen_alias)
        raw_provider = payload.get("provider_name") or "Strategy"
        display_name = chosen_alias.strip() if (chosen_alias and chosen_alias.strip()) else raw_provider
        provider = _clean_export_filename(display_name)
        date_str = time.strftime("%Y%m%d_%H%M%S")
        filename = f"Sirius_Backtest_{provider}_{date_str}.html"

        return HTMLResponse(
            content=html_content,
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"'
            }
        )
    except Exception as e:
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


@router.delete("/runs/{run_id}")
async def delete_specific_backtest_run(run_id: int):
    try:
        deleted = bt_db.delete_backtest_run(run_id)
        return JSONResponse({"status": "success", "deleted": deleted})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.delete("/runs")
async def clear_all_runs():
    try:
        count = bt_db.clear_all_backtest_runs()
        return JSONResponse({"status": "success", "deleted_count": count})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/run")
async def execute_backtest(request: Request):
    if backtest_state["is_running"]:
        return JSONResponse(
            {"error": "A backtest is already running. Please wait for it to finish."},
            status_code=409,
        )

    try:
        payload = await request.json()
        channels = payload.get("channels") or []
        channel_id = str(payload.get("channel_id") or "").strip()
        topic_id = int(payload.get("topic_id") or 0)
        provider_name = str(payload.get("provider_name") or channel_id).strip()
        eval_mode = str(payload.get("eval_mode") or "single").lower()
        start_time = int(payload.get("start_time") or 0)
        end_time = int(payload.get("end_time") or 0)
        force_refresh = bool(payload.get("force_refresh", False))
        use_ai_fallback = bool(payload.get("use_ai_fallback", False))
        rules = payload.get("rules") or {}

        # Normalize channels list if only single channel was passed
        if not channels and channel_id:
            channels = [{"channel_id": channel_id, "topic_id": topic_id, "name": provider_name}]

        if not channels:
            return JSONResponse({"error": "Please select at least one Provider or Telegram Channel/Topic."}, status_code=400)
        if start_time <= 0 or end_time <= 0 or end_time <= start_time:
            return JSONResponse({"error": "Invalid date range selected."}, status_code=400)

        backtest_state["is_running"] = True
        backtest_state["last_error"] = None
        backtest_state["started_at"] = round(time.time(), 2)
        backtest_state["heartbeat_ts"] = round(time.time(), 2)
        _set_progress("Initializing backtest...", 5)

        def _sync_worker():
            # If multiple channels and comparative ranking requested
            if len(channels) > 1 and eval_mode == "comparative":
                comparative_results = []
                total_chs = len(channels)
                for idx, ch in enumerate(channels):
                    c_id = str(ch.get("channel_id"))
                    t_id = int(ch.get("topic_id") or 0)
                    c_name = ch.get("name") or ch.get("title") or f"Chat {c_id}"
                    _set_progress(f"Comparing [{idx+1}/{total_chs}]: {c_name}...", int(10 + (idx / total_chs) * 80))
                    try:
                        sigs, ch_title, msg_count = fetch_and_build_signals(
                            channel_id=c_id,
                            topic_id=t_id,
                            start_time=start_time,
                            end_time=end_time,
                            force_refresh=force_refresh,
                            use_ai_fallback=use_ai_fallback,
                            progress_cb=None,
                        )
                        disp_name = c_name if c_name and c_name != c_id else ch_title
                        summary, graph, sim_signals, deals = run_simulation(
                            signals=sigs,
                            provider_name=disp_name,
                            rules=rules,
                            total_messages_scanned=msg_count,
                            progress_cb=None,
                        )
                        run_id = bt_db.save_backtest_run(
                            provider_name=disp_name,
                            channel_id=c_id,
                            topic_id=t_id,
                            start_time=start_time,
                            end_time=end_time,
                            rules=rules,
                            summary=summary,
                            graph=graph,
                            signals=sim_signals,
                            deals=deals,
                        )
                        comparative_results.append({
                            "run_id": run_id,
                            "provider_name": disp_name,
                            "channel_id": c_id,
                            "topic_id": t_id,
                            "trades_count": summary.get("trades_count", 0),
                            "win_rate": summary.get("win_rate", 0.0),
                            "profit": summary.get("profit", 0.0),
                            "gain_pct": summary.get("gain_pct", 0.0),
                            "max_drawdown_dollar": summary.get("max_drawdown_dollar", 0.0),
                            "max_drawdown_pct": summary.get("max_drawdown_pct", 0.0),
                            "profit_factor": summary.get("profit_factor", 0.0),
                            "recovery_ratio": summary.get("recovery_ratio", 0.0),
                            "summary": summary,
                        })
                    except Exception as ch_err:
                        print(f"[WARN] Error running comparative for {c_name}: {ch_err}")

                comparative_results.sort(key=lambda x: x.get("profit", 0.0), reverse=True)
                return {"mode": "comparative", "comparative": comparative_results}

            # Blended Portfolio (or single channel)
            all_signals = []
            total_msg_count = 0
            titles = []

            for idx, ch in enumerate(channels):
                c_id = str(ch.get("channel_id"))
                t_id = int(ch.get("topic_id") or 0)
                c_name = ch.get("name") or ch.get("title") or f"Chat {c_id}"
                _set_progress(f"Fetching signals [{idx+1}/{len(channels)}]: {c_name}...", int(10 + (idx / len(channels)) * 30))
                sigs, ch_title, msg_count = fetch_and_build_signals(
                    channel_id=c_id,
                    topic_id=t_id,
                    start_time=start_time,
                    end_time=end_time,
                    force_refresh=force_refresh,
                    use_ai_fallback=use_ai_fallback,
                    progress_cb=_set_progress if len(channels) == 1 else None,
                )
                disp_name = c_name if c_name and c_name != c_id else ch_title
                titles.append(disp_name)
                total_msg_count += msg_count
                for s in sigs:
                    s_copy = dict(s)
                    s_copy["provider_name"] = disp_name
                    s_copy["source_channel_id"] = c_id
                    s_copy["source_topic_id"] = t_id
                    all_signals.append(s_copy)

            all_signals.sort(key=lambda x: int(x.get("msg_time") or 0))

            if len(channels) > 1:
                display_name = f"Blended Portfolio ({len(channels)} Channels: {', '.join(titles[:2])}"
                if len(channels) > 2:
                    display_name += f" +{len(channels)-2} more"
                display_name += ")"
                save_cid = "PORTFOLIO"
                save_tid = len(channels)
            else:
                display_name = titles[0] if titles else provider_name
                save_cid = channels[0]["channel_id"]
                save_tid = channels[0].get("topic_id", 0)

            _set_progress(f"Found {len(all_signals)} total signals across {total_msg_count:,} messages. Simulating on MT5...", 50)

            summary, graph, sim_signals, deals = run_simulation(
                signals=all_signals,
                provider_name=display_name,
                rules=rules,
                total_messages_scanned=total_msg_count,
                progress_cb=_set_progress,
            )
            _set_progress("Saving backtest results...", 96)
            run_id = bt_db.save_backtest_run(
                provider_name=display_name,
                channel_id=save_cid,
                topic_id=save_tid,
                start_time=start_time,
                end_time=end_time,
                rules=rules,
                summary=summary,
                graph=graph,
                signals=sim_signals,
                deals=deals,
            )
            return {
                "mode": "single" if len(channels) == 1 else "blended",
                "run": {
                    "run_id": run_id,
                    "provider_name": display_name,
                    "channel_id": save_cid,
                    "topic_id": save_tid,
                    "start_time": start_time,
                    "end_time": end_time,
                    "rules": rules,
                    "summary": summary,
                    "graph": graph,
                    "signals": sim_signals,
                    "deals": deals,
                }
            }

        res = await asyncio.to_thread(_sync_worker)
        if "run" in res:
            backtest_state["last_run_id"] = res["run"]["run_id"]
        _set_progress("Completed", 100)
        return JSONResponse({"status": "success", **res})

    except Exception as e:
        traceback.print_exc()
        backtest_state["last_error"] = str(e)
        backtest_state["progress_text"] = f"Error: {e}"
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        backtest_state["is_running"] = False


@router.post("/autotest")
async def execute_autotester(request: Request):
    if backtest_state["is_running"]:
        return JSONResponse(
            {"error": "A backtest or AutoTester is already running. Please wait for it to finish."},
            status_code=409,
        )

    try:
        payload = await request.json()
        channels = payload.get("channels") or []
        channel_id = str(payload.get("channel_id") or "").strip()
        topic_id = int(payload.get("topic_id") or 0)
        provider_name = str(payload.get("provider_name") or channel_id).strip()
        eval_mode = str(payload.get("eval_mode") or "single").lower()
        start_time = int(payload.get("start_time") or 0)
        end_time = int(payload.get("end_time") or 0)
        force_refresh = bool(payload.get("force_refresh", False))
        use_ai_fallback = bool(payload.get("use_ai_fallback", False))
        base_rules = payload.get("rules") or {}

        if not channels and channel_id:
            channels = [{"channel_id": channel_id, "topic_id": topic_id, "name": provider_name}]

        if not channels:
            return JSONResponse({"error": "Please select a Provider or Telegram Channel/Topic."}, status_code=400)
        if start_time <= 0 or end_time <= 0 or end_time <= start_time:
            return JSONResponse({"error": "Invalid date range selected."}, status_code=400)

        backtest_state["is_running"] = True
        backtest_state["last_error"] = None
        backtest_state["started_at"] = round(time.time(), 2)
        backtest_state["heartbeat_ts"] = round(time.time(), 2)
        _set_progress("AutoTester: Initializing grid search...", 5)

        def _sync_autotest():
            # If multiple channels and comparative ranking requested
            if len(channels) > 1 and eval_mode == "comparative":
                comparative_results = []
                total_chs = len(channels)
                for idx, ch in enumerate(channels):
                    c_id = str(ch.get("channel_id"))
                    t_id = int(ch.get("topic_id") or 0)
                    c_name = ch.get("name") or ch.get("title") or f"Chat {c_id}"
                    _set_progress(f"AutoTester [{idx+1}/{total_chs}]: {c_name}...", int(10 + (idx / total_chs) * 85))
                    try:
                        sigs, ch_title, msg_count = fetch_and_build_signals(
                            channel_id=c_id,
                            topic_id=t_id,
                            start_time=start_time,
                            end_time=end_time,
                            force_refresh=force_refresh,
                            use_ai_fallback=use_ai_fallback,
                            progress_cb=None,
                        )
                        disp_name = c_name if c_name and c_name != c_id else ch_title
                        opt_data = run_autotester_grid(
                            signals=sigs,
                            provider_name=disp_name,
                            base_rules=base_rules,
                            total_messages_scanned=msg_count,
                            progress_cb=None,
                        )
                        lb = opt_data.get("leaderboard") or []
                        best_combo = lb[0] if lb else {}
                        comparative_results.append({
                            "channel_id": c_id,
                            "topic_id": t_id,
                            "provider_name": disp_name,
                            "trades_count": best_combo.get("trades_count", 0),
                            "win_rate": best_combo.get("win_rate", 0.0),
                            "profit": best_combo.get("profit", 0.0),
                            "gain_pct": best_combo.get("gain_pct", 0.0),
                            "max_drawdown_dollar": best_combo.get("max_drawdown_dollar", 0.0),
                            "max_drawdown_pct": best_combo.get("max_drawdown_pct", 0.0),
                            "profit_factor": best_combo.get("profit_factor", 0.0),
                            "recovery_ratio": best_combo.get("recovery_ratio", 0.0),
                            "score": best_combo.get("score", 0.0),
                            "tp_profile_name": best_combo.get("tp_profile_name", ""),
                            "sl_profile_name": best_combo.get("sl_profile_name", ""),
                            "expiry_label": best_combo.get("expiry_label", ""),
                            "rules": best_combo.get("rules", {}),
                            "top_combo": best_combo,
                        })
                    except Exception as ch_err:
                        print(f"[WARN] Error running comparative autotest for {c_name}: {ch_err}")

                comparative_results.sort(key=lambda x: x.get("profit", 0.0), reverse=True)
                return {"mode": "comparative_autotest", "comparative": comparative_results}

            # Blended Portfolio (or single channel)
            all_signals = []
            total_msg_count = 0
            titles = []

            for idx, ch in enumerate(channels):
                c_id = str(ch.get("channel_id"))
                t_id = int(ch.get("topic_id") or 0)
                c_name = ch.get("name") or ch.get("title") or f"Chat {c_id}"
                _set_progress(f"Fetching signals [{idx+1}/{len(channels)}]: {c_name}...", int(10 + (idx / len(channels)) * 30))
                sigs, ch_title, msg_count = fetch_and_build_signals(
                    channel_id=c_id,
                    topic_id=t_id,
                    start_time=start_time,
                    end_time=end_time,
                    force_refresh=force_refresh,
                    use_ai_fallback=use_ai_fallback,
                    progress_cb=_set_progress if len(channels) == 1 else None,
                )
                disp_name = c_name if c_name and c_name != c_id else ch_title
                titles.append(disp_name)
                total_msg_count += msg_count
                for s in sigs:
                    s_copy = dict(s)
                    s_copy["provider_name"] = disp_name
                    s_copy["source_channel_id"] = c_id
                    s_copy["source_topic_id"] = t_id
                    all_signals.append(s_copy)

            all_signals.sort(key=lambda x: int(x.get("msg_time") or 0))

            if len(channels) > 1:
                display_name = f"Blended Portfolio ({len(channels)} Channels: {', '.join(titles[:2])}"
                if len(channels) > 2:
                    display_name += f" +{len(channels)-2} more"
                display_name += ")"
            else:
                display_name = titles[0] if titles else provider_name

            _set_progress(f"AutoTester: Found {len(all_signals)} signals across {total_msg_count:,} msgs. Optimizing 104 combos...", 40)

            opt_data = run_autotester_grid(
                signals=all_signals,
                provider_name=display_name,
                base_rules=base_rules,
                total_messages_scanned=total_msg_count,
                progress_cb=_set_progress,
            )
            return {"mode": "single" if len(channels) == 1 else "blended_autotest", "autotest": opt_data}

        result = await asyncio.to_thread(_sync_autotest)
        _set_progress("AutoTester Completed", 100)
        return JSONResponse({"status": "success", **result})

    except Exception as e:
        traceback.print_exc()
        backtest_state["last_error"] = str(e)
        backtest_state["progress_text"] = f"Error: {e}"
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        backtest_state["is_running"] = False


@router.post("/sync-provider")
async def sync_provider_settings(request: Request):
    """Saves updated provider settings dict directly to providers_config.json."""
    try:
        import json
        import os
        data = await request.json()
        settings_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "providers_config.json")
        with open(settings_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        return JSONResponse({"status": "success"})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# =========================================================================
# Fleet Strategy Scanner Endpoints
# =========================================================================

@router.get("/fleet/status")
async def fleet_status():
    return JSONResponse(get_fleet_status())


@router.get("/fleet/league")
async def fleet_league(scan_id: int = None):
    try:
        items = bt_db.get_fleet_league_table(scan_id=scan_id)
        return JSONResponse({"league": items, "count": len(items), "scan_id": scan_id})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/fleet/history")
async def fleet_history():
    try:
        scans = bt_db.get_fleet_scans_history()
        return JSONResponse({"scans": scans, "count": len(scans)})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.delete("/fleet/league")
async def clear_league(scan_id: int = None):
    try:
        bt_db.clear_fleet_league(scan_id=scan_id)
        return JSONResponse({"status": "cleared", "scan_id": scan_id})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/fleet/start")
async def start_fleet_scan(request: Request):
    status = get_fleet_status()
    if status["is_running"]:
        return JSONResponse({"error": "Fleet scan is already in progress."}, status_code=409)

    try:
        payload = await request.json()
        start_time = int(payload.get("start_time") or (time.time() - 365 * 86400))
        end_time = int(payload.get("end_time") or time.time())
        base_rules = payload.get("rules") or {
            "starting_balance": 10000.0,
            "risk_mode": "percent",
            "risk_value": 1.0,
            "spread_offset_entry": True,
            "spread_offset_tp": True,
        }

        # Fetch all live Telegram dialogs from signal_listener / cache
        import signal_listener
        dialogs = signal_listener.load_cached_dialogs()
        if not dialogs:
            return JSONResponse({"error": "No Telegram dialogs cached. Please open the Backtester to refresh channels first."}, status_code=400)

        # Pre-create the new scan_id synchronously and reset fleet_state immediately
        # so the frontend gets the exact new scan_id with 0 providers right away
        new_scan_id = bt_db.create_fleet_scan(
            total_dialogs=len(dialogs),
            settings={"start_time": start_time, "end_time": end_time, "rules": base_rules}
        )
        from backtester.fleet_scanner import fleet_state
        fleet_state["is_running"] = True
        fleet_state["is_paused"] = False
        fleet_state["should_stop"] = False
        fleet_state["scan_id"] = new_scan_id
        fleet_state["total_dialogs"] = len(dialogs)
        fleet_state["probed_count"] = 0
        fleet_state["viable_count"] = 0
        fleet_state["completed_count"] = 0
        fleet_state["progress_pct"] = 0
        fleet_state["status_text"] = "Starting new fleet scan..."
        fleet_state["current_channel"] = ""

        # Launch background pipeline task
        asyncio.create_task(
            run_fleet_scan_pipeline(
                dialogs_list=dialogs,
                start_time=start_time,
                end_time=end_time,
                base_rules=base_rules,
                precreated_scan_id=new_scan_id,
            )
        )
        return JSONResponse({"status": "started", "scan_id": new_scan_id, "dialogs_count": len(dialogs)})
    except Exception as e:
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/fleet/pause")
async def pause_fleet():
    res = pause_fleet_scan()
    return JSONResponse(res)


@router.post("/fleet/stop")
async def stop_fleet():
    res = stop_fleet_scan()
    return JSONResponse(res)



