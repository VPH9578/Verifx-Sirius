import uvicorn
from typing import Optional, List, Dict, Any
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
import asyncio
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
import os
import config
from mt5_executor import get_account_info, get_open_positions, get_tick, get_symbol_info, is_connected
from signal_listener import listener_state
from trade_logger import get_recent_trades

app = FastAPI(title="Verifx Telegram Signal Copier Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
static_dir = os.path.join(BASE_DIR, "web", "static")
os.makedirs(static_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")

templates_dir = os.path.join(BASE_DIR, "web", "templates")
templates = Jinja2Templates(directory=templates_dir)

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except:
                pass

manager = ConnectionManager()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

last_pos_count = -1
last_tg_msg = None

async def event_monitor():
    global last_pos_count, last_tg_msg
    while True:
        await asyncio.sleep(2)
        needs_update = False
        
        if is_connected():
            import MetaTrader5 as mt5
            pos = mt5.positions_get()
            current_count = len(pos) if pos else 0
            if current_count != last_pos_count:
                needs_update = True
                last_pos_count = current_count
                
        if listener_state.get("last_message") != last_tg_msg:
            needs_update = True
            last_tg_msg = listener_state.get("last_message")
            
        if needs_update:
            await manager.broadcast('{"event": "update"}')

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(event_monitor())
    try:
        import signal_watch
        loop = asyncio.get_running_loop()

        def _notify_sw():
            if loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    manager.broadcast('{"event": "signal_watch_update"}'),
                    loop,
                )

        signal_watch.set_ws_notifier(_notify_sw)
        signal_watch.start_observer()
    except Exception:
        pass

@app.get("/")
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

import time

total_profit_cache = {"time": 0, "value": 0.0}
broker_offset_cache = {"offset": None}

@app.get("/api/state")
async def get_state(start_time: int = None, end_time: int = None):
    try:
        from account_manager import AccountManager
        acc_mgr = AccountManager.get_instance()
        active_id = acc_mgr.active_account_id or "acc_1"
        active_cfg = acc_mgr.accounts_config.get(active_id, {})
        active_platform = str(active_cfg.get("platform", "mt5")).lower()

        mt5_conn = is_connected()
        acc = get_account_info() if (mt5_conn and active_platform not in ("ctrader", "mt4")) else None
        tick = get_tick(config.SYMBOL) if (mt5_conn and active_platform not in ("ctrader", "mt4")) else None
        sym = get_symbol_info(config.SYMBOL) if (mt5_conn and active_platform not in ("ctrader", "mt4")) else None

        if mt5_conn and active_platform not in ("ctrader", "mt4") and (time.time() - total_profit_cache["time"] > 30):
            try:
                import MetaTrader5 as mt5
                from datetime import datetime, timedelta
                import db
                
                # Check for a user reset timestamp and custom starting balance
                meta_reset_time = db.get_metadata('reset_time')
                reset_timestamp = int(meta_reset_time) if meta_reset_time else 0

                # Fetch history deals (from reset_time if set, else from 2020)
                history_start = datetime.fromtimestamp(reset_timestamp) if reset_timestamp > 0 else datetime(2020, 1, 1)
                all_deals = mt5.history_deals_get(history_start, datetime.now() + timedelta(days=2))
                
                meta_start_bal = db.get_metadata('starting_balance')
                net_deposits = float(meta_start_bal) if meta_start_bal else 0.0
                total_trade_profit = 0.0
                
                if all_deals:
                    pos_open = {}
                    pos_close = {}

                    # Build map of magic numbers to provider IDs once per sync
                    all_providers = load_settings()
                    magic_to_provider = {}
                    for pid, pdata in all_providers.items():
                        p_magic = pdata.get("magic_number")
                        if not p_magic:
                            try:
                                p_magic = 400400 + int(pid)
                            except:
                                p_magic = int(config.MAGIC_NUMBER)
                        magic_to_provider[int(p_magic)] = str(pid)
                    magic_to_provider[int(config.MAGIC_NUMBER)] = "1"

                    for d in all_deals:
                        # Ignore any deals timestamped before the reset cutoff
                        if reset_timestamp > 0 and d.time < reset_timestamp:
                            continue

                        # DEAL_TYPE_BALANCE = 2
                        if d.type == 2 and not meta_start_bal:
                            net_deposits += d.profit
                        
                        # Record closed trades in the DB if they match any provider's magic number
                        deal_magic = getattr(d, 'magic', 0)
                        if deal_magic in magic_to_provider:
                            if d.entry == 0:
                                pos_open[d.position_id] = d
                            elif d.entry == 1:
                                pos_close[d.position_id] = d
                                matched_pid = magic_to_provider[deal_magic]
                                total_trade_profit += d.profit
                                
                                op_deal = pos_open.get(d.position_id)
                                if not op_deal:
                                    in_deals = mt5.history_deals_get(ticket=d.position_id)
                                    if in_deals and len(in_deals) > 0:
                                        op_deal = in_deals[0]
                                open_price = op_deal.price if op_deal else d.price
                                open_time_val = int(getattr(op_deal, 'time', 0) or 0) if op_deal else 0
                                open_dir = ('BUY' if op_deal.type == 0 else 'SELL') if op_deal else ('BUY' if d.type == 1 else 'SELL')

                                # Determine close reason
                                reason = "TP/SL"
                                if getattr(d, 'comment', '').lower().find('manual') != -1:
                                    reason = "MANUAL"

                                # Save to DB tagged with provider_id and account_id
                                db.save_trade(
                                    ticket=d.ticket,
                                    provider_id=matched_pid,
                                    symbol=d.symbol,
                                    trade_type=open_dir,
                                    lots=d.volume,
                                    open_time=open_time_val,
                                    close_time=d.time,
                                    open_price=open_price,
                                    close_price=d.price,
                                    profit=d.profit,
                                    magic=d.magic,
                                    close_reason=reason,
                                    account_id="acc_1"
                                )

                    # Group deals into trade signals to track TP1, TP2, TP3, and SL hits
                    import re
                    signals_data = {}
                    for pos_id, op in pos_open.items():
                        comm = op.comment or ''
                        m_pt = re.search(r'P(\d+)[_\s]*T(\d+)', comm, re.IGNORECASE)
                        if m_pt:
                            sig_id = f"P{m_pt.group(1)}_T{m_pt.group(2)}"
                        else:
                            m_tr = re.search(r'(Trade\d+)', comm, re.IGNORECASE)
                            sig_id = m_tr.group(1).upper() if m_tr else f'POS_{pos_id}'
                        matched_pid = magic_to_provider.get(op.magic, "1")

                        if sig_id not in signals_data:
                            signals_data[sig_id] = {
                                'provider_id': matched_pid,
                                'symbol': op.symbol,
                                'trade_type': 'BUY' if op.type == 0 else 'SELL',
                                'lots': 0.0,
                                'profit': 0.0,
                                'positions': {},
                                'created_at': op.time,
                                'closed_at': 0,
                            }

                        tp_lvl = 1
                        if 'TP2' in (op.comment or ''): tp_lvl = 2
                        elif 'TP3' in (op.comment or ''): tp_lvl = 3

                        cl = pos_close.get(pos_id)
                        signals_data[sig_id]['lots'] += op.volume
                        if cl:
                            signals_data[sig_id]['profit'] += cl.profit
                            signals_data[sig_id]['closed_at'] = max(signals_data[sig_id]['closed_at'], cl.time)
                        signals_data[sig_id]['positions'][tp_lvl] = {'open': op, 'close': cl}

                    for sig_id, s in signals_data.items():
                        def is_deal_tp(cl_deal):
                            if not cl_deal:
                                return False
                            comm = (cl_deal.comment or '').lower()
                            if '[tp' in comm:
                                return True
                            if '[sl' in comm or '[so' in comm:
                                return False
                            # MT5 deal reason 5 is DEAL_REASON_TP, 4 is DEAL_REASON_SL, 6 is DEAL_REASON_SO
                            reason = getattr(cl_deal, 'reason', None)
                            if reason == 5:
                                return True
                            if reason in (4, 6):
                                return False
                            return False

                        # Rule: TP1 hit (must be actual TP trigger, not trailed SL in profit)
                        tp1_hit = False
                        if 1 in s['positions'] and s['positions'][1]['close']:
                            cl = s['positions'][1]['close']
                            if is_deal_tp(cl):
                                tp1_hit = True

                        # Rule: TP2 hit (must be actual TP trigger, not trailed SL in profit)
                        tp2_hit = False
                        if 2 in s['positions'] and s['positions'][2]['close']:
                            cl = s['positions'][2]['close']
                            if is_deal_tp(cl):
                                tp2_hit = True

                        # Rule: TP3 hit (must be actual TP trigger, not trailed SL in profit)
                        tp3_hit = False
                        if 3 in s['positions'] and s['positions'][3]['close']:
                            cl = s['positions'][3]['close']
                            if is_deal_tp(cl):
                                tp3_hit = True

                        # Rule: SL hit only if no TP hit and closed at original/manual SL
                        sl_hit = False
                        if not tp1_hit and not tp2_hit and not tp3_hit:
                            closes = [p['close'] for p in s['positions'].values() if p['close']]
                            if closes and all(c.profit < 0 or '[sl' in (c.comment or '').lower() for c in closes):
                                sl_hit = True

                        # Compute planned 1R risk amount for the signal
                        risk_amt = 0.0
                        if sl_hit and s['profit'] < 0:
                            # In full SL hit, the loss amount is exactly the 1R risk
                            risk_amt = abs(round(s['profit'], 2))
                        else:
                            # Try to deduce from order initial SL or default provider risk (1% of balance or lot basis)
                            sym_info = get_symbol_info(s['symbol'])
                            ref_op = s['positions'].get(1, {}).get('open') or next((p['open'] for p in s['positions'].values() if p['open']), None)
                            if ref_op and sym_info and sym_info.trade_tick_size > 0 and sym_info.trade_tick_value > 0:
                                # Check if initial order has sl
                                in_ords = mt5.history_orders_get(ticket=ref_op.order)
                                if in_ords and len(in_ords) > 0 and in_ords[0].sl > 0:
                                    sl_dist = abs(ref_op.price - in_ords[0].sl)
                                    risk_amt = round((sl_dist / sym_info.trade_tick_size) * sym_info.trade_tick_value * s['lots'], 2)
                        
                        # Fallback if unknown: default standard risk ~$500 based on typical 1% risk
                        if risk_amt <= 0:
                            risk_amt = 500.0

                        all_closed = all(p['close'] is not None for p in s['positions'].values())
                        status = 'CLOSED' if all_closed else 'OPEN'

                        db.save_or_update_signal(
                            signal_id=sig_id,
                            provider_id=s['provider_id'],
                            symbol=s['symbol'],
                            trade_type=s['trade_type'],
                            lots=round(s['lots'], 2),
                            tp1_hit=1 if tp1_hit else 0,
                            tp2_hit=1 if tp2_hit else 0,
                            tp3_hit=1 if tp3_hit else 0,
                            sl_hit=1 if sl_hit else 0,
                            profit=round(s['profit'], 2),
                            status=status,
                            created_at=s['created_at'],
                            closed_at=s['closed_at'],
                            risk_amount=risk_amt,
                            account_id="acc_1"
                        )

                # Compute overall account low-to-high profit and daily/monthly/yearly growth across trades
                acc_l2h = 0.0
                daily_profit = 0.0
                monthly_profit = 0.0
                yearly_profit = 0.0
                try:
                    conn_tmp = db.get_db()
                    all_tr = conn_tmp.execute("SELECT profit, close_time FROM trades WHERE account_id = 'acc_1' OR account_id = '' OR account_id IS NULL ORDER BY close_time ASC").fetchall()
                    conn_tmp.close()
                    if all_tr:
                        c_acc = [0.0]
                        c_acc_val = 0.0
                        now_dt = datetime.now()
                        start_of_today = datetime(now_dt.year, now_dt.month, now_dt.day).timestamp()
                        start_of_month = datetime(now_dt.year, now_dt.month, 1).timestamp()
                        start_of_year = datetime(now_dt.year, 1, 1).timestamp()

                        for r in all_tr:
                            p_val = float(r['profit'] or 0.0)
                            c_acc_val += p_val
                            c_acc.append(c_acc_val)

                            t_close = float(r['close_time'] or 0)
                            if t_close >= start_of_today:
                                daily_profit += p_val
                            if t_close >= start_of_month:
                                monthly_profit += p_val
                            if t_close >= start_of_year:
                                yearly_profit += p_val

                        acc_l2h = max(0.0, max(c_acc) - min(c_acc))
                except Exception as ex:
                    pass
                total_profit_cache["low_to_high_profit"] = round(acc_l2h, 2)
                total_profit_cache["daily_profit"] = round(daily_profit, 2)
                total_profit_cache["monthly_profit"] = round(monthly_profit, 2)
                total_profit_cache["yearly_profit"] = round(yearly_profit, 2)

                total_profit_cache["deposits"] = net_deposits
                total_profit_cache["trade_profit"] = total_trade_profit
                total_profit_cache["time"] = time.time()
            except Exception as e:
                print("[WARN] DB Sync Error:", e)

        account_data = None
        if acc:
            import db
            meta_start = db.get_metadata("starting_balance")
            start_bal = float(meta_start) if meta_start else total_profit_cache.get("deposits", acc.balance)
            tot_profit = total_profit_cache.get("trade_profit", 0.0)
            l2h_profit = total_profit_cache.get("low_to_high_profit", 0.0)
            daily_prof = total_profit_cache.get("daily_profit", 0.0)
            monthly_prof = total_profit_cache.get("monthly_profit", 0.0)
            yearly_prof = total_profit_cache.get("yearly_profit", 0.0)

            gain_pct = (tot_profit / start_bal * 100.0) if start_bal > 0 else 0.0
            l2h_pct = (l2h_profit / start_bal * 100.0) if start_bal > 0 else 0.0
            daily_pct = (daily_prof / start_bal * 100.0) if start_bal > 0 else 0.0
            monthly_pct = (monthly_prof / start_bal * 100.0) if start_bal > 0 else 0.0
            yearly_pct = (yearly_prof / start_bal * 100.0) if start_bal > 0 else 0.0

            account_data = {
                "balance": acc.balance,
                "starting_balance": start_bal,
                "total_profit": tot_profit,
                "low_to_high_profit": l2h_profit,
                "gain_pct": round(gain_pct, 2),
                "low_to_high_pct": round(l2h_pct, 2),
                "daily_growth": {
                    "profit": round(daily_prof, 2),
                    "pct": round(daily_pct, 2)
                },
                "monthly_growth": {
                    "profit": round(monthly_prof, 2),
                    "pct": round(monthly_pct, 2)
                },
                "yearly_growth": {
                    "profit": round(yearly_prof, 2),
                    "pct": round(yearly_pct, 2)
                }
            }
            if tick and sym:
                account_data["bid"] = tick.bid
                account_data["ask"] = tick.ask
                try:
                    account_data["spread"] = (tick.ask - tick.bid) / sym.point
                except:
                    account_data["spread"] = 0
                    
        broker_time_str = "--:--:--"
        if mt5_conn:
            from datetime import datetime
            import MetaTrader5 as mt5_mod
            
            # 1. Check if configured symbol or active symbols have a fresh tick
            bt_val = None
            if tick and (time.time() - tick.time < 30):
                bt_val = tick.time
            else:
                # 2. Check 24/7 crypto or forex symbols that tick continuously
                for sym_name in ['BTCUSD', 'ETHUSD', 'BTCUSD.crp', 'BCHUSD', 'LTCUSD']:
                    try:
                        c_tick = mt5_mod.symbol_info_tick(sym_name)
                        if c_tick and c_tick.time > 0:
                            # Update cached broker offset
                            broker_offset_cache["offset"] = c_tick.time - time.time()
                            bt_val = time.time() + broker_offset_cache["offset"]
                            break
                    except:
                        pass
            
            if bt_val is None and broker_offset_cache["offset"] is not None:
                bt_val = time.time() + broker_offset_cache["offset"]
            elif bt_val is None and tick:
                bt_val = tick.time

            if bt_val:
                try:
                    broker_time_str = datetime.fromtimestamp(bt_val).strftime("%H:%M:%S")
                except:
                    broker_time_str = "--:--:--"

        positions_raw = get_open_positions() if mt5_conn else []
        positions = []
        if positions_raw:
            for p in positions_raw:
                positions.append({
                    "ticket": p.ticket,
                    "symbol": getattr(p, "symbol", config.SYMBOL),
                    "type": "LONG" if p.type == 0 else "SHORT", # MT5 ORDER_TYPE_BUY = 0
                    "volume": p.volume,
                    "price_open": p.price_open,
                    "open_price": p.price_open,
                    "sl": p.sl,
                    "tp": p.tp,
                    "profit": p.profit,
                    "magic": getattr(p, "magic", 0),
                    "comment": getattr(p, "comment", ""),
                })

        from account_manager import AccountManager
        acc_mgr = AccountManager.get_instance()
        accounts_list = acc_mgr.get_accounts_list()
        active_id = acc_mgr.active_account_id or "acc_1"
        active_cfg = acc_mgr.accounts_config.get(active_id, {})
        active_platform = str(active_cfg.get("platform", "mt5")).lower()
        active_name = str(getattr(acc, "login", config.MT5_ACCOUNT)) if acc else str(config.MT5_ACCOUNT)
        broker_conn = mt5_conn
        active_pids = set(str(p) for p in acc_mgr.get_account_providers(active_id))

        # Fetch recent trades directly from SQLite DB (consolidated by signal), scoped to active account's providers
        trades = []
        try:
            import db
            trades = db.get_consolidated_history(20, provider_ids=active_pids, account_id=active_id)[:10]
        except Exception as e:
            print("[WARN] Error loading DB trades:", e)

        # Sanitize telegram state for JSON serialization
        tg_state = {}
        for k, v in listener_state.items():
            if k == "last_message_time" and v:
                tg_state[k] = v.strftime("%H:%M:%S")
            else:
                tg_state[k] = v

        # Fetch provider stats from SQLite DB, scoped to the active account's assigned providers
        provider_stats = {}
        try:
            import db
            all_cfg = load_settings()
            batch_stats = db.get_all_providers_stats(active_pids, start_time=start_time, end_time=end_time, account_id=active_id)
            for p_id in active_pids:
                p_stats = batch_stats.get(str(p_id)) or {
                    "trades_count": 0, "deal_count": 0, "lots": 0.0, "avg_rr": 1.0,
                    "rr_display": "1:1", "tp1_hits": 0, "tp2_hits": 0, "tp3_hits": 0,
                    "sl_hits": 0, "win_rate": 0.0, "profit": 0.0, "low_to_high_profit": 0.0, "gain": 0.0
                }
                p_stats['settings'] = acc_mgr.get_account_settings_for_provider(active_id, str(p_id))
                provider_stats[str(p_id)] = p_stats
        except Exception as e:
            print("[WARN] Error loading provider stats:", e)

        # If user is viewing MT5 (acc_1), scope metrics strictly to MT5's assigned providers
        if active_id == "acc_1" and active_platform != "ctrader":
            mt5_tot_profit = round(sum(float(ps.get("profit", 0.0)) for ps in provider_stats.values()), 2)
            mt5_l2h_profit = round(sum(float(ps.get("low_to_high_profit", 0.0)) for ps in provider_stats.values()), 2)

            now_dt = datetime.now()
            start_of_today = datetime(now_dt.year, now_dt.month, now_dt.day).timestamp()
            start_of_month = datetime(now_dt.year, now_dt.month, 1).timestamp()
            start_of_year = datetime(now_dt.year, 1, 1).timestamp()

            mt5_daily_prof = 0.0
            mt5_monthly_prof = 0.0
            mt5_yearly_prof = 0.0

            try:
                conn_tmp = db.get_db()
                pids_list = list(active_pids)
                if pids_list:
                    placeholders = ",".join(["?"] * len(pids_list))
                    q_clauses = [f"provider_id IN ({placeholders})", "account_id = ?"]
                    q_params = pids_list + [active_id]
                    if start_time is not None:
                        q_clauses.append("close_time >= ?")
                        q_params.append(int(start_time))
                    if end_time is not None:
                        q_clauses.append("close_time <= ?")
                        q_params.append(int(end_time))
                    where_sql = " AND ".join(q_clauses)
                    tr_rows = conn_tmp.execute(
                        f"SELECT profit, close_time FROM trades WHERE {where_sql} ORDER BY close_time ASC",
                        q_params
                    ).fetchall()
                    for r in tr_rows:
                        p_val = float(r["profit"] or 0.0)
                        t_close = float(r["close_time"] or 0)
                        if t_close >= start_of_today:
                            mt5_daily_prof += p_val
                        if t_close >= start_of_month:
                            mt5_monthly_prof += p_val
                        if t_close >= start_of_year:
                            mt5_yearly_prof += p_val
                conn_tmp.close()
            except Exception:
                pass

            if start_time is not None or end_time is not None:
                mt5_daily_prof = mt5_tot_profit
                mt5_monthly_prof = mt5_tot_profit
                mt5_yearly_prof = mt5_tot_profit

            meta_mt5_start = db.get_metadata(f"starting_balance_{active_id}") or meta_start
            if meta_mt5_start:
                mt5_start_bal = float(meta_mt5_start)
            else:
                mt5_start_bal = round(acc.balance - mt5_tot_profit, 2) if acc else 0.0
            if mt5_start_bal <= 0 and acc:
                mt5_start_bal = acc.balance
            mt5_gain_pct = round((mt5_tot_profit / mt5_start_bal * 100.0), 2) if mt5_start_bal > 0 else 0.0
            mt5_l2h_pct = round((mt5_l2h_profit / mt5_start_bal * 100.0), 2) if mt5_start_bal > 0 else 0.0

            account_data = {
                "balance": acc.balance if acc else 0.0,
                "starting_balance": mt5_start_bal,
                "equity": acc.equity if acc else (acc.balance if acc else 0.0),
                "profit": round((acc.equity - acc.balance), 2) if acc else 0.0,
                "total_profit": mt5_tot_profit,
                "low_to_high_profit": mt5_l2h_profit,
                "gain_pct": mt5_gain_pct,
                "low_to_high_pct": mt5_l2h_pct,
                "daily_growth": {
                    "profit": round(mt5_daily_prof, 2),
                    "pct": round((mt5_daily_prof / mt5_start_bal * 100.0), 2) if mt5_start_bal > 0 else 0.0
                },
                "monthly_growth": {
                    "profit": round(mt5_monthly_prof, 2),
                    "pct": round((mt5_monthly_prof / mt5_start_bal * 100.0), 2) if mt5_start_bal > 0 else 0.0
                },
                "yearly_growth": {
                    "profit": round(mt5_yearly_prof, 2),
                    "pct": round((mt5_yearly_prof / mt5_start_bal * 100.0), 2) if mt5_start_bal > 0 else 0.0
                },
                "currency": "$",
                "platform": active_platform,
            }
            if tick and sym:
                account_data["bid"] = tick.bid
                account_data["ask"] = tick.ask
                try:
                    account_data["spread"] = (tick.ask - tick.bid) / sym.point
                except:
                    account_data["spread"] = 0

        # If user switched the active dashboard view to a cTrader, MT4, or secondary worker account, overlay its live state
        if active_id != "acc_1" or active_platform in ("ctrader", "mt4"):
            w_state = acc_mgr.get_active_state(active_id, timeout=0.4)
            broker_conn = bool(w_state.get("connected", False))
            active_name = str(w_state.get("login") or active_cfg.get("ctrader_account_id") or active_cfg.get("name") or active_id)
            w_bal = float(w_state.get("balance", 0.0)) if broker_conn else 0.0
            w_eq = float(w_state.get("equity", w_bal)) if broker_conn else 0.0

            # Sum realized profits only for providers assigned to this account
            ct_tot_profit = round(sum(float(ps.get("profit", 0.0)) for ps in provider_stats.values()), 2)
            ct_l2h_profit = round(sum(float(ps.get("low_to_high_profit", 0.0)) for ps in provider_stats.values()), 2)

            now_dt = datetime.now()
            start_of_today = datetime(now_dt.year, now_dt.month, now_dt.day).timestamp()
            start_of_month = datetime(now_dt.year, now_dt.month, 1).timestamp()
            start_of_year = datetime(now_dt.year, 1, 1).timestamp()
            ct_daily_prof = 0.0
            ct_monthly_prof = 0.0
            ct_yearly_prof = 0.0
            reset_ts = int(float(db.get_metadata(f"reset_time_{active_id}") or 0))
            try:
                conn_tmp = db.get_db()
                pids_list = list(active_pids)
                q_clauses = ["account_id = ?"]
                q_params = [active_id]
                if pids_list:
                    placeholders = ",".join(["?"] * len(pids_list))
                    q_clauses.append(f"provider_id IN ({placeholders})")
                    q_params.extend(pids_list)
                if start_time is not None:
                    q_clauses.append("close_time >= ?")
                    q_params.append(int(start_time))
                elif reset_ts > 0:
                    q_clauses.append("close_time >= ?")
                    q_params.append(reset_ts)
                if end_time is not None:
                    q_clauses.append("close_time <= ?")
                    q_params.append(int(end_time))
                where_sql = " AND ".join(q_clauses)
                tr_rows = conn_tmp.execute(
                    f"SELECT profit, close_time FROM trades WHERE {where_sql} ORDER BY close_time ASC",
                    q_params
                ).fetchall()
                for r in tr_rows:
                    p_val = float(r["profit"] or 0.0)
                    t_close = float(r["close_time"] or 0)
                    if t_close >= start_of_today:
                        ct_daily_prof += p_val
                    if t_close >= start_of_month:
                        ct_monthly_prof += p_val
                    if t_close >= start_of_year:
                        ct_yearly_prof += p_val
                conn_tmp.close()
            except Exception:
                ct_daily_prof = ct_tot_profit
                ct_monthly_prof = ct_tot_profit
                ct_yearly_prof = ct_tot_profit

            if start_time is not None or end_time is not None:
                ct_daily_prof = ct_tot_profit
                ct_monthly_prof = ct_tot_profit
                ct_yearly_prof = ct_tot_profit

            meta_ct_start = db.get_metadata(f"starting_balance_{active_id}")
            if meta_ct_start and float(meta_ct_start) > 0:
                ct_start_bal = round(float(meta_ct_start), 2)
            else:
                ct_start_bal = round(max(0.0, w_bal - ct_tot_profit), 2) if w_bal > 0 else 0.0
            ct_gain_pct = round((ct_tot_profit / ct_start_bal * 100.0), 2) if ct_start_bal > 0 else 0.0
            ct_l2h_pct = round((ct_l2h_profit / ct_start_bal * 100.0), 2) if ct_start_bal > 0 else 0.0

            account_data = {
                "balance": w_bal,
                "starting_balance": ct_start_bal if ct_start_bal > 0 else w_bal,
                "equity": w_eq,
                "profit": round(w_eq - w_bal, 2),
                "total_profit": ct_tot_profit,
                "low_to_high_profit": ct_l2h_profit,
                "gain_pct": ct_gain_pct,
                "low_to_high_pct": ct_l2h_pct,
                "daily_growth": {
                    "profit": round(ct_daily_prof, 2),
                    "pct": round((ct_daily_prof / ct_start_bal * 100.0), 2) if ct_start_bal > 0 else 0.0
                },
                "monthly_growth": {
                    "profit": round(ct_monthly_prof, 2),
                    "pct": round((ct_monthly_prof / ct_start_bal * 100.0), 2) if ct_start_bal > 0 else 0.0
                },
                "yearly_growth": {
                    "profit": round(ct_yearly_prof, 2),
                    "pct": round((ct_yearly_prof / ct_start_bal * 100.0), 2) if ct_start_bal > 0 else 0.0
                },
                "currency": "$",
                "platform": active_platform,
            }
            raw_pos = w_state.get("open_positions", []) if broker_conn else []
            positions = []
            for p in raw_pos:
                p_dict = dict(p) if isinstance(p, dict) else p.__dict__.copy()
                op = p_dict.get("price_open")
                if op is None:
                    op = p_dict.get("open_price", 0.0)
                p_dict["price_open"] = op
                p_dict["open_price"] = op
                p_dict["symbol"] = p_dict.get("symbol") or config.SYMBOL
                positions.append(p_dict)

        # Enrich all positions with provider name, ID, tier, and fallback SL/TP from trade state
        try:
            ts_data = {}
            for cand_file in [f"trade_state_{active_id}.json", f"trade_state_acc_{active_id}.json", "trade_state.json"]:
                if os.path.exists(cand_file):
                    try:
                        with open(cand_file, "r", encoding="utf-8") as f:
                            c_data = json.load(f)
                            if c_data.get("active_trade_groups"):
                                ts_data = c_data
                                break
                    except Exception:
                        pass

            ticket_map = {}
            for gid, g in (ts_data.get("active_trade_groups") or {}).items():
                pid = str(g.get("provider_id") or "")
                g_sl = g.get("sl_price") or g.get("sl")
                for t_str, t_info in (g.get("tickets_info") or {}).items():
                    try:
                        ticket_map[int(t_str)] = {
                            "provider_id": pid,
                            "sl": t_info.get("sl") or g_sl,
                            "tp": t_info.get("target_tp") or g.get("tp1_price"),
                            "tier": t_info.get("tier"),
                        }
                    except Exception:
                        pass
                for key, tier_lbl in [("tp1_ticket", "TP1"), ("tp2_ticket", "TP2"), ("tp3_ticket", "TP3")]:
                    t = g.get(key)
                    if t:
                        try:
                            t_int = int(t)
                            if t_int not in ticket_map:
                                tp_tier_key = tier_lbl.lower() + "_price"
                                ticket_map[t_int] = {
                                    "provider_id": pid,
                                    "sl": g_sl,
                                    "tp": g.get(tp_tier_key) or g.get("tp1_price"),
                                    "tier": tier_lbl,
                                }
                        except Exception:
                            pass
                for t in g.get("tickets", []):
                    try:
                        t_int = int(t)
                        if t_int not in ticket_map:
                            ticket_map[t_int] = {
                                "provider_id": pid,
                                "sl": g_sl,
                                "tp": g.get("tp1_price"),
                                "tier": None,
                            }
                    except Exception:
                        pass

            prov_catalog = all_cfg if "all_cfg" in locals() and all_cfg else load_settings()

            for p in positions:
                t = int(p.get("ticket") or 0)
                t_info = ticket_map.get(t, {})
                pid = p.get("provider_id") or t_info.get("provider_id")
                tier = p.get("tier") or t_info.get("tier")

                # Fallback to comment e.g. P159_TP1 or [P159]
                comm = str(p.get("comment") or "")
                if not pid and comm:
                    if comm.startswith("P") and "_" in comm:
                        part = comm[1:].split("_")[0]
                        if part.isdigit():
                            pid = part
                        tier_part = comm[1:].split("_")[1] if len(comm[1:].split("_")) > 1 else None
                        if tier_part and not tier:
                            tier = tier_part
                    elif "[P" in comm:
                        import re
                        m = re.search(r"\[P?(\d+)\]", comm)
                        if m:
                            pid = m.group(1)

                # Fallback to magic number: e.g. 400159 or 400559
                mag = int(p.get("magic") or 0)
                if not pid and mag:
                    if 400000 < mag < 500000:
                        pid = str(mag - 400000)
                    elif mag > 400400:
                        pid = str(mag - 400400)

                # Set provider info
                p_name = "Manual / Unknown"
                if pid:
                    p["provider_id"] = str(pid)
                    p_info = prov_catalog.get(str(pid), {})
                    p_name = p_info.get("name") or f"Provider {pid}"
                p["provider_name"] = p_name
                p["tier"] = tier

                # Fallback SL / TP if 0 or missing
                cur_sl = p.get("sl")
                if cur_sl is None or float(cur_sl or 0) <= 0:
                    sl_fb = t_info.get("sl")
                    if sl_fb and float(sl_fb) > 0:
                        p["sl"] = float(sl_fb)

                cur_tp = p.get("tp")
                if cur_tp is None or float(cur_tp or 0) <= 0:
                    tp_fb = t_info.get("tp")
                    if tp_fb and float(tp_fb) > 0:
                        p["tp"] = float(tp_fb)
        except Exception as ex:
            print("[WARN] Error enriching positions:", ex)

        return JSONResponse({
            "status": {
                "mt5": broker_conn,
                "platform": active_platform,
                "telegram": tg_state.get("connected", False),
                "channel": str(tg_state.get("channel_name", "")),
                "topic": config.SIGNAL_TOPIC_ID,
                "is_paused": bool(active_cfg.get("is_paused", False)),
                "active_account_id": active_id,
                "active_account_name": active_name,
            },
            "accounts": accounts_list,
            "broker_time": broker_time_str,
            "account": account_data,
            "symbol": config.SYMBOL,
            "telegram_state": tg_state,
            "positions": positions,
            "trades": trades,
            "providers": provider_stats,
            "errors": db.get_recent_errors(20)
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/errors")
async def get_errors(limit: int = 50, provider_id: str = None):
    try:
        import db
        return JSONResponse({"status": "success", "errors": db.get_recent_errors(limit, provider_id)})
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)

@app.get("/api/graph")
async def get_graph_data(start_time: int = None, end_time: int = None, providers: str = None, account_id: str = None):
    try:
        from datetime import datetime
        import db

        from account_manager import AccountManager
        acc_mgr = AccountManager.get_instance()
        active_id = str(account_id).strip() if (account_id and str(account_id).strip()) else (acc_mgr.active_account_id or "acc_1")
        active_cfg = acc_mgr.accounts_config.get(active_id, {})
        active_platform = str(active_cfg.get("platform", "mt5")).lower()
        active_pids = set(str(p) for p in acc_mgr.get_account_providers(active_id))

        # Parse provider filter if provided
        selected_pids = None
        if providers:
            selected_pids = [p.strip() for p in providers.split(",") if p.strip()]

        if selected_pids is not None and (len(selected_pids) == 0 or "__NONE__" in selected_pids):
            return JSONResponse({"trade_by_trade": [], "daily": {}, "weekly": {}, "monthly": {}})

        conn = db.get_db()
        c = conn.cursor()

        # Query all signals and trades strictly scoped to the active account
        if active_id == "acc_1":
            c.execute("SELECT * FROM trade_signals WHERE account_id = 'acc_1' OR account_id = '' OR account_id IS NULL ORDER BY closed_at ASC, created_at ASC")
            all_signals = [dict(s) for s in c.fetchall()]

            c.execute("SELECT * FROM trades WHERE account_id = 'acc_1' OR account_id = '' OR account_id IS NULL ORDER BY close_time ASC")
            all_trades = [dict(t) for t in c.fetchall()]
        else:
            c.execute("SELECT * FROM trade_signals WHERE account_id = ? ORDER BY closed_at ASC, created_at ASC", (active_id,))
            all_signals = [dict(s) for s in c.fetchall()]

            c.execute("SELECT * FROM trades WHERE account_id = ? ORDER BY close_time ASC", (active_id,))
            all_trades = [dict(t) for t in c.fetchall()]
        conn.close()

        # Map trades to originating signals
        matched_tickets = set()
        signal_points = []

        for s in all_signals:
            sid = s.get('signal_id')
            pid = str(s.get('provider_id') or '1')
            s_created = s.get('created_at') or 0
            s_closed = s.get('closed_at') or 0
            s_symbol = s.get('symbol')

            candidate_trades = []
            for t in all_trades:
                if t['ticket'] in matched_tickets:
                    continue
                if str(t.get('provider_id') or '1') != pid:
                    continue
                if s_symbol and t.get('symbol') and s_symbol != t.get('symbol'):
                    continue
                t_open = t.get('open_time') or 0
                t_close = t.get('close_time') or 0

                # Match by open_time close to signal creation, or close_time within signal lifetime window
                if t_open > 0:
                    if abs(t_open - s_created) <= 120:
                        candidate_trades.append(t)
                else:
                    eff_closed = s_closed if (s_closed and s_closed > 0) else (s_created + 14400)
                    if (s_created - 300) <= t_close <= (eff_closed + 300):
                        candidate_trades.append(t)

            if candidate_trades:
                for ct in candidate_trades:
                    matched_tickets.add(ct['ticket'])
                tot_prof = round(sum(ct['profit'] for ct in candidate_trades), 2)
                close_t = max(ct['close_time'] for ct in candidate_trades)
                signal_points.append({
                    "signal_id": sid,
                    "provider_id": pid,
                    "profit": tot_prof,
                    "close_time": close_t
                })
            else:
                tot_prof = round(float(s.get('profit') or 0.0), 2)
                close_t = s.get('closed_at') or s.get('created_at') or 0
                if tot_prof != 0.0 or close_t > 0:
                    signal_points.append({
                        "signal_id": sid,
                        "provider_id": pid,
                        "profit": tot_prof,
                        "close_time": close_t
                    })

        # Include any orphan trades not linked to recorded trade_signals
        for t in all_trades:
            if t['ticket'] not in matched_tickets:
                signal_points.append({
                    "signal_id": f"T_{t['ticket']}",
                    "provider_id": str(t.get('provider_id') or '1'),
                    "profit": round(float(t.get('profit') or 0.0), 2),
                    "close_time": t.get('close_time') or 0
                })

        # Sort signal points chronologically
        signal_points.sort(key=lambda x: x['close_time'] or 0)

        trade_by_trade = []
        daily = {}
        weekly = {}
        monthly = {}

        cum_profit = 0.0
        for sp in signal_points:
            # Scope graph strictly to providers assigned to the currently active account
            if sp["provider_id"] not in active_pids:
                continue

            p_val = float(sp["profit"] or 0.0)
            if p_val == 0.0:
                continue

            c_time = sp["close_time"]
            # Apply time filter
            if start_time is not None and c_time < int(start_time):
                continue
            if end_time is not None and c_time > int(end_time):
                continue
            # Apply provider filter
            if selected_pids is not None and sp["provider_id"] not in selected_pids:
                continue

            dt = datetime.fromtimestamp(c_time) if c_time else datetime.now()
            cum_profit += p_val

            trade_by_trade.append({
                "time": dt.isoformat(),
                "profit": round(cum_profit, 2),
                "deal_profit": round(p_val, 2),
                "signal_id": sp["signal_id"],
                "provider_id": sp["provider_id"]
            })

            day_str = dt.strftime("%Y-%m-%d")
            daily[day_str] = round(daily.get(day_str, 0.0) + p_val, 2)

            week_str = dt.strftime("%Y-W%V")
            weekly[week_str] = round(weekly.get(week_str, 0.0) + p_val, 2)

            month_str = dt.strftime("%Y-%m")
            monthly[month_str] = round(monthly.get(month_str, 0.0) + p_val, 2)

        # Retrieve starting balance scoped to the active account
        if active_id != "acc_1" or active_platform == "ctrader":
            meta_start = db.get_metadata(f"starting_balance_{active_id}")
            if meta_start and float(meta_start) > 0:
                starting_balance = float(meta_start)
            else:
                w_state = acc_mgr.get_active_state(active_id, timeout=1.0)
                w_bal = float(w_state.get("balance", 0.0) or 0.0)
                if w_bal <= 0:
                    cached = acc_mgr.cached_states.get(active_id, {})
                    w_bal = float(cached.get("balance", 0.0) or 0.0)
                starting_balance = max(0.0, w_bal - cum_profit) if w_bal > 0 else 0.0
        else:
            meta_start = db.get_metadata(f"starting_balance_{active_id}") or db.get_metadata("starting_balance")
            starting_balance = float(meta_start) if meta_start else (total_profit_cache.get("deposits") or 0.0)

        return JSONResponse({
            "starting_balance": round(starting_balance, 2),
            "trade_by_trade": trade_by_trade,
            "daily": daily,
            "weekly": weekly,
            "monthly": monthly
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/providers/{provider_id}/statistics")
def get_provider_statistics(provider_id: str, account_id: Optional[str] = None, refresh: bool = True):
    """Deep statistics for the 'View Statistics' popup (see provider_analytics.py).
    Plain `def` -> runs in FastAPI's threadpool so MT5 history sync never blocks the event loop."""
    try:
        import db
        import provider_analytics as pa
        from account_manager import AccountManager

        all_cfg = load_settings()
        if str(provider_id) not in all_cfg:
            return JSONResponse({"error": "Provider not found"}, status_code=404)

        acc_mgr = AccountManager.get_instance()
        acc_mgr.load_accounts()

        # Determine target account
        target_acc_id = str(account_id or "").strip()
        if not target_acc_id or target_acc_id not in acc_mgr.accounts_config:
            # Check which account contains this provider
            for a_id, a_cfg in acc_mgr.accounts_config.items():
                if str(provider_id) in [str(x) for x in a_cfg.get("providers", [])]:
                    target_acc_id = a_id
                    break
        if not target_acc_id:
            target_acc_id = acc_mgr.active_account_id or "acc_1"

        acc_cfg = acc_mgr.accounts_config.get(target_acc_id, {})
        platform = str(acc_cfg.get("platform", "mt5")).lower()

        reset_ts = int(float(db.get_metadata(f'reset_time_{target_acc_id}') or db.get_metadata('reset_time') or 0))

        # Always sync trades from SQLite DB (covers cTrader and already closed trades)
        try:
            pa.sync_from_trades_db(reset_ts=reset_ts)
        except Exception as _sync_err:
            print(f"[Stats] pa.sync_from_trades_db error: {_sync_err}")

        sync_info = {"status": "ok", "platform": platform}
        broker_offset = 0
        base_balance = 0.0

        if platform == "ctrader":
            # For cTrader:
            w_state = acc_mgr.get_active_state(target_acc_id, timeout=0.5)
            is_conn = bool(w_state.get("connected", False))
            sync_info = {
                "status": "ok" if is_conn else "ctrader_disconnected",
                "platform": "ctrader"
            }
            broker_offset = 0  # cTrader timestamps are UTC (GMT+0)
            w_bal = float(w_state.get("balance", 0.0)) if is_conn else 0.0

            meta_start = db.get_metadata(f"starting_balance_{target_acc_id}") or db.get_metadata("starting_balance")
            if meta_start:
                try:
                    base_balance = float(meta_start)
                except Exception:
                    pass
            if not base_balance:
                base_balance = float(acc_cfg.get("starting_balance") or 0.0)
            if not base_balance and w_bal > 0:
                base_balance = w_bal
        else:
            # For MT5 / MT4:
            if target_acc_id != "acc_1":
                w_state = acc_mgr.get_active_state(target_acc_id, timeout=0.5)
                is_conn = bool(w_state.get("connected", False))
                offline_tag = f"{platform}_offline" if platform in ("mt4", "mt5") else "mt5_offline"
                sync_info = {
                    "status": "ok" if is_conn else offline_tag,
                    "platform": platform
                }
                broker_offset = pa.get_broker_offset()
                w_bal = float(w_state.get("balance", 0.0)) if is_conn else 0.0

                meta_start = db.get_metadata(f"starting_balance_{target_acc_id}")
                if meta_start:
                    try:
                        base_balance = float(meta_start)
                    except Exception:
                        pass
                if not base_balance:
                    base_balance = float(acc_cfg.get("starting_balance") or 0.0)
                if not base_balance and w_bal > 0:
                    base_balance = w_bal
            else:
                sync_info = {"status": "mt5_offline", "platform": "mt5"}
                broker_offset = pa.get_broker_offset()
                acc = None
                if is_connected():
                    import MetaTrader5 as mt5
                    if refresh:
                        magic_map = pa.build_magic_map(all_cfg, config.MAGIC_NUMBER)
                        sync_info = pa.sync_from_mt5(mt5, magic_map, reset_ts=reset_ts)
                        sync_info["platform"] = "mt5"
                    broker_offset = pa.get_broker_offset(mt5, [config.SYMBOL])
                    acc = get_account_info()

                meta_start = db.get_metadata(f"starting_balance_{target_acc_id}") or db.get_metadata("starting_balance")
                if meta_start:
                    base_balance = float(meta_start)
                else:
                    base_balance = total_profit_cache.get("deposits") or (acc.balance if acc else 0.0)

        stats = pa.compute_provider_statistics(str(provider_id), base_balance=base_balance,
                                               broker_offset=broker_offset, reset_ts=reset_ts,
                                               account_id=target_acc_id)
        stats["provider_name"] = all_cfg[str(provider_id)].get("name") or f"Provider {provider_id}"
        stats["account_id"] = target_acc_id
        stats["platform"] = platform
        stats["sync"] = sync_info
        return JSONResponse(stats)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/candles")
def get_historical_candles(
    symbol: str,
    timeframe: str = "M15",
    from_ts: int = 0,
    to_ts: int = 0,
    account_id: Optional[str] = None,
    provider_id: Optional[str] = None,
):
    """Fetch historical OHLC candlesticks from MetaTrader 5 for the Trade History Chart.
    Only enabled for MT5 accounts."""
    try:
        from datetime import datetime, timezone
        import db
        import provider_analytics as pa
        from account_manager import AccountManager
        from symbol_mapper import SymbolMapper

        acc_mgr = AccountManager.get_instance()
        acc_mgr.load_accounts()

        target_acc_id = str(account_id or "").strip()
        if not target_acc_id or target_acc_id not in acc_mgr.accounts_config:
            if provider_id:
                for a_id, a_cfg in acc_mgr.accounts_config.items():
                    if str(provider_id) in [str(x) for x in a_cfg.get("providers", [])]:
                        target_acc_id = a_id
                        break
        if not target_acc_id:
            target_acc_id = acc_mgr.active_account_id or "acc_1"

        acc_cfg = acc_mgr.accounts_config.get(target_acc_id, {})
        platform = str(acc_cfg.get("platform", "mt5")).lower()
        if platform not in ("mt5", "ctrader"):
            return JSONResponse(
                {"error": "Trade history charting is only supported on MetaTrader 5 and cTrader accounts.", "platform": platform},
                status_code=400,
            )

        tf_str = str(timeframe or "M15").upper()
        tf_sec_map = {
            "M1": 60,
            "M5": 300,
            "M15": 900,
            "M30": 1800,
            "H1": 3600,
            "H4": 14400,
            "D1": 86400,
        }
        tf_sec = tf_sec_map.get(tf_str, 900)

        raw_sym = str(symbol or "").strip()
        if not raw_sym:
            return JSONResponse({"error": "Missing symbol"}, status_code=400)

        # If from_ts / to_ts not provided, auto-frame around the provider's trades on this symbol
        if (from_ts <= 0 or to_ts <= 0) and provider_id:
            conn = db.get_db()
            row = conn.execute(
                "SELECT MIN(CASE WHEN open_time > 0 THEN open_time ELSE close_time END), MAX(close_time) "
                "FROM trade_analytics WHERE provider_id = ? AND account_id = ? AND symbol = ?",
                (str(provider_id), target_acc_id, raw_sym),
            ).fetchone()
            conn.close()
            if row and row[0] and row[1]:
                from_ts = int(row[0])
                to_ts = int(row[1])

        now_ts = int(time.time())
        if to_ts <= 0:
            to_ts = now_ts
        if from_ts <= 0 or from_ts >= to_ts:
            from_ts = to_ts - 120 * tf_sec

        # Add generous padding before open_time and after close_time so zooming out shows plenty of surrounding candles
        fetch_from = max(0, int(from_ts) - 250 * tf_sec)
        fetch_to = int(to_ts) + 150 * tf_sec
        # Cap total span to at most 6000 bars while preserving from_ts
        if (fetch_to - fetch_from) // tf_sec > 6000:
            fetch_from = max(0, min(int(from_ts) - 20 * tf_sec, fetch_to - 6000 * tf_sec))

        # --- cTrader Path ---
        if platform == "ctrader":
            worker_res = acc_mgr.get_candles(target_acc_id, raw_sym, tf_str, fetch_from, fetch_to, timeout=8.0)
            if worker_res and worker_res.get("candles"):
                worker_res["account_id"] = target_acc_id
                worker_res["platform"] = "ctrader"
                worker_res["pip_size"] = pa.get_pip_size(worker_res.get("symbol") or raw_sym)
                return JSONResponse(worker_res)

            # Direct fallback to local cTrader bridge if worker queue didn't respond
            from ctrader_client import CTraderClient
            ct_client = CTraderClient(
                host=str(acc_cfg.get("ctrader_host") or "127.0.0.1"),
                port=int(acc_cfg.get("ctrader_port") or 8888),
                account_id=int(acc_cfg.get("ctrader_account_id") or 0),
            )
            try:
                if ct_client.connect():
                    resolved_sym = ct_client.resolve_symbol(raw_sym) or SymbolMapper.resolve_symbol(target_acc_id, raw_sym) or raw_sym
                    s_info = ct_client.get_symbol_info(resolved_sym)
                    digits = int(getattr(s_info, "digits", 2) or 2) if s_info else 2

                    bridge_tf = tf_str
                    agg_sec = 0
                    if tf_str == "M30":
                        bridge_tf, agg_sec = "M15", 1800
                    elif tf_str == "H4":
                        bridge_tf, agg_sec = "H1", 14400
                    elif tf_str == "D1":
                        bridge_tf, agg_sec = "H1", 86400

                    res = ct_client.send_command(
                        {
                            "cmd": "GET_TRENDBARS",
                            "symbol": resolved_sym,
                            "timeframe": bridge_tf,
                            "from_ts": int(fetch_from),
                            "to_ts": int(fetch_to),
                        },
                        timeout=12.0,
                    )
                    raw_bars = (res.get("bars") or []) if (res and res.get("ok")) else []
                    if not raw_bars:
                        for fb_tf, fb_sec in [("M5", 300), ("M15", 900), ("H1", 3600)]:
                            if fb_tf == bridge_tf:
                                continue
                            fb_res = ct_client.send_command(
                                {
                                    "cmd": "GET_TRENDBARS",
                                    "symbol": resolved_sym,
                                    "timeframe": fb_tf,
                                    "from_ts": max(0, int(fetch_from) - 100 * fb_sec),
                                    "to_ts": int(fetch_to) + 100 * fb_sec,
                                },
                                timeout=12.0,
                            )
                            if fb_res and fb_res.get("ok") and fb_res.get("bars"):
                                raw_bars = fb_res.get("bars") or []
                                tf_str = fb_tf
                                agg_sec = 0
                                break
                    candles = []
                    seen_t = set()
                    for r in raw_bars:
                        t_val = int(r["time"])
                        if t_val in seen_t:
                            continue
                        seen_t.add(t_val)
                        candles.append({
                            "time": t_val,
                            "open": float(r["open"]),
                            "high": float(r["high"]),
                            "low": float(r["low"]),
                            "close": float(r["close"]),
                        })
                    candles.sort(key=lambda x: x["time"])

                    if agg_sec > 0 and candles:
                        buckets = {}
                        for c in candles:
                            b_time = (c["time"] // agg_sec) * agg_sec
                            if b_time not in buckets:
                                buckets[b_time] = dict(c, time=b_time)
                            else:
                                b = buckets[b_time]
                                if c["high"] > b["high"]:
                                    b["high"] = c["high"]
                                if c["low"] < b["low"]:
                                    b["low"] = c["low"]
                                b["close"] = c["close"]
                        candles = [buckets[k] for k in sorted(buckets.keys())]

                    return JSONResponse({
                        "symbol": resolved_sym,
                        "requested_symbol": raw_sym,
                        "account_id": target_acc_id,
                        "platform": "ctrader",
                        "timeframe": tf_str,
                        "digits": digits,
                        "pip_size": pa.get_pip_size(resolved_sym),
                        "candles": candles,
                    })
            finally:
                try:
                    ct_client.disconnect()
                except Exception:
                    pass

            return JSONResponse({"error": "cTrader bridge not connected", "platform": "ctrader", "candles": []}, status_code=503)

        # --- MT5 Path ---
        import MetaTrader5 as mt5
        tf_map = {
            "M1": (mt5.TIMEFRAME_M1, 60),
            "M5": (mt5.TIMEFRAME_M5, 300),
            "M15": (mt5.TIMEFRAME_M15, 900),
            "M30": (mt5.TIMEFRAME_M30, 1800),
            "H1": (mt5.TIMEFRAME_H1, 3600),
            "H4": (mt5.TIMEFRAME_H4, 14400),
            "D1": (mt5.TIMEFRAME_D1, 86400),
        }
        tf_const, tf_sec = tf_map.get(tf_str, (mt5.TIMEFRAME_M15, 900))

        # 1. Try worker process if this is a secondary MT5 account with an active worker
        if target_acc_id != "acc_1":
            worker_res = acc_mgr.get_candles(target_acc_id, raw_sym, tf_str, fetch_from, fetch_to, timeout=3.0)
            if worker_res and worker_res.get("candles"):
                worker_res["account_id"] = target_acc_id
                worker_res["pip_size"] = pa.get_pip_size(worker_res.get("symbol") or raw_sym)
                return JSONResponse(worker_res)

        # 2. Direct MT5 fetch (uses existing main-process MT5 connection or initializes terminal)
        if not is_connected():
            mt5_path = acc_cfg.get("mt5_path") or (acc_mgr.accounts_config.get("acc_1", {}) or {}).get("mt5_path")
            if mt5_path:
                mt5.initialize(path=mt5_path)
            else:
                mt5.initialize()

        resolved_sym = SymbolMapper.resolve_symbol(target_acc_id, raw_sym) or raw_sym
        s_info = mt5.symbol_info(resolved_sym)
        if not s_info and resolved_sym != raw_sym:
            s_info = mt5.symbol_info(raw_sym)
            if s_info:
                resolved_sym = raw_sym
        if not s_info:
            # Try common suffix fallback (.sc)
            for cand in (f"{raw_sym.split('.')[0]}.sc", raw_sym.split('.')[0]):
                s_info = mt5.symbol_info(cand)
                if s_info:
                    resolved_sym = cand
                    break

        if s_info and not s_info.visible:
            mt5.symbol_select(resolved_sym, True)
        digits = int(getattr(s_info, "digits", 2) or 2) if s_info else 2

        dt_from = datetime.fromtimestamp(fetch_from, tz=timezone.utc)
        dt_to = datetime.fromtimestamp(fetch_to, tz=timezone.utc)
        rates = mt5.copy_rates_range(resolved_sym, tf_const, dt_from, dt_to)
        if rates is None or len(rates) == 0:
            rates = mt5.copy_rates_from(resolved_sym, tf_const, dt_to, 300)
        if rates is None or len(rates) == 0:
            rates = mt5.copy_rates_from_pos(resolved_sym, tf_const, 0, 300)

        candles = []
        seen_t = set()
        if rates is not None and len(rates) > 0:
            for r in rates:
                t_val = int(r["time"])
                if t_val in seen_t:
                    continue
                seen_t.add(t_val)
                candles.append({
                    "time": t_val,
                    "open": float(r["open"]),
                    "high": float(r["high"]),
                    "low": float(r["low"]),
                    "close": float(r["close"]),
                })
            candles.sort(key=lambda x: x["time"])

        return JSONResponse({
            "symbol": resolved_sym,
            "requested_symbol": raw_sym,
            "account_id": target_acc_id,
            "timeframe": tf_str,
            "digits": digits,
            "pip_size": pa.get_pip_size(resolved_sym),
            "candles": candles,
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e), "candles": []}, status_code=500)

SETTINGS_FILE = "providers_config.json"
SETTINGS_BACKUP_FILE = "providers_config.json.bak"

def _atomic_save_json(filepath: str, data: dict):
    import json
    import os
    import shutil
    tmp_path = filepath + ".tmp"
    with open(tmp_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, filepath)
    try:
        shutil.copy2(filepath, SETTINGS_BACKUP_FILE)
    except Exception:
        pass

def load_settings():
    import json
    import os
    res = {}
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if content:
                    res = json.loads(content)
        except Exception as e:
            print(f"[WARN] Error reading {SETTINGS_FILE}:", e)
    elif os.path.exists(SETTINGS_BACKUP_FILE):
        try:
            with open(SETTINGS_BACKUP_FILE, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if content:
                    res = json.loads(content)
        except Exception as e:
            print(f"[WARN] Error reading {SETTINGS_BACKUP_FILE}:", e)

    if isinstance(res, dict):
        for pid, pcfg in res.items():
            if isinstance(pcfg, dict) and "max_lot" not in pcfg:
                pcfg["max_lot"] = 10.0
    return res

@app.get("/api/settings")
async def get_settings():
    return JSONResponse(load_settings())

@app.get("/api/providers/{provider_id}/settings")
async def get_provider_settings(provider_id: str, account_id: Optional[str] = None):
    p_id = str(provider_id).strip()
    from account_manager import AccountManager
    acc_mgr = AccountManager.get_instance()
    acc_mgr.load_accounts()
    target_acc = str(account_id).strip() if account_id else (acc_mgr.active_account_id or "acc_1")

    # First attempt to load account-specific settings for this provider
    settings = acc_mgr.get_account_settings_for_provider(target_acc, p_id)
    if settings and any(k in settings for k in ("risk_value", "risk_mode", "name", "channel")):
        return JSONResponse({"status": "success", "provider_id": p_id, "account_id": target_acc, "settings": settings})

    all_cfg = load_settings()
    if p_id in all_cfg:
        return JSONResponse({"status": "success", "provider_id": p_id, "account_id": target_acc, "settings": all_cfg[p_id]})
    return JSONResponse({"error": "Provider not found", "provider_id": p_id}, status_code=404)

@app.post("/api/providers/{provider_id}/settings")
async def save_provider_settings_endpoint(provider_id: str, request: Request):
    try:
        data = await request.json()
        p_id = str(provider_id).strip()
        from account_manager import AccountManager
        acc_mgr = AccountManager.get_instance()
        acc_mgr.load_accounts()

        # Determine target account safely without accidentally cross-contaminating accounts:
        # 1. Explicit account_id in request body
        target_acc = str(data.get("account_id") or "").strip()
        # 2. Query parameter if provided
        if not target_acc or target_acc not in acc_mgr.accounts_config:
            target_acc = str(request.query_params.get("account_id") or "").strip()
        # 3. Check which account currently owns this provider in accounts_config
        if not target_acc or target_acc not in acc_mgr.accounts_config:
            for a_id, a_cfg in acc_mgr.accounts_config.items():
                if p_id in [str(x) for x in a_cfg.get("providers", [])]:
                    target_acc = a_id
                    break
        # 4. Check base providers_config.json for designated account_id
        if not target_acc or target_acc not in acc_mgr.accounts_config:
            cfg_base = load_settings()
            if p_id in cfg_base and str(cfg_base[p_id].get("account_id") or "") in acc_mgr.accounts_config:
                target_acc = str(cfg_base[p_id]["account_id"])
        # 5. Fallback to active account only if provider is completely unassigned
        if not target_acc or target_acc not in acc_mgr.accounts_config:
            target_acc = acc_mgr.active_account_id or "acc_1"

        # Update account-specific settings for this provider
        data_to_save = dict(data)
        data_to_save["account_id"] = target_acc
        acc_mgr.set_account_settings_for_provider(target_acc, p_id, data_to_save)

        # Also update providers_config.json if this provider exists in base settings
        cfg = load_settings()
        if p_id in cfg:
            cfg[p_id].update({k: v for k, v in data_to_save.items() if v is not None})
            _atomic_save_json(SETTINGS_FILE, cfg)

        return JSONResponse({
            "status": "success",
            "provider_id": p_id,
            "account_id": target_acc,
            "settings": acc_mgr.get_account_settings_for_provider(target_acc, p_id)
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/providers")
async def add_single_provider(request: Request):
    """
    Creates a new signal provider and assigns it cleanly to the specified account.
    """
    try:
        data = await request.json()
        name = str(data.get("name") or "").strip()
        channel = str(data.get("channel") or "").strip()
        topic_id = int(data.get("topic_id") or 0)
        target_acc = str(data.get("account_id") or "").strip()

        if not channel:
            return JSONResponse({"status": "error", "error": "Telegram channel is required"}, status_code=400)
        if not name:
            return JSONResponse({"status": "error", "error": "Provider name is required"}, status_code=400)

        from account_manager import AccountManager
        acc_mgr = AccountManager.get_instance()
        acc_mgr.load_accounts()

        if not target_acc or target_acc not in acc_mgr.accounts_config:
            target_acc = acc_mgr.active_account_id or "acc_1"

        current_settings = load_settings()
        int_ids = [int(k) for k in current_settings.keys() if str(k).isdigit()]
        new_id = str(max(int_ids) + 1 if int_ids else 1)
        magic_number = 400400 + int(new_id)

        risk_mode = "fixed" if str(data.get("risk_mode", "")).lower() == "fixed" else "percent"
        try:
            risk_value = float(data.get("risk_value", 1.0))
        except (TypeError, ValueError):
            risk_value = 1.0
        try:
            max_lot = float(data.get("max_lot", 10.0))
        except (TypeError, ValueError):
            max_lot = 10.0
        try:
            tp1 = float(data.get("tp1_percent", 60.0))
            tp2 = float(data.get("tp2_percent", 30.0))
            tp3 = float(data.get("tp3_percent", 10.0))
        except (TypeError, ValueError):
            tp1, tp2, tp3 = 60.0, 30.0, 10.0

        prov_data = {
            "name": name,
            "channel": channel,
            "topic_id": topic_id,
            "account_id": target_acc,
            "magic_number": magic_number,
            "risk_mode": risk_mode,
            "risk_value": risk_value,
            "tp1_percent": tp1,
            "tp2_percent": tp2,
            "tp3_percent": tp3,
            "tp1_close_percent": tp1,
            "sl_tp1_strategy": str(data.get("sl_tp1_strategy") or "ENTRY"),
            "sl_tp2_strategy": str(data.get("sl_tp2_strategy") or "TP1"),
            "sl_tp3_strategy": str(data.get("sl_tp3_strategy") or "TP2"),
            "sl_breakeven_at": str(data.get("sl_breakeven_at") or "TP1"),
            "spread_offset_entry": bool(data.get("spread_offset_entry", True)),
            "spread_offset_tp": bool(data.get("spread_offset_tp", True)),
            "spread_offset_sl": bool(data.get("spread_offset_sl", True)),
            "trading_enabled": bool(data.get("trading_enabled", True)),
            "entry_zone_mode": str(data.get("entry_zone_mode") or "furthest"),
            "max_lot": max_lot
        }

        current_settings[new_id] = prov_data
        _atomic_save_json(SETTINGS_FILE, current_settings)

        acc_mgr.assign_provider_to_account(target_acc, new_id)
        acc_mgr.set_account_settings_for_provider(target_acc, new_id, prov_data)
        acc_mgr.save_accounts()

        return JSONResponse({
            "status": "success",
            "provider_id": new_id,
            "account_id": target_acc,
            "provider": prov_data
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)

@app.post("/api/settings")
async def save_settings(request: Request):
    try:
        data = await request.json()
        if isinstance(data, dict):
            from account_manager import AccountManager
            acc_mgr = AccountManager.get_instance()
            acc_mgr.load_accounts()
            active_id = acc_mgr.active_account_id or "acc_1"
            all_assigned = set()
            for a_cfg in acc_mgr.accounts_config.values():
                for p in a_cfg.get("providers", []):
                    all_assigned.add(str(p))

            for p_id, p_cfg in data.items():
                if not isinstance(p_cfg, dict):
                    continue
                target_acc = str(p_cfg.get("account_id") or "").strip()
                if target_acc and target_acc in acc_mgr.accounts_config:
                    acc_mgr.assign_provider_to_account(target_acc, str(p_id))
                    # Only set settings if account doesn't already have account-specific settings
                    existing_s = acc_mgr.accounts_config.get(target_acc, {}).get("provider_settings", {}).get(str(p_id))
                    if not existing_s:
                        acc_mgr.set_account_settings_for_provider(target_acc, str(p_id), p_cfg)
                elif str(p_id) not in all_assigned:
                    p_cfg["account_id"] = active_id
                    acc_mgr.assign_provider_to_account(active_id, str(p_id))
                    acc_mgr.set_account_settings_for_provider(active_id, str(p_id), p_cfg)

        _atomic_save_json(SETTINGS_FILE, data)
        return JSONResponse({"status": "success"})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

def add_all_telegram_providers(
    risk_mode: str = "percent",
    risk_value: float = 1.0,
    tp1_percent: float = 60.0,
    tp2_percent: float = 30.0,
    tp3_percent: float = 10.0,
    max_lot: float = 10.0,
    account_id: str = None,
    skip_existing: bool = True,
    dialogs: list = None
) -> dict:
    import signal_listener
    from account_manager import AccountManager

    # If dialogs not explicitly supplied, load from cache
    if dialogs is None:
        dialogs = signal_listener.load_cached_dialogs()

    if not dialogs:
        return {"error": "No Telegram dialogs found. Please scan/refresh Telegram dialogs first."}

    acc_mgr = AccountManager.get_instance()
    target_acc = str(account_id or "").strip()
    if not target_acc or target_acc not in acc_mgr.accounts_config:
        target_acc = acc_mgr.active_account_id or "acc_1"

    current_settings = load_settings()

    # Track existing channel + topic combinations to prevent duplicates
    existing_pairs = set()
    for pid, pdata in current_settings.items():
        if isinstance(pdata, dict):
            ch = str(pdata.get("channel") or "").strip()
            top = int(pdata.get("topic_id") or 0)
            existing_pairs.add((ch, top))

    # Determine highest existing integer ID
    int_ids = [int(k) for k in current_settings.keys() if str(k).isdigit()]
    curr_id = max(int_ids) if int_ids else 1

    # Validate numbers
    risk_mode = "fixed" if str(risk_mode).lower() == "fixed" else "percent"
    try:
        risk_value = float(risk_value)
    except (TypeError, ValueError):
        risk_value = 1.0
    try:
        tp1_percent = float(tp1_percent)
    except (TypeError, ValueError):
        tp1_percent = 60.0
    try:
        tp2_percent = float(tp2_percent)
    except (TypeError, ValueError):
        tp2_percent = 30.0
    try:
        tp3_percent = float(tp3_percent)
    except (TypeError, ValueError):
        tp3_percent = 10.0
    try:
        max_lot = float(max_lot)
    except (TypeError, ValueError):
        max_lot = 10.0

    added = []
    skipped = 0

    for d in dialogs:
        if not isinstance(d, dict):
            continue
        ch_id = d.get("channel_id")
        if ch_id is None:
            continue
        channel_str = str(ch_id).strip()
        topic_int = int(d.get("topic_id") or 0)

        if skip_existing and (channel_str, topic_int) in existing_pairs:
            skipped += 1
            continue

        c_name = (d.get("channel_name") or "").strip()
        t_name = (d.get("topic_name") or "").strip()
        d_name = (d.get("display_name") or "").strip()
        d_type = d.get("type", "")

        if d_type == "topic" and t_name:
            name = f"{c_name} - {t_name}" if c_name else t_name
        elif c_name:
            name = c_name
        elif d_name:
            name = d_name
        else:
            name = f"Telegram {channel_str}"

        name = name.strip('\'" \t\r\n')
        if not name:
            name = f"Telegram Provider {curr_id + 1}"

        curr_id += 1
        new_pid = str(curr_id)
        magic_number = 400400 + curr_id

        prov_data = {
            "name": name,
            "channel": channel_str,
            "topic_id": topic_int,
            "account_id": target_acc,
            "magic_number": magic_number,
            "risk_mode": risk_mode,
            "risk_value": risk_value,
            "tp1_percent": tp1_percent,
            "tp2_percent": tp2_percent,
            "tp3_percent": tp3_percent,
            "tp1_close_percent": tp1_percent,
            "sl_tp1_strategy": "ENTRY",
            "sl_tp2_strategy": "TP1",
            "sl_tp3_strategy": "TP2",
            "sl_breakeven_at": "TP1",
            "spread_offset_entry": True,
            "spread_offset_tp": True,
            "spread_offset_sl": True,
            "trading_enabled": True,
            "entry_zone_mode": "furthest",
            "max_lot": max_lot
        }

        current_settings[new_pid] = prov_data
        existing_pairs.add((channel_str, topic_int))
        acc_mgr.assign_provider_to_account(target_acc, new_pid)
        added.append({"id": new_pid, "name": name, "channel": channel_str, "topic_id": topic_int})

    if added:
        _atomic_save_json(SETTINGS_FILE, current_settings)
        try:
            acc_mgr.save_accounts()
        except Exception as ex:
            print(f"[WARN] Failed saving accounts in add_all_telegram_providers: {ex}")

    return {
        "status": "success",
        "added_count": len(added),
        "skipped_count": skipped,
        "total_providers": len(current_settings),
        "account_id": target_acc,
        "added_sample": added[:5]
    }

@app.post("/api/providers/add-all")
async def api_add_all_providers(request: Request):
    try:
        data = await request.json()
        if not isinstance(data, dict):
            data = {}
        res = add_all_telegram_providers(
            risk_mode=data.get("risk_mode", "percent"),
            risk_value=data.get("risk_value", 1.0),
            tp1_percent=data.get("tp1_percent", 60.0),
            tp2_percent=data.get("tp2_percent", 30.0),
            tp3_percent=data.get("tp3_percent", 10.0),
            max_lot=data.get("max_lot", 10.0),
            account_id=data.get("account_id"),
            skip_existing=data.get("skip_existing", True),
            dialogs=data.get("dialogs")
        )
        if "error" in res:
            return JSONResponse(res, status_code=400)
        return JSONResponse(res)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/providers/{provider_id}/toggle-trading")
async def toggle_provider_trading(provider_id: str):
    try:
        cfg = load_settings()
        p_id = str(provider_id)
        if p_id not in cfg:
            return JSONResponse({"error": "Provider not found"}, status_code=404)
        current = cfg[p_id].get("trading_enabled", True)
        cfg[p_id]["trading_enabled"] = not current
        _atomic_save_json(SETTINGS_FILE, cfg)
        return JSONResponse({"status": "success", "provider_id": p_id, "trading_enabled": cfg[p_id]["trading_enabled"]})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/providers/{provider_id}/copy")
async def copy_provider_to_account(provider_id: str, request: Request):
    try:
        data = await request.json()
        target_account_id = data.get("target_account_id")
        target_account_ids = data.get("target_account_ids")
        source_account_id = data.get("source_account_id")
        settings_override = data.get("settings") or data.get("settings_override")

        if not target_account_ids and target_account_id:
            target_account_ids = [target_account_id]

        if not target_account_ids:
            return JSONResponse({"error": "Missing target_account_id parameter"}, status_code=400)

        from account_manager import AccountManager
        acc_mgr = AccountManager.get_instance()

        results = []
        for tgt_id in target_account_ids:
            res = acc_mgr.copy_provider_to_account(
                source_acc_id=source_account_id,
                provider_id=provider_id,
                target_acc_id=tgt_id,
                settings_override=settings_override
            )
            results.append(res)

        try:
            import json
            await manager.broadcast(json.dumps({"event": "update"}))
        except Exception:
            pass

        if len(results) == 1:
            return JSONResponse(results[0])
        else:
            acc_names = [r.get("target_account_name", r.get("target_account_id")) for r in results]
            return JSONResponse({
                "status": "success",
                "message": f"Provider successfully copied to {len(results)} accounts: {', '.join(acc_names)}.",
                "results": results,
                "provider_id": str(provider_id),
                "target_account_id": target_account_ids[0]
            })
    except ValueError as ve:
        return JSONResponse({"error": str(ve)}, status_code=400)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.delete("/api/providers/{provider_id}")
async def delete_provider(provider_id: str):
    try:
        cfg = load_settings()
        p_id = str(provider_id)
        if p_id in cfg:
            del cfg[p_id]
            _atomic_save_json(SETTINGS_FILE, cfg)
            
            # Clean up provider from accounts_config.json if referenced
            try:
                from account_manager import AccountManager
                acc_mgr = AccountManager.get_instance()
                changed = False
                for a_id, a_cfg in acc_mgr.accounts_config.items():
                    prov_list = a_cfg.get("providers", [])
                    if p_id in [str(x) for x in prov_list]:
                        a_cfg["providers"] = [x for x in prov_list if str(x) != p_id]
                        changed = True
                    if "provider_settings" in a_cfg and p_id in a_cfg["provider_settings"]:
                        del a_cfg["provider_settings"][p_id]
                        changed = True
                if changed:
                    acc_mgr.save_accounts()
            except Exception as ex:
                print(f"[WARN] Failed cleaning up provider {p_id} from accounts: {ex}")

            return JSONResponse({"status": "deleted"})
        return JSONResponse({"error": "Provider not found"}, status_code=404)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/account/reset")
async def reset_account(request: Request):
    """
    Resets account data with explicit options:
    - keep_providers: bool (default True). When True, preserves all configured providers and custom settings.
                      When False (full factory reset), permanently removes all providers.
    - scope: "current" | "all" (default "current"). Whether to reset only the selected account or all accounts.
    - account_id: str (optional). Target account ID if scope is current.
    - close_open_trades: bool (default True). Whether to close open trades and cancel pending orders.
    """
    try:
        data = {}
        try:
            data = await request.json()
        except Exception:
            pass

        from account_manager import AccountManager
        acc_mgr = AccountManager.get_instance()

        # Parse options
        # Default keep_providers to True to prevent accidental provider loss
        if "keep_providers" in data:
            keep_providers = bool(data["keep_providers"])
        elif "reset_type" in data:
            keep_providers = (str(data["reset_type"]).lower() != "full")
        elif "keep_primary" in data:
            keep_providers = bool(data["keep_primary"])
        else:
            keep_providers = True

        scope = str(data.get("scope", "current")).lower()
        target_acc_id = data.get("account_id") or acc_mgr.active_account_id or "acc_1"
        close_open_trades = bool(data.get("close_open_trades", True))

        if scope == "current" and target_acc_id in acc_mgr.accounts_config:
            target_pids = set(str(p) for p in acc_mgr.get_account_providers(target_acc_id))
            target_accounts = [target_acc_id]
        else:
            scope = "all"
            target_pids = None
            target_accounts = list(acc_mgr.accounts_config.keys())

        # 0. Close all existing open positions and cancel pending orders if requested
        closed_trades_total = 0
        cancelled_orders_total = 0
        close_summary = {}
        if close_open_trades:
            for acc_id in target_accounts:
                try:
                    summary = acc_mgr.close_and_cancel_account_trades(
                        acc_id,
                        provider_ids=list(target_pids) if (scope == "current" and target_pids) else None
                    )
                    c_pos = int(summary.get("closed_positions_count") or 0)
                    c_ord = int(summary.get("cancelled_orders_count") or 0)
                    closed_trades_total += c_pos
                    cancelled_orders_total += c_ord
                    close_summary[acc_id] = summary
                except Exception as ex:
                    print(f"[Reset] Error closing trades for {acc_id}: {ex}")
                    close_summary[acc_id] = {"error": str(ex)}

        # 1. Get updated live balance from broker for target account(s) to benchmark starting_balance
        live_balance = None
        try:
            if target_acc_id == "acc_1" or (scope == "all" and "acc_1" in target_accounts):
                acc = get_account_info()
                if acc and getattr(acc, 'balance', None) is not None:
                    live_balance = float(acc.balance)
        except Exception:
            pass

        if live_balance is None or (scope == "current" and target_acc_id != "acc_1"):
            try:
                w_state = acc_mgr.get_active_state(target_acc_id, timeout=0.8)
                if w_state and "balance" in w_state and float(w_state.get("balance", 0.0)) > 0:
                    live_balance = float(w_state["balance"])
            except Exception:
                pass

        # 2. Clear database trades/signals/analytics and update starting_balance to live balance
        import db
        reset_ts = db.reset_account_data(
            provider_ids=target_pids,
            keep_providers=keep_providers,
            new_starting_balance=live_balance,
            account_id=target_acc_id if scope == "current" else None
        )

        # Clear in-memory profit caches
        global total_profit_cache
        total_profit_cache["time"] = 0
        total_profit_cache["trade_profit"] = 0.0
        total_profit_cache["low_to_high_profit"] = 0.0
        total_profit_cache["daily_profit"] = 0.0
        total_profit_cache["monthly_profit"] = 0.0
        total_profit_cache["yearly_profit"] = 0.0
        if live_balance is not None:
            total_profit_cache["deposits"] = live_balance

        # 3. Modify providers_config.json ONLY if keep_providers is False (Full Factory Reset)
        if not keep_providers:
            cfg = load_settings()
            if scope == "all":
                _atomic_save_json(SETTINGS_FILE, {})
                if os.path.exists(SETTINGS_BACKUP_FILE):
                    try:
                        with open(SETTINGS_BACKUP_FILE, 'w', encoding='utf-8') as f:
                            json.dump({}, f, indent=4)
                    except Exception:
                        pass
                for a_id, a_cfg in acc_mgr.accounts_config.items():
                    a_cfg["providers"] = []
                    a_cfg["provider_settings"] = {}
                acc_mgr.save_accounts()
            else:
                # Remove only the target account's providers from providers_config.json
                if target_pids:
                    for pid in target_pids:
                        cfg.pop(str(pid), None)
                    _atomic_save_json(SETTINGS_FILE, cfg)
                a_cfg = acc_mgr.accounts_config.get(target_acc_id)
                if a_cfg:
                    a_cfg["providers"] = []
                    if "provider_settings" in a_cfg:
                        a_cfg["provider_settings"] = {}
                    acc_mgr.save_accounts()

        # 4. Clear JSON trade states & logs
        import glob
        if scope == "all":
            try:
                with open("trade_log.json", "w", encoding="utf-8") as f:
                    json.dump([], f, indent=4)
            except Exception:
                pass
            for state_file in glob.glob("trade_state*.json"):
                try:
                    with open(state_file, "w", encoding="utf-8") as f:
                        json.dump({}, f, indent=4)
                except Exception:
                    pass
            # Reset mt5_executor runtime memory
            try:
                import mt5_executor
                mt5_executor.active_trade_groups = {}
                mt5_executor.message_to_tickets = {}
            except Exception:
                pass
            # Broadcast state reset to all running workers
            try:
                acc_mgr.send_broadcast_command("RESET_STATE", {"provider_ids": None})
            except Exception:
                pass
        else:
            # Current account only
            state_file = f"trade_state_{target_acc_id}.json" if target_acc_id != "acc_1" else "trade_state.json"
            try:
                if os.path.exists(state_file):
                    with open(state_file, "w", encoding="utf-8") as f:
                        json.dump({}, f, indent=4)
                if target_acc_id == "acc_1" and os.path.exists("trade_state_acc_1.json"):
                    with open("trade_state_acc_1.json", "w", encoding="utf-8") as f:
                        json.dump({}, f, indent=4)
            except Exception:
                pass

            # Filter trade_log.json for current account
            try:
                if os.path.exists("trade_log.json"):
                    with open("trade_log.json", "r", encoding="utf-8") as f:
                        logs = json.load(f)
                    filtered_logs = []
                    target_platform = str(acc_mgr.accounts_config.get(target_acc_id, {}).get("platform", "")).lower()
                    other_pids = set()
                    for other_id, other_cfg in acc_mgr.accounts_config.items():
                        if other_id != target_acc_id:
                            other_pids.update(str(p) for p in other_cfg.get("providers", []))
                    exclusive_pids = (set(str(p) for p in target_pids) - other_pids) if target_pids else set()
                    for entry in logs:
                        execs = entry.get("executions", [])
                        remaining_execs = []
                        for ex in execs:
                            ex_acc = str(ex.get("account_id", ""))
                            ex_plat = str(ex.get("platform", "mt5")).lower()
                            ex_pid = str(ex.get("provider_id", ""))
                            if ex_acc == target_acc_id:
                                continue
                            if target_platform == "ctrader" and ex_plat == "ctrader":
                                continue
                            if ex_pid and ex_pid in exclusive_pids:
                                continue
                            remaining_execs.append(ex)
                        if remaining_execs:
                            entry["executions"] = remaining_execs
                            filtered_logs.append(entry)
                    with open("trade_log.json", "w", encoding="utf-8") as f:
                        json.dump(filtered_logs, f, indent=4)
            except Exception:
                pass

            if target_acc_id == "acc_1":
                try:
                    import mt5_executor
                    if target_pids:
                        mt5_executor.active_trade_groups = {
                            k: v for k, v in mt5_executor.active_trade_groups.items()
                            if str(v.get("provider_id")) not in target_pids
                        }
                    else:
                        mt5_executor.active_trade_groups = {}
                        mt5_executor.message_to_tickets = {}
                except Exception:
                    pass
            else:
                try:
                    worker = acc_mgr.workers.get(target_acc_id)
                    if worker:
                        worker["in_q"].put({"cmd": "RESET_STATE", "payload": {"provider_ids": list(target_pids) if target_pids else None}})
                except Exception:
                    pass

        msg_parts = [
            "Account trade data successfully reset. All providers and settings preserved." if keep_providers
            else "Account and all providers completely reset (factory reset)."
        ]
        if close_open_trades:
            msg_parts.append(f"Closed {closed_trades_total} open position(s) and cancelled {cancelled_orders_total} pending order(s).")
        msg = " ".join(msg_parts)

        return JSONResponse({
            "status": "success",
            "message": msg,
            "keep_providers": keep_providers,
            "scope": scope,
            "account_id": target_acc_id,
            "reset_time": reset_ts,
            "closed_trades_count": closed_trades_total,
            "cancelled_orders_count": cancelled_orders_total,
            "close_summary": close_summary
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/copier/toggle-pause")
async def toggle_copier_pause(request: Request):
    try:
        body = {}
        try:
            body = await request.json()
        except Exception:
            pass
        from account_manager import AccountManager
        acc_mgr = AccountManager.get_instance()
        acc_mgr.load_accounts()
        target_id = body.get("account_id") or acc_mgr.active_account_id or "acc_1"
        new_state = acc_mgr.toggle_account_pause(target_id)
        return JSONResponse({"status": "success", "account_id": target_id, "is_paused": new_state})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/copier/pause")
async def pause_copier(request: Request):
    try:
        body = {}
        try:
            body = await request.json()
        except Exception:
            pass
        from account_manager import AccountManager
        acc_mgr = AccountManager.get_instance()
        acc_mgr.load_accounts()
        target_id = body.get("account_id") or acc_mgr.active_account_id or "acc_1"
        cfg = acc_mgr.accounts_config.setdefault(target_id, {})
        cfg["is_paused"] = True
        acc_mgr.save_accounts()
        return JSONResponse({"status": "success", "account_id": target_id, "is_paused": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/copier/resume")
async def resume_copier(request: Request):
    try:
        body = {}
        try:
            body = await request.json()
        except Exception:
            pass
        from account_manager import AccountManager
        acc_mgr = AccountManager.get_instance()
        acc_mgr.load_accounts()
        target_id = body.get("account_id") or acc_mgr.active_account_id or "acc_1"
        cfg = acc_mgr.accounts_config.setdefault(target_id, {})
        cfg["is_paused"] = False
        acc_mgr.save_accounts()
        return JSONResponse({"status": "success", "account_id": target_id, "is_paused": False})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/telegram/dialogs")
async def get_telegram_dialogs(refresh: bool = False):
    import signal_listener
    if refresh and signal_listener.telegram_client and signal_listener.telegram_loop:
        try:
            future = asyncio.run_coroutine_threadsafe(
                signal_listener.fetch_dialogs_and_topics(force_refresh=True),
                signal_listener.telegram_loop
            )
            dialogs = await asyncio.wait_for(asyncio.wrap_future(future), timeout=25.0)
            return JSONResponse({"dialogs": dialogs, "refreshed": True})
        except Exception as e:
            print("[WARN] Error refreshing dialogs via Telegram:", e)
    
    dialogs = signal_listener.load_cached_dialogs()
    return JSONResponse({"dialogs": dialogs, "refreshed": False})

@app.get("/api/accounts")
async def get_accounts():
    from account_manager import AccountManager
    acc_mgr = AccountManager.get_instance()
    return JSONResponse({
        "accounts": acc_mgr.get_accounts_list(),
        "active_account_id": acc_mgr.active_account_id,
    })


@app.post("/api/accounts")
async def save_account(request: Request):
    try:
        from account_manager import AccountManager
        payload = await request.json()
        acc_mgr = AccountManager.get_instance()
        acc_id = payload.get("id")
        if acc_id and acc_id in acc_mgr.accounts_config:
            # Preserve existing secrets if masked or omitted
            existing = acc_mgr.accounts_config[acc_id]
            for secret_key in ("mt5_password", "ctrader_client_secret", "ctrader_access_token", "provider_settings"):
                if not payload.get(secret_key) and existing.get(secret_key):
                    payload[secret_key] = existing[secret_key]

        saved = acc_mgr.add_or_update_account(payload)
        if payload.get("set_active", False):
            acc_mgr.set_active_account(saved["id"])
        return JSONResponse({"status": "success", "account": saved, "accounts": acc_mgr.get_accounts_list()})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/accounts/active")
async def switch_active_account(request: Request):
    try:
        from account_manager import AccountManager
        payload = await request.json()
        acc_id = str(payload.get("account_id") or "acc_1")
        acc_mgr = AccountManager.get_instance()
        ok = acc_mgr.set_active_account(acc_id)
        if not ok:
            return JSONResponse({"error": "Account not found"}, status_code=404)
        return JSONResponse({"status": "success", "active_account_id": acc_id, "accounts": acc_mgr.get_accounts_list()})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.delete("/api/accounts/{acc_id}")
async def remove_account(acc_id: str):
    try:
        if acc_id == "acc_1":
            return JSONResponse({"error": "Cannot delete primary default account (acc_1)."}, status_code=400)
        from account_manager import AccountManager
        acc_mgr = AccountManager.get_instance()
        ok = acc_mgr.delete_account(acc_id)
        return JSONResponse({"status": "deleted" if ok else "not_found", "accounts": acc_mgr.get_accounts_list()})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/accounts/{acc_id}/sync-symbols")
async def sync_account_symbols(acc_id: str):
    """Pulls symbol tickers directly from the broker and rebuilds the account symbol map."""
    try:
        from account_manager import AccountManager
        acc_mgr = AccountManager.get_instance()
        res = await asyncio.to_thread(acc_mgr.sync_symbols, acc_id)
        return JSONResponse(res)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/accounts/{acc_id}/symbols")
async def get_account_symbols(acc_id: str):
    """Returns the mapped symbol dictionary for an account."""
    try:
        from symbol_mapper import SymbolMapper
        mapping = SymbolMapper.load_cached_map(acc_id)
        raw_list = SymbolMapper._raw_symbols_cache.get(acc_id, [])
        return JSONResponse({
            "account_id": acc_id,
            "raw_broker_symbols_count": len(raw_list),
            "mapped_aliases_count": len(mapping),
            "sample_mappings": {k: mapping[k] for k in ["XAUUSD", "BTCUSD", "ETHUSD", "SOLUSD", "USOUSD", "NAS100", "US30"] if k in mapping}
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/accounts/{acc_id}/test-signal")
async def test_signal_on_account(acc_id: str, request: Request):
    """Executes a test signal or command specifically on a designated account for verification."""
    try:
        from account_manager import AccountManager
        payload = await request.json()
        signal = payload.get("signal", {})
        provider_id = str(payload.get("provider_id", "544"))
        message_id = payload.get("message_id")
        action = str(payload.get("action", "EXECUTE_SIGNAL")).upper().strip()

        acc_mgr = AccountManager.get_instance()
        acc_mgr.load_accounts()
        acc_mgr.ensure_workers_running(skip_primary_mt5=True)

        w = acc_mgr.workers.get(acc_id)
        if not w:
            return JSONResponse({"ok": False, "error": f"Worker for account {acc_id} not running or not found"}, status_code=404)

        req_id = f"test_{acc_id}_{int(time.time()*1000)}"
        if action == "EXECUTE_SIGNAL":
            p_settings = acc_mgr.get_account_settings_for_provider(acc_id, provider_id)
            w["in_q"].put({
                "cmd": "EXECUTE_SIGNAL",
                "req_id": req_id,
                "payload": {
                    "signal": signal,
                    "msg_id": message_id,
                    "provider_id": provider_id,
                    "settings": p_settings
                }
            })
        elif action == "MODIFY_SL":
            w["in_q"].put({
                "cmd": "MODIFY_SL",
                "req_id": req_id,
                "payload": {"reply_id": message_id, "new_sl": float(payload.get("new_sl", 0.0))}
            })
        elif action == "MODIFY_SIGNAL_LEVELS":
            w["in_q"].put({
                "cmd": "MODIFY_SIGNAL_LEVELS",
                "req_id": req_id,
                "payload": {
                    "message_id": message_id,
                    "new_tps": payload.get("new_tps", []),
                    "new_sl": float(payload.get("new_sl", 0.0)),
                    "new_entry": float(payload.get("new_entry", 0.0)),
                    "provider_id": provider_id
                }
            })
        elif action == "CLOSE_TRADE":
            w["in_q"].put({
                "cmd": "CLOSE_TRADE",
                "req_id": req_id,
                "payload": {"reply_id": message_id}
            })
        elif action == "CANCEL_TRADE":
            w["in_q"].put({
                "cmd": "CANCEL_TRADE",
                "req_id": req_id,
                "payload": {"reply_id": message_id}
            })
        elif action == "SET_BE":
            w["in_q"].put({
                "cmd": "SET_BE",
                "req_id": req_id,
                "payload": {"provider_id": provider_id}
            })
        elif action == "CLOSE_ALL":
            w["in_q"].put({
                "cmd": "CLOSE_ALL",
                "req_id": req_id,
                "payload": {"provider_id": provider_id}
            })
        elif action == "GET_STATE":
            w["in_q"].put({
                "cmd": "GET_STATE",
                "req_id": req_id,
                "payload": {}
            })
        else:
            return JSONResponse({"ok": False, "error": f"Unknown action {action}"}, status_code=400)

        # Wait for worker response
        deadline = time.time() + 12.0
        while time.time() < deadline:
            acc_mgr._drain_worker_out_q(acc_id)
            if req_id in acc_mgr.pending_results:
                msg = acc_mgr.pending_results.pop(req_id)
                return JSONResponse({"ok": True, "account_id": acc_id, "response": msg})
            await asyncio.sleep(0.05)

        return JSONResponse({"ok": False, "error": "Timed out waiting for worker response"}, status_code=504)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)



@app.post("/api/ctrader/test-connection")
async def test_ctrader_connection(request: Request):
    """Tests connection to the local SiriusBridge cBot in cTrader Desktop and auto-discovers account details."""
    try:
        from ctrader_client import CTraderClient
        payload = {}
        try:
            payload = await request.json()
        except Exception:
            pass
        host = str(payload.get("ctrader_host") or "127.0.0.1").strip()
        port = int(payload.get("ctrader_port") or 8888)

        def _probe():
            client = CTraderClient(host=host, port=port)
            ok = client.connect()
            if not ok:
                return {"connected": False, "error": client.last_error()}
            info = client.get_account_info()
            sym_count = len(client.symbols_by_name)
            client.disconnect()
            return {
                "connected": True,
                "login": getattr(info, "login", client.trader_login),
                "ctid_trader_account_id": client.ctid_trader_account_id,
                "balance": getattr(info, "balance", 0.0),
                "equity": getattr(info, "equity", 0.0),
                "server": getattr(info, "server", f"cTrader ({host}:{port})"),
                "symbols_loaded": sym_count,
                "env_type": client.env,
            }

        res = await asyncio.to_thread(_probe)
        return JSONResponse(res)
    except Exception as e:
        return JSONResponse({"connected": False, "error": str(e)}, status_code=500)


@app.get("/api/mt5/discovered-terminals")
async def get_discovered_mt5_terminals():
    """Scans Windows Registry, Program Files folders, and active running processes for MT5 terminal64.exe paths."""
    def _scan():
        import glob
        import winreg
        terminals = []
        seen_paths = set()
        running_paths = set()

        # 1. Active running processes
        try:
            import psutil
            for p in psutil.process_iter(['name', 'exe']):
                try:
                    pname = p.info.get('name') or ''
                    if 'terminal64' in pname.lower():
                        exe_path = p.info.get('exe')
                        if exe_path and os.path.exists(exe_path):
                            running_paths.add(os.path.normpath(exe_path).lower())
                except Exception:
                    pass
        except Exception:
            pass

        # 2. Windows Registry
        try:
            for root in (winreg.HKEY_LOCAL_MACHINE, winreg.HKEY_CURRENT_USER):
                for subkey in (r'Software\Microsoft\Windows\CurrentVersion\Uninstall', r'Software\Wow6432Node\Microsoft\Windows\CurrentVersion\Uninstall'):
                    try:
                        with winreg.OpenKey(root, subkey) as key:
                            for i in range(winreg.QueryInfoKey(key)[0]):
                                try:
                                    skey_name = winreg.EnumKey(key, i)
                                    with winreg.OpenKey(key, skey_name) as skey:
                                        try:
                                            disp, _ = winreg.QueryValueEx(skey, 'DisplayName')
                                        except Exception:
                                            disp = ''
                                        try:
                                            loc, _ = winreg.QueryValueEx(skey, 'InstallLocation')
                                        except Exception:
                                            loc = ''
                                        if loc:
                                            t_path = os.path.join(loc, 'terminal64.exe')
                                            if os.path.exists(t_path):
                                                norm_p = os.path.normpath(t_path)
                                                low_p = norm_p.lower()
                                                if low_p not in seen_paths:
                                                    seen_paths.add(low_p)
                                                    name = disp or os.path.basename(os.path.normpath(loc))
                                                    terminals.append({
                                                        'name': name,
                                                        'path': norm_p,
                                                        'is_running': low_p in running_paths
                                                    })
                                except Exception:
                                    pass
                    except Exception:
                        pass
        except Exception:
            pass

        # 3. File System sweep in Program Files
        scan_patterns = [
            r'C:\Program Files\*\terminal64.exe',
            r'C:\Program Files (x86)\*\terminal64.exe',
            r'C:\Program Files\*\*\terminal64.exe',
            r'C:\Program Files (x86)\*\*\terminal64.exe',
        ]
        for pat in scan_patterns:
            for fpath in glob.glob(pat):
                if os.path.exists(fpath):
                    norm_p = os.path.normpath(fpath)
                    low_p = norm_p.lower()
                    if low_p not in seen_paths:
                        seen_paths.add(low_p)
                        parts = norm_p.split(os.sep)
                        parent = parts[-2]
                        grandparent = parts[-3] if len(parts) >= 3 else ''
                        if 'program files' in grandparent.lower() or not grandparent:
                            name = parent
                        else:
                            name = f"{grandparent} ({parent})"
                        terminals.append({
                            'name': name,
                            'path': norm_p,
                            'is_running': low_p in running_paths
                        })

        # Enrich display names to distinguish multiple installations of the same broker
        for item in terminals:
            parts = item['path'].split(os.sep)
            if len(parts) >= 3:
                parent = parts[-2]
                if parent.lower() not in item['name'].lower() and 'program files' not in parent.lower():
                    item['display_name'] = f"{item['name']} ({parent})"
                else:
                    item['display_name'] = item['name']
            else:
                item['display_name'] = item['name']

        terminals.sort(key=lambda x: (not x['is_running'], x['display_name'].lower()))
        return terminals

    results = await asyncio.to_thread(_scan)
    return JSONResponse({"ok": True, "terminals": results, "count": len(results)})


@app.get("/api/ctrader/ports")
async def get_ctrader_port_allocation():
    """Scans loopback ports 8888-8899 to discover active SiriusBridge cBots and allocate the next free port."""
    def _probe_ports():
        import socket
        import json
        from account_manager import AccountManager
        acc_mgr = AccountManager.get_instance()
        acc_mgr.load_accounts()

        used_ports = set()
        for acc in acc_mgr.accounts_config.values():
            if str(acc.get("platform", "")).lower() == "ctrader":
                try:
                    p = int(acc.get("ctrader_port") or 8888)
                    used_ports.add(p)
                except Exception:
                    pass

        active_bridges = []
        free_ports = []

        for port in range(8888, 8910):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.12)
            try:
                s.connect(('127.0.0.1', port))
                s.settimeout(0.3)
                s.sendall(b'{"cmd":"PING"}\n')
                line = s.makefile('r', encoding='utf-8').readline()
                if line:
                    try:
                        data = json.loads(line.strip())
                        if data.get('ok'):
                            active_bridges.append({
                                'port': port,
                                'login': data.get('login'),
                                'broker': data.get('broker', 'cTrader'),
                                'balance': data.get('balance', 0.0),
                                'is_live': data.get('is_live', False),
                                'already_registered': port in used_ports
                            })
                    except Exception:
                        pass
                s.close()
            except Exception:
                free_ports.append(port)
                try:
                    s.close()
                except Exception:
                    pass

        unregistered = [b for b in active_bridges if not b['already_registered']]
        if unregistered:
            suggested = unregistered[0]['port']
        else:
            suggested = 8888
            for p in range(8888, 8950):
                if p not in used_ports:
                    suggested = p
                    break

        return {
            "ok": True,
            "suggested_port": suggested,
            "used_ports": sorted(list(used_ports)),
            "active_bridges": active_bridges,
            "free_ports": free_ports[:10]
        }

    res = await asyncio.to_thread(_probe_ports)
    return JSONResponse(res)


@app.get("/api/ctrader/cbot-code")
async def get_ctrader_cbot_code(port: int = 8888):
    """Returns the SiriusBridge.cs cBot C# source code configured for the specified port."""
    cbot_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cbot", "SiriusBridge.cs")
    if os.path.exists(cbot_path):
        with open(cbot_path, "r", encoding="utf-8") as f:
            code = f.read()
            if port and int(port) != 8888:
                code = code.replace(
                    'Parameter("Local Bridge Port", DefaultValue = 8888,',
                    f'Parameter("Local Bridge Port", DefaultValue = {int(port)},'
                )
            return JSONResponse({"ok": True, "code": code, "port": port, "path": cbot_path})
    return JSONResponse({"ok": False, "error": "SiriusBridge.cs not found"}, status_code=404)


@app.post("/api/mt4/test-connection")
async def test_mt4_connection(request: Request):
    """Tests connection to the local SiriusBridge.mq4 EA in MetaTrader 4."""
    try:
        from mt4_client import MT4Client
        payload = {}
        try:
            payload = await request.json()
        except Exception:
            pass
        host = str(payload.get("mt4_host") or "127.0.0.1").strip()
        port = int(payload.get("mt4_port") or 9001)

        def _probe():
            client = MT4Client(host=host, port=port)
            ok = client.connect(timeout=2.5)
            if not ok:
                return {"connected": False, "error": client.last_error()}
            info = client.get_account_info()
            sym_count = len(client.symbols_by_name)
            client.disconnect()
            return {
                "connected": True,
                "login": getattr(info, "login", client.trader_login),
                "balance": getattr(info, "balance", 0.0),
                "equity": getattr(info, "equity", 0.0),
                "server": getattr(info, "server", f"MT4 ({host}:{port})"),
                "symbols_loaded": sym_count,
            }

        res = await asyncio.to_thread(_probe)
        return JSONResponse(res)
    except Exception as e:
        return JSONResponse({"connected": False, "error": str(e)}, status_code=500)


@app.get("/api/mt4/ports")
async def get_mt4_port_allocation():
    """Scans ports 9001-9020 to discover active SiriusBridge.mq4 EAs and allocate the next free port."""
    def _probe():
        import socket
        import json
        from account_manager import AccountManager
        acc_mgr = AccountManager.get_instance()
        acc_mgr.load_accounts()

        used_ports = set()
        for acc in acc_mgr.accounts_config.values():
            if str(acc.get("platform", "")).lower() == "mt4":
                try:
                    p = int(acc.get("mt4_port") or 9001)
                    used_ports.add(p)
                except Exception:
                    pass

        active_bridges = []
        free_ports = []

        for port in range(9001, 9020):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.1)
            try:
                s.connect(('127.0.0.1', port))
                s.settimeout(0.25)
                s.sendall(b'{"cmd":"PING"}\n')
                line = s.makefile('r', encoding='utf-8').readline()
                if line:
                    try:
                        data = json.loads(line.strip())
                        if data.get('ok'):
                            active_bridges.append({
                                'port': port,
                                'login': data.get('login'),
                                'broker': data.get('broker', 'MetaTrader 4'),
                                'balance': data.get('balance', 0.0),
                                'already_registered': port in used_ports
                            })
                    except Exception:
                        pass
                s.close()
            except Exception:
                free_ports.append(port)
                try:
                    s.close()
                except Exception:
                    pass

        unregistered = [b for b in active_bridges if not b['already_registered']]
        if unregistered:
            suggested = unregistered[0]['port']
        else:
            suggested = 9001
            for p in range(9001, 9050):
                if p not in used_ports:
                    suggested = p
                    break

        return {
            "ok": True,
            "suggested_port": suggested,
            "used_ports": sorted(list(used_ports)),
            "active_bridges": active_bridges,
            "free_ports": free_ports[:10]
        }

    res = await asyncio.to_thread(_probe)
    return JSONResponse(res)


@app.get("/api/mt4/ea-code")
async def get_mt4_ea_code(port: int = 9001):
    """Returns the SiriusBridge.mq4 source code configured for the specified port."""
    ea_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mt4", "SiriusBridge.mq4")
    if os.path.exists(ea_path):
        with open(ea_path, "r", encoding="utf-8") as f:
            code = f.read()
            if port and int(port) != 9001:
                code = code.replace(
                    'input int    InpPort       = 9001;',
                    f'input int    InpPort       = {int(port)};'
                )
            return JSONResponse({"ok": True, "code": code, "port": port, "path": ea_path})
    return JSONResponse({"ok": False, "error": "SiriusBridge.mq4 not found"}, status_code=404)


@app.post("/api/mt4/install-ea")
async def install_mt4_ea_all_terminals():
    """
    Installs SiriusBridge.mq4 (and compiled SiriusBridge.ex4) to all detected
    MetaTrader 4 terminals across both user AppData and Program Files/portable directories.
    Also compiles with MetaEditor if available so the EA is immediately ready in MT4 without restart.
    """
    def _do_install():
        import shutil
        import subprocess

        base_dir = os.path.dirname(os.path.abspath(__file__))
        src_mq4 = os.path.join(base_dir, "mt4", "SiriusBridge.mq4")
        src_ex4 = os.path.join(base_dir, "mt4", "SiriusBridge.ex4")

        if not os.path.exists(src_mq4):
            return {"ok": False, "error": "mt4/SiriusBridge.mq4 source file not found"}

        installed_targets = []
        seen_dirs = set()

        # 1. User AppData MetaQuotes/Terminal folders
        appdata = os.environ.get("APPDATA", "")
        terminal_root = os.path.join(appdata, "MetaQuotes", "Terminal")
        if os.path.exists(terminal_root):
            for item in os.listdir(terminal_root):
                p = os.path.join(terminal_root, item)
                if not os.path.isdir(p):
                    continue
                mql4_exp = os.path.join(p, "MQL4", "Experts")
                if not os.path.exists(mql4_exp):
                    continue

                orig_file = os.path.join(p, "origin.txt")
                origin = ""
                if os.path.exists(orig_file):
                    try:
                        with open(orig_file, "rb") as f:
                            origin = f.read().decode("utf-16le", errors="ignore").strip().replace("\ufeff", "")
                    except Exception:
                        pass
                norm = os.path.normpath(mql4_exp).lower()
                if norm not in seen_dirs:
                    seen_dirs.add(norm)
                    name = os.path.basename(origin) if origin else item
                    installed_targets.append({
                        "name": name or f"MT4 Terminal ({item[:8]})",
                        "origin": origin,
                        "experts_dir": mql4_exp,
                        "data_dir": p,
                        "type": "appdata"
                    })

        # 2. Program Files and portable locations
        for pf in [os.environ.get("ProgramFiles", ""), os.environ.get("ProgramFiles(x86)", ""), "C:\\Trading"]:
            if not pf or not os.path.exists(pf):
                continue
            try:
                for d in os.listdir(pf):
                    full_d = os.path.join(pf, d)
                    if not os.path.isdir(full_d):
                        continue
                    if os.path.exists(os.path.join(full_d, "terminal.exe")):
                        exp = os.path.join(full_d, "MQL4", "Experts")
                        if os.path.exists(exp):
                            norm = os.path.normpath(exp).lower()
                            if norm not in seen_dirs:
                                seen_dirs.add(norm)
                                installed_targets.append({
                                    "name": d,
                                    "origin": full_d,
                                    "experts_dir": exp,
                                    "data_dir": full_d,
                                    "type": "portable"
                                })
            except Exception:
                pass

        if not installed_targets:
            return {"ok": False, "error": "No MetaTrader 4 data directories found on this computer"}

        results = []
        for t in installed_targets:
            exp_dir = t["experts_dir"]
            os.makedirs(exp_dir, exist_ok=True)
            dest_mq4 = os.path.join(exp_dir, "SiriusBridge.mq4")
            dest_ex4 = os.path.join(exp_dir, "SiriusBridge.ex4")

            # Copy MQ4 source
            shutil.copy2(src_mq4, dest_mq4)
            has_ex4 = False

            # If pre-compiled EX4 is available in repo, copy it directly
            if os.path.exists(src_ex4):
                shutil.copy2(src_ex4, dest_ex4)
                has_ex4 = True

            # Also attempt compiling with the terminal's MetaEditor if present
            origin = t.get("origin")
            if origin and os.path.exists(os.path.join(origin, "metaeditor.exe")):
                me = os.path.join(origin, "metaeditor.exe")
                log_f = os.path.join(exp_dir, "SiriusBridge_install.log")
                try:
                    subprocess.run([me, f"/compile:{dest_mq4}", f"/log:{log_f}"], capture_output=True, timeout=10)
                    if os.path.exists(dest_ex4):
                        has_ex4 = True
                    if os.path.exists(log_f):
                        try:
                            os.remove(log_f)
                        except Exception:
                            pass
                except Exception:
                    pass

            results.append({
                "name": t["name"],
                "path": exp_dir,
                "mq4_installed": os.path.exists(dest_mq4),
                "ex4_compiled": has_ex4
            })

        return {
            "ok": True,
            "installed_count": len(results),
            "terminals": results
        }

    res = await asyncio.to_thread(_do_install)
    return JSONResponse(res)


# ==================== SIGNAL WATCH API ====================

@app.get("/api/signal-watch")
async def api_get_signal_watch(provider_id: Optional[str] = None, status: Optional[str] = None, limit: int = 100):
    """Lists Signal Watch monitored events with optional provider & status filtering."""
    try:
        import db
        all_events = db.get_signal_watch_events(limit=max(200, limit))
        filtered = db.get_signal_watch_events(limit=limit, provider_id=provider_id, status=status)

        # Build provider filter options from configured providers + DB events
        prov_map = {}
        try:
            cfg = load_settings()
            for pid, pdata in cfg.items():
                prov_map[str(pid)] = pdata.get("name") or f"Provider {pid}"
        except Exception:
            pass
        for ev in all_events:
            pid = str(ev.get("provider_id") or "")
            if pid and pid not in prov_map:
                prov_map[pid] = ev.get("provider_name") or f"Provider {pid}"

        providers_list = [
            {"provider_id": pid, "name": pname}
            for pid, pname in sorted(prov_map.items(), key=lambda x: (int(x[0]) if x[0].isdigit() else 999999, x[1]))
        ]

        summary = {
            "total": len(all_events),
            "copied": sum(1 for e in all_events if e.get("overall_status") == "COPIED"),
            "issues": sum(1 for e in all_events if e.get("has_discrepancy")),
            "investigating": sum(1 for e in all_events if e.get("report_status") == "INVESTIGATING"),
            "reports_ready": sum(1 for e in all_events if e.get("report_status") == "READY"),
        }
        return JSONResponse({
            "status": "success",
            "events": filtered,
            "providers": providers_list,
            "summary": summary,
        })
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


@app.get("/api/signal-watch/{watch_id}")
async def api_get_signal_watch_detail(watch_id: int):
    try:
        import db
        ev = db.get_signal_watch_event(watch_id)
        if not ev:
            return JSONResponse({"status": "error", "error": "Watch event not found"}, status_code=404)
        return JSONResponse({"status": "success", "event": ev})
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


@app.post("/api/signal-watch/{watch_id}/reinvestigate")
async def api_reinvestigate_signal_watch(watch_id: int):
    try:
        import db
        import signal_watch
        import threading
        ev = db.get_signal_watch_event(watch_id)
        if not ev:
            return JSONResponse({"status": "error", "error": "Watch event not found"}, status_code=404)
        t = threading.Thread(target=signal_watch.reinvestigate_watch_event, args=(watch_id,), daemon=True)
        t.start()
        return JSONResponse({"status": "success", "message": "Re-investigation started"})
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


@app.delete("/api/signal-watch/{watch_id}")
async def api_delete_signal_watch(watch_id: int):
    try:
        import db
        ok = db.delete_signal_watch_event(watch_id)
        return JSONResponse({"status": "success", "deleted": ok})
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


@app.post("/api/signal-watch/clear")
async def api_clear_signal_watch():
    try:
        import db
        db.clear_signal_watch_events()
        return JSONResponse({"status": "success"})
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


if getattr(config, "BACKTESTER_ENABLED", True):
    from backtester.router import router as backtester_router
    app.include_router(backtester_router)

def run_web_server():
    print("[INFO] Starting Web Dashboard on http://127.0.0.1:7770")
    for attempt in range(20):
        try:
            uvicorn.run(app, host="127.0.0.1", port=7770, log_level="error")
            break
        except Exception as e:
            print(f"[WARN] Web server bind retry {attempt+1}/20: {e}")
            time.sleep(2)

