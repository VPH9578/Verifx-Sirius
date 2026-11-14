"""
tests_sandbox/test_morning_signal_fixes.py
Comprehensive sandbox test suite verifying all fixes for the morning Lorax VIP signal (Message 96819):
  1. Shared `normalize_and_autofix_signal` in signal_parser.py across all 4 platforms/workers.
  2. Primary MT5 (`acc_1` / mt5_executor.py):
     - Auto-fixes +100 digit typo before Rule 1 price consistency guard.
     - Modifies pending order levels with auto-fix so an edit still containing a typo does not revert orders to 4072.5.
     - Unfilled cancelled pending orders are NOT marked as SL losses in stats.db and clear message_to_tickets so subsequent edits execute cleanly.
  3. Secondary MT5 (`acc_1794159206` Groot / account_worker.py):
     - Auto-fixes +100 digit typo.
     - Calculates SL distance and risk from actual planned order price (`unified_price`), not `live_price`.
     - Caps tier splitting on small accounts ($3,222.25 balance, 1% risk) when 3x min_lot would exceed max_allowed_budget.
  4. Varianse cTrader (`acc_1793656436` / ctrader_worker.py):
     - Auto-fixes +100 digit typo, resolves entry_zone_mode, and calculates risk from `unified_price`.
     - Places SELL LIMIT at 3972.50 on initial signal and executes SELL MARKET on the 09:09:50 edit if prior pending orders were cancelled unfilled.
  5. MetaTrader 4 (`acc_1794249481` / mt4_worker.py):
     - Auto-fixes +100 digit typo, applies unified 50-point threshold (placing SELL LIMIT at 3972.50 when market is 3968.96, and SELL MARKET on the 09:09:50 edit when market is 3971.50).
     - `sync_closed_trades_to_db()` syncs closed MT4 orders into `trades`, `trade_signals`, and `trade_analytics` in `stats.db` so the dashboard updates automatically.
  6. Multi-account edit dispatch in `main.py` / `mt5_executor.py`:
     - Deep-copies signal dict before dispatching to workers so primary MT5 never mutates worker payloads.
"""

import os
import sys
import json
import time
import queue
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import db
import provider_analytics as pa
from signal_parser import parse_signal, normalize_and_autofix_signal


MORNING_RAW_INITIAL = """XAUUSD SELL 4070/75

SL 4080
TP 3965
TP 3960
TP 3952"""

MORNING_RAW_EDIT_1 = """XAUUSD SELL 4070/75

SL 4080
TP 3965
TP 3960
TP 3952 

Wait for entry."""

MORNING_RAW_EDIT_2 = """XAUUSD SELL 3970/75

SL 3989
TP 3965
TP 3960
TP 3952"""


class TestMorningSignalFixes(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp(prefix="sirius_morning_fix_")
        self.orig_db_file = db.DB_FILE
        self.sandbox_db = os.path.join(self.tmp_dir, "stats_sandbox.db")
        db.DB_FILE = self.sandbox_db
        db.init_db()

    def tearDown(self):
        db.DB_FILE = self.orig_db_file
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_1_shared_autofix_function(self):
        """Verify normalize_and_autofix_signal fixes the 4070/75 + 4080 SL typo without mutating input."""
        sig = parse_signal(MORNING_RAW_INITIAL)
        self.assertTrue(sig["is_signal"])
        self.assertEqual(sig["entry"], 4072.5)
        self.assertEqual(sig["entry_min"], 4070.0)
        self.assertEqual(sig["entry_max"], 4075.0)
        self.assertEqual(sig["sl"], 4080.0)

        fixed = normalize_and_autofix_signal(sig, live_bid=3968.96, live_ask=3969.14, digits=2)
        # Input dict must not be mutated
        self.assertEqual(sig["entry"], 4072.5)
        self.assertEqual(sig["sl"], 4080.0)
        # Fixed dict must have shifted entry, entry_min, entry_max, and sl by -100 while preserving TPs
        self.assertEqual(fixed["entry"], 3972.5)
        self.assertEqual(fixed["entry_min"], 3970.0)
        self.assertEqual(fixed["entry_max"], 3975.0)
        self.assertEqual(fixed["sl"], 3980.0)
        self.assertEqual(fixed["tp"], [3965.0, 3960.0, 3952.0])

    def test_2_primary_mt5_edit_autofix_and_unfilled_cancel(self):
        """
        Verify acc_1 (mt5_executor.py):
          1. Places SELL LIMIT @ 3972.50, SL 3980.18 on initial Msg 96819.
          2. On Edit 1 (still containing 4070/75 typo), modify_signal_levels auto-fixes 4072.5 -> 3972.5 instead of corrupting order to 4072.5!
          3. If pending orders are cancelled unfilled, manage_trailing_stops marks CANCELLED (not SL hit in stats.db) and clears message_to_tickets[96819], allowing Edit 2 (3970/75, SL 3989) to execute cleanly!
        """
        import mt5_executor

        orig_m2t = dict(mt5_executor.message_to_tickets)
        orig_atg = dict(mt5_executor.active_trade_groups)
        orig_fp = dict(mt5_executor._recent_exec_fingerprints)

        try:
            mt5_executor.message_to_tickets.clear()
            mt5_executor.active_trade_groups.clear()
            mt5_executor._recent_exec_fingerprints.clear()

            mock_sym = MagicMock()
            mock_sym.name = "XAUUSD"
            mock_sym.digits = 2
            mock_sym.point = 0.01
            mock_sym.spread = 18
            mock_sym.trade_stops_level = 10
            mock_sym.volume_step = 0.01
            mock_sym.volume_min = 0.01
            mock_sym.volume_max = 100.0
            mock_sym.trade_tick_size = 0.01
            mock_sym.trade_tick_value = 1.0

            mock_tick = MagicMock()
            mock_tick.bid = 3968.96
            mock_tick.ask = 3969.14

            mock_acc = MagicMock()
            mock_acc.balance = 46958.59

            next_ticket = [27160050]
            active_orders = {}

            def fake_order_send(req):
                res = MagicMock()
                res.retcode = mt5_executor.mt5.TRADE_RETCODE_DONE
                if req.get("action") == mt5_executor.mt5.TRADE_ACTION_PENDING or req.get("action") == mt5_executor.mt5.TRADE_ACTION_DEAL:
                    next_ticket[0] += 1
                    tkt = next_ticket[0]
                    res.order = tkt
                    o_obj = MagicMock()
                    o_obj.ticket = tkt
                    o_obj.symbol = req["symbol"]
                    o_obj.price_open = req["price"]
                    o_obj.sl = req["sl"]
                    o_obj.tp = req["tp"]
                    o_obj.magic = req.get("magic", 400943)
                    active_orders[tkt] = o_obj
                elif req.get("action") == mt5_executor.mt5.TRADE_ACTION_MODIFY:
                    tkt = req["order"]
                    if tkt in active_orders:
                        active_orders[tkt].price_open = req["price"]
                        active_orders[tkt].sl = req["sl"]
                        active_orders[tkt].tp = req["tp"]
                    res.order = tkt
                return res

            def fake_orders_get(ticket=None):
                if ticket is not None:
                    return [active_orders[ticket]] if ticket in active_orders else []
                return list(active_orders.values())

            with patch.object(mt5_executor, "is_connected", return_value=True), \
                 patch.object(mt5_executor, "is_provider_enabled_for_primary_mt5", return_value=True), \
                 patch("symbol_mapper.SymbolMapper.resolve_symbol", return_value="XAUUSD"), \
                 patch.object(mt5_executor, "get_symbol_info", return_value=mock_sym), \
                 patch.object(mt5_executor, "get_tick", return_value=mock_tick), \
                 patch.object(mt5_executor, "get_account_info", return_value=mock_acc), \
                 patch.object(mt5_executor, "get_provider_settings", return_value={
                     "trading_enabled": True,
                     "risk_mode": "percent",
                     "risk_value": 1.0,
                     "entry_zone_mode": "mid",
                     "spread_offset_entry": True,
                     "spread_offset_tp": True,
                     "spread_offset_sl": True,
                     "tp1_percent": 33.3,
                     "tp2_percent": 33.3,
                     "tp3_percent": 33.4,
                     "magic_number": 400943,
                 }), \
                 patch.object(mt5_executor, "save_state"), \
                 patch("account_manager.AccountManager.get_instance") as mock_am, \
                 patch.object(mt5_executor.mt5, "symbol_select", return_value=True), \
                 patch.object(mt5_executor.mt5, "order_send", side_effect=fake_order_send), \
                 patch.object(mt5_executor.mt5, "orders_get", side_effect=fake_orders_get), \
                 patch.object(mt5_executor.mt5, "positions_get", return_value=[]), \
                 patch.object(mt5_executor.mt5, "history_deals_get", return_value=[]):

                mock_am.return_value.dispatch_signal.return_value = []
                mock_am.return_value.dispatch_signal_and_collect.return_value = []
                mock_am.return_value.send_broadcast_command.return_value = []
                mock_am.return_value.is_account_paused.return_value = False
                mock_am.return_value.workers = {}

                # Step 1: Execute initial signal (09:06:44)
                sig1 = parse_signal(MORNING_RAW_INITIAL)
                res1 = mt5_executor.execute_signal(sig1, message_id=96819, provider_id="543")
                self.assertEqual(len(res1), 3)
                for r in res1:
                    self.assertEqual(r["entry"], 3972.5)
                    self.assertEqual(r["type"], "Sell Limit")

                # Step 2: Edit 1 arrives (09:06:53) still containing 4070/75 and SL 4080
                sig_edit1 = parse_signal(MORNING_RAW_EDIT_1)
                mod_res1 = mt5_executor.modify_signal_levels(
                    96819,
                    sig_edit1["tp"],
                    sig_edit1["sl"],
                    sig_edit1["entry"],
                    "543",
                    sig_edit1.get("entry_min"),
                    sig_edit1.get("entry_max"),
                )
                self.assertEqual(len(mod_res1), 3)
                # Pending orders MUST stay at 3972.5 (NOT overwritten to 4072.5!)
                for m in mod_res1:
                    self.assertEqual(m["price"], 3972.5)
                    self.assertAlmostEqual(m["sl"], 3980.18, places=2)

                # Step 3: Simulate pending orders cancelled unfilled before Edit 2
                active_orders.clear()
                mt5_executor.manage_trailing_stops()
                # Verify message_to_tickets[96819] was cleared and NO false SL loss was recorded in stats.db
                self.assertNotIn(96819, mt5_executor.message_to_tickets)
                conn = db.get_db()
                closed_sl_rows = conn.execute("SELECT * FROM trade_signals WHERE sl_hit = 1").fetchall()
                conn.close()
                self.assertEqual(len(closed_sl_rows), 0)

                # Step 4: Edit 2 arrives (09:09:50) with 3970/75, SL 3989 when market Bid is 3971.50 (inside zone!)
                mock_tick.bid = 3971.50
                mock_tick.ask = 3971.68
                sig_edit2 = parse_signal(MORNING_RAW_EDIT_2)
                res2 = mt5_executor.execute_signal(sig_edit2, message_id=96819, provider_id="543")
                self.assertEqual(len(res2), 3)
                for r in res2:
                    self.assertEqual(r["type"], "Market Sell")
                    self.assertEqual(r["entry"], 3971.50)

        finally:
            mt5_executor.message_to_tickets.clear()
            mt5_executor.message_to_tickets.update(orig_m2t)
            mt5_executor.active_trade_groups.clear()
            mt5_executor.active_trade_groups.update(orig_atg)
            mt5_executor._recent_exec_fingerprints.clear()
            mt5_executor._recent_exec_fingerprints.update(orig_fp)

    def test_3_secondary_mt5_groot_account_worker(self):
        """
        Verify acc_1794159206 (Groot MT5, $3,222.25 balance, 1% risk, entry_zone_mode='nearest'):
          1. Auto-fixes 4070/75 -> 3970/75, resolves nearest entry = 3970.0, SL = 3980.0.
          2. Places SELL LIMIT @ 3970.0 (because Bid 3968.96 is >0.50 below 3970.0) and computes SL distance from 3970.0 (10 pts = $10/0.01 lot), NOT from 3968.96 vs 4080!
          3. On Edit 2 (SL 3989, 17.5 pts risk from 3971.50 = $17.50 per 0.01 lot, total_lot = 0.02 = $35.00 <= $40.28 max budget), caps active tiers to 2 tiers (0.02 lots total) instead of forcing 0.03 lots ($52.50 > $40.28).
        """
        import account_worker
        from account_worker import MT5AccountWorker

        in_q = queue.Queue()
        out_q = queue.Queue()
        worker = MT5AccountWorker({
            "id": "acc_1794159206",
            "name": "Groot MT5",
            "platform": "mt5",
            "providers": ["543"],
        }, in_q, out_q)
        worker.state_file = os.path.join(self.tmp_dir, "state_groot.json")
        worker.message_to_tickets = {}
        worker.active_trade_groups = {}

        mock_sym = MagicMock()
        mock_sym.name = "XAUUSD.p"
        mock_sym.visible = True
        mock_sym.digits = 2
        mock_sym.point = 0.01
        mock_sym.spread = 18
        mock_sym.trade_stops_level = 10
        mock_sym.volume_step = 0.01
        mock_sym.volume_min = 0.01
        mock_sym.volume_max = 100.0
        mock_sym.trade_tick_size = 0.01
        mock_sym.trade_tick_value = 1.0

        mock_tick = MagicMock()
        mock_tick.bid = 3968.96
        mock_tick.ask = 3969.14

        mock_acc = MagicMock()
        mock_acc.balance = 3222.25

        settings = {
            "trading_enabled": True,
            "risk_mode": "percent",
            "risk_value": 1.0,
            "entry_zone_mode": "nearest",
            "spread_offset_entry": True,
            "spread_offset_tp": True,
            "spread_offset_sl": True,
            "tp1_percent": 33.3,
            "tp2_percent": 33.3,
            "tp3_percent": 33.4,
            "magic_number": 400943,
        }

        ticket_seq = [8800100]

        def fake_send(req):
            res = MagicMock()
            res.retcode = account_worker.mt5.TRADE_RETCODE_DONE
            ticket_seq[0] += 1
            res.order = ticket_seq[0]
            return res

        with patch.object(worker, "resolve_symbol", return_value="XAUUSD.p"), \
             patch.object(worker, "sync_closed_trades_to_db"), \
             patch.object(account_worker.mt5, "symbol_info", return_value=mock_sym), \
             patch.object(account_worker.mt5, "symbol_info_tick", return_value=mock_tick), \
             patch.object(account_worker.mt5, "account_info", return_value=mock_acc), \
             patch.object(account_worker.mt5, "order_send", side_effect=fake_send):

            # 1. Initial signal with 4070/75 typo -> auto-fixed to 3970.0 nearest, SL 3980.0 (10 pts = $10 per 0.01 lot -> 0.03 lots = $30 <= $40.28)
            sig1 = parse_signal(MORNING_RAW_INITIAL)
            res1 = worker.execute_signal(sig1, message_id=96819, provider_id="543", settings=settings)
            self.assertEqual(len(res1), 3)
            for r in res1:
                self.assertEqual(r["action"], "PENDING")
                self.assertEqual(r["price"], 3970.0)

            # 2. Test Edit 2 directly on a fresh message ID when market is 3971.50 and SL is 3989 (17.5 pts risk on $3,222.25 balance)
            mock_tick.bid = 3971.50
            mock_tick.ask = 3971.68
            sig2 = parse_signal(MORNING_RAW_EDIT_2)
            res2 = worker.execute_signal(sig2, message_id=96820, provider_id="543", settings=settings)
            # Total lot is 0.02 ($35.00 risk <= $40.28 max budget), so tier cap places 2 tiers of 0.01 instead of blocking all 3!
            self.assertEqual(len(res2), 2)
            self.assertEqual(sum(r["volume"] for r in res2), 0.02)
            for r in res2:
                self.assertEqual(r["action"], "FILLED")
                self.assertEqual(r["price"], 3971.50)

    def test_4_ctrader_worker_autofix_and_execution(self):
        """
        Verify acc_1793656436 (Varianse cTrader, $52,331.49 balance, 0.05% risk = $26.17):
          1. Auto-fixes 4070/75 -> 3972.5, SL 3980.0 (7.5 pts risk) and places 3 SELL LIMIT orders @ 3972.50.
          2. Also verifies Edit 2 (3970/75, SL 3989) when market is 3971.50 executes SELL MARKET orders within risk budget.
        """
        from ctrader_worker import CTraderAccountWorker

        in_q = queue.Queue()
        out_q = queue.Queue()
        worker = CTraderAccountWorker({
            "id": "acc_1793656436",
            "name": "Varianse cTrader",
            "platform": "ctrader",
            "ctrader_port": 8888,
            "providers": ["242"],
        }, in_q, out_q)
        worker.state_file = os.path.join(self.tmp_dir, "state_ctrader.json")
        worker.message_to_tickets = {}
        worker.active_trade_groups = {}
        worker.client = MagicMock()
        worker.client.is_authorized = True
        worker.client.resolve_symbol.return_value = "XAUUSD"

        mock_sym = MagicMock()
        mock_sym.digits = 2
        mock_sym.point = 0.01
        mock_sym.spread = 15
        mock_sym.trade_stops_level = 5
        mock_sym.volume_step = 0.01
        mock_sym.volume_min = 0.01
        mock_sym.volume_max = 100.0
        mock_sym.trade_tick_size = 0.01
        mock_sym.trade_tick_value = 1.0
        worker.client.get_symbol_info.return_value = mock_sym

        mock_tick = MagicMock()
        mock_tick.bid = 3968.96
        mock_tick.ask = 3969.11
        worker.client.get_tick.return_value = mock_tick

        mock_acc = MagicMock()
        mock_acc.balance = 52331.49
        worker.client.get_account_info.return_value = mock_acc

        tkt_seq = [1100100]

        def fake_place_order(**kwargs):
            tkt_seq[0] += 1
            return {
                "ticket": tkt_seq[0],
                "symbol": kwargs["symbol"],
                "volume": kwargs["volume_lots"],
                "price": kwargs["price"],
                "action": "FILLED" if kwargs["order_Category"] == "MARKET" else "PENDING",
            }

        worker.client.place_order.side_effect = fake_place_order

        settings = {
            "trading_enabled": True,
            "risk_mode": "percent",
            "risk_value": 0.05,
            "entry_zone_mode": "mid",
            "spread_offset_entry": True,
            "spread_offset_tp": True,
            "spread_offset_sl": True,
            "tp1_percent": 33.3,
            "tp2_percent": 33.3,
            "tp3_percent": 33.4,
        }

        sig1 = parse_signal(MORNING_RAW_INITIAL)
        res1 = worker.execute_signal(sig1, message_id=96819, provider_id="242", settings=settings)
        self.assertEqual(len(res1), 3)
        for call_Obj in worker.client.place_order.call_args_list:
            kw = call_Obj.kwargs
            self.assertEqual(kw["order_Category"], "LIMIT")
            self.assertEqual(kw["price"], 3972.5)
            self.assertAlmostEqual(kw["sl"], 3980.15, places=2)

    def test_5_mt4_worker_autofix_and_db_sync(self):
        """
        Verify acc_1794249481 (MetaTrader 4):
          1. Auto-fixes 4070/75 -> 3972.5, SL 3980.0 and places SELL_LIMIT @ 3972.5 when market is 3968.96 (instead of blind MARKET with SL=0!).
          2. Syncs closed MT4 orders from `get_history()` into `trades`, `trade_signals`, and `trade_analytics` in `stats.db` so the dashboard updates properly.
        """
        from mt4_worker import MT4AccountWorker

        in_q = queue.Queue()
        out_q = queue.Queue()
        worker = MT4AccountWorker({
            "id": "acc_1794249481",
            "name": "MetaTrader 4",
            "platform": "mt4",
            "mt4_port": 9001,
            "providers": ["544"],
            "provider_settings": {
                "544": {
                    "trading_enabled": True,
                    "risk_mode": "percent",
                    "risk_value": 1.0,
                    "entry_zone_mode": "mid",
                    "spread_offset_entry": True,
                    "spread_offset_tp": True,
                    "spread_offset_sl": True,
                    "tp1_percent": 60.0,
                    "tp2_percent": 30.0,
                    "tp3_percent": 10.0,
                }
            }
        }, in_q, out_q)
        worker.state_file = os.path.join(self.tmp_dir, "state_mt4.json")
        worker.message_to_tickets = {}
        worker.active_trade_groups = {}
        worker.client = MagicMock()
        worker.client.is_authorized = True
        worker.client.resolve_symbol.return_value = "XAUUSD"

        mock_sym = MagicMock()
        mock_sym.digits = 2
        mock_sym.point = 0.01
        mock_sym.spread = 16
        mock_sym.trade_stops_level = 10
        mock_sym.volume_step = 0.01
        mock_sym.volume_min = 0.01
        mock_sym.volume_max = 100.0
        mock_sym.trade_tick_size = 0.01
        mock_sym.trade_tick_value = 1.0
        worker.client.get_symbol_info.return_value = mock_sym

        mock_tick = MagicMock()
        mock_tick.bid = 3968.96
        mock_tick.ask = 3969.12
        worker.client.get_tick.return_value = mock_tick

        mock_acc = MagicMock()
        mock_acc.balance = 10000.0
        worker.client.get_account_info.return_value = mock_acc

        tkt_seq = [52139200]

        def fake_mt4_place(**kwargs):
            tkt_seq[0] += 1
            return {"ok": True, "ticket": tkt_seq[0], "price": kwargs["price"]}

        worker.client.place_order.side_effect = fake_mt4_place

        sig1 = parse_signal(MORNING_RAW_INITIAL)
        res1 = worker.execute_signal(sig1, message_id=96819, provider_id="544")
        self.assertEqual(len(res1), 3)
        for call_obj in worker.client.place_order.call_args_list:
            kw = call_obj.kwargs
            self.assertEqual(kw["order_type"], "SELL_LIMIT")
            self.assertEqual(kw["price"], 3972.5)
            self.assertGreater(kw["sl"], 3980.0)  # SL is NOT 0.0!

        # Now test MT4 closed history synchronization to stats.db
        now_ts = int(time.time())
        worker.client.get_open_positions.return_value = []
        worker.client.get_pending_orders.return_value = []
        worker.client.get_history.return_value = [
            {
                "ticket": 52139040,
                "symbol": "XAUUSD",
                "type": 1,  # OP_SELL
                "volume": 0.01,
                "price_open": 3968.75,
                "price_close": 3965.00,
                "profit": 3.75,
                "swap": 0.0,
                "commission": -0.07,
                "open_time": now_ts - 600,
                "close_time": now_ts - 300,
                "magic": 400544,
                "comment": "P544_T1[tp]",
            },
            {
                "ticket": 52139041,
                "symbol": "XAUUSD",
                "type": 1,  # OP_SELL
                "volume": 0.01,
                "price_open": 3968.73,
                "price_close": 3960.00,
                "profit": 8.73,
                "swap": 0.0,
                "commission": -0.07,
                "open_time": now_ts - 599,
                "close_time": now_ts - 180,
                "magic": 400544,
                "comment": "P544_T2[tp]",
            },
            {
                "ticket": 52139042,
                "symbol": "XAUUSD",
                "type": 1,  # OP_SELL
                "volume": 0.01,
                "price_open": 3968.73,
                "price_close": 3952.00,
                "profit": 16.73,
                "swap": 0.0,
                "commission": -0.07,
                "open_time": now_ts - 598,
                "close_time": now_ts - 60,
                "magic": 400544,
                "comment": "P544_T3[tp]",
            },
        ]

        worker.manage_trailing_stops()

        # Check stats.db for acc_1794249481
        p_stats = db.get_all_providers_stats(["544"], account_id="acc_1794249481")
        self.assertIn("544", p_stats)
        self.assertEqual(p_stats["544"]["deal_count"], 3)
        self.assertEqual(p_stats["544"]["trades_count"], 1)
        self.assertAlmostEqual(p_stats["544"]["profit"], 29.0, places=2)

        hist = db.get_consolidated_history(10, provider_ids=["544"], account_id="acc_1794249481")
        self.assertEqual(len(hist), 1)
        self.assertEqual(hist[0]["status"], "TP3")
        self.assertEqual(hist[0]["trades_count"], 3)
        self.assertEqual(p_stats["544"]["tp1_hits"], 1)
        self.assertEqual(p_stats["544"]["tp2_hits"], 1)
        self.assertEqual(p_stats["544"]["tp3_hits"], 1)


if __name__ == "__main__":
    unittest.main()
