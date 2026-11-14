import os
import sys
import json
import time
import queue
import unittest
from unittest.mock import MagicMock, patch

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


class TestMultiAccountDispatch(unittest.TestCase):
    def setUp(self):
        pass

    def test_mt4_worker_deduplication(self):
        """Verify that MT4AccountWorker deduplicates repeated Telegram messages independently."""
        from mt4_worker import MT4AccountWorker
        in_q = queue.Queue()
        out_q = queue.Queue()
        worker = MT4AccountWorker({
            "id": "acc_mt4_test",
            "name": "MT4 Test Account",
            "platform": "mt4",
            "port": 9999
        }, in_q, out_q)
        worker.client = MagicMock()
        worker.client.is_authorized = True
        worker.client.resolve_symbol.return_value = "EURUSD"

        acc_info = MagicMock()
        acc_info.balance = 10000.0
        acc_info.equity = 10000.0
        worker.client.get_account_info.return_value = acc_info

        sym_info = MagicMock()
        sym_info.digits = 5
        sym_info.ask = 1.0850
        sym_info.bid = 1.0848
        sym_info.stops_level = 10
        sym_info.point = 0.0001
        sym_info.trade_tick_value = 1.0
        sym_info.trade_tick_size = 0.0001
        sym_info.volume_min = 0.01
        sym_info.volume_step = 0.01
        sym_info.volume_max = 100.0
        worker.client.get_symbol_info.return_value = sym_info

        tick = MagicMock()
        tick.ask = 1.0850
        tick.bid = 1.0848
        worker.client.get_tick.return_value = tick

        worker.client.place_order.return_value = {"ok": True, "ticket": 90001, "price": 1.0850, "error": ""}
        worker.save_state = MagicMock()

        signal = {
            "direction": "BUY",
            "order_type": "MARKET",
            "instrument": "EURUSD",
            "entry": 1.0850,
            "sl": 1.0800,
            "tp": [1.0900],
        }

        # First execution with message_id 1001 -> should place order
        res1 = worker.execute_signal(signal, message_id=1001, provider_id="544")
        self.assertEqual(len(res1), 1)
        self.assertEqual(res1[0]["ticket"], 90001)
        self.assertIn(1001, worker.message_to_tickets)
        self.assertEqual(worker.message_to_tickets[1001], [90001])

        # Second execution with same message_id 1001 -> must be blocked by SANITY-GUARD
        res2 = worker.execute_signal(signal, message_id=1001, provider_id="544")
        self.assertEqual(res2, [])
        self.assertEqual(worker.client.place_order.call_count, 1)

    def test_ctrader_worker_deduplication(self):
        """Verify that CTraderAccountWorker deduplicates repeated Telegram messages independently."""
        from ctrader_worker import CTraderAccountWorker
        in_q = queue.Queue()
        out_q = queue.Queue()
        worker = CTraderAccountWorker({
            "id": "acc_ctrader_test",
            "name": "cTrader Test Account",
            "platform": "ctrader",
            "ctrader_port": 9998
        }, in_q, out_q)
        worker.client = MagicMock()
        worker.client.is_authorized = True
        worker.client.resolve_symbol.return_value = "EURUSD"

        acc_info = MagicMock()
        acc_info.balance = 10000.0
        acc_info.equity = 10000.0
        worker.client.get_account_info.return_value = acc_info

        sym_info = MagicMock()
        sym_info.digits = 5
        sym_info.bid = 1.0848
        sym_info.ask = 1.0850
        sym_info.point = 0.0001
        sym_info.trade_tick_value = 1.0
        sym_info.trade_tick_size = 0.0001
        sym_info.volume_min = 0.01
        sym_info.volume_step = 0.01
        sym_info.volume_max = 100.0
        sym_info.trade_stops_level = 10
        worker.client.get_symbol_info.return_value = sym_info

        tick = MagicMock()
        tick.ask = 1.0850
        tick.bid = 1.0848
        worker.client.get_tick.return_value = tick

        worker.client.place_order.return_value = {"ticket": 80001, "action": "FILLED", "price": 1.0850}
        worker.save_state = MagicMock()

        signal = {
            "direction": "BUY",
            "order_type": "MARKET",
            "instrument": "EURUSD",
            "entry": 1.0850,
            "sl": 1.0800,
            "tp": [1.0900],
        }

        # First execution with message_id 2002 -> should place order
        res1 = worker.execute_signal(signal, message_id=2002, provider_id="242")
        self.assertEqual(len(res1), 1)
        self.assertEqual(res1[0]["ticket"], 80001)
        self.assertIn(2002, worker.message_to_tickets)

        # Second execution with same message_id 2002 -> must be blocked by SANITY-GUARD
        res2 = worker.execute_signal(signal, message_id=2002, provider_id="242")
        self.assertEqual(res2, [])
        self.assertEqual(worker.client.place_order.call_count, 1)

    def test_multi_account_signal_fanout(self):
        """
        Verify that when a signal message triggers multiple providers sequentially
        (e.g. cTrader Prov 242, MT5 Prov 543, MT4 Prov 544),
        secondary workers are NOT blocked by primary MT5's deduplication.
        """
        import mt5_executor

        fake_message_id = 777888
        signal = {
            "direction": "BUY",
            "order_type": "MARKET",
            "instrument": "GOLD",
            "entry": 2650.0,
            "sl": 2640.0,
            "tp": [2660.0, 2670.0],
        }

        # Mock AccountManager to simulate dispatching
        mock_acc_mgr = MagicMock()
        def fake_dispatch(provider_id, sig, msg_id):
            if str(provider_id) == "242":
                return [{"ticket": 11111, "symbol": "GOLD", "tp_number": 1, "lots": 0.01, "status": "OPEN"}]
            elif str(provider_id) == "543":
                # Secondary MT5 worker execution
                return [{"ticket": 22222, "symbol": "GOLD", "tp_number": 1, "lots": 0.05, "status": "OPEN"}]
            elif str(provider_id) == "544":
                # MT4 worker execution
                return [{"ticket": 33333, "symbol": "GOLD", "tp_number": 1, "lots": 0.02, "status": "OPEN"}]
            return []

        mock_acc_mgr.dispatch_signal_and_collect.side_effect = fake_dispatch
        mock_acc_mgr.workers = {"acc_ctrader": {}, "acc_mt5_sec": {}, "acc_mt4": {}}
        mock_acc_mgr.is_account_paused.return_value = False

        # Provider 543 is assigned to primary MT5; 242 and 544 are NOT assigned to primary MT5
        def fake_is_provider_enabled_for_primary_mt5(pid):
            return str(pid) == "543"

        with patch("account_manager.AccountManager.get_instance", return_value=mock_acc_mgr), \
             patch("mt5_executor.is_provider_enabled_for_primary_mt5", side_effect=fake_is_provider_enabled_for_primary_mt5), \
             patch("mt5_executor.is_connected", return_value=True), \
             patch("mt5_executor.get_account_info") as mock_acc_info, \
             patch("mt5_executor.get_symbol_info") as mock_sym_info, \
             patch("mt5_executor.get_tick") as mock_tick, \
             patch("mt5_executor.mt5.symbol_select", return_value=True), \
             patch("mt5_executor.mt5.order_send") as mock_order_send, \
             patch("mt5_executor.save_state"):

            # Set up MT5 mocks for Provider 543 execution
            mock_acc = MagicMock()
            mock_acc.balance = 10000.0
            mock_acc_info.return_value = mock_acc

            sym = MagicMock()
            sym.point = 0.01
            sym.digits = 2
            sym.volume_step = 0.01
            sym.trade_tick_value = 1.0
            sym.trade_tick_size = 0.01
            sym.trade_stops_level = 10
            sym.spread = 50
            mock_sym_info.return_value = sym

            tick = MagicMock()
            tick.ask = 2650.0
            tick.bid = 2649.5
            mock_tick.return_value = tick

            order_res = MagicMock()
            order_res.retcode = 10009  # TRADE_RETCODE_DONE
            order_res.order = 55555
            mock_order_send.return_value = order_res

            # Clean any prior message_id state
            mt5_executor.message_to_tickets.pop(fake_message_id, None)

            # 1. Execute Provider 242 (cTrader)
            res_242 = mt5_executor.execute_signal(signal, message_id=fake_message_id, provider_id="242")
            self.assertEqual(len(res_242), 1)
            self.assertEqual(res_242[0]["ticket"], 11111)

            # 2. Execute Provider 543 (MT5 Primary + Secondary)
            res_543 = mt5_executor.execute_signal(signal, message_id=fake_message_id, provider_id="543")
            # Should have secondary (22222) + primary MT5 orders (55555)
            tickets_543 = [r["ticket"] for r in res_543]
            self.assertIn(22222, tickets_543)
            self.assertIn(55555, tickets_543)
            # Primary MT5 tickets now recorded in message_to_tickets
            self.assertIn(fake_message_id, mt5_executor.message_to_tickets)

            # 3. Execute Provider 544 (MT4)
            # CRITICAL CHECK: In the buggy version, Provider 544 returned [] because message_to_tickets had tickets!
            # In the fixed version, Provider 544 MUST dispatch to MT4 and return MT4 orders!
            res_544 = mt5_executor.execute_signal(signal, message_id=fake_message_id, provider_id="544")
            self.assertEqual(len(res_544), 1)
            self.assertEqual(res_544[0]["ticket"], 33333)

            # Clean up
            mt5_executor.message_to_tickets.pop(fake_message_id, None)


if __name__ == "__main__":
    unittest.main()
