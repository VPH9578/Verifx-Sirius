"""
Sandbox test suite for Break-Even (BE) instructions when a trade is in profit vs. in the negative (underwater).
Tests:
  1. mt5_executor.py (set_breakeven_for_provider + manage_trailing_stops fallback)
  2. account_worker.py (MT5AccountWorker.set_be_for_provider + manage_trailing_stops + DB classification)
  3. ctrader_worker.py (CTraderAccountWorker.set_be_for_provider + manage_trailing_stops)
  4. mt4_worker.py (MT4AccountWorker.set_be_for_provider + manage_trailing_stops)
  5. backtester/engine.py (_simulate_signal_ticks & _simulate_signal_bars with underwater SET_BE)
  6. provider_analytics.py (classify_close for BE TP and Sirius BE Close)
"""

import os
import sys
import types
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _make_mock_mt5():
    m = types.ModuleType("MetaTrader5")
    m.ORDER_TYPE_BUY = 0
    m.ORDER_TYPE_SELL = 1
    m.TRADE_ACTION_DEAL = 1
    m.TRADE_ACTION_SLTP = 6
    m.TRADE_ACTION_REMOVE = 8
    m.ORDER_TIME_GTC = 0
    m.ORDER_FILLING_IOC = 1
    m.TRADE_RETCODE_DONE = 10009
    m.TRADE_RETCODE_INVALID_STOPS = 10016
    return m


def test_mt5_executor_be():
    print("=== Test 1: mt5_executor.py (Profit BE, Underwater BE, and Stops-Level Fallback) ===")
    import mt5_executor
    import account_manager

    sent_orders = []

    def fake_order_send(req):
        sent_orders.append(req)
        return SimpleNamespace(retcode=10009, comment="Done")

    sym_info = SimpleNamespace(digits=2, point=0.01, spread=20, trade_stops_level=10)

    # Case 1A: BUY position in the negative (entry=2650.00, spread=0.20 -> be_price=2650.20, current bid=2645.00)
    pos_buy_neg = SimpleNamespace(
        ticket=1001,
        symbol="XAUUSD",
        type=0,  # BUY
        price_open=2650.00,
        price_current=2645.00,
        sl=2635.00,
        tp=2670.00,
        volume=0.10,
        magic=400401,
    )
    tick_neg = SimpleNamespace(bid=2645.00, ask=2645.20)

    mt5_executor.active_trade_groups = {
        "1": {
            "symbol": "XAUUSD",
            "provider_id": "1",
            "trade_type": "BUY",
            "status": "OPEN",
            "entry": 2650.00,
            "tp1_price": 2670.00,
            "tp1_ticket": 1001,
            "tp2_ticket": None,
            "tp3_ticket": None,
        }
    }

    with patch.object(account_manager.AccountManager, "get_instance", return_value=MagicMock()), \
         patch.object(mt5_executor, "is_provider_enabled_for_primary_mt5", return_value=True), \
         patch.object(mt5_executor, "get_provider_settings", return_value={"magic_number": 400401}), \
         patch.object(mt5_executor, "get_open_positions", return_value=[pos_buy_neg]), \
         patch.object(mt5_executor, "get_symbol_info", return_value=sym_info), \
         patch.object(mt5_executor, "get_tick", return_value=tick_neg), \
         patch.object(mt5_executor, "save_state"), \
         patch.object(mt5_executor.mt5, "order_send", side_effect=fake_order_send):
        ok = mt5_executor.set_breakeven_for_provider("1")

    assert ok is True, "set_breakeven_for_provider should return True for underwater BUY"
    assert len(sent_orders) == 1
    assert sent_orders[0]["action"] == mt5_executor.mt5.TRADE_ACTION_SLTP
    assert sent_orders[0]["sl"] == 2635.00, f"Original SL should be preserved, got {sent_orders[0]['sl']}"
    assert sent_orders[0]["tp"] == 2650.20, f"TP should be moved to BE (2650.20), got {sent_orders[0]['tp']}"
    grp = mt5_executor.active_trade_groups["1"]
    assert grp.get("be_recovery") is True
    assert grp["be_targets"]["1001"] == {"price": 2650.20, "mode": "TP"}
    print("  [PASS] Case 1A: Underwater BUY moved TP to 2650.20 and kept SL at 2635.00")

    # Case 1B: BUY position in profit (entry=2650.00, spread=0.20 -> be_price=2650.20, current bid=2658.00)
    sent_orders.clear()
    pos_buy_prof = SimpleNamespace(
        ticket=1002,
        symbol="XAUUSD",
        type=0,  # BUY
        price_open=2650.00,
        price_current=2658.00,
        sl=2635.00,
        tp=2670.00,
        volume=0.10,
        magic=400401,
    )
    tick_prof = SimpleNamespace(bid=2658.00, ask=2658.20)
    with patch.object(account_manager.AccountManager, "get_instance", return_value=MagicMock()), \
         patch.object(mt5_executor, "is_provider_enabled_for_primary_mt5", return_value=True), \
         patch.object(mt5_executor, "get_provider_settings", return_value={"magic_number": 400401}), \
         patch.object(mt5_executor, "get_open_positions", return_value=[pos_buy_prof]), \
         patch.object(mt5_executor, "get_symbol_info", return_value=sym_info), \
         patch.object(mt5_executor, "get_tick", return_value=tick_prof), \
         patch.object(mt5_executor, "save_state"), \
         patch.object(mt5_executor.mt5, "order_send", side_effect=fake_order_send):
        ok = mt5_executor.set_breakeven_for_provider("1")

    assert ok is True
    assert len(sent_orders) == 1
    assert sent_orders[0]["sl"] == 2650.20, f"SL should move to BE (2650.20), got {sent_orders[0]['sl']}"
    assert sent_orders[0]["tp"] == 2670.00, f"TP should remain 2670.00, got {sent_orders[0]['tp']}"
    print("  [PASS] Case 1B: In-profit BUY moved SL to 2650.20 and kept TP at 2670.00")

    # Case 1C: Stops-level fallback in manage_trailing_stops when price recovers to BE
    sent_orders.clear()
    pos_recover = SimpleNamespace(
        ticket=1001,
        symbol="XAUUSD",
        type=0,
        price_open=2650.00,
        price_current=2650.25,
        sl=2635.00,
        tp=2670.00,  # Suppose broker rejected earlier TP mod due to stops_level
        volume=0.10,
        magic=400401,
    )
    tick_recovered = SimpleNamespace(bid=2650.25, ask=2650.45)
    with patch.object(mt5_executor, "get_open_positions", return_value=[pos_recover]), \
         patch.object(mt5_executor, "get_symbol_info", return_value=sym_info), \
         patch.object(mt5_executor, "get_tick", return_value=tick_recovered), \
         patch.object(mt5_executor, "save_state"), \
         patch.object(mt5_executor.mt5, "orders_get", return_value=[]), \
         patch.object(mt5_executor.mt5, "order_send", side_effect=fake_order_send):
        mt5_executor.manage_trailing_stops()

    assert len(sent_orders) == 1
    assert sent_orders[0]["action"] == mt5_executor.mt5.TRADE_ACTION_DEAL
    assert sent_orders[0]["comment"] == "Sirius BE Close"
    assert sent_orders[0]["position"] == 1001
    print("  [PASS] Case 1C: manage_trailing_stops closed underwater trade at market once price recovered to BE")


def test_account_worker_be():
    print("\n=== Test 2: account_worker.py (SELL Underwater BE & Profit BE) ===")
    import account_worker

    sent_orders = []

    def fake_order_send(req):
        sent_orders.append(req)
        return SimpleNamespace(retcode=10009, comment="Done")

    worker = account_worker.MT5AccountWorker.__new__(account_worker.MT5AccountWorker)
    worker.account_id = "acc_test"
    worker.deviation = 20
    worker.magic_base = 400000
    worker.active_trade_groups = {
        "10": {
            "symbol": "XAUUSD",
            "provider_id": "5",
            "direction": "SELL",
            "status": "OPEN",
            "entry": 2650.00,
            "tp1_ticket": 2001,
            "tp2_ticket": 2002,
            "tp3_ticket": None,
        }
    }
    worker.message_to_tickets = {}
    worker.save_state = MagicMock()
    worker._matches_provider = lambda pos, pids: True

    sym_info = SimpleNamespace(digits=2, point=0.01, spread=20, trade_stops_level=10)
    # SELL opened at 2650.00, spread=0.20 -> be_price = 2649.80.
    # Currently underwater: ask = 2655.00 (> 2649.80)
    pos1 = SimpleNamespace(ticket=2001, symbol="XAUUSD", type=1, price_open=2650.00, price_current=2655.00, sl=2665.00, tp=2630.00, volume=0.05, magic=400005)
    pos2 = SimpleNamespace(ticket=2002, symbol="XAUUSD", type=1, price_open=2650.00, price_current=2655.00, sl=2665.00, tp=2620.00, volume=0.05, magic=400005)
    tick_sell_neg = SimpleNamespace(bid=2654.80, ask=2655.00)

    with patch.object(account_worker.mt5, "positions_get", return_value=[pos1, pos2]), \
         patch.object(account_worker.mt5, "symbol_info", return_value=sym_info), \
         patch.object(account_worker.mt5, "symbol_info_tick", return_value=tick_sell_neg), \
         patch.object(account_worker.mt5, "order_send", side_effect=fake_order_send):
        ok = worker.set_be_for_provider("5")

    assert ok is True
    assert len(sent_orders) == 2
    for o in sent_orders:
        assert o["sl"] == 2665.00, "SELL underwater BE must preserve SL=2665.00"
        assert o["tp"] == 2649.80, f"SELL underwater BE must move TP to 2649.80, got {o['tp']}"
    assert worker.active_trade_groups["10"]["be_targets"]["2001"] == {"price": 2649.80, "mode": "TP"}
    assert worker.active_trade_groups["10"]["be_targets"]["2002"] == {"price": 2649.80, "mode": "TP"}
    print("  [PASS] Underwater SELL legs 2001 & 2002 moved TP to 2649.80 and preserved SL at 2665.00")


def test_ctrader_and_mt4_workers_be():
    print("\n=== Test 3: ctrader_worker.py & mt4_worker.py Underwater BE ===")
    import ctrader_worker
    import mt4_worker

    # cTrader worker
    ct_worker = ctrader_worker.CTraderAccountWorker.__new__(ctrader_worker.CTraderAccountWorker)
    ct_worker.account_id = "ct_test"
    ct_worker.magic_base = 400000
    ct_worker.active_trade_groups = {
        "1": {"symbol": "XAUUSD", "provider_id": "2", "tp1_ticket": 3001, "tp2_ticket": None, "tp3_ticket": None, "status": "OPEN"}
    }
    ct_worker.save_state = MagicMock()
    ct_worker.client = MagicMock()
    ct_worker.client.get_open_positions.return_value = [
        SimpleNamespace(ticket=3001, symbol="XAUUSD", type=0, price_open=2650.00, price_current=2642.00, sl=2630.00, tp=2680.00, volume=0.10)
    ]
    ct_worker.client.get_symbol_info.return_value = SimpleNamespace(digits=2, point=0.01, spread=20)
    ct_worker.client.get_tick.return_value = SimpleNamespace(bid=2642.00, ask=2642.20)
    ct_worker.client.modify_position_sltp.return_value = True

    ok_ct = ct_worker.set_be_for_provider("2")
    assert ok_ct is True
    ct_worker.client.modify_position_sltp.assert_called_once_with(3001, sl=2630.00, tp=2650.20, digits=2)
    print("  [PASS] cTraderAccountWorker moved underwater BUY TP to 2650.20 and preserved SL at 2630.00")

    # MT4 worker
    m4_worker = mt4_worker.MT4AccountWorker.__new__(mt4_worker.MT4AccountWorker)
    m4_worker.account_id = "mt4_test"
    m4_worker.magic_base = 400000
    m4_worker.active_trade_groups = {
        "1": {"symbol": "XAUUSD", "provider_id": "3", "tp1_ticket": 4001, "tp2_ticket": None, "tp3_ticket": None, "status": "OPEN"}
    }
    m4_worker.save_state = MagicMock()
    m4_worker.client = MagicMock()
    m4_worker.client.is_authorized = True
    m4_worker.client.get_open_positions.return_value = [
        SimpleNamespace(ticket=4001, symbol="XAUUSD", type=1, price_open=2650.00, price_current=2656.00, sl=2665.00, tp=2620.00, volume=0.10)
    ]
    m4_worker.client.get_symbol_info.return_value = SimpleNamespace(digits=2, point=0.01, spread=20)
    m4_worker.client.get_tick.return_value = SimpleNamespace(bid=2655.80, ask=2656.00)
    m4_worker.client.modify_position.return_value = {"ok": True}

    cnt_m4 = m4_worker.set_be_for_provider("3")
    assert cnt_m4 == 1
    m4_worker.client.modify_position.assert_called_once_with(4001, sl=2665.00, tp=2649.80)
    print("  [PASS] MT4AccountWorker moved underwater SELL TP to 2649.80 and preserved SL at 2665.00")


def test_backtester_underwater_be():
    print("\n=== Test 4: backtester/engine.py (Tick & Bar Simulation with Underwater SET_BE) ===")
    from backtester import engine

    sym_info = SimpleNamespace(
        name="XAUUSD",
        digits=2,
        point=0.01,
        trade_contract_size=100.0,
        trade_tick_size=0.01,
        trade_tick_value=1.0,
        volume_min=0.01,
        volume_max=100.0,
        volume_step=0.01,
        spread=20,
    )
    rules = {
        "initial_balance": 10000.0,
        "risk_mode": "fixed_cash",
        "risk_value": 100.0,
        "tp1_Weight": 50.0,
        "tp2_Weight": 30.0,
        "tp3_Weight": 20.0,
        "sl_tp1_strategy": "ENTRY",
        "sl_tp2_strategy": "TP1",
        "platform": "mt5",
    }

    # Signal: BUY XAUUSD @ 2650, SL 2640, TP 2660 / 2670 / 2680
    sig = {
        "msg_id": 501,
        "timestamp": 1700000000,
        "symbol": "XAUUSD",
        "direction": "BUY",
        "order_type": "MARKET",
        "entry": 2650.0,
        "sl": 2640.0,
        "tp": [2660.0, 2670.0, 2680.0],
        "updates": [
            {"timestamp": 1700000120, "action": "SET_BE", "text": "Set BE"}
        ],
    }

    # Ticks:
    # t=1700000000: opens BUY at ask=2650.0 (bid=2649.8)
    # t=1700000060: drops into negative (bid=2644.0, ask=2644.2)
    # t=1700000120: provider sends SET_BE while bid=2645.0 (< 2650.0) -> TP moves to 2650.0!
    # t=1700000180: price recovers to bid=2650.1 -> hits BE TP at 2650.0 and closes!
    # t=1700000240: price drops to 2635.0 (would have hit SL if still open!)
    ticks = [
        {"time": 1700000000, "bid": 2649.8, "ask": 2650.0},
        {"time": 1700000060, "bid": 2644.0, "ask": 2644.2},
        {"time": 1700000120, "bid": 2645.0, "ask": 2645.2},
        {"time": 1700000180, "bid": 2650.1, "ask": 2650.3},
        {"time": 1700000240, "bid": 2635.0, "ask": 2635.2},
    ]

    res_ticks = engine._simulate_signal_ticks(
        sig=sig,
        actual_symbol="XAUUSD",
        sym_info=sym_info,
        broker_offset_sec=0,
        rules=rules,
        current_balance=10000.0,
        ticks=ticks,
        sim_spread=0.0,
        spread_mode="live",
    )
    assert res_ticks["status"] == "CLOSED"
    assert res_ticks["be_hit"] == 1, f"Expected be_hit=1, got {res_ticks}"
    assert res_ticks["tp1_hit"] == 0, f"Expected tp1_hit=0, got {res_ticks}"
    assert res_ticks["sl_hit"] == 0, f"Expected sl_hit=0, got {res_ticks}"
    assert res_ticks["outcome_badge"] == "BE1", f"Expected outcome_badge='BE1', got {res_ticks['outcome_badge']}"
    assert abs(res_ticks["profit"]) < 0.01, f"Expected $0 profit on BE recovery, got {res_ticks['profit']}"
    print("  [PASS] Tick simulation: underwater SET_BE recovered to entry and closed at BE1 ($0.00)")

    # Bar simulation of the same scenario:
    # Bar 1 (1700000000): open=2650.0, high=2650.5, low=2644.0, close=2645.0 (opens at 2650, dips to 2645)
    # Bar 2 (1700000060): open=2645.0, high=2646.0, low=2643.0, close=2645.0 (SET_BE arrives at 1700000120; b_open=2645.0 < 2650.0 -> moves TP to 2650.0)
    # Bar 3 (1700000120): open=2645.0, high=2651.0, low=2644.5, close=2648.0 (recovers to 2650.0 -> closes at BE1!)
    # Bar 4 (1700000180): open=2648.0, high=2648.0, low=2635.0, close=2636.0 (drops below original SL, but trade is already closed!)
    def _mk_bar(t, o, h, l, c):
        # Bullish bar: o -> l -> h -> c; Bearish bar: o -> h -> l -> c
        segs = [(o, l), (l, h), (h, c)] if c >= o else [(o, h), (h, l), (l, c)]
        return (t, o, h, l, c, segs)

    bars = [
        _mk_bar(1700000000, 2650.0, 2650.5, 2644.0, 2645.0),
        _mk_bar(1700000060, 2645.0, 2646.0, 2643.0, 2645.0),
        _mk_bar(1700000120, 2645.0, 2651.0, 2644.5, 2648.0),
        _mk_bar(1700000180, 2648.0, 2648.0, 2635.0, 2636.0),
    ]
    candle_cache = {(501, "XAUUSD", "mt5"): (bars, "M1", 60)}
    sym_cache = {"XAUUSD": sym_info}
    rules_m1 = dict(rules, execution_fidelity="m1", spread_mode="manual", manual_spread_pips=0.0)

    res_bars = engine._simulate_single_signal(
        sig=sig,
        sym_map={"XAUUSD": "XAUUSD"},
        sym_info_cache=sym_cache,
        broker_offset_sec=0,
        rules=rules_m1,
        current_balance=10000.0,
        candle_cache=candle_cache,
    )

    assert res_bars["status"] == "CLOSED"
    assert res_bars["be_hit"] == 1, f"Expected be_hit=1, got {res_bars}"
    assert res_bars["tp1_hit"] == 0, f"Expected tp1_hit=0, got {res_bars}"
    assert res_bars["sl_hit"] == 0, f"Expected sl_hit=0, got {res_bars}"
    assert res_bars["outcome_badge"] == "BE1", f"Expected outcome_badge='BE1', got {res_bars['outcome_badge']}"
    assert abs(res_bars["profit"]) < 0.01, f"Expected $0 profit on BE recovery, got {res_bars['profit']}"
    print("  [PASS] Bar simulation: underwater SET_BE recovered to entry and closed at BE1 ($0.00)")

    # Also test when underwater SET_BE does NOT recover and hits SL instead
    ticks_sl = [
        {"time": 1700000000, "bid": 2649.8, "ask": 2650.0},
        {"time": 1700000060, "bid": 2644.0, "ask": 2644.2},
        {"time": 1700000120, "bid": 2645.0, "ask": 2645.2},  # SET_BE moves TP to 2650.0, SL stays 2640.0
        {"time": 1700000180, "bid": 2639.5, "ask": 2639.7},  # Drops into SL (2640.0) without recovering to 2650.0
    ]
    res_ticks_sl = engine._simulate_signal_ticks(
        sig=sig,
        actual_symbol="XAUUSD",
        sym_info=sym_info,
        broker_offset_sec=0,
        rules=rules,
        current_balance=10000.0,
        ticks=ticks_sl,
        sim_spread=0.0,
        spread_mode="live",
    )
    assert res_ticks_sl["sl_hit"] == 1
    assert res_ticks_sl["be_hit"] == 0
    assert res_ticks_sl["outcome_badge"] == "SL"
    print("  [PASS] Tick simulation: underwater SET_BE that fails to recover still respects original SL")


def test_provider_analytics_classify():
    print("\n=== Test 5: provider_analytics.py & scraper.py ===")
    import provider_analytics as pa
    from backtester.scraper import classify_update_message

    assert pa.classify_close(pa.DEAL_REASON_TP, "[tp 2650.20]", 2.0, 1) == "SL BE"
    assert pa.classify_close(pa.DEAL_REASON_TP, "[tp 2670.00]", 200.0, 1) == "TP1"
    assert pa.classify_close(pa.DEAL_REASON_EXPERT, "Sirius BE Close", 0.5, 1) == "SL BE"
    assert classify_update_message("Set BE")["action"] == "SET_BE"
    assert classify_update_message("Setting BE guys")["action"] == "SET_BE"
    assert classify_update_message("Close at BE")["action"] == "SET_BE"
    assert classify_update_message("Move TP to BE")["action"] == "SET_BE"
    print("  [PASS] provider_analytics & scraper classify underwater BE closes and phrases accurately")


if __name__ == "__main__":
    test_mt5_executor_be()
    test_account_worker_be()
    test_ctrader_and_mt4_workers_be()
    test_backtester_underwater_be()
    test_provider_analytics_classify()
    print("\nALL SANDBOX TESTS PASSED SUCCESSFULLY!")
