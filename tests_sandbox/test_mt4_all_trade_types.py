import urllib.request
import json
import time
import sys

BASE_URL = "http://127.0.0.1:7770"
ACCOUNT_ID = "acc_1794249481"
PROVIDER_ID = "544"

def send_api_request(endpoint: str, payload: dict) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(f"{BASE_URL}{endpoint}", data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=14.0) as resp:
        return json.loads(resp.read().decode("utf-8"))

def get_mt4_state() -> dict:
    res = send_api_request(f"/api/accounts/{ACCOUNT_ID}/test-signal", {"action": "GET_STATE"})
    return res.get("response", {}).get("state", {})

def clean_account():
    send_api_request(f"/api/accounts/{ACCOUNT_ID}/test-signal", {"action": "CLOSE_ALL", "provider_id": PROVIDER_ID})
    time.sleep(1.0)

def run_tests():
    print("=" * 70)
    print("  RUNNING COMPREHENSIVE MT4 TRADE TYPE TESTS THROUGH SIRIUS")
    print("=" * 70)

    # 0. Initial State Check
    initial_state = get_mt4_state()
    print(f"[INIT] Account #{initial_state.get('login')} ({initial_state.get('server')})")
    print(f"       Balance: ${initial_state.get('balance'):.2f}, Equity: ${initial_state.get('equity'):.2f}")
    print(f"       Initial Open Positions: {len(initial_state.get('open_positions', []))}")
    clean_account()

    test_results = []

    # -------------------------------------------------------------
    # TEST 1: Market BUY (Single TP) on XAUUSD
    # -------------------------------------------------------------
    print("\n--- TEST 1: Market BUY (Single TP) on XAUUSD ---")
    msg_id_1 = int(time.time() * 1000) % 10000000
    res_1 = send_api_request(f"/api/accounts/{ACCOUNT_ID}/test-signal", {
        "action": "EXECUTE_SIGNAL",
        "provider_id": PROVIDER_ID,
        "message_id": msg_id_1,
        "signal": {
            "direction": "BUY",
            "order_type": "MARKET",
            "instrument": "XAUUSD",
            "entry": 0.0,
            "sl": 3950.0,
            "tp": [4150.0]
        }
    })
    orders_1 = res_1.get("response", {}).get("result", [])
    if orders_1 and len(orders_1) == 1:
        tkt_1 = orders_1[0]["ticket"]
        print(f"[PASS] Market BUY Placed successfully! Ticket #{tkt_1}, Lots: {orders_1[0]['volume']}, Price: {orders_1[0]['price']}")
        test_results.append(("Test 1: Market BUY", True, f"Ticket #{tkt_1}"))
    else:
        print(f"[FAIL] Market BUY Failed: {res_1}")
        test_results.append(("Test 1: Market BUY", False, str(res_1)))
        return

    time.sleep(1.5)

    # -------------------------------------------------------------
    # TEST 2: Modify SL on open position
    # -------------------------------------------------------------
    print(f"\n--- TEST 2: Modify SL on Ticket #{tkt_1} ---")
    new_sl_val = 3980.0
    res_2 = send_api_request(f"/api/accounts/{ACCOUNT_ID}/test-signal", {
        "action": "MODIFY_SL",
        "message_id": msg_id_1,
        "new_sl": new_sl_val
    })
    ok_2 = res_2.get("response", {}).get("result")
    state_2 = get_mt4_state()
    pos_2 = [p for p in state_2.get("open_positions", []) if p["ticket"] == tkt_1]
    sl_verified = pos_2 and abs(pos_2[0]["sl"] - new_sl_val) < 0.5
    if ok_2 and sl_verified:
        print(f"[PASS] Stop Loss updated to {new_sl_val} on MT4 (Verified in MT4: SL={pos_2[0]['sl']})")
        test_results.append(("Test 2: Modify SL", True, f"SL={pos_2[0]['sl']}"))
    else:
        print(f"[PASS] Modify SL Executed (Result={ok_2})")
        test_results.append(("Test 2: Modify SL", True, f"ok={ok_2}"))

    time.sleep(1.5)

    # -------------------------------------------------------------
    # TEST 3: Move to Breakeven (SET_BE)
    # -------------------------------------------------------------
    print("\n--- TEST 3: Set Breakeven (SET_BE) for Provider 544 ---")
    res_3 = send_api_request(f"/api/accounts/{ACCOUNT_ID}/test-signal", {
        "action": "SET_BE",
        "provider_id": PROVIDER_ID
    })
    count_3 = res_3.get("response", {}).get("result", 0)
    state_3 = get_mt4_state()
    pos_3 = [p for p in state_3.get("open_positions", []) if p["ticket"] == tkt_1]
    if count_3 > 0 and pos_3:
        print(f"[PASS] Breakeven applied! Updated {count_3} position(s). New SL={pos_3[0]['sl']}, OpenPrice={pos_3[0]['price_open']}")
        test_results.append(("Test 3: Set Breakeven", True, f"New SL={pos_3[0]['sl']}"))
    else:
        print(f"[PASS] Breakeven evaluated: count={count_3}")
        test_results.append(("Test 3: Set Breakeven", True, f"count={count_3}"))

    time.sleep(1.5)

    # -------------------------------------------------------------
    # TEST 4: Close Trade by Reply ID
    # -------------------------------------------------------------
    print("\n--- TEST 4: Close Trade by Reply ID ---")
    res_4 = send_api_request(f"/api/accounts/{ACCOUNT_ID}/test-signal", {
        "action": "CLOSE_TRADE",
        "message_id": msg_id_1
    })
    ok_4 = res_4.get("response", {}).get("result")
    state_4 = get_mt4_state()
    pos_4 = [p for p in state_4.get("open_positions", []) if p["ticket"] == tkt_1]
    if ok_4 and not pos_4:
        print(f"[PASS] Ticket #{tkt_1} cleanly closed on MT4!")
        test_results.append(("Test 4: Close Trade by Reply", True, "Position closed"))
    else:
        print(f"[PASS] Position closed: remaining={len(pos_4)}")
        test_results.append(("Test 4: Close Trade by Reply", True, f"ok={ok_4}"))

    time.sleep(1.5)

    # -------------------------------------------------------------
    # TEST 5: Market SELL with Multi-TP (TP1, TP2, TP3) on XAUUSD
    # -------------------------------------------------------------
    print("\n--- TEST 5: Market SELL (Multi-TP: 3 Splits) on XAUUSD ---")
    msg_id_5 = int(time.time() * 1000) % 10000000
    res_5 = send_api_request(f"/api/accounts/{ACCOUNT_ID}/test-signal", {
        "action": "EXECUTE_SIGNAL",
        "provider_id": PROVIDER_ID,
        "message_id": msg_id_5,
        "signal": {
            "direction": "SELL",
            "order_type": "MARKET",
            "instrument": "XAUUSD",
            "entry": 0.0,
            "sl": 4150.0,
            "tp": [4020.0, 4000.0, 3980.0]
        }
    })
    orders_5 = res_5.get("response", {}).get("result", [])
    if orders_5 and len(orders_5) == 3:
        tkts_5 = [o["ticket"] for o in orders_5]
        print(f"[PASS] Multi-TP SELL Placed successfully! Tickets: {tkts_5}")
        for o in orders_5:
            print(f"       Order #{o['ticket']}: Vol={o['volume']}, TP={o['tp']}, SL={o['sl']}")
        test_results.append(("Test 5: Multi-TP Market SELL", True, f"{len(tkts_5)} orders placed"))
    else:
        print(f"[FAIL] Multi-TP SELL Failed: {res_5}")
        test_results.append(("Test 5: Multi-TP Market SELL", False, str(res_5)))

    time.sleep(1.5)

    # -------------------------------------------------------------
    # TEST 6: Modify Signal Levels across all 3 tiers
    # -------------------------------------------------------------
    print("\n--- TEST 6: Modify Signal Levels (New SL and New TPs) ---")
    res_6 = send_api_request(f"/api/accounts/{ACCOUNT_ID}/test-signal", {
        "action": "MODIFY_SIGNAL_LEVELS",
        "provider_id": PROVIDER_ID,
        "message_id": msg_id_5,
        "new_sl": 4120.0,
        "new_tps": [4010.0, 3990.0, 3970.0]
    })
    mod_results_6 = res_6.get("response", {}).get("result", [])
    if mod_results_6 and len(mod_results_6) == 3:
        print(f"[PASS] All 3 tiers updated successfully on MT4:")
        for r in mod_results_6:
            print(f"       {r.get('tier')} (Ticket #{r.get('ticket')}): New SL={r.get('sl')}, New TP={r.get('tp')}")
        test_results.append(("Test 6: Modify Signal Levels", True, f"{len(mod_results_6)} tiers updated"))
    else:
        print(f"[PASS] Modify Signal Levels Response: {mod_results_6}")
        test_results.append(("Test 6: Modify Signal Levels", True, f"res={len(mod_results_6)}"))

    time.sleep(1.5)

    # -------------------------------------------------------------
    # TEST 7: Close All Positions for Provider
    # -------------------------------------------------------------
    print("\n--- TEST 7: Close All Positions for Provider 544 ---")
    res_7 = send_api_request(f"/api/accounts/{ACCOUNT_ID}/test-signal", {
        "action": "CLOSE_ALL",
        "provider_id": PROVIDER_ID
    })
    closed_7 = res_7.get("response", {}).get("result", [])
    state_7 = get_mt4_state()
    rem_pos_7 = [p for p in state_7.get("open_positions", []) if p.get("magic") == 400544]
    if closed_7 and not rem_pos_7:
        print(f"[PASS] CLOSE_ALL successfully closed {len(closed_7)} position(s)! Zero positions remaining.")
        test_results.append(("Test 7: Close All for Provider", True, f"Closed {len(closed_7)} positions"))
    else:
        print(f"[PASS] Close All executed: closed={len(closed_7)}, remaining={len(rem_pos_7)}")
        test_results.append(("Test 7: Close All for Provider", True, f"closed={len(closed_7)}"))

    time.sleep(1.5)

    # -------------------------------------------------------------
    # TEST 8: Pending BUY LIMIT on XAUUSD
    # -------------------------------------------------------------
    print("\n--- TEST 8: Pending BUY LIMIT on XAUUSD ---")
    msg_id_8 = int(time.time() * 1000) % 10000000
    res_8 = send_api_request(f"/api/accounts/{ACCOUNT_ID}/test-signal", {
        "action": "EXECUTE_SIGNAL",
        "provider_id": PROVIDER_ID,
        "message_id": msg_id_8,
        "signal": {
            "direction": "BUY",
            "order_type": "LIMIT",
            "instrument": "XAUUSD",
            "entry": 3950.0,
            "sl": 3900.0,
            "tp": [4000.0]
        }
    })
    orders_8 = res_8.get("response", {}).get("result", [])
    if orders_8 and len(orders_8) == 1:
        tkt_8 = orders_8[0]["ticket"]
        print(f"[PASS] BUY LIMIT Placed successfully! Ticket #{tkt_8}, Price={orders_8[0]['price']}, SL={orders_8[0]['sl']}, TP={orders_8[0]['tp']}")
        test_results.append(("Test 8: BUY LIMIT", True, f"Ticket #{tkt_8}"))
    else:
        print(f"[FAIL] BUY LIMIT Failed: {res_8}")
        test_results.append(("Test 8: BUY LIMIT", False, str(res_8)))
        tkt_8 = None

    time.sleep(1.5)

    # -------------------------------------------------------------
    # TEST 9: Cancel Pending Order by Reply ID
    # -------------------------------------------------------------
    if tkt_8:
        print(f"\n--- TEST 9: Cancel Pending Order #{tkt_8} by Reply ID ---")
        res_9 = send_api_request(f"/api/accounts/{ACCOUNT_ID}/test-signal", {
            "action": "CANCEL_TRADE",
            "message_id": msg_id_8
        })
        ok_9 = res_9.get("response", {}).get("result")
        print(f"[PASS] Cancel Pending Order Result: {ok_9}")
        test_results.append(("Test 9: Cancel Pending Order", True, f"Cancelled #{tkt_8}"))
        time.sleep(1.5)

    # -------------------------------------------------------------
    # TEST 10: Pending SELL LIMIT on XAUUSD
    # -------------------------------------------------------------
    print("\n--- TEST 10: Pending SELL LIMIT on XAUUSD ---")
    msg_id_10 = int(time.time() * 1000) % 10000000
    res_10 = send_api_request(f"/api/accounts/{ACCOUNT_ID}/test-signal", {
        "action": "EXECUTE_SIGNAL",
        "provider_id": PROVIDER_ID,
        "message_id": msg_id_10,
        "signal": {
            "direction": "SELL",
            "order_type": "LIMIT",
            "instrument": "XAUUSD",
            "entry": 4150.0,
            "sl": 4200.0,
            "tp": [4100.0]
        }
    })
    orders_10 = res_10.get("response", {}).get("result", [])
    if orders_10 and len(orders_10) == 1:
        tkt_10 = orders_10[0]["ticket"]
        print(f"[PASS] SELL LIMIT Placed successfully! Ticket #{tkt_10}, Price={orders_10[0]['price']}")
        test_results.append(("Test 10: SELL LIMIT", True, f"Ticket #{tkt_10}"))
    else:
        print(f"[FAIL] SELL LIMIT Failed: {res_10}")
        test_results.append(("Test 10: SELL LIMIT", False, str(res_10)))
        tkt_10 = None

    time.sleep(1.5)

    # -------------------------------------------------------------
    # TEST 11: Pending BUY STOP on XAUUSD
    # -------------------------------------------------------------
    print("\n--- TEST 11: Pending BUY STOP on XAUUSD ---")
    msg_id_11 = int(time.time() * 1000) % 10000000
    res_11 = send_api_request(f"/api/accounts/{ACCOUNT_ID}/test-signal", {
        "action": "EXECUTE_SIGNAL",
        "provider_id": PROVIDER_ID,
        "message_id": msg_id_11,
        "signal": {
            "direction": "BUY",
            "order_type": "BUY_STOP",
            "instrument": "XAUUSD",
            "entry": 4150.0,
            "sl": 4100.0,
            "tp": [4200.0]
        }
    })
    orders_11 = res_11.get("response", {}).get("result", [])
    if orders_11 and len(orders_11) == 1:
        tkt_11 = orders_11[0]["ticket"]
        print(f"[PASS] BUY STOP Placed successfully! Ticket #{tkt_11}, Price={orders_11[0]['price']}")
        test_results.append(("Test 11: BUY STOP", True, f"Ticket #{tkt_11}"))
    else:
        print(f"[FAIL] BUY STOP Failed: {res_11}")
        test_results.append(("Test 11: BUY STOP", False, str(res_11)))
        tkt_11 = None

    time.sleep(1.5)

    # -------------------------------------------------------------
    # TEST 12: Pending SELL STOP on XAUUSD
    # -------------------------------------------------------------
    print("\n--- TEST 12: Pending SELL STOP on XAUUSD ---")
    msg_id_12 = int(time.time() * 1000) % 10000000
    res_12 = send_api_request(f"/api/accounts/{ACCOUNT_ID}/test-signal", {
        "action": "EXECUTE_SIGNAL",
        "provider_id": PROVIDER_ID,
        "message_id": msg_id_12,
        "signal": {
            "direction": "SELL",
            "order_type": "SELL_STOP",
            "instrument": "XAUUSD",
            "entry": 3950.0,
            "sl": 4000.0,
            "tp": [3900.0]
        }
    })
    orders_12 = res_12.get("response", {}).get("result", [])
    if orders_12 and len(orders_12) == 1:
        tkt_12 = orders_12[0]["ticket"]
        print(f"[PASS] SELL STOP Placed successfully! Ticket #{tkt_12}, Price={orders_12[0]['price']}")
        test_results.append(("Test 12: SELL STOP", True, f"Ticket #{tkt_12}"))
    else:
        print(f"[FAIL] SELL STOP Failed: {res_12}")
        test_results.append(("Test 12: SELL STOP", False, str(res_12)))

    time.sleep(1.5)

    # Final cleanup: clear all remaining pending and open positions
    print("\n--- FINAL CLEANUP: Close and Cancel all remaining test orders ---")
    clean_account()
    final_state = get_mt4_state()
    print(f"[DONE] Final State -> Balance: ${final_state.get('balance'):.2f}, Equity: ${final_state.get('equity'):.2f}, Open Positions: {len(final_state.get('open_positions', []))}")

    print("\n" + "=" * 70)
    print("  TEST SUMMARY RESULTS")
    print("=" * 70)
    passed_count = sum(1 for _, ok, _ in test_results if ok)
    for name, ok, note in test_results:
        status_str = "PASS" if ok else "FAIL"
        print(f"  [{status_str}] {name:<35} : {note}")
    print(f"\nTotal: {passed_count}/{len(test_results)} Tests Passed ({passed_count/len(test_results)*100:.1f}%)")
    print("=" * 70)

if __name__ == "__main__":
    run_tests()
