import sys
import os

# Add parent directory to path so signal_parser can be imported
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from signal_parser import parse_with_regex, parse_signal, normalize_price_formatting

def test_normalization():
    print("--- 1. Testing normalize_price_formatting ---")
    cases = [
        ("#BTCUSD BUY: 73,800, SL: 72,800, TP: 75,800", "#BTCUSD BUY: 73800, SL: 72800, TP: 75800"),
        ("Entry: 1,897.35 SL: 1,901.67", "Entry: 1897.35 SL: 1901.67"),
        ("Gold BUY Entry: 4 116 TP: 4 165 SL: 4 092", "Gold BUY Entry: 4116 TP: 4165 SL: 4092"),
        ("US30 BUY Entry: 52 680 TP: 53 137 SL: 52 291", "US30 BUY Entry: 52680 TP: 53137 SL: 52291"),
        ("TP1: 100, 200, 300", "TP1: 100, 200, 300"),
        ("TP 4056 4057 4059", "TP 4056 4057 4059"),
    ]
    for inp, expected in cases:
        out = normalize_price_formatting(inp)
        assert out == expected, f"Failed on '{inp}': got '{out}', expected '{expected}'"
        print(f"PASS: '{inp}' -> '{out}'")

def test_parsing_provider_125():
    print("\n--- 2. Testing Provider 125 (Comma formatting) ---")
    msg = "#BTCUSD🔥 \n\n📊 BUY: 73,800\n\n🏆 TP1: 74,800\n\n🏆 TP2: 75,800\n\n☠️ SL: 72,800"
    res = parse_with_regex(msg)
    assert res is not None, "Failed to parse Prov 125 message"
    assert res["is_signal"] is True
    assert res["instrument"] == "BTCUSD"
    assert res["direction"] == "BUY"
    assert res["sl"] == 72800.0, f"Expected SL 72800.0, got {res['sl']}"
    assert res["tp"] == [74800.0, 75800.0], f"Expected TPs [74800.0, 75800.0], got {res['tp']}"
    assert res["entry"] == 73800.0, f"Expected entry 73800.0, got {res['entry']}"
    print(f"PASS: Provider 125 parsed correctly: {res}")

def test_parsing_provider_295():
    print("\n--- 3. Testing Provider 295 (Space formatting) ---")
    msg_gold = "🥇Gold\nDirection: 📈(BUY)\nEntry: 4 116\nTP: 4 165\nSL: 4 092"
    res_gold = parse_with_regex(msg_gold)
    assert res_gold is not None, "Failed to parse Prov 295 Gold"
    assert res_gold["is_signal"] is True
    assert res_gold["instrument"] == "XAUUSD"
    assert res_gold["direction"] == "BUY"
    assert res_gold["entry"] == 4116.0, f"Expected entry 4116.0, got {res_gold['entry']}"
    assert res_gold["sl"] == 4092.0, f"Expected SL 4092.0, got {res_gold['sl']}"
    assert res_gold["tp"] == [4165.0], f"Expected TP [4165.0], got {res_gold['tp']}"
    print(f"PASS: Provider 295 Gold parsed correctly: {res_gold}")

    msg_us30 = "🇺🇸US30\nDirection: 📈(BUY)\nEntry: 52 680\nTP: 53 137\nSL: 52 291"
    res_us30 = parse_with_regex(msg_us30)
    assert res_us30 is not None, "Failed to parse Prov 295 US30"
    assert res_us30["is_signal"] is True
    assert res_us30["instrument"] == "US30"
    assert res_us30["direction"] == "BUY"
    assert res_us30["entry"] == 52680.0, f"Expected entry 52680.0, got {res_us30['entry']}"
    assert res_us30["sl"] == 52291.0, f"Expected SL 52291.0, got {res_us30['sl']}"
    assert res_us30["tp"] == [53137.0], f"Expected TP [53137.0], got {res_us30['tp']}"
    print(f"PASS: Provider 295 US30 parsed correctly: {res_us30}")

def test_tp_list_unaffected():
    print("\n--- 4. Testing Multi-TP List Integrity ---")
    msg = "XAUUSD BUY NOW\nEntry: 4050\nTP1: 4055\nTP2: 4060\nTP3: 4065\nSL: 4040"
    res = parse_with_regex(msg)
    assert res is not None
    assert res["tp"] == [4055.0, 4060.0, 4065.0], f"Expected [4055.0, 4060.0, 4065.0], got {res['tp']}"
    assert res["sl"] == 4040.0
    print(f"PASS: Multi-TP preserved: {res['tp']}")

if __name__ == "__main__":
    test_normalization()
    test_parsing_provider_125()
    test_parsing_provider_295()
    test_tp_list_unaffected()
    print("\nALL UNIT TESTS PASSED SUCCESSFULLY!")
