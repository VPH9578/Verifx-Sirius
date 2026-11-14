import json
import os
import queue
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import db
import signal_parser
import signal_watch
from account_manager import AccountManager


class TestE1ThroughE7Fixes(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self._orig_db = db.DB_FILE
        db.DB_FILE = os.path.join(self.tmpdir.name, "test_e1_e7.db")
        db.init_db()

        self.acc_mgr = AccountManager.get_instance()
        self._orig_state_dir = self.acc_mgr.state_dir
        self._orig_accs = dict(self.acc_mgr.accounts_config)
        self._orig_workers = dict(self.acc_mgr.workers)
        self._orig_pending = dict(self.acc_mgr.pending_results)
        self._orig_late = dict(self.acc_mgr.late_exec_results)
        self.acc_mgr.state_dir = self.tmpdir.name

    def tearDown(self):
        db.DB_FILE = self._orig_db
        self.acc_mgr.state_dir = self._orig_state_dir
        self.acc_mgr.accounts_config = self._orig_accs
        self.acc_mgr.workers = self._orig_workers
        self.acc_mgr.pending_results = self._orig_pending
        self.acc_mgr.late_exec_results = self._orig_late
        self.tmpdir.cleanup()

    def _mock_expected_ctrader(self, provider_id="195"):
        self.acc_mgr.accounts_config = {
            "acc_1793656436": {
                "enabled": True,
                "name": "Varianse cTrader",
                "platform": "ctrader",
                "ctrader_account_id": 1793656436,
                "is_paused": False,
                "providers": [str(provider_id)],
                "provider_settings": {},
            }
        }

    def test_e1_late_worker_reply_confirms_copied_without_alert(self):
        self._mock_expected_ctrader("496")
        in_q = queue.Queue()
        out_q = queue.Queue()
        self.acc_mgr.workers = {
            "acc_1793656436": {
                "process": None,
                "in_q": in_q,
                "out_q": out_q,
                "config": self.acc_mgr.accounts_config["acc_1793656436"],
            }
        }
        req_id = "acc_1793656436_5958_test1"
        ai_calls = []
        alerts = []

        # Worker stays silent for 0.15s, then replies with placed tickets
        def delayed_worker():
            time.sleep(0.15)
            out_q.put({
                "req_id": req_id,
                "result": [{"ticket": 1341896}, {"ticket": 1341897}, {"ticket": 1341898}],
                "diag": {"account_id": "acc_1793656436", "status": "COPIED", "reason": "Placed 3 order(s) late."},
            })

        t = threading.Thread(target=delayed_worker, daemon=True)
        t.start()

        out = signal_watch.process_signal_event(
            {
                "provider_id": "496",
                "provider_name": "Alpha Fx",
                "message_id": 5958,
                "raw_message": "XAUUSD BUY NOW ENTRY POINT 3930 SL 3923 TP 3935",
                "parsed_signal": {"instrument": "XAUUSD", "direction": "BUY", "sl": 3923.0, "tp": [3935.0]},
                "exec_results": [],
                "account_diagnostics": {
                    "acc_1793656436": {
                        "account_id": "acc_1793656436",
                        "status": "TIMEOUT",
                        "reason": "Worker acc_1793656436 did not reply within 3.5s timeout.",
                        "extra": {"req_id": req_id},
                    }
                },
                "event_kind": "NEW_SIGNAL",
                "grace_seconds": 1.2,
            },
            ai_caller=lambda p: (ai_calls.append(p) or {}),
            alert_sender=lambda *a: (alerts.append(a) or True),
            skip_journals=True,
        )
        t.join(timeout=1.0)
        self.assertEqual(out["overall_status"], "COPIED")
        self.assertFalse(out["has_discrepancy"])
        self.assertEqual(out["account_statuses"]["acc_1793656436"]["tickets"], [1341896, 1341897, 1341898])
        self.assertTrue(out["account_statuses"]["acc_1793656436"]["extra"].get("confirmed_late"))
        self.assertEqual(len(ai_calls), 0)
        self.assertEqual(len(alerts), 0)

    def test_e1_trade_state_file_confirms_copied_without_alert(self):
        self._mock_expected_ctrader("200")
        self.acc_mgr.workers = {}
        state_path = os.path.join(self.tmpdir.name, "trade_state_acc_1793656436.json")
        with open(state_path, "w", encoding="utf-8") as f:
            json.dump({"message_to_tickets": {"22831": [1341886, 1341887, 1341888]}}, f)

        ai_calls = []
        alerts = []
        out = signal_watch.process_signal_event(
            {
                "provider_id": "200",
                "provider_name": "Synergy",
                "message_id": 22831,
                "raw_message": "BUY GOLD @ 3933/3927 SL 3926 TP 3936",
                "parsed_signal": {"instrument": "XAUUSD", "direction": "BUY", "sl": 3926.0, "tp": [3936.0]},
                "exec_results": [],
                "account_diagnostics": {
                    "acc_1793656436": {
                        "account_id": "acc_1793656436",
                        "status": "TIMEOUT",
                        "reason": "Worker acc_1793656436 did not reply within 3.5s timeout.",
                        "extra": {"req_id": "missing_req"},
                    }
                },
                "event_kind": "NEW_SIGNAL",
                "grace_seconds": 0.4,
            },
            ai_caller=lambda p: (ai_calls.append(p) or {}),
            alert_sender=lambda *a: (alerts.append(a) or True),
            skip_journals=True,
        )
        self.assertEqual(out["overall_status"], "COPIED")
        self.assertFalse(out["has_discrepancy"])
        self.assertEqual(out["account_statuses"]["acc_1793656436"]["tickets"], [1341886, 1341887, 1341888])
        self.assertEqual(len(ai_calls), 0)
        self.assertEqual(len(alerts), 0)

    def test_e2_low_confidence_skips_alert_and_no_magic_code_context_on_timeout(self):
        self._mock_expected_ctrader("200")
        self.acc_mgr.workers = {}
        alerts = []
        captured_prompts = []

        # Verify code context does NOT inject ctrader_client.py on bare TIMEOUT
        ctx = signal_watch.collect_relevant_code_context(
            ["acc_1793656436: TIMEOUT - Worker acc_1793656436 did not reply within 3.5s timeout."],
            ["ctrader"],
            broker_journals={"acc_1793656436": {"lines": []}},
        )
        self.assertNotIn("ctrader_client.py", ctx)

        out = signal_watch.process_signal_event(
            {
                "provider_id": "200",
                "provider_name": "Synergy",
                "message_id": 99999,
                "raw_message": "BUY GOLD @ 3933 SL 3926 TP 3936",
                "parsed_signal": {"instrument": "XAUUSD", "direction": "BUY", "sl": 3926.0, "tp": [3936.0]},
                "exec_results": [],
                "account_diagnostics": {
                    "acc_1793656436": {
                        "account_id": "acc_1793656436",
                        "status": "TIMEOUT",
                        "reason": "Worker did not reply within 3.5s timeout.",
                        "extra": {},
                    }
                },
                "event_kind": "NEW_SIGNAL",
                "grace_seconds": 0.1,
            },
            ai_caller=lambda p: (captured_prompts.append(p) or {
                "short_summary": "Worker timed out; logs contained no explicit rejection line.",
                "report_markdown": "## Incident Overview\nNo rejection logged.",
                "antigravity_prompt": "Check worker heartbeat.",
                "confidence": "low",
            }),
            alert_sender=lambda *a: (alerts.append(a) or True),
            skip_journals=True,
        )
        self.assertEqual(out["overall_status"], "FAILED")
        self.assertEqual(out["report"]["confidence"], "low")
        self.assertEqual(len(alerts), 0)
        row = db.get_signal_watch_event(out["watch_id"])
        self.assertEqual(row["ai_confidence"], "low")
        self.assertEqual(row["alert_sent"], 0)
        self.assertIn("STRICT EVIDENCE", captured_prompts[0])

    def test_e4_incognito_scanner_elitetradingsignals_and_gemini_gate(self):
        scanner_msg = """🔔 SIGNAL ACTIVE
---
Instrument: USDJPY
Trade: SELL
Level: 159.638

🎯 T1 - 159.548
🎯 T2 - 159.458
🎯 T3 - 159.368
🎯 T4 - 

🛑 Stop Loss: 159.728"""
        parsed = signal_parser.parse_with_regex(scanner_msg)
        self.assertIsNotNone(parsed)
        self.assertTrue(parsed["is_signal"])
        self.assertEqual(parsed["instrument"], "USDJPY")
        self.assertEqual(parsed["direction"], "SELL")
        self.assertEqual(parsed["order_type"], "MARKET")
        self.assertAlmostEqual(parsed["entry"], 159.638, places=3)
        self.assertAlmostEqual(parsed["sl"], 159.728, places=3)
        self.assertEqual(parsed["tp"], [159.548, 159.458, 159.368])

        elite_msg = """**#GOLD**** LONG FROM SUPPORT****🟢**
📈**GOLD SIGNAL**
✔️**Trade Direction: long
****✔️****Entry Level: 3,891.11
****✔️****Target Level: 3,966.32
****✔️****Stop Loss: 3,841.20"""
        parsed2 = signal_parser.parse_with_regex(elite_msg)
        self.assertIsNotNone(parsed2)
        self.assertEqual(parsed2["instrument"], "XAUUSD")
        self.assertEqual(parsed2["direction"], "BUY")
        self.assertEqual(parsed2["order_type"], "MARKET")
        self.assertAlmostEqual(parsed2["entry"], 3891.11, places=2)
        self.assertAlmostEqual(parsed2["sl"], 3841.20, places=2)
        self.assertEqual(parsed2["tp"], [3966.32])

        # Verify widened Gemini backup gate fires when literal "TP"/"SL" are absent but "TARGET"+"STOP LOSS" exist
        called_urls = []
        class FakeResp:
            status_code = 200
            def json(self):
                return {"candidates": [{"content": {"parts": [{"text": json.dumps({
                    "is_signal": True, "direction": "BUY", "order_type": "MARKET",
                    "entry": 3900.0, "sl": 3885.0, "tp": [3930.0], "instrument": "XAUUSD"
                })}]}}]}

        with patch.object(signal_parser, "parse_with_regex", return_value=None), \
             patch.object(signal_parser.httpx, "post", side_effect=lambda u, **kw: (called_urls.append(u) or FakeResp())):
            gres = signal_parser.parse_signal("Buy Gold around 3900, Target 3930, Stop Loss 3885")
        self.assertEqual(len(called_urls), 1)
        self.assertEqual(gres["instrument"], "XAUUSD")

    def test_e5_selllimit_joined_keyword_parsed_as_limit_with_entry(self):
        obfx_msg = """XAGUSD
SELLLIMIT:51.20780
SL:51.30431
TP:50.61699"""
        parsed = signal_parser.parse_with_regex(obfx_msg)
        self.assertIsNotNone(parsed)
        self.assertTrue(parsed["is_signal"])
        self.assertEqual(parsed["instrument"], "XAGUSD")
        self.assertEqual(parsed["direction"], "SELL")
        self.assertEqual(parsed["order_type"], "LIMIT")
        self.assertAlmostEqual(parsed["entry"], 51.20780, places=5)
        self.assertAlmostEqual(parsed["sl"], 51.30431, places=5)
        self.assertEqual(parsed["tp"], [50.61699])

    def test_e6_entry_point_emoji_extracted(self):
        alpha_msg = """**✅****XAUUSD BUY  NOW 

****🇦🇪****ENTRY POINT 👉 3930

****✅****TP¹   3935
****✅****TP²   3940
****✅****TP³   3945
****✅****TP⁴   OPEN

****✅****SL     3923"""
        parsed = signal_parser.parse_with_regex(alpha_msg)
        self.assertIsNotNone(parsed)
        self.assertTrue(parsed["is_signal"])
        self.assertEqual(parsed["instrument"], "XAUUSD")
        self.assertEqual(parsed["direction"], "BUY")
        self.assertEqual(parsed["order_type"], "MARKET")
        self.assertAlmostEqual(parsed["entry"], 3930.0, places=2)
        self.assertAlmostEqual(parsed["sl"], 3923.0, places=2)
        self.assertEqual(parsed["tp"], [3935.0, 3940.0, 3945.0])

    def test_e7_sl_single_digit_fix_and_unfixable_guard(self):
        metlife_msg = """**✔️XAUUSD BUY 3908
🔹TP 3911
🔹TP 3914
🔹TP 3917
🔹TP 3920
🔶SL 3997**"""
        parsed = signal_parser.parse_with_regex(metlife_msg)
        self.assertIsNotNone(parsed)
        self.assertTrue(parsed["is_signal"])
        self.assertEqual(parsed["sl"], 3897.0)
        self.assertEqual(parsed["sl_original"], 3997.0)
        self.assertIn("3997", parsed["sl_autofix_note"])

        # Same fix holds in shared executor autofix helper
        fixed = signal_parser.normalize_and_autofix_signal(
            {"instrument": "XAUUSD", "direction": "BUY", "entry": 3908.0, "sl": 3997.0, "tp": [3911.0, 3914.0, 3920.0]},
            live_bid=3907.8,
            live_ask=3908.1,
        )
        self.assertEqual(fixed["sl"], 3897.0)

        # Unfixable wrong-side SL must be rejected instead of placing a garbage SL
        bad_msg = "XAUUSD BUY 3908 SL 5555 TP 3911 TP 3914"
        bad_parsed = signal_parser.parse_signal(bad_msg)
        self.assertIsNotNone(bad_parsed)
        self.assertFalse(bad_parsed["is_signal"])
        self.assertEqual(bad_parsed["error_code"], "SL_TYPO_UNFIXABLE")


if __name__ == "__main__":
    unittest.main(verbosity=2)
