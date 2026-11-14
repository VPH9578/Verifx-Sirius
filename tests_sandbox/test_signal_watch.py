import json
import os
import tempfile
import threading
import time
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

import db
import signal_watch
from account_manager import AccountManager


class SignalWatchSandboxTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.test_db_path = os.path.join(self.tmpdir.name, "test_signal_watch.db")
        self.orig_db_file = db.DB_FILE
        db.DB_FILE = self.test_db_path
        db.init_db()

        self.fake_accounts = {
            "acc_1": {
                "id": "acc_1",
                "name": "PUPrime MT5",
                "platform": "mt5",
                "mt5_login": 33849921,
                "enabled": True,
                "is_paused": False,
                "providers": ["543"],
                "provider_settings": {"543": {"trading_enabled": True, "risk_mode": "percent", "risk_value": 1.0}},
            },
            "acc_1794159206": {
                "id": "acc_1794159206",
                "name": "Groot MT5",
                "platform": "mt5",
                "mt5_login": 27794357,
                "enabled": True,
                "is_paused": False,
                "providers": ["543"],
                "provider_settings": {"543": {"trading_enabled": True, "risk_mode": "percent", "risk_value": 1.0}},
            },
            "acc_1793656436": {
                "id": "acc_1793656436",
                "name": "Varianse cTrader",
                "platform": "ctrader",
                "ctrader_account_id": 10010341,
                "enabled": True,
                "is_paused": False,
                "providers": ["543", "11"],
                "provider_settings": {"543": {"trading_enabled": True}, "11": {"trading_enabled": True}},
            },
            "acc_1794249481": {
                "id": "acc_1794249481",
                "name": "MetaTrader 4",
                "platform": "mt4",
                "mt4_login": 2100151340,
                "enabled": True,
                "is_paused": True,
                "providers": ["543"],
                "provider_settings": {"543": {"trading_enabled": True}},
            },
        }

        acc_mgr = AccountManager.get_instance()
        self.orig_load = acc_mgr.load_accounts
        self.orig_accounts_cfg = acc_mgr.accounts_config
        acc_mgr.accounts_config = self.fake_accounts
        acc_mgr.load_accounts = lambda: self.fake_accounts

    def tearDown(self):
        acc_mgr = AccountManager.get_instance()
        acc_mgr.load_accounts = self.orig_load
        acc_mgr.accounts_config = self.orig_accounts_cfg
        db.DB_FILE = self.orig_db_file
        self.tmpdir.cleanup()

    def test_1_all_unpaused_accounts_copied_no_alert(self):
        ai_called = []
        alert_called = []

        event = {
            "provider_id": "543",
            "provider_name": "Lorax VIP",
            "channel_id": "-1002385852838",
            "topic_id": 8,
            "message_id": 99001,
            "raw_message": "XAUUSD BUY @ 4050\nSL 4040\nTP 4060",
            "parsed_signal": {"is_signal": True, "instrument": "XAUUSD", "direction": "BUY", "entry": 4050.0, "sl": 4040.0, "tp": [4060.0]},
            "exec_results": [
                {"account_id": "acc_1", "ticket": 1001},
                {"account_id": "acc_1794159206", "ticket": 2001},
                {"account_id": "acc_1793656436", "ticket": 3001},
            ],
            "account_diagnostics": {},
            "event_kind": "NEW_SIGNAL",
        }

        res = signal_watch.process_signal_event(
            event,
            ai_caller=lambda p: ai_called.append(p) or {},
            alert_sender=lambda *a: alert_called.append(a) or True,
            skip_journals=True,
        )

        self.assertEqual(res["overall_status"], "COPIED")
        self.assertFalse(res["has_discrepancy"])
        self.assertEqual(res["account_statuses"]["acc_1"]["status"], "COPIED")
        self.assertEqual(res["account_statuses"]["acc_1794249481"]["status"], "PAUSED")
        self.assertEqual(len(ai_called), 0)
        self.assertEqual(len(alert_called), 0)

        saved = db.get_signal_watch_event(res["watch_id"])
        self.assertEqual(saved["overall_status"], "COPIED")
        self.assertEqual(saved["report_status"], "NONE")

    def test_2_partial_failure_triggers_ai_report_and_telegram_alert(self):
        alerts = []
        ai_prompts = []

        def fake_ai(prompt):
            ai_prompts.append(prompt)
            return {
                "short_summary": "acc_1793656436 (cTrader) rejected due to POSITION_PRICE_TOO_FAR_FROM_MARKET; acc_1794159206 blocked by Risk Guard.",
                "report_markdown": "## Incident Overview\ncTrader and Groot MT5 missed Msg #96819.\n## Recommended Fix\nWiden cTrader deviation buffer and enable tier-capping.",
                "antigravity_prompt": "Please update ctrader_client.py deviation tolerance and verify acc_1794159206 risk sizing in tests_sandbox/.",
            }

        event = {
            "provider_id": "543",
            "provider_name": "Lorax VIP",
            "channel_id": "-1002385852838",
            "topic_id": 8,
            "message_id": 96819,
            "raw_message": "SELL XAUUSD @ 4703\nSL 4712\nTP 4690",
            "parsed_signal": {"is_signal": True, "instrument": "XAUUSD", "direction": "SELL", "entry": 4703.0, "sl": 4712.0, "tp": [4690.0]},
            "exec_results": [{"account_id": "acc_1", "ticket": 55501}],
            "account_diagnostics": {
                "acc_1": {"status": "COPIED", "reason": "Placed 1 order on XAUUSD.sc"},
                "acc_1794159206": {"status": "BLOCKED_RISK_GUARD", "reason": "Total risk $27.00 exceeds max $12.50"},
                "acc_1793656436": {"status": "ORDER_SEND_FAILED", "reason": "TP1: POSITION_PRICE_TOO_FAR_FROM_MARKET"},
            },
            "event_kind": "NEW_SIGNAL",
        }

        res = signal_watch.process_signal_event(
            event,
            ai_caller=fake_ai,
            alert_sender=lambda ev, wid, sts, summ: alerts.append((wid, summ)) or True,
            skip_journals=True,
        )

        self.assertEqual(res["overall_status"], "PARTIAL_FAIL")
        self.assertTrue(res["has_discrepancy"])
        self.assertEqual(len(ai_prompts), 1)
        # Telegram buzz alerts are disabled; reports persist to Signal Watch dashboard DB
        self.assertEqual(len(alerts), 0)

        saved = db.get_signal_watch_event(res["watch_id"])
        self.assertEqual(saved["report_status"], "READY")
        self.assertEqual(saved["alert_sent"], 0)
        self.assertIn("ctrader_client.py", saved["antigravity_prompt"])
        self.assertFalse(signal_watch.send_watch_telegram_alert(event, res["watch_id"], res["account_statuses"], "summary"))

    def test_3_utf16le_journal_readers_mt5_mt4_ctrader(self):
        day_str = datetime.now().strftime("%Y%m%d")

        # 3A: Fake MT5 terminal directory with UTF-16LE origin.txt + logs + MQL5/Logs
        mq_root = os.path.join(self.tmpdir.name, "MetaQuotes", "Terminal")
        mt5_term = os.path.join(mq_root, "HASH_MT5_1")
        os.makedirs(os.path.join(mt5_term, "logs"), exist_ok=True)
        os.makedirs(os.path.join(mt5_term, "MQL5", "Logs"), exist_ok=True)
        with open(os.path.join(mt5_term, "origin.txt"), "wb") as f:
            f.write(b"\xff\xfe" + r"C:\Program Files\Groot PU Prime MT5 Terminal".encode("utf-16-le"))
        with open(os.path.join(mt5_term, "logs", f"{day_str}.log"), "wb") as f:
            f.write(b"\xff\xfe" + "09:06:44.100 Trades '27794357': failed buy limit 0.01 XAUUSD retcode=10016 [Invalid stops]\n".encode("utf-16-le"))
        with open(os.path.join(mt5_term, "MQL5", "Logs", f"{day_str}.log"), "w", encoding="utf-8") as f:
            f.write("09:06:44 Sirius Python bridge check XAUUSD\n")

        mt5_logs = signal_watch.read_mt5_journal_logs(
            {"mt5_path": r"C:\Program Files\Groot PU Prime MT5 Terminal\terminal64.exe"},
            int(time.time()),
            ["XAUUSD"],
            metaquotes_root=mq_root,
        )
        self.assertEqual(mt5_logs["terminal_dir"], mt5_term)
        self.assertTrue(any("10016" in ln for ln in mt5_logs["journal_lines"]))
        self.assertTrue(any("XAUUSD" in ln for ln in mt5_logs["mql5_lines"]))

        # 3B: Fake MT4 mt4_debug.log + MQL4 logs
        mt4_term = os.path.join(mq_root, "HASH_MT4_1")
        os.makedirs(os.path.join(mt4_term, "logs"), exist_ok=True)
        os.makedirs(os.path.join(mt4_term, "MQL4", "Logs"), exist_ok=True)
        with open(os.path.join(self.tmpdir.name, "mt4_debug.log"), "w", encoding="utf-8") as f:
            f.write("[2026-11-12 09:06:45] Order result: {'ok': False, 'error': 'ERR_TRADE_DISABLED (133) XAUUSD'}\n")
        with open(os.path.join(mt4_term, "logs", f"{day_str}.log"), "w", encoding="utf-8") as f:
            f.write("09:06:45 SiriusBridge XAUUSD rejected off quotes\n")

        mt4_logs = signal_watch.read_mt4_journal_logs(
            {},
            int(time.time()),
            ["XAUUSD"],
            metaquotes_root=mq_root,
            workspace_dir=self.tmpdir.name,
        )
        self.assertTrue(any("ERR_TRADE_DISABLED" in ln for ln in mt4_logs["mt4_debug_lines"]))
        self.assertTrue(any("off quotes" in ln for ln in mt4_logs["journal_lines"]))

        # 3C: Fake Spotware cTrader logs
        ct_root = os.path.join(self.tmpdir.name, "Spotware", "cTrader", "abc", "x64")
        os.makedirs(ct_root, exist_ok=True)
        with open(os.path.join(ct_root, "desktop_20261112.log"), "w", encoding="utf-8") as f:
            f.write("09:06:44 [SiriusBridge] Error: POSITION_PRICE_TOO_FAR_FROM_MARKET XAUUSD\n")

        ct_logs = signal_watch.read_ctrader_journal_logs(
            {},
            ["XAUUSD"],
            spotware_root=os.path.join(self.tmpdir.name, "Spotware", "cTrader"),
        )
        self.assertTrue(any("POSITION_PRICE_TOO_FAR_FROM_MARKET" in ln for ln in ct_logs["lines"]))

    def test_4_unparsed_signal_detector_and_parser_missed_report(self):
        self.assertTrue(signal_watch.looks_like_unparsed_signal("GOLD BUY NOW @ 2715\nSL 2705\nTP 2730"))
        self.assertFalse(signal_watch.looks_like_unparsed_signal("Good morning team! Big news later today."))

        res = signal_watch.process_signal_event(
            {
                "provider_id": "11",
                "provider_name": "FX Viper",
                "message_id": 77111,
                "raw_message": "LONG XAU @ 2700 SL 2690 TP 2720",
                "parsed_signal": {},
                "exec_results": [],
                "event_kind": "PARSER_MISSED",
                "force_issue_reason": "Parser returned is_signal=False",
            },
            ai_caller=lambda _: None,  # Verify deterministic fallback works when Gemini offline!
            alert_sender=lambda *a: True,
            skip_journals=True,
        )
        self.assertEqual(res["overall_status"], "FAILED")
        self.assertTrue(res["has_discrepancy"])
        saved = db.get_signal_watch_event(res["watch_id"])
        self.assertEqual(saved["report_status"], "READY")
        self.assertIn("Fallback Mode", saved["ai_report_md"])
        self.assertIn("PARSER_MISSED", saved["ai_summary"] or saved["ai_report_md"])

    def test_5_fastapi_endpoints_provider_filter_and_ui_elements(self):
        from fastapi.testclient import TestClient
        import web_server

        # Seed two different providers into sandbox DB
        db.upsert_signal_watch_event({
            "event_key": "543_1001_NEW_SIGNAL",
            "message_id": 1001,
            "provider_id": "543",
            "provider_name": "Lorax VIP",
            "symbol": "XAUUSD",
            "direction": "BUY",
            "raw_message": "BUY XAUUSD",
            "overall_status": "COPIED",
            "has_discrepancy": False,
            "report_status": "NONE",
            "created_at": 1700000001,
        })
        wid2 = db.upsert_signal_watch_event({
            "event_key": "11_2002_NEW_SIGNAL",
            "message_id": 2002,
            "provider_id": "11",
            "provider_name": "FX Viper",
            "symbol": "EURUSD",
            "direction": "SELL",
            "raw_message": "SELL EURUSD",
            "overall_status": "FAILED",
            "has_discrepancy": True,
            "report_status": "READY",
            "ai_summary": "Symbol EURUSD not found",
            "ai_report_md": "## Report\nFix suffix.",
            "antigravity_prompt": "Fix EURUSD mapping.",
            "created_at": 1700000002,
        })

        with patch.object(web_server, "load_settings", return_value={"543": {"name": "Lorax VIP"}, "11": {"name": "FX Viper"}}):
            client = TestClient(web_server.app)

            # UI contains sidebar tab, view panel, provider filter, and modal
            home = client.get("/")
            self.assertEqual(home.status_code, 200)
            self.assertIn("nav-signalwatch", home.text)
            self.assertIn("Signal Watch", home.text)
            self.assertIn("view-signalwatch", home.text)
            self.assertIn("sw-provider-filter", home.text)
            self.assertIn("sw-report-modal", home.text)

            # Unfiltered API response
            r_all = client.get("/api/signal-watch")
            self.assertEqual(r_all.status_code, 200)
            data_all = r_all.json()
            self.assertEqual(len(data_all["events"]), 2)
            self.assertEqual(data_all["summary"]["issues"], 1)
            self.assertEqual(data_all["summary"]["copied"], 1)

            # Filter by provider_id=11
            r_p11 = client.get("/api/signal-watch?provider_id=11")
            self.assertEqual(r_p11.status_code, 200)
            ev11 = r_p11.json()["events"]
            self.assertEqual(len(ev11), 1)
            self.assertEqual(ev11[0]["provider_id"], "11")

            # Filter by status=ISSUES
            r_iss = client.get("/api/signal-watch?status=ISSUES")
            self.assertEqual(r_iss.status_code, 200)
            self.assertEqual(len(r_iss.json()["events"]), 1)

            # Single event detail endpoint
            r_det = client.get(f"/api/signal-watch/{wid2}")
            self.assertEqual(r_det.status_code, 200)
            self.assertEqual(r_det.json()["event"]["antigravity_prompt"], "Fix EURUSD mapping.")

            # Delete & clear endpoints
            r_del = client.delete(f"/api/signal-watch/{wid2}")
            self.assertTrue(r_del.json()["deleted"])
            r_clr = client.post("/api/signal-watch/clear")
            self.assertEqual(r_clr.status_code, 200)
            self.assertEqual(len(client.get("/api/signal-watch").json()["events"]), 0)

    def test_6_non_blocking_enqueue_completes_in_background(self):
        t0 = time.perf_counter()
        with patch("signal_watch.process_signal_event", return_value={"watch_id": 1}) as mock_proc:
            ok = signal_watch.enqueue_signal_event(
                provider_id="543",
                provider_name="Lorax VIP",
                channel_id="-1002385852838",
                topic_id=8,
                message_id=88801,
                raw_message="BUY GOLD",
                parsed_signal={"is_signal": True, "instrument": "XAUUSD", "direction": "BUY", "sl": 4000},
                exec_results=[{"account_id": "acc_1", "ticket": 1}],
                account_diagnostics={},
            )
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            self.assertTrue(ok)
            self.assertLess(elapsed_ms, 100.0)
            signal_watch._watch_queue.join()
            self.assertEqual(mock_proc.call_count, 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
