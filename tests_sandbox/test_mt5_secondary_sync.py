"""
Sandbox verification suite for secondary MT5 account (acc_1794159206 / 27794357)
deal synchronization, database stats, and dashboard endpoints.
"""
import os
import sys
import json
import shutil
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import db
import provider_analytics as pa
from account_worker import MT5AccountWorker
from multiprocessing import Queue

def run_sandbox_tests():
    print("=" * 60)
    print("RUNNING SANDBOX TESTS FOR MT5 ACCOUNT 27794357 (acc_1794159206)")
    print("=" * 60)

    # 1. Create an isolated sandbox copy of stats.db so we test cleanly first
    orig_db_file = db.DB_FILE
    tmp_dir = tempfile.mkdtemp(prefix="sirius_sandbox_")
    sandbox_db = os.path.join(tmp_dir, "stats_sandbox.db")
    shutil.copy2(orig_db_file, sandbox_db)
    db.DB_FILE = sandbox_db
    print(f"[1] Created isolated sandbox DB at: {sandbox_db}")

    try:
        with open("accounts_config.json", "r", encoding="utf-8") as f:
            accs_cfg = json.load(f)
        acc_cfg = accs_cfg["acc_1794159206"]

        # 2. Instantiate MT5AccountWorker in sandbox
        in_q = Queue()
        out_q = Queue()
        worker = MT5AccountWorker(acc_cfg, in_q, out_q)
        worker.state_file = os.path.join(tmp_dir, "trade_state_sandbox.json")

        assert hasattr(worker, "account_config"), "Missing self.account_config"
        assert hasattr(worker, "trade_state"), "Missing self.trade_state"
        print("[2] Worker attributes verified (account_config, trade_state).")

        # 3. Connect to terminal 27794357 and run sync_closed_trades_to_db()
        conn_ok = worker.connect()
        assert conn_ok, "Failed to connect to MT5 terminal 27794357"
        worker.sync_closed_trades_to_db()

        # 4. Verify trades and trade_signals in sandbox DB for acc_1794159206
        conn = db.get_db()
        trades_rows = conn.execute(
            "SELECT * FROM trades WHERE account_id = 'acc_1794159206' ORDER BY close_time ASC"
        ).fetchall()
        signals_rows = conn.execute(
            "SELECT * FROM trade_signals WHERE account_id = 'acc_1794159206' ORDER BY created_at ASC"
        ).fetchall()
        analytics_rows = conn.execute(
            "SELECT * FROM trade_analytics WHERE account_id = 'acc_1794159206' ORDER BY close_time ASC"
        ).fetchall()
        conn.close()

        print(f"[3] Synced rows in sandbox DB for acc_1794159206:")
        print(f"    - trades count:          {len(trades_rows)}")
        print(f"    - trade_signals count:   {len(signals_rows)}")
        print(f"    - trade_analytics count: {len(analytics_rows)}")

        assert len(trades_rows) == 24, f"Expected 24 closed deal legs, got {len(trades_rows)}"
        assert len(signals_rows) == 7, f"Expected 7 signals, got {len(signals_rows)}"
        assert len(analytics_rows) == 24, f"Expected 24 analytics rows, got {len(analytics_rows)}"

        # 5. Verify db.get_all_providers_stats for acc_1794159206
        p_stats = db.get_all_providers_stats(["543", "239", "413"], account_id="acc_1794159206")
        print("[4] Provider stats from db.get_all_providers_stats:")
        for pid, st in p_stats.items():
            print(f"    Provider {pid}: {st}")

        assert p_stats["543"]["trades_count"] == 5, f"Expected 5 signals for 543, got {p_stats['543']['trades_count']}"
        assert p_stats["543"]["deal_count"] == 18, f"Expected 18 deals for 543, got {p_stats['543']['deal_count']}"
        assert p_stats["413"]["trades_count"] == 2, f"Expected 2 signals for 413, got {p_stats['413']['trades_count']}"
        assert p_stats["413"]["deal_count"] == 6, f"Expected 6 deals for 413, got {p_stats['413']['deal_count']}"

        # 6. Verify db.get_consolidated_history for acc_1794159206
        hist = db.get_consolidated_history(20, provider_ids=["543", "239", "413"], account_id="acc_1794159206")
        print(f"[5] Consolidated trade history count: {len(hist)}")
        for h in hist:
            print(f"    - Signal {h['signal_id']}: P={h['provider_id']} {h['symbol']} {h['trade_type']} Lots={h['lots']} Profit=${h['profit']} Status={h['status']} SubTrades={h['trades_count']}")
        assert len(hist) == 7, f"Expected 7 consolidated history rows, got {len(hist)}"

        # 7. Verify provider_analytics.compute_provider_statistics for Provider 543 and 413
        pa_543 = pa.compute_provider_statistics("543", base_balance=990.0, account_id="acc_1794159206")
        pa_413 = pa.compute_provider_statistics("413", base_balance=990.0, account_id="acc_1794159206")
        print(f"[6] Deep analytics cards for Provider 543: total_orders={pa_543['cards']['total_orders']}, profitability={pa_543['cards']['profitability']}%, profit={pa_543['periods'][-1]['profit']}")
        print(f"    Deep analytics cards for Provider 413: total_orders={pa_413['cards']['total_orders']}, profitability={pa_413['cards']['profitability']}%, profit={pa_413['periods'][-1]['profit']}")
        assert pa_543["cards"]["total_orders"] == 18
        assert pa_413["cards"]["total_orders"] == 6

        # 8. Verify cross-account signal_id collision protection
        db.save_or_update_signal(
            signal_id="P543_T1",
            provider_id="543",
            symbol="XAUUSD",
            trade_type="BUY",
            lots=0.03,
            tp1_hit=1, tp2_hit=0, tp3_hit=0, sl_hit=0,
            profit=15.0,
            status="CLOSED",
            created_at=1794300000,
            closed_at=1794300500,
            account_id="acc_1"
        )
        db.save_or_update_signal(
            signal_id="P543_T1",
            provider_id="543",
            symbol="XAUUSD.sc",
            trade_type="BUY",
            lots=0.03,
            tp1_hit=1, tp2_hit=0, tp3_hit=0, sl_hit=0,
            profit=5.0,
            status="CLOSED",
            created_at=1794300000,
            closed_at=1794300500,
            account_id="acc_1794159206"
        )
        conn = db.get_db()
        r1 = conn.execute("SELECT account_id, profit FROM trade_signals WHERE signal_id = 'P543_T1'").fetchone()
        r2 = conn.execute("SELECT account_id, profit FROM trade_signals WHERE signal_id = 'acc_1794159206_P543_T1'").fetchone()
        conn.close()
        assert r1["account_id"] == "acc_1" and float(r1["profit"]) == 15.0
        assert r2["account_id"] == "acc_1794159206" and float(r2["profit"]) == 5.0
        print("[7] Cross-account signal_id collision protection verified!")

        print("\nALL SANDBOX TESTS PASSED SUCCESSFULLY!")
    finally:
        db.DB_FILE = orig_db_file
        shutil.rmtree(tmp_dir, ignore_errors=True)

if __name__ == "__main__":
    run_sandbox_tests()
