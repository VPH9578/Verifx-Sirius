"""
account_manager.py — Central Manager orchestrating multi-account worker processes.
Provides a clean interface for main.py, web_server.py, and signal_listener.py.
"""

import os
import time
import json
from multiprocessing import Process, Queue
from typing import Dict, List, Optional
from account_worker import run_worker_process
from ctrader_worker import run_ctrader_worker_process
from mt4_worker import run_mt4_worker_process

ACCOUNTS_FILE = "accounts_config.json"
PROVIDERS_FILE = "providers_config.json"
ACTIVE_ACCOUNT_FILE = "active_account.json"

DEFAULT_RULE = {
    "risk_mode": "percent",
    "risk_value": 1.0,
    "tp1_percent": 20.0,
    "tp2_percent": 20.0,
    "tp3_percent": 60.0,
    "tp1_close_percent": 20.0,
    "sl_tp1_strategy": "ENTRY",
    "sl_tp2_strategy": "TP1",
    "sl_tp3_strategy": "TP2",
    "sl_breakeven_at": "TP1",
    "spread_offset_entry": True,
    "spread_offset_tp": True,
    "spread_offset_sl": True
}


class AccountManager:
    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = AccountManager()
        return cls._instance

    def __init__(self):
        self.workers = {}  # acc_id -> { "process": Process, "in_q": Queue, "out_q": Queue, "config": dict }
        self.accounts_config = {}
        self.active_account_id = "acc_1"
        self.cached_states = {}
        self.pending_results = {}
        self.late_exec_results = {}
        self.last_dispatch_req_ids = {}
        self.last_dispatch_diagnostics = {}
        self.state_dir = ""
        self.load_accounts()

    def load_active_account_id(self) -> str:
        if os.path.exists(ACTIVE_ACCOUNT_FILE):
            try:
                with open(ACTIVE_ACCOUNT_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    val = data.get("active_account_id")
                    if val:
                        return str(val)
            except Exception:
                pass
        return "acc_1"

    def save_active_account_id(self, acc_id: str):
        try:
            with open(ACTIVE_ACCOUNT_FILE, "w", encoding="utf-8") as f:
                json.dump({"active_account_id": str(acc_id)}, f, indent=4)
        except Exception as e:
            print(f"[AccountManager] Failed to save {ACTIVE_ACCOUNT_FILE}: {e}")

    def load_accounts(self) -> dict:
        if os.path.exists(ACCOUNTS_FILE):
            try:
                with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
                    self.accounts_config = json.load(f)
                    saved_active = self.load_active_account_id()
                    if saved_active in self.accounts_config:
                        self.active_account_id = saved_active
                    elif self.accounts_config and self.active_account_id not in self.accounts_config:
                        self.active_account_id = list(self.accounts_config.keys())[0]
                    # Keep running worker configs synced with latest disk state
                    for acc_id, w in list(self.workers.items()):
                        if acc_id in self.accounts_config:
                            w["config"] = self.accounts_config[acc_id]
                    return self.accounts_config
            except Exception as e:
                print(f"[AccountManager] Failed to load {ACCOUNTS_FILE}: {e}")
        return {}

    def save_accounts(self):
        try:
            with open(ACCOUNTS_FILE, "w", encoding="utf-8") as f:
                json.dump(self.accounts_config, f, indent=4)
            for acc_id, w in list(self.workers.items()):
                if acc_id in self.accounts_config:
                    w["config"] = self.accounts_config[acc_id]
        except Exception as e:
            print(f"[AccountManager] Failed to save {ACCOUNTS_FILE}: {e}")

    def ensure_workers_running(self, skip_primary_mt5: bool = True):
        """Ensures all enabled secondary/cTrader accounts have a live worker process."""
        for acc_id, cfg in list(self.accounts_config.items()):
            platform = str(cfg.get("platform", "mt5")).lower()
            if skip_primary_mt5 and acc_id == "acc_1" and platform == "mt5":
                continue
            if cfg.get("enabled", True):
                existing = self.workers.get(acc_id)
                if not existing or not existing["process"].is_alive():
                    if existing:
                        try:
                            del self.workers[acc_id]
                        except Exception:
                            pass
                    self.start_worker(acc_id, cfg)

    def start_all_workers(self, skip_primary_mt5: bool = False):
        """Starts worker processes for all enabled accounts."""
        self.load_accounts()
        self.ensure_workers_running(skip_primary_mt5=skip_primary_mt5)

    def start_worker(self, acc_id: str, cfg: dict):
        in_q = Queue()
        out_q = Queue()
        platform = str(cfg.get("platform", "mt5")).lower()
        if platform == "ctrader":
            target_fn = run_ctrader_worker_process
        elif platform == "mt4":
            target_fn = run_mt4_worker_process
        else:
            target_fn = run_worker_process
        p = Process(target=target_fn, args=(cfg, in_q, out_q))
        p.daemon = True
        p.start()
        self.workers[acc_id] = {
            "process": p,
            "in_q": in_q,
            "out_q": out_q,
            "config": cfg
        }
        print(f"[AccountManager] Started {platform.upper()} worker process for {cfg.get('name', acc_id)} (PID: {p.pid})")

    def stop_worker(self, acc_id: str):
        if acc_id in self.workers:
            w = self.workers[acc_id]
            try:
                w["in_q"].put({"cmd": "STOP"})
                w["process"].join(timeout=1.0)
            except Exception as e:
                print(f"[AccountManager] Error stopping worker {acc_id}: {e}")
            del self.workers[acc_id]

    def stop_all_workers(self):
        for acc_id in list(self.workers.keys()):
            self.stop_worker(acc_id)

    def add_or_update_account(self, acc_data: dict, skip_primary_mt5: bool = True) -> dict:
        self.load_accounts()
        acc_id = acc_data.get("id")
        platform = str(acc_data.get("platform", "mt5")).lower()

        # Prevent duplicate cTrader or MT4 accounts on the same port if id was not explicitly supplied
        if not acc_id and platform == "ctrader":
            target_port = int(acc_data.get("ctrader_port") or 8888)
            for existing_id, existing_cfg in self.accounts_config.items():
                if str(existing_cfg.get("platform", "")).lower() == "ctrader" and int(existing_cfg.get("ctrader_port") or 8888) == target_port:
                    acc_id = existing_id
                    break
        elif not acc_id and platform == "mt4":
            target_port = int(acc_data.get("mt4_port") or 9001)
            for existing_id, existing_cfg in self.accounts_config.items():
                if str(existing_cfg.get("platform", "")).lower() == "mt4" and int(existing_cfg.get("mt4_port") or 9001) == target_port:
                    acc_id = existing_id
                    break

        if not acc_id:
            acc_id = f"acc_{int(time.time())}"

        existing = self.accounts_config.get(acc_id, {})
        acc_data["id"] = acc_id
        acc_data.setdefault("platform", platform)
        if "providers" not in acc_data:
            acc_data["providers"] = list(existing.get("providers", []))
        if "provider_settings" not in acc_data:
            acc_data["provider_settings"] = dict(existing.get("provider_settings", {}))

        # Stop existing worker if running
        if acc_id in self.workers:
            self.stop_worker(acc_id)

        self.accounts_config[acc_id] = acc_data
        self.save_accounts()

        # Automatically pull and map all broker symbols upon adding/updating account
        try:
            from symbol_mapper import SymbolMapper
            sync_res = SymbolMapper.sync_account_symbols_from_broker(acc_id, acc_data)
            if sync_res and not sync_res.get("error"):
                acc_data["symbols_mapped"] = True
                acc_data["broker_symbols_count"] = sync_res.get("broker_symbols_count", 0)
                acc_data["mapped_aliases_count"] = sync_res.get("mapped_aliases_count", 0)
                self.accounts_config[acc_id] = acc_data
                self.save_accounts()
                print(f"[AccountManager] Successfully mapped symbols for account {acc_id}: {sync_res.get('mapped_aliases_count')} tradeable aliases.")
        except Exception as e:
            print(f"[AccountManager] Symbol mapping during account creation warning: {e}")

        if acc_data.get("enabled", True) and not (skip_primary_mt5 and acc_id == "acc_1" and platform == "mt5"):
            self.start_worker(acc_id, acc_data)

        return acc_data

    def sync_symbols(self, acc_id: str) -> dict:
        self.load_accounts()
        acc = self.accounts_config.get(acc_id)
        if not acc:
            return {"error": f"Account {acc_id} not found"}

        w = self.workers.get(acc_id)
        if w:
            req_id = f"sync_{acc_id}_{int(time.time()*1000)}"
            w["in_q"].put({"cmd": "SYNC_SYMBOLS", "req_id": req_id})
            deadline = time.time() + 6.0
            while time.time() < deadline:
                self._drain_worker_out_q(acc_id)
                if req_id in self.pending_results:
                    msg = self.pending_results.pop(req_id)
                    res = msg.get("result") or {}
                    if res and not res.get("error"):
                        acc["symbols_mapped"] = True
                        acc["broker_symbols_count"] = res.get("broker_symbols_count", 0)
                        acc["mapped_aliases_count"] = res.get("mapped_aliases_count", 0)
                        self.accounts_config[acc_id] = acc
                        self.save_accounts()
                    return res
                time.sleep(0.05)

        from symbol_mapper import SymbolMapper
        res = SymbolMapper.sync_account_symbols_from_broker(acc_id, acc)
        if res and not res.get("error"):
            acc["symbols_mapped"] = True
            acc["broker_symbols_count"] = res.get("broker_symbols_count", 0)
            acc["mapped_aliases_count"] = res.get("mapped_aliases_count", 0)
            self.accounts_config[acc_id] = acc
            self.save_accounts()
        return res

    def get_candles(self, acc_id: str, symbol: str, timeframe: str, from_ts: int, to_ts: int, timeout: float = 4.0) -> Optional[dict]:
        w = self.workers.get(acc_id)
        if not w or not w["process"].is_alive():
            return None
        req_id = f"candles_{acc_id}_{int(time.time()*1000)}"
        w["in_q"].put({
            "cmd": "GET_CANDLES",
            "req_id": req_id,
            "payload": {"symbol": symbol, "timeframe": timeframe, "from_ts": from_ts, "to_ts": to_ts}
        })
        deadline = time.time() + timeout
        while time.time() < deadline:
            self._drain_worker_out_q(acc_id)
            if req_id in self.pending_results:
                msg = self.pending_results.pop(req_id)
                return msg.get("result")
            time.sleep(0.03)
        return None

    def set_active_account(self, acc_id: str) -> bool:
        self.load_accounts()
        if acc_id in self.accounts_config:
            self.active_account_id = acc_id
            self.save_active_account_id(acc_id)
            return True
        return False

    def is_provider_assigned_to_account(self, acc_id: str, provider_id: str) -> bool:
        """Returns True if provider_id is assigned to acc_id."""
        self.load_accounts()
        p_str = str(provider_id)
        acc = self.accounts_config.get(acc_id, {})
        acc_provs = [str(x) for x in (acc.get("providers") or [])]
        if p_str in acc_provs:
            return True

        # Also check provider's own account_id field in providers_config.json as fallback
        if os.path.exists(PROVIDERS_FILE):
            try:
                with open(PROVIDERS_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    p_cfg = data.get(p_str, {})
                    p_acc = p_cfg.get("account_id")
                    if p_acc and str(p_acc) == str(acc_id):
                        return True
            except Exception:
                pass
        return False

    def assign_provider_to_account(self, acc_id: str, provider_id: str, exclusive: bool = False):
        """Assigns provider_id to acc_id (and optionally removes from other accounts if exclusive=True)."""
        self.load_accounts()
        p_str = str(provider_id)
        changed = False
        for a_id, a_cfg in self.accounts_config.items():
            provs = [str(x) for x in (a_cfg.get("providers") or [])]
            if a_id == acc_id:
                if p_str not in provs:
                    provs.append(p_str)
                    a_cfg["providers"] = provs
                    changed = True
            elif exclusive and p_str in provs:
                a_cfg["providers"] = [x for x in provs if x != p_str]
                changed = True
        if changed:
            self.save_accounts()

    def copy_provider_to_account(self, source_acc_id: str, provider_id: str, target_acc_id: str, settings_override: dict = None) -> dict:
        """Copies provider_id and its settings from source_acc_id to target_acc_id."""
        import copy
        self.load_accounts()
        p_str = str(provider_id).strip()
        src_id = str(source_acc_id or self.active_account_id or "acc_1").strip()
        tgt_id = str(target_acc_id).strip()

        if tgt_id not in self.accounts_config:
            raise ValueError(f"Target account '{tgt_id}' not found in accounts config.")

        # 1. Fetch source settings for this provider
        source_settings = self.get_account_settings_for_provider(src_id, p_str)
        if settings_override and isinstance(settings_override, dict):
            cleaned_override = {k: v for k, v in settings_override.items() if v is not None}
            final_settings = {**source_settings, **cleaned_override}
        else:
            final_settings = copy.deepcopy(source_settings)

        # 2. Assign provider to target account if not already in its list
        target_acc = self.accounts_config[tgt_id]
        tgt_provs = [str(x) for x in (target_acc.get("providers") or [])]
        if p_str not in tgt_provs:
            tgt_provs.append(p_str)
            target_acc["providers"] = tgt_provs

        # 3. Copy provider settings override into target account
        final_settings["account_id"] = tgt_id
        tgt_settings = target_acc.setdefault("provider_settings", {})
        tgt_settings[p_str] = copy.deepcopy(final_settings)

        # 4. Save accounts configuration
        self.save_accounts()

        # 5. Ensure that the target account starts with clean, isolated trade history & stats for this provider
        try:
            import db
            conn = db.get_db()
            conn.execute("DELETE FROM trades WHERE provider_id = ? AND account_id = ?", (p_str, tgt_id))
            conn.execute("DELETE FROM trade_signals WHERE provider_id = ? AND account_id = ?", (p_str, tgt_id))
            try:
                conn.execute("DELETE FROM trade_analytics WHERE provider_id = ? AND account_id = ?", (p_str, tgt_id))
            except Exception:
                pass
            conn.commit()
            conn.close()
        except Exception as _e:
            print(f"[AccountManager] Clean stats error for target {tgt_id}: {_e}")

        def _format_acc_display(a_id: str, a_cfg: dict) -> str:
            plat = str(a_cfg.get("platform", "mt5")).lower()
            if plat == "ctrader":
                login = a_cfg.get("ctrader_account_id") or a_cfg.get("login") or a_id
            elif plat == "mt4":
                login = a_cfg.get("mt4_login") or a_cfg.get("login") or a_id
            else:
                login = a_cfg.get("mt5_login") or a_cfg.get("login") or a_id
            name = a_cfg.get("name", "")
            extra = ""
            if name:
                import re
                clean = re.sub(r'PU\s*Prime|MT5|MT4|cTrader|Terminal|[()]', '', name, flags=re.I).strip()
                if clean and len(clean) > 1:
                    extra = f" ({clean})"
            return f"Account #{login}{extra}"

        target_name = _format_acc_display(tgt_id, target_acc)
        source_name = _format_acc_display(src_id, self.accounts_config.get(src_id, {}))
        provider_name = source_settings.get("name") or f"Provider {p_str}"

        return {
            "status": "success",
            "message": f"Provider '{provider_name}' successfully copied from {source_name} to {target_name}.",
            "source_account_id": src_id,
            "target_account_id": tgt_id,
            "target_account_name": target_name,
            "provider_id": p_str,
            "provider_name": provider_name,
            "settings": source_settings
        }

    def get_account_providers(self, acc_id: str = None) -> list:
        """Returns list of provider ID strings assigned to acc_id (defaults to active_account_id)."""
        self.load_accounts()
        target_id = acc_id or self.active_account_id or "acc_1"
        acc = self.accounts_config.get(target_id, {})
        provs = [str(x) for x in (acc.get("providers") or [])]
        # Also include any provider in providers_config.json explicitly tagged with account_id == target_id
        if os.path.exists(PROVIDERS_FILE):
            try:
                with open(PROVIDERS_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    for pid, pcfg in data.items():
                        if str(pcfg.get("account_id", "")) == str(target_id) and str(pid) not in provs:
                            provs.append(str(pid))
            except Exception:
                pass
        return provs

    def get_accounts_list(self) -> list:
        self.load_accounts()
        self.ensure_workers_running(skip_primary_mt5=True)
        out = []
        for acc_id, cfg in self.accounts_config.items():
            platform = str(cfg.get("platform", "mt5")).lower()
            if platform == "ctrader":
                port_val = int(cfg.get("ctrader_port") or 8888)
                login_val = cfg.get("ctrader_account_id") or f"Port {port_val}"
                server_val = f"cBot Bridge (127.0.0.1:{port_val})"
            elif platform == "mt4":
                port_val = int(cfg.get("mt4_port") or 9001)
                login_val = cfg.get("mt4_login") or f"Port {port_val}"
                server_val = f"MT4 EA Bridge (127.0.0.1:{port_val})"
            else:
                port_val = 0
                login_val = cfg.get("mt5_login")
                server_val = cfg.get("mt5_server", "")

            connected = False
            balance = 0.0
            equity = 0.0
            open_positions_count = 0

            if acc_id == "acc_1" and platform == "mt5":
                try:
                    import mt5_executor
                    connected = mt5_executor.is_connected()
                    if connected:
                        info = mt5_executor.get_account_info()
                        if info:
                            balance = float(getattr(info, "balance", 0.0))
                            equity = float(getattr(info, "equity", balance))
                            login_val = getattr(info, "login", login_val)
                        pos = mt5_executor.get_open_positions()
                        open_positions_count = len(pos) if pos else 0
                except Exception:
                    pass
            else:
                w_state = self.get_active_state(acc_id, timeout=0.35)
                connected = bool(w_state.get("connected", False))
                balance = float(w_state.get("balance", 0.0))
                equity = float(w_state.get("equity", balance))
                if w_state.get("login"):
                    login_val = w_state.get("login")
                open_positions_count = len(w_state.get("open_positions") or [])

            provs = self.get_account_providers(acc_id)
            out.append({
                "id": acc_id,
                "name": cfg.get("name") or str(login_val or acc_id),
                "platform": platform,
                "login": login_val,
                "server": server_val,
                "enabled": bool(cfg.get("enabled", True)),
                "connected": connected,
                "balance": balance,
                "equity": equity,
                "open_positions_count": open_positions_count,
                "is_active": (acc_id == self.active_account_id),
                "ctrader_env": cfg.get("ctrader_env", "demo"),
                "ctrader_account_id": cfg.get("ctrader_account_id", ""),
                "ctrader_host": cfg.get("ctrader_host", "127.0.0.1"),
                "ctrader_port": port_val if platform == "ctrader" else 0,
                "mt4_login": cfg.get("mt4_login", ""),
                "mt4_port": port_val if platform == "mt4" else int(cfg.get("mt4_port") or 9001),
                "mt4_host": cfg.get("mt4_host", "127.0.0.1"),
                "commission_per_lot": float(cfg.get("commission_per_lot", 6.0 if platform == "ctrader" else 0.0)),
                "mt5_login": cfg.get("mt5_login", ""),
                "mt5_server": cfg.get("mt5_server", ""),
                "mt5_path": cfg.get("mt5_path", ""),
                "providers": provs,
                "providers_count": len(provs),
                "is_paused": bool(cfg.get("is_paused", False)),
                "symbols_mapped": bool(cfg.get("symbols_mapped", False)),
                "broker_symbols_count": int(cfg.get("broker_symbols_count", 0)),
                "mapped_aliases_count": int(cfg.get("mapped_aliases_count", 0)),
            })
        return out

    def toggle_account_pause(self, acc_id: str) -> bool:
        """Toggles pause state specifically for acc_id and persists to accounts_config.json."""
        self.load_accounts()
        if acc_id not in self.accounts_config:
            acc_id = self.active_account_id or "acc_1"
        cfg = self.accounts_config.setdefault(acc_id, {})
        new_state = not bool(cfg.get("is_paused", False))
        cfg["is_paused"] = new_state
        self.save_accounts()
        if acc_id in self.workers:
            self.workers[acc_id]["config"]["is_paused"] = new_state
        print(f"[AccountManager] Account {acc_id} pause toggled -> is_paused={new_state}")
        return new_state

    def is_account_paused(self, acc_id: str) -> bool:
        """Returns True if the specific account is paused."""
        self.load_accounts()
        cfg = self.accounts_config.get(acc_id, {})
        return bool(cfg.get("is_paused", False))

    def are_all_accounts_paused(self) -> bool:
        """Returns True if every enabled configured account is paused."""
        self.load_accounts()
        if not self.accounts_config:
            return False
        enabled = [cfg for cfg in self.accounts_config.values() if cfg.get("enabled", True)]
        if not enabled:
            return False
        return all(bool(cfg.get("is_paused", False)) for cfg in enabled)

    def delete_account(self, acc_id: str) -> bool:
        if acc_id in self.accounts_config:
            self.stop_worker(acc_id)
            del self.accounts_config[acc_id]
            self.save_accounts()
            if self.active_account_id == acc_id:
                keys = list(self.accounts_config.keys())
                new_active = keys[0] if keys else "acc_1"
                self.active_account_id = new_active
                self.save_active_account_id(new_active)
            return True
        return False

    def get_account_settings_for_provider(self, acc_id: str, provider_id: str) -> dict:
        self.load_accounts()
        acc = self.accounts_config.get(acc_id, {})
        overrides = acc.get("provider_settings", {}).get(str(provider_id), {})

        # Read base provider defaults if present
        base_prov = {}
        if os.path.exists(PROVIDERS_FILE):
            try:
                with open(PROVIDERS_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    base_prov = data.get(str(provider_id), {})
            except Exception:
                pass

        return {**DEFAULT_RULE, **base_prov, **overrides}

    def set_account_settings_for_provider(self, acc_id: str, provider_id: str, settings: dict) -> bool:
        """Saves account-specific trade settings override for a given provider."""
        self.load_accounts()
        p_str = str(provider_id).strip()
        acc = self.accounts_config.get(acc_id)
        if not acc:
            return False

        # Guard against cross-account contamination:
        # Only add provider to this account's providers list if it is NOT assigned to another account
        provs = [str(x) for x in (acc.get("providers") or [])]
        if p_str not in provs:
            assigned_elsewhere = False
            for other_id, other_cfg in self.accounts_config.items():
                if other_id != acc_id and p_str in [str(x) for x in (other_cfg.get("providers") or [])]:
                    assigned_elsewhere = True
                    break
            if not assigned_elsewhere and os.path.exists(PROVIDERS_FILE):
                try:
                    with open(PROVIDERS_FILE, "r", encoding="utf-8") as f:
                        base_cfg = json.load(f)
                        p_acc = base_cfg.get(p_str, {}).get("account_id")
                        if p_acc and str(p_acc) != str(acc_id) and str(p_acc) in self.accounts_config:
                            assigned_elsewhere = True
                except Exception:
                    pass
            if not assigned_elsewhere:
                provs.append(p_str)
                acc["providers"] = provs

        tgt_settings = acc.setdefault("provider_settings", {})
        existing = tgt_settings.get(p_str, {})
        # Only copy valid non-None fields
        cleaned = {k: v for k, v in settings.items() if v is not None}
        tgt_settings[p_str] = {**existing, **cleaned}
        self.save_accounts()
        return True

    def _drain_worker_out_q(self, acc_id: str):
        """Drains out_q for acc_id into cached_states and pending_results without losing messages."""
        w = self.workers.get(acc_id)
        if not w:
            return
        out_q = w["out_q"]
        while not out_q.empty():
            try:
                msg = out_q.get_nowait()
            except Exception:
                break
            if not isinstance(msg, dict):
                continue
            if "state" in msg and msg["state"]:
                self.cached_states[acc_id] = msg["state"]
            if "result" in msg or "state" in msg:
                r_id = str(msg.get("req_id") or "")
                if r_id and not r_id.startswith("bcast_"):
                    self.pending_results[r_id] = msg
                    if "result" in msg or "diag" in msg:
                        self.late_exec_results[r_id] = msg
                        if len(self.late_exec_results) > 120:
                            oldest_l = next(iter(self.late_exec_results))
                            self.late_exec_results.pop(oldest_l, None)
                    if len(self.pending_results) > 150:
                        oldest_key = next(iter(self.pending_results))
                        self.pending_results.pop(oldest_key, None)

    def get_expected_accounts_for_provider(self, provider_id: str) -> list:
        """Returns metadata for every enabled account assigned to provider_id."""
        self.load_accounts()
        p_str = str(provider_id)
        out = []
        for acc_id, cfg in self.accounts_config.items():
            if not cfg.get("enabled", True):
                continue
            if not self.is_provider_assigned_to_account(acc_id, p_str):
                continue
            settings = self.get_account_settings_for_provider(acc_id, p_str)
            platform = str(cfg.get("platform", "mt5")).lower()
            login = cfg.get("ctrader_account_id") if platform == "ctrader" else (cfg.get("mt4_login") if platform == "mt4" else cfg.get("mt5_login"))
            out.append({
                "account_id": acc_id,
                "name": cfg.get("name") or acc_id,
                "platform": platform,
                "login": login,
                "is_paused": bool(cfg.get("is_paused", False)),
                "trading_enabled": bool(settings.get("trading_enabled", True)),
                "settings": settings,
            })
        return out

    def dispatch_signal(self, provider_id: str, signal: dict, message_id: int = None) -> list:
        self.load_accounts()
        self.ensure_workers_running(skip_primary_mt5=True)
        dispatched_accounts = []
        self.last_dispatch_req_ids = {}
        self.last_dispatch_diagnostics = {}
        for acc_id, worker_meta in list(self.workers.items()):
            cfg = self.accounts_config.get(acc_id, worker_meta["config"])
            if not cfg.get("enabled", True):
                continue

            # Strict per-account provider check
            if not self.is_provider_assigned_to_account(acc_id, provider_id):
                continue

            # Per-account pause check: skip if this specific account is paused
            if cfg.get("is_paused", False):
                print(f"[PAUSED] Account {acc_id} ({cfg.get('name', acc_id)}) is PAUSED. Skipping signal execution.")
                self.last_dispatch_diagnostics[acc_id] = {
                    "account_id": acc_id,
                    "status": "PAUSED",
                    "reason": f"Account {acc_id} ({cfg.get('name', acc_id)}) is paused.",
                    "extra": {},
                    "timestamp": time.time(),
                }
                continue

            settings = self.get_account_settings_for_provider(acc_id, provider_id)
            if not settings.get("trading_enabled", True):
                self.last_dispatch_diagnostics[acc_id] = {
                    "account_id": acc_id,
                    "status": "TRADING_DISABLED",
                    "reason": f"Trading disabled for Provider {provider_id} on {acc_id}.",
                    "extra": {},
                    "timestamp": time.time(),
                }
                continue
            req_id = f"{acc_id}_{message_id}_{int(time.time()*1000)}"
            req = {
                "cmd": "EXECUTE_SIGNAL",
                "req_id": req_id,
                "payload": {
                    "signal": signal,
                    "provider_id": provider_id,
                    "settings": settings,
                    "msg_id": message_id
                }
            }
            worker_meta["in_q"].put(req)
            self.last_dispatch_req_ids[acc_id] = req_id
            dispatched_accounts.append(acc_id)

        return dispatched_accounts

    def dispatch_signal_and_collect(self, provider_id: str, signal: dict, message_id: int = None, timeout: float = 3.5) -> list:
        """Dispatches a signal to all active worker processes (e.g. cTrader accounts) and collects placed orders."""
        dispatched = self.dispatch_signal(provider_id, signal, message_id)
        if not dispatched:
            return []
        expected_reqs = dict(self.last_dispatch_req_ids)
        collected = []
        deadline = time.time() + timeout
        pending = set(dispatched)
        while pending and time.time() < deadline:
            for acc_id in list(pending):
                w = self.workers.get(acc_id)
                if not w:
                    pending.discard(acc_id)
                    continue
                self._drain_worker_out_q(acc_id)
                exp_id = expected_reqs.get(acc_id)
                if exp_id and exp_id in self.pending_results:
                    msg = self.pending_results.pop(exp_id)
                    res_list = msg.get("result") or []
                    if isinstance(res_list, list):
                        collected.extend(res_list)
                    diag = msg.get("diag")
                    if isinstance(diag, dict) and diag:
                        self.last_dispatch_diagnostics[acc_id] = diag
                    elif isinstance(res_list, list) and res_list:
                        self.last_dispatch_diagnostics[acc_id] = {
                            "account_id": acc_id,
                            "status": "COPIED",
                            "reason": f"Placed {len(res_list)} order(s).",
                            "extra": {"tickets": [r.get("ticket") for r in res_list if isinstance(r, dict)]},
                            "timestamp": time.time(),
                        }
                    pending.discard(acc_id)
            if pending:
                time.sleep(0.02)
        for timed_out_acc in pending:
            if timed_out_acc not in self.last_dispatch_diagnostics:
                self.last_dispatch_diagnostics[timed_out_acc] = {
                    "account_id": timed_out_acc,
                    "status": "TIMEOUT",
                    "reason": f"Worker {timed_out_acc} did not reply within {timeout}s timeout.",
                    "extra": {"req_id": expected_reqs.get(timed_out_acc, "")},
                    "timestamp": time.time(),
                }
        return collected

    def _get_state_file_path(self, acc_id: str) -> str:
        fname = "trade_state.json" if acc_id == "acc_1" else f"trade_state_{acc_id}.json"
        if self.state_dir:
            return os.path.join(self.state_dir, fname)
        if os.path.exists(fname):
            return fname
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), fname)

    def read_account_tickets_for_message(self, acc_id: str, message_id: int) -> list:
        """Reads message_to_tickets[str(message_id)] from the account's trade_state JSON."""
        if not message_id:
            return []
        fpath = self._get_state_file_path(acc_id)
        if not os.path.exists(fpath):
            return []
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                data = json.load(f)
            m2t = data.get("message_to_tickets") or {}
            tkts = m2t.get(str(message_id))
            if tkts is None and int(message_id) in m2t:
                tkts = m2t.get(int(message_id))
            return [int(t) for t in (tkts or []) if t]
        except Exception:
            return []

    def recheck_late_dispatch(self, acc_id: str, req_id: str = "", message_id: int = 0) -> Optional[dict]:
        """
        Checks if a timed-out dispatch actually completed late via:
          1. Late EXECUTE_SIGNAL worker reply queued in worker out_q / late_exec_results.
          2. Account trade_state JSON (message_to_tickets[message_id]).
        Returns dict with {confirmed: bool, source, status, tickets, reason} or None if still pending.
        """
        self._drain_worker_out_q(acc_id)

        msg = None
        if req_id:
            msg = self.late_exec_results.pop(req_id, None) or self.pending_results.pop(req_id, None)
        if isinstance(msg, dict):
            res_list = msg.get("result") or []
            diag = msg.get("diag") or {}
            tkts = [int(r["ticket"]) for r in res_list if isinstance(r, dict) and r.get("ticket")]
            d_status = str(diag.get("status") or "")
            if tkts or d_status in ("COPIED", "ALREADY_EXECUTED"):
                return {
                    "confirmed": True,
                    "source": "late_worker_reply",
                    "status": "ALREADY_EXECUTED" if (d_status == "ALREADY_EXECUTED" and not tkts) else "COPIED",
                    "tickets": tkts,
                    "reason": diag.get("reason") or f"Late worker reply confirmed {len(tkts)} order(s): {tkts}",
                }
            if d_status and d_status not in ("TIMEOUT", "NOT_REACHED", ""):
                return {
                    "confirmed": False,
                    "source": "late_worker_reply",
                    "status": d_status,
                    "tickets": [],
                    "reason": diag.get("reason") or f"Late worker reply reported {d_status}.",
                }

        tkts = self.read_account_tickets_for_message(acc_id, message_id)
        if tkts:
            return {
                "confirmed": True,
                "source": "trade_state",
                "status": "COPIED",
                "tickets": tkts,
                "reason": f"Confirmed late in trade_state ({len(tkts)} ticket(s): {tkts}).",
            }
        return None

    def send_broadcast_command(self, cmd: str, payload: dict = None):
        """Broadcasts CLOSE_ALL, MODIFY_SL, CANCEL_TRADE across matching running account workers."""
        self.load_accounts()
        self.ensure_workers_running(skip_primary_mt5=True)
        prov_id = (payload or {}).get("provider_id")
        # Safety Guard: Never broadcast a blind CLOSE_ALL without a specific provider_id
        if cmd == "CLOSE_ALL" and not prov_id:
            return
        for acc_id, worker_meta in list(self.workers.items()):
            if prov_id is not None and not self.is_provider_assigned_to_account(acc_id, prov_id):
                continue
            worker_meta["in_q"].put({
                "cmd": cmd,
                "req_id": f"bcast_{acc_id}_{cmd}_{int(time.time()*1000)}",
                "payload": payload or {}
            })

    def get_active_state(self, account_id: str = None, timeout: float = 0.5) -> dict:
        target_id = account_id or self.active_account_id
        if not target_id or target_id not in self.workers:
            # Fallback to cached or empty
            return self.cached_states.get(target_id, {
                "account_id": target_id,
                "connected": False,
                "balance": 0.0,
                "equity": 0.0,
                "open_positions": []
            })

        worker_meta = self.workers[target_id]
        in_q = worker_meta["in_q"]

        req_id = f"state_{int(time.time()*1000)}"
        in_q.put({"cmd": "GET_STATE", "req_id": req_id})

        start = time.time()
        while time.time() - start < timeout:
            self._drain_worker_out_q(target_id)
            if target_id in self.cached_states and self.cached_states[target_id]:
                return self.cached_states[target_id]
            time.sleep(0.01)

        cached = self.cached_states.get(target_id)
        if cached:
            p = worker_meta.get("process")
            if p and p.is_alive():
                return cached

        return self.cached_states.get(target_id, {
            "account_id": target_id,
            "connected": False,
            "balance": 0.0,
            "equity": 0.0,
            "open_positions": []
        })

    def send_worker_command_sync(self, account_id: str, cmd: str, payload: dict = None, timeout: float = 6.0) -> dict:
        """Sends a command to a specific running account worker and waits for response."""
        if account_id not in self.workers:
            return {"ok": False, "error": f"Worker {account_id} not running"}
        worker_meta = self.workers[account_id]
        in_q = worker_meta["in_q"]
        req_id = f"{account_id}_{cmd}_{int(time.time()*1000)}"
        in_q.put({"cmd": cmd, "req_id": req_id, "payload": payload or {}})

        start = time.time()
        while time.time() - start < timeout:
            self._drain_worker_out_q(account_id)
            if req_id in self.pending_results:
                msg = self.pending_results.pop(req_id)
                return {"ok": True, "result": msg.get("result")}
            time.sleep(0.02)
        return {"ok": False, "error": f"Timeout waiting for worker {account_id} to respond to {cmd}"}

    def close_and_cancel_account_trades(self, account_id: str, provider_ids: list = None) -> dict:
        """Closes all open positions and cancels all pending orders for an account."""
        self.load_accounts()
        acc_cfg = self.accounts_config.get(account_id, {})
        platform = str(acc_cfg.get("platform", "mt5")).lower()

        if account_id == "acc_1" and platform == "mt5":
            try:
                import mt5_executor
                return mt5_executor.close_all_positions_and_orders(provider_ids=provider_ids, broadcast=False)
            except Exception as e:
                return {"closed_positions_count": 0, "cancelled_orders_count": 0, "error": str(e)}

        # Worker accounts (cTrader or secondary MT5)
        self.ensure_workers_running(skip_primary_mt5=True)
        res = self.send_worker_command_sync(account_id, "CLOSE_AND_CANCEL_ALL", {"provider_ids": provider_ids}, timeout=8.0)
        if res.get("ok"):
            r = res.get("result") or {}
            if isinstance(r, dict):
                return r
            elif isinstance(r, list):
                return {"closed_positions_count": len(r), "cancelled_orders_count": 0}
        return {"closed_positions_count": 0, "cancelled_orders_count": 0, "error": res.get("error")}
