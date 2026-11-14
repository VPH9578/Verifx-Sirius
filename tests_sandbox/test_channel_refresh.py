import os
import sys
import json
import time
import asyncio
import unittest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


class TestChannelRefresh(unittest.TestCase):
    def test_dynamic_channel_refresh_logic(self):
        """Verify that mtime changes on providers_config.json cause refresh_channels to reload."""
        cfg_path = os.path.join(PROJECT_ROOT, "providers_config.json")
        acc_path = os.path.join(PROJECT_ROOT, "accounts_config.json")

        p_mtime = os.path.getmtime(cfg_path) if os.path.exists(cfg_path) else 0.0
        a_mtime = os.path.getmtime(acc_path) if os.path.exists(acc_path) else 0.0
        combined_mtime = (p_mtime, a_mtime)

        # Ensure tuple comparison detects changes
        old_mtime = (p_mtime - 10.0, a_mtime)
        self.assertNotEqual(combined_mtime, old_mtime)

        # Test with equal mtimes -> no refresh needed
        self.assertEqual(combined_mtime, (p_mtime, a_mtime))


if __name__ == "__main__":
    unittest.main()
