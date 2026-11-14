import psutil
import time
import os

sirius_procs = []
for p in psutil.process_iter(['pid', 'name', 'cmdline']):
    try:
        cmd = p.info.get('cmdline') or []
        cmd_str = ' '.join(cmd)
        if 'python' in p.info['name'].lower() and ('main.py' in cmd_str or 'multiprocessing.spawn' in cmd_str):
            sirius_procs.append(p)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass

print(f"Found {len(sirius_procs)} Sirius python processes.")
for p in sirius_procs:
    try:
        p.terminate()
    except Exception:
        pass

gone, alive = psutil.wait_procs(sirius_procs, timeout=3)
for p in alive:
    try:
        p.kill()
    except Exception:
        pass

time.sleep(1)
lock_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "sirius_copier.lock")
if os.path.exists(lock_file):
    try:
        os.remove(lock_file)
        print("Removed lock file.")
    except Exception as e:
        print("Lock file remove error:", e)
