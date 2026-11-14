# Sirius Copier - Architecture & Project Guidelines

## CORE MANDATE: EXTREME RAM EFFICIENCY

This project is built with a strict requirement for **minimal RAM utilization**. To ensure the lowest possible latency and prevent system crashes (especially when running on budget VPS instances), every component must be optimized for memory efficiency.

### Key Optimization Directives:

1. **Lightweight Web UI Serving:**
   - The dashboard is decoupled from heavy rendering frameworks. It uses a lightweight `FastAPI` instance.
   - The UI is accessed via **Windows Edge** (automatically launched), which is explicitly chosen for its lower baseline RAM footprint compared to Chrome or Electron wrappers.

2. **Backend Memory Management:**
   - **Data Structures:** Use generators and iterators instead of loading large lists into memory (e.g., when processing historical trades).
   - **Database/Logs:** Do not hold unbounded trade logs in RAM. Rotate and paginate logs. The `trade_logger.py` and `signal_listener.py` must flush old messages and trades to disk.
   - **Garbage Collection:** Avoid cyclic references to ensure the Python garbage collector can rapidly free memory.

3. **Concurrency:**
   - Use `asyncio` wherever possible for I/O bound tasks (like Telegram listening) to avoid the overhead of heavy OS threads.
   - Daemon threads are used sparingly and only when blocking synchronous libraries (like `MetaTrader5` or `uvicorn` runner) require them.

4. **MetaTrader 5 Integration:**
   - Poll MT5 only when necessary or limit the frequency of deep account history pulls.
   - Use targeted ticket queries instead of pulling the entire history for every loop.

5. **Backtester Non-Interference Mandate:**
   - The Backtester module (`backtester/`) is strictly isolated from Sirius Copier's live trading pipeline (`mt5_executor.py`, `signal_listener.py`, `db.py`, `main.py`).
   - It uses its own SQLite database (`backtester/backtest.db`), performs 100% read-only historical queries on MT5 and Telegram, and only integrates with the main application via the FastAPI router (`/api/backtest/*`) and the `Backtester` sidebar tab in `web/templates/index.html`.

*Note for Developers: Before adding any new feature, library, or data structure, evaluate its RAM impact. If it causes memory bloating, it must be rewritten.*

