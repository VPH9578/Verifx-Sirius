# 🚀 Sirius Telegram Signal Copier (Sirius TSC)

> **High-Performance Multi-Platform Signal Copier & Execution Engine**  
> Connects Telegram signal channels to **MetaTrader 5 (MT5)**, **MetaTrader 4 (MT4)**, and **cTrader (Spotware)** with real-time AI signal parsing, intelligent trade management, and multi-account dispatch.

---

## 🌟 Key Highlights & Architecture

Sirius TSC is a full-featured, institutional-grade automated signal copy trading system built in Python for Windows environments. It bridges raw Telegram trading calls directly to your execution brokers with sub-second latency and extreme memory efficiency.

### 1. Multi-Platform Execution
* **MetaTrader 5 (MT5):** Native high-speed execution using the official MetaTrader5 Python package.
* **cTrader (Spotware):** Direct local low-latency TCP bridge via custom cTrader Open API / cBot driver ([cbot/SiriusBridge.cs](cbot/SiriusBridge.cs)).
* **MetaTrader 4 (MT4):** Ultra-fast local socket/file IPC bridge via EA ([mt4/SiriusBridge.mq4](mt4/SiriusBridge.mq4)).
* **Multi-Account Dispatching:** Independent parallel account worker processes (`account_worker.py`, `ctrader_worker.py`, `mt4_worker.py`) routing specific signal providers to distinct broker accounts with unique risk profiles.

### 2. Hybrid AI & Regex Parsing Engine
* **Gemini AI Integration:** Rotates multiple Gemini API keys (`gemini-2.5-flash`) to parse complex, messy, non-standard, or conversational Telegram signals.
* **Deterministic Rule Engine:** High-speed cached regex rules per signal provider (`provider_parser_manager.py`) to execute unambiguous signals instantly without external API latency.
* **Typo & Price Invariant Auto-Fixing:** Automatically validates and corrects symbol syntax, decimal point errors, inverted stop losses, and TP ladder inconsistencies against live broker tick data.

### 3. Advanced Trade Management
* **Dynamic Multi-TP Split:** Customizable lot splitting across TP1, TP2, TP3 (e.g., 50% / 30% / 20%).
* **Trailing & Breakeven Automation:** Automated SL shift to Breakeven (`ENTRY`), `TP1`, or `TP2` upon reaching target profit levels.
* **Smart Spread Offset:** Entry and SL adjustments that factor in real-time broker spreads.
* **Partial Closes & Modifies:** Real-time handling of "close half", "close TP1", "move SL to entry", and "delete pending" Telegram messages.

### 4. Real-Time Web Dashboard & Diagnostics
* **FastAPI Backend:** Lightweight asynchronous web server (`web_server.py`) serving live analytics and controls.
* **Interactive Web Interface:** Single-page dashboard (`web/templates/index.html`) displaying account balances, open positions, recent signals, parser accuracy, and execution logs.
* **Signal-Watch Diagnostics:** Background monitoring subsystem (`signal_watch.py`) detecting skipped or rejected signals, analyzing terminal journal logs, and firing immediate alerts.
* **Built-in Backtester Module:** Isolated historical backtesting suite (`backtester/`) for simulating provider performance against historical tick data.

---

## 📁 Repository Overview

```
├── .env.example                     # Environment variables template
├── accounts_config.example.json     # Multi-account configuration template
├── main.py                          # Main application entry point & process orchestrator
├── config.py                        # Centralized application settings & env loader
├── web_server.py                    # FastAPI web server for dashboard and API controls
├── account_manager.py               # Process supervisor for multi-account workers
├── account_worker.py                # Dedicated worker process for MT5 accounts
├── ctrader_worker.py                # Dedicated worker process for cTrader accounts
├── mt4_worker.py                    # Dedicated worker process for MT4 accounts
├── signal_listener.py               # Telethon Telegram client listening to channels & topics
├── signal_parser.py                 # Hybrid AI + Regex signal extraction engine
├── signal_watch.py                  # Self-healing diagnostic scanner & root-cause analyzer
├── symbol_mapper.py                 # Dynamic symbol mapping across brokers (e.g. Gold -> XAUUSD.sc)
├── provider_analytics.py            # Provider win-rate, profit, and reliability tracking
├── provider_parser_manager.py       # Custom per-provider parsing rules manager
├── db.py                            # SQLite database schema & persistence layer
├── alert.py                         # Instant Telegram alert dispatch utility
├── push_to_github.bat               # 1-Click tool to stage, commit, and push updates to GitHub
├── Launch Sirius.bat                # Supervisor startup script with auto-crash recovery
├── Stop Sirius.bat                  # Clean shutdown script for all background workers
├── cbot/                            # cTrader bridge cBot source code
├── mt4/                             # MT4 bridge Expert Advisor source code
├── symbol_maps/                     # Broker-specific symbol dictionary files
├── backtester/                      # Isolated backtesting engine and reports generator
└── web/                             # Dashboard web templates and static assets
```

---

## 🚀 Getting Started

### Prerequisites
* Windows 10/11 or Windows Server (VPS)
* Python 3.12+ (64-bit)
* MetaTrader 5, MetaTrader 4, or cTrader installed locally

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/VPH9578/Verifx-Sirius.git
   cd Verifx-Sirius
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Environment Secrets:**
   * Copy `.env.example` to `.env`:
     ```bash
     cp .env.example .env
     ```
   * Fill in your Telegram API credentials (from [my.telegram.org](https://my.telegram.org)), MetaTrader credentials, and your Gemini API key(s).

4. **Configure Trading Accounts:**
   * Copy `accounts_config.example.json` to `accounts_config.json`:
     ```bash
     cp accounts_config.example.json accounts_config.json
     ```
   * Add your MT5, MT4, and cTrader account logins, risk percentages, and provider subscriptions.

---

## 🖥️ Running the Application

### Option A: 1-Click Launcher (Recommended)
Double-click **`Launch Sirius.bat`**. This launches:
1. The background Telegram listener
2. Multi-broker worker processes
3. The real-time web dashboard on `http://127.0.0.1:8000`
4. Automated crash recovery and health monitor

### Option B: Terminal Command
```bash
python main.py
```

### Stopping the Copier
Double-click **`Stop Sirius.bat`** to gracefully close all background workers and disconnect safely from brokers.

---

## 🔒 Security Best Practices

* **Zero Leaks:** Always keep `.env`, `accounts_config.json`, `*.session`, and `*.db` files untracked. They are already listed in `.gitignore`.
* **GitHub Syncing:** Use the included **`push_to_github.bat`** script to push code changes safely without leaking credentials.

---

## 📄 License
This project is proprietary and confidential. Unauthorized copying or redistribution is strictly prohibited.
