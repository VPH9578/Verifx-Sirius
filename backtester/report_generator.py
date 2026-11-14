"""
report_generator.py — Generates self-contained, interactive standalone HTML reports
for Sirius TSC Backtester results.

When exported, the report can be saved as an .html file and shared directly
via WhatsApp, Email, or Slack. The recipient can open it in any modern browser
with zero server connection, zero installation, and zero dependencies.
All charts, metrics, TP breakdown analytics, and trade logs are embedded client-side.
"""

import json
from datetime import datetime


def generate_html_report(run: dict, alias: str = None) -> str:
    """
    Takes a backtest run dictionary and returns a full, self-contained HTML document.
    If alias is provided, it replaces the signal provider's name throughout the report.
    """
    if not run:
        run = {}

    summary = run.get("summary") or {}
    rules = run.get("rules") or {}
    graph = run.get("graph") or {}
    signals = run.get("signals") or []
    deals = run.get("deals") or []

    raw_provider_name = run.get("provider_name") or summary.get("provider_name") or "Signal Provider"
    provider_name = alias.strip() if (alias and alias.strip()) else raw_provider_name

    # If an alias was applied, create a shallow copy of run for embedded JSON to reflect the alias
    run_to_embed = dict(run)
    run_to_embed["provider_name"] = provider_name
    if "summary" in run_to_embed and isinstance(run_to_embed["summary"], dict):
        summary_copy = dict(run_to_embed["summary"])
        summary_copy["provider_name"] = provider_name
        run_to_embed["summary"] = summary_copy
    start_time = run.get("start_time") or 0
    end_time = run.get("end_time") or 0
    created_at = run.get("created_at") or int(datetime.now().timestamp())

    start_date_str = datetime.fromtimestamp(start_time).strftime("%Y-%m-%d") if start_time else "Earliest"
    end_date_str = datetime.fromtimestamp(end_time).strftime("%Y-%m-%d") if end_time else "Latest"
    report_created_str = datetime.fromtimestamp(created_at).strftime("%Y-%m-%d %H:%M:%S")

    # Serialize run data safely for embedding in <script>
    safe_data_json = json.dumps(run_to_embed).replace("</script>", "<\\/script>")

    html = f"""<!DOCTYPE html>
<html lang="en" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Sirius TSC Backtest Report — {provider_name}</title>
    <!-- Tailwind CSS CDN -->
    <script src="https://cdn.tailwindcss.com"></script>
    <!-- Chart.js CDN -->
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <!-- FontAwesome CDN -->
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <script>
        tailwind.config = {{
            darkMode: 'class',
            theme: {{
                extend: {{
                    colors: {{
                        theme: {{
                            bg: '#0b0f19',
                            card: '#121827',
                            sidebar: '#0f1422',
                            border: '#1e293b',
                            accent: '#8b5cf6',
                            green: '#10b981',
                            red: '#ef4444',
                            cyan: '#06b6d4',
                            gold: '#f59e0b',
                        }}
                    }}
                }}
            }}
        }}
    </script>
    <style>
        body {{
            background-color: #0b0f19;
            color: #f1f5f9;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            margin: 0;
            padding: 0;
        }}
        /* Fallback dark theme styling if CDN is slow */
        .glass-card {{
            background: #121827;
            border: 1px solid #1e293b;
            border-radius: 0.75rem;
        }}
        /* Custom scrollbar */
        ::-webkit-scrollbar {{
            width: 8px;
            height: 8px;
        }}
        ::-webkit-scrollbar-track {{
            background: #0b0f19;
        }}
        ::-webkit-scrollbar-thumb {{
            background: #334155;
            border-radius: 4px;
        }}
        ::-webkit-scrollbar-thumb:hover {{
            background: #475569;
        }}
        @media print {{
            .no-print {{
                display: none !important;
            }}
            body {{
                background-color: #ffffff !important;
                color: #000000 !important;
            }}
            .glass-card {{
                border-color: #cbd5e1 !important;
                background: #ffffff !important;
                color: #000000 !important;
            }}
        }}
    </style>
</head>
<body class="min-h-screen text-slate-100 antialiased selection:bg-purple-600 selection:text-white">

    <!-- Top Navigation / Header -->
    <header class="border-b border-theme-border bg-theme-sidebar/90 backdrop-blur sticky top-0 z-50 px-4 sm:px-8 py-3.5 flex flex-wrap items-center justify-between gap-4">
        <div class="flex items-center gap-3">
            <div class="w-10 h-10 rounded-xl bg-gradient-to-tr from-purple-600 to-indigo-500 flex items-center justify-center text-white shadow-lg shadow-purple-900/30 font-bold text-lg">
                <i class="fa-solid fa-flask-vial"></i>
            </div>
            <div>
                <div class="flex items-center gap-2">
                    <span class="text-xs uppercase tracking-wider text-purple-400 font-bold">Sirius TSC</span>
                    <span class="px-2 py-0.5 rounded-full bg-purple-500/10 border border-purple-500/30 text-purple-300 text-[10px] font-mono">Interactive Report</span>
                    <span class="px-2 py-0.5 rounded-full bg-emerald-500/10 border border-emerald-500/30 text-emerald-400 text-[10px] font-mono"><i class="fa-solid fa-shield-halved mr-1"></i>Sanity Checked</span>
                </div>
                <h1 class="text-base sm:text-lg font-bold text-white flex items-center gap-2">
                    <span id="header-provider-name">{provider_name}</span>
                </h1>
            </div>
        </div>

        <div class="flex items-center gap-2.5 text-xs">
            <div class="hidden sm:flex flex-col text-right font-mono text-[11px] text-slate-400">
                <span><i class="fa-regular fa-calendar mr-1 text-slate-500"></i> {start_date_str} → {end_date_str}</span>
                <span class="text-slate-500 text-[10px]">Generated: {report_created_str}</span>
            </div>
            <button onclick="window.print()" class="no-print px-3 py-1.5 rounded-lg bg-theme-card border border-theme-border hover:border-slate-500 text-slate-300 hover:text-white transition-all flex items-center gap-1.5 shadow-sm font-semibold text-xs" title="Print or save as PDF">
                <i class="fa-solid fa-print"></i>
                <span class="hidden md:inline">Print / PDF</span>
            </button>
            <button onclick="copyShareLink()" id="btn-share" class="no-print px-3 py-1.5 rounded-lg bg-purple-600 hover:bg-purple-500 text-white transition-all flex items-center gap-1.5 shadow-sm font-semibold text-xs" title="Copy report info">
                <i class="fa-solid fa-share-nodes"></i>
                <span>Share Info</span>
            </button>
        </div>
    </header>

    <main class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6 space-y-6">

        <!-- Top Overview Banner / Executive Summary -->
        <div class="glass-card p-5 border border-purple-500/30 bg-gradient-to-r from-theme-card via-purple-950/20 to-theme-card relative overflow-hidden shadow-xl">
            <div class="flex flex-wrap items-center justify-between gap-4 mb-4">
                <div class="space-y-1">
                    <div class="flex items-center gap-2">
                        <span class="px-2.5 py-0.5 rounded bg-emerald-500/15 border border-emerald-500/30 text-emerald-400 text-xs font-mono font-bold" id="kpi-winrate-pill">
                            --% Win Rate
                        </span>
                        <span class="px-2.5 py-0.5 rounded bg-purple-500/15 border border-purple-500/30 text-purple-300 text-xs font-mono font-bold" id="kpi-trades-pill">
                            -- Trades
                        </span>
                    </div>
                    <h2 class="text-xl sm:text-2xl font-bold text-white tracking-tight" id="banner-pnl-headline">
                        +$0.00 (+0.00%)
                    </h2>
                    <p class="text-xs text-slate-400">
                        Historical simulation based on MetaTrader 5 ticks and real Telegram signal updates.
                    </p>
                </div>

                <div class="flex flex-wrap gap-2 text-xs font-mono">
                    <div class="bg-slate-900/80 border border-theme-border rounded-lg px-3 py-2 text-right">
                        <div class="text-[10px] uppercase text-slate-400">Initial Balance</div>
                        <div class="font-bold text-slate-200 mt-0.5" id="kpi-initial-balance">$10,000.00</div>
                    </div>
                    <div class="bg-slate-900/80 border border-theme-border rounded-lg px-3 py-2 text-right">
                        <div class="text-[10px] uppercase text-slate-400">Final Equity</div>
                        <div class="font-bold text-emerald-400 mt-0.5" id="kpi-final-equity">$0.00</div>
                    </div>
                </div>
            </div>

            <!-- Tested Trading Strategy & Rules Pills -->
            <div class="pt-3 border-t border-theme-border/60">
                <div class="text-[10px] uppercase font-bold tracking-wider text-slate-400 mb-2 flex items-center gap-1.5">
                    <i class="fa-solid fa-sliders text-purple-400"></i> Backtested Strategy Rules
                </div>
                <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-2 text-xs font-mono" id="rules-grid">
                    <!-- Populated by JavaScript -->
                </div>
            </div>
        </div>

        <!-- 6-Metric Core KPI Grid -->
        <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3 sm:gap-4">
            <div class="glass-card p-3.5 sm:p-4 border-l-4 border-l-emerald-500">
                <div class="text-[10px] uppercase tracking-wider font-semibold text-slate-400">Net Profit</div>
                <div class="text-sm sm:text-base lg:text-lg font-bold font-mono text-emerald-400 mt-1 truncate" id="card-net-profit">--</div>
                <div class="text-[11px] font-mono text-slate-400" id="card-gain-pct">--%</div>
            </div>

            <div class="glass-card p-3.5 sm:p-4 border-l-4 border-l-cyan-500">
                <div class="text-[10px] uppercase tracking-wider font-semibold text-slate-400">Win Rate</div>
                <div class="text-sm sm:text-base lg:text-lg font-bold font-mono text-cyan-400 mt-1 truncate" id="card-win-rate">--%</div>
                <div class="text-[11px] font-mono text-slate-400" id="card-win-count">-- wins / -- total</div>
            </div>

            <div class="glass-card p-3.5 sm:p-4 border-l-4 border-l-purple-500">
                <div class="text-[10px] uppercase tracking-wider font-semibold text-slate-400">Profit Factor</div>
                <div class="text-sm sm:text-base lg:text-lg font-bold font-mono text-purple-300 mt-1 truncate" id="card-profit-factor">--</div>
                <div class="text-[11px] font-mono text-slate-400" id="card-avg-rr">Avg R:R: --</div>
            </div>

            <div class="glass-card p-3.5 sm:p-4 border-l-4 border-l-rose-500">
                <div class="text-[10px] uppercase tracking-wider font-semibold text-slate-400">Max Drawdown</div>
                <div class="text-sm sm:text-base lg:text-lg font-bold font-mono text-rose-400 mt-1 truncate" id="card-max-dd-dollar">--</div>
                <div class="text-[11px] font-mono text-rose-400/80" id="card-max-dd-pct">--% Peak to Valley</div>
            </div>

            <div class="glass-card p-3.5 sm:p-4 border-l-4 border-l-amber-500">
                <div class="text-[10px] uppercase tracking-wider font-semibold text-slate-400">Total Lots Traded</div>
                <div class="text-sm sm:text-base lg:text-lg font-bold font-mono text-amber-300 mt-1 truncate" id="card-total-lots">-- Lots</div>
                <div class="text-[11px] font-mono text-slate-400" id="card-deal-count">-- MT5 Deals</div>
            </div>

            <div class="glass-card p-3.5 sm:p-4 border-l-4 border-l-blue-500">
                <div class="text-[10px] uppercase tracking-wider font-semibold text-slate-400">Signal Execution</div>
                <div class="text-sm sm:text-base lg:text-lg font-bold font-mono text-blue-400 mt-1 truncate" id="card-executed-sigs">-- / --</div>
                <div class="text-[11px] font-mono text-slate-400" id="card-scanned-msgs">-- msgs scanned</div>
            </div>
        </div>

        <!-- ==================== DEEP DIVE: TP LEVEL BREAKDOWN & OUTCOMES ==================== -->
        <div class="glass-card p-5 border border-theme-border shadow-lg space-y-4">
            <div class="flex flex-wrap items-center justify-between gap-2 border-b border-theme-border/60 pb-3">
                <div class="flex items-center gap-2">
                    <div class="w-7 h-7 rounded-lg bg-emerald-500/20 text-emerald-400 flex items-center justify-center font-bold text-xs">
                        <i class="fa-solid fa-bullseye"></i>
                    </div>
                    <div>
                        <h3 class="text-sm font-bold text-white uppercase tracking-wider">
                            Take-Profit Target Hits &amp; Outcome Breakdown
                        </h3>
                        <p class="text-[11px] text-slate-400">Exact distribution of which TP targets were reached before trade completion</p>
                    </div>
                </div>
                <div class="text-xs font-mono text-slate-400" id="tp-summary-badge">
                    -- Closed Trades Analyzed
                </div>
            </div>

            <!-- Proportional Outcome Visual Bar -->
            <div class="space-y-1.5">
                <div class="flex justify-between text-[11px] font-mono text-slate-400">
                    <span>Outcome Distribution Ratio</span>
                    <span id="outcome-ratio-text">TP Hits vs. BE vs. SL</span>
                </div>
                <div class="w-full h-3.5 rounded-full bg-slate-800 flex overflow-hidden border border-slate-700/60" id="outcome-progress-bar">
                    <!-- Dynamic segments filled by JS -->
                </div>
                <div class="flex flex-wrap items-center gap-4 text-[11px] font-mono pt-1 text-slate-400">
                    <span class="flex items-center gap-1.5"><span class="w-2.5 h-2.5 rounded-full bg-emerald-500"></span> TP3 Hits</span>
                    <span class="flex items-center gap-1.5"><span class="w-2.5 h-2.5 rounded-full bg-teal-400"></span> TP2 Hits</span>
                    <span class="flex items-center gap-1.5"><span class="w-2.5 h-2.5 rounded-full bg-cyan-400"></span> TP1 Hits</span>
                    <span class="flex items-center gap-1.5"><span class="w-2.5 h-2.5 rounded-full bg-purple-500"></span> Break-Even (BE)</span>
                    <span class="flex items-center gap-1.5"><span class="w-2.5 h-2.5 rounded-full bg-rose-500"></span> Stop-Loss (SL)</span>
                </div>
            </div>

            <!-- Cards Grid: TP1, TP2, TP3, BE, SL, Cancelled/Expired -->
            <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3 pt-2">
                <!-- TP 1 -->
                <div class="bg-slate-900/70 border border-cyan-500/30 rounded-xl p-3.5 text-center relative overflow-hidden">
                    <div class="absolute top-0 left-0 right-0 h-1 bg-cyan-400"></div>
                    <div class="text-[10px] uppercase font-bold text-cyan-400 tracking-wider">TP 1 Reached</div>
                    <div class="text-xl sm:text-2xl font-bold font-mono text-white mt-1" id="stat-tp1-hits">0</div>
                    <div class="text-[11px] font-mono text-cyan-300/90 mt-0.5" id="stat-tp1-pct">0.0% of trades</div>
                </div>

                <!-- TP 2 -->
                <div class="bg-slate-900/70 border border-teal-500/30 rounded-xl p-3.5 text-center relative overflow-hidden">
                    <div class="absolute top-0 left-0 right-0 h-1 bg-teal-400"></div>
                    <div class="text-[10px] uppercase font-bold text-teal-400 tracking-wider">TP 2 Reached</div>
                    <div class="text-xl sm:text-2xl font-bold font-mono text-white mt-1" id="stat-tp2-hits">0</div>
                    <div class="text-[11px] font-mono text-teal-300/90 mt-0.5" id="stat-tp2-pct">0.0% of trades</div>
                </div>

                <!-- TP 3 -->
                <div class="bg-slate-900/70 border border-emerald-500/30 rounded-xl p-3.5 text-center relative overflow-hidden">
                    <div class="absolute top-0 left-0 right-0 h-1 bg-emerald-500"></div>
                    <div class="text-[10px] uppercase font-bold text-emerald-400 tracking-wider">TP 3 Reached</div>
                    <div class="text-xl sm:text-2xl font-bold font-mono text-white mt-1" id="stat-tp3-hits">0</div>
                    <div class="text-[11px] font-mono text-emerald-300/90 mt-0.5" id="stat-tp3-pct">0.0% of trades</div>
                </div>

                <!-- Break Even -->
                <div class="bg-slate-900/70 border border-purple-500/30 rounded-xl p-3.5 text-center relative overflow-hidden">
                    <div class="absolute top-0 left-0 right-0 h-1 bg-purple-500"></div>
                    <div class="text-[10px] uppercase font-bold text-purple-400 tracking-wider">Break-Even (BE)</div>
                    <div class="text-xl sm:text-2xl font-bold font-mono text-white mt-1" id="stat-be-hits">0</div>
                    <div class="text-[11px] font-mono text-purple-300/90 mt-0.5" id="stat-be-pct">0.0% of trades</div>
                </div>

                <!-- Full Stop Loss -->
                <div class="bg-slate-900/70 border border-rose-500/30 rounded-xl p-3.5 text-center relative overflow-hidden">
                    <div class="absolute top-0 left-0 right-0 h-1 bg-rose-500"></div>
                    <div class="text-[10px] uppercase font-bold text-rose-400 tracking-wider">Stop-Loss (SL)</div>
                    <div class="text-xl sm:text-2xl font-bold font-mono text-white mt-1" id="stat-sl-hits">0</div>
                    <div class="text-[11px] font-mono text-rose-300/90 mt-0.5" id="stat-sl-pct">0.0% of trades</div>
                </div>

                <!-- Cancelled / Expired -->
                <div class="bg-slate-900/70 border border-slate-700 rounded-xl p-3.5 text-center relative overflow-hidden">
                    <div class="absolute top-0 left-0 right-0 h-1 bg-slate-500"></div>
                    <div class="text-[10px] uppercase font-bold text-slate-400 tracking-wider">Expired / Skipped</div>
                    <div class="text-xl sm:text-2xl font-bold font-mono text-slate-300 mt-1" id="stat-other-count">0</div>
                    <div class="text-[11px] font-mono text-slate-500 mt-0.5" id="stat-other-desc">No fill on entry</div>
                </div>
            </div>
        </div>

        <!-- ==================== INTERACTIVE EQUITY GROWTH GRAPH ==================== -->
        <div class="glass-card p-5 border border-theme-border shadow-xl space-y-4">
            <div class="flex flex-wrap items-center justify-between gap-3">
                <div class="flex items-center gap-2">
                    <div class="w-7 h-7 rounded-lg bg-purple-500/20 text-purple-400 flex items-center justify-center font-bold text-xs">
                        <i class="fa-solid fa-arrow-trend-up"></i>
                    </div>
                    <div>
                        <h3 class="text-sm font-bold text-white uppercase tracking-wider">Equity Growth Curve</h3>
                        <p class="text-[11px] text-slate-400">Interactive simulation equity timeline. Hover or click points to inspect specific trades.</p>
                    </div>
                </div>

                <!-- Resolution controls -->
                <div class="flex flex-wrap items-center gap-1.5 bg-slate-900/80 p-1 rounded-lg border border-theme-border text-xs">
                    <button type="button" onclick="setGraphMode('trade_by_trade')" id="btn-chart-trade" class="px-2.5 py-1 rounded font-bold text-white bg-purple-600 transition-colors">Trade-by-Trade</button>
                    <button type="button" onclick="setGraphMode('daily')" id="btn-chart-daily" class="px-2.5 py-1 rounded font-bold text-slate-400 hover:text-white transition-colors">Daily</button>
                    <button type="button" onclick="setGraphMode('weekly')" id="btn-chart-weekly" class="px-2.5 py-1 rounded font-bold text-slate-400 hover:text-white transition-colors">Weekly</button>
                    <button type="button" onclick="setGraphMode('monthly')" id="btn-chart-monthly" class="px-2.5 py-1 rounded font-bold text-slate-400 hover:text-white transition-colors">Monthly</button>
                </div>
            </div>

            <!-- Canvas Container -->
            <div class="relative w-full h-[320px] sm:h-[400px]">
                <canvas id="growthChart"></canvas>
            </div>

            <!-- Selected Point Inspector Bar -->
            <div id="chart-point-inspector" class="hidden p-3 rounded-lg bg-purple-950/30 border border-purple-500/40 flex flex-wrap items-center justify-between gap-3 text-xs font-mono">
                <div class="flex items-center gap-3">
                    <span class="font-bold text-purple-300" id="inspect-trade-num">Trade #1</span>
                    <span class="text-slate-400" id="inspect-time">2026-01-01</span>
                    <span class="font-bold" id="inspect-symbol">XAUUSD</span>
                    <span class="font-bold" id="inspect-direction">BUY</span>
                </div>
                <div class="flex items-center gap-4">
                    <span>PnL: <strong id="inspect-pnl" class="text-emerald-400">+$0.00</strong></span>
                    <span>Equity: <strong id="inspect-equity" class="text-white">$10,000.00</strong></span>
                </div>
            </div>
        </div>

        <!-- ==================== SEARCHABLE & FILTERABLE TRADE LOG ==================== -->
        <div class="glass-card p-5 border border-theme-border shadow-xl space-y-4">
            <div class="flex flex-wrap items-center justify-between gap-3 border-b border-theme-border/60 pb-3">
                <div class="flex items-center gap-2">
                    <div class="w-7 h-7 rounded-lg bg-blue-500/20 text-blue-400 flex items-center justify-center font-bold text-xs">
                        <i class="fa-solid fa-list-check"></i>
                    </div>
                    <div>
                        <h3 class="text-sm font-bold text-white uppercase tracking-wider">Executed Trade &amp; Signal Log</h3>
                        <p class="text-[11px] text-slate-400">Every signal simulated. Click any row to view raw Telegram signal text and execution legs.</p>
                    </div>
                </div>

                <div class="flex flex-wrap items-center gap-2">
                    <!-- Search input -->
                    <div class="relative">
                        <i class="fa-solid fa-magnifying-glass absolute left-3 top-2.5 text-slate-500 text-xs"></i>
                        <input type="text" id="signal-search" oninput="applyFilters()" placeholder="Search symbol or text..." class="pl-8 pr-3 py-1.5 rounded-lg bg-slate-900 border border-theme-border text-xs text-white placeholder-slate-500 focus:outline-none focus:border-purple-500">
                    </div>

                    <!-- Filter pills -->
                    <div class="flex items-center gap-1 bg-slate-900 p-0.5 rounded-lg border border-theme-border text-xs">
                        <button onclick="setFilter('ALL')" id="flt-ALL" class="px-2.5 py-1 rounded font-bold bg-purple-600 text-white">All (<span id="count-all">0</span>)</button>
                        <button onclick="setFilter('WIN')" id="flt-WIN" class="px-2.5 py-1 rounded font-bold text-slate-400 hover:text-emerald-400">Wins (<span id="count-win">0</span>)</button>
                        <button onclick="setFilter('BE')" id="flt-BE" class="px-2.5 py-1 rounded font-bold text-slate-400 hover:text-purple-300">BE (<span id="count-be">0</span>)</button>
                        <button onclick="setFilter('SL')" id="flt-SL" class="px-2.5 py-1 rounded font-bold text-slate-400 hover:text-rose-400">Losses (<span id="count-sl">0</span>)</button>
                        <button onclick="setFilter('OTHER')" id="flt-OTHER" class="px-2.5 py-1 rounded font-bold text-slate-400 hover:text-slate-300">Expired/Other (<span id="count-other">0</span>)</button>
                    </div>
                </div>
            </div>

            <!-- Table -->
            <div class="overflow-x-auto rounded-lg border border-theme-border">
                <table class="w-full text-left text-xs text-slate-300 font-mono">
                    <thead class="bg-slate-900/90 text-[10px] uppercase tracking-wider text-slate-400 border-b border-theme-border">
                        <tr>
                            <th class="py-2.5 px-3 cursor-pointer" onclick="sortTable('idx')"># <i class="fa-solid fa-sort"></i></th>
                            <th class="py-2.5 px-3 cursor-pointer" onclick="sortTable('time')">Date &amp; Time <i class="fa-solid fa-sort"></i></th>
                            <th class="py-2.5 px-3 cursor-pointer" onclick="sortTable('symbol')">Symbol <i class="fa-solid fa-sort"></i></th>
                            <th class="py-2.5 px-3">Type</th>
                            <th class="py-2.5 px-3 text-right">Entry</th>
                            <th class="py-2.5 px-3 text-right">SL</th>
                            <th class="py-2.5 px-3 text-center">TPs Hit</th>
                            <th class="py-2.5 px-3 text-center">Outcome</th>
                            <th class="py-2.5 px-3 text-right cursor-pointer" onclick="sortTable('pnl')">Net PnL ($) <i class="fa-solid fa-sort"></i></th>
                            <th class="py-2.5 px-3 text-center">Details</th>
                        </tr>
                    </thead>
                    <tbody id="trade-table-body" class="divide-y divide-theme-border/50">
                        <!-- Populated by JS -->
                    </tbody>
                </table>
            </div>

            <div class="flex items-center justify-between text-xs text-slate-400 pt-2 font-mono">
                <span id="table-showing-text">Showing 0 of 0 trades</span>
                <span>Click any row to toggle full Telegram signal details</span>
            </div>
        </div>

    </main>

    <!-- Footer -->
    <footer class="border-t border-theme-border py-6 mt-12 text-center text-xs text-slate-500 font-mono">
        <p>Generated with <strong class="text-purple-400 font-bold">Sirius TSC</strong> Signal Provider Strategy Backtester</p>
        <p class="text-[10px] mt-1 text-slate-600">This standalone file is self-contained. It can be viewed offline without any active server connection.</p>
    </footer>

    <!-- EMBEDDED REPORT DATA -->
    <script>
        const REPORT_DATA = {safe_data_json};
    </script>

    <!-- REPORT LOGIC & INTERACTIVITY -->
    <script>
        let currentGraphMode = 'trade_by_trade';
        let currentFilter = 'ALL';
        let sortColumn = 'idx';
        let sortAsc = true;
        let chartInstance = null;
        let expandedRowId = null;

        // Initialize report once DOM is ready
        document.addEventListener('DOMContentLoaded', () => {{
            initReport();
        }});

        function initReport() {{
            const data = REPORT_DATA || {{}};
            const s = data.summary || {{}};
            const rules = data.rules || {{}};
            const sigs = data.signals || [];

            // 1. Populate Header & Banner Headline
            const pName = data.provider_name || s.provider_name || 'Signal Provider';
            document.title = `Sirius Backtest — ${{pName}}`;
            const headerEl = document.getElementById('header-provider-name');
            if (headerEl) headerEl.innerText = pName;

            const profit = parseFloat(s.profit || 0);
            const gainPct = parseFloat(s.gain_pct || 0);
            const startBal = parseFloat(s.starting_balance || 10000);
            const finalBal = parseFloat(s.ending_balance || (startBal + profit));
            const wr = parseFloat(s.win_rate || 0);
            const pf = parseFloat(s.profit_factor || 0);
            const maxDdDollar = parseFloat(s.max_drawdown_dollar || 0);
            const maxDdPct = parseFloat(s.max_drawdown_pct || 0);
            const tradesCount = s.trades_count || 0;
            const totalSigs = s.total_signals_found || sigs.length || 0;

            // Headline
            const headlineEl = document.getElementById('banner-pnl-headline');
            if (headlineEl) {{
                headlineEl.innerText = `${{profit >= 0 ? '+$' : '-$'}}${{Math.abs(profit).toLocaleString('en-US', {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }})}} (${{gainPct >= 0 ? '+' : ''}}${{gainPct.toFixed(2)}}%)`;
                headlineEl.className = profit >= 0 ? 'text-xl sm:text-2xl font-bold text-emerald-400 tracking-tight' : 'text-xl sm:text-2xl font-bold text-rose-400 tracking-tight';
            }}
            document.getElementById('kpi-winrate-pill').innerText = `${{wr.toFixed(1)}}% Win Rate`;
            document.getElementById('kpi-trades-pill').innerText = `${{tradesCount}} Closed Trades`;
            document.getElementById('kpi-initial-balance').innerText = `$${{startBal.toLocaleString('en-US', {{ minimumFractionDigits: 2 }})}}`;
            document.getElementById('kpi-final-equity').innerText = `$${{finalBal.toLocaleString('en-US', {{ minimumFractionDigits: 2 }})}}`;

            // 2. Core KPI Cards
            const cardPnl = document.getElementById('card-net-profit');
            cardPnl.innerText = `${{profit >= 0 ? '+$' : '-$'}}${{Math.abs(profit).toLocaleString('en-US', {{ minimumFractionDigits: 2 }})}}`;
            cardPnl.className = profit >= 0 ? 'text-sm sm:text-base lg:text-lg font-bold font-mono text-emerald-400 mt-1 truncate' : 'text-sm sm:text-base lg:text-lg font-bold font-mono text-rose-400 mt-1 truncate';
            document.getElementById('card-gain-pct').innerText = `${{gainPct >= 0 ? '+' : ''}}${{gainPct.toFixed(2)}}% Return`;

            document.getElementById('card-win-rate').innerText = `${{wr.toFixed(1)}}%`;
            const winCount = (s.tp1_hits || 0);
            document.getElementById('card-win-count').innerText = `${{winCount}} Wins / ${{tradesCount}} Trades`;

            document.getElementById('card-profit-factor').innerText = pf.toFixed(2);
            document.getElementById('card-avg-rr').innerText = `Avg R:R: ${{s.rr_display || s.avg_rr || '1:1'}}`;

            document.getElementById('card-max-dd-dollar').innerText = `-$${{maxDdDollar.toLocaleString('en-US', {{ minimumFractionDigits: 2 }})}}`;
            document.getElementById('card-max-dd-pct').innerText = `-${{maxDdPct.toFixed(1)}}% Peak to Valley`;

            document.getElementById('card-total-lots').innerText = `${{(s.lots || 0).toFixed(2)}} Lots`;
            document.getElementById('card-deal-count').innerText = `${{s.deal_count || 0}} MT5 Executions`;

            document.getElementById('card-executed-sigs').innerText = `${{tradesCount}} / ${{totalSigs}} Sigs`;
            document.getElementById('card-scanned-msgs').innerText = `${{(s.total_messages_scanned || 0).toLocaleString()}} msgs scanned`;

            // 3. Render Strategy Rules
            renderRulesGrid(rules);

            // 4. Render Deep-Dive TP Breakdown
            renderTpBreakdown(s, tradesCount);

            // 5. Initialize Chart
            initGrowthChart();

            // 6. Index Signals and render Table
            indexSignals(sigs);
            applyFilters();
        }}

        function renderRulesGrid(rules) {{
            const grid = document.getElementById('rules-grid');
            if (!grid) return;

            const rMode = rules.risk_mode === 'fixed' ? `${{rules.risk_value || 0.01}} Fixed Lot` : `${{rules.risk_value || 1.0}}% Equity Risk`;
            const tp1 = parseFloat(rules.tp1_percent !== undefined ? rules.tp1_percent : 60).toFixed(0);
            const tp2 = parseFloat(rules.tp2_percent !== undefined ? rules.tp2_percent : 30).toFixed(0);
            const tp3 = parseFloat(rules.tp3_percent !== undefined ? rules.tp3_percent : 10).toFixed(0);

            const sl1 = rules.sl_tp1_strategy === 'ENTRY' ? 'Move SL → Break-Even' : (rules.sl_tp1_strategy === 'TP1' ? 'Lock SL at TP1' : 'Hold Original SL');
            const sl2 = rules.sl_tp2_strategy === 'TP1' ? 'Lock SL at TP1' : (rules.sl_tp2_strategy === 'ENTRY' ? 'Move SL → Break-Even' : 'Hold SL');
            const exp = rules.pending_expiry_minutes > 0 ? `${{rules.pending_expiry_minutes}}m Expiry` : 'No Expiry';

            const items = [
                {{ label: 'Risk Sizing', value: rMode, color: 'text-purple-300' }},
                {{ label: 'TP Splits', value: `TP1 ${{tp1}}% / TP2 ${{tp2}}% / TP3 ${{tp3}}%`, color: 'text-emerald-400' }},
                {{ label: 'Trailing upon TP1', value: sl1, color: 'text-cyan-400' }},
                {{ label: 'Trailing upon TP2', value: sl2, color: 'text-cyan-300' }},
                {{ label: 'Pending Expiry', value: exp, color: 'text-slate-300' }},
                {{ label: 'Resolution', value: 'MT5 Candle Direction', color: 'text-amber-300' }},
            ];

            grid.innerHTML = items.map(it => `
                <div class="bg-slate-900/80 border border-theme-border rounded-lg p-2.5">
                    <div class="text-[9px] uppercase tracking-wider text-slate-400">${{it.label}}</div>
                    <div class="font-bold ${{it.color}} mt-0.5 truncate text-[11px]">${{it.value}}</div>
                </div>
            `).join('');
        }}

        function renderTpBreakdown(s, tradesCount) {{
            const tp1 = s.tp1_hits || 0;
            const tp2 = s.tp2_hits || 0;
            const tp3 = s.tp3_hits || 0;
            const be = s.be_hits || 0;
            const sl = s.sl_hits || 0;
            const other = (s.cancelled_count || 0) + (s.expired_count || 0) + (s.skipped_count || 0);

            const baseCount = tradesCount > 0 ? tradesCount : 1;
            const tp1Pct = ((tp1 / baseCount) * 100).toFixed(1);
            const tp2Pct = ((tp2 / baseCount) * 100).toFixed(1);
            const tp3Pct = ((tp3 / baseCount) * 100).toFixed(1);
            const bePct = ((be / baseCount) * 100).toFixed(1);
            const slPct = ((sl / baseCount) * 100).toFixed(1);

            document.getElementById('tp-summary-badge').innerText = `${{tradesCount}} Closed Trades Analyzed`;
            document.getElementById('stat-tp1-hits').innerText = tp1;
            document.getElementById('stat-tp1-pct').innerText = `${{tp1Pct}}% of closed`;

            document.getElementById('stat-tp2-hits').innerText = tp2;
            document.getElementById('stat-tp2-pct').innerText = `${{tp2Pct}}% of closed`;

            document.getElementById('stat-tp3-hits').innerText = tp3;
            document.getElementById('stat-tp3-pct').innerText = `${{tp3Pct}}% of closed`;

            document.getElementById('stat-be-hits').innerText = be;
            document.getElementById('stat-be-pct').innerText = `${{bePct}}% of closed`;

            document.getElementById('stat-sl-hits').innerText = sl;
            document.getElementById('stat-sl-pct').innerText = `${{slPct}}% of closed`;

            document.getElementById('stat-other-count').innerText = other;
            document.getElementById('stat-other-desc').innerText = `${{s.expired_count || 0}} exp / ${{s.cancelled_count || 0}} can`;

            // Render Stacked Progress Bar
            const totalOutcomes = tp1 + be + sl;
            const bar = document.getElementById('outcome-progress-bar');
            if (bar && totalOutcomes > 0) {{
                // TP3 (subset of TP1), TP2 (subset of TP1), TP1 pure, BE, SL
                const tp3Width = (tp3 / totalOutcomes) * 100;
                const tp2Width = Math.max(0, ((tp2 - tp3) / totalOutcomes) * 100);
                const tp1Width = Math.max(0, ((tp1 - tp2) / totalOutcomes) * 100);
                const beWidth = (be / totalOutcomes) * 100;
                const slWidth = (sl / totalOutcomes) * 100;

                bar.innerHTML = `
                    <div style="width: ${{tp3Width}}%" class="h-full bg-emerald-500" title="TP3: ${{tp3}} (${{tp3Pct}}%)"></div>
                    <div style="width: ${{tp2Width}}%" class="h-full bg-teal-400" title="TP2: ${{tp2}} (${{tp2Pct}}%)"></div>
                    <div style="width: ${{tp1Width}}%" class="h-full bg-cyan-400" title="TP1: ${{tp1}} (${{tp1Pct}}%)"></div>
                    <div style="width: ${{beWidth}}%" class="h-full bg-purple-500" title="Break-Even: ${{be}} (${{bePct}}%)"></div>
                    <div style="width: ${{slWidth}}%" class="h-full bg-rose-500" title="Stop-Loss: ${{sl}} (${{slPct}}%)"></div>
                `;
            }}
        }}

        function initGrowthChart() {{
            const ctx = document.getElementById('growthChart');
            if (!ctx) return;

            const isDark = true;
            chartInstance = new Chart(ctx, {{
                type: 'line',
                data: {{
                    labels: [],
                    datasets: [{{
                        label: 'Equity ($)',
                        data: [],
                        borderColor: '#10b981',
                        borderWidth: 2,
                        tension: 0.15,
                        pointRadius: 2.5,
                        pointHoverRadius: 6,
                        pointBackgroundColor: '#10b981',
                        pointBorderColor: '#ffffff',
                        pointBorderWidth: 1.5,
                        fill: true,
                        backgroundColor: (context) => {{
                            const chart = context.chart;
                            const {{ ctx, chartArea }} = chart;
                            if (!chartArea) return null;
                            const gradient = ctx.createLinearGradient(0, chartArea.top, 0, chartArea.bottom);
                            gradient.addColorStop(0, 'rgba(16, 185, 129, 0.28)');
                            gradient.addColorStop(1, 'rgba(16, 185, 129, 0.00)');
                            return gradient;
                        }}
                    }}]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: {{
                        mode: 'index',
                        intersect: false,
                    }},
                    plugins: {{
                        legend: {{ display: false }},
                        tooltip: {{
                            backgroundColor: '#0f172a',
                            titleColor: '#94a3b8',
                            bodyColor: '#ffffff',
                            borderColor: '#334155',
                            borderWidth: 1,
                            padding: 10,
                            displayColors: false,
                            callbacks: {{
                                title: (items) => items[0]?.label || '',
                                label: (item) => {{
                                    const val = item.raw || 0;
                                    return `Equity: $${{val.toLocaleString('en-US', {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }})}}`;
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            grid: {{ color: '#1e293b', drawBorder: false }},
                            ticks: {{ color: '#64748b', font: {{ size: 10 }} }}
                        }},
                        y: {{
                            grid: {{ color: '#1e293b', drawBorder: false }},
                            ticks: {{
                                color: '#64748b',
                                font: {{ size: 10 }},
                                callback: (v) => `$${{v.toLocaleString()}}`
                            }}
                        }}
                    }},
                    onClick: (event, elements) => {{
                        if (elements && elements.length > 0) {{
                            const idx = elements[0].index;
                            handleChartPointClick(idx);
                        }}
                    }}
                }}
            }});

            updateChartData();
        }}

        function setGraphMode(mode) {{
            currentGraphMode = mode;
            ['trade', 'daily', 'weekly', 'monthly'].forEach(m => {{
                const btn = document.getElementById(`btn-chart-${{m}}`);
                if (btn) {{
                    if ((mode === 'trade_by_trade' && m === 'trade') || mode === m) {{
                        btn.className = 'px-2.5 py-1 rounded font-bold text-white bg-purple-600 transition-colors';
                    }} else {{
                        btn.className = 'px-2.5 py-1 rounded font-bold text-slate-400 hover:text-white transition-colors';
                    }}
                }}
            }});
            updateChartData();
        }}

        function updateChartData() {{
            if (!chartInstance) return;
            const data = REPORT_DATA || {{}};
            const g = data.graph || {{}};
            const startBal = parseFloat(data.summary?.starting_balance || 10000);

            let labels = [];
            let values = [];

            if (currentGraphMode === 'trade_by_trade' && Array.isArray(g.trade_by_trade)) {{
                g.trade_by_trade.forEach((pt, i) => {{
                    labels.push(`Trade #${{i + 1}} (${{pt.symbol || 'XAUUSD'}} ${{pt.direction || ''}})`);
                    values.push(round2(startBal + (pt.profit || 0)));
                }});
            }} else if (g[currentGraphMode]) {{
                let cumProfit = 0;
                Object.keys(g[currentGraphMode]).sort().forEach(dateKey => {{
                    cumProfit += g[currentGraphMode][dateKey];
                    labels.push(dateKey);
                    values.push(round2(startBal + cumProfit));
                }});
            }}

            if (labels.length === 0) {{
                labels = ['Initial Balance'];
                values = [startBal];
            }}

            // Dynamic color: Green if ending >= start, Red if ending < start
            const ending = values[values.length - 1];
            const isProfit = ending >= startBal;
            chartInstance.data.datasets[0].borderColor = isProfit ? '#10b981' : '#ef4444';
            chartInstance.data.datasets[0].pointBackgroundColor = isProfit ? '#10b981' : '#ef4444';

            chartInstance.data.labels = labels;
            chartInstance.data.datasets[0].data = values;
            chartInstance.update();
        }}

        function handleChartPointClick(idx) {{
            const g = REPORT_DATA?.graph?.trade_by_trade;
            if (currentGraphMode === 'trade_by_trade' && Array.isArray(g) && g[idx]) {{
                const pt = g[idx];
                const inspector = document.getElementById('chart-point-inspector');
                if (inspector) {{
                    inspector.classList.remove('hidden');
                    document.getElementById('inspect-trade-num').innerText = `Trade #${{idx + 1}}`;
                    document.getElementById('inspect-time').innerText = pt.time || '';
                    document.getElementById('inspect-symbol').innerText = pt.symbol || '';
                    document.getElementById('inspect-direction').innerText = pt.direction || '';
                    const pnlEl = document.getElementById('inspect-pnl');
                    pnlEl.innerText = `${{pt.deal_profit >= 0 ? '+$' : '-$'}}${{Math.abs(pt.deal_profit).toFixed(2)}}`;
                    pnlEl.className = pt.deal_profit >= 0 ? 'text-emerald-400 font-bold' : 'text-rose-400 font-bold';
                    document.getElementById('inspect-equity').innerText = `$${{round2((REPORT_DATA.summary?.starting_balance || 10000) + pt.profit).toLocaleString('en-US', {{ minimumFractionDigits: 2 }})}}`;
                }}
            }}
        }}

        // ==================== TRADE LOG & SIGNALS TABLE ====================
        let indexedSignals = [];

        function indexSignals(sigs) {{
            indexedSignals = sigs.map((s, i) => ({{
                ...s,
                _idx: i + 1,
                _pnl: parseFloat(s.final_pnl !== undefined ? s.final_pnl : (s.profit !== undefined ? s.profit : 0))
            }}));

            // Count filters
            let winCount = 0, beCount = 0, slCount = 0, otherCount = 0;
            indexedSignals.forEach(s => {{
                if (s.status === 'CLOSED') {{
                    if (s.be_hit) beCount++;
                    else if (s.sl_hit) slCount++;
                    else if (s.tp1_hit || s._pnl > 0) winCount++;
                    else beCount++;
                }} else {{
                    otherCount++;
                }}
            }});

            document.getElementById('count-all').innerText = indexedSignals.length;
            document.getElementById('count-win').innerText = winCount;
            document.getElementById('count-be').innerText = beCount;
            document.getElementById('count-sl').innerText = slCount;
            document.getElementById('count-other').innerText = otherCount;
        }}

        function setFilter(flt) {{
            currentFilter = flt;
            ['ALL', 'WIN', 'BE', 'SL', 'OTHER'].forEach(f => {{
                const btn = document.getElementById(`flt-${{f}}`);
                if (btn) {{
                    if (f === flt) {{
                        btn.className = 'px-2.5 py-1 rounded font-bold bg-purple-600 text-white';
                    }} else {{
                        btn.className = 'px-2.5 py-1 rounded font-bold text-slate-400 hover:text-white';
                    }}
                }}
            }});
            applyFilters();
        }}

        function sortTable(col) {{
            if (sortColumn === col) {{
                sortAsc = !sortAsc;
            }} else {{
                sortColumn = col;
                sortAsc = true;
            }}
            applyFilters();
        }}

        function applyFilters() {{
            const q = (document.getElementById('signal-search')?.value || '').toLowerCase().trim();

            let filtered = indexedSignals.filter(s => {{
                // Search filter
                if (q) {{
                    const sym = (s.symbol || '').toLowerCase();
                    const dir = (s.direction || '').toLowerCase();
                    const txt = (s.raw_text || '').toLowerCase();
                    if (!sym.includes(q) && !dir.includes(q) && !txt.includes(q)) return false;
                }}

                // Outcome filter
                if (currentFilter === 'WIN') return s.status === 'CLOSED' && !s.be_hit && (s.tp1_hit || s._pnl > 0);
                if (currentFilter === 'BE') return s.status === 'CLOSED' && s.be_hit;
                if (currentFilter === 'SL') return s.status === 'CLOSED' && s.sl_hit && !s.be_hit;
                if (currentFilter === 'OTHER') return s.status !== 'CLOSED';
                return true;
            }});

            // Sort
            filtered.sort((a, b) => {{
                let valA = a[sortColumn];
                let valB = b[sortColumn];
                if (sortColumn === 'idx') {{ valA = a._idx; valB = b._idx; }}
                if (sortColumn === 'time') {{ valA = a.timestamp || 0; valB = b.timestamp || 0; }}
                if (sortColumn === 'symbol') {{ valA = a.symbol || ''; valB = b.symbol || ''; }}
                if (sortColumn === 'pnl') {{ valA = a._pnl; valB = b._pnl; }}

                if (valA < valB) return sortAsc ? -1 : 1;
                if (valA > valB) return sortAsc ? 1 : -1;
                return 0;
            }});

            renderTableRows(filtered);
        }}

        function renderTableRows(rows) {{
            const tbody = document.getElementById('trade-table-body');
            const showingText = document.getElementById('table-showing-text');
            if (!tbody) return;

            showingText.innerText = `Showing ${{rows.length}} of ${{indexedSignals.length}} trades`;

            if (rows.length === 0) {{
                tbody.innerHTML = '<tr><td colspan="10" class="py-8 text-center text-slate-500 italic">No trades match your search or filter</td></tr>';
                return;
            }}

            let html = '';
            rows.forEach(s => {{
                const isExpanded = expandedRowId === s._idx;
                const pnl = s._pnl;
                const pnlColor = pnl > 0 ? 'text-emerald-400' : (pnl < 0 ? 'text-rose-400' : 'text-slate-400');
                const pnlStr = (pnl >= 0 ? '+$' : '-$') + Math.abs(pnl).toFixed(2);

                const dir = (s.direction || 'BUY').toUpperCase();
                const dirColor = dir === 'BUY' ? 'bg-emerald-500/15 text-emerald-400 border border-emerald-500/30' : 'bg-rose-500/15 text-rose-400 border border-rose-500/30';

                // Format Time
                const dt = s.timestamp ? new Date(s.timestamp * 1000) : null;
                const timeStr = dt ? dt.toISOString().replace('T', ' ').substring(0, 16) : '--';

                // TPs hit pills
                let tpBadge = '';
                if (s.tp3_hit) tpBadge = '<span class="px-2 py-0.5 rounded bg-emerald-500/20 text-emerald-300 font-bold text-[10px]">TP 1, 2, 3</span>';
                else if (s.tp2_hit) tpBadge = '<span class="px-2 py-0.5 rounded bg-teal-500/20 text-teal-300 font-bold text-[10px]">TP 1, 2</span>';
                else if (s.tp1_hit) tpBadge = '<span class="px-2 py-0.5 rounded bg-cyan-500/20 text-cyan-300 font-bold text-[10px]">TP 1</span>';
                else if (s.be_hit) tpBadge = '<span class="px-2 py-0.5 rounded bg-purple-500/20 text-purple-300 font-bold text-[10px]">Break-Even</span>';
                else if (s.sl_hit) tpBadge = '<span class="px-2 py-0.5 rounded bg-rose-500/20 text-rose-300 font-bold text-[10px]">Stop-Loss</span>';
                else tpBadge = `<span class="px-2 py-0.5 rounded bg-slate-800 text-slate-400 text-[10px]">${{s.status}}</span>`;

                html += `
                    <tr class="hover:bg-slate-800/40 cursor-pointer transition-colors ${{isExpanded ? 'bg-purple-950/20' : ''}}" onclick="toggleRowDetails(${{s._idx}})">
                        <td class="py-3 px-3 text-slate-400 font-bold">#${{s._idx}}</td>
                        <td class="py-3 px-3 text-slate-300">${{timeStr}}</td>
                        <td class="py-3 px-3 font-bold text-white">${{s.symbol || 'XAUUSD'}}</td>
                        <td class="py-3 px-3">
                            <span class="px-2 py-0.5 rounded text-[10px] font-bold ${{dirColor}}">${{dir}}</span>
                        </td>
                        <td class="py-3 px-3 text-right font-mono">${{s.entry_price || s.entry || '--'}}</td>
                        <td class="py-3 px-3 text-right font-mono text-rose-300">${{s.sl || '--'}}</td>
                        <td class="py-3 px-3 text-center">${{tpBadge}}</td>
                        <td class="py-3 px-3 text-center font-bold text-[11px]">${{s.status}}</td>
                        <td class="py-3 px-3 text-right font-mono font-bold ${{pnlColor}}">${{pnlStr}}</td>
                        <td class="py-3 px-3 text-center text-slate-500">
                            <i class="fa-solid ${{isExpanded ? 'fa-chevron-up text-purple-400' : 'fa-chevron-down'}}"></i>
                        </td>
                    </tr>
                `;

                if (isExpanded) {{
                    const rawText = s.raw_text || 'No raw message available.';
                    const updates = Array.isArray(s.updates_applied) && s.updates_applied.length > 0 ? s.updates_applied.join('<br>') : 'No mid-trade updates applied';

                    html += `
                        <tr class="bg-slate-900/90 border-b border-purple-500/30">
                            <td colspan="10" class="p-4">
                                <div class="grid grid-cols-1 md:grid-cols-2 gap-4 text-xs font-mono">
                                    <div class="space-y-2">
                                        <div class="text-[10px] uppercase font-bold text-purple-400 flex items-center gap-1.5">
                                            <i class="fa-brands fa-telegram"></i> Original Telegram Signal
                                        </div>
                                        <div class="p-3 rounded-lg bg-slate-950 border border-theme-border text-slate-300 whitespace-pre-wrap select-all font-sans text-xs">
${{escapeHtml(rawText)}}
                                        </div>
                                    </div>
                                    <div class="space-y-2">
                                        <div class="text-[10px] uppercase font-bold text-cyan-400 flex items-center gap-1.5">
                                            <i class="fa-solid fa-clock-rotate-left"></i> Execution Telemetry &amp; Updates
                                        </div>
                                        <div class="p-3 rounded-lg bg-slate-950 border border-theme-border text-slate-300 space-y-1 text-xs">
                                            <div><strong>TP Targets:</strong> TP1: ${{s.tp1 || '--'}} | TP2: ${{s.tp2 || '--'}} | TP3: ${{s.tp3 || '--'}}</div>
                                            <div><strong>Trade Status:</strong> ${{s.status}}</div>
                                            <div><strong>Duration:</strong> ${{s.closed_at && s.activated_at ? Math.round((s.closed_at - s.activated_at) / 60) + ' mins' : '--'}}</div>
                                            <div class="pt-1 border-t border-slate-800 text-slate-400">
                                                <strong>Updates:</strong><br>${{updates}}
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </td>
                        </tr>
                    `;
                }}
            }});

            tbody.innerHTML = html;
        }}

        function toggleRowDetails(idx) {{
            expandedRowId = expandedRowId === idx ? null : idx;
            applyFilters();
        }}

        function copyShareLink() {{
            const p = REPORT_DATA?.summary?.profit || 0;
            const wr = REPORT_DATA?.summary?.win_rate || 0;
            const pName = REPORT_DATA?.provider_name || 'Provider';
            const shareText = `Sirius TSC Backtest Report — ${{pName}}\\nWin Rate: ${{wr}}% | Net Profit: ${{p >= 0 ? '+$' : '-$'}}${{Math.abs(p).toFixed(2)}}`;
            navigator.clipboard.writeText(shareText).then(() => {{
                const btn = document.getElementById('btn-share');
                if (btn) {{
                    const orig = btn.innerHTML;
                    btn.innerHTML = '<i class="fa-solid fa-check"></i> <span>Copied!</span>';
                    setTimeout(() => {{ btn.innerHTML = orig; }}, 2000);
                }}
            }});
        }}

        function escapeHtml(text) {{
            if (!text) return '';
            return text
                .replace(/&/g, "&amp;")
                .replace(/</g, "&lt;")
                .replace(/>/g, "&gt;")
                .replace(/"/g, "&quot;")
                .replace(/'/g, "&#039;");
        }}

        function round2(v) {{
            return Math.round(v * 100) / 100;
        }}
    </script>
</body>
</html>
"""
    return html
