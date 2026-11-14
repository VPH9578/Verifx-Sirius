// =========================================================================
// SiriusBridge.cs — Local TCP Bridge cBot for Sirius Signal Copier & Backtester
// =========================================================================
// HOW TO USE IN VARIANSE cTRADER DESKTOP (Takes 60 Seconds):
//   1. Open your Varianse cTrader Desktop app and click "Automate" on the left menu.
//   2. Click "+ New" (New cBot) and name it: SiriusBridge
//   3. Select all the default code in the editor, delete it, and PASTE this entire file.
//   4. Click "Build" at the top of the code editor (you will see "Build succeeded").
//   5. Click "+ Add Instance" on SiriusBridge (attach to any chart, e.g. XAUUSD) and press Play!
//
// Once playing, Sirius Copier on your PC (http://127.0.0.1:7770) automatically
// connects to this cBot over local TCP port 8888 — NO Spotware API approval needed!
// =========================================================================

using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Text.Json;
using cAlgo.API;
using cAlgo.API.Internals;

namespace cAlgo.Robots
{
    [Robot(TimeZone = TimeZones.UTC, AccessRights = AccessRights.FullAccess)]
    public class SiriusBridge : Robot
    {
        [Parameter("Local Bridge Port", DefaultValue = 8888, MinValue = 1024, MaxValue = 65535)]
        public int Port { get; set; }

        private TcpListener _listener;
        private Thread _listenerThread;
        private volatile bool _isRunning;

        // Thread-safe queue so network requests execute safely on cTrader's main Automate thread
        private readonly ConcurrentQueue<BridgeCommand> _commandQueue = new ConcurrentQueue<BridgeCommand>();

        private class BridgeCommand
        {
            public string RawJson { get; set; }
            public ManualResetEventSlim DoneEvent { get; } = new ManualResetEventSlim(false);
            public string ResponseJson { get; set; } = "{\"ok\":false,\"error\":\"Timed out\"}";
        }

        protected override void OnStart()
        {
            _isRunning = true;
            // Poll the command queue every 20 milliseconds on the main cTrader thread for instant execution
            Timer.Start(TimeSpan.FromMilliseconds(20));

            _listenerThread = new Thread(ListenLoop)
            {
                IsBackground = true,
                Name = "SiriusBridgeTcpListener"
            };
            _listenerThread.Start();

            Print("[SiriusBridge] Started on 127.0.0.1:{0} — Account #{1} ({2})", Port, Account.Number, Account.BrokerName);
            Chart.DrawStaticText(
                "SiriusStatus",
                string.Format("🟢 Sirius Bridge Active (Port {0}) | Account #{1}", Port, Account.Number),
                VerticalAlignment.Top,
                HorizontalAlignment.Left,
                Color.LimeGreen
            );
        }

        protected override void OnTimer()
        {
            while (_commandQueue.TryDequeue(out BridgeCommand item))
            {
                try
                {
                    item.ResponseJson = ExecuteOnMainThread(item.RawJson);
                }
                catch (Exception ex)
                {
                    item.ResponseJson = JsonSerializer.Serialize(new Dictionary<string, object>
                    {
                        ["ok"] = false,
                        ["error"] = ex.Message
                    });
                }
                finally
                {
                    item.DoneEvent.Set();
                }
            }
        }

        protected override void OnStop()
        {
            _isRunning = false;
            try
            {
                _listener?.Stop();
            }
            catch { }
            Print("[SiriusBridge] Stopped.");
        }

        private void ListenLoop()
        {
            try
            {
                _listener = new TcpListener(IPAddress.Loopback, Port);
                _listener.Start();

                while (_isRunning)
                {
                    if (!_listener.Pending())
                    {
                        Thread.Sleep(15);
                        continue;
                    }

                    TcpClient client = _listener.AcceptTcpClient();
                    ThreadPool.QueueUserWorkItem(_ => HandleClient(client));
                }
            }
            catch (Exception ex)
            {
                if (_isRunning)
                    Print("[SiriusBridge] Listener error: {0}", ex.Message);
            }
        }

        private void HandleClient(TcpClient client)
        {
            using (client)
            {
                client.NoDelay = true;
                client.ReceiveTimeout = 30000;
                client.SendTimeout = 15000;

                try
                {
                    using (NetworkStream stream = client.GetStream())
                    using (StreamReader reader = new StreamReader(stream, new UTF8Encoding(false)))
                    using (StreamWriter writer = new StreamWriter(stream, new UTF8Encoding(false)) { AutoFlush = true })
                    {
                        while (_isRunning && client.Connected)
                        {
                            string line = reader.ReadLine();
                            if (line == null)
                                break;
                            if (string.IsNullOrWhiteSpace(line))
                                continue;

                            var cmd = new BridgeCommand { RawJson = line };
                            _commandQueue.Enqueue(cmd);

                            // Wait up to 15 seconds for main cTrader thread to execute the command
                            cmd.DoneEvent.Wait(15000);
                            writer.WriteLine(cmd.ResponseJson);
                        }
                    }
                }
                catch { }
            }
        }

        private string ExecuteOnMainThread(string rawJson)
        {
            using (JsonDocument doc = JsonDocument.Parse(rawJson))
            {
                JsonElement root = doc.RootElement;
                string cmd = root.TryGetProperty("cmd", out JsonElement cEl) ? (cEl.GetString() ?? "").ToUpperInvariant() : "PING";

                switch (cmd)
                {
                    case "PING":
                    case "GET_STATE":
                        return HandleGetState();

                    case "GET_SYMBOLS":
                        return HandleGetSymbols();

                    case "GET_SYMBOL_INFO":
                        return HandleGetSymbolInfo(root);

                    case "GET_TICK":
                        return HandleGetTick(root);

                    case "PLACE_ORDER":
                        return HandlePlaceOrder(root);

                    case "MODIFY_POSITION":
                        return HandleModifyPosition(root);

                    case "CLOSE_POSITION":
                        return HandleClosePosition(root);

                    case "CANCEL_ORDER":
                        return HandleCancelOrder(root);

                    case "GET_TRENDBARS":
                        return HandleGetTrendbars(root);

                    case "GET_TICKS":
                        return HandleGetTicks(root);

                    case "GET_HISTORY":
                        return HandleGetHistory(root);

                    default:
                        return JsonSerializer.Serialize(new Dictionary<string, object>
                        {
                            ["ok"] = false,
                            ["error"] = "Unknown command: " + cmd
                        });
                }
            }
        }

        private string HandleGetState()
        {
            var posList = new List<Dictionary<string, object>>();
            foreach (var p in Positions)
            {
                posList.Add(new Dictionary<string, object>
                {
                    ["ticket"] = p.Id,
                    ["symbol"] = p.SymbolName,
                    ["type"] = p.TradeType == TradeType.Buy ? 0 : 1,
                    ["volume"] = Math.Round(p.Quantity, 2),
                    ["price_open"] = p.EntryPrice,
                    ["price_current"] = p.CurrentPrice,
                    ["sl"] = p.StopLoss ?? 0.0,
                    ["tp"] = p.TakeProfit ?? 0.0,
                    ["profit"] = Math.Round(p.NetProfit, 2),
                    ["swap"] = Math.Round(p.Swap, 2),
                    ["commission"] = Math.Round(p.Commissions, 2),
                    ["label"] = p.Label ?? "",
                    ["comment"] = p.Comment ?? "",
                    ["time"] = new DateTimeOffset(p.EntryTime, TimeSpan.Zero).ToUnixTimeSeconds()
                });
            }

            var ordList = new List<Dictionary<string, object>>();
            foreach (var o in PendingOrders)
            {
                ordList.Add(new Dictionary<string, object>
                {
                    ["ticket"] = o.Id,
                    ["symbol"] = o.SymbolName,
                    ["type"] = o.TradeType == TradeType.Buy ? 0 : 1,
                    ["volume"] = Math.Round(o.Quantity, 2),
                    ["price"] = o.TargetPrice,
                    ["sl"] = o.StopLoss ?? 0.0,
                    ["tp"] = o.TakeProfit ?? 0.0,
                    ["label"] = o.Label ?? "",
                    ["comment"] = o.Comment ?? ""
                });
            }

            var resp = new Dictionary<string, object>
            {
                ["ok"] = true,
                ["connected"] = true,
                ["login"] = Account.Number,
                ["broker"] = Account.BrokerName ?? "cTrader",
                ["is_live"] = Account.IsLive,
                ["currency"] = Account.Asset?.Name ?? "USD",
                ["balance"] = Math.Round(Account.Balance, 2),
                ["equity"] = Math.Round(Account.Equity, 2),
                ["margin"] = Math.Round(Account.Margin, 2),
                ["margin_free"] = Math.Round(Account.FreeMargin, 2),
                ["profit"] = Math.Round(Account.UnrealizedNetProfit, 2),
                ["leverage"] = Account.PreciseLeverage,
                ["positions"] = posList,
                ["orders"] = ordList,
                ["symbols_count"] = Symbols.Count,
                ["history"] = GetRecentHistoryList(30)
            };
            return JsonSerializer.Serialize(resp);
        }

        private string HandleGetSymbols()
        {
            var names = new List<string>();
            foreach (string sName in Symbols)
            {
                names.Add(sName);
            }
            return JsonSerializer.Serialize(new Dictionary<string, object>
            {
                ["ok"] = true,
                ["symbols"] = names
            });
        }

        private Symbol ResolveBrokerSymbol(string requested)
        {
            if (string.IsNullOrWhiteSpace(requested))
                return null;

            if (Symbols.Exists(requested))
                return Symbols.GetSymbol(requested);

            string cleanReq = requested.ToUpperInvariant().Replace("/", "").Replace("-", "").Replace("_", "").Split('.')[0];

            // Canonical alias mappings for brokers with special naming conventions (e.g. Bitcoin, Ethereum, Solana, USOil.v, UT100.v)
            var aliases = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase)
            {
                ["Bitcoin"] = new[] { "BTCUSD", "BTC", "BITCOIN", "XBTUSD", "BTCUSDT" },
                ["Ethereum"] = new[] { "ETHUSD", "ETH", "ETHEREUM", "ETHUSDT" },
                ["Solana"] = new[] { "SOLUSD", "SOL", "SOLANA", "SOLUSDT" },
                ["Ripple"] = new[] { "XRPUSD", "XRP", "RIPPLE", "XRPUSDT" },
                ["Dogecoin"] = new[] { "DOGEUSD", "DOGE", "DOGECOIN", "DOGEUSDT" },
                ["Cardano"] = new[] { "ADAUSD", "ADA", "CARDANO", "ADAUSDT" },
                ["Polkadot"] = new[] { "DOTUSD", "DOT", "POLKADOT" },
                ["Polygon"] = new[] { "MATICUSD", "MATIC", "POLYGON", "POL" },
                ["Litecoin"] = new[] { "LTCUSD", "LTC", "LITECOIN" },
                ["Chainlink"] = new[] { "LINKUSD", "LINK", "CHAINLINK" },
                ["Bitcoin cash"] = new[] { "BCHUSD", "BCH", "BITCOINCASH" },
                ["XAUUSD"] = new[] { "GOLD", "XAU", "GOLDUSD" },
                ["USOil.v"] = new[] { "USOUSD", "OIL", "WTI", "CRUDE", "USOIL" },
                ["UKOil.v"] = new[] { "UKOIL", "BRENT", "BRENTOIL" },
                ["UT100.v"] = new[] { "NAS100", "NQ", "USTEC", "NDX", "NASDAQ", "UT100" },
                ["US30.v"] = new[] { "US30", "DJ30", "WS30", "DOW" },
                ["US500.v"] = new[] { "US500", "SPX500", "SP500", "SPX" },
                ["DE30.v"] = new[] { "GER40", "GER30", "DAX", "DE30", "DAX40" },
                ["UK100.v"] = new[] { "UK100", "FTSE", "FTSE100" },
                ["JP225.v"] = new[] { "JP225", "NIKKEI" }
            };

            foreach (var kvp in aliases)
            {
                if (Symbols.Exists(kvp.Key))
                {
                    foreach (var a in kvp.Value)
                    {
                        if (string.Equals(cleanReq, a, StringComparison.OrdinalIgnoreCase))
                            return Symbols.GetSymbol(kvp.Key);
                    }
                }
            }

            foreach (string sName in Symbols)
            {
                string cleanCandidate = sName.ToUpperInvariant().Replace("/", "").Replace("-", "").Replace("_", "").Split('.')[0];
                if (cleanCandidate == cleanReq || (cleanReq == "GOLD" && cleanCandidate == "XAUUSD"))
                {
                    return Symbols.GetSymbol(sName);
                }
            }
            return null;
        }

        private string HandleGetSymbolInfo(JsonElement root)
        {
            string symName = root.TryGetProperty("symbol", out JsonElement sEl) ? sEl.GetString() : "";
            Symbol s = ResolveBrokerSymbol(symName);
            if (s == null)
            {
                return JsonSerializer.Serialize(new Dictionary<string, object>
                {
                    ["ok"] = false,
                    ["error"] = "Symbol not found: " + symName
                });
            }

            double spreadPts = s.TickSize > 0 ? Math.Round((s.Ask - s.Bid) / s.TickSize) : 15;
            var resp = new Dictionary<string, object>
            {
                ["ok"] = true,
                ["name"] = s.Name,
                ["digits"] = s.Digits,
                ["point"] = s.TickSize,
                ["pip_size"] = s.PipSize,
                ["lot_size"] = s.LotSize,
                ["volume_min"] = s.VolumeInUnitsToQuantity(s.VolumeInUnitsMin),
                ["volume_max"] = s.VolumeInUnitsToQuantity(s.VolumeInUnitsMax),
                ["volume_step"] = s.VolumeInUnitsToQuantity(s.VolumeInUnitsStep),
                ["tick_size"] = s.TickSize,
                ["tick_value"] = s.TickValue * s.LotSize,
                ["spread"] = (int)Math.Max(1, spreadPts),
                ["bid"] = s.Bid,
                ["ask"] = s.Ask
            };
            return JsonSerializer.Serialize(resp);
        }

        private string HandleGetTick(JsonElement root)
        {
            string symName = root.TryGetProperty("symbol", out JsonElement sEl) ? sEl.GetString() : "";
            Symbol s = ResolveBrokerSymbol(symName);
            if (s == null)
            {
                return JsonSerializer.Serialize(new Dictionary<string, object>
                {
                    ["ok"] = false,
                    ["error"] = "Symbol not found: " + symName
                });
            }

            long tsMs = new DateTimeOffset(Server.TimeInUtc, TimeSpan.Zero).ToUnixTimeMilliseconds();
            return JsonSerializer.Serialize(new Dictionary<string, object>
            {
                ["ok"] = true,
                ["symbol"] = s.Name,
                ["time"] = tsMs / 1000,
                ["time_msc"] = tsMs,
                ["bid"] = s.Bid,
                ["ask"] = s.Ask
            });
        }

        private string HandlePlaceOrder(JsonElement root)
        {
            string symName = root.TryGetProperty("symbol", out JsonElement sEl) ? sEl.GetString() : "";
            Symbol s = ResolveBrokerSymbol(symName);
            if (s == null)
            {
                return JsonSerializer.Serialize(new Dictionary<string, object>
                {
                    ["ok"] = false,
                    ["error"] = "Symbol not found: " + symName
                });
            }

            string dir = root.TryGetProperty("direction", out JsonElement dEl) ? (dEl.GetString() ?? "BUY").ToUpperInvariant() : "BUY";
            string cat = root.TryGetProperty("category", out JsonElement catEl) ? (catEl.GetString() ?? "MARKET").ToUpperInvariant() : "MARKET";
            double lots = root.TryGetProperty("volume_lots", out JsonElement vEl) ? vEl.GetDouble() : 0.01;
            double price = root.TryGetProperty("price", out JsonElement pEl) ? pEl.GetDouble() : 0.0;
            double slPrice = root.TryGetProperty("sl", out JsonElement slEl) ? slEl.GetDouble() : 0.0;
            double tpPrice = root.TryGetProperty("tp", out JsonElement tpEl) ? tpEl.GetDouble() : 0.0;
            string label = root.TryGetProperty("label", out JsonElement lEl) ? (lEl.GetString() ?? "") : "";
            string comment = root.TryGetProperty("comment", out JsonElement cmEl) ? (cmEl.GetString() ?? "") : "";

            TradeType tradeType = dir == "SELL" ? TradeType.Sell : TradeType.Buy;
            double rawUnits = s.QuantityToVolumeInUnits(lots);
            double normalizedUnits = s.NormalizeVolumeInUnits(rawUnits, RoundingMode.ToNearest);
            if (normalizedUnits < s.VolumeInUnitsMin)
                normalizedUnits = s.VolumeInUnitsMin;

            double? absSl = slPrice > 0 ? Math.Round(slPrice, s.Digits) : (double?)null;
            double? absTp = tpPrice > 0 ? Math.Round(tpPrice, s.Digits) : (double?)null;

            TradeResult result;
            if (cat == "LIMIT")
            {
                result = PlaceLimitOrder(tradeType, s.Name, normalizedUnits, price, label, absSl, absTp, ProtectionType.Absolute, (DateTime?)null, comment);
            }
            else if (cat == "STOP")
            {
                result = PlaceStopOrder(tradeType, s.Name, normalizedUnits, price, label, absSl, absTp, ProtectionType.Absolute, (DateTime?)null, comment);
            }
            else
            {
                result = ExecuteMarketOrder(tradeType, s.Name, normalizedUnits, label, (double?)null, (double?)null, comment);
                if (result.IsSuccessful && result.Position != null && (absSl != null || absTp != null))
                {
                    double fill = result.Position.EntryPrice;
                    double? validSl = absSl;
                    double? validTp = absTp;

                    if (validSl != null)
                    {
                        if (tradeType == TradeType.Buy && validSl >= fill) validSl = null;
                        else if (tradeType == TradeType.Sell && validSl <= fill) validSl = null;
                    }
                    if (validTp != null)
                    {
                        if (tradeType == TradeType.Buy && validTp <= fill) validTp = null;
                        else if (tradeType == TradeType.Sell && validTp >= fill) validTp = null;
                    }

                    if (validSl != null || validTp != null)
                    {
                        TradeResult modRes = ModifyPosition(result.Position, validSl, validTp, ProtectionType.Absolute);
                        if (!modRes.IsSuccessful && validSl != null && validTp != null)
                        {
                            ModifyPosition(result.Position, validSl, (double?)null, ProtectionType.Absolute);
                        }
                    }
                }
            }

            if (!result.IsSuccessful)
            {
                return JsonSerializer.Serialize(new Dictionary<string, object>
                {
                    ["ok"] = false,
                    ["error"] = result.Error.ToString()
                });
            }

            long posId = result.Position != null ? result.Position.Id : 0;
            long ordId = result.PendingOrder != null ? result.PendingOrder.Id : 0;
            long ticket = posId > 0 ? posId : ordId;
            double fillPrice = result.Position != null ? result.Position.EntryPrice : (result.PendingOrder != null ? result.PendingOrder.TargetPrice : price);

            return JsonSerializer.Serialize(new Dictionary<string, object>
            {
                ["ok"] = true,
                ["ticket"] = ticket,
                ["position_id"] = posId,
                ["order_id"] = ordId,
                ["symbol"] = s.Name,
                ["volume"] = s.VolumeInUnitsToQuantity(normalizedUnits),
                ["price"] = Math.Round(fillPrice, s.Digits),
                ["action"] = cat == "MARKET" ? "FILLED" : "PENDING"
            });
        }

        private string HandleModifyPosition(JsonElement root)
        {
            long posId = root.TryGetProperty("position_id", out JsonElement idEl) ? idEl.GetInt64() : 0;
            double sl = root.TryGetProperty("sl", out JsonElement slEl) ? slEl.GetDouble() : 0.0;
            double tp = root.TryGetProperty("tp", out JsonElement tpEl) ? tpEl.GetDouble() : 0.0;

            Position pos = null;
            foreach (var p in Positions)
            {
                if (p.Id == posId)
                {
                    pos = p;
                    break;
                }
            }
            if (pos == null)
            {
                return JsonSerializer.Serialize(new Dictionary<string, object>
                {
                    ["ok"] = false,
                    ["error"] = "Position not found: " + posId
                });
            }

            double? newSl = sl > 0 ? sl : pos.StopLoss;
            double? newTp = tp > 0 ? tp : pos.TakeProfit;
            TradeResult res = ModifyPosition(pos, newSl, newTp, ProtectionType.Absolute);
            return JsonSerializer.Serialize(new Dictionary<string, object>
            {
                ["ok"] = res.IsSuccessful,
                ["error"] = res.IsSuccessful ? "" : res.Error.ToString()
            });
        }

        private string HandleClosePosition(JsonElement root)
        {
            long posId = root.TryGetProperty("position_id", out JsonElement idEl) ? idEl.GetInt64() : 0;
            Position pos = null;
            foreach (var p in Positions)
            {
                if (p.Id == posId)
                {
                    pos = p;
                    break;
                }
            }
            if (pos == null)
            {
                return JsonSerializer.Serialize(new Dictionary<string, object>
                {
                    ["ok"] = false,
                    ["error"] = "Position not found: " + posId
                });
            }

            TradeResult res = ClosePosition(pos);
            return JsonSerializer.Serialize(new Dictionary<string, object>
            {
                ["ok"] = res.IsSuccessful,
                ["error"] = res.IsSuccessful ? "" : res.Error.ToString()
            });
        }

        private string HandleCancelOrder(JsonElement root)
        {
            long ordId = root.TryGetProperty("order_id", out JsonElement idEl) ? idEl.GetInt64() : 0;
            PendingOrder target = null;
            foreach (var o in PendingOrders)
            {
                if (o.Id == ordId)
                {
                    target = o;
                    break;
                }
            }
            if (target == null)
            {
                return JsonSerializer.Serialize(new Dictionary<string, object>
                {
                    ["ok"] = false,
                    ["error"] = "PendingOrder not found: " + ordId
                });
            }

            TradeResult res = CancelPendingOrder(target);
            return JsonSerializer.Serialize(new Dictionary<string, object>
            {
                ["ok"] = res.IsSuccessful,
                ["error"] = res.IsSuccessful ? "" : res.Error.ToString()
            });
        }

        private string HandleGetTrendbars(JsonElement root)
        {
            string symName = root.TryGetProperty("symbol", out JsonElement sEl) ? sEl.GetString() : "";
            string tfStr = root.TryGetProperty("timeframe", out JsonElement tfEl) ? (tfEl.GetString() ?? "M1").ToUpperInvariant() : "M1";
            long fromSec = root.TryGetProperty("from_ts", out JsonElement fEl) ? fEl.GetInt64() : 0;
            long toSec = root.TryGetProperty("to_ts", out JsonElement tEl) ? tEl.GetInt64() : 0;

            Symbol s = ResolveBrokerSymbol(symName);
            if (s == null)
            {
                return JsonSerializer.Serialize(new Dictionary<string, object>
                {
                    ["ok"] = false,
                    ["error"] = "Symbol not found"
                });
            }

            TimeFrame tf = TimeFrame.Minute;
            if (tfStr == "M5") tf = TimeFrame.Minute5;
            else if (tfStr == "M15") tf = TimeFrame.Minute15;
            else if (tfStr == "H1") tf = TimeFrame.Hour;

            Bars bars = MarketData.GetBars(tf, s.Name);
            DateTime fromDt = DateTimeOffset.FromUnixTimeSeconds(fromSec).UtcDateTime;

            int safetyLoads = 0;
            while (bars.Count > 0 && bars.OpenTimes[0] > fromDt && safetyLoads < 25)
            {
                int loaded = bars.LoadMoreHistory();
                if (loaded <= 0)
                    break;
                safetyLoads++;
            }

            var list = new List<Dictionary<string, object>>();
            for (int i = 0; i < bars.Count; i++)
            {
                long ts = new DateTimeOffset(bars.OpenTimes[i], TimeSpan.Zero).ToUnixTimeSeconds();
                if (ts < fromSec)
                    continue;
                if (toSec > 0 && ts > toSec)
                    break;

                list.Add(new Dictionary<string, object>
                {
                    ["time"] = ts,
                    ["open"] = Math.Round(bars.OpenPrices[i], s.Digits),
                    ["high"] = Math.Round(bars.HighPrices[i], s.Digits),
                    ["low"] = Math.Round(bars.LowPrices[i], s.Digits),
                    ["close"] = Math.Round(bars.ClosePrices[i], s.Digits)
                });
            }

            return JsonSerializer.Serialize(new Dictionary<string, object>
            {
                ["ok"] = true,
                ["bars"] = list
            });
        }

        private string HandleGetTicks(JsonElement root)
        {
            string symName = root.TryGetProperty("symbol", out JsonElement sEl) ? sEl.GetString() : "";
            long fromSec = root.TryGetProperty("from_ts", out JsonElement fEl) ? fEl.GetInt64() : 0;
            long toSec = root.TryGetProperty("to_ts", out JsonElement tEl) ? tEl.GetInt64() : 0;

            Symbol s = ResolveBrokerSymbol(symName);
            if (s == null)
            {
                return JsonSerializer.Serialize(new Dictionary<string, object>
                {
                    ["ok"] = false,
                    ["error"] = "Symbol not found"
                });
            }

            Ticks ticks = MarketData.GetTicks(s.Name);
            DateTime fromDt = DateTimeOffset.FromUnixTimeSeconds(fromSec).UtcDateTime;

            int safetyLoads = 0;
            while (ticks.Count > 0 && ticks[0].Time > fromDt && safetyLoads < 25)
            {
                int loaded = ticks.LoadMoreHistory();
                if (loaded <= 0)
                    break;
                safetyLoads++;
            }

            var list = new List<Dictionary<string, object>>();
            for (int i = 0; i < ticks.Count; i++)
            {
                Tick tk = ticks[i];
                long ms = new DateTimeOffset(tk.Time, TimeSpan.Zero).ToUnixTimeMilliseconds();
                long sec = ms / 1000;
                if (sec < fromSec)
                    continue;
                if (toSec > 0 && sec > toSec)
                    break;

                list.Add(new Dictionary<string, object>
                {
                    ["time"] = sec,
                    ["time_msc"] = ms,
                    ["bid"] = Math.Round(tk.Bid, s.Digits),
                    ["ask"] = Math.Round(tk.Ask, s.Digits)
                });

                if (list.Count >= 60000)
                    break;
            }

            return JsonSerializer.Serialize(new Dictionary<string, object>
            {
                ["ok"] = true,
                ["ticks"] = list
            });
        }
    }
}
