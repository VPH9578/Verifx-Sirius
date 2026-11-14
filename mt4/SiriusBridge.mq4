//+------------------------------------------------------------------+
//|                                                 SiriusBridge.mq4 |
//|              High-Speed Local TCP Bridge EA for Sirius Copier    |
//|               Connects MT4 to Sirius Web Dashboard & Copier      |
//+------------------------------------------------------------------+
// HOW TO USE IN METATRADER 4 (Takes 60 Seconds):
//   1. In MT4, click File -> Open Data Folder.
//   2. Navigate to MQL4 -> Experts, and save/paste this file as SiriusBridge.mq4.
//   3. In MT4 Navigator window, right-click 'Expert Advisors' and click 'Refresh'.
//   4. In MT4 menu, click Tools -> Options -> Expert Advisors:
//      - Check 'Allow automated trading'
//      - Check 'Allow DLL imports'
//   5. Drag SiriusBridge onto ANY chart (e.g. EURUSD M1) and click OK.
//   6. Sirius Copier connects automatically over 127.0.0.1:9001!
//+------------------------------------------------------------------+
#property copyright "Sirius Copier Ecosystem"
#property link      "http://127.0.0.1:7770"
#property version   "1.00"
#property strict

#include <stdlib.mqh>

#import "ws2_32.dll"
   int WSAStartup(int wVersionRequested, int &lpWSAData[]);
   int WSACleanup();
   int WSAGetLastError();
   int socket(int af, int type, int protocol);
   int connect(int s, int &name[], int namelen);
   int send(int s, uchar &buf[], int len, int flags);
   int recv(int s, uchar &buf[], int len, int flags);
   int closesocket(int s);
   int ioctlsocket(int s, int cmd, int &argp[]);
   int htons(int hostshort);
   int inet_addr(uchar &cp[]);
#import

#define AF_INET        2
#define SOCK_STREAM    1
#define IPPROTO_TCP    6
#define FIONBIO        -2147195266
#define WSAEWOULDBLOCK 10035

//--- Inputs
input string InpHost       = "127.0.0.1"; // Sirius Local Host
input int    InpPort       = 9001;        // Local Bridge Port (matches Sirius account port)
input int    InpSlippage   = 20;          // Max Slippage (points)
input bool   InpECNMode    = true;        // ECN Mode (modify SL/TP after market entry)

//--- Global Variables
int      g_socket = -1;
bool     g_wsa_initialized = false;
uint     g_last_connect_attempt = 0;
string   g_recv_buffer = "";
bool     g_is_connected = false;

//+------------------------------------------------------------------+
//| Initialize Winsock 2.2 stack                                     |
//+------------------------------------------------------------------+
bool InitWinsock()
{
   if(g_wsa_initialized) return true;
   int wsaData[100];
   ArrayInitialize(wsaData, 0);
   int res = WSAStartup(0x0202, wsaData);
   if(res != 0)
   {
      PrintFormat("[SiriusBridge] WSAStartup failed with code %d", res);
      return false;
   }
   g_wsa_initialized = true;
   return true;
}

//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit()
{
   EventSetMillisecondTimer(25);
   UpdateChartBanner("Connecting to Sirius Copier on " + InpHost + ":" + IntegerToString(InpPort) + "...", clrGold);
   ConnectToSirius();
   return(INIT_SUCCEEDED);
}

//+------------------------------------------------------------------+
//| Expert deinitialization function                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
   EventKillTimer();
   CloseSocket();
   if(g_wsa_initialized)
   {
      WSACleanup();
      g_wsa_initialized = false;
   }
   ObjectDelete(0, "SiriusMT4Status");
   Print("[SiriusBridge] EA stopped.");
}

//+------------------------------------------------------------------+
//| Expert timer function (25ms execution loop)                      |
//+------------------------------------------------------------------+
void OnTimer()
{
   // Check connection
   if(g_socket == -1 || !g_is_connected)
   {
      g_is_connected = false;
      uint now_tick = GetTickCount();
      if(now_tick - g_last_connect_attempt >= 2000 || g_last_connect_attempt == 0)
      {
         CloseSocket();
         ConnectToSirius();
      }
      return;
   }

   // Socket is connected - read incoming commands non-blocking
   uchar buffer[];
   ArrayResize(buffer, 8192);
   int read = recv(g_socket, buffer, 8191, 0);
   if(read > 0)
   {
      buffer[read] = 0;
      string chunk = CharArrayToString(buffer, 0, read, CP_UTF8);
      g_recv_buffer += chunk;

      // Process newline-delimited JSON commands
      int newline_pos = StringFind(g_recv_buffer, "\n");
      while(newline_pos >= 0)
      {
         string line = StringSubstr(g_recv_buffer, 0, newline_pos);
         g_recv_buffer = StringSubstr(g_recv_buffer, newline_pos + 1);
         line = StringTrim(line);

         if(StringLen(line) > 0)
         {
            string response = ProcessCommand(line);
            if(StringLen(response) > 0)
            {
               SendResponse(response + "\n");
            }
         }
         newline_pos = StringFind(g_recv_buffer, "\n");
      }
   }
   else if(read == 0)
   {
      // Remote peer cleanly closed connection
      CloseSocket();
   }
   else
   {
      // read < 0: check error code
      int werr = WSAGetLastError();
      if(werr != WSAEWOULDBLOCK && werr != 0)
      {
         // Fatal socket error, reset
         CloseSocket();
      }
   }
}

//+------------------------------------------------------------------+
//| Connect socket to Sirius Python process                          |
//+------------------------------------------------------------------+
bool ConnectToSirius()
{
   g_last_connect_attempt = GetTickCount();
   if(!InitWinsock())
   {
      UpdateChartBanner("Winsock init failed", clrCrimson);
      return false;
   }

   CloseSocket();
   g_socket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
   if(g_socket == -1)
   {
      UpdateChartBanner("Socket creation failed", clrCrimson);
      return false;
   }

   uchar ip_bytes[];
   StringToCharArray(InpHost, ip_bytes);
   int ip = inet_addr(ip_bytes);

   int sin_port = htons(InpPort);
   int sockaddr[4];
   ArrayInitialize(sockaddr, 0);
   sockaddr[0] = AF_INET | (sin_port << 16);
   sockaddr[1] = ip;

   int res = connect(g_socket, sockaddr, 16);
   if(res != 0)
   {
      CloseSocket();
      UpdateChartBanner("Waiting for Sirius Copier on Port " + IntegerToString(InpPort) + "...", clrDarkGray);
      return false;
   }

   // Set socket to non-blocking mode
   int nonblock[1];
   nonblock[0] = 1;
   ioctlsocket(g_socket, FIONBIO, nonblock);

   g_is_connected = true;
   g_recv_buffer = "";
   PrintFormat("[SiriusBridge] Connected to Sirius Copier on %s:%d - Account #%d (%s)", InpHost, InpPort, AccountNumber(), AccountCompany());
   UpdateChartBanner(StringFormat("Sirius Bridge Active (Port %d) | Account #%d (%s)", InpPort, AccountNumber(), AccountCompany()), clrLimeGreen);
   return true;
}

//+------------------------------------------------------------------+
//| Close socket safely                                              |
//+------------------------------------------------------------------+
void CloseSocket()
{
   if(g_socket != -1)
   {
      closesocket(g_socket);
      g_socket = -1;
   }
   g_is_connected = false;
}

//+------------------------------------------------------------------+
//| Send response line to Sirius                                     |
//+------------------------------------------------------------------+
bool SendResponse(string text)
{
   if(g_socket == -1 || !g_is_connected)
      return false;

   uchar buffer[];
   int len = StringToCharArray(text, buffer, 0, WHOLE_ARRAY, CP_UTF8);
   if(len <= 1) return false;
   int sent = send(g_socket, buffer, len - 1, 0);
   return (sent == len - 1);
}

//+------------------------------------------------------------------+
//| Dispatch command to handler                                      |
//+------------------------------------------------------------------+
string ProcessCommand(string raw_json)
{
   string cmd = GetJsonString(raw_json, "cmd");
   StringToUpper(cmd);

   if(cmd == "PING")
      return HandlePing();
   else if(cmd == "GET_STATE")
      return HandleGetState();
   else if(cmd == "GET_SYMBOLS")
      return HandleGetSymbols();
   else if(cmd == "GET_SYMBOL_INFO")
      return HandleGetSymbolInfo(raw_json);
   else if(cmd == "GET_TICK")
      return HandleGetTick(raw_json);
   else if(cmd == "PLACE_ORDER")
      return HandlePlaceOrder(raw_json);
   else if(cmd == "MODIFY_POSITION")
      return HandleModifyPosition(raw_json);
   else if(cmd == "CLOSE_POSITION")
      return HandleClosePosition(raw_json);
   else if(cmd == "CANCEL_ORDER")
      return HandleCancelOrder(raw_json);
   else if(cmd == "GET_HISTORY")
      return HandleGetHistory(raw_json);

   return "{\"ok\":false,\"error\":\"Unknown command: " + cmd + "\"}";
}

//+------------------------------------------------------------------+
//| Command: PING                                                    |
//+------------------------------------------------------------------+
string HandlePing()
{
   return StringFormat(
      "{\"ok\":true,\"login\":%d,\"broker\":\"%s\",\"server\":\"%s\",\"balance\":%.2f,\"equity\":%.2f,\"currency\":\"%s\",\"leverage\":%d}",
      AccountNumber(),
      AccountCompany(),
      AccountServer(),
      AccountBalance(),
      AccountEquity(),
      AccountCurrency(),
      AccountLeverage()
   );
}

//+------------------------------------------------------------------+
//| Command: GET_STATE                                               |
//+------------------------------------------------------------------+
string HandleGetState()
{
   string pos_json = "[";
   int pos_count = 0;

   string ord_json = "[";
   int ord_count = 0;

   int total = OrdersTotal();
   for(int i = 0; i < total; i++)
   {
      if(!OrderSelect(i, SELECT_BY_POS, MODE_TRADES))
         continue;

      int type = OrderType();
      if(type == OP_BUY || type == OP_SELL)
      {
         if(pos_count > 0) pos_json += ",";
         pos_json += StringFormat(
            "{\"ticket\":%d,\"symbol\":\"%s\",\"type\":%d,\"volume\":%.2f,\"price_open\":%.5f,\"price_current\":%.5f,\"sl\":%.5f,\"tp\":%.5f,\"profit\":%.2f,\"swap\":%.2f,\"commission\":%.2f,\"magic\":%d,\"comment\":\"%s\",\"time\":%d}",
            OrderTicket(),
            OrderSymbol(),
            type,
            OrderLots(),
            OrderOpenPrice(),
            OrderClosePrice(),
            OrderStopLoss(),
            OrderTakeProfit(),
            OrderProfit(),
            OrderSwap(),
            OrderCommission(),
            OrderMagicNumber(),
            EscapeJson(OrderComment()),
            (int)OrderOpenTime()
         );
         pos_count++;
      }
      else
      {
         if(ord_count > 0) ord_json += ",";
         ord_json += StringFormat(
            "{\"ticket\":%d,\"symbol\":\"%s\",\"type\":%d,\"volume\":%.2f,\"price\":%.5f,\"sl\":%.5f,\"tp\":%.5f,\"magic\":%d,\"comment\":\"%s\"}",
            OrderTicket(),
            OrderSymbol(),
            type,
            OrderLots(),
            OrderOpenPrice(),
            OrderStopLoss(),
            OrderTakeProfit(),
            OrderMagicNumber(),
            EscapeJson(OrderComment())
         );
         ord_count++;
      }
   }

   pos_json += "]";
   ord_json += "]";

   return StringFormat(
      "{\"ok\":true,\"login\":%d,\"broker\":\"%s\",\"server\":\"%s\",\"balance\":%.2f,\"equity\":%.2f,\"margin\":%.2f,\"margin_free\":%.2f,\"profit\":%.2f,\"currency\":\"%s\",\"leverage\":%d,\"positions\":%s,\"orders\":%s}",
      AccountNumber(),
      AccountCompany(),
      AccountServer(),
      AccountBalance(),
      AccountEquity(),
      AccountMargin(),
      AccountFreeMargin(),
      AccountEquity() - AccountBalance(),
      AccountCurrency(),
      AccountLeverage(),
      pos_json,
      ord_json
   );
}

//+------------------------------------------------------------------+
//| Command: GET_SYMBOLS                                             |
//+------------------------------------------------------------------+
string HandleGetSymbols()
{
   string sym_json = "[";
   int total = SymbolsTotal(false);
   int added = 0;

   // If Market Watch has symbols, pull those first
   int mw_total = SymbolsTotal(true);
   int scan_total = (mw_total > 5) ? mw_total : total;
   bool use_mw = (mw_total > 5);

   for(int i = 0; i < scan_total; i++)
   {
      string name = SymbolName(i, use_mw);
      if(StringLen(name) > 0)
      {
         if(added > 0) sym_json += ",";
         sym_json += "\"" + name + "\"";
         added++;
      }
   }
   sym_json += "]";

   return "{\"ok\":true,\"symbols\":" + sym_json + "}";
}

//+------------------------------------------------------------------+
//| Command: GET_SYMBOL_INFO                                         |
//+------------------------------------------------------------------+
string HandleGetSymbolInfo(string raw_json)
{
   string symbol = GetJsonString(raw_json, "symbol");
   if(StringLen(symbol) == 0) symbol = Symbol();

   int digits = (int)MarketInfo(symbol, MODE_DIGITS);
   double point = MarketInfo(symbol, MODE_POINT);
   double lot_size = MarketInfo(symbol, MODE_LOTSIZE);
   double min_lot = MarketInfo(symbol, MODE_MINLOT);
   double max_lot = MarketInfo(symbol, MODE_MAXLOT);
   double lot_step = MarketInfo(symbol, MODE_LOTSTEP);
   double tick_val = MarketInfo(symbol, MODE_TICKVALUE);
   double tick_size = MarketInfo(symbol, MODE_TICKSIZE);
   int spread = (int)MarketInfo(symbol, MODE_SPREAD);

   if(lot_size <= 0) lot_size = 100000;
   if(min_lot <= 0) min_lot = 0.01;
   if(lot_step <= 0) lot_step = 0.01;

   return StringFormat(
      "{\"ok\":true,\"symbol\":\"%s\",\"digits\":%d,\"point\":%.6f,\"lot_size\":%.0f,\"volume_min\":%.2f,\"volume_max\":%.2f,\"volume_step\":%.2f,\"tick_value\":%.4f,\"tick_size\":%.6f,\"spread\":%d}",
      symbol, digits, point, lot_size, min_lot, max_lot, lot_step, tick_val, tick_size, spread
   );
}

//+------------------------------------------------------------------+
//| Command: GET_TICK                                                |
//+------------------------------------------------------------------+
string HandleGetTick(string raw_json)
{
   string symbol = GetJsonString(raw_json, "symbol");
   if(StringLen(symbol) == 0) symbol = Symbol();

   double bid = MarketInfo(symbol, MODE_BID);
   double ask = MarketInfo(symbol, MODE_ASK);
   datetime t = TimeCurrent();

   return StringFormat("{\"ok\":true,\"symbol\":\"%s\",\"bid\":%.5f,\"ask\":%.5f,\"time\":%d,\"time_msc\":%d}",
      symbol, bid, ask, (int)t, (int)t * 1000
   );
}

//+------------------------------------------------------------------+
//| Command: PLACE_ORDER                                             |
//+------------------------------------------------------------------+
string HandlePlaceOrder(string raw_json)
{
   string symbol    = GetJsonString(raw_json, "symbol");
   string type_str  = GetJsonString(raw_json, "type");
   double volume    = NormalizeDouble(GetJsonDouble(raw_json, "volume"), 2);
   double req_price = GetJsonDouble(raw_json, "price");
   double sl        = GetJsonDouble(raw_json, "sl");
   double tp        = GetJsonDouble(raw_json, "tp");
   int    magic     = (int)GetJsonDouble(raw_json, "magic");
   string comment   = GetJsonString(raw_json, "comment");

   if(StringLen(symbol) == 0) symbol = Symbol();
   int digits = (int)MarketInfo(symbol, MODE_DIGITS);
   req_price = NormalizeDouble(req_price, digits);
   sl = NormalizeDouble(sl, digits);
   tp = NormalizeDouble(tp, digits);

   StringToUpper(type_str);
   int cmd = OP_BUY;
   double open_price = MarketInfo(symbol, MODE_ASK);

   if(type_str == "BUY" || type_str == "MARKET_BUY")
   {
      cmd = OP_BUY;
      open_price = MarketInfo(symbol, MODE_ASK);
   }
   else if(type_str == "SELL" || type_str == "MARKET_SELL")
   {
      cmd = OP_SELL;
      open_price = MarketInfo(symbol, MODE_BID);
   }
   else if(type_str == "BUY_LIMIT" || type_str == "BUYLIMIT")
   {
      cmd = OP_BUYLIMIT;
      open_price = req_price;
   }
   else if(type_str == "SELL_LIMIT" || type_str == "SELLLIMIT")
   {
      cmd = OP_SELLLIMIT;
      open_price = req_price;
   }
   else if(type_str == "BUY_STOP" || type_str == "BUYSTOP")
   {
      cmd = OP_BUYSTOP;
      open_price = req_price;
   }
   else if(type_str == "SELL_STOP" || type_str == "SELLSTOP")
   {
      cmd = OP_SELLSTOP;
      open_price = req_price;
   }

   // ECN Handling: On ECN brokers, market orders must open with SL/TP = 0 first, then modified
   double init_sl = (InpECNMode && (cmd == OP_BUY || cmd == OP_SELL)) ? 0.0 : sl;
   double init_tp = (InpECNMode && (cmd == OP_BUY || cmd == OP_SELL)) ? 0.0 : tp;

   ResetLastError();
   int ticket = OrderSend(symbol, cmd, volume, open_price, InpSlippage, init_sl, init_tp, comment, magic, 0, (cmd == OP_BUY ? clrDodgerBlue : clrCrimson));

   // If failed due to error 130 (Invalid Stops), retry immediately with 0 stops
   if(ticket < 0 && GetLastError() == 130)
   {
      ResetLastError();
      ticket = OrderSend(symbol, cmd, volume, open_price, InpSlippage, 0.0, 0.0, comment, magic, 0, (cmd == OP_BUY || cmd == OP_BUYLIMIT || cmd == OP_BUYSTOP ? clrDodgerBlue : clrCrimson));
   }

   if(ticket < 0)
   {
      int err = GetLastError();
      return StringFormat("{\"ok\":false,\"error\":\"OrderSend failed (Error %d: %s)\",\"retcode\":%d}", err, ErrorDescription(err), err);
   }

   // Select order and verify open price
   if(OrderSelect(ticket, SELECT_BY_TICKET))
   {
      open_price = OrderOpenPrice();

      // If ECN mode and SL/TP were specified, modify now
      if((sl > 0.0 || tp > 0.0) && (OrderStopLoss() != sl || OrderTakeProfit() != tp))
      {
         bool mod = OrderModify(ticket, open_price, sl, tp, 0, clrBlue);
         if(!mod)
         {
            PrintFormat("[SiriusBridge] Notice: Post-entry SL/TP modify for #%d failed (Error %d)", ticket, GetLastError());
         }
      }
   }

   return StringFormat(
      "{\"ok\":true,\"ticket\":%d,\"symbol\":\"%s\",\"volume\":%.2f,\"price\":%.5f,\"sl\":%.5f,\"tp\":%.5f,\"magic\":%d}",
      ticket, symbol, volume, open_price, sl, tp, magic
   );
}

//+------------------------------------------------------------------+
//| Command: MODIFY_POSITION                                         |
//+------------------------------------------------------------------+
string HandleModifyPosition(string raw_json)
{
   int ticket = (int)GetJsonDouble(raw_json, "ticket");
   double sl  = GetJsonDouble(raw_json, "sl");
   double tp  = GetJsonDouble(raw_json, "tp");

   if(!OrderSelect(ticket, SELECT_BY_TICKET))
   {
      return StringFormat("{\"ok\":false,\"error\":\"Ticket #%d not found\"}", ticket);
   }

   int digits = (int)MarketInfo(OrderSymbol(), MODE_DIGITS);
   sl = NormalizeDouble(sl, digits);
   tp = NormalizeDouble(tp, digits);

   ResetLastError();
   bool res = OrderModify(ticket, OrderOpenPrice(), sl, tp, 0, clrGold);
   if(!res)
   {
      int err = GetLastError();
      // If error is 1 (ERR_NO_RESULT: already has these exact stops), consider success
      if(err == 1)
         return StringFormat("{\"ok\":true,\"ticket\":%d,\"sl\":%.5f,\"tp\":%.5f,\"note\":\"already_set\"}", ticket, sl, tp);
      return StringFormat("{\"ok\":false,\"error\":\"OrderModify failed (Error %d: %s)\"}", err, ErrorDescription(err));
   }

   return StringFormat("{\"ok\":true,\"ticket\":%d,\"sl\":%.5f,\"tp\":%.5f}", ticket, sl, tp);
}

//+------------------------------------------------------------------+
//| Command: CLOSE_POSITION                                          |
//+------------------------------------------------------------------+
string HandleClosePosition(string raw_json)
{
   int ticket    = (int)GetJsonDouble(raw_json, "ticket");
   double volume = NormalizeDouble(GetJsonDouble(raw_json, "volume"), 2);

   if(!OrderSelect(ticket, SELECT_BY_TICKET))
   {
      return StringFormat("{\"ok\":false,\"error\":\"Ticket #%d not found or already closed\"}", ticket);
   }

   // If order is pending, delete it instead of close
   if(OrderType() > OP_SELL)
   {
      ResetLastError();
      bool del_res = OrderDelete(ticket, clrCrimson);
      if(!del_res)
      {
         int err = GetLastError();
         return StringFormat("{\"ok\":false,\"error\":\"OrderDelete for pending #%d failed (Error %d: %s)\"}", err, ErrorDescription(err), err);
      }
      return StringFormat("{\"ok\":true,\"ticket\":%d,\"status\":\"CANCELLED\"}", ticket);
   }

   if(volume <= 0.0 || volume > OrderLots())
      volume = OrderLots();

   double close_price = (OrderType() == OP_BUY) ? MarketInfo(OrderSymbol(), MODE_BID) : MarketInfo(OrderSymbol(), MODE_ASK);

   ResetLastError();
   bool res = OrderClose(ticket, volume, close_price, InpSlippage, clrDarkOrange);
   if(!res)
   {
      int err = GetLastError();
      return StringFormat("{\"ok\":false,\"error\":\"OrderClose failed (Error %d: %s)\"}", err, ErrorDescription(err));
   }

   return StringFormat("{\"ok\":true,\"ticket\":%d,\"volume_closed\":%.2f,\"close_price\":%.5f}", ticket, volume, close_price);
}

//+------------------------------------------------------------------+
//| Command: CANCEL_ORDER                                            |
//+------------------------------------------------------------------+
string HandleCancelOrder(string raw_json)
{
   int ticket = (int)GetJsonDouble(raw_json, "ticket");

   if(!OrderSelect(ticket, SELECT_BY_TICKET))
   {
      return StringFormat("{\"ok\":false,\"error\":\"Order #%d not found\"}", ticket);
   }

   ResetLastError();
   bool res = OrderDelete(ticket, clrCrimson);
   if(!res)
   {
      int err = GetLastError();
      return StringFormat("{\"ok\":false,\"error\":\"OrderDelete failed (Error %d: %s)\"}", err, ErrorDescription(err));
   }

   return StringFormat("{\"ok\":true,\"ticket\":%d,\"status\":\"CANCELLED\"}", ticket);
}

//+------------------------------------------------------------------+
//| Command: GET_HISTORY                                             |
//+------------------------------------------------------------------+
string HandleGetHistory(string raw_json)
{
   int limit = (int)GetJsonDouble(raw_json, "limit");
   if(limit <= 0) limit = 50;

   string hist_json = "[";
   int total = OrdersHistoryTotal();
   int count = 0;

   for(int i = total - 1; i >= 0 && count < limit; i--)
   {
      if(!OrderSelect(i, SELECT_BY_POS, MODE_HISTORY))
         continue;

      if(OrderType() != OP_BUY && OrderType() != OP_SELL)
         continue;

      if(count > 0) hist_json += ",";
      hist_json += StringFormat(
         "{\"ticket\":%d,\"symbol\":\"%s\",\"type\":%d,\"volume\":%.2f,\"price_open\":%.5f,\"price_close\":%.5f,\"profit\":%.2f,\"swap\":%.2f,\"commission\":%.2f,\"open_time\":%d,\"close_time\":%d,\"magic\":%d,\"comment\":\"%s\"}",
         OrderTicket(),
         OrderSymbol(),
         OrderType(),
         OrderLots(),
         OrderOpenPrice(),
         OrderClosePrice(),
         OrderProfit(),
         OrderSwap(),
         OrderCommission(),
         (int)OrderOpenTime(),
         (int)OrderCloseTime(),
         OrderMagicNumber(),
         EscapeJson(OrderComment())
      );
      count++;
   }
   hist_json += "]";

   return "{\"ok\":true,\"history\":" + hist_json + "}";
}

//+------------------------------------------------------------------+
//| Utility: Visual Chart Status Banner                              |
//+------------------------------------------------------------------+
void UpdateChartBanner(string text, color clr)
{
   string name = "SiriusMT4Status";
   if(ObjectFind(0, name) < 0)
   {
      ObjectCreate(0, name, OBJ_LABEL, 0, 0, 0);
      ObjectSetInteger(0, name, OBJPROP_CORNER, CORNER_LEFT_UPPER);
      ObjectSetInteger(0, name, OBJPROP_XDISTANCE, 15);
      ObjectSetInteger(0, name, OBJPROP_YDISTANCE, 20);
      ObjectSetInteger(0, name, OBJPROP_FONTSIZE, 10);
      ObjectSetString(0, name, OBJPROP_FONT, "Trebuchet MS");
   }
   ObjectSetString(0, name, OBJPROP_TEXT, text);
   ObjectSetInteger(0, name, OBJPROP_COLOR, clr);
   ChartRedraw(0);
}

//+------------------------------------------------------------------+
//| Utility: Ultra-lightweight JSON string value extractor           |
//+------------------------------------------------------------------+
string GetJsonString(string json, string key)
{
   string search = "\"" + key + "\"";
   int pos = StringFind(json, search);
   if(pos < 0) return "";

   int colon = StringFind(json, ":", pos + StringLen(search));
   if(colon < 0) return "";

   int quote1 = StringFind(json, "\"", colon + 1);
   if(quote1 < 0) return "";

   int quote2 = StringFind(json, "\"", quote1 + 1);
   if(quote2 < 0) return "";

   return StringSubstr(json, quote1 + 1, quote2 - quote1 - 1);
}

//+------------------------------------------------------------------+
//| Utility: Ultra-lightweight JSON numeric value extractor          |
//+------------------------------------------------------------------+
double GetJsonDouble(string json, string key)
{
   string search = "\"" + key + "\"";
   int pos = StringFind(json, search);
   if(pos < 0) return 0.0;

   int colon = StringFind(json, ":", pos + StringLen(search));
   if(colon < 0) return 0.0;

   // Skip spaces
   int start = colon + 1;
   while(start < StringLen(json) && (StringGetCharacter(json, start) == ' ' || StringGetCharacter(json, start) == '\"'))
      start++;

   int end = start;
   while(end < StringLen(json))
   {
      ushort c = StringGetCharacter(json, end);
      if((c >= '0' && c <= '9') || c == '.' || c == '-')
         end++;
      else
         break;
   }

   string val = StringSubstr(json, start, end - start);
   return StringToDouble(val);
}

//+------------------------------------------------------------------+
//| Utility: Escape JSON characters                                  |
//+------------------------------------------------------------------+
string EscapeJson(string s)
{
   StringReplace(s, "\\", "\\\\");
   StringReplace(s, "\"", "\\\"");
   StringReplace(s, "\r", "");
   StringReplace(s, "\n", " ");
   return s;
}

//+------------------------------------------------------------------+
//| Utility: String Trim helper                                      |
//+------------------------------------------------------------------+
string StringTrim(string s)
{
   StringTrimLeft(s);
   StringTrimRight(s);
   return s;
}
//+------------------------------------------------------------------+
