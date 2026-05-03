//+------------------------------------------------------------------+
//| AqeMt5BridgeEA.mq5                                              |
//| Local RPC bridge EA for AlgoQuant Engine MT5 runtime integration.|
//+------------------------------------------------------------------+
#property strict
#property version "0.2"

#include <Trade/Trade.mqh>

input string InpBridgeUrl = "http://127.0.0.1:18080";
input string InpBridgeToken = "";
input int InpPollIntervalMs = 250;
input int InpRequestTimeoutMs = 5000;

CTrade trade;
ulong g_event_seq = 0;
string g_session_id = "";
string g_symbols[];
datetime g_last_bar_time[];
ENUM_TIMEFRAMES g_timeframe = PERIOD_M1;
datetime g_last_snapshot = 0;

string JsonEscape(string value)
{
   StringReplace(value, "\\", "\\\\");
   StringReplace(value, "\"", "\\\"");
   return value;
}

string IsoTime(datetime value)
{
   MqlDateTime dt;
   TimeToStruct(value, dt);
   return StringFormat(
      "%04d-%02d-%02dT%02d:%02d:%02dZ",
      dt.year,
      dt.mon,
      dt.day,
      dt.hour,
      dt.min,
      dt.sec
   );
}

datetime ParseIsoTime(string value)
{
   StringReplace(value, "T", " ");
   StringReplace(value, "Z", "");
   return StringToTime(value);
}

string RequestId()
{
   return IntegerToString((int)GetTickCount()) + "-" + IntegerToString((int)MathRand());
}

string Envelope(string request_id, string payload)
{
   g_event_seq++;
   return "{"
      "\"protocolVersion\":1,"
      "\"sessionId\":\"" + JsonEscape(g_session_id) + "\","
      "\"requestId\":\"" + JsonEscape(request_id) + "\","
      "\"eventSeq\":" + IntegerToString((int)g_event_seq) + ","
      "\"serverTime\":null,"
      "\"payload\":" + payload +
   "}";
}

bool PostJson(string path, string payload, string &response)
{
   string request_id = RequestId();
   string body = Envelope(request_id, payload);
   string headers =
      "Content-Type: application/json\r\n"
      "X-AQE-MT5-Session: " + g_session_id + "\r\n"
      "X-AQE-MT5-Token: " + InpBridgeToken + "\r\n"
      "X-AQE-MT5-Seq: " + IntegerToString((int)g_event_seq) + "\r\n";

   char data[];
   char result[];
   string result_headers;
   StringToCharArray(body, data, 0, StringLen(body), CP_UTF8);

   int status = WebRequest(
      "POST",
      InpBridgeUrl + path,
      headers,
      InpRequestTimeoutMs,
      data,
      result,
      result_headers
   );

   response = CharArrayToString(result, 0, -1, CP_UTF8);
   if(status == -1)
   {
      Print("AQE bridge WebRequest failed. Error=", GetLastError(),
            ". Check Tools > Options > Expert Advisors > Allow WebRequest URL: ", InpBridgeUrl);
      return false;
   }
   if(status < 200 || status >= 300)
   {
      Print("AQE bridge returned HTTP ", status, " path=", path, " response=", response);
      return false;
   }
   return true;
}

string ExtractString(string json, string key)
{
   string needle = "\"" + key + "\":";
   int start = StringFind(json, needle);
   if(start < 0) return "";
   start += StringLen(needle);
   while(start < StringLen(json) && StringGetCharacter(json, start) == ' ') start++;
   if(start >= StringLen(json) || StringGetCharacter(json, start) != '"') return "";
   start++;
   int end = StringFind(json, "\"", start);
   if(end < 0) return "";
   return StringSubstr(json, start, end - start);
}

double ExtractNumber(string json, string key, double fallback = 0.0)
{
   string needle = "\"" + key + "\":";
   int start = StringFind(json, needle);
   if(start < 0) return fallback;
   start += StringLen(needle);
   while(start < StringLen(json) && StringGetCharacter(json, start) == ' ') start++;
   int end = start;
   while(end < StringLen(json))
   {
      int ch = StringGetCharacter(json, end);
      if((ch >= '0' && ch <= '9') || ch == '.' || ch == '-' || ch == '+')
         end++;
      else
         break;
   }
   if(end <= start) return fallback;
   return StringToDouble(StringSubstr(json, start, end - start));
}

string ExtractStringArray(string json, string key)
{
   string needle = "\"" + key + "\":[";
   int start = StringFind(json, needle);
   if(start < 0) return "";
   start += StringLen(needle);
   int end = StringFind(json, "]", start);
   if(end < 0) return "";
   string array_body = StringSubstr(json, start, end - start);
   StringReplace(array_body, "\"", "");
   return array_body;
}

string ExtractFirstRpcRequest(string json)
{
   int requests_start = StringFind(json, "\"requests\":[");
   if(requests_start < 0) return "";
   int object_start = StringFind(json, "{", requests_start);
   if(object_start < 0) return "";

   int depth = 0;
   bool in_string = false;
   bool escaped = false;
   for(int i = object_start; i < StringLen(json); i++)
   {
      int ch = StringGetCharacter(json, i);
      if(escaped)
      {
         escaped = false;
         continue;
      }
      if(ch == '\\')
      {
         escaped = true;
         continue;
      }
      if(ch == '"')
      {
         in_string = !in_string;
         continue;
      }
      if(in_string) continue;
      if(ch == '{') depth++;
      if(ch == '}')
      {
         depth--;
         if(depth == 0)
            return StringSubstr(json, object_start, i - object_start + 1);
      }
   }
   return "";
}

ENUM_TIMEFRAMES TimeframeFromCode(string code)
{
   if(code == "PERIOD_M1") return PERIOD_M1;
   if(code == "PERIOD_M2") return PERIOD_M2;
   if(code == "PERIOD_M3") return PERIOD_M3;
   if(code == "PERIOD_M4") return PERIOD_M4;
   if(code == "PERIOD_M5") return PERIOD_M5;
   if(code == "PERIOD_M6") return PERIOD_M6;
   if(code == "PERIOD_M10") return PERIOD_M10;
   if(code == "PERIOD_M12") return PERIOD_M12;
   if(code == "PERIOD_M15") return PERIOD_M15;
   if(code == "PERIOD_M20") return PERIOD_M20;
   if(code == "PERIOD_M30") return PERIOD_M30;
   if(code == "PERIOD_H1") return PERIOD_H1;
   if(code == "PERIOD_H2") return PERIOD_H2;
   if(code == "PERIOD_H3") return PERIOD_H3;
   if(code == "PERIOD_H4") return PERIOD_H4;
   if(code == "PERIOD_H6") return PERIOD_H6;
   if(code == "PERIOD_H8") return PERIOD_H8;
   if(code == "PERIOD_H12") return PERIOD_H12;
   if(code == "PERIOD_D1") return PERIOD_D1;
   if(code == "PERIOD_MN1") return PERIOD_MN1;
   return PERIOD_M1;
}

string AccountJson()
{
   return "{"
      "\"account_id\":\"" + IntegerToString(AccountInfoInteger(ACCOUNT_LOGIN)) + "\","
      "\"account_type\":\"Live\","
      "\"equity\":" + DoubleToString(AccountInfoDouble(ACCOUNT_EQUITY), 2) + ","
      "\"cash\":" + DoubleToString(AccountInfoDouble(ACCOUNT_BALANCE), 2) + ","
      "\"currency\":\"" + JsonEscape(AccountInfoString(ACCOUNT_CURRENCY)) + "\","
      "\"buying_power\":" + DoubleToString(AccountInfoDouble(ACCOUNT_MARGIN_FREE), 2) + ","
      "\"shorting_enabled\":true,"
      "\"leverage\":" + IntegerToString((int)MathMin(255, AccountInfoInteger(ACCOUNT_LEVERAGE))) +
   "}";
}

string AssetJson(string symbol)
{
   SymbolSelect(symbol, true);
   double volume_min = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MIN);
   double volume_max = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MAX);
   double point = SymbolInfoDouble(symbol, SYMBOL_POINT);
   int contract_size = (int)SymbolInfoDouble(symbol, SYMBOL_TRADE_CONTRACT_SIZE);
   return "{"
      "\"id\":\"" + JsonEscape(symbol) + "\","
      "\"symbol\":\"" + JsonEscape(symbol) + "\","
      "\"name\":\"" + JsonEscape(symbol) + "\","
      "\"asset_type\":\"Forex\","
      "\"status\":\"Active\","
      "\"exchange\":{\"UNKNOWN\":\"MT5\"},"
      "\"tradable\":true,"
      "\"marginable\":true,"
      "\"shortable\":true,"
      "\"fractional\":true,"
      "\"min_order_size\":" + DoubleToString(volume_min, 8) + ","
      "\"quantity_base\":null,"
      "\"max_order_size\":" + DoubleToString(volume_max, 8) + ","
      "\"min_price_increment\":" + DoubleToString(point, 10) + ","
      "\"price_base\":" + IntegerToString((int)MathPow(10, (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS))) + ","
      "\"contract_size\":" + IntegerToString(contract_size) +
   "}";
}

string QuoteJson(string symbol)
{
   MqlTick tick;
   SymbolInfoTick(symbol, tick);
   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   double last = tick.last > 0.0 ? tick.last : (tick.bid + tick.ask) / 2.0;
   return "{"
      "\"symbol\":\"" + JsonEscape(symbol) + "\","
      "\"bid\":" + DoubleToString(tick.bid, digits) + ","
      "\"ask\":" + DoubleToString(tick.ask, digits) + ","
      "\"bid_size\":0.0,"
      "\"ask_size\":0.0,"
      "\"last\":" + DoubleToString(last, digits) + ","
      "\"last_size\":null,"
      "\"timestamp\":\"" + IsoTime(TimeCurrent()) + "\""
   "}";
}

string BarJson(string symbol, ENUM_TIMEFRAMES timeframe, int shift)
{
   datetime ts = iTime(symbol, timeframe, shift);
   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   return "{"
      "\"symbol\":\"" + JsonEscape(symbol) + "\","
      "\"open\":" + DoubleToString(iOpen(symbol, timeframe, shift), digits) + ","
      "\"high\":" + DoubleToString(iHigh(symbol, timeframe, shift), digits) + ","
      "\"low\":" + DoubleToString(iLow(symbol, timeframe, shift), digits) + ","
      "\"close\":" + DoubleToString(iClose(symbol, timeframe, shift), digits) + ","
      "\"volume\":" + DoubleToString((double)iVolume(symbol, timeframe, shift), 0) + ","
      "\"timestamp\":\"" + IsoTime(ts) + "\""
   "}";
}

string HistoryJson(string symbol, ENUM_TIMEFRAMES timeframe, datetime start_time, datetime end_time)
{
   string bars = "";
   int count = Bars(symbol, timeframe);
   int emitted = 0;
   for(int shift = MathMin(count - 1, 1000); shift >= 0; shift--)
   {
      datetime ts = iTime(symbol, timeframe, shift);
      if(ts <= 0) continue;
      if(start_time > 0 && ts < start_time) continue;
      if(end_time > 0 && ts > end_time) continue;
      if(emitted > 0) bars += ",";
      bars += BarJson(symbol, timeframe, shift);
      emitted++;
      if(emitted >= 1000) break;
   }
   return "[" + bars + "]";
}

string OrderJson(string order_id, string symbol, double qty, string side, string order_type, string status, double price, string rejection_reason = "")
{
   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   return "{"
      "\"order_id\":\"" + JsonEscape(order_id) + "\","
      "\"insight_id\":null,"
      "\"strategy_type\":null,"
      "\"asset\":" + AssetJson(symbol) + ","
      "\"qty\":" + DoubleToString(qty, 8) + ","
      "\"filled_qty\":" + (status == "Filled" ? DoubleToString(qty, 8) : "0.0") + ","
      "\"limit_price\":null,"
      "\"filled_price\":" + (price > 0.0 ? DoubleToString(price, digits) : "null") + ","
      "\"stop_price\":null,"
      "\"side\":\"" + side + "\","
      "\"order_type\":\"" + order_type + "\","
      "\"time_in_force\":\"GTC\","
      "\"status\":\"" + status + "\","
      "\"order_class\":\"Simple\","
      "\"created_at\":" + IntegerToString((int)TimeCurrent()) + ","
      "\"updated_at\":" + IntegerToString((int)TimeCurrent()) + ","
      "\"submitted_at\":" + IntegerToString((int)TimeCurrent()) + ","
      "\"filled_at\":" + (status == "Filled" ? IntegerToString((int)TimeCurrent()) : "null") + ","
      "\"rejection_reason\":" + (rejection_reason == "" ? "null" : "\"" + JsonEscape(rejection_reason) + "\"") + ","
      "\"legs\":null"
   "}";
}

string PositionsJson()
{
   string positions = "";
   int emitted = 0;
   for(int i = 0; i < PositionsTotal(); i++)
   {
      string symbol = PositionGetSymbol(i);
      if(symbol == "") continue;
      double qty = PositionGetDouble(POSITION_VOLUME);
      double entry = PositionGetDouble(POSITION_PRICE_OPEN);
      double current = PositionGetDouble(POSITION_PRICE_CURRENT);
      double pnl = PositionGetDouble(POSITION_PROFIT);
      long type = PositionGetInteger(POSITION_TYPE);
      string side = type == POSITION_TYPE_SELL ? "Sell" : "Buy";
      if(emitted > 0) positions += ",";
      positions += "{"
         "\"account_id\":\"" + IntegerToString(AccountInfoInteger(ACCOUNT_LOGIN)) + "\","
         "\"asset\":" + AssetJson(symbol) + ","
         "\"avg_entry_price\":" + DoubleToString(entry, (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS)) + ","
         "\"qty\":" + DoubleToString(qty, 8) + ","
         "\"side\":\"" + side + "\","
         "\"market_value\":" + DoubleToString(qty * current, 2) + ","
         "\"cost_basis\":" + DoubleToString(qty * entry, 2) + ","
         "\"current_price\":" + DoubleToString(current, (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS)) + ","
         "\"unrealized_pnl\":" + DoubleToString(pnl, 2) + ","
         "\"realized_pnl\":0.0,"
         "\"margin_required\":null"
      "}";
      emitted++;
   }
   return "[" + positions + "]";
}

string OrdersJson()
{
   string orders = "";
   int emitted = 0;
   for(int i = 0; i < OrdersTotal(); i++)
   {
      ulong ticket = OrderGetTicket(i);
      if(ticket == 0) continue;
      string symbol = OrderGetString(ORDER_SYMBOL);
      long type = OrderGetInteger(ORDER_TYPE);
      double qty = OrderGetDouble(ORDER_VOLUME_CURRENT);
      double price = OrderGetDouble(ORDER_PRICE_OPEN);
      string side = (type == ORDER_TYPE_SELL || type == ORDER_TYPE_SELL_LIMIT || type == ORDER_TYPE_SELL_STOP || type == ORDER_TYPE_SELL_STOP_LIMIT) ? "Sell" : "Buy";
      string order_type = "Limit";
      if(type == ORDER_TYPE_BUY_STOP || type == ORDER_TYPE_SELL_STOP) order_type = "Stop";
      if(type == ORDER_TYPE_BUY_STOP_LIMIT || type == ORDER_TYPE_SELL_STOP_LIMIT) order_type = "StopLimit";
      if(emitted > 0) orders += ",";
      orders += OrderJson(IntegerToString((long)ticket), symbol, qty, side, order_type, "Accepted", price);
      emitted++;
   }
   return "[" + orders + "]";
}

void SendRpcResponse(string request_id, bool ok, string message, string payload)
{
   string response;
   string body = "{"
      "\"requestId\":\"" + JsonEscape(request_id) + "\","
      "\"ok\":" + (ok ? "true" : "false") + ","
      "\"message\":" + (message == "" ? "null" : "\"" + JsonEscape(message) + "\"") + ","
      "\"payload\":" + (payload == "" ? "null" : payload) +
   "}";
   PostJson("/v1/rpc/response", body, response);
}

void SendHeartbeat()
{
   if(g_session_id == "") return;
   string response;
   string payload = "{"
      "\"terminalName\":\"" + JsonEscape(TerminalInfoString(TERMINAL_NAME)) + "\","
      "\"accountId\":\"" + IntegerToString(AccountInfoInteger(ACCOUNT_LOGIN)) + "\","
      "\"serverTime\":\"" + IsoTime(TimeCurrent()) + "\""
   "}";
   PostJson("/v1/heartbeat", payload, response);
}

void SendSnapshot()
{
   if(g_session_id == "") return;
   string assets = "";
   for(int i = 0; i < ArraySize(g_symbols); i++)
   {
      if(i > 0) assets += ",";
      assets += AssetJson(g_symbols[i]);
   }
   string response;
   string payload = "{"
      "\"account\":" + AccountJson() + ","
      "\"assets\":[" + assets + "],"
      "\"positions\":" + PositionsJson() + ","
      "\"orders\":[]"
   "}";
   if(PostJson("/v1/snapshot", payload, response))
      g_last_snapshot = TimeCurrent();
}

void SendMarketData()
{
   if(g_session_id == "" || ArraySize(g_symbols) == 0) return;
   string bars = "";
   string quotes = "";
   for(int i = 0; i < ArraySize(g_symbols); i++)
   {
      string symbol = g_symbols[i];
      SymbolSelect(symbol, true);
      datetime completed = iTime(symbol, g_timeframe, 1);
      if(completed > 0 && completed != g_last_bar_time[i])
      {
         if(StringLen(bars) > 0) bars += ",";
         bars += BarJson(symbol, g_timeframe, 1);
         g_last_bar_time[i] = completed;
      }
      if(StringLen(quotes) > 0) quotes += ",";
      quotes += QuoteJson(symbol);
   }
   if(StringLen(bars) == 0 && StringLen(quotes) == 0) return;

   string response;
   string payload = "{"
      "\"quotes\":[" + quotes + "],"
      "\"bars\":[" + bars + "],"
      "\"history\":[]"
   "}";
   PostJson("/v1/market-data", payload, response);
}

void ConfigureSubscriptions(string symbols_csv, string timeframe_code)
{
   int count = StringSplit(symbols_csv, ',', g_symbols);
   ArrayResize(g_last_bar_time, count);
   g_timeframe = TimeframeFromCode(timeframe_code);
   for(int i = 0; i < count; i++)
   {
      StringTrimLeft(g_symbols[i]);
      StringTrimRight(g_symbols[i]);
      SymbolSelect(g_symbols[i], true);
      g_last_bar_time[i] = 0;
   }
}

void ExecuteRpcRequest(string json)
{
   string request_id = ExtractString(json, "requestId");
   string action = ExtractString(json, "action");
   string symbol = ExtractString(json, "symbol");
   string timeframe_code = ExtractString(json, "timeframe");

   if(request_id == "" || action == "")
      return;

   if(action == "GET_ACCOUNT")
   {
      SendRpcResponse(request_id, true, "", AccountJson());
      return;
   }
   if(action == "GET_TICKER_INFO")
   {
      SendRpcResponse(request_id, symbol != "", symbol == "" ? "symbol is required" : "", symbol == "" ? "" : AssetJson(symbol));
      return;
   }
   if(action == "GET_LATEST_QUOTE")
   {
      SendRpcResponse(request_id, symbol != "", symbol == "" ? "symbol is required" : "", symbol == "" ? "" : QuoteJson(symbol));
      return;
   }
   if(action == "GET_LATEST_BAR")
   {
      ENUM_TIMEFRAMES tf = timeframe_code == "" ? g_timeframe : TimeframeFromCode(timeframe_code);
      SendRpcResponse(request_id, symbol != "", symbol == "" ? "symbol is required" : "", symbol == "" ? "" : BarJson(symbol, tf, 1));
      return;
   }
   if(action == "GET_HISTORY")
   {
      ENUM_TIMEFRAMES tf = TimeframeFromCode(timeframe_code);
      datetime start_time = ParseIsoTime(ExtractString(json, "start"));
      datetime end_time = ParseIsoTime(ExtractString(json, "end"));
      SendRpcResponse(request_id, symbol != "", symbol == "" ? "symbol is required" : "", symbol == "" ? "" : HistoryJson(symbol, tf, start_time, end_time));
      return;
   }
   if(action == "GET_POSITIONS")
   {
      SendRpcResponse(request_id, true, "", PositionsJson());
      return;
   }
   if(action == "GET_ORDERS")
   {
      SendRpcResponse(request_id, true, "", OrdersJson());
      return;
   }
   if(action == "SUBSCRIBE_BARS")
   {
      ConfigureSubscriptions(ExtractStringArray(json, "symbols"), timeframe_code);
      SendRpcResponse(request_id, true, "", "{\"subscribed\":true}");
      SendSnapshot();
      return;
   }
   if(action == "UNSUBSCRIBE_BARS")
   {
      ArrayResize(g_symbols, 0);
      ArrayResize(g_last_bar_time, 0);
      SendRpcResponse(request_id, true, "", "{\"subscribed\":false}");
      return;
   }

   double qty = ExtractNumber(json, "qty", 0.0);
   double price = ExtractNumber(json, "price", 0.0);
   double take_profit = ExtractNumber(json, "takeProfit", 0.0);
   double stop_loss = ExtractNumber(json, "stopLoss", 0.0);
   string side = ExtractString(json, "side");
   string order_type = ExtractString(json, "orderType");
   string order_id = ExtractString(json, "orderId");
   string client_order_id = ExtractString(json, "clientOrderId");

   if(action == "SUBMIT_ORDER")
   {
      if(symbol == "" || qty <= 0.0)
      {
         SendRpcResponse(request_id, false, "invalid submit order request", "");
         return;
      }
      bool ok = false;
      trade.SetExpertMagicNumber(27042026);
      if(order_type == "Market")
         ok = (side == "Sell") ? trade.Sell(qty, symbol, 0.0, stop_loss, take_profit, client_order_id)
                               : trade.Buy(qty, symbol, 0.0, stop_loss, take_profit, client_order_id);
      else if(order_type == "Limit")
         ok = (side == "Sell") ? trade.SellLimit(qty, price, symbol, stop_loss, take_profit, ORDER_TIME_GTC, 0, client_order_id)
                               : trade.BuyLimit(qty, price, symbol, stop_loss, take_profit, ORDER_TIME_GTC, 0, client_order_id);
      else if(order_type == "Stop" || order_type == "StopLimit")
         ok = (side == "Sell") ? trade.SellStop(qty, price, symbol, stop_loss, take_profit, ORDER_TIME_GTC, 0, client_order_id)
                               : trade.BuyStop(qty, price, symbol, stop_loss, take_profit, ORDER_TIME_GTC, 0, client_order_id);

      string broker_id = IntegerToString((int)MathMax((double)trade.ResultOrder(), (double)trade.ResultDeal()));
      string status = ok ? (order_type == "Market" ? "Filled" : "Accepted") : "Rejected";
      string reason = ok ? "" : IntegerToString((int)trade.ResultRetcode());
      double result_price = trade.ResultPrice();
      string payload = OrderJson(broker_id == "0" ? client_order_id : broker_id, symbol, qty, side, order_type, status, result_price, reason);
      SendRpcResponse(request_id, true, reason, payload);
      return;
   }
   if(action == "CANCEL_ORDER")
   {
      bool ok = trade.OrderDelete((ulong)StringToInteger(order_id));
      SendRpcResponse(request_id, ok, ok ? "" : IntegerToString((int)GetLastError()), "{\"cancelled\":true}");
      return;
   }
   if(action == "CLOSE_POSITION")
   {
      bool ok = trade.PositionClose((ulong)StringToInteger(order_id));
      SendRpcResponse(request_id, ok, ok ? "" : IntegerToString((int)GetLastError()), "{\"closed\":true}");
      return;
   }
   if(action == "CLOSE_ALL_POSITIONS")
   {
      bool ok = true;
      for(int i = PositionsTotal() - 1; i >= 0; i--)
      {
         string sym = PositionGetSymbol(i);
         if(sym != "") ok = trade.PositionClose(sym) && ok;
      }
      SendRpcResponse(request_id, ok, ok ? "" : IntegerToString((int)GetLastError()), "{\"closed\":true}");
      return;
   }

   SendRpcResponse(request_id, false, "unknown RPC action: " + action, "");
}

void PollRpc()
{
   string response;
   string payload = "{\"maxRequests\":1}";
   if(!PostJson("/v1/rpc/poll", payload, response)) return;
   string session_id = ExtractString(response, "sessionId");
   if(session_id != "") g_session_id = session_id;
   string request_json = ExtractFirstRpcRequest(response);
   if(request_json == "") return;
   ExecuteRpcRequest(request_json);
}

int OnInit()
{
   string bridge_url = InpBridgeUrl;
   string bridge_token = InpBridgeToken;
   StringTrimLeft(bridge_url);
   StringTrimRight(bridge_url);
   StringTrimLeft(bridge_token);
   StringTrimRight(bridge_token);

   if(bridge_url == "")
   {
      Print("AqeMt5BridgeEA parameter error: InpBridgeUrl is required.");
      return INIT_PARAMETERS_INCORRECT;
   }
   if(bridge_token == "")
   {
      Print("AqeMt5BridgeEA parameter error: InpBridgeToken is required and must match AQE_MT5_BRIDGE_TOKEN.");
      return INIT_PARAMETERS_INCORRECT;
   }
   if(InpPollIntervalMs < 100)
   {
      Print("AqeMt5BridgeEA parameter error: InpPollIntervalMs must be at least 100.");
      return INIT_PARAMETERS_INCORRECT;
   }
   if(InpRequestTimeoutMs < 1000)
   {
      Print("AqeMt5BridgeEA parameter error: InpRequestTimeoutMs must be at least 1000.");
      return INIT_PARAMETERS_INCORRECT;
   }

   EventSetMillisecondTimer(MathMax(100, InpPollIntervalMs));
   Print("AqeMt5BridgeEA started. bridge_url=", bridge_url, " poll_ms=", InpPollIntervalMs, " timeout_ms=", InpRequestTimeoutMs);
   return INIT_SUCCEEDED;
}

void OnDeinit(const int reason)
{
   EventKillTimer();
}

void OnTimer()
{
   PollRpc();
   SendHeartbeat();
   if(TimeCurrent() - g_last_snapshot > 30) SendSnapshot();
   SendMarketData();
}

void OnTradeTransaction(
   const MqlTradeTransaction &trans,
   const MqlTradeRequest &request,
   const MqlTradeResult &result
)
{
   if(g_session_id == "" || trans.order == 0) return;
   string event_name = "Accepted";
   if(trans.type == TRADE_TRANSACTION_DEAL_ADD) event_name = "Filled";
   if(trans.type == TRADE_TRANSACTION_ORDER_DELETE) event_name = "Canceled";
   if(result.retcode != TRADE_RETCODE_DONE && result.retcode != TRADE_RETCODE_PLACED && result.retcode != 0)
      event_name = "Rejected";
   string symbol = request.symbol == "" ? _Symbol : request.symbol;
   string side = request.type == ORDER_TYPE_SELL || request.type == ORDER_TYPE_SELL_LIMIT || request.type == ORDER_TYPE_SELL_STOP ? "Sell" : "Buy";
   string native_id = IntegerToString((int)trans.type) + ":" + IntegerToString((long)trans.order) + ":" + IntegerToString((long)trans.deal);
   string response;
   string payload = "{"
      "\"nativeEventId\":\"" + JsonEscape(native_id) + "\","
      "\"event\":\"" + event_name + "\","
      "\"order\":" + OrderJson(IntegerToString((long)trans.order), symbol, request.volume, side, "Market", event_name, result.price) +
   "}";
   PostJson("/v1/trade-event", payload, response);
}
