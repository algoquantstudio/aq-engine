//+------------------------------------------------------------------+
//| AqeMt5BridgeEA.mq5                                              |
//| Local bridge EA for AlgoQuant Engine MT5 runtime integration.    |
//+------------------------------------------------------------------+
#property strict
#property version "0.1"

#include <Trade/Trade.mqh>

input string InpBridgeUrl = "http://127.0.0.1:18080";
input string InpSessionId = "";
input string InpBridgeToken = "";
input string InpSymbols = "EURUSD";
input ENUM_TIMEFRAMES InpTimeframe = PERIOD_M1;
input int InpPollIntervalMs = 250;
input int InpRequestTimeoutMs = 5000;

CTrade trade;
ulong g_event_seq = 0;
datetime g_last_snapshot = 0;
datetime g_last_bar_time[];
string g_symbols[];

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

string RequestId()
{
   return IntegerToString((int)GetTickCount()) + "-" + IntegerToString((int)MathRand());
}

string Envelope(string request_id, string payload)
{
   g_event_seq++;
   return "{"
      "\"protocolVersion\":1,"
      "\"sessionId\":\"" + JsonEscape(InpSessionId) + "\","
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
      "X-AQE-MT5-Session: " + InpSessionId + "\r\n"
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
      Print("AQE bridge returned HTTP ", status, " response=", response);
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

string AccountJson()
{
   return "{"
      "\"accountId\":\"" + IntegerToString((int)AccountInfoInteger(ACCOUNT_LOGIN)) + "\","
      "\"accountType\":\"Live\","
      "\"equity\":" + DoubleToString(AccountInfoDouble(ACCOUNT_EQUITY), 2) + ","
      "\"cash\":" + DoubleToString(AccountInfoDouble(ACCOUNT_BALANCE), 2) + ","
      "\"currency\":\"" + JsonEscape(AccountInfoString(ACCOUNT_CURRENCY)) + "\","
      "\"buyingPower\":" + DoubleToString(AccountInfoDouble(ACCOUNT_MARGIN_FREE), 2) + ","
      "\"shortingEnabled\":true,"
      "\"leverage\":" + IntegerToString((int)AccountInfoInteger(ACCOUNT_LEVERAGE)) +
   "}";
}

string AssetJson(string symbol)
{
   SymbolSelect(symbol, true);
   double volume_min = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MIN);
   double volume_step = SymbolInfoDouble(symbol, SYMBOL_VOLUME_STEP);
   double volume_max = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MAX);
   double point = SymbolInfoDouble(symbol, SYMBOL_POINT);
   int contract_size = (int)SymbolInfoDouble(symbol, SYMBOL_TRADE_CONTRACT_SIZE);
   return "{"
      "\"id\":\"" + JsonEscape(symbol) + "\","
      "\"symbol\":\"" + JsonEscape(symbol) + "\","
      "\"name\":\"" + JsonEscape(symbol) + "\","
      "\"assetType\":\"Forex\","
      "\"status\":\"Active\","
      "\"exchange\":{\"UNKNOWN\":\"MT5\"},"
      "\"tradable\":true,"
      "\"marginable\":true,"
      "\"shortable\":true,"
      "\"fractional\":true,"
      "\"minOrderSize\":" + DoubleToString(volume_min, 8) + ","
      "\"quantityBase\":null,"
      "\"maxOrderSize\":" + DoubleToString(volume_max, 8) + ","
      "\"minPriceIncrement\":" + DoubleToString(point, 10) + ","
      "\"priceBase\":" + IntegerToString((int)MathPow(10, (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS))) + ","
      "\"contractSize\":" + IntegerToString(contract_size) +
   "}";
}

string QuoteJson(string symbol)
{
   MqlTick tick;
   SymbolInfoTick(symbol, tick);
   return "{"
      "\"symbol\":\"" + JsonEscape(symbol) + "\","
      "\"bid\":" + DoubleToString(tick.bid, (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS)) + ","
      "\"ask\":" + DoubleToString(tick.ask, (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS)) + ","
      "\"bidSize\":0.0,"
      "\"askSize\":0.0,"
      "\"last\":" + DoubleToString(tick.last, (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS)) + ","
      "\"lastSize\":null,"
      "\"timestamp\":\"" + IsoTime(TimeCurrent()) + "\""
   "}";
}

string BarJson(string symbol, int shift)
{
   datetime ts = iTime(symbol, InpTimeframe, shift);
   return "{"
      "\"symbol\":\"" + JsonEscape(symbol) + "\","
      "\"open\":" + DoubleToString(iOpen(symbol, InpTimeframe, shift), (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS)) + ","
      "\"high\":" + DoubleToString(iHigh(symbol, InpTimeframe, shift), (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS)) + ","
      "\"low\":" + DoubleToString(iLow(symbol, InpTimeframe, shift), (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS)) + ","
      "\"close\":" + DoubleToString(iClose(symbol, InpTimeframe, shift), (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS)) + ","
      "\"volume\":" + DoubleToString((double)iVolume(symbol, InpTimeframe, shift), 0) + ","
      "\"timestamp\":\"" + IsoTime(ts) + "\""
   "}";
}

void SendHeartbeat()
{
   string response;
   string payload = "{"
      "\"terminalName\":\"" + JsonEscape(TerminalInfoString(TERMINAL_NAME)) + "\","
      "\"accountId\":\"" + IntegerToString((int)AccountInfoInteger(ACCOUNT_LOGIN)) + "\","
      "\"serverTime\":\"" + IsoTime(TimeCurrent()) + "\""
   "}";
   PostJson("/v1/heartbeat", payload, response);
}

void SendSnapshot()
{
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
      "\"positions\":[],"
      "\"orders\":[]"
   "}";
   if(PostJson("/v1/snapshot", payload, response))
      g_last_snapshot = TimeCurrent();
}

void SendMarketData()
{
   string bars = "";
   string quotes = "";
   for(int i = 0; i < ArraySize(g_symbols); i++)
   {
      string symbol = g_symbols[i];
      SymbolSelect(symbol, true);
      datetime completed = iTime(symbol, InpTimeframe, 1);
      if(completed > 0 && completed != g_last_bar_time[i])
      {
         if(StringLen(bars) > 0) bars += ",";
         bars += BarJson(symbol, 1);
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

void SendTradeEvent(string native_id, string event_name, ulong order_ticket)
{
   string symbol = OrderGetString(ORDER_SYMBOL);
   if(symbol == "") symbol = _Symbol;
   double volume = OrderGetDouble(ORDER_VOLUME_CURRENT);
   double price = OrderGetDouble(ORDER_PRICE_OPEN);
   string side = "Buy";
   long order_type = OrderGetInteger(ORDER_TYPE);
   if(order_type == ORDER_TYPE_SELL || order_type == ORDER_TYPE_SELL_LIMIT || order_type == ORDER_TYPE_SELL_STOP)
      side = "Sell";

   string order_json = "{"
      "\"orderId\":\"" + IntegerToString((int)order_ticket) + "\","
      "\"insightId\":null,"
      "\"strategyType\":null,"
      "\"asset\":" + AssetJson(symbol) + ","
      "\"qty\":" + DoubleToString(volume, 8) + ","
      "\"filledQty\":" + DoubleToString(volume, 8) + ","
      "\"limitPrice\":null,"
      "\"filledPrice\":" + DoubleToString(price, (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS)) + ","
      "\"stopPrice\":null,"
      "\"side\":\"" + side + "\","
      "\"orderType\":\"Market\","
      "\"timeInForce\":\"GTC\","
      "\"status\":\"" + event_name + "\","
      "\"orderClass\":\"Simple\","
      "\"createdAt\":" + IntegerToString((int)TimeCurrent()) + ","
      "\"updatedAt\":" + IntegerToString((int)TimeCurrent()) + ","
      "\"submittedAt\":" + IntegerToString((int)TimeCurrent()) + ","
      "\"filledAt\":" + IntegerToString((int)TimeCurrent()) + ","
      "\"rejectionReason\":null,"
      "\"legs\":null"
   "}";

   string response;
   string payload = "{"
      "\"nativeEventId\":\"" + JsonEscape(native_id) + "\","
      "\"event\":\"" + event_name + "\","
      "\"order\":" + order_json +
   "}";
   PostJson("/v1/trade-event", payload, response);
}

void AckCommand(string command_id, bool ok, string message)
{
   string response;
   string payload = "{"
      "\"commandId\":\"" + JsonEscape(command_id) + "\","
      "\"ok\":" + (ok ? "true" : "false") + ","
      "\"message\":\"" + JsonEscape(message) + "\","
      "\"order\":null"
   "}";
   PostJson("/v1/commands/ack", payload, response);
}

void ExecuteCommand(string json)
{
   string command_id = ExtractString(json, "commandId");
   string action = ExtractString(json, "action");
   string symbol = ExtractString(json, "symbol");
   string side = ExtractString(json, "side");
   string order_type = ExtractString(json, "orderType");
   string order_id = ExtractString(json, "orderId");
   double qty = ExtractNumber(json, "qty", 0.0);
   double price = ExtractNumber(json, "price", 0.0);
   double take_profit = ExtractNumber(json, "takeProfit", 0.0);
   double stop_loss = ExtractNumber(json, "stopLoss", 0.0);

   if(action == "SUBMIT_ORDER")
   {
      if(symbol == "" || qty <= 0.0)
      {
         AckCommand(command_id, false, "invalid submit command");
         return;
      }
      bool ok = false;
      trade.SetExpertMagicNumber(27042026);
      if(order_type == "Market")
         ok = (side == "Sell") ? trade.Sell(qty, symbol, 0.0, stop_loss, take_profit, command_id)
                               : trade.Buy(qty, symbol, 0.0, stop_loss, take_profit, command_id);
      else if(order_type == "Limit")
         ok = (side == "Sell") ? trade.SellLimit(qty, price, symbol, stop_loss, take_profit, ORDER_TIME_GTC, 0, command_id)
                               : trade.BuyLimit(qty, price, symbol, stop_loss, take_profit, ORDER_TIME_GTC, 0, command_id);
      else if(order_type == "Stop" || order_type == "StopLimit")
         ok = (side == "Sell") ? trade.SellStop(qty, price, symbol, stop_loss, take_profit, ORDER_TIME_GTC, 0, command_id)
                               : trade.BuyStop(qty, price, symbol, stop_loss, take_profit, ORDER_TIME_GTC, 0, command_id);
      AckCommand(command_id, ok, ok ? "accepted" : IntegerToString((int)GetLastError()));
      return;
   }

   if(action == "CANCEL_ORDER")
   {
      ulong ticket = (ulong)StringToInteger(order_id);
      bool ok = trade.OrderDelete(ticket);
      AckCommand(command_id, ok, ok ? "cancelled" : IntegerToString((int)GetLastError()));
      return;
   }

   if(action == "CLOSE_POSITION")
   {
      ulong ticket = (ulong)StringToInteger(order_id);
      bool ok = trade.PositionClose(ticket);
      AckCommand(command_id, ok, ok ? "closed" : IntegerToString((int)GetLastError()));
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
      AckCommand(command_id, ok, ok ? "closed all" : IntegerToString((int)GetLastError()));
   }
}

void PollCommands()
{
   string response;
   string payload = "{\"maxCommands\":1}";
   if(!PostJson("/v1/commands/poll", payload, response)) return;
   if(StringFind(response, "\"commands\":[]") >= 0) return;
   if(StringFind(response, "\"action\"") >= 0)
      ExecuteCommand(response);
}

int OnInit()
{
   if(InpBridgeToken == "" || InpSessionId == "")
   {
      Print("AqeMt5BridgeEA requires InpBridgeToken and InpSessionId.");
      return INIT_PARAMETERS_INCORRECT;
   }
   int count = StringSplit(InpSymbols, ',', g_symbols);
   ArrayResize(g_last_bar_time, count);
   for(int i = 0; i < count; i++)
   {
      StringTrimLeft(g_symbols[i]);
      StringTrimRight(g_symbols[i]);
      SymbolSelect(g_symbols[i], true);
      g_last_bar_time[i] = 0;
   }
   EventSetMillisecondTimer(MathMax(100, InpPollIntervalMs));
   SendHeartbeat();
   SendSnapshot();
   Print("AqeMt5BridgeEA connected to ", InpBridgeUrl, " session=", InpSessionId);
   return INIT_SUCCEEDED;
}

void OnDeinit(const int reason)
{
   EventKillTimer();
}

void OnTimer()
{
   SendHeartbeat();
   if(TimeCurrent() - g_last_snapshot > 30) SendSnapshot();
   SendMarketData();
   PollCommands();
}

void OnTradeTransaction(
   const MqlTradeTransaction &trans,
   const MqlTradeRequest &request,
   const MqlTradeResult &result
)
{
   if(trans.order == 0) return;
   string event_name = "Accepted";
   if(trans.type == TRADE_TRANSACTION_DEAL_ADD) event_name = "Filled";
   if(trans.type == TRADE_TRANSACTION_ORDER_DELETE) event_name = "Canceled";
   if(result.retcode != TRADE_RETCODE_DONE && result.retcode != TRADE_RETCODE_PLACED && result.retcode != 0)
      event_name = "Rejected";
   string native_id = IntegerToString((int)trans.type) + ":" + IntegerToString((int)trans.order) + ":" + IntegerToString((int)trans.deal);
   SendTradeEvent(native_id, event_name, trans.order);
}
