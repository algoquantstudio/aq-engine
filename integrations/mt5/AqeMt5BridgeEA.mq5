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
string g_subscription_symbols[];
string g_subscription_timeframes[];
datetime g_last_bar_time[];
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

string NormalizeOrderComment(string comment)
{
   StringTrimLeft(comment);
   StringTrimRight(comment);
   if(StringLen(comment) > 31)
      return StringSubstr(comment, 0, 31);
   return comment;
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
   string response_session_id = ExtractString(response, "sessionId");
   if(response_session_id != "") g_session_id = response_session_id;

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

double NormalizeToDigits(string symbol, double price)
{
   return NormalizeDouble(price, (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS));
}

double MinStopDistance(string symbol)
{
   double point = SymbolInfoDouble(symbol, SYMBOL_POINT);
   int stops_level = (int)SymbolInfoInteger(symbol, SYMBOL_TRADE_STOPS_LEVEL);
   return MathMax(0.0, stops_level * point);
}

bool IsTradeRetcodeSuccess(uint retcode)
{
   return retcode == TRADE_RETCODE_DONE
       || retcode == TRADE_RETCODE_DONE_PARTIAL
       || retcode == TRADE_RETCODE_PLACED;
}

double ClampBuyStopLoss(string symbol, double requested_sl, double bid_price)
{
   double min_dist = MinStopDistance(symbol);
   if(requested_sl <= 0.0)
      return 0.0;
   double max_sl = bid_price - min_dist;
   if(max_sl <= 0.0)
      return 0.0;
   return NormalizeToDigits(symbol, MathMin(requested_sl, max_sl));
}

double ClampBuyTakeProfit(string symbol, double requested_tp, double ask_price)
{
   double min_dist = MinStopDistance(symbol);
   if(requested_tp <= 0.0)
      return 0.0;
   double min_tp = ask_price + min_dist;
   return NormalizeToDigits(symbol, MathMax(requested_tp, min_tp));
}

double ClampSellStopLoss(string symbol, double requested_sl, double ask_price)
{
   double min_dist = MinStopDistance(symbol);
   if(requested_sl <= 0.0)
      return 0.0;
   double min_sl = ask_price + min_dist;
   return NormalizeToDigits(symbol, MathMax(requested_sl, min_sl));
}

double ClampSellTakeProfit(string symbol, double requested_tp, double bid_price)
{
   double min_dist = MinStopDistance(symbol);
   if(requested_tp <= 0.0)
      return 0.0;
   double max_tp = bid_price - min_dist;
   if(max_tp <= 0.0)
      return 0.0;
   return NormalizeToDigits(symbol, MathMin(requested_tp, max_tp));
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

bool StringArrayContains(string &values[], string candidate)
{
   for(int i = 0; i < ArraySize(values); i++)
   {
      if(values[i] == candidate) return true;
   }
   return false;
}

ENUM_ORDER_TYPE_FILLING PreferredFillingType(string symbol)
{
   long filling_mode = SymbolInfoInteger(symbol, SYMBOL_FILLING_MODE);
   if((filling_mode & SYMBOL_FILLING_FOK) == SYMBOL_FILLING_FOK)
      return ORDER_FILLING_FOK;
   if((filling_mode & SYMBOL_FILLING_IOC) == SYMBOL_FILLING_IOC)
      return ORDER_FILLING_IOC;
   return ORDER_FILLING_RETURN;
}

bool ClosePositionWithComment(ulong position_ticket, string symbol, double qty, string comment, uint &retcode)
{
   if(position_ticket == 0 || !PositionSelectByTicket(position_ticket))
   {
      retcode = 0;
      return false;
   }

   long position_type = PositionGetInteger(POSITION_TYPE);
   double volume = qty > 0.0 ? qty : PositionGetDouble(POSITION_VOLUME);
   if(volume <= 0.0)
   {
      retcode = 0;
      return false;
   }

   MqlTick tick;
   SymbolInfoTick(symbol, tick);
   MqlTradeRequest request;
   MqlTradeResult result;
   ZeroMemory(request);
   ZeroMemory(result);

   request.action = TRADE_ACTION_DEAL;
   request.position = position_ticket;
   request.symbol = symbol;
   request.volume = volume;
   request.deviation = 10;
   request.magic = 27042026;
   request.comment = NormalizeOrderComment(comment);
   request.type_filling = PreferredFillingType(symbol);

   if(position_type == POSITION_TYPE_BUY)
   {
      request.type = ORDER_TYPE_SELL;
      request.price = tick.bid > 0.0 ? tick.bid : SymbolInfoDouble(symbol, SYMBOL_BID);
   }
   else
   {
      request.type = ORDER_TYPE_BUY;
      request.price = tick.ask > 0.0 ? tick.ask : SymbolInfoDouble(symbol, SYMBOL_ASK);
   }

   bool ok = OrderSend(request, result);
   retcode = result.retcode;
   return ok && IsTradeRetcodeSuccess(result.retcode);
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

string JsonBool(bool value)
{
   return value ? "true" : "false";
}

int QuantityBaseFromStep(double step)
{
   if(step <= 0.0) return 0;
   for(int decimals = 0; decimals <= 8; decimals++)
   {
      double scaled = step * MathPow(10.0, decimals);
      if(MathAbs(scaled - MathRound(scaled)) < 0.00000001)
         return decimals;
   }
   return 8;
}

bool ContainsAny(string haystack, string needle_a, string needle_b = "", string needle_c = "", string needle_d = "")
{
   StringToUpper(haystack);
   if(needle_a != "" && StringFind(haystack, needle_a) >= 0) return true;
   if(needle_b != "" && StringFind(haystack, needle_b) >= 0) return true;
   if(needle_c != "" && StringFind(haystack, needle_c) >= 0) return true;
   if(needle_d != "" && StringFind(haystack, needle_d) >= 0) return true;
   return false;
}

string SymbolAssetTypeJson(string symbol)
{
   string path = SymbolInfoString(symbol, SYMBOL_PATH);
   string description = SymbolInfoString(symbol, SYMBOL_DESCRIPTION);
   string probe = symbol + " " + path + " " + description;
   long calc_mode = SymbolInfoInteger(symbol, SYMBOL_TRADE_CALC_MODE);

   if(ContainsAny(probe, "CRYPTO", "BTC", "ETH", "XRP")) return "\"Crypto\"";
   if(ContainsAny(probe, "INDEX", "INDICES", "IDX")) return "\"Index\"";
   if(ContainsAny(probe, "METAL", "GOLD", "SILVER", "OIL")) return "\"Commodity\"";
   if(calc_mode == SYMBOL_CALC_MODE_FOREX || calc_mode == SYMBOL_CALC_MODE_FOREX_NO_LEVERAGE)
      return "\"Forex\"";
   if(calc_mode == SYMBOL_CALC_MODE_CFDINDEX)
      return "\"Index\"";
   if(calc_mode == SYMBOL_CALC_MODE_FUTURES || calc_mode == SYMBOL_CALC_MODE_EXCH_FUTURES || calc_mode == SYMBOL_CALC_MODE_EXCH_FUTURES_FORTS)
      return "\"Commodity\"";
   if(calc_mode == SYMBOL_CALC_MODE_EXCH_STOCKS || calc_mode == SYMBOL_CALC_MODE_EXCH_STOCKS_MOEX)
      return "\"Stock\"";
   if(calc_mode == SYMBOL_CALC_MODE_CFD || calc_mode == SYMBOL_CALC_MODE_CFDLEVERAGE)
      return ContainsAny(probe, "BTC", "ETH", "CRYPTO") ? "\"Crypto\"" : "{\"UNKNOWN\":\"CFD\"}";
   return "{\"UNKNOWN\":\"MT5\"}";
}

bool IsTradeSessionOpen(string symbol)
{
   MqlDateTime now;
   TimeToStruct(TimeCurrent(), now);
   int seconds_now = now.hour * 3600 + now.min * 60 + now.sec;
   bool has_sessions = false;

   for(uint session = 0; session < 24; session++)
   {
      datetime from_time;
      datetime to_time;
      if(!SymbolInfoSessionTrade(symbol, (ENUM_DAY_OF_WEEK)now.day_of_week, session, from_time, to_time))
         break;
      has_sessions = true;
      MqlDateTime from_parts;
      MqlDateTime to_parts;
      TimeToStruct(from_time, from_parts);
      TimeToStruct(to_time, to_parts);
      int from_seconds = from_parts.hour * 3600 + from_parts.min * 60 + from_parts.sec;
      int to_seconds = to_parts.hour * 3600 + to_parts.min * 60 + to_parts.sec;
      if(from_seconds <= to_seconds)
      {
         if(seconds_now >= from_seconds && seconds_now <= to_seconds)
            return true;
      }
      else if(seconds_now >= from_seconds || seconds_now <= to_seconds)
      {
         return true;
      }
   }

   return !has_sessions;
}

bool IsTradableNow(string symbol)
{
   long trade_mode = SymbolInfoInteger(symbol, SYMBOL_TRADE_MODE);
   if(trade_mode == SYMBOL_TRADE_MODE_DISABLED || trade_mode == SYMBOL_TRADE_MODE_CLOSEONLY)
      return false;
   return IsTradeSessionOpen(symbol);
}

bool IsShortable(string symbol)
{
   long trade_mode = SymbolInfoInteger(symbol, SYMBOL_TRADE_MODE);
   return trade_mode == SYMBOL_TRADE_MODE_FULL || trade_mode == SYMBOL_TRADE_MODE_SHORTONLY;
}

string AssetJson(string symbol)
{
   SymbolSelect(symbol, true);
   double volume_min = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MIN);
   double volume_step = SymbolInfoDouble(symbol, SYMBOL_VOLUME_STEP);
   double volume_max = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MAX);
   double point = SymbolInfoDouble(symbol, SYMBOL_POINT);
   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   int contract_size = (int)SymbolInfoDouble(symbol, SYMBOL_TRADE_CONTRACT_SIZE);
   int quantity_base = QuantityBaseFromStep(volume_step > 0.0 ? volume_step : volume_min);
   bool active = SymbolInfoInteger(symbol, SYMBOL_SELECT) != 0 && SymbolInfoInteger(symbol, SYMBOL_TRADE_MODE) != SYMBOL_TRADE_MODE_DISABLED;
   bool tradable = active && IsTradableNow(symbol);
   bool shortable = active && IsShortable(symbol);
   bool fractional = quantity_base > 0;
   return "{"
      "\"id\":\"" + JsonEscape(symbol) + "\","
      "\"symbol\":\"" + JsonEscape(symbol) + "\","
      "\"name\":\"" + JsonEscape(SymbolInfoString(symbol, SYMBOL_DESCRIPTION) == "" ? symbol : SymbolInfoString(symbol, SYMBOL_DESCRIPTION)) + "\","
      "\"asset_type\":" + SymbolAssetTypeJson(symbol) + ","
      "\"status\":\"" + (active ? "Active" : "Inactive") + "\","
      "\"exchange\":{\"UNKNOWN\":\"MT5\"},"
      "\"tradable\":" + JsonBool(tradable) + ","
      "\"marginable\":" + JsonBool(AccountInfoInteger(ACCOUNT_LEVERAGE) > 1) + ","
      "\"shortable\":" + JsonBool(shortable) + ","
      "\"fractional\":" + JsonBool(fractional) + ","
      "\"min_order_size\":" + DoubleToString(volume_min, 8) + ","
      "\"quantity_base\":" + IntegerToString(quantity_base) + ","
      "\"max_order_size\":" + DoubleToString(volume_max, 8) + ","
      "\"min_price_increment\":" + DoubleToString(point, 10) + ","
      "\"price_base\":" + IntegerToString(digits) + ","
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

string RateBarJson(string symbol, MqlRates &rate)
{
   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   return "{"
      "\"symbol\":\"" + JsonEscape(symbol) + "\","
      "\"open\":" + DoubleToString(rate.open, digits) + ","
      "\"high\":" + DoubleToString(rate.high, digits) + ","
      "\"low\":" + DoubleToString(rate.low, digits) + ","
      "\"close\":" + DoubleToString(rate.close, digits) + ","
      "\"volume\":" + DoubleToString((double)rate.tick_volume, 0) + ","
      "\"timestamp\":\"" + IsoTime(rate.time) + "\""
   "}";
}

string HistoryJson(string symbol, ENUM_TIMEFRAMES timeframe, datetime start_time, datetime end_time)
{
   string bars = "";
   MqlRates rates[];
   ArraySetAsSeries(rates, false);
   int copied = CopyRates(symbol, timeframe, start_time, end_time, rates);
   if(copied <= 0)
   {
      Print("AQE bridge history request returned no rates symbol=", symbol,
            " timeframe=", EnumToString(timeframe),
            " start=", IsoTime(start_time),
            " end=", IsoTime(end_time),
            " copied=", copied,
            " last_error=", GetLastError());
      return "[]";
   }

   for(int i = 0; i < copied; i++)
   {
      if(rates[i].time <= 0) continue;
      if(i > 0) bars += ",";
      bars += RateBarJson(symbol, rates[i]);
   }

   return "[" + bars + "]";
}

string OrderJson(string order_id, string symbol, double qty, string side, string order_type, string status, double price, string rejection_reason = "", double realized_pnl = 0.0, bool has_realized_pnl = false)
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
      "\"realized_pnl\":" + (has_realized_pnl ? DoubleToString(realized_pnl, 8) : "null") + ","
      "\"rejection_reason\":" + (rejection_reason == "" ? "null" : "\"" + JsonEscape(rejection_reason) + "\"") + ","
      "\"legs\":null"
   "}";
}

ulong FindPositionTicketById(string order_id)
{
   ulong requested = (ulong)StringToInteger(order_id);
   if(requested == 0)
      return 0;

   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0 || !PositionSelectByTicket(ticket)) continue;

      ulong identifier = (ulong)PositionGetInteger(POSITION_IDENTIFIER);
      if(ticket == requested || identifier == requested)
         return ticket;
   }
   return 0;
}

string MarketPositionTicketAfterDeal(string symbol, ulong deal_ticket, ulong order_ticket)
{
   ulong position_id = 0;
   if(deal_ticket > 0 && HistoryDealSelect(deal_ticket))
      position_id = (ulong)HistoryDealGetInteger(deal_ticket, DEAL_POSITION_ID);

   ulong ticket = FindPositionTicketById(position_id > 0 ? IntegerToString((long)position_id) : IntegerToString((long)order_ticket));
   if(ticket > 0)
      return IntegerToString((long)ticket);
   if(position_id > 0)
      return IntegerToString((long)position_id);
   if(order_ticket > 0)
      return IntegerToString((long)order_ticket);
   if(deal_ticket > 0)
      return IntegerToString((long)deal_ticket);
   return "";
}

string PositionsJson()
{
   string positions = "";
   int emitted = 0;
   for(int i = 0; i < PositionsTotal(); i++)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0 || !PositionSelectByTicket(ticket)) continue;
      string symbol = PositionGetString(POSITION_SYMBOL);
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
   string emitted_symbols[];
   for(int i = 0; i < ArraySize(g_subscription_symbols); i++)
   {
      string symbol = g_subscription_symbols[i];
      if(StringArrayContains(emitted_symbols, symbol)) continue;
      int next_index = ArraySize(emitted_symbols);
      ArrayResize(emitted_symbols, next_index + 1);
      emitted_symbols[next_index] = symbol;
      if(StringLen(assets) > 0) assets += ",";
      assets += AssetJson(symbol);
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
   if(g_session_id == "" || ArraySize(g_subscription_symbols) == 0) return;
   string bars = "";
   string quotes = "";
   string emitted_quotes[];
   for(int i = 0; i < ArraySize(g_subscription_symbols); i++)
   {
      string symbol = g_subscription_symbols[i];
      string timeframe_code = g_subscription_timeframes[i];
      ENUM_TIMEFRAMES timeframe = TimeframeFromCode(timeframe_code);
      SymbolSelect(symbol, true);
      datetime completed = iTime(symbol, timeframe, 1);
      if(completed > 0 && completed != g_last_bar_time[i])
      {
         string bar_json = BarJson(symbol, timeframe, 1);
         if(StringLen(bars) > 0) bars += ",";
         bars += StringSubstr(bar_json, 0, StringLen(bar_json) - 1)
            + ",\"timeframe\":\"" + JsonEscape(timeframe_code) + "\"}";
         g_last_bar_time[i] = completed;
      }
      if(!StringArrayContains(emitted_quotes, symbol))
      {
         int next_index = ArraySize(emitted_quotes);
         ArrayResize(emitted_quotes, next_index + 1);
         emitted_quotes[next_index] = symbol;
         if(StringLen(quotes) > 0) quotes += ",";
         quotes += QuoteJson(symbol);
      }
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

void ConfigureSubscriptions(string symbols_csv)
{
   string entries[];
   int count = StringSplit(symbols_csv, ',', entries);
   ArrayResize(g_subscription_symbols, count);
   ArrayResize(g_subscription_timeframes, count);
   ArrayResize(g_last_bar_time, count);
   for(int i = 0; i < count; i++)
   {
      string entry = entries[i];
      StringTrimLeft(entry);
      StringTrimRight(entry);
      int separator = StringFind(entry, "|");
      string symbol = separator >= 0 ? StringSubstr(entry, 0, separator) : entry;
      string timeframe_code = separator >= 0 ? StringSubstr(entry, separator + 1) : "PERIOD_M1";
      StringTrimLeft(symbol);
      StringTrimRight(symbol);
      StringTrimLeft(timeframe_code);
      StringTrimRight(timeframe_code);
      g_subscription_symbols[i] = symbol;
      g_subscription_timeframes[i] = timeframe_code;
      SymbolSelect(symbol, true);
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
      ENUM_TIMEFRAMES tf = timeframe_code == "" ? PERIOD_M1 : TimeframeFromCode(timeframe_code);
      SendRpcResponse(request_id, symbol != "", symbol == "" ? "symbol is required" : "", symbol == "" ? "" : BarJson(symbol, tf, 1));
      return;
   }
   if(action == "GET_HISTORY")
   {
      ENUM_TIMEFRAMES tf = TimeframeFromCode(timeframe_code);
      datetime start_time = (datetime)ExtractNumber(json, "start_ts", 0.0);
      datetime end_time = (datetime)ExtractNumber(json, "end_ts", 0.0);
      if(start_time <= 0)
         start_time = ParseIsoTime(ExtractString(json, "start"));
      if(end_time <= 0)
         end_time = ParseIsoTime(ExtractString(json, "end"));
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
      ConfigureSubscriptions(ExtractStringArray(json, "symbols"));
      SendRpcResponse(request_id, true, "", "{\"subscribed\":true}");
      SendSnapshot();
      return;
   }
   if(action == "UNSUBSCRIBE_BARS")
   {
      ArrayResize(g_subscription_symbols, 0);
      ArrayResize(g_subscription_timeframes, 0);
      ArrayResize(g_last_bar_time, 0);
      SendRpcResponse(request_id, true, "", "{\"subscribed\":false}");
      return;
   }

   double qty = ExtractNumber(json, "qty", 0.0);
   double price = ExtractNumber(json, "price", 0.0);
   string side = ExtractString(json, "side");
   string order_type = ExtractString(json, "orderType");
   string order_id = ExtractString(json, "orderId");
   string client_order_id = ExtractString(json, "clientOrderId");
   string comment = NormalizeOrderComment(ExtractString(json, "comment"));

   if(action == "SUBMIT_ORDER")
   {
      if(symbol == "" || qty <= 0.0)
      {
         SendRpcResponse(request_id, false, "invalid submit order request", "");
         return;
      }
      bool ok = false;
      double take_profit = ExtractNumber(json, "takeProfit", 0.0);
      double stop_loss = ExtractNumber(json, "stopLoss", 0.0);
      MqlTick tick;
      SymbolInfoTick(symbol, tick);
      double bid = tick.bid > 0.0 ? tick.bid : SymbolInfoDouble(symbol, SYMBOL_BID);
      double ask = tick.ask > 0.0 ? tick.ask : SymbolInfoDouble(symbol, SYMBOL_ASK);
      double normalized_tp = take_profit;
      double normalized_sl = stop_loss;
      if(order_type == "Market")
      {
         if(side == "Sell")
         {
            normalized_sl = ClampSellStopLoss(symbol, stop_loss, ask);
            normalized_tp = ClampSellTakeProfit(symbol, take_profit, bid);
         }
         else
         {
            normalized_sl = ClampBuyStopLoss(symbol, stop_loss, bid);
            normalized_tp = ClampBuyTakeProfit(symbol, take_profit, ask);
         }
      }
      else
      {
         normalized_tp = NormalizeToDigits(symbol, take_profit);
         normalized_sl = NormalizeToDigits(symbol, stop_loss);
      }
      trade.SetExpertMagicNumber(27042026);
      if(order_type == "Market")
      {
         ok = (side == "Sell") ? trade.Sell(qty, symbol, 0.0, 0.0, 0.0, comment)
                               : trade.Buy(qty, symbol, 0.0, 0.0, 0.0, comment);
      }
      else if(order_type == "Limit")
         ok = (side == "Sell") ? trade.SellLimit(qty, price, symbol, normalized_sl, normalized_tp, ORDER_TIME_GTC, 0, comment)
                               : trade.BuyLimit(qty, price, symbol, normalized_sl, normalized_tp, ORDER_TIME_GTC, 0, comment);
      else if(order_type == "Stop" || order_type == "StopLimit")
         ok = (side == "Sell") ? trade.SellStop(qty, price, symbol, normalized_sl, normalized_tp, ORDER_TIME_GTC, 0, comment)
                               : trade.BuyStop(qty, price, symbol, normalized_sl, normalized_tp, ORDER_TIME_GTC, 0, comment);

      uint result_retcode = trade.ResultRetcode();
      ok = ok && IsTradeRetcodeSuccess(result_retcode);
      ulong result_order = trade.ResultOrder();
      ulong result_deal = trade.ResultDeal();
      double result_price = trade.ResultPrice();
      string broker_id = client_order_id;
      if(ok && order_type == "Market")
      {
         broker_id = MarketPositionTicketAfterDeal(symbol, result_deal, result_order);
         ulong position_ticket = (ulong)StringToInteger(broker_id);
         if(position_ticket > 0 && PositionSelectByTicket(position_ticket))
         {
            long position_type = PositionGetInteger(POSITION_TYPE);
            bool selected_side_matches = (side == "Sell" && position_type == POSITION_TYPE_SELL)
                                      || (side != "Sell" && position_type == POSITION_TYPE_BUY);
            if(!selected_side_matches)
            {
               ok = false;
               result_retcode = 10036;
            }
            double open_price = PositionGetDouble(POSITION_PRICE_OPEN);
            double current_sl = PositionGetDouble(POSITION_SL);
            double current_tp = PositionGetDouble(POSITION_TP);
            result_price = open_price > 0.0 ? open_price : result_price;
            if(ok && open_price > 0.0)
            {
               if(side == "Sell")
               {
                  normalized_sl = ClampSellStopLoss(symbol, stop_loss, open_price);
                  normalized_tp = ClampSellTakeProfit(symbol, take_profit, open_price);
               }
               else
               {
                  normalized_sl = ClampBuyStopLoss(symbol, stop_loss, open_price);
                  normalized_tp = ClampBuyTakeProfit(symbol, take_profit, open_price);
               }
            }
            bool has_sl = normalized_sl > 0.0;
            bool has_tp = normalized_tp > 0.0;
            if(ok && (has_sl || has_tp))
            {
               double modify_sl = has_sl ? normalized_sl : current_sl;
               double modify_tp = has_tp ? normalized_tp : current_tp;
               bool modify_ok = trade.PositionModify(position_ticket, modify_sl, modify_tp);
               uint modify_retcode = trade.ResultRetcode();
               if(!modify_ok || !IsTradeRetcodeSuccess(modify_retcode))
                  Print("AQE bridge could not attach market order stops ticket=", broker_id,
                        " sl=", DoubleToString(modify_sl, (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS)),
                        " tp=", DoubleToString(modify_tp, (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS)),
                        " retcode=", (int)modify_retcode);
            }
         }
      }
      else if(ok)
         broker_id = IntegerToString((long)result_order);
      string status = ok ? (order_type == "Market" ? "Filled" : "Accepted") : "Rejected";
      string reason = ok ? "" : IntegerToString((int)result_retcode);
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
      ulong position_ticket = FindPositionTicketById(order_id);
      uint retcode = 0;
      bool ok = position_ticket > 0 && ClosePositionWithComment(position_ticket, symbol, qty, comment, retcode);
      string reason = position_ticket == 0 ? "position ticket not found" : (ok ? "" : IntegerToString((int)retcode));
      SendRpcResponse(request_id, ok, reason, "{\"closed\":true}");
      return;
   }
   if(action == "CLOSE_ALL_POSITIONS")
   {
      bool ok = true;
      for(int i = PositionsTotal() - 1; i >= 0; i--)
      {
         ulong position_ticket = PositionGetTicket(i);
         string sym = position_ticket > 0 && PositionSelectByTicket(position_ticket)
            ? PositionGetString(POSITION_SYMBOL)
            : "";
         uint retcode = 0;
         if(sym != "") ok = ClosePositionWithComment(position_ticket, sym, 0.0, comment, retcode) && ok;
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
   if(trans.type == TRADE_TRANSACTION_ORDER_DELETE)
   {
      if(!HistoryOrderSelect(trans.order))
         return;
      ENUM_ORDER_STATE order_state = (ENUM_ORDER_STATE)HistoryOrderGetInteger(trans.order, ORDER_STATE);
      if(order_state == ORDER_STATE_CANCELED)
         event_name = "Canceled";
      else if(order_state == ORDER_STATE_EXPIRED)
         event_name = "Expired";
      else
         return;
   }
   if(result.retcode != TRADE_RETCODE_DONE && result.retcode != TRADE_RETCODE_PLACED && result.retcode != 0)
      event_name = "Rejected";
   string symbol = request.symbol == "" ? _Symbol : request.symbol;
   string side = request.type == ORDER_TYPE_SELL || request.type == ORDER_TYPE_SELL_LIMIT || request.type == ORDER_TYPE_SELL_STOP ? "Sell" : "Buy";
   double volume = request.volume;
   double price = result.price;
   double realized_pnl = 0.0;
   bool has_realized_pnl = false;
   string event_order_id = IntegerToString((long)trans.order);
   if(trans.type == TRADE_TRANSACTION_DEAL_ADD && HistoryDealSelect(trans.deal))
   {
      ENUM_DEAL_ENTRY deal_entry = (ENUM_DEAL_ENTRY)HistoryDealGetInteger(trans.deal, DEAL_ENTRY);
      ENUM_DEAL_TYPE deal_type = (ENUM_DEAL_TYPE)HistoryDealGetInteger(trans.deal, DEAL_TYPE);
      ulong deal_position_id = (ulong)HistoryDealGetInteger(trans.deal, DEAL_POSITION_ID);
      string deal_symbol = HistoryDealGetString(trans.deal, DEAL_SYMBOL);
      double deal_volume = HistoryDealGetDouble(trans.deal, DEAL_VOLUME);
      double deal_price = HistoryDealGetDouble(trans.deal, DEAL_PRICE);
      if(deal_entry == DEAL_ENTRY_OUT || deal_entry == DEAL_ENTRY_OUT_BY)
      {
         event_name = "Closed";
         realized_pnl =
            HistoryDealGetDouble(trans.deal, DEAL_PROFIT)
            + HistoryDealGetDouble(trans.deal, DEAL_COMMISSION)
            + HistoryDealGetDouble(trans.deal, DEAL_SWAP)
            + HistoryDealGetDouble(trans.deal, DEAL_FEE);
         has_realized_pnl = true;
      }
      if(deal_symbol != "") symbol = deal_symbol;
      if(deal_volume > 0.0) volume = deal_volume;
      if(deal_price > 0.0) price = deal_price;
      side = (deal_type == DEAL_TYPE_SELL) ? "Sell" : "Buy";
      ulong position_ticket = FindPositionTicketById(IntegerToString((long)deal_position_id));
      if(position_ticket > 0)
         event_order_id = IntegerToString((long)position_ticket);
      else if(deal_position_id > 0)
         event_order_id = IntegerToString((long)deal_position_id);
   }
   string native_id = IntegerToString((int)trans.type) + ":" + IntegerToString((long)trans.order) + ":" + IntegerToString((long)trans.deal);
   string response;
   string payload = "{"
      "\"nativeEventId\":\"" + JsonEscape(native_id) + "\","
      "\"event\":\"" + event_name + "\","
      "\"order\":" + OrderJson(event_order_id, symbol, volume, side, "Market", event_name, price, "", realized_pnl, has_realized_pnl) +
   "}";
   PostJson("/v1/trade-event", payload, response);
}
