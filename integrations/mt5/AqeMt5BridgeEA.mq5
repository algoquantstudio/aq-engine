//+------------------------------------------------------------------+
//| AqeMt5BridgeEA.mq5                                              |
//| Local RPC bridge EA for AlgoQuant Engine MT5 runtime integration.|
//+------------------------------------------------------------------+
#property strict
#property version "1.303"

#include <Trade/Trade.mqh>

input string InpBridgeUrl = "http://127.0.0.1:18080";
input string InpBridgeToken = "";
input string InpBridgeConnections = "";
input bool InpProbeInactiveConnections = false;
input int InpInactiveProbeIntervalMs = 500;
input int InpInactiveProbeTimeoutMs = 100;
input int InpInactiveProbeMaxCooldownMs = 2000;
input int InpPollIntervalMs = 100;
input int InpRequestTimeoutMs = 5000;
input int InpTradeEventFlushIntervalMs = 100;
input int InpTradeEventPostTimeoutMs = 750;
input int InpTradeEventBatchSize = 32;
input int InpTradeEventQueueCapacity = 2048;

CTrade trade;

struct BridgeConnection
{
   string url;
   string session_id;
   ulong event_seq;
   datetime last_snapshot;
   int consecutive_failures;
   uint next_poll_after_ms;
   uint last_heartbeat_ms;
   uint last_market_data_ms;
   uint last_trade_event_flush_ms;
   bool session_logged;
};

struct BridgeSubscription
{
   int bridge_index;
   string symbol;
   string timeframe_code;
   datetime last_bar_time;
};

struct OrderRoute
{
   string key;
   int bridge_index;
   string session_id;
   string client_order_id;
   string insight_id;
   string strategy_type;
};

struct PendingTradeEvent
{
   int bridge_index;
   ulong event_seq;
   string payload;
};

BridgeConnection g_bridges[];
BridgeSubscription g_subscriptions[];
OrderRoute g_order_routes[];
PendingTradeEvent g_pending_trade_events[];
int g_next_probe_bridge_index = 0;
ulong g_trade_event_seq = 0;
ulong g_trade_event_drop_count = 0;

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

int RoundOffsetToMinute(int seconds)
{
   if(seconds >= 0)
      return ((seconds + 30) / 60) * 60;
   return -(((-seconds + 30) / 60) * 60);
}

int BrokerUtcOffsetSeconds()
{
   datetime utc_time = TimeGMT();
   datetime broker_time = TimeTradeServer();
   if(broker_time <= 0)
      broker_time = TimeCurrent();
   if(utc_time <= 0 || broker_time <= 0)
      return 0;
   return RoundOffsetToMinute((int)(broker_time - utc_time));
}

datetime UtcToBrokerTimeWithOffset(datetime utc_time, int offset_seconds)
{
   if(utc_time <= 0) return 0;
   return (datetime)(utc_time + offset_seconds);
}

datetime BrokerToUtcTimeWithOffset(datetime broker_time, int offset_seconds)
{
   if(broker_time <= 0) return 0;
   return (datetime)(broker_time - offset_seconds);
}

datetime BrokerToUtcTime(datetime broker_time)
{
   return BrokerToUtcTimeWithOffset(broker_time, BrokerUtcOffsetSeconds());
}

datetime BrokerNowUtc()
{
   return BrokerToUtcTime(TimeCurrent());
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

string NormalizeBridgeUrl(string bridge_url)
{
   StringTrimLeft(bridge_url);
   StringTrimRight(bridge_url);
   while(StringLen(bridge_url) > 0 && StringSubstr(bridge_url, StringLen(bridge_url) - 1) == "/")
      bridge_url = StringSubstr(bridge_url, 0, StringLen(bridge_url) - 1);
   return bridge_url;
}

string BridgeToken()
{
   string token = InpBridgeToken;
   StringTrimLeft(token);
   StringTrimRight(token);
   return token;
}

bool IsValidBridgeIndex(int bridge_index)
{
   return bridge_index >= 0 && bridge_index < ArraySize(g_bridges);
}

int ActivePollDelayMs()
{
   int delay_ms = InpPollIntervalMs;
   if(delay_ms < 100) delay_ms = 100;
   if(delay_ms > 1000) delay_ms = 1000;
   return delay_ms;
}

int ClampInt(int value, int min_value, int max_value)
{
   if(value < min_value) return min_value;
   if(value > max_value) return max_value;
   return value;
}

int FailedPollDelayMs(int consecutive_failures)
{
   if(consecutive_failures <= 1) return 500;
   if(consecutive_failures <= 3) return 1000;
   return 2000;
}

int InactiveProbeTimeoutMs()
{
   return ClampInt(InpInactiveProbeTimeoutMs, 100, 500);
}

int InactiveProbeBaseIntervalMs()
{
   return ClampInt(InpInactiveProbeIntervalMs, 250, 60000);
}

int InactiveProbeMaxCooldownMs()
{
   return ClampInt(InpInactiveProbeMaxCooldownMs, InactiveProbeBaseIntervalMs(), 300000);
}

int InactiveProbeFailureDelayMs(int consecutive_failures, uint duration_ms)
{
   int delay_ms = InpProbeInactiveConnections ? FailedPollDelayMs(consecutive_failures)
                                              : InactiveProbeBaseIntervalMs();
   int multiplier = 1;
   int exponent = ClampInt(consecutive_failures - 1, 0, 4);
   for(int i = 0; i < exponent; i++)
      multiplier *= 2;
   delay_ms *= multiplier;
   if(duration_ms > (uint)InactiveProbeTimeoutMs())
      delay_ms = delay_ms < 1000 ? 1000 : delay_ms;
   return ClampInt(delay_ms, 250, InactiveProbeMaxCooldownMs());
}

bool HasElapsedMs(uint last_ms, int interval_ms)
{
   if(last_ms == 0)
      return true;
   int interval = MathMax(0, interval_ms);
   return GetTickCount() - last_ms >= (uint)interval;
}

int BridgePostTimeoutMs(string path)
{
   int timeout_ms = InpRequestTimeoutMs;
   int max_timeout_ms = 500;
   if(path == "/v1/rpc/response")
      max_timeout_ms = 1000;
   if(path == "/v1/trade-events" || path == "/v1/trade-event")
      max_timeout_ms = MathMax(100, InpTradeEventPostTimeoutMs);
   if(timeout_ms > max_timeout_ms)
      timeout_ms = max_timeout_ms;
   if(timeout_ms < 100)
      timeout_ms = 100;
   return timeout_ms;
}

bool IsBridgePollDue(int bridge_index)
{
   if(!IsValidBridgeIndex(bridge_index)) return false;
   uint next_poll_after_ms = g_bridges[bridge_index].next_poll_after_ms;
   return next_poll_after_ms == 0 || GetTickCount() >= next_poll_after_ms;
}

bool IsBridgeSessionActive(int bridge_index)
{
   return IsValidBridgeIndex(bridge_index) && g_bridges[bridge_index].session_id != "";
}

bool HasBridgeSubscriptions(int bridge_index)
{
   if(!IsValidBridgeIndex(bridge_index)) return false;
   for(int i = 0; i < ArraySize(g_subscriptions); i++)
   {
      if(g_subscriptions[i].bridge_index == bridge_index)
         return true;
   }
   return false;
}

bool HasBridgeOrderRoutes(int bridge_index)
{
   if(!IsValidBridgeIndex(bridge_index)) return false;
   for(int i = 0; i < ArraySize(g_order_routes); i++)
   {
      if(g_order_routes[i].bridge_index == bridge_index)
         return true;
   }
   return false;
}

void MarkBridgePollSuccess(int bridge_index)
{
   if(!IsValidBridgeIndex(bridge_index)) return;
   int previous_failures = g_bridges[bridge_index].consecutive_failures;
   g_bridges[bridge_index].consecutive_failures = 0;
   g_bridges[bridge_index].next_poll_after_ms = GetTickCount() + (uint)ActivePollDelayMs();
   if(previous_failures > 0)
      Print("AQE bridge[", bridge_index, "] poll recovered url=", g_bridges[bridge_index].url);
}

void MarkBridgePollFailure(int bridge_index, bool inactive_probe = false, uint duration_ms = 0)
{
   if(!IsValidBridgeIndex(bridge_index)) return;
   g_bridges[bridge_index].consecutive_failures++;
   int delay_ms = inactive_probe
      ? InactiveProbeFailureDelayMs(g_bridges[bridge_index].consecutive_failures, duration_ms)
      : FailedPollDelayMs(g_bridges[bridge_index].consecutive_failures);
   g_bridges[bridge_index].next_poll_after_ms = GetTickCount() + (uint)delay_ms;
   int failures = g_bridges[bridge_index].consecutive_failures;
   if(inactive_probe && (failures <= 3 || failures % 10 == 0))
      Print("AQE bridge[", bridge_index, "] inactive probe failed url=", g_bridges[bridge_index].url,
            " failures=", failures,
            " elapsed_ms=", (int)duration_ms,
            " next_probe_ms=", delay_ms);
   if(!inactive_probe && (failures <= 3 || failures % 10 == 0))
      Print("AQE bridge[", bridge_index, "] active poll failed url=", g_bridges[bridge_index].url,
            " failures=", failures,
            " elapsed_ms=", (int)duration_ms,
            " next_poll_ms=", delay_ms);
}

string BridgeSessionShort(int bridge_index)
{
   if(!IsValidBridgeIndex(bridge_index)) return "";
   string session_id = g_bridges[bridge_index].session_id;
   if(StringLen(session_id) <= 8) return session_id;
   return StringSubstr(session_id, 0, 8);
}

string RoutedOrderComment(int bridge_index, string comment)
{
   comment = NormalizeOrderComment(comment);
   string session_short = BridgeSessionShort(bridge_index);
   if(session_short == "") return comment;
   string prefix = "AQE:" + session_short + ":";
   int remaining = 31 - StringLen(prefix);
   if(remaining <= 0) return NormalizeOrderComment(prefix);
   return NormalizeOrderComment(prefix + StringSubstr(comment, 0, remaining));
}

int FindBridgeByComment(string comment)
{
   if(StringFind(comment, "AQE:") != 0) return -1;
   string session_short = StringSubstr(comment, 4, 8);
   for(int i = 0; i < ArraySize(g_bridges); i++)
   {
      if(BridgeSessionShort(i) == session_short)
         return i;
   }
   return -1;
}

int FindOrderRouteIndex(string key)
{
   if(key == "" || key == "0") return -1;
   for(int i = ArraySize(g_order_routes) - 1; i >= 0; i--)
   {
      if(g_order_routes[i].key == key)
         return i;
   }
   return -1;
}

int FindOrderBridgeIndex(string key)
{
   int index = FindOrderRouteIndex(key);
   return index >= 0 ? g_order_routes[index].bridge_index : -1;
}

void RememberOrderRouteMetadata(int bridge_index, string key, string client_order_id, string insight_id, string strategy_type)
{
   if(!IsValidBridgeIndex(bridge_index) || key == "" || key == "0") return;
   int existing = FindOrderRouteIndex(key);
   if(existing >= 0)
   {
      g_order_routes[existing].bridge_index = bridge_index;
      g_order_routes[existing].session_id = g_bridges[bridge_index].session_id;
      if(client_order_id != "") g_order_routes[existing].client_order_id = client_order_id;
      if(insight_id != "") g_order_routes[existing].insight_id = insight_id;
      if(strategy_type != "") g_order_routes[existing].strategy_type = strategy_type;
      return;
   }
   int index = ArraySize(g_order_routes);
   ArrayResize(g_order_routes, index + 1);
   g_order_routes[index].key = key;
   g_order_routes[index].bridge_index = bridge_index;
   g_order_routes[index].session_id = g_bridges[bridge_index].session_id;
   g_order_routes[index].client_order_id = client_order_id;
   g_order_routes[index].insight_id = insight_id;
   g_order_routes[index].strategy_type = strategy_type;
}

void RememberOrderRoute(int bridge_index, string key)
{
   RememberOrderRouteMetadata(bridge_index, key, "", "", "");
}

int FindOrderRouteIndexByAliases(string key_a, string key_b, string key_c, string key_d)
{
   int index = FindOrderRouteIndex(key_a);
   if(index >= 0) return index;
   index = FindOrderRouteIndex(key_b);
   if(index >= 0) return index;
   index = FindOrderRouteIndex(key_c);
   if(index >= 0) return index;
   return FindOrderRouteIndex(key_d);
}

void RememberOrderRouteFromMetadata(int bridge_index, string key, string client_order_id, string insight_id, string strategy_type)
{
   RememberOrderRouteMetadata(bridge_index, key, client_order_id, insight_id, strategy_type);
}

string Envelope(int bridge_index, string request_id, string payload)
{
   if(!IsValidBridgeIndex(bridge_index)) return "";
   g_bridges[bridge_index].event_seq++;
   return "{"
      "\"protocolVersion\":1,"
      "\"sessionId\":\"" + JsonEscape(g_bridges[bridge_index].session_id) + "\","
      "\"requestId\":\"" + JsonEscape(request_id) + "\","
      "\"eventSeq\":" + IntegerToString((long)g_bridges[bridge_index].event_seq) + ","
      "\"serverTime\":null,"
      "\"payload\":" + payload +
   "}";
}

bool PostJsonWithTimeout(int bridge_index, string path, string payload, int timeout_ms, string &response, bool quiet_transport_error = false)
{
   if(!IsValidBridgeIndex(bridge_index)) return false;
   string request_id = RequestId();
   string body = Envelope(bridge_index, request_id, payload);
   string headers =
      "Content-Type: application/json\r\n"
      "X-AQE-MT5-Session: " + g_bridges[bridge_index].session_id + "\r\n"
      "X-AQE-MT5-Token: " + BridgeToken() + "\r\n"
      "X-AQE-MT5-Seq: " + IntegerToString((long)g_bridges[bridge_index].event_seq) + "\r\n";

   char data[];
   char result[];
   string result_headers;
   StringToCharArray(body, data, 0, StringLen(body), CP_UTF8);

   int status = WebRequest(
      "POST",
      g_bridges[bridge_index].url + path,
      headers,
      timeout_ms,
      data,
      result,
      result_headers
   );

   response = CharArrayToString(result, 0, -1, CP_UTF8);
   string previous_session_id = g_bridges[bridge_index].session_id;
   string response_session_id = ExtractString(response, "sessionId");
   bool session_changed = false;
   if(response_session_id != "")
   {
      session_changed = response_session_id != previous_session_id;
      if(session_changed && previous_session_id != "")
         ClearBridgeSessionRuntime(bridge_index);
      g_bridges[bridge_index].session_id = response_session_id;
      if(session_changed)
      {
         g_bridges[bridge_index].session_logged = false;
         g_bridges[bridge_index].next_poll_after_ms = 0;
      }
   }

   if(status == -1)
   {
      if(!quiet_transport_error)
         Print("AQE bridge[", bridge_index, "] WebRequest failed. Error=", GetLastError(),
               ". Check Tools > Options > Expert Advisors > Allow WebRequest URL: ", g_bridges[bridge_index].url);
      ClearBridgeSessionRuntime(bridge_index);
      g_bridges[bridge_index].session_id = "";
      g_bridges[bridge_index].session_logged = false;
      return false;
   }
   if(status < 200 || status >= 300)
   {
      Print("AQE bridge[", bridge_index, "] returned HTTP ", status,
            " url=", g_bridges[bridge_index].url,
            " path=", path,
            " response=", response);
      if(response_session_id == "")
      {
         ClearBridgeSessionRuntime(bridge_index);
         g_bridges[bridge_index].session_id = "";
         g_bridges[bridge_index].session_logged = false;
      }
      return false;
   }
   if(session_changed && !g_bridges[bridge_index].session_logged)
   {
      Print("AQE bridge[", bridge_index, "] session established url=", g_bridges[bridge_index].url,
            " path=", path,
            " session=", BridgeSessionShort(bridge_index));
      g_bridges[bridge_index].session_logged = true;
   }
   return true;
}

bool PostJson(int bridge_index, string path, string payload, string &response)
{
   return PostJsonWithTimeout(bridge_index, path, payload, BridgePostTimeoutMs(path), response);
}

int TradeEventQueueCapacity()
{
   return ClampInt(InpTradeEventQueueCapacity, 128, 50000);
}

int TradeEventBatchSize()
{
   return ClampInt(InpTradeEventBatchSize, 1, 256);
}

int TradeEventFlushIntervalMs()
{
   return ClampInt(InpTradeEventFlushIntervalMs, 10, 5000);
}

void DropOldestTradeEvent()
{
   int total = ArraySize(g_pending_trade_events);
   if(total <= 0) return;
   for(int i = 1; i < total; i++)
      g_pending_trade_events[i - 1] = g_pending_trade_events[i];
   ArrayResize(g_pending_trade_events, total - 1);
   g_trade_event_drop_count++;
}

void QueueTradeEvent(int bridge_index, string payload)
{
   if(!IsValidBridgeIndex(bridge_index) || payload == "") return;
   while(ArraySize(g_pending_trade_events) >= TradeEventQueueCapacity())
      DropOldestTradeEvent();
   int index = ArraySize(g_pending_trade_events);
   ArrayResize(g_pending_trade_events, index + 1);
   g_trade_event_seq++;
   g_pending_trade_events[index].bridge_index = bridge_index;
   g_pending_trade_events[index].event_seq = g_trade_event_seq;
   g_pending_trade_events[index].payload = payload;
}

void RemoveQueuedTradeEventsForBridge(int bridge_index, int remove_count)
{
   if(remove_count <= 0) return;
   int write = 0;
   int removed = 0;
   int total = ArraySize(g_pending_trade_events);
   for(int read = 0; read < total; read++)
   {
      if(g_pending_trade_events[read].bridge_index == bridge_index && removed < remove_count)
      {
         removed++;
         continue;
      }
      if(write != read)
         g_pending_trade_events[write] = g_pending_trade_events[read];
      write++;
   }
   ArrayResize(g_pending_trade_events, write);
}

void FlushTradeEvents(int bridge_index)
{
   if(!IsValidBridgeIndex(bridge_index) || g_bridges[bridge_index].session_id == "") return;
   int batch_size = TradeEventBatchSize();
   string events = "";
   int emitted = 0;
   for(int i = 0; i < ArraySize(g_pending_trade_events) && emitted < batch_size; i++)
   {
      if(g_pending_trade_events[i].bridge_index != bridge_index) continue;
      string event_payload = g_pending_trade_events[i].payload;
      if(StringLen(event_payload) <= 1) continue;
      if(emitted > 0) events += ",";
      events += StringSubstr(event_payload, 0, StringLen(event_payload) - 1)
         + ",\"eventSeq\":" + IntegerToString((long)g_pending_trade_events[i].event_seq) + "}";
      emitted++;
   }
   if(emitted <= 0) return;

   string response;
   string payload = "{"
      "\"events\":[" + events + "],"
      "\"droppedCount\":" + IntegerToString((long)g_trade_event_drop_count) +
   "}";
   if(PostJson(bridge_index, "/v1/trade-events", payload, response))
   {
      g_trade_event_drop_count = 0;
      RemoveQueuedTradeEventsForBridge(bridge_index, emitted);
   }
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

int ExtractRpcRequests(string json, string &requests[])
{
   ArrayResize(requests, 0);
   int requests_start = StringFind(json, "\"requests\":[");
   if(requests_start < 0) return 0;
   int array_start = StringFind(json, "[", requests_start);
   if(array_start < 0) return 0;

   int depth = 0;
   int object_start = -1;
   bool in_string = false;
   bool escaped = false;
   for(int i = array_start + 1; i < StringLen(json); i++)
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
      if(ch == '{')
      {
         if(depth == 0)
            object_start = i;
         depth++;
         continue;
      }
      if(ch == '}')
      {
         depth--;
         if(depth == 0 && object_start >= 0)
         {
            int index = ArraySize(requests);
            ArrayResize(requests, index + 1);
            requests[index] = StringSubstr(json, object_start, i - object_start + 1);
            object_start = -1;
         }
         continue;
      }
      if(ch == ']' && depth == 0)
         break;
   }
   return ArraySize(requests);
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

string QuoteJsonWithOffset(string symbol, int broker_offset_seconds)
{
   MqlTick tick;
   SymbolInfoTick(symbol, tick);
   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   double last = tick.last > 0.0 ? tick.last : (tick.bid + tick.ask) / 2.0;
   datetime quote_time = tick.time > 0 ? (datetime)tick.time : TimeCurrent();
   return "{"
      "\"symbol\":\"" + JsonEscape(symbol) + "\","
      "\"bid\":" + DoubleToString(tick.bid, digits) + ","
      "\"ask\":" + DoubleToString(tick.ask, digits) + ","
      "\"bid_size\":0.0,"
      "\"ask_size\":0.0,"
      "\"last\":" + DoubleToString(last, digits) + ","
      "\"last_size\":null,"
      "\"timestamp\":\"" + IsoTime(BrokerToUtcTimeWithOffset(quote_time, broker_offset_seconds)) + "\""
   "}";
}

string QuoteJson(string symbol)
{
   return QuoteJsonWithOffset(symbol, BrokerUtcOffsetSeconds());
}

string BarJsonWithOffset(string symbol, ENUM_TIMEFRAMES timeframe, int shift, int broker_offset_seconds)
{
   datetime broker_ts = iTime(symbol, timeframe, shift);
   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   return "{"
      "\"symbol\":\"" + JsonEscape(symbol) + "\","
      "\"open\":" + DoubleToString(iOpen(symbol, timeframe, shift), digits) + ","
      "\"high\":" + DoubleToString(iHigh(symbol, timeframe, shift), digits) + ","
      "\"low\":" + DoubleToString(iLow(symbol, timeframe, shift), digits) + ","
      "\"close\":" + DoubleToString(iClose(symbol, timeframe, shift), digits) + ","
      "\"volume\":" + DoubleToString((double)iVolume(symbol, timeframe, shift), 0) + ","
      "\"timestamp\":\"" + IsoTime(BrokerToUtcTimeWithOffset(broker_ts, broker_offset_seconds)) + "\""
   "}";
}

string BarJson(string symbol, ENUM_TIMEFRAMES timeframe, int shift)
{
   return BarJsonWithOffset(symbol, timeframe, shift, BrokerUtcOffsetSeconds());
}

string RateBarJsonWithOffset(string symbol, MqlRates &rate, int broker_offset_seconds)
{
   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   return "{"
      "\"symbol\":\"" + JsonEscape(symbol) + "\","
      "\"open\":" + DoubleToString(rate.open, digits) + ","
      "\"high\":" + DoubleToString(rate.high, digits) + ","
      "\"low\":" + DoubleToString(rate.low, digits) + ","
      "\"close\":" + DoubleToString(rate.close, digits) + ","
      "\"volume\":" + DoubleToString((double)rate.tick_volume, 0) + ","
      "\"timestamp\":\"" + IsoTime(BrokerToUtcTimeWithOffset(rate.time, broker_offset_seconds)) + "\""
   "}";
}

string HistoryJson(string symbol, ENUM_TIMEFRAMES timeframe, datetime start_utc, datetime end_utc)
{
   string bars = "";
   MqlRates rates[];
   ArraySetAsSeries(rates, false);
   int broker_offset_seconds = BrokerUtcOffsetSeconds();
   datetime start_time = UtcToBrokerTimeWithOffset(start_utc, broker_offset_seconds);
   datetime end_time = UtcToBrokerTimeWithOffset(end_utc, broker_offset_seconds);
   int copied = CopyRates(symbol, timeframe, start_time, end_time, rates);
   if(copied <= 0)
   {
      Print("AQE bridge history request returned no rates symbol=", symbol,
            " timeframe=", EnumToString(timeframe),
            " utc_start=", IsoTime(start_utc),
            " utc_end=", IsoTime(end_utc),
            " broker_start=", IsoTime(start_time),
            " broker_end=", IsoTime(end_time),
            " broker_utc_offset_seconds=", broker_offset_seconds,
            " copied=", copied,
            " last_error=", GetLastError());
      return "[]";
   }

   for(int i = 0; i < copied; i++)
   {
      if(rates[i].time <= 0) continue;
      if(i > 0) bars += ",";
      bars += RateBarJsonWithOffset(symbol, rates[i], broker_offset_seconds);
   }

   return "[" + bars + "]";
}

string OrderJson(string order_id, string symbol, double qty, string side, string order_type, string status, double price, string rejection_reason = "", double realized_pnl = 0.0, bool has_realized_pnl = false, string insight_id = "", string strategy_type = "")
{
   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   datetime now_utc = BrokerNowUtc();
   return "{"
      "\"order_id\":\"" + JsonEscape(order_id) + "\","
      "\"insight_id\":" + (insight_id == "" ? "null" : "\"" + JsonEscape(insight_id) + "\"") + ","
      "\"strategy_type\":" + (strategy_type == "" ? "null" : "\"" + JsonEscape(strategy_type) + "\"") + ","
      "\"asset\":" + AssetJson(symbol) + ","
      "\"qty\":" + DoubleToString(qty, 8) + ","
      "\"filled_qty\":" + ((status == "Filled" || status == "Closed") ? DoubleToString(qty, 8) : "0.0") + ","
      "\"limit_price\":null,"
      "\"filled_price\":" + (price > 0.0 ? DoubleToString(price, digits) : "null") + ","
      "\"stop_price\":null,"
      "\"side\":\"" + side + "\","
      "\"order_type\":\"" + order_type + "\","
      "\"time_in_force\":\"GTC\","
      "\"status\":\"" + status + "\","
      "\"order_class\":\"Simple\","
      "\"created_at\":" + IntegerToString((int)now_utc) + ","
      "\"updated_at\":" + IntegerToString((int)now_utc) + ","
      "\"submitted_at\":" + IntegerToString((int)now_utc) + ","
      "\"filled_at\":" + ((status == "Filled" || status == "Closed") ? IntegerToString((int)now_utc) : "null") + ","
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

void SendRpcResponse(int bridge_index, string request_id, bool ok, string message, string payload, string action = "")
{
   string body = "{"
      "\"requestId\":\"" + JsonEscape(request_id) + "\","
      "\"ok\":" + (ok ? "true" : "false") + ","
      "\"message\":" + (message == "" ? "null" : "\"" + JsonEscape(message) + "\"") + ","
      "\"payload\":" + (payload == "" ? "null" : payload) +
   "}";
   for(int attempt = 1; attempt <= 3; attempt++)
   {
      string response;
      if(PostJson(bridge_index, "/v1/rpc/response", body, response))
      {
         if(attempt > 1)
            Print("AQE bridge[", bridge_index, "] sent RPC response after retry action=", action,
                  " request_id=", request_id,
                  " attempt=", attempt);
         return;
      }
      Print("AQE bridge[", bridge_index, "] failed to send RPC response action=", action,
            " request_id=", request_id,
            " attempt=", attempt, "/3");
      if(!IsValidBridgeIndex(bridge_index) || g_bridges[bridge_index].session_id == "")
         break;
      Sleep(25);
   }
}

void SendHeartbeat(int bridge_index)
{
   if(!IsValidBridgeIndex(bridge_index) || g_bridges[bridge_index].session_id == "") return;
   string response;
   string payload = "{"
      "\"terminalName\":\"" + JsonEscape(TerminalInfoString(TERMINAL_NAME)) + "\","
      "\"accountId\":\"" + IntegerToString(AccountInfoInteger(ACCOUNT_LOGIN)) + "\","
      "\"serverTime\":\"" + IsoTime(BrokerNowUtc()) + "\","
      "\"queuedTradeEvents\":" + IntegerToString(ArraySize(g_pending_trade_events)) + ","
      "\"droppedTradeEvents\":" + IntegerToString((long)g_trade_event_drop_count) +
   "}";
   PostJson(bridge_index, "/v1/heartbeat", payload, response);
}

void SendSnapshot(int bridge_index)
{
   if(!IsValidBridgeIndex(bridge_index) || g_bridges[bridge_index].session_id == "") return;
   string assets = "";
   string emitted_symbols[];
   for(int i = 0; i < ArraySize(g_subscriptions); i++)
   {
      if(g_subscriptions[i].bridge_index != bridge_index) continue;
      string symbol = g_subscriptions[i].symbol;
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
   PostJson(bridge_index, "/v1/snapshot", payload, response);
   g_bridges[bridge_index].last_snapshot = TimeCurrent();
}

void SendMarketData(int bridge_index)
{
   if(!IsValidBridgeIndex(bridge_index) || g_bridges[bridge_index].session_id == "") return;
   string bars = "";
   string quotes = "";
   string emitted_quotes[];
   int broker_offset_seconds = BrokerUtcOffsetSeconds();
   for(int i = 0; i < ArraySize(g_subscriptions); i++)
   {
      if(g_subscriptions[i].bridge_index != bridge_index) continue;
      string symbol = g_subscriptions[i].symbol;
      string timeframe_code = g_subscriptions[i].timeframe_code;
      ENUM_TIMEFRAMES timeframe = TimeframeFromCode(timeframe_code);
      SymbolSelect(symbol, true);
      datetime completed = iTime(symbol, timeframe, 1);
      if(completed > 0 && completed != g_subscriptions[i].last_bar_time)
      {
         string bar_json = BarJsonWithOffset(symbol, timeframe, 1, broker_offset_seconds);
         if(StringLen(bars) > 0) bars += ",";
         bars += StringSubstr(bar_json, 0, StringLen(bar_json) - 1)
            + ",\"timeframe\":\"" + JsonEscape(timeframe_code) + "\"}";
         g_subscriptions[i].last_bar_time = completed;
      }
      if(!StringArrayContains(emitted_quotes, symbol))
      {
         int next_index = ArraySize(emitted_quotes);
         ArrayResize(emitted_quotes, next_index + 1);
         emitted_quotes[next_index] = symbol;
         if(StringLen(quotes) > 0) quotes += ",";
         quotes += QuoteJsonWithOffset(symbol, broker_offset_seconds);
      }
   }
   if(StringLen(bars) == 0 && StringLen(quotes) == 0) return;

   string response;
   string payload = "{"
      "\"quotes\":[" + quotes + "],"
      "\"bars\":[" + bars + "],"
      "\"history\":[]"
   "}";
   PostJson(bridge_index, "/v1/market-data", payload, response);
}

void ClearSubscriptions(int bridge_index)
{
   int write = 0;
   for(int read = 0; read < ArraySize(g_subscriptions); read++)
   {
      if(g_subscriptions[read].bridge_index == bridge_index) continue;
      if(write != read)
         g_subscriptions[write] = g_subscriptions[read];
      write++;
   }
   ArrayResize(g_subscriptions, write);
}

void ClearOrderRoutesForBridge(int bridge_index)
{
   int write = 0;
   for(int read = 0; read < ArraySize(g_order_routes); read++)
   {
      if(g_order_routes[read].bridge_index == bridge_index) continue;
      if(write != read)
         g_order_routes[write] = g_order_routes[read];
      write++;
   }
   ArrayResize(g_order_routes, write);
}

void ClearBridgeSessionRuntime(int bridge_index)
{
   if(!IsValidBridgeIndex(bridge_index)) return;
   ClearSubscriptions(bridge_index);
   ClearOrderRoutesForBridge(bridge_index);
   g_bridges[bridge_index].last_snapshot = 0;
   g_bridges[bridge_index].last_heartbeat_ms = 0;
   g_bridges[bridge_index].last_market_data_ms = 0;
}

void ConfigureSubscriptions(int bridge_index, string symbols_csv)
{
   if(!IsValidBridgeIndex(bridge_index)) return;
   ClearSubscriptions(bridge_index);
   string entries[];
   int count = StringSplit(symbols_csv, ',', entries);
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
      if(symbol == "") continue;
      int index = ArraySize(g_subscriptions);
      ArrayResize(g_subscriptions, index + 1);
      g_subscriptions[index].bridge_index = bridge_index;
      g_subscriptions[index].symbol = symbol;
      g_subscriptions[index].timeframe_code = timeframe_code;
      g_subscriptions[index].last_bar_time = 0;
      SymbolSelect(symbol, true);
   }
}

void ExecuteRpcRequest(int bridge_index, string json)
{
   string request_id = ExtractString(json, "requestId");
   string action = ExtractString(json, "action");
   string symbol = ExtractString(json, "symbol");
   string timeframe_code = ExtractString(json, "timeframe");

   if(request_id == "" || action == "")
   {
      Print("AQE bridge[", bridge_index, "] ignored malformed RPC request: ", StringSubstr(json, 0, 240));
      return;
   }

   if(action == "GET_ACCOUNT")
   {
      SendRpcResponse(bridge_index, request_id, true, "", AccountJson(), action);
      return;
   }
   if(action == "GET_TICKER_INFO")
   {
      SendRpcResponse(bridge_index, request_id, symbol != "", symbol == "" ? "symbol is required" : "", symbol == "" ? "" : AssetJson(symbol), action);
      return;
   }
   if(action == "GET_LATEST_QUOTE")
   {
      SendRpcResponse(bridge_index, request_id, symbol != "", symbol == "" ? "symbol is required" : "", symbol == "" ? "" : QuoteJson(symbol), action);
      return;
   }
   if(action == "GET_LATEST_BAR")
   {
      ENUM_TIMEFRAMES tf = timeframe_code == "" ? PERIOD_M1 : TimeframeFromCode(timeframe_code);
      SendRpcResponse(bridge_index, request_id, symbol != "", symbol == "" ? "symbol is required" : "", symbol == "" ? "" : BarJson(symbol, tf, 1), action);
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
      SendRpcResponse(bridge_index, request_id, symbol != "", symbol == "" ? "symbol is required" : "", symbol == "" ? "" : HistoryJson(symbol, tf, start_time, end_time), action);
      return;
   }
   if(action == "GET_POSITIONS")
   {
      SendRpcResponse(bridge_index, request_id, true, "", PositionsJson(), action);
      return;
   }
   if(action == "GET_ORDERS")
   {
      SendRpcResponse(bridge_index, request_id, true, "", OrdersJson(), action);
      return;
   }
   if(action == "SUBSCRIBE_BARS")
   {
      ConfigureSubscriptions(bridge_index, ExtractStringArray(json, "symbols"));
      SendRpcResponse(bridge_index, request_id, true, "", "{\"subscribed\":true}", action);
      SendSnapshot(bridge_index);
      return;
   }
   if(action == "UNSUBSCRIBE_BARS")
   {
      ClearSubscriptions(bridge_index);
      SendRpcResponse(bridge_index, request_id, true, "", "{\"subscribed\":false}", action);
      return;
   }

   double qty = ExtractNumber(json, "qty", 0.0);
   double price = ExtractNumber(json, "price", 0.0);
   string side = ExtractString(json, "side");
   string order_type = ExtractString(json, "orderType");
   string order_id = ExtractString(json, "orderId");
   string client_order_id = ExtractString(json, "clientOrderId");
   string insight_id = ExtractString(json, "insightId");
   string strategy_type = ExtractString(json, "strategyType");
   string comment = NormalizeOrderComment(ExtractString(json, "comment"));

   if(action == "SUBMIT_ORDER")
   {
      if(symbol == "" || qty <= 0.0)
      {
         SendRpcResponse(bridge_index, request_id, false, "invalid submit order request", "", action);
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
         string routed_comment = RoutedOrderComment(bridge_index, comment);
         ok = (side == "Sell") ? trade.Sell(qty, symbol, 0.0, 0.0, 0.0, routed_comment)
                               : trade.Buy(qty, symbol, 0.0, 0.0, 0.0, routed_comment);
      }
      else if(order_type == "Limit")
      {
         string routed_comment = RoutedOrderComment(bridge_index, comment);
         ok = (side == "Sell") ? trade.SellLimit(qty, price, symbol, normalized_sl, normalized_tp, ORDER_TIME_GTC, 0, routed_comment)
                               : trade.BuyLimit(qty, price, symbol, normalized_sl, normalized_tp, ORDER_TIME_GTC, 0, routed_comment);
      }
      else if(order_type == "Stop" || order_type == "StopLimit")
      {
         string routed_comment = RoutedOrderComment(bridge_index, comment);
         ok = (side == "Sell") ? trade.SellStop(qty, price, symbol, normalized_sl, normalized_tp, ORDER_TIME_GTC, 0, routed_comment)
                               : trade.BuyStop(qty, price, symbol, normalized_sl, normalized_tp, ORDER_TIME_GTC, 0, routed_comment);
      }

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
      RememberOrderRouteMetadata(bridge_index, client_order_id, client_order_id, insight_id, strategy_type);
      RememberOrderRouteMetadata(bridge_index, broker_id, client_order_id, insight_id, strategy_type);
      if(result_order > 0) RememberOrderRouteMetadata(bridge_index, IntegerToString((long)result_order), client_order_id, insight_id, strategy_type);
      if(result_deal > 0) RememberOrderRouteMetadata(bridge_index, IntegerToString((long)result_deal), client_order_id, insight_id, strategy_type);
      string status = ok ? (order_type == "Market" ? "Filled" : "Accepted") : "Rejected";
      string reason = ok ? "" : IntegerToString((int)result_retcode);
      string payload = OrderJson(broker_id == "0" ? client_order_id : broker_id, symbol, qty, side, order_type, status, result_price, reason, 0.0, false, insight_id, strategy_type);
      SendRpcResponse(bridge_index, request_id, true, reason, payload, action);
      return;
   }
   if(action == "CANCEL_ORDER")
   {
      bool ok = trade.OrderDelete((ulong)StringToInteger(order_id));
      RememberOrderRouteMetadata(bridge_index, order_id, order_id, insight_id, strategy_type);
      SendRpcResponse(bridge_index, request_id, ok, ok ? "" : IntegerToString((int)GetLastError()), "{\"cancelled\":true}", action);
      return;
   }
   if(action == "UPDATE_ORDER")
   {
      ulong position_ticket = FindPositionTicketById(order_id);
      if(position_ticket > 0)
      {
         RememberOrderRouteMetadata(bridge_index, order_id, order_id, insight_id, strategy_type);
         RememberOrderRouteMetadata(bridge_index, IntegerToString((long)position_ticket), order_id, insight_id, strategy_type);
      }

      bool ok = false;
      string reason = "";
      double take_profit = ExtractNumber(json, "takeProfit", price);
      double stop_loss = ExtractNumber(json, "stopLoss", -1.0);
      if(position_ticket == 0 || !PositionSelectByTicket(position_ticket))
      {
         reason = "position ticket not found";
      }
      else
      {
         string position_symbol = PositionGetString(POSITION_SYMBOL);
         long position_type = PositionGetInteger(POSITION_TYPE);
         double current_sl = PositionGetDouble(POSITION_SL);
         double current_tp = PositionGetDouble(POSITION_TP);
         MqlTick tick;
         SymbolInfoTick(position_symbol, tick);
         double bid = tick.bid > 0.0 ? tick.bid : SymbolInfoDouble(position_symbol, SYMBOL_BID);
         double ask = tick.ask > 0.0 ? tick.ask : SymbolInfoDouble(position_symbol, SYMBOL_ASK);
         double modify_sl = current_sl;
         double modify_tp = current_tp;

         if(take_profit > 0.0)
         {
            modify_tp = position_type == POSITION_TYPE_SELL
               ? ClampSellTakeProfit(position_symbol, take_profit, bid)
               : ClampBuyTakeProfit(position_symbol, take_profit, ask);
            if(modify_tp <= 0.0)
               reason = "invalid take profit";
         }
         else if(take_profit == 0.0)
         {
            modify_tp = 0.0;
         }

         if(reason == "" && stop_loss >= 0.0)
         {
            if(stop_loss > 0.0)
            {
               modify_sl = position_type == POSITION_TYPE_SELL
                  ? ClampSellStopLoss(position_symbol, stop_loss, ask)
                  : ClampBuyStopLoss(position_symbol, stop_loss, bid);
               if(modify_sl <= 0.0)
                  reason = "invalid stop loss";
            }
            else
            {
               modify_sl = 0.0;
            }
         }

         if(reason == "")
         {
            trade.SetExpertMagicNumber(27042026);
            ok = trade.PositionModify(position_ticket, modify_sl, modify_tp);
            uint result_retcode = trade.ResultRetcode();
            ok = ok && IsTradeRetcodeSuccess(result_retcode);
            if(!ok)
               reason = IntegerToString((int)result_retcode);
         }
      }

      SendRpcResponse(bridge_index, request_id, ok, reason, "{\"updated\":true}", action);
      return;
   }
   if(action == "CLOSE_POSITION")
   {
      ulong position_ticket = FindPositionTicketById(order_id);
      string close_symbol = symbol;
      string close_side = "Sell";
      double close_qty = qty;
      double close_price = price;
      if(position_ticket > 0)
      {
         RememberOrderRouteMetadata(bridge_index, order_id, order_id, insight_id, strategy_type);
         RememberOrderRouteMetadata(bridge_index, IntegerToString((long)position_ticket), order_id, insight_id, strategy_type);
         if(PositionSelectByTicket(position_ticket))
         {
            close_symbol = PositionGetString(POSITION_SYMBOL);
            long position_type = PositionGetInteger(POSITION_TYPE);
            close_side = position_type == POSITION_TYPE_BUY ? "Sell" : "Buy";
            if(close_qty <= 0.0)
               close_qty = PositionGetDouble(POSITION_VOLUME);
         }
      }
      uint retcode = 0;
      bool ok = position_ticket > 0 && ClosePositionWithComment(position_ticket, close_symbol, close_qty, comment, retcode);
      string reason = position_ticket == 0 ? "position ticket not found" : (ok ? "" : IntegerToString((int)retcode));
      if(ok)
      {
         double result_price = trade.ResultPrice();
         if(result_price > 0.0)
            close_price = result_price;
         if(close_price <= 0.0)
         {
            MqlTick tick;
            SymbolInfoTick(close_symbol, tick);
            close_price = close_side == "Sell" ? (tick.bid > 0.0 ? tick.bid : SymbolInfoDouble(close_symbol, SYMBOL_BID))
                                               : (tick.ask > 0.0 ? tick.ask : SymbolInfoDouble(close_symbol, SYMBOL_ASK));
         }
      }
      string payload = ok
         ? OrderJson(IntegerToString((long)position_ticket), close_symbol, close_qty, close_side, "Market", "Closed", close_price, "", 0.0, false, insight_id, strategy_type)
         : "{\"closed\":false}";
      SendRpcResponse(bridge_index, request_id, ok, reason, payload, action);
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
      SendRpcResponse(bridge_index, request_id, ok, ok ? "" : IntegerToString((int)GetLastError()), "{\"closed\":true}", action);
      return;
   }

   SendRpcResponse(bridge_index, request_id, false, "unknown RPC action: " + action, "", action);
}

void ClearRuntimeState()
{
   ArrayResize(g_bridges, 0);
   ArrayResize(g_subscriptions, 0);
   ArrayResize(g_order_routes, 0);
   ArrayResize(g_pending_trade_events, 0);
   g_next_probe_bridge_index = 0;
   g_trade_event_seq = 0;
   g_trade_event_drop_count = 0;
}

bool AddBridgeConnection(string bridge_url)
{
   bridge_url = NormalizeBridgeUrl(bridge_url);
   if(bridge_url == "")
      return false;
   if(StringFind(bridge_url, "|") >= 0)
   {
      Print("AqeMt5BridgeEA ignored legacy bridge connection entry: ", bridge_url,
            ". Use comma-separated URLs only; all endpoints use InpBridgeToken.");
      return false;
   }

   for(int i = 0; i < ArraySize(g_bridges); i++)
   {
      if(g_bridges[i].url != bridge_url) continue;
      Print("AqeMt5BridgeEA ignored duplicate bridge URL: ", bridge_url);
      return true;
   }

   int index = ArraySize(g_bridges);
   ArrayResize(g_bridges, index + 1);
   g_bridges[index].url = bridge_url;
   g_bridges[index].session_id = "";
   g_bridges[index].event_seq = 0;
   g_bridges[index].last_snapshot = 0;
   g_bridges[index].consecutive_failures = 0;
   g_bridges[index].next_poll_after_ms = 0;
   g_bridges[index].last_heartbeat_ms = 0;
   g_bridges[index].last_market_data_ms = 0;
   g_bridges[index].session_logged = false;
   return true;
}

int ConfigureBridgeConnections()
{
   ClearRuntimeState();
   AddBridgeConnection(InpBridgeUrl);

   string entries[];
   int count = StringSplit(InpBridgeConnections, ',', entries);
   for(int i = 0; i < count; i++)
   {
      string entry = entries[i];
      StringTrimLeft(entry);
      StringTrimRight(entry);
      if(entry == "") continue;
      if(!AddBridgeConnection(entry))
         Print("AqeMt5BridgeEA ignored invalid bridge connection: ", entry);
   }

   return ArraySize(g_bridges);
}

int ActivePollTimeoutMs()
{
   int poll_timeout_ms = InpRequestTimeoutMs;
   int max_poll_timeout_ms = ActivePollDelayMs() * 2;
   if(max_poll_timeout_ms < 150) max_poll_timeout_ms = 150;
   if(max_poll_timeout_ms > 250) max_poll_timeout_ms = 250;
   if(poll_timeout_ms > max_poll_timeout_ms) poll_timeout_ms = max_poll_timeout_ms;
   if(poll_timeout_ms < 100) poll_timeout_ms = 100;
   return poll_timeout_ms;
}

bool PollRpc(int bridge_index, bool inactive_probe = false)
{
   if(!IsBridgePollDue(bridge_index)) return false;

   bool polled = false;
   int max_cycles = inactive_probe ? 1 : 8;
   for(int cycle = 0; cycle < max_cycles; cycle++)
   {
      if(!IsValidBridgeIndex(bridge_index)) return polled;
      string response;
      string payload = "{\"maxRequests\":16}";
      int poll_timeout_ms = inactive_probe ? InactiveProbeTimeoutMs() : ActivePollTimeoutMs();
      uint started_ms = GetTickCount();
      if(!PostJsonWithTimeout(bridge_index, "/v1/rpc/poll", payload, poll_timeout_ms, response, inactive_probe))
      {
         uint elapsed_ms = GetTickCount() - started_ms;
         MarkBridgePollFailure(bridge_index, inactive_probe, elapsed_ms);
         return polled;
      }

      polled = true;
      MarkBridgePollSuccess(bridge_index);
      string session_id = ExtractString(response, "sessionId");
      if(session_id != "") g_bridges[bridge_index].session_id = session_id;
      string request_jsons[];
      int request_count = ExtractRpcRequests(response, request_jsons);
      if(request_count > 0)
         Print("AQE bridge[", bridge_index, "] executing ", request_count, " RPC request(s)");
      for(int i = 0; i < request_count; i++)
         ExecuteRpcRequest(bridge_index, request_jsons[i]);
      if(request_count <= 0)
         return true;
      if(IsValidBridgeIndex(bridge_index))
         g_bridges[bridge_index].next_poll_after_ms = 0;
   }

   if(IsValidBridgeIndex(bridge_index))
      g_bridges[bridge_index].next_poll_after_ms = 0;
   return polled;
}

void PollConnectedBridges()
{
   for(int i = 0; i < ArraySize(g_bridges); i++)
   {
      if(!IsBridgeSessionActive(i))
         continue;
      PollRpc(i, false);
   }
}

void ProbeOneDisconnectedBridge()
{
   int bridge_count = ArraySize(g_bridges);
   if(bridge_count <= 0)
      return;
   if(g_next_probe_bridge_index < 0 || g_next_probe_bridge_index >= bridge_count)
      g_next_probe_bridge_index = 0;

   for(int offset = 0; offset < bridge_count; offset++)
   {
      int bridge_index = (g_next_probe_bridge_index + offset) % bridge_count;
      if(IsBridgeSessionActive(bridge_index) || !IsBridgePollDue(bridge_index))
         continue;

      g_next_probe_bridge_index = (bridge_index + 1) % bridge_count;
      PollRpc(bridge_index, true);
      return;
   }
}

int OnInit()
{
   string bridge_token = BridgeToken();
   if(bridge_token == "")
   {
      Print("AqeMt5BridgeEA parameter error: InpBridgeToken is required and is shared by all bridge endpoints.");
      return INIT_PARAMETERS_INCORRECT;
   }

   int bridge_count = ConfigureBridgeConnections();
   if(bridge_count <= 0)
   {
      Print("AqeMt5BridgeEA parameter error: at least one bridge URL is required. Use InpBridgeUrl and optional comma-separated InpBridgeConnections.");
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
   Print("AqeMt5BridgeEA started. bridge_count=", bridge_count,
         " poll_ms=", InpPollIntervalMs,
         " timeout_ms=", InpRequestTimeoutMs,
         " probe_inactive=", (InpProbeInactiveConnections ? "aggressive" : "rate-limited"),
         " inactive_probe_ms=", InactiveProbeTimeoutMs(),
         " inactive_interval_ms=", InactiveProbeBaseIntervalMs(),
         " broker_utc_offset_seconds=", BrokerUtcOffsetSeconds());
   for(int i = 0; i < bridge_count; i++)
      Print("AqeMt5BridgeEA bridge[", i, "] url=", g_bridges[i].url);
   return INIT_SUCCEEDED;
}

void OnDeinit(const int reason)
{
   EventKillTimer();
   ClearRuntimeState();
}

void OnTimer()
{
   PollConnectedBridges();

   uint now_ms = GetTickCount();
   for(int i = 0; i < ArraySize(g_bridges); i++)
   {
      if(!IsValidBridgeIndex(i) || g_bridges[i].session_id == "")
         continue;
      if(HasElapsedMs(g_bridges[i].last_trade_event_flush_ms, TradeEventFlushIntervalMs()))
      {
         g_bridges[i].last_trade_event_flush_ms = now_ms;
         FlushTradeEvents(i);
      }
      if(HasElapsedMs(g_bridges[i].last_heartbeat_ms, 2000))
      {
         g_bridges[i].last_heartbeat_ms = now_ms;
         SendHeartbeat(i);
      }
      if((HasBridgeSubscriptions(i) || HasBridgeOrderRoutes(i)) && TimeCurrent() - g_bridges[i].last_snapshot > 30)
         SendSnapshot(i);
      if(HasBridgeSubscriptions(i) && HasElapsedMs(g_bridges[i].last_market_data_ms, ActivePollDelayMs()))
      {
         g_bridges[i].last_market_data_ms = now_ms;
         SendMarketData(i);
      }
   }

   ProbeOneDisconnectedBridge();
}

void OnTradeTransaction(
   const MqlTradeTransaction &trans,
   const MqlTradeRequest &request,
   const MqlTradeResult &result
)
{
   if(trans.order == 0) return;
   int route_bridge = FindOrderBridgeIndex(IntegerToString((long)trans.order));
   if(route_bridge < 0 && trans.deal > 0)
      route_bridge = FindOrderBridgeIndex(IntegerToString((long)trans.deal));
   if(route_bridge < 0)
      route_bridge = FindBridgeByComment(request.comment);

   string event_name = "Accepted";
   if(trans.type == TRADE_TRANSACTION_DEAL_ADD) event_name = "Filled";
   if(trans.type == TRADE_TRANSACTION_ORDER_DELETE)
   {
      if(!HistoryOrderSelect(trans.order))
         return;
      ENUM_ORDER_STATE order_state = (ENUM_ORDER_STATE)HistoryOrderGetInteger(trans.order, ORDER_STATE);
      if(order_state == ORDER_STATE_CANCELED)
         event_name = "Cancelled";
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
   ulong deal_position_id = 0;
   if(trans.type == TRADE_TRANSACTION_DEAL_ADD && HistoryDealSelect(trans.deal))
   {
      ENUM_DEAL_ENTRY deal_entry = (ENUM_DEAL_ENTRY)HistoryDealGetInteger(trans.deal, DEAL_ENTRY);
      ENUM_DEAL_TYPE deal_type = (ENUM_DEAL_TYPE)HistoryDealGetInteger(trans.deal, DEAL_TYPE);
      deal_position_id = (ulong)HistoryDealGetInteger(trans.deal, DEAL_POSITION_ID);
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
   if(route_bridge < 0)
      route_bridge = FindOrderBridgeIndex(event_order_id);
   if(route_bridge < 0 && deal_position_id > 0)
      route_bridge = FindOrderBridgeIndex(IntegerToString((long)deal_position_id));
   string order_key = IntegerToString((long)trans.order);
   string deal_key = trans.deal > 0 ? IntegerToString((long)trans.deal) : "";
   string position_key = deal_position_id > 0 ? IntegerToString((long)deal_position_id) : "";
   int route_index = FindOrderRouteIndexByAliases(event_order_id, order_key, deal_key, position_key);
   if(route_bridge < 0 && route_index >= 0)
      route_bridge = g_order_routes[route_index].bridge_index;
   if(route_bridge < 0 || !IsValidBridgeIndex(route_bridge) || g_bridges[route_bridge].session_id == "")
      return;

   string route_client_order_id = route_index >= 0 ? g_order_routes[route_index].client_order_id : "";
   string route_insight_id = route_index >= 0 ? g_order_routes[route_index].insight_id : "";
   string route_strategy_type = route_index >= 0 ? g_order_routes[route_index].strategy_type : "";

   RememberOrderRouteMetadata(route_bridge, event_order_id, route_client_order_id, route_insight_id, route_strategy_type);
   if(deal_position_id > 0) RememberOrderRouteMetadata(route_bridge, IntegerToString((long)deal_position_id), route_client_order_id, route_insight_id, route_strategy_type);
   if(trans.order > 0) RememberOrderRouteMetadata(route_bridge, IntegerToString((long)trans.order), route_client_order_id, route_insight_id, route_strategy_type);
   if(trans.deal > 0) RememberOrderRouteMetadata(route_bridge, IntegerToString((long)trans.deal), route_client_order_id, route_insight_id, route_strategy_type);

   string native_id = IntegerToString((int)trans.type) + ":" + IntegerToString((long)trans.order) + ":" + IntegerToString((long)trans.deal);
   string payload = "{"
      "\"nativeEventId\":\"" + JsonEscape(native_id) + "\","
      "\"event\":\"" + event_name + "\","
      "\"order\":" + OrderJson(event_order_id, symbol, volume, side, "Market", event_name, price, "", realized_pnl, has_realized_pnl, route_insight_id, route_strategy_type) +
   "}";
   QueueTradeEvent(route_bridge, payload);
}
