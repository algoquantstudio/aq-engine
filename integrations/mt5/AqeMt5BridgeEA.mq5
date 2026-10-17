//+------------------------------------------------------------------+
//| AqeMt5BridgeEA.mq5                                              |
//| Local RPC bridge EA for AlgoQuant Engine MT5 runtime integration.|
//+------------------------------------------------------------------+
#property strict
#property version "1.425"

#include <Trade/Trade.mqh>

input string InpBridgeUrl = "http://127.0.0.1:18080";
input string InpBridgeToken = "";
input string InpBridgeConnections = "";
enum AqeTransportMode
{
   AQE_TRANSPORT_AUTO = 0,
   AQE_TRANSPORT_WEBSOCKET = 1,
   AQE_TRANSPORT_HTTP = 2
};

input int InpConfigVersion = 2;
input AqeTransportMode InpTransportMode = AQE_TRANSPORT_AUTO;
input int InpSocketReconnectBaseMs = 100;
input int InpSocketReconnectMaxMs = 5000;
input int InpHttpFallbackAfterMs = 5000;
input int InpWebSocketProbeIntervalMs = 30000;
input int InpPollIntervalMs = 50;
input int InpRequestTimeoutMs = 5000;
input int InpTradeEventFlushIntervalMs = 10;
input int InpTradeEventPostTimeoutMs = 50;
input int InpTradeEventBatchSize = 32;
input int InpTradeEventQueueCapacity = 2048;
input int InpMarketDataIntervalMs = 25;
input int InpMarketDataPostTimeoutMs = 100;
// Evidence, not user settings. Only a continuously observed stable server-clock
// span can qualify historical conversions. Restart/gaps/DST do not backfill it.
int g_history_utc_offset_seconds = 86400;
datetime g_history_clock_utc_from = 0;
datetime g_history_clock_utc_through = 0;
datetime g_history_clock_last_observed = 0;
long g_history_clock_account = 0;
string g_history_clock_server = "";

const int TIMER_NONBLOCKING_WORK_BUDGET_MS = 8;
const int WS_RX_CAPACITY = 131072;
const int WS_TX_FRAGMENT_BYTES = 32768;
const int WS_MAX_MESSAGE_BYTES = 16 * 1024 * 1024;
const int WS_STATE_DISCONNECTED = 0;
const int WS_STATE_HANDSHAKE = 1;
const int WS_STATE_AWAIT_HELLO = 2;
const int WS_STATE_AWAIT_AUTH = 3;
const int WS_STATE_ACTIVE = 4;

// Optional bridge URLs must be discovered from the EA because WebRequest is
// client-initiated. Keep discovery automatic and rate-limited rather than
// exposing transport tuning controls to end users.
const int INACTIVE_PROBE_INTERVAL_MS = 500;
const int INACTIVE_PROBE_TIMEOUT_MS = 100;
const int INACTIVE_PROBE_MAX_COOLDOWN_MS = 2000;

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
   bool supports_fill_revisions;
   int socket_handle;
   int ws_state;
   uint ws_first_attempt_ms;
   uint ws_next_attempt_ms;
   int ws_reconnect_delay_ms;
   uint ws_last_probe_ms;
   uint ws_last_io_ms;
   ulong ws_last_acknowledged_sequence;
   ulong ws_inflight_trade_sequence;
   int ws_inflight_trade_count;
   int ws_rx_length;
   string ws_rx_hex;
};

struct BridgeSubscription
{
   int bridge_index;
   string symbol;
   string timeframe_code;
   datetime last_bar_time;
   datetime pending_bar_time;
   ulong pending_market_sequence;
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
   bool analytics_only;
};

struct PendingRpcCommand
{
   int bridge_index;
   string payload;
};

struct PendingRpcResponse
{
   int bridge_index;
   string request_id;
   string action;
   string payload;
};

// MT5 has no broker-native trailing-stop order. Keep the requested gap against
// the position and monotonically tighten POSITION_SL from the EA timer.
struct TrailingStopRoute
{
   string source_order_id;
   double gap;
};

BridgeConnection g_bridges[];
BridgeSubscription g_subscriptions[];
OrderRoute g_order_routes[];
PendingTradeEvent g_pending_trade_events[];
PendingRpcCommand g_pending_rpc_commands[];
PendingRpcResponse g_pending_rpc_responses[];
TrailingStopRoute g_trailing_stop_routes[];
MqlTradeTransaction g_pending_trade_transactions[];
MqlTradeRequest g_pending_trade_requests[];
MqlTradeResult g_pending_trade_results[];
long g_pending_trade_adapter_queued_us[];
int g_next_probe_bridge_index = 0;
int g_next_active_bridge_index = 0;
int g_next_market_bridge_index = 0;
int g_next_ws_bridge_index = 0;
ulong g_trade_event_seq = 0;
long g_last_fill_as_of_us = 0;
ulong g_trade_event_drop_count = 0;
uint g_last_trailing_stop_ms = 0;
int g_pending_trade_transaction_head = 0;
int g_pending_trade_transaction_count = 0;

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

string IsoTimeMillis(long unix_time_ms)
{
   datetime seconds = (datetime)(unix_time_ms / 1000);
   int millis = (int)(unix_time_ms % 1000);
   if(millis < 0) millis = -millis;
   MqlDateTime dt;
   TimeToStruct(seconds, dt);
   return StringFormat(
      "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ",
      dt.year,
      dt.mon,
      dt.day,
      dt.hour,
      dt.min,
      dt.sec,
      millis
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

// BEGIN automatic execution clock evidence.
void ObserveExecutionClock()
{
   datetime utc_time = TimeGMT();
   datetime broker_time = TimeTradeServer();
   datetime last_tick_time = TimeCurrent();
   long account = AccountInfoInteger(ACCOUNT_LOGIN);
   string server = AccountInfoString(ACCOUNT_SERVER);
   if(account != g_history_clock_account || server != g_history_clock_server)
   {
      g_history_utc_offset_seconds = 86400;
      g_history_clock_utc_from = 0;
      g_history_clock_utc_through = 0;
      g_history_clock_last_observed = 0;
      g_history_clock_account = account;
      g_history_clock_server = server;
   }
   // TimeTradeServer can advance locally without fresh broker traffic. Do not
   // extend an evidence window from that estimate while disconnected/idle.
   if(!TerminalInfoInteger(TERMINAL_CONNECTED) || utc_time <= 0 || broker_time <= 0
      || last_tick_time <= 0 || MathAbs((double)(broker_time - last_tick_time)) > 5.0) return;
   if(account <= 0 || server == "") return;
   if(utc_time == g_history_clock_last_observed && account == g_history_clock_account
      && server == g_history_clock_server) return;
   int offset = RoundOffsetToMinute((int)(broker_time - utc_time));
   if(offset < -50400 || offset > 50400
      || MathAbs((double)(broker_time - utc_time - offset)) > 2.0) return;
   if(g_history_utc_offset_seconds != offset || g_history_clock_last_observed <= 0
      || account != g_history_clock_account || server != g_history_clock_server
      || utc_time < g_history_clock_last_observed || utc_time - g_history_clock_last_observed > 10)
   {
      g_history_utc_offset_seconds = offset;
      // Exclude the partially observed first second.
      g_history_clock_utc_from = utc_time + 1;
      g_history_clock_utc_through = 0;
   }
   // Exclude the partially observed latest second as well.
   g_history_clock_utc_through = utc_time - 1;
   g_history_clock_last_observed = utc_time;
   g_history_clock_account = account;
   g_history_clock_server = server;
}
// END automatic execution clock evidence.

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

bool WebSocketEnabled()
{
   return InpTransportMode != AQE_TRANSPORT_HTTP;
}

bool IsWebSocketActive(int bridge_index)
{
   return IsValidBridgeIndex(bridge_index)
      && g_bridges[bridge_index].socket_handle != INVALID_HANDLE
      && g_bridges[bridge_index].ws_state == WS_STATE_ACTIVE
      && SocketIsConnected(g_bridges[bridge_index].socket_handle);
}

int SocketReconnectBaseMs()
{
   return ClampInt(InpSocketReconnectBaseMs, 50, 60000);
}

int SocketReconnectMaxMs()
{
   return ClampInt(InpSocketReconnectMaxMs, SocketReconnectBaseMs(), 300000);
}

int HttpFallbackAfterMs()
{
   return ClampInt(InpHttpFallbackAfterMs, 250, 300000);
}

int WebSocketProbeIntervalMs()
{
   return ClampInt(InpWebSocketProbeIntervalMs, 1000, 600000);
}

bool HttpTransportAllowed(int bridge_index)
{
   if(InpTransportMode == AQE_TRANSPORT_HTTP)
      return true;
   if(InpTransportMode == AQE_TRANSPORT_WEBSOCKET)
      return false;
   // A synchronous WebRequest to any dead endpoint can block every other
   // connection for 14+ seconds under Wine, even with a 100 ms timeout. Auto
   // multi-bridge deployments therefore use sockets only. Explicit HTTP mode
   // remains available for legacy single-transport installations.
   if(ArraySize(g_bridges) > 1)
      return false;
   if(IsWebSocketActive(bridge_index))
      return false;
   if(!IsValidBridgeIndex(bridge_index))
      return false;
   uint first_attempt = g_bridges[bridge_index].ws_first_attempt_ms;
   return first_attempt > 0 && HasElapsedMs(first_attempt, HttpFallbackAfterMs());
}

bool ParseBridgeSocketEndpoint(string bridge_url, string &host, int &port)
{
   host = "";
   port = 0;
   string value = NormalizeBridgeUrl(bridge_url);
   int scheme_end = StringFind(value, "://");
   string scheme = scheme_end >= 0 ? StringSubstr(value, 0, scheme_end) : "http";
   if(scheme != "http" && scheme != "ws")
      return false;
   string authority = scheme_end >= 0 ? StringSubstr(value, scheme_end + 3) : value;
   int slash = StringFind(authority, "/");
   if(slash >= 0)
      authority = StringSubstr(authority, 0, slash);
   int colon = StringFind(authority, ":");
   if(colon >= 0)
   {
      host = StringSubstr(authority, 0, colon);
      port = (int)StringToInteger(StringSubstr(authority, colon + 1));
   }
   else
   {
      host = authority;
      port = 80;
   }
   return host != "" && port > 0 && port <= 65535;
}

string Base64Encode16(uchar &bytes[])
{
   string alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
   string encoded = "";
   int count = MathMin(16, ArraySize(bytes));
   for(int i = 0; i < count; i += 3)
   {
      int remaining = count - i;
      int a = (int)bytes[i];
      int b = remaining > 1 ? (int)bytes[i + 1] : 0;
      int c = remaining > 2 ? (int)bytes[i + 2] : 0;
      int packed = (a << 16) | (b << 8) | c;
      encoded += StringSubstr(alphabet, (packed >> 18) & 63, 1);
      encoded += StringSubstr(alphabet, (packed >> 12) & 63, 1);
      encoded += remaining > 1 ? StringSubstr(alphabet, (packed >> 6) & 63, 1) : "=";
      encoded += remaining > 2 ? StringSubstr(alphabet, packed & 63, 1) : "=";
   }
   return encoded;
}

string WebSocketByteHex(uchar value)
{
   string digits = "0123456789ABCDEF";
   return StringSubstr(digits, ((int)value >> 4) & 15, 1)
      + StringSubstr(digits, (int)value & 15, 1);
}

int WebSocketHexNibble(int character)
{
   if(character >= '0' && character <= '9') return character - '0';
   if(character >= 'A' && character <= 'F') return character - 'A' + 10;
   if(character >= 'a' && character <= 'f') return character - 'a' + 10;
   return 0;
}

uchar WebSocketReceiveByte(int bridge_index, int index)
{
   if(!IsValidBridgeIndex(bridge_index) || index < 0 || index >= g_bridges[bridge_index].ws_rx_length)
      return 0;
   int offset = index * 2;
   int high = WebSocketHexNibble(StringGetCharacter(g_bridges[bridge_index].ws_rx_hex, offset));
   int low = WebSocketHexNibble(StringGetCharacter(g_bridges[bridge_index].ws_rx_hex, offset + 1));
   return (uchar)((high << 4) | low);
}

void ShiftWebSocketReceiveBuffer(int bridge_index, int consumed)
{
   if(!IsValidBridgeIndex(bridge_index) || consumed <= 0)
      return;
   int length = g_bridges[bridge_index].ws_rx_length;
   if(consumed >= length)
   {
      g_bridges[bridge_index].ws_rx_length = 0;
      g_bridges[bridge_index].ws_rx_hex = "";
      return;
   }
   g_bridges[bridge_index].ws_rx_hex = StringSubstr(
      g_bridges[bridge_index].ws_rx_hex,
      consumed * 2
   );
   g_bridges[bridge_index].ws_rx_length = length - consumed;
}

void CloseWebSocket(int bridge_index, string reason, bool schedule_retry = true)
{
   if(!IsValidBridgeIndex(bridge_index))
      return;
   if(g_bridges[bridge_index].socket_handle != INVALID_HANDLE)
      SocketClose(g_bridges[bridge_index].socket_handle);
   g_bridges[bridge_index].socket_handle = INVALID_HANDLE;
   g_bridges[bridge_index].ws_state = WS_STATE_DISCONNECTED;
   g_bridges[bridge_index].ws_rx_length = 0;
   g_bridges[bridge_index].ws_rx_hex = "";
   g_bridges[bridge_index].ws_inflight_trade_count = 0;
   g_bridges[bridge_index].ws_inflight_trade_sequence = 0;
   for(int i = 0; i < ArraySize(g_subscriptions); i++)
   {
      if(g_subscriptions[i].bridge_index == bridge_index)
         g_subscriptions[i].pending_market_sequence = 0;
   }
   if(schedule_retry)
   {
      int delay = g_bridges[bridge_index].ws_reconnect_delay_ms;
      if(delay <= 0) delay = SocketReconnectBaseMs();
      g_bridges[bridge_index].ws_next_attempt_ms = GetTickCount() + (uint)delay;
      g_bridges[bridge_index].ws_reconnect_delay_ms = MathMin(delay * 2, SocketReconnectMaxMs());
   }
   if(reason != "")
      Print("AQE bridge[", bridge_index, "] WebSocket disconnected: ", reason);
}

bool SendWebSocketBytesFrame(int bridge_index, int opcode, const uchar &body[], int offset, int payload_length, bool final_frame)
{
   if(!IsValidBridgeIndex(bridge_index)
      || g_bridges[bridge_index].socket_handle == INVALID_HANDLE
      || !SocketIsConnected(g_bridges[bridge_index].socket_handle))
      return false;

   int extended = payload_length <= 125 ? 0 : (payload_length <= 65535 ? 2 : 8);
   int header_length = 2 + extended + 4;
   uchar frame[];
   ArrayResize(frame, header_length + payload_length);
   frame[0] = (uchar)((final_frame ? 0x80 : 0) | (opcode & 0x0F));
   int cursor = 2;
   if(payload_length <= 125)
      frame[1] = (uchar)(0x80 | payload_length);
   else if(payload_length <= 65535)
   {
      frame[1] = (uchar)(0x80 | 126);
      frame[2] = (uchar)((payload_length >> 8) & 0xFF);
      frame[3] = (uchar)(payload_length & 0xFF);
      cursor = 4;
   }
   else
   {
      frame[1] = (uchar)(0x80 | 127);
      ulong size = (ulong)payload_length;
      for(int i = 0; i < 8; i++)
         frame[2 + i] = (uchar)((size >> ((7 - i) * 8)) & 0xFF);
      cursor = 10;
   }

   uchar mask[4];
   for(int i = 0; i < 4; i++)
   {
      mask[i] = (uchar)(MathRand() & 0xFF);
      frame[cursor + i] = mask[i];
   }
   cursor += 4;
   for(int i = 0; i < payload_length; i++)
      frame[cursor + i] = (uchar)(body[offset + i] ^ mask[i % 4]);

   if(SocketIsWritable(g_bridges[bridge_index].socket_handle) == 0)
      return false;
   int sent = SocketSend(g_bridges[bridge_index].socket_handle, frame, ArraySize(frame));
   if(sent != ArraySize(frame))
   {
      CloseWebSocket(bridge_index, "short socket write", true);
      return false;
   }
   g_bridges[bridge_index].ws_last_io_ms = GetTickCount();
   return true;
}

bool SendWebSocketFrame(int bridge_index, int opcode, string payload)
{
   uchar body[];
   int converted = StringToCharArray(payload, body, 0, WHOLE_ARRAY, CP_UTF8);
   int length = MathMax(0, converted - 1);
   return SendWebSocketBytesFrame(bridge_index, opcode, body, 0, length, true);
}

bool SendWebSocketJson(int bridge_index, string payload)
{
   uchar body[];
   int converted = StringToCharArray(payload, body, 0, WHOLE_ARRAY, CP_UTF8);
   int length = MathMax(0, converted - 1);
   if(length > WS_MAX_MESSAGE_BYTES)
   {
      Print("AQE bridge[", bridge_index, "] outgoing message exceeds 16 MiB; bytes=", length);
      return false;
   }
   // RFC 6455 continuation frames: split UTF-8 bytes, not MQL characters.
   // AQE retains its 64 KiB frame limit and reassembles at most 16 MiB.
   for(int offset = 0; offset < length; offset += WS_TX_FRAGMENT_BYTES)
   {
      int count = MathMin(WS_TX_FRAGMENT_BYTES, length - offset);
      if(!SendWebSocketBytesFrame(bridge_index, offset == 0 ? 0x1 : 0x0,
            body, offset, count, offset + count == length))
      {
         // Never start a different data message in the middle of a failed one.
         CloseWebSocket(bridge_index, "fragmented message write failed", true);
         return false;
      }
   }
   return true;
}

bool StartWebSocket(int bridge_index)
{
   if(!WebSocketEnabled() || !IsValidBridgeIndex(bridge_index))
      return false;
   string host;
   int port = 0;
   if(!ParseBridgeSocketEndpoint(g_bridges[bridge_index].url, host, port))
      return false;

   if(g_bridges[bridge_index].ws_first_attempt_ms == 0)
      g_bridges[bridge_index].ws_first_attempt_ms = MathMax(1, (int)GetTickCount());
   if(g_bridges[bridge_index].socket_handle != INVALID_HANDLE)
      SocketClose(g_bridges[bridge_index].socket_handle);
   int socket_handle = SocketCreate();
   if(socket_handle == INVALID_HANDLE)
   {
      CloseWebSocket(bridge_index, "SocketCreate failed", true);
      return true;
   }
   g_bridges[bridge_index].socket_handle = socket_handle;
   // Bound OS-level sends as well as explicit SocketRead/SocketConnect calls.
   // Otherwise a handshake write to a stalled peer can block the whole EA.
   if(!SocketTimeouts(socket_handle, 50, 10))
   {
      CloseWebSocket(bridge_index, "setting socket timeouts failed", true);
      return true;
   }
   if(!SocketConnect(socket_handle, host, (uint)port, (uint)MathMin(100, SocketReconnectBaseMs())))
   {
      CloseWebSocket(bridge_index, "connection failed", true);
      return true;
   }

   uchar nonce_bytes[];
   ArrayResize(nonce_bytes, 16);
   for(int i = 0; i < 16; i++) nonce_bytes[i] = (uchar)(MathRand() & 0xFF);
   string key = Base64Encode16(nonce_bytes);
   string request =
      "GET /v2/bridge HTTP/1.1\r\n"
      "Host: " + host + ":" + IntegerToString(port) + "\r\n"
      "Upgrade: websocket\r\n"
      "Connection: Upgrade\r\n"
      "Sec-WebSocket-Version: 13\r\n"
      "Sec-WebSocket-Key: " + key + "\r\n\r\n";
   uchar bytes[];
   int converted = StringToCharArray(request, bytes, 0, WHOLE_ARRAY, CP_UTF8);
   int byte_count = MathMax(0, converted - 1);
   int sent = SocketSend(socket_handle, bytes, byte_count);
   if(sent != byte_count)
   {
      CloseWebSocket(bridge_index, "handshake write failed", true);
      return true;
   }
   g_bridges[bridge_index].ws_state = WS_STATE_HANDSHAKE;
   g_bridges[bridge_index].ws_rx_length = 0;
   g_bridges[bridge_index].ws_rx_hex = "";
   g_bridges[bridge_index].ws_last_io_ms = GetTickCount();
   return true;
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
   if(delay_ms < 50) delay_ms = 50;
   if(delay_ms > 1000) delay_ms = 1000;
   return delay_ms;
}

int MarketDataIntervalMs()
{
   int interval_ms = InpMarketDataIntervalMs;
   if(interval_ms < 10) interval_ms = 10;
   if(interval_ms > 1000) interval_ms = 1000;
   return interval_ms;
}

int EaTimerIntervalMs()
{
   int trade_interval_ms = InpTradeEventFlushIntervalMs;
   if(trade_interval_ms < 10) trade_interval_ms = 10;
   if(trade_interval_ms > 5000) trade_interval_ms = 5000;
   int interval_ms = trade_interval_ms < MarketDataIntervalMs()
      ? trade_interval_ms
      : MarketDataIntervalMs();
   if(interval_ms < 10) interval_ms = 10;
   if(interval_ms > 100) interval_ms = 100;
   return interval_ms;
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
   return INACTIVE_PROBE_TIMEOUT_MS;
}

int InactiveProbeBaseIntervalMs()
{
   return INACTIVE_PROBE_INTERVAL_MS;
}

int InactiveProbeMaxCooldownMs()
{
   return INACTIVE_PROBE_MAX_COOLDOWN_MS;
}

int InactiveProbeFailureDelayMs(int consecutive_failures, uint duration_ms)
{
   int delay_ms = FailedPollDelayMs(consecutive_failures);
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
   if(path == "/v1/market-data")
      max_timeout_ms = MathMax(100, InpMarketDataPostTimeoutMs);
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

bool SendMappedWebSocketPayload(int bridge_index, string path, string payload)
{
   if(!IsWebSocketActive(bridge_index))
      return false;
   if(path == "/v1/heartbeat")
      return SendWebSocketJson(bridge_index, "{\"type\":\"Heartbeat\",\"payload\":" + payload + "}");
   if(path == "/v1/market-data")
      return SendWebSocketJson(bridge_index, "{\"type\":\"MarketDataBatch\",\"payload\":" + payload + "}");
   if(path == "/v1/snapshot")
      return SendWebSocketJson(bridge_index, "{\"type\":\"AccountSnapshot\",\"payload\":" + payload + "}");
   if(path == "/v1/rpc/response")
      return SendWebSocketJson(bridge_index, "{\"type\":\"BrokerCommandResult\",\"payload\":" + payload + "}");
   return false;
}

bool PostJsonWithTimeout(int bridge_index, string path, string payload, int timeout_ms, string &response, bool quiet_transport_error = false)
{
   if(!IsValidBridgeIndex(bridge_index)) return false;
   if(IsWebSocketActive(bridge_index) && path != "/v1/rpc/poll" && path != "/v1/trade-events")
   {
      response = "";
      return SendMappedWebSocketPayload(bridge_index, path, payload);
   }
   if(!HttpTransportAllowed(bridge_index))
      return false;
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
   // Capability is re-established only by a successful response. An older or
   // failed peer must not inherit support from the previous connection.
   g_bridges[bridge_index].supports_fill_revisions = false;
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
   g_bridges[bridge_index].supports_fill_revisions = ExtractNumber(response, "executionFillRevisionVersion", 0.0) == 1.0;
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

void QueueTradeEvent(int bridge_index, string payload, bool analytics_only = false)
{
   if(!IsValidBridgeIndex(bridge_index) || payload == "") return;
   if(ArraySize(g_pending_trade_events) >= TradeEventQueueCapacity())
   {
      // Preserve the FIFO prefix. Dropping the oldest event would allow a later
      // fill/close to overtake the event which established its prior state.
      g_trade_event_drop_count++;
      Print("AQE trade-event queue is full; rejected newest event to preserve FIFO ordering. dropped=",
            (long)g_trade_event_drop_count);
      return;
   }
   int index = ArraySize(g_pending_trade_events);
   ArrayResize(g_pending_trade_events, index + 1);
   g_trade_event_seq++;
   g_pending_trade_events[index].bridge_index = bridge_index;
   g_pending_trade_events[index].event_seq = g_trade_event_seq;
   g_pending_trade_events[index].payload = payload;
   g_pending_trade_events[index].analytics_only = analytics_only;
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

bool FlushTradeEvents(int bridge_index)
{
   if(!IsValidBridgeIndex(bridge_index) || g_bridges[bridge_index].session_id == "") return false;
   if(!g_bridges[bridge_index].supports_fill_revisions)
   {
      // A pending revision may outlive an engine reconnect/downgrade. Older
      // engines ignore unknown JSON flags, so never send it as an order event.
      int write = 0;
      for(int read = 0; read < ArraySize(g_pending_trade_events); read++)
      {
         if(g_pending_trade_events[read].bridge_index == bridge_index && g_pending_trade_events[read].analytics_only)
         {
            g_trade_event_drop_count++;
            continue;
         }
         if(write != read) g_pending_trade_events[write] = g_pending_trade_events[read];
         write++;
      }
      ArrayResize(g_pending_trade_events, write);
   }
   int batch_size = TradeEventBatchSize();
   string events = "";
   int emitted = 0;
   bool revisions_only = false;
   for(int i = 0; i < ArraySize(g_pending_trade_events) && emitted < batch_size; i++)
   {
      if(g_pending_trade_events[i].bridge_index != bridge_index) continue;
      if(emitted == 0) revisions_only = g_pending_trade_events[i].analytics_only;
      else if(revisions_only != g_pending_trade_events[i].analytics_only) break;
      string event_payload = g_pending_trade_events[i].payload;
      if(StringLen(event_payload) <= 1) continue;
      if(emitted > 0) events += ",";
      events += StringSubstr(event_payload, 0, StringLen(event_payload) - 1)
         + ",\"eventSeq\":" + IntegerToString((long)g_pending_trade_events[i].event_seq) + "}";
      emitted++;
   }
   if(emitted <= 0) return false;

   string response;
   string payload = "{"
      "\"events\":[" + events + "],"
      "\"droppedCount\":" + IntegerToString((long)g_trade_event_drop_count) +
   "}";
   if(IsWebSocketActive(bridge_index))
   {
      if(g_bridges[bridge_index].ws_inflight_trade_count > 0)
         return false;
      ulong last_sequence = 0;
      int seen = 0;
      for(int i = 0; i < ArraySize(g_pending_trade_events) && seen < emitted; i++)
      {
         if(g_pending_trade_events[i].bridge_index != bridge_index) continue;
         last_sequence = g_pending_trade_events[i].event_seq;
         seen++;
      }
      string frame = "{"
         "\"type\":\"" + (revisions_only ? "ExecutionFillRevisionBatch" : "TradeEventBatch") + "\","
         "\"sequence\":" + IntegerToString((long)last_sequence) + ","
         "\"events\":[" + events + "],"
         "\"droppedCount\":" + IntegerToString((long)g_trade_event_drop_count) +
      "}";
      if(SendWebSocketJson(bridge_index, frame))
      {
         g_bridges[bridge_index].ws_inflight_trade_count = emitted;
         g_bridges[bridge_index].ws_inflight_trade_sequence = last_sequence;
      }
      return true;
   }

   // The distinct route/frame is a hard boundary: an older engine rejects it
   // even if it restarted between capability negotiation and this delivery.
   if(PostJson(bridge_index, revisions_only ? "/v1/execution-fill-revisions" : "/v1/trade-events", payload, response))
   {
      g_trade_event_drop_count = 0;
      RemoveQueuedTradeEventsForBridge(bridge_index, emitted);
   }
   // A request was attempted. On failure the same front-of-queue batch remains
   // in place and is retried before any later batch for this bridge.
   return true;
}

bool FlushNextTradeEventBatch()
{
   int total = ArraySize(g_pending_trade_events);
   for(int i = 0; i < total; i++)
   {
      int bridge_index = g_pending_trade_events[i].bridge_index;
      if(!IsBridgeSessionActive(bridge_index))
         continue;
      if(!HasElapsedMs(g_bridges[bridge_index].last_trade_event_flush_ms, TradeEventFlushIntervalMs()))
         continue;
      g_bridges[bridge_index].last_trade_event_flush_ms = GetTickCount();
      return FlushTradeEvents(bridge_index);
   }
   return false;
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
       || retcode == TRADE_RETCODE_PLACED
       // PositionModify/OrderModify returns NO_CHANGES when an idempotent
       // protection update has already been applied. That is success from
       // AQE's perspective and must not reject an active filled insight.
       || retcode == TRADE_RETCODE_NO_CHANGES;
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

string ExtractJsonObject(string json, string key)
{
   string needle = "\"" + key + "\":";
   int start = StringFind(json, needle);
   if(start < 0) return "";
   start += StringLen(needle);
   while(start < StringLen(json) && StringGetCharacter(json, start) == ' ') start++;
   if(start >= StringLen(json) || StringGetCharacter(json, start) != '{') return "";

   int depth = 0;
   bool in_string = false;
   bool escaped = false;
   for(int i = start; i < StringLen(json); i++)
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
            return StringSubstr(json, start, i - start + 1);
      }
   }
   return "";
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

bool ClosePositionWithComment(ulong position_ticket, string symbol, double qty, string comment, uint &retcode, ulong &deal_ticket)
{
   deal_ticket = 0;
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
   deal_ticket = result.deal;
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

bool ReceiveWebSocketBytes(int bridge_index)
{
   if(!IsValidBridgeIndex(bridge_index)
      || g_bridges[bridge_index].socket_handle == INVALID_HANDLE)
      return false;
   uint readable = SocketIsReadable(g_bridges[bridge_index].socket_handle);
   if(readable == 0)
      return false;
   int available_capacity = WS_RX_CAPACITY - g_bridges[bridge_index].ws_rx_length;
   if(available_capacity <= 0)
   {
      CloseWebSocket(bridge_index, "receive buffer exhausted", true);
      return true;
   }
   int request_count = (int)MathMin((double)readable, (double)MathMin(8192, available_capacity));
   uchar chunk[];
   ArrayResize(chunk, request_count);
   int received = SocketRead(g_bridges[bridge_index].socket_handle, chunk, request_count, 1);
   if(received <= 0)
   {
      if(!SocketIsConnected(g_bridges[bridge_index].socket_handle))
         CloseWebSocket(bridge_index, "peer closed the connection", true);
      return true;
   }
   for(int i = 0; i < received; i++)
      g_bridges[bridge_index].ws_rx_hex += WebSocketByteHex(chunk[i]);
   g_bridges[bridge_index].ws_rx_length += received;
   g_bridges[bridge_index].ws_last_io_ms = GetTickCount();
   return true;
}

bool ProcessWebSocketHandshake(int bridge_index)
{
   if(!IsValidBridgeIndex(bridge_index)
      || g_bridges[bridge_index].ws_state != WS_STATE_HANDSHAKE)
      return false;
   int length = g_bridges[bridge_index].ws_rx_length;
   int header_end = -1;
   for(int i = 0; i + 3 < length; i++)
   {
      if(WebSocketReceiveByte(bridge_index, i) == 13
         && WebSocketReceiveByte(bridge_index, i + 1) == 10
         && WebSocketReceiveByte(bridge_index, i + 2) == 13
         && WebSocketReceiveByte(bridge_index, i + 3) == 10)
      {
         header_end = i + 4;
         break;
      }
   }
   if(header_end < 0)
      return false;

   char header_bytes[];
   ArrayResize(header_bytes, header_end + 1);
   for(int i = 0; i < header_end; i++)
      header_bytes[i] = (char)WebSocketReceiveByte(bridge_index, i);
   header_bytes[header_end] = 0;
   string header = CharArrayToString(header_bytes, 0, header_end, CP_UTF8);
   string normalized_header = header;
   StringToLower(normalized_header);
   if(StringFind(normalized_header, " 101 ") < 0
      || StringFind(normalized_header, "upgrade: websocket") < 0)
   {
      CloseWebSocket(bridge_index, "server rejected /v2/bridge upgrade", true);
      return true;
   }
   ShiftWebSocketReceiveBuffer(bridge_index, header_end);
   g_bridges[bridge_index].ws_state = WS_STATE_AWAIT_HELLO;
   return true;
}

void HandleWebSocketText(int bridge_index, string json)
{
   string frame_type = ExtractString(json, "type");
   if(frame_type == "Hello")
   {
      string nonce = ExtractString(json, "nonce");
      string session_id = ExtractString(json, "bridgeSessionId");
      if(nonce == "" || session_id == "")
      {
         CloseWebSocket(bridge_index, "invalid Hello frame", true);
         return;
      }
      bool session_changed = g_bridges[bridge_index].session_id != ""
         && g_bridges[bridge_index].session_id != session_id;
      if(session_changed)
         ClearBridgeSessionRuntime(bridge_index);
      g_bridges[bridge_index].session_id = session_id;
      g_bridges[bridge_index].supports_fill_revisions = ExtractNumber(json, "executionFillRevisionVersion", 0.0) == 1.0;
      string authenticate = "{"
         "\"type\":\"Authenticate\","
         "\"nonce\":\"" + JsonEscape(nonce) + "\","
         "\"token\":\"" + JsonEscape(BridgeToken()) + "\","
         "\"sessionId\":\"" + JsonEscape(session_id) + "\","
         "\"lastAcknowledgedSequence\":"
            + IntegerToString((long)g_bridges[bridge_index].ws_last_acknowledged_sequence) +
      "}";
      if(!SendWebSocketJson(bridge_index, authenticate))
      {
         CloseWebSocket(bridge_index, "authentication frame write failed", true);
         return;
      }
      g_bridges[bridge_index].ws_state = WS_STATE_AWAIT_AUTH;
      return;
   }
   if(frame_type == "Authenticated")
   {
      g_bridges[bridge_index].ws_state = WS_STATE_ACTIVE;
      g_bridges[bridge_index].ws_reconnect_delay_ms = SocketReconnectBaseMs();
      g_bridges[bridge_index].ws_next_attempt_ms = 0;
      g_bridges[bridge_index].session_logged = true;
      Print("AQE bridge[", bridge_index, "] persistent /v2/bridge authenticated url=",
            g_bridges[bridge_index].url,
            " session=", BridgeSessionShort(bridge_index));
      return;
   }
   if(frame_type == "Rejected")
   {
      CloseWebSocket(bridge_index, "server rejected transport: " + ExtractString(json, "reason"), true);
      return;
   }
   if(frame_type == "BatchAcknowledgement")
   {
      ulong acknowledged = (ulong)ExtractNumber(json, "brokerEventSequence", 0.0);
      string correlation_id = ExtractString(json, "correlationId");
      if(acknowledged > g_bridges[bridge_index].ws_last_acknowledged_sequence)
         g_bridges[bridge_index].ws_last_acknowledged_sequence = acknowledged;
      if(g_bridges[bridge_index].ws_inflight_trade_count > 0
         && acknowledged >= g_bridges[bridge_index].ws_inflight_trade_sequence)
      {
         RemoveQueuedTradeEventsForBridge(
            bridge_index,
            g_bridges[bridge_index].ws_inflight_trade_count
         );
         g_bridges[bridge_index].ws_inflight_trade_count = 0;
         g_bridges[bridge_index].ws_inflight_trade_sequence = 0;
         g_trade_event_drop_count = 0;
      }
      if(StringFind(correlation_id, "market:") == 0)
      {
         ulong market_sequence = (ulong)StringToInteger(StringSubstr(correlation_id, 7));
         for(int i = 0; i < ArraySize(g_subscriptions); i++)
         {
            if(g_subscriptions[i].bridge_index == bridge_index
               && g_subscriptions[i].pending_market_sequence == market_sequence)
            {
               g_subscriptions[i].last_bar_time = g_subscriptions[i].pending_bar_time;
               g_subscriptions[i].pending_bar_time = 0;
               g_subscriptions[i].pending_market_sequence = 0;
            }
         }
      }
      return;
   }
   if(frame_type == "BrokerCommand")
   {
      string request = ExtractJsonObject(json, "request");
      if(request == "")
         return;
      int index = ArraySize(g_pending_rpc_commands);
      ArrayResize(g_pending_rpc_commands, index + 1);
      g_pending_rpc_commands[index].bridge_index = bridge_index;
      g_pending_rpc_commands[index].payload = request;
      return;
   }
   if(frame_type == "Backpressure")
   {
      // Critical trade batches remain at the front of the FIFO and are retried after the
      // persistent connection recovers. Disconnecting prevents a busy-loop on a slow AQE.
      CloseWebSocket(bridge_index, "AQE reported backpressure", true);
      return;
   }
}

bool ProcessOneWebSocketFrame(int bridge_index)
{
   if(!IsValidBridgeIndex(bridge_index))
      return false;
   int length = g_bridges[bridge_index].ws_rx_length;
   if(length < 2)
      return false;
   int first = (int)WebSocketReceiveByte(bridge_index, 0);
   int second = (int)WebSocketReceiveByte(bridge_index, 1);
   int opcode = first & 0x0F;
   bool masked = (second & 0x80) != 0;
   ulong payload_length = (ulong)(second & 0x7F);
   int cursor = 2;
   if(payload_length == 126)
   {
      if(length < 4) return false;
      payload_length = ((ulong)WebSocketReceiveByte(bridge_index, 2) << 8)
         | (ulong)WebSocketReceiveByte(bridge_index, 3);
      cursor = 4;
   }
   else if(payload_length == 127)
   {
      if(length < 10) return false;
      payload_length = 0;
      for(int i = 0; i < 8; i++)
         payload_length = (payload_length << 8) | (ulong)WebSocketReceiveByte(bridge_index, 2 + i);
      cursor = 10;
   }
   uchar mask[4];
   if(masked)
   {
      if(length < cursor + 4) return false;
      for(int i = 0; i < 4; i++) mask[i] = WebSocketReceiveByte(bridge_index, cursor + i);
      cursor += 4;
   }
   if(payload_length + (ulong)cursor > (ulong)WS_RX_CAPACITY)
   {
      CloseWebSocket(bridge_index, "incoming frame exceeds receive capacity", true);
      return true;
   }
   if((ulong)length < (ulong)cursor + payload_length)
      return false;

   char payload_bytes[];
   ArrayResize(payload_bytes, (int)payload_length + 1);
   for(int i = 0; i < (int)payload_length; i++)
   {
      uchar value = WebSocketReceiveByte(bridge_index, cursor + i);
      if(masked) value = (uchar)(value ^ mask[i % 4]);
      payload_bytes[i] = (char)value;
   }
   payload_bytes[(int)payload_length] = 0;
   string payload = CharArrayToString(payload_bytes, 0, (int)payload_length, CP_UTF8);
   ShiftWebSocketReceiveBuffer(bridge_index, cursor + (int)payload_length);

   if(opcode == 0x1)
      HandleWebSocketText(bridge_index, payload);
   else if(opcode == 0x8)
      CloseWebSocket(bridge_index, "server closed the connection", true);
   else if(opcode == 0x9)
      SendWebSocketFrame(bridge_index, 0xA, payload);
   return true;
}

bool ServiceWebSocketBridge(int bridge_index)
{
   if(!WebSocketEnabled() || !IsValidBridgeIndex(bridge_index))
      return false;
   if(g_bridges[bridge_index].socket_handle != INVALID_HANDLE
      && !SocketIsConnected(g_bridges[bridge_index].socket_handle))
      CloseWebSocket(bridge_index, "socket is no longer connected", true);

   if(g_bridges[bridge_index].socket_handle != INVALID_HANDLE)
   {
      bool received = ReceiveWebSocketBytes(bridge_index);
      if(g_bridges[bridge_index].ws_state != WS_STATE_ACTIVE
         && HasElapsedMs(g_bridges[bridge_index].ws_last_io_ms, HttpFallbackAfterMs()))
      {
         CloseWebSocket(bridge_index, "persistent transport negotiation timed out", true);
         return true;
      }
      if(g_bridges[bridge_index].ws_state == WS_STATE_HANDSHAKE)
         ProcessWebSocketHandshake(bridge_index);
      uint started_ms = GetTickCount();
      // Drain buffered frames too, not just one per new socket read. Bounds
      // preserve fairness for trade events, other bridges and current candles.
      for(int frames = 0; frames < 32; frames++)
      {
         if(g_bridges[bridge_index].ws_state < WS_STATE_AWAIT_HELLO
            || !ProcessOneWebSocketFrame(bridge_index)) break;
         received = true;
         if(HasElapsedMs(started_ms, TIMER_NONBLOCKING_WORK_BUDGET_MS)) break;
      }
      return received;
   }

   uint now_ms = GetTickCount();
   if(g_bridges[bridge_index].ws_next_attempt_ms > 0
      && now_ms < g_bridges[bridge_index].ws_next_attempt_ms)
      return false;
   if(InpTransportMode == AQE_TRANSPORT_AUTO
      && IsBridgeSessionActive(bridge_index)
      && g_bridges[bridge_index].ws_last_probe_ms > 0
      && !HasElapsedMs(g_bridges[bridge_index].ws_last_probe_ms, WebSocketProbeIntervalMs()))
      return false;
   g_bridges[bridge_index].ws_last_probe_ms = now_ms;
   return StartWebSocket(bridge_index);
}

bool ServiceOneWebSocket()
{
   int count = ArraySize(g_bridges);
   if(!WebSocketEnabled() || count <= 0)
      return false;
   if(g_next_ws_bridge_index < 0 || g_next_ws_bridge_index >= count)
      g_next_ws_bridge_index = 0;
   for(int offset = 0; offset < count; offset++)
   {
      int bridge_index = (g_next_ws_bridge_index + offset) % count;
      bool due = g_bridges[bridge_index].socket_handle != INVALID_HANDLE
         || g_bridges[bridge_index].ws_next_attempt_ms == 0
         || GetTickCount() >= g_bridges[bridge_index].ws_next_attempt_ms;
      if(!due) continue;
      g_next_ws_bridge_index = (bridge_index + 1) % count;
      return ServiceWebSocketBridge(bridge_index);
   }
   return false;
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
   long max_total_orders = AccountInfoInteger(ACCOUNT_LIMIT_ORDERS);
   if(max_total_orders <= 0) max_total_orders = 4096;
   return "{"
      "\"account_id\":\"" + IntegerToString(AccountInfoInteger(ACCOUNT_LOGIN)) + "\","
      "\"account_type\":\"Live\","
      "\"equity\":" + DoubleToString(AccountInfoDouble(ACCOUNT_EQUITY), 2) + ","
      "\"cash\":" + DoubleToString(AccountInfoDouble(ACCOUNT_BALANCE), 2) + ","
      "\"currency\":\"" + JsonEscape(AccountInfoString(ACCOUNT_CURRENCY)) + "\","
      "\"buying_power\":" + DoubleToString(AccountInfoDouble(ACCOUNT_MARGIN_FREE), 2) + ","
      "\"shorting_enabled\":true,"
      "\"max_total_orders\":" + IntegerToString(max_total_orders) + ","
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

string AssetFeeJson(long swap_mode, double value)
{
   if(swap_mode == SYMBOL_SWAP_MODE_DISABLED || MathAbs(value) <= 0.00000001)
      return "\"none\"";

   if(swap_mode == SYMBOL_SWAP_MODE_POINTS)
      return "{\"points\":" + DoubleToString(value, 8) + "}";
   if(swap_mode == SYMBOL_SWAP_MODE_CURRENCY_SYMBOL
      || swap_mode == SYMBOL_SWAP_MODE_CURRENCY_MARGIN
      || swap_mode == SYMBOL_SWAP_MODE_CURRENCY_DEPOSIT
      || swap_mode == SYMBOL_SWAP_MODE_CURRENCY_PROFIT)
      return "{\"perContractFee\":" + DoubleToString(value, 8) + "}";
   if(swap_mode == SYMBOL_SWAP_MODE_INTEREST_CURRENT
      || swap_mode == SYMBOL_SWAP_MODE_INTEREST_OPEN)
      return "{\"percentagePerContractFee\":" + DoubleToString(value / 100.0 / 360.0, 12) + "}";
   if(swap_mode == SYMBOL_SWAP_MODE_REOPEN_CURRENT
      || swap_mode == SYMBOL_SWAP_MODE_REOPEN_BID)
      return "{\"points\":" + DoubleToString(value, 8) + "}";

   return "\"none\"";
}

string AssetFeesJson(string symbol)
{
   long swap_mode = SymbolInfoInteger(symbol, SYMBOL_SWAP_MODE);
   double swap_long = SymbolInfoDouble(symbol, SYMBOL_SWAP_LONG);
   double swap_short = SymbolInfoDouble(symbol, SYMBOL_SWAP_SHORT);
   return "{"
      "\"commission\":{"
         "\"entry\":{\"long\":\"none\",\"short\":\"none\"},"
         "\"exit\":{\"long\":\"none\",\"short\":\"none\"}"
      "},"
      "\"swap\":{"
         "\"long\":" + AssetFeeJson(swap_mode, swap_long) + ","
         "\"short\":" + AssetFeeJson(swap_mode, swap_short) +
      "},"
      "\"entry\":\"none\","
      "\"exit\":\"none\""
   "}";
}

// Capture the broker's actual weekly sessions, not an asset-name heuristic.
// Non-continuous schedules remain descriptive: no guessed 252-day calendar.
string TradingCalendarJson(string symbol)
{
   string week = "[";
   bool continuous = true;
   bool any_sessions = false;
   bool ambiguous_session = false;
   for(int day = 0; day < 7; day++)
   {
      if(day > 0) week += ",";
      week += "[";
      bool full_day = false;
      uint count = 0;
      for(uint session = 0; session < 24; session++)
      {
         datetime from_time, to_time;
         if(!SymbolInfoSessionTrade(symbol, (ENUM_DAY_OF_WEEK)day, session, from_time, to_time)) break;
         any_sessions = true;
         int from_seconds = (int)((long)from_time % 86400);
         int to_seconds = (int)((long)to_time % 86400);
         // A zero-duration 00:00 -> 00:00 response is ambiguous. Only an
         // explicit full-day span qualifies as continuous evidence.
         bool explicit_full_day = from_seconds == 0 && to_seconds == 0 && (long)to_time - (long)from_time == 86400;
         if(to_seconds == from_seconds && !explicit_full_day) ambiguous_session = true;
         if(to_seconds < from_seconds || explicit_full_day) to_seconds += 86400;
         if(count > 0) week += ",";
         week += "[" + IntegerToString(from_seconds) + "," + IntegerToString(to_seconds) + "]";
         if(from_seconds == 0 && to_seconds == 86400) full_day = true;
         count++;
      }
      week += "]";
      if(!full_day) continuous = false;
   }
   week += "]";
   string evidence = "{\"kind\":\"" + (continuous && !ambiguous_session ? "continuous" : (any_sessions && !ambiguous_session ? "sessions" : "unknown")) + "\",\"source\":\"mt5_weekly_trade_sessions\",\"weekly_seconds\":" + week
      + ",\"observed_utc_offset_seconds\":" + IntegerToString(BrokerUtcOffsetSeconds())
      + ",\"ambiguous_session\":" + (ambiguous_session ? "true" : "false")
      + ",\"holiday_exceptions_recorded\":false}";
   return "\"" + JsonEscape(evidence) + "\"";
}

string AssetJson(string symbol)
{
   SymbolSelect(symbol, true);
   double volume_min = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MIN);
   double volume_step = SymbolInfoDouble(symbol, SYMBOL_VOLUME_STEP);
   double volume_max = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MAX);
   double volume_limit = SymbolInfoDouble(symbol, SYMBOL_VOLUME_LIMIT);
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
      "\"max_total_size\":" + (volume_limit > 0.0 ? DoubleToString(volume_limit, 8) : "null") + ","
      "\"min_price_increment\":" + DoubleToString(point, 10) + ","
      "\"price_base\":" + IntegerToString(digits) + ","
      "\"contract_size\":" + IntegerToString(contract_size) + ","
      "\"trading_calendar\":" + TradingCalendarJson(symbol) + ","
      "\"fees\":" + AssetFeesJson(symbol) +
   "}";
}

string QuoteJsonWithOffset(string symbol, int broker_offset_seconds)
{
   MqlTick tick;
   SymbolInfoTick(symbol, tick);
   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   double last = tick.last > 0.0 ? tick.last : (tick.bid + tick.ask) / 2.0;
   long quote_time_ms = tick.time_msc > 0
      ? tick.time_msc
      : ((long)TimeCurrent() * 1000);
   quote_time_ms -= ((long)broker_offset_seconds * 1000);
   return "{"
      "\"symbol\":\"" + JsonEscape(symbol) + "\","
      "\"bid\":" + DoubleToString(tick.bid, digits) + ","
      "\"ask\":" + DoubleToString(tick.ask, digits) + ","
      "\"bid_size\":0.0,"
      "\"ask_size\":0.0,"
      "\"last\":" + DoubleToString(last, digits) + ","
      "\"last_size\":null,"
      "\"timestamp\":\"" + IsoTimeMillis(quote_time_ms) + "\""
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

string JsonNullableString(string value)
{
   return value == "" ? "null" : "\"" + JsonEscape(value) + "\"";
}

string JsonNullableNumber(double value, bool has_value, int precision = 8)
{
   return has_value ? DoubleToString(value, precision) : "null";
}

string OrderLegJson(
   string order_id,
   double limit_price,
   bool has_limit_price,
   double trail_price,
   bool has_trail_price,
   string side,
   string order_type,
   string status,
   double filled_price,
   bool has_filled_price
)
{
   datetime now_utc = BrokerNowUtc();
   return "{"
      "\"order_id\":" + JsonNullableString(order_id) + ","
      "\"limit_price\":" + JsonNullableNumber(limit_price, has_limit_price) + ","
      "\"trail_price\":" + JsonNullableNumber(trail_price, has_trail_price) + ","
      "\"side\":\"" + side + "\","
      "\"filled_price\":" + JsonNullableNumber(filled_price, has_filled_price) + ","
      "\"order_type\":\"" + order_type + "\","
      "\"status\":\"" + status + "\","
      "\"order_class\":\"Bracket\","
      "\"created_at\":" + IntegerToString((int)now_utc) + ","
      "\"updated_at\":" + IntegerToString((int)now_utc) + ","
      "\"submitted_at\":" + IntegerToString((int)now_utc) + ","
      "\"filled_at\":" + (has_filled_price ? IntegerToString((int)now_utc) : "null") +
   "}";
}

string OrderLegsJson(string hit_leg, string hit_order_id, string close_side, double close_price, double take_profit, double stop_loss)
{
   bool hit_tp = hit_leg == "TakeProfit";
   bool hit_sl = hit_leg == "StopLoss";

   string take_profit_json = "null";
   if(hit_tp || take_profit > 0.0)
   {
      double tp_limit = take_profit > 0.0 ? take_profit : close_price;
      take_profit_json = OrderLegJson(
         hit_tp ? hit_order_id : "",
         tp_limit,
         tp_limit > 0.0,
         0.0,
         false,
         close_side,
         "Limit",
         hit_tp ? "Filled" : "Cancelled",
         close_price,
         hit_tp && close_price > 0.0
      );
   }

   string stop_loss_json = "null";
   if(hit_sl || stop_loss > 0.0)
   {
      double sl_limit = stop_loss > 0.0 ? stop_loss : close_price;
      stop_loss_json = OrderLegJson(
         hit_sl ? hit_order_id : "",
         sl_limit,
         sl_limit > 0.0,
         0.0,
         false,
         close_side,
         "Stop",
         hit_sl ? "Filled" : "Cancelled",
         close_price,
         hit_sl && close_price > 0.0
      );
   }

   if(take_profit_json == "null" && stop_loss_json == "null")
      return "";

   return "{"
      "\"take_profit\":" + take_profit_json + ","
      "\"stop_loss\":" + stop_loss_json + ","
      "\"trailing_stop\":null"
   "}";
}

string HitLegFromDeal(ENUM_DEAL_REASON reason, string comment)
{
   if(reason == DEAL_REASON_TP)
      return "TakeProfit";
   if(reason == DEAL_REASON_SL)
      return "StopLoss";
   if(StringFind(comment, "[tp") >= 0 || StringFind(comment, "TP") >= 0)
      return "TakeProfit";
   if(StringFind(comment, "[sl") >= 0 || StringFind(comment, "SL") >= 0)
      return "StopLoss";
   return "";
}

bool FindEntryOrderForPosition(ulong position_id, string symbol, string &entry_order_id, string &entry_comment, double &entry_take_profit, double &entry_stop_loss)
{
   entry_order_id = "";
   entry_comment = "";
   entry_take_profit = 0.0;
   entry_stop_loss = 0.0;
   if(position_id == 0)
      return false;

   // Manual closes have a new order ticket and may arrive after an EA reload
   // cleared g_order_routes. Recover ownership from this exact position's
   // entry deals, without scanning the account's entire trading history.
   if(!HistorySelectByPosition(position_id))
      return false;

   for(int i = HistoryDealsTotal() - 1; i >= 0; i--)
   {
      ulong deal_ticket = HistoryDealGetTicket(i);
      // HistoryDealGetTicket selects the deal without replacing the list.
      // HistoryDealSelect here would collapse it to the latest (often exit)
      // deal, making the original entry and its AQE comment unreachable.
      if(deal_ticket == 0)
         continue;
      if((ulong)HistoryDealGetInteger(deal_ticket, DEAL_POSITION_ID) != position_id)
         continue;

      ENUM_DEAL_ENTRY deal_entry = (ENUM_DEAL_ENTRY)HistoryDealGetInteger(deal_ticket, DEAL_ENTRY);
      if(deal_entry != DEAL_ENTRY_IN && deal_entry != DEAL_ENTRY_INOUT)
         continue;

      string deal_symbol = HistoryDealGetString(deal_ticket, DEAL_SYMBOL);
      if(symbol != "" && deal_symbol != "" && deal_symbol != symbol)
         continue;

      ulong order_ticket = (ulong)HistoryDealGetInteger(deal_ticket, DEAL_ORDER);
      if(order_ticket == 0)
         continue;

      entry_order_id = IntegerToString((long)order_ticket);
      entry_comment = HistoryDealGetString(deal_ticket, DEAL_COMMENT);
      if(HistoryOrderSelect(order_ticket))
      {
         if(entry_comment == "")
            entry_comment = HistoryOrderGetString(order_ticket, ORDER_COMMENT);
         entry_take_profit = HistoryOrderGetDouble(order_ticket, ORDER_TP);
         entry_stop_loss = HistoryOrderGetDouble(order_ticket, ORDER_SL);
      }
      return true;
   }

   return false;
}

string OrderJson(string order_id, string symbol, double qty, string side, string order_type, string status, double price, string rejection_reason = "", double realized_pnl = 0.0, bool has_realized_pnl = false, string insight_id = "", string strategy_type = "", string legs_json = "", double commission = 0.0, bool has_commission = false, double swap = 0.0, bool has_swap = false, string position_accounting_json = "null")
{
   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   datetime now_utc = BrokerNowUtc();
   double pending_limit = 0.0;
   double pending_stop = 0.0;
   bool pending = status == "Accepted" || status == "Pending" || status == "New" || status == "Replaced";
   if(pending && OrderSelect((ulong)StringToInteger(order_id)))
   {
      ENUM_ORDER_TYPE kind = (ENUM_ORDER_TYPE)OrderGetInteger(ORDER_TYPE);
      if(kind == ORDER_TYPE_BUY_LIMIT || kind == ORDER_TYPE_SELL_LIMIT)
         pending_limit = OrderGetDouble(ORDER_PRICE_OPEN);
      if(kind == ORDER_TYPE_BUY_STOP || kind == ORDER_TYPE_SELL_STOP || kind == ORDER_TYPE_BUY_STOP_LIMIT || kind == ORDER_TYPE_SELL_STOP_LIMIT)
         pending_stop = OrderGetDouble(ORDER_PRICE_OPEN);
      if(kind == ORDER_TYPE_BUY_STOP_LIMIT || kind == ORDER_TYPE_SELL_STOP_LIMIT)
         pending_limit = OrderGetDouble(ORDER_PRICE_STOPLIMIT);
   }
   return "{"
      "\"order_id\":\"" + JsonEscape(order_id) + "\","
      "\"insight_id\":" + JsonNullableString(insight_id) + ","
      "\"strategy_type\":" + JsonNullableString(strategy_type) + ","
      "\"asset\":" + AssetJson(symbol) + ","
      "\"qty\":" + DoubleToString(qty, 8) + ","
      "\"filled_qty\":" + ((status == "Filled" || status == "Closed") ? DoubleToString(qty, 8) : "0.0") + ","
      "\"limit_price\":" + JsonNullableNumber(pending_limit, pending_limit > 0.0) + ","
      "\"filled_price\":" + (!pending && price > 0.0 ? DoubleToString(price, digits) : "null") + ","
      "\"stop_price\":" + JsonNullableNumber(pending_stop, pending_stop > 0.0) + ","
      "\"side\":\"" + side + "\","
      "\"order_type\":\"" + order_type + "\","
      "\"time_in_force\":\"GTC\","
      "\"status\":\"" + status + "\","
      "\"order_class\":\"" + (legs_json == "" ? "Simple" : "Bracket") + "\","
      "\"created_at\":" + IntegerToString((int)now_utc) + ","
      "\"updated_at\":" + IntegerToString((int)now_utc) + ","
      "\"submitted_at\":" + IntegerToString((int)now_utc) + ","
      "\"filled_at\":" + ((status == "Filled" || status == "Closed") ? IntegerToString((int)now_utc) : "null") + ","
      "\"realized_pnl\":" + (has_realized_pnl ? DoubleToString(realized_pnl, 8) : "null") + ","
      "\"position_accounting\":" + position_accounting_json + ","
      "\"commission\":" + (has_commission ? DoubleToString(commission, 8) : "null") + ","
      "\"swap\":" + (has_swap ? DoubleToString(swap, 8) : "null") + ","
      "\"rejection_reason\":" + JsonNullableString(rejection_reason) + ","
      "\"legs\":" + (legs_json == "" ? "null" : legs_json) +
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

ulong FindPositionTicketByEntryOrderId(string order_id)
{
   ulong requested = (ulong)StringToInteger(order_id);
   if(requested == 0)
      return 0;

   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0 || !PositionSelectByTicket(ticket)) continue;

      ulong position_id = (ulong)PositionGetInteger(POSITION_IDENTIFIER);
      string symbol = PositionGetString(POSITION_SYMBOL);
      string entry_order_id = "";
      string entry_comment = "";
      double entry_take_profit = 0.0;
      double entry_stop_loss = 0.0;
      if(!FindEntryOrderForPosition(
         position_id,
         symbol,
         entry_order_id,
         entry_comment,
         entry_take_profit,
         entry_stop_loss
      ))
         continue;
      if((ulong)StringToInteger(entry_order_id) == requested)
         return ticket;
   }
   return 0;
}

bool PositionRouteMatchesInsight(
   int bridge_index,
   ulong ticket,
   string insight_id
)
{
   if(insight_id == "" || ticket == 0 || !PositionSelectByTicket(ticket))
      return false;

   string ticket_key = IntegerToString((long)ticket);
   string identifier_key = IntegerToString((long)PositionGetInteger(POSITION_IDENTIFIER));
   int route_index = FindOrderRouteIndexByAliases(ticket_key, identifier_key, "", "");
   if(route_index < 0)
   {
      string entry_order_id = "";
      string entry_comment = "";
      double entry_take_profit = 0.0;
      double entry_stop_loss = 0.0;
      if(FindEntryOrderForPosition(
         (ulong)PositionGetInteger(POSITION_IDENTIFIER),
         PositionGetString(POSITION_SYMBOL),
         entry_order_id,
         entry_comment,
         entry_take_profit,
         entry_stop_loss
      ))
         route_index = FindOrderRouteIndex(entry_order_id);
   }
   if(route_index < 0)
      return false;

   return g_order_routes[route_index].bridge_index == bridge_index
      && g_order_routes[route_index].session_id == g_bridges[bridge_index].session_id
      && g_order_routes[route_index].insight_id == insight_id;
}

ulong FindUniquePositionTicketByInsightId(
   int bridge_index,
   string insight_id,
   string symbol,
   bool &ambiguous
)
{
   ambiguous = false;
   if(!IsValidBridgeIndex(bridge_index) || insight_id == "")
      return 0;

   ulong matched_ticket = 0;
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0 || !PositionSelectByTicket(ticket)) continue;
      if(symbol != "" && PositionGetString(POSITION_SYMBOL) != symbol) continue;
      if(!PositionRouteMatchesInsight(bridge_index, ticket, insight_id)) continue;

      if(matched_ticket > 0 && matched_ticket != ticket)
      {
         ambiguous = true;
         return 0;
      }
      matched_ticket = ticket;
   }
   return matched_ticket;
}

int FindTrailingStopRoute(string source_order_id)
{
   for(int i = 0; i < ArraySize(g_trailing_stop_routes); i++)
   {
      if(g_trailing_stop_routes[i].source_order_id == source_order_id)
         return i;
   }
   return -1;
}

void RegisterTrailingStop(string source_order_id, double gap)
{
   if(source_order_id == "" || gap <= 0.0)
      return;
   int index = FindTrailingStopRoute(source_order_id);
   if(index < 0)
   {
      index = ArraySize(g_trailing_stop_routes);
      ArrayResize(g_trailing_stop_routes, index + 1);
      g_trailing_stop_routes[index].source_order_id = source_order_id;
   }
   g_trailing_stop_routes[index].gap = gap;
}

void RemoveTrailingStopAt(int index)
{
   if(index < 0 || index >= ArraySize(g_trailing_stop_routes))
      return;
   for(int i = index; i < ArraySize(g_trailing_stop_routes) - 1; i++)
      g_trailing_stop_routes[i] = g_trailing_stop_routes[i + 1];
   ArrayResize(g_trailing_stop_routes, ArraySize(g_trailing_stop_routes) - 1);
}

void ProcessTrailingStops()
{
   for(int i = ArraySize(g_trailing_stop_routes) - 1; i >= 0; i--)
   {
      string source_order_id = g_trailing_stop_routes[i].source_order_id;
      ulong position_ticket = FindPositionTicketById(source_order_id);
      if(position_ticket == 0 || !PositionSelectByTicket(position_ticket))
      {
         ulong order_ticket = (ulong)StringToInteger(source_order_id);
         // Keep protection registered while a stop/limit entry remains pending.
         if(order_ticket > 0 && OrderSelect(order_ticket))
            continue;
         RemoveTrailingStopAt(i);
         continue;
      }

      string symbol = PositionGetString(POSITION_SYMBOL);
      long position_type = PositionGetInteger(POSITION_TYPE);
      double current_sl = PositionGetDouble(POSITION_SL);
      double current_tp = PositionGetDouble(POSITION_TP);
      MqlTick tick;
      SymbolInfoTick(symbol, tick);
      double bid = tick.bid > 0.0 ? tick.bid : SymbolInfoDouble(symbol, SYMBOL_BID);
      double ask = tick.ask > 0.0 ? tick.ask : SymbolInfoDouble(symbol, SYMBOL_ASK);
      double requested_sl = position_type == POSITION_TYPE_SELL
         ? ask + g_trailing_stop_routes[i].gap
         : bid - g_trailing_stop_routes[i].gap;
      double next_sl = position_type == POSITION_TYPE_SELL
         ? ClampSellStopLoss(symbol, requested_sl, ask)
         : ClampBuyStopLoss(symbol, requested_sl, bid);
      if(next_sl <= 0.0)
         continue;

      bool improves = position_type == POSITION_TYPE_SELL
         ? (current_sl <= 0.0 || next_sl < current_sl)
         : (current_sl <= 0.0 || next_sl > current_sl);
      if(!improves)
         continue;

      trade.SetExpertMagicNumber(27042026);
      if(!trade.PositionModify(position_ticket, next_sl, current_tp) || !IsTradeRetcodeSuccess(trade.ResultRetcode()))
         Print("AQE bridge trailing stop update failed ticket=", position_ticket,
               " sl=", DoubleToString(next_sl, (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS)),
               " retcode=", (int)trade.ResultRetcode());
   }
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

// BEGIN read-only position valuation. Kept separate from legacy position
// snapshots: quantity*price there is not account-currency contract exposure.
string PositionQuoteTimeJson(long tick_time_ms, long as_of_us)
{
   if(tick_time_ms <= 0
      || g_history_utc_offset_seconds < -50400 || g_history_utc_offset_seconds > 50400
      || g_history_clock_utc_from <= 0 || g_history_clock_utc_through < g_history_clock_utc_from
      || (long)g_history_clock_last_observed * 1000000 < as_of_us - 3000000)
      return "null";
   long utc_us = (tick_time_ms - (long)g_history_utc_offset_seconds * 1000) * 1000;
   long from_us = (long)g_history_clock_utc_from * 1000000;
   // Current marks may use the latest observed clock second. Historical fills
   // use only fully observed seconds in the separate recovery window.
   long through_us = (long)g_history_clock_last_observed * 1000000 + 999999;
   if(utc_us < from_us || utc_us > through_us || utc_us > as_of_us + 999999) return "null";
   return "{\"occurred_at_us\":" + IntegerToString(utc_us)
      + ",\"clock\":{\"basis\":\"declared_fixed_offset\",\"seconds\":" + IntegerToString(g_history_utc_offset_seconds)
      + ",\"valid_from_us\":" + IntegerToString(from_us) + ",\"valid_through_us\":" + IntegerToString(through_us) + "}}";
}

string PositionNotionalJson(string symbol, string currency, double quantity, double price)
{
   long mode = -1;
   double multiplier = 0.0;
   string price_currency = "";
   if(!SymbolInfoInteger(symbol, SYMBOL_TRADE_CALC_MODE, mode)
      || !SymbolInfoDouble(symbol, SYMBOL_TRADE_CONTRACT_SIZE, multiplier)
      || !SymbolInfoString(symbol, SYMBOL_CURRENCY_PROFIT, price_currency)
      || !MathIsValidNumber(multiplier) || multiplier <= 0.0 || price_currency == "")
      return "{\"basis\":\"unavailable\",\"reason\":\"invalid_contract_metadata\"}";
   // Futures, bonds and collateral have different economic valuation rules.
   // OrderCalcProfit is a P&L calculator, not a generic notional/FX oracle.
   bool linear = mode == SYMBOL_CALC_MODE_FOREX || mode == SYMBOL_CALC_MODE_FOREX_NO_LEVERAGE
      || mode == SYMBOL_CALC_MODE_CFD || mode == SYMBOL_CALC_MODE_CFDINDEX
      || mode == SYMBOL_CALC_MODE_CFDLEVERAGE || mode == SYMBOL_CALC_MODE_EXCH_STOCKS
      || mode == SYMBOL_CALC_MODE_EXCH_STOCKS_MOEX;
   if(!linear) return "{\"basis\":\"unavailable\",\"reason\":\"unsupported_contract\"}";
   if(price_currency != currency)
      return "{\"basis\":\"unavailable\",\"reason\":\"currency_conversion_unavailable\"}";
   double value = MathAbs(quantity * price * multiplier);
   if(!MathIsValidNumber(value)) return "{\"basis\":\"unavailable\",\"reason\":\"invalid_contract_metadata\"}";
   return "{\"basis\":\"linear_contract_same_currency\",\"account_value\":" + DoubleToString(value, 16)
      + ",\"units_per_quantity\":" + DoubleToString(multiplier, 16)
      + ",\"price_currency\":\"" + JsonEscape(price_currency) + "\"}";
}

bool ReadPositionValuationPass(string symbol, string currency, string &ids[], string &values[], string quote_time, double bid, double ask)
{
   int total = PositionsTotal();
   if(total < 0 || total > 4096 || !TerminalInfoInteger(TERMINAL_CONNECTED)) return false;
   ArrayResize(values, ArraySize(ids));
   for(int j = 0; j < ArraySize(ids); j++) values[j] = "";
   for(int i = 0; i < total; i++)
   {
      ulong ticket = PositionGetTicket(i);
      long identifier = 0;
      if(ticket == 0 || !PositionSelectByTicket(ticket)
         || !PositionGetInteger(POSITION_IDENTIFIER, identifier)) return false;
      string id = IntegerToString(identifier);
      for(int j = 0; j < ArraySize(ids); j++)
      {
         if(ids[j] != id) continue;
         string actual_symbol = "";
         long type = -1, updated_ms = 0;
         double quantity = 0.0, entry = 0.0, price = 0.0, pnl = 0.0, carry = 0.0;
         if(values[j] != "" || !PositionGetString(POSITION_SYMBOL, actual_symbol) || actual_symbol != symbol
            || !PositionGetInteger(POSITION_TYPE, type) || !PositionGetInteger(POSITION_TIME_UPDATE_MSC, updated_ms)
            || !PositionGetDouble(POSITION_VOLUME, quantity) || !PositionGetDouble(POSITION_PRICE_OPEN, entry)
            || !PositionGetDouble(POSITION_PRICE_CURRENT, price) || !PositionGetDouble(POSITION_PROFIT, pnl)
            || !PositionGetDouble(POSITION_SWAP, carry) || !MathIsValidNumber(quantity) || quantity <= 0.0
            || !MathIsValidNumber(entry) || !MathIsValidNumber(price) || !MathIsValidNumber(pnl) || !MathIsValidNumber(carry)
            || (type != POSITION_TYPE_BUY && type != POSITION_TYPE_SELL)) return false;
         double quote_price = type == POSITION_TYPE_SELL ? ask : bid;
         string price_time = MathIsValidNumber(quote_price) && quote_price > 0.0
            && MathAbs(price - quote_price) <= MathAbs(price) * 0.000000001 ? quote_time : "null";
         values[j] = "{\"position_id\":\"" + id + "\",\"status\":\"open\",\"quantity\":" + DoubleToString(quantity, 16)
            + ",\"side\":\"" + (type == POSITION_TYPE_SELL ? "sell" : "buy") + "\",\"average_entry_price\":" + DoubleToString(entry, 16)
            + ",\"current_price\":" + DoubleToString(price, 16) + ",\"gross_unrealized_pnl\":" + DoubleToString(pnl, 16)
            + ",\"carry\":" + DoubleToString(carry, 16) + ",\"quote_time\":" + price_time
            + ",\"notional\":" + PositionNotionalJson(symbol, currency, quantity, price)
            // The update stamp participates in the stability check but is not
            // advertised as a mark timestamp (it only tracks position changes).
            + ",\"broker_update_ms\":" + IntegerToString(updated_ms) + "}";
      }
   }
   return PositionsTotal() == total && TerminalInfoInteger(TERMINAL_CONNECTED);
}

string PositionValuationsJson(string account_id, string symbol, string ids_csv)
{
   if(account_id != IntegerToString(AccountInfoInteger(ACCOUNT_LOGIN)) || symbol == ""
      || StringLen(symbol) > 256 || StringLen(ids_csv) > 1024) return "null";
   string ids[];
   int count = StringSplit(ids_csv, ',', ids);
   if(count < 1 || count > 32) return "null";
   for(int i = 0; i < count; i++)
   {
      long id = StringToInteger(ids[i]);
      if(id <= 0 || IntegerToString(id) != ids[i]) return "null";
      for(int j = 0; j < i; j++) if(ids[j] == ids[i]) return "null";
   }
   string currency = AccountInfoString(ACCOUNT_CURRENCY);
   if(currency == "") return "null";
   long as_of_us = (long)TimeGMT() * 1000000;
   MqlTick before_tick, after_tick;
   ZeroMemory(before_tick); ZeroMemory(after_tick);
   bool quote_available = SymbolInfoTick(symbol, before_tick);
   string quote_time = quote_available ? PositionQuoteTimeJson(before_tick.time_msc, as_of_us) : "null";
   string before[], after[];
   if(!ReadPositionValuationPass(symbol, currency, ids, before, quote_time, before_tick.bid, before_tick.ask)
      || !ReadPositionValuationPass(symbol, currency, ids, after, quote_time, before_tick.bid, before_tick.ask)
      || quote_available != SymbolInfoTick(symbol, after_tick)
      || (quote_available && (before_tick.time_msc != after_tick.time_msc
         || before_tick.bid != after_tick.bid || before_tick.ask != after_tick.ask))
      || account_id != IntegerToString(AccountInfoInteger(ACCOUNT_LOGIN))
      || currency != AccountInfoString(ACCOUNT_CURRENCY) || !TerminalInfoInteger(TERMINAL_CONNECTED)) return "null";
   string positions = "";
   for(int i = 0; i < count; i++)
   {
      if(before[i] != after[i]) return "null";
      if(i > 0) positions += ",";
      positions += before[i] != "" ? before[i] : "{\"position_id\":\"" + ids[i] + "\",\"status\":\"not_open\"}";
   }
   return "{\"version\":1,\"provider\":\"mt5-position-v1\",\"account_id\":\"" + JsonEscape(account_id)
      + "\",\"account_currency\":\"" + JsonEscape(currency) + "\",\"symbol\":\"" + JsonEscape(symbol)
      + "\",\"as_of_us\":" + IntegerToString(as_of_us) + ",\"positions\":[" + positions + "]}";
}
// END read-only position valuation.

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

string OrderLifecycleSnapshotJson(
   string order_id,
   string status,
   string position_id = "",
   double position_qty = 0.0,
   bool has_position_qty = false,
   string symbol = "",
   string detail = ""
)
{
   return "{"
      "\"orderId\":\"" + JsonEscape(order_id) + "\","
      "\"status\":\"" + status + "\","
      "\"positionId\":" + JsonNullableString(position_id) + ","
      "\"positionQty\":" + (has_position_qty ? DoubleToString(position_qty, 8) : "null") + ","
      "\"symbol\":" + JsonNullableString(symbol) + ","
      "\"detail\":" + JsonNullableString(detail) + ","
      "\"closeDetails\":" + (status == "Closed" ? PositionCloseDetailsJson((ulong)StringToInteger(position_id)) : "null") +
   "}";
}

string OpenPositionLifecycleJson(string order_id, ulong position_ticket, string detail)
{
   if(position_ticket == 0 || !PositionSelectByTicket(position_ticket))
      return "";
   ulong position_identifier = (ulong)PositionGetInteger(POSITION_IDENTIFIER);
   string position_id = IntegerToString((long)(position_identifier > 0 ? position_identifier : position_ticket));
   return OrderLifecycleSnapshotJson(
      order_id,
      "PositionOpen",
      position_id,
      PositionGetDouble(POSITION_VOLUME),
      true,
      PositionGetString(POSITION_SYMBOL),
      detail
   );
}

bool HasConfirmedPositionClosure(ulong position_identifier)
{
   if(position_identifier == 0 || !HistorySelectByPosition(position_identifier))
      return false;

   double entered_volume = 0.0;
   double exited_volume = 0.0;
   for(int i = 0; i < HistoryDealsTotal(); i++)
   {
      ulong ticket = HistoryDealGetTicket(i);
      if(ticket == 0)
         return false;
      if((ulong)HistoryDealGetInteger(ticket, DEAL_POSITION_ID) != position_identifier)
         return false;
      ENUM_DEAL_TYPE type = (ENUM_DEAL_TYPE)HistoryDealGetInteger(ticket, DEAL_TYPE);
      // Charges and balance operations do not establish position closure.
      if(type != DEAL_TYPE_BUY && type != DEAL_TYPE_SELL)
         continue;
      ENUM_DEAL_ENTRY entry = (ENUM_DEAL_ENTRY)HistoryDealGetInteger(ticket, DEAL_ENTRY);
      double volume = HistoryDealGetDouble(ticket, DEAL_VOLUME);
      if(volume <= 0.0)
         return false;
      if(entry == DEAL_ENTRY_IN)
         entered_volume += volume;
      else if(entry == DEAL_ENTRY_OUT || entry == DEAL_ENTRY_OUT_BY)
         exited_volume += volume;
      else
         // INOUT reversals require allocation of closing vs opening volume;
         // do not invent that evidence from an absent current position.
         return false;
   }
   // Require actual exit deals covering all known entries. A partial exit,
   // entry-only history or exit-only history is not conclusive evidence.
   return entered_volume > 0.0 && exited_volume > 0.0
      && MathAbs(entered_volume - exited_volume) <= 0.00000001
      && FindPositionTicketById(IntegerToString((long)position_identifier)) == 0;
}

// One broker deal, not an accumulated order or complete position. Keep this
// separate from OrderJson so the trading reducer's quantity semantics do not
// determine accounting. Ticket identity is stable across bridge sessions.
string ExecutionFillJson(ulong deal_ticket, bool select_deal = true, int history_offset = 0, long history_as_of_us = 0, bool historical_revision = false)
{
   if(deal_ticket == 0 || (select_deal && !HistoryDealSelect(deal_ticket))) return "null";
   if(historical_revision && (g_history_utc_offset_seconds < -50400 || g_history_utc_offset_seconds > 50400
      || g_history_clock_utc_from <= 0 || g_history_clock_utc_through < g_history_clock_utc_from)) return "null";
   ENUM_DEAL_TYPE deal_type = (ENUM_DEAL_TYPE)HistoryDealGetInteger(deal_ticket, DEAL_TYPE);
   if(deal_type != DEAL_TYPE_BUY && deal_type != DEAL_TYPE_SELL) return "null";
   ulong order_ticket = (ulong)HistoryDealGetInteger(deal_ticket, DEAL_ORDER);
   ulong position_id = (ulong)HistoryDealGetInteger(deal_ticket, DEAL_POSITION_ID);
   long time_ms = HistoryDealGetInteger(deal_ticket, DEAL_TIME_MSC);
   int offset = select_deal ? BrokerUtcOffsetSeconds() : history_offset;
   if(historical_revision) offset = g_history_utc_offset_seconds;
   long occurred_at_us = (time_ms - ((long)offset * 1000)) * 1000;
   if(historical_revision && (occurred_at_us < (long)g_history_clock_utc_from * 1000000
      || occurred_at_us > (long)g_history_clock_utc_through * 1000000 + 999999)) return "null";
   double quantity = HistoryDealGetDouble(deal_ticket, DEAL_VOLUME);
   double price = HistoryDealGetDouble(deal_ticket, DEAL_PRICE);
   if(order_ticket == 0 || occurred_at_us <= 0 || quantity <= 0.0
      || !MathIsValidNumber(quantity) || !MathIsValidNumber(price)) return "null";
   ENUM_DEAL_ENTRY entry = (ENUM_DEAL_ENTRY)HistoryDealGetInteger(deal_ticket, DEAL_ENTRY);
   string effect = "unknown";
   if(entry == DEAL_ENTRY_IN) effect = "entry";
   if(entry == DEAL_ENTRY_OUT || entry == DEAL_ENTRY_OUT_BY) effect = "exit";
   if(entry == DEAL_ENTRY_INOUT) effect = "reversal";
   // TimeGMT is second-granularity. Distinct history observations in the same
   // second need ordered watermarks; this logical tie-break is NOT a latency
   // measurement. It survives bridge reconnects in this EA process. Across a
   // terminal restart a conflicting old watermark is rejected by the journal,
   // never accepted as proof of complete broker-history reconciliation.
   long as_of_us = history_as_of_us;
   if(select_deal)
   {
      as_of_us = (long)TimeGMT() * 1000000;
      if(as_of_us < occurred_at_us) as_of_us = occurred_at_us;
      if(as_of_us <= g_last_fill_as_of_us) as_of_us = g_last_fill_as_of_us + 1;
      g_last_fill_as_of_us = as_of_us;
   }
   if(as_of_us < occurred_at_us) return "null";
   double gross = HistoryDealGetDouble(deal_ticket, DEAL_PROFIT);
   double commission = -(HistoryDealGetDouble(deal_ticket, DEAL_COMMISSION)
      + HistoryDealGetDouble(deal_ticket, DEAL_FEE));
   double carry = HistoryDealGetDouble(deal_ticket, DEAL_SWAP);
   string currency = AccountInfoString(ACCOUNT_CURRENCY);
   string cash = "null";
   if(currency != "" && MathIsValidNumber(gross) && MathIsValidNumber(commission) && MathIsValidNumber(carry))
      cash = "{\"currency\":\"" + JsonEscape(currency) + "\",\"gross_pnl\":" + DoubleToString(gross,16)
         + ",\"commission\":" + DoubleToString(commission,16) + ",\"carry\":" + DoubleToString(carry,16) + "}";
   return "{\"version\":1,\"provider\":\"mt5-deal-v1\",\"fill_id\":\"" + IntegerToString((long)deal_ticket)
      + "\",\"order_id\":\"" + IntegerToString((long)order_ticket) + "\",\"position_id\":"
      + (position_id > 0 ? "\"" + IntegerToString((long)position_id) + "\"" : "null")
      + ",\"symbol\":\"" + JsonEscape(HistoryDealGetString(deal_ticket,DEAL_SYMBOL))
      + "\",\"side\":\"" + (deal_type == DEAL_TYPE_SELL ? "sell" : "buy") + "\",\"effect\":\"" + effect
      + "\",\"quantity\":" + DoubleToString(quantity,16) + ",\"price\":" + DoubleToString(price,16)
      + ",\"occurred_at_us\":" + IntegerToString(occurred_at_us) + ",\"as_of_us\":" + IntegerToString(as_of_us)
      + ",\"cash\":" + cash + ",\"account_notional\":null}";
}

// Read-only, exact owned-entry-order lookup. No account-wide history scan,
// mutation, symbol-based ownership inference or incomplete-page acceptance.
bool ReadPositionQuantity(ulong identifier, string symbol, double &quantity, string &side, long &updated_ms)
{
   quantity = 0.0; side = ""; updated_ms = 0;
   if(!TerminalInfoInteger(TERMINAL_CONNECTED)) return false;
   int total = PositionsTotal();
   if(total < 0 || total > 4096) return false;
   bool found = false;
   for(int i = 0; i < total; i++)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0 || !PositionSelectByTicket(ticket)) return false;
      if((ulong)PositionGetInteger(POSITION_IDENTIFIER) != identifier) continue;
      if(found || PositionGetString(POSITION_SYMBOL) != symbol) return false;
      found = true;
      quantity = PositionGetDouble(POSITION_VOLUME);
      long type = PositionGetInteger(POSITION_TYPE);
      if(!MathIsValidNumber(quantity) || quantity <= 0.0
         || (type != POSITION_TYPE_BUY && type != POSITION_TYPE_SELL)) return false;
      side = type == POSITION_TYPE_SELL ? "sell" : "buy";
      updated_ms = PositionGetInteger(POSITION_TIME_UPDATE_MSC);
   }
   return PositionsTotal() == total && TerminalInfoInteger(TERMINAL_CONNECTED);
}

string ExecutionFillHistoryJson(string account_id, string entry_order_id, string symbol, bool supports_invalidations = false, bool supports_position_snapshot = false)
{
   if(account_id != IntegerToString(AccountInfoInteger(ACCOUNT_LOGIN)) || symbol == ""
      || g_history_utc_offset_seconds < -50400 || g_history_utc_offset_seconds > 50400
      || g_history_clock_utc_from <= 0 || g_history_clock_utc_through < g_history_clock_utc_from)
      return "null";
   ulong order = (ulong)StringToInteger(entry_order_id);
   if(order == 0 || IntegerToString((long)order) != entry_order_id || !HistoryOrderSelect(order)
      || HistoryOrderGetString(order, ORDER_SYMBOL) != symbol) return "null";
   ulong position = (ulong)HistoryOrderGetInteger(order, ORDER_POSITION_ID);
   if(position == 0 || !HistorySelectByPosition(position)) return "null";
   double quantity_before = 0.0;
   string side_before = "";
   long updated_before = 0;
   if(supports_position_snapshot && !ReadPositionQuantity(position, symbol, quantity_before, side_before, updated_before)) return "null";
   int count = HistoryDealsTotal();
   if(count <= 0 || count > 256) return "null";
   long from_us = (long)g_history_clock_utc_from * 1000000;
   long through_us = (long)g_history_clock_utc_through * 1000000 + 999999;
   long as_of_us = (long)TimeGMT() * 1000000;
   if(as_of_us <= g_last_fill_as_of_us) as_of_us = g_last_fill_as_of_us + 1;
   bool has_entry = false;
   string fills = "";
   string invalidations = "";
   for(int i = 0; i < count; i++)
   {
      ulong ticket = HistoryDealGetTicket(i);
      if(ticket == 0 || (ulong)HistoryDealGetInteger(ticket, DEAL_POSITION_ID) != position) return "null";
      ENUM_DEAL_TYPE type = (ENUM_DEAL_TYPE)HistoryDealGetInteger(ticket, DEAL_TYPE);
      bool cancelled = type == DEAL_TYPE_BUY_CANCELED || type == DEAL_TYPE_SELL_CANCELED;
      if(cancelled && !supports_invalidations) return "null";
      if(!cancelled && type != DEAL_TYPE_BUY && type != DEAL_TYPE_SELL) continue;
      if(HistoryDealGetString(ticket, DEAL_SYMBOL) != symbol) return "null";
      ENUM_DEAL_ENTRY entry = (ENUM_DEAL_ENTRY)HistoryDealGetInteger(ticket, DEAL_ENTRY);
      if(entry == DEAL_ENTRY_IN)
      {
         if((ulong)HistoryDealGetInteger(ticket, DEAL_ORDER) != order) return "null";
         has_entry = true;
      }
      else if(entry != DEAL_ENTRY_OUT && entry != DEAL_ENTRY_OUT_BY) return "null";
      long occurred_us = (HistoryDealGetInteger(ticket, DEAL_TIME_MSC)
         - (long)g_history_utc_offset_seconds * 1000) * 1000;
      if(occurred_us < from_us || occurred_us > through_us || occurred_us > as_of_us) return "null";
      if(cancelled)
      {
         ulong cancelled_order = (ulong)HistoryDealGetInteger(ticket, DEAL_ORDER);
         if(cancelled_order == 0) return "null";
         if(invalidations != "") invalidations += ",";
         invalidations += "{\"provider\":\"mt5-deal-v1\",\"fill_id\":\"" + IntegerToString((long)ticket)
            + "\",\"order_id\":\"" + IntegerToString((long)cancelled_order)
            + "\",\"position_id\":\"" + IntegerToString((long)position) + "\",\"symbol\":\"" + JsonEscape(symbol)
            + "\",\"side\":\"" + (type == DEAL_TYPE_SELL_CANCELED ? "sell" : "buy")
            + "\",\"effect\":\"" + (entry == DEAL_ENTRY_IN ? "entry" : "exit")
            + "\",\"occurred_at_us\":" + IntegerToString(occurred_us) + ",\"as_of_us\":" + IntegerToString(as_of_us)
            + ",\"reason\":\"broker_cancelled\"}";
      }
      else
      {
      // Do not HistoryDealSelect here: it resets the list being traversed.
      string fill = ExecutionFillJson(ticket, false, g_history_utc_offset_seconds, as_of_us);
      if(fill == "null") return "null";
      if(fills != "") fills += ",";
      fills += fill;
      }
      if(StringLen(fills) + StringLen(invalidations) > 262144) return "null";
   }
   if(!has_entry || account_id != IntegerToString(AccountInfoInteger(ACCOUNT_LOGIN))) return "null";
   string position_snapshot = "null";
   if(supports_position_snapshot)
   {
      double quantity_after = 0.0;
      string side_after = "";
      long updated_after = 0;
      if(!ReadPositionQuantity(position, symbol, quantity_after, side_after, updated_after)
         || quantity_before != quantity_after || side_before != side_after || updated_before != updated_after
         || account_id != IntegerToString(AccountInfoInteger(ACCOUNT_LOGIN)))
         return "null";
      position_snapshot = "{\"position_id\":\"" + IntegerToString((long)position) + "\",\"symbol\":\"" + JsonEscape(symbol)
         + "\",\"quantity\":" + DoubleToString(quantity_after,16) + ",\"side\":"
         + (side_after == "" ? "null" : "\"" + side_after + "\"")
         + ",\"as_of_us\":" + IntegerToString(as_of_us) + "}";
   }
   g_last_fill_as_of_us = as_of_us;
   return "{\"account_id\":\"" + JsonEscape(account_id) + "\",\"entry_order_id\":\"" + entry_order_id
      + "\",\"position_id\":\"" + IntegerToString((long)position) + "\",\"symbol\":\"" + JsonEscape(symbol)
      + "\",\"as_of_us\":" + IntegerToString(as_of_us)
      + ",\"clock\":{\"basis\":\"declared_fixed_offset\",\"seconds\":" + IntegerToString(g_history_utc_offset_seconds)
      + ",\"valid_from_us\":" + IntegerToString(from_us) + ",\"valid_through_us\":" + IntegerToString(through_us)
      + "},\"fills\":[" + fills + "],\"invalidations\":[" + invalidations + "],\"position_snapshot\":" + position_snapshot + "}";
}

// Analytics-only lifetime economics from trade deals, not another exit fill.
// Balance/account-level charges remain unallocated. Multiple distinct entry
// orders (netting/mixed ownership) cannot be assigned to a single Insight.
string PositionAccountingJson(ulong position_identifier)
{
   if(!HasConfirmedPositionClosure(position_identifier)) return "null";
   ulong entry_order = 0;
   long first_entry_ms = 0, last_exit_ms = 0;
   double profit = 0.0, costs = 0.0, swap = 0.0;
   for(int i = 0; i < HistoryDealsTotal(); i++)
   {
      ulong ticket = HistoryDealGetTicket(i);
      if(ticket == 0 || (ulong)HistoryDealGetInteger(ticket, DEAL_POSITION_ID) != position_identifier)
         return "null";
      ENUM_DEAL_TYPE type = (ENUM_DEAL_TYPE)HistoryDealGetInteger(ticket, DEAL_TYPE);
      if(type != DEAL_TYPE_BUY && type != DEAL_TYPE_SELL) continue;
      long at_ms = HistoryDealGetInteger(ticket, DEAL_TIME_MSC);
      if(at_ms <= 0) return "null";
      ENUM_DEAL_ENTRY entry = (ENUM_DEAL_ENTRY)HistoryDealGetInteger(ticket, DEAL_ENTRY);
      if(entry == DEAL_ENTRY_IN)
      {
         ulong order = (ulong)HistoryDealGetInteger(ticket, DEAL_ORDER);
         if(order == 0 || (entry_order != 0 && entry_order != order)) return "null";
         entry_order = order;
         if(first_entry_ms == 0 || at_ms < first_entry_ms) first_entry_ms = at_ms;
      }
      else if(entry == DEAL_ENTRY_OUT || entry == DEAL_ENTRY_OUT_BY)
      {
         if(at_ms > last_exit_ms) last_exit_ms = at_ms;
      }
      else return "null";
      profit += HistoryDealGetDouble(ticket, DEAL_PROFIT);
      costs += HistoryDealGetDouble(ticket, DEAL_COMMISSION) + HistoryDealGetDouble(ticket, DEAL_FEE);
      swap += HistoryDealGetDouble(ticket, DEAL_SWAP);
   }
   string currency = AccountInfoString(ACCOUNT_CURRENCY);
   if(entry_order == 0 || first_entry_ms <= 0 || last_exit_ms < first_entry_ms || currency == ""
      || !MathIsValidNumber(profit) || !MathIsValidNumber(costs) || !MathIsValidNumber(swap)
      || !MathIsValidNumber(profit + costs + swap)) return "null";
   long offset_ms = (long)BrokerUtcOffsetSeconds() * 1000;
   long opened_us = (first_entry_ms - offset_ms) * 1000;
   long closed_us = (last_exit_ms - offset_ms) * 1000;
   long as_of_us = (long)TimeGMT() * 1000000;
   // TimeGMT has second precision; a confirmed deal may lie in this second.
   if(as_of_us < closed_us) as_of_us = closed_us;
   return "{\"version\":1,\"provider\":\"mt5_position_trade_deals_v1\""
      + ",\"position_id\":\"" + IntegerToString((long)position_identifier) + "\""
      + ",\"entry_order_id\":\"" + IntegerToString((long)entry_order) + "\""
      + ",\"currency\":\"" + JsonEscape(currency) + "\""
      + ",\"opened_at_us\":" + IntegerToString(opened_us)
      + ",\"closed_at_us\":" + IntegerToString(closed_us)
      + ",\"as_of_us\":" + IntegerToString(as_of_us)
      + ",\"net_pnl\":" + DoubleToString(profit + costs + swap, 8)
      + ",\"commission\":" + DoubleToString(-costs, 8)
      + ",\"swap\":" + DoubleToString(swap, 8) + "}";
}

string PositionCloseDetailsJson(ulong position_identifier)
{
   if(!HasConfirmedPositionClosure(position_identifier))
      return "null";
   double exit_value = 0.0, exit_volume = 0.0, profit = 0.0, costs = 0.0, swap = 0.0;
   long last_exit_ms = 0;
   for(int i = 0; i < HistoryDealsTotal(); i++)
   {
      ulong ticket = HistoryDealGetTicket(i);
      ENUM_DEAL_TYPE type = (ENUM_DEAL_TYPE)HistoryDealGetInteger(ticket, DEAL_TYPE);
      if(type != DEAL_TYPE_BUY && type != DEAL_TYPE_SELL) continue;
      profit += HistoryDealGetDouble(ticket, DEAL_PROFIT);
      costs += HistoryDealGetDouble(ticket, DEAL_COMMISSION) + HistoryDealGetDouble(ticket, DEAL_FEE);
      swap += HistoryDealGetDouble(ticket, DEAL_SWAP);
      ENUM_DEAL_ENTRY entry = (ENUM_DEAL_ENTRY)HistoryDealGetInteger(ticket, DEAL_ENTRY);
      if(entry == DEAL_ENTRY_OUT || entry == DEAL_ENTRY_OUT_BY)
      {
         double volume = HistoryDealGetDouble(ticket, DEAL_VOLUME);
         double price = HistoryDealGetDouble(ticket, DEAL_PRICE);
         long at_ms = HistoryDealGetInteger(ticket, DEAL_TIME_MSC);
         if(price <= 0.0 || at_ms <= 0) return "null";
         exit_volume += volume;
         exit_value += price * volume;
         if(at_ms > last_exit_ms) last_exit_ms = at_ms;
      }
   }
   if(exit_volume <= 0.0) return "null";
   return "{\"closePrice\":" + DoubleToString(exit_value / exit_volume, 8)
      + ",\"closedAt\":\"" + IsoTimeMillis(last_exit_ms - ((long)BrokerUtcOffsetSeconds() * 1000)) + "\""
      + ",\"realizedPnl\":" + DoubleToString(profit + costs + swap, 8)
      + ",\"commission\":" + DoubleToString(-costs, 8)
      + ",\"swap\":" + DoubleToString(swap, 8)
      + ",\"positionAccounting\":" + PositionAccountingJson(position_identifier) + "}";
}

string ReconcileOrderLifecycleJson(string order_id)
{
   StringTrimLeft(order_id);
   StringTrimRight(order_id);
   ulong requested = (ulong)StringToInteger(order_id);
   if(requested == 0)
      return OrderLifecycleSnapshotJson(order_id, "Unknown", "", 0.0, false, "", "invalid_order_id");

   ulong position_ticket = FindPositionTicketById(order_id);
   if(position_ticket == 0)
      position_ticket = FindPositionTicketByEntryOrderId(order_id);
   string open_position = OpenPositionLifecycleJson(order_id, position_ticket, "open_position");
   if(open_position != "")
      return open_position;

   if(OrderSelect(requested))
      return OrderLifecycleSnapshotJson(
         order_id,
         "Pending",
         "",
         0.0,
         false,
         OrderGetString(ORDER_SYMBOL),
         "pending_order"
      );

   if(HistoryOrderSelect(requested))
   {
      ulong position_identifier = (ulong)HistoryOrderGetInteger(requested, ORDER_POSITION_ID);
      if(position_identifier > 0)
      {
         position_ticket = FindPositionTicketById(IntegerToString((long)position_identifier));
         open_position = OpenPositionLifecycleJson(order_id, position_ticket, "historical_order_open_position");
         if(open_position != "")
            return open_position;
      }

      ENUM_ORDER_STATE order_state = (ENUM_ORDER_STATE)HistoryOrderGetInteger(requested, ORDER_STATE);
      string order_symbol = HistoryOrderGetString(requested, ORDER_SYMBOL);
      string status = "Unknown";
      string detail = "historical_order_unknown";
      if(order_state == ORDER_STATE_FILLED)
      {
         bool confirmed_closed = HasConfirmedPositionClosure(position_identifier);
         status = confirmed_closed ? "Closed" : "Filled";
         detail = confirmed_closed ? "position_entry_and_exit_volume_confirmed"
            : "historical_fill_without_conclusive_exit_evidence";
      }
      else if(order_state == ORDER_STATE_CANCELED)
      {
         status = "Cancelled";
         detail = "historical_cancelled_order";
      }
      else if(order_state == ORDER_STATE_REJECTED)
      {
         status = "Rejected";
         detail = "historical_rejected_order";
      }
      else if(order_state == ORDER_STATE_EXPIRED)
      {
         status = "Expired";
         detail = "historical_expired_order";
      }
      else if(order_state == ORDER_STATE_PARTIAL)
      {
         status = "Filled";
         detail = "historical_partial_fill_without_open_position_confirmation";
      }
      return OrderLifecycleSnapshotJson(
         order_id,
         status,
         position_identifier > 0 ? IntegerToString((long)position_identifier) : "",
         0.0,
         false,
         order_symbol,
         detail
      );
   }

   if(HistoryDealSelect(requested))
   {
      ulong position_identifier = (ulong)HistoryDealGetInteger(requested, DEAL_POSITION_ID);
      string deal_symbol = HistoryDealGetString(requested, DEAL_SYMBOL);
      if(position_identifier > 0)
      {
         position_ticket = FindPositionTicketById(IntegerToString((long)position_identifier));
         open_position = OpenPositionLifecycleJson(order_id, position_ticket, "historical_deal_open_position");
         if(open_position != "")
            return open_position;
      }
      bool confirmed_closed = HasConfirmedPositionClosure(position_identifier);
      return OrderLifecycleSnapshotJson(
         order_id,
         confirmed_closed ? "Closed" : "Unknown",
         position_identifier > 0 ? IntegerToString((long)position_identifier) : "",
         0.0,
         false,
         deal_symbol,
         confirmed_closed ? "position_entry_and_exit_volume_confirmed"
            : "historical_deal_without_conclusive_exit_evidence"
      );
   }

   if(HasConfirmedPositionClosure(requested))
      return OrderLifecycleSnapshotJson(
         order_id,
         "Closed",
         order_id,
         0.0,
         false,
         "",
         "position_entry_and_exit_volume_confirmed"
      );

   return OrderLifecycleSnapshotJson(order_id, "Unknown", "", 0.0, false, "", "not_found");
}

string ReconcileOrdersJson(string order_ids_csv)
{
   string order_ids[];
   int count = StringSplit(order_ids_csv, ',', order_ids);
   string snapshots = "";
   int emitted = 0;
   for(int i = 0; i < count; i++)
   {
      string order_id = order_ids[i];
      StringTrimLeft(order_id);
      StringTrimRight(order_id);
      if(order_id == "") continue;
      if(emitted > 0) snapshots += ",";
      snapshots += ReconcileOrderLifecycleJson(order_id);
      emitted++;
   }
   return "[" + snapshots + "]";
}

void QueueRpcResponse(int bridge_index, string request_id, string action, string payload)
{
   int index = ArraySize(g_pending_rpc_responses);
   ArrayResize(g_pending_rpc_responses, index + 1);
   g_pending_rpc_responses[index].bridge_index = bridge_index;
   g_pending_rpc_responses[index].request_id = request_id;
   g_pending_rpc_responses[index].action = action;
   g_pending_rpc_responses[index].payload = payload;
}

void SendRpcResponse(int bridge_index, string request_id, bool ok, string message, string payload, string action = "")
{
   string body = "{"
      "\"requestId\":\"" + JsonEscape(request_id) + "\","
      "\"ok\":" + (ok ? "true" : "false") + ","
      "\"message\":" + (message == "" ? "null" : "\"" + JsonEscape(message) + "\"") + ","
      "\"payload\":" + (payload == "" ? "null" : payload) +
   "}";
   uchar encoded[];
   if(StringToCharArray(body, encoded, 0, WHOLE_ARRAY, CP_UTF8) > WS_MAX_MESSAGE_BYTES - 4096)
   {
      // Reject once as an RPC error, not a permanently unsendable FIFO head.
      body = "{\"requestId\":\"" + JsonEscape(request_id)
         + "\",\"ok\":false,\"message\":\"MT5 response exceeds 16 MiB; request a smaller history window\",\"payload\":null}";
   }
   QueueRpcResponse(bridge_index, request_id, action, body);
}

bool SendOneQueuedRpcResponse()
{
   if(ArraySize(g_pending_rpc_responses) <= 0)
      return false;
   PendingRpcResponse pending = g_pending_rpc_responses[0];
   string response;
   bool sent = PostJson(pending.bridge_index, "/v1/rpc/response", pending.payload, response);
   if(!sent)
      return true;
   int total = ArraySize(g_pending_rpc_responses);
   for(int i = 1; i < total; i++)
      g_pending_rpc_responses[i - 1] = g_pending_rpc_responses[i];
   ArrayResize(g_pending_rpc_responses, total - 1);
   return true;
}

void SendHeartbeat(int bridge_index)
{
   if(!IsValidBridgeIndex(bridge_index) || g_bridges[bridge_index].session_id == "") return;
   string response;
   string payload = "{"
      "\"terminalName\":\"" + JsonEscape(TerminalInfoString(TERMINAL_NAME)) + "\","
      "\"accountId\":\"" + IntegerToString(AccountInfoInteger(ACCOUNT_LOGIN)) + "\","
      "\"terminalConnected\":" + (TerminalInfoInteger(TERMINAL_CONNECTED) ? "true" : "false") + ","
      "\"terminalTradeAllowed\":" + ((TerminalInfoInteger(TERMINAL_TRADE_ALLOWED) && MQLInfoInteger(MQL_TRADE_ALLOWED)) ? "true" : "false") + ","
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

bool SendMarketData(int bridge_index)
{
   if(!IsValidBridgeIndex(bridge_index) || g_bridges[bridge_index].session_id == "") return false;
   string bars = "";
   string quotes = "";
   string intrabar_bars = "";
   string emitted_quotes[];
   int completed_subscription_indexes[];
   datetime completed_bar_times[];
   int broker_offset_seconds = BrokerUtcOffsetSeconds();
   for(int i = 0; i < ArraySize(g_subscriptions); i++)
   {
      if(g_subscriptions[i].bridge_index != bridge_index) continue;
      string symbol = g_subscriptions[i].symbol;
      string timeframe_code = g_subscriptions[i].timeframe_code;
      ENUM_TIMEFRAMES timeframe = TimeframeFromCode(timeframe_code);
      SymbolSelect(symbol, true);
      // Read one coherent shift-zero snapshot. Quote sampling cannot reconstruct
      // tick volume or the candle high/low when multiple ticks arrive per batch.
      MqlRates current[1];
      if(CopyRates(symbol, timeframe, 0, 1, current) == 1 && current[0].time > 0)
      {
         string current_json = RateBarJsonWithOffset(symbol, current[0], broker_offset_seconds);
         if(StringLen(intrabar_bars) > 0) intrabar_bars += ",";
         intrabar_bars += StringSubstr(current_json, 0, StringLen(current_json) - 1)
            + ",\"timeframe\":\"" + JsonEscape(timeframe_code) + "\"}";
      }
      datetime completed = iTime(symbol, timeframe, 1);
      if(completed > 0
         && completed != g_subscriptions[i].last_bar_time
         && g_subscriptions[i].pending_market_sequence == 0)
      {
         string bar_json = BarJsonWithOffset(symbol, timeframe, 1, broker_offset_seconds);
         if(StringLen(bars) > 0) bars += ",";
         bars += StringSubstr(bar_json, 0, StringLen(bar_json) - 1)
            + ",\"timeframe\":\"" + JsonEscape(timeframe_code) + "\"}";
         int completed_index = ArraySize(completed_subscription_indexes);
         ArrayResize(completed_subscription_indexes, completed_index + 1);
         ArrayResize(completed_bar_times, completed_index + 1);
         completed_subscription_indexes[completed_index] = i;
         completed_bar_times[completed_index] = completed;
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
   if(StringLen(bars) == 0 && StringLen(quotes) == 0) return false;

   string response;
   string payload = "{"
      "\"quotes\":[" + quotes + "],"
      "\"bars\":[" + bars + "],"
      "\"intrabarBars\":[" + intrabar_bars + "],"
      "\"history\":[]"
   "}";
   if(IsWebSocketActive(bridge_index))
   {
      g_bridges[bridge_index].event_seq++;
      ulong market_sequence = g_bridges[bridge_index].event_seq;
      string frame = "{"
         "\"type\":\"MarketDataBatch\","
         "\"sequence\":" + IntegerToString((long)market_sequence) + ","
         "\"payload\":" + payload +
      "}";
      bool sent = SendWebSocketJson(bridge_index, frame);
      if(sent)
      {
         for(int i = 0; i < ArraySize(completed_subscription_indexes); i++)
         {
            int subscription_index = completed_subscription_indexes[i];
            if(subscription_index >= 0 && subscription_index < ArraySize(g_subscriptions))
            {
               g_subscriptions[subscription_index].pending_bar_time = completed_bar_times[i];
               g_subscriptions[subscription_index].pending_market_sequence = market_sequence;
            }
         }
      }
      return true;
   }
   bool posted = PostJson(bridge_index, "/v1/market-data", payload, response);
   if(posted)
   {
      // A completed candle is acknowledged only after AQE accepted it. If the
      // request fails, the same candle is retained and retried on reconnect.
      for(int i = 0; i < ArraySize(completed_subscription_indexes); i++)
      {
         int subscription_index = completed_subscription_indexes[i];
         if(subscription_index >= 0 && subscription_index < ArraySize(g_subscriptions))
            g_subscriptions[subscription_index].last_bar_time = completed_bar_times[i];
      }
   }
   return true;
}

bool SendNextMarketDataUpdate()
{
   int bridge_count = ArraySize(g_bridges);
   if(bridge_count <= 0)
      return false;
   if(g_next_market_bridge_index < 0 || g_next_market_bridge_index >= bridge_count)
      g_next_market_bridge_index = 0;

   for(int offset = 0; offset < bridge_count; offset++)
   {
      int bridge_index = (g_next_market_bridge_index + offset) % bridge_count;
      if(!IsBridgeSessionActive(bridge_index) || !HasBridgeSubscriptions(bridge_index)
         || (!IsWebSocketActive(bridge_index) && !HttpTransportAllowed(bridge_index)))
         continue;
      if(!HasElapsedMs(g_bridges[bridge_index].last_market_data_ms, MarketDataIntervalMs()))
         continue;

      g_next_market_bridge_index = (bridge_index + 1) % bridge_count;
      g_bridges[bridge_index].last_market_data_ms = GetTickCount();
      return SendMarketData(bridge_index);
   }
   return false;
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
   g_bridges[bridge_index].supports_fill_revisions = false;
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
      g_subscriptions[index].pending_bar_time = 0;
      g_subscriptions[index].pending_market_sequence = 0;
      SymbolSelect(symbol, true);
   }
}

bool SubmissionHexDigest(string value)
{
   if(StringLen(value) != 64) return false;
   for(int i = 0; i < 64; i++)
   {
      ushort c = StringGetCharacter(value, i);
      if(!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return false;
   }
   return true;
}

string SubmissionReceiptPath(string json)
{
   string key = ExtractString(json, "submissionKey");
   string fingerprint = ExtractString(json, "submissionFingerprint");
   string account = ExtractString(json, "submissionAccount");
   if(!SubmissionHexDigest(key) || !SubmissionHexDigest(fingerprint)
      || account != IntegerToString(AccountInfoInteger(ACCOUNT_LOGIN))) return "";
   return "AQE\\SubmissionReceipts\\" + account + "\\" + key + ".txt";
}

string SubmissionReceiptIdentity(string json)
{
   // The same login can exist at different broker servers. Keep the full server
   // identity in the receipt; a path collision fails closed, never reuses proof.
   return ExtractString(json, "submissionFingerprint") + "|" + JsonEscape(AccountInfoString(ACCOUNT_SERVER));
}

// Exclusive handle covers marker flush, broker mutation and result persistence.
// A marker without a result survives crashes and MUST NOT be resubmitted.
int BeginSubmissionReceipt(string json, string &cached, string &error)
{
   string path = SubmissionReceiptPath(json);
   if(path == "") { error = "invalid submission correlation/account"; return INVALID_HANDLE; }
   int handle = FileOpen(path, FILE_READ | FILE_WRITE | FILE_TXT | FILE_ANSI | FILE_COMMON, 0, CP_UTF8);
   if(handle == INVALID_HANDLE) { error = "submission receipt unavailable or busy"; return INVALID_HANDLE; }
   string fingerprint = SubmissionReceiptIdentity(json);
   if(FileSize(handle) > 0)
   {
      if(FileSize(handle) > 65536 || FileReadString(handle) != fingerprint)
         error = "submission receipt identity conflict or corrupt record";
      else if(!FileIsEnding(handle)) cached = FileReadString(handle);
      if(error == "" && cached == "") error = "submission outcome unknown; replay refused";
      if(error != "") { FileClose(handle); return INVALID_HANDLE; }
      return handle;
   }
   ResetLastError();
   uint written = FileWriteString(handle, fingerprint + "\r\n");
   FileFlush(handle);
   if(written < (uint)StringLen(fingerprint + "\r\n") || GetLastError() != 0)
   {
      error = "submission intent receipt could not be flushed";
      FileClose(handle);
      return INVALID_HANDLE;
   }
   return handle;
}

string LookupSubmissionReceipt(string json)
{
   string path = SubmissionReceiptPath(json);
   if(path == "") return "";
   int handle = FileOpen(path, FILE_READ | FILE_TXT | FILE_ANSI | FILE_COMMON, 0, CP_UTF8);
   if(handle == INVALID_HANDLE) return "";
   string payload = "";
   if(FileSize(handle) <= 65536 && FileReadString(handle) == SubmissionReceiptIdentity(json)
      && !FileIsEnding(handle)) payload = FileReadString(handle);
   FileClose(handle);
   return payload;
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

   if(action == "GET_SUBMISSION")
   {
      string path = SubmissionReceiptPath(json);
      if(path == "")
      {
         SendRpcResponse(bridge_index, request_id, false, "invalid submission correlation/account", "", action);
         return;
      }
      string receipt = LookupSubmissionReceipt(json);
      string payload = "{\"fingerprint\":\"" + ExtractString(json, "submissionFingerprint")
         + "\",\"order\":" + (receipt == "" ? "null" : receipt) + "}";
      SendRpcResponse(bridge_index, request_id, true, "", payload, action);
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
      uint history_started_ms = GetTickCount();
      Print("AQE bridge[", bridge_index, "] GetHistory started request_id=", request_id,
            " symbol=", symbol, " timeframe=", timeframe_code);
      string result = symbol == "" ? "" : HistoryJson(symbol, tf, start_time, end_time);
      Print("AQE bridge[", bridge_index, "] GetHistory prepared request_id=", request_id,
            " elapsed_ms=", GetTickCount() - history_started_ms, " response_chars=", StringLen(result));
      SendRpcResponse(bridge_index, request_id, symbol != "", symbol == "" ? "symbol is required" : "", result, action);
      return;
   }
   if(action == "GET_POSITIONS")
   {
      SendRpcResponse(bridge_index, request_id, true, "", PositionsJson(), action);
      return;
   }
   if(action == "GET_POSITION_VALUATIONS")
   {
      SendRpcResponse(bridge_index, request_id, true, "",
         ExtractNumber(json, "valuationVersion", 0.0) == 1.0
         ? PositionValuationsJson(ExtractString(json, "accountId"), symbol, ExtractStringArray(json, "positionIds"))
         : "null", action);
      return;
   }
   if(action == "GET_EXECUTION_FILL_HISTORY")
   {
      if(g_history_utc_offset_seconds < -50400 || g_history_utc_offset_seconds > 50400
         || g_history_clock_utc_from <= 0 || g_history_clock_utc_through < g_history_clock_utc_from)
      {
         SendRpcResponse(bridge_index, request_id, false, "historical clock evidence is not yet available", "", action);
         return;
      }
      SendRpcResponse(bridge_index, request_id, true, "", ExecutionFillHistoryJson(
         ExtractString(json, "accountId"), ExtractString(json, "entryOrderId"), symbol,
         ExtractNumber(json, "invalidationVersion", 0.0) == 1.0,
         ExtractNumber(json, "positionSnapshotVersion", 0.0) == 1.0), action);
      return;
   }
   if(action == "GET_ORDERS")
   {
      SendRpcResponse(bridge_index, request_id, true, "", OrdersJson(), action);
      return;
   }
   if(action == "RECONCILE_ORDERS")
   {
      SendRpcResponse(
         bridge_index,
         request_id,
         true,
         "",
         ReconcileOrdersJson(ExtractStringArray(json, "orderIds")),
         action
      );
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
   double trailing_stop = ExtractNumber(json, "trailingStop", 0.0);
   string side = ExtractString(json, "side");
   string order_type = ExtractString(json, "orderType");
   string order_id = ExtractString(json, "orderId");
   string client_order_id = ExtractString(json, "clientOrderId");
   string insight_id = ExtractString(json, "insightId");
   string strategy_type = ExtractString(json, "strategyType");
   string comment = NormalizeOrderComment(ExtractString(json, "comment"));

   if(action == "ESTIMATE_MARGIN")
   {
      if(symbol == "" || qty <= 0.0 || (side != "Buy" && side != "Sell"))
      {
         SendRpcResponse(bridge_index, request_id, false, "invalid margin estimate request", "", action);
         return;
      }
      MqlTick margin_tick;
      if(!SymbolInfoTick(symbol, margin_tick))
      {
         SendRpcResponse(bridge_index, request_id, false, "margin estimate quote unavailable", "", action);
         return;
      }
      if(price <= 0.0)
         price = side == "Sell" ? margin_tick.bid : margin_tick.ask;
      double required_margin = 0.0;
      ENUM_ORDER_TYPE margin_side = side == "Sell" ? ORDER_TYPE_SELL : ORDER_TYPE_BUY;
      ResetLastError();
      bool margin_ok = OrderCalcMargin(margin_side, symbol, qty, price, required_margin);
      if(!margin_ok || !MathIsValidNumber(required_margin) || required_margin < 0.0)
      {
         SendRpcResponse(bridge_index, request_id, false,
            "broker margin estimate failed error=" + IntegerToString(GetLastError()), "", action);
         return;
      }
      string margin_payload = "{\"margin\":" + DoubleToString(required_margin, 8) + "}";
      SendRpcResponse(bridge_index, request_id, true, "", margin_payload, action);
      return;
   }

   if(action == "SUBMIT_ORDER")
   {
      if(symbol == "" || qty <= 0.0)
      {
         SendRpcResponse(bridge_index, request_id, false, "invalid submit order request", "", action);
         return;
      }
      string cached_submission = "";
      string receipt_error = "";
      int submission_receipt = BeginSubmissionReceipt(json, cached_submission, receipt_error);
      if(submission_receipt == INVALID_HANDLE)
      {
         SendRpcResponse(bridge_index, request_id, false, receipt_error, "", action);
         return;
      }
      if(cached_submission != "")
      {
         FileClose(submission_receipt);
         SendRpcResponse(bridge_index, request_id, true, "", cached_submission, action);
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
      else if(order_type == "StopLimit")
      {
         // A stop-limit has two distinct entry prices; never downgrade it to Stop.
         double trigger = ExtractNumber(json, "stopPrice", 0.0);
         double entry_limit = ExtractNumber(json, "limitPrice", 0.0);
         if(trigger > 0.0 && entry_limit > 0.0)
            ok = trade.OrderOpen(symbol, side == "Sell" ? ORDER_TYPE_SELL_STOP_LIMIT : ORDER_TYPE_BUY_STOP_LIMIT,
               qty, NormalizeToDigits(symbol, entry_limit), NormalizeToDigits(symbol, trigger),
               normalized_sl, normalized_tp, ORDER_TIME_GTC, 0, RoutedOrderComment(bridge_index, comment));
      }
      else if(order_type == "Stop")
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
      if(ok && trailing_stop > 0.0)
         RegisterTrailingStop(broker_id == "0" ? client_order_id : broker_id, trailing_stop);
      string status = ok ? (order_type == "Market" ? "Filled" : "Accepted") : "Rejected";
      bool definitive_refusal = result_order == 0 && result_deal == 0 &&
         (result_retcode == 10004 || result_retcode == 10006 || result_retcode == 10007
          || (result_retcode >= 10013 && result_retcode <= 10022)
          || result_retcode == 10024 || result_retcode == 10026 || result_retcode == 10027
          || result_retcode == 10030 || result_retcode == 10033 || result_retcode == 10034
          || result_retcode == 10035 || result_retcode == 10040);
      if(!ok && !definitive_refusal)
      {
         // Timeout/connection/unknown errors may have executed remotely. Preserve
         // only the intent marker and do not manufacture a Rejected receipt.
         FileClose(submission_receipt);
         SendRpcResponse(bridge_index, request_id, false, "submission outcome unknown; reconciliation required", "", action);
         return;
      }
      string reason = ok ? "" : IntegerToString((int)result_retcode);
      string payload = OrderJson(broker_id == "0" ? client_order_id : broker_id, symbol, qty, side, order_type, status, result_price, reason, 0.0, false, insight_id, strategy_type);
      ResetLastError();
      uint receipt_written = FileWriteString(submission_receipt, payload + "\r\n");
      FileFlush(submission_receipt);
      if(receipt_written == 0 || GetLastError() != 0)
         Print("AQE submission receipt result persistence failed; retained intent must remain unresolved");
      FileClose(submission_receipt);
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
   if(action == "AMEND_ENTRY")
   {
      ulong ticket = (ulong)StringToInteger(order_id);
      string amend_reason = "";
      bool amended = false;
      if(ticket == 0 || !OrderSelect(ticket))
         amend_reason = "Entry has filled or is no longer pending";
      else
      {
         ENUM_ORDER_TYPE kind = (ENUM_ORDER_TYPE)OrderGetInteger(ORDER_TYPE);
         bool limit_kind = kind == ORDER_TYPE_BUY_LIMIT || kind == ORDER_TYPE_SELL_LIMIT;
         bool stop_kind = kind == ORDER_TYPE_BUY_STOP || kind == ORDER_TYPE_SELL_STOP;
         bool stop_limit_kind = kind == ORDER_TYPE_BUY_STOP_LIMIT || kind == ORDER_TYPE_SELL_STOP_LIMIT;
         double limit_price = ExtractNumber(json, "limitPrice", -1.0);
         double stop_price = ExtractNumber(json, "stopPrice", -1.0);
         string entry_symbol = OrderGetString(ORDER_SYMBOL);
         double entry = OrderGetDouble(ORDER_PRICE_OPEN);
         double stop_limit_price = OrderGetDouble(ORDER_PRICE_STOPLIMIT);
         if(OrderGetDouble(ORDER_VOLUME_CURRENT) < OrderGetDouble(ORDER_VOLUME_INITIAL))
            amend_reason = "Entry has already partially filled";
         else if((limit_price <= 0.0 && stop_price <= 0.0)
            || (limit_price > 0.0 && !limit_kind && !stop_limit_kind)
            || (stop_price > 0.0 && !stop_kind && !stop_limit_kind))
            amend_reason = "Invalid entry price amendment for order type";
         else
         {
            if(limit_price > 0.0)
            {
               if(stop_limit_kind) stop_limit_price = NormalizeToDigits(entry_symbol, limit_price);
               else entry = NormalizeToDigits(entry_symbol, limit_price);
            }
            if(stop_price > 0.0) entry = NormalizeToDigits(entry_symbol, stop_price);
            // Preserve broker-owned protection, volume, expiry and ticket.
            amended = trade.OrderModify(ticket, entry, OrderGetDouble(ORDER_SL), OrderGetDouble(ORDER_TP),
               (ENUM_ORDER_TYPE_TIME)OrderGetInteger(ORDER_TYPE_TIME),
               (datetime)OrderGetInteger(ORDER_TIME_EXPIRATION), stop_limit_price);
            uint amend_retcode = trade.ResultRetcode();
            amended = (amended && IsTradeRetcodeSuccess(amend_retcode)) || amend_retcode == TRADE_RETCODE_NO_CHANGES;
            if(!amended) amend_reason = "Broker declined entry amendment: " + IntegerToString((int)amend_retcode);
         }
      }
      // Transport success with explicit broker result distinguishes a definitive
      // decline from a lost response, which AQE must not blindly retry.
      SendRpcResponse(bridge_index, request_id, true, "",
         "{\"amended\":" + (amended ? "true" : "false") + ",\"reason\":\"" + amend_reason + "\"}", action);
      return;
   }

   if(action == "UPDATE_ORDER")
   {
      ulong position_ticket = FindPositionTicketById(order_id);
      string position_resolution = "ticket-or-identifier";
      if(position_ticket == 0)
      {
         position_ticket = FindPositionTicketByEntryOrderId(order_id);
         if(position_ticket > 0)
            position_resolution = "entry-order";
      }
      bool ambiguous_insight_position = false;
      if(position_ticket == 0 && insight_id != "")
      {
         position_ticket = FindUniquePositionTicketByInsightId(
            bridge_index,
            insight_id,
            symbol,
            ambiguous_insight_position
         );
         if(position_ticket > 0)
            position_resolution = "insight-id";
      }
      if(position_ticket > 0)
      {
         RememberOrderRouteMetadata(bridge_index, order_id, order_id, insight_id, strategy_type);
         RememberOrderRouteMetadata(bridge_index, IntegerToString((long)position_ticket), order_id, insight_id, strategy_type);
         if(position_resolution != "ticket-or-identifier")
            Print("AQE bridge recovered UPDATE_ORDER position order_id=", order_id,
                  " ticket=", position_ticket,
                  " resolution=", position_resolution,
                  " insight_id=", insight_id);
      }

      bool ok = false;
      string reason = "";
      // Missing bracket fields mean preserve. A stop-only update's generic
      // price must never become a TP and be clamped into an immediate exit.
      double take_profit = ExtractNumber(json, "takeProfit", -1.0);
      double stop_loss = ExtractNumber(json, "stopLoss", -1.0);
      ulong pending_order_ticket = 0;
      if(position_ticket == 0)
      {
         ulong requested_order_ticket = (ulong)StringToInteger(order_id);
         if(requested_order_ticket > 0 && OrderSelect(requested_order_ticket))
            pending_order_ticket = requested_order_ticket;
      }

      if(position_ticket == 0 && pending_order_ticket > 0 && OrderSelect(pending_order_ticket))
      {
         string pending_symbol = OrderGetString(ORDER_SYMBOL);
         ENUM_ORDER_TYPE pending_type = (ENUM_ORDER_TYPE)OrderGetInteger(ORDER_TYPE);
         double entry_price = OrderGetDouble(ORDER_PRICE_OPEN);
         double current_sl = OrderGetDouble(ORDER_SL);
         double current_tp = OrderGetDouble(ORDER_TP);
         double stop_limit = OrderGetDouble(ORDER_PRICE_STOPLIMIT);
         ENUM_ORDER_TYPE_TIME type_time = (ENUM_ORDER_TYPE_TIME)OrderGetInteger(ORDER_TYPE_TIME);
         datetime expiration = (datetime)OrderGetInteger(ORDER_TIME_EXPIRATION);
         bool is_buy_order = pending_type == ORDER_TYPE_BUY_LIMIT
                          || pending_type == ORDER_TYPE_BUY_STOP
                          || pending_type == ORDER_TYPE_BUY_STOP_LIMIT;
         bool is_sell_order = pending_type == ORDER_TYPE_SELL_LIMIT
                           || pending_type == ORDER_TYPE_SELL_STOP
                           || pending_type == ORDER_TYPE_SELL_STOP_LIMIT;
         double modify_sl = current_sl;
         double modify_tp = current_tp;

         if(!is_buy_order && !is_sell_order)
         {
            reason = "order ticket is not a modifiable pending entry";
         }
         else if(take_profit > 0.0)
         {
            modify_tp = NormalizeToDigits(pending_symbol, take_profit);
            if((is_buy_order && modify_tp <= entry_price)
               || (is_sell_order && modify_tp >= entry_price))
               reason = "invalid pending-order take profit";
         }
         else if(take_profit == 0.0)
         {
            modify_tp = 0.0;
         }

         if(reason == "" && stop_loss >= 0.0)
         {
            if(stop_loss > 0.0)
            {
               modify_sl = NormalizeToDigits(pending_symbol, stop_loss);
               if((is_buy_order && modify_sl >= entry_price)
                  || (is_sell_order && modify_sl <= entry_price))
                  reason = "invalid pending-order stop loss";
            }
            else
            {
               modify_sl = 0.0;
            }
         }

         if(reason == "")
         {
            trade.SetExpertMagicNumber(27042026);
            ok = trade.OrderModify(
               pending_order_ticket,
               entry_price,
               modify_sl,
               modify_tp,
               type_time,
               expiration,
               stop_limit
            );
            uint result_retcode = trade.ResultRetcode();
            ok = ok && IsTradeRetcodeSuccess(result_retcode);
            if(!ok)
               reason = IntegerToString((int)result_retcode);
         }
      }
      else if(position_ticket == 0 || !PositionSelectByTicket(position_ticket))
      {
         reason = ambiguous_insight_position
            ? "position ticket not found and insight id matched multiple open positions"
            : "position or pending-order ticket not found";
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

         // MT5 represents the native trail and explicit stop as the same
         // position SL. Once a trailing route has tightened it, a later
         // stepped-stop request must never widen the live protection.
         if(reason == "" && FindTrailingStopRoute(order_id) >= 0 && current_sl > 0.0 && modify_sl > 0.0)
         {
            bool would_loosen = position_type == POSITION_TYPE_SELL
               ? modify_sl > current_sl
               : modify_sl < current_sl;
            if(would_loosen)
               modify_sl = current_sl;
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
      ulong result_deal = 0;
      bool ok = position_ticket > 0 && ClosePositionWithComment(position_ticket, close_symbol, close_qty, comment, retcode, result_deal);
      string reason = position_ticket == 0 ? "position ticket not found" : (ok ? "" : IntegerToString((int)retcode));
      double realized_pnl = 0.0;
      bool has_realized_pnl = false;
      double commission = 0.0;
      bool has_commission = false;
      double swap = 0.0;
      bool has_swap = false;
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
         if(result_deal > 0 && HistoryDealSelect(result_deal))
         {
            commission = MathAbs(
               HistoryDealGetDouble(result_deal, DEAL_COMMISSION)
               + HistoryDealGetDouble(result_deal, DEAL_FEE)
            );
            has_commission = true;
            swap = HistoryDealGetDouble(result_deal, DEAL_SWAP);
            has_swap = true;
            realized_pnl =
               HistoryDealGetDouble(result_deal, DEAL_PROFIT)
               + HistoryDealGetDouble(result_deal, DEAL_COMMISSION)
               + HistoryDealGetDouble(result_deal, DEAL_SWAP)
               + HistoryDealGetDouble(result_deal, DEAL_FEE);
            has_realized_pnl = true;
         }
      }
      string payload = ok
         ? OrderJson(IntegerToString((long)position_ticket), close_symbol, close_qty, close_side, "Market", "Closed", close_price, "", realized_pnl, has_realized_pnl, insight_id, strategy_type, "", commission, has_commission, swap, has_swap)
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
         ulong result_deal = 0;
         if(sym != "") ok = ClosePositionWithComment(position_ticket, sym, 0.0, comment, retcode, result_deal) && ok;
      }
      SendRpcResponse(bridge_index, request_id, ok, ok ? "" : IntegerToString((int)GetLastError()), "{\"closed\":true}", action);
      return;
   }

   SendRpcResponse(bridge_index, request_id, false, "unknown RPC action: " + action, "", action);
}

void ClearRuntimeState()
{
   for(int i = 0; i < ArraySize(g_bridges); i++)
   {
      if(g_bridges[i].socket_handle != INVALID_HANDLE)
      {
         if(g_bridges[i].ws_state == WS_STATE_ACTIVE)
            SendWebSocketJson(i, "{\"type\":\"GracefulShutdown\"}");
         SocketClose(g_bridges[i].socket_handle);
      }
   }
   ArrayResize(g_bridges, 0);
   ArrayResize(g_subscriptions, 0);
   ArrayResize(g_order_routes, 0);
   ArrayResize(g_pending_trade_events, 0);
   ArrayResize(g_pending_rpc_commands, 0);
   ArrayResize(g_pending_rpc_responses, 0);
   ArrayResize(g_trailing_stop_routes, 0);
   ArrayResize(g_pending_trade_transactions, 0);
   ArrayResize(g_pending_trade_requests, 0);
   ArrayResize(g_pending_trade_results, 0);
   ArrayResize(g_pending_trade_adapter_queued_us, 0);
   g_next_probe_bridge_index = 0;
   g_next_active_bridge_index = 0;
   g_next_market_bridge_index = 0;
   g_next_ws_bridge_index = 0;
   g_trade_event_seq = 0;
   g_trade_event_drop_count = 0;
   g_last_trailing_stop_ms = 0;
   g_pending_trade_transaction_head = 0;
   g_pending_trade_transaction_count = 0;
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
   g_bridges[index].last_trade_event_flush_ms = 0;
   g_bridges[index].session_logged = false;
   g_bridges[index].supports_fill_revisions = false;
   g_bridges[index].socket_handle = INVALID_HANDLE;
   g_bridges[index].ws_state = WS_STATE_DISCONNECTED;
   g_bridges[index].ws_first_attempt_ms = 0;
   g_bridges[index].ws_next_attempt_ms = 0;
   g_bridges[index].ws_reconnect_delay_ms = SocketReconnectBaseMs();
   g_bridges[index].ws_last_probe_ms = 0;
   g_bridges[index].ws_last_io_ms = 0;
   g_bridges[index].ws_last_acknowledged_sequence = 0;
   g_bridges[index].ws_inflight_trade_sequence = 0;
   g_bridges[index].ws_inflight_trade_count = 0;
   g_bridges[index].ws_rx_length = 0;
   g_bridges[index].ws_rx_hex = "";
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
   // Execute at most one HTTP/RPC batch per timer turn. More queued work is
   // picked up on the next high-resolution timer event, allowing trade events
   // and market data to run between batches.
   int max_cycles = 1;
   for(int cycle = 0; cycle < max_cycles; cycle++)
   {
      if(!IsValidBridgeIndex(bridge_index)) return polled;
      string response;
      string payload = "{\"maxRequests\":1}";
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
      {
         int queue_index = ArraySize(g_pending_rpc_commands);
         ArrayResize(g_pending_rpc_commands, queue_index + request_count);
         for(int i = 0; i < request_count; i++)
         {
            g_pending_rpc_commands[queue_index + i].bridge_index = bridge_index;
            g_pending_rpc_commands[queue_index + i].payload = request_jsons[i];
         }
      }
      if(request_count <= 0)
         return true;
      if(IsValidBridgeIndex(bridge_index))
         g_bridges[bridge_index].next_poll_after_ms = 0;
   }

   if(IsValidBridgeIndex(bridge_index))
      g_bridges[bridge_index].next_poll_after_ms = 0;
   return polled;
}

bool ProcessOneQueuedRpcCommand()
{
   int total = ArraySize(g_pending_rpc_commands);
   if(total <= 0)
      return false;
   PendingRpcCommand command = g_pending_rpc_commands[0];
   for(int i = 1; i < total; i++)
      g_pending_rpc_commands[i - 1] = g_pending_rpc_commands[i];
   ArrayResize(g_pending_rpc_commands, total - 1);
   ExecuteRpcRequest(command.bridge_index, command.payload);
   return true;
}

bool PollOneConnectedBridge()
{
   int bridge_count = ArraySize(g_bridges);
   if(bridge_count <= 0)
      return false;
   if(g_next_active_bridge_index < 0 || g_next_active_bridge_index >= bridge_count)
      g_next_active_bridge_index = 0;

   for(int offset = 0; offset < bridge_count; offset++)
   {
      int bridge_index = (g_next_active_bridge_index + offset) % bridge_count;
      // In Auto mode optional bridges are discovered and maintained by the
      // bounded WebSocket connection path. A failed MQL WebRequest can ignore
      // its requested timeout under Wine and block the EA event thread for
      // several seconds, so never let an optional HTTP fallback delay trade
      // events or command responses for an already active bridge. Users that
      // need an HTTP-only optional bridge can select AQE_TRANSPORT_HTTP.
      if(InpTransportMode == AQE_TRANSPORT_AUTO && bridge_index != 0)
         continue;
      if(!IsBridgeSessionActive(bridge_index)
         || IsWebSocketActive(bridge_index)
         || !HttpTransportAllowed(bridge_index)
         || !IsBridgePollDue(bridge_index))
         continue;
      g_next_active_bridge_index = (bridge_index + 1) % bridge_count;
      return PollRpc(bridge_index, false);
   }
   return false;
}

void ProbeOneDisconnectedBridge()
{
   int bridge_count = ArraySize(g_bridges);
   if(bridge_count <= 0)
      return;

   // The primary InpBridgeUrl must always establish its first session. Until
   // that first poll succeeds, PollOneConnectedBridge() skips it because it has
   // no session id. Optional bridge connections remain opt-in probes.
   if(!IsBridgeSessionActive(0) && HttpTransportAllowed(0) && IsBridgePollDue(0))
   {
      PollRpc(0, false);
      return;
   }

   // Auto mode uses the non-HTTP WebSocket connection attempts in
   // ServiceOneWebSocket() to discover optional bridge URLs. Synchronous HTTP
   // probing of absent optional ports can block MetaTrader's single event
   // thread for seconds despite a small WebRequest timeout.
   if(InpTransportMode != AQE_TRANSPORT_HTTP)
      return;

   if(g_next_probe_bridge_index < 0 || g_next_probe_bridge_index >= bridge_count)
      g_next_probe_bridge_index = 0;

   for(int offset = 0; offset < bridge_count; offset++)
   {
      int bridge_index = (g_next_probe_bridge_index + offset) % bridge_count;
      if(IsBridgeSessionActive(bridge_index)
         || !HttpTransportAllowed(bridge_index)
         || !IsBridgePollDue(bridge_index))
         continue;

      g_next_probe_bridge_index = (bridge_index + 1) % bridge_count;
      PollRpc(bridge_index, true);
      return;
   }
}

bool SendOneBackgroundUpdate()
{
   uint now_ms = GetTickCount();
   for(int i = 0; i < ArraySize(g_bridges); i++)
   {
      if(!IsBridgeSessionActive(i))
         continue;
      if(HasElapsedMs(g_bridges[i].last_heartbeat_ms, 2000))
      {
         g_bridges[i].last_heartbeat_ms = now_ms;
         SendHeartbeat(i);
         return true;
      }
   }
   for(int i = 0; i < ArraySize(g_bridges); i++)
   {
      if(!IsBridgeSessionActive(i))
         continue;
      if((HasBridgeSubscriptions(i) || HasBridgeOrderRoutes(i))
         && TimeCurrent() - g_bridges[i].last_snapshot > 30)
      {
         SendSnapshot(i);
         return true;
      }
   }
   return false;
}

bool QueueTradeTransaction(
   const MqlTradeTransaction &trans,
   const MqlTradeRequest &request,
   const MqlTradeResult &result,
   long adapter_queued_at_us
)
{
   int capacity = ArraySize(g_pending_trade_transactions);
   if(capacity <= 0 || g_pending_trade_transaction_count >= capacity)
   {
      g_trade_event_drop_count++;
      if(g_trade_event_drop_count <= 3 || g_trade_event_drop_count % 100 == 0)
         Print("AQE raw trade-transaction queue is full; rejected newest event to preserve FIFO ordering. dropped=",
               (long)g_trade_event_drop_count);
      return false;
   }

   int index = (g_pending_trade_transaction_head + g_pending_trade_transaction_count) % capacity;
   g_pending_trade_transactions[index] = trans;
   g_pending_trade_requests[index] = request;
   g_pending_trade_results[index] = result;
   g_pending_trade_adapter_queued_us[index] = adapter_queued_at_us;
   g_pending_trade_transaction_count++;
   return true;
}

int ProcessPendingTradeTransactions(int max_count)
{
   int processed = 0;
   uint started_ms = GetTickCount();
   int capacity = ArraySize(g_pending_trade_transactions);
   while(capacity > 0 && g_pending_trade_transaction_count > 0 && processed < max_count)
   {
      int index = g_pending_trade_transaction_head;
      MqlTradeTransaction trans;
      MqlTradeRequest request;
      MqlTradeResult result;
      trans = g_pending_trade_transactions[index];
      request = g_pending_trade_requests[index];
      result = g_pending_trade_results[index];
      long adapter_queued_at_us = g_pending_trade_adapter_queued_us[index];
      g_pending_trade_transaction_head = (g_pending_trade_transaction_head + 1) % capacity;
      g_pending_trade_transaction_count--;
      ProcessTradeTransaction(trans, request, result, adapter_queued_at_us);
      processed++;
      if(GetTickCount() - started_ms >= (uint)TIMER_NONBLOCKING_WORK_BUDGET_MS)
         break;
   }
   return processed;
}

int OnInit()
{
   MathSrand((int)GetTickCount());
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
   if(InpPollIntervalMs < 50)
   {
      Print("AqeMt5BridgeEA parameter error: InpPollIntervalMs must be at least 50.");
      return INIT_PARAMETERS_INCORRECT;
   }
   if(InpRequestTimeoutMs < 1000)
   {
      Print("AqeMt5BridgeEA parameter error: InpRequestTimeoutMs must be at least 1000.");
      return INIT_PARAMETERS_INCORRECT;
   }
   if(InpSocketReconnectBaseMs < 50
      || InpSocketReconnectMaxMs < InpSocketReconnectBaseMs
      || InpHttpFallbackAfterMs < 250
      || InpWebSocketProbeIntervalMs < 1000)
   {
      Print("AqeMt5BridgeEA parameter error: invalid persistent transport timing configuration.");
      return INIT_PARAMETERS_INCORRECT;
   }

   int transaction_capacity = TradeEventQueueCapacity();
   if(ArrayResize(g_pending_trade_transactions, transaction_capacity) != transaction_capacity
      || ArrayResize(g_pending_trade_requests, transaction_capacity) != transaction_capacity
      || ArrayResize(g_pending_trade_results, transaction_capacity) != transaction_capacity
      || ArrayResize(g_pending_trade_adapter_queued_us, transaction_capacity) != transaction_capacity)
   {
      Print("AqeMt5BridgeEA failed to allocate the trade-transaction ring buffer capacity=",
            transaction_capacity);
      return INIT_FAILED;
   }

   int timer_interval_ms = EaTimerIntervalMs();
   EventSetMillisecondTimer(timer_interval_ms);
   Print("AqeMt5BridgeEA started. bridge_count=", bridge_count,
         " config_version=", InpConfigVersion,
         " transport_mode=", EnumToString(InpTransportMode),
         " timer_ms=", timer_interval_ms,
         " poll_ms=", InpPollIntervalMs,
         " market_data_ms=", MarketDataIntervalMs(),
         " trade_ring_capacity=", transaction_capacity,
         " timeout_ms=", InpRequestTimeoutMs,
         " probe_inactive=automatic-rate-limited",
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
   ObserveExecutionClock();
   // Cooperative priority scheduler: one potentially blocking HTTP request per
   // timer turn. MetaTrader serializes EA event handlers, so returning quickly
   // is what lets the next trade transaction or completed candle run promptly.
   // Expand raw MT5 transactions outside OnTradeTransaction, then flush the
   // ordered JSON batch in the same timer turn. The terminal callback itself
   // now only copies three native structs into a preallocated ring buffer.
   ProcessPendingTradeTransactions(TradeEventBatchSize());
   uint started_ms = GetTickCount();
   bool legacy_http_turn = InpTransportMode == AQE_TRANSPORT_HTTP
      || (InpTransportMode == AQE_TRANSPORT_AUTO && ArraySize(g_bridges) == 1 && !IsWebSocketActive(0));
   if(FlushNextTradeEventBatch() && legacy_http_turn) return;
   if(HasElapsedMs(started_ms, TIMER_NONBLOCKING_WORK_BUDGET_MS)) return;
   // Broker command results are critical lifecycle traffic. Flush an already
   // prepared result before servicing connection probes or ordinary market
   // data, then execute the next command so its result is sent on the next
   // 10 ms timer turn.
   if(SendOneQueuedRpcResponse() && legacy_http_turn) return;
   if(HasElapsedMs(started_ms, TIMER_NONBLOCKING_WORK_BUDGET_MS)) return;
   // Market data gets a bounded turn even while ACKs or RPCs keep arriving.
   if(SendNextMarketDataUpdate() && legacy_http_turn) return;
   if(HasElapsedMs(started_ms, TIMER_NONBLOCKING_WORK_BUDGET_MS)) return;
   ServiceOneWebSocket();
   if(HasElapsedMs(started_ms, TIMER_NONBLOCKING_WORK_BUDGET_MS)) return;
   if(ProcessOneQueuedRpcCommand())
      return;
   if(ArraySize(g_trailing_stop_routes) > 0
      && HasElapsedMs(g_last_trailing_stop_ms, ActivePollDelayMs()))
   {
      g_last_trailing_stop_ms = GetTickCount();
      ProcessTrailingStops();
      return;
   }
   if(PollOneConnectedBridge())
      return;
   if(SendOneBackgroundUpdate())
      return;
   ProbeOneDisconnectedBridge();
}

void ProcessTradeTransaction(
   const MqlTradeTransaction &trans,
   const MqlTradeRequest &request,
   const MqlTradeResult &result,
   long adapter_queued_at_us
)
{
   if(trans.order == 0) return;
   // Only these transactions describe an order lifecycle change. History records
   // may confirm cancellation, but must not manufacture late Accepted events.
   if(trans.type != TRADE_TRANSACTION_ORDER_ADD
      && trans.type != TRADE_TRANSACTION_ORDER_UPDATE
      && trans.type != TRADE_TRANSACTION_ORDER_DELETE
      && trans.type != TRADE_TRANSACTION_HISTORY_ADD
      && trans.type != TRADE_TRANSACTION_DEAL_ADD
      && trans.type != TRADE_TRANSACTION_DEAL_UPDATE)
      return;
   bool analytics_only = trans.type == TRADE_TRANSACTION_DEAL_UPDATE;
   int route_bridge = FindOrderBridgeIndex(IntegerToString((long)trans.order));
   if(route_bridge < 0 && trans.deal > 0)
      route_bridge = FindOrderBridgeIndex(IntegerToString((long)trans.deal));
   string event_name = "Accepted";
   if(trans.type == TRADE_TRANSACTION_ORDER_UPDATE) event_name = "Replaced";
   if(trans.type == TRADE_TRANSACTION_DEAL_ADD) event_name = "Filled";
   if(trans.type == TRADE_TRANSACTION_ORDER_DELETE || trans.type == TRADE_TRANSACTION_HISTORY_ADD)
   {
      ENUM_ORDER_STATE order_state = trans.order_state;
      if(HistoryOrderSelect(trans.order))
         order_state = (ENUM_ORDER_STATE)HistoryOrderGetInteger(trans.order, ORDER_STATE);
      if(order_state == ORDER_STATE_CANCELED)
         event_name = "Cancelled";
      else if(order_state == ORDER_STATE_EXPIRED)
         event_name = "Expired";
      else if(order_state == ORDER_STATE_REJECTED)
         event_name = "Rejected";
      else
         return;
   }
   // request/result are only defined for TRADE_TRANSACTION_REQUEST, not the
   // order/deal callbacks above. Resolve identity from the actual ticket, never
   // from the chart hosting this EA (which may trade many other symbols).
   string symbol = trans.symbol;
   long order_kind = trans.order_type;
   double volume = trans.volume;
   double price = trans.price;
   if(OrderSelect(trans.order))
   {
      symbol = OrderGetString(ORDER_SYMBOL);
      order_kind = OrderGetInteger(ORDER_TYPE);
      volume = OrderGetDouble(ORDER_VOLUME_INITIAL);
      price = OrderGetDouble(ORDER_PRICE_OPEN);
      if(route_bridge < 0) route_bridge = FindBridgeByComment(OrderGetString(ORDER_COMMENT));
   }
   else if(HistoryOrderSelect(trans.order))
   {
      symbol = HistoryOrderGetString(trans.order, ORDER_SYMBOL);
      order_kind = HistoryOrderGetInteger(trans.order, ORDER_TYPE);
      volume = HistoryOrderGetDouble(trans.order, ORDER_VOLUME_INITIAL);
      price = HistoryOrderGetDouble(trans.order, ORDER_PRICE_OPEN);
      if(route_bridge < 0) route_bridge = FindBridgeByComment(HistoryOrderGetString(trans.order, ORDER_COMMENT));
   }
   string side = (order_kind == ORDER_TYPE_SELL || order_kind == ORDER_TYPE_SELL_LIMIT
      || order_kind == ORDER_TYPE_SELL_STOP || order_kind == ORDER_TYPE_SELL_STOP_LIMIT) ? "Sell" : "Buy";
   string order_type = "Market";
   if(order_kind == ORDER_TYPE_BUY_LIMIT || order_kind == ORDER_TYPE_SELL_LIMIT) order_type = "Limit";
   if(order_kind == ORDER_TYPE_BUY_STOP || order_kind == ORDER_TYPE_SELL_STOP) order_type = "Stop";
   if(order_kind == ORDER_TYPE_BUY_STOP_LIMIT || order_kind == ORDER_TYPE_SELL_STOP_LIMIT) order_type = "StopLimit";
   double realized_pnl = 0.0;
   bool has_realized_pnl = false;
   bool has_commission = false;
   double commission = 0.0;
   bool has_swap = false;
   double swap = 0.0;
   long source_occurred_at_us = 0;
   string event_order_id = IntegerToString((long)trans.order);
   ulong deal_position_id = 0;
   string entry_order_id = "";
   string entry_comment = "";
   double entry_take_profit = 0.0;
   double entry_stop_loss = 0.0;
   string hit_leg = "";
   string execution_fill_json = "null";
   if((trans.type == TRADE_TRANSACTION_DEAL_ADD || analytics_only) && HistoryDealSelect(trans.deal))
   {
      execution_fill_json = ExecutionFillJson(trans.deal, true, 0, 0, analytics_only);
      long broker_time_ms = HistoryDealGetInteger(trans.deal, DEAL_TIME_MSC);
      if(broker_time_ms > 0)
         source_occurred_at_us = (broker_time_ms - ((long)(analytics_only ? g_history_utc_offset_seconds : BrokerUtcOffsetSeconds()) * 1000)) * 1000;
      ENUM_DEAL_ENTRY deal_entry = (ENUM_DEAL_ENTRY)HistoryDealGetInteger(trans.deal, DEAL_ENTRY);
      ENUM_DEAL_TYPE deal_type = (ENUM_DEAL_TYPE)HistoryDealGetInteger(trans.deal, DEAL_TYPE);
      ENUM_DEAL_REASON deal_reason = (ENUM_DEAL_REASON)HistoryDealGetInteger(trans.deal, DEAL_REASON);
      deal_position_id = (ulong)HistoryDealGetInteger(trans.deal, DEAL_POSITION_ID);
      string deal_symbol = HistoryDealGetString(trans.deal, DEAL_SYMBOL);
      string deal_comment = HistoryDealGetString(trans.deal, DEAL_COMMENT);
      double deal_volume = HistoryDealGetDouble(trans.deal, DEAL_VOLUME);
      double deal_price = HistoryDealGetDouble(trans.deal, DEAL_PRICE);
      if(deal_entry == DEAL_ENTRY_OUT || deal_entry == DEAL_ENTRY_OUT_BY)
      {
         event_name = "Closed";
         hit_leg = HitLegFromDeal(deal_reason, deal_comment);
         commission = MathAbs(
            HistoryDealGetDouble(trans.deal, DEAL_COMMISSION)
            + HistoryDealGetDouble(trans.deal, DEAL_FEE)
         );
         has_commission = true;
         swap = HistoryDealGetDouble(trans.deal, DEAL_SWAP);
         has_swap = true;
         realized_pnl =
            HistoryDealGetDouble(trans.deal, DEAL_PROFIT)
            + HistoryDealGetDouble(trans.deal, DEAL_COMMISSION)
            + HistoryDealGetDouble(trans.deal, DEAL_SWAP)
            + HistoryDealGetDouble(trans.deal, DEAL_FEE);
         has_realized_pnl = true;
         FindEntryOrderForPosition(
            deal_position_id,
            deal_symbol,
            entry_order_id,
            entry_comment,
            entry_take_profit,
            entry_stop_loss
         );
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
   if(symbol == "")
   {
      Print("AQE trade event unresolved symbol; order=", trans.order, " deal=", trans.deal,
            " transaction=", EnumToString(trans.type), "; not substituting chart symbol");
      return;
   }
   if(analytics_only && execution_fill_json == "null")
   {
      // Deletions/cancelled deal types need explicit invalidation evidence;
      // neither a zero-cash fill nor a new order event is a safe substitute.
      Print("AQE deal correction lacks readable execution evidence; deal=", trans.deal);
      return;
   }
   if(route_bridge < 0)
      route_bridge = FindOrderBridgeIndex(event_order_id);
   if(route_bridge < 0 && deal_position_id > 0)
      route_bridge = FindOrderBridgeIndex(IntegerToString((long)deal_position_id));
   if(route_bridge < 0 && entry_order_id != "")
      route_bridge = FindOrderBridgeIndex(entry_order_id);
   if(route_bridge < 0 && entry_comment != "")
      route_bridge = FindBridgeByComment(entry_comment);
   string order_key = IntegerToString((long)trans.order);
   string deal_key = trans.deal > 0 ? IntegerToString((long)trans.deal) : "";
   string position_key = deal_position_id > 0 ? IntegerToString((long)deal_position_id) : "";
   int route_index = FindOrderRouteIndexByAliases(event_order_id, order_key, deal_key, position_key);
   if(route_index < 0 && entry_order_id != "")
      route_index = FindOrderRouteIndex(entry_order_id);
   if(route_bridge < 0 && route_index >= 0)
      route_bridge = g_order_routes[route_index].bridge_index;
   if(route_bridge < 0 || !IsValidBridgeIndex(route_bridge) || g_bridges[route_bridge].session_id == "")
      return;
   if(analytics_only && !g_bridges[route_bridge].supports_fill_revisions)
   {
      Print("AQE deal correction requires a bridge with executionFillRevisionVersion=1; deal=", trans.deal);
      return;
   }

   string route_client_order_id = route_index >= 0 ? g_order_routes[route_index].client_order_id : "";
   string route_insight_id = route_index >= 0 ? g_order_routes[route_index].insight_id : "";
   string route_strategy_type = route_index >= 0 ? g_order_routes[route_index].strategy_type : "";
   if(event_name == "Closed" && route_insight_id == "" && entry_order_id != "")
      event_order_id = entry_order_id;

   string legs_json = "";
   if(event_name == "Closed" && hit_leg != "")
   {
      if(hit_leg == "TakeProfit" && entry_take_profit <= 0.0)
         entry_take_profit = price;
      if(hit_leg == "StopLoss" && entry_stop_loss <= 0.0)
         entry_stop_loss = price;
      legs_json = OrderLegsJson(hit_leg, order_key, side, price, entry_take_profit, entry_stop_loss);
   }

   RememberOrderRouteMetadata(route_bridge, event_order_id, route_client_order_id, route_insight_id, route_strategy_type);
   if(deal_position_id > 0) RememberOrderRouteMetadata(route_bridge, IntegerToString((long)deal_position_id), route_client_order_id, route_insight_id, route_strategy_type);
   if(entry_order_id != "") RememberOrderRouteMetadata(route_bridge, entry_order_id, route_client_order_id, route_insight_id, route_strategy_type);
   if(trans.order > 0) RememberOrderRouteMetadata(route_bridge, IntegerToString((long)trans.order), route_client_order_id, route_insight_id, route_strategy_type);
   if(trans.deal > 0) RememberOrderRouteMetadata(route_bridge, IntegerToString((long)trans.deal), route_client_order_id, route_insight_id, route_strategy_type);

   string position_accounting_json = !analytics_only && event_name == "Closed" ? PositionAccountingJson(deal_position_id) : "null";
   string native_id = IntegerToString((int)trans.type) + ":" + IntegerToString((long)trans.order) + ":" + IntegerToString((long)trans.deal);
   // Repeated UPDATE callbacks for one ticket are distinct observations. Queue
   // retries retain this frozen ID; economic deduplication is by deal identity.
   if(analytics_only) native_id += ":revision:" + IntegerToString((long)(g_trade_event_seq + 1));
   string payload = "{"
      "\"nativeEventId\":\"" + JsonEscape(native_id) + "\","
      "\"analyticsOnly\":" + (analytics_only ? "true" : "false") + ","
      "\"sourceOccurredAtUs\":" + (source_occurred_at_us > 0 ? IntegerToString(source_occurred_at_us) : "null") + ","
      "\"adapterQueuedAtUs\":" + (adapter_queued_at_us > 0 ? IntegerToString(adapter_queued_at_us) : "null") + ","
      "\"executionFill\":" + execution_fill_json + ","
      "\"event\":\"" + event_name + "\","
      "\"order\":" + OrderJson(event_order_id, symbol, volume, side, order_type, event_name, price, "", realized_pnl, has_realized_pnl, route_insight_id, route_strategy_type, legs_json, commission, has_commission, swap, has_swap, position_accounting_json) +
   "}";
   QueueTradeEvent(route_bridge, payload, analytics_only);
   // The same timer turn which expanded this native transaction can now flush
   // the ordered batch without ever doing HTTP in OnTradeTransaction.
   g_bridges[route_bridge].last_trade_event_flush_ms = 0;
}

void OnTradeTransaction(
   const MqlTradeTransaction &trans,
   const MqlTradeRequest &request,
   const MqlTradeResult &result
)
{
   if(trans.order == 0)
      return;
   QueueTradeTransaction(trans, request, result, ((long)TimeGMT()) * 1000000);
}
