// Execute the actual MQL lookup body against a C++ shim modelling MT5's
// selected-history semantics. No terminal connection or trading is performed.
import assert from 'node:assert/strict';
import { readFileSync, mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';

const source = readFileSync(new URL('./AqeMt5BridgeEA.mq5', import.meta.url), 'utf8');
const start = source.indexOf('bool FindEntryOrderForPosition(');
const end = source.indexOf('\nstring OrderJson(', start);
assert(start >= 0 && end > start);
const lookup = source.slice(start, end);
const closureStart = source.indexOf('bool HasConfirmedPositionClosure(');
const closureEnd = source.indexOf('\nstring PositionCloseDetailsJson(', closureStart);
assert(closureStart >= 0 && closureEnd > closureStart);
const closure = source.slice(closureStart, closureEnd);
const details = source.slice(closureEnd, source.indexOf('\nstring ReconcileOrderLifecycleJson(', closureEnd));
const shim = String.raw`
#include <string>
#include <vector>
#include <map>
#include <stdexcept>
#include <iostream>
#include <cmath>
#include <sstream>
#include <iomanip>
using std::string;
using ulong = unsigned long;
enum ENUM_DEAL_ENTRY { DEAL_ENTRY_IN, DEAL_ENTRY_OUT, DEAL_ENTRY_INOUT, DEAL_ENTRY_OUT_BY };
enum ENUM_DEAL_TYPE { DEAL_TYPE_BUY, DEAL_TYPE_SELL, DEAL_TYPE_BALANCE, DEAL_TYPE_BUY_CANCELED, DEAL_TYPE_SELL_CANCELED };
enum { DEAL_POSITION_ID, DEAL_ENTRY, DEAL_ORDER, DEAL_SYMBOL, DEAL_COMMENT,
       ORDER_COMMENT, ORDER_TP, ORDER_SL, DEAL_TYPE, DEAL_VOLUME,
       DEAL_PRICE, DEAL_PROFIT, DEAL_COMMISSION, DEAL_FEE, DEAL_SWAP, DEAL_TIME_MSC, ORDER_SYMBOL, ORDER_POSITION_ID };
struct Deal { ulong ticket, position, order; ENUM_DEAL_ENTRY entry; string symbol, comment;
              double volume = 1.0; ENUM_DEAL_TYPE type = DEAL_TYPE_BUY;
              double price = 100, profit = 0, commission = 0, fee = 0, swap = 0;
              long time_ms = 10000000; };
std::vector<Deal> deals;
std::vector<ulong> selected;
std::map<ulong, string> orders;
bool available = true;
int visited = 0;
ulong open_position = 0;
enum { POSITION_IDENTIFIER, POSITION_SYMBOL, POSITION_VOLUME, POSITION_TYPE, POSITION_TIME_UPDATE_MSC,
       POSITION_TYPE_BUY, POSITION_TYPE_SELL };
double open_quantity = 1.;
bool positions_unreadable = false, position_changes_during_read = false;
enum { TERMINAL_CONNECTED };
bool terminal_connected = true;
long TerminalInfoInteger(int) { return terminal_connected ? 1 : 0; }
int position_reads = 0;
int PositionsTotal() { return open_position > 0 ? 1 : 0; }
ulong PositionGetTicket(int) { return positions_unreadable ? 0 : open_position + 10; }
bool PositionSelectByTicket(ulong ticket) { return !positions_unreadable && ticket == open_position + 10; }
long PositionGetInteger(int field) {
  if(field == POSITION_IDENTIFIER) return open_position;
  if(field == POSITION_TYPE) return POSITION_TYPE_BUY;
  return 12345;
}
string PositionGetString(int) { return "BTCUSD"; }
double PositionGetDouble(int) {
  ++position_reads;
  return open_quantity + (position_changes_during_read && position_reads > 1 ? 1 : 0);
}
ulong FindPositionTicketById(string id) { return id == std::to_string(open_position) ? open_position : 0; }
double MathAbs(double value) { return std::abs(value); }
string DoubleToString(double value, int digits) { std::ostringstream s; s << std::fixed << std::setprecision(digits) << value; return s.str(); }
int BrokerUtcOffsetSeconds() { return 3600; }
long g_last_fill_as_of_us = 0;
int g_history_utc_offset_seconds = 3600;
long g_history_clock_utc_from = 1, g_history_clock_utc_through = 20000;
enum { ACCOUNT_CURRENCY, ACCOUNT_LOGIN };
long AccountInfoInteger(int) { return 42; }
long StringToInteger(string s) { try { return std::stol(s); } catch (...) { return 0; } }
int StringLen(string s) { return s.size(); }
string AccountInfoString(int) { return "EUR"; }
long TimeGMT() { return 20000; }
bool MathIsValidNumber(double n) { return std::isfinite(n); }
string JsonEscape(string s) { return s; }
string IsoTimeMillis(long value) { return std::to_string(value); }
Deal& deal(ulong id) {
  for (auto& d : deals) if (d.ticket == id) return d;
  throw std::runtime_error("unknown deal");
}
bool HistorySelectByPosition(ulong id) {
  selected.clear();
  if (!available) return false;
  for (auto& d : deals) if (d.position == id) selected.push_back(d.ticket);
  return true;
}
int HistoryDealsTotal() { return selected.size(); }
ulong HistoryDealGetTicket(int i) {
  visited++;
  return i >= 0 && i < (int)selected.size() ? selected[i] : 0;
}
// Intentionally model the destructive API used by the broken implementation.
bool HistoryDealSelect(ulong id) { selected = {id}; return true; }
long HistoryDealGetInteger(ulong id, int field) {
  auto& d = deal(id);
  if (field == DEAL_POSITION_ID) return d.position;
  if (field == DEAL_ENTRY) return d.entry;
  if (field == DEAL_TYPE) return d.type;
  if (field == DEAL_TIME_MSC) return d.time_ms;
  return d.order;
}
string HistoryDealGetString(ulong id, int field) {
  auto& d = deal(id);
  return field == DEAL_SYMBOL ? d.symbol : d.comment;
}
double HistoryDealGetDouble(ulong id, int field) {
  auto& d = deal(id);
  if (field == DEAL_PRICE) return d.price;
  if (field == DEAL_PROFIT) return d.profit;
  if (field == DEAL_COMMISSION) return d.commission;
  if (field == DEAL_FEE) return d.fee;
  if (field == DEAL_SWAP) return d.swap;
  return d.volume;
}
bool HistoryOrderSelect(ulong id) { return orders.count(id); }
string HistoryOrderGetString(ulong id, int field) {
  if (field == ORDER_SYMBOL) { for (auto& d : deals) if (d.order == id) return d.symbol; return ""; }
  return orders.at(id);
}
long HistoryOrderGetInteger(ulong id, int) { for (auto& d : deals) if (d.order == id) return d.position; return 0; }
double HistoryOrderGetDouble(ulong, int field) { return field == ORDER_TP ? 79475.17 : 79718.84; }
string IntegerToString(long v) { return std::to_string(v); }
void check(bool ok, const char* message) { if (!ok) throw std::runtime_error(message); }
`;
const cases = String.raw`
int main() { try {
  const string owner = "AQE:be891349:Openingrange:4f04d";
  deals = {
    {1, 1910983148, 1910983148, DEAL_ENTRY_IN, "BTCUSD", owner},
    {2, 1910983148, 1910985000, DEAL_ENTRY_OUT, "BTCUSD", "partial close"},
    {3, 1910983148, 1910985012, DEAL_ENTRY_OUT, "BTCUSD", "manual close"},
    {4, 1910983153, 1910983153, DEAL_ENTRY_IN, "BTCUSD", ""},
    {5, 1910983153, 1910986434, DEAL_ENTRY_OUT_BY, "BTCUSD", "manual close"}
  };
  orders[1910983148] = owner;
  orders[1910983153] = "AQE:be891349:Openingrange-CHILD";
  // Unrelated account history must not increase the exact-position scan.
  for (ulong i = 0; i < 10000; ++i)
    deals.push_back({100 + i, 999, 999, DEAL_ENTRY_OUT, "ETHUSD", ""});
  string id, comment;
  double tp, sl;
  check(FindEntryOrderForPosition(1910983148, "BTCUSD", id, comment, tp, sl), "parent entry missing after manual close");
  check(id == "1910983148" && comment == owner, "parent ownership mismatch");
  check(visited == 3, "lookup scanned unrelated history");
  check(tp == 79475.17 && sl == 79718.84, "entry levels missing");
  check(FindEntryOrderForPosition(1910983153, "BTCUSD", id, comment, tp, sl), "child entry missing");
  check(id == "1910983153" && comment == orders[1910983153], "order-comment fallback failed");
  check(!FindEntryOrderForPosition(1910983148, "ETHUSD", id, comment, tp, sl), "wrong symbol accepted");
  check(!FindEntryOrderForPosition(0, "BTCUSD", id, comment, tp, sl), "zero identifier accepted");
  check(id.empty() && comment.empty() && tp == 0 && sl == 0, "failed lookup retained ownership");
  check(!FindEntryOrderForPosition(123, "BTCUSD", id, comment, tp, sl), "unknown position accepted");
  available = false;
  check(!FindEntryOrderForPosition(1910983148, "BTCUSD", id, comment, tp, sl), "unavailable history accepted");
  check(!HasConfirmedPositionClosure(1910983148), "unavailable history proves closure");
  available = true;
  deal(1).volume = 10; deal(2).volume = 4; deal(3).volume = 6;
  deal(4).volume = 6.89; deal(5).volume = 6.89;
  check(HasConfirmedPositionClosure(1910983148), "full close after partial close not confirmed");
  check(HasConfirmedPositionClosure(1910983153), "close-by not confirmed");
  deal(2).price = 100; deal(3).price = 110;
  deal(2).profit = 10; deal(3).profit = 20;
  deal(1).commission = -1; deal(3).commission = -2;
  deal(3).fee = -0.5; deal(3).swap = -3;
  deal(3).time_ms = 11000000;
  const string execution = PositionCloseDetailsJson(1910983148);
  check(execution.find("\"closePrice\":106.00000000") != string::npos, "exit price not volume weighted");
  check(execution.find("\"realizedPnl\":23.50000000") != string::npos, "net realized PnL incorrect");
  check(execution.find("\"commission\":3.50000000") != string::npos, "entry/exit costs missing");
  check(execution.find("\"closedAt\":\"7400000\"") != string::npos, "last broker exit timestamp not used");
  const string partial = ExecutionFillJson(2);
  check(partial.find("\"fill_id\":\"2\"") != string::npos, "durable deal identity missing");
  check(partial.find("\"order_id\":\"1910985000\"") != string::npos, "fill used position alias instead of order ticket");
  check(partial.find("\"quantity\":4.00000000") != string::npos, "fill used cumulative position quantity");
  check(partial.find("\"effect\":\"exit\"") != string::npos, "partial exit classification lost");
  check(partial.find("\"occurred_at_us\":6400000000") != string::npos, "deal timestamp/UTC conversion missing");
  check(partial.find("\"gross_pnl\":10.00000000") != string::npos, "lifetime PnL substituted for deal PnL");
  check(partial.find("\"account_notional\":null") != string::npos, "unverified contract/FX notional inferred");
  deal(2).commission=2.; deal(2).fee=-0.5; deal(2).swap=1.; deal(2).price=-5.;
  const string rebate = ExecutionFillJson(2);
  check(rebate.find("\"commission\":-1.50000000") != string::npos, "rebate sign lost");
  check(rebate.find("\"carry\":1.00000000") != string::npos, "carry sign lost");
  check(rebate.find("\"price\":-5.00000000") != string::npos, "valid negative execution price discarded");
  deal(2).type=DEAL_TYPE_BALANCE;
  check(ExecutionFillJson(2)=="null", "account balance operation fabricated a trade fill");
  deal(2).type=DEAL_TYPE_SELL; deal(2).entry=DEAL_ENTRY_INOUT;
  check(ExecutionFillJson(2).find("\"effect\":\"reversal\"") != string::npos, "reversal silently split into inferred entries/exits");
  check(ExecutionFillJson(0)=="null", "missing ticket produced fill evidence");
  deal(2).type=DEAL_TYPE_BUY; deal(2).entry=DEAL_ENTRY_OUT; deal(2).price=100.;
  open_position = 1910983148;
  check(!HasConfirmedPositionClosure(1910983148), "existing position falsely closed");
  open_position = 0;
  deal(3).volume = 5;
  check(!HasConfirmedPositionClosure(1910983148), "partial exit falsely closed");
  check(PositionCloseDetailsJson(1910983148) == "null", "partial exit returned final execution values");
  deal(3).volume = 7;
  check(!HasConfirmedPositionClosure(1910983148), "unbalanced history falsely closed");
  deal(3).volume = 6;
  deal(1).entry = DEAL_ENTRY_INOUT;
  check(!HasConfirmedPositionClosure(1910983148), "ambiguous reversal falsely closed");
  deals = {{1, 42, 42, DEAL_ENTRY_IN, "BTCUSD", owner}};
  check(!HasConfirmedPositionClosure(42), "entry-only history falsely closed");
  deal(1).entry = DEAL_ENTRY_OUT;
  check(!HasConfirmedPositionClosure(42), "exit-only history falsely closed");
  deals.clear();
  check(!HasConfirmedPositionClosure(42), "empty history falsely closed");
  check(!HasConfirmedPositionClosure(0), "zero position falsely closed");
  deals = {{1, 42, 42, DEAL_ENTRY_IN, "BTCUSD", owner},
           {2, 42, 43, DEAL_ENTRY_OUT, "BTCUSD", "partial"},
           {3, 42, 44, DEAL_ENTRY_OUT, "BTCUSD", "close"}};
  deals[0].volume = 2;
  deals[1].type = deals[2].type = DEAL_TYPE_SELL;
  orders[42] = owner;
  visited = 0;
  const string recovered = ExecutionFillHistoryJson("42", "42", "BTCUSD");
  check(recovered.find("\"fill_id\":\"1\"") != string::npos
        && recovered.find("\"fill_id\":\"2\"") != string::npos
        && recovered.find("\"fill_id\":\"3\"") != string::npos, "history recovery lost entry or partial exits");
  check(visited == 3, "history recovery did not use bounded exact-position traversal");
  check(recovered.find("declared_fixed_offset") != string::npos, "historical clock assumption missing");
  check(ExecutionFillHistoryJson("foreign", "42", "BTCUSD") == "null", "foreign account accepted");
  check(ExecutionFillHistoryJson("42", "42", "ETHUSD") == "null", "foreign symbol accepted");
  check(ExecutionFillHistoryJson("42", "042", "BTCUSD") == "null", "noncanonical identity accepted");
  g_history_utc_offset_seconds = 86400;
  check(ExecutionFillHistoryJson("42", "42", "BTCUSD") == "null", "unknown historical offset inferred");
  check(ExecutionFillJson(1,true,0,0,true) == "null", "old revision used current broker offset without historical proof");
  g_history_utc_offset_seconds = 3600; g_history_clock_utc_from = 10000;
  check(ExecutionFillHistoryJson("42", "42", "BTCUSD") == "null", "out-of-window history accepted");
  check(ExecutionFillJson(1,true,0,0,true) == "null", "out-of-window revision accepted");
  g_history_clock_utc_from = 1;
  check(ExecutionFillJson(1,true,0,0,true) != "null", "known-clock revision rejected");
  deals[1].entry = DEAL_ENTRY_IN;
  check(ExecutionFillHistoryJson("42", "42", "BTCUSD") == "null", "mixed-entry netting attributed to one owner");
  deals[1].entry = DEAL_ENTRY_INOUT;
  check(ExecutionFillHistoryJson("42", "42", "BTCUSD") == "null", "reversal guessed");
  deals[1].entry = DEAL_ENTRY_OUT; deals[1].type = DEAL_TYPE_SELL_CANCELED;
  check(ExecutionFillHistoryJson("42", "42", "BTCUSD") == "null", "cancelled deal economics accepted");
  const string cancelled = ExecutionFillHistoryJson("42", "42", "BTCUSD", true);
  check(cancelled.find("\"invalidations\":[{\"provider\":\"mt5-deal-v1\",\"fill_id\":\"2\"") != string::npos,
        "cancelled execution did not produce an explicit tombstone");
  check(cancelled.find("\"reason\":\"broker_cancelled\"") != string::npos, "cancellation reason missing");
  check(cancelled.find("\"fill_id\":\"2\"") == cancelled.rfind("\"fill_id\":\"2\""), "cancelled deal also emitted as a fill");
  deals[1].symbol = "OTHER";
  check(ExecutionFillHistoryJson("42", "42", "BTCUSD", true) == "null", "foreign cancelled fill accepted");
  deals[1].symbol = "BTCUSD";
  deals[1].type = DEAL_TYPE_SELL;
  open_position = 42; open_quantity = 1.25;
  const string checked = ExecutionFillHistoryJson("42", "42", "BTCUSD", true, true);
  check(checked.find("\"position_snapshot\":{\"position_id\":\"42\"") != string::npos, "position identity observation absent");
  check(checked.find("\"quantity\":1.2500000000000000,\"side\":\"buy\"") != string::npos, "position snapshot does not use actual remaining quantity");
  check(ExecutionFillHistoryJson("42", "42", "BTCUSD", true).find("\"position_snapshot\":null") != string::npos, "legacy request got an unrequested position scan");
  position_reads = 0; position_changes_during_read = true;
  check(ExecutionFillHistoryJson("42", "42", "BTCUSD", true, true) == "null", "racing position accepted as a stable snapshot");
  position_changes_during_read = false; positions_unreadable = true;
  check(ExecutionFillHistoryJson("42", "42", "BTCUSD", true, true) == "null", "failed position read invented a flat position");
  positions_unreadable = false; open_position = 0;
  terminal_connected = false;
  check(ExecutionFillHistoryJson("42", "42", "BTCUSD", true, true) == "null", "offline account reported a confirmed flat position");
  terminal_connected = true;
  check(ExecutionFillHistoryJson("42", "42", "BTCUSD", true, true).find("\"quantity\":0.0000000000000000,\"side\":null") != string::npos, "confirmed absent position not explicit");
  for (int i=0; i<254; i++) deals.push_back({(ulong)(100+i), 42, 42, DEAL_ENTRY_IN, "BTCUSD", owner});
  check(ExecutionFillHistoryJson("42", "42", "BTCUSD") == "null", "oversized history truncated and accepted");
  std::cout << "entry-history regression cases passed\n";
  return 0;
} catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; } }
`;
const dir = mkdtempSync(join(tmpdir(), 'aqe-entry-history-'));
try {
  function run(body, name) {
    const cpp = join(dir, `${name}.cpp`), binary = join(dir, name);
    // MQL string literals are strings; C++ literals are char arrays. Normalize
    // only the leading return operand, retaining the actual producer equations.
    const mql = (body + closure + details).replace(/return ("(?:[^"\\]|\\.)*")/g, 'return string($1)');
    writeFileSync(cpp, shim + mql + cases);
    const compile = spawnSync(process.env.CXX || 'c++', ['-std=c++17', cpp, '-o', binary], { encoding: 'utf8' });
    assert.equal(compile.status, 0, compile.stderr || String(compile.error));
    return spawnSync(binary, [], { encoding: 'utf8' });
  }
  const result = run(lookup, 'fixed');
  assert.equal(result.status, 0, result.stderr);
  process.stdout.write(result.stdout);
  const broken = lookup.replace('if(deal_ticket == 0)', 'if(deal_ticket == 0 || !HistoryDealSelect(deal_ticket))');
  assert.notEqual(broken, lookup);
  const regression = run(broken, 'mutation');
  assert.equal(regression.status, 1, 'test must reject the original destructive-selection bug');
  assert.match(regression.stderr, /parent entry missing/);
  console.log('original-bug mutation rejected');
} finally { rmSync(dir, { recursive: true, force: true }); }
