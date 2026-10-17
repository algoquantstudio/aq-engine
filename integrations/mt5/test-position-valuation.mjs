// Execute the actual read-only MQL valuation implementation against a small
// terminal shim. This validates source semantics, not MetaEditor compilation.
import assert from 'node:assert/strict';
import { readFileSync, mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';

const source = readFileSync(new URL('./AqeMt5BridgeEA.mq5', import.meta.url), 'utf8');
const start = source.indexOf('// BEGIN read-only position valuation.');
const end = source.indexOf('// END read-only position valuation.', start);
assert(start >= 0 && end > start);
const mql = source.slice(start, end);
const clock = source.slice(source.indexOf('// BEGIN automatic execution clock evidence.'), source.indexOf('// END automatic execution clock evidence.'));
const rounding = source.slice(source.indexOf('int RoundOffsetToMinute('), source.indexOf('int BrokerUtcOffsetSeconds('));
assert(clock.includes('void ObserveExecutionClock()'));
assert.doesNotMatch(source, /input\s+(int|datetime)\s+InpHistory/);
assert.match(source, /void OnTimer\(\)\s*\{\s*ObserveExecutionClock\(\)/);
assert.doesNotMatch(mql, /\b(?:trade\.|OrderSend|OrderSendAsync|SymbolSelect|HistorySelect|HistoryDealSelect|PositionClose|PositionModify)/);
assert(source.includes('action == "GET_POSITION_VALUATIONS"'));
const body = mql.replace(/string &(ids|values)\[\]/g, 'std::vector<string> &$1')
  .replace('string ids[];', 'std::vector<string> ids;')
  .replace('string before[], after[];', 'std::vector<string> before, after;');
const shim = String.raw`
#include <string>
#include <vector>
#include <sstream>
#include <iomanip>
#include <iostream>
#include <cmath>
#include <limits>
#include <cassert>
using std::string;
using ulong = unsigned long;
using datetime = long;
enum { ACCOUNT_LOGIN, ACCOUNT_CURRENCY, ACCOUNT_SERVER, TERMINAL_CONNECTED, SYMBOL_TRADE_CALC_MODE,
 SYMBOL_TRADE_CONTRACT_SIZE, SYMBOL_CURRENCY_PROFIT, POSITION_IDENTIFIER, POSITION_SYMBOL,
 POSITION_VOLUME, POSITION_PRICE_OPEN, POSITION_PRICE_CURRENT, POSITION_PROFIT, POSITION_SWAP,
 POSITION_TYPE, POSITION_TIME_UPDATE_MSC, POSITION_TYPE_BUY, POSITION_TYPE_SELL };
enum { SYMBOL_CALC_MODE_FOREX, SYMBOL_CALC_MODE_FOREX_NO_LEVERAGE, SYMBOL_CALC_MODE_CFD,
 SYMBOL_CALC_MODE_CFDINDEX, SYMBOL_CALC_MODE_CFDLEVERAGE, SYMBOL_CALC_MODE_EXCH_STOCKS,
 SYMBOL_CALC_MODE_EXCH_STOCKS_MOEX, SYMBOL_CALC_MODE_FUTURES, SYMBOL_CALC_MODE_EXCH_BONDS };
struct MqlTick { long time_msc=0; double bid=0,ask=0; };
struct P {long id=101,ticket=901,type=POSITION_TYPE_BUY,updated=10000; string symbol="GENERIC";
 double qty=2,entry=99,price=100,pnl=2,carry=-0.2;};
std::vector<P> positions;
int selected=0,passes=0,tick_reads=0,account_reads=0,currency_reads=0;
string scenario="valid",currency="GBP",price_currency="GBP";
int mode=SYMBOL_CALC_MODE_CFD;
double multiplier=100;
long g_history_utc_offset_seconds=3600,g_history_clock_utc_from=1,g_history_clock_utc_through=30000,g_history_clock_last_observed=20000;
long g_history_clock_account=42,utc_clock=20000,server_offset=3600,tick_lag=0;
string g_history_clock_server="test-server",server="test-server";
void reset(string s){ scenario=s; positions={P{}}; selected=passes=tick_reads=account_reads=currency_reads=0;
 currency=price_currency="GBP";mode=SYMBOL_CALC_MODE_CFD;multiplier=100;
 g_history_utc_offset_seconds=3600;g_history_clock_utc_from=1;g_history_clock_utc_through=30000;g_history_clock_last_observed=20000;
 g_history_clock_account=42;utc_clock=20000;server_offset=3600;tick_lag=0;g_history_clock_server=server="test-server"; }
long TimeGMT(){return utc_clock;}
long TimeTradeServer(){return utc_clock+server_offset;}
long TimeCurrent(){return TimeTradeServer()-tick_lag;}
long AccountInfoInteger(int){++account_reads;return scenario=="account_changed" && account_reads>1 ? 43 : 42;}
string AccountInfoString(int field){if(field==ACCOUNT_SERVER)return server;++currency_reads;return scenario=="currency_changed" && currency_reads>1 ? "USD" : currency;}
long TerminalInfoInteger(int){return scenario=="disconnected" || (scenario=="disconnect_during" && passes>=2) ? 0 : 1;}
bool SymbolInfoInteger(string,int,long &v){v=mode;return scenario!="metadata_missing";}
bool SymbolInfoDouble(string,int,double &v){v=multiplier;return scenario!="metadata_missing";}
bool SymbolInfoString(string,int,string &v){v=price_currency;return scenario!="metadata_missing";}
bool SymbolInfoTick(string,MqlTick &v){++tick_reads;if(scenario=="no_quote")return false;
 v.time_msc=23600000;v.bid=100;v.ask=101;
 if(scenario=="quote_changed" && tick_reads>1){v.time_msc++;v.bid+=1;}
 if(scenario=="future_quote")v.time_msc+=10000;
 return true;}
void ZeroMemory(MqlTick &v){v=MqlTick{};}
int PositionsTotal(){return scenario=="oversized" ? 4097 : positions.size();}
ulong PositionGetTicket(int i){if(i==0)passes++; if(scenario=="read_failed")return 0;return positions.at(i).ticket;}
bool PositionSelectByTicket(ulong t){for(int i=0;i<(int)positions.size();i++)if(positions[i].ticket==(long)t){selected=i;return true;}return false;}
bool PositionGetInteger(int field,long &v){auto &p=positions.at(selected);if(scenario=="field_failed")return false;
 v=field==POSITION_IDENTIFIER?p.id:field==POSITION_TYPE?p.type:p.updated;
 if(scenario=="update_changed"&&field==POSITION_TIME_UPDATE_MSC&&passes>1)v++;
 return true;}
bool PositionGetString(int,string &v){v=positions.at(selected).symbol;return true;}
bool PositionGetDouble(int f,double &v){auto &p=positions.at(selected);
 v=f==POSITION_VOLUME?p.qty:f==POSITION_PRICE_OPEN?p.entry:f==POSITION_PRICE_CURRENT?p.price:f==POSITION_PROFIT?p.pnl:p.carry;
 if(scenario=="quantity_changed"&&f==POSITION_VOLUME&&passes>1)v++;
 if(scenario=="pnl_changed"&&f==POSITION_PROFIT&&passes>1)v++;
 return true;}
int StringLen(string s){return s.size();}
long StringToInteger(string s){try{return std::stol(s);}catch(...){return 0;}}
string IntegerToString(long v){return std::to_string(v);}
string DoubleToString(double v,int n){std::ostringstream s;s<<std::fixed<<std::setprecision(n)<<v;return s.str();}
string JsonEscape(string s){return s;}
double MathAbs(double v){return std::abs(v);}
bool MathIsValidNumber(double v){return std::isfinite(v);}
int ArraySize(const std::vector<string> &v){return v.size();}
void ArrayResize(std::vector<string> &v,int n){v.resize(n);}
int StringSplit(string s,char delim,std::vector<string> &v){v.clear();if(s.empty())return 0;
 size_t at=0;while(true){auto next=s.find(delim,at);v.push_back(s.substr(at,next==string::npos?next:next-at));if(next==string::npos)break;at=next+1;}return v.size();}
`;
const cases = String.raw`
void emit(string label,string account="42",string symbol="GENERIC",string ids="101,102"){
 std::cout<<label<<"\t"<<PositionValuationsJson(account,symbol,ids)<<"\n";}
int main(){
 reset("valid");emit("valid");
 reset("valid");positions[0].type=POSITION_TYPE_SELL;positions[0].price=101;emit("short");
 reset("valid");price_currency="USD";emit("fx_missing");
 reset("valid");mode=SYMBOL_CALC_MODE_FUTURES;emit("futures");
 reset("valid");mode=SYMBOL_CALC_MODE_EXCH_BONDS;emit("bonds");
 reset("metadata_missing");emit("metadata_missing");
 reset("valid");multiplier=0;emit("invalid_multiplier");
 reset("valid");g_history_utc_offset_seconds=86400;emit("unknown_clock");
 reset("valid");g_history_clock_last_observed=10000;emit("outside_clock");
 reset("no_quote");emit("no_quote");
 reset("future_quote");emit("future_quote");
 reset("valid");positions[0].price=99;emit("price_not_quote");
 reset("valid");positions.clear();emit("not_open");
 for(auto s:{"account_changed","currency_changed","quote_changed","quantity_changed","pnl_changed","update_changed",
    "disconnected","disconnect_during","oversized","read_failed","field_failed"}){reset(s);emit(s);}
 reset("valid");emit("wrong_account","43");
 reset("valid");emit("wrong_symbol","42","OTHER");
 reset("valid");positions.push_back(P{});positions[1].ticket=902;emit("duplicate_broker_identity");
 reset("valid");emit("duplicate_request","42","GENERIC","101,101");
 reset("valid");emit("noncanonical","42","GENERIC","0101");
 reset("valid");emit("invalid_identifier","42","GENERIC","101oops");
 reset("valid");emit("empty_request","42","GENERIC","");
 reset("valid");positions[0].qty=std::numeric_limits<double>::quiet_NaN();emit("nan_quantity");
 reset("valid");positions[0].pnl=std::numeric_limits<double>::infinity();emit("infinite_pnl");
 reset("valid");string ids="";for(int i=1;i<=32;i++){if(i>1)ids+=",";ids+=std::to_string(i);}emit("bounded_32","42","GENERIC",ids);
 reset("valid");emit("oversized_request","42","GENERIC",ids+",33");
 // Automatic clock windows never require user input or certify old history.
 reset("valid");g_history_utc_offset_seconds=86400;g_history_clock_utc_from=g_history_clock_utc_through=g_history_clock_last_observed=0;
 ObserveExecutionClock();assert(g_history_utc_offset_seconds==3600&&g_history_clock_utc_from==20001&&g_history_clock_utc_through==19999);
 utc_clock+=3;ObserveExecutionClock();assert(g_history_clock_utc_from==20001&&g_history_clock_utc_through==20002);
 server_offset=7200;utc_clock++;ObserveExecutionClock();assert(g_history_utc_offset_seconds==7200&&g_history_clock_utc_from==20005);
 utc_clock+=20;ObserveExecutionClock();assert(g_history_clock_utc_from==20025);
 utc_clock=19000;ObserveExecutionClock();assert(g_history_clock_utc_from==19001);
 server="another-server";ObserveExecutionClock();assert(g_history_clock_server==server);
 utc_clock+=3;ObserveExecutionClock();long last=g_history_clock_last_observed,through=g_history_clock_utc_through;
 tick_lag=60;utc_clock++;ObserveExecutionClock();assert(g_history_clock_last_observed==last&&g_history_clock_utc_through==through);
 tick_lag=0;scenario="disconnected";utc_clock++;ObserveExecutionClock();assert(g_history_clock_last_observed==last);
 scenario="valid";server_offset=3607;ObserveExecutionClock();assert(g_history_clock_last_observed==last);
 server_offset=15*3600;ObserveExecutionClock();assert(g_history_clock_last_observed==last);
}
`;
const dir = mkdtempSync(join(tmpdir(), 'aqe-valuation-'));
try {
  const file = join(dir, 'test.cpp'), binary = join(dir, 'test');
  writeFileSync(file, shim + rounding + clock + body + cases);
  const build = spawnSync('clang++', ['-std=c++17', '-Wall', '-Wextra', file, '-o', binary], { encoding: 'utf8' });
  assert.equal(build.status, 0, build.stderr);
  const run = spawnSync(binary, [], { encoding: 'utf8' });
  assert.equal(run.status, 0, run.stderr);
  const results = Object.fromEntries(run.stdout.trim().split('\n').map(line => {
    const [key, json] = line.split('\t'); return [key, JSON.parse(json)];
  }));
  const open = name => results[name].positions[0];
  assert.equal(open('valid').notional.account_value, 20000);
  assert.equal(open('valid').notional.units_per_quantity, 100);
  assert.equal(open('valid').gross_unrealized_pnl, 2);
  assert.equal(open('valid').carry, -0.2);
  assert.equal(open('valid').quote_time.occurred_at_us, 20_000_000_000);
  assert.equal(open('short').side, 'sell');
  assert.equal(open('short').notional.account_value, 20200);
  assert.equal(open('fx_missing').notional.reason, 'currency_conversion_unavailable');
  for (const name of ['futures', 'bonds']) assert.equal(open(name).notional.reason, 'unsupported_contract');
  for (const name of ['metadata_missing', 'invalid_multiplier']) assert.equal(open(name).notional.reason, 'invalid_contract_metadata');
  for (const name of ['unknown_clock','outside_clock','no_quote','future_quote','price_not_quote']) assert.equal(open(name).quote_time, null, name);
  assert.deepEqual(results.not_open.positions, [{ position_id:'101', status:'not_open' }, { position_id:'102', status:'not_open' }]);
  assert.equal(results.bounded_32.positions.length, 32);
  const valid = new Set(['valid','short','fx_missing','futures','bonds','metadata_missing','invalid_multiplier',
    'unknown_clock','outside_clock','no_quote','future_quote','price_not_quote','not_open','bounded_32']);
  for (const [name,value] of Object.entries(results)) if (!valid.has(name)) assert.equal(value, null, name);
  console.log(`MT5 actual-body valuation regression passed: ${Object.keys(results).length} valuation cases plus automatic-clock transitions; no terminal or trading commands.`);
} finally { rmSync(dir, { recursive:true, force:true }); }
