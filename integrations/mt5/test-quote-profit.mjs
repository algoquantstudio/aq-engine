// Execute the actual read-only MQL calibration function in a terminal shim.
// This checks source semantics; it does not replace a MetaEditor compilation.
import assert from 'node:assert/strict';
import {readFileSync,mkdtempSync,writeFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {spawnSync} from 'node:child_process';
const source=readFileSync(new URL('./AqeMt5BridgeEA.mq5',import.meta.url),'utf8');
const start=source.indexOf('double QuoteProfitCoefficient(');
const body=source.slice(start,source.indexOf('string QuoteJson(string symbol)',start));
assert(start>=0);
assert.doesNotMatch(body,/\b(?:OrderSend|OrderSendAsync|PositionClose|PositionModify|HistorySelect|SymbolSelect)\b/);
assert.equal((body.match(/OrderCalcProfit\(/g)||[]).length,4);
const shim=String.raw`
#include <string>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <cmath>
#include <algorithm>
using std::string;
enum {SYMBOL_TRADE_CALC_MODE,TERMINAL_CONNECTED,SYMBOL_VOLUME_MIN,SYMBOL_TRADE_TICK_SIZE,ACCOUNT_LOGIN,ACCOUNT_CURRENCY,SYMBOL_DIGITS,
 SYMBOL_VOLUME_MAX,SYMBOL_TRADE_TICK_VALUE_PROFIT,SYMBOL_TRADE_TICK_VALUE_LOSS,ACCOUNT_CURRENCY_DIGITS};
enum {SYMBOL_CALC_MODE_FOREX,SYMBOL_CALC_MODE_FOREX_NO_LEVERAGE,SYMBOL_CALC_MODE_CFD,SYMBOL_CALC_MODE_CFDINDEX,
 SYMBOL_CALC_MODE_CFDLEVERAGE,SYMBOL_CALC_MODE_EXCH_STOCKS,SYMBOL_CALC_MODE_EXCH_STOCKS_MOEX,
 SYMBOL_CALC_MODE_FUTURES,SYMBOL_CALC_MODE_EXCH_FUTURES,SYMBOL_CALC_MODE_EXCH_FUTURES_FORTS,SYMBOL_CALC_MODE_EXCH_BONDS};
enum {ORDER_TYPE_BUY,ORDER_TYPE_SELL};
struct MqlTick {long time_msc=100000;double bid=100,ask=101,last=0;};
int mode=SYMBOL_CALC_MODE_CFD,calls=0,account_reads=0,currency_reads=0;string scenario="valid",currency="GBP";
long SymbolInfoInteger(string,int f){return f==SYMBOL_DIGITS?2:mode;}
long TerminalInfoInteger(int){return scenario!="disconnected";}
double coefficient(bool loss){return (mode==SYMBOL_CALC_MODE_FUTURES?200.:10.)*(currency=="GBP"?(scenario=="rounded"?.7538721:(loss?.76:.75)):1.);}
double SymbolInfoDouble(string,int f){
 if(f==SYMBOL_VOLUME_MIN)return scenario=="bad_quantity"?0.:0.01;
 if(f==SYMBOL_VOLUME_MAX)return 10.;
 if(f==SYMBOL_TRADE_TICK_VALUE_PROFIT || f==SYMBOL_TRADE_TICK_VALUE_LOSS)return scenario=="bad_tick_value"?99.:.01*coefficient(f==SYMBOL_TRADE_TICK_VALUE_LOSS);
 return .01;
}
long AccountInfoInteger(int f){if(f==ACCOUNT_CURRENCY_DIGITS)return 2;return scenario=="account_changed" && ++account_reads>1?43:42;}
string AccountInfoString(int){return scenario=="currency_changed" && ++currency_reads>1?"USD":currency;}
long TimeGMT(){return 100;}
long TimeCurrent(){return 100;}
string IsoTimeMillis(long){return "1970-01-01T00:01:40Z";}
double MathMax(double a,double b){return std::max(a,b);}
double MathMin(double a,double b){return std::min(a,b);}
double MathAbs(double v){return std::abs(v);}
double MathPow(double v,double power){return std::pow(v,power);}
bool MathIsValidNumber(double v){return std::isfinite(v);}
string IntegerToString(long v){return std::to_string(v);}
string DoubleToString(double v,int n){std::ostringstream s;s<<std::fixed<<std::setprecision(n)<<v;return s.str();}
string JsonEscape(string s){return s;}
bool SymbolInfoTick(string,MqlTick &q){q=MqlTick{};if(scenario=="changed_tick")q.bid+=1;return true;}
bool OrderCalcProfit(int side,string,double qty,double entry,double exit,double &profit){
 ++calls;if(scenario=="calc_failed")return false;
 double delta=side==ORDER_TYPE_BUY?exit-entry:entry-exit;
 profit=scenario=="bad_coefficient"?0.:delta*qty*coefficient(side==ORDER_TYPE_BUY);
 if(scenario=="rounded")profit=std::round(profit*100.)/100.;
 return true;
}
`;
const cases=String.raw`
void emit(string label){MqlTick q;calls=account_reads=currency_reads=0;std::cout<<label<<"\t"<<QuoteProfitModelJson("X",q)<<"\t"<<calls<<"\n";}
int main(){
 emit("GBP-CFD");currency="USD";emit("USD-CFD");currency="GBP";mode=SYMBOL_CALC_MODE_FUTURES;emit("GBP-Futures");
 mode=SYMBOL_CALC_MODE_EXCH_BONDS;emit("unsupported");mode=SYMBOL_CALC_MODE_CFD;
 for(auto s:{"disconnected","calc_failed","bad_coefficient","changed_tick","bad_quantity","account_changed","currency_changed","bad_tick_value","rounded"}){scenario=s;emit(s);}
 scenario="valid";calls=account_reads=currency_reads=0;std::cout<<"full-quote\t"<<QuoteJsonWithOffset("X",0)<<"\t"<<calls<<"\n";
}
`;
const dir=mkdtempSync(join(tmpdir(),'aqs-quote-profit-'));
try {
 const cpp=join(dir,'test.cpp'),bin=join(dir,'test');writeFileSync(cpp,shim+body+cases);
 const compile=spawnSync('c++',['-std=c++17',cpp,'-o',bin],{encoding:'utf8'});
 assert.equal(compile.status,0,compile.stderr);
 const run=spawnSync(bin,[],{encoding:'utf8'});assert.equal(run.status,0,run.stderr);
 const rows=new Map(run.stdout.trim().split('\n').map(line=>{const [label,json,calls]=line.split('\t');return [label,{model:JSON.parse(json),calls:Number(calls)}];}));
 for(const [label,coeffs] of [['GBP-CFD',[7.6,7.6,7.5,7.5]],['USD-CFD',[10,10,10,10]],['GBP-Futures',[152,152,150,150]],['rounded',[7.538721,7.538721,7.538721,7.538721]]]) {
  const {model,calls}=rows.get(label);assert.equal(calls,4);assert.equal(model.account_id,'42');assert.equal(model.symbol,'X');
  for(const [key,i] of ['buy_gain','buy_loss','sell_gain','sell_loss'].map((key,i)=>[key,i])) assert(Math.abs(model[key]-coeffs[i])<1e-8);
 }
 for(const label of ['unsupported','disconnected','calc_failed','bad_coefficient','changed_tick','bad_quantity','account_changed','currency_changed','bad_tick_value']) assert.equal(rows.get(label).model,null,label);
 assert.equal(rows.get('unsupported').calls,0);assert.equal(rows.get('disconnected').calls,0);
 assert.equal(rows.get('full-quote').model.profit_model.currency,'GBP');
 assert.equal(rows.get('full-quote').model.bid,100);
 console.log('MQL quote calibration: native contract/FX coefficients, buy/sell gain/loss, bounded reads, unsupported/error/racing-tick fallbacks passed. MetaEditor/live EA parity still requires the new EA build.');
} finally {rmSync(dir,{recursive:true,force:true});}
