use super::*;
use crate::{ExecutionFillPage, PositionMarkPage};
use serde_json::{Value, json};

fn fills(count: u64) -> Value {
    // Exercise exact sequences above JavaScript's integer precision boundary.
    let after = 9_007_199_254_740_992u64;
    let page: ExecutionFillPage = serde_json::from_value(json!({
        "version":1,"source_id":"source","strategy_id":"strategy","account_id":"account",
        "after_sequence":after,"through_sequence":after+count,"next_sequence":after+count,
        "has_more":false,"generated_at_us":200,"coverage":"observed","valuation_basis":"broker",
        "records":(1..=count).map(|n|json!({"sequence":after+n,"execution_epoch":1,"recorded_at_us":200,
            "observation":{"insight_id":format!("insight-{n}"),"fill":{"version":1,"provider":"deals",
                "fill_id":format!("deal-{n}"),"order_id":format!("order-{n}"),"position_id":null,
                "symbol":"ANY","side":"buy","effect":"entry","quantity":2.0,"price":-10.0,
                "occurred_at_us":100,"as_of_us":200,"cash":{"currency":"GBP","gross_pnl":0.0,
                "commission":1.0,"carry":-0.25},"account_notional":20.0}}
        })).collect::<Vec<_>>()
    })).unwrap();
    serde_json::to_value(ExecutionFillTransfer::from_page(&page).unwrap()).unwrap()
}

fn marks(count: u64) -> Value {
    let after = 9_007_199_254_740_992u64;
    let page: PositionMarkPage = serde_json::from_value(json!({
        "version":1,"source_id":"source","strategy_id":"strategy","account_id":"account",
        "after_sequence":after,"through_sequence":after+count,"next_sequence":after+count,
        "has_more":false,"generated_at_us":120_000_002,
        "records":(1..=count).map(|n|json!({"sequence":after+n,"execution_epoch":4,"recorded_at_us":120_000_001,
            "observation":{"insight_id":format!("insight-{n}"),"fill_provider":"fills","ledger_sequence":8,
                "sample_minute_us":120_000_000,"values":{"version":1,"provider":"marks","account_id":"account",
                    "account_currency":"GBP","symbol":"ANY","as_of_us":120_000_000,
                    "positions":[{"position_id":format!("position-{n}"),"status":"not_open"}]}}
        })).collect::<Vec<_>>()
    })).unwrap();
    serde_json::to_value(PositionMarkTransfer::from_page(&page).unwrap()).unwrap()
}

fn owned_fills(value: &Value) -> Result<ExecutionFillPage, String> {
    serde_json::from_value::<ExecutionFillTransfer>(value.clone())
        .map_err(|_| "invalid execution fill payload".to_owned())?.page()
}
fn owned_marks(value: &Value) -> Result<PositionMarkPage, String> {
    serde_json::from_value::<PositionMarkTransfer>(value.clone())
        .map_err(|_| "invalid position mark payload".to_owned())?.page()
}

#[test]
fn borrowed_transfers_preserve_complete_pages_errors_and_exact_large_sequences() {
    for (value, is_mark) in [(fills(1),false),(fills(32),false),(marks(1),true),(marks(32),true)] {
        let check = |v: &Value| {
            if is_mark {
                assert_eq!(PositionMarkTransfer::page_from_value(v), owned_marks(v));
            } else {
                assert_eq!(ExecutionFillTransfer::page_from_value(v), owned_fills(v));
            }
        };
        check(&value);
        let envelope = Envelope::deserialize(&value).unwrap();
        assert!(envelope.next_sequence > 9_007_199_254_740_992);
        assert_eq!(envelope.body.as_ptr(), value["body"].as_str().unwrap().as_ptr());
        for field in ["schema_version","source_id","strategy_id","account_id","after_sequence","next_sequence","transfer_id","body"] {
            let mut missing = value.clone(); missing.as_object_mut().unwrap().remove(field); check(&missing);
            for bad in [Value::Null,json!([]),json!({}),json!(false),json!(-1),json!(1.5),json!("1")] {
                let mut malformed=value.clone();malformed[field]=bad;check(&malformed);
            }
        }
        for (field,bad) in [("schema_version",json!(65536)),("source_id",json!("foreign")),
            ("strategy_id",json!("foreign")),("account_id",json!("foreign")),
            ("next_sequence",json!(9_007_199_254_741_100u64)),("transfer_id",json!("bad")),
            ("body",json!("")),("body",json!("x".repeat(32*1024+1))),
            ("body",json!(value["body"].as_str().unwrap().to_owned()+" "))] {
            let mut malformed=value.clone();malformed[field]=bad;check(&malformed);
            assert!(if is_mark {PositionMarkTransfer::page_from_value(&malformed).is_err()}
                else {ExecutionFillTransfer::page_from_value(&malformed).is_err()});
        }
        // A valid checksum never substitutes for semantic page validation.
        for invalidation in 0..4 {
            let mut body:Value=serde_json::from_str(value["body"].as_str().unwrap()).unwrap();
            match invalidation {
                0 => body["records"][0]["sequence"]=json!(0),
                1 => body["records"][0]["recorded_at_us"]=json!(-1),
                2 => body["account_id"]=json!("foreign"),
                _ => body["records"]=json!([]),
            }
            let body=serde_json::to_string(&body).unwrap();
            let mut malformed=value.clone();
            malformed["transfer_id"]=json!(crate::execution_evidence_transfer::checksum(body.as_bytes()));
            malformed["body"]=json!(body);check(&malformed);
            assert!(if is_mark {PositionMarkTransfer::page_from_value(&malformed).is_err()}
                else {ExecutionFillTransfer::page_from_value(&malformed).is_err()});
        }
        // Unknown extensions and Unicode/escaped envelope strings remain the
        // same contract as the owned decoder. No payload rewriting occurs.
        let mut extended=value.clone();extended["extension"]=json!("العربية 中文 한국어 \\\"");check(&extended);
        assert_eq!(value["body"], json!(envelope.body));
    }
}

// Passive, calling-thread allocation traffic only. Never enabled in the
// protocol dependency used by the gateway or in release builds.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
thread_local! {
    static ACTIVE: Cell<bool> = const { Cell::new(false) };
    static CALLS: Cell<u64> = const { Cell::new(0) };
    static BYTES: Cell<u64> = const { Cell::new(0) };
}
struct Allocator;
#[global_allocator]
static ALLOCATOR: Allocator = Allocator;
fn record(bytes: usize) {
    let _ = ACTIVE.try_with(|active| if active.get() {
        let _=CALLS.try_with(|n|n.set(n.get()+1));
        let _=BYTES.try_with(|n|n.set(n.get()+bytes as u64));
    });
}
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {let p=unsafe{System.alloc(layout)};if !p.is_null(){record(layout.size());}p}
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {let p=unsafe{System.alloc_zeroed(layout)};if !p.is_null(){record(layout.size());}p}
    unsafe fn realloc(&self, p:*mut u8, layout:Layout, size:usize)->*mut u8 {let p=unsafe{System.realloc(p,layout,size)};if !p.is_null(){record(size);}p}
    unsafe fn dealloc(&self, p:*mut u8, layout:Layout){unsafe{System.dealloc(p,layout)};}
}

#[test]
#[ignore = "native transfer-validation allocation profile; no broker, DB or network"]
fn transfer_validation_allocation_profile() {
    for count in [1,32] {
        for (value, is_mark) in [(fills(count),false),(marks(count),true)] {
            let mut results=Vec::new();
            for borrowed in [false,true,true,false] {
                let decode=|| {
                    if is_mark {std::hint::black_box(if borrowed {PositionMarkTransfer::page_from_value(&value)} else {owned_marks(&value)}).unwrap().next_sequence}
                    else {std::hint::black_box(if borrowed {ExecutionFillTransfer::page_from_value(&value)} else {owned_fills(&value)}).unwrap().next_sequence}
                };
                for _ in 0..4 {decode();}
                let mut times=Vec::new();let mut calls=0;let mut bytes=0;
                for _ in 0..20 {
                    CALLS.with(|n|n.set(0));BYTES.with(|n|n.set(0));ACTIVE.with(|n|assert!(!n.replace(true)));
                    let started=std::time::Instant::now();
                    for _ in 0..50 {assert_eq!(decode(),9_007_199_254_740_992+count);}
                    let elapsed=started.elapsed().as_nanos();ACTIVE.with(|n|n.set(false));
                    times.push(elapsed);calls+=CALLS.with(Cell::get);bytes+=BYTES.with(Cell::get);
                }
                times.sort_unstable();results.push(json!({"borrowed":borrowed,"samples":20,"decodes_per_sample":50,
                    "mean_allocation_calls_per_decode":calls/1000,"mean_requested_bytes_per_decode":bytes/1000,
                    "p50_ns_per_decode":times[10]/50,"p95_ns_per_decode":times[18]/50}));
            }
            println!("TRANSFER_VALIDATION_PROFILE {}",json!({"kind":if is_mark{"marks"}else{"fills"},"rows":count,
                "body_bytes":value["body"].as_str().unwrap().len(),"order":"ABBA owned/borrowed/borrowed/owned","phases":results,
                "scope":"native unoptimized complete page validation; cumulative allocation traffic, not live heap or full gateway RSS/latency"}));
        }
    }
}
