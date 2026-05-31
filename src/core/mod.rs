#[cfg(not(target_arch = "wasm32"))]
pub mod alpha;
#[cfg(all(not(target_arch = "wasm32"), feature = "runtime"))]
pub mod backtest_storage;
#[cfg(not(target_arch = "wasm32"))]
pub mod broker;
#[cfg(not(target_arch = "wasm32"))]
pub mod indicators;
pub mod insight;
#[cfg(not(target_arch = "wasm32"))]
pub mod lifecycle;
#[cfg(not(target_arch = "wasm32"))]
pub mod pipeline;
#[cfg(not(target_arch = "wasm32"))]
mod portfolio;
#[cfg(not(target_arch = "wasm32"))]
mod risk;
#[cfg(not(target_arch = "wasm32"))]
pub mod strategy;
#[cfg(not(target_arch = "wasm32"))]
pub mod universe;
pub mod utils;
