pub mod builtins;
#[cfg(not(target_arch = "wasm32"))]
pub mod codegen;

pub mod config;
pub mod models;
pub mod types;

// Re-export everything for easy access backwards compatibility
pub use config::*;
pub use models::*;
pub use types::*;

// Expose BacktestMetrics for Tauri usage when the runtime is enabled.
#[cfg(all(feature = "runtime", not(target_arch = "wasm32")))]
pub use crate::core::broker::backtest_state::BacktestMetrics;
