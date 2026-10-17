#[cfg(feature = "runtime")]
pub mod core;

#[cfg(feature = "node")]
pub mod node;

/// Versioned, broker-neutral launch configuration. Standalone CLI/environment
/// inputs remain supported when no protected launch file is selected.
pub mod launch_config;

/// Common imports for an AQE strategy entrypoint.
///
/// This intentionally exports only the types generated for every strategy. Built-in alpha
/// models and insight pipes remain explicit codegen imports so a strategy only imports the
/// components it actually registers.
#[cfg(feature = "runtime")]
pub mod prelude;
