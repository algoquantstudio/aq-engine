#[cfg(feature = "runtime")]
pub mod core;

#[cfg(feature = "node")]
pub mod node;

/// Common imports for an AQE strategy entrypoint.
///
/// This intentionally exports only the types generated for every strategy. Built-in alpha
/// models and insight pipes remain explicit codegen imports so a strategy only imports the
/// components it actually registers.
#[cfg(feature = "runtime")]
pub mod prelude;
