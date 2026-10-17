use super::{ARGUMENT, LaunchConfig, read_private};
use std::sync::OnceLock;

pub fn from_process() -> Result<Option<&'static LaunchConfig>, String> {
    static CONFIG: OnceLock<Result<Option<LaunchConfig>, String>> = OnceLock::new();
    match CONFIG.get_or_init(|| from_args(std::env::args())) {
        Ok(value) => Ok(value.as_ref()),
        Err(error) => Err(error.clone()),
    }
}

pub(super) fn from_args(
    args: impl IntoIterator<Item = String>,
) -> Result<Option<LaunchConfig>, String> {
    let args = args.into_iter().collect::<Vec<_>>();
    if args.iter().any(|a| a.starts_with("--launch-config=")) {
        return Err("Use --launch-config followed by one private file path".into());
    }
    let positions = args
        .iter()
        .enumerate()
        .filter(|(_, arg)| arg.as_str() == ARGUMENT)
        .map(|(i, _)| i)
        .collect::<Vec<_>>();
    if positions.is_empty() {
        return Ok(None);
    }
    if positions.len() != 1 {
        return Err("Select exactly one AQE launch configuration".into());
    }
    let path = args
        .get(positions[0] + 1)
        .filter(|p| !p.is_empty() && !p.starts_with("--"))
        .ok_or("AQE launch configuration path is missing")?;
    // Credentials/identity come from one source; never silently mix a stale
    // file with different command-line credentials. Old CLI-only runs still work.
    if args.iter().any(|a| {
        [
            "--aqs-url",
            "--session-secret",
            "--strategy-id",
            "--user-id",
            "--session-id",
            "--live-session-id",
            "--node-id",
            "--access-method",
        ]
        .iter()
        .any(|flag| a == flag || a.starts_with(&format!("{flag}=")))
    }) {
        return Err(
            "Do not combine AQE launch configuration with cloud credential arguments".into(),
        );
    }
    read_private(std::path::Path::new(path)).map(Some)
}

pub fn setting(name: &str) -> Result<Option<String>, String> {
    Ok(from_process()?
        .and_then(|config| config.setting(name))
        .or_else(|| std::env::var(name).ok()))
}
