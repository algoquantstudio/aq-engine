//! Portable, collision-resistant filenames for one strategy/session tuple.
use std::path::{Path, PathBuf};

pub(super) fn scope_stem(domain: &[u8], prefix: &str, strategy: &str, session: &str) -> String {
    use sha2::{Digest, Sha256};
    let ordinary = format!("{strategy}--{session}");
    // Lowercase avoids aliases on case-insensitive filesystems. Hyphens next to
    // the separator and embedded separators would make tuple boundaries ambiguous.
    if ordinary.len() <= 200
        && [strategy, session].iter().all(|value| {
            !value.is_empty()
                && !value.starts_with('-')
                && !value.ends_with('-')
                && !value.contains("--")
                && value.bytes().all(|b| {
                    b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'-' | b'_')
                })
        })
    {
        return ordinary;
    }
    let mut hash = Sha256::new();
    hash.update(domain);
    for value in [strategy, session] {
        hash.update((value.len() as u64).to_le_bytes());
        hash.update(value.as_bytes());
    }
    // Ordinary names always contain `--`; digest names never do.
    format!("{prefix}-{}", hex::encode(hash.finalize()))
}

/// Separate publisher recovery storage from the receiver's own outbox. Ordinary
/// generated IDs retain their old names; opaque IDs never become path fragments.
pub fn publisher_outbox_path(receiver: &Path, strategy: &str, session: &str) -> PathBuf {
    let scope = scope_stem(
        b"aqs-publisher-outbox-file-v1\0",
        "scope",
        strategy,
        session,
    );
    let mut name = receiver.as_os_str().to_os_string();
    name.push(format!(".publisher-{scope}.db"));
    PathBuf::from(name)
}

/// Discovery only, never use this lossy name for new writers. A retained legacy
/// file must pass leased, whole-file scope validation before recovery can adopt it.
pub fn legacy_publisher_outbox_path(
    receiver: &Path,
    strategy: &str,
    session: &str,
) -> Option<PathBuf> {
    let scope = format!("{strategy}--{session}")
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect::<String>();
    let mut name = receiver.as_os_str().to_os_string();
    name.push(format!(".publisher-{scope}.db"));
    let path = PathBuf::from(name);
    // The old sanitizer produced an ASCII suffix. Oversized components could
    // never have been written on supported filesystems and must not cause an
    // endless ENAMETOOLONG recovery loop.
    (path.file_name()?.as_encoded_bytes().len() <= 255).then_some(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publisher_names_preserve_generated_ids_and_distinguish_opaque_tuples() {
        let receiver = Path::new("runtime/sync-agent.db");
        assert_eq!(
            publisher_outbox_path(receiver, "strategy-a", "session-b"),
            Path::new("runtime/sync-agent.db.publisher-strategy-a--session-b.db")
        );
        let mut portable_names = std::collections::HashSet::new();
        for (strategy, session) in [
            ("a/b", "c"),
            ("a_b", "c"),
            ("a:b", "c"),
            ("a--b", "c"),
            ("a", "b--c"),
            ("a-", "b"),
            ("a", "-b"),
            ("Case", "session"),
            ("case", "session"),
            ("", "session"),
            ("../outside", "session"),
            ("a\0b", "c"),
            ("a", "b\0c"),
            ("東京", "session"),
            (&"x".repeat(512), "session"),
        ] {
            let path = publisher_outbox_path(receiver, strategy, session);
            assert_eq!(path.parent(), receiver.parent());
            let name = path.file_name().unwrap().to_str().unwrap();
            assert!(name.len() < 240 && !name.contains('\0'));
            assert!(portable_names.insert(name.to_ascii_lowercase()));
        }
        assert_eq!(
            legacy_publisher_outbox_path(receiver, "a/b", "c"),
            legacy_publisher_outbox_path(receiver, "a:b", "c")
        );
        assert!(legacy_publisher_outbox_path(receiver, &"x".repeat(512), "session").is_none());
    }
}
