use super::{LaunchConfig, MAX_BYTES};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::Path,
};

pub fn write_private(path: &Path, config: &LaunchConfig) -> Result<(), String> {
    config.validate()?;
    let bytes = serde_json::to_vec(config).map_err(|_| "Cannot encode AQE launch configuration")?;
    if bytes.len() > MAX_BYTES {
        return Err("AQE launch configuration is too large".into());
    }
    let parent = path
        .parent()
        .ok_or("AQE launch configuration needs a private directory")?;
    fs::create_dir_all(parent).map_err(|_| "Cannot create AQE launch directory")?;
    if fs::symlink_metadata(parent)
        .map_err(|_| "Cannot inspect AQE launch directory")?
        .file_type()
        .is_symlink()
    {
        return Err("AQE launch directory must not be a symlink".into());
    }
    protect(parent, true)?;
    // Unique immutable filenames avoid overwriting a running process's config.
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .map_err(|_| "Cannot create private AQE launch configuration")?;
    let result = (|| {
        protect(path, false)?;
        file.write_all(&bytes)
            .and_then(|_| file.sync_all())
            .map_err(|_| "Cannot save AQE launch configuration".to_string())
    })();
    if result.is_err() {
        drop(file);
        let _ = fs::remove_file(path);
    }
    result
}

pub fn read_private(path: &Path) -> Result<LaunchConfig, String> {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|_| "Cannot resolve AQE launch path")?
            .join(path)
    };
    let before = fs::symlink_metadata(&path).map_err(
        |_| "Cannot read AQE launch configuration; rebuild/copy the command with current AQS",
    )?;
    if !before.is_file() || before.file_type().is_symlink() || before.len() > MAX_BYTES as u64 {
        return Err("AQE launch configuration must be a bounded regular private file".into());
    }
    check_private(&path, &before)?;
    let parent = path.parent().ok_or("AQE launch directory is missing")?;
    let parent_meta =
        fs::symlink_metadata(parent).map_err(|_| "Cannot inspect AQE launch directory")?;
    if !parent_meta.is_dir() || parent_meta.file_type().is_symlink() {
        return Err("AQE launch directory must be private and not a symlink".into());
    }
    check_private(parent, &parent_meta)?;
    let file = File::open(&path).map_err(|_| "Cannot open AQE launch configuration")?;
    let opened = file
        .metadata()
        .map_err(|_| "Cannot inspect AQE launch configuration")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if before.dev() != opened.dev() || before.ino() != opened.ino() {
            return Err("AQE launch configuration changed while opening".into());
        }
    }
    check_private(&path, &opened)?;
    let mut bytes = Vec::new();
    file.take(MAX_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "Cannot read AQE launch configuration")?;
    if bytes.len() > MAX_BYTES {
        return Err("AQE launch configuration is too large".into());
    }
    let config: LaunchConfig = serde_json::from_slice(&bytes)
        .map_err(|_| "Invalid AQE launch configuration JSON (contents redacted)")?;
    config.validate()?;
    Ok(config)
}

fn protect(path: &Path, directory: bool) -> Result<(), String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(
            path,
            fs::Permissions::from_mode(if directory { 0o700 } else { 0o600 }),
        )
        .map_err(|_| "Cannot restrict AQE launch permissions")?;
    }
    #[cfg(windows)]
    windows_acl(path, directory, false)?;
    Ok(())
}
fn check_private(path: &Path, metadata: &fs::Metadata) -> Result<(), String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err("AQE launch file/directory is accessible to other users; restrict permissions before launching".into());
        }
    }
    #[cfg(windows)]
    windows_acl(path, metadata.is_dir(), true)?;
    let _ = (path, metadata);
    Ok(())
}

#[cfg(windows)]
fn windows_acl(path: &Path, directory: bool, check: bool) -> Result<(), String> {
    use std::os::windows::process::CommandExt;
    // Use the current SID, not an unqualified USERNAME. Only the file path is
    // passed to PowerShell; config contents and credentials never enter argv.
    let script = r#"param($Path,$Directory,$Check)
$ErrorActionPreference='Stop'
$sid=[System.Security.Principal.WindowsIdentity]::GetCurrent().User
if ($Check -eq 'true') {
  $acl=Get-Acl -LiteralPath $Path
  if ($acl.GetOwner([System.Security.Principal.SecurityIdentifier]).Value -ne $sid.Value) { exit 3 }
  foreach ($rule in $acl.GetAccessRules($true,$true,[System.Security.Principal.SecurityIdentifier])) {
    if ($rule.AccessControlType -eq 'Allow' -and $rule.IdentityReference.Value -ne $sid.Value) { exit 4 }
  }
} else {
  if ($Directory -eq 'true') {
    $acl=New-Object System.Security.AccessControl.DirectorySecurity
    $rule=New-Object System.Security.AccessControl.FileSystemAccessRule($sid,'FullControl','ContainerInherit,ObjectInherit','None','Allow')
  } else {
    $acl=New-Object System.Security.AccessControl.FileSecurity
    $rule=New-Object System.Security.AccessControl.FileSystemAccessRule($sid,'FullControl','Allow')
  }
  $acl.SetOwner($sid); $acl.SetAccessRuleProtection($true,$false); $acl.AddAccessRule($rule)
  Set-Acl -LiteralPath $Path -AclObject $acl
}
"#;
    // -Command with a scriptblock and separately quoted path avoids a script
    // file whose ACL could differ. Escape PowerShell single quotes in the path.
    let command = format!(
        "& {{ {script} }} '{}' '{}' '{}'",
        path.to_string_lossy().replace('\'', "''"),
        directory,
        check
    );
    let status = std::process::Command::new("powershell.exe")
        .args([
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            &command,
        ])
        .creation_flags(0x08000000)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map_err(|_| "Cannot validate/restrict Windows AQE launch ACL")?;
    if status.success() {
        Ok(())
    } else {
        Err("Windows AQE launch file/directory must be accessible only to its owner".into())
    }
}
