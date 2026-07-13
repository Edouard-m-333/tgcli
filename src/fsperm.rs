//! Best-effort permission hardening for the store directory and its sensitive
//! files.
//!
//! The store holds the raw MTProto auth key (possession = full account access,
//! no 2FA re-challenge) and the entire message archive, including contact phone
//! numbers. Files and directories are created through the process umask, which
//! on a typical system yields world-readable `0755` directories and `0644`
//! files — any other local user could copy the session or read the database.
//! We therefore tighten permissions explicitly right after creation.
//!
//! These are best-effort: a failure to chmod (exotic filesystem, already-gone
//! path) logs a warning rather than aborting the command. On non-Unix targets
//! the functions are no-ops.

use std::path::Path;

/// Restrict a directory to owner-only access (`0700`) on Unix.
pub fn harden_dir<P: AsRef<Path>>(path: P) {
    set_mode(path.as_ref(), 0o700, "directory");
}

/// Restrict a file to owner-only read/write (`0600`) on Unix.
pub fn harden_file<P: AsRef<Path>>(path: P) {
    set_mode(path.as_ref(), 0o600, "file");
}

#[cfg(unix)]
fn set_mode(path: &Path, mode: u32, kind: &str) {
    use std::os::unix::fs::PermissionsExt;

    match std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)) {
        Ok(()) => {}
        // The file simply may not exist yet (e.g. a WAL sidecar before first
        // write); nothing to harden.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => log::warn!(
            "could not restrict permissions on {} '{}': {}",
            kind,
            path.display(),
            e
        ),
    }
}

#[cfg(not(unix))]
fn set_mode(_path: &Path, _mode: u32, _kind: &str) {}
