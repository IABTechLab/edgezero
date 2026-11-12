//! Private sibling-file replacement for operator configuration.

use std::fs::{self, File, OpenOptions, Permissions};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_FILE: AtomicU64 = AtomicU64::new(0);

struct PendingFile(PathBuf);

#[expect(
    clippy::missing_trait_methods,
    reason = "ordinary file cleanup needs only Drop::drop"
)]
impl Drop for PendingFile {
    fn drop(&mut self) {
        drop(fs::remove_file(&self.0));
    }
}

/// Replace a file after its complete contents have been written and synced.
/// `permissions` preserves an existing file's mode; `None` keeps the new file
/// private (0600 on Unix). The caller validates paths and serializes writers.
///
/// # Errors
/// Errors before rename leave the destination unchanged and remove the
/// temporary file. A Unix directory-sync error after rename explicitly reports
/// that replacement is installed but its crash durability is uncertain.
#[inline]
pub fn replace(path: &Path, body: &[u8], permissions: Option<Permissions>) -> io::Result<()> {
    replace_with(path, permissions, |file| file.write_all(body))
}

fn replace_with(
    path: &Path,
    permissions: Option<Permissions>,
    write: impl FnOnce(&mut File) -> io::Result<()>,
) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|dir| !dir.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    #[cfg(unix)]
    let directory = File::open(parent)?;
    replace_with_sync(path, parent, permissions, write, || {
        #[cfg(unix)]
        directory.sync_all()?;
        Ok(())
    })
}

fn replace_with_sync(
    path: &Path,
    parent: &Path,
    permissions: Option<Permissions>,
    write: impl FnOnce(&mut File) -> io::Result<()>,
    sync_parent: impl FnOnce() -> io::Result<()>,
) -> io::Result<()> {
    let (pending, mut file) = create_sibling(parent)?;
    write(&mut file)?;
    if let Some(mode) = permissions {
        file.set_permissions(mode)?;
    }
    file.sync_all()?;
    drop(file);
    fs::rename(&pending.0, path)?;
    sync_parent().map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "replacement installed at {} but syncing parent directory failed: {error}",
                path.display()
            ),
        )
    })
}

fn create_sibling(parent: &Path) -> io::Result<(PendingFile, File)> {
    for _ in 0_u8..100 {
        let nonce = NEXT_FILE.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!(".edgezero-write-{}-{nonce}", process::id()));
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600)
        };
        match options.open(&path) {
            Ok(file) => return Ok((PendingFile(path), file)),
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {}
            Err(err) => return Err(err),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "cannot create an unused configuration staging file",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn directory_sync_failure_reports_the_already_installed_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("operator.env");
        fs::write(&path, "original").unwrap();
        let error = replace_with_sync(
            &path,
            dir.path(),
            None,
            |file| file.write_all(b"replacement"),
            || {
                assert_eq!(fs::read_to_string(&path).unwrap(), "replacement");
                Err(io::Error::other("injected directory sync failure"))
            },
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert!(error.to_string().contains("replacement installed"));
        assert!(
            error
                .to_string()
                .contains("syncing parent directory failed")
        );
        assert_eq!(fs::read_to_string(path).unwrap(), "replacement");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn replacement_checks_parent_sync_capability_before_installing() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("operator.env");
        fs::write(&path, "original").unwrap();
        fs::set_permissions(dir.path(), Permissions::from_mode(0o300)).unwrap();
        let result = replace(&path, b"replacement", None);
        fs::set_permissions(dir.path(), Permissions::from_mode(0o755)).unwrap();
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(fs::read_to_string(path).unwrap(), "original");
    }

    #[test]
    fn partial_write_failure_preserves_original_and_removes_temporary_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("operator.env");
        fs::write(&path, "original-secret").unwrap();
        let error = replace_with(&path, None, |file| {
            file.write_all(b"partial replacement")?;
            Err(io::Error::other("injected write failure"))
        })
        .unwrap_err();
        assert_eq!(error.to_string(), "injected write failure");
        assert_eq!(fs::read_to_string(&path).unwrap(), "original-secret");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }
}
