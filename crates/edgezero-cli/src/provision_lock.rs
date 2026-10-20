//! Cross-process serialization for leaf provision/config write transactions.
//!
//! Deploy shells hold no project lock. Every Rust-owned write transaction
//! acquires the same real OS lock, with no inherited lock advertisement.
use crate::path_safety::reject_symlink_components;
use fs4::fs_std::FileExt as _;
use std::fs::{self, File, OpenOptions};
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;

#[must_use = "bind the guard for the complete write transaction"]
pub(crate) struct ProvisionLock {
    _file: File,
    #[cfg(test)]
    path: PathBuf,
}

impl ProvisionLock {
    pub(crate) fn acquire(root: &Path) -> Result<Self, String> {
        let path = root.join(".edgezero/provision.lock");
        reject_symlink_components(root, &path, "the provision lock")?;
        fs::create_dir_all(root.join(".edgezero"))
            .map_err(|err| format!("create lock directory: {err}"))?;
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .map_err(|err| format!("open {}: {err}", path.display()))?;
        file.lock_exclusive()
            .map_err(|err| format!("lock {}: {err}", path.display()))?;
        Ok(Self {
            _file: file,
            #[cfg(test)]
            path,
        })
    }

    #[cfg(test)]
    fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsStr;
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    #[test]
    fn acquire_creates_lockfile_under_dot_edgezero_dir() {
        let temp = TempDir::new().expect("tempdir");
        let lock = ProvisionLock::acquire(temp.path()).expect("acquire");
        assert!(
            lock.path().exists(),
            "lockfile must be created: {}",
            lock.path().display()
        );
        assert_eq!(
            lock.path().file_name().and_then(|name| name.to_str()),
            Some("provision.lock")
        );
        assert!(
            lock.path().parent().and_then(Path::file_name) == Some(OsStr::new(".edgezero")),
            "lockfile must sit inside .edgezero/: {}",
            lock.path().display()
        );
    }

    /// Regression: `.edgezero/`
    /// is gitignored, so a symlinked one never shows up in review.
    /// `create_dir_all` + `OpenOptions::create` would follow it and we
    /// would hold a writable descriptor outside the project tree.
    #[cfg(unix)]
    #[test]
    fn acquire_refuses_a_symlinked_dot_edgezero_dir() {
        use std::fs::create_dir_all;
        use std::os::unix::fs::symlink;
        let temp = TempDir::new().expect("tempdir");
        let root = temp.path().join("project");
        let outside = temp.path().join("outside");
        create_dir_all(&root).expect("mkdir project");
        create_dir_all(&outside).expect("mkdir outside");
        symlink(&outside, root.join(".edgezero")).expect("symlink");

        // `expect_err` would need `ProvisionLock: Debug`; the guard
        // wraps a live descriptor and has no reason to derive it.
        let Err(err) = ProvisionLock::acquire(&root) else {
            panic!("symlinked .edgezero must be refused")
        };
        assert!(err.contains("symlink"), "{err}");
        assert!(
            !outside.join("provision.lock").exists(),
            "the refused acquire must not have created a lockfile outside the project"
        );
    }

    /// The lockfile itself is the other half: a symlinked
    /// `provision.lock` inside a legitimate `.edgezero/` would have
    /// `OpenOptions::create(true).write(true)` create/open the target.
    #[cfg(unix)]
    #[test]
    fn acquire_refuses_a_symlinked_lockfile() {
        use std::fs::create_dir_all;
        use std::os::unix::fs::symlink;
        let temp = TempDir::new().expect("tempdir");
        let root = temp.path().join("project");
        create_dir_all(root.join(".edgezero")).expect("mkdir .edgezero");
        let victim = temp.path().join("victim");
        symlink(&victim, root.join(".edgezero/provision.lock")).expect("symlink");

        let Err(err) = ProvisionLock::acquire(&root) else {
            panic!("symlinked provision.lock must be refused")
        };
        assert!(err.contains("symlink"), "{err}");
        assert!(
            !victim.exists(),
            "the refused acquire must not have created the link target"
        );
    }

    #[test]
    fn two_concurrent_acquires_serialise_via_the_lock() {
        let temp = TempDir::new().expect("tempdir");
        let root_a = temp.path().to_path_buf();
        let root_b = root_a.clone();

        let (tx, rx) = mpsc::channel();
        // Thread A takes the lock and holds it for 50ms.
        let handle_a = thread::spawn(move || {
            let lock = ProvisionLock::acquire(&root_a).expect("A acquire");
            tx.send(()).expect("signal");
            thread::sleep(Duration::from_millis(50));
            drop(lock);
        });
        // Wait until A has definitely acquired.
        rx.recv().expect("await A");
        let start = Instant::now();
        // Thread B tries; must block until A releases.
        let lock_b = ProvisionLock::acquire(&root_b).expect("B acquire");
        let elapsed = start.elapsed();
        drop(lock_b);
        handle_a.join().expect("join A");
        assert!(
            elapsed >= Duration::from_millis(30),
            "B must have waited on A's lock; only waited {elapsed:?}"
        );
    }

    #[test]
    fn dropping_the_lock_releases_it_for_the_next_acquire() {
        let temp = TempDir::new().expect("tempdir");
        let lock = ProvisionLock::acquire(temp.path()).expect("acquire 1");
        drop(lock);
        // Should be immediately available.
        let start = Instant::now();
        let _lock2 = ProvisionLock::acquire(temp.path()).expect("acquire 2");
        assert!(
            start.elapsed() < Duration::from_millis(100),
            "second acquire must not block after drop"
        );
    }
}
