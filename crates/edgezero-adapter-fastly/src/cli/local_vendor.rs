//! Isolate native CLI manifest rewrites from the operator's source files.
use super::{ManifestLock, atomically_replace_file};
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::{NamedTempFile, TempDir};
use toml_edit::{DocumentMut, value};

pub(super) struct PrivateManifest {
    startup: TempDir,
    selected: NamedTempFile,
    source: PathBuf,
    snapshot_id: Option<String>,
    environment: String,
}

impl PrivateManifest {
    pub(super) fn new(requested_source: &Path) -> Result<Self, String> {
        let (source, snapshot) = {
            let lock = ManifestLock::acquire(requested_source)?;
            let snapshot = fs::read_to_string(lock.target()).map_err(|err| err.to_string())?;
            (lock.target().to_path_buf(), snapshot)
        };
        let snapshot_id = manifest_id(&snapshot)?;
        let startup = tempfile::tempdir().map_err(|err| err.to_string())?;
        // Fastly reads this before parsing --dir/--env and may upgrade it.
        fs::write(startup.path().join("fastly.toml"), &snapshot).map_err(|err| err.to_string())?;
        let directory = source.parent().ok_or("Fastly manifest has no parent")?;
        let mut selected = tempfile::Builder::new()
            .prefix("fastly.edgezero-")
            .suffix(".toml")
            .tempfile_in(directory)
            .map_err(|err| err.to_string())?;
        selected
            .write_all(snapshot.as_bytes())
            .map_err(|err| err.to_string())?;
        let filename = selected
            .path()
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or("private Fastly manifest filename is not UTF-8")?;
        let environment = filename
            .strip_prefix("fastly.")
            .and_then(|name| name.strip_suffix(".toml"))
            .ok_or("invalid private Fastly manifest filename")?
            .to_owned();
        Ok(Self {
            startup,
            selected,
            source,
            snapshot_id,
            environment,
        })
    }

    pub(super) fn configure(&self, command: &mut Command) {
        command
            .current_dir(self.startup.path())
            .arg("--dir")
            .arg(self.source.parent().unwrap_or_else(|| Path::new(".")))
            .arg("--env")
            .arg(&self.environment);
    }

    pub(super) fn merge_service_id(&self) -> Result<(), String> {
        let vendor = fs::read_to_string(self.selected.path()).map_err(|err| err.to_string())?;
        let vendor_state = manifest_id(&vendor)?;
        if vendor_state == self.snapshot_id {
            return Ok(());
        }
        let Some(vendor_id) = vendor_state else {
            return Err(
                "Fastly removed service_id from its private manifest; source was preserved"
                    .to_owned(),
            );
        };
        super::staging::validate_service_id(&vendor_id)?;
        let lock = ManifestLock::acquire(&self.source)?;
        let current = fs::read_to_string(lock.target()).map_err(|err| err.to_string())?;
        let current_id = manifest_id(&current)?;
        if current_id != self.snapshot_id && current_id.as_deref() != Some(&vendor_id) {
            return Err("Fastly service_id changed concurrently; refusing vendor writeback and preserving the source manifest".to_owned());
        }
        let mut document: DocumentMut = current
            .parse()
            .map_err(|_err| "invalid Fastly manifest (parse details redacted)".to_owned())?;
        document.insert("service_id", value(vendor_id));
        atomically_replace_file(lock.target(), &current, &document.to_string())
    }
}

fn manifest_id(body: &str) -> Result<Option<String>, String> {
    let document: DocumentMut = body
        .parse()
        .map_err(|_err| "invalid Fastly manifest (parse details redacted)".to_owned())?;
    document
        .get("service_id")
        .map(|item| {
            item.as_str()
                .map(str::to_owned)
                .ok_or_else(|| "Fastly service_id must be a string".to_owned())
        })
        .transpose()
        .map(|optional_id| optional_id.filter(|id| !id.is_empty()))
}

pub(super) fn validate_native_layout_args(args: &[String]) -> Result<(), String> {
    for arg in args {
        if arg == "--dir"
            || arg.starts_with("--dir=")
            || arg.starts_with("-C")
            || arg == "--env"
            || arg.starts_with("--env=")
        {
            return Err(format!(
                "Fastly argument {arg:?} overrides the adapter-owned manifest selection; select the manifest in edgezero.toml"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::iter::once;
    #[cfg(unix)]
    use std::time::{Duration, Instant};
    #[cfg(unix)]
    use std::{env, thread};

    #[cfg(unix)]
    fn write_vendor_shim(directory: &Path) {
        use std::os::unix::fs::PermissionsExt as _;
        let shim = directory.join("fastly");
        fs::write(
            &shim,
            r#"#!/bin/sh
# Model Fastly's manifest upgrade before global flag parsing.
printf 'startup_upgraded = true\n' > fastly.toml
selected_dir='.'
selected_env=''
while [ "$#" -gt 0 ]; do
  case "$1" in
    --dir) selected_dir="$2"; shift ;;
    --env) selected_env="$2"; shift ;;
  esac
  shift
done
if [ -n "$EDGEZERO_VENDOR_BARRIER" ]; then
  touch "$EDGEZERO_VENDOR_BARRIER/ready"
  while [ ! -e "$EDGEZERO_VENDOR_BARRIER/release" ]; do sleep 0.01; done
fi
printf 'name = "vendor"\nservice_id = "NewService"\n' > "$selected_dir/fastly.$selected_env.toml"
if [ -n "$EDGEZERO_VENDOR_BARRIER" ]; then touch "$EDGEZERO_VENDOR_BARRIER/done"; fi
"#,
        )
        .unwrap();
        fs::set_permissions(shim, fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn native_deploy_isolates_startup_and_selected_manifest_writes() {
        use edgezero_adapter::registry::AdapterExecContext;
        use edgezero_core::test_env::{EnvOverride, PathPrepend};
        let _lock = super::super::path_mutation_guard().lock().unwrap();
        let _no_service = EnvOverride::remove("FASTLY_SERVICE_ID");
        let dir = tempfile::tempdir().unwrap();
        write_vendor_shim(dir.path());
        let _path = PathPrepend::new(dir.path());
        let source = dir.path().join("custom.toml");
        let sibling = dir.path().join("fastly.toml");
        fs::write(&source, "# source\nname = 'original'\n").unwrap();
        fs::write(&sibling, "# unrelated\nname = 'sibling'\n").unwrap();
        super::super::run::deploy(
            &[],
            &AdapterExecContext::new().with_adapter_manifest(&source),
        )
        .unwrap();
        let actual = fs::read_to_string(&source).unwrap();
        assert!(
            actual.contains("# source")
                && actual.contains("'original'")
                && actual.contains("NewService"),
            "{actual}"
        );
        assert_eq!(
            fs::read_to_string(sibling).unwrap(),
            "# unrelated\nname = 'sibling'\n"
        );
        assert!(!fs::read_dir(dir.path()).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("fastly.edgezero-")
        }));
    }

    #[cfg(unix)]
    #[test]
    fn cancelled_parent_cannot_leave_vendor_writing_source_manifests() {
        use edgezero_adapter::registry::{AdapterDeployContext, AdapterExecContext};
        use std::process::{Command, Stdio};
        const CHILD_ENV: &str = "EDGEZERO_VENDOR_CANCEL_CHILD";
        if let Some(root) = env::var_os(CHILD_ENV) {
            let source = Path::new(&root).join("custom.toml");
            let environment = vec![(
                "EDGEZERO_VENDOR_BARRIER".to_owned(),
                root.to_string_lossy().into_owned(),
            )];
            let context = AdapterDeployContext {
                adapter_manifest_path: Some(source),
                ..Default::default()
            };
            super::super::run::deploy_with_context(
                &context,
                &[],
                &AdapterExecContext::new().with_env(&environment),
            )
            .unwrap();
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        write_vendor_shim(dir.path());
        let source = dir.path().join("custom.toml");
        let sibling = dir.path().join("fastly.toml");
        fs::write(&source, "name = 'original'\n").unwrap();
        fs::write(&sibling, "name = 'sibling'\n").unwrap();
        let path = env::join_paths(
            once(dir.path().to_path_buf())
                .chain(env::split_paths(&env::var_os("PATH").unwrap_or_default())),
        )
        .unwrap();
        let mut parent = Command::new(env::current_exe().unwrap())
            .args(["--exact", "cli::local_vendor::tests::cancelled_parent_cannot_leave_vendor_writing_source_manifests", "--nocapture"])
            .env(CHILD_ENV, dir.path()).env("PATH", path).env_remove("FASTLY_SERVICE_ID")
            .stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap();
        wait_for_barrier(&dir.path().join("ready"));
        parent.kill().unwrap();
        parent.wait().unwrap();
        let edited = "# concurrent writer\nname = 'changed'\nservice_id = 'Concurrent'\n";
        fs::write(&source, edited).unwrap();
        fs::write(dir.path().join("release"), "").unwrap();
        wait_for_barrier(&dir.path().join("done"));
        assert_eq!(fs::read_to_string(source).unwrap(), edited);
        assert_eq!(fs::read_to_string(sibling).unwrap(), "name = 'sibling'\n");
    }

    #[cfg(unix)]
    fn wait_for_barrier(path: &Path) {
        let started = Instant::now();
        while !path.exists() {
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "vendor did not reach {}",
                path.display()
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn writeback_preserves_concurrent_unrelated_edits() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("custom.toml");
        fs::write(&source, "name = 'original'\n").unwrap();
        let private = PrivateManifest::new(&source).unwrap();
        fs::write(
            private.selected.path(),
            "name = 'vendor'\nservice_id = 'NewService'\n",
        )
        .unwrap();
        fs::write(&source, "# operator\nname = 'edited'\n").unwrap();
        private.merge_service_id().unwrap();
        let actual = fs::read_to_string(source).unwrap();
        assert!(
            actual.contains("# operator")
                && actual.contains("'edited'")
                && actual.contains("NewService"),
            "{actual}"
        );
        assert!(!actual.contains("vendor"));
    }

    #[test]
    fn writeback_refuses_a_concurrent_service_change() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("fastly.toml");
        fs::write(&source, "service_id = 'Original'\n").unwrap();
        let private = PrivateManifest::new(&source).unwrap();
        fs::write(private.selected.path(), "service_id = 'Vendor'\n").unwrap();
        let edited = "service_id = 'Concurrent'\n";
        fs::write(&source, edited).unwrap();
        assert!(
            private
                .merge_service_id()
                .unwrap_err()
                .contains("concurrently")
        );
        assert_eq!(fs::read_to_string(source).unwrap(), edited);
    }

    #[test]
    fn native_directory_and_environment_overrides_are_adapter_owned() {
        for arg in [
            "--dir",
            "--dir=x",
            "-C",
            "-Cpath",
            "--env",
            "--env=production",
        ] {
            assert!(validate_native_layout_args(&[arg.to_owned()]).is_err());
        }
    }
}
