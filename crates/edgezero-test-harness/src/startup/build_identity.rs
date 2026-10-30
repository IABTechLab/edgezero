use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    env,
    ffi::{OsStr, OsString},
    fs,
    io::{self, Read as _},
    path::{Path, PathBuf},
    process::Command,
};

// Hash source trees directly, including dirty/untracked inputs, but never build outputs.
const ROOTS: &[&str] = &[
    "crates",
    "tests/fixtures/reusable-app",
    "tests/fixtures/request-fidelity",
];

pub(super) fn current() -> crate::Result<String> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()?;
    let environment: BTreeMap<_, _> = env::vars_os().collect();
    let compiler = environment
        .get(OsStr::new("RUSTC"))
        .map(PathBuf::from)
        .unwrap_or_else(crate::rustc);
    let cargo = environment
        .get(OsStr::new("CARGO"))
        .map(OsString::as_os_str)
        .unwrap_or(OsStr::new("cargo"));
    let compiler = tool_version(compiler.as_os_str())?;
    let cargo = tool_version(cargo)?;
    let mut configs = Vec::new();
    for ancestor in root.ancestors().skip(1) {
        configs.extend([
            ancestor.join(".cargo/config"),
            ancestor.join(".cargo/config.toml"),
        ]);
    }
    let cargo_home = environment
        .get(OsStr::new("CARGO_HOME"))
        .map(PathBuf::from)
        .or_else(|| {
            environment
                .get(OsStr::new("HOME"))
                .map(|home| Path::new(home).join(".cargo"))
        })
        .ok_or_else(|| io::Error::other("cannot locate Cargo home for build identity"))?;
    configs.extend([cargo_home.join("config"), cargo_home.join("config.toml")]);
    Ok(capture(&root, &compiler, &cargo, &environment, &configs)?)
}

fn tool_version(program: &OsStr) -> crate::Result<Vec<u8>> {
    let directory = tempfile::tempdir()?;
    let stdout = directory.path().join("stdout");
    let log = directory.path().join("version.log");
    let mut command = Command::new(program);
    command.args(["--version", "--verbose"]);
    crate::Process::spawn_with_stdout(&mut command, &log, &stdout)?.wait_success()?;
    let output = fs::read(stdout)?;
    if output.iter().all(u8::is_ascii_whitespace) {
        return Err(io::Error::other(format!(
            "build identity version probe {program:?} returned no version"
        ))
        .into());
    }
    Ok(output)
}

fn build_variable(name: &OsStr) -> bool {
    let name = name.to_string_lossy();
    if name == "CARGO_TARGET_DIR" || name.ends_with("_RUNNER") {
        return false;
    }
    matches!(
        name.as_ref(),
        "RUSTFLAGS"
            | "RUSTDOCFLAGS"
            | "RUSTC"
            | "RUSTUP_TOOLCHAIN"
            | "ASDF_RUST_VERSION"
            | "CARGO_ENCODED_RUSTFLAGS"
            | "CARGO_ENCODED_RUSTDOCFLAGS"
            | "CARGO_INCREMENTAL"
            | "SDKROOT"
            | "MACOSX_DEPLOYMENT_TARGET"
            | "SOURCE_DATE_EPOCH"
            | "HOST"
            | "TARGET"
            | "PROFILE"
            | "OPT_LEVEL"
            | "DEBUG"
    ) || [
        "CARGO_PROFILE_",
        "CARGO_TARGET_",
        "CARGO_BUILD_",
        "RUSTC_",
        "FIXTURE_",
        "EDGEZERO_QUALIFICATION_",
        "PKG_CONFIG",
        "BINDGEN_",
    ]
    .iter()
    .any(|prefix| name.starts_with(prefix))
        || [
            "CC", "CXX", "AR", "CFLAGS", "CPPFLAGS", "CXXFLAGS", "LDFLAGS", "ARFLAGS",
        ]
        .iter()
        .any(|key| {
            name == *key
                || name.starts_with(&format!("{key}_"))
                || name.ends_with(&format!("_{key}"))
        })
}

fn field(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_le_bytes());
    hash.update(value);
}

fn file_field(hash: &mut Sha256, path: &Path) -> io::Result<()> {
    let mut file = fs::File::open(path)?;
    let length = file.metadata()?.len();
    hash.update(length.to_le_bytes());
    let mut observed = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        observed += count as u64;
        hash.update(&buffer[..count]);
    }
    if observed != length {
        return Err(io::Error::other(format!(
            "build identity input changed while reading: {}",
            path.display()
        )));
    }
    Ok(())
}

fn ignored_output(path: &Path) -> bool {
    let name = path.file_name().unwrap_or_default();
    [
        "target",
        ".runs",
        "node_modules",
        ".git",
        ".wrangler",
        ".spin",
    ]
    .iter()
    .any(|ignored| name == *ignored)
        || (name == "build"
            && path
                .parent()
                .and_then(Path::file_name)
                .is_some_and(|parent| {
                    parent == "fixture-cloudflare" || parent == "request-fidelity"
                }))
}

fn hash_tree(hash: &mut Sha256, root: &Path, path: &Path) -> io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.is_dir() {
        let mut entries = fs::read_dir(path)?.collect::<io::Result<Vec<_>>>()?;
        entries.sort_by_key(fs::DirEntry::file_name);
        for entry in entries {
            if !ignored_output(&entry.path()) {
                hash_tree(hash, root, &entry.path())?;
            }
        }
    } else if metadata.is_file() {
        field(
            hash,
            path.strip_prefix(root)
                .map_err(io::Error::other)?
                .as_os_str()
                .as_encoded_bytes(),
        );
        file_field(hash, path)?;
    } else {
        return Err(io::Error::other(format!(
            "unsupported build identity input: {}",
            path.display()
        )));
    }
    Ok(())
}

fn capture(
    root: &Path,
    compiler: &[u8],
    cargo: &[u8],
    environment: &BTreeMap<OsString, OsString>,
    configs: &[PathBuf],
) -> io::Result<String> {
    if compiler.is_empty() || cargo.is_empty() {
        return Err(io::Error::other("missing compiler/Cargo build identity"));
    }
    let mut hash = Sha256::new();
    field(&mut hash, b"edgezero-fixture-build-v1");
    field(&mut hash, env::consts::OS.as_bytes());
    field(&mut hash, env::consts::ARCH.as_bytes());
    field(&mut hash, compiler);
    field(&mut hash, cargo);
    for (name, value) in environment.iter().filter(|(name, _)| build_variable(name)) {
        field(&mut hash, name.as_os_str().as_encoded_bytes());
        field(&mut hash, value.as_os_str().as_encoded_bytes());
    }
    for input in ["Cargo.toml", "Cargo.lock", ".tool-versions"] {
        hash_tree(&mut hash, root, &root.join(input))?;
    }
    for input in ROOTS {
        hash_tree(&mut hash, root, &root.join(input))?;
    }
    let local_configs = [
        ".cargo/config",
        ".cargo/config.toml",
        "rust-toolchain",
        "rust-toolchain.toml",
    ]
    .map(|name| root.join(name));
    for (index, path) in local_configs.iter().chain(configs).enumerate() {
        field(&mut hash, format!("cargo-config-{index}").as_bytes());
        match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.is_file() => {
                field(&mut hash, b"file");
                file_field(&mut hash, path)?;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => field(&mut hash, b"absent"),
            Err(error) => return Err(error),
            _ => {
                return Err(io::Error::other(format!(
                    "unsupported Cargo config input: {}",
                    path.display()
                )));
            }
        }
    }
    Ok(format!("{:x}", hash.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const FILES: &[&str] = &[
        "Cargo.toml",
        "Cargo.lock",
        ".tool-versions",
        ".cargo/config.toml",
        "crates/edgezero-core/src/lib.rs",
        "crates/edgezero-core/Cargo.toml",
        "crates/edgezero-adapter-spin/tests/support/helper.rs",
        "crates/edgezero-test-harness/src/runners.rs",
        "tests/fixtures/reusable-app/Cargo.toml",
        "tests/fixtures/reusable-app/Cargo.lock",
        "tests/fixtures/reusable-app/crates/fixture-spin-wire/src/lib.rs",
        "tests/fixtures/reusable-app/crates/fixture-spin-wire/spin.toml",
        "tests/fixtures/request-fidelity/Cargo.toml",
        "tests/fixtures/request-fidelity/Cargo.lock",
        "tests/fixtures/request-fidelity/src/lib.rs",
        "crates/edgezero-core/src/build/embedded.txt",
        "rust-toolchain.toml",
    ];

    fn fixture() -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        for path in FILES {
            write(root.path(), path, b"original");
        }
        root
    }

    fn write(root: &Path, path: &str, contents: &[u8]) {
        let path = root.join(path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, contents).unwrap();
    }

    fn identity(root: &Path) -> String {
        capture(
            root,
            b"rustc fixture",
            b"cargo fixture",
            &BTreeMap::new(),
            &[],
        )
        .unwrap()
    }

    #[test]
    fn unchanged_inputs_reuse_a_content_identity_not_mtimes() {
        let root = fixture();
        let first = identity(root.path());
        assert_eq!(first.len(), 64);
        assert!(first.bytes().all(|byte| byte.is_ascii_hexdigit()));
        write(root.path(), FILES[4], b"original");
        assert_eq!(identity(root.path()), first);
    }

    #[test]
    fn every_source_manifest_lock_and_config_mutation_changes_identity() {
        let root = fixture();
        let first = identity(root.path());
        for path in FILES {
            write(root.path(), path, b"dirty source");
            assert_ne!(identity(root.path()), first, "ignored {path}");
            write(root.path(), path, b"original");
        }
    }

    #[test]
    fn untracked_sources_additions_renames_and_deletions_change_identity() {
        let root = fixture();
        let first = identity(root.path());
        let added = "crates/edgezero-core/src/untracked.rs";
        write(root.path(), added, b"new untracked source");
        let second = identity(root.path());
        assert_ne!(second, first);
        fs::rename(
            root.path().join(added),
            root.path().join("crates/edgezero-core/src/renamed.rs"),
        )
        .unwrap();
        assert_ne!(identity(root.path()), second);
        fs::remove_file(root.path().join("crates/edgezero-core/src/renamed.rs")).unwrap();
        assert_eq!(identity(root.path()), first);
        fs::remove_file(root.path().join(FILES[4])).unwrap();
        assert_ne!(identity(root.path()), first);
    }

    #[test]
    fn compiler_cargo_and_build_environment_changes_are_inputs() {
        let root = fixture();
        let first = identity(root.path());
        assert_ne!(
            capture(
                root.path(),
                b"different rustc",
                b"cargo fixture",
                &BTreeMap::new(),
                &[]
            )
            .unwrap(),
            first
        );
        assert_ne!(
            capture(
                root.path(),
                b"rustc fixture",
                b"different cargo",
                &BTreeMap::new(),
                &[]
            )
            .unwrap(),
            first
        );
        for key in [
            "RUSTFLAGS",
            "CARGO_ENCODED_RUSTFLAGS",
            "CARGO_PROFILE_DEV_DEBUG",
            "CARGO_TARGET_WASM32_WASIP2_RUSTFLAGS",
            "RUSTC_WRAPPER",
            "CC",
            "CFLAGS_wasm32_wasip2",
            "FIXTURE_BUILD_ROUNDS",
            "EDGEZERO_QUALIFICATION_REVISION",
        ] {
            let env = BTreeMap::from([(OsString::from(key), OsString::from("changed"))]);
            assert_ne!(
                capture(root.path(), b"rustc fixture", b"cargo fixture", &env, &[]).unwrap(),
                first,
                "ignored {key}"
            );
        }
    }

    #[test]
    fn external_cargo_config_presence_and_contents_are_inputs() {
        let root = fixture();
        let home = tempfile::tempdir().unwrap();
        let configs = [home.path().join("config.toml")];
        let get = || {
            capture(
                root.path(),
                b"rustc fixture",
                b"cargo fixture",
                &BTreeMap::new(),
                &configs,
            )
            .unwrap()
        };
        let first = get();
        fs::write(&configs[0], "absent").unwrap();
        assert_ne!(get(), first, "missing config differs from file contents");
        fs::write(&configs[0], "first config").unwrap();
        let second = get();
        assert_ne!(second, first);
        fs::write(&configs[0], "changed config").unwrap();
        assert_ne!(get(), second);
    }

    #[test]
    fn generated_outputs_and_per_launch_values_do_not_change_build_identity() {
        let root = fixture();
        let first = identity(root.path());
        for output in [
            "crates/edgezero-core/target/a",
            "crates/edgezero-core/node_modules/a",
            "tests/fixtures/reusable-app/target/a",
            "tests/fixtures/reusable-app/.runs/a",
            "tests/fixtures/request-fidelity/node_modules/a",
            "tests/fixtures/request-fidelity/build/a",
            "tests/fixtures/reusable-app/crates/fixture-cloudflare/build/a",
        ] {
            write(root.path(), output, b"ignored output");
        }
        let env = BTreeMap::from([
            (
                OsString::from(super::super::NONCE),
                OsString::from(crate::token()),
            ),
            (
                OsString::from("EDGEZERO_FIXTURE_BUILD_ID"),
                OsString::from("prior build"),
            ),
            (
                OsString::from("CARGO_TARGET_DIR"),
                OsString::from("different output directory"),
            ),
        ]);
        assert_eq!(
            capture(root.path(), b"rustc fixture", b"cargo fixture", &env, &[]).unwrap(),
            first
        );
    }

    #[test]
    fn incomplete_identity_capture_fails_closed() {
        let root = fixture();
        assert!(capture(root.path(), b"", b"cargo fixture", &BTreeMap::new(), &[]).is_err());
        fs::remove_file(root.path().join("Cargo.lock")).unwrap();
        assert!(
            capture(
                root.path(),
                b"rustc fixture",
                b"cargo fixture",
                &BTreeMap::new(),
                &[]
            )
            .is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn failed_or_empty_compiler_probes_do_not_supply_an_identity() {
        use std::os::unix::fs::PermissionsExt as _;
        let root = tempfile::tempdir().unwrap();
        let tool = root.path().join("compiler");
        for script in [
            "#!/bin/sh\nexit 0\n",
            "#!/bin/sh\necho false-success\nexit 23\n",
        ] {
            fs::write(&tool, script).unwrap();
            fs::set_permissions(&tool, fs::Permissions::from_mode(0o700)).unwrap();
            assert!(tool_version(tool.as_os_str()).is_err());
        }
    }

    #[cfg(unix)]
    #[test]
    fn source_symlinks_fail_closed_instead_of_hiding_external_mutations() {
        let root = fixture();
        std::os::unix::fs::symlink(
            "/missing",
            root.path().join("crates/edgezero-core/src/linked.rs"),
        )
        .unwrap();
        assert!(
            capture(
                root.path(),
                b"rustc fixture",
                b"cargo fixture",
                &BTreeMap::new(),
                &[]
            )
            .is_err()
        );
    }
}
