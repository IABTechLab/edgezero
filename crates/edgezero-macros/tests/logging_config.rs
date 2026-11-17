#[cfg(test)]
mod tests {
    mod absent {
        edgezero_core::app!("tests/fixtures/logging_config_absent.toml");
    }

    mod configured {
        edgezero_core::app!("tests/fixtures/logging_config.toml");
    }

    use edgezero_core::app::Hooks as _;
    use edgezero_core::manifest::{ManifestLoader, ResolvedLoggingConfig};
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::{Command, Output};

    fn provision_compiler_fixtures() {
        let metadata_output = Command::new(env!("CARGO"))
            .args([
                "metadata",
                "--offline",
                "--locked",
                "--no-deps",
                "--format-version=1",
            ])
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .output()
            .expect("Cargo metadata");
        assert!(
            metadata_output.status.success(),
            "{}",
            String::from_utf8_lossy(&metadata_output.stderr)
        );
        let metadata: serde_json::Value =
            serde_json::from_slice(&metadata_output.stdout).expect("metadata JSON");
        let fixtures = PathBuf::from(
            metadata["target_directory"]
                .as_str()
                .expect("target directory"),
        )
        .join("tests/trybuild/edgezero-macros/tests/fixtures");
        fs::create_dir_all(&fixtures).expect("compiler fixture directory");
        let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        for entry_result in fs::read_dir(source).expect("fixture sources") {
            let entry = entry_result.expect("fixture entry");
            let name = entry.file_name();
            if name.to_string_lossy().starts_with("logging_config") || name == "owns_logging.toml" {
                fs::copy(entry.path(), fixtures.join(name)).expect("compiler manifest fixture");
            }
        }
        fs::write(fixtures.join("logging_config_non_utf8.toml"), [0xff])
            .expect("invalid UTF-8 fixture");
        fs::create_dir_all(fixtures.join("logging_config_directory.toml"))
            .expect("directory fixture");
    }

    #[test]
    fn logging_config_public_macro_compiles() {
        provision_compiler_fixtures();
        let cases = trybuild::TestCases::new();
        for name in ["basic", "hygiene", "manual_routes"] {
            cases.pass(format!("tests/ui/logging_config_{name}.rs"));
        }
        for name in [
            "bad_args",
            "non_utf8",
            "bad_level",
            "case_duplicate",
            "bad_endpoint",
            "malformed",
            "bad_type",
        ] {
            cases.compile_fail(format!("tests/ui/logging_config_{name}.rs"));
        }
        if cfg!(unix) {
            // OS error text is Unix-specific; portable reader coverage lives in the macro unit tests.
            cases.compile_fail("tests/ui/logging_config_missing.rs");
            cases.compile_fail("tests/ui/logging_config_unreadable.rs");
        }
    }

    fn assert_logging_eq(actual: &ResolvedLoggingConfig, expected: &ResolvedLoggingConfig) {
        assert_eq!(actual.level, expected.level);
        assert_eq!(actual.endpoint, expected.endpoint);
        assert_eq!(actual.echo_stdout, expected.echo_stdout);
    }

    #[test]
    fn logging_config_matches_app_and_manifest_resolution() {
        let manifest =
            ManifestLoader::try_load_from_str(include_str!("fixtures/logging_config.toml"))
                .expect("valid logging fixture");
        for adapter in [
            "axum",
            "AXUM",
            "Cloudflare",
            "FASTLY",
            "spin",
            "Custom",
            "partial",
            "empty",
            "error_only",
            "no_logging",
            "unknown",
        ] {
            let actual =
                edgezero_core::logging_config!("tests/fixtures/logging_config.toml", adapter);
            assert_logging_eq(&actual, &configured::App::logging_for(adapter));
            assert_logging_eq(&actual, &manifest.manifest().logging_or_default(adapter));
        }
        let overridden =
            edgezero_core::logging_config!("tests/fixtures/logging_config.toml", "cloudflare");
        assert!(
            overridden.endpoint.is_none(),
            "sections must not merge fields"
        );
        assert!(overridden.echo_stdout.is_none());
    }

    #[test]
    fn logging_config_without_logging_uses_portable_defaults() {
        for adapter in ["axum", "cloudflare", "fastly", "spin", "unknown"] {
            let actual = edgezero_core::logging_config!(
                "tests/fixtures/logging_config_absent.toml",
                adapter
            );
            assert_logging_eq(&actual, &ResolvedLoggingConfig::default());
            assert_logging_eq(&actual, &absent::App::logging_for(adapter));
        }
    }

    fn cargo(project: &Path, target: &Path, args: &[&str]) -> Output {
        Command::new(env!("CARGO"))
            .args(args)
            .env("CARGO_NET_OFFLINE", "true")
            .env("CARGO_TARGET_DIR", target)
            .current_dir(project)
            .output()
            .expect("consumer Cargo command")
    }

    fn build_consumer(project: &Path, target: &Path, host: &str) -> Output {
        cargo(
            project,
            target,
            &[
                "build",
                "--offline",
                "--locked",
                "--manifest-path",
                "Cargo.toml",
                "--target",
                host,
                "--message-format=json",
            ],
        )
    }

    fn executable(output: &Output, project: &Path) -> PathBuf {
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&output.stdout)
        );
        let manifest = project
            .join("Cargo.toml")
            .canonicalize()
            .expect("consumer manifest");
        let paths: Vec<_> = output
            .stdout
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice::<serde_json::Value>(line).expect("Cargo record"))
            .filter(|record| {
                record["reason"] == "compiler-artifact"
                    && record["target"]["name"] == "logging-rebuild-consumer"
                    && record["target"]["kind"] == serde_json::json!(["bin"])
                    && record["profile"]["test"] == false
                    && record["manifest_path"].as_str().is_some_and(|path| {
                        Path::new(path)
                            .canonicalize()
                            .is_ok_and(|candidate| candidate == manifest)
                    })
            })
            .map(|record| PathBuf::from(record["executable"].as_str().expect("binary executable")))
            .collect();
        assert_eq!(paths.len(), 1, "one consumer executable");
        paths.into_iter().next().expect("consumer executable")
    }

    fn assert_consumer_output(executable: &Path, expected: &str) {
        let output = Command::new(executable)
            .output()
            .expect("execute baked logging");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            String::from_utf8(output.stdout).expect("text output"),
            expected
        );
    }

    fn compiler_inputs(core: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
        let root = core
            .parent()
            .and_then(Path::parent)
            .expect("workspace root");
        let macros = root.join("crates/edgezero-macros");
        let mut pending = vec![core.join("src"), macros.join("src"), root.join(".cargo")];
        let mut files = BTreeMap::new();
        for path in [
            root.join("Cargo.toml"),
            root.join("Cargo.lock"),
            core.join("Cargo.toml"),
            macros.join("Cargo.toml"),
        ] {
            let contents = fs::read(&path).expect("compiler manifest input");
            files.insert(path, contents);
        }
        while let Some(directory) = pending.pop() {
            for entry_result in fs::read_dir(directory).expect("compiler source inputs") {
                let entry = entry_result.expect("compiler source entry");
                let path = entry.path();
                if entry.file_type().expect("compiler input type").is_dir() {
                    pending.push(path);
                } else {
                    let contents = fs::read(&path).expect("compiler source input");
                    files.insert(path, contents);
                }
            }
        }
        files
    }

    fn create_consumer(project: &Path, core: &Path) -> (String, &'static str) {
        let manifest_value = toml::Value::try_from(serde_json::json!({
            "package": {"name": "logging-rebuild-consumer", "version": "0.0.0", "edition": "2024"},
            "workspace": {},
            "dependencies": {"edgezero-core": {"path": core, "default-features": false}}
        }))
        .expect("structured Cargo manifest");
        let manifest = toml::to_string(&manifest_value).expect("serialized Cargo manifest");
        fs::write(project.join("Cargo.toml"), &manifest).expect("Cargo manifest");
        fs::create_dir_all(project.join("src")).expect("consumer source directory");
        let source = r#"
macro_rules! include_bytes { ($path:expr) => { b"" }; }
fn main() {
    let _ = include_bytes!("not-the-manifest");
    let config = edgezero_core::logging_config!("edgezero.toml", String::from("FASTLY").as_str());
    println!("{:?}|{:?}|{:?}", config.level, config.endpoint, config.echo_stdout);
}
"#;
        fs::write(project.join("src/main.rs"), source).expect("consumer source");
        (manifest, source)
    }

    #[test]
    fn logging_config_manifest_only_rebuild_and_runtime_independence() {
        let project_directory = tempfile::tempdir().expect("consumer workspace");
        let target = tempfile::tempdir().expect("independent consumer target");
        let project = project_directory.path();
        let core = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../edgezero-core")
            .canonicalize()
            .expect("local core dependency");
        let fixed_compiler_inputs = compiler_inputs(&core);
        let (manifest, source) = create_consumer(project, &core);
        let mut logging = toml::toml! { [logging.fastly] level = "debug" endpoint = "initial_logs" echo_stdout = false };
        fs::write(
            project.join("edgezero.toml"),
            toml::to_string(&logging).expect("logging TOML"),
        )
        .expect("initial manifest");
        let locked = cargo(
            project,
            target.path(),
            &[
                "generate-lockfile",
                "--offline",
                "--manifest-path",
                "Cargo.toml",
            ],
        );
        assert!(
            locked.status.success(),
            "{}",
            String::from_utf8_lossy(&locked.stderr)
        );
        let lock = fs::read(project.join("Cargo.lock")).expect("fixed lockfile");
        let version_output = Command::new("rustc")
            .arg("-vV")
            .output()
            .expect("native compiler");
        assert!(version_output.status.success());
        let version = String::from_utf8(version_output.stdout).expect("compiler version");
        let host = version
            .lines()
            .find_map(|line| line.strip_prefix("host: "))
            .expect("native host target");
        let binary = executable(&build_consumer(project, target.path(), host), project);
        assert_eq!(
            compiler_inputs(&core),
            fixed_compiler_inputs,
            "compiler inputs changed during initial build"
        );
        assert_consumer_output(&binary, "Debug|Some(\"initial_logs\")|Some(false)\n");
        fs::remove_file(project.join("edgezero.toml")).expect("remove runtime manifest");
        assert_consumer_output(&binary, "Debug|Some(\"initial_logs\")|Some(false)\n");
        logging["logging"]["fastly"]["level"] = "error".into();
        logging["logging"]["fastly"]["endpoint"] = "updated_logs".into();
        fs::write(
            project.join("edgezero.toml"),
            toml::to_string(&logging).expect("logging TOML"),
        )
        .expect("manifest-only edit");
        let updated_binary = executable(&build_consumer(project, target.path(), host), project);
        assert_eq!(
            compiler_inputs(&core),
            fixed_compiler_inputs,
            "compiler inputs changed during manifest-only rebuild"
        );
        assert_consumer_output(
            &updated_binary,
            "Error|Some(\"updated_logs\")|Some(false)\n",
        );
        logging["logging"]["fastly"]["level"] = "not-a-level".into();
        fs::write(
            project.join("edgezero.toml"),
            toml::to_string(&logging).expect("logging TOML"),
        )
        .expect("invalid manifest-only edit");
        let invalid = build_consumer(project, target.path(), host);
        assert_eq!(
            compiler_inputs(&core),
            fixed_compiler_inputs,
            "compiler inputs changed during invalid-manifest build"
        );
        assert!(
            !invalid.status.success(),
            "invalid manifest-only edit must invalidate compilation"
        );
        let diagnostics = String::from_utf8_lossy(&invalid.stdout);
        assert!(
            diagnostics.contains("logging level must be trace, debug, info, warn, error, or off"),
            "{diagnostics}"
        );
        assert_eq!(
            fs::read_to_string(project.join("src/main.rs")).expect("unchanged Rust source"),
            source
        );
        assert_eq!(
            fs::read_to_string(project.join("Cargo.toml")).expect("unchanged Cargo manifest"),
            manifest
        );
        assert_eq!(
            fs::read(project.join("Cargo.lock")).expect("unchanged lockfile"),
            lock
        );
    }
}
