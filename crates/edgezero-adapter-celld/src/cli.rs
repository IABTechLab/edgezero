//! Host-side CLI registration for local celld build and serve.

use std::env;
use std::ffi::OsStr;
use std::io;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::process::Command;

use ctor::ctor;
use edgezero_adapter::cli_support::find_manifest_upwards;
use edgezero_adapter::registry::{
    Adapter, AdapterAction, AdapterDeployContext, AdapterExecutionTarget, DeployOwnership,
    ProvisionStores, register_adapter,
};
use edgezero_adapter::scaffold::{
    AdapterBlueprint, AdapterFileSpec, CommandTemplates, DependencySpec, LoggingDefaults,
    ManifestSpec, ReadmeInfo, TemplateRegistration, register_adapter_blueprint,
};
use edgezero_core::{Capability, CapabilitySupport, PlatformMetadata};

use crate::{CELLD_ADAPTER, CELLD_PLATFORM};

static CELLD_ADAPTER_IMPL: CelldCliAdapter = CelldCliAdapter;

static CELLD_BLUEPRINT: AdapterBlueprint = AdapterBlueprint {
    id: "celld",
    display_name: "celld",
    crate_suffix: "adapter-celld",
    dependency_crate: "edgezero-adapter-celld",
    dependency_repo_path: "crates/edgezero-adapter-celld",
    template_registrations: CELLD_TEMPLATE_REGISTRATIONS,
    files: CELLD_FILE_SPECS,
    extra_dirs: &["src", ".cargo"],
    dependencies: CELLD_DEPENDENCIES,
    manifest: ManifestSpec {
        manifest_filename: "wrangler.json",
        build_target: "wasm32-unknown-unknown",
        build_profile: "release",
        build_features: &["celld"],
    },
    // Empty command overrides keep this adapter on registered execute_target,
    // where build-before-serve and loopback argument validation live.
    commands: CommandTemplates {
        build: "",
        deploy: "",
        serve: "",
    },
    logging: LoggingDefaults {
        endpoint: None,
        level: "info",
        echo_stdout: None,
    },
    readme: ReadmeInfo {
        description: "Experimental celld local Worker entrypoint.",
        dev_heading: "celld (experimental local runtime)",
        dev_steps: &["`edgezero serve --adapter celld` (loopback only)"],
    },
    run_module: "edgezero_adapter_celld",
};

static CELLD_DEPENDENCIES: &[DependencySpec] = &[DependencySpec {
    key: "dep_edgezero_adapter_celld",
    repo_crate: "crates/edgezero-adapter-celld",
    fallback: "edgezero-adapter-celld = { git = \"https://git@github.com/stackpop/edgezero.git\", package = \"edgezero-adapter-celld\", default-features = false, features = [\"celld\"] }",
    features: &["celld"],
}];

static CELLD_FILE_SPECS: &[AdapterFileSpec] = &[
    AdapterFileSpec {
        template: "celld_Cargo_toml",
        output: "Cargo.toml",
    },
    AdapterFileSpec {
        template: "celld_src_lib_rs",
        output: "src/lib.rs",
    },
    AdapterFileSpec {
        template: "celld_src_main_rs",
        output: "src/main.rs",
    },
    AdapterFileSpec {
        template: "celld_cargo_config_toml",
        output: ".cargo/config.toml",
    },
    AdapterFileSpec {
        template: "celld_wrangler_json",
        output: "wrangler.json",
    },
];

static CELLD_TEMPLATE_REGISTRATIONS: &[TemplateRegistration] = &[
    TemplateRegistration {
        name: "celld_Cargo_toml",
        contents: include_str!("templates/Cargo.toml.hbs"),
    },
    TemplateRegistration {
        name: "celld_src_lib_rs",
        contents: include_str!("templates/src/lib.rs.hbs"),
    },
    TemplateRegistration {
        name: "celld_src_main_rs",
        contents: include_str!("templates/src/main.rs.hbs"),
    },
    TemplateRegistration {
        name: "celld_cargo_config_toml",
        contents: include_str!("templates/.cargo/config.toml.hbs"),
    },
    TemplateRegistration {
        name: "celld_wrangler_json",
        contents: include_str!("templates/wrangler.json.hbs"),
    },
];

struct CelldCliAdapter;

#[expect(
    clippy::missing_trait_methods,
    reason = "celld supports only local build/serve and deliberately has no deploy, provisioning, or config publication implementation"
)]
impl Adapter for CelldCliAdapter {
    fn capability(&self, capability: Capability) -> CapabilitySupport {
        match capability {
            Capability::IngressAdmission => CapabilitySupport::Native,
            Capability::ConfigReadDeadlines
            | Capability::InboundReadDeadlines
            | Capability::IngressAdmissionAbort
            | Capability::LazyStreamedResponsePassthrough
            | Capability::OutboundAuthorityOverride
            | Capability::OutboundBatchCancellation
            | Capability::OutboundBatchCompletionOrder
            | Capability::OutboundBatchSlotIsolation
            | Capability::OutboundCacheBypass
            | Capability::OutboundDeadlines
            | Capability::OutboundFlexiblePhaseBudget
            | Capability::OutboundHeaderFidelity
            | Capability::OutboundHttp
            | Capability::ResponseEgressAbort
            | Capability::ResponseEgressBackpressure
            | Capability::ResponseEgressCompletion
            | Capability::ResponseWriteDeadlines
            | Capability::StreamedUploadDeadlines => CapabilitySupport::BestEffort,
            // Default stream reads bound Rust-owned copies, but the runtime may
            // have allocated an entire KV value as one JS chunk before delivery.
            Capability::ConfigReadAllocationBounds
            | Capability::OutboundCompleteResourceAccounting
            | Capability::RawIngressFramingValidation
            | Capability::RawIngressHeadLimits
            | _ => CapabilitySupport::Unsupported,
        }
    }

    fn execute(&self, action: AdapterAction, args: &[String]) -> Result<(), String> {
        match action {
            AdapterAction::Build => {
                let manifest =
                    find_celld_manifest(&env::current_dir().map_err(|err| err.to_string())?)?;
                build_from_manifest(&manifest, args).map(|_| ())
            }
            AdapterAction::Serve => {
                let manifest =
                    find_celld_manifest(&env::current_dir().map_err(|err| err.to_string())?)?;
                serve_from_manifest(&manifest, args)
            }
            AdapterAction::AuthLogin
            | AdapterAction::AuthLogout
            | AdapterAction::AuthStatus
            | AdapterAction::Deploy
            | AdapterAction::DeployStaging
            | AdapterAction::EmitVersion
            | AdapterAction::Healthcheck
            | AdapterAction::Rollback => Err(unsupported_action(action)),
            _ => Err("celld adapter does not implement this action".to_owned()),
        }
    }

    fn execute_target(
        &self,
        action: AdapterAction,
        target: &AdapterExecutionTarget,
        args: &[String],
    ) -> Result<(), String> {
        let manifest = target.platform_manifest().ok_or_else(|| {
            "celld target is missing [adapters.celld.adapter].manifest".to_owned()
        })?;
        match action {
            AdapterAction::Build => build_from_manifest(manifest, args).map(|_| ()),
            AdapterAction::Serve => serve_from_manifest(manifest, args),
            AdapterAction::AuthLogin
            | AdapterAction::AuthLogout
            | AdapterAction::AuthStatus
            | AdapterAction::Deploy
            | AdapterAction::DeployStaging
            | AdapterAction::EmitVersion
            | AdapterAction::Healthcheck
            | AdapterAction::Rollback => Err(unsupported_action(action)),
            _ => Err("celld adapter does not implement this action".to_owned()),
        }
    }

    fn merged_id_kinds(&self) -> &'static [&'static str] {
        &["kv", "config"]
    }

    fn name(&self) -> &'static str {
        CELLD_ADAPTER
    }

    fn platform_metadata(&self) -> PlatformMetadata {
        CELLD_PLATFORM
    }

    fn preflight_deploy(
        &self,
        _context: &AdapterDeployContext,
        _args: &[String],
    ) -> Result<DeployOwnership, String> {
        Err("celld adapter supports local build and serve only; deploy is unsupported".to_owned())
    }

    fn provision(
        &self,
        _manifest_root: &Path,
        _adapter_manifest_path: Option<&str>,
        _component_selector: Option<&str>,
        _stores: &ProvisionStores<'_>,
        _dry_run: bool,
    ) -> Result<Vec<String>, String> {
        Err("celld adapter does not provision platform resources".to_owned())
    }
}

fn unsupported_action(action: AdapterAction) -> String {
    format!("celld adapter supports local build and serve only; {action:?} is unsupported")
}

fn find_celld_manifest(start: &Path) -> Result<PathBuf, String> {
    find_manifest_upwards(start, "wrangler.json")
        .or_else(|| find_manifest_upwards(start, "wrangler.jsonc"))
        .ok_or_else(|| "could not locate wrangler.json or wrangler.jsonc for celld".to_owned())
}

fn build_from_manifest(manifest: &Path, extra_args: &[String]) -> Result<PathBuf, String> {
    build_with_program(manifest, extra_args, OsStr::new("worker-build"))
}

fn build_with_program(
    manifest: &Path,
    extra_args: &[String],
    worker_build: &OsStr,
) -> Result<PathBuf, String> {
    let manifest_dir = manifest
        .parent()
        .ok_or_else(|| "celld manifest has no parent directory".to_owned())?;
    let status = Command::new(worker_build)
        .arg("--release")
        .arg(manifest_dir)
        .args(extra_args)
        .current_dir(manifest_dir)
        .status()
        .map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                "`worker-build` is not on PATH; install workers-rs worker-build 0.8.5".to_owned()
            } else {
                format!("failed to run `worker-build`: {error}")
            }
        })?;
    if !status.success() {
        return Err(format!(
            "`worker-build --release` failed with status {status}"
        ));
    }
    let shim = manifest_dir.join("build/worker/shim.mjs");
    if !shim.is_file() {
        return Err(format!(
            "`worker-build` succeeded but did not emit {}",
            shim.display()
        ));
    }
    Ok(shim)
}

fn serve_from_manifest(manifest: &Path, args: &[String]) -> Result<(), String> {
    serve_with_programs(
        manifest,
        args,
        OsStr::new("worker-build"),
        OsStr::new("celld"),
    )
}

fn serve_with_programs(
    manifest: &Path,
    args: &[String],
    worker_build: &OsStr,
    celld_program: &OsStr,
) -> Result<(), String> {
    let safe_args = validate_serve_args(args)?;
    let _shim = build_with_program(manifest, &[], worker_build)?;
    let canonical_manifest = manifest
        .canonicalize()
        .map_err(|error| format!("failed to resolve celld manifest: {error}"))?;
    let status = Command::new(celld_program)
        .arg("dev")
        .arg(&canonical_manifest)
        .args(safe_args)
        .arg("--host")
        .arg("127.0.0.1")
        .current_dir(canonical_manifest.parent().unwrap_or(Path::new(".")))
        .status()
        .map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                "`celld` is not on PATH; install celld v0.6.2".to_owned()
            } else {
                format!("failed to run `celld dev`: {error}")
            }
        })?;
    if !status.success() {
        return Err(format!("`celld dev` failed with status {status}"));
    }
    Ok(())
}

fn validate_serve_args(args: &[String]) -> Result<Vec<String>, String> {
    let mut safe = Vec::with_capacity(args.len());
    let mut arguments = args.iter();
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--host" => {
                let value = arguments
                    .next()
                    .ok_or_else(|| "celld serve --host requires a loopback IP".to_owned())?;
                validate_loopback_host(value)?;
            }
            value => {
                if let Some(host) = value.strip_prefix("--host=") {
                    validate_loopback_host(host)?;
                } else if matches!(value, "--port" | "--watch-ignore") {
                    let argument_value = arguments
                        .next()
                        .ok_or_else(|| format!("celld serve {value} requires a value"))?;
                    if value == "--port" {
                        validate_port(argument_value)?;
                    }
                    safe.push(value.to_owned());
                    safe.push(argument_value.clone());
                } else if matches!(value, "--clean" | "--logs" | "--no-watch") {
                    safe.push(value.to_owned());
                } else {
                    return Err(format!(
                        "celld serve does not accept forwarded argument {value:?}"
                    ));
                }
            }
        }
    }
    Ok(safe)
}

fn validate_loopback_host(value: &str) -> Result<(), String> {
    match value.parse::<IpAddr>() {
        Ok(ip) if ip.is_loopback() => Ok(()),
        _ => Err("celld serve only permits loopback --host values (127.0.0.1 or ::1)".to_owned()),
    }
}

fn validate_port(value: &str) -> Result<(), String> {
    match value.parse::<u16>() {
        Ok(port) if port != 0 => Ok(()),
        _ => Err(format!("invalid loopback port {value:?}")),
    }
}

/// Register celld CLI and scaffold support.
#[inline]
pub fn register() {
    register_adapter(&CELLD_ADAPTER_IMPL);
    register_adapter_blueprint(&CELLD_BLUEPRINT);
}

#[ctor(unsafe)]
fn register_ctor() {
    register();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capability_map_does_not_claim_raw_ingress_or_resource_accounting() {
        assert_eq!(
            CELLD_ADAPTER_IMPL.capability(Capability::IngressAdmission),
            CapabilitySupport::Native
        );
        assert_eq!(
            CELLD_ADAPTER_IMPL.capability(Capability::RawIngressFramingValidation),
            CapabilitySupport::Unsupported
        );
        assert_eq!(
            CELLD_ADAPTER_IMPL.capability(Capability::ConfigReadAllocationBounds),
            CapabilitySupport::Unsupported
        );
        assert_eq!(
            CELLD_ADAPTER_IMPL.capability(Capability::OutboundCompleteResourceAccounting),
            CapabilitySupport::Unsupported
        );
    }

    #[test]
    fn loopback_host_is_required_and_forwarded_public_hosts_are_rejected() {
        validate_loopback_host("127.0.0.1").expect("IPv4 loopback");
        validate_loopback_host("::1").expect("IPv6 loopback");
        validate_loopback_host("0.0.0.0").expect_err("wildcard host rejected");
        validate_loopback_host("192.0.2.1").expect_err("public host rejected");
        validate_serve_args(&["--host".to_owned(), "0.0.0.0".to_owned()])
            .expect_err("public forwarded host rejected");
    }

    #[test]
    fn only_local_celld_dev_arguments_are_forwarded() {
        assert_eq!(
            validate_serve_args(&["--port".to_owned(), "9877".to_owned(), "--logs".to_owned()])
                .expect("safe local args"),
            vec!["--port", "9877", "--logs"]
        );
        validate_serve_args(&["--remote".to_owned()]).expect_err("remote option rejected");
        validate_serve_args(&["--port".to_owned(), "0".to_owned()])
            .expect_err("zero port rejected");
    }

    #[test]
    fn unsupported_actions_never_fall_through_to_cloud_or_fleet_commands() {
        assert!(
            CELLD_ADAPTER_IMPL
                .execute(AdapterAction::AuthLogin, &[])
                .expect_err("auth unsupported")
                .contains("unsupported")
        );
        assert!(
            CELLD_ADAPTER_IMPL
                .execute(AdapterAction::Deploy, &[])
                .expect_err("deploy unsupported")
                .contains("unsupported")
        );
        assert!(
            CELLD_ADAPTER_IMPL
                .execute(AdapterAction::DeployStaging, &[])
                .expect_err("release action unsupported")
                .contains("unsupported")
        );
        assert!(
            CELLD_ADAPTER_IMPL
                .preflight_deploy(&AdapterDeployContext::default(), &[])
                .expect_err("deploy preflight fails closed")
                .contains("unsupported")
        );
    }

    #[test]
    fn provisioning_fails_instead_of_succeeding_as_a_noop() {
        let empty = ProvisionStores {
            kv: &[],
            config: &[],
            secrets: &[],
        };
        assert!(
            CELLD_ADAPTER_IMPL
                .provision(Path::new("."), None, None, &empty, false)
                .expect_err("provision unsupported")
                .contains("does not provision")
        );
    }

    #[cfg(unix)]
    #[test]
    fn serve_builds_first_then_starts_celld_with_loopback_only() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt as _;

        let temp = tempfile::tempdir().expect("temp dir");
        let bin = temp.path().join("bin");
        let app = temp.path().join("app");
        fs::create_dir_all(&bin).expect("bin directory");
        fs::create_dir_all(&app).expect("app directory");
        let manifest = app.join("wrangler.json");
        fs::write(
            &manifest,
            r#"{"name":"test-celld","main":"build/worker/shim.mjs"}"#,
        )
        .expect("manifest");
        let worker_build = bin.join("worker-build");
        fs::write(
            &worker_build,
            "#!/bin/sh\nprintf build > build-step\nmkdir -p build/worker\nprintf shim > build/worker/shim.mjs\n",
        )
        .expect("fake worker-build");
        let celld = bin.join("celld");
        fs::write(
            &celld,
            "#!/bin/sh\ntest -f build-step || exit 3\nprintf '%s\\n' \"$*\" > celld-args\n",
        )
        .expect("fake celld");
        for executable in [&worker_build, &celld] {
            let mut permissions = fs::metadata(executable)
                .expect("executable metadata")
                .permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(executable, permissions).expect("make executable");
        }

        serve_with_programs(
            &manifest,
            &["--port".to_owned(), "9877".to_owned()],
            worker_build.as_os_str(),
            celld.as_os_str(),
        )
        .expect("serve through fake local tools");

        assert_eq!(
            fs::read_to_string(app.join("build-step")).expect("build marker"),
            "build"
        );
        let celld_args = fs::read_to_string(app.join("celld-args")).expect("celld args");
        assert!(celld_args.starts_with("dev "));
        assert!(celld_args.contains("--port 9877 --host 127.0.0.1"));
        assert!(app.join("build/worker/shim.mjs").is_file());
    }

    #[cfg(unix)]
    #[test]
    fn public_host_rejection_happens_before_build_or_serve() {
        use std::fs;

        let temp = tempfile::tempdir().expect("temp dir");
        let manifest = temp.path().join("wrangler.json");
        fs::write(&manifest, "{}\n").expect("manifest");
        let error = serve_from_manifest(&manifest, &["--host".to_owned(), "0.0.0.0".to_owned()])
            .expect_err("public host rejected");
        assert!(error.contains("only permits loopback"));
        assert!(!temp.path().join("build/worker/shim.mjs").exists());
    }

    #[test]
    fn blueprint_uses_worker_build_and_default_bundling_manifest() {
        assert_eq!(CELLD_BLUEPRINT.manifest.manifest_filename, "wrangler.json");
        let cargo = include_str!("templates/Cargo.toml.hbs")
            .replace("{{proj_celld}}", "test-celld")
            .replace("{{proj_core}}", "test-core")
            .replace(
                "{{{dep_edgezero_adapter_celld}}}",
                "edgezero-adapter-celld = { path = \"../../crates/edgezero-adapter-celld\" }",
            );
        assert!(cargo.contains("test-core = { path = \"../test-core\" }"));
        assert!(CELLD_BLUEPRINT.commands.build.is_empty());
        assert!(CELLD_BLUEPRINT.commands.deploy.is_empty());
        assert!(CELLD_BLUEPRINT.commands.serve.is_empty());
        let rendered =
            include_str!("templates/wrangler.json.hbs").replace("{{proj_celld}}", "test-celld");
        let manifest: serde_json::Value =
            serde_json::from_str(&rendered).expect("valid rendered JSON manifest");
        assert_eq!(manifest["main"], "build/worker/shim.mjs");
        assert_eq!(manifest["no_bundle"], false);
    }
}
