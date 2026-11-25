//! Host helpers shared with application-specific secret commands.

pub use edgezero_adapter::registry::{AdapterDeployContext, DeployStoreIds};
pub use edgezero_adapter_fastly::cli::secrets::{
    FASTLY_SECRET_MAX_BYTES, FastlySecretResources, FastlySecretResponse, FastlySecretTransport,
    FastlyStoreResource, SecretWriteAction, resolve_resources, resolve_resources_with_transport,
    secret_exists, secret_exists_with_transport, write_secret, write_secret_with_transport,
};

use edgezero_core::manifest::ManifestLoader;
use std::path::Path;

/// Build the same manifest-selected paths, logical stores and variable defaults
/// used by deployment. The caller supplies its effective application manifest
/// path, including any existing `EDGEZERO_MANIFEST` selection.
///
/// # Errors
/// Returns an error for invalid manifests, missing Fastly configuration or
/// adapter manifest paths rejected by the lifecycle resolver.
#[inline]
pub fn context_from_manifest(
    path: &Path,
    service_id: Option<&str>,
) -> Result<AdapterDeployContext, String> {
    let loader = ManifestLoader::from_path(path).map_err(|error| {
        // Parser errors can contain source lines, including inline values.
        format!(
            "failed to load application manifest {} for Fastly secret operations ({:?})",
            path.display(),
            error.kind()
        )
    })?;
    crate::ensure_adapter_defined("fastly", Some(&loader))?;
    let application_manifest_path = crate::checked_application_manifest_path(path)?;
    let (context, path_error) =
        crate::deploy_context_from_loader(Some(&loader), "fastly", Some(application_manifest_path));
    if let Some(error) = path_error {
        return Err(error);
    }
    Ok(AdapterDeployContext {
        service_id: service_id.map(str::to_owned),
        ..context
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn manifest_errors_name_the_path_without_exposing_parser_source() {
        let dir = tempfile::tempdir().expect("directory");
        let manifest = dir.path().join("selected-edgezero.toml");
        let missing = context_from_manifest(&manifest, None).expect_err("missing manifest");
        assert!(missing.contains(&manifest.display().to_string()));
        assert!(missing.contains("NotFound"));
        fs::write(
            &manifest,
            "[app]\nname = \"fixture\"\nprivate = \"private-canary-source\" trailing\n",
        )
        .expect("invalid manifest");
        let error = context_from_manifest(&manifest, None).expect_err("malformed manifest");
        assert!(error.contains(&manifest.display().to_string()));
        assert!(error.contains("InvalidData"));
        assert!(!error.contains("private-canary") && !error.contains("trailing"));
    }

    #[test]
    fn shared_context_defers_adapter_path_errors_for_deploy_but_secret_helpers_reject() {
        let dir = tempfile::tempdir().expect("directory");
        let manifest = dir.path().join("edgezero.toml");
        fs::write(&manifest, "[app]\nname = \"fixture\"\n[adapters.fastly.adapter]\ncrate = \"fixture\"\nmanifest = \"missing-fastly.toml\"\n")
            .expect("manifest");
        let loader = ManifestLoader::from_path(&manifest).expect("loaded manifest");
        let (context, path_error) = crate::deploy_context_from_loader(
            Some(&loader),
            "fastly",
            Some(manifest.canonicalize().expect("path")),
        );
        assert!(context.adapter_manifest_path.is_none());
        let expected = path_error.expect("deferred deploy error");
        assert!(expected.contains("missing-fastly.toml"));
        let error = context_from_manifest(&manifest, None)
            .expect_err("secret helpers require adapter path");
        assert_eq!(error, expected);
    }

    #[test]
    fn context_uses_selected_manifest_paths_stores_and_adapter_defaults() {
        let dir = tempfile::tempdir().expect("directory");
        let adapter = dir.path().join("selected-fastly.toml");
        fs::write(&adapter, "name = \"fictional\"").expect("adapter manifest");
        let manifest = dir.path().join("selected-edgezero.toml");
        fs::write(
            &manifest,
            r#"
[app]
name = "fictional"
[adapters.fastly.adapter]
crate = "fictional-fastly"
manifest = "selected-fastly.toml"
[stores.config]
ids = ["fictional_config"]
[stores.kv]
ids = ["fictional_kv"]
[stores.secrets]
ids = ["fictional_secrets"]
[[environment.variables]]
name = "SECRETS"
env = "EDGEZERO__STORES__SECRETS__FICTIONAL_SECRETS__NAME"
value = "physical-secrets"
adapters = ["fastly"]
"#,
        )
        .expect("application manifest");
        let context = context_from_manifest(&manifest, Some("FictionalService")).expect("context");
        assert_eq!(
            context.adapter_manifest_path,
            Some(adapter.canonicalize().expect("path"))
        );
        assert_eq!(
            context.application_manifest_path,
            Some(manifest.canonicalize().expect("path"))
        );
        let loader = ManifestLoader::from_path(&manifest).expect("loaded manifest");
        let (mut deploy_context, path_error) = crate::deploy_context_from_loader(
            Some(&loader),
            "fastly",
            Some(manifest.canonicalize().expect("path")),
        );
        assert!(path_error.is_none());
        deploy_context.service_id = Some("FictionalService".to_owned());
        assert_eq!(
            context, deploy_context,
            "secret helpers use deployment-selected context"
        );
        assert_eq!(context.service_id.as_deref(), Some("FictionalService"));
        assert_eq!(context.stores.secrets, ["fictional_secrets"]);
        assert_eq!(context.stores.config, ["fictional_config"]);
        assert_eq!(context.stores.kv, ["fictional_kv"]);
        assert_eq!(
            context
                .variable_defaults
                .get("EDGEZERO__STORES__SECRETS__FICTIONAL_SECRETS__NAME")
                .map(String::as_str),
            Some("physical-secrets")
        );
    }
}
