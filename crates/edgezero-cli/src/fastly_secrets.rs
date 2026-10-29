//! Host helpers shared with application-specific secret commands.

pub use edgezero_adapter::registry::{AdapterDeployContext, DeployStoreIds};
pub use edgezero_adapter_fastly::cli::secrets::*;

use edgezero_core::manifest::ManifestLoader;
use std::path::{Path, PathBuf};

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
    let loader = ManifestLoader::from_path(path).map_err(|_error| {
        "failed to load application manifest for Fastly secret operations".to_owned()
    })?;
    crate::ensure_adapter_defined("fastly", Some(&loader))?;
    let stores = &loader.manifest().stores;
    Ok(AdapterDeployContext {
        adapter_manifest_path: crate::resolve_adapter_manifest_path(Some(&loader), "fastly")?
            .map(PathBuf::from),
        application_manifest_path: Some(
            path.canonicalize()
                .map_err(|_error| "failed to resolve application manifest path".to_owned())?,
        ),
        service_id: service_id.map(str::to_owned),
        stores: DeployStoreIds {
            config: stores
                .config
                .as_ref()
                .map(|decl| decl.ids.clone())
                .unwrap_or_default(),
            kv: stores
                .kv
                .as_ref()
                .map(|decl| decl.ids.clone())
                .unwrap_or_default(),
            secrets: stores
                .secrets
                .as_ref()
                .map(|decl| decl.ids.clone())
                .unwrap_or_default(),
        },
        variable_defaults: crate::manifest_variable_defaults(loader.manifest(), "fastly"),
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

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
