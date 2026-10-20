#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "the managed deployment planning and execution state machine is kept as one cohesive block"
)]

const FASTLY_API_TOKEN_ENV: &str = "FASTLY_API_TOKEN";
/// Env var carrying the default Fastly service id, used when
/// `--service-id` is not passed explicitly.
pub(super) const FASTLY_SERVICE_ID_ENV: &str = "FASTLY_SERVICE_ID";

/// Bound every Fastly API call so an outage or a stalled connection cannot hang the
/// job — potentially until the surrounding workflow timeout, hours later — during a
/// time-sensitive operation like a rollback. `curl` exits 28 when either limit is
/// hit, which `curl_config_capture` turns into an explicit timeout error.
const FASTLY_API_CONNECT_TIMEOUT_SECS: u64 = 10;
const FASTLY_API_MAX_TIME_SECS: u64 = 30;
/// curl's exit code for an operation that exceeded `--connect-timeout`/`--max-time`.
const CURL_EXIT_TIMEOUT: i32 = 28;

use super::FASTLY_INSTALL_HINT;
use super::provision_cloud::SelectedFastlyService;
use super::push_cloud::{redact_stderr, strict_stdout};
use super::run::{find_fastly_manifest, has_non_interactive};
use edgezero_adapter::registry::{AdapterDeployContext, AdapterExecContext, DeployStoreIds};
use edgezero_adapter::release::{VerifiedApplicationRelease, verify_application_release};
use edgezero_core::env_config::{EnvConfig, merge_env_defaults};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::env;
use std::fmt::{self, Write as _};
use std::fs;
use std::io::{ErrorKind, Write as _};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::process::{ChildStdin, Command, Stdio};
use std::thread;
use std::time::Duration;

/// Non-targeting Fastly CLI global booleans accepted by the managed deploy
/// path. Lifecycle-owned `--autoclone` is deliberately absent.
const MANAGED_DEPLOY_GLOBAL_BOOL_FLAGS: &[&str] = &[
    "--accept-defaults",
    "-d",
    "--auto-yes",
    "-y",
    "--debug-mode",
    "--non-interactive",
    "-i",
    "--quiet",
    "-q",
    "--verbose",
    "-v",
];

/// Version-scoped logging endpoint collections exposed by the Fastly API.
/// `service logging debug` streams endpoint errors and is not a collection.
const FASTLY_LOGGING_PROVIDER_KINDS: &[&str] = &[
    "azureblob",
    "bigquery",
    "cloudfiles",
    "datadog",
    "digitalocean",
    "elasticsearch",
    "ftp",
    "gcs",
    "pubsub",
    "grafanacloudlogs",
    "heroku",
    "honeycomb",
    "https",
    "kafka",
    "kinesis",
    "logentries",
    "loggly",
    "logshuttle",
    "newrelic",
    "newrelicotlp",
    "openstack",
    "papertrail",
    "s3",
    "scalyr",
    "sftp",
    "splunk",
    "sumologic",
    "syslog",
];

#[derive(Debug, Eq, PartialEq)]
pub(super) struct ReleaseManagedDeployArgs {
    comment: Option<String>,
    globals: Vec<String>,
}

pub(super) struct FastlyApiToken {
    value: String,
    env: Vec<(String, String)>,
}

impl fmt::Debug for FastlyApiToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FastlyApiToken([REDACTED])")
    }
}

impl FastlyApiToken {
    fn as_str(&self) -> &str {
        &self.value
    }
}

#[derive(Clone, Debug, serde::Deserialize, Eq, PartialEq)]
struct ServiceVersionRecord {
    #[serde(alias = "Active")]
    active: bool,
    #[serde(alias = "Environments")]
    environments: Vec<ServiceEnvironmentRecord>,
    #[serde(alias = "Locked")]
    locked: bool,
    #[serde(alias = "Number")]
    number: u64,
}

#[derive(Clone, Debug, serde::Deserialize, Eq, PartialEq)]
struct ServiceEnvironmentRecord {
    #[serde(alias = "ServiceVersion")]
    active_version: u64,
    #[serde(alias = "Name")]
    name: String,
    #[serde(alias = "ServiceID")]
    service_id: String,
}

#[derive(Debug, serde::Deserialize)]
struct ComputePackageRecord {
    metadata: ComputePackageMetadata,
    service_id: String,
    version: u64,
}

#[derive(Debug, serde::Deserialize)]
struct ComputePackageMetadata {
    files_hash: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VersionSource {
    Active(u64),
    InitialDraft(u64),
    Retired(u64),
    Staging(u64),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublishTarget {
    Production,
    Staging,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StagingRollbackDecision {
    Deactivate,
    NoopDraft,
}

#[derive(Debug)]
struct InitialDraftSnapshot {
    configuration: VersionConfigurationSnapshot,
    links: serde_json::Value,
    metadata: serde_json::Value,
    version: ServiceVersionRecord,
}

#[derive(Debug)]
struct InactiveSourceSnapshot {
    links: serde_json::Value,
    metadata: serde_json::Value,
    version: ServiceVersionRecord,
}

#[derive(Clone, Debug, PartialEq)]
struct LoggingProviderSnapshot {
    endpoints: serde_json::Value,
    kind: &'static str,
}

#[derive(Clone, Debug, PartialEq)]
struct VersionConfigurationSnapshot {
    backends: serde_json::Value,
    domains: serde_json::Value,
    healthchecks: serde_json::Value,
    logging: Vec<LoggingProviderSnapshot>,
    settings: serde_json::Value,
}

#[derive(Debug)]
enum EditableVersionSource {
    CloneActive { active_version: u64 },
    CloneRetired(Box<InactiveSourceSnapshot>),
    CloneStaging(Box<InactiveSourceSnapshot>),
    InitialDraft(Box<InitialDraftSnapshot>),
}

#[derive(Debug)]
struct ManagedDeployPlan {
    arguments: ReleaseManagedDeployArgs,
    child_env: Vec<(String, String)>,
    command_dir: tempfile::TempDir,
    links: LinkReconciliation,
    package_files_hash: String,
    package_sha256: String,
    release: VerifiedApplicationRelease,
    service_id: String,
    source_configuration: VersionConfigurationSnapshot,
    source_links: Vec<ExistingResourceLink>,
    target: PublishTarget,
    token: FastlyApiToken,
    version_source: EditableVersionSource,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum ResourceKind {
    Config,
    Kv,
    Secret,
}

impl ResourceKind {
    fn display_name(self) -> &'static str {
        match self {
            Self::Config => "Config Store",
            Self::Kv => "KV Store",
            Self::Secret => "Secret Store",
        }
    }

    fn selector_kind(self) -> &'static str {
        match self {
            Self::Config => "config",
            Self::Kv => "kv",
            Self::Secret => "secrets",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DesiredResourceLink {
    alias: String,
    kind: ResourceKind,
    resource_id: String,
    selected_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExistingResourceLink {
    alias: String,
    kind: ResourceKind,
    link_id: String,
    resource_id: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct LinkReconciliation {
    create: Vec<DesiredResourceLink>,
    delete_link_ids: Vec<String>,
}

#[derive(Clone, Debug)]
struct StoreInventory {
    by_name: BTreeMap<String, String>,
}

#[derive(serde::Deserialize)]
struct PaginatedStoreInventoryPage {
    #[serde(alias = "Data")]
    data: Vec<PaginatedStoreInventoryRecord>,
    #[serde(alias = "Meta")]
    meta: PaginatedStoreInventoryMeta,
}

#[derive(serde::Deserialize)]
struct PaginatedStoreInventoryRecord {
    #[serde(alias = "StoreID")]
    id: String,
    #[serde(alias = "Name")]
    name: String,
}

#[derive(serde::Deserialize)]
struct PaginatedStoreInventoryMeta {
    next_cursor: Option<String>,
}

#[derive(Clone, Debug)]
struct ResourceInventories {
    config: StoreInventory,
    kind_by_resource_id: BTreeMap<String, ResourceKind>,
    kv: StoreInventory,
    secret: StoreInventory,
}

impl ResourceInventories {
    fn for_kind(&self, kind: ResourceKind) -> &StoreInventory {
        match kind {
            ResourceKind::Config => &self.config,
            ResourceKind::Kv => &self.kv,
            ResourceKind::Secret => &self.secret,
        }
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the pure JSON constructor is retained for clean-cutover planner tests"
        )
    )]
    fn from_json(raw_config: &str, raw_kv: &str, raw_secret: &str) -> Result<Self, String> {
        let config = parse_store_inventory(ResourceKind::Config, raw_config)?;
        let kv = parse_store_inventory(ResourceKind::Kv, raw_kv)?;
        let secret = parse_store_inventory(ResourceKind::Secret, raw_secret)?;
        Self::from_inventories(config, kv, secret)
    }

    fn from_inventories(
        config: StoreInventory,
        kv: StoreInventory,
        secret: StoreInventory,
    ) -> Result<Self, String> {
        let mut kind_by_resource_id = BTreeMap::new();
        for (kind, inventory) in [
            (ResourceKind::Config, &config),
            (ResourceKind::Kv, &kv),
            (ResourceKind::Secret, &secret),
        ] {
            for resource_id in inventory.by_name.values() {
                if let Some(prior_kind) = kind_by_resource_id.insert(resource_id.clone(), kind) {
                    return Err(format!(
                        "Fastly resource id `{resource_id}` appears in both {} and {} inventories; resource kind is ambiguous",
                        prior_kind.display_name(),
                        kind.display_name()
                    ));
                }
            }
        }
        Ok(Self {
            config,
            kind_by_resource_id,
            kv,
            secret,
        })
    }

    fn resolve(&self, kind: ResourceKind, name: &str) -> Result<String, String> {
        self.for_kind(kind)
            .by_name
            .get(name)
            .cloned()
            .ok_or_else(|| {
                format!(
                    "selected Fastly {} `{name}` does not exist in the complete provider inventory",
                    kind.display_name()
                )
            })
    }
}

fn parse_store_inventory(kind: ResourceKind, raw: &str) -> Result<StoreInventory, String> {
    let parsed: serde_json::Value = serde_json::from_str(raw).map_err(|error| {
        format!(
            "failed to parse Fastly {} inventory as JSON: {error}",
            kind.display_name()
        )
    })?;
    let rows = parsed.as_array().ok_or_else(|| {
        format!(
            "Fastly {} inventory must be one complete bare JSON array; paginated or enveloped results are not accepted",
            kind.display_name()
        )
    })?;
    let mut records = Vec::with_capacity(rows.len());
    for (index, row) in rows.iter().enumerate() {
        let object = row.as_object().ok_or_else(|| {
            format!(
                "Fastly {} inventory record #{index} is not an object",
                kind.display_name()
            )
        })?;
        let field = |name| {
            object
                .get(name)
                .and_then(serde_json::Value::as_str)
                .filter(|field_value| !field_value.is_empty())
        };
        let (Some(name), Some(id)) = (field("name"), field("id")) else {
            return Err(format!(
                "Fastly {} inventory record #{index} requires non-empty string `name` and `id` fields",
                kind.display_name()
            ));
        };
        records.push((name.to_owned(), id.to_owned()));
    }
    store_inventory_from_records(kind, records)
}

fn store_inventory_from_records(
    kind: ResourceKind,
    records: impl IntoIterator<Item = (String, String)>,
) -> Result<StoreInventory, String> {
    let mut by_name = BTreeMap::new();
    let mut ids = BTreeSet::new();
    for (index, (name, id)) in records.into_iter().enumerate() {
        if id.is_empty()
            || !id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        {
            return Err(format!(
                "Fastly {} inventory record #{index} has invalid resource id",
                kind.display_name()
            ));
        }
        if name.is_empty()
            || name.chars().all(char::is_whitespace)
            || name.chars().any(char::is_control)
        {
            return Err(format!(
                "Fastly {} inventory record #{index} has invalid store name",
                kind.display_name()
            ));
        }
        if by_name.insert(name.clone(), id.clone()).is_some() {
            return Err(format!(
                "Fastly {} inventory contains duplicate name `{name}`",
                kind.display_name()
            ));
        }
        if !ids.insert(id.clone()) {
            return Err(format!(
                "Fastly {} inventory contains duplicate resource id `{id}`",
                kind.display_name()
            ));
        }
    }
    Ok(StoreInventory { by_name })
}

fn percent_encode_query_value(value: &str) -> Result<String, String> {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            write!(encoded, "%{byte:02X}")
                .map_err(|error| format!("failed to encode Fastly pagination cursor: {error}"))?;
        }
    }
    Ok(encoded)
}

fn collect_paginated_store_inventory(
    kind: ResourceKind,
    first_path: &str,
    mut fetch: impl FnMut(&str) -> Result<String, String>,
) -> Result<StoreInventory, String> {
    let mut path = first_path.to_owned();
    let mut seen_cursors = BTreeSet::new();
    let mut records = Vec::new();
    loop {
        let raw = fetch(&path)?;
        let page: PaginatedStoreInventoryPage = serde_json::from_str(&raw).map_err(|_error| {
            format!(
                "Fastly {} inventory page has an invalid paginated response shape (payload redacted)",
                kind.display_name()
            )
        })?;
        records.extend(page.data.into_iter().map(|record| (record.name, record.id)));
        let Some(cursor) = page.meta.next_cursor.filter(|cursor| !cursor.is_empty()) else {
            break;
        };
        if !seen_cursors.insert(cursor.clone()) {
            return Err(format!(
                "Fastly {} inventory repeated a pagination cursor; completeness cannot be proven",
                kind.display_name()
            ));
        }
        path = format!(
            "{first_path}&cursor={}",
            percent_encode_query_value(&cursor)?
        );
    }
    store_inventory_from_records(kind, records)
}

fn fetch_complete_paginated_store_inventory(
    kind: ResourceKind,
    endpoint: &str,
    token: &FastlyApiToken,
) -> Result<StoreInventory, String> {
    let first_path = format!("{endpoint}?limit=100");
    collect_paginated_store_inventory(kind, &first_path, |path| fastly_api_get(path, token))
}

fn desired_resource_links(
    stores: &RuntimeStoreIds,
    environment: &EnvConfig,
    inventories: &ResourceInventories,
) -> Result<Vec<DesiredResourceLink>, String> {
    let mut desired_by_identity = BTreeMap::<(ResourceKind, String), DesiredResourceLink>::new();

    for (kind, logical_ids) in [
        (ResourceKind::Config, &stores.config),
        (ResourceKind::Kv, &stores.kv),
        (ResourceKind::Secret, &stores.secrets),
    ] {
        for logical_id in logical_ids {
            let selected_name = environment
                .store_name_checked(kind.selector_kind(), logical_id)
                .map_err(|error| format!("invalid Fastly deploy environment: {error}"))?;
            let resource_id = inventories.resolve(kind, &selected_name)?;
            let desired = DesiredResourceLink {
                kind,
                alias: logical_id.clone(),
                selected_name: selected_name.clone(),
                resource_id,
            };
            let identity = (kind, logical_id.clone());
            if desired_by_identity.insert(identity, desired).is_some() {
                return Err(format!(
                    "Fastly {} logical store id `{logical_id}` is declared more than once",
                    kind.display_name()
                ));
            }
        }
    }
    Ok(desired_by_identity.into_values().collect())
}

fn plan_link_reconciliation(
    desired: &[DesiredResourceLink],
    existing: &[ExistingResourceLink],
    inventories: &ResourceInventories,
) -> Result<LinkReconciliation, String> {
    // Fastly exposes Config, KV, and Secret Stores through separate typed
    // runtime APIs, so a resource-link identity is `(kind, alias)`. Reusing an
    // alias across kinds is valid; two links with the same kind and alias are
    // ambiguous to EdgeZero and fail closed below.
    let mut desired_identities = BTreeSet::new();
    for link in desired {
        let identity = (link.kind, link.alias.as_str());
        if !desired_identities.insert(identity) {
            return Err(format!(
                "desired Fastly {} resource-link alias `{}` is duplicated",
                link.kind.display_name(),
                link.alias
            ));
        }
    }
    let mut existing_by_identity = BTreeMap::new();
    let mut existing_ids = BTreeSet::new();
    for link in existing {
        if !existing_ids.insert(link.link_id.as_str()) {
            return Err(format!(
                "Fastly resource-link inventory contains duplicate link id `{}`",
                link.link_id
            ));
        }
        match inventories.kind_by_resource_id.get(&link.resource_id) {
            Some(kind) if *kind == link.kind => {}
            Some(kind) => {
                return Err(format!(
                    "Fastly resource link `{}` reports {} but resource `{}` belongs to {}",
                    link.alias,
                    link.kind.display_name(),
                    link.resource_id,
                    kind.display_name()
                ));
            }
            // Account inventory visibility can be narrower than the resource
            // links inherited by this service version. The link response is
            // authoritative for an undeclared inherited link, which must
            // survive reconciliation even when the token cannot list its
            // physical resource.
            None => {}
        }
        let identity = (link.kind, link.alias.as_str());
        if existing_by_identity.insert(identity, link).is_some() {
            return Err(format!(
                "Fastly resource-link inventory contains duplicate {} alias `{}`",
                link.kind.display_name(),
                link.alias
            ));
        }
    }

    let mut create = Vec::new();
    let mut delete_link_ids = Vec::new();
    for desired_link in desired {
        let identity = (desired_link.kind, desired_link.alias.as_str());
        match existing_by_identity.get(&identity) {
            Some(existing_link) if existing_link.resource_id == desired_link.resource_id => {}
            Some(existing_link) => {
                delete_link_ids.push(existing_link.link_id.clone());
                create.push(desired_link.clone());
            }
            None => create.push(desired_link.clone()),
        }
    }

    delete_link_ids.sort();
    delete_link_ids.dedup();
    create.sort_by(|left, right| (left.kind, &left.alias).cmp(&(right.kind, &right.alias)));
    Ok(LinkReconciliation {
        create,
        delete_link_ids,
    })
}

fn managed_startup_directory(root: &Path, manifest: &Path) -> Result<tempfile::TempDir, String> {
    let command_dir = tempfile::tempdir().map_err(|error| error.to_string())?;
    let canonical_root = root.canonicalize().map_err(|error| error.to_string())?;
    if command_dir
        .path()
        .canonicalize()
        .map_err(|error| error.to_string())?
        .starts_with(canonical_root)
    {
        return Err("TMPDIR must be outside the immutable application release".to_owned());
    }
    fs::copy(manifest, command_dir.path().join("fastly.toml"))
        .map_err(|error| format!("prepare private Fastly startup manifest: {error}"))?;
    Ok(command_dir)
}

struct FastlyCommandContext<'ctx> {
    cwd: &'ctx Path,
    env: &'ctx [(String, String)],
}

#[expect(
    clippy::too_many_lines,
    reason = "the read-only checkpoint intentionally assembles every validated input into one reviewable plan"
)]
fn build_managed_deploy_plan(
    context: &AdapterDeployContext,
    args: &[String],
    exec: &AdapterExecContext<'_>,
) -> Result<ManagedDeployPlan, String> {
    let arguments = parse_release_managed_deploy_args(args)?;
    let target = if context.staging {
        PublishTarget::Staging
    } else {
        PublishTarget::Production
    };
    let environment = effective_deploy_environment(context, exec)?;
    let release_root = context
        .application_release_root
        .as_deref()
        .ok_or_else(|| "managed Fastly deployment requires --application-release".to_owned())?;
    let application_manifest = context
        .application_manifest_path
        .as_deref()
        .ok_or_else(|| {
            "managed Fastly deployment requires the exact loaded application manifest path"
                .to_owned()
        })?;
    let adapter_manifest = context.adapter_manifest_path.as_deref().ok_or_else(|| {
        "managed Fastly deployment requires the exact referenced Fastly manifest path".to_owned()
    })?;
    let release = verify_application_release(
        release_root,
        application_manifest,
        adapter_manifest,
        "fastly",
        1,
    )?;
    let service_id = resolve_managed_plan_service_id(context, &release, exec)?;
    let token = require_token_with_exec(exec)?;
    if token.as_str().is_empty() {
        return Err(format!(
            "{FASTLY_API_TOKEN_ENV} must be non-empty in the environment"
        ));
    }

    let stores = RuntimeStoreIds::from(&context.stores);
    for logical_id in &stores.config {
        let key = environment
            .store_key_checked(ResourceKind::Config.selector_kind(), logical_id)
            .map_err(|error| format!("invalid Fastly deploy environment: {error}"))?;
        validate_fastly_config_key(logical_id, &key, target == PublishTarget::Staging, false)?;
    }
    let command_dir = managed_startup_directory(release_root, release.adapter_manifest())?;
    let command_context = FastlyCommandContext {
        cwd: command_dir.path(),
        env: exec.env(),
    };
    let cwd = &command_context;
    let package_files_hash = compute_package_files_hash(release.package(), cwd, &token)?;

    let raw_config_inventory = run_fastly_json_capture(&["config-store", "list", "--json"], cwd)?;
    let config_inventory = parse_store_inventory(ResourceKind::Config, &raw_config_inventory)?;
    let kv_inventory =
        fetch_complete_paginated_store_inventory(ResourceKind::Kv, "/resources/stores/kv", &token)?;
    let secret_inventory = fetch_complete_paginated_store_inventory(
        ResourceKind::Secret,
        "/resources/stores/secret",
        &token,
    )?;
    let inventories =
        ResourceInventories::from_inventories(config_inventory, kv_inventory, secret_inventory)?;
    let desired_links = desired_resource_links(&stores, &environment, &inventories)?;

    let versions_raw = fastly_api_get(&format!("/service/{service_id}/version"), &token)?;
    let versions = parse_service_versions(&versions_raw)?;
    let selected_source = select_version_source(&versions)?;
    let source_version = match selected_source {
        VersionSource::Active(version)
        | VersionSource::InitialDraft(version)
        | VersionSource::Retired(version)
        | VersionSource::Staging(version) => version,
    };
    let links_raw = run_fastly_json_capture(
        &[
            "service",
            "resource-link",
            "list",
            &format!("--service-id={service_id}"),
            &format!("--version={source_version}"),
            "--json",
        ],
        cwd,
    )?;
    let (source_links, links_snapshot) = parse_resource_links(&links_raw)?;
    let source_configuration =
        read_version_configuration_snapshot_for(&service_id, &token, source_version)?;

    let links = plan_link_reconciliation(&desired_links, &source_links, &inventories)?;

    let version_source = match selected_source {
        VersionSource::Active(active_version) => {
            EditableVersionSource::CloneActive { active_version }
        }
        VersionSource::InitialDraft(draft_version) => {
            let version = versions
                .iter()
                .find(|version| version.number == draft_version)
                .cloned()
                .ok_or_else(|| {
                    format!("selected initial draft version {draft_version} disappeared")
                })?;
            let metadata = exact_version_metadata(&versions_raw, draft_version)?;
            EditableVersionSource::InitialDraft(Box::new(snapshot_initial_draft(
                version,
                metadata,
                links_snapshot,
                source_configuration.clone(),
            )))
        }
        VersionSource::Retired(inactive_version) | VersionSource::Staging(inactive_version) => {
            let version = versions
                .iter()
                .find(|version| version.number == inactive_version)
                .cloned()
                .ok_or_else(|| {
                    format!("selected inactive version {inactive_version} disappeared")
                })?;
            let metadata = exact_version_metadata(&versions_raw, inactive_version)?;
            let snapshot = Box::new(InactiveSourceSnapshot {
                links: links_snapshot,
                metadata,
                version,
            });
            if matches!(selected_source, VersionSource::Retired(_)) {
                EditableVersionSource::CloneRetired(snapshot)
            } else {
                EditableVersionSource::CloneStaging(snapshot)
            }
        }
    };
    let package_sha256 = release.package_sha256().to_owned();

    Ok(ManagedDeployPlan {
        command_dir,
        child_env: exec.env().to_vec(),
        arguments,
        links,
        package_files_hash,
        package_sha256,
        release,
        service_id,
        source_configuration,
        source_links,
        target,
        token,
        version_source,
    })
}

pub(super) fn deploy_managed_with_context(
    context: &AdapterDeployContext,
    args: &[String],
    exec: &AdapterExecContext<'_>,
) -> Result<(), String> {
    let plan = build_managed_deploy_plan(context, args, exec)?;
    execute_managed_deploy_plan(&plan)
}

fn execute_managed_deploy_plan(plan: &ManagedDeployPlan) -> Result<(), String> {
    execute_managed_deploy_plan_with_emit(plan, &mut |line| log::info!("{line}"))
}

fn execute_managed_deploy_plan_with_emit(
    plan: &ManagedDeployPlan,
    emit: &mut dyn FnMut(&str),
) -> Result<(), String> {
    let command_context = FastlyCommandContext {
        cwd: plan.command_dir.path(),
        env: &plan.child_env,
    };
    let cwd = &command_context;
    emit(&format!("package-sha256={}", plan.package_sha256));
    let version = prepare_managed_version(plan, cwd, emit)?;

    if let Some(comment) = plan.arguments.comment.as_deref() {
        run_fastly_status(
            &[
                "service".to_owned(),
                "version".to_owned(),
                "update".to_owned(),
                format!("--service-id={}", plan.service_id),
                format!("--version={version}"),
                format!("--comment={comment}"),
            ],
            cwd,
        )?;
    }

    let inherited = read_version_links(&plan.service_id, version, cwd)?;
    require_same_link_resources(&plan.source_links, &inherited, "inherited draft")?;
    let delete_identities = planned_delete_identities(plan)?;
    let expected_links = expected_final_link_resources(plan, &delete_identities)?;
    let inherited_by_identity = links_by_identity(&inherited)?;
    for identity in &delete_identities {
        let link = inherited_by_identity.get(identity).ok_or_else(|| {
            format!(
                "planned stale Fastly {} resource link `{}` is absent from the draft",
                identity.0.display_name(),
                identity.1
            )
        })?;
        run_fastly_status(
            &[
                "service".to_owned(),
                "resource-link".to_owned(),
                "delete".to_owned(),
                format!("--service-id={}", plan.service_id),
                format!("--version={version}"),
                format!("--id={}", link.link_id),
            ],
            cwd,
        )?;
    }
    for link in &plan.links.create {
        run_fastly_status(
            &[
                "service".to_owned(),
                "resource-link".to_owned(),
                "create".to_owned(),
                format!("--service-id={}", plan.service_id),
                format!("--version={version}"),
                format!("--resource-id={}", link.resource_id),
                format!("--name={}", link.alias),
            ],
            cwd,
        )?;
    }
    let reconciled = read_version_links(&plan.service_id, version, cwd)?;
    require_exact_link_resources(&expected_links, &reconciled, "reconciled draft")?;

    // Capture the versioned Compute configuration only after EdgeZero has
    // finished every intended mutation, then compare it again in the immediate
    // publication barrier below. Package identity and resource links have
    // dedicated checks alongside this snapshot.
    let expected_configuration = read_version_configuration_snapshot(plan, version)?;
    if expected_configuration != plan.source_configuration {
        return Err(format!(
            "Fastly version {version} protected Compute configuration no longer matches its source"
        ));
    }

    revalidate_managed_draft(plan, version, &expected_links, &expected_configuration, cwd)?;

    match plan.target {
        PublishTarget::Staging => run_fastly_status(
            &[
                "service".to_owned(),
                "version".to_owned(),
                "stage".to_owned(),
                format!("--service-id={}", plan.service_id),
                format!("--version={version}"),
            ],
            cwd,
        ),
        PublishTarget::Production => fastly_api_put(
            &format!("/service/{}/version/{version}/activate", plan.service_id),
            &plan.token,
        )
        .map(|_status| ()),
    }
}

struct CapturedFastlyCommand {
    combined: String,
    status: String,
    success: bool,
}

fn prepare_managed_version(
    plan: &ManagedDeployPlan,
    cwd: &FastlyCommandContext<'_>,
    emit: &mut dyn FnMut(&str),
) -> Result<u64, String> {
    let package = plan.release.package().to_str().ok_or_else(|| {
        "verified Fastly package path is not valid UTF-8 and cannot be passed to the Fastly CLI"
            .to_owned()
    })?;
    let version = match &plan.version_source {
        EditableVersionSource::CloneActive { active_version } => {
            revalidate_active_source(plan, *active_version, cwd, None)?;
            clone_managed_source(plan, *active_version, cwd, emit)?
        }
        EditableVersionSource::CloneRetired(snapshot)
        | EditableVersionSource::CloneStaging(snapshot) => {
            revalidate_inactive_source(plan, snapshot, cwd, None)?;
            require_source_configuration(plan, snapshot.version.number)?;
            clone_managed_source(plan, snapshot.version.number, cwd, emit)?
        }
        EditableVersionSource::InitialDraft(snapshot) => {
            revalidate_initial_draft_before_update(plan, snapshot, cwd)?;
            let version = snapshot.version.number;
            emit(&format!("version={version}"));
            version
        }
    };
    let mut update = vec![
        "compute".to_owned(),
        "update".to_owned(),
        format!("--service-id={}", plan.service_id),
        format!("--version={version}"),
    ];
    update.push(format!("--package={package}"));
    update.extend(plan.arguments.globals.iter().cloned());
    if !has_non_interactive(&plan.arguments.globals) {
        update.push("--non-interactive".to_owned());
    }

    let outcome = run_fastly_capture_outcome(&update, cwd)?;
    let reported_version = parse_fastly_version(&outcome.combined);
    if !outcome.success {
        let redacted_output = outcome.combined.replace(plan.token.as_str(), "[REDACTED]");
        return Err(format!(
            "`fastly {}` exited with status {}\n{}",
            update.join(" "),
            outcome.status,
            redacted_output.trim()
        ));
    }

    if let Some(reported) = reported_version
        && reported != version
    {
        return Err(format!(
            "Fastly updated version {reported}, but the verified draft was version {version}"
        ));
    }
    Ok(version)
}

fn clone_managed_source(
    plan: &ManagedDeployPlan,
    source_version: u64,
    cwd: &FastlyCommandContext<'_>,
    emit: &mut dyn FnMut(&str),
) -> Result<u64, String> {
    let raw = fastly_api_put_capture(
        &format!(
            "/service/{}/version/{source_version}/clone",
            plan.service_id
        ),
        &plan.token,
    )?;
    let version = parse_cloned_version(&raw, &plan.service_id, source_version)?;
    emit(&format!("version={version}"));

    match &plan.version_source {
        EditableVersionSource::CloneActive { active_version } => {
            revalidate_active_source(plan, *active_version, cwd, Some(version))?;
        }
        EditableVersionSource::CloneRetired(snapshot)
        | EditableVersionSource::CloneStaging(snapshot) => {
            revalidate_inactive_source(plan, snapshot, cwd, Some(version))?;
            require_source_configuration(plan, snapshot.version.number)?;
        }
        EditableVersionSource::InitialDraft(_) => {
            return Err("internal managed deploy clone source mismatch".to_owned());
        }
    }

    let versions_raw = fastly_api_get(
        &format!("/service/{}/version", plan.service_id),
        &plan.token,
    )?;
    let versions = parse_service_versions(&versions_raw)?;
    let draft = versions
        .iter()
        .find(|candidate| candidate.number == version)
        .ok_or_else(|| format!("cloned Fastly draft version {version} is absent"))?;
    if draft.active || draft.locked || !draft.environments.is_empty() {
        return Err(format!(
            "cloned Fastly version {version} is not an unpublished editable draft"
        ));
    }
    let cloned_links = read_version_links(&plan.service_id, version, cwd)?;
    require_same_link_resources(&plan.source_links, &cloned_links, "fresh clone")?;
    let cloned_configuration = read_version_configuration_snapshot(plan, version)?;
    if cloned_configuration != plan.source_configuration {
        return Err(format!(
            "cloned Fastly version {version} does not match the preflight source configuration"
        ));
    }
    Ok(version)
}

fn parse_cloned_version(raw: &str, service_id: &str, source_version: u64) -> Result<u64, String> {
    let value: serde_json::Value = serde_json::from_str(raw)
        .map_err(|_error| "Fastly clone response is malformed".to_owned())?;
    let object = value
        .as_object()
        .ok_or_else(|| "Fastly clone response must be one JSON object".to_owned())?;
    let cloned_service = object
        .get("service_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "Fastly clone response has no service_id".to_owned())?;
    let version = object
        .get("number")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| "Fastly clone response has no numeric version".to_owned())?;
    if cloned_service != service_id || version == source_version {
        return Err(
            "Fastly clone response does not identify a new version for the requested service"
                .to_owned(),
        );
    }
    Ok(version)
}

fn run_fastly_capture_outcome(
    fastly_args: &[String],
    cwd: &FastlyCommandContext<'_>,
) -> Result<CapturedFastlyCommand, String> {
    let output = Command::new("fastly")
        .args(fastly_args)
        .current_dir(cwd.cwd)
        .envs(cwd.env.iter().map(|(key, value)| (key, value)))
        .output()
        .map_err(|error| {
            if error.kind() == ErrorKind::NotFound {
                format!("`fastly` not found on PATH; {FASTLY_INSTALL_HINT}")
            } else {
                format!("failed to run fastly CLI: {error}")
            }
        })?;
    let mut combined = String::from_utf8_lossy(&output.stdout).into_owned();
    combined.push_str(&String::from_utf8_lossy(&output.stderr));
    Ok(CapturedFastlyCommand {
        combined,
        status: output.status.to_string(),
        success: output.status.success(),
    })
}

fn is_canonical_sha512_hex(value: &str) -> bool {
    value.len() == 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn parse_package_files_hash_output(output: &str) -> Result<String, String> {
    let hashes = output
        .lines()
        .map(str::trim)
        .filter(|line| is_canonical_sha512_hex(line))
        .collect::<BTreeSet<_>>();
    if hashes.len() != 1 {
        return Err(
            "Fastly package hash command did not return one unambiguous SHA-512 files hash"
                .to_owned(),
        );
    }
    hashes.into_iter().next().map(str::to_owned).ok_or_else(|| {
        "Fastly package hash command did not return one unambiguous SHA-512 files hash".to_owned()
    })
}

fn compute_package_files_hash(
    package: &Path,
    cwd: &FastlyCommandContext<'_>,
    token: &FastlyApiToken,
) -> Result<String, String> {
    let package_path = package.to_str().ok_or_else(|| {
        "verified Fastly package path is not valid UTF-8 and cannot be hashed by the Fastly CLI"
            .to_owned()
    })?;
    let outcome = run_fastly_capture_outcome(
        &[
            "compute".to_owned(),
            "hash-files".to_owned(),
            format!("--package={package_path}"),
            "--skip-build".to_owned(),
            "--non-interactive".to_owned(),
            "--quiet".to_owned(),
        ],
        cwd,
    )?;
    if !outcome.success {
        let redacted = outcome.combined.replace(token.as_str(), "[REDACTED]");
        return Err(format!(
            "Fastly package hash command exited with status {}\n{}",
            outcome.status,
            redacted.trim()
        ));
    }
    parse_package_files_hash_output(&outcome.combined)
}

fn parse_package_metadata_files_hash(
    raw: &str,
    expected_service_id: &str,
    expected_version: u64,
) -> Result<String, String> {
    let package: ComputePackageRecord = serde_json::from_str(raw)
        .map_err(|_error| "Fastly package metadata response is malformed".to_owned())?;
    if package.service_id != expected_service_id || package.version != expected_version {
        return Err(
            "Fastly package metadata does not identify the requested service version".to_owned(),
        );
    }
    if !is_canonical_sha512_hex(&package.metadata.files_hash) {
        return Err("Fastly package metadata contains an invalid files hash".to_owned());
    }
    Ok(package.metadata.files_hash)
}

fn read_version_links(
    service_id: &str,
    version: u64,
    cwd: &FastlyCommandContext<'_>,
) -> Result<Vec<ExistingResourceLink>, String> {
    read_version_links_with_snapshot(service_id, version, cwd).map(|(links, _snapshot)| links)
}

fn read_version_links_with_snapshot(
    service_id: &str,
    version: u64,
    cwd: &FastlyCommandContext<'_>,
) -> Result<(Vec<ExistingResourceLink>, serde_json::Value), String> {
    let service_arg = format!("--service-id={service_id}");
    let version_arg = format!("--version={version}");
    let raw = run_fastly_json_capture(
        &[
            "service",
            "resource-link",
            "list",
            &service_arg,
            &version_arg,
            "--json",
        ],
        cwd,
    )?;
    parse_resource_links(&raw)
}

type ResourceLinkIdentity = (ResourceKind, String);

fn links_by_identity(
    links: &[ExistingResourceLink],
) -> Result<BTreeMap<ResourceLinkIdentity, ExistingResourceLink>, String> {
    let mut by_identity = BTreeMap::new();
    let mut ids = BTreeSet::new();
    for link in links {
        if !ids.insert(link.link_id.as_str()) {
            return Err(format!(
                "Fastly resource-link inventory contains duplicate link id `{}`",
                link.link_id
            ));
        }
        let identity = (link.kind, link.alias.clone());
        if by_identity.insert(identity, link.clone()).is_some() {
            return Err(format!(
                "Fastly resource-link inventory contains duplicate {} alias `{}`",
                link.kind.display_name(),
                link.alias
            ));
        }
    }
    Ok(by_identity)
}

fn link_resource_map(
    links: &[ExistingResourceLink],
) -> Result<BTreeMap<ResourceLinkIdentity, String>, String> {
    Ok(links_by_identity(links)?
        .into_iter()
        .map(|(identity, link)| (identity, link.resource_id))
        .collect())
}

fn require_same_link_resources(
    expected: &[ExistingResourceLink],
    actual: &[ExistingResourceLink],
    label: &str,
) -> Result<(), String> {
    let expected_resources = link_resource_map(expected)?;
    require_exact_link_resources(&expected_resources, actual, label)
}

fn require_exact_link_resources(
    expected: &BTreeMap<ResourceLinkIdentity, String>,
    actual: &[ExistingResourceLink],
    label: &str,
) -> Result<(), String> {
    let actual_resources = link_resource_map(actual)?;
    if actual_resources == *expected {
        Ok(())
    } else {
        Err(format!(
            "Fastly {label} resource links changed unexpectedly; refusing to publish"
        ))
    }
}

fn planned_delete_identities(
    plan: &ManagedDeployPlan,
) -> Result<BTreeSet<ResourceLinkIdentity>, String> {
    let by_id = plan
        .source_links
        .iter()
        .map(|link| (link.link_id.as_str(), (link.kind, link.alias.as_str())))
        .collect::<BTreeMap<_, _>>();
    plan.links
        .delete_link_ids
        .iter()
        .map(|id| {
            by_id
                .get(id.as_str())
                .map(|(kind, alias)| (*kind, (*alias).to_owned()))
                .ok_or_else(|| {
                    format!("planned Fastly resource-link deletion `{id}` has no source link")
                })
        })
        .collect()
}

fn expected_final_link_resources(
    plan: &ManagedDeployPlan,
    delete_identities: &BTreeSet<ResourceLinkIdentity>,
) -> Result<BTreeMap<ResourceLinkIdentity, String>, String> {
    let mut expected = link_resource_map(&plan.source_links)?;
    for identity in delete_identities {
        if expected.remove(identity).is_none() {
            return Err(format!(
                "planned stale Fastly {} resource-link alias `{}` has no source link",
                identity.0.display_name(),
                identity.1
            ));
        }
    }
    for link in &plan.links.create {
        let identity = (link.kind, link.alias.clone());
        if expected
            .insert(identity, link.resource_id.clone())
            .is_some()
        {
            return Err(format!(
                "planned Fastly {} resource-link creation `{}` collides with an inherited identity",
                link.kind.display_name(),
                link.alias
            ));
        }
    }
    Ok(expected)
}

fn revalidate_initial_draft_before_update(
    plan: &ManagedDeployPlan,
    snapshot: &InitialDraftSnapshot,
    cwd: &FastlyCommandContext<'_>,
) -> Result<(), String> {
    let versions_raw = fastly_api_get(
        &format!("/service/{}/version", plan.service_id),
        &plan.token,
    )?;
    let versions = parse_service_versions(&versions_raw)?;
    if select_version_source(&versions)? != VersionSource::InitialDraft(snapshot.version.number) {
        return Err("Fastly initial draft selection changed after preflight".to_owned());
    }
    let current = versions
        .iter()
        .find(|version| version.number == snapshot.version.number)
        .ok_or_else(|| "Fastly initial draft disappeared after preflight".to_owned())?;
    if current != &snapshot.version
        || exact_version_metadata(&versions_raw, snapshot.version.number)? != snapshot.metadata
    {
        return Err("Fastly initial draft metadata changed after preflight".to_owned());
    }
    let service_id = &plan.service_id;
    let version = snapshot.version.number;
    let (_current_links, links_value) = read_version_links_with_snapshot(service_id, version, cwd)?;
    if links_value != snapshot.links {
        return Err("Fastly initial draft links changed after preflight".to_owned());
    }
    require_protected_initial_snapshot(plan, snapshot)?;
    Ok(())
}

fn revalidate_active_source(
    plan: &ManagedDeployPlan,
    active_version: u64,
    cwd: &FastlyCommandContext<'_>,
    excluded_target: Option<u64>,
) -> Result<(), String> {
    let versions_raw = fastly_api_get(
        &format!("/service/{}/version", plan.service_id),
        &plan.token,
    )?;
    let versions = parse_service_versions(&versions_raw)?;
    let source_versions = source_versions_excluding(&versions, excluded_target);
    if select_version_source(&source_versions)? != VersionSource::Active(active_version) {
        return Err("Fastly active source selection changed after preflight".to_owned());
    }
    let source = source_versions
        .iter()
        .find(|version| version.number == active_version)
        .ok_or_else(|| "Fastly active source disappeared after preflight".to_owned())?;
    if !source.locked {
        return Err("Fastly active source is unexpectedly editable".to_owned());
    }
    let current_links = read_version_links(&plan.service_id, active_version, cwd)?;
    require_same_link_resources(&plan.source_links, &current_links, "active source")?;
    require_source_configuration(plan, active_version)
}

fn require_source_configuration(
    plan: &ManagedDeployPlan,
    source_version: u64,
) -> Result<(), String> {
    let current = read_version_configuration_snapshot(plan, source_version)?;
    if current == plan.source_configuration {
        Ok(())
    } else {
        Err(format!(
            "Fastly source version {source_version} protected Compute configuration changed after preflight"
        ))
    }
}

fn revalidate_inactive_source(
    plan: &ManagedDeployPlan,
    snapshot: &InactiveSourceSnapshot,
    cwd: &FastlyCommandContext<'_>,
    excluded_target: Option<u64>,
) -> Result<(), String> {
    let versions_raw = fastly_api_get(
        &format!("/service/{}/version", plan.service_id),
        &plan.token,
    )?;
    let versions = parse_service_versions(&versions_raw)?;
    let source_versions = source_versions_excluding(&versions, excluded_target);
    let expected = match &plan.version_source {
        EditableVersionSource::CloneRetired(_) => VersionSource::Retired(snapshot.version.number),
        EditableVersionSource::CloneStaging(_) => VersionSource::Staging(snapshot.version.number),
        EditableVersionSource::CloneActive { .. } | EditableVersionSource::InitialDraft(_) => {
            return Err("internal managed deploy inactive source mismatch".to_owned());
        }
    };
    if select_version_source(&source_versions)? != expected {
        return Err("Fastly inactive source selection changed after preflight".to_owned());
    }
    let current = versions
        .iter()
        .find(|version| version.number == snapshot.version.number)
        .ok_or_else(|| "Fastly inactive source disappeared after preflight".to_owned())?;
    if current != &snapshot.version
        || exact_version_metadata(&versions_raw, snapshot.version.number)? != snapshot.metadata
    {
        return Err("Fastly inactive source metadata changed after preflight".to_owned());
    }
    let (_current_links, links_value) =
        read_version_links_with_snapshot(&plan.service_id, snapshot.version.number, cwd)?;
    if links_value != snapshot.links {
        return Err("Fastly inactive source links changed after preflight".to_owned());
    }
    Ok(())
}

fn require_protected_initial_snapshot(
    plan: &ManagedDeployPlan,
    snapshot: &InitialDraftSnapshot,
) -> Result<(), String> {
    let current = read_version_configuration_snapshot(plan, snapshot.version.number)?;
    if current == snapshot.configuration {
        Ok(())
    } else {
        Err("Fastly initial draft protected configuration changed after preflight".to_owned())
    }
}

fn revalidate_managed_draft(
    plan: &ManagedDeployPlan,
    version: u64,
    expected_links: &BTreeMap<ResourceLinkIdentity, String>,
    expected_configuration: &VersionConfigurationSnapshot,
    cwd: &FastlyCommandContext<'_>,
) -> Result<(), String> {
    let versions_raw = fastly_api_get(
        &format!("/service/{}/version", plan.service_id),
        &plan.token,
    )?;
    let versions = parse_service_versions(&versions_raw)?;
    let source_state = match &plan.version_source {
        EditableVersionSource::CloneActive { active_version } => {
            VersionSource::Active(*active_version)
        }
        EditableVersionSource::CloneRetired(snapshot) => {
            VersionSource::Retired(snapshot.version.number)
        }
        EditableVersionSource::CloneStaging(snapshot) => {
            VersionSource::Staging(snapshot.version.number)
        }
        EditableVersionSource::InitialDraft(snapshot) => {
            VersionSource::InitialDraft(snapshot.version.number)
        }
    };
    require_managed_draft_state(source_state, &versions, version)?;
    match &plan.version_source {
        EditableVersionSource::CloneRetired(snapshot)
        | EditableVersionSource::CloneStaging(snapshot) => {
            revalidate_inactive_source(plan, snapshot, cwd, Some(version))?;
        }
        EditableVersionSource::CloneActive { .. } | EditableVersionSource::InitialDraft(_) => {}
    }
    let links = read_version_links(&plan.service_id, version, cwd)?;
    require_exact_link_resources(expected_links, &links, "final draft")?;
    let package_raw = fastly_api_get(
        &format!("/service/{}/version/{version}/package", plan.service_id),
        &plan.token,
    )?;
    let package_files_hash =
        parse_package_metadata_files_hash(&package_raw, &plan.service_id, version)?;
    require_package_identity(&plan.package_files_hash, &package_files_hash, version)?;
    let current_configuration = read_version_configuration_snapshot(plan, version)?;
    require_draft_configuration(expected_configuration, &current_configuration, version)
}

fn source_versions_excluding(
    versions: &[ServiceVersionRecord],
    excluded_target: Option<u64>,
) -> Vec<ServiceVersionRecord> {
    versions
        .iter()
        .filter(|version| Some(version.number) != excluded_target)
        .cloned()
        .collect()
}

fn require_managed_draft_state(
    source: VersionSource,
    versions: &[ServiceVersionRecord],
    version: u64,
) -> Result<(), String> {
    let draft = versions
        .iter()
        .find(|candidate| candidate.number == version)
        .ok_or_else(|| format!("Fastly draft version {version} disappeared before publication"))?;
    if draft.active || draft.locked || !draft.environments.is_empty() {
        return Err(format!(
            "Fastly version {version} is no longer an unpublished editable draft"
        ));
    }

    let active_versions = versions
        .iter()
        .filter(|candidate| candidate.active)
        .map(|candidate| candidate.number)
        .collect::<Vec<_>>();
    match source {
        VersionSource::Active(active_version) if active_versions != [active_version] => {
            return Err("Fastly active version changed before publication".to_owned());
        }
        VersionSource::Active(_) => {}
        _ if !active_versions.is_empty() => {
            return Err("Fastly active version appeared before publication".to_owned());
        }
        VersionSource::Retired(source_version) if version == source_version => {
            return Err("managed Fastly retired clone version did not advance".to_owned());
        }
        VersionSource::Staging(source_version) if version == source_version => {
            return Err("managed Fastly staged clone version did not advance".to_owned());
        }
        VersionSource::InitialDraft(source_version) if version != source_version => {
            return Err("managed Fastly initial draft version changed".to_owned());
        }
        VersionSource::Retired(_) | VersionSource::Staging(_) | VersionSource::InitialDraft(_) => {}
    }
    Ok(())
}

fn require_package_identity(expected: &str, actual: &str, version: u64) -> Result<(), String> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "Fastly version {version} package identity changed before publication"
        ))
    }
}

fn require_draft_configuration(
    expected: &VersionConfigurationSnapshot,
    actual: &VersionConfigurationSnapshot,
    version: u64,
) -> Result<(), String> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "Fastly version {version} protected Compute configuration changed before publication"
        ))
    }
}

fn read_version_configuration_snapshot(
    plan: &ManagedDeployPlan,
    version: u64,
) -> Result<VersionConfigurationSnapshot, String> {
    read_version_configuration_snapshot_for(&plan.service_id, &plan.token, version)
}

fn read_version_configuration_snapshot_for(
    service_id: &str,
    token: &FastlyApiToken,
    version: u64,
) -> Result<VersionConfigurationSnapshot, String> {
    let base = format!("/service/{service_id}/version/{version}");
    let domains = parse_snapshot_array(
        "domains",
        &fastly_api_get(&format!("{base}/domain"), token)?,
    )?;
    let backends = parse_snapshot_array(
        "backends",
        &fastly_api_get(&format!("{base}/backend"), token)?,
    )?;
    let healthchecks = parse_snapshot_array(
        "health checks",
        &fastly_api_get(&format!("{base}/healthcheck"), token)?,
    )?;
    let logging = snapshot_logging_providers(&base, token)?;
    let settings = parse_snapshot_object(
        "settings",
        &fastly_api_get(&format!("{base}/settings"), token)?,
    )?;
    Ok(VersionConfigurationSnapshot {
        backends,
        domains,
        healthchecks,
        logging,
        settings,
    })
}

fn resolve_managed_plan_service_id(
    context: &AdapterDeployContext,
    release: &VerifiedApplicationRelease,
    exec: &AdapterExecContext<'_>,
) -> Result<String, String> {
    if let Some(service_id) = context.service_id.as_deref() {
        validate_service_id(service_id)?;
        return Ok(service_id.to_owned());
    }
    effective_fastly_service_id_with_exec(release.adapter_manifest(), exec)?
        .map(SelectedFastlyService::into_id)
        .ok_or_else(|| {
            format!(
                "managed Fastly deployment requires a service id in typed context, the verified Fastly manifest, or {FASTLY_SERVICE_ID_ENV}"
            )
        })
}

fn run_fastly_json_capture(
    args: &[&str],
    cwd: &FastlyCommandContext<'_>,
) -> Result<String, String> {
    let output = Command::new("fastly")
        .args(args)
        .current_dir(cwd.cwd)
        .envs(cwd.env.iter().map(|(key, value)| (key, value)))
        .output()
        .map_err(|error| {
            if error.kind() == ErrorKind::NotFound {
                format!("`fastly` not found on PATH; {FASTLY_INSTALL_HINT}")
            } else {
                format!("failed to run read-only Fastly command: {error}")
            }
        })?;
    if !output.status.success() {
        return Err(format!(
            "read-only `fastly {}` exited with status {}\nstderr: {}",
            args.join(" "),
            output.status,
            redact_stderr(&String::from_utf8_lossy(&output.stderr))
        ));
    }
    strict_stdout(output.stdout, "read-only Fastly JSON command")
}

fn parse_resource_links(
    raw: &str,
) -> Result<(Vec<ExistingResourceLink>, serde_json::Value), String> {
    let snapshot: serde_json::Value = serde_json::from_str(raw)
        .map_err(|error| format!("failed to parse Fastly resource-link inventory: {error}"))?;
    let rows = snapshot.as_array().ok_or_else(|| {
        "Fastly resource-link inventory must be one complete bare JSON array".to_owned()
    })?;
    let mut links = Vec::with_capacity(rows.len());
    for (index, row) in rows.iter().enumerate() {
        let object = row.as_object().ok_or_else(|| {
            format!("Fastly resource-link inventory record #{index} is not an object")
        })?;
        let required = |field: &str| {
            object
                .get(field)
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| {
                    format!(
                        "Fastly resource-link inventory record #{index} requires non-empty string `{field}`"
                    )
                })
        };
        let resource_type = required("resource_type")?;
        let kind = match resource_type.as_str() {
            "config" => ResourceKind::Config,
            "kv-store" => ResourceKind::Kv,
            "secret-store" => ResourceKind::Secret,
            _ => {
                return Err(format!(
                    "Fastly resource-link inventory record #{index} has unknown `resource_type` `{resource_type}`"
                ));
            }
        };
        links.push(ExistingResourceLink {
            link_id: required("id")?,
            alias: required("name")?,
            kind,
            resource_id: required("resource_id")?,
        });
    }
    Ok((links, snapshot))
}

fn snapshot_initial_draft(
    version: ServiceVersionRecord,
    metadata: serde_json::Value,
    links: serde_json::Value,
    configuration: VersionConfigurationSnapshot,
) -> InitialDraftSnapshot {
    InitialDraftSnapshot {
        configuration,
        links,
        metadata,
        version,
    }
}

fn snapshot_logging_providers(
    version_base: &str,
    token: &FastlyApiToken,
) -> Result<Vec<LoggingProviderSnapshot>, String> {
    FASTLY_LOGGING_PROVIDER_KINDS
        .iter()
        .map(|&kind| {
            let endpoints = parse_snapshot_array(
                &format!("logging/{kind}"),
                &fastly_api_get(&format!("{version_base}/logging/{kind}"), token)?,
            )?;
            Ok(LoggingProviderSnapshot { endpoints, kind })
        })
        .collect()
}

fn exact_version_metadata(raw: &str, number: u64) -> Result<serde_json::Value, String> {
    let value: serde_json::Value = serde_json::from_str(raw)
        .map_err(|error| format!("failed to preserve Fastly version metadata: {error}"))?;
    value
        .as_array()
        .and_then(|versions| {
            versions.iter().find(|version| {
                version.get("number").and_then(serde_json::Value::as_u64) == Some(number)
            })
        })
        .cloned()
        .ok_or_else(|| format!("selected Fastly version {number} has no exact metadata record"))
}

fn parse_snapshot_array(label: &str, raw: &str) -> Result<serde_json::Value, String> {
    let mut value: serde_json::Value = serde_json::from_str(raw)
        .map_err(|error| format!("failed to parse Fastly {label} snapshot: {error}"))?;
    let rows = value
        .as_array_mut()
        .ok_or_else(|| format!("Fastly {label} snapshot must be a complete JSON array"))?;
    for (index, row) in rows.iter_mut().enumerate() {
        let object = row
            .as_object_mut()
            .ok_or_else(|| format!("Fastly {label} snapshot record #{index} must be an object"))?;
        remove_snapshot_metadata(object);
        row.sort_all_objects();
    }
    rows.sort_by_cached_key(ToString::to_string);
    Ok(value)
}

fn remove_snapshot_metadata(object: &mut serde_json::Map<String, serde_json::Value>) {
    for field in [
        "created_at",
        "locked",
        "service_id",
        "updated_at",
        "version",
    ] {
        object.remove(field);
    }
}

fn parse_snapshot_object(label: &str, raw: &str) -> Result<serde_json::Value, String> {
    let mut value: serde_json::Value = serde_json::from_str(raw)
        .map_err(|error| format!("failed to parse Fastly {label} snapshot: {error}"))?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| format!("Fastly {label} snapshot must be a complete JSON object"))?;
    remove_snapshot_metadata(object);
    value.sort_all_objects();
    Ok(value)
}

#[derive(Debug, Default, PartialEq, Eq)]
struct RuntimeStoreIds {
    config: Vec<String>,
    kv: Vec<String>,
    secrets: Vec<String>,
}

impl From<&DeployStoreIds> for RuntimeStoreIds {
    fn from(stores: &DeployStoreIds) -> Self {
        Self {
            config: stores.config.clone(),
            kv: stores.kv.clone(),
            secrets: stores.secrets.clone(),
        }
    }
}

pub(super) fn owns_managed_deploy(context: &AdapterDeployContext) -> bool {
    context.application_release_root.is_some() || context.staging || !context.stores.is_empty()
}

// ===================================================================
// Fastly lifecycle
// ===================================================================
//
// The adapter-managed deployment path verifies the immutable application release,
// uploads its recorded package with `compute update` to an exact unreachable draft,
// reconciles and reads back exact logical resource links and the package identity, and
// stages or activates only after verification. It emits `version=<N>` and
// `package-sha256=<SHA256>`. The bare store-free production manifest command is a
// compatibility path outside this managed lifecycle.
//
// These entry points also back the `healthcheck` and `rollback` app-CLI subcommands:
//
//   * healthcheck     → curl the domain (production) or the version's
//     resolved staging IP (`--staging`); non-zero exit when unhealthy.
//   * rollback        → activate the explicit `--rollback-to` version
//     (production) or deactivate `<v>` (staging) via the Fastly API.
// Rollback prints `rolled-back-to=<N>`.
//
// Provider HTTP calls shell out to `curl` (matching the lifecycle action
// conventions and avoiding a WASM-incompatible HTTP client
// in the adapter). The `FASTLY_API_TOKEN` is passed to `curl` via a
// `--config -` stdin file rather than on argv, so it never appears in
// `ps` / `/proc/<pid>/cmdline` (same discipline as
// `create_config_store_entry`'s `--stdin`).

/// Value that follows `flag` in a `--flag value` arg slice, if present.
fn arg_value<'args>(args: &'args [String], flag: &str) -> Option<&'args str> {
    args.iter()
        .position(|arg| arg == flag)
        .and_then(|idx| idx.checked_add(1))
        .and_then(|idx| args.get(idx))
        .map(String::as_str)
}

/// Whether a boolean `flag` (e.g. `--staging`) is present in `args`.
fn arg_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|arg| arg == flag)
}

/// Copy of `args` with `--flag value` removed (both tokens). Used to
/// forward operator passthrough (e.g. `--comment`) to `fastly compute
/// update` without re-passing `--service-id`, which is threaded
/// explicitly.
pub(super) fn args_without_flag_value(args: &[String], flag: &str) -> Vec<String> {
    let mut out = Vec::with_capacity(args.len());
    let mut skip = false;
    for arg in args {
        if skip {
            skip = false;
            continue;
        }
        if arg == flag {
            skip = true;
            continue;
        }
        out.push(arg.clone());
    }
    out
}

pub(super) fn scan_reserved_deploy_args(args: &[String]) -> Result<(), String> {
    for arg in args {
        let reserved_flag = match arg.as_str() {
            "--service-id" | "-s" | "--service-name" | "--version" | "--autoclone" | "--token"
            | "-t" => Some(arg.as_str()),
            value if value.starts_with("--service-id=") => Some("--service-id"),
            value if value.starts_with("--service-name=") => Some("--service-name"),
            value if value.starts_with("--version=") => Some("--version"),
            value if value.starts_with("--autoclone=") => Some("--autoclone"),
            value if value.starts_with("--token=") => Some("--token"),
            value if value.starts_with("-s") && !value.starts_with("--") && value.len() > 2 => {
                Some("-s")
            }
            value if value.starts_with("-t") && !value.starts_with("--") && value.len() > 2 => {
                Some("-t")
            }
            _ => None,
        };
        if let Some(flag) = reserved_flag {
            return Err(format!(
                "Fastly deploy argument `{flag}` is reserved for the EdgeZero deployment lifecycle"
            ));
        }
    }
    Ok(())
}

fn set_managed_deploy_value(
    slot: &mut Option<String>,
    flag: &str,
    value: &str,
) -> Result<(), String> {
    if value.is_empty() {
        return Err(format!(
            "Fastly deploy argument `{flag}` requires a non-empty value"
        ));
    }
    if slot.is_some() {
        return Err(format!(
            "Fastly deploy argument `{flag}` may be provided only once"
        ));
    }
    *slot = Some(value.to_owned());
    Ok(())
}

fn detached_managed_deploy_value<'args>(
    args: &'args [String],
    index: usize,
    flag: &str,
) -> Result<&'args str, String> {
    let value = args
        .get(index.saturating_add(1))
        .ok_or_else(|| format!("Fastly deploy argument `{flag}` requires a value"))?;
    if value.is_empty() || value.starts_with('-') {
        return Err(format!(
            "Fastly deploy argument `{flag}` requires a non-empty value"
        ));
    }
    Ok(value)
}

pub(super) fn parse_release_managed_deploy_args(
    args: &[String],
) -> Result<ReleaseManagedDeployArgs, String> {
    scan_reserved_deploy_args(args)?;
    let mut parsed = ReleaseManagedDeployArgs {
        comment: None,
        globals: Vec::new(),
    };
    let mut index = 0;
    while let Some(arg) = args.get(index) {
        if arg == "--comment" {
            let value = detached_managed_deploy_value(args, index, "--comment")?;
            set_managed_deploy_value(&mut parsed.comment, "--comment", value)?;
            index = index.saturating_add(2);
        } else if let Some(value) = arg.strip_prefix("--comment=") {
            set_managed_deploy_value(&mut parsed.comment, "--comment", value)?;
            index = index.saturating_add(1);
        } else if arg == "--package"
            || arg == "-p"
            || arg.starts_with("--package=")
            || (arg.starts_with("-p") && arg.len() > 2)
        {
            return Err(
                "Fastly deploy argument `--package/-p` is owned by the immutable application release"
                    .to_owned(),
            );
        } else if MANAGED_DEPLOY_GLOBAL_BOOL_FLAGS.contains(&arg.as_str()) {
            parsed.globals.push(arg.clone());
            index = index.saturating_add(1);
        } else if let Some((flag, _)) = arg.split_once('=')
            && MANAGED_DEPLOY_GLOBAL_BOOL_FLAGS.contains(&flag)
        {
            return Err(format!(
                "Fastly boolean deploy argument `{flag}` does not accept a value"
            ));
        } else if arg.starts_with('-') {
            return Err(format!("unsupported Fastly deploy argument {arg:?}"));
        } else {
            return Err(format!(
                "unexpected positional Fastly deploy argument {arg:?}"
            ));
        }
    }
    Ok(parsed)
}

/// Resolve the target service id from `--service-id` or, failing that,
/// `FASTLY_SERVICE_ID`.
fn resolve_service_id(args: &[String]) -> Result<String, String> {
    if let Some(value) = arg_value(args, "--service-id") {
        return Ok(value.to_owned());
    }
    env::var(FASTLY_SERVICE_ID_ENV).map_err(|_err| {
        format!("no service id: pass `--service-id <id>` or set {FASTLY_SERVICE_ID_ENV}")
    })
}

pub(super) fn validate_effective_deploy_service_id(
    context: &AdapterDeployContext,
    exec: &AdapterExecContext<'_>,
) -> Result<(), String> {
    if let Some(service_id) = context.service_id.as_deref() {
        return validate_service_id(service_id);
    }
    if let Some(manifest_path) = context.adapter_manifest_path.as_deref() {
        return validate_deploy_service_id_for_manifest(context, manifest_path, exec);
    }
    if let Some((_, id)) = exec
        .env()
        .iter()
        .rev()
        .find(|(key, _)| key == FASTLY_SERVICE_ID_ENV)
    {
        return validate_service_id(id);
    }
    match env::var(FASTLY_SERVICE_ID_ENV) {
        Ok(service_id) => validate_service_id(&service_id),
        Err(env::VarError::NotPresent) => Ok(()),
        Err(env::VarError::NotUnicode(_)) => Err(format!(
            "invalid service id from {FASTLY_SERVICE_ID_ENV}: expected ASCII letters and digits only"
        )),
    }
}

pub(super) fn validate_deploy_service_id_for_manifest(
    context: &AdapterDeployContext,
    manifest_path: &Path,
    exec: &AdapterExecContext<'_>,
) -> Result<(), String> {
    if let Some(service_id) = context.service_id.as_deref() {
        return validate_service_id(service_id);
    }
    effective_fastly_service_id_with_exec(manifest_path, exec)?;
    Ok(())
}

fn effective_deploy_environment(
    context: &AdapterDeployContext,
    exec: &AdapterExecContext<'_>,
) -> Result<EnvConfig, String> {
    let mut resolved: BTreeMap<String, String> = env::vars().collect();
    resolved.extend(exec.env().iter().cloned());
    let variables = merge_env_defaults(context.variable_defaults.iter(), resolved);
    let environment = EnvConfig::from_vars(variables);
    for (kind, logical_ids) in [
        (ResourceKind::Config, &context.stores.config),
        (ResourceKind::Kv, &context.stores.kv),
        (ResourceKind::Secret, &context.stores.secrets),
    ] {
        for logical_id in logical_ids {
            environment
                .store_name_checked(kind.selector_kind(), logical_id)
                .map_err(|error| format!("invalid Fastly deploy environment: {error}"))?;
            if kind == ResourceKind::Config {
                environment
                    .store_key_checked(kind.selector_kind(), logical_id)
                    .map_err(|error| format!("invalid Fastly deploy environment: {error}"))?;
            }
        }
    }
    Ok(environment)
}

pub(super) fn validate_fastly_config_key(
    logical_store_id: &str,
    key: &str,
    _staging: bool,
    _local: bool,
) -> Result<(), String> {
    let expected = logical_store_id;
    if key == expected {
        Ok(())
    } else {
        Err(format!(
            "Fastly uses logical config key `{expected}` for every target; remove the conflicting --key or EDGEZERO__STORES__CONFIG__{}__KEY override and select the environment's physical store with __NAME",
            logical_store_id.to_ascii_uppercase()
        ))
    }
}

fn effective_fastly_service_id_with_exec(
    path: &Path,
    exec: &AdapterExecContext<'_>,
) -> Result<Option<SelectedFastlyService>, String> {
    let Some((_, environment_id)) = exec
        .env()
        .iter()
        .rev()
        .find(|(key, _)| key == FASTLY_SERVICE_ID_ENV)
    else {
        return super::provision_cloud::effective_fastly_service_id(path);
    };
    let manifest_id = super::provision_cloud::read_fastly_service_id(path)?;
    super::provision_cloud::select_fastly_service_id(manifest_id, Some(environment_id.clone()))
}

pub(super) fn require_token_with_exec(
    exec: &AdapterExecContext<'_>,
) -> Result<FastlyApiToken, String> {
    let value = exec
        .env()
        .iter()
        .rev()
        .find(|(key, _)| key == FASTLY_API_TOKEN_ENV)
        .map(|(_, value)| value.clone())
        .map_or_else(
            || {
                env::var(FASTLY_API_TOKEN_ENV).map_err(|_error| {
                    format!("{FASTLY_API_TOKEN_ENV} must be set in the environment")
                })
            },
            Ok,
        )?;
    Ok(FastlyApiToken {
        value,
        env: exec.env().to_vec(),
    })
}

/// Read ambient credentials for standalone API actions.
pub(super) fn require_token() -> Result<FastlyApiToken, String> {
    require_token_with_exec(&AdapterExecContext::new())
}

/// Whether an HTTP status counts as healthy (2xx only).
///
/// A passing probe gates against an automatic rollback, so a 3xx is deliberately
/// NOT healthy: a staged version answering `301` to an error page (the probe does
/// not follow redirects) would otherwise mask a bad deploy as healthy.
fn is_healthy_status(code: u16) -> bool {
    (200..300).contains(&code)
}

/// Digits immediately following `marker` in `lower` (a lowercased
/// haystack), for the LAST occurrence of `marker`. The number must be
/// terminated by `terminator` — so a partial/confusable match (e.g. a
/// semver `15.2.0`) yields `None` rather than a bogus version.
fn last_version_after(lower: &str, marker: &str, terminator: char) -> Option<u64> {
    let mut result = None;
    for (idx, _) in lower.match_indices(marker) {
        let after = idx.saturating_add(marker.len());
        let Some(rest) = lower.get(after..) else {
            continue;
        };
        let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
        if digits.is_empty() || rest.chars().nth(digits.len()) != Some(terminator) {
            continue;
        }
        if let Ok(parsed) = digits.parse::<u64>() {
            result = Some(parsed);
        }
    }
    result
}

/// Parse a Fastly service version out of Fastly CLI output, accepting
/// ONLY the shapes the CLI actually emits, in precedence order:
///
///   1. Our canonical `version=<N>` contract line.
///   2. The CLI's success line, whose Go format string is
///      `"Updated package (service %s, version %v)"` (and
///      `"Deployed package (...)"` for `compute deploy`) — matched as
///      `, version <N>)`. This names the version the package landed on,
///      so it wins over (3).
///   3. The `--autoclone` notice, `"... Now operating on version %d."` —
///      the freshly-cloned draft, used when the success line is absent.
///
/// Everything else yields `None` and the caller FAILS CLOSED.
///
/// Deliberately strict. The previous implementation took ANY digits
/// appearing after the word "version" and let the last match win, so:
///   * `Uploaded package to service 12345, version unchanged` parsed as
///     version 12345, and
///   * the autoclone notice's *pre-clone* version
///     (`Service version 3 is not editable...`) could beat the real one,
///     since stdout and stderr are concatenated and their relative order
///     is not guaranteed.
///
/// A misparse here stages, comments, or rolls back the WRONG service
/// version, so ambiguity must be an error, not a guess.
pub(super) fn parse_fastly_version(text: &str) -> Option<u64> {
    let lower = text.to_ascii_lowercase();
    parse_canonical_version_line(&lower)
        .or_else(|| last_version_after(&lower, ", version ", ')'))
        .or_else(|| last_version_after(&lower, "now operating on version ", '.'))
}

/// Last standalone `version=<N>` line (the whole trimmed line must be
/// exactly that, so a `--version=active` flag echoed in a command line
/// cannot masquerade as one).
fn parse_canonical_version_line(lower: &str) -> Option<u64> {
    lower.lines().rev().find_map(|line| {
        let digits = line.trim().strip_prefix("version=")?;
        (!digits.is_empty() && digits.chars().all(|ch| ch.is_ascii_digit()))
            .then(|| digits.parse().ok())
            .flatten()
    })
}

/// Parse `fastly service version list --json` (or the Fastly API
/// `/service/<id>/version` array) for the `number` of the `active`
/// version.
/// Resolve the active version from a Fastly version-list JSON.
///
/// `Ok(Some(n))` — exactly one version is active. `Ok(None)` — the list parsed
/// but NO version is active (a first-ever deploy; the caller records an empty
/// rollback target and proceeds). `Err(_)` — the payload could not be parsed as
/// a version list, OR it is MALFORMED (a selection/safety field is missing or
/// has the wrong type, or MORE THAN ONE active version exists). All are
/// OPERATIONAL failures the caller must
/// NOT silently treat as "no active version" — otherwise a garbled or ambiguous
/// response would fail open and let a production deploy proceed with no rollback
/// target.
///
/// The ENTIRE list is scanned (not short-circuited at the first active entry) so
/// that a malformed field or a second active version anywhere in the response
/// is caught rather than ignored.
fn resolve_active_version(json: &str) -> Result<Option<u64>, String> {
    let versions = parse_service_versions(json)?;
    let mut active_version: Option<u64> = None;
    for version in versions {
        if version.active {
            if active_version.is_some() {
                return Err(format!(
                    "the Fastly version list reports more than one active version ({} and {}); the response is ambiguous, refusing to pick one",
                    active_version.unwrap_or_default(),
                    version.number,
                ));
            }
            active_version = Some(version.number);
        }
    }
    Ok(active_version)
}

fn parse_service_versions(json: &str) -> Result<Vec<ServiceVersionRecord>, String> {
    let versions: Vec<ServiceVersionRecord> = serde_json::from_str(json)
        .map_err(|error| format!("failed to parse the Fastly version list as JSON: {error}"))?;
    if versions.is_empty() {
        return Err(
            "the Fastly version list is empty; a service always has at least an initial version, so this response cannot be trusted".to_owned(),
        );
    }
    let mut numbers = HashSet::with_capacity(versions.len());
    for version in &versions {
        if !numbers.insert(version.number) {
            return Err(format!(
                "the Fastly version list contains duplicate version number {}; refusing an ambiguous response",
                version.number
            ));
        }
        for environment in &version.environments {
            if environment.name.is_empty() || environment.service_id.is_empty() {
                return Err(format!(
                    "Fastly version {} contains an incomplete environment record",
                    version.number
                ));
            }
        }
    }
    Ok(versions)
}

fn select_version_source(versions: &[ServiceVersionRecord]) -> Result<VersionSource, String> {
    let staging_record_count = versions
        .iter()
        .flat_map(|version| &version.environments)
        .filter(|environment| environment.name == "staging")
        .count();
    if staging_record_count > 1 {
        return Err(format!(
            "the Fastly version list reports more than one staging environment record ({staging_record_count}); the response is ambiguous"
        ));
    }

    let active_versions = versions
        .iter()
        .filter(|version| version.active)
        .collect::<Vec<_>>();
    if active_versions.len() > 1 {
        return Err("the Fastly version list reports more than one active version".to_owned());
    }
    if let Some(active_version) = active_versions.first() {
        return Ok(VersionSource::Active(active_version.number));
    }

    let highest = versions
        .iter()
        .map(|version| version.number)
        .max()
        .ok_or_else(|| "the Fastly version list is empty".to_owned())?;
    let drafts = versions
        .iter()
        .filter(|version| !version.active && !version.locked && version.environments.is_empty())
        .collect::<Vec<_>>();
    let staging_versions = versions
        .iter()
        .filter(|version| {
            !version.active
                && version.environments.len() == 1
                && version.environments.iter().all(|environment| {
                    environment.name == "staging" && environment.active_version == version.number
                })
        })
        .collect::<Vec<_>>();
    let retired = versions
        .iter()
        .filter(|version| !version.active && version.locked && version.environments.is_empty())
        .collect::<Vec<_>>();
    match (drafts.as_slice(), staging_versions.as_slice()) {
        ([draft], []) if draft.number == highest && retired.is_empty() => {
            return Ok(VersionSource::InitialDraft(draft.number));
        }
        ([draft], []) if !retired.is_empty() => {
            return Err(format!(
                "editable Fastly draft {} exists beside retired version(s); EdgeZero cannot prove the draft belongs to a retired source. Restore a single unambiguous source version before retrying",
                draft.number
            ));
        }
        ([], [staging_version]) => return Ok(VersionSource::Staging(staging_version.number)),
        ([draft], [staging_version]) => {
            return Err(format!(
                "editable Fastly draft {draft_version} exists beside staged version {staging_version}; EdgeZero cannot prove the draft belongs to the staged source. Lock the orphan with `fastly service version lock --version={draft_version}` (and pass `--service-id` when fastly.toml does not select the service), then retry",
                draft_version = draft.number,
                staging_version = staging_version.number,
            ));
        }
        _ => {}
    }
    if drafts.is_empty()
        && staging_versions.is_empty()
        && let Some(retired_version) = retired.iter().find(|version| version.number == highest)
    {
        return Ok(VersionSource::Retired(retired_version.number));
    }
    if drafts.len() == 1 && staging_versions.len() <= 1 {
        return Err(format!(
            "first deployment requires the highest service version ({highest}) to be the initialized editable draft"
        ));
    }
    if staging_versions.len() > 1 || drafts.len() > 1 {
        return Err(format!(
            "deployment without an active version requires one highest editable draft, staged source, or retired source; found {} drafts, {} staged versions, and {} retired versions",
            drafts.len(),
            staging_versions.len(),
            retired.len()
        ));
    }
    Err(format!(
        "deployment without an active version requires one highest editable draft, staged source, or retired source; found {} drafts, {} staged versions, and {} retired versions",
        drafts.len(),
        staging_versions.len(),
        retired.len()
    ))
}

/// Best-effort staleness guard for a production rollback: the version being
/// rolled back FROM (`from_version`, the caller's `--version`) must still be the
/// ACTIVE version. A rollback can run long after its deploy; if a newer version
/// was activated since, activating the old target would clobber it — so refuse.
///
/// This narrows but does NOT close the race: the caller reads the active version
/// and activates in two separate requests, and Fastly's activate endpoint has no
/// precondition, so a deploy landing between them can still be clobbered.
/// Service-scoped serialization is required to eliminate it.
fn ensure_rollback_from_is_active(
    active: Option<u64>,
    from_version: u64,
    service_id: &str,
) -> Result<(), String> {
    match active {
        Some(active_version) if active_version == from_version => Ok(()),
        Some(active_version) => Err(format!(
            "refusing to roll back service {service_id}: the active version is now {active_version}, not the {from_version} being rolled back from -- a newer deploy is live and rolling back would clobber it"
        )),
        None => Err(format!(
            "refusing to roll back service {service_id}: it has no active version"
        )),
    }
}

fn staging_rollback_decision(
    versions: &[ServiceVersionRecord],
    requested_version: u64,
    service_id: &str,
) -> Result<StagingRollbackDecision, String> {
    let staging_records = versions
        .iter()
        .flat_map(|version| {
            version
                .environments
                .iter()
                .filter(|environment| environment.name == "staging")
                .map(move |environment| (version.number, environment))
        })
        .collect::<Vec<_>>();
    if staging_records.len() > 1 {
        return Err(format!(
            "Fastly service {service_id} reports more than one staging environment record; refusing staging rollback"
        ));
    }
    let version = versions
        .iter()
        .find(|candidate| candidate.number == requested_version)
        .ok_or_else(|| {
            format!(
                "Fastly version {requested_version} is absent from service {service_id}; refusing staging rollback"
            )
        })?;
    let is_staging_version = matches!(
        staging_records.as_slice(),
        [(record_version, environment)]
            if *record_version == requested_version
                && environment.active_version == requested_version
    );
    if is_staging_version {
        return Ok(StagingRollbackDecision::Deactivate);
    }
    let unpublished_draft = !version.active && !version.locked && version.environments.is_empty();
    if unpublished_draft {
        return Ok(StagingRollbackDecision::NoopDraft);
    }
    Err(format!(
        "Fastly version {requested_version} is neither the exact staged version nor an unpublished editable draft; refusing staging rollback"
    ))
}

/// Staging IP for one exact domain in a Fastly
/// `GET /service/<id>/version/<n>/domain?include=staging_ips` response.
///
/// The response is an ARRAY of domain objects, and the staging address
/// is a SINGULAR, nullable STRING field named `staging_ip` on each
/// domain (`staging_ips` is only the `include=` query-param value, never
/// a field name). Verified against the go-fastly `Domain` model, whose
/// field is `StagingIP` with the mapstructure tag `staging_ip`, and its
/// recorded API fixture `fixtures/domains/list_with_staging_ips.yaml`,
/// plus Fastly's "working with staging" guide.
fn parse_staging_ip(json: &str, domain: &str) -> Result<String, String> {
    #[derive(serde::Deserialize)]
    struct DomainRecord {
        name: String,
        staging_ip: Option<String>,
    }

    let records: Vec<DomainRecord> = serde_json::from_str(json).map_err(|error| {
        format!(
            "Fastly staging domain inventory has an invalid response shape (payload redacted): {error}"
        )
    })?;
    let mut matches = records.iter().filter(|record| record.name == domain);
    let record = matches.next().ok_or_else(|| {
        format!("Fastly staging domain inventory does not contain domain `{domain}`")
    })?;
    if matches.next().is_some() {
        return Err(format!(
            "Fastly staging domain inventory contains duplicate domain `{domain}`"
        ));
    }
    record
        .staging_ip
        .as_deref()
        .filter(|ip| !ip.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| format!("Fastly staging domain `{domain}` has no staging IP"))
}

/// Build the `curl` argv for a health probe. Production probes the
/// domain directly; staging reroutes the TLS connection to the
/// resolved staging IP via `--connect-to ::<ip>:443`. `path` is the
/// URL path (always begins with '/'), applied identically to both.
fn build_curl_probe_args(
    domain: &str,
    path: &str,
    staging_ip: Option<&str>,
    timeout_secs: u64,
) -> Vec<String> {
    let mut args = vec![
        // `-q` first so curl never merges `~/.curlrc` into a probe (a planted
        // `proxy`/`output` there could otherwise redirect or corrupt the check).
        "-q".to_owned(),
        "-sS".to_owned(),
        // Disable curl's URL globbing: a valid probe path may contain `[` `]` `{`
        // `}` (e.g. `/health?ids[0]=1`), which curl would otherwise treat as a
        // glob — failing with exit 3 or firing multiple requests, and so
        // mis-reporting a healthy deployment as unhealthy.
        "--globoff".to_owned(),
        "-o".to_owned(),
        "/dev/null".to_owned(),
        "-w".to_owned(),
        "%{http_code}".to_owned(),
        "--max-time".to_owned(),
        timeout_secs.to_string(),
    ];
    if let Some(ip) = staging_ip {
        // `--connect-to ::HOST:PORT` reroutes the TLS connection to the staging
        // IP. An IPv6 literal must be bracketed or curl mis-parses the colons;
        // the caller has already validated `ip` parses as an `IpAddr`.
        let target = if ip.contains(':') {
            format!("::[{ip}]:443")
        } else {
            format!("::{ip}:443")
        };
        args.push("--connect-to".to_owned());
        args.push(target);
    }
    args.push(format!("https://{domain}{path}"));
    args
}

/// Validate a caller-supplied probe path. It is appended to
/// `https://{domain}` to form one curl argument, so it must begin with
/// '/' and carry no whitespace or control characters that would break
/// the URL or smuggle a second token.
fn validate_probe_path(path: &str) -> Result<(), String> {
    if !path.starts_with('/') {
        return Err(format!("healthcheck --path must begin with '/': '{path}'"));
    }
    if path.chars().any(|ch| ch.is_whitespace() || ch.is_control()) {
        return Err(format!(
            "healthcheck --path must not contain whitespace or control characters: '{path}'"
        ));
    }
    Ok(())
}

/// Retry a health probe. Returns `Ok(code)` on the first healthy
/// status, or `Err((last_code, message))` after exhausting attempts.
/// `between` runs between attempts (not after the last) so it can be a
/// no-op in tests.
fn probe_with_retries<P, S>(
    retry: u32,
    mut prober: P,
    mut between: S,
) -> Result<u16, (Option<u16>, String)>
where
    P: FnMut() -> Result<u16, String>,
    S: FnMut(),
{
    let attempts = retry.max(1);
    let mut last_code = None;
    let mut last_msg = "no probe attempts were made".to_owned();
    for attempt in 0..attempts {
        match prober() {
            Ok(code) if is_healthy_status(code) => return Ok(code),
            Ok(code) => {
                last_code = Some(code);
                last_msg = format!("unhealthy HTTP status {code}");
            }
            Err(err) => last_msg = err,
        }
        if attempt.saturating_add(1) < attempts {
            between();
        }
    }
    Err((last_code, last_msg))
}

/// Run `fastly <args>` in `cwd`, inheriting stdio, and map a non-zero
/// exit to an error.
fn run_fastly_status(fastly_args: &[String], cwd: &FastlyCommandContext<'_>) -> Result<(), String> {
    let status = Command::new("fastly")
        .args(fastly_args)
        .current_dir(cwd.cwd)
        .envs(cwd.env.iter().map(|(key, value)| (key, value)))
        .status()
        .map_err(|err| {
            if err.kind() == ErrorKind::NotFound {
                format!("`fastly` not found on PATH; {FASTLY_INSTALL_HINT}")
            } else {
                format!("failed to run fastly CLI: {err}")
            }
        })?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "`fastly {}` exited with status {status}",
            fastly_args.join(" ")
        ))
    }
}

/// Run `fastly <args>` in `cwd` capturing stdout+stderr (combined) for
/// version parsing. Errors on a non-zero exit.
/// Run `curl -q -sS --config -`, piping `config` (which carries the
/// `Fastly-Key` header + url) through stdin so the token never touches
/// argv. Returns stdout on a zero exit.
///
/// `-q` MUST be the first argument: without it curl reads `~/.curlrc`
/// (or `$CURL_HOME/.curlrc`) and merges it into this token-bearing
/// config, so a `proxy = …` directive planted by an earlier same-job
/// build step could exfiltrate the `Fastly-Key` header. `--connect-timeout`
/// / `--max-time` bound the call.
fn curl_config_capture(config: &str, environment: &[(String, String)]) -> Result<String, String> {
    let connect_timeout = FASTLY_API_CONNECT_TIMEOUT_SECS.to_string();
    let max_time = FASTLY_API_MAX_TIME_SECS.to_string();
    let mut child = Command::new("curl")
        .envs(environment.iter().map(|(key, value)| (key, value)))
        .args([
            "-q",
            "-sS",
            "--connect-timeout",
            &connect_timeout,
            "--max-time",
            &max_time,
            "--config",
            "-",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| {
            if err.kind() == ErrorKind::NotFound {
                "`curl` not found on PATH; install curl and retry".to_owned()
            } else {
                format!("failed to spawn `curl`: {err}")
            }
        })?;
    // Take stdin OUT of the child and hand it to a helper BY VALUE, so it drops at
    // that helper's scope end — a natural drop rather than an explicit `drop(stdin)`,
    // which trips `clippy::drop_non_drop` on wasm targets where `ChildStdin` is not
    // `Drop`. The drop must precede `wait_with_output` so curl sees EOF (same pattern
    // as `write_value_to_fastly_stdin` on the fastly path).
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| "failed to open stdin pipe to `curl`".to_owned())?;
    write_config_to_curl_stdin(stdin, config)?;
    let output = child
        .wait_with_output()
        .map_err(|err| format!("failed to wait on `curl`: {err}"))?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    } else if output.status.code() == Some(CURL_EXIT_TIMEOUT) {
        Err(format!(
            "`curl` timed out after connect-timeout {FASTLY_API_CONNECT_TIMEOUT_SECS}s / max-time {FASTLY_API_MAX_TIME_SECS}s: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    } else {
        Err(format!(
            "`curl` exited with status {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}

/// Write `config` to curl's stdin, taking the handle BY VALUE so it drops at this
/// function's scope end. That natural drop closes the pipe (curl sees EOF) without
/// an explicit `drop(stdin)`, which trips `clippy::drop_non_drop` on wasm targets
/// where `ChildStdin` is not `Drop` (mirrors `write_value_to_fastly_stdin`).
fn write_config_to_curl_stdin(mut stdin: ChildStdin, config: &str) -> Result<(), String> {
    stdin
        .write_all(config.as_bytes())
        .map_err(|err| format!("failed to write curl config to stdin: {err}"))
}

/// Wrap `value` in a curl-config double-quoted string, escaping the
/// characters that would otherwise let a value terminate its quote and
/// inject additional curl options. Within a curl `--config` file a
/// double-quoted value only honours the escapes `\\`, `\"`, `\n`, `\r`,
/// `\t` (and the config is parsed line-by-line, so a raw newline ends
/// the directive regardless of quoting). We escape backslash and quote
/// so the value cannot break out of the quotes, and map raw control
/// characters to their escape form so NO raw newline (or CR/tab) is
/// ever written into the config file. This is the second half of the
/// injection defence: untrusted identifiers are also validated (see
/// `validate_service_id` / `validate_version_str` / `validate_domain`),
/// but the token is a secret we cannot constrain to a charset, so it
/// relies on this escaping alone.
fn curl_quote(value: &str) -> String {
    let mut out = String::with_capacity(value.len().saturating_add(2));
    out.push('"');
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            other => out.push(other),
        }
    }
    out.push('"');
    out
}

/// Validate an operator-supplied Fastly service id before it is
/// interpolated into an API URL. Fastly service ids contain only ASCII
/// letters and digits.
/// Values carrying a quote, newline, or space could inject curl options via
/// the `--config` file.
pub(super) fn validate_service_id(id: &str) -> Result<(), String> {
    if !id.is_empty() && id.chars().all(|ch| ch.is_ascii_alphanumeric()) {
        Ok(())
    } else {
        Err(format!(
            "invalid service id {id:?}: expected ASCII letters and digits only"
        ))
    }
}

/// Validate a service-version string is a plain non-negative integer
/// before it is interpolated into an API URL. Returns the parsed value
/// so callers can reuse it.
fn validate_version_str(version: &str) -> Result<u64, String> {
    version.parse::<u64>().map_err(|err| {
        format!("invalid version {version:?}: expected a non-negative integer: {err}")
    })
}

/// Validate a domain is a plausible hostname before it is placed into a
/// `curl` URL. Rejects anything outside the DNS label charset
/// (`[A-Za-z0-9-.]`), empty / over-long values, leading/trailing dots,
/// and empty labels so an injected quote / slash / space / newline
/// cannot smuggle curl options or a second URL.
fn validate_domain(domain: &str) -> Result<(), String> {
    let charset_ok = domain
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '.');
    let shape_ok = !domain.is_empty()
        && domain.len() <= 253
        && !domain.starts_with('.')
        && !domain.ends_with('.')
        && !domain.contains("..");
    if charset_ok && shape_ok {
        Ok(())
    } else {
        Err(format!(
            "invalid domain {domain:?}: expected a hostname like `example.com`"
        ))
    }
}

/// `GET https://api.fastly.com<path>` with the `Fastly-Key` header;
/// returns the response body ONLY on a 2xx status. Both the header (carrying the
/// secret token) and the URL are written through `curl_quote` so neither can
/// inject curl options into the `--config` document.
///
/// The HTTP status is captured explicitly via `write-out` (as the PUT helper
/// does) and required to be 2xx before the body is trusted. `--fail` alone would
/// reject 4xx/5xx but still accept a 3xx — whose (array-shaped) body could
/// otherwise be parsed as version data. No `location` directive is set, so a
/// redirect is never followed.
fn fastly_api_get(path: &str, token: &FastlyApiToken) -> Result<String, String> {
    let header = curl_quote(&format!("Fastly-Key: {}", token.as_str()));
    let url = curl_quote(&format!("https://api.fastly.com{path}"));
    // `write-out` appends the status on its own trailing line AFTER the body.
    let config = format!("header = {header}\nurl = {url}\nwrite-out = \"\\n%{{http_code}}\"\n");
    let out = curl_config_capture(&config, &token.env)
        .map_err(|err| format!("Fastly API GET {path} failed: {err}"))?;
    let (body, status_line) = out
        .rsplit_once('\n')
        .ok_or_else(|| format!("Fastly API GET {path}: no HTTP status in the curl output"))?;
    let status: u16 = status_line.trim().parse().map_err(|err| {
        format!(
            "Fastly API GET {path}: could not parse the HTTP status {:?}: {err}",
            status_line.trim()
        )
    })?;
    if !(200..300).contains(&status) {
        return Err(format!("Fastly API GET {path} returned HTTP {status}"));
    }
    Ok(body.to_owned())
}

/// `PUT https://api.fastly.com<path>` with the `Fastly-Key` header;
/// returns the HTTP status, erroring on non-2xx. Fastly's version
/// activate/deactivate endpoints require `PUT` (not `POST`). Header and
/// URL are escaped via `curl_quote`; the literal `request`, `output`,
/// and `write-out` directives are fixed constants.
fn fastly_api_put(path: &str, token: &FastlyApiToken) -> Result<u16, String> {
    let header = curl_quote(&format!("Fastly-Key: {}", token.as_str()));
    let url = curl_quote(&format!("https://api.fastly.com{path}"));
    let config = format!(
        "request = \"PUT\"\nheader = {header}\nurl = {url}\noutput = \"/dev/null\"\nwrite-out = \"%{{http_code}}\"\n"
    );
    let out = curl_config_capture(&config, &token.env)?;
    let code: u16 = out.trim().parse().map_err(|err| {
        format!(
            "could not parse HTTP status from curl output {:?}: {err}",
            out.trim()
        )
    })?;
    if (200..300).contains(&code) {
        Ok(code)
    } else {
        Err(format!("Fastly API PUT {path} returned HTTP {code}"))
    }
}

/// `PUT https://api.fastly.com<path>` and return a non-empty response body.
/// This is used for mutations, such as cloning a version, whose response
/// identifies the newly-created provider object needed for recovery.
fn fastly_api_put_capture(path: &str, token: &FastlyApiToken) -> Result<String, String> {
    let header = curl_quote(&format!("Fastly-Key: {}", token.as_str()));
    let url = curl_quote(&format!("https://api.fastly.com{path}"));
    let config = format!(
        "request = \"PUT\"\nheader = {header}\nurl = {url}\nwrite-out = \"\\n%{{http_code}}\"\n"
    );
    let out = curl_config_capture(&config, &token.env)?;
    let (body, status_line) = out
        .rsplit_once('\n')
        .ok_or_else(|| format!("Fastly API PUT {path}: no HTTP status in the curl output"))?;
    let code: u16 = status_line.trim().parse().map_err(|error| {
        format!(
            "Fastly API PUT {path}: could not parse the HTTP status {:?}: {error}",
            status_line.trim()
        )
    })?;
    if !(200..300).contains(&code) {
        return Err(format!("Fastly API PUT {path} returned HTTP {code}"));
    }
    if body.trim().is_empty() {
        return Err(format!("Fastly API PUT {path} returned an empty response"));
    }
    Ok(body.to_owned())
}

pub(super) fn legacy_deploy_context(args: &[String], staging: bool) -> AdapterDeployContext {
    AdapterDeployContext {
        adapter_manifest_path: arg_value(args, "--manifest-path").map(PathBuf::from),
        application_manifest_path: None,
        application_release_root: None,
        service_id: arg_value(args, "--service-id").map(str::to_owned),
        stores: DeployStoreIds::default(),
        staging,
        variable_defaults: BTreeMap::default(),
    }
}

/// Resolve the directory containing the Fastly manifest selected by the
/// application manifest. Fall back to discovery for direct adapter callers.
pub(super) fn resolve_deploy_manifest_path(
    context: &AdapterDeployContext,
) -> Result<PathBuf, String> {
    if let Some(path) = context.adapter_manifest_path.as_deref() {
        return Ok(path.to_path_buf());
    }
    find_fastly_manifest(env::current_dir().map_err(|err| err.to_string())?.as_path())
}

/// Production companion to `deploy`: resolve the active service version via the
/// Fastly API and emit it as a `version=<N>` line.
///
/// Distinguishes "confirmed no active version" from an operational failure: a
/// service with no active version yet (a first-ever deploy) is NOT an error — it
/// emits an empty `version=` line and succeeds, so the caller records an empty
/// rollback target. Only a real failure (API/auth error, or a version list that
/// cannot be parsed) returns `Err`, so the caller can fail closed instead of
/// silently proceeding without a rollback target.
///
/// `--require-active` flips the no-active-version case to an error: it is passed
/// by the production-`deploy` version fallback, where a version was JUST
/// activated, so "no active version" is not a valid first-deploy state but an
/// operational failure the CLI must not report as success.
pub(super) fn emit_active_version(args: &[String]) -> Result<(), String> {
    let service_id = resolve_service_id(args)?;
    validate_service_id(&service_id)?;
    emit_active_version_for(&service_id, arg_flag(args, "--require-active"))
}

pub(super) fn emit_active_version_for(
    service_id: &str,
    require_active: bool,
) -> Result<(), String> {
    emit_active_version_with_token(service_id, require_active, &require_token()?)
}

pub(super) fn emit_active_version_with_token(
    service_id: &str,
    require_active: bool,
    token: &FastlyApiToken,
) -> Result<(), String> {
    let json = fastly_api_get(&format!("/service/{service_id}/version"), token)?;
    if let Some(version) = active_version_or_require(&json, require_active, service_id)? {
        log::info!("version={version}");
    } else {
        // Confirmed no active version (first-ever deploy), and it was not
        // required. Emit an explicit empty line so the caller records an empty
        // rollback target and succeeds — distinct from a failure (`Err`).
        log::info!("version=");
        log::info!(
            "service {service_id} has no active version yet; emitting an empty rollback target"
        );
    }
    Ok(())
}

/// Resolve the active version and apply the `--require-active` policy.
///
/// `Ok(Some(n))` — a version is active. `Ok(None)` — no active version and
/// `require_active` is false (a first-ever `active-version` call; the caller
/// records an empty rollback target). `Err` — the response was malformed
/// ([`resolve_active_version`]), OR no version is active while `require_active`
/// is true. The latter is the production-`deploy` fallback: a version was JUST
/// activated, so "no active version" is an error, not a valid empty result.
fn active_version_or_require(
    json: &str,
    require_active: bool,
    service_id: &str,
) -> Result<Option<u64>, String> {
    match resolve_active_version(json)? {
        Some(version) => Ok(Some(version)),
        None if require_active => Err(format!(
            "the deploy reported success but the Fastly API returns no active version for service {service_id}; refusing to report a deploy with no resolvable version"
        )),
        None => Ok(None),
    }
}

/// Require `version` to be the currently ACTIVE service version — the
/// production healthcheck's version contract.
///
/// The production probe hits the live domain, which serves whatever version is
/// active, so "healthcheck version N" is only a true statement about N while N is
/// active. `phase` (`before probing` / `after probing`) names when the check ran,
/// so a version activated by a concurrent deploy is reported clearly rather than
/// masquerading as a healthy `version`.
pub(super) fn verify_version_active(
    service_id: &str,
    version: u64,
    token: &FastlyApiToken,
    phase: &str,
) -> Result<(), String> {
    let json = fastly_api_get(&format!("/service/{service_id}/version"), token)?;
    version_active_verdict(resolve_active_version(&json)?, version, service_id, phase)
}

/// The pure decision behind [`verify_version_active`], split out so the version
/// contract is unit-testable without a live Fastly API.
fn version_active_verdict(
    active: Option<u64>,
    version: u64,
    service_id: &str,
    phase: &str,
) -> Result<(), String> {
    match active {
        Some(active_version) if active_version == version => Ok(()),
        Some(active_version) => Err(format!(
            "production healthcheck version {version} is not active {phase}: service {service_id} currently has version {active_version} active, so the live-domain probe reflects version {active_version}, not {version}"
        )),
        None => Err(format!(
            "production healthcheck version {version} could not be confirmed active {phase}: service {service_id} has no active version"
        )),
    }
}

/// `healthcheck --adapter fastly ...`: probe the domain
/// (production) or the version's staging IP (`--staging`), retrying up
/// to `--retry` times. Emits `status-code` / `healthy` and returns
/// `Err` (non-zero exit) when unhealthy after retries.
///
/// `--domain`, `--service-id` and `--version` are REQUIRED and validated
/// on BOTH the production and the staging path. GitHub Actions' `required:
/// true` does not actually fail a workflow when an input is omitted or
/// empty, so this is the real guard: a production healthcheck must never
/// probe on behalf of an absent/empty version it never verified — the
/// caller chains that same version into rollback.
///
/// On the PRODUCTION path the probe reaches whatever version is live, so when a
/// token is available `version` is verified ACTIVE before and after the probe
/// (see [`verify_version_active`]); without a token the check is service-level.
pub(super) fn healthcheck(args: &[String]) -> Result<(), String> {
    let domain =
        arg_value(args, "--domain").ok_or_else(|| "healthcheck requires --domain".to_owned())?;
    validate_domain(domain)?;
    let service_id = resolve_service_id(args)?;
    validate_service_id(&service_id)?;
    let version_str =
        arg_value(args, "--version").ok_or_else(|| "healthcheck requires --version".to_owned())?;
    let version = validate_version_str(version_str)?;
    let path = arg_value(args, "--path").unwrap_or("/");
    validate_probe_path(path)?;
    let retry = arg_value(args, "--retry")
        .and_then(|value| value.parse().ok())
        .unwrap_or(3_u32);
    let retry_delay = arg_value(args, "--retry-delay")
        .and_then(|value| value.parse().ok())
        .unwrap_or(5_u64);
    let timeout = arg_value(args, "--timeout")
        .and_then(|value| value.parse().ok())
        .unwrap_or(10_u64);
    // curl reads `--max-time 0` as "no limit", so a zero timeout lets a single
    // probe run indefinitely. Require a positive value.
    if timeout == 0 {
        return Err("healthcheck --timeout must be a positive number of seconds".to_owned());
    }

    let is_staging = arg_flag(args, "--staging");
    let staging_ip = if is_staging {
        let token = require_token()?;
        let json = fastly_api_get(
            &format!("/service/{service_id}/version/{version}/domain?include=staging_ips"),
            &token,
        )?;
        let ip = parse_staging_ip(&json, domain)?;
        // Require a real `IpAddr` before it reaches curl's `--connect-to`, which
        // also settles IPv4-vs-IPv6 formatting.
        ip.parse::<IpAddr>().map_err(|err| {
            format!("resolved staging IP {ip:?} is not a valid IP address: {err}")
        })?;
        Some(ip)
    } else {
        None
    };

    // Production version contract: the probe hits the live domain, which serves
    // whatever version is ACTIVE — not necessarily `version`. When a token is
    // available, require `version` to be active both BEFORE and AFTER the probe, so
    // a version activated concurrently (by another deploy) cannot be reported as a
    // healthy `version`. Without a token the production check is inherently
    // service-level — say so rather than imply a version-specific guarantee. The
    // staging path already targets the specific version's staging IP, so it needs
    // no such check.
    let production_token = if is_staging {
        None
    } else {
        match env::var(FASTLY_API_TOKEN_ENV) {
            Ok(value) if !value.is_empty() => Some(FastlyApiToken {
                value,
                env: Vec::new(),
            }),
            _ => {
                log::info!(
                    "no {FASTLY_API_TOKEN_ENV} available; production healthcheck is service-level (probes the live domain for service {service_id}, not specifically version {version})"
                );
                None
            }
        }
    };
    if let Some(token) = production_token.as_ref() {
        verify_version_active(&service_id, version, token, "before probing")?;
    }

    let curl_args = build_curl_probe_args(domain, path, staging_ip.as_deref(), timeout);
    let delay = Duration::from_secs(retry_delay);
    let outcome = probe_with_retries(retry, || curl_status(&curl_args), || thread::sleep(delay));
    match outcome {
        Ok(code) => {
            // Confirm `version` is STILL active, so a deploy that activated a newer
            // version during the probe+retries is not reported as a healthy `version`.
            if let Some(token) = production_token.as_ref() {
                verify_version_active(&service_id, version, token, "after probing")?;
            }
            log::info!("status-code={code}");
            log::info!("healthy=true");
            Ok(())
        }
        Err((last_code, msg)) => {
            if let Some(code) = last_code {
                log::info!("status-code={code}");
            }
            log::info!("healthy=false");
            Err(format!(
                "healthcheck for {domain} failed after {} attempt(s): {msg}",
                retry.max(1)
            ))
        }
    }
}

/// Run a single `curl` health probe, returning the HTTP status. A
/// transport failure (timeout, DNS, refused) surfaces as `Err` so the
/// retry loop treats it as an unhealthy attempt.
fn curl_status(args: &[String]) -> Result<u16, String> {
    let output = Command::new("curl").args(args).output().map_err(|err| {
        if err.kind() == ErrorKind::NotFound {
            "`curl` not found on PATH; install curl and retry".to_owned()
        } else {
            format!("failed to spawn `curl`: {err}")
        }
    })?;
    if !output.status.success() {
        return Err(format!(
            "curl transport failure (status {}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout.trim().parse::<u16>().map_err(|err| {
        format!(
            "could not parse HTTP status from curl output {:?}: {err}",
            stdout.trim()
        )
    })
}

/// `rollback --adapter fastly ...`: production activates the explicit
/// `--rollback-to` version (Fastly cannot infer a previous version);
/// staging deactivates `<version>`.
pub(super) fn rollback(args: &[String]) -> Result<(), String> {
    let service_id = resolve_service_id(args)?;
    validate_service_id(&service_id)?;
    let version_str =
        arg_value(args, "--version").ok_or_else(|| "rollback requires --version".to_owned())?;
    let version = validate_version_str(version_str)?;
    let token = require_token()?;

    if arg_flag(args, "--staging") {
        let json = fastly_api_get(&format!("/service/{service_id}/version"), &token)?;
        let versions = parse_service_versions(&json)?;
        match staging_rollback_decision(&versions, version, &service_id)? {
            StagingRollbackDecision::Deactivate => {
                // Fastly's environment-scoped deactivate is
                // `PUT .../deactivate/staging`; a plain `.../deactivate`
                // targets production activation.
                fastly_api_put(
                    &format!("/service/{service_id}/version/{version}/deactivate/staging"),
                    &token,
                )?;
                log::info!(
                    "[edgezero] deactivated staged version {version} on Fastly service {service_id}"
                );
            }
            StagingRollbackDecision::NoopDraft => log::info!(
                "[edgezero] Fastly version {version} is an unpublished draft; staging rollback has nothing to deactivate"
            ),
        }
    } else {
        // Production rollback re-activates an EXPLICIT target. Fastly's version
        // list has no field distinguishing a previously-live version from a
        // staged one (`staging`/`deployed` are documented "Unused"; `locked`
        // only means "not editable"), so the target cannot be inferred — it is
        // captured before the superseding deploy and passed in as --rollback-to.
        let previous = arg_value(args, "--rollback-to")
            .and_then(|raw| validate_version_str(raw).ok())
            .ok_or_else(|| {
                "production rollback requires a valid --rollback-to version".to_owned()
            })?;
        // Best-effort staleness check: the version being rolled back FROM
        // (`--version`) must STILL be the active version. A rollback workflow can
        // run long after its deploy — if a newer version was activated meanwhile,
        // activating the old target would clobber that newer deploy, so refuse.
        //
        // This is NOT atomic: Fastly's activate endpoint has no precondition, so
        // a deploy that lands BETWEEN this read and the activate below can still
        // be clobbered. It narrows the window (catching the common much-later
        // rollback) but does not close it — serialise deploys and rollbacks per
        // SERVICE (a service-scoped concurrency group) to eliminate the race.
        let json = fastly_api_get(&format!("/service/{service_id}/version"), &token)?;
        ensure_rollback_from_is_active(resolve_active_version(&json)?, version, &service_id)?;
        // Fastly's activate endpoint requires `PUT` (not `POST`).
        fastly_api_put(
            &format!("/service/{service_id}/version/{previous}/activate"),
            &token,
        )?;
        log::info!("rolled-back-to={previous}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::FastlyCliAdapter;
    #[cfg(unix)]
    use super::super::path_mutation_guard;
    use super::super::run::build_compute_deploy_args;
    #[cfg(unix)]
    use super::super::run::deploy;
    use super::*;
    use edgezero_adapter::registry::{Adapter as _, DeployOwnership};
    #[cfg(unix)]
    use edgezero_adapter::registry::{ProvisionMode, ProvisionStores, ResolvedStoreId};
    #[cfg(unix)]
    use edgezero_core::test_env::{EnvOverride, PathPrepend};
    #[cfg(unix)]
    use std::fs;
    #[cfg(unix)]
    use std::iter::once;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt as _;
    #[cfg(unix)]
    use tempfile::tempdir;
    #[cfg(unix)]
    #[test]
    fn api_transport_uses_execution_path_marker_and_token() {
        let _guard = super::super::path_mutation_guard().lock().unwrap();
        let fallback = tempdir().unwrap();
        fs::write(fallback.path().join("curl"), "#!/bin/sh\nexit 93\n").unwrap();
        fs::set_permissions(
            fallback.path().join("curl"),
            fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        let _path = EnvOverride::set("PATH", fallback.path());
        let selected = tempdir().unwrap();
        fs::write(
            selected.path().join("curl"),
            r#"#!/bin/sh
[ "$EDGEZERO_EXPLICIT_CURL_MARKER" = "selected" ] || exit 91
config=$(/bin/cat)
case "$config" in *'Fastly-Key: explicit-context-token'*) ;; *) exit 92 ;; esac
printf '[]\n200'
"#,
        )
        .unwrap();
        fs::set_permissions(
            selected.path().join("curl"),
            fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        let variables = vec![
            (
                "PATH".to_owned(),
                selected.path().to_string_lossy().into_owned(),
            ),
            (
                "FASTLY_API_TOKEN".to_owned(),
                "explicit-context-token".to_owned(),
            ),
            (
                "EDGEZERO_EXPLICIT_CURL_MARKER".to_owned(),
                "selected".to_owned(),
            ),
        ];
        let exec = AdapterExecContext::new().with_env(&variables);
        assert_eq!(
            fastly_api_get(
                "/service/Selected/version",
                &require_token_with_exec(&exec).unwrap()
            )
            .unwrap(),
            "[]"
        );
    }

    #[cfg(unix)]
    #[test]
    fn managed_native_startup_updates_only_the_external_copy() {
        let root = tempdir().unwrap();
        let manifest = root.path().join("fastly.toml");
        let original = "name = 'demo'\n";
        fs::write(&manifest, original).unwrap();
        let startup = managed_startup_directory(root.path(), &manifest).unwrap();
        let tools = tempdir().unwrap();
        fs::write(
            tools.path().join("fastly"),
            "#!/bin/sh\nprintf 'manifest_version = 3\\n' >> fastly.toml\n",
        )
        .unwrap();
        fs::set_permissions(
            tools.path().join("fastly"),
            fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        let variables = vec![(
            "PATH".to_owned(),
            tools.path().to_string_lossy().into_owned(),
        )];
        let command_context = FastlyCommandContext {
            cwd: startup.path(),
            env: &variables,
        };
        assert!(
            run_fastly_capture_outcome(&[], &command_context)
                .unwrap()
                .success
        );
        assert_eq!(fs::read_to_string(&manifest).unwrap(), original);
        assert!(
            fs::read_to_string(startup.path().join("fastly.toml"))
                .unwrap()
                .contains("manifest_version")
        );
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn managed_native_commands_and_selection_use_explicit_execution_environment() {
        let _guard = super::super::path_mutation_guard().lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let manifest = dir.path().join("fastly.toml");
        fs::write(&manifest, "name = 'demo'\n").unwrap();
        let key = "EDGEZERO__STORES__CONFIG__APP_CONFIG__NAME";
        let variables = vec![
            (
                "FASTLY_API_TOKEN".to_owned(),
                "explicit-context-token".to_owned(),
            ),
            ("FASTLY_SERVICE_ID".to_owned(), "ContextService".to_owned()),
            (key.to_owned(), "context_config".to_owned()),
        ];
        let exec = AdapterExecContext::new().with_env(&variables);
        assert_eq!(
            require_token_with_exec(&exec).unwrap().as_str(),
            "explicit-context-token"
        );
        assert_eq!(
            effective_fastly_service_id_with_exec(&manifest, &exec)
                .unwrap()
                .unwrap()
                .into_id(),
            "ContextService"
        );
        let context = AdapterDeployContext {
            stores: DeployStoreIds {
                config: vec!["app_config".to_owned()],
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(
            effective_deploy_environment(&context, &exec)
                .unwrap()
                .store_name("config", "app_config"),
            "context_config"
        );
        let cwd = FastlyCommandContext {
            cwd: dir.path(),
            env: exec.env(),
        };
        // The command context keeps the provider-selected directory independent of the CLI root.
        assert_eq!(cwd.cwd, dir.path());
        assert_eq!(cwd.env, variables);
    }

    #[test]
    fn probe_with_retries_treats_zero_retry_as_one_attempt() {
        let mut calls: i32 = 0;
        let result = probe_with_retries(
            0,
            || {
                calls += 1_i32;
                Ok(500)
            },
            || {},
        );
        assert_eq!(
            result,
            Err((Some(500), "unhealthy HTTP status 500".to_owned()))
        );
        assert_eq!(calls, 1_i32);
    }

    #[test]
    fn probe_with_retries_reports_transport_error() {
        let result: Result<u16, (Option<u16>, String)> =
            probe_with_retries(1, || Err("connection refused".to_owned()), || {});
        assert_eq!(result, Err((None, "connection refused".to_owned())));
    }

    #[test]
    fn probe_with_retries_exhausts_and_reports_last_code() {
        let mut between: i32 = 0;
        let result = probe_with_retries(3, || Ok(500), || between += 1_i32);
        assert_eq!(
            result,
            Err((Some(500), "unhealthy HTTP status 500".to_owned()))
        );
        assert_eq!(
            between, 2_i32,
            "delay runs between attempts, not after the last"
        );
    }

    #[test]
    fn probe_with_retries_succeeds_after_unhealthy_attempts() {
        let mut calls: i32 = 0;
        let mut between: i32 = 0;
        let result = probe_with_retries(
            5,
            || {
                calls += 1_i32;
                if calls < 3_i32 { Ok(503) } else { Ok(200) }
            },
            || between += 1_i32,
        );
        assert_eq!(result, Ok(200));
        assert_eq!(calls, 3_i32);
        assert_eq!(
            between, 2_i32,
            "delay runs between each of the first 3 attempts"
        );
    }

    #[test]
    fn probe_with_retries_returns_first_healthy() {
        let mut calls: i32 = 0;
        let mut between: i32 = 0;
        let result = probe_with_retries(
            5,
            || {
                calls += 1_i32;
                Ok(200)
            },
            || between += 1_i32,
        );
        assert_eq!(result, Ok(200));
        assert_eq!(calls, 1_i32, "should stop after first healthy probe");
        assert_eq!(between, 0_i32, "no delay before the first attempt");
    }

    #[test]
    fn healthcheck_rejects_zero_timeout() {
        // A zero timeout becomes curl `--max-time 0` (no limit); reject it before
        // any probe. The other required args are valid so we reach the check.
        let args = [
            "--adapter",
            "fastly",
            "--domain",
            "example.com",
            "--service-id",
            "svc123",
            "--version",
            "1",
            "--timeout",
            "0",
        ]
        .map(str::to_owned)
        .to_vec();
        let err = healthcheck(&args).expect_err("zero timeout must be rejected");
        assert!(err.contains("timeout"), "unexpected error: {err}");
    }

    #[test]
    fn validate_probe_path_requires_leading_slash_and_no_whitespace() {
        validate_probe_path("/").expect("root");
        validate_probe_path("/health").expect("simple path");
        validate_probe_path("/api/v1/status?ready=1").expect("path with query");
        validate_probe_path("health").expect_err("no leading slash");
        validate_probe_path("").expect_err("empty");
        validate_probe_path("/ with space").expect_err("whitespace");
        validate_probe_path("/inject\nHost: evil").expect_err("newline injection");
    }

    #[test]
    fn build_curl_probe_args_honors_path_on_production_and_staging() {
        // Production: the path is appended to the domain URL.
        let prod = build_curl_probe_args("example.com", "/health", None, 10);
        assert!(prod.contains(&"https://example.com/health".to_owned()));
        // Staging: same URL (with the path), rerouted to the staging IP.
        let staging =
            build_curl_probe_args("staging.example.com", "/health", Some("151.101.2.10"), 10);
        assert!(staging.contains(&"https://staging.example.com/health".to_owned()));
        let idx = staging
            .iter()
            .position(|arg| arg == "--connect-to")
            .expect("--connect-to present for staging");
        assert_eq!(staging[idx + 1], "::151.101.2.10:443");
    }

    #[test]
    fn build_curl_probe_args_brackets_ipv6_connect_to() {
        let args = build_curl_probe_args("staging.example.com", "/", Some("2001:db8::1"), 10);
        let idx = args
            .iter()
            .position(|arg| arg == "--connect-to")
            .expect("--connect-to present for staging");
        // An IPv6 literal must be bracketed so curl does not misparse the colons.
        assert_eq!(args[idx + 1], "::[2001:db8::1]:443");
    }

    #[test]
    fn build_curl_probe_args_leads_with_q_to_ignore_curlrc() {
        // `-q` must be the FIRST argument or curl merges `~/.curlrc` before it.
        let args = build_curl_probe_args("example.com", "/", None, 10);
        assert_eq!(args.first().map(String::as_str), Some("-q"));
    }

    #[test]
    fn build_curl_probe_args_staging_reroutes_to_ip() {
        let args = build_curl_probe_args("staging.example.com", "/", Some("151.101.2.10"), 15);
        let idx = args
            .iter()
            .position(|arg| arg == "--connect-to")
            .expect("--connect-to present for staging");
        assert_eq!(args[idx + 1], "::151.101.2.10:443");
        assert!(args.contains(&"https://staging.example.com/".to_owned()));
    }

    #[test]
    fn build_curl_probe_args_path_with_glob_chars_is_literal() {
        // A path with `[` `]` would be a curl glob without --globoff; here it must
        // appear verbatim in the single URL argument, with globbing disabled.
        let args = build_curl_probe_args("example.com", "/health?ids[0]=1", None, 10);
        assert!(args.contains(&"--globoff".to_owned()));
        assert!(args.contains(&"https://example.com/health?ids[0]=1".to_owned()));
    }

    #[test]
    fn build_curl_probe_args_production_has_no_connect_to() {
        let args = build_curl_probe_args("example.com", "/", None, 10);
        assert!(!args.iter().any(|arg| arg == "--connect-to"));
        assert!(args.contains(&"https://example.com/".to_owned()));
        assert!(args.contains(&"--max-time".to_owned()));
        assert!(args.contains(&"10".to_owned()));
        // Globbing must be off so bracket/brace characters in a path are literal.
        assert!(args.contains(&"--globoff".to_owned()));
    }

    #[test]
    fn arg_value_reads_flag_value() {
        let args = vec![
            "--service-id".to_owned(),
            "SVC1".to_owned(),
            "--version".to_owned(),
            "42".to_owned(),
        ];
        assert_eq!(arg_value(&args, "--service-id"), Some("SVC1"));
        assert_eq!(arg_value(&args, "--version"), Some("42"));
        assert_eq!(arg_value(&args, "--missing"), None);
    }

    #[test]
    fn arg_value_none_when_flag_is_last() {
        let args = vec!["--version".to_owned()];
        assert_eq!(arg_value(&args, "--version"), None);
    }

    #[test]
    fn arg_flag_detects_presence() {
        let args = vec!["--staging".to_owned()];
        assert!(arg_flag(&args, "--staging"));
        assert!(!arg_flag(&args, "--nope"));
    }

    #[test]
    fn args_without_flag_value_strips_pair() {
        let args = vec![
            "--service-id".to_owned(),
            "SVC1".to_owned(),
            "--comment".to_owned(),
            "ci".to_owned(),
        ];
        assert_eq!(
            args_without_flag_value(&args, "--service-id"),
            vec!["--comment".to_owned(), "ci".to_owned()]
        );
    }

    #[test]
    fn resolve_service_id_prefers_flag() {
        let args = vec!["--service-id".to_owned(), "SVCFROMARG".to_owned()];
        assert_eq!(resolve_service_id(&args).unwrap(), "SVCFROMARG");
    }

    fn owned(args: &[&str]) -> Vec<String> {
        args.iter().map(|arg| (*arg).to_owned()).collect()
    }

    #[cfg(unix)]
    fn fake_provider_invocation_marker(marker: &Path) -> tempfile::TempDir {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempdir().expect("provider fake dir");
        for binary in ["fastly", "curl"] {
            let script_path = dir.path().join(binary);
            fs::write(
                &script_path,
                format!(
                    "#!/bin/sh\nprintf '%s\\n' \"$0 $*\" >> '{}'\nexit 0\n",
                    marker.display()
                ),
            )
            .expect("write provider fake");
            let mut permissions = fs::metadata(&script_path)
                .expect("provider fake metadata")
                .permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&script_path, permissions).expect("chmod provider fake");
        }
        dir
    }

    #[cfg(unix)]
    fn assert_provider_not_invoked(marker: &Path) {
        assert!(
            !marker.exists()
                || fs::read_to_string(marker)
                    .expect("provider marker")
                    .is_empty(),
            "invalid input must be rejected before the provider fake is invoked: {}",
            fs::read_to_string(marker).unwrap_or_default()
        );
    }

    fn assert_service_id_error(error: &str) {
        assert!(
            error.contains("ASCII letters and digits only"),
            "service-id error must state the exact accepted alphabet: {error}"
        );
    }

    #[test]
    fn deploy_arg_scan_rejects_every_reserved_spelling() {
        for args in [
            owned(&["--service-id", "service1"]),
            owned(&["--service-id=service1"]),
            owned(&["-s", "service1"]),
            owned(&["-s=service1"]),
            owned(&["-sservice1"]),
            owned(&["--service-name", "demo"]),
            owned(&["--service-name=demo"]),
            owned(&["--version", "active"]),
            owned(&["--version=active"]),
            owned(&["--autoclone"]),
            owned(&["--token", "secret"]),
            owned(&["--token=secret"]),
            owned(&["-t", "secret"]),
            owned(&["-t=secret"]),
            owned(&["-tsecret"]),
        ] {
            let error = scan_reserved_deploy_args(&args)
                .expect_err("lifecycle-owned deploy argument must be rejected");
            assert!(
                error.contains("reserved") || error.contains("lifecycle"),
                "reserved argument error must explain ownership for {args:?}: {error}"
            );
        }
    }

    #[test]
    fn deploy_arg_scan_redacts_inline_token_values() {
        const SENTINEL: &str = "SUPER_SECRET_TOKEN_SENTINEL";
        for (arg, expected_flag) in [
            (format!("--token={SENTINEL}"), "--token"),
            (format!("-t={SENTINEL}"), "-t"),
            (format!("-t{SENTINEL}"), "-t"),
        ] {
            let error = scan_reserved_deploy_args(&[arg])
                .expect_err("inline credential arguments must be reserved");
            assert!(
                !error.contains(SENTINEL),
                "reserved-argument error leaked a credential: {error}"
            );
            assert!(
                error.contains(expected_flag),
                "reserved-argument error must identify {expected_flag}: {error}"
            );
        }
    }

    #[test]
    fn deploy_arg_scan_allows_unrelated_long_flags_beginning_with_s_or_t() {
        scan_reserved_deploy_args(&owned(&[
            "--skip-build",
            "--status",
            "--timeout=30",
            "--trace",
        ]))
        .expect("long flags must not be mistaken for attached -s or -t values");
    }

    #[test]
    fn package_files_hash_parsers_require_exact_provider_identity() {
        let hash = "a".repeat(128);
        assert_eq!(
            parse_package_files_hash_output(&format!("notice\n{hash}\n"))
                .expect("canonical Fastly CLI hash"),
            hash
        );
        parse_package_files_hash_output("abc").expect_err("short hash must fail");
        parse_package_files_hash_output(&format!("{}\n{}", "a".repeat(128), "b".repeat(128)))
            .expect_err("conflicting hashes must fail");

        let response = serde_json::json!({
            "service_id": "SVC1",
            "version": 8_u64,
            "metadata": { "files_hash": "a".repeat(128) }
        })
        .to_string();
        assert_eq!(
            parse_package_metadata_files_hash(&response, "SVC1", 8)
                .expect("matching package metadata"),
            "a".repeat(128)
        );
        parse_package_metadata_files_hash(&response, "OTHER", 8)
            .expect_err("wrong service must fail");
        parse_package_metadata_files_hash(&response, "SVC1", 9)
            .expect_err("wrong version must fail");
    }

    #[test]
    fn deploy_arg_preflight_scans_store_free_manifest_deploys() {
        let context = AdapterDeployContext {
            service_id: Some("SVC1".to_owned()),
            ..AdapterDeployContext::default()
        };
        assert_eq!(
            FastlyCliAdapter
                .preflight_deploy(
                    &context,
                    &owned(&["--comment", "release"]),
                    &edgezero_adapter::registry::AdapterExecContext::new()
                )
                .expect("safe manifest-command argument"),
            DeployOwnership::ManifestCommand
        );
        FastlyCliAdapter
            .preflight_deploy(
                &context,
                &owned(&["-tsecret"]),
                &edgezero_adapter::registry::AdapterExecContext::new(),
            )
            .expect_err("reserved scan must run before store-free manifest dispatch");
        assert_eq!(
            FastlyCliAdapter.preflight_deploy(
                &context,
                &owned(&["--package", "manifest-package.tar.gz"]),
                &edgezero_adapter::registry::AdapterExecContext::new()
            ),
            Ok(DeployOwnership::ManifestCommand),
            "store-free manifest commands retain provider package passthrough"
        );
    }

    #[test]
    fn managed_deploy_preflight_rejects_release_owned_package_spellings() {
        let context = AdapterDeployContext {
            service_id: Some("SVC1".to_owned()),
            application_release_root: Some(PathBuf::from("release")),
            ..AdapterDeployContext::default()
        };
        for args in [
            owned(&["--package", "app.tar.gz"]),
            owned(&["--package=app.tar.gz"]),
            owned(&["-p", "app.tar.gz"]),
            owned(&["-p=app.tar.gz"]),
            owned(&["-papp.tar.gz"]),
        ] {
            let error = FastlyCliAdapter
                .preflight_deploy(
                    &context,
                    &args,
                    &edgezero_adapter::registry::AdapterExecContext::new(),
                )
                .expect_err("the immutable release owns the package path");
            assert!(error.contains("--package/-p"), "{args:?}: {error}");
        }
    }

    #[test]
    fn managed_deploy_preflight_owns_release_staging_and_each_store_kind() {
        let direct = AdapterDeployContext {
            service_id: Some("SVC1".to_owned()),
            application_manifest_path: Some(PathBuf::from("edgezero.toml")),
            ..AdapterDeployContext::default()
        };
        assert_eq!(
            FastlyCliAdapter.preflight_deploy(
                &direct,
                &[],
                &edgezero_adapter::registry::AdapterExecContext::new()
            ),
            Ok(DeployOwnership::ManifestCommand)
        );

        let mut managed = direct.clone();
        managed.application_release_root = Some(PathBuf::from("release"));
        assert_eq!(
            FastlyCliAdapter.preflight_deploy(
                &managed,
                &[],
                &edgezero_adapter::registry::AdapterExecContext::new()
            ),
            Ok(DeployOwnership::AdapterManaged)
        );

        managed = direct.clone();
        managed.staging = true;
        assert_eq!(
            FastlyCliAdapter.preflight_deploy(
                &managed,
                &[],
                &edgezero_adapter::registry::AdapterExecContext::new()
            ),
            Ok(DeployOwnership::AdapterManaged)
        );

        for stores in [
            DeployStoreIds {
                config: vec!["config".to_owned()],
                ..DeployStoreIds::default()
            },
            DeployStoreIds {
                kv: vec!["kv".to_owned()],
                ..DeployStoreIds::default()
            },
            DeployStoreIds {
                secrets: vec!["secret".to_owned()],
                ..DeployStoreIds::default()
            },
        ] {
            managed = direct.clone();
            managed.stores = stores;
            assert_eq!(
                FastlyCliAdapter.preflight_deploy(
                    &managed,
                    &[],
                    &edgezero_adapter::registry::AdapterExecContext::new()
                ),
                Ok(DeployOwnership::AdapterManaged)
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn managed_deploy_requires_application_release_before_provider_mutation() {
        let _lock = path_mutation_guard().lock().expect("guard");
        let _token = EnvOverride::set(FASTLY_API_TOKEN_ENV, "test-token");
        let context = AdapterDeployContext {
            service_id: Some("SVC1".to_owned()),
            staging: true,
            ..AdapterDeployContext::default()
        };

        let error = FastlyCliAdapter
            .deploy(
                &context,
                &[],
                &edgezero_adapter::registry::AdapterExecContext::new(),
            )
            .expect_err("managed staging deploy requires an immutable release");
        assert!(error.contains("--application-release"), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn manifest_command_finalization_binds_reported_version_to_active_service_version() {
        let _lock = path_mutation_guard().lock().expect("guard");

        let fake = tempdir().expect("provider fake");
        let marker = fake.path().join("provider.log");
        let curl = fake.path().join("curl");
        fs::write(
            &curl,
            format!(
                "#!/bin/sh\ncat >/dev/null\nprintf 'called\\n' >> '{}'\nprintf '[{{\"number\":8,\"active\":true,\"locked\":true,\"staging\":false,\"deployed\":true,\"environments\":[{{\"active_version\":8,\"name\":\"production\",\"service_id\":\"SVC1\"}}]}}]\\n200'\n",
                marker.display()
            ),
        )
        .expect("curl fake");
        let mut permissions = fs::metadata(&curl).expect("curl metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&curl, permissions).expect("curl executable");
        let _path = PathPrepend::new(fake.path());
        let _token = EnvOverride::set(FASTLY_API_TOKEN_ENV, "test-token");
        let context = AdapterDeployContext {
            service_id: Some("SVC1".to_owned()),
            ..AdapterDeployContext::default()
        };

        FastlyCliAdapter
            .finalize_deploy(
                &context,
                Some("version=8"),
                &edgezero_adapter::registry::AdapterExecContext::new(),
            )
            .expect("active version finalization");
        assert!(
            marker.exists(),
            "finalization must consult the exact service"
        );

        let error = FastlyCliAdapter
            .finalize_deploy(
                &context,
                Some("version=9"),
                &edgezero_adapter::registry::AdapterExecContext::new(),
            )
            .expect_err("a version from another service or deployment must fail closed");
        assert!(error.contains('8') && error.contains('9'), "{error}");
    }

    #[test]
    fn deploy_plan_final_argument_parser_accepts_comment_and_global_booleans() {
        let parsed = parse_release_managed_deploy_args(&owned(&[
            "--comment=publisher deployment",
            "--accept-defaults",
            "-d",
            "--auto-yes",
            "-y",
            "--debug-mode",
            "--non-interactive",
            "-i",
            "--quiet",
            "-q",
            "--verbose",
            "-v",
        ]))
        .expect("final managed argument forms");
        assert_eq!(parsed.comment.as_deref(), Some("publisher deployment"));
        assert_eq!(
            parsed.globals,
            owned(&[
                "--accept-defaults",
                "-d",
                "--auto-yes",
                "-y",
                "--debug-mode",
                "--non-interactive",
                "-i",
                "--quiet",
                "-q",
                "--verbose",
                "-v"
            ])
        );

        let detached =
            parse_release_managed_deploy_args(&owned(&["--comment", "publisher deployment"]))
                .expect("detached comment form");
        assert_eq!(detached.comment.as_deref(), Some("publisher deployment"));

        let option_like = parse_release_managed_deploy_args(&owned(&["--comment=--json"]))
            .expect("an attached option-like comment remains a literal value");
        assert_eq!(option_like.comment.as_deref(), Some("--json"));
    }

    #[test]
    fn deploy_plan_final_argument_parser_rejects_every_package_spelling() {
        for args in [
            owned(&["--package", "app.tar.gz"]),
            owned(&["--package=app.tar.gz"]),
            owned(&["-p", "app.tar.gz"]),
            owned(&["-p=app.tar.gz"]),
            owned(&["-papp.tar.gz"]),
        ] {
            let error = parse_release_managed_deploy_args(&args)
                .expect_err("the immutable release owns the package path");
            assert!(error.contains("--package/-p"), "{args:?}: {error}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn deploy_environment_parent_overrides_defaults_then_falls_back_to_logical_ids() {
        let _lock = path_mutation_guard().lock().expect("guard");
        let config_name = "EDGEZERO__STORES__CONFIG__APP_CONFIG__NAME";
        let manifest_config_name = "EDGEZERO__stores__config__app_config__name";
        let kv_name = "EDGEZERO__STORES__KV__SESSIONS__NAME";
        let config_key = "EDGEZERO__STORES__CONFIG__APP_CONFIG__KEY";
        let _parent = EnvOverride::set(config_name, "parent_config");
        let _no_parent_kv = EnvOverride::remove(kv_name);
        let _parent_key = EnvOverride::set(config_key, "parent_key");
        let context = AdapterDeployContext {
            stores: DeployStoreIds {
                config: vec!["app_config".to_owned(), "feature_flags".to_owned()],
                kv: vec!["sessions".to_owned()],
                secrets: vec!["default".to_owned()],
            },
            variable_defaults: BTreeMap::from([
                (
                    manifest_config_name.to_owned(),
                    "manifest_config".to_owned(),
                ),
                (config_key.to_owned(), "manifest_key".to_owned()),
                (kv_name.to_owned(), "manifest_sessions".to_owned()),
            ]),
            ..AdapterDeployContext::default()
        };

        let environment = effective_deploy_environment(&context, &AdapterExecContext::new())
            .expect("valid effective environment");
        assert_eq!(
            environment.store_name("config", "app_config"),
            "parent_config",
            "the parent process must override the manifest default"
        );
        assert_eq!(
            environment.store_name("kv", "sessions"),
            "manifest_sessions",
            "the manifest default must fill an absent parent value"
        );
        assert_eq!(
            environment.store_key("config", "app_config"),
            "parent_key",
            "the parent config key must override the manifest default"
        );
        assert_eq!(
            environment.store_name("config", "feature_flags"),
            "feature_flags",
            "an absent selector must fall back to the logical ID"
        );
        assert_eq!(
            environment.store_name("secrets", "default"),
            "default",
            "the logical id must fill an absent optional Secret Store selector"
        );
    }

    #[cfg(unix)]
    #[test]
    fn deploy_environment_rejects_invalid_present_store_selectors() {
        let _lock = path_mutation_guard().lock().expect("guard");
        let selector = "EDGEZERO__STORES__CONFIG__APP_CONFIG__NAME";
        let _no_parent_selector = EnvOverride::remove(selector);
        let base = AdapterDeployContext {
            stores: DeployStoreIds {
                config: vec!["app_config".to_owned()],
                ..DeployStoreIds::default()
            },
            ..AdapterDeployContext::default()
        };

        let mut invalid_selector = base.clone();
        invalid_selector
            .variable_defaults
            .insert(selector.to_owned(), String::new());
        let selector_error =
            effective_deploy_environment(&invalid_selector, &AdapterExecContext::new())
                .expect_err("a present empty selector must not fall back to the logical id");
        assert!(
            selector_error.contains(selector),
            "error names invalid selector: {selector_error}"
        );

        let _invalid_parent_selector = EnvOverride::set(selector, "bad\nselector");
        let mut invalid_parent = base.clone();
        invalid_parent
            .variable_defaults
            .insert(selector.to_owned(), "valid_manifest_selector".to_owned());
        let parent_error =
            effective_deploy_environment(&invalid_parent, &AdapterExecContext::new())
                .expect_err("an invalid parent selector must not fall back to a valid default");
        assert!(
            parent_error.contains(selector),
            "error names parent selector: {parent_error}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn deploy_environment_rejects_invalid_value_before_provider_cli() {
        let _lock = path_mutation_guard().lock().expect("guard");
        let dir = tempdir().expect("tempdir");
        let manifest = dir.path().join("fastly.toml");
        fs::write(&manifest, "name = \"app\"\n").expect("manifest");
        let marker = dir.path().join("provider.log");
        let fake = fake_provider_invocation_marker(&marker);
        let _path = PathPrepend::new(fake.path());
        let _token = EnvOverride::set(FASTLY_API_TOKEN_ENV, "token");
        let selector = "EDGEZERO__STORES__CONFIG__APP_CONFIG__NAME";
        let _no_parent_selector = EnvOverride::remove(selector);
        let context = AdapterDeployContext {
            adapter_manifest_path: Some(manifest),
            application_manifest_path: None,
            application_release_root: None,
            service_id: Some("SVC1".to_owned()),
            staging: true,
            stores: DeployStoreIds {
                config: vec!["app_config".to_owned()],
                ..DeployStoreIds::default()
            },
            variable_defaults: BTreeMap::from([(selector.to_owned(), String::new())]),
        };

        let error = FastlyCliAdapter
            .deploy(
                &context,
                &[],
                &edgezero_adapter::registry::AdapterExecContext::new(),
            )
            .expect_err("invalid runtime environment must fail managed deploy");
        assert!(error.contains(selector), "error names selector: {error}");
        assert_provider_not_invoked(&marker);
    }

    #[test]
    fn build_compute_deploy_args_is_non_interactive() {
        // Without this a production deploy can block on an interactive
        // prompt in CI.
        let argv = build_compute_deploy_args(&owned(&["--service-id", "SVC1"]));
        assert_eq!(
            argv,
            owned(&[
                "compute",
                "deploy",
                "--service-id",
                "SVC1",
                "--non-interactive"
            ])
        );
    }

    #[test]
    fn build_compute_deploy_args_does_not_duplicate_caller_flag() {
        for flag in ["--non-interactive", "-i"] {
            let argv = build_compute_deploy_args(&owned(&[flag]));
            assert_eq!(
                argv.iter()
                    .filter(|arg| *arg == "--non-interactive" || *arg == "-i")
                    .count(),
                1,
                "must not pass the non-interactive switch twice ({flag})"
            );
        }
    }

    #[test]
    fn healthcheck_rejects_missing_or_empty_required_values_on_production() {
        for (args, needle) in [
            (
                owned(&["--domain", "example.com", "--service-id", "SVC1"]),
                "--version",
            ),
            (
                owned(&[
                    "--domain",
                    "example.com",
                    "--service-id",
                    "SVC1",
                    "--version",
                    "",
                ]),
                "invalid version",
            ),
            (
                owned(&[
                    "--domain",
                    "example.com",
                    "--service-id",
                    "SVC1",
                    "--version",
                    "15.2.0",
                ]),
                "invalid version",
            ),
            (
                owned(&[
                    "--domain",
                    "example.com",
                    "--service-id",
                    "",
                    "--version",
                    "7",
                ]),
                "invalid service id",
            ),
            (
                owned(&["--domain", "", "--service-id", "SVC1", "--version", "7"]),
                "invalid domain",
            ),
            (
                owned(&["--service-id", "SVC1", "--version", "7"]),
                "--domain",
            ),
        ] {
            let err = healthcheck(&args).expect_err("must reject absent/empty required value");
            assert!(
                err.contains(needle),
                "expected {needle:?} in error for {args:?}, got: {err}"
            );
        }
    }

    #[test]
    fn healthcheck_rejects_empty_required_values_on_staging() {
        for args in [
            owned(&[
                "--staging",
                "--domain",
                "example.com",
                "--service-id",
                "",
                "--version",
                "7",
            ]),
            owned(&[
                "--staging",
                "--domain",
                "example.com",
                "--service-id",
                "SVC1",
                "--version",
                "",
            ]),
        ] {
            healthcheck(&args).expect_err("staging must reject empty required values");
        }
    }

    #[test]
    fn rollback_rejects_missing_or_invalid_required_values() {
        for staging in [&[][..], &["--staging".to_owned()][..]] {
            for bad in [
                owned(&["--service-id", "SVC1"]),
                owned(&["--service-id", "SVC1", "--version", ""]),
                owned(&["--service-id", "SVC1", "--version", "12abc"]),
                owned(&["--service-id", "", "--version", "7"]),
            ] {
                let mut args = bad.clone();
                args.extend_from_slice(staging);
                rollback(&args).expect_err("rollback must reject invalid required values");
            }
        }
    }

    #[test]
    fn curl_quote_escapes_quotes_and_backslashes() {
        assert_eq!(curl_quote("plain"), "\"plain\"");
        assert_eq!(curl_quote("a\"b"), "\"a\\\"b\"");
        assert_eq!(curl_quote("a\\b"), "\"a\\\\b\"");
    }

    #[test]
    fn curl_quote_never_emits_raw_control_characters() {
        // A token carrying a `"` and a newline must not be able to
        // terminate its quoted value and inject a second `url = "..."`
        // directive. The `"` is escaped and the newline is folded to a
        // `\n` escape so NO raw newline reaches the curl config file.
        let token = "tok\"en\nurl = \"https://evil.example\"";
        let quoted = curl_quote(token);
        assert!(quoted.starts_with('"') && quoted.ends_with('"'));
        assert!(!quoted.contains('\n'), "no raw newline: {quoted}");
        assert!(!quoted.contains('\r'));
        // The only unescaped `"` are the wrapping pair; every interior
        // quote is preceded by a backslash.
        assert_eq!(quoted, "\"tok\\\"en\\nurl = \\\"https://evil.example\\\"\"");
        // A tab folds too.
        assert_eq!(curl_quote("a\tb"), "\"a\\tb\"");
    }

    #[test]
    fn validate_service_id_accepts_fastly_handle() {
        validate_service_id("SU1Z0isxPaozGVKXdv0eY").expect("alphanumeric handle");
    }

    #[test]
    fn validate_service_id_rejects_punctuation() {
        for invalid in ["SVC1_", "SVC-1", "SVC__OTHER"] {
            let error = validate_service_id(invalid).expect_err("punctuation is not valid");
            assert_service_id_error(&error);
        }
    }

    #[test]
    fn validate_service_id_rejects_injection_and_empty() {
        // The canonical attack: a service id that closes the url value
        // and appends a second url directive.
        validate_service_id("abc\nurl = \"http://evil\"").expect_err("newline injection");
        validate_service_id("abc\"def").expect_err("quote");
        validate_service_id("has space").expect_err("space");
        validate_service_id("has/slash").expect_err("slash");
        validate_service_id("").expect_err("empty");
    }

    #[cfg(unix)]
    #[test]
    fn deploy_rejects_invalid_service_id_before_provider_cli() {
        let _lock = path_mutation_guard().lock().expect("guard");
        let app = tempdir().expect("app dir");
        let manifest = app.path().join("fastly.toml");
        fs::write(&manifest, "name = \"app\"\n").expect("manifest");
        let marker = app.path().join("provider.log");
        let fake = fake_provider_invocation_marker(&marker);
        let _path = PathPrepend::new(fake.path());
        let args = owned(&[
            "--manifest-path",
            manifest.to_str().expect("utf8 manifest"),
            "--service-id",
            "SVC-1",
        ]);

        let error = deploy(
            &args,
            &edgezero_adapter::registry::AdapterExecContext::new(),
        )
        .expect_err("invalid service id must fail deploy");
        assert_service_id_error(&error);
        assert_provider_not_invoked(&marker);
    }

    #[cfg(unix)]
    #[test]
    fn active_version_rejects_invalid_service_id_before_provider_api() {
        let _lock = path_mutation_guard().lock().expect("guard");
        let dir = tempdir().expect("tempdir");
        let marker = dir.path().join("provider.log");
        let fake = fake_provider_invocation_marker(&marker);
        let _path = PathPrepend::new(fake.path());
        let _token = EnvOverride::set(FASTLY_API_TOKEN_ENV, "token");

        let error = emit_active_version(&owned(&["--service-id", "SVC_1"]))
            .expect_err("invalid service id must fail active-version capture");
        assert_service_id_error(&error);
        assert_provider_not_invoked(&marker);
    }

    #[cfg(unix)]
    #[test]
    fn healthcheck_rejects_invalid_service_id_before_provider_api() {
        let _lock = path_mutation_guard().lock().expect("guard");
        let dir = tempdir().expect("tempdir");
        let marker = dir.path().join("provider.log");
        let fake = fake_provider_invocation_marker(&marker);
        let _path = PathPrepend::new(fake.path());
        let _token = EnvOverride::set(FASTLY_API_TOKEN_ENV, "token");
        let args = owned(&[
            "--domain",
            "example.com",
            "--service-id",
            "SVC-1",
            "--version",
            "7",
        ]);

        let error = healthcheck(&args).expect_err("invalid service id must fail healthcheck");
        assert_service_id_error(&error);
        assert_provider_not_invoked(&marker);
    }

    #[cfg(unix)]
    #[test]
    fn rollback_rejects_invalid_service_id_before_provider_api() {
        let _lock = path_mutation_guard().lock().expect("guard");
        let dir = tempdir().expect("tempdir");
        let marker = dir.path().join("provider.log");
        let fake = fake_provider_invocation_marker(&marker);
        let _path = PathPrepend::new(fake.path());
        let _token = EnvOverride::set(FASTLY_API_TOKEN_ENV, "token");
        let args = owned(&[
            "--service-id",
            "SVC_1",
            "--version",
            "7",
            "--rollback-to",
            "6",
        ]);

        let error = rollback(&args).expect_err("invalid service id must fail rollback");
        assert_service_id_error(&error);
        assert_provider_not_invoked(&marker);
    }

    #[cfg(unix)]
    #[test]
    fn provision_rejects_invalid_service_id_before_provider_cli() {
        let _lock = path_mutation_guard().lock().expect("guard");
        let dir = tempdir().expect("tempdir");
        fs::write(
            dir.path().join("fastly.toml"),
            "name = \"app\"\nservice_id = \"SVC-1\"\n",
        )
        .expect("manifest");
        let marker = dir.path().join("provider.log");
        let fake = fake_provider_invocation_marker(&marker);
        let _path = PathPrepend::new(fake.path());
        let _service_env = EnvOverride::remove(FASTLY_SERVICE_ID_ENV);
        let kv = vec![ResolvedStoreId::from_logical("sessions")];
        let stores = ProvisionStores {
            config: &[],
            kv: &kv,
            secrets: &[],
        };

        let error = FastlyCliAdapter
            .provision(
                dir.path(),
                Some("fastly.toml"),
                None,
                &stores,
                None,
                ProvisionMode::Cloud,
                false,
            )
            .expect_err("invalid manifest service id must fail provision");
        assert_service_id_error(&error);
        assert_provider_not_invoked(&marker);
    }

    #[cfg(unix)]
    #[test]
    fn adapter_deploy_rejects_invalid_service_id_before_provider_cli() {
        let _lock = path_mutation_guard().lock().expect("guard");
        let dir = tempdir().expect("tempdir");
        let manifest = dir.path().join("fastly.toml");
        fs::write(&manifest, "name = \"app\"\nservice_id = \"SVC_1\"\n").expect("manifest");
        let marker = dir.path().join("provider.log");
        let fake = fake_provider_invocation_marker(&marker);
        let _path = PathPrepend::new(fake.path());
        let _service_env = EnvOverride::remove(FASTLY_SERVICE_ID_ENV);
        let context = AdapterDeployContext {
            adapter_manifest_path: Some(manifest),
            ..AdapterDeployContext::default()
        };

        let error = FastlyCliAdapter
            .deploy(
                &context,
                &[],
                &edgezero_adapter::registry::AdapterExecContext::new(),
            )
            .expect_err("direct adapter deploy must reject invalid service id");
        assert_service_id_error(&error);
        assert_provider_not_invoked(&marker);
    }

    #[cfg(unix)]
    #[test]
    fn adapter_deploy_discovers_and_rejects_invalid_service_id_before_provider_cli() {
        const CHILD_ENV: &str = "EDGEZERO_FASTLY_DISCOVERY_TEST_CHILD";
        const MARKER_ENV: &str = "EDGEZERO_FASTLY_DISCOVERY_TEST_MARKER";

        if env::var_os(CHILD_ENV).is_some() {
            let marker = PathBuf::from(env::var_os(MARKER_ENV).expect("child marker path"));
            let error = FastlyCliAdapter
                .deploy(
                    &AdapterDeployContext::default(),
                    &[],
                    &edgezero_adapter::registry::AdapterExecContext::new(),
                )
                .expect_err("discovered invalid service id must fail direct adapter deploy");
            assert_service_id_error(&error);
            assert_provider_not_invoked(&marker);
            return;
        }

        let dir = tempdir().expect("tempdir");
        fs::write(dir.path().join("Cargo.toml"), "[package]\nname = \"app\"\n")
            .expect("Cargo manifest");
        fs::write(
            dir.path().join("fastly.toml"),
            "name = \"app\"\nservice_id = \"SVC-1\"\n",
        )
        .expect("manifest");
        let marker = dir.path().join("provider.log");
        let fake = fake_provider_invocation_marker(&marker);
        let path = env::join_paths(
            once(fake.path().to_path_buf())
                .chain(env::split_paths(&env::var_os("PATH").unwrap_or_default())),
        )
        .expect("test PATH");
        let output = Command::new(env::current_exe().expect("current test binary"))
            .args([
                "--exact",
                "cli::tests::adapter_deploy_discovers_and_rejects_invalid_service_id_before_provider_cli",
                "--nocapture",
            ])
            .current_dir(dir.path())
            .env(CHILD_ENV, "1")
            .env(MARKER_ENV, &marker)
            .env("PATH", path)
            .env_remove(FASTLY_SERVICE_ID_ENV)
            .output()
            .expect("run isolated discovery regression");

        assert!(
            output.status.success(),
            "isolated discovery regression failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_provider_not_invoked(&marker);
    }

    #[test]
    fn validate_version_str_accepts_integer_rejects_junk() {
        assert_eq!(validate_version_str("42"), Ok(42));
        assert_eq!(validate_version_str("0"), Ok(0));
        validate_version_str("-1").expect_err("negative");
        validate_version_str("4.2").expect_err("float");
        validate_version_str("42\nurl = \"x\"").expect_err("newline injection");
        validate_version_str("").expect_err("empty");
    }

    #[test]
    fn validate_domain_accepts_hostnames_rejects_injection() {
        validate_domain("example.com").expect("bare hostname");
        validate_domain("staging.example.co.uk").expect("multi-label hostname");
        validate_domain("host-1.example.com").expect("hostname with dash");
        validate_domain("").expect_err("empty");
        validate_domain(".example.com").expect_err("leading dot");
        validate_domain("example.com.").expect_err("trailing dot");
        validate_domain("exa..mple.com").expect_err("empty label");
        validate_domain("example.com/evil").expect_err("slash");
        validate_domain("example.com\nurl = \"x\"").expect_err("newline injection");
        validate_domain("has space.com").expect_err("space");
    }

    #[test]
    fn version_active_verdict_enforces_the_production_version_contract() {
        // The requested version is the active one: healthy.
        version_active_verdict(Some(7), 7, "SVC1", "before probing").expect("match is ok");
        // A different active version (a concurrent deploy) must fail closed and name
        // BOTH versions so the mismatch is diagnosable.
        let err = version_active_verdict(Some(9), 7, "SVC1", "after probing")
            .expect_err("a newer active version must fail the version contract");
        assert!(err.contains('7') && err.contains('9'), "{err}");
        // No active version at all is not a healthy version-7 report either.
        version_active_verdict(None, 7, "SVC1", "before probing")
            .expect_err("no active version must fail the contract");
    }

    #[test]
    fn is_healthy_status_covers_2xx_only() {
        assert!(is_healthy_status(200));
        assert!(is_healthy_status(204));
        assert!(is_healthy_status(299));
        // 3xx is NOT healthy: the probe does not follow redirects, so a 301 to an
        // error page must not pass a gate that suppresses an automatic rollback.
        assert!(!is_healthy_status(301));
        assert!(!is_healthy_status(399));
        assert!(!is_healthy_status(400));
        assert!(!is_healthy_status(500));
        assert!(!is_healthy_status(199));
    }

    #[test]
    fn parse_fastly_version_handles_the_shapes_fastly_emits() {
        // The Fastly CLI's own success lines. Go format strings:
        //   "Updated package (service %s, version %v)"  (compute update)
        //   "Deployed package (service %s, version %v)" (compute deploy)
        assert_eq!(
            parse_fastly_version("SUCCESS: Deployed package (service abc, version 7)"),
            Some(7)
        );
        assert_eq!(
            parse_fastly_version("\nSUCCESS: Updated package (service SU1Z0, version 42)\n"),
            Some(42)
        );
        // Our canonical contract line.
        assert_eq!(parse_fastly_version("version=12"), Some(12));
        // The --autoclone notice, when no success line is present.
        assert_eq!(
            parse_fastly_version(
                "Service version 3 is not editable, so it was automatically cloned because \
                 --autoclone is enabled. Now operating on version 4."
            ),
            Some(4)
        );
        // Full autoclone + success output: the SUCCESS line wins, and the
        // PRE-clone version (3) never does — even though stdout/stderr are
        // concatenated and their relative order is not guaranteed.
        let combined = "SUCCESS: \nUpdated package (service abc, version 4)\n\
             Service version 3 is not editable, so it was automatically cloned. \
             Now operating on version 4.";
        assert_eq!(parse_fastly_version(combined), Some(4));
        assert_eq!(parse_fastly_version("no numbers here"), None);
    }

    #[test]
    fn parse_fastly_version_rejects_confusable_lines() {
        // The old parser took ANY digits after the word "version", so each
        // of these silently produced a WRONG service version. They must now
        // all be `None`, which makes managed deployment fail closed.
        assert_eq!(
            parse_fastly_version("Uploaded package to service 12345, version unchanged"),
            None
        );
        // The CLI's own semver must not be mistaken for a service version.
        assert_eq!(parse_fastly_version("Fastly CLI version 15.2.0"), None);
        assert_eq!(
            parse_fastly_version("Checking version compatibility for service 99"),
            None
        );
        // A bare `version <N>` mention with no success-line context is not
        // trusted either.
        assert_eq!(parse_fastly_version("cloning version 3"), None);
        // `--version=active` echoed in a command line is not a contract line.
        assert_eq!(
            parse_fastly_version("running: fastly compute update --version=active"),
            None
        );
    }

    #[test]
    fn cloned_version_requires_a_new_version_for_the_exact_service() {
        assert_eq!(
            parse_cloned_version(r#"{"service_id":"svc","number":42}"#, "svc", 40),
            Ok(42)
        );
        for invalid in [
            r#"{"service_id":"other","number":42}"#,
            r#"{"service_id":"svc","number":40}"#,
            r#"{"service_id":"svc","number":"42"}"#,
            "[]",
            "not json",
        ] {
            parse_cloned_version(invalid, "svc", 40)
                .expect_err("an ambiguous clone response must fail closed");
        }
    }

    #[test]
    fn parse_active_version_finds_active_entry() {
        let json = r#"[
            {"number":1,"active":false,"locked":true,"staging":false,"deployed":true,"environments":[]},
            {"number":2,"active":true,"locked":true,"staging":false,"deployed":true,"environments":[]},
            {"number":3,"active":false,"locked":false,"staging":false,"deployed":false,"environments":[]}
        ]"#;
        assert_eq!(resolve_active_version(json), Ok(Some(2)));
    }

    #[test]
    fn deploy_plan_version_source_parses_state_and_selects_active() {
        let versions = parse_service_versions(
            r#"[
                {"number":1,"active":false,"locked":true,"staging":false,"deployed":true,"environments":[{"active_version":2,"name":"production","service_id":"SVC1"}]},
                {"number":2,"active":true,"locked":true,"staging":false,"deployed":true,"environments":[{"active_version":2,"name":"production","service_id":"SVC1"}]}
            ]"#,
        )
        .expect("typed version list");
        assert_eq!(versions[1].number, 2);
        assert!(versions[1].active);
        assert!(versions[1].locked);
        assert_eq!(versions[1].environments[0].name, "production");
        assert_eq!(
            select_version_source(&versions),
            Ok(VersionSource::Active(2))
        );
    }

    #[test]
    fn deploy_plan_version_source_parses_complete_fastly_api_records() {
        let versions = parse_service_versions(
            r#"[{
                "active":true,
                "comment":"publisher release",
                "created_at":"2026-09-16T00:00:00Z",
                "deployed":true,
                "environments":[{
                    "active_version":7,
                    "name":"production",
                    "service_id":"SVC1"
                }],
                "locked":true,
                "number":7,
                "service_id":"SVC1",
                "staging":false,
                "testing":false,
                "updated_at":"2026-09-16T00:00:00Z"
            }]"#,
        )
        .expect("authoritative Fastly API version list");
        assert_eq!(versions[0].environments[0].active_version, 7);
        assert_eq!(versions[0].environments[0].name, "production");
        assert_eq!(versions[0].environments[0].service_id, "SVC1");
        assert_eq!(
            select_version_source(&versions),
            Ok(VersionSource::Active(7))
        );
    }

    #[test]
    fn version_source_parses_fastly_cli_15_1_capitalized_records() {
        let versions = parse_service_versions(
            r#"[{
                "Active":true,
                "Comment":"publisher release",
                "Deployed":true,
                "Environments":[{
                    "ServiceVersion":7,
                    "Name":"production",
                    "ServiceID":"SVC1"
                }],
                "Locked":true,
                "Number":7,
                "ServiceID":"SVC1",
                "Staging":false,
                "Testing":false
            }]"#,
        )
        .expect("Fastly CLI 15.1 version JSON");
        assert_eq!(versions[0].environments[0].name, "production");
        assert_eq!(
            select_version_source(&versions),
            Ok(VersionSource::Active(7))
        );
    }

    #[test]
    fn deploy_plan_version_source_rejects_every_missing_authoritative_field() {
        const VERSION_NUMBER: u64 = 1;

        let complete = serde_json::json!({
            "active": false,
            "deployed": false,
            "environments": [],
            "locked": false,
            "number": VERSION_NUMBER,
            "staging": false
        });
        for field in ["active", "environments", "locked", "number"] {
            let mut record = complete.clone();
            record
                .as_object_mut()
                .expect("version object")
                .remove(field);
            let raw = serde_json::json!([record]).to_string();
            parse_service_versions(&raw).expect_err(&format!("missing `{field}` must fail closed"));
        }
    }

    #[test]
    fn deploy_plan_version_source_rejects_orphan_draft_beside_retired_source() {
        let versions = parse_service_versions(
            r#"[
                {"number":1,"active":false,"locked":true,"staging":false,"deployed":false,"environments":[]},
                {"number":2,"active":false,"locked":false,"staging":true,"deployed":true,"environments":[]}
            ]"#,
        )
        .expect("typed version list");
        let error = select_version_source(&versions)
            .expect_err("an editable draft beside a retired source has no provable owner");
        assert!(
            error.contains("editable Fastly draft 2")
                && error.contains("retired version")
                && error.contains("cannot prove"),
            "error must identify the orphan draft and retired source: {error}"
        );
    }

    #[test]
    fn deploy_plan_version_source_selects_unique_shadow_staging_source_without_active() {
        let versions = parse_service_versions(
            r#"[{"number":3,"active":false,"locked":false,"staging":false,"deployed":false,"environments":[{"active_version":3,"name":"staging","service_id":"shadow-staging-service"}]}]"#,
        )
        .expect("staged version list");
        assert_eq!(
            select_version_source(&versions),
            Ok(VersionSource::Staging(3))
        );
    }

    #[test]
    fn deploy_plan_version_source_recovers_first_staging_deactivation() {
        let versions = parse_service_versions(
            r#"[{"number":1,"active":false,"locked":true,"staging":false,"deployed":false,"environments":[]}]"#,
        )
        .expect("retired first staging version");
        assert_eq!(
            select_version_source(&versions),
            Ok(VersionSource::Retired(1))
        );
    }

    #[test]
    fn deploy_plan_version_source_rejects_retry_draft_beside_staging() {
        let versions = parse_service_versions(
            r#"[
                {"number":1,"active":false,"locked":true,"environments":[{"active_version":1,"name":"staging","service_id":"shadow-staging-service"}]},
                {"number":2,"active":false,"locked":false,"environments":[]}
            ]"#,
        )
        .expect("staged source and one retry draft");
        let error = select_version_source(&versions)
            .expect_err("a retry draft must not replace the real staged source");
        assert!(
            error.contains("editable Fastly draft 2")
                && error.contains("staged version 1")
                && error.contains("fastly service version lock --version=2")
                && !error.contains("first deployment"),
            "error must identify both versions and the fail-closed recovery: {error}"
        );
    }

    #[test]
    fn deploy_plan_version_source_rejects_missing_duplicate_or_ambiguous_staging_source() {
        for invalid in [
            r#"[{"number":2,"active":false,"locked":true,"staging":false,"deployed":false,"environments":[{"active_version":2,"name":"staging","service_id":"shadow-staging-service"}]},{"number":3,"active":false,"locked":true,"staging":false,"deployed":true,"environments":[{"active_version":3,"name":"staging","service_id":"shadow-staging-service"}]}]"#,
            r#"[{"number":3,"active":false,"locked":true,"staging":false,"deployed":true,"environments":[{"active_version":2,"name":"staging","service_id":"shadow-staging-service"}]}]"#,
            r#"[{"number":3,"active":false,"locked":true,"staging":true,"deployed":true,"environments":[{"active_version":3,"name":"production","service_id":"SVC1"}]}]"#,
        ] {
            let versions = parse_service_versions(invalid).expect("well-formed version list");
            assert!(
                select_version_source(&versions).is_err(),
                "unsafe staged source must fail closed: {invalid}"
            );
        }
    }

    #[test]
    fn deploy_plan_version_source_rejects_duplicate_global_staging_records() {
        let versions = parse_service_versions(
            r#"[
                {"number":3,"active":false,"locked":true,"environments":[{"active_version":3,"name":"staging","service_id":"shadow-staging-service"}]},
                {"number":4,"active":false,"locked":true,"environments":[{"active_version":3,"name":"staging","service_id":"shadow-staging-service"}]}
            ]"#,
        )
        .expect("well-formed but ambiguous version list");

        let error = select_version_source(&versions)
            .expect_err("more than one staging environment record must fail closed");
        assert!(
            error.contains("more than one staging environment record"),
            "error must identify the provider-state ambiguity: {error}"
        );
    }

    #[test]
    fn deploy_plan_version_source_rejects_missing_duplicate_and_malformed_versions() {
        for invalid in [
            "[]",
            r#"[{"number":1},{"number":1}]"#,
            r#"[{"number":"1"}]"#,
            r#"[{"number":1,"active":"false"}]"#,
            r#"[{"number":1,"locked":"false"}]"#,
            r#"[{"number":1,"environments":"staging"}]"#,
        ] {
            assert!(
                parse_service_versions(invalid).is_err(),
                "invalid version list must fail: {invalid}"
            );
        }
    }

    #[test]
    fn deploy_plan_version_source_ignores_unused_deployed_and_staging_fields() {
        let versions = parse_service_versions(
            r#"[{"number":1,"active":false,"locked":false,"staging":"unused","deployed":{"unused":true},"environments":[]}]"#,
        )
        .expect("unused provider fields must not control version state");
        assert_eq!(
            select_version_source(&versions),
            Ok(VersionSource::InitialDraft(1))
        );
    }

    #[test]
    fn deploy_plan_version_source_rejects_unsafe_or_ambiguous_first_drafts() {
        for invalid in [
            r#"[{"number":1,"active":false,"locked":false,"staging":false,"deployed":false,"environments":[]},{"number":2,"active":false,"locked":true,"staging":false,"deployed":false,"environments":[]}]"#,
            r#"[{"number":1,"active":false,"locked":false,"staging":false,"deployed":false,"environments":[]},{"number":2,"active":false,"locked":false,"staging":true,"deployed":false,"environments":[]}]"#,
            r#"[{"number":1,"active":false,"locked":false,"staging":false,"deployed":false,"environments":[]},{"number":2,"active":false,"locked":false,"staging":false,"deployed":true,"environments":[]}]"#,
            r#"[{"number":1,"active":false,"locked":false,"staging":false,"deployed":false,"environments":[]},{"number":2,"active":false,"locked":false,"staging":false,"deployed":false,"environments":[{"active_version":2,"name":"staging","service_id":"SVC1"}]}]"#,
            r#"[{"number":1,"active":false,"locked":false,"staging":false,"deployed":false,"environments":[]},{"number":2,"active":false,"locked":false,"staging":false,"deployed":false,"environments":[]}]"#,
        ] {
            let versions = parse_service_versions(invalid).expect("well-formed versions");
            assert!(
                select_version_source(&versions).is_err(),
                "unsafe first-deploy source must fail: {invalid}"
            );
        }
    }

    #[test]
    fn publication_barrier_rejects_draft_that_is_no_longer_editable() {
        let versions = parse_service_versions(
            r#"[{"number":7,"active":false,"locked":true,"environments":[]}]"#,
        )
        .expect("locked draft record");

        let error = require_managed_draft_state(VersionSource::InitialDraft(7), &versions, 7)
            .expect_err("publication must reject a locked target");
        assert!(error.contains("no longer an unpublished editable draft"));
    }

    #[test]
    fn publication_barrier_rejects_active_version_appearing_for_initial_draft() {
        let versions = parse_service_versions(
            r#"[
                {"number":6,"active":true,"locked":true,"environments":[{"active_version":6,"name":"production","service_id":"production-service"}]},
                {"number":7,"active":false,"locked":false,"environments":[]}
            ]"#,
        )
        .expect("draft plus foreign active version");

        let error = require_managed_draft_state(VersionSource::InitialDraft(7), &versions, 7)
            .expect_err("publication must reject a newly active version");
        assert!(error.contains("active version appeared before publication"));
    }

    #[test]
    fn publication_barrier_excludes_the_new_clone_when_revalidating_its_source() {
        let versions = parse_service_versions(
            r#"[
                {"number":3,"active":false,"locked":true,"environments":[{"active_version":3,"name":"staging","service_id":"shadow-staging-service"}]},
                {"number":4,"active":false,"locked":false,"environments":[]}
            ]"#,
        )
        .expect("staged source plus cloned target");

        let source_versions = source_versions_excluding(&versions, Some(4));
        assert_eq!(
            select_version_source(&source_versions),
            Ok(VersionSource::Staging(3))
        );
        assert!(
            select_version_source(&versions).is_err(),
            "including the target would misclassify the provider state as ambiguous"
        );
    }

    #[test]
    fn publication_barrier_rejects_package_identity_mismatch() {
        let error = require_package_identity("expected-hash", "foreign-hash", 42)
            .expect_err("publication must reject a replaced package");
        assert!(error.contains("version 42 package identity changed"));
    }

    #[test]
    fn publication_barrier_rejects_protected_configuration_mismatch() {
        fn snapshot(domain: &str) -> VersionConfigurationSnapshot {
            VersionConfigurationSnapshot {
                backends: serde_json::json!([]),
                domains: serde_json::json!([{ "name": domain }]),
                healthchecks: serde_json::json!([]),
                logging: Vec::new(),
                settings: serde_json::json!({}),
            }
        }

        let error = require_draft_configuration(
            &snapshot("expected.example"),
            &snapshot("foreign.example"),
            42,
        )
        .expect_err("publication must reject protected configuration drift");
        assert!(error.contains("protected Compute configuration changed"));
    }

    fn deploy_plan_inventories() -> ResourceInventories {
        ResourceInventories::from_json(
            r#"[
                {"id":"CONFIG_A","name":"config-a"},
                {"id":"CONFIG_B","name":"config-b"}
            ]"#,
            r#"[{"id":"KV_A","name":"kv-a"},{"id":"KV_B","name":"kv-b"}]"#,
            r#"[{"id":"SECRET_A","name":"secret-a"}]"#,
        )
        .expect("valid inventories")
    }

    fn desired_link(
        kind: ResourceKind,
        alias: &str,
        selected_name: &str,
        resource_id: &str,
    ) -> DesiredResourceLink {
        DesiredResourceLink {
            alias: alias.to_owned(),
            kind,
            resource_id: resource_id.to_owned(),
            selected_name: selected_name.to_owned(),
        }
    }

    fn existing_link(
        kind: ResourceKind,
        alias: &str,
        resource_id: &str,
        link_id: &str,
    ) -> ExistingResourceLink {
        ExistingResourceLink {
            alias: alias.to_owned(),
            kind,
            link_id: link_id.to_owned(),
            resource_id: resource_id.to_owned(),
        }
    }

    #[test]
    fn deploy_plan_uses_logical_aliases_and_selected_physical_resources() {
        let environment = EnvConfig::from_vars([
            ("EDGEZERO__STORES__CONFIG__SHARED__NAME", "config-a"),
            ("EDGEZERO__STORES__KV__SHARED__NAME", "kv-a"),
            ("EDGEZERO__STORES__SECRETS__CREDENTIALS__NAME", "secret-a"),
        ]);
        let desired = desired_resource_links(
            &RuntimeStoreIds {
                config: vec!["shared".to_owned()],
                kv: vec!["shared".to_owned()],
                secrets: vec!["credentials".to_owned()],
            },
            &environment,
            &deploy_plan_inventories(),
        )
        .expect("desired links");

        assert_eq!(
            desired,
            vec![
                desired_link(ResourceKind::Config, "shared", "config-a", "CONFIG_A"),
                desired_link(ResourceKind::Kv, "shared", "kv-a", "KV_A"),
                desired_link(ResourceKind::Secret, "credentials", "secret-a", "SECRET_A",),
            ]
        );
    }

    #[test]
    fn deploy_plan_rejects_present_invalid_store_name_before_inventory_lookup() {
        for value in ["", "  ", "bad\nname"] {
            let environment =
                EnvConfig::from_vars([("EDGEZERO__STORES__CONFIG__APP_CONFIG__NAME", value)]);
            let error = desired_resource_links(
                &RuntimeStoreIds {
                    config: vec!["app_config".to_owned()],
                    ..RuntimeStoreIds::default()
                },
                &environment,
                &deploy_plan_inventories(),
            )
            .expect_err("present invalid selector must fail");
            assert!(
                error.contains("EDGEZERO__STORES__CONFIG__APP_CONFIG__NAME"),
                "invalid selector error must name its canonical variable"
            );
        }
    }

    #[test]
    fn deploy_plan_replaces_declared_identity_and_preserves_undeclared_links() {
        let desired = vec![desired_link(
            ResourceKind::Config,
            "app_config",
            "config-b",
            "CONFIG_B",
        )];
        let existing = vec![
            existing_link(ResourceKind::Config, "app_config", "CONFIG_A", "OLD_CONFIG"),
            existing_link(ResourceKind::Kv, "sessions", "KV_A", "KEEP_KV"),
        ];

        let plan = plan_link_reconciliation(&desired, &existing, &deploy_plan_inventories())
            .expect("reconciliation");
        assert_eq!(plan.delete_link_ids, vec!["OLD_CONFIG"]);
        assert_eq!(plan.create, desired);
    }

    #[test]
    fn deploy_plan_preserves_undeclared_link_absent_from_visible_inventories() {
        let existing = vec![existing_link(
            ResourceKind::Secret,
            "shared_by_another_app",
            "INACCESSIBLE_SECRET",
            "KEEP_SECRET",
        )];

        let plan = plan_link_reconciliation(&[], &existing, &deploy_plan_inventories())
            .expect("an undeclared inherited link does not require inventory visibility");
        assert!(plan.delete_link_ids.is_empty());
        assert!(plan.create.is_empty());
    }

    #[test]
    fn deploy_plan_keeps_same_alias_isolated_by_resource_kind() {
        let desired = vec![
            desired_link(ResourceKind::Config, "shared", "config-a", "CONFIG_A"),
            desired_link(ResourceKind::Kv, "shared", "kv-b", "KV_B"),
        ];
        let existing = vec![
            existing_link(ResourceKind::Config, "shared", "CONFIG_A", "CONFIG_LINK"),
            existing_link(ResourceKind::Kv, "shared", "KV_A", "KV_LINK"),
        ];

        let plan = plan_link_reconciliation(&desired, &existing, &deploy_plan_inventories())
            .expect("kind-isolated reconciliation");
        assert_eq!(plan.delete_link_ids, vec!["KV_LINK"]);
        assert_eq!(
            plan.create,
            vec![desired_link(ResourceKind::Kv, "shared", "kv-b", "KV_B")]
        );
    }

    #[test]
    fn deploy_plan_rejects_reported_kind_that_conflicts_with_inventory() {
        let existing = vec![existing_link(
            ResourceKind::Secret,
            "credentials",
            "CONFIG_A",
            "BAD_KIND",
        )];
        let error = plan_link_reconciliation(&[], &existing, &deploy_plan_inventories())
            .expect_err("kind conflict must fail");
        assert!(error.contains("reports Secret Store"), "{error}");
        assert!(error.contains("belongs to Config Store"), "{error}");
    }

    #[test]
    fn resource_link_parser_requires_known_resource_type() {
        let raw = r#"[
            {"id":"CONFIG_LINK","name":"shared","resource_id":"CONFIG_A","resource_type":"config"},
            {"id":"KV_LINK","name":"shared","resource_id":"KV_A","resource_type":"kv-store"},
            {"id":"SECRET_LINK","name":"credentials","resource_id":"SECRET_A","resource_type":"secret-store"}
        ]"#;
        let (links, _) = parse_resource_links(raw).expect("typed links");
        assert_eq!(links[0].kind, ResourceKind::Config);
        assert_eq!(links[1].kind, ResourceKind::Kv);
        assert_eq!(links[2].kind, ResourceKind::Secret);

        let unknown =
            r#"[{"id":"LINK","name":"x","resource_id":"X","resource_type":"dictionary"}]"#;
        let error = parse_resource_links(unknown).expect_err("unknown type must fail");
        assert!(error.contains("unknown `resource_type`"), "{error}");
    }

    #[test]
    fn version_configuration_snapshot_normalizes_clone_metadata_and_order() {
        assert_eq!(
            parse_snapshot_array(
                "backends",
                r#"[
                    {"name":"origin-b","hostname":"b.example","service_id":"SVC","version":40,"created_at":"old"},
                    {"name":"origin-a","hostname":"a.example","locked":true,"updated_at":"old"}
                ]"#,
            )
            .expect("source snapshot"),
            parse_snapshot_array(
                "backends",
                r#"[
                    {"name":"origin-a","hostname":"a.example","locked":false,"updated_at":"new"},
                    {"name":"origin-b","hostname":"b.example","service_id":"SVC","version":42,"created_at":"new"}
                ]"#,
            )
            .expect("clone snapshot")
        );

        parse_snapshot_array("backends", r#"[{"name":"origin"}, null]"#)
            .expect_err("non-object collection entries must fail closed");
        parse_snapshot_array("backends", "{}").expect_err("objects are not collections");
        parse_snapshot_object("settings", "[]").expect_err("arrays are not settings objects");
        assert_eq!(
            parse_snapshot_object(
                "settings",
                r#"{"general.default_ttl":3600,"service_id":"SVC","version":40}"#,
            ),
            parse_snapshot_object(
                "settings",
                r#"{"general.default_ttl":3600,"service_id":"SVC","version":42}"#,
            )
        );
    }

    #[test]
    fn version_configuration_snapshot_ignores_json_object_key_order() {
        let source = parse_snapshot_array(
            "backends",
            r#"[
                {"name":"origin-a","hostname":"z.example","tls":{"check":true,"sni":"z.example"}},
                {"name":"origin-b","hostname":"a.example","tls":{"check":false,"sni":"a.example"}}
            ]"#,
        )
        .expect("source snapshot");
        let reread = parse_snapshot_array(
            "backends",
            r#"[
                {"tls":{"sni":"a.example","check":false},"hostname":"a.example","name":"origin-b"},
                {"tls":{"sni":"z.example","check":true},"hostname":"z.example","name":"origin-a"}
            ]"#,
        )
        .expect("reordered snapshot");

        assert_eq!(source, reread);
    }

    #[test]
    fn logging_snapshot_uses_fastly_api_provider_paths() {
        assert!(FASTLY_LOGGING_PROVIDER_KINDS.contains(&"pubsub"));
        assert!(FASTLY_LOGGING_PROVIDER_KINDS.contains(&"logentries"));
        assert!(FASTLY_LOGGING_PROVIDER_KINDS.contains(&"s3"));
        assert!(!FASTLY_LOGGING_PROVIDER_KINDS.contains(&"googlepubsub"));
    }

    #[test]
    fn fastly_config_keys_use_the_logical_id_for_every_target() {
        validate_fastly_config_key("app_config", "app_config", false, false)
            .expect("production key");
        validate_fastly_config_key("app_config", "app_config", true, false)
            .expect("staging target key");
        validate_fastly_config_key("app_config", "app_config", false, true).expect("local key");

        for (key, staging, local) in [
            ("custom", false, false),
            ("alternate", true, false),
            ("alternate", false, true),
        ] {
            let error = validate_fastly_config_key("app_config", key, staging, local)
                .expect_err("conflicting key must fail");
            assert!(error.contains("logical config key"), "{error}");
        }
    }

    #[test]
    fn parse_active_version_none_when_no_active() {
        // A parsed list with no active version is `Ok(None)` — confirmed
        // no active version (first deploy), NOT an operational failure.
        let json = r#"[{"number":1,"active":false,"locked":false,"staging":false,"deployed":false,"environments":[]}]"#;
        assert_eq!(resolve_active_version(json), Ok(None));
    }

    #[test]
    fn resolve_active_version_errors_on_unparseable_payload() {
        // A truncated / non-array body is an operational failure, distinct from
        // "no active version" — the caller must fail closed, not record empty.
        resolve_active_version("not json").expect_err("non-JSON must be an operational error");
        resolve_active_version(r#"{"error":"unauthorized"}"#)
            .expect_err("a non-array body must be an operational error");
    }

    #[test]
    fn resolve_active_version_errors_on_malformed_active_entries() {
        // A garbled ACTIVE entry must fail closed, not read as "no active
        // version" — otherwise a production deploy proceeds with no rollback
        // target. Each of these is malformed and must be an operational error.
        resolve_active_version(r#"[{"active":true}]"#)
            .expect_err("active entry with no `number` must error");
        resolve_active_version(r#"[{"active":true,"number":"7"}]"#)
            .expect_err("active entry with a string `number` must error");
        resolve_active_version(r#"[{"active":"true","number":7}]"#)
            .expect_err("a non-boolean `active` must error");
        // A non-boolean `active` ANYWHERE is schema drift — the whole list is
        // scanned, so it is caught even AFTER a valid active entry (a naive
        // first-match parser would miss this one).
        resolve_active_version(r#"[{"active":"false"},{"active":true,"number":9}]"#)
            .expect_err("a non-boolean `active` before the active entry is schema drift");
        resolve_active_version(r#"[{"active":true,"number":9},{"active":"nope"}]"#)
            .expect_err("a non-boolean `active` AFTER the active entry is still schema drift");
        // More than one active version is ambiguous — refuse rather than pick one.
        resolve_active_version(r#"[{"active":true,"number":9,"locked":true,"staging":false,"deployed":true,"environments":[]},{"active":true,"number":10,"locked":true,"staging":false,"deployed":true,"environments":[]}]"#)
            .expect_err("two active versions must error as ambiguous");
        // EVERY element must be a version object with a numeric `number` — a
        // garbled entry must fail closed, not be skipped as "not active".
        resolve_active_version("[]").expect_err("an empty version list is an invalid response");
        resolve_active_version("[null]").expect_err("a null element must error");
        resolve_active_version("[{}]").expect_err("an entry with no `number` must error");
        resolve_active_version(r#"[{"number":"invalid"}]"#)
            .expect_err("a non-numeric `number` must error");
        // Every safety field is mandatory; omission is schema drift.
        resolve_active_version(
            r#"[{"number":42,"locked":false,"staging":false,"deployed":false,"environments":[]}]"#,
        )
        .expect_err("an omitted active field must fail closed");
        // Sanity: a well-formed list still resolves.
        assert_eq!(
            resolve_active_version(
                r#"[{"active":false,"number":1,"locked":true,"staging":false,"deployed":true,"environments":[]},{"active":true,"number":2,"locked":true,"staging":false,"deployed":true,"environments":[]}]"#
            ),
            Ok(Some(2))
        );
    }

    #[test]
    fn ensure_rollback_from_is_active_blocks_racing_deploys() {
        // The version being rolled back FROM is still active → proceed.
        assert_eq!(ensure_rollback_from_is_active(Some(7), 7, "svc"), Ok(()));
        // A NEWER version is active (a deploy raced the rollback) → refuse, so
        // the newer deploy is not clobbered.
        ensure_rollback_from_is_active(Some(9), 7, "svc")
            .expect_err("a newer active version must block the rollback");
        // No active version at all → refuse.
        ensure_rollback_from_is_active(None, 7, "svc")
            .expect_err("no active version must block the rollback");
    }

    #[test]
    fn staging_rollback_after_failure_before_stage_is_a_noop() {
        let versions = parse_service_versions(
            r#"[{"active":false,"number":7,"locked":false,"staging":true,"deployed":true,"environments":[]}]"#,
        )
        .expect("version list");
        assert_eq!(
            staging_rollback_decision(&versions, 7, "svc"),
            Ok(StagingRollbackDecision::NoopDraft)
        );
    }

    #[test]
    fn staging_rollback_after_failure_after_stage_deactivates_exact_version() {
        let versions = parse_service_versions(
            r#"[{"active":false,"number":7,"locked":false,"staging":false,"deployed":false,"environments":[{"active_version":7,"name":"staging","service_id":"shadow-staging-service"}]}]"#,
        )
        .expect("version list");
        assert_eq!(
            staging_rollback_decision(&versions, 7, "svc"),
            Ok(StagingRollbackDecision::Deactivate)
        );
        staging_rollback_decision(&versions, 8, "svc")
            .expect_err("an absent version must fail closed");
    }

    #[test]
    fn staging_rollback_uses_the_exact_staging_environment_record() {
        let versions = parse_service_versions(
            r#"[{"active":true,"number":7,"locked":true,"staging":false,"deployed":false,"environments":[{"active_version":7,"name":"production","service_id":"production-service"},{"active_version":7,"name":"staging","service_id":"shadow-staging-service"}]}]"#,
        )
        .expect("version list");
        assert_eq!(
            staging_rollback_decision(&versions, 7, "svc"),
            Ok(StagingRollbackDecision::Deactivate)
        );

        let duplicate = parse_service_versions(
            r#"[{"active":false,"number":7,"locked":true,"environments":[{"active_version":7,"name":"staging","service_id":"shadow-staging-service"},{"active_version":7,"name":"staging","service_id":"shadow-staging-service"}]}]"#,
        )
        .expect("version list");
        staging_rollback_decision(&duplicate, 7, "svc")
            .expect_err("duplicate staging environment records must fail closed");

        let cross_version_duplicate = parse_service_versions(
            r#"[{"active":false,"number":7,"locked":true,"environments":[{"active_version":7,"name":"staging","service_id":"shadow-staging-service"}]},{"active":false,"number":8,"locked":true,"environments":[{"active_version":8,"name":"staging","service_id":"shadow-staging-service"}]}]"#,
        )
        .expect("version list");
        staging_rollback_decision(&cross_version_duplicate, 7, "svc")
            .expect_err("staging records on multiple versions must fail closed");
    }

    #[test]
    fn active_version_or_require_enforces_require_active() {
        let active = r#"[{"active":true,"number":5,"locked":true,"staging":false,"deployed":true,"environments":[]}]"#;
        let none = r#"[{"active":false,"number":5,"locked":false,"staging":false,"deployed":false,"environments":[]}]"#;

        // A resolvable active version is returned regardless of the flag.
        assert_eq!(active_version_or_require(active, false, "svc"), Ok(Some(5)));
        assert_eq!(active_version_or_require(active, true, "svc"), Ok(Some(5)));

        // No active version: tolerated for `active-version` (first deploy), but an
        // ERROR for the production-deploy fallback (`--require-active`), which
        // must never report a deploy with no resolvable version.
        assert_eq!(active_version_or_require(none, false, "svc"), Ok(None));
        active_version_or_require(none, true, "svc")
            .expect_err("require-active with no active version must fail closed");

        // A malformed response is an error either way.
        active_version_or_require("not json", false, "svc").expect_err("malformed must error");
    }

    #[test]
    fn parse_staging_ip_reads_the_singular_staging_ip_field() {
        // The REAL Fastly response shape for
        // `GET /service/<id>/version/<n>/domain?include=staging_ips`:
        // an array of domain objects, each with a SINGULAR `staging_ip`
        // STRING. Body copied from go-fastly's recorded API fixture
        // `fastly/fixtures/domains/list_with_staging_ips.yaml`, matching
        // its `StagingIP *string `mapstructure:"staging_ip"`` field.
        // (`staging_ips` is only the `include=` query value, never a
        // field name — the previous parser looked for it as an array and
        // therefore NEVER found a staging IP.)
        let json = r#"[
            {
                "created_at": "2022-11-04T17:36:56Z",
                "service_id": "kKJb5bOFI47uHeBVluGfX1",
                "name": "integ-test-20221104.go-fastly-1.com",
                "version": 73,
                "comment": "comment",
                "deleted_at": null,
                "staging_ip": "167.82.81.194"
            }
        ]"#;
        assert_eq!(
            parse_staging_ip(json, "integ-test-20221104.go-fastly-1.com"),
            Ok("167.82.81.194".to_owned())
        );
    }

    #[test]
    fn parse_staging_ip_selects_the_requested_domain() {
        let json = r#"[
            {"name":"other.example.com","staging_ip":"151.101.1.10"},
            {"name":"example.com","staging_ip":"151.101.2.10"}
        ]"#;
        assert_eq!(
            parse_staging_ip(json, "example.com"),
            Ok("151.101.2.10".to_owned())
        );
    }

    #[test]
    fn parse_staging_ip_rejects_missing_duplicate_or_malformed_domain_records() {
        for json in [
            r#"[{"name":"other.example.com","staging_ip":"151.101.1.10"}]"#,
            r#"[{"name":"example.com","staging_ip":null}]"#,
            r#"[{"name":"example.com","staging_ips":["151.101.2.10"]}]"#,
            r#"[{"name":"example.com","staging_ip":"151.101.2.10"},{"name":"example.com","staging_ip":"151.101.2.11"}]"#,
            r#"{"name":"example.com","staging_ip":"151.101.2.10"}"#,
        ] {
            parse_staging_ip(json, "example.com")
                .expect_err("ambiguous or malformed domain inventory must fail closed");
        }
    }
}
