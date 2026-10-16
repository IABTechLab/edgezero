//! Public-runner child for deterministic scoped HTTP startup tests, not a celld service.

use std::env::var_os;
use std::fs::write;
use std::future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use edgezero_adapter_axum::dev_server::{
    AxumRunOptions, LocalStartupFuture, PreparedStores, run_app_with_native_stores,
};
use edgezero_adapter_axum::native_stores::{NativeStoreOverrides, NativeStorePreparationError};
use edgezero_core::app::{App, Hooks, StoreMetadata, StoresMetadata};
use edgezero_core::context::RequestContext;
use edgezero_core::extractor::{AppConfig as TypedAppConfig, State};
use edgezero_core::router::RouterService;
use edgezero_core::{EdgeError, action};
use serde::Deserialize;
use tokio::time::{sleep, timeout};
use validator::Validate;

struct NativeCelldApp;

#[derive(Deserialize, Validate, edgezero_core::AppConfig)]
struct FixtureConfig {
    #[validate(length(min = 4_u64))]
    greeting: String,
}

#[expect(
    clippy::missing_trait_methods,
    reason = "fixture retains unused Hooks defaults"
)]
impl Hooks for NativeCelldApp {
    fn routes() -> RouterService {
        RouterService::builder()
            .get("/snapshot", snapshot)
            .get("/kv", kv)
            .build()
    }

    fn stores() -> StoresMetadata {
        StoresMetadata {
            config: Some(StoreMetadata {
                ids: &["app"],
                default: "app",
            }),
            kv: Some(StoreMetadata {
                ids: &["state"],
                default: "state",
            }),
            secrets: None,
        }
    }
}

#[action]
async fn snapshot(State(greeting): State<Arc<str>>) -> Result<String, EdgeError> {
    Ok(greeting.to_string())
}

#[action]
async fn kv(ctx: RequestContext) -> Result<String, EdgeError> {
    let store = ctx
        .kv_store("state")
        .ok_or_else(|| EdgeError::service_unavailable("fixture KV missing"))?;
    let bytes = store
        .get_bytes("value")
        .await?
        .ok_or_else(|| EdgeError::service_unavailable("fixture value missing"))?;
    String::from_utf8(bytes.to_vec())
        .map_err(|_error| EdgeError::internal(anyhow::anyhow!("fixture value malformed")))
}

fn initialize<'startup>(
    app: &'startup mut App,
    stores: &'startup PreparedStores,
) -> LocalStartupFuture<'startup> {
    Box::pin(async move {
        let binding = stores
            .config()
            .and_then(|registry| registry.named_ref("app"))
            .ok_or_else(|| EdgeError::service_unavailable("fixture config missing"))?;
        let config = TypedAppConfig::<FixtureConfig>::from_binding(
            binding,
            None,
            stores.secrets(),
            app.config_extraction_limits(),
            app.monotonic_clock(),
        )
        .await?;
        if let Some(marker) = var_os("FIXTURE_INITIALIZER_ENTERED") {
            write(marker, b"entered").map_err(|_error| {
                EdgeError::internal(anyhow::anyhow!("fixture marker unavailable"))
            })?;
        }
        if let Some(path) = var_os("FIXTURE_INITIALIZER_GATE") {
            let gate = PathBuf::from(path);
            timeout(Duration::from_secs(8), async {
                while !gate.is_file() {
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .map_err(|_error| {
                EdgeError::internal(anyhow::anyhow!("fixture initializer deadline"))
            })?;
        }
        app.insert_state(Arc::<str>::from(config.greeting));
        Ok(())
    })
}

fn main() -> anyhow::Result<()> {
    let mut overrides = NativeStoreOverrides::default();
    if let Some(marker) = var_os("FIXTURE_DEFERRED_MARKER") {
        overrides.prepare_kv("state", async move {
            write(marker, b"polled").map_err(|_error| NativeStorePreparationError::Unavailable)?;
            Err(NativeStorePreparationError::Unavailable)
        })?;
    }
    run_app_with_native_stores::<NativeCelldApp, _, _>(
        AxumRunOptions::from_env()?,
        overrides,
        future::pending::<()>(),
        initialize,
    )?;
    Ok(())
}
