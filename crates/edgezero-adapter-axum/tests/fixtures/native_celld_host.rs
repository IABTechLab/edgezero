//! Public-runner child for deterministic store-service HTTP startup tests, not a celld service.

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
use edgezero_core::extractor::State;
use edgezero_core::router::RouterService;
use edgezero_core::{EdgeError, action};
use tokio::time::{sleep, timeout};

struct NativeCelldApp;

#[expect(
    clippy::missing_trait_methods,
    reason = "fixture retains unused Hooks defaults"
)]
impl Hooks for NativeCelldApp {
    fn routes() -> Result<RouterService, EdgeError> {
        Ok(RouterService::builder()
            .get("/startup", startup)
            .get("/kv", kv)
            .build())
    }

    fn stores() -> StoresMetadata {
        StoresMetadata {
            config: None,
            kv: Some(StoreMetadata {
                ids: &["state", "other"],
                default: "state",
            }),
            secrets: None,
        }
    }
}

#[action]
async fn startup(State(value): State<Arc<str>>) -> Result<String, EdgeError> {
    Ok(value.to_string())
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
        // The initializer may use live KV, which performs its first remote call here.
        let store = stores
            .kv()
            .and_then(|registry| registry.named("state"))
            .ok_or_else(|| EdgeError::service_unavailable("fixture KV missing"))?;
        let bytes = store
            .get_bytes("value")
            .await?
            .ok_or_else(|| EdgeError::service_unavailable("fixture value missing"))?;
        let value = String::from_utf8(bytes.to_vec())
            .map_err(|_error| EdgeError::internal(anyhow::anyhow!("fixture value malformed")))?;
        app.insert_state(Arc::<str>::from(value));
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
