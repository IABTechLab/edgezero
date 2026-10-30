#![cfg(target_arch = "wasm32")]
#![allow(
    unsafe_code,
    reason = "spin's #[http_service] macro generates the unsafe wasm export"
)]

#[cfg(feature = "progress")]
mod progress;
use edgezero_core::MonotonicClock;
use edgezero_core::app::{App, Hooks};
#[cfg(not(feature = "qualification"))]
use fixture_core::DeclaredKvApp as FixtureApp;
use fixture_core::assembly::{ConfigurationFailureApp, InternalFailureApp, RetryApp};
#[cfg(feature = "qualification")]
use fixture_core::qualification::QualificationApp as FixtureApp;
use fixture_core::{OtherApp, retained_app};
use spin_sdk::{
    http::{IntoResponse, Request},
    http_service,
};
use std::sync::OnceLock;
static APP: OnceLock<App> = OnceLock::new();
static OTHER_APP: OnceLock<App> = OnceLock::new();
static ASSEMBLY_APP: OnceLock<App> = OnceLock::new();

fn telemetry(line: &str) {
    eprintln!("{line}");
}

async fn dispatch_selected<A: Hooks>(
    req: Request,
    retained: bool,
    cell: &OnceLock<App>,
) -> anyhow::Result<edgezero_adapter_spin::SpinResponse> {
    if !retained {
        return edgezero_adapter_spin::run_app::<A>(req).await;
    }
    let clock = MonotonicClock::default();
    let start = clock.now();
    match retained_app::<A>(cell, edgezero_adapter_spin::SPIN_PLATFORM) {
        Ok(app) => edgezero_adapter_spin::dispatch_app::<A>(app, req).await,
        Err(error) => Ok(edgezero_adapter_spin::request::send_assembly_error(
            req, error, start, clock,
        )?),
    }
}

#[http_service]
async fn handle(req: Request) -> anyhow::Result<edgezero_adapter_spin::SpinResponse> {
    let mode = std::env::var("FIXTURE_MODE").unwrap_or_else(|_| "per-request".to_owned());
    fixture_core::launch::initialize_environment(&format!("fixture-spin-{mode}"));
    #[cfg(feature = "progress")]
    if req.uri().path().starts_with("/progress/") {
        return progress::dispatch(req).await;
    }
    if !FixtureApp::owns_logging() {
        drop(edgezero_adapter_spin::init_logger());
    } else {
        fixture_core::init_fixture_logging(telemetry)?;
    }
    let retained = std::env::var("FIXTURE_MODE").as_deref() == Ok("retained");
    match req.uri().path() {
        "/assembly/configuration" => {
            return edgezero_adapter_spin::run_app::<ConfigurationFailureApp>(req).await;
        }
        "/assembly/internal" => {
            return edgezero_adapter_spin::run_app::<InternalFailureApp>(req).await;
        }
        "/assembly/retry" | "/assembly/retry/missing" => {
            return dispatch_selected::<RetryApp<false>>(req, retained, &ASSEMBLY_APP).await;
        }
        _ => {}
    }
    if retained && req.uri().path() == "/binding-failure" {
        let result =
            edgezero_adapter_spin::key_value_store::SpinKvStore::open("missing_fixture_kv").await;
        return match result {
            Err(error) => {
                let body = serde_json::json!({"instance":fixture_core::instance_id(),"source":"injected_required_binding","error":format!("{error:#}")}).to_string();
                edgezero_core::http::response_builder()
                    .status(503)
                    .header("content-type", "application/json")
                    .body(body)?
                    .into_response()
                    .map_err(|error| {
                        anyhow::anyhow!("fixture response conversion failed: {error:?}")
                    })
            }
            Ok(_) => Err(anyhow::anyhow!(
                "injected missing binding unexpectedly opened"
            )),
        };
    }
    if req.uri().path() == "/bindings" {
        let store = spin_sdk::key_value::Store::open("fixture_config").await?;
        store.set("marker", b"local-fixture").await?;
    }
    if req.uri().path() == "/other" {
        return dispatch_selected::<OtherApp>(req, retained, &OTHER_APP).await;
    }
    dispatch_selected::<FixtureApp>(req, retained, &APP).await
}
