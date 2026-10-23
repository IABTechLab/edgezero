#![cfg(target_arch = "wasm32")]

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
use std::sync::OnceLock;
use worker::{Context, Env, Request, Response, event};
static APP: OnceLock<App> = OnceLock::new();
static OTHER_APP: OnceLock<App> = OnceLock::new();
static ASSEMBLY_APP: OnceLock<App> = OnceLock::new();

fn telemetry(line: &str) {
    worker::console_log!("{line}");
}

async fn dispatch_selected<A: Hooks>(
    req: Request,
    env: Env,
    ctx: Context,
    retained: bool,
    cell: &OnceLock<App>,
) -> worker::Result<Response> {
    if !retained {
        return edgezero_adapter_cloudflare::run_app::<A>(req, env, ctx).await;
    }
    let clock = MonotonicClock::default();
    let start = clock.now();
    match retained_app::<A>(cell, edgezero_adapter_cloudflare::CLOUDFLARE_PLATFORM) {
        Ok(app) => edgezero_adapter_cloudflare::dispatch_app::<A>(app, req, env, ctx).await,
        Err(error) => {
            drop(env);
            edgezero_adapter_cloudflare::request::send_assembly_error(req, error, ctx, start, clock)
        }
    }
}

#[event(fetch)]
async fn fetch(req: Request, env: Env, ctx: Context) -> worker::Result<Response> {
    #[cfg(feature = "progress")]
    if req.path().starts_with("/progress/") {
        return progress::dispatch(req, env, ctx).await;
    }
    if !FixtureApp::owns_logging() {
        drop(edgezero_adapter_cloudflare::init_logger());
    } else {
        fixture_core::init_fixture_logging(telemetry)
            .map_err(|_error| worker::Error::RustError("fixture logging failed".to_owned()))?;
    }
    let retained = env
        .var("FIXTURE_MODE")
        .map(|v| v.to_string() == "retained")
        .unwrap_or(false);
    match req.path().as_str() {
        "/assembly/configuration" => {
            return edgezero_adapter_cloudflare::run_app::<ConfigurationFailureApp>(req, env, ctx)
                .await;
        }
        "/assembly/internal" => {
            return edgezero_adapter_cloudflare::run_app::<InternalFailureApp>(req, env, ctx).await;
        }
        "/assembly/retry" => {
            return dispatch_selected::<RetryApp<false>>(req, env, ctx, retained, &ASSEMBLY_APP)
                .await;
        }
        _ => {}
    }
    if retained && req.path() == "/binding-failure" {
        let clock = MonotonicClock::default();
        let start = clock.now();
        let app = match retained_app::<FixtureApp>(
            &APP,
            edgezero_adapter_cloudflare::CLOUDFLARE_PLATFORM,
        ) {
            Ok(app) => app,
            Err(error) => {
                return edgezero_adapter_cloudflare::request::send_assembly_error(
                    req, error, ctx, start, clock,
                );
            }
        };
        let result = edgezero_adapter_cloudflare::request::CloudflareService::new(app)
            .with_kv("missing_fixture_kv")
            .require_kv()
            .dispatch(req, env, ctx)
            .await;
        return match result {
            Err(error) => Response::from_json(&serde_json::json!({"instance":fixture_core::instance_id(),"source":"injected_required_binding","error":error.to_string()})).map(|r|r.with_status(503)),
            Ok(response) => Ok(response),
        };
    }
    if req.path() == "/other" {
        return dispatch_selected::<OtherApp>(req, env, ctx, retained, &OTHER_APP).await;
    }
    dispatch_selected::<FixtureApp>(req, env, ctx, retained, &APP).await
}
