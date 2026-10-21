#![cfg(target_arch = "wasm32")]

#[cfg(feature = "progress")]
mod progress;

use edgezero_core::app::{App, Hooks};
#[cfg(not(feature = "qualification"))]
use fixture_core::DeclaredKvApp as FixtureApp;
#[cfg(feature = "qualification")]
use fixture_core::qualification::QualificationApp as FixtureApp;
use fixture_core::{OtherApp, retained_app};
use std::sync::OnceLock;
use worker::{Context, Env, Request, Response, event};
static APP: OnceLock<App> = OnceLock::new();
static OTHER_APP: OnceLock<App> = OnceLock::new();

fn telemetry(line: &str) {
    worker::console_log!("{line}");
}

fn retained_app_for_dispatch<A: Hooks>(cell: &OnceLock<App>) -> worker::Result<&App> {
    retained_app::<A>(cell, edgezero_adapter_cloudflare::CLOUDFLARE_PLATFORM)
        .map_err(|_error| worker::Error::RustError("application configuration failed".to_owned()))
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
    if retained && req.path() == "/binding-failure" {
        let app = retained_app_for_dispatch::<FixtureApp>(&APP)?;
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
        if !retained {
            return edgezero_adapter_cloudflare::run_app::<OtherApp>(req, env, ctx).await;
        }
        return edgezero_adapter_cloudflare::dispatch_app::<OtherApp>(
            retained_app_for_dispatch::<OtherApp>(&OTHER_APP)?,
            req,
            env,
            ctx,
        )
        .await;
    }
    if retained {
        edgezero_adapter_cloudflare::dispatch_app::<FixtureApp>(
            retained_app_for_dispatch::<FixtureApp>(&APP)?,
            req,
            env,
            ctx,
        )
        .await
    } else {
        edgezero_adapter_cloudflare::run_app::<FixtureApp>(req, env, ctx).await
    }
}
