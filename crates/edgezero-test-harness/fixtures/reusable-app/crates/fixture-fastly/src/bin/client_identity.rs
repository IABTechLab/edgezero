//! Client identity contract fixture.
//!
//! Sends outbound requests that select a declared TLS client identity through the
//! managed `run_app` path, so core attaches the identity policy at dispatch and
//! the Fastly adapter resolves the runtime binding from Viceroy's local stores.
//! The target origin comes from the `x-fixture-backend` header. Every case answers 200
//! with a JSON report; the harness checks origin-side records separately.
//!
//! The manifest is loaded into `Hooks::manifest()` directly rather than through
//! `app!`, because this fixture workspace has no `edgezero.toml` app layout for
//! the macro to read. The policy path from `Hooks::manifest()` onward is the same.

use std::sync::LazyLock;

use edgezero_core::OutboundImageOptimizerOptions;
use edgezero_core::app::Hooks;
use edgezero_core::context::RequestContext;
use edgezero_core::error::EdgeError;
use edgezero_core::manifest::{BakedManifest, ManifestLoader};
use edgezero_core::outbound::{
    ClientIdentityName, HttpClient, OutboundFollowFailure, OutboundRedirectAction,
    OutboundRedirectContext, OutboundRedirectLimit, OutboundRequest,
};
use edgezero_core::router::RouterService;
use serde_json::{Value, json};

const MANIFEST: &str = r#"
[capabilities.outbound.client-identities.fixture-mtls]
config-store = "fixture_mtls_config"
secret-store = "fixture_mtls_secrets"

[stores.config]
ids = ["fixture_mtls_config"]

[stores.secrets]
ids = ["fixture_mtls_secrets"]
"#;

static LOADED: LazyLock<ManifestLoader> = LazyLock::new(|| ManifestLoader::load_from_str(MANIFEST));

struct ClientIdentityApp;

impl Hooks for ClientIdentityApp {
    fn manifest() -> BakedManifest {
        BakedManifest::Present(LOADED.manifest())
    }

    fn routes() -> Result<RouterService, EdgeError> {
        Ok(RouterService::builder()
            .middleware(fixture_core::launch::Identity)
            .get("/identity/{case}", identity_case)
            .build())
    }
}

fn follow_all(_context: &mut OutboundRedirectContext<'_>) -> Result<OutboundRedirectAction, ()> {
    Ok(OutboundRedirectAction::FollowPrepared)
}

fn failure_report(case: &str, error: &EdgeError, hop: Option<usize>) -> Value {
    json!({
        "case": case,
        "outcome": "error",
        "status": error.status().as_u16(),
        "kind": error.kind(),
        "reason": error.client_identity_error().map(|reason| format!("{reason:?}")),
        "provider_cause": error.provider_failure_detail().map(|detail| format!("{detail:?}")),
        "hop": hop,
    })
}

async fn send(case: &str, client: &HttpClient, request: OutboundRequest) -> Value {
    match client.send(request).await {
        Ok(response) => json!({
            "case": case,
            "outcome": "response",
            "status": response.status().as_u16(),
        }),
        Err(error) => failure_report(case, &error, None),
    }
}

async fn identity_case(ctx: RequestContext) -> Result<String, EdgeError> {
    let case = ctx
        .path_params()
        .get("case")
        .ok_or_else(|| EdgeError::bad_request("missing case"))?
        .to_owned();
    let target = ctx
        .headers()
        .get("x-fixture-backend")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| EdgeError::bad_request("missing x-fixture-backend"))?
        .to_owned();
    let client = ctx
        .http_client()
        .ok_or_else(|| EdgeError::bad_request("missing outbound client"))?;
    let mtls = ClientIdentityName::new("fixture-mtls")?;
    let request = OutboundRequest::get(target.as_str())?;
    let report = match case.as_str() {
        "with" => send(&case, &client, request.client_identity(mtls)).await,
        "without" => send(&case, &client, request).await,
        "undeclared" => {
            let undeclared = ClientIdentityName::new("undeclared-mtls")?;
            send(&case, &client, request.client_identity(undeclared)).await
        }
        "optimizer" => {
            let optimized = request
                .client_identity(mtls)
                .image_optimizer(OutboundImageOptimizerOptions::new());
            send(&case, &client, optimized).await
        }
        "follow" => {
            let limit = OutboundRedirectLimit::new(3)?;
            match client
                .follow(request.client_identity(mtls), limit, follow_all)
                .await
            {
                Ok(followed) => {
                    let (response, final_uri, redirects, _stop) = followed.into_parts();
                    json!({
                        "case": case,
                        "outcome": "response",
                        "status": response.status().as_u16(),
                        "final_uri": final_uri.to_string(),
                        "redirects": redirects,
                    })
                }
                Err(OutboundFollowFailure::Hop { index, error }) => {
                    failure_report(&case, &error, Some(index))
                }
                Err(OutboundFollowFailure::Policy { .. }) => {
                    return Err(EdgeError::bad_request("follow policy never rejects"));
                }
            }
        }
        _ => return Err(EdgeError::bad_request("unknown case")),
    };
    Ok(report.to_string())
}

fn main() -> Result<(), fastly::Error> {
    fixture_fastly::initialize_launch(env!("CARGO_BIN_NAME"));
    edgezero_adapter_fastly::run_app::<ClientIdentityApp>()
}
