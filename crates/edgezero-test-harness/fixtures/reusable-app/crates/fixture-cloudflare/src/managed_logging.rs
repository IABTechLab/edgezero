use fixture_core::managed_logging::LoggingApp;
use worker::{Context, Env, Request, Response};

pub async fn dispatch(req: Request, env: Env, ctx: Context) -> worker::Result<Response> {
    let owned = req.path().ends_with("/owned");
    let failure = req.path().ends_with("/failure");
    let token = req
        .headers()
        .get("x-request-token")?
        .filter(|token| token.len() <= 64)
        .unwrap_or_default();
    if owned {
        fixture_core::init_fixture_logging(super::telemetry)
            .map_err(|_error| worker::Error::RustError("fixture logging failed".to_owned()))?;
    }
    let before = log::max_level();
    let logging = env
        .var("EDGEZERO__LOGGING__LEVEL")
        .ok()
        .map(|value| value.to_string());
    let config = edgezero_core::env_config::EnvConfig::from_vars(
        logging
            .as_deref()
            .map(|value| ("EDGEZERO__LOGGING__LEVEL", value)),
    );
    let level = edgezero_core::resolve_logging_level(&config);
    let result = if owned {
        edgezero_adapter_cloudflare::run_app::<LoggingApp<true, false>>(req, env, ctx).await
    } else if failure {
        edgezero_adapter_cloudflare::run_app::<LoggingApp<false, true>>(req, env, ctx).await
    } else {
        edgezero_adapter_cloudflare::run_app::<LoggingApp<false, false>>(req, env, ctx).await
    };
    let observation = fixture_core::managed_logging::reinitialization_probe(
        owned,
        before,
        level,
        edgezero_adapter_cloudflare::init_logger,
    );
    if !failure {
        fixture_core::managed_logging::emit_records(&format!("after-{token}"));
        super::telemetry(&observation.to_string());
    }
    result
}
