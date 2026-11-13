use fixture_core::managed_logging::LoggingApp;
use spin_sdk::http::Request;

pub async fn dispatch(req: Request) -> anyhow::Result<edgezero_adapter_spin::SpinResponse> {
    let owned = req.uri().path().ends_with("/owned");
    let failure = req.uri().path().ends_with("/failure");
    let token = req
        .headers()
        .get("x-request-token")
        .and_then(|value| value.to_str().ok())
        .filter(|token| token.len() <= 64)
        .unwrap_or_default()
        .to_owned();
    if owned {
        fixture_core::init_fixture_logging(super::telemetry)?;
    }
    let before = log::max_level();
    let level =
        edgezero_core::resolve_logging_level(&edgezero_core::env_config::EnvConfig::from_env());
    let result = if owned {
        edgezero_adapter_spin::run_app::<LoggingApp<true, false>>(req).await
    } else if failure {
        edgezero_adapter_spin::run_app::<LoggingApp<false, true>>(req).await
    } else {
        edgezero_adapter_spin::run_app::<LoggingApp<false, false>>(req).await
    };
    let observation = fixture_core::managed_logging::reinitialization_probe(
        owned,
        before,
        level,
        edgezero_adapter_spin::init_logger,
    );
    if !failure {
        fixture_core::managed_logging::emit_records(&format!("after-{token}"));
        super::telemetry(&observation.to_string());
    }
    result
}
