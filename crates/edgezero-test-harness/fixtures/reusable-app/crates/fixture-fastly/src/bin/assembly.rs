use std::time::Duration;

use edgezero_adapter_fastly::lifecycle::{Sandbox, serve_custom};
use edgezero_adapter_fastly::request::{
    send_assembly_error, send_request_with_registries_and_hooks,
};
use edgezero_adapter_fastly::{FASTLY_PLATFORM, FastlyLogging, Serve};
use edgezero_core::MonotonicClock;
use edgezero_core::app::{App, Hooks};
use fixture_core::assembly::{ConfigurationFailureApp, InternalFailureApp, RetryApp};

fn main() -> Result<(), fastly::Error> {
    fixture_fastly::initialize_launch(env!("CARGO_BIN_NAME"));
    fastly::init();
    fixture_core::init_fixture_logging(|line| println!("{line}"))?;
    let case = fastly::ConfigStore::open("assembly")
        .get("case")
        .ok_or_else(|| fastly::Error::msg("missing assembly case"))?;
    let serve = Serve::new()
        .with_max_requests(4)
        .with_timeout(Duration::from_secs(5));
    match case.as_str() {
        "standard-retained" => {
            fixture_fastly::finish(edgezero_adapter_fastly::serve_app::<RetryApp<false>>(serve))
        }
        "custom-retained" => {
            fixture_fastly::finish(serve_custom(serve, |req, retained: &mut Sandbox<App>| {
                let token = req
                    .get_header_str("x-request-token")
                    .filter(|value| {
                        !value.is_empty()
                            && value.len() <= 64
                            && value
                                .bytes()
                                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                    })
                    .map(str::to_owned);
                let clock = MonotonicClock::default();
                let start = clock.now();
                let delivered =
                    match retained.initialize(|| App::build::<RetryApp<true>>(FASTLY_PLATFORM)) {
                        Ok(()) => send_request_with_registries_and_hooks(
                            retained
                                .state()
                                .ok_or_else(|| fastly::Error::msg("app was not retained"))?,
                            RetryApp::<true>::stores(),
                            req,
                            |_, _| {},
                            |_| (),
                        ),
                        Err(error) => send_assembly_error(req, error, start, clock),
                    };
                if let Some(delivery) = delivered.delivery() {
                    println!(
                        "{}",
                        serde_json::json!({"event":"assembly-delivery","token":token,
                    "attempts":retained.initialization_attempts(),
                    "retained":retained.state().is_some(),
                    "disposition":if matches!(delivered, edgezero_adapter_fastly::delivery::FastlyRequestDelivery::Detached(_)) { "detached" } else { "routed" },
                    "commitment":format!("{:?}",delivery.commitment()),
                    "outcome":format!("{:?}",delivery.report().outcome),
                    "fallback":delivery.report().fallback_disposition.is_some(),
                    "may_send_native":delivered.may_send_native_response()})
                    );
                }
                fixture_fastly::strict_delivery(delivered)
            }))
        }
        "single-routes" => edgezero_adapter_fastly::run_app::<RetryApp<false>>(),
        "single-config" => edgezero_adapter_fastly::run_app::<ConfigurationFailureApp>(),
        "single-internal" => edgezero_adapter_fastly::run_app::<InternalFailureApp>(),
        "single-hooks" => {
            edgezero_adapter_fastly::run_app_with_hooks::<ConfigurationFailureApp, _, _, ()>(
                |_, _| panic!("prepare must not run after failed assembly"),
                |_| panic!("finalize must not run after failed assembly"),
            )
            .map(|_| ())
        }
        "single-config-explicit" => {
            edgezero_adapter_fastly::run_app_with_config::<ConfigurationFailureApp>(
                &FastlyLogging::from(ConfigurationFailureApp::logging_for("fastly")),
                None,
            )
        }
        _ => Err(fastly::Error::msg("unknown assembly case")),
    }
}
