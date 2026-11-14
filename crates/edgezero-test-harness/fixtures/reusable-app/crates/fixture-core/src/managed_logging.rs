//! Local managed/owned logging controls, independent of the fixture's normal logger.

use std::sync::OnceLock;

use edgezero_core::app::{App, Hooks};
use edgezero_core::context::RequestContext;
use edgezero_core::http::{Response, response_builder};
use edgezero_core::router::RouterService;
use edgezero_core::{Body, EdgeError, action};
use serde_json::json;

static INSTANCE: OnceLock<String> = OnceLock::new();

pub struct LoggingApp<const OWNS: bool, const FAIL: bool>;

pub fn instance() -> &'static str {
    INSTANCE.get_or_init(|| {
        format!(
            "logging-{:?}",
            edgezero_core::MonotonicClock::default().now()
        )
    })
}

impl<const OWNS: bool, const FAIL: bool> Hooks for LoggingApp<OWNS, FAIL> {
    fn owns_logging() -> bool {
        OWNS
    }

    fn routes() -> Result<RouterService, EdgeError> {
        Ok(RouterService::builder()
            .get("/managed-logging", probe)
            .get("/managed-logging/owned", probe)
            .get("/managed-logging/failure", probe)
            .build())
    }

    fn configure(_app: &mut App) -> Result<(), EdgeError> {
        if FAIL {
            return Err(EdgeError::service_unavailable(
                "managed-logging-startup-failure",
            ));
        }
        Ok(())
    }
}

#[action]
async fn probe(ctx: RequestContext) -> Result<Response, EdgeError> {
    let token = ctx
        .headers()
        .get("x-request-token")
        .and_then(|value| value.to_str().ok())
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
        .ok_or_else(|| EdgeError::bad_request("missing logging token"))?;
    emit_records(token);
    let mut response = response_builder()
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"instance":instance(),"token":token,"max_level":log::max_level().to_string()})
                .to_string(),
        ))
        .map_err(EdgeError::internal)?;
    crate::launch::stamp(&mut response);
    Ok(response)
}

pub fn emit_records(token: &str) {
    for level in [
        log::Level::Error,
        log::Level::Warn,
        log::Level::Info,
        log::Level::Debug,
        log::Level::Trace,
    ] {
        log::log!(target: "managed_logging_probe", level, "managed-logging-record {token} {level}");
        log::log!(target: edgezero_core::BOOT_LOG_TARGET, level, "managed-logging-boot {token} {level}");
    }
}

pub fn reinitialization_probe(
    owned: bool,
    before: log::LevelFilter,
    level: log::LevelFilter,
    initialize: fn(log::LevelFilter) -> Result<(), edgezero_core::logging::ManagedLoggerError>,
) -> serde_json::Value {
    let after = log::max_level();
    let (repeated, conflict) = if owned {
        (true, true)
    } else {
        let repeated = initialize(level).is_ok() && initialize(level).is_ok();
        let other = if level == log::LevelFilter::Trace {
            log::LevelFilter::Off
        } else {
            log::LevelFilter::Trace
        };
        (
            repeated,
            initialize(other)
                == Err(edgezero_core::logging::ManagedLoggerError::ConfigurationChanged),
        )
    };
    json!({"event":"managed-logging-owner","instance":instance(),"owned":owned,
        "before":before.to_string(),"after":after.to_string(),"unchanged":after == log::max_level(),
        "repeated":repeated,"conflict":conflict,"qualification":false})
}
