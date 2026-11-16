use edgezero_core::app::Hooks;
use edgezero_core::manifest::{LogLevel, ResolvedLoggingConfig};
use edgezero_core::router::RouterService;
use edgezero_core::{EdgeError, action};

struct ManualApp;

impl Hooks for ManualApp {
    fn logging_for(adapter: &str) -> ResolvedLoggingConfig {
        edgezero_core::logging_config!("tests/fixtures/logging_config_manual_routes.toml", adapter)
    }

    fn routes() -> Result<RouterService, EdgeError> {
        Ok(RouterService::builder().get("/manual", manual).build())
    }
}

#[action]
async fn manual() -> &'static str {
    "manual"
}

fn main() {
    let logging = ManualApp::logging_for("FASTLY");
    assert_eq!(logging.level, LogLevel::Debug);
    assert_eq!(logging.endpoint.as_deref(), Some("manual_logs"));
    assert_eq!(ManualApp::routes().unwrap().routes().len(), 1);
}
