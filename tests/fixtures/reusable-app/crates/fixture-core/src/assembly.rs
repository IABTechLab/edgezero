use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use edgezero_core::app::{App, Hooks};
use edgezero_core::body::Body;
use edgezero_core::extractor::State;
use edgezero_core::http::{Response, response_builder};
use edgezero_core::router::RouterService;
use edgezero_core::{EdgeError, MonotonicClock, action};

static ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
static CONFIGURATIONS: AtomicUsize = AtomicUsize::new(0);
static INSTANCE: OnceLock<String> = OnceLock::new();

pub struct ConfigurationFailureApp;
pub struct InternalFailureApp;
pub struct RetryApp<const CONFIGURE_FAILURE: bool>;

struct ProbeState {
    generation: usize,
    visits: AtomicUsize,
}

impl Drop for ProbeState {
    fn drop(&mut self) {
        crate::emit(serde_json::json!({"event":"assembly-state-drop",
            "instance":instance(),"generation":self.generation}));
    }
}

impl Hooks for ConfigurationFailureApp {
    fn configure(app: &mut App) -> Result<(), EdgeError> {
        app.set_error_response_renderer(|_| {
            Response::new(Body::from("private-configured-renderer"))
        });
        app.set_response_egress_policy(|_, _| panic!("failed app policy must not run"));
        Err(EdgeError::config_out_of_date(
            "configuration unavailable",
            "",
        ))
    }

    fn owns_logging() -> bool {
        true
    }

    fn routes() -> Result<RouterService, EdgeError> {
        Ok(RouterService::builder().build())
    }
}

impl Hooks for InternalFailureApp {
    fn owns_logging() -> bool {
        true
    }

    fn routes() -> Result<RouterService, EdgeError> {
        Err(EdgeError::internal(std::io::Error::other(
            "assembly-private-token=secret",
        )))
    }
}

impl<const CONFIGURE_FAILURE: bool> Hooks for RetryApp<CONFIGURE_FAILURE> {
    fn configure(app: &mut App) -> Result<(), EdgeError> {
        let state = app
            .state::<Arc<ProbeState>>()
            .ok_or_else(|| EdgeError::service_unavailable("registered state missing"))?;
        CONFIGURATIONS.fetch_add(1, Ordering::SeqCst);
        if CONFIGURE_FAILURE && state.generation == 1 {
            return Err(EdgeError::service_unavailable("configuration unavailable"));
        }
        Ok(())
    }

    fn owns_logging() -> bool {
        true
    }

    fn routes() -> Result<RouterService, EdgeError> {
        let attempt = ATTEMPTS.fetch_add(1, Ordering::SeqCst) + 1;
        crate::emit(serde_json::json!({"event":"assembly-attempt",
            "instance":instance(),"attempt":attempt}));
        if !CONFIGURE_FAILURE && attempt == 1 {
            return Err(EdgeError::service_unavailable("state unavailable"));
        }
        Ok(RouterService::builder()
            .with_state(Arc::new(ProbeState {
                generation: attempt,
                visits: AtomicUsize::new(0),
            }))
            .get("/assembly/retry", probe)
            .build())
    }
}

fn instance() -> &'static str {
    INSTANCE.get_or_init(|| format!("assembly-{:?}", MonotonicClock::default().now()))
}

#[action]
async fn probe(State(state): State<Arc<ProbeState>>) -> Result<Response, EdgeError> {
    let visits = state.visits.fetch_add(1, Ordering::SeqCst) + 1;
    response_builder()
        .header("content-type", "application/json")
        .header("x-assembly-configured", "true")
        .body(Body::from(
            serde_json::json!({"instance":instance(),
            "attempts":ATTEMPTS.load(Ordering::SeqCst), "generation":state.generation,
            "configurations":CONFIGURATIONS.load(Ordering::SeqCst),"visits":visits})
            .to_string(),
        ))
        .map_err(EdgeError::internal)
}

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;

    use edgezero_core::PlatformMetadata;
    use edgezero_core::app::App;
    use edgezero_core::http::StatusCode;

    #[test]
    fn assembly_fixture_retries_failure_and_retains_only_configured_success() {
        let cell = OnceLock::<App>::new();
        let Err(error) =
            crate::retained_app::<super::RetryApp<false>>(&cell, PlatformMetadata::default())
        else {
            panic!("first assembly must fail");
        };
        assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(cell.get().is_none());
        let app = crate::retained_app::<super::RetryApp<false>>(&cell, PlatformMetadata::default())
            .unwrap();
        let retained =
            crate::retained_app::<super::RetryApp<false>>(&cell, PlatformMetadata::default())
                .unwrap();
        assert!(std::ptr::eq(app, retained));
        assert_eq!(super::ATTEMPTS.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(
            super::CONFIGURATIONS.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }
}
