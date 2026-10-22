//! Executable counterpart of the routing guide's manual `Hooks` workaround.

#[cfg(test)]
mod tests {
    mod manifest_app {
        edgezero_core::app!(
            "tests/fixtures/hooked_app.toml",
            ManifestApp,
            owns_logging = true,
            state = 42_u32
        );
    }

    use async_trait::async_trait;
    use edgezero_core::action;
    use edgezero_core::app::{App, Hooks, StoresMetadata};
    use edgezero_core::body::Body;
    use edgezero_core::context::RequestContext;
    use edgezero_core::error::EdgeError;
    use edgezero_core::extractor::State;
    use edgezero_core::http::{Request, Response, request_builder, response_builder};
    use edgezero_core::introspection::ManifestJson;
    use edgezero_core::manifest::{ManifestLoader, ResolvedLoggingConfig};
    use edgezero_core::middleware::{Middleware, Next};
    use edgezero_core::router::{PreDispatchHook, RouterService};
    use futures::executor::block_on;

    use manifest_app::ManifestApp;

    struct HookedApp;
    impl Hooks for HookedApp {
        fn build_app() -> App {
            let mut app = App::with_name(Self::routes(), Self::name());
            Self::configure(&mut app);
            app
        }
        fn configure(app: &mut App) {
            ManifestApp::configure(app);
        }
        fn logging_for(adapter: &str) -> ResolvedLoggingConfig {
            ManifestApp::logging_for(adapter)
        }
        fn name() -> &'static str {
            ManifestApp::name()
        }
        fn owns_logging() -> bool {
            ManifestApp::owns_logging()
        }
        fn routes() -> RouterService {
            let manifest = ManifestLoader::load_from_str(include_str!("fixtures/hooked_app.toml"));
            RouterService::builder()
                .pre_dispatch_hook(ReservedGate)
                .with_manifest_json(
                    serde_json::to_string(manifest.manifest()).expect("valid manifest"),
                )
                .with_state(42_u32)
                .middleware(MarkMiddleware)
                .get("/ordinary", ordinary)
                .build()
        }
        fn stores() -> StoresMetadata {
            ManifestApp::stores()
        }
    }

    struct ReservedGate;
    #[async_trait(?Send)]
    impl PreDispatchHook for ReservedGate {
        async fn handle(&self, request: &mut Request) -> Result<Option<Response>, EdgeError> {
            assert!(
                request.extensions().get::<u32>().is_none(),
                "hook precedes state"
            );
            assert!(
                request.extensions().get::<ManifestJson>().is_none(),
                "hook precedes introspection"
            );
            if request.uri().path() == "/reserved" {
                return response_builder()
                    .status(204)
                    .body(Body::empty())
                    .map(Some)
                    .map_err(EdgeError::internal);
            }
            Ok(None)
        }
    }

    pub struct MarkMiddleware;
    #[async_trait(?Send)]
    impl Middleware for MarkMiddleware {
        #[inline]
        async fn handle(&self, ctx: RequestContext, next: Next<'_>) -> Result<Response, EdgeError> {
            let mut response = next.run(ctx).await?;
            response
                .headers_mut()
                .insert("x-middleware", "present".parse().expect("header"));
            Ok(response)
        }
    }

    #[action(manifest)]
    async fn ordinary(State(value): State<u32>, ManifestJson(json): ManifestJson) -> &'static str {
        assert_eq!(value, 42);
        assert!(json.contains("hooked-manifest-app"), "manifest retained");
        "continued"
    }

    #[test]
    fn manual_hook_wrapper_preserves_every_manifest_metadata_surface() {
        assert_eq!(HookedApp::stores(), ManifestApp::stores());
        assert_eq!(HookedApp::stores().kv.expect("kv").ids, &["sessions"]);
        assert_eq!(
            HookedApp::stores().config.expect("config").ids,
            &["settings"]
        );
        assert_eq!(
            HookedApp::stores().secrets.expect("secrets").ids,
            &["tokens"]
        );
        assert_eq!(HookedApp::name(), "hooked-manifest-app");
        assert!(HookedApp::owns_logging());
        let actual = HookedApp::logging_for("fastly");
        let expected = ManifestApp::logging_for("fastly");
        assert_eq!(actual.endpoint, expected.endpoint);
        assert_eq!(actual.level, expected.level);
        assert_eq!(actual.echo_stdout, expected.echo_stdout);
    }

    #[test]
    fn manual_router_preserves_continuation_and_terminal_hook_boundaries() {
        block_on(async {
            let router = HookedApp::routes();
            let continued = router
                .oneshot(
                    request_builder()
                        .uri("/ordinary")
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(continued.status(), 200);
            assert_eq!(continued.headers()["x-middleware"], "present");
            let terminal = router
                .oneshot(
                    request_builder()
                        .method("EXAMPLE-METHOD")
                        .uri("/reserved")
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(terminal.status(), 204);
            assert!(terminal.headers().get("x-middleware").is_none());
        });
    }
}
