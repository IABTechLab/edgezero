//! Executable counterpart of the routing guide's manual `Hooks` workaround.

#[cfg(test)]
mod tests {
    mod manifest_app {
        edgezero_core::app!(
            "tests/fixtures/hooked_app.toml",
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
    use edgezero_core::manifest::{BakedManifest, LogLevel, ManifestLoader, ResolvedLoggingConfig};
    use edgezero_core::middleware::{Middleware, Next};
    use edgezero_core::router::{PreDispatchHook, RouterService};
    use futures::executor::block_on;
    use std::sync::{Arc, LazyLock};

    use manifest_app::App as ManifestApp;

    static MANIFEST_JSON: LazyLock<Arc<str>> = LazyLock::new(|| {
        let manifest = ManifestLoader::load_from_str(include_str!("fixtures/hooked_app.toml"));
        serde_json::to_string(manifest.manifest())
            .expect("valid manifest")
            .into()
    });

    struct HookedApp;
    impl Hooks for HookedApp {
        fn configure(app: &mut App) -> Result<(), EdgeError> {
            ManifestApp::configure(app)
        }
        fn logging_for(adapter: &str) -> ResolvedLoggingConfig {
            ManifestApp::logging_for(adapter)
        }
        fn manifest() -> BakedManifest {
            ManifestApp::manifest()
        }
        fn manifest_json() -> Option<&'static str> {
            ManifestApp::manifest_json()
        }
        fn name() -> &'static str {
            ManifestApp::name()
        }
        fn owns_logging() -> bool {
            ManifestApp::owns_logging()
        }
        fn routes() -> Result<RouterService, EdgeError> {
            Ok(RouterService::builder()
                .pre_dispatch_hook(ReservedGate)
                .with_manifest_json(Arc::clone(&MANIFEST_JSON))
                .with_state(42_u32)
                .middleware(MarkMiddleware)
                .get("/ordinary", ordinary)
                .build())
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
            if request.uri().path() == "/reserved"
                || request.headers().contains_key("x-example-stop")
            {
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
    async fn ordinary(State(value): State<u32>, ManifestJson(json): ManifestJson) -> String {
        assert_eq!(value, 42);
        json.to_string()
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
        assert_eq!(HookedApp::manifest_json(), ManifestApp::manifest_json());
        assert!(HookedApp::owns_logging());
        let actual = HookedApp::logging_for("fastly");
        assert_eq!(actual.endpoint.as_deref(), Some("fixture_logs"));
        assert_eq!(actual.level, LogLevel::Debug);
        let expected = ManifestApp::logging_for("fastly");
        assert_eq!(actual.endpoint, expected.endpoint);
        assert_eq!(actual.level, expected.level);
        assert_eq!(actual.echo_stdout, expected.echo_stdout);
    }

    #[test]
    fn manual_router_preserves_continuation_and_terminal_hook_boundaries() {
        block_on(async {
            let app = App::build::<HookedApp>(edgezero_core::PlatformMetadata::default())
                .expect("build hooked app");
            assert_eq!(app.name(), "hooked-manifest-app");
            let router = app.router();
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
            let generated = App::build::<ManifestApp>(edgezero_core::PlatformMetadata::default())
                .expect("build manifest app")
                .into_router()
                .oneshot(
                    request_builder()
                        .uri("/ordinary")
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("macro response");
            assert_eq!(generated.status(), 200);
            assert_eq!(generated.headers()["x-middleware"], "present");
            assert_eq!(
                continued.body().as_bytes().expect("buffered manifest"),
                generated.body().as_bytes().expect("macro manifest"),
                "manifest retained byte for byte"
            );
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
            let matched_terminal = router
                .oneshot(
                    request_builder()
                        .uri("/ordinary")
                        .header("x-example-stop", "true")
                        .body(Body::empty())
                        .expect("matched request"),
                )
                .await
                .expect("matched_terminal response on matched route");
            assert_eq!(matched_terminal.status(), 204);
            assert!(matched_terminal.headers().get("x-middleware").is_none());
        });
    }
}
