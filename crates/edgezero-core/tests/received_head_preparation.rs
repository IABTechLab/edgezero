#[cfg(test)]
mod tests {
    use edgezero_core::app::App;
    use edgezero_core::http::{Extensions, HeaderMap, HeaderValue};
    use edgezero_core::router::RouterService;
    use edgezero_core::time::{MonotonicClock, MonotonicInstant};

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct AuthenticatedAuthority(String);

    #[test]
    fn absent_preparation_callback_leaves_extensions_unchanged() {
        let app = App::new(RouterService::builder().build());
        let mut extensions = Extensions::new();
        extensions.insert(AuthenticatedAuthority("retained.example.com".to_owned()));

        app.prepare_received_head(&HeaderMap::new(), &mut extensions);

        assert_eq!(
            extensions.get::<AuthenticatedAuthority>(),
            Some(&AuthenticatedAuthority("retained.example.com".to_owned())),
            "the default should preserve existing extensions"
        );
    }

    #[test]
    fn prepared_startup_router_keeps_application_clock_and_platform() {
        let old_router = RouterService::builder().with_state(1_u32).build();
        let mut app = App::new(old_router.clone());
        let platform = app.platform();
        let instant = MonotonicInstant::now();
        app.set_monotonic_clock(MonotonicClock::new(move || instant));

        app.set_router(RouterService::builder().with_state(2_u32).build());

        assert_eq!(
            app.platform(),
            platform,
            "startup should preserve target metadata"
        );
        assert_eq!(
            app.monotonic_now(),
            instant,
            "startup should preserve the clock domain"
        );
        assert_eq!(
            app.state::<u32>(),
            Some(&2),
            "startup should install prepared router state"
        );
        assert_eq!(
            old_router.state::<u32>(),
            Some(&1),
            "startup should not mutate peer routers"
        );
    }

    #[test]
    fn received_head_preparation_retains_only_authenticated_fact_without_mutating_headers() {
        let app = App::new(
            RouterService::builder()
                .received_head_preparer(|headers, extensions| {
                    if headers.get("x-fixture-auth")
                        == Some(&HeaderValue::from_static("fictional-secret"))
                        && let Some(authority) = headers
                            .get("x-forwarded-host")
                            .and_then(|value| value.to_str().ok())
                    {
                        extensions.insert(AuthenticatedAuthority(authority.to_owned()));
                    }
                })
                .build(),
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-fixture-auth",
            HeaderValue::from_static("fictional-secret"),
        );
        headers.insert(
            "x-forwarded-host",
            HeaderValue::from_static("publisher.example.com:8443"),
        );
        let original = headers.clone();
        let mut extensions = Extensions::new();

        app.prepare_received_head(&headers, &mut extensions);

        assert_eq!(
            headers, original,
            "preparation should not mutate received headers"
        );
        assert_eq!(
            extensions.get::<AuthenticatedAuthority>(),
            Some(&AuthenticatedAuthority(
                "publisher.example.com:8443".to_owned()
            )),
            "preparation should retain the authenticated public authority"
        );
        assert_eq!(
            extensions.len(),
            1,
            "preparation should retain only the public fact"
        );

        headers.remove("x-fixture-auth");
        let mut rejected = Extensions::new();
        app.prepare_received_head(&headers, &mut rejected);
        assert!(
            rejected.is_empty(),
            "missing authentication should retain no fact"
        );
    }
}
