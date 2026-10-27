use edgezero_core::{
    OutboundCropFocus, OutboundImageFormat, OutboundImageOptimizerOptions,
    OutboundImageOptimizerRegion, OutboundRequest, OutboundResizeFilter,
};

#[test]
#[expect(
    clippy::tests_outside_test_module,
    reason = "external supplied-client API integration test"
)]
fn image_optimizer_options_round_trip() {
    let options = OutboundImageOptimizerOptions::new()
        .region(OutboundImageOptimizerRegion::UsWest)
        .preserve_origin_query(true)
        .format(OutboundImageFormat::WebP)
        .quality(80)
        .expect("quality")
        .resize_filter(OutboundResizeFilter::Bicubic)
        .width(100)
        .expect("width")
        .height(50)
        .expect("height")
        .crop(2, 1, OutboundCropFocus::Percent { x: 25, y: 75 })
        .expect("crop");
    let request = OutboundRequest::get("https://example.com/image%20one.jpg?signed=one%2Ftwo")
        .expect("target")
        .header("authorization", "fixture-signature")
        .expect("signature")
        .image_optimizer(options.clone());
    assert_eq!(request.image_optimizer_options(), Some(&options));
    let target = request.uri().clone();
    let parts = request.into_parts();
    assert_eq!(parts.image_optimizer, Some(options.clone()));
    let rebuilt = OutboundRequest::from_parts(parts).expect("parts");
    assert_eq!(rebuilt.image_optimizer_options(), Some(&options));
    assert_eq!(rebuilt.uri(), &target);
    assert_eq!(rebuilt.headers()["authorization"], "fixture-signature");
    let replacement =
        OutboundImageOptimizerOptions::new().region(OutboundImageOptimizerRegion::Asia);
    let replaced = rebuilt.image_optimizer(replacement.clone());
    assert_eq!(replaced.image_optimizer_options(), Some(&replacement));
    let mut cleared_parts = replaced.into_parts();
    cleared_parts.image_optimizer = None;
    let cleared = OutboundRequest::from_parts(cleared_parts).expect("cleared parts");
    assert!(cleared.image_optimizer_options().is_none());
}

#[test]
#[expect(
    clippy::tests_outside_test_module,
    reason = "external supplied-client API integration test"
)]
fn public_image_optimizer_value_readbacks_compile_contract() {
    let region = OutboundImageOptimizerRegion::UsWest;
    let options = OutboundImageOptimizerOptions::new()
        .region(region)
        .preserve_origin_query(true)
        .format(OutboundImageFormat::WebP)
        .quality(80)
        .expect("quality")
        .resize_filter(OutboundResizeFilter::Bicubic)
        .width(100)
        .expect("width")
        .height(50)
        .expect("height")
        .crop(2, 1, OutboundCropFocus::Percent { x: 25, y: 75 })
        .expect("crop");
    assert_eq!(options.region_setting(), Some(&region));
    assert_eq!(region.as_str(), "us_west");
    assert!(options.preserve_origin_query_setting());
    assert_eq!(options.format_setting(), Some(OutboundImageFormat::WebP));
    assert_eq!(options.quality_setting(), Some(80));
    assert_eq!(
        options.resize_filter_setting(),
        Some(OutboundResizeFilter::Bicubic)
    );
    assert_eq!(options.width_setting(), Some(100));
    assert_eq!(options.height_setting(), Some(50));
    assert_eq!(
        options.crop_setting(),
        Some((2, 1, OutboundCropFocus::Percent { x: 25, y: 75 }))
    );
    assert_eq!(options.clone(), options);
    assert_eq!(
        OutboundImageOptimizerOptions::new(),
        OutboundImageOptimizerOptions::default()
    );
}

#[test]
#[expect(
    clippy::tests_outside_test_module,
    reason = "external supplied-client API integration test"
)]
fn public_image_optimizer_regions_support_exhaustive_matching() {
    for region in [
        OutboundImageOptimizerRegion::Asia,
        OutboundImageOptimizerRegion::Australia,
        OutboundImageOptimizerRegion::EuCentral,
        OutboundImageOptimizerRegion::EuWest,
        OutboundImageOptimizerRegion::UsCentral,
        OutboundImageOptimizerRegion::UsEast,
        OutboundImageOptimizerRegion::UsWest,
    ] {
        let token = match region {
            OutboundImageOptimizerRegion::Asia => "asia",
            OutboundImageOptimizerRegion::Australia => "australia",
            OutboundImageOptimizerRegion::EuCentral => "eu_central",
            OutboundImageOptimizerRegion::EuWest => "eu_west",
            OutboundImageOptimizerRegion::UsCentral => "us_central",
            OutboundImageOptimizerRegion::UsEast => "us_east",
            OutboundImageOptimizerRegion::UsWest => "us_west",
        };
        assert_eq!(region.as_str(), token);
        assert_eq!(
            OutboundImageOptimizerRegion::parse(token).expect("region"),
            region
        );
    }
}

#[test]
#[expect(
    clippy::tests_outside_test_module,
    reason = "external implementer validation API contract"
)]
fn public_dispatch_validation_modes_compile_contract() {
    use edgezero_core::{Body, EdgeError, OutboundSendMode, validate_for_dispatch};
    use futures_util::stream;

    let validate: fn(&OutboundRequest, OutboundSendMode) -> Result<(), EdgeError> =
        validate_for_dispatch;
    let request = OutboundRequest::post("https://example.com/")
        .expect("request")
        .stream_response();
    for mode in [
        OutboundSendMode::Send,
        OutboundSendMode::Begin,
        OutboundSendMode::Follow,
    ] {
        validate(&request, mode).expect("single exchange shape");
    }
    assert!(matches!(
        validate(&request, OutboundSendMode::Batch),
        Err(EdgeError::BadRequest { .. })
    ));
    let streamed_upload = OutboundRequest::post("https://example.com/")
        .expect("request")
        .body(Body::from_stream(stream::pending()));
    validate(&streamed_upload, OutboundSendMode::Send).expect("send accepts streamed upload");
    validate(&streamed_upload, OutboundSendMode::Follow)
        .expect("follow accepts non-replayable upload");
    assert!(matches!(
        validate(&streamed_upload, OutboundSendMode::Begin),
        Err(EdgeError::BadRequest { .. })
    ));
}

#[test]
#[expect(
    clippy::tests_outside_test_module,
    reason = "external head-first API contract"
)]
fn owned_head_first_facade_compile_contract() {
    use edgezero_core::{HttpClient, OutboundDispatchRequest, OutboundFuture, OutboundPending};
    use std::time::Duration;

    fn owned_send(
        client: &HttpClient,
        request: OutboundRequest,
    ) -> OutboundFuture<edgezero_core::OutboundResponse> {
        client.send(request)
    }
    fn owned_begin(
        client: &HttpClient,
        request: OutboundRequest,
    ) -> OutboundFuture<OutboundPending> {
        client.begin(request)
    }
    fn inspect(request: &OutboundDispatchRequest) {
        assert!(request.entered_at() <= request.budget().deadline.instant());
        assert!(request.request().uri().scheme().is_some());
        assert!(request.clock().now() <= request.time().clock().now());
    }
    let _ = (owned_send, owned_begin, inspect);
    let request = OutboundRequest::get("https://example.com/")
        .expect("request")
        .connect_timeout(Duration::from_millis(10));
    assert_eq!(
        request.connect_timeout_setting(),
        Some(Duration::from_millis(10))
    );
}
