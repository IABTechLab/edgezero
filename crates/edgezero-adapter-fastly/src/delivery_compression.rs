//! Final delivery-compression headers shared by standard and custom senders.

use edgezero_core::delivery_compression::DeliveryCompressionIntent;
use edgezero_core::http::{HeaderValue, Response, header};

/// Restore the delivery contract of an explicitly marked identity response.
///
/// Standard conversion calls this automatically. Custom senders must call it
/// after their last header effects and before consuming response parts or
/// committing headers. It never polls or modifies the body. Unmarked responses
/// are unchanged, including already encoded output and manually supplied hints.
///
/// The intent asserts that the installed body produces identity bytes. A body
/// transformer must remove it before replacing or re-encoding that representation.
/// This function only corrects stale headers; it cannot decode an encoded body.
#[inline]
pub fn finalize_response(response: &mut Response) {
    if response
        .extensions()
        .get::<DeliveryCompressionIntent>()
        .is_none()
    {
        return;
    }
    response.headers_mut().remove(header::CONTENT_ENCODING);
    response.headers_mut().remove(header::CONTENT_LENGTH);
    response
        .headers_mut()
        .insert("x-compress-hint", HeaderValue::from_static("on"));
    let varies_on_encoding = response
        .headers()
        .get_all(header::VARY)
        .iter()
        .any(|value| {
            value.to_str().is_ok_and(|text| {
                text.split(',').any(|token| {
                    token.trim() == "*" || token.trim().eq_ignore_ascii_case("accept-encoding")
                })
            })
        });
    if !varies_on_encoding {
        response
            .headers_mut()
            .append(header::VARY, HeaderValue::from_static("Accept-Encoding"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use edgezero_core::body::Body;
    use edgezero_core::delivery_compression::{
        DeliveryCompressionIntent, DeliveryCompressionSupport,
    };
    use edgezero_core::http::{HeaderValue, StatusCode, header};
    use futures::executor::block_on;
    use futures_util::stream;
    use std::cell::Cell;
    use std::rc::Rc;
    use std::task::Poll;

    fn response(streaming: bool) -> Response {
        let body = if streaming {
            Body::stream(stream::iter(vec![Bytes::from_static(b"identity output")]))
        } else {
            Body::text("identity output")
        };
        let mut response = Response::new(body);
        *response.status_mut() = StatusCode::CREATED;
        for (name, value) in [
            ("content-length", "15"),
            ("cache-control", "private, no-store"),
            ("surrogate-control", "no-store"),
            ("etag", "\"original\""),
            ("set-cookie", "a=1"),
            ("set-cookie", "b=2"),
            ("vary", "Origin"),
            ("x-other", "preserve"),
        ] {
            response
                .headers_mut()
                .append(name, HeaderValue::from_static(value));
        }
        response.extensions_mut().insert(42_u32);
        response
    }

    #[test]
    fn unmarked_identity_encoded_and_manual_hints_remain_unchanged() {
        for streaming in [false, true] {
            for maybe_encoding in [
                None,
                Some("gzip"),
                Some("br"),
                Some("deflate"),
                Some("zstd"),
            ] {
                let mut response = response(streaming);
                if let Some(encoding) = maybe_encoding {
                    response
                        .headers_mut()
                        .insert(header::CONTENT_ENCODING, HeaderValue::from_static(encoding));
                }
                response
                    .headers_mut()
                    .insert("x-compress-hint", HeaderValue::from_static("off"));
                let headers = response.headers().clone();
                finalize_response(&mut response);
                assert_eq!(response.headers(), &headers);
                assert_eq!(response.status(), StatusCode::CREATED);
                assert_eq!(response.extensions().get::<u32>(), Some(&42));
                assert_eq!(
                    block_on(response.into_body().into_bytes_bounded(1024)).unwrap(),
                    "identity output"
                );
            }
        }
    }

    #[test]
    fn marked_once_and_stream_restore_late_headers_without_changing_output_or_privacy() {
        for streaming in [false, true] {
            let mut response = response(streaming);
            DeliveryCompressionSupport::GzipBrotli
                .mark_identity_response(&mut response)
                .unwrap();
            response
                .headers_mut()
                .insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
            response
                .headers_mut()
                .insert(header::CONTENT_LENGTH, HeaderValue::from_static("9999"));
            response
                .headers_mut()
                .insert("x-compress-hint", HeaderValue::from_static("off"));
            finalize_response(&mut response);
            let headers = response.headers().clone();
            finalize_response(&mut response);
            assert_eq!(response.headers(), &headers);
            assert!(!response.headers().contains_key(header::CONTENT_ENCODING));
            assert!(!response.headers().contains_key(header::CONTENT_LENGTH));
            assert_eq!(response.headers()["x-compress-hint"], "on");
            assert_eq!(
                response.headers().get_all("x-compress-hint").iter().count(),
                1
            );
            assert_eq!(response.status(), StatusCode::CREATED);
            assert_eq!(
                response.headers()[header::CACHE_CONTROL],
                "private, no-store"
            );
            assert_eq!(response.headers()["surrogate-control"], "no-store");
            assert_eq!(response.headers()[header::ETAG], "\"original\"");
            assert_eq!(response.headers()["x-other"], "preserve");
            assert_eq!(
                response
                    .headers()
                    .get_all(header::SET_COOKIE)
                    .iter()
                    .collect::<Vec<_>>(),
                vec!["a=1", "b=2"]
            );
            assert_eq!(response.extensions().get::<u32>(), Some(&42));
            assert!(
                response
                    .extensions()
                    .get::<DeliveryCompressionIntent>()
                    .is_some()
            );
            assert_eq!(
                block_on(response.into_body().into_bytes_bounded(1024)).unwrap(),
                "identity output"
            );
        }
    }

    #[test]
    fn vary_preserves_fields_and_avoids_duplicate_or_wildcard_variation() {
        for values in [
            vec![],
            vec!["Origin"],
            vec!["Origin", "Accept-Language"],
            vec!["Origin, AcCePt-EnCoDiNg"],
            vec!["Origin", " accept-encoding \t"],
            vec!["*"],
            vec!["Origin", "Accept-Language, *"],
        ] {
            let mut response = Response::new(Body::empty());
            for value in &values {
                response
                    .headers_mut()
                    .append(header::VARY, HeaderValue::from_static(value));
            }
            DeliveryCompressionSupport::GzipBrotli
                .mark_identity_response(&mut response)
                .unwrap();
            finalize_response(&mut response);
            let mut expected = values.clone();
            if !values.iter().any(|value| {
                value.split(',').any(|token| {
                    token.trim() == "*" || token.trim().eq_ignore_ascii_case("accept-encoding")
                })
            }) {
                expected.push("Accept-Encoding");
            }
            let actual = response
                .headers()
                .get_all(header::VARY)
                .iter()
                .map(|value| value.to_str().unwrap())
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn opaque_vary_value_is_preserved_alongside_required_variation() {
        let mut response = Response::new(Body::empty());
        let opaque = HeaderValue::from_bytes(&[0xff]).unwrap();
        response.headers_mut().append(header::VARY, opaque.clone());
        DeliveryCompressionSupport::GzipBrotli
            .mark_identity_response(&mut response)
            .unwrap();
        finalize_response(&mut response);
        assert_eq!(
            response
                .headers()
                .get_all(header::VARY)
                .iter()
                .collect::<Vec<_>>(),
            vec![&opaque, &HeaderValue::from_static("Accept-Encoding")]
        );
    }

    #[test]
    fn finalization_does_not_poll_or_replace_a_lazy_stream() {
        let polls = Rc::new(Cell::new(0_usize));
        let observed = Rc::clone(&polls);
        let mut chunk = Some(Bytes::from_static(b"prefix"));
        let body = Body::stream(stream::poll_fn(move |_| {
            observed.set(observed.get() + 1);
            Poll::Ready(chunk.take())
        }));
        let mut response = Response::new(body);
        DeliveryCompressionSupport::GzipBrotli
            .mark_identity_response(&mut response)
            .unwrap();
        finalize_response(&mut response);
        finalize_response(&mut response);
        assert_eq!(polls.get(), 0);
        assert!(response.body().is_stream());
        assert_eq!(
            block_on(response.into_body().into_bytes_bounded(1024)).unwrap(),
            "prefix"
        );
        assert_eq!(polls.get(), 2);
    }

    #[test]
    fn intent_removed_before_reencoding_preserves_encoded_representation() {
        let mut response = Response::new(Body::text("identity output"));
        DeliveryCompressionSupport::GzipBrotli
            .mark_identity_response(&mut response)
            .unwrap();
        response
            .extensions_mut()
            .remove::<DeliveryCompressionIntent>();
        let encoded = vec![0x1f, 0x8b, 0x08, 0x00];
        *response.body_mut() = Body::from(encoded.clone());
        response
            .headers_mut()
            .insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        response
            .headers_mut()
            .insert(header::CONTENT_LENGTH, HeaderValue::from_static("4"));
        let headers = response.headers().clone();
        finalize_response(&mut response);
        assert_eq!(response.headers(), &headers);
        assert_eq!(response.body().as_bytes(), Some(encoded.as_slice()));
    }
}
