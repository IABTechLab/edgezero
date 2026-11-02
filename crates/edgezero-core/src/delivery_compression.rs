//! Adapter delivery compression capabilities and explicit identity-output intent.

use crate::error::EdgeError;
use crate::http::{Request, Response, header::CONTENT_ENCODING};

/// Compression an adapter can apply when delivering identity output to a reader.
///
/// This does not select a codec or negotiate the reader's `Accept-Encoding`.
/// Applications must check support before choosing to skip their own encoder.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DeliveryCompressionSupport {
    /// The adapter supports reader-negotiated gzip and Brotli delivery.
    GzipBrotli,
    /// The application retains responsibility for output encoding.
    #[default]
    Unavailable,
}

/// An application assertion that the response already produces identity bytes.
///
/// Attach through [`DeliveryCompressionSupport::mark_identity_response`] only
/// after installing completed identity bytes or the final identity-producing
/// stream. This marker does not prove the body's encoding. A component that
/// later replaces or re-encodes the body representation must remove this
/// extension first. Header-only mutations retain the intent for the adapter's
/// finalization step.
#[derive(Clone, Copy, Debug)]
pub struct DeliveryCompressionIntent {
    _private: (),
}

impl DeliveryCompressionSupport {
    /// Read adapter support from request extensions, defaulting to unavailable.
    #[must_use]
    #[inline]
    pub fn from_request(request: &Request) -> Self {
        request
            .extensions()
            .get::<Self>()
            .copied()
            .unwrap_or_default()
    }

    /// Select adapter compression for an already identity-producing response.
    ///
    /// Does not decode, inspect, poll, or modify the body or headers. The caller
    /// owns eligibility, negotiation, and installing the identity output first.
    /// Query or remove the intent through ordinary response extensions.
    ///
    /// # Errors
    /// Returns [`EdgeError::Internal`] without modifying the response when
    /// support is unavailable or `Content-Encoding` is neither absent nor a
    /// single case-insensitive `identity` field with optional whitespace.
    ///
    /// # Examples
    /// ```
    /// use edgezero_core::body::Body;
    /// use edgezero_core::delivery_compression::{
    ///     DeliveryCompressionIntent, DeliveryCompressionSupport,
    /// };
    /// use edgezero_core::http::{Request, Response};
    /// # let mut request = Request::new(Body::empty());
    /// # request.extensions_mut().insert(DeliveryCompressionSupport::GzipBrotli);
    ///
    /// // Check support before choosing to skip the output encoder.
    /// let support = DeliveryCompressionSupport::from_request(&request);
    /// if support == DeliveryCompressionSupport::GzipBrotli {
    ///     let mut response = Response::new(Body::text("identity output"));
    ///     support.mark_identity_response(&mut response)?;
    ///     assert!(response.extensions().get::<DeliveryCompressionIntent>().is_some());
    ///     response.extensions_mut().remove::<DeliveryCompressionIntent>();
    /// }
    /// # Ok::<(), edgezero_core::error::EdgeError>(())
    /// ```
    #[inline]
    pub fn mark_identity_response(self, response: &mut Response) -> Result<(), EdgeError> {
        if self == Self::Unavailable {
            return Err(EdgeError::internal(anyhow::anyhow!(
                "adapter delivery compression is unavailable"
            )));
        }
        let encoding_fields = response.headers().get_all(CONTENT_ENCODING);
        let mut encodings = encoding_fields.iter();
        if let Some(encoding) = encodings.next()
            && (!encoding
                .to_str()
                .is_ok_and(|value| value.trim().eq_ignore_ascii_case("identity"))
                || encodings.next().is_some())
        {
            return Err(EdgeError::internal(anyhow::anyhow!(
                "adapter delivery compression requires identity output"
            )));
        }
        response
            .extensions_mut()
            .insert(DeliveryCompressionIntent { _private: () });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::body::Body;
    use crate::http::{HeaderValue, header::CONTENT_ENCODING};
    use bytes::Bytes;
    use futures_util::stream;
    use std::cell::Cell;
    use std::rc::Rc;
    use std::task::Poll;

    #[test]
    fn support_defaults_to_unavailable_and_reads_only_typed_extensions() {
        let mut request = Request::new(Body::empty());
        request
            .headers_mut()
            .insert("x-compress-hint", HeaderValue::from_static("on"));
        request
            .headers_mut()
            .insert("surrogate-control", HeaderValue::from_static("max-age=60"));
        assert_eq!(
            DeliveryCompressionSupport::default(),
            DeliveryCompressionSupport::Unavailable
        );
        assert_eq!(
            DeliveryCompressionSupport::from_request(&request),
            DeliveryCompressionSupport::Unavailable
        );
        request
            .extensions_mut()
            .insert(DeliveryCompressionSupport::GzipBrotli);
        assert_eq!(
            DeliveryCompressionSupport::from_request(&request),
            DeliveryCompressionSupport::GzipBrotli
        );
        request
            .extensions_mut()
            .insert(DeliveryCompressionSupport::Unavailable);
        assert_eq!(
            DeliveryCompressionSupport::from_request(&request),
            DeliveryCompressionSupport::Unavailable
        );
    }

    #[test]
    fn marking_identity_output_preserves_headers_body_and_other_extensions() {
        for maybe_encoding in [None, Some("identity"), Some(" \tIdEnTiTy \t")] {
            let mut response = Response::new(Body::text("identity output"));
            response
                .headers_mut()
                .insert("content-length", HeaderValue::from_static("15"));
            if let Some(encoding) = maybe_encoding {
                response
                    .headers_mut()
                    .insert(CONTENT_ENCODING, HeaderValue::from_str(encoding).unwrap());
            }
            response.extensions_mut().insert(42_u32);
            let headers = response.headers().clone();
            DeliveryCompressionSupport::GzipBrotli
                .mark_identity_response(&mut response)
                .unwrap();
            assert!(
                response
                    .extensions()
                    .get::<DeliveryCompressionIntent>()
                    .is_some()
            );
            assert_eq!(response.extensions().get::<u32>(), Some(&42));
            assert_eq!(response.headers(), &headers);
            assert_eq!(
                response.body().as_bytes(),
                Some(b"identity output".as_slice())
            );
        }
    }

    #[test]
    fn unavailable_marking_does_not_mutate_response() {
        let mut response = Response::new(Body::text("unchanged"));
        response
            .headers_mut()
            .insert("x-test", HeaderValue::from_static("original"));
        response.extensions_mut().insert(42_u32);
        let headers = response.headers().clone();
        assert!(
            DeliveryCompressionSupport::Unavailable
                .mark_identity_response(&mut response)
                .is_err()
        );
        assert_eq!(response.headers(), &headers);
        assert_eq!(response.body().as_bytes(), Some(b"unchanged".as_slice()));
        assert_eq!(response.extensions().get::<u32>(), Some(&42));
        assert!(
            response
                .extensions()
                .get::<DeliveryCompressionIntent>()
                .is_none()
        );
    }

    #[test]
    fn coded_stacked_repeated_and_malformed_encodings_are_rejected_unchanged() {
        let opaque = HeaderValue::from_bytes(&[0xff]).unwrap();
        for encodings in [
            vec![HeaderValue::from_static("gzip")],
            vec![HeaderValue::from_static("br")],
            vec![HeaderValue::from_static("deflate")],
            vec![HeaderValue::from_static("zstd")],
            vec![HeaderValue::from_static("identity, gzip")],
            vec![HeaderValue::from_static("identity, identity")],
            vec![HeaderValue::from_static("")],
            vec![HeaderValue::from_static(" ")],
            vec![HeaderValue::from_static("identity;q=1")],
            vec![
                HeaderValue::from_static("identity"),
                HeaderValue::from_static("gzip"),
            ],
            vec![
                HeaderValue::from_static("identity"),
                HeaderValue::from_static("identity"),
            ],
            vec![opaque],
        ] {
            let mut response = Response::new(Body::from(vec![0x1f, 0x8b, 0x08]));
            for encoding in encodings {
                response.headers_mut().append(CONTENT_ENCODING, encoding);
            }
            response.extensions_mut().insert(42_u32);
            let headers = response.headers().clone();
            assert!(
                DeliveryCompressionSupport::GzipBrotli
                    .mark_identity_response(&mut response)
                    .is_err()
            );
            assert_eq!(response.headers(), &headers);
            assert_eq!(
                response.body().as_bytes(),
                Some([0x1f, 0x8b, 0x08].as_slice())
            );
            assert_eq!(response.extensions().get::<u32>(), Some(&42));
            assert!(
                response
                    .extensions()
                    .get::<DeliveryCompressionIntent>()
                    .is_none()
            );
        }
    }

    #[test]
    fn failed_remarking_preserves_existing_intent() {
        let mut response = Response::new(Body::text("identity output"));
        DeliveryCompressionSupport::GzipBrotli
            .mark_identity_response(&mut response)
            .unwrap();
        response
            .headers_mut()
            .insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        assert!(
            DeliveryCompressionSupport::GzipBrotli
                .mark_identity_response(&mut response)
                .is_err()
        );
        assert!(
            response
                .extensions()
                .get::<DeliveryCompressionIntent>()
                .is_some()
        );
        assert_eq!(response.headers()[CONTENT_ENCODING], "gzip");
    }

    #[test]
    fn marking_does_not_poll_streams_on_success_or_failure() {
        let polls = Rc::new(Cell::new(0_usize));
        let observed = Rc::clone(&polls);
        let body = Body::stream(stream::poll_fn(move |_| {
            observed.set(observed.get() + 1);
            Poll::<Option<Bytes>>::Pending
        }));
        let mut response = Response::new(body);
        DeliveryCompressionSupport::GzipBrotli
            .mark_identity_response(&mut response)
            .unwrap();
        response
            .headers_mut()
            .insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        assert!(
            DeliveryCompressionSupport::GzipBrotli
                .mark_identity_response(&mut response)
                .is_err()
        );
        assert_eq!(polls.get(), 0);
        assert!(response.body().is_stream());
    }

    #[test]
    fn intent_survives_parts_and_can_be_removed_before_reencoding() {
        let mut response = Response::new(Body::text("identity output"));
        DeliveryCompressionSupport::GzipBrotli
            .mark_identity_response(&mut response)
            .unwrap();
        let (parts, body) = response.into_parts();
        let mut rebuilt = Response::from_parts(parts, body);
        assert!(
            rebuilt
                .extensions_mut()
                .remove::<DeliveryCompressionIntent>()
                .is_some()
        );
        assert!(
            rebuilt
                .extensions()
                .get::<DeliveryCompressionIntent>()
                .is_none()
        );
    }
}
