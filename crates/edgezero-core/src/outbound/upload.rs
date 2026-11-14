use crate::body::Body;
use crate::error::{EdgeError, RequestBodyOverflowOrigin};

/// Scalar validation for one demand-driven upload; owns no reader, timer or buffer.
pub struct OutboundUploadCounter {
    declared: Option<u64>,
    maximum: u64,
    observed: Option<u64>,
}

impl OutboundUploadCounter {
    /// Check equality only after clean source EOF, never after cancellation or reader loss.
    ///
    /// # Errors
    /// Returns local 400 for an incomplete declared upload.
    #[inline]
    pub fn finish(self) -> Result<(), EdgeError> {
        let observed = self.observed.ok_or_else(upload_limit)?;
        if observed > self.maximum {
            return Err(upload_limit());
        }
        if self.declared.is_some_and(|declared| declared != observed) {
            return Err(length_mismatch());
        }
        Ok(())
    }

    /// Capture the immutable declaration before the provider consumes the Body.
    #[must_use]
    #[inline]
    pub fn new(body: &Body, maximum: u64) -> Self {
        Self {
            declared: body.forwarded_content_length(),
            maximum,
            observed: Some(0),
        }
    }

    /// Validate one entire chunk before writing any of it. A failure is terminal.
    ///
    /// # Errors
    /// Returns upload-origin 413 before a length mismatch, or local 400 for overrun.
    #[inline]
    pub fn observe(&mut self, chunk_bytes: usize) -> Result<(), EdgeError> {
        self.observed = self.observed.and_then(|observed| {
            let length = u64::try_from(chunk_bytes).ok()?;
            observed.checked_add(length)
        });
        let next = self.observed.ok_or_else(upload_limit)?;
        if next > self.maximum {
            return Err(upload_limit());
        }
        if self.declared.is_some_and(|declared| next > declared) {
            return Err(length_mismatch());
        }
        Ok(())
    }

    /// Source bytes observed, including a rejected chunk; None denotes arithmetic overflow.
    #[must_use]
    #[inline]
    pub fn observed_bytes(&self) -> Option<u64> {
        self.observed
    }
}

fn length_mismatch() -> EdgeError {
    EdgeError::bad_request("forwarded upload length does not match its declaration")
}

fn upload_limit() -> EdgeError {
    EdgeError::payload_too_large(RequestBodyOverflowOrigin::OutboundUpload)
}

#[cfg(test)]
mod tests {
    use super::OutboundUploadCounter;
    use crate::{Body, EdgeError, RequestBodyOverflowOrigin};
    use futures_util::stream;
    use std::mem::size_of;

    fn body(length: Option<u64>) -> Body {
        let mut body = Body::from_stream(stream::empty());
        body.set_forwarded_content_length(length);
        body
    }

    #[test]
    fn complete_upload_accepts_exact_length_empty_chunks_and_unknown_length() {
        for declared in [None, Some(4)] {
            let mut counter = OutboundUploadCounter::new(&body(declared), 8);
            for length in [0, 1, 0, 3, 0] {
                counter.observe(length).unwrap();
            }
            counter.finish().unwrap();
        }
        OutboundUploadCounter::new(&body(Some(0)), 8)
            .finish()
            .unwrap();
    }

    #[test]
    fn incomplete_or_overrun_upload_is_local_bad_request_without_provider_evidence() {
        let mut incomplete = OutboundUploadCounter::new(&body(Some(4)), 8);
        incomplete.observe(3).unwrap();
        let short = incomplete.finish().unwrap_err();
        assert!(matches!(short, EdgeError::BadRequest { .. }));
        assert!(short.provider_failure_detail().is_none());
        let mut overrun = OutboundUploadCounter::new(&body(Some(4)), 8);
        overrun.observe(3).unwrap();
        let long = overrun.observe(2).unwrap_err();
        assert!(matches!(long, EdgeError::BadRequest { .. }));
        assert!(long.provider_failure_detail().is_none());
    }

    #[test]
    fn upload_cap_precedes_length_mismatch() {
        let mut counter = OutboundUploadCounter::new(&body(Some(1)), 2);
        let error = counter.observe(3).unwrap_err();
        assert!(matches!(
            error,
            EdgeError::PayloadTooLarge {
                origin: RequestBodyOverflowOrigin::OutboundUpload
            }
        ));
        assert_eq!(counter.observed_bytes(), Some(3));
    }

    #[test]
    #[expect(
        clippy::print_stdout,
        reason = "retain native and WASM Body layout measurements for allocation review"
    )]
    fn body_layout_and_metadata_do_not_require_reading_the_stream() {
        println!("Body layout: {} bytes", size_of::<Body>());
        println!("Scalar length storage: {} bytes", size_of::<Option<u64>>());
        assert_eq!(body(Some(4096)).forwarded_content_length(), Some(4096));
    }
}
