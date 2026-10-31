#![allow(
    dead_code,
    reason = "shared scenarios are selected by their contract owner"
)]
use std::future::pending;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;

use bytes::Bytes;
use edgezero_adapter_spin::outbound::test_support::{deadline_body, dispatch_ingress_with};
use edgezero_core::app::App;
use edgezero_core::error::EdgeError;
use edgezero_core::http::{Method, Uri};
use edgezero_core::response_egress::ResponseEgressEnvelope;
use edgezero_core::time::{Deadline, MonotonicClock, MonotonicInstant};
use futures::executor::block_on;
use futures_util::StreamExt as _;
use futures_util::stream::{Empty, poll_fn};

struct DropSignal(Arc<AtomicUsize>);

#[expect(
    clippy::missing_trait_methods,
    reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
)]
impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Runs the terminal source-release probe used by the WASM contract suite.
#[must_use]
#[inline]
pub fn deadline_body_releases_source() -> bool {
    let dropped = Arc::new(AtomicUsize::new(0));
    let signal = DropSignal(Arc::clone(&dropped));
    let source = poll_fn(move |_cx| {
        let _keep_alive = &signal;
        Poll::<Option<Result<Bytes, io::Error>>>::Pending
    });
    let start = MonotonicInstant::now();
    let clock = MonotonicClock::new(move || start);
    let body = deadline_body(source, Deadline::at_instant(start), clock, |_| {
        pending::<()>()
    });
    let Ok(mut body_stream) = body.into_stream() else {
        return false;
    };
    let Some(Err(error)) = block_on(body_stream.next()) else {
        return false;
    };
    matches!(error, EdgeError::RequestTimeout { .. }) && dropped.load(Ordering::SeqCst) == 1
}

/// Exercises a source-construction failure after admission through the production seam.
#[doc(hidden)]
#[inline]
pub async fn dispatch_ingress_source_error(
    app: &App,
    method: Method,
    uri: Uri,
) -> anyhow::Result<ResponseEgressEnvelope> {
    dispatch_ingress_with(
        app,
        method,
        uri,
        || -> Result<Empty<Result<Bytes, io::Error>>, EdgeError> {
            Err(EdgeError::internal(anyhow::anyhow!(
                "spin source construction failed"
            )))
        },
        Ok,
    )
    .await
}
