use anyhow::{Result, ensure};
use edgezero_adapter_axum::outbound::AxumOutboundClient;
use edgezero_adapter_axum::outbound_executor::AxumOutboundExecutor;
use edgezero_core::{Body, Deadline, EdgeError, HttpClient, OutboundRequest};
use futures_util::stream::poll_fn;
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::Poll;
use std::thread;
use std::time::Duration;
use tokio::runtime::Builder;

async fn rejected(client: &HttpClient) -> Result<()> {
    let polls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&polls);
    let body = Body::from_stream(poll_fn(move |_| {
        counted.fetch_add(1, Ordering::SeqCst);
        Poll::Pending
    }));
    ensure!(
        matches!(
            client
                .send(OutboundRequest::get("http://127.0.0.1:9/")?.body(body))
                .await,
            Err(EdgeError::Internal { .. })
        ),
        "invalid owner must reject Send"
    );
    ensure!(
        polls.load(Ordering::SeqCst) == 0,
        "invalid owner polled request body"
    );
    ensure!(
        Arc::strong_count(&polls) == 1,
        "invalid owner retained request body"
    );
    ensure!(
        matches!(
            client
                .begin(OutboundRequest::post("http://127.0.0.1:9/")?.body("buffered"))
                .await,
            Err(EdgeError::Internal { .. })
        ),
        "invalid owner must reject Begin"
    );
    let failure = client
        .start_batch_until(
            vec![OutboundRequest::get("http://127.0.0.1:9/")?],
            Deadline::after(Duration::from_secs(1)),
        )
        .collect()
        .await
        .expect_err("invalid owner must reject Batch");
    ensure!(
        matches!(failure.into_parts().0, EdgeError::Internal { .. }),
        "invalid batch error category"
    );
    Ok(())
}

fn main() -> Result<()> {
    ensure!(
        cfg!(panic = "abort"),
        "ownership fixture must be compiled with panic=abort"
    );
    let runtime = Builder::new_current_thread().enable_all().build()?;
    let owner = AxumOutboundExecutor::new();
    let client = HttpClient::with_client(AxumOutboundClient::try_new(owner.handle())?);
    // No ambient LocalSet: this must schedule on the explicit owner without a panic.
    runtime.block_on(async {
        let pending = client
            .begin(OutboundRequest::post("http://127.0.0.1:9/")?.body("buffered"))
            .await?;
        drop(pending);
        Ok::<(), EdgeError>(())
    })?;
    let foreign = client.clone();
    thread::spawn(move || {
        Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(rejected(&foreign))
    })
    .join()
    .expect("ownership worker")?;
    drop(owner);
    runtime.block_on(rejected(&client))?;
    println!(
        "{}",
        json!({"event":"axum-executor-ownership", "status":"pass", "panic":"abort", "live_without_ambient":true, "foreign_rejected":true, "dropped_rejected":true, "qualification":false})
    );
    Ok(())
}
