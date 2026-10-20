use edgezero_core::EdgeError;
use fixture_core::qualification::progress::{self, Job, ProbeRuntime, ProgressHooks};
use futures::{channel::oneshot, future::LocalBoxFuture};
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

struct Runtime;

impl ProbeRuntime for Runtime {
    fn adapter(&self) -> &'static str {
        "cloudflare"
    }
    fn perform(&self, target: String) -> Job {
        Box::pin(async move {
            let request = worker::Request::new(&target, worker::Method::Get)
                .map_err(|_| progress::probe_failure())?;
            let mut response = worker::Fetch::Request(request)
                .send()
                .await
                .map_err(|_| progress::probe_failure())?;
            if response.status_code() != 200 {
                return Err(progress::probe_failure());
            }
            let body = response
                .bytes()
                .await
                .map_err(|_| progress::probe_failure())?;
            if body.len() > 64 {
                return Err(progress::probe_failure());
            }
            Ok(body.len() as u64)
        })
    }
    fn start(&self, target: String) -> Result<Job, EdgeError> {
        let job = self.perform(target);
        let (send, receive) = oneshot::channel();
        worker::wasm_bindgen_futures::spawn_local(async move {
            let _ = send.send(job.await);
        });
        Ok(Box::pin(async move {
            receive.await.map_err(|_| progress::probe_failure())?
        }))
    }
    fn pause(&self, duration: Duration) -> LocalBoxFuture<'static, ()> {
        Box::pin(worker::Delay::from(duration))
    }
}

pub async fn dispatch(
    request: worker::Request,
    env: worker::Env,
    ctx: worker::Context,
) -> worker::Result<worker::Response> {
    // Inspect the original object without cloning or consuming its body.
    let signal = json!({"source":"workerd-before-conversion", "body_absent":request.inner().body().is_none()});
    let returned = Arc::new(AtomicBool::new(false));
    let app = progress::application(
        Arc::new(Runtime),
        signal,
        Some(Arc::clone(&returned)),
        edgezero_adapter_cloudflare::CLOUDFLARE_PLATFORM,
    );
    let response =
        edgezero_adapter_cloudflare::dispatch_app::<ProgressHooks>(&app, request, env, ctx).await;
    returned.store(true, Ordering::SeqCst);
    response
}
