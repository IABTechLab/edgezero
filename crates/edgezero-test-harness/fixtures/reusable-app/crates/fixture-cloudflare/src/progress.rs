use edgezero_core::EdgeError;
use fixture_core::qualification::progress::{self, Job, ProbeRuntime, ProgressHooks};
use futures::{channel::oneshot, future::LocalBoxFuture};
use serde_json::json;
use std::{sync::Arc, time::Duration};

struct Runtime;

impl ProbeRuntime for Runtime {
    fn adapter(&self) -> &'static str {
        "cloudflare"
    }
    fn outbound_client(&self) -> Result<Arc<dyn edgezero_core::OutboundHttpClient>, EdgeError> {
        Ok(Arc::new(
            edgezero_adapter_cloudflare::outbound::CloudflareOutboundClient::new(),
        ))
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
    let probe = request
        .path()
        .strip_prefix("/progress/overflow/")
        .filter(|case| {
            matches!(
                *case,
                "inbound"
                    | "buffered-7"
                    | "buffered-8"
                    | "buffered-9"
                    | "streamed-7"
                    | "streamed-8"
                    | "streamed-9"
                    | "upstream-413"
            )
        })
        .map(str::to_owned)
        .zip(
            request
                .headers()
                .get("x-request-token")
                .ok()
                .flatten()
                .filter(|token| {
                    !token.is_empty()
                        && token.len() <= 64
                        && token
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                }),
        );
    if let Some((case, token)) = &probe {
        worker::console_log!(
            "{}",
            json!({"event":"overflow-dispatch","phase":"entry","case":case,"token":token,"qualification":false})
        );
    }
    // Inspect the original object without cloning or consuming its body.
    let signal = json!({"source":"workerd-before-conversion", "body_absent":request.inner().body().is_none()});
    let returned = Arc::new(progress::DispatchReturn::default());
    let app = if request.path().starts_with("/progress/forward/") {
        fixture_core::qualification::forwarding::application(
            edgezero_adapter_cloudflare::CLOUDFLARE_PLATFORM,
            signal,
        )
    } else {
        progress::application(
            Arc::new(Runtime),
            signal,
            Some(Arc::clone(&returned)),
            edgezero_adapter_cloudflare::CLOUDFLARE_PLATFORM,
        )
        .map_err(|_| worker::Error::RustError("progress probe configuration failed".into()))?
    };
    let response =
        edgezero_adapter_cloudflare::dispatch_app::<ProgressHooks>(&app, request, env, ctx).await;
    returned.mark_returned();
    if let Some((case, token)) = &probe {
        worker::console_log!(
            "{}",
            json!({"event":"overflow-dispatch","phase":"return","case":case,"token":token,
            "status":response.as_ref().ok().map(worker::Response::status_code),"outcome":if response.is_ok(){"response"}else{"error"},"qualification":false})
        );
    }
    response
}
