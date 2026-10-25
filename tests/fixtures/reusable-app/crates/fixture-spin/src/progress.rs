use edgezero_adapter_spin::{
    outbound::connect_timeout_fallback_holds_for_test,
    response::response_source_diagnostics_hold_for_test,
};
use edgezero_core::EdgeError;
use fixture_core::qualification::progress::{self, Job, ProbeRuntime, ProgressHooks};
use futures::{channel::oneshot, future::LocalBoxFuture};
use http_body::Body as _;
use serde_json::json;
use spin_sdk::http::body::IncomingBodyExt as _;
use std::{
    sync::{
        Arc, Once,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

struct Runtime;

impl ProbeRuntime for Runtime {
    fn adapter(&self) -> &'static str {
        "spin"
    }
    fn perform(&self, target: String) -> Job {
        Box::pin(async move {
            let response = spin_sdk::http::get(&target)
                .await
                .map_err(|_| progress::probe_failure())?;
            if response.status() != 200 {
                return Err(progress::probe_failure());
            }
            let body = response
                .into_body()
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
        spin_sdk::wasip3::spawn(async move {
            let _ = send.send(job.await);
        });
        Ok(Box::pin(async move {
            receive.await.map_err(|_| progress::probe_failure())?
        }))
    }
    fn pause(&self, duration: Duration) -> LocalBoxFuture<'static, ()> {
        Box::pin(spin_sdk::time::sleep(duration))
    }
}

pub async fn dispatch(
    request: spin_sdk::http::Request,
) -> anyhow::Result<edgezero_adapter_spin::SpinResponse> {
    static PANIC_DIAGNOSTIC: Once = Once::new();
    PANIC_DIAGNOSTIC.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |panic| {
            // Local diagnostic only: preserve failure and the original hook, never render it on wire.
            let message: String = panic.to_string().chars().take(512).collect();
            eprintln!(
                "{}",
                json!({"event":"progress-guest-panic","message":message,"qualification":false})
            );
            previous(panic);
        }));
    });
    eprintln!(
        "{}",
        json!({"event":"progress-dispatch-start","qualification":false})
    );
    let case = request
        .uri()
        .path()
        .strip_prefix("/progress/")
        .unwrap_or("");
    if case == "sdk-source-diagnostics" {
        response_source_diagnostics_hold_for_test().await?;
        return sdk_probe_response(
            json!({"source_diagnostics":"pass", "cases":8, "qualification":false}),
        );
    }
    if case == "sdk-connect-fallback" {
        if !connect_timeout_fallback_holds_for_test().await {
            anyhow::bail!("connect refusal lost aggregate deadline enforcement");
        }
        return sdk_probe_response(
            json!({"connect_fallback":"pass", "outcomes":5, "budget_sources":4, "checks":60, "qualification":false}),
        );
    }
    if case == "sdk-marker-headers" {
        return sdk_marker_headers(&request).await;
    }
    if matches!(case, "sdk-stream-ready" | "sdk-stream-timer-only") {
        return sdk_response_control(&request, case.to_owned());
    }
    let hint = request.body().size_hint();
    let signal = json!({"source":"spin-sdk-after-conversion", "size_hint_lower":hint.lower(),
        "size_hint_upper":hint.upper(), "end_stream":request.body().is_end_stream(), "raw_framing_evidence":false});
    let returned = Arc::new(AtomicBool::new(false));
    let app = progress::application(
        Arc::new(Runtime),
        signal,
        Some(Arc::clone(&returned)),
        edgezero_adapter_spin::SPIN_PLATFORM,
    )?;
    let response = edgezero_adapter_spin::dispatch_app::<ProgressHooks>(&app, request).await;
    returned.store(true, Ordering::SeqCst);
    eprintln!(
        "{}",
        json!({"event":"progress-dispatch-returned","qualification":false})
    );
    response
}

fn sdk_probe_response(
    probe: serde_json::Value,
) -> anyhow::Result<edgezero_adapter_spin::SpinResponse> {
    use spin_sdk::http::IntoResponse as _;
    edgezero_core::http::response_builder()
        .header("content-type", "application/json")
        .body(probe.to_string())?
        .into_response()
        .map_err(|_| anyhow::anyhow!("SDK probe fixture conversion failed"))
}

async fn sdk_marker_headers(
    request: &spin_sdk::http::Request,
) -> anyhow::Result<edgezero_adapter_spin::SpinResponse> {
    use spin_sdk::http::{FromResponse as _, IntoRequest as _, IntoResponse as _};
    let target: edgezero_core::http::Uri = request
        .headers()
        .get("x-fixture-backend")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| anyhow::anyhow!("missing SDK probe origin"))?
        .parse()?;
    if target.scheme_str() != Some("http")
        || target.host() != Some("127.0.0.1")
        || target.port_u16().is_none()
        || target.query().is_some()
        || !["/marker/get/200/upstream", "/marker/get/200/nominated"].contains(&target.path())
    {
        return Err(anyhow::anyhow!("SDK probe requires controlled origin"));
    }
    let outgoing = edgezero_core::http::request_builder()
        .uri(target)
        .body(spin_sdk::http::EmptyBody::new())?
        .into_request()
        .map_err(|_| anyhow::anyhow!("SDK probe request conversion failed"))?;
    let native = spin_sdk::wasip3::http::client::send(outgoing)
        .await
        .map_err(|_| anyhow::anyhow!("SDK probe send failed"))?;
    let fields = native.get_headers().copy_all();
    let connection: Vec<_> = fields
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("connection"))
        .map(|(_, value)| String::from_utf8_lossy(value).into_owned())
        .collect();
    let markers: Vec<_> = fields
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("x-edgezero-proxy"))
        .map(|(_, value)| String::from_utf8_lossy(value).into_owned())
        .collect();
    let converted = spin_sdk::http::Response::from_response(native)
        .map_err(|_| anyhow::anyhow!("SDK probe response conversion failed"))?;
    let bytes = converted
        .into_body()
        .bytes()
        .await
        .map_err(|_| anyhow::anyhow!("SDK probe body failed"))?;
    if !bytes.is_empty() {
        return Err(anyhow::anyhow!("SDK probe expected empty body"));
    }
    let report = json!({"case":"sdk-marker-headers","sdk_native":true,"connection":connection,"markers":markers,"qualification":false});
    edgezero_core::http::response_builder()
        .header("content-type", "application/json")
        .body(report.to_string())?
        .into_response()
        .map_err(|_| anyhow::anyhow!("SDK probe output conversion failed"))
}

fn sdk_response_control(
    request: &spin_sdk::http::Request,
    case: String,
) -> anyhow::Result<edgezero_adapter_spin::SpinResponse> {
    use spin_sdk::wasip3::{http::types::Fields, http_compat::BodyWriter};
    use std::future::IntoFuture as _;

    let token = request
        .headers()
        .get("x-request-token")
        .and_then(|v| v.to_str().ok())
        .filter(|s| {
            !s.is_empty()
                && s.len() <= 64
                && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
        .ok_or_else(|| anyhow::anyhow!("invalid SDK control token"))?
        .to_owned();
    let fields = Fields::from_list(&[("content-type".to_owned(), b"application/json".to_vec())])
        .map_err(|_| anyhow::anyhow!("SDK control field construction failed"))?;
    let (writer, contents, body_result) = BodyWriter::new();
    let (response, transmission) =
        edgezero_adapter_spin::SpinResponse::new(fields, Some(contents), body_result);
    spin_sdk::wasip3::spawn(async move {
        let BodyWriter {
            mut stream_writer,
            result_writer,
            ..
        } = writer;
        let result = async {
            if !stream_writer.write_all(b" ".to_vec()).await.is_empty() {return Err(());}
            let started = edgezero_core::time::MonotonicInstant::now();
            if case.ends_with("timer-only") {spin_sdk::time::sleep(Duration::from_millis(100)).await;}
            let pause_ns = started.elapsed().as_nanos() as u64;
            let report = json!({"adapter":"spin","case":case,"token":token,"sdk_native":true,"pause_ns":pause_ns,
                "revision":option_env!("EDGEZERO_QUALIFICATION_REVISION").unwrap_or("unrecorded"),"qualification":false});
            if !stream_writer.write_all(report.to_string().into_bytes()).await.is_empty() {return Err(());}
            drop(stream_writer);
            result_writer.write(Ok(None)).await.map_err(|_|())?;
            transmission.into_future().await.map_err(|_|())?;
            Ok(())
        }.await;
        eprintln!(
            "{}",
            json!({"event":"sdk-stream-coordinator-terminal","case":case,"token":token,"coordinator_ok":result.is_ok(),"qualification":false})
        );
    });
    Ok(response)
}
