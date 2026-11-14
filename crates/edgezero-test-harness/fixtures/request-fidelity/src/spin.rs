use crate::Fixture;
use spin_sdk::http::Request;
use spin_sdk::http_service;

#[http_service]
async fn handle(req: Request) -> anyhow::Result<edgezero_adapter_spin::SpinResponse> {
    edgezero_adapter_spin::run_app::<Fixture>(req).await
}
