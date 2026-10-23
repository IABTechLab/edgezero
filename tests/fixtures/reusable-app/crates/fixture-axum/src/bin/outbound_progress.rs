#[path = "../progress.rs"]
mod progress;

fn main() -> anyhow::Result<()> {
    edgezero_adapter_axum::dev_server::run_app::<progress::NativeProgressApp>()
}
