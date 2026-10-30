#[path = "../progress.rs"]
mod progress;

fn main() -> anyhow::Result<()> {
    fixture_core::launch::initialize_environment(env!("CARGO_BIN_NAME"));
    edgezero_adapter_axum::dev_server::run_app::<progress::NativeProgressApp>()
}
