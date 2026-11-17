fn main() -> anyhow::Result<()> {
    fixture_core::init_fixture_logging(|line| eprintln!("{line}"))?;
    edgezero_adapter_axum::dev_server::run_app::<fixture_core::assembly::ConfigurationFailureApp>()
}
