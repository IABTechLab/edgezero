fn main() -> anyhow::Result<()> {
    fixture_core::launch::initialize_environment(env!("CARGO_BIN_NAME"));
    edgezero_adapter_axum::dev_server::run_app::<ActiveApp>()
}

#[cfg(not(feature = "qualification"))]
use fixture_core::FixtureApp as ActiveApp;
#[cfg(feature = "qualification")]
use fixture_core::qualification::QualificationApp as ActiveApp;
