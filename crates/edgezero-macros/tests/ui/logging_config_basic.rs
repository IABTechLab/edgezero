use edgezero_core::manifest::LogLevel;

fn main() {
    let logging = edgezero_core::logging_config!("tests/fixtures/owns_logging.toml", "FASTLY");
    assert_eq!(logging.level, LogLevel::Debug);
    assert_eq!(logging.endpoint.as_deref(), Some("fixture_logs"));
    assert_eq!(logging.echo_stdout, Some(false));
}
