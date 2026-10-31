#[cfg(test)]
mod tests {
    use edgezero_core::response_egress_framing::ResponseEgressFramingMode;
    use std::sync::{Mutex, PoisonError};

    use edgezero_core::app::App;
    use edgezero_core::http::Method;
    use edgezero_core::{BOOT_LOG_TARGET, EdgeError, MonotonicClock};
    use futures::executor::block_on;

    static LOGGER: BootLogger = BootLogger(Mutex::new(Vec::new()));

    struct BootLogger(Mutex<Vec<String>>);

    impl log::Log for BootLogger {
        fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
            metadata.target() == BOOT_LOG_TARGET
        }

        fn flush(&self) {}

        fn log(&self, record: &log::Record<'_>) {
            if self.enabled(record.metadata()) {
                self.0
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(format!("{}: {}", record.level(), record.args()));
            }
        }
    }

    #[test]
    fn assembly_error_logs_full_source_once_and_never_exposes_it_on_the_wire() {
        log::set_logger(&LOGGER).unwrap();
        log::set_max_level(log::LevelFilter::Error);
        let source = anyhow::anyhow!("private-token=secret").context("state initialization failed");
        let clock = MonotonicClock::default();
        let envelope = App::assembly_error_egress(
            EdgeError::internal(source),
            Method::GET,
            clock.now(),
            clock,
        );
        let Ok((prepared, _, mut attempt, egress_clock)) =
            envelope.begin(ResponseEgressFramingMode::ExplicitBufferedLength)
        else {
            panic!("assembly failure response must prepare");
        };
        let bytes = block_on(
            prepared
                .into_response()
                .into_body()
                .into_bytes_bounded(2048),
        )
        .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["message"], "internal server error");
        assert!(attempt.begin_writing());
        assert!(attempt.complete(egress_clock.now()));
        drop(attempt);
        let records = LOGGER.0.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert!(records[0].starts_with("ERROR: "));
        assert!(records[0].contains("state initialization failed"));
        assert!(records[0].contains("private-token=secret"));
    }
}
