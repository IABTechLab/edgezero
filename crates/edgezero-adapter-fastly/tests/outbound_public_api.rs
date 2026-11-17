#[cfg(test)]
mod tests {
    use edgezero_adapter_fastly::context::{FastlyOutboundObservation, FastlyOutboundStats};
    use std::fmt::Debug;
    use std::hint::black_box;
    use std::time::Duration;

    #[test]
    fn registration_observation_public_api() {
        fn handle<T>()
        where
            T: Clone + Debug + Send + Sync,
        {
        }
        fn snapshot<T>()
        where
            T: Copy + Clone + Debug + Eq + PartialEq,
        {
        }
        fn read(stats: FastlyOutboundStats) -> (u64, u64, Duration, Duration) {
            (
                stats.registration_count(),
                stats.configuration_count(),
                stats.total_registration_duration(),
                stats.max_registration_duration(),
            )
        }
        handle::<FastlyOutboundObservation>();
        snapshot::<FastlyOutboundStats>();
        let reader: fn(FastlyOutboundStats) -> (u64, u64, Duration, Duration) = read;
        let snapshotter: fn(&FastlyOutboundObservation) -> FastlyOutboundStats =
            FastlyOutboundObservation::snapshot;
        assert!(FastlyOutboundObservation::current().is_none());
        black_box((reader, snapshotter));
    }
}
