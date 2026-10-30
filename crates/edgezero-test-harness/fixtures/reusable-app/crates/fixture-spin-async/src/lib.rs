#[cfg(target_arch = "wasm32")]
mod sdk {
    use fixture_spin_support::outbound::eager_outbound_paths_hold;
    use spin_sdk::wasip3;
    use spin_sdk::wasip3::exports::cli::run::Guest;

    struct OutboundEager;

    impl Guest for OutboundEager {
        #[expect(
            clippy::print_stdout,
            clippy::print_stderr,
            reason = "the executable SDK fixture reports its nonzero result to the local runner"
        )]
        async fn run() -> Result<(), ()> {
            eager_outbound_paths_hold().await.map_err(|error| {
                eprintln!("Spin eager SDK checks failed: {error}");
            })?;
            println!(
                "Spin eager SDK checks passed: callback cleanup, foreign scope, bodyless GET/HEAD resources, begin, batch, late collection, body handoff, cutoff"
            );
            Ok(())
        }
    }

    wasip3::cli::command::export!(OutboundEager);
}
