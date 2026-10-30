fn main() -> Result<(), fastly::Error> {
    fixture_fastly::initialize_launch(env!("CARGO_BIN_NAME"));
    fixture_fastly::faults()
}
