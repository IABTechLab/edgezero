fn main() -> Result<(), fastly::Error> {
    fixture_fastly::initialize_launch(env!("CARGO_BIN_NAME"));
    edgezero_adapter_fastly::run_app_with_hooks::<fixture_fastly::MeasuredApp, _, _, _>(
        fixture_fastly::extend,
        |_response| (),
    )
    .map(|_state| ())
}
