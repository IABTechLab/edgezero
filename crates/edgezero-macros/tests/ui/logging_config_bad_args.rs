fn main() {
    let _ = edgezero_core::logging_config!(String::from("edgezero.toml"), "fastly");
    let _ = edgezero_core::logging_config!("edgezero.toml");
    let _ = edgezero_core::logging_config!("edgezero.toml", "fastly", "extra");
}
