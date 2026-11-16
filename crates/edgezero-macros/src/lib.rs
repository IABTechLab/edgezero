mod action;
mod app;
mod app_config;
mod logging_config;
mod manifest_definitions;

use proc_macro::TokenStream;

#[proc_macro_attribute]
#[inline]
pub fn action(attr: TokenStream, item: TokenStream) -> TokenStream {
    action::expand_action(attr, item)
}

#[proc_macro]
#[inline]
pub fn app(input: TokenStream) -> TokenStream {
    app::expand_app(input)
}

/// Resolve manifest logging at compile time without generating an application.
///
/// Use `edgezero_core::logging_config!("edgezero.toml", adapter)` in a hand-written
/// `Hooks::logging_for` implementation. The manifest path is relative to the
/// consuming crate's `CARGO_MANIFEST_DIR`; `adapter` is evaluated once as `&str`.
/// Resolution and validation match `app!`, including whole-section overrides.
/// This supplies metadata only, without runtime parsing or logger initialization.
#[proc_macro]
#[inline]
pub fn logging_config(input: TokenStream) -> TokenStream {
    logging_config::expand_logging_config(input)
}

#[proc_macro_derive(AppConfig, attributes(secret, app_config))]
#[inline]
pub fn app_config_derive(input: TokenStream) -> TokenStream {
    app_config::derive(input)
}
