use crate::app::{build_logging_lookup, load_manifest};
use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Expr, Ident, LitStr, Token, parse_macro_input};

struct LoggingConfigArgs {
    adapter: Expr,
    path: LitStr,
}

impl Parse for LoggingConfigArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let path = input.parse()?;
        input.parse::<Token![,]>()?;
        let adapter = input.parse()?;
        if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
        }
        Ok(Self { adapter, path })
    }
}

pub fn expand_logging_config(input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(input as LoggingConfigArgs);
    expand(&args)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn expand(args: &LoggingConfigArgs) -> syn::Result<TokenStream2> {
    let (path, manifest) =
        load_manifest(&args.path).map_err(|message| syn::Error::new(args.path.span(), message))?;
    let path_literal = LitStr::new(&path.to_string_lossy(), args.path.span());
    let adapter = Ident::new("__edgezero_logging_adapter", Span::mixed_site());
    let lookup = build_logging_lookup(&manifest, &adapter);
    let expression = &args.adapter;
    Ok(quote! {
        {
            const _: &[::core::primitive::u8] = ::core::include_bytes!(#path_literal);
            (|#adapter: &::core::primitive::str| { #lookup })(#expression)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{LoggingConfigArgs, expand};
    use proc_macro2::Span;
    use syn::{LitStr, parse_quote};

    #[test]
    fn logging_read_failures_are_compile_errors_on_every_host() {
        let directory = tempfile::tempdir().expect("isolated reader failures");
        for path in [
            directory.path().join("missing.toml"),
            directory.path().to_path_buf(),
        ] {
            let args = LoggingConfigArgs {
                adapter: parse_quote!("fastly"),
                path: LitStr::new(&path.to_string_lossy(), Span::call_site()),
            };
            let error = expand(&args).expect_err("missing or directory input must fail");
            assert!(error.to_string().starts_with("failed to read "));
            assert!(error.to_string().contains(&*path.to_string_lossy()));
            assert!(
                error
                    .into_compile_error()
                    .to_string()
                    .contains("compile_error")
            );
        }
    }

    #[test]
    fn logging_arguments_require_literal_path_and_one_expression() {
        for input in [
            r#""edgezero.toml", adapter"#,
            r#""edgezero.toml", String::from("fastly").as_str(),"#,
        ] {
            assert!(
                syn::parse_str::<LoggingConfigArgs>(input).is_ok(),
                "{input}"
            );
        }
        for input in [
            "path, adapter",
            r#""edgezero.toml""#,
            r#""edgezero.toml", adapter, extra"#,
        ] {
            assert!(
                syn::parse_str::<LoggingConfigArgs>(input).is_err(),
                "{input}"
            );
        }
    }

    #[test]
    fn logging_expansion_excludes_runtime_parsing_and_route_generation() {
        let args = syn::parse_str::<LoggingConfigArgs>(
            r#""tests/fixtures/logging_config_manual_routes.toml", adapter"#,
        )
        .expect("valid arguments");
        let expanded = expand(&args).expect("logging ignores Rust handler symbols");
        syn::parse2::<syn::Block>(expanded.clone()).expect("logging is a block expression");
        let rendered = expanded.to_string();
        assert!(
            rendered.contains(":: core :: include_bytes !"),
            "{rendered}"
        );
        for forbidden in [
            "ManifestLoader",
            "from_baked_json",
            "serde_json",
            "std :: fs",
            "build_router",
            "RouterService",
            "return",
        ] {
            assert!(!rendered.contains(forbidden), "{forbidden}: {rendered}");
        }
    }
}
