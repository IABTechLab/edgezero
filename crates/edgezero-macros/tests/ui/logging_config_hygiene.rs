use edgezero_core::manifest::LogLevel;

struct Option;
struct Default;
struct Some;
struct None;

macro_rules! include_bytes {
    ($path:expr) => { b"" };
}

mod nested {
    pub fn level() -> edgezero_core::manifest::LogLevel {
        edgezero_core::logging_config!(
            "tests/fixtures/owns_logging.toml",
            String::from("fastly").as_str(),
        ).level
    }
}

#[allow(non_camel_case_types)]
mod primitives {
    type str = u32;
    type u8 = u16;

    pub fn level() -> edgezero_core::manifest::LogLevel {
        let _: (str, u8) = (0, 0);
        edgezero_core::logging_config!("tests/fixtures/owns_logging.toml", "fastly").level
    }
}

mod competing_from {
    trait CompetingFrom {
        fn from(value: &str) -> Self;
    }

    impl CompetingFrom for String {
        fn from(_: &str) -> Self {
            String::new()
        }
    }

    edgezero_core::app!("tests/fixtures/owns_logging.toml", CompetingApp);

    pub fn verify() {
        use edgezero_core::app::Hooks as _;
        let _ = <String as CompetingFrom>::from("unused");
        let logging = edgezero_core::logging_config!("tests/fixtures/owns_logging.toml", "fastly");
        assert_eq!(logging.endpoint.as_deref(), Some("fixture_logs"));
        assert_eq!(CompetingApp::logging_for("fastly").endpoint, logging.endpoint);
    }
}

fn main() {
    let _ = (Option, Default, Some, None, include_bytes!("ignored"));
    let adapter = "axum";
    let __edgezero_logging_adapter = "spin";
    let mut calls = 0;
    let logging = edgezero_core::logging_config!("tests/fixtures/owns_logging.toml", {
        calls += 1;
        "FaStLy"
    });
    assert_eq!(calls, 1);
    assert_eq!(adapter, "axum");
    assert_eq!(__edgezero_logging_adapter, "spin");
    assert_eq!(logging.level, LogLevel::Debug);
    assert_eq!(logging.endpoint.as_deref(), ::core::option::Option::Some("fixture_logs"));
    assert_eq!(nested::level(), LogLevel::Debug);
    assert_eq!(primitives::level(), LogLevel::Debug);
    competing_from::verify();
    assert_eq!(edgezero_core::logging_config!("tests/fixtures/logging_config.toml", "fastly").level, LogLevel::Off);
    assert_eq!(edgezero_core::logging_config!("tests/fixtures/owns_logging.toml", "fastly").level, LogLevel::Debug);
}
