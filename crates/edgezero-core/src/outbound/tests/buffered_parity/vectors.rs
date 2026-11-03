//! Private contract vectors shared by the core companion and native fixture/controller.
#![allow(
    clippy::arbitrary_source_item_ordering,
    clippy::too_many_lines,
    clippy::wildcard_enum_match_arm,
    reason = "the frozen execution table precedes its renderer; unrelated cases use the baseline reply"
)]

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Coding {
    Br,
    Deflate,
    Gzip,
    Identity,
    Passthrough,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HeaderCase {
    Bytes,
    Count,
    Duplicate,
    MarkerBytesOff,
    MarkerBytesOn,
    MarkerCountOff,
    MarkerCountOn,
    Normalize,
    Tie,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarkerCase {
    InheritOff,
    Off,
    On,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorCase {
    Body,
    BrotliWindow,
    DecoderMemory,
    Gzip,
    Head,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Authority,
    Bodyless205,
    Cache,
    Coding {
        coding: Coding,
        original: bool,
        partial: bool,
    },
    Connect,
    Delay {
        body: bool,
        deadline: bool,
    },
    Error(ErrorCase),
    GetBody,
    Header(HeaderCase),
    Marker(MarkerCase),
    OriginalIdentity {
        declared: bool,
        explicit: bool,
        size: u8,
    },
    Passthrough,
    Policy {
        coding: Coding,
        declared: bool,
        size: u8,
    },
    Rechunk {
        coding: Coding,
        original: bool,
    },
    RequestNormalize,
    StreamLive,
    Upload {
        cap: u64,
    },
    Upstream413,
}

#[derive(Clone, Copy, Debug)]
pub struct Case {
    pub kind: Kind,
    pub name: &'static str,
    pub streamed: bool,
}

// This is the original nonredirect body/declared table, in its original execution order.
pub const POLICY_CASES: &[Case] = &[
    Case::new(
        "pair-policy-b-identity-10",
        Kind::Policy {
            coding: Coding::Identity,
            declared: false,
            size: 10,
        },
        false,
    ),
    Case::new(
        "pair-policy-b-identity-11",
        Kind::Policy {
            coding: Coding::Identity,
            declared: false,
            size: 11,
        },
        false,
    ),
    Case::new(
        "pair-policy-b-identity-declared",
        Kind::Policy {
            coding: Coding::Identity,
            declared: true,
            size: 11,
        },
        false,
    ),
    Case::new(
        "pair-policy-b-gzip-10",
        Kind::Policy {
            coding: Coding::Gzip,
            declared: false,
            size: 10,
        },
        false,
    ),
    Case::new(
        "pair-policy-b-gzip-11",
        Kind::Policy {
            coding: Coding::Gzip,
            declared: false,
            size: 11,
        },
        false,
    ),
    Case::new(
        "pair-policy-b-gzip-declared",
        Kind::Policy {
            coding: Coding::Gzip,
            declared: true,
            size: 11,
        },
        false,
    ),
    Case::new(
        "pair-policy-b-pass-10",
        Kind::Policy {
            coding: Coding::Passthrough,
            declared: false,
            size: 10,
        },
        false,
    ),
    Case::new(
        "pair-policy-b-pass-11",
        Kind::Policy {
            coding: Coding::Passthrough,
            declared: false,
            size: 11,
        },
        false,
    ),
    Case::new(
        "pair-policy-b-pass-declared",
        Kind::Policy {
            coding: Coding::Passthrough,
            declared: true,
            size: 11,
        },
        false,
    ),
    Case::new(
        "pair-policy-s-identity-10",
        Kind::Policy {
            coding: Coding::Identity,
            declared: false,
            size: 10,
        },
        true,
    ),
    Case::new(
        "pair-policy-s-identity-11",
        Kind::Policy {
            coding: Coding::Identity,
            declared: false,
            size: 11,
        },
        true,
    ),
    Case::new(
        "pair-policy-s-identity-declared",
        Kind::Policy {
            coding: Coding::Identity,
            declared: true,
            size: 11,
        },
        true,
    ),
    Case::new(
        "pair-policy-s-gzip-10",
        Kind::Policy {
            coding: Coding::Gzip,
            declared: false,
            size: 10,
        },
        true,
    ),
    Case::new(
        "pair-policy-s-gzip-11",
        Kind::Policy {
            coding: Coding::Gzip,
            declared: false,
            size: 11,
        },
        true,
    ),
    Case::new(
        "pair-policy-s-gzip-declared",
        Kind::Policy {
            coding: Coding::Gzip,
            declared: true,
            size: 11,
        },
        true,
    ),
    Case::new(
        "pair-policy-s-pass-10",
        Kind::Policy {
            coding: Coding::Passthrough,
            declared: false,
            size: 10,
        },
        true,
    ),
    Case::new(
        "pair-policy-s-pass-11",
        Kind::Policy {
            coding: Coding::Passthrough,
            declared: false,
            size: 11,
        },
        true,
    ),
    Case::new(
        "pair-policy-s-pass-declared",
        Kind::Policy {
            coding: Coding::Passthrough,
            declared: true,
            size: 11,
        },
        true,
    ),
];

pub const EXTRA_CASES: &[Case] = &[
    Case::new(
        "pair-head-call-timeout",
        Kind::Delay {
            body: false,
            deadline: false,
        },
        false,
    ),
    Case::new(
        "pair-head-deadline",
        Kind::Delay {
            body: false,
            deadline: true,
        },
        false,
    ),
    Case::new(
        "pair-body-b-call-timeout",
        Kind::Delay {
            body: true,
            deadline: false,
        },
        false,
    ),
    Case::new(
        "pair-body-b-deadline",
        Kind::Delay {
            body: true,
            deadline: true,
        },
        false,
    ),
    Case::new(
        "pair-body-s-call-timeout",
        Kind::Delay {
            body: true,
            deadline: false,
        },
        true,
    ),
    Case::new(
        "pair-body-s-deadline",
        Kind::Delay {
            body: true,
            deadline: true,
        },
        true,
    ),
    Case::new(
        "pair-deflate-b",
        Kind::Coding {
            coding: Coding::Deflate,
            original: false,
            partial: false,
        },
        false,
    ),
    Case::new(
        "pair-deflate-s",
        Kind::Coding {
            coding: Coding::Deflate,
            original: false,
            partial: false,
        },
        true,
    ),
    Case::new(
        "pair-br-b",
        Kind::Coding {
            coding: Coding::Br,
            original: false,
            partial: false,
        },
        false,
    ),
    Case::new(
        "pair-br-s",
        Kind::Coding {
            coding: Coding::Br,
            original: false,
            partial: false,
        },
        true,
    ),
    Case::new(
        "pair-original-deflate-b",
        Kind::Coding {
            coding: Coding::Deflate,
            original: true,
            partial: false,
        },
        false,
    ),
    Case::new(
        "pair-original-deflate-s",
        Kind::Coding {
            coding: Coding::Deflate,
            original: true,
            partial: false,
        },
        true,
    ),
    Case::new(
        "pair-partial-gzip-decode-b",
        Kind::Coding {
            coding: Coding::Gzip,
            original: false,
            partial: true,
        },
        false,
    ),
    Case::new(
        "pair-partial-gzip-decode-s",
        Kind::Coding {
            coding: Coding::Gzip,
            original: false,
            partial: true,
        },
        true,
    ),
    Case::new(
        "pair-partial-gzip-original-b",
        Kind::Coding {
            coding: Coding::Gzip,
            original: true,
            partial: true,
        },
        false,
    ),
    Case::new(
        "pair-partial-gzip-original-s",
        Kind::Coding {
            coding: Coding::Gzip,
            original: true,
            partial: true,
        },
        true,
    ),
    Case::new(
        "pair-partial-deflate-decode-b",
        Kind::Coding {
            coding: Coding::Deflate,
            original: false,
            partial: true,
        },
        false,
    ),
    Case::new(
        "pair-partial-deflate-decode-s",
        Kind::Coding {
            coding: Coding::Deflate,
            original: false,
            partial: true,
        },
        true,
    ),
    Case::new(
        "pair-partial-deflate-original-b",
        Kind::Coding {
            coding: Coding::Deflate,
            original: true,
            partial: true,
        },
        false,
    ),
    Case::new(
        "pair-partial-deflate-original-s",
        Kind::Coding {
            coding: Coding::Deflate,
            original: true,
            partial: true,
        },
        true,
    ),
    Case::new("pair-passthrough-small-decoded-b", Kind::Passthrough, false),
    Case::new("pair-passthrough-small-decoded-s", Kind::Passthrough, true),
    Case::new(
        "pair-original-identity-declared-b",
        Kind::OriginalIdentity {
            declared: true,
            explicit: true,
            size: 2,
        },
        false,
    ),
    Case::new(
        "pair-original-identity-declared-s",
        Kind::OriginalIdentity {
            declared: true,
            explicit: true,
            size: 2,
        },
        true,
    ),
    Case::new(
        "pair-original-identity-observed-b",
        Kind::OriginalIdentity {
            declared: false,
            explicit: true,
            size: 2,
        },
        false,
    ),
    Case::new(
        "pair-original-identity-observed-s",
        Kind::OriginalIdentity {
            declared: false,
            explicit: true,
            size: 2,
        },
        true,
    ),
    Case::new(
        "pair-original-absent-declared-b",
        Kind::OriginalIdentity {
            declared: true,
            explicit: false,
            size: 2,
        },
        false,
    ),
    Case::new(
        "pair-original-absent-declared-s",
        Kind::OriginalIdentity {
            declared: true,
            explicit: false,
            size: 2,
        },
        true,
    ),
    Case::new(
        "pair-original-absent-observed-b",
        Kind::OriginalIdentity {
            declared: false,
            explicit: false,
            size: 2,
        },
        false,
    ),
    Case::new(
        "pair-original-absent-observed-s",
        Kind::OriginalIdentity {
            declared: false,
            explicit: false,
            size: 2,
        },
        true,
    ),
    Case::new(
        "pair-original-exact-declared-b",
        Kind::OriginalIdentity {
            declared: true,
            explicit: true,
            size: 1,
        },
        false,
    ),
    Case::new(
        "pair-original-exact-declared-s",
        Kind::OriginalIdentity {
            declared: true,
            explicit: true,
            size: 1,
        },
        true,
    ),
    Case::new(
        "pair-original-exact-observed-b",
        Kind::OriginalIdentity {
            declared: false,
            explicit: true,
            size: 1,
        },
        false,
    ),
    Case::new(
        "pair-original-exact-observed-s",
        Kind::OriginalIdentity {
            declared: false,
            explicit: true,
            size: 1,
        },
        true,
    ),
    Case::new(
        "pair-original-absent-exact-declared-b",
        Kind::OriginalIdentity {
            declared: true,
            explicit: false,
            size: 1,
        },
        false,
    ),
    Case::new(
        "pair-original-absent-exact-declared-s",
        Kind::OriginalIdentity {
            declared: true,
            explicit: false,
            size: 1,
        },
        true,
    ),
    Case::new(
        "pair-original-absent-exact-observed-b",
        Kind::OriginalIdentity {
            declared: false,
            explicit: false,
            size: 1,
        },
        false,
    ),
    Case::new(
        "pair-original-absent-exact-observed-s",
        Kind::OriginalIdentity {
            declared: false,
            explicit: false,
            size: 1,
        },
        true,
    ),
    Case::new(
        "pair-duplicates-b",
        Kind::Header(HeaderCase::Duplicate),
        false,
    ),
    Case::new(
        "pair-duplicates-s",
        Kind::Header(HeaderCase::Duplicate),
        true,
    ),
    Case::new(
        "pair-normalize-b",
        Kind::Header(HeaderCase::Normalize),
        false,
    ),
    Case::new(
        "pair-normalize-s",
        Kind::Header(HeaderCase::Normalize),
        true,
    ),
    Case::new("pair-header-bytes", Kind::Header(HeaderCase::Bytes), false),
    Case::new("pair-header-count", Kind::Header(HeaderCase::Count), false),
    Case::new("pair-header-tie", Kind::Header(HeaderCase::Tie), false),
    Case::new(
        "pair-marker-bytes-on",
        Kind::Header(HeaderCase::MarkerBytesOn),
        false,
    ),
    Case::new(
        "pair-marker-bytes-off",
        Kind::Header(HeaderCase::MarkerBytesOff),
        false,
    ),
    Case::new(
        "pair-marker-count-on",
        Kind::Header(HeaderCase::MarkerCountOn),
        false,
    ),
    Case::new(
        "pair-marker-count-off",
        Kind::Header(HeaderCase::MarkerCountOff),
        false,
    ),
    Case::new(
        "pair-marker-explicit-off",
        Kind::Marker(MarkerCase::Off),
        false,
    ),
    Case::new(
        "pair-marker-overwrite-on",
        Kind::Marker(MarkerCase::On),
        false,
    ),
    Case::new(
        "pair-marker-inherit-off",
        Kind::Marker(MarkerCase::InheritOff),
        false,
    ),
    Case::new("pair-error-head-b", Kind::Error(ErrorCase::Head), false),
    Case::new("pair-error-head-s", Kind::Error(ErrorCase::Head), true),
    Case::new("pair-error-body-b", Kind::Error(ErrorCase::Body), false),
    Case::new("pair-error-body-s", Kind::Error(ErrorCase::Body), true),
    Case::new("pair-error-gzip-b", Kind::Error(ErrorCase::Gzip), false),
    Case::new("pair-error-gzip-s", Kind::Error(ErrorCase::Gzip), true),
    Case::new(
        "pair-decoder-memory",
        Kind::Error(ErrorCase::DecoderMemory),
        false,
    ),
    Case::new(
        "pair-brotli-window",
        Kind::Error(ErrorCase::BrotliWindow),
        true,
    ),
    Case::new("pair-upload-under", Kind::Upload { cap: 4 }, false),
    Case::new("pair-upload-exact", Kind::Upload { cap: 3 }, false),
    Case::new("pair-upload-over", Kind::Upload { cap: 2 }, false),
    Case::new("pair-upload-under-s", Kind::Upload { cap: 4 }, true),
    Case::new("pair-upload-exact-s", Kind::Upload { cap: 3 }, true),
    Case::new("pair-upload-over-s", Kind::Upload { cap: 2 }, true),
    Case::new("pair-upstream-413-b", Kind::Upstream413, false),
    Case::new("pair-upstream-413-s", Kind::Upstream413, true),
    Case::new("pair-get-body", Kind::GetBody, false),
    Case::new("pair-authority", Kind::Authority, false),
    Case::new("pair-cache-bypass", Kind::Cache, false),
    Case::new("pair-connect-setting", Kind::Connect, false),
    Case::new("pair-request-normalization", Kind::RequestNormalize, false),
    Case::new("pair-205-b", Kind::Bodyless205, false),
    Case::new("pair-205-s", Kind::Bodyless205, true),
    Case::new(
        "pair-rechunk-identity",
        Kind::Rechunk {
            coding: Coding::Identity,
            original: false,
        },
        true,
    ),
    Case::new(
        "pair-rechunk-gzip",
        Kind::Rechunk {
            coding: Coding::Gzip,
            original: false,
        },
        true,
    ),
    Case::new(
        "pair-rechunk-original",
        Kind::Rechunk {
            coding: Coding::Gzip,
            original: true,
        },
        true,
    ),
    Case::new("pair-stream-live", Kind::StreamLive, true),
];

pub fn all() -> impl Iterator<Item = Case> {
    POLICY_CASES.iter().chain(EXTRA_CASES).copied()
}
pub fn find(name: &str) -> Option<Case> {
    all().find(|case| case.name == name)
}

pub const GZIP_TEN: &[u8] = &[
    31, 139, 8, 0, 0, 0, 0, 0, 0, 3, 75, 76, 132, 1, 0, 240, 205, 17, 76, 10, 0, 0, 0,
];
pub const GZIP_ELEVEN: &[u8] = &[
    31, 139, 8, 0, 0, 0, 0, 0, 0, 3, 75, 76, 132, 3, 0, 146, 93, 70, 85, 11, 0, 0, 0,
];
pub const DEFLATE_TEN: &[u8] = &[120, 156, 75, 76, 132, 1, 0, 20, 225, 3, 203];
pub const BR_TEN: &[u8] = &[27, 9, 0, 248, 37, 194, 130, 132, 0, 0];

pub fn coding_name(coding: Coding) -> &'static str {
    match coding {
        Coding::Br => "br",
        Coding::Deflate => "deflate",
        Coding::Gzip => "gzip",
        Coding::Identity => "identity",
        Coding::Passthrough => "x-p5-opaque",
    }
}

pub fn policy_payload(coding: Coding, size: u8, declared: bool) -> &'static [u8] {
    if declared {
        return b"aaaaaaaaaaa";
    }
    match (coding, size) {
        (Coding::Gzip, 10) => GZIP_TEN,
        (Coding::Gzip, _) => GZIP_ELEVEN,
        (Coding::Deflate, _) => DEFLATE_TEN,
        (Coding::Br, _) => BR_TEN,
        (_, 10) => b"aaaaaaaaaa",
        _ => b"aaaaaaaaaaa",
    }
}

#[derive(Clone, Debug)]
pub struct Reply {
    pub body: Vec<u8>,
    pub body_delay_ms: u64,
    pub close_body: bool,
    pub close_head: bool,
    pub declared: Option<usize>,
    pub fields: Vec<(&'static str, String)>,
    pub head_delay_ms: u64,
    pub status: u16,
}

impl Case {
    pub const fn new(name: &'static str, kind: Kind, streamed: bool) -> Self {
        Self {
            kind,
            name,
            streamed,
        }
    }

    pub fn cancels_response(self) -> bool {
        match self.kind {
            Kind::Policy { declared, size, .. } => declared || size > 10,
            Kind::OriginalIdentity { size: 2.., .. }
            | Kind::Delay { .. }
            | Kind::Header(
                HeaderCase::Bytes
                | HeaderCase::Count
                | HeaderCase::Tie
                | HeaderCase::MarkerBytesOn
                | HeaderCase::MarkerCountOn,
            )
            | Kind::Error(ErrorCase::BrotliWindow | ErrorCase::DecoderMemory | ErrorCase::Gzip) => {
                true
            }
            _ => false,
        }
    }

    pub fn reply(self, token: &str) -> Reply {
        let mut reply = Reply {
            body: b"ok".to_vec(),
            body_delay_ms: 0,
            close_body: false,
            close_head: false,
            declared: Some(2),
            fields: vec![
                ("date", "Tue, 27 Oct 2026 00:00:00 GMT".to_owned()),
                ("content-type", "application/octet-stream".to_owned()),
                ("x-transport-fixture", "native".to_owned()),
                ("x-request-token", token.to_owned()),
            ],
            head_delay_ms: 0,
            status: 200,
        };
        let mut coding = None;
        match self.kind {
            Kind::Policy {
                coding: choice,
                declared,
                size,
            } => {
                coding = Some(choice);
                reply.body = policy_payload(choice, size, declared).to_vec();
                reply.declared = declared.then_some(11);
            }
            Kind::Coding {
                coding: choice,
                partial,
                ..
            } => {
                coding = Some(choice);
                reply.body = policy_payload(choice, 10, false).to_vec();
                reply.declared = Some(reply.body.len());
                if partial {
                    reply.status = 206;
                    reply.fields.extend([
                        ("content-range", "bytes 0-3/8".to_owned()),
                        ("etag", "\"original\"".to_owned()),
                        ("vary", "Accept-Encoding".to_owned()),
                    ]);
                }
            }
            Kind::Rechunk { coding: choice, .. } => {
                coding = Some(choice);
                reply.body = policy_payload(choice, 10, false).to_vec();
                reply.declared = None;
            }
            Kind::Passthrough => {
                coding = Some(Coding::Passthrough);
                reply.body = b"opaque".to_vec();
                reply.declared = Some(6);
            }
            Kind::OriginalIdentity {
                declared,
                explicit,
                size,
            } => {
                coding = explicit.then_some(Coding::Identity);
                reply.body = vec![b'a'; usize::from(size)];
                reply.declared = declared.then_some(usize::from(size));
            }
            Kind::Delay { body, .. } => {
                if body {
                    reply.body_delay_ms = 500;
                } else {
                    reply.head_delay_ms = 500;
                }
            }
            Kind::StreamLive => reply.body_delay_ms = 250,
            Kind::Error(ErrorCase::Head) => reply.close_head = true,
            Kind::Error(ErrorCase::Body) => {
                reply.declared = Some(4);
                reply.close_body = true;
            }
            Kind::Error(ErrorCase::Gzip) => {
                coding = Some(Coding::Gzip);
                reply.body = b"not-a-gzip".to_vec();
                reply.declared = Some(reply.body.len());
            }
            Kind::Error(ErrorCase::DecoderMemory) => {
                coding = Some(Coding::Gzip);
                reply.body = GZIP_TEN.to_vec();
                reply.declared = Some(reply.body.len());
            }
            Kind::Error(ErrorCase::BrotliWindow) => {
                coding = Some(Coding::Br);
                reply.body = BR_TEN.to_vec();
                reply.declared = Some(reply.body.len());
            }
            Kind::Header(HeaderCase::Duplicate) => reply.fields.extend([
                ("set-cookie", "a=1".to_owned()),
                ("set-cookie", "b=2".to_owned()),
            ]),
            Kind::Header(HeaderCase::Normalize) => reply.fields.extend([
                ("connection", "x-private, x-edgezero-proxy".to_owned()),
                ("x-private", "removed".to_owned()),
                ("x-edgezero-proxy", "upstream".to_owned()),
                ("proxy-connection", "keep-alive".to_owned()),
            ]),
            Kind::Marker(_) => reply
                .fields
                .push(("x-edgezero-proxy", "upstream".to_owned())),
            Kind::Upstream413 => reply.status = 413,
            Kind::Bodyless205 => {
                reply.status = 205;
                reply.body.clear();
                reply.declared = Some(0);
            }
            Kind::Authority
            | Kind::Cache
            | Kind::Connect
            | Kind::GetBody
            | Kind::Header(_)
            | Kind::RequestNormalize
            | Kind::Upload { .. } => {}
        }
        if let Some(choice) = coding {
            reply
                .fields
                .push(("content-encoding", coding_name(choice).to_owned()));
        }
        reply
    }

    pub fn request_body(self) -> &'static str {
        match self.kind {
            Kind::GetBody => "x",
            Kind::Upload { .. } | Kind::Upstream413 => "abc",
            _ => "",
        }
    }

    pub fn request_method(self) -> &'static str {
        if matches!(self.kind, Kind::Upload { .. } | Kind::Upstream413) {
            "POST"
        } else {
            "GET"
        }
    }

    pub fn expects_origin(self, adapter: &str) -> bool {
        !(matches!(self.kind, Kind::GetBody | Kind::Upload { cap: 0..=2 })
            || self.kind == Kind::Authority && adapter == "spin")
    }

    pub fn client_marker(self) -> bool {
        !matches!(
            self.kind,
            Kind::Marker(MarkerCase::On | MarkerCase::InheritOff)
                | Kind::Header(HeaderCase::MarkerBytesOff | HeaderCase::MarkerCountOff)
        )
    }
}
