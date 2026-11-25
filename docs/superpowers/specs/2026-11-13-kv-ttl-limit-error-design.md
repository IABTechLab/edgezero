# Distinguishable KV TTL limit errors

## Problem and scope

Issue #323 reports that `KvHandle` rejects retention windows longer than 365
days with a generic validation error. Named handles returned by `KvRegistry`
have the same restriction. Applications cannot distinguish this rejection from
other invalid inputs without inspecting error text.

This fix implements the issue's explicitly permitted structured-error option.
It does not enable multi-year retention or add configurable limits.

## Contract

- Add `KvError::TtlLimitExceeded { requested: Duration, maximum: Duration }`.
- Both JSON and raw-byte TTL writes return this error when the requested TTL
  exceeds `KvHandle::MAX_TTL`, before invoking the backend.
- `maximum` identifies the handle's current portable policy: exactly 365 days.
  This is not a statement of each provider's maximum capability.
- TTLs below 60 seconds continue returning `KvError::Validation`. Both
  endpoints are inclusive. Excessive durations, including `Duration::MAX`,
  remain rejected before time arithmetic or provider conversion.
- Converting the new error into `EdgeError` preserves HTTP 400.
- The error applies equally to directly constructed, default, and named
  handles because registry lookup returns the same handle type.

## Alternatives and provider behavior

A configurable handle cap would enable longer retention on some backends but
needs a provider capability contract first. The pinned Fastly SDK converts TTL
seconds to `u32`, falling back to zero when conversion overflows; an arbitrary
override would expose that conversion. Cloudflare receives whole seconds, and
Axum checks addition to its system clock. Spin does not support TTL writes:
requests passing handle validation continue returning `KvError::Unsupported`
without writing. These adapter behaviors remain unchanged.

Keeping the cap with a structured error is the smallest correction supported
by the issue. `KvError` is already non-exhaustive, so consumers must allow for
additional variants. Consumers explicitly matching `Validation` for excessive
TTLs must update their handling.

## Downstream migration

Applications may replace unconditional warning-and-clamp code with a match on
`TtlLimitExceeded` and an explicit retention-policy decision. They must propagate
the rejection or deliberately choose an allowed retention window and retry;
silently shortening retention is not required. Removing a clamp alone does not
make multi-year retention succeed. Applications requiring full multi-year
retention still need a future provider-aware extension or a different storage
path. Ordinary backend failures retain their existing handling.

## Verification

Exercise actual named registry handles with both typed and raw writes. Check
the exact minimum and maximum, one nanosecond below and above the bounds,
two-year retention, and `Duration::MAX`. Inspect exact error fields, verify no
backend invocation for rejected TTLs, and verify accepted values round-trip.
Check the HTTP status mapping. Demonstrate that the regression rejects the
previous generic error, then run core and workspace tests, formatting, clippy,
feature compilation, adapter WASM checks, and documentation checks.
