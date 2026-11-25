# Safe app-config validation diagnostics

Issue: [#457](https://github.com/IABTechLab/edgezero/issues/457).

## Contract

Typed app-config validation may preserve explicitly approved operator diagnostics without retaining validator messages, parameters, or resolved values. A root-owned static registry maps structural path patterns and error codes to fixed explanations. `AppConfigMeta` defaults to no approved diagnostics; an optional `#[app_config(diagnostics = REGISTRY)]` derive attribute enables the registry. Paths use named field segments and array wildcard segments, matched against the validation tree, rather than arbitrary string interpolation.

Only approved names, codes, explanations, and numeric list positions enter diagnostics. Unknown validator keys are redacted even if they resemble identifiers. Generic failures continue to fail closed. Redacting previously echoed, unapproved field names is an intentional security adjustment.

Bound registry inspection to 64 entries, output to 8 diagnostics, codes to 64 bytes, single-line explanations to 512 bytes, paths to 256 bytes, traversal to 32 levels and 1,024 visited nodes. Invalid declarations are ignored. Exhaustion never converts validation failure to success. Do not truncate explanations into a different meaning.

Attach approved diagnostics to a new opt-in `EdgeError` variant, preserving existing `ConfigOutOfDate` constructors. Both variants render HTTP 503, kind `config_out_of_date`, and `Retry-After: 60`. Public messages remain coarse; an explicit operator accessor provides approved details to logging. Applications opting in must account for the additional variant or use the shared diagnostic/classification accessor.

CLI validate/push/diff use the same sanitizer after existing secret-validator pruning. Runtime validates resolved secrets; CLI validates key names and skips secret-field validators. CLI success does not certify a differently compiled guest. A diagnostic for a missing compiled implementation may explain its required Cargo feature and instruct rebuilding both CLI and serving guest.

The existing validation result is inspected once; do not read the store again, parse the complete policy again, or run a second validation. Existing extraction/deadline/resource behavior is preserved. Adapter production behavior remains shared through core.

## Alternatives

A callback could construct explanations from resolved values and permits accidental disclosures. Static declarations provide a smaller trust boundary. Adding fields to `ConfigOutOfDate` breaks existing constructors; the new variant limits migration to applications opting in. Complete schema derivation solely to preserve all old field names would broaden macro scope substantially; unknown paths instead remain redacted.

## Evidence required

Tests must cover approved semantic refusal, compiled-availability success control, ordinary and nested/list validation, hostile keys/messages/parameters, resolved-secret and rejected-URL canaries, bounds, and the single-read invariant. Verify CLI entry points and logger/public response separation. Exercise affected adapter contracts with coarse 503 responses and report native tests separately from actual WASM runner execution.
