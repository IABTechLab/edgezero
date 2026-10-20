# Adapter-owned CLI behavior and local-state correctness

## Approved direction

PR #287 is conflict-free and its checks pass at `334dd8e9`, but the deep
review reproduced secret disclosure/corruption, adapter selection errors,
dry-run divergence, and a broken nested-lock lifetime. The user approved
bounded reimplementation and explicitly requires every adapter to own its CLI
behavior: `edgezero-cli` must not switch on adapter names or provider types.

## Ownership

- `edgezero-cli` parses common commands, loads the application manifest and
  typed configuration, resolves the selected registered adapter, applies shared
  safety/validation, and reports outcomes.
- Each adapter owns native commands, provider flags and validation, default
  manifest names, local output paths, serve environment inputs, and provider
  writeback. Scaffold tool pins, install hints, logging templates, and command
  templates belong to its blueprint.
- `edgezero-adapter` supplies provider-neutral traits and reusable filesystem,
  environment-record, and execution helpers. It must not depend on
  `edgezero-core` or a provider enum.
- The core manifest parser validates structure; provider field ownership is
  validated through the registered adapter. Targeted commands validate their
  selected adapter; whole-project `config validate` validates all adapters.

Names may remain in test fixtures, dependency declarations, and adapter-owned
metadata. Production orchestration must not compare adapter identities to
choose behavior. Existing common CLI flags remain stable and adapter methods
decide whether their requested semantics are supported.

## Adapter-owned local layout

Add a pure registry hook returning a local layout from neutral, declared
manifest/crate/component inputs. Paths are project-relative; no hook performs
I/O. The layout contains the manifest path, provision artifacts, and an optional
serve environment-file path. Each artifact declares its content kind (`Text`
or `Env`) so generic reporting does not recognize provider filenames.

The default hook describes the declared manifest without sidecars. Built-in
adapters supply their own defaults and artifacts. A third-party adapter can
return an unusual manifest, sidecar, and environment filename without changing
the CLI.

An absent manifest is represented by `None`. Duplicate artifact paths must have
consistent content kinds. The optional serve env input receives the same
original-tree validation as output artifacts, even when it is not an output.

CLI orchestration validates every returned path in the original tree before
staging: relative paths, no traversal/drive prefix, project containment, and no
existing symlink components. The same artifact list drives pristine snapshots
and dry-run diffs. The serve wrapper reads only the adapter-declared env input,
then uses common parent/overlay/default precedence.

Remove provider switches in `provision.rs`, `lib.rs`, and `generator.rs`.
Blueprint logging endpoints use the existing template expansion vocabulary;
blueprints also provide their tool pins and installation notes. Unsupported
config-read consent reports the selected adapter's supplied reason rather than
assuming a particular cloud provider.

## One local provisioning flow

Resolve one selected-adapter operation context, then share these ordered steps
between real/dry-run and typed/untyped calls:

1. Resolve and preflight declared paths and adapter-owned artifacts.
2. Synthesize missing baseline bytes.
3. Validate the selected adapter and resolve stores.
4. Run base provision.
5. On success, run optional typed provision.
6. Preserve partial outcomes and return the original error when present.

Real and staged wrappers choose the filesystem root and reporting; they do not
reimplement provider policy. Cloud provisioning remains a distinct dispatch
mode and does not enter local bootstrap. Existing adapter writers and their
idempotency/comment-preservation behavior are retained.

## Lossless environment records

Implement one record scanner in `edgezero-adapter::env_file`. Records preserve
raw bytes, line terminators, and assignment spans. Active single-, double-, and
backtick-quoted values can span lines. Assignment-like lines and apparent
headers inside such a value are data, never separate records.

Writer dedup/upsert, CLI overlay reading, and dry-run redaction consume these
records. Untouched records remain byte-identical, including CRLF and missing
final newlines. Writers reject unterminated active quotes before mutation.
Readers reject invalid environment keys/NUL bytes and never evaluate shell
expressions. Escape decoding follows an explicitly tested dotenv grammar.

Single-hash commented assignments retain placeholder-dedup semantics, but a
commented quote does not consume later active assignments. Redaction may
conservatively hide suspected stashed secret continuations. Backtick and
unterminated values must never expose continuation bytes in a preview.

## Lock and subprocess lifetime

Remove the deploy-wide project lock and its environment-advertised token,
depth, and sibling borrowing protocol. It cannot preserve exclusion when an
ancestor exits while descendants continue, and arbitrary command composition
must not hold a non-reentrant write transaction around nested CLI operations.

Provision and config writes retain an ordinary exclusive project lock for
their Rust-owned read/modify/write transaction. No protected local write may be
delegated to a child whose lifetime can exceed that lock.
These guarantees concern cooperating Rust-owned file transactions. Native
emulator databases retain their native storage locking; their adapter must
validate the persistence boundary before every initializing read or write.

The Fastly adapter owns isolation for native source deployment: snapshot its
selected manifest, run the vendor against a private manifest, and merge only
the resulting service identifier under the adapter's existing `ManifestLock`.
Keep original source-relative package/input semantics while preventing vendor
startup or writeback from rewriting the source manifest. A private startup
directory avoids Fastly's pre-argument base-manifest rewrite; a unique private
environment manifest can retain the original project directory for relative
inputs. Adapter code owns and validates the native directory/environment args.
Cleanup must not delete operator files. Immutable release deployment remains
read-only and creates no lock state inside the release.

Take the snapshot and merge in separate short manifest-lock transactions. At
merge time re-read the current document. Update only `service_id`, and only if
the current identifier equals the snapshot identifier or already equals the
validated vendor result. Otherwise report a same-field conflict without
overwriting the source. Preserve all unrelated concurrent edits.

Operator-authored shell overrides are composition points. They run outside a
project transaction; nested EdgeZero writers acquire their own ordinary locks.
Direct arbitrary file writes in such a shell are outside the cooperating
writer contract. This limitation must be documented explicitly.

## Provider execution and local persistence

Fastly managed subprocesses and API credential resolution use the same
resolved execution environment. Applying environment overrides must preserve
the adapter-selected command working directory.
Carry this context through deploy preflight and finalization hooks as well as
managed planning/native command helpers; finalization must not fall back to an
ambient-only credential lookup.

Cloudflare validates its own local persistence path before both local reads and
writes: a read can initialize Miniflare databases. This policy belongs in the
Cloudflare adapter, not a generic CLI provider branch.

## Verification

Regression coverage must reproduce the reviewed failures, not merely check
metadata declarations:

- No multiline secret sentinel appears in a real dry-run report for any quote
  delimiter, including backticks.
- Re-provision preserves a multiline secret containing a managed-looking
  assignment and updates the actual outer assignment exactly once.
- Quoted inner keys/headers do not suppress real appended keys/headers.
- A selected adapter succeeds when an unselected adapter has different valid
  layout or typed namespace requirements; whole-project validation still
  reports actual incompatibilities.
- Derived symlink targets are rejected identically before real/staged work.
- Cancelling a composing deploy cannot bypass a surviving leaf writer's lock;
  vendor writes after parent termination affect only private manifests.
  Use explicit process barriers and terminate only the composing ancestor.
- A fake vendor writes at startup before reading args and after parent exit;
  both the selected source manifest and any sibling `fastly.toml` stay intact.
  Same-field service-id changes conflict; unrelated edits survive a merge.
- Local Cloudflare reads reject redirected persistence before spawning vendor
  code. Managed Fastly commands receive the configured child environment.
- A registered fifth test adapter supplies an unusual artifact/env path and
  blueprint data (tool pins, hints, logging template) and works without
  production CLI changes.

Run scoped tests after code changes, then workspace tests, formatting, strict
Clippy, combined-feature compilation, adapter WASM checks, generated-project
compilation, docs checks, and fake-provider deployment smoke tests. Preserve
existing user changes in other worktrees. Do not merge the PR or post review
comments as part of implementation.

## Implementation decisions and review

The implementation adds neutral local-layout metadata and moves tool pins,
install notes, ignore patterns, and logging expansion policy to adapter
blueprints. Targeted validation is selected-adapter policy; core validates only
manifest structure. A common local transaction handles staged and real writes.

The initial handwritten env scanner was replaced after differential review
found valid Wrangler forms with escaped quotes, colon separators, and Unicode
whitespace. The shared lossless records use the dotenv grammar and a literal
serializer. Read-only review compared 629 parser inputs and 2,696 serialized
values with the installed dotenv consumer; no mismatches remained. Nine direct
writer/redactor probes cover multiline records and all line endings.

Fastly private environment selection is confined to native deploy. Upstream
Fastly build temporarily replaces its source manifest when an environment is
selected, so native serve retains the standard manifest build path and rejects
custom basenames explicitly. Managed deployments instead retain an external
startup directory with a manifest copy, protecting immutable releases from
startup upgrades. Resolved execution environment also reaches curl transport.

Read-only proposal/review agents covered adapter boundaries, lock lifetime,
env records, and the implementation plan. Review found and corrected unsafe
serve layout resolution, missing crate/component metadata, rollback policy in
the CLI, env grammar differences, typed-error checkpoint loss, native serve
writeback, immutable startup writes, and API child-environment propagation.
The chosen design keeps adapter policy in adapters and removes the nested lock
protocol rather than extending its ownership tokens.
