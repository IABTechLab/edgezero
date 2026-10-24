# Portable Outbound Policies Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the portable request/response policies and redirect contract needed for adoption while retaining all existing outbound bounds and failure invariants.

**Architecture:** Refactor only the dispatch/response-policy boundary that must become head-first. Keep normalization, provenance, decoding, and limits shared; put redirect chain policy and scripted test utilities in focused sibling modules under `outbound/`. Adapters continue owning network I/O, clocks, cancellation, and transport resources.

**Tech Stack:** `edgezero-core`, existing `http` re-exports, pinned `url`, `futures`, existing compression helpers, opt-in `test-utils`.

---

Dependencies: G1 scope decisions, G3 scheduling map, and G4's recorded API approval. G2 gates the dependent Fastly engine.
Paths below are relative to the worktree root. All new public names are proposed, with no aliases
for superseded implementer signatures. Update every in-tree implementation in the same buildable
series; phase 3 supplies adapter integration, not a releasable default-stub period.

## Interfaces: Proposed Hard-Cut Contract

G4 owns approval of this single interface contract before P1 changes runtime code. The following
are signature declarations, not claimed existing or completed implementations. Supporting types
are owned by core `outbound`, except the explicitly Fastly-local observation types below;
their fields are private unless shown public. Do not let individual
adapters invent competing head/context/pending/result types. These signatures preserve local
`!Send` futures and `OutboundHttpClient: Send + Sync` without unsafe wrappers.

```rust,ignore
pub type OutboundFuture<T> =
    futures_util::future::LocalBoxFuture<'static, Result<T, EdgeError>>;

pub enum OutboundSendMode {
    Batch,
    Begin,
    Follow,
    Send,
}

pub trait OutboundHttpClient: Send + Sync {
    fn time(&self) -> OutboundTime;
    fn validate_supported_options(&self, request: &OutboundRequest,
        mode: OutboundSendMode) -> Result<(), EdgeError>;
    fn dispatch_head(&self, request: OutboundDispatchRequest)
        -> OutboundFuture<OutboundHead>;
    fn begin(&self, request: OutboundDispatchRequest)
        -> Result<OutboundPending, EdgeError>;
    fn start_batch_until(&self, batch: OutboundPreparedBatch) -> OutboundBatch;
}

impl HttpClient {
    pub fn new(client: std::sync::Arc<dyn OutboundHttpClient>) -> Self;
    pub fn with_client<Client>(client: Client) -> Self
    where Client: OutboundHttpClient + 'static;
    pub fn send(&self, request: OutboundRequest) -> OutboundFuture<OutboundResponse>;
    pub fn begin(&self, request: OutboundRequest) -> OutboundFuture<OutboundPending>;
    pub fn start_batch_until(&self, requests: Vec<OutboundRequest>, cutoff: Deadline)
        -> OutboundBatch;
    pub fn send_all_until(&self, requests: Vec<OutboundRequest>, cutoff: Deadline)
        -> futures_util::future::LocalBoxFuture<'static,
            Result<OutboundBatchResults, OutboundBatchFailure>>;
    pub fn with_proxy_marker(self, enabled: bool) -> Self;
    pub fn proxy_marker_enabled(&self) -> bool;
    pub fn effective_proxy_marker_enabled(&self, request: &OutboundRequest) -> bool;
}
```

There is one network primitive, `dispatch_head`, plus mandatory eager scheduling and non-I/O
preparation hooks, not three transports. `HttpClient::send` and explicit redirect following use the same primitive and
head finalizer; adapter begin/batch hooks schedule that same pipeline before returning. No trait
default implements eager work as unpolled futures. Remove the old implementer-facing `send`
requirement in the atomic migration; application-facing `HttpClient::send` remains.
`new`/`with_client` retain these signatures and obtain the paired time source through the required
trait hook. They do not create another transport, clock or inert timer. Client marker configuration
belongs to the facade, with default on; effective inspection combines it with the request's override
without polling, validating unrelated options or performing I/O.

### Request Options and Readback

```rust,ignore
pub enum ResponseDecoding { Decode, Original }
pub enum OutboundProxyMarker { Inherit, On, Off }
pub enum OutboundImageFormat { Auto, Avif, Gif, Jpeg, JpegXl, Mp4, Png, WebP }
pub enum OutboundResizeFilter { Nearest, Bilinear, Bicubic, Lanczos2, Lanczos3 }
pub enum OutboundCropFocus { Default, Smart, Percent { x: u8, y: u8 } }
pub enum OutboundImageOptimizerRegion {
    Asia, Australia, EuCentral, EuWest, UsCentral, UsEast, UsWest,
}

impl OutboundRequest {
    pub fn connect_timeout(self, timeout: Duration) -> Self;
    pub fn connect_timeout_setting(&self) -> Option<Duration>;
    pub fn response_decoding(self, choice: ResponseDecoding) -> Self;
    pub fn response_decoding_setting(&self) -> ResponseDecoding;
    pub fn proxy_marker(self, setting: OutboundProxyMarker) -> Self;
    pub fn proxy_marker_setting(&self) -> OutboundProxyMarker;
    pub fn image_optimizer(self, options: OutboundImageOptimizerOptions) -> Self;
    pub fn image_optimizer_options(&self) -> Option<&OutboundImageOptimizerOptions>;
}

impl OutboundImageOptimizerRegion {
    pub fn parse(token: &str) -> Result<Self, EdgeError>;
    pub fn as_str(&self) -> &str;
}

impl OutboundImageOptimizerOptions {
    pub fn new() -> Self;
    pub fn region(self, region: OutboundImageOptimizerRegion) -> Self;
    pub fn region_setting(&self) -> Option<&OutboundImageOptimizerRegion>;
    pub fn preserve_origin_query(self, preserve: bool) -> Self;
    pub fn preserve_origin_query_setting(&self) -> bool;
    pub fn format(self, format: OutboundImageFormat) -> Self;
    pub fn format_setting(&self) -> Option<OutboundImageFormat>;
    pub fn quality(self, quality: u8) -> Result<Self, EdgeError>;
    pub fn quality_setting(&self) -> Option<u8>;
    pub fn resize_filter(self, filter: OutboundResizeFilter) -> Self;
    pub fn resize_filter_setting(&self) -> Option<OutboundResizeFilter>;
    pub fn width(self, width: u32) -> Result<Self, EdgeError>;
    pub fn width_setting(&self) -> Option<u32>;
    pub fn height(self, height: u32) -> Result<Self, EdgeError>;
    pub fn height_setting(&self) -> Option<u32>;
    pub fn crop(self, width: u32, height: u32, focus: OutboundCropFocus)
        -> Result<Self, EdgeError>;
    pub fn crop_setting(&self) -> Option<(u32, u32, OutboundCropFocus)>;
}
```

The selector enums, including region, are Copy/Clone/Debug/PartialEq/Eq values. Region supports
direct typed construction and exhaustive matching; `parse` and `as_str` convert exactly the pinned
SDK's seven region tokens without making core depend on that SDK. Only options have private
fields and support Default/Clone/Debug/equality; `new` leaves selectors unset and query
preservation false. Quality above 100, zero dimensions/crop ratio and percent focus above 100 fail
at construction as typed client errors. Paired percentage focus makes incomplete offsets and
smart-plus-offset combinations unrepresentable. Core validates public enum payloads at
the options builder, not after provider dispatch. No implicit region or format is invented.

Request defaults are no connect timeout, Decode, Inherit and no optimizer. Zero connect timeout
is retained as invalid input and rejected at ordinary preflight under EZ-4.5; unsupported options
still win first. Preserve all settings through parts round-trips and prepared redirect requests.
The client resolves Inherit into the effective on/off value before every dispatch, including
supplied clients. Its effective inspector, not the request's raw Inherit getter alone, satisfies EZ-7.7.

### Explicit Redirect Following

```rust,ignore
impl OutboundRedirectLimit {
    pub fn new(limit: usize) -> Result<Self, EdgeError>;
    pub fn get(&self) -> usize;
}

impl<'head> OutboundRedirectContext<'head> {
    pub fn redirect_number(&self) -> usize;
    pub fn status(&self) -> StatusCode;
    pub fn request_uri(&self) -> &Uri;
    pub fn resolved_target(&self) -> &str;
    pub fn headers(&self) -> &HeaderMap;
    pub fn is_cross_origin(&self) -> bool;
    pub fn prepared_request(&self) -> Option<&OutboundRequest>;
    pub fn take_prepared_request(&mut self) -> Option<OutboundRequest>;
}

pub enum OutboundRedirectAction { FollowPrepared, Follow(OutboundRequest), Stop }
pub enum OutboundRedirectStopReason {
    NoLocation, MethodNotRewritable, BodyNotReplayable, LimitReached, StoppedByPolicy,
}
pub enum OutboundFollowFailure<E> {
    Hop { index: usize, error: EdgeError },
    Policy { redirect_number: usize, error: E },
}

impl OutboundFollowResult {
    pub fn response(&self) -> &OutboundResponse;
    pub fn final_uri(&self) -> &Uri;
    pub fn redirects_followed(&self) -> usize;
    pub fn stop_reason(&self) -> Option<OutboundRedirectStopReason>;
    pub fn into_parts(self)
        -> (OutboundResponse, Uri, usize, Option<OutboundRedirectStopReason>);
}

impl HttpClient {
    pub fn follow<P, E>(&self, request: OutboundRequest,
        limit: OutboundRedirectLimit, policy: P)
        -> futures_util::future::LocalBoxFuture<'static,
            Result<OutboundFollowResult, OutboundFollowFailure<E>>>
    where
        P: for<'head> FnMut(&mut OutboundRedirectContext<'head>)
            -> Result<OutboundRedirectAction, E> + 'static,
        E: 'static;
}
```

Limit, context and success types have private fields. Limit accepts 1..=20 with no Default;
success/context cannot be publicly reconstructed. The synchronous trusted policy may own Rc and
return an arbitrary non-Clone, non-Send application error unchanged; neither P nor E requires
Send/Sync/Debug/Error. Context borrows the answering head and owns its optional prepared request
for that policy call only. Redirect numbers start at one, hop indices at zero. `FollowPrepared`
uses the remaining prepared request; `Follow(request)` transfers a modified prepared request or
a complete fresh replacement. Taking the prepared request and then selecting FollowPrepared is
a protocol failure, not a second use or a hidden copy.

The policy runs only at EZ-9.18's prescribed stage and never for the initial request. Invalid
Location evidence/references, user information or unusable HTTP(S) targets fail as typed 502
Protocol at the answering hop. A valid non-HTTP target is exposed as a resolved string with no
prepared request; Stop/rejection remain possible, while FollowPrepared fails as 502 Protocol.
Errors in a request the policy returns are unchanged hop errors, not reclassified URL failures.
No policy rejection value is converted to EdgeError or exposed on the wire by this API.

Each hop uses the same client's Follow-mode head-first pipeline and ordinary option validation.
Preparation performs exactly the specified method/body-header/cross-origin credential rules;
policy-authorized replacement credentials are not stripped again. Capture the initial request's
absolute deadline independently, not its per-call timeout deadline. That bound survives every
replacement through final-body consumption. Each hop captures entry before its own shared
preparation and keeps that request's per-call timeout; policy time counts against the original
absolute chain deadline, not an invented chain-wide per-call timeout. A synchronous policy cannot
be preempted: preserve Err(E) unchanged even if it consumed the remaining time; after Ok(action),
recheck the original chain deadline before dispatch or finalization. Unsupported-option validation
still precedes budget rejection for a selected next request. No expired chain returns a late Ok.
Followed heads are discarded without polling;
final heads receive ordinary finalization. At most limit+1 requests are sent. Success reports the
actual final dispatched URI and a stop reason exactly for a followable final status. Dropping the
owned future cancels the current exchange and prevents later hops; no second transport is used.

### Dispatch and Response Context

The facade obtains `time()` and samples its clock synchronously at method invocation, before
returning its future. It then calls `validate_supported_options` before ordinary validation,
budget rejection, timer creation or body polling. This required synchronous hook checks the
adapter's support and mode combinations without network I/O, backend registration, body reads,
or runtime task creation. Unsupported Spin authority/Image Optimizer settings therefore reject
even if a body never becomes ready. Supplied clients use the same mode contract. Adapter dispatch
may defensively recheck, but a late-only boundary check is not sufficient.

The Image Optimizer support matrix is explicit; `Follow` performs the same single-send
primitive on every permitted redirect hop, not the eager begin/batch operation:

| Adapter                | Send                                                 | Follow                          | Begin       | Batch       |
| ---------------------- | ---------------------------------------------------- | ------------------------------- | ----------- | ----------- |
| Fastly                 | Supported subject to EZ-8's entitlement/horizon gate | Same mapping and checks as Send | Unsupported | Unsupported |
| Axum, Cloudflare, Spin | Unsupported                                          | Unsupported                     | Unsupported | Unsupported |

Validate method/cache combinations and target policy on each prepared hop, preserving the
immutable original chain deadline. Do not reject Fastly `Follow` merely because it is not `Send`.

After supported-option validation, shared preparation computes the selected budget
once, resolves only permitted GET/HEAD streams, and calls the adapter with `OutboundDispatchRequest`.
Settlement, scheduling and redirect policy time never restart that budget. Transport uses remaining
time to the captured absolute limit, not a newly started `budget.duration`.

```rust,ignore
impl OutboundTime {
    pub fn new(
        clock: MonotonicClock,
        wait: impl Fn(Deadline, MonotonicClock) -> OutboundFuture<()>
            + Send + Sync + 'static,
    ) -> Self;
    pub fn clock(&self) -> &MonotonicClock;
    pub fn wait_until(&self, deadline: Deadline) -> OutboundFuture<()>;
}

impl OutboundDispatchRequest {
    pub fn entered_at(&self) -> MonotonicInstant;
    pub fn budget(&self) -> DispatchBudget;
    pub fn clock(&self) -> &MonotonicClock;
    pub fn time(&self) -> &OutboundTime;
    pub fn request(&self) -> &OutboundRequest;
    pub fn into_parts(self) -> (OutboundRequestParts, OutboundResponseContext);
}

impl OutboundResponseContext {
    pub fn entered_at(&self) -> MonotonicInstant;
    pub fn budget(&self) -> DispatchBudget;
    pub fn clock(&self) -> &MonotonicClock;
    pub fn time(&self) -> &OutboundTime;
    pub fn mode(&self) -> OutboundSendMode;
    pub fn request_method(&self) -> &Method;
    pub fn response_body_policy(&self) -> &ResponseBodyPolicy;
    pub fn original_chain_deadline(&self) -> Option<Deadline>;
}

impl OutboundPreparedBatch {
    pub fn slot_count(&self) -> usize;
    pub fn started_at(&self) -> MonotonicInstant;
    pub fn observation_cutoff(&self) -> Deadline;
    pub fn clock(&self) -> &MonotonicClock;
    pub fn time(&self) -> &OutboundTime;
    pub fn into_slots(self)
        -> (OutboundBatchContext, Vec<(usize, OutboundDispatchRequest)>);
}

impl OutboundBatchContext {
    pub fn slot_count(&self) -> usize;
    pub fn started_at(&self) -> MonotonicInstant;
    pub fn observation_cutoff(&self) -> Deadline;
    pub fn clock(&self) -> &MonotonicClock;
    pub fn time(&self) -> &OutboundTime;
}

impl OutboundHead {
    pub fn new(
        status: StatusCode,
        headers: OutboundHeaderInput,
        encoded: BodyStream,
        context: OutboundResponseContext,
        owner: OutboundExchangeOwner,
    ) -> Result<Self, EdgeError>;
    pub fn status(&self) -> StatusCode;
    pub fn headers(&self) -> &HeaderMap;
    pub fn location_evidence(&self) -> &OutboundLocationEvidence;
    pub fn finish(self) -> OutboundFuture<OutboundResponse>;
    pub fn discard(self);
}

impl OutboundHeaderInput {
    pub fn new(adapter: &'static str, headers: HeaderMap,
        fidelity: OutboundHeaderFidelity) -> Self;
}

pub enum OutboundHeaderFidelity {
    AdapterVisibleFields,
    ProviderFieldBoundariesUnknown,
}

impl OutboundLocationEvidence {
    pub fn observed_count(&self) -> usize;
    pub fn has_non_text(&self) -> bool;
    pub fn retained_singleton(&self) -> bool;
    pub fn fidelity(&self) -> OutboundHeaderFidelity;
    pub fn is_missing_or_empty(&self) -> bool;
}

impl OutboundExchangeOwner {
    pub fn new(release: impl FnOnce() + 'static) -> Self;
}

impl OutboundPending {
    pub fn new(already_scheduled: OutboundFuture<OutboundResponse>,
        owner: OutboundExchangeOwner) -> Self;
    pub fn is_ready(&mut self) -> bool;
    pub fn collect(self) -> OutboundFuture<OutboundResponse>;
    pub fn cancel(self);
}
```

Prepared-batch indices are original input indices. `into_slots` transfers eligible requests
with explicit indices and one opaque batch context. That context privately owns original slot
count, entry instant, paired time source, cutoff, per-slot selected deadline/source metadata and
exact preflight outcomes with their terminal observations. Producers must not enumerate the
filtered eligible vector as if its positions were original indices or re-emit preflight errors.
The facade constructs validated contexts; implementations
read them, not manufacture a new start time. `OutboundResponseContext` owns the selected budget,
paired time source, mode, complete effective response/header policies and immutable original chain deadline.
It also carries the local progress-scope token; it never borrows a request/client stack frame.
It has private fields, is not Clone, and has no public constructor, setters or destructive
decomposition. Adapters may inspect it and pass it unchanged into `OutboundHead::new`; core
retains the complete effective header/marker/decoding policies and scope token privately. The
read-only body-policy view uses P1's effective policy, not defaults reconstructed by the adapter.
`clock()` is the clock inside the captured `time()`. Neither getter permits restarting a budget
or changing the immutable original chain deadline. Prepared batches likewise have no public
constructor; supplied clients receive validated inputs from the facade.
`OutboundBatchContext` likewise has private fields with no public constructor, Clone, Default,
setters or destructive decomposition. It carries terminal metadata, not a duplicate response
context or SDK cleanup owner. Batch constructors consume it once and seed the preflight ledger.

`OutboundTime` is cloneable and Send + Sync: it owns the existing MonotonicClock and an Arc-backed
timer factory. The factory receives that same clock, creates an owned local future, and uses the
adapter's existing timer facility; core adds no executor or Tokio dependency. The facade races
settlement against `wait_until` for the selected outbound deadline while the inbound stream keeps
its own read-deadline enforcement. Compare both absolute limits in the same application domain;
preserve EZ-1's 408/504 precedence and original typed source errors. A timer or executor setup
failure is typed Internal before body polling or transport I/O, never a panic or silent unbounded read.
Create and initially poll the timer before the first source poll so missing runtime/setup is caught
without reading the body. A later operational timer failure terminates the race with its typed error.
Timer completion is a wakeup to recheck the paired clock, not proof of expiry: early wakeups must
re-arm with a cooperative scheduling opportunity, not spin or extend the captured deadline.
Drop cancels the pending timer registration and releases its resources.
Only the shared factory and captured factory state must be Send + Sync; the returned timer future
is local and may own Rc state. A fake shared waiter registry therefore needs Send + Sync storage
(for example Arc/Mutex), while an exchange owner may capture Rc directly. This intentional split
does not make local futures Send or require weakening the client's Send + Sync contract.

Each production adapter supplies the existing app clock plus its own deadline-wakeup factory.
Hard-migrate clock-only constructors and update request-context injection, manual clients,
fixtures, demo and generator together:

| Existing Constructor                                                           | Required Replacement                                                  |
| ------------------------------------------------------------------------------ | --------------------------------------------------------------------- |
| Axum `try_with_clock(MonotonicClock)`                                          | `try_with_time(OutboundTime) -> Result<Self, reqwest::Error>`         |
| Axum internal `with_transport_and_clock(Arc<reqwest::Client>, MonotonicClock)` | `with_transport_and_time(Arc<reqwest::Client>, OutboundTime) -> Self` |
| Fastly/Cloudflare/Spin `with_clock(MonotonicClock)`                            | `with_time(OutboundTime) -> Self`                                     |

Native `try_new()` remains fallible; other adapters' `new()` remain infallible. These default
constructors pair the process-default clock with the real adapter timer, never an inert timer.
`HttpClient::new`/`with_client` obtain this pair from the required trait hook, not a hidden second
default clock. Scripted clients install a fake clock AND wakeable timer queue; advancing fake time
wakes waiters without network or wall-clock sleeps. Source and timer pairing is an explicit
implementer obligation with conformance tests, not something MonotonicClock alone can provide.
Fastly's uninterruptible inbound-read exception in EZ-1.5 remains BestEffort; this interface does
not claim to preempt that host read. G2 separately gates bounded outbound reads.

`OutboundHeaderInput::new(adapter, headers, fidelity)` consumes the adapter-visible HeaderMap,
records the adapter identity for marker policy, and records
whether provider field boundaries are exposed or unknown. `OutboundHead::new` charges header
caps and captures immutable Content-Type and Location evidence **before** EdgeZero filtering.
Location evidence records observed multiplicity, non-text values, retained-singleton status, and
provider fidelity, without copying the raw list. Earlier stop rules use original presence; URL
resolution consults this evidence at EZ-9.18's prescribed stage. Unknown provider information
never becomes certified raw-header fidelity. The same constructor is available to supplied clients.
Evidence has private immutable fields and no public constructor. `observed_count` counts only
adapter-visible original fields. `has_non_text` uses the existing UTF-8 eligibility rule, not a
new ASCII filter. `retained_singleton` means exactly one textual original field survived unchanged
in the visible map; it makes no stronger claim when provider boundaries are unknown.
`is_missing_or_empty` is true only for zero original observed fields or exactly one textual,
originally empty value. Duplicates, non-text fields and normalization removal cannot turn into
absence. Store these scalar facts, not a second header copy. Capture at head construction but
defer duplicate/invalid-location rejection until EZ-9.18's resolution stage; earlier stop rules win.

The owner is local and may capture `Rc`/SDK handles. It releases exactly once on head drop,
discard, finalization failure, or terminal body drop. `finish` transfers the owner into a streamed
body or releases it after bounded buffered completion; `discard` never polls body bytes. Pending
construction requires work already issued/scheduled; its future is only the completion receiver,
not a lazy exchange. `is_ready` is non-consuming, treats an error as ready, and never blocks.
Successful streamed collection transfers/disarms the pending cancellation owner before returning
the live body; dropping the completed pending handle must not abort that body. The same SDK
resource is never assigned two independent release callbacks. Add a collected-stream-stays-live
regression in addition to cancellation and drop tests.

### Batch Ownership and Mixed Collection

```rust,ignore
pub enum OutboundBatchCheck {
    Finished(OutboundBatchTermination),
    Item(OutboundBatchItem),
    Pending,
}

pub enum OutboundCollectedSlot {
    AlreadyDelivered,
    Resolved(OutboundSlotResult),
    Unresolved,
}

pub enum OutboundBatchDriverEvent {
    Cutoff,
    Failed(EdgeError),
    Item {
        index: usize,
        completed_at: MonotonicInstant,
        outcome: Result<OutboundResponse, EdgeError>,
    },
}

impl OutboundBatchItem {
    pub fn index(&self) -> usize;
    pub fn result(&self) -> &OutboundSlotResult;
    pub fn into_parts(self) -> (usize, OutboundSlotResult);
}

impl OutboundBatchResults {
    pub fn slots(&self) -> &[OutboundCollectedSlot];
    pub fn termination(&self) -> OutboundBatchTermination;
    pub fn into_parts(self) -> (Vec<OutboundCollectedSlot>, OutboundBatchTermination);
}

impl OutboundBatchFailure {
    pub fn slots(&self) -> &[OutboundCollectedSlot];
    pub fn error(&self) -> &EdgeError;
    pub fn into_parts(self) -> (EdgeError, Vec<OutboundCollectedSlot>);
}

impl OutboundBatchInsertFailure {
    pub fn error(&self) -> &EdgeError;
    pub fn into_parts(self) -> (EdgeError, OutboundBatchItem);
}

impl OutboundBatchMergeFailure {
    pub fn error(&self) -> &EdgeError;
    pub fn into_parts(self) -> (
        EdgeError,
        OutboundBatchAccumulator,
        Result<OutboundBatchResults, OutboundBatchFailure>,
    );
}

impl OutboundBatch {
    pub fn cutoff(context: OutboundBatchContext) -> Self;
    pub fn from_driver<S>(context: OutboundBatchContext, events: S,
        owner: OutboundExchangeOwner) -> Self
    where S: futures::Stream<Item = OutboundBatchDriverEvent> + 'static;
    pub fn accumulator(&self) -> OutboundBatchAccumulator;
    pub fn check(&mut self) -> Result<OutboundBatchCheck, EdgeError>;
    pub async fn next(&mut self) -> Result<OutboundBatchNext, EdgeError>;
    pub async fn collect(self) -> Result<OutboundBatchResults, OutboundBatchFailure>;
    pub fn cancel(self) -> Vec<usize>;
}

impl OutboundBatchAccumulator {
    pub fn insert(&mut self, item: OutboundBatchItem)
        -> Result<(), OutboundBatchInsertFailure>;
    pub fn merge_success(self, result: OutboundBatchResults)
        -> Result<OutboundBatchResults, OutboundBatchMergeFailure>;
    pub fn merge_failure(self, failure: OutboundBatchFailure)
        -> Result<OutboundBatchFailure, OutboundBatchMergeFailure>;
}
```

Both result and failure `slots` become `Vec<OutboundCollectedSlot>` in this hard cut; no Option
compatibility accessor remains. `check`/`next` transfer a result once. Collection records consumed
positions as `AlreadyDelivered`, new outcomes as `Resolved`, and pending positions as `Unresolved`.
`Completed` forbids `Unresolved`; `Cutoff` requires at least one. Failure retains the exact error
and those same position states. Clean EOF/trailing-event validation and poisoning are unchanged.
Results/failures have private identity and payload fields, no public constructor, Clone, mutable
access or reconstruction. `slots()`/`termination()`/`error()` permit read-only inspection while
retaining merge eligibility. `into_parts` consumes identity and transfers the original outcomes
and termination/error; it relinquishes merge eligibility, just like consuming an item receipt.
Callers merge first, then decompose when taking response bodies. Hard-migrate old public-field
access rather than preserving mutable field compatibility under the new identity contract.

Core stamps an opaque batch identity on validated consumer items and collections. Driver events
carry only unvalidated index/observation/outcome payloads; they carry no consumer identity.
`OutboundBatchItem` has private identity/index/result, no Clone, no public `new`, mutable accessor
or reconstruction method. `into_parts` consumes the receipt and relinquishes accumulator
eligibility; a consumer inspecting status/headers may still insert the intact receipt, while a
consumer taking its response body handles AlreadyDelivered itself. Hard-migrate the existing
public constructor/field callers. Remove `from_futures` and `finish_batch_item` rather than keeping
lazy defaults, raw count/clock/cutoff overloads or an alternate receipt constructor. Producers send
raw observations of already scheduled work into `from_driver`; its stream does not schedule I/O.
Core derives elapsed time and cutoff eligibility from the trusted context and earliest actual
adapter terminal observation, not later collection. It preserves the adapter's typed outcome,
including ConnectTimeout, unless the applicable governing deadline had expired at that observation
and precedence requires the selected budget timeout. Later collection never changes a recorded
outcome or cause. A backward sample produces the existing typed internal invariant result with
zero elapsed, never an enlarged budget. No raw successful result is backdated to manufacture
timeliness. IdleTimeout follows EZ-14's streamed-body rules, not this buffered batch ledger.

The driver still enforces transport and individual deadlines and records progress while consumers
are idle. For slots without an earlier recorded terminal outcome, it emits terminal observations
whose own deadline precedes cutoff before emitting Cutoff, including a timeout first observed
after cutoff. Never replace an earlier on-time success or failure with such a timeout. A late first
poll does not excuse lazy scheduling or waiting behind a sibling. Core applies the existing logical
own-deadline exception using retained metadata; own-deadline equality remains unresolved. A Cutoff
that omits such a required timeout is a typed driver invariant failure, not silent abandonment.
Drain already-recorded on-time outcomes before terminal cutoff/failure handling. Preserve clean
EOF/trailing-event validation and poisoning; premature EOF never becomes cutoff or a timeout.

`from_driver` owns one task-group/multiplexer cancellation guard; it drops that guard on cancel,
failure or driver completion. Individual exchange owners still release SDK resources exactly once;
the group guard unregisters/cancels work rather than duplicating SDK release callbacks. Scheduling
or setup failure after any work was issued must still return an owned driver (including Failed),
not discard its cancellation handle. Core adds no SDK controls or executor.

`cutoff(context)` is only a no-scheduled-work constructor. It validates the captured cutoff against
the same clock, preserves deliverable preflight outcomes and leaves cutoff-unresolved slots
unresolved. With unresolved slots, premature use is typed Internal through the batch protocol.
Empty/all-resolved input completes normally without an invalid trailing Cutoff. Once work is scheduled, even if time expires
during scheduling, use `from_driver` with its owner. External supplied clients obtain preparation
through `HttpClient`, not a public fixture constructor; core unit tests may use private test helpers.
The accumulator comes from that batch, validates identity/count/index/duplicate/conflict before
mutation, and substitutes its owned items only for `AlreadyDelivered`. A missing prior item is a
typed merge error, not a timeout. Insert/merge failures own the rejected item or both input values,
so validation never silently discards results. No response clone or full-history replay is needed.
Failure types have private fields and implement Debug/Display/Error with their validation error
as source. `insert` validates completely before mutation, returns the exact rejected receipt on
failure and leaves the accumulator unchanged. `merge_success`/`merge_failure` validate identity,
length, indices, duplicate/conflict and missing-receipt conditions before moving any outcome. On
failure, `into_parts` returns the validation error, unchanged accumulator and exact original
collection; Ok/Err in the nested Result tags success/failure input, not a second attempted merge.
In particular an original driver error remains inside the recovered failure, distinct from the
merge error. No lossy conversion may discard either input or clone non-Clone responses.
`cancel` returns the union of all never-delivered indices, including recorded-but-undelivered
and still-in-flight slots, excluding delivered ones. It deliberately does not distinguish those
two undelivered subcategories; checking first is how a caller retains recorded outcomes.
The TS recipe inserts checked items after inspecting them, then merges final success/failure.
Consumers that already consumed response bodies may handle explicit AlreadyDelivered states
themselves instead of using the optional response accumulator.

### Phase Causes and Defaults

Add `BudgetSource::ConnectTimeout` and `BudgetSource::IdleTimeout` to the existing non-exhaustive
enum. `dispatch_budget` still chooses only aggregate budget sources; phase expiry classifiers emit
the new variants. Do not add another cause field or change wire text. An expired governing budget
wins connect ties; an expired application/chain absolute deadline wins idle ties and late observation.
After idle-mode headers, per-call/default expiry does not govern the body. Marker default is on;
client off plus per-request inherit/on/off is one policy, not parallel APIs. Decoding default stays
decode; identity always keeps its decoded cap, including original mode.

### Fastly Request-Scoped Registration Observation

These proposed types live in `edgezero_adapter_fastly::context`, not core or the client trait.
G4 approves their signatures and semantics with the rest of this Interfaces section; D1b owns
implementation and external compile/runtime tests. `FastlyRequestContext` stays unchanged.

```rust,ignore
impl FastlyOutboundObservation {
    pub fn current() -> Option<Self>;
    pub fn snapshot(&self) -> FastlyOutboundStats;
}

impl FastlyOutboundStats {
    pub fn registration_count(&self) -> u64;
    pub fn configuration_count(&self) -> u64;
    pub fn total_registration_duration(&self) -> Duration;
    pub fn max_registration_duration(&self) -> Duration;
}
```

The observation handle has private fields and supports Clone/Debug/Send/Sync; a snapshot is an
immutable Copy/Clone/Debug/Eq/PartialEq value. Neither has a public constructor, Default, setter
or reset. Install the handle in the request extension bag before prepare runs; handlers obtain it
with `ctx.extensions().get::<FastlyOutboundObservation>().cloned()`. `current()` returns a handle
for the active callback or existing helper-fallback scope, and None outside those scopes. Both
paths observe the same inbound-request accounting, without downcasting the type-erased HttpClient.

Attribute each operation through D1a's current request scope, never through client construction.
Default, router-state and prepare-supplied Fastly clients, including retained clients, contribute
to that same request's record; a supplied non-Fastly client doing no Fastly registration contributes
nothing. Client precedence is unchanged. Count actual provider registration attempts, including
failures and late returns; do not hide retries. Configuration count is the number of distinct
eligible configuration keys encountered by that request, including cache hits and failed attempts.
Unsupported/already-expired preflight performs no registration/configuration accounting.
Total/max duration measure registration calls through return in the paired application clock,
including failed and late successful registration. Observation must not change wire outcomes,
logical budgets, successful-late-registration caching, or the >=50 ms warning rule.

The handle reads a coherent numeric snapshot without retaining the callback guard, exchanges,
request data or configuration-key set. Callback exit freezes that record and releases its key
set; a retained handle cannot keep work alive, resume recording or attach to the next request.
After response delivery, the still-active outer callback can continue recording post-delivery
work; helper return and response cleanup do not freeze its record. A standalone helper freezes
only its own fallback record. No observation data is added to response headers or public errors.

### Compile Contract and Acceptance Sentinels

G4/P1 compile-contract tests must prove object safety, a Send + Sync client, and owned local heads/
futures/owners that can capture `Rc` without unsafe Send. The eight implementation sites at F are
the four adapters, core `outbound.rs` and `context.rs` test clients, the demo client, and the CLI
template client. Re-run `rg 'impl OutboundHttpClient'` at implementation time; do not trust a stale count.

Create `crates/edgezero-core/tests/outbound_public_api.rs` with
`public_follow_and_option_readback_compile_contract`. Compile an external supplied client through
`HttpClient::new` and `with_client`, all four request builders/readbacks, effective marker inspection,
the redirect limit/context/actions/result accessors, and a policy owning Rc with a non-Clone error
that implements neither Debug nor Error. Verify the exact owned local-future return type without
adding Send bounds. Run
`scripts/run_test_nonzero.sh public_follow_and_option_readback_compile_contract cargo test -p edgezero-core --test outbound_public_api`.
Expected: missing or unusable public declarations fail compilation before implementation; after
implementation the named test executes and passes without a network or provider SDK.

Add `batch_producer_uses_prepared_context` to the same external test file and execute it with
`scripts/run_test_nonzero.sh batch_producer_uses_prepared_context cargo test -p edgezero-core --test outbound_public_api`.
Expected: a supplied producer consumes indexed eligible slots once, returns a context-backed
driver with its cancellation owner, and retains exact preflight outcomes. In colocated core ledger
tests also cover cutoff without scheduled work, setup failure after scheduling, earlier recorded
success across late collection, an early ConnectTimeout collected after both governing deadline
and cutoff with its cause unchanged, own-deadline timeout observed after cutoff, invalid cutoff
omitting that timeout, premature EOF, trailing/duplicate events and one release per owner. External
compile-fail doctests reject raw context construction, cloning and removed producer factories.
These tests establish a usable producer surface without publishing ledger receipts or an executor.

| Sentinel                                             | Expected Behavior                                                                                            |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| `head_disposition_precedes_payload_processing`       | Followed pending/error/oversized bodies are not polled; final bodies still enforce policy                    |
| `identity_original_mode_keeps_decoded_cap`           | Declared/observed 11-byte identity fails decoded cap 10 under both choices                                   |
| `location_evidence_survives_header_filtering`        | Valid plus non-text Location is still duplicate; earlier redirect stop rules win                             |
| `mixed_batch_collection_preserves_delivery_state`    | Check then next then collect distinguishes AlreadyDelivered/Resolved/Unresolved                              |
| `batch_accumulator_rejects_foreign_or_missing_items` | Merge rejects before mutation and returns ownership of all inputs                                            |
| `batch_recovery_returns_exact_nonclone_inputs`       | Rejected insert/merge returns original bodies and driver error; one release per owner, unchanged accumulator |
| `batch_collections_preserve_identity_until_consumed` | Read-only access permits merge; consuming decomposition transfers bodies/error once without reconstruction   |
| `begin_settlement_does_not_restart_budget`           | Entry snapshot precedes settlement; expiry polls no body or starts no transport                              |

Also add these preparation sentinels:

| Sentinel                                          | Expected Behavior                                                                                                |
| ------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `adapter_preflight_precedes_pending_settlement`   | Unsupported Spin authority/Axum Image Optimizer returns 400 with zero body polls, timer registrations or sends   |
| `settlement_timer_wakes_without_body_data`        | Outbound 200 ms expires as 504 while inbound read is pending under a 10 s deadline; read/timer resources release |
| `settlement_uses_paired_fake_time`                | Fake clock advancement wakes the timer; early/frozen-clock wake does not cause premature timeout or a busy loop  |
| `configured_connect_timeout_reaches_dispatch`     | Facade and redirect rebuild pass 1 s through prepared request readback; the aggregate budget is unchanged        |
| `image_optimizer_follow_uses_single_send_mapping` | Fastly Send/Follow use one mapping; Begin/Batch and other adapters reject with zero body/provider activity       |

Adapter-local connect sentinels supplement those core tests: Fastly and Spin implement
`connect_timeout_reaches_provider_settings`, observing the actual setter input. Fastly also
implements `connect_expiry_preserves_governing_budget_precedence`, asserting ConnectTimeout
before the governing expiry, its original budget source on equality/late observation, and a
full response-head budget. Run the native mapping/classifier tests with these exact feature
selections; do not enable WASM SDK features in a native test executable:

```sh
scripts/run_test_nonzero.sh connect_timeout_reaches_provider_settings cargo test --offline --locked -p edgezero-adapter-fastly --no-default-features --features test-utils --lib
scripts/run_test_nonzero.sh connect_expiry_preserves_governing_budget_precedence cargo test --offline --locked -p edgezero-adapter-fastly --no-default-features --features test-utils --lib
scripts/run_test_nonzero.sh connect_timeout_reaches_provider_settings cargo test --offline --locked -p edgezero-adapter-spin --no-default-features --features test-utils --lib
```

Place native mapping tests in portable policy code with a recording provider boundary. D1/D4
also execute actual SDK setter/transport tests on their WASM targets with the platform feature
and runner from Q2; a native mapping test cannot stand in for the SDK-resource leg.
These are mapping/classifier proofs, not substitutes for hosted expiry in EZ-4 criterion 8.

For each sentinel, run `scripts/run_test_nonzero.sh <sentinel> cargo test -p edgezero-core --lib`:
Expected before implementation: the named test fails for the described behavior or missing API.
Expected after implementation: it executes and passes, then the index's per-task test/fmt/clippy
card passes. These are compile/test targets to implement, not claims of current passing tests.

## Task P1: Shared Head-First Dispatch and Request Options

**Files:** Modify `crates/edgezero-core/src/outbound.rs`, `error.rs`, `lib.rs`, `time.rs`;
create `crates/edgezero-core/src/outbound/exchange.rs` and `outbound/image_optimizer.rs`; migrate four adapter `src/outbound.rs`
implementations and all test implementations found by `rg 'impl OutboundHttpClient'`.
Create `crates/edgezero-core/tests/outbound_public_api.rs` for the external compile contracts above.
Migrate callers in all four adapter `tests/contract.rs` and Spin `tests/sdk_resources.rs` in the
same atomic cut, along with core/context tests, demo and template recording clients. Discover
callers as well as implementations with `rg 'OutboundHttpClient|start_batch_until' crates examples tests`.
Also search `rg 'OutboundBatch(Item|Results|Failure)|finish_batch_item' crates examples tests` and
migrate item/collection field access, constructors and helper return handling to the approved API.
Search `rg 'OutboundBatch::(cutoff|from_driver|from_futures)' crates examples tests` as well: migrate
all producers directly to context-taking construction and remove the old future/helper factories.
Explicitly include `crates/edgezero-cli/src/generator.rs` literal source assertions in P1, not only
Q2. Its `failure.error`, `slot.elapsed`, clock-field/import and driver-construction expectations
must change with the template API cut. Search those spellings and `handlers.contains` as well as
type names; string assertions do not necessarily contain the implementing type names.
Behavioral integration tests wrap adapters with `HttpClient::with_client` and use its request-vector
API; generic trait-conformance assertions remain trait assertions. Do not add an unchecked public
`OutboundPreparedBatch` constructor or an old-signature compatibility helper to keep tests compiling.

| Caller File                                            | Migration Owner                              |
| ------------------------------------------------------ | -------------------------------------------- |
| `crates/edgezero-adapter-axum/tests/contract.rs`       | P1 atomic facade cut; D2/D5 runtime coverage |
| `crates/edgezero-adapter-cloudflare/tests/contract.rs` | P1 atomic facade cut; D3/D5 runtime coverage |
| `crates/edgezero-adapter-fastly/tests/contract.rs`     | P1 atomic facade cut; D1/D5 runtime coverage |
| `crates/edgezero-adapter-spin/tests/contract.rs`       | P1 atomic facade cut; D4/D5 runtime coverage |
| `crates/edgezero-adapter-spin/tests/sdk_resources.rs`  | P1 atomic facade cut; D4 SDK execution       |

- [ ] Add portable tests named `head_disposition_precedes_payload_processing`,
      `unsupported_options_precede_expired_budget`, and `request_options_round_trip`.
      Run `scripts/run_test_nonzero.sh head_disposition_precedes_payload_processing cargo test -p edgezero-core --lib`;
      establish the current inability to discard a permanently pending 302 before body processing.
- [ ] Add `batch_recovery_returns_exact_nonclone_inputs` and external compile-fail doctests proving
      consumers cannot construct receipts/collections or mutate identity/index/result/slots/termination.
      Add `batch_collections_preserve_identity_until_consumed` for external readback, merge eligibility
      and consuming body/error access. Cover recovery after
      insert, merge-success and merge-failure rejection, including distinct driver and validation
      errors and one release for each original body owner. Run the named core sentinel and
      `cargo test -p edgezero-core --doc`; Expected: runtime ownership assertions pass only after
      implementation and privacy examples fail to compile for the intended reason. Migrate
      old constructor/field consumers and every driver in the same hard cut; remove
      `finish_batch_item` and `from_futures` rather than retaining alternate producer paths.
- [ ] Before changing public traits, read G4's approved interface digest and G2/G3 artifacts. Stop
      if approval is missing, rejected, or refers to a different interface. Implement the Interfaces
      contract above; do not choose new public signatures inside this task.
- [ ] Define one required implementer primitive `dispatch_head`, returning status, normalized
      headers/provenance, a lazy encoded body, effective budget, response policy inputs, and the
      adapter teardown owner. Apply response-header limits before exposing the head; defer declared
      body length, decoding, and payload collection until disposition. An owned finalization method
      consumes it once using existing shared helpers. Facade `send` uses final disposition; redirect
      following uses the same primitive once per hop. No raw response bypass or duplicate client.
      Capture Location evidence before filtering and retain it independently from the visible map.
      Preserve all existing normalization precedence: original coding and 206 cannot recover
      Connection-nominated or UTF-8-filtered fields. Header caps still charge upstream fields
      before filtering, and bodyless/205 semantics still precede payload handling.
- [ ] Add the required synchronous support hook and paired clock/timer hook from Interfaces.
      Migrate every constructor and context injector in the same hard cut. Add the three preparation
      sentinels above and a missing-runtime/timer setup case; prove unsupported options never
      register a timer, poll the body or touch the network. Do not rely on dispatch-time checks.
- [ ] Add request choices for response decoding (default decode/original), marker (default on),
      optional connect timeout, and optional Fastly Image Optimizer settings. Preserve every existing
      field in `into_parts`/`from_parts`; add getters. Use typed phase causes for connect and later
      idle expiry using the explicit BudgetSource variants above without putting detail on the wire. Reject zero connect timeout;
      clamp durations; preserve existing effective-budget tie ordering and unsupported-first checks.
      Add `configured_connect_timeout_reaches_dispatch` as a facade-to-recording-client proof, not
      only a parts round-trip. D1/D4 own the corresponding provider-setter and expiry sentinels.
- [ ] Move shared mode/combination validation into one portable helper used by adapters and tests.
      Reject Image Optimizer on begin/batch and with bypass/tags before body resolution, budget,
      backend registration, or dispatch. Other adapter-specific Unsupported options are checked
      by the synchronous pre-settlement support hook. Validate the exact EZ-8 portable Image Optimizer subset at construction: quality bounds,
      nonzero dimensions, offset bounds/pairing, and incompatible focus modes. Preserve typed option
      readback. Run `cargo test -p edgezero-core` and the four native `test-utils` contracts; migrate
      macro/scaffold/demo test implementations rather than leaving an old-trait compatibility path.
      Suggested commit: `feat: establish head-first outbound policy dispatch`.

## Task P2: Bounded GET/HEAD Body Settlement

**Files:** Modify core `src/outbound.rs`, `context.rs`, `body.rs`; four adapters' `src/request.rs`
and `src/outbound.rs`; Axum `src/connection.rs` tests; adapter `tests/contract.rs`.

- [ ] Add core tests for streamed empty chunks then EOF, first nonempty byte, failed/taken/cached
      bodies, unsupported option with permanently pending input, expired outbound budget with zero
      polls, and competing 408/504 limits. Run `cargo test -p edgezero-core --lib` to show failures.
- [ ] Carry a genuinely known-empty body as empty buffered data after admission. Otherwise settle
      only GET/HEAD through the first byte or EOF under the original outbound budget and inbound
      read deadline. Preserve source error identity and sticky `BodyCell` behavior. Empty chunks must
      neither certify nonempty data nor starve timer progress. Batches still reject streamed uploads.
- [ ] Use the paired adapter timer to wake a permanently pending settlement read at the outbound
      limit, not only the inbound stream's longer deadline. Test 200 ms versus 10 s, timer/body
      readiness races, missing timer context, early wakeup and cancellation with deterministic fake
      time in core; repeat through actual Axum/workerd/Spin timers. Fastly records its EZ-1.5 exception
      explicitly rather than inheriting a false universal interruption claim.
- [ ] Integrate actual host emptiness evidence from G3, never raw header guessing. Leave ingress
      framing `HostManaged`. Nonempty GET/HEAD fails 400 before connection/backend setup; other
      methods keep stream acceptance and existing bounded prebuffering. Ensure outbound framing is
      derived from the sent body and empty GET/HEAD never uses chunked encoding.
- [ ] Run `cargo test -p edgezero-core` and each adapter contract; add actual ingress forwarding
      cases for GET/HEAD/OPTIONS/DELETE/POST. In Axum TCP tests, reject a body and then pipeline a
      healthy request; prove close or correct-offset parsing, not reuse of unread bytes. Complete
      EZ-1's environment-specific legs via the G1 inventory. Suggested commit: `fix: forward empty inbound GET and HEAD requests`.

## Task P3: Original Coding, Partial Responses, and Marker Accounting

**Files:** Modify core `src/outbound.rs` and `compression.rs`, four adapter `src/outbound.rs`
and Cloudflare `src/response.rs`; tests colocated plus four `tests/contract.rs`.

- [ ] Add table tests over identity/gzip/deflate/br/stacked/unsupported/malformed coding, both
      response modes and decoding choices, 206, HEAD, 304, and declared/observed cap boundaries.
      Add `original_coding_skips_decoder_but_keeps_transport_caps` as a nonzero sentinel.
- [ ] Add `original_coding_preserves_normalization_precedence` through the actual normalized
      head/finalizer: nominated coding/length/representation fields stay absent, invalid ETag stays
      dropped, surviving malformed length still fails, and surviving valid fields are preserved.
      Include 204/205, HEAD/304 and 206 controls plus identity decoded caps when coding was removed.
      Keep `response_normalization_precedence_table` and the identity-cap regression unchanged.
      Run the new sentinel with `scripts/run_test_nonzero.sh original_coding_preserves_normalization_precedence cargo test -p edgezero-core --lib`;
      Expected red before implementation and nonzero pass after, followed by scoped fmt/clippy.
- [ ] Preserve original payload bytes and representation fields surviving ordinary normalization
      in original mode; never restore stripped coding/length/metadata or bypass security filtering. Do not
      allocate a decoder for an encoded passthrough or apply its decoded cap. Identity still receives
      declared-length and streamed decoded-cap enforcement under both choices. Test identity length
      11 against decoded cap 10 and encoded/final caps 20, plus an exact-cap success, through the
      actual shared response-policy path. Always preserve 206 coding. Enforce encoded and
      final limits as applicable, bodyless disposition first, and retain Brotli-configuration validation,
      chunk shaping, normalization, immutable Content-Type provenance, and typed cap/decode failures.
- [ ] Add a client-wide marker setting and a per-request tri-state override (inherit/on/off), with
      effective-policy inspection before dispatch. Resolve the client setting on all send modes and
      redirect requests before handing them to a recording client. Keep the existing default on unless
      an explicitly approved requirement revision changes it; EZ-7.5 fixes on for this plan and the
      application configures its relay client off. Do not leave the default undecided or keep dual APIs.
      Implement opt-out without consuming synthetic header budget when omitted, preserve an upstream
      marker field unchanged when off, and retain pre-normalization upstream accounting. Get adapter identity
      through existing out-of-band metadata if adopted, not a new required response header. Do not
      change user-supplied `Accept-Encoding`; absent still becomes identity.
- [ ] Run core/adapter tests and actual encoded relay cases, especially workerd manual egress and
      Spin raw-host characterization. P3 presents separate EZ-5.9/5.10 adoption decisions for
      maintainer attribution in G1/G4 before implementation. If adopted, add actual finalizer/adapter
      tests for HEAD/304 removal of recognized coding/encoded length and for decoded strong-ETag
      weakening plus Accept-Ranges/Content-Digest removal, respectively. Implement only the approved
      metadata change; original coding and prior normalization still win. If deferred, retain baseline
      decoding behavior and record the deferral rather than silently applying the SHOULD. Include
      named sentinels `decoding_bodyless_metadata_follows_adopted_policy` and
      `decoded_representation_metadata_follows_adopted_policy` only for their adopted parts, with
      weak-ETag, nondecoded, original-mode and 206 controls. Execute the applicable core/native/target
      sentinels and activate them in Q2 CI. Run decoder audits. Suggested
      commit: `feat: support original response coding and proxy marker policy`.

## Task P4: Observation API and Arrival Ledger

**Files:** Create core `src/outbound/progress.rs`; modify `src/outbound.rs`, `time.rs`, and
`lib.rs`; tests colocated. Adapter driver implementation is D1-D4.

- [ ] Add deterministic tests for recorded arrival versus collection, equality at cutoff, own
      deadline before cutoff, equal deadline/cutoff, input-index ties, late `next`, and repeated terminal
      observations. Keep every malformed-driver and failure-with-partials regression.
      Add the mixed check/next/collect success, cutoff and failure cases and matching/missing/
      foreign/duplicate accumulator cases from the Interfaces block; do not weaken at-most-once ownership.
- [ ] Add an owned begun-request handle with async collection and a nonblocking ready check;
      `begin` may await GET/HEAD settlement but not connect/head receipt. Keep `start_batch_until`
      synchronous: it validates and hands eligible buffered slots to a verified adapter scheduler or
      Fastly pending handle before returning. No lazy-first-poll driver may masquerade as eager.
- [ ] Add a nonblocking batch observation result that distinguishes no-new-result from terminal
      completion; route it and `next` through the same validation/poisoning ledger. Store recorded
      terminal instants privately with input indices and original budget provenance. Sort observation
      by arrival then input index; collection remains input-indexed. Preserve completed outcomes
      before cutoff/failure; cancellation reports every not-yet-delivered index. Replace ambiguous
      Option slots with the specified collected-slot states in both success and failure results.
- [ ] Run `cargo test -p edgezero-core` and a cancellation-safety test that polls then drops `next`
      without consuming an item. A response at cutoff is unresolved; own-deadline timeout before
      cutoff remains deliverable. No public terminal metadata is invented before actual observation.
      Suggested commit: `feat: define eager exchange and nonblocking batch observation`.
- [ ] Add `arrival_ledger_preserves_late_collection_and_cancel_safety` and run
      `scripts/run_test_nonzero.sh arrival_ledger_preserves_late_collection_and_cancel_safety cargo test -p edgezero-core --features test-utils --lib`
      before/after implementation. Expected: a real red assertion for missing eager/arrival behavior,
      then strict cutoff equality, preserved earlier arrivals, mixed ownership states and cancelled
      pending `next` all pass without wall-clock sleeps. Apply the index's task lint card.

## Task P5: Redirect Following Through the Same Client

**Files:** Create core `src/outbound/redirect.rs`; modify `src/outbound.rs` and `lib.rs`;
test in that module and each adapter `tests/contract.rs`.

- [ ] Write the complete EZ-9 status/method/body decision table and precedence tests with a
      recording head-first implementation. Include a followed 302 whose body is permanently pending,
      malformed compressed bytes, oversized declared length, or a read error; first demonstrate the
      missing policy path with `cargo test -p edgezero-core --lib`.
- [ ] Add explicit following with validated hop limit and a synchronous application target policy.
      Plain send/begin/batch never follow. Return final URI, hop count, typed stop reason, or unchanged
      hop/policy error with its index. Resolve relative Location and fragments with the pinned parser;
      enforce the exact EZ-9.18 decision order, duplicate-Location rule, and non-HTTP policy case.
- [ ] Clone only buffered replay bytes. Settle inbound GET/HEAD once. Drop content headers and
      Expect on rewrites; strip only the specified credentials/override on cross-origin prepared
      requests. Run policy before the next network action and never strip its deliberately replaced
      credentials again. Copy options on prepared requests; fresh policy requests use their own
      defaults. Retain the initial absolute chain deadline independently through final body consumption.
- [ ] Consume or discard each head once. Followed bodies are dropped without polling by default;
      do not add optional drains until their nonblocking work bound is independently proved. Final
      3xx receives ordinary body policy. Run `cargo test -p edgezero-core` and adapter no-redirect/
      followed-response parity cases. Capture logs and prove query tokens never appear. Suggested
      commit: `feat: follow redirects with per-hop application policy`.
- [ ] Add `redirect_chain_uses_supplied_head_client_and_original_deadline`; run
      `scripts/run_test_nonzero.sh redirect_chain_uses_supplied_head_client_and_original_deadline cargo test -p edgezero-core --features test-utils --lib`.
      Expected: red for missing following, then one supplied-client dispatch per hop, no polling of
      followed bodies, unchanged typed policy/hop errors and an unextended original chain deadline.
      Include each stop reason and normalized Location controls; apply the task lint card.

## Task P6: Published Portable Scripted Client, Only If Promoted

**Files:** Create core `src/outbound/testing.rs`; modify core `Cargo.toml`, `src/lib.rs`, and
`src/outbound.rs`; add CI activation in qualification Q2. Otherwise keep this harness crate-private.

- [ ] Record G1's decision before exporting EZ-24.1-24.10 for host/WASI. Do not implicitly adopt
      JavaScript support, script-abandonment reporting, or template replacement. Add deterministic
      fixtures using `json!` and an explicitly advanced shared `MonotonicClock`, without wall time.
      P6 depends on P1-P5 and their implemented semantics; its promotion never makes production
      acceptance depend on the published double. Use the private fixture harness before P6 exists.
- [ ] Record every effective request field/body, send mode, batch/index, dispatch time, and budget.
      Use the portable validation/head/finalization helpers, not an adapter capability emulator. Accept
      otherwise-valid single Image Optimizer/authority settings; reject universally invalid combinations
      and modes before consuming a script. No answer must end under the selected deadline.
- [ ] Script head and chunk times separately, errors, caps, and a failed driver after one item.
      Prove `next` indices 1/2/0 at 10/20/30 ms versus `collect` indices 0/1/2, 99 ms kept versus
      100 ms cutoff unresolved, late-check retention, origin/batch overlap, and pending redirect-body
      bypass. Unscripted requests fail without panic; recordings remain accessible after injection.
- [ ] Add `scripted_replies_preserve_cutoff_and_arrival_order`; run
      `scripts/run_test_nonzero.sh scripted_replies_preserve_cutoff_and_arrival_order cargo test -p edgezero-core --features test-utils --lib`
      and `CARGO_TARGET_WASM32_WASIP1_RUNNER='viceroy run' scripts/run_test_nonzero.sh scripted_replies_preserve_cutoff_and_arrival_order cargo test --locked -p edgezero-core --features test-utils --target wasm32-wasip1 --lib scripted_replies_preserve_cutoff_and_arrival_order`.
      Expected: red for missing published utility, then nonzero host/WASI passes with 1/2/0 arrival
      order, positional collection and strict cutoff boundary using paired fake time. JS legs run
      only if adopted. The explicit WASI test filter is required: the nonzero helper verifies
      presence but does not filter, and unrelated native thread tests cannot execute under WASIp1.
      Apply the task lint card. Document portable
      conformance versus real provider behavior. Suggested commit: `test: publish deterministic outbound scripts`.

## Phase Exit

- [ ] Run the core commands from the index's per-task card, `cargo test -p edgezero-core --doc`,
      and the applicable named sentinels above, then Q2's seven explicit feature-matrix commands.
      Expected: nonzero runtime cases, intended privacy compile failures and all caller builds pass;
      no unchecked constructor or compatibility implementation was needed to keep tests compiling.
- [ ] Core tests preserve all baseline invariants; all in-tree clients compile with the new protocol.
- [ ] No new API has an unimplemented default that silently drops an option or defers dispatch.
- [ ] Adapter integration and actual ingress/host evidence are completed in phase 3, not inferred
      from these portable tests. No intermediate contract-breaking revision is merge-ready.
