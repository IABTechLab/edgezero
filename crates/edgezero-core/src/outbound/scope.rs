use std::any::{Any, TypeId};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::convert::identity;
use std::fmt;
use std::future::Future;
use std::io;
use std::mem;
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::task::{Context, Poll, Waker};

use futures_util::future::{LocalBoxFuture, poll_fn, ready};

use super::{OutboundFuture, OutboundTime};
use crate::error::EdgeError;
use crate::http::Response;
use crate::time::MonotonicInstant;

std::thread_local! {
    static CURRENT: RefCell<Option<Current>> = const { RefCell::new(None) };
}

/// The canonical lifecycle boundary that can revoke an outbound operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboundScopeKind {
    Callback,
    ResponseBody,
}

/// Local provider progress and activation around exactly one inner poll.
///
/// A runtime must invoke `inner` once and preserve its readiness unless returning an error.
/// Provider setup may fail before polling `inner`. Activation must be restored before return;
/// no provider or thread-local activation guard may survive a Pending result.
pub trait OutboundProgressRuntime {
    fn poll(
        &self,
        scope: &OutboundScope,
        time: &OutboundTime,
        entered_at: MonotonicInstant,
        cx: &mut Context<'_>,
        inner: &mut dyn FnMut(&mut Context<'_>) -> Poll<()>,
    ) -> Poll<Result<(), EdgeError>>;
}

/// Progress for providers whose owned futures are driven by their existing local executor.
#[derive(Debug)]
pub struct IndependentOutboundProgress;

/// Unique local authority to close a callback or response-body lifetime.
/// Holding a captured scope never keeps this owner alive or delays its closure.
pub struct OutboundScopeOwner {
    provisional: RefCell<Option<Revocable<Provisional>>>,
    state: Rc<State>,
}

/// Invocation-bound local scope and provider runtime, independent of the retained client.
///
/// Canonical captures weakly reference their owner. An explicitly unmanaged capture instead
/// owns its local state until the last invocation token is dropped; it claims no callback TTL.
#[derive(Clone)]
pub struct OutboundScope {
    current: Current,
    owned: Option<Rc<State>>,
    progress: Rc<dyn OutboundProgressRuntime>,
}

#[derive(Clone)]
struct Current {
    kind: OutboundScopeKind,
    managed: bool,
    state: Weak<State>,
}

struct State {
    closed: Cell<bool>,
    kind: OutboundScopeKind,
    next_id: Cell<u64>,
    providers: RefCell<Option<ProviderCache>>,
    provisional_parent: RefCell<Option<Current>>,
    roots: RefCell<BTreeMap<u64, Weak<dyn Root>>>,
}

type ProviderCache = Rc<RefCell<HashMap<TypeId, Rc<dyn Any>>>>;

struct Provisional {
    armed: bool,
    child: Weak<State>,
}

trait Root {
    fn drain(&self) -> Cleanup;
    fn mark_revoked(&self);
}

struct Cleanup {
    payload: Option<Box<dyn Any>>,
    waiting: Option<Waker>,
}

struct Node<T> {
    active: Cell<bool>,
    revoked: Cell<bool>,
    value: RefCell<Option<T>>,
    waker: RefCell<Option<Waker>>,
}

/// A weakly registered owned payload. Its value is never polled or dropped under a borrow.
pub(crate) struct Revocable<T: 'static> {
    node: Rc<Node<T>>,
    registration: Cell<Option<u64>>,
    scope: OutboundScope,
}

struct RestoreCurrent(Option<Current>);

struct RestoreActive<'value>(&'value Cell<bool>);

#[derive(Debug, thiserror::Error)]
#[error("outbound {kind:?} scope closed")]
struct ScopeClosed {
    kind: OutboundScopeKind,
}

impl OutboundProgressRuntime for IndependentOutboundProgress {
    #[inline]
    fn poll(
        &self,
        _scope: &OutboundScope,
        _time: &OutboundTime,
        _entered_at: MonotonicInstant,
        cx: &mut Context<'_>,
        inner: &mut dyn FnMut(&mut Context<'_>) -> Poll<()>,
    ) -> Poll<Result<(), EdgeError>> {
        inner(cx).map(Ok)
    }
}

impl OutboundScopeOwner {
    pub(crate) fn accept_body_handoff(&self) -> Result<(), EdgeError> {
        if self.state.kind != OutboundScopeKind::ResponseBody {
            return Err(invariant(
                "only a response-body outbound scope owner can be handed off",
            ));
        }
        self.scope().check()?;
        let registration = self.provisional.borrow_mut().take();
        if let Some(mut lease) = registration {
            let mut guard = lease.take()?;
            guard.armed = false;
            let dependency = self.state.provisional_parent.borrow_mut().take();
            drop(dependency);
        }
        self.scope().check()
    }

    /// Transfers this exact body lifetime into response-body metadata before any body poll.
    /// Dropping an unhanded response closes its precreated outbound work.
    ///
    /// # Errors
    /// Returns Internal for a callback owner, a closed owner, or an already attached body.
    #[inline]
    pub fn attach(self, mut response: Response) -> Result<Response, EdgeError> {
        if self.state.kind != OutboundScopeKind::ResponseBody {
            return Err(invariant(
                "only a response-body outbound scope owner can be attached",
            ));
        }
        self.scope().check()?;
        let body = mem::take(response.body_mut()).attach_egress_owner(self)?;
        *response.body_mut() = body;
        Ok(response)
    }

    /// Creates one canonical serving-callback owner without activating it.
    #[must_use]
    #[inline]
    pub fn callback() -> Self {
        let ambient = CURRENT.with(|current| current.borrow().clone());
        let state = State::new(OutboundScopeKind::Callback, None);
        state.closed.set(current_closed(ambient.as_ref()));
        Self {
            provisional: RefCell::new(None),
            state: Rc::new(state),
        }
    }

    /// Creates a callback owner only when canonical helper composition has no managed owner.
    /// A closed managed ambient scope is reused as closed, never replaced by a fresh lifetime.
    #[must_use]
    #[inline]
    pub fn callback_if_absent() -> Option<Self> {
        let managed = CURRENT.with(|current| {
            current
                .borrow()
                .as_ref()
                .is_some_and(|ambient| ambient.managed)
        });
        if managed {
            None
        } else {
            Some(Self::callback())
        }
    }

    /// Revokes every root before running any payload cleanup or notification.
    #[inline]
    pub fn close(&self) {
        self.with(|| {
            let provisional = self.provisional.borrow_mut().take();
            self.state.close();
            drop(provisional);
        });
    }

    /// Creates an independently cancellable body owner sharing its active provider family.
    /// A family lease keeps neither the callback owner nor its cancellation roots alive.
    /// Until framework handoff, a managed parent also revokes this provisional body lifetime.
    /// A closed ambient owner cannot be escaped by opening a response-body owner.
    #[must_use]
    #[inline]
    pub fn response_body() -> Self {
        let parent = CURRENT.with(|current| current.borrow().clone());
        let closed = current_closed(parent.as_ref());
        let state = Rc::new(State::new(OutboundScopeKind::ResponseBody, parent.clone()));
        state.closed.set(closed);
        let provisional = parent
            .filter(|current| current.managed)
            .and_then(|current| {
                let scope = OutboundScope {
                    current,
                    owned: None,
                    progress: Rc::new(IndependentOutboundProgress),
                };
                Revocable::new(
                    &scope,
                    Provisional {
                        armed: true,
                        child: Rc::downgrade(&state),
                    },
                )
                .ok()
            });
        Self {
            provisional: RefCell::new(provisional),
            state,
        }
    }

    /// Activates this owner only while each poll executes; the returned future borrows no owner.
    #[inline]
    pub fn run<'future, F>(&self, future: F) -> LocalBoxFuture<'future, F::Output>
    where
        F: Future + 'future,
        F::Output: 'future,
    {
        let current = self.scope().current;
        let mut pinned = Box::pin(future);
        Box::pin(poll_fn(move |cx| {
            with_current(current.clone(), || pinned.as_mut().poll(cx))
        }))
    }

    /// Returns a weak capability with independent progress for synchronous activation.
    #[must_use]
    #[inline]
    pub fn scope(&self) -> OutboundScope {
        OutboundScope {
            current: Current {
                kind: self.state.kind,
                managed: true,
                state: Rc::downgrade(&self.state),
            },
            owned: None,
            progress: Rc::new(IndependentOutboundProgress),
        }
    }

    /// Activates this canonical owner for one synchronous call, restoring the prior scope.
    #[expect(
        clippy::impl_trait_in_params,
        reason = "Keep only the return type as a public generic parameter"
    )]
    #[inline]
    pub fn with<R>(&self, action: impl FnOnce() -> R) -> R {
        with_current(self.scope().current, action)
    }
}

impl fmt::Debug for OutboundScopeOwner {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutboundScopeOwner")
            .field("closed", &self.state.is_closed())
            .field("kind", &self.state.kind)
            .finish_non_exhaustive()
    }
}

#[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
impl Drop for OutboundScopeOwner {
    #[inline]
    fn drop(&mut self) {
        self.close();
    }
}

impl OutboundScope {
    /// Captures the current owner once, replacing only the provider runtime, never ownership.
    /// With no canonical owner, this creates an explicitly caller-owned manual lifetime.
    #[must_use]
    #[inline]
    pub fn capture(progress: Rc<dyn OutboundProgressRuntime>) -> Self {
        let ambient = CURRENT.with(|current| current.borrow().clone());
        if let Some(current) = ambient {
            let owned = if current.managed {
                None
            } else {
                current.state.upgrade()
            };
            Self {
                current,
                owned,
                progress,
            }
        } else {
            let state = Rc::new(State::new(OutboundScopeKind::Callback, None));
            Self {
                current: Current {
                    kind: state.kind,
                    managed: false,
                    state: Rc::downgrade(&state),
                },
                owned: Some(state),
                progress,
            }
        }
    }

    /// Checks the captured owner without consulting or replacing the ambient request.
    ///
    /// # Errors
    /// Returns typed Internal with the original owner kind after closure or owner destruction.
    #[inline]
    pub fn check(&self) -> Result<(), EdgeError> {
        if self.state().is_some_and(|state| !state.is_closed()) {
            Ok(())
        } else {
            Err(self.closed_error())
        }
    }

    fn closed_error(&self) -> EdgeError {
        EdgeError::internal(ScopeClosed {
            kind: self.current.kind,
        })
    }

    #[must_use]
    #[inline]
    pub fn is_managed(&self) -> bool {
        self.current.managed
    }

    #[must_use]
    #[inline]
    pub fn kind(&self) -> Option<OutboundScopeKind> {
        self.current.managed.then_some(self.current.kind)
    }

    /// Activates the captured scope before the provider runtime and the inner future are polled.
    ///
    /// # Errors
    /// Preserves provider failures, rejects closed owners, and reports invalid runtime polling.
    #[inline]
    pub fn poll<F>(
        &self,
        time: &OutboundTime,
        entered_at: MonotonicInstant,
        cx: &mut Context<'_>,
        mut future: Pin<&mut F>,
    ) -> Poll<Result<F::Output, EdgeError>>
    where
        F: Future + ?Sized,
    {
        let mut called = false;
        let mut multiple = false;
        let mut output = None;
        let observation = self.with(|| {
            self.progress
                .poll(self, time, entered_at, cx, &mut |inner_cx| {
                    if called {
                        multiple = true;
                        return Poll::Ready(());
                    }
                    called = true;
                    match future.as_mut().poll(inner_cx) {
                        Poll::Pending => Poll::Pending,
                        Poll::Ready(value) => {
                            output = Some(value);
                            Poll::Ready(())
                        }
                    }
                })
        });
        if multiple {
            return Poll::Ready(Err(invariant(
                "outbound progress runtime polled an operation more than once",
            )));
        }
        match observation {
            Err(error) | Ok(Poll::Ready(Err(error))) => Poll::Ready(Err(error)),
            Ok(Poll::Pending) if called && output.is_none() => Poll::Pending,
            Ok(Poll::Ready(Ok(()))) if called => match output {
                Some(value) => Poll::Ready(Ok(value)),
                None => Poll::Ready(Err(invariant(
                    "outbound progress runtime completed a pending operation",
                ))),
            },
            _ => Poll::Ready(Err(invariant(
                "outbound progress runtime did not preserve operation readiness",
            ))),
        }
    }

    pub(super) fn progress(&self) -> Rc<dyn OutboundProgressRuntime> {
        Rc::clone(&self.progress)
    }

    /// Resolves typed provider state in the shared callback and response-body family cache.
    /// Initializers and displaced state are never run or dropped under a registry borrow.
    ///
    /// # Errors
    /// Returns typed Internal if initialization races owner closure or registration is invalid.
    #[expect(
        clippy::impl_trait_in_params,
        reason = "Keep the state type as the only public generic parameter"
    )]
    #[inline]
    pub fn provider_state<T>(&self, init: impl FnOnce() -> T) -> Result<Rc<T>, EdgeError>
    where
        T: 'static,
    {
        self.check()?;
        let state = self.state().ok_or_else(|| self.closed_error())?;
        let family = state
            .providers
            .borrow()
            .as_ref()
            .map(Rc::clone)
            .ok_or_else(|| self.closed_error())?;
        let id = TypeId::of::<T>();
        let inherited = { family.borrow().get(&id).cloned() };
        let proposed: Rc<dyn Any> = inherited.unwrap_or_else(|| Rc::new(init()));
        self.check()?;
        let retained = {
            let mut providers = family.borrow_mut();
            Rc::clone(providers.entry(id).or_insert_with(|| Rc::clone(&proposed)))
        };
        drop(proposed);
        self.check()?;
        retained
            .downcast::<T>()
            .map_err(|_wrong_type| invariant("outbound provider state has an inconsistent type"))
    }

    /// Runs an already-owned provider task with scope/progress activation, without registering
    /// another cancellation root. Pending handles and batch producers own that registration.
    #[must_use]
    #[inline]
    pub fn run<T>(
        &self,
        time: OutboundTime,
        entered_at: MonotonicInstant,
        future: OutboundFuture<T>,
    ) -> OutboundFuture<T>
    where
        T: 'static,
    {
        let mut active = Some((self.clone(), time, future));
        Box::pin(poll_fn(move |cx| {
            let result = if let Some((scope, paired, pinned)) = &mut active {
                scope
                    .poll(paired, entered_at, cx, pinned.as_mut())
                    .map(|outer| outer.and_then(identity))
            } else {
                Poll::Ready(Err(invariant(
                    "outbound scoped task polled after completion",
                )))
            };
            if result.is_ready() {
                drop(active.take());
            }
            result
        }))
    }

    fn state(&self) -> Option<Rc<State>> {
        self.owned
            .as_ref()
            .map(Rc::clone)
            .or_else(|| self.current.state.upgrade())
    }

    /// Activates this captured capability for one synchronous operation.
    ///
    /// # Errors
    /// Returns typed Internal before executing the action, or after reentrant owner closure.
    #[expect(
        clippy::impl_trait_in_params,
        reason = "Keep only the return type as a public generic parameter"
    )]
    #[inline]
    pub fn with<R>(&self, action: impl FnOnce() -> R) -> Result<R, EdgeError> {
        self.check()?;
        let output = self.with_cleanup(action);
        self.check()?;
        Ok(output)
    }

    pub(crate) fn with_cleanup<R, A>(&self, action: A) -> R
    where
        A: FnOnce() -> R,
    {
        with_current(self.current.clone(), action)
    }
}

impl fmt::Debug for OutboundScope {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutboundScope")
            .field("kind", &self.kind())
            .field("managed", &self.is_managed())
            .finish_non_exhaustive()
    }
}

impl State {
    fn close(&self) {
        if self.closed.replace(true) {
            return;
        }
        let roots: Vec<_> = mem::take(&mut *self.roots.borrow_mut())
            .into_values()
            .filter_map(|root| root.upgrade())
            .collect();
        let providers = self.providers.borrow_mut().take();
        // No teardown or notification may run until every sibling is visibly revoked.
        for root in &roots {
            root.mark_revoked();
        }
        // Detach every payload before user code runs, including when one destructor unwinds.
        let cleanups: Vec<_> = roots.iter().map(|root| root.drain()).collect();
        for cleanup in cleanups {
            cleanup.release();
        }
        drop(providers);
    }

    fn is_closed(&self) -> bool {
        let dependency = self.provisional_parent.borrow().clone();
        self.closed.get() || current_closed(dependency.as_ref())
    }

    fn new(kind: OutboundScopeKind, parent: Option<Current>) -> Self {
        let providers = parent
            .as_ref()
            .and_then(|captured| captured.state.upgrade())
            .filter(|active| !active.is_closed())
            .and_then(|active| active.providers.borrow().as_ref().map(Rc::clone))
            .unwrap_or_else(|| Rc::new(RefCell::new(HashMap::new())));
        Self {
            closed: Cell::new(false),
            kind,
            next_id: Cell::new(0),
            providers: RefCell::new(Some(providers)),
            provisional_parent: RefCell::new(parent.filter(|current| current.managed)),
            roots: RefCell::new(BTreeMap::new()),
        }
    }
}

impl<T: 'static> Revocable<T> {
    pub(crate) fn is_revoked(&self) -> bool {
        self.node.revoked.get() || self.scope.state().is_none_or(|state| state.is_closed())
    }

    pub(crate) fn new(scope: &OutboundScope, value: T) -> Result<Self, EdgeError> {
        scope.with_cleanup(|| {
            scope.check()?;
            let state = scope.state().ok_or_else(|| scope.closed_error())?;
            let id = state.next_id.get();
            state.next_id.set(
                id.checked_add(1)
                    .ok_or_else(|| invariant("outbound scope registration exhausted"))?,
            );
            let node = Rc::new(Node {
                active: Cell::new(false),
                revoked: Cell::new(false),
                value: RefCell::new(Some(value)),
                waker: RefCell::new(None),
            });
            let erased: Rc<dyn Root> = Rc::<Node<T>>::clone(&node);
            state.roots.borrow_mut().insert(id, Rc::downgrade(&erased));
            Ok(Self {
                node,
                registration: Cell::new(Some(id)),
                scope: scope.clone(),
            })
        })
    }

    pub(crate) fn revoke(&self) {
        self.scope.with_cleanup(|| {
            self.unregister();
            self.node.mark_revoked();
            self.node.drain().release();
        });
    }

    pub(crate) fn take(&mut self) -> Result<T, EdgeError> {
        self.scope.with_cleanup(|| {
            self.scope.check()?;
            if self.node.revoked.get() {
                return Err(self.scope.closed_error());
            }
            let value = self
                .node
                .value
                .borrow_mut()
                .take()
                .ok_or_else(|| invariant("outbound scoped value already consumed or in use"))?;
            self.unregister();
            let previous = self.node.waker.borrow_mut().take();
            drop(previous);
            self.scope.check()?;
            Ok(value)
        })
    }

    fn unregister(&self) {
        if let Some(id) = self.registration.take()
            && let Some(state) = self.scope.state()
        {
            state.roots.borrow_mut().remove(&id);
        }
    }

    pub(crate) fn with<R, A>(&self, cx: &mut Context<'_>, action: A) -> Result<R, EdgeError>
    where
        A: FnOnce(&mut T, &mut Context<'_>) -> R,
    {
        // Keep activation through value/output destruction on early return or unwind.
        self.scope.with_cleanup(|| {
            self.scope.check()?;
            if self.node.revoked.get() {
                return Err(self.scope.closed_error());
            }
            if self.node.active.replace(true) {
                return Err(invariant("outbound scoped value is already in use"));
            }
            let active = RestoreActive(&self.node.active);
            let mut value = self
                .node
                .value
                .borrow_mut()
                .take()
                .ok_or_else(|| invariant("outbound scoped value already consumed"))?;
            let replacement = cx.waker().clone();
            self.scope.check()?;
            let previous = self.node.waker.borrow_mut().replace(replacement);
            drop(previous);
            self.scope.check()?;
            if self.node.revoked.get() {
                return Err(self.scope.closed_error());
            }
            let output = action(&mut value, cx);
            if self.is_revoked() {
                return Err(self.scope.closed_error());
            }
            let displaced = self.node.value.borrow_mut().replace(value);
            drop(displaced);
            drop(active);
            Ok(output)
        })
    }
}

impl<T: 'static> fmt::Debug for Revocable<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Revocable")
            .field("revoked", &self.is_revoked())
            .finish_non_exhaustive()
    }
}

#[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
impl<T: 'static> Drop for Revocable<T> {
    fn drop(&mut self) {
        self.revoke();
    }
}

impl Cleanup {
    fn release(mut self) {
        drop(self.payload.take());
        if let Some(waiting) = self.waiting.take() {
            waiting.wake();
        }
    }
}

#[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
impl Drop for Provisional {
    fn drop(&mut self) {
        if self.armed
            && let Some(state) = self.child.upgrade()
        {
            let current = Current {
                kind: state.kind,
                managed: true,
                state: Rc::downgrade(&state),
            };
            with_current(current, || state.close());
        }
    }
}

impl<T: 'static> Root for Node<T> {
    fn drain(&self) -> Cleanup {
        let value = self.value.borrow_mut().take();
        let waiting = self.waker.borrow_mut().take();
        Cleanup {
            payload: value.map(|payload| -> Box<dyn Any> { Box::new(payload) }),
            waiting,
        }
    }

    fn mark_revoked(&self) {
        self.revoked.set(true);
    }
}

#[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
impl Drop for RestoreCurrent {
    fn drop(&mut self) {
        CURRENT.with(|current| current.replace(self.0.take()));
    }
}

#[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
impl Drop for RestoreActive<'_> {
    fn drop(&mut self) {
        self.0.set(false);
    }
}

pub(super) fn bind_future<T>(
    scope: OutboundScope,
    time: OutboundTime,
    entered_at: MonotonicInstant,
    future: OutboundFuture<T>,
) -> OutboundFuture<T>
where
    T: 'static,
{
    let registered = match Revocable::new(&scope, future) {
        Ok(value) => value,
        Err(error) => return Box::pin(ready(Err(error))),
    };
    let mut active = Some((registered, scope, time));
    Box::pin(poll_fn(move |cx| {
        let result = if let Some((value, captured, paired)) = &active {
            match value.with(cx, |pinned, inner_cx| {
                captured.poll(paired, entered_at, inner_cx, pinned.as_mut())
            }) {
                Err(error) => Poll::Ready(Err(error)),
                Ok(observed) => observed.map(|outer| outer.and_then(identity)),
            }
        } else {
            Poll::Ready(Err(invariant(
                "outbound scoped future polled after completion",
            )))
        };
        if result.is_ready() {
            drop(active.take());
        }
        result
    }))
}

fn current_closed(current: Option<&Current>) -> bool {
    current.is_some_and(|captured| {
        captured
            .state
            .upgrade()
            .is_none_or(|state| state.is_closed())
    })
}

fn invariant(message: &'static str) -> EdgeError {
    EdgeError::internal(io::Error::other(message))
}

fn with_current<R, A>(current: Current, action: A) -> R
where
    A: FnOnce() -> R,
{
    let previous = CURRENT.with(|active| active.replace(Some(current)));
    let restore = RestoreCurrent(previous);
    let result = action();
    drop(restore);
    result
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::rc::{Rc, Weak};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use futures::executor::block_on;
    use futures_util::future::{pending, poll_fn};
    use futures_util::task::{ArcWake, waker};

    use super::{
        IndependentOutboundProgress, OutboundProgressRuntime, OutboundScope, OutboundScopeOwner,
        Revocable, bind_future,
    };
    use crate::body::Body;
    use crate::error::EdgeError;
    use crate::http::Response;
    use crate::outbound::{OutboundFuture, OutboundTime};
    use crate::time::{MonotonicClock, MonotonicInstant};

    struct Release(Rc<Cell<usize>>);

    struct DropAction(Option<Box<dyn FnOnce()>>);

    struct WakeAction;

    struct Runtime {
        active: Arc<AtomicBool>,
        polls: Rc<Cell<usize>>,
    }

    struct InvalidRuntime(u8);

    #[derive(Debug, thiserror::Error)]
    #[error("original scoped timer fault")]
    struct TimerFault(Arc<()>);

    std::thread_local! {
        static DROP_ACTION: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
        static WAKE_ACTION: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
    }

    #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
    impl Drop for DropAction {
        fn drop(&mut self) {
            if let Some(action) = self.0.take() {
                action();
            }
        }
    }

    #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
    impl Drop for WakeAction {
        fn drop(&mut self) {
            let action = DROP_ACTION.with(|stored| stored.borrow_mut().take());
            if let Some(callback) = action {
                callback();
            }
        }
    }

    impl ArcWake for WakeAction {
        fn wake(self: Arc<Self>) {
            Self::wake_by_ref(&self);
        }
        fn wake_by_ref(_self: &Arc<Self>) {
            let action = WAKE_ACTION.with(|stored| stored.borrow_mut().take());
            if let Some(callback) = action {
                callback();
            }
        }
    }

    impl OutboundProgressRuntime for Runtime {
        fn poll(
            &self,
            _: &OutboundScope,
            _: &OutboundTime,
            _: MonotonicInstant,
            cx: &mut Context<'_>,
            inner: &mut dyn FnMut(&mut Context<'_>) -> Poll<()>,
        ) -> Poll<Result<(), EdgeError>> {
            self.polls.set(self.polls.get().saturating_add(1));
            let previous = self.active.swap(true, Ordering::SeqCst);
            let active = Arc::clone(&self.active);
            let restore = DropAction(Some(Box::new(move || {
                active.store(previous, Ordering::SeqCst);
            })));
            let result = inner(cx);
            drop(restore);
            result.map(Ok)
        }
    }

    impl OutboundProgressRuntime for InvalidRuntime {
        fn poll(
            &self,
            _: &OutboundScope,
            _: &OutboundTime,
            _: MonotonicInstant,
            cx: &mut Context<'_>,
            inner: &mut dyn FnMut(&mut Context<'_>) -> Poll<()>,
        ) -> Poll<Result<(), EdgeError>> {
            match self.0 {
                0 => {
                    let _first = inner(cx);
                    inner(cx).map(Ok)
                }
                1 => Poll::Ready(Ok(())),
                _ => {
                    let _first = inner(cx);
                    Poll::Pending
                }
            }
        }
    }

    #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.set(self.0.get().saturating_add(1));
        }
    }

    fn capture() -> OutboundScope {
        OutboundScope::capture(Rc::new(IndependentOutboundProgress))
    }

    fn time() -> (MonotonicInstant, OutboundTime) {
        let now = MonotonicInstant::now();
        (
            now,
            OutboundTime::new(MonotonicClock::new(move || now), |_, _| Box::pin(pending())),
        )
    }

    #[test]
    fn closing_owner_releases_unpolled_owned_future() {
        let owner = OutboundScopeOwner::callback();
        let scope = owner.with(capture);
        let releases = Rc::new(Cell::new(0_usize));
        let lease = Release(Rc::clone(&releases));
        let (entered, paired) = time();
        let source: OutboundFuture<()> = Box::pin(async move {
            let _lease = lease;
            pending().await
        });
        let retained = bind_future(scope, paired, entered, source);
        owner.close();
        assert_eq!(
            releases.get(),
            1,
            "close must not wait for polling or handle drop"
        );
        assert!(matches!(
            block_on(retained),
            Err(EdgeError::Internal { .. })
        ));
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn revocable_with_observes_reentrant_close_and_drops_its_output() {
        let owner = OutboundScopeOwner::callback();
        let scope = owner.with(capture);
        let input_releases = Rc::new(Cell::new(0_usize));
        let output_releases = Rc::new(Cell::new(0_usize));
        let value = Revocable::new(&scope, Release(Rc::clone(&input_releases))).expect("register");
        let result = value.with(&mut Context::from_waker(Waker::noop()), |_, _| {
            owner.close();
            Release(Rc::clone(&output_releases))
        });
        assert!(matches!(result, Err(EdgeError::Internal { .. })));
        assert!(value.is_revoked());
        assert_eq!(input_releases.get(), 1);
        assert_eq!(output_releases.get(), 1);
    }

    #[test]
    fn revocable_take_unregisters_without_revoking_transferred_value() {
        let owner = OutboundScopeOwner::callback();
        let scope = owner.with(capture);
        let releases = Rc::new(Cell::new(0_usize));
        let mut value = Revocable::new(&scope, Release(Rc::clone(&releases))).expect("register");
        let transferred = value.take().expect("take");
        owner.close();
        assert_eq!(releases.get(), 0);
        drop(value);
        assert_eq!(releases.get(), 0);
        drop(transferred);
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn scope_capability_does_not_keep_owner_open_or_resurrect_it() {
        let owner = OutboundScopeOwner::callback();
        let scope = owner.with(capture);
        assert!(scope.is_managed());
        drop(owner);
        assert!(matches!(scope.check(), Err(EdgeError::Internal { .. })));
        let mut source = Box::pin(async { panic!("closed scope cannot poll source") });
        let (entered, paired) = time();
        assert!(matches!(
            scope.poll(
                &paired,
                entered,
                &mut Context::from_waker(Waker::noop()),
                source.as_mut()
            ),
            Poll::Ready(Err(EdgeError::Internal { .. }))
        ));
    }

    #[test]
    fn caller_owned_scope_releases_on_ordinary_future_drop() {
        let scope = capture();
        assert!(!scope.is_managed());
        let releases = Rc::new(Cell::new(0_usize));
        let lease = Release(Rc::clone(&releases));
        let (entered, paired) = time();
        let retained = bind_future(
            scope,
            paired,
            entered,
            Box::pin(async move {
                let _lease = lease;
                pending::<Result<(), EdgeError>>().await
            }),
        );
        assert_eq!(releases.get(), 0);
        drop(retained);
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn response_body_shares_provider_state_but_not_callback_cancellation() {
        let callback = OutboundScopeOwner::callback();
        let callback_scope = callback.with(capture);
        let original = callback_scope
            .provider_state(|| 17_u8)
            .expect("callback provider state");
        let body = callback.with(OutboundScopeOwner::response_body);
        let body_scope = body.with(capture);
        let inherited = body_scope
            .provider_state(|| 99_u8)
            .expect("inherited provider state");
        assert!(Rc::ptr_eq(&original, &inherited));
        let mut value = Revocable::new(&body_scope, 3_u8).expect("body registration");
        body.accept_body_handoff().expect("accepted body handoff");
        callback.close();
        body_scope.check().expect("body lifetime remains open");
        assert_eq!(value.take().expect("independent body root"), 3);
        body.close();
        assert!(matches!(
            body_scope.check(),
            Err(EdgeError::Internal { .. })
        ));
    }

    #[test]
    fn owner_run_restores_current_scope_between_polls() {
        let owner = OutboundScopeOwner::callback();
        let mut polled = false;
        let mut wrapped = Box::pin(owner.run(poll_fn(move |cx| {
            assert!(capture().is_managed());
            if polled {
                Poll::Ready(7_u8)
            } else {
                polled = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        })));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(wrapped.as_mut().poll(&mut cx).is_pending());
        assert!(!capture().is_managed());
        assert_eq!(wrapped.as_mut().poll(&mut cx), Poll::Ready(7));
        assert!(!capture().is_managed());
    }

    #[test]
    fn nested_owner_cannot_resurrect_a_closed_ambient_lifetime() {
        let owner = OutboundScopeOwner::callback();
        owner.with(|| {
            owner.close();
            let nested = OutboundScopeOwner::callback();
            assert!(matches!(
                nested.scope().check(),
                Err(EdgeError::Internal { .. })
            ));
            let body = OutboundScopeOwner::response_body();
            assert!(matches!(
                body.scope().check(),
                Err(EdgeError::Internal { .. })
            ));
        });
        let fresh = OutboundScopeOwner::callback();
        fresh.scope().check().expect("next request is independent");
    }

    #[test]
    fn provider_state_rechecks_closure_after_displaced_initializer_drops() {
        let owner = Rc::new(OutboundScopeOwner::callback());
        let scope = owner.with(capture);
        let closer = Rc::clone(&owner);
        let result = scope.provider_state(|| {
            drop(
                scope
                    .provider_state(|| DropAction(None))
                    .expect("reentrant initialization"),
            );
            DropAction(Some(Box::new(move || closer.close())))
        });
        assert!(matches!(result, Err(EdgeError::Internal { .. })));
    }

    #[test]
    fn every_root_is_marked_before_payload_provider_or_waker_cleanup() {
        let owner = Rc::new(OutboundScopeOwner::callback());
        let scope = owner.with(capture);
        let count = Rc::new(Cell::new(0_usize));
        let checks = Rc::new(RefCell::new(Vec::<Box<dyn Fn() -> bool>>::new()));
        let inspect = || {
            let inspect_checks = Rc::clone(&checks);
            let inspect_count = Rc::clone(&count);
            DropAction(Some(Box::new(move || {
                assert!(inspect_checks.borrow().iter().all(|check| check()));
                inspect_count.set(inspect_count.get().saturating_add(1));
            })))
        };
        let first = Rc::new(Revocable::new(&scope, inspect()).expect("first"));
        let second = Rc::new(Revocable::new(&scope, inspect()).expect("second"));
        for value in [&first, &second] {
            let weak = Rc::downgrade(value);
            checks.borrow_mut().push(Box::new(move || {
                weak.upgrade().expect("retained root").node.revoked.get()
            }));
        }
        drop(scope.provider_state(inspect).expect("provider"));
        let waiting = waker(Arc::new(WakeAction));
        first
            .with(&mut Context::from_waker(&waiting), |_, _| ())
            .expect("register wake");
        let closer = Rc::clone(&owner);
        let first_weak = Rc::downgrade(&first);
        let second_weak = Rc::downgrade(&second);
        WAKE_ACTION.with(|stored| {
            stored.replace(Some(Box::new(move || {
                assert!(
                    first_weak
                        .upgrade()
                        .expect("first retained")
                        .node
                        .revoked
                        .get()
                );
                assert!(
                    second_weak
                        .upgrade()
                        .expect("second retained")
                        .node
                        .revoked
                        .get()
                );
                closer.close();
            })))
        });
        owner.with(|| owner.close());
        assert_eq!(count.get(), 3);
        assert!(
            scope
                .state()
                .expect("retained owner")
                .roots
                .borrow()
                .is_empty()
        );
        drop((first, second, waiting));
        assert_eq!(count.get(), 3);
    }

    #[test]
    fn dropping_replaced_waker_can_close_owner_without_borrow_panics() {
        let owner = Rc::new(OutboundScopeOwner::callback());
        let scope = owner.with(capture);
        let count = Rc::new(Cell::new(0_usize));
        let value = Revocable::new(&scope, Release(Rc::clone(&count))).expect("register");
        let waiting = waker(Arc::new(WakeAction));
        value
            .with(&mut Context::from_waker(&waiting), |_, _| ())
            .expect("register wake");
        drop(waiting);
        let closer = Rc::clone(&owner);
        DROP_ACTION.with(|stored| stored.replace(Some(Box::new(move || closer.close()))));
        let result = value.with(&mut Context::from_waker(Waker::noop()), |_, _| {
            panic!("revoked before action")
        });
        assert!(matches!(result, Err(EdgeError::Internal { .. })));
        assert_eq!(count.get(), 1);
    }

    #[test]
    fn failed_registration_drops_its_owned_input() {
        let owner = OutboundScopeOwner::callback();
        let scope = owner.with(capture);
        owner.close();
        let count = Rc::new(Cell::new(0_usize));
        let result = Revocable::new(&scope, Release(Rc::clone(&count)));
        assert!(matches!(result, Err(EdgeError::Internal { .. })));
        assert_eq!(count.get(), 1);
    }

    #[test]
    fn closing_from_foreign_scope_does_not_reparent_cleanup_work() {
        let owner = OutboundScopeOwner::callback();
        let foreign = OutboundScopeOwner::callback();
        let value = Revocable::new(
            &owner.scope(),
            DropAction(Some(Box::new(|| {
                assert!(matches!(capture().check(), Err(EdgeError::Internal { .. })));
            }))),
        )
        .expect("register cleanup");
        foreign.with(|| {
            owner.close();
            capture()
                .check()
                .expect("foreign scope restored after cleanup");
        });
        drop(value);
    }

    #[test]
    fn completed_bound_future_releases_payload_and_registration_before_ready() {
        let owner = OutboundScopeOwner::callback();
        let scope = owner.with(capture);
        let count = Rc::new(Cell::new(0_usize));
        let lease = Release(Rc::clone(&count));
        let (entered, paired) = time();
        let source = Box::pin(poll_fn(move |_| {
            let _retain = &lease;
            Poll::Ready(Ok(11_u8))
        }));
        let mut wrapped = bind_future(scope.clone(), paired, entered, source);
        assert_eq!(scope.state().expect("owner").roots.borrow().len(), 1);
        assert!(matches!(
            wrapped
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Ok(11))
        ));
        assert_eq!(count.get(), 1);
        assert!(scope.state().expect("owner").roots.borrow().is_empty());
        drop(wrapped);
        assert_eq!(count.get(), 1);
    }

    #[test]
    fn scope_run_activates_progress_before_timer_setup_without_another_root() {
        let owner = OutboundScopeOwner::callback();
        let active = Arc::new(AtomicBool::new(false));
        let polls = Rc::new(Cell::new(0_usize));
        let scope = owner.with(|| {
            OutboundScope::capture(Rc::new(Runtime {
                active: Arc::clone(&active),
                polls: Rc::clone(&polls),
            }))
        });
        let entered = MonotonicInstant::now();
        let identity = Arc::new(());
        let timer_identity = Arc::clone(&identity);
        let timer_active = Arc::clone(&active);
        let paired = OutboundTime::new(MonotonicClock::new(move || entered), move |_, _| {
            assert!(timer_active.load(Ordering::SeqCst));
            assert!(
                capture().is_managed(),
                "original scope is installed before timer factory"
            );
            let error = EdgeError::internal(TimerFault(Arc::clone(&timer_identity)));
            Box::pin(async move { Err(error) })
        });
        let deadline = paired.clock().deadline_after(Duration::from_secs(1));
        let mut timer = paired.wait_until(deadline);
        let drops = Rc::new(Cell::new(0_usize));
        let lease = Release(Rc::clone(&drops));
        let mut running = scope.run(
            paired,
            entered,
            Box::pin(poll_fn(move |cx| {
                let _keep = &lease;
                timer.as_mut().poll(cx)
            })),
        );
        assert!(scope.state().expect("owner").roots.borrow().is_empty());
        let Poll::Ready(Err(EdgeError::Internal { source, .. })) = running
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("original timer failure");
        };
        assert!(Arc::ptr_eq(
            &source.downcast_ref::<TimerFault>().expect("typed source").0,
            &identity
        ));
        assert_eq!(polls.get(), 1);
        assert_eq!(
            drops.get(),
            1,
            "terminal future releases its captures before Ready"
        );
        assert!(!active.load(Ordering::SeqCst));
        assert!(!capture().is_managed());
        assert!(scope.state().expect("owner").roots.borrow().is_empty());
    }

    #[test]
    fn foreign_poll_retains_original_scope_and_restores_the_callers_scope() {
        let original = OutboundScopeOwner::callback();
        let foreign = OutboundScopeOwner::callback();
        let scope = original.with(capture);
        let expected = scope.provider_state(|| 3_u8).expect("original provider");
        let other = foreign
            .scope()
            .provider_state(|| 7_u8)
            .expect("foreign provider");
        let (entered, paired) = time();
        let mut source = Box::pin(async {
            let observed = capture()
                .provider_state(|| 11_u8)
                .expect("captured original provider");
            assert!(Rc::ptr_eq(&observed, &expected));
        });
        foreign.with(|| {
            assert!(matches!(
                scope.poll(
                    &paired,
                    entered,
                    &mut Context::from_waker(Waker::noop()),
                    source.as_mut()
                ),
                Poll::Ready(Ok(()))
            ));
            let restored = capture()
                .provider_state(|| 19_u8)
                .expect("restored foreign provider");
            assert!(Rc::ptr_eq(&restored, &other));
        });
        assert!(!capture().is_managed());
    }

    #[test]
    fn invalid_runtime_never_double_polls_or_discards_a_ready_output_as_pending() {
        for mode in 0_u8..3 {
            let owner = OutboundScopeOwner::callback();
            let scope = owner.with(|| OutboundScope::capture(Rc::new(InvalidRuntime(mode))));
            let (entered, paired) = time();
            let polls = Rc::new(Cell::new(0_usize));
            let observed = Rc::clone(&polls);
            let mut source = Box::pin(poll_fn(move |_| {
                observed.set(observed.get().saturating_add(1));
                assert_eq!(observed.get(), 1, "a completed future is never repolled");
                Poll::Ready(7_u8)
            }));
            assert!(matches!(
                scope.poll(
                    &paired,
                    entered,
                    &mut Context::from_waker(Waker::noop()),
                    source.as_mut()
                ),
                Poll::Ready(Err(EdgeError::Internal { .. }))
            ));
            assert!(!capture().is_managed());
        }
    }

    #[test]
    fn a_reentrant_value_borrow_fails_without_mutating_the_outer_value() {
        let owner = OutboundScopeOwner::callback();
        let value = Revocable::new(&owner.scope(), 3_u8).expect("register");
        let result = value.with(&mut Context::from_waker(Waker::noop()), |number, cx| {
            assert!(matches!(
                value.with(cx, |_, _| ()),
                Err(EdgeError::Internal { .. })
            ));
            *number = 7;
        });
        result.expect("outer operation remains valid");
        assert_eq!(
            value
                .with(&mut Context::from_waker(Waker::noop()), |number, _| *number)
                .expect("value retained"),
            7
        );
    }

    #[cfg(panic = "unwind")]
    #[test]
    fn panicking_payload_cleanup_still_releases_retained_siblings_and_provider_state() {
        let owner = OutboundScopeOwner::callback();
        let scope = owner.scope();
        let first = Revocable::new(
            &scope,
            DropAction(Some(Box::new(|| panic!("application stream drop")))),
        )
        .expect("first root");
        let sibling_releases = Rc::new(Cell::new(0_usize));
        let second = Revocable::new(&scope, Release(Rc::clone(&sibling_releases)))
            .expect("retained sibling");
        let provider_releases = Rc::new(Cell::new(0_usize));
        drop(
            scope
                .provider_state(|| Release(Rc::clone(&provider_releases)))
                .expect("provider state"),
        );
        assert!(catch_unwind(AssertUnwindSafe(|| owner.close())).is_err());
        assert!(first.is_revoked());
        assert!(second.is_revoked());
        assert_eq!(
            sibling_releases.get(),
            1,
            "a retained sibling cannot delay release during unwind"
        );
        assert_eq!(provider_releases.get(), 1);
        assert!(!capture().is_managed(), "ambient scope restores on unwind");
        drop((first, second));
        assert_eq!(sibling_releases.get(), 1);
    }

    #[test]
    fn callback_helpers_reuse_managed_owners_including_closed_ambient_scopes() {
        let owner = OutboundScopeOwner::callback_if_absent().expect("outer callback");
        let expected = owner
            .scope()
            .provider_state(|| 3_u8)
            .expect("shared provider state");
        let releases = Rc::new(Cell::new(0_usize));
        let retained = owner.with(|| {
            assert!(OutboundScopeOwner::callback_if_absent().is_none());
            let nested = capture();
            let observed = nested.provider_state(|| 7_u8).expect("same owner");
            assert!(Rc::ptr_eq(&expected, &observed));
            Revocable::new(&nested, Release(Rc::clone(&releases))).expect("same registry")
        });
        owner.with(|| {
            owner.close();
            assert!(OutboundScopeOwner::callback_if_absent().is_none());
            assert!(matches!(capture().check(), Err(EdgeError::Internal { .. })));
        });
        assert!(retained.is_revoked());
        assert_eq!(releases.get(), 1);
        assert!(OutboundScopeOwner::callback_if_absent().is_some());
        capture()
            .with(|| {
                let canonical = OutboundScopeOwner::callback_if_absent()
                    .expect("manual invocation is not a canonical boundary");
                assert!(canonical.scope().is_managed());
            })
            .expect("manual activation");
    }

    #[test]
    fn response_body_family_resources_do_not_retain_callback_or_roots() {
        let callback = OutboundScopeOwner::callback();
        let callback_scope = callback.scope();
        let callback_state = Rc::downgrade(&callback.state);
        let provider_releases = Rc::new(Cell::new(0_usize));
        let resource = callback_scope
            .provider_state(|| Release(Rc::clone(&provider_releases)))
            .expect("callback provider");
        let identity = Rc::downgrade(&resource);
        drop(resource);
        let root_releases = Rc::new(Cell::new(0_usize));
        let retained = Revocable::new(&callback_scope, Release(Rc::clone(&root_releases)))
            .expect("callback root");
        let body = callback.with(OutboundScopeOwner::response_body);
        body.accept_body_handoff().expect("accepted body handoff");
        callback.close();
        drop(callback);
        assert_eq!(root_releases.get(), 1);
        assert!(retained.is_revoked());
        assert!(callback_state.upgrade().is_none());
        assert_eq!(provider_releases.get(), 0);
        let inherited = body
            .scope()
            .provider_state::<Release>(|| panic!("handoff must not initialize a second provider"))
            .expect("shared family provider");
        assert!(Rc::ptr_eq(
            &inherited,
            &identity.upgrade().expect("retained provider identity")
        ));
        drop(inherited);
        body.close();
        assert_eq!(provider_releases.get(), 1);
        assert!(identity.upgrade().is_none());
    }

    #[test]
    fn response_body_first_provider_initialization_is_shared_with_callback_and_descendants() {
        let callback = OutboundScopeOwner::callback();
        let body = callback.with(OutboundScopeOwner::response_body);
        let closest = body.scope().provider_state(|| 2_u8).expect("body provider");
        let callback_resource = callback
            .scope()
            .provider_state::<u8>(|| panic!("body already initialized the family provider"))
            .expect("callback reuses body-first provider");
        assert!(Rc::ptr_eq(&callback_resource, &closest));
        let ancestor_only = callback
            .scope()
            .provider_state(|| 17_u16)
            .expect("ancestor-only provider");
        let descendant = body.with(OutboundScopeOwner::response_body);
        body.accept_body_handoff()
            .expect("accepted outer body handoff");
        descendant
            .accept_body_handoff()
            .expect("accepted nested body handoff");
        callback.close();
        body.close();
        let scope = descendant.scope();
        let inherited = scope
            .provider_state::<u8>(|| panic!("closest provider already exists"))
            .expect("closest provider retained");
        let inherited_ancestor = scope
            .provider_state::<u16>(|| panic!("ancestor provider already exists"))
            .expect("ancestor-only provider retained");
        assert!(Rc::ptr_eq(&inherited, &closest));
        assert!(Rc::ptr_eq(&inherited_ancestor, &ancestor_only));
        let fresh = OutboundScopeOwner::callback();
        let unrelated = fresh.scope().provider_state(|| 99_u8).expect("new family");
        assert!(!Rc::ptr_eq(&unrelated, &closest));
        assert_eq!(*unrelated, 99);
    }

    #[test]
    fn last_family_lease_closes_resources_even_while_scope_tokens_are_retained() {
        let callback = OutboundScopeOwner::callback();
        let body = callback.with(OutboundScopeOwner::response_body);
        let callback_scope = callback.scope();
        let body_scope = body.scope();
        let releases = Rc::new(Cell::new(0_usize));
        drop(
            body_scope
                .provider_state(|| Release(Rc::clone(&releases)))
                .expect("body-first provider"),
        );
        body.close();
        assert_eq!(releases.get(), 0, "callback still holds a family lease");
        callback.close();
        assert_eq!(
            releases.get(),
            1,
            "last owner closure releases the family cache"
        );
        assert!(matches!(
            callback_scope.check(),
            Err(EdgeError::Internal { .. })
        ));
        assert!(matches!(
            body_scope.check(),
            Err(EdgeError::Internal { .. })
        ));
    }

    #[test]
    fn response_attachment_transfers_the_exact_owner_without_polling_or_adoption() {
        let owner = OutboundScopeOwner::response_body();
        let scope = owner.scope();
        let identity = Rc::downgrade(&owner.state);
        let releases = Rc::new(Cell::new(0_usize));
        let retained = Revocable::new(&scope, Release(Rc::clone(&releases)))
            .expect("precreated body operation");
        let mut response = owner.attach(Response::new(Body::empty())).expect("attach");
        assert_eq!(releases.get(), 0);
        let transferred = response
            .body_mut()
            .take_egress_owner()
            .expect("exact attached body owner");
        assert!(Rc::ptr_eq(
            &transferred.state,
            &identity.upgrade().expect("same owner state")
        ));
        assert!(response.body_mut().take_egress_owner().is_none());
        drop(response);
        assert_eq!(releases.get(), 0);
        transferred.close();
        assert_eq!(releases.get(), 1);
        assert!(retained.is_revoked());
    }

    #[test]
    fn dropping_unhanded_response_closes_precreated_unpolled_body_work() {
        let owner = OutboundScopeOwner::response_body();
        let scope = owner.scope();
        let releases = Rc::new(Cell::new(0_usize));
        let lease = Release(Rc::clone(&releases));
        let (entered, paired) = time();
        let retained = bind_future(
            scope.clone(),
            paired,
            entered,
            Box::pin(async move {
                let _keep = lease;
                pending::<Result<(), EdgeError>>().await
            }),
        );
        let response = owner.attach(Response::new(Body::empty())).expect("attach");
        assert_eq!(releases.get(), 0);
        drop(response);
        assert_eq!(releases.get(), 1);
        assert!(matches!(scope.check(), Err(EdgeError::Internal { .. })));
        assert!(matches!(
            block_on(retained),
            Err(EdgeError::Internal { .. })
        ));
    }

    #[test]
    fn response_attachment_rejects_callback_closed_and_duplicate_owners() {
        let callback = OutboundScopeOwner::callback();
        assert!(matches!(
            callback.attach(Response::new(Body::empty())),
            Err(EdgeError::Internal { .. })
        ));
        let closed = OutboundScopeOwner::response_body();
        closed.close();
        assert!(matches!(
            closed.attach(Response::new(Body::empty())),
            Err(EdgeError::Internal { .. })
        ));
        let first = OutboundScopeOwner::response_body();
        let first_scope = first.scope();
        let response = first
            .attach(Response::new(Body::empty()))
            .expect("first attachment");
        let duplicate = OutboundScopeOwner::response_body();
        let duplicate_scope = duplicate.scope();
        assert!(matches!(
            duplicate.attach(response),
            Err(EdgeError::Internal { .. })
        ));
        assert!(matches!(
            first_scope.check(),
            Err(EdgeError::Internal { .. })
        ));
        assert!(matches!(
            duplicate_scope.check(),
            Err(EdgeError::Internal { .. })
        ));
    }

    #[test]
    fn retained_provisional_body_is_closed_before_parent_cleanup_can_observe_it() {
        let callback = OutboundScopeOwner::callback();
        let observed = Rc::new(RefCell::new(None::<OutboundScope>));
        let observed_in_drop = Rc::clone(&observed);
        let releases = Rc::new(Cell::new(0_usize));
        let releases_in_drop = Rc::clone(&releases);
        let earlier = Revocable::new(
            &callback.scope(),
            DropAction(Some(Box::new(move || {
                let child = observed_in_drop.borrow().clone().expect("child scope");
                assert!(matches!(child.check(), Err(EdgeError::Internal { .. })));
                assert_eq!(releases_in_drop.get(), 0, "parent guard has not run yet");
            }))),
        )
        .expect("earlier callback root");
        let body = callback.with(OutboundScopeOwner::response_body);
        observed.replace(Some(body.scope()));
        let retained = Revocable::new(&body.scope(), Release(Rc::clone(&releases)))
            .expect("provisional body operation");
        callback.close();
        assert_eq!(releases.get(), 1);
        assert!(retained.is_revoked());
        assert!(matches!(
            body.scope().check(),
            Err(EdgeError::Internal { .. })
        ));
        assert!(matches!(
            body.accept_body_handoff(),
            Err(EdgeError::Internal { .. })
        ));
        drop(earlier);
    }

    #[test]
    fn accepted_body_handoff_unregisters_parent_guard_and_preserves_child_root_identity() {
        let callback = OutboundScopeOwner::callback();
        let body = callback.with(OutboundScopeOwner::response_body);
        let scope = body.scope();
        let releases = Rc::new(Cell::new(0_usize));
        let retained =
            Revocable::new(&scope, Release(Rc::clone(&releases))).expect("precreated body root");
        let identity = Rc::downgrade(&retained.node);
        assert_eq!(callback.state.roots.borrow().len(), 1, "one parent guard");
        body.accept_body_handoff()
            .expect("framework accepts body handoff");
        assert!(callback.state.roots.borrow().is_empty());
        callback.close();
        scope
            .check()
            .expect("accepted body lifetime is independent");
        assert_eq!(releases.get(), 0);
        assert!(Rc::ptr_eq(
            &retained.node,
            &identity.upgrade().expect("same root")
        ));
        body.close();
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn attached_response_is_still_provisional_until_framework_acceptance() {
        let callback = OutboundScopeOwner::callback();
        let body = callback.with(OutboundScopeOwner::response_body);
        let scope = body.scope();
        let releases = Rc::new(Cell::new(0_usize));
        let retained = Revocable::new(&scope, Release(Rc::clone(&releases)))
            .expect("body work before attachment");
        let response = body
            .attach(Response::new(Body::empty()))
            .expect("attach only");
        callback.close();
        assert_eq!(releases.get(), 1);
        assert!(retained.is_revoked());
        assert!(matches!(scope.check(), Err(EdgeError::Internal { .. })));
        drop(response);
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn callback_drop_revokes_externally_retained_provisional_owner_without_a_strong_parent() {
        let callback = OutboundScopeOwner::callback();
        let parent_identity = Rc::downgrade(&callback.state);
        let retained_owner = callback.with(OutboundScopeOwner::response_body);
        let releases = Rc::new(Cell::new(0_usize));
        let retained_work = Revocable::new(&retained_owner.scope(), Release(Rc::clone(&releases)))
            .expect("externally retained work");
        drop(callback);
        assert!(parent_identity.upgrade().is_none());
        assert_eq!(releases.get(), 1);
        assert!(retained_work.is_revoked());
        assert!(matches!(
            retained_owner.scope().check(),
            Err(EdgeError::Internal { .. })
        ));
        drop(retained_owner);
        assert_eq!(releases.get(), 1);
    }

    #[test]
    fn revocable_cleanup_explicit_and_implicit_drop_keep_captured_scope_identity() {
        for implicit in [false, true] {
            let owner = OutboundScopeOwner::response_body();
            let scope = owner.scope();
            let foreign = OutboundScopeOwner::callback();
            let observed = Rc::new(RefCell::new(None));
            let captured_on_drop = Rc::clone(&observed);
            let value = Revocable::new(
                &scope,
                DropAction(Some(Box::new(move || {
                    captured_on_drop.replace(Some(capture()));
                }))),
            )
            .expect("register owned value");
            foreign.with(|| {
                if implicit {
                    drop(Some(value));
                } else {
                    drop(value);
                }
                assert!(Weak::ptr_eq(
                    &capture().current.state,
                    &foreign.scope().current.state
                ));
            });
            let retained = observed.borrow_mut().take().expect("destructor capture");
            assert!(Weak::ptr_eq(&retained.current.state, &scope.current.state));
            retained.check().expect("original body is still live");
            owner.close();
            assert!(matches!(retained.check(), Err(EdgeError::Internal { .. })));
            assert!(!capture().is_managed());
        }
    }

    #[test]
    fn revocable_cleanup_revoked_active_value_and_output_keep_closed_captured_scope() {
        let owner = OutboundScopeOwner::response_body();
        let scope = owner.scope();
        let foreign = OutboundScopeOwner::callback();
        let input_observed = Rc::new(RefCell::new(None));
        let input_capture = Rc::clone(&input_observed);
        let value = Revocable::new(
            &scope,
            DropAction(Some(Box::new(move || {
                input_capture.replace(Some(capture()));
            }))),
        )
        .expect("register input");
        let output_observed = Rc::new(RefCell::new(None));
        let output_capture = Rc::clone(&output_observed);
        foreign.with(|| {
            let result = value.with(&mut Context::from_waker(Waker::noop()), |_, _| {
                owner.close();
                DropAction(Some(Box::new(move || {
                    output_capture.replace(Some(capture()));
                })))
            });
            assert!(matches!(result, Err(EdgeError::Internal { .. })));
            assert!(Weak::ptr_eq(
                &capture().current.state,
                &foreign.scope().current.state
            ));
        });
        for observed in [input_observed, output_observed] {
            let retained = observed
                .borrow_mut()
                .take()
                .expect("closed destructor capture");
            assert!(Weak::ptr_eq(&retained.current.state, &scope.current.state));
            assert!(matches!(retained.check(), Err(EdgeError::Internal { .. })));
        }
    }

    #[test]
    fn revocable_cleanup_failed_registration_keeps_closed_captured_scope() {
        let owner = OutboundScopeOwner::response_body();
        let scope = owner.scope();
        owner.close();
        let foreign = OutboundScopeOwner::callback();
        let observed = Rc::new(RefCell::new(None));
        let captured_on_drop = Rc::clone(&observed);
        foreign.with(|| {
            let failed = Revocable::new(
                &scope,
                DropAction(Some(Box::new(move || {
                    captured_on_drop.replace(Some(capture()));
                }))),
            );
            assert!(matches!(failed, Err(EdgeError::Internal { .. })));
        });
        let retained = observed
            .borrow_mut()
            .take()
            .expect("failed input destructor capture");
        assert!(Weak::ptr_eq(&retained.current.state, &scope.current.state));
        assert!(matches!(retained.check(), Err(EdgeError::Internal { .. })));
    }
}
