#![allow(
    unsafe_code,
    reason = "the status-returning Fastly ABI avoids panic-on-error SDK response wrappers"
)]

use fastly_shared::{
    BodyWriteEnd, FastlyStatus, FramingHeadersMode, INVALID_BODY_HANDLE, INVALID_RESPONSE_HANDLE,
};
use fastly_sys::{fastly_http_body, fastly_http_resp};
#[cfg(all(test, target_arch = "wasm32"))]
use std::cell::RefCell;
#[cfg(all(test, target_arch = "wasm32"))]
use std::mem;

// These wrappers have one guest owner. The surrounding prepared response/sink owns cleanup;
// the wrappers deliberately have no Drop implementation or infallible SDK conversion.
#[derive(Debug)]
pub(super) struct BodyHandle(fastly_sys::BodyHandle);

#[derive(Debug)]
pub(super) struct ResponseHandle(fastly_sys::ResponseHandle);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct FastlyAbiError;

impl BodyHandle {
    fn into_raw(self) -> fastly_sys::BodyHandle {
        self.0
    }
}

impl ResponseHandle {
    fn into_raw(self) -> fastly_sys::ResponseHandle {
        self.0
    }
}

#[cfg(all(test, target_arch = "wasm32"))]
std::thread_local! {
    static CLEANUP_CALLS: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
}

#[cfg(all(test, target_arch = "wasm32"))]
pub(super) fn take_cleanup_calls() -> Vec<&'static str> {
    CLEANUP_CALLS.with(|calls| mem::take(&mut *calls.borrow_mut()))
}

pub(super) fn abandon_body(body: BodyHandle) -> Result<(), FastlyAbiError> {
    #[cfg(all(test, target_arch = "wasm32"))]
    CLEANUP_CALLS.with(|calls| calls.borrow_mut().push("abandon-body"));
    // SAFETY: the guest owner relinquishes this handle for its sole abandonment attempt.
    result(unsafe { fastly_http_body::abandon(body.into_raw()) })
}

pub(super) fn append_header(
    response: &ResponseHandle,
    name: &[u8],
    value: &[u8],
) -> Result<(), FastlyAbiError> {
    // SAFETY: both byte slices remain live for the duration of the synchronous hostcall.
    result(unsafe {
        fastly_http_resp::header_append(
            response.0,
            name.as_ptr(),
            name.len(),
            value.as_ptr(),
            value.len(),
        )
    })
}

pub(super) fn close_body(body: BodyHandle) -> Result<(), FastlyAbiError> {
    #[cfg(all(test, target_arch = "wasm32"))]
    CLEANUP_CALLS.with(|calls| calls.borrow_mut().push("close-body"));
    // SAFETY: the guest owner relinquishes this handle for its sole close attempt.
    result(unsafe { fastly_http_body::close(body.into_raw()) })
}

pub(super) fn close_response(response: ResponseHandle) -> Result<(), FastlyAbiError> {
    #[cfg(all(test, target_arch = "wasm32"))]
    CLEANUP_CALLS.with(|calls| calls.borrow_mut().push("close-response"));
    // SAFETY: `response` is an owned, live response handle and this call consumes it.
    result(unsafe { fastly_http_resp::close(response.into_raw()) })
}

pub(super) fn new_body() -> Result<BodyHandle, FastlyAbiError> {
    let mut handle = INVALID_BODY_HANDLE;
    // SAFETY: the output pointer is valid and points to initialized `u32` storage.
    result(unsafe { fastly_http_body::new(&raw mut handle) })?;
    Ok(BodyHandle(handle))
}

pub(super) fn new_response() -> Result<ResponseHandle, FastlyAbiError> {
    let mut handle = INVALID_RESPONSE_HANDLE;
    // SAFETY: the output pointer is valid and points to initialized `u32` storage.
    result(unsafe { fastly_http_resp::new(&raw mut handle) })?;
    Ok(ResponseHandle(handle))
}

fn result(status: FastlyStatus) -> Result<(), FastlyAbiError> {
    status.result().map_err(|_status| FastlyAbiError)
}

pub(super) fn send_buffered(
    response: ResponseHandle,
    body: BodyHandle,
) -> Result<(), FastlyAbiError> {
    // SAFETY: both exclusively owned handles are relinquished to this send attempt. Failure
    // is an unknown commitment, not permission to retry or close possibly consumed handles.
    result(unsafe { fastly_http_resp::send_downstream(response.into_raw(), body.into_raw(), 0) })
}

pub(super) fn send_streaming(
    response: ResponseHandle,
    body: &BodyHandle,
) -> Result<(), FastlyAbiError> {
    // SAFETY: the response is relinquished, but the streaming body stays guest-owned for
    // writes and one terminal close/abandon attempt. Pinned Viceroy transitions that body
    // in place; this does not establish hosted handle validity after every error status.
    result(unsafe { fastly_http_resp::send_downstream(response.into_raw(), body.0, 1) })
}

pub(super) fn set_manual_framing(response: &ResponseHandle) -> Result<(), FastlyAbiError> {
    // SAFETY: `response` is a live, exclusively owned response handle.
    result(unsafe {
        fastly_http_resp::framing_headers_mode_set(
            response.0,
            FramingHeadersMode::ManuallyFromHeaders,
        )
    })
}

pub(super) fn set_status(response: &ResponseHandle, status: u16) -> Result<(), FastlyAbiError> {
    // SAFETY: `response` is a live, exclusively owned response handle.
    result(unsafe { fastly_http_resp::status_set(response.0, status) })
}

pub(super) fn write_body(body: &BodyHandle, bytes: &[u8]) -> Result<usize, FastlyAbiError> {
    let mut written = 0;
    // SAFETY: `body` is live, `bytes` remains live during the synchronous hostcall, and the
    // output pointer refers to initialized `usize` storage.
    result(unsafe {
        fastly_http_body::write(
            body.0,
            bytes.as_ptr(),
            bytes.len(),
            BodyWriteEnd::Back,
            &raw mut written,
        )
    })?;
    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::{FastlyAbiError, FastlyStatus, result};

    #[test]
    fn status_result_preserves_success_and_collapses_host_failure() {
        assert_eq!(result(FastlyStatus::OK), Ok(()));
        assert_eq!(result(FastlyStatus::ERROR), Err(FastlyAbiError));
    }
}
