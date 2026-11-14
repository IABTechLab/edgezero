//! The production pump against live SDK peers, within the host's real async task.
#![cfg(target_arch = "wasm32")]

use std::cell::Cell;
use std::rc::Rc;
use std::task::Poll;

use async_stream::stream;
use bytes::Bytes;
use edgezero_adapter_spin::outbound::test_support::pump_request_body;
use edgezero_core::http::{Method, request_builder};
use edgezero_core::time::dispatch_budget;
use edgezero_core::{Body, EdgeError, MonotonicClock, OutboundRequest, OutboundTime};
use futures_util::future::{join, poll_fn};
use futures_util::task::AtomicWaker;
use serde_json::{Value, json};
use spin_sdk::wasip3::http_compat::BodyWriter;

struct Released(Rc<Cell<u64>>);
impl Drop for Released {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

pub async fn run(case: &str) -> anyhow::Result<Value> {
    anyhow::ensure!(
        matches!(case, "short" | "long" | "cap" | "source" | "closed"),
        "invalid upload case"
    );
    let drops = Rc::new(Cell::new(0));
    let source_owner = Released(Rc::clone(&drops));
    let polls = Rc::new(Cell::new(0));
    let source_polls = Rc::clone(&polls);
    let prefix_seen = Rc::new(Cell::new(0_usize));
    let source_prefix = Rc::clone(&prefix_seen);
    let prefix_waker = Rc::new(AtomicWaker::new());
    let source_waker = Rc::clone(&prefix_waker);
    let case = case.to_owned();
    let source_case = case.clone();
    let source = stream! {
        let _owner = source_owner;
        source_polls.set(source_polls.get() + 1);
        yield Ok(Bytes::from_static(if source_case == "cap" {b"abcde"} else {b"abc"}));
        // The next input chunk does not exist until the live peer has received
        // the first one: a prebuffering implementation cannot complete this test.
        poll_fn(|cx| {
            source_waker.register(cx.waker());
            if source_prefix.get() >= 3 { Poll::Ready(()) } else { Poll::Pending }
        }).await;
        source_polls.set(source_polls.get() + 1);
        match source_case.as_str() {
            "long" => yield Ok(Bytes::from_static(b"de")),
            "source" => yield Err(EdgeError::request_timeout("upload fixture source timeout")),
            _ => {}
        }
        assert!(!matches!(source_case.as_str(), "long" | "source"), "source polled after terminal upload error");
    };
    let inbound = request_builder()
        .method(Method::POST)
        .header("content-length", "4")
        .body(Body::from_stream(source))?;
    let request = OutboundRequest::from_request(inbound, "http://example.com/upload".parse()?)?;
    let clock = MonotonicClock::default();
    let budget = dispatch_budget(&request, clock.now(), None)?;
    let time = OutboundTime::new(clock, |_, _| Box::pin(async { Ok(()) }));
    let (writer, reader, completion) = BodyWriter::new();
    let holds_completion = matches!(case.as_str(), "short" | "long");
    // This runtime forbids intra-component transfer of resource-bearing future
    // payloads. Keep this reader unpolled for the local-failure proof, and close
    // it up front for tests that intentionally publish a typed completion error.
    let held_completion = if holds_completion {
        Some(completion)
    } else {
        drop(completion);
        None
    };
    let done = Rc::new(Cell::new(false));
    let done_waker = Rc::new(AtomicWaker::new());
    let pumped = Rc::clone(&done);
    let pump_waker = Rc::clone(&done_waker);
    let maximum = if case == "cap" { 2 } else { 8 };
    let pump = async {
        let result =
            pump_request_body(request.into_parts().body, maximum, writer, budget, time).await;
        pumped.set(true);
        pump_waker.wake();
        result
    };
    let receive = async {
        let mut bytes = Vec::new();
        if case == "closed" {
            drop(reader);
        } else {
            let mut reader = reader;
            loop {
                let (_state, chunk) = reader.read(Vec::with_capacity(16)).await;
                if chunk.is_empty() {
                    break;
                }
                bytes.extend_from_slice(&chunk);
                assert!(bytes.len() <= 4, "rejected upload chunk reached peer");
                prefix_seen.set(bytes.len());
                prefix_waker.wake();
            }
        }
        if holds_completion {
            // Keep the completion reader live but unpolled until local failure.
            // Waiting for its acknowledgment would deadlock and fail the HTTP gate.
            poll_fn(|cx| {
                done_waker.register(cx.waker());
                if done.get() {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
        }
        drop(held_completion);
        bytes
    };
    let (result, bytes) = join(pump, receive).await;
    let (status, complete, provider_detail) = match result {
        Ok(complete) => (200, complete, false),
        Err(error) => (
            error.status().as_u16(),
            false,
            error.provider_failure_detail().is_some(),
        ),
    };
    Ok(
        json!({"case":case,"status":status,"complete":complete,"held_completion_until_failure":holds_completion,
        "prefix":String::from_utf8(bytes)?,"source_steps":polls.get(),"source_drops":drops.get(),
        "provider_detail":provider_detail,"qualification":false}),
    )
}
