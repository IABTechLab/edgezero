use serde_json::{Value, json};

pub(super) fn snapshot() -> Value {
    let snapshot = edgezero_adapter_fastly::context::FastlyOutboundObservation::current()
        .expect("canonical native transport callback")
        .snapshot();
    json!({"configurations":snapshot.configuration_count(),"registrations":snapshot.registration_count()})
}
