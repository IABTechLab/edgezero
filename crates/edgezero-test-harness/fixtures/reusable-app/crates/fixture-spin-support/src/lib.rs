//! Shared guest scenarios; the native harness supplies the runtime and owns processes.

#![cfg(target_arch = "wasm32")]

pub mod outbound;
pub mod response;

#[cfg(test)]
mod tests {
    mod ingress_contract {
        use crate::outbound;
        use futures::executor::block_on;

        #[test]
        fn deferred_request_and_response_paths_retain_the_injected_clock() {
            assert!(block_on(outbound::deferred_clock_paths_hold()));
        }

        #[test]
        fn converted_response_retains_injected_clock() {
            assert!(block_on(
                outbound::converted_response_retains_injected_clock()
            ));
        }

        #[test]
        fn converted_response_preserves_original_content_type_provenance() {
            assert!(block_on(
                outbound::converted_response_content_type_provenance_holds()
            ));
        }
    }

    mod sdk_resources {
        use crate::outbound;
        use futures::executor::block_on;

        #[test]
        fn response_conversion_checks_deadline_before_handoff() {
            assert!(
                block_on(outbound::deferred_clock_paths_hold()),
                "response handoff must retain deadlines and release SDK body consumers"
            );
        }

        #[test]
        fn provider_diagnostics_survive_ready_deadlines() {
            assert!(block_on(outbound::provider_deadline_diagnostics_hold()));
        }
    }

    mod clock_tests {
        use crate::outbound;
        use spin_sdk::wasip3::wit_bindgen::block_on;

        #[test]
        fn response_conversion_checks_deadline_before_handoff() {
            block_on(outbound::handoff_tests::check_response_handoff()).expect("handoff deadlines");
        }

        #[test]
        fn converted_response_retains_injected_clock() {
            assert!(block_on(
                outbound::converted_response_retains_injected_clock()
            ));
        }

        #[test]
        fn converted_response_preserves_original_content_type_provenance() {
            block_on(
                outbound::content_type_tests::check_converted_response_content_type_provenance(),
            )
            .expect("converted content-type provenance");
        }
    }
}
