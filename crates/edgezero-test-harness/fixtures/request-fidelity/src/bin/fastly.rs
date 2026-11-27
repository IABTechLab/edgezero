use edgezero_adapter_fastly::request::send_request_with_registries_and_hooks;
use edgezero_core::app::{App, StoresMetadata};
use fastly::{Error, Request, Response};
use request_fidelity::{Fixture, NativeClient};

fn main() -> Result<(), Error> {
    let req = Request::from_client();
    let client_ip = req.get_client_ip_addr();
    // This generic shortcut deliberately demonstrates the SDK normalization gate.
    if req.get_path() == "/native" {
        Response::from_body(
            serde_json::json!({"native":true,"raw_target_supported":false}).to_string(),
        )
        .send_to_client();
        return Ok(());
    }
    // The snapshot must keep the original origin through later native mutation.
    let app = App::build::<Fixture>(edgezero_adapter_fastly::FASTLY_PLATFORM)?;
    let delivery = send_request_with_registries_and_hooks(
        &app,
        StoresMetadata::default(),
        req,
        |native, extensions| {
            if native.get_header_str("x-fixture") == Some("snapshot-origin") {
                native.set_url("https://spoof.example.com/changed");
                native.set_header("host", "spoof.example.com");
            }
            extensions.insert(NativeClient(client_ip));
        },
        |_response| (),
    );
    if delivery
        .delivery()
        .is_some_and(|result| result.error().is_none())
    {
        Ok(())
    } else {
        Err(Error::msg("request fidelity fixture delivery failed"))
    }
}
