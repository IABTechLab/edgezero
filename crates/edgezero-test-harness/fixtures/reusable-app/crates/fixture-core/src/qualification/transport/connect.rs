use super::*;

fn connect_request(uri: Uri, token: &str) -> Result<OutboundRequest, EdgeError> {
    Ok(request(uri, token, "connect-timeout")?
        .timeout(Duration::from_secs(2))
        .connect_timeout(Duration::from_millis(200)))
}

pub(super) async fn run(
    client: &HttpClient,
    native: &dyn OutboundHttpClient,
    first: &Uri,
    second: &Uri,
    token: &str,
) -> Result<[Value; 3], EdgeError> {
    let time = native.time();
    let clock = time.clock();
    let epoch = clock.now();
    let control = |role: &str| -> Result<OutboundRequest, EdgeError> {
        let uri = format!(
            "http://{}/lifetimes/{token}/connect-timeout/{role}",
            second.authority().ok_or_else(invalid)?
        )
        .parse()
        .map_err(|_| invalid())?;
        request(
            uri,
            token,
            if role == "start" {
                "connect-start"
            } else {
                "recovery"
            },
        )?
        .timeout(Duration::from_secs(1))
        .header(
            "x-fixture-at-us",
            clock
                .now()
                .checked_duration_since(epoch)
                .ok_or_else(invalid)?
                .as_micros()
                .to_string(),
        )
    };
    let initial = send_step(client, control("start")?, "connect-start", None).await?;
    let uri = target(first, "plain", token)?;
    let mut outgoing = connect_request(uri.clone(), token)?;
    let observation = NativeObservation::attach(&mut outgoing)?;
    let started = clock.now();
    let outcome = client.send(outgoing).await;
    let finished = clock.now();
    let elapsed = u64::try_from(
        finished
            .checked_duration_since(started)
            .ok_or_else(invalid)?
            .as_micros(),
    )
    .map_err(EdgeError::internal)?;
    let fault = json!({"step":"connect-timeout","uri":uri.to_string(),
        "settings":{"timeout_ms":2000,"connect_ms":200},"elapsed_us":elapsed,
        "outcome":response(outcome).await?,"native":observation.report(),"before":null,"after":null});
    let recovered = send_step(client, control("done")?, "recovery", None).await?;
    Ok([initial, fault, recovered])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn silent_connect_probe_preserves_literal_phase_and_aggregate_budgets() {
        let target = "http://127.0.0.1:1234/transport/plain/control"
            .parse()
            .unwrap();
        let parts = connect_request(target, "control").unwrap().into_parts();
        assert_eq!(parts.connect_timeout, Some(Duration::from_millis(200)));
        assert_eq!(parts.timeout, Some(Duration::from_secs(2)));
        assert_eq!(parts.headers["x-request-token"], "control");
        assert_eq!(parts.headers["x-transport-step"], "connect-timeout");
        assert_eq!(parts.max_decoded_response_bytes, Some(64));
    }
}
