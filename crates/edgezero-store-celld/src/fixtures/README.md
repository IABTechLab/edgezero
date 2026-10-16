# Disposable TLS fixtures

These DER certificates and the PKCS#8 key are public, test-only material. Never use
this key or trust this root outside the loopback test fixture.

- `tls-valid.der`: SAN `127.0.0.1`, valid from 2020 through 2040.
- `tls-wrong.der`: SAN `127.0.0.2`, valid from 2020 through 2040.
- `tls-expired.der`: SAN `127.0.0.1`, expired January 2020.
- `tls-ca.der`: disposable signing root; its private key was deleted.
- `tls-key.der`: shared disposable P-256 leaf key.

Generated locally with OpenSSL. Runtime tests use the normal rustls server and
production reqwest policy; their private seam changes only the trusted root.
No OpenSSL executable, system trust changes or external service is needed to run
these tests. Replace the valid leaves before 2040 if these tests are maintained
that long.
