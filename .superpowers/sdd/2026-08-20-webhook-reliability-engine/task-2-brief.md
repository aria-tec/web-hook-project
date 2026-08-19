# Task 2 Brief: Cryptographic HMAC Signing & SSRF Protection Engine

## Plan Context
- Spec: `docs/superpowers/specs/2026-08-20-webhook-engine-design.md`
- Plan: `docs/superpowers/plans/2026-08-20-webhook-reliability-engine.md` (Task 2)

## Requirements
1. **HMAC-SHA256 Cryptographic Engine (`internal/dispatcher/hmac.go`):**
   - `SignPayload(secret string, timestamp int64, payload []byte) string`: signs `timestamp + "." + payload` using HMAC-SHA256, returns header string format `t=<timestamp>,v1=<hex_signature>`.
   - `VerifySignature(secret string, header string, payload []byte, toleranceSeconds int64) bool`: parses header, checks timestamp age against `toleranceSeconds` (if > 0), computes expected signature and validates securely using `hmac.Equal` to prevent timing attacks.
2. **Egress SSRF Protection & Connection Pooling (`internal/dispatcher/ssrf.go`):**
   - `ErrRestrictedDestination`: exported error returned when target IP is blocked.
   - `IsRestrictedIP(ip net.IP) bool`: blocks IPv4 loopback (`127.0.0.0/8`), IPv6 loopback (`::1`), Private IPv4 (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`), Cloud metadata / link-local (`169.254.0.0/16`, `fe80::/10`), Unique local IPv6 (`fc00::/7`), and multicast.
   - `NewSafeHTTPClient(timeout time.Duration) *http.Client`: returns `*http.Client` with custom `http.Transport` resolving DNS and checking each IP via `IsRestrictedIP` and `net.Dialer.Control` callback. Connection pooling configured for high throughput: `MaxIdleConns: 2000`, `MaxIdleConnsPerHost: 200`, `IdleConnTimeout: 90 * time.Second`.
3. **Unit Test Suites:**
   - `internal/dispatcher/hmac_test.go`: table-driven tests for sign, verify valid, verify tampered payload, verify expired timestamp.
   - `internal/dispatcher/ssrf_test.go`: comprehensive blocked vs allowed IP matrix (including `169.254.169.254` AWS/GCP metadata) and safe client integration tests.
4. **Constraints:**
   - Pass `go test -race ./internal/dispatcher/...` with 0 data races.
   - Write execution report to `.superpowers/sdd/2026-08-20-webhook-reliability-engine/task-2-report.md`.
