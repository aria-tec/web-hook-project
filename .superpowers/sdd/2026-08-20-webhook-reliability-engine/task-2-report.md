# Task 2 Execution Report: Cryptographic HMAC Signing & SSRF Protection Engine

## Status: DONE

## Overview
Successfully implemented the cryptographic HMAC-SHA256 signing engine and SSRF egress protection engine with connection pooling and DNS resolution validation, following strict Test-Driven Development (TDD).

## Files Created & Implemented
1. [`internal/dispatcher/hmac.go`](file:///Users/arias/Documents/antigravity/ExProject/web-hook-project/internal/dispatcher/hmac.go):
   - `SignPayload(secret string, timestamp int64, payload []byte) string`: formats `t=<timestamp>,v1=<hex_signature>` over `<timestamp>.<payload>`.
   - `VerifySignature(secret string, header string, payload []byte, toleranceSeconds int64) bool`: parses signature header, validates timestamp age against configurable tolerance, and verifies HMAC-SHA256 signature using constant-time `hmac.Equal` to prevent timing attacks.
2. [`internal/dispatcher/ssrf.go`](file:///Users/arias/Documents/antigravity/ExProject/web-hook-project/internal/dispatcher/ssrf.go):
   - `ErrRestrictedDestination`: exported sentinel error for blocked destination IPs.
   - `IsRestrictedIP(ip net.IP) bool`: comprehensive filter blocking IPv4 loopback (`127.0.0.0/8`), RFC1918 private subnets (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`), cloud metadata & link-local (`169.254.0.0/16`, `fe80::/10`), unique local IPv6 (`fc00::/7`), carrier-grade NAT (`100.64.0.0/10`), unspecified (`0.0.0.0`, `::`), multicast, and broadcast addresses.
   - `NewSafeHTTPClient(timeout time.Duration) *http.Client`: returns an `*http.Client` equipped with dual-layer SSRF filtering (`DialContext` DNS pre-check and `net.Dialer.Control` socket-level check) and high-throughput connection pooling (`MaxIdleConns: 2000`, `MaxIdleConnsPerHost: 200`, `IdleConnTimeout: 90s`).
3. [`internal/dispatcher/hmac_test.go`](file:///Users/arias/Documents/antigravity/ExProject/web-hook-project/internal/dispatcher/hmac_test.go):
   - Unit tests covering signing and verification roundtrip, tampered payloads, incorrect secrets, malformed headers, timestamp expiration, clock-drift tolerance, and known-vector verification.
4. [`internal/dispatcher/ssrf_test.go`](file:///Users/arias/Documents/antigravity/ExProject/web-hook-project/internal/dispatcher/ssrf_test.go):
   - Comprehensive IP classification matrix tests (loopback, private, link-local, cloud metadata, multicast, public IPs, nil check) and safe client integration tests against local servers and pool settings.

## Test Verification Summary
- **Failing Phase:** Verified test failure with `/usr/local/go/bin/go test ./internal/dispatcher/...` before creating implementations.
- **Passing Phase:** Verified all tests pass with race detector enabled:
  ```bash
  /usr/local/go/bin/go test -race -v ./internal/dispatcher/...
  ```
  - `TestHMAC_SignAndVerify`: PASS
  - `TestHMAC_ToleranceAndExpiry`: PASS
  - `TestHMAC_MalformedHeaders` (9 subtests): PASS
  - `TestHMAC_KnownVector`: PASS
  - `TestSSRF_IsRestrictedIP` (22 subtests): PASS
  - `TestSSRF_NewSafeHTTPClient_Configuration`: PASS
  - `TestSSRF_NewSafeHTTPClient_BlocksRestrictedRequests`: PASS
  - Full repo check (`go test -race ./...`): PASS (0 race conditions).

## Concerns & Blockers
None. All components meet specification requirements and are ready for integration into the Worker Dispatcher (Task 6).
