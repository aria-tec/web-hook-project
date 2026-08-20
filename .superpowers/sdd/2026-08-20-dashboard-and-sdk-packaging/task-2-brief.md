# Task 2: Mock Webhook Receiver & Echo Server (`cmd/mockreceiver`)

**Files:**
- Create: `cmd/mockreceiver/main.go`
- Test: `cmd/mockreceiver/main_test.go`

**Interfaces:**
- Binary: `mockreceiver` listening on `PORT` (default `:9090`).
- HTTP Endpoints:
  1. `POST /webhook/success` -> Returns 200 OK (`{"status":"success","received_at":"..."}`)
  2. `POST /webhook/flaky` -> Simulates intermittent 500 failure (succeeds every 3rd attempt per webhook ID or 50% flaky, returning 500 on retries and 200 eventually)
  3. `POST /webhook/poison` -> Simulates unprocessable entity / bad payload, returns 400 Bad Request (`{"error":"poison_pill_rejected"}`) to push directly to DLQ
  4. `GET /inspect/logs` -> Returns list of last 50 received webhook payloads and headers in JSON
  5. `POST /inspect/clear` -> Clears received webhook inspection log

**Requirements:**
1. Follow TDD: Write `cmd/mockreceiver/main_test.go` testing all endpoints, verify failure, implement `cmd/mockreceiver/main.go`, and verify passing.
2. In-memory thread-safe circular log buffer for received requests (capturing URL, Method, Headers including `X-Webhook-Signature` and `X-Webhook-ID`, Body, and ReceivedAt).
3. CORS headers support on all mock endpoints for dashboard inspection if queried from browser.
4. Clean shutdown handling on SIGINT/SIGTERM.
5. All tests must pass `go test -race ./cmd/mockreceiver/...`.
