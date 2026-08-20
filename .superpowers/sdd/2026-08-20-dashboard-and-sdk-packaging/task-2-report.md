# Task 2 Report: Mock Webhook Receiver & Echo Server (`cmd/mockreceiver`)

**Status:** DONE  
**Timestamp:** 2026-08-20T10:04:41+07:00  
**Commit:** `9ea761a` - `feat(mockreceiver): implement programmable webhook receiver and echo server`

---

## 1. Summary of Work

Implemented a high-performance standalone Mock Webhook Receiver & Echo Server in Go (`cmd/mockreceiver`) designed for local simulation, chaos testing, interactive dashboard inspection, and the Docker Compose demo stack.

### Key Deliverables:

1. **`cmd/mockreceiver/main.go`**:
   - **`CapturedRequest`**: Structured representation capturing request ID (UUID), URL, Method, Headers, Webhook Signature (`X-Webhook-Signature` / `Svix-Signature`), Webhook ID (`X-Webhook-ID` / `Svix-Id`), Body string, and UTC Timestamp (`ReceivedAt`).
   - **`RingBuffer`**: Thread-safe circular buffer (default capacity: 50) protected with `sync.RWMutex` to capture inbound webhook invocations with automatic FIFO eviction of oldest requests when capacity is exceeded.
   - **`FlakyState`**: Concurrency-safe attempt tracker supporting both per-`X-Webhook-ID` scoping and global sequence tracking to simulate intermittent failures (returns HTTP 500 on attempts 1 & 2, then HTTP 200 on attempt 3).
   - **`MockServer` & Routes**:
     - `POST /webhook/success`: Returns HTTP 200 OK (`{"status":"success","received":true,"received_at":"..."}`).
     - `POST /webhook/flaky`: Intermittent 500 failure simulation (fails twice with 500, succeeds on 3rd with 200).
     - `POST /webhook/poison`: Returns HTTP 400 Bad Request (`{"error":"poison_pill_rejected","status":"rejected"}`) to push directly to DLQ.
     - `POST /webhook/slow`: Supports query param `?delay=...` (default 4s) then returns HTTP 200 OK.
     - `GET /inspect/logs` & `GET /requests`: Returns JSON list of captured webhook payloads and headers in chronological order.
     - `POST /inspect/clear`: Resets the inspection log buffer.
     - `GET /healthz` & `GET /health`: Health probes returning HTTP 200 OK (`{"status":"ok"}`).
   - **`corsMiddleware`**: Universal CORS support handling OPTIONS preflight requests (returning HTTP 204) and setting `Access-Control-Allow-Origin: *`, `Access-Control-Allow-Methods`, `Access-Control-Allow-Headers`, and `Access-Control-Expose-Headers`.
   - **`main()` Lifecycle**: CLI `-port` flag and `PORT` environment variable support (default `:9090`), with clean graceful shutdown on `SIGINT` / `SIGTERM`.

2. **`cmd/mockreceiver/main_test.go`**:
   - TDD unit and concurrency test suite:
     - `TestMockReceiver_Routes`: Validates `/webhook/success`, `/webhook/poison`, `/webhook/flaky` (global and scoped by `X-Webhook-ID`), and `/webhook/slow`.
     - `TestMockReceiver_InspectLogsAndClear`: Validates payload/header capture, JSON output formatting, and log clearing.
     - `TestMockReceiver_RingBuffer_Capacity`: Verifies circular buffer bounds and FIFO eviction over 70 sequential requests.
     - `TestMockReceiver_CORS`: Validates OPTIONS preflight status 204 and CORS header presence on normal requests.
     - `TestMockReceiver_Healthz`: Validates health probe endpoints.
     - `TestMockReceiver_ConcurrentRequests`: Validates thread safety across all routes under high concurrent load with Go race detector enabled.

---

## 2. Test Verification

### Test Suite Execution
- **Command:** `go test -v -count=1 -race ./cmd/mockreceiver/...`
- **Output:**
```
=== RUN   TestMockReceiver_Routes
--- PASS: TestMockReceiver_Routes (0.01s)
=== RUN   TestMockReceiver_InspectLogsAndClear
--- PASS: TestMockReceiver_InspectLogsAndClear (0.00s)
=== RUN   TestMockReceiver_RingBuffer_Capacity
--- PASS: TestMockReceiver_RingBuffer_Capacity (0.01s)
=== RUN   TestMockReceiver_CORS
--- PASS: TestMockReceiver_CORS (0.00s)
=== RUN   TestMockReceiver_Healthz
--- PASS: TestMockReceiver_Healthz (0.00s)
=== RUN   TestMockReceiver_ConcurrentRequests
--- PASS: TestMockReceiver_ConcurrentRequests (0.00s)
PASS
ok  	web-hook-project/cmd/mockreceiver	1.366s
```

---

## 3. Interfaces & Symbol Signatures

- `main.CapturedRequest`:
  - `ID string`
  - `URL string`
  - `Method string`
  - `Headers map[string][]string`
  - `Signature string`
  - `WebhookID string`
  - `Body string`
  - `ReceivedAt time.Time`
- `main.RingBuffer`:
  - `NewRingBuffer(capacity int) *RingBuffer`
  - `(rb *RingBuffer) Add(req CapturedRequest)`
  - `(rb *RingBuffer) GetAll() []CapturedRequest`
  - `(rb *RingBuffer) Clear()`
  - `(rb *RingBuffer) Len() int`
- `main.FlakyState`:
  - `NewFlakyState() *FlakyState`
  - `(fs *FlakyState) NextAttempt(key string) int`
  - `(fs *FlakyState) Reset()`
- `main.MockServer` / `main.Server`:
  - `NewMockServer() *Server`
  - `NewMockServerWithCapacity(capacity int) *MockServer`
  - `(s *MockServer) Handler() http.Handler`
  - `(s *MockServer) Buffer() *RingBuffer`
  - `(s *MockServer) Reset()`
