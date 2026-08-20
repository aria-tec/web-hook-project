# Task 1: Go Engine SSE Live Stream Broadcast & CORS Support

**Files:**
- Create: `internal/api/stream.go`
- Modify: `internal/api/router.go`
- Modify: `internal/api/handler.go`
- Test: `internal/api/stream_test.go`

**Interfaces:**
- Produces: `api.SSEBroker` broadcasting delivery attempts to connected HTTP clients via `GET /api/v1/events/stream`.
- Produces: `Handler.HandleEventStream(w http.ResponseWriter, r *http.Request)`.
- Updates: `NewRouter` with CORS middleware permitting `localhost:3000` and `*` in development.
- Produces: `Handler.WithSSEBroker(b *SSEBroker) *Handler` and broadcast hook inside `dispatcher.Dispatcher` or `handler.go` when delivery attempts are recorded.

**Requirements:**
1. Follow TDD: Write `internal/api/stream_test.go` first. Run test to verify failure.
2. Implement `SSEBroker` in `internal/api/stream.go`:
   - Thread-safe client management (registration, unregistration, broadcast channels).
   - `Broadcast(attempt *domain.DeliveryAttempt)`.
   - `ServeHTTP(w http.ResponseWriter, r *http.Request)` sending `Content-Type: text/event-stream`, `Cache-Control: no-cache`, `Connection: keep-alive`, `Access-Control-Allow-Origin: *`.
   - Periodic keep-alive comment (`: ping\n\n`) every 15s to keep connection alive.
   - Graceful client disconnect handling when `r.Context().Done()` fires.
3. Update `internal/api/router.go` to add `GET /api/v1/events/stream` and CORS headers (`Access-Control-Allow-Methods`, `Access-Control-Allow-Headers`).
4. Connect SSE broker to `Dispatcher` or Worker Pool so every delivery attempt (`domain.DeliveryAttempt`) is broadcasted live in real-time.
5. All tests must pass `go test -race ./...`.
