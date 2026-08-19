# Task 4 Brief: Ingestion REST API & Redis Idempotency Guard

## Plan Context
- Spec: `docs/superpowers/specs/2026-08-20-webhook-engine-design.md`
- Plan: `docs/superpowers/plans/2026-08-20-webhook-reliability-engine.md` (Task 4)

## Requirements
1. **Idempotency Guard Engine (`internal/idempotency/guard.go`):**
   - Define `CachedResponse` struct (`StatusCode int`, `Body []byte`, `CompletedAt time.Time`).
   - Define `Guard` interface:
     - `AcquireLock(ctx context.Context, tenantID, key string, ttl time.Duration) (acquired bool, cached *CachedResponse, err error)`
     - `SetResponse(ctx context.Context, tenantID, key string, statusCode int, responseBody []byte, ttl time.Duration) error`
     - `ReleaseLock(ctx context.Context, tenantID, key string) error`
   - Implement `RedisGuard` using `github.com/redis/go-redis/v9` with `SET key value NX EX` distributed lock and cached result storage.
   - Implement `MemoryGuard` for fast deterministic testing and CI environments.
2. **Ingestion REST API (`internal/api/`):**
   - `internal/api/router.go`: Standard library `http.ServeMux` or lightweight router exposing:
     - `POST /api/v1/events` -> Ingestion handler
     - `POST /api/v1/endpoints` -> Create endpoint handler
     - `GET /api/v1/endpoints` -> List endpoints handler
     - `GET /healthz` -> Health check endpoint
   - `internal/api/handler.go`:
     - Validates `X-Tenant-ID` header (400 if missing).
     - Validates JSON payload (`event_type` and `payload` required).
     - Idempotency handling: If `X-Idempotency-Key` present, calls `AcquireLock`.
       - If cached response exists -> returns cached status code & body immediately.
       - If lock not acquired (in-flight parallel request) -> returns `409 Conflict`.
     - Creates Event + OutboxEvent in single atomic transaction via `storage.Repository.CreateEventWithOutbox`.
     - Records cached response in `Guard` and returns `202 Accepted` with JSON: `{"id": "evt_...", "status": "PENDING", "created_at": "..."}`.
3. **Unit & Integration Test Suites:**
   - `internal/idempotency/guard_test.go`: tests acquire lock, conflict on concurrent acquire with goroutines, set cached response, release lock.
   - `internal/api/handler_test.go`: table-driven tests for event ingestion, idempotency deduplication, missing headers, malformed payloads, endpoint creation, and healthz.
4. **Constraints:**
   - Add `go-redis/v9` and `google/uuid` dependencies if needed.
   - Pass `go test -race ./internal/api/... ./internal/idempotency/...` with 0 data races.
   - Write execution report to `.superpowers/sdd/2026-08-20-webhook-reliability-engine/task-4-report.md`.
