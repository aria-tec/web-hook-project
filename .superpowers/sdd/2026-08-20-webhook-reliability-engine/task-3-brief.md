# Task 3 Brief: Database Migrations & Transactional Outbox Storage Repository

## Plan Context
- Spec: `docs/superpowers/specs/2026-08-20-webhook-engine-design.md`
- Plan: `docs/superpowers/plans/2026-08-20-webhook-reliability-engine.md` (Task 3)

## Requirements
1. **Docker Compose Setup (`docker-compose.yml`):**
   - PostgreSQL 16 (port 5432, user `postgres`, pass `postgres`, db `webhook_db`)
   - Redis 7-alpine (port 6379)
   - Proper health checks.
2. **Schema Migrations (`migrations/`):**
   - `000001_init_schema.up.sql`: creates `tenants`, `endpoints`, `events`, `outbox_events`, `delivery_attempts` with indexes (`idx_outbox_pending`, `idx_events_tenant_created`, `idx_attempts_event`, unique constraint on `(tenant_id, idempotency_key)`).
   - `000001_init_schema.down.sql`: drops tables in reverse foreign-key order.
3. **Repository Interface & Implementation (`internal/storage/`):**
   - `internal/storage/repository.go`: `Repository` interface:
     - `CreateTenant(ctx context.Context, tenant *domain.Tenant) error`
     - `GetTenant(ctx context.Context, id string) (*domain.Tenant, error)`
     - `CreateEndpoint(ctx context.Context, endpoint *domain.Endpoint) error`
     - `GetEndpoint(ctx context.Context, id string) (*domain.Endpoint, error)`
     - `GetEndpointsByTenant(ctx context.Context, tenantID string) ([]domain.Endpoint, error)`
     - `CreateEventWithOutbox(ctx context.Context, event *domain.Event, outbox *domain.OutboxEvent) error` -> single atomic transaction (`BEGIN ... COMMIT`)
     - `FetchAndLockPendingOutbox(ctx context.Context, limit int) ([]domain.OutboxEvent, error)` -> `SELECT ... FOR UPDATE SKIP LOCKED`
     - `MarkOutboxPublished(ctx context.Context, outboxID int64) error`
     - `RecordDeliveryAttempt(ctx context.Context, attempt *domain.DeliveryAttempt) error`
     - `UpdateEventStatus(ctx context.Context, eventID string, status domain.EventStatus) error`
     - `GetEvent(ctx context.Context, id string) (*domain.Event, error)`
   - `internal/storage/postgres.go`: PostgreSQL driver implementation with `pgx/v5` (`pgxpool.Pool`).
   - `internal/storage/memory.go`: In-memory thread-safe implementation with `sync.RWMutex` for fast deterministic unit tests and CI environments without live database containers.
4. **Unit Test Suite (`internal/storage/repository_test.go`):**
   - Tests `CreateEventWithOutbox` atomicity.
   - Tests duplicate idempotency key rejection.
   - Tests `FetchAndLockPendingOutbox` and `MarkOutboxPublished` state machine transitions.
   - Tests `RecordDeliveryAttempt` and `UpdateEventStatus`.
5. **Constraints:**
   - Add `pgx/v5` dependency via `go get github.com/jackc/pgx/v5`.
   - Ensure all tests pass with `go test -race ./internal/storage/...`.
   - Write execution report to `.superpowers/sdd/2026-08-20-webhook-reliability-engine/task-3-report.md`.
