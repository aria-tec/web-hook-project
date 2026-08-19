# Task 1 Brief: Project Scaffolding & Core Domain Models

## Plan Context
- Spec: `docs/superpowers/specs/2026-08-20-webhook-engine-design.md`
- Plan: `docs/superpowers/plans/2026-08-20-webhook-reliability-engine.md`

## Requirements
1. Initialize Go module: `module web-hook-project` (Go 1.23+)
2. Create Core Domain Models in `internal/domain/`:
   - `event.go`: `Event` struct, `EventStatus` enum (`PENDING`, `DELIVERED`, `FAILED`, `DLQ`), `Validate()` method.
   - `endpoint.go`: `Endpoint` struct with `ID`, `TenantID`, `URL`, `Secret`, `RateLimit`, `IsActive`, `CreatedAt`, `UpdatedAt`.
   - `attempt.go`: `DeliveryAttempt` struct with `ID`, `EventID`, `EndpointID`, `AttemptNumber`, `ResponseStatus`, `ResponseBody`, `DurationMs`, `Status`, `ErrorMessage`, `ExecutedAt`.
   - `outbox.go`: `OutboxEvent` struct with `ID`, `EventID`, `Status`, `RetryCount`, `CreatedAt`, `ProcessedAt`.
3. Create unit test suite in `internal/domain/event_test.go` and `internal/domain/endpoint_test.go`.
4. Ensure all tests pass with `go test -race ./internal/domain/...`.
5. Report file: `.superpowers/sdd/2026-08-20-webhook-reliability-engine/task-1-report.md`.
