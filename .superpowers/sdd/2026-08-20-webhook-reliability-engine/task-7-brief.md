# Task 7 Brief: Telemetry, Observability, Server Entrypoint & k6 Load Verification

## Plan Context
- Spec: `docs/superpowers/specs/2026-08-20-webhook-engine-design.md`
- Plan: `docs/superpowers/plans/2026-08-20-webhook-reliability-engine.md` (Task 7)

## Requirements
1. **Prometheus Telemetry & Observability (`internal/telemetry/metrics.go`):**
   - Prometheus metrics registry (`github.com/prometheus/client_golang/prometheus`):
     - `events_ingested_total` (counter, labels: `tenant_id`, `event_type`)
     - `events_delivered_total` (counter, labels: `tenant_id`, `endpoint_id`, `status_code`)
     - `delivery_duration_seconds` (histogram, labels: `tenant_id`, `endpoint_id`, buckets: `[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]`)
     - `dlq_events_total` (counter, labels: `tenant_id`, `endpoint_id`, `reason`)
   - `Metrics` struct exposing `IncIngested`, `IncDelivered`, `ObserveDeliveryDuration`, `IncDLQ`, and `Handler() http.Handler`.
   - `internal/telemetry/metrics_test.go`: unit test verifying metrics registration, counters, histogram observations, and HTTP handler output.
2. **Production Server Entrypoint (`cmd/server/main.go`):**
   - Wires all modules into a unified binary:
     - Configuration from env vars (`PORT`, `DATABASE_URL`, `REDIS_URL`, `WORKER_COUNT`, `RELAY_BATCH_SIZE`).
     - PostgreSQL storage (`storage.NewPostgresRepository` with fallback to `MemoryRepository`).
     - Redis Streams & Idempotency Guard (`queue.NewRedisStreamQueue`, `idempotency.NewRedisGuard` with fallback to memory guards).
     - Outbox Publisher Relay background daemon.
     - SSRF-safe Dispatcher with `DefaultBackoffPolicy`.
     - Bounded Worker Pool with configurable concurrency.
     - HTTP router exposing `/api/v1/events`, `/api/v1/endpoints`, `/healthz`, and `/metrics`.
     - Graceful shutdown on `SIGINT`/`SIGTERM` draining HTTP server, worker pool, and relay goroutines cleanly.
3. **High-Throughput k6 Load Test (`tests/load/load_test.js`):**
   - Scenario targeting 2.000 RPS sustained ingestion.
   - Tests concurrent requests with unique/replayed `X-Idempotency-Key` headers.
   - Asserts SLA thresholds: `p99 latency < 40ms`, `http_req_failed < 0.01%`.
4. **Multi-stage Dockerfile (`Dockerfile`):**
   - Multi-stage build producing static binary under 25MB.
5. **Constraints:**
   - Add `github.com/prometheus/client_golang` dependency if needed.
   - Ensure `go test -race ./...` passes across ALL packages in the repository with 0 data races.
   - Write execution report to `.superpowers/sdd/2026-08-20-webhook-reliability-engine/task-7-report.md`.
