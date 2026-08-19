# Task 6 Brief: Bounded Worker Pool Dispatcher & Exponential Backoff Retry with DLQ

## Plan Context
- Spec: `docs/superpowers/specs/2026-08-20-webhook-engine-design.md`
- Plan: `docs/superpowers/plans/2026-08-20-webhook-reliability-engine.md` (Task 6)

## Requirements
1. **Exponential Backoff Scheduler & DLQ Router (`internal/retry/scheduler.go`):**
   - `BackoffPolicy` struct (`InitialInterval time.Duration`, `MaxInterval time.Duration`, `Multiplier float64`, `MaxRetries int`).
   - `DefaultBackoffPolicy()`: Initial 5s, Max 1h, Multiplier 2.0, MaxRetries 5.
   - `CalculateBackoff(attempt int, policy BackoffPolicy) time.Duration`: Full Jitter exponential backoff $T = \text{random}(0, \min(T_{\max}, T_{\text{initial}} \times M^{(\text{attempt}-1)}))$.
   - `IsRetryable(statusCode int, err error) bool`: Returns `true` for HTTP 408, 429, 500, 502, 503, 504, context deadline/timeout, network connection refused. Returns `false` for 2xx, 3xx, 4xx (except 408/429).
2. **Dispatcher Engine (`internal/dispatcher/client.go`):**
   - `Dispatcher` struct wrapping `*http.Client` (SSRF safe), `storage.Repository`, and `retry.BackoffPolicy`.
   - `Dispatch(ctx context.Context, endpoint *domain.Endpoint, event *domain.Event, attemptNum int) (*domain.DeliveryAttempt, error)`:
     - Cryptographically signs payload with `hmac.SignPayload`.
     - Attaches headers: `Content-Type: application/json`, `User-Agent: WebhookEngine-Dispatcher/1.0`, `X-Webhook-ID`, `X-Webhook-Timestamp`, `X-Webhook-Signature`.
     - Executes HTTP POST and records `DurationMs`.
     - Calls `storage.RecordDeliveryAttempt(ctx, attempt)`.
     - If 2xx: updates event status to `DELIVERED`.
     - If error / non-2xx:
       - If retryable and `attemptNum < policy.MaxRetries`: status `RETRYING`.
       - If max retries reached or non-retryable: status `FAILED`, updates event status to `DLQ`.
3. **Bounded Worker Pool (`internal/worker/pool.go`):**
   - `WorkerPool` struct: configurable concurrency (e.g., `numWorkers = 10`), stream name, consumer group name.
   - `Start(ctx context.Context)`: Spawns `numWorkers` goroutines consuming from `queue.StreamQueue.ReadEvents`.
   - For each event message:
     - Fetches active endpoints for `TenantID` via `storage.GetEndpointsByTenant`.
     - Dispatches webhook to each endpoint using `Dispatcher.Dispatch`.
     - ACKs message in queue via `queue.AckEvent`.
   - `Stop()` / `ctx.Done()`: Graceful shutdown draining worker goroutines with `sync.WaitGroup`.
4. **Unit & Integration Test Suites:**
   - `internal/retry/scheduler_test.go`: tests backoff jitter ranges and retryable status classification.
   - `internal/dispatcher/client_test.go`: test mock receiver verifying HMAC signature, 200 OK delivery, 500 retry classification, and 400 DLQ routing.
   - `internal/worker/pool_test.go`: tests parallel stream consumption, delivery attempt recording, and graceful worker pool shutdown.
5. **Constraints:**
   - Pass `go test -race ./internal/retry/... ./internal/dispatcher/... ./internal/worker/...` with 0 data races.
   - Write execution report to `.superpowers/sdd/2026-08-20-webhook-reliability-engine/task-6-report.md`.
