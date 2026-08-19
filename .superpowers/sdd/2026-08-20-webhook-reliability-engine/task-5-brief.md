# Task 5 Brief: Redis Streams Queue & Outbox Publisher Relay

## Plan Context
- Spec: `docs/superpowers/specs/2026-08-20-webhook-engine-design.md`
- Plan: `docs/superpowers/plans/2026-08-20-webhook-reliability-engine.md` (Task 5)

## Requirements
1. **Stream Queue Abstraction (`internal/queue/stream.go`):**
   - `QueueMessage` struct (`ID string`, `EventID string`, `TenantID string`, `EventType string`, `Payload []byte`, `CreatedAt time.Time`).
   - `StreamQueue` interface:
     - `PublishEvent(ctx context.Context, streamName string, event *domain.Event) (messageID string, err error)`
     - `CreateConsumerGroup(ctx context.Context, streamName, groupName, startID string) error`
     - `ReadEvents(ctx context.Context, streamName, groupName, consumerName string, count int64, block time.Duration) ([]QueueMessage, error)`
     - `AckEvent(ctx context.Context, streamName, groupName string, messageIDs ...string) error`
   - Implement `RedisStreamQueue` using `github.com/redis/go-redis/v9` (`XADD`, `XGROUP CREATE MKSTREAM`, `XREADGROUP`, `XACK`).
   - Implement `MemoryStreamQueue` with thread-safe consumer group mechanics for fast deterministic testing and CI environments.
2. **Transactional Outbox Publisher Relay (`internal/outbox/relay.go`):**
   - `Relay` struct managing background publishing from database outbox to Redis Streams.
   - `ProcessNextBatch(ctx context.Context, batchSize int) (int, error)`:
     - Fetches pending outbox records (`storage.FetchAndLockPendingOutbox`).
     - Fetches corresponding `Event` from `storage.GetEvent`.
     - Publishes to Redis Streams (`queue.PublishEvent(ctx, "stream:events:pending", event)`).
     - Marks outbox record as published (`storage.MarkOutboxPublished`).
   - `Start(ctx context.Context, pollInterval time.Duration, batchSize int)`:
     - Runs continuous polling loop with ticker, processing batches until `ctx.Done()`.
3. **Unit Test Suites:**
   - `internal/queue/stream_test.go`: tests publishing, consumer group consumption, and message acknowledgement.
   - `internal/outbox/relay_test.go`: tests single batch processing, outbox status update, missing event handling, and graceful worker shutdown with `context.WithCancel`.
4. **Constraints:**
   - Pass `go test -race ./internal/queue/... ./internal/outbox/...` with 0 data races.
   - Write execution report to `.superpowers/sdd/2026-08-20-webhook-reliability-engine/task-5-report.md`.
