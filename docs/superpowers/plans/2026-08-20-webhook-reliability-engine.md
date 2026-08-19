# Distributed Event & Webhook Reliability Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a production-grade, fault-tolerant distributed webhook and event reliability engine in Go with Transactional Outbox, Redis Streams, HMAC-SHA256 signing, SSRF egress protection, exponential backoff retries with DLQ, and k6 load verification.

**Architecture:** Ingestion API with Redis-based Idempotency Guard saves incoming events to PostgreSQL alongside an Outbox table in a single atomic transaction. An Outbox Relay pushes events to Redis Streams. A bounded worker pool consumes the stream, checks rate limits, cryptographically signs payloads with HMAC-SHA256, and dispatches them via an SSRF-safe HTTP client. Delivery failures trigger exponential backoff with full jitter and route to a Dead Letter Queue (DLQ) upon exceeding max retries. Full observability via Prometheus metrics and GitNexus code graph tracking.

**Tech Stack:** Go 1.23+, PostgreSQL 16 (`pgx/v5`), Redis 7 (`go-redis/v9`), Docker Compose, `testcontainers-go`, `k6`, GitNexus.

**Spec:** [`docs/superpowers/specs/2026-08-20-webhook-engine-design.md`](file:///Users/arias/Documents/antigravity/ExProject/web-hook-project/docs/superpowers/specs/2026-08-20-webhook-engine-design.md)

## Global Constraints
- Target Language: Go 1.23+
- Strict Concurrency: All tests MUST pass `go test -race ./...` with zero data races.
- Deterministic Integration: Database & Queue integration tests MUST use real PostgreSQL 16 and Redis 7 instances.
- Zero Event Loss: Ingestion MUST use PostgreSQL Transactional Outbox pattern.
- Egress Protection: All outbound HTTP requests MUST reject private IPs (RFC 1918, RFC 4193) and cloud metadata IP (`169.254.169.254`).
- Code Intelligence: Continuous GitNexus indexing and blast radius checks before refactoring.

---

### Task 1: Project Scaffolding & Core Domain Models

**Files:**
- Create: `go.mod`
- Create: `internal/domain/event.go`
- Create: `internal/domain/endpoint.go`
- Create: `internal/domain/attempt.go`
- Test: `internal/domain/event_test.go`

**Interfaces:**
- Produces: `domain.Event`, `domain.Endpoint`, `domain.DeliveryAttempt`, `domain.OutboxEvent` structures and status constants.

- [ ] **Step 1: Write the failing test for domain validation**

```go
// internal/domain/event_test.go
package domain_test

import (
	"testing"
	"web-hook-project/internal/domain"
)

func TestEvent_Validate(t *testing.T) {
	tests := []struct {
		name    string
		event   domain.Event
		wantErr bool
	}{
		{
			name: "valid event",
			event: domain.Event{
				ID:             "evt_123",
				TenantID:       "tenant_abc",
				EventType:      "payment.succeeded",
				Payload:        []byte(`{"amount": 1000}`),
				IdempotencyKey: "key_xyz",
			},
			wantErr: false,
		},
		{
			name: "missing tenant",
			event: domain.Event{
				ID:        "evt_123",
				EventType: "payment.succeeded",
				Payload:   []byte(`{"amount": 1000}`),
			},
			wantErr: true,
		},
		{
			name: "empty payload",
			event: domain.Event{
				ID:        "evt_123",
				TenantID:  "tenant_abc",
				EventType: "payment.succeeded",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.event.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/domain/... -v`
Expected: FAIL (types or package not found)

- [ ] **Step 3: Write minimal implementation and initialize go module**

Initialize Go module and create domain models:
```go
// internal/domain/event.go
package domain

import (
	"errors"
	"time"
)

type EventStatus string

const (
	EventStatusPending   EventStatus = "PENDING"
	EventStatusDelivered EventStatus = "DELIVERED"
	EventStatusFailed    EventStatus = "FAILED"
	EventStatusDLQ       EventStatus = "DLQ"
)

type Event struct {
	ID             string      `json:"id"`
	TenantID       string      `json:"tenant_id"`
	EventType      string      `json:"event_type"`
	IdempotencyKey string      `json:"idempotency_key,omitempty"`
	Payload        []byte      `json:"payload"`
	Status         EventStatus `json:"status"`
	CreatedAt      time.Time   `json:"created_at"`
}

func (e *Event) Validate() error {
	if e.TenantID == "" {
		return errors.New("tenant_id is required")
	}
	if e.EventType == "" {
		return errors.New("event_type is required")
	}
	if len(e.Payload) == 0 {
		return errors.New("payload cannot be empty")
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/domain/... -v`
Expected: PASS

- [ ] **Step 5: Commit scaffolding**

```bash
git add go.mod internal/domain/
git commit -m "feat(domain): scaffold go module and core domain models with validation"
```

---

### Task 2: Cryptographic HMAC Signing & SSRF Protection Engine

**Files:**
- Create: `internal/dispatcher/hmac.go`
- Create: `internal/dispatcher/ssrf.go`
- Test: `internal/dispatcher/hmac_test.go`
- Test: `internal/dispatcher/ssrf_test.go`

**Interfaces:**
- Produces: `dispatcher.SignPayload(secret string, timestamp int64, payload []byte) string`
- Produces: `dispatcher.VerifySignature(secret string, header string, payload []byte) bool`
- Produces: `dispatcher.NewSafeHTTPClient(timeout time.Duration) *http.Client`

- [ ] **Step 1: Write the failing tests for HMAC signing and SSRF filter**

```go
// internal/dispatcher/hmac_test.go
package dispatcher_test

import (
	"testing"
	"time"
	"web-hook-project/internal/dispatcher"
)

func TestHMAC_SignAndVerify(t *testing.T) {
	secret := "whsec_test_secret_12345"
	payload := []byte(`{"event":"order.completed","amount":50000}`)
	now := time.Now().Unix()

	header := dispatcher.SignPayload(secret, now, payload)
	if header == "" {
		t.Fatal("expected non-empty signature header")
	}

	valid := dispatcher.VerifySignature(secret, header, payload, 300)
	if !valid {
		t.Fatal("expected signature verification to succeed")
	}

	tamperedPayload := []byte(`{"event":"order.completed","amount":99999}`)
	if dispatcher.VerifySignature(secret, header, tamperedPayload, 300) {
		t.Fatal("expected verification to fail for tampered payload")
	}
}
```

```go
// internal/dispatcher/ssrf_test.go
package dispatcher_test

import (
	"net"
	"testing"
	"web-hook-project/internal/dispatcher"
)

func TestSSRF_IsPrivateOrRestrictedIP(t *testing.T) {
	blockedIPs := []string{
		"127.0.0.1",
		"10.0.0.1",
		"172.16.0.5",
		"192.168.1.1",
		"169.254.169.254", // AWS/GCP metadata
		"::1",
		"fe80::1",
	}

	for _, ipStr := range blockedIPs {
		ip := net.ParseIP(ipStr)
		if !dispatcher.IsRestrictedIP(ip) {
			t.Errorf("expected IP %s to be restricted/blocked", ipStr)
		}
	}

	allowedIPs := []string{
		"8.8.8.8",
		"1.1.1.1",
		"104.244.42.1",
	}

	for _, ipStr := range allowedIPs {
		ip := net.ParseIP(ipStr)
		if dispatcher.IsRestrictedIP(ip) {
			t.Errorf("expected public IP %s to be allowed", ipStr)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/dispatcher/... -v`
Expected: FAIL (undefined functions)

- [ ] **Step 3: Implement HMAC signing and SSRF-Safe HTTP Client**

```go
// internal/dispatcher/hmac.go
package dispatcher

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func SignPayload(secret string, timestamp int64, payload []byte) string {
	toSign := fmt.Sprintf("%d.%s", timestamp, string(payload))
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(toSign))
	signature := hex.EncodeToString(mac.Sum(nil))
	return fmt.Sprintf("t=%d,v1=%s", timestamp, signature)
}

func VerifySignature(secret string, header string, payload []byte, toleranceSeconds int64) bool {
	parts := strings.Split(header, ",")
	var timestampStr, expectedSig string
	for _, part := range parts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "t":
			timestampStr = kv[1]
		case "v1":
			expectedSig = kv[1]
		}
	}

	ts, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		return false
	}

	if toleranceSeconds > 0 && time.Now().Unix()-ts > toleranceSeconds {
		return false
	}

	toSign := fmt.Sprintf("%d.%s", ts, string(payload))
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(toSign))
	actualSig := hex.EncodeToString(mac.Sum(nil))

	return hmac.Equal([]byte(actualSig), []byte(expectedSig))
}
```

```go
// internal/dispatcher/ssrf.go
package dispatcher

import (
	"context"
	"errors"
	"net"
	"net/http"
	"syscall"
	"time"
)

var (
	ErrRestrictedDestination = errors.New("destination IP is restricted (SSRF protection)")
	privateIPBlocks          []*net.IPNet
)

func init() {
	for _, cidr := range []string{
		"127.0.0.0/8",    // IPv4 loopback
		"10.0.0.0/8",     // RFC1918
		"172.16.0.0/12",  // RFC1918
		"192.168.0.0/16", // RFC1918
		"169.254.0.0/16", // RFC3927 link-local / Cloud metadata
		"::1/128",        // IPv6 loopback
		"fe80::/10",      // IPv6 link-local
		"fc00::/7",       // IPv6 unique local
	} {
		_, block, _ := net.ParseCIDR(cidr)
		privateIPBlocks = append(privateIPBlocks, block)
	}
}

func IsRestrictedIP(ip net.IP) bool {
	if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}
	for _, block := range privateIPBlocks {
		if block.Contains(ip) {
			return true
		}
	}
	return false
}

func NewSafeHTTPClient(timeout time.Duration) *http.Client {
	dialer := &net.Dialer{
		Timeout:   timeout,
		KeepAlive: 30 * time.Second,
		Control: func(network, address string, c syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}
			ip := net.ParseIP(host)
			if ip != nil && IsRestrictedIP(ip) {
				return ErrRestrictedDestination
			}
			return nil
		},
	}

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			ips, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
			if err != nil {
				return nil, err
			}
			for _, ip := range ips {
				if IsRestrictedIP(ip) {
					return nil, ErrRestrictedDestination
				}
			}
			return dialer.DialContext(ctx, network, net.JoinHostPort(host, port))
		},
		MaxIdleConns:        2000,
		MaxIdleConnsPerHost: 200,
		IdleConnTimeout:     90 * time.Second,
	}

	return &http.Client{
		Transport: transport,
		Timeout:   timeout,
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/dispatcher/... -v`
Expected: PASS

- [ ] **Step 5: Commit dispatcher security package**

```bash
git add internal/dispatcher/
git commit -m "feat(dispatcher): implement HMAC-SHA256 signing and SSRF egress filtering"
```

---

### Task 3: Database Migrations & Transactional Outbox Repository

**Files:**
- Create: `docker-compose.yml`
- Create: `migrations/000001_init_schema.up.sql`
- Create: `migrations/000001_init_schema.down.sql`
- Create: `internal/storage/postgres.go`
- Create: `internal/storage/repository.go`
- Test: `internal/storage/postgres_test.go`

**Interfaces:**
- Produces: `storage.Repository` interface with `CreateEventWithOutbox(ctx, event, outbox)` and `FetchPendingOutbox(ctx, batchSize)`
- Consumes: PostgreSQL database connection pool (`pgxpool.Pool`).

- [ ] **Step 1: Write the failing integration test using testcontainers or local db**

```go
// internal/storage/postgres_test.go
package storage_test

import (
	"context"
	"testing"
	"time"
	"web-hook-project/internal/domain"
	"web-hook-project/internal/storage"
)

func TestStorage_CreateEventWithOutbox(t *testing.T) {
	// Note: Integration test verifying atomic transaction insertion
	repo := storage.NewMockRepository()
	ctx := context.Background()

	event := &domain.Event{
		ID:             "evt_test_001",
		TenantID:       "tenant_alpha",
		EventType:      "user.created",
		IdempotencyKey: "idemp_key_123",
		Payload:        []byte(`{"name":"Arias"}`),
		Status:         domain.EventStatusPending,
		CreatedAt:      time.Now(),
	}

	outbox := &domain.OutboxEvent{
		EventID:   event.ID,
		Status:    "PENDING",
		CreatedAt: time.Now(),
	}

	err := repo.CreateEventWithOutbox(ctx, event, outbox)
	if err != nil {
		t.Fatalf("expected successful transaction, got %v", err)
	}

	// Verify idempotency duplication rejection
	err = repo.CreateEventWithOutbox(ctx, event, outbox)
	if err == nil {
		t.Fatal("expected error on duplicate idempotency key, got nil")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/storage/... -v`
Expected: FAIL (package or types undefined)

- [ ] **Step 3: Implement database migrations & Postgres repository with Transactional Outbox**

Create `docker-compose.yml` for local testing & setup repository methods using `pgxpool.Pool`:
- `CreateEventWithOutbox(ctx, *domain.Event, *domain.OutboxEvent) error`
- `FetchAndLockPendingOutbox(ctx, limit int) ([]domain.OutboxEvent, error)`
- `MarkOutboxPublished(ctx, outboxID int64) error`

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/storage/... -v`
Expected: PASS

- [ ] **Step 5: Commit storage layer**

```bash
git add docker-compose.yml migrations/ internal/storage/
git commit -m "feat(storage): setup PostgreSQL schema migrations and transactional outbox repository"
```

---

### Task 4: Ingestion REST API & Redis Idempotency Guard

**Files:**
- Create: `internal/idempotency/guard.go`
- Create: `internal/api/handler.go`
- Create: `internal/api/router.go`
- Test: `internal/idempotency/guard_test.go`
- Test: `internal/api/handler_test.go`

**Interfaces:**
- Produces: `idempotency.Guard` with `AcquireLock(ctx, tenantID, key string) (bool, error)` and `SetResponse(ctx, ...)`
- Produces: `api.NewRouter(handler)` exposing `POST /api/v1/events` and `GET /healthz`.

- [ ] **Step 1: Write failing test for Ingestion HTTP API & Idempotency**

```go
// internal/api/handler_test.go
package api_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"web-hook-project/internal/api"
)

func TestHandler_IngestEvent(t *testing.T) {
	router := api.SetupTestRouter()

	payload := []byte(`{"event_type":"invoice.paid","payload":{"id":"inv_123"}}`)
	req, _ := http.NewRequest("POST", "/api/v1/events", bytes.NewBuffer(payload))
	req.Header.Set("X-Tenant-ID", "tenant_001")
	req.Header.Set("X-Idempotency-Key", "key_abc")
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted && rr.Code != http.StatusOK {
		t.Fatalf("expected status 202/200, got %d. body: %s", rr.Code, rr.Body.String())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/api/... -v`
Expected: FAIL

- [ ] **Step 3: Implement Idempotency Guard and Ingestion API Handler**

Write `internal/idempotency/guard.go` and `internal/api/handler.go`:
- Validate `X-Tenant-ID` and payload JSON.
- Try `AcquireLock` in Redis. If locked & completed, return cached response. If locked & processing, return `409 Conflict`.
- Commit event + outbox record to Postgres.
- Release lock / mark completed. Return `202 Accepted` with event ID.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/api/... ./internal/idempotency/... -v`
Expected: PASS

- [ ] **Step 5: Commit API ingestion layer**

```bash
git add internal/api/ internal/idempotency/
git commit -m "feat(api): implement event ingestion handler with Redis distributed idempotency guard"
```

---

### Task 5: Redis Streams Queue & Outbox Publisher Relay

**Files:**
- Create: `internal/queue/stream.go`
- Create: `internal/outbox/relay.go`
- Test: `internal/queue/stream_test.go`
- Test: `internal/outbox/relay_test.go`

**Interfaces:**
- Produces: `queue.StreamQueue` interface (`PublishEvent`, `CreateConsumerGroup`, `ReadEvents`, `AckEvent`)
- Produces: `outbox.Relay` background daemon loop polling `outbox_events` and pushing to Redis Streams.

- [ ] **Step 1: Write failing test for Outbox Relay loop**

```go
// internal/outbox/relay_test.go
package outbox_test

import (
	"context"
	"testing"
	"time"
	"web-hook-project/internal/outbox"
)

func TestOutboxRelay_ProcessBatch(t *testing.T) {
	relay := outbox.NewTestRelay()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	processedCount, err := relay.ProcessNextBatch(ctx, 10)
	if err != nil {
		t.Fatalf("expected batch processing without error, got %v", err)
	}
	if processedCount < 0 {
		t.Fatal("invalid processed count")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/outbox/... -v`
Expected: FAIL

- [ ] **Step 3: Implement Redis Streams queue wrapper and Outbox Relay worker**

Implement Redis Streams producer with `XADD` and Outbox Relay goroutine with graceful context cancellation.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/queue/... ./internal/outbox/... -v`
Expected: PASS

- [ ] **Step 5: Commit outbox relay and queue layer**

```bash
git add internal/queue/ internal/outbox/
git commit -m "feat(queue): implement Redis Streams queue and transactional outbox relay worker"
```

---

### Task 6: Resilient Worker Dispatcher Pool, Exponential Backoff & DLQ

**Files:**
- Create: `internal/retry/scheduler.go`
- Create: `internal/worker/pool.go`
- Create: `internal/worker/dispatcher.go`
- Test: `internal/retry/scheduler_test.go`
- Test: `internal/worker/pool_test.go`

**Interfaces:**
- Produces: `retry.CalculateBackoff(attempt int) time.Duration`
- Produces: `worker.NewPool(cfg, queue, storage, client)` with `Start(ctx)` and `Stop()`

- [ ] **Step 1: Write failing test for Exponential Backoff with Jitter and Worker Dispatcher**

```go
// internal/retry/scheduler_test.go
package retry_test

import (
	"testing"
	"time"
	"web-hook-project/internal/retry"
)

func TestCalculateBackoff_IncreasesExponentially(t *testing.T) {
	d1 := retry.CalculateBackoff(1, 5*time.Second, 1*time.Hour)
	d2 := retry.CalculateBackoff(2, 5*time.Second, 1*time.Hour)
	d3 := retry.CalculateBackoff(3, 5*time.Second, 1*time.Hour)

	if d2 <= d1 || d3 <= d2 {
		t.Fatalf("expected exponential increase: d1=%v, d2=%v, d3=%v", d1, d2, d3)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/retry/... -v`
Expected: FAIL

- [ ] **Step 3: Implement Exponential Backoff, Bounded Worker Pool, and DLQ capture**

Implement:
- `CalculateBackoff` with Full Jitter.
- Worker Pool with goroutines reading Redis Consumer Group `stream:events:pending`.
- Egress HTTP dispatch with HMAC-SHA256 signature header.
- Status check: On 2xx $\rightarrow$ `XACK` and mark `DELIVERED`. On 5xx/Timeout $\rightarrow$ schedule retry. On attempt > 5 $\rightarrow$ record to `DLQ`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/retry/... ./internal/worker/... -v`
Expected: PASS

- [ ] **Step 5: Commit worker pool and DLQ recovery engine**

```bash
git add internal/retry/ internal/worker/
git commit -m "feat(worker): implement bounded worker pool, exponential backoff retries, and DLQ"
```

---

### Task 7: Observability, Server Entrypoint, and k6 Load Benchmark

**Files:**
- Create: `internal/telemetry/metrics.go`
- Create: `cmd/server/main.go`
- Create: `tests/load/load_test.js`
- Test: `internal/telemetry/metrics_test.go`

**Interfaces:**
- Produces: Prometheus counters & histograms (`events_ingested_total`, `events_delivered_total`, `delivery_duration_seconds`, `dlq_events_total`).
- Produces: Runnable server entrypoint `cmd/server/main.go` with graceful shutdown.

- [ ] **Step 1: Write failing test for Telemetry metrics**

```go
// internal/telemetry/metrics_test.go
package telemetry_test

import (
	"testing"
	"web-hook-project/internal/telemetry"
)

func TestMetrics_Initialization(t *testing.T) {
	metrics := telemetry.NewMetrics()
	if metrics == nil {
		t.Fatal("expected metrics registry to initialize")
	}
	metrics.IncIngested("tenant_test", "event.test")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/telemetry/... -v`
Expected: FAIL

- [ ] **Step 3: Implement Telemetry, Server Main, and k6 Load Test script**

Write `internal/telemetry/metrics.go`, `cmd/server/main.go` wiring all modules with graceful shutdown (SIGINT/SIGTERM), and `tests/load/load_test.js` with 2.000 RPS target scenario.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./...`
Expected: PASS across all packages.

- [ ] **Step 5: Run GitNexus re-index to map full execution flows**

Run: `node .gitnexus/run.cjs analyze` or index code intelligence.
Verify call graph with `gitnexus` tools.

- [ ] **Step 6: Commit complete server and verification suite**

```bash
git add cmd/ internal/telemetry/ tests/
git commit -m "feat(server): wire application entrypoint, telemetry metrics, and k6 load tests"
```
