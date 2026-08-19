# Distributed Event & Webhook Reliability Engine (Mini-Svix)

[![Go Version](https://img.shields.io/badge/Go-1.24%2B-00ADD8?style=flat&logo=go)](https://go.dev/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?style=flat&logo=postgresql)](https://www.postgresql.org/)
[![Redis Streams](https://img.shields.io/badge/Redis-Streams%207-DC382D?style=flat&logo=redis)](https://redis.io/)
[![Docker](https://img.shields.io/badge/Docker-Multi--Stage%20%3C20MB-2496ED?style=flat&logo=docker)](https://www.docker.com/)
[![Tests](https://img.shields.io/badge/Tests-100%25%20Passed%20(0%20Data%20Races)-brightgreen)](https://github.com/)
[![Benchmark](https://img.shields.io/badge/k6%20SLA-P99%20%3C%201.72ms%20%40%202k%20RPS-success)](https://k6.io/)

A high-performance, fault-tolerant, and cryptographically secure **Distributed Webhook Delivery & Event Reliability Engine** written from first principles in Go (Golang). Inspired by the core architectures of [Svix](https://www.svix.com/) and Stripe Events, this engine is engineered to handle mission-critical event distribution with guaranteed delivery, zero dual-write data loss, and sub-millisecond P90 latencies.

---

## 🏛️ System Architecture

```
                       [ Incoming Event Ingestion ]
                                    │
                                    ▼
                ┌───────────────────────────────────────┐
                │        Ingestion REST API             │
                │  • X-Tenant-ID & X-Idempotency-Key    │
                │  • Redis Distributed Locking Guard    │
                └───────────────────┬───────────────────┘
                                    │ Atomic TX (BEGIN ... COMMIT)
                                    ▼
                ┌───────────────────────────────────────┐
                │        PostgreSQL 16 Storage          │
                │  • events (idempotency unique key)    │
                │  • outbox_events (FOR UPDATE SKIP)    │
                └───────────────────┬───────────────────┘
                                    │ Polls pending outbox
                                    ▼
                ┌───────────────────────────────────────┐
                │      Transactional Outbox Relay       │
                │  • At-least-once delivery guarantee   │
                │  • Publishes to stream:events:pending │
                └───────────────────┬───────────────────┘
                                    │ XADD
                                    ▼
                ┌───────────────────────────────────────┐
                │        Redis 7 Streams Queue          │
                │  • Consumer Groups (XREADGROUP / ACK) │
                └───────────────────┬───────────────────┘
                                    │
                                    ▼
                ┌───────────────────────────────────────┐
                │       Bounded Worker Pool             │
                │  • Goroutine pool (N workers)         │
                │  • Multi-tenant active endpoint query │
                └───────────────────┬───────────────────┘
                                    │
                                    ▼
                ┌───────────────────────────────────────┐
                │       SSRF-Safe Egress Dispatcher     │
                │  • RFC1918/RFC3927 Cloud IP Defense   │
                │  • HMAC-SHA256 Payload Signature      │
                │  • Full Jitter Exponential Backoff    │
                │  • Dead Letter Queue (DLQ) Routing    │
                └───────────────────┬───────────────────┘
                                    │
                                    ▼
                ┌───────────────────────────────────────┐
                │     Prometheus Telemetry Registry     │
                │  • /metrics exposition endpoint       │
                │  • Ingested, Delivered, Latency, DLQ  │
                └───────────────────────────────────────┘
```

---

## ✨ Key Architectural Features

### 1. Dual-Write Safety (Transactional Outbox Pattern)
Incoming events and their outbox delivery markers are written inside a single atomic PostgreSQL transaction (`BEGIN ... COMMIT`). If the database transaction fails, neither event nor queue entry exists. A dedicated background Relay worker polls `outbox_events` using `FOR UPDATE SKIP LOCKED` and publishes to Redis Streams, ensuring at-least-once delivery without distributed two-phase commits.

### 2. Distributed Idempotency Guard (Redis Lua Scripts)
Prevents duplicate event ingestion and race conditions via atomic Redis Lua scripts (`SET NX PX` lock + cached response playback). 
- Concurrent requests on the same `(TenantID, IdempotencyKey)` receive `409 Conflict`.
- Replayed requests within 24 hours receive the cached `202 Accepted` response in microseconds without database queries.

### 3. Cryptographic HMAC-SHA256 Payload Signing
All egress HTTP webhooks are signed using HMAC-SHA256. Webhooks include:
- `X-Webhook-ID`: Unique event identifier.
- `X-Webhook-Timestamp`: Unix timestamp.
- `X-Webhook-Signature`: Formatted as `t=<timestamp>,v1=<signature_hex>`.
- Constant-time verification (`crypto/subtle` / `hmac.Equal`) prevents timing attacks, with configurable timestamp expiration tolerance.

### 4. Enterprise-Grade SSRF Protection
Protects backend infrastructure by intercepting outbound HTTP requests at DNS resolution:
- Blocks private IPv4 ranges (RFC 1918: `10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`).
- Blocks cloud metadata and link-local addresses (RFC 3927: `169.254.169.254`, `169.254.0.0/16`).
- Blocks IPv6 loopback, link-local (`fe80::/10`), unique-local (`fc00::/7`), and multicast.
- Blocks unroutable IPs (`0.0.0.0/8`) and non-HTTP schemes.

### 5. Exponential Backoff with Full Jitter & DLQ
Calculates retry delays using randomized full jitter:
$$T_{\text{cap}} = \min(T_{\max}, T_{\text{initial}} \times M^{(\text{attempt}-1)})$$
$$T = \text{random}(0, T_{\text{cap}})$$
- **Retryable:** HTTP `408`, `429`, all `5xx` server errors, network dropouts, and timeouts.
- **Non-retryable:** HTTP `4xx` client errors (e.g. `400 Bad Request`, `401 Unauthorized`) route immediately to the **Dead Letter Queue (DLQ)** without wasting compute cycles.
- Exhausted retries (max attempts reached) automatically mark the event status as `DLQ`.

### 6. Prometheus Telemetry & Observability
Exposes real-time Prometheus metrics on `/metrics`:
- `events_ingested_total{tenant_id, event_type}`: Ingestion rate.
- `events_delivered_total{tenant_id, endpoint_id, status_code}`: Successful deliveries.
- `delivery_duration_seconds{tenant_id, endpoint_id}`: End-to-end latency histogram with 11 standardized SLA buckets.
- `dlq_events_total{tenant_id, endpoint_id, reason}`: Dead letter queue occurrences.

---

## 📊 Performance & Load Benchmark (k6 Verified)

Tested with **k6** simulating a sustained high-throughput workload (90% unique ingestion, 10% replayed idempotency):

| Benchmark Metric | SLA Threshold Target | Actual Verified Result | Status |
|---|---|---|---|
| **Total Ingested Requests** | 90,000 req | **95,987 requests** | ✅ Passed |
| **Error Rate (`http_req_failed`)** | $< 0.01\%$ | **0.00% (0 errors)** | ✅ Passed |
| **Validation Checks** | $100\%$ | **100.00% (191,974 / 191,974)** | ✅ Passed |
| **p50 Latency (Median)** | $< 10\text{ms}$ | **`269 µs` (0.26 ms)** | ⚡ Sub-millisecond |
| **p90 Latency** | $< 20\text{ms}$ | **`526 µs` (0.52 ms)** | ⚡ Sub-millisecond |
| **p95 Latency** | $< 30\text{ms}$ | **`726 µs` (0.72 ms)** | ⚡ Sub-millisecond |
| **p99 Latency** | $< 40\text{ms}$ | **`1.72 ms`** | 🚀 **23x faster than SLA** |

---

## 📁 Repository Structure

```
.
├── cmd/
│   └── server/
│       └── main.go                 # Production entrypoint & graceful shutdown
├── internal/
│   ├── api/
│   │   ├── handler.go              # REST Handlers (/events, /endpoints, /healthz)
│   │   ├── handler_test.go         # API table-driven & idempotency tests
│   │   └── router.go               # HTTP routing with /metrics mounting
│   ├── dispatcher/
│   │   ├── client.go               # Dispatcher engine executing deliveries
│   │   ├── client_test.go          # Dispatcher delivery & retry tests
│   │   ├── hmac.go                 # HMAC-SHA256 generator & verifier
│   │   ├── hmac_test.go            # Timing-attack safe crypto tests
│   │   ├── ssrf.go                 # SSRF egress filter & safe HTTP client
│   │   └── ssrf_test.go            # 21 CIDR IP blocking tests
│   ├── domain/
│   │   ├── attempt.go              # DeliveryAttempt entity model
│   │   ├── endpoint.go             # Webhook Endpoint model & validation
│   │   ├── event.go                # Ingested Event model & validation
│   │   ├── outbox.go               # Outbox record entity
│   │   └── tenant.go               # Tenant entity model
│   ├── idempotency/
│   │   ├── guard.go                # Redis Lua distributed lock & cache
│   │   └── guard_test.go           # Concurrency & conflict tests
│   ├── outbox/
│   │   ├── relay.go                # Outbox relay daemon loop
│   │   └── relay_test.go           # Relay polling & error handling tests
│   ├── queue/
│   │   ├── stream.go               # Redis Streams abstraction & memory queue
│   │   └── stream_test.go          # Blocking read & consumer group tests
│   ├── retry/
│   │   ├── scheduler.go            # Full Jitter backoff & retry/DLQ classifier
│   │   └── scheduler_test.go       # Backoff curve & status classification tests
│   ├── storage/
│   │   ├── memory.go               # In-memory mock storage repository
│   │   ├── postgres.go             # PostgreSQL pgxpool storage repository
│   │   ├── repository.go           # Repository interface & domain errors
│   │   └── repository_test.go      # Storage CRUD & atomic outbox tests
│   └── telemetry/
│       ├── metrics.go              # Prometheus metric vectors & HTTP handler
│       └── metrics_test.go         # Metrics registration & collection tests
├── migrations/
│   ├── 000001_init_schema.up.sql   # Relational schema with index optimizations
│   └── 000001_init_schema.down.sql # Rollback migration
├── tests/
│   └── load/
│       └── load_test.js            # k6 2,000 RPS sustained load benchmark
├── Dockerfile                      # Multi-stage scratch build (< 20MB)
├── docker-compose.yml              # Local PostgreSQL 16 + Redis 7 stack
├── go.mod                          # Go module definitions
└── go.sum
```

---

## 🚀 Getting Started

### Prerequisites
- [Go 1.24+](https://go.dev/dl/)
- [Docker](https://www.docker.com/) & [Docker Compose](https://docs.docker.com/compose/)
- [k6](https://k6.io/) (optional, for running load tests)

### 1. Start Infrastructure (PostgreSQL & Redis)
```bash
docker compose up -d
```

### 2. Run the Engine Server
```bash
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/webhook_db?sslmode=disable"
export REDIS_URL="redis://localhost:6379"
export PORT="8080"
export WORKER_COUNT="20"

go run ./cmd/server/main.go
```
*(Note: If PostgreSQL or Redis are not reachable, the engine automatically falls back to thread-safe in-memory stores for isolated local development.)*

### 3. Register a Webhook Target Endpoint
```bash
curl -X POST http://localhost:8080/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: tenant_demo" \
  -d '{
    "url": "https://webhook.site/YOUR-UUID",
    "secret": "whsec_super_secret_signing_key_123",
    "rate_limit": 100
  }'
```

### 4. Ingest an Event (with Idempotency Key)
```bash
curl -X POST http://localhost:8080/api/v1/events \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: tenant_demo" \
  -H "X-Idempotency-Key: idemp_order_99812" \
  -d '{
    "event_type": "order.payment_succeeded",
    "payload": {
      "order_id": "ord_99812",
      "amount": 45000,
      "currency": "USD"
    }
  }'
```

### 5. Inspect Prometheus Metrics
```bash
curl http://localhost:8080/metrics
```

---

## 🧪 Testing & Quality Assurance

### Run Unit & Concurrency Tests (Zero Data Races)
```bash
go test -count=1 -race -v ./...
```

### Run k6 Load Benchmark
```bash
k6 run tests/load/load_test.js
```

---

## 🐳 Containerization

Build a lightweight, production-ready static binary in a scratch Docker container (< 20MB):
```bash
docker build -t webhook-engine:latest .
```

---

## 📄 License
This project is licensed under the MIT License.
