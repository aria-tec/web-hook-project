# Architecture Design Specification: Distributed Event & Webhook Reliability Engine (Mini-Svix)

**Status:** Proposed  
**Author:** Antigravity Engineering (Pair-Programming with User)  
**Date:** 2026-08-20  
**Stack:** Go 1.23+, PostgreSQL 16, Redis Streams 7, Docker, k6, Prometheus/Grafana  
**Code Intelligence:** GitNexus (Graph-based call graph, impact analysis, flow tracking)

---

## 1. Executive Summary & Goals

### 1.1 Objective
Membangun sistem *Production-Grade Distributed Event & Webhook Reliability Engine* (terinspirasi dari arsitektur inti Svix dan Stripe Events) yang mampu menangani pengiriman event/webhook dalam skala tinggi (1.000–3.000 RPS) dengan garansi keandalan (*zero data loss*), ketahanan kegagalan (*fault tolerance*), dan perlindungan keamanan egress (*SSRF protection*).

### 1.2 Key Metrics & Success Criteria
1. **Throughput & Latency:** Mampu menerima ingestion $\ge 2.000\text{ RPS}$ dengan $\text{p99 latency} < 40\text{ms}$ pada API layer.
2. **Zero-Event Loss (Dual-Write Safety):** Menggunakan *Transactional Outbox Pattern* di PostgreSQL sehingga event tersimpan permanen sebelum masuk antrean.
3. **Strict Concurrency & Idempotency:** Mencegah *double-send* dan *race condition* menggunakan Redis distributed lock dan DB unique constraints.
4. **Automated Retry & DLQ:** *Exponential Backoff with Full Jitter* (misal: 5s, 30s, 2m, 10m, 1h) dan rute otomatis ke Dead Letter Queue (DLQ) saat downstream gagal terus menerus.
5. **Egress Security:** Perlunya SSRF filter untuk memblokir DNS rebinding dan akses ke private subnet (RFC 1918, RFC 4193) dan AWS/GCP metadata (`169.254.169.254`).
6. **Code Intelligence:** Terindeks penuh di **GitNexus** untuk visualisasi execution flows, pemantauan blast radius (`impact`), dan verifikasi perubahan (`detect_changes`).

---

## 2. System Architecture & High-Level Design

```mermaid
flowchart TD
    subgraph Ingestion ["1. Ingestion Layer (REST API)"]
        Client["Client / Producer"] -->|POST /api/v1/events| IngestionHandler["Ingestion API Handler"]
        IngestionHandler -->|Check & Set Lock| IdempGuard["Idempotency Guard<br>(Redis Key)"]
        IngestionHandler -->|BEGIN TX| DBOutbox[("PostgreSQL 16<br>events & outbox_events")]
    end

    subgraph OutboxDispatcher ["2. Transactional Outbox Relay"]
        DBOutbox -->|Poll / Stream (SKIP LOCKED)| OutboxRelay["Outbox Relay Service"]
        OutboxRelay -->|XADD| RedisQueue[("Redis Streams<br>stream:events:pending")]
    end

    subgraph WorkerPool ["3. Resilient Worker Dispatcher"]
        RedisQueue -->|XREADGROUP Consumer Group| WorkerMgr["Worker Pool Manager<br>(Bounded Goroutines)"]
        WorkerMgr -->|Check Rate Limit| TokenBucket["Token Bucket / Circuit Breaker"]
        WorkerMgr -->|Sign HMAC-SHA256| HMACSigner["Payload Signer"]
        HMACSigner -->|Safe HTTP Client (SSRF Guard)| ExtConsumer["Third-Party Webhook Endpoint"]
    end

    subgraph FailureHandling ["4. Failure & Retry Engine"]
        ExtConsumer -->|HTTP 200/201 OK| SuccessAck["XACK Stream & Mark Delivered"]
        ExtConsumer -->|HTTP 5xx / Timeout / ConnRefused| RetryScheduler["Exponential Backoff Scheduler<br>(ZSET delay queue)"]
        RetryScheduler -->|Max Retries Exceeded| DLQ[("PostgreSQL & Redis DLQ<br>Manual Replay Available")]
    end

    subgraph Telemetry ["5. Observability & GitNexus"]
        WorkerMgr -.->|Metrics| Prometheus["Prometheus & Grafana"]
        WorkerMgr -.->|Trace / Impact| GitNexusEngine["GitNexus Code Intelligence"]
    end
```

---

## 3. Detailed Component Specifications

### 3.1 Ingestion Layer (`internal/api` & `internal/idempotency`)
* **Endpoint:** `POST /api/v1/events`
* **Headers:** 
  * `X-Tenant-ID`: Identitas tenant / organisasi pengirim.
  * `X-Idempotency-Key`: Kunci unik untuk menjamin permintaan tidak diproses ulang.
* **Mekanisme Idempotency:**
  1. Redis `SET idempotency:<tenant_id>:<key> <status:processing> NX EX 120` (Lock 2 menit).
  2. Jika kunci sudah ada dan status `completed`, kembalikan *cached response* HTTP 200 secara instan tanpa memproses ulang.
  3. Jika kunci sedang diproses oleh request paralel lain, kembalikan HTTP 409 Conflict.

### 3.2 Storage & Transactional Outbox (`internal/storage`)
* **Pola Dual-Write Guard:**
  Alih-alih menulis ke PostgreSQL lalu langsung publish ke Redis (yang rentan gagal jika server mati di antara kedua langkah), kita menggunakan **Transactional Outbox**:
  1. Dalam satu transaksi database (`BEGIN ... COMMIT`):
     - Masukkan event ke tabel `events`.
     - Masukkan task ke tabel `outbox_events` (`status = 'PENDING'`).
  2. Outbox Relay Goroutine membaca `outbox_events` menggunakan query `SELECT ... FOR UPDATE SKIP LOCKED` lalu mem-publish ke Redis Streams (`XADD stream:events:pending * ...`).
  3. Setelah sukses `XADD`, ubah status outbox menjadi `PROCESSED`.

### 3.3 Dispatcher, Bounded Worker Pool & Egress Security (`internal/worker`, `internal/dispatcher`)
* **Worker Concurrency:** Menggunakan channel-based worker pool (misal: 100–500 goroutine workers terkonfigurasi) yang membaca dari Consumer Group Redis Streams (`XREADGROUP`).
* **Egress Safe HTTP Client (Anti-SSRF):**
  * Custom `net.Dialer` dengan `Control` callback yang memvalidasi resolusi IP:
    - Blokir IPv4/IPv6 loopback (`127.0.0.0/8`, `::1`).
    - Blokir Private Subnet (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`).
    - Blokir Cloud Metadata link-local (`169.254.169.254`, `fe80::/10`).
    - Blokir Multicast & Broadcast.
* **HTTP Connection Pooling:**
  * `MaxIdleConns: 2000`, `MaxIdleConnsPerHost: 200`, `IdleConnTimeout: 90s` untuk mencegah *TCP TIME_WAIT socket exhaustion*.
* **Cryptographic Signing (HMAC-SHA256):**
  * Header `Svix-Signature` / `X-Signature-256`: `t=<timestamp>,v1=<hmac_sha256(secret, t + "." + payload)>`.

### 3.4 Exponential Backoff Retry & Dead Letter Queue (`internal/retry`)
* **Jadwal Retry:** $T_{\text{delay}} = \min(T_{\text{max}}, T_{\text{base}} \times 2^{\text{attempt}}) \pm \text{jitter}$.
  * Default: Percobaan 1 (5s), 2 (30s), 3 (2m), 4 (10m), 5 (1h).
* **Dead Letter Queue (DLQ):**
  * Jika setelah 5 kali percobaan downstream endpoint tetap gagal (5xx, connection refused, DNS timeout), event ditandai sebagai `FAILED_DLQ` di database.
  * Disediakan endpoint `POST /api/v1/dlq/replay` untuk memicu pemrosesan ulang secara manual setelah pihak downstream memperbaiki server mereka.

---

## 4. PostgreSQL Database Schema (`migrations/`)

```sql
-- Migration 000001_init_schema.up.sql

CREATE TABLE IF NOT EXISTS tenants (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS endpoints (
    id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    secret VARCHAR(255) NOT NULL,
    rate_limit INT NOT NULL DEFAULT 100, -- Max requests per second
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS events (
    id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    event_type VARCHAR(100) NOT NULL,
    idempotency_key VARCHAR(128),
    payload JSONB NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING', -- PENDING, DELIVERED, FAILED, DLQ
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_tenant_idempotency UNIQUE(tenant_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS outbox_events (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(64) NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING', -- PENDING, PUBLISHED, FAILED
    retry_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS delivery_attempts (
    id VARCHAR(64) PRIMARY KEY,
    event_id VARCHAR(64) NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    endpoint_id VARCHAR(64) NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
    attempt_number INT NOT NULL,
    response_status INT,
    response_body TEXT,
    duration_ms INT NOT NULL,
    status VARCHAR(32) NOT NULL, -- SUCCESS, RETRYING, FAILED
    error_message TEXT,
    executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexing for high-speed queries
CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox_events(status, id) WHERE status = 'PENDING';
CREATE INDEX IF NOT EXISTS idx_events_tenant_created ON events(tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_attempts_event ON delivery_attempts(event_id);
```

---

## 5. Testing & Verification Strategy (TDD)

```
Test Pyramid for Webhook Engine:
        ▲
       / \      k6 Load Tests (3.000 RPS Benchmark & pprof memory checks)
      /---\     
     /     \    Integration Tests (testcontainers-go: Real Postgres 16 & Redis 7)
    /-------\   
   /         \  Unit Tests (go test -race: HMAC, SSRF Guard, Exponential Backoff, Idempotency)
```

1. **Unit Tests (`go test -race ./...`):**
   * Verifikasi deterministik HMAC-SHA256 signature generator & validator.
   * Verifikasi SSRF Guard (memastikan IP lokal & `169.254.169.254` tertolak).
   * Verifikasi Exponential Backoff formula dengan full jitter.
   * Validasi concurrency race conditions pada in-memory buffers.
2. **Integration Tests (`testcontainers-go`):**
   * Inisialisasi container Postgres & Redis sungguhan.
   * Uji alur lengkap: Ingestion $\rightarrow$ Outbox $\rightarrow$ Redis Stream $\rightarrow$ Worker Dispatch $\rightarrow$ Success Delivery.
   * Uji skenario kegagalan: Simulasi mock downstream server mengembalikan 500 error $\rightarrow$ verifikasi retry bertahap $\rightarrow$ verifikasi masuk DLQ pada percobaan ke-5.
3. **Load Testing (`k6`):**
   * Ingestion stress test: 2.000 RPS stabil selama 60 detik. Target: 0% packet drop, p99 latency < 40ms.

---

## 6. GitNexus Code Intelligence Integration

Untuk menjamin pemeliharaan jangka panjang dan visibilitas arsitektur:
1. Repositori diindeks menggunakan **GitNexus** (`node .gitnexus/run.cjs analyze`).
2. Setiap modifikasi simbol inti (`IngestionHandler`, `OutboxRelay`, `WorkerPool`, `SSRFGuard`) wajib diawali dengan verifikasi dampak `impact({target: "symbolName", direction: "upstream"})`.
3. Setiap commit diproteksi dengan `detect_changes()` untuk memastikan tidak ada efek samping regresi pada *execution flows*.

---

## 7. Project Directory Structure

```
web-hook-project/
├── cmd/
│   └── server/
│       └── main.go                 # Application Entrypoint
├── internal/
│   ├── api/                        # HTTP Handlers & Router (Chi / Standard Library)
│   │   ├── handler.go
│   │   └── middleware.go
│   ├── config/                     # Configuration Loader (Env / Yaml)
│   ├── dispatcher/                 # HTTP Egress Client with SSRF protection & HMAC
│   │   ├── client.go
│   │   ├── hmac.go
│   │   └── ssrf.go
│   ├── domain/                     # Core Entities & Interfaces
│   │   ├── event.go
│   │   ├── endpoint.go
│   │   └── attempt.go
│   ├── idempotency/                # Redis Distributed Lock & Cache Guard
│   │   └── guard.go
│   ├── outbox/                     # Transactional Outbox Relay Goroutine
│   │   └── relay.go
│   ├── queue/                      # Redis Streams Producer & Consumer Group
│   │   └── stream.go
│   ├── retry/                      # Exponential Backoff Scheduler & DLQ
│   │   └── scheduler.go
│   ├── storage/                    # Postgres Repository (pgxpool)
│   │   ├── postgres.go
│   │   └── repository.go
│   └── telemetry/                  # Prometheus Metrics & Healthcheck
│       └── metrics.go
├── migrations/                     # SQL Migrations (Up & Down)
│   ├── 000001_init_schema.up.sql
│   └── 000001_init_schema.down.sql
├── tests/
│   ├── integration/                # testcontainers-go integration suite
│   └── load/                       # k6 benchmark scripts
│       └── load_test.js
├── docs/
│   └── superpowers/
│       └── specs/
│           └── 2026-08-20-webhook-engine-design.md
├── docker-compose.yml              # Local Dev (Postgres 16 + Redis 7 + Grafana)
├── Dockerfile                      # Multi-stage Scratch Build (<20MB)
├── go.mod
└── go.sum
```
