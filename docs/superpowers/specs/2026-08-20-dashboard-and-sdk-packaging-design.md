# Mini-Svix Operational Dashboard, Developer SDKs & Demo Stack Design Specification

**Date:** 2026-08-20  
**Author:** Pair Programming Session (Council & Antigravity)  
**Status:** Validated by LLM Council  

---

## 1. Overview & Objectives

Following the successful creation and hardening of the core Go-based Distributed Webhook & Event Reliability Engine (95k reqs @ 1.72ms p99, Transactional Outbox, Redis Streams, HMAC-SHA256, SSRF protection, and DLQ), this specification defines the **Packaging & Turnkey Developer Experience Layer**:
1. **Interactive Operational Dashboard (Web SPA):** Real-time webhook delivery stream, deep payload/HMAC inspector, 1-click DLQ replay, and instant simulation triggers.
2. **Lightweight Developer SDKs (`sdk/typescript` and `sdk/go`):** Zero-dependency Producer (Publish) and Consumer (constant-time HMAC signature verification) libraries.
3. **1-Command Interactive Stack (`docker-compose.yml`):** Complete local runtime with PostgreSQL 16, Redis 7, Go Engine (`:8080`), Web Dashboard (`:3000`), and Mock Webhook Receiver (`:9090`).

---

## 2. System Architecture & Component Boundaries

```
                 ┌──────────────────────────────────────────────────────────┐
                 │                 Interactive Web Dashboard                │
                 │     (React / Tailwind / UI-UX Pro Max Glassmorphism)     │
                 │   [Live Stream]  [Payload Inspector]  [1-Click DLQ]      │
                 └──────────────┬────────────────────────────▲──────────────┘
                                │ REST Actions               │ SSE Event Stream
                                ▼                            │
                 ┌───────────────────────────────────────────┴──────────────┐
                 │             Mini-Svix Backend Engine (Go)                │
                 │   - REST API (Ingestion, Endpoints, DLQ Replay)          │
                 │   - SSE /api/v1/events/stream (Live Attempt Feed)        │
                 │   - Transactional Outbox Relay & Redis Stream Queue      │
                 │   - SSRF-Safe Dispatcher with HMAC-SHA256 Signing        │
                 └──────────┬─────────────────────────────┬─────────────────┘
                            │                             │
               PostgreSQL 16│                  Redis 7    │ Stream & Locks
              (Events/Outbox)                          (Streams/Idemp)
                            │                             │
                            ▼                             ▼
                 ┌──────────────────────────────────────────────────────────┐
                 │         Mock Webhook Receiver & Echo Server (:9090)      │
                 │  - Configurable Mode: 200 OK, 500 Flaky, 400 Poison Pill │
                 └──────────────────────────────────────────────────────────┘
```

### 2.1 Single Source of Truth Invariant
* The **Go Engine** is the sole authority for state transitions (`PENDING`, `DELIVERED`, `RETRYING`, `DLQ`), retry schedules, and cryptographic signing.
* The **Web Dashboard** and **SDKs** are strictly presentation/client layers and contain no duplicated delivery or retry logic.

---

## 3. Component Specifications

### 3.1 Operational Dashboard (`web/`)
* **Technology:** React SPA + Vite + Tailwind CSS + Lucide Icons (lightweight, zero Node runtime bloat in production).
* **UI/UX Guidelines (from `ui-ux-pro-max`):**
  * **Theme:** Dark Tech Glassmorphism (`#0F172A` Slate-950 background, `#1B2336` card surface, `#22C55E` success emerald, `#F59E0B` retry amber, `#EF4444` DLQ rose, `#38BDF8` cyan highlight).
  * **Typography:** `Inter` (system body) + `Fira Code` (JSON payload, HMAC header, and telemetry latencies).
  * **Zero-Emoji Rule:** All status and navigation elements use crisp Lucide/Phosphor vector icons with `aria-hidden="true"` or accessible labels.
* **Core Views & Capabilities:**
  1. **Live Webhook Delivery Stream:**
     - Connected via Server-Sent Events (`GET /api/v1/events/stream`).
     - **Ring Buffer Protection:** Maintains a bounded in-memory array of the latest 200 events in state to prevent browser memory exhaustion during traffic spikes.
     - Column fields: Attempt ID, Tenant ID, Event Type, Target URL, Status Badge, HTTP Code, Duration (ms), Timestamp.
  2. **Deep Payload & Security Inspector Modal:**
     - Formatted JSON payload viewer with copy button.
     - Security details: `X-Webhook-Signature` (v1 HMAC-SHA256), `X-Webhook-Timestamp`, and SSRF safety indicator (destination IP validation).
     - Response body & headers from destination endpoint.
  3. **Interactive 1-Click DLQ Manual Replay:**
     - Filtered view of events currently in `DLQ` status.
     - Single or multi-select replay triggering `POST /api/v1/dlq/replay`.
     - **Replay Freshness Invariant:** Engine generates a fresh timestamp (`now`) and fresh HMAC signature upon replay so destination receivers don't reject it for signature expiry.
  4. **Simulation & Quick Start Action Bar:**
     - **Zero-Empty State:** Banner with *"Click here to fire 5 demo events"* on initial load.
     - Quick action buttons:
       - 🟢 *Send Normal Event* (fires to mock endpoint `:9090/webhook` returning `200 OK`).
       - 🟡 *Simulate 500 Flaky Endpoint* (fires to `:9090/flaky` returning `500`, triggers exponential backoff live).
       - 🔴 *Simulate 400 Poison Pill* (fires to `:9090/poison` returning `400`, routes immediately to DLQ).
  5. **SDK Code Snippet Tab:**
     - Instant copy-paste code examples for publishing and signature verification in TypeScript and Go.

---

### 3.2 Developer SDKs

#### A. TypeScript / Node.js SDK (`sdk/typescript`)
* **Zero External Dependencies:** Built using native `fetch` and the standard Web Crypto API (`crypto.subtle`). Compatible with Node 18+, Bun, Deno, Cloudflare Workers, and modern browsers.
* **Producer Client (`WebhookClient`):**
  ```typescript
  import { WebhookClient } from "@minisvix/client";

  const client = new WebhookClient({
    baseUrl: "http://localhost:8080",
    tenantId: "tenant_alpha"
  });

  // Publish event with automatic idempotency key
  const event = await client.publish("payment.succeeded", {
    orderId: "ord_999",
    amount: 150000
  });

  // DLQ operations
  const dlqEvents = await client.dlq.list();
  await client.dlq.replay([event.id]);
  ```
* **Consumer Signature Verifier (`WebhookSignature`):**
  ```typescript
  import { WebhookSignature } from "@minisvix/client";

  const isValid = WebhookSignature.verify({
    secret: "whsec_...",
    header: request.headers["x-webhook-signature"],
    payload: rawBodyString,
    toleranceSeconds: 300 // default 5 minutes
  });
  ```

#### B. Go SDK (`sdk/go/webhookclient`)
* **Standard Library Only:** Built with standard `net/http`, `crypto/hmac`, and `crypto/sha256`.
* **Producer & Verifier:**
  ```go
  package main

  import (
    "context"
    "web-hook-project/sdk/go/webhookclient"
  )

  func main() {
    client := webhookclient.NewClient("http://localhost:8080", "tenant_alpha")
    
    // Publish
    resp, err := client.Publish(context.Background(), "invoice.paid", map[string]any{"id": "inv_123"})
    
    // Verify
    valid := webhookclient.VerifySignature("whsec_...", headerSig, rawPayloadBytes, 300)
  }
  ```

---

### 3.3 Mock Webhook Receiver (`cmd/mockreceiver`)
* Ultra-lightweight Go HTTP server running on `:9090`:
  - `POST /webhook/success` $\rightarrow$ returns `200 OK {"received": true}`.
  - `POST /webhook/flaky` $\rightarrow$ returns `500 Internal Server Error` on first 2 attempts, `200 OK` on 3rd attempt.
  - `POST /webhook/poison` $\rightarrow$ returns `400 Bad Request {"error": "invalid payload schema"}`.
  - `POST /webhook/slow` $\rightarrow$ sleeps for 4s before returning `200 OK`.
  - Records all received requests for verification.

---

### 3.4 1-Command Demo Stack (`docker-compose.yml`)
* Unified Docker orchestration:
  1. `postgres` (port `5432`): PostgreSQL 16 Alpine with init schema.
  2. `redis` (port `6379`): Redis 7 Alpine.
  3. `engine` (port `8080`): Go backend server with REST API, outbox relay, worker pool, and `/metrics`.
  4. `mock-receiver` (port `9090`): Mock target sink.
  5. `dashboard` (port `3000`): Static SPA served via lightweight Nginx/Alpine.

---

## 4. Verification & Testing Plan

1. **Unit & Interoperability Tests:**
   - Test TypeScript SDK HMAC verification against Go backend HMAC generator (`internal/dispatcher/hmac.go`) across various edge cases (special characters, whitespace, timestamp tolerance).
   - Test Go SDK against live Go Engine HTTP API.
2. **Dashboard Resilience & Ring Buffer Test:**
   - Verify that streaming 1,000 rapid delivery attempts caps memory at 200 items without memory leaks or UI lag.
3. **End-to-End Demo Flow (`docker compose up`):**
   - Run `docker compose up -d`.
   - Open `http://localhost:3000`.
   - Click "Simulate Flaky Endpoint" $\rightarrow$ verify UI shows `RETRYING` badges with exponential backoff timers.
   - Click "Simulate Poison Pill" $\rightarrow$ verify UI displays event in `DLQ`.
   - Click "1-Click DLQ Replay" $\rightarrow$ verify event is re-queued, re-dispatched, and transitions to `DELIVERED`.
