# Mini-Svix Dashboard, SDKs & Demo Stack Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the turnkey developer presentation & ecosystem layer for Mini-Svix: a high-performance React SPA dashboard with real-time SSE stream & 1-click DLQ replay, zero-dependency TypeScript & Go SDKs, and a 1-command Docker Compose stack.

**Architecture:** The Go Engine remains the single source of truth, broadcasting delivery attempts via SSE (`GET /api/v1/events/stream`) and serving REST endpoints for ingestion, endpoint management, and DLQ replay with fresh signature recalculation. A lightweight React SPA (`web/`) with a 200-event ring buffer connects to SSE and provides deep HMAC payload inspection, simulation triggers, and DLQ recovery. Zero-dependency SDKs in TypeScript (`sdk/typescript`) and Go (`sdk/go/webhookclient`) provide idiomatic publishing and constant-time signature verification. Everything runs out-of-the-box via `docker compose up`.

**Tech Stack:** Go 1.23+, React 18, Vite, Tailwind CSS, Lucide Icons, Web Crypto API, Docker Compose, PostgreSQL 16, Redis 7.

**Spec:** [`docs/superpowers/specs/2026-08-20-dashboard-and-sdk-packaging-design.md`](file:///Users/arias/Documents/antigravity/ExProject/web-hook-project/docs/superpowers/specs/2026-08-20-dashboard-and-sdk-packaging-design.md)

## Global Constraints
- Single Source of Truth: Go backend engine manages all delivery state transitions, retry schedules, and signatures.
- Zero External Dependencies for SDKs: TypeScript uses native `fetch` + Web Crypto API (`crypto.subtle`); Go uses standard library `net/http` + `crypto/hmac`.
- UI Resilience: Live Delivery Stream must enforce a 200-event in-memory ring buffer to prevent browser freeze during traffic bursts.
- Fresh Replay Invariant: DLQ manual replay must generate a new timestamp (`now`) and fresh HMAC signature upon replay.
- Rapid Startup: Entire stack (`docker compose up`) must boot and be fully operational in `< 15` seconds without heavy Node.js SSR containers.

---

### Task 1: Go Engine SSE Live Stream Broadcast & CORS Support

**Files:**
- Create: `internal/api/stream.go`
- Modify: `internal/api/router.go`
- Modify: `internal/api/handler.go`
- Test: `internal/api/stream_test.go`

**Interfaces:**
- Produces: `api.SSEBroker` broadcasting delivery attempts to connected HTTP clients via `GET /api/v1/events/stream`.
- Produces: `Handler.HandleEventStream(w http.ResponseWriter, r *http.Request)`.
- Updates: `NewRouter` with CORS middleware permitting `localhost:3000` and `*` in development.

- [ ] **Step 1: Write failing test for SSE broker and event streaming**

```go
// internal/api/stream_test.go
package api_test

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"web-hook-project/internal/api"
	"web-hook-project/internal/domain"
	"web-hook-project/internal/idempotency"
	"web-hook-project/internal/storage"
)

func TestSSE_StreamDeliveryAttempts(t *testing.T) {
	repo := storage.NewMemoryRepository()
	guard := idempotency.NewMemoryGuard()
	broker := api.NewSSEBroker()
	go broker.Run(context.Background())

	h := api.NewHandler(repo, guard).WithSSEBroker(broker)
	router := api.NewRouter(h)

	server := httptest.NewServer(router)
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/api/v1/events/stream", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to connect to SSE stream: %v", err)
	}
	defer resp.Body.Close()

	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("expected Content-Type text/event-stream, got %s", resp.Header.Get("Content-Type"))
	}

	// Broadcast test attempt
	attempt := &domain.DeliveryAttempt{
		ID:             "att_test_sse",
		EventID:        "evt_test_123",
		EndpointID:     "ep_test_456",
		AttemptNumber:  1,
		ResponseStatus: 200,
		Status:         domain.DeliveryStatusSuccess,
		ExecutedAt:     time.Now(),
	}

	time.Sleep(50 * time.Millisecond)
	broker.Broadcast(attempt)

	reader := bufio.NewReader(resp.Body)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read SSE data: %v", err)
	}

	if !strings.Contains(line, "data:") && !strings.Contains(line, "att_test_sse") {
		// Read next line for data payload
		line2, _ := reader.ReadString('\n')
		if !strings.Contains(line+line2, "att_test_sse") {
			t.Fatalf("expected SSE stream to contain att_test_sse, got: %s %s", line, line2)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/api/... -run TestSSE_StreamDeliveryAttempts -v`
Expected: FAIL (types/methods undefined)

- [ ] **Step 3: Implement SSE Broker, Handler method, and CORS middleware**

Implement:
- `internal/api/stream.go`: SSE broker with client registration/unregistration channels, thread-safe broadcast, and keep-alive ping.
- Wire broker with `internal/worker/pool.go` or dispatcher to broadcast attempts when recorded.
- Update `internal/api/router.go` to add `GET /api/v1/events/stream` and CORS headers.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/api/... -v`
Expected: PASS

- [ ] **Step 5: Commit SSE streaming capability**

```bash
git add internal/api/ internal/worker/
git commit -m "feat(api): implement real-time SSE delivery attempt streaming and CORS support"
```

---

### Task 2: Mock Webhook Receiver & Echo Server (`cmd/mockreceiver`)

**Files:**
- Create: `cmd/mockreceiver/main.go`
- Test: `cmd/mockreceiver/main_test.go`

**Interfaces:**
- Produces: Runnable mock server listening on `:9090` supporting:
  - `POST /webhook/success` $\rightarrow$ returns 200 OK.
  - `POST /webhook/flaky` $\rightarrow$ returns 500 on first 2 requests, 200 on 3rd request.
  - `POST /webhook/poison` $\rightarrow$ returns 400 Bad Request.
  - `POST /webhook/slow` $\rightarrow$ delays 4s then returns 200 OK.
  - `GET /requests` $\rightarrow$ returns list of captured requests with headers for inspection.

- [ ] **Step 1: Write unit tests for mock receiver endpoints**

```go
// cmd/mockreceiver/main_test.go
package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMockReceiver_Routes(t *testing.T) {
	server := NewMockServer()

	// 1. Success endpoint
	req := httptest.NewRequest("POST", "/webhook/success", bytes.NewBufferString(`{"test":true}`))
	rr := httptest.NewRecorder()
	server.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 for /webhook/success, got %d", rr.Code)
	}

	// 2. Poison endpoint
	req = httptest.NewRequest("POST", "/webhook/poison", bytes.NewBufferString(`{"bad":"data"}`))
	rr = httptest.NewRecorder()
	server.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for /webhook/poison, got %d", rr.Code)
	}

	// 3. Flaky endpoint (fails twice with 500, succeeds on 3rd)
	for i := 1; i <= 3; i++ {
		req = httptest.NewRequest("POST", "/webhook/flaky", bytes.NewBufferString(`{}`))
		rr = httptest.NewRecorder()
		server.Handler().ServeHTTP(rr, req)
		if i < 3 && rr.Code != http.StatusInternalServerError {
			t.Errorf("attempt %d: expected 500, got %d", i, rr.Code)
		} else if i == 3 && rr.Code != http.StatusOK {
			t.Errorf("attempt 3: expected 200, got %d", rr.Code)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/mockreceiver/... -v`
Expected: FAIL

- [ ] **Step 3: Implement mock receiver server**

Write `cmd/mockreceiver/main.go` with structured request capture buffer, atomic state for flaky retry simulation, and CLI flags (`-port 9090`).

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./cmd/mockreceiver/... -v`
Expected: PASS

- [ ] **Step 5: Commit mock receiver**

```bash
git add cmd/mockreceiver/
git commit -m "feat(mockreceiver): implement standalone mock target sink with success, flaky, and poison routes"
```

---

### Task 3: Zero-Dependency TypeScript SDK (`sdk/typescript`)

**Files:**
- Create: `sdk/typescript/package.json`
- Create: `sdk/typescript/tsconfig.json`
- Create: `sdk/typescript/src/index.ts`
- Create: `sdk/typescript/src/client.ts`
- Create: `sdk/typescript/src/signature.ts`
- Test: `sdk/typescript/tests/signature.test.ts`

**Interfaces:**
- Produces: `@minisvix/client` npm package.
- Produces: `WebhookClient` class with `publish(eventType, payload, options?)`, `dlq.list()`, `dlq.replay(eventIds)`.
- Produces: `WebhookSignature.verify(options: { secret, header, payload, toleranceSeconds? }): Promise<boolean>`.

- [ ] **Step 1: Write unit test for HMAC signature verification in TypeScript**

```typescript
// sdk/typescript/tests/signature.test.ts
import { describe, it, expect } from "vitest";
import { WebhookSignature } from "../src/signature";

describe("WebhookSignature", () => {
  const secret = "whsec_test_secret_12345";
  const payload = JSON.stringify({ event: "order.completed", amount: 50000 });

  it("should verify valid signature header generated with known timestamp", async () => {
    const timestamp = Math.floor(Date.now() / 1000);
    const header = await WebhookSignature.sign(secret, timestamp, payload);
    
    const isValid = await WebhookSignature.verify({
      secret,
      header,
      payload,
      toleranceSeconds: 300
    });
    expect(isValid).toBe(true);
  });

  it("should reject tampered payload", async () => {
    const timestamp = Math.floor(Date.now() / 1000);
    const header = await WebhookSignature.sign(secret, timestamp, payload);
    
    const isValid = await WebhookSignature.verify({
      secret,
      header,
      payload: JSON.stringify({ event: "order.completed", amount: 99999 }),
      toleranceSeconds: 300
    });
    expect(isValid).toBe(false);
  });

  it("should reject expired timestamp", async () => {
    const oldTimestamp = Math.floor(Date.now() / 1000) - 600; // 10 minutes ago
    const header = await WebhookSignature.sign(secret, oldTimestamp, payload);
    
    const isValid = await WebhookSignature.verify({
      secret,
      header,
      payload,
      toleranceSeconds: 300 // 5 min tolerance
    });
    expect(isValid).toBe(false);
  });
});
```

- [ ] **Step 2: Implement zero-dependency TypeScript SDK**

Write `sdk/typescript/src/signature.ts` using `crypto.subtle.importKey`, `crypto.subtle.sign`, and constant-time string comparison, and `sdk/typescript/src/client.ts` using standard `fetch`.

- [ ] **Step 3: Run TypeScript tests to verify they pass**

Run: `cd sdk/typescript && npm test`
Expected: PASS

- [ ] **Step 4: Commit TypeScript SDK**

```bash
git add sdk/typescript/
git commit -m "feat(sdk-ts): implement zero-dependency TypeScript client and Web Crypto signature verifier"
```

---

### Task 4: Zero-Dependency Go SDK (`sdk/go/webhookclient`)

**Files:**
- Create: `sdk/go/webhookclient/client.go`
- Create: `sdk/go/webhookclient/signature.go`
- Test: `sdk/go/webhookclient/client_test.go`

**Interfaces:**
- Produces: `webhookclient.Client` (`Publish(ctx, eventType, payload, opts...)`, `ListDLQ(ctx)`, `ReplayDLQ(ctx, eventIDs)`).
- Produces: `webhookclient.VerifySignature(secret, header, payload, toleranceSeconds) bool`.

- [ ] **Step 1: Write unit tests for Go SDK client and verifier**

```go
// sdk/go/webhookclient/client_test.go
package webhookclient_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"web-hook-project/sdk/go/webhookclient"
)

func TestGoSDK_PublishAndVerify(t *testing.T) {
	secret := "whsec_sdk_test_98765"
	payload := map[string]interface{}{"invoice_id": "inv_001", "amount": 25000}
	rawBytes, _ := json.Marshal(payload)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"id":         "evt_sdk_123",
			"status":     "PENDING",
			"created_at": time.Now(),
		})
	}))
	defer server.Close()

	client := webhookclient.NewClient(server.URL, "tenant_test")
	resp, err := client.Publish(context.Background(), "invoice.created", payload)
	if err != nil {
		t.Fatalf("expected successful publish, got %v", err)
	}
	if resp.ID != "evt_sdk_123" {
		t.Errorf("expected ID evt_sdk_123, got %s", resp.ID)
	}

	// Verify Signature Helper
	header := webhookclient.SignPayload(secret, time.Now().Unix(), rawBytes)
	if !webhookclient.VerifySignature(secret, header, rawBytes, 300) {
		t.Error("expected valid signature verification")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./sdk/go/... -v`
Expected: FAIL

- [ ] **Step 3: Implement Go SDK**

Write `sdk/go/webhookclient/signature.go` and `client.go` using only Go standard library packages.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./sdk/go/... -v`
Expected: PASS

- [ ] **Step 5: Commit Go SDK**

```bash
git add sdk/go/
git commit -m "feat(sdk-go): implement zero-dependency Go client library and HMAC verifier"
```

---

### Task 5: Operational Dashboard Web SPA (`web/`)

**Files:**
- Create: `web/package.json`
- Create: `web/index.html`
- Create: `web/vite.config.ts`
- Create: `web/tailwind.config.js`
- Create: `web/src/main.tsx`
- Create: `web/src/App.tsx`
- Create: `web/src/components/Navbar.tsx`
- Create: `web/src/components/StatsCards.tsx`
- Create: `web/src/components/SimulationBar.tsx`
- Create: `web/src/components/DeliveryStream.tsx`
- Create: `web/src/components/PayloadInspector.tsx`
- Create: `web/src/components/DLQManager.tsx`
- Create: `web/src/components/CodeSnippets.tsx`
- Test: `web/src/App.test.tsx`

**Interfaces:**
- Produces: Standalone React 18 SPA compiled into `web/dist/`.
- Features:
  - Real-time SSE connection with 200-item ring buffer.
  - Zero-empty state quick-start banner.
  - Dark Tech Glassmorphism theme per `ui-ux-pro-max` (`#0F172A` Slate-950).
  - 1-Click DLQ manual replay modal and simulation trigger buttons.
  - Lucide vector icons.

- [ ] **Step 1: Scaffold Vite + React + Tailwind frontend application**

Setup `web/` directory with Vite, Tailwind CSS, and Lucide React icons.

- [ ] **Step 2: Implement UI components with 200-event Ring Buffer and Dark Tech Styling**

Write components:
- `DeliveryStream.tsx`: Virtualized/bounded table with status badges (`DELIVERED` emerald, `RETRYING` amber, `DLQ` rose).
- `PayloadInspector.tsx`: Formatted JSON inspector with HMAC-SHA256 signature and SSRF safety badges.
- `DLQManager.tsx`: Real-time failed events queue with select & 1-click replay trigger.
- `SimulationBar.tsx`: Quick demo triggers (Send Normal, Simulate Flaky 500, Simulate Poison Pill 400).
- `CodeSnippets.tsx`: Copy-paste integration guides in TypeScript and Go.

- [ ] **Step 3: Run build test to verify frontend compiles cleanly**

Run: `cd web && npm run build`
Expected: Static assets generated in `web/dist/` with 0 errors.

- [ ] **Step 4: Commit Web Dashboard**

```bash
git add web/
git commit -m "feat(web): build React operational dashboard with SSE live stream, DLQ manager, and dark glassmorphism styling"
```

---

### Task 6: 1-Command Interactive Stack (`docker-compose.yml`) & Quickstart Verification

**Files:**
- Create: `web/Dockerfile`
- Create: `web/nginx.conf`
- Modify: `docker-compose.yml`
- Create: `tests/e2e/quickstart_test.sh`

**Interfaces:**
- Produces: Production-ready `docker-compose.yml` spinning up:
  - `postgres` (PostgreSQL 16)
  - `redis` (Redis 7)
  - `engine` (Go backend on `:8080`)
  - `dashboard` (Nginx serving SPA on `:3000`)
  - `mock-receiver` (Go mock sink on `:9090`)

- [ ] **Step 1: Write Nginx configuration and multi-stage Dockerfile for web dashboard**

Create lightweight `web/Dockerfile` with multi-stage build (Node build $\rightarrow$ Nginx Alpine, total image size < 25MB).

- [ ] **Step 2: Update `docker-compose.yml` with all 5 services, health checks, and seed data**

Update `docker-compose.yml` to automatically initialize the database schema, register default tenant (`tenant_demo`) and default target endpoint (`http://mock-receiver:9090/webhook/success`).

- [ ] **Step 3: Write E2E verification test script**

Create `tests/e2e/quickstart_test.sh` to automatically test the full loop:
1. Ingest normal event $\rightarrow$ assert 200 OK delivery.
2. Ingest flaky event $\rightarrow$ assert retry attempts recorded in database.
3. Ingest poison event $\rightarrow$ assert routed to DLQ.
4. Trigger DLQ replay $\rightarrow$ assert fresh timestamp and re-queued outbox event.

- [ ] **Step 4: Run full verification and GitNexus re-index**

Run:
- `go test ./...`
- `node .gitnexus/run.cjs analyze`

- [ ] **Step 5: Commit complete demo stack and documentation**

```bash
git add docker-compose.yml web/Dockerfile web/nginx.conf tests/e2e/ README.md
git commit -m "feat(compose): package 1-command demo stack and automated quickstart verification"
```
