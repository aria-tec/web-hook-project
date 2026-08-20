# Task 6: 1-Command Demo Stack (`docker-compose.yml`) & Quickstart Verification

**Files:**
- Create: `web/Dockerfile`
- Create: `web/nginx.conf`
- Create: `cmd/mockreceiver/Dockerfile`
- Modify: `docker-compose.yml`
- Create: `tests/e2e/quickstart_test.sh`
- Update: `README.md` (to feature 1-command quickstart with visual architecture, SDK snippets, and dashboard walkthrough)

**Interfaces:**
- Produces: Production-ready `docker-compose.yml` spinning up:
  1. `postgres` (PostgreSQL 16 on `:5432` with auto-migration)
  2. `redis` (Redis 7 on `:6379`)
  3. `engine` (Go Engine server on `:8080`, depends on postgres & redis health checks)
  4. `mock-receiver` (Go Mock Webhook Receiver on `:9090`)
  5. `dashboard` (Nginx serving React SPA on `:3000` with reverse proxy to engine)

**Requirements:**
1. Multi-stage `web/Dockerfile` (Node 22 build $\rightarrow$ Nginx Alpine, < 25MB).
2. `web/nginx.conf` with gzip compression, SPA fallback `try_files $uri $uri/ /index.html;`, and proxy passes for `/api/`, `/healthz`, and `/metrics` to `engine:8080`.
3. `cmd/mockreceiver/Dockerfile` minimal Scratch / Alpine runtime image.
4. `tests/e2e/quickstart_test.sh`:
   - Automates the full reliability verification:
     a. Checks health of all 5 services (`/healthz`).
     b. Registers tenant `tenant_quickstart` and endpoints (`:9090/webhook/success`, `:9090/webhook/flaky`, `:9090/webhook/poison`).
     c. Publishes normal event $\rightarrow$ verifies 200 OK delivery.
     d. Publishes flaky event $\rightarrow$ verifies retry attempts recorded.
     e. Publishes poison event $\rightarrow$ verifies routing to DLQ.
     f. Calls `POST /api/v1/dlq/replay` $\rightarrow$ verifies fresh timestamp re-queueing and status updates.
5. All Go tests passing (`go test -race ./...`) and GitNexus index fresh (`node .gitnexus/run.cjs analyze`).
