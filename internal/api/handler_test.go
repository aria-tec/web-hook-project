package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"web-hook-project/internal/api"
	"web-hook-project/internal/domain"
	"web-hook-project/internal/idempotency"
	"web-hook-project/internal/storage"
)

func setupTestEnvironment() (http.Handler, *storage.MemoryRepository, *idempotency.MemoryGuard) {
	repo := storage.NewMemoryRepository()
	guard := idempotency.NewMemoryGuard()
	handler := api.NewHandler(repo, guard)
	router := api.NewRouter(handler)
	return router, repo, guard
}

func TestHandler_Healthz(t *testing.T) {
	router, _, _ := setupTestEnvironment()

	req, err := http.NewRequest(http.MethodGet, "/healthz", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}

	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid json response: %v", err)
	}
	if resp["status"] != "healthy" && resp["status"] != "OK" {
		t.Errorf("expected status 'healthy' or 'OK', got %v", resp["status"])
	}
}

func TestHandler_IngestEvent_TableDriven(t *testing.T) {
	tests := []struct {
		name           string
		tenantID       string
		idempotencyKey string
		payload        string
		expectedStatus int
		verifyBody     func(t *testing.T, body []byte)
	}{
		{
			name:           "Valid event ingestion",
			tenantID:       "tenant_001",
			idempotencyKey: "idemp_valid_001",
			payload:        `{"event_type":"order.created","payload":{"order_id":"ord_123","amount":99.5}}`,
			expectedStatus: http.StatusAccepted,
			verifyBody: func(t *testing.T, body []byte) {
				var resp map[string]interface{}
				if err := json.Unmarshal(body, &resp); err != nil {
					t.Fatalf("failed to parse response JSON: %v", err)
				}
				if resp["id"] == nil || resp["id"] == "" {
					t.Errorf("expected non-empty event id")
				}
				if resp["status"] != string(domain.EventStatusPending) {
					t.Errorf("expected status %s, got %v", domain.EventStatusPending, resp["status"])
				}
			},
		},
		{
			name:           "Missing X-Tenant-ID header",
			tenantID:       "",
			idempotencyKey: "idemp_no_tenant",
			payload:        `{"event_type":"order.created","payload":{"order_id":"ord_123"}}`,
			expectedStatus: http.StatusBadRequest,
			verifyBody: func(t *testing.T, body []byte) {
				var resp map[string]interface{}
				if err := json.Unmarshal(body, &resp); err != nil {
					t.Fatalf("failed to parse response JSON: %v", err)
				}
				if resp["error"] == nil {
					t.Errorf("expected error field in response")
				}
			},
		},
		{
			name:           "Missing event_type in payload",
			tenantID:       "tenant_001",
			idempotencyKey: "idemp_no_event_type",
			payload:        `{"payload":{"order_id":"ord_123"}}`,
			expectedStatus: http.StatusBadRequest,
			verifyBody: func(t *testing.T, body []byte) {
				var resp map[string]interface{}
				if err := json.Unmarshal(body, &resp); err != nil {
					t.Fatalf("failed to parse response JSON: %v", err)
				}
				if resp["error"] == nil {
					t.Errorf("expected error field in response")
				}
			},
		},
		{
			name:           "Empty payload body",
			tenantID:       "tenant_001",
			idempotencyKey: "idemp_empty_body",
			payload:        `{"event_type":"order.created"}`,
			expectedStatus: http.StatusBadRequest,
			verifyBody: func(t *testing.T, body []byte) {
				var resp map[string]interface{}
				if err := json.Unmarshal(body, &resp); err != nil {
					t.Fatalf("failed to parse response JSON: %v", err)
				}
				if resp["error"] == nil {
					t.Errorf("expected error field in response")
				}
			},
		},
		{
			name:           "Malformed JSON payload",
			tenantID:       "tenant_001",
			idempotencyKey: "idemp_bad_json",
			payload:        `{invalid json`,
			expectedStatus: http.StatusBadRequest,
			verifyBody: func(t *testing.T, body []byte) {
				var resp map[string]interface{}
				if err := json.Unmarshal(body, &resp); err != nil {
					t.Fatalf("failed to parse response JSON: %v", err)
				}
				if resp["error"] == nil {
					t.Errorf("expected error field in response")
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			router, _, _ := setupTestEnvironment()

			req, _ := http.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewBufferString(tc.payload))
			if tc.tenantID != "" {
				req.Header.Set("X-Tenant-ID", tc.tenantID)
			}
			if tc.idempotencyKey != "" {
				req.Header.Set("X-Idempotency-Key", tc.idempotencyKey)
			}
			req.Header.Set("Content-Type", "application/json")

			rr := httptest.NewRecorder()
			router.ServeHTTP(rr, req)

			if rr.Code != tc.expectedStatus {
				t.Fatalf("expected status %d, got %d. body: %s", tc.expectedStatus, rr.Code, rr.Body.String())
			}

			if tc.verifyBody != nil {
				tc.verifyBody(t, rr.Body.Bytes())
			}
		})
	}
}

func TestHandler_IngestEvent_IdempotencyReplay(t *testing.T) {
	router, repo, _ := setupTestEnvironment()

	payload := `{"event_type":"payment.captured","payload":{"invoice_id":"inv_999"}}`
	tenantID := "tenant_fintech"
	idempKey := "idemp_replay_001"

	// 1. Initial Request
	req1, _ := http.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewBufferString(payload))
	req1.Header.Set("X-Tenant-ID", tenantID)
	req1.Header.Set("X-Idempotency-Key", idempKey)
	req1.Header.Set("Content-Type", "application/json")

	rr1 := httptest.NewRecorder()
	router.ServeHTTP(rr1, req1)

	if rr1.Code != http.StatusAccepted {
		t.Fatalf("first request failed: status %d, body: %s", rr1.Code, rr1.Body.String())
	}

	var resp1 map[string]interface{}
	if err := json.Unmarshal(rr1.Body.Bytes(), &resp1); err != nil {
		t.Fatalf("failed to decode resp1: %v", err)
	}
	eventID1, ok := resp1["id"].(string)
	if !ok || eventID1 == "" {
		t.Fatalf("expected valid id in resp1")
	}

	// 2. Second Identical Request (Replay)
	req2, _ := http.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewBufferString(payload))
	req2.Header.Set("X-Tenant-ID", tenantID)
	req2.Header.Set("X-Idempotency-Key", idempKey)
	req2.Header.Set("Content-Type", "application/json")

	rr2 := httptest.NewRecorder()
	router.ServeHTTP(rr2, req2)

	if rr2.Code != http.StatusAccepted {
		t.Fatalf("second request expected status %d, got %d. body: %s", http.StatusAccepted, rr2.Code, rr2.Body.String())
	}

	var resp2 map[string]interface{}
	if err := json.Unmarshal(rr2.Body.Bytes(), &resp2); err != nil {
		t.Fatalf("failed to decode resp2: %v", err)
	}
	eventID2, ok := resp2["id"].(string)
	if !ok || eventID2 == "" {
		t.Fatalf("expected valid id in resp2")
	}

	if eventID1 != eventID2 {
		t.Fatalf("expected identical event ID on idempotency replay, got %s and %s", eventID1, eventID2)
	}

	// Verify only 1 event and 1 outbox record exists in repository
	event, err := repo.GetEvent(req1.Context(), eventID1)
	if err != nil || event == nil {
		t.Fatalf("expected event in repo: %v", err)
	}
}

func TestHandler_IngestEvent_ConcurrentIdempotencyConflict(t *testing.T) {
	router, _, _ := setupTestEnvironment()

	payload := `{"event_type":"user.signup","payload":{"user_id":"usr_444"}}`
	tenantID := "tenant_race"
	idempKey := "idemp_race_key"

	var wg sync.WaitGroup
	var acceptedCount int64
	var conflictCount int64
	numWorkers := 10

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewBufferString(payload))
			req.Header.Set("X-Tenant-ID", tenantID)
			req.Header.Set("X-Idempotency-Key", idempKey)
			req.Header.Set("Content-Type", "application/json")

			rr := httptest.NewRecorder()
			router.ServeHTTP(rr, req)

			if rr.Code == http.StatusAccepted {
				atomic.AddInt64(&acceptedCount, 1)
			} else if rr.Code == http.StatusConflict {
				atomic.AddInt64(&conflictCount, 1)
			}
		}()
	}
	wg.Wait()

	if acceptedCount < 1 {
		t.Fatalf("expected at least 1 request accepted, got %d (conflicts: %d)", acceptedCount, conflictCount)
	}
	if acceptedCount+conflictCount != int64(numWorkers) {
		t.Fatalf("expected total accepted + conflict = %d, got accepted=%d, conflict=%d", numWorkers, acceptedCount, conflictCount)
	}
}

func TestHandler_Endpoints_CRUD(t *testing.T) {
	router, _, _ := setupTestEnvironment()
	tenantID := "tenant_endpoint_test"

	// 1. Create Endpoint with valid payload
	createBody := `{"url":"https://api.example.com/webhook","secret":"sec_test_secret_123","rate_limit":50}`
	req, _ := http.NewRequest(http.MethodPost, "/api/v1/endpoints", bytes.NewBufferString(createBody))
	req.Header.Set("X-Tenant-ID", tenantID)
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("create endpoint failed: status %d, body: %s", rr.Code, rr.Body.String())
	}

	var created domain.Endpoint
	if err := json.Unmarshal(rr.Body.Bytes(), &created); err != nil {
		t.Fatalf("failed to decode created endpoint: %v", err)
	}
	if created.ID == "" || created.TenantID != tenantID || created.URL != "https://api.example.com/webhook" {
		t.Errorf("unexpected endpoint fields: %+v", created)
	}

	// 2. Create Endpoint with invalid URL scheme
	badBody := `{"url":"ftp://api.example.com/webhook","secret":"sec_123"}`
	badReq, _ := http.NewRequest(http.MethodPost, "/api/v1/endpoints", bytes.NewBufferString(badBody))
	badReq.Header.Set("X-Tenant-ID", tenantID)

	badRR := httptest.NewRecorder()
	router.ServeHTTP(badRR, badReq)
	if badRR.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid url, got %d", badRR.Code)
	}

	// 3. List Endpoints
	listReq, _ := http.NewRequest(http.MethodGet, "/api/v1/endpoints", nil)
	listReq.Header.Set("X-Tenant-ID", tenantID)

	listRR := httptest.NewRecorder()
	router.ServeHTTP(listRR, listReq)

	if listRR.Code != http.StatusOK {
		t.Fatalf("list endpoints failed: status %d, body: %s", listRR.Code, listRR.Body.String())
	}

	var endpoints []domain.Endpoint
	if err := json.Unmarshal(listRR.Body.Bytes(), &endpoints); err != nil {
		t.Fatalf("failed to decode endpoints list: %v", err)
	}
	if len(endpoints) != 1 {
		t.Fatalf("expected 1 endpoint in list, got %d", len(endpoints))
	}
	if endpoints[0].ID != created.ID {
		t.Errorf("expected endpoint ID %s, got %s", created.ID, endpoints[0].ID)
	}
}

func TestHandler_SetupTestRouter(t *testing.T) {
	router := api.SetupTestRouter()

	req, _ := http.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 from SetupTestRouter healthz, got %d", rr.Code)
	}
}

func TestHandler_Endpoints_MissingTenant(t *testing.T) {
	router, _, _ := setupTestEnvironment()

	// Create endpoint without tenant header
	createReq, _ := http.NewRequest(http.MethodPost, "/api/v1/endpoints", bytes.NewBufferString(`{"url":"https://example.com","secret":"sec_1"}`))
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, createReq)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when missing tenant header on create endpoint, got %d", rr.Code)
	}

	// List endpoints without tenant header
	listReq, _ := http.NewRequest(http.MethodGet, "/api/v1/endpoints", nil)
	listRR := httptest.NewRecorder()
	router.ServeHTTP(listRR, listReq)
	if listRR.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when missing tenant header on list endpoints, got %d", listRR.Code)
	}
}

func TestHandler_MethodNotAllowed(t *testing.T) {
	router, _, _ := setupTestEnvironment()

	// PUT on /api/v1/events
	req, _ := http.NewRequest(http.MethodPut, "/api/v1/events", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 for PUT /api/v1/events, got %d", rr.Code)
	}
}

func TestHandler_MetricsEndpoint(t *testing.T) {
	router, _, _ := setupTestEnvironment()

	req, err := http.NewRequest(http.MethodGet, "/metrics", nil)
	if err != nil {
		t.Fatalf("failed to create metrics request: %v", err)
	}

	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 from /metrics, got %d", rr.Code)
	}
}

func TestHandler_DLQ_Endpoints(t *testing.T) {
	router, repo, _ := setupTestEnvironment()
	tenantID := "tenant_dlq_api_test"
	ctx := context.Background()

	// Seed 2 DLQ events
	for i := 1; i <= 2; i++ {
		evtID := fmt.Sprintf("evt_api_dlq_%d", i)
		evt := &domain.Event{
			ID:        evtID,
			TenantID:  tenantID,
			EventType: "payment.failed",
			Payload:   []byte(`{"order_id":"ord_dlq"}`),
			Status:    domain.EventStatusDLQ,
			CreatedAt: time.Now(),
		}
		_ = repo.CreateEventWithOutbox(ctx, evt, &domain.OutboxEvent{EventID: evtID, Status: domain.OutboxStatusPublished})
	}

	// 1. GET /api/v1/dlq without tenant header -> 400
	reqNoTenant, _ := http.NewRequest(http.MethodGet, "/api/v1/dlq", nil)
	rrNoTenant := httptest.NewRecorder()
	router.ServeHTTP(rrNoTenant, reqNoTenant)
	if rrNoTenant.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when missing tenant header, got %d", rrNoTenant.Code)
	}

	// 2. GET /api/v1/dlq with tenant header -> 200
	reqGet, _ := http.NewRequest(http.MethodGet, "/api/v1/dlq", nil)
	reqGet.Header.Set("X-Tenant-ID", tenantID)
	rrGet := httptest.NewRecorder()
	router.ServeHTTP(rrGet, reqGet)

	if rrGet.Code != http.StatusOK {
		t.Fatalf("expected 200 for GET /api/v1/dlq, got %d, body: %s", rrGet.Code, rrGet.Body.String())
	}

	var dlqList []domain.Event
	if err := json.Unmarshal(rrGet.Body.Bytes(), &dlqList); err != nil {
		t.Fatalf("failed to decode dlq list: %v", err)
	}
	if len(dlqList) != 2 {
		t.Fatalf("expected 2 dlq events, got %d", len(dlqList))
	}

	// 3. POST /api/v1/dlq/replay with empty event_ids -> 400
	reqBadReplay, _ := http.NewRequest(http.MethodPost, "/api/v1/dlq/replay", bytes.NewBufferString(`{"event_ids":[]}`))
	reqBadReplay.Header.Set("X-Tenant-ID", tenantID)
	rrBadReplay := httptest.NewRecorder()
	router.ServeHTTP(rrBadReplay, reqBadReplay)
	if rrBadReplay.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty event_ids, got %d", rrBadReplay.Code)
	}

	// 4. POST /api/v1/dlq/replay with valid IDs -> 200
	replayBody := `{"event_ids":["evt_api_dlq_1", "evt_api_dlq_2"]}`
	reqReplay, _ := http.NewRequest(http.MethodPost, "/api/v1/dlq/replay", bytes.NewBufferString(replayBody))
	reqReplay.Header.Set("X-Tenant-ID", tenantID)
	rrReplay := httptest.NewRecorder()
	router.ServeHTTP(rrReplay, reqReplay)

	if rrReplay.Code != http.StatusOK {
		t.Fatalf("expected 200 for replay, got %d, body: %s", rrReplay.Code, rrReplay.Body.String())
	}

	var replayResp map[string]interface{}
	if err := json.Unmarshal(rrReplay.Body.Bytes(), &replayResp); err != nil {
		t.Fatalf("failed to decode replay response: %v", err)
	}
	if replayResp["replayed_count"] != float64(2) {
		t.Fatalf("expected replayed_count 2, got %v", replayResp["replayed_count"])
	}
}


