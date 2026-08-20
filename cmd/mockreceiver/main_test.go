package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestMockReceiver_Routes(t *testing.T) {
	server := NewMockServer()
	handler := server.Handler()

	// 1. Success endpoint
	req := httptest.NewRequest(http.MethodPost, "/webhook/success", bytes.NewBufferString(`{"event":"order.created","amount":100}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Webhook-ID", "wh_test_123")
	req.Header.Set("X-Webhook-Signature", "t=123456,v1=abcdef")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 for /webhook/success, got %d", rr.Code)
	}

	var successResp map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &successResp); err != nil {
		t.Fatalf("failed to decode success response: %v", err)
	}
	if successResp["status"] != "success" {
		t.Errorf("expected status=success, got %v", successResp["status"])
	}

	// 2. Poison endpoint
	req = httptest.NewRequest(http.MethodPost, "/webhook/poison", bytes.NewBufferString(`{"bad":"payload"}`))
	req.Header.Set("Content-Type", "application/json")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for /webhook/poison, got %d", rr.Code)
	}

	var poisonResp map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &poisonResp); err != nil {
		t.Fatalf("failed to decode poison response: %v", err)
	}
	if poisonResp["error"] != "poison_pill_rejected" {
		t.Errorf("expected error=poison_pill_rejected, got %v", poisonResp["error"])
	}

	// 3. Flaky endpoint (fails twice with 500, succeeds on 3rd)
	for i := 1; i <= 3; i++ {
		req = httptest.NewRequest(http.MethodPost, "/webhook/flaky", bytes.NewBufferString(`{}`))
		rr = httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if i < 3 && rr.Code != http.StatusInternalServerError {
			t.Errorf("attempt %d: expected 500, got %d", i, rr.Code)
		} else if i == 3 && rr.Code != http.StatusOK {
			t.Errorf("attempt 3: expected 200, got %d", rr.Code)
		}
	}

	// 4. Flaky endpoint per X-Webhook-ID
	webhookID := "wh_flaky_scoped_999"
	for i := 1; i <= 3; i++ {
		req = httptest.NewRequest(http.MethodPost, "/webhook/flaky", bytes.NewBufferString(`{}`))
		req.Header.Set("X-Webhook-ID", webhookID)
		rr = httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if i < 3 && rr.Code != http.StatusInternalServerError {
			t.Errorf("scoped attempt %d: expected 500, got %d", i, rr.Code)
		} else if i == 3 && rr.Code != http.StatusOK {
			t.Errorf("scoped attempt 3: expected 200, got %d", rr.Code)
		}
	}

	// 5. Slow endpoint
	req = httptest.NewRequest(http.MethodPost, "/webhook/slow?delay=10ms", bytes.NewBufferString(`{}`))
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 for /webhook/slow, got %d", rr.Code)
	}
}

func TestMockReceiver_InspectLogsAndClear(t *testing.T) {
	server := NewMockServer()
	handler := server.Handler()

	// Clear initial buffer
	reqClear := httptest.NewRequest(http.MethodPost, "/inspect/clear", nil)
	rrClear := httptest.NewRecorder()
	handler.ServeHTTP(rrClear, reqClear)
	if rrClear.Code != http.StatusOK {
		t.Fatalf("expected 200 for /inspect/clear, got %d", rrClear.Code)
	}

	// Send 3 requests
	for i := 1; i <= 3; i++ {
		body := fmt.Sprintf(`{"msg":"item %d"}`, i)
		req := httptest.NewRequest(http.MethodPost, "/webhook/success", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Webhook-ID", fmt.Sprintf("wh_item_%d", i))
		req.Header.Set("X-Webhook-Signature", fmt.Sprintf("t=1000,v1=sig%d", i))
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("request %d failed: %d", i, rr.Code)
		}
	}

	// GET /inspect/logs
	reqLogs := httptest.NewRequest(http.MethodGet, "/inspect/logs", nil)
	rrLogs := httptest.NewRecorder()
	handler.ServeHTTP(rrLogs, reqLogs)

	if rrLogs.Code != http.StatusOK {
		t.Fatalf("expected 200 for /inspect/logs, got %d", rrLogs.Code)
	}

	var logs []CapturedRequest
	if err := json.Unmarshal(rrLogs.Body.Bytes(), &logs); err != nil {
		t.Fatalf("failed to decode inspect logs: %v", err)
	}

	if len(logs) != 3 {
		t.Fatalf("expected 3 captured requests, got %d", len(logs))
	}

	if logs[0].WebhookID != "wh_item_1" || logs[0].Signature != "t=1000,v1=sig1" {
		t.Errorf("unexpected logs[0]: %+v", logs[0])
	}
	if logs[2].Body != `{"msg":"item 3"}` {
		t.Errorf("unexpected logs[2] body: %s", logs[2].Body)
	}

	// Also check /requests alias
	reqAlias := httptest.NewRequest(http.MethodGet, "/requests", nil)
	rrAlias := httptest.NewRecorder()
	handler.ServeHTTP(rrAlias, reqAlias)
	if rrAlias.Code != http.StatusOK {
		t.Fatalf("expected 200 for /requests, got %d", rrAlias.Code)
	}

	// POST /inspect/clear
	reqClear = httptest.NewRequest(http.MethodPost, "/inspect/clear", nil)
	rrClear = httptest.NewRecorder()
	handler.ServeHTTP(rrClear, reqClear)
	if rrClear.Code != http.StatusOK {
		t.Fatalf("expected 200 for /inspect/clear, got %d", rrClear.Code)
	}

	// Verify empty logs
	reqLogs = httptest.NewRequest(http.MethodGet, "/inspect/logs", nil)
	rrLogs = httptest.NewRecorder()
	handler.ServeHTTP(rrLogs, reqLogs)
	var clearedLogs []CapturedRequest
	_ = json.Unmarshal(rrLogs.Body.Bytes(), &clearedLogs)
	if len(clearedLogs) != 0 {
		t.Fatalf("expected 0 logs after clear, got %d", len(clearedLogs))
	}
}

func TestMockReceiver_RingBuffer_Capacity(t *testing.T) {
	server := NewMockServer()
	handler := server.Handler()

	// Send 70 requests (capacity is 50)
	for i := 1; i <= 70; i++ {
		body := fmt.Sprintf(`{"index":%d}`, i)
		req := httptest.NewRequest(http.MethodPost, "/webhook/success", bytes.NewBufferString(body))
		req.Header.Set("X-Webhook-ID", fmt.Sprintf("id_%d", i))
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
	}

	reqLogs := httptest.NewRequest(http.MethodGet, "/inspect/logs", nil)
	rrLogs := httptest.NewRecorder()
	handler.ServeHTTP(rrLogs, reqLogs)

	var logs []CapturedRequest
	if err := json.Unmarshal(rrLogs.Body.Bytes(), &logs); err != nil {
		t.Fatalf("failed to decode inspect logs: %v", err)
	}

	if len(logs) != 50 {
		t.Fatalf("expected 50 captured requests capped at capacity, got %d", len(logs))
	}

	// First item in logs should be index 21 (items 1..20 evicted)
	if logs[0].WebhookID != "id_21" {
		t.Errorf("expected logs[0] to be id_21, got %s", logs[0].WebhookID)
	}
	// Last item in logs should be index 70
	if logs[49].WebhookID != "id_70" {
		t.Errorf("expected logs[49] to be id_70, got %s", logs[49].WebhookID)
	}
}

func TestMockReceiver_CORS(t *testing.T) {
	server := NewMockServer()
	handler := server.Handler()

	// Preflight OPTIONS request
	req := httptest.NewRequest(http.MethodOptions, "/webhook/success", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	req.Header.Set("Access-Control-Request-Method", "POST")
	req.Header.Set("Access-Control-Request-Headers", "Content-Type, X-Webhook-ID")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected 204 for OPTIONS preflight, got %d", rr.Code)
	}
	if rr.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Errorf("expected Access-Control-Allow-Origin: *, got %s", rr.Header().Get("Access-Control-Allow-Origin"))
	}
	if rr.Header().Get("Access-Control-Allow-Methods") == "" {
		t.Errorf("expected Access-Control-Allow-Methods header to be set")
	}

	// Normal GET request has CORS headers
	reqGet := httptest.NewRequest(http.MethodGet, "/inspect/logs", nil)
	rrGet := httptest.NewRecorder()
	handler.ServeHTTP(rrGet, reqGet)
	if rrGet.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Errorf("expected CORS header on GET request")
	}
}

func TestMockReceiver_Healthz(t *testing.T) {
	server := NewMockServer()
	handler := server.Handler()

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 for /healthz, got %d", rr.Code)
	}

	reqHealth := httptest.NewRequest(http.MethodGet, "/health", nil)
	rrHealth := httptest.NewRecorder()
	handler.ServeHTTP(rrHealth, reqHealth)

	if rrHealth.Code != http.StatusOK {
		t.Fatalf("expected 200 for /health, got %d", rrHealth.Code)
	}
}

func TestMockReceiver_ConcurrentRequests(t *testing.T) {
	server := NewMockServer()
	handler := server.Handler()

	const concurrency = 50
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()
			var req *http.Request
			switch idx % 5 {
			case 0:
				req = httptest.NewRequest(http.MethodPost, "/webhook/success", bytes.NewBufferString(`{"idx":`+fmt.Sprint(idx)+`}`))
				req.Header.Set("X-Webhook-ID", fmt.Sprintf("id_%d", idx))
			case 1:
				req = httptest.NewRequest(http.MethodPost, "/webhook/flaky", bytes.NewBufferString(`{}`))
				req.Header.Set("X-Webhook-ID", fmt.Sprintf("flaky_id_%d", idx%3))
			case 2:
				req = httptest.NewRequest(http.MethodPost, "/webhook/poison", bytes.NewBufferString(`{}`))
			case 3:
				req = httptest.NewRequest(http.MethodGet, "/inspect/logs", nil)
			case 4:
				req = httptest.NewRequest(http.MethodPost, "/inspect/clear", nil)
			}
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)
		}(i)
	}

	wg.Wait()
}
