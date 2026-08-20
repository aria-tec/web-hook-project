package api_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"web-hook-project/internal/api"
	"web-hook-project/internal/domain"
	"web-hook-project/internal/idempotency"
	"web-hook-project/internal/storage"
)

type streamRecorder struct {
	mu          sync.Mutex
	header      http.Header
	pw          *io.PipeWriter
	code        int
	headerReady chan struct{}
	once        sync.Once
}

func newStreamRecorder() (*streamRecorder, *io.PipeReader) {
	pr, pw := io.Pipe()
	return &streamRecorder{
		header:      make(http.Header),
		pw:          pw,
		code:        http.StatusOK,
		headerReady: make(chan struct{}),
	}, pr
}

func (s *streamRecorder) Header() http.Header {
	return s.header
}

func (s *streamRecorder) Write(b []byte) (int, error) {
	return s.pw.Write(b)
}

func (s *streamRecorder) WriteHeader(statusCode int) {
	s.mu.Lock()
	s.code = statusCode
	s.mu.Unlock()
	s.once.Do(func() {
		close(s.headerReady)
	})
}

func (s *streamRecorder) Flush() {
	// Flusher interface implementation
}

func TestSSE_StreamDeliveryAttempts(t *testing.T) {
	repo := storage.NewMemoryRepository()
	guard := idempotency.NewMemoryGuard()
	broker := api.NewSSEBroker()

	h := api.NewHandler(repo, guard).WithSSEBroker(broker)
	router := api.NewRouter(h)

	rec, pr := newStreamRecorder()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/events/stream", nil).WithContext(ctx)

	go func() {
		defer rec.pw.Close()
		router.ServeHTTP(rec, req)
	}()

	// Wait for headers to be set and flushed
	select {
	case <-rec.headerReady:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for headers")
	}

	if contentType := rec.Header().Get("Content-Type"); !strings.Contains(contentType, "text/event-stream") {
		t.Fatalf("expected Content-Type text/event-stream, got %s", contentType)
	}
	if cacheControl := rec.Header().Get("Cache-Control"); !strings.Contains(cacheControl, "no-cache") {
		t.Fatalf("expected Cache-Control no-cache, got %s", cacheControl)
	}
	if allowOrigin := rec.Header().Get("Access-Control-Allow-Origin"); allowOrigin == "" {
		t.Fatalf("expected Access-Control-Allow-Origin header to be set")
	}

	// Wait briefly for client registration
	time.Sleep(20 * time.Millisecond)

	// Broadcast test attempt
	attempt := &domain.DeliveryAttempt{
		ID:             "att_test_sse",
		EventID:        "evt_test_123",
		EndpointID:     "ep_test_456",
		AttemptNumber:  1,
		ResponseStatus: 200,
		ResponseBody:   `{"status":"ok"}`,
		DurationMs:     42,
		Status:         domain.DeliveryStatusSuccess,
		ExecutedAt:     time.Now().UTC(),
	}

	broker.Broadcast(attempt)

	reader := bufio.NewReader(pr)
	var foundData string
	for i := 0; i < 5; i++ {
		line, readErr := reader.ReadString('\n')
		if readErr != nil {
			t.Fatalf("failed to read SSE line: %v", readErr)
		}
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "data:") {
			foundData = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			break
		}
	}

	if foundData == "" {
		t.Fatalf("did not receive data line from SSE stream")
	}

	var receivedAttempt domain.DeliveryAttempt
	if err := json.Unmarshal([]byte(foundData), &receivedAttempt); err != nil {
		t.Fatalf("failed to unmarshal SSE data payload %q: %v", foundData, err)
	}

	if receivedAttempt.ID != attempt.ID {
		t.Errorf("expected attempt ID %q, got %q", attempt.ID, receivedAttempt.ID)
	}
	if receivedAttempt.EventID != attempt.EventID {
		t.Errorf("expected event ID %q, got %q", attempt.EventID, receivedAttempt.EventID)
	}
	if receivedAttempt.Status != attempt.Status {
		t.Errorf("expected status %v, got %v", attempt.Status, receivedAttempt.Status)
	}
}

func TestSSE_MultipleClients_ConcurrentBroadcast(t *testing.T) {
	repo := storage.NewMemoryRepository()
	guard := idempotency.NewMemoryGuard()
	broker := api.NewSSEBroker()

	h := api.NewHandler(repo, guard).WithSSEBroker(broker)
	router := api.NewRouter(h)

	clientCount := 5
	var wg sync.WaitGroup
	readyWg := sync.WaitGroup{}
	readyWg.Add(clientCount)

	for i := 0; i < clientCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			rec, pr := newStreamRecorder()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			req := httptest.NewRequest(http.MethodGet, "/api/v1/events/stream", nil).WithContext(ctx)

			go func() {
				defer rec.pw.Close()
				router.ServeHTTP(rec, req)
			}()

			select {
			case <-rec.headerReady:
			case <-time.After(500 * time.Millisecond):
				t.Errorf("client %d header ready timeout", idx)
				readyWg.Done()
				return
			}

			readyWg.Done()

			reader := bufio.NewReader(pr)
			for {
				line, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				if strings.Contains(line, "att_multi_1") {
					return // successfully received broadcast
				}
			}
		}(i)
	}

	readyWg.Wait()
	time.Sleep(30 * time.Millisecond)

	attempt := &domain.DeliveryAttempt{
		ID:         "att_multi_1",
		EventID:    "evt_multi_1",
		Status:     domain.DeliveryStatusSuccess,
		ExecutedAt: time.Now().UTC(),
	}

	broker.Broadcast(attempt)
	wg.Wait()
}

func TestSSE_KeepAlivePing(t *testing.T) {
	broker := api.NewSSEBrokerWithPingInterval(20 * time.Millisecond)
	repo := storage.NewMemoryRepository()
	guard := idempotency.NewMemoryGuard()

	h := api.NewHandler(repo, guard).WithSSEBroker(broker)
	router := api.NewRouter(h)

	rec, pr := newStreamRecorder()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/events/stream", nil).WithContext(ctx)

	go func() {
		defer rec.pw.Close()
		router.ServeHTTP(rec, req)
	}()

	reader := bufio.NewReader(pr)
	var gotPing bool
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		if strings.TrimSpace(line) == ": ping" {
			gotPing = true
			break
		}
	}

	if !gotPing {
		t.Fatalf("expected keep-alive ': ping' comment in SSE stream")
	}
}

func TestCORS_Preflight_And_Headers(t *testing.T) {
	repo := storage.NewMemoryRepository()
	guard := idempotency.NewMemoryGuard()
	h := api.NewHandler(repo, guard)
	router := api.NewRouter(h)

	// 1. Test OPTIONS Preflight Request
	req := httptest.NewRequest(http.MethodOptions, "/api/v1/events", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	req.Header.Set("Access-Control-Request-Method", "POST")
	req.Header.Set("Access-Control-Request-Headers", "Content-Type, X-Tenant-ID")

	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusNoContent && rr.Code != http.StatusOK {
		t.Errorf("expected 204 or 200 for OPTIONS, got %d", rr.Code)
	}
	if origin := rr.Header().Get("Access-Control-Allow-Origin"); origin != "http://localhost:3000" && origin != "*" {
		t.Errorf("expected Access-Control-Allow-Origin http://localhost:3000 or *, got %s", origin)
	}
	if methods := rr.Header().Get("Access-Control-Allow-Methods"); !strings.Contains(methods, "POST") {
		t.Errorf("expected Access-Control-Allow-Methods to include POST, got %s", methods)
	}
	if headers := rr.Header().Get("Access-Control-Allow-Headers"); !strings.Contains(headers, "X-Tenant-ID") && !strings.Contains(headers, "*") {
		t.Errorf("expected Access-Control-Allow-Headers to include X-Tenant-ID or *, got %s", headers)
	}

	// 2. Test GET with Origin Header
	getReq := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	getReq.Header.Set("Origin", "http://localhost:3000")
	getRR := httptest.NewRecorder()
	router.ServeHTTP(getRR, getReq)

	if origin := getRR.Header().Get("Access-Control-Allow-Origin"); origin != "http://localhost:3000" && origin != "*" {
		t.Errorf("expected Access-Control-Allow-Origin on GET response, got %s", origin)
	}
}

func TestSSE_ClientCount_And_SlowClientDrop(t *testing.T) {
	broker := api.NewSSEBroker()
	if count := broker.ClientCount(); count != 0 {
		t.Fatalf("expected 0 clients initially, got %d", count)
	}

	fullChan := make(chan *domain.DeliveryAttempt, 1)
	fullChan <- &domain.DeliveryAttempt{ID: "existing"}

	broker.RegisterClient(fullChan)
	if count := broker.ClientCount(); count != 1 {
		t.Fatalf("expected 1 client after register, got %d", count)
	}

	done := make(chan struct{})
	go func() {
		broker.Broadcast(&domain.DeliveryAttempt{ID: "att_dropped_if_full"})
		close(done)
	}()

	select {
	case <-done:
		// success, non-blocking
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("broadcast blocked on slow client channel")
	}

	broker.UnregisterClient(fullChan)
	if count := broker.ClientCount(); count != 0 {
		t.Fatalf("expected 0 clients after unregister, got %d", count)
	}
}
