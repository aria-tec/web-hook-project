package tests

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"go.uber.org/goleak"
	"web-hook-project/internal/dispatcher"
	"web-hook-project/internal/domain"
	"web-hook-project/internal/retry"
	"web-hook-project/internal/storage"
)

// TestGoroutineLeakVerification asserts zero lingering goroutines using uber-go/goleak.
func TestGoroutineLeakVerification(t *testing.T) {
	defer goleak.VerifyNone(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	repo := storage.NewMemoryRepository()
	client := dispatcher.NewSafeHTTPClientWithAllowPrivate(2*time.Second, true)
	d := dispatcher.NewDispatcher(client, repo, retry.DefaultBackoffPolicy())

	ctx := context.Background()
	endpoint := &domain.Endpoint{
		ID:       "ep_leak_1",
		TenantID: "tenant_leak",
		URL:      server.URL,
		Secret:   "whsec_leak_test",
		IsActive: true,
	}

	for i := 0; i < 500; i++ {
		event := &domain.Event{
			ID:        fmt.Sprintf("evt_leak_%d", i),
			TenantID:  "tenant_leak",
			EventType: "user.created",
			Payload:   []byte(`{"user_id":123}`),
			CreatedAt: time.Now(),
		}
		_, err := d.Dispatch(ctx, endpoint, event, 1)
		if err != nil {
			t.Fatalf("dispatch %d failed: %v", i, err)
		}
	}
}

// TestHeapMemorySlopeFlat asserts that residual heap memory footprint remains bounded after high-volume dispatches.
func TestHeapMemorySlopeFlat(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	repo := storage.NewMemoryRepository()
	client := dispatcher.NewSafeHTTPClientWithAllowPrivate(2*time.Second, true)
	d := dispatcher.NewDispatcher(client, repo, retry.DefaultBackoffPolicy())

	endpoint := &domain.Endpoint{
		ID:       "ep_slope",
		TenantID: "tenant_slope",
		URL:      server.URL,
		Secret:   "whsec_slope_test",
		IsActive: true,
	}

	// Warm up
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_, _ = d.Dispatch(ctx, endpoint, &domain.Event{
			ID:        fmt.Sprintf("evt_warmup_%d", i),
			TenantID:  "tenant_slope",
			EventType: "test.warmup",
			Payload:   []byte(`{}`),
			CreatedAt: time.Now(),
		}, 1)
	}

	runtime.GC()
	var mBefore runtime.MemStats
	runtime.ReadMemStats(&mBefore)

	// Dispatch 2000 events
	for i := 0; i < 2000; i++ {
		_, _ = d.Dispatch(ctx, endpoint, &domain.Event{
			ID:        fmt.Sprintf("evt_slope_%d", i),
			TenantID:  "tenant_slope",
			EventType: "test.event",
			Payload:   []byte(`{"index":123,"data":"slope verification test"}`),
			CreatedAt: time.Now(),
		}, 1)
	}

	runtime.GC()
	var mAfter runtime.MemStats
	runtime.ReadMemStats(&mAfter)

	// Heap growth delta should be well under 10MB for 2000 in-memory items
	heapGrowthBytes := int64(mAfter.HeapAlloc) - int64(mBefore.HeapAlloc)
	maxAllowedGrowth := int64(15 * 1024 * 1024) // 15 MB threshold

	if heapGrowthBytes > maxAllowedGrowth {
		t.Errorf("heap growth excessive (potential leak): %d bytes > %d max allowed", heapGrowthBytes, maxAllowedGrowth)
	}
}
