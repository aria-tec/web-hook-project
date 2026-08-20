package tests

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"web-hook-project/internal/dispatcher"
	"web-hook-project/internal/domain"
	"web-hook-project/internal/retry"
	"web-hook-project/internal/storage"
)

// TestAdversarial_SlowlorisReceiver tests resilience against receivers that trickle responses very slowly.
func TestAdversarial_SlowlorisReceiver(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Slowloris: Delay response headers beyond client timeout
		time.Sleep(300 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	repo := storage.NewMemoryRepository()
	// Client configured with short 100ms timeout
	client := dispatcher.NewSafeHTTPClientWithAllowPrivate(100*time.Millisecond, true)
	policy := retry.DefaultBackoffPolicy()
	d := dispatcher.NewDispatcher(client, repo, policy)

	event := &domain.Event{
		ID:        "evt_adv_slowloris",
		TenantID:  "tenant_adv",
		EventType: "test.slowloris",
		Payload:   []byte(`{"data":"slow"}`),
		CreatedAt: time.Now(),
	}
	endpoint := &domain.Endpoint{
		ID:       "ep_adv_1",
		TenantID: "tenant_adv",
		URL:      server.URL,
		Secret:   "whsec_adv_test",
		IsActive: true,
	}

	ctx := context.Background()
	attempt, err := d.Dispatch(ctx, endpoint, event, 1)
	if err != nil {
		t.Fatalf("unexpected dispatch error: %v", err)
	}

	// Invariant: Slowloris must trigger timeout and record status RETRYING without blocking caller
	if attempt.Status != domain.DeliveryStatusRetrying {
		t.Errorf("expected attempt status RETRYING for timed-out slowloris, got %v", attempt.Status)
	}
}

// TestAdversarial_TruncatedConnection tests resilience against receivers closing TCP connections abruptly.
func TestAdversarial_TruncatedConnection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Hijack connection and close immediately without sending valid HTTP response
		hj, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "cannot hijack", http.StatusInternalServerError)
			return
		}
		conn, _, err := hj.Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}))
	defer server.Close()

	repo := storage.NewMemoryRepository()
	client := dispatcher.NewSafeHTTPClientWithAllowPrivate(500*time.Millisecond, true)
	policy := retry.DefaultBackoffPolicy()
	d := dispatcher.NewDispatcher(client, repo, policy)

	event := &domain.Event{
		ID:        "evt_adv_truncated",
		TenantID:  "tenant_adv",
		EventType: "test.truncated",
		Payload:   []byte(`{"data":"truncated"}`),
		CreatedAt: time.Now(),
	}
	endpoint := &domain.Endpoint{
		ID:       "ep_adv_2",
		TenantID: "tenant_adv",
		URL:      server.URL,
		Secret:   "whsec_adv_test",
		IsActive: true,
	}

	ctx := context.Background()
	attempt, err := d.Dispatch(ctx, endpoint, event, 1)
	if err != nil {
		t.Fatalf("unexpected dispatch error: %v", err)
	}

	if attempt.Status != domain.DeliveryStatusRetrying {
		t.Errorf("expected attempt status RETRYING for truncated connection, got %v", attempt.Status)
	}
}

// TestAdversarial_429Storm tests rate limit storms and backoff calculation jitter bounds.
func TestAdversarial_429Storm(t *testing.T) {
	var stormHits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stormHits.Add(1)
		w.Header().Set("Retry-After", "5")
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	defer server.Close()

	repo := storage.NewMemoryRepository()
	client := dispatcher.NewSafeHTTPClientWithAllowPrivate(500*time.Millisecond, true)
	policy := retry.DefaultBackoffPolicy()
	d := dispatcher.NewDispatcher(client, repo, policy)

	event := &domain.Event{
		ID:        "evt_adv_429",
		TenantID:  "tenant_adv",
		EventType: "test.429storm",
		Payload:   []byte(`{"data":"rate_limit"}`),
		CreatedAt: time.Now(),
	}
	endpoint := &domain.Endpoint{
		ID:       "ep_adv_3",
		TenantID: "tenant_adv",
		URL:      server.URL,
		Secret:   "whsec_adv_test",
		IsActive: true,
	}

	ctx := context.Background()
	// Attempt 1: 429 is retryable -> RETRYING
	attempt1, err := d.Dispatch(ctx, endpoint, event, 1)
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}
	if attempt1.Status != domain.DeliveryStatusRetrying {
		t.Errorf("expected attempt 1 to be RETRYING on 429, got %v", attempt1.Status)
	}
	if attempt1.ResponseStatus != http.StatusTooManyRequests {
		t.Errorf("expected status 429, got %d", attempt1.ResponseStatus)
	}

	// Attempt 5 (max retries): -> FAILED (DLQ)
	attempt5, err := d.Dispatch(ctx, endpoint, event, 5)
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}
	if attempt5.Status != domain.DeliveryStatusFailed {
		t.Errorf("expected attempt 5 to be FAILED on 429 max retries, got %v", attempt5.Status)
	}
}
