package worker_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"web-hook-project/internal/dispatcher"
	"web-hook-project/internal/domain"
	"web-hook-project/internal/queue"
	"web-hook-project/internal/retry"
	"web-hook-project/internal/storage"
	"web-hook-project/internal/worker"
)

func TestWorkerPool_ParallelDispatchAndAck(t *testing.T) {
	var deliveryCount atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deliveryCount.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"received":true}`))
	}))
	defer server.Close()

	repo := storage.NewMemoryRepository()
	q := queue.NewMemoryStreamQueue()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tenantID := "tenant_pool_001"
	_ = repo.CreateTenant(ctx, &domain.Tenant{
		ID:        tenantID,
		Name:      "Pool Test Tenant",
		CreatedAt: time.Now(),
	})

	// Create 2 endpoints for this tenant
	ep1 := &domain.Endpoint{
		ID:        "ep_pool_001",
		TenantID:  tenantID,
		URL:       server.URL,
		Secret:    "whsec_pool_test_1",
		RateLimit: 100,
		IsActive:  true,
	}
	ep2 := &domain.Endpoint{
		ID:        "ep_pool_002",
		TenantID:  tenantID,
		URL:       server.URL,
		Secret:    "whsec_pool_test_2",
		RateLimit: 100,
		IsActive:  true,
	}
	_ = repo.CreateEndpoint(ctx, ep1)
	_ = repo.CreateEndpoint(ctx, ep2)

	policy := retry.DefaultBackoffPolicy()
	disp := dispatcher.NewDispatcher(server.Client(), repo, policy)

	streamName := "stream:events:test_pool"
	groupName := "pool-test-group"

	pool := worker.NewWorkerPool(worker.Config{
		NumWorkers:   4,
		StreamName:   streamName,
		GroupName:    groupName,
		BatchSize:    5,
		PollInterval: 20 * time.Millisecond,
	}, q, repo, disp)

	err := pool.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start worker pool: %v", err)
	}

	numEvents := 5
	for i := 0; i < numEvents; i++ {
		event := &domain.Event{
			ID:        fmt.Sprintf("evt_pool_%d", i),
			TenantID:  tenantID,
			EventType: "order.created",
			Payload:   []byte(fmt.Sprintf(`{"order_id":%d}`, i)),
			Status:    domain.EventStatusPending,
			CreatedAt: time.Now(),
		}
		outbox := &domain.OutboxEvent{
			EventID:   event.ID,
			Status:    domain.OutboxStatusPending,
			CreatedAt: time.Now(),
		}
		_ = repo.CreateEventWithOutbox(ctx, event, outbox)
		_, err := q.PublishEvent(ctx, streamName, event)
		if err != nil {
			t.Fatalf("failed to publish event: %v", err)
		}
	}

	// 5 events * 2 endpoints = 10 deliveries expected
	expectedDeliveries := int64(numEvents * 2)
	deadline := time.Now().Add(5 * time.Second)
	for deliveryCount.Load() < expectedDeliveries && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	if got := deliveryCount.Load(); got != expectedDeliveries {
		t.Fatalf("expected %d deliveries, got %d", expectedDeliveries, got)
	}

	pool.Stop()
}

func TestWorkerPool_GracefulShutdown(t *testing.T) {
	repo := storage.NewMemoryRepository()
	q := queue.NewMemoryStreamQueue()
	disp := dispatcher.NewDispatcher(nil, repo, retry.DefaultBackoffPolicy())

	pool := worker.NewWorkerPool(worker.Config{
		NumWorkers:   3,
		StreamName:   "stream:test:shutdown",
		GroupName:    "shutdown-group",
		PollInterval: 20 * time.Millisecond,
	}, q, repo, disp)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := pool.Start(ctx); err != nil {
		t.Fatalf("failed to start worker pool: %v", err)
	}

	// Stop should return cleanly without deadlock or errors
	done := make(chan struct{})
	go func() {
		pool.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(3 * time.Second):
		t.Fatal("worker pool Stop() timed out or deadlocked")
	}
}

func TestWorkerPool_InactiveEndpointsSkipped(t *testing.T) {
	var deliveryCount atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deliveryCount.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	repo := storage.NewMemoryRepository()
	q := queue.NewMemoryStreamQueue()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tenantID := "tenant_inactive_test"
	_ = repo.CreateTenant(ctx, &domain.Tenant{ID: tenantID, Name: "Inactive Ep Tenant"})

	activeEp := &domain.Endpoint{
		ID:        "ep_active",
		TenantID:  tenantID,
		URL:       server.URL,
		Secret:    "whsec_active",
		IsActive:  true,
		RateLimit: 100,
	}
	inactiveEp := &domain.Endpoint{
		ID:        "ep_inactive",
		TenantID:  tenantID,
		URL:       server.URL,
		Secret:    "whsec_inactive",
		IsActive:  false, // INACTIVE
		RateLimit: 100,
	}
	_ = repo.CreateEndpoint(ctx, activeEp)
	_ = repo.CreateEndpoint(ctx, inactiveEp)

	disp := dispatcher.NewDispatcher(server.Client(), repo, retry.DefaultBackoffPolicy())
	streamName := "stream:inactive:test"

	pool := worker.NewWorkerPool(worker.Config{
		NumWorkers:   2,
		StreamName:   streamName,
		GroupName:    "inactive-group",
		PollInterval: 20 * time.Millisecond,
	}, q, repo, disp)

	_ = pool.Start(ctx)
	defer pool.Stop()

	event := &domain.Event{
		ID:        "evt_inactive_1",
		TenantID:  tenantID,
		EventType: "item.updated",
		Payload:   []byte(`{"item_id":"item_99"}`),
		Status:    domain.EventStatusPending,
		CreatedAt: time.Now(),
	}
	_ = repo.CreateEventWithOutbox(ctx, event, &domain.OutboxEvent{EventID: event.ID, Status: domain.OutboxStatusPending})
	_, _ = q.PublishEvent(ctx, streamName, event)

	deadline := time.Now().Add(2 * time.Second)
	for deliveryCount.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	// Only active endpoint should have received delivery (count == 1)
	if got := deliveryCount.Load(); got != 1 {
		t.Errorf("expected exactly 1 delivery (inactive skipped), got %d", got)
	}
}

func TestWorkerPool_HighConcurrencyRace(t *testing.T) {
	var deliveryCount atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deliveryCount.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer server.Close()

	repo := storage.NewMemoryRepository()
	q := queue.NewMemoryStreamQueue()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	numTenants := 5
	for tIdx := 0; tIdx < numTenants; tIdx++ {
		tID := fmt.Sprintf("tenant_hc_%d", tIdx)
		_ = repo.CreateTenant(ctx, &domain.Tenant{ID: tID, Name: tID})
		ep := &domain.Endpoint{
			ID:        fmt.Sprintf("ep_hc_%d", tIdx),
			TenantID:  tID,
			URL:       server.URL,
			Secret:    "whsec_secret",
			RateLimit: 1000,
			IsActive:  true,
		}
		_ = repo.CreateEndpoint(ctx, ep)
	}

	disp := dispatcher.NewDispatcher(server.Client(), repo, retry.DefaultBackoffPolicy())
	streamName := "stream:events:race"

	pool := worker.NewWorkerPool(worker.Config{
		NumWorkers:   8,
		StreamName:   streamName,
		GroupName:    "race-group",
		BatchSize:    10,
		PollInterval: 10 * time.Millisecond,
	}, q, repo, disp)

	_ = pool.Start(ctx)
	defer pool.Stop()

	eventsPerTenant := 10
	totalEvents := numTenants * eventsPerTenant

	for tIdx := 0; tIdx < numTenants; tIdx++ {
		tID := fmt.Sprintf("tenant_hc_%d", tIdx)
		for eIdx := 0; eIdx < eventsPerTenant; eIdx++ {
			evtID := fmt.Sprintf("evt_race_%d_%d", tIdx, eIdx)
			evt := &domain.Event{
				ID:        evtID,
				TenantID:  tID,
				EventType: "race.test",
				Payload:   []byte(`{"data":"race"}`),
				Status:    domain.EventStatusPending,
				CreatedAt: time.Now(),
			}
			_ = repo.CreateEventWithOutbox(ctx, evt, &domain.OutboxEvent{EventID: evtID, Status: domain.OutboxStatusPending})
			_, _ = q.PublishEvent(ctx, streamName, evt)
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	for deliveryCount.Load() < int64(totalEvents) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	if got := deliveryCount.Load(); got != int64(totalEvents) {
		t.Fatalf("expected %d deliveries, got %d", totalEvents, got)
	}
}
