package storage_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"web-hook-project/internal/domain"
	"web-hook-project/internal/storage"
)

func TestRepository_Tenant(t *testing.T) {
	repo := storage.NewMemoryRepository()
	ctx := context.Background()

	tenant := &domain.Tenant{
		ID:        "tenant_001",
		Name:      "ACME Corporation",
		CreatedAt: time.Now(),
	}

	// 1. Create Tenant
	err := repo.CreateTenant(ctx, tenant)
	if err != nil {
		t.Fatalf("CreateTenant() failed: %v", err)
	}

	// 2. Get Existing Tenant
	got, err := repo.GetTenant(ctx, tenant.ID)
	if err != nil {
		t.Fatalf("GetTenant() failed: %v", err)
	}
	if got.ID != tenant.ID || got.Name != tenant.Name {
		t.Fatalf("GetTenant() got %+v, want %+v", got, tenant)
	}

	// 3. Get Non-Existing Tenant
	_, err = repo.GetTenant(ctx, "non_existent")
	if err == nil {
		t.Fatal("GetTenant() expected error for non-existent tenant, got nil")
	}
}

func TestRepository_Endpoint(t *testing.T) {
	repo := storage.NewMemoryRepository()
	ctx := context.Background()

	tenant := &domain.Tenant{
		ID:        "tenant_001",
		Name:      "ACME Corporation",
		CreatedAt: time.Now(),
	}
	_ = repo.CreateTenant(ctx, tenant)

	ep1 := &domain.Endpoint{
		ID:        "ep_001",
		TenantID:  tenant.ID,
		URL:       "https://api.acme.com/webhook",
		Secret:    "whsec_secret_1",
		RateLimit: 100,
		IsActive:  true,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	ep2 := &domain.Endpoint{
		ID:        "ep_002",
		TenantID:  tenant.ID,
		URL:       "https://api.acme.com/backup",
		Secret:    "whsec_secret_2",
		RateLimit: 50,
		IsActive:  true,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// Create Endpoints
	if err := repo.CreateEndpoint(ctx, ep1); err != nil {
		t.Fatalf("CreateEndpoint(ep1) failed: %v", err)
	}
	if err := repo.CreateEndpoint(ctx, ep2); err != nil {
		t.Fatalf("CreateEndpoint(ep2) failed: %v", err)
	}

	// Get Endpoint
	got, err := repo.GetEndpoint(ctx, ep1.ID)
	if err != nil {
		t.Fatalf("GetEndpoint(ep1) failed: %v", err)
	}
	if got.ID != ep1.ID || got.URL != ep1.URL {
		t.Fatalf("GetEndpoint(ep1) got %+v, want %+v", got, ep1)
	}

	// Get Endpoints By Tenant
	endpoints, err := repo.GetEndpointsByTenant(ctx, tenant.ID)
	if err != nil {
		t.Fatalf("GetEndpointsByTenant() failed: %v", err)
	}
	if len(endpoints) != 2 {
		t.Fatalf("GetEndpointsByTenant() got %d endpoints, want 2", len(endpoints))
	}
}

func TestRepository_CreateEventWithOutbox_Atomicity_And_Idempotency(t *testing.T) {
	repo := storage.NewMemoryRepository()
	ctx := context.Background()

	event := &domain.Event{
		ID:             "evt_001",
		TenantID:       "tenant_001",
		EventType:      "order.created",
		IdempotencyKey: "idemp_1001",
		Payload:        []byte(`{"order_id": "ord_1001"}`),
		Status:         domain.EventStatusPending,
		CreatedAt:      time.Now(),
	}
	outbox := &domain.OutboxEvent{
		EventID:   event.ID,
		Status:    domain.OutboxStatusPending,
		CreatedAt: time.Now(),
	}

	// 1. Create Event and Outbox
	err := repo.CreateEventWithOutbox(ctx, event, outbox)
	if err != nil {
		t.Fatalf("CreateEventWithOutbox() failed: %v", err)
	}
	if outbox.ID == 0 {
		t.Fatalf("expected outbox.ID to be assigned (got 0)")
	}

	// 2. Fetch Event to verify existence
	savedEvent, err := repo.GetEvent(ctx, event.ID)
	if err != nil {
		t.Fatalf("GetEvent() failed: %v", err)
	}
	if savedEvent.ID != event.ID || savedEvent.EventType != event.EventType {
		t.Fatalf("GetEvent() mismatch: got %+v, want %+v", savedEvent, event)
	}

	// 3. Duplicate idempotency key for same tenant should fail
	dupEvent := &domain.Event{
		ID:             "evt_002",
		TenantID:       "tenant_001",
		EventType:      "order.created",
		IdempotencyKey: "idemp_1001", // duplicate key!
		Payload:        []byte(`{"order_id": "ord_1002"}`),
		Status:         domain.EventStatusPending,
		CreatedAt:      time.Now(),
	}
	dupOutbox := &domain.OutboxEvent{
		EventID:   dupEvent.ID,
		Status:    domain.OutboxStatusPending,
		CreatedAt: time.Now(),
	}

	err = repo.CreateEventWithOutbox(ctx, dupEvent, dupOutbox)
	if err == nil {
		t.Fatal("CreateEventWithOutbox() expected error on duplicate idempotency key, got nil")
	}

	// 4. Same idempotency key for different tenant should succeed
	diffTenantEvent := &domain.Event{
		ID:             "evt_003",
		TenantID:       "tenant_002", // different tenant
		EventType:      "order.created",
		IdempotencyKey: "idemp_1001",
		Payload:        []byte(`{"order_id": "ord_1003"}`),
		Status:         domain.EventStatusPending,
		CreatedAt:      time.Now(),
	}
	diffTenantOutbox := &domain.OutboxEvent{
		EventID:   diffTenantEvent.ID,
		Status:    domain.OutboxStatusPending,
		CreatedAt: time.Now(),
	}
	err = repo.CreateEventWithOutbox(ctx, diffTenantEvent, diffTenantOutbox)
	if err != nil {
		t.Fatalf("CreateEventWithOutbox() failed for different tenant: %v", err)
	}
}

func TestRepository_Outbox_Lifecycle(t *testing.T) {
	repo := storage.NewMemoryRepository()
	ctx := context.Background()

	// Seed 3 events with outbox
	for i := 1; i <= 3; i++ {
		evt := &domain.Event{
			ID:        string(rune('A' + i)),
			TenantID:  "tenant_001",
			EventType: "user.signup",
			Payload:   []byte(`{}`),
			Status:    domain.EventStatusPending,
			CreatedAt: time.Now(),
		}
		ob := &domain.OutboxEvent{
			EventID:   evt.ID,
			Status:    domain.OutboxStatusPending,
			CreatedAt: time.Now(),
		}
		if err := repo.CreateEventWithOutbox(ctx, evt, ob); err != nil {
			t.Fatalf("failed seeding event %d: %v", i, err)
		}
	}

	// 1. Fetch pending outbox with limit 2
	pending, err := repo.FetchAndLockPendingOutbox(ctx, 2)
	if err != nil {
		t.Fatalf("FetchAndLockPendingOutbox() failed: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("FetchAndLockPendingOutbox() expected 2, got %d", len(pending))
	}

	// 2. Mark one published
	targetID := pending[0].ID
	if err := repo.MarkOutboxPublished(ctx, targetID); err != nil {
		t.Fatalf("MarkOutboxPublished() failed: %v", err)
	}

	// 3. Fetch pending again with limit 10
	pendingAfter, err := repo.FetchAndLockPendingOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("FetchAndLockPendingOutbox() after mark failed: %v", err)
	}
	if len(pendingAfter) != 2 {
		t.Fatalf("FetchAndLockPendingOutbox() expected 2 remaining pending items, got %d", len(pendingAfter))
	}

	for _, item := range pendingAfter {
		if item.ID == targetID {
			t.Fatalf("outbox item %d was marked published but still returned as pending", targetID)
		}
	}
}

func TestRepository_DeliveryAttempt_And_UpdateEventStatus(t *testing.T) {
	repo := storage.NewMemoryRepository()
	ctx := context.Background()

	event := &domain.Event{
		ID:        "evt_delivery_test",
		TenantID:  "tenant_001",
		EventType: "invoice.payment_failed",
		Payload:   []byte(`{"invoice_id": "inv_123"}`),
		Status:    domain.EventStatusPending,
		CreatedAt: time.Now(),
	}
	outbox := &domain.OutboxEvent{
		EventID:   event.ID,
		Status:    domain.OutboxStatusPending,
		CreatedAt: time.Now(),
	}
	if err := repo.CreateEventWithOutbox(ctx, event, outbox); err != nil {
		t.Fatalf("CreateEventWithOutbox() failed: %v", err)
	}

	// Record delivery attempt
	attempt := &domain.DeliveryAttempt{
		ID:             "att_001",
		EventID:        event.ID,
		EndpointID:     "ep_001",
		AttemptNumber:  1,
		ResponseStatus: 200,
		ResponseBody:   `{"received": true}`,
		DurationMs:     45,
		Status:         domain.DeliveryStatusSuccess,
		ExecutedAt:     time.Now(),
	}

	if err := repo.RecordDeliveryAttempt(ctx, attempt); err != nil {
		t.Fatalf("RecordDeliveryAttempt() failed: %v", err)
	}

	// Update event status
	if err := repo.UpdateEventStatus(ctx, event.ID, domain.EventStatusDelivered); err != nil {
		t.Fatalf("UpdateEventStatus() failed: %v", err)
	}

	// Verify updated event status
	updatedEvt, err := repo.GetEvent(ctx, event.ID)
	if err != nil {
		t.Fatalf("GetEvent() failed: %v", err)
	}
	if updatedEvt.Status != domain.EventStatusDelivered {
		t.Fatalf("expected event status %s, got %s", domain.EventStatusDelivered, updatedEvt.Status)
	}
}

func TestRepository_ConcurrentAccess(t *testing.T) {
	repo := storage.NewMemoryRepository()
	ctx := context.Background()

	const numGoroutines = 20
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			evt := &domain.Event{
				ID:        string(rune('A' + idx)),
				TenantID:  "tenant_conc",
				EventType: "ping",
				Payload:   []byte(`{}`),
				Status:    domain.EventStatusPending,
				CreatedAt: time.Now(),
			}
			ob := &domain.OutboxEvent{
				EventID:   evt.ID,
				Status:    domain.OutboxStatusPending,
				CreatedAt: time.Now(),
			}
			_ = repo.CreateEventWithOutbox(ctx, evt, ob)
			_, _ = repo.FetchAndLockPendingOutbox(ctx, 5)
			_ = repo.UpdateEventStatus(ctx, evt.ID, domain.EventStatusDelivered)
		}(i)
	}

	wg.Wait()
}

func TestRepository_DLQ_Lifecycle(t *testing.T) {
	repo := storage.NewMemoryRepository()
	ctx := context.Background()
	tenantID := "tenant_dlq_test"

	// Create 3 events: 2 DLQ, 1 DELIVERED
	for i := 1; i <= 3; i++ {
		status := domain.EventStatusDLQ
		if i == 3 {
			status = domain.EventStatusDelivered
		}
		evt := &domain.Event{
			ID:        fmt.Sprintf("evt_dlq_%d", i),
			TenantID:  tenantID,
			EventType: "payment.failed",
			Payload:   []byte(`{"amount":100}`),
			Status:    status,
			CreatedAt: time.Now().Add(time.Duration(i) * time.Minute),
		}
		ob := &domain.OutboxEvent{
			EventID:   evt.ID,
			Status:    domain.OutboxStatusPublished,
			CreatedAt: time.Now(),
		}
		_ = repo.CreateEventWithOutbox(ctx, evt, ob)
	}

	// 1. Get DLQ events for tenant
	dlqList, err := repo.GetDLQEvents(ctx, tenantID, 10, 0)
	if err != nil {
		t.Fatalf("GetDLQEvents() error: %v", err)
	}
	if len(dlqList) != 2 {
		t.Fatalf("expected 2 DLQ events, got %d", len(dlqList))
	}

	// 2. Replay DLQ events
	replayed, err := repo.ReplayDLQEvents(ctx, tenantID, []string{"evt_dlq_1", "evt_dlq_2", "evt_dlq_3"})
	if err != nil {
		t.Fatalf("ReplayDLQEvents() error: %v", err)
	}
	if replayed != 2 { // evt_dlq_3 was DELIVERED, not DLQ, so only 2 should be replayed
		t.Fatalf("expected 2 events replayed, got %d", replayed)
	}

	// 3. Verify event status updated to PENDING and outbox created
	evt1, _ := repo.GetEvent(ctx, "evt_dlq_1")
	if evt1.Status != domain.EventStatusPending {
		t.Fatalf("expected evt_dlq_1 status PENDING, got %v", evt1.Status)
	}

	pendingOutbox, err := repo.FetchAndLockPendingOutbox(ctx, 10)
	if err != nil || len(pendingOutbox) != 2 {
		t.Fatalf("expected 2 new pending outbox items after replay, got %d", len(pendingOutbox))
	}
}

