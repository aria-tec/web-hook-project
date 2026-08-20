package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"web-hook-project/internal/domain"
	"web-hook-project/internal/storage"
)

// TestStorageLifecycle_HighVolumeOutbox asserts storage lifecycle operations under high-volume event insertions.
func TestStorageLifecycle_HighVolumeOutbox(t *testing.T) {
	repo := storage.NewMemoryRepository()
	ctx := context.Background()

	tenant := &domain.Tenant{
		ID:        "tenant_lifecycle",
		Name:      "Lifecycle Corp",
		CreatedAt: time.Now(),
	}
	if err := repo.CreateTenant(ctx, tenant); err != nil {
		t.Fatalf("failed to create tenant: %v", err)
	}

	endpoint := &domain.Endpoint{
		ID:        "ep_lifecycle",
		TenantID:  tenant.ID,
		URL:       "https://api.example.com/webhook",
		Secret:    "whsec_lifecycle",
		RateLimit: 100,
		IsActive:  true,
	}
	if err := repo.CreateEndpoint(ctx, endpoint); err != nil {
		t.Fatalf("failed to create endpoint: %v", err)
	}

	// Insert 1000 events with outbox
	totalEvents := 1000
	for i := 0; i < totalEvents; i++ {
		event := &domain.Event{
			ID:        fmt.Sprintf("evt_life_%d", i),
			TenantID:  tenant.ID,
			EventType: "order.created",
			Payload:   []byte(`{"order_id":1234}`),
			Status:    domain.EventStatusPending,
			CreatedAt: time.Now(),
		}
		outbox := &domain.OutboxEvent{
			EventID:   event.ID,
			Status:    domain.OutboxStatusPending,
			CreatedAt: time.Now(),
		}
		if err := repo.CreateEventWithOutbox(ctx, event, outbox); err != nil {
			t.Fatalf("failed to insert event %d: %v", i, err)
		}
	}

	// Fetch pending outbox batches
	pending, err := repo.FetchAndLockPendingOutbox(ctx, 500)
	if err != nil {
		t.Fatalf("failed to fetch pending outbox events: %v", err)
	}
	if len(pending) != 500 {
		t.Errorf("expected 500 pending events, got %d", len(pending))
	}

	// Transition first 500 events to published
	for _, o := range pending {
		if err := repo.MarkOutboxPublished(ctx, o.ID); err != nil {
			t.Fatalf("failed to mark outbox published: %v", err)
		}
	}

	// Fetch remaining pending outbox
	remaining, err := repo.FetchAndLockPendingOutbox(ctx, 1000)
	if err != nil {
		t.Fatalf("failed to fetch remaining outbox: %v", err)
	}
	if len(remaining) != 500 {
		t.Errorf("expected 500 remaining pending events, got %d", len(remaining))
	}
}
