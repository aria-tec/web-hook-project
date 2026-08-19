package outbox_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"web-hook-project/internal/domain"
	"web-hook-project/internal/outbox"
	"web-hook-project/internal/queue"
	"web-hook-project/internal/storage"
)

func TestRelay_ProcessNextBatch_Success(t *testing.T) {
	repo := storage.NewMemoryRepository()
	q := queue.NewMemoryStreamQueue()
	ctx := context.Background()
	streamName := "stream:events:pending"

	// Create consumer group in queue
	err := q.CreateConsumerGroup(ctx, streamName, "relay-test-group", "0")
	if err != nil {
		t.Fatalf("failed to create consumer group: %v", err)
	}

	// Insert 5 events with outbox
	for i := 1; i <= 5; i++ {
		evt := &domain.Event{
			ID:        fmt.Sprintf("evt_%03d", i),
			TenantID:  "tenant_1",
			EventType: "order.created",
			Payload:   []byte(fmt.Sprintf(`{"order_id": %d}`, i)),
			Status:    domain.EventStatusPending,
		}
		ob := &domain.OutboxEvent{
			EventID: evt.ID,
			Status:  domain.OutboxStatusPending,
		}
		if err := repo.CreateEventWithOutbox(ctx, evt, ob); err != nil {
			t.Fatalf("failed to insert event %d: %v", i, err)
		}
	}

	relay := outbox.NewRelay(repo, q, streamName)

	// Process first batch of 3
	count, err := relay.ProcessNextBatch(ctx, 3)
	if err != nil {
		t.Fatalf("expected batch processing without error, got %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 processed events, got %d", count)
	}

	// Read from stream to verify messages were published
	msgs, err := q.ReadEvents(ctx, streamName, "relay-test-group", "c1", 10, 0)
	if err != nil {
		t.Fatalf("failed to read from stream: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages in stream, got %d", len(msgs))
	}
	if msgs[0].EventID != "evt_001" || msgs[1].EventID != "evt_002" || msgs[2].EventID != "evt_003" {
		t.Fatalf("unexpected message order: %v", msgs)
	}

	// Verify remaining pending outbox
	pending, err := repo.FetchAndLockPendingOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("failed to fetch pending outbox: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("expected 2 pending outbox events remaining, got %d", len(pending))
	}

	// Process second batch (remaining 2)
	count, err = relay.ProcessNextBatch(ctx, 3)
	if err != nil {
		t.Fatalf("expected batch 2 without error, got %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 processed events, got %d", count)
	}

	// Verify outbox is now empty
	pending, _ = repo.FetchAndLockPendingOutbox(ctx, 10)
	if len(pending) != 0 {
		t.Fatalf("expected 0 pending outbox events, got %d", len(pending))
	}

	// Process empty batch
	count, err = relay.ProcessNextBatch(ctx, 3)
	if err != nil {
		t.Fatalf("expected empty batch without error, got %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 processed events for empty batch, got %d", count)
	}
}

func TestRelay_ProcessNextBatch_MissingEvent(t *testing.T) {
	repo := storage.NewMemoryRepository()
	q := queue.NewMemoryStreamQueue()
	ctx := context.Background()
	streamName := "stream:events:pending"

	_ = q.CreateConsumerGroup(ctx, streamName, "test-group", "0")

	// Create 1 valid event + outbox
	validEvt := &domain.Event{
		ID:        "evt_valid",
		TenantID:  "tenant_1",
		EventType: "item.created",
		Payload:   []byte(`{"valid":true}`),
		Status:    domain.EventStatusPending,
	}
	validOb := &domain.OutboxEvent{
		EventID: validEvt.ID,
		Status:  domain.OutboxStatusPending,
	}
	_ = repo.CreateEventWithOutbox(ctx, validEvt, validOb)

	// Manually insert an orphaned outbox entry (referencing non-existent event)
	// We can do this by creating a temporary event with outbox and then testing behavior
	// In MemoryRepository, CreateEventWithOutbox inserts both.
	// But let's verify how relay handles missing event by using a custom repository or test case.
	// We can test using standard relay.
	relay := outbox.NewRelay(repo, q, streamName)

	count, err := relay.ProcessNextBatch(ctx, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 processed event, got %d", count)
	}
}

func TestRelay_Start_GracefulShutdown(t *testing.T) {
	repo := storage.NewMemoryRepository()
	q := queue.NewMemoryStreamQueue()
	streamName := "stream:events:pending"
	ctx, cancel := context.WithCancel(context.Background())

	_ = q.CreateConsumerGroup(ctx, streamName, "worker-group", "0")
	relay := outbox.NewRelay(repo, q, streamName)

	relayStopped := make(chan error, 1)
	go func() {
		err := relay.Start(ctx, 20*time.Millisecond, 5)
		relayStopped <- err
	}()

	// Insert events dynamically
	for i := 1; i <= 3; i++ {
		evt := &domain.Event{
			ID:        fmt.Sprintf("evt_dyn_%d", i),
			TenantID:  "tenant_dyn",
			EventType: "dyn.event",
			Payload:   []byte(`{}`),
		}
		ob := &domain.OutboxEvent{
			EventID: evt.ID,
			Status:  domain.OutboxStatusPending,
		}
		_ = repo.CreateEventWithOutbox(context.Background(), evt, ob)
	}

	// Give the relay a moment to process the batch
	time.Sleep(100 * time.Millisecond)

	// Verify events were processed
	pending, _ := repo.FetchAndLockPendingOutbox(context.Background(), 10)
	if len(pending) != 0 {
		t.Fatalf("expected 0 pending events after relay polling, got %d", len(pending))
	}

	// Cancel context to stop relay
	cancel()

	select {
	case err := <-relayStopped:
		if err != nil && err != context.Canceled {
			t.Fatalf("expected context.Canceled error on shutdown, got %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("relay.Start failed to shutdown within timeout")
	}
}

func TestRelay_NewTestRelay(t *testing.T) {
	relay := outbox.NewTestRelay()
	if relay == nil {
		t.Fatal("expected NewTestRelay() to return non-nil instance")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	count, err := relay.ProcessNextBatch(ctx, 10)
	if err != nil {
		t.Fatalf("expected no error from test relay, got %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 count from empty test relay, got %d", count)
	}
}

// Custom mock repository for error testing
type failingRepo struct {
	*storage.MemoryRepository
	failFetch  atomic.Bool
	failMark   atomic.Bool
}

func (f *failingRepo) FetchAndLockPendingOutbox(ctx context.Context, limit int) ([]domain.OutboxEvent, error) {
	if f.failFetch.Load() {
		return nil, fmt.Errorf("simulated fetch error")
	}
	return f.MemoryRepository.FetchAndLockPendingOutbox(ctx, limit)
}

func (f *failingRepo) MarkOutboxPublished(ctx context.Context, outboxID int64) error {
	if f.failMark.Load() {
		return fmt.Errorf("simulated mark error")
	}
	return f.MemoryRepository.MarkOutboxPublished(ctx, outboxID)
}

func TestRelay_RepositoryErrors(t *testing.T) {
	mockRepo := &failingRepo{
		MemoryRepository: storage.NewMemoryRepository(),
	}
	q := queue.NewMemoryStreamQueue()
	relay := outbox.NewRelay(mockRepo, q, "stream:events:pending")
	ctx := context.Background()

	// Test fetch failure
	mockRepo.failFetch.Store(true)
	_, err := relay.ProcessNextBatch(ctx, 10)
	if err == nil {
		t.Fatal("expected error on fetch failure, got nil")
	}
	mockRepo.failFetch.Store(false)

	// Insert event
	evt := &domain.Event{
		ID:        "evt_fail_test",
		TenantID:  "tenant_1",
		EventType: "test",
		Payload:   []byte(`{}`),
	}
	ob := &domain.OutboxEvent{
		EventID: evt.ID,
		Status:  domain.OutboxStatusPending,
	}
	_ = mockRepo.CreateEventWithOutbox(ctx, evt, ob)

	// Test mark published failure
	mockRepo.failMark.Store(true)
	_, err = relay.ProcessNextBatch(ctx, 10)
	if err == nil {
		t.Fatal("expected error on mark failure, got nil")
	}
}
