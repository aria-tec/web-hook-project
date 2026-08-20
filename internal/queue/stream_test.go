package queue_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"web-hook-project/internal/domain"
	"web-hook-project/internal/queue"
)

func TestMemoryStreamQueue_PublishAndRead(t *testing.T) {
	q := queue.NewMemoryStreamQueue()
	ctx := context.Background()
	stream := "stream:events:pending"
	group := "workers"

	err := q.CreateConsumerGroup(ctx, stream, group, "0")
	if err != nil {
		t.Fatalf("failed to create consumer group: %v", err)
	}

	// Publish 3 events
	for i := 1; i <= 3; i++ {
		evt := &domain.Event{
			ID:        fmt.Sprintf("evt_%03d", i),
			TenantID:  "tenant_alpha",
			EventType: "order.created",
			Payload:   []byte(fmt.Sprintf(`{"order_id": %d}`, i)),
			Status:    domain.EventStatusPending,
			CreatedAt: time.Now(),
		}
		msgID, err := q.PublishEvent(ctx, stream, evt)
		if err != nil {
			t.Fatalf("failed to publish event %d: %v", i, err)
		}
		if msgID == "" {
			t.Fatalf("expected non-empty message ID for event %d", i)
		}
	}

	// Read batch of 2
	msgs, err := q.ReadEvents(ctx, stream, group, "consumer_1", 2, 0)
	if err != nil {
		t.Fatalf("failed to read events: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}
	if msgs[0].EventID != "evt_001" || msgs[1].EventID != "evt_002" {
		t.Fatalf("unexpected event IDs: %s, %s", msgs[0].EventID, msgs[1].EventID)
	}
	if msgs[0].TenantID != "tenant_alpha" || msgs[0].EventType != "order.created" {
		t.Fatalf("unexpected tenant/type: %s, %s", msgs[0].TenantID, msgs[0].EventType)
	}
	if string(msgs[0].Payload) != `{"order_id": 1}` {
		t.Fatalf("unexpected payload: %s", string(msgs[0].Payload))
	}

	// Read next batch
	msgs2, err := q.ReadEvents(ctx, stream, group, "consumer_1", 2, 0)
	if err != nil {
		t.Fatalf("failed to read events batch 2: %v", err)
	}
	if len(msgs2) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs2))
	}
	if msgs2[0].EventID != "evt_003" {
		t.Fatalf("unexpected event ID: %s", msgs2[0].EventID)
	}

	// Read on drained stream
	msgs3, err := q.ReadEvents(ctx, stream, group, "consumer_1", 2, 0)
	if err != nil {
		t.Fatalf("failed to read from drained stream: %v", err)
	}
	if len(msgs3) != 0 {
		t.Fatalf("expected 0 messages on drained stream, got %d", len(msgs3))
	}
}

func TestMemoryStreamQueue_ConsumerGroupStartID(t *testing.T) {
	ctx := context.Background()
	stream := "stream:start_id_test"

	q := queue.NewMemoryStreamQueue()

	// Publish before group creation
	evt1 := &domain.Event{
		ID:        "evt_early",
		TenantID:  "tenant_1",
		EventType: "test",
		Payload:   []byte(`{"test":1}`),
		CreatedAt: time.Now(),
	}
	_, err := q.PublishEvent(ctx, stream, evt1)
	if err != nil {
		t.Fatalf("publish error: %v", err)
	}

	// Group with startID = "$" (only new events)
	err = q.CreateConsumerGroup(ctx, stream, "group_dollar", "$")
	if err != nil {
		t.Fatalf("create group $ error: %v", err)
	}

	// Group with startID = "0" (all events from start)
	err = q.CreateConsumerGroup(ctx, stream, "group_zero", "0")
	if err != nil {
		t.Fatalf("create group 0 error: %v", err)
	}

	// Publish second event
	evt2 := &domain.Event{
		ID:        "evt_late",
		TenantID:  "tenant_1",
		EventType: "test",
		Payload:   []byte(`{"test":2}`),
		CreatedAt: time.Now(),
	}
	_, err = q.PublishEvent(ctx, stream, evt2)
	if err != nil {
		t.Fatalf("publish error: %v", err)
	}

	// group_dollar should only get evt_late
	msgsDollar, err := q.ReadEvents(ctx, stream, "group_dollar", "c1", 10, 0)
	if err != nil {
		t.Fatalf("read group_dollar error: %v", err)
	}
	if len(msgsDollar) != 1 || msgsDollar[0].EventID != "evt_late" {
		t.Fatalf("expected 1 late message in group_dollar, got %v", msgsDollar)
	}

	// group_zero should get both evt_early and evt_late
	msgsZero, err := q.ReadEvents(ctx, stream, "group_zero", "c1", 10, 0)
	if err != nil {
		t.Fatalf("read group_zero error: %v", err)
	}
	if len(msgsZero) != 2 {
		t.Fatalf("expected 2 messages in group_zero, got %d", len(msgsZero))
	}
	if msgsZero[0].EventID != "evt_early" || msgsZero[1].EventID != "evt_late" {
		t.Fatalf("unexpected messages in group_zero: %v", msgsZero)
	}
}

func TestMemoryStreamQueue_AckEvent(t *testing.T) {
	q := queue.NewMemoryStreamQueue()
	ctx := context.Background()
	stream := "stream:ack_test"
	group := "workers"

	_ = q.CreateConsumerGroup(ctx, stream, group, "0")

	evt := &domain.Event{
		ID:        "evt_ack_01",
		TenantID:  "tenant_1",
		EventType: "test",
		Payload:   []byte(`{}`),
		CreatedAt: time.Now(),
	}
	msgID, _ := q.PublishEvent(ctx, stream, evt)

	msgs, _ := q.ReadEvents(ctx, stream, group, "c1", 1, 0)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}

	// Ack message
	if err := q.AckEvent(ctx, stream, group, msgID); err != nil {
		t.Fatalf("failed to ack message: %v", err)
	}

	// Empty ack should not error
	if err := q.AckEvent(ctx, stream, group); err != nil {
		t.Fatalf("empty ack should succeed: %v", err)
	}
}

func TestMemoryStreamQueue_BlockingRead(t *testing.T) {
	q := queue.NewMemoryStreamQueue()
	ctx := context.Background()
	stream := "stream:block_test"
	group := "workers"

	_ = q.CreateConsumerGroup(ctx, stream, group, "0")

	readDone := make(chan []queue.QueueMessage, 1)
	errChan := make(chan error, 1)

	go func() {
		msgs, err := q.ReadEvents(ctx, stream, group, "consumer_block", 1, 2*time.Second)
		if err != nil {
			errChan <- err
			return
		}
		readDone <- msgs
	}()

	time.Sleep(50 * time.Millisecond)

	evt := &domain.Event{
		ID:        "evt_async_01",
		TenantID:  "tenant_block",
		EventType: "async.event",
		Payload:   []byte(`{"async":true}`),
		CreatedAt: time.Now(),
	}
	_, err := q.PublishEvent(ctx, stream, evt)
	if err != nil {
		t.Fatalf("failed to publish async event: %v", err)
	}

	select {
	case err := <-errChan:
		t.Fatalf("read goroutine returned error: %v", err)
	case msgs := <-readDone:
		if len(msgs) != 1 {
			t.Fatalf("expected 1 message from blocking read, got %d", len(msgs))
		}
		if msgs[0].EventID != "evt_async_01" {
			t.Fatalf("expected evt_async_01, got %s", msgs[0].EventID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("blocking read timed out waiting for published message")
	}
}

func TestMemoryStreamQueue_BlockingReadTimeout(t *testing.T) {
	q := queue.NewMemoryStreamQueue()
	ctx := context.Background()
	stream := "stream:timeout_test"
	group := "workers"

	_ = q.CreateConsumerGroup(ctx, stream, group, "0")

	start := time.Now()
	msgs, err := q.ReadEvents(ctx, stream, group, "c1", 5, 50*time.Millisecond)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected no error on timeout, got %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("expected 0 messages on timeout, got %d", len(msgs))
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("expected blocking read to wait at least ~40ms, waited %v", elapsed)
	}
}

func TestMemoryStreamQueue_ContextCancellation(t *testing.T) {
	q := queue.NewMemoryStreamQueue()
	ctx, cancel := context.WithCancel(context.Background())
	stream := "stream:cancel_test"
	group := "workers"

	_ = q.CreateConsumerGroup(ctx, stream, group, "0")

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, err := q.ReadEvents(ctx, stream, group, "c1", 1, 2*time.Second)
	if err == nil {
		t.Fatal("expected context cancellation error, got nil")
	}
}

func TestMemoryStreamQueue_ConcurrentPublishAndRead(t *testing.T) {
	q := queue.NewMemoryStreamQueue()
	ctx := context.Background()
	stream := "stream:concurrent_test"
	group := "workers"

	_ = q.CreateConsumerGroup(ctx, stream, group, "0")

	numPublishers := 10
	eventsPerPublisher := 50
	totalEvents := numPublishers * eventsPerPublisher

	var pubWg sync.WaitGroup
	pubWg.Add(numPublishers)

	for p := 0; p < numPublishers; p++ {
		go func(pubIdx int) {
			defer pubWg.Done()
			for e := 0; e < eventsPerPublisher; e++ {
				evt := &domain.Event{
					ID:        fmt.Sprintf("evt_p%d_e%d", pubIdx, e),
					TenantID:  "tenant_conc",
					EventType: "concurrent.test",
					Payload:   []byte(`{}`),
					CreatedAt: time.Now(),
				}
				_, err := q.PublishEvent(ctx, stream, evt)
				if err != nil {
					t.Errorf("publish error: %v", err)
				}
			}
		}(p)
	}

	pubWg.Wait()

	// Read all events concurrently using 5 consumers
	numConsumers := 5
	var readCount int64
	var countMu sync.Mutex
	var readWg sync.WaitGroup
	readWg.Add(numConsumers)

	for c := 0; c < numConsumers; c++ {
		go func(cIdx int) {
			defer readWg.Done()
			consumerName := fmt.Sprintf("consumer_%d", cIdx)
			for {
				msgs, err := q.ReadEvents(ctx, stream, group, consumerName, 10, 20*time.Millisecond)
				if err != nil {
					t.Errorf("read error: %v", err)
					return
				}
				if len(msgs) == 0 {
					break
				}
				countMu.Lock()
				readCount += int64(len(msgs))
				countMu.Unlock()
			}
		}(c)
	}

	readWg.Wait()

	if int(readCount) != totalEvents {
		t.Fatalf("expected to read %d total events, got %d", totalEvents, readCount)
	}
}

func TestMemoryStreamQueue_InvalidInputs(t *testing.T) {
	q := queue.NewMemoryStreamQueue()
	ctx := context.Background()

	_, err := q.PublishEvent(ctx, "stream", nil)
	if err == nil {
		t.Fatal("expected error when publishing nil event")
	}

	// Reading with empty stream or group
	_, err = q.ReadEvents(ctx, "", "group", "c1", 1, 0)
	if err == nil {
		t.Fatal("expected error with empty stream name")
	}

	_, err = q.ReadEvents(ctx, "stream", "", "c1", 1, 0)
	if err == nil {
		t.Fatal("expected error with empty group name")
	}
}

func TestMemoryStreamQueue_AutoClaim(t *testing.T) {
	q := queue.NewMemoryStreamQueue()
	ctx := context.Background()
	stream := "stream:autoclaim_test"
	group := "workers"

	_ = q.CreateConsumerGroup(ctx, stream, group, "0")

	// Publish 2 events
	for i := 1; i <= 2; i++ {
		evt := &domain.Event{
			ID:        fmt.Sprintf("evt_claim_%d", i),
			TenantID:  "tenant_1",
			EventType: "test.claim",
			Payload:   []byte(`{"test":true}`),
			CreatedAt: time.Now(),
		}
		_, err := q.PublishEvent(ctx, stream, evt)
		if err != nil {
			t.Fatalf("publish error: %v", err)
		}
	}

	// Read by consumer_dead
	msgs, err := q.ReadEvents(ctx, stream, group, "consumer_dead", 2, 0)
	if err != nil || len(msgs) != 2 {
		t.Fatalf("expected 2 messages read, got %d, err: %v", len(msgs), err)
	}

	// AutoClaim immediately with 100ms minIdle -> should return 0 (not idle yet)
	claimed, _, err := q.AutoClaim(ctx, stream, group, "consumer_alive", 100*time.Millisecond, "0-0", 10)
	if err != nil {
		t.Fatalf("autoclaim error: %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("expected 0 claimed messages before minIdle, got %d", len(claimed))
	}

	// Wait 120ms
	time.Sleep(120 * time.Millisecond)

	// AutoClaim now -> should return both messages
	claimed, _, err = q.AutoClaim(ctx, stream, group, "consumer_alive", 100*time.Millisecond, "0-0", 10)
	if err != nil {
		t.Fatalf("autoclaim error after minIdle: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("expected 2 claimed messages after minIdle, got %d", len(claimed))
	}
	if claimed[0].EventID != "evt_claim_1" && claimed[1].EventID != "evt_claim_1" {
		t.Fatalf("missing evt_claim_1 in claimed messages: %v", claimed)
	}

	// Acking one message
	err = q.AckEvent(ctx, stream, group, claimed[0].ID)
	if err != nil {
		t.Fatalf("ack error: %v", err)
	}

	// Wait 120ms again
	time.Sleep(120 * time.Millisecond)

	// AutoClaim now -> should only claim the 1 unacked message
	claimed2, _, err := q.AutoClaim(ctx, stream, group, "consumer_alive", 100*time.Millisecond, "0-0", 10)
	if err != nil {
		t.Fatalf("autoclaim error: %v", err)
	}
	if len(claimed2) != 1 {
		t.Fatalf("expected 1 claimed message after ack, got %d", len(claimed2))
	}
}

