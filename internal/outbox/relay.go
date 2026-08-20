package outbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"web-hook-project/internal/queue"
	"web-hook-project/internal/storage"
)

// DefaultStreamName is the default Redis stream destination for pending events.
const DefaultStreamName = "stream:events:pending"

// Relay manages background polling and publishing of transactional outbox records to Redis Streams.
type Relay struct {
	repo       storage.Repository
	queue      queue.StreamQueue
	streamName string
}

// NewRelay creates a new outbox Relay worker instance.
func NewRelay(repo storage.Repository, q queue.StreamQueue, streamName ...string) *Relay {
	sn := DefaultStreamName
	if len(streamName) > 0 && streamName[0] != "" {
		sn = streamName[0]
	}
	return &Relay{
		repo:       repo,
		queue:      q,
		streamName: sn,
	}
}

// NewTestRelay returns a test-ready Relay wired with in-memory repository and stream queue.
func NewTestRelay() *Relay {
	return NewRelay(storage.NewMemoryRepository(), queue.NewMemoryStreamQueue())
}

// ProcessNextBatch fetches a batch of pending outbox records, fetches the associated events,
// publishes them to the stream queue, and marks the outbox records as published.
// Returns the number of successfully published records and any error encountered.
func (r *Relay) ProcessNextBatch(ctx context.Context, batchSize int) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if batchSize <= 0 {
		batchSize = 100
	}

	outboxEvents, err := r.repo.FetchAndLockPendingOutbox(ctx, batchSize)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch pending outbox events: %w", err)
	}

	if len(outboxEvents) == 0 {
		return 0, nil
	}

	processedCount := 0
	for _, ob := range outboxEvents {
		if err := ctx.Err(); err != nil {
			return processedCount, err
		}

		evt, err := r.repo.GetEvent(ctx, ob.EventID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				// Missing event record: skip so subsequent valid outbox records are not blocked
				continue
			}
			return processedCount, fmt.Errorf("failed to get event %s: %w", ob.EventID, err)
		}

		_, err = r.queue.PublishEvent(ctx, r.streamName, evt)
		if err != nil {
			return processedCount, fmt.Errorf("failed to publish event %s to queue: %w", evt.ID, err)
		}

		err = r.repo.MarkOutboxPublished(ctx, ob.ID)
		if err != nil {
			return processedCount, fmt.Errorf("failed to mark outbox %d as published: %w", ob.ID, err)
		}

		processedCount++
	}

	return processedCount, nil
}

// Start runs a continuous background polling loop that processes pending outbox batches
// at the specified pollInterval until the context is canceled.
func (r *Relay) Start(ctx context.Context, pollInterval time.Duration, batchSize int) error {
	if pollInterval <= 0 {
		pollInterval = 100 * time.Millisecond
	}
	if batchSize <= 0 {
		batchSize = 100
	}

	consecutiveErrors := 0

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if consecutiveErrors > 0 {
			// Progressive backoff with jitter on broker/db outages
			shift := consecutiveErrors
			if shift > 6 {
				shift = 6
			}
			backoff := time.Duration(1<<shift) * 50 * time.Millisecond
			if backoff > 3*time.Second {
				backoff = 3 * time.Second
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		} else {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(pollInterval):
			}
		}

		for {
			count, err := r.ProcessNextBatch(ctx, batchSize)
			if err != nil {
				consecutiveErrors++
				break
			}

			consecutiveErrors = 0
			// If fewer items than batchSize were processed, the pending queue is drained
			if count < batchSize {
				break
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
	}
}

