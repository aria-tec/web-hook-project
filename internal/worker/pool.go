package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"web-hook-project/internal/dispatcher"
	"web-hook-project/internal/domain"
	"web-hook-project/internal/queue"
	"web-hook-project/internal/storage"
)

// Default configurations for worker pool.
const (
	DefaultNumWorkers   = 10
	DefaultStreamName   = "stream:events:pending"
	DefaultGroupName    = "worker-group"
	DefaultBatchSize    = 10
	DefaultPollInterval = 100 * time.Millisecond
)

// Config configures the bounded worker pool.
type Config struct {
	NumWorkers   int
	StreamName   string
	GroupName    string
	BatchSize    int64
	PollInterval time.Duration
}

// WorkerPool manages a bounded set of worker goroutines consuming events from Redis Streams,
// resolving endpoints, and executing egress HTTP deliveries via Dispatcher.
type WorkerPool struct {
	cfg        Config
	queue      queue.StreamQueue
	repo       storage.Repository
	dispatcher *dispatcher.Dispatcher

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started atomic.Bool
	stopped atomic.Bool
}

// NewWorkerPool constructs a new bounded WorkerPool with sensible defaults.
func NewWorkerPool(cfg Config, q queue.StreamQueue, repo storage.Repository, disp *dispatcher.Dispatcher) *WorkerPool {
	if cfg.NumWorkers <= 0 {
		cfg.NumWorkers = DefaultNumWorkers
	}
	if cfg.StreamName == "" {
		cfg.StreamName = DefaultStreamName
	}
	if cfg.GroupName == "" {
		cfg.GroupName = DefaultGroupName
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = DefaultBatchSize
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = DefaultPollInterval
	}

	return &WorkerPool{
		cfg:        cfg,
		queue:      q,
		repo:       repo,
		dispatcher: disp,
	}
}

// Start creates the consumer group in the stream queue and launches the worker goroutines.
func (p *WorkerPool) Start(ctx context.Context) error {
	if !p.started.CompareAndSwap(false, true) {
		return errors.New("worker pool is already started")
	}

	// Idempotently create consumer group starting from earliest unread message or stream end
	_ = p.queue.CreateConsumerGroup(ctx, p.cfg.StreamName, p.cfg.GroupName, "0")

	p.ctx, p.cancel = context.WithCancel(ctx)

	for i := 0; i < p.cfg.NumWorkers; i++ {
		p.wg.Add(1)
		go p.workerLoop(p.ctx, i)
	}

	return nil
}

func (p *WorkerPool) workerLoop(ctx context.Context, workerID int) {
	defer p.wg.Done()

	consumerName := fmt.Sprintf("worker-%d", workerID)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		messages, err := p.queue.ReadEvents(ctx, p.cfg.StreamName, p.cfg.GroupName, consumerName, p.cfg.BatchSize, p.cfg.PollInterval)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return
			}
			// Transient read error: backoff briefly before next read attempt
			select {
			case <-ctx.Done():
				return
			case <-time.After(20 * time.Millisecond):
				continue
			}
		}

		if len(messages) == 0 {
			continue
		}

		for _, msg := range messages {
			if ctx.Err() != nil {
				return
			}

			p.processMessage(ctx, msg)
			_ = p.queue.AckEvent(ctx, p.cfg.StreamName, p.cfg.GroupName, msg.ID)
		}
	}
}

func (p *WorkerPool) processMessage(ctx context.Context, msg queue.QueueMessage) {
	if msg.TenantID == "" {
		return
	}

	// 1. Resolve registered target endpoints for tenant
	endpoints, err := p.repo.GetEndpointsByTenant(ctx, msg.TenantID)
	if err != nil {
		return
	}

	// Filter active endpoints
	var activeEndpoints []domain.Endpoint
	for _, ep := range endpoints {
		if ep.IsActive {
			activeEndpoints = append(activeEndpoints, ep)
		}
	}

	if len(activeEndpoints) == 0 {
		return
	}

	// 2. Resolve event object
	event, err := p.repo.GetEvent(ctx, msg.EventID)
	if err != nil || event == nil {
		event = &domain.Event{
			ID:        msg.EventID,
			TenantID:  msg.TenantID,
			EventType: msg.EventType,
			Payload:   msg.Payload,
			CreatedAt: msg.CreatedAt,
		}
	}

	// 3. Dispatch to all active endpoints
	for _, ep := range activeEndpoints {
		if ctx.Err() != nil {
			return
		}
		endpointCopy := ep
		_, _ = p.dispatcher.Dispatch(ctx, &endpointCopy, event, 1)
	}
}

// Stop signals all workers to stop and waits for them to drain cleanly.
func (p *WorkerPool) Stop() {
	if !p.stopped.CompareAndSwap(false, true) {
		return
	}

	if p.cancel != nil {
		p.cancel()
	}

	p.wg.Wait()
}
