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
	"web-hook-project/internal/retry"
	"web-hook-project/internal/storage"
)

// Default configurations for worker pool.
const (
	DefaultNumWorkers        = 10
	DefaultStreamName        = "stream:events:pending"
	DefaultGroupName         = "worker-group"
	DefaultBatchSize         = 10
	DefaultPollInterval      = 100 * time.Millisecond
	DefaultMinIdleDuration   = 10 * time.Second
	DefaultClaimInterval     = 5 * time.Second
	DefaultMaxClaimAttempts  = 5
)

// Config configures the bounded worker pool.
type Config struct {
	NumWorkers        int
	StreamName        string
	GroupName         string
	BatchSize         int64
	PollInterval      time.Duration
	MinIdleDuration   time.Duration
	ClaimInterval     time.Duration
	MaxClaimAttempts  int
}

// WorkerPool manages a bounded set of worker goroutines consuming events from Redis Streams,
// resolving endpoints, and executing egress HTTP deliveries via Dispatcher.
type WorkerPool struct {
	cfg        Config
	queue      queue.StreamQueue
	repo       storage.Repository
	dispatcher *dispatcher.Dispatcher

	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	started       atomic.Bool
	stopped       atomic.Bool
	claimMu       sync.Mutex
	claimAttempts map[string]int
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
	if cfg.MinIdleDuration <= 0 {
		cfg.MinIdleDuration = DefaultMinIdleDuration
	}
	if cfg.ClaimInterval <= 0 {
		cfg.ClaimInterval = DefaultClaimInterval
	}
	if cfg.MaxClaimAttempts <= 0 {
		cfg.MaxClaimAttempts = DefaultMaxClaimAttempts
	}

	return &WorkerPool{
		cfg:           cfg,
		queue:         q,
		repo:          repo,
		dispatcher:    disp,
		claimAttempts: make(map[string]int),
	}
}

// Start creates the consumer group in the stream queue and launches the worker goroutines and PEL reclaimer.
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

	// Launch background PEL (Pending Entries List) recovery loop
	p.wg.Add(1)
	go p.pelRecoveryLoop(p.ctx)

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

// pelRecoveryLoop periodically claims and recovers stuck/idle messages in Redis Streams PEL.
func (p *WorkerPool) pelRecoveryLoop(ctx context.Context) {
	defer p.wg.Done()

	ticker := time.NewTicker(p.cfg.ClaimInterval)
	defer ticker.Stop()

	consumerName := "pel-auto-reclaimer"
	startID := "0-0"

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for {
				if ctx.Err() != nil {
					return
				}

				claimed, nextStart, err := p.queue.AutoClaim(
					ctx,
					p.cfg.StreamName,
					p.cfg.GroupName,
					consumerName,
					p.cfg.MinIdleDuration,
					startID,
					p.cfg.BatchSize,
				)
				if err != nil {
					if errors.Is(err, context.Canceled) || ctx.Err() != nil {
						return
					}
					// Pause on transient error
					break
				}

				if len(claimed) == 0 {
					startID = "0-0"
					break
				}

				for _, msg := range claimed {
					if ctx.Err() != nil {
						return
					}

					// Poison Pill Ceiling: Track claim counts per message ID
					p.claimMu.Lock()
					p.claimAttempts[msg.ID]++
					count := p.claimAttempts[msg.ID]
					p.claimMu.Unlock()

					if count > p.cfg.MaxClaimAttempts {
						// Exceeded max claims -> poison pill: route to DLQ and ACK to unblock queue
						if p.repo != nil {
							_ = p.repo.UpdateEventStatus(ctx, msg.EventID, domain.EventStatusDLQ)
						}
						_ = p.queue.AckEvent(ctx, p.cfg.StreamName, p.cfg.GroupName, msg.ID)
						continue
					}

					p.processMessage(ctx, msg)
					_ = p.queue.AckEvent(ctx, p.cfg.StreamName, p.cfg.GroupName, msg.ID)
				}

				startID = nextStart
				if startID == "0-0" || startID == "" {
					break
				}
			}
		}
	}
}

func (p *WorkerPool) processMessage(ctx context.Context, msg queue.QueueMessage) {
	if msg.TenantID == "" {
		return
	}

	// 1. Zombie Worker Fencing / Optimistic Status Guard:
	// If the event was already delivered or routed to DLQ by another worker/claim, skip re-dispatching
	if p.repo != nil {
		latestEvt, err := p.repo.GetEvent(ctx, msg.EventID)
		if err == nil && latestEvt != nil {
			if latestEvt.Status == domain.EventStatusDelivered || latestEvt.Status == domain.EventStatusDLQ {
				return
			}
		}
	}

	// 2. Resolve registered target endpoints for tenant
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

	// 3. Resolve event object
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

	// 4. Dispatch to all active endpoints with exponential retry backoff
	for _, ep := range activeEndpoints {
		if ctx.Err() != nil {
			return
		}
		endpointCopy := ep
		attemptNum := 1
		for {
			if ctx.Err() != nil {
				return
			}
			attempt, _ := p.dispatcher.Dispatch(ctx, &endpointCopy, event, attemptNum)
			if attempt == nil || attempt.Status != domain.DeliveryStatusRetrying {
				break
			}
			attemptNum++
			if attemptNum > p.dispatcher.Policy().MaxRetries {
				break
			}
			backoff := retry.CalculateBackoff(attemptNum, p.dispatcher.Policy())
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
		}
	}
}


// Stop signals all workers and the PEL reclaimer to stop and waits for them to drain cleanly.
func (p *WorkerPool) Stop() {
	if !p.stopped.CompareAndSwap(false, true) {
		return
	}

	if p.cancel != nil {
		p.cancel()
	}

	p.wg.Wait()
}

