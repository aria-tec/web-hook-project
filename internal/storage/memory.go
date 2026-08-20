package storage

import (
	"context"
	"sort"
	"sync"
	"time"

	"web-hook-project/internal/domain"
)

// MemoryRepository is an in-memory thread-safe implementation of Repository.
type MemoryRepository struct {
	mu               sync.RWMutex
	tenants          map[string]*domain.Tenant
	endpoints        map[string]*domain.Endpoint
	events           map[string]*domain.Event
	outboxEvents     map[int64]*domain.OutboxEvent
	attempts         map[string]*domain.DeliveryAttempt
	idempotencyIndex map[string]string // "tenantID:idempotencyKey" -> eventID
	nextOutboxID     int64
}

// NewMemoryRepository creates a new in-memory repository instance.
func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		tenants:          make(map[string]*domain.Tenant),
		endpoints:        make(map[string]*domain.Endpoint),
		events:           make(map[string]*domain.Event),
		outboxEvents:     make(map[int64]*domain.OutboxEvent),
		attempts:         make(map[string]*domain.DeliveryAttempt),
		idempotencyIndex: make(map[string]string),
	}
}

// NewMockRepository is an alias for NewMemoryRepository.
func NewMockRepository() *MemoryRepository {
	return NewMemoryRepository()
}

func (m *MemoryRepository) CreateTenant(ctx context.Context, tenant *domain.Tenant) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.tenants[tenant.ID]; exists {
		return ErrDuplicateKey
	}

	tCopy := *tenant
	if tCopy.CreatedAt.IsZero() {
		tCopy.CreatedAt = time.Now()
	}
	m.tenants[tenant.ID] = &tCopy
	return nil
}

func (m *MemoryRepository) GetTenant(ctx context.Context, id string) (*domain.Tenant, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	t, exists := m.tenants[id]
	if !exists {
		return nil, ErrNotFound
	}

	tCopy := *t
	return &tCopy, nil
}

func (m *MemoryRepository) CreateEndpoint(ctx context.Context, endpoint *domain.Endpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.endpoints[endpoint.ID]; exists {
		return ErrDuplicateKey
	}

	epCopy := *endpoint
	now := time.Now()
	if epCopy.CreatedAt.IsZero() {
		epCopy.CreatedAt = now
	}
	if epCopy.UpdatedAt.IsZero() {
		epCopy.UpdatedAt = now
	}
	m.endpoints[endpoint.ID] = &epCopy
	return nil
}

func (m *MemoryRepository) GetEndpoint(ctx context.Context, id string) (*domain.Endpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	ep, exists := m.endpoints[id]
	if !exists {
		return nil, ErrNotFound
	}

	epCopy := *ep
	return &epCopy, nil
}

func (m *MemoryRepository) GetEndpointsByTenant(ctx context.Context, tenantID string) ([]domain.Endpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []domain.Endpoint
	for _, ep := range m.endpoints {
		if ep.TenantID == tenantID {
			result = append(result, *ep)
		}
	}
	return result, nil
}

func (m *MemoryRepository) CreateEventWithOutbox(ctx context.Context, event *domain.Event, outbox *domain.OutboxEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check idempotency constraint
	if event.IdempotencyKey != "" {
		idempKey := event.TenantID + ":" + event.IdempotencyKey
		if _, exists := m.idempotencyIndex[idempKey]; exists {
			return ErrDuplicateKey
		}
		m.idempotencyIndex[idempKey] = event.ID
	}

	if _, exists := m.events[event.ID]; exists {
		return ErrDuplicateKey
	}

	m.nextOutboxID++
	outbox.ID = m.nextOutboxID
	now := time.Now()
	if event.CreatedAt.IsZero() {
		event.CreatedAt = now
	}
	if outbox.CreatedAt.IsZero() {
		outbox.CreatedAt = now
	}

	eCopy := *event
	if len(event.Payload) > 0 {
		eCopy.Payload = make([]byte, len(event.Payload))
		copy(eCopy.Payload, event.Payload)
	}
	obCopy := *outbox

	m.events[event.ID] = &eCopy
	m.outboxEvents[outbox.ID] = &obCopy

	return nil
}

func (m *MemoryRepository) FetchAndLockPendingOutbox(ctx context.Context, limit int) ([]domain.OutboxEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	var pending []domain.OutboxEvent
	for _, ob := range m.outboxEvents {
		if ob.Status == domain.OutboxStatusPending {
			pending = append(pending, *ob)
		}
	}

	sort.Slice(pending, func(i, j int) bool {
		return pending[i].ID < pending[j].ID
	})

	if limit > 0 && len(pending) > limit {
		pending = pending[:limit]
	}

	return pending, nil
}

func (m *MemoryRepository) MarkOutboxPublished(ctx context.Context, outboxID int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	ob, exists := m.outboxEvents[outboxID]
	if !exists {
		return ErrNotFound
	}

	ob.Status = domain.OutboxStatusPublished
	now := time.Now()
	ob.ProcessedAt = &now
	return nil
}

func (m *MemoryRepository) RecordDeliveryAttempt(ctx context.Context, attempt *domain.DeliveryAttempt) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.attempts[attempt.ID]; exists {
		return ErrDuplicateKey
	}

	aCopy := *attempt
	if aCopy.ExecutedAt.IsZero() {
		aCopy.ExecutedAt = time.Now()
	}
	m.attempts[attempt.ID] = &aCopy
	return nil
}

func (m *MemoryRepository) UpdateEventStatus(ctx context.Context, eventID string, status domain.EventStatus) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	evt, exists := m.events[eventID]
	if !exists {
		return ErrNotFound
	}

	evt.Status = status
	return nil
}

func (m *MemoryRepository) GetEvent(ctx context.Context, id string) (*domain.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	evt, exists := m.events[id]
	if !exists {
		return nil, ErrNotFound
	}

	eCopy := *evt
	if len(evt.Payload) > 0 {
		eCopy.Payload = make([]byte, len(evt.Payload))
		copy(eCopy.Payload, evt.Payload)
	}
	return &eCopy, nil
}

func (m *MemoryRepository) GetDLQEvents(ctx context.Context, tenantID string, limit, offset int) ([]domain.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	var dlqEvents []domain.Event
	for _, evt := range m.events {
		if evt.TenantID == tenantID && evt.Status == domain.EventStatusDLQ {
			eCopy := *evt
			if len(evt.Payload) > 0 {
				eCopy.Payload = make([]byte, len(evt.Payload))
				copy(eCopy.Payload, evt.Payload)
			}
			dlqEvents = append(dlqEvents, eCopy)
		}
	}

	sort.Slice(dlqEvents, func(i, j int) bool {
		return dlqEvents[i].CreatedAt.After(dlqEvents[j].CreatedAt)
	})

	if offset > 0 {
		if offset >= len(dlqEvents) {
			return []domain.Event{}, nil
		}
		dlqEvents = dlqEvents[offset:]
	}

	if limit > 0 && len(dlqEvents) > limit {
		dlqEvents = dlqEvents[:limit]
	}

	return dlqEvents, nil
}

func (m *MemoryRepository) ReplayDLQEvents(ctx context.Context, tenantID string, eventIDs []string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	replayedCount := 0
	now := time.Now()

	for _, id := range eventIDs {
		evt, exists := m.events[id]
		if !exists || evt.TenantID != tenantID || evt.Status != domain.EventStatusDLQ {
			continue
		}

		// Replay: mark event as PENDING and create a new PENDING outbox record
		evt.Status = domain.EventStatusPending

		m.nextOutboxID++
		ob := &domain.OutboxEvent{
			ID:         m.nextOutboxID,
			EventID:    evt.ID,
			Status:     domain.OutboxStatusPending,
			RetryCount: 0,
			CreatedAt:  now,
		}
		m.outboxEvents[ob.ID] = ob
		replayedCount++
	}

	return replayedCount, nil
}

