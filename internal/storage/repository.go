package storage

import (
	"context"
	"errors"

	"web-hook-project/internal/domain"
)

var (
	ErrNotFound     = errors.New("record not found")
	ErrDuplicateKey = errors.New("duplicate key violation")
)

// Repository defines data access methods for tenants, endpoints, events, outbox, and delivery attempts.
type Repository interface {
	CreateTenant(ctx context.Context, tenant *domain.Tenant) error
	GetTenant(ctx context.Context, id string) (*domain.Tenant, error)
	CreateEndpoint(ctx context.Context, endpoint *domain.Endpoint) error
	GetEndpoint(ctx context.Context, id string) (*domain.Endpoint, error)
	GetEndpointsByTenant(ctx context.Context, tenantID string) ([]domain.Endpoint, error)
	CreateEventWithOutbox(ctx context.Context, event *domain.Event, outbox *domain.OutboxEvent) error
	FetchAndLockPendingOutbox(ctx context.Context, limit int) ([]domain.OutboxEvent, error)
	MarkOutboxPublished(ctx context.Context, outboxID int64) error
	RecordDeliveryAttempt(ctx context.Context, attempt *domain.DeliveryAttempt) error
	UpdateEventStatus(ctx context.Context, eventID string, status domain.EventStatus) error
	GetEvent(ctx context.Context, id string) (*domain.Event, error)
}
