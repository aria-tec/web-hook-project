package storage

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"web-hook-project/internal/domain"
)

// PostgresRepository implements Repository using PostgreSQL.
type PostgresRepository struct {
	db *sql.DB
}

// NewPostgresRepository creates a new PostgresRepository instance.
func NewPostgresRepository(db *sql.DB) *PostgresRepository {
	return &PostgresRepository{db: db}
}

func (r *PostgresRepository) CreateTenant(ctx context.Context, tenant *domain.Tenant) error {
	createdAt := tenant.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}

	query := `
		INSERT INTO tenants (id, name, created_at)
		VALUES ($1, $2, $3)
	`
	_, err := r.db.ExecContext(ctx, query, tenant.ID, tenant.Name, createdAt)
	return err
}

func (r *PostgresRepository) GetTenant(ctx context.Context, id string) (*domain.Tenant, error) {
	query := `
		SELECT id, name, created_at
		FROM tenants
		WHERE id = $1
	`
	var t domain.Tenant
	err := r.db.QueryRowContext(ctx, query, id).Scan(&t.ID, &t.Name, &t.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &t, nil
}

func (r *PostgresRepository) CreateEndpoint(ctx context.Context, endpoint *domain.Endpoint) error {
	now := time.Now()
	createdAt := endpoint.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	updatedAt := endpoint.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = now
	}

	query := `
		INSERT INTO endpoints (id, tenant_id, url, secret, rate_limit, is_active, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	_, err := r.db.ExecContext(ctx, query,
		endpoint.ID,
		endpoint.TenantID,
		endpoint.URL,
		endpoint.Secret,
		endpoint.RateLimit,
		endpoint.IsActive,
		createdAt,
		updatedAt,
	)
	return err
}

func (r *PostgresRepository) GetEndpoint(ctx context.Context, id string) (*domain.Endpoint, error) {
	query := `
		SELECT id, tenant_id, url, secret, rate_limit, is_active, created_at, updated_at
		FROM endpoints
		WHERE id = $1
	`
	var ep domain.Endpoint
	err := r.db.QueryRowContext(ctx, query, id).Scan(
		&ep.ID,
		&ep.TenantID,
		&ep.URL,
		&ep.Secret,
		&ep.RateLimit,
		&ep.IsActive,
		&ep.CreatedAt,
		&ep.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &ep, nil
}

func (r *PostgresRepository) GetEndpointsByTenant(ctx context.Context, tenantID string) ([]domain.Endpoint, error) {
	query := `
		SELECT id, tenant_id, url, secret, rate_limit, is_active, created_at, updated_at
		FROM endpoints
		WHERE tenant_id = $1
		ORDER BY created_at ASC
	`
	rows, err := r.db.QueryContext(ctx, query, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var endpoints []domain.Endpoint
	for rows.Next() {
		var ep domain.Endpoint
		if err := rows.Scan(
			&ep.ID,
			&ep.TenantID,
			&ep.URL,
			&ep.Secret,
			&ep.RateLimit,
			&ep.IsActive,
			&ep.CreatedAt,
			&ep.UpdatedAt,
		); err != nil {
			return nil, err
		}
		endpoints = append(endpoints, ep)
	}
	return endpoints, rows.Err()
}

func (r *PostgresRepository) CreateEventWithOutbox(ctx context.Context, event *domain.Event, outbox *domain.OutboxEvent) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := time.Now()
	if event.CreatedAt.IsZero() {
		event.CreatedAt = now
	}
	if outbox.CreatedAt.IsZero() {
		outbox.CreatedAt = now
	}

	var idempKey *string
	if event.IdempotencyKey != "" {
		idempKey = &event.IdempotencyKey
	}

	eventQuery := `
		INSERT INTO events (id, tenant_id, event_type, idempotency_key, payload, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`
	_, err = tx.ExecContext(ctx, eventQuery,
		event.ID,
		event.TenantID,
		event.EventType,
		idempKey,
		event.Payload,
		string(event.Status),
		event.CreatedAt,
	)
	if err != nil {
		return err
	}

	outboxQuery := `
		INSERT INTO outbox_events (event_id, status, retry_count, created_at)
		VALUES ($1, $2, $3, $4)
		RETURNING id
	`
	err = tx.QueryRowContext(ctx, outboxQuery,
		event.ID,
		string(outbox.Status),
		outbox.RetryCount,
		outbox.CreatedAt,
	).Scan(&outbox.ID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (r *PostgresRepository) FetchAndLockPendingOutbox(ctx context.Context, limit int) ([]domain.OutboxEvent, error) {
	if limit <= 0 {
		limit = 100
	}

	query := `
		SELECT id, event_id, status, retry_count, created_at, processed_at
		FROM outbox_events
		WHERE status = 'PENDING'
		ORDER BY id ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`
	rows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []domain.OutboxEvent
	for rows.Next() {
		var ob domain.OutboxEvent
		var statusStr string
		var processedAt sql.NullTime

		if err := rows.Scan(
			&ob.ID,
			&ob.EventID,
			&statusStr,
			&ob.RetryCount,
			&ob.CreatedAt,
			&processedAt,
		); err != nil {
			return nil, err
		}

		ob.Status = domain.OutboxStatus(statusStr)
		if processedAt.Valid {
			ob.ProcessedAt = &processedAt.Time
		}
		events = append(events, ob)
	}

	return events, rows.Err()
}

func (r *PostgresRepository) MarkOutboxPublished(ctx context.Context, outboxID int64) error {
	query := `
		UPDATE outbox_events
		SET status = 'PUBLISHED', processed_at = $1
		WHERE id = $2
	`
	res, err := r.db.ExecContext(ctx, query, time.Now(), outboxID)
	if err != nil {
		return err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *PostgresRepository) RecordDeliveryAttempt(ctx context.Context, attempt *domain.DeliveryAttempt) error {
	executedAt := attempt.ExecutedAt
	if executedAt.IsZero() {
		executedAt = time.Now()
	}

	var respStatus *int
	if attempt.ResponseStatus != 0 {
		respStatus = &attempt.ResponseStatus
	}

	var respBody *string
	if attempt.ResponseBody != "" {
		respBody = &attempt.ResponseBody
	}

	var errMsg *string
	if attempt.ErrorMessage != "" {
		errMsg = &attempt.ErrorMessage
	}

	query := `
		INSERT INTO delivery_attempts (
			id, event_id, endpoint_id, attempt_number, response_status,
			response_body, duration_ms, status, error_message, executed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`
	_, err := r.db.ExecContext(ctx, query,
		attempt.ID,
		attempt.EventID,
		attempt.EndpointID,
		attempt.AttemptNumber,
		respStatus,
		respBody,
		attempt.DurationMs,
		string(attempt.Status),
		errMsg,
		executedAt,
	)
	return err
}

func (r *PostgresRepository) UpdateEventStatus(ctx context.Context, eventID string, status domain.EventStatus) error {
	query := `
		UPDATE events
		SET status = $1
		WHERE id = $2
	`
	res, err := r.db.ExecContext(ctx, query, string(status), eventID)
	if err != nil {
		return err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *PostgresRepository) GetEvent(ctx context.Context, id string) (*domain.Event, error) {
	query := `
		SELECT id, tenant_id, event_type, idempotency_key, payload, status, created_at
		FROM events
		WHERE id = $1
	`
	var evt domain.Event
	var idempKey sql.NullString
	var statusStr string

	err := r.db.QueryRowContext(ctx, query, id).Scan(
		&evt.ID,
		&evt.TenantID,
		&evt.EventType,
		&idempKey,
		&evt.Payload,
		&statusStr,
		&evt.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	if idempKey.Valid {
		evt.IdempotencyKey = idempKey.String
	}
	evt.Status = domain.EventStatus(statusStr)
	return &evt, nil
}
