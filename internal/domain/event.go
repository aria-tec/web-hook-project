package domain

import (
	"errors"
	"time"
)

type EventStatus string

const (
	EventStatusPending   EventStatus = "PENDING"
	EventStatusDelivered EventStatus = "DELIVERED"
	EventStatusFailed    EventStatus = "FAILED"
	EventStatusDLQ       EventStatus = "DLQ"
)

type Event struct {
	ID             string      `json:"id"`
	TenantID       string      `json:"tenant_id"`
	EventType      string      `json:"event_type"`
	IdempotencyKey string      `json:"idempotency_key,omitempty"`
	Payload        []byte      `json:"payload"`
	Status         EventStatus `json:"status"`
	CreatedAt      time.Time   `json:"created_at"`
}

func (e *Event) Validate() error {
	if e.TenantID == "" {
		return errors.New("tenant_id is required")
	}
	if e.EventType == "" {
		return errors.New("event_type is required")
	}
	if len(e.Payload) == 0 {
		return errors.New("payload cannot be empty")
	}
	return nil
}
