package domain

import "time"

type OutboxStatus string

const (
	OutboxStatusPending   OutboxStatus = "PENDING"
	OutboxStatusPublished OutboxStatus = "PUBLISHED"
	OutboxStatusFailed    OutboxStatus = "FAILED"
)

type OutboxEvent struct {
	ID          int64        `json:"id"`
	EventID     string       `json:"event_id"`
	Status      OutboxStatus `json:"status"`
	RetryCount  int          `json:"retry_count"`
	CreatedAt   time.Time    `json:"created_at"`
	ProcessedAt *time.Time   `json:"processed_at,omitempty"`
}
