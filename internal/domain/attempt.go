package domain

import "time"

type DeliveryStatus string

const (
	DeliveryStatusSuccess  DeliveryStatus = "SUCCESS"
	DeliveryStatusRetrying DeliveryStatus = "RETRYING"
	DeliveryStatusFailed   DeliveryStatus = "FAILED"
)

type DeliveryAttempt struct {
	ID             string         `json:"id"`
	EventID        string         `json:"event_id"`
	EndpointID     string         `json:"endpoint_id"`
	AttemptNumber  int            `json:"attempt_number"`
	ResponseStatus int            `json:"response_status,omitempty"`
	ResponseBody   string         `json:"response_body,omitempty"`
	DurationMs     int            `json:"duration_ms"`
	Status         DeliveryStatus `json:"status"`
	ErrorMessage   string         `json:"error_message,omitempty"`
	ExecutedAt     time.Time      `json:"executed_at"`
}
