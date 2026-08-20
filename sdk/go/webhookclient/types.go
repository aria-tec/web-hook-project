package webhookclient

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Option configures a Client instance.
type Option func(*Client)

// PublishOption configures individual Publish requests.
type PublishOption func(*publishOptions)

type publishOptions struct {
	idempotencyKey string
}

// WithIdempotencyKey sets the X-Idempotency-Key header for duplicate prevention.
func WithIdempotencyKey(key string) PublishOption {
	return func(o *publishOptions) {
		o.idempotencyKey = key
	}
}

// WithAPIKey configures the Bearer token authorization header.
func WithAPIKey(apiKey string) Option {
	return func(c *Client) {
		c.apiKey = apiKey
	}
}

// WithHTTPClient configures a custom *http.Client for the SDK.
func WithHTTPClient(client *http.Client) Option {
	return func(c *Client) {
		if client != nil {
			c.httpClient = client
		}
	}
}

// WithTimeout configures a default timeout for the client's HTTP transport.
func WithTimeout(d time.Duration) Option {
	return func(c *Client) {
		if c.httpClient != nil {
			c.httpClient.Timeout = d
		} else {
			c.timeout = d
		}
	}
}

// PublishResult contains the outcome of a published webhook event.
type PublishResult struct {
	ID        string    `json:"id"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// DLQEvent represents a failed event routed to the Dead-Letter Queue.
type DLQEvent struct {
	ID             string          `json:"id"`
	TenantID       string          `json:"tenant_id"`
	EventType      string          `json:"event_type"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Payload        json.RawMessage `json:"payload"`
	Status         string          `json:"status"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      *time.Time      `json:"updated_at,omitempty"`
}

// ReplayResult contains the summary of dead-letter events re-queued for delivery.
type ReplayResult struct {
	Status        string `json:"status"`
	ReplayedCount int    `json:"replayed_count"`
}

// APIError is returned when the API responds with a non-2xx status code.
type APIError struct {
	StatusCode int    `json:"status_code"`
	Message    string `json:"message"`
	Body       []byte `json:"-"`
}

func (e *APIError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("webhook API error (status %d): %s", e.StatusCode, e.Message)
	}
	return fmt.Sprintf("webhook API error (status %d)", e.StatusCode)
}
