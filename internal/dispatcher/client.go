package dispatcher

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"web-hook-project/internal/domain"
	"web-hook-project/internal/retry"
	"web-hook-project/internal/storage"
	"web-hook-project/internal/telemetry"
)

// Dispatcher executes egress HTTP webhook deliveries with SSRF safety, HMAC-SHA256 signing,
// response tracking, and DLQ/retry classification.
type Dispatcher struct {
	client  *http.Client
	repo    storage.Repository
	policy  retry.BackoffPolicy
	metrics *telemetry.Metrics
}

// NewDispatcher creates a new Dispatcher instance with the given HTTP client, storage repository, and retry policy.
func NewDispatcher(client *http.Client, repo storage.Repository, policy retry.BackoffPolicy) *Dispatcher {
	if client == nil {
		client = NewSafeHTTPClient(10 * time.Second)
	}
	if policy.MaxRetries == 0 {
		policy = retry.DefaultBackoffPolicy()
	}
	return &Dispatcher{
		client: client,
		repo:   repo,
		policy: policy,
	}
}

// WithMetrics sets the telemetry metrics collector on the dispatcher.
func (d *Dispatcher) WithMetrics(m *telemetry.Metrics) *Dispatcher {
	d.metrics = m
	return d
}

// Policy returns the dispatcher's configured BackoffPolicy.
func (d *Dispatcher) Policy() retry.BackoffPolicy {
	return d.policy
}

// Dispatch signs the payload, executes the HTTP POST request to the destination endpoint,
// records the delivery attempt in storage, and updates the event status accordingly.
func (d *Dispatcher) Dispatch(ctx context.Context, endpoint *domain.Endpoint, event *domain.Event, attemptNum int) (*domain.DeliveryAttempt, error) {
	if endpoint == nil {
		return nil, errors.New("endpoint cannot be nil")
	}
	if event == nil {
		return nil, errors.New("event cannot be nil")
	}
	if attemptNum <= 0 {
		attemptNum = 1
	}

	ts := time.Now().Unix()
	sigHeader := SignPayload(endpoint.Secret, ts, event.Payload)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.URL, bytes.NewReader(event.Payload))
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "WebhookEngine-Dispatcher/1.0")
	req.Header.Set("X-Webhook-ID", event.ID)
	req.Header.Set("X-Webhook-Timestamp", strconv.FormatInt(ts, 10))
	req.Header.Set("X-Webhook-Signature", sigHeader)

	start := time.Now()
	resp, httpErr := d.client.Do(req)
	duration := time.Since(start)
	durationMs := int(duration.Milliseconds())

	var statusCode int
	var respBody string
	if resp != nil {
		statusCode = resp.StatusCode
		if resp.Body != nil {
			bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			respBody = string(bodyBytes)
		}
	}

	var status domain.DeliveryStatus
	var errMsg string

	if httpErr == nil && statusCode >= 200 && statusCode < 300 {
		status = domain.DeliveryStatusSuccess
	} else {
		if httpErr != nil {
			errMsg = httpErr.Error()
		} else {
			errMsg = fmt.Sprintf("HTTP %d: %s", statusCode, http.StatusText(statusCode))
		}

		if retry.IsRetryable(statusCode, httpErr) && attemptNum < d.policy.MaxRetries {
			status = domain.DeliveryStatusRetrying
		} else {
			status = domain.DeliveryStatusFailed
		}
	}

	attempt := &domain.DeliveryAttempt{
		ID:             fmt.Sprintf("att_%s", uuid.New().String()),
		EventID:        event.ID,
		EndpointID:     endpoint.ID,
		AttemptNumber:  attemptNum,
		ResponseStatus: statusCode,
		ResponseBody:   respBody,
		DurationMs:     durationMs,
		Status:         status,
		ErrorMessage:   errMsg,
		ExecutedAt:     start,
	}

	if d.repo != nil {
		if recErr := d.repo.RecordDeliveryAttempt(ctx, attempt); recErr != nil {
			return attempt, fmt.Errorf("failed to record delivery attempt: %w", recErr)
		}

		if status == domain.DeliveryStatusSuccess {
			_ = d.repo.UpdateEventStatus(ctx, event.ID, domain.EventStatusDelivered)
		} else if status == domain.DeliveryStatusFailed {
			_ = d.repo.UpdateEventStatus(ctx, event.ID, domain.EventStatusDLQ)
		}
	}

	if d.metrics != nil {
		d.metrics.ObserveDeliveryDuration(endpoint.TenantID, endpoint.ID, duration.Seconds())
		if status == domain.DeliveryStatusSuccess {
			d.metrics.IncDelivered(endpoint.TenantID, endpoint.ID, strconv.Itoa(statusCode))
		} else if status == domain.DeliveryStatusFailed {
			reason := errMsg
			if reason == "" {
				reason = fmt.Sprintf("status_%d", statusCode)
			}
			d.metrics.IncDLQ(endpoint.TenantID, endpoint.ID, reason)
		}
	}

	return attempt, nil
}
