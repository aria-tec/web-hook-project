package webhookclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client is the official Go client for Mini-Svix event publishing and DLQ management.
type Client struct {
	baseURL    string
	tenantID   string
	apiKey     string
	httpClient *http.Client
	timeout    time.Duration
}

// New creates a new WebhookClient with the provided base URL, tenant ID, and options.
func New(baseURL string, tenantID string, opts ...Option) *Client {
	c := &Client{
		baseURL:  strings.TrimRight(baseURL, "/"),
		tenantID: tenantID,
		timeout:  30 * time.Second,
	}

	for _, opt := range opts {
		opt(c)
	}

	if c.httpClient == nil {
		c.httpClient = &http.Client{
			Timeout: c.timeout,
		}
	}

	return c
}

// NewClient is an alias for New.
func NewClient(baseURL string, tenantID string, opts ...Option) *Client {
	return New(baseURL, tenantID, opts...)
}

// Publish sends an event to the Mini-Svix ingestion endpoint.
func (c *Client) Publish(ctx context.Context, eventType string, payload any, opts ...PublishOption) (*PublishResult, error) {
	if eventType == "" {
		return nil, errors.New("eventType is required")
	}

	po := &publishOptions{}
	for _, opt := range opts {
		opt(po)
	}

	var reqPayload any = payload
	if b, ok := payload.([]byte); ok {
		if json.Valid(b) {
			reqPayload = json.RawMessage(b)
		}
	}

	bodyBytes, err := json.Marshal(map[string]any{
		"event_type": eventType,
		"payload":    reqPayload,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal publish payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v1/events", bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", c.tenantID)
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	if po.idempotencyKey != "" {
		req.Header.Set("X-Idempotency-Key", po.idempotencyKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, parseAPIError(resp.StatusCode, respBody)
	}

	var result PublishResult
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("failed to decode publish response: %w", err)
	}

	return &result, nil
}

// ListDLQ retrieves dead-lettered events for the client's tenant with pagination.
func (c *Client) ListDLQ(ctx context.Context, limit, offset int) ([]DLQEvent, error) {
	reqURL, err := url.Parse(c.baseURL + "/api/v1/dlq")
	if err != nil {
		return nil, fmt.Errorf("invalid DLQ URL: %w", err)
	}

	q := reqURL.Query()
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	if offset > 0 {
		q.Set("offset", strconv.Itoa(offset))
	}
	reqURL.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Tenant-ID", c.tenantID)
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, parseAPIError(resp.StatusCode, respBody)
	}

	var events []DLQEvent
	if err := json.Unmarshal(respBody, &events); err != nil {
		return nil, fmt.Errorf("failed to decode DLQ events: %w", err)
	}
	if events == nil {
		events = []DLQEvent{}
	}

	return events, nil
}

// ReplayDLQ replays dead-lettered events back into the delivery pipeline.
func (c *Client) ReplayDLQ(ctx context.Context, eventIDs []string) (*ReplayResult, error) {
	if len(eventIDs) == 0 {
		return nil, errors.New("eventIDs cannot be empty")
	}

	bodyBytes, err := json.Marshal(map[string]any{
		"event_ids": eventIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal replay request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v1/dlq/replay", bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", c.tenantID)
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, parseAPIError(resp.StatusCode, respBody)
	}

	var result ReplayResult
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("failed to decode replay response: %w", err)
	}

	return &result, nil
}

func parseAPIError(statusCode int, body []byte) *APIError {
	apiErr := &APIError{
		StatusCode: statusCode,
		Body:       body,
	}

	var errResp struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &errResp); err == nil && errResp.Error != "" {
		apiErr.Message = errResp.Error
	} else if len(body) > 0 {
		apiErr.Message = strings.TrimSpace(string(body))
	}

	return apiErr
}
