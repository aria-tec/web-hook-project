package dispatcher_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"web-hook-project/internal/dispatcher"
	"web-hook-project/internal/domain"
	"web-hook-project/internal/retry"
	"web-hook-project/internal/storage"
)

func TestDispatcher_SuccessDelivery200(t *testing.T) {
	secret := "whsec_super_secret_test_123"
	payload := []byte(`{"event":"payment.succeeded","amount":15000}`)

	var receivedHeaderSig string
	var receivedHeaderTimestamp string
	var receivedHeaderID string
	var receivedUserAgent string
	var receivedContentType string
	var receivedBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHeaderSig = r.Header.Get("X-Webhook-Signature")
		receivedHeaderTimestamp = r.Header.Get("X-Webhook-Timestamp")
		receivedHeaderID = r.Header.Get("X-Webhook-ID")
		receivedUserAgent = r.Header.Get("User-Agent")
		receivedContentType = r.Header.Get("Content-Type")

		var err error
		receivedBody, err = io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"received":true}`))
	}))
	defer server.Close()

	repo := storage.NewMemoryRepository()
	ctx := context.Background()

	event := &domain.Event{
		ID:        "evt_disp_001",
		TenantID:  "tenant_alpha",
		EventType: "payment.succeeded",
		Payload:   payload,
		Status:    domain.EventStatusPending,
		CreatedAt: time.Now(),
	}
	outbox := &domain.OutboxEvent{
		EventID:   event.ID,
		Status:    domain.OutboxStatusPending,
		CreatedAt: time.Now(),
	}
	if err := repo.CreateEventWithOutbox(ctx, event, outbox); err != nil {
		t.Fatalf("failed to create event: %v", err)
	}

	endpoint := &domain.Endpoint{
		ID:        "ep_disp_001",
		TenantID:  "tenant_alpha",
		URL:       server.URL,
		Secret:    secret,
		RateLimit: 100,
		IsActive:  true,
	}

	policy := retry.DefaultBackoffPolicy()
	d := dispatcher.NewDispatcher(server.Client(), repo, policy)

	attempt, err := d.Dispatch(ctx, endpoint, event, 1)
	if err != nil {
		t.Fatalf("unexpected error during dispatch: %v", err)
	}

	if attempt == nil {
		t.Fatal("expected non-nil delivery attempt")
	}
	if attempt.Status != domain.DeliveryStatusSuccess {
		t.Errorf("expected attempt status SUCCESS, got %v", attempt.Status)
	}
	if attempt.ResponseStatus != http.StatusOK {
		t.Errorf("expected response status 200, got %d", attempt.ResponseStatus)
	}
	if attempt.ResponseBody != `{"received":true}` {
		t.Errorf("expected response body {\"received\":true}, got %s", attempt.ResponseBody)
	}
	if attempt.AttemptNumber != 1 {
		t.Errorf("expected attempt number 1, got %d", attempt.AttemptNumber)
	}

	// Verify headers received by server
	if receivedUserAgent != "WebhookEngine-Dispatcher/1.0" {
		t.Errorf("expected User-Agent WebhookEngine-Dispatcher/1.0, got %s", receivedUserAgent)
	}
	if receivedContentType != "application/json" {
		t.Errorf("expected Content-Type application/json, got %s", receivedContentType)
	}
	if receivedHeaderID != event.ID {
		t.Errorf("expected X-Webhook-ID %s, got %s", event.ID, receivedHeaderID)
	}
	if receivedHeaderTimestamp == "" {
		t.Error("expected non-empty X-Webhook-Timestamp header")
	}
	if string(receivedBody) != string(payload) {
		t.Errorf("expected payload %s, got %s", string(payload), string(receivedBody))
	}

	// Cryptographic HMAC Verification
	if !dispatcher.VerifySignature(secret, receivedHeaderSig, payload, 300) {
		t.Errorf("HMAC signature verification failed for header: %s", receivedHeaderSig)
	}

	// Verify event updated to DELIVERED in repository
	updatedEvt, err := repo.GetEvent(ctx, event.ID)
	if err != nil {
		t.Fatalf("failed to fetch updated event: %v", err)
	}
	if updatedEvt.Status != domain.EventStatusDelivered {
		t.Errorf("expected event status DELIVERED, got %v", updatedEvt.Status)
	}
}

func TestDispatcher_Retryable500_Attempt1(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}))
	defer server.Close()

	repo := storage.NewMemoryRepository()
	ctx := context.Background()

	event := &domain.Event{
		ID:        "evt_disp_002",
		TenantID:  "tenant_alpha",
		EventType: "invoice.payment_failed",
		Payload:   []byte(`{"invoice":"inv_001"}`),
		Status:    domain.EventStatusPending,
		CreatedAt: time.Now(),
	}
	outbox := &domain.OutboxEvent{
		EventID:   event.ID,
		Status:    domain.OutboxStatusPending,
		CreatedAt: time.Now(),
	}
	_ = repo.CreateEventWithOutbox(ctx, event, outbox)

	endpoint := &domain.Endpoint{
		ID:        "ep_disp_002",
		TenantID:  "tenant_alpha",
		URL:       server.URL,
		Secret:    "whsec_test",
		IsActive:  true,
	}

	policy := retry.DefaultBackoffPolicy() // MaxRetries = 5
	d := dispatcher.NewDispatcher(server.Client(), repo, policy)

	attempt, err := d.Dispatch(ctx, endpoint, event, 1)
	if err != nil {
		t.Fatalf("unexpected dispatch error: %v", err)
	}

	if attempt.Status != domain.DeliveryStatusRetrying {
		t.Errorf("expected attempt status RETRYING, got %v", attempt.Status)
	}
	if attempt.ResponseStatus != http.StatusInternalServerError {
		t.Errorf("expected response status 500, got %d", attempt.ResponseStatus)
	}

	// Verify event status is NOT marked DELIVERED or DLQ
	updatedEvt, _ := repo.GetEvent(ctx, event.ID)
	if updatedEvt.Status == domain.EventStatusDelivered || updatedEvt.Status == domain.EventStatusDLQ {
		t.Errorf("expected event status to remain pending/retrying, got %v", updatedEvt.Status)
	}
}

func TestDispatcher_Retryable500_MaxRetriesReached_RoutesToDLQ(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	repo := storage.NewMemoryRepository()
	ctx := context.Background()

	event := &domain.Event{
		ID:        "evt_disp_003",
		TenantID:  "tenant_beta",
		EventType: "order.created",
		Payload:   []byte(`{"order_id":"ord_123"}`),
		Status:    domain.EventStatusPending,
		CreatedAt: time.Now(),
	}
	outbox := &domain.OutboxEvent{
		EventID:   event.ID,
		Status:    domain.OutboxStatusPending,
		CreatedAt: time.Now(),
	}
	_ = repo.CreateEventWithOutbox(ctx, event, outbox)

	endpoint := &domain.Endpoint{
		ID:        "ep_disp_003",
		TenantID:  "tenant_beta",
		URL:       server.URL,
		Secret:    "whsec_test",
		IsActive:  true,
	}

	policy := retry.DefaultBackoffPolicy() // MaxRetries = 5
	d := dispatcher.NewDispatcher(server.Client(), repo, policy)

	// Attempt number 5 (max retries reached)
	attempt, err := d.Dispatch(ctx, endpoint, event, 5)
	if err != nil {
		t.Fatalf("unexpected dispatch error: %v", err)
	}

	if attempt.Status != domain.DeliveryStatusFailed {
		t.Errorf("expected attempt status FAILED on max retries, got %v", attempt.Status)
	}

	// Verify event is routed to DLQ
	updatedEvt, _ := repo.GetEvent(ctx, event.ID)
	if updatedEvt.Status != domain.EventStatusDLQ {
		t.Errorf("expected event status DLQ, got %v", updatedEvt.Status)
	}
}

func TestDispatcher_NonRetryable400_ImmediateDLQ(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad request schema", http.StatusBadRequest)
	}))
	defer server.Close()

	repo := storage.NewMemoryRepository()
	ctx := context.Background()

	event := &domain.Event{
		ID:        "evt_disp_004",
		TenantID:  "tenant_gamma",
		EventType: "subscription.canceled",
		Payload:   []byte(`{"sub_id":"sub_999"}`),
		Status:    domain.EventStatusPending,
		CreatedAt: time.Now(),
	}
	outbox := &domain.OutboxEvent{
		EventID:   event.ID,
		Status:    domain.OutboxStatusPending,
		CreatedAt: time.Now(),
	}
	_ = repo.CreateEventWithOutbox(ctx, event, outbox)

	endpoint := &domain.Endpoint{
		ID:        "ep_disp_004",
		TenantID:  "tenant_gamma",
		URL:       server.URL,
		Secret:    "whsec_test",
		IsActive:  true,
	}

	policy := retry.DefaultBackoffPolicy()
	d := dispatcher.NewDispatcher(server.Client(), repo, policy)

	// Attempt 1 with 400 Bad Request should fail immediately and route to DLQ
	attempt, err := d.Dispatch(ctx, endpoint, event, 1)
	if err != nil {
		t.Fatalf("unexpected dispatch error: %v", err)
	}

	if attempt.Status != domain.DeliveryStatusFailed {
		t.Errorf("expected attempt status FAILED for HTTP 400, got %v", attempt.Status)
	}
	if attempt.ResponseStatus != http.StatusBadRequest {
		t.Errorf("expected response status 400, got %d", attempt.ResponseStatus)
	}

	// Verify event updated to DLQ
	updatedEvt, _ := repo.GetEvent(ctx, event.ID)
	if updatedEvt.Status != domain.EventStatusDLQ {
		t.Errorf("expected event status DLQ for non-retryable error, got %v", updatedEvt.Status)
	}
}

func TestDispatcher_NetworkFailure_Retrying(t *testing.T) {
	// Using a URL with an unallocated local port (simulate connection refused)
	repo := storage.NewMemoryRepository()
	ctx := context.Background()

	event := &domain.Event{
		ID:        "evt_disp_005",
		TenantID:  "tenant_delta",
		EventType: "user.created",
		Payload:   []byte(`{"user":"u_1"}`),
		Status:    domain.EventStatusPending,
		CreatedAt: time.Now(),
	}
	outbox := &domain.OutboxEvent{
		EventID:   event.ID,
		Status:    domain.OutboxStatusPending,
		CreatedAt: time.Now(),
	}
	_ = repo.CreateEventWithOutbox(ctx, event, outbox)

	endpoint := &domain.Endpoint{
		ID:        "ep_disp_005",
		TenantID:  "tenant_delta",
		URL:       "http://127.0.0.1:59999/webhook", // closed port
		Secret:    "whsec_test",
		IsActive:  true,
	}

	client := &http.Client{Timeout: 100 * time.Millisecond}
	policy := retry.DefaultBackoffPolicy()
	d := dispatcher.NewDispatcher(client, repo, policy)

	attempt, err := d.Dispatch(ctx, endpoint, event, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if attempt.Status != domain.DeliveryStatusRetrying {
		t.Errorf("expected attempt status RETRYING for network error, got %v", attempt.Status)
	}
	if attempt.ErrorMessage == "" {
		t.Error("expected non-empty ErrorMessage on attempt")
	}
}

func TestDispatcher_NilInputs(t *testing.T) {
	repo := storage.NewMemoryRepository()
	d := dispatcher.NewDispatcher(nil, repo, retry.DefaultBackoffPolicy())
	ctx := context.Background()

	_, err := d.Dispatch(ctx, nil, &domain.Event{}, 1)
	if err == nil {
		t.Error("expected error for nil endpoint")
	}

	endpoint := &domain.Endpoint{ID: "ep_1", URL: "http://example.com"}
	_, err = d.Dispatch(ctx, endpoint, nil, 1)
	if err == nil {
		t.Error("expected error for nil event")
	}
}
