package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"

	"web-hook-project/internal/domain"
	"web-hook-project/internal/idempotency"
	"web-hook-project/internal/storage"
	"web-hook-project/internal/telemetry"
)

const (
	HeaderTenantID       = "X-Tenant-ID"
	HeaderIdempotencyKey = "X-Idempotency-Key"
	DefaultIdempTTL      = 24 * time.Hour
)

// Handler provides HTTP handlers for ingestion, endpoints management, and health checks.
type Handler struct {
	repo      storage.Repository
	guard     idempotency.Guard
	idempTTL  time.Duration
	metrics   *telemetry.Metrics
	sseBroker *SSEBroker
}

// NewHandler creates a new API Handler instance.
func NewHandler(repo storage.Repository, guard idempotency.Guard) *Handler {
	return &Handler{
		repo:     repo,
		guard:    guard,
		idempTTL: DefaultIdempTTL,
	}
}

// WithMetrics sets the telemetry metrics collector on the handler.
func (h *Handler) WithMetrics(m *telemetry.Metrics) *Handler {
	h.metrics = m
	return h
}

// WithSSEBroker attaches an SSEBroker to the handler for live event streaming.
func (h *Handler) WithSSEBroker(b *SSEBroker) *Handler {
	h.sseBroker = b
	return h
}

// GetSSEBroker returns the configured SSEBroker instance, if any.
func (h *Handler) GetSSEBroker() *SSEBroker {
	return h.sseBroker
}

// IngestEventRequest represents incoming webhook payload for event ingestion.
type IngestEventRequest struct {
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
}

// IngestEventResponse represents response on accepted webhook ingestion.
type IngestEventResponse struct {
	ID        string    `json:"id"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// CreateEndpointRequest represents payload to register a tenant webhook endpoint.
type CreateEndpointRequest struct {
	URL       string `json:"url"`
	Secret    string `json:"secret"`
	RateLimit int    `json:"rate_limit"`
}

// ErrorResponse standardizes error messages.
type ErrorResponse struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(data)
}

func writeJSONError(w http.ResponseWriter, statusCode int, message string) {
	writeJSON(w, statusCode, ErrorResponse{Error: message})
}

// HandleIngestEvent processes event ingestion with idempotency and transactional outbox.
func (h *Handler) HandleIngestEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	tenantID := r.Header.Get(HeaderTenantID)
	if tenantID == "" {
		writeJSONError(w, http.StatusBadRequest, "X-Tenant-ID header is required")
		return
	}

	idempKey := r.Header.Get(HeaderIdempotencyKey)
	var lockAcquired bool

	if idempKey != "" && h.guard != nil {
		acquired, cached, err := h.guard.AcquireLock(r.Context(), tenantID, idempKey, h.idempTTL)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, "idempotency check failed")
			return
		}
		if cached != nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Idempotency-Replay", "true")
			w.WriteHeader(cached.StatusCode)
			_, _ = w.Write(cached.Body)
			return
		}
		if !acquired {
			writeJSONError(w, http.StatusConflict, "concurrent request in flight with same idempotency key")
			return
		}
		lockAcquired = true
	}

	var req IngestEventRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if lockAcquired {
			_ = h.guard.ReleaseLock(r.Context(), tenantID, idempKey)
		}
		writeJSONError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}

	if req.EventType == "" {
		if lockAcquired {
			_ = h.guard.ReleaseLock(r.Context(), tenantID, idempKey)
		}
		writeJSONError(w, http.StatusBadRequest, "event_type is required")
		return
	}

	if len(req.Payload) == 0 || string(req.Payload) == "null" {
		if lockAcquired {
			_ = h.guard.ReleaseLock(r.Context(), tenantID, idempKey)
		}
		writeJSONError(w, http.StatusBadRequest, "payload cannot be empty")
		return
	}

	if !json.Valid(req.Payload) {
		if lockAcquired {
			_ = h.guard.ReleaseLock(r.Context(), tenantID, idempKey)
		}
		writeJSONError(w, http.StatusBadRequest, "payload must be valid JSON")
		return
	}

	now := time.Now().UTC()
	eventID := fmt.Sprintf("evt_%s", uuid.New().String())

	event := &domain.Event{
		ID:             eventID,
		TenantID:       tenantID,
		EventType:      req.EventType,
		IdempotencyKey: idempKey,
		Payload:        req.Payload,
		Status:         domain.EventStatusPending,
		CreatedAt:      now,
	}

	outbox := &domain.OutboxEvent{
		EventID:   eventID,
		Status:    domain.OutboxStatusPending,
		CreatedAt: now,
	}

	if err := h.repo.CreateEventWithOutbox(r.Context(), event, outbox); err != nil {
		if lockAcquired {
			_ = h.guard.ReleaseLock(r.Context(), tenantID, idempKey)
		}
		if errors.Is(err, storage.ErrDuplicateKey) {
			writeJSONError(w, http.StatusConflict, "duplicate event or idempotency key violation")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "failed to persist event and outbox record")
		return
	}

	if h.metrics != nil {
		h.metrics.IncIngested(tenantID, req.EventType)
	}

	resp := IngestEventResponse{
		ID:        event.ID,
		Status:    string(event.Status),
		CreatedAt: event.CreatedAt,
	}

	respBytes, err := json.Marshal(resp)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to marshal response")
		return
	}

	if lockAcquired {
		_ = h.guard.SetResponse(r.Context(), tenantID, idempKey, http.StatusAccepted, respBytes, h.idempTTL)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write(respBytes)
}

// HandleCreateEndpoint registers a new webhook target endpoint for a tenant.
func (h *Handler) HandleCreateEndpoint(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	tenantID := r.Header.Get(HeaderTenantID)
	if tenantID == "" {
		writeJSONError(w, http.StatusBadRequest, "X-Tenant-ID header is required")
		return
	}

	var req CreateEndpointRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	rateLimit := req.RateLimit
	if rateLimit <= 0 {
		rateLimit = 100
	}

	now := time.Now().UTC()
	endpoint := &domain.Endpoint{
		ID:        fmt.Sprintf("ep_%s", uuid.New().String()),
		TenantID:  tenantID,
		URL:       req.URL,
		Secret:    req.Secret,
		RateLimit: rateLimit,
		IsActive:  true,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := endpoint.Validate(); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := h.repo.CreateEndpoint(r.Context(), endpoint); err != nil {
		if errors.Is(err, storage.ErrDuplicateKey) {
			writeJSONError(w, http.StatusConflict, "endpoint already exists")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "failed to create endpoint")
		return
	}

	writeJSON(w, http.StatusCreated, endpoint)
}

// HandleListEndpoints returns all registered webhook target endpoints for a tenant.
func (h *Handler) HandleListEndpoints(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	tenantID := r.Header.Get(HeaderTenantID)
	if tenantID == "" {
		writeJSONError(w, http.StatusBadRequest, "X-Tenant-ID header is required")
		return
	}

	endpoints, err := h.repo.GetEndpointsByTenant(r.Context(), tenantID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to retrieve endpoints")
		return
	}

	if endpoints == nil {
		endpoints = []domain.Endpoint{}
	}

	writeJSON(w, http.StatusOK, endpoints)
}

// HandleHealthz responds with health status.
func (h *Handler) HandleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().UTC(),
	})
}

// ReplayDLQRequest represents the request body for replaying DLQ events.
type ReplayDLQRequest struct {
	EventIDs []string `json:"event_ids"`
}

// HandleListDLQ returns dead-lettered events for a tenant.
func (h *Handler) HandleListDLQ(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	tenantID := r.Header.Get(HeaderTenantID)
	if tenantID == "" {
		writeJSONError(w, http.StatusBadRequest, "X-Tenant-ID header is required")
		return
	}

	limit := 50
	offset := 0

	events, err := h.repo.GetDLQEvents(r.Context(), tenantID, limit, offset)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to retrieve DLQ events")
		return
	}

	if events == nil {
		events = []domain.Event{}
	}

	writeJSON(w, http.StatusOK, events)
}

// HandleReplayDLQ replays dead-lettered events back into the transactional outbox pipeline.
func (h *Handler) HandleReplayDLQ(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	tenantID := r.Header.Get(HeaderTenantID)
	if tenantID == "" {
		writeJSONError(w, http.StatusBadRequest, "X-Tenant-ID header is required")
		return
	}

	var req ReplayDLQRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request JSON")
		return
	}

	if len(req.EventIDs) == 0 {
		writeJSONError(w, http.StatusBadRequest, "event_ids array cannot be empty")
		return
	}

	if len(req.EventIDs) > 100 {
		writeJSONError(w, http.StatusBadRequest, "event_ids batch size cannot exceed 100")
		return
	}

	replayedCount, err := h.repo.ReplayDLQEvents(r.Context(), tenantID, req.EventIDs)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to replay events: %v", err))
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":         "QUEUED_FOR_RETRY",
		"replayed_count": replayedCount,
	})
}

// HandleEventStream handles SSE live stream requests for delivery attempts.
func (h *Handler) HandleEventStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodOptions {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if h.sseBroker != nil {
		h.sseBroker.ServeHTTP(w, r)
		return
	}
	writeJSONError(w, http.StatusServiceUnavailable, "SSE stream broker not configured")
}


