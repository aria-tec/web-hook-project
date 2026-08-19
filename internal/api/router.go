package api

import (
	"net/http"

	"web-hook-project/internal/idempotency"
	"web-hook-project/internal/storage"
	"web-hook-project/internal/telemetry"
)

// NewRouter constructs an http.Handler with all routes configured.
func NewRouter(h *Handler, metrics ...*telemetry.Metrics) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/v1/events", h.HandleIngestEvent)
	mux.HandleFunc("POST /api/v1/endpoints", h.HandleCreateEndpoint)
	mux.HandleFunc("GET /api/v1/endpoints", h.HandleListEndpoints)
	mux.HandleFunc("GET /healthz", h.HandleHealthz)

	var m *telemetry.Metrics
	if len(metrics) > 0 && metrics[0] != nil {
		m = metrics[0]
	} else if h.metrics != nil {
		m = h.metrics
	} else {
		m = telemetry.NewMetrics()
	}
	mux.Handle("GET /metrics", m.Handler())

	return mux
}

// SetupTestRouter constructs a router with in-memory storage and idempotency guard for testing.
func SetupTestRouter() http.Handler {
	repo := storage.NewMemoryRepository()
	guard := idempotency.NewMemoryGuard()
	h := NewHandler(repo, guard)
	return NewRouter(h)
}
