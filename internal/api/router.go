package api

import (
	"net/http"

	"web-hook-project/internal/idempotency"
	"web-hook-project/internal/storage"
	"web-hook-project/internal/telemetry"
)

// NewRouter constructs an http.Handler with all routes configured and CORS middleware applied.
func NewRouter(h *Handler, metrics ...*telemetry.Metrics) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/v1/events", h.HandleIngestEvent)
	mux.HandleFunc("POST /api/v1/endpoints", h.HandleCreateEndpoint)
	mux.HandleFunc("GET /api/v1/endpoints", h.HandleListEndpoints)
	mux.HandleFunc("GET /api/v1/dlq", h.HandleListDLQ)
	mux.HandleFunc("POST /api/v1/dlq/replay", h.HandleReplayDLQ)
	mux.HandleFunc("GET /api/v1/events/stream", h.HandleEventStream)
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

	return corsMiddleware(mux)
}

// corsMiddleware attaches CORS headers to responses and handles OPTIONS preflight requests.
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
		} else {
			w.Header().Set("Access-Control-Allow-Origin", "*")
		}
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Authorization, Content-Type, X-Tenant-ID, X-Idempotency-Key, Cache-Control")
		w.Header().Set("Access-Control-Expose-Headers", "Content-Length, X-Idempotency-Replay")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// SetupTestRouter constructs a router with in-memory storage, idempotency guard, and SSE broker for testing.
func SetupTestRouter() http.Handler {
	repo := storage.NewMemoryRepository()
	guard := idempotency.NewMemoryGuard()
	broker := NewSSEBroker()
	h := NewHandler(repo, guard).WithSSEBroker(broker)
	return NewRouter(h)
}

