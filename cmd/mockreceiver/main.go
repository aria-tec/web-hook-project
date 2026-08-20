package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
)

// CapturedRequest represents an ingested webhook payload for inspection and verification.
type CapturedRequest struct {
	ID         string              `json:"id"`
	URL        string              `json:"url"`
	Method     string              `json:"method"`
	Headers    map[string][]string `json:"headers"`
	Signature  string              `json:"signature,omitempty"`
	WebhookID  string              `json:"webhook_id,omitempty"`
	Body       string              `json:"body"`
	ReceivedAt time.Time           `json:"received_at"`
}

// RingBuffer provides a concurrency-safe circular log buffer for captured requests.
type RingBuffer struct {
	mu       sync.RWMutex
	capacity int
	entries  []CapturedRequest
}

// NewRingBuffer creates a RingBuffer bounded by the given capacity.
func NewRingBuffer(capacity int) *RingBuffer {
	if capacity <= 0 {
		capacity = 50
	}
	return &RingBuffer{
		capacity: capacity,
		entries:  make([]CapturedRequest, 0, capacity),
	}
}

// Add stores a captured request into the ring buffer, evicting the oldest entries if at capacity.
func (rb *RingBuffer) Add(req CapturedRequest) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if len(rb.entries) >= rb.capacity {
		// Evict oldest item
		rb.entries = append(rb.entries[1:], req)
	} else {
		rb.entries = append(rb.entries, req)
	}
}

// GetAll returns a thread-safe snapshot of all captured requests in chronological order.
func (rb *RingBuffer) GetAll() []CapturedRequest {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	result := make([]CapturedRequest, len(rb.entries))
	copy(result, rb.entries)
	return result
}

// Clear empties the circular log buffer.
func (rb *RingBuffer) Clear() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.entries = make([]CapturedRequest, 0, rb.capacity)
}

// Len returns the current count of captured requests.
func (rb *RingBuffer) Len() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return len(rb.entries)
}

// FlakyState manages attempt counts for flaky endpoint simulation.
type FlakyState struct {
	mu       sync.Mutex
	attempts map[string]int
	global   int
}

// NewFlakyState creates a initialized FlakyState.
func NewFlakyState() *FlakyState {
	return &FlakyState{
		attempts: make(map[string]int),
	}
}

// NextAttempt increments and returns the attempt count for the given key (or global count if key is empty).
func (fs *FlakyState) NextAttempt(key string) int {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if key != "" {
		fs.attempts[key]++
		return fs.attempts[key]
	}
	fs.global++
	return fs.global
}

// Reset clears the flaky state counters.
func (fs *FlakyState) Reset() {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.attempts = make(map[string]int)
	fs.global = 0
}

// MockServer holds the state and routes for the mock webhook receiver.
type MockServer struct {
	buffer     *RingBuffer
	flakyState *FlakyState
	router     http.Handler
}

// NewMockServer constructs a new MockServer with default routes and buffer capacity.
func NewMockServer() *Server {
	return NewMockServerWithCapacity(50)
}

// Server alias for MockServer
type Server = MockServer

// NewMockServerWithCapacity constructs a new MockServer with a specified log buffer capacity.
func NewMockServerWithCapacity(capacity int) *MockServer {
	s := &MockServer{
		buffer:     NewRingBuffer(capacity),
		flakyState: NewFlakyState(),
	}

	mux := http.NewServeMux()

	// Webhook target sink routes
	mux.HandleFunc("/webhook/success", s.handleWebhookSuccess)
	mux.HandleFunc("/webhook/flaky", s.handleWebhookFlaky)
	mux.HandleFunc("/webhook/poison", s.handleWebhookPoison)
	mux.HandleFunc("/webhook/slow", s.handleWebhookSlow)

	// Inspection and management routes
	mux.HandleFunc("/inspect/logs", s.handleInspectLogs)
	mux.HandleFunc("/requests", s.handleInspectLogs) // Alias for backwards-compatibility / convenience
	mux.HandleFunc("/inspect/clear", s.handleInspectClear)

	// Health check routes
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/health", s.handleHealth)

	s.router = s.corsMiddleware(mux)
	return s
}

// Handler returns the HTTP handler for the server.
func (s *MockServer) Handler() http.Handler {
	return s.router
}

// Buffer returns the underlying log buffer.
func (s *MockServer) Buffer() *RingBuffer {
	return s.buffer
}

// Reset clears both the inspection buffer and flaky state.
func (s *MockServer) Reset() {
	s.buffer.Clear()
	s.flakyState.Reset()
}

// corsMiddleware wraps requests with universal CORS headers and handles preflight OPTIONS requests.
func (s *MockServer) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Tenant-ID, X-Webhook-ID, X-Webhook-Signature, X-Idempotency-Key, Svix-Id, Svix-Timestamp, Svix-Signature")
		w.Header().Set("Access-Control-Expose-Headers", "*")
		w.Header().Set("Access-Control-Max-Age", "86400")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// captureRequest reads the request body, records headers/signature, and stores into the ring buffer.
func (s *MockServer) captureRequest(r *http.Request) (string, error) {
	var bodyBytes []byte
	var err error
	if r.Body != nil {
		bodyBytes, err = io.ReadAll(r.Body)
		if err != nil {
			return "", err
		}
	}

	bodyStr := string(bodyBytes)

	// Extract webhook signature and ID headers
	sig := r.Header.Get("X-Webhook-Signature")
	if sig == "" {
		sig = r.Header.Get("Svix-Signature")
	}

	webhookID := r.Header.Get("X-Webhook-ID")
	if webhookID == "" {
		webhookID = r.Header.Get("Svix-Id")
	}

	captured := CapturedRequest{
		ID:         uuid.New().String(),
		URL:        r.URL.String(),
		Method:     r.Method,
		Headers:    r.Header.Clone(),
		Signature:  sig,
		WebhookID:  webhookID,
		Body:       bodyStr,
		ReceivedAt: time.Now().UTC(),
	}

	s.buffer.Add(captured)
	return bodyStr, nil
}

// handleWebhookSuccess processes 200 OK webhook deliveries.
func (s *MockServer) handleWebhookSuccess(w http.ResponseWriter, r *http.Request) {
	if _, err := s.captureRequest(r); err != nil {
		http.Error(w, `{"error":"failed to read body"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":      "success",
		"received":    true,
		"received_at": time.Now().UTC().Format(time.RFC3339Nano),
	})
}

// handleWebhookFlaky simulates intermittent 500 failures (fails on 1st & 2nd attempts, succeeds on 3rd).
func (s *MockServer) handleWebhookFlaky(w http.ResponseWriter, r *http.Request) {
	if _, err := s.captureRequest(r); err != nil {
		http.Error(w, `{"error":"failed to read body"}`, http.StatusInternalServerError)
		return
	}

	key := r.Header.Get("X-Webhook-ID")
	if key == "" {
		key = r.Header.Get("Svix-Id")
	}

	attempt := s.flakyState.NextAttempt(key)

	w.Header().Set("Content-Type", "application/json")
	if attempt%3 != 0 {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "error",
			"error":   "intermittent_failure",
			"attempt": attempt,
		})
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":      "success",
		"received":    true,
		"received_at": time.Now().UTC().Format(time.RFC3339Nano),
		"attempt":     attempt,
	})
}

// handleWebhookPoison returns 400 Bad Request simulating unprocessable payload to push directly to DLQ.
func (s *MockServer) handleWebhookPoison(w http.ResponseWriter, r *http.Request) {
	if _, err := s.captureRequest(r); err != nil {
		http.Error(w, `{"error":"failed to read body"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"error":  "poison_pill_rejected",
		"status": "rejected",
	})
}

// handleWebhookSlow introduces an artificial delay before returning 200 OK.
func (s *MockServer) handleWebhookSlow(w http.ResponseWriter, r *http.Request) {
	if _, err := s.captureRequest(r); err != nil {
		http.Error(w, `{"error":"failed to read body"}`, http.StatusInternalServerError)
		return
	}

	delay := 4 * time.Second
	if dStr := r.URL.Query().Get("delay"); dStr != "" {
		if parsed, err := time.ParseDuration(dStr); err == nil {
			delay = parsed
		} else if parsedMs, err := strconv.Atoi(dStr); err == nil {
			delay = time.Duration(parsedMs) * time.Millisecond
		}
	}

	select {
	case <-time.After(delay):
	case <-r.Context().Done():
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":      "success",
		"slow":        true,
		"received_at": time.Now().UTC().Format(time.RFC3339Nano),
	})
}

// handleInspectLogs returns captured webhook requests.
func (s *MockServer) handleInspectLogs(w http.ResponseWriter, r *http.Request) {
	logs := s.buffer.GetAll()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(logs)
}

// handleInspectClear resets the log buffer.
func (s *MockServer) handleInspectClear(w http.ResponseWriter, r *http.Request) {
	s.buffer.Clear()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status": "cleared",
	})
}

// handleHealth returns 200 OK for health probes.
func (s *MockServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status": "ok",
	})
}

func getPort(flagPort string) string {
	if flagPort != "" && flagPort != "9090" {
		return strings.TrimPrefix(flagPort, ":")
	}
	if envPort := os.Getenv("PORT"); envPort != "" {
		return strings.TrimPrefix(envPort, ":")
	}
	return strings.TrimPrefix(flagPort, ":")
}

func main() {
	portFlag := flag.String("port", "9090", "Port to bind mock receiver server")
	flag.Parse()

	port := getPort(*portFlag)
	server := NewMockServer()

	httpServer := &http.Server{
		Addr:              ":" + port,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		log.Printf("[INFO] Mock Webhook Receiver listening on http://0.0.0.0:%s", port)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("[FATAL] Mock receiver server failed: %v", err)
		}
	}()

	// Graceful shutdown on SIGINT/SIGTERM
	stopSig := make(chan os.Signal, 1)
	signal.Notify(stopSig, syscall.SIGINT, syscall.SIGTERM)
	sig := <-stopSig
	log.Printf("[INFO] Received signal %v. Shutting down mock receiver...", sig)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("[WARN] HTTP server shutdown error: %v", err)
	}

	log.Printf("[INFO] Mock receiver shut down cleanly.")
}
