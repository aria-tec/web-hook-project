package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"web-hook-project/internal/domain"
)

const (
	DefaultPingInterval = 15 * time.Second
	DefaultClientBufferSize = 64
)

// SSEBroker manages connected Server-Sent Events (SSE) clients and broadcasts
// real-time webhook delivery attempts.
type SSEBroker struct {
	mu           sync.RWMutex
	clients      map[chan *domain.DeliveryAttempt]struct{}
	pingInterval time.Duration
}

// NewSSEBroker constructs a new SSEBroker with the default 15-second keep-alive ping interval.
func NewSSEBroker() *SSEBroker {
	return &SSEBroker{
		clients:      make(map[chan *domain.DeliveryAttempt]struct{}),
		pingInterval: DefaultPingInterval,
	}
}

// NewSSEBrokerWithPingInterval constructs a new SSEBroker with a custom keep-alive ping interval.
func NewSSEBrokerWithPingInterval(interval time.Duration) *SSEBroker {
	if interval <= 0 {
		interval = DefaultPingInterval
	}
	return &SSEBroker{
		clients:      make(map[chan *domain.DeliveryAttempt]struct{}),
		pingInterval: interval,
	}
}

// RegisterClient registers a new client channel to receive broadcasts.
func (b *SSEBroker) RegisterClient(ch chan *domain.DeliveryAttempt) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.clients[ch] = struct{}{}
}

// UnregisterClient removes a client channel from the active broadcast pool.
func (b *SSEBroker) UnregisterClient(ch chan *domain.DeliveryAttempt) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.clients, ch)
}

// ClientCount returns the number of currently connected SSE clients.
func (b *SSEBroker) ClientCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.clients)
}

// Broadcast dispatches a delivery attempt to all connected SSE clients in a non-blocking manner.
func (b *SSEBroker) Broadcast(attempt *domain.DeliveryAttempt) {
	if attempt == nil {
		return
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	for ch := range b.clients {
		select {
		case ch <- attempt:
		default:
			// Buffer full on slow client; drop message to prevent stalling workers/dispatcher
		}
	}
}

// ServeHTTP handles incoming HTTP requests for the SSE stream endpoint (/api/v1/events/stream).
func (b *SSEBroker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	clientChan := make(chan *domain.DeliveryAttempt, DefaultClientBufferSize)
	b.RegisterClient(clientChan)
	defer b.UnregisterClient(clientChan)

	ticker := time.NewTicker(b.pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			if _, err := fmt.Fprintf(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case attempt, ok := <-clientChan:
			if !ok {
				return
			}
			data, err := json.Marshal(attempt)
			if err != nil {
				continue
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}
