package telemetry_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"web-hook-project/internal/telemetry"
)

func TestMetrics_Initialization(t *testing.T) {
	metrics := telemetry.NewMetrics()
	if metrics == nil {
		t.Fatal("expected metrics to initialize")
	}

	metrics.IncIngested("tenant_alpha", "order.created")
	metrics.IncDelivered("tenant_alpha", "ep_001", "200")
	metrics.ObserveDeliveryDuration("tenant_alpha", "ep_001", 0.045)
	metrics.IncDLQ("tenant_alpha", "ep_001", "max_retries_exceeded")

	handler := metrics.Handler()
	if handler == nil {
		t.Fatal("expected HTTP handler from metrics")
	}

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 from /metrics, got %d", rec.Code)
	}

	body := rec.Body.String()

	// Verify metric names and label values in Prometheus format
	if !strings.Contains(body, "events_ingested_total") {
		t.Errorf("expected body to contain events_ingested_total metric, got:\n%s", body)
	}
	if !strings.Contains(body, `tenant_id="tenant_alpha"`) {
		t.Errorf("expected body to contain tenant_alpha label, got:\n%s", body)
	}
	if !strings.Contains(body, `event_type="order.created"`) {
		t.Errorf("expected body to contain event_type label, got:\n%s", body)
	}
	if !strings.Contains(body, "events_delivered_total") {
		t.Errorf("expected body to contain events_delivered_total metric, got:\n%s", body)
	}
	if !strings.Contains(body, `status_code="200"`) {
		t.Errorf("expected body to contain status_code label, got:\n%s", body)
	}
	if !strings.Contains(body, "delivery_duration_seconds") {
		t.Errorf("expected body to contain delivery_duration_seconds metric, got:\n%s", body)
	}
	if !strings.Contains(body, "dlq_events_total") {
		t.Errorf("expected body to contain dlq_events_total metric, got:\n%s", body)
	}
	if !strings.Contains(body, `reason="max_retries_exceeded"`) {
		t.Errorf("expected body to contain reason label, got:\n%s", body)
	}
}

func TestMetrics_MultipleObservations(t *testing.T) {
	metrics := telemetry.NewMetrics()

	for i := 0; i < 5; i++ {
		metrics.IncIngested("tenant_beta", "payment.succeeded")
		metrics.IncDelivered("tenant_beta", "ep_002", "200")
		metrics.ObserveDeliveryDuration("tenant_beta", "ep_002", 0.012)
	}

	for i := 0; i < 2; i++ {
		metrics.IncDLQ("tenant_beta", "ep_002", "connection_refused")
	}

	handler := metrics.Handler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", rec.Code)
	}

	body, err := io.ReadAll(rec.Body)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}

	bodyStr := string(body)
	if !strings.Contains(bodyStr, "tenant_beta") {
		t.Errorf("expected metrics to include tenant_beta, got:\n%s", bodyStr)
	}
	if !strings.Contains(bodyStr, `reason="connection_refused"`) {
		t.Errorf("expected metrics to include connection_refused reason, got:\n%s", bodyStr)
	}
}

func TestMetrics_EmptyAndDefaultLabels(t *testing.T) {
	metrics := telemetry.NewMetrics()

	// Should not panic on empty strings
	metrics.IncIngested("", "")
	metrics.IncDelivered("", "", "")
	metrics.ObserveDeliveryDuration("", "", 0.0)
	metrics.IncDLQ("", "", "")

	handler := metrics.Handler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 with empty labels, got %d", rec.Code)
	}
}

func TestMetrics_ConcurrentAccess(t *testing.T) {
	metrics := telemetry.NewMetrics()
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func(workerID int) {
			for j := 0; j < 50; j++ {
				metrics.IncIngested("tenant_c", "event.batch")
				metrics.IncDelivered("tenant_c", "ep_003", "204")
				metrics.ObserveDeliveryDuration("tenant_c", "ep_003", float64(j)*0.001)
				metrics.IncDLQ("tenant_c", "ep_003", "timeout")
			}
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("concurrent metric updates timed out")
		}
	}
}
