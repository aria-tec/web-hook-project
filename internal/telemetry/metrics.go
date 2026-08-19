package telemetry

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// DefaultBuckets defines the latency histogram bucket distribution in seconds.
var DefaultBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// Metrics encapsulates Prometheus counters and histograms for the Webhook Reliability Engine.
type Metrics struct {
	registry                 *prometheus.Registry
	eventsIngestedTotal      *prometheus.CounterVec
	eventsDeliveredTotal     *prometheus.CounterVec
	deliveryDurationSeconds  *prometheus.HistogramVec
	dlqEventsTotal           *prometheus.CounterVec
}

// NewMetrics initializes a new Metrics registry with isolated Prometheus collectors.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()

	eventsIngested := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "events_ingested_total",
			Help: "Total number of webhook events ingested by the API.",
		},
		[]string{"tenant_id", "event_type"},
	)

	eventsDelivered := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "events_delivered_total",
			Help: "Total number of webhook events successfully delivered to endpoints.",
		},
		[]string{"tenant_id", "endpoint_id", "status_code"},
	)

	deliveryDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "delivery_duration_seconds",
			Help:    "Histogram of delivery latency in seconds.",
			Buckets: DefaultBuckets,
		},
		[]string{"tenant_id", "endpoint_id"},
	)

	dlqEvents := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dlq_events_total",
			Help: "Total number of webhook events routed to Dead Letter Queue.",
		},
		[]string{"tenant_id", "endpoint_id", "reason"},
	)

	reg.MustRegister(eventsIngested, eventsDelivered, deliveryDuration, dlqEvents)

	return &Metrics{
		registry:                reg,
		eventsIngestedTotal:     eventsIngested,
		eventsDeliveredTotal:    eventsDelivered,
		deliveryDurationSeconds: deliveryDuration,
		dlqEventsTotal:          dlqEvents,
	}
}

// IncIngested increments the counter of ingested events for a tenant and event type.
func (m *Metrics) IncIngested(tenantID, eventType string) {
	if m == nil || m.eventsIngestedTotal == nil {
		return
	}
	m.eventsIngestedTotal.WithLabelValues(tenantID, eventType).Inc()
}

// IncDelivered increments the counter of successfully delivered events.
func (m *Metrics) IncDelivered(tenantID, endpointID, statusCode string) {
	if m == nil || m.eventsDeliveredTotal == nil {
		return
	}
	m.eventsDeliveredTotal.WithLabelValues(tenantID, endpointID, statusCode).Inc()
}

// ObserveDeliveryDuration records the delivery duration in seconds for an endpoint.
func (m *Metrics) ObserveDeliveryDuration(tenantID, endpointID string, durationSeconds float64) {
	if m == nil || m.deliveryDurationSeconds == nil {
		return
	}
	m.deliveryDurationSeconds.WithLabelValues(tenantID, endpointID).Observe(durationSeconds)
}

// IncDLQ increments the DLQ counter with failure reason.
func (m *Metrics) IncDLQ(tenantID, endpointID, reason string) {
	if m == nil || m.dlqEventsTotal == nil {
		return
	}
	m.dlqEventsTotal.WithLabelValues(tenantID, endpointID, reason).Inc()
}

// Registry returns the underlying Prometheus registry.
func (m *Metrics) Registry() *prometheus.Registry {
	return m.registry
}

// Handler returns an http.Handler that serves the Prometheus metrics endpoint.
func (m *Metrics) Handler() http.Handler {
	if m == nil || m.registry == nil {
		return promhttp.Handler()
	}
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}
