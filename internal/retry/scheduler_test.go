package retry_test

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	"web-hook-project/internal/retry"
)

func TestDefaultBackoffPolicy(t *testing.T) {
	policy := retry.DefaultBackoffPolicy()

	if policy.InitialInterval != 5*time.Second {
		t.Errorf("expected InitialInterval 5s, got %v", policy.InitialInterval)
	}
	if policy.MaxInterval != 1*time.Hour {
		t.Errorf("expected MaxInterval 1h, got %v", policy.MaxInterval)
	}
	if policy.Multiplier != 2.0 {
		t.Errorf("expected Multiplier 2.0, got %f", policy.Multiplier)
	}
	if policy.MaxRetries != 5 {
		t.Errorf("expected MaxRetries 5, got %d", policy.MaxRetries)
	}
}

func TestCalculateBackoff_Bounds(t *testing.T) {
	policy := retry.DefaultBackoffPolicy()

	for attempt := 1; attempt <= 10; attempt++ {
		for i := 0; i < 50; i++ {
			d := retry.CalculateBackoff(attempt, policy)
			if d < 0 {
				t.Fatalf("attempt %d: backoff duration cannot be negative: %v", attempt, d)
			}
			if d > policy.MaxInterval {
				t.Fatalf("attempt %d: backoff duration %v exceeded MaxInterval %v", attempt, d, policy.MaxInterval)
			}
		}
	}
}

func TestCalculateBackoff_DeterministicWithRand(t *testing.T) {
	// With RandFunc returning upper bound (n), we test the exact exponential curve
	policy := retry.BackoffPolicy{
		InitialInterval: 1 * time.Second,
		MaxInterval:     10 * time.Second,
		Multiplier:      2.0,
		MaxRetries:      5,
		RandFunc: func(n int64) int64 {
			return n
		},
	}

	expected := []time.Duration{
		1 * time.Second,  // attempt 1: 1 * 2^0 = 1s
		2 * time.Second,  // attempt 2: 1 * 2^1 = 2s
		4 * time.Second,  // attempt 3: 1 * 2^2 = 4s
		8 * time.Second,  // attempt 4: 1 * 2^3 = 8s
		10 * time.Second, // attempt 5: min(10s, 1 * 2^4 = 16s) = 10s
		10 * time.Second, // attempt 6: min(10s, 32s) = 10s
	}

	for i, exp := range expected {
		attempt := i + 1
		got := retry.CalculateBackoff(attempt, policy)
		if got != exp {
			t.Errorf("attempt %d: expected backoff %v, got %v", attempt, exp, got)
		}
	}
}

func TestCalculateBackoff_EdgeCases(t *testing.T) {
	policy := retry.DefaultBackoffPolicy()

	// Zero or negative attempt should be treated as attempt 1
	d0 := retry.CalculateBackoff(0, policy)
	dNeg := retry.CalculateBackoff(-5, policy)

	if d0 < 0 || d0 > policy.InitialInterval {
		t.Errorf("expected attempt 0 backoff <= InitialInterval, got %v", d0)
	}
	if dNeg < 0 || dNeg > policy.InitialInterval {
		t.Errorf("expected attempt -5 backoff <= InitialInterval, got %v", dNeg)
	}
}

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		err        error
		expected   bool
	}{
		// Non-retryable HTTP 2xx
		{"200 OK", 200, nil, false},
		{"201 Created", 201, nil, false},
		{"204 No Content", 204, nil, false},

		// Non-retryable HTTP 3xx
		{"301 Moved Permanently", 301, nil, false},
		{"302 Found", 302, nil, false},

		// Non-retryable HTTP 4xx (client errors)
		{"400 Bad Request", 400, nil, false},
		{"401 Unauthorized", 401, nil, false},
		{"403 Forbidden", 403, nil, false},
		{"404 Not Found", 404, nil, false},
		{"405 Method Not Allowed", 405, nil, false},
		{"422 Unprocessable Entity", 422, nil, false},

		// Retryable HTTP 4xx
		{"408 Request Timeout", 408, nil, true},
		{"429 Too Many Requests", 429, nil, true},

		// Retryable HTTP 5xx (server errors)
		{"500 Internal Server Error", 500, nil, true},
		{"502 Bad Gateway", 502, nil, true},
		{"503 Service Unavailable", 503, nil, true},
		{"504 Gateway Timeout", 504, nil, true},
		{"521 Web Server Is Down", 521, nil, true},

		// Retryable Network and Context errors
		{"Context Deadline Exceeded", 0, context.DeadlineExceeded, true},
		{"Context Canceled", 0, context.Canceled, true},
		{"Network Connection Refused", 0, &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, true},
		{"Generic Error", 0, errors.New("connection reset by peer"), true},
		{"500 with Error", 500, errors.New("http: server closed idle conn"), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := retry.IsRetryable(tt.statusCode, tt.err)
			if got != tt.expected {
				t.Errorf("IsRetryable(%d, %v) = %v; expected %v", tt.statusCode, tt.err, got, tt.expected)
			}
		})
	}
}
