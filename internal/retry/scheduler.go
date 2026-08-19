package retry

import (
	"math"
	"math/rand/v2"
	"time"
)

// BackoffPolicy defines the retry and exponential backoff configuration.
type BackoffPolicy struct {
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
	MaxRetries      int
	RandFunc        func(n int64) int64 // Optional custom rand generator for deterministic testing
}

// DefaultBackoffPolicy returns the standard production retry policy:
// InitialInterval: 5s, MaxInterval: 1h, Multiplier: 2.0, MaxRetries: 5.
func DefaultBackoffPolicy() BackoffPolicy {
	return BackoffPolicy{
		InitialInterval: 5 * time.Second,
		MaxInterval:     1 * time.Hour,
		Multiplier:      2.0,
		MaxRetries:      5,
	}
}

// CalculateBackoff calculates full jitter exponential backoff duration:
// T = random(0, min(MaxInterval, InitialInterval * Multiplier^(attempt - 1)))
func CalculateBackoff(attempt int, policy BackoffPolicy) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}

	initial := policy.InitialInterval
	if initial <= 0 {
		initial = 5 * time.Second
	}

	maxInterval := policy.MaxInterval
	if maxInterval <= 0 {
		maxInterval = 1 * time.Hour
	}

	multiplier := policy.Multiplier
	if multiplier <= 1.0 {
		multiplier = 2.0
	}

	capFloat := float64(initial) * math.Pow(multiplier, float64(attempt-1))
	if capFloat > float64(maxInterval) {
		capFloat = float64(maxInterval)
	}

	capDuration := int64(capFloat)
	if capDuration <= 0 {
		return 0
	}

	if policy.RandFunc != nil {
		return time.Duration(policy.RandFunc(capDuration))
	}

	// Full Jitter: random duration uniformly chosen in [0, capDuration]
	return time.Duration(rand.Int64N(capDuration + 1))
}

// IsRetryable checks if a delivery failure qualifies for retry according to HTTP status code and network errors.
// Returns true for HTTP 408 (Request Timeout), 429 (Too Many Requests), 5xx server errors,
// and all network/context errors (err != nil).
// Returns false for HTTP 2xx, 3xx, and 4xx client errors (except 408 and 429).
func IsRetryable(statusCode int, err error) bool {
	if err != nil {
		return true
	}

	// Retryable 4xx
	if statusCode == 408 || statusCode == 429 {
		return true
	}

	// Retryable 5xx
	if statusCode >= 500 && statusCode < 600 {
		return true
	}

	return false
}
