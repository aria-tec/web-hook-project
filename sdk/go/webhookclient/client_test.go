package webhookclient_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"web-hook-project/sdk/go/webhookclient"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func newTestHTTPClient(fn roundTripFunc) *http.Client {
	return &http.Client{
		Transport: fn,
		Timeout:   5 * time.Second,
	}
}

func TestClient_ConstructorsAndOptions(t *testing.T) {
	customHTTPClient := &http.Client{Timeout: 5 * time.Second}

	c1 := webhookclient.New("https://api.example.com", "tenant_alpha",
		webhookclient.WithAPIKey("test_key_123"),
		webhookclient.WithHTTPClient(customHTTPClient),
		webhookclient.WithTimeout(10*time.Second),
	)
	if c1 == nil {
		t.Fatal("expected non-nil client from New")
	}

	c2 := webhookclient.NewClient("https://api.example.com/", "tenant_alpha")
	if c2 == nil {
		t.Fatal("expected non-nil client from NewClient")
	}
}

func TestClient_Publish(t *testing.T) {
	t.Run("successful publish with map payload and options", func(t *testing.T) {
		var capturedHeader http.Header
		var capturedBody map[string]interface{}

		httpClient := newTestHTTPClient(func(req *http.Request) (*http.Response, error) {
			capturedHeader = req.Header.Clone()
			bodyBytes, _ := io.ReadAll(req.Body)
			_ = json.Unmarshal(bodyBytes, &capturedBody)

			respData, _ := json.Marshal(map[string]interface{}{
				"id":         "evt_pub_001",
				"status":     "PENDING",
				"created_at": time.Now().UTC().Format(time.RFC3339Nano),
			})

			return &http.Response{
				StatusCode: http.StatusAccepted,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader(respData)),
			}, nil
		})

		client := webhookclient.New(
			"https://api.minisvix.io",
			"tenant_123",
			webhookclient.WithHTTPClient(httpClient),
			webhookclient.WithAPIKey("sec_key_abc"),
		)
		payload := map[string]interface{}{
			"order_id": "ord_999",
			"amount":   15000,
		}

		res, err := client.Publish(
			context.Background(),
			"order.created",
			payload,
			webhookclient.WithIdempotencyKey("idemp_key_456"),
		)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if res.ID != "evt_pub_001" {
			t.Errorf("expected ID evt_pub_001, got %s", res.ID)
		}
		if res.Status != "PENDING" {
			t.Errorf("expected status PENDING, got %s", res.Status)
		}
		if res.CreatedAt.IsZero() {
			t.Error("expected non-zero CreatedAt")
		}

		if capturedHeader.Get("X-Tenant-ID") != "tenant_123" {
			t.Errorf("expected X-Tenant-ID tenant_123, got %s", capturedHeader.Get("X-Tenant-ID"))
		}
		if capturedHeader.Get("Authorization") != "Bearer sec_key_abc" {
			t.Errorf("expected Authorization Bearer sec_key_abc, got %s", capturedHeader.Get("Authorization"))
		}
		if capturedHeader.Get("X-Idempotency-Key") != "idemp_key_456" {
			t.Errorf("expected X-Idempotency-Key idemp_key_456, got %s", capturedHeader.Get("X-Idempotency-Key"))
		}
		if capturedHeader.Get("Content-Type") != "application/json" {
			t.Errorf("expected Content-Type application/json, got %s", capturedHeader.Get("Content-Type"))
		}

		if capturedBody["event_type"] != "order.created" {
			t.Errorf("expected event_type order.created, got %v", capturedBody["event_type"])
		}
	})

	t.Run("publish with raw JSON bytes payload", func(t *testing.T) {
		var capturedBody map[string]interface{}

		httpClient := newTestHTTPClient(func(req *http.Request) (*http.Response, error) {
			bodyBytes, _ := io.ReadAll(req.Body)
			_ = json.Unmarshal(bodyBytes, &capturedBody)

			respData, _ := json.Marshal(map[string]interface{}{
				"id":         "evt_raw_002",
				"status":     "PENDING",
				"created_at": time.Now().UTC(),
			})

			return &http.Response{
				StatusCode: http.StatusAccepted,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader(respData)),
			}, nil
		})

		client := webhookclient.New("https://api.minisvix.io", "tenant_raw", webhookclient.WithHTTPClient(httpClient))
		rawJSON := []byte(`{"customer_id":"cust_77","tier":"gold"}`)

		res, err := client.Publish(context.Background(), "customer.upgraded", rawJSON)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if res.ID != "evt_raw_002" {
			t.Errorf("expected ID evt_raw_002, got %s", res.ID)
		}

		// Ensure payload wasn't base64 encoded, but preserved as JSON map
		payloadMap, ok := capturedBody["payload"].(map[string]interface{})
		if !ok {
			t.Fatalf("expected payload to decode as map, got %T: %v", capturedBody["payload"], capturedBody["payload"])
		}
		if payloadMap["customer_id"] != "cust_77" {
			t.Errorf("expected customer_id cust_77, got %v", payloadMap["customer_id"])
		}
	})

	t.Run("publish validation errors", func(t *testing.T) {
		client := webhookclient.New("https://api.minisvix.io", "tenant_test")

		_, err := client.Publish(context.Background(), "", map[string]string{"foo": "bar"})
		if err == nil {
			t.Fatal("expected error for empty eventType")
		}
	})

	t.Run("publish server error handling", func(t *testing.T) {
		httpClient := newTestHTTPClient(func(req *http.Request) (*http.Response, error) {
			respData, _ := json.Marshal(map[string]interface{}{
				"error": "duplicate event or idempotency key violation",
			})
			return &http.Response{
				StatusCode: http.StatusConflict,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader(respData)),
			}, nil
		})

		client := webhookclient.New("https://api.minisvix.io", "tenant_err", webhookclient.WithHTTPClient(httpClient))
		_, err := client.Publish(context.Background(), "user.created", map[string]string{"user": "u1"})
		if err == nil {
			t.Fatal("expected error on 409 Conflict")
		}

		var apiErr *webhookclient.APIError
		if errors.As(err, &apiErr) {
			if apiErr.StatusCode != http.StatusConflict {
				t.Errorf("expected status 409, got %d", apiErr.StatusCode)
			}
			if apiErr.Message != "duplicate event or idempotency key violation" {
				t.Errorf("expected error message to match server response, got %q", apiErr.Message)
			}
		} else {
			t.Errorf("expected *webhookclient.APIError, got %T: %v", err, err)
		}
	})

	t.Run("publish context cancellation", func(t *testing.T) {
		httpClient := newTestHTTPClient(func(req *http.Request) (*http.Response, error) {
			select {
			case <-req.Context().Done():
				return nil, req.Context().Err()
			case <-time.After(100 * time.Millisecond):
				return &http.Response{
					StatusCode: http.StatusAccepted,
					Body:       io.NopCloser(bytes.NewReader([]byte(`{"id":"evt_1"}`))),
				}, nil
			}
		})

		client := webhookclient.New("https://api.minisvix.io", "tenant_ctx", webhookclient.WithHTTPClient(httpClient))
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		_, err := client.Publish(ctx, "test.timeout", map[string]string{"k": "v"})
		if err == nil {
			t.Fatal("expected error on context timeout")
		}
	})
}

func TestClient_ListDLQ(t *testing.T) {
	t.Run("successful list with limit and offset query params", func(t *testing.T) {
		var capturedPath string
		var capturedQuery string
		var capturedTenant string

		now := time.Now().UTC().Truncate(time.Second)

		httpClient := newTestHTTPClient(func(req *http.Request) (*http.Response, error) {
			capturedPath = req.URL.Path
			capturedQuery = req.URL.RawQuery
			capturedTenant = req.Header.Get("X-Tenant-ID")

			respData, _ := json.Marshal([]map[string]interface{}{
				{
					"id":              "evt_dlq_1",
					"tenant_id":       "tenant_dlq",
					"event_type":      "payment.failed",
					"idempotency_key": "idemp_1",
					"payload":         map[string]interface{}{"card": "declined"},
					"status":          "DLQ",
					"created_at":      now.Format(time.RFC3339Nano),
				},
				{
					"id":         "evt_dlq_2",
					"tenant_id":  "tenant_dlq",
					"event_type": "refund.failed",
					"payload":    map[string]interface{}{"reason": "insufficient_funds"},
					"status":     "DLQ",
					"created_at": now.Format(time.RFC3339Nano),
				},
			})

			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader(respData)),
			}, nil
		})

		client := webhookclient.New("https://api.minisvix.io", "tenant_dlq", webhookclient.WithHTTPClient(httpClient))
		events, err := client.ListDLQ(context.Background(), 25, 10)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if capturedPath != "/api/v1/dlq" {
			t.Errorf("expected path /api/v1/dlq, got %s", capturedPath)
		}
		if capturedTenant != "tenant_dlq" {
			t.Errorf("expected tenant tenant_dlq, got %s", capturedTenant)
		}
		if capturedQuery != "limit=25&offset=10" && capturedQuery != "offset=10&limit=25" {
			t.Errorf("unexpected query string: %s", capturedQuery)
		}

		if len(events) != 2 {
			t.Fatalf("expected 2 DLQ events, got %d", len(events))
		}
		if events[0].ID != "evt_dlq_1" || events[0].EventType != "payment.failed" {
			t.Errorf("unexpected first event: %+v", events[0])
		}
		if events[0].IdempotencyKey != "idemp_1" {
			t.Errorf("expected idempotency key idemp_1, got %s", events[0].IdempotencyKey)
		}
		if events[1].ID != "evt_dlq_2" || events[1].EventType != "refund.failed" {
			t.Errorf("unexpected second event: %+v", events[1])
		}
	})

	t.Run("list DLQ server error", func(t *testing.T) {
		httpClient := newTestHTTPClient(func(req *http.Request) (*http.Response, error) {
			respData, _ := json.Marshal(map[string]string{"error": "database down"})
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader(respData)),
			}, nil
		})

		client := webhookclient.New("https://api.minisvix.io", "tenant_err", webhookclient.WithHTTPClient(httpClient))
		_, err := client.ListDLQ(context.Background(), 10, 0)
		if err == nil {
			t.Fatal("expected error on 500 Internal Server Error")
		}
	})
}

func TestClient_ReplayDLQ(t *testing.T) {
	t.Run("successful replay", func(t *testing.T) {
		var capturedBody map[string]interface{}
		var capturedTenant string

		httpClient := newTestHTTPClient(func(req *http.Request) (*http.Response, error) {
			capturedTenant = req.Header.Get("X-Tenant-ID")
			bodyBytes, _ := io.ReadAll(req.Body)
			_ = json.Unmarshal(bodyBytes, &capturedBody)

			respData, _ := json.Marshal(map[string]interface{}{
				"status":         "QUEUED_FOR_RETRY",
				"replayed_count": 3,
			})

			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader(respData)),
			}, nil
		})

		client := webhookclient.New("https://api.minisvix.io", "tenant_replay", webhookclient.WithHTTPClient(httpClient))
		res, err := client.ReplayDLQ(context.Background(), []string{"evt_1", "evt_2", "evt_3"})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if res.Status != "QUEUED_FOR_RETRY" {
			t.Errorf("expected status QUEUED_FOR_RETRY, got %s", res.Status)
		}
		if res.ReplayedCount != 3 {
			t.Errorf("expected replayedCount 3, got %d", res.ReplayedCount)
		}
		if capturedTenant != "tenant_replay" {
			t.Errorf("expected tenant tenant_replay, got %s", capturedTenant)
		}

		eventIDs, ok := capturedBody["event_ids"].([]interface{})
		if !ok || len(eventIDs) != 3 {
			t.Fatalf("expected 3 event_ids in request body, got %+v", capturedBody["event_ids"])
		}
	})

	t.Run("replay with empty eventIDs validation", func(t *testing.T) {
		client := webhookclient.New("https://api.minisvix.io", "tenant_test")
		_, err := client.ReplayDLQ(context.Background(), []string{})
		if err == nil {
			t.Fatal("expected error when eventIDs is empty")
		}
	})

	t.Run("replay server error", func(t *testing.T) {
		httpClient := newTestHTTPClient(func(req *http.Request) (*http.Response, error) {
			respData, _ := json.Marshal(map[string]string{"error": "event_ids batch size cannot exceed 100"})
			return &http.Response{
				StatusCode: http.StatusBadRequest,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader(respData)),
			}, nil
		})

		client := webhookclient.New("https://api.minisvix.io", "tenant_err", webhookclient.WithHTTPClient(httpClient))
		_, err := client.ReplayDLQ(context.Background(), []string{"evt_1"})
		if err == nil {
			t.Fatal("expected error on 400 Bad Request")
		}
	})
}

func TestClient_Concurrency(t *testing.T) {
	var publishCalls int64
	var listCalls int64
	var replayCalls int64

	httpClient := newTestHTTPClient(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/api/v1/events":
			atomic.AddInt64(&publishCalls, 1)
			respData, _ := json.Marshal(map[string]interface{}{
				"id":         "evt_conc",
				"status":     "PENDING",
				"created_at": time.Now().UTC(),
			})
			return &http.Response{
				StatusCode: http.StatusAccepted,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader(respData)),
			}, nil

		case "/api/v1/dlq":
			atomic.AddInt64(&listCalls, 1)
			respData, _ := json.Marshal([]map[string]interface{}{})
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader(respData)),
			}, nil

		case "/api/v1/dlq/replay":
			atomic.AddInt64(&replayCalls, 1)
			respData, _ := json.Marshal(map[string]interface{}{
				"status":         "QUEUED_FOR_RETRY",
				"replayed_count": 1,
			})
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader(respData)),
			}, nil

		default:
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(bytes.NewReader([]byte(`{"error":"not found"}`))),
			}, nil
		}
	})

	client := webhookclient.New("https://api.minisvix.io", "tenant_concurrent",
		webhookclient.WithHTTPClient(httpClient),
		webhookclient.WithAPIKey("concurrent_api_key"),
		webhookclient.WithTimeout(5*time.Second),
	)

	var wg sync.WaitGroup
	numWorkers := 50

	for i := 0; i < numWorkers; i++ {
		wg.Add(3)

		// 1. Publish goroutine
		go func(idx int) {
			defer wg.Done()
			_, err := client.Publish(
				context.Background(),
				"concurrent.event",
				map[string]int{"idx": idx},
				webhookclient.WithIdempotencyKey("idemp_conc"),
			)
			if err != nil {
				t.Errorf("concurrent publish %d failed: %v", idx, err)
			}
		}(i)

		// 2. ListDLQ goroutine
		go func(idx int) {
			defer wg.Done()
			_, err := client.ListDLQ(context.Background(), 10, 0)
			if err != nil {
				t.Errorf("concurrent listDLQ %d failed: %v", idx, err)
			}
		}(i)

		// 3. ReplayDLQ goroutine
		go func(idx int) {
			defer wg.Done()
			_, err := client.ReplayDLQ(context.Background(), []string{"evt_conc_1"})
			if err != nil {
				t.Errorf("concurrent replayDLQ %d failed: %v", idx, err)
			}
		}(i)
	}

	wg.Wait()

	if atomic.LoadInt64(&publishCalls) != int64(numWorkers) {
		t.Errorf("expected %d publish calls, got %d", numWorkers, publishCalls)
	}
	if atomic.LoadInt64(&listCalls) != int64(numWorkers) {
		t.Errorf("expected %d list calls, got %d", numWorkers, listCalls)
	}
	if atomic.LoadInt64(&replayCalls) != int64(numWorkers) {
		t.Errorf("expected %d replay calls, got %d", numWorkers, replayCalls)
	}
}
