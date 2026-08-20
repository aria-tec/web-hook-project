package chaos_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"web-hook-project/internal/api"
	"web-hook-project/internal/dispatcher"
	"web-hook-project/internal/domain"
	"web-hook-project/internal/idempotency"
	"web-hook-project/internal/outbox"
	"web-hook-project/internal/queue"
	"web-hook-project/internal/retry"
	"web-hook-project/internal/storage"
	"web-hook-project/internal/telemetry"
	"web-hook-project/internal/worker"
)

// ChaosTestHarness wires an isolated end-to-end engine stack for chaos simulation.
type ChaosTestHarness struct {
	Repo        storage.Repository
	Queue       queue.StreamQueue
	Guard       idempotency.Guard
	Dispatcher  *dispatcher.Dispatcher
	WorkerPool  *worker.WorkerPool
	Relay       *outbox.Relay
	Metrics     *telemetry.Metrics
	Server      *httptest.Server
	StreamName  string
	GroupName   string
	WorkerCount int
}

func newChaosHarness(t *testing.T, customDestClient *http.Client, workerCfgModifier ...func(*worker.Config)) (*ChaosTestHarness, func()) {
	repo := storage.NewMemoryRepository()
	q := queue.NewMemoryStreamQueue()
	guard := idempotency.NewMemoryGuard()
	metrics := telemetry.NewMetrics()

	streamName := fmt.Sprintf("stream:chaos:%d", time.Now().UnixNano())
	groupName := "chaos-workers"

	if customDestClient == nil {
		customDestClient = &http.Client{Timeout: 5 * time.Second}
	}

	backoffPolicy := retry.BackoffPolicy{
		InitialInterval: 20 * time.Millisecond,
		MaxInterval:     200 * time.Millisecond,
		Multiplier:      2.0,
		MaxRetries:      4,
	}
	disp := dispatcher.NewDispatcher(customDestClient, repo, backoffPolicy).WithMetrics(metrics)

	wCfg := worker.Config{
		NumWorkers:       4,
		StreamName:       streamName,
		GroupName:        groupName,
		BatchSize:        10,
		PollInterval:     10 * time.Millisecond,
		MinIdleDuration:  40 * time.Millisecond,
		ClaimInterval:    40 * time.Millisecond,
		MaxClaimAttempts: 4,
	}
	for _, mod := range workerCfgModifier {
		mod(&wCfg)
	}

	workerPool := worker.NewWorkerPool(wCfg, q, repo, disp)
	relay := outbox.NewRelay(repo, q, streamName)

	handler := api.NewHandler(repo, guard).WithMetrics(metrics)
	router := api.NewRouter(handler, metrics)
	ts := httptest.NewServer(router)

	ctx, cancel := context.WithCancel(context.Background())

	_ = workerPool.Start(ctx)

	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		_ = relay.Start(ctx, 10*time.Millisecond, 20)
	}()

	harness := &ChaosTestHarness{
		Repo:        repo,
		Queue:       q,
		Guard:       guard,
		Dispatcher:  disp,
		WorkerPool:  workerPool,
		Relay:       relay,
		Metrics:     metrics,
		Server:      ts,
		StreamName:  streamName,
		GroupName:   groupName,
		WorkerCount: wCfg.NumWorkers,
	}

	cleanup := func() {
		cancel()
		<-relayDone
		workerPool.Stop()
		ts.Close()
	}

	return harness, cleanup
}

// Skenario 1: Broker Flakiness & Outbox Zero-Loss Recovery under Load
func TestChaos_Scenario1_OutboxBrokerDisruption_ZeroLoss(t *testing.T) {
	var deliveredCount atomic.Int64

	destServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deliveredCount.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"received":true}`))
	}))
	defer destServer.Close()

	harness, cleanup := newChaosHarness(t, destServer.Client())
	defer cleanup()

	ctx := context.Background()
	tenantID := "tenant_chaos_s1"
	_ = harness.Repo.CreateTenant(ctx, &domain.Tenant{ID: tenantID, Name: "Chaos S1 Tenant"})
	_ = harness.Repo.CreateEndpoint(ctx, &domain.Endpoint{
		ID:        "ep_chaos_s1",
		TenantID:  tenantID,
		URL:       destServer.URL,
		Secret:    "whsec_chaos_1",
		RateLimit: 500,
		IsActive:  true,
	})

	const totalEvents = 100
	var wg sync.WaitGroup
	wg.Add(totalEvents)

	// Send 100 events concurrently via Ingestion API
	for i := 0; i < totalEvents; i++ {
		go func(idx int) {
			defer wg.Done()
			payload := fmt.Sprintf(`{"event_type":"order.placed","payload":{"order_id":%d}}`, idx)
			req, _ := http.NewRequest(http.MethodPost, harness.Server.URL+"/api/v1/events", bytes.NewBufferString(payload))
			req.Header.Set("X-Tenant-ID", tenantID)
			req.Header.Set("X-Idempotency-Key", fmt.Sprintf("idemp_s1_%d", idx))
			req.Header.Set("Content-Type", "application/json")

			resp, err := http.DefaultClient.Do(req)
			if err != nil || resp.StatusCode != http.StatusAccepted {
				t.Errorf("event %d ingestion failed: %v", idx, err)
			}
			if resp != nil {
				_ = resp.Body.Close()
			}
		}(i)
	}

	wg.Wait()

	// Wait for all 100 events to be dispatched and acknowledged
	deadline := time.Now().Add(6 * time.Second)
	for deliveredCount.Load() < totalEvents && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	gotDelivered := deliveredCount.Load()
	if gotDelivered != totalEvents {
		t.Fatalf("ZERO LOSS VIOLATION: expected %d delivered events, got %d", totalEvents, gotDelivered)
	}

	pendingOutbox, err := harness.Repo.FetchAndLockPendingOutbox(ctx, 10)
	if err != nil || len(pendingOutbox) != 0 {
		t.Fatalf("expected 0 pending outbox records remaining, got %d", len(pendingOutbox))
	}
}

// Skenario 2: Worker In-Flight Crash & PEL Auto-Claim Recovery
func TestChaos_Scenario2_WorkerCrash_PELAutoClaim(t *testing.T) {
	var deliveryCount atomic.Int64

	destServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deliveryCount.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"received":true}`))
	}))
	defer destServer.Close()

	repo := storage.NewMemoryRepository()
	q := queue.NewMemoryStreamQueue()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tenantID := "tenant_chaos_s2"
	_ = repo.CreateTenant(ctx, &domain.Tenant{ID: tenantID, Name: "Chaos S2 Tenant"})
	_ = repo.CreateEndpoint(ctx, &domain.Endpoint{
		ID:        "ep_chaos_s2",
		TenantID:  tenantID,
		URL:       destServer.URL,
		Secret:    "whsec_s2",
		RateLimit: 100,
		IsActive:  true,
	})

	streamName := "stream:chaos:s2"
	groupName := "chaos-s2-group"
	_ = q.CreateConsumerGroup(ctx, streamName, groupName, "0")

	// Publish 10 events
	const numEvents = 10
	for i := 0; i < numEvents; i++ {
		evt := &domain.Event{
			ID:        fmt.Sprintf("evt_s2_%d", i),
			TenantID:  tenantID,
			EventType: "worker.crash_test",
			Payload:   []byte(`{"test":"pel"}`),
			Status:    domain.EventStatusPending,
			CreatedAt: time.Now(),
		}
		_ = repo.CreateEventWithOutbox(ctx, evt, &domain.OutboxEvent{EventID: evt.ID, Status: domain.OutboxStatusPending})
		_, _ = q.PublishEvent(ctx, streamName, evt)
	}

	// Simulate Worker 1 reading all 10 messages from Redis Streams, then crashing immediately without ACK
	crashedMessages, err := q.ReadEvents(ctx, streamName, groupName, "worker-doomed", numEvents, 0)
	if err != nil || len(crashedMessages) != numEvents {
		t.Fatalf("failed to simulate crashed worker read: %v", err)
	}
	// "worker-doomed" is killed now: no AckEvent called!

	// Start a fresh WorkerPool with PEL auto-claim configured
	disp := dispatcher.NewDispatcher(destServer.Client(), repo, retry.DefaultBackoffPolicy())
	pool := worker.NewWorkerPool(worker.Config{
		NumWorkers:       2,
		StreamName:       streamName,
		GroupName:        groupName,
		MinIdleDuration:  30 * time.Millisecond,
		ClaimInterval:    30 * time.Millisecond,
		MaxClaimAttempts: 4,
	}, q, repo, disp)

	_ = pool.Start(ctx)
	defer pool.Stop()

	// Wait for PEL reclaimer to reclaim all 10 messages and deliver them
	deadline := time.Now().Add(4 * time.Second)
	for deliveryCount.Load() < int64(numEvents) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	if got := deliveryCount.Load(); got != int64(numEvents) {
		t.Fatalf("PEL Auto-Claim failed: expected %d delivered events, got %d", numEvents, got)
	}
}

// Skenario 3: Destination Chaos (503s Flakiness, Poison Pill 400 to DLQ, and Guarded Replay)
func TestChaos_Scenario3_DestinationChaos_PoisonPillDLQ_And_Replay(t *testing.T) {
	var reqAttempts atomic.Int64

	// Endpoint that fails with 503 for first 2 attempts, then succeeds on 3rd attempt
	flakyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := reqAttempts.Add(1)
		if attempt <= 2 {
			http.Error(w, "temporary upstream overload", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"recovered":true}`))
	}))
	defer flakyServer.Close()

	// Endpoint that returns 400 Bad Request (Poison Pill)
	poisonServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "invalid webhook schema", http.StatusBadRequest)
	}))
	defer poisonServer.Close()

	harness, cleanup := newChaosHarness(t, http.DefaultClient)
	defer cleanup()

	ctx := context.Background()
	tenantID := "tenant_chaos_s3"
	_ = harness.Repo.CreateTenant(ctx, &domain.Tenant{ID: tenantID, Name: "Chaos S3 Tenant"})

	// Register flaky endpoint
	_ = harness.Repo.CreateEndpoint(ctx, &domain.Endpoint{
		ID:        "ep_flaky",
		TenantID:  tenantID,
		URL:       flakyServer.URL,
		Secret:    "whsec_flaky",
		RateLimit: 100,
		IsActive:  true,
	})

	// 1. Ingest event to flaky endpoint
	flakyPayload := `{"event_type":"flaky.event","payload":{"item":"flaky_1"}}`
	req1, _ := http.NewRequest(http.MethodPost, harness.Server.URL+"/api/v1/events", bytes.NewBufferString(flakyPayload))
	req1.Header.Set("X-Tenant-ID", tenantID)
	req1.Header.Set("X-Idempotency-Key", "idemp_flaky_1")
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil || resp1.StatusCode != http.StatusAccepted {
		t.Fatalf("ingest flaky event failed: %v", err)
	}
	var resBody1 map[string]interface{}
	_ = json.NewDecoder(resp1.Body).Decode(&resBody1)
	_ = resp1.Body.Close()
	flakyEventID := resBody1["id"].(string)

	// Wait for retries to resolve flaky event to DELIVERED
	deadline := time.Now().Add(4 * time.Second)
	var flakyEvt *domain.Event
	for time.Now().Before(deadline) {
		flakyEvt, _ = harness.Repo.GetEvent(ctx, flakyEventID)
		if flakyEvt != nil && flakyEvt.Status == domain.EventStatusDelivered {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if flakyEvt == nil || flakyEvt.Status != domain.EventStatusDelivered {
		t.Fatalf("expected flaky event to eventually succeed (DELIVERED), got %+v", flakyEvt)
	}

	// 2. Register poison pill endpoint and send poison event
	poisonTenantID := "tenant_chaos_poison"
	_ = harness.Repo.CreateTenant(ctx, &domain.Tenant{ID: poisonTenantID, Name: "Chaos Poison Tenant"})
	_ = harness.Repo.CreateEndpoint(ctx, &domain.Endpoint{
		ID:        "ep_poison",
		TenantID:  poisonTenantID,
		URL:       poisonServer.URL,
		Secret:    "whsec_poison",
		RateLimit: 100,
		IsActive:  true,
	})

	poisonPayload := `{"event_type":"poison.event","payload":{"bad":"schema"}}`
	req2, _ := http.NewRequest(http.MethodPost, harness.Server.URL+"/api/v1/events", bytes.NewBufferString(poisonPayload))
	req2.Header.Set("X-Tenant-ID", poisonTenantID)
	req2.Header.Set("X-Idempotency-Key", "idemp_poison_1")
	resp2, _ := http.DefaultClient.Do(req2)
	var resBody2 map[string]interface{}
	_ = json.NewDecoder(resp2.Body).Decode(&resBody2)
	_ = resp2.Body.Close()
	poisonEventID := resBody2["id"].(string)

	// Verify poison event is isolated to DLQ without crashing workers
	deadline = time.Now().Add(3 * time.Second)
	var poisonEvt *domain.Event
	for time.Now().Before(deadline) {
		poisonEvt, _ = harness.Repo.GetEvent(ctx, poisonEventID)
		if poisonEvt != nil && poisonEvt.Status == domain.EventStatusDLQ {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if poisonEvt == nil || poisonEvt.Status != domain.EventStatusDLQ {
		t.Fatalf("expected poison event to be routed immediately to DLQ, got %+v", poisonEvt)
	}

	// 3. Replay DLQ event via API
	replayPayload := fmt.Sprintf(`{"event_ids":["%s"]}`, poisonEventID)
	reqReplay, _ := http.NewRequest(http.MethodPost, harness.Server.URL+"/api/v1/dlq/replay", bytes.NewBufferString(replayPayload))
	reqReplay.Header.Set("X-Tenant-ID", poisonTenantID)
	respReplay, err := http.DefaultClient.Do(reqReplay)
	if err != nil || respReplay.StatusCode != http.StatusOK {
		t.Fatalf("DLQ replay request failed: %v", err)
	}
	_ = respReplay.Body.Close()

	// Verify event was re-queued (status transitioned to PENDING)
	replayedEvt, _ := harness.Repo.GetEvent(ctx, poisonEventID)
	if replayedEvt == nil || (replayedEvt.Status != domain.EventStatusPending && replayedEvt.Status != domain.EventStatusDLQ) {
		t.Fatalf("expected replayed event status to be PENDING, got %+v", replayedEvt)
	}
}

// Skenario 4: Concurrent Idempotency Storm Under Network Flakiness
func TestChaos_Scenario4_ConcurrentIdempotencyStorm(t *testing.T) {
	var deliveryCount atomic.Int64

	destServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deliveryCount.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"received":true}`))
	}))
	defer destServer.Close()

	harness, cleanup := newChaosHarness(t, destServer.Client())
	defer cleanup()

	ctx := context.Background()
	tenantID := "tenant_chaos_s4"
	_ = harness.Repo.CreateTenant(ctx, &domain.Tenant{ID: tenantID, Name: "Chaos S4 Tenant"})
	_ = harness.Repo.CreateEndpoint(ctx, &domain.Endpoint{
		ID:        "ep_chaos_s4",
		TenantID:  tenantID,
		URL:       destServer.URL,
		Secret:    "whsec_s4",
		RateLimit: 500,
		IsActive:  true,
	})

	const numKeys = 10
	const burstPerKey = 10
	totalRequests := numKeys * burstPerKey

	var wg sync.WaitGroup
	wg.Add(totalRequests)

	var acceptedResponses atomic.Int64
	var replayResponses atomic.Int64
	var conflictResponses atomic.Int64

	for keyIdx := 0; keyIdx < numKeys; keyIdx++ {
		for burst := 0; burst < burstPerKey; burst++ {
			go func(k, b int) {
				defer wg.Done()
				idempKey := fmt.Sprintf("idemp_storm_%d", k)
				payload := fmt.Sprintf(`{"event_type":"storm.test","payload":{"key":%d,"burst":%d}}`, k, b)

				req, _ := http.NewRequest(http.MethodPost, harness.Server.URL+"/api/v1/events", bytes.NewBufferString(payload))
				req.Header.Set("X-Tenant-ID", tenantID)
				req.Header.Set("X-Idempotency-Key", idempKey)
				req.Header.Set("Content-Type", "application/json")

				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return
				}
				defer resp.Body.Close()

				if resp.StatusCode == http.StatusAccepted {
					if resp.Header.Get("X-Idempotency-Replay") == "true" {
						replayResponses.Add(1)
					} else {
						acceptedResponses.Add(1)
					}
				} else if resp.StatusCode == http.StatusConflict {
					conflictResponses.Add(1)
				}
			}(keyIdx, burst)
		}
	}

	wg.Wait()

	// Wait for all unique events to be delivered
	deadline := time.Now().Add(5 * time.Second)
	for deliveryCount.Load() < int64(numKeys) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	// Mathematical Invariant: Exactly numKeys (10) unique deliveries must occur
	if got := deliveryCount.Load(); got != int64(numKeys) {
		t.Fatalf("CONCURRENCY IDEMPOTENCY VIOLATION: expected exactly %d deliveries, got %d", numKeys, got)
	}
}
