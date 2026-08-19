package idempotency_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"web-hook-project/internal/idempotency"
)

func TestMemoryGuard_AcquireLock_Single(t *testing.T) {
	guard := idempotency.NewMemoryGuard()
	ctx := context.Background()

	acquired, cached, err := guard.AcquireLock(ctx, "tenant_1", "key_1", 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !acquired {
		t.Fatalf("expected lock to be acquired")
	}
	if cached != nil {
		t.Fatalf("expected cached response to be nil")
	}
}

func TestMemoryGuard_AcquireLock_Concurrent(t *testing.T) {
	guard := idempotency.NewMemoryGuard()
	ctx := context.Background()

	var wg sync.WaitGroup
	var acquiredCount int64
	numWorkers := 20

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			acquired, cached, err := guard.AcquireLock(ctx, "tenant_concurrent", "idemp_same_key", 5*time.Second)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if acquired {
				atomic.AddInt64(&acquiredCount, 1)
			}
			if cached != nil {
				t.Errorf("expected cached to be nil during in-flight concurrency")
			}
		}()
	}
	wg.Wait()

	if acquiredCount != 1 {
		t.Fatalf("expected exactly 1 goroutine to acquire lock, got %d", acquiredCount)
	}
}

func TestMemoryGuard_SetResponse_And_Replay(t *testing.T) {
	guard := idempotency.NewMemoryGuard()
	ctx := context.Background()

	tenant := "tenant_abc"
	key := "key_xyz"

	// 1. Acquire lock
	acquired, cached, err := guard.AcquireLock(ctx, tenant, key, 5*time.Second)
	if err != nil || !acquired || cached != nil {
		t.Fatalf("initial acquire failed: acquired=%v, cached=%v, err=%v", acquired, cached, err)
	}

	// 2. Set response
	responseBody := []byte(`{"id":"evt_123","status":"PENDING"}`)
	err = guard.SetResponse(ctx, tenant, key, 202, responseBody, 5*time.Second)
	if err != nil {
		t.Fatalf("set response failed: %v", err)
	}

	// 3. Acquire again (should return cached response)
	acquired2, cached2, err2 := guard.AcquireLock(ctx, tenant, key, 5*time.Second)
	if err2 != nil {
		t.Fatalf("second acquire error: %v", err2)
	}
	if acquired2 {
		t.Fatalf("expected acquired to be false for cached key")
	}
	if cached2 == nil {
		t.Fatalf("expected cached response to be returned")
	}
	if cached2.StatusCode != 202 {
		t.Errorf("expected status code 202, got %d", cached2.StatusCode)
	}
	if string(cached2.Body) != string(responseBody) {
		t.Errorf("expected body %s, got %s", string(responseBody), string(cached2.Body))
	}
	if cached2.CompletedAt.IsZero() {
		t.Errorf("expected non-zero CompletedAt")
	}
}

func TestMemoryGuard_ReleaseLock(t *testing.T) {
	guard := idempotency.NewMemoryGuard()
	ctx := context.Background()

	tenant := "tenant_rel"
	key := "key_rel"

	// Acquire lock
	acquired, _, err := guard.AcquireLock(ctx, tenant, key, 5*time.Second)
	if err != nil || !acquired {
		t.Fatalf("acquire lock failed: %v", err)
	}

	// Release lock (e.g. after failure)
	err = guard.ReleaseLock(ctx, tenant, key)
	if err != nil {
		t.Fatalf("release lock failed: %v", err)
	}

	// Re-acquire lock (should succeed now)
	acquired2, cached2, err2 := guard.AcquireLock(ctx, tenant, key, 5*time.Second)
	if err2 != nil {
		t.Fatalf("re-acquire lock failed: %v", err2)
	}
	if !acquired2 {
		t.Fatalf("expected lock to be acquired after release")
	}
	if cached2 != nil {
		t.Fatalf("expected cached to be nil after release")
	}
}

func TestMemoryGuard_TenantIsolation(t *testing.T) {
	guard := idempotency.NewMemoryGuard()
	ctx := context.Background()

	key := "shared_key_name"

	// Tenant 1 acquires
	acq1, _, err1 := guard.AcquireLock(ctx, "tenant_1", key, 5*time.Second)
	if err1 != nil || !acq1 {
		t.Fatalf("tenant 1 acquire failed: %v", err1)
	}

	// Tenant 2 acquires same key (should succeed due to tenant isolation)
	acq2, _, err2 := guard.AcquireLock(ctx, "tenant_2", key, 5*time.Second)
	if err2 != nil || !acq2 {
		t.Fatalf("tenant 2 acquire should succeed: %v", err2)
	}
}

func TestMemoryGuard_Expiry(t *testing.T) {
	guard := idempotency.NewMemoryGuard()
	ctx := context.Background()

	tenant := "tenant_exp"
	key := "key_exp"

	// Short TTL
	acquired, _, err := guard.AcquireLock(ctx, tenant, key, 50*time.Millisecond)
	if err != nil || !acquired {
		t.Fatalf("initial acquire failed: %v", err)
	}

	// Wait for expiration
	time.Sleep(70 * time.Millisecond)

	// Re-acquire should succeed because previous lock expired
	acquired2, _, err2 := guard.AcquireLock(ctx, tenant, key, 5*time.Second)
	if err2 != nil || !acquired2 {
		t.Fatalf("re-acquire after expiry failed: %v", err2)
	}
}

func TestGuard_Validation(t *testing.T) {
	memGuard := idempotency.NewMemoryGuard()
	redisGuard := idempotency.NewRedisGuard(nil)
	ctx := context.Background()

	// MemoryGuard validations
	if _, _, err := memGuard.AcquireLock(ctx, "", "key", time.Minute); err == nil {
		t.Error("expected error for empty tenantID")
	}
	if _, _, err := memGuard.AcquireLock(ctx, "t1", "", time.Minute); err == nil {
		t.Error("expected error for empty key")
	}
	if err := memGuard.SetResponse(ctx, "", "key", 200, []byte("ok"), time.Minute); err == nil {
		t.Error("expected error for empty tenantID in SetResponse")
	}
	if err := memGuard.SetResponse(ctx, "t1", "", 200, []byte("ok"), time.Minute); err == nil {
		t.Error("expected error for empty key in SetResponse")
	}
	if err := memGuard.ReleaseLock(ctx, "", "key"); err == nil {
		t.Error("expected error for empty tenantID in ReleaseLock")
	}
	if err := memGuard.ReleaseLock(ctx, "t1", ""); err == nil {
		t.Error("expected error for empty key in ReleaseLock")
	}

	// RedisGuard validations
	if _, _, err := redisGuard.AcquireLock(ctx, "", "key", time.Minute); err == nil {
		t.Error("expected error for empty tenantID in RedisGuard.AcquireLock")
	}
	if _, _, err := redisGuard.AcquireLock(ctx, "t1", "", time.Minute); err == nil {
		t.Error("expected error for empty key in RedisGuard.AcquireLock")
	}
	if err := redisGuard.SetResponse(ctx, "", "key", 200, []byte("ok"), time.Minute); err == nil {
		t.Error("expected error for empty tenantID in RedisGuard.SetResponse")
	}
	if err := redisGuard.SetResponse(ctx, "t1", "", 200, []byte("ok"), time.Minute); err == nil {
		t.Error("expected error for empty key in RedisGuard.SetResponse")
	}
	if err := redisGuard.ReleaseLock(ctx, "", "key"); err == nil {
		t.Error("expected error for empty tenantID in RedisGuard.ReleaseLock")
	}
	if err := redisGuard.ReleaseLock(ctx, "t1", ""); err == nil {
		t.Error("expected error for empty key in RedisGuard.ReleaseLock")
	}
}

