package idempotency

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrInvalidParameters = errors.New("tenantID and key must not be empty")
)

// CachedResponse encapsulates the cached HTTP response for an idempotent request.
type CachedResponse struct {
	StatusCode  int       `json:"status_code"`
	Body        []byte    `json:"body"`
	CompletedAt time.Time `json:"completed_at"`
}

// Guard provides distributed locking and response caching for idempotent operations.
type Guard interface {
	// AcquireLock attempts to acquire a lock for the given tenant and idempotency key.
	// If acquired == true: caller owns the lock and should process the request.
	// If acquired == false and cached != nil: request was already completed, caller should return cached response.
	// If acquired == false and cached == nil: concurrent in-flight request is currently processing.
	AcquireLock(ctx context.Context, tenantID, key string, ttl time.Duration) (acquired bool, cached *CachedResponse, err error)

	// SetResponse stores the completed response for the given tenant and idempotency key.
	SetResponse(ctx context.Context, tenantID, key string, statusCode int, responseBody []byte, ttl time.Duration) error

	// ReleaseLock releases an in-flight lock if the operation failed and should not be cached.
	ReleaseLock(ctx context.Context, tenantID, key string) error
}

const inFlightMarker = "IN_FLIGHT"

// RedisGuard implements Guard backed by Redis.
type RedisGuard struct {
	client *redis.Client
}

// NewRedisGuard creates a new Redis-backed Guard instance.
func NewRedisGuard(client *redis.Client) *RedisGuard {
	return &RedisGuard{client: client}
}

var acquireLockLua = redis.NewScript(`
local val = redis.call('GET', KEYS[1])
if val then
    return val
else
    local ok = redis.call('SET', KEYS[1], 'IN_FLIGHT', 'NX', 'PX', ARGV[1])
    if ok then
        return 'ACQUIRED'
    else
        return redis.call('GET', KEYS[1])
    end
end
`)

var releaseLockLua = redis.NewScript(`
if redis.call('GET', KEYS[1]) == 'IN_FLIGHT' then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
`)

func (r *RedisGuard) redisKey(tenantID, key string) string {
	return fmt.Sprintf("idemp:%s:%s", tenantID, key)
}

func (r *RedisGuard) AcquireLock(ctx context.Context, tenantID, key string, ttl time.Duration) (bool, *CachedResponse, error) {
	if tenantID == "" || key == "" {
		return false, nil, ErrInvalidParameters
	}

	keyName := r.redisKey(tenantID, key)
	ttlMs := ttl.Milliseconds()
	if ttlMs <= 0 {
		ttlMs = (24 * time.Hour).Milliseconds()
	}

	res, err := acquireLockLua.Run(ctx, r.client, []string{keyName}, ttlMs).Result()
	if err != nil {
		return false, nil, fmt.Errorf("redis acquire lock error: %w", err)
	}

	strVal, ok := res.(string)
	if !ok {
		return false, nil, fmt.Errorf("unexpected redis result type: %T", res)
	}

	if strVal == "ACQUIRED" {
		return true, nil, nil
	}

	if strVal == inFlightMarker {
		return false, nil, nil
	}

	var cached CachedResponse
	if err := json.Unmarshal([]byte(strVal), &cached); err != nil {
		return false, nil, fmt.Errorf("failed to unmarshal cached response: %w", err)
	}

	return false, &cached, nil
}

func (r *RedisGuard) SetResponse(ctx context.Context, tenantID, key string, statusCode int, responseBody []byte, ttl time.Duration) error {
	if tenantID == "" || key == "" {
		return ErrInvalidParameters
	}

	resp := CachedResponse{
		StatusCode:  statusCode,
		Body:        responseBody,
		CompletedAt: time.Now().UTC(),
	}

	data, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("failed to marshal cached response: %w", err)
	}

	keyName := r.redisKey(tenantID, key)
	if err := r.client.Set(ctx, keyName, string(data), ttl).Err(); err != nil {
		return fmt.Errorf("redis set response error: %w", err)
	}

	return nil
}

func (r *RedisGuard) ReleaseLock(ctx context.Context, tenantID, key string) error {
	if tenantID == "" || key == "" {
		return ErrInvalidParameters
	}

	keyName := r.redisKey(tenantID, key)
	if err := releaseLockLua.Run(ctx, r.client, []string{keyName}).Err(); err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("redis release lock error: %w", err)
	}

	return nil
}

// memoryEntry tracks in-flight and completed idempotency records in memory.
type memoryEntry struct {
	inFlight  bool
	cached    *CachedResponse
	expiresAt time.Time
}

// MemoryGuard is an in-memory thread-safe implementation of Guard for unit tests and local dev.
type MemoryGuard struct {
	mu      sync.RWMutex
	entries map[string]*memoryEntry
}

// NewMemoryGuard creates a new MemoryGuard instance.
func NewMemoryGuard() *MemoryGuard {
	return &MemoryGuard{
		entries: make(map[string]*memoryEntry),
	}
}

func (m *MemoryGuard) memoryKey(tenantID, key string) string {
	return tenantID + ":" + key
}

func (m *MemoryGuard) AcquireLock(ctx context.Context, tenantID, key string, ttl time.Duration) (bool, *CachedResponse, error) {
	if err := ctx.Err(); err != nil {
		return false, nil, err
	}
	if tenantID == "" || key == "" {
		return false, nil, ErrInvalidParameters
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	k := m.memoryKey(tenantID, key)
	now := time.Now()

	if entry, exists := m.entries[k]; exists {
		if now.Before(entry.expiresAt) {
			if entry.cached != nil {
				cachedCopy := *entry.cached
				if len(entry.cached.Body) > 0 {
					cachedCopy.Body = make([]byte, len(entry.cached.Body))
					copy(cachedCopy.Body, entry.cached.Body)
				}
				return false, &cachedCopy, nil
			}
			if entry.inFlight {
				return false, nil, nil
			}
		}
	}

	// Not exists or expired -> acquire lock
	m.entries[k] = &memoryEntry{
		inFlight:  true,
		expiresAt: now.Add(ttl),
	}
	return true, nil, nil
}

func (m *MemoryGuard) SetResponse(ctx context.Context, tenantID, key string, statusCode int, responseBody []byte, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if tenantID == "" || key == "" {
		return ErrInvalidParameters
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	k := m.memoryKey(tenantID, key)
	now := time.Now()

	cached := &CachedResponse{
		StatusCode:  statusCode,
		CompletedAt: now.UTC(),
	}
	if len(responseBody) > 0 {
		cached.Body = make([]byte, len(responseBody))
		copy(cached.Body, responseBody)
	}

	m.entries[k] = &memoryEntry{
		inFlight:  false,
		cached:    cached,
		expiresAt: now.Add(ttl),
	}
	return nil
}

func (m *MemoryGuard) ReleaseLock(ctx context.Context, tenantID, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if tenantID == "" || key == "" {
		return ErrInvalidParameters
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	k := m.memoryKey(tenantID, key)
	if entry, exists := m.entries[k]; exists && entry.inFlight {
		delete(m.entries, k)
	}
	return nil
}
