package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"web-hook-project/internal/domain"
)

var (
	ErrNilEvent     = errors.New("event cannot be nil")
	ErrEmptyStream  = errors.New("streamName cannot be empty")
	ErrEmptyGroup   = errors.New("groupName cannot be empty")
)

// QueueMessage represents an event message delivered through the stream queue.
type QueueMessage struct {
	ID        string    `json:"id"`
	EventID   string    `json:"event_id"`
	TenantID  string    `json:"tenant_id"`
	EventType string    `json:"event_type"`
	Payload   []byte    `json:"payload"`
	CreatedAt time.Time `json:"created_at"`
}

// StreamQueue defines the interface for publishing and consuming events via streams.
type StreamQueue interface {
	PublishEvent(ctx context.Context, streamName string, event *domain.Event) (messageID string, err error)
	CreateConsumerGroup(ctx context.Context, streamName, groupName, startID string) error
	ReadEvents(ctx context.Context, streamName, groupName, consumerName string, count int64, block time.Duration) ([]QueueMessage, error)
	AckEvent(ctx context.Context, streamName, groupName string, messageIDs ...string) error
	AutoClaim(ctx context.Context, streamName, groupName, consumerName string, minIdle time.Duration, startID string, count int64) (messages []QueueMessage, nextStartID string, err error)
}

// RedisStreamQueue is a Redis Streams backed implementation of StreamQueue.
type RedisStreamQueue struct {
	client       *redis.Client
	maxLenApprox int64
}

// NewRedisStreamQueue creates a new RedisStreamQueue instance.
func NewRedisStreamQueue(client *redis.Client) *RedisStreamQueue {
	return &RedisStreamQueue{
		client:       client,
		maxLenApprox: 500000,
	}
}

// WithMaxLenApprox sets the approximate stream trimming length for Redis memory safety.
func (r *RedisStreamQueue) WithMaxLenApprox(maxLen int64) *RedisStreamQueue {
	r.maxLenApprox = maxLen
	return r
}

func (r *RedisStreamQueue) PublishEvent(ctx context.Context, streamName string, event *domain.Event) (string, error) {
	if event == nil {
		return "", ErrNilEvent
	}
	if streamName == "" {
		return "", ErrEmptyStream
	}

	createdAt := event.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	values := map[string]interface{}{
		"event_id":   event.ID,
		"tenant_id":  event.TenantID,
		"event_type": event.EventType,
		"payload":    string(event.Payload),
		"created_at": createdAt.UTC().Format(time.RFC3339Nano),
	}

	xAddArgs := &redis.XAddArgs{
		Stream: streamName,
		Values: values,
	}
	if r.maxLenApprox > 0 {
		xAddArgs.MaxLen = r.maxLenApprox
		xAddArgs.Approx = true
	}

	msgID, err := r.client.XAdd(ctx, xAddArgs).Result()
	if err != nil {
		return "", fmt.Errorf("redis xadd error: %w", err)
	}

	return msgID, nil
}

func (r *RedisStreamQueue) CreateConsumerGroup(ctx context.Context, streamName, groupName, startID string) error {
	if streamName == "" {
		return ErrEmptyStream
	}
	if groupName == "" {
		return ErrEmptyGroup
	}
	if startID == "" {
		startID = "$"
	}

	err := r.client.XGroupCreateMkStream(ctx, streamName, groupName, startID).Err()
	if err != nil {
		// Idempotent creation: ignore BUSYGROUP error if group already exists
		if strings.Contains(err.Error(), "BUSYGROUP") {
			return nil
		}
		return fmt.Errorf("redis xgroup create error: %w", err)
	}

	return nil
}

func (r *RedisStreamQueue) ReadEvents(ctx context.Context, streamName, groupName, consumerName string, count int64, block time.Duration) ([]QueueMessage, error) {
	if streamName == "" {
		return nil, ErrEmptyStream
	}
	if groupName == "" {
		return nil, ErrEmptyGroup
	}
	if count <= 0 {
		count = 10
	}

	res, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamName, ">"},
		Count:    count,
		Block:    block,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return []QueueMessage{}, nil
		}
		return nil, fmt.Errorf("redis xreadgroup error: %w", err)
	}

	var messages []QueueMessage
	for _, stream := range res {
		for _, msg := range stream.Messages {
			messages = append(messages, parseRedisMessage(msg))
		}
	}

	return messages, nil
}

func (r *RedisStreamQueue) AckEvent(ctx context.Context, streamName, groupName string, messageIDs ...string) error {
	if len(messageIDs) == 0 {
		return nil
	}
	if streamName == "" {
		return ErrEmptyStream
	}
	if groupName == "" {
		return ErrEmptyGroup
	}

	err := r.client.XAck(ctx, streamName, groupName, messageIDs...).Err()
	if err != nil {
		return fmt.Errorf("redis xack error: %w", err)
	}

	return nil
}

func (r *RedisStreamQueue) AutoClaim(ctx context.Context, streamName, groupName, consumerName string, minIdle time.Duration, startID string, count int64) ([]QueueMessage, string, error) {
	if streamName == "" {
		return nil, "", ErrEmptyStream
	}
	if groupName == "" {
		return nil, "", ErrEmptyGroup
	}
	if startID == "" {
		startID = "0-0"
	}
	if count <= 0 {
		count = 10
	}

	res, nextStart, err := r.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   streamName,
		Group:    groupName,
		Consumer: consumerName,
		MinIdle:  minIdle,
		Start:    startID,
		Count:    count,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return []QueueMessage{}, "0-0", nil
		}
		return nil, "", fmt.Errorf("redis xautoclaim error: %w", err)
	}

	var messages []QueueMessage
	for _, msg := range res {
		messages = append(messages, parseRedisMessage(msg))
	}

	return messages, nextStart, nil
}

func parseRedisMessage(msg redis.XMessage) QueueMessage {
	qm := QueueMessage{
		ID: msg.ID,
	}

	if v, ok := msg.Values["event_id"].(string); ok {
		qm.EventID = v
	}
	if v, ok := msg.Values["tenant_id"].(string); ok {
		qm.TenantID = v
	}
	if v, ok := msg.Values["event_type"].(string); ok {
		qm.EventType = v
	}
	if v, ok := msg.Values["payload"]; ok {
		switch val := v.(type) {
		case string:
			qm.Payload = []byte(val)
		case []byte:
			qm.Payload = val
		}
	}
	if v, ok := msg.Values["created_at"].(string); ok {
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			qm.CreatedAt = t
		} else if t, err := time.Parse(time.RFC3339, v); err == nil {
			qm.CreatedAt = t
		}
	}

	return qm
}

// memPendingEntry stores message delivery time and active consumer in memory.
type memPendingEntry struct {
	msg          QueueMessage
	deliveryTime time.Time
	consumer     string
}

// memGroup tracks state for a consumer group within an in-memory stream.
type memGroup struct {
	lastDeliveredIdx int
	pending          map[string]memPendingEntry
}

// memStream represents a single stream in memory.
type memStream struct {
	messages []QueueMessage
	groups   map[string]*memGroup
	notifyCh chan struct{}
}

// MemoryStreamQueue provides a thread-safe, in-memory StreamQueue for testing and local dev.
type MemoryStreamQueue struct {
	mu      sync.Mutex
	streams map[string]*memStream
	seq     int64
}

// NewMemoryStreamQueue creates a new MemoryStreamQueue instance.
func NewMemoryStreamQueue() *MemoryStreamQueue {
	return &MemoryStreamQueue{
		streams: make(map[string]*memStream),
	}
}

func (m *MemoryStreamQueue) getOrCreateStreamLocked(streamName string) *memStream {
	st, exists := m.streams[streamName]
	if !exists {
		st = &memStream{
			groups:   make(map[string]*memGroup),
			notifyCh: make(chan struct{}),
		}
		m.streams[streamName] = st
	}
	return st
}

func (m *MemoryStreamQueue) PublishEvent(ctx context.Context, streamName string, event *domain.Event) (string, error) {
	if event == nil {
		return "", ErrNilEvent
	}
	if streamName == "" {
		return "", ErrEmptyStream
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.seq++
	now := time.Now().UTC()
	msgID := fmt.Sprintf("%d-%d", now.UnixMilli(), m.seq)

	var payloadCopy []byte
	if len(event.Payload) > 0 {
		payloadCopy = make([]byte, len(event.Payload))
		copy(payloadCopy, event.Payload)
	}

	createdAt := event.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}

	qm := QueueMessage{
		ID:        msgID,
		EventID:   event.ID,
		TenantID:  event.TenantID,
		EventType: event.EventType,
		Payload:   payloadCopy,
		CreatedAt: createdAt,
	}

	st := m.getOrCreateStreamLocked(streamName)
	st.messages = append(st.messages, qm)

	// Notify all waiting consumers
	oldNotify := st.notifyCh
	st.notifyCh = make(chan struct{})
	close(oldNotify)

	return msgID, nil
}

func (m *MemoryStreamQueue) CreateConsumerGroup(ctx context.Context, streamName, groupName, startID string) error {
	if streamName == "" {
		return ErrEmptyStream
	}
	if groupName == "" {
		return ErrEmptyGroup
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	st := m.getOrCreateStreamLocked(streamName)
	if _, exists := st.groups[groupName]; exists {
		// Idempotent creation
		return nil
	}

	lastIdx := -1
	if startID == "$" || startID == "" {
		lastIdx = len(st.messages) - 1
	} else if startID == "0" || startID == "0-0" {
		lastIdx = -1
	}

	st.groups[groupName] = &memGroup{
		lastDeliveredIdx: lastIdx,
		pending:          make(map[string]memPendingEntry),
	}

	return nil
}

func (m *MemoryStreamQueue) ReadEvents(ctx context.Context, streamName, groupName, consumerName string, count int64, block time.Duration) ([]QueueMessage, error) {
	if streamName == "" {
		return nil, ErrEmptyStream
	}
	if groupName == "" {
		return nil, ErrEmptyGroup
	}
	if count <= 0 {
		count = 10
	}

	var deadline time.Time
	if block > 0 {
		deadline = time.Now().Add(block)
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		m.mu.Lock()
		st := m.getOrCreateStreamLocked(streamName)
		grp, exists := st.groups[groupName]
		if !exists {
			grp = &memGroup{
				lastDeliveredIdx: -1,
				pending:          make(map[string]memPendingEntry),
			}
			st.groups[groupName] = grp
		}

		start := grp.lastDeliveredIdx + 1
		if start < len(st.messages) {
			end := start + int(count)
			if end > len(st.messages) {
				end = len(st.messages)
			}

			now := time.Now()
			result := make([]QueueMessage, 0, end-start)
			for i := start; i < end; i++ {
				msg := st.messages[i]
				if len(msg.Payload) > 0 {
					pCopy := make([]byte, len(msg.Payload))
					copy(pCopy, msg.Payload)
					msg.Payload = pCopy
				}
				grp.pending[msg.ID] = memPendingEntry{
					msg:          msg,
					deliveryTime: now,
					consumer:     consumerName,
				}
				result = append(result, msg)
			}
			grp.lastDeliveredIdx = end - 1
			m.mu.Unlock()
			return result, nil
		}

		if block <= 0 {
			m.mu.Unlock()
			return []QueueMessage{}, nil
		}

		notifyCh := st.notifyCh
		m.mu.Unlock()

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return []QueueMessage{}, nil
		}

		timer := time.NewTimer(remaining)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
			return []QueueMessage{}, nil
		case <-notifyCh:
			timer.Stop()
		}
	}
}

func (m *MemoryStreamQueue) AckEvent(ctx context.Context, streamName, groupName string, messageIDs ...string) error {
	if len(messageIDs) == 0 {
		return nil
	}
	if streamName == "" {
		return ErrEmptyStream
	}
	if groupName == "" {
		return ErrEmptyGroup
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	st, exists := m.streams[streamName]
	if !exists {
		return nil
	}

	grp, exists := st.groups[groupName]
	if !exists {
		return nil
	}

	for _, id := range messageIDs {
		delete(grp.pending, id)
	}

	return nil
}

func (m *MemoryStreamQueue) AutoClaim(ctx context.Context, streamName, groupName, consumerName string, minIdle time.Duration, startID string, count int64) ([]QueueMessage, string, error) {
	if streamName == "" {
		return nil, "", ErrEmptyStream
	}
	if groupName == "" {
		return nil, "", ErrEmptyGroup
	}
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if count <= 0 {
		count = 10
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	st, exists := m.streams[streamName]
	if !exists {
		return []QueueMessage{}, "0-0", nil
	}

	grp, exists := st.groups[groupName]
	if !exists {
		return []QueueMessage{}, "0-0", nil
	}

	now := time.Now()
	var claimed []QueueMessage

	for id, entry := range grp.pending {
		if now.Sub(entry.deliveryTime) >= minIdle {
			claimed = append(claimed, entry.msg)
			// update entry
			grp.pending[id] = memPendingEntry{
				msg:          entry.msg,
				deliveryTime: now,
				consumer:     consumerName,
			}
			if int64(len(claimed)) >= count {
				break
			}
		}
	}

	return claimed, "0-0", nil
}

