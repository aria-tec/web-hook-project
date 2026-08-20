import { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import { DeliveryAttempt, ConnectionState, SystemStats } from '../types';

const MAX_BUFFER_SIZE = 200;
const RECONNECT_DELAY_MS = 3000;

export interface UseEventStreamOptions {
  url?: string;
  autoConnect?: boolean;
}

export function useEventStream(options: UseEventStreamOptions = {}) {
  const { url = '/api/v1/events/stream', autoConnect = true } = options;

  const [attempts, setAttempts] = useState<DeliveryAttempt[]>([]);
  const [connectionState, setConnectionState] = useState<ConnectionState>('disconnected');
  const [isPaused, setIsPaused] = useState<boolean>(false);
  const [lastHeartbeat, setLastHeartbeat] = useState<Date | null>(null);

  const eventSourceRef = useRef<EventSource | null>(null);
  const isPausedRef = useRef<boolean>(false);
  const reconnectTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  isPausedRef.current = isPaused;

  const connect = useCallback(() => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }

    if (reconnectTimeoutRef.current) {
      clearTimeout(reconnectTimeoutRef.current);
      reconnectTimeoutRef.current = null;
    }

    setConnectionState('connecting');

    try {
      // Determine stream URL (supports relative or absolute URL)
      const targetUrl = url.startsWith('http')
        ? url
        : `${window.location.origin}${url.startsWith('/') ? url : `/${url}`}`;

      const es = new EventSource(targetUrl);
      eventSourceRef.current = es;

      es.onopen = () => {
        setConnectionState('connected');
        setLastHeartbeat(new Date());
      };

      es.onmessage = (event) => {
        setLastHeartbeat(new Date());
        if (isPausedRef.current) {
          return;
        }

        try {
          const raw = JSON.parse(event.data) as DeliveryAttempt;
          if (raw && raw.id) {
            setAttempts((prev) => {
              // Prepend new attempt and enforce strictly bounded 200-event ring buffer
              const updated = [raw, ...prev];
              if (updated.length > MAX_BUFFER_SIZE) {
                return updated.slice(0, MAX_BUFFER_SIZE);
              }
              return updated;
            });
          }
        } catch {
          // Non-JSON SSE messages (e.g. heartbeat pings)
        }
      };

      es.onerror = () => {
        setConnectionState('error');
        if (eventSourceRef.current) {
          eventSourceRef.current.close();
          eventSourceRef.current = null;
        }

        // Schedule auto-reconnect
        if (!reconnectTimeoutRef.current) {
          reconnectTimeoutRef.current = setTimeout(() => {
            reconnectTimeoutRef.current = null;
            connect();
          }, RECONNECT_DELAY_MS);
        }
      };
    } catch {
      setConnectionState('error');
    }
  }, [url]);

  const disconnect = useCallback(() => {
    if (reconnectTimeoutRef.current) {
      clearTimeout(reconnectTimeoutRef.current);
      reconnectTimeoutRef.current = null;
    }
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
    setConnectionState('disconnected');
  }, []);

  const clearStream = useCallback(() => {
    setAttempts([]);
  }, []);

  const togglePause = useCallback(() => {
    setIsPaused((prev) => !prev);
  }, []);

  useEffect(() => {
    if (autoConnect) {
      connect();
    }
    return () => {
      disconnect();
    };
  }, [autoConnect, connect, disconnect]);

  // Derive stats from current buffer
  const stats = useMemo<SystemStats>(() => {
    let successCount = 0;
    let retryingCount = 0;
    let failedCount = 0;
    let totalDuration = 0;

    for (const item of attempts) {
      if (item.status === 'SUCCESS') successCount++;
      else if (item.status === 'RETRYING') retryingCount++;
      else if (item.status === 'FAILED') failedCount++;
      totalDuration += item.duration_ms || 0;
    }

    const avgDurationMs = attempts.length > 0 ? Math.round(totalDuration / attempts.length) : 0;

    return {
      totalAttempts: attempts.length,
      successCount,
      retryingCount,
      failedCount,
      avgDurationMs,
      dlqCount: failedCount,
    };
  }, [attempts]);

  return {
    attempts,
    connectionState,
    isPaused,
    lastHeartbeat,
    stats,
    connect,
    disconnect,
    clearStream,
    togglePause,
    setIsPaused,
  };
}
