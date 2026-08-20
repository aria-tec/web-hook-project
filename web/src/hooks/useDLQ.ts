import { useState, useEffect, useCallback } from 'react';
import { DLQEvent, ReplayResponse } from '../types';

export interface UseDLQOptions {
  tenantId: string;
  baseUrl?: string;
  autoFetch?: boolean;
}

export function useDLQ(options: UseDLQOptions) {
  const { tenantId, baseUrl = '', autoFetch = true } = options;

  const [events, setEvents] = useState<DLQEvent[]>([]);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [isReplaying, setIsReplaying] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [lastReplayedResult, setLastReplayedResult] = useState<ReplayResponse | null>(null);

  const fetchDLQ = useCallback(async () => {
    if (!tenantId) return;
    setIsLoading(true);
    setError(null);

    try {
      const response = await fetch(`${baseUrl}/api/v1/dlq`, {
        method: 'GET',
        headers: {
          'Accept': 'application/json',
          'X-Tenant-ID': tenantId,
        },
      });

      if (!response.ok) {
        const text = await response.text();
        throw new Error(`Failed to load DLQ (${response.status}): ${text || response.statusText}`);
      }

      const data = await response.json();
      const list: DLQEvent[] = Array.isArray(data) ? data : [];
      setEvents(list);
      // Clean up selected IDs that no longer exist
      setSelectedIds((prev) => {
        const next = new Set<string>();
        for (const id of prev) {
          if (list.some((e) => e.id === id)) {
            next.add(id);
          }
        }
        return next;
      });
    } catch (err: any) {
      setError(err.message || 'Error fetching DLQ events');
    } finally {
      setIsLoading(false);
    }
  }, [baseUrl, tenantId]);

  const toggleSelect = useCallback((eventId: string) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(eventId)) {
        next.delete(eventId);
      } else {
        next.add(eventId);
      }
      return next;
    });
  }, []);

  const selectAll = useCallback(() => {
    setSelectedIds(new Set(events.map((e) => e.id)));
  }, [events]);

  const clearSelection = useCallback(() => {
    setSelectedIds(new Set());
  }, []);

  const replayEvents = useCallback(async (eventIds: string[]): Promise<ReplayResponse | null> => {
    if (!tenantId || eventIds.length === 0) return null;
    setIsReplaying(true);
    setError(null);

    try {
      const response = await fetch(`${baseUrl}/api/v1/dlq/replay`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Tenant-ID': tenantId,
        },
        body: JSON.stringify({ event_ids: eventIds }),
      });

      if (!response.ok) {
        const text = await response.text();
        throw new Error(`Failed to replay DLQ (${response.status}): ${text || response.statusText}`);
      }

      const result: ReplayResponse = await response.json();
      setLastReplayedResult(result);
      // Refresh DLQ after replay
      await fetchDLQ();
      return result;
    } catch (err: any) {
      setError(err.message || 'Error replaying DLQ events');
      return null;
    } finally {
      setIsReplaying(false);
    }
  }, [baseUrl, fetchDLQ, tenantId]);

  const replaySelected = useCallback(async () => {
    const ids = Array.from(selectedIds);
    if (ids.length > 0) {
      return await replayEvents(ids);
    }
    return null;
  }, [replayEvents, selectedIds]);

  const replayAll = useCallback(async () => {
    const ids = events.map((e) => e.id);
    if (ids.length > 0) {
      return await replayEvents(ids);
    }
    return null;
  }, [events, replayEvents]);

  useEffect(() => {
    if (autoFetch && tenantId) {
      fetchDLQ();
    }
  }, [autoFetch, fetchDLQ, tenantId]);

  return {
    events,
    selectedIds,
    isLoading,
    isReplaying,
    error,
    lastReplayedResult,
    fetchDLQ,
    toggleSelect,
    selectAll,
    clearSelection,
    replayEvents,
    replaySelected,
    replayAll,
  };
}
