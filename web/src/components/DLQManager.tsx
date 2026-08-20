import React, { useState } from 'react';
import {
  Flame,
  RotateCcw,
  RefreshCw,
  CheckSquare,
  Square,
  FileCode,
  CheckCircle2,
  AlertCircle,
  Loader2,
  Copy,
  Check,
  X,
} from 'lucide-react';
import { DLQEvent, ReplayResponse } from '../types';

interface DLQManagerProps {
  events: DLQEvent[];
  selectedIds: Set<string>;
  isLoading: boolean;
  isReplaying: boolean;
  error: string | null;
  lastReplayedResult: ReplayResponse | null;
  onRefresh: () => void;
  onToggleSelect: (id: string) => void;
  onSelectAll: () => void;
  onClearSelection: () => void;
  onReplaySelected: () => Promise<ReplayResponse | null>;
  onReplayAll: () => Promise<ReplayResponse | null>;
  tenantId: string;
}

export const DLQManager: React.FC<DLQManagerProps> = ({
  events,
  selectedIds,
  isLoading,
  isReplaying,
  error,
  lastReplayedResult,
  onRefresh,
  onToggleSelect,
  onSelectAll,
  onClearSelection,
  onReplaySelected,
  onReplayAll,
  tenantId,
}) => {
  const [inspectingEvent, setInspectingEvent] = useState<DLQEvent | null>(null);
  const [copiedKey, setCopiedKey] = useState<string | null>(null);

  const copyToClipboard = (text: string, key: string) => {
    navigator.clipboard.writeText(text);
    setCopiedKey(key);
    setTimeout(() => setCopiedKey(null), 2000);
  };

  const isAllSelected = events.length > 0 && selectedIds.size === events.length;

  const handleSelectAllToggle = () => {
    if (isAllSelected) {
      onClearSelection();
    } else {
      onSelectAll();
    }
  };

  return (
    <div className="space-y-4">
      {/* Top Banner / Invariant Notice */}
      <div className="bg-gradient-to-r from-rose-950/40 via-slate-900/60 to-indigo-950/40 border border-slate-800 rounded-xl p-4 flex flex-col md:flex-row md:items-center md:justify-between gap-3 backdrop-blur-md">
        <div className="flex items-start space-x-3">
          <div className="p-2 rounded-lg bg-rose-500/10 border border-rose-500/20 text-rose-400 mt-0.5">
            <Flame className="w-5 h-5" aria-hidden="true" />
          </div>
          <div>
            <h3 className="text-sm font-bold text-white flex items-center">
              Dead-Letter Queue (DLQ) Recovery Center
              <span className="ml-2 px-2 py-0.5 rounded-full text-xs font-mono bg-rose-500/20 text-rose-300 border border-rose-500/30">
                {events.length} Poisoned Events
              </span>
            </h3>
            <p className="text-xs text-slate-400 mt-1 max-w-2xl">
              Events that exhausted max retries or were rejected with 4xx poison responses are preserved in PostgreSQL storage.
              When replayed, the Go Engine automatically mints <strong className="text-indigo-300">fresh timestamps and HMAC signatures</strong> to guarantee receiver freshness.
            </p>
          </div>
        </div>

        {/* DLQ Actions */}
        <div className="flex flex-wrap items-center gap-2 self-end md:self-center">
          <button
            onClick={onRefresh}
            disabled={isLoading || isReplaying}
            className="p-2 rounded-lg bg-slate-800 hover:bg-slate-700 border border-slate-700 text-slate-300 text-xs flex items-center space-x-1.5 transition-colors disabled:opacity-50"
            title="Refresh DLQ list from Go Engine"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${isLoading ? 'animate-spin' : ''}`} aria-hidden="true" />
            <span>Refresh</span>
          </button>

          <button
            onClick={onReplaySelected}
            disabled={selectedIds.size === 0 || isReplaying}
            className="px-3 py-2 rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-semibold flex items-center space-x-1.5 shadow-md shadow-indigo-600/30 transition-all disabled:opacity-40 disabled:pointer-events-none"
          >
            {isReplaying ? (
              <Loader2 className="w-3.5 h-3.5 animate-spin" aria-hidden="true" />
            ) : (
              <RotateCcw className="w-3.5 h-3.5" aria-hidden="true" />
            )}
            <span>Replay Selected ({selectedIds.size})</span>
          </button>

          <button
            onClick={onReplayAll}
            disabled={events.length === 0 || isReplaying}
            className="px-3 py-2 rounded-lg bg-rose-600 hover:bg-rose-500 text-white text-xs font-semibold flex items-center space-x-1.5 shadow-md shadow-rose-600/30 transition-all disabled:opacity-40 disabled:pointer-events-none"
          >
            {isReplaying ? (
              <Loader2 className="w-3.5 h-3.5 animate-spin" aria-hidden="true" />
            ) : (
              <Flame className="w-3.5 h-3.5 fill-current" aria-hidden="true" />
            )}
            <span>Replay All ({events.length})</span>
          </button>
        </div>
      </div>

      {/* Error Banner */}
      {error && (
        <div className="p-3 bg-rose-950/60 border border-rose-800/80 rounded-lg text-xs font-mono text-rose-300 flex items-center space-x-2">
          <AlertCircle className="w-4 h-4 text-rose-400 flex-shrink-0" aria-hidden="true" />
          <span>{error}</span>
        </div>
      )}

      {/* Replay feedback */}
      {lastReplayedResult && (
        <div className="p-3 bg-emerald-950/60 border border-emerald-800/80 rounded-lg text-xs font-mono text-emerald-300 flex items-center space-x-2 animate-fade-in">
          <CheckCircle2 className="w-4 h-4 text-emerald-400 flex-shrink-0" aria-hidden="true" />
          <span>
            Successfully queued {lastReplayedResult.replayed_count} events for retry with fresh HMAC signing!
          </span>
        </div>
      )}

      {/* DLQ Event Table */}
      <div className="bg-slate-900/60 border border-slate-800/80 rounded-xl shadow-glass backdrop-blur-md overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs border-collapse">
            <thead>
              <tr className="border-b border-slate-800 bg-slate-950/40 text-slate-400 font-mono uppercase text-[10px] tracking-wider">
                <th className="py-3 px-4 w-10">
                  <button
                    onClick={handleSelectAllToggle}
                    className="text-slate-400 hover:text-white"
                    title={isAllSelected ? 'Deselect all' : 'Select all'}
                  >
                    {isAllSelected ? (
                      <CheckSquare className="w-4 h-4 text-indigo-400" aria-hidden="true" />
                    ) : (
                      <Square className="w-4 h-4 text-slate-500" aria-hidden="true" />
                    )}
                  </button>
                </th>
                <th className="py-3 px-4">Event ID</th>
                <th className="py-3 px-4">Event Type</th>
                <th className="py-3 px-4">Idempotency Key</th>
                <th className="py-3 px-4">Created At</th>
                <th className="py-3 px-4 text-right">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/40 font-mono">
              {events.length === 0 ? (
                <tr>
                  <td colSpan={6} className="py-12 text-center text-slate-500">
                    <div className="flex flex-col items-center justify-center space-y-2">
                      <div className="p-3 rounded-full bg-emerald-500/10 text-emerald-400">
                        <CheckCircle2 className="w-8 h-8" aria-hidden="true" />
                      </div>
                      <p className="text-sm font-sans font-medium text-slate-300">
                        Dead-Letter Queue is Clean!
                      </p>
                      <p className="text-xs font-sans text-slate-500 max-w-md">
                        There are currently no failed or poisoned events for tenant <code className="text-indigo-300 font-mono">{tenantId}</code>.
                        Use the Poison Pill simulation button above to test DLQ routing.
                      </p>
                    </div>
                  </td>
                </tr>
              ) : (
                events.map((event) => {
                  const isSelected = selectedIds.has(event.id);
                  return (
                    <tr
                      key={event.id}
                      className={`hover:bg-slate-800/40 transition-colors ${
                        isSelected ? 'bg-indigo-950/20' : ''
                      }`}
                    >
                      <td className="py-3 px-4">
                        <button
                          onClick={() => onToggleSelect(event.id)}
                          className="text-slate-400 hover:text-white"
                        >
                          {isSelected ? (
                            <CheckSquare className="w-4 h-4 text-indigo-400" aria-hidden="true" />
                          ) : (
                            <Square className="w-4 h-4 text-slate-500" aria-hidden="true" />
                          )}
                        </button>
                      </td>
                      <td className="py-3 px-4 font-semibold text-rose-300 whitespace-nowrap">
                        {event.id}
                      </td>
                      <td className="py-3 px-4 whitespace-nowrap">
                        <span className="px-2 py-0.5 rounded bg-slate-800 border border-slate-700 text-slate-200">
                          {event.event_type}
                        </span>
                      </td>
                      <td className="py-3 px-4 text-slate-400 whitespace-nowrap">
                        {event.idempotency_key || <span className="text-slate-600 font-sans italic">None</span>}
                      </td>
                      <td className="py-3 px-4 text-slate-400 whitespace-nowrap">
                        {new Date(event.created_at).toLocaleString()}
                      </td>
                      <td className="py-3 px-4 text-right whitespace-nowrap space-x-2">
                        <button
                          onClick={() => setInspectingEvent(event)}
                          className="inline-flex items-center space-x-1 px-2.5 py-1 rounded bg-slate-800 hover:bg-slate-700 border border-slate-700 text-slate-300 text-xs font-sans transition-colors"
                          title="Inspect raw payload JSON"
                        >
                          <FileCode className="w-3.5 h-3.5 text-indigo-400" aria-hidden="true" />
                          <span>Payload</span>
                        </button>
                        <button
                          onClick={() => onReplaySelected()}
                          className="inline-flex items-center space-x-1 px-2.5 py-1 rounded bg-indigo-600/20 hover:bg-indigo-600/30 border border-indigo-500/40 text-indigo-300 text-xs font-sans font-medium transition-colors"
                          title="Replay this event"
                        >
                          <RotateCcw className="w-3.5 h-3.5 text-indigo-400" aria-hidden="true" />
                          <span>1-Click Replay</span>
                        </button>
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Payload Inspection Drawer / Modal */}
      {inspectingEvent && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-950/80 backdrop-blur-sm animate-fade-in">
          <div className="bg-[#0F172A] border border-slate-700 rounded-2xl w-full max-w-2xl max-h-[85vh] flex flex-col shadow-2xl overflow-hidden">
            <div className="px-6 py-4 border-b border-slate-800 flex items-center justify-between bg-slate-900/80">
              <div className="flex items-center space-x-3">
                <div className="p-2 rounded-xl bg-indigo-500/10 text-indigo-400">
                  <FileCode className="w-5 h-5" aria-hidden="true" />
                </div>
                <div>
                  <h4 className="text-sm font-bold text-white">DLQ Event Payload</h4>
                  <p className="text-xs font-mono text-slate-400">{inspectingEvent.id}</p>
                </div>
              </div>
              <button
                onClick={() => setInspectingEvent(null)}
                className="p-1 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800"
              >
                <X className="w-5 h-5" aria-hidden="true" />
              </button>
            </div>

            <div className="p-6 overflow-y-auto space-y-4">
              <div className="flex items-center justify-between text-xs text-slate-400">
                <span>Event Type: <strong className="text-white">{inspectingEvent.event_type}</strong></span>
                <button
                  onClick={() =>
                    copyToClipboard(
                      typeof inspectingEvent.payload === 'string'
                        ? inspectingEvent.payload
                        : JSON.stringify(inspectingEvent.payload, null, 2),
                      'dlq_payload'
                    )
                  }
                  className="flex items-center space-x-1 text-indigo-400 hover:text-indigo-300"
                >
                  {copiedKey === 'dlq_payload' ? (
                    <Check className="w-3.5 h-3.5 text-emerald-400" aria-hidden="true" />
                  ) : (
                    <Copy className="w-3.5 h-3.5" aria-hidden="true" />
                  )}
                  <span>{copiedKey === 'dlq_payload' ? 'Copied' : 'Copy JSON'}</span>
                </button>
              </div>

              <pre className="bg-slate-950 p-4 rounded-xl border border-slate-800 font-mono text-xs text-slate-200 overflow-x-auto max-h-96 leading-relaxed">
                <code>
                  {typeof inspectingEvent.payload === 'string'
                    ? inspectingEvent.payload
                    : JSON.stringify(inspectingEvent.payload, null, 2)}
                </code>
              </pre>
            </div>

            <div className="px-6 py-3.5 border-t border-slate-800 bg-slate-900/80 flex justify-end">
              <button
                onClick={() => setInspectingEvent(null)}
                className="px-4 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-white text-xs font-semibold"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
