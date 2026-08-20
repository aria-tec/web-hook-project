import React, { useState, useMemo } from 'react';
import {
  Activity,
  CheckCircle2,
  AlertTriangle,
  Flame,
  Pause,
  Play,
  Trash2,
  Search,
  KeyRound,
  Clock,
} from 'lucide-react';
import { DeliveryAttempt, DeliveryStatus } from '../types';

interface LiveStreamProps {
  attempts: DeliveryAttempt[];
  isPaused: boolean;
  onTogglePause: () => void;
  onClearStream: () => void;
  onSelectAttempt: (attempt: DeliveryAttempt) => void;
}

export const LiveStream: React.FC<LiveStreamProps> = ({
  attempts,
  isPaused,
  onTogglePause,
  onClearStream,
  onSelectAttempt,
}) => {
  const [filterStatus, setFilterStatus] = useState<DeliveryStatus | 'ALL'>('ALL');
  const [searchQuery, setSearchQuery] = useState<string>('');

  const filteredAttempts = useMemo(() => {
    return attempts.filter((attempt) => {
      // Status filter
      if (filterStatus !== 'ALL' && attempt.status !== filterStatus) {
        return false;
      }
      // Search query filter
      if (searchQuery.trim()) {
        const query = searchQuery.toLowerCase();
        const matchId = attempt.id.toLowerCase().includes(query);
        const matchEventId = attempt.event_id.toLowerCase().includes(query);
        const matchEndpoint = attempt.endpoint_id.toLowerCase().includes(query);
        const matchError = (attempt.error_message || '').toLowerCase().includes(query);
        if (!matchId && !matchEventId && !matchEndpoint && !matchError) {
          return false;
        }
      }
      return true;
    });
  }, [attempts, filterStatus, searchQuery]);

  const renderStatusBadge = (status: DeliveryStatus, statusCode?: number) => {
    switch (status) {
      case 'SUCCESS':
        return (
          <span className="inline-flex items-center space-x-1 px-2.5 py-0.5 rounded-full text-xs font-mono font-medium bg-emerald-500/10 text-emerald-400 border border-emerald-500/30">
            <CheckCircle2 className="w-3.5 h-3.5" aria-hidden="true" />
            <span>200 OK</span>
          </span>
        );
      case 'RETRYING':
        return (
          <span className="inline-flex items-center space-x-1 px-2.5 py-0.5 rounded-full text-xs font-mono font-medium bg-amber-500/10 text-amber-400 border border-amber-500/30">
            <AlertTriangle className="w-3.5 h-3.5" aria-hidden="true" />
            <span>{statusCode ? `${statusCode} RETRYING` : 'RETRYING'}</span>
          </span>
        );
      case 'FAILED':
      default:
        return (
          <span className="inline-flex items-center space-x-1 px-2.5 py-0.5 rounded-full text-xs font-mono font-medium bg-rose-500/10 text-rose-400 border border-rose-500/30">
            <Flame className="w-3.5 h-3.5" aria-hidden="true" />
            <span>{statusCode ? `${statusCode} DLQ` : 'DLQ POISON'}</span>
          </span>
        );
    }
  };

  return (
    <div className="bg-slate-900/60 border border-slate-800/80 rounded-xl shadow-glass backdrop-blur-md overflow-hidden">
      {/* Stream Controls Header */}
      <div className="p-4 border-b border-slate-800 flex flex-col md:flex-row md:items-center md:justify-between gap-3 bg-slate-900/40">
        <div className="flex items-center space-x-3">
          <div className="flex items-center space-x-2">
            <div className="relative flex h-2.5 w-2.5">
              <span className={`animate-ping absolute inline-flex h-full w-full rounded-full opacity-75 ${isPaused ? 'bg-amber-400' : 'bg-indigo-400'}`}></span>
              <span className={`relative inline-flex rounded-full h-2.5 w-2.5 ${isPaused ? 'bg-amber-500' : 'bg-indigo-500'}`}></span>
            </div>
            <h3 className="text-sm font-bold text-white tracking-wide flex items-center">
              Real-Time Attempt Stream
              <span className="ml-2 text-xs font-mono px-2 py-0.5 rounded bg-slate-800 text-slate-300 border border-slate-700">
                {attempts.length} events
              </span>
            </h3>
          </div>

          {isPaused && (
            <span className="px-2 py-0.5 rounded bg-amber-500/10 border border-amber-500/30 text-amber-400 text-xs font-medium font-mono">
              STREAM PAUSED
            </span>
          )}
        </div>

        {/* Filter & Search Bar */}
        <div className="flex flex-wrap items-center gap-2">
          {/* Status Tabs */}
          <div className="flex items-center bg-slate-950 rounded-lg p-1 border border-slate-800 text-xs">
            {(['ALL', 'SUCCESS', 'RETRYING', 'FAILED'] as const).map((status) => (
              <button
                key={status}
                onClick={() => setFilterStatus(status)}
                className={`px-2.5 py-1 rounded transition-colors font-medium text-[11px] ${
                  filterStatus === status
                    ? 'bg-indigo-600 text-white shadow-sm'
                    : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                {status}
              </button>
            ))}
          </div>

          {/* Search box */}
          <div className="relative">
            <Search className="w-3.5 h-3.5 text-slate-500 absolute left-2.5 top-1/2 -translate-y-1/2" aria-hidden="true" />
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="Search IDs or errors..."
              className="bg-slate-950 border border-slate-800 rounded-lg pl-8 pr-3 py-1 text-xs text-slate-200 focus:outline-none focus:ring-1 focus:ring-indigo-500 placeholder-slate-500 font-mono w-44"
            />
          </div>

          {/* Pause / Resume Button */}
          <button
            onClick={onTogglePause}
            className={`p-1.5 rounded-lg border text-xs font-medium flex items-center space-x-1 transition-colors ${
              isPaused
                ? 'bg-emerald-600/20 border-emerald-500/40 text-emerald-300 hover:bg-emerald-600/30'
                : 'bg-slate-800 border-slate-700 text-slate-300 hover:bg-slate-700'
            }`}
            title={isPaused ? 'Resume live SSE stream' : 'Pause live SSE stream'}
          >
            {isPaused ? <Play className="w-3.5 h-3.5 fill-current" aria-hidden="true" /> : <Pause className="w-3.5 h-3.5" aria-hidden="true" />}
          </button>

          {/* Clear Buffer */}
          <button
            onClick={onClearStream}
            disabled={attempts.length === 0}
            className="p-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 border border-slate-700 text-slate-400 hover:text-rose-300 text-xs transition-colors disabled:opacity-40 disabled:pointer-events-none"
            title="Clear in-memory stream buffer"
          >
            <Trash2 className="w-3.5 h-3.5" aria-hidden="true" />
          </button>
        </div>
      </div>

      {/* Stream Table */}
      <div className="overflow-x-auto">
        <table className="w-full text-left text-xs border-collapse">
          <thead>
            <tr className="border-b border-slate-800/80 bg-slate-950/40 text-slate-400 font-mono uppercase text-[10px] tracking-wider">
              <th className="py-3 px-4">Status / Code</th>
              <th className="py-3 px-4">Attempt ID</th>
              <th className="py-3 px-4">Event ID</th>
              <th className="py-3 px-4">Attempt #</th>
              <th className="py-3 px-4">Latency</th>
              <th className="py-3 px-4">Timestamp</th>
              <th className="py-3 px-4 text-right">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800/40 font-mono">
            {filteredAttempts.length === 0 ? (
              <tr>
                <td colSpan={7} className="py-12 text-center text-slate-500">
                  <div className="flex flex-col items-center justify-center space-y-2">
                    <Activity className="w-8 h-8 text-slate-600 stroke-[1.5]" aria-hidden="true" />
                    <p className="text-sm font-sans text-slate-400">
                      {attempts.length === 0
                        ? 'Waiting for incoming webhook attempts from Go Engine...'
                        : 'No delivery attempts match your search criteria.'}
                    </p>
                    {attempts.length === 0 && (
                      <p className="text-xs font-sans text-slate-500 max-w-sm">
                        Use the simulation bar above to fire test events or connect a producer SDK to begin streaming.
                      </p>
                    )}
                  </div>
                </td>
              </tr>
            ) : (
              filteredAttempts.map((attempt) => (
                <tr
                  key={attempt.id}
                  className="hover:bg-slate-800/40 transition-colors group cursor-pointer"
                  onClick={() => onSelectAttempt(attempt)}
                >
                  <td className="py-3 px-4 whitespace-nowrap">
                    {renderStatusBadge(attempt.status, attempt.response_status)}
                  </td>
                  <td className="py-3 px-4 font-semibold text-slate-200 whitespace-nowrap">
                    {attempt.id.substring(0, 16)}...
                  </td>
                  <td className="py-3 px-4 text-indigo-300 whitespace-nowrap">
                    {attempt.event_id.substring(0, 16)}...
                  </td>
                  <td className="py-3 px-4 text-slate-300 whitespace-nowrap">
                    <span className="px-2 py-0.5 rounded bg-slate-800 border border-slate-700 text-slate-200">
                      #{attempt.attempt_number}
                    </span>
                  </td>
                  <td className="py-3 px-4 whitespace-nowrap text-cyan-400 font-medium">
                    {attempt.duration_ms} ms
                  </td>
                  <td className="py-3 px-4 text-slate-400 whitespace-nowrap">
                    <div className="flex items-center space-x-1">
                      <Clock className="w-3 h-3 text-slate-500" aria-hidden="true" />
                      <span>{new Date(attempt.executed_at).toLocaleTimeString()}</span>
                    </div>
                  </td>
                  <td className="py-3 px-4 text-right whitespace-nowrap" onClick={(e) => e.stopPropagation()}>
                    <button
                      onClick={() => onSelectAttempt(attempt)}
                      className="inline-flex items-center space-x-1.5 px-2.5 py-1 rounded bg-indigo-600/20 hover:bg-indigo-600/30 border border-indigo-500/40 text-indigo-300 text-xs font-sans font-medium transition-colors"
                      title="Inspect HMAC signature and request body"
                    >
                      <KeyRound className="w-3 h-3 text-indigo-400" aria-hidden="true" />
                      <span>Inspect HMAC</span>
                    </button>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
};
