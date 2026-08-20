import { useState } from 'react';
import {
  Activity,
  Flame,
  BookOpen,
  Shield,
  Cpu,
} from 'lucide-react';
import { useEventStream } from './hooks/useEventStream';
import { useDLQ } from './hooks/useDLQ';
import { Header } from './components/Header';
import { SimulationBar } from './components/SimulationBar';
import { LiveStream } from './components/LiveStream';
import { DLQManager } from './components/DLQManager';
import { SDKGuide } from './components/SDKGuide';
import { HMACInspector } from './components/HMACInspector';
import { DeliveryAttempt } from './types';

export function App() {
  const [tenantId, setTenantId] = useState<string>('tenant_alpha');
  const [activeTab, setActiveTab] = useState<'stream' | 'dlq' | 'sdk'>('stream');
  const [inspectedAttempt, setInspectedAttempt] = useState<DeliveryAttempt | null>(null);

  // Hook 1: SSE Delivery Attempt Ring Buffer (200 items max)
  const {
    attempts,
    connectionState,
    isPaused,
    stats,
    connect,
    clearStream,
    togglePause,
  } = useEventStream({
    url: '/api/v1/events/stream',
    autoConnect: true,
  });

  // Hook 2: DLQ Management
  const dlq = useDLQ({
    tenantId,
    autoFetch: true,
  });

  return (
    <div className="min-h-screen bg-[#0B0F17] text-slate-100 flex flex-col selection:bg-indigo-500/30 selection:text-indigo-200">
      {/* Top Header */}
      <Header
        connectionState={connectionState}
        stats={stats}
        tenantId={tenantId}
        onTenantChange={(t) => setTenantId(t)}
        onReconnect={connect}
        dlqCount={dlq.events.length}
      />

      {/* Main Content Area */}
      <main className="flex-1 max-w-7xl w-full mx-auto px-4 sm:px-6 lg:px-8 py-6">
        {/* Interactive Simulation Bar */}
        <SimulationBar
          tenantId={tenantId}
          onEventSent={() => {
            // Give outbox relay half a second then refresh DLQ in case of poison pills
            setTimeout(() => {
              dlq.fetchDLQ();
            }, 600);
          }}
        />

        {/* Tab Navigation */}
        <div className="flex items-center justify-between border-b border-slate-800 mb-6">
          <div className="flex items-center space-x-2">
            <button
              onClick={() => setActiveTab('stream')}
              className={`flex items-center space-x-2 py-3 px-4 text-xs font-semibold uppercase tracking-wider border-b-2 transition-all ${
                activeTab === 'stream'
                  ? 'border-indigo-500 text-indigo-400 bg-indigo-500/5'
                  : 'border-transparent text-slate-400 hover:text-slate-200 hover:border-slate-700'
              }`}
            >
              <Activity className="w-4 h-4 text-indigo-400" aria-hidden="true" />
              <span>Live Delivery Stream</span>
              <span className="ml-1.5 px-2 py-0.5 rounded-full text-[10px] font-mono bg-slate-800 text-slate-300">
                {attempts.length}
              </span>
            </button>

            <button
              onClick={() => setActiveTab('dlq')}
              className={`flex items-center space-x-2 py-3 px-4 text-xs font-semibold uppercase tracking-wider border-b-2 transition-all ${
                activeTab === 'dlq'
                  ? 'border-rose-500 text-rose-400 bg-rose-500/5'
                  : 'border-transparent text-slate-400 hover:text-slate-200 hover:border-slate-700'
              }`}
            >
              <Flame className="w-4 h-4 text-rose-400" aria-hidden="true" />
              <span>DLQ Recovery Center</span>
              {dlq.events.length > 0 && (
                <span className="ml-1.5 px-2 py-0.5 rounded-full text-[10px] font-mono bg-rose-500/20 text-rose-300 border border-rose-500/30 font-bold">
                  {dlq.events.length}
                </span>
              )}
            </button>

            <button
              onClick={() => setActiveTab('sdk')}
              className={`flex items-center space-x-2 py-3 px-4 text-xs font-semibold uppercase tracking-wider border-b-2 transition-all ${
                activeTab === 'sdk'
                  ? 'border-indigo-500 text-indigo-400 bg-indigo-500/5'
                  : 'border-transparent text-slate-400 hover:text-slate-200 hover:border-slate-700'
              }`}
            >
              <BookOpen className="w-4 h-4 text-indigo-400" aria-hidden="true" />
              <span>Developer SDKs</span>
            </button>
          </div>

          <div className="hidden sm:flex items-center space-x-3 text-xs text-slate-500 font-mono">
            <span className="flex items-center">
              <Shield className="w-3.5 h-3.5 mr-1 text-emerald-400" aria-hidden="true" />
              HMAC-SHA256
            </span>
            <span>&bull;</span>
            <span className="flex items-center">
              <Cpu className="w-3.5 h-3.5 mr-1 text-indigo-400" aria-hidden="true" />
              Outbox Relay
            </span>
          </div>
        </div>

        {/* Tab Panels */}
        {activeTab === 'stream' && (
          <LiveStream
            attempts={attempts}
            isPaused={isPaused}
            onTogglePause={togglePause}
            onClearStream={clearStream}
            onSelectAttempt={(att) => setInspectedAttempt(att)}
          />
        )}

        {activeTab === 'dlq' && (
          <DLQManager
            events={dlq.events}
            selectedIds={dlq.selectedIds}
            isLoading={dlq.isLoading}
            isReplaying={dlq.isReplaying}
            error={dlq.error}
            lastReplayedResult={dlq.lastReplayedResult}
            onRefresh={dlq.fetchDLQ}
            onToggleSelect={dlq.toggleSelect}
            onSelectAll={dlq.selectAll}
            onClearSelection={dlq.clearSelection}
            onReplaySelected={dlq.replaySelected}
            onReplayAll={dlq.replayAll}
            tenantId={tenantId}
          />
        )}

        {activeTab === 'sdk' && <SDKGuide />}
      </main>

      {/* Deep HMAC & Payload Inspector Modal */}
      {inspectedAttempt && (
        <HMACInspector
          attempt={inspectedAttempt}
          onClose={() => setInspectedAttempt(null)}
        />
      )}

      {/* Footer */}
      <footer className="border-t border-slate-800/80 bg-[#0B0F17]/90 py-4 mt-8">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 flex flex-col sm:flex-row items-center justify-between gap-2 text-xs text-slate-500">
          <div className="flex items-center space-x-2">
            <span className="font-semibold text-slate-300">Mini-Svix Web Reliability Suite</span>
            <span>&bull;</span>
            <span className="font-mono">v1.0.0</span>
          </div>
          <div className="flex items-center space-x-4">
            <span className="flex items-center text-slate-400 font-mono">
              <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 mr-1.5"></span>
              PostgreSQL Outbox + Redis Streams
            </span>
          </div>
        </div>
      </footer>
    </div>
  );
}

export default App;
