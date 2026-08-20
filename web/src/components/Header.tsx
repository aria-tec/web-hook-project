import React, { useState } from 'react';
import {
  Activity,
  Zap,
  CheckCircle2,
  AlertTriangle,
  Flame,
  Clock,
  RefreshCw,
  Layers,
} from 'lucide-react';
import { ConnectionState, SystemStats } from '../types';

interface HeaderProps {
  connectionState: ConnectionState;
  stats: SystemStats;
  tenantId: string;
  onTenantChange: (tenant: string) => void;
  onReconnect: () => void;
  dlqCount: number;
}

export const Header: React.FC<HeaderProps> = ({
  connectionState,
  stats,
  tenantId,
  onTenantChange,
  onReconnect,
  dlqCount,
}) => {
  const [isEditingTenant, setIsEditingTenant] = useState(false);
  const [customTenant, setCustomTenant] = useState(tenantId);

  const predefinedTenants = ['tenant_alpha', 'tenant_beta', 'tenant_prod'];

  const handleCustomTenantSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (customTenant.trim()) {
      onTenantChange(customTenant.trim());
      setIsEditingTenant(false);
    }
  };

  const getStatusBadge = () => {
    switch (connectionState) {
      case 'connected':
        return (
          <div className="flex items-center space-x-2 px-3 py-1 bg-emerald-500/10 border border-emerald-500/30 rounded-full text-emerald-400 text-xs font-medium">
            <span className="relative flex h-2 w-2">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
              <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span>
            </span>
            <span>SSE LIVE STREAM</span>
          </div>
        );
      case 'connecting':
        return (
          <div className="flex items-center space-x-2 px-3 py-1 bg-amber-500/10 border border-amber-500/30 rounded-full text-amber-400 text-xs font-medium">
            <RefreshCw className="w-3 h-3 animate-spin" aria-hidden="true" />
            <span>CONNECTING...</span>
          </div>
        );
      case 'error':
      case 'disconnected':
      default:
        return (
          <button
            onClick={onReconnect}
            className="flex items-center space-x-1.5 px-3 py-1 bg-rose-500/10 hover:bg-rose-500/20 border border-rose-500/30 rounded-full text-rose-400 text-xs font-medium transition-colors"
            title="Click to reconnect SSE stream"
          >
            <span className="h-2 w-2 rounded-full bg-rose-500"></span>
            <span>OFFLINE (RETRY)</span>
          </button>
        );
    }
  };

  return (
    <header className="border-b border-slate-800/80 bg-[#0B0F17]/80 backdrop-blur-xl sticky top-0 z-40">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-3.5">
        <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
          {/* Logo & Product Title */}
          <div className="flex items-center space-x-3">
            <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-indigo-500 to-purple-600 p-0.5 shadow-lg shadow-indigo-500/20 flex items-center justify-center">
              <div className="w-full h-full bg-slate-950 rounded-[10px] flex items-center justify-center">
                <Zap className="w-5 h-5 text-indigo-400" aria-hidden="true" />
              </div>
            </div>
            <div>
              <div className="flex items-center space-x-2.5">
                <h1 className="text-lg font-bold tracking-tight text-white flex items-center">
                  Mini-Svix <span className="ml-2 text-xs font-mono px-2 py-0.5 rounded bg-indigo-950/80 text-indigo-300 border border-indigo-700/50">OPERATIONS</span>
                </h1>
                {getStatusBadge()}
              </div>
              <p className="text-xs text-slate-400 mt-0.5">
                Distributed Webhook & Event Reliability Engine
              </p>
            </div>
          </div>

          {/* Tenant Switcher & Global Actions */}
          <div className="flex items-center space-x-3 self-end md:self-auto">
            <div className="flex items-center bg-slate-900/90 border border-slate-800 rounded-lg p-1 text-xs">
              <span className="flex items-center text-slate-400 px-2 font-medium">
                <Layers className="w-3.5 h-3.5 mr-1.5 text-indigo-400" aria-hidden="true" />
                Tenant:
              </span>
              {isEditingTenant ? (
                <form onSubmit={handleCustomTenantSubmit} className="flex items-center">
                  <input
                    type="text"
                    value={customTenant}
                    onChange={(e) => setCustomTenant(e.target.value)}
                    placeholder="custom_tenant"
                    className="bg-slate-950 border border-indigo-500/50 rounded px-2 py-1 text-xs text-white focus:outline-none focus:ring-1 focus:ring-indigo-500 font-mono w-32"
                    autoFocus
                    onBlur={() => setIsEditingTenant(false)}
                  />
                </form>
              ) : (
                <div className="flex items-center space-x-1">
                  {predefinedTenants.map((t) => (
                    <button
                      key={t}
                      onClick={() => onTenantChange(t)}
                      className={`px-2 py-1 rounded transition-colors font-mono ${
                        tenantId === t
                          ? 'bg-indigo-600 text-white font-medium shadow-sm'
                          : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800'
                      }`}
                    >
                      {t}
                    </button>
                  ))}
                  {!predefinedTenants.includes(tenantId) && (
                    <button
                      onClick={() => setIsEditingTenant(true)}
                      className="px-2 py-1 rounded bg-indigo-600 text-white font-medium font-mono text-xs"
                    >
                      {tenantId}
                    </button>
                  )}
                  <button
                    onClick={() => {
                      setCustomTenant(tenantId);
                      setIsEditingTenant(true);
                    }}
                    className="px-1.5 py-1 text-slate-500 hover:text-indigo-400 rounded transition-colors text-[10px] uppercase font-semibold"
                    title="Enter custom tenant ID"
                  >
                    + Custom
                  </button>
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Telemetry Summary Cards */}
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3 mt-4 pt-3 border-t border-slate-800/60">
          <div className="bg-slate-900/40 border border-slate-800/60 rounded-lg p-2.5 flex items-center space-x-3">
            <div className="p-2 rounded-md bg-indigo-500/10 text-indigo-400">
              <Activity className="w-4 h-4" aria-hidden="true" />
            </div>
            <div>
              <div className="text-[11px] uppercase tracking-wider text-slate-400 font-medium">Attempts (Buffer)</div>
              <div className="text-lg font-bold font-mono text-white leading-tight">
                {stats.totalAttempts} <span className="text-[10px] text-slate-500 font-normal">/ 200</span>
              </div>
            </div>
          </div>

          <div className="bg-slate-900/40 border border-slate-800/60 rounded-lg p-2.5 flex items-center space-x-3">
            <div className="p-2 rounded-md bg-emerald-500/10 text-emerald-400">
              <CheckCircle2 className="w-4 h-4" aria-hidden="true" />
            </div>
            <div>
              <div className="text-[11px] uppercase tracking-wider text-slate-400 font-medium">Delivered (200 OK)</div>
              <div className="text-lg font-bold font-mono text-emerald-400 leading-tight">
                {stats.successCount}
              </div>
            </div>
          </div>

          <div className="bg-slate-900/40 border border-slate-800/60 rounded-lg p-2.5 flex items-center space-x-3">
            <div className="p-2 rounded-md bg-amber-500/10 text-amber-400">
              <AlertTriangle className="w-4 h-4" aria-hidden="true" />
            </div>
            <div>
              <div className="text-[11px] uppercase tracking-wider text-slate-400 font-medium">Retrying (500 Backoff)</div>
              <div className="text-lg font-bold font-mono text-amber-400 leading-tight">
                {stats.retryingCount}
              </div>
            </div>
          </div>

          <div className="bg-slate-900/40 border border-slate-800/60 rounded-lg p-2.5 flex items-center space-x-3">
            <div className="p-2 rounded-md bg-rose-500/10 text-rose-400">
              <Flame className="w-4 h-4" aria-hidden="true" />
            </div>
            <div>
              <div className="text-[11px] uppercase tracking-wider text-slate-400 font-medium">DLQ Active</div>
              <div className="text-lg font-bold font-mono text-rose-400 leading-tight">
                {dlqCount}
              </div>
            </div>
          </div>

          <div className="bg-slate-900/40 border border-slate-800/60 rounded-lg p-2.5 flex items-center space-x-3 col-span-2 sm:col-span-1">
            <div className="p-2 rounded-md bg-cyan-500/10 text-cyan-400">
              <Clock className="w-4 h-4" aria-hidden="true" />
            </div>
            <div>
              <div className="text-[11px] uppercase tracking-wider text-slate-400 font-medium">Avg Latency</div>
              <div className="text-lg font-bold font-mono text-cyan-400 leading-tight">
                {stats.avgDurationMs} <span className="text-xs text-slate-400">ms</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </header>
  );
};
