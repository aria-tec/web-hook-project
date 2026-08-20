import React, { useState } from 'react';
import {
  Play,
  CheckCircle,
  AlertTriangle,
  Flame,
  Zap,
  Loader2,
  Settings,
  Check,
} from 'lucide-react';

interface SimulationBarProps {
  tenantId: string;
  baseUrl?: string;
  onEventSent?: () => void;
}

export const SimulationBar: React.FC<SimulationBarProps> = ({
  tenantId,
  baseUrl = '',
  onEventSent,
}) => {
  const [loadingAction, setLoadingAction] = useState<string | null>(null);
  const [statusMessage, setStatusMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null);
  const [isProvisioning, setIsProvisioning] = useState<boolean>(false);
  const [endpointsProvisioned, setEndpointsProvisioned] = useState<boolean>(false);

  const showToast = (type: 'success' | 'error', text: string) => {
    setStatusMessage({ type, text });
    setTimeout(() => {
      setStatusMessage(null);
    }, 4000);
  };

  // Helper to ensure target endpoints exist for this tenant
  const ensureEndpoints = async () => {
    try {
      setIsProvisioning(true);
      const secret = 'whsec_demo_operational_secret_999';
      const endpoints = [
        { url: 'http://mock-receiver:9090/webhook/success', secret, rate_limit: 100 },
        { url: 'http://mock-receiver:9090/webhook/flaky', secret, rate_limit: 50 },
        { url: 'http://mock-receiver:9090/webhook/poison', secret, rate_limit: 50 },
        { url: 'http://localhost:9090/webhook/success', secret, rate_limit: 100 },
        { url: 'http://localhost:9090/webhook/flaky', secret, rate_limit: 50 },
        { url: 'http://localhost:9090/webhook/poison', secret, rate_limit: 50 },
      ];

      for (const ep of endpoints) {
        await fetch(`${baseUrl}/api/v1/endpoints`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Tenant-ID': tenantId,
          },
          body: JSON.stringify(ep),
        });
      }
      setEndpointsProvisioned(true);
      showToast('success', `Provisioned mock receiver endpoints for ${tenantId}`);
    } catch (err: any) {
      showToast('error', `Provisioning failed: ${err.message}`);
    } finally {
      setIsProvisioning(false);
    }
  };

  const sendEvent = async (
    eventType: string,
    payload: Record<string, any>,
    idempotencyKeyPrefix: string,
    actionKey: string
  ) => {
    setLoadingAction(actionKey);
    try {
      const idempKey = `${idempotencyKeyPrefix}_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
      const response = await fetch(`${baseUrl}/api/v1/events`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Tenant-ID': tenantId,
          'X-Idempotency-Key': idempKey,
        },
        body: JSON.stringify({
          event_type: eventType,
          payload,
        }),
      });

      if (!response.ok) {
        const text = await response.text();
        throw new Error(`Engine rejected event (${response.status}): ${text}`);
      }

      const data = await response.json();
      showToast('success', `Event queued: ${data.id} (${eventType})`);
      onEventSent?.();
    } catch (err: any) {
      showToast('error', err.message || 'Failed to send event');
    } finally {
      setLoadingAction(null);
    }
  };

  const handleSendNormal = () => {
    sendEvent(
      'order.created',
      {
        order_id: `ord_${Math.floor(100000 + Math.random() * 900000)}`,
        amount: 14999,
        currency: 'USD',
        customer: {
          id: 'cust_alpha_01',
          name: 'Jane Doe',
          email: 'jane@example.com',
        },
        items: [{ sku: 'PRO-100', qty: 1, price: 14999 }],
      },
      'idemp_normal',
      'normal'
    );
  };

  const handleSendFlaky = () => {
    sendEvent(
      'invoice.sync',
      {
        invoice_id: `inv_${Math.floor(10000 + Math.random() * 90000)}`,
        flaky_simulation: true,
        description: 'Endpoint will respond 500 on 1st and 2nd tries, 200 on 3rd try',
        timestamp: new Date().toISOString(),
      },
      'idemp_flaky',
      'flaky'
    );
  };

  const handleSendPoison = () => {
    sendEvent(
      'payment.poison_pill',
      {
        transaction_id: `tx_${Math.random().toString(36).substring(2, 9)}`,
        poison_pill: true,
        error_trigger: 'simulate_400_bad_request',
        instructions: 'Destination endpoint returns 400 Bad Request to route straight to DLQ',
      },
      'idemp_poison',
      'poison'
    );
  };

  const handleSendBurst = async () => {
    setLoadingAction('burst');
    try {
      showToast('success', 'Firing 5 burst simulation events...');
      // 3 normal, 1 flaky, 1 poison
      await Promise.all([
        sendEvent('order.created', { order_id: `ord_burst_1`, amount: 4900 }, 'idemp_burst_1', 'burst_internal'),
        sendEvent('payment.captured', { payment_id: `pay_burst_2`, amount: 8900 }, 'idemp_burst_2', 'burst_internal'),
        sendEvent('invoice.sync', { invoice_id: `inv_burst_3`, flaky: true }, 'idemp_burst_3', 'burst_internal'),
        sendEvent('payment.poison_pill', { poison: true }, 'idemp_burst_4', 'burst_internal'),
        sendEvent('customer.upgraded', { tier: 'enterprise' }, 'idemp_burst_5', 'burst_internal'),
      ]);
      showToast('success', 'Burst of 5 demo events ingested successfully!');
      onEventSent?.();
    } catch (err: any) {
      showToast('error', err.message || 'Burst simulation error');
    } finally {
      setLoadingAction(null);
    }
  };

  return (
    <div className="bg-slate-900/60 border border-slate-800/80 rounded-xl p-4 mb-6 shadow-glass backdrop-blur-md">
      <div className="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-4">
        {/* Left: Section Info */}
        <div className="flex items-center space-x-3">
          <div className="p-2 rounded-lg bg-indigo-500/10 border border-indigo-500/20 text-indigo-400">
            <Zap className="w-5 h-5" aria-hidden="true" />
          </div>
          <div>
            <h2 className="text-sm font-semibold text-white tracking-wide flex items-center">
              Operational Simulation Control Bar
              <span className="ml-2 text-[10px] uppercase font-mono px-2 py-0.5 rounded bg-slate-800 text-slate-300 border border-slate-700">
                Tenant: {tenantId}
              </span>
            </h2>
            <p className="text-xs text-slate-400">
              Fire test events with realistic payload schemas directly into the Outbox Relay & Dispatcher.
            </p>
          </div>
        </div>

        {/* Right: Simulation Action Buttons */}
        <div className="flex flex-wrap items-center gap-2.5">
          {/* Send Normal 200 OK */}
          <button
            onClick={handleSendNormal}
            disabled={loadingAction !== null}
            className="flex items-center space-x-2 px-3.5 py-2 rounded-lg bg-emerald-600/20 hover:bg-emerald-600/30 border border-emerald-500/40 text-emerald-300 text-xs font-semibold shadow-sm transition-all hover:scale-[1.02] active:scale-[0.98] disabled:opacity-50 disabled:pointer-events-none"
          >
            {loadingAction === 'normal' ? (
              <Loader2 className="w-3.5 h-3.5 animate-spin" aria-hidden="true" />
            ) : (
              <CheckCircle className="w-3.5 h-3.5 text-emerald-400" aria-hidden="true" />
            )}
            <span>Normal (200 OK)</span>
          </button>

          {/* Simulate 500 Flaky */}
          <button
            onClick={handleSendFlaky}
            disabled={loadingAction !== null}
            className="flex items-center space-x-2 px-3.5 py-2 rounded-lg bg-amber-600/20 hover:bg-amber-600/30 border border-amber-500/40 text-amber-300 text-xs font-semibold shadow-sm transition-all hover:scale-[1.02] active:scale-[0.98] disabled:opacity-50 disabled:pointer-events-none"
          >
            {loadingAction === 'flaky' ? (
              <Loader2 className="w-3.5 h-3.5 animate-spin" aria-hidden="true" />
            ) : (
              <AlertTriangle className="w-3.5 h-3.5 text-amber-400" aria-hidden="true" />
            )}
            <span>Flaky (500 Retry)</span>
          </button>

          {/* Simulate 400 Poison Pill */}
          <button
            onClick={handleSendPoison}
            disabled={loadingAction !== null}
            className="flex items-center space-x-2 px-3.5 py-2 rounded-lg bg-rose-600/20 hover:bg-rose-600/30 border border-rose-500/40 text-rose-300 text-xs font-semibold shadow-sm transition-all hover:scale-[1.02] active:scale-[0.98] disabled:opacity-50 disabled:pointer-events-none"
          >
            {loadingAction === 'poison' ? (
              <Loader2 className="w-3.5 h-3.5 animate-spin" aria-hidden="true" />
            ) : (
              <Flame className="w-3.5 h-3.5 text-rose-400" aria-hidden="true" />
            )}
            <span>Poison Pill (400 DLQ)</span>
          </button>

          {/* Fire 5 Demo Burst */}
          <button
            onClick={handleSendBurst}
            disabled={loadingAction !== null}
            className="flex items-center space-x-2 px-3.5 py-2 rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-semibold shadow-md shadow-indigo-600/30 transition-all hover:scale-[1.02] active:scale-[0.98] disabled:opacity-50 disabled:pointer-events-none"
          >
            {loadingAction === 'burst' ? (
              <Loader2 className="w-3.5 h-3.5 animate-spin" aria-hidden="true" />
            ) : (
              <Play className="w-3.5 h-3.5 fill-current" aria-hidden="true" />
            )}
            <span>Burst (5 Demo)</span>
          </button>

          {/* Auto-provision mock endpoints */}
          <button
            onClick={ensureEndpoints}
            disabled={isProvisioning}
            className="flex items-center space-x-1.5 px-2.5 py-2 rounded-lg bg-slate-800/80 hover:bg-slate-800 border border-slate-700 text-slate-300 text-xs transition-colors"
            title="Register Mock Receiver sink endpoints for this tenant"
          >
            {isProvisioning ? (
              <Loader2 className="w-3.5 h-3.5 animate-spin" aria-hidden="true" />
            ) : endpointsProvisioned ? (
              <Check className="w-3.5 h-3.5 text-emerald-400" aria-hidden="true" />
            ) : (
              <Settings className="w-3.5 h-3.5 text-slate-400" aria-hidden="true" />
            )}
            <span className="text-[11px] font-medium">Provision Sink</span>
          </button>
        </div>
      </div>

      {/* Toast Feedback Notification */}
      {statusMessage && (
        <div
          className={`mt-3 px-3 py-2 rounded-lg text-xs font-mono flex items-center justify-between border animate-fade-in ${
            statusMessage.type === 'success'
              ? 'bg-emerald-950/60 border-emerald-800/60 text-emerald-300'
              : 'bg-rose-950/60 border-rose-800/60 text-rose-300'
          }`}
        >
          <div className="flex items-center space-x-2">
            {statusMessage.type === 'success' ? (
              <CheckCircle className="w-4 h-4 text-emerald-400 flex-shrink-0" aria-hidden="true" />
            ) : (
              <AlertTriangle className="w-4 h-4 text-rose-400 flex-shrink-0" aria-hidden="true" />
            )}
            <span>{statusMessage.text}</span>
          </div>
          <button
            onClick={() => setStatusMessage(null)}
            className="text-slate-400 hover:text-white ml-2 text-xs"
          >
            ✕
          </button>
        </div>
      )}
    </div>
  );
};
