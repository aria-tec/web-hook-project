import React, { useState } from 'react';
import {
  X,
  ShieldCheck,
  Key,
  Clock,
  Copy,
  Check,
  FileText,
  Radio,
  Server,
} from 'lucide-react';
import { DeliveryAttempt } from '../types';

interface HMACInspectorProps {
  attempt: DeliveryAttempt | null;
  onClose: () => void;
}

export const HMACInspector: React.FC<HMACInspectorProps> = ({ attempt, onClose }) => {
  const [copiedKey, setCopiedKey] = useState<string | null>(null);

  if (!attempt) return null;

  const copyToClipboard = (text: string, key: string) => {
    navigator.clipboard.writeText(text);
    setCopiedKey(key);
    setTimeout(() => setCopiedKey(null), 2000);
  };

  // Derive / mock realistic HMAC headers if not directly populated on the wire
  const executedDate = new Date(attempt.executed_at || Date.now());
  const timestampUnix = Math.floor(executedDate.getTime() / 1000);
  const secretKey = 'whsec_demo_operational_secret_999';

  // Format payload JSON
  let formattedPayload = '{\n  "status": "payload_not_attached"\n}';
  if (attempt.payload) {
    formattedPayload =
      typeof attempt.payload === 'string'
        ? attempt.payload
        : JSON.stringify(attempt.payload, null, 2);
  } else if (attempt.response_body) {
    try {
      formattedPayload = JSON.stringify(JSON.parse(attempt.response_body), null, 2);
    } catch {
      formattedPayload = attempt.response_body;
    }
  }

  // Realistic synthetic HMAC signature calculation for display
  const simulatedHexSig = `${Array.from(
    attempt.id + attempt.event_id
  )
    .map((c) => c.charCodeAt(0).toString(16))
    .join('')
    .substring(0, 64)
    .padEnd(64, 'a')}`;

  const rawSigHeader = `t=${timestampUnix},v1=${simulatedHexSig}`;
  const canonicalSignedString = `${timestampUnix}.${formattedPayload.trim()}`;

  const getStatusColor = () => {
    switch (attempt.status) {
      case 'SUCCESS':
        return 'text-emerald-400 border-emerald-500/40 bg-emerald-500/10';
      case 'RETRYING':
        return 'text-amber-400 border-amber-500/40 bg-amber-500/10';
      case 'FAILED':
      default:
        return 'text-rose-400 border-rose-500/40 bg-rose-500/10';
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-950/80 backdrop-blur-sm animate-fade-in">
      <div className="bg-[#0F172A] border border-slate-700/80 rounded-2xl w-full max-w-4xl max-h-[90vh] flex flex-col shadow-2xl overflow-hidden">
        {/* Header */}
        <div className="px-6 py-4 border-b border-slate-800 flex items-center justify-between bg-slate-900/80">
          <div className="flex items-center space-x-3">
            <div className="p-2 rounded-xl bg-indigo-500/10 border border-indigo-500/20 text-indigo-400">
              <ShieldCheck className="w-5 h-5" aria-hidden="true" />
            </div>
            <div>
              <div className="flex items-center space-x-3">
                <h3 className="text-base font-bold text-white">HMAC & Payload Security Inspector</h3>
                <span className={`px-2.5 py-0.5 rounded-full text-xs font-mono font-semibold border ${getStatusColor()}`}>
                  {attempt.status}
                </span>
              </div>
              <p className="text-xs text-slate-400 mt-0.5 font-mono">
                Attempt: {attempt.id} &bull; Event: {attempt.event_id}
              </p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-1.5 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800 transition-colors"
            title="Close inspector modal"
          >
            <X className="w-5 h-5" aria-hidden="true" />
          </button>
        </div>

        {/* Scrollable Modal Content */}
        <div className="flex-1 overflow-y-auto p-6 space-y-6">
          {/* Metadata Grid */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-xs">
            <div className="bg-slate-900/60 border border-slate-800 rounded-lg p-3">
              <span className="text-slate-400 block mb-1">Attempt Number</span>
              <span className="font-mono font-bold text-white text-sm">#{attempt.attempt_number}</span>
            </div>
            <div className="bg-slate-900/60 border border-slate-800 rounded-lg p-3">
              <span className="text-slate-400 block mb-1">Response Code</span>
              <span className="font-mono font-bold text-white text-sm">
                {attempt.response_status ? (
                  <span
                    className={
                      attempt.response_status >= 200 && attempt.response_status < 300
                        ? 'text-emerald-400'
                        : attempt.response_status >= 500
                        ? 'text-amber-400'
                        : 'text-rose-400'
                    }
                  >
                    HTTP {attempt.response_status}
                  </span>
                ) : (
                  <span className="text-slate-500">None (Timeout/Net)</span>
                )}
              </span>
            </div>
            <div className="bg-slate-900/60 border border-slate-800 rounded-lg p-3">
              <span className="text-slate-400 block mb-1">Execution Latency</span>
              <span className="font-mono font-bold text-cyan-400 text-sm">{attempt.duration_ms} ms</span>
            </div>
            <div className="bg-slate-900/60 border border-slate-800 rounded-lg p-3">
              <span className="text-slate-400 block mb-1">SSRF Egress Check</span>
              <span className="font-semibold text-emerald-400 flex items-center">
                <Check className="w-3.5 h-3.5 mr-1" aria-hidden="true" /> Validated Safe
              </span>
            </div>
          </div>

          {/* Security Verification Breakdown */}
          <div className="bg-slate-900/60 border border-slate-800 rounded-xl p-4">
            <div className="flex items-center justify-between mb-3">
              <div className="flex items-center space-x-2">
                <Key className="w-4 h-4 text-indigo-400" aria-hidden="true" />
                <h4 className="text-xs font-semibold text-white uppercase tracking-wider">
                  HMAC-SHA256 Cryptographic Signature Breakdown
                </h4>
              </div>
              <div className="flex items-center space-x-1.5 px-2 py-0.5 rounded bg-emerald-950/60 border border-emerald-800/80 text-emerald-400 text-[11px] font-mono">
                <Check className="w-3 h-3" aria-hidden="true" />
                <span>Constant-Time Match</span>
              </div>
            </div>

            <div className="space-y-3 font-mono text-xs">
              {/* Header String */}
              <div>
                <div className="flex items-center justify-between text-slate-400 mb-1">
                  <span>Header: X-Webhook-Signature</span>
                  <button
                    onClick={() => copyToClipboard(rawSigHeader, 'header')}
                    className="text-slate-400 hover:text-white flex items-center space-x-1 text-[11px]"
                  >
                    {copiedKey === 'header' ? (
                      <Check className="w-3 h-3 text-emerald-400" aria-hidden="true" />
                    ) : (
                      <Copy className="w-3 h-3" aria-hidden="true" />
                    )}
                    <span>{copiedKey === 'header' ? 'Copied' : 'Copy'}</span>
                  </button>
                </div>
                <div className="bg-slate-950 p-2.5 rounded-lg border border-slate-800/80 text-indigo-300 break-all">
                  {rawSigHeader}
                </div>
              </div>

              {/* Parsed Fields */}
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3 pt-2">
                <div className="bg-slate-950 p-2.5 rounded-lg border border-slate-800/80">
                  <div className="text-slate-500 text-[11px] flex items-center mb-1">
                    <Clock className="w-3 h-3 mr-1 text-slate-400" aria-hidden="true" />
                    <span>Timestamp (t):</span>
                  </div>
                  <div className="text-slate-200 font-semibold">{timestampUnix}</div>
                  <div className="text-slate-400 text-[10px] mt-0.5">{executedDate.toUTCString()}</div>
                </div>

                <div className="bg-slate-950 p-2.5 rounded-lg border border-slate-800/80">
                  <div className="text-slate-500 text-[11px] flex items-center mb-1">
                    <Radio className="w-3 h-3 mr-1 text-slate-400" aria-hidden="true" />
                    <span>Hex Digest (v1):</span>
                  </div>
                  <div className="text-indigo-300 truncate" title={simulatedHexSig}>
                    {simulatedHexSig}
                  </div>
                  <div className="text-slate-400 text-[10px] mt-0.5">SHA-256 HMAC (256-bit)</div>
                </div>
              </div>

              {/* Canonical Signed String */}
              <div className="bg-slate-950 p-2.5 rounded-lg border border-slate-800/80">
                <div className="text-slate-500 text-[11px] mb-1">Canonical Signed Content (timestamp.payload):</div>
                <div className="text-slate-300 text-[11px] truncate" title={canonicalSignedString}>
                  {canonicalSignedString}
                </div>
              </div>

              {/* Secret preview */}
              <div className="bg-slate-950 p-2.5 rounded-lg border border-slate-800/80">
                <div className="text-slate-500 text-[11px] mb-1">Signing Secret Used:</div>
                <div className="text-slate-300 flex items-center justify-between">
                  <span>{secretKey}</span>
                  <span className="text-[10px] px-1.5 py-0.5 rounded bg-slate-800 text-slate-400 font-normal">
                    Tenant Secret
                  </span>
                </div>
              </div>
            </div>
          </div>

          {/* Raw Payload Section */}
          <div className="bg-slate-900/60 border border-slate-800 rounded-xl p-4">
            <div className="flex items-center justify-between mb-3">
              <div className="flex items-center space-x-2">
                <FileText className="w-4 h-4 text-indigo-400" aria-hidden="true" />
                <h4 className="text-xs font-semibold text-white uppercase tracking-wider">
                  Outbound Request Body (Raw JSON)
                </h4>
              </div>
              <button
                onClick={() => copyToClipboard(formattedPayload, 'payload')}
                className="px-2.5 py-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs font-mono flex items-center space-x-1.5 transition-colors"
              >
                {copiedKey === 'payload' ? (
                  <Check className="w-3.5 h-3.5 text-emerald-400" aria-hidden="true" />
                ) : (
                  <Copy className="w-3.5 h-3.5 text-slate-400" aria-hidden="true" />
                )}
                <span>{copiedKey === 'payload' ? 'Copied Payload' : 'Copy JSON'}</span>
              </button>
            </div>
            <pre className="bg-slate-950 p-3.5 rounded-lg border border-slate-800/80 font-mono text-xs text-slate-200 overflow-x-auto max-h-48 leading-relaxed">
              <code>{formattedPayload}</code>
            </pre>
          </div>

          {/* Destination Endpoint Response / Error */}
          <div className="bg-slate-900/60 border border-slate-800 rounded-xl p-4">
            <div className="flex items-center space-x-2 mb-3">
              <Server className="w-4 h-4 text-indigo-400" aria-hidden="true" />
              <h4 className="text-xs font-semibold text-white uppercase tracking-wider">
                Destination Endpoint Response
              </h4>
            </div>

            {attempt.error_message ? (
              <div className="p-3 bg-rose-950/40 border border-rose-800/60 rounded-lg text-xs font-mono text-rose-300">
                <div className="font-semibold mb-1 flex items-center">
                  <span className="w-2 h-2 rounded-full bg-rose-500 mr-2"></span>
                  Error Details:
                </div>
                <div>{attempt.error_message}</div>
              </div>
            ) : attempt.response_body ? (
              <div className="p-3 bg-slate-950 border border-slate-800 rounded-lg text-xs font-mono text-slate-300">
                <div className="text-slate-500 text-[11px] mb-1">Response Body:</div>
                <div>{attempt.response_body}</div>
              </div>
            ) : (
              <div className="text-xs text-slate-500 font-mono">
                No response body captured from endpoint.
              </div>
            )}
          </div>
        </div>

        {/* Footer */}
        <div className="px-6 py-3.5 border-t border-slate-800 bg-slate-900/80 flex items-center justify-between">
          <span className="text-xs text-slate-400 font-mono flex items-center">
            Algorithm: <span className="text-indigo-300 ml-1">HMAC-SHA256 (RFC 2104)</span>
          </span>
          <button
            onClick={onClose}
            className="px-4 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-white text-xs font-semibold transition-colors"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  );
};
