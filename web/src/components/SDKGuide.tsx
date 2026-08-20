import React, { useState } from 'react';
import {
  Terminal,
  Copy,
  Check,
  FileCode2,
  Cpu,
  BookOpen,
} from 'lucide-react';

export const SDKGuide: React.FC = () => {
  const [activeLang, setActiveLang] = useState<'typescript' | 'go' | 'curl'>('typescript');
  const [copiedKey, setCopiedKey] = useState<string | null>(null);

  const copyToClipboard = (code: string, key: string) => {
    navigator.clipboard.writeText(code);
    setCopiedKey(key);
    setTimeout(() => setCopiedKey(null), 2000);
  };

  const snippets = {
    typescript: {
      install: `npm install @minisvix/client`,
      producer: `import { WebhookClient } from "@minisvix/client";

// Initialize Producer Client with tenant isolation
const client = new WebhookClient({
  baseUrl: "http://localhost:8080",
  tenantId: "tenant_alpha",
  apiKey: "optional_bearer_token"
});

// Publish event with automatic idempotency key & transactional outbox
const event = await client.publish(
  "order.created",
  {
    orderId: "ord_98765",
    amount: 15000,
    currency: "USD",
    customer: { id: "cust_123", email: "alex@example.com" }
  },
  {
    idempotencyKey: "idemp_order_98765_v1"
  }
);

console.log("Ingested Event ID:", event.id); // "evt_..."
console.log("Status:", event.status);         // "PENDING"`,
      consumer: `import { WebhookSignature } from "@minisvix/client";

// Express / Next.js / Cloudflare Worker webhook receiver handler
app.post("/webhook", async (req, res) => {
  const rawBody = req.rawBody; // Ensure raw body string or buffer is preserved
  const signatureHeader = req.headers["x-webhook-signature"];
  const secret = process.env.WEBHOOK_SECRET || "whsec_...";

  // Verify HMAC-SHA256 in constant time with 5-minute freshness tolerance
  const isValid = await WebhookSignature.verify({
    payload: rawBody,
    header: signatureHeader,
    secret: secret,
    toleranceSeconds: 300 // 5 minutes tolerance
  });

  if (!isValid) {
    return res.status(401).json({ error: "Invalid or expired signature" });
  }

  const data = JSON.parse(rawBody);
  console.log("Received valid webhook payload:", data);
  res.status(200).json({ received: true });
});`,
      dlq: `// List dead-lettered events
const dlqEvents = await client.listDLQ({ limit: 50, offset: 0 });
console.log(\`Found \${dlqEvents.length} dead-lettered events.\`);

// 1-Click Replay with fresh signature recalculation
const replayResult = await client.replayDLQ(["evt_failed_1", "evt_failed_2"]);
console.log(\`Replayed \${replayResult.replayed_count} events.\`);`,
    },
    go: {
      install: `go get web-hook-project/sdk/go/webhookclient`,
      producer: `package main

import (
    "context"
    "fmt"
    "log"
    "web-hook-project/sdk/go/webhookclient"
)

func main() {
    // Construct zero-dependency Go client
    client := webhookclient.NewClient("http://localhost:8080", "tenant_alpha")

    // Publish event
    payload := map[string]any{
        "order_id": "ord_555",
        "amount":   25000,
        "currency": "USD",
    }

    result, err := client.Publish(
        context.Background(),
        "payment.succeeded",
        payload,
        webhookclient.WithIdempotencyKey("idemp_payment_555"),
    )
    if err != nil {
        log.Fatalf("Publish failed: %v", err)
    }

    fmt.Printf("Queued Event ID: %s (Status: %s)\\n", result.ID, result.Status)
}`,
      consumer: `package main

import (
    "fmt"
    "io"
    "net/http"
    "web-hook-project/sdk/go/webhookclient"
)

func webhookHandler(w http.ResponseWriter, r *http.Request) {
    rawPayload, err := io.ReadAll(r.Body)
    if err != nil {
        http.Error(w, "Cannot read body", http.StatusBadRequest)
        return
    }

    secret := "whsec_..."
    sigHeader := r.Header.Get("X-Webhook-Signature")

    // Constant-time HMAC-SHA256 verification (5 min tolerance)
    if !webhookclient.VerifySignature(secret, sigHeader, rawPayload, 300) {
        http.Error(w, "Invalid webhook signature", http.StatusUnauthorized)
        return
    }

    w.WriteHeader(http.StatusOK)
    w.Write([]byte(\`{"received":true}\`))
}`,
      dlq: `// List DLQ items
events, err := client.ListDLQ(context.Background(), 50, 0)
if err != nil {
    log.Fatal(err)
}

// Batch Replay
replayed, err := client.ReplayDLQ(context.Background(), []string{"evt_abc", "evt_def"})
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Replayed %d events\\n", replayed.ReplayedCount)`,
    },
    curl: {
      install: `# cURL / REST API Direct Integration`,
      producer: `# 1. Register Webhook Endpoint
curl -X POST http://localhost:8080/api/v1/endpoints \\
  -H "Content-Type: application/json" \\
  -H "X-Tenant-ID: tenant_alpha" \\
  -d '{
    "url": "http://localhost:9090/webhook/success",
    "secret": "whsec_secret_key_123",
    "rate_limit": 100
  }'

# 2. Ingest Event with Idempotency
curl -X POST http://localhost:8080/api/v1/events \\
  -H "Content-Type: application/json" \\
  -H "X-Tenant-ID: tenant_alpha" \\
  -H "X-Idempotency-Key: idemp_curl_test_01" \\
  -d '{
    "event_type": "user.registered",
    "payload": {
      "user_id": "usr_999",
      "email": "dev@minisvix.local"
    }
  }'`,
      consumer: `# Inbound Webhook Headers delivered to your destination endpoint:
# X-Webhook-ID: evt_01a2b3...
# X-Webhook-Timestamp: 1724138400
# X-Webhook-Signature: t=1724138400,v1=5d41402abc4b2a76b9719d911017c592...

# To verify manually via OpenSSL / bash:
TIMESTAMP="1724138400"
SECRET="whsec_secret_key_123"
PAYLOAD='{"user_id":"usr_999","email":"dev@minisvix.local"}'

# Sign format: <timestamp>.<payload>
echo -n "\${TIMESTAMP}.\${PAYLOAD}" | openssl dgst -sha256 -hmac "\${SECRET}"`,
      dlq: `# 1. List DLQ Events
curl -X GET http://localhost:8080/api/v1/dlq \\
  -H "X-Tenant-ID: tenant_alpha"

# 2. Replay DLQ Events
curl -X POST http://localhost:8080/api/v1/dlq/replay \\
  -H "Content-Type: application/json" \\
  -H "X-Tenant-ID: tenant_alpha" \\
  -d '{
    "event_ids": ["evt_failed_01", "evt_failed_02"]
  }'`,
    },
  };

  const currentSnippets = snippets[activeLang];

  return (
    <div className="space-y-6">
      {/* Overview Card */}
      <div className="bg-slate-900/60 border border-slate-800/80 rounded-xl p-5 shadow-glass backdrop-blur-md">
        <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
          <div className="flex items-start space-x-3">
            <div className="p-2.5 rounded-xl bg-indigo-500/10 border border-indigo-500/20 text-indigo-400">
              <BookOpen className="w-6 h-6" aria-hidden="true" />
            </div>
            <div>
              <h2 className="text-base font-bold text-white tracking-tight">
                Developer SDKs & Integration Guide
              </h2>
              <p className="text-xs text-slate-400 mt-1 max-w-2xl">
                Zero-dependency idiomatic client libraries for TypeScript/Node.js and Go. Includes built-in constant-time HMAC signature verifiers matching the Go engine byte-for-byte.
              </p>
            </div>
          </div>

          {/* Language Switcher */}
          <div className="flex items-center bg-slate-950 p-1 rounded-xl border border-slate-800 text-xs">
            <button
              onClick={() => setActiveLang('typescript')}
              className={`flex items-center space-x-2 px-3.5 py-1.5 rounded-lg font-medium transition-all ${
                activeLang === 'typescript'
                  ? 'bg-indigo-600 text-white shadow-md'
                  : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              <FileCode2 className="w-4 h-4 text-cyan-300" aria-hidden="true" />
              <span>TypeScript / Node</span>
            </button>
            <button
              onClick={() => setActiveLang('go')}
              className={`flex items-center space-x-2 px-3.5 py-1.5 rounded-lg font-medium transition-all ${
                activeLang === 'go'
                  ? 'bg-indigo-600 text-white shadow-md'
                  : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              <Cpu className="w-4 h-4 text-cyan-400" aria-hidden="true" />
              <span>Go SDK</span>
            </button>
            <button
              onClick={() => setActiveLang('curl')}
              className={`flex items-center space-x-2 px-3.5 py-1.5 rounded-lg font-medium transition-all ${
                activeLang === 'curl'
                  ? 'bg-indigo-600 text-white shadow-md'
                  : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              <Terminal className="w-4 h-4 text-emerald-400" aria-hidden="true" />
              <span>cURL / REST API</span>
            </button>
          </div>
        </div>
      </div>

      {/* Code Sections */}
      <div className="grid grid-cols-1 gap-6">
        {/* Step 1: Install */}
        <div className="bg-slate-900/60 border border-slate-800/80 rounded-xl p-5 shadow-glass backdrop-blur-md">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-xs font-bold uppercase tracking-wider text-slate-300 flex items-center">
              <span className="w-5 h-5 rounded-full bg-indigo-500/20 text-indigo-400 flex items-center justify-center text-[10px] mr-2">
                1
              </span>
              Package Installation
            </h3>
            <button
              onClick={() => copyToClipboard(currentSnippets.install, 'install')}
              className="text-xs text-slate-400 hover:text-white flex items-center space-x-1"
            >
              {copiedKey === 'install' ? (
                <Check className="w-3.5 h-3.5 text-emerald-400" aria-hidden="true" />
              ) : (
                <Copy className="w-3.5 h-3.5" aria-hidden="true" />
              )}
              <span>{copiedKey === 'install' ? 'Copied' : 'Copy'}</span>
            </button>
          </div>
          <pre className="bg-slate-950 p-3.5 rounded-lg border border-slate-800 font-mono text-xs text-indigo-300 overflow-x-auto">
            <code>{currentSnippets.install}</code>
          </pre>
        </div>

        {/* Step 2: Producer */}
        <div className="bg-slate-900/60 border border-slate-800/80 rounded-xl p-5 shadow-glass backdrop-blur-md">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-xs font-bold uppercase tracking-wider text-slate-300 flex items-center">
              <span className="w-5 h-5 rounded-full bg-indigo-500/20 text-indigo-400 flex items-center justify-center text-[10px] mr-2">
                2
              </span>
              Producer: Ingest Events with Idempotency
            </h3>
            <button
              onClick={() => copyToClipboard(currentSnippets.producer, 'producer')}
              className="text-xs text-slate-400 hover:text-white flex items-center space-x-1"
            >
              {copiedKey === 'producer' ? (
                <Check className="w-3.5 h-3.5 text-emerald-400" aria-hidden="true" />
              ) : (
                <Copy className="w-3.5 h-3.5" aria-hidden="true" />
              )}
              <span>{copiedKey === 'producer' ? 'Copied' : 'Copy'}</span>
            </button>
          </div>
          <pre className="bg-slate-950 p-4 rounded-lg border border-slate-800 font-mono text-xs text-slate-200 overflow-x-auto leading-relaxed">
            <code>{currentSnippets.producer}</code>
          </pre>
        </div>

        {/* Step 3: Consumer */}
        <div className="bg-slate-900/60 border border-slate-800/80 rounded-xl p-5 shadow-glass backdrop-blur-md">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-xs font-bold uppercase tracking-wider text-slate-300 flex items-center">
              <span className="w-5 h-5 rounded-full bg-indigo-500/20 text-indigo-400 flex items-center justify-center text-[10px] mr-2">
                3
              </span>
              Consumer: Verify HMAC-SHA256 Signatures
            </h3>
            <button
              onClick={() => copyToClipboard(currentSnippets.consumer, 'consumer')}
              className="text-xs text-slate-400 hover:text-white flex items-center space-x-1"
            >
              {copiedKey === 'consumer' ? (
                <Check className="w-3.5 h-3.5 text-emerald-400" aria-hidden="true" />
              ) : (
                <Copy className="w-3.5 h-3.5" aria-hidden="true" />
              )}
              <span>{copiedKey === 'consumer' ? 'Copied' : 'Copy'}</span>
            </button>
          </div>
          <pre className="bg-slate-950 p-4 rounded-lg border border-slate-800 font-mono text-xs text-slate-200 overflow-x-auto leading-relaxed">
            <code>{currentSnippets.consumer}</code>
          </pre>
        </div>

        {/* Step 4: DLQ Management */}
        <div className="bg-slate-900/60 border border-slate-800/80 rounded-xl p-5 shadow-glass backdrop-blur-md">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-xs font-bold uppercase tracking-wider text-slate-300 flex items-center">
              <span className="w-5 h-5 rounded-full bg-indigo-500/20 text-indigo-400 flex items-center justify-center text-[10px] mr-2">
                4
              </span>
              DLQ Management & Replay
            </h3>
            <button
              onClick={() => copyToClipboard(currentSnippets.dlq, 'dlq')}
              className="text-xs text-slate-400 hover:text-white flex items-center space-x-1"
            >
              {copiedKey === 'dlq' ? (
                <Check className="w-3.5 h-3.5 text-emerald-400" aria-hidden="true" />
              ) : (
                <Copy className="w-3.5 h-3.5" aria-hidden="true" />
              )}
              <span>{copiedKey === 'dlq' ? 'Copied' : 'Copy'}</span>
            </button>
          </div>
          <pre className="bg-slate-950 p-4 rounded-lg border border-slate-800 font-mono text-xs text-slate-200 overflow-x-auto leading-relaxed">
            <code>{currentSnippets.dlq}</code>
          </pre>
        </div>
      </div>
    </div>
  );
};
