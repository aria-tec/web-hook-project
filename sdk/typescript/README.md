# @minisvix/client

Official zero-dependency TypeScript/JavaScript SDK and Web Crypto HMAC verifier for **Mini-Svix**.

## Features

- 🚀 **Zero Runtime Dependencies**: Built entirely with native `fetch` and the standard Web Crypto API (`crypto.subtle`).
- 🔒 **Timing-Safe HMAC Verification**: Constant-time comparison (`t=<timestamp>,v1=<hex_hmac_sha256>`) matching the Go engine byte-for-byte.
- 🌐 **Universal Runtime Support**: Works seamlessly in Node.js (18+), Bun, Deno, Cloudflare Workers, Next.js / Edge Runtime, and modern browsers.
- 🔁 **DLQ & Ingestion Support**: Full API support for event ingestion with idempotency keys, listing dead-letter queues, and 1-click batch replays.
- 📦 **Strict TypeScript Types**: Fully typed interfaces with auto-generated declaration files and source maps.

## Installation

```bash
npm install @minisvix/client
```

## Quick Start

### 1. Ingest / Publish Events (Producer)

```typescript
import { WebhookClient } from "@minisvix/client";

const client = new WebhookClient({
  baseUrl: "http://localhost:8080",
  tenantId: "tenant_alpha",
  apiKey: "optional_api_key_here",
});

// Ingest an event with idempotency
const event = await client.publish(
  "order.created",
  {
    orderId: "ord_12345",
    amount: 50000,
    currency: "USD",
  },
  {
    idempotencyKey: "idemp_order_12345_attempt_1",
  }
);

console.log("Published event ID:", event.id); // "evt_..."
console.log("Status:", event.status);         // "PENDING"
```

### 2. Verify Webhook Signatures (Consumer)

```typescript
import { WebhookSignature } from "@minisvix/client";

// In your webhook receiver endpoint (Express, Next.js API route, Cloudflare Worker):
const isValid = await WebhookSignature.verify({
  payload: rawBodyString, // or Uint8Array
  header: request.headers["x-webhook-signature"],
  secret: "whsec_your_webhook_secret",
  toleranceSeconds: 300, // 5 minutes (default)
});

if (!isValid) {
  return response.status(401).send("Invalid or expired signature");
}

// Process validated webhook...
```

You can also pass arguments positionally:

```typescript
const isValid = await WebhookSignature.verify(
  rawBodyString,
  headerString,
  secretString,
  { toleranceSeconds: 300 }
);
```

### 3. Dead-Letter Queue (DLQ) Management

```typescript
// List DLQ events with pagination
const dlqEvents = await client.listDLQ({ limit: 50, offset: 0 });

// Or via namespaced helper:
// const dlqEvents = await client.dlq.list({ limit: 50 });

for (const event of dlqEvents) {
  console.log(`Failed event: ${event.id} (Type: ${event.eventType})`);
}

// Replay dead-lettered events
const replayResult = await client.replayDLQ(["evt_failed_1", "evt_failed_2"]);
console.log(`Replayed ${replayResult.replayedCount} events.`);
```

## API Reference

### `WebhookClient`
- `constructor(config: WebhookClientConfig)`
  - `baseUrl`: Root URL of Mini-Svix engine
  - `tenantId`: Multi-tenant isolation identifier
  - `apiKey` *(optional)*: Bearer token for authentication
  - `fetch` *(optional)*: Custom `fetch` implementation
- `publish<T>(eventType: string, payload: T, options?: PublishOptions): Promise<PublishResult>`
- `listDLQ(options?: ListDLQOptions): Promise<DLQEvent[]>`
- `replayDLQ(eventIds: string[]): Promise<ReplayResult>`
- `dlq.list(options?: ListDLQOptions): Promise<DLQEvent[]>`
- `dlq.replay(eventIds: string[]): Promise<ReplayResult>`

### `WebhookSignature`
- `static async verify(options: VerifyOptions): Promise<boolean>`
- `static async verify(payload: string | Uint8Array, header: string, secret: string, options?: VerifySignatureOptions): Promise<boolean>`
- `static async sign(secret: string, timestamp: number, payload: string | Uint8Array): Promise<string>`
- `static parseHeader(signatureHeader: string): { timestamp: number; signature: string } | null`
- `constantTimeEqual(a: string, b: string): boolean`

## Cryptographic Compatibility

The signature format and verification algorithm strictly match the Go Engine:
- Format: `t=<timestamp>,v1=<hex_hmac_sha256>`
- Signed Content: `<timestamp>.<payload>`
- Hash Function: HMAC-SHA256 via native Web Crypto (`crypto.subtle`)
- Tolerance: Configurable freshness window in seconds (bypassed if $\le 0$).

## License

MIT
