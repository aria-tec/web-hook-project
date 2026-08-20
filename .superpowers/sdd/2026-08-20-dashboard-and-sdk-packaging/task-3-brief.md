# Task 3: Zero-Dependency TypeScript SDK (`sdk/typescript`)

**Files:**
- Create: `sdk/typescript/package.json`
- Create: `sdk/typescript/tsconfig.json`
- Create: `sdk/typescript/src/types.ts`
- Create: `sdk/typescript/src/signature.ts`
- Create: `sdk/typescript/src/client.ts`
- Create: `sdk/typescript/src/index.ts`
- Test: `sdk/typescript/test/signature.test.ts`
- Test: `sdk/typescript/test/client.test.ts`

**Interfaces:**
- Produces: `WebhookClient` class:
  - `constructor(config: { baseUrl: string; tenantId: string; apiKey?: string })`
  - `publish<T>(eventType: string, payload: T, options?: { idempotencyKey?: string }): Promise<PublishResult>`
  - `listDLQ(options?: { limit?: number; offset?: number }): Promise<DLQEvent[]>`
  - `replayDLQ(eventIds: string[]): Promise<ReplayResult>`
- Produces: `WebhookSignature` utility class:
  - `static async verify(payload: string | Uint8Array, signatureHeader: string, secret: string, options?: { toleranceSeconds?: number; currentTime?: number }): Promise<boolean>`
  - Uses Web Crypto API (`crypto.subtle` / native HMAC-SHA256) with constant-time equality check.
  - Verifies signature matching `t=<timestamp>,v1=<hex_hmac_sha256>`.
  - Enforces timestamp freshness tolerance (default: 300 seconds / 5 minutes).

**Requirements:**
1. Zero runtime dependencies in `package.json` (only devDependencies for TypeScript, Vitest/Node test runner).
2. Follow TDD: Write test files first, implement SDK, and verify all tests pass.
3. Signature verification must produce 100% byte-for-byte cryptographic compatibility with Go Engine's HMAC generator (`internal/dispatcher/hmac.go`).
4. Type definitions strictly exported for browser, Node.js, Cloudflare Workers, and Next.js runtimes.
