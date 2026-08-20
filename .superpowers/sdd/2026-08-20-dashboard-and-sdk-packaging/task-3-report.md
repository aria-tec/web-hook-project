# Task 3: Zero-Dependency TypeScript SDK (`sdk/typescript`) Report

**Status:** DONE  
**Date:** 2026-08-20  
**Commit:** `9eb4542` (`feat(sdk-ts): implement zero-dependency TypeScript SDK with Web Crypto HMAC verifier`)  

---

## 1. Executive Summary

Implemented the official `@minisvix/client` zero-runtime-dependency TypeScript SDK in `sdk/typescript/`. The SDK provides full Producer functionality (publishing events with idempotency keys and auth) and Consumer functionality (constant-time HMAC-SHA256 signature verification matching the Go engine byte-for-byte using the native Web Crypto API), along with complete Dead-Letter Queue (DLQ) inspection and 1-click batch replay operations.

---

## 2. Deliverables & Created Files

| File | Description |
|------|-------------|
| `sdk/typescript/package.json` | Package manifest with `dependencies: {}`, exports for ESM/CJS, build and test scripts. |
| `sdk/typescript/tsconfig.json` | Strict TypeScript compiler configuration targeting ES2022 with declarations and source maps. |
| `sdk/typescript/src/types.ts` | Complete TypeScript type definitions (`WebhookClientConfig`, `PublishOptions`, `PublishResult`, `DLQEvent`, `ReplayResult`, `VerifyOptions`, `VerifySignatureOptions`). |
| `sdk/typescript/src/signature.ts` | `WebhookSignature` utility class utilizing Web Crypto API (`crypto.subtle`) for timing-safe HMAC verification and signing, plus `constantTimeEqual`. |
| `sdk/typescript/src/client.ts` | `WebhookClient` class implementing `publish`, `listDLQ`, `replayDLQ`, `dlq.list`, and `dlq.replay` using native `fetch`. |
| `sdk/typescript/src/index.ts` | Package entrypoint re-exporting all classes, functions, and types. |
| `sdk/typescript/test/signature.test.ts` | 15 test cases covering signature signing, verification, tolerance windows, clock skew, malformed headers, and Go engine known test vector. |
| `sdk/typescript/test/client.test.ts` | 7 test cases covering publishing, idempotency headers, DLQ listing, DLQ batch replay, and error handling. |
| `sdk/typescript/README.md` | Comprehensive documentation with quickstart examples for producers and consumers. |

---

## 3. Cryptographic Compatibility & Invariants

1. **Byte-for-Byte Go Engine Compatibility:**
   - Go engine format: `fmt.Sprintf("%d.%s", timestamp, string(payload))` signed with HMAC-SHA256 and formatted as `t=<timestamp>,v1=<hex_hmac_sha256>`.
   - TypeScript implementation builds identical byte buffer `Uint8Array` prefixing `${timestamp}.` to raw string or `Uint8Array` payload and calculates HMAC-SHA256 via `crypto.subtle.sign("HMAC", cryptoKey, toSign)`.
   - Verified against the Go engine known test vector (`secret = "super-secret-key"`, `ts = 1700000000`, `payload = '{"hello":"world"}'` $\rightarrow$ `t=1700000000,v1=d579771a571d05d2852e374e3dc1f67276baff5bbdbccd57c63cb0557afc2777`).

2. **Timing Attack Resistance:**
   - Implemented `constantTimeEqual` comparing characters in constant time across the full string length to prevent timing side-channel attacks.

3. **Timestamp Freshness & Tolerance:**
   - Default tolerance: 300 seconds (5 minutes).
   - Tolerance $\le 0$ disables timestamp expiration checks (matching Go engine behavior).
   - Clock skew in the future exceeding tolerance is rejected.

4. **Zero Runtime Dependencies:**
   - `"dependencies": {}` in `package.json`.
   - Relies exclusively on standard Web APIs (`fetch`, `crypto.subtle`, `TextEncoder`, `URLSearchParams`) available in Node.js 18+, Bun, Deno, Cloudflare Workers, Next.js / Edge runtimes, and modern browsers.

---

## 4. Test Verification Summary

Command executed: `npm test` (`tsc && node --test test/signature.test.ts test/client.test.ts`)

```
▶ WebhookClient
  ✔ should normalize baseUrl by trimming trailing slashes (0.767375ms)
  ✔ should publish event successfully with default headers and payload (16.4975ms)
  ✔ should include X-Idempotency-Key when idempotencyKey option is provided (0.281ms)
  ✔ should throw WebhookAPIError on non-2xx HTTP responses (0.414625ms)
  ✔ should list DLQ events and map fields (0.439166ms)
  ✔ should replay DLQ events (0.329875ms)
  ✔ should validate replayDLQ input array (0.108083ms)
✔ WebhookClient (19.541792ms)
▶ WebhookSignature
  ✔ should generate valid signature header matching format t=<timestamp>,v1=<hex> (5.675542ms)
  ✔ should verify valid signature header with positional arguments (0.726ms)
  ✔ should verify valid signature header with options object argument (0.464625ms)
  ✔ should reject tampered payload (0.333916ms)
  ✔ should reject wrong secret (0.357083ms)
  ✔ should reject tampered header signature (0.314458ms)
  ✔ should reject expired timestamp exceeding tolerance (0.238875ms)
  ✔ should accept timestamp within tolerance (0.267167ms)
  ✔ should bypass expiration check when toleranceSeconds <= 0 (0.294667ms)
  ✔ should reject timestamp in the future exceeding tolerance (clock skew) (0.2475ms)
  ✔ should correctly handle Uint8Array payload (0.438834ms)
  ✔ should reject all malformed headers (0.135083ms)
  ✔ should verify 100% cryptographic compatibility with Go Engine Known Vector (0.936791ms)
  ✔ should parse header correctly with parseHeader (0.672125ms)
  ✔ should perform constant-time comparison correctly (0.043375ms)
✔ WebhookSignature (12.089958ms)
ℹ tests 22
ℹ suites 2
ℹ pass 22
ℹ fail 0
ℹ cancelled 0
ℹ skipped 0
ℹ todo 0
ℹ duration_ms 94.684583
```

All 22 unit tests passed.
