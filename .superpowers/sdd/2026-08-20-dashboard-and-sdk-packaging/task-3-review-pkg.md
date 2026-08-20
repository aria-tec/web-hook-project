# Review Package: Task 3

**Commit Range:** `9ea761a..9eb4542`
**Plan File:** `docs/superpowers/plans/2026-08-20-dashboard-and-sdk-packaging.md`

## Summary of Changes
- Created `sdk/typescript/` package `@minisvix/client` with 0 runtime dependencies:
  - `package.json`, `tsconfig.json`, `.gitignore`, `README.md`.
  - `src/types.ts`: Comprehensive interface definitions.
  - `src/signature.ts`: `WebhookSignature` utilizing Web Crypto API (`crypto.subtle`) for constant-time HMAC verification (`t=<timestamp>,v1=<hex>`).
  - `src/client.ts`: `WebhookClient` implementing `publish`, `listDLQ`, `replayDLQ` via native `fetch`.
  - `src/index.ts`: Public API export surface.
  - `test/signature.test.ts` & `test/client.test.ts`: 22 unit tests passing in 95ms with 100% Go compatibility.
