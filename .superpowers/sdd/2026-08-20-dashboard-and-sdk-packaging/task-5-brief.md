# Task 5: Operational Dashboard Web SPA (`web/`)

**Files:**
- Create: `web/package.json`
- Create: `web/vite.config.ts`
- Create: `web/tsconfig.json`
- Create: `web/tsconfig.node.json`
- Create: `web/tailwind.config.js`
- Create: `web/postcss.config.js`
- Create: `web/index.html`
- Create: `web/src/index.css`
- Create: `web/src/main.tsx`
- Create: `web/src/types.ts`
- Create: `web/src/hooks/useEventStream.ts`
- Create: `web/src/hooks/useDLQ.ts`
- Create: `web/src/components/Header.tsx`
- Create: `web/src/components/SimulationBar.tsx`
- Create: `web/src/components/LiveStream.tsx`
- Create: `web/src/components/HMACInspector.tsx`
- Create: `web/src/components/DLQManager.tsx`
- Create: `web/src/components/SDKGuide.tsx`
- Create: `web/src/App.tsx`

**Key Features:**
1. **Dark Tech Glassmorphism Design System:**
   - Background `#0B0F17`, cards `bg-slate-900/60 backdrop-blur-md border border-slate-800/80`.
   - Emerald `#10B981` (200 OK / Delivered), Amber `#F59E0B` (500 Retrying), Crimson `#EF4444` (400 DLQ / Poison), Indigo `#6366F1` (Live Stream active).
2. **200-Event Ring Buffer Hook (`useEventStream`):**
   - Connects to `GET /api/v1/events/stream`.
   - Maintains fixed-capacity circular array of max 200 items in React state to prevent browser tab lag during high throughput.
   - Handles auto-reconnection on network drop.
3. **Interactive Simulation Bar:**
   - 1-Click buttons for Normal (200 OK), Flaky Endpoint (500 Retry -> 200 OK), and Poison Pill (400 DLQ).
   - Ingests events directly to engine via `POST /api/v1/events` targeting mock receiver endpoints.
4. **Deep HMAC & Payload Inspector:**
   - Interactive modal/drawer showing raw payload, `X-Webhook-Signature` breakdown (`t=<timestamp>`, `v1=<hex>`), secret preview, and verification status.
5. **Dead-Letter Queue (DLQ) Manager:**
   - Lists DLQ events via `GET /api/v1/dlq`.
   - 1-Click individual and batch replay via `POST /api/v1/dlq/replay`.
6. **SDK Guide Tab:**
   - Copy-paste ready TypeScript and Go SDK snippets with active syntax formatting.

**Requirements:**
- Follow TDD / build verification: `npm run build` in `web/` must succeed with zero TypeScript or Vite bundle errors.
- Clean responsive layout without horizontal overflows.
