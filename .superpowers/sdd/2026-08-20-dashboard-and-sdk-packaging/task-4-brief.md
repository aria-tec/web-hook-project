# Task 4: Zero-Dependency Go SDK (`sdk/go/webhookclient`)

**Files:**
- Create: `sdk/go/webhookclient/types.go`
- Create: `sdk/go/webhookclient/signature.go`
- Create: `sdk/go/webhookclient/client.go`
- Test: `sdk/go/webhookclient/signature_test.go`
- Test: `sdk/go/webhookclient/client_test.go`

**Interfaces:**
- Produces: `webhookclient.Client` struct:
  - `New(baseURL string, tenantID string, opts ...Option) *Client`
  - `(c *Client) Publish(ctx context.Context, eventType string, payload any, opts ...PublishOption) (*PublishResult, error)`
  - `(c *Client) ListDLQ(ctx context.Context, limit, offset int) ([]DLQEvent, error)`
  - `(c *Client) ReplayDLQ(ctx context.Context, eventIDs []string) (*ReplayResult, error)`
- Options:
  - `WithAPIKey(apiKey string) Option`
  - `WithHTTPClient(client *http.Client) Option`
  - `WithTimeout(d time.Duration) Option`
  - `WithIdempotencyKey(key string) PublishOption`
- Produces: `webhookclient.VerifySignature(secret string, header string, payload []byte, toleranceSeconds int64) bool`:
  - Uses standard library `crypto/hmac` and `crypto/subtle` / `hmac.Equal`.
  - Enforces `t=<timestamp>,v1=<hex>` format and timestamp freshness tolerance (<= 0 bypasses check).
- Produces: `webhookclient.SignPayload(secret string, timestamp int64, payload []byte) string`.

**Requirements:**
1. Zero external dependencies: Uses only Go standard library.
2. Follow TDD: Write test files first in `sdk/go/webhookclient/`, verify failure, implement SDK, and verify all tests pass.
3. 100% cryptographic compatibility with Go Engine's `internal/dispatcher/hmac.go` and TypeScript SDK's `WebhookSignature`.
4. All tests must pass `go test -v -count=1 -race ./sdk/go/webhookclient/...`.
