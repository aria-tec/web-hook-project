import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { WebhookClient, WebhookAPIError } from "../dist/index.js";

describe("WebhookClient", () => {
  const baseUrl = "http://localhost:8080";
  const tenantId = "tenant_test_123";
  const apiKey = "key_secret_abc";

  it("should normalize baseUrl by trimming trailing slashes", () => {
    const client = new WebhookClient({
      baseUrl: "http://localhost:8080///",
      tenantId,
    });
    assert.strictEqual(client.baseUrl, "http://localhost:8080");
  });

  it("should publish event successfully with default headers and payload", async () => {
    let capturedUrl = "";
    let capturedMethod = "";
    let capturedHeaders: Record<string, string> = {};
    let capturedBody: any = null;

    const mockFetch = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      capturedUrl = input.toString();
      capturedMethod = init?.method ?? "GET";
      capturedHeaders = (init?.headers as Record<string, string>) ?? {};
      capturedBody = init?.body ? JSON.parse(init.body.toString()) : null;

      return new Response(
        JSON.stringify({
          id: "evt_test_999",
          status: "PENDING",
          created_at: "2026-08-20T10:00:00Z",
        }),
        {
          status: 202,
          headers: { "Content-Type": "application/json" },
        }
      );
    };

    const client = new WebhookClient({
      baseUrl,
      tenantId,
      apiKey,
      fetch: mockFetch as any,
    });

    const result = await client.publish("payment.completed", {
      orderId: "ord_123",
      amount: 4500,
    });

    assert.strictEqual(capturedUrl, "http://localhost:8080/api/v1/events");
    assert.strictEqual(capturedMethod, "POST");
    assert.strictEqual(capturedHeaders["X-Tenant-ID"], tenantId);
    assert.strictEqual(capturedHeaders["Authorization"], `Bearer ${apiKey}`);
    assert.strictEqual(capturedHeaders["Content-Type"], "application/json");
    assert.deepStrictEqual(capturedBody, {
      event_type: "payment.completed",
      payload: { orderId: "ord_123", amount: 4500 },
    });

    assert.strictEqual(result.id, "evt_test_999");
    assert.strictEqual(result.status, "PENDING");
    assert.strictEqual(result.createdAt, "2026-08-20T10:00:00Z");
    assert.strictEqual(result.created_at, "2026-08-20T10:00:00Z");
  });

  it("should include X-Idempotency-Key when idempotencyKey option is provided", async () => {
    let capturedHeaders: Record<string, string> = {};

    const mockFetch = async (_input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      capturedHeaders = (init?.headers as Record<string, string>) ?? {};
      return new Response(
        JSON.stringify({
          id: "evt_idemp_1",
          status: "PENDING",
          created_at: "2026-08-20T10:00:00Z",
        }),
        { status: 202, headers: { "Content-Type": "application/json" } }
      );
    };

    const client = new WebhookClient({
      baseUrl,
      tenantId,
      fetch: mockFetch as any,
    });

    await client.publish(
      "user.created",
      { email: "user@example.com" },
      { idempotencyKey: "idemp_unique_key_001" }
    );

    assert.strictEqual(capturedHeaders["X-Idempotency-Key"], "idemp_unique_key_001");
  });

  it("should throw WebhookAPIError on non-2xx HTTP responses", async () => {
    const mockFetch = async (): Promise<Response> => {
      return new Response(JSON.stringify({ error: "duplicate event or idempotency key violation" }), {
        status: 409,
        statusText: "Conflict",
        headers: { "Content-Type": "application/json" },
      });
    };

    const client = new WebhookClient({
      baseUrl,
      tenantId,
      fetch: mockFetch as any,
    });

    await assert.rejects(
      async () => {
        await client.publish("user.signup", { id: "1" });
      },
      (err: any) => {
        assert.ok(err instanceof WebhookAPIError);
        assert.strictEqual(err.statusCode, 409);
        assert.ok(err.message.includes("duplicate event"));
        return true;
      }
    );
  });

  it("should list DLQ events and map fields", async () => {
    let capturedUrl = "";
    let capturedHeaders: Record<string, string> = {};

    const mockFetch = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      capturedUrl = input.toString();
      capturedHeaders = (init?.headers as Record<string, string>) ?? {};

      return new Response(
        JSON.stringify([
          {
            id: "evt_dlq_001",
            tenant_id: tenantId,
            event_type: "invoice.payment_failed",
            idempotency_key: "idemp_99",
            payload: { invoiceId: "inv_1" },
            status: "DLQ",
            created_at: "2026-08-20T09:00:00Z",
          },
        ]),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    };

    const client = new WebhookClient({
      baseUrl,
      tenantId,
      fetch: mockFetch as any,
    });

    const events = await client.listDLQ({ limit: 10, offset: 20 });

    assert.strictEqual(capturedUrl, "http://localhost:8080/api/v1/dlq?limit=10&offset=20");
    assert.strictEqual(capturedHeaders["X-Tenant-ID"], tenantId);
    assert.strictEqual(events.length, 1);
    assert.strictEqual(events[0]?.id, "evt_dlq_001");
    assert.strictEqual(events[0]?.tenantId, tenantId);
    assert.strictEqual(events[0]?.tenant_id, tenantId);
    assert.strictEqual(events[0]?.eventType, "invoice.payment_failed");
    assert.strictEqual(events[0]?.event_type, "invoice.payment_failed");
    assert.strictEqual(events[0]?.idempotencyKey, "idemp_99");
    assert.strictEqual(events[0]?.idempotency_key, "idemp_99");
    assert.strictEqual(events[0]?.status, "DLQ");

    // Check dlq.list alias
    const eventsAlias = await client.dlq.list({ limit: 10, offset: 20 });
    assert.strictEqual(eventsAlias.length, 1);
    assert.strictEqual(eventsAlias[0]?.id, "evt_dlq_001");
  });

  it("should replay DLQ events", async () => {
    let capturedUrl = "";
    let capturedMethod = "";
    let capturedBody: any = null;

    const mockFetch = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      capturedUrl = input.toString();
      capturedMethod = init?.method ?? "GET";
      capturedBody = init?.body ? JSON.parse(init.body.toString()) : null;

      return new Response(
        JSON.stringify({
          status: "QUEUED_FOR_RETRY",
          replayed_count: 2,
        }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    };

    const client = new WebhookClient({
      baseUrl,
      tenantId,
      fetch: mockFetch as any,
    });

    const result = await client.replayDLQ(["evt_dlq_1", "evt_dlq_2"]);

    assert.strictEqual(capturedUrl, "http://localhost:8080/api/v1/dlq/replay");
    assert.strictEqual(capturedMethod, "POST");
    assert.deepStrictEqual(capturedBody, { event_ids: ["evt_dlq_1", "evt_dlq_2"] });
    assert.strictEqual(result.status, "QUEUED_FOR_RETRY");
    assert.strictEqual(result.replayedCount, 2);
    assert.strictEqual(result.replayed_count, 2);

    // Check dlq.replay alias
    const resultAlias = await client.dlq.replay(["evt_dlq_1", "evt_dlq_2"]);
    assert.strictEqual(resultAlias.replayedCount, 2);
  });

  it("should validate replayDLQ input array", async () => {
    const client = new WebhookClient({ baseUrl, tenantId });

    await assert.rejects(async () => {
      await client.replayDLQ([]);
    }, /eventIds array cannot be empty/);
  });
});
