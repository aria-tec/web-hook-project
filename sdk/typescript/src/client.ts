import type {
  DLQEvent,
  ListDLQOptions,
  PublishOptions,
  PublishResult,
  ReplayResult,
  WebhookClientConfig,
} from "./types.js";

/**
 * Custom error thrown when the Mini-Svix API returns a non-2xx response.
 */
export class WebhookAPIError extends Error {
  readonly statusCode: number;
  readonly statusText: string;
  readonly responseBody: unknown;

  constructor(statusCode: number, statusText: string, responseBody: unknown) {
    let message = `API request failed with status ${statusCode} (${statusText})`;
    if (
      responseBody &&
      typeof responseBody === "object" &&
      "error" in responseBody &&
      typeof responseBody.error === "string"
    ) {
      message = `${message}: ${responseBody.error}`;
    } else if (typeof responseBody === "string" && responseBody.length > 0) {
      message = `${message}: ${responseBody}`;
    }

    super(message);
    this.name = "WebhookAPIError";
    this.statusCode = statusCode;
    this.statusText = statusText;
    this.responseBody = responseBody;
  }
}

/**
 * Mini-Svix Webhook Client for event ingestion and DLQ management.
 * Built with native Fetch and zero runtime dependencies.
 */
export class WebhookClient {
  readonly baseUrl: string;
  readonly tenantId: string;
  readonly apiKey?: string;
  private readonly _fetch: typeof fetch;

  /**
   * Namespaced access for DLQ operations.
   */
  readonly dlq = {
    list: <T = unknown>(options?: ListDLQOptions): Promise<DLQEvent<T>[]> =>
      this.listDLQ<T>(options),
    replay: (eventIds: string[]): Promise<ReplayResult> => this.replayDLQ(eventIds),
  };

  constructor(config: WebhookClientConfig) {
    if (!config.baseUrl) {
      throw new Error("WebhookClient: baseUrl is required");
    }
    if (!config.tenantId) {
      throw new Error("WebhookClient: tenantId is required");
    }

    // Strip any trailing slashes from baseUrl
    this.baseUrl = config.baseUrl.replace(/\/+$/, "");
    this.tenantId = config.tenantId;
    this.apiKey = config.apiKey;
    this._fetch = config.fetch ?? globalThis.fetch.bind(globalThis);
  }

  /**
   * Helper to build common headers for requests.
   */
  private buildHeaders(customHeaders: Record<string, string> = {}): Record<string, string> {
    const headers: Record<string, string> = {
      "X-Tenant-ID": this.tenantId,
      ...customHeaders,
    };

    if (this.apiKey) {
      headers["Authorization"] = `Bearer ${this.apiKey}`;
    }

    return headers;
  }

  /**
   * Publishes an event to the Mini-Svix engine.
   *
   * @param eventType Domain event type identifier (e.g. "payment.succeeded")
   * @param payload Event payload data
   * @param options Optional configuration including idempotencyKey
   */
  async publish<T = unknown>(
    eventType: string,
    payload: T,
    options?: PublishOptions
  ): Promise<PublishResult> {
    if (!eventType) {
      throw new Error("eventType is required");
    }

    const headers = this.buildHeaders({
      "Content-Type": "application/json",
    });

    if (options?.idempotencyKey) {
      headers["X-Idempotency-Key"] = options.idempotencyKey;
    }

    const response = await this._fetch(`${this.baseUrl}/api/v1/events`, {
      method: "POST",
      headers,
      body: JSON.stringify({
        event_type: eventType,
        payload,
      }),
    });

    if (!response.ok) {
      let errorBody: unknown;
      try {
        errorBody = await response.json();
      } catch {
        errorBody = await response.text().catch(() => null);
      }
      throw new WebhookAPIError(response.status, response.statusText, errorBody);
    }

    const data = (await response.json()) as { id: string; status: string; created_at: string };

    return {
      id: data.id,
      status: data.status,
      createdAt: data.created_at,
      created_at: data.created_at,
    };
  }

  /**
   * Lists events currently in the Dead-Letter Queue (DLQ).
   *
   * @param options Pagination options (limit and offset)
   */
  async listDLQ<T = unknown>(options?: ListDLQOptions): Promise<DLQEvent<T>[]> {
    const params = new URLSearchParams();
    if (options?.limit !== undefined) {
      params.set("limit", options.limit.toString());
    }
    if (options?.offset !== undefined) {
      params.set("offset", options.offset.toString());
    }

    const queryString = params.toString();
    const url = `${this.baseUrl}/api/v1/dlq${queryString ? `?${queryString}` : ""}`;

    const headers = this.buildHeaders({
      Accept: "application/json",
    });

    const response = await this._fetch(url, {
      method: "GET",
      headers,
    });

    if (!response.ok) {
      let errorBody: unknown;
      try {
        errorBody = await response.json();
      } catch {
        errorBody = await response.text().catch(() => null);
      }
      throw new WebhookAPIError(response.status, response.statusText, errorBody);
    }

    const rawList = (await response.json()) as Array<{
      id: string;
      tenant_id: string;
      event_type: string;
      idempotency_key?: string;
      payload: T;
      status: string;
      created_at: string;
      updated_at?: string;
    }>;

    return rawList.map((item) => ({
      id: item.id,
      tenantId: item.tenant_id,
      tenant_id: item.tenant_id,
      eventType: item.event_type,
      event_type: item.event_type,
      idempotencyKey: item.idempotency_key,
      idempotency_key: item.idempotency_key,
      payload: item.payload,
      status: item.status,
      createdAt: item.created_at,
      created_at: item.created_at,
      updatedAt: item.updated_at,
      updated_at: item.updated_at,
    }));
  }

  /**
   * Replays dead-lettered events back into the transactional outbox retry pipeline.
   *
   * @param eventIds Array of event IDs to replay
   */
  async replayDLQ(eventIds: string[]): Promise<ReplayResult> {
    if (!eventIds || !Array.isArray(eventIds) || eventIds.length === 0) {
      throw new Error("eventIds array cannot be empty");
    }

    const headers = this.buildHeaders({
      "Content-Type": "application/json",
    });

    const response = await this._fetch(`${this.baseUrl}/api/v1/dlq/replay`, {
      method: "POST",
      headers,
      body: JSON.stringify({
        event_ids: eventIds,
      }),
    });

    if (!response.ok) {
      let errorBody: unknown;
      try {
        errorBody = await response.json();
      } catch {
        errorBody = await response.text().catch(() => null);
      }
      throw new WebhookAPIError(response.status, response.statusText, errorBody);
    }

    const data = (await response.json()) as { status: string; replayed_count: number };

    return {
      status: data.status,
      replayedCount: data.replayed_count,
      replayed_count: data.replayed_count,
    };
  }
}
