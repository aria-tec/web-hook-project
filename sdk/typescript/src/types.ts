/**
 * Configuration options for WebhookClient.
 */
export interface WebhookClientConfig {
  /**
   * Base URL of the Mini-Svix engine (e.g. "http://localhost:8080").
   */
  baseUrl: string;

  /**
   * Tenant identifier for multi-tenant isolation (sent in X-Tenant-ID header).
   */
  tenantId: string;

  /**
   * Optional API key or bearer token (sent in Authorization: Bearer <apiKey> header).
   */
  apiKey?: string;

  /**
   * Optional custom fetch implementation (useful for testing or custom HTTP agents).
   */
  fetch?: typeof fetch;
}

/**
 * Options for publishing events.
 */
export interface PublishOptions {
  /**
   * Optional idempotency key to prevent duplicate processing (sent in X-Idempotency-Key header).
   */
  idempotencyKey?: string;
}

/**
 * Result returned upon successfully publishing an event.
 */
export interface PublishResult {
  /**
   * Unique event identifier (e.g. "evt_123456...").
   */
  id: string;

  /**
   * Current status of the ingested event (e.g. "PENDING").
   */
  status: string;

  /**
   * ISO 8601 creation timestamp string.
   */
  createdAt: string;

  /**
   * Alias matching server snake_case field.
   */
  created_at?: string;
}

/**
 * Options for listing dead-letter queue (DLQ) events.
 */
export interface ListDLQOptions {
  /**
   * Maximum number of DLQ events to retrieve (default: 50).
   */
  limit?: number;

  /**
   * Pagination offset (default: 0).
   */
  offset?: number;
}

/**
 * A dead-letter queue (DLQ) event record.
 */
export interface DLQEvent<T = unknown> {
  id: string;
  tenantId: string;
  tenant_id?: string;
  eventType: string;
  event_type?: string;
  idempotencyKey?: string;
  idempotency_key?: string;
  payload: T;
  status: string;
  createdAt: string;
  created_at?: string;
  updatedAt?: string;
  updated_at?: string;
}

/**
 * Result returned upon replaying DLQ events.
 */
export interface ReplayResult {
  /**
   * Replay operation status (e.g. "QUEUED_FOR_RETRY").
   */
  status: string;

  /**
   * Total number of events successfully queued for replay.
   */
  replayedCount: number;

  /**
   * Alias matching server snake_case field.
   */
  replayed_count?: number;
}

/**
 * Options for verifying webhook HMAC signatures.
 */
export interface VerifySignatureOptions {
  /**
   * Maximum allowed clock drift / age of the webhook signature in seconds.
   * Default is 300 seconds (5 minutes). Set to <= 0 to disable timestamp verification.
   */
  toleranceSeconds?: number;

  /**
   * Optional current timestamp in Unix seconds (useful for deterministic tests).
   * Defaults to Math.floor(Date.now() / 1000).
   */
  currentTime?: number;
}

/**
 * Object parameter format for WebhookSignature.verify.
 */
export interface VerifyOptions extends VerifySignatureOptions {
  payload: string | Uint8Array;
  header: string;
  secret: string;
}
