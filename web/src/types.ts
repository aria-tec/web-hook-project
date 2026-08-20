export type DeliveryStatus = 'SUCCESS' | 'RETRYING' | 'FAILED';
export type EventStatus = 'PENDING' | 'DELIVERED' | 'FAILED' | 'DLQ';
export type ConnectionState = 'connected' | 'connecting' | 'disconnected' | 'error';

export interface DeliveryAttempt {
  id: string;
  event_id: string;
  endpoint_id: string;
  attempt_number: number;
  response_status?: number;
  response_body?: string;
  duration_ms: number;
  status: DeliveryStatus;
  error_message?: string;
  executed_at: string;
  // Synthetic / UI enriched fields
  tenant_id?: string;
  event_type?: string;
  target_url?: string;
  payload?: any;
}

export interface DLQEvent {
  id: string;
  tenant_id: string;
  event_type: string;
  idempotency_key?: string;
  payload: string | Record<string, any>;
  status: EventStatus;
  created_at: string;
}

export interface Endpoint {
  id: string;
  tenant_id: string;
  url: string;
  secret: string;
  rate_limit: number;
  is_active: boolean;
  created_at: string;
  updated_at: string;
}

export interface ReplayResponse {
  status: string;
  replayed_count: number;
}

export interface IngestResponse {
  id: string;
  status: string;
  created_at: string;
}

export interface SystemStats {
  totalAttempts: number;
  successCount: number;
  retryingCount: number;
  failedCount: number;
  avgDurationMs: number;
  dlqCount: number;
}

export interface HMACSignatureBreakdown {
  rawHeader: string;
  timestamp: number;
  formattedTimestamp: string;
  signatureHex: string;
  secret: string;
  signedString: string;
  isTimestampFresh: boolean;
  isValid: boolean;
}
