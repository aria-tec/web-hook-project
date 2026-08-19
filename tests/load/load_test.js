import http from 'k6/http';
import { check, sleep } from 'k6';
import { randomString, uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

export const options = {
  scenarios: {
    sustained_high_throughput: {
      executor: 'ramping-arrival-rate',
      startRate: 200,
      timeUnit: '1s',
      preAllocatedVUs: 100,
      maxVUs: 800,
      stages: [
        { target: 1000, duration: '10s' }, // Warm-up ramp
        { target: 2000, duration: '30s' }, // Peak 2,000 RPS sustained ingestion
        { target: 2000, duration: '20s' }, // Hold SLA benchmark
        { target: 0, duration: '5s' },     // Graceful ramp-down
      ],
    },
  },
  thresholds: {
    // SLA 1: P99 latency must be under 40ms
    'http_req_duration': [
      'p(90)<20',
      'p(95)<30',
      'p(99)<40',
    ],
    // SLA 2: Error rate must be under 0.01% (< 0.0001)
    'http_req_failed': ['rate<0.0001'],
  },
};

const BASE_URL = __ENV.TARGET_URL || 'http://localhost:8080';
const TENANT_ID = 'tenant_load_bench';

export default function () {
  // 90% unique ingestion, 10% replayed idempotency test
  const isReplay = Math.random() < 0.10;
  const idempKey = isReplay ? `idemp_shared_${__VU % 20}` : `idemp_${uuidv4()}`;

  const payload = JSON.stringify({
    event_type: 'payment.completed',
    payload: {
      order_id: `ord_${randomString(10)}`,
      amount: Math.floor(Math.random() * 50000) + 100,
      currency: 'USD',
      customer_id: `cust_${__VU}`,
      timestamp: Date.now(),
    },
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'X-Tenant-ID': TENANT_ID,
      'X-Idempotency-Key': idempKey,
    },
  };

  const res = http.post(`${BASE_URL}/api/v1/events`, payload, params);

  check(res, {
    'status is 202 or 200': (r) => r.status === 202 || r.status === 200,
    'response has valid json body': (r) => {
      try {
        const body = JSON.parse(r.body);
        return body.id !== undefined || body.error === undefined;
      } catch (e) {
        return false;
      }
    },
  });
}
