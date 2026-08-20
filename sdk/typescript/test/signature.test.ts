import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { WebhookSignature, constantTimeEqual } from "../dist/index.js";

describe("WebhookSignature", () => {
  const secret = "whsec_test_secret_12345";
  const payload = JSON.stringify({ event: "order.completed", amount: 50000 });

  it("should generate valid signature header matching format t=<timestamp>,v1=<hex>", async () => {
    const timestamp = Math.floor(Date.now() / 1000);
    const header = await WebhookSignature.sign(secret, timestamp, payload);

    assert.ok(header.startsWith(`t=${timestamp},v1=`));
    const parts = header.split(",");
    assert.strictEqual(parts.length, 2);
    assert.strictEqual(parts[0], `t=${timestamp}`);
    assert.ok(parts[1]?.startsWith("v1="));
    // Hex sha256 is 64 characters
    assert.strictEqual(parts[1]?.slice(3).length, 64);
  });

  it("should verify valid signature header with positional arguments", async () => {
    const timestamp = Math.floor(Date.now() / 1000);
    const header = await WebhookSignature.sign(secret, timestamp, payload);

    const isValid = await WebhookSignature.verify(payload, header, secret, {
      toleranceSeconds: 300,
    });
    assert.strictEqual(isValid, true);
  });

  it("should verify valid signature header with options object argument", async () => {
    const timestamp = Math.floor(Date.now() / 1000);
    const header = await WebhookSignature.sign(secret, timestamp, payload);

    const isValid = await WebhookSignature.verify({
      payload,
      header,
      secret,
      toleranceSeconds: 300,
    });
    assert.strictEqual(isValid, true);
  });

  it("should reject tampered payload", async () => {
    const timestamp = Math.floor(Date.now() / 1000);
    const header = await WebhookSignature.sign(secret, timestamp, payload);

    const tamperedPayload = JSON.stringify({ event: "order.completed", amount: 99999 });
    const isValid = await WebhookSignature.verify(tamperedPayload, header, secret, {
      toleranceSeconds: 300,
    });
    assert.strictEqual(isValid, false);
  });

  it("should reject wrong secret", async () => {
    const timestamp = Math.floor(Date.now() / 1000);
    const header = await WebhookSignature.sign(secret, timestamp, payload);

    const wrongSecret = "whsec_wrong_secret_67890";
    const isValid = await WebhookSignature.verify(payload, header, wrongSecret, {
      toleranceSeconds: 300,
    });
    assert.strictEqual(isValid, false);
  });

  it("should reject tampered header signature", async () => {
    const timestamp = Math.floor(Date.now() / 1000);
    const header = await WebhookSignature.sign(secret, timestamp, payload);

    const tamperedHeader = header + "ab";
    const isValid = await WebhookSignature.verify(payload, tamperedHeader, secret, {
      toleranceSeconds: 300,
    });
    assert.strictEqual(isValid, false);
  });

  it("should reject expired timestamp exceeding tolerance", async () => {
    const now = Math.floor(Date.now() / 1000);
    const oldTimestamp = now - 600; // 10 minutes ago
    const header = await WebhookSignature.sign(secret, oldTimestamp, payload);

    const isValid = await WebhookSignature.verify(payload, header, secret, {
      toleranceSeconds: 300, // 5 min tolerance
      currentTime: now,
    });
    assert.strictEqual(isValid, false);
  });

  it("should accept timestamp within tolerance", async () => {
    const now = Math.floor(Date.now() / 1000);
    const t10Ago = now - 10;
    const header = await WebhookSignature.sign(secret, t10Ago, payload);

    const isValid = await WebhookSignature.verify(payload, header, secret, {
      toleranceSeconds: 30,
      currentTime: now,
    });
    assert.strictEqual(isValid, true);
  });

  it("should bypass expiration check when toleranceSeconds <= 0", async () => {
    const now = Math.floor(Date.now() / 1000);
    const oldTimestamp = now - 10000;
    const header = await WebhookSignature.sign(secret, oldTimestamp, payload);

    const validZero = await WebhookSignature.verify(payload, header, secret, {
      toleranceSeconds: 0,
      currentTime: now,
    });
    assert.strictEqual(validZero, true);

    const validNegative = await WebhookSignature.verify(payload, header, secret, {
      toleranceSeconds: -1,
      currentTime: now,
    });
    assert.strictEqual(validNegative, true);
  });

  it("should reject timestamp in the future exceeding tolerance (clock skew)", async () => {
    const now = Math.floor(Date.now() / 1000);
    const futureTimestamp = now + 500;
    const header = await WebhookSignature.sign(secret, futureTimestamp, payload);

    const isValid = await WebhookSignature.verify(payload, header, secret, {
      toleranceSeconds: 300,
      currentTime: now,
    });
    assert.strictEqual(isValid, false);
  });

  it("should correctly handle Uint8Array payload", async () => {
    const encoder = new TextEncoder();
    const payloadBytes = encoder.encode(payload);
    const timestamp = Math.floor(Date.now() / 1000);
    const header = await WebhookSignature.sign(secret, timestamp, payloadBytes);

    const isValid = await WebhookSignature.verify(payloadBytes, header, secret);
    assert.strictEqual(isValid, true);

    const isValidWithString = await WebhookSignature.verify(payload, header, secret);
    assert.strictEqual(isValidWithString, true);
  });

  it("should reject all malformed headers", async () => {
    const malformedHeaders = [
      "",
      "invalid_header",
      "t=not_a_number,v1=abcdef",
      "v1=abcdef",
      "t=123456789",
      "t=123456789,v2=abcdef",
      "t=123456789,v1=",
      ",,,,",
      "t=,v1=",
    ];

    for (const hdr of malformedHeaders) {
      const isValid = await WebhookSignature.verify(payload, hdr, secret, {
        toleranceSeconds: 300,
      });
      assert.strictEqual(isValid, false, `Failed to reject malformed header: ${hdr}`);
    }
  });

  it("should verify 100% cryptographic compatibility with Go Engine Known Vector", async () => {
    const knownSecret = "super-secret-key";
    const knownTs = 1700000000;
    const knownPayload = JSON.stringify({ hello: "world" });
    const expectedHeader = "t=1700000000,v1=d579771a571d05d2852e374e3dc1f67276baff5bbdbccd57c63cb0557afc2777";

    // 1. Check generated header matches exact expected header
    const generatedHeader = await WebhookSignature.sign(knownSecret, knownTs, knownPayload);
    assert.strictEqual(generatedHeader, expectedHeader);

    // 2. Verify with tolerance = 0 (bypassing time check)
    const isValid = await WebhookSignature.verify(knownPayload, generatedHeader, knownSecret, {
      toleranceSeconds: 0,
    });
    assert.strictEqual(isValid, true);
  });

  it("should parse header correctly with parseHeader", () => {
    const parsed = WebhookSignature.parseHeader("t=1700000000,v1=abcdef123456");
    assert.deepStrictEqual(parsed, {
      timestamp: 1700000000,
      signature: "abcdef123456",
    });

    assert.strictEqual(WebhookSignature.parseHeader(""), null);
    assert.strictEqual(WebhookSignature.parseHeader("invalid"), null);
  });

  it("should perform constant-time comparison correctly", () => {
    assert.strictEqual(constantTimeEqual("hello", "hello"), true);
    assert.strictEqual(constantTimeEqual("hello", "world"), false);
    assert.strictEqual(constantTimeEqual("hello", "hell"), false);
    assert.strictEqual(constantTimeEqual("", ""), true);
    assert.strictEqual(constantTimeEqual("a", ""), false);
  });
});

