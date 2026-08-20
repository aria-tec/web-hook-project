import type { VerifyOptions, VerifySignatureOptions } from "./types.js";

/**
 * Performs constant-time string comparison to prevent timing attacks.
 */
export function constantTimeEqual(a: string, b: string): boolean {
  const aLen = a.length;
  const bLen = b.length;
  let diff = aLen ^ bLen;
  const maxLen = Math.max(aLen, bLen);
  for (let i = 0; i < maxLen; i++) {
    const charA = i < aLen ? a.charCodeAt(i) : 0;
    const charB = i < bLen ? b.charCodeAt(i) : 0;
    diff |= charA ^ charB;
  }
  return diff === 0;
}

/**
 * Builds the UTF-8 bytes to sign formatted as: `<timestamp>.<payload>`
 */
function buildToSignBytes(timestamp: number, payload: string | Uint8Array): Uint8Array {
  const encoder = new TextEncoder();
  const prefixBytes = encoder.encode(`${timestamp}.`);
  const payloadBytes = typeof payload === "string" ? encoder.encode(payload) : payload;

  const combined = new Uint8Array(prefixBytes.length + payloadBytes.length);
  combined.set(prefixBytes, 0);
  combined.set(payloadBytes, prefixBytes.length);
  return combined;
}

/**
 * Computes the HMAC-SHA256 signature in lowercase hex using the standard Web Crypto API.
 */
async function computeHmacSha256Hex(secret: string, data: Uint8Array): Promise<string> {
  const subtle = globalThis.crypto?.subtle;
  if (!subtle) {
    throw new Error(
      "Web Crypto API (crypto.subtle) is not available in the current runtime environment."
    );
  }

  const encoder = new TextEncoder();
  const keyData = encoder.encode(secret);
  const cryptoKey = await subtle.importKey(
    "raw",
    keyData,
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );

  const signatureBuffer = await subtle.sign("HMAC", cryptoKey, data as unknown as BufferSource);
  const signatureBytes = new Uint8Array(signatureBuffer);

  let hex = "";
  for (let i = 0; i < signatureBytes.length; i++) {
    const byte = signatureBytes[i];
    if (byte !== undefined) {
      hex += byte.toString(16).padStart(2, "0");
    }
  }
  return hex;
}

/**
 * WebhookSignature provides HMAC-SHA256 signature generation and constant-time verification.
 * Byte-for-byte compatible with Mini-Svix Go Engine dispatcher.
 */
export class WebhookSignature {
  /**
   * Generates a signature header string in format: `t=<timestamp>,v1=<hex_hmac_sha256>`.
   *
   * @param secret Shared webhook signing secret
   * @param timestamp Unix timestamp in seconds
   * @param payload Raw webhook payload (string or Uint8Array)
   */
  static async sign(
    secret: string,
    timestamp: number,
    payload: string | Uint8Array
  ): Promise<string> {
    const toSignBytes = buildToSignBytes(timestamp, payload);
    const hex = await computeHmacSha256Hex(secret, toSignBytes);
    return `t=${timestamp},v1=${hex}`;
  }

  /**
   * Parses a signature header string (`t=<timestamp>,v1=<signature>`) into component parts.
   */
  static parseHeader(signatureHeader: string): { timestamp: number; signature: string } | null {
    if (!signatureHeader || typeof signatureHeader !== "string") {
      return null;
    }

    const parts = signatureHeader.split(",");
    let timestampStr: string | undefined;
    let signature: string | undefined;

    for (const part of parts) {
      const eqIdx = part.indexOf("=");
      if (eqIdx === -1) {
        continue;
      }
      const key = part.slice(0, eqIdx).trim();
      const val = part.slice(eqIdx + 1).trim();
      if (key === "t") {
        timestampStr = val;
      } else if (key === "v1") {
        signature = val;
      }
    }

    if (!timestampStr || !signature) {
      return null;
    }

    if (!/^-?\d+$/.test(timestampStr)) {
      return null;
    }

    const timestamp = parseInt(timestampStr, 10);
    if (Number.isNaN(timestamp)) {
      return null;
    }

    return { timestamp, signature };
  }

  /**
   * Verifies an incoming webhook HMAC signature.
   *
   * @param options Object containing payload, header, secret, toleranceSeconds, and optional currentTime
   */
  static async verify(options: VerifyOptions): Promise<boolean>;

  /**
   * Verifies an incoming webhook HMAC signature.
   *
   * @param payload Raw webhook payload as string or Uint8Array
   * @param signatureHeader Value of the signature header (e.g. X-Webhook-Signature or Webhook-Signature)
   * @param secret Shared webhook signing secret
   * @param options Optional tolerance and currentTime settings (default tolerance: 300s)
   */
  static async verify(
    payload: string | Uint8Array,
    signatureHeader: string,
    secret: string,
    options?: VerifySignatureOptions
  ): Promise<boolean>;

  static async verify(
    payloadOrOptions: string | Uint8Array | VerifyOptions,
    signatureHeader?: string,
    secret?: string,
    options?: VerifySignatureOptions
  ): Promise<boolean> {
    let payload: string | Uint8Array;
    let header: string;
    let sec: string;
    let toleranceSeconds = 300;
    let currentTime: number | undefined;

    if (
      typeof payloadOrOptions === "object" &&
      !(payloadOrOptions instanceof Uint8Array) &&
      "header" in payloadOrOptions &&
      "secret" in payloadOrOptions
    ) {
      payload = payloadOrOptions.payload;
      header = payloadOrOptions.header;
      sec = payloadOrOptions.secret;
      if (payloadOrOptions.toleranceSeconds !== undefined) {
        toleranceSeconds = payloadOrOptions.toleranceSeconds;
      }
      currentTime = payloadOrOptions.currentTime;
    } else {
      payload = payloadOrOptions as string | Uint8Array;
      header = signatureHeader ?? "";
      sec = secret ?? "";
      if (options?.toleranceSeconds !== undefined) {
        toleranceSeconds = options.toleranceSeconds;
      }
      currentTime = options?.currentTime;
    }

    if (!header || !sec) {
      return false;
    }

    const parsed = WebhookSignature.parseHeader(header);
    if (!parsed) {
      return false;
    }

    const { timestamp, signature: expectedSig } = parsed;

    if (toleranceSeconds > 0) {
      const now = currentTime !== undefined ? currentTime : Math.floor(Date.now() / 1000);
      const diff = now - timestamp;
      if (Math.abs(diff) > toleranceSeconds) {
        return false;
      }
    }

    const toSignBytes = buildToSignBytes(timestamp, payload);
    const actualSig = await computeHmacSha256Hex(sec, toSignBytes);

    return constantTimeEqual(actualSig.toLowerCase(), expectedSig.toLowerCase());
  }
}
