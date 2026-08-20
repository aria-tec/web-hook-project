package dispatcher

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// SignPayload signs timestamp + "." + payload using HMAC-SHA256 and returns
// the header string in format: t=<timestamp>,v1=<hex_signature>.
func SignPayload(secret string, timestamp int64, payload []byte) string {
	toSign := fmt.Sprintf("%d.%s", timestamp, string(payload))
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(toSign))
	signature := hex.EncodeToString(mac.Sum(nil))
	return fmt.Sprintf("t=%d,v1=%s", timestamp, signature)
}

// VerifySignature validates an HMAC-SHA256 signature header against the payload and secret.
// If toleranceSeconds > 0, it validates that the timestamp is within tolerance of time.Now().
// Uses hmac.Equal to prevent timing attacks.
func VerifySignature(secret string, header string, payload []byte, toleranceSeconds int64) bool {
	if header == "" {
		return false
	}

	parts := strings.Split(header, ",")
	var timestampStr, expectedSig string
	for _, part := range parts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "t":
			timestampStr = kv[1]
		case "v1":
			expectedSig = kv[1]
		}
	}

	if timestampStr == "" || expectedSig == "" {
		return false
	}

	ts, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		return false
	}

	if toleranceSeconds > 0 {
		now := time.Now().Unix()
		diff := now - ts
		if diff > toleranceSeconds || diff < -toleranceSeconds {
			return false
		}
	}

	toSign := fmt.Sprintf("%d.%s", ts, string(payload))
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(toSign))
	actualSig := hex.EncodeToString(mac.Sum(nil))

	return hmac.Equal([]byte(actualSig), []byte(expectedSig))
}

// VerifySignatureWithSecrets validates an HMAC-SHA256 signature header against multiple candidate secrets.
// This enables zero-downtime secret rotation during transition grace periods.
func VerifySignatureWithSecrets(secrets []string, header string, payload []byte, toleranceSeconds int64) bool {
	if len(secrets) == 0 || header == "" {
		return false
	}
	for _, secret := range secrets {
		if secret != "" && VerifySignature(secret, header, payload, toleranceSeconds) {
			return true
		}
	}
	return false
}
