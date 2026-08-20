package webhookclient_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	"web-hook-project/sdk/go/webhookclient"
)

func TestHMAC_SignAndVerify(t *testing.T) {
	secret := "whsec_test_secret_12345"
	payload := []byte(`{"event":"order.completed","amount":50000}`)
	now := time.Now().Unix()

	header := webhookclient.SignPayload(secret, now, payload)
	if header == "" {
		t.Fatal("expected non-empty signature header")
	}

	expectedPrefix := fmt.Sprintf("t=%d,v1=", now)
	if len(header) <= len(expectedPrefix) || header[:len(expectedPrefix)] != expectedPrefix {
		t.Fatalf("expected header to start with %q, got %q", expectedPrefix, header)
	}

	valid := webhookclient.VerifySignature(secret, header, payload, 300)
	if !valid {
		t.Fatal("expected signature verification to succeed for valid signature")
	}

	tamperedPayload := []byte(`{"event":"order.completed","amount":99999}`)
	if webhookclient.VerifySignature(secret, header, tamperedPayload, 300) {
		t.Fatal("expected verification to fail for tampered payload")
	}

	wrongSecret := "whsec_wrong_secret_67890"
	if webhookclient.VerifySignature(wrongSecret, header, payload, 300) {
		t.Fatal("expected verification to fail for wrong secret")
	}

	tamperedHeader := header + "ab"
	if webhookclient.VerifySignature(secret, tamperedHeader, payload, 300) {
		t.Fatal("expected verification to fail for tampered signature header")
	}
}

func TestHMAC_ToleranceAndExpiry(t *testing.T) {
	secret := "whsec_expiry_secret_abc"
	payload := []byte(`{"event":"test.expiry"}`)
	now := time.Now().Unix()

	// 1. Signature from 10 seconds ago with 30s tolerance -> Valid
	t10Ago := now - 10
	h1 := webhookclient.SignPayload(secret, t10Ago, payload)
	if !webhookclient.VerifySignature(secret, h1, payload, 30) {
		t.Fatal("expected signature from 10s ago to be valid with 30s tolerance")
	}

	// 2. Signature from 100 seconds ago with 30s tolerance -> Expired
	t100Ago := now - 100
	h2 := webhookclient.SignPayload(secret, t100Ago, payload)
	if webhookclient.VerifySignature(secret, h2, payload, 30) {
		t.Fatal("expected signature from 100s ago to fail with 30s tolerance")
	}

	// 3. Tolerance <= 0 disables expiration check
	if !webhookclient.VerifySignature(secret, h2, payload, 0) {
		t.Fatal("expected verification with tolerance=0 to bypass expiration check")
	}
	if !webhookclient.VerifySignature(secret, h2, payload, -1) {
		t.Fatal("expected verification with tolerance < 0 to bypass expiration check")
	}

	// 4. Future timestamp (clock skew > tolerance) -> Invalid
	tFuture := now + 100
	hFuture := webhookclient.SignPayload(secret, tFuture, payload)
	if webhookclient.VerifySignature(secret, hFuture, payload, 30) {
		t.Fatal("expected signature far in the future to fail when exceeding tolerance")
	}
}

func TestHMAC_MalformedHeaders(t *testing.T) {
	secret := "whsec_malformed_secret"
	payload := []byte(`{"event":"test.malformed"}`)

	malformedHeaders := []string{
		"",
		"invalid_header",
		"t=not_a_number,v1=abcdef",
		"v1=abcdef",
		"t=123456789",
		"t=123456789,v2=abcdef",
		"t=123456789,v1=",
		",,,,",
		"t=,v1=",
		"t=-invalid,v1=1234",
	}

	for _, hdr := range malformedHeaders {
		t.Run("header: "+hdr, func(t *testing.T) {
			if webhookclient.VerifySignature(secret, hdr, payload, 300) {
				t.Fatalf("expected verification to fail for malformed header: %q", hdr)
			}
		})
	}
}

func TestHMAC_KnownVector(t *testing.T) {
	secret := "super-secret-key"
	var ts int64 = 1700000000
	payload := []byte(`{"hello":"world"}`)

	// Compute expected signature manually:
	// toSign = "1700000000.{\"hello\":\"world\"}"
	toSign := fmt.Sprintf("%d.%s", ts, string(payload))
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(toSign))
	expectedSig := hex.EncodeToString(mac.Sum(nil))
	expectedHeader := fmt.Sprintf("t=%d,v1=%s", ts, expectedSig)

	generatedHeader := webhookclient.SignPayload(secret, ts, payload)
	if generatedHeader != expectedHeader {
		t.Fatalf("expected header %q, got %q", expectedHeader, generatedHeader)
	}

	// Verify using tolerance = 0 (bypassing time check)
	if !webhookclient.VerifySignature(secret, generatedHeader, payload, 0) {
		t.Fatal("expected known vector verification to succeed")
	}

	// Verify with whitespace in header parts
	spacedHeader := fmt.Sprintf("t=%d, v1=%s", ts, expectedSig)
	if !webhookclient.VerifySignature(secret, spacedHeader, payload, 0) {
		t.Fatal("expected verification with spaced header to succeed")
	}
}

func TestHMAC_Concurrency(t *testing.T) {
	secret := "whsec_concurrent_secret"
	payload := []byte(`{"test":"concurrency"}`)
	now := time.Now().Unix()

	var wg sync.WaitGroup
	numWorkers := 50

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			hdr := webhookclient.SignPayload(secret, now, payload)
			if !webhookclient.VerifySignature(secret, hdr, payload, 300) {
				t.Errorf("worker %d failed signature verification", idx)
			}
		}(i)
	}

	wg.Wait()
}
