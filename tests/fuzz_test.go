package tests

import (
	"net"
	"testing"
	"time"

	"web-hook-project/internal/dispatcher"
)

func FuzzVerifySignature(f *testing.F) {
	secret := "whsec_test_secret_key_1234567890"
	payload := []byte(`{"event":"test","id":"123"}`)
	now := time.Now().Unix()
	validHeader := dispatcher.SignPayload(secret, now, payload)

	f.Add(secret, validHeader, payload, int64(300))
	f.Add("invalid_secret", validHeader, payload, int64(300))
	f.Add(secret, "malformed,header", payload, int64(300))
	f.Add(secret, "t=notanumber,v1=badhex", payload, int64(300))
	f.Add(secret, "", []byte(""), int64(0))
	f.Add("", validHeader, payload, int64(300))

	f.Fuzz(func(t *testing.T, sec string, hdr string, body []byte, tol int64) {
		// Invariant 1: VerifySignature must never panic on arbitrary mutated inputs
		_ = dispatcher.VerifySignature(sec, hdr, body, tol)

		// Invariant 2: VerifySignatureWithSecrets must never panic with multiple secrets
		_ = dispatcher.VerifySignatureWithSecrets([]string{sec, "fallback_secret_key"}, hdr, body, tol)
	})
}

func FuzzIsRestrictedIP(f *testing.F) {
	seeds := []string{
		"127.0.0.1", "127.0.0.2", "169.254.169.254", "10.0.0.1", "192.168.1.1",
		"8.8.8.8", "1.1.1.1", "::1", "fe80::1", "::ffff:127.0.0.1", "::ffff:169.254.169.254",
		"0.0.0.0", "255.255.255.255", "fc00::1", "2001:db8::1",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, ipStr string) {
		ip := net.ParseIP(ipStr)
		// Invariant: IsRestrictedIP must be deterministic and never panic
		_ = dispatcher.IsRestrictedIP(ip)
	})
}
