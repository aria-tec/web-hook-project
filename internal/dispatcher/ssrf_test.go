package dispatcher_test

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"web-hook-project/internal/dispatcher"
)

func TestSSRF_IsRestrictedIP(t *testing.T) {
	blockedIPs := []struct {
		ipStr  string
		reason string
	}{
		// IPv4 Loopback
		{"127.0.0.1", "IPv4 loopback"},
		{"127.0.0.2", "IPv4 loopback range"},
		{"127.255.255.255", "IPv4 loopback broadcast"},

		// RFC1918 Private IPv4
		{"10.0.0.1", "RFC1918 10.0.0.0/8"},
		{"10.255.255.255", "RFC1918 10.0.0.0/8"},
		{"172.16.0.1", "RFC1918 172.16.0.0/12 lower"},
		{"172.31.255.254", "RFC1918 172.16.0.0/12 upper"},
		{"192.168.0.1", "RFC1918 192.168.0.0/16"},
		{"192.168.254.254", "RFC1918 192.168.0.0/16"},

		// Cloud Metadata / Link-Local IPv4 (RFC3927)
		{"169.254.169.254", "AWS/GCP/Azure metadata IP"},
		{"169.254.1.1", "RFC3927 link-local"},

		// IPv4 Unspecified and Multicast
		{"0.0.0.0", "IPv4 unspecified"},
		{"224.0.0.1", "IPv4 multicast"},
		{"239.255.255.250", "IPv4 multicast SSDP"},

		// IPv6 Loopback & Unspecified
		{"::1", "IPv6 loopback"},
		{"::", "IPv6 unspecified"},

		// IPv6 Link-Local & Unique Local
		{"fe80::1", "IPv6 link-local fe80::/10"},
		{"fe80::dead:beef:1234", "IPv6 link-local fe80::/10"},
		{"fc00::1", "IPv6 unique local fc00::/7"},
		{"fd12:3456:789a::1", "IPv6 unique local fc00::/7"},

		// IPv6 Multicast
		{"ff02::1", "IPv6 multicast"},

		// IPv4-mapped IPv6
		{"::ffff:127.0.0.1", "IPv4-mapped IPv6 loopback"},
		{"::ffff:169.254.169.254", "IPv4-mapped IPv6 cloud metadata"},
		{"::ffff:10.0.0.1", "IPv4-mapped IPv6 RFC1918"},
		{"::ffff:192.168.1.1", "IPv4-mapped IPv6 RFC1918"},
	}

	for _, tc := range blockedIPs {
		t.Run("blocked_"+tc.ipStr, func(t *testing.T) {
			ip := net.ParseIP(tc.ipStr)
			if ip == nil {
				t.Fatalf("failed to parse test IP: %s", tc.ipStr)
			}
			if !dispatcher.IsRestrictedIP(ip) {
				t.Errorf("expected IP %s (%s) to be restricted/blocked", tc.ipStr, tc.reason)
			}
		})
	}

	// Test nil IP handling
	t.Run("blocked_nil_ip", func(t *testing.T) {
		if !dispatcher.IsRestrictedIP(nil) {
			t.Errorf("expected nil IP to be restricted/blocked")
		}
	})

	allowedIPs := []struct {
		ipStr  string
		reason string
	}{
		{"8.8.8.8", "Google Public DNS"},
		{"8.8.4.4", "Google Public DNS Secondary"},
		{"1.1.1.1", "Cloudflare DNS"},
		{"1.0.0.1", "Cloudflare DNS Secondary"},
		{"104.244.42.1", "Twitter / X Public IP"},
		{"93.184.216.34", "example.com Public IP"},
		{"2606:4700:4700::1111", "Cloudflare Public IPv6"},
		{"2001:4860:4860::8888", "Google Public IPv6"},
	}

	for _, tc := range allowedIPs {
		t.Run("allowed_"+tc.ipStr, func(t *testing.T) {
			ip := net.ParseIP(tc.ipStr)
			if ip == nil {
				t.Fatalf("failed to parse test IP: %s", tc.ipStr)
			}
			if dispatcher.IsRestrictedIP(ip) {
				t.Errorf("expected public IP %s (%s) to be allowed", tc.ipStr, tc.reason)
			}
		})
	}
}

func TestSSRF_NewSafeHTTPClient_Configuration(t *testing.T) {
	client := dispatcher.NewSafeHTTPClient(5 * time.Second)
	if client == nil {
		t.Fatal("expected non-nil http.Client")
	}
	if client.Timeout != 5*time.Second {
		t.Fatalf("expected timeout 5s, got %v", client.Timeout)
	}

	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("expected *http.Transport, got %T", client.Transport)
	}

	if transport.MaxIdleConns != 2000 {
		t.Fatalf("expected MaxIdleConns=2000, got %d", transport.MaxIdleConns)
	}
	if transport.MaxIdleConnsPerHost != 200 {
		t.Fatalf("expected MaxIdleConnsPerHost=200, got %d", transport.MaxIdleConnsPerHost)
	}
	if transport.IdleConnTimeout != 90*time.Second {
		t.Fatalf("expected IdleConnTimeout=90s, got %v", transport.IdleConnTimeout)
	}
}

func TestSSRF_NewSafeHTTPClient_BlocksRestrictedRequests(t *testing.T) {
	// Start a local test server on 127.0.0.1
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	client := dispatcher.NewSafeHTTPClient(2 * time.Second)

	// Attempting to call the local httptest server must be blocked by SSRF protection
	_, err := client.Get(server.URL)
	if err == nil {
		t.Fatal("expected request to local test server (127.0.0.1) to be blocked by SSRF filter, got success")
	}

	if !errors.Is(err, dispatcher.ErrRestrictedDestination) {
		t.Logf("got error: %v", err)
		// Error should contain or wrap ErrRestrictedDestination
		if !errors.Is(err, dispatcher.ErrRestrictedDestination) {
			t.Fatalf("expected error to be or wrap ErrRestrictedDestination, got: %v", err)
		}
	}
}
