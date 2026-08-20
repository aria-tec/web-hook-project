package dispatcher

import (
	"context"
	"errors"
	"net"
	"net/http"
	"syscall"
	"time"
)

var (
	// ErrRestrictedDestination is returned when an outbound request targets a restricted/private IP address.
	ErrRestrictedDestination = errors.New("destination IP is restricted (SSRF protection)")

	// privateIPBlocks contains CIDRs for loopback, private networks, cloud metadata, and link-local ranges.
	privateIPBlocks []*net.IPNet
)

func init() {
	restrictedCIDRs := []string{
		"127.0.0.0/8",       // IPv4 loopback (RFC 1122)
		"10.0.0.0/8",        // IPv4 private RFC 1918
		"172.16.0.0/12",     // IPv4 private RFC 1918
		"192.168.0.0/16",    // IPv4 private RFC 1918
		"169.254.0.0/16",    // IPv4 link-local & cloud metadata (RFC 3927)
		"0.0.0.0/8",         // IPv4 current network
		"100.64.0.0/10",     // IPv4 carrier-grade NAT (RFC 6598)
		"192.0.0.0/24",      // IPv4 IETF protocol assignments
		"192.0.2.0/24",      // IPv4 TEST-NET-1 (RFC 5737)
		"198.18.0.0/15",     // IPv4 benchmark tests (RFC 2544)
		"198.51.100.0/24",   // IPv4 TEST-NET-2 (RFC 5737)
		"203.0.113.0/24",    // IPv4 TEST-NET-3 (RFC 5737)
		"224.0.0.0/4",       // IPv4 multicast (RFC 5771)
		"240.0.0.0/4",       // IPv4 reserved for future use (RFC 1112)
		"255.255.255.255/32", // IPv4 broadcast
		"::1/128",           // IPv6 loopback (RFC 4291)
		"::/128",            // IPv6 unspecified
		"fe80::/10",         // IPv6 link-local unicast (RFC 4291)
		"fc00::/7",          // IPv6 unique local (RFC 4193)
		"ff00::/8",          // IPv6 multicast (RFC 4291)
		"2001:db8::/32",     // IPv6 documentation (RFC 3849)
	}

	for _, cidr := range restrictedCIDRs {
		_, block, err := net.ParseCIDR(cidr)
		if err == nil {
			privateIPBlocks = append(privateIPBlocks, block)
		}
	}
}

// IsRestrictedIP checks if an IP belongs to private, loopback, link-local, cloud metadata, or multicast ranges.
func IsRestrictedIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	// Normalize IPv4-mapped IPv6 (e.g. ::ffff:127.0.0.1 -> 127.0.0.1)
	if ipv4 := ip.To4(); ipv4 != nil {
		ip = ipv4
	}
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() || ip.IsPrivate() {
		return true
	}
	for _, block := range privateIPBlocks {
		if block.Contains(ip) {
			return true
		}
	}
	return false
}

// NewSafeHTTPClient returns an *http.Client configured with SSRF protection and high-throughput connection pooling.
func NewSafeHTTPClient(timeout time.Duration) *http.Client {
	return NewSafeHTTPClientWithAllowPrivate(timeout, false)
}

// NewSafeHTTPClientWithAllowPrivate returns an *http.Client configured with SSRF protection and optional allowance of private IP ranges (for local demo & test environments).
func NewSafeHTTPClientWithAllowPrivate(timeout time.Duration, allowPrivate bool) *http.Client {
	dialer := &net.Dialer{
		Timeout:   timeout,
		KeepAlive: 30 * time.Second,
		Control: func(network, address string, c syscall.RawConn) error {
			if allowPrivate {
				return nil
			}
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}
			ip := net.ParseIP(host)
			if ip != nil && IsRestrictedIP(ip) {
				return ErrRestrictedDestination
			}
			return nil
		},
	}

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}

			// If host is an IP literal, validate immediately
			if ip := net.ParseIP(host); ip != nil {
				if !allowPrivate && IsRestrictedIP(ip) {
					return nil, ErrRestrictedDestination
				}
				return dialer.DialContext(ctx, network, addr)
			}

			if allowPrivate {
				return dialer.DialContext(ctx, network, net.JoinHostPort(host, port))
			}

			// Resolve host DNS and validate all returned IP addresses
			ips, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
			if err != nil {
				return nil, err
			}
			if len(ips) == 0 {
				return nil, errors.New("no IP addresses found for destination host")
			}

			for _, ip := range ips {
				if IsRestrictedIP(ip) {
					return nil, ErrRestrictedDestination
				}
			}

			return dialer.DialContext(ctx, network, net.JoinHostPort(host, port))
		},
		MaxIdleConns:        2000,
		MaxIdleConnsPerHost: 200,
		IdleConnTimeout:     90 * time.Second,
	}

	return &http.Client{
		Transport: transport,
		Timeout:   timeout,
	}
}
