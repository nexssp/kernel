package xerr

import (
	"context"
	"errors"
	"net"
)

// MapTransportError converts raw transport/network errors into the xerr taxonomy.
// Call this at every adapter boundary (HTTP, NATS, gRPC).
func MapTransportError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return Timeout("operation timed out", err)
	}
	if errors.Is(err, context.Canceled) {
		return Canceled("operation canceled", err)
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return Timeout("network timeout", err)
		}
		return Unavailable("network error", err)
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return Unavailable("DNS resolution failed", dnsErr)
	}
	return Unavailable("transport error", err)
}
