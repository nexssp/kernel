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
	if dnsErr, ok := errors.AsType[*net.DNSError](err); ok {
		return Unavailable("DNS resolution failed", dnsErr)
	}
	if netErr, ok := errors.AsType[net.Error](err); ok {
		if netErr.Timeout() {
			return Timeout("network timeout", err)
		}
		return Unavailable("network error", err)
	}
	return Unavailable("transport error", err)
}
