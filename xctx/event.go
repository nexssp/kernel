package xctx

import "context"

// EventPublisher defines the contract for transactional or in-memory event publishing.
type EventPublisher interface {
	PublishEvent(ctx context.Context, subject string, payload any) error
}

type eventPublisherKey struct{}

// WithEventPublisher injects an event bus or outbox publisher into the context.
func WithEventPublisher(ctx context.Context, pub EventPublisher) context.Context {
	return context.WithValue(ctx, eventPublisherKey{}, pub)
}

// EventPublisherFrom retrieves the event publisher from context.
func EventPublisherFrom(ctx context.Context) (EventPublisher, bool) {
	pub, ok := ctx.Value(eventPublisherKey{}).(EventPublisher)
	return pub, ok
}
