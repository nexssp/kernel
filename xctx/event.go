package xctx

import "context"

type EventPublisher interface {
	PublishEvent(ctx context.Context, subject string, payload any) error
}

var EventPublisherKey = NewKey[EventPublisher]("nexss.event.publisher")

func WithEventPublisher(ctx context.Context, pub EventPublisher) context.Context {
	return EventPublisherKey.With(ctx, pub)
}

func EventPublisherFrom(ctx context.Context) (EventPublisher, bool) {
	return EventPublisherKey.From(ctx)
}
