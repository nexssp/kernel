package action

import (
	"context"
	"fmt"

	"github.com/nexssp/kernel/xctx"
)

// Emits registers automatic event emission upon successful action execution.
// Payload mapping is evaluated lazily only when an active EventPublisher is in context.
func (b *Builder[Req, Res]) Emits(subject string, mapper func(res Res) any) *Builder[Req, Res] {
	return b.HookExecuted(func(ctx context.Context, _ Req, res Res, _ *Meta) {
		// Short-circuit: Exit without payload allocation if no publisher is configured
		pub, ok := xctx.EventPublisherFrom(ctx)
		if !ok || pub == nil {
			return
		}

		// Lazy evaluation: build DTO only when sending
		var payload any = res
		if mapper != nil {
			payload = mapper(res)
		}

		if err := pub.PublishEvent(ctx, subject, payload); err != nil {
			xctx.AddTrace(ctx, fmt.Sprintf("event_emit_failed:%s: %v", subject, err))
		}
	})
}
