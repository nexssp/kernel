package xctx

import (
	"context"
	"crypto/rand"
	"io"
	"time"
)

type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time {
	return time.Now()
}

var (
	clockKey   = NewKey[Clock]("kernel.clock")
	entropyKey = NewKey[io.Reader]("kernel.entropy")
	sysClock   = systemClock{}
)

func WithClock(ctx context.Context, clock Clock) context.Context {
	if clock == nil {
		return ctx
	}
	return clockKey.With(ctx, clock)
}

func WithEntropy(ctx context.Context, reader io.Reader) context.Context {
	if reader == nil {
		return ctx
	}
	return entropyKey.With(ctx, reader)
}

func Now(ctx context.Context) time.Time {
	if clock, ok := clockKey.From(ctx); ok && clock != nil {
		return clock.Now()
	}
	return sysClock.Now()
}

func RandomBytes(ctx context.Context, destination []byte) error {
	if reader, ok := entropyKey.From(ctx); ok && reader != nil {
		_, err := io.ReadFull(reader, destination)
		return err
	}
	_, err := rand.Read(destination)
	return err
}
