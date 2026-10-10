package xsync_test

import (
	"testing"

	"github.com/nexssp/kernel/xsync"
	"github.com/nexssp/kernel/xtest"
)

type Entity struct {
	ID     uint64
	Active bool
}

func TestPool_ZeroAlloc(t *testing.T) {
	p := xsync.NewPool(func() *Entity {
		return &Entity{}
	})

	// Pre-warm
	e := p.Get()
	p.Put(e)

	xtest.RequireZeroAlloc(t, 1000, func() {
		item := p.Get()
		item.ID = 42
		item.Active = true
		item.ID = 0
		item.Active = false
		p.Put(item)
	})
}
