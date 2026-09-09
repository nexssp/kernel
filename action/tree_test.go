package action_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
)

// Compile-time interface verifications for test fixtures
var (
	_ action.AnyAction = (*mockGraphNode)(nil)
	_ action.Composite = (*mockGraphNode)(nil)
	_ action.AnyAction = (*mockFixedHookNode)(nil)
	_ action.Composite = (*mockFixedHookNode)(nil)
)

type mockGraphNode struct {
	action.AnyAction
	children []action.AnyAction
}

func (m *mockGraphNode) Children() []action.AnyAction {
	return m.children
}

// mockFixedHookNode has an in-place fixed array for hooks so testing inside
// AllocsPerRun does not cause heap allocations through dynamic slice resizing.
type mockFixedHookNode struct {
	name     string
	children []action.AnyAction
	hooks    [8]action.AnyHook
	hookLen  int
}

func (m *mockFixedHookNode) DoAny(ctx context.Context, req any) (any, error) { return req, nil }
func (m *mockFixedHookNode) Describe() *action.Meta                          { return &action.Meta{Name: m.name} }
func (m *mockFixedHookNode) GetBindings() []action.Binding                   { return nil }
func (m *mockFixedHookNode) ExecuteDecoded(_ context.Context, _ action.DecodeFunc) (any, error) {
	return nil, nil
}
func (m *mockFixedHookNode) AddAnyHook(hooks ...action.AnyHook) {
	for i := 0; i < len(hooks); i++ {
		if m.hookLen < len(m.hooks) {
			m.hooks[m.hookLen] = hooks[i]
			m.hookLen++
		}
	}
}
func (m *mockFixedHookNode) GetAnyHooks() []action.AnyHook {
	return m.hooks[:m.hookLen]
}
func (m *mockFixedHookNode) Children() []action.AnyAction {
	return m.children
}
func (m *mockFixedHookNode) ResetHooks() {
	m.hookLen = 0
}

func TestApplyTreeHook_Deduplication_DiamondDAG(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	var dCalls atomic.Int32
	var bCalls atomic.Int32
	var cCalls atomic.Int32

	d := action.New("D", func(_ context.Context, _ any) (string, error) {
		return "D", nil
	}).Build()

	b := &mockGraphNode{
		AnyAction: action.New("B", func(_ context.Context, _ any) (string, error) { return "B", nil }).Build(),
		children:  []action.AnyAction{d},
	}

	c := &mockGraphNode{
		AnyAction: action.New("C", func(_ context.Context, _ any) (string, error) { return "C", nil }).Build(),
		children:  []action.AnyAction{d},
	}

	root := &mockGraphNode{
		AnyAction: action.New("A", func(_ context.Context, _ any) (string, error) { return "A", nil }).Build(),
		children:  []action.AnyAction{b, c},
	}

	hook := action.AnyHook{
		Before: func(c context.Context, _ any, meta *action.Meta) (context.Context, error) {
			switch meta.Name {
			case "D":
				dCalls.Add(1)
			case "B":
				bCalls.Add(1)
			case "C":
				cCalls.Add(1)
			}
			return c, nil
		},
	}

	if err := action.ApplyTreeHook(root, hook); err != nil {
		t.Fatalf("ApplyTreeHook failed: %v", err)
	}

	_, _ = d.DoAny(ctx, nil)
	_, _ = b.DoAny(ctx, nil)
	_, _ = c.DoAny(ctx, nil)

	if dCalls.Load() != 1 {
		t.Fatalf("diamond node D bound hook %d times, expected exactly 1", dCalls.Load())
	}
	if bCalls.Load() != 1 || cCalls.Load() != 1 {
		t.Fatalf("branch nodes B or C failed hook invocation count")
	}
}

func TestApplyTreeHook_CycleProtection(t *testing.T) {
	t.Parallel()

	nodeA := &mockGraphNode{
		AnyAction: action.New("A", func(_ context.Context, _ any) (string, error) { return "A", nil }).Build(),
	}
	nodeB := &mockGraphNode{
		AnyAction: action.New("B", func(_ context.Context, _ any) (string, error) { return "B", nil }).Build(),
	}

	nodeA.children = []action.AnyAction{nodeB}
	nodeB.children = []action.AnyAction{nodeA}

	hook := action.AnyHook{}
	done := make(chan error, 1)
	go func() {
		done <- action.ApplyTreeHook(nodeA, hook)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected cycle to be safely broken, got: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("tree hook recursion trapped in infinite loop on cyclic DAG")
	}
}

func TestApplyTreeHook_MaxDepthExceeded(t *testing.T) {
	t.Parallel()

	var current action.AnyAction = action.New("leaf", func(_ context.Context, _ any) (string, error) {
		return "", nil
	}).Build()

	for i := 0; i < 20; i++ {
		current = &mockGraphNode{
			AnyAction: action.New("step", func(_ context.Context, _ any) (string, error) { return "", nil }).Build(),
			children:  []action.AnyAction{current},
		}
	}

	err := action.ApplyTreeHookOpts(current, action.TreeHookOptions{MaxDepth: 10}, action.AnyHook{})
	if err == nil || !strings.Contains(err.Error(), "max depth exceeded") {
		t.Fatalf("expected max depth exceeded error, got: %v", err)
	}
}

func TestApplyTreeHook_NestedProxyChains(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	var calls atomic.Int32

	leaf := action.New("leaf", func(_ context.Context, _ any) (string, error) {
		return "ok", nil
	}).Build()

	p1 := action.NewProxy(leaf)
	p2 := action.NewProxy(p1)
	pRoot := action.NewProxy(p2)

	hook := action.AnyHook{
		Before: func(c context.Context, _ any, meta *action.Meta) (context.Context, error) {
			if meta.Name == "leaf" {
				calls.Add(1)
			}
			return c, nil
		},
	}

	if err := action.ApplyTreeHook(pRoot, hook); err != nil {
		t.Fatalf("ApplyTreeHook failed: %v", err)
	}

	_, _ = pRoot.DoAny(ctx, nil)

	if calls.Load() != 1 {
		t.Fatalf("nested proxy unwrap failed, expected 1 invocation, got: %d", calls.Load())
	}
}

func BenchmarkApplyTreeHook_DeepLinearDAG(b *testing.B) {
	var current action.AnyAction = action.New("leaf", func(_ context.Context, _ any) (string, error) {
		return "", nil
	}).Build()

	for i := 0; i < 16; i++ {
		current = &mockGraphNode{
			AnyAction: action.New("step", func(_ context.Context, _ any) (string, error) { return "", nil }).Build(),
			children:  []action.AnyAction{current},
		}
	}

	hook := action.AnyHook{}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = action.ApplyTreeHook(current, hook)
	}
}

func TestApplyTreeHook_Linear_ZeroAlloc(t *testing.T) {
	nodes := make([]mockFixedHookNode, 11)
	for i := 0; i < 10; i++ {
		nodes[i] = mockFixedHookNode{
			name:     "step",
			children: []action.AnyAction{&nodes[i+1]},
		}
	}
	nodes[10] = mockFixedHookNode{name: "leaf"}

	root := &nodes[0]
	hookSlice := []action.AnyHook{{}}
	opts := action.TreeHookOptions{MaxDepth: action.DefaultMaxTreeDepth}

	for i := 0; i < 10; i++ {
		_ = action.ApplyTreeHookOpts(root, opts, hookSlice...)
		for j := range nodes {
			nodes[j].ResetHooks()
		}
	}

	allocs := testing.AllocsPerRun(1000, func() {
		_ = action.ApplyTreeHookOpts(root, opts, hookSlice...)
		for j := range nodes {
			nodes[j].ResetHooks()
		}
	})

	if allocs != 0 {
		t.Fatalf("expected 0 allocs/op for ApplyTreeHook on depth <= 32, got %v", allocs)
	}
}

func TestApplyTreeHook_DiamondDAG_ZeroAlloc(t *testing.T) {
	d := &mockFixedHookNode{name: "D"}
	b := &mockFixedHookNode{name: "B", children: []action.AnyAction{d}}
	c := &mockFixedHookNode{name: "C", children: []action.AnyAction{d}}
	root := &mockFixedHookNode{name: "A", children: []action.AnyAction{b, c}}

	hookSlice := []action.AnyHook{{}}
	opts := action.TreeHookOptions{MaxDepth: action.DefaultMaxTreeDepth}

	resetAll := func() {
		root.ResetHooks()
		b.ResetHooks()
		c.ResetHooks()
		d.ResetHooks()
	}

	for i := 0; i < 10; i++ {
		_ = action.ApplyTreeHookOpts(root, opts, hookSlice...)
		resetAll()
	}

	allocs := testing.AllocsPerRun(1000, func() {
		_ = action.ApplyTreeHookOpts(root, opts, hookSlice...)
		resetAll()
	})

	if allocs != 0 {
		t.Fatalf("expected 0 allocs/op for Diamond DAG tree hook application, got %v", allocs)
	}
}
