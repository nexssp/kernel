// Package pmap provides an immutable persistent AVL map keyed by strings.
package pmap

// Map is an immutable persistent AVL map from string keys to values of type V.
//
// Its zero value is an empty map. Updates return a new map and never modify the
// receiver or any node reachable from it, so existing roots remain usable.
// A Map should be treated as immutable; its roots can safely be shared between
// goroutines provided values stored in it are themselves used safely.
type Map[V any] struct {
	root *node[V]
}

type node[V any] struct {
	key         string
	value       V
	left, right *node[V]
	height      int
	size        int
}

// New returns an empty map.
func New[V any]() *Map[V] {
	return &Map[V]{}
}

// Get returns the value associated with key and whether key is present.
// It is safe to call Get on a nil *Map.
func (m *Map[V]) Get(key string) (V, bool) {
	var zero V
	if m == nil {
		return zero, false
	}
	for current := m.root; current != nil; {
		switch {
		case key < current.key:
			current = current.left
		case key > current.key:
			current = current.right
		default:
			return current.value, true
		}
	}
	return zero, false
}

// Set returns a new map containing key mapped to value. The receiver and all
// previously returned maps are left unchanged. If key already exists, its
// value is replaced without changing the map's length. Set is also safe on a
// nil *Map; in that case it behaves like an update to an empty map.
func (m *Map[V]) Set(key string, value V) *Map[V] {
	var root *node[V]
	if m != nil {
		root = m.root
	}
	return &Map[V]{root: insert(root, key, value)}
}

// Len returns the number of keys in the map. It is safe to call Len on a nil
// *Map.
func (m *Map[V]) Len() int {
	if m == nil {
		return 0
	}
	return nodeSize(m.root)
}

// Range calls yield for each key/value pair in ascending key order. If yield
// returns false, Range stops immediately. A nil callback is treated as an
// immediate stop. It is safe to call Range on a nil *Map.
func (m *Map[V]) Range(yield func(key string, value V) bool) {
	if m == nil || yield == nil {
		return
	}
	var walk func(*node[V]) bool
	walk = func(current *node[V]) bool {
		if current == nil {
			return true
		}
		if !walk(current.left) || !yield(current.key, current.value) {
			return false
		}
		return walk(current.right)
	}
	walk(m.root)
}

func insert[V any](current *node[V], key string, value V) *node[V] {
	if current == nil {
		return makeNode(key, value, nil, nil)
	}
	switch {
	case key < current.key:
		return rebalance(makeNode(current.key, current.value,
			insert(current.left, key, value), current.right))
	case key > current.key:
		return rebalance(makeNode(current.key, current.value,
			current.left, insert(current.right, key, value)))
	default:
		return makeNode(key, value, current.left, current.right)
	}
}

func rebalance[V any](current *node[V]) *node[V] {
	balance := nodeHeight(current.left) - nodeHeight(current.right)
	if balance > 1 {
		if nodeHeight(current.left.left) < nodeHeight(current.left.right) {
			left := rotateLeft(current.left)
			current = makeNode(current.key, current.value, left, current.right)
		}
		return rotateRight(current)
	}
	if balance < -1 {
		if nodeHeight(current.right.right) < nodeHeight(current.right.left) {
			right := rotateRight(current.right)
			current = makeNode(current.key, current.value, current.left, right)
		}
		return rotateLeft(current)
	}
	return current
}

func rotateLeft[V any](root *node[V]) *node[V] {
	pivot := root.right
	newLeft := makeNode(root.key, root.value, root.left, pivot.left)
	return makeNode(pivot.key, pivot.value, newLeft, pivot.right)
}

func rotateRight[V any](root *node[V]) *node[V] {
	pivot := root.left
	newRight := makeNode(root.key, root.value, pivot.right, root.right)
	return makeNode(pivot.key, pivot.value, pivot.left, newRight)
}

func makeNode[V any](key string, value V, left, right *node[V]) *node[V] {
	height := nodeHeight(left)
	if nodeHeight(right) > height {
		height = nodeHeight(right)
	}
	return &node[V]{
		key:    key,
		value:  value,
		left:   left,
		right:  right,
		height: height + 1,
		size:   nodeSize(left) + nodeSize(right) + 1,
	}
}

func nodeHeight[V any](current *node[V]) int {
	if current == nil {
		return 0
	}
	return current.height
}

func nodeSize[V any](current *node[V]) int {
	if current == nil {
		return 0
	}
	return current.size
}
