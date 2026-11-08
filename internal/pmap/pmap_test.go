package pmap

import (
	"fmt"
	"maps"
	"reflect"
	"sort"
	"strconv"
	"testing"
)

func TestZeroAndNilMapReads(t *testing.T) {
	var zero Map[int]
	var nilMap *Map[int]
	for name, m := range map[string]*Map[int]{"zero": &zero, "nil": nilMap, "new": New[int]()} {
		t.Run(name, func(t *testing.T) {
			if got := m.Len(); got != 0 {
				t.Fatalf("Len() = %d, want 0", got)
			}
			if value, ok := m.Get("missing"); ok || value != 0 {
				t.Fatalf("Get(missing) = (%d, %t), want (0, false)", value, ok)
			}
			calls := 0
			m.Range(func(string, int) bool { calls++; return true })
			if calls != 0 {
				t.Fatalf("Range called callback %d times on empty map", calls)
			}
		})
	}

	var nilMapPointer *Map[int]
	updated := nilMapPointer.Set("from-nil", 7)
	if got, ok := updated.Get("from-nil"); !ok || got != 7 {
		t.Fatalf("Set on nil map: Get() = (%d, %t), want (7, true)", got, ok)
	}
	if got := nilMapPointer.Len(); got != 0 {
		t.Fatalf("Set on nil map changed receiver; Len() = %d", got)
	}
}

func TestEmptyKeyNilValueAndDuplicateUpdate(t *testing.T) {
	var m *Map[any]
	first := m.Set("", nil)
	if got := first.Len(); got != 1 {
		t.Fatalf("Len after empty key insertion = %d, want 1", got)
	}
	if got, ok := first.Get(""); !ok || got != nil {
		t.Fatalf("Get(empty key) = (%#v, %t), want (nil, true)", got, ok)
	}

	second := first.Set("", "updated")
	if got := second.Len(); got != 1 {
		t.Fatalf("Len after duplicate update = %d, want 1", got)
	}
	if got, ok := second.Get(""); !ok || got != "updated" {
		t.Fatalf("updated Get(empty key) = (%#v, %t), want (updated, true)", got, ok)
	}
	if got, ok := first.Get(""); !ok || got != nil {
		t.Fatalf("old root changed after update: Get(empty key) = (%#v, %t)", got, ok)
	}
}

func TestRangeSortedAndEarlyStop(t *testing.T) {
	m := New[int]()
	for i, key := range []string{"delta", "", "alpha", "charlie", "bravo"} {
		m = m.Set(key, i)
	}
	var keys []string
	m.Range(func(key string, _ int) bool {
		keys = append(keys, key)
		return true
	})
	want := []string{"", "alpha", "bravo", "charlie", "delta"}
	if !reflect.DeepEqual(keys, want) {
		t.Fatalf("Range keys = %#v, want %#v", keys, want)
	}

	calls := 0
	m.Range(func(key string, _ int) bool {
		calls++
		return key != "bravo"
	})
	if calls != 3 {
		t.Fatalf("early-stop Range called callback %d times, want 3", calls)
	}
	m.Range(nil)
}

func TestPersistentSnapshotsAndPathCopying(t *testing.T) {
	root := New[int]()
	root = root.Set("d", 4).Set("b", 2).Set("f", 6).Set("a", 1).Set("c", 3).Set("e", 5).Set("g", 7)
	before := root
	updated := root.Set("c", 30).Set("h", 8)

	if got, ok := root.Get("c"); !ok || got != 3 {
		t.Fatalf("old root changed: Get(c) = (%d, %t), want (3, true)", got, ok)
	}
	if got, ok := updated.Get("c"); !ok || got != 30 {
		t.Fatalf("new root Get(c) = (%d, %t), want (30, true)", got, ok)
	}
	if root.Len() != 7 || updated.Len() != 8 || before.Len() != 7 {
		t.Fatalf("unexpected snapshot lengths: old=%d updated=%d before=%d", root.Len(), updated.Len(), before.Len())
	}
	if root.root != before.root {
		t.Fatal("an update changed the receiver's root pointer")
	}
	if root.root.left == nil || updated.root.left == nil || root.root.left == updated.root.left {
		t.Fatal("expected unchanged left subtree to be structurally shared")
	}
	assertValid(t, root)
	assertValid(t, updated)
}

func TestDeterministicPropertiesAgainstGoMap(t *testing.T) {
	rng := deterministicRNG{state: 0x5eed}
	current := New[int]()
	reference := make(map[string]int)
	type snapshot struct {
		root *Map[int]
		want map[string]int
	}
	var snapshots []snapshot

	for step := range 3000 {
		key := propertyKey(&rng)
		value := rng.intn(60_001) - 30_000
		current = current.Set(key, value)
		reference[key] = value

		if step%41 == 0 {
			snapshots = append(snapshots, snapshot{root: current, want: cloneMap(reference)})
		}
		if step%17 == 0 {
			assertMatches(t, current, reference)
			assertValid(t, current)
		}
		query := propertyKey(&rng)
		got, gotOK := current.Get(query)
		want, wantOK := reference[query]
		if got != want || gotOK != wantOK {
			t.Fatalf("step %d Get(%q) = (%d, %t), want (%d, %t)", step, query, got, gotOK, want, wantOK)
		}
	}
	assertMatches(t, current, reference)
	assertValid(t, current)

	for i, snap := range snapshots {
		assertMatches(t, snap.root, snap.want)
		assertValid(t, snap.root)
		if i > 0 && snap.root == nil {
			t.Fatalf("snapshot %d unexpectedly nil", i)
		}
	}
}

func TestAVLRotationsAndMetadata(t *testing.T) {
	orders := [][]int{
		{3, 2, 1}, // left-left
		{1, 2, 3}, // right-right
		{3, 1, 2}, // left-right
		{1, 3, 2}, // right-left
	}
	for _, order := range orders {
		m := New[int]()
		for _, n := range order {
			m = m.Set(strconv.Itoa(n), n)
			assertValid(t, m)
		}
		if m.root.key != "2" {
			t.Errorf("insertion order %v root key = %q, want 2", order, m.root.key)
		}
	}
}

type deterministicRNG struct {
	state uint16
}

func (rng *deterministicRNG) intn(limit uint16) int {
	value := rng.state
	value ^= value << 7
	value ^= value >> 9
	value ^= value << 8
	rng.state = value
	return int(value % limit)
}

func propertyKey(rng *deterministicRNG) string {
	// Deliberately includes the empty key, repeated keys, and varied lexicographic forms.
	if rng.intn(9) == 0 {
		return ""
	}
	const letters = "abcdef"
	return fmt.Sprintf("k%03d-%c", rng.intn(180), letters[rng.intn(6)])
}

func cloneMap(source map[string]int) map[string]int {
	clone := make(map[string]int, len(source))
	maps.Copy(clone, source)
	return clone
}

func assertMatches(t *testing.T, m *Map[int], want map[string]int) {
	t.Helper()
	if got := m.Len(); got != len(want) {
		t.Fatalf("Len() = %d, want %d", got, len(want))
	}
	keys := make([]string, 0, len(want))
	for key := range want {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	index := 0
	m.Range(func(key string, value int) bool {
		if index >= len(keys) {
			t.Fatalf("Range yielded extra pair (%q, %d)", key, value)
		}
		if key != keys[index] || value != want[key] {
			t.Fatalf("Range pair %d = (%q, %d), want (%q, %d)", index, key, value, keys[index], want[keys[index]])
		}
		index++
		return true
	})
	if index != len(keys) {
		t.Fatalf("Range yielded %d pairs, want %d", index, len(keys))
	}
	for key, value := range want {
		if got, ok := m.Get(key); !ok || got != value {
			t.Fatalf("Get(%q) = (%d, %t), want (%d, true)", key, got, ok, value)
		}
	}
}

func assertValid(t *testing.T, m *Map[int]) {
	t.Helper()
	if m == nil {
		t.Fatal("unexpected nil map")
	}
	count := 0
	last := ""
	hasLast := false
	var check func(*node[int]) (height, size int)
	check = func(current *node[int]) (int, int) {
		if current == nil {
			return 0, 0
		}
		leftHeight, leftSize := check(current.left)
		if hasLast && current.key <= last {
			t.Fatalf("in-order keys are not strictly increasing: %q after %q", current.key, last)
		}
		last, hasLast = current.key, true
		count++
		rightHeight, rightSize := check(current.right)
		wantHeight := max(leftHeight, rightHeight) + 1
		wantSize := leftSize + rightSize + 1
		if current.height != wantHeight {
			t.Fatalf("node %q cached height = %d, want %d", current.key, current.height, wantHeight)
		}
		if current.size != wantSize {
			t.Fatalf("node %q cached size = %d, want %d", current.key, current.size, wantSize)
		}
		if difference := leftHeight - rightHeight; difference < -1 || difference > 1 {
			t.Fatalf("node %q is unbalanced: left height %d, right height %d", current.key, leftHeight, rightHeight)
		}
		return wantHeight, wantSize
	}
	_, size := check(m.root)
	if count != m.Len() || size != m.Len() {
		t.Fatalf("tree contains %d nodes / cached size %d, Len() = %d", count, size, m.Len())
	}
}
