package action

import "testing"

func TestDefaultCacheEvictionOnlyForNewKeys(t *testing.T) {
	cache := newDefaultMemoryKV[int](0)
	cache.maxCapacity = 2
	cache.data["existing"] = cacheItem[int]{val: 1}
	cache.data["other"] = cacheItem[int]{val: 2}

	if cache.needsEvictionLocked("existing") {
		t.Fatal("overwriting an existing key should not require eviction")
	}
	if !cache.needsEvictionLocked("new") {
		t.Fatal("inserting a new key at capacity should require eviction")
	}
}
