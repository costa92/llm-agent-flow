package server

import "testing"

func TestEngineCacheUnboundedWhenCapNonPositive(t *testing.T) {
	c := newEngineCache(0)
	for i := 0; i < 5; i++ {
		c.Set(string(rune('a'+i)), i)
	}
	if got := c.Len(); got != 5 {
		t.Fatalf("Len = %d, want 5 (cap=0 disables bounding)", got)
	}
}

func TestEngineCacheLRUEviction(t *testing.T) {
	c := newEngineCache(3)
	c.Set("a", 1)
	c.Set("b", 2)
	c.Set("c", 3)
	// Touch "a" so it becomes MRU again.
	if _, ok := c.Get("a"); !ok {
		t.Fatal("a missing")
	}
	c.Set("d", 4)
	// "b" should be evicted (LRU after the "a" touch).
	if _, ok := c.Get("b"); ok {
		t.Fatal("b should have been evicted")
	}
	if _, ok := c.Get("a"); !ok {
		t.Fatal("a should still be present (MRU)")
	}
	if got := c.Len(); got != 3 {
		t.Fatalf("Len = %d, want 3", got)
	}
}

func TestEngineCacheSetOverwrites(t *testing.T) {
	c := newEngineCache(2)
	c.Set("a", 1)
	c.Set("a", 2)
	if got := c.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1 after overwrite", got)
	}
	v, _ := c.Get("a")
	if v != 2 {
		t.Fatalf("Get = %v, want 2", v)
	}
}

func TestEngineCacheDeleteIdempotent(t *testing.T) {
	c := newEngineCache(4)
	c.Set("a", 1)
	c.Delete("a")
	c.Delete("a") // no-op
	if _, ok := c.Get("a"); ok {
		t.Fatal("a should be gone")
	}
}

func TestEngineCacheDeleteAtCapacity(t *testing.T) {
	c := newEngineCache(2)
	c.Set("a", 1)
	c.Set("b", 2)
	c.Delete("a")
	c.Set("c", 3)
	if got := c.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2 (delete should have freed a slot)", got)
	}
}
