package server

import (
	"container/list"
	"sync"
)

// engineCache is a small capacity-bounded LRU keyed by flow id. The
// underlying value is *flow.Engine (kept as `any` to avoid an import
// cycle with this package's type declarations).
//
// All operations take O(1). Eviction happens on Set when len > cap;
// the least-recently-used entry is dropped. Touch (via Get) is
// linear-time-free.
//
// A non-positive cap disables bounding — the cache behaves like a
// plain map and never evicts. Useful for tests + small deployments.
type engineCache struct {
	mu    sync.Mutex
	cap   int
	items map[string]*list.Element // key → list element
	order *list.List               // MRU at front, LRU at back
}

// cacheEntry is the value stored in each list element. Holding the
// key here lets us delete from `items` when an entry is evicted from
// the list without walking the map.
type cacheEntry struct {
	key   string
	value any
}

func newEngineCache(cap int) *engineCache {
	return &engineCache{
		cap:   cap,
		items: make(map[string]*list.Element),
		order: list.New(),
	}
}

// Get returns the cached value for key (if any) and marks the entry
// as most-recently-used.
func (c *engineCache) Get(key string) (any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(el)
	return el.Value.(*cacheEntry).value, true
}

// Set inserts or replaces key=value. If the cache is full and the
// key is new, the LRU entry is evicted to make room.
func (c *engineCache) Set(key string, value any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		el.Value.(*cacheEntry).value = value
		c.order.MoveToFront(el)
		return
	}
	entry := &cacheEntry{key: key, value: value}
	el := c.order.PushFront(entry)
	c.items[key] = el
	if c.cap > 0 && c.order.Len() > c.cap {
		c.evictLRU()
	}
}

// Delete drops key from the cache. Idempotent.
func (c *engineCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		return
	}
	c.order.Remove(el)
	delete(c.items, key)
}

// Len returns the current number of cached entries (without touching
// LRU order).
func (c *engineCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}

// evictLRU drops the least-recently-used entry. Caller must hold mu.
func (c *engineCache) evictLRU() {
	el := c.order.Back()
	if el == nil {
		return
	}
	entry := el.Value.(*cacheEntry)
	c.order.Remove(el)
	delete(c.items, entry.key)
}
