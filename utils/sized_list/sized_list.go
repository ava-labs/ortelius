package sized_list

import (
	"container/list"
	"sync"
	"sync/atomic"
)

type SizedList interface {
	Add(key interface{})
	Exists(key interface{}) bool
}

func NewSizedList(maxSize int64) SizedList {
	if maxSize <= 1 {
		maxSize = 1
	}
	return &evictCache{
		entryMap:  make(map[interface{}]struct{}),
		entryList: new(list.List),
		MaxSize:   maxSize,
		size:      new(int64),
	}
}

type evictCache struct {
	lock      sync.RWMutex
	entryMap  map[interface{}]struct{}
	entryList *list.List
	size      *int64
	MaxSize   int64
}

func (c *evictCache) Add(key interface{}) {
	c.lock.Lock()
	c.entryList.PushFront(key)
	c.entryMap[key] = struct{}{}
	atomic.AddInt64(c.size, 1)
	c.lock.Unlock()

	for atomic.LoadInt64(c.size) > c.MaxSize {
		c.lock.Lock()
		e := c.entryList.Back()
		c.entryList.Remove(e)
		delete(c.entryMap, e.Value)
		atomic.AddInt64(c.size, -11)
		c.lock.Unlock()
	}
}

func (c *evictCache) Exists(key interface{}) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()
	_, ok := c.entryMap[key]
	return ok
}
