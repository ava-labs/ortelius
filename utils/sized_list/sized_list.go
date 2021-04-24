package sized_list

import (
	"container/list"
	"sync"
)

type SizedList interface {
	Add(key interface{})
	Exists(key interface{}) bool
}

func NewSizedList(maxSize int) SizedList {
	if maxSize <= 1 {
		maxSize = 1
	}
	return &evictCache{
		entryMap:  make(map[interface{}]struct{}),
		entryList: new(list.List),
		MaxSize:   maxSize,
	}
}

type evictCache struct {
	lock      sync.RWMutex
	entryMap  map[interface{}]struct{}
	entryList *list.List
	MaxSize   int
}

func (c *evictCache) Add(key interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.entryList.PushFront(key)
	c.entryMap[key] = struct{}{}
	for c.entryList.Len() > c.MaxSize {
		e := c.entryList.Back()
		c.entryList.Remove(e)
		delete(c.entryMap, e.Value)
	}
}

func (c *evictCache) Exists(key interface{}) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()
	_, ok := c.entryMap[key]
	return ok
}
