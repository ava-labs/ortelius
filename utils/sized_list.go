package utils

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
	c.entryList.PushFront(key)
	c.entryMap[key] = struct{}{}
	c.lock.Unlock()

	for {
		c.lock.RLock()
		l := c.entryList.Len()
		c.lock.RUnlock()
		if l <= c.MaxSize {
			break
		}
		c.lock.Lock()
		l = c.entryList.Len()
		if l > c.MaxSize {
			e := c.entryList.Back()
			c.entryList.Remove(e)
			delete(c.entryMap, e)
			l--
		}
		c.lock.Unlock()
		if l <= c.MaxSize {
			break
		}
	}
}

func (c *evictCache) Exists(key interface{}) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()
	_, ok := c.entryMap[key]
	return ok
}
