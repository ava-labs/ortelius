package indexed_list

import (
	"container/list"
	"sync"
)

type IndexedList interface {
	Add(key interface{})
	Exists(key interface{}) bool
}

func NewIndexedList(maxSize int) IndexedList {
	if maxSize <= 1 {
		maxSize = 1
	}
	return &indexedList{
		entryMap:  make(map[interface{}]struct{}),
		entryList: new(list.List),
		MaxSize:   maxSize,
	}
}

type indexedList struct {
	lock      sync.RWMutex
	entryMap  map[interface{}]struct{}
	entryList *list.List
	MaxSize   int
}

func (c *indexedList) Add(key interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.exists(key) {
		return
	}
	c.entryList.PushFront(key)
	c.entryMap[key] = struct{}{}
	for c.entryList.Len() > c.MaxSize {
		e := c.entryList.Back()
		c.entryList.Remove(e)
		delete(c.entryMap, e.Value)
	}
}

func (c *indexedList) Exists(key interface{}) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.exists(key)
}

func (c *indexedList) exists(key interface{}) bool {
	_, ok := c.entryMap[key]
	return ok
}
