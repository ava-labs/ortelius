package indexedlist

import (
	"container/list"
	"sync"
)

type IndexedList interface {
	PushFront(key, val interface{})
	Exists(key interface{}) bool
	Value(key interface{}) (interface{}, bool)
	Copy(f func(interface{}))
	Len() int
	First() (interface{}, bool)
}

func NewIndexedList(maxSize int) IndexedList {
	if maxSize <= 1 {
		maxSize = 1
	}
	return &indexedList{
		entryMap:  make(map[interface{}]interface{}),
		entryList: new(list.List),
		MaxSize:   maxSize,
	}
}

type indexedList struct {
	lock      sync.RWMutex
	entryMap  map[interface{}]interface{}
	entryList *list.List
	MaxSize   int
}

func (c *indexedList) PushFront(key, val interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.exists(key) {
		return
	}
	c.entryList.PushFront(key)
	c.entryMap[key] = val
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

func (c *indexedList) Value(key interface{}) (interface{}, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	val, ok := c.entryMap[key]
	return val, ok
}

func (c *indexedList) Copy(f func(interface{})) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	for e := c.entryList.Front(); e != nil; e = e.Next() {
		val := c.entryMap[e.Value]
		f(val)
	}
}

func (c *indexedList) Len() int {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.entryList.Len()
}

func (c *indexedList) First() (interface{}, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if c.entryList.Len() == 0 {
		return nil, false
	}
	e := c.entryList.Front()
	val, ok := c.entryMap[e.Value]
	return val, ok
}
