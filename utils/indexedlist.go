// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package utils

import (
	"container/list"
	"sync"
)

type IndexedList interface {
	PushFront(key, val interface{})
	Exists(key interface{}) bool
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
