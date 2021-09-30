// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package utils

import (
	"sync"
	"sync/atomic"
)

type CounterID struct {
	lock     sync.RWMutex
	counters map[string]*int64
}

func NewCounterID() *CounterID {
	return &CounterID{counters: make(map[string]*int64)}
}

func (c *CounterID) Inc(v string) {
	c.Add(v, 1)
}

func (c *CounterID) Add(v string, delta int64) {
	found := false
	c.lock.RLock()
	if counter, ok := c.counters[v]; ok {
		atomic.AddInt64(counter, delta)
		found = true
	}
	c.lock.RUnlock()

	if found {
		return
	}

	c.lock.Lock()
	if _, ok := c.counters[v]; !ok {
		c.counters[v] = new(int64)
	}
	atomic.AddInt64(c.counters[v], delta)
	c.lock.Unlock()
}

func (c *CounterID) Clone() map[string]int64 {
	countersValues := make(map[string]int64)
	c.lock.RLock()
	for cnter := range c.counters {
		cv := atomic.LoadInt64(c.counters[cnter])
		if cv != 0 {
			countersValues[cnter] = cv
		}
	}
	c.lock.RUnlock()
	return countersValues
}
