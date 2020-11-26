package utils

import (
	"sync"
	"sync/atomic"
)

type CounterID struct {
	lock     sync.RWMutex
	counters map[string]*uint64
}

func NewCounterId() *CounterID {
	return &CounterID{counters: make(map[string]*uint64)}
}

func (c *CounterID) Inc(v string) {
	found := false
	c.lock.RLock()
	if counter, ok := c.counters[v]; ok {
		atomic.AddUint64(counter, 1)
		found = true
	}
	c.lock.RUnlock()

	if found {
		return
	}

	c.lock.Lock()
	if _, ok := c.counters[v]; !ok {
		c.counters[v] = new(uint64)
	}
	atomic.AddUint64(c.counters[v], 1)
	c.lock.Unlock()
}

func (c *CounterID) Clone() map[string]uint64 {
	countersValues := make(map[string]uint64)
	c.lock.RLock()
	for cnter := range c.counters {
		cv := atomic.LoadUint64(c.counters[cnter])
		if cv != 0 {
			countersValues[cnter] = cv
		}
	}
	c.lock.RUnlock()
	return countersValues
}
