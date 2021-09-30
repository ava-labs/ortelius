// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package utils

import (
	"context"
	"crypto/sha256"
	"sync"
	"time"

	"github.com/neilotoole/errgroup"
)

type item struct {
	value  []byte
	expire time.Time
	last   time.Time
}

func (i *item) expired() bool {
	return time.Now().After(i.expire)
}

type bucket struct {
	m map[string]*item
	l sync.RWMutex
}

type LCache struct {
	buckets []*bucket
	stopCh  chan struct{}
}

func NewTTLMap() *LCache {
	buckets := make([]*bucket, 256)
	for i := range buckets {
		buckets[i] = &bucket{
			m: make(map[string]*item),
		}
	}
	m := &LCache{
		buckets: buckets,
		stopCh:  make(chan struct{}),
	}
	go func() {
		m.expire()
	}()
	return m
}

func (m *LCache) Stop() {
	close(m.stopCh)
}

func (m *LCache) expire() {
	ticker := time.NewTicker(time.Second)
	defer func() {
		ticker.Stop()
	}()
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			ctx := context.Background()
			errgrp, _ := errgroup.WithContextN(ctx, 3, 255)
			for _, bmx := range m.buckets {
				now := time.Now()
				bm := bmx
				errgrp.Go(func() error {
					bm.l.RLock()
					bml := len(bm.m)
					bm.l.RUnlock()
					if bml == 0 {
						return nil
					}
					bml++
					if bml > 1000 {
						bml = 1000
					}
					l := make([]string, 0, bml)
					bm.l.RLock()
					for k, v := range bm.m {
						if now.After(v.expire) {
							l = append(l, k)
						}
					}
					bm.l.RUnlock()
					if len(l) != 0 {
						for _, k := range l {
							bm.l.Lock()
							if it, ok := bm.m[k]; ok {
								if it.expired() {
									delete(bm.m, k)
								}
							}
							bm.l.Unlock()
						}
					}
					return nil
				})
			}
			_ = errgrp.Wait()
		}
	}
}

func (m *LCache) Put(k string, v []byte, ttl time.Duration) {
	chksum := sha256.Sum256([]byte(k))
	bm := m.buckets[chksum[0]]
	bm.l.Lock()
	defer bm.l.Unlock()
	it, ok := bm.m[k]
	if !ok {
		it = &item{value: v}
		bm.m[k] = it
	}
	it.last = time.Now()
	it.expire = it.last.Add(ttl)
}

func (m *LCache) Get(k string) ([]byte, bool) {
	chksum := sha256.Sum256([]byte(k))
	bm := m.buckets[chksum[0]]
	var found bool
	var v []byte
	bm.l.RLock()
	defer bm.l.RUnlock()
	if it, ok := bm.m[k]; ok {
		if !it.expired() {
			v = it.value
			found = true
		}
	}
	return v, found
}
