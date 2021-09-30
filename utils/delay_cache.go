// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package utils

import (
	"context"
	"time"

	"github.com/ava-labs/ortelius/cfg"
)

const (
	WorkerQueueSize   = 10
	WorkerThreadCount = 1
)

type CacheJob struct {
	Key  string
	Body *[]byte
	TTL  time.Duration
}

type DelayCache struct {
	Cache  Cacher
	Worker Worker
}

func NewDelayCache(cache Cacher) *DelayCache {
	c := &DelayCache{Cache: cache}
	c.Worker = NewWorker(WorkerQueueSize, WorkerThreadCount, c.Processor)
	return c
}

func (c *DelayCache) Processor(_ int, job interface{}) {
	if j, ok := job.(*CacheJob); ok {
		ctxset, cancelFnSet := context.WithTimeout(context.Background(), cfg.CacheTimeout)
		defer cancelFnSet()

		// if cache did not set, we can just ignore.
		_ = c.Cache.Set(ctxset, j.Key, *j.Body, j.TTL)
	}
}
