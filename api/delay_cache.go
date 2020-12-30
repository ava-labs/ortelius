package api

import (
	"context"
	"time"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/utils"
)

type CacheJob struct {
	key  string
	body *[]byte
	ttl  time.Duration
}

type DelayCache struct {
	cache  cacher
	worker utils.Worker
}

func NewDelayCache(cache cacher) *DelayCache {
	c := &DelayCache{cache: cache}
	c.worker = utils.NewWorker(workerQueueSize, workerThreadCount, c.Processor)
	return c
}

func (c *DelayCache) Processor(_ int, job interface{}) {
	if j, ok := job.(*CacheJob); ok {
		ctxset, cancelFnSet := context.WithTimeout(context.Background(), cfg.CacheTimeout)
		defer cancelFnSet()

		// if cache did not set, we can just ignore.
		_ = c.cache.Set(ctxset, j.key, *j.body, j.ttl)
	}
}
