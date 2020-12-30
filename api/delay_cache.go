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

type cacheUpdate struct {
	cache  cacher
	worker utils.Worker
}

func NewCacheUpdate(cache cacher) *cacheUpdate {
	cacheUpdate := &cacheUpdate{cache: cache}
	cacheUpdate.worker = utils.NewWorker(workerQueueSize, workerThreadCount, cacheUpdate.Processor)
	return cacheUpdate
}

func (cacheUpdate *cacheUpdate) Processor(_ int, job interface{}) {
	if j, ok := job.(*CacheJob); ok {
		ctxset, cancelFnSet := context.WithTimeout(context.Background(), cfg.CacheTimeout)
		defer cancelFnSet()

		// if cache did not set, we can just ignore.
		_ = cacheUpdate.cache.Set(ctxset, j.key, *j.body, j.ttl)
	}
}
