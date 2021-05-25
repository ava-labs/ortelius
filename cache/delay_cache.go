package cache

import (
	"context"
	"time"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/utils/worker"
)

var (
	workerQueueSize   = 10
	workerThreadCount = 1
)

type Job struct {
	Key  string
	Body *[]byte
	TTL  time.Duration
}

type DelayCache struct {
	Cache  Cache
	worker worker.Worker
}

func NewDelayCache(cache Cache) *DelayCache {
	c := &DelayCache{Cache: cache}
	c.worker = worker.NewWorker(workerQueueSize, workerThreadCount, c.Processor)
	return c
}

func (c *DelayCache) Enque(j *Job) {
	if c.worker.JobCnt() < int64(workerQueueSize) {
		return
	}
	c.worker.Enque(j)
}

func (c *DelayCache) Processor(_ int, job interface{}) {
	if j, ok := job.(*Job); ok {
		ctxset, cancelFnSet := context.WithTimeout(context.Background(), cfg.CacheTimeout)
		defer cancelFnSet()

		// if cache did not set, we can just ignore.
		_ = c.Cache.Set(ctxset, j.Key, *j.Body, j.TTL)
	}
}
