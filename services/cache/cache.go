// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package cache

import (
	"context"
	"strings"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/go-redis/cache/v8"
	"github.com/go-redis/redis/v8"
)

const (
	CacheSeparator = "|"
)

var (
	ErrMiss = cache.ErrCacheMiss

	DefaultTTL = 5 * time.Minute
)

func KeyFromParts(parts ...string) string {
	return strings.Join(parts, CacheSeparator)
}

type Cache struct {
	cache      *cache.Cache
	defaultTTL time.Duration
}

func New(redisConn *redis.Client) *Cache {
	c := cache.New(&cache.Options{
		Redis:      redisConn,
		LocalCache: fastcache.New(100 << 20), // 100 MB
	})

	return &Cache{cache: c, defaultTTL: DefaultTTL}
}

func (c *Cache) Get(ctx context.Context, key string) ([]byte, error) {
	resp := []byte{}
	err := c.cache.Get(ctx, key, &resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Cache) Set(ctx context.Context, key string, bytes []byte, ttl time.Duration) error {
	if ttl < 1 {
		ttl = c.defaultTTL
	}

	return c.cache.Set(&cache.Item{
		Ctx:   ctx,
		Key:   key,
		Value: bytes,
		TTL:   ttl,
	})
}
