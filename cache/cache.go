// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package cache

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

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

// CacheableFn is a function whose output can safely be cached
type CacheableFn func(context.Context) (interface{}, error)

// Cacheable is a keyed CacheableFn
type Cacheable struct {
	Key         []string
	CacheableFn CacheableFn
	TTL         time.Duration
}

func KeyFromParts(parts ...string) string {
	return strings.Join(parts, CacheSeparator)
}

func CacheKey(networkID uint32, parts ...string) string {
	k := make([]string, 1, len(parts)+1)
	k[0] = strconv.Itoa(int(networkID))
	return KeyFromParts(append(k, parts...)...)
}

type nullCache struct{}

func NewNullCache() Cache {
	return &nullCache{}
}

func (nullCache) Get(_ context.Context, _ string) ([]byte, error) {
	return nil, fmt.Errorf("invalid")
}

func (nullCache) Set(_ context.Context, _ string, _ []byte, _ time.Duration) error {
	return nil
}

type Cache interface {
	Get(context.Context, string) ([]byte, error)
	Set(context.Context, string, []byte, time.Duration) error
}

type cacheContainer struct {
	cache      *cache.Cache
	defaultTTL time.Duration
}

func New(redisConn *redis.Client) Cache {
	c := cache.New(&cache.Options{
		Redis: redisConn,
	})

	return &cacheContainer{cache: c, defaultTTL: DefaultTTL}
}

func (c *cacheContainer) Get(ctx context.Context, key string) ([]byte, error) {
	resp := []byte{}
	err := c.cache.Get(ctx, key, &resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *cacheContainer) Set(ctx context.Context, key string, bytes []byte, ttl time.Duration) error {
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
