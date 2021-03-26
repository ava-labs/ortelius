// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"context"
	"strconv"
	"time"

	"github.com/ava-labs/ortelius/services/cache"
)

// CacheableFn is a function whose output can safely be cached
type CacheableFn func(context.Context) (interface{}, error)

// Cacheable is a keyed CacheableFn
type Cacheable struct {
	Key         []string
	CacheableFn CacheableFn
	TTL         time.Duration
}

type cacher interface {
	Get(context.Context, string) ([]byte, error)
	Set(context.Context, string, []byte, time.Duration) error
}

func cacheKey(networkID uint32, parts ...string) string {
	k := make([]string, 1, len(parts)+1)
	k[0] = strconv.Itoa(int(networkID))
	return cache.KeyFromParts(append(k, parts...)...)
}

type nullCache struct{}

func NewNullCache() cache.Cache {
	return &nullCache{}
}

func (nullCache) Get(_ context.Context, _ string) ([]byte, error) {
	return nil, nil
}

func (nullCache) Set(_ context.Context, _ string, _ []byte, _ time.Duration) error {
	return nil
}
