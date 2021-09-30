// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package utils

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"
)

const (
	CacheSeparator = "|"
)

var ErrNotFound = errors.New("not found")

// CacheableFn is a function whose output can safely be cached
type CacheableFn func(context.Context) (interface{}, error)

// Cacheable is a keyed CacheableFn
type Cacheable struct {
	Key         []string
	CacheableFn CacheableFn
	TTL         time.Duration
}

type Cacher interface {
	Get(context.Context, string) ([]byte, error)
	Set(context.Context, string, []byte, time.Duration) error
}

func CacheKey(networkID uint32, parts ...string) string {
	k := make([]string, 1, len(parts)+1)
	k[0] = strconv.Itoa(int(networkID))
	return KeyFromParts(append(k, parts...)...)
}

func KeyFromParts(parts ...string) string {
	return strings.Join(parts, CacheSeparator)
}

type Cache interface {
	Get(context.Context, string) ([]byte, error)
	Set(context.Context, string, []byte, time.Duration) error
}

type cacheContainer struct {
	cache *LCache
}

func NewCache() Cache {
	return &cacheContainer{cache: NewTTLMap()}
}

func (c *cacheContainer) Get(_ context.Context, key string) ([]byte, error) {
	v, ok := c.cache.Get(key)
	if !ok {
		return v, ErrNotFound
	}
	return v, nil
}

func (c *cacheContainer) Set(_ context.Context, key string, bytes []byte, ttl time.Duration) error {
	c.cache.Put(key, bytes, ttl)
	return nil
}
