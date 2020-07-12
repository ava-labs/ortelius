// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/ava-labs/ortelius/services/cache"
)

type CachableFn func(context.Context) (interface{}, error)

type Cachable struct {
	Key        []string
	CachableFn CachableFn
}

type cacher interface {
	Get(context.Context, string) ([]byte, error)
	Set(context.Context, string, []byte) error
}

func cacheKey(networkID uint32, parts ...string) string {
	k := make([]string, 1, len(parts)+1)
	k[0] = strconv.Itoa(int(networkID))
	return cache.KeyFromParts(append(k, parts...)...)
}

func updateCachable(ctx context.Context, cache cacher, key string, cachableFn CachableFn) ([]byte, error) {
	obj, err := cachableFn(ctx)
	if err != nil {
		return nil, err
	}

	objBytes, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}

	if err := cache.Set(ctx, key, objBytes); err != nil {
		return nil, err
	}

	return objBytes, nil
}
