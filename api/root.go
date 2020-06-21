// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/gocraft/health"
	"github.com/gocraft/web"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services/cache"
)

var (
	ErrCacheableFnFailed = errors.New("failed to load resource")
)

type index struct {
	Chains map[string]chainInfo `json:"chains"`
}

type chainInfo struct {
	Alias  string `json:"alias"`
	VMType string `json:"vmType"`
}

type RootRequestContext struct {
	ctx       context.Context
	job       *health.Job
	networkID uint32
	err       error

	cache cacher
}

func (c *RootRequestContext) Ctx() context.Context {
	return c.ctx
}

func (c *RootRequestContext) NetworkID() uint32 {
	return c.networkID
}

func (c *RootRequestContext) WriteCacheable(w http.ResponseWriter, cachable Cachable) {
	key := cacheKey(c.NetworkID(), cachable.Key...)

	// Get from cache or, if there is a cache miss, from the cachablefn
	resp, err := c.cache.Get(c.Ctx(), key)
	if err == cache.ErrMiss {
		c.job.KeyValue("cache", "miss")
		resp, err = updateCachable(c.ctx, c.cache, key, cachable.CachableFn)
	} else if err == nil {
		c.job.KeyValue("cache", "hit")
	}

	// Write error or response
	if err != nil {
		c.WriteErr(w, 500, ErrCacheableFnFailed)
		return
	}
	WriteJSON(w, resp)
}

func (c *RootRequestContext) WriteErr(w http.ResponseWriter, code int, err error) {
	c.err = err

	errBytes, err := json.Marshal(&ErrorResponse{
		Code:    code,
		Message: err.Error(),
	})
	if err != nil {
		w.WriteHeader(500)
		c.job.EventErr("marshal_error", err)
		return
	}

	w.WriteHeader(code)
	fmt.Fprint(w, string(errBytes))
}

func newRootRouter(params RouterParams, chainsConf cfg.Chains) (*web.Router, error) {
	indexResponder, err := newIndexResponder(chainsConf)
	if err != nil {
		return nil, err
	}

	router := web.New(RootRequestContext{}).
		Middleware(newContextSetter(params.NetworkID, params.Connections.Stream(), params.Connections.Cache())).
		Middleware((*RootRequestContext).setHeaders).
		NotFound((*RootRequestContext).notFoundHandler).
		Get("/", indexResponder)

	return router, nil
}

func newContextSetter(networkID uint32, stream *health.Stream, cache *cache.Cache) func(*RootRequestContext, web.ResponseWriter, *web.Request, web.NextMiddlewareFunc) {
	return func(c *RootRequestContext, w web.ResponseWriter, r *web.Request, next web.NextMiddlewareFunc) {
		// Set context properties, context last
		c.cache = cache
		c.networkID = networkID
		c.job = stream.NewJob(jobNameForPath(r.Request.URL.Path))

		ctx := context.Background()
		ctx, cancelFn := context.WithTimeout(ctx, RequestTimeout)
		c.ctx = ctx

		// Execute handler
		next(w, r)

		// Stop context
		cancelFn()

		// Complete job
		if c.err == nil {
			c.job.Complete(health.Success)
		} else {
			c.job.Complete(health.Error)
		}
	}
}

func newIndexResponder(chainsConf cfg.Chains) (func(*RootRequestContext, web.ResponseWriter, *web.Request), error) {
	i := &index{Chains: make(map[string]chainInfo, len(chainsConf))}
	for id, info := range chainsConf {
		i.Chains[id] = chainInfo{
			Alias:  info.Alias,
			VMType: info.VMType,
		}
	}

	indexBytes, err := json.Marshal(i)
	if err != nil {
		return nil, err
	}

	return func(c *RootRequestContext, resp web.ResponseWriter, _ *web.Request) {
		if _, err := resp.Write(indexBytes); err != nil {
			c.err = err
		}
	}, nil
}

func (*RootRequestContext) setHeaders(w web.ResponseWriter, r *web.Request, next web.NextMiddlewareFunc) {
	h := w.Header()
	h.Add("access-control-allow-headers", "Accept, Content-Type, Content-Length, Accept-Encoding")
	h.Add("access-control-allow-methods", "GET")
	h.Add("access-control-allow-origin", "*")

	h.Add("Content-Type", "application/json")

	next(w, r)
}

func (*RootRequestContext) notFoundHandler(w web.ResponseWriter, r *web.Request) {
	WriteErr(w, 404, "Not Found")
}

func jobNameForPath(path string) string {
	path = strings.ReplaceAll(path, "/", ".")
	if path == "" {
		path = "root"
	}

	return "request." + strings.TrimPrefix(path, ".")
}
