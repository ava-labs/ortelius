// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/ortelius/services/indexes/avax"
	avmIndex "github.com/ava-labs/ortelius/services/indexes/avm"
	"github.com/ava-labs/ortelius/services/indexes/params"
	pvmIndex "github.com/ava-labs/ortelius/services/indexes/pvm"
	"net/http"
	"strings"

	"github.com/gocraft/health"
	"github.com/gocraft/web"

	"github.com/ava-labs/ortelius/services/cache"
)

var (
	// ErrCacheableFnFailed is returned when the execution of a CacheableFn fails
	ErrCacheableFnFailed = errors.New("failed to load resource")
)

// Context is the base context for APIs in the ortelius systems
type Context struct {
	ctx       context.Context
	job       *health.Job
	networkID uint32
	err       error
	cache     cacher

	avaxAssetID ids.ID
	avaxReader  *avax.Reader
	avmReader   *avmIndex.Reader
	pvmReader   *pvmIndex.Reader
}

// Ctx returns the context.Context for this request context
func (c *Context) Ctx() context.Context {
	return c.ctx
}

// NetworkID returns the networkID this request is for
func (c *Context) NetworkID() uint32 {
	return c.networkID
}

// WriteCacheable writes to the http response the output of the given Cacheable's
// function, either from the cache or from a new execution of the function
func (c *Context) WriteCacheable(w http.ResponseWriter, cacheable Cacheable) {
	key := cacheKey(c.NetworkID(), cacheable.Key...)

	// Get from cache or, if there is a cache miss, from the cacheablefn
	resp, err := c.cache.Get(c.Ctx(), key)
	if err == cache.ErrMiss {
		c.job.KeyValue("cache", "miss")
		resp, err = updateCacheable(c.ctx, c.cache, key, cacheable.CacheableFn, cacheable.TTL)
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

// WriteErr writes an error response to the http response
func (c *Context) WriteErr(w http.ResponseWriter, code int, err error) {
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

func (*Context) setHeaders(w web.ResponseWriter, r *web.Request, next web.NextMiddlewareFunc) {
	h := w.Header()
	h.Add("access-control-allow-headers", "Accept, Content-Type, Content-Length, Accept-Encoding")
	h.Add("access-control-allow-methods", "GET")
	h.Add("access-control-allow-origin", "*")

	h.Add("Content-Type", "application/json")

	next(w, r)
}

func (*Context) notFoundHandler(w web.ResponseWriter, r *web.Request) {
	WriteErr(w, 404, "Not Found")
}

func (c *Context) cacheKeyForID(name string, id string) []string {
	return []string{"avax", name, params.CacheKey("id", id)}
}

func (c *Context) cacheKeyForParams(name string, p params.Param) []string {
	return append([]string{"avax", name}, p.CacheKey()...)
}

func newContextSetter(networkID uint32, stream *health.Stream, cache cacher) func(*Context, web.ResponseWriter, *web.Request, web.NextMiddlewareFunc) {
	return func(c *Context, w web.ResponseWriter, r *web.Request, next web.NextMiddlewareFunc) {
		// Set context properties, context last
		c.cache = cache
		c.networkID = networkID
		c.job = stream.NewJob(jobNameForPath(r.Request.URL.Path))

		// Tag stream with request data
		remoteAddr := r.RemoteAddr
		if addrs, ok := r.Header["X-Forwarded-For"]; ok {
			remoteAddr = strings.Join(addrs, ",")
		}
		c.job.KeyValue("remote_addrs", remoteAddr)
		c.job.KeyValue("url", r.RequestURI)

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

func jobNameForPath(path string) string {
	path = strings.ReplaceAll(path, "/", ".")
	if path == "" {
		path = "root"
	}

	return "request." + strings.TrimPrefix(path, ".")
}
