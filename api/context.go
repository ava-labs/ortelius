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

	"github.com/ava-labs/ortelius/services"

	"github.com/ava-labs/ortelius/cfg"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/ortelius/services/cache"
	"github.com/ava-labs/ortelius/services/indexes/avax"
	"github.com/ava-labs/ortelius/services/indexes/avm"
	"github.com/ava-labs/ortelius/services/indexes/params"
	"github.com/ava-labs/ortelius/services/indexes/pvm"
	"github.com/gocraft/health"
	"github.com/gocraft/web"
)

var (
	// ErrCacheableFnFailed is returned when the execution of a CacheableFn
	// fails.
	ErrCacheableFnFailed = errors.New("failed to load resource")

	// errInternalServerError is returned when errors occur that are not due to
	// a mistake from the caller.
	errInternalServerError = errors.New("internal server error")

	workerQueueSize   = 10
	workerThreadCount = 1
)

// Context is the base context for APIs in the ortelius systems
type Context struct {
	job *health.Job
	err error

	networkID   uint32
	avaxAssetID ids.ID

	cache       cacher
	delayCache  *DelayCache
	avaxReader  *avax.Reader
	avmReader   *avm.Reader
	pvmReader   *pvm.Reader
	connections *services.Connections
}

// NetworkID returns the networkID this request is for
func (c *Context) NetworkID() uint32 {
	return c.networkID
}

// WriteCacheable writes to the http response the output of the given Cacheable's
// function, either from the cache or from a new execution of the function
func (c *Context) WriteCacheable(w http.ResponseWriter, cacheable Cacheable) {
	key := cacheKey(c.NetworkID(), cacheable.Key...)

	ctxget, cancelFnGet := context.WithTimeout(context.Background(), cfg.CacheTimeout)
	defer cancelFnGet()

	var err error
	var resp []byte

	// Get from cache or, if there is a cache miss, from the cacheablefn
	resp, err = c.cache.Get(ctxget, key)
	if err == cache.ErrMiss {
		c.job.KeyValue("cache", "miss")

		ctxreq, cancelFnReq := context.WithTimeout(context.Background(), cfg.RequestTimeout)
		defer cancelFnReq()

		var obj interface{}
		obj, err = cacheable.CacheableFn(ctxreq)

		if err == nil {
			resp, err = json.Marshal(obj)
			if err == nil {
				// if we have room in the queue, enque the cache job..
				if c.delayCache.worker.JobCnt() < int64(workerQueueSize) {
					c.delayCache.worker.Enque(&CacheJob{key: key, body: &resp, ttl: cacheable.TTL})
				}
			}
		}
	} else if err == nil {
		c.job.KeyValue("cache", "hit")
	}

	// Write error or response
	if err != nil {
		c.connections.Logger().Warn("server error %v", err)
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

// WriteErr writes an error response to the http response
func (c *Context) Write500Err(w http.ResponseWriter, actualErr error) {
	c.err = actualErr

	errBytes, err := json.Marshal(&ErrorResponse{
		Code:    500,
		Message: errInternalServerError.Error(),
	})

	w.WriteHeader(500)

	if err != nil {
		c.job.EventErr("marshal_error", err)
		return
	}
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

func newContextSetter(networkID uint32, stream *health.Stream, cache cacher, connections *services.Connections, delayCache *DelayCache) func(*Context, web.ResponseWriter, *web.Request, web.NextMiddlewareFunc) {
	return func(c *Context, w web.ResponseWriter, r *web.Request, next web.NextMiddlewareFunc) {
		// Set context properties, context last
		c.cache = cache
		c.connections = connections
		c.delayCache = delayCache
		c.networkID = networkID
		c.job = stream.NewJob(jobNameForPath(r.Request.URL.Path))

		// Tag stream with request data
		remoteAddr := r.RemoteAddr
		if addrs, ok := r.Header["X-Forwarded-For"]; ok {
			remoteAddr = strings.Join(addrs, ",")
		}
		c.job.KeyValue("remote_addrs", remoteAddr)
		c.job.KeyValue("url", r.RequestURI)

		// Execute handler
		next(w, r)

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
