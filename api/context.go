// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ava-labs/ortelius/services/servicesconn"
	"github.com/ava-labs/ortelius/services/servicesctrl"
	"net/http"
	"strings"
	"time"

	"github.com/ava-labs/ortelius/cfg"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/ortelius/services/indexes/avax"
	"github.com/ava-labs/ortelius/services/indexes/params"
	"github.com/gocraft/health"
	"github.com/gocraft/web"
)

var (
	// ErrCacheableFnFailed is returned when the execution of a CacheableFn
	// fails.
	ErrCacheableFnFailed = errors.New("failed to load resource")

	workerQueueSize   = 10
	workerThreadCount = 1
)

// Context is the base context for APIs in the ortelius systems
type Context struct {
	sc *servicesctrl.Control

	job *health.Job
	err error

	networkID   uint32
	avaxAssetID ids.ID

	delayCache  *DelayCache
	avaxReader  *avax.Reader
	connections *servicesconn.Connections
}

// NetworkID returns the networkID this request is for
func (c *Context) NetworkID() uint32 {
	return c.networkID
}

func (c *Context) cacheGet(key string) ([]byte, error) {
	ctxget, cancelFnGet := context.WithTimeout(context.Background(), cfg.CacheTimeout)
	defer cancelFnGet()
	// Get from cache or, if there is a cache miss, from the cacheablefn
	return c.delayCache.Cache.Get(ctxget, key)
}

func (c *Context) cacheRun(reqTime time.Duration, cacheable Cacheable) (interface{}, error) {
	ctxreq, cancelFnReq := context.WithTimeout(context.Background(), reqTime)
	defer cancelFnReq()

	return cacheable.CacheableFn(ctxreq)
}

// WriteCacheable writes to the http response the output of the given Cacheable's
// function, either from the cache or from a new execution of the function
func (c *Context) WriteCacheable(w http.ResponseWriter, cacheable Cacheable) {
	key := cacheKey(c.NetworkID(), cacheable.Key...)

	// Get from cache or, if there is a cache miss, from the cacheablefn
	resp, err := c.cacheGet(key)
	switch err {
	case nil:
		c.job.KeyValue("cache", "hit")
	default:
		c.job.KeyValue("cache", "miss")

		var obj interface{}
		obj, err = c.cacheRun(cfg.RequestTimeout, cacheable)
		if err == nil {
			resp, err = json.Marshal(obj)
			if err == nil {
				// if we have room in the queue, enque the cache job..
				if c.delayCache.worker.JobCnt() < int64(workerQueueSize) {
					c.delayCache.worker.Enque(&CacheJob{key: key, body: &resp, ttl: cacheable.TTL})
				}
			}
		}
	}

	// Write error or response
	if err != nil {
		c.sc.Log.Warn("server error %v", err)
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

func newContextSetter(sc *servicesctrl.Control, networkID uint32, stream *health.Stream, connections *servicesconn.Connections, delayCache *DelayCache) func(*Context, web.ResponseWriter, *web.Request, web.NextMiddlewareFunc) {
	return func(c *Context, w web.ResponseWriter, r *web.Request, next web.NextMiddlewareFunc) {
		c.sc = sc
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
