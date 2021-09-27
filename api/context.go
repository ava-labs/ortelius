// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services/indexes/avax"
	"github.com/ava-labs/ortelius/services/indexes/params"
	"github.com/ava-labs/ortelius/servicesctrl"
	"github.com/ava-labs/ortelius/utils"
	"github.com/gocraft/web"
)

var (
	// ErrCacheableFnFailed is returned when the execution of a CacheableFn
	// fails.
	ErrCacheableFnFailed = errors.New("failed to load resource")
)

// Context is the base context for APIs in the ortelius systems
type Context struct {
	sc *servicesctrl.Control

	networkID   uint32
	avaxAssetID ids.ID

	delayCache  *utils.DelayCache
	avaxReader  *avax.Reader
	connections *utils.Connections
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

func (c *Context) cacheRun(reqTime time.Duration, cacheable utils.Cacheable) (interface{}, error) {
	ctxreq, cancelFnReq := context.WithTimeout(context.Background(), reqTime)
	defer cancelFnReq()

	return cacheable.CacheableFn(ctxreq)
}

// WriteCacheable writes to the http response the output of the given Cacheable's
// function, either from the cache or from a new execution of the function
func (c *Context) WriteCacheable(w http.ResponseWriter, cacheable utils.Cacheable) {
	key := utils.CacheKey(c.NetworkID(), cacheable.Key...)

	// Get from cache or, if there is a cache miss, from the cacheablefn
	resp, err := c.cacheGet(key)
	switch err {
	case nil:
	default:
		var obj interface{}
		obj, err = c.cacheRun(cfg.RequestTimeout, cacheable)
		if err == nil {
			resp, err = json.Marshal(obj)
			if err == nil {
				c.delayCache.Worker.TryEnque(&utils.CacheJob{Key: key, Body: &resp, TTL: cacheable.TTL})
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
	errBytes, err := json.Marshal(&ErrorResponse{
		Code:    code,
		Message: err.Error(),
	})
	if err != nil {
		w.WriteHeader(500)
		c.sc.Log.Warn("marshal %v", err)
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

func newContextSetter(sc *servicesctrl.Control, networkID uint32, connections *utils.Connections, delayCache *utils.DelayCache) func(*Context, web.ResponseWriter, *web.Request, web.NextMiddlewareFunc) {
	return func(c *Context, w web.ResponseWriter, r *web.Request, next web.NextMiddlewareFunc) {
		c.sc = sc
		c.connections = connections
		c.delayCache = delayCache
		c.networkID = networkID

		// Execute handler
		next(w, r)
	}
}
