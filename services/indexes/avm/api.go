// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm

import (
	"context"

	"github.com/ava-labs/gecko/ids"
	"github.com/gocraft/web"

	"github.com/ava-labs/ortelius/api"
	"github.com/ava-labs/ortelius/services/params"
)

type APIContext struct {
	*api.RootRequestContext

	index      *Index
	chainID    string
	chainAlias string

	rw web.ResponseWriter
}

func NewAPIRouter(params api.RouterParams) error {
	index, err := newForConnections(params.Connections, params.NetworkID, params.ChainConfig.ID)
	if err != nil {
		return err
	}

	params.Router.
		// Setup the context for each request
		Middleware(func(c *APIContext, w web.ResponseWriter, r *web.Request, next web.NextMiddlewareFunc) {
			c.index = index
			c.chainID = params.ChainConfig.ID
			c.chainAlias = params.ChainConfig.Alias

			c.rw = w

			next(w, r)
		}).

		// General routes
		Get("/", (*APIContext).Overview).
		Get("/search", (*APIContext).Search).
		Get("/aggregates", (*APIContext).Aggregate).
		Get("/transactions/aggregates", (*APIContext).Aggregate). // DEPRECATED

		// List and Get routes
		Get("/transactions", (*APIContext).ListTransactions).
		Get("/transactions/:id", (*APIContext).GetTransaction).
		Get("/assets", (*APIContext).ListAssets).
		Get("/assets/:id", (*APIContext).GetAsset).
		Get("/addresses", (*APIContext).ListAddresses).
		Get("/addresses/:id", (*APIContext).GetAddress).
		Get("/outputs", (*APIContext).ListOutputs).
		Get("/outputs/:id", (*APIContext).GetOutput)

	return nil
}

//
// General routes
//

func (c *APIContext) Overview(w web.ResponseWriter, _ *web.Request) {
	overview, err := c.index.GetChainInfo(c.chainAlias, c.NetworkID())
	if err != nil {
		c.WriteErr(w, 500, err)
		return
	}
	api.WriteObject(w, overview)
}

func (c *APIContext) Search(w web.ResponseWriter, r *web.Request) {
	p := &SearchParams{}
	if err := p.ForValues(r.URL.Query()); err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	c.WriteCacheable(w, api.Cachable{
		Key: c.cacheKeyForParams("search", p),
		CachableFn: func(ctx context.Context) (interface{}, error) {
			return c.index.Search(ctx, p)
		},
	})
}

func (c *APIContext) Aggregate(w web.ResponseWriter, r *web.Request) {
	p := &AggregateParams{}
	if err := p.ForValues(r.URL.Query()); err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	c.WriteCacheable(w, api.Cachable{
		Key: c.cacheKeyForParams("aggregate", p),
		CachableFn: func(ctx context.Context) (interface{}, error) {
			return c.index.Aggregate(ctx, p)
		},
	})
}

func (c *APIContext) ListTransactions(w web.ResponseWriter, r *web.Request) {
	p := &ListTransactionsParams{}
	if err := p.ForValues(r.URL.Query()); err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	c.WriteCacheable(w, api.Cachable{
		Key: c.cacheKeyForParams("list_transactions", p),
		CachableFn: func(ctx context.Context) (interface{}, error) {
			return c.index.ListTransactions(ctx, p)
		},
	})
}

func (c *APIContext) GetTransaction(w web.ResponseWriter, r *web.Request) {
	id, err := ids.FromString(r.PathParams["id"])
	if err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	c.WriteCacheable(w, api.Cachable{
		Key: c.cacheKeyForID("get_transaction", r.PathParams["id"]),
		CachableFn: func(ctx context.Context) (interface{}, error) {
			return c.index.GetTransaction(ctx, id)
		},
	})
}

func (c *APIContext) ListAssets(w web.ResponseWriter, r *web.Request) {
	p := &ListAssetsParams{}
	if err := p.ForValues(r.URL.Query()); err != nil {
		c.WriteErr(w, 400, err)
		return
	}
	c.WriteCacheable(w, api.Cachable{
		Key: c.cacheKeyForParams("list_assets", p),
		CachableFn: func(ctx context.Context) (interface{}, error) {
			return c.index.ListAssets(ctx, p)
		},
	})
}

func (c *APIContext) GetAsset(w web.ResponseWriter, r *web.Request) {
	id := r.PathParams["id"]
	c.WriteCacheable(w, api.Cachable{
		Key: c.cacheKeyForID("get_address", id),
		CachableFn: func(ctx context.Context) (interface{}, error) {
			return c.index.GetAsset(ctx, id)
		},
	})
}

func (c *APIContext) ListAddresses(w web.ResponseWriter, r *web.Request) {
	p := &ListAddressesParams{}
	if err := p.ForValues(r.URL.Query()); err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	c.WriteCacheable(w, api.Cachable{
		Key: c.cacheKeyForParams("list_addresses", p),
		CachableFn: func(ctx context.Context) (interface{}, error) {
			return c.index.ListAddresses(ctx, p)
		},
	})
}

func (c *APIContext) GetAddress(w web.ResponseWriter, r *web.Request) {
	id, err := ids.ShortFromString(r.PathParams["id"])
	if err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	c.WriteCacheable(w, api.Cachable{
		Key: c.cacheKeyForID("get_address", r.PathParams["id"]),
		CachableFn: func(ctx context.Context) (interface{}, error) {
			return c.index.GetAddress(ctx, id)
		},
	})
}

func (c *APIContext) ListOutputs(w web.ResponseWriter, r *web.Request) {
	p := &ListOutputsParams{}
	if err := p.ForValues(r.URL.Query()); err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	c.WriteCacheable(w, api.Cachable{
		Key: c.cacheKeyForParams("list_outputs", p),
		CachableFn: func(ctx context.Context) (interface{}, error) {
			return c.index.ListOutputs(ctx, p)
		},
	})
}

func (c *APIContext) GetOutput(w web.ResponseWriter, r *web.Request) {
	id, err := ids.FromString(r.PathParams["id"])
	if err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	c.WriteCacheable(w, api.Cachable{
		Key: c.cacheKeyForID("get_output", r.PathParams["id"]),
		CachableFn: func(ctx context.Context) (interface{}, error) {
			return c.index.GetOutput(ctx, id)
		},
	})
}

func (c *APIContext) cacheKeyForID(name string, id string) []string {
	return []string{"avm", c.chainID, name, params.CacheKey("id", id)}
}

func (c *APIContext) cacheKeyForParams(name string, p params.Param) []string {
	return append([]string{"avm", c.chainID, name}, p.CacheKey()...)
}
