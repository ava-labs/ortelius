// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ava-labs/avalanchego/genesis"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/gocraft/web"

	"github.com/ava-labs/ortelius/api"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/models"
	"github.com/ava-labs/ortelius/services/indexes/params"
)

const VMName = "avm"

func init() {
	api.RegisterRouter(VMName, NewAPIRouter, APIContext{})
}

type APIContext struct {
	*api.RootRequestContext

	reader     *Reader
	chainID    string
	chainAlias string

	rw web.ResponseWriter
}

func NewAPIRouter(params api.RouterParams) error {
	reader := NewReader(
		params.Connections.Stream(),
		services.NewDB(params.Connections.Stream(), params.Connections.DB()),
		params.ChainConfig.ID)

	_, avaxAssetID, err := genesis.Genesis(params.NetworkID)
	if err != nil {
		return err
	}

	overviewBytes, err := json.Marshal(&models.ChainInfo{
		VM:          VMName,
		NetworkID:   params.NetworkID,
		Alias:       params.ChainConfig.Alias,
		AVAXAssetID: models.StringID(avaxAssetID.String()),
		ID:          models.StringID(params.ChainConfig.ID),
	})

	params.Router.
		// Setup the context for each request
		Middleware(func(c *APIContext, w web.ResponseWriter, r *web.Request, next web.NextMiddlewareFunc) {
			c.reader = reader
			c.chainID = params.ChainConfig.ID
			c.chainAlias = params.ChainConfig.Alias

			c.rw = w

			next(w, r)
		}).

		// General routes
		Get("/", func(c *APIContext, w web.ResponseWriter, _ *web.Request) {
			api.WriteJSON(w, overviewBytes)
		}).
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

func (c *APIContext) Search(w web.ResponseWriter, r *web.Request) {
	p := &params.SearchParams{}
	if err := p.ForValues(r.URL.Query()); err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	c.WriteCacheable(w, api.Cachable{
		Key: c.cacheKeyForParams("search", p),
		CachableFn: func(ctx context.Context) (interface{}, error) {
			return c.reader.Search(ctx, p)
		},
	})
}

func (c *APIContext) Aggregate(w web.ResponseWriter, r *web.Request) {
	p := &params.AggregateParams{}
	if err := p.ForValues(r.URL.Query()); err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	c.WriteCacheable(w, api.Cachable{
		Key: c.cacheKeyForParams("aggregate", p),
		CachableFn: func(ctx context.Context) (interface{}, error) {
			return c.reader.Aggregate(ctx, p)
		},
	})
}

func (c *APIContext) ListTransactions(w web.ResponseWriter, r *web.Request) {
	p := &params.ListTransactionsParams{}
	if err := p.ForValues(r.URL.Query()); err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	c.WriteCacheable(w, api.Cachable{
		TTL: 5 * time.Second,
		Key: c.cacheKeyForParams("list_transactions", p),
		CachableFn: func(ctx context.Context) (interface{}, error) {
			return c.reader.ListTransactions(ctx, p)
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
		TTL: 5 * time.Second,
		Key: c.cacheKeyForID("get_transaction", r.PathParams["id"]),
		CachableFn: func(ctx context.Context) (interface{}, error) {
			return c.reader.GetTransaction(ctx, id)
		},
	})
}

func (c *APIContext) ListAssets(w web.ResponseWriter, r *web.Request) {
	p := &params.ListAssetsParams{}
	if err := p.ForValues(r.URL.Query()); err != nil {
		c.WriteErr(w, 400, err)
		return
	}
	c.WriteCacheable(w, api.Cachable{
		Key: c.cacheKeyForParams("list_assets", p),
		CachableFn: func(ctx context.Context) (interface{}, error) {
			return c.reader.ListAssets(ctx, p)
		},
	})
}

func (c *APIContext) GetAsset(w web.ResponseWriter, r *web.Request) {
	id := r.PathParams["id"]
	c.WriteCacheable(w, api.Cachable{
		Key: c.cacheKeyForID("get_address", id),
		CachableFn: func(ctx context.Context) (interface{}, error) {
			return c.reader.GetAsset(ctx, id)
		},
	})
}

func (c *APIContext) ListAddresses(w web.ResponseWriter, r *web.Request) {
	p := &params.ListAddressesParams{}
	if err := p.ForValues(r.URL.Query()); err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	c.WriteCacheable(w, api.Cachable{
		TTL: 5 * time.Second,
		Key: c.cacheKeyForParams("list_addresses", p),
		CachableFn: func(ctx context.Context) (interface{}, error) {
			return c.reader.ListAddresses(ctx, p)
		},
	})
}

func (c *APIContext) GetAddress(w web.ResponseWriter, r *web.Request) {
	id, err := params.AddressFromString(r.PathParams["id"])
	if err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	c.WriteCacheable(w, api.Cachable{
		TTL: 1 * time.Second,
		Key: c.cacheKeyForID("get_address", r.PathParams["id"]),
		CachableFn: func(ctx context.Context) (interface{}, error) {
			return c.reader.GetAddress(ctx, id)
		},
	})
}

func (c *APIContext) ListOutputs(w web.ResponseWriter, r *web.Request) {
	p := &params.ListOutputsParams{}
	if err := p.ForValues(r.URL.Query()); err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	c.WriteCacheable(w, api.Cachable{
		TTL: 5 * time.Second,
		Key: c.cacheKeyForParams("list_outputs", p),
		CachableFn: func(ctx context.Context) (interface{}, error) {
			return c.reader.ListOutputs(ctx, p)
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
			return c.reader.GetOutput(ctx, id)
		},
	})
}

func (c *APIContext) cacheKeyForID(name string, id string) []string {
	return []string{"avm", c.chainID, name, params.CacheKey("id", id)}
}

func (c *APIContext) cacheKeyForParams(name string, p params.Param) []string {
	return append([]string{"avm", c.chainID, name}, p.CacheKey()...)
}
