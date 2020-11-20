// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"context"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/ortelius/services/indexes/params"
	"github.com/gocraft/web"
)

type V2Context struct {
	*Context
	version uint8
	chainID *ids.ID
}

// AddV2Routes mounts a V2 API router at the given path, displaying the given
// indexBytes at the root. If chainID is not nil the handlers run in v1
// compatible mode where the `version` param is set to "1" and requests to
// default to filtering by the given chainID.
func AddV2Routes(router *web.Router, path string, indexBytes []byte, chainID *ids.ID) {
	router.Subrouter(V2Context{}, path).
		Get("/", func(c *V2Context, resp web.ResponseWriter, _ *web.Request) {
			if _, err := resp.Write(indexBytes); err != nil {
				c.err = err
			}
		}).

		// Handle legacy v1 logic
		Middleware(func(c *V2Context, w web.ResponseWriter, r *web.Request, next web.NextMiddlewareFunc) {
			c.version = 2
			if chainID != nil {
				c.chainID = chainID
				c.version = 1
			}
			next(w, r)
		}).
		Get("/search", (*V2Context).Search).
		Get("/aggregates", (*V2Context).Aggregate).
		Get("/txfeeAggregates", (*V2Context).TxfeeAggregate).
		Get("/transactions/aggregates", (*V2Context).Aggregate).
		Get("/addressChains", (*V2Context).AddressChains).

		// List and Get routes
		Get("/transactions", (*V2Context).ListTransactions).
		Get("/transactions/:id", (*V2Context).GetTransaction).
		Get("/addresses", (*V2Context).ListAddresses).
		Get("/addresses/:id", (*V2Context).GetAddress).
		Get("/outputs", (*V2Context).ListOutputs).
		Get("/outputs/:id", (*V2Context).GetOutput).
		Get("/assets", (*V2Context).ListAssets).
		Get("/assets/:id", (*V2Context).GetAsset)
}

//
// AVAX
//

func (c *V2Context) Search(w web.ResponseWriter, r *web.Request) {
	p := &params.SearchParams{}
	if err := p.ForValues(c.version, r.URL.Query()); err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	c.WriteCacheable(w, Cacheable{
		Key: c.cacheKeyForParams("search", p),
		CacheableFn: func(ctx context.Context) (interface{}, error) {
			return c.avaxReader.Search(ctx, p, c.avaxAssetID)
		},
	})
}

func (c *V2Context) TxfeeAggregate(w web.ResponseWriter, r *web.Request) {
	p := &params.TxfeeAggregateParams{}
	if err := p.ForValues(c.version, r.URL.Query()); err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	p.ChainIDs = params.ForValueChainID(c.chainID, p.ChainIDs)

	c.WriteCacheable(w, Cacheable{
		Key: c.cacheKeyForParams("aggregate_txfee", p),
		CacheableFn: func(ctx context.Context) (interface{}, error) {
			return c.avaxReader.TxfeeAggregate(ctx, p)
		},
	})
}

func (c *V2Context) Aggregate(w web.ResponseWriter, r *web.Request) {
	p := &params.AggregateParams{}
	if err := p.ForValues(c.version, r.URL.Query()); err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	p.ChainIDs = params.ForValueChainID(c.chainID, p.ChainIDs)

	c.WriteCacheable(w, Cacheable{
		Key: c.cacheKeyForParams("aggregate", p),
		CacheableFn: func(ctx context.Context) (interface{}, error) {
			return c.avaxReader.Aggregate(ctx, p)
		},
	})
}

func (c *V2Context) ListTransactions(w web.ResponseWriter, r *web.Request) {
	p := &params.ListTransactionsParams{}
	if err := p.ForValues(c.version, r.URL.Query()); err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	p.ChainIDs = params.ForValueChainID(c.chainID, p.ChainIDs)

	c.WriteCacheable(w, Cacheable{
		TTL: 5 * time.Second,
		Key: c.cacheKeyForParams("list_transactions", p),
		CacheableFn: func(ctx context.Context) (interface{}, error) {
			return c.avaxReader.ListTransactions(ctx, p, c.avaxAssetID)
		},
	})
}

func (c *V2Context) GetTransaction(w web.ResponseWriter, r *web.Request) {
	id, err := ids.FromString(r.PathParams["id"])
	if err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	c.WriteCacheable(w, Cacheable{
		TTL: 5 * time.Second,
		Key: c.cacheKeyForID("get_transaction", r.PathParams["id"]),
		CacheableFn: func(ctx context.Context) (interface{}, error) {
			return c.avaxReader.GetTransaction(ctx, id, c.avaxAssetID)
		},
	})
}

func (c *V2Context) ListAddresses(w web.ResponseWriter, r *web.Request) {
	p := &params.ListAddressesParams{}
	if err := p.ForValues(c.version, r.URL.Query()); err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	p.ChainIDs = params.ForValueChainID(c.chainID, p.ChainIDs)

	c.WriteCacheable(w, Cacheable{
		TTL: 5 * time.Second,
		Key: c.cacheKeyForParams("list_addresses", p),
		CacheableFn: func(ctx context.Context) (interface{}, error) {
			return c.avaxReader.ListAddresses(ctx, p)
		},
	})
}

func (c *V2Context) GetAddress(w web.ResponseWriter, r *web.Request) {
	p := &params.ListAddressesParams{}
	if err := p.ForValues(c.version, r.URL.Query()); err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	id, err := params.AddressFromString(r.PathParams["id"])
	if err != nil {
		c.WriteErr(w, 400, err)
		return
	}
	p.Address = &id
	p.ListParams.DisableCounting = true
	p.ChainIDs = params.ForValueChainID(c.chainID, p.ChainIDs)

	c.WriteCacheable(w, Cacheable{
		TTL: 1 * time.Second,
		Key: c.cacheKeyForParams("get_address", p),
		CacheableFn: func(ctx context.Context) (interface{}, error) {
			return c.avaxReader.GetAddress(ctx, p)
		},
	})
}

func (c *V2Context) AddressChains(w web.ResponseWriter, r *web.Request) {
	p := &params.AddressChainsParams{}
	if err := p.ForValues(c.version, r.URL.Query()); err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	c.WriteCacheable(w, Cacheable{
		TTL: 5 * time.Second,
		Key: c.cacheKeyForParams("address_chains", p),
		CacheableFn: func(ctx context.Context) (interface{}, error) {
			return c.avaxReader.AddressChains(ctx, p)
		},
	})
}

func (c *V2Context) ListOutputs(w web.ResponseWriter, r *web.Request) {
	p := &params.ListOutputsParams{}
	if err := p.ForValues(c.version, r.URL.Query()); err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	p.ChainIDs = params.ForValueChainID(c.chainID, p.ChainIDs)

	c.WriteCacheable(w, Cacheable{
		TTL: 5 * time.Second,
		Key: c.cacheKeyForParams("list_outputs", p),
		CacheableFn: func(ctx context.Context) (interface{}, error) {
			return c.avaxReader.ListOutputs(ctx, p)
		},
	})
}

func (c *V2Context) GetOutput(w web.ResponseWriter, r *web.Request) {
	id, err := ids.FromString(r.PathParams["id"])
	if err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	c.WriteCacheable(w, Cacheable{
		Key: c.cacheKeyForID("get_output", r.PathParams["id"]),
		CacheableFn: func(ctx context.Context) (interface{}, error) {
			return c.avaxReader.GetOutput(ctx, id)
		},
	})
}

//
// AVM
//

func (c *V2Context) ListAssets(w web.ResponseWriter, r *web.Request) {
	p := &params.ListAssetsParams{}
	if err := p.ForValues(c.version, r.URL.Query()); err != nil {
		c.WriteErr(w, 400, err)
		return
	}
	c.WriteCacheable(w, Cacheable{
		Key: c.cacheKeyForParams("list_assets", p),
		CacheableFn: func(ctx context.Context) (interface{}, error) {
			return c.avmReader.ListAssets(ctx, p)
		},
	})
}

func (c *V2Context) GetAsset(w web.ResponseWriter, r *web.Request) {
	p := &params.ListAssetsParams{}
	if err := p.ForValues(c.version, r.URL.Query()); err != nil {
		c.WriteErr(w, 400, err)
		return
	}
	id := r.PathParams["id"]
	p.PathParamID = id

	c.WriteCacheable(w, Cacheable{
		Key: c.cacheKeyForParams("get_asset", p),
		CacheableFn: func(ctx context.Context) (interface{}, error) {
			return c.avmReader.GetAsset(ctx, p, id)
		},
	})
}

//
// PVM
//
func (c *V2Context) ListBlocks(w web.ResponseWriter, r *web.Request) {
	p := &params.ListBlocksParams{}
	if err := p.ForValues(c.version, r.URL.Query()); err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	c.WriteCacheable(w, Cacheable{
		TTL: 5 * time.Second,
		Key: c.cacheKeyForParams("list_blocks", p),
		CacheableFn: func(ctx context.Context) (interface{}, error) {
			return c.pvmReader.ListBlocks(ctx, p)
		},
	})
}

func (c *V2Context) GetBlock(w web.ResponseWriter, r *web.Request) {
	id, err := ids.FromString(r.PathParams["id"])
	if err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	c.WriteCacheable(w, Cacheable{
		Key: c.cacheKeyForID("get_block", r.PathParams["id"]),
		CacheableFn: func(ctx context.Context) (interface{}, error) {
			return c.pvmReader.GetBlock(ctx, id)
		},
	})
}
