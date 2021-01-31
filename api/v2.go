// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"context"
	"fmt"
	"time"

	"github.com/ava-labs/ortelius/services/metrics"

	"github.com/ava-labs/ortelius/cfg"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/ortelius/services/indexes/params"
	"github.com/gocraft/web"
)

const DefaultOffsetLimit = 10000

type V2Context struct {
	*Context
	version uint8
	chainID *ids.ID
}

const MetricCount = "api_count"
const MetricMillis = "api_millis"

const MetricTransactionsCount = "api_transactions_count"
const MetricTransactionsMillis = "api_transactions_millis"
const MetricAddressesCount = "api_addresses_count"
const MetricAddressesMillis = "api_addresses_millis"
const MetricAddressChainsCount = "api_address_chains_count"
const MetricAddressChainsMillis = "api_address_chains_millis"
const MetricAggregateCount = "api_aggregate_count"
const MetricAggregateMillis = "api_aggregate_millis"
const MetricAssetCount = "api_asset_count"
const MetricAssetMillis = "api_asset_millis"
const MetricSearchCount = "api_search_count"
const MetricSearchMillis = "api_search_millis"

// AddV2Routes mounts a V2 API router at the given path, displaying the given
// indexBytes at the root. If chainID is not nil the handlers run in v1
// compatible mode where the `version` param is set to "1" and requests to
// default to filtering by the given chainID.
func AddV2Routes(ctx *Context, router *web.Router, path string, indexBytes []byte, chainID *ids.ID) {
	metrics.Prometheus.CounterInit(MetricCount, MetricCount)
	metrics.Prometheus.CounterInit(MetricMillis, MetricMillis)

	metrics.Prometheus.CounterInit(MetricTransactionsCount, MetricTransactionsCount)
	metrics.Prometheus.CounterInit(MetricTransactionsMillis, MetricTransactionsMillis)

	metrics.Prometheus.CounterInit(MetricAddressesCount, MetricAddressesCount)
	metrics.Prometheus.CounterInit(MetricAddressesMillis, MetricAddressesMillis)

	metrics.Prometheus.CounterInit(MetricAddressChainsCount, MetricAddressChainsCount)
	metrics.Prometheus.CounterInit(MetricAddressChainsMillis, MetricAddressChainsMillis)

	metrics.Prometheus.CounterInit(MetricAggregateCount, MetricAggregateCount)
	metrics.Prometheus.CounterInit(MetricAggregateMillis, MetricAggregateMillis)

	metrics.Prometheus.CounterInit(MetricAssetCount, MetricAssetCount)
	metrics.Prometheus.CounterInit(MetricAssetMillis, MetricAssetMillis)

	metrics.Prometheus.CounterInit(MetricSearchCount, MetricSearchCount)
	metrics.Prometheus.CounterInit(MetricSearchMillis, MetricSearchMillis)

	v2ctx := V2Context{Context: ctx}
	router.Subrouter(v2ctx, path).
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
		Post("/addressChains", (*V2Context).AddressChainsPost).

		// List and Get routes
		Get("/transactions", (*V2Context).ListTransactions).
		Post("/transactions", (*V2Context).ListTransactionsPost).
		Get("/transactions/:id", (*V2Context).GetTransaction).
		Get("/addresses", (*V2Context).ListAddresses).
		Get("/addresses/:id", (*V2Context).GetAddress).
		Get("/outputs", (*V2Context).ListOutputs).
		Get("/outputs/:id", (*V2Context).GetOutput).
		Get("/assets", (*V2Context).ListAssets).
		Get("/assets/:id", (*V2Context).GetAsset).
		Get("/txjson/:id", (*V2Context).TxJSON)
}

//
// AVAX
//

func (c *V2Context) Search(w web.ResponseWriter, r *web.Request) {
	collectors := metrics.NewCollectors(
		metrics.NewCounterObserveMillisCollect(MetricMillis),
		metrics.NewCounterIncCollect(MetricCount),
		metrics.NewCounterObserveMillisCollect(MetricSearchMillis),
		metrics.NewCounterIncCollect(MetricSearchCount),
	)
	defer func() {
		_ = collectors.Collect()
	}()

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
	collectors := metrics.NewCollectors(
		metrics.NewCounterObserveMillisCollect(MetricMillis),
		metrics.NewCounterIncCollect(MetricCount),
	)
	defer func() {
		_ = collectors.Collect()
	}()

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
	collectors := metrics.NewCollectors(
		metrics.NewCounterObserveMillisCollect(MetricMillis),
		metrics.NewCounterIncCollect(MetricCount),
		metrics.NewCounterObserveMillisCollect(MetricAggregateMillis),
		metrics.NewCounterIncCollect(MetricAggregateCount),
	)
	defer func() {
		_ = collectors.Collect()
	}()

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
	collectors := metrics.NewCollectors(
		metrics.NewCounterObserveMillisCollect(MetricMillis),
		metrics.NewCounterIncCollect(MetricCount),
		metrics.NewCounterObserveMillisCollect(MetricTransactionsMillis),
		metrics.NewCounterIncCollect(MetricTransactionsCount),
	)
	defer func() {
		_ = collectors.Collect()
	}()

	p := &params.ListTransactionsParams{}
	if err := p.ForValues(c.version, r.URL.Query()); err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	p.ChainIDs = params.ForValueChainID(c.chainID, p.ChainIDs)

	if p.ListParams.Offset > DefaultOffsetLimit {
		c.WriteErr(w, 400, fmt.Errorf("invalid offset"))
		return
	}

	c.WriteCacheable(w, Cacheable{
		TTL: 5 * time.Second,
		Key: c.cacheKeyForParams("list_transactions", p),
		CacheableFn: func(ctx context.Context) (interface{}, error) {
			return c.avaxReader.ListTransactions(ctx, p, c.avaxAssetID)
		},
	})
}

func (c *V2Context) ListTransactionsPost(w web.ResponseWriter, r *web.Request) {
	collectors := metrics.NewCollectors(
		metrics.NewCounterObserveMillisCollect(MetricMillis),
		metrics.NewCounterIncCollect(MetricCount),
		metrics.NewCounterObserveMillisCollect(MetricTransactionsMillis),
		metrics.NewCounterIncCollect(MetricTransactionsCount),
	)
	defer func() {
		_ = collectors.Collect()
	}()

	p := &params.ListTransactionsParams{}
	q, err := ParseGetJSON(r, cfg.RequestGetMaxSize)
	if err != nil {
		c.WriteErr(w, 400, err)
		return
	}
	if err := p.ForValues(c.version, q); err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	p.ChainIDs = params.ForValueChainID(c.chainID, p.ChainIDs)

	if p.ListParams.Offset > DefaultOffsetLimit {
		c.WriteErr(w, 400, fmt.Errorf("invalid offset"))
		return
	}

	c.WriteCacheable(w, Cacheable{
		TTL: 5 * time.Second,
		Key: c.cacheKeyForParams("list_transactions", p),
		CacheableFn: func(ctx context.Context) (interface{}, error) {
			return c.avaxReader.ListTransactions(ctx, p, c.avaxAssetID)
		},
	})
}

func (c *V2Context) GetTransaction(w web.ResponseWriter, r *web.Request) {
	collectors := metrics.NewCollectors(
		metrics.NewCounterObserveMillisCollect(MetricMillis),
		metrics.NewCounterIncCollect(MetricCount),
		metrics.NewCounterObserveMillisCollect(MetricTransactionsMillis),
		metrics.NewCounterIncCollect(MetricTransactionsCount),
	)
	defer func() {
		_ = collectors.Collect()
	}()

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
	collectors := metrics.NewCollectors(
		metrics.NewCounterObserveMillisCollect(MetricMillis),
		metrics.NewCounterIncCollect(MetricCount),
		metrics.NewCounterObserveMillisCollect(MetricAddressesMillis),
		metrics.NewCounterIncCollect(MetricAddressesCount),
	)
	defer func() {
		_ = collectors.Collect()
	}()

	p := &params.ListAddressesParams{}
	if err := p.ForValues(c.version, r.URL.Query()); err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	p.ChainIDs = params.ForValueChainID(c.chainID, p.ChainIDs)
	p.ListParams.DisableCounting = true

	c.WriteCacheable(w, Cacheable{
		TTL: 5 * time.Second,
		Key: c.cacheKeyForParams("list_addresses", p),
		CacheableFn: func(ctx context.Context) (interface{}, error) {
			return c.avaxReader.ListAddresses(ctx, p)
		},
	})
}

func (c *V2Context) GetAddress(w web.ResponseWriter, r *web.Request) {
	collectors := metrics.NewCollectors(
		metrics.NewCounterObserveMillisCollect(MetricMillis),
		metrics.NewCounterIncCollect(MetricCount),
		metrics.NewCounterObserveMillisCollect(MetricAddressesMillis),
		metrics.NewCounterIncCollect(MetricAddressesCount),
	)
	defer func() {
		_ = collectors.Collect()
	}()

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
	collectors := metrics.NewCollectors(
		metrics.NewCounterObserveMillisCollect(MetricMillis),
		metrics.NewCounterIncCollect(MetricCount),
		metrics.NewCounterObserveMillisCollect(MetricAddressChainsMillis),
		metrics.NewCounterIncCollect(MetricAddressChainsCount),
	)
	defer func() {
		_ = collectors.Collect()
	}()

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

func (c *V2Context) AddressChainsPost(w web.ResponseWriter, r *web.Request) {
	collectors := metrics.NewCollectors(
		metrics.NewCounterObserveMillisCollect(MetricMillis),
		metrics.NewCounterIncCollect(MetricCount),
		metrics.NewCounterObserveMillisCollect(MetricAddressChainsMillis),
		metrics.NewCounterIncCollect(MetricAddressChainsCount),
	)
	defer func() {
		_ = collectors.Collect()
	}()

	p := &params.AddressChainsParams{}
	q, err := ParseGetJSON(r, cfg.RequestGetMaxSize)
	if err != nil {
		c.WriteErr(w, 400, err)
		return
	}
	if err := p.ForValues(c.version, q); err != nil {
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
	collectors := metrics.NewCollectors(
		metrics.NewCounterObserveMillisCollect(MetricMillis),
		metrics.NewCounterIncCollect(MetricCount),
	)
	defer func() {
		_ = collectors.Collect()
	}()

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
	collectors := metrics.NewCollectors(
		metrics.NewCounterObserveMillisCollect(MetricMillis),
		metrics.NewCounterIncCollect(MetricCount),
	)
	defer func() {
		_ = collectors.Collect()
	}()

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
	collectors := metrics.NewCollectors(
		metrics.NewCounterObserveMillisCollect(MetricMillis),
		metrics.NewCounterIncCollect(MetricCount),
		metrics.NewCounterObserveMillisCollect(MetricAssetMillis),
		metrics.NewCounterIncCollect(MetricAssetCount),
	)
	defer func() {
		_ = collectors.Collect()
	}()

	p := &params.ListAssetsParams{}
	if err := p.ForValues(c.version, r.URL.Query()); err != nil {
		c.WriteErr(w, 400, err)
		return
	}
	c.WriteCacheable(w, Cacheable{
		Key: c.cacheKeyForParams("list_assets", p),
		CacheableFn: func(ctx context.Context) (interface{}, error) {
			return c.avaxReader.ListAssets(ctx, p)
		},
	})
}

func (c *V2Context) GetAsset(w web.ResponseWriter, r *web.Request) {
	collectors := metrics.NewCollectors(
		metrics.NewCounterObserveMillisCollect(MetricMillis),
		metrics.NewCounterIncCollect(MetricCount),
		metrics.NewCounterObserveMillisCollect(MetricAssetMillis),
		metrics.NewCounterIncCollect(MetricAssetCount),
	)
	defer func() {
		_ = collectors.Collect()
	}()

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
			return c.avaxReader.GetAsset(ctx, p, id)
		},
	})
}

//
// PVM
//
func (c *V2Context) ListBlocks(w web.ResponseWriter, r *web.Request) {
	collectors := metrics.NewCollectors(
		metrics.NewCounterObserveMillisCollect(MetricMillis),
		metrics.NewCounterIncCollect(MetricCount),
	)
	defer func() {
		_ = collectors.Collect()
	}()

	p := &params.ListBlocksParams{}
	if err := p.ForValues(c.version, r.URL.Query()); err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	c.WriteCacheable(w, Cacheable{
		TTL: 5 * time.Second,
		Key: c.cacheKeyForParams("list_blocks", p),
		CacheableFn: func(ctx context.Context) (interface{}, error) {
			return c.avaxReader.ListBlocks(ctx, p)
		},
	})
}

func (c *V2Context) GetBlock(w web.ResponseWriter, r *web.Request) {
	collectors := metrics.NewCollectors(
		metrics.NewCounterObserveMillisCollect(MetricMillis),
		metrics.NewCounterIncCollect(MetricCount),
	)
	defer func() {
		_ = collectors.Collect()
	}()

	id, err := ids.FromString(r.PathParams["id"])
	if err != nil {
		c.WriteErr(w, 400, err)
		return
	}

	c.WriteCacheable(w, Cacheable{
		Key: c.cacheKeyForID("get_block", r.PathParams["id"]),
		CacheableFn: func(ctx context.Context) (interface{}, error) {
			return c.avaxReader.GetBlock(ctx, id)
		},
	})
}

func (c *V2Context) TxJSON(w web.ResponseWriter, r *web.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.RequestTimeout)
	defer cancel()
	p := &params.TxJsonParam{}
	if err := p.ForValues(c.version, r.URL.Query()); err != nil {
		c.WriteErr(w, 400, err)
		return
	}
	b, err := c.avaxReader.TxJSON(ctx, p)
	if err != nil {
		c.WriteErr(w, 400, err)
		return
	}
	WriteJSON(w, b)
}
