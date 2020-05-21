// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm

import (
	"github.com/ava-labs/gecko/ids"
	"github.com/gocraft/web"

	"github.com/ava-labs/ortelius/api"
)

type APIContext struct {
	*api.RootRequestContext

	index      *Index
	networkID  uint32
	chainAlias string
}

func NewAPIRouter(params api.RouterFactoryParams) error {
	index, err := New(params.ServiceConfig, params.NetworkID, params.ChainConfig.ID)
	if err != nil {
		return err
	}

	params.Router.
		// Setup the context for each request
		Middleware(func(c *APIContext, w web.ResponseWriter, r *web.Request, next web.NextMiddlewareFunc) {
			c.index = index
			c.networkID = params.NetworkID
			c.chainAlias = params.ChainConfig.Alias
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
		Get("/assets/:alias_or_id", (*APIContext).GetAsset).
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
	overview, err := c.index.GetChainInfo(c.chainAlias, c.networkID)
	if err != nil {
		api.WriteErr(w, 500, err.Error())
		return
	}

	api.WriteObject(w, overview)
}

func (c *APIContext) Search(w web.ResponseWriter, r *web.Request) {
	params, err := SearchParamsForHTTPRequest(r.Request)
	if err != nil {
		api.WriteErr(w, 400, err.Error())
		return
	}

	results, err := c.index.Search(*params)
	if err != nil {
		api.WriteErr(w, 500, err.Error())
		return
	}

	api.WriteObject(w, results)
}

func (c *APIContext) Aggregate(w web.ResponseWriter, r *web.Request) {
	params, err := GetAggregateTransactionsParamsForHTTPRequest(r.Request)
	if err != nil {
		api.WriteErr(w, 400, err.Error())
		return
	}

	aggs, err := c.index.Aggregate(*params)
	if err != nil {
		api.WriteErr(w, 500, err.Error())
		return
	}

	api.WriteObject(w, aggs)
}

//
// List and Get routes
//

func (c *APIContext) ListTransactions(w web.ResponseWriter, r *web.Request) {
	params, err := ListTransactionsParamsForHTTPRequest(r.Request)
	if err != nil {
		api.WriteErr(w, 400, err.Error())
		return
	}

	txs, err := c.index.ListTransactions(params)
	if err != nil {
		api.WriteErr(w, 500, err.Error())
		return
	}

	api.WriteObject(w, txs)
}

func (c *APIContext) GetTransaction(w web.ResponseWriter, r *web.Request) {
	id, err := ids.FromString(r.PathParams["id"])
	if err != nil {
		api.WriteErr(w, 400, err.Error())
		return
	}
	tx, err := c.index.GetTransaction(id)
	if err != nil {
		api.WriteErr(w, 404, err.Error())
		return
	}
	api.WriteObject(w, tx)
}

func (c *APIContext) ListAssets(w web.ResponseWriter, r *web.Request) {
	params, err := ListAssetsParamsForHTTPRequest(r.Request)
	if err != nil {
		api.WriteErr(w, 400, err.Error())
		return
	}

	assets, err := c.index.ListAssets(params)
	if err != nil {
		api.WriteErr(w, 500, err.Error())
		return
	}

	api.WriteObject(w, assets)
}

func (c *APIContext) GetAsset(w web.ResponseWriter, r *web.Request) {
	assets, err := c.index.GetAsset(r.PathParams["alias_or_id"])
	if err != nil {
		api.WriteErr(w, 404, err.Error())
		return
	}

	api.WriteObject(w, assets)
}

func (c *APIContext) ListAddresses(w web.ResponseWriter, r *web.Request) {
	params, err := ListAddressesParamsForHTTPRequest(r.Request)
	if err != nil {
		api.WriteErr(w, 400, err.Error())
		return
	}

	assets, err := c.index.ListAddresses(params)
	if err != nil {
		api.WriteErr(w, 500, err.Error())
		return
	}

	api.WriteObject(w, assets)
}

func (c *APIContext) GetAddress(w web.ResponseWriter, r *web.Request) {
	id, err := ids.ShortFromString(r.PathParams["id"])
	if err != nil {
		api.WriteErr(w, 400, err.Error())
		return
	}
	addr, err := c.index.GetAddress(id)
	if err != nil {
		api.WriteErr(w, 404, err.Error())
		return
	}

	api.WriteObject(w, addr)
}

func (c *APIContext) ListOutputs(w web.ResponseWriter, r *web.Request) {
	params, err := ListOutputsParamsForHTTPRequest(r.Request)
	if err != nil {
		api.WriteErr(w, 400, err.Error())
		return
	}

	assets, err := c.index.ListOutputs(params)
	if err != nil {
		api.WriteErr(w, 500, err.Error())
		return
	}

	api.WriteObject(w, assets)
}

func (c *APIContext) GetOutput(w web.ResponseWriter, r *web.Request) {
	id, err := ids.FromString(r.PathParams["id"])
	if err != nil {
		api.WriteErr(w, 400, err.Error())
		return
	}
	addr, err := c.index.GetOutput(id)
	if err != nil {
		api.WriteErr(w, 404, err.Error())
		return
	}

	api.WriteObject(w, addr)
}
