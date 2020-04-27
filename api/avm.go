// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"github.com/ava-labs/gecko/ids"
	"github.com/gocraft/web"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services/avm_index"
)

type AVMServerContext struct {
	*RootRequestContext

	chainAlias string
	networkID  uint32
	index      *avm_index.Index
}

func NewAVMRouter(router *web.Router, conf cfg.ServiceConfig, networkID uint32, chainID ids.ID, chainAlias string) error {
	index, err := avm_index.New(conf, networkID, chainID)
	if err != nil {
		return err
	}

	router.Subrouter(AVMServerContext{}, "/"+chainAlias).
		// Setup the context for each request
		Middleware(func(c *AVMServerContext, w web.ResponseWriter, r *web.Request, next web.NextMiddlewareFunc) {
			c.chainAlias = chainAlias
			c.networkID = networkID

			c.index = index

			next(w, r)
		}).

		// General routes
		Get("/", (*AVMServerContext).Overview).
		Get("/search", (*AVMServerContext).Search).
		Get("/aggregates", (*AVMServerContext).Aggregate).
		Get("/transactions/aggregates", (*AVMServerContext).Aggregate). // DEPRECATED

		// List and Get routes
		Get("/transactions", (*AVMServerContext).ListTransactions).
		Get("/transactions/:id", (*AVMServerContext).GetTransaction).
		Get("/assets", (*AVMServerContext).ListAssets).
		Get("/assets/:alias_or_id", (*AVMServerContext).GetAsset).
		Get("/addresses", (*AVMServerContext).ListAddresses).
		Get("/addresses/:id", (*AVMServerContext).GetAddress).
		Get("/outputs", (*AVMServerContext).ListOutputs).
		Get("/outputs/:id", (*AVMServerContext).GetOutput)

	return nil
}

//
// General routes
//

func (c *AVMServerContext) Overview(w web.ResponseWriter, _ *web.Request) {
	overview, err := c.index.GetChainInfo(c.chainAlias, c.networkID)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}

	writeObject(w, overview)
}

func (c *AVMServerContext) Search(w web.ResponseWriter, r *web.Request) {
	params, err := avm_index.SearchParamsForHTTPRequest(r.Request)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	results, err := c.index.Search(*params)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}

	writeObject(w, results)
}

func (c *AVMServerContext) Aggregate(w web.ResponseWriter, r *web.Request) {
	params, err := avm_index.GetAggregateTransactionsParamsForHTTPRequest(r.Request)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	aggs, err := c.index.Aggregate(*params)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}

	writeObject(w, aggs)
}

//
// List and Get routes
//

func (c *AVMServerContext) ListTransactions(w web.ResponseWriter, r *web.Request) {
	params, err := avm_index.ListTransactionsParamsForHTTPRequest(r.Request)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	txs, err := c.index.ListTransactions(params)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}

	writeObject(w, txs)
}

func (c *AVMServerContext) GetTransaction(w web.ResponseWriter, r *web.Request) {
	id, err := ids.FromString(r.PathParams["id"])
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	tx, err := c.index.GetTransaction(id)
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}
	writeObject(w, tx)
}

func (c *AVMServerContext) ListAssets(w web.ResponseWriter, r *web.Request) {
	params, err := avm_index.ListAssetsParamsForHTTPRequest(r.Request)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	assets, err := c.index.ListAssets(params)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}

	writeObject(w, assets)
}

func (c *AVMServerContext) GetAsset(w web.ResponseWriter, r *web.Request) {
	assets, err := c.index.GetAsset(r.PathParams["alias_or_id"])
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}

	writeObject(w, assets)
}

func (c *AVMServerContext) ListAddresses(w web.ResponseWriter, r *web.Request) {
	params, err := avm_index.ListAddressesParamsForHTTPRequest(r.Request)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	assets, err := c.index.ListAddresses(params)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}

	writeObject(w, assets)
}

func (c *AVMServerContext) GetAddress(w web.ResponseWriter, r *web.Request) {
	id, err := ids.ShortFromString(r.PathParams["id"])
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	addr, err := c.index.GetAddress(id)
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}

	writeObject(w, addr)
}

func (c *AVMServerContext) ListOutputs(w web.ResponseWriter, r *web.Request) {
	params, err := avm_index.ListOutputsParamsForHTTPRequest(r.Request)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	assets, err := c.index.ListOutputs(params)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}

	writeObject(w, assets)
}

func (c *AVMServerContext) GetOutput(w web.ResponseWriter, r *web.Request) {
	id, err := ids.FromString(r.PathParams["id"])
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	addr, err := c.index.GetOutput(id)
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}

	writeObject(w, addr)
}
