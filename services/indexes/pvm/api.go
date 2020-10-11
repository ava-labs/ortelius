// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package pvm

import (
	"context"
	"time"

	"github.com/ava-labs/avalanchego/genesis"
	"github.com/ava-labs/avalanchego/ids"

	"github.com/gocraft/web"

	"github.com/ava-labs/ortelius/api"
	"github.com/ava-labs/ortelius/services/indexes/avm"
	"github.com/ava-labs/ortelius/services/indexes/params"
)

const VMName = "pvm"

func init() {
	api.RegisterRouter(VMName, NewAPIRouter, APIContext{})
}

type APIContext struct {
	*api.RootRequestContext

	networkID  uint32
	chainAlias string

	reader      *Reader
	avaxReader  *avm.Reader
	avaxAssetID ids.ID
}

func NewAPIRouter(params api.RouterParams) error {
	reader := NewReader(params.Connections)
	avaxReader := avm.NewReader(params.Connections, ChainID.String())

	_, avaxAssetID, err := genesis.Genesis(params.NetworkID)
	if err != nil {
		return err
	}

	params.Router.
		// Setup the context for each request
		Middleware(func(c *APIContext, w web.ResponseWriter, r *web.Request, next web.NextMiddlewareFunc) {
			c.reader = reader
			c.avaxReader = avaxReader

			c.networkID = params.NetworkID
			c.chainAlias = params.ChainConfig.Alias
			c.avaxAssetID = avaxAssetID
			next(w, r)
		}).

		// General routes
		// Get("/", (*APIContext).Overview).

		// List and Get routes
		Get("/transactions", (*APIContext).ListTransactions).
		Get("/blocks", (*APIContext).ListBlocks).
		// Get("/blocks/:id", (*APIContext).GetBlock).
		Get("/subnets", (*APIContext).ListSubnets).
		// Get("/subnets/:id", (*APIContext).GetSubnet).
		Get("/validators", (*APIContext).ListValidators).
		// Get("/validator/:id", (*APIContext).GetValidator).
		Get("/chains", (*APIContext).ListChains)
		// Get("/chains/:id", (*APIContext).GetChain)

	return nil
}

// func (c *APIContext) Overview(w web.ResponseWriter, _ *web.Request) {
// 	overview, err := c.reader.GetChainInfo(c.chainAlias, c.networkID)
// 	if err != nil {
// 		api.WriteErr(w, 500, err.Error())
// 		return
// 	}
//
// 	api.WriteObject(w, overview)
// }

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
			return c.avaxReader.ListTransactions(ctx, p, c.avaxAssetID)
		},
	})
}

func (c *APIContext) cacheKeyForParams(name string, p params.Param) []string {
	return append([]string{"pvm", name}, p.CacheKey()...)
}

func (c *APIContext) ListBlocks(w web.ResponseWriter, r *web.Request) {
	p := &params.ListParams{}
	if err := p.ForValues(r.URL.Query()); err != nil {
		api.WriteErr(w, 400, err.Error())
		return
	}

	blocks, err := c.reader.ListBlocks(c.Ctx(), params.ListBlocksParams{ListParams: *p})
	if err != nil {
		api.WriteErr(w, 500, err.Error())
		return
	}

	api.WriteObject(w, blocks)
}

func (c *APIContext) ListSubnets(w web.ResponseWriter, r *web.Request) {
	p := &params.ListParams{}
	if err := p.ForValues(r.URL.Query()); err != nil {
		api.WriteErr(w, 400, err.Error())
		return
	}

	blocks, err := c.reader.ListSubnets(c.Ctx(), params.ListSubnetsParams{ListParams: *p})
	if err != nil {
		api.WriteErr(w, 500, err.Error())
		return
	}

	api.WriteObject(w, blocks)
}

func (c *APIContext) ListValidators(w web.ResponseWriter, r *web.Request) {
	p := &params.ListParams{}
	if err := p.ForValues(r.URL.Query()); err != nil {
		api.WriteErr(w, 400, err.Error())
		return
	}

	blocks, err := c.reader.ListValidators(c.Ctx(), params.ListValidatorsParams{ListParams: *p})
	if err != nil {
		api.WriteErr(w, 500, err.Error())
		return
	}

	api.WriteObject(w, blocks)
}

func (c *APIContext) ListChains(w web.ResponseWriter, r *web.Request) {
	p := &params.ListParams{}
	if err := p.ForValues(r.URL.Query()); err != nil {
		api.WriteErr(w, 400, err.Error())
		return
	}

	blocks, err := c.reader.ListChains(c.Ctx(), params.ListChainsParams{ListParams: *p})
	if err != nil {
		api.WriteErr(w, 500, err.Error())
		return
	}

	api.WriteObject(w, blocks)
}
