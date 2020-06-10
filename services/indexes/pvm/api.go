// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package pvm

import (
	"github.com/gocraft/web"

	"github.com/ava-labs/ortelius/api"
	"github.com/ava-labs/ortelius/services/params"
)

type APIContext struct {
	*api.RootRequestContext

	index      *Index
	networkID  uint32
	chainAlias string
}

func NewAPIRouter(params api.RouterParams) error {
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

		// List and Get routes
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

func (c *APIContext) Overview(w web.ResponseWriter, _ *web.Request) {
	overview, err := c.index.GetChainInfo(c.chainAlias, c.networkID)
	if err != nil {
		api.WriteErr(w, 500, err.Error())
		return
	}

	api.WriteObject(w, overview)
}

func (c *APIContext) ListBlocks(w web.ResponseWriter, r *web.Request) {
	p := &params.ListParams{}
	if err := p.ForValues(r.URL.Query()); err != nil {
		api.WriteErr(w, 400, err.Error())
		return
	}

	blocks, err := c.index.ListBlocks(c.Ctx(), ListBlocksParams{ListParams: *p})
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

	blocks, err := c.index.ListSubnets(c.Ctx(), ListSubnetsParams{ListParams: *p})
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

	blocks, err := c.index.ListValidators(c.Ctx(), ListValidatorsParams{ListParams: *p})
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

	blocks, err := c.index.ListChains(c.Ctx(), ListChainsParams{ListParams: *p})
	if err != nil {
		api.WriteErr(w, 500, err.Error())
		return
	}

	api.WriteObject(w, blocks)
}
