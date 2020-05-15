// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package pvm_index

import (
	"github.com/ava-labs/gecko/ids"
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
	p, err := params.ListParamsForHTTPRequest(r.Request)
	if err != nil {
		api.WriteErr(w, 400, err.Error())
		return
	}

	blocks, err := c.index.ListBlocks(ListBlocksParams{ListParams: p})
	if err != nil {
		api.WriteErr(w, 500, err.Error())
		return
	}

	api.WriteObject(w, blocks)
}

func (i *Index) GetBlock(id ids.ID) (*Block, error) {
	list, err := i.db.ListBlocks(ListBlocksParams{ID: &id})
	if err != nil || len(list.Blocks) == 0 {
		return nil, err
	}
	return list.Blocks[0], nil
}

func (c *APIContext) ListSubnets(w web.ResponseWriter, r *web.Request) {
	p, err := params.ListParamsForHTTPRequest(r.Request)
	if err != nil {
		api.WriteErr(w, 400, err.Error())
		return
	}

	blocks, err := c.index.ListSubnets(ListSubnetsParams{ListParams: p})
	if err != nil {
		api.WriteErr(w, 500, err.Error())
		return
	}

	api.WriteObject(w, blocks)
}

func (i *Index) GetSubnet(id ids.ID) (*Subnet, error) {
	list, err := i.db.ListSubnets(ListSubnetsParams{ID: &id})
	if err != nil || len(list.Subnets) == 0 {
		return nil, err
	}
	return list.Subnets[0], nil
}

func (c *APIContext) ListValidators(w web.ResponseWriter, r *web.Request) {
	p, err := params.ListParamsForHTTPRequest(r.Request)
	if err != nil {
		api.WriteErr(w, 400, err.Error())
		return
	}

	blocks, err := c.index.ListValidators(ListValidatorsParams{ListParams: p})
	if err != nil {
		api.WriteErr(w, 500, err.Error())
		return
	}

	api.WriteObject(w, blocks)
}

func (i *Index) GetValidator(id ids.ID) (*Validator, error) {
	list, err := i.db.ListValidators(ListValidatorsParams{ID: &id})
	if err != nil || len(list.Validators) == 0 {
		return nil, err
	}
	return list.Validators[0], nil
}

func (c *APIContext) ListChains(w web.ResponseWriter, r *web.Request) {
	p, err := params.ListParamsForHTTPRequest(r.Request)
	if err != nil {
		api.WriteErr(w, 400, err.Error())
		return
	}

	blocks, err := c.index.ListChains(ListChainsParams{ListParams: p})
	if err != nil {
		api.WriteErr(w, 500, err.Error())
		return
	}

	api.WriteObject(w, blocks)
}

func (i *Index) GetChain(id ids.ID) (*Chain, error) {
	list, err := i.db.ListChains(ListChainsParams{ID: &id})
	if err != nil || len(list.Chains) == 0 {
		return nil, err
	}
	return list.Chains[0], nil
}
