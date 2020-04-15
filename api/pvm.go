// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"github.com/gocraft/web"

	"github.com/ava-labs/gecko/ids"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services/pvm_index"
)

type PVMServerContext struct {
	*RootRequestContext

	chainAlias string
	networkID  uint32
	index      *pvm_index.Index
}

func NewPVMRouter(router *web.Router, conf cfg.ServiceConfig, chainID ids.ID, chainAlias string, networkID uint32) error {
	index, err := pvm_index.New(conf, chainID)
	if err != nil {
		return err
	}

	err = index.Bootstrap()
	if err != nil {
		return err
	}

	router.Subrouter(PVMServerContext{}, "/"+chainAlias).
		// Setup the context for each request
		Middleware(func(c *PVMServerContext, w web.ResponseWriter, r *web.Request, next web.NextMiddlewareFunc) {
			c.chainAlias = chainAlias
			c.networkID = networkID

			c.index = index

			next(w, r)
		}).
		Get("/", (*PVMServerContext).Overview).

		// Transaction index
		Get("/transactions", (*PVMServerContext).GetTxs).
		Get("/transactions/:id", (*PVMServerContext).GetTx).

		// Account index
		Get("/accounts/:addr/transactions", (*PVMServerContext).GetTxsForAccount).

		// Chain index
		Get("/chains", (*PVMServerContext).GetChains).
		Get("/chains/:id", (*PVMServerContext).GetChain).

		// Subnet index
		Get("/subnets", (*PVMServerContext).GetSubnets).
		Get("/subnets/:id", (*PVMServerContext).GetSubnet).

		// Validator index
		Get("/validators", (*PVMServerContext).GetValidators).
		Get("/validators/:subnet_id", (*PVMServerContext).GetValidatorsForSubnet).
		Get("/validators/current", (*PVMServerContext).GetCurrentValidators).
		Get("/validators/:subnet_id/current", (*PVMServerContext).GetCurrentValidatorsForSubnet)

	return nil
}

//
// Routes
//

func (c *PVMServerContext) Overview(w web.ResponseWriter, _ *web.Request) {
	overview, err := c.index.GetChainInfo(c.chainAlias, c.networkID)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}

	writeObject(w, overview)
}

//
// Transactions
//

func (c *PVMServerContext) GetTxs(w web.ResponseWriter, _ *web.Request) {
	txs, err := c.index.GetTxs()
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}

	writeObject(w, txs)
}

func (c *PVMServerContext) GetTx(w web.ResponseWriter, r *web.Request) {
	txIDStr, present := getRequiredStringParam(w, r, "id")
	if !present {
		return
	}

	txID, err := ids.FromString(txIDStr)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	tx, err := c.index.GetTx(txID)
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}

	writeObject(w, tx)
}

func (c *PVMServerContext) GetTxsForAccount(w web.ResponseWriter, r *web.Request) {
	addrStr, present := getRequiredStringParam(w, r, "addr")
	if !present {
		return
	}

	addr, err := ids.ShortFromString(addrStr)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	txs, err := c.index.GetTxsForAccount(addr)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	writeObject(w, txs)
}

//
// Chains
//

func (c *PVMServerContext) GetChains(w web.ResponseWriter, _ *web.Request) {
	chains, err := c.index.GetChains()
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}

	writeObject(w, chains)
}

func (c *PVMServerContext) GetChain(w web.ResponseWriter, r *web.Request) {
	idStr, present := getRequiredStringParam(w, r, "alias_or_id")
	if !present {
		return
	}

	id, err := ids.FromString(idStr)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	chain, err := c.index.GetChain(id)
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}

	writeObject(w, chain)
}

//
// Subnets
//

func (c *PVMServerContext) GetSubnets(w web.ResponseWriter, _ *web.Request) {
	subnets, err := c.index.GetSubnets()
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}

	writeObject(w, subnets)
}

func (c *PVMServerContext) GetSubnet(w web.ResponseWriter, r *web.Request) {
	idStr, present := getRequiredStringParam(w, r, "alias_or_id")
	if !present {
		return
	}

	id, err := ids.FromString(idStr)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	subnet, err := c.index.GetSubnet(id)
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}

	writeObject(w, subnet)
}

//
// Validators
//

func (c *PVMServerContext) GetValidators(w web.ResponseWriter, _ *web.Request) {
	subnets, err := c.index.GetValidatorsForSubnet(ids.Empty)
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}

	writeObject(w, subnets)
}

func (c *PVMServerContext) GetCurrentValidators(w web.ResponseWriter, _ *web.Request) {
	subnets, err := c.index.GetCurrentValidatorsForSubnet(ids.Empty)
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}

	writeObject(w, subnets)
}

func (c *PVMServerContext) GetValidatorsForSubnet(w web.ResponseWriter, _ *web.Request) {
	subnets, err := c.index.GetValidatorsForSubnet(ids.Empty)
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}

	writeObject(w, subnets)
}

func (c *PVMServerContext) GetCurrentValidatorsForSubnet(w web.ResponseWriter, _ *web.Request) {
	subnets, err := c.index.GetCurrentValidatorsForSubnet(ids.Empty)
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}

	writeObject(w, subnets)
}
