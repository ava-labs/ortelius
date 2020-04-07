// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"encoding/json"

	"github.com/ava-labs/gecko/ids"
	"github.com/go-redis/redis"
	"github.com/gocraft/web"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services/avm_index"
)

type AVMServerContext struct {
	*RootRequestContext

	chainID ids.ID
	index   *avm_index.Index
}

func NewAVMRouter(router *web.Router, path string, conf cfg.ServiceConfig, chainID ids.ID) error {
	index, err := avm_index.New(conf)
	if err != nil {
		return err
	}

	router.Subrouter(AVMServerContext{}, path).
		Middleware(func(c *AVMServerContext, w web.ResponseWriter, r *web.Request, next web.NextMiddlewareFunc) {
			c.chainID = chainID
			c.index = index
			next(w, r)
		}).
		Get("/", (*AVMServerContext).Overview).

		// Transaction index
		Get("/transactions/:id", (*AVMServerContext).GetTx).
		Get("/transactions/recent", (*AVMServerContext).GetRecentTxs).
		Get("/transactions/count", (*AVMServerContext).GetTxCount).

		// Address index
		Get("/addresses/:addr/transactions", (*AVMServerContext).GetTxsByAddr).
		Get("/addresses/:addr/transaction_outputs", (*AVMServerContext).GetTXOsByAddr)

	return nil
}

//
// Routes
//

func (c *AVMServerContext) Overview(_ web.ResponseWriter, _ *web.Request) {}

func (c *AVMServerContext) GetTx(w web.ResponseWriter, r *web.Request) {
	txIDStr, present := getRequiredStringParam(w, r, "id")
	if !present {
		return
	}

	txID, err := ids.FromString(txIDStr)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	tx, err := c.index.GetTx(c.chainID, txID)
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}

	writeJSON(w, tx)
}

func (c *AVMServerContext) GetRecentTxs(w web.ResponseWriter, _ *web.Request) {
	txIDs, err := c.index.GetRecentTxs(c.chainID, 100)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}

	jsonBytes, err := json.Marshal(txIDs)
	if err != nil {
		if err == redis.Nil {
			writeJSON(w, []byte{})
			return
		}
		writeErr(w, 500, err.Error())
		return
	}

	writeJSON(w, jsonBytes)
}

func (c *AVMServerContext) GetTxCount(w web.ResponseWriter, _ *web.Request) {
	count, err := c.index.GetTxCount(c.chainID)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}

	jsonBytes, err := json.Marshal(map[string]int64{"tx_count": count})
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}

	writeJSON(w, jsonBytes)
}

func (c *AVMServerContext) GetTxsByAddr(w web.ResponseWriter, r *web.Request) {
	addrStr, present := getRequiredStringParam(w, r, "addr")
	if !present {
		return
	}

	addr, err := ids.ShortFromString(addrStr)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	txs, err := c.index.GetTxsForAddr(c.chainID, addr)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	bytes, err := json.Marshal(txs)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	writeJSON(w, bytes)
}

func (c *AVMServerContext) GetTXOsByAddr(w web.ResponseWriter, r *web.Request) {
	addrStr, present := getRequiredStringParam(w, r, "addr")
	if !present {
		return
	}

	addr, err := ids.ShortFromString(addrStr)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	var spentPtr *bool
	if spentStr, ok := r.PathParams["spent"]; ok {
		spent := spentStr == "true"
		spentPtr = &spent
	}

	txos, err := c.index.GetTXOsForAddr(c.chainID, addr, spentPtr)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	bytes, err := json.Marshal(txos)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	writeJSON(w, bytes)
}

func getRequiredStringParam(w web.ResponseWriter, r *web.Request, name string) (string, bool) {
	addrStr := r.PathParams[name]
	if addrStr == "" {
		writeErr(w, 400, name+" is required")
		return "", false
	}
	return addrStr, true
}
