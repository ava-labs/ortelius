// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"sync"

	"github.com/ava-labs/gecko/ids"
	"github.com/gocraft/web"
	"github.com/gorilla/websocket"

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
		Get("/", (*AVMServerContext).Overview).
		Get("/search", (*AVMServerContext).Search).

		// Transaction index
		Get("/transactions", (*AVMServerContext).GetTxs).
		Get("/transactions/recent", (*AVMServerContext).GetRecentTxs).
		Get("/transactions/aggregates", (*AVMServerContext).GetTxAggregates).
		Get("/transactions/:id", (*AVMServerContext).GetTx).

		// Address index
		Get("/addresses/:addr/transactions", (*AVMServerContext).GetTxsForAddr).
		Get("/addresses/:addr/transaction_outputs", (*AVMServerContext).GetTXOsForAddr).

		// Asset index
		Get("/assets", (*AVMServerContext).GetAssets).
		Get("/assets/:alias_or_id", (*AVMServerContext).GetAsset).
		Get("/assets/:alias_or_id/transactions", (*AVMServerContext).GetTxsForAsset)

	return nil
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

type Notifier struct {
	mu        *sync.RWMutex
	id        uint64
	listeners map[uint64]chan string
}

func newNotifier() *Notifier {
	return &Notifier{
		mu:        &sync.RWMutex{},
		listeners: map[uint64]chan string{},
	}
}

func (n *Notifier) register() (chan string, func()) {
	n.mu.Lock()
	defer n.mu.Unlock()

	id := n.id
	n.id++

	ch := make(chan string)
	n.listeners[id] = ch

	return ch, func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		delete(n.listeners, id)
	}
}

var notifier = newNotifier()

func (c *AVMServerContext) NotificationsTransactions(w web.ResponseWriter, r *web.Request) {
	conn, err := upgrader.Upgrade(w, r.Request, nil)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}

	notifCh, unregisterFn := notifier.register()
	defer unregisterFn()

	for {
		notif := <-notifCh
		if err = conn.WriteJSON(notif); err != nil {
			writeErr(w, 500, err.Error())
			return
		}
	}
}

//
// Routes
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

//
// Transaction index
//

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

	tx, err := c.index.GetTx(txID)
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}

	writeObject(w, tx)
}

func (c *AVMServerContext) GetRecentTxs(w web.ResponseWriter, r *web.Request) {
	params, err := avm_index.ListTxParamForHTTPRequest(r.Request)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	params.Sort = avm_index.TxSortTimestampDesc

	txs, err := c.index.GetTxs(params)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}

	txIDS := make([]string, len(txs))
	for i, tx := range txs {
		id, _ := ids.FromString(string(tx.ID))
		txIDS[i] = id.String()
	}

	writeObject(w, txIDS)
}

func (c *AVMServerContext) GetTxAggregates(w web.ResponseWriter, r *web.Request) {
	params, err := avm_index.GetTransactionAggregatesParamsForHTTPRequest(r.Request)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	aggs, err := c.index.GetTransactionAggregatesHistogram(*params)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}

	writeObject(w, aggs)
}

func (c *AVMServerContext) GetTxs(w web.ResponseWriter, r *web.Request) {
	params, err := avm_index.ListTxParamForHTTPRequest(r.Request)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	txs, err := c.index.GetTxs(params)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}

	writeObject(w, txs)
}

func (c *AVMServerContext) GetTxsForAddr(w web.ResponseWriter, r *web.Request) {
	addrStr, present := getRequiredStringParam(w, r, "addr")
	if !present {
		return
	}

	addr, err := ids.ShortFromString(addrStr)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	params, err := avm_index.ListTxParamForHTTPRequest(r.Request)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	txs, err := c.index.GetTxsForAddr(addr, params)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	writeObject(w, txs)
}

func (c *AVMServerContext) GetTxsForAsset(w web.ResponseWriter, r *web.Request) {
	aliasOrID, present := getRequiredStringParam(w, r, "alias_or_id")
	if !present {
		return
	}

	assetID, err := ids.FromString(aliasOrID)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	params, err := avm_index.ListTxParamForHTTPRequest(r.Request)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	txs, err := c.index.GetTxsForAsset(assetID, params)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	writeObject(w, txs)
}

func (c *AVMServerContext) GetTXOsForAddr(w web.ResponseWriter, r *web.Request) {
	addrStr, present := getRequiredStringParam(w, r, "addr")
	if !present {
		return
	}

	addr, err := ids.ShortFromString(addrStr)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	params, err := avm_index.ListTXOParamForHTTPRequest(r.Request)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	txos, err := c.index.GetTXOsForAddr(addr, params)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	writeObject(w, txos)
}

//
// Transaction index
//

func (c *AVMServerContext) GetAssets(w web.ResponseWriter, r *web.Request) {
	params, err := avm_index.ListParamForHTTPRequest(r.Request)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	assets, err := c.index.GetAssets(params)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}

	writeObject(w, assets)
}

func (c *AVMServerContext) GetAsset(w web.ResponseWriter, r *web.Request) {
	aliasOrID, present := getRequiredStringParam(w, r, "alias_or_id")
	if !present {
		return
	}

	assets, err := c.index.GetAsset(aliasOrID)
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}

	writeObject(w, assets)
}

func getRequiredStringParam(w web.ResponseWriter, r *web.Request, name string) (string, bool) {
	addrStr := r.PathParams[name]
	if addrStr == "" {
		writeErr(w, 400, name+" is required")
		return "", false
	}
	return addrStr, true
}
