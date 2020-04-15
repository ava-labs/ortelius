// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"encoding/json"
	"sync"

	"github.com/ava-labs/gecko/ids"
	"github.com/go-redis/redis"
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

func NewAVMRouter(router *web.Router, conf cfg.ServiceConfig, chainID ids.ID, chainAlias string, networkID uint32) error {
	index, err := avm_index.New(conf, chainID)
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

		// Transaction index
		Get("/transactions", (*AVMServerContext).GetTxs).
		Get("/transactions/:id", (*AVMServerContext).GetTx).
		Get("/transactions/recent", (*AVMServerContext).GetRecentTxs).

		// Address index
		Get("/addresses/:addr/transactions", (*AVMServerContext).GetTxsByAddr).
		Get("/addresses/:addr/transaction_outputs", (*AVMServerContext).GetTXOsByAddr).

		// Asset index
		Get("/assets", (*AVMServerContext).GetAssets).
		Get("/assets/:alias_or_id", (*AVMServerContext).GetAsset).
		Get("/assets/:alias_or_id/transactions", (*AVMServerContext).GetTxsByAsset)

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

//
// Transaction index
//

func (c *AVMServerContext) GetRecentTxs(w web.ResponseWriter, _ *web.Request) {
	txIDs, err := c.index.GetRecentTxs(100)
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

func (c *AVMServerContext) GetTxs(w web.ResponseWriter, _ *web.Request) {
	txs, err := c.index.GetTxs()
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}

	writeObject(w, txs)
}

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

	writeJSON(w, tx)
}

//
// Transaction index
//

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

	txs, err := c.index.GetTxsForAddr(addr)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	writeObject(w, txs)
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

	txos, err := c.index.GetTXOsForAddr(addr, spentPtr)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	writeObject(w, txos)
}

//
// Transaction index
//

func (c *AVMServerContext) GetAssets(w web.ResponseWriter, _ *web.Request) {
	assets, err := c.index.GetAssets()
	if err != nil {
		writeErr(w, 404, err.Error())
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

func (c *AVMServerContext) GetTxsByAsset(w web.ResponseWriter, r *web.Request) {
	aliasOrID, present := getRequiredStringParam(w, r, "alias_or_id")
	if !present {
		return
	}

	assetID, err := ids.FromString(aliasOrID)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	txs, err := c.index.GetTxsForAsset(assetID)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}

	writeObject(w, txs)
}

func getRequiredStringParam(w web.ResponseWriter, r *web.Request, name string) (string, bool) {
	addrStr := r.PathParams[name]
	if addrStr == "" {
		writeErr(w, 400, name+" is required")
		return "", false
	}
	return addrStr, true
}

./build/ava --http-port=21000 --staking-port=21001 --public-ip=73.202.190.144 --bootstrap-ips=3.227.207.132:9630,107.23.241.199:9630,54.197.215.186:9630 --snow-sample-size=1 --snow-quorum-size=1 --staking-tls-enabled=false

3.227.207.132
34.207.133.167
107.23.241.199
54.197.215.186
18.234.153.22
http port = 21000
staking port = 21001

./build/ava --log-level=verbo --network-id=cascade --public-ip=73.202.190.144 --bootstrap-ips=3.227.207.132:21001 --bootstrap-ids=NX4zVkuiRJZYe6Nzzav7GXN3TakUet3Co
