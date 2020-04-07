// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"encoding/json"

	"github.com/gocraft/web"

	"github.com/ava-labs/ortelius/cfg"
)

type index struct {
	ChainIDs cfg.ChainAliasConfig `json:"chainIDs"`
}

type RootRequestContext struct{}

func newRootRouter(chainIDs cfg.ChainAliasConfig) (*web.Router, error) {
	indexBytes, err := json.Marshal(&index{
		ChainIDs: chainIDs,
	})
	if err != nil {
		return nil, err
	}

	router := web.New(RootRequestContext{}).
		Middleware((*RootRequestContext).setCORsHeaders).
		Get("/", func(resp web.ResponseWriter, _ *web.Request) {
			resp.Write(indexBytes)
		})

	return router, nil
}

func (*RootRequestContext) setCORsHeaders(w web.ResponseWriter, r *web.Request, next web.NextMiddlewareFunc) {
	h := w.Header()
	h.Add("access-control-allow-headers", "Accept, Content-Type, Content-Length, Accept-Encoding")
	h.Add("access-control-allow-methods", "GET")
	h.Add("access-control-allow-origin", "*")
	next(w, r)
}
