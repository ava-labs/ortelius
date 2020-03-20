// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"context"
	"net/http"
	"time"

	"github.com/ava-labs/gecko/utils/logging"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"

	"github.com/gorilla/mux"
)

type Server struct {
	log    logging.Logger
	server *http.Server
}

func NewServer(conf cfg.APIConfig) (*Server, error) {
	log, err := logging.New(conf.Logging)
	if err != nil {
		return nil, err
	}

	index, err := services.NewRedisIndex(&conf.Redis)
	if err != nil {
		return nil, err
	}

	return &Server{
		log: log,
		server: &http.Server{
			Addr:         conf.ListenAddr,
			Handler:      newRouter(index),
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  15 * time.Second,
		},
	}, err
}

func (s *Server) Listen() error {
	s.log.Info("Server listening on %s", s.server.Addr)
	return s.server.ListenAndServe()
}

func (s *Server) Shutdown() error {
	ctx, cancelFn := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFn()
	return s.server.Shutdown(ctx)
}

func newRouter(index services.Index) *mux.Router {
	h := newHandlers(index)
	r := mux.NewRouter()
	r.HandleFunc("/", h.overview)
	r.HandleFunc("/tx/{id}", h.getTxByID)
	r.HandleFunc("/recent_txs", h.getRecentTxs)
	r.HandleFunc("/tx_count", h.getTxCount)
	return r
}
