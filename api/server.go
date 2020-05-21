// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/ava-labs/gecko/utils/logging"

	"github.com/ava-labs/ortelius/cfg"
)

var (
	ErrUndefinedRouterFactory = errors.New("undefined Router factory")
	ErrUnimplemented          = errors.New("unimplemented Router factory")
)

type Server struct {
	log    logging.Logger
	server *http.Server
}

func NewServer(conf cfg.Config) (*Server, error) {
	loggingConf, err := logging.DefaultConfig()
	if err != nil {
		return nil, err
	}
	loggingConf.Directory = conf.LogDirectory

	log, err := logging.New(loggingConf)
	if err != nil {
		return nil, err
	}

	router, err := newRouter(conf)
	if err != nil {
		return nil, err
	}

	return &Server{
		log: log,
		server: &http.Server{
			Addr:         conf.ListenAddr,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  15 * time.Second,
			Handler:      router,
		},
	}, err
}

func (s *Server) Listen() error {
	s.log.Info("Server listening on %s", s.server.Addr)
	return s.server.ListenAndServe()
}

func (s *Server) Close() error {
	s.log.Info("Server shutting down")
	ctx, cancelFn := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFn()
	return s.server.Shutdown(ctx)
}
