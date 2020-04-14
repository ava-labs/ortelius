// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/utils/logging"
	"github.com/gocraft/web"

	"github.com/ava-labs/ortelius/cfg"
)

var (
	ErrUndefinedRouterFactory = errors.New("undefined router factory")
	ErrUnimplemented          = errors.New("unimplemented router factory")
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

func (s *Server) Shutdown() error {
	s.log.Info("Server shutting down")
	ctx, cancelFn := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFn()
	return s.server.Shutdown(ctx)
}

type routerFactory func(*web.Router, cfg.ServiceConfig, uint32, ids.ID, string) error

func routerFactorForVM(vmType string) routerFactory {
	switch vmType {
	case "avm":
		return NewAVMRouter
	case "pvm":
		return NewPVMRouter
	case "cvm":
		return NewCVMRouter
	}
	fmt.Println("type:", vmType)
	return undefinedRouterFactory(vmType)
}

func newRouter(conf cfg.APIConfig) (*web.Router, error) {
	// Create a root router that does the work common to all requests and provides
	// chain-agnostic endpoints
	router, err := newRootRouter(conf.ChainsConfig)
	if err != nil {
		return nil, err
	}

	for chainID, chainInfo := range conf.ChainsConfig {
		err = routerFactorForVM(chainInfo.VMType)(router, conf.ServiceConfig, conf.NetworkID, chainID, chainInfo.Alias)
		if err != nil {
			return nil, err
		}
	}

	return router, nil
}

func undefinedRouterFactory(vmType string) routerFactory {
	return func(_ *web.Router, _ cfg.ServiceConfig, _ uint32, _ ids.ID, _ string) error {
		return ErrUndefinedRouterFactory
	}
}
