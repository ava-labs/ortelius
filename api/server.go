// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"context"
	"net/http"
	"time"

	"github.com/ava-labs/avalanchego/utils/logging"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services/indexes/models"
)

var (
	// RequestTimeout is the maximum duration to allow an API request to execute
	RequestTimeout = 2 * time.Minute
)

// Server is an HTTP server configured with various ortelius APIs
type Server struct {
	log    logging.Logger
	server *http.Server
}

// NewServer creates a new *Server based on the given config
func NewServer(conf cfg.Config) (*Server, error) {
	log, err := logging.New(conf.Logging)
	if err != nil {
		return nil, err
	}

	router, err := newRouter(conf)
	if err != nil {
		return nil, err
	}

	// Set address prefix to use the configured network
	models.SetBech32HRP(conf.NetworkID)

	return &Server{
		log: log,
		server: &http.Server{
			Addr:         conf.ListenAddr,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: RequestTimeout,
			IdleTimeout:  15 * time.Second,
			Handler:      router,
		},
	}, err
}

// Listen begins listening for new socket connections and blocks until closed
func (s *Server) Listen() error {
	s.log.Info("Server listening on %s", s.server.Addr)
	return s.server.ListenAndServe()
}

// Close shuts the server down
func (s *Server) Close() error {
	s.log.Info("Server shutting down")
	ctx, cancelFn := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFn()
	return s.server.Shutdown(ctx)
}
