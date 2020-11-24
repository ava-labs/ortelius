// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"context"
	"net/http"
	"time"

	"github.com/ava-labs/avalanchego/genesis"
	"github.com/ava-labs/avalanchego/utils/logging"
	avmVM "github.com/ava-labs/avalanchego/vms/avm"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/avax"
	"github.com/ava-labs/ortelius/services/indexes/avm"
	"github.com/ava-labs/ortelius/services/indexes/models"
	"github.com/ava-labs/ortelius/services/indexes/pvm"
	"github.com/gocraft/web"
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

	router, err := newRouter(conf, true)
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
			WriteTimeout: cfg.RequestTimeout,
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

func newRouter(conf cfg.Config, ro bool) (*web.Router, error) {
	// Pre-calculate IDs and index responses
	_, avaxAssetID, err := genesis.Genesis(conf.NetworkID)
	if err != nil {
		return nil, err
	}

	xChainGenesisTx, err := genesis.VMGenesis(conf.NetworkID, avmVM.ID)
	if err != nil {
		return nil, err
	}

	xChainID := xChainGenesisTx.ID()
	conf.Log.Info("Router chainID %s", xChainID.String())

	indexBytes, err := newIndexResponse(conf.NetworkID, xChainID, avaxAssetID)
	if err != nil {
		return nil, err
	}

	legacyIndexResponse, err := newLegacyIndexResponse(conf.NetworkID, xChainID, avaxAssetID)
	if err != nil {
		return nil, err
	}

	// Create connections and readers
	connections, err := services.NewConnectionsFromConfig(conf.Services, ro)
	if err != nil {
		return nil, err
	}

	var cache cacher = connections.Cache()
	if cache == nil {
		cache = &nullCache{}
	}

	avaxReader := avax.NewReader(connections)
	avmReader := avm.NewReader(connections)
	pvmReader := pvm.NewReader(connections)

	// Build router
	router := web.New(Context{}).
		Middleware(newContextSetter(conf.NetworkID, connections.Stream(), cache)).
		Middleware((*Context).setHeaders).
		Get("/", func(c *Context, resp web.ResponseWriter, _ *web.Request) {
			if _, err := resp.Write(indexBytes); err != nil {
				c.err = err
			}
		}).
		NotFound((*Context).notFoundHandler).
		Middleware(func(c *Context, w web.ResponseWriter, r *web.Request, next web.NextMiddlewareFunc) {
			c.avmReader = avmReader
			c.pvmReader = pvmReader
			c.avaxReader = avaxReader
			c.avaxAssetID = avaxAssetID

			next(w, r)
		})

	AddV2Routes(router, "/v2", indexBytes, nil)

	// Legacy routes.
	AddV2Routes(router, "/x", legacyIndexResponse, &xChainID)
	AddV2Routes(router, "/X", legacyIndexResponse, &xChainID)

	return router, nil
}
